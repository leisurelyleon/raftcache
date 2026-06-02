//! The Raft consensus state machine: `step(event) -> actions`, with no I/O,
//! no clock, and no threads. All timing arrives as `Event::Tick`; all messaging
//! is expressed as `Action::Send`.

use std::collections::{BTreeMap, BTreeSet};

use crate::event::{Action, Event};
use crate::log::{Log, LogEntry};
use crate::message::{
    AppendEntries, AppendEntriesReply, Message, RequestVote, RequestVoteReply,
};
use crate::types::{LogIndex, NodeId, Role, Term};

/// Timeout configuration, in logical ticks.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub election_timeout: u32,
    pub heartbeat_timeout: u32,
}

/// A single Raft node.
pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,
    config: Config,

    // Persistent state (in-memory in this implementation).
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Volatile state.
    role: Role,
    commit_index: LogIndex,
    last_applied: LogIndex,
    leader_id: Option<NodeId>,

    // Timers.
    election_elapsed: u32,
    heartbeat_elapsed: u32,

    // Candidate state.
    votes_received: BTreeSet<NodeId>,

    // Leader state.
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, config: Config) -> Self {
        Self {
            id,
            peers,
            config,
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            votes_received: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
        }
    }

    // --- Accessors (used by the simulation and tests) ---
    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn log_last_index(&self) -> LogIndex {
        self.log.last_index()
    }

    /// The number of votes constituting a majority of the cluster.
    fn quorum(&self) -> usize {
        let total = self.peers.len() + 1;
        total / 2 + 1
    }

    /// Drives the state machine with one event, returning the resulting actions.
    pub fn step(&mut self, event: Event) -> Vec<Action> {
        let mut actions = Vec::new();
        match event {
            Event::Tick => self.on_tick(&mut actions),
            Event::Propose { command } => self.on_propose(command, &mut actions),
            Event::Message { from, message } => {
                // Universal rule: any message with a higher term forces a
                // step-down to follower and resets the vote for the new term.
                if message.term() > self.current_term {
                    self.current_term = message.term();
                    self.voted_for = None;
                    self.role = Role::Follower;
                    self.leader_id = None;
                    self.votes_received.clear();
                }
                match message {
                    Message::RequestVote(m) => self.on_request_vote(from, m, &mut actions),
                    Message::RequestVoteReply(m) => {
                        self.on_request_vote_reply(from, m, &mut actions);
                    }
                    Message::AppendEntries(m) => self.on_append_entries(from, m, &mut actions),
                    Message::AppendEntriesReply(m) => {
                        self.on_append_entries_reply(from, m, &mut actions);
                    }
                }
            }
        }
        actions
    }

    fn on_tick(&mut self, out: &mut Vec<Action>) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_timeout {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append_entries(out);
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.config.election_timeout {
                    self.become_candidate(out);
                }
            }
        }
    }

    fn on_propose(&mut self, command: Vec<u8>, out: &mut Vec<Action>) {
        if self.role != Role::Leader {
            return; // only the leader accepts proposals
        }
        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            command,
        };
        self.log.append(entry);
        if self.peers.is_empty() {
            self.advance_commit(out); // single-node cluster commits immediately
        } else {
            self.broadcast_append_entries(out);
        }
    }

    fn on_request_vote(&mut self, from: NodeId, req: RequestVote, out: &mut Vec<Action>) {
        let grant = req.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id))
            && self.log_is_up_to_date(req.last_log_term, req.last_log_index);
        if grant {
            self.voted_for = Some(req.candidate_id);
            self.election_elapsed = 0;
        }
        out.push(Action::Send {
            to: from,
            message: Message::RequestVoteReply(RequestVoteReply {
                term: self.current_term,
                vote_granted: grant,
            }),
        });
    }

    fn on_request_vote_reply(
        &mut self,
        from: NodeId,
        reply: RequestVoteReply,
        out: &mut Vec<Action>,
    ) {
        if self.role != Role::Candidate || reply.term != self.current_term {
            return;
        }
        if reply.vote_granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.quorum() {
                self.become_leader(out);
            }
        }
    }

    fn on_append_entries(&mut self, _from: NodeId, ae: AppendEntries, out: &mut Vec<Action>) {
        if ae.term < self.current_term {
            // Stale leader: reject and let it learn our higher term.
            self.reply_append(ae.leader_id, false, 0, out);
            return;
        }

        // A valid leader for the current term: (re)establish follower status.
        self.role = Role::Follower;
        self.leader_id = Some(ae.leader_id);
        self.election_elapsed = 0;

        // Log-matching check: we must already hold prev_log_index at prev_log_term.
        let prev_ok = match self.log.term_at(ae.prev_log_index) {
            Some(term) => term == ae.prev_log_term,
            None => false,
        };
        if !prev_ok {
            self.reply_append(ae.leader_id, false, 0, out);
            return;
        }

        // Append entries, truncating on any conflicting term.
        for entry in &ae.entries {
            match self.log.entry_at(entry.index) {
                Some(existing) if existing.term == entry.term => {} // already present
                Some(_) => {
                    self.log.truncate_from(entry.index);
                    self.log.append(entry.clone());
                }
                None => self.log.append(entry.clone()),
            }
        }

        let match_index = ae.prev_log_index + ae.entries.len() as u64;

        if ae.leader_commit > self.commit_index {
            let new_commit = ae.leader_commit.min(match_index).min(self.log.last_index());
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.apply_committed(out);
            }
        }

        self.reply_append(ae.leader_id, true, match_index, out);
    }

    fn on_append_entries_reply(
        &mut self,
        from: NodeId,
        reply: AppendEntriesReply,
        out: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        if reply.success {
            self.match_index.insert(from, reply.match_index);
            self.next_index.insert(from, reply.match_index + 1);
            self.advance_commit(out);
        } else {
            // Back off and retry from an earlier point in the follower's log.
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            let new_next = next.saturating_sub(1).max(1);
            self.next_index.insert(from, new_next);
            out.push(Action::Send {
                to: from,
                message: self.build_append_entries(from),
            });
        }
    }

    fn become_candidate(&mut self, out: &mut Vec<Action>) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.election_elapsed = 0;
        self.leader_id = None;

        if self.peers.is_empty() {
            self.become_leader(out); // a lone node wins instantly
            return;
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for &peer in &self.peers {
            out.push(Action::Send {
                to: peer,
                message: Message::RequestVote(RequestVote {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.log.last_index() + 1;
        // Build both maps from `peers` without holding a borrow of `self.peers`
        // across a mutation of `self.next_index`: each RHS fully evaluates
        // (releasing the peers borrow) before the assignment writes the field.
        self.next_index = self.peers.iter().map(|&p| (p, next)).collect();
        self.match_index = self.peers.iter().map(|&p| (p, 0u64)).collect();
        self.heartbeat_elapsed = 0;
        self.broadcast_append_entries(out);
    }

    fn advance_commit(&mut self, out: &mut Vec<Action>) {
        let last = self.log.last_index();
        let mut new_commit = self.commit_index;
        let mut n = last;
        while n > self.commit_index {
            let mut count = 1usize; // self always holds up to last_index
            for peer in &self.peers {
                if self.match_index.get(peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }
            // Safety: only commit an entry from the CURRENT term by replica count.
            if count >= self.quorum() && self.log.term_at(n) == Some(self.current_term) {
                new_commit = n;
                break;
            }
            n -= 1;
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed(out);
        }
    }

    fn apply_committed(&mut self, out: &mut Vec<Action>) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.entry_at(self.last_applied) {
                out.push(Action::Apply {
                    index: entry.index,
                    command: entry.command.clone(),
                });
            }
        }
    }

    fn broadcast_append_entries(&self, out: &mut Vec<Action>) {
        for &peer in &self.peers {
            let message = self.build_append_entries(peer);
            out.push(Action::Send { to: peer, message });
        }
    }

    fn build_append_entries(&self, to: NodeId) -> Message {
        let next = self.next_index.get(&to).copied().unwrap_or(self.log.last_index() + 1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        let entries = self.log.entries_from(next).to_vec();
        Message::AppendEntries(AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        })
    }

    fn reply_append(
        &self,
        to: NodeId,
        success: bool,
        match_index: LogIndex,
        out: &mut Vec<Action>,
    ) {
        out.push(Action::Send {
            to,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: self.current_term,
                success,
                match_index,
            }),
        });
    }

    /// The election restriction: a candidate's log must be at least as
    /// up-to-date as ours (higher last term, or equal term and >= index).
    fn log_is_up_to_date(&self, cand_term: Term, cand_index: LogIndex) -> bool {
        let my_term = self.log.last_term();
        let my_index = self.log.last_index();
        cand_term > my_term || (cand_term == my_term && cand_index >= my_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config { election_timeout: 3, heartbeat_timeout: 1 }
    }

    fn node() -> RaftNode {
        RaftNode::new(0, vec![1, 2], cfg())
    }

    fn count_sends(actions: &[Action]) -> usize {
        actions.iter().filter(|a| matches!(a, Action::Send { .. })).count()
    }

    #[test]
    fn new_node_is_follower_at_term_zero() {
        let n = node();
        assert_eq!(n.role(), Role::Follower);
        assert_eq!(n.current_term(), 0);
    }

    #[test]
    fn tick_to_timeout_starts_election() {
        let mut n = node();
        n.step(Event::Tick);
        n.step(Event::Tick);
        let actions = n.step(Event::Tick); // third tick hits election_timeout
        assert_eq!(n.role(), Role::Candidate);
        assert_eq!(n.current_term(), 1);
        assert_eq!(count_sends(&actions), 2); // RequestVote to both peers
    }

    #[test]
    fn grants_vote_to_up_to_date_candidate() {
        let mut n = node();
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::RequestVote(RequestVote {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            }),
        });
        match &actions[0] {
            Action::Send { message: Message::RequestVoteReply(r), .. } => {
                assert!(r.vote_granted);
            }
            _ => panic!("expected a vote reply"),
        }
    }

    #[test]
    fn denies_second_vote_in_same_term() {
        let mut n = node();
        let rv = |cand| RequestVote {
            term: 1,
            candidate_id: cand,
            last_log_index: 0,
            last_log_term: 0,
        };
        n.step(Event::Message { from: 1, message: Message::RequestVote(rv(1)) });
        let actions = n.step(Event::Message { from: 2, message: Message::RequestVote(rv(2)) });
        match &actions[0] {
            Action::Send { message: Message::RequestVoteReply(r), .. } => {
                assert!(!r.vote_granted);
            }
            _ => panic!("expected a vote reply"),
        }
    }

    #[test]
    fn election_restriction_denies_behind_candidate() {
        let mut n = node();
        // Give n a log entry at term 2 via a leader.
        n.step(Event::Message {
            from: 9,
            message: Message::AppendEntries(AppendEntries {
                term: 2,
                leader_id: 9,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry { term: 2, index: 1, command: vec![] }],
                leader_commit: 0,
            }),
        });
        // A candidate whose log ends at term 1 is behind -> deny.
        let actions = n.step(Event::Message {
            from: 5,
            message: Message::RequestVote(RequestVote {
                term: 3,
                candidate_id: 5,
                last_log_index: 1,
                last_log_term: 1,
            }),
        });
        match &actions[0] {
            Action::Send { message: Message::RequestVoteReply(r), .. } => {
                assert!(!r.vote_granted);
            }
            _ => panic!("expected a vote reply"),
        }
    }

    #[test]
    fn candidate_becomes_leader_on_majority() {
        let mut n = node();
        n.step(Event::Tick);
        n.step(Event::Tick);
        n.step(Event::Tick); // now candidate, term 1
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::RequestVoteReply(RequestVoteReply { term: 1, vote_granted: true }),
        });
        assert_eq!(n.role(), Role::Leader);
        assert_eq!(count_sends(&actions), 2); // initial heartbeats
    }

    fn make_leader() -> RaftNode {
        let mut n = node();
        n.step(Event::Tick);
        n.step(Event::Tick);
        n.step(Event::Tick);
        n.step(Event::Message {
            from: 1,
            message: Message::RequestVoteReply(RequestVoteReply { term: 1, vote_granted: true }),
        });
        n
    }

    #[test]
    fn leader_appends_on_propose() {
        let mut n = make_leader();
        let actions = n.step(Event::Propose { command: vec![7] });
        assert_eq!(n.log_last_index(), 1);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Send { message: Message::AppendEntries(ae), .. } if !ae.entries.is_empty()
        )));
    }

    #[test]
    fn leader_commits_current_term_entry_on_majority() {
        let mut n = make_leader();
        n.step(Event::Propose { command: vec![7] }); // entry index 1, term 1
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: 1,
                success: true,
                match_index: 1,
            }),
        });
        assert_eq!(n.commit_index(), 1);
        assert!(actions.iter().any(|a| matches!(a, Action::Apply { index: 1, .. })));
    }

    #[test]
    fn follower_appends_and_acks() {
        let mut n = node();
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry { term: 1, index: 1, command: vec![] }],
                leader_commit: 0,
            }),
        });
        assert_eq!(n.log_last_index(), 1);
        match &actions[0] {
            Action::Send { message: Message::AppendEntriesReply(r), .. } => {
                assert!(r.success);
                assert_eq!(r.match_index, 1);
            }
            _ => panic!("expected an append reply"),
        }
    }

    #[test]
    fn follower_rejects_mismatched_prev() {
        let mut n = node();
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 1,
                prev_log_index: 5, // we don't have index 5
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        });
        match &actions[0] {
            Action::Send { message: Message::AppendEntriesReply(r), .. } => assert!(!r.success),
            _ => panic!("expected an append reply"),
        }
    }

    #[test]
    fn follower_applies_committed_entries() {
        let mut n = node();
        let actions = n.step(Event::Message {
            from: 1,
            message: Message::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry { term: 1, index: 1, command: vec![42] }],
                leader_commit: 1,
            }),
        });
        assert_eq!(n.commit_index(), 1);
        assert!(actions.iter().any(|a| matches!(a, Action::Apply { index: 1, .. })));
    }

    #[test]
    fn steps_down_on_higher_term() {
        let mut n = node();
        n.step(Event::Tick);
        n.step(Event::Tick);
        n.step(Event::Tick); // candidate, term 1
        n.step(Event::Message {
            from: 1,
            message: Message::AppendEntries(AppendEntries {
                term: 5,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        });
        assert_eq!(n.role(), Role::Follower);
        assert_eq!(n.current_term(), 5);
    }
}
