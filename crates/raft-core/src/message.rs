//! Raft RPCs and their replies.

use serde::{Deserialize, Serialize};

use crate::log::LogEntry;
use crate::types::{LogIndex, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    /// On success, the highest log index the follower now matches.
    pub match_index: LogIndex,
}

/// A Raft message: one of the four RPC payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
}

impl Message {
    /// The term carried by this message (used for the universal step-down rule).
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote(m) => m.term,
            Message::RequestVoteReply(m) => m.term,
            Message::AppendEntries(m) => m.term,
            Message::AppendEntriesReply(m) => m.term,
        }
    }
}
