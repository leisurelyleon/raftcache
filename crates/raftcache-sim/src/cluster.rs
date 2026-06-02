//! Drives a virtual multi-node Raft cluster on a logical clock and checks
//! Raft's safety properties.

use std::collections::BTreeMap;

use raft_core::{Action, Config, Event, LogIndex, NodeId, RaftNode, Term};

use crate::network::VirtualNetwork;

/// A simulated cluster of Raft nodes over a virtual network.
pub struct Cluster {
    nodes: BTreeMap<NodeId, RaftNode>,
    network: VirtualNetwork,
    /// Per-node record of applied entries, for the consistency safety check.
    applied: BTreeMap<NodeId, BTreeMap<LogIndex, Vec<u8>>>,
    time: u64,
}

impl Cluster {
    /// Builds an `n`-node cluster with staggered election timeouts (so a leader
    /// emerges deterministically) and frequent heartbeats.
    pub fn new(node_count: u64) -> Self {
        let ids: Vec<NodeId> = (0..node_count).collect();
        let mut nodes = BTreeMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            let config = Config {
                election_timeout: 5 + (id as u32) * 2,
                heartbeat_timeout: 1,
            };
            nodes.insert(id, RaftNode::new(id, peers, config));
        }
        Self {
            nodes,
            network: VirtualNetwork::new(1),
            applied: BTreeMap::new(),
            time: 0,
        }
    }

    /// Advances the logical clock by one step: deliver due messages, then tick
    /// every node, dispatching all resulting actions.
    pub fn tick(&mut self) {
        self.time += 1;

        let due = self.network.deliver_due(self.time);
        for (from, to, message) in due {
            let actions = match self.nodes.get_mut(&to) {
                Some(node) => node.step(Event::Message { from, message }),
                None => continue,
            };
            self.dispatch(to, actions);
        }

        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            let actions = match self.nodes.get_mut(&id) {
                Some(node) => node.step(Event::Tick),
                None => continue,
            };
            self.dispatch(id, actions);
        }
    }

    /// Advances the clock by `ticks` steps.
    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    /// Routes a node's emitted actions: sends go to the network; applies are
    /// recorded for the consistency check.
    fn dispatch(&mut self, source: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => self.network.send(source, to, message, self.time),
                Action::Apply { index, command } => {
                    self.applied.entry(source).or_default().insert(index, command);
                }
            }
        }
    }

    /// The current leader (the leader with the highest term, if any).
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.is_leader())
            .max_by_key(|n| n.current_term())
            .map(|n| n.id())
    }

    /// Proposes a command to the current leader. Returns false if there is none.
    pub fn propose(&mut self, command: Vec<u8>) -> bool {
        match self.leader() {
            Some(leader_id) => {
                let actions = match self.nodes.get_mut(&leader_id) {
                    Some(node) => node.step(Event::Propose { command }),
                    None => return false,
                };
                self.dispatch(leader_id, actions);
                true
            }
            None => false,
        }
    }

    pub fn partition(&mut self, nodes: &[NodeId]) {
        self.network.partition(nodes);
    }

    pub fn heal(&mut self) {
        self.network.heal();
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn term_of(&self, node: NodeId) -> Option<Term> {
        self.nodes.get(&node).map(|n| n.current_term())
    }

    /// The committed command bytes applied at `index` on `node`, if any.
    pub fn applied_command(&self, node: NodeId, index: LogIndex) -> Option<Vec<u8>> {
        self.applied.get(&node).and_then(|m| m.get(&index)).cloned()
    }

    /// All entries applied on `node`, in index order.
    pub fn applied_entries(&self, node: NodeId) -> Vec<(LogIndex, Vec<u8>)> {
        self.applied
            .get(&node)
            .map(|m| m.iter().map(|(i, c)| (*i, c.clone())).collect())
            .unwrap_or_default()
    }

    // --- Safety properties ---

    /// Election Safety: at most one leader is elected per term.
    pub fn check_one_leader_per_term(&self) -> Result<(), String> {
        let mut leaders: BTreeMap<Term, usize> = BTreeMap::new();
        for node in self.nodes.values() {
            if node.is_leader() {
                *leaders.entry(node.current_term()).or_default() += 1;
            }
        }
        for (term, count) in &leaders {
            if *count > 1 {
                return Err(format!("term {term} has {count} leaders"));
            }
        }
        Ok(())
    }

    /// State Machine Safety: if two nodes have applied an entry at the same
    /// index, the commands must be identical (no committed-entry divergence).
    pub fn check_applied_consistency(&self) -> Result<(), String> {
        let mut by_index: BTreeMap<LogIndex, Vec<u8>> = BTreeMap::new();
        for per_node in self.applied.values() {
            for (index, command) in per_node {
                match by_index.get(index) {
                    Some(existing) if existing != command => {
                        return Err(format!("divergent command applied at index {index}"));
                    }
                    _ => {
                        by_index.insert(*index, command.clone());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs until a leader exists, up to `max` ticks.
    fn run_until_leader(cluster: &mut Cluster, max: u64) {
        for _ in 0..max {
            if cluster.leader().is_some() {
                return;
            }
            cluster.tick();
        }
    }

    #[test]
    fn three_nodes_elect_single_leader() {
        let mut cluster = Cluster::new(3);
        run_until_leader(&mut cluster, 30);
        assert!(cluster.leader().is_some());
        cluster.check_one_leader_per_term().unwrap();
    }

    #[test]
    fn leader_replicates_to_all_nodes() {
        let mut cluster = Cluster::new(3);
        run_until_leader(&mut cluster, 30);
        assert!(cluster.propose(vec![1, 2, 3]));
        cluster.run(20);

        for id in cluster.node_ids() {
            assert_eq!(
                cluster.applied_command(id, 1),
                Some(vec![1, 2, 3]),
                "node {id} should have applied index 1"
            );
        }
        cluster.check_applied_consistency().unwrap();
    }

    #[test]
    fn partition_then_heal_preserves_safety() {
        let mut cluster = Cluster::new(3);
        run_until_leader(&mut cluster, 30);

        // Replicate an entry to the whole cluster first.
        assert!(cluster.propose(vec![9]));
        cluster.run(15);
        for id in cluster.node_ids() {
            assert_eq!(cluster.applied_command(id, 1), Some(vec![9]));
        }

        // Isolate the leader; the majority side must elect a new one.
        let old_leader = cluster.leader().expect("a leader before partition");
        cluster.partition(&[old_leader]);
        cluster.run(40);
        let new_leader = cluster.leader().expect("majority side elects a leader");
        assert_ne!(new_leader, old_leader);

        // Heal and let terms reconcile; safety must hold throughout.
        cluster.heal();
        cluster.run(40);
        cluster.check_one_leader_per_term().unwrap();
        cluster.check_applied_consistency().unwrap();

        // The originally-committed entry is never lost.
        for id in cluster.node_ids() {
            assert_eq!(cluster.applied_command(id, 1), Some(vec![9]));
        }
    }
}
