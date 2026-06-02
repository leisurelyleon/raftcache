//! A virtual, in-memory network with a logical clock, fixed delivery delay, and
//! clean two-way partitions. Fully deterministic.

use std::collections::BTreeSet;

use raft_core::{Message, NodeId};

struct InFlight {
    from: NodeId,
    to: NodeId,
    message: Message,
    deliver_at: u64,
}

/// Routes messages between nodes with a fixed delay, dropping any message that
/// crosses a partition boundary.
pub struct VirtualNetwork {
    queue: Vec<InFlight>,
    isolated: BTreeSet<NodeId>,
    delay: u64,
}

impl VirtualNetwork {
    pub fn new(delay: u64) -> Self {
        Self { queue: Vec::new(), isolated: BTreeSet::new(), delay }
    }

    /// Two nodes are connected iff they are on the same side of the partition
    /// (both isolated, or both not).
    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        self.isolated.contains(&a) == self.isolated.contains(&b)
    }

    /// Enqueues a message for delivery at `now + delay`.
    pub fn send(&mut self, from: NodeId, to: NodeId, message: Message, now: u64) {
        self.queue.push(InFlight { from, to, message, deliver_at: now + self.delay });
    }

    /// Removes and returns every message due at or before `now` whose link is
    /// currently connected; due-but-partitioned messages are dropped.
    pub fn deliver_due(&mut self, now: u64) -> Vec<(NodeId, NodeId, Message)> {
        let mut due = Vec::new();
        let mut remaining = Vec::new();
        for item in std::mem::take(&mut self.queue) {
            if item.deliver_at > now {
                remaining.push(item);
            } else if self.connected(item.from, item.to) {
                due.push((item.from, item.to, item.message));
            }
            // else: due but partitioned -> dropped
        }
        self.queue = remaining;
        due
    }

    /// Isolates `nodes` from the rest of the cluster.
    pub fn partition(&mut self, nodes: &[NodeId]) {
        self.isolated = nodes.iter().copied().collect();
    }

    /// Removes all partitions.
    pub fn heal(&mut self) {
        self.isolated.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::{Message, RequestVoteReply};

    fn msg() -> Message {
        Message::RequestVoteReply(RequestVoteReply { term: 1, vote_granted: true })
    }

    #[test]
    fn delivers_after_delay() {
        let mut net = VirtualNetwork::new(2);
        net.send(0, 1, msg(), 0); // deliver_at = 2
        assert!(net.deliver_due(1).is_empty());
        assert_eq!(net.deliver_due(2).len(), 1);
    }

    #[test]
    fn drops_across_partition() {
        let mut net = VirtualNetwork::new(1);
        net.partition(&[0]); // isolate node 0
        net.send(0, 1, msg(), 0); // 0 -> 1 crosses the boundary
        assert!(net.deliver_due(5).is_empty());
    }

    #[test]
    fn delivers_within_same_side() {
        let mut net = VirtualNetwork::new(1);
        net.partition(&[0]);
        net.send(1, 2, msg(), 0); // both outside the isolated set
        assert_eq!(net.deliver_due(5).len(), 1);
    }
}
