//! Core scalar types.

/// A node's stable identifier.
pub type NodeId = u64;

/// A Raft term number (monotonically increasing).
pub type Term = u64;

/// A 1-based index into the replicated log. Index 0 means "before the first
/// entry" (a sentinel used by the log-matching check).
pub type LogIndex = u64;

/// A node's role in the current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
