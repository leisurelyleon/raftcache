//! Deterministic simulation harness for `raftcache`.
//!
//! [`Cluster`] drives N [`raft_core::RaftNode`]s over a [`VirtualNetwork`] on a
//! logical clock, supports partitions and message loss, and checks Raft's
//! safety properties — election safety and applied-entry consistency.

pub mod cluster;
pub mod network;

pub use cluster::Cluster;
pub use network::VirtualNetwork;

// Re-exported for convenience so downstream crates (the CLI) need only depend
// on the simulation crate for these scalar types.
pub use raft_core::{LogIndex, NodeId, Role, Term};
