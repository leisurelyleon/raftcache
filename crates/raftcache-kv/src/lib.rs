//! The replicated key-value state machine for `raftcache`.
//!
//! Committed log entries carry encoded [`Command`]s; applying them in log order
//! to a [`KvStore`] yields a consistent replicated state across the cluster.

pub mod command;
pub mod store;

pub use command::{Command, CommandError};
pub use store::KvStore;
