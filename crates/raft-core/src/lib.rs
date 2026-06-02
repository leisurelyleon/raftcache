//! A pure, event-driven Raft consensus state machine.
//!
//! [`RaftNode::step`] maps an [`Event`] (a tick, an incoming [`Message`], or a
//! client proposal) to a list of [`Action`]s (messages to send, entries to
//! apply). It performs no I/O, reads no clock, and spawns no threads, so the
//! algorithm is deterministically testable — see the `raftcache-sim` crate.

pub mod error;
pub mod event;
pub mod log;
pub mod message;
pub mod node;
pub mod types;

pub use error::CoreError;
pub use event::{Action, Event};
pub use log::{Log, LogEntry};
pub use message::{AppendEntries, AppendEntriesReply, Message, RequestVote, RequestVoteReply};
pub use node::{Config, RaftNode};
pub use types::{LogIndex, NodeId, Role, Term};
