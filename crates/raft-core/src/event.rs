//! Inputs to and outputs from the consensus state machine.

use crate::message::Message;
use crate::types::{LogIndex, NodeId};

/// An input event the node reacts to.
#[derive(Debug, Clone)]
pub enum Event {
    /// A logical clock tick (drives election and heartbeat timeouts).
    Tick,
    /// An incoming message from another node.
    Message { from: NodeId, message: Message },
    /// A client proposing a command (only meaningful on the leader).
    Propose { command: Vec<u8> },
}

/// A side effect the node asks its environment to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Send a message to another node.
    Send { to: NodeId, message: Message },
    /// Apply a committed command to the application state machine.
    Apply { index: LogIndex, command: Vec<u8> },
}
