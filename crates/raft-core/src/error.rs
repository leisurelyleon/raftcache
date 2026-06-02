//! Core error type.

/// Errors produced by the consensus core.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("log inconsistency: {0}")]
    Log(String),
}
