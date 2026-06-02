//! The replicated commands and their wire encoding.

use serde::{Deserialize, Serialize};

/// Errors decoding a command from bytes.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("decode error: {0}")]
    Decode(#[from] serde_json::Error),
}

/// A state-changing operation on the key-value store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

impl Command {
    /// Serializes the command to the opaque bytes the log replicates.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Command is always serializable")
    }

    /// Decodes a command from replicated bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CommandError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_roundtrips() {
        let cmd = Command::Set { key: "k".into(), value: "v".into() };
        let bytes = cmd.encode();
        assert_eq!(Command::decode(&bytes).unwrap(), cmd);
    }

    #[test]
    fn delete_roundtrips() {
        let cmd = Command::Delete { key: "k".into() };
        assert_eq!(Command::decode(&cmd.encode()).unwrap(), cmd);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(Command::decode(b"not json").is_err());
    }
}
