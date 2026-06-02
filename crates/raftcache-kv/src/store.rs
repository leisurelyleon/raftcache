//! The key-value store that applies committed commands deterministically.

use std::collections::BTreeMap;

use crate::command::Command;

/// An in-memory key-value store. Applying the same sequence of committed
/// commands always yields the same state — the property replication relies on.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct KvStore {
    map: BTreeMap<String, String>,
    applied_index: u64,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a committed command stamped with its log `index`.
    pub fn apply(&mut self, index: u64, command: &Command) {
        match command {
            Command::Set { key, value } => {
                self.map.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.map.remove(key);
            }
        }
        self.applied_index = index;
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    /// A sorted snapshot of the key-value pairs (for display).
    pub fn pairs(&self) -> Vec<(String, String)> {
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_get() {
        let mut store = KvStore::new();
        store.apply(
            1,
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            },
        );
        assert_eq!(store.get("a"), Some(&"1".to_string()));
        assert_eq!(store.applied_index(), 1);
    }

    #[test]
    fn delete_removes_key() {
        let mut store = KvStore::new();
        store.apply(
            1,
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            },
        );
        store.apply(2, &Command::Delete { key: "a".into() });
        assert_eq!(store.get("a"), None);
        assert_eq!(store.applied_index(), 2);
    }

    #[test]
    fn same_command_sequence_is_deterministic() {
        let cmds = [
            Command::Set {
                key: "a".into(),
                value: "1".into(),
            },
            Command::Set {
                key: "b".into(),
                value: "2".into(),
            },
            Command::Delete { key: "a".into() },
        ];
        let mut first = KvStore::new();
        let mut second = KvStore::new();
        for (i, cmd) in cmds.iter().enumerate() {
            first.apply(i as u64 + 1, cmd);
            second.apply(i as u64 + 1, cmd);
        }
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
    }
}
