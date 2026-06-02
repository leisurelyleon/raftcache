//! The replicated log. Entries are 1-indexed; index 0 is a sentinel.

use serde::{Deserialize, Serialize};

use crate::types::{LogIndex, Term};

/// One log entry: a term-stamped, opaque application command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

/// The replicated log: a contiguous sequence of entries starting at index 1.
#[derive(Debug, Clone, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    /// The index of the last entry, or 0 if empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(0, |e| e.index)
    }

    /// The term of the last entry, or 0 if empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(0, |e| e.term)
    }

    /// The term of the entry at `index`. Index 0 is the sentinel (term 0).
    /// Returns `None` if `index` is beyond the log.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entries.get((index - 1) as usize).map(|e| e.term)
    }

    /// The entry at `index`, if present (index 0 has no entry).
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// Appends an entry. Callers maintain contiguity (index == last_index + 1).
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Removes every entry with index >= `index`.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if index == 0 {
            self.entries.clear();
        } else {
            self.entries.truncate((index - 1) as usize);
        }
    }

    /// A borrowed slice of all entries with index >= `index`.
    pub fn entries_from(&self, index: LogIndex) -> &[LogEntry] {
        if index == 0 {
            return &self.entries;
        }
        let start = (index - 1) as usize;
        if start >= self.entries.len() {
            &[]
        } else {
            &self.entries[start..]
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: Term, index: LogIndex) -> LogEntry {
        LogEntry { term, index, command: vec![] }
    }

    #[test]
    fn empty_log_sentinels() {
        let log = Log::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);
    }

    #[test]
    fn append_and_term_at() {
        let mut log = Log::new();
        log.append(entry(1, 1));
        log.append(entry(2, 2));
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));
    }

    #[test]
    fn truncate_from_removes_tail() {
        let mut log = Log::new();
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(1, 3));
        log.truncate_from(2);
        assert_eq!(log.last_index(), 1);
        assert!(log.entry_at(2).is_none());
    }

    #[test]
    fn entries_from_borrows_tail() {
        let mut log = Log::new();
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        assert_eq!(log.entries_from(2).len(), 1);
        assert_eq!(log.entries_from(3).len(), 0);
    }
}
