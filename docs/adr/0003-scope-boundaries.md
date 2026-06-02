# 3. Scope boundaries

- Status: Accepted
- Date: 2026-06

## Context

A full production Raft includes PreVote, snapshotting, membership changes, and
durable storage. Implementing all of them well is a large undertaking; doing
them halfway introduces subtle bugs.

## Decision

Implement the core safety algorithm — election (with the election restriction),
replication (with log matching), and the current-term commit rule — correctly
and test it hard. Explicitly document the omitted features and why (see
`raft-notes.md`).

## Consequences

- The implemented subset is correct and verified, not approximate.
- A reader knows exactly what is and isn't present.
- The omitted features are clean future extensions behind the same core.
