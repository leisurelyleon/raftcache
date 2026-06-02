# 4. Key-value store as the replicated state machine

- Status: Accepted
- Date: 2026-06

## Context

Consensus replicates an opaque command log; an application must define what the
commands mean and apply them deterministically.

## Decision

Layer a key-value store on top of the consensus core. Committed entries decode
to `Set`/`Delete` commands applied in log order. The KV crate does not depend on
the consensus crate — consensus replicates opaque bytes; the KV layer interprets
them.

## Consequences

- Applying the same committed sequence yields identical state on every replica.
- The layering mirrors real systems (consensus underneath, application on top).
- Either layer could be reused independently.
