# 2. Deterministic simulation for correctness

- Status: Accepted
- Date: 2026-06

## Context

Distributed correctness must be verified under adversarial conditions —
partitions, message loss, reordering — which are impossible to reproduce
reliably against a live network.

## Decision

Test the system with a deterministic simulation: a virtual network with a
logical clock, controllable partitions and drops, driving the pure cores. Assert
Raft's safety properties (Election Safety, State Machine Safety) hold throughout.

## Consequences

- Adversarial scenarios are reproducible and fast.
- Safety is demonstrated by assertion, not assumed.
- A failing scenario is a deterministic, debuggable test case.
