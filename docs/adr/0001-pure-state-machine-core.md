# 1. A pure, event-driven consensus core

- Status: Accepted
- Date: 2026-06

## Context

Consensus algorithms are notoriously hard to implement and harder to test when
entangled with real networking, timers, and threads, where bugs hide behind
nondeterminism.

## Decision

Implement the Raft core as a pure state machine: `step(event) -> actions`, with
no I/O, no clock, and no threads. Timing arrives as tick events; messaging is
expressed as send actions performed by the environment.

## Consequences

- The algorithm is deterministically testable in isolation.
- A simulation can reproduce any scenario exactly, including failures.
- The same core can run over a real network by supplying a real driver.
