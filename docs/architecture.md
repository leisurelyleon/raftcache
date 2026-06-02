# Architecture

`raftcache` is a Rust workspace implementing a replicated key-value cache on a
Raft consensus core, organized so the consensus algorithm is pure and
deterministically testable.

## Crates

```text
raft-core       the pure consensus state machine: elections, replication, commit rules
raftcache-kv    the replicated state machine: applies committed Set/Delete commands
raftcache-sim   the deterministic harness: virtual network, logical clock, safety checks
raftcache-cli   the binary: run a scripted cluster scenario
```

## The pure consensus core

`RaftNode::step(event) -> Vec<Action>` is the entire algorithm. Events are
logical clock ticks, incoming messages, and client proposals; actions are
outgoing messages and "apply this committed entry" commands. The core reads no
clock, opens no socket, and spawns no thread — so a simulation can drive it
deterministically and reproduce any scenario exactly.

## Layering

Consensus replicates opaque `Vec<u8>` commands; it does not know what they mean.
`raftcache-kv` defines the meaning: committed bytes decode to `Set`/`Delete`
commands applied to a key-value store. Because application is deterministic,
every replica that applies the same committed sequence reaches the same state.
The two layers are decoupled: the core could replicate any application's
commands, and the store could sit on any log.

## The simulation harness

`raftcache-sim` owns N nodes, a `VirtualNetwork` (fixed-delay delivery with
clean two-way partitions and message drops), and a logical clock. It runs
scenarios and checks two Raft safety properties:

- **Election Safety** — at most one leader per term.
- **State Machine Safety** — no two nodes apply different commands at the same
  log index.

See [`raft-notes.md`](raft-notes.md) for what is and isn't implemented.
