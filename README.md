# raftcache

> A distributed, replicated key-value cache built on a deterministic Raft consensus core.

`raftcache` is a replicated key-value store kept consistent across a cluster by
the Raft consensus algorithm. Its defining characteristic is *how* the consensus
core is built: as a pure, side-effect-free state machine, which makes the
algorithm deterministically testable. A simulation harness drives a virtual
multi-node cluster, injects partitions and message loss, and asserts Raft's
safety properties hold throughout.

## Approach

Consensus is hard to get right and harder to test under a live network. So the
Raft core here performs no I/O and reads no clock: it is a function from
`(state, event)` to `(state, actions)`. All timing and messaging are external.
This lets a simulation deterministically reproduce adversarial conditions —
split votes, a partitioned leader, dropped messages — and verify that:

- at most one leader is elected per term, and
- an entry committed by any node is never lost or overwritten.

## Architecture

```
raft-core       the pure consensus state machine: elections, replication, commit rules
raftcache-kv    the replicated state machine: applies committed Set/Delete commands
raftcache-sim   the deterministic harness: virtual network, logical clock, safety checks
raftcache-cli   the binary: run scripted cluster scenarios
```

See [`docs/architecture.md`](docs/architecture.md) and
[`docs/raft-notes.md`](docs/raft-notes.md), which documents exactly which parts
of Raft are implemented and what is intentionally out of scope.

## Build & Test

```bash
cargo build
cargo test
```

## Run

```bash
# Run a scripted cluster scenario: elect a leader, replicate, partition, heal
cargo run -p raftcache-cli -- demo
```

## License

MIT — see [LICENSE](LICENSE).
