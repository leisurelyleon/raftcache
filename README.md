# raftcache
A distributed, replicated key-value cache in Rust built on a Raft consensus core. The consensus engine is a deterministic, side-effect-free state machine, enabling a simulation harness that injects partitions and message loss to verify safety properties — one leader per term, committed entries never lost — across a multi-node cluster.
