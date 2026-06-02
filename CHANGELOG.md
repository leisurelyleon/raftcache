# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial workspace scaffold: raft-core, raftcache-kv, raftcache-sim, raftcache-cli.

## [0.1.0] - TBD

### Added
- Pure, event-driven Raft consensus core: leader election, log replication, and
  commit-index advancement as a side-effect-free state machine.
- Replicated key-value store applying committed Set/Delete commands.
- Deterministic simulation harness with a virtual network (drop/delay/partition)
  and logical clock, asserting Raft safety properties.
- CLI to run scripted cluster scenarios.

[Unreleased]: https://github.com/leisurelyleon/raftcache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/leisurelyleon/raftcache/releases/tag/v0.1.0
