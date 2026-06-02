#!/usr/bin/env bash
# Run the scripted cluster scenario: elect a leader, replicate key-value
# commands, partition the leader, heal, and verify Raft safety properties.
set -euo pipefail

cargo build --release

echo "== raftcache cluster scenario =="
./target/release/raftcache demo
