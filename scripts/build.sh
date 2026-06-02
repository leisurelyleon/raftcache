#!/usr/bin/env bash
# Build the workspace in release mode.
set -euo pipefail

cargo build --release
echo "Build complete. Binary at target/release/raftcache"
