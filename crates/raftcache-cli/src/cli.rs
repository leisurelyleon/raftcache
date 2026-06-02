//! Command-line argument definitions.

use clap::{Parser, Subcommand};

/// A distributed, replicated key-value cache built on a Raft consensus core.
#[derive(Debug, Parser)]
#[command(name = "raftcache", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a scripted 3-node cluster scenario: elect, replicate, partition, heal.
    Demo,
}
