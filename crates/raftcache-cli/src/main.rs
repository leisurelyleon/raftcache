//! `raftcache` command-line entry point.

use clap::Parser;

use raftcache_cli::cli::{Cli, Command};
use raftcache_kv::{Command as KvCommand, KvStore};
use raftcache_sim::{Cluster, NodeId};

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Demo => run_demo(),
    }
}

/// Advances the cluster until a leader exists, up to `max` ticks.
fn run_until_leader(cluster: &mut Cluster, max: u64) {
    for _ in 0..max {
        if cluster.leader().is_some() {
            return;
        }
        cluster.tick();
    }
}

/// Reconstructs a node's key-value state by applying its committed entries.
fn store_for(cluster: &Cluster, node: NodeId) -> KvStore {
    let mut store = KvStore::new();
    for (index, bytes) in cluster.applied_entries(node) {
        if let Ok(command) = KvCommand::decode(&bytes) {
            store.apply(index, &command);
        }
    }
    store
}

fn print_kv_state(cluster: &Cluster) {
    for id in cluster.node_ids() {
        let store = store_for(cluster, id);
        let pairs: Vec<String> =
            store.pairs().into_iter().map(|(k, v)| format!("{k}={v}")).collect();
        println!("  node {id}: {{ {} }}", pairs.join(", "));
    }
}

fn run_demo() {
    let mut cluster = Cluster::new(3);

    println!("== Election ==");
    run_until_leader(&mut cluster, 50);
    match cluster.leader() {
        Some(leader) => println!("Leader elected: node {leader} (term {})",
                                 cluster.term_of(leader).unwrap_or(0)),
        None => println!("No leader elected within the tick budget."),
    }

    println!("\n== Replication ==");
    cluster.propose(KvCommand::Set { key: "alpha".into(), value: "1".into() }.encode());
    cluster.run(20);
    cluster.propose(KvCommand::Set { key: "beta".into(), value: "2".into() }.encode());
    cluster.run(20);
    println!("Replicated key-value state across the cluster:");
    print_kv_state(&cluster);

    println!("\n== Partition the leader ==");
    let old_leader = cluster.leader().expect("a leader before partition");
    cluster.partition(&[old_leader]);
    cluster.run(40);
    match cluster.leader() {
        Some(leader) => println!(
            "Old leader node {old_leader} isolated; majority elected node {leader} (term {})",
            cluster.term_of(leader).unwrap_or(0)
        ),
        None => println!("No leader on the majority side yet."),
    }

    println!("\n== Heal ==");
    cluster.heal();
    cluster.run(40);
    if let Some(leader) = cluster.leader() {
        println!("Cluster healed; leader is node {leader} (term {})",
                 cluster.term_of(leader).unwrap_or(0));
    }

    println!("\n== Safety checks ==");
    match cluster.check_one_leader_per_term() {
        Ok(()) => println!("  election safety: OK (at most one leader per term)"),
        Err(why) => println!("  election safety: VIOLATED ({why})"),
    }
    match cluster.check_applied_consistency() {
        Ok(()) => println!("  state-machine safety: OK (no divergent committed entries)"),
        Err(why) => println!("  state-machine safety: VIOLATED ({why})"),
    }

    println!("\nFinal replicated state:");
    print_kv_state(&cluster);
}
