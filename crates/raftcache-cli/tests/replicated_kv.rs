//! End-to-end integration: drive a real cluster through the simulation, feed it
//! actual KV commands, then reconstruct each node's key-value state from its
//! committed log and assert all replicas agree — the property the whole system
//! exists to provide.

use raftcache_kv::{Command, KvStore};
use raftcache_sim::{Cluster, NodeId};

/// Advances until a leader exists, up to `max` ticks.
fn run_until_leader(cluster: &mut Cluster, max: u64) {
    for _ in 0..max {
        if cluster.leader().is_some() {
            return;
        }
        cluster.tick();
    }
}

/// Reconstructs a node's KV state by applying its committed entries in order.
fn store_for(cluster: &Cluster, node: NodeId) -> KvStore {
    let mut store = KvStore::new();
    for (index, bytes) in cluster.applied_entries(node) {
        let command = Command::decode(&bytes).expect("committed bytes decode as a Command");
        store.apply(index, &command);
    }
    store
}

#[test]
fn replicated_kv_state_agrees_across_nodes() {
    let mut cluster = Cluster::new(3);
    run_until_leader(&mut cluster, 50);
    assert!(cluster.leader().is_some(), "a leader should be elected");

    // Apply a sequence of real KV commands through consensus.
    let commands = [
        Command::Set {
            key: "alpha".into(),
            value: "1".into(),
        },
        Command::Set {
            key: "beta".into(),
            value: "2".into(),
        },
        Command::Set {
            key: "alpha".into(),
            value: "updated".into(),
        },
        Command::Delete { key: "beta".into() },
    ];
    for command in &commands {
        assert!(
            cluster.propose(command.encode()),
            "leader should accept the proposal"
        );
        cluster.run(8);
    }
    cluster.run(20); // drain replication

    // Every node's reconstructed store must agree, and reflect the final state.
    let ids = cluster.node_ids();
    let reference = store_for(&cluster, ids[0]);
    for &id in &ids {
        assert_eq!(store_for(&cluster, id), reference, "node {id} diverged");
    }

    // Final semantics: alpha was overwritten, beta was deleted.
    assert_eq!(reference.get("alpha"), Some(&"updated".to_string()));
    assert_eq!(reference.get("beta"), None);

    cluster
        .check_applied_consistency()
        .expect("no divergent committed entries");
}

#[test]
fn committed_entries_survive_leader_partition() {
    let mut cluster = Cluster::new(3);
    run_until_leader(&mut cluster, 50);

    // Commit an entry cluster-wide.
    assert!(
        cluster.propose(
            Command::Set {
                key: "durable".into(),
                value: "yes".into()
            }
            .encode()
        )
    );
    cluster.run(15);

    // Partition the leader; the majority must continue and re-elect.
    let old_leader = cluster.leader().expect("a leader before partition");
    cluster.partition(&[old_leader]);
    cluster.run(40);

    // Propose more on the majority side.
    assert!(
        cluster.propose(
            Command::Set {
                key: "after".into(),
                value: "heal".into()
            }
            .encode()
        )
    );
    cluster.run(20);

    cluster.heal();
    cluster.run(40);

    // Safety holds, and the originally-committed entry is intact everywhere.
    cluster
        .check_applied_consistency()
        .expect("no divergence across partition/heal");
    for id in cluster.node_ids() {
        let store = store_for(&cluster, id);
        assert_eq!(
            store.get("durable"),
            Some(&"yes".to_string()),
            "node {id} lost a committed entry"
        );
    }
}
