//! Replication-throughput benchmark: how quickly a healthy cluster elects a
//! leader and replicates a batch of proposals to commitment across all nodes.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use raftcache_sim::Cluster;

/// Runs until a leader exists, up to `max` ticks.
fn run_until_leader(cluster: &mut Cluster, max: u64) {
    for _ in 0..max {
        if cluster.leader().is_some() {
            return;
        }
        cluster.tick();
    }
}

fn bench_replication(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicate_batch");

    for &proposals in &[10u64, 100, 500] {
        group.bench_with_input(
            BenchmarkId::from_parameter(proposals),
            &proposals,
            |b, &n| {
                b.iter(|| {
                    let mut cluster = Cluster::new(3);
                    run_until_leader(&mut cluster, 50);
                    for i in 0..n {
                        cluster.propose(i.to_le_bytes().to_vec());
                        cluster.run(4); // let each proposal replicate
                    }
                    cluster.run(20); // drain remaining replication
                    black_box(cluster.leader())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_replication);
criterion_main!(benches);
