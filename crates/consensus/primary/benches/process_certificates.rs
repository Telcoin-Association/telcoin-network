// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fastcrypto::hash::Hash;
use narwhal_primary::consensus::{
    Bullshark, ConsensusMetrics, ConsensusState, LeaderSchedule, LeaderSwapTable,
};
use narwhal_storage::NodeStorage;
use narwhal_typed_store::{open_db, RawDatabaseType};
use std::{collections::BTreeSet, sync::Arc};
use tn_types::{
    encode,
    test_utils::{make_optimal_certificates, temp_dir, CommitteeFixture},
    Certificate, Round, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use tokio::time::Instant;

pub fn process_certificates(c: &mut Criterion) {
    let mut consensus_group = c.benchmark_group("processing certificates");
    consensus_group.sampling_mode(SamplingMode::Flat);

    static BATCH_SIZES: [u64; 4] = [100, 500, 1000, 5000];

    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let keys: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    for size in &BATCH_SIZES {
        let gc_depth = 12;
        let rounds: Round = *size;

        // process certificates for rounds, check we don't grow the dag too much
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let (certificates, _next_parents) =
            make_optimal_certificates(&committee, 1..=rounds, &genesis, &keys);

        let store_path = temp_dir();
        // In case the DB dir does not yet exist.
        let _ = std::fs::create_dir_all(&store_path);
        let db = open_db(store_path);
        let store = NodeStorage::reopen(db, Option::<RawDatabaseType>::None);
        let metrics = Arc::new(ConsensusMetrics::default());

        let mut state = ConsensusState::new(metrics.clone(), gc_depth);

        let data_size: usize = certificates.iter().map(|cert| encode(&cert).len()).sum();
        consensus_group.throughput(Throughput::Bytes(data_size as u64));

        let mut ordering_engine = Bullshark {
            committee: committee.clone(),
            store: store.consensus_store,
            metrics,
            last_successful_leader_election_timestamp: Instant::now(),
            max_inserted_certificate_round: 0,
            num_sub_dags_per_schedule: 100,
            leader_schedule: LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            bad_nodes_stake_threshold: DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        };
        consensus_group.bench_with_input(
            BenchmarkId::new("batched", certificates.len()),
            &certificates,
            |b, i| {
                b.iter(|| {
                    for cert in i {
                        let _ = ordering_engine.process_certificate(&mut state, cert.clone());
                    }
                })
            },
        );
    }
}

criterion_group! {
    name = consensus_group;
    config = Criterion::default();
    targets = process_certificates
}
criterion_main!(consensus_group);
