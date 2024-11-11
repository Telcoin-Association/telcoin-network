// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy::primitives::FixedBytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fastcrypto::{hash::Hash, traits::KeyPair};
use rand::rngs::ThreadRng;
use reth_primitives::Address;
use std::collections::{BTreeMap, BTreeSet};
use tn_test_utils::make_optimal_signed_certificates;
use tn_types::{
    encode, BlsKeypair, Certificate, CommitteeBuilder, Multiaddr, NetworkKeypair, WorkerCache,
    WorkerIndex,
};

pub fn verify_certificates(c: &mut Criterion) {
    let mut bench_group = c.benchmark_group("verify_certificate");
    bench_group.sampling_mode(SamplingMode::Flat);

    static COMMITTEE_SIZES: [usize; 4] = [4, 10, 40, 100];
    for committee_size in COMMITTEE_SIZES {
        let mut committee_builder = CommitteeBuilder::new(0);
        let mut keys = Vec::with_capacity(committee_size);
        for id in 0..committee_size {
            let mut rng = ThreadRng::default();
            let bls_keypair = BlsKeypair::generate(&mut rng);
            let network_keypair = NetworkKeypair::generate(&mut rng);
            keys.push(((id as u16).into(), bls_keypair));
            committee_builder.add_authority(
                keys.last().expect("just added a key!").1.public().clone(),
                1,
                Multiaddr::empty(),
                Address::from_word(FixedBytes::default()),
                network_keypair.public().clone(),
                Multiaddr::empty().to_string(),
            );
        }
        let committee = committee_builder.build();

        // process certificates for rounds, check we don't grow the dag too much
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let (certificates, _next_parents) =
            make_optimal_signed_certificates(1..=1, &genesis, &committee, keys.as_slice());
        let certificate = certificates.front().unwrap().clone();

        let data_size: usize = encode(&certificate).len();
        bench_group.throughput(Throughput::Bytes(data_size as u64));

        bench_group.bench_with_input(
            BenchmarkId::new("with_committee_size", committee_size),
            &certificate,
            |b, cert| {
                let worker_cache = WorkerCache {
                    epoch: committee.epoch(),
                    workers: committee
                        .authorities()
                        .map(|a| (a.protocol_key().clone(), WorkerIndex(BTreeMap::new())))
                        .collect(),
                };
                b.iter(|| {
                    cert.clone().verify(&committee, &worker_cache).expect("Verification failed");
                })
            },
        );
    }
}

criterion_group! {
    name = verify_certificate;
    config = Criterion::default().sample_size(1000).noise_threshold(0.1);
    targets = verify_certificates
}
criterion_main!(verify_certificate);
