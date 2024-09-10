// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};

use rand::rngs::ThreadRng;
use reth_primitives::{SealedHeader, TransactionSigned};
use tn_types::{test_utils::transaction_with_rand, WorkerBlock};

pub fn batch_digest(c: &mut Criterion) {
    let mut digest_group = c.benchmark_group("Batch digests");
    digest_group.sampling_mode(SamplingMode::Flat);

    static BATCH_SIZES: [usize; 4] = [100, 500, 1000, 5000];

    let mut rand = rand::thread_rng();
    for size in BATCH_SIZES {
        let prand = &mut rand;
        let tx_gen = |rand: &mut ThreadRng| {
            (0..512).map(move |_| transaction_with_rand(rand)).collect::<Vec<TransactionSigned>>()
        };
        let batch = WorkerBlock::new(
            (0..size).map(move |_| tx_gen(prand)).flatten().collect::<Vec<_>>(),
            SealedHeader::default(),
        );
        digest_group.throughput(Throughput::Bytes(512 * size as u64));
        digest_group.bench_with_input(BenchmarkId::new("batch digest", size), &batch, |b, i| {
            b.iter(|| i.digest())
        });
    }
}

criterion_group! {
    name = consensus_group;
    config = Criterion::default();
    targets = batch_digest
}
criterion_main!(consensus_group);
