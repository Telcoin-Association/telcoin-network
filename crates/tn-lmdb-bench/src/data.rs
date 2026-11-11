//! The seeded inputs of the generic rows, and the generators every row shares. Every stream has a
//! fixed seed, so two runs (and two revisions of the engine) replay the same keys, values and
//! orders. The generators follow `crates/tn-lmdb-reth/src/engine_bench.rs`, so a row here does what
//! the comparison bench's row of the same workload does.

use std::collections::VecDeque;

/// xorshift64: the generator of every seeded stream (the one the engine's tests use).
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    /// The next value.
    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A value in `0..n`.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A value in `lo..=hi`.
    pub(crate) fn within(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below((hi - lo + 1) as u64) as usize
    }

    /// `N` random bytes.
    pub(crate) fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes()[..chunk.len()]);
        }
        out
    }
}

/// Append `len` bytes from `rng` to `out`.
pub(crate) fn random_bytes(rng: &mut Rng, out: &mut Vec<u8>, len: usize) {
    let start = out.len();
    out.resize(start + len, 0);
    for chunk in out[start..].chunks_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes()[..chunk.len()]);
    }
}

/// A seeded random permutation of `0..n`.
pub(crate) fn shuffled(rng: &mut Rng, n: usize) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n as u32).collect();
    for i in (1..n).rev() {
        v.swap(i, rng.below(i as u64 + 1) as usize);
    }
    v
}

/// The keys a delete of 90% of `count` keys removes.
pub(crate) const fn deleted(count: usize) -> usize {
    count - count / 10
}

/// Value length of the generic rows' entries.
pub(crate) const VALUE_LEN: usize = 100;
/// Distinct values cycled through (picking one costs nothing in a timed region).
const VALUE_POOL: usize = 4096;
/// Entries each seek of `seek.range` reads (the seek, then `SEEK_RANGE - 1` steps).
pub(crate) const SEEK_RANGE: usize = 10;
/// Puts in each transaction of `commit.small`.
pub(crate) const SMALL_PUTS: usize = 3;
/// Operations per transaction of `mixed`.
pub(crate) const MIXED_BATCH: usize = 1000;
/// Rounds of fill and delete in `churn`.
pub(crate) const CHURN_ROUNDS: usize = 10;
/// The large-value rows' value lengths and their entry counts at full size (`n` of 1M).
pub(crate) const BIG: [(usize, usize); 3] =
    [(2 << 10, 20_000), (8 << 10, 10_000), (32 << 10, 4_000)];
/// The large values are windows of one buffer at this many offsets.
const BIG_SHIFTS: usize = 64;
/// Point gets in each read transaction of `ro.txn` and `ro.txn.mt`.
pub(crate) const TXN_GETS: usize = 2;
/// Threads of `ro.txn.mt`. Fixed, not the machine's core count, so runs on two machines measure
/// the same workload; four stay on performance cores with room to spare on any machine the
/// bench targets (an M1 Max has 8, in two clusters of 4), so no thread lands on an efficiency
/// core or waits for the main thread or the system.
pub(crate) const TXN_THREADS: usize = 4;

/// How many of each workload's operations a run of `n` takes: the bulk tables hold `n` entries,
/// and the other counts follow `n` up to the comparison bench's fixed counts.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sizes {
    /// Entries of each bulk table.
    pub(crate) n: usize,
    /// Seeks of `seek.range` and `seek.range.sorted`.
    pub(crate) seeks: usize,
    /// Transactions of `commit.small`.
    pub(crate) small_txns: usize,
    /// Operations of `mixed`.
    pub(crate) mixed_ops: usize,
    /// Keys of the `churn` table when full.
    pub(crate) churn: usize,
    /// Entries of each large-value row, in [`BIG`]'s order.
    pub(crate) big: [usize; 3],
    /// Entries of the table of `ro.txn` and `ro.txn.mt`.
    pub(crate) txn_keys: usize,
    /// Read transactions of `ro.txn`, and of each thread of `ro.txn.mt`.
    pub(crate) txns: usize,
}

impl Sizes {
    /// The counts of a run of `n` entries a bulk table.
    pub(crate) fn new(n: usize) -> Self {
        Self {
            n,
            seeks: n.min(100_000),
            small_txns: (n / 50).clamp(20, 2000),
            mixed_ops: n.min(100_000),
            churn: n / 4,
            // A quarter of the full counts at the quick tier's 100k, the full counts from 400k.
            big: BIG.map(|(_, full)| (full * n / 400_000).clamp(16, full)),
            // The full counts from the quick tier's 100k on.
            txn_keys: (n / 10).clamp(16, 10_000),
            txns: (n / 5).clamp(16, 20_000),
        }
    }
}

/// The generic rows' seeded inputs.
pub(crate) struct Data {
    /// The counts.
    pub(crate) sizes: Sizes,
    /// The random table's keys, in insertion order.
    pub(crate) rand_keys: Vec<[u8; 32]>,
    /// Keys absent from the random table (the first `seeks` are also the seek targets).
    pub(crate) miss_keys: Vec<[u8; 32]>,
    /// The seek targets sorted ascending (`seek.range.sorted`).
    pub(crate) sorted_seeks: Vec<[u8; 32]>,
    /// The values, picked by index modulo the pool.
    values: Vec<[u8; VALUE_LEN]>,
    /// The large-value rows' keys: `i` scrambled by a bijection of `u64`, so their order is not
    /// the insertion order.
    pub(crate) big_keys: Vec<[u8; 8]>,
    /// The large values: windows of this random buffer.
    big_buf: Vec<u8>,
    /// `delete.rand90`'s order: a permutation of `0..n` (indexes into `rand_keys`), whose first
    /// `deleted(n)` are deleted.
    pub(crate) del_order: Vec<u32>,
    /// `mixed`'s operations: (index into `rand_keys`, delete?).
    pub(crate) mixed: Vec<(usize, bool)>,
    /// `churn`'s rounds' delete orders: `CHURN_ROUNDS` permutations of `0..churn`, one after the
    /// other (see [`Data::churn_order`]).
    churn_orders: Vec<u32>,
    /// The gets of `ro.txn` and `ro.txn.mt`: indexes into the first `txn_keys` of `rand_keys`,
    /// `TXN_GETS` a transaction, `txns` transactions for each of `TXN_THREADS` threads, one
    /// after the other (see [`Data::txn_gets`]).
    txn_gets: Vec<u32>,
}

/// `mixed`'s `ops` operations over the random table's `n` keys: (index into `rand_keys`,
/// delete?). Half are deletes of a random present key; each put re-inserts the key deleted
/// longest ago (or updates a random key while none is deleted), so the table stays within a few
/// hundred entries of `n` and almost every put is an insert.
fn mixed_half(rng: &mut Rng, n: usize, ops: usize) -> Vec<(usize, bool)> {
    let mut present = vec![true; n];
    let mut deleted = VecDeque::new();
    (0..ops)
        .map(|_| {
            if rng.below(2) == 0 && deleted.len() < n {
                let idx = loop {
                    let i = rng.below(n as u64) as usize;
                    if present[i] {
                        break i;
                    }
                };
                present[idx] = false;
                deleted.push_back(idx);
                (idx, true)
            } else if let Some(idx) = deleted.pop_front() {
                present[idx] = true;
                (idx, false)
            } else {
                (rng.below(n as u64) as usize, false)
            }
        })
        .collect()
}

impl Data {
    /// The inputs of a run of `n` entries a bulk table.
    pub(crate) fn new(n: usize) -> Self {
        let sizes = Sizes::new(n);
        let mut rng = Rng(0x7d1b_b0b5_eed0_0001);
        let rand_keys: Vec<[u8; 32]> = (0..n).map(|_| rng.array()).collect();
        let miss_keys: Vec<[u8; 32]> = (0..n).map(|_| rng.array()).collect();
        let mut sorted_seeks = miss_keys[..sizes.seeks].to_vec();
        sorted_seeks.sort_unstable();
        let mut rng = Rng(0x5eed_0000_0000_0002);
        let values = (0..VALUE_POOL)
            .map(|_| {
                let mut v = [0u8; VALUE_LEN];
                for b in v.iter_mut() {
                    *b = rng.next() as u8;
                }
                v
            })
            .collect();
        let most = sizes.big.iter().copied().max().unwrap_or(0) as u64;
        let big_keys =
            (0..most).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes()).collect();
        let big_buf = (0..BIG[2].0 + BIG_SHIFTS).map(|_| rng.next() as u8).collect();
        let mut rng = Rng(0x5eed_0000_0000_0003);
        let del_order = shuffled(&mut rng, n);
        let mixed = mixed_half(&mut rng, n, sizes.mixed_ops);
        let churn_orders =
            (0..CHURN_ROUNDS).flat_map(|_| shuffled(&mut rng, sizes.churn)).collect();
        let mut rng = Rng(0x5eed_0000_0000_0004);
        let txn_gets = (0..TXN_THREADS * sizes.txns * TXN_GETS)
            .map(|_| rng.below(sizes.txn_keys as u64) as u32)
            .collect();
        Self {
            sizes,
            rand_keys,
            miss_keys,
            sorted_seeks,
            values,
            big_keys,
            big_buf,
            del_order,
            mixed,
            churn_orders,
            txn_gets,
        }
    }

    /// Entry `i`'s value.
    pub(crate) fn value(&self, i: usize) -> &[u8] {
        &self.values[i % VALUE_POOL]
    }

    /// Churn round `r`'s order: a permutation of `0..churn` (indexes into `rand_keys`), whose
    /// first `deleted(churn)` the round deletes.
    pub(crate) fn churn_order(&self, r: usize) -> &[u32] {
        &self.churn_orders[r * self.sizes.churn..][..self.sizes.churn]
    }

    /// Thread `thread`'s gets in `ro.txn.mt` (thread 0's are `ro.txn`'s): `txns` transactions'
    /// `TXN_GETS` indexes into `rand_keys`, each below `txn_keys`.
    pub(crate) fn txn_gets(&self, thread: usize) -> &[u32] {
        let len = self.sizes.txns * TXN_GETS;
        &self.txn_gets[thread * len..][..len]
    }

    /// Entry `i`'s value in a large-value row of `len`-byte values.
    pub(crate) fn big_value(&self, i: usize, len: usize) -> &[u8] {
        let at = i % BIG_SHIFTS;
        &self.big_buf[at..at + len]
    }

    /// The `j`-th get of a large-value row of `count` entries: every key once, in an order that
    /// is neither the keys' nor the insertion order (a prime stride that does not divide `count`;
    /// no count up to [`BIG`]'s holds both primes).
    pub(crate) fn big_get_key(&self, j: usize, count: usize) -> &[u8; 8] {
        let stride = if count.is_multiple_of(7919) { 7907 } else { 7919 };
        &self.big_keys[j * stride % count]
    }
}
