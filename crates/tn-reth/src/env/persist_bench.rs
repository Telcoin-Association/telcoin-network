//! On-demand benchmark of TN's per-block persist on the reth database: the time of
//! [`RethEnv::persist_executed_output`] on this build's engine, MDBX or tndb-b.
//!
//! Run it once per build (it is `#[ignore]`d out of the default suite), each build in its own
//! target directory so neither rebuild overwrites the other's binary:
//!
//! ```text
//! mkdir -p target/persist-bench-tmp
//! TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-mdbx \
//!     cargo test --release -p tn-reth --lib env::persist_bench::per_block_persist -- \
//!     --ignored --exact --nocapture
//! TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-tndb \
//!     cargo test --release -p tn-reth --features tndb-b --lib \
//!     env::persist_bench::per_block_persist -- --ignored --exact --nocapture
//! ```
//!
//! **What it measures.** A chain on production `--db.*` settings (`DatabaseArgs::default()` with
//! the chosen sync mode), opened through [`RethEnv::new_database`] and [`RethEnv::new`] as the
//! node opens it. Its genesis funds `SENDERS` senders and places a storage-writer contract
//! (`STORE_CODE`: `SSTORE(calldata[0], calldata[32]); STOP`) with `GenesisAccount::with_code`,
//! and it prefills state so the tries have depth. Each block is a mix of four kinds of
//! transaction in equal shares: transfers to new accounts, transfers to prefilled accounts,
//! contract calls writing new slots, and contract calls overwriting existing slots (slot keys are
//! hashes, as a mapping's are). Blocks are built with
//! [`RethEnv::build_block_from_batch_payload`] and advance the in-memory canonical state as the
//! engine's `execute_payload` does: one [`OutputTrieOverlay`] per commit, shared by its blocks.
//! Only [`RethEnv::persist_executed_output`] is timed (`save_blocks`, the finalized/safe markers
//! and the commit); building, signing and `finalize_block` run outside the timer.
//!
//! Two modes run, each on a fresh datadir after `WARMUP_COMMITS` untimed commits: one block per
//! commit, and `TN_PERSIST_BENCH_PER_COMMIT` blocks per commit, as the node persists one
//! consensus output holding one block per batch. Each mode prints one line,
//!
//! ```text
//! persist_bench backend=tndb-b sync=nosync per_commit=1 n=400 blocks=400 txs=200 median_us=..
//! ```
//!
//! with the per-commit median, p90, p99 (nearest rank) and max, the total, blocks per second of
//! persist time, and the database file's length and allocated bytes at the end (`mdbx.dat` or
//! `tndb.dat`). The `db_*` and `sf_*` fields are reth's own split of each persist, from the
//! timings `DatabaseProvider` records in its metrics: `db_write` is the database writes of
//! `save_blocks` (the static-file writes, `sf_write`, run beside them on another thread),
//! `sf_commit` the static files' commit and sync, and `db_commit` the database commit after it;
//! `db` is `db_write + db_commit`, the engine's part. The run goes under a local metrics recorder
//! to read them, as a node started with `--metrics` runs under its own. After each mode the bench
//! reads the last block number, a slot written by the last block and an account created by it
//! back from the database tables.
//!
//! **Knobs** (environment variables):
//!
//! - `TN_PERSIST_BENCH_BLOCKS` (400): timed blocks per mode; the per-commit mode times `BLOCKS /
//!   PER_COMMIT` commits.
//! - `TN_PERSIST_BENCH_TXS` (200): transactions per block, a multiple of 4.
//! - `TN_PERSIST_BENCH_PER_COMMIT` (4): blocks per commit in the second mode.
//! - `TN_PERSIST_BENCH_ACCOUNTS` (10000): prefilled accounts, and as many prefilled slots in the
//!   writer contract.
//! - `TN_PERSIST_BENCH_SYNC` (`nosync`): `nosync` or `durable` (`--db.sync-mode durable`). On MDBX
//!   `nosync` is `--db.sync-mode safe-no-sync`. tndb-b refuses that flag (`tndb_args`: its `NoSync`
//!   reuses freed pages only after a sync, so the file grows until one), so the tndb build opens
//!   its database with the engine's `SyncMode::NoSync` through the test-only
//!   `database::TEST_NO_SYNC`, on otherwise default `--db.*` settings.
//!
//! **Caveats.**
//!
//! - On macOS reth's bundled libmdbx makes each durable commit with `fcntl(F_FULLFSYNC)`, which
//!   flushes the drive's cache, while tndb-b syncs with a plain `fsync`, which does not. A durable
//!   run on macOS therefore compares unequal syncs; judge the engines there by `nosync`, where
//!   neither waits for the disk, and leave the durable verdict to Linux.
//! - reth's static-file writes (`save_blocks` appends, and the commit syncs them with
//!   `File::sync_all` whatever the database's sync mode) are the same code in both builds and add
//!   the same cost to both, so they narrow the ratio between the engines.
//! - The run prints its temp dir (on Linux with its file system) and stops if that is a tmpfs or
//!   ramfs, where syncs cost nothing: set `TMPDIR` to a directory on a real disk, for the bench
//!   command only.

use super::RethEnv;
use crate::{
    init_reth_defaults,
    payload::TNPayload,
    rpc_server_args::RpcServerArgs,
    test_utils::{
        consensus_output_for_tests_at,
        load::{announce_temp_dir, db_file_bytes},
        TransactionFactory,
    },
    ExecutedBlock, NewCanonicalChain, OutputTrieOverlay, RethChainSpec, RethCommand, RethConfig,
};
use alloy::primitives::keccak256;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use reth_db::{
    cursor::DbDupCursorRO as _,
    mdbx::SyncMode,
    tables::{PlainAccountState, PlainStorageState},
    transaction::DbTx as _,
};
use reth_node_core::args::{DatabaseArgs, TxPoolArgs};
use reth_provider::{BlockNumReader as _, DatabaseProviderFactory as _};
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tn_types::{
    gas_accumulator::GasAccumulator, test_genesis, Address, Bytes, GenesisAccount, SealedHeader,
    TaskManager, B256, MIN_PROTOCOL_BASE_FEE, U256,
};

/// The storage-writer contract: `SSTORE(calldataload(0), calldataload(32)); STOP`.
const STORE_CODE: &[u8] = &[
    0x60, 0x20, 0x35, // PUSH1 32 CALLDATALOAD: the value
    0x60, 0x00, 0x35, // PUSH1 0 CALLDATALOAD: the key
    0x55, // SSTORE
    0x00, // STOP
];

/// Where genesis places the storage-writer contract.
const STORE_ADDRESS: Address = Address::repeat_byte(0x5e);

/// Funded senders; a block's transactions take them in turn.
const SENDERS: u64 = 16;

/// Untimed commits before each mode's timed ones.
const WARMUP_COMMITS: usize = 8;

/// Gas limit of a transfer.
const TRANSFER_GAS: u64 = 21_000;

/// Gas limit of a storage-writer call (a new slot costs about 45k).
const STORE_GAS: u64 = 100_000;

/// Wei sent by each transfer.
const TRANSFER_VALUE: u64 = 1_000;

/// The bench's settings, from the environment.
struct Knobs {
    blocks: usize,
    txs: usize,
    per_commit: usize,
    accounts: usize,
    durable: bool,
}

impl Knobs {
    fn from_env() -> Self {
        let txs = knob("TN_PERSIST_BENCH_TXS", 200);
        assert!(txs.is_multiple_of(4), "TN_PERSIST_BENCH_TXS must be a multiple of 4, got {txs}");
        let durable = match std::env::var("TN_PERSIST_BENCH_SYNC").as_deref() {
            Err(_) | Ok("nosync") => false,
            Ok("durable") => true,
            Ok(other) => panic!("TN_PERSIST_BENCH_SYNC must be durable or nosync, got {other}"),
        };
        Self {
            blocks: knob("TN_PERSIST_BENCH_BLOCKS", 400),
            txs,
            per_commit: knob("TN_PERSIST_BENCH_PER_COMMIT", 4),
            accounts: knob("TN_PERSIST_BENCH_ACCOUNTS", 10_000),
            durable,
        }
    }

    fn sync_name(&self) -> &'static str {
        if self.durable {
            "durable"
        } else {
            "nosync"
        }
    }
}

/// A positive integer knob, or `default` when the variable is unset.
fn knob(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| {
        let value = v.parse().unwrap_or_else(|_| panic!("{name} must be a number, got {v}"));
        assert!(value > 0, "{name} must be positive");
        value
    })
}

/// A splitmix64 generator: the workload's slots, recipients and values from a fixed seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A non-zero storage value.
    fn value(&mut self) -> U256 {
        U256::from_limbs([self.next() | 1, self.next(), self.next(), self.next()])
    }
}

/// The `i`th account of the workload: prefilled below `Knobs::accounts`, created above it.
fn account(i: u64) -> Address {
    let hash = keccak256([b"persist-bench-account".as_slice(), &i.to_be_bytes()].concat());
    Address::from_slice(&hash[12..])
}

/// The `i`th storage slot of the writer contract, hashed as a mapping's slot is.
fn slot(i: u64) -> B256 {
    keccak256([b"persist-bench-slot".as_slice(), &i.to_be_bytes()].concat())
}

/// The genesis senders, from fixed seeds.
fn senders() -> Vec<TransactionFactory> {
    (0..SENDERS)
        .map(|i| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(1_000 + i)))
        .collect()
}

/// The bench chain: the test genesis plus funded senders, prefilled accounts and the writer
/// contract with `accounts` prefilled slots.
fn chain(accounts: usize) -> Arc<RethChainSpec> {
    let sender_balance = U256::from(10).pow(U256::from(30));
    let mut alloc: Vec<(Address, GenesisAccount)> = senders()
        .iter()
        .map(|s| (s.address(), GenesisAccount::default().with_balance(sender_balance)))
        .collect();
    alloc.extend(
        (0..accounts as u64)
            .map(|i| (account(i), GenesisAccount::default().with_balance(U256::from(1)))),
    );
    let storage = (0..accounts as u64).map(|i| (slot(i), B256::from(U256::from(i + 1)))).collect();
    alloc.push((
        STORE_ADDRESS,
        GenesisAccount::default()
            .with_code(Some(Bytes::from_static(STORE_CODE)))
            .with_storage(Some(storage)),
    ));
    Arc::new(test_genesis().extend_accounts(alloc).into())
}

/// One block's transactions and what it leaves to read back.
struct BlockTxs {
    txs: Vec<Vec<u8>>,
    /// The last new slot the block writes, and its value.
    new_slot: (B256, U256),
    /// The last account the block creates.
    new_account: Address,
}

/// Generates each block's transactions: equal shares of transfers to new accounts, transfers to
/// prefilled accounts, writes of new slots and overwrites of existing slots.
struct Workload {
    chain: Arc<RethChainSpec>,
    senders: Vec<TransactionFactory>,
    next_sender: usize,
    rng: Rng,
    prefilled: u64,
    next_account: u64,
    next_slot: u64,
}

impl Workload {
    fn new(chain: Arc<RethChainSpec>, accounts: usize) -> Self {
        Self {
            chain,
            senders: senders(),
            next_sender: 0,
            rng: Rng(0x7e1c_0de5_eed0_0001),
            prefilled: accounts as u64,
            next_account: accounts as u64,
            next_slot: accounts as u64,
        }
    }

    fn tx(&mut self, to: Address, gas: u64, value: U256, input: Bytes) -> Vec<u8> {
        let next = (self.next_sender + 1) % self.senders.len();
        let sender = &mut self.senders[std::mem::replace(&mut self.next_sender, next)];
        sender.create_eip1559_encoded(
            self.chain.clone(),
            Some(gas),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(to),
            value,
            input,
        )
    }

    fn store(&mut self, key: B256, value: U256) -> Vec<u8> {
        let input = Bytes::from([key.as_slice(), &value.to_be_bytes::<32>()].concat());
        self.tx(STORE_ADDRESS, STORE_GAS, U256::ZERO, input)
    }

    fn block(&mut self, txs: usize) -> BlockTxs {
        let quarter = txs / 4;
        let mut out = Vec::with_capacity(txs);
        let mut new_slot = (B256::ZERO, U256::ZERO);
        let mut new_account = Address::ZERO;
        for _ in 0..quarter {
            new_account = account(self.next_account);
            self.next_account += 1;
            out.push(self.tx(new_account, TRANSFER_GAS, U256::from(TRANSFER_VALUE), Bytes::new()));

            let existing = account(self.rng.below(self.prefilled));
            out.push(self.tx(existing, TRANSFER_GAS, U256::from(TRANSFER_VALUE), Bytes::new()));

            let key = slot(self.next_slot);
            self.next_slot += 1;
            let value = self.rng.value();
            new_slot = (key, value);
            out.push(self.store(key, value));

            let key = slot(self.rng.below(self.prefilled));
            let value = self.rng.value();
            out.push(self.store(key, value));
        }
        BlockTxs { txs: out, new_slot, new_account }
    }
}

/// reth's own split of one `persist_executed_output`, from the timings `DatabaseProvider` records
/// in its metrics (reth names the database side `mdbx` whatever the engine).
#[derive(Clone, Copy, Default)]
struct Split {
    /// The database writes of `save_blocks`, while the static-file writes run beside them.
    db_write: Duration,
    /// The database transaction's commit.
    db_commit: Duration,
    /// The static-file writes of `save_blocks`, on their own thread.
    sf_write: Duration,
    /// The static files' commit (`finalize`, which syncs them), before the database commit.
    sf_commit: Duration,
}

impl Split {
    /// The split one persist recorded, read (and cleared) from `snapshotter`.
    fn take(snapshotter: &Snapshotter) -> Self {
        let snapshot = snapshotter.snapshot().into_vec();
        let seconds = |name: &str| {
            let name = format!("storage.providers.database.{name}");
            let values = snapshot
                .iter()
                .find_map(|(key, _, _, value)| match value {
                    DebugValue::Histogram(values) if key.key().name() == name => Some(values),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("reth recorded no {name}"));
            assert_eq!(values.len(), 1, "one {name} per persist");
            Duration::from_secs_f64(values[0].into_inner())
        };
        Self {
            db_write: seconds("save_blocks_mdbx"),
            db_commit: seconds("save_blocks_commit_mdbx"),
            sf_write: seconds("save_blocks_sf"),
            sf_commit: seconds("save_blocks_commit_sf"),
        }
    }

    /// The engine's part of the persist: its writes and its commit.
    fn db(self) -> Duration {
        self.db_write + self.db_commit
    }
}

/// One mode's results.
struct ModeRun {
    commits: Vec<Duration>,
    splits: Vec<Split>,
    db_bytes: u64,
    db_alloc_bytes: u64,
}

/// The value at nearest rank `p` (0..=1) of `sorted`.
fn rank(sorted: &[Duration], p: f64) -> Duration {
    let index = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[index]
}

/// Open a chain on production `--db.*` settings in `datadir`, persist `WARMUP_COMMITS` and then
/// `knobs.blocks` blocks in commits of `per_commit`, timing each `persist_executed_output`, and
/// read the last block back.
///
/// `snapshotter` reads the recorder the caller runs this under, the one reth's provider metrics
/// bound to.
fn run_mode(
    knobs: &Knobs,
    per_commit: usize,
    datadir: &Path,
    snapshotter: &Snapshotter,
) -> ModeRun {
    init_reth_defaults();
    let chain = chain(knobs.accounts);
    #[cfg(not(feature = "tndb-b"))]
    let sync_mode = Some(if knobs.durable { SyncMode::Durable } else { SyncMode::SafeNoSync });
    // The tndb build refuses `--db.sync-mode safe-no-sync`, so `nosync` reaches the engine's
    // `NoSync` through the test-only `TEST_NO_SYNC`, read by the open on this thread.
    #[cfg(feature = "tndb-b")]
    let sync_mode = {
        super::database::TEST_NO_SYNC.set(!knobs.durable);
        knobs.durable.then_some(SyncMode::Durable)
    };
    let command = RethCommand {
        rpc: RpcServerArgs { ipcdisable: true, ..Default::default() },
        txpool: TxPoolArgs::default(),
        db: DatabaseArgs { sync_mode, ..Default::default() },
    };
    let reth_config = RethConfig::new(command, None, datadir, false, chain.clone());
    let db_dir = datadir.join("db");
    let task_manager = TaskManager::new("persist bench");
    let database = RethEnv::new_database(&reth_config, &db_dir).expect("open the database");
    #[cfg(feature = "tndb-b")]
    super::database::TEST_NO_SYNC.set(false);
    let reth_env =
        RethEnv::new(&reth_config, &task_manager, database, None, GasAccumulator::default())
            .expect("open the reth env");
    // drop what opening recorded (the genesis commit), so each read below holds one persist
    snapshotter.snapshot();

    let mut workload = Workload::new(chain, knobs.accounts);
    let mut parent = reth_env.canonical_tip();
    let mut subdag_index = 0u64;
    let beneficiary = Address::repeat_byte(0xbe);
    let timed = knobs.blocks / per_commit;
    let mut commits = Vec::with_capacity(timed);
    let mut splits = Vec::with_capacity(timed);
    let mut last = None;

    for commit in 0..WARMUP_COMMITS + timed {
        let cims = reth_env.canonical_in_memory_state();
        let mut overlay = OutputTrieOverlay::new();
        let mut blocks: Vec<ExecutedBlock> = Vec::with_capacity(per_commit);
        for _ in 0..per_commit {
            subdag_index += 1;
            let output = consensus_output_for_tests_at(
                subdag_index as u32,
                0,
                subdag_index,
                false,
                parent.timestamp + 1,
            );
            let payload = TNPayload::new_for_test_with(
                parent.clone(),
                &output,
                beneficiary,
                B256::from(U256::from(subdag_index)),
                B256::ZERO,
            );
            let block_txs = workload.block(knobs.txs);
            let block = reth_env
                .build_block_from_batch_payload(payload, &block_txs.txs, &mut overlay)
                .expect("build block");
            let receipts = &block.execution_output.result.receipts;
            assert_eq!(receipts.len(), knobs.txs, "every transaction is in the block");
            assert!(receipts.iter().all(|r| r.success), "every transaction succeeds");
            parent = block.recovered_block.clone_sealed_header();
            cims.set_pending_block(block.clone());
            cims.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
            cims.set_canonical_head(parent.clone());
            blocks.push(block);
            last = Some((parent.number, block_txs));
        }

        let start = Instant::now();
        reth_env.persist_executed_output(&blocks).expect("persist");
        let elapsed = start.elapsed();
        let split = Split::take(snapshotter);

        if commit >= WARMUP_COMMITS {
            commits.push(elapsed);
            splits.push(split);
        }
        reth_env.finalize_block(parent.clone()).expect("finalize");
    }

    let (last_number, last_txs) = last.expect("the run persisted blocks");
    read_back(&reth_env, &parent, last_number, &last_txs);
    drop(reth_env);

    let (db_bytes, db_alloc_bytes) = db_file_bytes(&db_dir).expect("database file");
    ModeRun { commits, splits, db_bytes, db_alloc_bytes }
}

/// Assert the last persisted block is in the database tables: the last block number, the last
/// new slot it wrote and the last account it created.
fn read_back(reth_env: &RethEnv, tip: &SealedHeader, last_number: u64, last: &BlockTxs) {
    assert_eq!(tip.number, last_number);
    let provider = reth_env.blockchain_provider().database_provider_ro().expect("provider");
    assert_eq!(provider.last_block_number().expect("last block"), last_number);
    let (key, value) = last.new_slot;
    let entry = provider
        .tx_ref()
        .cursor_dup_read::<PlainStorageState>()
        .expect("storage cursor")
        .seek_by_key_subkey(STORE_ADDRESS, key)
        .expect("storage seek")
        .expect("the last new slot is persisted");
    assert_eq!((entry.key, entry.value), (key, value), "the last new slot holds its value");
    let account = provider
        .tx_ref()
        .get::<PlainAccountState>(last.new_account)
        .expect("account read")
        .expect("the last new account is persisted");
    assert_eq!(account.balance, U256::from(TRANSFER_VALUE));
}

/// `durations` sorted.
fn sorted(durations: impl Iterator<Item = Duration>) -> Vec<Duration> {
    let mut sorted: Vec<_> = durations.collect();
    sorted.sort_unstable();
    sorted
}

/// Print one parseable line for a mode.
fn report(knobs: &Knobs, per_commit: usize, run: &ModeRun) {
    let backend = if cfg!(feature = "tndb-b") { "tndb-b" } else { "mdbx" };
    let persist = sorted(run.commits.iter().copied());
    let db = sorted(run.splits.iter().map(|s| s.db()));
    let db_write = sorted(run.splits.iter().map(|s| s.db_write));
    let db_commit = sorted(run.splits.iter().map(|s| s.db_commit));
    let sf_write = sorted(run.splits.iter().map(|s| s.sf_write));
    let sf_commit = sorted(run.splits.iter().map(|s| s.sf_commit));
    let total: Duration = persist.iter().sum();
    let us = |d: Duration| d.as_micros();
    let blocks = persist.len() * per_commit;
    println!(
        "persist_bench backend={backend} sync={} per_commit={per_commit} n={} blocks={blocks} \
         txs={} median_us={} p90_us={} p99_us={} max_us={} mean_us={} total_ms={} \
         blocks_per_s={:.1} db_median_us={} db_p99_us={} db_write_median_us={} \
         db_commit_median_us={} db_commit_p99_us={} sf_write_median_us={} sf_commit_median_us={} \
         sf_commit_p99_us={} db_bytes={} db_alloc_bytes={}",
        knobs.sync_name(),
        persist.len(),
        knobs.txs,
        us(rank(&persist, 0.5)),
        us(rank(&persist, 0.9)),
        us(rank(&persist, 0.99)),
        us(persist[persist.len() - 1]),
        us(total / persist.len() as u32),
        total.as_millis(),
        blocks as f64 / total.as_secs_f64(),
        us(rank(&db, 0.5)),
        us(rank(&db, 0.99)),
        us(rank(&db_write, 0.5)),
        us(rank(&db_commit, 0.5)),
        us(rank(&db_commit, 0.99)),
        us(rank(&sf_write, 0.5)),
        us(rank(&sf_commit, 0.5)),
        us(rank(&sf_commit, 0.99)),
        run.db_bytes,
        run.db_alloc_bytes,
    );
}

/// TN's per-block persist on this build's engine: one block per commit, then
/// `TN_PERSIST_BENCH_PER_COMMIT` blocks per commit.
#[test]
#[ignore = "on-demand performance measurement; run with --release --ignored --exact --nocapture"]
fn per_block_persist() {
    let knobs = Knobs::from_env();
    assert!(knobs.blocks >= knobs.per_commit, "TN_PERSIST_BENCH_BLOCKS is below PER_COMMIT");
    let tmp = tempfile::tempdir().expect("temp dir");
    announce_temp_dir(tmp.path());
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let _guard = runtime.enter();
    let mut modes = vec![1];
    if knobs.per_commit > 1 {
        modes.push(knobs.per_commit);
    }
    // reth's provider metrics register once per process (a static `OnceLock`), with the recorder
    // current at the first registration, so the whole run goes under one local recorder, as a
    // node started with `--metrics` runs under its own.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        for per_commit in modes {
            let dir = tmp.path().join(format!("per-commit-{per_commit}"));
            let run = run_mode(&knobs, per_commit, &dir, &snapshotter);
            report(&knobs, per_commit, &run);
        }
    });
}
