//! On-demand benchmarks: reth's database benches run on [`TndbEnv`] and on reth's MDBX
//! `DatabaseEnv`, side by side, with the same data.
//!
//! Run them with (they are `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1
//! ```
//!
//! The workloads are those of reth's `crates/storage/db/benches` (reth v1.11.3), written once,
//! generic over reth's `Database`, and run on both environments:
//!
//! - **`criterion.rs`, `measure_table_db`,** on each table reth's bench iterates
//!   (`CanonicalHeaders`, `HeaderNumbers`, `Headers`, `BlockBodyIndices`, `BlockOmmers`,
//!   `TransactionHashNumbers`, `Transactions`, `PlainAccountState`): `SeqWrite` (a fresh
//!   environment, one transaction, a write cursor's `append` of every row in key order, the
//!   commit), `RandomWrite` (a fresh environment, a write cursor's `insert` of the 10 rows of
//!   `RANDOM_INDEXES`, the commit), `SeqRead` (a read transaction, a cursor's `walk` from the first
//!   key over every row) and `RandomRead` (a read transaction, then for each of the 10 rows a new
//!   cursor and its `seek_exact`). The read rows run on a table filled by `tx.put` of every row in
//!   one transaction, as reth's `set_up_db` fills it.
//! - **`criterion.rs`, `measure_dupsort_db`,** on `PlainStorageState`: `SeqWrite` (a dup write
//!   cursor's `append_dup` of every row), `RandomWrite` (`tx.put` of the 10 rows), `SeqRead`
//!   (`walk_dup(None, Some(B256::ZERO))`: the first key's rows, as in reth's bench), and
//!   `RandomRead`, which reth's bench leaves commented out: for each of the 10 rows a new dup
//!   cursor and its `seek_by_key_subkey`.
//! - **`hash_keys.rs`:** `TransactionHashNumbers` with random `B256` keys, at each batch size N:
//!   `append_all` (2N presorted rows appended into an empty table), `append_input` (N),
//!   `insert_sorted` and `insert_unsorted` (a write cursor's `insert` of N rows into a table
//!   holding N others), `put_sorted` and `put_unsorted` (`tx.put` of the same), each one
//!   transaction and its commit. The table preload is untimed. reth sorts the input inside the
//!   timed region; here the sort runs before the timer, so the row times the database alone. Each
//!   scenario also reports the table's size afterwards (its pages times the page size, from the
//!   metrics), as reth's prints the table's stats.
//! - **`get.rs`:** `tx.get` and `tx.get_by_encoded_key` of `GET_KEYS` random `B256` keys in
//!   `TransactionHashNumbers` of an empty environment (reth's bench takes 10M keys; the time per
//!   get does not depend on the count): the cost of a lookup's path through the API.
//! - **`put.rs`:** `tx.put` and `tx.append` of `PUT_BLOCKS` rows `(i, B256::ZERO)` into
//!   `CanonicalHeaders` of a fresh environment, never committed: the transaction drops (aborts)
//!   inside the timed region, as in reth's bench.
//!
//! **The data** comes from a fixed seed, in the shape of reth's test vectors (its
//! `test-vectors tables` command): `PER_TABLE` rows per table (`PER_TABLE_SMALL` for
//! `BlockOmmers` and `Transactions`), unique keys, sorted; a dup-sort table holds keys of 100 to
//! 300 sorted values each, until it has `PER_TABLE` rows. Where reth draws arbitrary values, these
//! are realistic ones of realistic sizes: block and transaction numbers in mainnet's ranges,
//! post-Cancun headers (about 600 bytes compressed), 0 to 2 ommers, legacy and EIP-1559
//! transactions with 0 to 300 bytes of input, accounts with and without code, storage values of 1
//! to 32 bytes.
//!
//! **The timing** follows criterion's as reth configures it: one untimed warm-up sample per
//! environment, then samples until each environment has `TNDB_RETH_BENCH_REPEAT` of them and 200
//! ms of measured time (past that minimum, at most `MAX_SAMPLES`, and no more once the row has run
//! `ROW_WALL` with its untimed setup); a cell is the median sample divided by the row's
//! operations. The two environments alternate within a row (A B, then B A), so neither gains from
//! running second. A write row's sample makes a fresh environment, untimed; its time runs from
//! `tx_mut` through the commit. tndb opens with [`TndbArgs::default`] (4 KiB pages,
//! [`SyncMode::Durable`](crate::SyncMode::Durable)); MDBX as reth's benches open it
//! (`create_test_rw_db_with_path`: `init_db` with `DatabaseArguments::test()`, a `write_map`
//! environment syncing `Durable`, with the OS page size), its size limit raised from 64 MiB so the
//! larger batches fit.
//!
//! ## Selecting what runs
//!
//! - `TNDB_RETH_BENCH_N`: the `hash_keys` batch sizes, comma-separated (default `10000,100000`;
//!   reth's are `10000,100000,1000000`).
//! - `TNDB_RETH_BENCH_ROWS`: comma-separated; a row runs when one of them names its group (`table`,
//!   `dupsort`, `hash`, `get`, `put`) or is part of its label (for example `Headers.SeqRead`,
//!   `RandomWrite` or `insert_unsorted`). Default: every row.
//! - `TNDB_RETH_BENCH_REPEAT`: the minimum samples per cell (default 5). A large one makes one row
//!   run long enough to profile: for example `TNDB_RETH_BENCH_ENGINES=tndb
//!   TNDB_RETH_BENCH_ROWS=RandomRead TNDB_RETH_BENCH_REPEAT=200000`.
//! - `TNDB_RETH_BENCH_ENGINES`: `tndb`, `mdbx` or both (default), to profile one side.
//! - `TNDB_RETH_BENCH_MDBX_PAGE`: MDBX's page size (default reth's: the OS page size, clamped to 4
//!   KiB..=64 KiB). On Apple Silicon the OS page is 16 KiB; `4096` there gives the like-for-like
//!   column, which on Linux x86_64 is the default.
//!
//! ## Caveats (printed with the results too)
//!
//! - On macOS `msync` and `fsync` do not flush the drive's cache, so commits cost far less than on
//!   Linux, where every durable commit takes the file system's sync path. The Linux run decides.
//! - The write rows of `measure_table_db` and `measure_dupsort_db` are one transaction of 10 to
//!   1000 rows into a fresh environment: their time is mostly the commit.
//! - Keys are encoded and values compressed by reth's codecs on both sides; that cost is common to
//!   both columns.
//! - The run prints its temp dir (on Linux with its file system) and stops if that is a tmpfs or
//!   ramfs, where syncs cost nothing: set `TMPDIR` to a directory on a real disk.

// Workloads adapted from reth v1.11.3, `crates/storage/db/benches/criterion.rs`, `hash_keys.rs`,
// `get.rs`, `put.rs` and `utils.rs` (`RANDOM_INDEXES`), and the vector shapes of
// `crates/cli/commands/src/test_vectors/tables.rs`; reth is licensed MIT OR Apache-2.0.

use crate::{TndbArgs, TndbEnv, TndbEnvKind};
use alloy::{
    consensus::{EthereumTxEnvelope, Header, Signed, TxEip1559, TxEip4844, TxLegacy},
    primitives::{Address, Bloom, Bytes, Signature, TxKind, B256, B64, U256},
};
use reth_db::{mdbx::DatabaseArguments, DatabaseEnv};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    database::Database,
    database_metrics::DatabaseMetrics,
    models::{StoredBlockBodyIndices, StoredBlockOmmers},
    table::{DupSort, Table},
    tables::{
        BlockBodyIndices, BlockOmmers, CanonicalHeaders, HeaderNumbers, Headers, PlainAccountState,
        PlainStorageState, TransactionHashNumbers, Transactions,
    },
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{Account, StorageEntry};
use std::{
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Rows per table, as reth's `generate_vectors` writes them.
const PER_TABLE: usize = 1000;
/// Rows of `BlockOmmers` and `Transactions` (reth's vectors hold 100 of each).
const PER_TABLE_SMALL: usize = 100;
/// The rows the `RandomWrite` and `RandomRead` rows touch (reth's `utils.rs`).
const RANDOM_INDEXES: [usize; 10] = [23, 2, 42, 5, 3, 99, 54, 0, 33, 64];
/// `hash_keys` batch sizes unless `TNDB_RETH_BENCH_N` says otherwise.
const DEFAULT_HASH_SIZES: [usize; 2] = [10_000, 100_000];
/// Keys the get rows look up (reth's `get.rs` takes 10M).
const GET_KEYS: usize = 1_000_000;
/// Rows of the put rows (reth's `put.rs` `NUM_BLOCKS`).
const PUT_BLOCKS: u64 = 1_000_000;
/// Minimum samples per cell unless `TNDB_RETH_BENCH_REPEAT` says otherwise.
const DEFAULT_REPEAT: usize = 5;
/// Measured time each cell gathers at least (reth's criterion `measurement_time`).
const MEASURE: Duration = Duration::from_millis(200);
/// Samples per cell at most.
const MAX_SAMPLES: usize = 10_000;
/// Wall time (untimed setup included) after which a row stops sampling once each environment
/// has its minimum of samples, even short of `MEASURE`.
const ROW_WALL: Duration = Duration::from_secs(2);
/// MDBX's size limit (reth's `DatabaseArguments::test()` has 64 MiB).
const MDBX_MAX_SIZE: usize = 16 << 30;
/// The data's seed.
const SEED: u64 = 0x7e1c_0de5_eed5_0001;

/// A small deterministic generator (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `lo..hi`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }

    fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes()[..chunk.len()]);
        }
    }

    fn b256(&mut self) -> B256 {
        let mut b = B256::ZERO;
        self.fill(b.as_mut_slice());
        b
    }

    fn address(&mut self) -> Address {
        let mut a = Address::ZERO;
        self.fill(a.as_mut_slice());
        a
    }

    fn bytes(&mut self, len: usize) -> Bytes {
        let mut b = vec![0; len];
        self.fill(&mut b);
        b.into()
    }

    /// A number of `bytes` significant bytes.
    fn u256(&mut self, bytes: usize) -> U256 {
        let mut b = [0u8; 32];
        self.fill(&mut b[32 - bytes..]);
        U256::from_be_bytes(b)
    }

    /// A block number in mainnet's range.
    fn block(&mut self) -> u64 {
        self.range(0, 24_000_000)
    }

    /// A transaction number in mainnet's range.
    fn tx_number(&mut self) -> u64 {
        self.range(0, 3_000_000_000)
    }

    fn header(&mut self) -> Header {
        Header {
            parent_hash: self.b256(),
            ommers_hash: self.b256(),
            beneficiary: self.address(),
            state_root: self.b256(),
            transactions_root: self.b256(),
            receipts_root: self.b256(),
            logs_bloom: {
                let mut bloom = Bloom::ZERO;
                self.fill(bloom.as_mut_slice());
                bloom
            },
            number: self.block(),
            gas_limit: 36_000_000,
            gas_used: self.range(0, 36_000_000),
            timestamp: self.range(1_700_000_000, 1_800_000_000),
            extra_data: {
                let len = self.range(0, 33) as usize;
                self.bytes(len)
            },
            mix_hash: self.b256(),
            nonce: B64::ZERO,
            base_fee_per_gas: Some(self.range(1, 100_000_000_000)),
            withdrawals_root: Some(self.b256()),
            blob_gas_used: Some(self.range(0, 7) * 131_072),
            excess_blob_gas: Some(self.range(0, 10_000_000)),
            parent_beacon_block_root: Some(self.b256()),
            ..Default::default()
        }
    }

    fn transaction(&mut self) -> EthereumTxEnvelope<TxEip4844> {
        let signature = Signature::new(self.u256(32), self.u256(32), self.next().is_multiple_of(2));
        let to = TxKind::Call(self.address());
        let input_len = self.range(0, 301) as usize;
        if self.next().is_multiple_of(4) {
            let tx = TxLegacy {
                chain_id: Some(1),
                nonce: self.range(0, 10_000),
                gas_price: u128::from(self.range(1, 100_000_000_000)),
                gas_limit: self.range(21_000, 1_000_000),
                to,
                value: self.u256(8),
                input: self.bytes(input_len),
            };
            EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, signature))
        } else {
            let tx = TxEip1559 {
                chain_id: 1,
                nonce: self.range(0, 10_000),
                gas_limit: self.range(21_000, 1_000_000),
                max_fee_per_gas: u128::from(self.range(1, 100_000_000_000)),
                max_priority_fee_per_gas: u128::from(self.range(1, 1_000_000_000)),
                to,
                value: self.u256(8),
                input: self.bytes(input_len),
                ..Default::default()
            };
            EthereumTxEnvelope::Eip1559(Signed::new_unhashed(tx, signature))
        }
    }
}

/// `n` rows of table `T` with unique keys, sorted by key, as reth's test vectors are.
fn vectors<T: Table>(
    rng: &mut Rng,
    n: usize,
    mut row: impl FnMut(&mut Rng) -> (T::Key, T::Value),
) -> Vec<(T::Key, T::Value)> {
    let mut rows = BTreeMap::new();
    while rows.len() < n {
        let (key, value) = row(rng);
        rows.entry(key).or_insert(value);
    }
    rows.into_iter().collect()
}

/// `PlainStorageState` rows as reth's dup-sort vectors: keys of 100 to 300 sorted values each,
/// until there are `PER_TABLE` rows.
fn storage_vectors(rng: &mut Rng) -> Vec<(Address, StorageEntry)> {
    let mut rows = Vec::new();
    let mut keys = BTreeSet::new();
    while rows.len() < PER_TABLE {
        let address = rng.address();
        if !keys.insert(address) {
            continue;
        }
        let count = rng.range(100, 300) as usize;
        let mut values: Vec<StorageEntry> = (0..count)
            .map(|_| {
                let bytes = rng.range(1, 33) as usize;
                StorageEntry { key: rng.b256(), value: rng.u256(bytes) }
            })
            .collect();
        values.sort();
        rows.extend(values.into_iter().map(|v| (address, v)));
    }
    rows.sort_by_key(|a| a.0);
    rows
}

/// Rows of `TransactionHashNumbers`: a transaction hash and its number.
type HashRows = Vec<(B256, u64)>;

/// `hash_keys.rs`'s `generate_batches`: a preload batch and an input batch of `size` rows each,
/// keys unique across both.
fn hash_batches(rng: &mut Rng, size: usize) -> (HashRows, HashRows) {
    let mut seen = std::collections::HashSet::with_capacity(2 * size);
    let mut batch = |rng: &mut Rng| {
        let mut rows = Vec::with_capacity(size);
        while rows.len() < size {
            let key = rng.b256();
            if seen.insert(key) {
                rows.push((key, rng.tx_number()));
            }
        }
        rows
    };
    let preload = batch(rng);
    let input = batch(rng);
    (preload, input)
}

/// An environment the bench runs on.
trait BenchDb: Database + DatabaseMetrics + Sized {
    /// A fresh environment in the empty directory `dir`, with every one of reth's tables.
    fn create(dir: &Path, mdbx_page: Option<usize>) -> Self;
}

impl BenchDb for TndbEnv {
    fn create(dir: &Path, _: Option<usize>) -> Self {
        let mut db =
            crate::tests::open_env(dir, TndbEnvKind::RW, TndbArgs::default()).expect("open tndb");
        db.create_tables().expect("create tables");
        db
    }
}

impl BenchDb for DatabaseEnv {
    fn create(dir: &Path, mdbx_page: Option<usize>) -> Self {
        let args = DatabaseArguments::test()
            .with_geometry_max_size(Some(MDBX_MAX_SIZE))
            .with_geometry_page_size(mdbx_page);
        reth_db::init_db(dir, args).expect("open mdbx")
    }
}

/// The size of table `T` (its pages times the page size), from the environment's metrics.
fn table_bytes<T: Table>(db: &impl DatabaseMetrics) -> u64 {
    let metrics = db.gauge_metrics();
    let found = metrics.iter().find(|(name, _, labels)| {
        *name == "db.table_size"
            && labels.iter().any(|l| l.key() == "table" && l.value() == T::NAME)
    });
    found.map_or(0, |(_, bytes, _)| *bytes as u64)
}

/// Which environment a sample runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Engine {
    Tndb,
    Mdbx,
}

impl Engine {
    const fn index(self) -> usize {
        match self {
            Self::Tndb => 0,
            Self::Mdbx => 1,
        }
    }
}

/// Fresh directories under the bench's temp dir.
struct Dirs {
    root: PathBuf,
    made: usize,
    mdbx_page: Option<usize>,
}

impl Dirs {
    /// A fresh environment of type `DB` in a new directory.
    fn fresh<DB: BenchDb>(&mut self) -> (PathBuf, DB) {
        self.made += 1;
        let dir = self.root.join(format!("env-{}", self.made));
        std::fs::create_dir(&dir).expect("bench dir");
        let db = DB::create(&dir, self.mdbx_page);
        (dir, db)
    }

    /// Drop `db` and remove its directory.
    fn discard<DB>(dir: PathBuf, db: DB) {
        drop(db);
        std::fs::remove_dir_all(dir).expect("remove bench dir");
    }
}

/// One measured cell.
#[derive(Clone, Copy, Debug)]
enum Cell {
    /// Time per operation.
    PerOp(Duration),
    /// A size in bytes.
    Bytes(u64),
}

impl Cell {
    fn render(self) -> String {
        match self {
            Self::PerOp(d) => {
                let ns = d.as_secs_f64() * 1e9;
                if ns < 1_000.0 {
                    format!("{ns:.1} ns")
                } else if ns < 1_000_000.0 {
                    format!("{:.2} us", ns / 1e3)
                } else {
                    format!("{:.2} ms", ns / 1e6)
                }
            }
            Self::Bytes(b) => format!("{:.2} MiB", b as f64 / (1024.0 * 1024.0)),
        }
    }

    fn value(self) -> f64 {
        match self {
            Self::PerOp(d) => d.as_secs_f64(),
            Self::Bytes(b) => b as f64,
        }
    }
}

/// One printed row.
struct Row {
    label: String,
    ops: usize,
    cells: [Option<Cell>; 2],
}

/// What runs (see the module docs) and what it measured.
struct Bench {
    dirs: Dirs,
    hash_sizes: Vec<usize>,
    rows_filter: Vec<String>,
    repeat: usize,
    engines: [bool; 2],
    rows: Vec<Row>,
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn list(v: &str) -> Vec<String> {
    v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
}

fn number(name: &str, v: &str) -> usize {
    v.parse().unwrap_or_else(|_| panic!("{name} = {v:?} is not a number"))
}

impl Bench {
    fn from_env(root: &Path) -> Self {
        let hash_sizes = env_var("TNDB_RETH_BENCH_N").map_or(DEFAULT_HASH_SIZES.to_vec(), |v| {
            list(&v).iter().map(|s| number("TNDB_RETH_BENCH_N", s)).collect()
        });
        let rows_filter = env_var("TNDB_RETH_BENCH_ROWS").map(|v| list(&v)).unwrap_or_default();
        let repeat = env_var("TNDB_RETH_BENCH_REPEAT")
            .map_or(DEFAULT_REPEAT, |v| number("TNDB_RETH_BENCH_REPEAT", &v));
        assert!(repeat >= 1, "TNDB_RETH_BENCH_REPEAT must be at least 1");
        let engines = match env_var("TNDB_RETH_BENCH_ENGINES") {
            None => [true, true],
            Some(v) => {
                let picked = list(&v);
                for p in &picked {
                    assert!(
                        p == "tndb" || p == "mdbx",
                        "TNDB_RETH_BENCH_ENGINES: {p:?} is not tndb or mdbx"
                    );
                }
                [picked.iter().any(|p| p == "tndb"), picked.iter().any(|p| p == "mdbx")]
            }
        };
        let mdbx_page =
            env_var("TNDB_RETH_BENCH_MDBX_PAGE").map(|v| number("TNDB_RETH_BENCH_MDBX_PAGE", &v));
        let dirs = Dirs { root: root.to_path_buf(), made: 0, mdbx_page };
        Self { dirs, hash_sizes, rows_filter, repeat, engines, rows: Vec::new() }
    }

    /// Whether the row of `group` labelled `label` runs.
    fn wants(&self, group: &str, label: &str) -> bool {
        self.rows_filter.is_empty()
            || self.rows_filter.iter().any(|f| f == group || label.contains(f.as_str()))
    }

    const fn has(&self, engine: Engine) -> bool {
        self.engines[engine.index()]
    }

    /// Sample `run` on each selected environment (see the module docs) and add the row of
    /// `ops` operations per sample.
    fn measure(
        &mut self,
        label: String,
        ops: usize,
        mut run: impl FnMut(&mut Dirs, Engine) -> Duration,
    ) {
        let engines: Vec<Engine> =
            [Engine::Tndb, Engine::Mdbx].into_iter().filter(|&e| self.has(e)).collect();
        for &e in &engines {
            run(&mut self.dirs, e);
        }
        let mut times: [Vec<Duration>; 2] = Default::default();
        let mut spent = [Duration::ZERO; 2];
        let started = Instant::now();
        let done = |times: &[Vec<Duration>; 2], spent: &[Duration; 2]| {
            let out_of_time = started.elapsed() >= ROW_WALL;
            engines.iter().all(|e| {
                let i = e.index();
                times[i].len() >= self.repeat
                    && (times[i].len() >= MAX_SAMPLES || spent[i] >= MEASURE || out_of_time)
            })
        };
        let mut round = 0;
        while !done(&times, &spent) {
            let mut order = engines.clone();
            if round % 2 == 1 {
                order.reverse();
            }
            for e in order {
                let d = run(&mut self.dirs, e);
                times[e.index()].push(d);
                spent[e.index()] += d;
            }
            round += 1;
        }
        let cells = times.map(|mut t| {
            (!t.is_empty()).then(|| {
                t.sort_unstable();
                Cell::PerOp(t[t.len() / 2] / ops.max(1) as u32)
            })
        });
        println!("  {label}: {}", render_cells(&cells));
        self.rows.push(Row { label, ops, cells });
    }

    /// Add a row of sizes.
    fn sizes(&mut self, label: String, sizes: [Option<u64>; 2]) {
        let cells = sizes.map(|s| s.map(Cell::Bytes));
        println!("  {label}: {}", render_cells(&cells));
        self.rows.push(Row { label, ops: 0, cells });
    }

    fn print(&self) {
        let header = ["bench", "ops", "tndb", "MDBX", "tndb/MDBX"];
        let lines: Vec<[String; 5]> = self
            .rows
            .iter()
            .map(|r| {
                let cell = |c: Option<Cell>| c.map_or("-".to_string(), Cell::render);
                let ratio = match r.cells {
                    [Some(t), Some(m)] if m.value() > 0.0 => {
                        format!("{:.2}", t.value() / m.value())
                    }
                    _ => "-".to_string(),
                };
                let ops = if r.ops == 0 { String::new() } else { r.ops.to_string() };
                [r.label.clone(), ops, cell(r.cells[0]), cell(r.cells[1]), ratio]
            })
            .collect();
        let widths: Vec<usize> = (0..5)
            .map(|i| lines.iter().map(|l| l[i].len()).chain([header[i].len()]).max().unwrap_or(0))
            .collect();
        let mut out = format!("| {:w$} |", header[0], w = widths[0]);
        let mut rule = format!("|{}|", "-".repeat(widths[0] + 2));
        for i in 1..5 {
            out.push_str(&format!(" {:>w$} |", header[i], w = widths[i]));
            rule.push_str(&format!("{}:|", "-".repeat(widths[i] + 1)));
        }
        println!("{out}\n{rule}");
        for l in &lines {
            let mut line = format!("| {:w$} |", l[0], w = widths[0]);
            for i in 1..5 {
                line.push_str(&format!(" {:>w$} |", l[i], w = widths[i]));
            }
            println!("{line}");
        }
    }
}

fn render_cells(cells: &[Option<Cell>; 2]) -> String {
    let cell = |c: Option<Cell>| c.map_or("-".to_string(), Cell::render);
    format!("tndb {}, MDBX {}", cell(cells[0]), cell(cells[1]))
}

/// A fresh environment filled with `input` by `tx.put` in one transaction (reth's `set_up_db`).
fn filled<DB: BenchDb, T: Table>(dirs: &mut Dirs, input: &[(T::Key, T::Value)]) -> (PathBuf, DB)
where
    T::Key: Clone,
    T::Value: Clone,
{
    let (dir, db) = dirs.fresh::<DB>();
    let tx = db.tx_mut().expect("tx");
    for (k, v) in input {
        tx.put::<T>(k.clone(), v.clone()).expect("put");
    }
    tx.commit().expect("commit");
    (dir, db)
}

/// `measure_table_db`'s `SeqWrite`: a write cursor's `append` of every row into a fresh
/// environment, and the commit.
fn seq_write<DB: BenchDb, T: Table>(dirs: &mut Dirs, input: &[(T::Key, T::Value)]) -> Duration
where
    T::Key: Clone,
{
    let (dir, db) = dirs.fresh::<DB>();
    let start = Instant::now();
    let tx = db.tx_mut().expect("tx");
    let mut cursor = tx.cursor_write::<T>().expect("cursor");
    for (k, v) in input {
        cursor.append(k.clone(), v).expect("append");
    }
    drop(cursor);
    tx.commit().expect("commit");
    let elapsed = start.elapsed();
    Dirs::discard(dir, db);
    elapsed
}

/// `measure_table_db`'s `RandomWrite`: a write cursor's `insert` of the rows of
/// `RANDOM_INDEXES` into a fresh environment, and the commit.
fn random_write<DB: BenchDb, T: Table>(dirs: &mut Dirs, input: &[(T::Key, T::Value)]) -> Duration
where
    T::Key: Clone,
{
    let (dir, db) = dirs.fresh::<DB>();
    let start = Instant::now();
    let tx = db.tx_mut().expect("tx");
    let mut cursor = tx.cursor_write::<T>().expect("cursor");
    for index in RANDOM_INDEXES {
        let (k, v) = &input[index];
        cursor.insert(k.clone(), v).expect("insert");
    }
    drop(cursor);
    tx.commit().expect("commit");
    let elapsed = start.elapsed();
    Dirs::discard(dir, db);
    elapsed
}

/// `measure_table_db`'s `SeqRead`: a cursor's walk from the first key over every row.
fn seq_read<T: Table>(db: &impl Database, first: &T::Key, rows: usize) -> Duration
where
    T::Key: Clone,
{
    let start = Instant::now();
    let tx = db.tx().expect("tx");
    let mut cursor = tx.cursor_read::<T>().expect("cursor");
    let mut walked = 0;
    for row in cursor.walk(Some(first.clone())).expect("walk") {
        black_box(row.expect("row"));
        walked += 1;
    }
    drop(cursor);
    drop(tx);
    let elapsed = start.elapsed();
    assert_eq!(walked, rows, "{} walk", T::NAME);
    elapsed
}

/// `measure_table_db`'s `RandomRead`: for each row of `RANDOM_INDEXES`, a new cursor and its
/// `seek_exact`.
fn random_read<T: Table>(db: &impl Database, input: &[(T::Key, T::Value)]) -> Duration
where
    T::Key: Clone,
{
    let start = Instant::now();
    let tx = db.tx().expect("tx");
    for index in RANDOM_INDEXES {
        let mut cursor = tx.cursor_read::<T>().expect("cursor");
        let found = cursor.seek_exact(input[index].0.clone()).expect("seek_exact");
        assert!(black_box(found).is_some(), "{} row {index}", T::NAME);
    }
    drop(tx);
    start.elapsed()
}

/// reth's `measure_table_db` on table `T`, and the size of the filled table.
fn table_db<T: Table>(bench: &mut Bench, input: &[(T::Key, T::Value)])
where
    T::Key: Clone,
    T::Value: Clone,
{
    let name = T::NAME;
    let label = |what: &str| format!("{name}.{what}");
    if bench.wants("table", &label("SeqWrite")) {
        bench.measure(label("SeqWrite"), input.len(), |dirs, e| match e {
            Engine::Tndb => seq_write::<TndbEnv, T>(dirs, input),
            Engine::Mdbx => seq_write::<DatabaseEnv, T>(dirs, input),
        });
    }
    if bench.wants("table", &label("RandomWrite")) {
        bench.measure(label("RandomWrite"), RANDOM_INDEXES.len(), |dirs, e| match e {
            Engine::Tndb => random_write::<TndbEnv, T>(dirs, input),
            Engine::Mdbx => random_write::<DatabaseEnv, T>(dirs, input),
        });
    }
    let reads = ["SeqRead", "RandomRead", "size"];
    if !reads.iter().any(|r| bench.wants("table", &label(r))) {
        return;
    }
    let tndb = bench.has(Engine::Tndb).then(|| filled::<TndbEnv, T>(&mut bench.dirs, input));
    let mdbx = bench.has(Engine::Mdbx).then(|| filled::<DatabaseEnv, T>(&mut bench.dirs, input));
    let first = &input[0].0;
    if bench.wants("table", &label("SeqRead")) {
        bench.measure(label("SeqRead"), input.len(), |_, e| match e {
            Engine::Tndb => seq_read::<T>(&tndb.as_ref().unwrap().1, first, input.len()),
            Engine::Mdbx => seq_read::<T>(&mdbx.as_ref().unwrap().1, first, input.len()),
        });
    }
    if bench.wants("table", &label("RandomRead")) {
        bench.measure(label("RandomRead"), RANDOM_INDEXES.len(), |_, e| match e {
            Engine::Tndb => random_read::<T>(&tndb.as_ref().unwrap().1, input),
            Engine::Mdbx => random_read::<T>(&mdbx.as_ref().unwrap().1, input),
        });
    }
    if bench.wants("table", &label("size")) {
        let sizes = [
            tndb.as_ref().map(|(_, db)| table_bytes::<T>(db)),
            mdbx.as_ref().map(|(_, db)| table_bytes::<T>(db)),
        ];
        bench.sizes(label("size"), sizes);
    }
    if let Some((dir, db)) = tndb {
        Dirs::discard(dir, db);
    }
    if let Some((dir, db)) = mdbx {
        Dirs::discard(dir, db);
    }
}

/// `measure_dupsort_db`'s `SeqWrite`: a dup write cursor's `append_dup` of every row.
fn dup_seq_write<DB: BenchDb, T: DupSort>(dirs: &mut Dirs, input: &[(T::Key, T::Value)]) -> Duration
where
    T::Key: Clone,
    T::Value: Clone,
{
    let (dir, db) = dirs.fresh::<DB>();
    let start = Instant::now();
    let tx = db.tx_mut().expect("tx");
    let mut cursor = tx.cursor_dup_write::<T>().expect("cursor");
    for (k, v) in input {
        cursor.append_dup(k.clone(), v.clone()).expect("append_dup");
    }
    drop(cursor);
    tx.commit().expect("commit");
    let elapsed = start.elapsed();
    Dirs::discard(dir, db);
    elapsed
}

/// `measure_dupsort_db`'s `RandomWrite`: `tx.put` of the rows of `RANDOM_INDEXES`.
fn dup_random_write<DB: BenchDb, T: DupSort>(
    dirs: &mut Dirs,
    input: &[(T::Key, T::Value)],
) -> Duration
where
    T::Key: Clone,
    T::Value: Clone,
{
    let (dir, db) = dirs.fresh::<DB>();
    let start = Instant::now();
    let tx = db.tx_mut().expect("tx");
    for index in RANDOM_INDEXES {
        let (k, v) = &input[index];
        tx.put::<T>(k.clone(), v.clone()).expect("put");
    }
    tx.commit().expect("commit");
    let elapsed = start.elapsed();
    Dirs::discard(dir, db);
    elapsed
}

/// `measure_dupsort_db`'s `SeqRead`: `walk_dup(None, Some(subkey 0))`, the first key's rows.
fn dup_seq_read<T: DupSort>(db: &impl Database, rows: usize) -> Duration
where
    T::SubKey: Default,
{
    let start = Instant::now();
    let tx = db.tx().expect("tx");
    let mut cursor = tx.cursor_dup_read::<T>().expect("cursor");
    let mut walked = 0;
    for row in cursor.walk_dup(None, Some(T::SubKey::default())).expect("walk_dup") {
        black_box(row.expect("row"));
        walked += 1;
    }
    drop(cursor);
    drop(tx);
    let elapsed = start.elapsed();
    assert_eq!(walked, rows, "{} walk_dup", T::NAME);
    elapsed
}

/// `PlainStorageState`'s `RandomRead`: for each row of `RANDOM_INDEXES`, a new dup cursor and its
/// `seek_by_key_subkey`.
fn storage_random_read(db: &impl Database, input: &[(Address, StorageEntry)]) -> Duration {
    let start = Instant::now();
    let tx = db.tx().expect("tx");
    for index in RANDOM_INDEXES {
        let (address, entry) = input[index];
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().expect("cursor");
        let found = cursor.seek_by_key_subkey(address, entry.key).expect("seek_by_key_subkey");
        assert_eq!(black_box(found), Some(entry), "PlainStorageState row {index}");
    }
    drop(tx);
    start.elapsed()
}

/// reth's `measure_dupsort_db` on `PlainStorageState`, with a `RandomRead`, and the size of the
/// filled table.
fn dupsort_db(bench: &mut Bench, input: &[(Address, StorageEntry)]) {
    type T = PlainStorageState;
    let label = |what: &str| format!("{}.{what}", T::NAME);
    if bench.wants("dupsort", &label("SeqWrite")) {
        bench.measure(label("SeqWrite"), input.len(), |dirs, e| match e {
            Engine::Tndb => dup_seq_write::<TndbEnv, T>(dirs, input),
            Engine::Mdbx => dup_seq_write::<DatabaseEnv, T>(dirs, input),
        });
    }
    if bench.wants("dupsort", &label("RandomWrite")) {
        bench.measure(label("RandomWrite"), RANDOM_INDEXES.len(), |dirs, e| match e {
            Engine::Tndb => dup_random_write::<TndbEnv, T>(dirs, input),
            Engine::Mdbx => dup_random_write::<DatabaseEnv, T>(dirs, input),
        });
    }
    let reads = ["SeqRead", "RandomRead", "size"];
    if !reads.iter().any(|r| bench.wants("dupsort", &label(r))) {
        return;
    }
    let tndb = bench.has(Engine::Tndb).then(|| filled::<TndbEnv, T>(&mut bench.dirs, input));
    let mdbx = bench.has(Engine::Mdbx).then(|| filled::<DatabaseEnv, T>(&mut bench.dirs, input));
    let first_key_rows = input.iter().take_while(|(k, _)| *k == input[0].0).count();
    if bench.wants("dupsort", &label("SeqRead")) {
        bench.measure(label("SeqRead"), first_key_rows, |_, e| match e {
            Engine::Tndb => dup_seq_read::<T>(&tndb.as_ref().unwrap().1, first_key_rows),
            Engine::Mdbx => dup_seq_read::<T>(&mdbx.as_ref().unwrap().1, first_key_rows),
        });
    }
    if bench.wants("dupsort", &label("RandomRead")) {
        bench.measure(label("RandomRead"), RANDOM_INDEXES.len(), |_, e| match e {
            Engine::Tndb => storage_random_read(&tndb.as_ref().unwrap().1, input),
            Engine::Mdbx => storage_random_read(&mdbx.as_ref().unwrap().1, input),
        });
    }
    if bench.wants("dupsort", &label("size")) {
        let sizes = [
            tndb.as_ref().map(|(_, db)| table_bytes::<T>(db)),
            mdbx.as_ref().map(|(_, db)| table_bytes::<T>(db)),
        ];
        bench.sizes(label("size"), sizes);
    }
    if let Some((dir, db)) = tndb {
        Dirs::discard(dir, db);
    }
    if let Some((dir, db)) = mdbx {
        Dirs::discard(dir, db);
    }
}

/// A `hash_keys.rs` scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashScenario {
    AppendAll,
    AppendInput,
    InsertUnsorted,
    InsertSorted,
    PutUnsorted,
    PutSorted,
}

impl HashScenario {
    const ALL: [Self; 6] = [
        Self::AppendAll,
        Self::AppendInput,
        Self::InsertUnsorted,
        Self::InsertSorted,
        Self::PutUnsorted,
        Self::PutSorted,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AppendAll => "append_all",
            Self::AppendInput => "append_input",
            Self::InsertUnsorted => "insert_unsorted",
            Self::InsertSorted => "insert_sorted",
            Self::PutUnsorted => "put_unsorted",
            Self::PutSorted => "put_sorted",
        }
    }

    const fn appends(self) -> bool {
        matches!(self, Self::AppendAll | Self::AppendInput)
    }

    const fn sorted(self) -> bool {
        !matches!(self, Self::InsertUnsorted | Self::PutUnsorted)
    }
}

/// One sample of a `hash_keys.rs` scenario: a fresh environment, the preload (untimed), the input
/// (sorted, untimed, when the scenario sorts), then the timed transaction. Returns the time and
/// the table's size afterwards.
fn hash_sample<DB: BenchDb>(
    dirs: &mut Dirs,
    scenario: HashScenario,
    preload: &[(B256, u64)],
    input: &[(B256, u64)],
) -> (Duration, u64) {
    type T = TransactionHashNumbers;
    let (dir, db) = dirs.fresh::<DB>();
    let mut input = input.to_vec();
    if scenario == HashScenario::AppendAll {
        input.extend_from_slice(preload);
    }
    if !scenario.appends() {
        db.update(|tx| {
            for &(k, v) in preload {
                tx.put::<T>(k, v).expect("preload");
            }
        })
        .expect("preload commit");
    }
    if scenario.sorted() {
        input.sort_by_key(|a| a.0);
    }
    let start = Instant::now();
    let tx = db.tx_mut().expect("tx");
    match scenario {
        HashScenario::AppendAll | HashScenario::AppendInput => {
            let mut cursor = tx.cursor_write::<T>().expect("cursor");
            for (k, v) in input {
                cursor.append(k, &v).expect("append");
            }
        }
        HashScenario::InsertUnsorted | HashScenario::InsertSorted => {
            let mut cursor = tx.cursor_write::<T>().expect("cursor");
            for (k, v) in input {
                cursor.insert(k, &v).expect("insert");
            }
        }
        HashScenario::PutUnsorted | HashScenario::PutSorted => {
            for (k, v) in input {
                tx.put::<T>(k, v).expect("put");
            }
        }
    }
    tx.commit().expect("commit");
    let elapsed = start.elapsed();
    let size = table_bytes::<T>(&db);
    Dirs::discard(dir, db);
    (elapsed, size)
}

/// reth's `hash_keys.rs` at batch size `size`.
fn hash_keys(bench: &mut Bench, rng: &mut Rng, size: usize) {
    let mut batches = None;
    for scenario in HashScenario::ALL {
        let (preload_size, input_size) = match scenario {
            HashScenario::AppendAll => (0, 2 * size),
            HashScenario::AppendInput => (0, size),
            _ => (size, size),
        };
        let label = format!(
            "TransactionHashNumbers.{} (preload {preload_size}, writing {input_size})",
            scenario.name()
        );
        if !bench.wants("hash", &label) {
            continue;
        }
        let (preload, input) = batches.get_or_insert_with(|| hash_batches(rng, size));
        let mut sizes = [None; 2];
        bench.measure(label.clone(), input_size, |dirs, e| {
            let (elapsed, bytes) = match e {
                Engine::Tndb => hash_sample::<TndbEnv>(dirs, scenario, preload, input),
                Engine::Mdbx => hash_sample::<DatabaseEnv>(dirs, scenario, preload, input),
            };
            sizes[e.index()] = Some(bytes);
            elapsed
        });
        bench.sizes(format!("{label} size"), sizes);
    }
}

/// `get.rs`: `get` and `get_by_encoded_key` of random keys in an empty table.
fn gets(bench: &mut Bench, rng: &mut Rng) {
    let labels = ["Get.get", "Get.get_by_encoded_key"];
    if !labels.iter().any(|l| bench.wants("get", l)) {
        return;
    }
    let keys: Vec<B256> = (0..GET_KEYS).map(|_| rng.b256()).collect();
    let tndb = bench.has(Engine::Tndb).then(|| bench.dirs.fresh::<TndbEnv>());
    let mdbx = bench.has(Engine::Mdbx).then(|| bench.dirs.fresh::<DatabaseEnv>());
    let tndb_tx = tndb.as_ref().map(|(_, db)| db.tx().expect("tx"));
    let mdbx_tx = mdbx.as_ref().map(|(_, db)| db.tx().expect("tx"));
    fn run(tx: &impl DbTx, keys: &[B256], encoded: bool) -> Duration {
        let start = Instant::now();
        for key in keys {
            let found = if encoded {
                tx.get_by_encoded_key::<TransactionHashNumbers>(key)
            } else {
                tx.get::<TransactionHashNumbers>(*key)
            };
            black_box(found.expect("get"));
        }
        start.elapsed()
    }
    for (label, encoded) in labels.into_iter().zip([false, true]) {
        if bench.wants("get", label) {
            bench.measure(label.to_string(), keys.len(), |_, e| match e {
                Engine::Tndb => run(tndb_tx.as_ref().unwrap(), &keys, encoded),
                Engine::Mdbx => run(mdbx_tx.as_ref().unwrap(), &keys, encoded),
            });
        }
    }
    drop((tndb_tx, mdbx_tx));
    if let Some((dir, db)) = tndb {
        Dirs::discard(dir, db);
    }
    if let Some((dir, db)) = mdbx {
        Dirs::discard(dir, db);
    }
}

/// One sample of `put.rs`: `PUT_BLOCKS` puts (or appends) into a fresh environment's
/// transaction, which then drops uncommitted; the loop and the drop are timed.
fn put_sample<DB: BenchDb>(dirs: &mut Dirs, append: bool) -> Duration {
    let (dir, db) = dirs.fresh::<DB>();
    let tx = db.tx_mut().expect("tx");
    let start = Instant::now();
    for i in 0..PUT_BLOCKS {
        if append {
            tx.append::<CanonicalHeaders>(i, B256::ZERO).expect("append");
        } else {
            tx.put::<CanonicalHeaders>(i, B256::ZERO).expect("put");
        }
    }
    drop(tx);
    let elapsed = start.elapsed();
    Dirs::discard(dir, db);
    elapsed
}

/// `put.rs`: `put` against `append` of rows in key order.
fn puts(bench: &mut Bench) {
    for (label, append) in [("Put.put", false), ("Put.append", true)] {
        if bench.wants("put", label) {
            bench.measure(label.to_string(), PUT_BLOCKS as usize, |dirs, e| match e {
                Engine::Tndb => put_sample::<TndbEnv>(dirs, append),
                Engine::Mdbx => put_sample::<DatabaseEnv>(dirs, append),
            });
        }
    }
}

/// Run reth's database benches on tndb and on MDBX and print one side-by-side table.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads
/// 1`.
#[test]
#[ignore = "on-demand performance comparison; run with --release --ignored --nocapture --test-threads 1"]
fn tndb_reth_vs_mdbx() {
    let tmp = tempfile::tempdir().expect("temp dir");
    announce_temp_dir(tmp.path());
    let mut bench = Bench::from_env(tmp.path());
    let mut rng = Rng(SEED);

    let rng = &mut rng;
    let canonical = vectors::<CanonicalHeaders>(rng, PER_TABLE, |r| (r.block(), r.b256()));
    let numbers = vectors::<HeaderNumbers>(rng, PER_TABLE, |r| (r.b256(), r.block()));
    let headers = vectors::<Headers>(rng, PER_TABLE, |r| (r.block(), r.header()));
    let indices = vectors::<BlockBodyIndices>(rng, PER_TABLE, |r| {
        let value =
            StoredBlockBodyIndices { first_tx_num: r.tx_number(), tx_count: r.range(0, 500) };
        (r.block(), value)
    });
    let ommers = vectors::<BlockOmmers>(rng, PER_TABLE_SMALL, |r| {
        let count = r.range(0, 3);
        (r.block(), StoredBlockOmmers { ommers: (0..count).map(|_| r.header()).collect() })
    });
    let hashes = vectors::<TransactionHashNumbers>(rng, PER_TABLE, |r| (r.b256(), r.tx_number()));
    let transactions =
        vectors::<Transactions>(rng, PER_TABLE_SMALL, |r| (r.tx_number(), r.transaction()));
    let storage = storage_vectors(rng);
    let accounts = vectors::<PlainAccountState>(rng, PER_TABLE, |r| {
        let bytes = r.range(1, 13) as usize;
        let account = Account {
            nonce: r.range(0, 1000),
            balance: r.u256(bytes),
            bytecode_hash: r.next().is_multiple_of(4).then(|| r.b256()),
        };
        (r.address(), account)
    });

    table_db::<CanonicalHeaders>(&mut bench, &canonical);
    table_db::<HeaderNumbers>(&mut bench, &numbers);
    table_db::<Headers>(&mut bench, &headers);
    table_db::<BlockBodyIndices>(&mut bench, &indices);
    table_db::<BlockOmmers>(&mut bench, &ommers);
    table_db::<TransactionHashNumbers>(&mut bench, &hashes);
    table_db::<Transactions>(&mut bench, &transactions);
    dupsort_db(&mut bench, &storage);
    table_db::<PlainAccountState>(&mut bench, &accounts);
    for size in bench.hash_sizes.clone() {
        hash_keys(&mut bench, rng, size);
    }
    gets(&mut bench, rng);
    puts(&mut bench);

    let mdbx_page = bench.dirs.mdbx_page.map_or_else(
        || format!("reth's default, the OS page ({} B)", page_size()),
        |p| format!("{p} B"),
    );
    println!(
        "\nreth's db benches, tndb (4 KiB pages, Durable) vs MDBX (Durable, page {mdbx_page}); \
         time per op, the median of at least {} samples and {} ms\n",
        bench.repeat,
        MEASURE.as_millis()
    );
    bench.print();
    println!(
        "\nNotes: on macOS msync/fsync do not flush the drive cache, so commits cost far less than \
         on Linux; the Linux run decides. The table write rows (SeqWrite, RandomWrite) are one \
         transaction into a fresh environment, so their time is mostly the commit. hash_keys \
         sorts its input before the timer (reth's bench times the sort too). Sizes are the \
         table's pages times the page size."
    );
}

/// The type of the file system holding `dir`, from Linux's `/proc/self/mountinfo` (the mount
/// whose point is the longest prefix of `dir`'s canonical path); `None` where that file does not
/// exist.
fn fs_type(dir: &Path) -> Option<String> {
    let dir = dir.canonicalize().ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    mounts
        .lines()
        .filter_map(|line| {
            let (fields, rest) = line.split_once(" - ")?;
            // The mount point is the fifth field, with a space as `\040`.
            let point = fields.split(' ').nth(4)?.replace("\\040", " ");
            let fs = rest.split(' ').next()?;
            dir.starts_with(&point).then(|| (point.len(), fs.to_owned()))
        })
        // Of equal lengths `max_by_key` takes the last: a later mount on a point hides the earlier.
        .max_by_key(|(len, _)| *len)
        .map(|(_, fs)| fs)
}

/// Print the bench's temp dir, and stop the run when it is on a file system held in memory
/// (tmpfs, ramfs): syncs there cost nothing, so durable commits would time no disk.
fn announce_temp_dir(dir: &Path) {
    let fs = fs_type(dir);
    match &fs {
        Some(fs) => println!("bench temp dir: {} ({fs})", dir.display()),
        None => println!("bench temp dir: {}", dir.display()),
    }
    assert!(
        !matches!(fs.as_deref(), Some("tmpfs" | "ramfs")),
        "the bench temp dir {} is on {}: syncs there cost nothing, so durable commits would time \
         no disk. Set TMPDIR to a directory on a real disk, for this command only.",
        dir.display(),
        fs.unwrap_or_default(),
    );
}

/// The OS page size.
fn page_size() -> usize {
    // SAFETY: `sysconf` reads a constant of the system.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(4096)
}
