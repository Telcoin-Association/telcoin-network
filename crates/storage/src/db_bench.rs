//! Comparative benchmark harness for the consensus [`Database`] backends.
//!
//! [`crate::test::db_simp_bench`] times a single backend on one big 50k insert/commit. That hides
//! what dominates real node DB cost: **many small, frequently-committed, durable transactions** on
//! the consensus hot path (per-certificate ~3 inserts, externalized under a durability barrier),
//! plus large epoch-boundary `clear_table`s. This module runs a *list* of backends through a
//! battery of **basic** and **representative-of-real-usage** benchmarks and prints one easy-to-read
//! side-by-side comparison, so the current DB implementation's performance can be examined.
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage db_backend_comparison -- --ignored --nocapture --test-threads 1
//! ```
//!
//! ## What it models
//!
//! Values are sized `Vec<u8>` **proxies** for the real serialized records — the KV backends measure
//! bytes, so a 1.5 KB blob stresses them exactly like a 1.5 KB `Certificate` without the heavy
//! committee/BLS construction. Sizes: ~48 B index rows, ~1.5 KB certificates, ~256 KB batch bodies.
//! Two tables exercise both `CompositeDatabase` routes: [`BenchEpoch`] (`Epoch` hint — the durable
//! hot path) and [`BenchCache`] (`Cache` hint — the batch cache).
//!
//! ## Caveats (printed with the results too)
//!
//! - `LayeredDatabase`/`CompositeDatabase` `commit()` is **async** — it queues the disk write and
//!   returns. The real per-commit durability cost shows up under `durable_commits`, which awaits
//!   the production async `persist()` barrier after each commit (the same barrier the proposer /
//!   vote / certifier `await` before externalizing); `small_commits` is the same workload without
//!   it, so the delta is the durability cost. (The test-only `sync_persist` — which polls with a
//!   100 ms sleep — is deliberately NOT used here; it would measure the poll, not the barrier.)
//! - Under `#[cfg(test)]` the MDBX env opens `SafeNoSync` (no per-commit `fsync`, see
//!   `mdbx/database.rs`), so these numbers reflect the **test** durability mode, not production
//!   fsync.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use tn_types::{Database, DbTx as _, DbTxMut as _, Table, TableHint};

use tokio::runtime::Runtime;

/// Durable hot-path table (certificates / indexes / votes live on the `Epoch` route).
#[derive(Debug)]
struct BenchEpoch;
impl Table for BenchEpoch {
    type Key = u64;
    type Value = Vec<u8>;
    const NAME: &'static str = "BenchEpoch";
    const HINT: TableHint = TableHint::Epoch;
}

/// Batch-cache table (worker batches live on the non-durable `Cache` route).
#[derive(Debug)]
struct BenchCache;
impl Table for BenchCache {
    type Key = u64;
    type Value = Vec<u8>;
    const NAME: &'static str = "BenchCache";
    const HINT: TableHint = TableHint::Cache;
}

// ---- workload sizing (heavier than db_simp_bench; this is an on-demand perf test) ----

const BASIC_N: u64 = 50_000; // baseline bulk workload, matches db_simp_bench
const SMALL_VAL: usize = 48; // index-row sized
const CERT_VAL: usize = 1_536; // ~1.5 KB, certificate sized
const BATCH_VAL: usize = 256 * 1024; // 256 KB, batch-body sized

// The commit counts are kept modest because they dominate wall time on the disk backends (raw ReDB
// is ~5 ms per durable commit — each is a full transaction). The relative signal is identical at
// larger N — scale these up for more samples / heavier load.
const SMALL_COMMITS: u64 = 500; // transactions, 3 small inserts each
const DURABLE_COMMITS: u64 = 500; // same as SMALL_COMMITS, but + a `persist()` barrier per commit
const CERT_ROWS: u64 = 5_000;
const CERT_PER_TXN: u64 = 100; // ~per-round batch of certificate writes
const BATCH_ROWS: u64 = 200;
const BATCH_PER_TXN: u64 = 10;
const CLEAR_ROWS: u64 = 20_000; // epoch-teardown population
const MIXED_N: u64 = 1_000;

/// A deterministic, non-trivial value of `size` bytes (so we aren't measuring an all-zero fast
/// path).
fn make_value(size: usize, seed: u64) -> Vec<u8> {
    let s = seed.to_le_bytes();
    (0..size).map(|i| s[i % 8].wrapping_add(i as u8)).collect()
}

/// Drive the production async `persist::<T>()` durability barrier to completion. Raw backends'
/// default `persist` is a ready-`Ok` no-op (they are synchronously durable), so this is ~instant
/// for them; the layered/composite backends await a real disk-commit ack. Driven on `rt` so the
/// sync harness can use the async production barrier — not the test-only `sync_persist`, which
/// merely polls with a sleep.
fn barrier<T: Table, DB: Database>(rt: &Runtime, db: &DB) {
    rt.block_on(db.persist::<T>()).expect("persist barrier");
}

/// Empty a table and durably settle it, so the next benchmark starts from a clean state. Untimed.
fn reset<T: Table, DB: Database>(rt: &Runtime, db: &DB) {
    db.clear_table::<T>().expect("clear_table");
    barrier::<T, _>(rt, db);
}

// ---- the battery: each returns the timed duration for its measured region ----

/// Bulk-load `BenchEpoch` with `BASIC_N` small rows in one transaction (insert + commit timed).
fn bench_bulk_insert<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let start = Instant::now();
    let mut txn = db.write_txn().expect("write_txn");
    for i in 0..BASIC_N {
        txn.insert::<BenchEpoch>(&i, &make_value(SMALL_VAL, i)).expect("insert");
    }
    txn.commit().expect("commit");
    let elapsed = start.elapsed();
    barrier::<BenchEpoch, _>(rt, db); // settle before the read benchmarks (untimed)
    assert!(db.get::<BenchEpoch>(&0).expect("get").is_some(), "bulk insert must be visible");
    elapsed
}

/// Full forward iteration over the `BASIC_N` rows left by [`bench_bulk_insert`].
fn bench_iter_forward<DB: Database>(db: &DB) -> Duration {
    let start = Instant::now();
    let count = db.iter::<BenchEpoch>().count();
    let elapsed = start.elapsed();
    assert_eq!(count as u64, BASIC_N, "forward iter must see every row");
    elapsed
}

/// Full reverse iteration over the same rows.
fn bench_iter_reverse<DB: Database>(db: &DB) -> Duration {
    let start = Instant::now();
    let count = db.reverse_iter::<BenchEpoch>().count();
    let elapsed = start.elapsed();
    assert_eq!(count as u64, BASIC_N, "reverse iter must see every row");
    elapsed
}

/// Point-get every key inside a single read transaction.
fn bench_point_get_txn<DB: Database>(db: &DB) -> Duration {
    let start = Instant::now();
    let txn = db.read_txn().expect("read_txn");
    let mut found = 0u64;
    for i in 0..BASIC_N {
        if txn.get::<BenchEpoch>(&i).expect("get").is_some() {
            found += 1;
        }
    }
    drop(txn);
    let elapsed = start.elapsed();
    assert_eq!(found, BASIC_N, "every key must be found");
    elapsed
}

/// Clear the populated `BenchEpoch` table in one transaction.
fn bench_clear_table<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    let start = Instant::now();
    let mut txn = db.write_txn().expect("write_txn");
    txn.clear_table::<BenchEpoch>().expect("clear_table");
    txn.commit().expect("commit");
    let elapsed = start.elapsed();
    barrier::<BenchEpoch, _>(rt, db);
    elapsed
}

/// `SMALL_COMMITS` tiny transactions, each 3 inserts + commit — isolates per-commit overhead (the
/// per-round certificate write pattern). Commit is async on the layered backends.
fn bench_small_commits<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let val = make_value(SMALL_VAL, 7);
    let start = Instant::now();
    for r in 0..SMALL_COMMITS {
        let mut txn = db.write_txn().expect("write_txn");
        let base = r * 3;
        txn.insert::<BenchEpoch>(&base, &val).expect("insert cert");
        txn.insert::<BenchEpoch>(&(base + 1), &val).expect("insert idx1");
        txn.insert::<BenchEpoch>(&(base + 2), &val).expect("insert idx2");
        txn.commit().expect("commit");
    }
    start.elapsed()
}

/// Like [`bench_small_commits`] but `sync_persist`s after each commit — the durable externalization
/// pattern (proposer header / vote / proposed-cert), where losing a record across a crash would let
/// an honest node equivocate. This is the row where async-layered and raw-durable diverge most.
fn bench_durable_commits<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let val = make_value(SMALL_VAL, 9);
    let start = Instant::now();
    for r in 0..DURABLE_COMMITS {
        let mut txn = db.write_txn().expect("write_txn");
        let base = r * 3;
        txn.insert::<BenchEpoch>(&base, &val).expect("insert cert");
        txn.insert::<BenchEpoch>(&(base + 1), &val).expect("insert idx1");
        txn.insert::<BenchEpoch>(&(base + 2), &val).expect("insert idx2");
        txn.commit().expect("commit");
        barrier::<BenchEpoch, _>(rt, db); // production async `persist()` durability barrier
    }
    start.elapsed()
}

/// `CERT_ROWS` certificate-sized (~1.5 KB) rows written in `CERT_PER_TXN`-sized batches (a round's
/// worth of certificates per commit).
fn bench_cert_writes<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let val = make_value(CERT_VAL, 11);
    let start = Instant::now();
    let mut key = 0u64;
    while key < CERT_ROWS {
        let mut txn = db.write_txn().expect("write_txn");
        for _ in 0..CERT_PER_TXN {
            if key >= CERT_ROWS {
                break;
            }
            txn.insert::<BenchEpoch>(&key, &val).expect("insert cert");
            key += 1;
        }
        txn.commit().expect("commit");
    }
    start.elapsed()
}

/// `BATCH_ROWS` batch-sized (256 KB) values into the `Cache`-routed table.
fn bench_batch_writes<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchCache, _>(rt, db);
    let val = make_value(BATCH_VAL, 13);
    let start = Instant::now();
    let mut key = 0u64;
    while key < BATCH_ROWS {
        let mut txn = db.write_txn().expect("write_txn");
        for _ in 0..BATCH_PER_TXN {
            if key >= BATCH_ROWS {
                break;
            }
            txn.insert::<BenchCache>(&key, &val).expect("insert batch");
            key += 1;
        }
        txn.commit().expect("commit");
    }
    let elapsed = start.elapsed();
    barrier::<BenchCache, _>(rt, db);
    elapsed
}

/// Populate `CLEAR_ROWS` durable rows (untimed) then clear them all in one transaction — the
/// epoch-teardown pattern (`close_epoch.rs` clears the epoch tables in a single txn).
fn bench_epoch_clear<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let val = make_value(SMALL_VAL, 15);
    let mut txn = db.write_txn().expect("write_txn");
    for i in 0..CLEAR_ROWS {
        txn.insert::<BenchEpoch>(&i, &val).expect("insert");
    }
    txn.commit().expect("commit");
    barrier::<BenchEpoch, _>(rt, db);

    let start = Instant::now();
    let mut txn = db.write_txn().expect("write_txn");
    txn.clear_table::<BenchEpoch>().expect("clear_table");
    txn.commit().expect("commit");
    let elapsed = start.elapsed();
    barrier::<BenchEpoch, _>(rt, db);
    elapsed
}

/// Interleave a committed insert with a read of a previously-written key (read-your-writes during
/// consensus).
fn bench_mixed_rw<DB: Database>(rt: &Runtime, db: &DB) -> Duration {
    reset::<BenchEpoch, _>(rt, db);
    let val = make_value(SMALL_VAL, 17);
    let start = Instant::now();
    for i in 0..MIXED_N {
        db.insert::<BenchEpoch>(&i, &val).expect("insert");
        if i > 0 {
            let _ = db.get::<BenchEpoch>(&(i - 1)).expect("get");
        }
    }
    start.elapsed()
}

/// The ordered battery. Keep names short and stable — they are the report's row labels.
fn run_battery<DB: Database>(rt: &Runtime, db: &DB) -> Vec<(&'static str, Duration)> {
    // Basic ops share `BenchEpoch` state in sequence, like `db_simp_bench`.
    vec![
        ("bulk_insert 50k", bench_bulk_insert(rt, db)),
        ("iter_forward 50k", bench_iter_forward(db)),
        ("iter_reverse 50k", bench_iter_reverse(db)),
        ("point_get_txn 50k", bench_point_get_txn(db)),
        ("clear_table 50k", bench_clear_table(rt, db)),
        // Representative node-usage patterns (each resets its own table first).
        ("small_commits 500 x3", bench_small_commits(rt, db)),
        ("durable_commits 500 x3", bench_durable_commits(rt, db)),
        ("cert_writes 5k x1.5KB", bench_cert_writes(rt, db)),
        ("batch_writes 200 x256KB", bench_batch_writes(rt, db)),
        ("epoch_clear 20k", bench_epoch_clear(rt, db)),
        ("mixed_rw 1k", bench_mixed_rw(rt, db)),
    ]
}

/// Collects one results column per backend and prints an aligned comparison table.
struct BenchSuite {
    /// Benchmark row labels, captured from the first backend and reused for alignment.
    order: Vec<&'static str>,
    /// `(backend name, per-benchmark durations aligned to `order`)`.
    columns: Vec<(String, Vec<Duration>)>,
}

impl BenchSuite {
    fn new() -> Self {
        Self { order: Vec::new(), columns: Vec::new() }
    }

    /// Run the whole battery on `db`, appending a comparison column. Generic per backend because
    /// `Database: Clone` is not object-safe (so no `Vec<Box<dyn Database>>`).
    fn run<DB: Database>(&mut self, rt: &Runtime, db: DB, name: &str) {
        println!("  running battery for {name} ...");
        let results = run_battery(rt, &db);
        if self.order.is_empty() {
            self.order = results.iter().map(|(n, _)| *n).collect();
        }
        self.columns.push((name.to_string(), results.into_iter().map(|(_, d)| d).collect()));
        // Drop the backend (and its temp files / background writer) before the next one.
        drop(db);
    }

    /// Print the side-by-side comparison (milliseconds; lower is better).
    fn report(&self) {
        let label_w = self.order.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
        let cell_w = 12usize;

        println!("\n=== DB backend comparison (ms; lower is better) ===");
        println!("legend: layered/composite commit() is async (see durable_commits); test-cfg MDBX is SafeNoSync (no fsync).");

        // header
        print!("{:<label_w$}", "benchmark", label_w = label_w);
        for (name, _) in &self.columns {
            print!(" {:>cell_w$}", name, cell_w = cell_w);
        }
        println!();

        // rows
        for (row, label) in self.order.iter().enumerate() {
            print!("{:<label_w$}", label, label_w = label_w);
            for (_, times) in &self.columns {
                let ms = times[row].as_secs_f64() * 1000.0;
                print!(" {:>cell_w$}", format!("{ms:.2}"), cell_w = cell_w);
            }
            println!();
        }
        println!();
    }
}

// ---- backend construction (mirrors the module-local `open_*` helpers) ----

use crate::{
    composite_db::CompositeDatabase, layered_db::LayeredDatabase, mem_db::MemDatabase, ReDB,
};

/// Open both bench tables on `db`; `open_table` propagates through the layered/composite wrappers.
fn open_bench_tables<DB: Database>(db: &DB) {
    db.open_table::<BenchEpoch>().expect("open BenchEpoch");
    db.open_table::<BenchCache>().expect("open BenchCache");
}

fn build_mem() -> MemDatabase {
    let db = MemDatabase::new();
    open_bench_tables(&db);
    db
}

fn build_redb(path: &Path) -> ReDB {
    let db = ReDB::open(path).expect("open redb");
    open_bench_tables(&db);
    db
}

fn build_layered_redb(path: &Path) -> LayeredDatabase<ReDB> {
    let db = LayeredDatabase::open(ReDB::open(path).expect("open redb"), true);
    open_bench_tables(&db);
    db
}

fn build_composite_redb(base: &Path) -> CompositeDatabase<ReDB> {
    std::fs::create_dir_all(base).expect("create composite dir");
    let epoch = ReDB::open(base.join("epoch")).expect("open redb epoch");
    let kad = ReDB::open(base.join("kad")).expect("open redb kad");
    let cache = ReDB::open(base.join("cache")).expect("open redb cache");
    let db = CompositeDatabase::open(epoch, kad, cache);
    open_bench_tables(&db);
    db
}

#[cfg(feature = "reth-libmdbx")]
use crate::mdbx::database::{MdbxDatabase, MEGABYTE};

/// Generous single-env size — the bench holds ~50 MB of batch bodies plus the small/cert tables.
#[cfg(feature = "reth-libmdbx")]
const MDBX_SIZE: usize = 512 * 1024 * 1024;
#[cfg(feature = "reth-libmdbx")]
fn build_mdbx(path: &Path) -> MdbxDatabase {
    let db = MdbxDatabase::open(path, 8, MDBX_SIZE, 8 * MEGABYTE).expect("open mdbx");
    open_bench_tables(&db);
    db
}

#[cfg(feature = "reth-libmdbx")]
fn build_layered_mdbx(path: &Path) -> LayeredDatabase<MdbxDatabase> {
    let inner = MdbxDatabase::open(path, 8, MDBX_SIZE, 8 * MEGABYTE).expect("open mdbx");
    let db = LayeredDatabase::open(inner, true);
    open_bench_tables(&db);
    db
}

#[cfg(feature = "reth-libmdbx")]
fn build_composite_mdbx(base: &Path) -> CompositeDatabase<MdbxDatabase> {
    std::fs::create_dir_all(base).expect("create composite dir");
    let epoch =
        MdbxDatabase::open(base.join("epoch"), 8, MDBX_SIZE, 8 * MEGABYTE).expect("mdbx epoch");
    let kad = MdbxDatabase::open(base.join("kad"), 8, MDBX_SIZE, 8 * MEGABYTE).expect("mdbx kad");
    let cache =
        MdbxDatabase::open(base.join("cache"), 8, MDBX_SIZE, 8 * MEGABYTE).expect("mdbx cache");
    let db = CompositeDatabase::open(epoch, kad, cache);
    open_bench_tables(&db);
    db
}

/// Compare every available consensus `Database` backend through the benchmark battery.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage db_backend_comparison -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand DB performance comparison; run with --ignored --nocapture --test-threads 1"]
fn db_backend_comparison() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let base = tmp.path();
    let mut suite = BenchSuite::new();
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");

    suite.run(&runtime, build_mem(), "MemDb");
    suite.run(&runtime, build_redb(&base.join("redb")), "ReDB");
    suite.run(&runtime, build_layered_redb(&base.join("redb_layered")), "Layered<ReDB>");
    suite.run(&runtime, build_composite_redb(&base.join("redb_composite")), "Composite<ReDB>");

    #[cfg(feature = "reth-libmdbx")]
    {
        suite.run(&runtime, build_mdbx(&base.join("mdbx")), "MDBX");
        suite.run(&runtime, build_layered_mdbx(&base.join("mdbx_layered")), "Layered<MDBX>");
        // The production default: CompositeDatabase<MdbxDatabase>.
        suite.run(&runtime, build_composite_mdbx(&base.join("mdbx_composite")), "Composite<MDBX>");
    }

    suite.report();
}
