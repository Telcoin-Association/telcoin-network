//! On-demand raw-KV benchmark: **the memory-mapped pack file vs MDBX**.
//!
//! Gauges whether a "pack file + added features" store could replace MDBX. It pits a minimal
//! pack-file KV — the append-only data log ([`Pack`](crate::archive::pack::Pack)) keyed by an index
//! — against MDBX, using **two index choices** so they can be compared head to head: the hash
//! digest index ([`HdxIndex`](crate::archive::digest_index::HdxIndex), point lookups) and the
//! sorted B+tree index ([`BtreeIndex`](crate::archive::btree_index::BtreeIndex), point + ordered).
//! A first table covers the point-KV subset all three share (bulk write, per-commit durable write,
//! random point reads); a second covers **ordered scans** the btree pack and MDBX can do but the
//! digest index cannot.
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage pack_vs_mdbx_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! ## Columns
//! - `pack-hdx` — the memory-mapped (`msync`) pack KV: append-only data log keyed by the mmap
//!   `HdxIndex`.
//! - `pack-btree` — the same log keyed by `BtreeIndex`.
//! - `tndb` — the pack-file-backed [`Database`](crate::tndb::TnDatabase): an append-log of values
//!   keyed by a `BtreeIndex`, driven through the typed table API.
//! - `mem` — the in-memory [`MemDatabase`](crate::mem_db::MemDatabase) baseline: a sorted
//!   `BTreeMap`, no disk.
//! - `mdbx-durable` — MDBX with real fsync-on-commit (`SyncMode::Durable`, chosen by the bench at
//!   open). This is the apples-to-apples durability comparison.
//! - `mdbx-nosync` — MDBX in `SafeNoSync` (the `#[cfg(test)]` default): commits without fsync, for
//!   context (its delta vs `mdbx-durable` is MDBX's own fsync cost).
//!
//! Sorted table (digest omitted): `pack-btree`, `tndb`, and `mem` (ordered scans, fetching each
//! value) and `mdbx` (ordered scan over the MDBX cursor, `iter` / `skip_to`).
//!
//! ## Rows (per value size)
//! - `write_bulk` — `N_BULK` inserts then **one** durability barrier (bulk-load throughput).
//! - `write_each_dur` — `N_EACH` inserts, a durability barrier **after each** (per-commit fsync).
//! - `read_rand` — `N_READ` random point-gets over the bulk-loaded keys.
//!
//! Values are [`ByteVec`] byte strings, encoded the way production stored byte fields are (one
//! length prefix and a copy). A plain `Vec<u8>` would go through serde one byte per call, and that
//! per-byte codec cost, not the storage engine, would dominate every row.
//! - `scan_all` / `range_scan` (sorted table) — full ascending scan / middle-half range scan, each
//!   visiting values in key order.
//!
//! ## Fairness caveats (printed with the results)
//! - A pack durable barrier msyncs **one file** (the data log); the hash digest index is
//!   WAL-derived and is not synced per barrier, exactly like `ConsensusPack::persist`. MDBX does a
//!   single-env commit.
//! - Pack gives O(1) point KV but **no ordered range scan / cursor** and no cross-key atomic
//!   transaction — features MDBX has that a replacement would need to add. This bench measures only
//!   the point-KV subset. Values use `PackCompression::None`.
//! - MDBX is itself mmap-backed, so `mdbx-durable` fsyncs its own mmap; `pack` uses `msync`.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Barrier,
    },
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::{ByteVec, Database, DbTx as _, DbTxMut as _, Table, TableHint, B256};

use crate::{
    archive::{
        btree_index::BtreeIndex,
        digest_index::HdxIndex,
        fxhasher::FxHasher,
        index::Index as _,
        pack::{Pack, PackCompression},
    },
    mem_db::MemDatabase,
    tndb::TnDatabase,
};

#[cfg(feature = "reth-libmdbx")]
use crate::mdbx::database::{MdbxDatabase, MEGABYTE};

// ---- workload sizing (on-demand perf test; scale up for heavier samples) ----
const N_BULK: u64 = 50_000; // bulk insert / read count
const N_EACH: u64 = 1_000; // per-commit durable inserts (a fsync each — kept modest)
const N_READ: u64 = 50_000; // random point-gets over the bulk keys
const PACK_VERSION: u16 = 1;
/// Value sizes: `(label, bytes)` — an index-row and a certificate-ish blob.
const VALUE_SIZES: &[(&str, usize)] = &[("64B", 64), ("1KB", 1024)];

/// A deterministic, non-trivial value of `size` bytes.
fn value(size: usize, seed: u64) -> ByteVec {
    let s = seed.to_le_bytes();
    ByteVec((0..size).map(|i| s[i % 8].wrapping_add((i & 0xff) as u8)).collect())
}

/// A well-distributed 64-bit mix (splitmix64) of `x` — used to spread keys and randomize read
/// order.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic, well-distributed 32-byte key for counter `i` (each 8-byte lane independently
/// mixed), so the hash index sees realistic key spread.
fn key(i: u64) -> B256 {
    let mut b = [0u8; 32];
    for (j, lane) in b.chunks_mut(8).enumerate() {
        let j = u64::try_from(j).expect("lane index fits u64");
        lane.copy_from_slice(&mix(i.wrapping_mul(4).wrapping_add(j)).to_le_bytes());
    }
    B256::from(b)
}

/// The common point-KV surface. Batch-level so the MDBX side needs no long-lived transaction.
trait KvStore {
    /// Insert every item, then apply ONE durability barrier.
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]);
    /// Insert each item under its OWN durability barrier (per-op fsync/commit).
    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]);
    /// Random point-get every key in `keys`; return the number found.
    fn read_rand(&mut self, keys: &[B256]) -> usize;
}

/// The ordered-scan surface a **sorted** index adds on top of [`KvStore`] — a full ascending scan
/// and a bounded range scan, both visiting values in key order. The hash digest index cannot do
/// these, so the digest pack deliberately does not implement this trait (and never appears in the
/// sorted results).
trait SortedKvStore: KvStore {
    /// Visit every entry in ascending key order, fetching each value; return the count.
    fn scan_all(&mut self) -> usize;
    /// Visit every entry whose key is in `[lo, hi)` in ascending order, fetching each value; return
    /// the count.
    fn range_scan(&mut self, lo: B256, hi: B256) -> usize;
}

// ---- pack-file KV: append-only data log keyed by a hash digest index ----

struct PackKv {
    data: Pack<ByteVec>,
    index: HdxIndex,
}

impl PackKv {
    fn open(dir: &Path) -> Self {
        let data =
            Pack::<ByteVec>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)
                .expect("open pack data");
        let index = HdxIndex::open_hdx_file(
            dir.join("idx"),
            data.header(),
            BuildHasherDefault::<FxHasher>::default(),
            false,
        )
        .expect("open digest index");
        Self { data, index }
    }

    /// Durably persist everything written so far: `msync` the data log. The hash index is not
    /// synced here — the data log acts as a WAL, so the index is rebuilt from it on recovery.
    fn barrier(&mut self) {
        self.data.commit().expect("pack commit");
        // No explicit index sync is needed here: the data log file acts as a WAL.
    }
}

impl KvStore for PackKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save(*k, pos).expect("save");
        }
        self.barrier();
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save(*k, pos).expect("save");
            self.barrier();
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let mut hits = 0;
        for k in keys {
            if let Ok(pos) = self.index.load(*k) {
                if self.data.fetch(pos).is_ok() {
                    hits += 1;
                }
            }
        }
        hits
    }
}

// ---- pack-file KV: the same data log keyed by the sorted B+tree index ----

struct PackBtreeKv {
    data: Pack<ByteVec>,
    index: BtreeIndex,
}

impl PackBtreeKv {
    fn open(dir: &Path) -> Self {
        let data =
            Pack::<ByteVec>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)
                .expect("open pack data");
        let index = BtreeIndex::open_btx_file(dir.join("btx"), data.header(), 32, false)
            .expect("open btree index");
        Self { data, index }
    }

    /// Durably persist the data log (the WAL). The index is rebuildable from the log, so — like
    /// `PackKv` — it is not synced on the barrier.
    fn barrier(&mut self) {
        self.data.commit().expect("pack commit");
    }
}

impl KvStore for PackBtreeKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save_digest(*k, pos).expect("save");
        }
        self.barrier();
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save_digest(*k, pos).expect("save");
            self.barrier();
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let mut hits = 0;
        for k in keys {
            if let Ok(pos) = self.index.load_digest(*k) {
                if self.data.fetch(pos).is_ok() {
                    hits += 1;
                }
            }
        }
        hits
    }
}

impl SortedKvStore for PackBtreeKv {
    fn scan_all(&mut self) -> usize {
        let mut count = 0;
        // The iterator borrows `self.index`; `self.data.fetch` borrows the disjoint `self.data`.
        let it = self.index.iter().expect("iter");
        for item in it {
            let (_key, pos) = item.expect("scan item");
            self.data.fetch(pos).expect("fetch");
            count += 1;
        }
        count
    }

    fn range_scan(&mut self, lo: B256, hi: B256) -> usize {
        let mut count = 0;
        let it = self.index.range(lo.0..hi.0).expect("range");
        for item in it {
            let (_key, pos) = item.expect("range item");
            self.data.fetch(pos).expect("fetch");
            count += 1;
        }
        count
    }
}

// ---- typed-Database KV: a shared table driven through the `Database` trait ----

/// Point-KV table: 32-byte key -> byte blob, on the durable `Epoch` route (shared by the `tndb`
/// and MDBX columns).
#[derive(Debug)]
struct KvTable;

impl Table for KvTable {
    type Key = B256;
    type Value = ByteVec;
    const NAME: &'static str = "kv";
    const HINT: TableHint = TableHint::Epoch;
}

// ---- tndb KV: the pack-file-backed `Database` (append-log values + sorted B+tree index) ----

struct TnKv {
    db: TnDatabase,
}

impl TnKv {
    fn open(dir: &Path) -> Self {
        let db = TnDatabase::open(dir).expect("open tndb");
        db.open_table::<KvTable>().expect("open table");
        Self { db }
    }
}

impl KvStore for TnKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        let mut txn = self.db.write_txn().expect("write_txn");
        for (k, v) in items {
            txn.insert::<KvTable>(k, v).expect("insert");
        }
        txn.commit().expect("commit"); // durably syncs the value log + the index
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let mut txn = self.db.write_txn().expect("write_txn");
            txn.insert::<KvTable>(k, v).expect("insert");
            txn.commit().expect("commit");
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let txn = self.db.read_txn().expect("read_txn");
        let mut hits = 0;
        for k in keys {
            if txn.get::<KvTable>(k).expect("get").is_some() {
                hits += 1;
            }
        }
        hits
    }
}

impl SortedKvStore for TnKv {
    fn scan_all(&mut self) -> usize {
        // The B+tree index yields keys in order; `iter` fetches each value from the log.
        self.db.iter::<KvTable>().count()
    }

    fn range_scan(&mut self, lo: B256, hi: B256) -> usize {
        // `skip_to` seeks to the first key >= lo; take while below hi for a `[lo, hi)` scan.
        self.db.skip_to::<KvTable>(&lo).expect("skip_to").take_while(|(k, _)| *k < hi).count()
    }
}

// ---- tndb in group-commit mode: writes return at once, `persist` is the durability barrier ----

struct TnGroupKv {
    db: TnDatabase,
    rt: tokio::runtime::Runtime,
}

impl TnGroupKv {
    fn open(dir: &Path) -> Self {
        let options = crate::tndb::TnDbOptions {
            commit: crate::tndb::CommitMode::Group,
            ..Default::default()
        };
        let db = TnDatabase::open_with(dir, options).expect("open tndb");
        db.open_table::<KvTable>().expect("open table");
        let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
        Self { db, rt }
    }

    /// The durability barrier.
    fn persist(&self) {
        self.rt.block_on(self.db.persist::<KvTable>()).expect("persist");
    }
}

impl KvStore for TnGroupKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        let mut txn = self.db.write_txn().expect("write_txn");
        for (k, v) in items {
            txn.insert::<KvTable>(k, v).expect("insert");
        }
        txn.commit().expect("commit");
        self.persist();
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            self.db.insert::<KvTable>(k, v).expect("insert");
            self.persist();
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let txn = self.db.read_txn().expect("read_txn");
        keys.iter().filter(|k| txn.get::<KvTable>(k).expect("get").is_some()).count()
    }
}

// ---- in-memory KV: the `MemDatabase` baseline (sorted `BTreeMap`, no disk) ----

struct MemKv {
    db: MemDatabase,
}

impl MemKv {
    fn open(_dir: &Path) -> Self {
        // In-memory: the bench dir is unused, and `commit` is a no-op, so the "durable" rows are a
        // pure in-memory structure cost — a lower bound for the disk-backed stores.
        let db = MemDatabase::new();
        db.open_table::<KvTable>().expect("open table");
        Self { db }
    }
}

impl KvStore for MemKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        let mut txn = self.db.write_txn().expect("write_txn");
        for (k, v) in items {
            txn.insert::<KvTable>(k, v).expect("insert");
        }
        txn.commit().expect("commit");
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let mut txn = self.db.write_txn().expect("write_txn");
            txn.insert::<KvTable>(k, v).expect("insert");
            txn.commit().expect("commit");
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let txn = self.db.read_txn().expect("read_txn");
        let mut hits = 0;
        for k in keys {
            if txn.get::<KvTable>(k).expect("get").is_some() {
                hits += 1;
            }
        }
        hits
    }
}

impl SortedKvStore for MemKv {
    fn scan_all(&mut self) -> usize {
        self.db.iter::<KvTable>().count()
    }

    fn range_scan(&mut self, lo: B256, hi: B256) -> usize {
        self.db.skip_to::<KvTable>(&lo).expect("skip_to").take_while(|(k, _)| *k < hi).count()
    }
}

// ---- MDBX KV (feature-gated) ----

#[cfg(feature = "reth-libmdbx")]
struct MdbxKv {
    db: MdbxDatabase,
}

#[cfg(feature = "reth-libmdbx")]
impl MdbxKv {
    fn open(dir: &Path, durable: bool) -> Self {
        // Pick the env sync mode at the open itself (test builds default to SafeNoSync), never
        // through the process environment, which other test threads read concurrently.
        let sync_mode = if durable {
            reth_libmdbx::SyncMode::Durable
        } else {
            reth_libmdbx::SyncMode::SafeNoSync
        };
        let db = MdbxDatabase::open_with_sync_mode(dir, 4, 512 * MEGABYTE, 8 * MEGABYTE, sync_mode)
            .expect("open mdbx");
        db.open_table::<KvTable>().expect("open table");
        Self { db }
    }
}

#[cfg(feature = "reth-libmdbx")]
impl KvStore for MdbxKv {
    fn write_bulk(&mut self, items: &[(B256, ByteVec)]) {
        let mut txn = self.db.write_txn().expect("write_txn");
        for (k, v) in items {
            txn.insert::<KvTable>(k, v).expect("insert");
        }
        txn.commit().expect("commit"); // fsyncs iff durable
    }

    fn write_each_durable(&mut self, items: &[(B256, ByteVec)]) {
        for (k, v) in items {
            let mut txn = self.db.write_txn().expect("write_txn");
            txn.insert::<KvTable>(k, v).expect("insert");
            txn.commit().expect("commit");
        }
    }

    fn read_rand(&mut self, keys: &[B256]) -> usize {
        let txn = self.db.read_txn().expect("read_txn");
        let mut hits = 0;
        for k in keys {
            if txn.get::<KvTable>(k).expect("get").is_some() {
                hits += 1;
            }
        }
        hits
    }
}

#[cfg(feature = "reth-libmdbx")]
impl SortedKvStore for MdbxKv {
    fn scan_all(&mut self) -> usize {
        // MDBX tables are key-ordered; `iter` yields (key, value) ascending and materializes
        // values.
        self.db.iter::<KvTable>().count()
    }

    fn range_scan(&mut self, lo: B256, hi: B256) -> usize {
        // `skip_to` seeks to the first key >= lo; take while below hi for a `[lo, hi)` cursor scan.
        self.db.skip_to::<KvTable>(&lo).expect("skip_to").take_while(|(k, _)| *k < hi).count()
    }
}

// ---- battery ----

fn timed(f: impl FnOnce()) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

/// Run the 3 timed ops at one value `size`, returning `[write_bulk, write_each_dur, read_rand]`.
/// Store A (bulk + reads) and store B (per-op durable) are fresh so the per-op barrier isn't
/// inflated by a huge pre-existing index.
fn run_size<S: KvStore>(open: impl Fn(&Path) -> S, size: usize) -> [Duration; 3] {
    let items: Vec<(B256, ByteVec)> = (0..N_BULK).map(|i| (key(i), value(size, i))).collect();
    let read_keys: Vec<B256> = (0..N_READ).map(|m| key(mix(m) % N_BULK)).collect();

    let dir_a = TempDir::with_prefix("packkv_a").expect("temp dir");
    let mut a = open(dir_a.path());
    let t_bulk = timed(|| a.write_bulk(&items));
    let mut hits = 0;
    let t_read = timed(|| hits = a.read_rand(&read_keys));
    assert_eq!(
        u64::try_from(hits).expect("hit count fits u64"),
        N_READ,
        "every read must hit a written key"
    );
    drop(a);
    drop(dir_a);

    let dir_b = TempDir::with_prefix("packkv_b").expect("temp dir");
    let mut b = open(dir_b.path());
    let t_each = timed(|| {
        b.write_each_durable(&items[..usize::try_from(N_EACH).expect("N_EACH fits usize")])
    });
    drop(b);
    drop(dir_b);

    [t_bulk, t_each, t_read]
}

/// One report column: the timed rows for every value size, flattened.
fn column<S: KvStore>(open: impl Fn(&Path) -> S + Copy) -> Vec<Duration> {
    let mut out = Vec::new();
    for (_, size) in VALUE_SIZES {
        out.extend_from_slice(&run_size(open, *size));
    }
    out
}

fn row_labels() -> Vec<String> {
    VALUE_SIZES
        .iter()
        .flat_map(|(name, _)| {
            [
                format!("write_bulk {name}"),
                format!("write_each_dur {name}"),
                format!("read_rand {name}"),
            ]
        })
        .collect()
}

/// Time the sorted ops at one value `size`, returning `[scan_all, range_scan]`. A fresh store is
/// bulk-loaded, then scanned in key order.
fn run_size_sorted<S: SortedKvStore>(
    open: impl Fn(&Path) -> S,
    size: usize,
    lo: B256,
    hi: B256,
) -> [Duration; 2] {
    let items: Vec<(B256, ByteVec)> = (0..N_BULK).map(|i| (key(i), value(size, i))).collect();
    let dir = TempDir::with_prefix("packkv_sorted").expect("temp dir");
    let mut s = open(dir.path());
    s.write_bulk(&items);

    let mut n_all = 0;
    let t_scan = timed(|| n_all = s.scan_all());
    assert_eq!(n_all as u64, N_BULK, "scan_all must visit every key in order");

    let mut n_range = 0;
    let t_range = timed(|| n_range = s.range_scan(lo, hi));
    assert!(n_range > 0 && (n_range as u64) <= N_BULK, "range_scan count out of range: {n_range}");

    drop(s);
    drop(dir);
    [t_scan, t_range]
}

/// One sorted-report column: the timed sorted rows for every value size, flattened.
fn column_sorted<S: SortedKvStore>(
    open: impl Fn(&Path) -> S + Copy,
    lo: B256,
    hi: B256,
) -> Vec<Duration> {
    let mut out = Vec::new();
    for (_, size) in VALUE_SIZES {
        out.extend_from_slice(&run_size_sorted(open, *size, lo, hi));
    }
    out
}

fn sorted_row_labels() -> Vec<String> {
    VALUE_SIZES
        .iter()
        .flat_map(|(name, _)| [format!("scan_all {name}"), format!("range_scan {name}")])
        .collect()
}

fn print_table(title: &str, legend: &str, rows: &[String], cols: &[(&str, Vec<Duration>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 13usize;

    println!("\n{title}");
    println!("{legend}");

    print!("{:<label_w$}", "benchmark", label_w = label_w);
    for (name, _) in cols {
        print!(" {:>cell_w$}", name, cell_w = cell_w);
    }
    println!();
    for (i, label) in rows.iter().enumerate() {
        print!("{:<label_w$}", label, label_w = label_w);
        for (_, times) in cols {
            print!(
                " {:>cell_w$}",
                format!("{:.2}", times[i].as_secs_f64() * 1000.0),
                cell_w = cell_w
            );
        }
        println!();
    }
    println!();
}

/// Compare the memory-mapped pack-file KV against MDBX (durable + nosync) on raw point-KV.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage pack_vs_mdbx_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand pack-file-KV vs MDBX comparison; run with --ignored --nocapture --test-threads 1"]
fn pack_vs_mdbx_bench() {
    // --- point-KV table: digest pack vs btree pack vs MDBX (the ops all three share) ---
    let rows = row_labels();
    let mut cols: Vec<(&str, Vec<Duration>)> = Vec::new();

    println!("  running pack-hdx ...");
    cols.push(("pack-hdx", column(PackKv::open)));
    println!("  running pack-btree ...");
    cols.push(("pack-btree", column(PackBtreeKv::open)));
    println!("  running tndb ...");
    cols.push(("tndb", column(TnKv::open)));
    println!("  running tndb-group ...");
    cols.push(("tndb-group", column(TnGroupKv::open)));
    println!("  running mem ...");
    cols.push(("mem", column(MemKv::open)));

    #[cfg(feature = "reth-libmdbx")]
    {
        println!("  running mdbx-durable ...");
        cols.push(("mdbx-durable", column(|p| MdbxKv::open(p, true))));
        println!("  running mdbx-nosync ...");
        cols.push(("mdbx-nosync", column(|p| MdbxKv::open(p, false))));
    }

    let legend = format!(
        "legend: pack-hdx = mmap append-log + hash digest index; pack-btree = same log + sorted B+tree index (both msync barrier; the log is the WAL so the index is not synced). tndb = the pack-file Database (append-log + B+tree index) via the typed table API — its commit syncs BOTH the log and the index. mdbx-durable = fsync-on-commit, mdbx-nosync = SafeNoSync. write_bulk = {N_BULK} inserts + ONE barrier; write_each_dur = {N_EACH} inserts, a barrier EACH; read_rand = {N_READ} random point-gets. NOTE: a pack barrier msyncs the data log vs MDBX's single env commit."
    );
    print_table(
        "=== pack-file KV vs MDBX — point ops (ms; lower is better) ===",
        &legend,
        &rows,
        &cols,
    );

    // --- sorted table: ordered scans, only for stores that can (btree pack + MDBX cursor) ---
    println!("  running sorted scans ...");
    let mut sorted_keys: Vec<B256> = (0..N_BULK).map(key).collect();
    sorted_keys.sort();
    let lo = sorted_keys[(N_BULK / 4) as usize];
    let hi = sorted_keys[(3 * N_BULK / 4) as usize];

    let sorted_rows = sorted_row_labels();
    let mut sorted_cols: Vec<(&str, Vec<Duration>)> = Vec::new();
    sorted_cols.push(("pack-btree", column_sorted(PackBtreeKv::open, lo, hi)));
    sorted_cols.push(("tndb", column_sorted(TnKv::open, lo, hi)));
    sorted_cols.push(("mem", column_sorted(MemKv::open, lo, hi)));
    #[cfg(feature = "reth-libmdbx")]
    {
        sorted_cols.push(("mdbx", column_sorted(|p| MdbxKv::open(p, false), lo, hi)));
    }

    let sorted_legend = format!(
        "legend: ordered scans the digest index CANNOT do, so it is omitted. scan_all = full ascending scan of all {N_BULK} entries (fetching each value); range_scan = ascending scan of the lexicographic middle-half key range [lo, hi) (~{} entries).",
        N_BULK / 2
    );
    print_table(
        "=== sorted scans — btree pack vs MDBX (ms; lower is better) ===",
        &sorted_legend,
        &sorted_rows,
        &sorted_cols,
    );
}

// ---- concurrent reads: N reader threads over one shared `Database` ----

/// Reader thread counts for [`kv_concurrent_read_bench`].
const READ_THREADS: &[usize] = &[1, 2, 4, 8];
/// Random point-gets per reader thread.
const N_READ_MT: u64 = 50_000;
/// Inserts per write transaction for the `shared+writer` variant's writer thread.
const WRITER_BATCH: u64 = 64;
/// The writer's pause between batches: a steady writer, not one holding the write lock nonstop.
const WRITER_PAUSE: Duration = Duration::from_millis(1);

/// Define per-reader-thread copies of [`KvTable`] for the `per-table` variant.
macro_rules! kv_tables {
    ($($ty:ident = $name:literal),* $(,)?) => {$(
        /// A per-reader-thread copy of [`KvTable`] for the `per-table` variant.
        #[derive(Debug)]
        struct $ty;

        impl Table for $ty {
            type Key = B256;
            type Value = ByteVec;
            const NAME: &'static str = $name;
            const HINT: TableHint = TableHint::Epoch;
        }
    )*};
}

kv_tables!(
    KvT0 = "kv0",
    KvT1 = "kv1",
    KvT2 = "kv2",
    KvT3 = "kv3",
    KvT4 = "kv4",
    KvT5 = "kv5",
    KvT6 = "kv6",
    KvT7 = "kv7",
);

/// How the reader threads share tables.
#[derive(Clone, Copy, Debug)]
enum ReadMode {
    /// Every reader reads the same table.
    Shared,
    /// Reader `t` reads its own table (`KvT<t>`), all holding the same data.
    PerTable,
    /// `Shared`, plus one writer thread committing batches into the same table.
    SharedWithWriter,
}

impl ReadMode {
    const ALL: [Self; 3] = [Self::Shared, Self::PerTable, Self::SharedWithWriter];

    fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::PerTable => "per-table",
            Self::SharedWithWriter => "shared+writer",
        }
    }
}

/// Open every table the concurrent bench uses.
fn open_mt_tables<D: Database>(db: &D) {
    db.open_table::<KvTable>().expect("open table");
    db.open_table::<KvT0>().expect("open table");
    db.open_table::<KvT1>().expect("open table");
    db.open_table::<KvT2>().expect("open table");
    db.open_table::<KvT3>().expect("open table");
    db.open_table::<KvT4>().expect("open table");
    db.open_table::<KvT5>().expect("open table");
    db.open_table::<KvT6>().expect("open table");
    db.open_table::<KvT7>().expect("open table");
}

/// Bulk-load `items` into table `T` with one commit.
fn load<D: Database, T: Table<Key = B256, Value = ByteVec>>(db: &D, items: &[(B256, ByteVec)]) {
    let mut txn = db.write_txn().expect("write_txn");
    for (k, v) in items {
        txn.insert::<T>(k, v).expect("insert");
    }
    txn.commit().expect("commit");
}

/// Load `items` into the shared table and every per-thread table.
fn load_mt_tables<D: Database>(db: &D, items: &[(B256, ByteVec)]) {
    load::<D, KvTable>(db, items);
    load::<D, KvT0>(db, items);
    load::<D, KvT1>(db, items);
    load::<D, KvT2>(db, items);
    load::<D, KvT3>(db, items);
    load::<D, KvT4>(db, items);
    load::<D, KvT5>(db, items);
    load::<D, KvT6>(db, items);
    load::<D, KvT7>(db, items);
}

/// Point-get every key from table `T` in one read transaction; return the number found.
fn read_keys<D: Database, T: Table<Key = B256, Value = ByteVec>>(db: &D, keys: &[B256]) -> usize {
    let txn = db.read_txn().expect("read_txn");
    keys.iter().filter(|k| txn.get::<T>(k).expect("get").is_some()).count()
}

/// [`read_keys`] on per-thread table `KvT<table>`.
fn read_per_table<D: Database>(db: &D, table: usize, keys: &[B256]) -> usize {
    match table {
        0 => read_keys::<D, KvT0>(db, keys),
        1 => read_keys::<D, KvT1>(db, keys),
        2 => read_keys::<D, KvT2>(db, keys),
        3 => read_keys::<D, KvT3>(db, keys),
        4 => read_keys::<D, KvT4>(db, keys),
        5 => read_keys::<D, KvT5>(db, keys),
        6 => read_keys::<D, KvT6>(db, keys),
        7 => read_keys::<D, KvT7>(db, keys),
        _ => unreachable!("at most 8 reader threads"),
    }
}

/// Aggregate reader throughput (million gets per second) for `threads` readers in `mode` over a
/// loaded `db`. Readers (and the writer, if any) start together on a barrier; the clock runs from
/// the barrier until the last reader finishes.
fn concurrent_reads<D: Database>(db: &D, mode: ReadMode, threads: usize, size: usize) -> f64 {
    let keys: Vec<Vec<B256>> = (0..threads as u64)
        .map(|t| (0..N_READ_MT).map(|m| key(mix(m + t * N_READ_MT) % N_BULK)).collect())
        .collect();
    let writer = matches!(mode, ReadMode::SharedWithWriter);
    let start = Barrier::new(threads + 1 + usize::from(writer));
    let stop = AtomicBool::new(false);
    let elapsed = std::thread::scope(|scope| {
        if writer {
            let (start, stop) = (&start, &stop);
            scope.spawn(move || {
                start.wait();
                let mut next = N_BULK; // new keys, past the loaded ones the readers ask for
                while !stop.load(Ordering::Relaxed) {
                    let mut txn = db.write_txn().expect("write_txn");
                    for _ in 0..WRITER_BATCH {
                        txn.insert::<KvTable>(&key(next), &value(size, next)).expect("insert");
                        next += 1;
                    }
                    txn.commit().expect("commit");
                    std::thread::sleep(WRITER_PAUSE);
                }
            });
        }
        let readers: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(t, keys)| {
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    match mode {
                        ReadMode::PerTable => read_per_table(db, t, keys),
                        ReadMode::Shared | ReadMode::SharedWithWriter => {
                            read_keys::<D, KvTable>(db, keys)
                        }
                    }
                })
            })
            .collect();
        start.wait();
        let began = Instant::now();
        for reader in readers {
            let hits = reader.join().expect("reader thread");
            assert_eq!(hits as u64, N_READ_MT, "every read must hit a loaded key");
        }
        let elapsed = began.elapsed();
        stop.store(true, Ordering::Relaxed);
        elapsed
    });
    (threads as u64 * N_READ_MT) as f64 / elapsed.as_secs_f64() / 1e6
}

/// One concurrent-report column: throughput for every value size, mode and thread count, in
/// [`concurrent_row_labels`] order. Each value size gets a fresh store with every table loaded.
fn concurrent_column<D: Database>(open: impl Fn(&Path) -> D) -> Vec<f64> {
    let mut out = Vec::new();
    for (_, size) in VALUE_SIZES {
        let dir = TempDir::with_prefix("packkv_mt").expect("temp dir");
        let db = open(dir.path());
        let items: Vec<(B256, ByteVec)> = (0..N_BULK).map(|i| (key(i), value(*size, i))).collect();
        load_mt_tables(&db, &items);
        for mode in ReadMode::ALL {
            for &threads in READ_THREADS {
                out.push(concurrent_reads(&db, mode, threads, *size));
            }
        }
        drop(db);
        drop(dir);
    }
    out
}

fn concurrent_row_labels() -> Vec<String> {
    VALUE_SIZES
        .iter()
        .flat_map(|(name, _)| {
            ReadMode::ALL.into_iter().flat_map(move |mode| {
                READ_THREADS
                    .iter()
                    .map(move |threads| format!("{} {name} x{threads}", mode.label()))
            })
        })
        .collect()
}

fn print_rate_table(title: &str, legend: &str, rows: &[String], cols: &[(&str, Vec<f64>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 13usize;

    println!("\n{title}");
    println!("{legend}");
    print!("{:<label_w$}", "benchmark");
    for (name, _) in cols {
        print!(" {name:>cell_w$}");
    }
    println!();
    for (i, label) in rows.iter().enumerate() {
        print!("{label:<label_w$}");
        for (_, rates) in cols {
            print!(" {:>cell_w$}", format!("{:.2}", rates[i]));
        }
        println!();
    }
    println!();
}

/// Concurrent point reads: reader throughput as threads are added, for tndb, `MemDatabase` and
/// MDBX — on one shared table, on a table per thread, and with a steady concurrent writer.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage kv_concurrent_read_bench -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand concurrent read benchmark; run with --ignored --nocapture --test-threads 1"]
fn kv_concurrent_read_bench() {
    let mut cols: Vec<(&str, Vec<f64>)> = Vec::new();
    println!("  running tndb ...");
    cols.push((
        "tndb",
        concurrent_column(|p| {
            let db = TnDatabase::open(p).expect("open tndb");
            open_mt_tables(&db);
            db
        }),
    ));
    println!("  running mem ...");
    cols.push((
        "mem",
        concurrent_column(|_| {
            let db = MemDatabase::new();
            open_mt_tables(&db);
            db
        }),
    ));
    #[cfg(feature = "reth-libmdbx")]
    {
        println!("  running mdbx ...");
        cols.push((
            "mdbx",
            concurrent_column(|p| {
                let db = MdbxDatabase::open_with_sync_mode(
                    p,
                    16,
                    4096 * MEGABYTE,
                    8 * MEGABYTE,
                    reth_libmdbx::SyncMode::SafeNoSync,
                )
                .expect("open mdbx");
                open_mt_tables(&db);
                db
            }),
        ));
    }

    let legend = format!(
        "legend: aggregate reader throughput in million point-gets/s (higher is better); each reader does {N_READ_MT} random gets over {N_BULK} loaded keys. shared = all readers on one table; per-table = reader t on its own table (same data), so no table state is shared; shared+writer = shared plus one writer committing {WRITER_BATCH}-insert batches into that table every {WRITER_PAUSE:?}. Scaling = compare xN against x1."
    );
    print_rate_table(
        "=== concurrent point reads (M gets/s; higher is better) ===",
        &legend,
        &concurrent_row_labels(),
        &cols,
    );
}
