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
//! Point-ops table: `pack-hdx` (mmap `msync` log keyed by `HdxIndex`), `pack-btree` (same log keyed
//! by `BtreeIndex`), `mdbx-durable` (real fsync-on-commit via `TN_TEST_MDBX_SYNC=durable`, set by
//! the bench — the apples-to-apples durability comparison), and `mdbx-nosync` (`SafeNoSync`, the
//! `#[cfg(test)]` default — its delta vs `mdbx-durable` is MDBX's own fsync cost).
//! Sorted table (digest omitted): `pack-btree` (ordered scan over the B+tree leaf chain, fetching
//! each value) and `mdbx` (ordered scan over the MDBX cursor, `iter` / `skip_to`).
//!
//! ## Rows (per value size)
//! - `write_bulk` — `N_BULK` inserts then **one** durability barrier (bulk-load throughput).
//! - `write_each_dur` — `N_EACH` inserts, a durability barrier **after each** (per-commit fsync).
//! - `read_rand` — `N_READ` random point-gets over the bulk-loaded keys.
//! - `scan_all` / `range_scan` (sorted table) — full ascending scan / middle-half range scan, each
//!   visiting values in key order.
//!
//! ## Fairness caveats (printed with the results)
//! - A pack barrier msyncs only the data log — the index is rebuildable from it, so it is not
//!   synced on the barrier — vs MDBX's single-env commit.
//! - The digest pack gives O(1) point KV but **no ordered scan**; the btree pack adds ordered scan
//!   (the sorted table) at some point-lookup cost. Neither pack has cross-key atomic transactions —
//!   a feature MDBX has that a replacement would need to add. Values use `PackCompression::None`.
//! - MDBX is itself mmap-backed, so `mdbx-durable` fsyncs its own mmap; the packs use `msync`.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::B256;

use crate::archive::{
    btree_index::BtreeIndex,
    digest_index::HdxIndex,
    fxhasher::FxHasher,
    index::Index as _,
    pack::{Pack, PackCompression},
};

#[cfg(feature = "reth-libmdbx")]
use tn_types::{Database, DbTx as _, DbTxMut as _, Table, TableHint};

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
fn value(size: usize, seed: u64) -> Vec<u8> {
    let s = seed.to_le_bytes();
    (0..size).map(|i| s[i % 8].wrapping_add(i as u8)).collect()
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
        lane.copy_from_slice(&mix(i.wrapping_mul(4).wrapping_add(j as u64)).to_le_bytes());
    }
    B256::from(b)
}

/// The common point-KV surface. Batch-level so the MDBX side needs no long-lived transaction.
trait KvStore {
    /// Insert every item, then apply ONE durability barrier.
    fn write_bulk(&mut self, items: &[(B256, Vec<u8>)]);
    /// Insert each item under its OWN durability barrier (per-op fsync/commit).
    fn write_each_durable(&mut self, items: &[(B256, Vec<u8>)]);
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
    data: Pack<Vec<u8>>,
    index: HdxIndex,
}

impl PackKv {
    fn open(dir: &Path) -> Self {
        let data =
            Pack::<Vec<u8>>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)
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

    /// Durably persist everything written so far: `msync` the data log,
    /// then sync the hash index (its `index.hdx` + `index.odx`).
    fn barrier(&mut self) {
        self.data.commit().expect("pack commit");
        // We won't need to explicity sync the index- the data log file acts as a WAL.
        //self.index.sync().expect("index sync");
    }
}

impl KvStore for PackKv {
    fn write_bulk(&mut self, items: &[(B256, Vec<u8>)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save(*k, pos).expect("save");
        }
        self.barrier();
    }

    fn write_each_durable(&mut self, items: &[(B256, Vec<u8>)]) {
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
    data: Pack<Vec<u8>>,
    index: BtreeIndex,
}

impl PackBtreeKv {
    fn open(dir: &Path) -> Self {
        let data =
            Pack::<Vec<u8>>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)
                .expect("open pack data");
        let index = BtreeIndex::open_btx_file(dir.join("btx"), data.header(), false)
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
    fn write_bulk(&mut self, items: &[(B256, Vec<u8>)]) {
        for (k, v) in items {
            let pos = self.data.append(v).expect("append");
            self.index.save_digest(*k, pos).expect("save");
        }
        self.barrier();
    }

    fn write_each_durable(&mut self, items: &[(B256, Vec<u8>)]) {
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

// ---- MDBX KV (feature-gated) ----

/// Point-KV table: 32-byte key -> byte blob, on the durable `Epoch` route.
#[cfg(feature = "reth-libmdbx")]
#[derive(Debug)]
struct KvTable;

#[cfg(feature = "reth-libmdbx")]
impl Table for KvTable {
    type Key = B256;
    type Value = Vec<u8>;
    const NAME: &'static str = "kv";
    const HINT: TableHint = TableHint::Epoch;
}

#[cfg(feature = "reth-libmdbx")]
struct MdbxKv {
    db: MdbxDatabase,
}

#[cfg(feature = "reth-libmdbx")]
impl MdbxKv {
    fn open(dir: &Path, durable: bool) -> Self {
        // Select the env sync mode (test builds default to SafeNoSync). Read by
        // `MdbxDatabase::open`. Safe here: the bench runs single-threaded (`--test-threads
        // 1`).
        std::env::set_var("TN_TEST_MDBX_SYNC", if durable { "durable" } else { "safe-no-sync" });
        let db = MdbxDatabase::open(dir, 4, 512 * MEGABYTE, 8 * MEGABYTE).expect("open mdbx");
        db.open_table::<KvTable>().expect("open table");
        Self { db }
    }
}

#[cfg(feature = "reth-libmdbx")]
impl KvStore for MdbxKv {
    fn write_bulk(&mut self, items: &[(B256, Vec<u8>)]) {
        let mut txn = self.db.write_txn().expect("write_txn");
        for (k, v) in items {
            txn.insert::<KvTable>(k, v).expect("insert");
        }
        txn.commit().expect("commit"); // fsyncs iff durable
    }

    fn write_each_durable(&mut self, items: &[(B256, Vec<u8>)]) {
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
    let items: Vec<(B256, Vec<u8>)> = (0..N_BULK).map(|i| (key(i), value(size, i))).collect();
    let read_keys: Vec<B256> = (0..N_READ).map(|m| key(mix(m) % N_BULK)).collect();

    let dir_a = TempDir::with_prefix("packkv_a").expect("temp dir");
    let mut a = open(dir_a.path());
    let t_bulk = timed(|| a.write_bulk(&items));
    let mut hits = 0;
    let t_read = timed(|| hits = a.read_rand(&read_keys));
    assert_eq!(hits as u64, N_READ, "every read must hit a written key");
    drop(a);
    drop(dir_a);

    let dir_b = TempDir::with_prefix("packkv_b").expect("temp dir");
    let mut b = open(dir_b.path());
    let t_each = timed(|| b.write_each_durable(&items[..N_EACH as usize]));
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
    let items: Vec<(B256, Vec<u8>)> = (0..N_BULK).map(|i| (key(i), value(size, i))).collect();
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

    #[cfg(feature = "reth-libmdbx")]
    {
        println!("  running mdbx-durable ...");
        cols.push(("mdbx-durable", column(|p| MdbxKv::open(p, true))));
        println!("  running mdbx-nosync ...");
        cols.push(("mdbx-nosync", column(|p| MdbxKv::open(p, false))));
    }

    let legend = format!(
        "legend: pack-hdx = mmap append-log + hash digest index; pack-btree = same log + sorted B+tree index (both msync barrier; the log is the WAL so the index is not synced). mdbx-durable = fsync-on-commit, mdbx-nosync = SafeNoSync. write_bulk = {N_BULK} inserts + ONE barrier; write_each_dur = {N_EACH} inserts, a barrier EACH; read_rand = {N_READ} random point-gets. NOTE: a pack barrier msyncs the data log vs MDBX's single env commit."
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
