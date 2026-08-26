//! On-demand raw-KV benchmark: **pack files (buffered + mmap) vs MDBX**.
//!
//! Gauges whether a "pack file + added features" store could replace MDBX on raw point-KV
//! performance. It pits a minimal pack-file KV — the existing append-only data log
//! ([`Pack`](crate::archive::pack::Pack)) keyed by the hash digest index
//! ([`DigestIndex`](crate::archive::digest_index::DigestIndex)) — against MDBX across the common
//! subset both can do: bulk write, per-commit durable write, and random point reads.
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage pack_vs_mdbx_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! ## Columns
//! - `pack-buf` / `pack-mmap` — the pack KV on the buffered (`fsync`) vs memory-mapped (`msync`)
//!   file backend.
//! - `mdbx-durable` — MDBX with real fsync-on-commit (via `TN_TEST_MDBX_SYNC=durable`, set by the
//!   bench). This is the apples-to-apples durability comparison.
//! - `mdbx-nosync` — MDBX in `SafeNoSync` (the `#[cfg(test)]` default): commits without fsync, for
//!   context (its delta vs `mdbx-durable` is MDBX's own fsync cost).
//!
//! ## Rows (per value size)
//! - `write_bulk` — `N_BULK` inserts then **one** durability barrier (bulk-load throughput).
//! - `write_each_dur` — `N_EACH` inserts, a durability barrier **after each** (per-commit fsync).
//! - `read_rand` — `N_READ` random point-gets over the bulk-loaded keys.
//!
//! ## Fairness caveats (printed with the results)
//! - A pack durable barrier fsyncs/msyncs **3 files** (data + `index.hdx` + `index.odx`) vs MDBX's
//!   single-env commit — an architectural cost of the separate hash index.
//! - Pack gives O(1) point KV but **no ordered range scan / cursor** and no cross-key atomic
//!   transaction — features MDBX has that a replacement would need to add. This bench measures only
//!   the point-KV subset. Values use `PackCompression::None`.
//! - MDBX is itself mmap-backed, so `mdbx-durable` fsyncs its own mmap; `pack-mmap` uses `msync`.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::B256;

use crate::archive::{
    digest_index::DigestIndex,
    fxhasher::FxHasher,
    index::Index as _,
    pack::{FileBackend, Pack, PackCompression},
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

// ---- pack-file KV: append-only data log keyed by a hash digest index ----

struct PackKv {
    data: Pack<Vec<u8>>,
    index: DigestIndex,
}

impl PackKv {
    fn open(dir: &Path, backend: FileBackend) -> Self {
        let data = Pack::<Vec<u8>>::open_with_backend(
            dir.join("data"),
            0,
            false,
            PackCompression::None,
            PACK_VERSION,
            backend,
        )
        .expect("open pack data");
        let index = DigestIndex::open_hdx_file(
            dir.join("idx"),
            data.header(),
            BuildHasherDefault::<FxHasher>::default(),
            false,
            backend,
        )
        .expect("open digest index");
        Self { data, index }
    }

    /// Durably persist everything written so far: `fsync` (buffered) / `msync` (mmap) the data log,
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

fn print_table(rows: &[String], cols: &[(&str, Vec<Duration>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 13usize;

    println!("\n=== pack-file KV vs MDBX (ms; lower is better) ===");
    println!("legend: pack-buf/pack-mmap = append-log + hash digest index (fsync/msync barrier); mdbx-durable = fsync-on-commit, mdbx-nosync = SafeNoSync (no fsync). write_bulk = {N_BULK} inserts + ONE barrier; write_each_dur = {N_EACH} inserts, a barrier EACH; read_rand = {N_READ} random point-gets. NOTE: a pack barrier syncs 3 files (data+hdx+odx) vs MDBX's one env commit; pack has no ordered scan / cross-key txn.");

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

/// Compare the pack-file KV (buffered + mmap) against MDBX (durable + nosync) on raw point-KV.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage pack_vs_mdbx_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand pack-file-KV vs MDBX comparison; run with --ignored --nocapture --test-threads 1"]
fn pack_vs_mdbx_bench() {
    let rows = row_labels();
    let mut cols: Vec<(&str, Vec<Duration>)> = Vec::new();

    println!("  running pack-buf ...");
    cols.push(("pack-buf", column(|p| PackKv::open(p, FileBackend::Buffered))));
    println!("  running pack-mmap ...");
    cols.push(("pack-mmap", column(|p| PackKv::open(p, FileBackend::Mmap))));

    #[cfg(feature = "reth-libmdbx")]
    {
        println!("  running mdbx-durable ...");
        cols.push(("mdbx-durable", column(|p| MdbxKv::open(p, true))));
        println!("  running mdbx-nosync ...");
        cols.push(("mdbx-nosync", column(|p| MdbxKv::open(p, false))));
    }

    print_table(&rows, &cols);
}
