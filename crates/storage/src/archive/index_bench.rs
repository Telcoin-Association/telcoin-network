//! On-demand performance-regression + comparison benchmark for the archive point-lookup indexes:
//! the memory-mapped [`HdxIndex`](crate::archive::digest_index::index::HdxIndex) hash index and the
//! paged [`BtreeIndex`](crate::archive::btree_index::index::BtreeIndex). It reports one column per
//! index so runs can be compared to each other and over time to catch regressions.
//!
//! Only the operations the two indexes **share** are measured — the point-lookup `Index` ops
//! (`save`/`load`/`sync`). The B+tree's sorted-only ops (range/prefix/iteration) are out of scope
//! here; there is nothing in the hash index to compare them against. Both indexes are benched on
//! the **same** 32 key bytes, each using its native key type (`B256` vs `[u8; 32]`).
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage index_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! It sweeps a set of index sizes `N` (env `INDEX_BENCH_N=200000,2000000` overrides; default
//! `[200k, 2M, 8M]`; the 8M size needs a few GB, so trim via the env if RAM/disk-constrained). Rows
//! per size: `insert` (N saves, no sync), `sync_bulk` (one `sync()`), `load_hit` / `load_miss` (N
//! random point lookups, present / absent), `reopen_load` (reopen read-only then N lookups); plus
//! one fixed `insert_dur` probe (K_DUR save+sync pairs).
//!
//! Framing (why the columns differ): `sync_bulk`/`insert_dur` measure each index's durability
//! barrier, and the two indexes make different tradeoffs:
//! - `HdxIndex`: cache-free mmap hash index. It is rebuilt from the data-log WAL on an unclean
//!   shutdown, so it defers CRC off the per-op path: each write zeros the bucket's CRC trailer (a
//!   "dirty" marker) and only the dirty buckets are CRC'd at `sync()` (reads do not verify a per-op
//!   CRC). A bloom filter accelerates `load_miss`. That makes per-op sync (`insert_dur`) cheap
//!   while a full-build `sync_bulk` (nearly all buckets dirty) is the worst case.
//! - `BtreeIndex`: paged, mmap-backed B+tree using the same lazy-CRC regime — a modified page's CRC
//!   trailer is zeroed as a dirty marker, only dirty pages are CRC'd at `sync()`, and reads do
//!   **not** verify a per-op CRC. There is no bloom, so `load_miss` is a full root->leaf descent (≈
//!   `load_hit` cost); `sync()` is `msync` + header-last.
//!
//! Caveats: under `#[cfg(test)]` the `HdxIndex` bloom filter is 64 KB (2 MB in prod), so its
//! `load_miss` probes more buckets than production. `reopen_load` is cold only in-process (the file
//! stays in the OS page cache). macOS `fsync` is not a full barrier — run on Linux/SSD for the
//! durability rows.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::B256;

use crate::archive::{
    btree_index::BtreeIndex,
    digest_index::index::HdxIndex,
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, PackCompression},
};

/// Per-op durable inserts (a `sync()` after each — 2 barriers/op; N-independent, kept modest).
const K_DUR: u64 = 2_000;

/// Index sizes to sweep — env `INDEX_BENCH_N` (comma list) overrides. The default's 8M size needs a
/// few GB; trim via the env if RAM/disk-constrained.
fn n_sizes() -> Vec<u64> {
    let parsed: Vec<u64> = std::env::var("INDEX_BENCH_N")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
        .unwrap_or_default();
    if parsed.is_empty() {
        vec![200_000, 2_000_000, 8_000_000]
    } else {
        parsed
    }
}

/// Compact size label (`200000` -> `200k`, `8000000` -> `8M`).
fn fmt_n(n: u64) -> String {
    if n >= 1_000_000 && n.is_multiple_of(1_000_000) {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 && n.is_multiple_of(1_000) {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

/// splitmix64 mix — spreads keys and randomizes lookup order.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic, well-distributed 32-byte key bytes for counter `i`. Each index turns these into
/// its native key type (`B256::from(..)` for the hash index, `[u8; 32]` as-is for the B+tree).
fn kbytes(i: u64) -> [u8; 32] {
    let mut b = [0u8; 32];
    for (j, lane) in b.chunks_mut(8).enumerate() {
        lane.copy_from_slice(&mix(i.wrapping_mul(4).wrapping_add(j as u64)).to_le_bytes());
    }
    b
}

fn timed(f: impl FnOnce()) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

/// Per-`n` battery: `[insert, sync_bulk, load_hit, load_miss, reopen_load]`. `key_fn(i)` builds the
/// index's native key from counter `i`; `open(dir, read_only)` builds the concrete index.
fn size_battery<K, I, FK, FO>(key_fn: &FK, open: &FO, n: u64) -> [Duration; 5]
where
    K: Copy,
    I: Index<K, u64>,
    FK: Fn(u64) -> K,
    FO: Fn(&Path, bool) -> I,
{
    let keys: Vec<K> = (0..n).map(key_fn).collect();
    let miss: Vec<K> = (n..2 * n).map(key_fn).collect();
    let order: Vec<usize> = (0..n).map(|m| (mix(m) % n) as usize).collect();

    let dir = TempDir::with_prefix("indexbench").expect("temp dir");
    let mut idx = open(dir.path(), false);

    let t_insert = timed(|| {
        for (i, k) in keys.iter().enumerate() {
            idx.save(*k, i as u64).expect("save");
        }
    });
    let t_sync = timed(|| idx.sync().expect("sync"));

    let mut hits = 0usize;
    let t_hit = timed(|| {
        for &m in &order {
            if idx.load(keys[m]).is_ok() {
                hits += 1;
            }
        }
    });
    assert_eq!(hits, n as usize, "every existing key must be found");

    let mut misses = 0usize;
    let t_miss = timed(|| {
        for k in &miss {
            if idx.load(*k).is_err() {
                misses += 1;
            }
        }
    });
    assert_eq!(misses, n as usize, "no absent key may be found");
    drop(idx);

    // Cold in-process cache: reopen read-only and look the keys up again.
    let mut ridx = open(dir.path(), true);
    let mut rhits = 0usize;
    let t_reopen = timed(|| {
        for &m in &order {
            if ridx.load(keys[m]).is_ok() {
                rhits += 1;
            }
        }
    });
    assert_eq!(rhits, n as usize, "reopened index must find every key");
    drop(ridx);
    drop(dir);

    [t_insert, t_sync, t_hit, t_miss, t_reopen]
}

/// Fixed per-op durable probe (N-independent): `K_DUR` save+sync pairs on a fresh index.
fn durable<K, I, FK, FO>(key_fn: &FK, open: &FO) -> Duration
where
    K: Copy,
    I: Index<K, u64>,
    FK: Fn(u64) -> K,
    FO: Fn(&Path, bool) -> I,
{
    let dir = TempDir::with_prefix("indexbench_dur").expect("temp dir");
    let mut idx = open(dir.path(), false);
    let t = timed(|| {
        for i in 0..K_DUR {
            idx.save(key_fn(i), i).expect("save");
            idx.sync().expect("sync");
        }
    });
    drop(idx);
    drop(dir);
    t
}

/// All rows for one column: `insert_dur`, then per-`n` `[insert, sync_bulk, load_hit, load_miss,
/// reopen_load]`.
fn column<K, I, FK, FO>(key_fn: FK, open: FO, sizes: &[u64]) -> Vec<Duration>
where
    K: Copy,
    I: Index<K, u64>,
    FK: Fn(u64) -> K,
    FO: Fn(&Path, bool) -> I,
{
    let mut out = vec![durable(&key_fn, &open)];
    for &n in sizes {
        out.extend_from_slice(&size_battery(&key_fn, &open, n));
    }
    out
}

fn row_labels(sizes: &[u64]) -> Vec<String> {
    let mut rows = vec![format!("insert_dur {}", fmt_n(K_DUR))];
    for &n in sizes {
        let s = fmt_n(n);
        rows.push(format!("insert {s}"));
        rows.push(format!("sync_bulk {s}"));
        rows.push(format!("load_hit {s}"));
        rows.push(format!("load_miss {s}"));
        rows.push(format!("reopen_load {s}"));
    }
    rows
}

fn open_mmap(header: &DataHeader, dir: &Path, read_only: bool) -> HdxIndex {
    HdxIndex::open_hdx_file(
        dir.join("hdx"),
        header,
        BuildHasherDefault::<FxHasher>::default(),
        read_only,
    )
    .expect("open mmap")
}

fn open_btree(header: &DataHeader, dir: &Path, read_only: bool) -> BtreeIndex<32> {
    BtreeIndex::open_btx_file(dir.join("btx"), header, read_only).expect("open btree")
}

fn print_table(rows: &[String], cols: &[(&str, Vec<Duration>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 12usize;

    println!("\n=== archive index comparison: HdxIndex vs BtreeIndex (ms; lower is better) ===");
    println!("legend: overlapping point-lookup ops only (no sorting). HdxIndex = cache-free mmap hash index; per-op CRC replaced by a zeroed dirty marker, only dirty buckets CRC'd at sync, reads do not verify a per-op CRC (WAL/rebuildable regime), and a bloom filter accelerates load_miss (64 KB under cfg(test), 2 MB prod). BtreeIndex = paged mmap-backed B+tree with the same lazy-CRC regime (modified page CRC zeroed as a dirty marker, only dirty pages CRC'd at sync, reads do not verify a per-op CRC); no bloom, so load_miss is a full root->leaf descent (~load_hit cost); sync is msync + header-last. insert_dur = {K_DUR} save+sync pairs; per size: insert/load_hit/load_miss/reopen_load = N, sync_bulk = 1. reopen_load is cold only in-process (file stays in the OS page cache). macOS fsync is not a full barrier — run on Linux/SSD for the durability rows.");

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

/// Benchmark the point-lookup indexes (`HdxIndex` and `BtreeIndex`) across index sizes, both as a
/// regression baseline and a head-to-head comparison of the ops they share.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage index_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand index perf-regression + comparison benchmark; run with --ignored --nocapture --test-threads 1"]
fn index_bench() {
    let header = DataHeader::new(0, PackCompression::None, 0);
    let sizes = n_sizes();
    let rows = row_labels(&sizes);
    let mut cols: Vec<(&str, Vec<Duration>)> = Vec::new();

    println!("  running HdxIndex ...");
    cols.push((
        "HdxIndex",
        column(|i| B256::from(kbytes(i)), |dir, ro| open_mmap(&header, dir, ro), &sizes),
    ));
    println!("  running BtreeIndex ...");
    cols.push(("BtreeIndex", column(kbytes, |dir, ro| open_btree(&header, dir, ro), &sizes)));

    print_table(&rows, &cols);
}
