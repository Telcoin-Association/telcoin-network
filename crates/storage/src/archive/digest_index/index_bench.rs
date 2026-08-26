//! On-demand benchmark comparing the two digest-index implementations: the buffered/direct-IO
//! [`HdxIndex`](super::index::HdxIndex) (raw `File` + in-memory bucket cache) vs the cache-free,
//! zero-copy memory-mapped [`HdxIndexMmap`](super::index_mmap::HdxIndexMmap). Both implement the
//! [`Index`](crate::archive::index::Index) trait and share the on-disk format, so one battery
//! drives all three columns:
//! - `hdx-buf`  — `HdxIndex` on the buffered `File` backend (the direct-IO version).
//! - `hdx-mmap` — `HdxIndex` on the mmap file backend (same cached logic — isolates the file
//!   backend).
//! - `mmap-cf`  — `HdxIndexMmap` (the cache-free, zero-copy mmap version).
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage digest_index_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! Rows: `insert` (N saves, no sync), `sync_bulk` (one `sync()` after the bulk), `insert_dur`
//! (K_DUR save+sync pairs — 2 barriers each: hdx+odx), `load_hit` / `load_miss` (N random point
//! lookups, present / absent), `reopen_load` (reopen read-only then N random lookups).
//!
//! Caveats: under `#[cfg(test)]` the bloom filter is 64 KB (2 MB in prod), so `load_miss` probes
//! more buckets than production — both impls share it, so the comparison is fair, but absolute miss
//! numbers are test-mode. `reopen_load` is cold only in-process (the file stays in the OS page
//! cache). macOS `fsync` is not a full barrier — run on Linux/SSD for the durability rows.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::B256;

use super::{index::HdxIndex, index_mmap::HdxIndexMmap};
use crate::archive::{
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, FileBackend, PackCompression},
};

// ---- workload sizing (on-demand; scale up for heavier samples) ----
/// Insert / lookup count — well past the 1000 initial buckets, so splits + overflow are exercised.
const N: u64 = 200_000;
/// Per-op durable inserts (a `sync()` after each — 2 barriers/op; kept modest).
const K_DUR: u64 = 2_000;

/// splitmix64 mix — spreads keys and randomizes lookup order.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic, well-distributed 32-byte key for counter `i`.
fn key(i: u64) -> B256 {
    let mut b = [0u8; 32];
    for (j, lane) in b.chunks_mut(8).enumerate() {
        lane.copy_from_slice(&mix(i.wrapping_mul(4).wrapping_add(j as u64)).to_le_bytes());
    }
    B256::from(b)
}

fn timed(f: impl FnOnce()) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

/// Run the six-row battery on one index implementation. `open(dir, read_only)` builds the concrete
/// index. Returns `[insert, sync_bulk, insert_dur, load_hit, load_miss, reopen_load]`.
fn column<I: Index<B256, u64>>(open: impl Fn(&Path, bool) -> I) -> Vec<Duration> {
    let keys: Vec<B256> = (0..N).map(key).collect();
    let miss: Vec<B256> = (N..2 * N).map(key).collect();
    let order: Vec<usize> = (0..N).map(|m| (mix(m) % N) as usize).collect();

    let dir = TempDir::with_prefix("hdxbench").expect("temp dir");
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
    assert_eq!(hits, N as usize, "every existing key must be found");

    let mut misses = 0usize;
    let t_miss = timed(|| {
        for k in &miss {
            if idx.load(*k).is_err() {
                misses += 1;
            }
        }
    });
    assert_eq!(misses, N as usize, "no absent key may be found");
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
    assert_eq!(rhits, N as usize, "reopened index must find every key");
    drop(ridx);
    drop(dir);

    // Per-op durable inserts on a fresh (small) index.
    let dir2 = TempDir::with_prefix("hdxbench_dur").expect("temp dir");
    let mut idx2 = open(dir2.path(), false);
    let t_dur = timed(|| {
        for i in 0..K_DUR {
            idx2.save(key(i), i).expect("save");
            idx2.sync().expect("sync");
        }
    });
    drop(idx2);
    drop(dir2);

    vec![t_insert, t_sync, t_dur, t_hit, t_miss, t_reopen]
}

fn print_table(rows: &[&str], cols: &[(&str, Vec<Duration>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 12usize;

    println!(
        "\n=== digest index: direct-IO (HdxIndex) vs mmap (HdxIndexMmap) (ms; lower is better) ==="
    );
    println!("legend: hdx-buf = HdxIndex on buffered File (direct IO + bucket cache); hdx-mmap = HdxIndex on mmap file (same cache, mmap'd); mmap-cf = HdxIndexMmap (cache-free zero-copy). ops per row: insert/load_hit/load_miss/reopen_load = {N}, sync_bulk = 1, insert_dur = {K_DUR} (a sync each = 2 fsync/msync barriers). test-cfg bloom is 64 KB; run on Linux/SSD for the durability rows.");

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

/// Compare the direct-IO `HdxIndex` (buffered + mmap file) against the cache-free `HdxIndexMmap`.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage digest_index_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand digest-index direct-IO vs mmap comparison; run with --ignored --nocapture --test-threads 1"]
fn digest_index_bench() {
    let header = DataHeader::new(0, PackCompression::None, 0);
    let rows = ["insert", "sync_bulk", "insert_dur", "load_hit", "load_miss", "reopen_load"];
    let mut cols: Vec<(&str, Vec<Duration>)> = Vec::new();

    println!("  running hdx-buf ...");
    cols.push((
        "hdx-buf",
        column::<HdxIndex>(|dir, ro| {
            HdxIndex::open_hdx_file_with_backend(
                dir.join("hdx"),
                &header,
                BuildHasherDefault::<FxHasher>::default(),
                ro,
                FileBackend::Buffered,
            )
            .expect("open hdx-buf")
        }),
    ));

    println!("  running hdx-mmap ...");
    cols.push((
        "hdx-mmap",
        column::<HdxIndex>(|dir, ro| {
            HdxIndex::open_hdx_file_with_backend(
                dir.join("hdx"),
                &header,
                BuildHasherDefault::<FxHasher>::default(),
                ro,
                FileBackend::Mmap,
            )
            .expect("open hdx-mmap")
        }),
    ));

    println!("  running mmap-cf ...");
    cols.push((
        "mmap-cf",
        column::<HdxIndexMmap>(|dir, ro| {
            HdxIndexMmap::open_hdx_file(
                dir.join("hdx"),
                &header,
                BuildHasherDefault::<FxHasher>::default(),
                ro,
            )
            .expect("open mmap-cf")
        }),
    ));

    print_table(&rows, &cols);
}
