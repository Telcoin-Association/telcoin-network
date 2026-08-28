//! On-demand benchmark comparing the digest-index implementations: the buffered/direct-IO
//! [`HdxIndexDirectIO`](super::index_directio::HdxIndexDirectIO) (raw `File` + in-memory bucket
//! cache) vs the cache-free, zero-copy memory-mapped [`HdxIndex`](super::index::HdxIndex). Three
//! columns:
//! - `hdx-buf`  — `HdxIndexDirectIO` on the buffered `File` backend (the direct-IO version).
//! - `hdx-mmap` — `HdxIndexDirectIO` on the mmap file backend (same cached logic — isolates the
//!   file backend).
//! - `mmap`     — `HdxIndex`, the cache-free, zero-copy index. It defers CRC: each write zeros the
//!   bucket's CRC trailer (a "dirty" marker) and only the dirty buckets are CRC'd at `sync()` (the
//!   WAL/rebuildable regime; reads do not verify a per-op CRC).
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage digest_index_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! It sweeps a set of index sizes `N` (env `HDX_BENCH_N=200000,2000000` overrides; default
//! `[200k, 2M, 8M]`). The default's 8M crosses the buffered `CACHED_BUCKETS = 400_000` cache
//! (~6.4M keys), where the ranking is expected to flip toward the page-cache-backed mmap index —
//! set a smaller env list if RAM/disk-constrained (the 8M column needs a few GB). Rows per size:
//! `insert` (N saves, no sync), `sync_bulk` (one `sync()`), `load_hit` / `load_miss` (N random
//! point lookups, present / absent), `reopen_load` (reopen read-only then N lookups); plus one
//! fixed `insert_dur` probe (K_DUR save+sync pairs).
//!
//! Framing: `sync_bulk`/`insert_dur` measure the *index* durability barrier. If the index is not
//! synced (rebuilt from the data-log WAL on unclean shutdown), those rows are moot and `mmap`'s
//! cheap insert/load is what matters — deferring the CRC off the per-op path to a targeted `sync`
//! (only the dirty, zero-CRC buckets are CRC'd) makes per-op sync (`insert_dur`) cheap, while a
//! full-build `sync_bulk` (nearly all buckets dirty) stays roughly the same.
//!
//! Caveats: under `#[cfg(test)]` the bloom filter is 64 KB (2 MB in prod), so `load_miss` probes
//! more buckets than production — both impls share it, so the comparison is fair. `reopen_load` is
//! cold only in-process (the file stays in the OS page cache). macOS `fsync` is not a full barrier
//! — run on Linux/SSD for the durability rows.

use std::{
    hash::BuildHasherDefault,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_types::B256;

use super::{index::HdxIndex, index_directio::HdxIndexDirectIO};
use crate::archive::{
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, FileBackend, PackCompression},
};

/// Per-op durable inserts (a `sync()` after each — 2 barriers/op; N-independent, kept modest).
const K_DUR: u64 = 2_000;

/// Index sizes to sweep — env `HDX_BENCH_N` (comma list) overrides. Default's 8M crosses the
/// 400k-bucket buffered cache (~6.4M keys); trim via the env if RAM/disk-constrained.
fn n_sizes() -> Vec<u64> {
    let parsed: Vec<u64> = std::env::var("HDX_BENCH_N")
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

/// Per-`n` battery: `[insert, sync_bulk, load_hit, load_miss, reopen_load]`. `open(dir, read_only)`
/// builds the concrete index.
fn size_battery<I: Index<B256, u64>, F: Fn(&Path, bool) -> I>(open: &F, n: u64) -> [Duration; 5] {
    let keys: Vec<B256> = (0..n).map(key).collect();
    let miss: Vec<B256> = (n..2 * n).map(key).collect();
    let order: Vec<usize> = (0..n).map(|m| (mix(m) % n) as usize).collect();

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
fn durable<I: Index<B256, u64>, F: Fn(&Path, bool) -> I>(open: &F) -> Duration {
    let dir = TempDir::with_prefix("hdxbench_dur").expect("temp dir");
    let mut idx = open(dir.path(), false);
    let t = timed(|| {
        for i in 0..K_DUR {
            idx.save(key(i), i).expect("save");
            idx.sync().expect("sync");
        }
    });
    drop(idx);
    drop(dir);
    t
}

/// All rows for one column: `insert_dur`, then per-`n` `[insert, sync_bulk, load_hit, load_miss,
/// reopen_load]`.
fn column<I: Index<B256, u64>, F: Fn(&Path, bool) -> I>(open: F, sizes: &[u64]) -> Vec<Duration> {
    let mut out = vec![durable(&open)];
    for &n in sizes {
        out.extend_from_slice(&size_battery(&open, n));
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

fn open_hdx(
    header: &DataHeader,
    dir: &Path,
    read_only: bool,
    backend: FileBackend,
) -> HdxIndexDirectIO {
    HdxIndexDirectIO::open_hdx_file_with_backend(
        dir.join("hdx"),
        header,
        BuildHasherDefault::<FxHasher>::default(),
        read_only,
        backend,
    )
    .expect("open hdx")
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

fn print_table(rows: &[String], cols: &[(&str, Vec<Duration>)]) {
    let label_w = rows.iter().map(|s| s.len()).max().unwrap_or(0).max("benchmark".len());
    let cell_w = 12usize;

    println!(
        "\n=== digest index: direct-IO (HdxIndexDirectIO) vs mmap (HdxIndex) (ms; lower is better) ==="
    );
    println!("legend: hdx-buf = buffered File + bucket cache; hdx-mmap = same cache on an mmap file; mmap = cache-free mmap, per-op CRC replaced by a zeroed dirty marker, only dirty buckets CRC'd at sync (WAL/rebuildable regime; no per-op CRC on read). insert_dur = {K_DUR} save+sync pairs; per size: insert/load_hit/load_miss/reopen_load = N, sync_bulk = 1. Default N sweep crosses the 400k-bucket buffered cache at 8M. test-cfg bloom is 64 KB; run on Linux/SSD.");

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

/// Compare the direct-IO `HdxIndexDirectIO` (buffered + mmap file) against the cache-free,
/// deferred-CRC `HdxIndex`, swept across index sizes.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage digest_index_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand digest-index direct-IO vs mmap comparison; run with --ignored --nocapture --test-threads 1"]
fn digest_index_bench() {
    let header = DataHeader::new(0, PackCompression::None, 0);
    let sizes = n_sizes();
    let rows = row_labels(&sizes);
    let mut cols: Vec<(&str, Vec<Duration>)> = Vec::new();

    println!("  running hdx-buf ...");
    cols.push((
        "hdx-buf",
        column(|dir, ro| open_hdx(&header, dir, ro, FileBackend::Buffered), &sizes),
    ));
    println!("  running hdx-mmap ...");
    cols.push((
        "hdx-mmap",
        column(|dir, ro| open_hdx(&header, dir, ro, FileBackend::Mmap), &sizes),
    ));
    println!("  running mmap ...");
    cols.push(("mmap", column(|dir, ro| open_mmap(&header, dir, ro), &sizes)));

    print_table(&rows, &cols);
}
