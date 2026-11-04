//! On-demand benchmark of two engine choices against their test-only switches: step 4's data sync
//! and merging on delete.
//!
//! Run it with (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tn-lmdb tn_lmdb_sync_and_merge -- --ignored --nocapture --test-threads 1
//! ```
//!
//! Every column gets a fresh temp dir and the same seeded streams (32-byte random keys, 100-byte
//! values), and runs two row groups:
//!
//! - **small:** `SMALL_TXNS` transactions of `SMALL_PUTS` sequential puts, each committed and
//!   timed, in each of two modes: step 4's own choice (one `msync` when the dirty pages form one
//!   range, else one `fdatasync` of the file), and step 4 forced to one `fdatasync` for every
//!   commit (the test-only `Writer::data_by_fdatasync`; the "fdatasync" rows). The modes differ
//!   only on the commits whose pages form one range. They alternate in blocks of `SMALL_BLOCK`
//!   transactions (A B B A order) on the same table, so neither gains a warmer free list or a
//!   smaller table by its place. A `NoSync` commit has no step 4, so its column runs the first mode
//!   only.
//! - **churn:** `CHURN_ROUNDS` rounds over a table of N/4 random keys, each a fill (one
//!   transaction: the first puts all the keys, a later one re-inserts the keys the last round
//!   deleted) and a delete of 90% of the keys at random (one transaction), timed as a whole; then,
//!   in bytes, the pages of the table's tree (`Env::check`), the file's used span (pages times the
//!   page size, from `Env::stat`) and its `st_size`. It runs twice, each time in a fresh
//!   environment: with merging, and without (the test-only `Writer::no_merge`; the "churn (no
//!   merge)" rows), to show what merging saves.
//!
//! The benchmark is a test, so it is compiled with the test configuration (`cfg(test)`) even in
//! a release build: test-only code must stay off the timed paths there. The test-only counters
//! exist only in debug builds, and a test-only knob costs at most a branch on a field.
//!
//! ## Selecting what runs
//!
//! - `TN_LMDB_BENCH_N`: the churn table holds N/4 keys (default N = 1,000,000).
//! - `TN_LMDB_BENCH_ENGINES` picks the columns, comma-separated (default all; printed in this
//!   order): `t4d` (4 KiB pages, `Durable`), `t16d` (16 KiB, `Durable`), `t4n` (4 KiB, `NoSync`).
//! - `TN_LMDB_BENCH_ROWS` picks the row groups, comma-separated (default both): `small`, `churn`.
//!
//! ## The stamp probe
//!
//! `stamp_probe`, also `#[ignore]`d, prices commit step 3 against its thread count: the
//! `IoStats::stamp_ns` of durable commits that each replace every value of a table, so step 3
//! stamps about 100k private 4 KiB tree pages, at 1 to 12 threads (`Writer::stamp_threads`), for
//! full leaves (a) and for leaves about a third free (c), whose free gap the page CRC skips; and
//! one thread's `crc::crc32` rate over a cache-resident and a DRAM-resident buffer (b) (see the
//! test's docs):
//!
//! ```text
//! cargo test --release -p tn-lmdb stamp_probe -- --ignored --nocapture --test-threads 1
//! ```

use std::{
    path::Path,
    time::{Duration, Instant},
};

use crate::{
    crc, page,
    tree::{test_harness::Rng, PageRead as _},
    Env, EnvOptions, SyncMode,
};

/// N unless `TN_LMDB_BENCH_N` says otherwise.
const DEFAULT_N: usize = 1_000_000;
/// Value length of every entry.
const VALUE_LEN: usize = 100;
/// Distinct values cycled through (generating them costs nothing in the timed regions).
const VALUE_POOL: usize = 4096;
/// Small transactions committed one by one, in each mode.
const SMALL_TXNS: usize = 2000;
/// Puts in each small transaction.
const SMALL_PUTS: usize = 3;
/// Small transactions in each block of one mode.
const SMALL_BLOCK: usize = 100;
const _: () = assert!(SMALL_TXNS.is_multiple_of(SMALL_BLOCK));
/// Rounds of fill and delete in the churn rows.
const CHURN_ROUNDS: usize = 10;
/// The churn rows' table holds `n / CHURN_DIV` keys when full.
const CHURN_DIV: usize = 4;

/// The columns `TN_LMDB_BENCH_ENGINES` accepts, in print order: (selector, title, page size, sync
/// mode).
const ENGINES: [(&str, &str, u32, SyncMode); 3] = [
    ("t4d", "tn-lmdb 4K Durable", 4096, SyncMode::Durable),
    ("t16d", "tn-lmdb 16K Durable", 16384, SyncMode::Durable),
    ("t4n", "tn-lmdb 4K NoSync", 4096, SyncMode::NoSync),
];

/// The row groups `TN_LMDB_BENCH_ROWS` accepts.
const GROUPS: [&str; 2] = ["small", "churn"];

/// The seeded inputs every column replays.
struct Data {
    /// The churn table's keys.
    keys: Vec<[u8; 32]>,
    /// The values, picked by index modulo the pool.
    values: Vec<[u8; VALUE_LEN]>,
    /// The churn rounds' delete orders: `CHURN_ROUNDS` permutations of `0..keys.len()`, one after
    /// the other (see [`Data::order`]).
    orders: Vec<u32>,
}

/// A seeded random permutation of `0..n`.
fn shuffled(rng: &mut Rng, n: usize) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n as u32).collect();
    for i in (1..n).rev() {
        v.swap(i, rng.below(i as u64 + 1) as usize);
    }
    v
}

/// The keys a delete of 90% of `count` keys removes.
fn deleted(count: usize) -> usize {
    count - count / 10
}

impl Data {
    fn new(n: usize) -> Self {
        let churn = n / CHURN_DIV;
        let mut rng = Rng(0x7d1b_b0b5_eed0_0001);
        let keys = (0..churn)
            .map(|_| {
                let mut k = [0u8; 32];
                for chunk in k.chunks_exact_mut(8) {
                    chunk.copy_from_slice(&rng.next().to_le_bytes());
                }
                k
            })
            .collect();
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
        let mut rng = Rng(0x5eed_0000_0000_0003);
        let orders = (0..CHURN_ROUNDS).flat_map(|_| shuffled(&mut rng, churn)).collect();
        Self { keys, values, orders }
    }

    fn value(&self, i: usize) -> &[u8] {
        &self.values[i % VALUE_POOL]
    }

    /// Churn round `r`'s order: a permutation of `0..keys.len()`, whose first
    /// `deleted(keys.len())` the round deletes.
    fn order(&self, r: usize) -> &[u32] {
        let n = self.keys.len();
        &self.orders[r * n..][..n]
    }
}

/// The table's rows, in print order.
#[derive(Clone, Copy)]
enum Row {
    SmallMedian,
    SmallP99,
    SmallTotal,
    FdatasyncMedian,
    FdatasyncP99,
    FdatasyncTotal,
    ChurnTime,
    ChurnTable,
    ChurnUsed,
    ChurnSize,
    NoMergeTime,
    NoMergeTable,
    NoMergeUsed,
    NoMergeSize,
}

const ROWS: usize = Row::NoMergeSize as usize + 1;

impl Row {
    /// Every row, in print order.
    const ALL: [Row; ROWS] = [
        Row::SmallMedian,
        Row::SmallP99,
        Row::SmallTotal,
        Row::FdatasyncMedian,
        Row::FdatasyncP99,
        Row::FdatasyncTotal,
        Row::ChurnTime,
        Row::ChurnTable,
        Row::ChurnUsed,
        Row::ChurnSize,
        Row::NoMergeTime,
        Row::NoMergeTable,
        Row::NoMergeUsed,
        Row::NoMergeSize,
    ];

    /// The row's label, for a churn table of `churn` keys.
    fn label(self, churn: usize) -> String {
        match self {
            Row::SmallMedian => format!("small txn ({SMALL_PUTS} puts) median"),
            Row::SmallP99 => "small txn p99".to_string(),
            Row::SmallTotal => format!("small txns total ({SMALL_TXNS})"),
            Row::FdatasyncMedian => "  fdatasync: median".to_string(),
            Row::FdatasyncP99 => "  fdatasync: p99".to_string(),
            Row::FdatasyncTotal => "  fdatasync: total".to_string(),
            Row::ChurnTime => {
                format!("churn {CHURN_ROUNDS} x fill + del 90% of {}k", churn / 1000)
            }
            Row::NoMergeTime => "churn (no merge)".to_string(),
            Row::ChurnTable | Row::NoMergeTable => "  table pages x P".to_string(),
            Row::ChurnUsed | Row::NoMergeUsed => "  file used (pages x P)".to_string(),
            Row::ChurnSize | Row::NoMergeSize => "  file size (st_size)".to_string(),
        }
    }
}

fn fmt_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} us", ns as f64 / 1e3)
    } else if ns < 10_000_000_000 {
        format!("{:.1} ms", ns as f64 / 1e6)
    } else {
        format!("{:.2} s", ns as f64 / 1e9)
    }
}

fn fmt_bytes(b: u64) -> String {
    format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0))
}

/// One column of results, each cell rendered (`None`: not measured).
struct Column {
    name: &'static str,
    cells: [Option<String>; ROWS],
}

impl Column {
    fn set(&mut self, row: Row, cell: String) {
        self.cells[row as usize] = Some(cell);
    }

    /// Record per-transaction times as median, p99 and total.
    fn set_commits(&mut self, rows: [Row; 3], mut times: Vec<Duration>) {
        let total = times.iter().sum();
        times.sort_unstable();
        let at = |q: f64| times[((times.len() - 1) as f64 * q).round() as usize];
        self.set(rows[0], fmt_duration(at(0.5)));
        self.set(rows[1], fmt_duration(at(0.99)));
        self.set(rows[2], fmt_duration(total));
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).expect("stat the data file").len()
}

/// The small-transaction rows in a fresh environment at `path` (see the module docs).
fn small(path: &Path, options: EnvOptions, data: &Data, col: &mut Column) {
    let env = Env::open(path, options).expect("open tn-lmdb");
    let mut txn = env.begin_rw().expect("begin");
    let small = txn.create_table("small").expect("create small");
    txn.commit().expect("commit");
    let mut next_key = 0u64;
    let mut small_txn = |env: &Env| {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for _ in 0..SMALL_PUTS {
            txn.put(&small, &next_key.to_be_bytes(), data.value(next_key as usize)).expect("put");
            next_key += 1;
        }
        txn.commit().expect("commit");
        start.elapsed()
    };
    let durable = options.sync == SyncMode::Durable;
    let (mut own, mut by_fdatasync) = (Vec::with_capacity(SMALL_TXNS), Vec::new());
    if durable {
        for block in 0..2 * SMALL_TXNS / SMALL_BLOCK {
            let fdatasync = matches!(block % 4, 1 | 2);
            env.with_writer(|w| w.data_by_fdatasync = fdatasync);
            let times = if fdatasync { &mut by_fdatasync } else { &mut own };
            for _ in 0..SMALL_BLOCK {
                times.push(small_txn(&env));
            }
        }
        env.with_writer(|w| w.data_by_fdatasync = false);
    } else {
        for _ in 0..SMALL_TXNS {
            own.push(small_txn(&env));
        }
    }
    col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], own);
    if durable {
        col.set_commits(
            [Row::FdatasyncMedian, Row::FdatasyncP99, Row::FdatasyncTotal],
            by_fdatasync,
        );
    }
    env.sync().expect("sync");
}

/// One churn run in a fresh environment at `path`, merging or not (see the module docs).
fn churn(
    path: &Path,
    options: EnvOptions,
    data: &Data,
    merge: bool,
    rows: [Row; 4],
    col: &mut Column,
) {
    let env = Env::open(path, options).expect("open tn-lmdb");
    env.with_writer(|w| w.no_merge = !merge);
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("churn").expect("create churn");
    txn.commit().expect("commit");
    let gone = deleted(data.keys.len());
    let start = Instant::now();
    for r in 0..CHURN_ROUNDS {
        // The fill puts every key the table lacks.
        let mut txn = env.begin_rw().expect("begin");
        let mut put = |i: usize| txn.put(&table, &data.keys[i], data.value(i)).expect("put");
        match r {
            0 => (0..data.keys.len()).for_each(&mut put),
            _ => data.order(r - 1)[..gone].iter().for_each(|&i| put(i as usize)),
        }
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("begin");
        for &i in &data.order(r)[..gone] {
            assert!(txn.delete(&table, &data.keys[i as usize]).expect("delete"), "a hit");
        }
        txn.commit().expect("commit");
    }
    col.set(rows[0], fmt_duration(start.elapsed()));
    let stat = env.stat();
    let page = u64::from(stat.page_size);
    let table = env.check().expect("check").tables["churn"].pages;
    col.set(rows[1], fmt_bytes(table * page));
    col.set(rows[2], fmt_bytes(u64::from(stat.page_count) * page));
    col.set(rows[3], fmt_bytes(file_size(path)));
    env.sync().expect("sync");
}

/// The selected names of a selector variable, as flags over `known` (default all).
fn pick(var: &str, known: &[&str]) -> Vec<bool> {
    let Some(v) = std::env::var(var).ok().filter(|v| !v.trim().is_empty()) else {
        return vec![true; known.len()];
    };
    let picked: Vec<&str> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
    for p in &picked {
        assert!(known.contains(p), "{var}: {p:?} is not one of {known:?}");
    }
    known.iter().map(|k| picked.contains(k)).collect()
}

fn print_table(columns: &[Column], churn: usize) {
    let shown: Vec<Row> = Row::ALL
        .into_iter()
        .filter(|&r| columns.iter().any(|c| c.cells[r as usize].is_some()))
        .collect();
    let labels: Vec<String> = shown.iter().map(|r| r.label(churn)).collect();
    let label_w = labels.iter().map(String::len).max().unwrap_or(0);
    let render = |cell: &Option<String>| cell.clone().unwrap_or_else(|| "-".to_string());
    let widths: Vec<usize> = columns
        .iter()
        .map(|c| {
            let cells = c.cells.iter().map(|cell| render(cell).len());
            cells.chain([c.name.len()]).max().unwrap_or(0)
        })
        .collect();
    let mut header = format!("| {:label_w$} |", "");
    let mut rule = format!("|{}|", "-".repeat(label_w + 2));
    for (c, w) in columns.iter().zip(&widths) {
        header.push_str(&format!(" {:>w$} |", c.name));
        rule.push_str(&format!("{}:|", "-".repeat(w + 1)));
    }
    println!("{header}\n{rule}");
    for (row, label) in shown.iter().zip(&labels) {
        let mut line = format!("| {label:label_w$} |");
        for (c, w) in columns.iter().zip(&widths) {
            line.push_str(&format!(" {:>w$} |", render(&c.cells[*row as usize])));
        }
        println!("{line}");
    }
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

/// Time step 4's own choice against a forced `fdatasync`, and deletes with merging against
/// without, and print one table.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-lmdb tn_lmdb_sync_and_merge -- --ignored --nocapture --test-threads
/// 1`.
#[test]
#[ignore = "on-demand performance comparison; run with --release --ignored --nocapture --test-threads 1"]
fn tn_lmdb_sync_and_merge() {
    let n = std::env::var("TN_LMDB_BENCH_N")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map_or(DEFAULT_N, |v| v.parse().unwrap_or_else(|_| panic!("TN_LMDB_BENCH_N = {v:?}")));
    let engines = pick("TN_LMDB_BENCH_ENGINES", &ENGINES.map(|(sel, ..)| sel));
    let [run_small, run_churn] = pick("TN_LMDB_BENCH_ROWS", &GROUPS)[..] else {
        unreachable!("one flag per group")
    };
    let tmp = tempfile::tempdir().expect("temp dir");
    announce_temp_dir(tmp.path());
    let data = Data::new(n);
    let churn_rows = [
        (true, [Row::ChurnTime, Row::ChurnTable, Row::ChurnUsed, Row::ChurnSize]),
        (false, [Row::NoMergeTime, Row::NoMergeTable, Row::NoMergeUsed, Row::NoMergeSize]),
    ];
    let columns: Vec<Column> = ENGINES
        .iter()
        .zip(&engines)
        .filter(|(_, &on)| on)
        .map(|(&(dir_name, name, page_size, sync), _)| {
            let dir = tmp.path().join(dir_name);
            std::fs::create_dir(&dir).expect("column dir");
            let options =
                EnvOptions { page_size, map_reserve: 8 << 30, sync, ..EnvOptions::default() };
            let mut col = Column { name, cells: Default::default() };
            if run_small {
                small(&dir.join("small.tn-lmdb"), options, &data, &mut col);
            }
            if run_churn {
                for (merge, rows) in churn_rows {
                    let name = if merge { "churn.tn-lmdb" } else { "churn-no-merge.tn-lmdb" };
                    churn(&dir.join(name), options, &data, merge, rows, &mut col);
                }
            }
            col
        })
        .collect();
    println!(
        "\ntn-lmdb step 4 and merging: small txns of {SMALL_PUTS} puts, a churn table of {} keys, \
         {VALUE_LEN}-byte values\n",
        data.keys.len()
    );
    print_table(&columns, data.keys.len());
    println!(
        "\nNotes: step 4 syncs a commit's data by one msync when its dirty pages form one range, \
         else by one fdatasync of the file. The fdatasync rows force the fdatasync for every \
         commit, so they differ from step 4's own choice only on commits whose pages form one \
         range; the two modes alternate in blocks of {SMALL_BLOCK} (A B B A). NoSync commits are \
         not durable and take no step 4. The churn (no merge) rows run with merging off (a \
         test-only switch). On macOS msync and fdatasync do not flush the drive cache, so durable \
         rows cost far less than on Linux."
    );
}

/// The stamp probe's keys: 8-byte, sequential, each with a [`PROBE_VALUE`]-byte value, four to a
/// 4 KiB leaf, so a commit that replaces every value writes about 100k tree pages (~392 MiB).
const PROBE_KEYS: u64 = 400_000;
/// The longest value of an 8-byte key whose entry is a quarter of a 4 KiB leaf
/// (`Geometry::max_entry_value`), so four fill a leaf.
const PROBE_VALUE: usize = 999;
/// The gapped shape's values: four to a leaf leave 1436 bytes (35.1%) of each 4 KiB leaf a free
/// gap, about the gap of a long-running writer's pages.
const PROBE_GAPPED_VALUE: usize = 640;
/// Distinct values the probe cycles through, shifted by one at each commit, so every commit
/// changes every value.
const PROBE_POOL: usize = 61;
/// The thread counts the probe stamps on (`Writer::stamp_threads`; 1 is the serial loop).
const PROBE_THREADS: [usize; 7] = [1, 2, 3, 4, 6, 8, 12];
/// Rounds of one commit per thread count, each round's order the previous one's rotated by one.
const PROBE_ROUNDS: usize = PROBE_THREADS.len();
/// Commits before the rounds, so the measured ones reuse freed pages as a steady writer does.
const PROBE_WARMUP: usize = 2;

/// The median and the least of `v`.
fn median_min(v: &mut [u64]) -> (u64, u64) {
    v.sort_unstable();
    (v[v.len() / 2], v[0])
}

/// Decimal GB a second for `bytes` in `ns`.
fn gb_s(bytes: u64, ns: u64) -> f64 {
    bytes as f64 / ns.max(1) as f64
}

/// Single-thread `crc::crc32` over `buf`, `reps` times a pass, best and median of 7 passes, in
/// GB/s.
fn crc_rate(buf: &[u8], reps: usize) -> (f64, f64) {
    let mut ns: Vec<u64> = (0..8)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..reps {
                std::hint::black_box(crc::crc32(std::hint::black_box(buf)));
            }
            start.elapsed().as_nanos() as u64
        })
        .skip(1)
        .collect();
    let (median, min) = median_min(&mut ns);
    let bytes = (buf.len() * reps) as u64;
    (gb_s(bytes, min), gb_s(bytes, median))
}

/// What one shape of the stamp probe measured ([`stamp_rounds`]).
struct StampRounds {
    /// Each thread count's step 3 times, in [`PROBE_THREADS`] order.
    ns: [Vec<u64>; PROBE_THREADS.len()],
    /// The median, least and most pages a measured commit wrote.
    pages: (u64, u64, u64),
    /// The table's tree pages after the last commit, and the bytes of their free gaps.
    gaps: (u64, u64),
    /// Each measured commit's step 1 free-list save time (`IoStats::save_ns`).
    save_ns: Vec<u64>,
    /// Each measured commit's free-list save rounds.
    save_rounds: Vec<u64>,
}

/// [`PROBE_POOL`] distinct values of `len` bytes from `rng`.
fn probe_values(rng: &mut Rng, len: usize) -> Vec<Vec<u8>> {
    (0..PROBE_POOL).map(|_| (0..len).map(|_| rng.next() as u8).collect()).collect()
}

/// The tree pages of table `name` in `env`'s newest state, and the bytes of their free gaps
/// (`page::free_gap`).
fn tree_gaps(env: &Env, name: &str) -> (u64, u64) {
    let snapshot = env.inner().current();
    let record = crate::catalog::get(&*snapshot, &snapshot.meta.catalog, name)
        .expect("get")
        .expect("the table");
    let (mut pages, mut gaps) = (0, 0);
    let mut stack = vec![record.root.root];
    while let Some(pgno) = stack.pop() {
        let page = snapshot.page(pgno).expect("a tree page");
        pages += 1;
        gaps += page::free_gap(page).map_or(0, |gap| gap.len()) as u64;
        if page::kind(page).expect("kind") == page::Kind::Branch {
            for i in 0..page::nkeys(page) {
                stack.push(page::branch_child(page, i).expect("child").0);
            }
        }
    }
    (pages, gaps)
}

/// Commit step 3's times against the thread count, for one shape of tree pages: a durable
/// environment of 4 KiB pages loads [`PROBE_KEYS`] keys with [`PROBE_VALUE`]-byte values (four to
/// a leaf), then each commit replaces every value with one of `value` bytes, so step 3 stamps
/// about 100k private tree pages, written just before by copy-on-write. The [`PROBE_WARMUP`]
/// commits come first (the first of them shrinks every value when `value` is smaller); each
/// measured commit's `IoStats::stamp_ns` is read at a thread count set by `Writer::stamp_threads`,
/// every count once a round in an order rotated each round.
fn stamp_rounds(rng: &mut Rng, value: usize) -> StampRounds {
    let tmp = tempfile::tempdir().expect("temp dir");
    announce_temp_dir(tmp.path());
    let options = EnvOptions {
        page_size: 4096,
        map_reserve: 8 << 30,
        sync: SyncMode::Durable,
        ..EnvOptions::default()
    };
    let env = Env::open(tmp.path().join("stamp.tn-lmdb"), options).expect("open tn-lmdb");
    let load = probe_values(rng, PROBE_VALUE);
    let values = if value == PROBE_VALUE { load.clone() } else { probe_values(rng, value) };
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("stamp").expect("create stamp");
    for k in 0..PROBE_KEYS {
        txn.append(&table, &k.to_be_bytes(), &load[k as usize % PROBE_POOL]).expect("append");
    }
    txn.commit().expect("commit");
    // One commit replacing every value with step 3 on `threads` threads: its stamp time and the
    // pages it wrote.
    let mut shift = 0;
    let mut save_ns = Vec::with_capacity(PROBE_ROUNDS * PROBE_THREADS.len());
    let mut save_rounds = Vec::with_capacity(PROBE_ROUNDS * PROBE_THREADS.len());
    let mut replace_all = |threads: usize| -> (u64, u64, u64, u64) {
        shift += 1;
        env.with_writer(|w| w.stamp_threads = Some(threads));
        let mut txn = env.begin_rw().expect("begin");
        for k in 0..PROBE_KEYS {
            let value = &values[(k as usize + shift) % PROBE_POOL];
            txn.put(&table, &k.to_be_bytes(), value).expect("put");
        }
        let before = env.io_stats();
        txn.commit().expect("commit");
        let d = env.io_stats().since(&before);
        assert_eq!(env.with_writer(|w| w.stamped_on), threads, "step 3's threads");
        (d.stamp_ns, d.pages_synced, d.save_ns, d.free_list_save_rounds)
    };
    for _ in 0..PROBE_WARMUP {
        replace_all(1);
    }
    let mut ns = [(); PROBE_THREADS.len()].map(|()| Vec::with_capacity(PROBE_ROUNDS));
    let mut pages = Vec::with_capacity(PROBE_ROUNDS * PROBE_THREADS.len());
    for round in 0..PROBE_ROUNDS {
        for i in 0..PROBE_THREADS.len() {
            let at = (i + round) % PROBE_THREADS.len();
            let (stamp, written, save, rounds) = replace_all(PROBE_THREADS[at]);
            ns[at].push(stamp);
            pages.push(written);
            save_ns.push(save);
            save_rounds.push(rounds);
        }
    }
    env.with_writer(|w| w.stamp_threads = None);
    let (median, _) = median_min(&mut pages);
    StampRounds {
        ns,
        pages: (median, pages[0], pages[pages.len() - 1]),
        gaps: tree_gaps(&env, "stamp"),
        save_ns,
        save_rounds,
    }
}

/// Print a [`StampRounds`]' commit step 1 free-list save times (`IoStats::save_ns`), apart
/// from the step 3 table.
fn print_save_line(rounds: &mut StampRounds) {
    let (median, min) = median_min(&mut rounds.save_ns);
    let (round_median, _) = median_min(&mut rounds.save_rounds);
    println!(
        "\ncommit step 1's free-list save (IoStats::save_ns): median {:.2} ms, min {:.2} ms, \
         {round_median} rounds a commit (median)",
        median as f64 / 1e6,
        min as f64 / 1e6,
    );
}

/// Print a [`StampRounds`]' table: each thread count's median and least step 3 time, for `bytes`
/// a commit.
fn print_stamp_table(rounds: &mut StampRounds, bytes: u64) {
    println!("| threads | median | min | GB/s (median) | GB/s (min) | median vs 1 thread |");
    println!("|---:|---:|---:|---:|---:|---:|");
    let serial = median_min(&mut rounds.ns[0]).0;
    for (threads, ns) in PROBE_THREADS.iter().zip(&mut rounds.ns) {
        let (median, min) = median_min(ns);
        println!(
            "| {threads} | {} | {} | {:.2} | {:.2} | {:.2}x |",
            fmt_duration(Duration::from_nanos(median)),
            fmt_duration(Duration::from_nanos(min)),
            gb_s(bytes, median),
            gb_s(bytes, min),
            serial as f64 / median.max(1) as f64,
        );
    }
}

/// Commit step 3's time against its thread count for two shapes of tree pages, and one thread's
/// CRC rate in and out of the caches, and print the three tables.
///
/// (a) Full leaves ([`stamp_rounds`] with [`PROBE_VALUE`]-byte values, four to a leaf): a page's
/// free gap is about 0, so its CRC covers the whole page. (c) Leaves about a third free
/// ([`PROBE_GAPPED_VALUE`]-byte values, four to a leaf), the gap no page CRC reads. Each commit
/// stamps about 100k private tree pages, written just before by copy-on-write (DRAM-resident on a
/// machine whose last-level cache is smaller). (b) `crc::crc32` on one thread over a 1 MiB
/// buffer, repeated (cache-resident), and over a 400 MiB one (DRAM-resident).
///
/// On-demand probe (kept out of the default suite). Run with:
/// `cargo test --release -p tn-lmdb stamp_probe -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand probe; run with --release --ignored --nocapture --test-threads 1"]
fn stamp_probe() {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let mut rng = Rng(0x57a3_b000_0000_0001);
    let mut full = stamp_rounds(&mut rng, PROBE_VALUE);
    let (page_median, lo, hi) = full.pages;
    let bytes = page_median * 4096;
    println!(
        "\nstamp probe (a): commit step 3 (IoStats::stamp_ns) of commits replacing every value of \
         {PROBE_KEYS} keys: {page_median} pages a commit ({lo}..{hi}), {}, {PROBE_ROUNDS} rounds; \
         {cpus} CPUs available, stamp::threads picks {} for these bytes\n",
        fmt_bytes(bytes),
        crate::stamp::threads(bytes as usize),
    );
    print_stamp_table(&mut full, bytes);
    print_save_line(&mut full);
    let mut big = vec![0u8; 400 << 20];
    for chunk in big.chunks_exact_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes());
    }
    let (cached_best, cached_median) = crc_rate(&big[..1 << 20], 64);
    let (dram_best, dram_median) = crc_rate(&big, 1);
    drop(big);
    println!("\nstamp probe (b): crc::crc32 on one thread, 7 passes\n");
    println!("| buffer | GB/s (best) | GB/s (median) |");
    println!("|---|---:|---:|");
    println!("| 1 MiB, 64 times a pass (cache-resident) | {cached_best:.2} | {cached_median:.2} |");
    println!("| 400 MiB, once a pass (DRAM-resident) | {dram_best:.2} | {dram_median:.2} |");
    let mut gapped = stamp_rounds(&mut rng, PROBE_GAPPED_VALUE);
    let (page_median, lo, hi) = gapped.pages;
    let (tree_pages, gap_bytes) = gapped.gaps;
    let bytes = page_median * 4096;
    println!(
        "\nstamp probe (c): as (a) with {PROBE_GAPPED_VALUE}-byte values, four to a leaf: the \
         table's {tree_pages} tree pages average a free gap of {:.1}% of a page; {page_median} \
         pages a commit ({lo}..{hi}), {}, {PROBE_ROUNDS} rounds; stamp::threads picks {} for \
         these bytes\n",
        gap_bytes as f64 * 100.0 / (tree_pages * 4096) as f64,
        fmt_bytes(bytes),
        crate::stamp::threads(bytes as usize),
    );
    print_stamp_table(&mut gapped, bytes);
    print_save_line(&mut gapped);
}
