//! On-demand benchmarks: tndb-b against MDBX (`reth-libmdbx`, opened the way the node opens it).
//!
//! Run them with (they are `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `TNDB_B_BENCH_N` scales the bulk tables (default 1,000,000 entries each) for a quick run.
//!
//! Every column gets a fresh temp dir and the same seeded key and value streams: 8-byte big-endian
//! sequential keys and 32-byte random keys, each with a 100-byte value. One column runs, in order:
//! a bulk sequential insert and a bulk random insert (one transaction each), the file size, point
//! gets that all hit and that all miss, forward and reverse full scans, seeks each followed by a
//! short range, small transactions committed one by one, random updates and deletes over the random
//! table, and a `clear_table` of the sequential table.
//!
//! ## Caveats (printed with the results too)
//!
//! - MDBX opens like `MdbxDatabase::open_with_sync_mode` in tn-storage: a `write_map` environment
//!   with the OS page size (16 KiB on Apple Silicon, 4 KiB on x86_64), so the tndb-b column with
//!   the same page size is the like-for-like one. The geometry is generous (no remap mid-run).
//! - tndb-b `Durable` and MDBX `Durable` make a commit durable the same way: `msync(MS_SYNC)` of
//!   the data (tndb-b: each dirty range; MDBX: the whole used span), then of the meta page, plus an
//!   `fsync` when the file grew. On macOS `msync` and `fsync` do not flush the drive's cache, so
//!   there the durable rows cost far less than on Linux, where `msync(MS_SYNC)` takes the file
//!   system's fsync path. Linux numbers are the ones that decide production questions.
//! - tndb-b `NoSync` and MDBX `SafeNoSync` commits are not durable: their "small commits" rows
//!   measure the commit path without a sync.
//! - The "fdatasync" small-commit rows run the same workload with step 4 of tndb-b's commit
//!   replaced by one `fdatasync` of the whole file (a test-only switch, see
//!   `Writer::data_by_fdatasync`), to compare it with the per-range `msync`. The two modes
//!   alternate in blocks of 100 transactions (A B B A order) on the same table, so neither gains
//!   from running second.

use std::{
    borrow::Cow,
    hint::black_box,
    path::Path,
    time::{Duration, Instant},
};

use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode as MdbxSync,
    WriteFlags,
};

use crate::{tree::test_harness::Rng, Env, EnvOptions, ReadTxn as _, SyncMode};

/// Entries in each bulk table unless `TNDB_B_BENCH_N` says otherwise.
const DEFAULT_N: usize = 1_000_000;
/// Value length of every entry.
const VALUE_LEN: usize = 100;
/// Distinct values cycled through (generating them costs nothing in the timed regions).
const VALUE_POOL: usize = 4096;
/// Small transactions committed one by one.
const SMALL_TXNS: usize = 2000;
/// Puts in each small transaction.
const SMALL_PUTS: usize = 3;
/// Small transactions in each block of one commit mode when the modes alternate (see
/// `run_tndb`).
const SMALL_BLOCK: usize = 100;
const _: () = assert!(SMALL_TXNS.is_multiple_of(SMALL_BLOCK));
/// Seeks in the seek row, each followed by `SEEK_RANGE - 1` steps.
const SEEKS: usize = 100_000;
/// Entries each seek reads.
const SEEK_RANGE: usize = 10;
/// Operations in the mixed row, in transactions of `MIXED_BATCH`.
const MIXED_OPS: usize = 100_000;
/// Operations per transaction in the mixed row.
const MIXED_BATCH: usize = 1000;

/// The seeded inputs every column replays.
struct Data {
    /// Entries in each bulk table.
    n: usize,
    /// The random table's keys, in insertion order.
    rand_keys: Vec<[u8; 32]>,
    /// Keys absent from the random table (also the seek targets).
    miss_keys: Vec<[u8; 32]>,
    /// The values, picked by index modulo the pool.
    values: Vec<[u8; VALUE_LEN]>,
    /// The mixed row: (index into `rand_keys`, delete?).
    mixed: Vec<(usize, bool)>,
}

impl Data {
    fn new(n: usize) -> Self {
        let mut rng = Rng(0x7d1b_b0b5_eed0_0001);
        let mut key = || {
            let mut k = [0u8; 32];
            for chunk in k.chunks_exact_mut(8) {
                chunk.copy_from_slice(&rng.next().to_le_bytes());
            }
            k
        };
        let rand_keys: Vec<[u8; 32]> = (0..n).map(|_| key()).collect();
        let miss_keys: Vec<[u8; 32]> = (0..n).map(|_| key()).collect();
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
        let mixed =
            (0..MIXED_OPS).map(|_| (rng.below(n as u64) as usize, rng.below(3) == 0)).collect();
        Self { n, rand_keys, miss_keys, values, mixed }
    }

    fn value(&self, i: usize) -> &[u8] {
        &self.values[i % VALUE_POOL]
    }
}

/// One measured cell.
#[derive(Clone, Copy)]
enum Cell {
    /// Not measured for this column.
    None,
    /// Wall time of the whole row.
    Total(Duration),
    /// Wall time of the row divided by its operations.
    PerOp(Duration, usize),
    /// A byte count.
    Bytes(u64),
}

impl Cell {
    fn render(self) -> String {
        match self {
            Cell::None => "-".to_string(),
            Cell::Total(d) => fmt_duration(d),
            Cell::PerOp(d, ops) => fmt_duration(d / ops.max(1) as u32),
            Cell::Bytes(b) => format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0)),
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

/// The table's rows, in print order.
#[derive(Clone, Copy)]
enum Row {
    SeqInsert,
    RandInsert,
    FileSize,
    FileUsed,
    GetHit,
    GetMiss,
    ScanForward,
    ScanReverse,
    SeekRange,
    SmallMedian,
    SmallP99,
    SmallTotal,
    FdatasyncMedian,
    FdatasyncP99,
    FdatasyncTotal,
    Mixed,
    Clear,
}

const ROWS: usize = Row::Clear as usize + 1;

fn row_label(row: usize, n: usize) -> String {
    let k = n / 1000;
    match row {
        0 => format!("seq insert {k}k, 1 txn"),
        1 => format!("random insert {k}k, 1 txn"),
        2 => "file size (st_size)".to_string(),
        3 => "file used (pages x P)".to_string(),
        4 => "get hit (per op)".to_string(),
        5 => "get miss (per op)".to_string(),
        6 => format!("scan forward {k}k"),
        7 => format!("scan reverse {k}k"),
        8 => format!("seek + {SEEK_RANGE} entries (per seek)"),
        9 => format!("small txn ({SMALL_PUTS} puts) median"),
        10 => "small txn p99".to_string(),
        11 => format!("small txns total ({SMALL_TXNS})"),
        12 => "  fdatasync: median".to_string(),
        13 => "  fdatasync: p99".to_string(),
        14 => "  fdatasync: total".to_string(),
        15 => format!("mixed {}k upd/del ({MIXED_BATCH}/txn)", MIXED_OPS / 1000),
        _ => format!("clear_table {k}k + commit"),
    }
}

/// One column of results.
struct Column {
    name: String,
    cells: [Cell; ROWS],
}

impl Column {
    fn new(name: &str) -> Self {
        Self { name: name.to_string(), cells: [Cell::None; ROWS] }
    }

    fn set(&mut self, row: Row, cell: Cell) {
        self.cells[row as usize] = cell;
    }

    /// Record per-transaction times as median, p99 and total.
    fn set_commits(&mut self, rows: [Row; 3], mut times: Vec<Duration>) {
        let total = times.iter().sum();
        times.sort_unstable();
        let at = |q: f64| times[((times.len() - 1) as f64 * q).round() as usize];
        self.set(rows[0], Cell::Total(at(0.5)));
        self.set(rows[1], Cell::Total(at(0.99)));
        self.set(rows[2], Cell::Total(total));
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).expect("stat the data file").len()
}

/// Run every row on a fresh tndb-b file.
fn run_tndb(dir: &Path, name: &str, page_size: u32, sync: SyncMode, data: &Data) -> Column {
    let mut col = Column::new(name);
    let path = dir.join("data.tndb-b");
    let options = EnvOptions { page_size, map_reserve: 8 << 30, sync };
    let env = Env::open(&path, options).expect("open tndb-b");
    let n = data.n;

    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    let seq = txn.create_table("seq").expect("create seq");
    for i in 0..n {
        txn.put(&seq, &(i as u64).to_be_bytes(), data.value(i)).expect("put");
    }
    txn.commit().expect("commit");
    col.set(Row::SeqInsert, Cell::Total(start.elapsed()));

    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    let rand = txn.create_table("rand").expect("create rand");
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(&rand, k, data.value(i)).expect("put");
    }
    txn.commit().expect("commit");
    col.set(Row::RandInsert, Cell::Total(start.elapsed()));

    let stat = env.stat();
    col.set(Row::FileSize, Cell::Bytes(file_size(&path)));
    col.set(Row::FileUsed, Cell::Bytes(u64::from(stat.page_count) * u64::from(stat.page_size)));

    let ro = env.begin_ro().expect("begin ro");
    let start = Instant::now();
    let mut bytes = 0;
    for k in &data.rand_keys {
        bytes += ro.get(&rand, k).expect("get").expect("a hit").len();
    }
    col.set(Row::GetHit, Cell::PerOp(start.elapsed(), n));
    black_box(bytes);

    let start = Instant::now();
    for k in &data.miss_keys {
        assert!(ro.get(&rand, k).expect("get").is_none(), "a miss");
    }
    col.set(Row::GetMiss, Cell::PerOp(start.elapsed(), n));

    let start = Instant::now();
    let mut cursor = ro.cursor(&rand).expect("cursor");
    let (mut count, mut bytes) = (0, 0);
    let mut entry = cursor.first().expect("first");
    while let Some((k, v)) = entry {
        count += 1;
        bytes += k.len() + v.len();
        entry = cursor.next().expect("next");
    }
    col.set(Row::ScanForward, Cell::Total(start.elapsed()));
    assert_eq!(count, n);
    black_box(bytes);

    let start = Instant::now();
    let (mut count, mut bytes) = (0, 0);
    let mut entry = cursor.last().expect("last");
    while let Some((k, v)) = entry {
        count += 1;
        bytes += k.len() + v.len();
        entry = cursor.prev().expect("prev");
    }
    col.set(Row::ScanReverse, Cell::Total(start.elapsed()));
    assert_eq!(count, n);
    black_box(bytes);

    let seeks = SEEKS.min(n);
    let start = Instant::now();
    let mut bytes = 0;
    for k in &data.miss_keys[..seeks] {
        let mut entry = cursor.seek(k).expect("seek");
        for _ in 1..SEEK_RANGE {
            let Some((_, v)) = entry else { break };
            bytes += v.len();
            entry = cursor.next().expect("next");
        }
    }
    col.set(Row::SeekRange, Cell::PerOp(start.elapsed(), seeks));
    black_box(bytes);
    drop(ro);

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
    let (mut by_msync, mut by_fdatasync) = (Vec::with_capacity(SMALL_TXNS), Vec::new());
    if sync == SyncMode::Durable {
        // The two ways of making the data durable alternate in blocks, in A B B A order, on the
        // same table, so neither gains a warmer free list or a smaller table by its place.
        for block in 0..2 * SMALL_TXNS / SMALL_BLOCK {
            let fdatasync = matches!(block % 4, 1 | 2);
            env.with_writer(|w| w.data_by_fdatasync = fdatasync);
            let times = if fdatasync { &mut by_fdatasync } else { &mut by_msync };
            for _ in 0..SMALL_BLOCK {
                times.push(small_txn(&env));
            }
        }
        env.with_writer(|w| w.data_by_fdatasync = false);
    } else {
        for _ in 0..SMALL_TXNS {
            by_msync.push(small_txn(&env));
        }
    }
    col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], by_msync);
    if sync == SyncMode::Durable {
        col.set_commits(
            [Row::FdatasyncMedian, Row::FdatasyncP99, Row::FdatasyncTotal],
            by_fdatasync,
        );
    }

    let start = Instant::now();
    for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
        let mut txn = env.begin_rw().expect("begin");
        for (j, &(idx, delete)) in batch.iter().enumerate() {
            let k = &data.rand_keys[idx];
            if delete {
                txn.delete(&rand, k).expect("delete");
            } else {
                txn.put(&rand, k, data.value(b + j)).expect("put");
            }
        }
        txn.commit().expect("commit");
    }
    col.set(Row::Mixed, Cell::Total(start.elapsed()));

    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    txn.clear_table(&seq).expect("clear");
    txn.commit().expect("commit");
    col.set(Row::Clear, Cell::Total(start.elapsed()));

    env.sync().expect("sync");
    col
}

/// Open MDBX as `MdbxDatabase::open_with_sync_mode` does, with a generous geometry.
fn open_mdbx(dir: &Path, sync_mode: MdbxSync) -> Environment {
    let mut builder = Environment::builder();
    builder
        .set_max_dbs(8)
        .write_map()
        .set_geometry(Geometry {
            size: Some(0..(64usize << 30)),
            growth_step: Some(256 << 20),
            shrink_threshold: Some(0),
            page_size: Some(PageSize::Set(crate::sys::os_page_size().clamp(4096, 65536) as usize)),
        })
        .set_flags(EnvironmentFlags { mode: Mode::ReadWrite { sync_mode }, ..Default::default() });
    builder.open(dir).expect("open mdbx")
}

/// Run every row on a fresh MDBX environment.
fn run_mdbx(dir: &Path, name: &str, sync_mode: MdbxSync, data: &Data) -> Column {
    let mut col = Column::new(name);
    let env = open_mdbx(dir, sync_mode);
    let n = data.n;

    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    let seq = txn.create_db(Some("seq"), DatabaseFlags::default()).expect("create seq").dbi();
    for i in 0..n {
        txn.put(seq, (i as u64).to_be_bytes(), data.value(i), WriteFlags::empty()).expect("put");
    }
    txn.commit().expect("commit");
    col.set(Row::SeqInsert, Cell::Total(start.elapsed()));

    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    let rand = txn.create_db(Some("rand"), DatabaseFlags::default()).expect("create rand").dbi();
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(rand, k, data.value(i), WriteFlags::empty()).expect("put");
    }
    txn.commit().expect("commit");
    col.set(Row::RandInsert, Cell::Total(start.elapsed()));

    let info = env.info().expect("info");
    let page_size = env.stat().expect("stat").page_size();
    col.set(Row::FileSize, Cell::Bytes(file_size(&dir.join("mdbx.dat"))));
    col.set(Row::FileUsed, Cell::Bytes((info.last_pgno() as u64 + 1) * u64::from(page_size)));

    let ro = env.begin_ro_txn().expect("begin ro");
    let start = Instant::now();
    let mut bytes = 0;
    for k in &data.rand_keys {
        bytes += ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").expect("a hit").len();
    }
    col.set(Row::GetHit, Cell::PerOp(start.elapsed(), n));
    black_box(bytes);

    let start = Instant::now();
    for k in &data.miss_keys {
        assert!(ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").is_none(), "a miss");
    }
    col.set(Row::GetMiss, Cell::PerOp(start.elapsed(), n));

    type Kv<'a> = Option<(Cow<'a, [u8]>, Cow<'a, [u8]>)>;
    let start = Instant::now();
    let mut cursor = ro.cursor(rand).expect("cursor");
    let (mut count, mut bytes) = (0, 0);
    let mut entry: Kv<'_> = cursor.first().expect("first");
    while let Some((k, v)) = entry {
        count += 1;
        bytes += k.len() + v.len();
        entry = cursor.next().expect("next");
    }
    col.set(Row::ScanForward, Cell::Total(start.elapsed()));
    assert_eq!(count, n);
    black_box(bytes);

    let start = Instant::now();
    let (mut count, mut bytes) = (0, 0);
    let mut entry: Kv<'_> = cursor.last().expect("last");
    while let Some((k, v)) = entry {
        count += 1;
        bytes += k.len() + v.len();
        entry = cursor.prev().expect("prev");
    }
    col.set(Row::ScanReverse, Cell::Total(start.elapsed()));
    assert_eq!(count, n);
    black_box(bytes);

    let seeks = SEEKS.min(n);
    let start = Instant::now();
    let mut bytes = 0;
    for k in &data.miss_keys[..seeks] {
        let mut entry: Kv<'_> = cursor.set_range(k).expect("seek");
        for _ in 1..SEEK_RANGE {
            let Some((_, v)) = entry else { break };
            bytes += v.len();
            entry = cursor.next().expect("next");
        }
    }
    col.set(Row::SeekRange, Cell::PerOp(start.elapsed(), seeks));
    black_box(bytes);
    drop(cursor);
    drop(ro);

    let txn = env.begin_rw_txn().expect("begin");
    let small = txn.create_db(Some("small"), DatabaseFlags::default()).expect("create").dbi();
    txn.commit().expect("commit");
    let mut times = Vec::with_capacity(SMALL_TXNS);
    let mut next_key = 0u64;
    for _ in 0..SMALL_TXNS {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        for _ in 0..SMALL_PUTS {
            txn.put(
                small,
                next_key.to_be_bytes(),
                data.value(next_key as usize),
                WriteFlags::empty(),
            )
            .expect("put");
            next_key += 1;
        }
        txn.commit().expect("commit");
        times.push(start.elapsed());
    }
    col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], times);

    let start = Instant::now();
    for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
        let txn = env.begin_rw_txn().expect("begin");
        for (j, &(idx, delete)) in batch.iter().enumerate() {
            let k = &data.rand_keys[idx];
            if delete {
                txn.del(rand, k, None).expect("delete");
            } else {
                txn.put(rand, k, data.value(b + j), WriteFlags::empty()).expect("put");
            }
        }
        txn.commit().expect("commit");
    }
    col.set(Row::Mixed, Cell::Total(start.elapsed()));

    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    txn.clear_db(seq).expect("clear");
    txn.commit().expect("commit");
    col.set(Row::Clear, Cell::Total(start.elapsed()));
    col
}

fn print_table(columns: &[Column], n: usize) {
    let labels: Vec<String> = (0..ROWS).map(|r| row_label(r, n)).collect();
    let label_w = labels.iter().map(String::len).max().unwrap_or(0);
    let widths: Vec<usize> = columns
        .iter()
        .map(|c| {
            let cells = c.cells.iter().map(|cell| cell.render().len());
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
    for (r, label) in labels.iter().enumerate() {
        let mut line = format!("| {label:label_w$} |");
        for (c, w) in columns.iter().zip(&widths) {
            line.push_str(&format!(" {:>w$} |", c.cells[r].render()));
        }
        println!("{line}");
    }
}

/// Compare tndb-b with MDBX across the benchmark rows and print one side-by-side table.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand performance comparison; run with --release --ignored --nocapture --test-threads 1"]
fn tndb_b_vs_mdbx() {
    let n = std::env::var("TNDB_B_BENCH_N")
        .ok()
        .map(|v| v.parse().expect("TNDB_B_BENCH_N is a number"))
        .unwrap_or(DEFAULT_N);
    let tmp = tempfile::tempdir().expect("temp dir");
    println!("bench temp dir: {}", tmp.path().display());
    let data = Data::new(n);
    let dir = |name: &str| {
        let d = tmp.path().join(name);
        std::fs::create_dir(&d).expect("column dir");
        d
    };
    let columns = [
        run_tndb(&dir("t4d"), "tndb-b 4K Durable", 4096, SyncMode::Durable, &data),
        run_tndb(&dir("t16d"), "tndb-b 16K Durable", 16384, SyncMode::Durable, &data),
        run_tndb(&dir("t4n"), "tndb-b 4K NoSync", 4096, SyncMode::NoSync, &data),
        run_mdbx(&dir("md"), "MDBX Durable", MdbxSync::Durable, &data),
        run_mdbx(&dir("mn"), "MDBX SafeNoSync", MdbxSync::SafeNoSync, &data),
    ];
    println!(
        "\ntndb-b vs MDBX: {n} entries per bulk table, {VALUE_LEN}-byte values, MDBX page {} B\n",
        crate::sys::os_page_size().clamp(4096, 65536)
    );
    print_table(&columns, n);
    println!(
        "\nNotes: NoSync/SafeNoSync commits are not durable. On macOS msync/fsync do not flush the \
         drive cache, so durable rows cost far less than on Linux. The fdatasync rows replace \
         tndb-b's per-range msync (commit step 4) with one fdatasync of the whole file."
    );
}
