//! On-demand benchmarks: tndb-b against MDBX (`reth-libmdbx`, opened the way the node opens it).
//!
//! Run them with (they are `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
//! ```
//!
//! Every column gets a fresh temp dir and the same seeded key and value streams: 8-byte big-endian
//! sequential keys and 32-byte random keys, each with a 100-byte value. One column runs, in order:
//! a bulk sequential insert and a bulk random insert (one transaction each, timed as a whole and
//! split into the put loop and the commit), the file size, point gets that all hit and that all
//! miss, forward and reverse full scans, seeks each followed by a short range, small transactions
//! committed one by one, random updates and deletes over the random table, a `clear_table` of the
//! sequential table, and then the put loops of the API rows (an append put and a cursor put with
//! and without append, each of N sequential keys into a fresh table of a fresh environment, so
//! they compare with the sequential insert's put loop).
//!
//! The benchmark is a test, so it is compiled with the test configuration (`cfg(test)`) even in
//! a release build: test-only code must stay off the timed paths there. The test-only counters
//! exist only in debug builds, and a test-only knob costs at most a branch on a field.
//!
//! ## Selecting what runs
//!
//! - `TNDB_B_BENCH_N` scales the bulk tables (default 1,000,000 entries each).
//! - `TNDB_B_BENCH_ENGINES` picks the columns, comma-separated (default all; printed in this
//!   order): `t4d` (tndb-b 4K Durable), `t16d` (16K Durable), `t4n` (4K NoSync), `md` (MDBX
//!   Durable), `md4` (MDBX Durable with 4 KiB pages), `mn` (MDBX SafeNoSync).
//! - `TNDB_B_BENCH_ROWS` picks the row groups, comma-separated (default all): `seq`, `rand`,
//!   `get-hit`, `get-miss`, `scan-fwd`, `scan-rev`, `seek`, `small`, `mixed`, `clear`, `append`,
//!   and the aliases `bulk` (`seq,rand`), `get` (both gets) and `scan` (both scans). The file size
//!   rows come with `seq` or `rand`. A group that needs a bulk table (`clear` the sequential one;
//!   the gets, scans, `seek` and `mixed` the random one) builds it untimed when its own group is
//!   not selected. Only rows with a measured cell are printed.
//! - `TNDB_B_BENCH_REPEAT=k` (default 1) runs each selected read row (the gets, the scans and
//!   `seek`) k times back to back and reports the median, so one read row can dominate a profile:
//!   for example `TNDB_B_BENCH_ENGINES=t16d TNDB_B_BENCH_ROWS=get-hit TNDB_B_BENCH_REPEAT=20`. To
//!   profile a put loop or a commit, select `seq` or `rand` and raise `TNDB_B_BENCH_N` instead.
//!
//! ## Caveats (printed with the results too)
//!
//! - MDBX opens like `MdbxDatabase::open_with_sync_mode` in tn-storage: a `write_map` environment
//!   with the OS page size (16 KiB on Apple Silicon, 4 KiB on x86_64), so the tndb-b column with
//!   the same page size is the like-for-like one. The `md4` column opens it the same way with 4 KiB
//!   pages (`PageSize::Set(4096)`): the like-for-like one for `t4d`, and what a Linux x86_64 node
//!   runs, though on Apple Silicon each 4 KiB page sits in a 16 KiB OS page. On x86_64 the two MDBX
//!   columns are the same configuration. The geometry is generous (no remap mid-run).
//! - tndb-b `Durable` and MDBX `Durable` make a commit durable the same way: `msync(MS_SYNC)` of
//!   the data (tndb-b: each dirty range; MDBX: the whole used span), then of the meta page, plus an
//!   `fsync` when the file grew. On macOS `msync` and `fsync` do not flush the drive's cache, so
//!   there the durable rows cost far less than on Linux, where `msync(MS_SYNC)` takes the file
//!   system's fsync path. Linux numbers are the ones that decide production questions.
//! - tndb-b `NoSync` and MDBX `SafeNoSync` commits are not durable: their "small commits" rows and
//!   the bulk "commit" rows measure the commit path without a sync.
//! - A bulk row's "put loop" runs from `begin` to just before `commit` (`begin` and the table's
//!   creation cost next to nothing); its "commit" is the rest. The two add up to the total.
//! - The API rows (append put, cursor put) time the put loop only; their commits match the
//!   sequential insert's. MDBX runs them with `WriteFlags::APPEND` on `Transaction::put` and with
//!   `Cursor::put`, with and without `WriteFlags::APPEND`; tndb-b with `RwTxn::append`,
//!   `RwCursor::put` and `RwCursor::append`.
//! - The "fdatasync" small-commit rows run the same workload with step 4 of tndb-b's commit
//!   replaced by one `fdatasync` of the whole file (a test-only switch, see
//!   `Writer::data_by_fdatasync`), to compare it with the per-range `msync`. The two modes
//!   alternate in blocks of 100 transactions (A B B A order) on the same table, so neither gains
//!   from running second.
//! - With `TNDB_B_BENCH_REPEAT` above 1, a read row reports the median of its runs, not a single
//!   cold-ish run: later runs find the tree in the CPU caches a first run had to fill.

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

use crate::{tree::test_harness::Rng, Env, EnvOptions, ReadTxn as _, SyncMode, Table};

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

/// A selectable group of rows (`TNDB_B_BENCH_ROWS`): one bit of [`Selection::rows`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Group {
    Seq,
    Rand,
    GetHit,
    GetMiss,
    ScanFwd,
    ScanRev,
    Seek,
    Small,
    Mixed,
    Clear,
    Append,
}

/// The names `TNDB_B_BENCH_ROWS` accepts, each with the groups it selects.
const GROUP_NAMES: [(&str, &[Group]); 14] = [
    ("seq", &[Group::Seq]),
    ("rand", &[Group::Rand]),
    ("bulk", &[Group::Seq, Group::Rand]),
    ("get-hit", &[Group::GetHit]),
    ("get-miss", &[Group::GetMiss]),
    ("get", &[Group::GetHit, Group::GetMiss]),
    ("scan-fwd", &[Group::ScanFwd]),
    ("scan-rev", &[Group::ScanRev]),
    ("scan", &[Group::ScanFwd, Group::ScanRev]),
    ("seek", &[Group::Seek]),
    ("small", &[Group::Small]),
    ("mixed", &[Group::Mixed]),
    ("clear", &[Group::Clear]),
    ("append", &[Group::Append]),
];

/// The column a bench run measures.
#[derive(Clone, Copy, Debug)]
enum Engine {
    /// tndb-b with this page size and sync mode.
    Tndb(u32, SyncMode),
    /// MDBX with this sync mode and page size (`None`: the OS page size).
    Mdbx(MdbxSync, Option<usize>),
}

/// The columns `TNDB_B_BENCH_ENGINES` accepts, in print order: (selector, title, engine).
const ENGINES: [(&str, &str, Engine); 6] = [
    ("t4d", "tndb-b 4K Durable", Engine::Tndb(4096, SyncMode::Durable)),
    ("t16d", "tndb-b 16K Durable", Engine::Tndb(16384, SyncMode::Durable)),
    ("t4n", "tndb-b 4K NoSync", Engine::Tndb(4096, SyncMode::NoSync)),
    ("md", "MDBX Durable", Engine::Mdbx(MdbxSync::Durable, None)),
    ("md4", "MDBX 4K Durable", Engine::Mdbx(MdbxSync::Durable, Some(4096))),
    ("mn", "MDBX SafeNoSync", Engine::Mdbx(MdbxSync::SafeNoSync, None)),
];

/// What a bench run measures, from the environment (see the module docs).
#[derive(Debug)]
struct Selection {
    /// Entries in each bulk table.
    n: usize,
    /// The selected columns, as indexes into [`ENGINES`], in print order.
    engines: Vec<usize>,
    /// One bit per selected [`Group`].
    rows: u32,
    /// Runs of each selected read row.
    repeat: usize,
}

/// The comma-separated names of a selector variable.
fn names(v: &str) -> Vec<&str> {
    v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
}

impl Selection {
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let number = |name: &str, default: usize| {
            var(name).map_or(default, |v| v.parse().unwrap_or_else(|_| panic!("{name} = {v:?}")))
        };
        let engines = match var("TNDB_B_BENCH_ENGINES") {
            None => (0..ENGINES.len()).collect(),
            Some(v) => {
                let picked = names(&v);
                let known = ENGINES.map(|(sel, ..)| sel);
                for p in &picked {
                    assert!(
                        known.contains(p),
                        "TNDB_B_BENCH_ENGINES: {p:?} is not one of {known:?}"
                    );
                }
                (0..ENGINES.len()).filter(|&e| picked.contains(&ENGINES[e].0)).collect()
            }
        };
        let rows = match var("TNDB_B_BENCH_ROWS") {
            None => u32::MAX,
            Some(v) => {
                let mut rows = 0;
                let picked = names(&v);
                for p in picked {
                    let Some((_, groups)) = GROUP_NAMES.iter().find(|(name, _)| *name == p) else {
                        let known = GROUP_NAMES.map(|(name, _)| name);
                        panic!("TNDB_B_BENCH_ROWS: {p:?} is not one of {known:?}");
                    };
                    rows |= groups.iter().fold(0, |bits, &g| bits | 1 << g as u32);
                }
                rows
            }
        };
        let repeat = number("TNDB_B_BENCH_REPEAT", 1);
        assert!(repeat >= 1, "TNDB_B_BENCH_REPEAT must be at least 1");
        Self { n: number("TNDB_B_BENCH_N", DEFAULT_N), engines, rows, repeat }
    }

    fn has(&self, group: Group) -> bool {
        self.rows & (1 << group as u32) != 0
    }

    /// Whether some selected group needs the sequential table.
    fn needs_seq(&self) -> bool {
        self.has(Group::Seq) || self.has(Group::Clear)
    }

    /// Whether some selected group needs the random table.
    fn needs_rand(&self) -> bool {
        use Group::*;
        [Rand, GetHit, GetMiss, ScanFwd, ScanRev, Seek, Mixed].into_iter().any(|g| self.has(g))
    }

    /// Run a read row `repeat` times and return the median wall time.
    fn median(&self, mut run: impl FnMut() -> Duration) -> Duration {
        let mut times: Vec<Duration> = (0..self.repeat).map(|_| run()).collect();
        times.sort_unstable();
        times[times.len() / 2]
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
    SeqPuts,
    SeqCommit,
    RandInsert,
    RandPuts,
    RandCommit,
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
    AppendPuts,
    CursorPuts,
    CursorAppendPuts,
}

impl Row {
    /// Every row, in print order.
    const ALL: [Row; ROWS] = [
        Row::SeqInsert,
        Row::SeqPuts,
        Row::SeqCommit,
        Row::RandInsert,
        Row::RandPuts,
        Row::RandCommit,
        Row::FileSize,
        Row::FileUsed,
        Row::GetHit,
        Row::GetMiss,
        Row::ScanForward,
        Row::ScanReverse,
        Row::SeekRange,
        Row::SmallMedian,
        Row::SmallP99,
        Row::SmallTotal,
        Row::FdatasyncMedian,
        Row::FdatasyncP99,
        Row::FdatasyncTotal,
        Row::Mixed,
        Row::Clear,
        Row::AppendPuts,
        Row::CursorPuts,
        Row::CursorAppendPuts,
    ];

    fn label(self, n: usize) -> String {
        let k = n / 1000;
        match self {
            Row::SeqInsert => format!("seq insert {k}k, 1 txn"),
            Row::RandInsert => format!("random insert {k}k, 1 txn"),
            Row::SeqPuts | Row::RandPuts => "  put loop".to_string(),
            Row::SeqCommit | Row::RandCommit => "  commit".to_string(),
            Row::FileSize => "file size (st_size)".to_string(),
            Row::FileUsed => "file used (pages x P)".to_string(),
            Row::GetHit => "get hit (per op)".to_string(),
            Row::GetMiss => "get miss (per op)".to_string(),
            Row::ScanForward => format!("scan forward {k}k"),
            Row::ScanReverse => format!("scan reverse {k}k"),
            Row::SeekRange => format!("seek + {SEEK_RANGE} entries (per seek)"),
            Row::SmallMedian => format!("small txn ({SMALL_PUTS} puts) median"),
            Row::SmallP99 => "small txn p99".to_string(),
            Row::SmallTotal => format!("small txns total ({SMALL_TXNS})"),
            Row::FdatasyncMedian => "  fdatasync: median".to_string(),
            Row::FdatasyncP99 => "  fdatasync: p99".to_string(),
            Row::FdatasyncTotal => "  fdatasync: total".to_string(),
            Row::Mixed => format!("mixed {}k upd/del ({MIXED_BATCH}/txn)", MIXED_OPS / 1000),
            Row::Clear => format!("clear_table {k}k + commit"),
            Row::AppendPuts => format!("append put {k}k (put loop)"),
            Row::CursorPuts => format!("cursor put {k}k (put loop)"),
            Row::CursorAppendPuts => format!("cursor put APPEND {k}k (put loop)"),
        }
    }
}

const ROWS: usize = Row::CursorAppendPuts as usize + 1;

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

    /// Record a bulk insert: its total and its split into the put loop and the commit.
    fn set_bulk(&mut self, rows: [Row; 3], puts: Duration, total: Duration) {
        self.set(rows[0], Cell::Total(total));
        self.set(rows[1], Cell::Total(puts));
        self.set(rows[2], Cell::Total(total - puts));
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

/// Run the selected rows on a fresh tndb-b file.
fn run_tndb(
    dir: &Path,
    name: &str,
    page_size: u32,
    sync: SyncMode,
    data: &Data,
    sel: &Selection,
) -> Column {
    let mut col = Column::new(name);
    let path = dir.join("data.tndb-b");
    let options = EnvOptions { page_size, map_reserve: 8 << 30, sync };
    let env = Env::open(&path, options).expect("open tndb-b");
    let n = data.n;

    let seq = sel.needs_seq().then(|| {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let seq = txn.create_table("seq").expect("create seq");
        for i in 0..n {
            txn.put(&seq, &(i as u64).to_be_bytes(), data.value(i)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Seq) {
            col.set_bulk([Row::SeqInsert, Row::SeqPuts, Row::SeqCommit], puts, start.elapsed());
        }
        seq
    });

    let rand = sel.needs_rand().then(|| {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let rand = txn.create_table("rand").expect("create rand");
        for (i, k) in data.rand_keys.iter().enumerate() {
            txn.put(&rand, k, data.value(i)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Rand) {
            col.set_bulk([Row::RandInsert, Row::RandPuts, Row::RandCommit], puts, start.elapsed());
        }
        rand
    });

    if sel.has(Group::Seq) || sel.has(Group::Rand) {
        let stat = env.stat();
        col.set(Row::FileSize, Cell::Bytes(file_size(&path)));
        col.set(Row::FileUsed, Cell::Bytes(u64::from(stat.page_count) * u64::from(stat.page_size)));
    }

    if let Some(rand) = &rand {
        tndb_reads(&env, rand, data, sel, &mut col);
    }

    if sel.has(Group::Small) {
        tndb_small(&env, sync, data, &mut col);
    }

    if let (true, Some(rand)) = (sel.has(Group::Mixed), &rand) {
        let start = Instant::now();
        for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
            let mut txn = env.begin_rw().expect("begin");
            for (j, &(idx, delete)) in batch.iter().enumerate() {
                let k = &data.rand_keys[idx];
                if delete {
                    txn.delete(rand, k).expect("delete");
                } else {
                    txn.put(rand, k, data.value(b + j)).expect("put");
                }
            }
            txn.commit().expect("commit");
        }
        col.set(Row::Mixed, Cell::Total(start.elapsed()));
    }

    if let (true, Some(seq)) = (sel.has(Group::Clear), &seq) {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        txn.clear_table(seq).expect("clear");
        txn.commit().expect("commit");
        col.set(Row::Clear, Cell::Total(start.elapsed()));
    }

    env.sync().expect("sync");
    drop(env);

    if sel.has(Group::Append) {
        let rows = [
            (Row::AppendPuts, "append", ApiPut::Append),
            (Row::CursorPuts, "cursor", ApiPut::Cursor),
            (Row::CursorAppendPuts, "cursor-append", ApiPut::CursorAppend),
        ];
        for (row, name, how) in rows {
            let path = dir.join(format!("{name}.tndb-b"));
            col.set(row, Cell::Total(tndb_fresh_puts(&path, options, data, how)));
        }
    }
    col
}

/// How an API row puts its keys.
#[derive(Clone, Copy)]
enum ApiPut {
    /// `RwTxn::append`.
    Append,
    /// `RwCursor::put`.
    Cursor,
    /// `RwCursor::append`.
    CursorAppend,
}

/// One API row: N sequential keys put `how` into a fresh table of a fresh environment at `path`;
/// returns the put loop's time (from `begin`), then commits.
fn tndb_fresh_puts(path: &Path, options: EnvOptions, data: &Data, how: ApiPut) -> Duration {
    let env = Env::open(path, options).expect("open tndb-b");
    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("fresh").expect("create");
    match how {
        ApiPut::Append => {
            for i in 0..data.n {
                txn.append(&table, &(i as u64).to_be_bytes(), data.value(i)).expect("append");
            }
        }
        ApiPut::Cursor | ApiPut::CursorAppend => {
            let mut cursor = txn.cursor_mut(&table).expect("cursor");
            for i in 0..data.n {
                let key = (i as u64).to_be_bytes();
                match how {
                    ApiPut::CursorAppend => cursor.append(&key, data.value(i)).expect("append"),
                    _ => cursor.put(&key, data.value(i)).expect("put"),
                }
            }
        }
    }
    let puts = start.elapsed();
    txn.commit().expect("commit");
    env.sync().expect("sync");
    puts
}

/// The read rows over the random table, each run `sel.repeat` times.
fn tndb_reads(env: &Env, rand: &Table, data: &Data, sel: &Selection, col: &mut Column) {
    let n = data.n;
    let ro = env.begin_ro().expect("begin ro");
    if sel.has(Group::GetHit) {
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in &data.rand_keys {
                bytes += ro.get(rand, k).expect("get").expect("a hit").len();
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::GetHit, Cell::PerOp(time, n));
    }

    if sel.has(Group::GetMiss) {
        let time = sel.median(|| {
            let start = Instant::now();
            for k in &data.miss_keys {
                assert!(ro.get(rand, k).expect("get").is_none(), "a miss");
            }
            start.elapsed()
        });
        col.set(Row::GetMiss, Cell::PerOp(time, n));
    }

    let mut cursor = ro.cursor(rand).expect("cursor");
    if sel.has(Group::ScanFwd) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry = cursor.first().expect("first");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.next().expect("next");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanForward, Cell::Total(time));
    }

    if sel.has(Group::ScanRev) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry = cursor.last().expect("last");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.prev().expect("prev");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanReverse, Cell::Total(time));
    }

    if sel.has(Group::Seek) {
        let seeks = SEEKS.min(n);
        let time = sel.median(|| {
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
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::SeekRange, Cell::PerOp(time, seeks));
    }
}

/// The small-transaction rows: `SMALL_TXNS` commits of `SMALL_PUTS` puts each (and as many again
/// with the fdatasync switch when `sync` is Durable).
fn tndb_small(env: &Env, sync: SyncMode, data: &Data, col: &mut Column) {
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
                times.push(small_txn(env));
            }
        }
        env.with_writer(|w| w.data_by_fdatasync = false);
    } else {
        for _ in 0..SMALL_TXNS {
            by_msync.push(small_txn(env));
        }
    }
    col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], by_msync);
    if sync == SyncMode::Durable {
        col.set_commits(
            [Row::FdatasyncMedian, Row::FdatasyncP99, Row::FdatasyncTotal],
            by_fdatasync,
        );
    }
}

/// The page size MDBX opens with when the column names none: the OS page size, as the node uses.
fn mdbx_os_page_size() -> usize {
    crate::sys::os_page_size().clamp(4096, 65536) as usize
}

/// Open MDBX as `MdbxDatabase::open_with_sync_mode` does (with this page size), with a generous
/// geometry.
fn open_mdbx(dir: &Path, sync_mode: MdbxSync, page_size: usize) -> Environment {
    let mut builder = Environment::builder();
    builder
        .set_max_dbs(8)
        .write_map()
        .set_geometry(Geometry {
            size: Some(0..(64usize << 30)),
            growth_step: Some(256 << 20),
            shrink_threshold: Some(0),
            page_size: Some(PageSize::Set(page_size)),
        })
        .set_flags(EnvironmentFlags { mode: Mode::ReadWrite { sync_mode }, ..Default::default() });
    builder.open(dir).expect("open mdbx")
}

/// Run the selected rows on a fresh MDBX environment.
fn run_mdbx(
    dir: &Path,
    name: &str,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    sel: &Selection,
) -> Column {
    let mut col = Column::new(name);
    let env = open_mdbx(dir, sync_mode, page_size);
    let n = data.n;

    let seq = sel.needs_seq().then(|| {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        let seq = txn.create_db(Some("seq"), DatabaseFlags::default()).expect("create seq").dbi();
        for i in 0..n {
            txn.put(seq, (i as u64).to_be_bytes(), data.value(i), WriteFlags::empty())
                .expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Seq) {
            col.set_bulk([Row::SeqInsert, Row::SeqPuts, Row::SeqCommit], puts, start.elapsed());
        }
        seq
    });

    let rand = sel.needs_rand().then(|| {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        let rand =
            txn.create_db(Some("rand"), DatabaseFlags::default()).expect("create rand").dbi();
        for (i, k) in data.rand_keys.iter().enumerate() {
            txn.put(rand, k, data.value(i), WriteFlags::empty()).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Rand) {
            col.set_bulk([Row::RandInsert, Row::RandPuts, Row::RandCommit], puts, start.elapsed());
        }
        rand
    });

    if sel.has(Group::Seq) || sel.has(Group::Rand) {
        let info = env.info().expect("info");
        let page_size = env.stat().expect("stat").page_size();
        col.set(Row::FileSize, Cell::Bytes(file_size(&dir.join("mdbx.dat"))));
        col.set(Row::FileUsed, Cell::Bytes((info.last_pgno() as u64 + 1) * u64::from(page_size)));
    }

    if let Some(rand) = rand {
        mdbx_reads(&env, rand, data, sel, &mut col);
    }

    if sel.has(Group::Small) {
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
    }

    if let (true, Some(rand)) = (sel.has(Group::Mixed), rand) {
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
    }

    if let (true, Some(seq)) = (sel.has(Group::Clear), seq) {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        txn.clear_db(seq).expect("clear");
        txn.commit().expect("commit");
        col.set(Row::Clear, Cell::Total(start.elapsed()));
    }
    drop(env);

    if sel.has(Group::Append) {
        let rows = [
            (Row::AppendPuts, "append", false, WriteFlags::APPEND),
            (Row::CursorPuts, "cursor", true, WriteFlags::empty()),
            (Row::CursorAppendPuts, "cursor-append", true, WriteFlags::APPEND),
        ];
        for (row, sub, cursor, flags) in rows {
            let time =
                mdbx_fresh_puts(&dir.join(sub), sync_mode, page_size, data, cursor, flags.bits());
            col.set(row, Cell::Total(time));
        }
    }
    col
}

/// The read rows over the random table, each run `sel.repeat` times.
fn mdbx_reads(env: &Environment, rand: u32, data: &Data, sel: &Selection, col: &mut Column) {
    type Kv<'a> = Option<(Cow<'a, [u8]>, Cow<'a, [u8]>)>;
    let n = data.n;
    let ro = env.begin_ro_txn().expect("begin ro");
    if sel.has(Group::GetHit) {
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in &data.rand_keys {
                bytes += ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").expect("a hit").len();
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::GetHit, Cell::PerOp(time, n));
    }

    if sel.has(Group::GetMiss) {
        let time = sel.median(|| {
            let start = Instant::now();
            for k in &data.miss_keys {
                assert!(ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").is_none(), "a miss");
            }
            start.elapsed()
        });
        col.set(Row::GetMiss, Cell::PerOp(time, n));
    }

    let mut cursor = ro.cursor(rand).expect("cursor");
    if sel.has(Group::ScanFwd) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry: Kv<'_> = cursor.first().expect("first");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.next().expect("next");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanForward, Cell::Total(time));
    }

    if sel.has(Group::ScanRev) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry: Kv<'_> = cursor.last().expect("last");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.prev().expect("prev");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanReverse, Cell::Total(time));
    }

    if sel.has(Group::Seek) {
        let seeks = SEEKS.min(n);
        let time = sel.median(|| {
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
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::SeekRange, Cell::PerOp(time, seeks));
    }
}

/// The put loop of `n` sequential keys into a fresh table of a fresh MDBX environment in `dir`,
/// through `Transaction::put` or `Cursor::put` with the `WriteFlags` bits `flags`; the commit
/// follows untimed.
fn mdbx_fresh_puts(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    cursor: bool,
    flags: u32,
) -> Duration {
    std::fs::create_dir(dir).expect("api row dir");
    let env = open_mdbx(dir, sync_mode, page_size);
    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    let db = txn.create_db(Some("fresh"), DatabaseFlags::default()).expect("create").dbi();
    if cursor {
        let mut cursor = txn.cursor(db).expect("cursor");
        for i in 0..data.n {
            cursor
                .put(
                    &(i as u64).to_be_bytes(),
                    data.value(i),
                    WriteFlags::from_bits_truncate(flags),
                )
                .expect("put");
        }
    } else {
        for i in 0..data.n {
            txn.put(
                db,
                (i as u64).to_be_bytes(),
                data.value(i),
                WriteFlags::from_bits_truncate(flags),
            )
            .expect("put");
        }
    }
    let puts = start.elapsed();
    txn.commit().expect("commit");
    puts
}

fn print_table(columns: &[Column], n: usize) {
    let shown: Vec<Row> = Row::ALL
        .into_iter()
        .filter(|&r| columns.iter().any(|c| !matches!(c.cells[r as usize], Cell::None)))
        .collect();
    let labels: Vec<String> = shown.iter().map(|r| r.label(n)).collect();
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
    for (row, label) in shown.iter().zip(&labels) {
        let mut line = format!("| {label:label_w$} |");
        for (c, w) in columns.iter().zip(&widths) {
            line.push_str(&format!(" {:>w$} |", c.cells[*row as usize].render()));
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
    let sel = Selection::from_env();
    let n = sel.n;
    let tmp = tempfile::tempdir().expect("temp dir");
    println!("bench temp dir: {}", tmp.path().display());
    let data = Data::new(n);
    let columns: Vec<Column> = sel
        .engines
        .iter()
        .map(|&e| {
            let (dir_name, title, engine) = ENGINES[e];
            let dir = tmp.path().join(dir_name);
            std::fs::create_dir(&dir).expect("column dir");
            match engine {
                Engine::Tndb(page_size, sync) => {
                    run_tndb(&dir, title, page_size, sync, &data, &sel)
                }
                Engine::Mdbx(sync, page) => {
                    run_mdbx(&dir, title, sync, page.unwrap_or_else(mdbx_os_page_size), &data, &sel)
                }
            }
        })
        .collect();
    println!(
        "\ntndb-b vs MDBX: {n} entries per bulk table, {VALUE_LEN}-byte values, MDBX page {} B \
         (md4: 4096 B){}\n",
        mdbx_os_page_size(),
        if sel.repeat > 1 {
            format!(", read rows the median of {} runs", sel.repeat)
        } else {
            String::new()
        }
    );
    print_table(&columns, n);
    println!(
        "\nNotes: NoSync/SafeNoSync commits are not durable. On macOS msync/fsync do not flush the \
         drive cache, so durable rows cost far less than on Linux. The fdatasync rows replace \
         tndb-b's per-range msync (commit step 4) with one fdatasync of the whole file. The API \
         rows (append put, cursor put) time the put loop into a fresh environment. MDBX opens with \
         the OS page size; md4 opens it with 4 KiB pages, the like-for-like column for t4d (on \
         x86_64 the OS page is 4 KiB, so md and md4 are the same configuration there)."
    );
}
