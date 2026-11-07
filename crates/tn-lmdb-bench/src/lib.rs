//! A regression benchmark of the `tn-lmdb` engine, driven only through its public API, so the
//! same source builds against two revisions of the engine and their results compare row by row
//! (`etc/tn-lmdb-bench-compare.sh` does that).
//!
//! Every row is a fixed, seeded workload with a stable name ([`ROWS`]). Each runs `reps` times,
//! each rep in a fresh environment in a fresh temp directory, and records one sample per rep:
//! nanoseconds per operation for a time row, or a deterministic size for an exact row (bytes, or
//! pages per block), which must not change between runs. Lower is better for every row. A time
//! row of a short measured loop runs it again within the rep (a read loop over the same state, a
//! put loop or a commit in a fresh environment each time) until its passes add up to a few tens
//! of milliseconds, and its sample is its fastest pass.
//!
//! Rows belong to a group: `generic` (the engine's basic operations on plain tables) and `reth`
//! (the shapes and write patterns of reth's tables, as the reth adapter lays them out). A row runs
//! under each selected sync mode ([`Mode`]) its result depends on: a row of a single transaction's
//! put loop or of reads does the same work under either mode, so it runs under `nosync` only.
//!
//! [`run`] runs a [`Config`] and returns a [`Report`], which serializes to the schema-1 JSON the
//! comparison script reads; [`cli_main`] is the command line. The crate's `README.md` lists the
//! rows and what each models.

use std::{
    collections::HashMap,
    fmt::Write as _,
    path::Path,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tn_lmdb::{Env, EnvOptions, SyncMode};

mod cli;
mod data;
mod generic;
mod reth;

pub use cli::{cli_main, parse_args, Command, USAGE};

/// The JSON schema version [`Report`] writes.
pub const SCHEMA: u32 = 1;

/// A row's group, selected by `--groups`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Group {
    /// The engine's basic operations on plain tables.
    Generic,
    /// reth's table shapes and write patterns.
    Reth,
}

impl Group {
    /// The group's name in the CLI and the JSON.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Reth => "reth",
        }
    }
}

/// A sync mode rows run under, selected by `--sync`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mode {
    /// `SyncMode::NoSync`: commits are not synced, and no timed region calls `Env::sync`.
    NoSync,
    /// `SyncMode::Durable`: every commit syncs.
    Durable,
}

impl Mode {
    /// The mode's name in the CLI and the JSON.
    pub const fn name(self) -> &'static str {
        match self {
            Self::NoSync => "nosync",
            Self::Durable => "durable",
        }
    }

    /// The engine's sync mode.
    const fn engine(self) -> SyncMode {
        match self {
            Self::NoSync => SyncMode::NoSync,
            Self::Durable => SyncMode::Durable,
        }
    }
}

/// What a row's samples measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    /// Nanoseconds per operation (the row's docs say which operation): a timed, noisy row.
    NsPerOp,
    /// A size in bytes: exact.
    Bytes,
    /// Pages per operation: exact.
    PagesPerOp,
}

impl Metric {
    /// The metric's name and unit in the JSON.
    pub const fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::NsPerOp => ("ns_per_op", "ns"),
            Self::Bytes => ("bytes", "B"),
            Self::PagesPerOp => ("pages_per_op", "pages"),
        }
    }

    /// Whether the metric is deterministic: the same workload gives the same value every run.
    pub const fn exact(self) -> bool {
        !matches!(self, Self::NsPerOp)
    }
}

/// The sync modes a row runs under, of those selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Modes {
    /// Every selected mode: the mode changes the work: a time row that includes commits or is
    /// timed between them, or a size row of a state built by several commits.
    Every,
    /// `nosync` only: one transaction's put or delete loop, the file one commit leaves, or reads
    /// (on any state: under `nosync` the settles make it the durable state), the same work under
    /// either mode.
    NoSyncOnly,
    /// `durable` only: a sync counter, zero under `nosync`.
    DurableOnly,
}

impl Modes {
    /// Whether a row of these modes runs under `mode`.
    pub const fn has(self, mode: Mode) -> bool {
        match self {
            Self::Every => true,
            Self::NoSyncOnly => matches!(mode, Mode::NoSync),
            Self::DurableOnly => matches!(mode, Mode::Durable),
        }
    }
}

/// The job that measures a row: one setup in a fresh environment per rep, shared by its rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Job {
    Seq,
    Rand,
    Txn,
    Small,
    Api,
    Mixed,
    Delete,
    Churn,
    Big,
    State,
    ChangeSets,
    HistoryRewrite,
    DupRewrite,
    Block,
}

impl Job {
    /// Every job, in run order.
    const ALL: [Self; 14] = [
        Self::Seq,
        Self::Rand,
        Self::Txn,
        Self::Small,
        Self::Api,
        Self::Mixed,
        Self::Delete,
        Self::Churn,
        Self::Big,
        Self::State,
        Self::ChangeSets,
        Self::HistoryRewrite,
        Self::DupRewrite,
        Self::Block,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Seq => "seq",
            Self::Rand => "rand",
            Self::Txn => "txn",
            Self::Small => "small",
            Self::Api => "api",
            Self::Mixed => "mixed",
            Self::Delete => "delete",
            Self::Churn => "churn",
            Self::Big => "big",
            Self::State => "reth.state",
            Self::ChangeSets => "reth.changesets",
            Self::HistoryRewrite => "reth.history.rewrite",
            Self::DupRewrite => "reth.dup.rewrite",
            Self::Block => "reth.block",
        }
    }
}

/// The entries a bulk table (`n`) from which the comparison gates every row: a smaller run (the
/// quick tier's 100k) reports the rows whose [`RowSpec::small_gate`] is `false` without gating
/// them.
pub const GATE_EVERY_ROW_FROM_N: usize = 1_000_000;

/// One row: a stable name (the key two runs compare by), its group, its metric and the sync modes
/// it runs under. The README's table says what one operation of a time row is.
#[derive(Clone, Copy, Debug)]
pub struct RowSpec {
    /// The row's stable name.
    pub name: &'static str,
    /// Its group.
    pub group: Group,
    /// What its samples measure.
    pub metric: Metric,
    /// The sync modes it runs under.
    pub modes: Modes,
    /// Whether the comparison gates the row in a run of fewer than [`GATE_EVERY_ROW_FROM_N`]
    /// entries a bulk table ([`RowSpec::gate`]). A commit row's sample in a run of the quick
    /// tier's 100k is one commit of 100k entries, a fraction of a millisecond spent mostly
    /// writing fresh file pages, whose cost moves by 10% and more from run to run with the
    /// machine's memory state however many passes it takes: such a row is reported in a small
    /// run, and gated from 1M entries (the default and full tiers) on. The run's size decides,
    /// not its tier's name, since `--n` overrides the tier's.
    pub small_gate: bool,
    /// The job that measures it.
    pub(crate) job: Job,
}

impl RowSpec {
    /// The row, not gated in a run of fewer than [`GATE_EVERY_ROW_FROM_N`] entries.
    const fn not_gated_when_small(self) -> Self {
        Self { small_gate: false, ..self }
    }

    /// Whether the comparison gates the row in a run of `n` entries a bulk table: its JSON's
    /// `gate`.
    pub const fn gate(&self, n: usize) -> bool {
        self.small_gate || n >= GATE_EVERY_ROW_FROM_N
    }
}

const fn row(name: &'static str, group: Group, metric: Metric, modes: Modes, job: Job) -> RowSpec {
    RowSpec { name, group, metric, modes, small_gate: true, job }
}

use Group::{Generic, Reth};
use Metric::{Bytes, NsPerOp, PagesPerOp};
use Modes::{DurableOnly, Every, NoSyncOnly};

/// Every row, in report order. The README's table describes each.
pub const ROWS: &[RowSpec] = &[
    row("put.seq", Generic, NsPerOp, NoSyncOnly, Job::Seq),
    row("put.seq.commit", Generic, NsPerOp, Every, Job::Seq).not_gated_when_small(),
    row("put.rand", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("put.rand.commit", Generic, NsPerOp, Every, Job::Rand).not_gated_when_small(),
    row("put.rand.file_bytes", Generic, Bytes, NoSyncOnly, Job::Rand),
    row("put.append", Generic, NsPerOp, NoSyncOnly, Job::Api),
    row("put.cursor", Generic, NsPerOp, NoSyncOnly, Job::Api),
    row("put.txn_cursor", Generic, NsPerOp, NoSyncOnly, Job::Api),
    row("get.hit", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("get.miss", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("scan.fwd", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("scan.rev", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("seek.range", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("seek.range.sorted", Generic, NsPerOp, NoSyncOnly, Job::Rand),
    row("ro.txn", Generic, NsPerOp, NoSyncOnly, Job::Txn),
    row("ro.txn.mt", Generic, NsPerOp, NoSyncOnly, Job::Txn),
    row("commit.small", Generic, NsPerOp, Every, Job::Small),
    row("mixed", Generic, NsPerOp, Every, Job::Mixed),
    row("delete.rand90", Generic, NsPerOp, NoSyncOnly, Job::Delete),
    row("churn", Generic, NsPerOp, Every, Job::Churn),
    row("churn.file_bytes", Generic, Bytes, Every, Job::Churn),
    row("churn.table_bytes", Generic, Bytes, Every, Job::Churn),
    row("big.2k", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("big.2k.get", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("big.8k", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("big.8k.get", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("big.32k", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("big.32k.get", Generic, NsPerOp, NoSyncOnly, Job::Big),
    row("reth.state.load", Reth, NsPerOp, Every, Job::State),
    row("reth.state.get", Reth, NsPerOp, NoSyncOnly, Job::State),
    row("reth.state.walk", Reth, NsPerOp, NoSyncOnly, Job::State),
    row("reth.state.file_bytes", Reth, Bytes, Every, Job::State),
    row("reth.changesets.append", Reth, NsPerOp, Every, Job::ChangeSets),
    row("reth.changesets.walk", Reth, NsPerOp, NoSyncOnly, Job::ChangeSets),
    row("reth.changesets.file_bytes", Reth, Bytes, Every, Job::ChangeSets),
    row("reth.history.rewrite", Reth, NsPerOp, Every, Job::HistoryRewrite),
    row("reth.dup.rewrite", Reth, NsPerOp, Every, Job::DupRewrite),
    row("reth.block", Reth, NsPerOp, Every, Job::Block),
    row("reth.block.file_bytes", Reth, Bytes, Every, Job::Block),
    row("reth.block.pages_synced", Reth, PagesPerOp, DurableOnly, Job::Block),
];

/// A benchmark tier: the default size, reps and sync modes of a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// `n` 100k, 3 reps, `nosync`: seconds.
    Quick,
    /// `n` 1M, 3 reps, `nosync`: minutes.
    Default,
    /// `n` 1M, 7 reps, `nosync` and `durable`.
    Full,
}

impl Tier {
    /// The tier's name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Default => "default",
            Self::Full => "full",
        }
    }

    /// The tier's entries a bulk table, reps and sync modes.
    pub fn defaults(self) -> (usize, usize, Vec<Mode>) {
        match self {
            Self::Quick => (100_000, 3, vec![Mode::NoSync]),
            Self::Default => (1_000_000, 3, vec![Mode::NoSync]),
            Self::Full => (1_000_000, 7, vec![Mode::NoSync, Mode::Durable]),
        }
    }
}

/// What a run measures.
#[derive(Clone, Debug)]
pub struct Config {
    /// The tier the run is named for (its defaults may be overridden).
    pub tier: Tier,
    /// Entries in each bulk table; the other workloads' counts follow it.
    pub n: usize,
    /// Samples per row, each in a fresh environment.
    pub reps: usize,
    /// The page size of every environment.
    pub page_size: u32,
    /// The sync modes, in run order.
    pub modes: Vec<Mode>,
    /// The selected rows, in [`ROWS`] order.
    pub rows: Vec<&'static RowSpec>,
    /// A free-form label for the report.
    pub label: String,
}

impl Config {
    /// The (row, mode) pairs the run measures, in report order: rows in [`ROWS`] order, each
    /// under its modes in the order given.
    pub fn pairs(&self) -> Vec<(&'static RowSpec, Mode)> {
        self.rows
            .iter()
            .flat_map(|&row| {
                self.modes.iter().filter(|&&m| row.modes.has(m)).map(move |&m| (row, m))
            })
            .collect()
    }
}

/// One row's results under one sync mode, as the JSON holds them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowResult {
    /// The row's stable name.
    pub name: String,
    /// Its group's name.
    pub group: String,
    /// The sync mode's name.
    pub sync: String,
    /// The metric's name (`ns_per_op`, `bytes`, `pages_per_op`).
    pub metric: String,
    /// The metric's unit (`ns`, `B`, `pages`).
    pub unit: String,
    /// One sample per rep, in run order.
    pub samples: Vec<f64>,
    /// The samples' median (the mean of the middle two for an even count).
    pub median: f64,
    /// The smallest sample.
    pub min: f64,
    /// The largest sample.
    pub max: f64,
    /// Whether the row is deterministic: any change is real.
    pub exact: bool,
    /// Whether the comparison gates the row at this run's size ([`RowSpec::gate`]); a row of
    /// `false` is reported, not gated.
    pub gate: bool,
}

/// A run's results: the schema-1 JSON `--out` writes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// The schema version, [`SCHEMA`].
    pub schema: u32,
    /// The bench's name, `tn-lmdb-bench`.
    pub bench: String,
    /// The run's `--label`, or empty.
    pub label: String,
    /// The tier's name.
    pub tier: String,
    /// Entries in each bulk table.
    pub n: u64,
    /// Samples per row.
    pub reps: u64,
    /// The environments' page size.
    pub page_size: u32,
    /// `std::env::consts::OS`.
    pub os: String,
    /// `std::env::consts::ARCH`.
    pub arch: String,
    /// The rows, in report order.
    pub rows: Vec<RowResult>,
}

/// The rows a job measured in one rep, kept only where the run wants them.
pub(crate) struct Out<'a> {
    /// The rows this rep of the job is for.
    want: &'a dyn Fn(&str) -> bool,
    /// The samples, by row name.
    rows: Vec<(&'static str, f64)>,
}

impl Out<'_> {
    /// Whether the run wants `row` from this rep (a job skips the work of a row it does not).
    pub(crate) fn wants(&self, row: &str) -> bool {
        (self.want)(row)
    }

    /// Record `row`'s sample, if the run wants it.
    pub(crate) fn push(&mut self, row: &'static str, value: f64) {
        if self.wants(row) {
            self.rows.push((row, value));
        }
    }
}

/// Open (create) the environment `name` in `dir`: the file `<name>.tn-lmdb`.
pub(crate) fn open(dir: &Path, name: &str, options: EnvOptions) -> Env {
    Env::open(dir.join(format!("{name}.tn-lmdb")), options).expect("open tn-lmdb")
}

/// Make `env`'s commits durable, never inside a timed region. A no-op under `SyncMode::Durable`.
/// Under `SyncMode::NoSync` a page a commit frees is reused only once a sync has made that commit
/// durable, so a row of several commits settles after each: its next commit reuses pages as a
/// durable run's would, and the file stays the size a durable run's is, instead of growing by
/// every page each commit copies.
pub(crate) fn settle(env: &Env) {
    env.sync().expect("sync");
}

/// `time` per one of `ops` operations, in nanoseconds.
pub(crate) fn per_op(time: Duration, ops: usize) -> f64 {
    time.as_nanos() as f64 / ops.max(1) as f64
}

/// `time` in nanoseconds.
pub(crate) fn nanos(time: Duration) -> f64 {
    time.as_nanos() as f64
}

/// The least measured time a time row's passes add up to in one rep. A pass of a few
/// milliseconds (a scan of 100k entries, a commit) lands whole in one scheduler hiccup, a load
/// spike or a stretch on an efficiency core, so such a row runs its pass again until its passes
/// add up to this.
const PASS_TARGET: Duration = Duration::from_millis(30);
/// The fewest passes a time row runs in one rep, unless they add up to [`ENOUGH`]: a row's sample
/// is its fastest pass, and the fastest of a few drops the passes a slowdown hit.
const FEWEST_PASSES: usize = 3;
/// The measured time after which a row stops short of [`FEWEST_PASSES`]: a pass this long or
/// longer (most rows at 1M entries, a workload of many commits at 100k) runs once.
const ENOUGH: Duration = Duration::from_millis(100);
/// The most passes a time row runs in one rep.
const MAX_PASSES: usize = 16;

/// The fastest of a time row's passes in one rep. A pass is added until the passes add up to
/// [`PASS_TARGET`] and number [`FEWEST_PASSES`] (or add up to [`ENOUGH`]), or number
/// [`MAX_PASSES`]. A pass is the row's measured loop: again over the same state for a read row,
/// on a fresh state (a fresh environment, set up untimed) for a row that writes.
pub(crate) struct Fastest {
    /// Whether the run wants the row: a row it does not want is done from the start, and its
    /// passes follow its job's other rows.
    wanted: bool,
    /// The fastest pass so far.
    best: Duration,
    /// The passes' measured time.
    total: Duration,
    /// The passes so far.
    passes: usize,
}

impl Fastest {
    /// No passes yet of a row the run wants (`wanted`) or does not.
    pub(crate) const fn new(wanted: bool) -> Self {
        Self { wanted, best: Duration::MAX, total: Duration::ZERO, passes: 0 }
    }

    /// Record a pass's measured time.
    pub(crate) fn add(&mut self, time: Duration) {
        self.best = self.best.min(time);
        self.total += time;
        self.passes += 1;
    }

    /// Whether the row has its sample.
    pub(crate) fn done(&self) -> bool {
        !self.wanted
            || self.passes >= MAX_PASSES
            || (self.total >= PASS_TARGET && (self.passes >= FEWEST_PASSES || self.total >= ENOUGH))
    }

    /// The fastest pass.
    pub(crate) const fn best(&self) -> Duration {
        self.best
    }
}

/// The sample of a time row whose pass needs no fresh state (a read loop): `pass` (one run of
/// the measured loop, returning its measured time) run until a [`Fastest`] is done; the fastest
/// pass.
pub(crate) fn fastest(mut pass: impl FnMut() -> Duration) -> Duration {
    let mut fastest = Fastest::new(true);
    while !fastest.done() {
        fastest.add(pass());
    }
    fastest.best()
}

/// The samples of the time rows a job measures together on a fresh state (a put loop and its
/// commit, a workload of many commits): `pass` sets a fresh environment `name` in `dir` up
/// (untimed) and returns its rows' measured times and a value, and runs again on a fresh
/// environment until every row's [`Fastest`] is done (`wanted` says which rows the run wants; a
/// job whose rows it does not want still runs once, for its exact rows). Each pass's environment
/// but the last is closed and its file removed; the rows' fastest passes come back with the last
/// pass's environment and value.
pub(crate) fn fresh_passes<const N: usize, T>(
    dir: &Path,
    name: &str,
    options: EnvOptions,
    wanted: [bool; N],
    mut pass: impl FnMut(&Env) -> ([Duration; N], T),
) -> ([Duration; N], Env, T) {
    let mut rows = wanted.map(Fastest::new);
    loop {
        let env = open(dir, name, options);
        let (times, value) = pass(&env);
        for (row, time) in rows.iter_mut().zip(times) {
            row.add(time);
        }
        if rows.iter().all(Fastest::done) {
            return (rows.map(|row| row.best()), env, value);
        }
        drop(env);
        std::fs::remove_file(dir.join(format!("{name}.tn-lmdb"))).expect("remove a pass's file");
    }
}

/// The file's used span: the newest committed state's pages times the page size.
pub(crate) fn used_bytes(env: &Env) -> f64 {
    let stat = env.stat();
    (u64::from(stat.page_count) * u64::from(stat.page_size)) as f64
}

/// The median of `samples` (the mean of the middle two for an even count).
fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// The type of the file system holding `dir` when it is held in memory (`tmpfs`, `ramfs`), from
/// Linux's `/proc/self/mountinfo` (the mount whose point is the longest prefix of `dir`'s
/// canonical path); `None` otherwise, and where that file does not exist. Syncs there cost
/// nothing, so a run refuses such a temp dir.
pub fn memory_fs(dir: &Path) -> Option<String> {
    let dir = dir.canonicalize().ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let fs = mounts
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
        .map(|(_, fs)| fs)?;
    matches!(fs.as_str(), "tmpfs" | "ramfs").then_some(fs)
}

/// Run `config`'s rows and return their results. `progress` gets a line per job rep (the CLI
/// prints them to stderr). Each rep of a job runs in a fresh temp directory under `$TMPDIR`,
/// removed after it; every environment is closed before the next opens.
///
/// # Panics
///
/// On an engine error, a failed check of a workload's own results, or a temp directory that
/// cannot be created: a run either measures every row it was asked for or stops.
pub fn run(config: &Config, progress: &mut dyn FnMut(&str)) -> Report {
    let pairs = config.pairs();
    let mut samples: HashMap<(&'static str, Mode), Vec<f64>> = HashMap::new();
    let jobs: Vec<Job> =
        Job::ALL.into_iter().filter(|&job| pairs.iter().any(|(r, _)| r.job == job)).collect();
    let mut generic: Option<data::Data> = None;
    for (j, &job) in jobs.iter().enumerate() {
        let mut runner = |each: &mut dyn FnMut(&Path, EnvOptions, &mut Out<'_>)| {
            for &mode in &config.modes {
                let wanted: Vec<&str> = pairs
                    .iter()
                    .filter(|(r, m)| r.job == job && *m == mode)
                    .map(|(r, _)| r.name)
                    .collect();
                if wanted.is_empty() {
                    continue;
                }
                let options = EnvOptions {
                    page_size: config.page_size,
                    map_reserve: 8 << 30,
                    sync: mode.engine(),
                    ..EnvOptions::default()
                };
                for rep in 1..=config.reps {
                    let dir = tempfile::Builder::new()
                        .prefix("tn-lmdb-bench-")
                        .tempdir()
                        .expect("create a temp dir");
                    let want = |name: &str| wanted.contains(&name);
                    let mut out = Out { want: &want, rows: Vec::new() };
                    let start = Instant::now();
                    each(dir.path(), options, &mut out);
                    let secs = start.elapsed().as_secs_f64();
                    drop(dir);
                    let mut line = format!(
                        "[{}/{}] {} ({}) rep {rep}/{}: {secs:.2} s:",
                        j + 1,
                        jobs.len(),
                        job.name(),
                        mode.name(),
                        config.reps,
                    );
                    for (name, value) in out.rows {
                        let _ = write!(line, " {name} {value:.1}");
                        samples.entry((name, mode)).or_default().push(value);
                    }
                    progress(&line);
                }
            }
        };
        let n = config.n;
        match job {
            Job::State | Job::ChangeSets | Job::HistoryRewrite | Job::DupRewrite | Job::Block => {
                generic = None;
            }
            _ => {}
        }
        match job {
            Job::State => {
                let data = reth::StateData::new(n);
                runner(&mut |dir, options, out| reth::state(dir, options, &data, out));
            }
            Job::ChangeSets => {
                let data = reth::ChangeSetData::new(n);
                runner(&mut |dir, options, out| reth::changesets(dir, options, &data, out));
            }
            Job::HistoryRewrite => {
                let data = reth::RewriteData::new(n);
                runner(&mut |dir, options, out| reth::history_rewrite(dir, options, &data, out));
            }
            Job::DupRewrite => {
                let data = reth::RewriteData::new(n);
                runner(&mut |dir, options, out| reth::dup_rewrite(dir, options, &data, out));
            }
            Job::Block => {
                let data = reth::BlockData::new(n);
                runner(&mut |dir, options, out| reth::block(dir, options, &data, out));
            }
            generic_job => {
                let data = &*generic.get_or_insert_with(|| data::Data::new(n));
                let each: fn(&Path, EnvOptions, &data::Data, &mut Out<'_>) = match generic_job {
                    Job::Seq => generic::seq,
                    Job::Rand => generic::rand,
                    Job::Txn => generic::txn,
                    Job::Small => generic::small,
                    Job::Api => generic::api,
                    Job::Mixed => generic::mixed,
                    Job::Delete => generic::delete,
                    Job::Churn => generic::churn,
                    Job::Big => generic::big,
                    reth_job => unreachable!("{reth_job:?} is matched above"),
                };
                runner(&mut |dir, options, out| each(dir, options, data, out));
            }
        }
    }
    let rows = pairs
        .iter()
        .map(|&(row, mode)| {
            let samples = samples.remove(&(row.name, mode)).unwrap_or_default();
            assert_eq!(
                samples.len(),
                config.reps,
                "{} ({}) has a sample per rep",
                row.name,
                mode.name()
            );
            let (metric, unit) = row.metric.names();
            let min = samples.iter().copied().fold(f64::INFINITY, f64::min);
            let max = samples.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            if row.metric.exact() && min != max {
                progress(&format!(
                    "warning: exact row {} ({}) differs between reps: {samples:?}",
                    row.name,
                    mode.name()
                ));
            }
            RowResult {
                name: row.name.to_owned(),
                group: row.group.name().to_owned(),
                sync: mode.name().to_owned(),
                metric: metric.to_owned(),
                unit: unit.to_owned(),
                median: median(&samples),
                min,
                max,
                samples,
                exact: row.metric.exact(),
                gate: row.gate(config.n),
            }
        })
        .collect();
    Report {
        schema: SCHEMA,
        bench: "tn-lmdb-bench".to_owned(),
        label: config.label.clone(),
        tier: config.tier.name().to_owned(),
        n: config.n as u64,
        reps: config.reps as u64,
        page_size: config.page_size,
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        rows,
    }
}

/// `report` as the human table the CLI prints: a row per (row, mode), its median, min and max.
pub fn table(report: &Report) -> String {
    let mut s = format!(
        "tn-lmdb-bench: tier {}, n {}, reps {}, page size {}, {} {}{}\n",
        report.tier,
        report.n,
        report.reps,
        report.page_size,
        report.os,
        report.arch,
        if report.label.is_empty() { String::new() } else { format!(", label {}", report.label) },
    );
    let _ = writeln!(
        s,
        "{:<28} {:<8} {:<6} {:>16} {:>16} {:>16}",
        "row", "sync", "unit", "median", "min", "max"
    );
    for r in &report.rows {
        let unit = if r.exact { r.unit.clone() } else { format!("{}/op", r.unit) };
        let _ = writeln!(
            s,
            "{:<28} {:<8} {:<6} {:>16.1} {:>16.1} {:>16.1}",
            r.name, r.sync, unit, r.median, r.min, r.max
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every row under both modes at a tiny size, one rep.
    fn tiny() -> Config {
        Config {
            tier: Tier::Quick,
            n: 2000,
            reps: 1,
            page_size: 4096,
            modes: vec![Mode::NoSync, Mode::Durable],
            rows: ROWS.iter().collect(),
            label: "smoke".to_owned(),
        }
    }

    #[test]
    fn row_names_are_unique() {
        let mut names: Vec<&str> = ROWS.iter().map(|r| r.name).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "a row name repeats");
        for r in ROWS {
            assert!(r.name.starts_with("reth.") == (r.group == Group::Reth), "{}", r.name);
        }
    }

    #[test]
    fn every_row_runs_and_exact_rows_repeat() {
        let config = tiny();
        let first = run(&config, &mut |_| {});
        let pairs = config.pairs();
        assert_eq!(first.rows.len(), pairs.len());
        for ((row, mode), result) in pairs.iter().zip(&first.rows) {
            assert_eq!((row.name, mode.name()), (result.name.as_str(), result.sync.as_str()));
            assert_eq!(result.samples.len(), 1, "{} ({})", row.name, mode.name());
            assert!(result.median > 0.0, "{} ({}) measured nothing", row.name, mode.name());
            assert_eq!(result.exact, row.metric.exact());
            assert_eq!(
                result.gate, row.small_gate,
                "{} in a run of {} entries",
                row.name, config.n
            );
        }
        assert!(first.rows.iter().any(|r| r.name == "put.seq.commit" && !r.gate));
        // Every row that has a durable twin ran under both modes, and pages_synced only durable.
        assert!(first
            .rows
            .iter()
            .any(|r| r.name == "reth.block.pages_synced" && r.sync == "durable"));
        assert!(!first
            .rows
            .iter()
            .any(|r| r.name == "reth.block.pages_synced" && r.sync == "nosync"));

        let json = serde_json::to_string_pretty(&first).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["schema"], 1);
        assert_eq!(value["bench"], "tn-lmdb-bench");
        for key in ["label", "tier", "n", "reps", "page_size", "os", "arch", "rows"] {
            assert!(value.get(key).is_some(), "the report has {key}");
        }
        for key in [
            "name", "group", "sync", "metric", "unit", "samples", "median", "min", "max", "exact",
            "gate",
        ] {
            assert!(value["rows"][0].get(key).is_some(), "a row has {key}");
        }
        let back: Report = serde_json::from_str(&json).expect("deserialize");
        // serde_json (without its `float_roundtrip` feature) parses a float to within an ulp of
        // the one it printed, not always to it: the floats round-trip that closely, the rest
        // exactly.
        let floats = |r: &Report| -> Vec<f64> {
            r.rows
                .iter()
                .flat_map(|w| w.samples.iter().copied().chain([w.median, w.min, w.max]))
                .collect()
        };
        let (got, want) = (floats(&back), floats(&first));
        assert_eq!(got.len(), want.len());
        for (a, b) in got.into_iter().zip(want) {
            assert!((a - b).abs() <= b.abs() * 1e-15, "{b} round-trips as {a}");
        }
        let without_floats = |r: &Report| Report {
            rows: r
                .rows
                .iter()
                .map(|w| RowResult {
                    samples: Vec::new(),
                    median: 0.0,
                    min: 0.0,
                    max: 0.0,
                    ..w.clone()
                })
                .collect(),
            ..r.clone()
        };
        assert_eq!(without_floats(&back), without_floats(&first), "the JSON round-trips");

        // The exact rows again, from their jobs alone: the same values.
        let exact = Config { rows: ROWS.iter().filter(|r| r.metric.exact()).collect(), ..tiny() };
        let second = run(&exact, &mut |_| {});
        assert!(!second.rows.is_empty());
        for again in &second.rows {
            let before = first
                .rows
                .iter()
                .find(|r| r.name == again.name && r.sync == again.sync)
                .expect("the first run has the row");
            assert_eq!(before.samples, again.samples, "{} ({}) is exact", again.name, again.sync);
        }

        // The run's size decides the gate, not its tier's name: a default-tier run of the tiny
        // size gates the small-run rows no more than a quick one.
        let commits = Config {
            tier: Tier::Default,
            rows: ROWS.iter().filter(|r| !r.small_gate).collect(),
            modes: vec![Mode::NoSync],
            ..tiny()
        };
        assert!(!commits.rows.is_empty());
        assert!(run(&commits, &mut |_| {}).rows.iter().all(|r| !r.gate));
    }

    #[test]
    fn every_row_is_gated_from_the_size_on() {
        let small: Vec<&str> = ROWS.iter().filter(|r| !r.gate(100_000)).map(|r| r.name).collect();
        assert_eq!(small, ["put.seq.commit", "put.rand.commit"]);
        assert!(ROWS.iter().all(|r| r.gate(GATE_EVERY_ROW_FROM_N)));
        assert!(ROWS.iter().any(|r| !r.gate(GATE_EVERY_ROW_FROM_N - 1)));
    }

    #[test]
    fn passes_stop_at_the_target() {
        let passes = |ms: u64| {
            let mut fastest = Fastest::new(true);
            while !fastest.done() {
                fastest.add(Duration::from_millis(ms + fastest.passes as u64 % 2));
            }
            (fastest.passes, fastest.best())
        };
        let ms = Duration::from_millis;
        assert_eq!(passes(1), (MAX_PASSES, ms(1)));
        assert_eq!(passes(5), (6, ms(5)), "30 ms of passes");
        assert_eq!(passes(20), (FEWEST_PASSES, ms(20)));
        assert_eq!(passes(60), (2, ms(60)), "100 ms of passes stop short of the fewest");
        assert_eq!(passes(150), (1, ms(150)));
        assert!(Fastest::new(false).done(), "a row the run does not want runs no pass of its own");
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
        assert_eq!(median(&[7.0]), 7.0);
    }
}
