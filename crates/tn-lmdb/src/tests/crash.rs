//! The crash matrix: a commit (or, in `SyncMode::NoSync`, a sync; or a `NoSync` commit past
//! `EnvOptions::sync_period`, which syncs itself) stopped at each point of the commit protocol,
//! then a process crash, a strict power loss or a random-writeback power loss (each power loss a
//! reboot too: `disk_model::power_loss`), must reopen to exactly the state the protocol allows,
//! pass `Env::check`, and reopen to the same state again after a crash right after that reopen
//! (or, from a weak head, to `L` after a power loss then). A power loss recovers the last durable
//! state `L` until the new meta is written, the new state `M` once its msync completed, and in
//! between `M` exactly when the slot holds `M`'s intact steady record. A process crash recovers
//! the newest record the slot holds intact, weak or steady: `M`'s once it is copied (in
//! `SyncMode::NoSync` its weak one, before the sync that stops), else that of the newest commit
//! before it (`NoSync`'s unsynced commits leave weak ones), else `L`.

use std::{fs, os::unix::fs::FileExt as _, time::Duration};

use super::{
    damage::tree,
    dup::{dup_key, Bound, OPTIONS, PACKED_SMALL},
    *,
};
use crate::{
    alloc::RunStats,
    catalog::{self, DupRecord},
    disk_model,
    env::{self, CrashAt},
    error::Result,
    file,
    meta::{slot_to_write, META_LEN},
    packed::PackedView,
    page::{self, Find, Val},
    tree::{read::descend, PageRead as _, TreeRoot},
};

/// How the machine went down once the process stopped.
#[derive(Clone, Copy, Debug)]
enum Loss {
    /// Only the process died: the page cache survives and reaches the disk.
    Process,
    /// The power failed: only flushed bytes (and durable file lengths) survive.
    Strict,
    /// The power failed after some unflushed pages (and perhaps the size) had been written back,
    /// some torn by sector (seeded).
    Random(u64),
}

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A closed file to crash commits on (closed cleanly, which trims its slack, or as a crash, which
/// keeps it: `crash_runs.rs`'s `runs_base`): its last commit's state and txn, the state before
/// it, its page size, and the workload a case runs on it: the changes of the two commits
/// `SyncMode::NoSync` cases leave unsynced (by round), the crashing commit's, whether that commit
/// grows the file (when it does not, [`run_case`] takes the base's slack as the reopened writer's
/// own growth, `PageFile::trust_tail`; when it does, the reopened writer distrusts any slack the
/// base kept, so its first growth cuts it), and a check of the layout the matrix relies on, run
/// on the state the crashing commit made (`dry_run`).
pub(super) struct Base {
    pub(super) dir: TempDir,
    pub(super) path: PathBuf,
    pub(super) state: State,
    pub(super) prev: State,
    pub(super) txn: u64,
    pub(super) page_size: u32,
    pub(super) unsynced: fn(&mut RwTxn, &mut State, u32),
    pub(super) crash: fn(&mut RwTxn, &mut State),
    pub(super) grows: bool,
    pub(super) layout: fn(&Env),
}

/// The pages the file holds past the newest state's page count (growth slack).
pub(super) fn slack(env: &Env) -> u64 {
    env.with_writer(|w| {
        w.file.file_len() / w.geometry.page_size() as u64 - u64::from(w.last.page_count)
    })
}

/// Build a [`Base`]: tables `a`, `b`, `g` and `h`; table `e` (low page numbers) dropped by the
/// second-to-last commit, so its pages are free records a reopened writer may reuse; and a filler
/// table `f`, sized to leave the file only a few pages of slack, dropped by the last commit, so
/// its pages are free but not yet reusable: a modest commit must grow the file.
pub(super) fn base(page_size: u32) -> Base {
    base_with(page_size, |_, _, _| {})
}

/// [`base`] with the tables `extra` writes (given the page size) in its first commit, after `h`.
fn base_with(page_size: u32, extra: impl FnOnce(&mut RwTxn, &mut State, u32)) -> Base {
    let (dir, path) = temp("tn_lmdb_crash");
    let env = Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..400 {
        put(&mut txn, &mut state, "a", &key(i), &value(1, i, 200));
    }
    for i in 0..200 {
        put(&mut txn, &mut state, "b", &key(i), &value(2, i, 100));
    }
    for i in 0..(30 * page_size / 1024) {
        put(&mut txn, &mut state, "e", &key(i), &value(5, i, 300));
    }
    for i in 0..50 {
        put(&mut txn, &mut state, "g", &key(i), &value(7, i, 100));
    }
    // About 72 pages between `e`'s pages and the end of the file, whatever the page size: the
    // crash commit's reused and new pages lie more than the merge gap apart (two data ranges).
    for i in 0..72 * page_size / 1000 {
        put(&mut txn, &mut state, "h", &key(i), &value(8, i, 900));
    }
    extra(&mut txn, &mut state, page_size);
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    for i in (0..400).step_by(2) {
        put(&mut txn, &mut state, "a", &key(i), &value(11, i, 220));
    }
    txn.commit().expect("commit");
    let per_page = u64::from(page_size) / 1000;
    let mut next = 0;
    while slack(&env) > 20 {
        let n = (slack(&env) - 14) * per_page;
        let mut txn = env.begin_rw().expect("begin");
        for _ in 0..n {
            put(&mut txn, &mut state, "f", &key(next), &value(6, next, 900));
            next += 1;
        }
        txn.commit().expect("commit");
    }
    let mut txn = env.begin_rw().expect("begin");
    drop_table(&mut txn, &mut state, "e");
    txn.commit().expect("commit");
    let prev = state.clone();
    let mut txn = env.begin_rw().expect("begin");
    drop_table(&mut txn, &mut state, "f");
    let txn = txn.commit().expect("commit");
    let left = slack(&env);
    assert!((1..=24).contains(&left), "{left} pages of slack");
    check(&env);
    drop(env);
    Base {
        dir,
        path,
        state,
        prev,
        txn,
        page_size,
        unsynced: unsynced_changes,
        crash: crash_changes,
        grows: true,
        layout: |_| {},
    }
}

/// Two small commits that `SyncMode::NoSync` cases leave unsynced before the crash commit.
fn unsynced_changes(txn: &mut RwTxn, state: &mut State, round: u32) {
    for i in (round..200).step_by(9) {
        put(txn, state, "b", &key(i), &value(30 + round, i, 120));
    }
}

/// The crashing commit: updates spread over `a` (copies of published pages), deletes in `b`,
/// table `g` dropped, and table `d` filled until the file grows (a size change to sync).
fn crash_changes(txn: &mut RwTxn, state: &mut State) {
    for i in (0..400).step_by(7) {
        put(txn, state, "a", &key(i), &value(21, i, 240));
    }
    for i in (0..200).step_by(4) {
        delete(txn, state, "b", &key(i));
    }
    drop_table(txn, state, "g");
    let start = txn.writer_mut().file.file_len();
    let mut i = 0;
    while txn.writer_mut().file.file_len() == start || i % 16 != 0 {
        put(txn, state, "d", &key(i), &value(24, i, 900));
        i += 1;
    }
}

/// How a case's environment commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    /// `SyncMode::Durable`: the crash commit is durable when it returns.
    Durable,
    /// `SyncMode::NoSync`: after the open's first commit (durable: [`durable_first_commit`]), two
    /// unsynced commits and the crash commit defer, and the `Env::sync` after them runs the
    /// durable steps.
    NoSync,
    /// `SyncMode::NoSync` with [`AUTO_PERIOD`] as `EnvOptions::sync_period`: after the open's
    /// first commit (durable), the two unsynced commits defer, and the crash commit, made with the
    /// writer's clock (`Writer::clock_at`) at the period, runs the durable steps itself, over its
    /// own pages and theirs.
    AutoSync,
}

impl Mode {
    /// The engine's sync mode.
    fn sync(self) -> SyncMode {
        match self {
            Mode::Durable => SyncMode::Durable,
            Mode::NoSync | Mode::AutoSync => SyncMode::NoSync,
        }
    }
}

/// [`Mode::AutoSync`]'s `EnvOptions::sync_period`: long enough that only the crash commit, made
/// with the writer's clock moved to it, reaches it.
const AUTO_PERIOD: Duration = Duration::from_secs(3600);

thread_local! {
    /// The `EnvOptions::gap_fill` every environment of this thread's matrix opens with
    /// ([`open_in`]; off, but under [`with_gap_fill`]).
    static GAP_FILL: std::cell::Cell<GapFill> = const { std::cell::Cell::new(GapFill::OFF) };
}

/// Run `f` (a matrix) with every environment it opens filling gaps as `gap_fill` says, and its
/// dry runs checking that the durable steps filled some at the largest width, and that auto chose
/// a width.
pub(super) fn with_gap_fill(gap_fill: GapFill, f: impl FnOnce()) {
    GAP_FILL.set(gap_fill);
    f();
    GAP_FILL.set(GapFill::OFF);
}

/// Open the file at `path` as `mode` commits.
fn open_in(path: &Path, mode: Mode) -> Env {
    let sync_period = (mode == Mode::AutoSync).then_some(AUTO_PERIOD);
    let gap_fill = GAP_FILL.get();
    Env::open(path, EnvOptions { sync_period, gap_fill, ..options(mode.sync()) }).expect("open")
}

/// The crash points of a commit, in protocol order.
const POINTS: [CrashAt; 7] = [
    CrashAt::BeforeSave,
    CrashAt::AfterSave,
    CrashAt::AfterStamp,
    CrashAt::AfterDataSync,
    CrashAt::AfterSizeSync,
    CrashAt::AfterMetaCopy,
    CrashAt::AfterMetaMsync,
];

/// True if `at` stops a commit in `mode` (else it stops the following sync).
fn stops_the_commit(mode: Mode, at: CrashAt) -> bool {
    mode != Mode::NoSync
        || matches!(at, CrashAt::BeforeSave | CrashAt::AfterSave | CrashAt::AfterStamp)
}

/// Copy the base to a tracked case file, open it as `mode` commits, make the commits (in
/// `SyncMode::NoSync` the open's first, durable, then the unsynced ones) and stop at `at` (`None`:
/// run to the end, with a sync). With `by_msync`, step 4 syncs the data by one `msync`
/// (`Writer::data_by_msync`), else by the sync `data_sync` picks. Returns the case's path, the
/// open's first txn, the last durable state `L` before the crash commit, the state `M` its last
/// commit made and the state before it, the slot `M`'s meta goes to, how the durable steps synced
/// the data, and what the writer reused and how it placed its runs. The environment is crashed.
pub(super) fn run_case(base: &Base, mode: Mode, by_msync: bool, at: Option<CrashAt>) -> Case {
    let path = base.dir.path().join("case");
    fs::copy(&base.path, &path).expect("copy");
    disk_model::track(&path);
    let first = first_txn(&path, base.page_size);
    let env = open_in(&path, mode);
    env.with_writer(|w| {
        w.data_by_msync = by_msync;
        w.alloc.log_pages = true;
        if !base.grows {
            // The base's slack stands for room this writer grew earlier in its process: a
            // reopened writer cuts and regrows an inherited tail at its first growth.
            w.file.trust_tail();
        }
    });
    let mut latest = base.state.clone();
    let mut durable = (base.txn, base.state.clone());
    if mode != Mode::Durable {
        assert_eq!(durable_first_commit(&env, &mut latest), first, "{mode:?}");
        durable = (first, latest.clone());
        for round in 0..2 {
            let mut txn = env.begin_rw().expect("begin");
            (base.unsynced)(&mut txn, &mut latest, round);
            txn.commit().expect("an unsynced commit");
        }
        let s = env.io_stats();
        let counts = (s.auto_syncs, s.nosync_commits);
        assert_eq!(counts, (1, 2), "{mode:?}: the first commit synced, the unsynced ones deferred");
    }
    if mode == Mode::AutoSync {
        env.with_writer(|w| w.clock_at = Some(w.durable_at + AUTO_PERIOD));
    }
    let before = latest.clone();
    let slot = env.with_writer(|w| slot_to_write(&w.slots()));
    let mut txn = env.begin_rw().expect("begin");
    (base.crash)(&mut txn, &mut latest);
    let stopped = match at {
        Some(at) if stops_the_commit(mode, at) => {
            txn.writer_mut().crash_at = Some(at);
            Some(txn.commit().expect_err("the commit stops at the crash point"))
        }
        _ => {
            txn.commit().expect("commit");
            let auto_syncs = u64::from(mode != Mode::Durable) + u64::from(mode == Mode::AutoSync);
            assert_eq!(env.io_stats().auto_syncs, auto_syncs, "{mode:?}: the crash commit");
            env.with_writer(|w| w.crash_at = at);
            env.sync().err()
        }
    };
    match (at, stopped) {
        (Some(at), Some(e)) => {
            assert!(e.to_string().contains("injected crash"), "{at:?}: {e}");
            assert!(env.failure().is_some(), "{at:?}: the crash point latched");
        }
        (None, None) => {}
        (at, e) => panic!("{at:?}: {e:?}"),
    }
    let (by_file, reused, runs) =
        env.with_writer(|w| (w.data_by_file, w.alloc.load_log.len(), w.alloc.run_stats));
    let s = env.io_stats();
    let (gaps_filled, gap_auto_steps, gap_fill_refusals) =
        (s.gaps_filled, s.gap_auto_steps, s.gap_fill_refusals);
    env.crash();
    Case {
        path,
        first,
        durable,
        latest,
        before,
        slot,
        by_file,
        reused,
        runs,
        gaps_filled,
        gap_auto_steps,
        gap_fill_refusals,
    }
}

/// What [`run_case`] left.
pub(super) struct Case {
    /// The case's file.
    pub(super) path: PathBuf,
    /// The txn the open gave its first transaction (`first_txn` of the base's slots): the crash
    /// commit's in `SyncMode::Durable`, the open's first commit's else.
    pub(super) first: u64,
    /// The last durable state before the crash commit (`L`) and its txn: the base's in
    /// `SyncMode::Durable`, the open's first commit's else.
    pub(super) durable: (u64, State),
    /// The state the last commit made (`M`).
    pub(super) latest: State,
    /// The state before it: the base's in `SyncMode::Durable`, the unsynced commits' else.
    pub(super) before: State,
    /// The meta slot `M`'s record goes to.
    pub(super) slot: usize,
    /// The last durable steps synced the data by one `fdatasync` of the file (on macOS also by
    /// the `msync`s of `DataSync::Ranges`, which take its place there).
    pub(super) by_file: bool,
    /// The pages the writer took from the free list.
    pub(super) reused: usize,
    /// How the writer placed the overflow runs of the case's commits.
    pub(super) runs: RunStats,
    /// The gaps the case's durable steps filled (`EnvOptions::gap_fill`, [`with_gap_fill`]).
    pub(super) gaps_filled: u64,
    /// The case's scattered durable steps whose width `GapFill::Auto` chose.
    pub(super) gap_auto_steps: u64,
    /// `IoStats::gap_fill_refusals`: 1 when the file system refused the gap fill's reads, which
    /// turned it off.
    pub(super) gap_fill_refusals: u64,
}

/// Every key of `want` reads back through a get (a point read, not the cursor `newest_state`
/// scans with), byte for byte.
pub(super) fn assert_gets(env: &Env, want: &State, case: &str) {
    let ro = env.begin_ro().expect("ro");
    for (name, rows) in want {
        let table = ro.open_table(name).expect("open").expect("the table exists");
        for (k, v) in rows {
            let got = ro.get(&table, k).expect("get");
            assert!(got == Some(&v[..]), "{case}: table {name} key {k:?}: the get differs");
        }
    }
}

/// One cell of the matrix: stop at `at`, go down by `loss`, and reopen (twice, with a crash and
/// a strict power loss right after the first reopen) to exactly the state allowed.
fn crash_case(base: &Base, mode: Mode, by_msync: bool, at: CrashAt, loss: Loss) {
    let case = format!("{mode:?} by_msync={by_msync} {at:?} {loss:?}");
    let Case { path, first, durable, latest, before, slot, .. } =
        run_case(base, mode, by_msync, Some(at));
    match loss {
        Loss::Process => {}
        Loss::Strict => disk_model::power_loss(&path),
        Loss::Random(seed) => disk_model::power_loss_random(&path, seed),
    }
    let process = matches!(loss, Loss::Process);
    let m_txn = first + if mode == Mode::Durable { 0 } else { 3 };
    // The slot every record of the case's commits but the open's first went to (the unsynced
    // commits' weak ones, the crash commit's, and the sync's steady one): a process crash
    // recovers what it holds intact, weak or steady, a power loss (a reboot) only a steady record.
    let record = slot_record(&path, base.page_size, slot);
    let trusted = record.filter(|&(_, weak)| process || !weak);
    let reaches_m = trusted.is_some_and(|(txn, _)| txn == m_txn);
    // `M`'s steady record survives exactly when step 6 completed, may during it, and must not
    // before it; in `NoSync` its weak record is in the slot from the crash commit on.
    let weak_m = mode == Mode::NoSync && !stops_the_commit(mode, at);
    match (at, loss) {
        (CrashAt::AfterMetaMsync, _) => assert!(reaches_m, "{case}: the msynced meta survives"),
        (CrashAt::AfterMetaCopy, Loss::Process) => {
            assert!(reaches_m, "{case}: the page cache keeps the meta")
        }
        (CrashAt::AfterMetaCopy, Loss::Strict) => {
            assert!(!reaches_m, "{case}: an unflushed meta is lost")
        }
        (CrashAt::AfterMetaCopy, Loss::Random(_)) => {}
        (_, Loss::Process) if weak_m => assert!(reaches_m, "{case}: the crash commit's weak meta"),
        _ => assert!(!reaches_m, "{case}: no meta of M is trusted before step 6"),
    }
    // Short of `M`, a process crash in `SyncMode::NoSync` recovers the unsynced commits' weak
    // head, which nothing replaced; anything else `L`.
    let weak_before = process && mode != Mode::Durable;
    if weak_before && !reaches_m {
        assert_eq!(trusted, Some((first + 2, true)), "{case}: the unsynced commits' head");
    }
    let mut want = match () {
        _ if reaches_m => (m_txn, &latest, trusted.is_some_and(|(_, weak)| weak)),
        _ if weak_before => (first + 2, &before, true),
        _ => (durable.0, &durable.1, false),
    };
    for reopen in 0..2 {
        let (want_txn, want_state, weak) = want;
        let env = open_in(&path, mode);
        assert_eq!(env.stat().txn_id, want_txn, "{case}: reopen {reopen}");
        assert!(newest_state(&env) == *want_state, "{case}: reopen {reopen}: the state differs");
        assert_gets(&env, want_state, &format!("{case}: reopen {reopen}"));
        env.crash();
        disk_model::power_loss(&path);
        // A weak head stays weak on disk until a sync: the reboot rolls it back to `L`.
        if weak {
            want = (durable.0, &durable.1, false);
        }
    }
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
}

/// Run the crash commit (or the sync after it) to the end, checked to exercise what the matrix
/// needs: the data synced by the `fdatasync` `data_sync` picks for pages that form several
/// ranges (on macOS the `msync`s of `DataSync::Ranges`; or, `by_msync`, by one `msync`), pages
/// reused from the free list, a growth of the file exactly when the base says so (a growth's size
/// sync is an `fsync`, which writes every page: only a commit that does not grow shows what one
/// `msync` alone covers), and the base's `layout`.
fn dry_run(base: &Base, mode: Mode, by_msync: bool) {
    let Case {
        path, latest, by_file, reused, gaps_filled, gap_auto_steps, gap_fill_refusals, ..
    } = run_case(base, mode, by_msync, None);
    assert_eq!(by_file, !by_msync, "{mode:?}: by_msync={by_msync}: synced by fdatasync");
    assert!(reused > 0, "{mode:?}: pages were reused");
    // Off, no gap is filled; at the largest gap fill the crash commit's runs always leave gaps to
    // fill. A smaller one fills those the layout leaves short enough, perhaps none. Auto chose the
    // width of the crash commit's durable step, a scattered one (its warm-up's first, 8 pages,
    // the crash commit's in `Mode::Durable`), and filled what that width finds. On a file system
    // that refuses reads that do not wait for the disk (tmpfs), the first gap read turns gap fill
    // off instead (at the largest, always), and the rest of the case runs without it: auto files
    // no step from the one refused on.
    let gap_fill = GAP_FILL.get();
    let case =
        format!("{mode:?} page size {} gap fill {gap_fill}, by_msync={by_msync}", base.page_size);
    let refused = gap_fill.is_on() && file::refuses_nowait_reads(base.dir.path());
    let counts = (gaps_filled, gap_fill_refusals);
    match gap_fill {
        GapFill::OFF => assert_eq!(counts, (0, 0), "{case}: off fills nothing"),
        GapFill::Pages(MAX_GAP_FILL) if refused => {
            assert_eq!(counts, (0, 1), "{case}: refused, so turned off")
        }
        _ if refused => assert!(gaps_filled == 0 && gap_fill_refusals <= 1, "{case}: {counts:?}"),
        GapFill::Pages(MAX_GAP_FILL) => {
            assert!(gaps_filled > 0 && gap_fill_refusals == 0, "{case}: {counts:?}")
        }
        _ => {
            assert_eq!(gap_fill_refusals, 0, "{case}");
            eprintln!("{case}: {gaps_filled} gaps filled");
        }
    }
    match gap_fill {
        GapFill::Auto if refused => {
            let chose = gap_auto_steps + gap_fill_refusals > 0;
            assert!(chose, "{case}: auto chose a width, filed or refused: {counts:?}")
        }
        GapFill::Auto => assert!(gap_auto_steps > 0, "{case}: auto chose a width"),
        _ => assert_eq!(gap_auto_steps, 0, "{case}: only auto chooses"),
    }
    if refused {
        eprintln!("{case}: the file system refuses RWF_NOWAIT: gap fill turned itself off");
    }
    let base_len = fs::metadata(&base.path).expect("metadata").len();
    let grew = fs::metadata(&path).expect("metadata").len() > base_len;
    assert_eq!(grew, base.grows, "{mode:?}: the file grew");
    let env = open_in(&path, mode);
    assert_eq!(newest_state(&env), latest);
    (base.layout)(&env);
    drop(env);
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
}

/// Every crash point of `base`'s crash commit x {process crash, strict power loss, `seeds`
/// random power losses} x {`Durable`, `NoSync`} x {the `fdatasync` step 4 picks, one `msync`}.
pub(super) fn matrix(base: &Base, seeds: u64) {
    matrix_of(base, seeds, &[Mode::Durable, Mode::NoSync]);
}

/// [`matrix`] in each of `modes`.
pub(super) fn matrix_of(base: &Base, seeds: u64, modes: &[Mode]) {
    for &mode in modes {
        for by_msync in [false, true] {
            dry_run(base, mode, by_msync);
            for (n, at) in POINTS.into_iter().enumerate() {
                for loss in [Loss::Process, Loss::Strict] {
                    crash_case(base, mode, by_msync, at, loss);
                }
                for seed in 0..seeds {
                    crash_case(base, mode, by_msync, at, Loss::Random(seed * 64 + n as u64));
                }
            }
        }
    }
}

/// The matrix: every crash point x {process crash, strict power loss, random power loss with
/// three seeds} x {`Durable`, `NoSync`} x {data synced by `fdatasync`, by one `msync`} reopens
/// to exactly the allowed state, which checks and survives a crash right after the reopen.
#[test]
fn every_crash_point_recovers_the_state_the_protocol_allows() {
    matrix(&base(4096), 3);
}

/// The matrix at two page sizes, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_recovers_the_state_the_protocol_allows_heavy() {
    for page_size in [4096, 16384] {
        matrix(&base(page_size), 40);
    }
}

/// The matrix with `EnvOptions::gap_fill` at its largest, in every environment it opens: the
/// durable steps rewrite the gaps between the crash commit's runs before the data sync (its dry
/// runs check that they filled some), and every cell recovers exactly as without it.
#[test]
fn every_crash_point_recovers_the_state_the_protocol_allows_with_gap_fill() {
    with_gap_fill(GapFill::Pages(MAX_GAP_FILL), || matrix(&base(4096), 3));
}

/// The matrix with `GapFill::Auto`: auto chooses the width of the crash commit's durable step
/// (the warm-up's first, 8 pages, in `Mode::Durable`) before its data sync and files its outcome
/// after, and every cell recovers exactly as without it. The fills' own crash safety at a width
/// that always fills is the matrix at `MAX_GAP_FILL`'s.
#[test]
fn every_crash_point_recovers_the_state_the_protocol_allows_with_auto_gap_fill() {
    with_gap_fill(GapFill::Auto, || matrix(&base(4096), 3));
}

/// The gap-filled matrix at two page sizes, at 4 pages, at the largest and at auto, with many
/// random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_recovers_the_state_the_protocol_allows_with_gap_fill_heavy() {
    for page_size in [4096, 16384] {
        for gap_fill in [GapFill::Pages(4), GapFill::Pages(MAX_GAP_FILL), GapFill::Auto] {
            with_gap_fill(gap_fill, || matrix(&base(page_size), 40));
        }
    }
}

/// The matrix of a `NoSync` commit past `EnvOptions::sync_period` ([`Mode::AutoSync`]), which
/// runs the durable steps over its own pages and those of the two commits before it, which
/// deferred theirs: every crash point recovers by the `Durable` rules, `L` being the last synced
/// state (the base's) and `M` the crash commit's, under a process crash, a strict power loss and
/// random power losses.
#[test]
fn every_crash_point_of_an_automatic_sync_recovers_the_state_the_protocol_allows() {
    matrix_of(&base(4096), 3, &[Mode::AutoSync]);
}

/// The automatic sync's matrix with `EnvOptions::gap_fill` at its largest: the sync fills the
/// gaps between the runs of its own pages and the deferred commits' pages.
#[test]
fn every_crash_point_of_an_automatic_sync_recovers_the_state_the_protocol_allows_with_gap_fill() {
    with_gap_fill(GapFill::Pages(MAX_GAP_FILL), || matrix_of(&base(4096), 3, &[Mode::AutoSync]));
}

/// The automatic sync's matrix with `GapFill::Auto`.
#[test]
fn every_crash_point_of_an_automatic_sync_recovers_the_state_the_protocol_allows_with_auto_gap_fill(
) {
    with_gap_fill(GapFill::Auto, || matrix_of(&base(4096), 3, &[Mode::AutoSync]));
}

/// The automatic sync's matrix at two page sizes, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_of_an_automatic_sync_recovers_the_state_the_protocol_allows_heavy() {
    for page_size in [4096, 16384] {
        matrix_of(&base(page_size), 40, &[Mode::AutoSync]);
    }
}

/// A commit that grew the file survives a power loss once it returned, by either data sync: the
/// `fdatasync` makes the new length durable with the data, and after one `msync` the size sync
/// does, before the meta naming it.
#[test]
fn a_commit_that_grew_the_file_survives_a_power_loss() {
    grown_commit_survives_a_power_loss(&base(4096));
}

/// [`a_commit_that_grew_the_file_survives_a_power_loss`] on `base`.
fn grown_commit_survives_a_power_loss(base: &Base) {
    for by_msync in [false, true] {
        let Case { path, first, latest, .. } = run_case(base, Mode::Durable, by_msync, None);
        disk_model::power_loss(&path);
        disk_model::untrack(&path);
        let env = open(&path, SyncMode::Durable);
        assert_eq!(env.stat().txn_id, first, "by_msync={by_msync}");
        assert_eq!(newest_state(&env), latest, "by_msync={by_msync}");
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}

/// The older durable state stays recoverable until a commit overwrites its meta: a commit
/// stopped once its data reached the disk has reused no page that state reaches (only pages
/// freed by txns no later than both slots), so if the newest meta is then found damaged, open
/// falls back to the older state intact.
#[test]
fn the_older_state_stays_recoverable_until_its_meta_is_overwritten() {
    older_state_stays_recoverable(&base(4096));
}

/// [`the_older_state_stays_recoverable_until_its_meta_is_overwritten`] on `base`.
fn older_state_stays_recoverable(base: &Base) {
    let Case { path, slot, reused, .. } =
        run_case(base, Mode::Durable, false, Some(CrashAt::AfterSizeSync));
    assert!(reused > 0, "the commit reused pages");
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    // Damage the newest meta (txn `base.txn`, in the slot the commit was not writing).
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let at = (1 - slot) as u64 * u64::from(base.page_size) + 20;
    let mut b = [0];
    file.read_exact_at(&mut b, at).expect("read");
    file.write_all_at(&[b[0] ^ 0x10], at).expect("write");
    let env = open(&path, SyncMode::Durable);
    assert_eq!(env.stat().txn_id, base.txn - 1);
    assert_eq!(newest_state(&env), base.prev);
}

/// How the machine went down after [`weak_case`]'s `SyncMode::NoSync` commits.
#[derive(Clone, Copy, Debug)]
pub(super) enum WeakLoss {
    /// Only the process died, in the same boot: the page cache survives.
    Process,
    /// The process died inside the last commit's copy of its weak meta (after any sync): the
    /// record's first [`TORN_AT`] bytes are the new one's, the rest the slot's record before it.
    Torn,
    /// Only the process died, and the reopen has `EnvOptions::trust_unsynced` off.
    Distrusted,
    /// The process died on a host whose boot id cannot be read: the weak metas carry zeros.
    UnknownBoot,
    /// A strict power loss, a reboot.
    Strict,
    /// A random-writeback power loss, a reboot (seeded).
    Random(u64),
    /// The volume lost in the same boot (pulled, then plugged back in), after the kernel wrote
    /// back the weak meta's page alone: the head is trusted, but the pages it names that were
    /// written since the last sync are lost.
    Volume,
    /// [`Self::Volume`] after random writeback of the other pages, some torn by sector (seeded).
    VolumeRandom(u64),
}

/// Where [`WeakLoss::Torn`] cuts the record: inside the catalog root, so its txn is the new one's.
const TORN_AT: usize = 40;

/// Every way down of [`weak_case`], with `seeds` random power losses and as many random volume
/// losses.
pub(super) fn weak_losses(seeds: u64) -> Vec<WeakLoss> {
    let mut losses = vec![
        WeakLoss::Process,
        WeakLoss::Torn,
        WeakLoss::Distrusted,
        WeakLoss::UnknownBoot,
        WeakLoss::Strict,
        WeakLoss::Volume,
    ];
    losses.extend((0..seeds).map(WeakLoss::Random));
    losses.extend((0..seeds).map(WeakLoss::VolumeRandom));
    losses
}

/// The weak metas of `SyncMode::NoSync` (no automatic sync) on `base`: after the open's first
/// commit (durable: [`durable_first_commit`]), its `commits` commits (1 to 3: the two unsynced
/// ones, then the crash commit), with a sync after the first when `synced`, go down by `loss`,
/// then reopen to exactly the state allowed. A process crash in the same boot recovers the last
/// commit, by its weak meta, once open's check of the pages written since the last sync passes;
/// every other way down the last sync (the open's first commit's, or the first of the `commits`
/// when `synced`): a torn weak record is invalid, a distrusted, zero-tagged or other boot's one
/// passed over for the steady slot beside it, and a head trusted after a volume loss in the same
/// boot fails that check (a strict loss always, a random one unless every page it names survived,
/// when it opens at the head). The reopen checks, and a power loss right after it recovers the last
/// sync too (a weak head stays weak until a sync). Returns whether the reopen fell back from a
/// trusted weak head.
pub(super) fn weak_case(base: &Base, commits: u32, synced: bool, loss: WeakLoss) -> bool {
    let case = format!("{commits} commits, synced {synced}, {loss:?}");
    let path = base.dir.path().join("weak");
    fs::copy(&base.path, &path).expect("copy");
    disk_model::track(&path);
    if let WeakLoss::UnknownBoot = loss {
        disk_model::set_boot_id(None);
    }
    let first = first_txn(&path, base.page_size);
    let env = open_in(&path, Mode::NoSync);
    if !base.grows {
        env.with_writer(|w| w.file.trust_tail());
    }
    let mut latest = (first, base.state.clone());
    assert_eq!(durable_first_commit(&env, &mut latest.1), first, "{case}");
    let mut last_sync = latest.clone();
    let mut torn = None;
    for n in 0..commits {
        let slot = env.with_writer(|w| slot_to_write(&w.slots()));
        let size = u64::from(base.page_size);
        let before = env.with_writer(|w| {
            w.file.pages(slot as u32, size as usize, META_LEN).expect("the slot").to_vec()
        });
        let mut txn = env.begin_rw().expect("begin");
        if n < 2 {
            (base.unsynced)(&mut txn, &mut latest.1, n);
        } else {
            (base.crash)(&mut txn, &mut latest.1);
        }
        latest.0 = txn.commit().expect("a NoSync commit");
        assert_eq!(latest.0, first + u64::from(n) + 1, "{case}");
        torn = Some((slot as u64 * size, before));
        if synced && n == 0 {
            env.sync().expect("sync");
            last_sync = latest.clone();
        }
    }
    let stats = env.io_stats();
    assert_eq!((stats.nosync_commits, stats.auto_syncs), (u64::from(commits), 1), "{case}");
    let volume = matches!(loss, WeakLoss::Volume | WeakLoss::VolumeRandom(_));
    if volume {
        env.with_writer(|w| {
            if let Some(slot) = w.slots().iter().position(|s| s.is_some_and(|s| s.weak)) {
                w.file.msync_page(slot as u32, base.page_size as usize).expect("the writeback");
            }
        });
    }
    let head = latest.0 > last_sync.0;
    env.crash();
    match loss {
        WeakLoss::Process | WeakLoss::Distrusted | WeakLoss::UnknownBoot => {}
        WeakLoss::Torn => {
            let (at, before) = torn.expect("a commit");
            let file = fs::OpenOptions::new().write(true).open(&path).expect("open");
            file.write_all_at(&before[TORN_AT..], at + TORN_AT as u64).expect("tear");
        }
        WeakLoss::Strict => disk_model::power_loss(&path),
        WeakLoss::Random(seed) => disk_model::power_loss_random(&path, seed),
        WeakLoss::Volume => disk_model::volume_loss(&path),
        WeakLoss::VolumeRandom(seed) => disk_model::volume_loss_random(&path, seed),
    }
    let mut want = match loss {
        WeakLoss::Process => &latest,
        _ => &last_sync,
    };
    let trust_unsynced = !matches!(loss, WeakLoss::Distrusted);
    let mut fell_back = false;
    for reopen in 0..2 {
        let options = EnvOptions { trust_unsynced, ..options(SyncMode::NoSync) };
        let env = Env::open(&path, options).expect("reopen");
        if reopen == 0 {
            // Open checks a weak head it trusts, and only then: after a process crash it passes,
            // after a strict volume loss it fails, after a random one either.
            let checked = env::take_weak_head_check();
            let trusted = head && (matches!(loss, WeakLoss::Process) || volume);
            assert_eq!(checked.is_some(), trusted, "{case}: {checked:?}");
            match (loss, checked) {
                (WeakLoss::Process, Some(found)) => {
                    assert!(found.is_ok_and(|pages| pages > 0), "{case}: {found:?}")
                }
                (WeakLoss::Volume, Some(found)) => assert!(found.is_err(), "{case}"),
                (WeakLoss::VolumeRandom(_), Some(Ok(_))) => want = &latest,
                _ => {}
            }
            fell_back = matches!(checked, Some(Err(())));
        }
        assert_eq!(env.stat().txn_id, want.0, "{case}: reopen {reopen}");
        assert!(newest_state(&env) == want.1, "{case}: reopen {reopen}: the state differs");
        assert_gets(&env, &want.1, &format!("{case}: reopen {reopen}"));
        env.crash();
        disk_model::power_loss(&path);
        want = &last_sync;
    }
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
    if let WeakLoss::UnknownBoot = loss {
        disk_model::reboot();
    }
    fell_back
}

/// [`weak_case`] on `base` after one to three commits, with and without a sync after the first,
/// every way down (but a torn weak record where the sync wrote the last record). Some random
/// volume loss must fall back from a trusted head too.
pub(super) fn weak_matrix(base: &Base, seeds: u64) {
    let mut random_falls_back = 0;
    for commits in 1..=3 {
        for synced in [false, true] {
            for loss in weak_losses(seeds) {
                if !(synced && commits == 1 && matches!(loss, WeakLoss::Torn)) {
                    let fell_back = weak_case(base, commits, synced, loss);
                    random_falls_back +=
                        u32::from(fell_back && matches!(loss, WeakLoss::VolumeRandom(_)));
                }
            }
        }
    }
    assert!(seeds == 0 || random_falls_back > 0, "a random volume loss falls back");
}

/// `SyncMode::NoSync`'s weak metas: after one to three commits (and a sync after the first, or
/// none), a process crash in the same boot recovers the last commit; a torn weak record, a reopen
/// with `trust_unsynced` off, an unknown boot id, a strict and random power losses (reboots)
/// recover the last sync exactly; every reopen checks.
#[test]
fn a_process_crash_keeps_every_nosync_commit_and_a_reboot_recovers_the_last_sync() {
    weak_matrix(&base(4096), 3);
}

/// The weak matrix at two page sizes, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn a_process_crash_keeps_every_nosync_commit_and_a_reboot_recovers_the_last_sync_heavy() {
    for page_size in [4096, 16384] {
        weak_matrix(&base(page_size), 30);
    }
}

/// A copy of `base` to crash on, tracked, opened in `SyncMode::NoSync` with the open's first
/// commit (durable: [`durable_first_commit`]) and then its unsynced commits (`rounds` of them, at
/// most 2) made: the environment, the last sync (the first commit's txn and state) and the state
/// the unsynced commits made.
fn weak_copy(base: &Base, name: &str, rounds: u32) -> (PathBuf, Env, (u64, State), State) {
    let path = base.dir.path().join(name);
    fs::copy(&base.path, &path).expect("copy");
    disk_model::track(&path);
    let first = first_txn(&path, base.page_size);
    let env = open_in(&path, Mode::NoSync);
    let mut state = base.state.clone();
    assert_eq!(durable_first_commit(&env, &mut state), first);
    let synced = (first, state.clone());
    for round in 0..rounds {
        let mut txn = env.begin_rw().expect("begin");
        (base.unsynced)(&mut txn, &mut state, round);
        txn.commit().expect("a NoSync commit");
    }
    (path, env, synced, state)
}

/// Open `path` in `SyncMode::NoSync`, trusting a weak head of this boot or not.
fn open_trusting(path: &Path, trust_unsynced: bool) -> Result<Env> {
    Env::open(path, EnvOptions { trust_unsynced, ..options(SyncMode::NoSync) })
}

/// A copy of `base` with its two unsynced commits and its crash commit made in
/// `SyncMode::NoSync` since the open's first commit (the last sync: [`weak_copy`]), then the
/// kernel's writeback of the weak meta's page alone (an msync of that page stands for it), then a
/// process crash: the path, the last sync's txn and state, and the head's.
fn lost_head(base: &Base, name: &str) -> (PathBuf, (u64, State), (u64, State)) {
    let (path, env, synced, mut latest) = weak_copy(base, name, 2);
    let mut txn = env.begin_rw().expect("begin");
    (base.crash)(&mut txn, &mut latest);
    let head = txn.commit().expect("a NoSync commit");
    env.with_writer(|w| {
        let slot = w.slots().iter().position(|s| s.is_some_and(|s| s.weak)).expect("a weak slot");
        w.file.msync_page(slot as u32, base.page_size as usize).expect("the meta page's writeback");
    });
    env.crash();
    (path, synced, (head, latest))
}

/// Reopen `path`, trusting a weak head of this boot, to exactly the head's state (`head`, after
/// open's check of its pages since the last sync passed) or the last sync (`synced`, after that
/// check failed), and pass `Env::check`: whether it fell back.
fn reopen_head_or_last_sync(
    path: &Path,
    synced: &(u64, State),
    head: &(u64, State),
    case: &str,
) -> bool {
    let env = open_trusting(path, true).unwrap_or_else(|e| panic!("{case}: {e:?}"));
    let checked = env::take_weak_head_check().unwrap_or_else(|| panic!("{case}: no check"));
    let want = if checked.is_ok() { head } else { synced };
    assert_eq!(env.stat().txn_id, want.0, "{case}: {checked:?}");
    assert!(newest_state(&env) == want.1, "{case}: the state differs");
    checked.is_err()
}

/// A volume lost without a reboot (pulled, then plugged back in the same boot) keeps the boot id,
/// so a weak meta the kernel wrote back is trusted though the pages it names may be lost. Open
/// checks the pages it names that were written since the last sync, and falls back to the last
/// sync when any fails: here the kernel wrote back the weak meta's page before the others, then
/// the volume went. Over random writebacks (some pages torn by sector) each copy opens to exactly
/// the head's state or the last sync, never `Corrupt`, and checks; after a strict loss it opens
/// at the last sync, which checks and takes a commit. With `trust_unsynced` off every one opens at
/// the last sync, unchecked.
#[test]
fn a_volume_lost_in_the_same_boot_opens_at_the_last_sync() {
    volume_lost_opens_at_the_last_sync(&base(4096), 32);
}

/// [`a_volume_lost_in_the_same_boot_opens_at_the_last_sync`] on the dup base and at 16 KiB pages,
/// with many random writebacks.
#[test]
#[ignore = "heavy: run with --release"]
fn a_volume_lost_in_the_same_boot_opens_at_the_last_sync_heavy() {
    volume_lost_opens_at_the_last_sync(&base(16384), 200);
    volume_lost_opens_at_the_last_sync(&dup_base(4096, Bound::Default), 200);
}

/// [`a_volume_lost_in_the_same_boot_opens_at_the_last_sync`] on `base` with `seeds` random
/// writebacks.
fn volume_lost_opens_at_the_last_sync(base: &Base, seeds: u64) {
    let (path, synced, head) = lost_head(base, "lost");
    let mut fell_back = 0;
    for seed in 0..seeds {
        let copy = base.dir.path().join(format!("lost{seed}"));
        disk_model::copy_tracked(&path, &copy);
        disk_model::volume_loss_random(&copy, seed);
        let case = format!("{seed}");
        fell_back += u32::from(reopen_head_or_last_sync(&copy, &synced, &head, &case));
        let env = open_trusting(&copy, false).expect("the last sync");
        assert_eq!(
            env::take_weak_head_check(),
            None,
            "seed {seed}: a distrusted head is unchecked"
        );
        assert!(newest_state(&env) == synced.1, "seed {seed}: the last sync");
        drop(env);
        disk_model::untrack(&copy);
        fs::remove_file(&copy).expect("remove");
    }
    assert!(fell_back > 0, "some writeback leaves the head naming lost pages");
    disk_model::volume_loss(&path);
    let strict = reopen_head_or_last_sync(&path, &synced, &head, "strict");
    assert!(strict, "a strict loss falls back");
    // Open wrote nothing: the head is still in its slot, and the next open falls back again.
    let again = reopen_head_or_last_sync(&path, &synced, &head, "strict again");
    assert!(again, "it falls back again");
    let env = open_trusting(&path, true).expect("the last sync");
    let mut state = synced.1.clone();
    let mut txn = env.begin_rw().expect("begin");
    let next = after_steady(synced.0);
    assert!(next > head.0);
    assert_eq!(txn.writer_mut().alloc.txn(), next, "past the head's txn");
    put(&mut txn, &mut state, "a", &key(0), b"on top");
    txn.commit().expect("a commit on the last sync");
    drop(env);
    let env = open_trusting(&path, true).expect("reopen");
    assert!(newest_state(&env) == state, "the commit on the last sync");
    drop(env);
    disk_model::untrack(&path);
}

/// The case reads cannot see: a page the head names, written since the last sync, torn by sector
/// so that its header (txn, kind, keys) is the new one but a later sector is the old bytes. Reads
/// check a page's header and identity, not its CRC, so only a full check finds it, and open's
/// check of a trusted weak head is one. For every page that differs between the head's file (a
/// process crash) and the last sync's (a strict volume loss), a copy holding the head's bytes
/// with that page alone torn opens to exactly the head's state (the page is one the head does not
/// name: written and freed since the sync) or falls back to the last sync, and checks. On the dup
/// base the head names nested trees, runs and packed sets written since the sync, so every kind of
/// page is torn; most pages fall back, among them some whose header still names the head's txn.
#[test]
fn a_page_torn_by_sector_since_the_last_sync_falls_back_at_open() {
    let base = dup_base(4096, Bound::Default);
    let (path, synced, head) = lost_head(&base, "torn");
    let new = fs::read(&path).expect("the head's bytes");
    let old_path = base.dir.path().join("torn-old");
    disk_model::copy_tracked(&path, &old_path);
    disk_model::volume_loss(&old_path);
    let mut old = fs::read(&old_path).expect("the last sync's bytes");
    old.resize(new.len(), 0);
    disk_model::untrack(&old_path);
    disk_model::untrack(&path);
    let size = base.page_size as usize;
    let (mut torn, mut fell_back, mut header_kept) = (0, 0, 0);
    let copy = base.dir.path().join("torn-copy");
    for p in 2..new.len() / size {
        let page = p * size..(p + 1) * size;
        let Some(sector) = (0..size / 512)
            .rev()
            .map(|i| page.start + i * 512..page.start + (i + 1) * 512)
            .find(|s| new[s.clone()] != old[s.clone()])
        else {
            continue;
        };
        let mut bytes = new.clone();
        bytes[sector.clone()].copy_from_slice(&old[sector.clone()]);
        fs::write(&copy, &bytes).expect("write the torn copy");
        torn += 1;
        let falls = reopen_head_or_last_sync(&copy, &synced, &head, &format!("page {p}"));
        fell_back += u32::from(falls);
        let named =
            page::kind(&bytes[page.clone()]).is_ok() && page::txn(&bytes[page.clone()]) == head.0;
        header_kept += u32::from(falls && sector.start > page.start && named);
    }
    fs::remove_file(&copy).expect("remove");
    assert!(torn > 20 && fell_back > torn / 2, "{fell_back} of {torn} torn pages fell back");
    assert!(
        header_kept > torn / 2,
        "{header_kept} torn pages kept the head's header: found by CRC"
    );
}

/// Open's check of a trusted weak head walks the free tree too, below its root: here the head's
/// one NoSync commit since the last sync (the open's first commit, durable) drops a large table,
/// so its free tree has two levels, written since the sync. After a volume lost in the same boot
/// that keeps the weak meta's page and loses a sector of one free-tree leaf (a byte of its last
/// sector changed), the open falls back to the last sync, which checks.
#[test]
fn a_free_tree_leaf_lost_since_the_last_sync_falls_back_at_open() {
    let (_dir, path) = temp("tn_lmdb_lost_free");
    let env = open(&path, SyncMode::Durable);
    let mut synced = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..200_u32 {
        put(&mut txn, &mut synced, "t", &i.to_be_bytes(), &value(1, i, 40));
    }
    for i in 0..4400_u32 {
        put(&mut txn, &mut synced, "big", &i.to_be_bytes(), &value(2, i, 900));
    }
    txn.commit().expect("commit");
    drop(env);
    disk_model::track(&path);
    let env = open(&path, SyncMode::NoSync);
    let synced_txn = durable_first_commit(&env, &mut synced);
    let mut txn = env.begin_rw().expect("begin");
    drop_table(&mut txn, &mut synced.clone(), "big");
    txn.commit().expect("a NoSync commit");
    let (free, size) = env.with_writer(|w| (w.last.free, w.geometry.page_size()));
    assert!(free.height >= 2 && free.root_txn as u64 > synced_txn, "{free:?}");
    let leaf = env.with_writer(|w| {
        let root = w.file.pages(free.root, size, size).expect("the free root");
        page::branch_child(root, 0).expect("a child").0
    });
    env.with_writer(|w| {
        let slot = w.slots().iter().position(|s| s.is_some_and(|s| s.weak)).expect("a weak slot");
        w.file.msync_page(slot as u32, size).expect("the meta page's writeback");
    });
    env.crash();
    disk_model::untrack(&path);
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let at = u64::from(leaf) * size as u64 + size as u64 - 100;
    let mut b = [0];
    file.read_exact_at(&mut b, at).expect("read");
    file.write_all_at(&[b[0] ^ 0x5A], at).expect("write");
    drop(file);
    let env = open_trusting(&path, true).expect("the last sync");
    assert_eq!(env::take_weak_head_check(), Some(Err(())), "the free-tree leaf fails its check");
    assert_eq!(env.stat().txn_id, synced_txn);
    assert!(newest_state(&env) == synced, "the last sync");
}

/// What open's check of a trusted weak head costs: the head over about `mib` MiB of pages written
/// since the last sync (1000-byte values at random keys, in NoSync commits of 4 MiB of rows, which
/// fill the leaves about half), opened after a process crash
/// with its pages in the page cache. The first open also writes back the pages the dead writer
/// left dirty (its fsync); the second, on clean pages, is the check alone, against a steady open
/// of the same file after a clean close made the head steady. Returns `(first, checked, steady,
/// pages checked)`.
fn weak_head_open_cost(mib: u64) -> (Duration, Duration, Duration, u64) {
    let (_dir, path) = temp("tn_lmdb_weak_cost");
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("m").expect("table");
    txn.put(&t, b"k", b"v").expect("put");
    txn.commit().expect("commit");
    drop(env);
    let env = open(&path, SyncMode::NoSync);
    durable_first_commit(&env, &mut State::new());
    let rows = mib * 512; // ~1 KiB a row, ~2 KiB of leaf
    let value = vec![0x5A; 1000];
    for chunk in (0..rows).collect::<Vec<_>>().chunks(4 * 1024) {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.open_table("m").expect("open").expect("m");
        for &i in chunk {
            txn.put(&t, &(i * 0x9E37_79B9 % rows).to_be_bytes(), &value).expect("put");
        }
        txn.commit().expect("a NoSync commit");
    }
    env.crash();
    let timed = |path: &Path| {
        let at = std::time::Instant::now();
        let env = open_trusting(path, true).expect("open");
        (at.elapsed(), env, env::take_weak_head_check())
    };
    let (first, env, _) = timed(&path);
    env.crash();
    let (checked, env, pages) = timed(&path);
    let pages = pages.expect("checked").expect("the check passes");
    drop(env);
    let (steady, env, none) = timed(&path);
    assert_eq!(none, None, "a steady head is not checked");
    drop(env);
    (first, checked, steady, pages)
}

/// The cost of open's check of a trusted weak head, at 64 and 512 MiB written since the last
/// sync: about the pages written since it (each page's CRC and structure), and only after an
/// unclean end. Guards that the check covers those pages, and a loose bound on its time.
#[test]
#[ignore = "measurement: run with --release --nocapture"]
fn a_weak_head_check_costs_the_pages_written_since_the_last_sync() {
    for mib in [64, 512] {
        let (first, checked, steady, pages) = weak_head_open_cost(mib);
        println!(
            "~{mib} MiB since the last sync: {pages} pages ({} MiB) checked; open after the crash \
             {first:?}, again on clean pages {checked:?}, steady open {steady:?}",
            (pages * 4096) >> 20
        );
        assert!(pages >= mib * 256 / 2, "{pages} pages checked for {mib} MiB");
        assert!(checked < Duration::from_secs(mib / 8 + 2), "{checked:?}");
    }
}

/// A writer opened past a weak head of its own boot (`trust_unsynced` off) gives its transactions
/// txns past the head's (`Chosen::next_txn`: past the run of the steady slot's, so past every txn
/// a writer stamped beside it), so no page it writes passes for one of the head's.
/// Here its first commit rewrites the rows the head's one commit wrote, with other values (the
/// same pages, by the same allocation), and stops after its stamp; a reopen that trusts the head
/// again finds those pages newer than the txn the head names in its check, and falls back to the
/// last sync, never the head's state over the new writer's bytes. With `trust_unsynced` off it
/// opens at the last sync too, unchecked.
#[test]
fn a_writer_past_a_distrusted_weak_head_never_reuses_its_txns() {
    let base = base(4096);
    let (path, env, synced, _) = weak_copy(&base, "past", 1);
    let head = synced.0 + 1;
    env.crash();
    let env = open_trusting(&path, false).expect("the last sync");
    assert_eq!(env.stat().txn_id, synced.0);
    let mut txn = env.begin_rw().expect("begin");
    let past = after_steady(synced.0);
    assert!(past > head);
    assert_eq!(txn.writer_mut().alloc.txn(), past, "past the weak head's txn");
    let table = txn.open_table("b").expect("open").expect("table b");
    for i in (0..200).step_by(9) {
        txn.put(&table, &key(i), &value(99, i, 120)).expect("put");
    }
    txn.writer_mut().crash_at = Some(CrashAt::AfterStamp);
    txn.commit().expect_err("the commit stops after its stamp");
    env.crash();
    let env = open_trusting(&path, true).expect("the last sync");
    assert_eq!(env::take_weak_head_check(), Some(Err(())), "the head fails its check");
    assert_eq!(env.stat().txn_id, synced.0);
    assert!(newest_state(&env) == synced.1, "the last sync");
    drop(env);
    let env = open_trusting(&path, false).expect("the last sync");
    assert!(newest_state(&env) == synced.1, "the last sync");
    drop(env);
    disk_model::untrack(&path);
}

/// The rows of table `t` in [`reuse_base`].
const REUSED_ROWS: u32 = 2000;

/// Rewrite every seventh row of [`reuse_base`]'s table `t`, with values tagged `tag`: a dead or
/// rolled-back writer and the writer after it run this over the same state, so they take the
/// same pages.
fn rewrite_rows(txn: &mut RwTxn, state: &mut State, tag: u32) {
    for i in (0..REUSED_ROWS).step_by(7) {
        put(txn, state, "t", &key(i), &value(tag, i, 100));
    }
}

/// A tracked file of `page_size`-byte pages holding table `t`'s [`REUSED_ROWS`] rows, every
/// commit durable: its directory, path, last txn and state. One commit, which frees nothing, so
/// a writer's reuse gate (the lowest slot's txn) leaves it the same pool whichever slot it opens
/// beside. With `filler`, that commit also fills a table of large values, the next drops it and
/// a last one rewrites a row, so later commits allocate inside the file's durable length and grow
/// nothing.
fn reuse_base(name: &str, page_size: u32, filler: bool) -> (TempDir, PathBuf, u64, State) {
    let (dir, path) = temp(name);
    let env =
        Env::open(&path, EnvOptions { page_size, ..options(SyncMode::Durable) }).expect("create");
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..REUSED_ROWS {
        put(&mut txn, &mut state, "t", &key(i), &value(1, i, 100));
    }
    if filler {
        let mut gone = State::new();
        for i in 0..400 {
            put(&mut txn, &mut gone, "f", &key(i), &value(3, i, 1000));
        }
    }
    let mut txn_id = txn.commit().expect("commit");
    if filler {
        let mut txn = env.begin_rw().expect("begin");
        let f = txn.open_table("f").expect("open").expect("f");
        txn.drop_table(f).expect("drop");
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("begin");
        put(&mut txn, &mut state, "t", &key(0), &value(2, 0, 100));
        txn_id = txn.commit().expect("commit");
    }
    drop(env);
    disk_model::track(&path);
    (dir, path, txn_id, state)
}

/// Whether the newest slot of `env`'s writer holds a weak record.
fn head_is_weak(env: &Env) -> bool {
    env.with_writer(|w| {
        let last = w.last.txn;
        w.slots().iter().flatten().any(|slot| slot.txn == last && slot.weak)
    })
}

/// Commit [`rewrite_rows`] (tags from `tag` on) over `state` until the newest slot is weak, at
/// most twice (the first commit after an open is durable): each commit's txn and state.
fn commit_until_weak(env: &Env, state: &State, tag: u32) -> Vec<(u64, State)> {
    let mut state = state.clone();
    let mut commits = Vec::new();
    for round in 0..2 {
        let mut txn = env.begin_rw().expect("begin");
        rewrite_rows(&mut txn, &mut state, tag + round);
        commits.push((txn.commit().expect("commit"), state.clone()));
        if head_is_weak(env) {
            return commits;
        }
    }
    panic!("two commits after an open left no weak head: {:?}", env.with_writer(|w| w.slots()));
}

/// The kernel's writeback of `env`'s weak meta page alone, then a process crash and a same-boot
/// volume loss (the pages the head names since the last sync never reach the disk), then a
/// reopen trusting the head: the txn and state it opens at, after `Env::check` passed.
fn lose_the_weak_pages_and_reopen(env: Env, path: &Path, page_size: u32) -> (u64, State) {
    env.with_writer(|w| {
        let slot = w.slots().iter().position(|s| s.is_some_and(|s| s.weak)).expect("a weak slot");
        w.file.msync_page(slot as u32, page_size as usize).expect("the meta page's writeback");
    });
    env.crash();
    disk_model::volume_loss(path);
    let env = open(path, SyncMode::NoSync);
    let _ = env::take_weak_head_check();
    check(&env);
    (env.stat().txn_id, newest_state(&env))
}

/// Assert `got` (a reopen's txn and state) is one of the `allowed` states, by txn and rows.
fn assert_one_of(got: &(u64, State), allowed: &[(u64, State)], case: &str) {
    let txns: Vec<u64> = allowed.iter().map(|(txn, _)| *txn).collect();
    let found = allowed.iter().find(|(txn, _)| *txn == got.0);
    assert!(found.is_some(), "{case}: opened at txn {}, none of {txns:?}", got.0);
    assert!(
        found.is_some_and(|(_, state)| *state == got.1),
        "{case}: the state of txn {} differs: \
         another writer's pages under its meta",
        got.0
    );
}

/// A commit that dies after its stamp leaves pages under its txn that no slot names, on the disk
/// (synced by its own data sync, or by the next writer open's sync). The next writer is given a
/// txn past it (`Chosen::next_txn`), so no commit of it that does not sync reuses it: here it
/// rewrites the same rows over the same state, by the same allocation, its weak meta reaches the
/// disk but its pages do not (a same-boot volume loss), and the reopen falls back to a state it
/// made durable or opened at, never to the dead commit's pages under its weak meta. For a
/// `SyncMode::Durable` commit dead after its stamp, its data sync and its size sync, a weak
/// `SyncMode::NoSync` commit dead after its stamp, and an automatic sync dead after its data
/// sync.
#[test]
fn a_dead_commits_txn_is_never_given_to_a_weak_commit() {
    let cases = [
        (SyncMode::Durable, None, CrashAt::AfterStamp),
        (SyncMode::Durable, None, CrashAt::AfterDataSync),
        (SyncMode::Durable, None, CrashAt::AfterSizeSync),
        (SyncMode::NoSync, None, CrashAt::AfterStamp),
        (SyncMode::NoSync, Some(1), CrashAt::AfterDataSync),
    ];
    for (sync, sync_bytes, at) in cases {
        let case = format!("{sync:?}, sync_bytes {sync_bytes:?}, {at:?}");
        let (_dir, path, _, base) = reuse_base("tn_lmdb_dead_txn", 4096, false);
        // A: a commit, then one that stops at `at`; the process dies.
        let env = Env::open(&path, EnvOptions { sync_bytes, ..options(sync) }).expect("open A");
        let mut state = base.clone();
        let mut txn = env.begin_rw().expect("begin");
        rewrite_rows(&mut txn, &mut state, 0xA0);
        txn.commit().expect("A's first commit");
        let mut txn = env.begin_rw().expect("begin");
        let dead = txn.writer_mut().alloc.txn();
        rewrite_rows(&mut txn, &mut state.clone(), 0xA1);
        txn.writer_mut().crash_at = Some(at);
        txn.commit().expect_err("A's commit stops");
        env.crash();
        // B, in the same boot: the dead commit's rows over the state it began from.
        let env = open(&path, SyncMode::NoSync);
        let mut allowed = vec![(env.stat().txn_id, state.clone())];
        let commits = commit_until_weak(&env, &state, 0xB0);
        let txns: Vec<u64> = commits.iter().map(|(txn, _)| *txn).collect();
        assert!(txns.iter().all(|&txn| txn > dead), "{case}: B's txns {txns:?}, the dead {dead}");
        allowed.extend(commits);
        let got = lose_the_weak_pages_and_reopen(env, &path, 4096);
        assert_one_of(&got, &allowed, &case);
        disk_model::untrack(&path);
    }
}

/// A death inside a weak record's copy leaves its slot invalid, so the next writer opens at the
/// last sync, and every commit the dead writer made since it left stamped pages under txns no
/// slot names (the next open's sync writes them to the disk). The next writer gives none of
/// those txns again (`Chosen::next_txn`): here it rewrites the rows the first of them rewrote
/// over the same state, its weak meta reaches the disk but its pages do not, and the reopen
/// falls back to a state it made durable or opened at, never to the dead writer's pages.
#[test]
fn a_torn_weak_meta_never_gives_its_txns_again() {
    let (_dir, path, txn_id, base) = reuse_base("tn_lmdb_torn_txn", 4096, false);
    let env = open(&path, SyncMode::NoSync);
    let mut states = BTreeMap::from([(txn_id, base)]);
    let mut state = states[&txn_id].clone();
    for round in 0..4 {
        let mut txn = env.begin_rw().expect("begin");
        rewrite_rows(&mut txn, &mut state, 0xA0 + round);
        states.insert(txn.commit().expect("A's commit"), state.clone());
    }
    assert!(head_is_weak(&env), "A's last commit is weak");
    let (slot, stamped) = env.with_writer(|w| {
        let slot = w.slots().iter().position(|s| s.is_some_and(|s| s.weak)).expect("a weak slot");
        (slot, w.last.txn)
    });
    env.crash();
    // A death inside the copy: part of the record overwritten, its CRC fails.
    let file = fs::OpenOptions::new().write(true).open(&path).expect("open");
    file.write_all_at(&[0xEE; 8], slot as u64 * 4096 + 64).expect("tear the record");
    drop(file);
    let env = open(&path, SyncMode::NoSync);
    let opened = env.stat().txn_id;
    assert!(opened < stamped, "the torn head opens at the last sync");
    let mut allowed = vec![(opened, states[&opened].clone())];
    let commits = commit_until_weak(&env, &states[&opened], 0xB0);
    let txns: Vec<u64> = commits.iter().map(|(txn, _)| *txn).collect();
    assert!(txns.iter().all(|&txn| txn > stamped), "B's txns {txns:?}, A's last {stamped}");
    allowed.extend(commits);
    let got = lose_the_weak_pages_and_reopen(env, &path, 4096);
    assert_one_of(&got, &allowed, "a torn weak meta");
    disk_model::untrack(&path);
}

/// A power loss after the kernel wrote back a weak commit's pages but not its meta rolls that
/// commit back while its pages, stamped with its txn, are on the disk. The next boot's writer
/// gives that txn to no commit that does not sync (`Chosen::next_txn`: it opens at the last
/// sync, which its first commit after any open keeps past every weak txn): here it rewrites the
/// rolled-back commit's rows over the same state, its weak meta reaches the disk but its pages
/// do not (a volume lost in that boot), and the reopen falls back to a state it made durable or
/// opened at. 16 KiB pages: each meta page fills an OS page of its own (macOS), so the early
/// writeback can leave both metas out and take every data page.
#[test]
fn weak_commits_a_reboot_rolled_back_never_give_their_txns_again() {
    const PAGE: u32 = 16384;
    let (_dir, path, txn_id, base) = reuse_base("tn_lmdb_rolled_txn", PAGE, true);
    // A, boot 1: a commit (durable, the first after the open) and a weak one; the kernel writes
    // back every page but the OS page holding the metas; then the power goes.
    let env = open(&path, SyncMode::NoSync);
    let mut states = BTreeMap::from([(txn_id, base)]);
    let mut state = states[&txn_id].clone();
    for round in 0..2 {
        let mut txn = env.begin_rw().expect("begin");
        rewrite_rows(&mut txn, &mut state, 0xA0 + round);
        states.insert(txn.commit().expect("A's commit"), state.clone());
    }
    assert!(head_is_weak(&env), "A's last commit is weak");
    let stamped = env.stat().txn_id;
    env.with_writer(|w| {
        let len = w.file.file_len();
        w.file.msync_range(2 * u64::from(PAGE)..len).expect("early writeback of the data pages");
    });
    env.crash();
    disk_model::power_loss(&path);
    // B, boot 2: opens at the last sync and rewrites the same rows.
    let env = open(&path, SyncMode::NoSync);
    let opened = env.stat().txn_id;
    assert!(opened < stamped, "the reboot rolls A's weak commit back");
    let mut allowed = vec![(opened, states[&opened].clone())];
    let commits = commit_until_weak(&env, &states[&opened], 0xB0);
    let txns: Vec<u64> = commits.iter().map(|(txn, _)| *txn).collect();
    assert!(txns.iter().all(|&txn| txn > stamped), "B's txns {txns:?}, A's last {stamped}");
    allowed.extend(commits);
    let got = lose_the_weak_pages_and_reopen(env, &path, PAGE);
    assert_one_of(&got, &allowed, "weak commits a reboot rolled back");
    disk_model::untrack(&path);
}

/// A writer that opens over a weak head starts past the steady slot beside it, not past the
/// head (`Chosen::next_txn`), and its first commit is durable. If it dies inside that commit's
/// steady copy, which goes over the weak head's slot, the slot is invalid and the next open
/// finds the steady slot alone: started past it, the next writer gives the dead commit's txn
/// to its own first, durable, commit (whose pages are synced before any meta names them), and
/// none of its commits that do not sync is given it. A start past the newest slot would put the
/// dead commit's txn among them.
#[test]
fn a_first_commit_torn_over_a_weak_slot_is_never_given_to_a_weak_commit() {
    let base = base(4096);
    let (path, env, _, state) = weak_copy(&base, "torn_first", 2);
    assert!(head_is_weak(&env), "a weak head beside a steady slot");
    env.crash();
    // L1: trusts the head; its first commit is durable and dies inside its meta copy.
    let env = open_trusting(&path, true).expect("the weak head");
    assert_eq!(env::take_weak_head_check().map(|c| c.is_ok()), Some(true), "the head is trusted");
    let mut txn = env.begin_rw().expect("begin");
    let dead = txn.writer_mut().alloc.txn();
    let slot = slot_to_write(&txn.writer_mut().slots());
    (base.crash)(&mut txn, &mut state.clone());
    txn.writer_mut().crash_at = Some(CrashAt::AfterMetaCopy);
    txn.commit().expect_err("the first commit after the open is durable, and stops in its copy");
    env.crash();
    let file = fs::OpenOptions::new().write(true).open(&path).expect("open");
    file.write_all_at(&[0xEE; 8], slot as u64 * 4096 + 64).expect("tear the record");
    drop(file);
    // L2: the steady slot alone.
    let steady = [0, 1].map(|s| slot_record(&path, 4096, s));
    assert!(steady.iter().flatten().all(|&(_, weak)| !weak), "one steady slot left: {steady:?}");
    let env = open(&path, SyncMode::NoSync);
    let mut state = newest_state(&env);
    let mut allowed = vec![(env.stat().txn_id, state.clone())];
    for round in 0..3 {
        let mut txn = env.begin_rw().expect("begin");
        if round < 2 {
            (base.unsynced)(&mut txn, &mut state, round);
        } else {
            (base.crash)(&mut txn, &mut state);
        }
        let txn = txn.commit().expect("L2's commit");
        assert!(txn >= dead, "L2's txn {txn} below the dead commit's {dead}");
        assert!(txn != dead || !head_is_weak(&env), "the dead commit's txn {dead} given weak");
        allowed.push((txn, state.clone()));
    }
    let got = lose_the_weak_pages_and_reopen(env, &path, 4096);
    assert_one_of(&got, &allowed, "a first commit torn over a weak slot");
    disk_model::untrack(&path);
}

/// The dup table of [`dup_base`].
const DUP: &str = "u";

/// [`DUP`]'s outer keys per 4 KiB of page size: enough for an outer tree of two levels.
const OUTER: u32 = 400;

/// An outer key of [`DUP`] with a nested tree of two levels and a value in a run, whose rows the
/// crash commit rewrites and adds to.
const HOT: u32 = 7;

/// An outer key like [`HOT`], whose nested tree the crash commit removes by a `delete_prefix`.
const GONE: u32 = 9;

/// An outer key of [`DUP`] with three rows, which the crash commit deletes one by one: a nested
/// tree at [`Bound::Unpacked`] and [`Bound::Small`], a packed set at [`Bound::Default`].
const EMPTIED: u32 = 20;

/// An outer key of [`DUP`] with one row (a packed set at the bounds that pack), which the crash
/// commit grows past the table's limit: promoted to a nested tree inside the crash commit.
const PROMOTED: u32 = 30;

/// The small keys of [`DUP`] the base writes past the new key, of 1 to 3 short rows each: packed
/// sets at both bounds that pack.
const SMALL: u32 = 24;

/// A packed row's bytes in a set beyond the set's own 2: its slot (4) and the subkey and value
/// [`dup_table`] gives the 1-row keys [`PROMOTED`] stands for (1 + 40).
const ROW: u16 = 4 + 1 + 40;

/// [`DUP`]'s outer keys at `page_size`.
fn outer_keys(page_size: u32) -> u32 {
    OUTER * page_size / 4096
}

/// The rows of [`HOT`] and [`GONE`] at `page_size`: enough for a nested tree of two levels.
fn big_rows(page_size: u32) -> u32 {
    300 * page_size / 4096
}

/// The outer key the crash commit creates, past every other but the small keys.
fn new_key(page_size: u32) -> u32 {
    outer_keys(page_size) + 5
}

/// Small key `i` at `page_size`: the base writes the even ones (`i < 2 * SMALL`), the crash commit
/// creates some odd ones between them.
fn small_key(page_size: u32, i: u32) -> u32 {
    outer_keys(page_size) + 10 + i
}

/// Store `value` under subkey `sub` of outer key `k` in [`DUP`], in the transaction and in the
/// model.
fn dup_put(txn: &mut RwTxn, state: &mut State, k: u32, sub: &[u8], value: &[u8]) {
    let table = txn.open_table(DUP).expect("open").expect("the dup table");
    let key = dup_key(k, sub);
    txn.put(&table, &key, value).expect("put");
    state.entry(DUP.to_string()).or_default().insert(key, value.to_vec());
}

/// The model's rows of outer key `k` in [`DUP`].
fn dup_keys_of(state: &State, k: u32) -> Vec<Vec<u8>> {
    let prefix = k.to_be_bytes();
    state[DUP].keys().filter(|key| key.starts_with(&prefix)).cloned().collect()
}

/// [`DUP`] at `bound` in the base's first commit: outer keys 0 to [`outer_keys`], 1 to 3 rows of
/// 1-byte subkeys and 40-byte values each (at [`Bound::Small`] the 1- and 2-row sets pack), but
/// [`HOT`] and [`GONE`]: [`big_rows`] rows of 2-byte subkeys and a value in a run each; then the
/// [`SMALL`] small keys, 1 to 3 rows of 8-byte values each.
fn dup_table(txn: &mut RwTxn, state: &mut State, page_size: u32, bound: Bound) {
    txn.create_dup_table(DUP, bound.options(OPTIONS.key_len)).expect("the dup table");
    for k in 0..outer_keys(page_size) {
        if k == HOT || k == GONE {
            for s in 0..big_rows(page_size) {
                dup_put(txn, state, k, &(s as u16).to_be_bytes(), &value(40 + k, s, 40));
            }
            dup_put(txn, state, k, b"run", &value(50 + k, 0, 3 * page_size as usize));
        } else {
            for s in 0..1 + k % 3 {
                dup_put(txn, state, k, &[s as u8], &value(41, 3 * k + s, 40));
            }
        }
    }
    for i in 0..SMALL {
        for s in 0..1 + i % 3 {
            dup_put(txn, state, small_key(page_size, 2 * i), &[s as u8], &value(42, i, 8));
        }
    }
}

/// [`unsynced_changes`], and a rewrite of one row of every 17th outer key of [`DUP`].
fn dup_unsynced(txn: &mut RwTxn, state: &mut State, round: u32) {
    unsynced_changes(txn, state, round);
    for k in (round..OUTER).step_by(17) {
        dup_put(txn, state, k, &[0], &value(70 + round, k, 40));
    }
}

/// The dup crash commit, then [`crash_changes`] (which grows the file): nested-only changes to
/// committed outer keys ([`HOT`]: every fifth row rewritten, rows added after the others and its
/// run replaced), so the flush rewrites their outer cells; one row of every 13th key rewritten
/// (packed rewrites where the bound packs); [`PROMOTED`] grown past the table's limit (where the
/// bound packs, `nested_trees` rises by one at the put that promotes it); the small keys' sets
/// rewritten, grown and shrunk, and small keys created between them; a new outer key; [`EMPTIED`]'s
/// rows deleted one by one, which removes its cell; and [`GONE`]'s nested tree (two levels and a
/// run) removed by a `delete_prefix`.
fn dup_crash(txn: &mut RwTxn, state: &mut State) {
    let page_size = txn.writer_mut().geometry.page_size() as u32;
    let table = txn.open_table(DUP).expect("open").expect("the dup table");
    let stat = |txn: &RwTxn| txn.table_stat(&table).expect("stat");
    let packed_max = stat(txn).dup.expect("a dup table").packed_max;
    let rows = big_rows(page_size);
    for s in (0..rows).step_by(5) {
        dup_put(txn, state, HOT, &(s as u16).to_be_bytes(), &value(60, s, 44));
    }
    for s in rows..rows + 40 {
        dup_put(txn, state, HOT, &(s as u16).to_be_bytes(), &value(61, s, 40));
    }
    dup_put(txn, state, HOT, b"run", &value(62, 0, 2 * page_size as usize + 100));
    for k in (0..OUTER).step_by(13) {
        dup_put(txn, state, k, &[0], &value(63, k, 40));
    }
    let trees = stat(txn).nested_trees;
    let grow = if packed_max == 0 { 4 } else { packed_max / ROW + 2 };
    for s in 1..=grow {
        dup_put(txn, state, PROMOTED, &[s as u8], &value(65, u32::from(s), 40));
    }
    let promoted = u32::from(packed_max > 0);
    assert_eq!(stat(txn).nested_trees, trees + promoted, "{packed_max}: {PROMOTED} promoted");
    for i in 0..SMALL {
        let k = small_key(page_size, 2 * i);
        match i % 3 {
            0 => dup_put(txn, state, k, &[0], &value(66, i, 12)),
            1 => dup_put(txn, state, k, &[2], &value(67, i, 8)),
            _ => delete(txn, state, DUP, &dup_key(k, &[1])),
        }
    }
    for i in (0..SMALL).step_by(3) {
        for s in 0..1 + i / 3 % 3 {
            dup_put(txn, state, small_key(page_size, 2 * i + 1), &[s as u8], &value(68, i, 8));
        }
    }
    for s in 0..3 {
        dup_put(txn, state, new_key(page_size), &[s], &value(64, u32::from(s), 40));
    }
    let emptied = dup_keys_of(state, EMPTIED);
    assert_eq!(emptied.len(), 3, "the rows of {EMPTIED}");
    for key in emptied {
        delete(txn, state, DUP, &key);
    }
    let gone = dup_keys_of(state, GONE);
    let removed = txn.delete_prefix(&table, &GONE.to_be_bytes()).expect("delete_prefix");
    assert_eq!(removed, gone.len() as u64, "the rows of {GONE}");
    let rows = state.get_mut(DUP).expect("the model has the dup table");
    gone.iter().for_each(|key| assert!(rows.remove(key).is_some()));
    crash_changes(txn, state);
}

/// What outer key `k`'s cell in [`DUP`] holds in the newest state of `env`.
#[derive(Debug)]
enum Cell {
    /// No cell.
    Missing,
    /// A packed set of this many rows.
    Packed(usize),
    /// A nested tree.
    Nested(TreeRoot),
}

/// [`DUP`]'s record in the newest state of `env`.
fn dup_record(env: &Env) -> DupRecord {
    let snapshot = env.inner().current();
    let record = catalog::get(&*snapshot, &snapshot.meta.catalog, DUP).expect("get");
    record.expect("the dup table").dup.expect("a dup record")
}

/// The bound [`DUP`] was created at, by its record's limit.
fn bound_of(record: &DupRecord) -> Bound {
    match record.packed_max {
        0 => Bound::Unpacked,
        PACKED_SMALL => Bound::Small,
        _ => Bound::Default,
    }
}

/// Outer key `k`'s cell in [`DUP`] in the newest state of `env`.
fn cell(env: &Env, k: u32) -> Cell {
    let snapshot = env.inner().current();
    let packed_max = usize::from(dup_record(env).packed_max);
    let key = k.to_be_bytes();
    let leaf = descend(&*snapshot, &tree(env, DUP), &key, |_, _, _, _| Ok(()))
        .expect("descend")
        .expect("a leaf");
    match page::leaf_find(snapshot.page(leaf.pgno).expect("the outer leaf"), &key).expect("find") {
        Find::Found { value: Val::Nested(root), .. } => {
            Cell::Nested(TreeRoot::decode(root).expect("a nested root"))
        }
        Find::Found { value: Val::Inline(set), .. } if packed_max > 0 => {
            Cell::Packed(PackedView::new(set, packed_max).expect("a packed set").rows())
        }
        Find::Found { value, .. } => panic!("outer key {k}: {value:?}"),
        Find::NotFound { .. } => Cell::Missing,
    }
}

/// Outer key `k`'s cell holds a packed set of `rows` rows where `bound` packs it (`packs`), else a
/// nested tree of `rows` rows.
fn assert_form(env: &Env, k: u32, packs: bool, rows: usize) {
    match cell(env, k) {
        Cell::Packed(n) if packs => assert_eq!(n, rows, "{k}: packed rows"),
        Cell::Nested(root) if !packs => assert_eq!(root.entries, rows as u64, "{k}: {root:?}"),
        other => {
            panic!("{k}: {other:?}, not {} of {rows} rows", ["nested", "packed"][packs as usize])
        }
    }
}

/// The dup shape the matrix relies on, in the state the crash commit made, at the bound [`DUP`]'s
/// record names: [`DUP`]'s outer tree and [`HOT`]'s nested tree of two levels or more (the nested
/// flush), [`HOT`]'s run, [`PROMOTED`] a nested tree, the small keys packed sets of the rows the
/// crash commit left (nested trees at [`Bound::Unpacked`], which has no packed row), no outer
/// cell left for [`EMPTIED`] or [`GONE`], and the new key's.
fn dup_layout(env: &Env) {
    let record = dup_record(env);
    let bound = bound_of(&record);
    let packs = bound != Bound::Unpacked;
    assert_eq!(record.packed_rows > 0, packs, "{bound:?}: {record:?}");
    let outer = tree(env, DUP);
    assert!(outer.height >= 2, "{outer:?}");
    let page_size = env.with_writer(|w| w.geometry.page_size() as u32);
    let Cell::Nested(hot) = cell(env, HOT) else { panic!("{HOT}: {:?}", cell(env, HOT)) };
    assert!(hot.height >= 2 && hot.overflow_pages > 0, "{hot:?}");
    let grow = if packs { usize::from(record.packed_max / ROW) + 2 } else { 4 };
    assert_form(env, PROMOTED, false, 1 + grow);
    for i in 0..SMALL {
        let rows = [1, 3, 2][i as usize % 3];
        assert_form(env, small_key(page_size, 2 * i), packs, rows);
    }
    for i in (0..SMALL).step_by(3) {
        assert_form(env, small_key(page_size, 2 * i + 1), packs, 1 + (i / 3 % 3) as usize);
    }
    assert!(matches!(cell(env, EMPTIED), Cell::Missing), "the emptied key's cell is removed");
    assert!(matches!(cell(env, GONE), Cell::Missing), "the delete_prefix removes the cell");
    assert_form(env, new_key(page_size), bound == Bound::Default, 3);
}

/// [`base`] with dup table [`DUP`] at `bound` ([`dup_table`]) beside the plain tables, its
/// unsynced commits ([`dup_unsynced`]) and crash commit ([`dup_crash`]) writing it too, checked
/// (on a copy) to hold the shape the crash commit changes: an outer tree of two levels or more,
/// [`HOT`]'s and [`GONE`]'s nested trees of two levels or more with a run, [`EMPTIED`]'s cell,
/// and where the bound packs, [`PROMOTED`] and the small keys as packed sets. On that copy the
/// crash commit then runs once to its end: it must change the record's `packed_rows` where the
/// bound packs (none at [`Bound::Unpacked`]) and leave [`dup_layout`]'s shape.
fn dup_base(page_size: u32, bound: Bound) -> Base {
    let base = Base {
        unsynced: dup_unsynced,
        crash: dup_crash,
        layout: dup_layout,
        ..base_with(page_size, |txn, state, page_size| dup_table(txn, state, page_size, bound))
    };
    let path = base.dir.path().join("shape");
    fs::copy(&base.path, &path).expect("copy");
    let env = open(&path, SyncMode::Durable);
    assert!(newest_state(&env) == base.state, "the base holds its model");
    let before = dup_record(&env);
    assert_eq!(bound_of(&before), bound, "{before:?}");
    let packs = bound != Bound::Unpacked;
    assert_eq!(before.packed_rows > 0, packs, "{bound:?}: {before:?}");
    assert!(tree(&env, DUP).height >= 2, "{:?}", tree(&env, DUP));
    for k in [HOT, GONE] {
        let Cell::Nested(root) = cell(&env, k) else { panic!("{k}: {:?}", cell(&env, k)) };
        assert!(root.height >= 2 && root.overflow_pages > 0, "{k}: {root:?}");
    }
    assert_form(&env, EMPTIED, bound == Bound::Default, 3);
    assert_form(&env, PROMOTED, packs, 1);
    for i in 0..SMALL {
        assert_form(&env, small_key(page_size, 2 * i), packs, 1 + i as usize % 3);
    }
    let mut state = base.state.clone();
    let mut txn = env.begin_rw().expect("begin");
    dup_crash(&mut txn, &mut state);
    txn.commit().expect("the crash commit, uncrashed");
    let after = dup_record(&env);
    assert_eq!(after.packed_rows != before.packed_rows, packs, "{before:?} -> {after:?}");
    dup_layout(&env);
    drop(env);
    fs::remove_file(&path).expect("remove");
    base
}

/// The matrix over a dup table beside plain ones, at the two bounds that pack: a crash commit of
/// nested-only changes (outer cells rewritten by the flush), packed rewrites, a promotion, packed
/// sets grown, shrunk and created, a new outer key, a key emptied and a `delete_prefix` reopens
/// at every crash point to exactly `L` or `M`, every row read back by a get and by a walk of every
/// tree (with the totals and the check's figures), and passes `Env::check`. The `_heavy` twin
/// adds [`Bound::Unpacked`].
#[test]
fn every_crash_point_recovers_a_dup_table() {
    for bound in [Bound::Small, Bound::Default] {
        matrix(&dup_base(4096, bound), 3);
    }
}

/// The dup matrix at every bound and two page sizes, with many random power losses (20 seeds a
/// bound: three bounds at 40 took about 12 minutes, past the heavy suite's budget).
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_recovers_a_dup_table_heavy() {
    for page_size in [4096, 16384] {
        for bound in Bound::ALL {
            matrix(&dup_base(page_size, bound), 20);
        }
    }
}

/// The weak matrix over a dup table beside plain ones: weak heads over one to three NoSync
/// commits since the last sync that change nested trees, runs and packed sets. After a process
/// crash open's check of the head's pages since the last sync passes and the reopen keeps every
/// commit; after a volume lost in the same boot the check fails and the reopen is the last sync.
#[test]
fn a_weak_head_over_a_dup_table_is_checked_since_the_last_sync() {
    weak_matrix(&dup_base(4096, Bound::Default), 1);
}

/// The dup weak matrix at every bound and two page sizes, with more random losses.
#[test]
#[ignore = "heavy: run with --release"]
fn a_weak_head_over_a_dup_table_is_checked_since_the_last_sync_heavy() {
    for page_size in [4096, 16384] {
        for bound in Bound::ALL {
            weak_matrix(&dup_base(page_size, bound), 8);
        }
    }
}

/// [`a_commit_that_grew_the_file_survives_a_power_loss`] with the dup crash commit, at every
/// bound.
#[test]
fn a_commit_that_grew_the_file_survives_a_power_loss_with_a_dup_table() {
    for bound in Bound::ALL {
        grown_commit_survives_a_power_loss(&dup_base(4096, bound));
    }
}

/// [`the_older_state_stays_recoverable_until_its_meta_is_overwritten`] with the dup crash commit,
/// at every bound: the pages it reused hold none of the older state's nested trees or packed sets
/// either.
#[test]
fn the_older_state_with_a_dup_table_stays_recoverable_until_its_meta_is_overwritten() {
    for bound in Bound::ALL {
        older_state_stays_recoverable(&dup_base(4096, bound));
    }
}
