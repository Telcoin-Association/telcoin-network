//! The crash matrix: a commit (or, in `SyncMode::NoSync`, a sync) stopped at each point of the
//! commit protocol, then a process crash, a strict power loss or a random-writeback power loss,
//! must reopen to exactly the state the protocol allows — the last durable state `L` until the
//! new meta is written, the new state `M` once its msync completed, and in between `M` exactly
//! when the slot holds `M`'s intact record — pass `Env::check`, and reopen to the same state again
//! after a crash right after that reopen.

use std::{fs, os::unix::fs::FileExt as _};

use super::*;
use crate::{alloc::RunStats, disk_model, env::CrashAt, meta::slot_to_write};

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

/// A cleanly closed file to crash commits on: its last commit's state and txn, the state before
/// it, its page size, and the workload a case runs on it: the changes of the two commits
/// `SyncMode::NoSync` cases leave unsynced (by round), the crashing commit's, whether that commit
/// grows the file, and a check of the layout the matrix relies on, run on the state the crashing
/// commit made (`dry_run`).
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
    let (dir, path) = temp("tndb_b_crash");
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

/// True if `at` stops a commit in `sync` mode (else it stops the following sync).
fn stops_the_commit(sync: SyncMode, at: CrashAt) -> bool {
    sync == SyncMode::Durable
        || matches!(at, CrashAt::BeforeSave | CrashAt::AfterSave | CrashAt::AfterStamp)
}

/// Copy the base to a tracked case file, open it in `sync` mode, make the commits (unsynced
/// ones first in `SyncMode::NoSync`) and stop at `at` (`None`: run to the end, with a sync).
/// With `by_msync`, step 4 syncs the data by one `msync` (`Writer::data_by_msync`), else by the
/// sync `data_sync` picks. Returns the case's path, the state `M` its last commit made, the slot
/// `M`'s meta goes to, how the durable steps synced the data, and what the writer reused and
/// how it placed its runs. The environment is crashed.
pub(super) fn run_case(base: &Base, sync: SyncMode, by_msync: bool, at: Option<CrashAt>) -> Case {
    let path = base.dir.path().join("case");
    fs::copy(&base.path, &path).expect("copy");
    disk_model::track(&path);
    let env = open(&path, sync);
    env.with_writer(|w| w.data_by_msync = by_msync);
    let mut latest = base.state.clone();
    if sync == SyncMode::NoSync {
        for round in 0..2 {
            let mut txn = env.begin_rw().expect("begin");
            (base.unsynced)(&mut txn, &mut latest, round);
            txn.commit().expect("an unsynced commit");
        }
    }
    let slot = env.with_writer(|w| slot_to_write(&w.slots()));
    let mut txn = env.begin_rw().expect("begin");
    (base.crash)(&mut txn, &mut latest);
    let stopped = match at {
        Some(at) if stops_the_commit(sync, at) => {
            txn.writer_mut().crash_at = Some(at);
            Some(txn.commit().expect_err("the commit stops at the crash point"))
        }
        _ => {
            txn.commit().expect("commit");
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
    env.crash();
    Case { path, latest, slot, by_file, reused, runs }
}

/// What [`run_case`] left.
pub(super) struct Case {
    /// The case's file.
    pub(super) path: PathBuf,
    /// The state the last commit made (`M`).
    pub(super) latest: State,
    /// The meta slot `M`'s record goes to.
    pub(super) slot: usize,
    /// The last durable steps synced the data by one `fdatasync` of the file.
    pub(super) by_file: bool,
    /// The pages the writer took from the free list.
    pub(super) reused: usize,
    /// How the writer placed the overflow runs of the case's commits.
    pub(super) runs: RunStats,
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
fn crash_case(base: &Base, sync: SyncMode, by_msync: bool, at: CrashAt, loss: Loss) {
    let case = format!("{sync:?} by_msync={by_msync} {at:?} {loss:?}");
    let Case { path, latest, slot, .. } = run_case(base, sync, by_msync, Some(at));
    match loss {
        Loss::Process => {}
        Loss::Strict => disk_model::power_loss(&path),
        Loss::Random(seed) => disk_model::power_loss_random(&path, seed),
    }
    let m_txn = base.txn + if sync == SyncMode::NoSync { 3 } else { 1 };
    // `M` survives exactly when its slot holds its intact record, which must be after step 6
    // completed, may be during it, and must not be before it (a `NoSync` commit never writes it).
    let reaches_m = slot_holds(&path, base.page_size, slot, m_txn);
    match (at, loss) {
        (CrashAt::AfterMetaMsync, _) => assert!(reaches_m, "{case}: the msynced meta survives"),
        (CrashAt::AfterMetaCopy, Loss::Process) => {
            assert!(reaches_m, "{case}: the page cache keeps the meta")
        }
        (CrashAt::AfterMetaCopy, Loss::Strict) => {
            assert!(!reaches_m, "{case}: an unflushed meta is lost")
        }
        (CrashAt::AfterMetaCopy, Loss::Random(_)) => {}
        _ => assert!(!reaches_m, "{case}: no meta is written before step 6"),
    }
    let (want_txn, want) = if reaches_m { (m_txn, &latest) } else { (base.txn, &base.state) };
    for reopen in 0..2 {
        let env = open(&path, sync);
        assert_eq!(env.stat().txn_id, want_txn, "{case}: reopen {reopen}");
        assert!(newest_state(&env) == *want, "{case}: reopen {reopen}: the state differs");
        assert_gets(&env, want, &format!("{case}: reopen {reopen}"));
        env.crash();
        disk_model::power_loss(&path);
    }
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
}

/// Run the crash commit (or the sync after it) to the end, checked to exercise what the matrix
/// needs: the data synced by the `fdatasync` `data_sync` picks for pages that form several
/// ranges (or, `by_msync`, by one `msync`), pages reused from the free list, a growth of the
/// file exactly when the base says so (a growth's size sync is an `fsync`, which writes every
/// page: only a commit that does not grow shows what one `msync` alone covers), and the base's
/// `layout`.
fn dry_run(base: &Base, sync: SyncMode, by_msync: bool) {
    let Case { path, latest, by_file, reused, .. } = run_case(base, sync, by_msync, None);
    assert_eq!(by_file, !by_msync, "{sync:?}: by_msync={by_msync}: synced by fdatasync");
    assert!(reused > 0, "{sync:?}: pages were reused");
    let base_len = fs::metadata(&base.path).expect("metadata").len();
    let grew = fs::metadata(&path).expect("metadata").len() > base_len;
    assert_eq!(grew, base.grows, "{sync:?}: the file grew");
    let env = open(&path, sync);
    assert_eq!(newest_state(&env), latest);
    (base.layout)(&env);
    drop(env);
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
}

/// Every crash point of `base`'s crash commit x {process crash, strict power loss, `seeds`
/// random power losses} x {`Durable`, `NoSync`} x {the `fdatasync` step 4 picks, one `msync`}.
pub(super) fn matrix(base: &Base, seeds: u64) {
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        for by_msync in [false, true] {
            dry_run(base, sync, by_msync);
            for (n, at) in POINTS.into_iter().enumerate() {
                for loss in [Loss::Process, Loss::Strict] {
                    crash_case(base, sync, by_msync, at, loss);
                }
                for seed in 0..seeds {
                    crash_case(base, sync, by_msync, at, Loss::Random(seed * 64 + n as u64));
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

/// A commit that grew the file survives a power loss once it returned, by either data sync: the
/// `fdatasync` makes the new length durable with the data, and after one `msync` the size sync
/// does, before the meta naming it.
#[test]
fn a_commit_that_grew_the_file_survives_a_power_loss() {
    let base = base(4096);
    for by_msync in [false, true] {
        let Case { path, latest, .. } = run_case(&base, SyncMode::Durable, by_msync, None);
        disk_model::power_loss(&path);
        disk_model::untrack(&path);
        let env = open(&path, SyncMode::Durable);
        assert_eq!(env.stat().txn_id, base.txn + 1, "by_msync={by_msync}");
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
    let base = base(4096);
    let Case { path, slot, reused, .. } =
        run_case(&base, SyncMode::Durable, false, Some(CrashAt::AfterSizeSync));
    assert!(reused > 0, "the commit reused pages");
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    // Damage the newest meta (txn `base.txn`, in the slot the commit was not writing).
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let at = (1 - slot) as u64 * 4096 + 20;
    let mut b = [0];
    file.read_exact_at(&mut b, at).expect("read");
    file.write_all_at(&[b[0] ^ 0x10], at).expect("write");
    let env = open(&path, SyncMode::Durable);
    assert_eq!(env.stat().txn_id, base.txn - 1);
    assert_eq!(newest_state(&env), base.prev);
}
