//! The crash matrix: a commit (or, in `SyncMode::NoSync`, a sync) stopped at each point of the
//! commit protocol, then a process crash, a strict power loss or a random-writeback power loss,
//! must reopen to exactly the state the protocol allows — the last durable state `L` until the
//! new meta is written, the new state `M` once its msync completed, and in between `M` exactly
//! when the slot holds `M`'s intact record — pass `Env::check`, and reopen to the same state again
//! after a crash right after that reopen.

use std::{fs, os::unix::fs::FileExt as _};

use super::*;
use crate::{disk_model, env::CrashAt, meta::slot_to_write};

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
/// it, and its page size.
struct Base {
    dir: TempDir,
    path: PathBuf,
    state: State,
    prev: State,
    txn: u64,
    page_size: u32,
}

/// The pages the file holds past the newest state's page count (growth slack).
fn slack(env: &Env) -> u64 {
    env.with_writer(|w| {
        w.file.file_len() / w.geometry.page_size() as u64 - u64::from(w.last.page_count)
    })
}

/// Build a [`Base`]: tables `a`, `b`, `g` and `h`; table `e` (low page numbers) dropped by the
/// second-to-last commit, so its pages are free records a reopened writer may reuse; and a filler
/// table `f`, sized to leave the file only a few pages of slack, dropped by the last commit, so
/// its pages are free but not yet reusable: a modest commit must grow the file.
fn base(page_size: u32) -> Base {
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
    Base { dir, path, state, prev, txn, page_size }
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

/// The crash points a commit in `sync` mode stops at, with `ranges` data ranges to msync.
fn points(ranges: usize, every_range: bool) -> Vec<CrashAt> {
    let mut points = vec![CrashAt::BeforeSave, CrashAt::AfterSave, CrashAt::AfterStamp];
    if every_range {
        points.extend((1..=ranges).map(CrashAt::DataMsync));
    } else {
        points.extend([CrashAt::DataMsync(1), CrashAt::DataMsync(ranges)]);
    }
    points.extend([CrashAt::AfterSizeSync, CrashAt::AfterMetaCopy, CrashAt::AfterMetaMsync]);
    points
}

/// True if `at` stops a commit in `sync` mode (else it stops the following sync).
fn stops_the_commit(sync: SyncMode, at: CrashAt) -> bool {
    sync == SyncMode::Durable
        || matches!(at, CrashAt::BeforeSave | CrashAt::AfterSave | CrashAt::AfterStamp)
}

/// Copy the base to a tracked case file, open it in `sync` mode, make the commits (unsynced
/// ones first in `SyncMode::NoSync`) and stop at `at` (`None`: run to the end, with a sync).
/// Returns the case's path, the state `M` its last commit made, the slot `M`'s meta goes to,
/// and the data ranges the durable steps msynced. The environment is crashed.
fn run_case(base: &Base, sync: SyncMode, at: Option<CrashAt>) -> Case {
    let path = base.dir.path().join("case");
    fs::copy(&base.path, &path).expect("copy");
    disk_model::track(&path);
    let env = open(&path, sync);
    let mut latest = base.state.clone();
    if sync == SyncMode::NoSync {
        for round in 0..2 {
            let mut txn = env.begin_rw().expect("begin");
            unsynced_changes(&mut txn, &mut latest, round);
            txn.commit().expect("an unsynced commit");
        }
    }
    let slot = env.with_writer(|w| slot_to_write(&w.slots()));
    let mut txn = env.begin_rw().expect("begin");
    crash_changes(&mut txn, &mut latest);
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
    let (ranges, reused) = env.with_writer(|w| (w.data_msyncs, w.alloc.load_log.len()));
    env.crash();
    Case { path, latest, slot, ranges, reused }
}

/// What [`run_case`] left.
struct Case {
    /// The case's file.
    path: PathBuf,
    /// The state the last commit made (`M`).
    latest: State,
    /// The meta slot `M`'s record goes to.
    slot: usize,
    /// The data ranges the last durable steps msynced.
    ranges: usize,
    /// The pages the writer took from the free list.
    reused: usize,
}

/// One cell of the matrix: stop at `at`, go down by `loss`, and reopen (twice, with a crash and
/// a strict power loss right after the first reopen) to exactly the state allowed.
fn crash_case(base: &Base, sync: SyncMode, at: CrashAt, loss: Loss) {
    let case = format!("{sync:?} {at:?} {loss:?}");
    let Case { path, latest, slot, .. } = run_case(base, sync, Some(at));
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
        env.crash();
        disk_model::power_loss(&path);
    }
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
}

/// The number of data ranges the durable steps of the crash commit (or of the sync after it)
/// msync in `sync` mode, checked to exercise what the matrix needs: at least two ranges, pages
/// reused from the free list, and a growth of the file.
fn dry_run(base: &Base, sync: SyncMode) -> usize {
    let Case { path, latest, ranges, reused, .. } = run_case(base, sync, None);
    assert!(ranges >= 2, "{sync:?}: {ranges} data ranges");
    assert!(reused > 0, "{sync:?}: pages were reused");
    let base_len = fs::metadata(&base.path).expect("metadata").len();
    assert!(fs::metadata(&path).expect("metadata").len() > base_len, "{sync:?}: the file grew");
    let env = open(&path, sync);
    assert_eq!(newest_state(&env), latest);
    drop(env);
    disk_model::untrack(&path);
    fs::remove_file(&path).expect("remove");
    ranges
}

/// The matrix: every crash point x {process crash, strict power loss, random power loss with
/// three seeds} x {`Durable`, `NoSync`} reopens to exactly the allowed state, which checks and
/// survives a crash right after the reopen.
#[test]
fn every_crash_point_recovers_the_state_the_protocol_allows() {
    let base = base(4096);
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let ranges = dry_run(&base, sync);
        for (n, at) in points(ranges, false).into_iter().enumerate() {
            for loss in [Loss::Process, Loss::Strict] {
                crash_case(&base, sync, at, loss);
            }
            for seed in 0..3 {
                crash_case(&base, sync, at, Loss::Random(seed * 64 + n as u64));
            }
        }
    }
}

/// The matrix at two page sizes, after every data range, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_recovers_the_state_the_protocol_allows_heavy() {
    for page_size in [4096, 16384] {
        let base = base(page_size);
        for sync in [SyncMode::Durable, SyncMode::NoSync] {
            let ranges = dry_run(&base, sync);
            for (n, at) in points(ranges, true).into_iter().enumerate() {
                for loss in [Loss::Process, Loss::Strict] {
                    crash_case(&base, sync, at, loss);
                }
                for seed in 0..40 {
                    crash_case(&base, sync, at, Loss::Random(seed * 64 + n as u64));
                }
            }
        }
    }
}

/// A commit that grew the file survives a power loss once it returned: the size sync made the
/// new length durable before the meta naming it.
#[test]
fn a_commit_that_grew_the_file_survives_a_power_loss() {
    let base = base(4096);
    let Case { path, latest, .. } = run_case(&base, SyncMode::Durable, None);
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    let env = open(&path, SyncMode::Durable);
    assert_eq!(env.stat().txn_id, base.txn + 1);
    assert_eq!(newest_state(&env), latest);
}

/// The older durable state stays recoverable until a commit overwrites its meta: a commit
/// stopped once its data reached the disk has reused no page that state reaches (only pages
/// freed by txns no later than both slots), so if the newest meta is then found damaged, open
/// falls back to the older state intact.
#[test]
fn the_older_state_stays_recoverable_until_its_meta_is_overwritten() {
    let base = base(4096);
    let Case { path, slot, reused, .. } =
        run_case(&base, SyncMode::Durable, Some(CrashAt::AfterSizeSync));
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
