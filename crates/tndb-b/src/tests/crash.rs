//! The crash matrix: a commit (or, in `SyncMode::NoSync`, a sync) stopped at each point of the
//! commit protocol, then a process crash, a strict power loss or a random-writeback power loss,
//! must reopen to exactly the state the protocol allows — the last durable state `L` until the
//! new meta is written, the new state `M` once its msync completed, and in between `M` exactly
//! when the slot holds `M`'s intact record — pass `Env::check`, and reopen to the same state again
//! after a crash right after that reopen.

use std::{fs, os::unix::fs::FileExt as _};

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
    meta::slot_to_write,
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
    base_with(page_size, |_, _, _| {})
}

/// [`base`] with the tables `extra` writes (given the page size) in its first commit, after `h`.
fn base_with(page_size: u32, extra: impl FnOnce(&mut RwTxn, &mut State, u32)) -> Base {
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

/// The matrix with every durable commit's step 3 sliced at three pages
/// (`env::with_writeback_slice`), so each slice's writeback is started before the next is
/// stamped: the same states recover, with one random power loss per crash point.
#[test]
fn every_crash_point_recovers_the_state_the_protocol_allows_with_a_sliced_stamp() {
    env::with_writeback_slice(3 * 4096, || matrix(&base(4096), 1));
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
    grown_commit_survives_a_power_loss(&base(4096));
}

/// [`a_commit_that_grew_the_file_survives_a_power_loss`] on `base`.
fn grown_commit_survives_a_power_loss(base: &Base) {
    for by_msync in [false, true] {
        let Case { path, latest, .. } = run_case(base, SyncMode::Durable, by_msync, None);
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
    older_state_stays_recoverable(&base(4096));
}

/// [`the_older_state_stays_recoverable_until_its_meta_is_overwritten`] on `base`.
fn older_state_stays_recoverable(base: &Base) {
    let Case { path, slot, reused, .. } =
        run_case(base, SyncMode::Durable, false, Some(CrashAt::AfterSizeSync));
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
    let leaf = descend(&*snapshot, &tree(env, DUP), &key, |_, _, _, _| Ok(()), None)
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
