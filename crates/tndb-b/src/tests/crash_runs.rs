//! The crash matrix ([`matrix`]) on a workload of overflow runs: a crashing commit that puts
//! values needing runs of 1 to 10 pages, replaces values in each direction (inline to a run, a run
//! to inline, a run to a larger and to a smaller run), deletes values held inline and in runs, and
//! drops a table holding runs. Every crash point, way down, sync mode and data-sync path reopens
//! to exactly the state the protocol allows, whose every value, a run's included, reads back byte
//! for byte through a get and both cursor scans. The commit ends with a run at the top of its
//! pages and does not grow the file, so the one `msync` of step 4 alone must cover that run's
//! interior pages (a growth's size sync writes every page). A variant that grows the file through
//! a run survives a power loss.

use std::fs;

use super::{
    crash::{assert_gets, matrix, run_case, slack, Base, Case},
    damage::tree,
    *,
};
use crate::{disk_model, overflow::run_pages, page::Val, tree::read};

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A value of `pages` pages of overflow run at `page_size` (0: inline, 100 bytes): its length
/// varies with `i` within those pages.
fn run_value(tag: u32, i: u32, pages: usize, page_size: u32) -> Vec<u8> {
    let len = match pages {
        0 => 100,
        n => n * page_size as usize - 28 - (i as usize * 37 % 400),
    };
    value(tag, i, len)
}

/// The pages of table `r`'s value of key `i` in the base: inline or a run of 1, 2, 4 or 9 pages.
fn base_pages(i: u32) -> usize {
    [0, 1, 0, 2, 4, 0, 9, 1][i as usize % 8]
}

/// Table `r`'s keys.
const R_KEYS: u32 = 24;

/// The growth slack the base leaves: room for every page the crash commits take past the reused
/// ones, so they never grow the file.
const SLACK: std::ops::RangeInclusive<u64> = 150..=190;

/// Build a [`Base`] for the run workload: table `a` (inline values, for copies of published
/// pages); table `r` ([`base_pages`]); table `x` (runs and inline values), which the crash commit
/// drops; table `h`, nine-page runs between `e`'s pages and the end of the file; table `e` (low
/// page numbers, runs among them) dropped by the second-to-last commit, so its pages are free
/// records a reopened writer may reuse for tree pages and runs; and a filler table `f`, sized to
/// leave [`SLACK`] pages of slack, dropped by the last commit.
fn runs_base(page_size: u32) -> Base {
    let (dir, path) = temp("tndb_b_crash_runs");
    let env = Env::open(&path, EnvOptions { page_size, ..EnvOptions::default() }).expect("open");
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..400 {
        put(&mut txn, &mut state, "a", &key(i), &value(1, i, 200));
    }
    for i in 0..R_KEYS {
        put(&mut txn, &mut state, "r", &key(i), &run_value(2, i, base_pages(i), page_size));
    }
    for i in 0..24 {
        let pages = [0, 3, 1][i as usize % 3];
        put(&mut txn, &mut state, "e", &key(i), &run_value(5, i, pages, page_size));
    }
    for i in 0..12 {
        put(&mut txn, &mut state, "x", &key(i), &run_value(7, i, i as usize % 5, page_size));
    }
    for i in 0..8 {
        put(&mut txn, &mut state, "h", &key(i), &run_value(8, i, 9, page_size));
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    for i in (0..400).step_by(2) {
        put(&mut txn, &mut state, "a", &key(i), &value(11, i, 220));
    }
    txn.commit().expect("commit");
    // Fill to the slack wanted: from above, else past the end first (the file then doubles).
    let per_page = u64::from(page_size) / 1000;
    let mut next = 0;
    while !SLACK.contains(&slack(&env)) {
        let left = slack(&env);
        let pages =
            if left > *SLACK.end() { left - (SLACK.start() + SLACK.end()) / 2 } else { left + 1 };
        let mut txn = env.begin_rw().expect("begin");
        for _ in 0..pages * per_page {
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
    assert!(SLACK.contains(&left), "{left} pages of slack");
    check(&env);
    drop(env);
    Base {
        dir,
        path,
        state,
        prev,
        txn,
        page_size,
        unsynced: runs_unsynced,
        crash: runs_crash,
        grows: false,
        layout: last_run_ends_the_file,
    }
}

/// The layout the matrix's `msync` cases rely on: the crash commit's highest page is the last page
/// of key 200's ten-page run, so the run ends at the state's page count. A step 4 `msync` range
/// that stopped short could miss only a page past every other dirty page, so the matrix guards a
/// run's pages on that path only while a run holds the commit's highest page.
fn last_run_ends_the_file(env: &Env) {
    let snapshot = env.inner().current();
    let found = read::find(&*snapshot, &tree(env, "r"), &key(200)).expect("find");
    let Some(Val::Overflow(ovf)) = found else { panic!("key 200's value is in a run: {found:?}") };
    let n = run_pages(ovf.len as usize, snapshot.geometry.page_size());
    assert_eq!(n, 10, "{ovf:?}: a ten-page run");
    let end = u64::from(ovf.pgno) + n as u64;
    assert_eq!(end, u64::from(snapshot.meta.page_count), "{ovf:?}: the run ends the file");
}

/// The page size of the case's transaction.
fn page_size(txn: &mut RwTxn) -> u32 {
    txn.writer_mut().geometry.page_size() as u32
}

/// Two small commits that `SyncMode::NoSync` cases leave unsynced before the crash commit: values
/// of `r` replaced by inline values and runs of 1 to 4 pages.
fn runs_unsynced(txn: &mut RwTxn, state: &mut State, round: u32) {
    let page_size = page_size(txn);
    for i in (round..R_KEYS).step_by(7) {
        let pages = (i + round) as usize % 5;
        put(txn, state, "r", &key(i), &run_value(30 + round, i, pages, page_size));
    }
}

/// The crashing commit: updates spread over `a` (copies of published pages); in `r`, inline
/// values replaced by runs, runs by inline values, by larger and by smaller runs, values deleted
/// from runs and from leaves, and new keys with runs of 1 to 10 pages; table `x` (runs) dropped.
/// It ends with a ten-page run that no pool fits (it goes past the newest page), then deletes a
/// run it wrote from a leaf it wrote, whose pages go back to the pool, where the free list's save
/// takes its pages: the commit's highest page stays the last run's last page.
fn runs_crash(txn: &mut RwTxn, state: &mut State) {
    let page_size = page_size(txn);
    for i in (0..400).step_by(7) {
        put(txn, state, "a", &key(i), &value(21, i, 240));
    }
    for i in 0..R_KEYS {
        let pages = match (base_pages(i), i % 3) {
            (0, 0) => 3,  // inline to a run
            (1, _) => 0,  // a run to inline
            (2, _) => 5,  // to a larger run
            (9, _) => 2,  // to a smaller run
            (4, 0) => 10, // to a larger run again
            _ => continue,
        };
        put(txn, state, "r", &key(i), &run_value(22, i, pages, page_size));
    }
    for i in (4..R_KEYS).step_by(8).chain((5..R_KEYS).step_by(16)) {
        delete(txn, state, "r", &key(i));
    }
    for (i, pages) in [1, 2, 5, 7, 10].into_iter().enumerate() {
        put(txn, state, "r", &key(100 + i as u32), &run_value(23, i as u32, pages, page_size));
    }
    drop_table(txn, state, "x");
    let top = txn.writer_mut().alloc.page_count();
    put(txn, state, "r", &key(200), &run_value(25, 200, 10, page_size));
    assert!(txn.writer_mut().alloc.page_count() > top, "the last run goes past the newest page");
    delete(txn, state, "r", &key(104));
}

/// [`runs_crash`], then table `d` filled with ten-page runs until one grows the file.
fn runs_crash_growing(txn: &mut RwTxn, state: &mut State) {
    runs_crash(txn, state);
    let page_size = page_size(txn);
    let start = txn.writer_mut().file.file_len();
    let mut i = 0;
    while txn.writer_mut().file.file_len() == start {
        put(txn, state, "d", &key(i), &run_value(24, i, 10, page_size));
        i += 1;
    }
}

/// The matrix on the run workload: every crash point x {process crash, strict power loss, random
/// power loss with two seeds} x {`Durable`, `NoSync`} x {data synced by `fdatasync`, by one
/// `msync`} reopens to exactly the allowed state, whose values read back byte for byte, which
/// checks and survives a crash right after the reopen.
#[test]
fn every_crash_point_recovers_a_workload_of_overflow_runs() {
    matrix(&runs_base(4096), 2);
}

/// The run workload's matrix at two page sizes, with many random power losses.
#[test]
#[ignore = "heavy: run with --release"]
fn every_crash_point_recovers_a_workload_of_overflow_runs_heavy() {
    for page_size in [4096, 16384] {
        matrix(&runs_base(page_size), 40);
    }
}

/// A crash commit that also grows the file through a run allocation places runs both in reused
/// pages and past the end of the file, and its growth survives a power loss once the commit
/// returned, by either data sync: the `fdatasync` makes the new length durable with the data, and
/// after one `msync` the size sync does, before the meta naming it.
#[test]
fn a_commit_that_grew_the_file_through_a_run_survives_a_power_loss() {
    let base = Base { crash: runs_crash_growing, grows: true, ..runs_base(4096) };
    let base_len = fs::metadata(&base.path).expect("metadata").len();
    for by_msync in [false, true] {
        let Case { path, latest, runs, reused, .. } =
            run_case(&base, SyncMode::Durable, by_msync, None);
        let what = format!("by_msync={by_msync}: {runs:?}");
        assert!(reused > 0, "{what}: pages were reused");
        assert!(runs.steps[0] + runs.steps[1] > 0, "{what}: runs in reused pages");
        assert!(runs.steps[2] + runs.steps[3] > 0, "{what}: runs past the newest page");
        assert!(fs::metadata(&path).expect("metadata").len() > base_len, "{what}: grown");
        disk_model::power_loss(&path);
        disk_model::untrack(&path);
        let env = open(&path, SyncMode::Durable);
        assert_eq!(env.stat().txn_id, base.txn + 1, "{what}");
        assert_eq!(newest_state(&env), latest, "{what}");
        assert_gets(&env, &latest, &what);
        drop(env);
        fs::remove_file(&path).expect("remove");
    }
}
