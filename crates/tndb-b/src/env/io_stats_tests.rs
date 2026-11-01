//! The I/O and allocation counters ([`Env::io_stats`]): what a commit syncs and grows, the
//! copies and checks of a write, NoSync commits and their sync, the reuse gate under a held
//! reader, overflow runs by policy step, and a read-only open.

use std::{fs, path::Path};

use tempfile::TempDir;

use super::*;
use crate::{ReadTxn, Table};

fn temp() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::with_prefix("tndb_b_io_stats").expect("temp dir");
    let path = dir.path().join("db");
    (dir, path)
}

fn open(path: &Path, sync: SyncMode) -> Env {
    Env::open(path, EnvOptions { sync, ..EnvOptions::default() }).expect("open")
}

/// A key of table `t`: 100 bytes, so a branch holds few and a small tree has three levels.
fn key(i: u32) -> Vec<u8> {
    let mut k = vec![b'k'; 100];
    k[96..].copy_from_slice(&i.to_be_bytes());
    k
}

/// Put `keys` into table `t` (created if missing) with `len`-byte values tagged `tag`, in one
/// committed transaction.
fn put_keys(env: &Env, keys: impl IntoIterator<Item = u32>, tag: u8, len: usize) {
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in keys {
        txn.put(&t, &key(i), &vec![tag; len]).expect("put");
    }
    txn.commit().expect("commit");
}

fn table(txn: &impl ReadTxn) -> Table {
    txn.open_table("t").expect("open").expect("t exists")
}

/// A durable commit on a new file: every page it wrote is new at the end of the file, and step 4
/// syncs exactly those (one range, so one `msync` spanning them); the file grew, so the commit
/// counts as grown, by the bytes the file's length grew. A second commit that does not grow the
/// file syncs the pages it wrote and counts no growth.
#[test]
fn a_growing_commit_counts_its_pages_sync_and_growth() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let opened_len = fs::metadata(&path).expect("len").len();
    assert_eq!(env.io_stats(), IoStats::default(), "nothing done yet");
    put_keys(&env, [1], 1, 10);
    let s = env.io_stats();
    let report = env.check().expect("check");
    assert_eq!(report.free_pages, 0, "every page past the metas is a tree page of this commit");
    let written = u64::from(report.page_count) - 2;
    assert_eq!((s.commits, s.nosync_commits, s.grown_commits), (1, 0, 1), "{s:?}");
    assert_eq!(s.pages_synced, written, "{s:?}");
    assert_eq!((s.pages_from_end, s.pages_from_pool), (written, 0), "{s:?}");
    assert_eq!((s.msync_syncs, s.fdatasync_syncs), (1, 0), "one range: one msync; {s:?}");
    assert_eq!(s.msync_span_pages, written, "adjacent pages: the span is the pages; {s:?}");
    let grown = fs::metadata(&path).expect("len").len() - opened_len;
    assert!(grown > 0);
    assert_eq!(s.bytes_grown, grown, "{s:?}");
    assert_eq!(s.free_list_save_rounds, 0, "nothing freed or taken: nothing to list");
    assert!(s.save_ns > 0 && s.stamp_ns > 0 && s.data_sync_ns > 0 && s.meta_sync_ns > 0, "{s:?}");
    assert_eq!(s.cow_copies, 0, "nothing was published to copy");

    let pages_before = env.stat().page_count;
    put_keys(&env, [1], 2, 10);
    let d = env.io_stats().since(&s);
    let new_pages = u64::from(env.stat().page_count - pages_before);
    assert_eq!((d.commits, d.grown_commits, d.bytes_grown), (1, 0, 0), "no growth; {d:?}");
    // The gate holds back txn 1's frees at txn 2, so every page written is new.
    assert_eq!((d.pages_synced, d.pages_from_end, d.pages_from_pool), (new_pages, new_pages, 0));
    assert_eq!(d.msync_syncs + d.fdatasync_syncs, 1, "{d:?}");
    assert!(d.cow_copies >= 2, "the table's leaf and the catalog's: {d:?}");
    assert!(d.free_list_save_rounds >= 1, "the copied pages are listed: {d:?}");
}

/// An update of one key in a published three-level tree copies its path, one copy per level, and
/// checks each page of it first, since this writer stamped none of them (the file was reopened);
/// a second update in that leaf copies and checks nothing. After the commit this writer stamped
/// the copies, so a later update of that key copies the path again but checks none of it. An
/// abort's work counts, published at the abort.
#[test]
fn an_update_copies_and_checks_its_path() {
    let (_dir, path) = temp();
    put_keys(&open(&path, SyncMode::Durable), 0..1500, 1, 100);
    let env = open(&path, SyncMode::Durable);
    let mut txn = env.begin_rw().expect("begin");
    let t = table(&txn);
    let height = u64::from(txn.table_stat(&t).expect("stat").height);
    assert!(height >= 3, "a three-level tree: {height}");
    let counted = |txn: &mut RwTxn| {
        let s = txn.writer_mut().io_stats();
        (s.cow_copies, s.verifies)
    };
    let before = counted(&mut txn);
    txn.put(&t, &key(700), &[2; 100]).expect("put");
    let after = counted(&mut txn);
    assert_eq!((after.0 - before.0, after.1 - before.1), (height, height), "the path");
    txn.put(&t, &key(701), &[2; 100]).expect("put");
    assert_eq!(counted(&mut txn), after, "the same leaf, private now");
    assert_eq!(env.io_stats(), IoStats::default(), "nothing published while the write runs");
    txn.commit().expect("commit");
    let committed = env.io_stats();
    assert!(committed.cow_copies > height, "the catalog's copy too: {committed:?}");
    assert!(committed.verify_ns > 0, "{committed:?}");

    let mut txn = env.begin_rw().expect("begin");
    let t = table(&txn);
    let before = counted(&mut txn);
    txn.put(&t, &key(700), &[3; 100]).expect("put");
    let after = counted(&mut txn);
    assert_eq!(after.0 - before.0, height, "the path is copied again");
    assert_eq!(after.1 - before.1, 0, "this writer stamped the path: no check");
    txn.abort();
    let aborted = env.io_stats().since(&committed);
    assert_eq!((aborted.cow_copies, aborted.verifies), (height, 0), "{aborted:?}");
    assert_eq!(aborted.commits, 0);
}

/// NoSync commits count as such and sync nothing; the `Env::sync` that makes their state
/// durable publishes its durable steps at once, the pages synced being every page the commits
/// stamped since the last sync; a sync with nothing pending counts nothing.
#[test]
fn nosync_commits_sync_their_pages_at_env_sync() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::NoSync);
    put_keys(&env, 0..20, 1, 50);
    put_keys(&env, [3], 2, 50);
    let s = env.io_stats();
    assert_eq!((s.nosync_commits, s.commits), (2, 0), "{s:?}");
    assert_eq!((s.pages_synced, s.msync_syncs, s.fdatasync_syncs), (0, 0, 0), "{s:?}");
    assert_eq!((s.data_sync_ns, s.size_sync_ns, s.meta_sync_ns), (0, 0, 0), "{s:?}");
    assert!(s.save_ns > 0 && s.stamp_ns > 0, "{s:?}");
    let pending = env.with_writer(|w| w.unsynced.count()) as u64;
    assert!(pending > 0);
    env.sync().expect("sync");
    let d = env.io_stats().since(&s);
    assert_eq!((d.commits, d.nosync_commits, d.grown_commits), (1, 0, 1), "{d:?}");
    assert_eq!(d.pages_synced, pending, "{d:?}");
    assert_eq!(d.msync_syncs + d.fdatasync_syncs, 1, "{d:?}");
    assert!(d.data_sync_ns > 0 && d.meta_sync_ns > 0, "{d:?}");
    let synced = env.io_stats();
    env.sync().expect("a sync with nothing pending");
    assert_eq!(env.io_stats(), synced, "nothing pending: nothing counted");
}

/// A reader held on an old snapshot holds the reuse gate back, so the gate's lag at each refill
/// grows with the commits since, and the freed pages wait; once it ends, the lag falls to the
/// floor of 2 (a page freed by txn N comes back at txn N + 2) and the pages come from the pool.
/// With the reader on txn 1, txns 2 to 7 refill with the gate at the lower slot's txn (0, then
/// 1) and then at the reader's (1): lags 2, 2, 3, 4, 5, 6.
#[test]
fn a_held_reader_raises_the_gate_lag_until_it_ends() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    put_keys(&env, 0..200, 1, 100);
    let reader = env.begin_ro().expect("ro");
    let start = env.io_stats();
    for round in 0..6 {
        put_keys(&env, [round * 30], 2, 100);
    }
    let held = env.io_stats().since(&start);
    assert_eq!(held.gate_lag_samples, 6, "one refill per commit: {held:?}");
    assert_eq!(held.gate_lag_sum, 2 + 2 + 3 + 4 + 5 + 6, "held back: {held:?}");
    assert_eq!(held.max_gate_lag, 6, "{held:?}");
    assert_eq!(held.pages_from_pool, 0, "nothing reusable while the reader holds it: {held:?}");
    drop(reader);
    put_keys(&env, [1], 3, 100);
    let pages = env.stat().page_count;
    let start = env.io_stats();
    for round in 0..4 {
        put_keys(&env, [round * 30 + 2], 4, 100);
    }
    let after = env.io_stats().since(&start);
    assert!(after.gate_lag_samples >= 4, "{after:?}");
    assert_eq!(after.gate_lag_sum, 2 * after.gate_lag_samples, "the floor: {after:?}");
    assert!(after.records_loaded > 0 && after.refill_ns > 0, "{after:?}");
    assert!(after.pages_from_pool > 0, "the held pages come back: {after:?}");
    assert_eq!(after.pages_from_end, 0, "{after:?}");
    assert_eq!(env.stat().page_count, pages, "the file's pages are reused, not added");
}

/// Overflow runs count by the run policy's step, as the test-only `RunStats` classifies them,
/// and their pages by where they came from: runs on a new file come from its end (step 3), and a
/// run once its predecessors were freed and the gate passed them takes those pages from the pool.
#[test]
fn overflow_runs_count_by_policy_step() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let value = vec![9; 10_000];
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..3 {
        txn.put(&t, &key(i), &value).expect("put");
    }
    txn.commit().expect("commit");
    let fresh = env.io_stats();
    assert_eq!(fresh.runs_by_step, [0, 0, 0, 3], "{fresh:?}");
    let written = u64::from(env.stat().page_count) - 2;
    assert_eq!((fresh.pages_from_end, fresh.pages_from_pool), (written, 0), "{fresh:?}");
    let mut txn = env.begin_rw().expect("begin");
    let t = table(&txn);
    for i in 0..3 {
        assert!(txn.delete(&t, &key(i)).expect("delete"));
    }
    txn.commit().expect("commit");
    put_keys(&env, [100], 1, 10);
    let mut txn = env.begin_rw().expect("begin");
    let w = txn.writer_mut();
    let (before, count) = (w.io_stats(), w.alloc.page_count());
    PageWrite::alloc_run(w, 3).expect("run");
    let d = w.io_stats().since(&before);
    assert_eq!(d.runs_by_step, [1, 0, 0, 0], "a fit in the pool the refill loaded: {d:?}");
    assert_eq!((d.pages_from_pool, d.pages_from_end), (3, 0), "{d:?}");
    assert_eq!(w.alloc.page_count(), count, "the file did not grow");
    txn.abort();
    let mut txn = env.begin_rw().expect("begin");
    let t = table(&txn);
    for i in 10..13 {
        txn.put(&t, &key(i), &value).expect("put");
    }
    txn.commit().expect("commit");
    let s = env.io_stats();
    let reused = s.since(&fresh);
    assert_eq!(reused.runs_by_step.iter().sum::<u64>(), 4, "the aborted run counts: {reused:?}");
    assert!(reused.runs_by_step[3] < 3, "some run reused freed pages: {reused:?}");
    assert!(reused.pages_from_pool >= 3, "{reused:?}");
    assert_eq!(s.runs_by_step, env.with_writer(|w| w.alloc.run_stats.steps), "as RunStats");
}

/// A read-only open has no writer: its counters stay zero whatever it reads.
#[test]
fn a_read_only_open_counts_nothing() {
    let (_dir, path) = temp();
    put_keys(&open(&path, SyncMode::Durable), 0..50, 1, 100);
    let env = Env::open_read_only(&path, EnvOptions::default()).expect("read-only");
    let ro = env.begin_ro().expect("ro");
    assert!(ro.get(&table(&ro), &key(7)).expect("get").is_some());
    env.sync().expect("a read-only sync does nothing");
    assert_eq!(env.io_stats(), IoStats::default());
}

/// A sliced step 3 (`Writer::writeback_slice`, every durable commit sliced) counts one writeback
/// start per slice where the target has the call (Linux) and none elsewhere, its time inside the
/// stamp's; `NoSync` commits and their sync start none, with the knob set too.
#[test]
fn sliced_commits_count_their_writeback_starts() {
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let (_dir, path) = temp();
        let env = open(&path, sync);
        env.with_writer(|w| {
            w.writeback_slice = Some(2 * 4096);
            *w.writeback_log.get_mut() = Some(Vec::new());
        });
        let before = env.io_stats();
        put_keys(&env, 0..200, 1, 300);
        env.sync().expect("sync");
        let d = env.io_stats().since(&before);
        let spans = env.with_writer(|w| w.writeback_log.get_mut().take().expect("log").len());
        assert_eq!(spans > 1, sync == SyncMode::Durable, "{sync:?}: {spans} slices");
        let counted = if cfg!(target_os = "linux") { spans as u64 } else { 0 };
        assert_eq!(d.writeback_starts, counted, "{sync:?}: {d:?}");
        assert!(d.writeback_ns <= d.stamp_ns, "{sync:?}: {d:?}");
        assert_eq!(d.writeback_ns > 0, d.writeback_starts > 0, "{sync:?}: {d:?}");
    }
}

/// On Linux a durable commit of 40 MiB of runs, past the slicing threshold with no knob set,
/// reaches `sync_file_range` once per slice: the counter moves, the calls succeed, and their time
/// lies inside the stamp's.
#[cfg(target_os = "linux")]
#[test]
fn a_big_durable_commit_starts_its_writeback_on_linux() {
    let (_dir, path) = temp();
    let env = open(&path, SyncMode::Durable);
    let before = env.io_stats();
    put_keys(&env, 0..40, 1, 1 << 20);
    let d = env.io_stats().since(&before);
    assert!(d.writeback_starts >= 2, "{d:?}");
    assert!(d.writeback_ns > 0 && d.writeback_ns <= d.stamp_ns, "{d:?}");
    assert_eq!(env.failure(), None);
}
