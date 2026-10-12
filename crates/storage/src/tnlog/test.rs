//! tnlog's tests: the shared `Database` suite in both commit modes, recovery, transaction
//! atomicity, batch rollback, and durability.

use std::{collections::BTreeMap, path::Path, time::Duration};

use tempfile::TempDir;
use tn_types::{Database as _, DbTx as _, DbTxMut as _};

use super::{
    database::{CommitMode, CompactionConfig, TnLogDatabase, TnLogOptions},
    record::{self, KIND_PUT},
};
use crate::test::*;

/// A second table for the cross-table tests.
#[derive(Debug)]
struct OtherTable;
impl tn_types::Table for OtherTable {
    type Key = u64;
    type Value = String;

    const NAME: &'static str = "OtherTable";
    const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
}

fn options(commit: CommitMode) -> TnLogOptions {
    TnLogOptions { commit, ..Default::default() }
}

/// The database at `path`, both test tables open.
fn open(path: &Path, commit: CommitMode) -> TnLogDatabase {
    let db = TnLogDatabase::open_with(path, options(commit)).expect("open tnlog");
    db.open_table::<TestTable>().expect("open table");
    db.open_table::<OtherTable>().expect("open table");
    db
}

/// Simulate a crash: stop the commit thread and release the lock, as a dead process's are, and
/// leak the handle so nothing is synced or sealed on the way out.
fn crash(db: TnLogDatabase) {
    db.release_locks_for_crash();
    std::mem::forget(db);
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime")
        .block_on(f)
}

fn rows<T: tn_types::Table<Key = u64, Value = String>>(db: &TnLogDatabase) -> Vec<(u64, String)> {
    db.iter::<T>().collect()
}

/// The end offset of each whole record of the log at `path`, read up to its first bad frame.
fn record_ends(path: &Path) -> Vec<u64> {
    use crate::archive::pack_iter::PackIter;
    let len = std::fs::metadata(path).expect("log").len();
    let file = std::fs::File::open(path).expect("open log");
    let mut iter = PackIter::<Vec<u8>, _>::open(file, 0, len).expect("log header");
    let mut ends = Vec::new();
    while let Some(Ok(_)) = iter.next_raw() {
        ends.push(iter.logical_position());
    }
    ends
}

fn log_path(root: &Path) -> std::path::PathBuf {
    root.join("gen-0").join("log")
}

fn truncate(path: &Path, len: u64) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open")
        .set_len(len)
        .expect("truncate");
}

#[test]
fn test_tnlog_shared_suite() {
    for commit in [CommitMode::Sync, CommitMode::Group] {
        let run = |test: fn(TnLogDatabase)| {
            let tmp = TempDir::with_prefix("tnlog_suite").expect("temp dir");
            test(open(tmp.path(), commit));
        };
        run(test_contains_key);
        run(test_get);
        run(test_multi_get);
        run(test_skip);
        run(test_skip_to_previous_simple);
        run(test_iter_skip_to_previous_gap);
        run(test_remove);
        run(test_iter);
        run(test_iter_reverse);
        run(test_clear);
        run(test_is_empty);
        run(test_multi_insert);
        run(test_multi_remove);
    }
}

/// A clean close keeps every row of every table, and the reopen needs no rebuild.
#[test]
fn test_tnlog_clean_reopen() {
    let tmp = TempDir::with_prefix("tnlog_reopen").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        for i in 0..50u64 {
            db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
            db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
        }
        db.remove::<TestTable>(&7).expect("remove");
        db.clear_table::<OtherTable>().expect("clear");
        db.insert::<OtherTable>(&3, &"after".to_string()).expect("insert");
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db).len(), 49);
    assert_eq!(db.get::<TestTable>(&7).expect("get"), None);
    assert_eq!(rows::<OtherTable>(&db), vec![(3, "after".to_string())]);
}

/// Interleaved writes to several tables, then a crash: the reopen rebuilds every table from the
/// one log, keeping every write made before the crash in `Sync` mode (each durable on return).
#[test]
fn test_tnlog_crash_rebuilds_interleaved_tables() {
    let tmp = TempDir::with_prefix("tnlog_crash").expect("temp dir");
    let mut test = BTreeMap::new();
    let mut other = BTreeMap::new();
    {
        let db = open(tmp.path(), CommitMode::Sync);
        for i in 0..200u64 {
            db.insert::<TestTable>(&(i % 40), &format!("t{i}")).expect("insert");
            test.insert(i % 40, format!("t{i}"));
            if i % 3 == 0 {
                db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
                other.insert(i, format!("o{i}"));
            }
            if i % 7 == 0 {
                db.remove::<TestTable>(&(i % 13)).expect("remove");
                test.remove(&(i % 13));
            }
        }
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db), test.into_iter().collect::<Vec<_>>());
    assert_eq!(rows::<OtherTable>(&db), other.into_iter().collect::<Vec<_>>());
}

/// A transaction is atomic across tables: a batch whose flagged last record is torn by the crash
/// is dropped whole (from both tables), and the batch before it is kept whole.
#[test]
fn test_tnlog_txn_atomic_across_tables() {
    let tmp = TempDir::with_prefix("tnlog_txn_atomic").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        txn.insert::<OtherTable>(&1, &"b".to_string()).expect("insert");
        txn.commit().expect("commit");
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&2, &"c".to_string()).expect("insert");
        txn.remove::<TestTable>(&1).expect("remove");
        txn.insert::<OtherTable>(&2, &"d".to_string()).expect("insert");
        txn.commit().expect("commit");
        crash(db);
    }
    let log = log_path(tmp.path());
    let ends = record_ends(&log);
    assert_eq!(ends.len(), 5, "two batches: 2 records, then 3");
    truncate(&log, ends[3] + 5); // tear the second batch's flagged last record
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db), vec![(1, "a".to_string())]);
    assert_eq!(rows::<OtherTable>(&db), vec![(1, "b".to_string())]);
}

/// A write transaction's own reads see its writes (and its clear); nobody else does until it
/// commits; a dropped transaction is written like a committed one.
#[test]
fn test_tnlog_txn_buffered() {
    let tmp = TempDir::with_prefix("tnlog_txn_buffered").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"old".to_string()).expect("insert");
    db.insert::<TestTable>(&2, &"two".to_string()).expect("insert");
    let mut txn = db.write_txn().expect("txn");
    txn.insert::<TestTable>(&1, &"new".to_string()).expect("insert");
    assert_eq!(txn.get::<TestTable>(&1).expect("get"), Some("new".to_string()));
    assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("old".to_string()), "not yet");
    txn.clear_table::<OtherTable>().expect("clear");
    txn.remove::<TestTable>(&2).expect("remove");
    assert_eq!(txn.get::<TestTable>(&2).expect("get"), None);
    txn.commit().expect("commit");
    assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("new".to_string()));
    assert_eq!(db.get::<TestTable>(&2).expect("get"), None);
    {
        let mut dropped = db.write_txn().expect("txn");
        dropped.insert::<OtherTable>(&9, &"kept".to_string()).expect("insert");
    }
    assert_eq!(db.get::<OtherTable>(&9).expect("get"), Some("kept".to_string()));
}

/// A clear between puts in one batch: the puts before it are gone, those after it stay, through
/// a crash rebuild.
#[test]
fn test_tnlog_clear_inside_a_batch() {
    let tmp = TempDir::with_prefix("tnlog_clear_batch").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        db.insert::<TestTable>(&1, &"before".to_string()).expect("insert");
        let mut txn = db.write_txn().expect("txn");
        txn.clear_table::<TestTable>().expect("clear");
        txn.insert::<TestTable>(&2, &"after".to_string()).expect("insert");
        txn.commit().expect("commit");
        assert_eq!(rows::<TestTable>(&db), vec![(2, "after".to_string())]);
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db), vec![(2, "after".to_string())]);
}

/// A batch whose index step fails part-way is taken back whole: the log rewound and every
/// touched index reverted, so nothing of it is readable or survives a crash, and later writes
/// work.
#[test]
fn test_tnlog_failed_batch_is_taken_back() {
    let tmp = TempDir::with_prefix("tnlog_rollback").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        for i in 0..20u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
        }
        let other = db.core().table("OtherTable").expect("table").id;
        db.core().with_index(other, |idx| idx.fail_allocations_after(Some(0))).expect("index");
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&100, &"x".to_string()).expect("insert");
        txn.remove::<TestTable>(&5).expect("remove");
        txn.insert::<OtherTable>(&100, &"y".to_string()).expect("insert");
        assert!(txn.commit().is_err(), "the index step fails");
        db.core().with_index(other, |idx| idx.fail_allocations_after(None)).expect("index");
        assert_eq!(db.get::<TestTable>(&100).expect("get"), None);
        assert_eq!(db.get::<TestTable>(&5).expect("get"), Some("v5".to_string()));
        db.insert::<TestTable>(&200, &"later".to_string()).expect("a later write works");
        // The later write published the table again: its working index was reverted, so the
        // failed batch's put (whose log bytes the later write reused) and removal are gone.
        assert_eq!(db.get::<TestTable>(&100).expect("get"), None);
        assert_eq!(db.get::<TestTable>(&5).expect("get"), Some("v5".to_string()));
        assert_eq!(db.get::<TestTable>(&200).expect("get"), Some("later".to_string()));
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(db.get::<TestTable>(&100).expect("get"), None, "the failed batch is gone");
    assert_eq!(db.get::<TestTable>(&5).expect("get"), Some("v5".to_string()));
    assert_eq!(db.get::<TestTable>(&200).expect("get"), Some("later".to_string()));
    assert_eq!(rows::<OtherTable>(&db).len(), 20);
}

/// Group mode: a write is readable at once and durable after `persist` (it survives a crash);
/// `persist` on one table covers every table's earlier writes.
#[test]
fn test_tnlog_group_persist() {
    let tmp = TempDir::with_prefix("tnlog_group").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..100u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.insert::<OtherTable>(&1, &"o".to_string()).expect("insert");
        assert_eq!(db.get::<TestTable>(&5).expect("get"), Some("v5".to_string()), "readable");
        block_on(db.persist::<TestTable>()).expect("persist");
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db).len(), 100);
    assert_eq!(db.get::<OtherTable>(&1).expect("get"), Some("o".to_string()));
}

/// Writers on several threads in `Sync` mode: every write returned is durable (it survives a
/// crash), and concurrent writers share syncs.
#[test]
fn test_tnlog_concurrent_sync_writers() {
    let tmp = TempDir::with_prefix("tnlog_threads").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let db = db.clone();
                s.spawn(move || {
                    for i in 0..300u64 {
                        let key = t * 1_000 + i;
                        if t % 2 == 0 {
                            db.insert::<TestTable>(&key, &format!("{key}")).expect("insert");
                        } else {
                            db.insert::<OtherTable>(&key, &format!("{key}")).expect("insert");
                        }
                    }
                });
            }
        });
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db).len(), 600);
    assert_eq!(rows::<OtherTable>(&db).len(), 600);
}

/// A record of a table the manifest does not have, or of an unknown kind, fails the open closed
/// and changes nothing.
#[test]
fn test_tnlog_unknown_record_fails_closed() {
    for bad in [record::head(KIND_PUT, 9, true).to_vec(), vec![0x7e, 0, 0]] {
        let tmp = TempDir::with_prefix("tnlog_bad_record").expect("temp dir");
        {
            let db = open(tmp.path(), CommitMode::Sync);
            db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
            drop(db);
        }
        // Append a well-framed record carrying `bad` with a fresh pack handle, unsealed.
        {
            use crate::archive::{
                data_file::MmapFileOptions,
                pack::{Pack, PackCompression},
            };
            let mut log: Pack<Vec<u8>> = Pack::open_with(
                log_path(tmp.path()),
                0,
                false,
                PackCompression::None,
                1,
                MmapFileOptions::default(),
            )
            .expect("open log");
            log.append_raw(&bad).expect("append");
            log.commit().expect("sync");
            log.poison(); // unsealed: the next open replays it
        }
        let len = std::fs::metadata(log_path(tmp.path())).expect("log").len();
        assert!(TnLogDatabase::open(tmp.path()).is_err(), "{bad:?} fails the open");
        assert_eq!(std::fs::metadata(log_path(tmp.path())).expect("log").len(), len);
    }
}

/// A scan that outlives every database handle keeps reading its snapshot (it holds the core, so
/// the files stay open), and the database closes cleanly once the scan is gone.
#[test]
fn test_tnlog_scan_outlives_the_handles() {
    let tmp = TempDir::with_prefix("tnlog_scan").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Sync);
    for i in 0..100u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    let core = std::sync::Arc::clone(db.core());
    let mut scan =
        core.table("TestTable").expect("table").scan(super::store::ScanKind::Forward, core.clone());
    drop(core);
    drop(db);
    let count = std::iter::from_fn(|| scan.next_with(|_, _| ())).count();
    assert_eq!(count, 100);
    assert!(TnLogDatabase::open(tmp.path()).is_err(), "still open while the scan lives");
    drop(scan);
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db).len(), 100);
}

/// A second open of a database is refused while the first holds it.
#[test]
fn test_tnlog_second_open_refused() {
    let tmp = TempDir::with_prefix("tnlog_lock").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Sync);
    assert!(TnLogDatabase::open(tmp.path()).is_err());
    drop(db);
    TnLogDatabase::open(tmp.path()).expect("reopen after close");
}

/// `persist` (group mode) returns within the time a sync takes, not a timeout.
#[test]
fn test_tnlog_group_persist_is_prompt() {
    let tmp = TempDir::with_prefix("tnlog_prompt").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let persisted = block_on(async {
        tokio::time::timeout(Duration::from_secs(5), db.persist::<TestTable>()).await
    });
    assert!(matches!(persisted, Ok(Ok(()))), "{persisted:?}");
}

/// A failed sync latches: `persist` fails, and so does every later write and `persist` (the
/// watermark never moves again).
#[test]
fn test_tnlog_failed_sync_latches() {
    let tmp = TempDir::with_prefix("tnlog_latch").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    db.core().durability.fail_next_sync.store(true, std::sync::atomic::Ordering::Relaxed);
    db.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    assert!(block_on(db.persist::<TestTable>()).is_err(), "the failed sync surfaces");
    assert!(db.insert::<OtherTable>(&3, &"c".to_string()).is_err(), "writes are refused");
    assert!(block_on(db.persist::<OtherTable>()).is_err(), "and it stays latched");
}

/// A batch that panics part-way is taken back (nothing of it is readable or survives a crash)
/// and latches the database.
#[test]
fn test_tnlog_panicking_batch_is_taken_back() {
    let tmp = TempDir::with_prefix("tnlog_panic").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        db.core().panic_next_batch_after(1);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut txn = db.write_txn().expect("txn");
            txn.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
            txn.insert::<OtherTable>(&2, &"c".to_string()).expect("insert");
            txn.commit()
        }));
        assert!(panicked.is_err(), "the batch panicked");
        assert_eq!(db.get::<TestTable>(&2).expect("get"), None, "nothing of it is readable");
        assert!(db.insert::<TestTable>(&3, &"d".to_string()).is_err(), "latched");
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db), vec![(1, "a".to_string())]);
    assert!(rows::<OtherTable>(&db).is_empty());
}

/// On a cleanly closed log, a missing (stale) index is rebuilt alone: the other tables' indexes
/// are trusted as they are. An index directory the manifest does not know is deleted.
#[test]
fn test_tnlog_stale_index_rebuilds_alone() {
    let tmp = TempDir::with_prefix("tnlog_stale").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        for i in 0..30u64 {
            db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
            db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
        }
    }
    let (test_id, other_id) = {
        let db = open(tmp.path(), CommitMode::Sync);
        assert!(db.core().rebuilt_on_open().is_empty(), "a clean close needs no rebuild");
        let core = db.core();
        (core.table("TestTable").expect("t").id, core.table("OtherTable").expect("o").id)
    };
    let gen = tmp.path().join("gen-0");
    std::fs::remove_dir_all(gen.join(format!("idx-{other_id}"))).expect("remove an index");
    std::fs::create_dir(gen.join("idx-77")).expect("an orphan index directory");
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(db.core().rebuilt_on_open(), vec![other_id], "only the stale table rebuilds");
    assert!(!gen.join("idx-77").exists(), "the orphan is deleted");
    assert_eq!(rows::<TestTable>(&db).len(), 30);
    assert_eq!(rows::<OtherTable>(&db).len(), 30);
    let _ = test_id;
}

/// Random bare writes, transactions (committed or dropped, over both tables), clears, persists
/// and crashes in group mode: after a crash both tables are exactly one of the states they passed
/// through together since the last persist (a transaction is never half there, in either table),
/// and never older than that persist.
#[test]
fn test_tnlog_random_crashes_against_model() {
    use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

    type Model = (BTreeMap<u64, String>, BTreeMap<u64, String>);
    let tmp = TempDir::with_prefix("tnlog_random").expect("temp dir");
    let mut rng = StdRng::seed_from_u64(0x7E10_6001);
    let mut db = open(tmp.path(), CommitMode::Group);
    let mut model: Model = Default::default();
    let mut states = vec![model.clone()];
    for step in 0..800 {
        match rng.random_range(0..100) {
            0..35 => {
                let k = rng.random_range(0..48);
                let v = format!("s{step}");
                if rng.random_bool(0.5) {
                    db.insert::<TestTable>(&k, &v).expect("insert");
                    model.0.insert(k, v);
                } else {
                    db.insert::<OtherTable>(&k, &v).expect("insert");
                    model.1.insert(k, v);
                }
                states.push(model.clone());
            }
            35..50 => {
                let k = rng.random_range(0..48);
                db.remove::<TestTable>(&k).expect("remove");
                model.0.remove(&k);
                states.push(model.clone());
            }
            50..68 => {
                let mut txn = db.write_txn().expect("txn");
                for _ in 0..rng.random_range(1..6) {
                    let k = rng.random_range(0..48);
                    let v = format!("t{step}");
                    match rng.random_range(0..4) {
                        0 => {
                            txn.remove::<TestTable>(&k).expect("txn remove");
                            model.0.remove(&k);
                        }
                        1 => {
                            txn.insert::<OtherTable>(&k, &v).expect("txn insert");
                            model.1.insert(k, v);
                        }
                        _ => {
                            txn.insert::<TestTable>(&k, &v).expect("txn insert");
                            model.0.insert(k, v);
                        }
                    }
                }
                if rng.random_bool(0.8) {
                    txn.commit().expect("commit");
                } // else dropped: written the same way
                states.push(model.clone());
            }
            68..71 => {
                if rng.random_bool(0.5) {
                    db.clear_table::<TestTable>().expect("clear");
                    model.0.clear();
                } else {
                    db.clear_table::<OtherTable>().expect("clear");
                    model.1.clear();
                }
                states.push(model.clone());
            }
            71..86 => {
                block_on(db.persist::<TestTable>()).expect("persist");
                states = vec![model.clone()];
            }
            86..90 => {
                // A compaction changes no state.
                assert_eq!(db.compact_now(), Ok(true), "step {step}: compacted");
            }
            _ => {
                crash(db);
                db = open(tmp.path(), CommitMode::Group);
                let found: Model =
                    (db.iter::<TestTable>().collect(), db.iter::<OtherTable>().collect());
                assert!(
                    states.contains(&found),
                    "step {step}: the reopened tables are not a state since the last persist"
                );
                model = found;
                states = vec![model.clone()];
            }
        }
    }
}

/// A batch becomes durable only once the commit thread's sync covering it has finished: while
/// that sync is paused, the watermark stays below the batch and `persist` waits.
#[test]
fn test_tnlog_durable_only_after_the_sync() {
    let tmp = TempDir::with_prefix("tnlog_durable_after").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    let before = db.core().durability.durable();
    let (arrived, release) = db.core().durability.gate_next_sync();
    db.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    arrived.recv_timeout(Duration::from_secs(10)).expect("the commit thread paused at its sync");
    assert_eq!(db.core().durability.durable(), before, "not durable before its sync");
    let pending = block_on(async {
        tokio::time::timeout(Duration::from_millis(200), db.persist::<TestTable>()).await
    });
    assert!(pending.is_err(), "persist waits for the sync");
    drop(release);
    block_on(db.persist::<TestTable>()).expect("persist after the sync");
}

/// In `Sync` mode a writer does not need the commit thread: the first writer waiting syncs the log
/// itself (and later waiters share its sync), so writes complete with the commit thread stopped.
#[test]
fn test_tnlog_sync_writers_sync_themselves() {
    let tmp = TempDir::with_prefix("tnlog_leader").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        db.stop_commit_thread();
        let (done, finished) = std::sync::mpsc::channel();
        let writer = db.clone();
        std::thread::spawn(move || {
            std::thread::scope(|s| {
                for t in 0..4u64 {
                    let db = writer.clone();
                    s.spawn(move || {
                        for i in 0..50u64 {
                            db.insert::<TestTable>(&(t * 100 + i), &format!("{t}-{i}"))
                                .expect("insert");
                        }
                    });
                }
            });
            let _ = done.send(());
        });
        finished.recv_timeout(Duration::from_secs(10)).expect("the writers synced themselves");
        crash(db);
    }
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db).len(), 200, "every returned write was durable");
}

/// Group mode syncs on demand: a burst of writes nobody waits on is made durable within a short
/// delay, by a few syncs rather than one per write.
#[test]
fn test_tnlog_group_syncs_on_demand() {
    let tmp = TempDir::with_prefix("tnlog_demand").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    let durability = &db.core().durability;
    let before = durability.syncs.load(std::sync::atomic::Ordering::Relaxed);
    for i in 0..2_000u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    let last = db.core().frontier.load().seq;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while durability.durable() < last && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(durability.durable(), last, "durable within the delay, with no persist");
    let syncs = durability.syncs.load(std::sync::atomic::Ordering::Relaxed) - before;
    assert!(syncs < 100, "{syncs} syncs for 2000 writes nobody waited on");
}

/// A `persist` syncs at once, without waiting out the delay that writes nobody waits on get.
#[test]
fn test_tnlog_persist_does_not_wait_for_the_delay() {
    let tmp = TempDir::with_prefix("tnlog_persist_now").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.core().durability.delay_ms.store(60_000, std::sync::atomic::Ordering::Relaxed);
    for i in 0..10u64 {
        db.insert::<TestTable>(&i, &"v".to_string()).expect("insert");
        let persisted = block_on(async {
            tokio::time::timeout(Duration::from_secs(5), db.persist::<TestTable>()).await
        });
        assert!(matches!(persisted, Ok(Ok(()))), "persist {i}: {persisted:?}");
    }
}

/// Group mode's writes nobody waits on are capped: once a log holds more unsynced bytes than the
/// cap, a writer waits for (or runs) a sync before returning, so unsynced data stays bounded even
/// when writes outrun the commit thread (here, stopped).
#[test]
fn test_tnlog_group_unsynced_bytes_are_capped() {
    let tmp = TempDir::with_prefix("tnlog_backpressure").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    let cap = 1 << 20;
    db.core().durability.max_unsynced.store(cap, std::sync::atomic::Ordering::Relaxed);
    db.stop_commit_thread();
    let value = "x".repeat(64 << 10);
    for i in 0..128u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
        let frontier = db.core().frontier.load();
        let unsynced =
            frontier.end - frontier.sync.synced.load(std::sync::atomic::Ordering::Acquire);
        assert!(unsynced <= cap + (64 << 10) + 1024, "write {i}: {unsynced} bytes unsynced");
    }
}

// ---- compaction ----

/// A third table, for the compaction tests.
#[derive(Debug)]
struct ThirdTable;
impl tn_types::Table for ThirdTable {
    type Key = u64;
    type Value = String;

    const NAME: &'static str = "ThirdTable";
    const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
}

/// No automatic compaction, and an unpaced copy: tests compact on request.
fn manual() -> CompactionConfig {
    CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 }
}

/// The database at `path`, both test tables open, compacting as `compaction` says.
fn open_compacting(path: &Path, commit: CommitMode, compaction: CompactionConfig) -> TnLogDatabase {
    let db = TnLogDatabase::open_with(path, TnLogOptions { commit, compaction }).expect("open");
    db.open_table::<TestTable>().expect("open table");
    db.open_table::<OtherTable>().expect("open table");
    db
}

/// The unfinished compactions under `root`.
fn compact_dirs(root: &Path) -> usize {
    std::fs::read_dir(root)
        .expect("read root")
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
        .count()
}

/// Wait (up to 10 s) until `root` holds generation `gen` alone (an old one is deleted in the
/// background).
fn wait_only_gen(root: &Path, gen: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let gens = crate::tndb::layout::list_gens(root).expect("list gens");
        if gens == [gen] {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "generations {gens:?}, want only {gen}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The log's length (its end, after the last batch).
fn log_len(db: &TnLogDatabase) -> u64 {
    db.core().writer.lock().log_len()
}

type Tables3 = [BTreeMap<u64, String>; 3];

fn insert_into(db: &TnLogDatabase, table: usize, k: u64, v: &String) {
    match table {
        0 => db.insert::<TestTable>(&k, v),
        1 => db.insert::<OtherTable>(&k, v),
        _ => db.insert::<ThirdTable>(&k, v),
    }
    .expect("insert");
}

fn all_rows(db: &TnLogDatabase) -> Tables3 {
    [
        db.iter::<TestTable>().collect(),
        db.iter::<OtherTable>().collect(),
        db.iter::<ThirdTable>().collect(),
    ]
}

/// `ops` random writes over the three tables (puts, overwrites, removes, transactions across
/// tables, a rare clear), applied to `model` too.
fn churn(
    db: &TnLogDatabase,
    model: &mut Tables3,
    rng: &mut rand::rngs::StdRng,
    ops: usize,
    tag: &str,
) {
    use rand::Rng as _;
    for i in 0..ops {
        let t = rng.random_range(0..3);
        let k = rng.random_range(0..64_u64);
        let v = format!("{tag}{i}-{}", "x".repeat(rng.random_range(0..200)));
        match rng.random_range(0..100) {
            0..45 => {
                insert_into(db, t, k, &v);
                model[t].insert(k, v);
            }
            45..70 => {
                match t {
                    0 => db.remove::<TestTable>(&k),
                    1 => db.remove::<OtherTable>(&k),
                    _ => db.remove::<ThirdTable>(&k),
                }
                .expect("remove");
                model[t].remove(&k);
            }
            70..98 => {
                let mut txn = db.write_txn().expect("txn");
                for j in 0..rng.random_range(1..5) {
                    let (t, k) = (rng.random_range(0..3), rng.random_range(0..64_u64));
                    if rng.random_bool(0.3) {
                        match t {
                            0 => txn.remove::<TestTable>(&k),
                            1 => txn.remove::<OtherTable>(&k),
                            _ => txn.remove::<ThirdTable>(&k),
                        }
                        .expect("txn remove");
                        model[t].remove(&k);
                    } else {
                        let v = format!("{tag}{i}.{j}");
                        match t {
                            0 => txn.insert::<TestTable>(&k, &v),
                            1 => txn.insert::<OtherTable>(&k, &v),
                            _ => txn.insert::<ThirdTable>(&k, &v),
                        }
                        .expect("txn insert");
                        model[t].insert(k, v);
                    }
                }
                txn.commit().expect("commit");
            }
            _ => {
                match t {
                    0 => db.clear_table::<TestTable>(),
                    1 => db.clear_table::<OtherTable>(),
                    _ => db.clear_table::<ThirdTable>(),
                }
                .expect("clear");
                model[t].clear();
            }
        }
    }
}

/// Wait for a gated compaction's next checkpoint.
fn next_checkpoint(arrivals: &std::sync::mpsc::Receiver<u32>) -> u32 {
    arrivals.recv_timeout(Duration::from_secs(30)).expect("a compaction checkpoint")
}

/// A compaction keeps every table's rows exactly, with writes landing at each of its steps (after
/// the copy, after its catch-up round, and holding the registry lock before the switch), and the
/// compacted generation reopens clean (no index rebuilt).
#[test]
fn test_tnlog_compaction_preserves_rows() {
    use rand::SeedableRng as _;
    let tmp = TempDir::with_prefix("tnlog_compact_rows").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    db.open_table::<ThirdTable>().expect("open table");
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0_4AC7);
    let mut model: Tables3 = Default::default();
    for (t, rows) in model.iter_mut().enumerate() {
        // Every table's key size fixed, so writes at checkpoint 3 need no registry lock.
        insert_into(&db, t, 0, &"first".to_string());
        rows.insert(0, "first".to_string());
    }
    churn(&db, &mut model, &mut rng, 800, "a");
    for round in 0..3 {
        let (arrivals, release) = db.core().compact.test.gate_next();
        let switched = std::thread::scope(|s| {
            let compaction = s.spawn(|| db.compact_now());
            let mut seen = Vec::new();
            loop {
                let n = next_checkpoint(&arrivals);
                seen.push(n);
                churn(&db, &mut model, &mut rng, 60, &format!("r{round}c{n}-"));
                release.send(()).expect("release");
                if n == 3 {
                    break;
                }
            }
            assert_eq!(seen, [1, 2, 3], "the checkpoints");
            compaction.join().expect("compaction thread")
        });
        assert_eq!(switched, Ok(true), "round {round}");
        assert_eq!(all_rows(&db), model, "round {round}: the rows after the switch");
        churn(&db, &mut model, &mut rng, 100, &format!("after{round}-"));
        assert_eq!(all_rows(&db), model, "round {round}: writes after the switch");
    }
    wait_only_gen(tmp.path(), 3);
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    db.open_table::<ThirdTable>().expect("open table");
    assert!(db.core().rebuilt_on_open().is_empty(), "a compacted generation reopens clean");
    assert_eq!(all_rows(&db), model, "the rows after a reopen");
}

/// A compaction gives back what removals and overwrites left dead, deletes the old generation,
/// and records the new log's length as the base the trigger doubles from.
#[test]
fn test_tnlog_compaction_returns_space() {
    let tmp = TempDir::with_prefix("tnlog_compact_space").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(4096);
    for i in 0..2_000_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    for i in 0..1_900_u64 {
        db.remove::<TestTable>(&i).expect("remove");
    }
    for i in 1_900..2_000_u64 {
        db.insert::<TestTable>(&i, &format!("{value}!")).expect("overwrite");
    }
    // A table with a key size but no rows: its (empty) index is in the new generation too.
    db.insert::<OtherTable>(&1, &value).expect("insert");
    db.remove::<OtherTable>(&1).expect("remove");
    let before = log_len(&db);
    assert_eq!(db.compact_now(), Ok(true));
    let after = log_len(&db);
    assert!(after < before / 10, "the log went from {before} to {after} bytes");
    assert_eq!(db.iter::<TestTable>().count(), 100, "the live rows");
    assert_eq!(db.get::<TestTable>(&1_950).expect("get"), Some(format!("{value}!")));
    wait_only_gen(tmp.path(), 1);
    assert_eq!(super::compact::read_base(&tmp.path().join("gen-1")), after, "the base");
    assert_eq!(compact_dirs(tmp.path()), 0);
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert!(db.core().rebuilt_on_open().is_empty(), "reopens clean, the empty table too");
    assert_eq!(db.iter::<TestTable>().count(), 100);
}

/// The automatic trigger: a log that only grows never compacts; one with dead rows compacts once
/// it is past the configured size and twice its last compacted length (the base), which a reopen
/// keeps.
#[test]
fn test_tnlog_compaction_trigger() {
    let tmp = TempDir::with_prefix("tnlog_compact_trigger").expect("temp dir");
    let config = CompactionConfig { auto_min_bytes: Some(1 << 20), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    let value = "v".repeat(4096);
    let quiet = || std::thread::sleep(Duration::from_millis(200));
    for i in 0..600_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    quiet();
    assert_eq!(db.core().compact.done(), 0, "an append-only log does not compact");

    db.insert::<TestTable>(&0, &value).expect("overwrite");
    assert_eq!(db.core().compact.wait_done(1), Some(Ok(true)), "a dead row: compacted");
    let base = super::compact::read_base(&tmp.path().join("gen-1"));
    assert!(base > 0, "the base is recorded");

    // Dead rows, but the log is still under twice the base.
    while log_len(&db) < base * 3 / 2 {
        db.insert::<TestTable>(&1, &value).expect("overwrite");
    }
    quiet();
    assert_eq!(db.core().compact.done(), 1, "under twice the base: no compaction");
    while log_len(&db) < base * 2 {
        db.insert::<TestTable>(&1, &value).expect("overwrite");
    }
    db.insert::<TestTable>(&1, &value).expect("overwrite");
    assert_eq!(db.core().compact.wait_done(2), Some(Ok(true)), "past twice the base");
    let rows = db.iter::<TestTable>().count();
    drop(db);

    // A reopen keeps the base: dead rows right after it do not compact again.
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    for _ in 0..10 {
        db.insert::<TestTable>(&1, &value).expect("overwrite");
    }
    quiet();
    assert_eq!(db.core().compact.done(), 0, "the base survived the reopen");
    assert_eq!(db.iter::<TestTable>().count(), rows);
}

/// A table whose key size was not yet fixed at the snapshot gets its first rows during the
/// compaction, after a catch-up round already read a record of it (a clear) while its key size was
/// still unfixed: the key size comes from the live table, and an unfixed one is never cached, so
/// the compaction keeps the rows.
#[test]
fn test_tnlog_compaction_first_rows_of_an_unfixed_table() {
    let tmp = TempDir::with_prefix("tnlog_compact_unfixed").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..50_u64 {
        db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    let switched = std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(next_checkpoint(&arrivals), 1);
        db.clear_table::<OtherTable>().expect("clear a table with no key size yet");
        // Over a round's worth, so the catch-up takes a second round.
        let big = "v".repeat(64 << 10);
        for i in 1_000..1_020_u64 {
            db.insert::<TestTable>(&i, &big).expect("insert");
        }
        release.send(()).expect("release");
        assert_eq!(next_checkpoint(&arrivals), 2, "the round that read the clear");
        for i in 0..10_u64 {
            db.insert::<OtherTable>(&i, &format!("o{i}")).expect("first rows");
        }
        drop(release);
        compaction.join().expect("compaction thread")
    });
    assert_eq!(switched, Ok(true));
    assert_eq!(db.iter::<OtherTable>().count(), 10);
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert!(db.core().rebuilt_on_open().is_empty(), "reopens clean");
    assert_eq!(db.iter::<OtherTable>().count(), 10);
    assert_eq!(db.iter::<TestTable>().count(), 70);
}

/// Each copied row is a whole batch: rows written inside a transaction (their records not its
/// last) survive a crash right after the compaction, with nothing written since.
#[test]
fn test_tnlog_compaction_copied_rows_are_whole_batches() {
    let tmp = TempDir::with_prefix("tnlog_compact_batches").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let mut txn = db.write_txn().expect("txn");
    for i in 1..=3_u64 {
        txn.insert::<TestTable>(&i, &format!("t{i}")).expect("txn insert");
    }
    txn.commit().expect("commit");
    // The batch's last (flagged) record is now dead: the copy's last row is an unflagged one.
    db.remove::<TestTable>(&3).expect("remove");
    block_on(db.persist::<TestTable>()).expect("persist");
    assert_eq!(db.compact_now(), Ok(true));
    crash(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert_eq!(db.iter::<TestTable>().map(|(k, _)| k).collect::<Vec<_>>(), [1, 2]);
}

/// A table registered (and its rows written) during a compaction, and a clear during one, are
/// carried into the compacted generation.
#[test]
fn test_tnlog_compaction_table_opened_and_cleared_during() {
    let tmp = TempDir::with_prefix("tnlog_compact_during").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..50_u64 {
        db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    let switched = std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(next_checkpoint(&arrivals), 1);
        db.open_table::<ThirdTable>().expect("open table");
        for i in 0..7_u64 {
            db.insert::<ThirdTable>(&i, &format!("n{i}")).expect("insert");
        }
        db.clear_table::<TestTable>().expect("clear");
        for i in 100..105_u64 {
            db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
        }
        drop(release);
        compaction.join().expect("compaction thread")
    });
    assert_eq!(switched, Ok(true));
    let expect_test: Vec<u64> = (100..105).collect();
    let keys = |db: &TnLogDatabase| db.iter::<TestTable>().map(|(k, _)| k).collect::<Vec<_>>();
    assert_eq!(keys(&db), expect_test, "only the rows written after the clear");
    assert_eq!(db.iter::<ThirdTable>().count(), 7);
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    db.open_table::<ThirdTable>().expect("open table");
    assert!(db.core().rebuilt_on_open().is_empty(), "reopens clean");
    assert_eq!(keys(&db), expect_test);
    assert_eq!(db.iter::<ThirdTable>().count(), 7);
}

/// A scan started before a switch reads its snapshot whole across it.
#[test]
fn test_tnlog_compaction_scan_across_the_switch() {
    let tmp = TempDir::with_prefix("tnlog_compact_scan").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
    }
    let expected: Vec<(u64, String)> = rows::<TestTable>(&db);
    let mut scan = db.iter::<TestTable>();
    let mut seen: Vec<(u64, String)> = scan.by_ref().take(10).collect();
    assert_eq!(db.compact_now(), Ok(true));
    for i in 0..100_u64 {
        db.remove::<TestTable>(&i).expect("remove after the switch");
    }
    wait_only_gen(tmp.path(), 1);
    seen.extend(scan);
    assert_eq!(seen, expected, "the scan read its snapshot whole");
}

/// The switch holds the registry lock: a table's key size cannot be fixed (its index created in
/// the generation being retired) until the switch is done.
#[test]
fn test_tnlog_compaction_switch_holds_the_registry() {
    let tmp = TempDir::with_prefix("tnlog_compact_registry").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..20_u64 {
        db.insert::<TestTable>(&i, &format!("t{i}")).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        for n in [1, 2] {
            assert_eq!(next_checkpoint(&arrivals), n);
            release.send(()).expect("release");
        }
        assert_eq!(next_checkpoint(&arrivals), 3);
        let first_row = s.spawn(|| db.insert::<OtherTable>(&1, &"o".to_string()));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!first_row.is_finished(), "a key size is fixed only after the switch");
        drop(release);
        assert_eq!(compaction.join().expect("compaction thread"), Ok(true));
        first_row.join().expect("first row").expect("insert");
    });
    assert_eq!(db.get::<OtherTable>(&1).expect("get"), Some("o".to_string()));
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert!(db.core().rebuilt_on_open().is_empty(), "the new table's index is in the new gen");
    assert_eq!(db.get::<OtherTable>(&1).expect("get"), Some("o".to_string()));
}

/// When more than the switch's cap lands while it waits for the write lock, it lets go, catches
/// up in the background, and switches on its next try, with every row kept.
#[test]
fn test_tnlog_compaction_switch_tail_is_bounded() {
    let tmp = TempDir::with_prefix("tnlog_compact_tail").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(64 << 10);
    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        for n in [1, 2] {
            assert_eq!(next_checkpoint(&arrivals), n);
            release.send(()).expect("release");
        }
        assert_eq!(next_checkpoint(&arrivals), 3);
        for i in 100..180_u64 {
            db.insert::<TestTable>(&i, &value).expect("a 5 MiB tail");
        }
        release.send(()).expect("release");
        assert_eq!(next_checkpoint(&arrivals), 3, "a second try");
        drop(release);
        assert_eq!(compaction.join().expect("compaction thread"), Ok(true));
    });
    let retries = db.core().compact.test.switch_retries.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(retries, 1, "the first switch let go of its locks");
    assert_eq!(db.iter::<TestTable>().count(), 90);
}

/// A crash after a switch: the compacted generation is current, and every write made durable
/// (before or after the switch) is there.
#[test]
fn test_tnlog_compaction_crash_after_the_switch() {
    let tmp = TempDir::with_prefix("tnlog_compact_crash").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
    }
    for i in 0..50_u64 {
        db.remove::<TestTable>(&i).expect("remove");
    }
    block_on(db.persist::<TestTable>()).expect("persist");
    assert_eq!(db.compact_now(), Ok(true));
    for i in 0..20_u64 {
        db.insert::<OtherTable>(&i, &format!("b{i}")).expect("insert");
    }
    block_on(db.persist::<OtherTable>()).expect("persist");
    let (test, other) = (rows::<TestTable>(&db), rows::<OtherTable>(&db));
    crash(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    wait_only_gen(tmp.path(), 1);
    assert_eq!(rows::<TestTable>(&db), test);
    assert_eq!(rows::<OtherTable>(&db), other);
}

/// An open deletes an unfinished compaction's directory (it is never a generation).
#[test]
fn test_tnlog_compaction_leftover_is_deleted_at_open() {
    let tmp = TempDir::with_prefix("tnlog_compact_leftover").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Sync, manual());
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    drop(db);
    let leftover = tmp.path().join("compact-1-999-0");
    std::fs::create_dir(&leftover).expect("mkdir");
    std::fs::write(leftover.join("log"), b"partial").expect("write");
    let db = open_compacting(tmp.path(), CommitMode::Sync, manual());
    assert_eq!(compact_dirs(tmp.path()), 0, "the leftover is gone");
    assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("a".to_string()));
}

/// A generation directory without a log is refused, not opened as an empty database.
#[test]
fn test_tnlog_generation_without_a_log_is_refused() {
    let tmp = TempDir::with_prefix("tnlog_gen_no_log").expect("temp dir");
    drop(open_compacting(tmp.path(), CommitMode::Sync, manual()));
    std::fs::create_dir(tmp.path().join("gen-1")).expect("mkdir");
    let err = TnLogDatabase::open(tmp.path()).expect_err("a generation with no log");
    assert!(format!("{err:#}").contains("has no log"), "{err:#}");
}

/// Closing the database cancels a running compaction promptly, and leaves nothing of it.
#[test]
fn test_tnlog_compaction_close_cancels() {
    let tmp = TempDir::with_prefix("tnlog_compact_close").expect("temp dir");
    let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 1 << 20 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    let value = "v".repeat(4096);
    for i in 0..2_048_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    db.compact().expect("request");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while compact_dirs(tmp.path()) == 0 {
        assert!(std::time::Instant::now() < deadline, "the compaction started");
        std::thread::sleep(Duration::from_millis(5));
    }
    let closing = std::time::Instant::now();
    drop(db);
    assert!(closing.elapsed() < Duration::from_secs(2), "closed in {:?}", closing.elapsed());
    assert_eq!(compact_dirs(tmp.path()), 0, "the cancelled compaction left nothing");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert_eq!(crate::tndb::layout::list_gens(tmp.path()).expect("gens"), [0]);
    assert_eq!(db.iter::<TestTable>().count(), 2_048);
}

/// A compaction that fails before its rename (at start, short of disk at start or midway, a
/// panic, a row count that does not match) is abandoned: its files are deleted, the database
/// carries on in its generation, and a later compaction still runs.
#[test]
fn test_tnlog_compaction_failures_before_the_rename_abandon() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tnlog_compact_fail").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
    }
    let hooks = &db.core().compact.test;
    let abandoned = |result: Result<bool, String>, why: &str| {
        let err = result.expect_err(why);
        assert!(err.contains(why), "{err}");
        assert_eq!(compact_dirs(tmp.path()), 0, "{why}: nothing left");
        assert_eq!(crate::tndb::layout::list_gens(tmp.path()).expect("gens"), [0]);
    };
    hooks.fail_next.store(true, Relaxed);
    abandoned(db.compact_now(), "injected");
    *hooks.free_space.lock() = Some(0);
    abandoned(db.compact_now(), "skipped");
    *hooks.free_space.lock() = None;
    let (arrivals, release) = hooks.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(next_checkpoint(&arrivals), 1);
        *hooks.free_space.lock() = Some(1 << 20);
        drop(release);
        abandoned(compaction.join().expect("compaction thread"), "bytes free, below");
    });
    *hooks.free_space.lock() = None;
    hooks.panic_before_rename.store(true, Relaxed);
    abandoned(db.compact_now(), "panicked");
    hooks.skip_a_row.store(true, Relaxed);
    abandoned(db.compact_now(), "rows compacted");
    hooks.fail_rename.store(true, Relaxed);
    abandoned(db.compact_now(), "injected rename failure");
    db.insert::<TestTable>(&100, &"b".to_string()).expect("writes carry on");
    block_on(db.persist::<TestTable>()).expect("persist");
    drop(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert_eq!(db.iter::<TestTable>().count(), 101, "every row, in the old generation");
    assert_eq!(db.compact_now(), Ok(true), "a later compaction runs");
    assert_eq!(db.iter::<TestTable>().count(), 101);
}

/// A switch that fails after its rename (its directory sync, or a panic) stops the writer:
/// which generation the next open picks is unknown. Either one holds every row.
#[test]
fn test_tnlog_compaction_failure_after_the_rename_stops_the_writer() {
    use std::sync::atomic::Ordering::Relaxed;
    for panic in [false, true] {
        let tmp = TempDir::with_prefix("tnlog_compact_after").expect("temp dir");
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
        }
        block_on(db.persist::<TestTable>()).expect("persist");
        let hooks = &db.core().compact.test;
        if panic {
            hooks.panic_after_rename.store(true, Relaxed);
        } else {
            hooks.fail_root_sync.store(true, Relaxed);
        }
        assert!(db.compact_now().is_err(), "panic {panic}: the switch failed");
        assert!(db.insert::<TestTable>(&200, &"x".to_string()).is_err(), "the writer stopped");
        drop(db);
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        assert_eq!(db.iter::<TestTable>().count(), 100, "panic {panic}: every row");
    }
}

/// A `persist` waiting while a sync round of the old generation is paused, and a switch in the
/// middle: the round finishes on the retired generation, latches nothing, and the persist
/// resolves; later writes in the new generation are made durable as usual.
#[test]
fn test_tnlog_compaction_sync_round_across_the_switch() {
    let tmp = TempDir::with_prefix("tnlog_compact_round").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..50_u64 {
        db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
    }
    block_on(db.persist::<TestTable>()).expect("persist");
    let (arrived, release) = db.core().durability.gate_next_sync();
    db.insert::<TestTable>(&100, &"pending".to_string()).expect("insert");
    std::thread::scope(|s| {
        let persist = s.spawn(|| block_on(db.persist::<TestTable>()));
        arrived.recv_timeout(Duration::from_secs(10)).expect("the round paused");
        assert_eq!(db.compact_now(), Ok(true));
        db.insert::<TestTable>(&101, &"new gen".to_string()).expect("insert");
        drop(release);
        persist.join().expect("persist thread").expect("persist across the switch");
    });
    block_on(db.persist::<TestTable>()).expect("persist in the new generation");
    assert!(!db.core().durability.is_failed(), "nothing latched");
    crash(db);
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert_eq!(db.get::<TestTable>(&101).expect("get"), Some("new gen".to_string()));
    assert_eq!(db.iter::<TestTable>().count(), 52);
}
