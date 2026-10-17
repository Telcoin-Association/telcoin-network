//! tndb's tests: the shared `Database` suite in both commit modes, recovery, transaction
//! atomicity, batch rollback, and durability.

use std::{collections::BTreeMap, path::Path, time::Duration};

use tempfile::TempDir;
use tn_types::{Database as _, DbTx as _, DbTxMut as _};

use super::{
    database::{CommitMode, CompactionConfig, TnDatabase, TnDbOptions},
    record::{self, KIND_CLEAR, KIND_PUT},
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

fn options(commit: CommitMode) -> TnDbOptions {
    TnDbOptions { commit, ..Default::default() }
}

/// The database at `path`, both test tables open.
fn open(path: &Path, commit: CommitMode) -> TnDatabase {
    let db = TnDatabase::open_with(path, options(commit)).expect("open tndb");
    db.open_table::<TestTable>().expect("open table");
    db.open_table::<OtherTable>().expect("open table");
    db
}

/// Simulate a crash: stop the commit thread and release the lock, as a dead process's are, and
/// leak the handle so nothing is synced or sealed on the way out.
fn crash(db: TnDatabase) {
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

fn rows<T: tn_types::Table<Key = u64, Value = String>>(db: &TnDatabase) -> Vec<(u64, String)> {
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

/// Append a well-framed record carrying `payload` to the log at `path` with a fresh pack handle,
/// and leave the log unsealed, so the next open replays it.
fn append_unsealed(path: &Path, payload: &[u8]) {
    use crate::archive::{
        data_file::MmapFileOptions,
        pack::{Pack, PackCompression},
    };
    let mut log: Pack<Vec<u8>> =
        Pack::open_with(path, 0, false, PackCompression::None, 1, MmapFileOptions::default())
            .expect("open log");
    log.append_raw(payload).expect("append");
    log.commit().expect("sync");
    log.poison();
}

/// Append a well-framed record carrying `payload` to the cleanly closed log at `path`, leaving it
/// sealed (as damage the writer never makes would).
fn append_sealed(path: &Path, payload: &[u8]) {
    use crate::archive::{
        data_file::MmapFileOptions,
        pack::{Pack, PackCompression},
    };
    let mut log: Pack<Vec<u8>> =
        Pack::open_with(path, 0, false, PackCompression::None, 1, MmapFileOptions::default())
            .expect("open log");
    log.append_raw(payload).expect("append");
    log.commit().expect("sync");
}

/// The commit marker at the tail of the unclean log at `path`, if one is there and valid.
fn commit_marker(path: &Path) -> Option<u64> {
    let bytes = std::fs::read(path).expect("read log");
    let tail = &bytes[bytes.len() - 16..];
    let committed = u64::from_le_bytes(tail[0..8].try_into().expect("8 bytes"));
    let crc = |range: std::ops::Range<usize>| crc32fast::hash(&tail[range]);
    (u32::from_le_bytes(tail[8..12].try_into().expect("4 bytes")) == crc(0..8)
        && u32::from_le_bytes(tail[12..16].try_into().expect("4 bytes")) == crc(8..12))
    .then_some(committed)
}

/// Flip a byte of the file at `path` at offset `at`.
fn flip_byte(path: &Path, at: u64) {
    use std::os::unix::fs::FileExt as _;
    let file = std::fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, at).expect("read");
    byte[0] ^= 0x5a;
    file.write_all_at(&byte, at).expect("write");
}

/// Copy the directory tree at `from` to `to`.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir").flatten() {
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
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
fn test_tndb_shared_suite() {
    for commit in [CommitMode::Sync, CommitMode::Group] {
        let run = |test: fn(TnDatabase)| {
            let tmp = TempDir::with_prefix("tndb_suite").expect("temp dir");
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
fn test_tndb_clean_reopen() {
    let tmp = TempDir::with_prefix("tndb_reopen").expect("temp dir");
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
fn test_tndb_crash_rebuilds_interleaved_tables() {
    let tmp = TempDir::with_prefix("tndb_crash").expect("temp dir");
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
fn test_tndb_txn_atomic_across_tables() {
    let tmp = TempDir::with_prefix("tndb_txn_atomic").expect("temp dir");
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
fn test_tndb_txn_buffered() {
    let tmp = TempDir::with_prefix("tndb_txn_buffered").expect("temp dir");
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
fn test_tndb_clear_inside_a_batch() {
    let tmp = TempDir::with_prefix("tndb_clear_batch").expect("temp dir");
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
fn test_tndb_failed_batch_is_taken_back() {
    let tmp = TempDir::with_prefix("tndb_rollback").expect("temp dir");
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
fn test_tndb_group_persist() {
    let tmp = TempDir::with_prefix("tndb_group").expect("temp dir");
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
fn test_tndb_concurrent_sync_writers() {
    let tmp = TempDir::with_prefix("tndb_threads").expect("temp dir");
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
fn test_tndb_unknown_record_fails_closed() {
    for bad in [record::head(KIND_PUT, 9, true).to_vec(), vec![0x7e, 0, 0]] {
        let tmp = TempDir::with_prefix("tndb_bad_record").expect("temp dir");
        {
            let db = open(tmp.path(), CommitMode::Sync);
            db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
            drop(db);
        }
        append_unsealed(&log_path(tmp.path()), &bad);
        let len = std::fs::metadata(log_path(tmp.path())).expect("log").len();
        assert!(TnDatabase::open(tmp.path()).is_err(), "{bad:?} fails the open");
        assert_eq!(std::fs::metadata(log_path(tmp.path())).expect("log").len(), len);
    }
}

/// A scan that outlives every database handle keeps reading its snapshot (it holds the core, so
/// the files stay open), and the database closes cleanly once the scan is gone.
#[test]
fn test_tndb_scan_outlives_the_handles() {
    let tmp = TempDir::with_prefix("tndb_scan").expect("temp dir");
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
    assert!(TnDatabase::open(tmp.path()).is_err(), "still open while the scan lives");
    drop(scan);
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db).len(), 100);
}

/// A second open of a database is refused while the first holds it.
#[test]
fn test_tndb_second_open_refused() {
    let tmp = TempDir::with_prefix("tndb_lock").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Sync);
    assert!(TnDatabase::open(tmp.path()).is_err());
    drop(db);
    TnDatabase::open(tmp.path()).expect("reopen after close");
}

/// `persist` (group mode) returns within the time a sync takes, not a timeout.
#[test]
fn test_tndb_group_persist_is_prompt() {
    let tmp = TempDir::with_prefix("tndb_prompt").expect("temp dir");
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
fn test_tndb_failed_sync_latches() {
    let tmp = TempDir::with_prefix("tndb_latch").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    db.core().durability.fail_next_sync.store(true, std::sync::atomic::Ordering::Relaxed);
    db.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    assert!(block_on(db.persist::<TestTable>()).is_err(), "the failed sync surfaces");
    assert!(db.insert::<OtherTable>(&3, &"c".to_string()).is_err(), "writes are refused");
    assert!(block_on(db.persist::<OtherTable>()).is_err(), "and it stays latched");
}

/// `failed` stays pending while the database is sound, and resolves promptly with the cause once
/// a failure latches (a failed sync on the commit thread here); one asked for after that resolves
/// at once.
#[test]
fn test_tndb_failed_resolves_once_a_failure_latches() {
    let tmp = TempDir::with_prefix("tndb_failed_future").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    block_on(async {
        let mut failed = std::pin::pin!(db.failed());
        let early = tokio::time::timeout(Duration::from_millis(50), failed.as_mut()).await;
        assert!(early.is_err(), "pending while nothing failed: {early:?}");
        db.core().durability.fail_next_sync.store(true, std::sync::atomic::Ordering::Relaxed);
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        let cause = tokio::time::timeout(Duration::from_secs(5), failed).await.expect("promptly");
        assert!(cause.contains("injected sync failure"), "{cause}");
    });
    let later = block_on(async { tokio::time::timeout(Duration::from_secs(1), db.failed()).await });
    let cause = later.expect("at once");
    assert!(cause.contains("injected sync failure"), "{cause}");
}

/// A batch that panics part-way is taken back (nothing of it is readable or survives a crash)
/// and latches the database.
#[test]
fn test_tndb_panicking_batch_is_taken_back() {
    let tmp = TempDir::with_prefix("tndb_panic").expect("temp dir");
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
fn test_tndb_stale_index_rebuilds_alone() {
    let tmp = TempDir::with_prefix("tndb_stale").expect("temp dir");
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
fn test_tndb_random_crashes_against_model() {
    random_crashes_against_model(0x7E10_6001, false);
}

/// The randomized model of [`test_tndb_random_crashes_against_model`], with half its crashes also
/// losing power: every reopen still finds a state written since the last persist.
#[test]
fn test_tndb_random_power_losses_against_model() {
    random_crashes_against_model(0x7E10_6002, true);
}

/// The log of the current generation under `root`.
fn current_log(root: &Path) -> std::path::PathBuf {
    let gens = super::layout::list_gens(root).expect("gens");
    root.join(format!("gen-{}", gens.last().expect("a generation"))).join("log")
}

/// Random writes, transactions, clears, persists, compactions and crashes (each a power loss too
/// when `power` and a coin says so), checked after every reopen against the states written since
/// the last persist.
fn random_crashes_against_model(seed: u64, power: bool) {
    use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

    use crate::archive::data_file::disk_model;

    type Model = (BTreeMap<u64, String>, BTreeMap<u64, String>);
    let tmp = TempDir::with_prefix("tndb_random").expect("temp dir");
    let mut rng = StdRng::seed_from_u64(seed);
    let mut db = open(tmp.path(), CommitMode::Group);
    if power {
        disk_model::track(&current_log(tmp.path()));
    }
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
                // A compaction changes no state; the model follows the new generation's log.
                if power {
                    disk_model::untrack(&current_log(tmp.path()));
                }
                assert_eq!(db.compact_now(), Ok(true), "step {step}: compacted");
                if power {
                    disk_model::track(&current_log(tmp.path()));
                }
            }
            _ => {
                let lose_power = power && rng.random_bool(0.5);
                crash(db);
                if lose_power {
                    disk_model::power_loss(&current_log(tmp.path()));
                }
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
    if power {
        drop(db);
        disk_model::untrack(&current_log(tmp.path()));
    }
}

/// A batch becomes durable only once the commit thread's sync covering it has finished: while
/// that sync is paused, the watermark stays below the batch and `persist` waits.
#[test]
fn test_tndb_durable_only_after_the_sync() {
    let tmp = TempDir::with_prefix("tndb_durable_after").expect("temp dir");
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
fn test_tndb_sync_writers_sync_themselves() {
    let tmp = TempDir::with_prefix("tndb_leader").expect("temp dir");
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
fn test_tndb_group_syncs_on_demand() {
    let tmp = TempDir::with_prefix("tndb_demand").expect("temp dir");
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
fn test_tndb_persist_does_not_wait_for_the_delay() {
    let tmp = TempDir::with_prefix("tndb_persist_now").expect("temp dir");
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
fn test_tndb_group_unsynced_bytes_are_capped() {
    let tmp = TempDir::with_prefix("tndb_backpressure").expect("temp dir");
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
fn open_compacting(path: &Path, commit: CommitMode, compaction: CompactionConfig) -> TnDatabase {
    let db = TnDatabase::open_with(path, TnDbOptions { commit, compaction }).expect("open");
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
        let gens = super::layout::list_gens(root).expect("list gens");
        if gens == [gen] {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "generations {gens:?}, want only {gen}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The log's length (its end, after the last batch).
fn log_len(db: &TnDatabase) -> u64 {
    db.core().writer.lock().log_len()
}

/// Every table emptied while a compaction runs (an epoch ends mid-compaction, and the next one's
/// first write lands before the switch): the switch keeps the emptied signal, so the reset follows
/// and the copied rows, dead after the catch-up's clears, leave at once.
#[test]
fn test_tndb_emptied_during_a_compaction_still_resets() {
    let tmp = TempDir::with_prefix("tndb_emptied_mid").expect("temp dir");
    // Only the emptied trigger fires automatically.
    let compaction = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, compaction);
    let value = "v".repeat(64 << 10);
    for i in 0..20_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
        db.insert::<OtherTable>(&i, &value).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
        db.clear_table::<TestTable>().expect("clear");
        db.clear_table::<OtherTable>().expect("clear");
        db.insert::<TestTable>(&1_000, &"next epoch".to_string()).expect("insert");
        drop(release);
        assert_eq!(compaction.join().expect("thread"), Ok(true));
    });
    assert!(log_len(&db) > 2 << 20, "the switch copied the cleared rows: {}", log_len(&db));
    // The next batch asks for the reset.
    db.insert::<TestTable>(&2_000, &"x".to_string()).expect("insert");
    assert_eq!(db.core().compact.wait_done(2), Some(Ok(true)), "the reset ran");
    assert!(log_len(&db) < 1 << 20, "the dead rows left: {}", log_len(&db));
    assert_eq!(rows::<TestTable>(&db).len(), 2, "the next epoch's rows");
}

/// An emptied signal a switch kept (the epoch ended during the copy) is dropped by the new
/// generation's reset, even when that reset's catch-up leaves a row dead: the emptying's position
/// was in the old, longer log, and every snapshot of the new generation comes after it. Kept, the
/// signal would start another compaction with each batch.
#[test]
fn test_tndb_kept_emptied_signal_is_dropped_by_the_next_switch() {
    let tmp = TempDir::with_prefix("tndb_emptied_rebased").expect("temp dir");
    // Only the emptied trigger fires automatically.
    let compaction = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, compaction);
    let value = "v".repeat(64 << 10);
    // Overwritten rows make the old log about three times as long as the copy.
    for _ in 0..3 {
        for i in 0..20_u64 {
            db.insert::<TestTable>(&i, &value).expect("insert");
            db.insert::<OtherTable>(&i, &value).expect("insert");
        }
    }
    let compact = &db.core().compact;
    let (arrivals, release) = compact.test.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
        db.clear_table::<TestTable>().expect("clear");
        db.clear_table::<OtherTable>().expect("clear");
        db.insert::<TestTable>(&1_000, &"next epoch".to_string()).expect("insert");
        drop(release);
        assert_eq!(compaction.join().expect("thread"), Ok(true));
    });
    assert!(db.core().emptied(), "the switch keeps the signal");
    // The next batch starts the reset; its catch-up overwrites a row, so the new generation
    // holds a dead row.
    let (arrivals, release) = compact.test.gate_next();
    db.insert::<TestTable>(&2_000, &"x".to_string()).expect("insert");
    assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the reset's copy's end");
    db.insert::<TestTable>(&1_000, &"overwritten".to_string()).expect("overwrite");
    drop(release);
    assert_eq!(compact.wait_done(2), Some(Ok(true)), "the reset ran");
    assert!(db.core().has_dead(), "the catch-up left a dead row");
    assert!(!db.core().emptied(), "the reset's switch drops the signal");
}

/// A second emptying while the first one's reset is still pending (that reset abandoned, and its
/// retry copying the rows written since) moves the signal on: the retry's switch keeps it, so the
/// rows the second emptying cleared (live when copied, dead after the catch-up) are reset too.
#[test]
fn test_tndb_second_emptying_during_a_retried_reset_still_resets() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tndb_emptied_twice").expect("temp dir");
    // Only the emptied trigger fires automatically.
    let compaction = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, compaction);
    let value = "v".repeat(64 << 10);
    let fill = |from: u64| {
        for i in from..from + 20 {
            db.insert::<TestTable>(&i, &value).expect("insert");
            db.insert::<OtherTable>(&i, &value).expect("insert");
        }
    };
    let empty = || {
        db.clear_table::<TestTable>().expect("clear");
        db.clear_table::<OtherTable>().expect("clear");
    };
    let compact = &db.core().compact;
    // An epoch ends; its reset is abandoned, and backs off.
    fill(0);
    compact.test.fail_next.store(true, Relaxed);
    empty();
    assert!(compact.wait_done(1).expect("the reset ended").is_err(), "abandoned");
    // The next epoch's rows (each batch asks for the reset again, which waits out the backoff).
    fill(100);
    // The retry copies them, and the next epoch ends before its switch.
    let (arrivals, release) = compact.test.gate_next();
    std::thread::scope(|s| {
        let retry = s.spawn(|| db.compact_now());
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
        empty();
        db.insert::<TestTable>(&1_000, &"next epoch".to_string()).expect("insert");
        drop(release);
        assert_eq!(retry.join().expect("thread"), Ok(true));
    });
    assert!(log_len(&db) > 2 << 20, "the switch copied the cleared rows: {}", log_len(&db));
    // The next batch asks for the reset.
    db.insert::<TestTable>(&2_000, &"x".to_string()).expect("insert");
    assert_eq!(compact.wait_done(3), Some(Ok(true)), "the reset ran");
    assert!(log_len(&db) < 1 << 20, "the dead rows left: {}", log_len(&db));
    assert_eq!(rows::<TestTable>(&db).len(), 2, "the next epoch's rows");
}

/// An emptying after a reset's snapshot that copied nothing (every table was empty already) leaves
/// the new generation nothing dead: the switch drops the signal rather than reset again.
#[test]
fn test_tndb_emptying_after_an_empty_snapshot_needs_no_second_reset() {
    let tmp = TempDir::with_prefix("tndb_emptied_empty").expect("temp dir");
    let compaction = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, compaction);
    let value = "v".repeat(64 << 10);
    for i in 0..20_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let compact = &db.core().compact;
    let (arrivals, release) = compact.test.gate_next();
    // The epoch ends: the reset starts, and copies nothing.
    db.clear_table::<TestTable>().expect("clear");
    assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
    // Another batch leaves every table empty before the switch.
    db.clear_table::<TestTable>().expect("clear again");
    drop(release);
    assert_eq!(compact.wait_done(1), Some(Ok(true)), "the reset ran");
    assert!(log_len(&db) < 1 << 20, "the reset: {}", log_len(&db));
    assert!(!db.core().emptied(), "nothing dead is left to reset");
}

/// A compaction's catch-up that removes a row the new log never held (written and removed after
/// the snapshot) writes nothing and leaves nothing dead in the new generation.
#[test]
fn test_tndb_compaction_removal_of_an_uncopied_row_leaves_nothing_dead() {
    let tmp = TempDir::with_prefix("tndb_compact_uncopied").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
        db.insert::<TestTable>(&500, &"short-lived".to_string()).expect("insert");
        db.remove::<TestTable>(&500).expect("remove");
        drop(release);
        assert_eq!(compaction.join().expect("thread"), Ok(true));
    });
    assert!(!db.core().has_dead(), "the new generation holds only live rows");
    assert_eq!(rows::<TestTable>(&db).len(), 100);
}

type Tables3 = [BTreeMap<u64, String>; 3];

fn insert_into(db: &TnDatabase, table: usize, k: u64, v: &String) {
    match table {
        0 => db.insert::<TestTable>(&k, v),
        1 => db.insert::<OtherTable>(&k, v),
        _ => db.insert::<ThirdTable>(&k, v),
    }
    .expect("insert");
}

fn all_rows(db: &TnDatabase) -> Tables3 {
    [
        db.iter::<TestTable>().collect(),
        db.iter::<OtherTable>().collect(),
        db.iter::<ThirdTable>().collect(),
    ]
}

/// `ops` random writes over the three tables (puts, overwrites, removes, transactions across
/// tables, a rare clear), applied to `model` too.
fn churn(
    db: &TnDatabase,
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
fn test_tndb_compaction_preserves_rows() {
    use rand::SeedableRng as _;
    let tmp = TempDir::with_prefix("tndb_compact_rows").expect("temp dir");
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
fn test_tndb_compaction_returns_space() {
    let tmp = TempDir::with_prefix("tndb_compact_space").expect("temp dir");
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
fn test_tndb_compaction_trigger() {
    let tmp = TempDir::with_prefix("tndb_compact_trigger").expect("temp dir");
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
fn test_tndb_compaction_first_rows_of_an_unfixed_table() {
    let tmp = TempDir::with_prefix("tndb_compact_unfixed").expect("temp dir");
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
fn test_tndb_compaction_copied_rows_are_whole_batches() {
    let tmp = TempDir::with_prefix("tndb_compact_batches").expect("temp dir");
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
fn test_tndb_compaction_table_opened_and_cleared_during() {
    let tmp = TempDir::with_prefix("tndb_compact_during").expect("temp dir");
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
    let keys = |db: &TnDatabase| db.iter::<TestTable>().map(|(k, _)| k).collect::<Vec<_>>();
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
fn test_tndb_compaction_scan_across_the_switch() {
    let tmp = TempDir::with_prefix("tndb_compact_scan").expect("temp dir");
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
fn test_tndb_compaction_switch_holds_the_registry() {
    let tmp = TempDir::with_prefix("tndb_compact_registry").expect("temp dir");
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
fn test_tndb_compaction_switch_tail_is_bounded() {
    let tmp = TempDir::with_prefix("tndb_compact_tail").expect("temp dir");
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
fn test_tndb_compaction_crash_after_the_switch() {
    let tmp = TempDir::with_prefix("tndb_compact_crash").expect("temp dir");
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
fn test_tndb_compaction_leftover_is_deleted_at_open() {
    let tmp = TempDir::with_prefix("tndb_compact_leftover").expect("temp dir");
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
fn test_tndb_generation_without_a_log_is_refused() {
    let tmp = TempDir::with_prefix("tndb_gen_no_log").expect("temp dir");
    drop(open_compacting(tmp.path(), CommitMode::Sync, manual()));
    std::fs::create_dir(tmp.path().join("gen-1")).expect("mkdir");
    let err = TnDatabase::open(tmp.path()).expect_err("a generation with no log");
    assert!(format!("{err:#}").contains("has no log"), "{err:#}");
}

/// Closing the database cancels a running compaction promptly, and leaves nothing of it.
#[test]
fn test_tndb_compaction_close_cancels() {
    let tmp = TempDir::with_prefix("tndb_compact_close").expect("temp dir");
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
    assert_eq!(super::layout::list_gens(tmp.path()).expect("gens"), [0]);
    assert_eq!(db.iter::<TestTable>().count(), 2_048);
}

/// A compaction that fails before its rename (at start, short of disk at start or midway, a
/// panic, a row count that does not match) is abandoned: its files are deleted, the database
/// carries on in its generation, and a later compaction still runs.
/// A write transaction dropped while a panic unwinds writes nothing (it was cut off part way); one
/// dropped without a commit otherwise is written.
#[test]
fn test_tndb_txn_dropped_while_panicking_writes_nothing() {
    let tmp = TempDir::with_prefix("tndb_txn_panic").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&1, &"half".to_string()).expect("insert");
        panic!("cut off before the rest of the transaction");
    }));
    assert!(panicked.is_err());
    assert_eq!(db.get::<TestTable>(&1).expect("get"), None, "nothing of the cut-off transaction");
    {
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&2, &"dropped".to_string()).expect("insert");
    }
    assert_eq!(db.get::<TestTable>(&2).expect("get"), Some("dropped".to_string()));
}

/// An automatic request that saw no compaction running, but takes the state lock only once one has
/// started, does not ask for another: when that one is abandoned, the next waits out the backoff
/// rather than starting at once.
#[test]
fn test_tndb_abandoned_compaction_waits_out_its_backoff() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tndb_compact_backoff").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
    }
    let compact = &db.core().compact;
    let (paused, unpause) = compact.test.pause_next_request();
    std::thread::scope(|s| {
        // An automatic request, past its lock-free check while no compaction runs.
        let request = s.spawn(|| compact.request_auto());
        assert_eq!(next_checkpoint(&paused), 0);
        // A compaction starts, and will be abandoned (its row count is wrong).
        let (arrivals, release) = compact.test.gate_next();
        compact.test.skip_a_row.store(true, Relaxed);
        compact.test.force.store(true, Relaxed);
        db.compact().expect("request");
        assert_eq!(next_checkpoint(&arrivals), 1);
        drop(unpause);
        request.join().expect("request thread");
        drop(release);
    });
    let abandoned = compact.wait_done(1).expect("the compaction ended");
    assert!(abandoned.expect_err("abandoned").contains("rows compacted"));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(compact.done(), 1, "no compaction before the backoff");
}

#[test]
fn test_tndb_compaction_failures_before_the_rename_abandon() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tndb_compact_fail").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..100_u64 {
        db.insert::<TestTable>(&i, &format!("a{i}")).expect("insert");
    }
    let hooks = &db.core().compact.test;
    let abandoned = |result: Result<bool, String>, why: &str| {
        let err = result.expect_err(why);
        assert!(err.contains(why), "{err}");
        assert_eq!(compact_dirs(tmp.path()), 0, "{why}: nothing left");
        assert_eq!(super::layout::list_gens(tmp.path()).expect("gens"), [0]);
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
fn test_tndb_compaction_failure_after_the_rename_stops_the_writer() {
    use std::sync::atomic::Ordering::Relaxed;
    for panic in [false, true] {
        let tmp = TempDir::with_prefix("tndb_compact_after").expect("temp dir");
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
fn test_tndb_compaction_sync_round_across_the_switch() {
    let tmp = TempDir::with_prefix("tndb_compact_round").expect("temp dir");
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

/// A writer that keeps outrunning the catch-up (more than a round's worth lands during every
/// round) no longer makes the compaction give up: once the rounds run out it goes to the switch,
/// which takes the rest under the write lock, and every row is kept.
#[test]
fn test_tndb_compaction_completes_when_the_catch_up_never_converges() {
    let tmp = TempDir::with_prefix("tndb_compact_outrun_rounds").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(64 << 10);
    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    let mut next = 1_000_u64;
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        loop {
            let n = next_checkpoint(&arrivals);
            if n == 3 {
                break;
            }
            // Over a round's worth (1 MiB), so no round ever counts as caught up.
            for _ in 0..20 {
                db.insert::<TestTable>(&next, &value).expect("insert");
                next += 1;
            }
            release.send(()).expect("release");
        }
        drop(release);
        assert_eq!(compaction.join().expect("compaction thread"), Ok(true));
    });
    assert_eq!(db.iter::<TestTable>().count() as u64, 10 + (next - 1_000));
}

/// A writer that lands more than the switch's usual cap before every try no longer makes the
/// compaction give up: the last try takes the tail under the write lock (a bounded stall), and
/// every row is kept.
#[test]
fn test_tndb_compaction_last_switch_takes_a_large_tail() {
    let tmp = TempDir::with_prefix("tndb_compact_outrun_switch").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(64 << 10);
    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let (arrivals, release) = db.core().compact.test.gate_next();
    let mut next = 1_000_u64;
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        for n in [1, 2] {
            assert_eq!(next_checkpoint(&arrivals), n);
            release.send(()).expect("release");
        }
        // Before every switch try, a tail over the usual 4 MiB cap.
        while let Ok(n) = arrivals.recv_timeout(Duration::from_secs(30)) {
            assert_eq!(n, 3, "only switch tries remain");
            for _ in 0..80 {
                db.insert::<TestTable>(&next, &value).expect("a 5 MiB tail");
                next += 1;
            }
            if release.send(()).is_err() {
                break;
            }
        }
        assert_eq!(compaction.join().expect("compaction thread"), Ok(true));
    });
    let retries = db.core().compact.test.switch_retries.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(retries, 2, "the usual cap twice, then the bounded stall");
    assert_eq!(db.iter::<TestTable>().count() as u64, 10 + (next - 1_000));
}

/// When every table of a log has been emptied (the epoch log at an epoch's end clears all of its
/// tables), the log compacts at once, whatever its growth threshold: nothing is live, so the
/// compaction is a near-free reset that returns the disk. Clearing only some tables does not.
#[test]
fn test_tndb_compaction_when_every_table_is_emptied() {
    let tmp = TempDir::with_prefix("tndb_compact_emptied").expect("temp dir");
    let config = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    let value = "v".repeat(64 << 10);
    for i in 0..20_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
        db.insert::<OtherTable>(&i, &value).expect("insert");
    }
    db.clear_table::<TestTable>().expect("clear");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(db.core().compact.done(), 0, "a table still holds rows: no compaction");
    db.clear_table::<OtherTable>().expect("clear");
    assert_eq!(db.core().compact.wait_done(1), Some(Ok(true)), "every table emptied: compacted");
    wait_only_gen(tmp.path(), 1);
    assert!(log_len(&db) < 64 << 10, "nothing live was copied: {} bytes", log_len(&db));
}

/// With compaction off, emptying every table compacts nothing.
#[test]
fn test_tndb_no_compaction_when_emptied_with_compaction_off() {
    let tmp = TempDir::with_prefix("tndb_compact_emptied_off").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(64 << 10);
    for i in 0..20_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    db.clear_table::<TestTable>().expect("clear");
    db.clear_table::<OtherTable>().expect("clear");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(db.core().compact.done(), 0);
}

/// A compaction starts writing its new log back as it goes (every MiB), so its output never piles
/// up unflushed (on Linux, dirty pages throttle every writer); its round syncs stay the durability
/// points. The counter shows the calls; whether pages were written back is not observable here.
#[test]
fn test_tndb_compaction_writes_back_as_it_goes() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tndb_compact_writeback").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let value = "v".repeat(64 << 10);
    for i in 0..48_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let before = super::stats::WRITEBACKS.load(Relaxed);
    assert_eq!(db.compact_now(), Ok(true));
    let started = super::stats::WRITEBACKS.load(Relaxed) - before;
    assert!(started >= 2, "3 MiB copied: writeback started {started} times");
}

/// A close (or a crash) between emptying every table and the compaction that follows loses no
/// reset: the next open notes the emptied log, and its first batch asks for the compaction.
#[test]
fn test_tndb_compaction_after_reopening_an_emptied_log() {
    let tmp = TempDir::with_prefix("tndb_compact_emptied_reopen").expect("temp dir");
    let value = "v".repeat(64 << 10);
    {
        // Compaction off: the clears leave the log as it is.
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        for i in 0..20_u64 {
            db.insert::<TestTable>(&i, &value).expect("insert");
            db.insert::<OtherTable>(&i, &value).expect("insert");
        }
        db.clear_table::<TestTable>().expect("clear");
        db.clear_table::<OtherTable>().expect("clear");
    }
    let config = CompactionConfig { auto_min_bytes: Some(1 << 40), bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    db.insert::<TestTable>(&1, &"new epoch".to_string()).expect("insert");
    assert_eq!(db.core().compact.wait_done(1), Some(Ok(true)), "the reopened emptied log resets");
    wait_only_gen(tmp.path(), 1);
    assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("new epoch".to_string()));
}

/// An open keeps an older generation until the newest one has opened: after a crash right after
/// a switch (the rename done, the old generation not yet deleted), the old one is the only other
/// copy if the newest fails to replay. No fallback to it: the newest may hold acked batches.
#[test]
fn test_tndb_failed_open_keeps_the_older_generation() {
    let tmp = TempDir::with_prefix("tndb_keep_old_gen").expect("temp dir");
    let saved = TempDir::with_prefix("tndb_keep_old_gen_saved").expect("temp dir");
    let root = tmp.path();
    {
        let db = open_compacting(root, CommitMode::Sync, manual());
        for i in 0..10_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.remove::<TestTable>(&0).expect("remove");
    }
    copy_dir(&root.join("gen-0"), saved.path());
    {
        let db = open_compacting(root, CommitMode::Sync, manual());
        assert_eq!(db.compact_now(), Ok(true));
    }
    wait_only_gen(root, 1);

    // Both generations whole: the open takes the newest and deletes the older one.
    copy_dir(saved.path(), &root.join("gen-0"));
    {
        let db = open_compacting(root, CommitMode::Sync, manual());
        assert_eq!(rows::<TestTable>(&db).len(), 9);
    }
    wait_only_gen(root, 1);

    // The newest fails to replay: the open fails, and the older generation is still there.
    copy_dir(saved.path(), &root.join("gen-0"));
    append_unsealed(&root.join("gen-1").join("log"), &record::head(KIND_PUT, 9, true));
    assert!(TnDatabase::open(root).is_err(), "the damaged newest generation fails the open");
    assert!(root.join("gen-0").join("log").exists(), "the older generation is kept");
}

/// The free-space guard also runs during the copy, the longest phase (once per MiB): a disk that
/// fills meanwhile abandons the compaction before its copy ends, not after it.
#[test]
fn test_tndb_compaction_copy_gives_way_to_a_full_disk() {
    let tmp = TempDir::with_prefix("tndb_compact_copy_room").expect("temp dir");
    // Paced at 1 MiB/s: the 4 MiB copy takes about 4 s.
    let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 1 << 20 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    let value = "v".repeat(64 << 10);
    for i in 0..64_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let hooks = &db.core().compact.test;
    let (arrivals, release) = hooks.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        std::thread::sleep(Duration::from_millis(300));
        *hooks.free_space.lock() = Some(1 << 20);
        // The gate's sender drops when the compaction ends; checkpoint 1 is the copy's end.
        let reached = arrivals.recv_timeout(Duration::from_secs(20));
        drop(release);
        let result = compaction.join().expect("compaction thread");
        assert!(reached.is_err(), "the copy ran to its end on a full disk: {reached:?}");
        let err = result.expect_err("abandoned");
        assert!(err.contains("bytes free, below"), "{err}");
    });
    *hooks.free_space.lock() = None;
    assert_eq!(compact_dirs(tmp.path()), 0, "nothing left");
    assert_eq!(db.iter::<TestTable>().count(), 64);
}

/// A catch-up round checks for room on the disk as it copies, as the copy does, not only before
/// it starts: the first round replays everything written during the copy, and a disk filling
/// meanwhile must abandon the compaction before the writer's growth finds it full.
#[test]
fn test_tndb_compaction_catch_up_checks_the_disk_as_it_goes() {
    let tmp = TempDir::with_prefix("tndb_compact_catch_up_room").expect("temp dir");
    let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 };
    let db = open_compacting(tmp.path(), CommitMode::Group, config);
    let value = "v".repeat(64 << 10);
    for i in 0..16_u64 {
        db.insert::<TestTable>(&i, &value).expect("insert");
    }
    let hooks = &db.core().compact.test;
    let (arrivals, release) = hooks.gate_next();
    std::thread::scope(|s| {
        let compaction = s.spawn(|| db.compact_now());
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(1), "the copy's end");
        // 4 MiB written while the copy ran: the first catch-up round copies it.
        for i in 100..164_u64 {
            db.insert::<TestTable>(&i, &value).expect("insert");
        }
        let before = hooks.room_checks.load(std::sync::atomic::Ordering::Relaxed);
        release.send(()).expect("release the copy's end");
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(20)), Ok(2), "the round's end");
        let checks = hooks.room_checks.load(std::sync::atomic::Ordering::Relaxed) - before;
        assert!(checks >= 4, "a 4 MiB round checked the disk {checks} times");
        drop(release);
        compaction.join().expect("compaction thread").expect("compacted");
    });
    assert_eq!(db.iter::<TestTable>().count(), 80);
}

/// Zero the CRC trailer of every page of each index in generation directory `gen` but its header
/// and root: at-rest damage the index reports as `CorruptIndex` on a read through those pages
/// (the open checks only the header and the root).
fn damage_index_below_the_root(gen: &Path) {
    use crate::archive::btree_index::{header::BtreeHeader, page::PAGE_SIZE};
    let mut damaged = 0;
    for entry in std::fs::read_dir(gen).expect("read gen").flatten() {
        if !entry.file_name().to_string_lossy().starts_with("idx-") {
            continue;
        }
        let file = entry.path().join("index.btx");
        let mut bytes = std::fs::read(&file).expect("read index");
        let header = BtreeHeader::from_page(&bytes[..PAGE_SIZE]).expect("index header");
        for page in (1..header.page_count).filter(|&p| p != header.root_page) {
            let end = (page as usize + 1) * PAGE_SIZE;
            bytes[end - 4..end].fill(0);
            damaged += 1;
        }
        std::fs::write(&file, bytes).expect("write index");
    }
    assert!(damaged > 0, "an index with pages below its root");
}

/// An index damaged at rest below its root (its open-time walk reaches a damaged page) is
/// refused at open and rebuilt from the log: every row reads back and a removal works.
#[test]
fn test_tndb_damaged_index_is_rebuilt_at_open() {
    let tmp = TempDir::with_prefix("tndb_rebuild_damaged").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..2_000_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
    }
    damage_index_below_the_root(&tmp.path().join("gen-0"));
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.iter::<TestTable>().count(), 2_000, "every row, through the rebuilt index");
    assert_eq!(db.get::<TestTable>(&1_000).expect("get"), Some("v1000".to_string()));
    db.remove::<TestTable>(&1_000).expect("a removal through the rebuilt index");
    assert_eq!(db.get::<TestTable>(&1_000).expect("get"), None);
}

/// Zero the CRC trailer of every leaf of each index in generation directory `gen` but the
/// leftmost: damage the open-time walk (internal pages and the leftmost leaf) does not reach, so
/// the open trusts the index and the damage is met by the reads and writes that reach it.
fn damage_index_leaves_past_the_first(gen: &Path) {
    use crate::archive::btree_index::{
        header::BtreeHeader,
        page::{Node, PAGE_SIZE},
    };
    let mut damaged = 0;
    for entry in std::fs::read_dir(gen).expect("read gen").flatten() {
        if !entry.file_name().to_string_lossy().starts_with("idx-") {
            continue;
        }
        let file = entry.path().join("index.btx");
        let mut bytes = std::fs::read(&file).expect("read index");
        let header = BtreeHeader::from_page(&bytes[..PAGE_SIZE]).expect("index header");
        let node = Node::new(header.ksize as usize);
        let page = |p: u32| p as usize * PAGE_SIZE..(p as usize + 1) * PAGE_SIZE;
        let mut leftmost = header.root_page;
        for _ in 1..header.height {
            leftmost = node.internal_child(&bytes[page(leftmost)], 0);
        }
        for p in (1..header.page_count).filter(|&p| p != leftmost) {
            if node.is_leaf(&bytes[page(p)]) {
                let end = page(p).end;
                bytes[end - 4..end].fill(0);
                damaged += 1;
            }
        }
        std::fs::write(&file, bytes).expect("write index");
    }
    assert!(damaged > 0, "an index with leaves past its first");
}

/// A removal whose key the index cannot read (a damaged page the open trusted) fails: counted as
/// "absent", it would write nothing, and the row would silently survive its removal. The damage
/// latches as a read's does: nothing of the batch was appended, so the close still seals the log
/// (with every index stale), and the next open replays it to rebuild every index, keeping every
/// acknowledged row, the session's own write included.
#[test]
fn test_tndb_remove_through_a_damaged_index_fails() {
    let tmp = TempDir::with_prefix("tndb_remove_damaged").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..2_000_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.insert::<OtherTable>(&1, &"o".to_string()).expect("insert");
    }
    damage_index_leaves_past_the_first(&tmp.path().join("gen-0"));
    {
        let db = open(tmp.path(), CommitMode::Group);
        db.insert::<OtherTable>(&2, &"o2".to_string()).expect("insert");
        block_on(db.persist::<OtherTable>()).expect("acknowledged");
        let removed = db.remove::<TestTable>(&1_000);
        assert!(removed.is_err(), "a removal through a damaged index: {removed:?}");
        // The database stopped: no table takes a write, and no persist acknowledges.
        let other = db.insert::<OtherTable>(&3, &"o".to_string());
        assert!(other.is_err(), "a write after the damage: {other:?}");
        assert!(block_on(db.persist::<OtherTable>()).is_err(), "a persist after the damage");
    }
    let log = super::store::open_log_read_only(&tmp.path().join("gen-0")).expect("open the log");
    assert!(!log.opened_unclean(), "the close sealed the log");
    drop(log);
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.core().rebuilt_on_open().len(), 2, "every index rebuilt from the sealed log");
    assert_eq!(db.get::<TestTable>(&1_000).expect("get"), Some("v1000".to_string()));
    assert_eq!(db.iter::<TestTable>().count(), 2_000, "the rebuilt index holds every row");
    assert_eq!(db.get::<OtherTable>(&2).expect("get"), Some("o2".to_string()), "acknowledged");
    assert_eq!(db.iter::<OtherTable>().count(), 2, "no row past the damage");
    db.remove::<TestTable>(&1_000).expect("a removal through the rebuilt index");
}

/// A scan that meets index damage cannot report it (a `DBIter` ends), so its shorter answer would
/// pass for a whole one: it stops the database instead, and the next open rebuilds the index.
#[test]
fn test_tndb_scan_through_a_damaged_index_stops_the_database() {
    let tmp = TempDir::with_prefix("tndb_scan_damaged").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..2_000_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
    }
    damage_index_leaves_past_the_first(&tmp.path().join("gen-0"));
    {
        let db = open(tmp.path(), CommitMode::Group);
        assert!(db.iter::<TestTable>().count() < 2_000, "the scan ended at the damage");
        let write = db.insert::<OtherTable>(&1, &"o".to_string());
        assert!(write.is_err(), "the short scan stopped the database: {write:?}");
    }
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.iter::<TestTable>().count(), 2_000, "the rebuilt index holds every row");
    db.insert::<OtherTable>(&1, &"o".to_string()).expect("writes resume after the rebuild");
}

/// Which read finds the damage in [`test_tndb_read_damage_in_the_log_refuses_the_next_open`].
#[derive(Clone, Copy, Debug)]
enum DamageRead {
    Get,
    Scan,
}

/// A read that finds a damaged log record (a failed CRC) stops the database, and its close still
/// seals the log, with every index stale: the next open replays the sealed log, fails naming the
/// damage, changes no byte, and fails again on every retry. Left unsealed, the replay would take
/// the damaged record for a crash's torn tail and cut every acknowledged batch after it, unless a
/// write in the session happened to stamp a commit marker past it. Checked with no write in the
/// session, with one acknowledged write (no marker stamped), with two (a marker stamped), and with
/// a scan finding the damage instead of a get.
#[test]
fn test_tndb_read_damage_in_the_log_refuses_the_next_open() {
    let cases =
        [(0, DamageRead::Get), (1, DamageRead::Get), (2, DamageRead::Get), (0, DamageRead::Scan)];
    for (writes, read) in cases {
        let tmp = TempDir::with_prefix("tndb_read_damage_log").expect("temp dir");
        {
            let db = open(tmp.path(), CommitMode::Group);
            for i in 0..200_u64 {
                db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
                block_on(db.persist::<TestTable>()).expect("acknowledged");
            }
        }
        let log = log_path(tmp.path());
        let ends = record_ends(&log);
        assert_eq!(ends.len(), 200, "one record per batch");
        // Row 10's record, which starts where row 9's ends.
        let damaged = ends[9];
        flip_byte(&log, damaged + 6);
        {
            let db = open(tmp.path(), CommitMode::Group);
            for i in 0..writes {
                db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
                block_on(db.persist::<OtherTable>()).expect("acknowledged");
            }
            match read {
                DamageRead::Get => {
                    let got = db.get::<TestTable>(&10);
                    assert!(got.is_err(), "a get of the damaged record: {got:?}");
                }
                DamageRead::Scan => {
                    assert_eq!(db.iter::<TestTable>().count(), 10, "the scan ended at the damage");
                }
            }
            let write = db.insert::<OtherTable>(&100, &"late".to_string());
            assert!(write.is_err(), "the damage stopped the database: {write:?}");
            assert!(block_on(db.persist::<OtherTable>()).is_err(), "and every persist");
        }
        let before = std::fs::read(&log).expect("read");
        for attempt in 0..3 {
            let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
                .err()
                .unwrap_or_else(|| panic!("attempt {attempt} opened ({writes} writes, {read:?})"));
            let err = format!("{err:#}");
            assert!(
                err.contains("closed cleanly")
                    && err.contains(&format!("a bad record at {damaged}")),
                "{err}"
            );
        }
        assert_eq!(
            std::fs::read(&log).expect("read"),
            before,
            "byte-identical after the retries ({writes} writes, {read:?})"
        );
    }
}

/// A get that finds a damaged index page stops the database the same way, without poisoning the
/// log: the close seals it, and the next open replays it (intact here) to rebuild every index.
/// Every row is back, and writes resume.
#[test]
fn test_tndb_read_damage_in_an_index_rebuilds_at_the_next_open() {
    let tmp = TempDir::with_prefix("tndb_read_damage_index").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..2_000_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.insert::<OtherTable>(&1, &"o".to_string()).expect("insert");
    }
    damage_index_leaves_past_the_first(&tmp.path().join("gen-0"));
    {
        let db = open(tmp.path(), CommitMode::Group);
        let got = db.get::<TestTable>(&1_000);
        assert!(got.is_err(), "a get through a damaged leaf: {got:?}");
        let write = db.insert::<OtherTable>(&2, &"o".to_string());
        assert!(write.is_err(), "the damage stopped the database: {write:?}");
    }
    let log = super::store::open_log_read_only(&tmp.path().join("gen-0")).expect("open the log");
    assert!(!log.opened_unclean(), "the close sealed the log");
    drop(log);
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.core().rebuilt_on_open().len(), 2, "every index rebuilt from the sealed log");
    assert_eq!(db.iter::<TestTable>().count(), 2_000, "every row");
    assert_eq!(db.get::<TestTable>(&1_000).expect("get"), Some("v1000".to_string()));
    db.insert::<OtherTable>(&2, &"o".to_string()).expect("writes resume after the rebuild");
}

/// A sync failure after a read's damage still leaves the log's state on disk unknown: the close
/// does not seal it, although the read's damage latched first (and so names the failure).
#[test]
fn test_tndb_failed_sync_after_read_damage_leaves_the_log_unsealed() {
    let tmp = TempDir::with_prefix("tndb_read_damage_sync").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..2_000_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
    }
    damage_index_leaves_past_the_first(&tmp.path().join("gen-0"));
    {
        let db = open(tmp.path(), CommitMode::Group);
        assert!(db.get::<TestTable>(&1_000).is_err(), "a get through a damaged leaf");
        // A sync round already running when the get latched fails.
        db.fail_durability_for_test();
    }
    let log = super::store::open_log_read_only(&tmp.path().join("gen-0")).expect("open the log");
    assert!(log.opened_unclean(), "the close left the log unsealed");
    drop(log);
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.iter::<TestTable>().count(), 2_000, "the replay rebuilt every row");
}

/// A read the disk fails during an unclean open's replay is an error, never a torn tail: the open
/// fails and cuts nothing, whether the read falls in the last acknowledged batch (which the commit
/// marker never covers after a crash) or anywhere once the marker is lost (a power loss). Once the
/// disk reads again, the next open keeps every acknowledged row.
#[test]
fn test_tndb_replay_read_error_fails_the_open() {
    use crate::archive::pack_iter::FAIL_READ_AT;
    for lose_marker in [false, true] {
        let tmp = TempDir::with_prefix("tndb_replay_eio").expect("temp dir");
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            block_on(db.persist::<TestTable>()).expect("acknowledged");
        }
        crash(db);
        let log = log_path(tmp.path());
        let ends = record_ends(&log);
        assert_eq!(ends.len(), 100, "one record per batch");
        if lose_marker {
            let len = std::fs::metadata(&log).expect("log").len();
            use std::os::unix::fs::FileExt as _;
            let file = std::fs::OpenOptions::new().write(true).open(&log).expect("open");
            file.write_all_at(&[0_u8; 16], len - 16).expect("zero the marker");
        } else {
            assert_eq!(commit_marker(&log), Some(ends[98]), "the marker misses the last batch");
        }
        // The last batch's first byte, or (with the marker lost) the middle batch's.
        let at = if lose_marker { ends[49] } else { ends[98] };
        let before = std::fs::read(&log).expect("read");
        FAIL_READ_AT.with(|fail| fail.set(Some(at)));
        let opened = TnDatabase::open_with(tmp.path(), options(CommitMode::Group));
        FAIL_READ_AT.with(|fail| fail.set(None));
        let err = opened.expect_err("a read error fails the open");
        assert!(format!("{err:#}").contains("read error"), "{err:#}");
        assert_eq!(std::fs::read(&log).expect("read"), before, "the failed open cut nothing");
        let db = open(tmp.path(), CommitMode::Group);
        assert_eq!(
            db.iter::<TestTable>().count(),
            100,
            "every acknowledged row (marker lost: {lose_marker})"
        );
    }
}

/// An unclean log damaged below its commit marker fails the open, changes no byte, and stays
/// refused on a retry: the cut happens only after a replay that passed every check.
#[test]
fn test_tndb_tear_below_the_commit_marker_fails_the_open() {
    let tmp = TempDir::with_prefix("tndb_below_marker").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    for round in 0..20_u64 {
        for i in 0..50_u64 {
            db.insert::<TestTable>(&(round * 1_000 + i), &format!("v{round}-{i}")).expect("insert");
        }
        block_on(db.persist::<TestTable>()).expect("persist");
    }
    // One more write stamps the marker up to the last sync.
    db.insert::<TestTable>(&999_999, &"last".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    crash(db);
    let log = log_path(tmp.path());
    let committed = commit_marker(&log).expect("a valid commit marker at the tail");
    let ends = record_ends(&log);
    let middle = ends[ends.len() / 2];
    assert!(middle < committed, "damage well below the marker ({middle} < {committed})");
    flip_byte(&log, middle + 6);
    let before = std::fs::read(&log).expect("read");
    for attempt in 0..2 {
        let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
            .err()
            .unwrap_or_else(|| panic!("attempt {attempt} opened"));
        assert!(format!("{err:#}").contains("below its commit marker"), "{err:#}");
    }
    assert_eq!(std::fs::read(&log).expect("read"), before, "the failed opens changed nothing");
}

/// A cleanly closed log that does not replay to its end (a damaged record, here with its index
/// gone so it must replay) fails the open, changes no byte, and stays refused on every retry: a
/// failed open re-seals it rather than leave it to be cut as a crash's tail.
#[test]
fn test_tndb_damaged_sealed_log_stays_refused() {
    let tmp = TempDir::with_prefix("tndb_sealed_damage").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..500_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
    }
    std::fs::remove_dir_all(tmp.path().join("gen-0").join("idx-0")).expect("remove the index");
    let log = log_path(tmp.path());
    let ends = record_ends(&log);
    flip_byte(&log, ends[ends.len() / 2] + 6);
    let before = std::fs::read(&log).expect("read");
    for attempt in 0..3 {
        let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
            .err()
            .unwrap_or_else(|| panic!("attempt {attempt} opened"));
        assert!(format!("{err:#}").contains("closed cleanly"), "{err:#}");
    }
    assert_eq!(std::fs::read(&log).expect("read"), before, "byte-identical after the retries");
}

/// A cleanly closed log must replay whole, in whole batches: one whose last record does not end
/// its batch (damage: the writer never closes inside one) fails the open rather than be cut.
#[test]
fn test_tndb_sealed_log_ending_inside_a_batch_is_refused() {
    let tmp = TempDir::with_prefix("tndb_sealed_partial").expect("temp dir");
    let id = {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..10_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.core().table(<TestTable as tn_types::Table>::NAME).expect("open").id
    };
    // The log is longer than the indexes cover now, so the open replays it.
    append_sealed(&log_path(tmp.path()), &record::head(KIND_CLEAR, id, false));
    let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
        .expect_err("a sealed log ending inside a batch");
    assert!(format!("{err:#}").contains("ends inside a batch"), "{err:#}");
}

/// A generation's log always holds its header before the generation exists, so a log that is
/// empty, or zeroed where its header goes, is damage: the open fails rather than re-initialise
/// it and open as an empty database.
#[test]
fn test_tndb_log_without_a_header_is_refused() {
    for zeroed in [false, true] {
        let tmp = TempDir::with_prefix("tndb_no_header").expect("temp dir");
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        block_on(db.persist::<TestTable>()).expect("persist");
        crash(db);
        let log = log_path(tmp.path());
        if zeroed {
            let len = std::fs::metadata(&log).expect("log").len();
            std::fs::write(&log, vec![0_u8; len.min(1 << 20) as usize]).expect("zero");
        } else {
            truncate(&log, 0);
        }
        let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
            .err()
            .unwrap_or_else(|| panic!("a log with no header opened (zeroed: {zeroed})"));
        assert!(format!("{err:#}").contains("no header"), "{err:#}");
    }
}

/// An index the manifest does not name means the manifest was rolled back (one is always written
/// before a table's index exists): the open replays the log, so the missing table's records fail
/// it, and the index is kept rather than deleted with its rows. An orphan index whose table has no
/// records in the log indexes nothing, and goes.
#[test]
fn test_tndb_rolled_back_manifest_fails_the_open() {
    let tmp = TempDir::with_prefix("tndb_old_manifest").expect("temp dir");
    {
        let db = TnDatabase::open_with(tmp.path(), options(CommitMode::Group)).expect("open");
        db.open_table::<TestTable>().expect("open table");
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    }
    let old = std::fs::read(tmp.path().join("tables")).expect("manifest");
    {
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..10_u64 {
            db.insert::<OtherTable>(&i, &format!("o{i}")).expect("insert");
        }
    }
    let orphan = tmp.path().join("gen-0").join("idx-1");
    assert!(orphan.exists(), "the newer table's index");
    std::fs::write(tmp.path().join("tables"), &old).expect("roll the manifest back");
    let err = TnDatabase::open_with(tmp.path(), options(CommitMode::Group))
        .expect_err("the missing table's records fail the open");
    assert!(format!("{err:#}").contains("unknown table"), "{err:#}");
    assert!(orphan.exists(), "the orphan index is kept");

    let tmp = TempDir::with_prefix("tndb_stray_index").expect("temp dir");
    drop(open(tmp.path(), CommitMode::Group));
    let stray = tmp.path().join("gen-0").join("idx-9");
    std::fs::create_dir(&stray).expect("a stray index");
    drop(open(tmp.path(), CommitMode::Group));
    assert!(!stray.exists(), "an orphan with no records in the log is removed");
}

/// Creating a database's first generation is atomic: a failure (or a crash) once its directory
/// exists but before its log does leaves no log-less generation, which every later open would
/// refuse; the next open creates the database afresh.
#[test]
fn test_tndb_interrupted_creation_reopens() {
    let tmp = TempDir::with_prefix("tndb_gen0_build").expect("temp dir");
    super::store::FAIL_GEN0_BUILD.with(|fail| fail.set(true));
    assert!(TnDatabase::open(tmp.path()).is_err(), "the injected failure");
    // A crash part way would also leave its build directory: the next open deletes it.
    std::fs::create_dir(tmp.path().join("compact-0-1-1")).expect("a leftover build");
    let db = open(tmp.path(), CommitMode::Sync);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    drop(db);
    assert_eq!(super::layout::list_gens(tmp.path()).expect("gens"), [0]);
    assert_eq!(compact_dirs(tmp.path()), 0, "no build left behind");
    let db = open(tmp.path(), CommitMode::Sync);
    assert_eq!(rows::<TestTable>(&db), [(1, "a".to_string())]);
}

/// A read checks that the record at an index position is the row it asked for: a damaged index
/// entry that lands on another row's record is `CorruptIndex`, never that row's value; a
/// compaction abandons rather than copy the mismatch; and a read that finds it latches the
/// database, so writes stop (and the node with them) rather than act on a short scan.
#[test]
fn test_tndb_misindexed_row_is_corrupt_not_another_value() {
    let tmp = TempDir::with_prefix("tndb_misindexed").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    let id = db.core().table(<TestTable as tn_types::Table>::NAME).expect("open").id;
    db.core().misindex(id, &tn_types::encode_key(&3_u64), &tn_types::encode_key(&7_u64));

    let compacted = db.compact_now().expect_err("the copy finds the mismatch");
    assert!(compacted.contains("index"), "{compacted}");
    db.insert::<TestTable>(&10, &"v10".to_string())
        .expect("a compaction's abandon latches nothing");

    let read = db.get::<TestTable>(&3);
    assert!(read.is_err(), "the record at 3's position is row 7's: {read:?}");
    let write = db.insert::<TestTable>(&11, &"v11".to_string());
    assert!(write.is_err(), "the damage the get found is latched: {write:?}");
    assert_eq!(db.get::<TestTable>(&7).expect("get"), Some("v7".to_string()), "7 itself reads");
    let scanned: Vec<u64> = db.iter::<TestTable>().map(|(k, _)| k).collect();
    assert_eq!(scanned, [0, 1, 2], "a scan ends at the mismatch");
    assert!(block_on(db.persist::<TestTable>()).is_err(), "and no persist acknowledges past it");
}

// ---- production metrics ----

type MetricsSnapshot = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    metrics_util::debugging::DebugValue,
)>;

/// The value of the series `name` carrying every label in `labels`, if registered. A snapshot
/// reports a counter's increments and a histogram's samples since the last one, a gauge's value.
fn series<'a>(
    snapshot: &'a MetricsSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<&'a metrics_util::debugging::DebugValue> {
    snapshot
        .iter()
        .find(|(key, ..)| {
            key.key().name() == name
                && labels.iter().all(|(k, v)| {
                    key.key().labels().any(|label| label.key() == *k && label.value() == *v)
                })
        })
        .map(|(.., value)| value)
}

fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    match series(snapshot, name, labels) {
        Some(metrics_util::debugging::DebugValue::Counter(n)) => *n,
        other => panic!("counter {name} {labels:?}: {other:?}"),
    }
}

fn gauge(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> f64 {
    match series(snapshot, name, labels) {
        Some(metrics_util::debugging::DebugValue::Gauge(v)) => v.into_inner(),
        other => panic!("gauge {name} {labels:?}: {other:?}"),
    }
}

/// The samples of histogram `name` since the last snapshot (none if never recorded).
fn samples(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
    match series(snapshot, name, labels) {
        Some(metrics_util::debugging::DebugValue::Histogram(v)) => {
            v.iter().map(|x| x.into_inner()).collect()
        }
        None => Vec::new(),
        other => panic!("histogram {name} {labels:?}: {other:?}"),
    }
}

const M_BATCHES: &str = "tn_storage.tndb_batches_total";
const M_SYNC: &str = "tn_storage.tndb_sync_seconds";
const M_UNSYNCED: &str = "tn_storage.tndb_unsynced_bytes";
const M_LOG: &str = "tn_storage.tndb_log_bytes";
const M_GEN: &str = "tn_storage.tndb_generation";
const M_STALL: &str = "tn_storage.tndb_write_stall_seconds";
const M_COMPACTIONS: &str = "tn_storage.tndb_compactions_total";
const M_ABANDONS: &str = "tn_storage.tndb_compaction_abandons_total";
const M_COMPACTION: &str = "tn_storage.tndb_compaction_seconds";
const M_SWITCH: &str = "tn_storage.tndb_compaction_switch_seconds";
const M_FAILED: &str = "tn_storage.tndb_failed";

/// A database's metrics: every series registered at open (counters at 0), then batches, syncs,
/// the log's and unsynced bytes, a compaction (outcome, generation, duration, switch hold) and
/// an abandon by cause, each recorded as it happens, through handles the background threads
/// share (no recorder lookup on their threads).
#[test]
fn test_tndb_metrics_record_writes_syncs_and_compactions() {
    use std::sync::atomic::Ordering::Relaxed;
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let tmp = TempDir::with_prefix("tndb_metrics").expect("temp dir");
    let db = metrics::with_local_recorder(&recorder, || {
        open_compacting(tmp.path(), CommitMode::Group, manual())
    });
    let env = [("env", "unnamed")];

    let opened = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&opened, M_BATCHES, &env), 0);
    for outcome in ["switched", "skipped", "abandoned", "cancelled"] {
        assert_eq!(counter(&opened, M_COMPACTIONS, &[env[0], ("outcome", outcome)]), 0);
    }
    for cause in ["disk_full", "writer_outran", "corruption", "panic", "error"] {
        assert_eq!(counter(&opened, M_ABANDONS, &[env[0], ("cause", cause)]), 0);
    }
    assert_eq!(gauge(&opened, M_FAILED, &env), 0.0);
    assert_eq!(gauge(&opened, M_GEN, &env), 0.0);

    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    block_on(db.persist::<TestTable>()).expect("persist");
    let wrote = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&wrote, M_BATCHES, &env), 10);
    assert!(!samples(&wrote, M_SYNC, &env).is_empty(), "the syncs are timed");
    assert_eq!(gauge(&wrote, M_LOG, &env), log_len(&db) as f64);
    assert_eq!(gauge(&wrote, M_UNSYNCED, &env), 0.0, "all synced after the persist");

    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &format!("w{i}")).expect("overwrite");
    }
    assert_eq!(db.compact_now(), Ok(true));
    let compacted = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&compacted, M_COMPACTIONS, &[env[0], ("outcome", "switched")]), 1);
    assert_eq!(gauge(&compacted, M_GEN, &env), 1.0);
    assert_eq!(gauge(&compacted, M_LOG, &env), log_len(&db) as f64);
    assert_eq!(samples(&compacted, M_COMPACTION, &env).len(), 1);
    assert_eq!(samples(&compacted, M_SWITCH, &env).len(), 1);

    *db.core().compact.test.free_space.lock() = Some(0);
    assert!(db.compact_now().is_err());
    *db.core().compact.test.free_space.lock() = None;
    db.core().compact.test.fail_next.store(true, Relaxed);
    assert!(db.compact_now().is_err());
    let abandoned = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&abandoned, M_COMPACTIONS, &[env[0], ("outcome", "abandoned")]), 2);
    assert_eq!(counter(&abandoned, M_ABANDONS, &[env[0], ("cause", "disk_full")]), 1);
    assert_eq!(counter(&abandoned, M_ABANDONS, &[env[0], ("cause", "error")]), 1);
    assert_eq!(gauge(&abandoned, M_FAILED, &env), 0.0, "an abandon latches nothing");
}

/// A write-lock hold of at least 1 ms is recorded as a stall; shorter ones are not.
#[test]
fn test_tndb_metrics_record_write_stalls() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let tmp = TempDir::with_prefix("tndb_metrics_stall").expect("temp dir");
    let db = metrics::with_local_recorder(&recorder, || open(tmp.path(), CommitMode::Group));
    let hold = [("env", "unnamed"), ("kind", "hold")];
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let _ = snapshotter.snapshot();
    db.core().hold_next_batch(Duration::from_millis(5));
    db.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    let stalled = snapshotter.snapshot().into_vec();
    let held = samples(&stalled, M_STALL, &hold);
    assert_eq!(held.len(), 1, "one stall: {held:?}");
    assert!(held[0] >= 0.005, "held {}s", held[0]);
}

/// A latched failure (here a failed sync) sets the database's `failed` gauge.
#[test]
fn test_tndb_metrics_failed_gauge_latches() {
    use std::sync::atomic::Ordering::Relaxed;
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let tmp = TempDir::with_prefix("tndb_metrics_failed").expect("temp dir");
    let db = metrics::with_local_recorder(&recorder, || open(tmp.path(), CommitMode::Group));
    db.core().durability.fail_next_sync.store(true, Relaxed);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    assert!(block_on(db.persist::<TestTable>()).is_err(), "the sync failed");
    let failed = snapshotter.snapshot().into_vec();
    assert_eq!(gauge(&failed, M_FAILED, &[("env", "unnamed")]), 1.0);
}

/// A close during a compaction counts it as cancelled, not abandoned.
#[test]
fn test_tndb_metrics_count_a_cancel() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let tmp = TempDir::with_prefix("tndb_metrics_cancel").expect("temp dir");
    let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 1 << 20 };
    let db = metrics::with_local_recorder(&recorder, || {
        open_compacting(tmp.path(), CommitMode::Group, config)
    });
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
    drop(db);
    let closed = snapshotter.snapshot().into_vec();
    let env = ("env", "unnamed");
    assert_eq!(counter(&closed, M_COMPACTIONS, &[env, ("outcome", "cancelled")]), 1);
    assert_eq!(counter(&closed, M_COMPACTIONS, &[env, ("outcome", "abandoned")]), 0);
}

/// A `SplitDatabase` labels its three logs' series with their environments.
#[test]
fn test_tndb_metrics_split_labels_its_environments() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let tmp = TempDir::with_prefix("tndb_metrics_split").expect("temp dir");
    let _db = metrics::with_local_recorder(&recorder, || {
        super::SplitDatabase::open_with(tmp.path(), TnDbOptions::default()).expect("open")
    });
    let opened = snapshotter.snapshot().into_vec();
    for env in ["epoch", "kad", "cache"] {
        assert_eq!(gauge(&opened, M_FAILED, &[("env", env)]), 0.0, "{env}");
    }
    assert!(series(&opened, M_FAILED, &[("env", "unnamed")]).is_none());
}

// ---- inspection (`db tndb-stat`) ----

/// Every file under `root` with its length and modification time (to show an inspection changed
/// nothing).
fn file_states(root: &Path) -> BTreeMap<std::path::PathBuf, (u64, std::time::SystemTime)> {
    let mut states = BTreeMap::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let meta = entry.metadata().expect("metadata");
            if meta.is_dir() {
                dirs.push(entry.path());
            } else {
                states.insert(entry.path(), (meta.len(), meta.modified().expect("mtime")));
            }
        }
    }
    states
}

/// The report of `table` in `report`.
fn table_report<'a>(report: &'a super::EnvReport, table: &str) -> &'a super::TableReport {
    report.tables.iter().find(|t| t.name == table).expect("the table is reported")
}

/// An inspection reports each table's live rows and bytes, the log's dead bytes (removed and
/// overwritten rows), the generation and its compacted base; after a compaction nothing is dead.
#[test]
fn test_tndb_inspect_reports_rows_and_dead_bytes() {
    use tn_types::Table as _;
    let tmp = TempDir::with_prefix("tndb_inspect").expect("temp dir");
    {
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        for i in 0..100_u64 {
            db.insert::<TestTable>(&i, &format!("value {i}")).expect("insert");
        }
        for i in 0..10_u64 {
            db.remove::<TestTable>(&i).expect("remove");
        }
        for i in 10..20_u64 {
            db.insert::<TestTable>(&i, &format!("again {i}")).expect("overwrite");
        }
        block_on(db.persist::<TestTable>()).expect("persist");
    }
    let report = super::inspect(tmp.path()).expect("inspect");
    assert_eq!(report.generation, 0);
    assert!(report.clean, "closed cleanly");
    let test = table_report(&report, TestTable::NAME);
    assert_eq!(test.rows, 90);
    assert_eq!(test.index, super::IndexState::Current);
    let other = table_report(&report, OtherTable::NAME);
    assert_eq!((other.rows, other.index), (0, super::IndexState::NoKeySize));
    assert!(report.dead_bytes > 0, "removed and overwritten rows are dead");
    let header = crate::archive::pack::DATA_HEADER_BYTES as u64;
    assert_eq!(header + report.live_bytes + report.dead_bytes, report.data_end);
    assert_eq!(report.live_bytes, test.live_bytes);

    {
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        assert_eq!(db.compact_now(), Ok(true));
    }
    let report = super::inspect(tmp.path()).expect("inspect");
    assert_eq!(report.generation, 1);
    assert_eq!(report.dead_bytes, 0, "a compacted log holds only live rows");
    assert_eq!(report.base, report.data_end);
    assert_eq!(table_report(&report, TestTable::NAME).rows, 90);
}

/// An inspection after a crash reports the log unclean and the index as the next open would
/// rebuild it, and changes no file (the next open still recovers every row).
#[test]
fn test_tndb_inspect_changes_nothing_after_a_crash() {
    use tn_types::Table as _;
    let tmp = TempDir::with_prefix("tndb_inspect_crash").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    for i in 0..50_u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    block_on(db.persist::<TestTable>()).expect("persist");
    crash(db);
    let before = file_states(tmp.path());
    let report = super::inspect(tmp.path()).expect("inspect");
    assert!(!report.clean, "a crashed log is unclean");
    let test = table_report(&report, TestTable::NAME);
    assert_eq!(test.rows, 50);
    assert_ne!(test.index, super::IndexState::Current, "the next open rebuilds it");
    assert_eq!(file_states(tmp.path()), before, "the inspection changed nothing");
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db).len(), 50);
}

/// An inspection lists an index directory the manifest does not name (it forces a replay at the
/// next open) and changes nothing; the next open's replay passes (the table has no records) and
/// deletes it.
#[test]
fn test_tndb_inspect_lists_orphan_indexes() {
    let tmp = TempDir::with_prefix("tndb_inspect_orphan").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    }
    let gen = tmp.path().join("gen-0");
    copy_dir(&gen.join("idx-0"), &gen.join("idx-9"));
    let before = file_states(tmp.path());
    let report = super::inspect(tmp.path()).expect("inspect");
    assert_eq!(report.orphan_indexes, ["idx-9"]);
    assert_eq!(file_states(tmp.path()), before, "the inspection changed nothing");
    drop(open(tmp.path(), CommitMode::Group));
    assert!(super::inspect(tmp.path()).expect("inspect").orphan_indexes.is_empty(), "deleted");
}

/// An inspection refuses a database another handle has open (the node's), and runs once it
/// closes.
#[test]
fn test_tndb_inspect_refuses_an_open_database() {
    let tmp = TempDir::with_prefix("tndb_inspect_open").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let err = super::inspect(tmp.path()).expect_err("the database is open");
    assert!(format!("{err:#}").contains("is open"), "{err:#}");
    drop(db);
    super::inspect(tmp.path()).expect("closed: inspected");
}

/// A damaged log fails the inspection (the record a replay cannot place), as it would fail the
/// next open.
#[test]
fn test_tndb_inspect_reports_damage() {
    let tmp = TempDir::with_prefix("tndb_inspect_damage").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Sync);
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    }
    append_unsealed(&log_path(tmp.path()), &record::head(KIND_PUT, 9, true));
    assert!(super::inspect(tmp.path()).is_err(), "a record of an unknown table");
}

/// A `SplitDatabase`'s inspection covers each environment present, and notes the MDBX
/// migration's marker.
#[test]
fn test_tndb_inspect_split() {
    let tmp = TempDir::with_prefix("tndb_inspect_split").expect("temp dir");
    drop(super::SplitDatabase::open_with(tmp.path(), TnDbOptions::default()).expect("open"));
    let report = super::inspect_split(tmp.path());
    let names: Vec<&str> = report.envs.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, ["epoch", "kad", "cache"]);
    assert!(report.envs.iter().all(|(_, env)| env.is_ok()));
    assert!(!report.migrated);
    std::fs::write(tmp.path().join(super::MDBX_MIGRATED), b"").expect("marker");
    assert!(super::inspect_split(tmp.path()).migrated);
}

// ---- failure handling: rollback, latches ----

/// A panic inside a failed batch's rollback leaves the writer half-reverted: the database latches
/// (every later write fails, on any table) instead of writing on after it.
#[test]
fn test_tndb_panic_in_rollback_latches() {
    let tmp = TempDir::with_prefix("tndb_rollback_panic").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    db.core().panic_next_batch_after(1);
    db.core().panic_in_next_rollback();
    let mut txn = db.write_txn().expect("txn");
    txn.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    txn.insert::<TestTable>(&3, &"c".to_string()).expect("insert");
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| txn.commit()));
    assert!(panicked.is_err(), "the batch and its rollback panicked");
    assert!(db.insert::<OtherTable>(&9, &"z".to_string()).is_err(), "latched");
}

/// A panic after a batch's records are appended (publishing it) leaves the writer half-applied:
/// the database latches.
#[test]
fn test_tndb_panic_in_publish_latches() {
    let tmp = TempDir::with_prefix("tndb_publish_panic").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    db.core().panic_in_next_publish();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.insert::<TestTable>(&2, &"b".to_string())
    }));
    assert!(panicked.is_err(), "the publish panicked");
    assert!(db.insert::<OtherTable>(&9, &"z".to_string()).is_err(), "latched");
}

/// A failed batch whose index is poisoned (a failed growth: the file is never sealed and refuses
/// every later growth) stops the writer: rather than one table refusing writes while the rest
/// carry on, the whole database latches.
#[test]
fn test_tndb_poisoned_index_latches() {
    let tmp = TempDir::with_prefix("tndb_index_poisoned").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&0, &"a".to_string()).expect("insert");
    db.insert::<OtherTable>(&0, &"o".to_string()).expect("insert");
    let id = db.core().table(<TestTable as tn_types::Table>::NAME).expect("open").id;
    db.core().with_index(id, |idx| idx.poison_for_test()).expect("index");
    let mut txn = db.write_txn().expect("txn");
    for i in 1..2_000_u64 {
        txn.insert::<TestTable>(&i, &"v".to_string()).expect("insert");
    }
    assert!(txn.commit().is_err(), "the poisoned index cannot grow");
    assert!(db.insert::<OtherTable>(&1, &"o".to_string()).is_err(), "latched");
}

/// A rolled-back batch's zeroed bytes are flushed at once: a sync or a writeback may already have
/// put the batch on disk, and only the zeros reaching it keep a power loss from bringing it back.
#[test]
fn test_tndb_rollback_flushes_the_zeroed_batch() {
    let tmp = TempDir::with_prefix("tndb_rollback_flush").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let other = db.core().table(<OtherTable as tn_types::Table>::NAME).expect("open").id;
    db.insert::<OtherTable>(&1, &"o".to_string()).expect("insert");
    db.core().with_index(other, |idx| idx.fail_allocations_after(Some(0))).expect("index");
    let before = db.core().rollback_flushes();
    let mut txn = db.write_txn().expect("txn");
    for i in 2..2_000_u64 {
        txn.insert::<OtherTable>(&i, &"v".repeat(64)).expect("insert");
    }
    assert!(txn.commit().is_err(), "the index step fails");
    assert_eq!(db.core().rollback_flushes() - before, 1, "the zeroed batch was flushed");
    db.core().with_index(other, |idx| idx.fail_allocations_after(None)).expect("index");
    db.insert::<TestTable>(&2, &"b".to_string()).expect("not latched: a later write works");
}

/// A database whose manifest (`<root>/tables`) is lost does not open as an empty one, which would
/// drop its tables' rows and hand their ids to new tables: its log is replayed, and the first
/// record, of a table no manifest names, fails the open. Nothing is deleted on the way.
#[test]
fn test_tndb_a_lost_manifest_fails_the_open() {
    let tmp = TempDir::with_prefix("tndb_lost_manifest").expect("temp dir");
    {
        let db = open(tmp.path(), CommitMode::Group);
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        block_on(db.persist::<TestTable>()).expect("persist");
    }
    let manifest = tmp.path().join("tables");
    assert!(manifest.exists(), "the manifest's path");
    std::fs::remove_file(&manifest).expect("lose the manifest");
    let before = files_under(tmp.path());
    let reopened = TnDatabase::open_with(
        tmp.path(),
        TnDbOptions { commit: CommitMode::Group, ..Default::default() },
    );
    assert!(reopened.is_err(), "a log with records and no manifest must not open empty");
    assert_eq!(files_under(tmp.path()), before, "and the failed open deleted nothing");

    // A fresh database (no records) still opens without one.
    let fresh = TempDir::with_prefix("tndb_fresh_no_manifest").expect("temp dir");
    drop(open(fresh.path(), CommitMode::Group));
    std::fs::remove_file(fresh.path().join("tables")).ok();
    drop(open(fresh.path(), CommitMode::Group));
}

/// Every file path under `dir`, sorted.
fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(dir) = todo.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                todo.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// A rolled-back batch's flush is a sync of the log like a round's, and runs under the same lock:
/// with a round in flight (its lock held here), the rollback waits for it instead of syncing beside
/// it, where it could take a writeback error the round must see.
#[test]
fn test_tndb_rollback_flush_waits_for_a_running_sync_round() {
    let tmp = TempDir::with_prefix("tndb_rollback_one_sync").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    let before = db.core().rollback_flushes();
    db.core().fail_next_batch();
    let db = &db;
    std::thread::scope(|s| {
        let round = db.core().sync_lock.lock();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = s.spawn(move || {
            let failed = db.insert::<TestTable>(&2, &"b".to_string());
            let _ = done_tx.send(());
            failed
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "the rollback flushed beside a running sync round"
        );
        drop(round);
        done_rx.recv_timeout(Duration::from_secs(10)).expect("the rollback finishes after it");
        assert!(writer.join().expect("writer").is_err(), "the batch failed");
    });
    assert_eq!(db.core().rollback_flushes() - before, 1, "and its zeros were flushed");
    db.insert::<TestTable>(&3, &"c".to_string()).expect("not latched: a later write works");
}

/// In `Sync` mode a write that changes nothing (removing an absent key) still waits until the
/// state it observed is durable: here the key's removal by another writer, whose sync is paused.
#[test]
fn test_tndb_sync_no_op_waits_for_the_state_it_saw() {
    let tmp = TempDir::with_prefix("tndb_sync_no_op").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Sync);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let (arrived, release) = db.core().durability.gate_next_sync();
    let db = &db;
    std::thread::scope(|s| {
        let first = s.spawn(|| db.remove::<TestTable>(&1));
        arrived.recv_timeout(Duration::from_secs(10)).expect("the removal's sync paused");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let no_op = s.spawn(move || {
            let removed = db.remove::<TestTable>(&1);
            let _ = done_tx.send(());
            removed
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the no-op returned before the removal it saw was durable"
        );
        drop(release);
        first.join().expect("thread").expect("the removal");
        no_op.join().expect("thread").expect("the no-op");
    });
}

/// In `Sync` mode `persist` covers another thread's write that is visible but not yet durable
/// (its sync paused here), as in group mode, rather than only checking for a failure.
#[test]
fn test_tndb_sync_persist_waits_for_another_writers_write() {
    let tmp = TempDir::with_prefix("tndb_sync_persist").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Sync);
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let (arrived, release) = db.core().durability.gate_next_sync();
    let db = &db;
    std::thread::scope(|s| {
        let writer = s.spawn(|| db.insert::<TestTable>(&2, &"b".to_string()));
        arrived.recv_timeout(Duration::from_secs(10)).expect("the write's sync paused");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let persist = s.spawn(move || {
            let persisted = block_on(db.persist::<TestTable>());
            let _ = done_tx.send(());
            persisted
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "persist returned before the write it covers was durable"
        );
        drop(release);
        writer.join().expect("thread").expect("the write");
        persist.join().expect("thread").expect("the persist");
    });
}

/// A `persist` waiting when the commit thread stops (a crashed process's) fails instead of
/// waiting forever for a sync no thread will run.
#[test]
fn test_tndb_persist_fails_once_the_commit_thread_stops() {
    let tmp = TempDir::with_prefix("tndb_persist_stopped").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.stop_commit_thread();
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    let waited = block_on(async {
        tokio::time::timeout(Duration::from_secs(5), db.persist::<TestTable>()).await
    });
    let persisted = waited.expect("persist returned instead of hanging");
    assert!(persisted.is_err(), "nothing will sync the write");
}

/// A thread's encode buffers do not keep the capacity of the largest value it ever wrote.
#[test]
fn test_tndb_encode_buffers_shrink_after_a_large_value() {
    let tmp = TempDir::with_prefix("tndb_bufs").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    db.insert::<TestTable>(&1, &"v".repeat(8 << 20)).expect("insert");
    let (key, value) = super::database::buf_capacities();
    assert!(key <= 4 << 20 && value <= 4 << 20, "kept ({key}, {value}) bytes");
}

/// The trigger's `base` after a compaction is the one written to the generation's `compacted`
/// file, even when batches landed in the switch's tail after it was written: a restart then
/// compacts on the same schedule as the running database.
#[test]
fn test_tndb_compaction_base_matches_the_written_one() {
    let tmp = TempDir::with_prefix("tndb_compact_base").expect("temp dir");
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
        db.insert::<TestTable>(&100, &"in the switch's tail".to_string()).expect("insert");
        drop(release);
        assert_eq!(compaction.join().expect("compaction thread"), Ok(true));
    });
    let written = super::compact::read_base(&tmp.path().join("gen-1"));
    assert_eq!(db.core().base(), written);
}

/// A value past the 16 MiB a default pack allows (a large consensus output can be): tndb's log
/// takes records up to 1 GiB, so it is written, read back, replayed after a crash and copied by a
/// compaction.
#[test]
fn test_tndb_values_past_16_mib() {
    let tmp = TempDir::with_prefix("tndb_big_value").expect("temp dir");
    let big = "v".repeat(20 << 20);
    {
        let db = open_compacting(tmp.path(), CommitMode::Group, manual());
        db.insert::<TestTable>(&1, &big).expect("a 20 MiB value");
        db.insert::<TestTable>(&2, &"small".to_string()).expect("insert");
        assert_eq!(db.get::<TestTable>(&1).expect("get").as_deref(), Some(big.as_str()));
        block_on(db.persist::<TestTable>()).expect("persist");
        crash(db);
    }
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    assert_eq!(db.get::<TestTable>(&1).expect("replayed").as_deref(), Some(big.as_str()));
    db.insert::<TestTable>(&2, &"dead".to_string()).expect("overwrite");
    assert_eq!(db.compact_now(), Ok(true));
    assert_eq!(db.get::<TestTable>(&1).expect("compacted").as_deref(), Some(big.as_str()));
}

/// A compaction stamps its new indexes' pages in its background rounds, so the switch (under the
/// write lock) stamps only the pages its own tail wrote, not the whole index.
#[test]
fn test_tndb_compaction_stamps_index_pages_before_the_switch() {
    use std::sync::atomic::Ordering::Relaxed;
    let tmp = TempDir::with_prefix("tndb_compact_stamp").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    for i in 0..20_000_u64 {
        db.insert::<TestTable>(&i, &"v".to_string()).expect("insert");
    }
    db.remove::<TestTable>(&0).expect("a dead row");
    assert_eq!(db.compact_now(), Ok(true));
    let private = db.core().compact.test.private_at_switch.load(Relaxed);
    assert!(private <= 8, "{private} pages left to stamp under the write lock");
}

// ---- power loss (the disk model in `archive::data_file`) ----

/// Crash `db` (a dead process: its writes stay in the page cache), then lose power: the log
/// keeps only what reached the disk.
fn power_loss(db: TnDatabase, root: &Path) {
    crash(db);
    crate::archive::data_file::disk_model::power_loss(&log_path(root));
}

/// Rows a crashed process wrote but never synced are made durable by the next open before it
/// builds on them: a power loss after that open's later (acknowledged) writes keeps them all.
#[test]
fn test_tndb_power_loss_keeps_rows_from_before_an_unclean_reopen() {
    use crate::archive::data_file::disk_model;
    let tmp = TempDir::with_prefix("tndb_power_unclean").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    disk_model::track(&log_path(tmp.path()));
    db.stop_commit_thread();
    // 40 KiB of rows: they span pages (16 KiB on Apple silicon) that a later msync, from where
    // the log then ends, does not reach.
    for i in 0..10_u64 {
        db.insert::<TestTable>(&i, &"v".repeat(4096)).expect("insert, never synced");
    }
    crash(db);
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db).len(), 10, "replayed from the page cache");
    for i in 10..20_u64 {
        db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
    }
    block_on(db.persist::<TestTable>()).expect("persist: acknowledged");
    power_loss(db, tmp.path());
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db).len(), 20, "every row the reopened log built on");
    drop(db);
    disk_model::untrack(&log_path(tmp.path()));
}

/// Recovery that cuts a log back to its last whole batch flushes the zeros it leaves at once. The
/// open-time sync made the old tail durable (here a damaged batch and a whole one after it), and
/// zeros only in memory would leave it on disk past the cut after a power loss, for a later
/// replay to reach behind newer batches.
#[test]
fn test_tndb_recovery_flushes_the_tail_it_cuts() {
    use std::os::unix::fs::FileExt as _;

    use crate::archive::data_file::disk_model;
    let tmp = TempDir::with_prefix("tndb_recovery_flush").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    disk_model::track(&log_path(tmp.path()));
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    let cut = db.core().frontier.load().end;
    db.stop_commit_thread();
    db.insert::<TestTable>(&2, &"x".repeat(4096)).expect("a batch, never synced");
    db.insert::<TestTable>(&3, &"y".repeat(4096)).expect("a whole batch after it");
    crash(db);
    // Damage the first of them: the next open replays only up to `cut`.
    let log = std::fs::OpenOptions::new().write(true).open(log_path(tmp.path())).expect("open");
    log.write_all_at(&[0xAA], cut + 40).expect("damage the batch");
    drop(log);

    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db), [(1, "a".to_string())], "cut at the damaged batch");
    crash(db);
    disk_model::power_loss(&log_path(tmp.path()));
    let log = std::fs::File::open(log_path(tmp.path())).expect("open");
    let mut tail = vec![0_u8; 8192];
    log.read_exact_at(&mut tail, cut).expect("read past the cut");
    assert!(tail.iter().all(|&b| b == 0), "the cut tail's zeros reached the disk");
    disk_model::untrack(&log_path(tmp.path()));
}

/// Recovery's cut warns only when bytes other than the commit marker followed the last whole
/// batch: after an ordinary crash (the capacity's zeros and a stamped marker past the acknowledged
/// batches) it cuts quietly, and a torn batch past them is reported.
#[test]
fn test_tndb_recovery_cut_warns_only_for_data() {
    use crate::consensus_pack::test::warnings_and_errors;
    for torn in [false, true] {
        let tmp = TempDir::with_prefix("tndb_cut_warning").expect("temp dir");
        let db = open(tmp.path(), CommitMode::Group);
        for i in 0..3_u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
            block_on(db.persist::<TestTable>()).expect("persist");
        }
        if torn {
            db.stop_commit_thread();
            db.insert::<TestTable>(&3, &"x".repeat(4096)).expect("a batch, never synced");
        }
        crash(db);
        let log = log_path(tmp.path());
        assert!(commit_marker(&log).is_some(), "a marker stamped at the tail");
        if torn {
            flip_byte(&log, record_ends(&log)[2] + 6);
        }
        let (db, levels) = warnings_and_errors(|| open(tmp.path(), CommitMode::Group));
        assert_eq!(rows::<TestTable>(&db).len(), 3, "the acknowledged rows (torn: {torn})");
        assert_eq!(levels.contains(&tracing::Level::WARN), torn, "torn: {torn}, {levels:?}");
    }
}

/// A failed batch whose bytes reached the disk before its rollback (the system's writeback) does
/// not come back after a power loss: the rollback's zeros are flushed over it.
#[test]
fn test_tndb_power_loss_never_brings_back_a_rolled_back_batch() {
    use crate::archive::data_file::disk_model;
    let tmp = TempDir::with_prefix("tndb_power_rollback").expect("temp dir");
    let db = open(tmp.path(), CommitMode::Group);
    disk_model::track(&log_path(tmp.path()));
    db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
    block_on(db.persist::<TestTable>()).expect("persist");
    db.core().fail_next_batch_after_writeback();
    let mut txn = db.write_txn().expect("txn");
    txn.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
    txn.insert::<TestTable>(&3, &"c".to_string()).expect("insert");
    assert!(txn.commit().is_err(), "the batch failed after its records were written");
    power_loss(db, tmp.path());
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(rows::<TestTable>(&db), [(1, "a".to_string())], "the failed batch stays gone");
    drop(db);
    disk_model::untrack(&log_path(tmp.path()));
}

// ---- concurrency ----

/// Writers (bare writes and transactions, each on its own keys), a scanner, a persister and
/// compactions, all at once: afterwards the tables hold exactly what the writers' models say, and
/// so does a reopen after a crash (all of it persisted).
#[test]
fn test_tndb_concurrent_stress() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

    type Model = (BTreeMap<u64, String>, BTreeMap<u64, String>);
    let tmp = TempDir::with_prefix("tndb_stress").expect("temp dir");
    let db = open_compacting(tmp.path(), CommitMode::Group, manual());
    let stop = AtomicBool::new(false);
    let (models, compacted) = std::thread::scope(|s| {
        let (db, stop) = (&db, &stop);
        let writers: Vec<_> = (0..4_u64)
            .map(|w| {
                s.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(0x5713_0000 + w);
                    let mut model = Model::default();
                    let mut n = 0_u64;
                    while !stop.load(Relaxed) {
                        n += 1;
                        let k = w * 1_000 + rng.random_range(0..200);
                        let v = format!("w{w}-{n}");
                        match rng.random_range(0..10) {
                            0..4 => {
                                db.insert::<TestTable>(&k, &v).expect("insert");
                                model.0.insert(k, v);
                            }
                            4..6 => {
                                db.remove::<TestTable>(&k).expect("remove");
                                model.0.remove(&k);
                            }
                            6..8 => {
                                db.insert::<OtherTable>(&k, &v).expect("insert");
                                model.1.insert(k, v);
                            }
                            _ => {
                                let mut txn = db.write_txn().expect("txn");
                                txn.insert::<TestTable>(&k, &v).expect("txn insert");
                                txn.insert::<OtherTable>(&(k + 1), &v).expect("txn insert");
                                txn.remove::<TestTable>(&(k + 2)).expect("txn remove");
                                txn.commit().expect("commit");
                                model.0.insert(k, v.clone());
                                model.1.insert(k + 1, v);
                                model.0.remove(&(k + 2));
                            }
                        }
                    }
                    model
                })
            })
            .collect();
        let scanner = s.spawn(move || {
            while !stop.load(Relaxed) {
                let keys: Vec<u64> = db.iter::<TestTable>().map(|(k, _)| k).collect();
                assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "a scan is in key order");
                assert!(keys.iter().all(|k| k % 1_000 < 202), "only keys some writer wrote");
            }
        });
        let persister = s.spawn(move || {
            while !stop.load(Relaxed) {
                block_on(db.persist::<TestTable>()).expect("persist");
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let compactor = s.spawn(move || {
            let mut switched = 0;
            while !stop.load(Relaxed) {
                if db.compact_now() == Ok(true) {
                    switched += 1;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            switched
        });
        std::thread::sleep(Duration::from_millis(1_500));
        stop.store(true, Relaxed);
        scanner.join().expect("scanner");
        persister.join().expect("persister");
        let compacted = compactor.join().expect("compactor");
        let models: Vec<Model> =
            writers.into_iter().map(|writer| writer.join().expect("writer")).collect();
        (models, compacted)
    });
    assert!(compacted > 0, "compactions ran alongside");
    let mut expected = Model::default();
    for (test, other) in models {
        expected.0.extend(test);
        expected.1.extend(other);
    }
    let found = |db: &TnDatabase| -> Model {
        (db.iter::<TestTable>().collect(), db.iter::<OtherTable>().collect())
    };
    assert!(found(&db) == expected, "the tables are the writers' models");
    block_on(db.persist::<TestTable>()).expect("persist");
    crash(db);
    let db = open(tmp.path(), CommitMode::Group);
    assert!(found(&db) == expected, "and so is the reopened database");
}

/// The log outgrows its mapping's reservation several times (each time remapping) while the
/// commit thread syncs and a reader scans: every read sees whole, correct rows, and the database
/// reopens after a crash with all of them.
#[test]
fn test_tndb_log_grows_past_its_reservation() {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    let tmp = TempDir::with_prefix("tndb_reserve").expect("temp dir");
    super::store::LOG_RESERVE.with(|reserve| reserve.set(Some(1 << 20)));
    let db = open(tmp.path(), CommitMode::Group);
    super::store::LOG_RESERVE.with(|reserve| reserve.set(None));
    let value = |i: u64| format!("{i:08}").repeat(64);
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        let (db, stop) = (&db, &stop);
        let reader = s.spawn(move || {
            let mut scans = 0;
            while !stop.load(Relaxed) {
                for (k, v) in db.iter::<TestTable>() {
                    assert_eq!(v, value(k), "row {k} reads whole");
                }
                scans += 1;
            }
            scans
        });
        for i in 0..16_000_u64 {
            db.insert::<TestTable>(&i, &value(i)).expect("insert");
            if i % 1_000 == 999 {
                block_on(db.persist::<TestTable>()).expect("persist");
            }
        }
        stop.store(true, Relaxed);
        assert!(reader.join().expect("reader") > 0, "the reader scanned meanwhile");
    });
    assert!(log_len(&db) > 8 << 20, "the log outgrew its 1 MiB reservation several times");
    block_on(db.persist::<TestTable>()).expect("persist");
    crash(db);
    let db = open(tmp.path(), CommitMode::Group);
    assert_eq!(db.iter::<TestTable>().count(), 16_000);
    assert_eq!(db.get::<TestTable>(&12_345).expect("get"), Some(value(12_345)));
}
