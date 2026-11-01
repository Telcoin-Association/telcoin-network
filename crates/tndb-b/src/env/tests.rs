//! Environment tests: create and reopen, the lock, snapshots, abort, recovery's meta choice,
//! failing closed, the latch, `SyncMode::NoSync`, the writer hand-off and table handles.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::FileExt as _,
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};

use tempfile::TempDir;

use super::*;
use crate::{catalog, disk_model, meta::PAGE_SIZES, ReadTxn, Table, TableStat};

fn temp() -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix("tndb_b_env").expect("temp dir");
    let path = dir.path().join("db");
    (dir, path)
}

fn options(page_size: u32, sync: SyncMode) -> EnvOptions {
    EnvOptions { page_size, sync, ..EnvOptions::default() }
}

fn open(path: &Path) -> Env {
    Env::open(path, EnvOptions::default()).expect("open")
}

fn val(txn: &impl ReadTxn, table: &Table, key: &[u8]) -> Option<Vec<u8>> {
    txn.get(table, key).expect("get").map(<[u8]>::to_vec)
}

/// Commit `entries` into table `name` (created if missing) in one transaction.
fn put_all(env: &Env, name: &str, entries: &[(&[u8], &[u8])]) -> u64 {
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table(name).expect("table");
    for (k, v) in entries {
        txn.put(&table, k, v).expect("put");
    }
    txn.commit().expect("commit")
}

fn table(txn: &impl ReadTxn, name: &str) -> Table {
    txn.open_table(name).expect("open table").expect("the table exists")
}

/// Flip one byte of the file at `offset` (the env must be closed or the byte unread).
pub(crate) fn flip(path: &Path, offset: u64) {
    let file = fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
    let mut b = [0];
    file.read_exact_at(&mut b, offset).expect("read");
    file.write_all_at(&[b[0] ^ 0x10], offset).expect("write");
}

/// Open must fail with an error `ok` accepts and leave every byte of the file as it was.
pub(crate) fn assert_fails_closed(path: &Path, ok: impl Fn(&Error) -> bool) {
    let before = fs::read(path).expect("read");
    match Env::open(path, EnvOptions::default()) {
        Err(e) => assert!(ok(&e), "unexpected error {e}"),
        Ok(_) => panic!("the open must fail"),
    }
    assert!(fs::read(path).expect("read") == before, "a failed open changed the file");
}

/// Every page size creates, commits multi-level trees, reopens (the file's page size wins over
/// the options') and keeps writing.
#[test]
fn create_and_reopen_at_every_page_size() {
    for p in PAGE_SIZES {
        let (_dir, path) = temp();
        let env = Env::open(&path, options(p, SyncMode::Durable)).expect("create");
        let geometry = Geometry::new(p).expect("geometry");
        // A short key takes a value up to 16 MiB (in an overflow run); a key too long for an
        // overflow cell takes its leaf's limit.
        assert_eq!((env.max_key_len(), env.max_value_len(8)), (geometry.max_key(), 16 << 20));
        let longest = env.max_key_len();
        assert_eq!(env.max_value_len(longest), geometry.max_value(longest));
        // A key over the limit has no value limit: it is refused even with an empty value.
        assert_eq!(env.max_value_len(env.max_key_len() + 1), 0);
        assert!(env.max_value_len(env.max_key_len()) > 0);
        let vlen = geometry.max_value(8) / 2;
        let mut model = BTreeMap::new();
        let mut txn = env.begin_rw().expect("begin");
        let (a, b) = (txn.create_table("a").expect("a"), txn.create_table("b").expect("b"));
        for i in 0..40_u32 {
            let (k, v) = (format!("k{i:07}").into_bytes(), vec![i as u8; vlen]);
            txn.put(&a, &k, &v).expect("put");
            model.insert(k, v);
        }
        txn.put(&b, b"only", b"one").expect("put b");
        assert_eq!(txn.commit().expect("commit"), 1);
        let mut txn = env.begin_rw().expect("begin");
        for i in (0..40_u32).step_by(3) {
            let k = format!("k{i:07}").into_bytes();
            assert!(txn.delete(&a, &k).expect("delete"));
            model.remove(&k);
        }
        assert_eq!(txn.commit().expect("commit"), 2);
        drop(env);

        let env = open(&path);
        assert_eq!(env.stat().page_size, p);
        assert_eq!((env.stat().txn_id, env.stat().tables), (2, 2));
        let ro = env.begin_ro().expect("ro");
        let (a, b) = (table(&ro, "a"), table(&ro, "b"));
        let stat = ro.table_stat(&a).expect("stat");
        assert!(stat.height >= 2, "{p}: the tree has split ({stat:?})");
        assert_eq!(stat.entries, model.len() as u64);
        for (k, v) in &model {
            assert_eq!(val(&ro, &a, k).as_ref(), Some(v), "{p}");
        }
        assert_eq!(val(&ro, &a, b"k0000000"), None);
        assert_eq!(val(&ro, &b, b"only").as_deref(), Some(&b"one"[..]));
        drop(ro);
        assert_eq!(put_all(&env, "b", &[(b"more", b"two")]), 3);
        drop(env);
        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        assert_eq!(val(&ro, &table(&ro, "b"), b"more").as_deref(), Some(&b"two"[..]));
    }
}

/// A new file is built aside and renamed into place: a leftover `<path>.creating` is
/// overwritten and gone, and an empty file at the path is created like a missing one.
#[test]
fn creation_replaces_leftovers_and_empty_files() {
    let (_dir, path) = temp();
    let creating = path.with_file_name("db.creating");
    fs::write(&creating, vec![0xEE; 10_000]).expect("leftover");
    fs::write(&path, b"").expect("empty file");
    let env = open(&path);
    assert!(!creating.exists());
    assert_eq!(env.stat(), EnvStat { page_size: 4096, txn_id: 0, page_count: 2, tables: 0 });
    drop(env);
    let bytes = fs::read(&path).expect("read");
    assert_eq!(bytes.len(), 2 * 4096);
    assert_eq!(bytes[..META_LEN], bytes[4096..4096 + META_LEN], "both metas are txn 0");
    assert!(matches!(
        Env::open(&path, options(3000, SyncMode::Durable)),
        Err(Error::InvalidPageSize(3000))
    ));
}

/// A `create` that finds the file already created by another handle keeps that file and removes
/// the empty `<path>.creating` it opened. A leftover holding bytes stays, and a temporary file
/// another creator holds locked is `Locked` at once and left alone.
#[test]
fn create_removes_its_unused_temporary_file() {
    let (_dir, path) = temp();
    let creating = path.with_file_name("db.creating");
    drop(open(&path));
    let before = fs::read(&path).expect("read");
    create(&path, 4096).expect("create over a created file");
    assert!(!creating.exists(), "the unused temporary file is removed");
    assert!(fs::read(&path).expect("read") == before, "the created file is kept");
    fs::write(&creating, b"partial").expect("leftover");
    create(&path, 4096).expect("create over a created file");
    assert_eq!(fs::read(&creating).expect("read"), b"partial", "a non-empty leftover stays");
    fs::write(&creating, b"").expect("empty");
    let held = File::open(&creating).expect("open");
    assert!(sys::flock_exclusive(&held).expect("flock"));
    assert!(matches!(create(&path, 4096), Err(Error::Locked)));
    assert!(creating.exists(), "another creator's file stays");
    drop(held);
    create(&path, 4096).expect("create over a created file");
    assert!(!creating.exists());
    assert!(fs::read(&path).expect("read") == before);
}

/// The file is locked while any handle or transaction of an environment lives, against
/// another open in this process too.
#[test]
fn a_second_open_is_locked() {
    let (_dir, path) = temp();
    let env = open(&path);
    assert!(matches!(Env::open(&path, EnvOptions::default()), Err(Error::Locked)));
    let clone = env.clone();
    drop(env);
    assert!(matches!(Env::open(&path, EnvOptions::default()), Err(Error::Locked)));
    let ro = clone.begin_ro().expect("ro");
    drop(clone);
    assert!(
        matches!(Env::open(&path, EnvOptions::default()), Err(Error::Locked)),
        "a txn holds it"
    );
    drop(ro);
    let env = open(&path);
    assert!(matches!(Env::open(&path, EnvOptions::default()), Err(Error::Locked)));
    drop(env);
}

/// A reader keeps the state of every table it began on (changed, cleared, dropped, created)
/// across commits; a new reader sees the new state.
#[test]
fn readers_keep_their_snapshot_of_every_table() {
    let (_dir, path) = temp();
    let env = open(&path);
    let mut txn = env.begin_rw().expect("begin");
    let tables: Vec<Table> =
        ["a", "b", "c"].iter().map(|n| txn.create_table(n).expect("t")).collect();
    for (i, t) in tables.iter().enumerate() {
        for k in 0..50_u32 {
            txn.put(t, &k.to_be_bytes(), format!("{i}-{k}-old").as_bytes()).expect("put");
        }
    }
    txn.commit().expect("commit");

    let old = env.begin_ro().expect("ro");
    let mut txn = env.begin_rw().expect("begin");
    for k in 0..50_u32 {
        txn.put(&tables[0], &k.to_be_bytes(), b"new").expect("put");
    }
    txn.clear_table(&tables[1]).expect("clear");
    txn.drop_table(tables[2].clone()).expect("drop");
    let d = txn.create_table("d").expect("d");
    txn.put(&d, b"k", b"v").expect("put d");
    txn.commit().expect("commit");

    assert_eq!(old.txn_id(), 1);
    for (i, t) in tables.iter().enumerate() {
        assert_eq!(old.table_stat(t).expect("stat").entries, 50);
        for k in [0_u32, 25, 49] {
            assert_eq!(val(&old, t, &k.to_be_bytes()), Some(format!("{i}-{k}-old").into_bytes()));
        }
    }
    assert_eq!(old.open_table("d").expect("open"), None);
    assert!(
        matches!(old.get(&d, b"k"), Err(Error::TableNotFound(_))),
        "d is newer than the snapshot"
    );

    let new = env.begin_ro().expect("ro");
    assert_eq!(val(&new, &tables[0], &7_u32.to_be_bytes()).as_deref(), Some(&b"new"[..]));
    assert_eq!(new.table_stat(&tables[1]).expect("stat"), TableStat::default());
    assert!(matches!(new.get(&tables[2], b"k"), Err(Error::StaleTable(_))));
    assert_eq!(val(&new, &d, b"k").as_deref(), Some(&b"v"[..]));
    assert_eq!(env.failure(), None);
}

/// An aborted transaction does no I/O and leaves no trace: the metas and every page the
/// committed state may reach are byte-identical, the next transaction starts from the same page
/// count, and a reopen finds the committed state.
#[test]
fn an_abort_leaves_no_trace() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k1", b"v1"), (b"k2", b"v2")]);
    let stat = env.stat();
    let size = 4096_usize;
    let (pool, page_count) = env.with_writer(|w| (w.alloc.pool().to_vec(), w.alloc.page_count()));
    let committed = |bytes: &[u8]| -> Vec<u8> {
        let mut kept = bytes[..stat.page_count as usize * size].to_vec();
        for &p in &pool {
            kept[p as usize * size..(p as usize + 1) * size].fill(0);
        }
        kept
    };
    let before = committed(&fs::read(&path).expect("read"));

    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    let b = txn.create_table("b").expect("b");
    for i in 0..2000_u32 {
        txn.put(&b, &i.to_be_bytes(), &[7; 100]).expect("put");
    }
    assert!(txn.delete(&a, b"k1").expect("delete"));
    txn.put(&a, b"k2", b"changed").expect("put");
    txn.abort();

    assert_eq!(env.stat(), stat);
    assert!(
        committed(&fs::read(&path).expect("read")) == before,
        "an abort changed committed bytes"
    );
    assert_eq!(
        env.with_writer(|w| (w.alloc.pool().to_vec(), w.alloc.page_count())),
        (pool, page_count)
    );
    let ro = env.begin_ro().expect("ro");
    assert_eq!(ro.open_table("b").expect("open"), None);
    assert_eq!(val(&ro, &a, b"k1").as_deref(), Some(&b"v1"[..]));
    drop(ro);

    // Dropping a transaction aborts it too.
    let mut txn = env.begin_rw().expect("begin");
    txn.put(&a, b"k3", b"v3").expect("put");
    drop(txn);
    drop(env);
    let env = open(&path);
    assert_eq!(env.stat(), stat);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &a, b"k2").as_deref(), Some(&b"v2"[..]));
    assert_eq!(val(&ro, &a, b"k3"), None);
}

/// A damaged newest meta falls back exactly one txn, and the next commit overwrites the damaged
/// slot.
#[test]
fn a_damaged_newest_meta_falls_back_one_txn() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]); // txn 1: slot 1 (a tie goes to slot 1)
    put_all(&env, "a", &[(b"k", b"2")]); // txn 2: slot 0
    assert_eq!(env.with_writer(|w| w.slots), [Some(2), Some(1)]);
    drop(env);
    flip(&path, 20);

    let env = open(&path);
    assert_eq!(env.stat().txn_id, 1);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"1"[..]));
    drop(ro);
    assert_eq!(env.with_writer(|w| w.slots), [None, Some(1)]);
    assert_eq!(put_all(&env, "a", &[(b"k", b"3")]), 2);
    drop(env);
    let mut record = [0; META_LEN];
    fs::File::open(&path).expect("open").read_exact_at(&mut record, 0).expect("read");
    assert_eq!(Meta::decode(&record).expect("slot 0 rewritten").txn, 2);
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"3"[..]));
}

/// Both metas damaged, or a damaged tree root: the open fails closed and changes nothing.
#[test]
fn damaged_metas_and_roots_fail_closed() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]);
    put_all(&env, "a", &[(b"k", b"2")]);
    let catalog_root = env.inner().current().meta.catalog;
    assert_eq!((catalog_root.root_txn, catalog_root.height), (2, 1));
    drop(env);

    // The catalog root fails its CRC (a byte of its last record's value: the page's structure
    // still checks).
    let at = u64::from(catalog_root.root) * 4096 + 4096 - 16;
    flip(&path, at);
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(m) if m.contains("catalog root")));
    flip(&path, at);
    // The catalog root is a valid page of another txn.
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let mut page = vec![0; 4096];
    file.read_exact_at(&mut page, u64::from(catalog_root.root) * 4096).expect("read");
    let original = page.clone();
    page[8] ^= 0x02; // header txn 2 -> 0: no later than the meta's, but not the root's
    crc::add_crc32_nonzero(&mut page);
    file.write_all_at(&page, u64::from(catalog_root.root) * 4096).expect("write");
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(m) if m.contains("catalog root")));
    file.write_all_at(&original, u64::from(catalog_root.root) * 4096).expect("restore");
    drop(open(&path));
    // The newest meta says the catalog has two levels, but its root is a leaf. The counters
    // (one branch and two leaves at 56 and 60) keep the root's shape possible, so the meta
    // decodes and open's root check is what refuses it.
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, 0).expect("read");
    let original = record;
    record[40] = 2;
    record[56] = 1;
    record[60] = 2;
    let crc = crc::crc32(&record[..124]);
    record[124..].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&record, 0).expect("write");
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(m) if m.contains("wrong kind")));
    file.write_all_at(&original, 0).expect("restore");
    drop(open(&path));

    flip(&path, 20);
    flip(&path, 4096 + 20);
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(_)));
}

/// A file shorter than its meta's page count, or one that is not a tndb-b file, fails closed
/// with every byte unchanged.
#[test]
fn short_and_foreign_files_fail_closed() {
    let (_dir, path) = temp();
    let env = open(&path);
    let rows: Vec<(Vec<u8>, Vec<u8>)> =
        (0..200_u32).map(|i| (i.to_be_bytes().to_vec(), vec![1; 200])).collect();
    let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(&env, "a", &rows);
    let pages = env.stat().page_count;
    assert!(pages > 3);
    drop(env);
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open")
        .set_len(u64::from(pages - 1) * 4096)
        .expect("truncate");
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(m) if m.contains("shorter")));

    fs::write(&path, vec![0x5A; 3 * 4096]).expect("garbage");
    assert_fails_closed(&path, |e| matches!(e, Error::NotTndbB));
    fs::write(&path, b"hello").expect("tiny");
    assert_fails_closed(&path, |e| matches!(e, Error::NotTndbB));
}

/// A failed msync in a commit latches: the commit errs, later writes and syncs fail, reads
/// continue on the last published state, and a reopen finds that state.
#[test]
fn a_failed_msync_latches() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]);
    env.with_writer(|w| w.file.fail_next_msync());
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    txn.put(&a, b"k", b"2").expect("put");
    assert!(matches!(txn.commit(), Err(Error::Io(_))));
    assert!(env.failure().is_some_and(|c| c.contains("injected msync failure")));
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))));
    assert!(matches!(env.try_begin_rw(), Err(Error::Failed(_))));
    assert!(matches!(env.sync(), Err(Error::Failed(_))));
    let ro = env.begin_ro().expect("reads continue");
    assert_eq!(val(&ro, &a, b"k").as_deref(), Some(&b"1"[..]));
    assert_eq!(env.stat().txn_id, 1);
    drop(ro);
    drop(env);
    let env = open(&path);
    assert_eq!(env.failure(), None, "a reopen clears the latch");
    assert_eq!(env.stat().txn_id, 1);
}

/// A write that fails part way latches: the open transaction's later writes and its commit
/// fail, and in `NoSync` the environment's drop does not sync, so the unsynced commits are
/// lost as in a crash.
#[test]
fn a_latched_env_neither_writes_nor_syncs_on_drop() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    put_all(&env, "a", &[(b"k", b"1")]);
    let mut txn = env.begin_rw().expect("begin");
    let b = txn.create_table("b").expect("b");
    txn.writer_mut().alloc.fail_allocs_after = Some(0);
    assert!(matches!(txn.put(&b, b"k", b"v"), Err(Error::Io(_))));
    txn.writer_mut().alloc.fail_allocs_after = None;
    assert!(matches!(txn.put(&b, b"k", b"v"), Err(Error::Failed(_))));
    assert!(matches!(txn.delete(&b, b"k"), Err(Error::Failed(_))));
    assert!(matches!(txn.create_table("c"), Err(Error::Failed(_))));
    assert_eq!(val(&txn, &table(&txn, "a"), b"k").as_deref(), Some(&b"1"[..]), "reads continue");
    assert!(matches!(txn.commit(), Err(Error::Failed(_))));
    drop(env);
    let env = open(&path);
    assert_eq!(env.stat().txn_id, 0, "the unsynced commit was not synced by the drop");
}

/// `NoSync` commits are visible in-process at once; a crash rolls back to the last sync; a
/// sync and a clean drop make them durable.
#[test]
fn nosync_commits_are_durable_only_once_synced() {
    let (_dir, path) = temp();
    let nosync = options(4096, SyncMode::NoSync);
    let env = Env::open(&path, nosync).expect("open");
    put_all(&env, "a", &[(b"k", b"1")]);
    env.crash();
    let env = Env::open(&path, nosync).expect("reopen");
    assert_eq!(env.stat(), EnvStat { page_size: 4096, txn_id: 0, page_count: 2, tables: 0 });

    put_all(&env, "a", &[(b"k", b"1")]);
    put_all(&env, "a", &[(b"k", b"2")]);
    let ro = env.begin_ro().expect("ro");
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (2, Some(b"2".to_vec())));
    drop(ro);
    assert_eq!(env.with_writer(|w| w.slots), [Some(0), Some(0)], "no meta written");
    assert!(env.with_writer(|w| w.unsynced.count()) > 0);
    env.sync().expect("sync");
    assert_eq!(env.with_writer(|w| (w.slots, w.unsynced.count())), ([Some(0), Some(2)], 0));
    env.sync().expect("a sync with nothing pending");
    put_all(&env, "a", &[(b"k", b"3")]);
    env.crash();

    let env = Env::open(&path, nosync).expect("reopen");
    let ro = env.begin_ro().expect("ro");
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (2, Some(b"2".to_vec())));
    drop(ro);
    put_all(&env, "a", &[(b"k", b"4")]);
    drop(env); // a clean drop syncs
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (3, Some(b"4".to_vec())));
}

/// Through a power loss that keeps only flushed bytes: a `Durable` commit survives whole, and a
/// `NoSync` one only once synced.
#[test]
fn commits_survive_a_power_loss_once_durable() {
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let (_dir, path) = temp();
        let env = Env::open(&path, options(4096, sync)).expect("open");
        disk_model::track(&path);
        let rows: Vec<(Vec<u8>, Vec<u8>)> =
            (0..300_u32).map(|i| (i.to_be_bytes().to_vec(), vec![2; 150])).collect();
        let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        put_all(&env, "a", &rows);
        put_all(&env, "b", &rows[..100]);
        env.sync().expect("sync");
        put_all(&env, "a", &[(b"late", b"x")]);
        env.crash();
        disk_model::power_loss(&path);
        disk_model::untrack(&path);

        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let want_txn = if sync == SyncMode::Durable { 3 } else { 2 };
        assert_eq!(ro.txn_id(), want_txn, "{sync:?}");
        let (a, b) = (table(&ro, "a"), table(&ro, "b"));
        for (k, v) in &rows {
            assert_eq!(val(&ro, &a, k).as_deref(), Some(*v), "{sync:?}");
        }
        assert_eq!(ro.table_stat(&b).expect("stat").entries, 100);
        assert_eq!(val(&ro, &a, b"late").is_some(), sync == SyncMode::Durable);
    }
}

/// A commit whose pages are scattered over the file (copies taking pages that earlier commits
/// freed all over it), so they form more than one range, syncs its data with one `fdatasync` of
/// the file, and survives a power loss.
#[test]
fn a_scattered_commit_is_durable_through_one_fdatasync() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::Durable)).expect("open");
    disk_model::track(&path);
    let key = |i: u32| i.to_be_bytes();
    let rows: Vec<([u8; 4], Vec<u8>)> = (0..3000).map(|i| (key(i), vec![1; 150])).collect();
    let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(&env, "a", &rows);
    // Each round copies a leaf in every two and frees the old ones; later rounds reuse them.
    let mut round = 1;
    while !env.with_writer(|w| w.data_by_file) {
        round += 1;
        assert!(round < 20, "no commit's pages were scattered");
        let mut txn = env.begin_rw().expect("begin");
        let a = txn.create_table("a").expect("table");
        for i in (0..3000).step_by(40) {
            txn.put(&a, &key(i), &[round; 150]).expect("put");
        }
        txn.commit().expect("commit");
    }
    env.crash();
    disk_model::power_loss(&path);
    disk_model::untrack(&path);

    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    let a = table(&ro, "a");
    for i in 0..3000 {
        let want = if i % 40 == 0 { round } else { 1 };
        assert_eq!(val(&ro, &a, &key(i)), Some(vec![want; 150]), "key {i}");
    }
    drop(ro);
    env.check().expect("check");
}

/// A commit of a few pages that form two ranges (pages reused from the free list, and new ones
/// further on) in a file that did not grow syncs them all by its one `fdatasync`, and survives a
/// power loss: an `msync` of one of the ranges would lose the other, and no size sync covers it.
/// Every commit up to the first such one is followed by a power loss and a reopen, so the check
/// does not depend on which sync step 4 took.
#[test]
fn a_commit_of_two_ranges_survives_a_power_loss() {
    let (_dir, path) = temp();
    let mut env = Env::open(&path, options(4096, SyncMode::Durable)).expect("open");
    disk_model::track(&path);
    let key = |i: u32| i.to_be_bytes();
    let rows: Vec<([u8; 4], Vec<u8>)> = (0..3000).map(|i| (key(i), vec![1; 150])).collect();
    let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(&env, "a", &rows);
    // One put copies a leaf, the root, the catalog and the free list's records: too few pages
    // for more than 16 runs, so a commit synced by `fdatasync` is one whose pages form two or
    // more ranges.
    let changed = |round: u8| key(u32::from(round) * 37 % 3000);
    let mut round = 1;
    loop {
        round += 1;
        assert!(round < 50, "no commit's pages formed two ranges in a file that did not grow");
        let len = env.with_writer(|w| w.file.file_len());
        put_all(&env, "a", &[(&changed(round), &[round; 150])]);
        let two_ranges = env.with_writer(|w| w.data_by_file && w.file.file_len() == len);
        env.crash();
        disk_model::power_loss(&path);
        env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let a = table(&ro, "a");
        for r in 2..=round {
            assert_eq!(val(&ro, &a, &changed(r)), Some(vec![r; 150]), "round {round}: key of {r}");
        }
        drop(ro);
        env.check().expect("check");
        if two_ranges {
            break;
        }
    }
    env.crash();
    disk_model::untrack(&path);
}

/// Open makes the meta it reads durable: a meta that reached the page cache but not the disk
/// (its commit's msync never finished) survives a power loss once a later open has read it.
#[test]
fn open_makes_the_meta_it_reads_durable() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]); // txn 1: slot 1
    put_all(&env, "a", &[(b"k", b"2")]); // txn 2: slot 0
    drop(env);
    disk_model::track(&path);
    // Rewrite slot 1 as txn 3 naming txn 2's state, in the page cache only.
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, 0).expect("read");
    let meta = Meta { txn: 3, ..Meta::decode(&record).expect("txn 2") };
    file.write_all_at(&meta.encode(), 4096).expect("write");
    drop(file);

    assert_eq!(open(&path).stat().txn_id, 3);
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    assert_eq!(open(&path).stat().txn_id, 3, "the open synced the meta it chose");
}

/// Damage a read finds latches the environment: writes fail, other reads continue.
#[test]
fn damage_found_by_a_read_latches() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"v")]);
    put_all(&env, "b", &[(b"k", b"v")]);
    let snapshot = env.inner().current();
    let root = catalog::get(&*snapshot, &snapshot.meta.catalog, "a").expect("get").expect("a").root;
    drop(snapshot);
    // Zero the root leaf's CRC trailer through another descriptor: the mapping sees it.
    let file = fs::OpenOptions::new().write(true).open(&path).expect("open");
    file.write_all_at(&[0; 4], u64::from(root.root + 1) * 4096 - 4).expect("write");

    let ro = env.begin_ro().expect("ro");
    let (a, b) = (table(&ro, "a"), table(&ro, "b"));
    assert!(matches!(ro.get(&a, b"k"), Err(Error::Corrupt(_))));
    assert!(env.failure().is_some_and(|c| c.contains("zero CRC")));
    assert_eq!(val(&ro, &b, b"k").as_deref(), Some(&b"v"[..]), "other reads continue");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))));
}

/// `begin_rw` waits while another write transaction is open (on any thread), and a write
/// transaction can move to another thread.
#[test]
fn begin_rw_waits_for_the_open_writer() {
    let (_dir, path) = temp();
    let env = open(&path);
    let mut first = env.begin_rw().expect("begin");
    let a = first.create_table("a").expect("a");
    first.put(&a, b"k", b"v").expect("put");
    assert!(env.try_begin_rw().expect("try").is_none());

    let (tx, rx) = mpsc::channel();
    let waiter = {
        let env = env.clone();
        std::thread::spawn(move || {
            let txn = env.begin_rw().expect("begin");
            tx.send(txn.txn_id()).expect("send");
        })
    };
    assert!(rx.recv_timeout(Duration::from_millis(300)).is_err(), "the second writer must wait");
    let committer = std::thread::spawn(move || first.commit().expect("commit"));
    assert_eq!(committer.join().expect("join"), 1);
    assert_eq!(rx.recv_timeout(Duration::from_secs(30)).expect("the waiter proceeds"), 2);
    waiter.join().expect("join");
    assert!(env.try_begin_rw().expect("try").is_some());
}

/// The first page of each overflow run of a `len`-byte value in the file at `path` (a page whose
/// header names itself, kind 3 and that length), for a test to damage.
fn runs_of_len(path: &Path, page_size: usize, len: usize) -> Vec<usize> {
    let bytes = fs::read(path).expect("read");
    let u32_at = |off: usize| u32::from_le_bytes(bytes[off..off + 4].try_into().expect("4 bytes"));
    (2..bytes.len() / page_size)
        .filter(|&p| {
            let at = p * page_size;
            bytes[at + 4] == crate::overflow::KIND_RUN
                && u32_at(at) as usize == p
                && u32_at(at + 16) as usize == len
        })
        .collect()
}

/// A value of several pages put through a write transaction reads back byte for byte: from the
/// writer's own private run, from a snapshot of the commit, after a reopen, and from the next
/// writer (a published run); a replace by another multi-page value too. `Env::check` counts the
/// runs. A flipped byte in a run's middle page is then `Corrupt` to a reader, to the writer and
/// to `Env::check`.
#[test]
fn a_multi_page_value_reads_back_after_commit_and_reopen() {
    for p in PAGE_SIZES {
        let (_dir, path) = temp();
        let env = Env::open(&path, options(p, SyncMode::Durable)).expect("create");
        let value = |len: usize, salt: usize| -> Vec<u8> {
            (0..len).map(|i| ((i * 31 + salt) % 251) as u8).collect()
        };
        let (big, bigger) = (value(3 * p as usize + 123, 1), value(5 * p as usize - 28, 2));
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("t").expect("table");
        txn.put(&t, b"big", &big).expect("put");
        txn.put(&t, b"small", b"s").expect("put");
        assert_eq!(val(&txn, &t, b"big").as_ref(), Some(&big), "{p}: the private run");
        txn.commit().expect("commit");
        let ro = env.begin_ro().expect("ro");
        assert_eq!(val(&ro, &table(&ro, "t"), b"big").as_ref(), Some(&big), "{p}: a snapshot");
        drop(ro);
        drop(env);

        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let t = table(&ro, "t");
        assert_eq!(val(&ro, &t, b"big").as_ref(), Some(&big), "{p}: after a reopen");
        assert_eq!(ro.table_stat(&t).expect("stat").overflow_pages, 4, "{p}: a 4-page run");
        drop(ro);
        let mut txn = env.begin_rw().expect("begin");
        assert_eq!(val(&txn, &t, b"big").as_ref(), Some(&big), "{p}: the writer, published");
        txn.put(&t, b"big", &bigger).expect("replace");
        txn.commit().expect("commit");
        env.check().expect("check");
        drop(env);

        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let t = table(&ro, "t");
        assert_eq!(val(&ro, &t, b"big").as_ref(), Some(&bigger), "{p}: the replacement");
        assert_eq!(ro.table_stat(&t).expect("stat").overflow_pages, 5, "{p}: a 5-page run");
        drop(ro);
        drop(env);

        let [run] = runs_of_len(&path, p as usize, bigger.len())[..] else {
            panic!("{p}: one run of the replacement")
        };
        flip(&path, ((run + 2) * p as usize + 10) as u64);
        // Each probe on a fresh open: the damage it finds latches the environment.
        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let got = ro.get(&table(&ro, "t"), b"big").map(drop);
        assert!(matches!(got, Err(Error::Corrupt(_))), "{p}: a reader: {got:?}");
        drop((ro, env));
        let env = open(&path);
        assert!(matches!(env.check(), Err(Error::Corrupt(_))), "{p}: check");
        drop(env);
        let env = open(&path);
        let txn = env.begin_rw().expect("begin");
        let got = txn.get(&table(&txn, "t"), b"big").map(drop);
        assert!(matches!(got, Err(Error::Corrupt(_))), "{p}: the writer: {got:?}");
    }
}

/// Entries over the limits are refused without latching; entries at the limits fit.
#[test]
fn oversized_entries_do_not_latch() {
    let (_dir, path) = temp();
    let env = open(&path);
    let (max_key, max_value) = (env.max_key_len(), env.max_value_len(16));
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    let long_key = vec![1; max_key + 1];
    assert!(matches!(
        txn.put(&a, &long_key, b"v"),
        Err(Error::KeyTooLarge { len, max }) if len == max_key + 1 && max == max_key
    ));
    assert!(matches!(
        txn.put(&a, &[2; 16], &vec![0; max_value + 1]),
        Err(Error::ValueTooLarge { len, max }) if len == max_value + 1 && max == max_value
    ));
    assert_eq!(env.failure(), None);
    txn.put(&a, &vec![3; max_key], b"v").expect("the longest key");
    txn.put(&a, &[2; 16], &vec![0; max_value]).expect("the longest value");
    assert_eq!(txn.commit().expect("commit"), 1);
}

/// A handle of a dropped table is stale, even once the name is re-created (the new table has a
/// new id), in the same transaction and later ones.
#[test]
fn stale_handles_are_refused() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "x", &[(b"k", b"old")]);
    let old = table(&env.begin_ro().expect("ro"), "x");

    let mut txn = env.begin_rw().expect("begin");
    assert_eq!(txn.create_table("x").expect("open existing"), old);
    txn.put(&old, b"k2", b"changed before the drop").expect("put");
    txn.drop_table(old.clone()).expect("drop");
    assert!(matches!(txn.put(&old, b"k", b"v"), Err(Error::StaleTable(_))));
    let new = txn.create_table("x").expect("re-create");
    assert_ne!(new, old);
    assert!(matches!(txn.get(&old, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(txn.drop_table(old.clone()), Err(Error::StaleTable(_))));
    assert!(matches!(txn.clear_table(&old), Err(Error::StaleTable(_))));
    assert!(matches!(txn.delete(&old, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(txn.table_stat(&old), Err(Error::StaleTable(_))));
    assert_eq!(val(&txn, &new, b"k"), None, "the new table starts empty");
    txn.put(&new, b"k", b"new").expect("put");
    txn.commit().expect("commit");
    assert_eq!(env.failure(), None);

    let ro = env.begin_ro().expect("ro");
    assert!(matches!(ro.get(&old, b"k"), Err(Error::StaleTable(_))));
    assert_eq!(val(&ro, &new, b"k").as_deref(), Some(&b"new"[..]));
}

/// A transaction that changed nothing (an existing table opened, a missed delete, an empty
/// table cleared) commits as the state it began from, writing nothing.
#[test]
fn a_commit_of_no_change_writes_nothing() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"v")]);
    put_all(&env, "empty", &[]);
    let before = fs::read(&path).expect("read");
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    let empty = txn.create_table("empty").expect("empty");
    assert!(!txn.delete(&a, b"missing").expect("delete"));
    txn.clear_table(&empty).expect("clear");
    assert_eq!(txn.commit().expect("commit"), 2);
    assert_eq!(env.stat().txn_id, 2);
    assert_eq!(env.with_writer(|w| w.slots), [Some(2), Some(1)]);
    assert!(fs::read(&path).expect("read") == before, "a commit of no change wrote");
}

/// Table names are 1..=255 bytes of UTF-8; others are refused without latching.
#[test]
fn table_names_are_validated() {
    let (_dir, path) = temp();
    let env = open(&path);
    let mut txn = env.begin_rw().expect("begin");
    let too_long = "x".repeat(256);
    for bad in ["", too_long.as_str()] {
        assert!(matches!(txn.create_table(bad), Err(Error::InvalidTableName)));
        assert!(matches!(txn.open_table(bad), Err(Error::InvalidTableName)));
        assert!(matches!(
            env.begin_ro().expect("ro").open_table(bad),
            Err(Error::InvalidTableName)
        ));
    }
    let longest = "x".repeat(255);
    for name in [longest.as_str(), "表", "a"] {
        let t = txn.create_table(name).expect("valid name");
        assert_eq!(t.name(), name);
    }
    assert_eq!(txn.commit().expect("commit"), 1);
    assert_eq!((env.failure(), env.stat().tables), (None, 3));
}

/// The oldest live reader's txn follows the snapshots readers hold (the current one at least),
/// and the queue of published snapshots stays bounded while a reader is stuck on an old one.
#[test]
fn oldest_reader_txn_tracks_live_snapshots() {
    let (_dir, path) = temp();
    let env = open(&path);
    let oldest = |env: &Env| env.with_writer(|w| w.oldest_reader_txn());
    assert_eq!(oldest(&env), 0);
    let r0 = env.begin_ro().expect("ro");
    put_all(&env, "a", &[(b"k", b"1")]);
    put_all(&env, "a", &[(b"k", b"2")]);
    assert_eq!(oldest(&env), 0);
    let r2 = env.begin_ro().expect("ro");
    drop(r0);
    assert_eq!(oldest(&env), 2);
    put_all(&env, "a", &[(b"k", b"3")]);
    assert_eq!(oldest(&env), 2);
    drop(r2);
    assert_eq!(oldest(&env), 3);

    let stuck = env.begin_ro().expect("ro");
    for i in 0..200_u32 {
        put_all(&env, "a", &[(b"k", &i.to_be_bytes())]);
    }
    assert_eq!(oldest(&env), 3);
    let queued = env.with_writer(|w| w.readers.len());
    assert!(queued <= 2 * 16 + 1, "the queue holds {queued} snapshots");
    drop(stuck);
    assert_eq!(oldest(&env), 203);
}

/// The oldest reader's age counts from when a commit superseded the oldest snapshot a live reader
/// holds: `None` for readers of the current snapshot, the older reader's age with two, and
/// `None` again once it ends and the next commit runs. A read-only environment never has one.
#[test]
fn oldest_reader_age_follows_the_oldest_superseded_reader() {
    const PAUSE: Duration = Duration::from_millis(50);
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    assert_eq!(env.oldest_reader_age(), None);
    put_all(&env, "a", &[(b"k", b"0")]);
    assert_eq!(env.oldest_reader_age(), None, "no reader");

    // A reader of the current snapshot does not count until a commit supersedes it, and its
    // age starts then, not when its snapshot was published.
    let r1 = env.begin_ro().expect("ro");
    std::thread::sleep(PAUSE);
    assert_eq!(env.oldest_reader_age(), None, "r1 reads the current snapshot");
    let before = Instant::now();
    put_all(&env, "a", &[(b"k", b"1")]);
    let first = env.oldest_reader_age().expect("r1's snapshot is superseded");
    let bound = before.elapsed();
    assert!(first <= bound, "counted from the commit, not the publish: {first:?} > {bound:?}");

    // The age grows while r1 lives, across commits; with a second reader it is r1's.
    std::thread::sleep(PAUSE);
    let r2 = env.begin_ro().expect("ro");
    let r2_superseded = Instant::now();
    put_all(&env, "a", &[(b"k", b"2")]);
    let both = env.oldest_reader_age().expect("two superseded readers");
    assert!(both >= first + PAUSE, "r1's age grows: {first:?} then {both:?}");

    // Once r1 ends, the next commit makes r2 the oldest, superseded at the commit after it began.
    drop(r1);
    put_all(&env, "a", &[(b"k", b"3")]);
    let second = env.oldest_reader_age().expect("r2's snapshot is superseded");
    let bound = r2_superseded.elapsed();
    assert!(second <= bound, "r2's age, from the commit after it began: {second:?} > {bound:?}");

    // With no reader left, the next commit clears it.
    drop(r2);
    assert!(env.oldest_reader_age().is_some(), "refreshed only at a commit");
    put_all(&env, "a", &[(b"k", b"4")]);
    assert_eq!(env.oldest_reader_age(), None);
    let r3 = env.begin_ro().expect("ro");
    assert_eq!(env.oldest_reader_age(), None, "r3 reads the current snapshot");
    drop(r3);
    drop(env);

    let env = Env::open_read_only(&path, EnvOptions::default()).expect("open read-only");
    let _reader = env.begin_ro().expect("ro");
    assert_eq!(env.oldest_reader_age(), None, "a read-only environment never commits");
}

/// The handles and transactions can be shared and sent across threads.
#[test]
fn handles_are_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn clone<T: Clone>() {}
    send_sync::<Env>();
    send_sync::<RoTxn>();
    send_sync::<RwTxn>();
    clone::<Env>();
    send_sync::<Table>();
}

/// Dirty pages merge across gaps of up to 64 clean pages: one merged range of at most 16 runs of
/// adjacent pages is synced by one `msync`, and anything else by one `fdatasync` of the file.
#[test]
fn dirty_pages_form_one_range_or_take_one_fdatasync() {
    let p = 4096;
    assert_eq!(data_sync(&[], p), DataSync::Nothing);
    assert_eq!(data_sync(&[7], p), DataSync::Range(7 * p..8 * p));
    assert_eq!(data_sync(&[2, 3, 4], p), DataSync::Range(2 * p..5 * p));
    assert_eq!(data_sync(&[2, 67], p), DataSync::Range(2 * p..68 * p), "a gap of 64 merges");
    assert_eq!(data_sync(&[2, 68], p), DataSync::File, "65 does not: two ranges");
    let apart: Vec<u32> = (0..3).map(|i| 2 + i * 100).collect();
    assert_eq!(data_sync(&apart, p), DataSync::File, "three ranges");
    // Runs that merge into one range still count one by one.
    let combed: Vec<u32> = (0..16).flat_map(|i| [2 + i * 3, 3 + i * 3]).collect();
    let one = DataSync::Range(2 * p..49 * p);
    assert_eq!(data_sync(&combed, p), one, "16 runs one page apart");
    let combed: Vec<u32> = (0..17).map(|i| 2 + i * 2).collect();
    assert_eq!(data_sync(&combed, p), DataSync::File, "17 runs one page apart");
}

/// A table record a transaction remembers serves only the handle it was resolved for: not a
/// dropped table's handle under the same name, and not another file's table with the same id.
#[test]
fn remembered_records_serve_only_their_own_handle() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "x", &[(b"k", b"old")]);
    let old = table(&env.begin_ro().expect("ro"), "x");
    let mut txn = env.begin_rw().expect("begin");
    txn.drop_table(old.clone()).expect("drop");
    let new = txn.create_table("x").expect("re-create");
    txn.put(&new, b"k", b"new").expect("put");
    txn.commit().expect("commit");

    let (_other_dir, other_path) = temp();
    let other = open(&other_path);
    put_all(&other, "y", &[(b"k", b"y")]);
    put_all(&other, "z", &[(b"k", b"z")]);
    let z = table(&other.begin_ro().expect("ro"), "z");
    assert_eq!(z.id(), new.id(), "the same id in two files");

    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &new, b"k").as_deref(), Some(&b"new"[..]));
    assert!(matches!(ro.get(&old, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(ro.get(&z, b"k"), Err(Error::StaleTable(_))));
    drop(ro);

    let mut txn = env.begin_rw().expect("begin");
    txn.put(&new, b"k2", b"v").expect("put");
    assert!(matches!(txn.get(&z, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(txn.put(&z, b"k", b"v"), Err(Error::StaleTable(_))));
    assert!(matches!(txn.put(&old, b"k", b"v"), Err(Error::StaleTable(_))));
    assert_eq!(env.failure(), None);
}

#[test]
fn free_pages_counts_the_pages_the_free_list_holds() {
    let (_dir, path) = temp();
    let env = open(&path);
    assert_eq!(env.free_pages().unwrap(), env.check().unwrap().free_pages);
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        (0u32..3000).map(|i| (i.to_be_bytes().to_vec(), vec![7u8; 100])).collect();
    let refs: Vec<(&[u8], &[u8])> =
        entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    put_all(&env, "t", &refs);
    let mut txn = env.begin_rw().unwrap();
    let t = table(&txn, "t");
    for (k, _) in &entries {
        assert!(txn.delete(&t, k).unwrap());
    }
    txn.commit().unwrap();
    let report = env.check().unwrap();
    assert!(report.free_pages > report.free_records, "{report:?}");
    assert_eq!(env.free_pages().unwrap(), report.free_pages);
}

/// Test-only: the handles read-only transactions on this thread resolved through the catalog
/// (debug builds only; 0 in release, where the counter does not exist).
fn catalog_resolves() -> u64 {
    #[cfg(debug_assertions)]
    return crate::txn::CATALOG_RESOLVES.with(std::cell::Cell::get);
    #[cfg(not(debug_assertions))]
    0
}

/// Every read-only transaction on one snapshot shares its resolved table roots: a handle goes
/// through the catalog once per snapshot, whichever read uses it (get, both cursors, stat), and
/// again in the snapshot of the next commit.
#[test]
fn a_snapshot_resolves_each_handle_once() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "x", &[(b"k", b"one")]);
    put_all(&env, "y", &[(b"k", b"y")]);
    let x = table(&env.begin_ro().expect("ro"), "x");
    let start = catalog_resolves();
    let resolved = || catalog_resolves() - start;

    let first = env.begin_ro().expect("ro");
    assert_eq!(val(&first, &x, b"k").as_deref(), Some(&b"one"[..]));
    assert_eq!(val(&first, &x, b"k").as_deref(), Some(&b"one"[..]));
    let second = Arc::new(env.begin_ro().expect("ro"));
    assert_eq!(second.txn_id(), first.txn_id(), "one snapshot");
    assert_eq!(val(&*second, &x, b"k").as_deref(), Some(&b"one"[..]));
    assert_eq!(
        second.cursor(&x).expect("cursor").first().expect("first").map(|e| e.1),
        Some(&b"one"[..])
    );
    let mut owned = second.cursor_owned(&x).expect("owned cursor");
    assert_eq!(owned.seek_exact(b"k").expect("seek"), Some(&b"one"[..]));
    assert_eq!(second.table_stat(&x).expect("stat").entries, 1);
    if cfg!(debug_assertions) {
        assert_eq!(resolved(), 1, "two transactions on one snapshot resolve the handle once");
    }

    put_all(&env, "x", &[(b"k", b"two")]);
    let after = env.begin_ro().expect("ro");
    assert_ne!(after.txn_id(), first.txn_id());
    assert_eq!(val(&after, &x, b"k").as_deref(), Some(&b"two"[..]));
    assert_eq!(val(&first, &x, b"k").as_deref(), Some(&b"one"[..]), "the old snapshot reads on");
    if cfg!(debug_assertions) {
        assert_eq!(resolved(), 2, "the commit's snapshot resolves the handle again");
    }
}

/// A snapshot's resolved roots live and die with it: after a table is dropped and re-created,
/// the next snapshot refuses the old handle (`StaleTable`), while transactions on the old
/// snapshot still read the old table through it, and refuse the new one (`TableNotFound`),
/// whether they resolved the handle before the commit or only after it.
#[test]
fn a_dropped_tables_handle_is_stale_only_in_later_snapshots() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "x", &[(b"k", b"old")]);
    let before = env.begin_ro().expect("ro");
    let late = env.begin_ro().expect("ro");
    let old = table(&before, "x");
    assert_eq!(val(&before, &old, b"k").as_deref(), Some(&b"old"[..]));

    let mut txn = env.begin_rw().expect("begin");
    txn.drop_table(old.clone()).expect("drop");
    let new = txn.create_table("x").expect("re-create");
    txn.put(&new, b"k", b"new").expect("put");
    txn.commit().expect("commit");

    let ro = env.begin_ro().expect("ro");
    assert!(matches!(ro.get(&old, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(ro.cursor(&old), Err(Error::StaleTable(_))));
    assert_eq!(val(&ro, &new, b"k").as_deref(), Some(&b"new"[..]));
    assert!(matches!(env.begin_ro().expect("ro").get(&old, b"k"), Err(Error::StaleTable(_))));

    for txn in [&before, &late] {
        assert_eq!(val(txn, &old, b"k").as_deref(), Some(&b"old"[..]));
        assert!(matches!(txn.get(&new, b"k"), Err(Error::TableNotFound(_))));
    }
    assert_eq!(env.failure(), None);
}

/// A remembered root serves only a handle with its name as well as its id. A table is never
/// renamed and a committed table's id is never given again, yet two handles can carry one id
/// under two names: one of another file, and in one file one of a table an aborted transaction
/// created, as `RwTxn::new` starts from the committed `next_table_id`, so the next table created
/// takes that id. Neither reaches the table holding the id, in a read or a write transaction.
#[test]
fn a_remembered_root_needs_the_handles_name_too() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "x", &[(b"k", b"x")]);
    let (_other_dir, other_path) = temp();
    let other = open(&other_path);
    put_all(&other, "z", &[(b"k", b"z")]);
    let z = table(&other.begin_ro().expect("ro"), "z");

    let ro = env.begin_ro().expect("ro");
    let x = table(&ro, "x");
    assert_eq!(z.id(), x.id(), "the same id in two files");
    assert_eq!(val(&ro, &x, b"k").as_deref(), Some(&b"x"[..]));
    let start = catalog_resolves();
    for txn in [&ro, &env.begin_ro().expect("ro")] {
        assert!(matches!(txn.get(&z, b"k"), Err(Error::StaleTable(_))));
        assert!(matches!(txn.table_stat(&z), Err(Error::StaleTable(_))));
    }
    // A handle opened again (its own name allocation) is the same table, so it hits.
    let again = table(&env.begin_ro().expect("ro"), "x");
    assert_eq!(val(&env.begin_ro().expect("ro"), &again, b"k").as_deref(), Some(&b"x"[..]));
    if cfg!(debug_assertions) {
        assert_eq!(catalog_resolves() - start, 4, "only the refused handle went to the catalog");
    }

    // In one file: an aborted create's id goes to the next table created, "b".
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("create");
    txn.abort();
    put_all(&env, "b", &[(b"k", b"b")]);
    let ro = env.begin_ro().expect("ro");
    let b = table(&ro, "b");
    assert_eq!(a.id(), b.id(), "the same id in one file");
    assert_eq!(val(&ro, &b, b"k").as_deref(), Some(&b"b"[..]));
    for txn in [&ro, &env.begin_ro().expect("ro")] {
        assert!(matches!(txn.get(&a, b"k"), Err(Error::StaleTable(_))));
        assert!(matches!(txn.table_stat(&a), Err(Error::StaleTable(_))));
    }
    // A write transaction holding b's working record refuses the handle too, and b is unchanged.
    let mut txn = env.begin_rw().expect("begin");
    txn.put(&b, b"j", b"b").expect("put");
    assert!(matches!(txn.get(&a, b"k"), Err(Error::StaleTable(_))));
    assert!(matches!(txn.put(&a, b"k", b"a"), Err(Error::StaleTable(_))));
    assert_eq!(val(&txn, &b, b"k").as_deref(), Some(&b"b"[..]));
    txn.abort();
    assert_eq!(env.failure(), None);
}

/// More tables than a snapshot remembers: the first 32 resolved take a slot each (some past
/// their first, wrapping), and the rest resolve through the catalog on every use, each to its own
/// table.
#[test]
fn more_tables_than_a_snapshot_remembers_still_resolve() {
    let (_dir, path) = temp();
    let env = open(&path);
    let names: Vec<String> = (0..40).map(|i| format!("t{i:02}")).collect();
    let mut txn = env.begin_rw().expect("begin");
    for name in &names {
        let t = txn.create_table(name).expect("table");
        txn.put(&t, b"k", name.as_bytes()).expect("put");
    }
    txn.commit().expect("commit");

    let ro = env.begin_ro().expect("ro");
    let mut tables: Vec<Table> = names.iter().map(|name| table(&ro, name)).collect();
    // The last 8 first: their ids take the first slots of the first 8, which then probe on.
    tables.rotate_left(32);
    let start = catalog_resolves();
    for txn in [&ro, &ro, &env.begin_ro().expect("ro")] {
        for t in &tables {
            assert_eq!(val(txn, t, b"k").as_deref(), Some(t.name().as_bytes()), "{}", t.name());
        }
    }
    if cfg!(debug_assertions) {
        assert_eq!(catalog_resolves() - start, 40 + 8 + 8, "32 remembered, 8 resolved each time");
    }
}

/// `EnvOptions::readahead` defaults to `false` on Linux and `true` elsewhere, and reaches every
/// mapping an environment makes: without it `Env::open`'s and each remap's past the reservation
/// are advised `MADV_RANDOM`, as is `Env::open_read_only`'s; with it, none is advised.
#[test]
fn every_mapping_follows_the_readahead_option() {
    assert_eq!(
        EnvOptions::default().readahead,
        !cfg!(target_os = "linux"),
        "random access is the default on Linux only"
    );
    for (readahead, want) in [(false, Some(memmap2::Advice::Random)), (true, None)] {
        let (_dir, path) = temp();
        // A 16 KiB reservation: the commit's first growth (to 1 MiB) remaps.
        let options = EnvOptions { map_reserve: 16 << 10, readahead, ..EnvOptions::default() };
        crate::file::take_advice_log();
        let env = Env::open(&path, options).expect("open");
        put_all(&env, "t", &[(b"k", &[7; 64])]);
        let log = crate::file::take_advice_log();
        assert!(log.len() >= 2, "readahead {readahead}: the open's mapping and a remap: {log:?}");
        assert!(log.iter().all(|a| *a == want), "readahead {readahead}: {log:?}");
        drop(env);
        let ro = Env::open_read_only(&path, EnvOptions { readahead, ..EnvOptions::default() })
            .expect("open read-only");
        assert_eq!(crate::file::take_advice_log(), [want], "readahead {readahead}: read-only");
        drop(ro);
    }
}

/// The `VmFlags` of each mapping of `path` in `smaps` (the format of Linux's `/proc/self/smaps`),
/// in address order. A mapping's header line starts with its address range (`lo-hi`, in hex) and
/// ends with the path it maps; its `VmFlags:` line lists two-letter flags (`rr`: random read,
/// `MADV_RANDOM`).
fn vm_flags_of<'a>(smaps: &'a str, path: &Path) -> Vec<Vec<&'a str>> {
    let path = path.to_str().expect("a UTF-8 path");
    let mut flags = Vec::new();
    let mut in_path = false;
    for line in smaps.lines() {
        let mut words = line.split_whitespace();
        let first = words.next().unwrap_or_default();
        if first.contains('-') && first.chars().all(|c| c == '-' || c.is_ascii_hexdigit()) {
            // A header: range, perms, offset, device, inode, then the path (if any).
            in_path = words.nth(4) == Some(path);
        } else if in_path && first == "VmFlags:" {
            flags.push(words.collect());
        }
    }
    flags
}

/// [`vm_flags_of`] picks the `VmFlags` of exactly the mappings of the path, past other mappings'
/// lines and its own other fields.
#[test]
fn vm_flags_of_reads_each_mapping_of_the_path() {
    let smaps = "\
7f0000000000-7f0000100000 rw-s 00000000 103:03 4242                       /tmp/x/db
Size:               1024 kB
VmFlags: rd wr sh mr mw me ms rr sd
7f0000200000-7f0000201000 r--p 00000000 103:03 99                         /usr/lib/libc.so
VmFlags: rd mr mw me sd
7f0000300000-7f0000500000 rw-s 00000000 103:03 4242                       /tmp/x/db
Rss:                   8 kB
VmFlags: rd wr sh mr mw me ms sd
7f0000600000-7f0000601000 rw-p 00000000 00:00 0
VmFlags: rd wr mr mw me ac sd
";
    let flags = vm_flags_of(smaps, Path::new("/tmp/x/db"));
    assert_eq!(
        flags,
        [
            vec!["rd", "wr", "sh", "mr", "mw", "me", "ms", "rr", "sd"],
            vec!["rd", "wr", "sh", "mr", "mw", "me", "ms", "sd"],
        ]
    );
    assert!(vm_flags_of(smaps, Path::new("/tmp/x/other")).is_empty());
}

/// On Linux the kernel holds the advice: every mapping of the file, the writer's (the current and
/// the retired ones, after a remap past the reservation) and a read-only environment's, carries
/// `rr` (`VM_RAND_READ`) in `/proc/self/smaps` by default, and none does with `readahead`.
#[cfg(target_os = "linux")]
#[test]
fn every_mapping_carries_the_random_read_flag_on_linux() {
    for readahead in [false, true] {
        let (dir, _) = temp();
        let path = dir.path().canonicalize().expect("canonical temp dir").join("db");
        let options = EnvOptions { map_reserve: 16 << 10, readahead, ..EnvOptions::default() };
        let env = Env::open(&path, options).expect("open");
        put_all(&env, "t", &[(b"k", &[7; 64])]);
        let smaps = fs::read_to_string("/proc/self/smaps").expect("read smaps");
        let flags = vm_flags_of(&smaps, &path);
        assert!(flags.len() >= 2, "readahead {readahead}: the current and a retired mapping");
        for f in &flags {
            assert_eq!(f.contains(&"rr"), !readahead, "readahead {readahead}: {f:?}");
        }
        drop(env);
        let ro = Env::open_read_only(&path, EnvOptions { readahead, ..EnvOptions::default() })
            .expect("open read-only");
        let smaps = fs::read_to_string("/proc/self/smaps").expect("read smaps");
        let flags = vm_flags_of(&smaps, &path);
        assert_eq!(flags.len(), 1, "readahead {readahead}: one read-only mapping");
        assert_eq!(flags[0].contains(&"rr"), !readahead, "readahead {readahead}: {:?}", flags[0]);
        drop(ro);
    }
}

/// A value of `len` bytes for the stamp tests' key `i`.
fn stamp_value(i: u32, len: usize) -> Vec<u8> {
    (0..len).map(|j| (j as u32).wrapping_mul(31).wrapping_add(i.wrapping_mul(7)) as u8).collect()
}

/// What one commit of [`stamp_workload`] did in step 3.
#[derive(Debug)]
struct Stamped {
    /// The most threads one of its CRC computations took (`Writer::stamped_on`).
    on: usize,
    /// Its writeback starts (`Writer::writeback_log`), in order.
    spans: Vec<WritebackSpan>,
    /// Its counters (`IoStats::since` the commit before).
    stats: IoStats,
}

/// Commit `txn`, a transaction of `env`, whose writer logs its writeback starts, and return what
/// its step 3 did.
fn commit_stamped(env: &Env, txn: RwTxn) -> Stamped {
    let before = env.io_stats();
    txn.commit().expect("commit");
    let (on, spans) = env.with_writer(|w| {
        let spans = w.writeback_log.get_mut().replace(Vec::new()).expect("the log is on");
        (w.stamped_on, spans)
    });
    Stamped { on, spans, stats: env.io_stats().since(&before) }
}

/// Run the stamp tests' commits on a new file at `path`, with commit step 3's CRCs computed on
/// `threads` threads (`Writer::stamp_threads`; `None`: `stamp::threads`' choice) and each
/// commit's step 3 sliced at `slice` bytes (`Writer::writeback_slice`; `None`: `Writer::slicing`'s
/// choice), then reopen it, check it and read every value back. The commits stamp tree pages
/// and overflow runs of 1 to 10 pages, replace values in both directions and delete some; `big`
/// adds a commit of 3 MiB of runs, past `stamp::threads`' threshold. Returns what each commit's
/// step 3 did.
fn stamp_workload(
    path: &Path,
    threads: Option<usize>,
    slice: Option<usize>,
    big: bool,
) -> Vec<Stamped> {
    const LENS: [usize; 6] = [100, 3000, 5000, 9000, 20_000, 37_000];
    let env = open(path);
    env.with_writer(|w| {
        w.stamp_threads = threads;
        w.writeback_slice = slice;
        *w.writeback_log.get_mut() = Some(Vec::new());
    });
    let mut model = BTreeMap::new();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("s").expect("table");
    for i in 0u32..60 {
        let v = stamp_value(i, LENS[i as usize % 6] + i as usize);
        txn.put(&t, &i.to_be_bytes(), &v).expect("put");
        model.insert(i, v);
    }
    let mut stamped = vec![commit_stamped(&env, txn)];
    let mut txn = env.begin_rw().expect("begin");
    for i in 0u32..60 {
        if i % 5 == 0 {
            assert!(txn.delete(&t, &i.to_be_bytes()).expect("delete"));
            model.remove(&i);
        } else if i % 3 == 0 {
            let v = stamp_value(i + 100, LENS[(i as usize + 3) % 6]);
            txn.put(&t, &i.to_be_bytes(), &v).expect("put");
            model.insert(i, v);
        }
    }
    stamped.push(commit_stamped(&env, txn));
    if big {
        let mut txn = env.begin_rw().expect("begin");
        for i in 100u32..148 {
            let v = stamp_value(i, 64 << 10);
            txn.put(&t, &i.to_be_bytes(), &v).expect("put");
            model.insert(i, v);
        }
        stamped.push(commit_stamped(&env, txn));
    }
    env.check().expect("check");
    drop(env);
    let env = open(path);
    env.check().expect("check after reopen");
    let txn = env.begin_ro().expect("begin");
    let t = table(&txn, "s");
    for (i, v) in &model {
        assert_eq!(val(&txn, &t, &i.to_be_bytes()).as_ref(), Some(v), "key {i}");
    }
    stamped
}

/// The threads each of `stamped`'s commits took, asserting that none started a writeback.
fn unsliced(stamped: &[Stamped]) -> Vec<usize> {
    for s in stamped {
        assert!(s.spans.is_empty() && s.stats.writeback_starts == 0, "no writeback start: {s:?}");
    }
    stamped.iter().map(|s| s.on).collect()
}

#[test]
fn a_parallel_stamp_writes_the_serial_stamps_bytes() {
    // The same commits, their CRCs computed on 2, 3 and 5 threads, leave the file the serial
    // loop leaves, byte for byte, which reopens, checks and reads back.
    let (_serial_dir, serial) = temp();
    assert_eq!(unsliced(&stamp_workload(&serial, Some(1), None, false)), [1, 1]);
    let serial = fs::read(&serial).expect("read");
    for threads in [2, 3, 5] {
        let (_dir, path) = temp();
        let on = unsliced(&stamp_workload(&path, Some(threads), None, false));
        assert_eq!(on, [threads, threads]);
        assert!(fs::read(&path).expect("read") == serial, "{threads} threads");
    }
}

#[test]
fn a_commit_past_the_stamp_threshold_writes_the_serial_stamps_bytes() {
    // A commit of 3 MiB of runs takes several threads by `stamp::threads`' own choice, unless
    // the process may use one CPU only, and leaves the file the serial loop leaves.
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let (_serial_dir, serial) = temp();
    assert_eq!(unsliced(&stamp_workload(&serial, Some(1), None, true)), [1, 1, 1]);
    let (_dir, path) = temp();
    let on = unsliced(&stamp_workload(&path, None, None, true));
    assert_eq!(on[..2], [1, 1], "the small commits stamp serially");
    assert!(cpus == 1 || on[2] > 1, "the 3 MiB commit takes several threads: {on:?}");
    assert!(fs::read(&path).expect("read") == fs::read(&serial).expect("read"));
}

#[test]
fn a_sliced_stamp_writes_the_serial_stamps_bytes() {
    // The same commits, step 3 sliced at one page, at a little over three pages, at 64 KiB and
    // at 1 MiB, each slice's CRCs on 1 and on 3 threads, leave the file the serial loop leaves,
    // byte for byte; every commit took slices.
    let (_serial_dir, serial) = temp();
    assert_eq!(unsliced(&stamp_workload(&serial, Some(1), None, true)), [1, 1, 1]);
    let serial = fs::read(&serial).expect("read");
    for slice in [4096, 3 * 4096 + 1, 64 << 10, 1 << 20] {
        for threads in [1, 3] {
            let (_dir, path) = temp();
            let stamped = stamp_workload(&path, Some(threads), Some(slice), true);
            for s in &stamped {
                assert_eq!(s.on, threads, "slice {slice}, {threads} threads");
                assert!(!s.spans.is_empty(), "slice {slice}, {threads} threads: sliced");
            }
            let spans = stamped[2].spans.len();
            assert!(slice > 64 << 10 || spans > 10, "slice {slice}: {spans} slices");
            assert!(fs::read(&path).expect("read") == serial, "slice {slice}, {threads} threads");
        }
    }
}

/// Check the writeback starts of one sliced commit cut at `slice` bytes: each covers exactly its
/// slice, from its first unit's first page to its last unit's end, and was made once every unit
/// of the slice held its CRC; the slices follow one another in page order, each closing at the
/// first unit boundary from `slice` bytes (the last at the end), and their units' pages are
/// distinct and as many as the commit synced, so every page the commit stamped lies in exactly
/// one started span. On Linux each start is counted, elsewhere none.
fn assert_spans(stamped: &Stamped, slice: usize, page_size: u64) {
    let Stamped { spans, stats, .. } = stamped;
    assert!(!spans.is_empty(), "the commit took slices");
    let mut pages = std::collections::BTreeSet::new();
    let mut end = 0;
    for (i, span) in spans.iter().enumerate() {
        assert!(span.stamped, "span {i} started before its slice's CRCs were stored: {span:?}");
        let (first, _) = span.units[0];
        let (last, n) = span.units[span.units.len() - 1];
        let bytes = u64::from(first) * page_size..u64::from(last + n) * page_size;
        assert_eq!(span.bytes, bytes, "span {i} covers its slice");
        assert!(span.bytes.start >= end, "span {i} follows the one before");
        end = span.bytes.end;
        let sizes: Vec<usize> =
            span.units.iter().map(|&(_, n)| n as usize * page_size as usize).collect();
        let total: usize = sizes.iter().sum();
        let closed = total - sizes[sizes.len() - 1];
        assert!(closed < slice, "span {i}: the slice closes at the first boundary from {slice}");
        assert!(i + 1 == spans.len() || total >= slice, "span {i}: only the last is short");
        for &(p, n) in &span.units {
            for q in p..p + n {
                assert!(pages.insert(q), "page {q} in two spans");
            }
        }
    }
    assert_eq!(pages.len() as u64, stats.pages_synced, "every page stamped is in a span");
    let counted = if cfg!(target_os = "linux") { spans.len() as u64 } else { 0 };
    assert_eq!(stats.writeback_starts, counted, "{stats:?}");
    assert!(stats.writeback_ns <= stats.stamp_ns, "{stats:?}");
}

#[test]
fn every_stamped_page_is_in_exactly_one_started_span() {
    // Each commit of the stamp workload, sliced at one page, at a little over three pages and at
    // 64 KiB, its CRCs on 1 and on 3 threads, started one writeback per slice, each over its
    // slice alone and only after its CRCs were stored, every page it stamped in one of them.
    for slice in [4096, 3 * 4096 + 1, 64 << 10] {
        for threads in [1, 3] {
            let (_dir, path) = temp();
            for stamped in stamp_workload(&path, Some(threads), Some(slice), true) {
                assert_spans(&stamped, slice, 4096);
            }
        }
    }
}

#[test]
fn nosync_and_small_commits_start_no_writeback() {
    // Step 3 is sliced only for a durable commit of `WRITEBACK_MIN` bytes of units or more, in
    // slices of `WRITEBACK_SLICE`; the test knob slices every durable commit, never a `NoSync`
    // one. A `NoSync` environment with the knob set starts no writeback, at its commits or at
    // the sync that makes them durable; the stamp workload's durable commits, all below the
    // threshold, start none either (`unsliced`, in the tests above).
    let (_dir, path) = temp();
    let env = open(&path);
    env.with_writer(|w| {
        assert_eq!(w.slicing(true, WRITEBACK_MIN - 1), None, "below the threshold");
        assert_eq!(w.slicing(true, WRITEBACK_MIN), Some(WRITEBACK_SLICE), "from the threshold");
        assert_eq!(w.slicing(false, usize::MAX), None, "a NoSync commit");
        w.writeback_slice = Some(4096);
        assert_eq!(w.slicing(true, 0), Some(4096), "the knob, durable");
        assert_eq!(w.slicing(false, usize::MAX), None, "the knob, NoSync");
    });
    drop(env);
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    env.with_writer(|w| {
        w.writeback_slice = Some(4096);
        *w.writeback_log.get_mut() = Some(Vec::new());
    });
    let before = env.io_stats();
    for round in 0u32..3 {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("s").expect("table");
        for i in 0u32..40 {
            txn.put(&t, &(round * 100 + i).to_be_bytes(), &stamp_value(i, 9000)).expect("put");
        }
        txn.commit().expect("commit");
    }
    env.sync().expect("sync");
    let d = env.io_stats().since(&before);
    assert_eq!((d.nosync_commits, d.commits), (3, 1), "{d:?}");
    assert_eq!((d.writeback_starts, d.writeback_ns), (0, 0), "{d:?}");
    let log = env.with_writer(|w| w.writeback_log.get_mut().take().expect("the log is on"));
    assert!(log.is_empty(), "{log:?}");
}

#[test]
fn a_durable_commit_past_the_threshold_starts_each_slices_writeback() {
    // A durable commit of 33 MiB of runs, past `WRITEBACK_MIN` with no knob set, is stamped in
    // slices of `WRITEBACK_SLICE`, each started once stamped, and reads back after a reopen.
    let (_dir, path) = temp();
    let env = open(&path);
    env.with_writer(|w| *w.writeback_log.get_mut() = Some(Vec::new()));
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("s").expect("table");
    for i in 0u32..33 {
        txn.put(&t, &i.to_be_bytes(), &stamp_value(i, 1 << 20)).expect("put");
    }
    let stamped = commit_stamped(&env, txn);
    assert!(stamped.spans.len() >= 2, "{} slices", stamped.spans.len());
    assert_spans(&stamped, WRITEBACK_SLICE, 4096);
    drop(env);
    let env = open(&path);
    env.check().expect("check");
    let txn = env.begin_ro().expect("begin");
    let t = table(&txn, "s");
    for i in 0u32..33 {
        assert!(val(&txn, &t, &i.to_be_bytes()) == Some(stamp_value(i, 1 << 20)), "key {i}");
    }
}

/// A failed writeback start fails the commit with an I/O error and latches, as a failed sync
/// does; a reopen finds the state before it.
#[test]
fn a_failed_writeback_start_latches() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]);
    env.with_writer(|w| {
        w.writeback_slice = Some(4096);
        w.file.fail_next_writeback();
    });
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    for i in 0u32..40 {
        txn.put(&a, &i.to_be_bytes(), &stamp_value(i, 3000)).expect("put");
    }
    assert!(matches!(txn.commit(), Err(Error::Io(_))));
    assert!(env.failure().is_some_and(|c| c.contains("injected writeback failure")));
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))));
    assert!(matches!(env.sync(), Err(Error::Failed(_))));
    assert_eq!(env.stat().txn_id, 1);
    drop(env);
    let env = open(&path);
    assert_eq!(env.failure(), None, "a reopen clears the latch");
    assert_eq!(env.stat().txn_id, 1);
    env.check().expect("check");
}
