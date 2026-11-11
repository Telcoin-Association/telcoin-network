//! Environment tests: create and reopen, the lock, snapshots, abort, recovery's meta choice,
//! failing closed, the latch, `SyncMode::NoSync`, the writer hand-off and table handles.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{FileExt as _, MetadataExt as _},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};

use tempfile::TempDir;

use super::*;
use crate::{
    catalog, crc, disk_model, meta::PAGE_SIZES, tests::after_steady, ReadTxn, Table, TableStat,
};

fn temp() -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix("tn_lmdb_env").expect("temp dir");
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

/// The first commit that changes something after an open, durable in `SyncMode::NoSync` too
/// (`Writer::sync_due`): one row in table `opened`, so a `NoSync` test's commits after it defer
/// until a bound or a sync. Its txn, the open's first ([`after_steady`] of the steady slot).
fn durable_first_commit(env: &Env) -> u64 {
    put_all(env, "opened", &[(b"k", b"v")])
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
        // A short key and the longest take a value up to 16 MiB (in an overflow run): every key
        // within the limit has room for an overflow cell.
        assert_eq!((env.max_key_len(), env.max_value_len(8)), (geometry.max_key(), 16 << 20));
        let longest = env.max_key_len();
        assert_eq!(env.max_value_len(longest), 16 << 20);
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
        let first = after_steady(0);
        assert_eq!(txn.commit().expect("commit"), first);
        let mut txn = env.begin_rw().expect("begin");
        for i in (0..40_u32).step_by(3) {
            let k = format!("k{i:07}").into_bytes();
            assert!(txn.delete(&a, &k).expect("delete"));
            model.remove(&k);
        }
        assert_eq!(txn.commit().expect("commit"), first + 1);
        drop(env);

        let env = open(&path);
        assert_eq!(env.stat().page_size, p);
        assert_eq!((env.stat().txn_id, env.stat().tables), (first + 1, 2));
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
        assert_eq!(put_all(&env, "b", &[(b"more", b"two")]), after_steady(first + 1));
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

/// `<path>.creating` is never opened through a symbolic link, nor used unless it is a regular
/// file: a link planted there (to a victim file, or dangling), a FIFO or a directory fails the
/// open, naming it, and the victim, the link and the path are as they were. A regular one is
/// then used.
#[test]
fn a_temporary_file_that_is_not_a_regular_file_is_refused() {
    use std::os::unix::fs::FileTypeExt as _;
    let (dir, path) = temp();
    let creating = path.with_file_name("db.creating");
    let victim = dir.path().join("victim");
    fs::write(&victim, b"the victim's bytes").expect("victim");
    let refused = |what: &str| {
        let e = Env::open(&path, EnvOptions::default()).expect_err("the open must fail");
        let Error::Io(io) = &e else { panic!("{what}: {e}") };
        let message = io.to_string();
        assert!(message.contains(&creating.display().to_string()), "{what}: {e}");
        assert!(message.contains(what), "{what}: {e}");
        assert!(fs::symlink_metadata(&path).is_err(), "{what}: the path is not created");
    };
    std::os::unix::fs::symlink(&victim, &creating).expect("link");
    refused("a symbolic link");
    assert_eq!(fs::read(&victim).expect("read"), b"the victim's bytes");
    assert_eq!(fs::read_link(&creating).expect("still a link"), victim);
    let gone = dir.path().join("gone");
    fs::remove_file(&creating).expect("remove");
    std::os::unix::fs::symlink(&gone, &creating).expect("link");
    refused("a symbolic link");
    assert!(fs::symlink_metadata(&gone).is_err(), "nothing is created through the link");

    fs::remove_file(&creating).expect("remove");
    let made = std::process::Command::new("mkfifo").arg(&creating).status().expect("mkfifo");
    assert!(made.success());
    refused("not a regular file");
    assert!(fs::symlink_metadata(&creating).expect("stat").file_type().is_fifo());
    fs::remove_file(&creating).expect("remove");
    fs::create_dir(&creating).expect("a directory");
    refused("not a regular file");
    assert!(fs::symlink_metadata(&creating).expect("stat").is_dir());
    fs::remove_dir(&creating).expect("remove");
    drop(open(&path));
    assert!(!creating.exists());
}

/// A symbolic link at the path is followed only to a file holding bytes: a dangling link (its
/// target's disk not mounted, say) or one to an empty file is refused, `Io(NotFound)` naming the
/// link, and nothing changes at either end; a read-only open refuses both too. A link to a
/// database opens and writes it.
#[test]
fn a_symlinked_path_opens_only_the_database_it_names() {
    let (dir, path) = temp();
    let creating = path.with_file_name("db.creating");
    let target = dir.path().join("unmounted").join("db");
    std::os::unix::fs::symlink(&target, &path).expect("link");
    let refused = |what: &str| {
        let e = Env::open(&path, EnvOptions::default()).expect_err("the open must fail");
        let Error::Io(io) = &e else { panic!("{what}: {e}") };
        assert_eq!(io.kind(), io::ErrorKind::NotFound, "{what}: {e}");
        assert!(io.to_string().contains(&path.display().to_string()), "{what}: {e}");
        assert_eq!(fs::read_link(&path).expect("still a link"), target, "{what}");
        assert!(!creating.exists(), "{what}: nothing is built aside");
    };
    refused("a dangling link");
    assert!(!target.exists());
    // A link made after open's check is refused by the creator's, which removes its unused file.
    assert!(matches!(create(&path, 4096), Err(Error::Io(_))));
    assert!(!creating.exists() && !target.exists());
    let e = Env::open_read_only(&path, EnvOptions::default()).expect_err("read-only");
    assert!(matches!(&e, Error::Io(io) if io.kind() == io::ErrorKind::NotFound), "{e}");

    fs::create_dir(target.parent().expect("parent")).expect("mount");
    fs::write(&target, b"").expect("empty target");
    refused("a link to an empty file");
    assert_eq!(fs::read(&target).expect("read"), b"", "the target is untouched");
    let e = Env::open_read_only(&path, EnvOptions::default()).expect_err("read-only");
    assert!(matches!(e, Error::NotTnLmdb), "{e}");

    fs::remove_file(&target).expect("remove");
    let first = put_all(&open(&target), "t", &[(b"k", b"v")]);
    assert_eq!(first, after_steady(0));
    let env = open(&path);
    assert_eq!(put_all(&env, "t", &[(b"k2", b"v2")]), after_steady(first));
    drop(env);
    assert_eq!(fs::read_link(&path).expect("still a link"), target);
    let ro = Env::open_read_only(&target, EnvOptions::default()).expect("the target");
    let txn = ro.begin_ro().expect("ro");
    assert_eq!(val(&txn, &table(&txn, "t"), b"k2").as_deref(), Some(&b"v2"[..]));
}

/// A file of another type than a regular file at the path (a FIFO, a socket, a directory), or at
/// a symbolic link's end (a FIFO, a device), is refused by both opens, `Io(InvalidInput)` naming
/// the path, and left as it is: a writer open never replaces it with a new database (a FIFO's
/// length is 0, as an empty file's), and a read-only open never blocks on it (a FIFO's open waits
/// for a writer). Each open runs on a thread under a watchdog, so a regression fails, not hangs.
#[test]
fn a_path_that_is_not_a_regular_file_is_refused() {
    use std::os::unix::{fs::FileTypeExt as _, net::UnixListener};
    let (dir, path) = temp();
    let creating = path.with_file_name("db.creating");
    let link = dir.path().join("link");
    let refused = |at: &Path, what: &str| {
        for read_only in [false, true] {
            let what = format!("{what}, read-only {read_only}");
            let (tx, rx) = mpsc::channel();
            let p = at.to_path_buf();
            std::thread::spawn(move || {
                let options = EnvOptions::default();
                let opened = if read_only {
                    Env::open_read_only(&p, options)
                } else {
                    Env::open(&p, options)
                };
                let _ = tx.send(opened.map(drop));
            });
            let Ok(got) = rx.recv_timeout(Duration::from_secs(10)) else {
                // Unblock a FIFO's open (a writer end) so the thread ends, then fail.
                let _w = fs::OpenOptions::new().write(true).open(at);
                panic!("{what}: the open blocked");
            };
            let e = got.expect_err("the open must fail");
            let Error::Io(io) = &e else { panic!("{what}: {e}") };
            assert_eq!(io.kind(), io::ErrorKind::InvalidInput, "{what}: {e}");
            assert!(io.to_string().contains(&at.display().to_string()), "{what}: {e}");
            assert!(!creating.exists(), "{what}: nothing is built aside");
        }
    };
    let made = std::process::Command::new("mkfifo").arg(&path).status().expect("mkfifo");
    assert!(made.success());
    refused(&path, "a FIFO");
    std::os::unix::fs::symlink(&path, &link).expect("link");
    refused(&link, "a link to a FIFO");
    assert!(fs::symlink_metadata(&path).expect("stat").file_type().is_fifo(), "still a FIFO");
    fs::remove_file(&link).expect("remove");
    std::os::unix::fs::symlink("/dev/null", &link).expect("link");
    refused(&link, "a link to a device");
    fs::remove_file(&path).expect("remove");
    let socket = UnixListener::bind(&path).expect("socket");
    refused(&path, "a socket");
    assert!(fs::symlink_metadata(&path).expect("stat").file_type().is_socket(), "still a socket");
    drop(socket);
    fs::remove_file(&path).expect("remove");
    fs::create_dir(&path).expect("a directory");
    refused(&path, "a directory");
    assert!(fs::symlink_metadata(&path).expect("stat").is_dir(), "still a directory");
    fs::remove_dir(&path).expect("remove");
    put_all(&open(&path), "t", &[(b"k", b"v")]);
    let ro = Env::open_read_only(&path, EnvOptions::default()).expect("a regular file");
    let txn = ro.begin_ro().expect("ro");
    assert_eq!(val(&txn, &table(&txn, "t"), b"k").as_deref(), Some(&b"v"[..]));
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

    assert_eq!(old.txn_id(), after_steady(0));
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
    let first = put_all(&env, "a", &[(b"k", b"1")]); // slot 1 (a tie goes to slot 1)
    assert_eq!(first, after_steady(0));
    put_all(&env, "a", &[(b"k", b"2")]); // first + 1: slot 0
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(first + 1), Slot::steady(first)]);
    drop(env);
    flip(&path, 20);

    let env = open(&path);
    assert_eq!(env.stat().txn_id, first);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"1"[..]));
    drop(ro);
    assert_eq!(env.with_writer(|w| w.slots), [None, Slot::steady(first)]);
    assert_eq!(put_all(&env, "a", &[(b"k", b"3")]), after_steady(first));
    drop(env);
    let mut record = [0; META_LEN];
    fs::File::open(&path).expect("open").read_exact_at(&mut record, 0).expect("read");
    assert_eq!(Meta::decode(&record).expect("slot 0 rewritten").0.txn, after_steady(first));
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"3"[..]));
}

/// With meta 0 damaged, open finds meta 1 and never reads a page past it as a meta: a file whose
/// run pages each start with the magic and another format version (a value's bytes) opens at
/// meta 1 and reads the value back.
#[test]
fn a_value_holding_meta_bytes_never_refuses_the_open() {
    let (_dir, path) = temp();
    let env = open(&path);
    let mut planted = [0; 12];
    planted[..8].copy_from_slice(&crate::meta::MAGIC);
    planted[8..].copy_from_slice(&(crate::meta::FORMAT_VERSION + 1).to_le_bytes());
    // A run's page after its first starts at value offset `4096 k - RUN_HEADER`.
    let mut value = vec![7; 20 * 4096];
    for at in (4096 - crate::overflow::RUN_HEADER..value.len() - planted.len()).step_by(4096) {
        value[at..at + planted.len()].copy_from_slice(&planted);
    }
    put_all(&env, "a", &[(b"k", &value)]); // slot 1 (a tie goes to slot 1)
    drop(env);
    let bytes = fs::read(&path).expect("read");
    for p in [8, 16] {
        assert_eq!(bytes[p * 4096..][..planted.len()], planted, "page {p} starts with them");
    }
    flip(&path, 20); // meta 0, txn 0

    let env = open(&path);
    assert_eq!(env.with_writer(|w| w.slots), [None, Slot::steady(after_steady(0))]);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k"), Some(value));
}

/// Both metas damaged, or a damaged tree root (its CRC, its txn, a header byte only
/// `page::validate` checks, its kind): the open fails closed and changes nothing.
#[test]
fn damaged_metas_and_roots_fail_closed() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]);
    put_all(&env, "a", &[(b"k", b"2")]);
    let catalog_root = env.inner().current().meta.catalog;
    let root_txn = (after_steady(0) + 1) as u32; // a page names its txn's low 32 bits
    assert_eq!((catalog_root.root_txn, catalog_root.height), (root_txn, 1));
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
    page[8] ^= 0x02; // the header txn less 2: no later than the meta's, but not the root's
    page::stamp(&mut page);
    file.write_all_at(&page, u64::from(catalog_root.root) * 4096).expect("write");
    assert_fails_closed(&path, |e| matches!(e, Error::Corrupt(m) if m.contains("catalog root")));
    // The catalog root has a reserved header byte set, the CRC restamped: only `page::validate`
    // refuses it.
    let mut page = original.clone();
    page[18] = 1;
    page::stamp(&mut page);
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

/// A flip in each region the page CRC covers ([`page::covered_offsets`]: the header with
/// `nkeys` and `upper`, a slot, a cell, the trailer) fails its CRC at open's root check, on the
/// catalog root, and at `Env::check`, on a table's leaf; a flip of the header's page number or
/// txn is refused first by the fetch checks (`read::checked_page`), which name the page.
/// Restored, the file opens and checks.
#[test]
fn a_flip_the_page_crc_covers_fails_open_and_check() {
    let (_dir, path) = temp();
    let env = open(&path);
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        (0..200_u32).map(|i| (i.to_be_bytes().to_vec(), vec![i as u8; 100])).collect();
    let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(&env, "t", &refs);
    let snapshot = env.inner().current();
    let catalog_root = snapshot.meta.catalog.root;
    let record = catalog::get(&*snapshot, &snapshot.meta.catalog, "t").expect("get").expect("t");
    assert!(record.root.height >= 2, "a branch root");
    let root = snapshot.page_any(record.root.root).expect("root");
    let leaf = page::branch_child(root, 0).expect("child").0;
    drop(snapshot);
    env.check().expect("intact");
    drop(env);
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    for (p, at_open) in [(catalog_root, true), (leaf, false)] {
        let mut original = vec![0; 4096];
        file.read_exact_at(&mut original, u64::from(p) * 4096).expect("read");
        assert!(page::free_gap(&original).is_some_and(|gap| !gap.is_empty()), "page {p}");
        for at in page::covered_offsets(&original) {
            let mut bad = original.clone();
            bad[at] ^= 0x10;
            file.write_all_at(&bad, u64::from(p) * 4096).expect("write");
            let want = match at {
                0 | 8 => format!("page {p} "),
                _ => format!("page {p} fails its CRC"),
            };
            if at_open {
                assert_fails_closed(
                    &path,
                    |e| matches!(e, Error::Corrupt(m) if m.contains("catalog root") && m.contains(&want)),
                );
            } else {
                let err = open(&path).check().expect_err("a covered flip").to_string();
                assert!(err.contains(&want), "page {p} byte {at}: {err}");
            }
        }
        file.write_all_at(&original, u64::from(p) * 4096).expect("restore");
    }
    open(&path).check().expect("restored");
}

/// A version-3 file (its tree pages CRC'd whole, their free gaps included) is refused at open
/// as `UnsupportedVersion(3)`, never read as damage, and left as it was.
#[test]
fn a_version_3_file_is_refused_at_open() {
    let (_dir, path) = temp();
    put_all(&open(&path), "a", &[(b"k", b"1")]);
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    for slot in [0_u64, 1] {
        let mut record = [0; META_LEN];
        file.read_exact_at(&mut record, slot * 4096).expect("read");
        record[8..12].copy_from_slice(&3_u32.to_le_bytes());
        let crc = crc::crc32(&record[..124]);
        record[124..].copy_from_slice(&crc.to_le_bytes());
        file.write_all_at(&record, slot * 4096).expect("write");
    }
    assert_fails_closed(&path, |e| matches!(e, Error::UnsupportedVersion(3)));
}

/// A version-4 file (leaves holding inline values only to `U / 4`, every larger one in an
/// overflow run) is refused at open as `UnsupportedVersion(4)`, never read as damage at a later
/// write, and left as it was.
#[test]
fn a_version_4_file_is_refused_at_open() {
    let (_dir, path) = temp();
    put_all(&open(&path), "a", &[(b"k", &[7; 1500])]);
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    for slot in [0_u64, 1] {
        let mut record = [0; META_LEN];
        file.read_exact_at(&mut record, slot * 4096).expect("read");
        record[8..12].copy_from_slice(&4_u32.to_le_bytes());
        let crc = crc::crc32(&record[..124]);
        record[124..].copy_from_slice(&crc.to_le_bytes());
        file.write_all_at(&record, slot * 4096).expect("write");
    }
    assert_fails_closed(&path, |e| matches!(e, Error::UnsupportedVersion(4)));
}

/// A file shorter than its meta's page count, or one that is not a tn-lmdb file, fails closed
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
    assert_fails_closed(&path, |e| matches!(e, Error::NotTnLmdb));
    fs::write(&path, b"hello").expect("tiny");
    assert_fails_closed(&path, |e| matches!(e, Error::NotTnLmdb));
}

/// A failed msync in a commit latches: the commit errs, later writes and syncs fail, reads
/// continue on the last published state, and a reopen finds that state.
#[test]
fn a_failed_msync_latches() {
    let (_dir, path) = temp();
    let env = open(&path);
    let first = put_all(&env, "a", &[(b"k", b"1")]);
    assert_eq!(first, after_steady(0));
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
    assert_eq!(env.stat().txn_id, first);
    drop(ro);
    drop(env);
    let env = open(&path);
    assert_eq!(env.failure(), None, "a reopen clears the latch");
    assert_eq!(env.stat().txn_id, first);
}

/// The sync a commit's injected failure hits, for [`a_failed_sync_latches_and_recovers`].
#[derive(Clone, Copy, Debug, PartialEq)]
enum FailedSync {
    /// Step 4 by one `fdatasync` of the file.
    DataFdatasync,
    /// Step 5: the size sync of a commit that grew the file, its data synced by one `msync`.
    SizeSync,
    /// Step 6: the meta page's `msync`, the data synced by one `fdatasync`.
    MetaMsync,
}

/// Commit L, then a commit M whose `failed` sync fails: M errs with the injected `Io` error, which
/// latches the environment, the slots keep their txns (the reuse gate stays where L left it), and
/// `Env::sync` is refused. After the crash, and a strict power loss when `strict`, the file reopens
/// at L and passes `Env::check`; after a failed meta msync without a power loss it reopens at M,
/// whose record the page cache kept (a failed commit's outcome is unknown until a reopen).
fn a_failed_sync_latches_and_recovers(failed: FailedSync) {
    for strict in [false, true] {
        let (_dir, path) = temp();
        let env = open(&path);
        let l = put_all(&env, "t", &[(b"k", b"1")]);
        disk_model::track(&path);
        let slots = env.with_writer(|w| w.slots());
        let len = env.with_writer(|w| w.file.file_len());
        let mut txn = env.begin_rw().expect("begin");
        let t = table(&txn, "t");
        txn.put(&t, b"k", b"2").expect("put");
        if failed == FailedSync::SizeSync {
            txn.put(&t, b"big", &vec![7; len as usize]).expect("a value past the file's end");
            assert!(txn.writer_mut().file.size_unsynced(), "the commit grows the file");
        }
        let w = txn.writer_mut();
        let cause = match failed {
            FailedSync::DataFdatasync => {
                w.data_by_fdatasync = true;
                w.file.fail_next_fsync();
                "injected fsync failure"
            }
            FailedSync::SizeSync => {
                w.data_by_msync = true;
                w.file.fail_next_fsync();
                "injected fsync failure"
            }
            FailedSync::MetaMsync => {
                w.data_by_fdatasync = true;
                w.file.fail_next_msync();
                "injected msync failure"
            }
        };
        let err = txn.commit().expect_err("the commit's sync fails");
        assert!(matches!(err, Error::Io(_)), "{failed:?}: {err:?}");
        assert!(
            env.failure().is_some_and(|c| c.contains(cause)),
            "{failed:?}: latched by the sync"
        );
        assert_eq!(env.with_writer(|w| w.slots()), slots, "{failed:?}: the slots keep their txns");
        assert!(matches!(env.sync(), Err(Error::Failed(_))), "{failed:?}");
        env.crash();
        if strict {
            disk_model::power_loss(&path);
        }
        disk_model::untrack(&path);
        let env = open(&path);
        let want = if failed == FailedSync::MetaMsync && !strict { l + 1 } else { l };
        assert_eq!(env.stat().txn_id, want, "{failed:?}, strict {strict}");
        let ro = env.begin_ro().expect("ro");
        let want = if want == l { b"1" } else { b"2" };
        assert_eq!(val(&ro, &table(&ro, "t"), b"k").as_deref(), Some(&want[..]), "{failed:?}");
        drop(ro);
        env.check().expect("check");
    }
}

/// A commit whose data `fdatasync` fails latches and recovers L
/// ([`a_failed_sync_latches_and_recovers`]).
#[test]
fn a_failed_data_fdatasync_latches_and_recovers_the_last_commit() {
    a_failed_sync_latches_and_recovers(FailedSync::DataFdatasync);
}

/// A commit whose size sync fails latches and recovers L ([`a_failed_sync_latches_and_recovers`]).
#[test]
fn a_failed_size_sync_latches_and_recovers_the_last_commit() {
    a_failed_sync_latches_and_recovers(FailedSync::SizeSync);
}

/// A commit whose meta msync fails latches and recovers L after a power loss, M after a process
/// crash ([`a_failed_sync_latches_and_recovers`]).
#[test]
fn a_failed_meta_msync_latches_and_recovers_the_last_or_the_new_commit() {
    a_failed_sync_latches_and_recovers(FailedSync::MetaMsync);
}

/// A write that fails part way latches: the open transaction's later writes and its commit
/// fail, and in `NoSync` the environment's drop does not sync, so the unsynced commits (those
/// after the open's first, durable one) stay weak, as after a crash: a reboot loses them.
#[test]
fn a_latched_env_neither_writes_nor_syncs_on_drop() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    let durable = durable_first_commit(&env);
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
    disk_model::reboot();
    let env = open(&path);
    assert_eq!(env.stat().txn_id, durable, "the unsynced commit was not synced by the drop");
}

/// A sync a destructor runs while an unrelated panic unwinds makes the state durable and leaves
/// the environment unlatched: only a panic of the sync itself latches.
#[test]
fn a_sync_a_destructor_runs_during_an_unrelated_unwind_does_not_latch() {
    struct SyncOnDrop(Env);
    impl Drop for SyncOnDrop {
        fn drop(&mut self) {
            self.0.sync().expect("sync");
        }
    }
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    let weak = durable_first_commit(&env) + 1;
    assert_eq!(put_all(&env, "a", &[(b"k", b"1")]), weak);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _sync = SyncOnDrop(env.clone());
        panic!("unrelated");
    }));
    assert!(caught.is_err());
    assert_eq!(env.failure(), None);
    assert_eq!(put_all(&env, "a", &[(b"k", b"2")]), weak + 1);
    env.crash();
    disk_model::reboot();
    let env = open(&path);
    assert_eq!(env.stat().txn_id, weak, "the sync made the weak commit durable");
}

/// A write and a commit that a destructor runs during an unrelated panic's unwind succeed and do
/// not latch: their guards latch only on a panic of their own.
#[test]
fn a_write_and_commit_a_destructor_runs_during_an_unrelated_unwind_do_not_latch() {
    struct PutOnDrop(Env);
    impl Drop for PutOnDrop {
        fn drop(&mut self) {
            put_all(&self.0, "a", &[(b"k", b"2")]);
        }
    }
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::Durable)).expect("open");
    put_all(&env, "a", &[(b"k", b"1")]);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _put = PutOnDrop(env.clone());
        panic!("unrelated");
    }));
    assert!(caught.is_err());
    assert_eq!(env.failure(), None);
    let ro = env.begin_ro().expect("ro");
    let txn = after_steady(0) + 1;
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (txn, Some(b"2".to_vec())));
}

/// `NoSync` commits are visible in-process at once, and each but an open's first (durable) writes
/// a weak meta into the slot that does not hold the last sync's: a process crash in the same boot
/// recovers the last commit, one after a reboot the last sync (the weak meta untrusted, though its
/// bytes are all there); a sync and a clean drop make the newest state steady. The writer reopened
/// past an untrusted weak head gives its first commit a txn past that head's, and that commit,
/// durable, replaces the head.
#[test]
fn nosync_commits_survive_a_process_crash_and_roll_back_at_a_reboot() {
    let (_dir, path) = temp();
    let nosync = options(4096, SyncMode::NoSync);
    let env = Env::open(&path, nosync).expect("open");
    let first = durable_first_commit(&env); // slot 1 (both steady at txn 0: a tie)
    assert_eq!(first, after_steady(0));
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(0), Slot::steady(first)]);
    put_all(&env, "a", &[(b"k", b"1")]); // weak, slot 0
    assert_eq!(env.with_writer(|w| w.slots), [Slot::weak(first + 1), Slot::steady(first)]);
    env.crash();
    let env = Env::open(&path, nosync).expect("reopen");
    let ro = env.begin_ro().expect("ro");
    let read = (ro.txn_id(), val(&ro, &table(&ro, "a"), b"k"));
    assert_eq!(read, (first + 1, Some(b"1".to_vec())));
    drop(ro);

    let reopened = durable_first_commit(&env);
    assert_eq!(reopened, after_steady(first), "past the run of the steady slot's");
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(reopened), Slot::steady(first)]);
    put_all(&env, "a", &[(b"k", b"2")]);
    let synced = reopened + 1;
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(reopened), Slot::weak(synced)]);
    assert!(env.with_writer(|w| w.unsynced.count()) > 0);
    env.sync().expect("sync");
    let both = [Slot::steady(reopened), Slot::steady(synced)];
    assert_eq!(env.with_writer(|w| (w.slots, w.unsynced.count())), (both, 0));
    env.sync().expect("a sync with nothing pending");
    put_all(&env, "a", &[(b"k", b"3")]);
    let head = synced + 1;
    assert_eq!(env.with_writer(|w| w.slots), [Slot::weak(head), Slot::steady(synced)]);
    env.crash();

    disk_model::reboot();
    let env = Env::open(&path, nosync).expect("reopen");
    let ro = env.begin_ro().expect("ro");
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (synced, Some(b"2".to_vec())));
    drop(ro);
    assert_eq!(env.with_writer(|w| w.slots), [Slot::weak(head), Slot::steady(synced)], "as found");
    let past = after_steady(synced);
    assert!(past > head);
    assert_eq!(put_all(&env, "a", &[(b"k", b"4")]), past, "past the untrusted head's txn");
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(past), Slot::steady(synced)]);
    put_all(&env, "a", &[(b"k", b"5")]);
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(past), Slot::weak(past + 1)]);
    drop(env); // a clean drop syncs
    let env = open(&path);
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(past), Slot::steady(past + 1)]);
    let ro = env.begin_ro().expect("ro");
    let read = (ro.txn_id(), val(&ro, &table(&ro, "a"), b"k"));
    assert_eq!(read, (past + 1, Some(b"5".to_vec())));
}

/// A reopen in the same boot after the device lost a writeback opens at the last sync, not at
/// the cached head. A `NoSync` run whose syncs are lost (the disk model's lost flushes: as a
/// kernel after a writeback error it reported once, every sync reports success and the device
/// takes nothing, the pages staying clean in the cache) crashes with a trusted weak head whose
/// pages the device never got. The next open's `fsync` (step 4) writes nothing; it then drops
/// the file's cached pages (`sys::drop_cached_pages`), so what it reads is the device's. When
/// the head's record never reached the device either, the open does not see the head and opens
/// at the steady slot; when the kernel had written the record's page back before the loss
/// (`record_on_device`: its meta page msynced first), the head is trusted, step 9's check finds
/// its pages old and falls back to the last sync. Either way the state is the last sync's, whose
/// pages the device holds; without the drop the cache would pass the check, and the next sync
/// would write a steady meta naming pages the device lacks.
#[test]
fn a_reopen_after_a_lost_writeback_falls_back_to_the_last_sync() {
    for record_on_device in [false, true] {
        let case = format!("record on device: {record_on_device}");
        let (_dir, path) = temp();
        let nosync = options(4096, SyncMode::NoSync);
        let env = Env::open(&path, nosync).expect("open");
        disk_model::track(&path);
        let first = durable_first_commit(&env);
        let synced = put_all(&env, "a", &[(b"k", b"synced")]);
        env.sync().expect("sync");
        let slots = env.with_writer(|w| w.slots);
        assert!(slots.contains(&Slot::steady(synced)) && slots.contains(&Slot::steady(first)));
        let head = put_all(&env, "a", &[(b"k", b"lost")]);
        let slots = env.with_writer(|w| w.slots);
        assert!(slots.contains(&Slot::weak(head)), "{case}: {slots:?}");
        if record_on_device {
            env.with_writer(|w| {
                let slot = w.slots.iter().position(|s| s.is_some_and(|s| s.weak)).expect("weak");
                w.file.msync_page(slot as u32, 4096).expect("the meta page's writeback");
            });
        }
        disk_model::set_flushes_lost(true);
        env.crash();

        let env = Env::open(&path, nosync).expect("reopen");
        disk_model::set_flushes_lost(false);
        let checked = take_weak_head_check();
        if record_on_device {
            assert_eq!(checked, Some(Err(())), "{case}: the head's pages are not the device's");
        } else {
            assert_eq!(checked, None, "{case}: the head's record is not the device's");
        }
        let ro = env.begin_ro().expect("ro");
        let read = (ro.txn_id(), val(&ro, &table(&ro, "a"), b"k"));
        assert_eq!(read, (synced, Some(b"synced".to_vec())), "{case}: the last sync");
        drop(ro);
        // The writer builds on the last sync: its next commit, durable, goes past the lost
        // head's txn, and a power loss keeps it.
        let next = put_all(&env, "a", &[(b"k", b"after")]);
        assert_eq!(next, after_steady(synced), "{case}");
        env.crash();
        disk_model::power_loss(&path);
        let env = open(&path);
        let ro = env.begin_ro().expect("ro");
        let read = (ro.txn_id(), val(&ro, &table(&ro, "a"), b"k"));
        assert_eq!(read, (next, Some(b"after".to_vec())), "{case}");
        drop(ro);
        drop(env);
        disk_model::untrack(&path);
    }
}

/// `Env::sync` after a `NoSync` commit runs the durable steps although a slot already holds the
/// commit's txn: that slot is weak, and the sync's check is against the newest steady slot. The
/// sync counts a durable commit, syncs the commit's pages, makes both slots steady, and the commit
/// survives a power loss.
#[test]
fn a_sync_after_a_weak_commit_runs_the_durable_steps() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    disk_model::track(&path);
    let first = durable_first_commit(&env);
    let txn = put_all(&env, "a", &[(b"k", b"1")]);
    assert_eq!(env.with_writer(|w| w.slots), [Slot::weak(txn), Slot::steady(first)]);
    let before = env.io_stats();
    env.sync().expect("sync");
    let synced = env.io_stats().since(&before);
    assert_eq!(synced.commits, 1, "{synced:?}");
    assert!(synced.pages_synced > 0, "{synced:?}");
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(txn), Slot::steady(first)]);
    env.crash();
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    assert_eq!((ro.txn_id(), val(&ro, &table(&ro, "a"), b"k")), (txn, Some(b"1".to_vec())));
}

/// A `Durable` open in the same boot trusts a `NoSync` writer's weak head, which stays weak until
/// a sync: an `Env::sync` makes it steady (a durable commit's work, in `Durable` mode too), and so
/// does the close when no commit replaced it; either way it then survives a reboot.
#[test]
fn a_durable_open_makes_a_trusted_weak_head_steady_at_sync_and_close() {
    for by_close in [false, true] {
        let (_dir, path) = temp();
        let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
        let first = durable_first_commit(&env);
        let txn = put_all(&env, "a", &[(b"k", b"1")]);
        env.crash();
        let env = open(&path);
        assert_eq!(env.stat().txn_id, txn, "by_close {by_close}: the trusted weak head");
        assert_eq!(env.with_writer(|w| w.slots), [Slot::weak(txn), Slot::steady(first)]);
        if by_close {
            drop(env);
        } else {
            env.sync().expect("sync");
            assert_eq!(env.io_stats().commits, 1, "by_close {by_close}: the sync's durable steps");
            assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(txn), Slot::steady(first)]);
            env.crash();
        }
        disk_model::reboot();
        let env = open(&path);
        assert_eq!(env.stat().txn_id, txn, "by_close {by_close}: steady across a reboot");
    }
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
        let first = after_steady(0);
        let want_txn = if sync == SyncMode::Durable { first + 2 } else { first + 1 };
        assert_eq!(ro.txn_id(), want_txn, "{sync:?}");
        let (a, b) = (table(&ro, "a"), table(&ro, "b"));
        for (k, v) in &rows {
            assert_eq!(val(&ro, &a, k).as_deref(), Some(*v), "{sync:?}");
        }
        assert_eq!(ro.table_stat(&b).expect("stat").entries, 100);
        assert_eq!(val(&ro, &a, b"late").is_some(), sync == SyncMode::Durable);
    }
}

/// Commit `round` of the automatic-sync tests, in table `a`: 300 rows of 150 bytes (about a dozen
/// leaves), then a rewrite of the first leaf's rows, then of a few rows of another leaf.
fn auto_sync_round(env: &Env, round: u8) {
    let rows = match round {
        0 => 0..300_u32,
        1 => 0..10,
        _ => 100..105,
    };
    let rows: Vec<([u8; 4], Vec<u8>)> =
        rows.map(|i| (i.to_be_bytes(), vec![round + 1; 150])).collect();
    let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(env, "a", &rows);
}

/// The value [`auto_sync_round`] leaves under row `i` after `rounds` rounds.
fn auto_sync_value(i: u32, rounds: u8) -> Vec<u8> {
    let round = match i {
        0..10 if rounds > 1 => 1,
        100..105 if rounds > 2 => 2,
        _ => 0,
    };
    vec![round + 1; 150]
}

/// Open `path` in `SyncMode::NoSync` with the automatic syncs `sync_period` and `sync_bytes`.
fn open_auto(path: &Path, sync_period: Option<Duration>, sync_bytes: Option<u64>) -> Env {
    let options = EnvOptions { sync_period, sync_bytes, ..options(4096, SyncMode::NoSync) };
    Env::open(path, options).expect("open")
}

/// The newest txn a steady meta slot holds: the last durable state.
fn durable_txn(env: &Env) -> u64 {
    env.with_writer(|w| durable_txn_of(w))
}

/// [`durable_txn`] of the writer's state.
fn durable_txn_of(w: &Writer) -> u64 {
    w.steady_txn().expect("a steady slot")
}

/// `NoSync` with `sync_bytes`, after the open's first commit (durable whatever the bound): the
/// commit whose stamped pages bring the unsynced pages to the bound commits durably, over its own
/// pages and the deferred commit's (a strict power loss keeps it), and the count starts again from
/// it, so the next, smaller commit defers; with a bound one byte higher, the same commit defers.
#[test]
fn a_nosync_commit_reaching_sync_bytes_commits_durably() {
    // The unsynced pages after each round, on a file that never syncs after the open's first
    // commit (the rounds allocate the same pages whatever the bound: nothing freed is reusable
    // before the second sync).
    let unsynced: Vec<u64> = {
        let (_dir, path) = temp();
        let env = open_auto(&path, None, None);
        durable_first_commit(&env);
        (0..3)
            .map(|round| {
                auto_sync_round(&env, round);
                env.with_writer(|w| w.unsynced_pages)
            })
            .collect()
    };
    assert!(unsynced[2] - unsynced[1] < unsynced[1], "the third round stamps fewer: {unsynced:?}");
    let reach = unsynced[1] * 4096;

    let (_dir, path) = temp();
    let env = open_auto(&path, None, Some(reach));
    disk_model::track(&path);
    let first = after_steady(0);
    assert_eq!(durable_first_commit(&env), first);
    let opened = env.io_stats();
    assert_eq!((opened.nosync_commits, opened.auto_syncs, opened.commits), (0, 1, 1));
    auto_sync_round(&env, 0);
    let deferred = env.io_stats().since(&opened);
    assert_eq!((deferred.nosync_commits, deferred.auto_syncs, deferred.commits), (1, 0, 0));
    let deferred = env.io_stats();
    auto_sync_round(&env, 1);
    let d = env.io_stats().since(&deferred);
    assert_eq!((d.nosync_commits, d.auto_syncs, d.commits), (0, 1, 1), "{d:?}");
    assert_eq!(d.pages_synced, unsynced[1], "both commits' pages: {d:?}");
    assert_eq!(env.with_writer(|w| (durable_txn_of(w), w.unsynced_pages)), (first + 2, 0));
    auto_sync_round(&env, 2);
    let s = env.io_stats().since(&opened);
    assert_eq!((s.nosync_commits, s.auto_syncs, s.commits), (2, 1, 1), "{s:?}");
    env.crash();
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    let reached = first + 2;
    assert_eq!(
        ro.txn_id(),
        reached,
        "the commit that reached the bound survives, the next does not"
    );
    let a = table(&ro, "a");
    for i in 0..300_u32 {
        assert_eq!(val(&ro, &a, &i.to_be_bytes()), Some(auto_sync_value(i, 2)), "row {i}");
    }
    drop(ro);
    env.check().expect("check");
    drop(env);

    let (_dir, path) = temp();
    let env = open_auto(&path, None, Some(reach + 1));
    disk_model::track(&path);
    assert_eq!(durable_first_commit(&env), first);
    let opened = env.io_stats();
    auto_sync_round(&env, 0);
    auto_sync_round(&env, 1);
    let s = env.io_stats().since(&opened);
    assert_eq!((s.nosync_commits, s.auto_syncs, s.commits), (2, 0, 0), "{s:?}");
    env.crash();
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    let txn = open(&path).stat().txn_id;
    assert_eq!(txn, first, "a byte short of the bound, nothing past the first commit was synced");
}

/// `NoSync` with `sync_period`: `Duration::ZERO` makes every commit that changes something
/// durable. With a period, the open's first commit is durable short of it (the open is no durable
/// state the period counts from); then a commit made one nanosecond short of it after the last
/// durable state defers and one made at it commits durably (the writer's clock moved by
/// `Writer::clock_at`, so nothing waits); the period counts from each durable commit's durable
/// steps, and from an `Env::sync`'s.
#[test]
fn a_nosync_commit_past_sync_period_commits_durably() {
    let (_dir, path) = temp();
    let env = open_auto(&path, Some(Duration::ZERO), None);
    for i in 0..3 {
        put_all(&env, "a", &[(b"k", &[i])]);
        assert_eq!(durable_txn(&env), after_steady(0) + u64::from(i));
    }
    env.begin_rw().expect("begin").commit().expect("a commit of no change");
    let s = env.io_stats();
    assert_eq!((s.nosync_commits, s.auto_syncs, s.commits), (0, 3, 3), "{s:?}");
    drop(env);

    let period = Duration::from_secs(3600);
    let (_dir, path) = temp();
    let env = open_auto(&path, Some(period), None);
    let due = |env: &Env| env.with_writer(|w| w.durable_at + period);
    let set_clock = |env: &Env, at: Instant| env.with_writer(|w| w.clock_at = Some(at));
    set_clock(&env, due(&env) - Duration::from_nanos(1));
    let first = put_all(&env, "a", &[(b"k", b"0")]);
    assert_eq!(first, after_steady(0));
    assert_eq!(durable_txn(&env), first, "the open's first commit, short of the period");
    let after_first = due(&env);
    set_clock(&env, after_first - Duration::from_nanos(1));
    put_all(&env, "a", &[(b"k", b"1")]);
    assert_eq!(durable_txn(&env), first, "short of the period since the first commit");
    set_clock(&env, after_first);
    put_all(&env, "a", &[(b"k", b"2")]);
    assert_eq!(durable_txn(&env), first + 2, "the period since the first commit");
    put_all(&env, "a", &[(b"k", b"3")]);
    assert_eq!(durable_txn(&env), first + 2, "short of the period since commit 2");
    set_clock(&env, due(&env));
    put_all(&env, "a", &[(b"k", b"4")]);
    assert_eq!(durable_txn(&env), first + 4, "the period since commit 2");
    let after_4 = due(&env);
    set_clock(&env, after_4 - Duration::from_nanos(1));
    put_all(&env, "a", &[(b"k", b"5")]);
    env.sync().expect("sync");
    set_clock(&env, after_4);
    put_all(&env, "a", &[(b"k", b"6")]);
    assert_eq!(durable_txn(&env), first + 5, "short of the period since the sync");
    let s = env.io_stats();
    assert_eq!((s.nosync_commits, s.auto_syncs, s.commits), (4, 3, 4), "{s:?}");
}

/// `sync_period` without the test clock (`Writer::clock_at` unset) reads the real one: after a
/// first commit, a second within an hour of the last durable state defers, and one 30 ms after
/// the first with a 5 ms period commits durably.
#[test]
fn sync_period_reads_the_real_clock() {
    for (period, wait) in [(Duration::from_secs(3600), false), (Duration::from_millis(5), true)] {
        let (_dir, path) = temp();
        let env = open_auto(&path, Some(period), None);
        assert!(env.with_writer(|w| w.clock_at.is_none()), "the real clock");
        put_all(&env, "a", &[(b"k", b"1")]);
        let syncs = env.io_stats().auto_syncs;
        if wait {
            std::thread::sleep(Duration::from_millis(30));
        }
        let txn = put_all(&env, "a", &[(b"k", b"2")]);
        assert_eq!(durable_txn(&env) == txn, wait, "{period:?}");
        assert_eq!(env.io_stats().auto_syncs - syncs, u64::from(wait), "{period:?}");
    }
}

/// A `NoSync` churn of rewrites, each freeing the pages it copies: with `sync_bytes` the file's
/// pages stay within the live pages plus what one sync interval and a few commits can hold (a page
/// freed by txn N is reused from the second commit after the first sync at or after N, once that
/// commit's weak meta replaced the older steady slot; an interval stamps less than the bound plus
/// its syncing commit), while with no bound (only the open's first commit, the load, syncs)
/// nothing is reused and every commit grows them. The file's length follows its pages by the growth
/// policy. (Before weak metas a freed page waited for two syncs, and the same churn held about 290
/// pages against this bound's 232.)
#[test]
fn nosync_churn_with_sync_bytes_keeps_the_file_bounded() {
    const ROWS: u32 = 2000;
    const ROUNDS: u32 = 200;
    let bound_pages = 100_u64;
    let churn = |env: &Env, round: u32| {
        let rows: Vec<([u8; 4], Vec<u8>)> = (0..20)
            .map(|j| ((round * 20 + j) * 97 % ROWS).to_be_bytes())
            .map(|k| (k, vec![round as u8; 100]))
            .collect();
        let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        put_all(env, "a", &rows);
    };
    let load = |env: &Env| {
        let rows: Vec<([u8; 4], Vec<u8>)> =
            (0..ROWS).map(|i| (i.to_be_bytes(), vec![0; 100])).collect();
        let rows: Vec<(&[u8], &[u8])> = rows.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        put_all(env, "a", &rows);
    };

    let (_dir, path) = temp();
    let env = open_auto(&path, None, Some(bound_pages * 4096));
    load(&env);
    let (mut live, mut most_allocated) = (0, 0);
    for round in 0..ROUNDS {
        let before = env.io_stats();
        churn(&env, round);
        let d = env.io_stats().since(&before);
        most_allocated = most_allocated.max(d.pages_from_pool + d.pages_from_end);
        let r = env.check().expect("check");
        live = live.max(u64::from(r.page_count) - r.free_pages);
    }
    let s = env.io_stats();
    let bound = live + bound_pages + 3 * most_allocated;
    let bounded = u64::from(env.stat().page_count);
    assert!(bounded <= bound, "{bounded} pages, past {bound}: {s:?}");
    assert!(s.auto_syncs > 1 && s.pages_from_pool > 0, "freed pages came back: {s:?}");
    drop(env);

    let (_dir, path) = temp();
    let env = open_auto(&path, None, None);
    load(&env);
    let (loaded, after_load) = (env.stat().page_count, env.io_stats());
    assert_eq!(after_load.auto_syncs, 1, "the open's first commit: {after_load:?}");
    for round in 0..ROUNDS {
        churn(&env, round);
    }
    let unbounded = u64::from(env.stat().page_count);
    let s = env.io_stats().since(&after_load);
    assert_eq!((s.auto_syncs, s.pages_from_pool), (0, 0), "{s:?}");
    assert!(unbounded >= u64::from(loaded) + u64::from(ROUNDS), "every commit grew the file");
    assert!(unbounded > bound, "{unbounded} pages, within the bounded run's {bound}");
}

/// A writer open is not a durable state, even over a trusted weak head: the first commit that
/// changes something after it commits durably whatever the bounds (`Writer::sync_due`). So a
/// loop of short runs in one boot, each a reopen over the last run's weak head, one commit of
/// 200 rewritten rows under `sync_bytes` (about 53 pages of 200) or `sync_period` (none of it
/// passing), or with neither bound, and a process crash, syncs once per run: the steady slot
/// moves with every run, the file stops growing once the reuse gate passes the first runs' freed
/// pages, and a power loss after the loop loses nothing.
#[test]
fn a_crash_loop_in_one_boot_syncs_once_per_run() {
    let bounds = [(None, Some(200 * 4096)), (Some(Duration::from_secs(3600)), None), (None, None)];
    for (sync_period, sync_bytes) in bounds {
        let case = format!("sync_period {sync_period:?}, sync_bytes {sync_bytes:?}");
        let (_dir, path) = temp();
        drop(open_auto(&path, sync_period, sync_bytes));
        disk_model::track(&path);
        let mut counts = Vec::new();
        let mut last = 0;
        for run in 0..10_u32 {
            let env = open_auto(&path, sync_period, sync_bytes);
            let mut txn = env.begin_rw().expect("begin");
            let table = txn.create_table("t").expect("t");
            for k in 0..200_u32 {
                txn.put(&table, &k.to_be_bytes(), &[run as u8; 900]).expect("put");
            }
            last = txn.commit().expect("commit");
            let s = env.io_stats();
            assert_eq!((s.auto_syncs, s.nosync_commits), (1, 0), "{case}, run {run}: {s:?}");
            assert_eq!(durable_txn(&env), last, "{case}, run {run}: the steady slot moved");
            counts.push(env.stat().page_count);
            env.crash();
        }
        assert!(counts[4..].iter().all(|&c| c == counts[3]), "{case}: the file grows: {counts:?}");
        disk_model::power_loss(&path);
        let env = open(&path);
        assert_eq!(env.stat().txn_id, last, "{case}: a power loss after the loop loses nothing");
        let ro = env.begin_ro().expect("ro");
        let table = ro.open_table("t").expect("open").expect("t");
        assert_eq!(val(&ro, &table, &0_u32.to_be_bytes()), Some(vec![9; 900]), "{case}");
        drop(ro);
        drop(env);
        disk_model::untrack(&path);
    }
}

/// `EnvOptions::sync_bytes` bounds what a reboot loses across a reopen over a trusted weak head
/// too: the reopen is not a durable state, so its first commit that changes something commits
/// durably, with the head's deferred commits, though alone it holds fewer pages than the bound.
/// Here the bound is the pages of the head's one deferred commit and of the reopen's commit: the
/// commit that brings the pages since the last durable state to it syncs, and a power loss after
/// it keeps it.
#[test]
fn the_sync_bytes_bound_holds_across_a_reopen_over_a_weak_head() {
    // The pages of rounds 1 and 2, each deferred after a synced round 0.
    let pages: Vec<u64> = {
        let (_dir, path) = temp();
        let env = open_auto(&path, None, None);
        auto_sync_round(&env, 0);
        env.sync().expect("sync");
        (1..3)
            .map(|round| {
                auto_sync_round(&env, round);
                env.with_writer(|w| w.unsynced_pages)
            })
            .collect()
    };
    assert!(pages[0] > 0 && pages[1] > pages[0], "{pages:?}");
    let reach = pages[1] * 4096;
    let (_dir, path) = temp();
    let env = open_auto(&path, None, Some(reach));
    disk_model::track(&path);
    auto_sync_round(&env, 0);
    env.sync().expect("sync");
    let deferred = env.io_stats().nosync_commits;
    auto_sync_round(&env, 1);
    assert_eq!(env.io_stats().nosync_commits - deferred, 1, "round 1 defers: under the bound");
    let head = env.stat().txn_id;
    env.crash();
    let env = open_auto(&path, None, Some(reach));
    assert_eq!(env.stat().txn_id, head, "the weak head is trusted");
    auto_sync_round(&env, 2);
    let s = env.io_stats();
    assert_eq!(
        (s.auto_syncs, s.nosync_commits),
        (1, 0),
        "round 2 brings the pages since the last \
         durable state to the bound: {s:?}"
    );
    let round2 = env.stat().txn_id;
    env.crash();
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    assert_eq!(open(&path).stat().txn_id, round2, "a power loss keeps the synced round 2");
}

/// An `Env::sync` of the state an open found (here a trusted weak head, made steady) is not a
/// commit of the writer's: its first commit that changes something after the open is still
/// durable (`Writer::weak_limit`). It starts past the steady slot the open found, not past the
/// head (`Chosen::next_txn`), so a weak first commit could be given a txn an earlier writer had
/// stamped pages with (a dead commit at the end of its run of weak commits).
#[test]
fn a_sync_of_the_opened_state_keeps_the_first_commit_durable() {
    let (_dir, path) = temp();
    let env = open_auto(&path, None, None);
    let steady = durable_first_commit(&env);
    auto_sync_round(&env, 0);
    let head = env.stat().txn_id;
    assert_eq!(durable_txn(&env), steady, "round 0 defers");
    env.crash();
    let env = open_auto(&path, None, None);
    assert_eq!(env.stat().txn_id, head, "the weak head is trusted");
    env.sync().expect("sync");
    assert_eq!(durable_txn(&env), head, "the sync made the head steady");
    auto_sync_round(&env, 1);
    let s = env.io_stats();
    assert_eq!((s.auto_syncs, s.nosync_commits), (1, 0), "the first commit is durable: {s:?}");
    assert_eq!(durable_txn(&env), after_steady(steady), "its txn: past the steady slot found");
}

/// A commit whose pages are scattered over the file (copies taking pages that earlier commits
/// freed all over it), so they form more than one range, syncs its data with one `fdatasync` of
/// the file (on macOS by the `msync`s of each merged range), and survives a power loss.
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
/// further on) in a file that did not grow syncs them all by its one `fdatasync` (on macOS by an
/// `msync` of each range), and survives a power loss: an `msync` of one of the ranges would lose
/// the other, and no size sync covers it. Every commit up to the first such one is followed by a
/// power loss and a reopen, so the check does not depend on which sync step 4 took.
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
    let first = put_all(&env, "a", &[(b"k", b"1")]); // slot 1
    assert_eq!(first, after_steady(0));
    put_all(&env, "a", &[(b"k", b"2")]); // first + 1: slot 0
    drop(env);
    disk_model::track(&path);
    // Rewrite slot 1 as txn first + 2 naming first + 1's state, in the page cache only.
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, 0).expect("read");
    let meta = Meta { txn: first + 2, ..Meta::decode(&record).expect("first + 1").0 };
    file.write_all_at(&meta.encode(Tag::default()), 4096).expect("write");
    drop(file);

    assert_eq!(open(&path).stat().txn_id, first + 2);
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    assert_eq!(open(&path).stat().txn_id, first + 2, "the open synced the meta it chose");
}

/// Every writer open syncs the directory naming the file, so a file whose creator died (or
/// failed) before its directory sync gets its name made durable by the next open; a read-only
/// open syncs nothing.
#[test]
fn a_writer_open_syncs_the_directory_naming_the_file() {
    let (dir, path) = temp();
    crate::sys::take_dir_sync_log();
    drop(open(&path));
    let dir = dir.path().to_path_buf();
    assert_eq!(crate::sys::take_dir_sync_log(), [dir.clone(), dir.clone()], "create, then open");
    drop(open(&path));
    assert_eq!(crate::sys::take_dir_sync_log(), [dir], "an open of an existing file");
    drop(Env::open_read_only(&path, EnvOptions::default()).expect("read-only"));
    assert_eq!(crate::sys::take_dir_sync_log(), Vec::<PathBuf>::new(), "a read-only open");
}

/// A directory the process may enter and write but not read (mode 0300) cannot be opened to be
/// synced: the create and the open of an existing file in it skip its sync (it never could make
/// the name durable) and open, syncing no directory. Any other failure of a directory sync fails,
/// naming the directory.
#[test]
fn a_directory_that_cannot_be_read_is_not_synced_and_the_open_goes_on() {
    use std::os::unix::fs::PermissionsExt as _;
    let tmp = TempDir::with_prefix("tn_lmdb_env").expect("temp dir");
    let dir = tmp.path().join("d");
    fs::create_dir(&dir).expect("dir");
    let path = dir.join("db");
    /// Gives the directory its read permission back however the test ends, before the
    /// temporary directory's removal, which must list it.
    struct Readable<'d>(&'d Path);
    impl Drop for Readable<'_> {
        fn drop(&mut self) {
            let _ = fs::set_permissions(self.0, fs::Permissions::from_mode(0o700));
        }
    }
    let readable = Readable(&dir);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o300)).expect("chmod");
    // A process that reads the directory anyway (root) syncs it: only the open is checked then.
    let unreadable = fs::read_dir(&dir).is_err();
    crate::sys::take_dir_sync_log();
    let created =
        Env::open(&path, EnvOptions::default()).map(|env| put_all(&env, "t", &[(b"k", b"v")]));
    let reopened = Env::open(&path, EnvOptions::default()).map(drop);
    let synced = crate::sys::take_dir_sync_log();
    drop(readable);
    assert!(created.is_ok() && reopened.is_ok(), "{created:?}, {reopened:?}");
    if unreadable {
        assert_eq!(synced, Vec::<PathBuf>::new(), "a directory it cannot open is not synced");
    }
    let env = open(&path);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "t"), b"k").as_deref(), Some(&b"v"[..]));
    let missing = tmp.path().join("missing");
    let e = crate::sys::fsync_directory(&missing).expect_err("a missing directory");
    assert_eq!(e.kind(), io::ErrorKind::NotFound);
    assert!(e.to_string().contains(&missing.display().to_string()), "{e}");
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
    let txn = after_steady(0);
    assert_eq!(committer.join().expect("join"), txn);
    assert_eq!(rx.recv_timeout(Duration::from_secs(30)).expect("the waiter proceeds"), txn + 1);
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
    assert_eq!(txn.commit().expect("commit"), after_steady(0));
}

/// A write that finds no page number left (the writer's allocator set at the last one) is
/// `Error::Full` and latches the environment: its transaction cannot commit, and a reopen finds
/// the last commit.
#[test]
fn a_write_with_no_page_number_left_is_full_and_latches() {
    let (_dir, path) = temp();
    let env = open(&path);
    let last = put_all(&env, "a", &[(b"k", b"v")]);
    env.with_writer(|w| w.alloc = Allocator::new(w.alloc.txn(), crate::tree::NULL_PGNO));
    let mut txn = env.begin_rw().expect("begin");
    let a = table(&txn, "a");
    let got = txn.put(&a, b"k2", b"w");
    assert!(matches!(&got, Err(Error::Full(_))), "{got:?}");
    assert!(env.failure().is_some(), "latched");
    let done = txn.commit();
    assert!(matches!(&done, Err(Error::Failed(_))), "{done:?}");
    drop(env);
    assert_eq!(open(&path).stat().txn_id, last);
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
    let first = put_all(&env, "a", &[(b"k", b"v")]);
    assert_eq!(first, after_steady(0));
    put_all(&env, "empty", &[]);
    let before = fs::read(&path).expect("read");
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    let empty = txn.create_table("empty").expect("empty");
    assert!(!txn.delete(&a, b"missing").expect("delete"));
    txn.clear_table(&empty).expect("clear");
    assert_eq!(txn.commit().expect("commit"), first + 1);
    assert_eq!(env.stat().txn_id, first + 1);
    assert_eq!(env.with_writer(|w| w.slots), [Slot::steady(first + 1), Slot::steady(first)]);
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
    assert_eq!(txn.commit().expect("commit"), after_steady(0));
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
    let first = put_all(&env, "a", &[(b"k", b"1")]);
    assert_eq!(first, after_steady(0));
    put_all(&env, "a", &[(b"k", b"2")]);
    assert_eq!(oldest(&env), 0);
    let r2 = env.begin_ro().expect("ro");
    drop(r0);
    assert_eq!(oldest(&env), first + 1);
    put_all(&env, "a", &[(b"k", b"3")]);
    assert_eq!(oldest(&env), first + 1);
    drop(r2);
    assert_eq!(oldest(&env), first + 2);

    let stuck = env.begin_ro().expect("ro");
    for i in 0..200_u32 {
        put_all(&env, "a", &[(b"k", &i.to_be_bytes())]);
    }
    assert_eq!(oldest(&env), first + 2);
    let queued = env.with_writer(|w| w.readers.len());
    assert!(queued <= 2 * 16 + 1, "the queue holds {queued} snapshots");
    drop(stuck);
    assert_eq!(oldest(&env), first + 202);
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
/// adjacent pages is synced by one `msync`, and anything else by one `fdatasync` of the file (on
/// macOS by the `msync`s of each merged range).
#[test]
fn dirty_pages_form_one_range_or_take_one_fdatasync() {
    #[cfg(target_os = "macos")]
    let many = DataSync::Ranges;
    #[cfg(not(target_os = "macos"))]
    let many = DataSync::File;
    let p = 4096;
    assert_eq!(data_sync(&[], p), DataSync::Nothing);
    assert_eq!(data_sync(&[7], p), DataSync::Range(7 * p..8 * p));
    assert_eq!(data_sync(&[2, 3, 4], p), DataSync::Range(2 * p..5 * p));
    assert_eq!(data_sync(&[2, 67], p), DataSync::Range(2 * p..68 * p), "a gap of 64 merges");
    assert_eq!(data_sync(&[2, 68], p), many, "65 does not: two ranges");
    let apart: Vec<u32> = (0..3).map(|i| 2 + i * 100).collect();
    assert_eq!(data_sync(&apart, p), many, "three ranges");
    // Runs that merge into one range still count one by one.
    let combed: Vec<u32> = (0..16).flat_map(|i| [2 + i * 3, 3 + i * 3]).collect();
    let one = DataSync::Range(2 * p..49 * p);
    assert_eq!(data_sync(&combed, p), one, "16 runs one page apart");
    let combed: Vec<u32> = (0..17).map(|i| 2 + i * 2).collect();
    assert_eq!(data_sync(&combed, p), many, "17 runs one page apart");
}

/// macOS: once a durable commit returns, no page of the file is dirty in memory only, the
/// scattered commits of a mature file included (step 4's [`DataSync::Ranges`]): an `fsync` there
/// leaves some pages dirtied through the mapping unwritten, while the meta naming them is on disk.
#[test]
#[cfg(target_os = "macos")]
fn a_scattered_durable_commit_leaves_no_page_dirty() {
    let (_dir, path) = temp();
    let env = open(&path);
    let n = 40_000u32;
    let key = |i: u32| i.wrapping_mul(2_654_435_761).to_be_bytes();
    let value = |round: u32, i: u32| [(round ^ i) as u8; 200];
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..n {
        txn.put(&t, &key(i), &value(0, i)).expect("put");
    }
    txn.commit().expect("commit");
    let mut scattered = 0;
    for round in 1..8u32 {
        let mut txn = env.begin_rw().expect("begin");
        let t = table(&txn, "t");
        for j in 0..3000u32 {
            let i = j.wrapping_mul(7919).wrapping_add(round * 104_729) % n;
            txn.put(&t, &key(i), &value(round, i)).expect("put");
        }
        let before = env.io_stats();
        txn.commit().expect("commit");
        scattered += env.io_stats().since(&before).fdatasync_syncs;
        let dirty = env.with_writer(|w| w.file.dirty_os_pages());
        assert_eq!(dirty, 0, "round {round}: OS pages left dirty after the commit returned");
    }
    assert!(scattered >= 4, "most commits took the scattered path ({scattered})");
}

/// macOS: a writer open writes the pages another mapping of the file left dirty before it builds
/// on them: those of a crashed writer, or here of a mapping still held. An `fsync` (open step 4)
/// writes none of them there.
#[test]
#[cfg(target_os = "macos")]
fn a_writer_open_writes_the_pages_another_mapping_left_dirty() {
    let (_dir, path) = temp();
    let env = open(&path);
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..3000u32 {
        txn.put(&t, &i.to_be_bytes(), &[i as u8; 4000]).expect("put");
    }
    txn.commit().expect("commit");
    drop(env);
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    // SAFETY: nothing else changes the file while the test maps it, and every store writes back
    // the byte it read, so the file holds what was committed.
    let mut map = unsafe { memmap2::MmapMut::map_mut(&file) }.expect("map");
    for at in (0..map.len()).step_by(sys::os_page_size() as usize) {
        map[at] = std::hint::black_box(map[at]);
    }
    drop(file);
    let env = open(&path);
    assert_eq!(env.with_writer(|w| w.file.dirty_os_pages()), 0, "pages left dirty after the open");
    env.check().expect("check");
    drop(map);
}

/// macOS: the ranges [`DataSync::Ranges`] msyncs are the merged ranges [`data_sync`] counts.
#[test]
#[cfg(target_os = "macos")]
fn scattered_pages_msync_their_merged_ranges() {
    let p = 4096;
    let ranges = |pages: &[u32]| {
        merged_ranges(pages, p).map(|r| (r.start / p, r.end / p)).collect::<Vec<_>>()
    };
    assert_eq!(ranges(&[7]), [(7, 8)]);
    assert_eq!(ranges(&[2, 3, 68]), [(2, 69)], "a gap of 64 merges");
    assert_eq!(ranges(&[2, 68, 69, 300]), [(2, 3), (68, 70), (300, 301)], "65 does not");
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

/// Run the stamp tests' commits on a new file at `path`, with commit step 3's CRCs computed on
/// `threads` threads (`Writer::stamp_threads`; `None`: `stamp::threads`' choice), then reopen
/// it, check it and read every value back. The commits stamp tree pages and overflow runs of 1 to
/// 10 pages, replace values in both directions and delete some; `big` adds a commit of 3 MiB of
/// runs, past `stamp::threads`' threshold. Returns the threads each commit's CRCs took
/// (`Writer::stamped_on`).
fn stamp_workload(path: &Path, threads: Option<usize>, big: bool) -> Vec<usize> {
    const LENS: [usize; 6] = [100, 3000, 5000, 9000, 20_000, 37_000];
    let env = open(path);
    env.with_writer(|w| w.stamp_threads = threads);
    let mut model = BTreeMap::new();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("s").expect("table");
    for i in 0u32..60 {
        let v = stamp_value(i, LENS[i as usize % 6] + i as usize);
        txn.put(&t, &i.to_be_bytes(), &v).expect("put");
        model.insert(i, v);
    }
    txn.commit().expect("commit");
    let mut on = vec![env.with_writer(|w| w.stamped_on)];
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
    txn.commit().expect("commit");
    on.push(env.with_writer(|w| w.stamped_on));
    if big {
        let mut txn = env.begin_rw().expect("begin");
        for i in 100u32..148 {
            let v = stamp_value(i, 64 << 10);
            txn.put(&t, &i.to_be_bytes(), &v).expect("put");
            model.insert(i, v);
        }
        txn.commit().expect("commit");
        on.push(env.with_writer(|w| w.stamped_on));
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
    on
}

#[test]
fn a_parallel_stamp_writes_the_serial_stamps_bytes() {
    // The same commits, their CRCs computed on 2, 3 and 5 threads, leave the file the serial
    // loop leaves, byte for byte, which reopens, checks and reads back.
    let (_serial_dir, serial) = temp();
    assert_eq!(stamp_workload(&serial, Some(1), false), [1, 1]);
    let serial = fs::read(&serial).expect("read");
    for threads in [2, 3, 5] {
        let (_dir, path) = temp();
        assert_eq!(stamp_workload(&path, Some(threads), false), [threads, threads]);
        assert!(fs::read(&path).expect("read") == serial, "{threads} threads");
    }
}

#[test]
fn a_commit_past_the_stamp_threshold_writes_the_serial_stamps_bytes() {
    // A commit of 3 MiB of runs takes several threads by `stamp::threads`' own choice, unless
    // the process may use one CPU only, and leaves the file the serial loop leaves.
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let (_serial_dir, serial) = temp();
    assert_eq!(stamp_workload(&serial, Some(1), true), [1, 1, 1]);
    let (_dir, path) = temp();
    let on = stamp_workload(&path, None, true);
    assert_eq!(on[..2], [1, 1], "the small commits stamp serially");
    assert!(cpus == 1 || on[2] > 1, "the 3 MiB commit takes several threads: {on:?}");
    assert!(fs::read(&path).expect("read") == fs::read(&serial).expect("read"));
}

/// Commit rows `start..start + n` into table `name` (created if missing) in one transaction,
/// each value `len` bytes of the row's low byte; returns the commit's txn.
fn commit_rows(env: &Env, name: &str, start: u32, n: u32, len: usize) -> u64 {
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table(name).expect("table");
    for i in start..start + n {
        txn.put(&table, &i.to_be_bytes(), &vec![i as u8; len]).expect("put");
    }
    txn.commit().expect("commit")
}

/// Every row `0..n` of table `name` reads back as [`commit_rows`] wrote it.
fn assert_rows(env: &Env, name: &str, n: u32, len: usize) {
    let ro = env.begin_ro().expect("ro");
    let table = table(&ro, name);
    for i in 0..n {
        assert_eq!(val(&ro, &table, &i.to_be_bytes()), Some(vec![i as u8; len]), "row {i}");
    }
    assert_eq!(ro.table_stat(&table).expect("stat").entries, u64::from(n));
}

/// The file's length on disk.
fn file_len(path: &Path) -> u64 {
    fs::metadata(path).expect("metadata").len()
}

/// `EnvOptions::max_growth` defaults to 1 GiB; under `MIN_GROWTH` it is refused at open as
/// `InvalidMaxGrowth`, before the file is touched: a missing file is not created, an existing
/// one keeps every byte. `MIN_GROWTH` itself opens, and a read-only open ignores the field.
#[test]
fn max_growth_under_the_minimum_is_refused_at_open() {
    assert_eq!(EnvOptions::default().max_growth, 1 << 30);
    let (dir, path) = temp();
    let small = EnvOptions { max_growth: crate::MIN_GROWTH - 1, ..EnvOptions::default() };
    assert!(matches!(
        Env::open(&path, small),
        Err(Error::InvalidMaxGrowth(g)) if g == crate::MIN_GROWTH - 1
    ));
    assert_eq!(
        fs::read_dir(dir.path()).expect("dir").count(),
        0,
        "the refused open created a file"
    );
    put_all(&open(&path), "a", &[(b"k", b"v")]);
    let before = fs::read(&path).expect("read");
    assert!(matches!(Env::open(&path, small), Err(Error::InvalidMaxGrowth(_))));
    assert!(fs::read(&path).expect("read") == before, "the refused open changed the file");
    let ro = Env::open_read_only(&path, small).expect("a read-only open ignores max_growth");
    drop(ro);
    let least = EnvOptions { max_growth: crate::MIN_GROWTH, ..EnvOptions::default() };
    let env = Env::open(&path, least).expect("the minimum opens");
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"v"[..]));
}

/// An `EnvOptions::map_reserve` past `isize::MAX` can never be mapped: it is refused at open as
/// `InvalidMapReserve`, before the file is touched (a missing file is not created, an existing
/// one keeps every byte), and a read-only open, which maps the file at its length, ignores it.
/// `isize::MAX` itself is no refusal but the mapping's (`OutOfMemory`, after the create). A tiny
/// reservation opens and grows (each growth moves the mapping).
#[test]
fn a_map_reserve_past_isize_max_is_refused_at_open() {
    let (dir, path) = temp();
    for reserve in [u64::MAX, isize::MAX as u64 + 1] {
        let huge = EnvOptions { map_reserve: reserve, ..EnvOptions::default() };
        assert!(matches!(Env::open(&path, huge), Err(Error::InvalidMapReserve(r)) if r == reserve));
        let created = fs::read_dir(dir.path()).expect("dir").count();
        assert_eq!(created, 0, "the refused open of {reserve} created a file");
    }
    let huge = EnvOptions { map_reserve: u64::MAX, ..EnvOptions::default() };
    put_all(&open(&path), "a", &[(b"k", b"v")]);
    let before = fs::read(&path).expect("read");
    assert!(matches!(Env::open(&path, huge), Err(Error::InvalidMapReserve(_))));
    assert!(fs::read(&path).expect("read") == before, "the refused open changed the file");
    let ro = Env::open_read_only(&path, huge).expect("a read-only open ignores map_reserve");
    drop(ro);
    let most = EnvOptions { map_reserve: isize::MAX as u64, ..EnvOptions::default() };
    let refused = Env::open(&path, most).map(drop);
    let Err(Error::OutOfMemory { bytes, source }) = &refused else { panic!("{refused:?}") };
    assert_eq!((*bytes, source.raw_os_error()), (isize::MAX as u64, Some(libc::ENOMEM)));
    assert!(refused.unwrap_err().to_string().contains("out of memory"), "the mapping's refusal");
    let tiny = EnvOptions { map_reserve: 1, ..EnvOptions::default() };
    let env = Env::open(&path, tiny).expect("a tiny reservation opens");
    put_all(&env, "b", &[(b"k", &[7; 3 << 20][..])]);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(val(&ro, &table(&ro, "a"), b"k").as_deref(), Some(&b"v"[..]));
    assert_eq!(val(&ro, &table(&ro, "b"), b"k").map(|v| v.len()), Some(3 << 20));
}

/// A growth cap of `MIN_GROWTH` bounds the file's tail past its pages while the environment
/// runs: after every commit of a load that grows the file to several MiB, the length past
/// `page_count` pages is at most the cap plus the OS-page rounding (doubling would leave up to
/// the file's length).
#[test]
fn a_growth_cap_bounds_the_tail_past_the_pages() {
    let (_dir, path) = temp();
    let cap = crate::MIN_GROWTH;
    let env =
        Env::open(&path, EnvOptions { max_growth: cap, ..EnvOptions::default() }).expect("open");
    for round in 0..16 {
        // 100 values of 3000 bytes, each a one-page run: about 400 KiB a commit.
        commit_rows(&env, "a", round * 100, 100, 3000);
        let pages = u64::from(env.stat().page_count) * 4096;
        let len = env.with_writer(|w| w.file.file_len());
        assert!(len >= pages);
        assert!(
            len - pages <= cap + sys::os_page_size(),
            "round {round}: {len} bytes for {pages} bytes of pages"
        );
    }
    assert!(env.with_writer(|w| w.file.file_len()) > 5 * cap, "the load grew the file");
}

/// The page sizes the trim tests run at: 4 KiB, and the OS page when it is larger (one page is
/// then one OS page, so the trim's rounding adds nothing and one page fewer is a shorter file).
fn trim_page_sizes() -> Vec<u32> {
    let os = sys::os_page_size() as u32;
    if os > 4096 {
        vec![4096, os]
    } else {
        vec![4096]
    }
}

/// The length a clean close trims a file of `env`'s newest state to: its pages, rounded up to
/// the OS page.
fn trimmed_len(env: &Env) -> u64 {
    let stat = env.stat();
    (u64::from(stat.page_count) * u64::from(stat.page_size)).next_multiple_of(sys::os_page_size())
}

/// The file's allocated bytes (`st_blocks` × 512).
fn allocated(path: &Path) -> u64 {
    fs::metadata(path).expect("metadata").blocks() * 512
}

/// A clean close of an environment whose file grew past its pages truncates the file to its
/// pages, rounded up to the OS page, in `Durable` and `NoSync` (whose final sync runs first, so
/// the commits since the last sync are there after the reopen). The file reopens to the newest
/// state, passes `check` and reads back every row. The allocated blocks never grow, and where
/// the file system preallocated the tail (blocks for the whole length) they fall by about the
/// bytes cut.
#[test]
fn a_clean_close_trims_the_file_to_its_pages() {
    for page_size in trim_page_sizes() {
        for sync in [SyncMode::Durable, SyncMode::NoSync] {
            let (_dir, path) = temp();
            let env = Env::open(&path, options(page_size, sync)).expect("open");
            commit_rows(&env, "a", 0, 300, 150);
            env.sync().expect("sync");
            let txn = commit_rows(&env, "a", 300, 100, 150);
            let want = trimmed_len(&env);
            let before = env.with_writer(|w| w.file.file_len());
            assert!(want < before, "{page_size} {sync:?}: the file grew past its pages");
            let blocks = allocated(&path);
            drop(env);
            let ctx = format!("{page_size} {sync:?}");
            let (trimmed, after) = (file_len(&path), allocated(&path));
            let env = Env::open(&path, options(page_size, sync)).expect("reopen");
            assert_eq!(env.stat().txn_id, txn, "{ctx}");
            env.check().expect("check");
            assert_rows(&env, "a", 400, 150);
            assert_eq!(trimmed, want, "{ctx}: trimmed to the pages");
            assert!(after <= blocks, "{ctx}: {after} allocated bytes, {blocks} before");
            if blocks >= before {
                let cut = before - want;
                assert!(
                    after + cut <= blocks + sys::os_page_size(),
                    "{ctx}: {after} allocated bytes, {blocks} before the {cut}-byte cut"
                );
            }
        }
    }
}

/// A latched environment drops without a trim: its file keeps its length. The latch is a failed
/// allocation's, which leaves the file unpoisoned, so only the latch stops the trim.
#[test]
fn a_latched_env_drops_without_a_trim() {
    let (_dir, path) = temp();
    let env = open(&path);
    commit_rows(&env, "a", 0, 50, 150);
    let mut txn = env.begin_rw().expect("begin");
    let a = txn.create_table("a").expect("a");
    txn.writer_mut().alloc.fail_allocs_after = Some(0);
    assert!(matches!(txn.put(&a, b"k", &[1; 3000]), Err(Error::Io(_))));
    drop(txn);
    assert!(env.failure().is_some(), "latched");
    assert!(!env.with_writer(|w| w.file.is_poisoned()), "the file is not poisoned");
    let before = env.with_writer(|w| w.file.file_len());
    assert!(trimmed_len(&env) < before, "a trim would shorten the file");
    drop(env);
    assert_eq!(file_len(&path), before, "a latched close trims nothing");
}

/// A crash (`Env::crash`) drops without a trim: the file keeps its length.
#[test]
fn a_crash_drops_without_a_trim() {
    let (_dir, path) = temp();
    let env = open(&path);
    commit_rows(&env, "a", 0, 50, 150);
    let before = env.with_writer(|w| w.file.file_len());
    assert!(trimmed_len(&env) < before, "a trim would shorten the file");
    env.crash();
    assert_eq!(file_len(&path), before, "a crash trims nothing");
}

/// A tail an earlier process left past the pages in use is not taken as preallocated: a crash
/// keeps the slack, and a sparse copy of the file can make it holes (modeled by cutting the tail
/// and extending the file again). The reopen writes nothing. The first growth past the pages cuts
/// the tail and grows from the pages, one step, with real blocks under the whole file, and a
/// clean close and a reopen find every row. On Linux the extension is a hole (ext4, XFS, btrfs,
/// tmpfs), which the test asserts, and stays one until each page is written, so there the blocks
/// check bites. APFS leaves the hole too, but fills a sparse file's holes at its first write and
/// msync through a mapping, so there the blocks check passes either way and the length check
/// shows the cut.
#[test]
fn an_inherited_tail_is_cut_and_regrown_with_real_blocks() {
    let (_dir, path) = temp();
    let env = open(&path);
    commit_rows(&env, "a", 0, 300, 150);
    let stat = env.stat();
    let used = u64::from(stat.page_count) * u64::from(stat.page_size);
    let len = env.with_writer(|w| w.file.file_len());
    assert!(used + 8 * 4096 <= len, "the file grew past its pages: {used} of {len}");
    env.crash();
    let file = fs::OpenOptions::new().write(true).open(&path).expect("open the file");
    file.set_len(used).expect("cut the tail");
    file.set_len(len).expect("extend it without blocks");
    drop(file);
    let sparse = allocated(&path);
    #[cfg(target_os = "linux")]
    assert!(
        sparse <= used + (len - used) / 2,
        "the tail is a hole: {sparse} allocated bytes, {used} of pages in a file of {len}"
    );

    let env = open(&path);
    assert_eq!((file_len(&path), allocated(&path)), (len, sparse), "the reopen writes nothing");
    commit_rows(&env, "a", 300, 100, 150);
    let grown = env.with_writer(|w| w.file.file_len());
    let want =
        (used + used.clamp(crate::MIN_GROWTH, 1 << 30)).next_multiple_of(sys::os_page_size());
    assert_eq!(grown, want, "one step from the pages in use, not from the inherited length {len}");
    assert!(
        allocated(&path) >= grown,
        "the regrown file is backed in full: {} allocated bytes of {grown} ({sparse} before)",
        allocated(&path)
    );
    drop(env);
    let env = open(&path);
    env.check().expect("check");
    assert_rows(&env, "a", 400, 150);
}

/// A `NoSync` close whose final sync fails drops without a trim: the file keeps its length and
/// reopens to the last commit's weak meta in the same boot, to the last synced state after a
/// reboot. The failure is an injected crash after the data sync, which leaves the file
/// unpoisoned, so only the failed sync stops the trim.
#[test]
fn a_failed_final_sync_skips_the_trim() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    commit_rows(&env, "a", 0, 300, 150);
    env.sync().expect("sync");
    let synced = env.stat().txn_id;
    commit_rows(&env, "a", 300, 100, 150);
    let before = env.with_writer(|w| w.file.file_len());
    assert!(trimmed_len(&env) < before, "a trim would shorten the file");
    env.with_writer(|w| w.crash_at = Some(CrashAt::AfterDataSync));
    drop(env);
    assert_eq!(file_len(&path), before, "a failed final sync trims nothing");
    let copy = path.with_extension("copy");
    fs::copy(&path, &copy).expect("copy");
    let env = open(&copy);
    assert_eq!(env.stat().txn_id, synced + 1, "the weak head, in the same boot");
    env.check().expect("check");
    assert_rows(&env, "a", 400, 150);
    disk_model::reboot();
    let env = open(&path);
    assert_eq!(env.stat().txn_id, synced, "the last synced state, after a reboot");
    env.check().expect("check");
    assert_rows(&env, "a", 300, 150);
}

/// The variable that makes [`a_forked_childs_drop_changes_nothing_in_the_child`] run: set only
/// in the process [`a_forked_childs_drop_leaves_the_parents_file_alone`] starts.
const FORK_CHILD: &str = "TN_LMDB_FORK_CHILD";

/// A process forks (without exec) while an environment is open, and the child drops its copy, as
/// a normal return would: the drop closes nothing, since the file, its lock and the writer's
/// state are the parent's. Without that the child's close would trim the shared file under the
/// parent (whose next writes into its slack would then fault, `SIGBUS`) and, in `NoSync`, sync
/// a stale meta. The forking test runs in a process of its own, this test binary run again, so
/// a fault or a hang fails this test rather than the suite.
#[test]
fn a_forked_childs_drop_leaves_the_parents_file_alone() {
    let name = "env::tests::a_forked_childs_drop_changes_nothing_in_the_child";
    let mut run = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", name, "--include-ignored", "--nocapture", "--test-threads", "1"])
        .env(FORK_CHILD, "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("run");
    let deadline = Instant::now() + Duration::from_secs(120);
    while run.try_wait().expect("wait").is_none() {
        if Instant::now() > deadline {
            let _ = run.kill();
            panic!("the forking process hung");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let out = run.wait_with_output().expect("output");
    let (stdout, stderr) =
        (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    use std::os::unix::process::ExitStatusExt as _;
    assert!(out.status.success(), "signal {:?}\n{stdout}\n{stderr}", out.status.signal());
    assert!(stdout.contains("1 passed"), "the forking test ran\n{stdout}");
}

/// The forking half of [`a_forked_childs_drop_leaves_the_parents_file_alone`], in `Durable` and
/// `NoSync` (an unsynced commit pending, after the open's first, durable, one: the
/// [`durable_first_commit`]): the child drops the environment and leaves at once
/// (`_exit`: nothing else of the process runs in it); the file is then byte for byte as it was,
/// and the parent's next commit, which writes into its slack, and the reopen are sound.
#[test]
#[ignore = "run by a_forked_childs_drop_leaves_the_parents_file_alone, in a process of its own"]
fn a_forked_childs_drop_changes_nothing_in_the_child() {
    if std::env::var_os(FORK_CHILD).is_none() {
        return;
    }
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let (_dir, path) = temp();
        let env = Env::open(&path, options(4096, sync)).expect("open");
        durable_first_commit(&env);
        commit_rows(&env, "a", 0, 50, 100);
        if sync == SyncMode::NoSync {
            assert!(env.with_writer(|w| w.unsynced_pages) > 0, "an unsynced commit pending");
        }
        let before = fs::read(&path).expect("read");
        // SAFETY: the child only drops the environment and calls `_exit`. The harness's other
        // thread waits on a channel, holding no lock the drop takes (both libcs make `malloc`
        // usable in a forked child).
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", io::Error::last_os_error());
        if pid == 0 {
            drop(env);
            // SAFETY: ends the child at once, running no exit handler of the parent's.
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        // SAFETY: waits for the child forked above, into a local.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0, "{sync:?}: {status}");
        assert!(fs::read(&path).expect("read") == before, "{sync:?}: the child changed the file");
        commit_rows(&env, "a", 50, 350, 1000);
        drop(env);
        let env = open(&path);
        env.check().expect("check");
        let ro = env.begin_ro().expect("ro");
        let a = table(&ro, "a");
        assert_eq!(ro.table_stat(&a).expect("stat").entries, 400, "{sync:?}");
        assert_eq!(val(&ro, &a, &399_u32.to_be_bytes()), Some(vec![399_u32 as u8; 1000]));
    }
}

/// After a trim the reopened file grows from its pages in use (the trimmed length less the
/// OS-page rounding, which the reopen does not trust: `PageFile::distrust_tail`), one step
/// (`clamp(used, MIN_GROWTH, max_growth)`) more, and a second clean close trims again.
#[test]
fn a_trimmed_file_grows_from_its_pages_in_use_and_trims_again() {
    let (_dir, path) = temp();
    let env = open(&path);
    commit_rows(&env, "a", 0, 300, 150);
    drop(env);
    let trimmed = file_len(&path);
    let env = open(&path);
    assert_eq!(env.with_writer(|w| w.file.file_len()), trimmed);
    assert_eq!(trimmed_len(&env), trimmed, "the close trimmed to the pages, rounded");
    let stat = env.stat();
    let used = u64::from(stat.page_count) * u64::from(stat.page_size);
    let mut rows = 300;
    while env.with_writer(|w| w.file.file_len()) == trimmed {
        assert!(rows < 20_000, "the file never grew");
        commit_rows(&env, "a", rows, 100, 150);
        rows += 100;
    }
    let grown = env.with_writer(|w| w.file.file_len());
    let step = used.clamp(crate::MIN_GROWTH, 1 << 30);
    assert_eq!(grown, (used + step).next_multiple_of(sys::os_page_size()), "one step from them");
    let want = trimmed_len(&env);
    assert!(want < grown);
    drop(env);
    assert_eq!(file_len(&path), want, "the second close trims again");
    let env = open(&path);
    env.check().expect("check");
    assert_rows(&env, "a", rows, 150);
}

/// A close of a file already at its trimmed length (reopened and never grown) trims nothing,
/// and its trim is no error. `EnvInner::drop` only logs a failed trim, so the test calls the
/// close's trim directly.
#[test]
fn a_close_of_a_file_at_its_trimmed_length_trims_nothing() {
    let (_dir, path) = temp();
    let env = open(&path);
    put_all(&env, "a", &[(b"k", b"1")]);
    drop(env);
    let trimmed = file_len(&path);
    let env = open(&path);
    assert_eq!(trimmed_len(&env), trimmed, "the first close trimmed to the pages, rounded");
    env.with_writer(|w| w.trim_tail()).expect("a trim to the file's own length");
    drop(env);
    assert_eq!(file_len(&path), trimmed);
}

/// A trim never cuts a page the newest meta names: after a commit followed by an aborted
/// transaction that grew the file, and after a commit whose new pages end the file (more than
/// an OS page of them, so the older slot's state names fewer pages), the close leaves at least
/// the newest state's pages, exactly its pages rounded to the OS page, and the file reopens to
/// that state.
#[test]
fn a_trim_never_cuts_a_page_the_newest_meta_names() {
    for page_size in trim_page_sizes() {
        // An aborted transaction grew the file past the committed pages.
        let (_dir, path) = temp();
        let env = Env::open(&path, options(page_size, SyncMode::Durable)).expect("open");
        commit_rows(&env, "a", 0, 100, 150);
        let committed = env.with_writer(|w| w.file.file_len());
        let mut txn = env.begin_rw().expect("begin");
        let a = txn.create_table("a").expect("a");
        let mut i = 100_u32;
        while txn.writer_mut().file.file_len() == committed {
            txn.put(&a, &i.to_be_bytes(), &[7; 3000]).expect("put");
            i += 1;
        }
        drop(txn);
        let pages = u64::from(env.stat().page_count) * u64::from(page_size);
        let want = trimmed_len(&env);
        drop(env);
        assert_eq!(file_len(&path), want, "{page_size}: the aborted pages are cut");
        assert!(want >= pages);
        let env = Env::open(&path, options(page_size, SyncMode::Durable)).expect("reopen");
        env.check().expect("check");
        assert_rows(&env, "a", 100, 150);
        drop(env);

        // The newest commit's pages end the file, the older slot's state names fewer.
        let (_dir, path) = temp();
        let env = Env::open(&path, options(page_size, SyncMode::Durable)).expect("open");
        commit_rows(&env, "a", 0, 100, 150);
        let older = env.stat().page_count;
        commit_rows(&env, "b", 0, 20, 3000);
        let newest = env.stat().page_count;
        let os_pages = (sys::os_page_size() / u64::from(page_size)) as u32;
        assert!(newest >= older + 2 * os_pages.max(1), "{older} then {newest} pages");
        let want = trimmed_len(&env);
        drop(env);
        assert_eq!(file_len(&path), want, "{page_size}: the newest state's pages are kept");
        assert!(want >= u64::from(newest) * u64::from(page_size));
        let env = Env::open(&path, options(page_size, SyncMode::Durable)).expect("reopen");
        assert_eq!(env.stat().page_count, newest);
        env.check().expect("check");
        assert_rows(&env, "a", 100, 150);
        assert_rows(&env, "b", 20, 3000);
    }
}

/// A meta slot whose record in the file no longer decodes to the txn the writer holds for it
/// is one the writer cannot account for: the close skips the trim, and the file keeps its
/// length.
#[test]
fn a_meta_slot_the_writer_cannot_account_for_skips_the_trim() {
    let (_dir, path) = temp();
    let env = open(&path);
    commit_rows(&env, "a", 0, 50, 150);
    commit_rows(&env, "a", 50, 50, 150);
    let before = env.with_writer(|w| {
        let txn = |slot: Option<Slot>| slot.expect("a valid slot").txn;
        let older = usize::from(txn(w.slots[1]) < txn(w.slots[0]));
        let page = w.geometry.page_size();
        w.file.page_mut(older as u32, page).expect("meta page")[8] ^= 0x10;
        w.file.file_len()
    });
    assert!(trimmed_len(&env) < before, "a trim would shorten the file");
    drop(env);
    assert_eq!(file_len(&path), before, "a slot in doubt trims nothing");
}

/// So is a slot whose record holds the txn the writer holds for it but is not of its kind:
/// `Writer::used_len` refuses the weak record of a `NoSync` commit rewritten as a steady one of
/// the same state, and takes it once it is put back.
#[test]
fn a_meta_slot_of_another_kind_is_one_the_writer_cannot_account_for() {
    let (_dir, path) = temp();
    let env = Env::open(&path, options(4096, SyncMode::NoSync)).expect("open");
    put_all(&env, "a", &[(b"k", b"1")]);
    let txn = put_all(&env, "a", &[(b"k", b"2")]);
    env.with_writer(|w| {
        let slot = w.slots.iter().position(|s| *s == Slot::weak(txn)).expect("the weak slot");
        let (meta, boot) = (w.last, w.boot_id);
        let write = |w: &mut Writer, tag: Tag| {
            w.file.page_mut(slot as u32, 4096).expect("meta page")[..META_LEN]
                .copy_from_slice(&meta.encode(tag));
        };
        write(w, Tag::steady(boot));
        let got = w.used_len();
        assert!(matches!(&got, Err(Error::Failed(m)) if m.contains("weak: true")), "{got:?}");
        write(w, Tag::weak(boot));
        w.used_len().expect("the record put back");
    });
}

/// A power loss right after a clean close's trim, whose truncation no sync made durable,
/// reopens to the last durable state with either length: a strict power loss brings the
/// untrimmed length back, a random one keeps either.
#[test]
fn a_power_loss_after_a_trim_reopens_to_the_last_durable_state() {
    let (dir, path) = temp();
    let env = open(&path);
    disk_model::track(&path);
    commit_rows(&env, "a", 0, 300, 150);
    let txn = commit_rows(&env, "a", 300, 100, 150);
    let untrimmed = env.with_writer(|w| w.file.file_len());
    let trimmed = trimmed_len(&env);
    assert!(trimmed < untrimmed);
    drop(env);
    assert_eq!(file_len(&path), trimmed);
    let reopen = |path: &Path| {
        let env = open(path);
        assert_eq!(env.stat().txn_id, txn);
        env.check().expect("check");
        assert_rows(&env, "a", 400, 150);
    };
    let mut lengths = std::collections::BTreeSet::new();
    for seed in 0..16 {
        let copy = dir.path().join(format!("copy{seed}"));
        disk_model::copy_tracked(&path, &copy);
        disk_model::power_loss_random(&copy, seed);
        let len = file_len(&copy);
        assert!(len == trimmed || len == untrimmed, "seed {seed}: {len} bytes");
        lengths.insert(len);
        disk_model::untrack(&copy);
        reopen(&copy);
    }
    assert_eq!(lengths.len(), 2, "the truncation is sometimes kept, sometimes lost");
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    assert_eq!(file_len(&path), untrimmed, "a strict power loss undoes the truncation");
    reopen(&path);
}
