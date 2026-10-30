//! Nested cells and dup table records at rest in a file whose tables are all plain: the engine
//! writes neither, so each is damage. A nested cell (flags `CELL_NESTED`, a valid encoded root)
//! in a published leaf, or a valid dup record in the catalog, with the page's CRC stamped again
//! so only the cell and record rules find it, is `Error::Corrupt` at every use that meets it (a
//! read, a write, a free, `Env::check`), and the error latches the environment.

use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use tempfile::TempDir;

use super::{
    damage::{restamped, tree, P},
    open, temp,
};
use crate::{
    catalog::{DupRecord, TableRecord},
    env::SyncMode,
    page::{self, Find, Fit, Val},
    tree::{read::descend, TreeRoot},
    Env, Error, ReadTxn as _, Result,
};

/// The rows of table `n`: several leaves at 4 KiB pages.
const ROWS: u32 = 300;

/// The row whose cell [`forged`] makes a nested cell, with a row either side in its leaf.
const K: u32 = 150;

/// What every use of a nested cell says ([`page::nested_error`]).
const NESTED: &str = "a nested cell in a tree that holds none";

/// What every use of a dup table's record says ([`TableRecord::plain`]).
const DUP: &str = "a dup table";

/// Row `i`'s key.
fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A cleanly closed file holding table `n`: [`ROWS`] rows of 32-byte values and one value in an
/// overflow run (so a free of the tree reads every leaf), whose row [`K`]'s cell was then made
/// a nested cell in its published leaf, the leaf's CRC stamped again.
struct Forged {
    _dir: TempDir,
    path: PathBuf,
}

fn forged() -> Forged {
    let (dir, path) = temp("tndb_b_nested");
    let leaf = {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("begin");
        let n = txn.create_table("n").expect("n");
        for i in 0..ROWS {
            txn.put(&n, &key(i), &[i as u8; 32]).expect("put");
        }
        txn.put(&n, &key(u32::MAX), &[7; 3 * P as usize]).expect("run");
        txn.commit().expect("commit");
        let root = tree(&env, "n");
        assert!(root.height >= 2 && root.overflow_pages > 0, "{root:?}");
        let snapshot = env.inner().current();
        let leaf_of = |i: u32| {
            descend(&*snapshot, &root, &key(i), |_, _, _, _| Ok(()), None)
                .expect("descend")
                .expect("leaf")
                .pgno
        };
        let leaf = leaf_of(K);
        assert!(leaf_of(K - 1) == leaf && leaf_of(K + 1) == leaf, "a row either side of K");
        leaf
    };
    let nested =
        TreeRoot { root: 9, root_txn: 7, height: 1, entries: 3, leaf_pages: 1, ..TreeRoot::EMPTY }
            .encode();
    restamped(&path, leaf, |page| {
        let Find::Found { index, .. } = page::leaf_find(page, &key(K)).expect("find") else {
            panic!("K is in its leaf");
        };
        let fit =
            page::LeafMut::new(page).expect("leaf").replace_value(index, Val::Nested(&nested));
        assert_eq!(fit.expect("a nested cell"), Fit::Done);
    });
    Forged { _dir: dir, path }
}

/// The environment of a fresh copy of the file at `path`, so each use meets the damage first.
fn copy_of(path: &Path) -> (TempDir, Env) {
    let (dir, copy) = temp("tndb_b_nested_case");
    fs::copy(path, &copy).expect("copy");
    let env = open(&copy, SyncMode::Durable);
    (dir, env)
}

/// `result` is `Error::Corrupt` saying `message`.
#[track_caller]
fn assert_corrupt<T: Debug>(what: &str, result: Result<T>, message: &str) {
    assert!(matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)), "{what}: {result:?}");
}

/// A forged nested cell in a committed plain table is met by every use that reads it: a get, a
/// public cursor's seek and its step onto it, an owned cursor's seek and its seek with the key's
/// tail, a detached write cursor's seek with the key's tail, a put over it, a put of another key
/// into its leaf (the leaf's copy-on-write checks every cell), its delete, a drop and a clear of
/// the table (which read every leaf, as the tree holds a run), and `Env::check`. Each is
/// `Corrupt` and latches.
#[test]
fn a_forged_nested_cell_is_corrupt_and_latches_at_every_use() {
    let fx = forged();
    let k = key(K);
    let beside = [&k[..], &[0]].concat();
    let value = [1; 32];
    type Read = (&'static str, fn(&Env, &[u8]) -> Result<()>);
    let reads: [Read; 5] = [
        ("get", |env, k| {
            let ro = env.begin_ro()?;
            let n = ro.open_table("n")?.expect("n");
            ro.get(&n, k).map(drop)
        }),
        ("cursor seek", |env, k| {
            let ro = env.begin_ro()?;
            let n = ro.open_table("n")?.expect("n");
            ro.cursor(&n)?.seek(k).map(drop)
        }),
        ("cursor next", |env, _| {
            let ro = env.begin_ro()?;
            let n = ro.open_table("n")?.expect("n");
            let mut cursor = ro.cursor(&n)?;
            assert!(cursor.seek(&key(K - 1))?.is_some(), "the row before K");
            cursor.next().map(drop)
        }),
        ("owned seek", |env, k| {
            let ro = Arc::new(env.begin_ro()?);
            let n = ro.open_table("n")?.expect("n");
            ro.cursor_owned(&n)?.seek(k).map(drop)
        }),
        ("owned seek_with_key_tail", |env, k| {
            let ro = Arc::new(env.begin_ro()?);
            let n = ro.open_table("n")?.expect("n");
            ro.cursor_owned(&n)?.seek_with_key_tail(k, 2).map(drop)
        }),
    ];
    for (what, read) in reads {
        let (_dir, env) = copy_of(&fx.path);
        assert_corrupt(what, read(&env, &k), NESTED);
        assert!(env.failure().is_some(), "{what}: latched");
    }
    for what in ["txn cursor seek_with_key_tail", "put", "put beside", "delete", "drop", "clear"] {
        let (_dir, env) = copy_of(&fx.path);
        let mut txn = env.begin_rw().expect("begin");
        let n = txn.open_table("n").expect("open").expect("n");
        let result = match what {
            "txn cursor seek_with_key_tail" => {
                let mut cursor = txn.cursor_detached(&n).expect("cursor");
                cursor.seek_with_key_tail(&txn, &k, 2).map(drop)
            }
            "put" => txn.put(&n, &k, &value),
            "put beside" => txn.put(&n, &beside, &value),
            "delete" => txn.delete(&n, &k).map(drop),
            "drop" => txn.drop_table(n),
            _ => txn.clear_table(&n),
        };
        assert_corrupt(what, result, NESTED);
        drop(txn);
        assert!(env.failure().is_some(), "{what}: latched");
    }
    let (_dir, env) = copy_of(&fx.path);
    assert_corrupt("check", env.check(), NESTED);
    assert!(env.failure().is_some(), "check: latched");
}

/// A valid dup table record (the table's tree intact, each row counted as a nested tree) in the
/// catalog, its leaf's CRC stamped again, is refused wherever a table is resolved: a read
/// transaction's and a write transaction's `open_table`, `create_table` of its name, a get and
/// a drop through a handle taken before, and `Env::check`'s catalog walk. Each is `Corrupt` and
/// latches.
#[test]
fn a_dup_table_record_is_refused_at_open_drop_and_check() {
    let (_dir, path) = temp("tndb_b_dup_record");
    let (d, catalog) = {
        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("begin");
        let d = txn.create_table("d").expect("d");
        let p = txn.create_table("p").expect("p");
        for i in 0..3 {
            txn.put(&d, &key(i), b"v").expect("put d");
            txn.put(&p, &key(i), b"v").expect("put p");
        }
        txn.commit().expect("commit");
        let meta = env.inner().current().meta;
        assert_eq!(meta.catalog.height, 1, "the catalog is one leaf");
        (d, meta.catalog.root)
    };
    restamped(&path, catalog, |page| {
        let i = (0..page::nkeys(page))
            .find(|&i| page::leaf_inline_entry(page, i).expect("entry").0 == b"d")
            .expect("d's record");
        let (_, value) = page::leaf_inline_entry(page, i).expect("entry");
        let off = value.as_ptr() as usize - page.as_ptr() as usize;
        let mut record = TableRecord::decode(value).expect("record");
        let rows = record.root.entries;
        assert_eq!((rows, record.dup), (3, None), "a plain table of three rows");
        record.dup = Some(DupRecord {
            key_len: 4,
            nested_trees: rows as u32,
            nested_entries: rows,
            nested_leaf_pages: rows as u32,
            ..DupRecord::default()
        });
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("a valid dup record"), record);
        page[off..off + bytes.len()].copy_from_slice(&bytes);
    });
    {
        let (_dir, env) = copy_of(&path);
        let ro = env.begin_ro().expect("ro");
        let p = ro.open_table("p").expect("a plain table's record").expect("p");
        assert_eq!(ro.get(&p, &key(0)).expect("get"), Some(&b"v"[..]));
        assert!(env.failure().is_none(), "the plain table met no damage");
    }
    for what in ["ro open_table", "ro get", "rw open_table", "create_table", "drop_table", "check"]
    {
        let (_dir, env) = copy_of(&path);
        let result = match what {
            "ro open_table" => env.begin_ro().expect("ro").open_table("d").map(drop),
            "ro get" => env.begin_ro().expect("ro").get(&d, &key(0)).map(drop),
            "rw open_table" => env.begin_rw().expect("rw").open_table("d").map(drop),
            "create_table" => env.begin_rw().expect("rw").create_table("d").map(drop),
            "drop_table" => env.begin_rw().expect("rw").drop_table(d.clone()),
            _ => env.check().map(drop),
        };
        assert_corrupt(what, result, DUP);
        assert!(env.failure().is_some(), "{what}: latched");
    }
}
