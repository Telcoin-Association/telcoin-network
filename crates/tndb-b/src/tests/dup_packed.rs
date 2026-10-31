//! Dup tables with packed sets through the public API: the packed-set limit a table is created
//! with and the number its record holds; the gets, puts and deletes of packed keys, their
//! promotion to nested trees past the limit, `delete_prefix`, and the free walks of a drop and a
//! clear over packed sets.

use std::{collections::BTreeMap, path::Path};

use tempfile::TempDir;

use super::{
    check,
    damage::tree,
    dup::{copy_of, restamp_cell, restamp_record},
    open, temp,
};
use crate::{
    env::SyncMode,
    packed,
    page::{self, Find, Val},
    tree::PageRead as _,
    DupOptions, Env, EnvOptions, Error, ReadTxn, RoTxn, RwTxn, Table,
};

/// `create_dup_table` resolves `packed_max` at the file's page size before it records it: 0, 7 and
/// the longest inline value of the outer key are kept, [`DupOptions::PACKED_DEFAULT`] becomes that
/// longest value (as [`DupOptions::resolved`] says), and 1..=6 or one past the longest value is
/// `InvalidDupOptions`, creating nothing and with no latch. Asking again for the default or for
/// its number gives the same handle and changes nothing; another number is `TableKind`. The table
/// stat, in the creating transaction, after its commit and after a reopen, reports the number.
#[test]
fn packed_dup_options_resolve() {
    const K: u16 = 20;
    let shape = |packed_max| DupOptions { key_len: K, packed_max };
    for page_size in [4096, 16384] {
        let (_dir, path) = temp("tndb_b_dup_packed_options");
        let env_options = EnvOptions { page_size, ..EnvOptions::default() };
        let env = Env::open(&path, env_options).expect("open");
        let most = env.inner().geometry.max_value(usize::from(K));
        let most = u16::try_from(most).expect("a u16 limit");
        assert_eq!(most, if page_size == 4096 { 987 } else { 4059 }, "{page_size}");
        let resolved = shape(most);
        assert_eq!(DupOptions::packed(K), shape(DupOptions::PACKED_DEFAULT));
        assert_eq!(DupOptions::packed(K).resolved(page_size).expect("resolved"), resolved);

        let mut txn = env.begin_rw().expect("rw");
        for packed_max in [1, 2, 3, 4, 5, 6, most + 1] {
            let result = txn.create_dup_table("bad", shape(packed_max));
            let what = format!("packed_max {packed_max} outside 7..={most} for this page size");
            assert!(
                matches!(&result, Err(Error::InvalidDupOptions(m)) if m.contains(&what)),
                "{page_size} {packed_max}: {result:?}"
            );
            assert!(shape(packed_max).resolved(page_size).is_err(), "{page_size} {packed_max}");
        }
        assert_eq!(txn.open_table("bad").expect("open"), None, "nothing created");
        for (name, packed_max) in [("never", 0), ("seven", 7), ("most", most)] {
            let t = txn.create_dup_table(name, shape(packed_max)).expect("a limit in range");
            assert_eq!(txn.table_stat(&t).expect("stat").dup, Some(shape(packed_max)), "{name}");
            assert_eq!(shape(packed_max).resolved(page_size).expect("kept"), shape(packed_max));
        }
        let d = txn.create_dup_table("d", DupOptions::packed(K)).expect("the default");
        assert_eq!(txn.table_stat(&d).expect("stat").dup, Some(resolved), "in its own txn");
        assert_eq!(txn.create_dup_table("d", DupOptions::packed(K)).expect("again"), d);
        let base = txn.commit().expect("commit");

        let mut txn = env.begin_rw().expect("rw");
        assert_eq!(txn.create_dup_table("d", DupOptions::packed(K)).expect("the default"), d);
        assert_eq!(txn.create_dup_table("d", resolved).expect("its number"), d);
        for other in [0, 7, most - 1] {
            let result = txn.create_dup_table("d", shape(other));
            assert!(matches!(&result, Err(Error::TableKind(n)) if n == "d"), "{other}: {result:?}");
        }
        let most_table = txn.open_table("most").expect("open").expect("most");
        let again = txn.create_dup_table("most", DupOptions::packed(K)).expect("the default");
        assert_eq!(again, most_table, "a table made with the number answers the default");
        assert_eq!(txn.table_stat(&d).expect("stat").dup, Some(resolved), "after its commit");
        assert_eq!(txn.commit().expect("commit"), base, "nothing changed");
        assert!(env.failure().is_none(), "{page_size}: no latch");
        check(&env);
        drop(env);

        let env = Env::open(&path, env_options).expect("reopen");
        let ro = env.begin_ro().expect("ro");
        let d = ro.open_table("d").expect("open").expect("d");
        assert_eq!(ro.table_stat(&d).expect("stat").dup, Some(resolved), "after a reopen");
        assert_eq!(check(&env).tables["d"].entries, 0);
    }
}

/// The outer key width of the tests below.
const K: u16 = 4;

/// A dup table's rows by logical key.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The logical key of outer key `[tag; K]` and subkey `s`.
fn lkey(tag: u8, s: &[u8]) -> Vec<u8> {
    [&[tag; K as usize][..], s].concat()
}

/// A value of `len` bytes, each `b`.
fn val(len: usize, b: u8) -> Vec<u8> {
    vec![b; len]
}

/// The rows of a dup table by the test-only walk, in either transaction.
trait Walked: ReadTxn {
    fn walked(&self, t: &Table) -> Vec<(Vec<u8>, Vec<u8>)>;
}

impl Walked for RwTxn {
    fn walked(&self, t: &Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.dup_rows_for_test(t)
    }
}

impl Walked for RoTxn {
    fn walked(&self, t: &Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.dup_rows_for_test(t)
    }
}

/// `t`'s rows are `model`'s, by the walk (which checks the totals against the trees and sets
/// walked) and by a get of each, a get beside each misses, and the table stat counts the rows and
/// `trees` nested trees.
fn assert_rows(txn: &impl Walked, t: &Table, model: &Model, trees: u32, what: &str) {
    let rows: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(txn.walked(t), rows, "{what}: the rows");
    for (k, v) in model {
        assert_eq!(txn.get(t, k).expect("get"), Some(v.as_slice()), "{what}: get {k:02x?}");
        let beside = [k.as_slice(), &[0]].concat();
        if !model.contains_key(&beside) {
            assert_eq!(txn.get(t, &beside).expect("miss"), None, "{what}: {beside:02x?}");
        }
    }
    let stat = txn.table_stat(t).expect("stat");
    assert_eq!(stat.entries, model.len() as u64, "{what}: entries");
    assert_eq!(stat.nested_trees, trees, "{what}: nested trees");
}

/// A put into `t` and `model`.
fn put(txn: &mut RwTxn, t: &Table, model: &mut Model, key: Vec<u8>, value: Vec<u8>) {
    txn.put(t, &key, &value).expect("put");
    model.insert(key, value);
}

/// A packed set of rows of subkey and value lengths `rows` takes `2 + 4n + sum` bytes: at the
/// limit it stays packed, one byte more promotes it to a nested tree: at create, by an insert,
/// by an upsert of a row's value (the row count unchanged) and by an append (the new key by an
/// outer append, then a row past the last). Promoted keys take later rows into their trees;
/// packed keys stay packed below the limit. Rows, gets, `entries` and `nested_trees` in the
/// writing transaction, after its commit and after a reopen, at a small limit and the largest.
#[test]
fn a_set_at_the_bound_stays_packed_and_one_byte_more_promotes() {
    let env_options = EnvOptions { page_size: 4096, ..EnvOptions::default() };
    for small in [true, false] {
        let (_dir, path) = temp("tndb_b_dup_packed_bound");
        let env = Env::open(&path, env_options).expect("open");
        let most = env.inner().geometry.max_value(usize::from(K));
        let p = if small { 64 } else { most };
        let options = DupOptions { key_len: K, packed_max: u16::try_from(p).expect("a u16") };
        let mut txn = env.begin_rw().expect("rw");
        let t = txn.create_dup_table("d", options).expect("create");
        let (mut model, mut trees) = (Model::new(), 0);
        let what = |w: &str| format!("packed_max {p}: {w}");

        // At create: one row of 2 + 4 + 1 + (p - 7) == p bytes, then one byte more.
        put(&mut txn, &t, &mut model, lkey(1, &[1]), val(p - 7, 1));
        assert_rows(&txn, &t, &model, trees, &what("created at the limit"));
        put(&mut txn, &t, &mut model, lkey(2, &[1]), val(p - 6, 2));
        trees += 1;
        assert_rows(&txn, &t, &model, trees, &what("created past the limit"));

        // By insert: 2 + (4 + 1 + 1) + (4 + 1 + b) == p for b == p - 13.
        for (tag, extra) in [(3, 0), (4, 1)] {
            let (k1, k2) = (lkey(tag, &[1]), lkey(tag, &[2]));
            txn.insert(&t, &k1, &val(1, tag)).expect("insert");
            model.insert(k1, val(1, tag));
            txn.insert(&t, &k2, &val(p - 13 + extra, tag)).expect("insert");
            model.insert(k2, val(p - 13 + extra, tag));
            trees += extra as u32;
            assert_rows(&txn, &t, &model, trees, &what(&format!("inserted, {extra} past")));
        }

        // By upsert: two rows of 6 bytes, then the first row's value grown to p - 13 bytes
        // (the set then p bytes) or one more; the row count is unchanged.
        for (tag, extra) in [(5, 0), (6, 1)] {
            put(&mut txn, &t, &mut model, lkey(tag, &[1]), val(1, tag));
            put(&mut txn, &t, &mut model, lkey(tag, &[2]), val(1, tag));
            assert_rows(&txn, &t, &model, trees, &what("two rows"));
            put(&mut txn, &t, &mut model, lkey(tag, &[1]), val(p - 13 + extra, tag));
            trees += extra as u32;
            assert_rows(&txn, &t, &model, trees, &what(&format!("upserted, {extra} past")));
        }

        // By append: a new last outer key, then a row after its last subkey.
        for (tag, extra) in [(7, 0), (8, 1)] {
            let (k1, k2) = (lkey(tag, &[1]), lkey(tag, &[2]));
            txn.append(&t, &k1, &val(1, tag)).expect("append");
            model.insert(k1, val(1, tag));
            txn.append(&t, &k2, &val(p - 13 + extra, tag)).expect("append");
            model.insert(k2, val(p - 13 + extra, tag));
            trees += extra as u32;
            assert_rows(&txn, &t, &model, trees, &what(&format!("appended, {extra} past")));
        }

        // A promoted key takes a new row into its tree; a packed key at the limit promotes on
        // any new row; a packed key below it keeps packing.
        put(&mut txn, &t, &mut model, lkey(4, &[3]), val(3, 4));
        assert_rows(&txn, &t, &model, trees, &what("a row into a promoted key"));
        put(&mut txn, &t, &mut model, lkey(1, &[0]), Vec::new());
        trees += 1;
        assert_rows(&txn, &t, &model, trees, &what("a row past a set at the limit"));
        put(&mut txn, &t, &mut model, lkey(9, &[1]), val(1, 9));
        put(&mut txn, &t, &mut model, lkey(9, &[0]), val(2, 9));
        assert_rows(&txn, &t, &model, trees, &what("two rows below the limit"));
        txn.commit().expect("commit");

        let txn = env.begin_rw().expect("rw");
        assert_rows(&txn, &t, &model, trees, &what("after the commit"));
        drop(txn);
        assert!(env.failure().is_none(), "{p}: no latch");
        drop(env);

        let env = Env::open(&path, env_options).expect("reopen");
        let ro = env.begin_ro().expect("ro");
        let t = ro.open_table("d").expect("open").expect("d");
        assert_rows(&ro, &t, &model, trees, &what("after a reopen"));
        assert!(env.failure().is_none(), "{p}: no latch");
    }
}

/// An upsert that grows one row's value past the limit promotes the set with its row count
/// unchanged: every row reads back, the grown value too. Shrunk again, the value stays in the
/// nested tree: in the transaction, after its commit and after a reopen.
#[test]
fn an_upsert_that_grows_a_value_past_the_bound_promotes() {
    const P: usize = 64;
    let (_dir, path) = temp("tndb_b_dup_packed_upsert");
    let env_options = EnvOptions { page_size: 4096, ..EnvOptions::default() };
    let env = Env::open(&path, env_options).expect("open");
    let mut txn = env.begin_rw().expect("rw");
    let options = DupOptions { key_len: K, packed_max: P as u16 };
    let t = txn.create_dup_table("d", options).expect("create");
    let mut model = Model::new();
    for (tag, s) in [(1, 1), (1, 2), (1, 3), (2, 1)] {
        put(&mut txn, &t, &mut model, lkey(tag, &[s]), val(4, s));
    }
    assert_rows(&txn, &t, &model, 0, "packed");
    // Three rows of 9 bytes: 29; the middle value grown by P - 29 + 1 bytes takes it to P + 1.
    put(&mut txn, &t, &mut model, lkey(1, &[2]), val(4 + P - 29 + 1, 0xee));
    assert_rows(&txn, &t, &model, 1, "promoted by the upsert");
    put(&mut txn, &t, &mut model, lkey(1, &[2]), val(4, 2));
    assert_rows(&txn, &t, &model, 1, "shrunk, still nested");
    txn.commit().expect("commit");
    let txn = env.begin_rw().expect("rw");
    assert_rows(&txn, &t, &model, 1, "after the commit");
    drop(txn);
    drop(env);

    let env = Env::open(&path, env_options).expect("reopen");
    let mut txn = env.begin_rw().expect("rw");
    let t = txn.open_table("d").expect("open").expect("d");
    assert_rows(&txn, &t, &model, 1, "after a reopen");
    put(&mut txn, &t, &mut model, lkey(1, &[2]), val(2, 2));
    put(&mut txn, &t, &mut model, lkey(2, &[2]), val(2, 2));
    assert_rows(&txn, &t, &model, 1, "a nested and a packed key written again");
    txn.commit().expect("commit");
    assert!(env.failure().is_none(), "no latch");
}

/// A promoted key deleted down to one row stays a nested tree (a nested key never goes back to
/// a packed set), across a commit and a reopen; a new row then goes into that tree.
#[test]
fn a_nested_key_never_demotes() {
    const P: usize = 64;
    let (_dir, path) = temp("tndb_b_dup_packed_demote");
    let env_options = EnvOptions { page_size: 4096, ..EnvOptions::default() };
    let env = Env::open(&path, env_options).expect("open");
    let mut txn = env.begin_rw().expect("rw");
    let options = DupOptions { key_len: K, packed_max: P as u16 };
    let t = txn.create_dup_table("d", options).expect("create");
    let mut model = Model::new();
    // Rows of 4 + 1 + 8 bytes: four fit (54), the fifth promotes (67).
    for s in 1..=5 {
        put(&mut txn, &t, &mut model, lkey(1, &[s]), val(8, s));
        assert_rows(&txn, &t, &model, u32::from(s == 5), &format!("{s} rows"));
    }
    for s in 1..=4 {
        assert!(txn.delete(&t, &lkey(1, &[s])).expect("delete"), "{s}");
        model.remove(&lkey(1, &[s]));
        assert_rows(&txn, &t, &model, 1, &format!("row {s} deleted"));
    }
    txn.commit().expect("commit");
    let txn = env.begin_rw().expect("rw");
    assert_rows(&txn, &t, &model, 1, "one row, after the commit");
    drop(txn);
    check(&env);
    drop(env);

    let env = Env::open(&path, env_options).expect("reopen");
    let mut txn = env.begin_rw().expect("rw");
    let t = txn.open_table("d").expect("open").expect("d");
    assert_rows(&txn, &t, &model, 1, "one row, after a reopen");
    put(&mut txn, &t, &mut model, lkey(1, &[9]), val(1, 9));
    assert_rows(&txn, &t, &model, 1, "a row into the one-row tree");
    txn.commit().expect("commit");
    check(&env);
    assert!(env.failure().is_none(), "no latch");
}

/// Each packed write moves the table's write stamp: a create, an in-place rewrite (the outer
/// cell replaced in its private leaf), a new row, a promotion. A get, a miss, a refused insert
/// and a refused append move nothing and latch nothing.
#[test]
fn packed_writes_move_the_table_stamp() {
    let (_dir, path) = temp("tndb_b_dup_packed_stamp");
    let env_options = EnvOptions { page_size: 4096, ..EnvOptions::default() };
    let env = Env::open(&path, env_options).expect("open");
    let mut txn = env.begin_rw().expect("rw");
    let t = txn.create_dup_table("d", DupOptions { key_len: K, packed_max: 64 }).expect("create");
    let mut model = Model::new();
    let mut writes = txn.table_writes(&t);
    let mut moved = |txn: &RwTxn, what: &str, moves: bool| {
        let now = txn.table_writes(&t);
        assert_eq!(now > writes, moves, "{what}: {writes} -> {now}");
        assert!(now >= writes, "{what}");
        writes = now;
    };
    put(&mut txn, &t, &mut model, lkey(1, &[1]), val(4, 1));
    moved(&txn, "a packed create", true);
    put(&mut txn, &t, &mut model, lkey(1, &[1]), val(4, 2));
    moved(&txn, "an in-place rewrite", true);
    put(&mut txn, &t, &mut model, lkey(1, &[2]), val(4, 2));
    moved(&txn, "a new row", true);
    assert_eq!(txn.get(&t, &lkey(1, &[1])).expect("get"), Some(&val(4, 2)[..]));
    moved(&txn, "a get", false);
    assert_eq!(txn.get(&t, &lkey(1, &[3])).expect("get"), None);
    assert_eq!(txn.get(&t, &lkey(2, &[3])).expect("get"), None);
    moved(&txn, "a miss", false);
    assert!(matches!(txn.insert(&t, &lkey(1, &[2]), &[0]), Err(Error::KeyExists)));
    moved(&txn, "a refused insert", false);
    assert!(matches!(txn.append(&t, &lkey(1, &[2]), &[0]), Err(Error::AppendOutOfOrder)));
    assert!(matches!(txn.append(&t, &lkey(1, &[0]), &[0]), Err(Error::AppendOutOfOrder)));
    moved(&txn, "a refused append", false);
    put(&mut txn, &t, &mut model, lkey(1, &[3]), val(40, 3));
    moved(&txn, "a promotion", true);
    assert_rows(&txn, &t, &model, 1, "promoted");
    txn.commit().expect("commit");

    let mut txn = env.begin_rw().expect("rw");
    let mut writes = txn.table_writes(&t);
    put(&mut txn, &t, &mut model, lkey(2, &[1]), val(4, 1));
    assert!(txn.table_writes(&t) > writes, "a packed create in a new transaction");
    writes = txn.table_writes(&t);
    put(&mut txn, &t, &mut model, lkey(2, &[1]), val(4, 5));
    assert!(txn.table_writes(&t) > writes, "a rewrite in a leaf this transaction copied");
    assert_rows(&txn, &t, &model, 1, "the second transaction");
    txn.commit().expect("commit");

    // The deletes: a row of a set of more, a set's last row (its cell removed) and a packed
    // key's `delete_prefix` move the stamp; a miss in a set, of an absent key and of an absent
    // prefix do not.
    let mut txn = env.begin_rw().expect("rw");
    let mut writes = txn.table_writes(&t);
    let mut moved = |txn: &RwTxn, what: &str, moves: bool| {
        let now = txn.table_writes(&t);
        assert_eq!(now > writes, moves, "{what}: {writes} -> {now}");
        writes = now;
    };
    put(&mut txn, &t, &mut model, lkey(2, &[2]), val(4, 2));
    put(&mut txn, &t, &mut model, lkey(3, &[1]), val(4, 3));
    put(&mut txn, &t, &mut model, lkey(3, &[2]), val(4, 3));
    moved(&txn, "the puts", true);
    assert!(!txn.delete(&t, &lkey(2, &[9])).expect("delete"));
    moved(&txn, "a miss in a set", false);
    assert!(txn.delete(&t, &lkey(2, &[2])).expect("delete"));
    model.remove(&lkey(2, &[2]));
    moved(&txn, "a row delete", true);
    assert!(txn.delete(&t, &lkey(2, &[1])).expect("delete"));
    model.remove(&lkey(2, &[1]));
    moved(&txn, "a cell removal", true);
    assert!(!txn.delete(&t, &lkey(2, &[1])).expect("delete"));
    moved(&txn, "a miss of an absent key", false);
    assert_eq!(txn.delete_prefix(&t, &[3; K as usize]).expect("delete_prefix"), 2);
    model.retain(|k, _| k[..K as usize] != [3; K as usize]);
    moved(&txn, "a packed delete_prefix", true);
    assert_eq!(txn.delete_prefix(&t, &[3; K as usize]).expect("delete_prefix"), 0);
    moved(&txn, "an absent delete_prefix", false);
    assert_rows(&txn, &t, &model, 1, "the deletes");
    txn.commit().expect("commit");
    assert!(env.failure().is_none(), "no latch");
}

/// The packed-set limit of the tests below: rows of 4 + 1 + 8 bytes, so four fit (54) and a
/// fifth promotes (67).
const P: u16 = 64;

/// A fresh file of 4 KiB pages at `path`, with dup table `d` of width [`K`] and limit
/// `packed_max`, in an open write transaction.
fn fresh(path: &Path, packed_max: u16) -> (Env, RwTxn, Table) {
    let env_options = EnvOptions { page_size: 4096, ..EnvOptions::default() };
    let env = Env::open(path, env_options).expect("open");
    let mut txn = env.begin_rw().expect("rw");
    let t = txn.create_dup_table("d", DupOptions { key_len: K, packed_max }).expect("create");
    (env, txn, t)
}

/// `n` rows of 1-byte subkeys `1..=n` and 8-byte values under outer key `[tag; K]`.
fn rows(txn: &mut RwTxn, t: &Table, model: &mut Model, tag: u8, n: u8) {
    for s in 1..=n {
        put(txn, t, model, lkey(tag, &[s]), val(8, s ^ tag));
    }
}

/// The rows under outer key `[tag; K]` gone from `model`, and how many there were.
fn forget(model: &mut Model, tag: u8) -> u64 {
    let before = model.len();
    model.retain(|k, _| k[..K as usize] != [tag; K as usize]);
    (before - model.len()) as u64
}

/// A key's committed nested tree, and one this transaction promoted, emptied by deleting each
/// row and by `delete_prefix`, then put again in the same transaction, are packed sets: new
/// keys, not demotions (`nested_trees` falls to 0). A drop of the table in that transaction
/// frees cleanly (every working entry met, none for a packed key) and `check` finds no leak;
/// otherwise the rows read back after the commit and a reopen, and `check` passes once the
/// packed rows are gone.
#[test]
fn a_nested_key_emptied_then_recreated_is_packed_in_one_txn() {
    for dropping in [false, true] {
        let (_dir, path) = temp("tndb_b_dup_packed_recreate");
        let (env, mut txn, t) = fresh(&path, P);
        let mut model = Model::new();
        rows(&mut txn, &t, &mut model, 1, 5);
        rows(&mut txn, &t, &mut model, 2, 5);
        assert_rows(&txn, &t, &model, 2, "two committed nested keys");
        txn.commit().expect("commit");

        let mut txn = env.begin_rw().expect("rw");
        rows(&mut txn, &t, &mut model, 3, 5);
        rows(&mut txn, &t, &mut model, 4, 5);
        assert_rows(&txn, &t, &model, 4, "and two promoted in this transaction");
        for tag in [1, 3] {
            for s in 1..=5 {
                assert!(txn.delete(&t, &lkey(tag, &[s])).expect("delete"), "{tag} {s}");
            }
            forget(&mut model, tag);
        }
        assert_rows(&txn, &t, &model, 2, "emptied row by row");
        for tag in [2, 4] {
            assert_eq!(txn.delete_prefix(&t, &[tag; K as usize]).expect("prefix"), 5, "{tag}");
            forget(&mut model, tag);
        }
        assert_rows(&txn, &t, &model, 0, "emptied by delete_prefix");
        for tag in 1..=4 {
            rows(&mut txn, &t, &mut model, tag, tag);
        }
        assert_rows(&txn, &t, &model, 0, "put again: packed");
        if dropping {
            txn.drop_table(t).expect("a clean drop");
            txn.commit().expect("commit");
            let report = check(&env);
            assert!(!report.tables.contains_key("d"), "the table is gone");
            assert_eq!(report.table_pages, 0, "no table page left");
            assert!(env.failure().is_none(), "no latch");
            continue;
        }
        txn.commit().expect("commit");
        drop(env);

        let env = open(&path, SyncMode::Durable);
        let mut txn = env.begin_rw().expect("rw");
        let t = txn.open_table("d").expect("open").expect("d");
        assert_rows(&txn, &t, &model, 0, "after a reopen");
        for tag in 1..=4 {
            assert_eq!(txn.delete_prefix(&t, &[tag; K as usize]).expect("prefix"), u64::from(tag));
            forget(&mut model, tag);
        }
        assert_rows(&txn, &t, &model, 0, "every packed key removed");
        txn.commit().expect("commit");
        assert_eq!(check(&env).tables["d"].entries, 0);
        assert!(env.failure().is_none(), "no latch");
    }
}

/// [`bulk_load`]'s key S, whose packed phase straddles a commit.
const S: u32 = 1_000_000;

/// [`bulk_load`]'s key B, of 2,500 rows.
const B: u32 = 2_000_000;

/// The page count of the newest state of `env`.
fn page_count(env: &Env) -> u32 {
    env.inner().current().meta.page_count
}

/// The logical key of outer key `k` and subkey `s`, both big-endian.
fn bulk_key(k: u32, s: u32) -> Vec<u8> {
    [k.to_be_bytes(), s.to_be_bytes()].concat()
}

/// A bulk load by ascending appends with a commit every 1,000 rows, into a dup table of limit
/// `packed_max` (4 + 4 + 8 bytes a row: 15 rows fit 256 bytes, a 16th promotes). Small keys of 3
/// rows up to row 990; then key S of 20 rows, whose packed phase straddles the first commit (10
/// rows before it) and which promotes after it; small keys up to row 2,100; key B of 2,500 rows,
/// which promotes in its first transaction and goes on nested in the next two; then keys of 1 to
/// 15 rows. After every append `nested_trees` counts the keys past 15 rows (none at
/// `packed_max` 0: every key nested); at every commit the walk gives the rows and, for a table
/// that never packs, `check` passes. Returns the final page count.
fn bulk_load(packed_max: u16) -> u32 {
    let (_dir, path) = temp("tndb_b_dup_packed_bulk");
    let (env, txn, t) = fresh(&path, packed_max);
    let mut bulk = Bulk {
        env,
        txn: Some(txn),
        t,
        packed_max,
        model: Model::new(),
        appended: 0,
        commits: 0,
        promoted: 0,
        keys: 0,
    };
    let mut k = 0;
    while bulk.appended < 990 {
        bulk.load(k, 3);
        k += 1;
    }
    bulk.load(S, 20);
    let mut k = S + 1;
    while bulk.appended < 2_100 {
        bulk.load(k, 3);
        k += 1;
    }
    bulk.load(B, 2_500);
    let mut k = B + 1;
    while bulk.appended < 5_300 {
        bulk.load(k, 1 + k % 15);
        k += 1;
    }
    assert_eq!(bulk.commits, 5, "B spans three transactions");
    let Bulk { env, txn, t, model, .. } = bulk;
    let txn = txn.expect("a live transaction");
    if packed_max != 0 {
        assert_eq!(txn.table_stat(&t).expect("stat").nested_trees, 2, "only S and B nested");
    }
    txn.commit().expect("commit");
    let ro = env.begin_ro().expect("ro");
    let rows: Vec<_> = model.into_iter().collect();
    assert_eq!(ro.dup_rows_for_test(&t), rows, "{packed_max}: the committed rows");
    drop(ro);
    if packed_max == 0 {
        check(&env);
    }
    assert!(env.failure().is_none(), "no latch");
    page_count(&env)
}

/// [`bulk_load`]'s state: the open transaction, the model, the rows appended, the commits, the
/// keys past 15 rows and every key begun.
struct Bulk {
    env: Env,
    txn: Option<RwTxn>,
    t: Table,
    packed_max: u16,
    model: Model,
    appended: u32,
    commits: u32,
    promoted: u32,
    keys: u32,
}

impl Bulk {
    /// Append `n` rows under outer key `key`, with the checks of [`bulk_load`].
    fn load(&mut self, key: u32, n: u32) {
        let (t, packed_max) = (self.t.clone(), self.packed_max);
        self.keys += 1;
        for s in 0..n {
            let (lk, v) = (bulk_key(key, s), val(8, s as u8));
            let txn = self.txn.as_mut().expect("a live transaction");
            txn.append(&t, &lk, &v).expect("append");
            self.model.insert(lk, v);
            self.appended += 1;
            self.promoted += u32::from(s == 15);
            let trees = if packed_max == 0 { self.keys } else { self.promoted };
            let stat = txn.table_stat(&t).expect("stat");
            assert_eq!(stat.nested_trees, trees, "{packed_max}: key {key} row {s}");
            if key == S && self.appended == 1_000 {
                assert_eq!(s, 9, "S has 10 rows at the first commit");
            }
            if key == B && s == 15 {
                assert_eq!(self.appended / 1_000, 2, "B promotes in its first transaction");
            }
            if !self.appended.is_multiple_of(1_000) {
                continue;
            }
            let rows: Vec<_> = self.model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            assert_eq!(txn.dup_rows_for_test(&t), rows, "{packed_max}: at {}", self.appended);
            self.txn.take().expect("a live transaction").commit().expect("commit");
            self.commits += 1;
            if packed_max == 0 {
                check(&self.env);
            }
            self.txn = Some(self.env.begin_rw().expect("rw"));
        }
    }
}

/// [`bulk_load`] packing at 256 bytes ends in fewer pages than the same script in a table that
/// never packs.
#[test]
fn a_bulk_append_crosses_the_bound_inside_a_key_larger_than_a_commit() {
    let (packed, unpacked) = (bulk_load(256), bulk_load(0));
    assert!(packed < unpacked, "packed {packed} pages, unpacked {unpacked}");
}

/// `delete_prefix` of a packed key, committed or made in the transaction, returns its rows and
/// frees no page (the outer leaf already copied: nothing freed, the table's pages unchanged;
/// the file's page count flat over 50 cycles of put and remove); of a nested key it frees the
/// tree. A drop and a clear of a table of packed and nested keys, committed and new, free every
/// page (`check`); the clear keeps the table's limit, its packed rows are 0, and a new row is
/// packed again.
#[test]
fn drop_clear_and_delete_prefix_with_packed_and_nested_keys() {
    let (_dir, path) = temp("tndb_b_dup_packed_prefix");
    let (env, mut txn, t) = fresh(&path, P);
    let mut model = Model::new();
    for tag in 1..=6 {
        rows(&mut txn, &t, &mut model, tag, if tag % 2 == 0 { 6 } else { 3 });
    }
    assert_rows(&txn, &t, &model, 3, "committed: tags 2, 4, 6 nested");
    txn.commit().expect("commit");

    let mut txn = env.begin_rw().expect("rw");
    let pages = |txn: &RwTxn| {
        let stat = txn.table_stat(&t).expect("stat");
        stat.branch_pages + stat.leaf_pages + stat.overflow_pages
    };
    let before = pages(&txn);
    assert_eq!(txn.delete_prefix(&t, &[2; K as usize]).expect("nested"), forget(&mut model, 2));
    assert_eq!(pages(&txn), before - 1, "the nested key's one leaf freed");
    let (before, freed) = (pages(&txn), txn.writer_mut().alloc.freed().len());
    assert_eq!(txn.delete_prefix(&t, &[1; K as usize]).expect("packed"), forget(&mut model, 1));
    rows(&mut txn, &t, &mut model, 7, 2);
    assert_eq!(txn.delete_prefix(&t, &[7; K as usize]).expect("new packed"), forget(&mut model, 7));
    assert_eq!(pages(&txn), before, "no page for a packed key");
    assert_eq!(txn.writer_mut().alloc.freed().len(), freed, "nothing freed");
    rows(&mut txn, &t, &mut model, 8, 7);
    assert_eq!(pages(&txn), before + 1, "a new nested key's leaf");
    assert_eq!(txn.delete_prefix(&t, &[8; K as usize]).expect("new nested"), forget(&mut model, 8));
    assert_eq!(pages(&txn), before, "its leaf freed");
    assert_rows(&txn, &t, &model, 2, "after the prefixes");
    txn.commit().expect("commit");

    let mut flat = 0;
    for cycle in 0..50 {
        let mut txn = env.begin_rw().expect("rw");
        rows(&mut txn, &t, &mut model, 9, 3);
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("rw");
        assert_eq!(txn.delete_prefix(&t, &[9; K as usize]).expect("cycle"), forget(&mut model, 9));
        txn.commit().expect("commit");
        match cycle {
            0..3 => flat = page_count(&env),
            _ => assert_eq!(page_count(&env), flat, "cycle {cycle}: the file grew"),
        }
    }
    drop(env);

    for dropping in [false, true] {
        let (_copy, env) = copy_of(&path);
        let mut txn = env.begin_rw().expect("rw");
        let t = txn.open_table("d").expect("open").expect("d");
        let mut model = model.clone();
        rows(&mut txn, &t, &mut model, 10, 2);
        rows(&mut txn, &t, &mut model, 11, 6);
        assert_rows(&txn, &t, &model, 3, &format!("drop {dropping}: before"));
        if dropping {
            txn.drop_table(t).expect("drop");
            txn.commit().expect("commit");
            let report = check(&env);
            assert!(!report.tables.contains_key("d"), "the table is gone");
            assert_eq!(report.table_pages, 0, "no table page left");
            continue;
        }
        txn.clear_table(&t).expect("clear");
        let stat = txn.table_stat(&t).expect("stat");
        assert_eq!((stat.entries, stat.nested_trees), (0, 0), "cleared");
        assert_eq!(stat.dup, Some(DupOptions { key_len: K, packed_max: P }), "the limit kept");
        txn.commit().expect("commit");
        assert_eq!(check(&env).tables["d"].entries, 0, "no leak");
        let mut txn = env.begin_rw().expect("rw");
        let mut model = Model::new();
        rows(&mut txn, &t, &mut model, 1, 2);
        assert_rows(&txn, &t, &model, 0, "packed again after the clear");
        assert!(env.failure().is_none(), "no latch");
    }
}

/// A cleanly closed file `dir/db` holding dup table `d` (of [`K`] and [`P`]) in one outer leaf:
/// key 1 nested (5 rows), keys 2, 3 and 4 packed (3, 2 and 1 rows). Returns the path and the
/// outer leaf.
fn packed_fixture(dir: &Path) -> (std::path::PathBuf, u32) {
    let path = dir.join("db");
    let (env, mut txn, t) = fresh(&path, P);
    let mut model = Model::new();
    for (tag, n) in [(1, 5), (2, 3), (3, 2), (4, 1)] {
        rows(&mut txn, &t, &mut model, tag, n);
    }
    assert_rows(&txn, &t, &model, 1, "the fixture");
    txn.commit().expect("commit");
    let outer = tree(&env, "d");
    assert_eq!((outer.height, outer.entries), (1, 4), "one outer leaf");
    (path, outer.root)
}

/// Key 2's packed set in the outer leaf `leaf` of the file at `path`.
fn packed_set_of_key_2(path: &Path, leaf: u32) -> Vec<u8> {
    let env = open(path, SyncMode::Durable);
    let snapshot = env.inner().current();
    let page = snapshot.page(leaf).expect("the outer leaf");
    let Find::Found { value: Val::Inline(blob), .. } =
        page::leaf_find(page, &[2; K as usize]).expect("find")
    else {
        panic!("key 2's packed set");
    };
    blob.to_vec()
}

/// A drop and a clear refuse, before anything is freed and latching, a table whose record or
/// packed sets were forged (each CRC stamped again) so the free walk's sums miss: the record's
/// `packed_rows` one more, its outer root's entries one more (keeping every record rule), key 2's
/// set replaced by a valid set of its first two rows, and that set with its two subkeys swapped
/// (each row's length kept, so only the walk's whole validation sees it).
#[test]
fn a_free_walk_refuses_packed_totals_that_miss_its_sets() {
    let dir = TempDir::with_prefix("tndb_b_dup_packed_free_walk").expect("temp dir");
    let cases: [(&str, &str); 4] = [
        ("record rows", "its packed sets count 6 packed rows, its record 7"),
        ("record entries", "its outer tree counts 5 outer entries, its walk 1 nested trees and 3"),
        ("set rows", "its packed sets count 5 packed rows, its record 6"),
        ("misordered", "the packed set of key [02, 02, 02, 02]"),
    ];
    for (case, message) in cases {
        let case_dir = dir.path().join(case.replace(' ', "_"));
        std::fs::create_dir(&case_dir).expect("case dir");
        let (path, leaf) = packed_fixture(&case_dir);
        match case {
            "record rows" => {
                restamp_record(&path, |r| r.dup.as_mut().expect("dup").packed_rows += 1);
            }
            "record entries" => restamp_record(&path, |r| r.root.entries += 1),
            _ => {
                let blob = packed_set_of_key_2(&path, leaf);
                let row = |s: u8| [s].into_iter().chain(val(8, s ^ 2)).collect::<Vec<_>>();
                let at = |row: &[u8]| blob.windows(row.len()).position(|w| w == row).expect("row");
                let forged = if case == "set rows" {
                    let view = packed::validate(&blob, usize::from(P), 64).expect("a valid set");
                    let mut two = Vec::new();
                    packed::build_without(&view, 2, &mut two);
                    two
                } else {
                    let mut swapped = blob.clone();
                    swapped.swap(at(&row(1)), at(&row(2)));
                    swapped
                };
                restamp_cell(&path, leaf, &[2; K as usize], &forged[..]);
            }
        }
        for dropping in [true, false] {
            let (_copy, env) = copy_of(&path);
            let mut txn = env.begin_rw().expect("rw");
            let t = txn.open_table("d").expect("open").expect("d");
            let result = if dropping { txn.drop_table(t) } else { txn.clear_table(&t) };
            assert!(
                matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)),
                "{case}, drop {dropping}: {result:?}"
            );
            assert!(env.failure().is_some(), "{case}, drop {dropping}: latched");
            let alloc = &txn.writer_mut().alloc;
            assert!(alloc.freed().is_empty(), "{case}, drop {dropping}: nothing freed");
        }
    }
}
