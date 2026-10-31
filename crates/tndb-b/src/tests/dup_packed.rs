//! Dup tables with packed sets through the public API: the packed-set limit a table is created
//! with and the number its record holds; the gets and puts of packed keys, and their promotion
//! to nested trees past the limit.

use std::collections::BTreeMap;

use super::{check, temp};
use crate::{DupOptions, Env, EnvOptions, Error, ReadTxn, RoTxn, RwTxn, Table};

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
    assert!(env.failure().is_none(), "no latch");
}
