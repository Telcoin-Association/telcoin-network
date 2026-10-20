//! `RwTxn::append`, `RwCursor::put` and `RwCursor::append` against `RwTxn::put`: the same entries
//! give the same rows and byte-identical files, an append out of order changes nothing and does
//! not latch, and a cursor put leaves the cursor at its entry.

use std::{collections::BTreeMap, fs};

use super::{newest_state, open, temp};
use crate::{env::SyncMode, tree::test_harness::Rng, Error, Result, RwCursor, RwTxn, Table};

/// One table's rows.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// An owned entry.
type Owned = Option<(Vec<u8>, Vec<u8>)>;

fn key(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// A value of 0..=300 bytes (so 4 KiB leaves split every few dozen entries) that differs by `i`.
fn val(rng: &mut Rng, i: u64) -> Vec<u8> {
    let len = rng.below(301) as usize;
    (0..len).map(|j| (i as usize).wrapping_mul(31).wrapping_add(j) as u8).collect()
}

fn refused(result: Result<()>) {
    assert!(
        matches!(result, Err(Error::AppendOutOfOrder)),
        "expected AppendOutOfOrder: {result:?}"
    );
}

/// An append of the model's last key is refused (nothing to check on an empty table).
fn last_is_refused(txn: &mut RwTxn, table: &Table, model: &Model) {
    if let Some((last, _)) = model.last_key_value() {
        refused(txn.append(table, last, b"refused"));
    }
}

fn same_files(a: &std::path::Path, b: &std::path::Path, what: &str) {
    assert!(fs::read(a).expect("read") == fs::read(b).expect("read"), "{what}: the files differ");
}

/// An append whose key equals or sorts below the table's last key is refused with
/// `AppendOutOfOrder`, on the cached rightmost leaf and on the descent (a fresh transaction),
/// through `RwTxn` and `RwCursor`. It does not latch, leaves the cursor where it was, and the
/// file the next commit writes is byte-identical to one written without the refused calls.
#[test]
fn an_out_of_order_append_changes_nothing() {
    let ((_da, pa), (_db, pb)) = (temp("append-a"), temp("append-b"));
    let (ea, eb) = (open(&pa, SyncMode::NoSync), open(&pb, SyncMode::NoSync));
    for round in 0..3u64 {
        let (mut ta, mut tb) = (ea.begin_rw().expect("begin"), eb.begin_rw().expect("begin"));
        let (t_a, t_b) = (ta.create_table("t").expect("t"), tb.create_table("t").expect("t"));
        let base = round * 200;
        if round > 0 {
            // No cache yet in this transaction: the descent refuses these.
            refused(ta.append(&t_a, &key(base - 1), b"x"));
            refused(ta.append(&t_a, &key(base / 2), b"x"));
        }
        for i in base..base + 200 {
            let value = vec![i as u8; 40];
            ta.append(&t_a, &key(i), &value).expect("append");
            tb.put(&t_b, &key(i), &value).expect("put");
            if i % 37 == 0 {
                // The cached rightmost leaf's last key, and a key of an earlier leaf.
                refused(ta.append(&t_a, &key(i), b"y"));
                refused(ta.append(&t_a, &key(i / 2), b"y"));
            }
        }
        {
            let mut cursor = ta.cursor_mut(&t_a).expect("cursor");
            cursor.seek(&key(base + 10)).expect("seek");
            refused(cursor.append(&key(base + 199), b"z"));
            refused(cursor.append(&key(base + 3), b"z"));
            let at = cursor.current().expect("current").map(|(k, _)| k.to_vec());
            assert_eq!(at, Some(key(base + 10)), "a refused append moves the cursor");
        }
        assert!(ea.failure().is_none(), "a refused append latched: {:?}", ea.failure());
        ta.commit().expect("commit");
        tb.commit().expect("commit");
        same_files(&pa, &pb, &format!("round {round}"));
    }
    assert_eq!(newest_state(&ea), newest_state(&eb));
}

/// Appends into an empty table, across leaf and branch splits, and after each way the table's
/// path cache goes stale (another table's splits, an abort, deletes from the top that unlink and
/// empty leaves, a commit) match a `BTreeMap` model and write the same file as `RwTxn::put` of the
/// same entries. After each of those an append of the last key is refused, so the descent's check
/// of the last key runs too.
#[test]
fn appends_match_puts_byte_for_byte() {
    let ((_da, pa), (_db, pb)) = (temp("append-a"), temp("append-b"));
    let envs = [open(&pa, SyncMode::NoSync), open(&pb, SyncMode::NoSync)];
    let mut rng = Rng(7);
    let mut model = Model::new();
    let (mut next, mut other) = (0u64, 0u64);
    // Entry `j` of each pair goes to environment `j`: the first appends, the second puts.
    let write = |txn: &mut RwTxn, j: usize, t: &Table, k: &[u8], v: &[u8]| {
        if j == 0 { txn.append(t, k, v) } else { txn.put(t, k, v) }.expect("write");
    };
    for round in 0..8u64 {
        if round % 3 == 1 {
            let entries: Vec<_> = (next..next + 50).map(|i| (key(i), val(&mut rng, i))).collect();
            for (j, env) in envs.iter().enumerate() {
                let mut txn = env.begin_rw().expect("begin");
                let t = txn.create_table("t").expect("t");
                entries.iter().for_each(|(k, v)| write(&mut txn, j, &t, k, v));
                txn.abort();
            }
        }
        let mut txns = [envs[0].begin_rw().expect("begin"), envs[1].begin_rw().expect("begin")];
        let t = [txns[0].create_table("t").expect("t"), txns[1].create_table("t").expect("t")];
        let u = [txns[0].create_table("u").expect("u"), txns[1].create_table("u").expect("u")];
        last_is_refused(&mut txns[0], &t[0], &model);
        for i in 0..150 {
            let (k, v) = (key(next), val(&mut rng, next));
            next += 1;
            (0..2).for_each(|j| write(&mut txns[j], j, &t[j], &k, &v));
            model.insert(k, v);
            if i % 40 == 39 {
                // Scattered puts into another table: its splits move the epoch.
                for _ in 0..40 {
                    let (k, v) = (key(rng.below(1 << 40)), val(&mut rng, other));
                    other += 1;
                    (0..2).for_each(|j| txns[j].put(&u[j], &k, &v).expect("put u"));
                }
                last_is_refused(&mut txns[0], &t[0], &model);
            }
        }
        if round % 3 == 2 || round == 5 {
            let n = if round == 5 { model.len() } else { 1 + rng.below(80) as usize };
            for _ in 0..n.min(model.len()) {
                let (k, _) = model.pop_last().expect("a key");
                (0..2).for_each(|j| assert!(txns[j].delete(&t[j], &k).expect("delete")));
            }
            // The next appends reuse the range just deleted.
            next = model.last_key_value().map_or(0, |(k, _)| {
                u64::from_be_bytes(k.as_slice().try_into().expect("8 bytes")) + 1
            });
            last_is_refused(&mut txns[0], &t[0], &model);
        }
        assert!(envs[0].failure().is_none(), "a refused append latched");
        let [ta, tb] = txns;
        ta.commit().expect("commit");
        tb.commit().expect("commit");
        same_files(&pa, &pb, &format!("round {round}"));
        assert_eq!(newest_state(&envs[0]).get("t"), Some(&model), "round {round}");
    }
    assert_eq!(newest_state(&envs[0]), newest_state(&envs[1]));
}

/// The cursor is at `k` (its model entry), and `next` and `prev` step to its model neighbours and
/// back (past either end and back to `k`).
fn assert_at(cursor: &mut RwCursor<'_>, model: &Model, k: &[u8], what: &str) {
    let owned = |e: Option<(&[u8], &[u8])>| e.map(|(k, v)| (k.to_vec(), v.to_vec()));
    let pair = |e: Option<(&Vec<u8>, &Vec<u8>)>| e.map(|(k, v)| (k.clone(), v.clone()));
    let here: Owned = pair(model.get_key_value(k));
    let after = pair(model.range(k.to_vec()..).nth(1));
    let before = pair(model.range(..k.to_vec()).next_back());
    assert_eq!(owned(cursor.current().expect("current")), here, "{what}: current");
    assert_eq!(owned(cursor.next().expect("next")), after, "{what}: next");
    assert_eq!(owned(cursor.prev().expect("prev")), here, "{what}: back from next");
    assert_eq!(owned(cursor.prev().expect("prev")), before, "{what}: prev");
    assert_eq!(owned(cursor.next().expect("next")), here, "{what}: back from prev");
}

/// `RwCursor::put` and `RwCursor::append` leave the cursor at the entry written, from where `next`
/// and `prev` reach its neighbours in a `BTreeMap` model, both after an in-place put (placed from
/// the path written) and after a split (found again by key); and they write the same file as
/// `RwTxn::put` of the same entries.
#[test]
fn a_cursor_put_lands_on_its_entry() {
    let ((_da, pa), (_db, pb)) = (temp("cursor-a"), temp("cursor-b"));
    let (ea, eb) = (open(&pa, SyncMode::NoSync), open(&pb, SyncMode::NoSync));
    let mut rng = Rng(11);
    let mut model = Model::new();
    let mut next = 1u64 << 20;
    for round in 0..4 {
        let mut ta = ea.begin_rw().expect("begin");
        let t = ta.create_table("t").expect("t");
        let mut written = Vec::new();
        {
            let mut cursor = ta.cursor_mut(&t).expect("cursor");
            for op in 0..250u64 {
                let choice = rng.below(4);
                let k = match choice {
                    0 | 1 => {
                        next += 1;
                        key(next)
                    }
                    2 => key(rng.below(4000)),
                    _ => match model.keys().nth(rng.below(model.len().max(1) as u64) as usize) {
                        Some(k) => k.clone(),
                        None => key(rng.below(4000)),
                    },
                };
                let v = val(&mut rng, op);
                if choice == 0 { cursor.append(&k, &v) } else { cursor.put(&k, &v) }
                    .expect("cursor write");
                model.insert(k.clone(), v.clone());
                assert_at(&mut cursor, &model, &k, &format!("round {round} op {op}"));
                written.push((k, v));
            }
        }
        ta.commit().expect("commit");
        let mut tb = eb.begin_rw().expect("begin");
        let tt = tb.create_table("t").expect("t");
        written.iter().for_each(|(k, v)| tb.put(&tt, k, v).expect("put"));
        tb.commit().expect("commit");
        same_files(&pa, &pb, &format!("round {round}"));
    }
    assert_eq!(newest_state(&ea).get("t"), Some(&model));
    assert_eq!(newest_state(&ea), newest_state(&eb));
}
