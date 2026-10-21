//! `current_with_key_tail` on every cursor kind ([`crate::Cursor`], [`crate::RoCursor`],
//! [`crate::TxnCursor`]) over a table of composite keys (`key ‖ subkey`, as a dup-sort table's
//! rows are stored) holding inline values, empty ones and values in overflow runs: at each entry
//! the key, and the key's last bytes joined to an inline value as one slice of the page, or an
//! overflow value apart, against what `current` returns.

use std::sync::Arc;

use super::{open, temp, value};
use crate::{env::SyncMode, Env, KeyTail, ReadTxn, Table};

/// The width of a row's key part; the rest of its 32-byte key is the subkey.
const KEY: usize = 8;
/// The width of a row's subkey.
const SUB: usize = 24;
/// The tails asked for: none, the subkey, the whole key, past the key (clamped to it).
const TAILS: [usize; 4] = [0, SUB, KEY + SUB, KEY + SUB + 9];
/// Rows in the table: several leaves at 4 KiB pages.
const ROWS: u32 = 300;
/// Values longer than this go to an overflow run (4 KiB pages).
const INLINE_MAX: usize = 4000;

/// Row `i`'s key: four rows to a key part, each with its own subkey.
fn row_key(i: u32) -> Vec<u8> {
    let mut key = u64::from(i / 4).to_be_bytes().to_vec();
    key.extend(value(7, i, SUB));
    key
}

/// Row `i`'s value: now and then empty, now and then one that takes an overflow run.
fn row_value(i: u32) -> Vec<u8> {
    let len = match i {
        _ if i.is_multiple_of(9) => 5000 + i as usize,
        _ if i.is_multiple_of(7) => 0,
        _ => (i % 60) as usize,
    };
    value(3, i, len)
}

/// A table of [`ROWS`] rows, committed.
fn filled(env: &Env) -> Table {
    let mut txn = env.begin_rw().expect("rw");
    let table = txn.create_table("rows").expect("create");
    for i in 0..ROWS {
        txn.put(&table, &row_key(i), &row_value(i)).expect("put");
    }
    txn.commit().expect("commit");
    table
}

/// `got`, a `current_with_key_tail(t)`, against `want`, the `current` of the same cursor: the
/// same key, and the key's last `t` bytes (all of them past its length) followed by an inline
/// value as one slice starting inside the key, or an overflow value apart.
fn assert_tail(got: Option<(&[u8], KeyTail<'_>)>, want: Option<(&[u8], &[u8])>, t: usize) {
    let (Some((key, tail)), Some((want_key, value))) = (got, want) else {
        assert_eq!(got.is_none(), want.is_none(), "t {t}: {got:?} where current gives {want:?}");
        return;
    };
    assert_eq!(key, want_key, "t {t}");
    if value.len() > INLINE_MAX {
        assert_eq!(tail, KeyTail::Apart(value), "t {t}: an overflow value");
        return;
    }
    let KeyTail::Joined(joined) = tail else { panic!("t {t}: an inline value apart: {tail:?}") };
    let start = key.len() - t.min(key.len());
    assert_eq!(joined, [&key[start..], value].concat(), "t {t}");
    assert_eq!(joined.as_ptr(), key[start..].as_ptr(), "t {t}: one slice of the page");
}

#[test]
fn every_cursor_joins_the_key_tail_to_inline_values() {
    let (_dir, path) = temp("tndb-b-key-tail");
    let env = open(&path, SyncMode::Durable);
    let table = filled(&env);
    let (mut overflow, mut empty) = (0, 0);

    // A read transaction's borrowing cursor and owned cursor, forward then backward.
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let mut cursor = ro.cursor(&table).expect("cursor");
    let mut owned = ro.cursor_owned(&table).expect("cursor_owned");
    assert_eq!(cursor.current_with_key_tail(SUB).expect("unset"), None);
    assert_eq!(owned.current_with_key_tail(SUB).expect("unset"), None);
    for forward in [true, false] {
        let mut seen = 0;
        loop {
            let moved = if forward { cursor.next() } else { cursor.prev() };
            let at_row = moved.expect("move").is_some();
            let moved = if forward { owned.next() } else { owned.prev() };
            assert_eq!(moved.expect("move").is_some(), at_row);
            if !at_row {
                break;
            }
            for t in TAILS {
                assert_tail(
                    cursor.current_with_key_tail(t).expect("tail"),
                    cursor.current().unwrap(),
                    t,
                );
                assert_tail(
                    owned.current_with_key_tail(t).expect("tail"),
                    owned.current().unwrap(),
                    t,
                );
            }
            let (_, value) = cursor.current().unwrap().expect("at a row");
            overflow += usize::from(value.len() > INLINE_MAX);
            empty += usize::from(value.is_empty());
            seen += 1;
        }
        assert_eq!(seen, ROWS, "forward {forward}");
        // Past either end: nothing.
        assert_eq!(cursor.current_with_key_tail(SUB).expect("end"), None);
        assert_eq!(owned.current_with_key_tail(SUB).expect("end"), None);
    }
    assert!(overflow >= 60 && empty >= 60, "{overflow} overflow and {empty} empty values");

    // After a seek and the prefix steps through one key part's rows.
    for i in [0, 37, 150, ROWS - 1] {
        let key = row_key(i);
        assert!(cursor.seek(&key[..KEY]).expect("seek").is_some());
        assert!(owned.seek(&key[..KEY]).expect("seek").is_some());
        loop {
            for t in TAILS {
                assert_tail(
                    cursor.current_with_key_tail(t).expect("tail"),
                    cursor.current().unwrap(),
                    t,
                );
                assert_tail(
                    owned.current_with_key_tail(t).expect("tail"),
                    owned.current().unwrap(),
                    t,
                );
            }
            let stepped = cursor.next_in_prefix(KEY).expect("step").is_some();
            assert_eq!(owned.next_in_prefix(KEY).expect("step").is_some(), stepped);
            if !stepped {
                break;
            }
        }
    }
    drop((owned, ro));

    // A detached write cursor: a walk, a reseek after the transaction's own write, and a delete.
    let mut txn = env.begin_rw().expect("rw");
    let mut c = txn.cursor_detached(&table).expect("cursor_detached");
    assert_eq!(c.current_with_key_tail(&txn, SUB).expect("unset"), None);
    let mut seen = 0;
    while c.next(&txn).expect("next").is_some() {
        for t in TAILS {
            assert_tail(
                c.current_with_key_tail(&txn, t).expect("tail"),
                c.current(&txn).unwrap(),
                t,
            );
        }
        seen += 1;
    }
    assert_eq!(seen, ROWS);
    assert_eq!(c.current_with_key_tail(&txn, SUB).expect("end"), None);
    let keys: Vec<Vec<u8>> = {
        let mut keys: Vec<_> = (0..ROWS).map(row_key).collect();
        keys.sort();
        keys
    };
    assert!(c.seek(&txn, &keys[99]).expect("seek").is_some());
    txn.put(&table, &keys[98], &[1; 3000]).expect("put");
    txn.put(&table, &keys[99], &[2; 9000]).expect("put");
    for t in TAILS {
        assert_tail(c.current_with_key_tail(&txn, t).expect("tail"), c.current(&txn).unwrap(), t);
    }
    // After a delete, the follower, as `current` gives it, which it leaves the cursor at as
    // `current` does: `next` steps past it.
    assert!(c.delete_current(&mut txn).expect("delete"));
    let follower = c.current_with_key_tail(&txn, SUB).expect("tail");
    let want = txn.get(&table, &keys[100]).expect("get").expect("the follower");
    assert_tail(follower, Some((&keys[100], want)), SUB);
    let (next, _) = c.next(&txn).expect("next").expect("a row after the follower");
    assert_eq!(next, keys[101]);
    txn.abort();
}
