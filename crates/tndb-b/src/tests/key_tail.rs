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

/// An entry with its value owned, for comparing two cursors' results across their next calls.
fn own(entry: Option<(&[u8], &[u8])>) -> Option<(Vec<u8>, Vec<u8>)> {
    entry.map(|(key, value)| (key.to_vec(), value.to_vec()))
}

/// The seek keys: each row's key (a hit), the key with a zero byte after it (a miss between it
/// and the next key), its key part (a miss before the key part's first row), the empty key
/// (before the first row) and a key past every row.
fn probes() -> Vec<Vec<u8>> {
    let mut probes = vec![Vec::new(), vec![0xFF; KEY + SUB + 1]];
    for i in 0..ROWS {
        let key = row_key(i);
        probes.push([&key[..], &[0]].concat());
        probes.push(key[..KEY].to_vec());
        probes.push(key);
    }
    probes
}

/// `seek_with_key_tail(key, t)` on an owned cursor and a detached write cursor equals `seek(key)`
/// followed by `current_with_key_tail(t)` on a twin cursor: hits on inline, empty and overflow
/// values, misses between keys, before the first and past the last, and an empty table; for each
/// tail, the same [`KeyTail`] (one slice of the page for an inline value), and the cursor left
/// where the seek leaves its twin: the same state, and the same `current`, `next` and `prev` after.
#[test]
fn seek_with_key_tail_is_a_seek_then_current_with_key_tail() {
    let (_dir, path) = temp("tndb-b-seek-key-tail");
    let env = open(&path, SyncMode::Durable);
    let table = filled(&env);
    let mut txn = env.begin_rw().expect("rw");
    let empty = txn.create_table("empty").expect("create");
    txn.commit().expect("commit");
    let (mut overflow, mut blank, mut misses, mut ends) = (0, 0, 0, 0);

    let ro = Arc::new(env.begin_ro().expect("ro"));
    for (table, probes) in [(&table, probes()), (&empty, vec![Vec::new(), row_key(5)])] {
        let mut fused = ro.cursor_owned(table).expect("cursor_owned");
        let mut twin = ro.cursor_owned(table).expect("cursor_owned");
        for probe in &probes {
            for t in TAILS {
                let got = fused.seek_with_key_tail(probe, t).expect("seek_with_key_tail");
                let found = twin.seek(probe).expect("seek").map(|(key, _)| key == &probe[..]);
                let want = twin.current_with_key_tail(t).expect("tail");
                assert_eq!(got, want, "probe {probe:?} t {t}");
                assert_tail(got, twin.current().unwrap(), t);
                assert_eq!(fused.state(), twin.state(), "probe {probe:?} t {t}");
                assert_eq!(own(fused.current().unwrap()), own(twin.current().unwrap()));
                match found {
                    Some(true) => {
                        let (_, value) = twin.current().unwrap().expect("a hit");
                        overflow += usize::from(value.len() > INLINE_MAX);
                        blank += usize::from(value.is_empty());
                    }
                    Some(false) => misses += 1,
                    None => ends += 1,
                }
                // Forward then back past the start, or back then forward, by `t`.
                let forward = t % 2 == 0;
                for step in [forward, !forward, !forward] {
                    let (a, b) = if step {
                        (own(fused.next().unwrap()), own(twin.next().unwrap()))
                    } else {
                        (own(fused.prev().unwrap()), own(twin.prev().unwrap()))
                    };
                    assert_eq!(a, b, "probe {probe:?} t {t}: step forward {step}");
                }
            }
        }
    }
    assert!(overflow >= 100 && blank >= 100, "{overflow} overflow and {blank} empty values hit");
    assert!(misses >= 2 * ROWS as usize && ends >= 4, "{misses} misses, {ends} at the end");
    drop(ro);

    // A detached write cursor, which keeps the key it lands on to find its place again after a
    // write: the same results, and after the transaction's write it is still where it landed.
    let mut txn = env.begin_rw().expect("rw");
    let mut fused = txn.cursor_detached(&table).expect("cursor_detached");
    let mut twin = txn.cursor_detached(&table).expect("cursor_detached");
    for (n, probe) in probes().iter().enumerate() {
        let t = TAILS[n % TAILS.len()];
        let got = fused.seek_with_key_tail(&txn, probe, t).expect("seek_with_key_tail");
        twin.seek(&txn, probe).expect("seek");
        assert_eq!(got, twin.current_with_key_tail(&txn, t).expect("tail"), "probe {probe:?}");
        assert_eq!(own(fused.current(&txn).unwrap()), own(twin.current(&txn).unwrap()));
        let step = n % 2 == 0;
        let (a, b) = if step {
            (own(fused.next(&txn).unwrap()), own(twin.next(&txn).unwrap()))
        } else {
            (own(fused.prev(&txn).unwrap()), own(twin.prev(&txn).unwrap()))
        };
        assert_eq!(a, b, "probe {probe:?}: step forward {step}");
    }
    assert!(fused.seek(&txn, &row_key(10)).expect("seek").is_some());
    let landed = fused.seek_with_key_tail(&txn, &row_key(200), SUB).expect("seek").map(|e| e.0);
    let landed = landed.expect("a hit").to_vec();
    txn.put(&table, &row_key(201), b"moved").expect("put");
    let (key, _) = fused.current(&txn).expect("current").expect("at the row");
    assert_eq!(key, landed, "the key a reseek finds");
    txn.abort();
}
