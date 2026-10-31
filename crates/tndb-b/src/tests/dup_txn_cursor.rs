//! A write transaction's dup cursor ([`DupTxnCursor`]) against the composite twin: a dup table
//! `d` (key width 4) and a plain table `p` keyed `k ‖ s` holding the same rows, a dup cursor over
//! `d` and a [`TxnCursor`] over `p` in one transaction, each call compared, with the
//! transaction's own writes between the moves; then the cases one by one: staleness after
//! another writer's put, delete, emptied key, `delete_prefix` and `clear_table`, the placements of
//! the cursor's own deletes and puts (from the path caches and by key), the deleted state and
//! `follower_shares_prefix`, the working map before the cells, and the refusals.

use tempfile::TempDir;

use super::{dup::OPTIONS, open, temp, Rows};
use crate::{
    env::SyncMode,
    tree::{
        cursor::{
            tests::{owned, owned_tail, Kv},
            State,
        },
        test_harness::{ReportSeed, Rng},
    },
    DupTxnCursor, Env, Error, ReadTxn as _, Result, RwTxn, Table, TxnCursor,
};

/// The key width of the dup tables here ([`OPTIONS`]).
const K: usize = OPTIONS.key_len as usize;

/// The logical key of outer key `k` and subkey number `s` (two bytes).
fn row_key(k: u32, s: u16) -> Vec<u8> {
    [&k.to_be_bytes()[..], &s.to_be_bytes()].concat()
}

/// The value of row `(k, s)`, tagged by `tag`.
fn row_value(k: u32, s: u16, tag: u8) -> Vec<u8> {
    vec![tag ^ k as u8 ^ s as u8; 1 + (usize::from(s) % 23)]
}

/// Dup table `d` and its twin `p`, committed with `rows` rows under each outer key `k` of
/// `keys` (`(k, rows)`), subkeys `0..rows`.
fn twins(env: &Env, keys: &[(u32, u16)]) -> (Table, Table, Rows) {
    let mut rows = Rows::new();
    for &(k, n) in keys {
        for s in 0..n {
            rows.insert(row_key(k, s), row_value(k, s, 0));
        }
    }
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let p = txn.create_table("p").expect("p");
    for (key, value) in &rows {
        txn.put(&d, key, value).expect("put d");
        txn.put(&p, key, value).expect("put p");
    }
    txn.commit().expect("commit");
    (d, p, rows)
}

/// A dup cursor over `d` and a twin cursor over `p`.
fn pair(txn: &RwTxn, d: &Table, p: &Table) -> (DupTxnCursor, TxnCursor) {
    (txn.dup_cursor_detached(d).expect("dup cursor"), txn.cursor_detached(p).expect("cursor"))
}

/// What a call gave, owned, refusals by name.
type Got = std::result::Result<Option<Kv>, String>;

fn got(r: Result<Option<(&[u8], &[u8])>>) -> Got {
    r.map(owned).map_err(|e| format!("{e:?}"))
}

fn done(r: Result<()>) -> Got {
    r.map(|()| None).map_err(|e| format!("{e:?}"))
}

fn flag(r: Result<bool>) -> Got {
    r.map(|b| b.then(|| (Vec::new(), Vec::new()))).map_err(|e| format!("{e:?}"))
}

/// The twin's `delete_prefix`: the adapter's wipe loop over a plain cursor (seek, then
/// `delete_current` and `next_in_prefix(K)` until it returns nothing), which it returns with the
/// rows it deleted.
fn wipe(txn: &mut RwTxn, p: &Table, k: &[u8]) -> (TxnCursor, u64) {
    let mut c = txn.cursor_detached(p).expect("wipe cursor");
    let mut more = c.seek(txn, k).expect("seek").is_some_and(|(key, _)| key.starts_with(k));
    let mut n = 0;
    while more {
        assert!(c.delete_current(txn).expect("delete"));
        n += 1;
        more = c.next_in_prefix(txn, K).expect("next_in_prefix").is_some();
    }
    (c, n)
}

/// One step of the lockstep run.
#[derive(Clone, Debug)]
enum Step {
    First,
    Last,
    Next,
    Prev,
    Current,
    CurrentWithKeyTail(usize),
    Seek(Vec<u8>),
    SeekExact(Vec<u8>),
    SeekWithKeyTail(Vec<u8>, usize),
    NextInPrefix(usize),
    PrevInPrefix(usize),
    Shares(usize),
    CursorPut(Vec<u8>, Vec<u8>),
    CursorInsert(Vec<u8>, Vec<u8>),
    CursorAppend(Vec<u8>, Vec<u8>),
    CursorDelete,
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeletePrefix(Vec<u8>),
    Clear,
}

/// The outer keys of the lockstep run: neighbours share 0 to 3 leading bytes.
const OUTER: [u32; 8] = [
    0x0000_0001,
    0x0000_0002,
    0x0000_0100,
    0x0001_0000,
    0x0001_0001,
    0x7f00_0000,
    0x7f00_0001,
    0xfe00_0000,
];

/// A logical key for a write: a row's own, or a new subkey under a known outer key, mostly.
fn write_key(rng: &mut Rng, model: &Rows) -> Vec<u8> {
    if !model.is_empty() && rng.below(3) == 0 {
        let i = rng.below(model.len() as u64) as usize;
        return model.keys().nth(i).expect("row").clone();
    }
    let k = OUTER[rng.below(OUTER.len() as u64) as usize];
    row_key(k, rng.below(400) as u16)
}

/// A seek target of every length class around a row's key (or a random one).
fn seek_target(rng: &mut Rng, model: &Rows) -> Vec<u8> {
    let r = write_key(rng, model);
    match rng.below(6) {
        0 => r[..rng.below(K as u64) as usize].to_vec(),
        1 => r[..K].to_vec(),
        2 => [&r[..], &[0]].concat(),
        3 => r[..K + 1].to_vec(),
        _ => r,
    }
}

impl Step {
    fn pick(rng: &mut Rng, model: &Rows) -> Self {
        let n = |rng: &mut Rng| [0, 1, K - 1, K, K + 1, K + 2, K + 3][rng.below(7) as usize];
        let v = |rng: &mut Rng| vec![rng.next() as u8; rng.below(30) as usize];
        match rng.below(40) {
            0 => Self::First,
            1 => Self::Last,
            2..=6 => Self::Next,
            7..=9 => Self::Prev,
            10 => Self::Current,
            11 => Self::CurrentWithKeyTail(rng.below(8) as usize),
            12 | 13 => Self::Seek(seek_target(rng, model)),
            14 => Self::SeekExact(seek_target(rng, model)),
            15 => Self::SeekWithKeyTail(seek_target(rng, model), rng.below(8) as usize),
            16..=18 => Self::NextInPrefix(n(rng)),
            19 | 20 => Self::PrevInPrefix(n(rng)),
            21 | 22 => Self::Shares(n(rng)),
            23 | 24 => Self::CursorPut(write_key(rng, model), v(rng)),
            25 => Self::CursorInsert(write_key(rng, model), v(rng)),
            26 => Self::CursorAppend(write_key(rng, model), v(rng)),
            27..=29 => Self::CursorDelete,
            30 | 31 => Self::Put(write_key(rng, model), v(rng)),
            32..=35 => Self::Delete(write_key(rng, model)),
            36 | 37 => Self::DeletePrefix(write_key(rng, model)[..K].to_vec()),
            38 if rng.below(30) == 0 => Self::Clear,
            _ => Self::Next,
        }
    }

    /// Run the step on cursor pair `(dup, twin)` and the tables, returning the dup's result and
    /// the twin's, and keep `model` (the rows) in step.
    fn run(
        &self,
        txn: &mut RwTxn,
        (d, p): (&Table, &Table),
        (dup, twin): (&mut DupTxnCursor, &mut TxnCursor),
        model: &mut Rows,
    ) -> (Got, Got) {
        let tail = |r: Result<Option<(&[u8], crate::KeyTail<'_>)>>, t| {
            r.map(|r| owned_tail(r, t)).map_err(|e| format!("{e:?}"))
        };
        let exact = |r: Result<Option<&[u8]>>| {
            r.map(|v| v.map(|v| (Vec::new(), v.to_vec()))).map_err(|e| format!("{e:?}"))
        };
        match self {
            Self::First => (got(dup.first(txn)), got(twin.first(txn))),
            Self::Last => (got(dup.last(txn)), got(twin.last(txn))),
            Self::Next => (got(dup.next(txn)), got(twin.next(txn))),
            Self::Prev => (got(dup.prev(txn)), got(twin.prev(txn))),
            Self::Current => (got(dup.current(txn)), got(twin.current(txn))),
            Self::CurrentWithKeyTail(t) => (
                tail(dup.current_with_key_tail(txn, *t), *t),
                tail(twin.current_with_key_tail(txn, *t), *t),
            ),
            Self::Seek(x) => (got(dup.seek(txn, x)), got(twin.seek(txn, x))),
            Self::SeekExact(x) => (exact(dup.seek_exact(txn, x)), exact(twin.seek_exact(txn, x))),
            Self::SeekWithKeyTail(x, t) => (
                tail(dup.seek_with_key_tail(txn, x, *t), *t),
                tail(twin.seek_with_key_tail(txn, x, *t), *t),
            ),
            Self::NextInPrefix(n) => {
                (got(dup.next_in_prefix(txn, *n)), got(twin.next_in_prefix(txn, *n)))
            }
            Self::PrevInPrefix(n) => {
                (got(dup.prev_in_prefix(txn, *n)), got(twin.prev_in_prefix(txn, *n)))
            }
            Self::Shares(n) => (
                flag(dup.follower_shares_prefix(txn, *n)),
                flag(twin.follower_shares_prefix(txn, *n)),
            ),
            Self::CursorPut(key, v) | Self::CursorInsert(key, v) | Self::CursorAppend(key, v) => {
                let r = match self {
                    Self::CursorPut(..) => {
                        (done(dup.put(txn, key, v)), done(twin.put(txn, key, v)))
                    }
                    Self::CursorInsert(..) => {
                        (done(dup.insert(txn, key, v)), done(twin.insert(txn, key, v)))
                    }
                    _ => (done(dup.append(txn, key, v)), done(twin.append(txn, key, v))),
                };
                if r.1.is_ok() {
                    model.insert(key.clone(), v.clone());
                }
                r
            }
            Self::CursorDelete => {
                let r = (flag(dup.delete_current(txn)), flag(twin.delete_current(txn)));
                if let Ok(Some(_)) = &r.0 {
                    model.remove(dup.deleted_key_for_test().expect("deleted"));
                }
                r
            }
            Self::Put(key, v) => {
                txn.put(d, key, v).expect("put d");
                txn.put(p, key, v).expect("put p");
                model.insert(key.clone(), v.clone());
                (Ok(None), Ok(None))
            }
            Self::Delete(key) => {
                let a = txn.delete(d, key).expect("delete d");
                let b = txn.delete(p, key).expect("delete p");
                model.remove(key);
                (flag(Ok(a)), flag(Ok(b)))
            }
            Self::DeletePrefix(k) => {
                let a = txn.delete_prefix(d, k).expect("delete_prefix");
                let (_, b) = wipe(txn, p, k);
                model.retain(|key, _| !key.starts_with(k));
                (
                    Ok(Some((a.to_be_bytes().to_vec(), Vec::new()))),
                    Ok(Some((b.to_be_bytes().to_vec(), Vec::new()))),
                )
            }
            Self::Clear => {
                txn.clear_table(d).expect("clear d");
                txn.clear_table(p).expect("clear p");
                model.clear();
                (Ok(None), Ok(None))
            }
        }
    }
}

/// A light lockstep run (the full one with its coverage gates is Oracle 2b): pairs of a dup
/// cursor over `d` and a twin cursor over `p` in one transaction take the same random calls,
/// cursor writes included, with the transaction's own puts, deletes, `delete_prefix` (the twin's:
/// the wipe loop) and clears between them; every call gives the same result and leaves the same
/// state, deleted flag included. Rounds commit (the file then checks) or abort.
#[test]
fn dup_txn_cursors_match_the_twin_under_interleaved_writes() {
    let mut reseeks = 0;
    let mut placements = 0;
    let mut deleted_seen = 0;
    for seed in 0..3u64 {
        let seed = 0x2b2b_0000_0000 + seed;
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let (_dir, path) = temp("tndb_b_dup_txn_cursor_lockstep");
        let env = open(&path, SyncMode::NoSync);
        let keys: Vec<(u32, u16)> = OUTER
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, if i == 2 { 300 } else { 1 + i as u16 * 3 }))
            .collect();
        let (d, p, mut model) = twins(&env, &keys);
        for round in 0..4 {
            let mut txn = env.begin_rw().expect("rw");
            let mut round_model = model.clone();
            let mut pairs: Vec<_> = (0..3).map(|_| pair(&txn, &d, &p)).collect();
            for step in 0..1500 {
                let j = rng.below(pairs.len() as u64) as usize;
                let op = Step::pick(&mut rng, &round_model);
                let (dup, twin) = &mut pairs[j];
                let from = (dup.state(), twin.state());
                let (a, b) = op.run(&mut txn, (&d, &p), (dup, twin), &mut round_model);
                assert_eq!(a, b, "round {round} step {step} pair {j} {op:?} from {from:?}");
                assert_eq!(dup.state(), twin.state(), "round {round} step {step} pair {j} {op:?}");
                deleted_seen += u32::from(dup.state().1);
            }
            for (dup, _) in &pairs {
                reseeks += dup.reseeks;
                placements += dup.placements;
            }
            let rows: Rows = txn.dup_rows_for_test(&d).into_iter().collect();
            assert_eq!(rows, round_model, "round {round}: the dup table's rows");
            if round % 2 == 0 {
                txn.commit().expect("commit");
                env.check().expect("check");
                model = round_model;
            } else {
                txn.abort();
            }
        }
        assert!(env.failure().is_none(), "no call latched: {:?}", env.failure());
    }
    assert!(
        reseeks > 0 && placements > 0 && deleted_seen > 0,
        "{reseeks} {placements} {deleted_seen}"
    );
}

/// A fresh environment with `d` and `p` over `keys`, and a write transaction on it.
fn setup(name: &str, keys: &[(u32, u16)]) -> (TempDir, Env, Table, Table, Rows) {
    let (dir, path) = temp(name);
    let env = open(&path, SyncMode::NoSync);
    let (d, p, rows) = twins(&env, keys);
    (dir, env, d, p, rows)
}

/// The logical key a dup cursor's call returned.
fn key_of(r: Result<Option<(&[u8], &[u8])>>) -> Option<Vec<u8>> {
    r.expect("call").map(|(k, _)| k.to_vec())
}

/// Another writer's put of another subkey under the cursor's key, delete of its row, emptying of
/// its key one row at a time, `delete_prefix` of its key and `clear_table`, each between two
/// calls: the cursor finds its row again (on it, else on the first row after it in the deleted
/// state, the next key's first row once its key is gone), as the twin's cursor does.
#[test]
fn a_dup_txn_cursor_finds_its_row_again_after_another_writers_change() {
    let (_dir, env, d, p, _) = setup("tndb_b_dup_txn_stale", &[(1, 3), (2, 200), (3, 2), (4, 1)]);
    let mut txn = env.begin_rw().expect("rw");
    let (mut c, mut t) = pair(&txn, &d, &p);
    let both = |txn: &mut RwTxn, f: &dyn Fn(&mut RwTxn, &Table)| {
        f(txn, &d);
        f(txn, &p);
    };
    // A call first (it finds the row again), then the states.
    let same = |txn: &RwTxn, c: &mut DupTxnCursor, t: &mut TxnCursor, what: &str| {
        for n in [K - 1, K, K + 1] {
            let a = c.follower_shares_prefix(txn, n).expect("shares");
            assert_eq!(a, t.follower_shares_prefix(txn, n).expect("shares"), "{what}: shares {n}");
        }
        assert_eq!(c.state(), t.state(), "{what}: state");
    };

    // A put of another subkey of the cursor's key: back on its own row.
    assert!(c.seek_exact(&txn, &row_key(2, 100)).expect("seek").is_some());
    assert!(t.seek_exact(&txn, &row_key(2, 100)).expect("seek").is_some());
    both(&mut txn, &|txn, tb| txn.put(tb, &row_key(2, 500), b"new").expect("put"));
    same(&txn, &mut c, &mut t, "put beside");
    assert_eq!(key_of(c.current(&txn)), Some(row_key(2, 100)));
    assert_eq!(c.reseeks, 1);

    // A delete of its row: on the follower, deleted; `next` returns the follower.
    both(&mut txn, &|txn, tb| assert!(txn.delete(tb, &row_key(2, 100)).expect("delete")));
    same(&txn, &mut c, &mut t, "row deleted");
    assert_eq!(c.state(), (State::At, true));
    assert_eq!(key_of(c.next(&txn)), Some(row_key(2, 101)));
    assert_eq!(key_of(t.next(&txn)), Some(row_key(2, 101)));

    // Its key emptied a row at a time: on the next key's first row, deleted.
    assert_eq!(key_of(c.seek(&txn, &row_key(3, 1))), Some(row_key(3, 1)));
    t.seek(&txn, &row_key(3, 1)).expect("seek");
    for s in 0..2 {
        both(&mut txn, &|txn, tb| assert!(txn.delete(tb, &row_key(3, s)).expect("delete")));
    }
    same(&txn, &mut c, &mut t, "key emptied");
    assert_eq!(c.state(), (State::At, true));
    assert_eq!(key_of(c.prev(&txn)), Some(row_key(2, 500)), "the row before the follower");
    assert_eq!(key_of(t.prev(&txn)), Some(row_key(2, 500)));

    // `delete_prefix` of its key: the wipe loop's state on the twin.
    assert_eq!(key_of(c.seek(&txn, &row_key(2, 7))), Some(row_key(2, 7)));
    txn.delete_prefix(&d, &2u32.to_be_bytes()).expect("delete_prefix");
    let (mut w, _) = wipe(&mut txn, &p, &2u32.to_be_bytes());
    same(&txn, &mut c, &mut w, "delete_prefix");
    assert_eq!(key_of(c.next(&txn)), Some(row_key(4, 0)), "the next key's first row");
    assert_eq!(key_of(w.next(&txn)), Some(row_key(4, 0)));

    // `clear_table`: after the end, deleted; `prev` of an empty table is before the start.
    both(&mut txn, &|txn, tb| txn.clear_table(tb).expect("clear"));
    same(&txn, &mut c, &mut w, "clear");
    assert_eq!(c.state(), (State::AfterEnd, true));
    assert_eq!(key_of(c.prev(&txn)), None);
    assert_eq!(key_of(w.prev(&txn)), None);
    assert_eq!(c.state(), w.state());
    assert!(env.failure().is_none());
    txn.commit().expect("commit");
    env.check().expect("check");
}

/// A call made on a dup cursor and a twin cursor alike, with both results.
type Probe = dyn Fn(&RwTxn, &mut DupTxnCursor, &mut TxnCursor) -> (Got, Got);

/// `delete_prefix` of the key a cursor is at leaves it as the adapter's wipe loop leaves the
/// twin's cursor: on the next key's first row in the deleted state (after the end with no next
/// key), with the same answers from `follower_shares_prefix`, and then from each next move, prefix
/// step or delete (each probe on a state of its own).
#[test]
fn delete_prefix_under_a_cursor_leaves_the_wipe_loops_state() {
    let keys = [(0x0101_0000, 4), (0x0101_0001, 30), (0x0101_0002, 3), (0x0102_0000, 2)];
    let probes: [&Probe; 8] = [
        &|txn, c, w| (got(c.next(txn)), got(w.next(txn))),
        &|txn, c, w| (got(c.prev(txn)), got(w.prev(txn))),
        &|txn, c, w| (got(c.current(txn)), got(w.current(txn))),
        &|txn, c, w| (got(c.prev_in_prefix(txn, 0)), got(w.prev_in_prefix(txn, 0))),
        &|txn, c, w| (got(c.prev_in_prefix(txn, K - 1)), got(w.prev_in_prefix(txn, K - 1))),
        &|txn, c, w| (got(c.prev_in_prefix(txn, K)), got(w.prev_in_prefix(txn, K))),
        &|txn, c, w| (got(c.next_in_prefix(txn, K - 1)), got(w.next_in_prefix(txn, K - 1))),
        &|txn, c, w| (got(c.next_in_prefix(txn, K)), got(w.next_in_prefix(txn, K))),
    ];
    for (victim, at) in
        [(0x0101_0001u32, 0u16), (0x0101_0001, 29), (0x0101_0002, 1), (0x0102_0000, 0)]
    {
        for (i, probe) in probes.iter().enumerate() {
            let (_dir, env, d, p, _) = setup("tndb_b_dup_txn_wipe", &keys);
            let mut txn = env.begin_rw().expect("rw");
            let mut c = txn.dup_cursor_detached(&d).expect("cursor");
            assert!(c.seek_exact(&txn, &row_key(victim, at)).expect("seek").is_some());
            let k = victim.to_be_bytes();
            let rows = txn.delete_prefix(&d, &k).expect("delete_prefix");
            let (mut w, wiped) = wipe(&mut txn, &p, &k);
            assert_eq!(rows, wiped, "{victim:x}");
            let what = format!("{victim:x} at {at} probe {i}");
            for n in [0, K - 1, K, K + 1, K + 2] {
                let a = c.follower_shares_prefix(&txn, n).expect("shares");
                assert_eq!(a, w.follower_shares_prefix(&txn, n).expect("shares"), "{what}: n {n}");
            }
            assert_eq!(c.state(), w.state(), "{what}: state");
            assert!(c.state().1, "{what}: deleted");
            assert_eq!(c.reseeks, 1, "{what}: one reseek");
            let (a, b) = probe(&txn, &mut c, &mut w);
            assert_eq!(a, b, "{what}");
            assert_eq!(c.state(), w.state(), "{what}: state after the probe");
            assert_eq!(got(c.next(&txn)), got(w.next(&txn)), "{what}: next after the probe");
            assert_eq!(
                flag(c.delete_current(&mut txn)),
                flag(w.delete_current(&mut txn)),
                "{what}: delete"
            );
            assert_eq!(c.state(), w.state(), "{what}: state after the delete");
            assert!(env.failure().is_none());
        }
    }
}

/// A delete through the cursor of a key's last row (the key keeps others) lands on the next key's
/// first row in the deleted state, and `prev_in_prefix(K)` then reaches the key's new last row;
/// at the table's last row, after the end. A delete inside a key lands on the next subkey. Placed
/// from the nested path cache, and by key with the caches forgotten; the twin's cursor agrees.
#[test]
fn a_delete_of_a_keys_last_row_lands_on_the_next_key() {
    for cached in [true, false] {
        let (_dir, env, d, p, _) = setup("tndb_b_dup_txn_del_last", &[(1, 2), (2, 300), (3, 5)]);
        env.with_writer(|w| w.no_path_cache = !cached);
        let mut txn = env.begin_rw().expect("rw");
        let (mut c, mut t) = pair(&txn, &d, &p);
        for (k, s, follower) in
            [(2u32, 299u16, Some(row_key(3, 0))), (2, 150, Some(row_key(2, 151)))]
        {
            assert!(c.seek_exact(&txn, &row_key(k, s)).expect("seek").is_some());
            assert!(t.seek_exact(&txn, &row_key(k, s)).expect("seek").is_some());
            assert!(c.delete_current(&mut txn).expect("delete"));
            assert!(t.delete_current(&mut txn).expect("delete"));
            assert_eq!(c.state(), (State::At, true));
            assert_eq!(c.state(), t.state());
            assert_eq!(c.follower_shares_prefix(&txn, K).expect("shares"), s != 299);
            assert_eq!(key_of(c.current(&txn)), follower);
            assert_eq!(key_of(t.current(&txn)), follower);
        }
        // The key's last row again, then back into the key with the prefix step.
        assert!(c.seek_exact(&txn, &row_key(2, 298)).expect("seek").is_some());
        assert!(t.seek_exact(&txn, &row_key(2, 298)).expect("seek").is_some());
        assert!(c.delete_current(&mut txn).expect("delete"));
        assert!(t.delete_current(&mut txn).expect("delete"));
        assert_eq!(key_of(c.prev_in_prefix(&txn, K)), Some(row_key(2, 297)));
        assert_eq!(key_of(t.prev_in_prefix(&txn, K)), Some(row_key(2, 297)));
        assert_eq!(c.state(), (State::At, false));
        // The table's last row: after the end, deleted.
        assert!(c.seek_exact(&txn, &row_key(3, 4)).expect("seek").is_some());
        assert!(c.delete_current(&mut txn).expect("delete"));
        assert_eq!(c.state(), (State::AfterEnd, true));
        assert_eq!(key_of(c.prev_in_prefix(&txn, K)), Some(row_key(3, 3)));
        assert_eq!(c.placements > 0, cached, "placements {}", c.placements);
        txn.delete(&p, &row_key(3, 4)).expect("delete");
        let rows: Rows = txn.dup_rows_for_test(&d).into_iter().collect();
        let mut twin = Rows::new();
        let mut w = txn.cursor_detached(&p).expect("cursor");
        let mut r = w.first(&txn).expect("first").map(|(k, v)| (k.to_vec(), v.to_vec()));
        while let Some((k, v)) = r {
            twin.insert(k, v);
            r = w.next(&txn).expect("next").map(|(k, v)| (k.to_vec(), v.to_vec()));
        }
        assert_eq!(rows, twin);
        txn.commit().expect("commit");
        env.check().expect("check");
        assert!(env.failure().is_none());
    }
}

/// A delete through the cursor that empties its key (a one-row key, and a key deleted row by row
/// with `delete_current` and `next_in_prefix(K)`) removes the key and lands on the next key's
/// first row, deleted: from the outer path cache, and by key with the caches forgotten. With no
/// next key, after the end.
#[test]
fn a_delete_that_empties_a_key_lands_on_the_next_key() {
    for cached in [true, false] {
        let (_dir, env, d, p, _) =
            setup("tndb_b_dup_txn_del_empty", &[(1, 1), (2, 1), (3, 120), (4, 2), (5, 1)]);
        env.with_writer(|w| w.no_path_cache = !cached);
        let mut txn = env.begin_rw().expect("rw");
        let (mut c, mut t) = pair(&txn, &d, &p);
        assert!(c.seek_exact(&txn, &row_key(2, 0)).expect("seek").is_some());
        assert!(t.seek_exact(&txn, &row_key(2, 0)).expect("seek").is_some());
        assert!(c.delete_current(&mut txn).expect("delete"));
        assert!(t.delete_current(&mut txn).expect("delete"));
        assert_eq!(c.state(), (State::At, true));
        assert!(!c.follower_shares_prefix(&txn, K).expect("shares"));
        assert!(c.follower_shares_prefix(&txn, K - 1).expect("shares"));
        assert_eq!(key_of(c.prev_in_prefix(&txn, K)), None, "no row of key 2 is left");
        assert_eq!(key_of(c.prev(&txn)), Some(row_key(1, 0)));
        assert_eq!(key_of(t.prev(&txn)), Some(row_key(1, 0)));
        assert_eq!(key_of(c.next(&txn)), Some(row_key(3, 0)));
        assert_eq!(key_of(t.next(&txn)), Some(row_key(3, 0)));
        // Key 3 row by row, as the adapter's wipe loop.
        let mut n = 0;
        let mut more = true;
        while more {
            assert!(c.delete_current(&mut txn).expect("delete"));
            assert!(t.delete_current(&mut txn).expect("delete"));
            n += 1;
            let a = key_of(c.next_in_prefix(&txn, K));
            assert_eq!(a, key_of(t.next_in_prefix(&txn, K)));
            more = a.is_some();
        }
        assert_eq!(n, 120);
        assert_eq!(c.state(), (State::At, true));
        assert_eq!(c.state(), t.state());
        assert_eq!(key_of(c.current(&txn)), Some(row_key(4, 0)));
        // The last key's only row: after the end.
        assert!(c.seek_exact(&txn, &row_key(5, 0)).expect("seek").is_some());
        assert!(c.delete_current(&mut txn).expect("delete"));
        assert_eq!(c.state(), (State::AfterEnd, true));
        assert_eq!(key_of(c.prev(&txn)), Some(row_key(4, 1)));
        assert_eq!(txn.get(&d, &row_key(3, 7)).expect("get"), None);
        let left: Vec<Vec<u8>> = txn.dup_rows_for_test(&d).into_iter().map(|(k, _)| k).collect();
        assert_eq!(left, [row_key(1, 0), row_key(4, 0), row_key(4, 1)]);
        txn.commit().expect("commit");
        env.check().expect("check");
        assert!(env.failure().is_none());
    }
}

/// A key whose nested tree the transaction wrote (its outer cell still naming the committed root
/// until the commit's flush) is read from the working map by a cursor that lands on it, whether
/// by a seek into it or by a step from the key before; a cursor's own put into its key is placed
/// from the nested path cache.
#[test]
fn a_cursor_reads_the_map_root_not_the_cell() {
    let (_dir, env, d, _p, _) = setup("tndb_b_dup_txn_map", &[(1, 3), (2, 40), (3, 3)]);
    let mut txn = env.begin_rw().expect("rw");
    for s in 40..400 {
        txn.put(&d, &row_key(2, s), b"new").expect("put");
    }
    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    assert_eq!(
        key_of(c.seek_dup(&txn, &2u32.to_be_bytes(), &399u16.to_be_bytes())),
        Some(row_key(2, 399))
    );
    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    assert!(c.seek_exact(&txn, &row_key(1, 2)).expect("seek").is_some());
    let mut n = 0;
    while let Some(k) = key_of(c.next_in_prefix(&txn, K - 1)) {
        assert!(
            k.starts_with(&2u32.to_be_bytes()) || k.starts_with(&3u32.to_be_bytes()),
            "{k:02x?}"
        );
        n += 1;
    }
    assert_eq!(n, 400 + 3, "every row of key 2, the new ones included, then key 3's");
    c.seek_exact(&txn, &row_key(2, 7)).expect("seek");
    c.put(&mut txn, &row_key(2, 8), b"again").expect("put");
    assert_eq!(c.placements, 1, "placed from the nested path cache");
    assert_eq!(c.current(&txn).expect("current"), Some((&row_key(2, 8)[..], &b"again"[..])));
    assert_eq!(key_of(c.next(&txn)), Some(row_key(2, 9)));
    assert_eq!(c.reseeks, 0, "its own write is no reseek");
    txn.commit().expect("commit");
    env.check().expect("check");
}

/// A cell naming a page private to the transaction with no working entry (the entry forgotten
/// through the test hook) is `Corrupt` under a dup cursor: the cursor is unset and the
/// environment latched.
#[test]
fn a_private_cell_with_no_map_entry_is_corrupt_under_a_dup_cursor() {
    let (_dir, env, d, _p, _) = setup("tndb_b_dup_txn_private", &[(1, 3), (3, 3)]);
    let mut txn = env.begin_rw().expect("rw");
    for s in 0..5 {
        txn.put(&d, &row_key(2, s), b"new").expect("put");
    }
    assert!(txn.forget_nested(&d, &2u32.to_be_bytes()));
    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    assert!(c.seek_exact(&txn, &row_key(1, 2)).expect("seek").is_some());
    let stepped = c.next(&txn);
    assert!(matches!(stepped, Err(Error::Corrupt(_))), "{stepped:?}");
    assert_eq!(c.state(), (State::Unset, false));
    assert!(env.failure().is_some(), "Corrupt latches");
    txn.abort();
}

/// A dup cursor used with another transaction is `WrongTxn`, and after its table was dropped
/// `StaleTable`: nothing changes and nothing latches.
#[test]
fn dup_txn_cursor_wrong_txn_and_stale_table() {
    let (_dir, env, d, _p, _) = setup("tndb_b_dup_txn_wrong", &[(1, 3), (2, 3)]);
    let txn = env.begin_rw().expect("rw");
    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    assert!(c.seek_exact(&txn, &row_key(1, 1)).expect("seek").is_some());
    txn.abort();
    let mut txn = env.begin_rw().expect("rw");
    let wrong = |r: Result<bool>| matches!(r, Err(Error::WrongTxn));
    assert!(wrong(c.next(&txn).map(|r| r.is_some())));
    assert!(wrong(c.seek_key(&txn, &2u32.to_be_bytes()).map(|r| r.is_some())));
    assert!(wrong(c.put(&mut txn, &row_key(1, 9), b"v").map(|()| true)));
    assert!(wrong(c.delete_current(&mut txn)));
    assert!(wrong(c.follower_shares_prefix(&txn, K)));
    assert_eq!(c.state(), (State::At, false), "unchanged");
    assert_eq!(txn.get(&d, &row_key(1, 9)).expect("get"), None);

    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    assert!(c.seek_exact(&txn, &row_key(2, 0)).expect("seek").is_some());
    txn.drop_table(d.clone()).expect("drop");
    let stale = |r: Result<bool>| matches!(r, Err(Error::StaleTable(_)));
    assert!(stale(c.next(&txn).map(|r| r.is_some())));
    assert!(stale(c.seek(&txn, b"").map(|r| r.is_some())));
    assert!(stale(c.put(&mut txn, &row_key(1, 9), b"v").map(|()| true)));
    assert!(stale(c.delete_current(&mut txn)));
    assert_eq!(c.state(), (State::At, false), "unchanged");
    assert!(env.failure().is_none(), "neither latches");
    txn.abort();

    let mut txn = env.begin_rw().expect("rw");
    let p = txn.create_table("plain").expect("plain");
    let refused = txn.dup_cursor_detached(&p);
    assert!(matches!(&refused, Err(Error::TableKind(n)) if n == "plain"), "{refused:?}");
    assert!(env.failure().is_none());
}

/// A refused write through a dup cursor (an insert of a present row, an append out of order, a
/// key with no subkey, a key over the limit) changes nothing: the rows, the cursor's state and
/// row, and its next call makes no reseek; a refused bounded seek of another width does not move
/// it either.
#[test]
fn refused_writes_through_a_dup_cursor_change_nothing() {
    let (_dir, env, d, _p, _) = setup("tndb_b_dup_txn_refused", &[(1, 3), (2, 30), (3, 3)]);
    let mut txn = env.begin_rw().expect("rw");
    let mut c = txn.dup_cursor_detached(&d).expect("cursor");
    let long = vec![7; env.max_key_len() + 1];
    for deleted in [false, true] {
        assert!(c
            .seek_exact(&txn, &row_key(2, 10 + 2 * u16::from(deleted)))
            .expect("seek")
            .is_some());
        if deleted {
            assert!(c.delete_current(&mut txn).expect("delete"));
        }
        let before = c.state();
        let reseeks = c.reseeks;
        let rows_before = txn.dup_rows_for_test(&d);
        let refusals = [
            c.insert(&mut txn, &row_key(2, 20), b"x"),
            c.append(&mut txn, &row_key(1, 50), b"x"),
            c.append(&mut txn, &row_key(3, 1), b"x"),
            c.put(&mut txn, &2u32.to_be_bytes(), b"x"),
            c.put(&mut txn, &long, b"x"),
        ];
        let names: Vec<String> = refusals.iter().map(|r| format!("{r:?}")).collect();
        assert!(matches!(refusals[0], Err(Error::KeyExists)), "{names:?}");
        assert!(matches!(refusals[1], Err(Error::AppendOutOfOrder)), "{names:?}");
        assert!(matches!(refusals[2], Err(Error::AppendOutOfOrder)), "{names:?}");
        assert!(matches!(refusals[3], Err(Error::KeyTooShort { .. })), "{names:?}");
        assert!(matches!(refusals[4], Err(Error::KeyTooLarge { .. })), "{names:?}");
        assert!(matches!(c.seek_key(&txn, &[1, 2]), Err(Error::KeyTooShort { .. })));
        assert!(matches!(c.seek_dup(&txn, &[1; K + 1], b"s"), Err(Error::KeyTooLarge { .. })));
        assert_eq!(txn.dup_rows_for_test(&d), rows_before, "the rows are unchanged");
        assert_eq!(c.state(), before, "the cursor is unmoved, its deleted flag included");
        let want = if deleted { row_key(2, 13) } else { row_key(2, 10) };
        assert_eq!(key_of(c.current(&txn)), Some(want), "on the same row (the follower)");
        assert_eq!(c.reseeks, reseeks, "no reseek after a refusal");
    }
    assert!(env.failure().is_none(), "refusals do not latch");
    txn.abort();
}

/// A cursor's put into the key of the row it is at, after another writer created a key before it
/// (moving the outer path's slots), finds its row by key rather than keep the stale outer path: a
/// scan from it then crosses to the right keys. A put that creates a key is found by key too.
#[test]
fn a_cursor_put_after_another_writers_change_finds_its_row() {
    let (_dir, env, d, p, _) = setup("tndb_b_dup_txn_put_stale", &[(2, 5), (4, 5), (6, 5)]);
    let mut txn = env.begin_rw().expect("rw");
    // Writes in this transaction first, so the outer leaf is its own and changes in place.
    for k in [8u32, 10] {
        txn.put(&d, &row_key(k, 0), b"x").expect("put");
        txn.put(&p, &row_key(k, 0), b"x").expect("put");
    }
    let (mut c, mut t) = pair(&txn, &d, &p);
    assert!(c.seek_exact(&txn, &row_key(4, 2)).expect("seek").is_some());
    assert!(t.seek_exact(&txn, &row_key(4, 2)).expect("seek").is_some());
    txn.put(&d, &row_key(3, 0), b"before").expect("put");
    txn.put(&p, &row_key(3, 0), b"before").expect("put");
    c.put(&mut txn, &row_key(4, 9), b"mine").expect("put");
    t.put(&mut txn, &row_key(4, 9), b"mine").expect("put");
    assert_eq!(c.placements, 0, "a stale cursor's put is found by key");
    loop {
        let a = got(c.next(&txn));
        assert_eq!(a, got(t.next(&txn)));
        if a.expect("next").is_none() {
            break;
        }
    }
    assert!(c.seek_exact(&txn, &row_key(6, 1)).expect("seek").is_some());
    c.put(&mut txn, &row_key(7, 0), b"created").expect("put");
    assert_eq!(key_of(c.current(&txn)), Some(row_key(7, 0)));
    assert_eq!(key_of(c.prev(&txn)), Some(row_key(6, 4)));
    assert_eq!(key_of(c.prev(&txn)), Some(row_key(6, 3)));
    assert!(env.failure().is_none());
    txn.commit().expect("commit");
    env.check().expect("check");
}
