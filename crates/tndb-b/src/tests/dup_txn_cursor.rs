//! A write transaction's dup cursor ([`DupTxnCursor`]) against the composite twin: a dup table
//! `d` (key width 4) and a plain table `p` keyed `k ‖ s` holding the same rows, a dup cursor over
//! `d` and a [`TxnCursor`] over `p` in one transaction, each call compared, with the
//! transaction's own writes between the moves; then the cases one by one: staleness after
//! another writer's put, delete, emptied key, `delete_prefix` and `clear_table`, the placements of
//! the cursor's own deletes and puts (from the path caches and by key), the deleted state and
//! `follower_shares_prefix`, the working map before the cells, and the refusals; last, damage met
//! by a write cursor (forged nested pages and outer keys, CRC stamped again).

use std::collections::BTreeMap;

use tempfile::TempDir;

use super::{
    damage::{restamped, P},
    dup::{copy_of, damaged, dup_key, restamp_cell, COLD, HOT, OPTIONS},
    open, temp, Rows,
};
use crate::{
    env::SyncMode,
    page::{self, Val},
    tree::{
        cursor::{
            tests::{owned, owned_tail, Kv},
            State,
        },
        test_harness::{ReportSeed, Rng},
        TreeRoot,
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
    SeekKey(Vec<u8>),
    SeekDup(Vec<u8>, Vec<u8>),
    SeekDupWithKeyTail(Vec<u8>, Vec<u8>, usize),
    Shares(usize),
    CursorPut(Vec<u8>, Vec<u8>),
    CursorInsert(Vec<u8>, Vec<u8>),
    CursorAppend(Vec<u8>, Vec<u8>),
    CursorDelete,
    Put(Vec<u8>, Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Append(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeletePrefix(Vec<u8>),
    /// The wipe loop on both tables, a dup cursor over `d` beside a plain one over `p`.
    Wipe(Vec<u8>),
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

/// The `i`th of the outer keys beside [`OUTER`] that give the outer tree its height (one row
/// each at the start), between `OUTER`'s fifth and sixth; neighbours share 2 or 3 leading bytes.
fn filler(i: u32) -> u32 {
    0x4000_0000 + (i << 8)
}

/// A logical key for a write: a row's own, or a new subkey under a known outer key (one of
/// [`OUTER`], or of `fillers` fillers), now and then a subkey one byte longer, mostly.
fn write_key(rng: &mut Rng, model: &Rows, fillers: u32) -> Vec<u8> {
    if !model.is_empty() && rng.below(3) == 0 {
        let i = rng.below(model.len() as u64) as usize;
        return model.keys().nth(i).expect("row").clone();
    }
    let k = match fillers > 0 && rng.below(8) == 0 {
        true => filler(rng.below(u64::from(fillers)) as u32),
        false => OUTER[rng.below(OUTER.len() as u64) as usize],
    };
    let key = row_key(k, rng.below(400) as u16);
    match rng.below(8) {
        0 => [&key[..], &[rng.next() as u8]].concat(),
        _ => key,
    }
}

/// A key for an append: past the last row half the time (the last key with its last byte one
/// up), else as [`write_key`].
fn append_key(rng: &mut Rng, model: &Rows, fillers: u32) -> Vec<u8> {
    match model.last_key_value() {
        Some((last, _)) if rng.below(2) == 0 => {
            let mut key = last.clone();
            match key.last_mut().expect("a row's key") {
                0xff => key.push(0),
                b => *b += 1,
            }
            key
        }
        _ => write_key(rng, model, fillers),
    }
}

/// An outer key for a bounded seek: a row's or a known one, now and then of another width.
fn seek_outer(rng: &mut Rng, model: &Rows, fillers: u32) -> Vec<u8> {
    let k = write_key(rng, model, fillers)[..K].to_vec();
    match rng.below(10) {
        0 => k[..K - 1].to_vec(),
        1 => [&k[..], &[0]].concat(),
        _ => k,
    }
}

/// A seek target of every length class around a row's key (or a random one).
fn seek_target(rng: &mut Rng, model: &Rows, fillers: u32) -> Vec<u8> {
    let r = write_key(rng, model, fillers);
    match rng.below(6) {
        0 => r[..rng.below(K as u64) as usize].to_vec(),
        1 => r[..K].to_vec(),
        2 => [&r[..], &[0]].concat(),
        3 => r[..K + 1].to_vec(),
        _ => r,
    }
}

/// A bounded seek's result on the twin: the seek's row kept only if it is under outer key `k`,
/// else the cursor unset (a `seek_exact` of the outer key alone, which no twin key equals).
fn bounded(txn: &RwTxn, twin: &mut TxnCursor, k: &[u8], found: Got) -> Got {
    match found {
        Ok(Some(row)) if row.0.starts_with(k) => Ok(Some(row)),
        Ok(_) => {
            assert_eq!(twin.seek_exact(txn, k).expect("seek_exact"), None, "an outer key alone");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// The wipe loop over outer key `k` on both tables at once, a dup cursor over `d` and a plain
/// cursor over `p`, each call and state compared; then the state it leaves: the deleted flag,
/// `follower_shares_prefix` around `K`, and a step (`next` after an even count of deletes,
/// `prev` after an odd one), whose results it returns.
fn wipe_both(txn: &mut RwTxn, (d, p): (&Table, &Table), k: &[u8]) -> (Got, Got) {
    let (mut a, mut b) = pair(txn, d, p);
    let seek = (got(a.seek(txn, k)), got(b.seek(txn, k)));
    assert_eq!(seek.0, seek.1, "wipe {k:02x?}: seek");
    let mut more = matches!(&seek.1, Ok(Some((key, _))) if key.starts_with(k));
    let mut n = 0;
    while more {
        let del = (flag(a.delete_current(txn)), flag(b.delete_current(txn)));
        assert_eq!(del.0, del.1, "wipe {k:02x?}: delete {n}");
        assert_eq!(a.state(), b.state(), "wipe {k:02x?}: after delete {n}");
        n += 1;
        let step = (got(a.next_in_prefix(txn, K)), got(b.next_in_prefix(txn, K)));
        assert_eq!(step.0, step.1, "wipe {k:02x?}: next_in_prefix after delete {n}");
        assert_eq!(a.state(), b.state(), "wipe {k:02x?}: after step {n}");
        more = matches!(&step.1, Ok(Some(_)));
    }
    for m in [K - 1, K, K + 1] {
        let shares =
            (flag(a.follower_shares_prefix(txn, m)), flag(b.follower_shares_prefix(txn, m)));
        assert_eq!(shares.0, shares.1, "wipe {k:02x?}: follower_shares_prefix({m})");
    }
    match n % 2 {
        0 => (got(a.next(txn)), got(b.next(txn))),
        _ => (got(a.prev(txn)), got(b.prev(txn))),
    }
}

impl Step {
    fn pick(rng: &mut Rng, model: &Rows, fillers: u32) -> Self {
        let n = |rng: &mut Rng| [0, 1, K - 1, K, K + 1, K + 2, K + 3][rng.below(7) as usize];
        let v = |rng: &mut Rng| vec![rng.next() as u8; rng.below(30) as usize];
        let t = |rng: &mut Rng| [0, 1, 2, 3, K + 2, K + 3, usize::MAX][rng.below(7) as usize];
        let sub = |rng: &mut Rng, model: &Rows| match rng.below(4) {
            0 => Vec::new(),
            _ => write_key(rng, model, fillers)[K..].to_vec(),
        };
        let key = |rng: &mut Rng, model: &Rows| write_key(rng, model, fillers);
        let target = |rng: &mut Rng, model: &Rows| seek_target(rng, model, fillers);
        match rng.below(48) {
            0 => Self::First,
            1 => Self::Last,
            2..=6 => Self::Next,
            7..=9 => Self::Prev,
            10 => Self::Current,
            11 => Self::CurrentWithKeyTail(t(rng)),
            12 | 13 => Self::Seek(target(rng, model)),
            14 => Self::SeekExact(target(rng, model)),
            15 => Self::SeekWithKeyTail(target(rng, model), t(rng)),
            16..=18 => Self::NextInPrefix(n(rng)),
            19..=21 => Self::PrevInPrefix(n(rng)),
            22 => Self::SeekKey(seek_outer(rng, model, fillers)),
            23 => Self::SeekDup(seek_outer(rng, model, fillers), sub(rng, model)),
            24 => {
                Self::SeekDupWithKeyTail(seek_outer(rng, model, fillers), sub(rng, model), t(rng))
            }
            25 | 26 => Self::Shares(n(rng)),
            27 | 28 => Self::CursorPut(key(rng, model), v(rng)),
            29 => Self::CursorInsert(key(rng, model), v(rng)),
            30 => Self::CursorAppend(append_key(rng, model, fillers), v(rng)),
            31..=33 => Self::CursorDelete,
            34 | 35 => Self::Put(key(rng, model), v(rng)),
            36 => Self::Insert(key(rng, model), v(rng)),
            37 => Self::Append(append_key(rng, model, fillers), v(rng)),
            38..=41 => Self::Delete(key(rng, model)),
            42 => Self::DeletePrefix(key(rng, model)[..K].to_vec()),
            43 => Self::Wipe(key(rng, model)[..K].to_vec()),
            44 if rng.below(60) == 0 => Self::Clear,
            _ => Self::Next,
        }
    }

    /// Whether the step is a write of the transaction's own beside the cursors.
    fn is_direct_write(&self) -> bool {
        matches!(
            self,
            Self::Put(..)
                | Self::Insert(..)
                | Self::Append(..)
                | Self::Delete(_)
                | Self::DeletePrefix(_)
                | Self::Wipe(_)
                | Self::Clear
        )
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
            Self::SeekKey(k) | Self::SeekDup(k, _) | Self::SeekDupWithKeyTail(k, _, _)
                if k.len() != K =>
            {
                let refusal = Err(format!(
                    "{:?}",
                    match k.len() < K {
                        true => Error::KeyTooShort { len: k.len(), min: K },
                        false => Error::KeyTooLarge { len: k.len(), max: K },
                    }
                ));
                let a = match self {
                    Self::SeekKey(k) => got(dup.seek_key(txn, k)),
                    Self::SeekDup(k, s) => got(dup.seek_dup(txn, k, s)),
                    Self::SeekDupWithKeyTail(k, s, t) => {
                        tail(dup.seek_dup_with_key_tail(txn, k, s, *t), *t)
                    }
                    _ => unreachable!(),
                };
                (a, refusal)
            }
            Self::SeekKey(k) => {
                let b = got(twin.seek(txn, k));
                (got(dup.seek_key(txn, k)), bounded(txn, twin, k, b))
            }
            Self::SeekDup(k, s) => {
                let b = got(twin.seek(txn, &[&k[..], s].concat()));
                (got(dup.seek_dup(txn, k, s)), bounded(txn, twin, k, b))
            }
            Self::SeekDupWithKeyTail(k, s, t) => {
                let b = tail(twin.seek_with_key_tail(txn, &[&k[..], s].concat(), *t), *t);
                (tail(dup.seek_dup_with_key_tail(txn, k, s, *t), *t), bounded(txn, twin, k, b))
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
            Self::Insert(key, v) | Self::Append(key, v) => {
                let r = match self {
                    Self::Insert(..) => (done(txn.insert(d, key, v)), done(txn.insert(p, key, v))),
                    _ => (done(txn.append(d, key, v)), done(txn.append(p, key, v))),
                };
                if r.1.is_ok() {
                    model.insert(key.clone(), v.clone());
                }
                r
            }
            Self::Wipe(k) => {
                let r = wipe_both(txn, (d, p), k);
                model.retain(|key, _| !key.starts_with(k));
                r
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

/// What Oracle 2b's runs saw: each count is a coverage gate ([`Seen::assert_complete`]).
#[derive(Debug, Default)]
struct Seen {
    /// Reseeks after another writer's change ([`DupTxnCursor::reseeks`]).
    reseeks: u32,
    /// The cursors' own deletes by how they landed ([`DupTxnCursor::landings`]): from the nested
    /// path cache, from the outer one (the key emptied), by key.
    landings: [u32; 3],
    /// Steps that left the cursor in the deleted state.
    deleted: u32,
    /// Another writer's delete of the row a cursor was at, and of a deleted cursor's follower.
    deleted_under: u32,
    follower_gone: u32,
    /// Another writer's delete, `delete_prefix` or wipe that emptied the key a cursor was in.
    emptied_under: u32,
    /// `delete_prefix` or a wipe of the key a cursor was in.
    prefix_under: u32,
    clears: u32,
    wipes: u32,
    /// The cursors' own deletes whose follower is in the next key, of the table's last row (no
    /// follower), and that emptied the key.
    follower_next_key: u32,
    table_last_row: u32,
    emptied_by_cursor: u32,
    /// Refused cursor writes, and cursor writes that created an outer key.
    refused: u32,
    created: u32,
    /// Prefix steps from a row (or a deleted row), by direction (next, prev), class (`n < K` to
    /// another key, `n < K` inside the key, `n < K` missing, `n = K`, `K < n <= L`, `n > L`; `L`
    /// the key's length) and outcome (miss, hit).
    prefix: [[[u32; 2]; 6]; 2],
    /// `prev_in_prefix(K)` in the deleted state after the deleted row was its key's last
    /// (miss: the key had no row before it either; hit).
    last_row_then_prev: [u32; 2],
    /// `follower_shares_prefix` in the deleted state (false, true).
    shares: [u32; 2],
}

impl Seen {
    /// Count a prefix step `next` (or not) by `n` from `base` (the cursor's key, or its deleted
    /// row's) that gave `r`.
    fn prefix_step(&mut self, next: bool, n: usize, base: Option<&[u8]>, r: &Got) {
        let (Some(base), Ok(r)) = (base, r) else { return };
        let class = match (n, r) {
            (n, _) if n > base.len() => 5,
            (n, _) if n > K => 4,
            (n, _) if n == K => 3,
            (_, Some((key, _))) if key[..K] != base[..K] => 0,
            (_, Some(_)) => 1,
            (_, None) => 2,
        };
        self.prefix[usize::from(!next)][class][usize::from(r.is_some())] += 1;
    }

    fn assert_complete(&self) {
        let mut gates = vec![
            ("reseeks", self.reseeks),
            ("landed from the nested cache", self.landings[0]),
            ("landed from the outer cache", self.landings[1]),
            ("landed by key", self.landings[2]),
            ("deleted states", self.deleted),
            ("deleted under", self.deleted_under),
            ("follower gone", self.follower_gone),
            ("emptied under", self.emptied_under),
            ("delete_prefix under", self.prefix_under),
            ("clears", self.clears),
            ("wipes", self.wipes),
            ("follower in the next key", self.follower_next_key),
            ("the table's last row", self.table_last_row),
            ("emptied by the cursor", self.emptied_by_cursor),
            ("refused cursor writes", self.refused),
            ("cursor writes creating a key", self.created),
            ("last row then prev_in_prefix(K) miss", self.last_row_then_prev[0]),
            ("last row then prev_in_prefix(K) hit", self.last_row_then_prev[1]),
            ("shares false", self.shares[0]),
            ("shares true", self.shares[1]),
        ];
        let classes = [
            ("n<K across", 1),
            ("n<K inside", 1),
            ("n<K miss", 0),
            ("n=K miss", 0),
            ("n=K hit", 1),
            ("K<n<=L miss", 0),
            ("K<n<=L hit", 1),
            ("n>L", 0),
        ];
        for (dir, name) in [(0, "next"), (1, "prev")] {
            for (i, (class, hit)) in classes.iter().enumerate() {
                let c = [0, 1, 2, 3, 3, 4, 4, 5][i];
                gates.push((leak(format!("{name} {class}")), self.prefix[dir][c][*hit]));
            }
        }
        let missing: Vec<_> = gates.iter().filter(|(_, n)| *n == 0).map(|(g, _)| *g).collect();
        assert!(missing.is_empty(), "coverage gates not reached: {missing:?}\n{self:#?}");
    }
}

/// A gate's name, for the life of the test.
fn leak(name: String) -> &'static str {
    Box::leak(name.into_boxed_str())
}

/// Oracle 2b over `seeds`: twins of [`OUTER`]'s keys (the third with `big` rows, the others a
/// few) and `fillers` [`filler`] keys of one row; `rounds` write transactions, each with `pairs`
/// cursor pairs taking `steps` random steps between them, a pair now and then replaced by a new
/// one; each round commits (the file then checks) or aborts. Each step's results and the
/// stepped pair's states are compared, and half the time its `follower_shares_prefix` around
/// `K`; the coverage gates must all be reached.
fn lockstep(
    seeds: std::ops::Range<u64>,
    big: u16,
    fillers: u32,
    rounds: u32,
    steps: u32,
    pairs: usize,
) {
    let mut seen = Seen::default();
    for seed in seeds {
        let seed = 0x2b2b_0000_0000 + seed;
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let (_dir, path) = temp("tndb_b_dup_txn_cursor_lockstep");
        let env = open(&path, SyncMode::NoSync);
        let mut keys: Vec<(u32, u16)> = OUTER
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, if i == 2 { big } else { 1 + i as u16 * 3 }))
            .collect();
        keys.extend((0..fillers).map(|i| (filler(i), 1)));
        keys.sort_unstable();
        let (d, p, initial) = twins(&env, &keys);
        let mut model = initial.clone();
        let stat = env.begin_ro().expect("ro").table_stat(&d).expect("stat");
        assert!(fillers == 0 || stat.height >= 2, "the outer tree's height: {stat:?}");
        for round in 0..rounds {
            if model.len() < initial.len() / 2 {
                let mut txn = env.begin_rw().expect("rw");
                for (key, v) in &initial {
                    txn.put(&d, key, v).expect("refill d");
                    txn.put(&p, key, v).expect("refill p");
                    model.insert(key.clone(), v.clone());
                }
                txn.commit().expect("commit");
            }
            let mut txn = env.begin_rw().expect("rw");
            let mut round_model = model.clone();
            let mut pairs: Vec<_> = (0..pairs).map(|_| pair(&txn, &d, &p)).collect();
            for step in 0..steps {
                let j = rng.below(pairs.len() as u64) as usize;
                if rng.below(400) == 0 {
                    pairs[j] = pair(&txn, &d, &p);
                }
                let op = Step::pick(&mut rng, &round_model, fillers);
                let before: Vec<(Option<Vec<u8>>, bool)> = match op.is_direct_write() {
                    true => pairs
                        .iter()
                        .map(|(c, _)| (c.key_for_test().map(<[u8]>::to_vec), c.state().1))
                        .collect(),
                    false => Vec::new(),
                };
                let (dup, twin) = &mut pairs[j];
                let from = (dup.state(), twin.state());
                let base = dup.deleted_key_for_test().or(dup.key_for_test()).map(<[u8]>::to_vec);
                let follower = dup.key_for_test().map(<[u8]>::to_vec);
                let creates = match &op {
                    Step::CursorPut(key, _)
                    | Step::CursorInsert(key, _)
                    | Step::CursorAppend(key, _) => !has_key(&round_model, &key[..K]),
                    _ => false,
                };
                let (a, b) = op.run(&mut txn, (&d, &p), (dup, twin), &mut round_model);
                let at = format!("round {round} step {step} pair {j} {op:?} from {from:?}");
                assert_eq!(a, b, "{at}");
                assert_eq!(dup.state(), twin.state(), "{at}");
                if rng.below(2) == 0 {
                    for n in [K - 1, K, K + 1] {
                        let x = flag(dup.follower_shares_prefix(&txn, n));
                        assert_eq!(
                            x,
                            flag(twin.follower_shares_prefix(&txn, n)),
                            "{at}: shares {n}"
                        );
                        if dup.state().1 {
                            seen.shares[usize::from(matches!(x, Ok(Some(_))))] += 1;
                        }
                    }
                }
                seen.deleted += u32::from(dup.state().1);
                match &op {
                    Step::NextInPrefix(n) => seen.prefix_step(true, *n, base.as_deref(), &b),
                    Step::PrevInPrefix(n) => {
                        seen.prefix_step(false, *n, base.as_deref(), &b);
                        if let (true, Some(dk), Ok(r)) = (*n == K && from.0 .1, &base, &b) {
                            if follower.as_ref().is_none_or(|f| f[..K] != dk[..K]) {
                                seen.last_row_then_prev[usize::from(r.is_some())] += 1;
                            }
                        }
                    }
                    Step::CursorPut(..) | Step::CursorInsert(..) | Step::CursorAppend(..) => {
                        seen.refused += u32::from(b.is_err());
                        seen.created += u32::from(b.is_ok() && creates);
                    }
                    Step::CursorDelete if matches!(b, Ok(Some(_))) => {
                        let dk = dup.deleted_key_for_test().expect("deleted").to_vec();
                        match dup.key_for_test() {
                            None => seen.table_last_row += 1,
                            Some(f) if f[..K] != dk[..K] => seen.follower_next_key += 1,
                            Some(_) => {}
                        }
                        seen.emptied_by_cursor += u32::from(!has_key(&round_model, &dk[..K]));
                    }
                    Step::Clear => seen.clears += 1,
                    _ => {}
                }
                for (key, deleted) in &before {
                    let Some(key) = key else { continue };
                    match &op {
                        Step::Delete(x) if x == key => match deleted {
                            true => seen.follower_gone += 1,
                            false => seen.deleted_under += 1,
                        },
                        Step::DeletePrefix(k) | Step::Wipe(k) if key.starts_with(k) => {
                            seen.prefix_under += 1
                        }
                        _ => {}
                    }
                    let emptied =
                        matches!(&op, Step::Delete(_) | Step::DeletePrefix(_) | Step::Wipe(_))
                            && !has_key(&round_model, &key[..K]);
                    seen.emptied_under += u32::from(emptied);
                }
                seen.wipes += u32::from(matches!(op, Step::Wipe(_)));
            }
            for (dup, _) in &pairs {
                seen.reseeks += dup.reseeks;
                for (s, n) in seen.landings.iter_mut().zip(dup.landings) {
                    *s += n;
                }
            }
            let rows: Rows = txn.dup_rows_for_test(&d).into_iter().collect();
            assert_eq!(rows, round_model, "round {round}: the dup table's rows");
            if rng.below(2) == 0 {
                txn.commit().expect("commit");
                env.check().expect("check");
                model = round_model;
            } else {
                txn.abort();
            }
        }
        assert!(env.failure().is_none(), "no call latched: {:?}", env.failure());
    }
    seen.assert_complete();
}

/// Whether `rows` holds a row under outer key `k`.
fn has_key(rows: &Rows, k: &[u8]) -> bool {
    rows.range(k.to_vec()..).next().is_some_and(|(key, _)| key.starts_with(k))
}

/// Oracle 2b: pairs of a dup cursor over `d` and a twin cursor over `p` in one transaction take
/// the same random calls (moves, seeks, the bounded seeks, prefix steps, cursor writes), with the
/// transaction's own puts, inserts, appends, deletes, `delete_prefix` (the twin's: the wipe
/// loop), wipe loops on both tables and clears between them; every call gives the same result
/// and leaves the same state, deleted flag included, and `follower_shares_prefix` agrees. The
/// coverage gates: reseeks, each landing of a cursor's delete, another writer's deletes under a
/// cursor (its row, its follower, emptying its key, `delete_prefix` of it), the cursor's own
/// deletes into the next key and of the table's last row, refused and key-creating cursor
/// writes, prefix steps of every class both ways, and `prev_in_prefix(K)` after a key's last
/// row is deleted.
#[test]
fn dup_txn_cursors_and_direct_writes_match_the_composite_twin() {
    lockstep(0..6, 300, 300, 5, 3000, 3);
}

/// [`dup_txn_cursors_and_direct_writes_match_the_composite_twin`] over more seeds, rounds and
/// steps, a larger nested tree and more cursor pairs.
#[test]
#[ignore = "heavy: run with --release"]
fn dup_txn_cursors_and_direct_writes_match_the_composite_twin_heavy() {
    lockstep(0..64, 1500, 600, 6, 6000, 5);
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

// ---- damage under a write cursor ----

/// Damage met by a write transaction's dup cursor, each case a file of [`damaged`]'s forged and
/// CRC-stamped again: a nested cell in [`COLD`]'s nested leaf (in place of its subkey 1), an
/// outer key of 5 bytes (COLD's, its nested root kept), an empty subkey (in place of COLD's
/// subkey 0), and a separator byte of [`HOT`]'s nested branch root changed (the first row it
/// misroutes found by a get that misses it, silently). Each is met, on a fresh copy each, by the
/// cursor's seek to the damaged row, by its step to it from the row before (`next`), and by
/// `delete_current` of the row before (its follower the damaged row; the separator's delete meets
/// the branch's key order as it copies the branch): `Corrupt` naming the damage, the cursor unset
/// and out of the deleted state, the environment latched.
#[test]
fn damage_under_a_dup_cursor_is_corrupt() {
    let dir = TempDir::with_prefix("tndb_b_dup_txn_damage").expect("temp dir");
    let forged =
        TreeRoot { root: 9, root_txn: 7, height: 1, entries: 3, leaf_pages: 1, ..TreeRoot::EMPTY };
    let (cold, hot_last) = (COLD.to_be_bytes(), dup_key(HOT, b"big"));
    type Forge = Box<dyn Fn(&std::path::Path, &BTreeMap<u32, (u32, TreeRoot)>)>;
    let cases: [(&str, &[&str], Forge); 4] = [
        (
            "a nested cell in a nested leaf",
            &["a nested cell in a tree that holds none"],
            Box::new(move |path, cells| {
                restamp_cell(path, cells[&COLD].1.root, &[1], Val::Nested(&forged.encode()))
            }),
        ),
        (
            "an outer key of another width",
            &["an outer key of 5 bytes, not the table's width 4"],
            Box::new(move |path, cells| {
                restamped(path, cells[&COLD].0, |page| {
                    let page::Find::Found { index, value: Val::Nested(root) } =
                        page::leaf_find(page, &cold).expect("find")
                    else {
                        panic!("COLD's nested cell");
                    };
                    let root = *root;
                    page::LeafMut::new(page).expect("a leaf").delete(index).expect("delete");
                    let wide = [&cold[..], &[0]].concat();
                    let fit = page::LeafMut::new(page).expect("a leaf").insert(
                        index,
                        &wide,
                        Val::Nested(&root),
                    );
                    assert_eq!(fit.expect("inserted"), page::Fit::Done);
                })
            }),
        ),
        (
            "an empty subkey",
            &["a subkey of 0 bytes"],
            Box::new(|path, cells| {
                restamped(path, cells[&COLD].1.root, |page| {
                    let (s, value) = page::leaf_inline_entry(page, 0).expect("row 0");
                    assert_eq!(s, [0], "COLD's first subkey");
                    let value = value.to_vec();
                    page::LeafMut::new(page).expect("a leaf").delete(0).expect("delete");
                    let fit = page::LeafMut::new(page).expect("a leaf").insert(0, &[], &value[..]);
                    assert_eq!(fit.expect("inserted"), page::Fit::Done);
                })
            }),
        ),
        (
            "a misrouted separator",
            &["outside its parents' separators", "out of key order"],
            Box::new(|path, cells| {
                let at = (P - 16) as usize;
                restamped(path, cells[&HOT].1.root, |page| page[at] ^= 0x10);
            }),
        ),
    ];
    for (i, (what, messages, forge)) in cases.into_iter().enumerate() {
        let sub = dir.path().join(i.to_string());
        std::fs::create_dir(&sub).expect("a case's dir");
        let (path, model) = damaged(&sub, |path, cells| forge(path, cells));
        // The damaged row and the row before it in F.
        let (target, before) = match i {
            0 => (dup_key(COLD, &[1]), dup_key(COLD, &[0])),
            1 | 2 => (cold.to_vec(), hot_last.clone()),
            _ => {
                let (_dir, env) = copy_of(&path);
                let ro = env.begin_ro().expect("ro");
                let d = ro.open_table("d").expect("open").expect("d");
                let missed = model
                    .keys()
                    .find(|key| ro.get(&d, key).expect("get").is_none())
                    .expect("a misrouted row")
                    .clone();
                assert!(env.failure().is_none(), "{what}: gets miss, unlatched");
                let before = model.range(..missed.clone()).next_back().expect("a row before");
                (missed, before.0.clone())
            }
        };
        for op in ["seek", "next", "delete_current"] {
            let (_dir, env) = copy_of(&path);
            let mut txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            let mut c = txn.dup_cursor_detached(&d).expect("cursor");
            let result = match op {
                "seek" => c.seek(&txn, &target).map(drop),
                _ => {
                    let at = c.seek_exact(&txn, &before).expect("the row before").is_some();
                    assert!(at, "{what}: {before:02x?} reads");
                    if op == "next" {
                        c.next(&txn).map(drop)
                    } else {
                        c.delete_current(&mut txn).map(drop)
                    }
                }
            };
            let corrupt = matches!(&result, Err(Error::Corrupt(m))
                if messages.iter().any(|message| m.contains(message)));
            assert!(corrupt, "{what} {op}: {result:?}");
            assert_eq!(c.state(), (State::Unset, false), "{what} {op}");
            assert!(env.failure().is_some(), "{what} {op}: latched");
        }
    }
}
