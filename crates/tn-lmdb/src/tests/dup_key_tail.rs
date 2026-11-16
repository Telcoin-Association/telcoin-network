//! The dup cursors' tail moves (`first_with_key_tail`, `next_in_prefix_with_key_tail`,
//! `seek_dup_with_key_tail`, ...) against their two-call form: two cursors over the same rows in
//! lockstep, one making each move through its tail twin, the other making the plain move and then,
//! if it landed, `current_with_key_tail`; every result, the [`KeyTail`] form included, and every
//! position compared. Over a [`DupRoCursor`] on `dup_cursor.rs`'s twins at every packing bound;
//! over a [`DupTxnCursor`] in two environments of the same rows, the same writes between the moves
//! (by the cursor, by another cursor on the table, by the transaction). Last, damage: each tail
//! move meets it with its plain move's `Corrupt`, and leaves the same state.

use std::{cmp::Ordering, collections::BTreeMap, fs, sync::Arc};

use tempfile::TempDir;

use super::{
    damage::{restamped, P},
    dup::{copy_of, damaged, dup_key, restamp_cell, Bound, COLD, HOT, OPTIONS},
    dup_cursor::{
        bounded_target, forms, pick_row, subkey, target, twins, value, Shape, HEAVY_SHAPE,
        SMALL_SHAPE,
    },
    open, temp,
};
use crate::{
    env::SyncMode,
    page::{self, Val},
    tree::{
        cursor::{
            tests::{joined, owned, Kv},
            KeyTail, State,
        },
        dup_cursor::InKey,
        test_harness::{ReportSeed, Rng},
        TreeRoot,
    },
    DupRoCursor, DupTxnCursor, ReadTxn as _, Result, RwTxn, Table,
};

/// The key width of the dup tables here ([`OPTIONS`]).
const K: usize = OPTIONS.key_len as usize;

/// The widest subkey of the twins ([`subkey`]): most rows are shorter.
const W: usize = 12;

/// A tail move's row, owned, its [`KeyTail`] form kept: the logical key, whether the value came
/// [`KeyTail::Apart`], and the slice.
type Tail = (Vec<u8>, bool, Vec<u8>);

/// What a tail call gave, or the error's text.
type Got = std::result::Result<Option<Tail>, String>;

/// What a plain move gave, or the error's text.
type Plain = std::result::Result<Option<Kv>, String>;

fn tailed(r: Result<Option<(&[u8], KeyTail<'_>)>>) -> Got {
    let own = |(key, tail): (&[u8], KeyTail<'_>)| match tail {
        KeyTail::Joined(j) => (key.to_vec(), false, j.to_vec()),
        KeyTail::Apart(v) => (key.to_vec(), true, v.to_vec()),
    };
    r.map(|e| e.map(own)).map_err(|e| format!("{e:?}"))
}

fn plain(r: Result<Option<(&[u8], &[u8])>>) -> Plain {
    r.map(owned).map_err(|e| format!("{e:?}"))
}

/// A tail row in the model's form ([`joined`]): an apart value joined to the key's last `t` bytes.
fn joined_form((key, apart, bytes): &Tail, t: usize) -> Kv {
    match apart {
        true => joined(key, t, bytes),
        false => (key.clone(), bytes.clone()),
    }
}

/// A move that has a tail twin.
#[derive(Clone, Debug)]
enum Move {
    First,
    Last,
    Next,
    Prev,
    NextInPrefix(usize),
    PrevInPrefix(usize),
    Seek(Vec<u8>),
    SeekDup(Vec<u8>, Vec<u8>),
}

impl Move {
    /// A random move for a cursor at `at` (its logical key) over `rows`: a prefix length from {0,
    /// 1, K-1, K, K+1, L, L+1, anything up to L+1} (`L` the key's length), the seeks'
    /// targets of every length class ([`target`], [`bounded_target`]).
    fn pick(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> Self {
        match rng.below(14) {
            0 => Self::First,
            1 => Self::Last,
            2..=9 => Self::step(rng, at),
            10 | 11 => Self::Seek(target(rng, rows)),
            _ => {
                let (k, s) = bounded_target(rng, rows, at);
                Self::SeekDup(k, s)
            }
        }
    }

    /// A random step: `next`, `prev` or a prefix step either way.
    fn step(rng: &mut Rng, at: Option<&[u8]>) -> Self {
        let l = at.map_or(K + W, <[u8]>::len);
        let mut n = || match rng.below(8) {
            0 => 0,
            1 => 1,
            2 => K - 1,
            3 => K,
            4 => K + 1,
            5 => l,
            6 => l + 1,
            _ => rng.below(l as u64 + 2) as usize,
        };
        match n() % 4 {
            0 => Self::Next,
            1 => Self::Prev,
            2 => Self::NextInPrefix(n()),
            _ => Self::PrevInPrefix(n()),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Last => "last",
            Self::Next => "next",
            Self::Prev => "prev",
            Self::NextInPrefix(_) => "next_in_prefix",
            Self::PrevInPrefix(_) => "prev_in_prefix",
            Self::Seek(_) => "seek",
            Self::SeekDup(..) => "seek_dup",
        }
    }

    /// A step from the cursor's row (not a jump to an edge or a seek).
    const fn is_step(&self) -> bool {
        matches!(self, Self::Next | Self::Prev | Self::NextInPrefix(_) | Self::PrevInPrefix(_))
    }
}

/// The calls of the lockstep on a dup cursor of either kind.
trait Moves {
    /// `mv` as its plain move.
    fn plain(&mut self, mv: &Move) -> Plain;
    /// `mv` as its tail move, of tail `t`.
    fn tail(&mut self, mv: &Move, t: usize) -> Got;
    /// `current_with_key_tail(t)`.
    fn current_tail(&mut self, t: usize) -> Got;
}

impl Moves for DupRoCursor {
    fn plain(&mut self, mv: &Move) -> Plain {
        plain(match mv {
            Move::First => self.first(),
            Move::Last => self.last(),
            Move::Next => self.next(),
            Move::Prev => self.prev(),
            Move::NextInPrefix(n) => self.next_in_prefix(*n),
            Move::PrevInPrefix(n) => self.prev_in_prefix(*n),
            Move::Seek(x) => self.seek(x),
            Move::SeekDup(k, s) => self.seek_dup(k, s),
        })
    }

    fn tail(&mut self, mv: &Move, t: usize) -> Got {
        tailed(match mv {
            Move::First => self.first_with_key_tail(t),
            Move::Last => self.last_with_key_tail(t),
            Move::Next => self.next_with_key_tail(t),
            Move::Prev => self.prev_with_key_tail(t),
            Move::NextInPrefix(n) => self.next_in_prefix_with_key_tail(*n, t),
            Move::PrevInPrefix(n) => self.prev_in_prefix_with_key_tail(*n, t),
            Move::Seek(x) => self.seek_with_key_tail(x, t),
            Move::SeekDup(k, s) => self.seek_dup_with_key_tail(k, s, t),
        })
    }

    fn current_tail(&mut self, t: usize) -> Got {
        tailed(self.current_with_key_tail(t))
    }
}

/// A write transaction's dup cursor with the transaction its moves read.
struct InTxn<'a> {
    c: &'a mut DupTxnCursor,
    txn: &'a RwTxn,
}

impl Moves for InTxn<'_> {
    fn plain(&mut self, mv: &Move) -> Plain {
        let (c, txn) = (&mut *self.c, self.txn);
        plain(match mv {
            Move::First => c.first(txn),
            Move::Last => c.last(txn),
            Move::Next => c.next(txn),
            Move::Prev => c.prev(txn),
            Move::NextInPrefix(n) => c.next_in_prefix(txn, *n),
            Move::PrevInPrefix(n) => c.prev_in_prefix(txn, *n),
            Move::Seek(x) => c.seek(txn, x),
            Move::SeekDup(k, s) => c.seek_dup(txn, k, s),
        })
    }

    fn tail(&mut self, mv: &Move, t: usize) -> Got {
        let (c, txn) = (&mut *self.c, self.txn);
        tailed(match mv {
            Move::First => c.first_with_key_tail(txn, t),
            Move::Last => c.last_with_key_tail(txn, t),
            Move::Next => c.next_with_key_tail(txn, t),
            Move::Prev => c.prev_with_key_tail(txn, t),
            Move::NextInPrefix(n) => c.next_in_prefix_with_key_tail(txn, *n, t),
            Move::PrevInPrefix(n) => c.prev_in_prefix_with_key_tail(txn, *n, t),
            Move::Seek(x) => c.seek_with_key_tail(txn, x, t),
            Move::SeekDup(k, s) => c.seek_dup_with_key_tail(txn, k, s, t),
        })
    }

    fn current_tail(&mut self, t: usize) -> Got {
        tailed(self.c.current_with_key_tail(self.txn, t))
    }
}

/// A tail from {0, 1, W, W+1, K+W+5} or the subkey length `sub` of the row it is for.
fn pick_t(rng: &mut Rng, sub: Option<usize>) -> usize {
    match rng.below(7) {
        0 => 0,
        1 => 1,
        2 => W,
        3 => W + 1,
        4 => K + W + 5,
        _ => sub.unwrap_or(W),
    }
}

/// One move of the lockstep: `b`'s plain move, then (only if it landed) its
/// `current_with_key_tail(t)`, `t` picked for the landed row ([`pick_t`]); `a`'s tail move of the
/// same `t` must give exactly that, the [`KeyTail`] form included, and its row must be the plain
/// move's (key, and the value joined to the key's tail). Returns `a`'s result and `t`.
fn tail_step(
    a: &mut impl Moves,
    b: &mut impl Moves,
    mv: &Move,
    rng: &mut Rng,
    what: &str,
) -> (Got, usize) {
    let moved = b.plain(mv);
    let t = pick_t(rng, moved.as_ref().ok().and_then(|r| r.as_ref()).map(|(k, _)| k.len() - K));
    let want = match &moved {
        Ok(Some(_)) => b.current_tail(t),
        Ok(None) => Ok(None),
        Err(e) => Err(e.clone()),
    };
    let got = a.tail(mv, t);
    assert_eq!(got, want, "{what}: {mv:?} t {t}");
    if let (Ok(Some((key, v))), Ok(Some(row))) = (&moved, &got) {
        assert_eq!(joined_form(row, t), joined(key, t, v), "{what}: {mv:?} t {t}, the move's row");
    }
    (got, t)
}

/// Coverage counts of the lockstep runs, by case.
#[derive(Debug, Default)]
struct Seen(BTreeMap<String, u64>);

impl Seen {
    fn note(&mut self, case: impl Into<String>) {
        *self.0.entry(case.into()).or_default() += 1;
    }

    fn merge(&mut self, other: Self) {
        for (case, n) in other.0 {
            *self.0.entry(case).or_default() += n;
        }
    }

    fn get(&self, case: &str) -> u64 {
        self.0.get(case).copied().unwrap_or(0)
    }

    /// Count tail move `mv` of tail `t`, made from the row `from` (the cursor's logical key before
    /// it), that gave `got`, landing in a packed key (`packed`) or a nested one.
    fn count(&mut self, mv: &Move, from: Option<&[u8]>, got: &Got, t: usize, packed: bool) {
        let kind = mv.kind();
        match got {
            Ok(Some((key, apart, _))) => {
                self.note(format!("{kind} hit"));
                let sub = key.len() - K;
                self.note(if *apart { "apart" } else { "joined" });
                if *apart && t <= sub {
                    self.note("apart inside the subkey (a run)");
                }
                self.note(match t.cmp(&sub) {
                    Ordering::Less => "t < |s|",
                    Ordering::Equal => "t = |s|",
                    Ordering::Greater => "t > |s|",
                });
                self.note(if packed { "packed landing" } else { "nested landing" });
                if mv.is_step() && from.is_some_and(|f| f[..K] != key[..K]) {
                    self.note("key crossing");
                    if matches!(mv, Move::NextInPrefix(_) | Move::PrevInPrefix(_)) {
                        self.note("prefix step crossing a key");
                    }
                }
            }
            Ok(None) if mv.is_step() && from.is_some() => {
                self.note(format!("{kind} none from a row"))
            }
            Ok(None) => self.note(format!("{kind} none")),
            Err(_) => self.note(format!("{kind} refused")),
        }
    }

    /// Every case of the read side's gates seen; packed landings exactly when `bound` packs.
    fn assert_moves(&self, bound: Bound, what: &str) {
        let kinds = [
            "first",
            "last",
            "next",
            "prev",
            "next_in_prefix",
            "prev_in_prefix",
            "seek",
            "seek_dup",
        ];
        let mut gates: Vec<String> = kinds.iter().map(|k| format!("{k} hit")).collect();
        gates.extend(
            ["next", "prev", "next_in_prefix", "prev_in_prefix"]
                .iter()
                .map(|k| format!("{k} none from a row")),
        );
        gates.extend(
            [
                "seek none",
                "seek_dup none",
                "seek_dup refused",
                "joined",
                "apart",
                "apart inside the subkey (a run)",
                "t < |s|",
                "t = |s|",
                "t > |s|",
                "nested landing",
                "key crossing",
                "prefix step crossing a key",
            ]
            .map(String::from),
        );
        if bound == Bound::Unpacked {
            assert_eq!(self.get("packed landing"), 0, "{what}: a table that never packs");
        } else {
            gates.push("packed landing".into());
        }
        self.assert_gates(&gates, what);
    }

    fn assert_gates(&self, gates: &[String], what: &str) {
        let missing: Vec<_> = gates.iter().filter(|g| self.get(g) == 0).collect();
        assert!(missing.is_empty(), "{what}: coverage gates not reached: {missing:?}\n{self:#?}");
    }
}

/// `run` for every seed of `seeds` at every bound, a thread each; each bound's counts merged.
fn on_threads(
    seeds: std::ops::Range<u64>,
    run: impl Fn(Bound, u64) -> Seen + Sync,
) -> Vec<(Bound, Seen)> {
    std::thread::scope(|scope| {
        let runs: Vec<_> = (Bound::ALL.iter())
            .flat_map(|&b| seeds.clone().map(move |seed| (b, seed)))
            .map(|(b, seed)| {
                let run = &run;
                (b, scope.spawn(move || run(b, seed)))
            })
            .collect();
        let mut merged: Vec<(Bound, Seen)> =
            Bound::ALL.iter().map(|&b| (b, Seen::default())).collect();
        for (b, run) in runs {
            let seen = run.join().unwrap_or_else(|e| std::panic::resume_unwind(e));
            merged.iter_mut().find(|(m, _)| *m == b).expect("a bound").1.merge(seen);
        }
        merged
    })
}

/// One read lockstep run: twins of `shape` at `bound` (nested trees of `height` levels or more),
/// `ops` calls on two [`DupRoCursor`]s over `d`, now and then both replaced by new ones. A tenth of
/// the calls place both by a plain `seek_exact` of a row or `seek_key`; the rest are tail moves
/// ([`tail_step`]). After each call the two have the same state, the same leaves and the same
/// `current_with_key_tail` for a tail from [`pick_t`].
fn ro_run(shape: &Shape, height: u32, bound: Bound, seed: u64, ops: usize) -> Seen {
    let mut seen = Seen::default();
    let seed = 0x2b2b_0000_0000 + seed;
    let _report = ReportSeed(seed);
    let mut rng = Rng(seed);
    let (_dir, path) = temp("tn_lmdb_dup_key_tail_ro");
    let env = open(&path, SyncMode::NoSync);
    let (d, _, rows) = twins(&env, shape, &mut rng, bound);
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let stat = ro.table_stat(&d).expect("stat");
    assert!(stat.height >= 2 && stat.overflow_pages > 0, "{stat:?}");
    let (packed, tallest) = forms(&env, &d, bound);
    assert!(tallest >= Some(height), "the tallest nested tree: {tallest:?}");
    let rows: Vec<Kv> = rows.into_iter().collect();
    let fresh = || ro.dup_cursor_owned(&d).expect("dup cursor");
    let (mut a, mut b) = (fresh(), fresh());
    for step in 0..ops {
        if rng.below(300) == 0 {
            (a, b) = (fresh(), fresh());
        }
        let from = b.current().expect("current").map(|(k, _)| k.to_vec());
        let what = format!("step {step} from {from:02x?}");
        if rng.below(10) == 0 {
            if rng.below(2) == 0 {
                let key = pick_row(&mut rng, &rows).to_vec();
                let place = |c: &mut DupRoCursor| c.seek_exact(&key).map(|v| v.map(<[u8]>::to_vec));
                assert_eq!(
                    format!("{:?}", place(&mut a)),
                    format!("{:?}", place(&mut b)),
                    "{what}"
                );
            } else {
                let k = bounded_target(&mut rng, &rows, from.as_deref()).0;
                assert_eq!(plain(a.seek_key(&k)), plain(b.seek_key(&k)), "{what}");
            }
        } else {
            let mv = Move::pick(&mut rng, &rows, from.as_deref());
            let (got, t) = tail_step(&mut a, &mut b, &mv, &mut rng, &what);
            let landed = got.as_ref().ok().and_then(|r| r.as_ref());
            let packed = landed.is_some_and(|(key, _, _)| packed.contains(&key[..K]));
            seen.count(&mv, from.as_deref(), &got, t, packed);
        }
        assert_eq!(a.state(), b.state(), "{what}");
        assert_eq!(a.leaf_bounds_for_test(), b.leaf_bounds_for_test(), "{what}");
        let sub = b.current().expect("current").map(|(k, _)| k.len() - K);
        let t2 = pick_t(&mut rng, sub);
        assert_eq!(a.current_tail(t2), b.current_tail(t2), "{what}: current t {t2}");
    }
    assert!(env.failure().is_none(), "no call latched: {:?}", env.failure());
    seen
}

/// [`ro_run`] over `seeds` at every bound, every bound's gates asserted.
fn ro_lockstep(shape: &Shape, height: u32, seeds: std::ops::Range<u64>, ops: usize) {
    for (bound, seen) in on_threads(seeds, |bound, seed| ro_run(shape, height, bound, seed, ops)) {
        seen.assert_moves(bound, &format!("read lockstep at {bound:?}"));
    }
}

/// Each tail move of a [`DupRoCursor`] gives exactly what its plain move followed by
/// `current_with_key_tail(t)` gives (`None` when the plain move gives `None`), its [`KeyTail`]
/// form included, and leaves the cursor where the plain move leaves it, over twins at every
/// packing bound: outer and nested trees of two levels or more, values inline and in runs,
/// subkeys of 1 to [`W`] bytes, `t` in {0, 1, W, W+1, K+W+5, the landed row's subkey length}.
/// The gates: every tail move kind landing, the steps' ends and prefix ends from a row, seek and
/// `seek_dup` misses and refusals, joined and apart tails, `t` below, at and past the subkey,
/// packed (where the table packs) and nested landings, key crossings by steps and prefix steps.
#[test]
fn dup_tail_moves_match_the_move_then_current_with_key_tail() {
    ro_lockstep(&SMALL_SHAPE, 2, 0..3, 4000);
}

/// [`dup_tail_moves_match_the_move_then_current_with_key_tail`] over more seeds and calls and
/// nested trees of three levels.
#[test]
#[ignore = "heavy: run with --release"]
fn dup_tail_moves_match_the_move_then_current_with_key_tail_heavy() {
    ro_lockstep(&SMALL_SHAPE, 2, 0..16, 20_000);
    ro_lockstep(&HEAVY_SHAPE, 3, 0..4, 20_000);
}

/// A write between the write lockstep's moves.
#[derive(Clone, Debug)]
enum Write {
    /// By the stepped cursor itself.
    CursorPut(Vec<u8>, Vec<u8>),
    CursorDelete,
    /// By another cursor on the table: a put, and a seek to a row then its `delete_current`.
    OtherPut(Vec<u8>, Vec<u8>),
    OtherDelete(Vec<u8>),
    /// By the transaction.
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeletePrefix(Vec<u8>),
}

impl Write {
    /// A random write near the cursor's row `at` (its row, another row of its key, a new subkey
    /// of its key) or anywhere in `rows`; a tenth of the values in runs.
    fn pick(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> Self {
        let r = match at {
            Some(at) if rng.below(2) == 0 => at.to_vec(),
            _ => pick_row(rng, rows).to_vec(),
        };
        let key = match rng.below(3) {
            0 => [&r[..K], &subkey(rng)].concat(),
            _ => r,
        };
        let v = value(rng, 10);
        match rng.below(32) {
            0..=5 => Self::CursorPut(key, v),
            6..=12 => Self::CursorDelete,
            13..=16 => Self::OtherPut(key, v),
            17..=21 => Self::OtherDelete(key),
            22..=25 => Self::Put(key, v),
            26..=30 => Self::Delete(key),
            _ => Self::DeletePrefix(key[..K].to_vec()),
        }
    }

    /// Apply the write in `txn` to `d`, `c` the stepped cursor and `other` the other; the
    /// result's text.
    fn apply(
        &self,
        txn: &mut RwTxn,
        d: &Table,
        c: &mut DupTxnCursor,
        other: &mut DupTxnCursor,
    ) -> String {
        let r: Result<u64> = match self {
            Self::CursorPut(key, v) => c.put(txn, key, v).map(|()| 0),
            Self::CursorDelete => c.delete_current(txn).map(u64::from),
            Self::OtherPut(key, v) => other.put(txn, key, v).map(|()| 0),
            Self::OtherDelete(key) => match other.seek_exact(txn, key).map(|v| v.is_some()) {
                Ok(true) => other.delete_current(txn).map(u64::from),
                found => found.map(|_| 2),
            },
            Self::Put(key, v) => txn.put(d, key, v).map(|()| 0),
            Self::Delete(key) => txn.delete(d, key).map(u64::from),
            Self::DeletePrefix(k) => txn.delete_prefix(d, k),
        };
        format!("{r:?}")
    }

    fn case(&self) -> &'static str {
        match self {
            Self::CursorPut(..) => "cursor put",
            Self::CursorDelete => "cursor delete",
            Self::OtherPut(..) | Self::OtherDelete(_) => "another cursor's write",
            Self::Put(..) | Self::Delete(_) | Self::DeletePrefix(_) => "the transaction's write",
        }
    }

    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::CursorPut(key, _) | Self::OtherPut(key, _) | Self::Put(key, _) => Some(key),
            _ => None,
        }
    }
}

/// One write lockstep run: twins of `shape` at `bound` built alike in two environments,
/// `rounds` write transactions of `steps` steps each, every round committed. In each round a
/// cursor `a` (tail moves) over the first environment's `d` and `b` (plain moves then
/// `current_with_key_tail`) over the second's, and another cursor in each; two steps in five are
/// the same write in both ([`Write`]), the rest moves ([`tail_step`]; in the deleted state
/// mostly steps). After each step the two cursors' positions ([`DupTxnCursor::position`]), states,
/// reseeks and delete landings are equal; after a move outside the deleted state, their
/// `current_with_key_tail` too. Nothing between a write and the next move syncs a cursor, so the
/// move itself meets the staleness.
fn txn_run(shape: &Shape, bound: Bound, seed: u64, rounds: usize, steps: usize) -> Seen {
    let mut seen = Seen::default();
    let seed = 0x2b2b_1000_0000 + seed;
    let _report = ReportSeed(seed);
    let ((_dir1, path1), (_dir2, path2)) =
        (temp("tn_lmdb_dup_key_tail_txn"), temp("tn_lmdb_dup_key_tail_txn"));
    let (env1, env2) = (open(&path1, SyncMode::NoSync), open(&path2, SyncMode::NoSync));
    let (d1, _, rows) = twins(&env1, shape, &mut Rng(seed), bound);
    let (d2, _, rows2) = twins(&env2, shape, &mut Rng(seed), bound);
    assert!(rows == rows2, "the same rows in both environments");
    let mut rows: Vec<Kv> = rows.into_iter().collect();
    let mut rng = Rng(seed ^ 0x5bd1_e995);
    for round in 0..rounds {
        let (mut txn1, mut txn2) = (env1.begin_rw().expect("rw 1"), env2.begin_rw().expect("rw 2"));
        let mut a = txn1.dup_cursor_detached(&d1).expect("a");
        let mut b = txn2.dup_cursor_detached(&d2).expect("b");
        let mut o1 = txn1.dup_cursor_detached(&d1).expect("other 1");
        let mut o2 = txn2.dup_cursor_detached(&d2).expect("other 2");
        for step in 0..steps {
            let from = b.key_for_test().map(<[u8]>::to_vec);
            let deleted = b.state().1;
            let what = format!("round {round} step {step} from {from:02x?} deleted {deleted}");
            if !deleted && rng.below(5) < 2 {
                let w = Write::pick(&mut rng, &rows, from.as_deref());
                let r1 = w.apply(&mut txn1, &d1, &mut a, &mut o1);
                let r2 = w.apply(&mut txn2, &d2, &mut b, &mut o2);
                assert_eq!(r1, r2, "{what}: {w:?}");
                seen.note(w.case());
                if let Some(key) = w.key() {
                    rows.push((key.to_vec(), Vec::new()));
                }
            } else {
                let mv = match deleted && rng.below(8) != 0 {
                    true => Move::step(&mut rng, from.as_deref()),
                    false => Move::pick(&mut rng, &rows, from.as_deref()),
                };
                let stale = !a.position(&txn1).4;
                let reseeks = a.reseeks;
                let (got, t) = tail_step(
                    &mut InTxn { c: &mut a, txn: &txn1 },
                    &mut InTxn { c: &mut b, txn: &txn2 },
                    &mv,
                    &mut rng,
                    &what,
                );
                let packed = matches!(a.position(&txn1).1, Some((_, InKey::Packed { .. })));
                seen.count(&mv, from.as_deref(), &got, t, packed);
                if stale {
                    seen.note("a tail move after another writer's change");
                }
                if a.reseeks > reseeks {
                    seen.note("a reseek in a tail move");
                }
                if deleted {
                    seen.note(format!("{} in the deleted state", mv.kind()));
                }
                if !b.state().1 {
                    let sub = b.key_for_test().map(|k| k.len() - K);
                    let t2 = pick_t(&mut rng, sub);
                    let a2 = InTxn { c: &mut a, txn: &txn1 }.current_tail(t2);
                    let b2 = InTxn { c: &mut b, txn: &txn2 }.current_tail(t2);
                    assert_eq!(a2, b2, "{what}: {mv:?}, current t {t2}");
                }
            }
            assert_eq!(a.position(&txn1), b.position(&txn2), "{what}");
            assert_eq!(a.state(), b.state(), "{what}");
            assert_eq!(a.deleted_key_for_test(), b.deleted_key_for_test(), "{what}");
            assert_eq!((a.reseeks, a.landings), (b.reseeks, b.landings), "{what}");
            assert_eq!(o1.position(&txn1), o2.position(&txn2), "{what}: the other cursors");
        }
        drop((a, b, o1, o2));
        txn1.commit().expect("commit 1");
        txn2.commit().expect("commit 2");
        assert!(env1.failure().is_none() && env2.failure().is_none(), "no call latched");
    }
    seen
}

/// [`txn_run`] over `seeds` at every bound, every bound's gates asserted: the read side's
/// ([`Seen::assert_moves`]) and the writes', each kind of write seen, tail moves after another
/// writer's change that reseeked, and each step in the deleted state.
fn txn_lockstep(shape: &Shape, seeds: std::ops::Range<u64>, rounds: usize, steps: usize) {
    let runs = on_threads(seeds, |bound, seed| txn_run(shape, bound, seed, rounds, steps));
    for (bound, seen) in runs {
        let what = format!("write lockstep at {bound:?}");
        seen.assert_moves(bound, &what);
        let gates = [
            "cursor put",
            "cursor delete",
            "another cursor's write",
            "the transaction's write",
            "a tail move after another writer's change",
            "a reseek in a tail move",
            "next in the deleted state",
            "prev in the deleted state",
            "next_in_prefix in the deleted state",
            "prev_in_prefix in the deleted state",
        ];
        seen.assert_gates(&gates.map(String::from), &what);
    }
}

/// [`dup_tail_moves_match_the_move_then_current_with_key_tail`] for a [`DupTxnCursor`]: two
/// environments of the same rows, the same writes between the moves in both (by the stepped
/// cursor, so its deleted state and the follower paths are met; by another cursor on the table,
/// so the moves meet staleness and reseek; by the transaction), every result and position
/// compared ([`txn_run`]).
#[test]
fn dup_txn_tail_moves_match_the_move_then_current_with_key_tail() {
    txn_lockstep(&SMALL_SHAPE, 0..2, 3, 1000);
}

/// [`dup_txn_tail_moves_match_the_move_then_current_with_key_tail`] over more seeds and steps
/// and nested trees of three levels.
#[test]
#[ignore = "heavy: run with --release"]
fn dup_txn_tail_moves_match_the_move_then_current_with_key_tail_heavy() {
    txn_lockstep(&SMALL_SHAPE, 0..8, 4, 4000);
    txn_lockstep(&HEAVY_SHAPE, 0..2, 3, 4000);
}

/// What a damage probe left: the final move's result (a plain move's row in the tail form,
/// joined), the cursor's state and the latched failure's text.
type Outcome<S> = (Got, S, Option<String>);

/// The final move `mv` of a damage probe on `c`, through its tail twin (`tail`) or plain.
fn final_move(c: &mut impl Moves, mv: &Move, tail: bool) -> Got {
    match tail {
        true => c.tail(mv, 2),
        false => c.plain(mv).map(|r| r.map(|(k, v)| (k, false, v))),
    }
}

/// The plain and the tail outcome of a damage probe are the same `Corrupt` (its text containing
/// one of `messages`), the cursor in `unset`, the environment latched.
fn same_damage<S: PartialEq + std::fmt::Debug>(
    plain: &Outcome<S>,
    tail: &Outcome<S>,
    messages: &[&str],
    unset: &S,
    what: &str,
) {
    assert_eq!(plain, tail, "{what}: the tail move meets the damage as the plain move");
    let corrupt = matches!(&plain.0, Err(e) if e.starts_with("Corrupt(") && messages.iter().any(|m| e.contains(m)));
    assert!(corrupt, "{what}: {:?}", plain.0);
    assert_eq!(&plain.1, unset, "{what}");
    assert!(plain.2.is_some(), "{what}: latched");
}

/// A probe of packed damage: a seek of a row of `packed_fixture`'s set, or a `seek_exact` of a
/// row then `steps - 1` plain steps forward (`true`) or back, before the final move.
#[derive(Clone, Copy, Debug)]
enum Probe {
    Seek(&'static [u8]),
    Step(u32, &'static [u8], bool, usize),
}

impl Probe {
    /// The final moves of the probe: a seek's `seek` and `seek_dup`; a step's plain step and
    /// prefix step of `K - 1` bytes (every outer key near the set shares them).
    fn finals(self, pk: u32) -> [Move; 2] {
        match self {
            Self::Seek(s) => {
                [Move::Seek(dup_key(pk, s)), Move::SeekDup(pk.to_be_bytes().to_vec(), s.to_vec())]
            }
            Self::Step(_, _, true, _) => [Move::Next, Move::NextInPrefix(K - 1)],
            Self::Step(_, _, false, _) => [Move::Prev, Move::PrevInPrefix(K - 1)],
        }
    }
}

/// Damage met by the tail moves, each beside its plain move on a fresh copy of the damaged file,
/// the same script up to the final move: the same `Corrupt` text, the same state (unset, out of
/// the deleted state) and a latched environment.
/// - A [`DupRoCursor`] on the packed damage of `packed_damage_met_by_a_dup_read_cursor_is_corrupt`
///   (its cases and probes), each seek probe as `seek` and `seek_dup`, each step probe as a step
///   and a prefix step.
/// - A [`DupTxnCursor`] on the nested damage of `damage_under_a_dup_cursor_is_corrupt` (a nested
///   cell in a nested leaf, an outer key of another width, an empty subkey, a misrouted separator):
///   `seek`, `seek_dup` of the damaged row, `next` and a prefix step from the row before; and on
///   the packed damage of `packed_damage_under_a_dup_cursor_is_corrupt` (an empty subkey, two rows
///   swapped), with its steps forward and, for the empty subkey, back.
#[test]
fn tail_moves_meet_damage_as_their_plain_moves() {
    use super::packed_damage::{cases, in_set, packed_fixture, set16, slen_at, sub, swap_1_2, PK};
    let fx = packed_fixture();
    let mut kinds: BTreeMap<(&str, &str), u32> = BTreeMap::new();

    // A read cursor on packed damage.
    let packed_cases = cases(&fx);
    const R3: &[u8] = &[0, 0, 0, 3];
    let probes: [(&str, Probe); 13] = [
        ("decreasing end", Probe::Seek(R3)),
        ("decreasing end", Probe::Step(PK - 1, &[1], true, 4)),
        ("empty subkey", Probe::Seek(R3)),
        ("empty subkey", Probe::Step(PK + 3, &[0], false, 3)),
        ("subkey past its row", Probe::Seek(R3)),
        ("subkey past its row", Probe::Step(PK - 1, &[1], true, 4)),
        ("empty set", Probe::Seek(&[0, 0, 0, 0])),
        ("short blob", Probe::Step(PK - 1, &[1], true, 1)),
        ("over packed_max", Probe::Seek(&[0, 0, 0, 0])),
        ("tiling short", Probe::Step(PK + 3, &[0], false, 1)),
        ("misordered", Probe::Step(PK, &[0, 0, 0, 0], true, 2)),
        ("misordered", Probe::Step(PK, &[0, 0, 0, 3], false, 2)),
        ("duplicate subkey", Probe::Step(PK, &[0, 0, 0, 1], true, 1)),
    ];
    for (name, probe) in probes {
        let (_, _, _, _, damage) = packed_cases.iter().find(|c| c.0 == name).expect("a case");
        for mv in probe.finals(PK) {
            let what = format!("read {name} {probe:?} {mv:?}");
            let outcome = |tail: bool| -> Outcome<State> {
                let path = fx.dir.path().join("case");
                fs::copy(&fx.path, &path).expect("copy");
                damage(&path);
                let env = open(&path, SyncMode::Durable);
                let ro = Arc::new(env.begin_ro().expect("ro"));
                let d = ro.open_table("d").expect("open").expect("d");
                let mut c = ro.dup_cursor_owned(&d).expect("cursor");
                if let Probe::Step(k, s, forward, steps) = probe {
                    assert!(c.seek_exact(&dup_key(k, s)).expect("the start").is_some(), "{what}");
                    for _ in 1..steps {
                        let r = if forward { c.next() } else { c.prev() };
                        assert!(r.expect("an undamaged step").is_some(), "{what}");
                    }
                }
                let got = final_move(&mut c, &mv, tail);
                let out = (got, c.state(), env.failure().map(|f| format!("{f:?}")));
                drop((c, ro, env));
                fs::remove_file(&path).expect("remove");
                out
            };
            same_damage(&outcome(false), &outcome(true), &["dup table d"], &State::Unset, &what);
            *kinds.entry(("read", mv.kind())).or_default() += 1;
        }
    }

    // A write cursor on nested damage.
    let dir = TempDir::with_prefix("tn_lmdb_dup_key_tail_damage").expect("temp dir");
    let forged =
        TreeRoot { root: 9, root_txn: 7, height: 1, entries: 3, leaf_pages: 1, ..TreeRoot::EMPTY };
    let (cold, hot_last) = (COLD.to_be_bytes(), dup_key(HOT, b"big"));
    type Forge = Box<dyn Fn(&std::path::Path, &BTreeMap<u32, (u32, TreeRoot)>)>;
    let nested: [(&str, &[&str], Forge); 4] = [
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
    let txn_outcome =
        |path: &std::path::Path, start: Option<(&[u8], usize, bool)>, mv: &Move, tail| {
            let (_dir, env) = copy_of(path);
            let txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            let mut c = txn.dup_cursor_detached(&d).expect("cursor");
            if let Some((from, steps, forward)) = start {
                assert!(c.seek_exact(&txn, from).expect("the start").is_some(), "{from:02x?}");
                for _ in 1..steps {
                    let r = if forward { c.next(&txn) } else { c.prev(&txn) };
                    assert!(r.expect("an undamaged step").is_some(), "{from:02x?}");
                }
            }
            let got = final_move(&mut InTxn { c: &mut c, txn: &txn }, mv, tail);
            let out: Outcome<(State, bool)> =
                (got, c.state(), env.failure().map(|f| format!("{f:?}")));
            out
        };
    for (i, (what, messages, forge)) in nested.into_iter().enumerate() {
        let sub_dir = dir.path().join(i.to_string());
        fs::create_dir(&sub_dir).expect("a case's dir");
        let (path, model) = damaged(&sub_dir, |path, cells| forge(path, cells));
        // The damaged row and the row before it, as damage_under_a_dup_cursor_is_corrupt finds
        // them.
        let (target, before) = match i {
            0 => (dup_key(COLD, &[1]), dup_key(COLD, &[0])),
            1 | 2 => (cold.to_vec(), hot_last.clone()),
            _ => {
                let (_dir, env) = copy_of(&path);
                let ro = env.begin_ro().expect("ro");
                let d = ro.open_table("d").expect("open").expect("d");
                let missed = (model.keys())
                    .find(|key| ro.get(&d, key).expect("get").is_none())
                    .expect("a misrouted row")
                    .clone();
                let before = model.range(..missed.clone()).next_back().expect("a row before");
                (missed, before.0.clone())
            }
        };
        let finals = [
            (None, Move::Seek(target.clone())),
            (None, Move::SeekDup(target[..K].to_vec(), target[K..].to_vec())),
            (Some((&before[..], 1, true)), Move::Next),
            (Some((&before[..], 1, true)), Move::NextInPrefix(K - 1)),
        ];
        for (start, mv) in finals {
            let what = format!("write {what} {mv:?}");
            let (plain, tail) =
                (txn_outcome(&path, start, &mv, false), txn_outcome(&path, start, &mv, true));
            same_damage(&plain, &tail, messages, &(State::Unset, false), &what);
            *kinds.entry(("write", mv.kind())).or_default() += 1;
        }
    }

    // A write cursor on packed damage: a case's forge, the row a seek targets, the row the
    // steps forward start from and how many meet the damage.
    type PackedCase = (&'static str, fn(&mut [u8]), Option<u32>, Vec<u8>, usize);
    let packed: [PackedCase; 2] = [
        ("an empty subkey", |b| set16(b, slen_at(3), 0), Some(3), dup_key(PK - 1, &[1]), 4),
        ("misordered", swap_1_2, None, dup_key(PK, &sub(0)), 2),
    ];
    for (what, forge, seek, from, steps) in packed {
        let path = fx.dir.path().join(what.replace(' ', "_"));
        fs::copy(&fx.path, &path).expect("copy");
        in_set(&path, fx.leaves[&PK], PK, forge);
        let back = dup_key(PK + 3, &[0]);
        let mut finals = vec![
            (Some((&from[..], steps, true)), Move::Next),
            (Some((&from[..], steps, true)), Move::NextInPrefix(K - 1)),
        ];
        if let Some(s) = seek {
            let row = sub(s);
            finals.push((None, Move::Seek(dup_key(PK, &row))));
            finals.push((None, Move::SeekDup(PK.to_be_bytes().to_vec(), row.to_vec())));
            finals.push((Some((&back[..], 3, false)), Move::Prev));
            finals.push((Some((&back[..], 3, false)), Move::PrevInPrefix(K - 1)));
        }
        for (start, mv) in finals {
            let what = format!("write packed {what} {mv:?}");
            let (plain, tail) =
                (txn_outcome(&path, start, &mv, false), txn_outcome(&path, start, &mv, true));
            same_damage(&plain, &tail, &["dup table d"], &(State::Unset, false), &what);
            *kinds.entry(("write", mv.kind())).or_default() += 1;
        }
    }

    for side in ["read", "write"] {
        for kind in ["next", "prev", "next_in_prefix", "prev_in_prefix", "seek", "seek_dup"] {
            assert!(kinds.get(&(side, kind)).is_some_and(|n| *n > 0), "{side} {kind}: {kinds:?}");
        }
    }
}
