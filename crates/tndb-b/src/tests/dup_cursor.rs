//! Dup cursors against the composite twin: a dup table `d` (key width 4) and a plain table `p`
//! keyed `k ‖ s` holding the same rows, a [`DupRoCursor`] over `d` and a [`RoCursor`] over `p`
//! side by side, every call compared (Oracle 2a); the bounded seeks' reach, the key tail's two
//! forms, the outer key width and the subkey length met by every landing, and the kind refusal.

use std::{collections::BTreeMap, sync::Arc};

use tempfile::TempDir;

use super::{
    damage::{read_page, restamped, tree, write_page},
    dup::{copy_of, damaged, dup_key, restamp_record, COLD, HOT, OPTIONS},
    open, temp, Rows,
};
use crate::{
    env::SyncMode,
    page,
    tree::{
        cursor::{
            tests::{owned, owned_tail, root_descents, Kv},
            KeyTail, State,
        },
        dup_cursor::LeafBounds,
        test_harness::{walk_outer_tree, ReportSeed, Rng},
        TreeRoot,
    },
    DupRoCursor, DupTxnCursor, Env, Error, ReadTxn as _, Result, RoCursor, RwTxn, Table,
};

/// The key width of the dup tables here ([`OPTIONS`]).
const K: usize = OPTIONS.key_len as usize;

/// The shape of a twin pair's rows: `keys` outer keys, the first `big_keys` of them with about
/// `big_rows` rows each (nested trees of `nested_height` levels or more), the others with 1 to
/// 5; `run_pct` percent of the values in overflow runs.
struct Shape {
    keys: usize,
    big_keys: usize,
    big_rows: usize,
    run_pct: u64,
    nested_height: u32,
}

/// A byte from a small alphabet most of the time, so neighbouring keys share leading bytes.
fn clustered(rng: &mut Rng) -> u8 {
    const BYTES: [u8; 4] = [0x00, 0x01, 0x7f, 0xfe];
    if rng.below(4) == 0 {
        rng.next() as u8
    } else {
        BYTES[rng.below(4) as usize]
    }
}

/// An outer key whose neighbours share 0 to `K - 1` leading bytes with it, at random.
fn outer_key(rng: &mut Rng) -> Vec<u8> {
    (0..K).map(|_| clustered(rng)).collect()
}

/// A subkey of 1 to 12 bytes.
fn subkey(rng: &mut Rng) -> Vec<u8> {
    (0..1 + rng.below(12)).map(|_| clustered(rng)).collect()
}

/// A value of up to 40 bytes, or a run's (5000 bytes) `run_pct` percent of the time.
fn value(rng: &mut Rng, run_pct: u64) -> Vec<u8> {
    if rng.below(100) < run_pct {
        return vec![rng.next() as u8; 5000];
    }
    (0..rng.below(41)).map(|_| rng.next() as u8).collect()
}

/// Dup table `d` and its composite twin `p` in `env`, committed with the same rows of `shape`.
fn twins(env: &Env, shape: &Shape, rng: &mut Rng) -> (Table, Table, Rows) {
    let mut rows = Rows::new();
    for i in 0..shape.keys {
        let k = outer_key(rng);
        let n = if i < shape.big_keys { shape.big_rows } else { 1 + rng.len_upto(4) };
        for _ in 0..n {
            rows.insert([&k[..], &subkey(rng)].concat(), value(rng, shape.run_pct));
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

/// One call of the twin tests.
#[derive(Clone, Debug)]
enum Op {
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
}

/// A random row's logical key.
fn pick_row<'r>(rng: &mut Rng, rows: &'r [Kv]) -> &'r [u8] {
    &rows[rng.below(rows.len() as u64) as usize].0
}

/// A seek target of every length class: a row's key, cut short of the key width, cut to it, cut
/// inside the subkey, just after or just before a row's key, the empty key, a key after every
/// key, or a random one.
fn target(rng: &mut Rng, rows: &[Kv]) -> Vec<u8> {
    let r = pick_row(rng, rows).to_vec();
    shaped(rng, r)
}

/// [`target`]'s length classes made from the row key `r`.
fn shaped(rng: &mut Rng, r: Vec<u8>) -> Vec<u8> {
    match rng.below(9) {
        0 => r,
        1 => r[..rng.below(K as u64) as usize].to_vec(),
        2 => r[..K].to_vec(),
        3 => r[..K + 1 + rng.below((r.len() - K) as u64) as usize].to_vec(),
        4 => [&r[..], &[0]].concat(),
        5 => {
            let mut r = r;
            match r.last_mut() {
                Some(b) if *b > 0 => {
                    *b -= 1;
                    r.push(0xFF);
                }
                _ => {
                    r.pop();
                }
            }
            r
        }
        6 => Vec::new(),
        7 => vec![0xFF; 16],
        _ => (0..1 + rng.below(16)).map(|_| rng.next() as u8).collect(),
    }
}

/// A bounded seek's outer key and subkey: mostly a row's own (or the current row's), sometimes a
/// neighbour of one, a random key, or a key of another width; the subkey the row's, just after
/// it, past every subkey, empty or random.
fn bounded_target(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> (Vec<u8>, Vec<u8>) {
    let r = match at {
        Some(at) if rng.below(2) == 0 => at,
        _ => pick_row(rng, rows),
    };
    let k = match rng.below(16) {
        0 => (0..K).map(|_| rng.next() as u8).collect(),
        1 => {
            let mut k = r[..K].to_vec();
            k[K - 1] = k[K - 1].wrapping_add(1);
            k
        }
        2 => r[..K - 1].to_vec(),
        3 => [&r[..K], &[0]].concat(),
        _ => r[..K].to_vec(),
    };
    let s = match rng.below(7) {
        0 => Vec::new(),
        1 => [&r[K..], &[0]].concat(),
        2 => vec![0xFF; 13],
        3 => subkey(rng),
        _ => r[K..].to_vec(),
    };
    (k, s)
}

impl Op {
    /// A random call for a cursor at `at` (its logical key) over `rows`: a prefix length from
    /// {0, 1, K-1, K, K+1, L, L+1, anything up to L+1}, a tail from {0, 1, |s|, |s|+1, L, past
    /// every key}.
    fn pick(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> Self {
        let l = at.map_or(K + 4, <[u8]>::len);
        let tail = |rng: &mut Rng| match rng.below(6) {
            0 => 0,
            1 => 1,
            2 => l - K,
            3 => l - K + 1,
            4 => l,
            _ => usize::MAX,
        };
        let n = |rng: &mut Rng| match rng.below(8) {
            0 => 0,
            1 => 1,
            2 => K - 1,
            3 => K,
            4 => K + 1,
            5 => l,
            6 => l + 1,
            _ => rng.below(l as u64 + 2) as usize,
        };
        match rng.below(18) {
            0 => Self::First,
            1 => Self::Last,
            2..=4 => Self::Next,
            5 | 6 => Self::Prev,
            7 => Self::Current,
            8 => Self::CurrentWithKeyTail(tail(rng)),
            9 => Self::Seek(target(rng, rows)),
            10 => Self::SeekExact(target(rng, rows)),
            11 => Self::SeekWithKeyTail(target(rng, rows), tail(rng)),
            12 => Self::NextInPrefix(n(rng)),
            13 => Self::PrevInPrefix(n(rng)),
            14 => Self::SeekKey(bounded_target(rng, rows, at).0),
            15 | 16 => {
                let (k, s) = bounded_target(rng, rows, at);
                Self::SeekDup(k, s)
            }
            _ => {
                let (k, s) = bounded_target(rng, rows, at);
                Self::SeekDupWithKeyTail(k, s, tail(rng))
            }
        }
    }
}

/// What a call gave: the row (a tail in `owned_tail`'s joined form), or the error's text.
type Got = std::result::Result<Option<Kv>, String>;

/// `op` on the dup cursor.
fn on_dup(c: &mut DupRoCursor, op: &Op) -> Got {
    let got: Result<Option<Kv>> = match op {
        Op::First => c.first().map(owned),
        Op::Last => c.last().map(owned),
        Op::Next => c.next().map(owned),
        Op::Prev => c.prev().map(owned),
        Op::Current => c.current().map(owned),
        Op::CurrentWithKeyTail(t) => c.current_with_key_tail(*t).map(|e| owned_tail(e, *t)),
        Op::Seek(x) => c.seek(x).map(owned),
        Op::SeekExact(x) => c.seek_exact(x).map(|v| v.map(|v| (x.clone(), v.to_vec()))),
        Op::SeekWithKeyTail(x, t) => c.seek_with_key_tail(x, *t).map(|e| owned_tail(e, *t)),
        Op::NextInPrefix(n) => c.next_in_prefix(*n).map(owned),
        Op::PrevInPrefix(n) => c.prev_in_prefix(*n).map(owned),
        Op::SeekKey(k) => c.seek_key(k).map(owned),
        Op::SeekDup(k, s) => c.seek_dup(k, s).map(owned),
        Op::SeekDupWithKeyTail(k, s, t) => {
            c.seek_dup_with_key_tail(k, s, *t).map(|e| owned_tail(e, *t))
        }
    };
    got.map_err(|e| format!("{e:?}"))
}

/// The width refusal a bounded seek of outer key `k` makes, if any.
fn width_refusal(k: &[u8]) -> Option<Error> {
    match k.len() {
        n if n < K => Some(Error::KeyTooShort { len: n, min: K }),
        n if n > K => Some(Error::KeyTooLarge { len: n, max: K }),
        _ => None,
    }
}

/// `op` on the twin's cursor; a bounded seek as a seek kept only on a row of its outer key, else
/// the cursor unset (a `seek_exact` of the outer key alone, which no twin key equals).
fn on_twin(c: &mut RoCursor, op: &Op) -> Got {
    let bounded = |c: &mut RoCursor, k: &[u8], found: Option<Kv>| -> Result<Option<Kv>> {
        match found {
            Some(row) if row.0.starts_with(k) => Ok(Some(row)),
            _ => {
                assert_eq!(c.seek_exact(k)?, None, "no twin key is an outer key alone");
                Ok(None)
            }
        }
    };
    let got: Result<Option<Kv>> = match op {
        Op::First => c.first().map(owned),
        Op::Last => c.last().map(owned),
        Op::Next => c.next().map(owned),
        Op::Prev => c.prev().map(owned),
        Op::Current => c.current().map(owned),
        Op::CurrentWithKeyTail(t) => c.current_with_key_tail(*t).map(|e| owned_tail(e, *t)),
        Op::Seek(x) => c.seek(x).map(owned),
        Op::SeekExact(x) => c.seek_exact(x).map(|v| v.map(|v| (x.clone(), v.to_vec()))),
        Op::SeekWithKeyTail(x, t) => c.seek_with_key_tail(x, *t).map(|e| owned_tail(e, *t)),
        Op::NextInPrefix(n) => c.next_in_prefix(*n).map(owned),
        Op::PrevInPrefix(n) => c.prev_in_prefix(*n).map(owned),
        Op::SeekKey(k) | Op::SeekDup(k, _) | Op::SeekDupWithKeyTail(k, _, _)
            if width_refusal(k).is_some() =>
        {
            Err(width_refusal(k).expect("a refusal"))
        }
        Op::SeekKey(k) => c.seek(k).map(owned).and_then(|f| bounded(c, k, f)),
        Op::SeekDup(k, s) => {
            c.seek(&[&k[..], s].concat()).map(owned).and_then(|f| bounded(c, k, f))
        }
        Op::SeekDupWithKeyTail(k, s, t) => c
            .seek_with_key_tail(&[&k[..], s].concat(), *t)
            .map(|e| owned_tail(e, *t))
            .and_then(|f| bounded(c, k, f)),
    };
    got.map_err(|e| format!("{e:?}"))
}

/// Coverage counts of the twin runs, by case.
#[derive(Default)]
struct Coverage(BTreeMap<&'static str, u64>);

impl Coverage {
    fn note(&mut self, case: &'static str) {
        *self.0.entry(case).or_default() += 1;
    }

    /// Count the case of `op`, made from the row `from` (the cursor's logical key before it), that
    /// gave `got`; `tail` is the dup cursor's [`KeyTail`] form after a tail call and its `t`.
    fn count(&mut self, op: &Op, from: Option<&[u8]>, got: &Got, tail: Option<(bool, usize)>) {
        let to = got.as_ref().ok().and_then(|g| g.as_ref()).map(|(k, _)| k.as_slice());
        let crossed = |from: &[u8], to: &[u8]| from[..K] != to[..K];
        match (op, from) {
            (Op::NextInPrefix(n) | Op::PrevInPrefix(n), Some(from)) => {
                let n = *n;
                self.note(match (n, to) {
                    _ if n > from.len() => "prefix n > L",
                    (_, Some(_)) if n >= K => "prefix K <= n <= L moved",
                    (_, None) if n == K => "prefix n = K at a key's edge",
                    (_, None) if n > K => "prefix K < n <= L stayed",
                    (_, Some(to)) if crossed(from, to) => "prefix n < K crossing keys",
                    (_, Some(_)) => "prefix n < K inside a key",
                    (_, None) => "prefix n < K stayed",
                });
            }
            (Op::Next, Some(from)) if to.is_some_and(|to| crossed(from, to)) => {
                self.note("next crossing keys")
            }
            (Op::Prev, Some(from)) if to.is_some_and(|to| crossed(from, to)) => {
                self.note("prev crossing keys")
            }
            (Op::Seek(x) | Op::SeekWithKeyTail(x, _), _) => self.note(match x.len() {
                n if n < K => "seek x < K",
                K => "seek x = K",
                _ => "seek x > K",
            }),
            (Op::SeekExact(x), _) => self.note(match (x.len() > K, to) {
                (false, _) => "seek_exact x <= K",
                (true, Some(_)) => "seek_exact hit",
                (true, None) => "seek_exact miss",
            }),
            (Op::SeekKey(_) | Op::SeekDup(..) | Op::SeekDupWithKeyTail(..), _) => {
                let same = from.is_some();
                self.note(match (got, same) {
                    (Err(_), _) => "bounded width refusal",
                    (Ok(Some(_)), true) => "bounded hit from a row",
                    (Ok(Some(_)), false) => "bounded hit fresh",
                    (Ok(None), true) => "bounded miss from a row",
                    (Ok(None), false) => "bounded miss fresh",
                })
            }
            _ => {}
        }
        if let (Some((apart, t)), Some(to)) = (tail, to) {
            self.note(match (apart, t > to.len() - K) {
                (false, _) => "tail joined",
                (true, false) => "tail apart: a run",
                (true, true) => "tail apart: t > |s|",
            });
        }
    }

    /// Every one of `cases` seen.
    #[track_caller]
    fn assert_has(&self, cases: &[&str], what: &str) {
        for case in cases {
            let seen = self.0.get(case).is_some_and(|&n| n > 0);
            assert!(seen, "{what}: never seen: {case}; {:?}", self.0);
        }
    }

    /// Every case seen.
    #[track_caller]
    fn assert_complete(&self) {
        const CASES: [&str; 25] = [
            "prefix n > L",
            "prefix K <= n <= L moved",
            "prefix n = K at a key's edge",
            "prefix K < n <= L stayed",
            "prefix n < K crossing keys",
            "prefix n < K inside a key",
            "prefix n < K stayed",
            "next crossing keys",
            "prev crossing keys",
            "seek x < K",
            "seek x = K",
            "seek x > K",
            "seek_exact x <= K",
            "seek_exact hit",
            "seek_exact miss",
            "bounded width refusal",
            "bounded hit from a row",
            "bounded hit fresh",
            "bounded miss from a row",
            "bounded miss fresh",
            "tail joined",
            "tail apart: a run",
            "tail apart: t > |s|",
            "first",
            "last",
        ];
        for case in CASES {
            assert!(self.0.get(case).is_some_and(|&n| n > 0), "never seen: {case}; {:?}", self.0);
        }
    }
}

/// The dup cursor's tail form after a tail call: `(apart, t)`.
fn tail_form(c: &DupRoCursor, op: &Op) -> Option<(bool, usize)> {
    let t = match op {
        Op::CurrentWithKeyTail(t) | Op::SeekWithKeyTail(_, t) | Op::SeekDupWithKeyTail(_, _, t) => {
            *t
        }
        _ => return None,
    };
    let (_, tail) = c.current_with_key_tail(t).expect("tail")?;
    Some((matches!(tail, KeyTail::Apart(_)), t))
}

/// Oracle 2a over `seeds` twin pairs of `shape`, `ops` calls each.
fn twin_moves(shape: &Shape, seeds: std::ops::Range<u64>, ops: usize) {
    let mut coverage = Coverage::default();
    for seed in seeds {
        let seed = 0x2a2a_0000_0000 + seed;
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let (_dir, path) = temp("tndb_b_dup_cursor_twins");
        let env = open(&path, SyncMode::NoSync);
        let (d, p, rows) = twins(&env, shape, &mut rng);
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let stat = ro.table_stat(&d).expect("stat");
        assert!(stat.height >= 2 && stat.overflow_pages > 0, "{stat:?}");
        let snapshot = env.inner().current();
        let tallest = walk_outer_tree(&*snapshot, &tree(&env, "d"))
            .entries
            .iter()
            .map(|(_, cell)| {
                let root = TreeRoot::decode(cell.as_slice().try_into().expect("32 bytes"));
                root.expect("a nested root").height
            })
            .max();
        assert!(tallest >= Some(shape.nested_height), "the tallest nested tree: {tallest:?}");
        drop(snapshot);
        let rows: Vec<Kv> = rows.into_iter().collect();
        let (mut dup, mut twin) = fresh(&ro, &d, &p);
        for step in 0..ops {
            if rng.below(200) == 0 {
                (dup, twin) = fresh(&ro, &d, &p);
            }
            let from = dup.current().expect("current").map(|(k, _)| k.to_vec());
            let op = Op::pick(&mut rng, &rows, from.as_deref());
            let want = on_twin(&mut twin, &op);
            let got = on_dup(&mut dup, &op);
            assert_eq!(got, want, "step {step} {op:?} from {from:02x?}");
            assert_eq!(dup.state(), twin.state(), "step {step} {op:?} from {from:02x?}");
            if matches!(op, Op::First) {
                coverage.note("first");
            }
            if matches!(op, Op::Last) {
                coverage.note("last");
            }
            coverage.count(&op, from.as_deref(), &got, tail_form(&dup, &op));
        }
        assert!(env.failure().is_none(), "no call latched: {:?}", env.failure());
    }
    coverage.assert_complete();
}

/// A new dup cursor over `d` and a new plain cursor over its twin `p`.
fn fresh(ro: &Arc<crate::RoTxn>, d: &Table, p: &Table) -> (DupRoCursor, RoCursor) {
    (ro.dup_cursor_owned(d).expect("dup cursor"), ro.cursor_owned(p).expect("twin cursor"))
}

/// Oracle 2a: random calls on a [`DupRoCursor`] and on a [`RoCursor`] over the composite twin give
/// the same rows and leave the same states, call for call, over outer and nested trees of two
/// levels or more, values inline and in runs, and outer keys sharing 0 to K-1 leading bytes
/// with their neighbours; every case of the coverage counts is seen. An empty dup table's ends
/// are the plain rules'.
#[test]
fn dup_cursor_moves_match_the_composite_twin() {
    let shape = Shape { keys: 400, big_keys: 3, big_rows: 300, run_pct: 2, nested_height: 2 };
    twin_moves(&shape, 0..3, 3000);

    let (_dir, path) = temp("tndb_b_dup_cursor_empty");
    let env = open(&path, SyncMode::NoSync);
    let mut txn = env.begin_rw().expect("rw");
    let e = txn.create_dup_table("e", OPTIONS).expect("e");
    txn.commit().expect("commit");
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let mut c = ro.dup_cursor_owned(&e).expect("cursor");
    assert_eq!(c.state(), State::Unset);
    assert_eq!(c.first().expect("first"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.next().expect("next"), None);
    assert_eq!(c.last().expect("last"), None);
    assert_eq!(c.state(), State::BeforeStart);
    assert_eq!(c.seek(b"").expect("seek"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.seek_key(&[0; K]).expect("seek_key"), None);
    assert_eq!(c.state(), State::Unset);
}

/// [`dup_cursor_moves_match_the_composite_twin`] over more seeds and calls and larger shapes:
/// more outer keys, and nested trees of three levels.
#[test]
#[ignore = "heavy: run with --release"]
fn dup_cursor_moves_match_the_composite_twin_heavy() {
    let shape = Shape { keys: 3000, big_keys: 2, big_rows: 40_000, run_pct: 2, nested_height: 3 };
    twin_moves(&shape, 0..16, 60_000);
}

/// A dup cursor of a plain table is refused with `TableKind`, with no latch.
#[test]
fn dup_cursors_on_a_plain_table_are_table_kind() {
    let (_dir, path) = temp("tndb_b_dup_cursor_kind");
    let env = open(&path, SyncMode::NoSync);
    let mut txn = env.begin_rw().expect("rw");
    let p = txn.create_table("p").expect("p");
    txn.commit().expect("commit");
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let refused = ro.dup_cursor_owned(&p);
    assert!(matches!(&refused, Err(Error::TableKind(n)) if n == "p"), "{refused:?}");
    assert!(env.failure().is_none());
}

/// A dup cursor finds its table's handle in the snapshot's slot that remembers it, the first of
/// its probe or one past it, and keeps a copy of the handle only when every slot holds another:
/// each cursor moves and names its table, and the plain table in the dup table's first slot is
/// still `TableKind` to a dup cursor.
#[test]
fn a_dup_cursor_names_its_table_from_any_slot_or_its_own_copy() {
    let (_dir, path) = temp("tndb_b_dup_cursor_handle");
    let env = open(&path, SyncMode::NoSync);
    let mut txn = env.begin_rw().expect("rw");
    let d0 = txn.create_dup_table("d0", OPTIONS).expect("d0");
    let plain: Vec<Table> =
        (0..40).map(|i| txn.create_table(&format!("p{i:02}")).expect("plain")).collect();
    let d1 = txn.create_dup_table("d1", OPTIONS).expect("d1");
    for d in [&d0, &d1] {
        txn.put(d, &dup_key(7, b"s"), d.name().as_bytes()).expect("put");
    }
    txn.commit().expect("commit");

    let ro = Arc::new(env.begin_ro().expect("ro"));
    let first = |t: &Table| t.id() as usize % 32;
    let rival = plain.iter().find(|p| first(p) == first(&d0)).expect("a table sharing d0's slot");
    ro.cursor_owned(rival).expect("the rival takes d0's first slot");
    let mut c0 = ro.dup_cursor_owned(&d0).expect("d0");
    for p in &plain {
        ro.cursor_owned(p).expect("plain");
    }
    let mut c1 = ro.dup_cursor_owned(&d1).expect("d1, every slot taken");
    assert_eq!(c0.handle_slot_for_test(), Some((first(&d0) + 1) % 32), "one past its first");
    assert_eq!(c1.handle_slot_for_test(), None, "its own copy");
    let mut again = ro.dup_cursor_owned(&d0).expect("d0 again");
    assert_eq!(again.handle_slot_for_test(), c0.handle_slot_for_test());
    for (c, d) in [(&mut c0, &d0), (&mut c1, &d1), (&mut again, &d0)] {
        let row = c.seek_key(&7u32.to_be_bytes()).expect("seek").expect("the row");
        assert_eq!(row, (&dup_key(7, b"s")[..], d.name().as_bytes()));
        assert!(format!("{c:?}").contains(&format!("{:?}", d.name())), "{c:?}");
    }
    let refused = ro.dup_cursor_owned(rival);
    assert!(matches!(&refused, Err(Error::TableKind(n)) if n == rival.name()), "{refused:?}");
    assert!(env.failure().is_none());
}

/// Zero page `pgno` of the file at `path`, its CRC trailer and header included: any fetch of it
/// is `Corrupt`.
fn garble(path: &std::path::Path, pgno: u32) {
    let page = read_page(path, pgno);
    write_page(path, pgno, &vec![0; page.len()]);
}

/// The bounded seeks read only their own key: with the nested roots of [`COLD`]'s neighbours
/// garbled, `seek_key` and `seek_dup` of [`COLD`] (hits, a miss past its last subkey, from a
/// fresh cursor and from one of its rows) and `seek_dup_with_key_tail` succeed and latch nothing;
/// a key of another width is refused with the cursor unmoved; a plain `seek` past the key's last
/// row must cross into the next key's tree and is `Corrupt`.
#[test]
fn bounded_seeks_never_read_another_key() {
    let dir = TempDir::with_prefix("tndb_b_dup_bounded").expect("temp dir");
    let (path, model) = damaged(dir.path(), |path, cells| {
        assert_eq!(COLD - 1, HOT, "the neighbour below is the tall tree");
        for k in [COLD - 1, COLD + 1] {
            garble(path, cells[&k].1.root);
        }
    });
    let (_dir, env) = copy_of(&path);
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let d = ro.open_table("d").expect("open").expect("d");
    let k = COLD.to_be_bytes();
    let own: Vec<Kv> = model
        .iter()
        .filter(|(key, _)| key.starts_with(&k))
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();
    assert!(own.len() >= 2, "{own:?}");
    let past = [0xFF];

    let mut fresh = ro.dup_cursor_owned(&d).expect("cursor");
    assert_eq!(fresh.seek_dup(&k, &past).expect("miss"), None);
    assert_eq!(fresh.state(), State::Unset);
    let mut c = ro.dup_cursor_owned(&d).expect("cursor");
    assert_eq!(owned(c.seek_key(&k).expect("seek_key")), Some(own[0].clone()));
    for row in own.iter().rev() {
        assert_eq!(owned(c.seek_dup(&k, &row.0[K..]).expect("hit")), Some(row.clone()));
    }
    let last = own.last().expect("a row");
    let tail = c.seek_dup_with_key_tail(&k, &last.0[K..], 1).expect("hit");
    assert_eq!(owned_tail(tail, 1).map(|(key, _)| key), Some(last.0.clone()));
    assert_eq!(c.seek_dup(&k, &past).expect("miss from a row"), None);
    assert_eq!(c.state(), State::Unset);
    assert_eq!(c.seek_dup_with_key_tail(&k, &past, 1).expect("miss"), None);

    assert_eq!(owned(c.seek_key(&k).expect("seek_key")), Some(own[0].clone()));
    let short = c.seek_key(&k[..K - 1]);
    assert!(matches!(short, Err(Error::KeyTooShort { len: 3, min: 4 })), "{short:?}");
    let long = c.seek_dup(&[&k[..], &[0]].concat(), &[]);
    assert!(matches!(long, Err(Error::KeyTooLarge { len: 5, max: 4 })), "{long:?}");
    assert_eq!(
        (c.state(), owned(c.current().expect("current"))),
        (State::At, Some(own[0].clone()))
    );
    assert!(env.failure().is_none(), "nothing latched: {:?}", env.failure());

    let crossed = c.seek(&dup_key(COLD, &past));
    assert!(matches!(crossed, Err(Error::Corrupt(_))), "{crossed:?}");
    assert_eq!(c.state(), State::Unset);
    assert!(env.failure().is_some(), "the crossing latched");
}

/// A tail inside the subkey comes joined from the nested leaf, one slice ending where the value
/// does; a tail past the subkey, and a value in a run, come apart.
#[test]
fn current_with_key_tail_joins_inside_the_subkey() {
    let (_dir, path) = temp("tndb_b_dup_cursor_tail");
    let env = open(&path, SyncMode::NoSync);
    let mut txn = env.begin_rw().expect("rw");
    let d = txn.create_dup_table("d", OPTIONS).expect("d");
    let (small, big) = (dup_key(5, b"abc"), dup_key(5, b"big"));
    txn.put(&d, &small, b"v1").expect("put");
    txn.put(&d, &big, &[0xb1; 10_000]).expect("put a run");
    txn.commit().expect("commit");
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let value_end = ro.get(&d, &small).expect("get").expect("the row").as_ptr_range().end;
    let mut c = ro.dup_cursor_owned(&d).expect("cursor");
    assert_eq!(c.seek_exact(&small).expect("seek_exact"), Some(&b"v1"[..]));
    for (t, want) in [(0, &b"v1"[..]), (2, b"bcv1"), (3, b"abcv1")] {
        let (key, tail) = c.current_with_key_tail(t).expect("tail").expect("a row");
        assert_eq!(key, &small[..]);
        let KeyTail::Joined(joined) = tail else { panic!("t {t}: {tail:?}") };
        assert_eq!((joined, joined.as_ptr_range().end), (want, value_end), "t {t}: the leaf's");
    }
    for t in [4, 7, usize::MAX] {
        let (_, tail) = c.current_with_key_tail(t).expect("tail").expect("a row");
        assert_eq!(tail, KeyTail::Apart(b"v1"), "t {t}");
    }
    let (_, tail) = c.seek_with_key_tail(&big, 2).expect("seek").expect("the run");
    assert!(matches!(tail, KeyTail::Apart(v) if v == [0xb1; 10_000]), "a run comes apart");
    let (key, tail) =
        c.seek_dup_with_key_tail(&5u32.to_be_bytes(), b"ab", 1).expect("seek").expect("abc");
    assert_eq!((key, tail), (&small[..], KeyTail::Joined(b"cv1")));
}

/// Every landing checks the outer key's width: with the record's width damaged to 3 over 4-byte
/// outer keys, each move that lands on an outer key, and each bounded seek whose outer seek lands
/// beside its key, is `Corrupt` naming the widths, leaves the cursor unset and latches.
#[test]
fn a_dup_cursor_meets_an_outer_key_of_another_width_as_corrupt() {
    let dir = TempDir::with_prefix("tndb_b_dup_cursor_width").expect("temp dir");
    let (path, _) = damaged(dir.path(), |path, _| {
        restamp_record(path, |record| record.dup.as_mut().expect("dup").key_len = 3);
    });
    let message = "dup table d: an outer key of 4 bytes, not the table's width 3";
    let key = dup_key(COLD, &[0]);
    for op in ["first", "last", "next", "prev", "seek", "seek_exact", "seek_key", "seek_dup"] {
        let (_dir, env) = copy_of(&path);
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let d = ro.open_table("d").expect("open").expect("d");
        let mut c = ro.dup_cursor_owned(&d).expect("cursor");
        let result = match op {
            "first" => c.first().map(drop),
            "last" => c.last().map(drop),
            "next" => c.next().map(drop),
            "prev" => c.prev().map(drop),
            "seek" => c.seek(&key).map(drop),
            "seek_exact" => c.seek_exact(&key).map(drop),
            "seek_key" => c.seek_key(&[0; 3]).map(drop),
            _ => c.seek_dup(&[0; 3], &[5]).map(drop),
        };
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains(message)),
            "{op}: {result:?}"
        );
        assert_eq!(c.state(), State::Unset, "{op}");
        assert!(env.failure().is_some(), "{op}: latched");
    }
}

/// A subkey of 0 bytes forged into [`COLD`]'s one nested leaf (its subkey 0 replaced by the empty
/// subkey, the CRC stamped again) is `Corrupt` at first use, the environment latched: a get that
/// misses beside it in a read-only and in a write transaction (`read::find_nested`),
/// and a dup cursor that lands on it, by a seek, a bounded seek or a step from either side;
/// `Env::check` names it.
#[test]
fn an_empty_subkey_is_corrupt_at_first_use() {
    let dir = TempDir::with_prefix("tndb_b_dup_empty_subkey").expect("temp dir");
    let (path, _) = damaged(dir.path(), |path, cells| {
        restamped(path, cells[&COLD].1.root, |page| {
            let (s, value) = page::leaf_inline_entry(page, 0).expect("row 0");
            assert_eq!(s, [0], "COLD's first subkey");
            let value = value.to_vec();
            page::LeafMut::new(page).expect("a leaf").delete(0).expect("delete");
            let fit = page::LeafMut::new(page).expect("a leaf").insert(0, &[], &value[..]);
            assert_eq!(fit.expect("inserted"), page::Fit::Done);
        });
    });
    let message = "a subkey of 0 bytes";
    let miss = dup_key(COLD, &[0]);
    for rw in [false, true] {
        let (_dir, env) = copy_of(&path);
        let result = if rw {
            let txn = env.begin_rw().expect("rw");
            let d = txn.open_table("d").expect("open").expect("d");
            txn.get(&d, &miss).map(drop)
        } else {
            let ro = env.begin_ro().expect("ro");
            let d = ro.open_table("d").expect("open").expect("d");
            ro.get(&d, &miss).map(drop)
        };
        let corrupt = matches!(&result, Err(Error::Corrupt(m)) if m.contains(message));
        assert!(corrupt, "get (rw {rw}): {result:?}");
        assert!(env.failure().is_some(), "get (rw {rw}): latched");
    }
    let cold = COLD.to_be_bytes();
    for op in ["seek", "seek_key", "seek_dup", "next", "prev"] {
        let (_dir, env) = copy_of(&path);
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let d = ro.open_table("d").expect("open").expect("d");
        let mut c = ro.dup_cursor_owned(&d).expect("cursor");
        let result = match op {
            "seek" => c.seek(&cold).map(drop),
            "seek_key" => c.seek_key(&cold).map(drop),
            "seek_dup" => c.seek_dup(&cold, &[]).map(drop),
            "next" => {
                // HOT's last row is `big` (its other subkeys are 4 bytes from 0): the next is
                // COLD's first.
                assert!(c.seek_exact(&dup_key(HOT, b"big")).expect("HOT's last").is_some());
                c.next().map(drop)
            }
            _ => {
                assert!(c.seek_exact(&dup_key(COLD, &[1])).expect("COLD's row 1").is_some());
                c.prev().map(drop)
            }
        };
        let corrupt = matches!(&result, Err(Error::Corrupt(m)) if m.contains(message));
        assert!(corrupt, "{op}: {result:?}");
        assert_eq!(c.state(), State::Unset, "{op}");
        assert!(env.failure().is_some(), "{op}: latched");
    }
    let (_dir, env) = copy_of(&path);
    let result = env.check();
    let corrupt = matches!(&result, Err(Error::Corrupt(m)) if m.contains("subkey of 0 bytes"));
    assert!(corrupt, "check: {result:?}");
}

// ---- seeks from random positions ----

/// A row key near `at` among the sorted `rows`: `at` itself, up to 16 rows either side, or up to
/// 400 (another nested leaf of a big key, another outer leaf); any row one time in four, or with
/// no `at`.
fn near_row<'r>(rng: &mut Rng, rows: &'r [Kv], at: Option<&'r [u8]>) -> &'r [u8] {
    let (at, reach) = match (at, rng.below(4)) {
        (Some(at), 0) => return at,
        (Some(at), 1) => (at, 16),
        (Some(at), 2) => (at, 400),
        _ => return pick_row(rng, rows),
    };
    let i = rows.partition_point(|(k, _)| k.as_slice() < at) + rng.below(2 * reach) as usize;
    &rows[i.saturating_sub(reach as usize).min(rows.len() - 1)].0
}

/// A seek of each kind for a cursor at `at`, of a target near it ([`near_row`]): in [`target`]'s
/// length classes for the unbounded seeks, or past the near key's last subkey;
/// [`bounded_target`]'s keys for the bounded ones.
fn seek_near(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> Op {
    let near = near_row(rng, rows, at).to_vec();
    let t = [0, 1, 3, usize::MAX][rng.below(4) as usize];
    match rng.below(7) {
        0 => Op::Seek(shaped(rng, near)),
        6 => Op::Seek([&near[..K], &[0xFF; 13][..]].concat()),
        1 => Op::SeekExact(shaped(rng, near)),
        2 => Op::SeekWithKeyTail(shaped(rng, near), t),
        3 => Op::SeekKey(bounded_target(rng, rows, Some(&near)).0),
        4 => {
            let (k, s) = bounded_target(rng, rows, Some(&near));
            Op::SeekDup(k, s)
        }
        _ => {
            let (k, s) = bounded_target(rng, rows, Some(&near));
            Op::SeekDupWithKeyTail(k, s, t)
        }
    }
}

/// The whole outer key (of the key width) whose rows seek `op` may land on: `x[..K]` of a seek
/// of at least K bytes or a `seek_exact` of more, a bounded seek's `k`.
fn outer_target(op: &Op) -> Option<&[u8]> {
    match op {
        Op::Seek(x) | Op::SeekWithKeyTail(x, _) if x.len() >= K => Some(&x[..K]),
        Op::SeekExact(x) if x.len() > K => Some(&x[..K]),
        Op::SeekKey(k) | Op::SeekDup(k, _) | Op::SeekDupWithKeyTail(k, _, _) if k.len() == K => {
            Some(k)
        }
        _ => None,
    }
}

/// A cursor at a row whose leaves a seek starts from: the row's logical key and the first and
/// last keys of its outer and nested leaves.
struct Held<'a> {
    key: &'a [u8],
    leaves: &'a [LeafBounds; 2],
}

/// `key` lies between the first and last keys of the leaf `bounds`: a seek from that leaf settles
/// in it.
fn inside(bounds: &LeafBounds, key: &[u8]) -> bool {
    bounds.as_ref().is_some_and(|(first, last)| first.as_slice() <= key && key <= last.as_slice())
}

/// The class of seek `op` and the root descents it makes at both levels: from `held` (`None`: a
/// cursor not at a row, or stale, which descends from the roots), landing on the row `landed` (its
/// logical key); `has_key`: [`outer_target`] has rows, `empty`: the table has none. A seek inside
/// the cursor's key never moves the outer path (0 inside the nested leaf, else 1; a seek past the
/// key's last subkey then steps the outer path from its leaf and descends the next key's nested
/// root); a seek to another key starts from the outer leaf when the key lies in it (else 1) and
/// descends the new key's nested root (1, and 1 more when it misses there and crosses to the next
/// key); from the roots the outer descent is 1 (0 on an empty table).
fn expected(
    op: &Op,
    held: Option<&Held<'_>>,
    landed: Option<&[u8]>,
    has_key: bool,
    empty: bool,
) -> (&'static str, u64) {
    let left = |xk: &[u8]| landed.is_none_or(|to| to[..K] != *xk);
    let crossed = |xk: &[u8]| landed.is_some_and(|to| to[..K] != *xk);
    let same = |xk: &[u8]| held.filter(|h| xk.len() == K && h.key[..K] == *xk);
    let nested = |h: &Held<'_>, s: &[u8]| {
        if inside(&h.leaves[1], s) {
            ("same key inside the nested leaf", 0)
        } else {
            ("same key outside the nested leaf", 1)
        }
    };
    let other = |xk: &[u8]| match held {
        Some(h) if inside(&h.leaves[0], xk) => ("another key inside the outer leaf", 0),
        Some(_) => ("another key outside the outer leaf", 1),
        None => ("from the roots", u64::from(!empty)),
    };
    match op {
        Op::Seek(x) | Op::SeekWithKeyTail(x, _) => {
            let (xk, xs) = x.split_at(K.min(x.len()));
            if let Some(h) = same(xk) {
                let (class, d) = nested(h, xs);
                if left(xk) {
                    return ("same key past its last subkey", d + u64::from(crossed(xk)));
                }
                return (class, d);
            }
            let (class, d) = other(xk);
            match (x.len() >= K && has_key, x.len() == K) {
                (false, _) => (class, d + u64::from(landed.is_some())),
                (true, true) => (class, d + 1),
                (true, false) => (class, d + 1 + u64::from(crossed(xk))),
            }
        }
        Op::SeekExact(x) if x.len() <= K => ("seek_exact of at most K bytes", 0),
        Op::SeekExact(x) => {
            let (xk, xs) = x.split_at(K);
            match same(xk) {
                Some(h) => nested(h, xs),
                None => {
                    let (class, d) = other(xk);
                    (class, d + u64::from(has_key))
                }
            }
        }
        Op::SeekKey(k) | Op::SeekDup(k, _) | Op::SeekDupWithKeyTail(k, _, _) if k.len() != K => {
            (REFUSED, 0)
        }
        Op::SeekKey(k) | Op::SeekDup(k, _) | Op::SeekDupWithKeyTail(k, _, _) => {
            let s: &[u8] = match op {
                Op::SeekDup(_, s) | Op::SeekDupWithKeyTail(_, s, _) => s,
                _ => &[],
            };
            match same(k) {
                Some(_) if s.is_empty() => ("same key, its first row", 1),
                Some(h) => nested(h, s),
                None => {
                    let (class, d) = other(k);
                    (class, d + u64::from(has_key))
                }
            }
        }
        other => panic!("not a seek: {other:?}"),
    }
}

/// `f`'s result and the root descents it made.
fn measured(f: impl FnOnce() -> Got) -> (Got, u64) {
    let before = root_descents();
    let got = f();
    (got, root_descents() - before)
}

/// A seek from a position against a new cursor's: the same result, and (in debug builds, which
/// count them) each cursor's root descents [`expected`]'s for its class; the class noted and
/// returned.
#[allow(clippy::too_many_arguments)]
fn check_seek(
    seek: &Op,
    held: Option<&Held<'_>>,
    (got, made): &(Got, u64),
    (want, fresh_made): &(Got, u64),
    (has_key, empty): (bool, bool),
    context: &str,
    seen: &mut Coverage,
) -> &'static str {
    assert_eq!(got, want, "{context}");
    let landed = |g: &Got| g.as_ref().ok().and_then(Option::as_ref).map(|(k, _)| k.to_vec());
    let (class, descents) = expected(seek, held, landed(got).as_deref(), has_key, empty);
    let (_, fresh) = expected(seek, None, landed(want).as_deref(), has_key, empty);
    if cfg!(debug_assertions) {
        assert_eq!(*made, descents, "{context}: {class}");
        assert_eq!(*fresh_made, fresh, "{context}: the new cursor's");
    }
    seen.note(class);
    class
}

/// A width refusal leaves the cursor where it was (a new cursor stays unset): no state to compare
/// and no steps after it.
const REFUSED: &str = "width refusal";

/// A call that positions a cursor at `at` somewhere: any call ([`Op::pick`]: rows, ends, past
/// them, unset), or half the time a seek near `at` (at a row).
fn position_op(rng: &mut Rng, rows: &[Kv], at: Option<&[u8]>) -> Op {
    if rng.below(2) == 0 {
        return Op::pick(rng, rows, at);
    }
    Op::Seek(near_row(rng, rows, at).to_vec())
}

/// A positioning call's result: a row, none, or a bounded seek's width refusal.
#[track_caller]
fn positioned(got: Got) {
    if let Err(e) = got {
        assert!(e.starts_with("KeyToo"), "a positioning call failed: {e}");
    }
}

/// The seek classes each cursor kind must meet.
const SEEK_CLASSES: [&str; 9] = [
    "same key inside the nested leaf",
    "same key outside the nested leaf",
    "same key, its first row",
    "same key past its last subkey",
    "another key inside the outer leaf",
    "another key outside the outer leaf",
    "from the roots",
    "seek_exact of at most K bytes",
    "width refusal",
];

/// `rounds` seeks of a [`DupRoCursor`] over `d` from the positions its random calls leave it at,
/// each with two steps after it, against a new cursor's.
fn ro_positions(ro: &Arc<crate::RoTxn>, d: &Table, rows: &[Kv], rng: &mut Rng, rounds: usize) {
    let mut seen = Coverage::default();
    let mut c = ro.dup_cursor_owned(d).expect("cursor");
    for round in 0..rounds {
        for _ in 0..=rng.below(2) {
            let at = c.current().expect("current").map(|(k, _)| k.to_vec());
            positioned(on_dup(&mut c, &position_op(rng, rows, at.as_deref())));
        }
        let at = c.current().expect("current").map(|(k, _)| k.to_vec());
        let leaves = c.leaf_bounds_for_test();
        let held = at.as_deref().map(|key| Held { key, leaves: &leaves });
        let seek = seek_near(rng, rows, at.as_deref());
        let mut probe = ro.dup_cursor_owned(d).expect("probe");
        let has_key = outer_target(&seek).is_some_and(|k| probe.seek_key(k).expect("p").is_some());
        let empty = probe.first().expect("probe").is_none();
        let context = format!("ro round {round}: {seek:02x?} from {at:02x?} {leaves:02x?}");
        let from = c.state();
        let mine = measured(|| on_dup(&mut c, &seek));
        let mut f = ro.dup_cursor_owned(d).expect("fresh");
        let theirs = measured(|| on_dup(&mut f, &seek));
        let class =
            check_seek(&seek, held.as_ref(), &mine, &theirs, (has_key, empty), &context, &mut seen);
        if class == REFUSED {
            assert_eq!(c.state(), from, "{context}");
            continue;
        }
        assert_eq!(c.state(), f.state(), "{context}");
        for _ in 0..2 {
            let step = if rng.below(2) == 0 { Op::Next } else { Op::Prev };
            assert_eq!(on_dup(&mut c, &step), on_dup(&mut f, &step), "{context}, then {step:?}");
            assert_eq!(c.state(), f.state(), "{context}, then {step:?}");
        }
    }
    seen.assert_has(&SEEK_CLASSES, "ro");
}

/// `op` on the write cursor.
fn on_txn(c: &mut DupTxnCursor, txn: &RwTxn, op: &Op) -> Got {
    let got: Result<Option<Kv>> = match op {
        Op::First => c.first(txn).map(owned),
        Op::Last => c.last(txn).map(owned),
        Op::Next => c.next(txn).map(owned),
        Op::Prev => c.prev(txn).map(owned),
        Op::Current => c.current(txn).map(owned),
        Op::CurrentWithKeyTail(t) => c.current_with_key_tail(txn, *t).map(|e| owned_tail(e, *t)),
        Op::Seek(x) => c.seek(txn, x).map(owned),
        Op::SeekExact(x) => c.seek_exact(txn, x).map(|v| v.map(|v| (x.clone(), v.to_vec()))),
        Op::SeekWithKeyTail(x, t) => c.seek_with_key_tail(txn, x, *t).map(|e| owned_tail(e, *t)),
        Op::NextInPrefix(n) => c.next_in_prefix(txn, *n).map(owned),
        Op::PrevInPrefix(n) => c.prev_in_prefix(txn, *n).map(owned),
        Op::SeekKey(k) => c.seek_key(txn, k).map(owned),
        Op::SeekDup(k, s) => c.seek_dup(txn, k, s).map(owned),
        Op::SeekDupWithKeyTail(k, s, t) => {
            c.seek_dup_with_key_tail(txn, k, s, *t).map(|e| owned_tail(e, *t))
        }
    };
    got.map_err(|e| format!("{e:?}"))
}

/// The logical keys of outer key `k`'s rows in `txn`.
fn rows_of(txn: &RwTxn, d: &Table, k: &[u8]) -> Vec<Vec<u8>> {
    let mut c = txn.dup_cursor_detached(d).expect("cursor");
    let mut keys = Vec::new();
    let mut row = c.seek_key(txn, k).expect("seek_key").map(|(key, _)| key.to_vec());
    while let Some(key) = row {
        keys.push(key);
        row = c.next_in_prefix(txn, K).expect("next_in_prefix").map(|(key, _)| key.to_vec());
    }
    keys
}

/// Between a write cursor's positioning and its seek: maybe its own `delete_current` (the
/// deleted state), or a change near its row by the transaction's direct writes: a put (of a
/// row or a new one after it), a delete of a row near it or of its own, every row of its key
/// deleted one by one, `delete_prefix` of its key; after a change, one time in three, a call that
/// finds its row again (the deleted state if the row went). Returns whether the cursor is stale
/// at the seek: the table changed since its last call.
fn disturb(rw: &mut RwTxn, d: &Table, c: &mut DupTxnCursor, rows: &[Kv], rng: &mut Rng) -> bool {
    let at = c.key_for_test().map(<[u8]>::to_vec);
    let near = near_row(rng, rows, at.as_deref()).to_vec();
    let k = at.as_deref().unwrap_or(&near)[..K].to_vec();
    let changed = match rng.below(9) {
        0 | 1 => {
            c.delete_current(rw).expect("delete_current");
            false
        }
        2 => {
            let key = if rng.below(2) == 0 { near } else { [&near[..], &[0]].concat() };
            rw.put(d, &key, &rng.next().to_le_bytes()).expect("put");
            true
        }
        3 => rw.delete(d, &near).expect("delete"),
        4 => at.as_ref().is_some_and(|at| rw.delete(d, at).expect("delete its row")),
        5 if rng.below(3) == 0 => {
            let doomed = rows_of(rw, d, &k);
            for key in &doomed {
                assert!(rw.delete(d, key).expect("delete"), "{key:02x?}");
            }
            !doomed.is_empty()
        }
        6 if rng.below(3) == 0 => rw.delete_prefix(d, &k).expect("delete_prefix") > 0,
        _ => false,
    };
    if changed && rng.below(3) == 0 {
        c.follower_shares_prefix(rw, K).expect("follower_shares_prefix");
        return false;
    }
    changed
}

/// `rounds` seeks of [`DupTxnCursor`]s over `d` in write transactions (each ended after 40
/// rounds, committed one time in four, else aborted; a new cursor opened), from the positions
/// random calls leave them at, disturbed between ([`disturb`]): up to date, stale, in the deleted
/// state; each with two steps after it, against a new cursor's.
fn txn_positions(env: &Env, d: &Table, rows: &[Kv], rng: &mut Rng, rounds: usize) {
    let mut seen = Coverage::default();
    let mut rw = env.begin_rw().expect("rw");
    let mut c = rw.dup_cursor_detached(d).expect("cursor");
    for round in 0..rounds {
        if round % 40 == 39 {
            if rng.below(4) == 0 {
                rw.commit().expect("commit");
            } else {
                rw.abort();
            }
            rw = env.begin_rw().expect("rw");
            c = rw.dup_cursor_detached(d).expect("cursor");
        }
        for _ in 0..=rng.below(2) {
            let at = c.key_for_test().map(<[u8]>::to_vec);
            positioned(on_txn(&mut c, &rw, &position_op(rng, rows, at.as_deref())));
        }
        let stale = disturb(&mut rw, d, &mut c, rows, rng);
        let from = c.state();
        let at = c.key_for_test().map(<[u8]>::to_vec);
        if stale && at.is_some() {
            seen.note("stale at a row");
        }
        if !stale && from == (State::At, true) {
            seen.note("deleted state");
        }
        let leaves = if stale { [None, None] } else { c.leaf_bounds_for_test(&rw) };
        let held = at.as_deref().filter(|_| !stale).map(|key| Held { key, leaves: &leaves });
        let seek = seek_near(rng, rows, at.as_deref());
        let mut probe = rw.dup_cursor_detached(d).expect("probe");
        let has_key =
            outer_target(&seek).is_some_and(|k| probe.seek_key(&rw, k).expect("p").is_some());
        let empty = probe.first(&rw).expect("probe").is_none();
        let context = format!(
            "txn round {round}: {seek:02x?} from {at:02x?} {from:?}, stale {stale}, {leaves:02x?}"
        );
        let mine = measured(|| on_txn(&mut c, &rw, &seek));
        let mut f = rw.dup_cursor_detached(d).expect("fresh");
        let theirs = measured(|| on_txn(&mut f, &rw, &seek));
        let class =
            check_seek(&seek, held.as_ref(), &mine, &theirs, (has_key, empty), &context, &mut seen);
        if class == REFUSED {
            assert_eq!(c.state(), from, "{context}");
            continue;
        }
        assert_eq!(c.state(), f.state(), "{context}");
        for _ in 0..2 {
            let step = if rng.below(2) == 0 { Op::Next } else { Op::Prev };
            let (got, want) = (on_txn(&mut c, &rw, &step), on_txn(&mut f, &rw, &step));
            assert_eq!(got, want, "{context}, then {step:?}");
            assert_eq!(c.state(), f.state(), "{context}, then {step:?}");
        }
    }
    rw.commit().expect("commit");
    env.check().expect("check");
    seen.assert_has(&SEEK_CLASSES, "txn");
    seen.assert_has(&["stale at a row", "deleted state"], "txn");
}

/// [`ro_positions`] and [`txn_positions`] over `seeds` twin tables of `shape`.
fn seeks_from_positions(shape: &Shape, seeds: std::ops::Range<u64>, rounds: usize) {
    for seed in seeds {
        let seed = 0x5EE4_D000_0000 + seed;
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let (_dir, path) = temp("tndb_b_dup_seek_positions");
        let env = open(&path, SyncMode::NoSync);
        let (d, _, rows) = twins(&env, shape, &mut rng);
        let rows: Vec<Kv> = rows.into_iter().collect();
        let ro = Arc::new(env.begin_ro().expect("ro"));
        assert!(ro.table_stat(&d).expect("stat").height >= 2);
        ro_positions(&ro, &d, &rows, &mut rng, rounds);
        drop(ro);
        txn_positions(&env, &d, &rows, &mut rng, rounds);
        assert!(env.failure().is_none(), "no call latched: {:?}", env.failure());
    }
}

/// Seeks of every kind (`seek`, `seek_exact`, `seek_with_key_tail`, `seek_key`, `seek_dup`,
/// `seek_dup_with_key_tail`) from random positions equal a new cursor's seek, and the two steps
/// after it: on a [`DupRoCursor`], and on a [`DupTxnCursor`] up to date, stale after the
/// transaction's own writes near its row (a put, deletes, its key emptied or `delete_prefix`ed),
/// and in the deleted state. In debug builds each seek's root descents at both levels are its
/// class's exactly ([`expected`]): a seek inside the cursor's key never descends the outer root
/// (0 inside the nested leaf, else 1), a seek to another key inside the outer leaf descends the
/// new key's nested root only (1), one outside it both roots (2), as does a stale cursor's; every
/// class is met by both cursor kinds.
#[test]
fn dup_seeks_from_random_positions_match_a_fresh_seek() {
    let shape = Shape { keys: 400, big_keys: 3, big_rows: 300, run_pct: 2, nested_height: 2 };
    seeks_from_positions(&shape, 0..2, 800);
}

/// [`dup_seeks_from_random_positions_match_a_fresh_seek`] over more seeds and rounds and larger
/// tables: more outer keys, and nested trees of three levels.
#[test]
#[ignore = "heavy: run with --release"]
fn dup_seeks_from_random_positions_match_a_fresh_seek_heavy() {
    let shape = Shape { keys: 3000, big_keys: 2, big_rows: 40_000, run_pct: 2, nested_height: 3 };
    seeks_from_positions(&shape, 0..12, 12_000);
}
