//! Dup cursors against the composite twin: a dup table `d` (key width 4) and a plain table `p`
//! keyed `k ‖ s` holding the same rows, a [`DupRoCursor`] over `d` and a [`RoCursor`] over `p`
//! side by side, every call compared (Oracle 2a); the bounded seeks' reach, the key tail's two
//! forms, the outer key width met by every landing, and the kind refusal.

use std::{collections::BTreeMap, sync::Arc};

use tempfile::TempDir;

use super::{
    damage::{read_page, write_page},
    dup::{copy_of, damaged, dup_key, restamp_record, COLD, HOT, OPTIONS},
    open, temp, Rows,
};
use crate::{
    env::SyncMode,
    tree::{
        cursor::{
            tests::{owned, owned_tail, Kv},
            KeyTail, State,
        },
        test_harness::{ReportSeed, Rng},
    },
    DupRoCursor, Env, Error, ReadTxn as _, Result, RoCursor, Table,
};

/// The key width of the dup tables here ([`OPTIONS`]).
const K: usize = OPTIONS.key_len as usize;

/// The shape of a twin pair's rows: `keys` outer keys, the first `big_keys` of them with about
/// `big_rows` rows each (nested trees of two levels or more), the others with 1 to 5; `run_pct`
/// percent of the values in overflow runs.
struct Shape {
    keys: usize,
    big_keys: usize,
    big_rows: usize,
    run_pct: u64,
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
    let shape = Shape { keys: 400, big_keys: 3, big_rows: 300, run_pct: 2 };
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
