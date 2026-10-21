//! Cursor tests: random moves against a sorted model over trees of many shapes, the ends of empty
//! and single-entry trees, leaf crossings both ways, seeks, damage under a cursor, and the cursors
//! of the environment's transactions.

use std::{collections::BTreeMap, os::unix::fs::FileExt as _, sync::Arc};

use tempfile::TempDir;

use super::*;
use crate::{
    catalog,
    tree::{
        test_harness::{walk_tree, MemStore, ReportSeed, Rng},
        write::plain::{delete, put},
    },
    Env, EnvOptions, ReadTxn, RoCursor, Table,
};

type Kv = (Vec<u8>, Vec<u8>);

fn owned(entry: Option<(&[u8], &[u8])>) -> Option<Kv> {
    entry.map(|(k, v)| (k.to_vec(), v.to_vec()))
}

/// The model of a cursor: the sorted entries and a position.
#[derive(Debug)]
struct Model {
    entries: Vec<Kv>,
    pos: Pos,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pos {
    Unset,
    At(usize),
    AfterEnd,
    BeforeStart,
}

impl Model {
    fn new(entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> Self {
        Self {
            entries: entries.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            pos: Pos::Unset,
        }
    }

    fn state(&self) -> State {
        match self.pos {
            Pos::Unset => State::Unset,
            Pos::At(_) => State::At,
            Pos::AfterEnd => State::AfterEnd,
            Pos::BeforeStart => State::BeforeStart,
        }
    }

    /// Move to entry `i` if there is one, else to `miss`.
    fn go(&mut self, i: Option<usize>, miss: Pos) -> Option<Kv> {
        match i.filter(|&i| i < self.entries.len()) {
            Some(i) => {
                self.pos = Pos::At(i);
                Some(self.entries[i].clone())
            }
            None => {
                self.pos = miss;
                None
            }
        }
    }

    fn first(&mut self) -> Option<Kv> {
        self.go(Some(0), Pos::AfterEnd)
    }

    fn last(&mut self) -> Option<Kv> {
        self.go(self.entries.len().checked_sub(1), Pos::BeforeStart)
    }

    fn next(&mut self) -> Option<Kv> {
        match self.pos {
            Pos::Unset | Pos::BeforeStart => self.first(),
            Pos::AfterEnd => None,
            Pos::At(i) => self.go(Some(i + 1), Pos::AfterEnd),
        }
    }

    fn prev(&mut self) -> Option<Kv> {
        match self.pos {
            Pos::Unset | Pos::AfterEnd => self.last(),
            Pos::BeforeStart => None,
            Pos::At(i) => self.go(i.checked_sub(1), Pos::BeforeStart),
        }
    }

    fn current(&self) -> Option<Kv> {
        match self.pos {
            Pos::At(i) => Some(self.entries[i].clone()),
            _ => None,
        }
    }

    fn seek(&mut self, key: &[u8]) -> Option<Kv> {
        let i = self.entries.partition_point(|(k, _)| k.as_slice() < key);
        self.go(Some(i), Pos::AfterEnd)
    }

    fn seek_exact(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let i = self.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)).ok();
        self.go(i, Pos::Unset).map(|(_, v)| v)
    }

    /// `next_in_prefix(n)` (`forward`) or `prev_in_prefix(n)`: to the adjacent entry only if it
    /// shares the first `n` bytes of the current key; else `None`, the position kept.
    fn step_in_prefix(&mut self, n: usize, forward: bool) -> Option<Kv> {
        let Pos::At(i) = self.pos else { return None };
        let prefix = self.entries[i].0.get(..n)?.to_vec();
        self.step_to_prefix(if forward { Some(i + 1) } else { i.checked_sub(1) }, &prefix)
    }

    /// Move to entry `j` if there is one and its key starts with `prefix`; else `None`, the
    /// position kept.
    fn step_to_prefix(&mut self, j: Option<usize>, prefix: &[u8]) -> Option<Kv> {
        let entry = self.entries.get(j?).filter(|(k, _)| k.starts_with(prefix))?.clone();
        self.pos = Pos::At(j?);
        Some(entry)
    }

    /// Apply `op`, returning what the cursor should (a `seek_exact` hit as `(key, value)`).
    fn apply(&mut self, op: &Op) -> Option<Kv> {
        match op {
            Op::First => self.first(),
            Op::Last => self.last(),
            Op::Next => self.next(),
            Op::Prev => self.prev(),
            Op::Current => self.current(),
            Op::Seek(key) => self.seek(key),
            Op::SeekExact(key) => self.seek_exact(key).map(|v| (key.clone(), v)),
            Op::NextInPrefix(n) => self.step_in_prefix(*n, true),
            Op::PrevInPrefix(n) => self.step_in_prefix(*n, false),
        }
    }
}

/// One cursor call of the model tests.
#[derive(Clone, Debug)]
enum Op {
    First,
    Last,
    Next,
    Prev,
    Current,
    Seek(Vec<u8>),
    SeekExact(Vec<u8>),
    NextInPrefix(usize),
    PrevInPrefix(usize),
}

impl Op {
    /// A random call for a cursor whose model is `model`: a seek's key from [`probe`], a prefix
    /// step's length 0, the current key's length, one past it, 4 (the group of a grouped key) or
    /// anything up to one past the key.
    fn pick(rng: &mut Rng, model: &Model) -> Self {
        match rng.below(10) {
            0 => Self::First,
            1 => Self::Last,
            2 | 3 => Self::Next,
            4 => Self::Prev,
            5 => Self::Current,
            6 => Self::Seek(probe(rng, &model.entries)),
            7 => Self::SeekExact(probe(rng, &model.entries)),
            op => {
                let klen = match model.pos {
                    Pos::At(i) => model.entries[i].0.len(),
                    _ => 8,
                };
                let n = match rng.below(6) {
                    0 => 0,
                    1 => klen,
                    2 => klen + 1,
                    3 => 4,
                    _ => rng.below(klen as u64 + 2) as usize,
                };
                if op == 8 {
                    Self::NextInPrefix(n)
                } else {
                    Self::PrevInPrefix(n)
                }
            }
        }
    }
}

/// A key to seek: one of the tree's, just after or just before one, the empty key (before every
/// key), a key after every key, or a random one.
fn probe(rng: &mut Rng, entries: &[Kv]) -> Vec<u8> {
    let pick = |rng: &mut Rng| entries[rng.below(entries.len() as u64) as usize].0.clone();
    match rng.below(6) {
        0 if !entries.is_empty() => pick(rng),
        1 if !entries.is_empty() => {
            let mut k = pick(rng);
            k.push(0);
            k
        }
        2 if !entries.is_empty() => {
            let mut k = pick(rng);
            match k.last_mut() {
                Some(b) if *b > 0 => {
                    *b -= 1;
                    k.push(0xFF);
                }
                _ => {
                    k.pop();
                }
            }
            k
        }
        3 => Vec::new(),
        4 => vec![0xFF; 16],
        _ => random_key(rng, 1, 24),
    }
}

/// A random key of `min..=max` bytes whose first byte is below 0xFF (so `[0xFF; 16]` follows
/// every key).
fn random_key(rng: &mut Rng, min: usize, max: usize) -> Vec<u8> {
    let len = min + rng.len_upto(max - min);
    let mut k: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
    if let Some(b) = k.first_mut() {
        *b %= 0xFF;
    }
    k
}

/// The moves the model test drives, on either cursor: the detached [`TreeCursor`] with its store
/// and root, or the public [`Cursor`] (which steps inside the leaf it remembers without a fetch).
trait Moves<'s> {
    fn first(&mut self) -> Result<Option<Entry<'s>>>;
    fn last(&mut self) -> Result<Option<Entry<'s>>>;
    fn next(&mut self) -> Result<Option<Entry<'s>>>;
    fn prev(&mut self) -> Result<Option<Entry<'s>>>;
    fn current(&self) -> Result<Option<Entry<'s>>>;
    fn seek(&mut self, key: &[u8]) -> Result<Option<Entry<'s>>>;
    fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&'s [u8]>>;
    fn next_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>>;
    fn prev_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>>;
    fn state(&self) -> State;

    /// Make the call `op` (a `seek_exact` hit returned as `(key, value)`).
    fn apply(&mut self, op: &Op) -> Option<Kv> {
        match op {
            Op::First => owned(self.first().expect("first")),
            Op::Last => owned(self.last().expect("last")),
            Op::Next => owned(self.next().expect("next")),
            Op::Prev => owned(self.prev().expect("prev")),
            Op::Current => owned(self.current().expect("current")),
            Op::Seek(key) => owned(self.seek(key).expect("seek")),
            Op::SeekExact(key) => {
                self.seek_exact(key).expect("seek_exact").map(|v| (key.clone(), v.to_vec()))
            }
            Op::NextInPrefix(n) => owned(self.next_in_prefix(*n).expect("next_in_prefix")),
            Op::PrevInPrefix(n) => owned(self.prev_in_prefix(*n).expect("prev_in_prefix")),
        }
    }
}

/// A [`TreeCursor`] bound to its store and root.
struct Detached<'s, S: ?Sized> {
    cursor: TreeCursor,
    store: &'s S,
    root: TreeRoot,
}

impl<'s, S: PageRead + ?Sized> Moves<'s> for Detached<'s, S> {
    fn first(&mut self) -> Result<Option<Entry<'s>>> {
        self.cursor.first(self.store, &self.root)
    }
    fn last(&mut self) -> Result<Option<Entry<'s>>> {
        self.cursor.last(self.store, &self.root)
    }
    fn next(&mut self) -> Result<Option<Entry<'s>>> {
        self.cursor.next(self.store, &self.root)
    }
    fn prev(&mut self) -> Result<Option<Entry<'s>>> {
        self.cursor.prev(self.store, &self.root)
    }
    fn current(&self) -> Result<Option<Entry<'s>>> {
        self.cursor.current(self.store)
    }
    fn seek(&mut self, key: &[u8]) -> Result<Option<Entry<'s>>> {
        self.cursor.seek(self.store, &self.root, key)
    }
    fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&'s [u8]>> {
        self.cursor.seek_exact(self.store, &self.root, key)
    }
    fn next_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>> {
        self.cursor.step_in_prefix(self.store, &self.root, None, n, true).map(entry)
    }
    fn prev_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>> {
        self.cursor.step_in_prefix(self.store, &self.root, None, n, false).map(entry)
    }
    fn state(&self) -> State {
        self.cursor.state()
    }
}

impl<'s> Moves<'s> for Cursor<'s> {
    fn first(&mut self) -> Result<Option<Entry<'s>>> {
        Cursor::first(self)
    }
    fn last(&mut self) -> Result<Option<Entry<'s>>> {
        Cursor::last(self)
    }
    fn next(&mut self) -> Result<Option<Entry<'s>>> {
        Cursor::next(self)
    }
    fn prev(&mut self) -> Result<Option<Entry<'s>>> {
        Cursor::prev(self)
    }
    fn current(&self) -> Result<Option<Entry<'s>>> {
        Cursor::current(self)
    }
    fn seek(&mut self, key: &[u8]) -> Result<Option<Entry<'s>>> {
        Cursor::seek(self, key)
    }
    fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&'s [u8]>> {
        Cursor::seek_exact(self, key)
    }
    fn next_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>> {
        Cursor::next_in_prefix(self, n)
    }
    fn prev_in_prefix(&mut self, n: usize) -> Result<Option<Entry<'s>>> {
        Cursor::prev_in_prefix(self, n)
    }
    fn state(&self) -> State {
        Cursor::state(self)
    }
}

/// Run `ops` random moves of a cursor over `root` in `store` against `model`, comparing every
/// result and every state: half the moves on the detached tree cursor, half on the public cursor.
fn run_moves<S: PageRead + Sync>(
    store: &S,
    root: &TreeRoot,
    model: &mut Model,
    rng: &mut Rng,
    ops: usize,
) {
    let mut detached = Detached { cursor: TreeCursor::default(), store, root: *root };
    run_moves_on(&mut detached, model, rng, ops / 2);
    let latch = Latch::default();
    let mut public = Cursor::new(store, *root, &latch);
    run_moves_on(&mut public, model, rng, ops - ops / 2);
    assert_eq!(latch.failure(), None);
}

/// Run `ops` random moves of `cursor` (new, so unset) against `model`.
fn run_moves_on<'s>(cursor: &mut impl Moves<'s>, model: &mut Model, rng: &mut Rng, ops: usize) {
    model.pos = Pos::Unset;
    for step in 0..ops {
        let op = Op::pick(rng, model);
        let from = model.pos;
        let got = cursor.apply(&op);
        assert_eq!(got, model.apply(&op), "step {step} {op:?} from {from:?}");
        assert_eq!(cursor.state(), model.state(), "step {step} {op:?} from {from:?}");
        if matches!(op, Op::NextInPrefix(_) | Op::PrevInPrefix(_)) {
            // A prefix step that returns nothing keeps the position.
            assert_eq!(owned(cursor.current().expect("current")), model.current(), "step {step}");
        }
    }
}

/// A tree shape: page size, entries inserted, key lengths, value lengths, and the share of
/// entries deleted again (leaving part-full leaves: deletes merge only the pages they leave
/// under a quarter full).
#[derive(Clone, Copy, Debug)]
struct Shape {
    page_size: u32,
    n: usize,
    keys: (usize, usize),
    vmax: usize,
    delete_pct: u64,
    /// With `g > 0`, every key is one of `g` 4-byte groups followed by the random key (which may
    /// be empty), so prefix steps of 4 bytes walk a group.
    groups: u64,
}

/// A key of `shape`: random, or a random group then a random suffix.
fn shape_key(rng: &mut Rng, shape: &Shape) -> Vec<u8> {
    let suffix = random_key(rng, shape.keys.0, shape.keys.1);
    if shape.groups == 0 {
        return suffix;
    }
    let mut key = (rng.below(shape.groups) as u32).to_be_bytes().to_vec();
    key.extend_from_slice(&suffix);
    key
}

/// Build `shape` (committed) and its model.
fn build(shape: Shape, rng: &mut Rng) -> (MemStore, TreeRoot, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut store = MemStore::new(shape.page_size);
    let mut root = TreeRoot::EMPTY;
    let mut model = BTreeMap::new();
    for _ in 0..shape.n {
        let key = shape_key(rng, &shape);
        let value = vec![rng.next() as u8; rng.len_upto(shape.vmax)];
        put(&mut store, &mut root, &key, &value).expect("put");
        model.insert(key, value);
    }
    let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    for key in keys {
        if rng.below(100) < shape.delete_pct {
            assert!(delete(&mut store, &mut root, &key).expect("delete"));
            model.remove(&key);
        }
    }
    store.commit_like();
    (store, root, model)
}

/// Random moves (first/last/next/prev/current/seek/seek_exact, interleaved) match a sorted model
/// in every result and state transition, over trees of many shapes and page sizes: empty, one
/// entry, one leaf, three levels, sparse leaves left by deletes, wide 64 KiB pages; read through
/// a committed snapshot and through the writer with uncommitted changes (private pages).
#[test]
fn random_moves_match_a_sorted_model() {
    let shapes = [
        Shape { page_size: 4096, n: 0, keys: (1, 8), vmax: 8, delete_pct: 0, groups: 0 },
        Shape { page_size: 4096, n: 1, keys: (1, 8), vmax: 8, delete_pct: 0, groups: 0 },
        Shape { page_size: 4096, n: 40, keys: (1, 24), vmax: 40, delete_pct: 0, groups: 0 },
        Shape { page_size: 4096, n: 1500, keys: (100, 250), vmax: 100, delete_pct: 0, groups: 0 },
        Shape { page_size: 4096, n: 2000, keys: (150, 250), vmax: 60, delete_pct: 85, groups: 0 },
        Shape { page_size: 8192, n: 800, keys: (200, 400), vmax: 200, delete_pct: 30, groups: 0 },
        Shape { page_size: 65536, n: 3000, keys: (4, 32), vmax: 300, delete_pct: 0, groups: 0 },
        // Groups spanning several leaves, and groups of a few short keys (a key may be the bare
        // group, so a prefix step's length can equal the key's or pass it).
        Shape { page_size: 4096, n: 1500, keys: (40, 120), vmax: 60, delete_pct: 20, groups: 9 },
        Shape { page_size: 4096, n: 900, keys: (0, 2), vmax: 300, delete_pct: 10, groups: 300 },
    ];
    let mut max_height = 0;
    for (i, shape) in shapes.into_iter().enumerate() {
        let seed = 0x00C0_FFEE_u64 + i as u64;
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let (mut store, mut root, mut map) = build(shape, &mut rng);
        let walk = walk_tree(&store.reader(), &root);
        assert_eq!(walk.entries, Model::new(&map).entries, "{shape:?}: the model is the tree");
        max_height = max_height.max(root.height);
        let mut model = Model::new(&map);
        run_moves(&store.reader(), &root, &mut model, &mut rng, 3000);

        // Uncommitted changes, read through the writer's own store.
        for _ in 0..shape.n / 4 + 2 {
            let key = probe(&mut rng, &model.entries);
            if key.is_empty() || key.len() > store.geometry().max_key() {
                continue;
            }
            if rng.below(2) == 0 {
                put(&mut store, &mut root, &key, b"uncommitted").expect("put");
                map.insert(key, b"uncommitted".to_vec());
            } else if delete(&mut store, &mut root, &key).expect("delete") {
                map.remove(&key);
            }
        }
        let mut model = Model::new(&map);
        run_moves(&store, &root, &mut model, &mut rng, 2000);
    }
    assert!(max_height >= 3, "some tree has branches under the root");
}

/// The ends of an empty tree and of a single-entry tree: every state and transition.
#[test]
fn the_ends_of_empty_and_single_entry_trees() {
    let store = MemStore::new(4096);
    let empty = TreeRoot::EMPTY;
    let mut c = TreeCursor::default();
    assert_eq!(c.current(&store).expect("current"), None);
    assert_eq!(c.next(&store, &empty).expect("next"), None);
    assert_eq!(c.state(), State::AfterEnd, "next from unset = first: after the end");
    assert_eq!(c.prev(&store, &empty).expect("prev"), None);
    assert_eq!(c.state(), State::BeforeStart, "prev after the end = last: before the start");
    assert_eq!(c.next(&store, &empty).expect("next"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.last(&store, &empty).expect("last"), None);
    assert_eq!(c.state(), State::BeforeStart);
    assert_eq!(c.seek(&store, &empty, b"k").expect("seek"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.seek_exact(&store, &empty, b"k").expect("seek_exact"), None);
    assert_eq!(c.state(), State::Unset);
    assert_eq!(c.prev(&store, &empty).expect("prev"), None);
    assert_eq!(c.state(), State::BeforeStart);

    let mut store = MemStore::new(4096);
    let mut root = TreeRoot::EMPTY;
    put(&mut store, &mut root, b"k", b"v").expect("put");
    store.commit_like();
    let (s, r) = (&store.reader(), &root);
    let kv = Some((&b"k"[..], &b"v"[..]));
    let mut c = TreeCursor::default();
    assert_eq!(c.prev(s, r).expect("prev"), kv, "prev from unset = last");
    assert_eq!(c.prev(s, r).expect("prev"), None);
    assert_eq!(c.state(), State::BeforeStart);
    assert_eq!(c.prev(s, r).expect("prev"), None, "stays before the start");
    assert_eq!(c.state(), State::BeforeStart);
    assert_eq!(c.current(s).expect("current"), None);
    assert_eq!(c.next(s, r).expect("next"), kv, "next before the start = first");
    assert_eq!(c.next(s, r).expect("next"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.next(s, r).expect("next"), None, "stays after the end");
    assert_eq!(c.prev(s, r).expect("prev"), kv, "prev after the end = last");
    assert_eq!(c.current(s).expect("current"), kv);
    assert_eq!(c.seek(s, r, b"a").expect("seek"), kv);
    assert_eq!(c.seek(s, r, b"k").expect("seek"), kv);
    assert_eq!(c.seek(s, r, b"k\0").expect("seek"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.seek_exact(s, r, b"k").expect("seek_exact"), Some(&b"v"[..]));
    assert_eq!(c.state(), State::At);
    assert_eq!(c.seek_exact(s, r, b"j").expect("seek_exact"), None);
    assert_eq!(c.state(), State::Unset);
    assert_eq!(c.next(s, r).expect("next"), kv, "next from unset = first");
}

/// A 200-byte key with `i` in front (wide, so a few thousand entries make three levels).
fn key_of(i: u64) -> Vec<u8> {
    let mut k = vec![0x5A; 200];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

/// A committed tree of keys `key_of(2i)` for `i in 0..n` (inserted out of order).
fn even_tree(n: u64) -> (MemStore, TreeRoot) {
    let mut store = MemStore::new(4096);
    let mut root = TreeRoot::EMPTY;
    for i in 0..n {
        let j = i * 7919 % n;
        put(&mut store, &mut root, &key_of(2 * j), &j.to_le_bytes()).expect("put");
    }
    store.commit_like();
    (store, root)
}

/// On a three-level tree: a full scan forward and a full scan backward yield every entry, and at
/// every pair of neighbours (so across every leaf boundary, including those whose leaves share
/// only the root) next/prev/next turn around correctly.
#[test]
fn moves_cross_leaf_boundaries_both_ways() {
    let n = 1_200;
    let (store, root) = even_tree(n);
    assert_eq!(root.height, 3);
    let (s, r) = (&store.reader(), &root);
    let keys: Vec<Vec<u8>> = (0..n).map(|i| key_of(2 * i)).collect();

    let mut c = TreeCursor::default();
    let mut forward = Vec::new();
    while let Some((k, _)) = c.next(s, r).expect("next") {
        forward.push(k.to_vec());
    }
    assert_eq!(forward, keys);
    let mut c = TreeCursor::default();
    let mut backward = Vec::new();
    while let Some((k, _)) = c.prev(s, r).expect("prev") {
        backward.push(k.to_vec());
    }
    backward.reverse();
    assert_eq!(backward, keys);

    let key = |e: Option<(&[u8], &[u8])>| e.expect("an entry").0.to_vec();
    for i in 0..keys.len() - 1 {
        let mut c = TreeCursor::default();
        assert!(c.seek_exact(s, r, &keys[i]).expect("seek_exact").is_some());
        assert_eq!(key(c.next(s, r).expect("next")), keys[i + 1], "next from {i}");
        assert_eq!(key(c.prev(s, r).expect("prev")), keys[i], "prev from {}", i + 1);
        assert_eq!(key(c.next(s, r).expect("next")), keys[i + 1], "next again from {i}");
        assert_eq!(key(c.current(s).expect("current")), keys[i + 1]);
    }
}

/// A page store that logs the page number of every leaf fetched.
struct LeafLog<'s, S: ?Sized> {
    store: &'s S,
    leaves: std::sync::Mutex<Vec<u32>>,
}

impl<S: PageRead + ?Sized> PageRead for LeafLog<'_, S> {
    fn geometry(&self) -> crate::page::Geometry {
        self.store.geometry()
    }

    fn page_count(&self) -> u32 {
        self.store.page_count()
    }

    fn page(&self, pgno: u32) -> Result<&[u8]> {
        let page = self.store.page(pgno)?;
        if page::kind(page)? == Kind::Leaf {
            self.leaves.lock().expect("log").push(pgno);
        }
        Ok(page)
    }

    fn run(&self, ovf: crate::overflow::Ovf, verify: crate::overflow::RunCrc) -> Result<&[u8]> {
        self.store.run(ovf, verify)
    }
}

/// A full scan of the public cursor, either way, fetches every leaf exactly once: a step inside a
/// leaf takes no fetch, and a leaf crossing fetches only the leaf it lands in.
#[test]
fn a_scan_fetches_each_leaf_once() {
    let n = 1_200;
    let (store, root) = even_tree(n);
    assert_eq!(root.height, 3);
    let log = LeafLog { store: &store.reader(), leaves: Default::default() };
    let latch = Latch::default();
    for forward in [true, false] {
        let mut c = Cursor::new(&log, root, &latch);
        let mut entries = 0;
        let mut entry = if forward { c.first() } else { c.last() }.expect("start");
        while entry.is_some() {
            entries += 1;
            entry = if forward { c.next() } else { c.prev() }.expect("step");
        }
        assert_eq!(entries, n);
        let mut fetched = std::mem::take(&mut *log.leaves.lock().expect("log"));
        let fetches = fetched.len();
        fetched.sort_unstable();
        fetched.dedup();
        assert!(fetched.len() > 10, "{} leaves", fetched.len());
        assert_eq!(fetches, fetched.len(), "each leaf once (forward: {forward})");
    }
}

/// Seeks between keys land on the next key (across a leaf boundary when the gap ends a leaf),
/// before the first key on the first, after the last key after the end; exact seeks of absent
/// keys leave the cursor unset.
#[test]
fn seeks_land_between_before_and_after() {
    let n = 600;
    let (store, root) = even_tree(n);
    assert!(root.height >= 2);
    let (s, r) = (&store.reader(), &root);
    let mut c = TreeCursor::default();
    for i in 0..n {
        let got = c.seek(s, r, &key_of(2 * i + 1)).expect("seek");
        if i + 1 < n {
            assert_eq!(got.expect("a next key").0, key_of(2 * i + 2), "seek between {i}");
            assert_eq!(c.prev(s, r).expect("prev").expect("entry").0, key_of(2 * i));
        } else {
            assert_eq!(got, None);
            assert_eq!(c.state(), State::AfterEnd);
            assert_eq!(c.prev(s, r).expect("prev").expect("entry").0, key_of(2 * i));
        }
        assert_eq!(c.seek_exact(s, r, &key_of(2 * i + 1)).expect("seek_exact"), None);
        assert_eq!(c.state(), State::Unset);
    }
    assert_eq!(c.seek(s, r, b"").expect("seek").expect("first").0, key_of(0));
    assert_eq!(c.seek(s, r, &[0xFF; 9]).expect("seek"), None);
    assert_eq!(c.state(), State::AfterEnd);
    assert_eq!(c.next(s, r).expect("next"), None);
    assert_eq!(c.prev(s, r).expect("prev").expect("last").0, key_of(2 * (n - 1)));
}

/// Collect a scan in one direction until it ends or fails.
fn scan<S: PageRead + ?Sized>(store: &S, root: &TreeRoot, forward: bool) -> Result<usize> {
    let mut c = TreeCursor::default();
    let mut n = 0;
    loop {
        let moved = if forward { c.next(store, root) } else { c.prev(store, root) };
        match moved {
            Ok(Some(_)) => n += 1,
            Ok(None) => return Ok(n),
            Err(e) => {
                assert_eq!(c.state(), State::Unset, "an error leaves the cursor unset");
                return Err(e);
            }
        }
    }
}

fn corrupt<T: std::fmt::Debug>(r: Result<T>) -> bool {
    matches!(r, Err(Error::Corrupt(_)))
}

/// Damage under a cursor is `Corrupt`, in both directions and for seeks: a root cell misrouted
/// to its sibling's subtree and a lower cell misrouted to a sibling leaf (both cell fields
/// copied, so only the intersection of the ancestors' ranges exposes them), a cell redirected to a
/// stale, CRC-valid copy of its child (only the child txn exposes it), an emptied non-root leaf,
/// and a tree root of the wrong height.
#[test]
fn damage_under_a_cursor_is_corrupt() {
    let n = 1_000;
    let (mut store, mut root) = even_tree(n);
    assert_eq!(root.height, 3, "the root's children are branches");
    assert_eq!(scan(&store.reader(), &root, true).expect("scan"), n as usize);
    let original = store.raw(root.root).to_vec();
    for (from, to) in [(0, 1), (1, 0)] {
        let target = page::branch_child(&original, to).expect("child");
        page::branch_set_child(store.raw_mut(root.root), from, target.0, target.1).expect("set");
        store.restamp(root.root);
        let (s, r) = (&store.reader(), &root);
        assert!(corrupt(scan(s, r, true)), "{from} -> {to}: forward");
        assert!(corrupt(scan(s, r, false)), "{from} -> {to}: backward");
        // A key of the lost subtree: the first key of its separator range.
        let lost = if from == 0 {
            key_of(0)
        } else {
            page::branch_key(&original, 1).expect("separator").to_vec()
        };
        assert!(corrupt(TreeCursor::default().seek(s, r, &lost)), "{from} -> {to}: seek");
        assert!(corrupt(TreeCursor::default().seek_exact(s, r, &lost)), "{from} -> {to}: exact");
        store.raw_mut(root.root).copy_from_slice(&original);
    }

    // A cell one level down misrouted to a sibling leaf: only the nearest branch's range, kept
    // by the intersection, exposes it.
    let (branch, _) = page::branch_child(&original, 1).expect("child");
    let saved = store.raw(branch).to_vec();
    assert!(page::nkeys(&saved) >= 3);
    for (from, to) in [(2, 1), (1, 2)] {
        let target = page::branch_child(&saved, to).expect("child");
        page::branch_set_child(store.raw_mut(branch), from, target.0, target.1).expect("set");
        store.restamp(branch);
        let (s, r) = (&store.reader(), &root);
        assert!(corrupt(scan(s, r, true)), "leaf {from} -> {to}: forward");
        assert!(corrupt(scan(s, r, false)), "leaf {from} -> {to}: backward");
        let lost = page::branch_key(&saved, from).expect("separator").to_vec();
        assert!(corrupt(TreeCursor::default().seek(s, r, &lost)), "leaf {from} -> {to}: seek");
        store.raw_mut(branch).copy_from_slice(&saved);
    }

    // A stale copy: rewrite the smallest key (its path is copied), then point the root's first
    // cell back at the old child with the new child's txn.
    let old_child = page::branch_child(&original, 0).expect("child").0;
    put(&mut store, &mut root, &key_of(0), b"rewritten").expect("put");
    store.commit_like();
    let original = store.raw(root.root).to_vec();
    let (new_child, child_txn) = page::branch_child(&original, 0).expect("child");
    assert_ne!(new_child, old_child, "the path was copied");
    page::branch_set_child(store.raw_mut(root.root), 0, old_child, child_txn).expect("set");
    store.restamp(root.root);
    assert!(corrupt(TreeCursor::default().first(&store.reader(), &root)), "stale copy");
    assert!(corrupt(scan(&store.reader(), &root, false)), "stale copy, backward");
    store.raw_mut(root.root).copy_from_slice(&original);

    // An emptied (CRC-valid) non-root leaf: a scan would silently skip its entries.
    let leaf = walk_tree(&store.reader(), &root).leaves[3];
    let saved = store.raw(leaf).to_vec();
    while page::nkeys(store.raw(leaf)) > 0 {
        page::leaf_delete(store.raw_mut(leaf), 0).expect("delete");
    }
    store.restamp(leaf);
    assert!(corrupt(scan(&store.reader(), &root, true)), "empty leaf, forward");
    assert!(corrupt(scan(&store.reader(), &root, false)), "empty leaf, backward");
    store.raw_mut(leaf).copy_from_slice(&saved);

    for height in [2, 4] {
        let wrong = TreeRoot { height, ..root };
        assert!(corrupt(scan(&store.reader(), &wrong, true)), "height {height}");
        assert!(corrupt(TreeCursor::default().seek(&store.reader(), &wrong, &key_of(8))));
    }
    assert_eq!(scan(&store.reader(), &root, false).expect("scan"), n as usize);
}

fn temp_env() -> (TempDir, Env) {
    let dir = TempDir::with_prefix("tndb_b_cursor").expect("temp dir");
    let env = Env::open(dir.path().join("db"), EnvOptions::default()).expect("open");
    (dir, env)
}

/// Every entry a cursor yields from the first on.
fn entries<'t>(cursor: &mut Cursor<'t>) -> Vec<(&'t [u8], &'t [u8])> {
    let mut out = Vec::new();
    while let Some(entry) = cursor.next().expect("next") {
        out.push(entry);
    }
    out
}

/// A read-only transaction's cursor sees its snapshot; a write transaction's cursor sees the
/// transaction's own puts, replacements and deletes (and the reader's does not); an empty table's
/// cursor yields nothing. Entries outlive the cursor (they borrow the transaction).
#[test]
fn transaction_cursors_see_their_own_state() {
    let (_dir, env) = temp_env();
    let mut rw = env.begin_rw().expect("begin");
    let table = rw.create_table("t").expect("table");
    let empty = rw.create_table("empty").expect("table");
    for i in 0..300_u32 {
        rw.put(&table, &i.to_be_bytes(), &[i as u8; 50]).expect("put");
    }
    rw.commit().expect("commit");

    let ro = env.begin_ro().expect("ro");
    let mut rw = env.begin_rw().expect("begin");
    for i in (0..300_u32).step_by(3) {
        assert!(rw.delete(&table, &i.to_be_bytes()).expect("delete"));
    }
    rw.put(&table, &1_u32.to_be_bytes(), b"replaced").expect("put");
    rw.put(&table, &1000_u32.to_be_bytes(), b"new").expect("put");

    let mut want: Vec<(Vec<u8>, Vec<u8>)> = (0..300_u32)
        .filter(|i| i % 3 != 0)
        .map(|i| (i.to_be_bytes().to_vec(), vec![i as u8; 50]))
        .collect();
    want[0].1 = b"replaced".to_vec();
    want.push((1000_u32.to_be_bytes().to_vec(), b"new".to_vec()));
    let got = {
        let mut cursor = rw.cursor(&table).expect("cursor");
        entries(&mut cursor)
    };
    let got: Vec<Kv> = got.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    assert_eq!(got, want, "the writer's cursor sees its own changes");
    assert_eq!(rw.cursor(&empty).expect("cursor").last().expect("last"), None);

    let mut cursor = ro.cursor(&table).expect("cursor");
    assert_eq!(entries(&mut cursor).len(), 300, "the snapshot is unchanged");
    assert_eq!(cursor.state(), State::AfterEnd);
    assert_eq!(cursor.seek_exact(&0_u32.to_be_bytes()).expect("seek"), Some(&[0; 50][..]));
    assert_eq!(cursor.prev().expect("prev"), None);
    assert_eq!(cursor.state(), State::BeforeStart);
    assert_eq!(ro.cursor(&empty).expect("cursor").first().expect("first"), None);
    drop(rw);
}

/// An environment with tables "a" and "b" of 200 entries each (several leaves), the CRC trailer
/// of "a"'s last leaf zeroed through another descriptor (the mapping sees it).
fn env_with_a_damaged_leaf() -> (TempDir, Env, Table, Table) {
    let (dir, env) = temp_env();
    let mut rw = env.begin_rw().expect("begin");
    let (a, b) = (rw.create_table("a").expect("a"), rw.create_table("b").expect("b"));
    for i in 0..200_u32 {
        rw.put(&a, &i.to_be_bytes(), &[1; 40]).expect("put");
        rw.put(&b, &i.to_be_bytes(), &[2; 40]).expect("put");
    }
    rw.commit().expect("commit");
    let snapshot = env.inner().current();
    let root = catalog::get(&*snapshot, &snapshot.meta.catalog, "a").expect("get").expect("a").root;
    let last_leaf = *walk_tree(&*snapshot, &root).leaves.last().expect("a leaf");
    drop(snapshot);
    let file = std::fs::OpenOptions::new().write(true).open(dir.path().join("db")).expect("open");
    file.write_all_at(&[0; 4], u64::from(last_leaf + 1) * 4096 - 4).expect("write");
    (dir, env, a, b)
}

/// Damage a reader's cursor finds is `Corrupt`, leaves the cursor unset and latches the
/// environment: writes fail, other reads continue.
#[test]
fn damage_under_a_reader_cursor_latches() {
    let (_dir, env, a, b) = env_with_a_damaged_leaf();
    let ro = env.begin_ro().expect("ro");
    let mut cursor = ro.cursor(&a).expect("cursor");
    assert!(cursor.first().expect("first").is_some(), "the first leaf is intact");
    assert!(env.failure().is_none());
    assert!(matches!(cursor.last(), Err(Error::Corrupt(_))));
    assert_eq!(cursor.state(), State::Unset);
    assert!(env.failure().is_some_and(|c| c.contains("zero CRC")), "{:?}", env.failure());
    assert_eq!(entries(&mut ro.cursor(&b).expect("cursor")).len(), 200, "other reads continue");
    assert!(matches!(env.begin_rw(), Err(Error::Failed(_))));
}

/// Damage a write cursor finds latches too: its writes then fail, its reads continue.
#[test]
fn damage_under_a_write_cursor_latches() {
    let (_dir, env, a, _b) = env_with_a_damaged_leaf();
    let mut rw = env.begin_rw().expect("begin");
    let mut cursor = rw.cursor_mut(&a).expect("cursor");
    assert!(cursor.first().expect("first").is_some(), "the first leaf is intact");
    assert!(env.failure().is_none());
    assert!(matches!(cursor.last(), Err(Error::Corrupt(_))));
    assert_eq!(cursor.state(), (State::Unset, false));
    assert!(env.failure().is_some_and(|c| c.contains("zero CRC")), "{:?}", env.failure());
    assert!(matches!(cursor.put(b"k", b"v"), Err(Error::Failed(_))));
    assert!(cursor.first().expect("first").is_some(), "reads continue");
}

/// Cursors can be sent and shared across threads.
#[test]
fn cursors_are_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Cursor<'static>>();
    send_sync::<RoCursor>();
}

/// The model of an [`crate::RwCursor`]: a [`Model`] whose position can also be "after a deleted
/// entry", given as the index of its follower in the remaining entries (`len`: none). As in
/// MDBX, the cursor is then on the follower: `next` and `current` return it and leave the
/// deleted state (so `next` after `current` steps past it), `prev` steps back from it, and
/// `delete_current` deletes it.
#[derive(Debug)]
struct RwModel {
    read: Model,
    deleted: Option<usize>,
    /// The deleted entry's key while `deleted` is set.
    deleted_key: Vec<u8>,
}

impl RwModel {
    /// The cursor's `(state, deleted)` pair.
    fn state(&self) -> (State, bool) {
        match self.deleted {
            Some(i) if i < self.read.entries.len() => (State::At, true),
            Some(_) => (State::AfterEnd, true),
            None => (self.read.state(), false),
        }
    }

    /// Apply the pending delete: a plain move then starts from the follower.
    fn take_deleted(&mut self) -> Option<usize> {
        let deleted = self.deleted.take();
        if let Some(i) = deleted {
            self.read.pos = if i < self.read.entries.len() { Pos::At(i) } else { Pos::AfterEnd };
        }
        deleted
    }

    fn next(&mut self) -> Option<Kv> {
        match self.take_deleted() {
            Some(_) => self.read.current(),
            None => self.read.next(),
        }
    }

    fn prev(&mut self) -> Option<Kv> {
        self.take_deleted();
        self.read.prev()
    }

    fn current(&mut self) -> Option<Kv> {
        self.take_deleted();
        self.read.current()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.deleted = None;
        let entries = &mut self.read.entries;
        let i = match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => {
                entries[i].1 = value.to_vec();
                i
            }
            Err(i) => {
                entries.insert(i, (key.to_vec(), value.to_vec()));
                i
            }
        };
        self.read.pos = Pos::At(i);
    }

    /// Delete the entry the cursor is on: after a delete, the follower (the deleted state is
    /// kept, and the next follower has the same index); with none, nothing changes.
    fn delete_current(&mut self) -> bool {
        let i = match (self.deleted, self.read.pos) {
            (Some(i), _) if i < self.read.entries.len() => i,
            (None, Pos::At(i)) => i,
            _ => return false,
        };
        self.deleted_key = self.read.entries.remove(i).0;
        self.deleted = Some(i);
        true
    }

    /// `next_in_prefix(n)` (`forward`) or `prev_in_prefix(n)`. After a delete the prefix is the
    /// deleted key's, the candidates are its follower and the entry before it, and a refusal
    /// keeps the cursor between them.
    fn step_in_prefix(&mut self, n: usize, forward: bool) -> Option<Kv> {
        let Some(i) = self.deleted else {
            return self.read.step_in_prefix(n, forward);
        };
        let prefix = self.deleted_key.get(..n)?.to_vec();
        let entry =
            self.read.step_to_prefix(if forward { Some(i) } else { i.checked_sub(1) }, &prefix);
        if entry.is_some() {
            self.deleted = None;
        }
        entry
    }
}

/// A key of the write-cursor tests: `i` in front of 60 bytes (so a few hundred entries make
/// several leaves).
fn rw_key(i: u64) -> Vec<u8> {
    let mut k = vec![0xA5; 60];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

/// Random moves, puts (inserts and replacements) and deletes of the entry under the cursor,
/// interleaved, match a model in every result and state, over tables that grow to several levels
/// and shrink; the committed result is what a reader's cursor scans and the page accounting is
/// exact.
#[test]
fn rw_cursor_moves_and_writes_match_a_model() {
    for (seed, page_size, keys) in [(1_u64, 4096_u32, 900_u64), (2, 4096, 60), (3, 16384, 3000)] {
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed * 0x9E37_79B9);
        let dir = TempDir::with_prefix("tndb_b_rw_cursor").expect("temp dir");
        let options = EnvOptions { page_size, ..EnvOptions::default() };
        let env = Env::open(dir.path().join("db"), options).expect("open");
        let mut rw = env.begin_rw().expect("begin");
        let table = rw.create_table("t").expect("table");
        let mut model = RwModel {
            read: Model { entries: Vec::new(), pos: Pos::Unset },
            deleted: None,
            deleted_key: Vec::new(),
        };
        let mut cursor = rw.cursor_mut(&table).expect("cursor");
        let (mut max_len, mut prefix_hits) = (0, 0);
        // Calls made in the deleted state: a `current` that returned the follower, a delete
        // that removed it.
        let (mut current_hits, mut redeletes) = (0, 0);
        for step in 0..12_000 {
            // While the table grows, op 12 is one more share of puts (deletes land often: one
            // in the deleted state removes the follower); after that, deletes take three shares.
            let growing = step % 6000 < 4200;
            let op = match rng.below(if growing { 13 } else { 14 }) {
                12 if growing => 7,
                op => op,
            };
            let after_delete = model.deleted.is_some();
            let (got, want) = match op {
                0 => {
                    model.deleted = None;
                    (owned(cursor.first().expect("first")), model.read.first())
                }
                1 => {
                    model.deleted = None;
                    (owned(cursor.last().expect("last")), model.read.last())
                }
                2 => (owned(cursor.next().expect("next")), model.next()),
                3 => (owned(cursor.prev().expect("prev")), model.prev()),
                4 => {
                    let got = owned(cursor.current().expect("current"));
                    current_hits += usize::from(after_delete && got.is_some());
                    (got, model.current())
                }
                5 => {
                    let key = rw_key(rng.below(keys));
                    model.deleted = None;
                    (owned(cursor.seek(&key).expect("seek")), model.read.seek(&key))
                }
                6 => {
                    let key = rw_key(rng.below(keys));
                    model.deleted = None;
                    let got = cursor.seek_exact(&key).expect("seek_exact").map(<[u8]>::to_vec);
                    (
                        got.map(|v| (Vec::new(), v)),
                        model.read.seek_exact(&key).map(|v| (Vec::new(), v)),
                    )
                }
                7 | 8 => {
                    let key = rw_key(rng.below(keys));
                    let value = vec![step as u8; rng.len_upto(120)];
                    cursor.put(&key, &value).expect("put");
                    model.put(&key, &value);
                    (owned(cursor.current().expect("current")), model.current())
                }
                9 | 10 => {
                    // The keys share 60 bytes after `i`: 7 bytes group 256 keys, 8 name one.
                    let n = [0, 6, 7, 7, 8, 68, 69][rng.below(7) as usize];
                    let n = if rng.below(4) == 0 { rng.below(70) as usize } else { n };
                    let got =
                        if op == 9 { cursor.next_in_prefix(n) } else { cursor.prev_in_prefix(n) };
                    let got = owned(got.expect("prefix step"));
                    prefix_hits += usize::from(got.is_some());
                    let want = model.step_in_prefix(n, op == 9);
                    (got, want)
                }
                _ => {
                    let deleted = cursor.delete_current().expect("delete_current");
                    assert_eq!(deleted, model.delete_current(), "step {step}: delete_current");
                    redeletes += usize::from(after_delete && deleted);
                    (None, None)
                }
            };
            assert_eq!(got, want, "step {step} op {op}");
            assert_eq!(cursor.state(), model.state(), "step {step} op {op}");
            max_len = max_len.max(model.read.entries.len());
        }
        drop(cursor);
        assert_eq!(rw.table_stat(&table).expect("stat").entries, model.read.entries.len() as u64);
        rw.commit().expect("commit");
        let ro = env.begin_ro().expect("ro");
        let scanned: Vec<Kv> = entries(&mut ro.cursor(&table).expect("cursor"))
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(scanned, model.read.entries, "seed {seed}: the committed table");
        env.check().expect("check");
        assert!(max_len > 400 || keys < 100, "seed {seed}: the table grew ({max_len})");
        assert!(prefix_hits > 100, "seed {seed}: prefix steps moved ({prefix_hits})");
        assert!(current_hits > 20, "seed {seed}: current after a delete ({current_hits})");
        assert!(redeletes > 20, "seed {seed}: a delete after a delete ({redeletes})");
    }
}

/// `delete_current` leaves the cursor on the deleted entry's follower, as MDBX does: `next`
/// returns the follower (so first + delete/next removes everything in order), `current` returns
/// it too and leaves the cursor at it (`next` then steps past it), `prev` returns the entry before
/// the deleted one, and a second delete removes the follower; with no entry under the cursor, or
/// no follower, it changes nothing. Over a multi-level tree, so deletes empty and unlink leaves
/// under the cursor.
#[test]
fn delete_current_then_next_returns_the_follower() {
    let (_dir, env) = temp_env();
    let mut rw = env.begin_rw().expect("begin");
    let table = rw.create_table("t").expect("table");
    let n = 400;
    for i in 0..n {
        rw.put(&table, &rw_key(i), &[i as u8; 40]).expect("put");
    }
    assert!(rw.table_stat(&table).expect("stat").height >= 2);
    let mut c = rw.cursor_mut(&table).expect("cursor");
    assert!(!c.delete_current().expect("delete"), "an unset cursor deletes nothing");
    let key = |e: Option<(&[u8], &[u8])>| e.expect("an entry").0.to_vec();

    assert!(c.seek_exact(&rw_key(10)).expect("seek").is_some());
    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.state(), (State::At, true));
    assert!(c.delete_current().expect("delete"), "a second delete removes the follower");
    assert_eq!(key(c.current().expect("current")), rw_key(12), "current: the follower");
    assert_eq!(c.state(), (State::At, false), "current leaves the deleted state");
    assert_eq!(key(c.next().expect("next")), rw_key(13), "next steps past the follower");
    assert!(c.delete_current().expect("delete"));
    assert_eq!(key(c.next().expect("next")), rw_key(14), "next: the follower");
    assert!(c.delete_current().expect("delete"));
    assert_eq!(key(c.prev().expect("prev")), rw_key(12), "prev: the entry before");

    // Delete the rest in order: every next returns the follower of the entry just deleted.
    let mut deleted = 4;
    let mut at = key(c.first().expect("first"));
    loop {
        assert!(c.delete_current().expect("delete"));
        deleted += 1;
        match c.next().expect("next") {
            Some((k, _)) => {
                assert!(k > at.as_slice(), "the follower comes after the deleted key");
                at = k.to_vec();
            }
            None => break,
        }
    }
    assert_eq!(deleted, n);
    assert_eq!(c.state(), (State::AfterEnd, false));
    assert_eq!(c.prev().expect("prev"), None, "an empty table");
    assert_eq!(c.first().expect("first"), None);
    drop(c);
    assert_eq!(rw.table_stat(&table).expect("stat").entries, 0);

    // The last entry: the cursor is after the end, and prev is the new last entry.
    let mut c = rw.cursor_mut(&table).expect("cursor");
    for i in 0..3 {
        c.put(&rw_key(i), b"v").expect("put");
    }
    assert_eq!(key(c.last().expect("last")), rw_key(2));
    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.state(), (State::AfterEnd, true));
    assert!(!c.delete_current().expect("delete"), "no follower: nothing to delete");
    assert_eq!(c.state(), (State::AfterEnd, true), "a refused delete changes nothing");
    // After the end `current` is nothing, after a delete as after a seek past the end (MDBX's
    // cursor stays on the new last entry there).
    assert_eq!(c.current().expect("current"), None);
    assert_eq!(c.state(), (State::AfterEnd, false));
    assert_eq!(key(c.prev().expect("prev")), rw_key(1));
    // A refused put changes nothing, the cursor included.
    let big = vec![0; env.max_key_len() + 1];
    assert!(matches!(c.put(&big, b"v"), Err(Error::KeyTooLarge { .. })));
    assert_eq!(key(c.current().expect("current")), rw_key(1));
    drop(c);
    rw.commit().expect("commit");
    env.check().expect("check");
}

/// A 200-byte key: group `g` and entry `j` (2 bytes each) in front of 196 filler bytes.
fn grouped_key(g: u16, j: u16) -> Vec<u8> {
    let mut k = vec![0x5A; 200];
    k[..2].copy_from_slice(&g.to_be_bytes());
    k[2..4].copy_from_slice(&j.to_be_bytes());
    k
}

/// From every entry of a three-level tree of groups that span leaves, and for prefix lengths 0
/// (anything), 1 and 3 (a byte that every key shares, or that only the group's do), 2 (the
/// group), 4 (the whole entry id), the key's length and one past it, `next_in_prefix` and
/// `prev_in_prefix` match the model on the detached and the public cursor: the entry returned,
/// and the position after, kept on a refusal. Steps that cross a leaf boundary and refusals at
/// one happen in both directions; a cursor that is not at an entry returns nothing and stays.
#[test]
fn prefix_steps_from_every_entry_match_a_model() {
    let mut store = MemStore::new(4096);
    let mut root = TreeRoot::EMPTY;
    let mut map = BTreeMap::new();
    for g in 0..30_u16 {
        for j in 0..(g * 7 % 40) + 1 {
            let k = grouped_key(g, j);
            put(&mut store, &mut root, &k, &j.to_le_bytes()).expect("put");
            map.insert(k, j.to_le_bytes().to_vec());
        }
    }
    store.commit_like();
    let reader = store.reader();
    assert!(root.height >= 3, "a tree of three levels: {}", root.height);
    let mut model = Model::new(&map);
    let keys: Vec<Vec<u8>> = map.keys().cloned().collect();
    // The leaf of every entry.
    let mut probe = TreeCursor::default();
    let leaf_of: Vec<u32> = keys
        .iter()
        .map(|k| {
            probe.seek_exact(&reader, &root, k).expect("seek").expect("a hit");
            probe.leaf_pgno().expect("at an entry")
        })
        .collect();
    let latch = Latch::default();
    let mut detached = Detached { cursor: TreeCursor::default(), store: &reader, root };
    let mut public = Cursor::new(&reader, root, &latch);
    // [direction][crossed with a match, refused at a boundary]
    let mut seen = [[0; 2]; 2];
    for (i, key) in keys.iter().enumerate() {
        for n in [0, 1, 2, 3, 4, 200, 201] {
            for forward in [true, false] {
                let op = if forward { Op::NextInPrefix(n) } else { Op::PrevInPrefix(n) };
                model.pos = Pos::At(i);
                let want = model.apply(&op);
                for cursor in [&mut detached as &mut dyn Moves<'_>, &mut public] {
                    assert!(cursor.seek_exact(key).expect("seek").is_some());
                    assert_eq!(cursor.apply(&op), want, "entry {i} {op:?}");
                    let here = owned(cursor.current().expect("current"));
                    assert_eq!(here, model.current(), "entry {i} {op:?}: the position after");
                }
                let j = if forward { Some(i + 1) } else { i.checked_sub(1) };
                if j.is_some_and(|j| j < keys.len() && leaf_of[j] != leaf_of[i]) {
                    seen[usize::from(forward)][usize::from(want.is_none())] += 1;
                }
            }
        }
    }
    assert!(seen.iter().flatten().all(|&c| c > 0), "boundary coverage {seen:?}");
    assert_eq!(latch.failure(), None);

    // Not at an entry: unset, after the end, before the start.
    let mut c = TreeCursor::default();
    for setup in 0..3 {
        match setup {
            0 => {}
            1 => assert_eq!(c.seek(&reader, &root, &[0xFF; 4]).expect("seek"), None),
            _ => {
                c.first(&reader, &root).expect("first");
                assert_eq!(c.prev(&reader, &root).expect("prev"), None);
            }
        }
        let state = c.state();
        assert_ne!(state, State::At);
        for forward in [true, false] {
            let got = c.step_in_prefix(&reader, &root, None, 0, forward).expect("step");
            assert!(got.is_none() && c.state() == state, "{state:?}");
        }
    }
}

/// Make the call `op` on an owned cursor (a `seek_exact` hit returned as `(key, value)`).
fn apply_owned(cursor: &mut RoCursor, op: &Op) -> Option<Kv> {
    match op {
        Op::First => owned(cursor.first().expect("first")),
        Op::Last => owned(cursor.last().expect("last")),
        Op::Next => owned(cursor.next().expect("next")),
        Op::Prev => owned(cursor.prev().expect("prev")),
        Op::Current => owned(cursor.current().expect("current")),
        Op::Seek(key) => owned(cursor.seek(key).expect("seek")),
        Op::SeekExact(key) => {
            cursor.seek_exact(key).expect("seek_exact").map(|v| (key.clone(), v.to_vec()))
        }
        Op::NextInPrefix(n) => owned(cursor.next_in_prefix(*n).expect("next_in_prefix")),
        Op::PrevInPrefix(n) => owned(cursor.prev_in_prefix(*n).expect("prev_in_prefix")),
    }
}

/// Random moves and prefix steps of an owned cursor match a sorted model, and the borrowed
/// cursor of the same snapshot driven alongside, in every result and state: over grouped keys,
/// with values in overflow runs (a few of 1.5–21 KB) and leaves thinned by deletes, at 4 and
/// 16 KiB pages. An empty table's ends hold too.
#[test]
fn owned_cursor_moves_match_a_model_and_the_borrowed_cursor() {
    for (seed, page_size) in [(1_u64, 4096_u32), (2, 16384)] {
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed * 0x0051_7CC1);
        let dir = TempDir::with_prefix("tndb_b_owned").expect("temp dir");
        let options = EnvOptions { page_size, ..EnvOptions::default() };
        let env = Env::open(dir.path().join("db"), options).expect("open");
        let shape =
            Shape { page_size, n: 1500, keys: (0, 60), vmax: 80, delete_pct: 15, groups: 12 };
        let mut map = BTreeMap::new();
        let mut rw = env.begin_rw().expect("begin");
        let (t, empty) = (rw.create_table("t").expect("t"), rw.create_table("e").expect("e"));
        for i in 0..shape.n {
            let key = shape_key(&mut rng, &shape);
            let len = if rng.below(25) == 0 {
                1500 + rng.len_upto(20_000)
            } else {
                rng.len_upto(shape.vmax)
            };
            let value: Vec<u8> = (0..len).map(|j| (i + j) as u8).collect();
            rw.put(&t, &key, &value).expect("put");
            map.insert(key, value);
        }
        let keys: Vec<Vec<u8>> = map.keys().cloned().collect();
        for key in keys {
            if rng.below(100) < shape.delete_pct {
                assert!(rw.delete(&t, &key).expect("delete"));
                map.remove(&key);
            }
        }
        rw.commit().expect("commit");
        let ro = Arc::new(env.begin_ro().expect("ro"));
        let stat = ro.table_stat(&t).expect("stat");
        assert!(stat.overflow_pages > 0 && stat.height >= 2, "{stat:?}");
        let mut owned_cursor = ro.cursor_owned(&t).expect("cursor");
        let mut borrowed = ro.cursor(&t).expect("cursor");
        let mut model = Model::new(&map);
        for step in 0..4000 {
            let op = Op::pick(&mut rng, &model);
            let from = model.pos;
            let want = model.apply(&op);
            assert_eq!(apply_owned(&mut owned_cursor, &op), want, "step {step} {op:?} {from:?}");
            assert_eq!(borrowed.apply(&op), want, "step {step} {op:?} from {from:?}");
            assert_eq!(owned_cursor.state(), model.state(), "step {step} {op:?} {from:?}");
            assert_eq!(Moves::state(&borrowed), model.state(), "step {step}");
        }
        let mut e = ro.cursor_owned(&empty).expect("cursor");
        assert_eq!(e.first().expect("first"), None);
        assert_eq!(e.state(), State::AfterEnd);
        assert_eq!(e.last().expect("last"), None);
        assert_eq!(e.state(), State::BeforeStart);
        assert_eq!(e.next_in_prefix(0).expect("step"), None);
        assert!(env.failure().is_none(), "{:?}", env.failure());
    }
}

/// An owned cursor whose transaction handle has dropped keeps the snapshot: while later commits
/// rewrite, clear and refill its table (freeing every page the cursor's tree used, and reusing
/// the pages they may), it reads the old values, overflow runs included, from another thread.
#[test]
fn an_owned_cursor_outlives_its_transaction_handle() {
    let (_dir, env) = temp_env();
    let value =
        |i: u32, round: u8| vec![round ^ i as u8; if i.is_multiple_of(10) { 9000 } else { 50 }];
    let mut rw = env.begin_rw().expect("begin");
    let t = rw.create_table("t").expect("table");
    for i in 0..400_u32 {
        rw.put(&t, &i.to_be_bytes(), &value(i, 1)).expect("put");
    }
    rw.commit().expect("commit");
    let want: Vec<Kv> = (0..400_u32).map(|i| (i.to_be_bytes().to_vec(), value(i, 1))).collect();
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let mut cursor = ro.cursor_owned(&t).expect("cursor");
    assert_eq!(owned(cursor.seek(&200_u32.to_be_bytes()).expect("seek")), Some(want[200].clone()));
    drop(ro);
    for round in 2..9_u8 {
        let mut rw = env.begin_rw().expect("begin");
        if round % 2 == 1 {
            rw.clear_table(&t).expect("clear");
        }
        for i in 0..400_u32 {
            rw.put(&t, &(i + u32::from(round % 2)).to_be_bytes(), &value(i, round)).expect("put");
        }
        rw.commit().expect("commit");
    }
    let cursor = std::thread::spawn(move || {
        assert_eq!(owned(cursor.current().expect("current")), Some(want[200].clone()));
        let mut got = Vec::new();
        let mut entry = owned(cursor.first().expect("first"));
        while let Some(kv) = entry {
            got.push(kv);
            entry = owned(cursor.next().expect("next"));
        }
        assert!(got == want, "the snapshot's entries");
        assert_eq!(owned(cursor.prev().expect("prev")), want.last().cloned());
        cursor
    })
    .join()
    .expect("the reader thread");
    drop(cursor);
    assert!(env.failure().is_none(), "{:?}", env.failure());
    env.check().expect("check");
}

/// Damage an owned cursor's move finds, by a plain move or by a prefix step peeking past a leaf's
/// end, is `Corrupt`, leaves the cursor unset and latches the environment; other reads continue.
#[test]
fn damage_under_an_owned_cursor_latches() {
    let (_dir, env, a, b) = env_with_a_damaged_leaf();
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let mut cursor = ro.cursor_owned(&a).expect("cursor");
    assert!(cursor.first().expect("first").is_some(), "the first leaf is intact");
    assert!(env.failure().is_none());
    assert!(matches!(cursor.last(), Err(Error::Corrupt(_))));
    assert_eq!(cursor.state(), State::Unset);
    assert!(env.failure().is_some_and(|c| c.contains("zero CRC")), "{:?}", env.failure());
    let mut other = ro.cursor_owned(&b).expect("cursor");
    let mut count = usize::from(other.first().expect("first").is_some());
    while other.next().expect("next").is_some() {
        count += 1;
    }
    assert_eq!(count, 200, "other reads continue");

    let (_dir, env, a, _b) = env_with_a_damaged_leaf();
    let ro = Arc::new(env.begin_ro().expect("ro"));
    let mut cursor = ro.cursor_owned(&a).expect("cursor");
    assert!(cursor.first().expect("first").is_some());
    let error = loop {
        match cursor.next_in_prefix(0) {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the scan passed the damaged leaf"),
            Err(e) => break e,
        }
    };
    assert!(matches!(error, Error::Corrupt(_)), "{error:?}");
    assert_eq!(cursor.state(), State::Unset);
    assert!(env.failure().is_some_and(|c| c.contains("zero CRC")), "{:?}", env.failure());
}

/// After `delete_current` the prefix steps compare with the deleted key (as an MDBX cursor keeps
/// its key): the follower (forward) or the entry before the deleted one (backward) is returned,
/// and the cursor moves to it, when it shares the prefix; otherwise nothing, and the cursor stays
/// on the follower in the deleted state (with `n` past the deleted key too). `n = 0` takes the
/// follower; the ends of the table hold.
#[test]
fn prefix_steps_after_delete_current_use_the_deleted_key() {
    let (_dir, env) = temp_env();
    let mut rw = env.begin_rw().expect("begin");
    let t = rw.create_table("t").expect("table");
    for g in 1..=3_u8 {
        for j in 0..3_u8 {
            rw.put(&t, &[g, j], &[g, j, 0]).expect("put");
        }
    }
    let mut c = rw.cursor_mut(&t).expect("cursor");
    let key = |e: Option<(&[u8], &[u8])>| e.map(|(k, _)| k.to_vec());
    let deleted = (State::At, true);

    assert!(c.seek_exact(&[2, 1]).expect("seek").is_some());
    assert!(c.delete_current().expect("delete"));
    assert_eq!(key(c.next_in_prefix(1).expect("step")), Some(vec![2, 2]), "the follower");
    assert_eq!(c.state(), (State::At, false));
    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.next_in_prefix(1).expect("step"), None, "[3, 0] is another group");
    assert_eq!(c.state(), deleted, "the cursor stays on the follower, in the deleted state");
    assert_eq!(key(c.prev_in_prefix(1).expect("step")), Some(vec![2, 0]), "the entry before");
    assert_eq!(c.state(), (State::At, false));

    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.next_in_prefix(3).expect("step"), None, "n past the deleted key");
    assert_eq!(c.state(), deleted);
    assert_eq!(c.prev_in_prefix(1).expect("step"), None, "[1, 2] is another group");
    assert_eq!(c.state(), deleted);
    assert_eq!(key(c.next_in_prefix(0).expect("step")), Some(vec![3, 0]), "n = 0");

    assert!(c.seek_exact(&[3, 2]).expect("seek").is_some());
    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.next_in_prefix(0).expect("step"), None, "after the end");
    assert_eq!(c.state(), (State::AfterEnd, true));
    assert_eq!(key(c.prev_in_prefix(2).expect("step")), None, "[3, 1] differs in byte 2");
    assert_eq!(key(c.prev_in_prefix(1).expect("step")), Some(vec![3, 1]));

    assert!(c.first().expect("first").is_some());
    assert!(c.delete_current().expect("delete"));
    assert_eq!(c.prev_in_prefix(0).expect("step"), None, "nothing before the first entry");
    assert_eq!(c.state(), deleted);
    assert_eq!(key(c.next().expect("next")), Some(vec![1, 1]), "next still takes the follower");

    // A second delete removes the follower, whose key the prefix steps then compare with:
    // [1, 2, 0] shares the first two bytes of [1, 2], not of [1, 1].
    c.put(&[1, 2, 0], b"v").expect("put");
    assert!(c.seek_exact(&[1, 1]).expect("seek").is_some());
    assert!(c.delete_current().expect("delete"));
    assert!(c.delete_current().expect("delete"), "the follower, [1, 2]");
    assert_eq!(key(c.next_in_prefix(2).expect("step")), Some(vec![1, 2, 0]));
    drop(c);
    rw.commit().expect("commit");
    env.check().expect("check");
}

/// Every new cursor zeroes its path and its creation moves the whole cursor (an `RoCursor` per
/// reth cursor), so the path's steps stay 8 bytes, a page number and a slot with no padding.
#[test]
fn tree_cursor_stays_small() {
    let path = 8 * MAX_HEIGHT as usize;
    assert!(
        size_of::<TreeCursor>() <= path + 16,
        "TreeCursor is {} bytes for a path of {path}",
        size_of::<TreeCursor>()
    );
}
