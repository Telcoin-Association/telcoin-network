//! Model tests of the B+tree index: sorted scans over every bound form against a `BTreeSet`
//! model, and randomized tndb-shaped write batches (injected allocation failures rolled back with
//! `revert_to_published`, held snapshots, syncs, a reopen) against a `BTreeMap` model, with a
//! structural walk of the tree after every operation.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Display,
    ops::Bound,
    path::Path,
};

use tempfile::TempDir;

use crate::archive::{
    btree_index::{
        index::{BtreeIndex, IndexSnapshot},
        iter::{BtreeCursor, PageSource},
        page::Node,
    },
    error::fetch::FetchError,
    pack::{DataHeader, PackCompression},
};

/// xorshift64: a seeded, reproducible stream for the randomized tests.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A value in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Prints the seed if the test panics while this is alive, so a failing run can be replayed.
struct ReportSeed(u64);

impl Drop for ReportSeed {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("failing seed: {:#018x}", self.0);
        }
    }
}

/// A writable index of `ksize`-byte keys in `dir`.
fn open(dir: &Path, ksize: usize) -> BtreeIndex {
    let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
    BtreeIndex::open_btx_file(dir, &data_header, ksize as u16, false).expect("open")
}

// ---- iteration bounds against a model ----

/// Key size for the iteration tests: 7 keys per leaf and per internal page, so a few hundred keys
/// make a tree of height 3 or more.
const ITER_KSIZE: usize = 512;

/// Seed of the two-sided ranges drawn by [`check_bounds`].
const PAIRS_SEED: u64 = 0x1234_5678_9ABC_DEF1;

/// An [`ITER_KSIZE`]-byte key with `i` big-endian in front, zeros after: byte order is numeric.
fn ikey(i: u64) -> Vec<u8> {
    let mut k = vec![0_u8; ITER_KSIZE];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

/// The model's keys within `lo..hi`, in scan order (empty for an empty or inverted range).
fn model_scan(m: &BTreeSet<u64>, rev: bool, lo: Bound<u64>, hi: Bound<u64>) -> Vec<u64> {
    let mut keys: Vec<u64> = m
        .iter()
        .copied()
        .filter(|&k| match lo {
            Bound::Included(b) => k >= b,
            Bound::Excluded(b) => k > b,
            Bound::Unbounded => true,
        })
        .filter(|&k| match hi {
            Bound::Included(b) => k <= b,
            Bound::Excluded(b) => k < b,
            Bound::Unbounded => true,
        })
        .collect();
    if rev {
        keys.reverse();
    }
    keys
}

/// A cursor scan of `src` within `lo..hi`, decoded back to the numbers the keys were made from.
/// Every key was saved with its own number as the value, which is checked on the way.
fn tree_scan<S: PageSource>(src: &S, rev: bool, lo: Bound<u64>, hi: Bound<u64>) -> Vec<u64> {
    let mut cursor = BtreeCursor::new(src, rev, lo.map(ikey), hi.map(ikey)).expect("cursor");
    let mut out = Vec::new();
    while let Some(item) = cursor.next(src) {
        let (key, value) = item.expect("scan step");
        assert!(key[8..] == [0; ITER_KSIZE - 8], "a key the test never wrote");
        let i = u64::from_be_bytes(key[..8].try_into().expect("8 bytes"));
        assert_eq!(value, i, "value of key {i}");
        out.push(i);
    }
    out
}

/// Every bound form at every probe, both directions, scanned from `src` and from the model `m`:
/// unbounded; one-sided inclusive and exclusive; two-sided over `pairs` random probe pairs (any
/// order); and the degenerate two-sided ranges `p..p` and `p+1..p` at every probe.
fn check_bounds<S: PageSource>(
    src: &S,
    m: &BTreeSet<u64>,
    probes: &[u64],
    pairs: usize,
    what: &str,
) {
    let check = |rev: bool, lo: Bound<u64>, hi: Bound<u64>| {
        assert_eq!(
            tree_scan(src, rev, lo, hi),
            model_scan(m, rev, lo, hi),
            "{what}: {lo:?}..{hi:?} reverse {rev}"
        );
    };
    let both = |p: u64| [Bound::Included(p), Bound::Excluded(p)];
    let two_sided = |a: u64, b: u64| {
        for lo in both(a) {
            for hi in both(b) {
                for rev in [false, true] {
                    check(rev, lo, hi);
                }
            }
        }
    };
    for rev in [false, true] {
        check(rev, Bound::Unbounded, Bound::Unbounded);
        for &p in probes {
            for b in both(p) {
                check(rev, b, Bound::Unbounded);
                check(rev, Bound::Unbounded, b);
            }
        }
    }
    for &p in probes {
        two_sided(p, p);
        two_sided(p + 1, p);
    }
    let mut rng = Rng(PAIRS_SEED);
    let n = probes.len() as u64;
    for _ in 0..pairs {
        two_sided(probes[rng.below(n) as usize], probes[rng.below(n) as usize]);
    }
}

/// Sorted scans over every bound form (unbounded; one- and two-sided, inclusive and exclusive;
/// empty and inverted ranges), forward and reverse, at every probe key (present, absent, below
/// and above every key) equal a model's: on the writer and on a snapshot of a tree of height 3+,
/// then again after removals that empty whole leaves and subtrees, while the older snapshot keeps
/// reading its own state. Prefix scans (`0xFF` carries, the empty prefix, a prefix longer than
/// the key) equal a model filter.
#[test]
fn test_archive_btx_iteration_bounds_match_a_model() {
    let tmp = TempDir::with_prefix("test_archive_btx_iter_model").expect("temp dir");
    let mut idx = open(&tmp.path().join("idx"), ITER_KSIZE);
    let mut m = BTreeSet::new();
    // Even keys 0..800, saved in a shuffled order.
    let mut order: Vec<u64> = (0..400).map(|i| i * 2).collect();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for &i in &order {
        idx.save(&ikey(i), i).expect("save");
        m.insert(i);
    }
    assert!(idx.height() >= 3, "height {}", idx.height());
    let probes: Vec<u64> = (0..=801).collect();
    check_bounds(&idx, &m, &probes, 1_000, "writer");
    let snap: IndexSnapshot = idx.publish();
    check_bounds(&snap, &m, &probes, 1_000, "snapshot");

    // Remove contiguous runs (whole leaves and subtrees empty out) and scattered keys.
    for i in (100..300).step_by(2).chain((500..800).step_by(6)) {
        assert!(idx.remove(&ikey(i)).expect("remove"), "key {i} was present");
        m.remove(&i);
    }
    check_bounds(&idx, &m, &probes, 1_000, "writer after removes");
    check_bounds(&idx.publish(), &m, &probes, 1_000, "snapshot after removes");
    let full: BTreeSet<u64> = order.iter().copied().collect();
    check_bounds(&snap, &full, &probes, 500, "the snapshot taken before the removes");

    // Prefixes, on the writer's public iterator, against a model filter.
    for i in [0x01FF_u64, 0x02FF, 0xFFFF_FFFF_FFFF_FFFF, 0xFF00_0000_0000_0000, 0x0100] {
        idx.save(&ikey(i), i).expect("save");
        m.insert(i);
    }
    let mut prefixes: Vec<Vec<u8>> = vec![vec![], vec![0xFF], vec![0xFF; 8], vec![0; 7]];
    for i in [0x01FF_u64, 0x02FF, 0x0100, 0x0200, 0x0000, 0x0302] {
        let bytes = i.to_be_bytes();
        prefixes.extend((1..=8).map(|len| bytes[..len].to_vec()));
    }
    prefixes.push(vec![0xAB; ITER_KSIZE + 3]); // longer than the key: truncated to it
    for p in prefixes {
        let got: Vec<Vec<u8>> =
            idx.prefix(&p).expect("prefix").map(|r| r.expect("item").0).collect();
        let len = p.len().min(ITER_KSIZE);
        let want: Vec<Vec<u8>> =
            m.iter().map(|&i| ikey(i)).filter(|k| k[..len] == p[..len]).collect();
        assert_eq!(got, want, "prefix {p:02x?}");
    }
}

/// Every bound form equals a model's on an empty tree, a single-leaf tree (writer and snapshot),
/// and a tree grown to height 3+ and then fully removed (the root collapsing back to an empty
/// leaf); a snapshot taken before the removal still reads the grown tree.
#[test]
fn test_archive_btx_single_leaf_and_emptied_bounds() {
    let tmp = TempDir::with_prefix("test_archive_btx_iter_emptied").expect("temp dir");
    let mut idx = open(&tmp.path().join("idx"), ITER_KSIZE);
    let mut m = BTreeSet::new();
    let probes: Vec<u64> = (0..=12).collect();
    check_bounds(&idx, &m, &probes, 100, "empty");
    for i in [2_u64, 4, 6, 8, 10] {
        idx.save(&ikey(i), i).expect("save");
        m.insert(i);
    }
    assert_eq!(idx.height(), 1);
    check_bounds(&idx, &m, &probes, 100, "single leaf");
    check_bounds(&idx.publish(), &m, &probes, 100, "single leaf snapshot");
    for i in 0..200_u64 {
        idx.save(&ikey(i), i).expect("save");
        m.insert(i);
    }
    assert!(idx.height() >= 3, "height {}", idx.height());
    let snap = idx.publish();
    for i in (0..200_u64).rev() {
        assert!(idx.remove(&ikey(i)).expect("remove"), "key {i} was present");
        m.remove(&i);
    }
    assert_eq!((idx.height(), idx.len()), (1, 0));
    let probes: Vec<u64> = (0..=201).collect();
    check_bounds(&idx, &m, &probes, 100, "emptied");
    check_bounds(&idx.publish(), &m, &probes, 100, "emptied snapshot");
    let full: BTreeSet<u64> = (0..200).collect();
    check_bounds(&snap, &full, &probes, 100, "the snapshot taken before the removal");
}

// ---- randomized batches against a model and the structural invariants ----

/// A `ksize`-byte key holding `i` big-endian (its low `ksize` bytes when `ksize < 8`).
fn bkey(i: u64, ksize: usize) -> Vec<u8> {
    let mut k = vec![0_u8; ksize];
    let n = ksize.min(8);
    k[..n].copy_from_slice(&i.to_be_bytes()[8 - n..]);
    k
}

/// Separator `i` of the internal page `buf`, read at its offset (after the 4-byte tag and the
/// `max_internal_keys + 1` 4-byte child pointers), not through the codec under test.
fn separator(node: Node, buf: &[u8], i: usize) -> &[u8] {
    let off = 4 + (node.max_internal_keys() + 1) * 4 + i * node.ksize();
    &buf[off..off + node.ksize()]
}

/// A walk of the tree a [`PageSource`] reads, checking its structure (see [`walk_tree`]).
struct TreeWalk<'a, S> {
    src: &'a S,
    node: Node,
    /// The depth every leaf must be at: the writer's height, else the first leaf's depth.
    leaf_depth: Option<u32>,
    seen: HashSet<u32>,
    entries: Vec<(Vec<u8>, u64)>,
}

impl<'a, S: PageSource> TreeWalk<'a, S> {
    /// Visit page `p` at `depth` (the root is at 1), whose keys must lie in `lo..hi` (the range
    /// its parent's separators give it).
    fn visit(&mut self, p: u32, depth: u32, lo: Option<&'a [u8]>, hi: Option<&'a [u8]>) {
        assert!(self.seen.insert(p), "page {p} is reached twice");
        let (src, node) = (self.src, self.node);
        let buf = src.page(p).unwrap_or_else(|e| panic!("page {p} is unreadable: {e:?}"));
        assert!(node.tag_ok(buf), "page {p} has an impossible type or entry count");
        let n = node.entry_count(buf);
        let in_range = |k: &[u8]| lo.is_none_or(|lo| k >= lo) && hi.is_none_or(|hi| k < hi);
        if node.is_leaf(buf) {
            let leaf_depth = *self.leaf_depth.get_or_insert(depth);
            assert_eq!(depth, leaf_depth, "leaf {p} is at depth {depth}, not {leaf_depth}");
            assert!(n > 0 || depth == 1, "non-root leaf {p} is empty");
            for i in 0..n {
                let k = node.leaf_key(buf, i);
                assert!(in_range(k), "leaf {p}: key {i} is outside its parent's range");
                assert!(i == 0 || node.leaf_key(buf, i - 1) < k, "leaf {p}: keys out of order");
                self.entries.push((k.to_vec(), node.leaf_value(buf, i)));
            }
            return;
        }
        assert!(self.leaf_depth.is_none_or(|d| depth < d), "internal page {p} at depth {depth}");
        assert!(n > 0 || depth > 1, "the root internal page {p} has no separator");
        for i in 0..n {
            let sep = separator(node, buf, i);
            assert!(in_range(sep), "internal page {p}: separator {i} is outside its range");
            assert!(
                i == 0 || separator(node, buf, i - 1) < sep,
                "internal page {p}: separators out of order"
            );
        }
        for i in 0..=n {
            let lo = if i == 0 { lo } else { Some(separator(node, buf, i - 1)) };
            let hi = if i == n { hi } else { Some(separator(node, buf, i)) };
            self.visit(node.internal_child(buf, i), depth + 1, lo, hi);
        }
    }
}

/// Walk every page of the tree `src` reads, from its root, and return its entries in order.
/// Panics unless: every page is readable, has a possible type and count, and is reached once;
/// every leaf is at the same depth (`height`, when known); only a root leaf is empty and only a
/// non-root internal page has no separator; keys and separators are strictly ascending and lie in
/// the range their parent's separators give them.
fn walk_tree<S: PageSource>(src: &S, height: Option<u32>) -> Vec<(Vec<u8>, u64)> {
    let mut walk = TreeWalk {
        src,
        node: src.node(),
        leaf_depth: height,
        seen: HashSet::new(),
        entries: vec![],
    };
    walk.visit(src.root(), 1, None, None);
    walk.entries
}

/// `entries` are exactly the model's, in order.
fn assert_model(entries: &[(Vec<u8>, u64)], model: &BTreeMap<Vec<u8>, u64>, what: impl Display) {
    let first_difference = entries.iter().map(|(k, v)| (k, v)).zip(model).position(|(e, m)| e != m);
    assert!(
        first_difference.is_none() && entries.len() == model.len(),
        "{what}: the tree differs from the model (first difference at entry {first_difference:?}; \
         {} entries, the model {})",
        entries.len(),
        model.len()
    );
}

/// A tree the model tests read: the writer's or a published snapshot, scanned through a
/// [`BtreeCursor`] and loaded one key at a time.
trait ModelReads: PageSource {
    /// The value stored for `key`, or [`FetchError::NotFound`].
    fn load_key(&self, key: &[u8]) -> Result<u64, FetchError>;
}

impl ModelReads for BtreeIndex {
    fn load_key(&self, key: &[u8]) -> Result<u64, FetchError> {
        self.load(key)
    }
}

impl ModelReads for IndexSnapshot {
    fn load_key(&self, key: &[u8]) -> Result<u64, FetchError> {
        self.load(key)
    }
}

/// Which of the model's keys [`check_reads`] loads.
#[derive(Clone, Copy, Debug)]
enum Loads {
    /// Every key.
    Every,
    /// The first and last few keys and about [`SAMPLED_LOADS`] spread evenly between them.
    Sample,
}

/// About how many keys a [`Loads::Sample`] check loads.
const SAMPLED_LOADS: usize = 16;

/// `key` one up (`up`) or one down as a big-endian number of its own length, or `None` past the
/// end. Between `key` and its neighbour in the model, unless that neighbour is the result.
fn key_step(key: &[u8], up: bool) -> Option<Vec<u8>> {
    let mut k = key.to_vec();
    let (from, to) = if up { (0xFF, 0x00) } else { (0x00, 0xFF) };
    for b in k.iter_mut().rev() {
        if *b != from {
            *b = if up { *b + 1 } else { *b - 1 };
            return Some(k);
        }
        *b = to;
    }
    None
}

/// Reads of `src` agree with `model`: a forward and a reverse full cursor scan each yield exactly
/// the model's entries, and point loads (see [`Loads`]) return each loaded key's value and
/// `NotFound` for absent keys: the smallest and largest keys of the key size, and the keys just
/// above and below each key loaded (those that are not in the model), which fall between it and
/// its neighbours, inside its leaf or in the gap to the next leaf.
fn check_reads<S: ModelReads>(
    src: &S,
    model: &BTreeMap<Vec<u8>, u64>,
    loads: Loads,
    what: impl Display,
) {
    // Compared entry by entry as the cursor yields them: copying out every key of every scan
    // would cost more than the scans.
    let head = |key: &[u8]| key[..key.len().min(8)].to_vec();
    for reverse in [false, true] {
        let mut want: Box<dyn Iterator<Item = (&Vec<u8>, &u64)>> =
            if reverse { Box::new(model.iter().rev()) } else { Box::new(model.iter()) };
        let mut cursor = BtreeCursor::new(src, reverse, Bound::Unbounded, Bound::Unbounded)
            .unwrap_or_else(|e| panic!("{what}: cursor (reverse {reverse}): {e:?}"));
        let mut n = 0;
        while let Some(item) = cursor.next(src) {
            let (key, value) =
                item.unwrap_or_else(|e| panic!("{what}: scan step {n} (reverse {reverse}): {e:?}"));
            let Some((k, v)) = want.next() else {
                panic!("{what}: a scan (reverse {reverse}) yields more than the model's {n}");
            };
            assert!(
                key == k.as_slice() && value == *v,
                "{what}: scan step {n} (reverse {reverse}) yields {:02x?}.. = {value}, the model \
                 {:02x?}.. = {v}",
                head(key),
                head(k)
            );
            n += 1;
        }
        assert_eq!(n, model.len(), "{what}: entries a scan (reverse {reverse}) yields");
    }

    let absent = |key: &[u8]| {
        assert!(
            matches!(src.load_key(key), Err(FetchError::NotFound)),
            "{what}: absent key {key:02x?} loads {:?}",
            src.load_key(key)
        );
    };
    let ksize = src.node().ksize();
    for edge in [vec![0; ksize], vec![0xFF; ksize]] {
        if !model.contains_key(&edge) {
            absent(&edge);
        }
    }
    let stride = match loads {
        Loads::Every => 1,
        Loads::Sample => (model.len() / SAMPLED_LOADS).max(1),
    };
    let last = model.len().saturating_sub(1);
    for (i, (key, value)) in model.iter().enumerate() {
        if i % stride != 0 && i >= 4 && i + 4 <= last {
            continue;
        }
        match src.load_key(key) {
            Ok(got) => assert_eq!(got, *value, "{what}: load of key {i}"),
            Err(e) => panic!("{what}: load of key {i} ({key:02x?}): {e:?}"),
        }
        for up in [false, true] {
            if let Some(near) = key_step(key, up).filter(|k| !model.contains_key(k)) {
                absent(&near);
            }
        }
    }
}

/// The writer's tree is sound, holds exactly the model, counts it right, and reads it back (see
/// [`check_reads`]).
fn check_writer(
    idx: &BtreeIndex,
    model: &BTreeMap<Vec<u8>, u64>,
    loads: Loads,
    what: impl Display,
) {
    assert_model(&walk_tree(idx, Some(idx.height())), model, &what);
    assert_eq!(idx.len(), model.len(), "{what}: value count");
    check_reads(idx, model, loads, &what);
}

/// How often each operation and path ran: every one must have run at least once.
#[derive(Debug, Default)]
struct Coverage {
    random_saves: usize,
    ascending_saves: usize,
    descending_saves: usize,
    removes: usize,
    missed_removes: usize,
    clears: usize,
    reverts: usize,
    syncs: usize,
    snapshots_held: usize,
    dropped_out_of_order: usize,
}

impl Coverage {
    /// The operations and paths that never ran.
    fn missing(&self) -> Vec<&'static str> {
        let Self {
            random_saves,
            ascending_saves,
            descending_saves,
            removes,
            missed_removes,
            clears,
            reverts,
            syncs,
            snapshots_held,
            dropped_out_of_order,
        } = *self;
        [
            ("random saves", random_saves),
            ("ascending saves", ascending_saves),
            ("descending saves", descending_saves),
            ("removes", removes),
            ("missed removes", missed_removes),
            ("clears", clears),
            ("reverts", reverts),
            ("syncs", syncs),
            ("held snapshots", snapshots_held),
            ("snapshots dropped out of order", dropped_out_of_order),
        ]
        .into_iter()
        .filter_map(|(what, n)| (n == 0).then_some(what))
        .collect()
    }
}

/// One randomized run of [`random_batches`].
#[derive(Clone, Copy, Debug)]
struct BatchRun {
    ksize: usize,
    /// Random keys are drawn from `0..key_space`; ascending saves go above it.
    key_space: u64,
    rounds: usize,
    /// Rounds per phase: phases alternate between growing (saves) and shrinking (removes and
    /// clears), so the tree deepens, empties out, and regrows.
    phase: usize,
    seed: u64,
    /// The height the tree must reach.
    min_height: u32,
    /// How much [`check_reads`] reads. The writer loads every key at each batch's end, one held
    /// snapshot a round (in turn) is walked and read, and every held snapshot is read in full at
    /// the end. Between those, with `full_reads` (the long run) each operation loads every key
    /// and every other held snapshot is scanned and loads a sample each round; without it (the
    /// default tests, which CI runs unoptimized) each operation loads a sample and every other
    /// held snapshot checks its count and its first and last keys.
    full_reads: bool,
}

/// tndb-shaped batches over a small-page tree, against a `BTreeMap` model. Each round is a batch
/// of random, ascending and descending saves, removes (hits and misses) and clears; one in four
/// batches has an allocation failure injected (the batch then rolls back with
/// `revert_to_published`, as tndb's does), the others publish. Snapshots are held and dropped out
/// of order, and some batches sync. After every operation the tree passes [`walk_tree`] and holds
/// exactly the model, and reads it back through forward and reverse cursor scans and point loads
/// ([`check_reads`]); a revert restores the published root and height; a publish leaves no private
/// page and its snapshot passes the walk; a sync leaves no dirty or corrupt page; every held
/// snapshot keeps reading the state it was published with (see [`BatchRun::full_reads`] for how
/// much is read when). Finally a clean close and reopen serves the same tree, and writes after it
/// (reusing the free pages recovered by the open) keep the tree sound.
fn random_batches(run: BatchRun) {
    let _seed = ReportSeed(run.seed);
    let BatchRun { ksize, key_space, rounds, phase, seed, min_height, full_reads } = run;
    let op_loads = if full_reads { Loads::Every } else { Loads::Sample };
    let tmp = TempDir::with_prefix("test_archive_btx_batches").expect("temp dir");
    let dir = tmp.path().join("idx");
    let mut idx = open(&dir, ksize);
    let mut rng = Rng(seed);
    let mut model: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    // A new tree has nothing to revert to: publish once, as tndb does on create.
    drop(idx.publish());
    let mut published = (model.clone(), idx.root(), idx.height());
    let mut held: Vec<(IndexSnapshot, BTreeMap<Vec<u8>, u64>)> = Vec::new();
    let mut ascending = 0_u64;
    let mut descending = key_space * 4;
    let (mut max_height, mut max_keys) = (0, 0);
    let mut seen = Coverage::default();
    for round in 0..rounds {
        let ops = 1 + rng.below(12);
        if rng.below(4) == 0 {
            idx.fail_allocations_after(Some(rng.below(6) as usize));
        }
        let grow = (round / phase) % 2 == 0;
        let mut failed = false;
        for op in 0..ops {
            let r = rng.below(100);
            let r = if grow { r * 7 / 10 } else { 30 + r * 7 / 10 };
            let result = if r < 30 {
                let (k, v) = (bkey(rng.below(key_space), ksize), rng.next());
                seen.random_saves += 1;
                idx.save(&k, v).map(|()| {
                    model.insert(k, v);
                })
            } else if r < 45 {
                ascending += 1;
                let k = bkey(key_space + ascending, ksize);
                seen.ascending_saves += 1;
                idx.save(&k, ascending).map(|()| {
                    model.insert(k, ascending);
                })
            } else if r < 50 {
                descending = descending.saturating_sub(1);
                let k = bkey(descending, ksize);
                seen.descending_saves += 1;
                idx.save(&k, descending).map(|()| {
                    model.insert(k, descending);
                })
            } else if r < 99 || rng.below(4) != 0 {
                // Mostly a present key (the smallest, or a random one), else likely a miss.
                let k = match rng.below(3) {
                    0 => model.keys().next().cloned(),
                    1 if !model.is_empty() => {
                        model.keys().nth(rng.below(model.len() as u64) as usize).cloned()
                    }
                    _ => None,
                }
                .unwrap_or_else(|| bkey(rng.below(key_space), ksize));
                let had = model.contains_key(&k);
                if had {
                    seen.removes += 1;
                } else {
                    seen.missed_removes += 1;
                }
                idx.remove(&k).map(|removed| {
                    assert_eq!(removed, had, "round {round} op {op}: remove's result");
                    model.remove(&k);
                })
            } else {
                seen.clears += 1;
                idx.clear().map(|()| model.clear())
            };
            if result.is_err() {
                failed = true;
                break;
            }
            check_writer(&idx, &model, op_loads, format_args!("round {round} op {op}"));
            max_height = max_height.max(idx.height());
            max_keys = max_keys.max(model.len());
        }
        idx.fail_allocations_after(None);
        if failed {
            seen.reverts += 1;
            idx.revert_to_published().expect("revert");
            model = published.0.clone();
            assert_eq!(
                (idx.root(), idx.height()),
                (published.1, published.2),
                "round {round}: a revert restores the published root and height"
            );
            assert_eq!(idx.private_pages(), 0, "round {round}: a revert frees every new page");
            check_writer(
                &idx,
                &model,
                Loads::Every,
                format_args!("round {round}, after the revert"),
            );
            continue;
        }
        let snap = idx.publish();
        assert_eq!(idx.private_pages(), 0, "round {round}: a publish stamps every new page");
        assert_eq!(snap.len(), model.len());
        assert_model(&walk_tree(&snap, None), &model, format_args!("round {round}, snapshot"));
        check_reads(&idx, &model, Loads::Every, format_args!("round {round}, after the publish"));
        if full_reads {
            check_reads(&snap, &model, Loads::Every, format_args!("round {round}, snapshot"));
        }
        published = (model.clone(), idx.root(), idx.height());
        if rng.below(3) == 0 {
            seen.snapshots_held += 1;
            held.push((snap, model.clone()));
        }
        if rng.below(4) == 0 && !held.is_empty() {
            let i = rng.below(held.len() as u64) as usize;
            if i + 1 != held.len() {
                seen.dropped_out_of_order += 1;
            }
            held.swap_remove(i);
        }
        if rng.below(9) == 0 {
            seen.syncs += 1;
            idx.sync().expect("sync");
            let crc = idx.page_crc_scan();
            assert_eq!((crc.dirty, crc.corrupt), (0, 0), "round {round}: pages after a sync");
            check_writer(&idx, &model, op_loads, format_args!("round {round}, after the sync"));
        }
        for (i, (s, m)) in held.iter().enumerate() {
            assert_eq!(s.len(), m.len(), "round {round}: held snapshot {i}'s count");
            let what = format_args!("round {round}, held snapshot {i}");
            if i == round % held.len() {
                assert_model(&walk_tree(s, None), m, what);
                check_reads(s, m, op_loads, what);
            } else if full_reads {
                check_reads(s, m, Loads::Sample, what);
            } else {
                for (k, v) in m.iter().take(4).chain(m.iter().rev().take(4)) {
                    assert_eq!(s.load(k).expect("load"), *v, "round {round}: held snapshot {i}");
                }
            }
        }
    }
    eprintln!(
        "ksize {ksize}: max height {max_height}, max keys {max_keys}, final keys {}, {seen:?}",
        model.len()
    );
    assert!(max_height >= min_height, "the tree reached height {max_height}, not {min_height}");
    assert!(seen.missing().is_empty(), "never ran: {:?} ({seen:?})", seen.missing());
    assert!(seen.reverts > 10, "too few failed batches: {seen:?}");
    for (i, (s, m)) in held.iter().enumerate() {
        assert_model(&walk_tree(s, None), m, format_args!("held snapshot {i} at the end"));
        check_reads(s, m, Loads::Every, format_args!("held snapshot {i} at the end"));
    }

    // A clean close (the drop syncs and seals) and reopen serves the same tree.
    drop(held);
    drop(idx);
    let mut idx = open(&dir, ksize);
    assert!(!idx.opened_unclean(), "a clean close reopens clean");
    check_writer(&idx, &model, Loads::Every, "after the reopen");
    // Writes after the reopen take their pages from the free list the open recovered (the file
    // does not grow), and none of those pages may still be in use.
    let pages_at_open = idx.page_count();
    for op in 0..64 {
        let k = bkey(rng.below(key_space), ksize);
        if rng.below(2) == 0 {
            let v = rng.next();
            idx.save(&k, v).expect("save after the reopen");
            model.insert(k, v);
        } else {
            let had = model.remove(&k).is_some();
            assert_eq!(idx.remove(&k).expect("remove after the reopen"), had);
        }
        check_writer(&idx, &model, op_loads, format_args!("after the reopen, op {op}"));
    }
    assert_eq!(idx.page_count(), pages_at_open, "writes after the reopen reuse free pages");
    idx.sync().expect("sync");
    let crc = idx.page_crc_scan();
    assert_eq!((crc.dirty, crc.corrupt), (0, 0), "pages after the last sync");
}

/// 8-byte keys (255 per leaf): wide, shallow trees of a few hundred keys.
#[test]
fn test_archive_btx_random_batches_keep_invariants_ksize_8() {
    random_batches(BatchRun {
        ksize: 8,
        key_space: 200_000,
        rounds: 550,
        phase: 125,
        seed: 0xA5A5_5A5A_1234_4321,
        min_height: 2,
        full_reads: false,
    });
}

/// 500-byte keys (8 per leaf, 8 separators per internal page).
#[test]
fn test_archive_btx_random_batches_keep_invariants_ksize_500() {
    random_batches(BatchRun {
        ksize: 500,
        key_space: 3_000,
        rounds: 550,
        phase: 125,
        seed: 0x5151_7272_9393_A4A4,
        min_height: 4,
        full_reads: false,
    });
}

/// 1000-byte keys (4 per leaf, 4 separators per internal page).
#[test]
fn test_archive_btx_random_batches_keep_invariants_ksize_1000() {
    random_batches(BatchRun {
        ksize: 1000,
        key_space: 1_500,
        rounds: 550,
        phase: 125,
        seed: 0x1234_5678_9ABC_DEF1,
        min_height: 4,
        full_reads: false,
    });
}

/// 2032-byte keys, the largest a page takes (2 per leaf, 2 separators per internal page): every
/// insert splits or nearly, and a few dozen keys make a deep tree.
#[test]
fn test_archive_btx_random_batches_keep_invariants_ksize_2032() {
    random_batches(BatchRun {
        ksize: 2032,
        key_space: 400,
        rounds: 550,
        phase: 125,
        seed: 0x9E37_79B9_7F4A_7C15,
        min_height: 5,
        full_reads: false,
    });
}

/// The randomized batches at every key size, over more seeds and longer runs, reading every key
/// and every held snapshot after each step: `TN_BTX_ROUNDS` rounds per run (default 3100). Without
/// `TN_BTX_SEED` each key size runs its fixed seeds and one seed drawn from entropy for this run
/// (printed at the start, and by a failing run); `TN_BTX_SEED` (decimal or `0x` hex) replays that
/// one seed at every key size instead.
#[test]
#[ignore = "on-demand long randomized run; run with --release --ignored --nocapture"]
fn test_archive_btx_random_batches_keep_invariants_long() {
    let env = |name: &str| std::env::var(name).ok();
    let rounds = env("TN_BTX_ROUNDS").map_or(3_100, |r| r.trim().parse().expect("TN_BTX_ROUNDS"));
    let replay = env("TN_BTX_SEED").map(|s| {
        let s = s.trim();
        s.strip_prefix("0x")
            .map_or_else(|| s.parse(), |hex| u64::from_str_radix(hex, 16))
            .expect("TN_BTX_SEED")
    });
    // xorshift needs a nonzero seed.
    let drawn = rand::random::<u64>().max(1);
    if replay.is_none() {
        eprintln!("this run's entropy seed: {drawn:#018x} (replay with TN_BTX_SEED)");
    }
    let runs: [(usize, u64, u32, &[u64]); 4] = [
        (8, 200_000, 2, &[0xA5A5_5A5A_1234_4321, 0x0BAD_C0DE_F00D_D00D]),
        (500, 3_000, 4, &[0x5151_7272_9393_A4A4, 0x7F4A_7C15_9E37_79B9]),
        (1000, 1_500, 4, &[0x1234_5678_9ABC_DEF1, 0x0F0F_F0F0_1357_9BDF]),
        (2032, 400, 5, &[0x9E37_79B9_7F4A_7C15, 0x2545_F491_4F6C_DD1D, 0xDEAD_BEEF_CAFE_F00D]),
    ];
    for (ksize, key_space, min_height, seeds) in runs {
        let seeds = match replay {
            Some(seed) => vec![seed],
            None => seeds.iter().copied().chain([drawn]).collect(),
        };
        for seed in seeds {
            random_batches(BatchRun {
                ksize,
                key_space,
                rounds,
                phase: 250,
                seed,
                min_height,
                full_reads: true,
            });
        }
    }
}
