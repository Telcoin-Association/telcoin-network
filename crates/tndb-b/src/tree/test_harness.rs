//! Test harness for the tree layer: an in-memory page store ([`MemStore`]), a structural walker
//! ([`walk_tree`]), and a seeded random stream ([`Rng`], [`ReportSeed`]).

use std::collections::HashSet;

use super::{
    read::{checked_child, checked_page},
    write::{MergeReport, Scratch},
    PageRead, PageWrite, TreeRoot,
};
use crate::{
    alloc::Allocator,
    crc::{self, CrcState},
    error::{Error, Result},
    overflow::{checked_run, run_pages, Ovf, RunCrc},
    page::{self, Geometry, Kind, Val},
};

/// xorshift64: a seeded, reproducible stream for the randomized tests.
#[derive(Debug)]
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    /// The next value.
    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A value in `0..n`.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A length in `0..=max`.
    pub(crate) fn len_upto(&mut self, max: usize) -> usize {
        self.below(max as u64 + 1) as usize
    }
}

/// Prints the seed if the test panics while this is alive, so a failing run can be replayed.
#[derive(Debug)]
pub(crate) struct ReportSeed(pub(crate) u64);

impl Drop for ReportSeed {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("failing seed: {:#018x}", self.0);
        }
    }
}

/// An in-memory page store: page `p` is bytes `p·P..(p + 1)·P` of `data`, one buffer so an
/// overflow run is one slice (pages 0 and 1 stand for the metas and are never tree pages). It is
/// the writer's store ([`PageWrite`]) for transaction `alloc.txn()`; [`Self::commit_like`] stamps
/// the private pages' CRCs as a commit would, so the tree code meets published pages (which must
/// carry a CRC) and copies them.
#[derive(Debug)]
pub(crate) struct MemStore {
    geometry: Geometry,
    data: Vec<u8>,
    /// The writer's allocator (its failure injection is `alloc.fail_allocs_after`).
    pub(crate) alloc: Allocator,
    /// Published pages freed by committed transactions, each with the id of the transaction that
    /// freed it: unreachable from that transaction's snapshot on, but intact (no reuse) until
    /// [`Self::recycle`] or [`Self::recycle_upto`].
    retired: Vec<(u64, u32)>,
    /// The pool and page count at the start of the writing transaction, which
    /// [`Self::abort_like`] returns to.
    base: (Vec<u32>, u32),
    /// The writer's reused buffers ([`PageWrite::take_scratch`]).
    scratch: Scratch,
    /// Deletes do not merge under-full pages ([`PageWrite::no_merge`]).
    pub(crate) no_merge: bool,
    /// What the last delete that merged, or read a sibling that did not fit, reported
    /// ([`PageWrite::merges_applied`]); any other delete leaves it as it was, so a test reading
    /// it for one delete resets it first.
    pub(crate) merge_report: MergeReport,
}

impl MemStore {
    /// An empty store of `page_size`-byte pages, writing transaction 1.
    pub(crate) fn new(page_size: u32) -> Self {
        let geometry = Geometry::new(page_size).expect("page size");
        Self {
            geometry,
            data: vec![0; 2 * geometry.page_size()],
            alloc: Allocator::new(1, 2),
            retired: Vec::new(),
            base: (Vec::new(), 2),
            scratch: Scratch::default(),
            no_merge: false,
            merge_report: MergeReport::default(),
        }
    }

    /// Commit as the environment will: stamp every private page's CRC and one over each private
    /// run, set the freed pages aside (intact, not reused), and start the next transaction.
    pub(crate) fn commit_like(&mut self) {
        let size = self.geometry.page_size();
        for (p, n) in self.alloc.stamp_private().units() {
            crc::add_crc32_nonzero(self.raw_span_mut(p, n as usize * size));
        }
        let txn = self.alloc.txn();
        self.retired.extend(self.alloc.take_freed().into_iter().map(|p| (txn, p)));
        self.alloc.begin_txn(txn + 1);
        self.set_base();
    }

    /// Make every page freed by committed transactions reusable (as if no reader could reach
    /// them any more).
    pub(crate) fn recycle(&mut self) {
        self.recycle_upto(u64::MAX);
    }

    /// Make the pages freed by committed transactions up to `gate` reusable: those no snapshot
    /// of txn `gate` or later can reach (the oldest live reader's snapshot txn, say).
    pub(crate) fn recycle_upto(&mut self, gate: u64) {
        let (ready, kept): (Vec<_>, Vec<_>) = self.retired.drain(..).partition(|&(t, _)| t <= gate);
        self.retired = kept;
        self.alloc.add_to_pool(ready.into_iter().map(|(_, p)| p));
        self.set_base();
    }

    /// Commit, then forget what this writer stamped, as a reopened file would: every page is
    /// then one read from disk, verified before its first copy. No reader survives a reopen.
    pub(crate) fn reopen(&mut self) {
        self.commit_like();
        self.retired.extend(self.alloc.pool().iter().map(|&p| (0, p)));
        self.alloc = Allocator::new(self.alloc.txn(), self.alloc.page_count());
        self.set_base();
    }

    /// Abort the writing transaction as the environment will: its private pages become
    /// unreferenced, the pages it freed stay in their published trees, and the pool and page
    /// count return to where the transaction began (pages past it are file slack). The caller
    /// goes back to its published root handles. The writer also forgets what it stamped (as
    /// [`Self::reopen`] does), which only costs re-verification.
    pub(crate) fn abort_like(&mut self) {
        let (pool, page_count) = self.base.clone();
        self.alloc = Allocator::new(self.alloc.txn(), page_count);
        self.alloc.add_to_pool(pool);
    }

    /// Record the pool and page count the writing transaction starts from.
    fn set_base(&mut self) {
        self.base = (self.alloc.pool().to_vec(), self.alloc.page_count());
    }

    /// A reader's view of the last commit: snapshot txn `alloc.txn() - 1`, no private pages.
    pub(crate) fn reader(&self) -> ReadView<'_> {
        self.reader_at(self.alloc.txn() - 1)
    }

    /// A reader's view of the snapshot of committed txn `txn` (a page written later is damage to
    /// it, so a page reused while the snapshot could still reach it surfaces as `Corrupt`).
    pub(crate) fn reader_at(&self, txn: u64) -> ReadView<'_> {
        ReadView { store: self, txn }
    }

    /// Page `p`'s bytes, unchecked.
    pub(crate) fn raw(&self, p: u32) -> &[u8] {
        self.span(p, self.geometry.page_size()).expect("a stored page")
    }

    /// The `len` bytes from page `p`, if stored.
    fn span(&self, p: u32, len: usize) -> Option<&[u8]> {
        let start = p as usize * self.geometry.page_size();
        self.data.get(start..start.checked_add(len)?)
    }

    /// Page `p`'s bytes, unchecked and writable (to inject damage). Past the page count it is
    /// file slack (created on demand), which no fetch may return.
    pub(crate) fn raw_mut(&mut self, p: u32) -> &mut [u8] {
        let size = self.geometry.page_size();
        self.raw_span_mut(p, size)
    }

    /// The `len` bytes from page `p`, unchecked and writable; storage past the end is created on
    /// demand (file slack).
    pub(crate) fn raw_span_mut(&mut self, p: u32, len: usize) -> &mut [u8] {
        let start = p as usize * self.geometry.page_size();
        if self.data.len() < start + len {
            self.data.resize(start + len, 0);
        }
        &mut self.data[start..start + len]
    }

    /// Re-stamp page `p`'s CRC (to make injected damage CRC-valid).
    pub(crate) fn restamp(&mut self, p: u32) {
        crc::add_crc32_nonzero(self.raw_mut(p));
    }

    /// Check the page accounting after a commit (no private or freed pages pending): the pages
    /// reachable from `roots` (tree pages and the pages of the overflow runs their leaves name),
    /// the retired pages and the pool are disjoint and together are every tree page
    /// `2..page_count`; and each root counts its reachable pages by kind.
    pub(crate) fn assert_accounting(&self, roots: &[TreeRoot]) {
        for root in roots {
            let counted = self.counted(*root);
            assert_eq!(
                (root.branch_pages, root.leaf_pages, root.overflow_pages as usize),
                (counted.branch_pages, counted.leaf_pages, self.run_pgnos(root).len()),
                "{root:?}: the page counters (branch, leaf, overflow) against the pages reached"
            );
        }
        let mut all: Vec<u32> = roots
            .iter()
            .flat_map(|r| self.reachable_pgnos(r).into_iter().chain(self.run_pgnos(r)))
            .collect();
        all.extend(self.retired.iter().map(|&(_, p)| p));
        all.extend_from_slice(self.alloc.pool());
        all.sort_unstable();
        let expect: Vec<u32> = (2..self.alloc.page_count()).collect();
        assert_eq!(all, expect, "reachable + retired + pool must be every page, once");
    }

    /// `root` with its branch and leaf counters set to the pages reachable from it: the handle of
    /// a tree a test built page by page.
    pub(crate) fn counted(&self, root: TreeRoot) -> TreeRoot {
        let pages = self.reachable_pgnos(&root);
        let leaves =
            pages.iter().filter(|&&p| page::kind(self.raw(p)).ok() == Some(Kind::Leaf)).count();
        let count = |n: usize| u32::try_from(n).expect("a page count");
        TreeRoot { leaf_pages: count(leaves), branch_pages: count(pages.len() - leaves), ..root }
    }

    /// Every page reachable from `root` with its bytes, in walk order.
    pub(crate) fn reachable(&self, root: &TreeRoot) -> Vec<(u32, Vec<u8>)> {
        self.reachable_pgnos(root).into_iter().map(|p| (p, self.raw(p).to_vec())).collect()
    }

    /// Every page of the overflow runs the leaves of `root` name, unchecked.
    pub(crate) fn run_pgnos(&self, root: &TreeRoot) -> Vec<u32> {
        let mut pages = Vec::new();
        for p in self.reachable_pgnos(root) {
            let buf = self.raw(p);
            if page::kind(buf).ok() != Some(Kind::Leaf) {
                continue;
            }
            for i in 0..page::nkeys(buf) {
                match page::leaf_entry(buf, i).expect("entry").1 {
                    Val::Inline(_) => {}
                    Val::Overflow(ovf) => {
                        let n = run_pages(ovf.len as usize, self.geometry.page_size()) as u32;
                        pages.extend(ovf.pgno..ovf.pgno + n);
                    }
                    Val::Nested(root) => panic!("leaf {p}, cell {i}: a nested cell {root:?}"),
                }
            }
        }
        pages
    }

    /// Every page reachable from `root`, in walk order.
    pub(crate) fn reachable_pgnos(&self, root: &TreeRoot) -> Vec<u32> {
        let mut pages = Vec::new();
        let mut stack = if root.is_empty() { vec![] } else { vec![root.root] };
        while let Some(p) = stack.pop() {
            let buf = self.raw(p);
            if matches!(page::kind(buf), Ok(Kind::Branch)) {
                for i in 0..page::nkeys(buf) {
                    stack.push(page::branch_child(buf, i).expect("child").0);
                }
            }
            pages.push(p);
        }
        pages
    }
}

impl PageRead for MemStore {
    fn geometry(&self) -> Geometry {
        self.geometry
    }

    fn page_count(&self) -> u32 {
        self.alloc.page_count()
    }

    fn page(&self, pgno: u32) -> Result<&[u8]> {
        checked_page(
            pgno,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private(pgno),
            |p| self.span(p, self.geometry.page_size()),
        )
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        checked_child(
            pgno,
            want_txn,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private(pgno),
            |p| self.span(p, self.geometry.page_size()),
        )
    }

    fn run(&self, ovf: Ovf, verify: RunCrc) -> Result<&[u8]> {
        checked_run(
            ovf,
            self.geometry.page_size(),
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private_run(ovf.pgno),
            verify,
            |p, len| self.span(p, len),
        )
    }
}

impl PageWrite for MemStore {
    fn txn(&self) -> u64 {
        self.alloc.txn()
    }

    fn is_private(&self, pgno: u32) -> bool {
        self.alloc.is_private(pgno)
    }

    fn is_stamped(&self, pgno: u32) -> bool {
        self.alloc.is_stamped(pgno)
    }

    fn private_page(&self, pgno: u32) -> Result<&[u8]> {
        self.span(pgno, self.geometry.page_size())
            .ok_or_else(|| Error::Corrupt(format!("private page {pgno} is not stored")))
    }

    fn epoch(&self) -> u64 {
        self.alloc.epoch()
    }

    fn alloc(&mut self) -> Result<u32> {
        let (data, size) = (&mut self.data, self.geometry.page_size());
        let p = self.alloc.allocate(|count| {
            if data.len() < count as usize * size {
                data.resize(count as usize * size, 0);
            }
            Ok(())
        })?;
        #[cfg(debug_assertions)]
        super::poison(self.raw_mut(p));
        Ok(p)
    }

    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]> {
        if !self.alloc.is_private(pgno) {
            return Err(Error::Failed(format!("page {pgno} is not private: copy it first")));
        }
        Ok(self.raw_mut(pgno))
    }

    /// One `copy_within` of the page's range: the buffer has no readers to alias.
    fn copy_published(&mut self, from: u32, to: u32) -> Result<&mut [u8]> {
        if self.alloc.is_private(from) {
            return Err(Error::Failed(format!("page {from} is private: it needs no copy")));
        }
        self.page(from)?;
        self.page_mut(to)?;
        let size = self.geometry.page_size();
        let start = from as usize * size;
        self.data.copy_within(start..start + size, to as usize * size);
        Ok(self.raw_mut(to))
    }

    fn free(&mut self, pgno: u32) {
        self.alloc.free(pgno);
    }

    fn run_privacy(&self, pgno: u32, npages: u32) -> Option<bool> {
        self.alloc.run_privacy(pgno, npages)
    }

    fn alloc_run(&mut self, npages: u32) -> Result<u32> {
        let (data, size) = (&mut self.data, self.geometry.page_size());
        let p = self.alloc.allocate_run(npages, |count| {
            if data.len() < count as usize * size {
                data.resize(count as usize * size, 0);
            }
            Ok(())
        })?;
        #[cfg(debug_assertions)]
        super::poison(self.raw_span_mut(p, npages as usize * size));
        Ok(p)
    }

    fn run_mut(&mut self, pgno: u32, npages: u32) -> Result<&mut [u8]> {
        if self.alloc.run_privacy(pgno, npages) != Some(true) {
            return Err(Error::Failed(format!("run {pgno} is not private: it is immutable")));
        }
        Ok(self.raw_span_mut(pgno, npages as usize * self.geometry.page_size()))
    }

    fn free_run(&mut self, pgno: u32, npages: u32) {
        self.alloc.free_run(pgno, npages);
    }

    fn take_scratch(&mut self) -> Scratch {
        std::mem::take(&mut self.scratch)
    }

    fn put_scratch(&mut self, scratch: Scratch) {
        self.scratch = scratch;
    }

    fn no_merge(&self) -> bool {
        self.no_merge
    }

    fn merges_applied(&mut self, report: MergeReport) {
        self.merge_report = report;
    }
}

/// A reader's snapshot of a [`MemStore`] (see [`MemStore::reader`]).
#[derive(Debug)]
pub(crate) struct ReadView<'a> {
    store: &'a MemStore,
    txn: u64,
}

impl PageRead for ReadView<'_> {
    fn geometry(&self) -> Geometry {
        self.store.geometry
    }

    fn page_count(&self) -> u32 {
        self.store.alloc.page_count()
    }

    fn page(&self, pgno: u32) -> Result<&[u8]> {
        checked_page(pgno, self.store.alloc.page_count(), self.txn, false, |p| {
            self.store.span(p, self.store.geometry.page_size())
        })
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        checked_child(pgno, want_txn, self.store.alloc.page_count(), self.txn, false, |p| {
            self.store.span(p, self.store.geometry.page_size())
        })
    }

    fn run(&self, ovf: Ovf, verify: RunCrc) -> Result<&[u8]> {
        let size = self.store.geometry.page_size();
        checked_run(ovf, size, self.store.alloc.page_count(), self.txn, false, verify, |p, len| {
            self.store.span(p, len)
        })
    }
}

/// What [`walk_tree`] found.
#[derive(Debug, Default)]
pub(crate) struct Walk {
    /// Every entry, in key order.
    pub(crate) entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Every page reached.
    pub(crate) pages: Vec<u32>,
    /// The leaves, left to right.
    pub(crate) leaves: Vec<u32>,
    /// The pages of the overflow runs the leaves name.
    pub(crate) overflow_pages: usize,
    /// The keys of a packing walk's inline cells (packed sets), each also in `entries`.
    pub(crate) packed: std::collections::BTreeSet<Vec<u8>>,
}

/// A walk of one tree, checking its structure (see [`walk_tree`]).
struct Walker<'s, S: ?Sized> {
    store: &'s S,
    geometry: Geometry,
    height: u32,
    /// A dup table's outer tree: every cell a nested root, its 32 raw bytes the entry's value.
    outer: bool,
    /// An outer tree that packs: an inline cell too, its bytes the entry's value.
    packing: bool,
    seen: HashSet<u32>,
    walk: Walk,
}

impl<'s, S: PageRead + ?Sized> Walker<'s, S> {
    /// Visit page `p` at `depth` (the root is at 1), whose header txn must be `want_txn` (low 32
    /// bits) and whose keys must lie in `lo..hi` (the range its ancestors give it).
    fn visit(
        &mut self,
        p: u32,
        want_txn: u32,
        depth: u32,
        lo: Option<&'s [u8]>,
        hi: Option<&'s [u8]>,
    ) {
        assert!(self.seen.insert(p), "page {p} is reached twice");
        self.walk.pages.push(p);
        let buf = self.store.page(p).unwrap_or_else(|e| panic!("page {p} is unreadable: {e}"));
        page::validate(buf, &self.geometry).unwrap_or_else(|e| panic!("page {p}: {e}"));
        assert_eq!(page::txn(buf) as u32, want_txn, "page {p}: txn is not its parent's child_txn");
        let state = crc::crc_state(buf);
        assert!(state != CrcState::Corrupt, "page {p} has a wrong CRC");
        let in_range = |k: &[u8]| lo.is_none_or(|lo| k >= lo) && hi.is_none_or(|hi| k < hi);
        let n = page::nkeys(buf);
        if page::kind(buf).expect("kind") == Kind::Leaf {
            assert_eq!(depth, self.height, "leaf {p} is at depth {depth}");
            assert!(n > 0 || depth == 1, "non-root leaf {p} is empty");
            for i in 0..n {
                let (k, v) = page::leaf_entry(buf, i).expect("entry");
                assert!(in_range(k), "leaf {p}: key {i} is outside its parents' range");
                if let Some((last, _)) = self.walk.entries.last() {
                    assert!(last.as_slice() < k, "leaf {p}: key {i} is not above the last key");
                }
                match v {
                    // The codec gives `Nested` for a cell of flags 2 with a 32-byte value only.
                    Val::Nested(root) if self.outer => {
                        self.walk.entries.push((k.to_vec(), root.to_vec()));
                        continue;
                    }
                    Val::Inline(set) if self.packing => {
                        self.walk.packed.insert(k.to_vec());
                        self.walk.entries.push((k.to_vec(), set.to_vec()));
                        continue;
                    }
                    other if self.outer => panic!("leaf {p}, key {i}: an outer cell {other:?}"),
                    Val::Inline(_) => {}
                    Val::Overflow(ovf) => {
                        let n = run_pages(ovf.len as usize, self.geometry.page_size()) as u32;
                        for q in ovf.pgno..ovf.pgno + n {
                            assert!(self.seen.insert(q), "leaf {p}: run page {q} is reached twice");
                        }
                        self.walk.overflow_pages += n as usize;
                    }
                    Val::Nested(root) => panic!("leaf {p}, key {i}: a nested cell {root:?}"),
                }
                let v = self.store.value(v).unwrap_or_else(|e| panic!("leaf {p}, key {i}: {e}"));
                self.walk.entries.push((k.to_vec(), v.to_vec()));
            }
            self.walk.leaves.push(p);
            return;
        }
        assert!(depth < self.height, "branch {p} at depth {depth} of height {}", self.height);
        assert!(n >= 2 || depth > 1, "root branch {p} has a single child");
        let keys: Vec<&'s [u8]> = (0..n).map(|i| page::branch_key(buf, i).expect("key")).collect();
        for (i, k) in keys.iter().enumerate().skip(1) {
            assert!(in_range(k), "branch {p}: key {i} is outside its parents' range");
        }
        for i in 0..n {
            let (child, child_txn) = page::branch_child(buf, i).expect("child");
            let lo = if i == 0 { lo } else { Some(keys[i]) };
            let hi = if i + 1 == n { hi } else { Some(keys[i + 1]) };
            self.visit(child, child_txn, depth + 1, lo, hi);
        }
    }
}

/// Walk every page of the tree `root` names and return what it holds. Panics unless: every page
/// is readable through `store`'s checked fetch, passes [`page::validate`], carries the txn its
/// parent's cell (or the root handle) names, has a valid or dirty (never a wrong) CRC, and is
/// reached once; every leaf is at depth `height` and every branch above it; only a root leaf is
/// empty and a root branch has two children or more; every key lies in the range its ancestors'
/// separators give it and keys ascend strictly across the tree; every overflow value reads back
/// through the store's checked run read, and no run page is reached twice; and the entry count
/// and the page counters (branch, leaf, overflow) match.
pub(crate) fn walk_tree<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Walk {
    walk(store, root, false, false)
}

/// [`walk_tree`] of a dup table's outer tree: every cell must be a nested cell (flags 2, a
/// 32-byte value), and each entry's value is its cell's raw root bytes. What the cells name is
/// not walked.
pub(crate) fn walk_outer_tree<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Walk {
    walk(store, root, true, false)
}

/// [`walk_outer_tree`] of an outer tree that packs: every cell a nested cell or an inline cell
/// (a packed set, its bytes the entry's value, not parsed).
pub(crate) fn walk_packing_tree<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Walk {
    walk(store, root, true, true)
}

/// [`walk_tree`], [`walk_outer_tree`] or [`walk_packing_tree`].
fn walk<S: PageRead + ?Sized>(store: &S, root: &TreeRoot, outer: bool, packing: bool) -> Walk {
    root.check().expect("root shape");
    let mut walker = Walker {
        store,
        geometry: store.geometry(),
        height: root.height,
        outer,
        packing,
        seen: HashSet::new(),
        walk: Walk::default(),
    };
    if !root.is_empty() {
        walker.visit(root.root, root.root_txn, 1, None, None);
    }
    let walk = walker.walk;
    assert_eq!(walk.entries.len() as u64, root.entries, "the root's entry count");
    let (leaves, branches) = (walk.leaves.len(), walk.pages.len() - walk.leaves.len());
    assert_eq!(
        (root.branch_pages as usize, root.leaf_pages as usize, root.overflow_pages as usize),
        (branches, leaves, walk.overflow_pages),
        "the root's page counters (branch, leaf, overflow) against the pages walked"
    );
    walk
}

/// The fraction of the leaves' usable bytes their entries fill.
pub(crate) fn leaf_fill<S: PageRead + ?Sized>(store: &S, leaves: &[u32]) -> f64 {
    let usable = store.geometry().usable();
    let used: usize = leaves
        .iter()
        .map(|&p| usable - page::free_space(store.page(p).expect("leaf")).expect("free"))
        .sum();
    used as f64 / (leaves.len() * usable) as f64
}

/// A hash of a page's kind, cell count and outer keys (the first and last keys of a leaf; key
/// 1 and the last of a branch, whose cell 0 has no key). A page only copied keeps it; a page that
/// lost a cell or absorbed a sibling does not, nor does it match another page of its level (keys
/// and separators are unique within a level), save single-child branches, which all look alike.
fn key_list_hash(buf: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let n = page::nkeys(buf);
    let key = |i: usize| match page::kind(buf).expect("kind") {
        Kind::Leaf => page::leaf_entry(buf, i).expect("entry").0,
        Kind::Branch => page::branch_key(buf, i).expect("key"),
    };
    (page::kind(buf).ok() == Some(Kind::Leaf), n).hash(&mut h);
    if n > 0 {
        (key(1.min(n - 1)), key(n - 1)).hash(&mut h);
    }
    h.finish()
}

/// The pages of `root`'s levels, the root first.
fn levels<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Vec<Vec<u32>> {
    let mut levels = Vec::new();
    let mut level = if root.is_empty() { vec![] } else { vec![root.root] };
    for depth in 1..=root.height {
        let mut next = Vec::new();
        if depth < root.height {
            for &p in &level {
                let buf = store.page(p).expect("branch");
                next.extend(
                    (0..page::nkeys(buf)).map(|i| page::branch_child(buf, i).expect("c").0),
                );
            }
        }
        levels.push(std::mem::replace(&mut level, next));
    }
    levels
}

/// The key lists of `root`'s pages ([`key_list_hash`]), level by level from the leaves up
/// (index 0: the leaves), for [`merge_open_pages`].
pub(crate) fn level_keys<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Vec<HashSet<u64>> {
    let mut levels = levels(store, root);
    levels.reverse();
    levels
        .iter()
        .map(|level| level.iter().map(|&p| key_list_hash(store.page(p).expect("page"))).collect())
        .collect()
}

/// The non-root pages of `root` that hold under a quarter of `U` beside a same-parent sibling
/// they would fit into: the merged page's cells and slots, `used + used(sibling)`, plus for
/// branches the parent's key for the right page of the pair (it comes down as that page's cell-0
/// key), within `U`. With `before` ([`level_keys`] of the tree before a delete), only the pages
/// the delete changed count (a key list new at its level); with an empty `before`, every page.
/// After a delete with merging on, the changed pages are merge-closed: this is empty.
pub(crate) fn merge_open_pages<S: PageRead + ?Sized>(
    store: &S,
    root: &TreeRoot,
    before: &[HashSet<u64>],
) -> Vec<u32> {
    let usable = store.geometry().usable();
    let used = |p: u32| usable - page::free_space(store.page(p).expect("page")).expect("free");
    let mut open = Vec::new();
    let levels = levels(store, root);
    for (depth, level) in levels.iter().enumerate().take(levels.len().saturating_sub(1)) {
        // The level index of the children, counted from the leaves.
        let below = levels.len() - depth - 2;
        for &p in level {
            let buf = store.page(p).expect("branch");
            let n = page::nkeys(buf);
            let kids: Vec<u32> = (0..n).map(|i| page::branch_child(buf, i).expect("c").0).collect();
            for (i, &c) in kids.iter().enumerate() {
                let cb = store.page(c).expect("child");
                let changed = before.get(below).is_none_or(|set| !set.contains(&key_list_hash(cb)));
                if !changed || 4 * used(c) >= usable {
                    continue;
                }
                let leaf = page::kind(cb).ok() == Some(Kind::Leaf);
                let fits = |j: usize, right: usize| {
                    let sep =
                        if leaf { 0 } else { page::branch_key(buf, right).expect("key").len() };
                    used(c) + used(kids[j]) + sep <= usable
                };
                if (i > 0 && fits(i - 1, i)) || (i + 1 < n && fits(i + 1, i + 1)) {
                    open.push(c);
                }
            }
        }
    }
    open
}

/// The fraction of the usable bytes of the tree's pages (all of them, or only its leaves) their
/// cells and slots fill.
pub(crate) fn page_fill<S: PageRead + ?Sized>(
    store: &S,
    root: &TreeRoot,
    leaves_only: bool,
) -> f64 {
    let levels = levels(store, root);
    let pages: Vec<u32> =
        if leaves_only { levels.last().cloned().unwrap_or_default() } else { levels.concat() };
    let usable = store.geometry().usable();
    let used: usize = pages
        .iter()
        .map(|&p| usable - page::free_space(store.page(p).expect("page")).expect("free"))
        .sum();
    used as f64 / (pages.len().max(1) * usable) as f64
}
