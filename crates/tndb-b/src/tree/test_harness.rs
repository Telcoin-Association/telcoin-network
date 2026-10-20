//! Test harness for the tree layer: an in-memory page store ([`MemStore`]), a structural walker
//! ([`walk_tree`]), and a seeded random stream ([`Rng`], [`ReportSeed`]).

use std::collections::HashSet;

use super::{
    read::{checked_child, checked_page},
    write::Scratch,
    PageRead, PageWrite, TreeRoot,
};
use crate::{
    alloc::Allocator,
    crc::{self, CrcState},
    error::{Error, Result},
    page::{self, Geometry, Kind},
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

/// An in-memory page store: page `p` is `pages[p]` (pages 0 and 1 stand for the metas and are
/// never tree pages). It is the writer's store ([`PageWrite`]) for transaction `alloc.txn()`;
/// [`Self::commit_like`] stamps the private pages' CRCs as a commit would, so the tree code meets
/// published pages (which must carry a CRC) and copies them.
#[derive(Debug)]
pub(crate) struct MemStore {
    geometry: Geometry,
    pages: Vec<Vec<u8>>,
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
}

impl MemStore {
    /// An empty store of `page_size`-byte pages, writing transaction 1.
    pub(crate) fn new(page_size: u32) -> Self {
        let geometry = Geometry::new(page_size).expect("page size");
        Self {
            geometry,
            pages: vec![vec![0; geometry.page_size()]; 2],
            alloc: Allocator::new(1, 2),
            retired: Vec::new(),
            base: (Vec::new(), 2),
            scratch: Scratch::default(),
        }
    }

    /// Commit as the environment will: stamp every private page's CRC, set the freed pages
    /// aside (intact, not reused), and start the next transaction.
    pub(crate) fn commit_like(&mut self) {
        for p in self.alloc.stamp_private() {
            crc::add_crc32_nonzero(&mut self.pages[p as usize]);
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
        &self.pages[p as usize]
    }

    /// Page `p`'s bytes, unchecked and writable (to inject damage). Past the page count it is
    /// file slack (created on demand), which no fetch may return.
    pub(crate) fn raw_mut(&mut self, p: u32) -> &mut [u8] {
        if p as usize >= self.pages.len() {
            self.pages.resize(p as usize + 1, vec![0; self.geometry.page_size()]);
        }
        &mut self.pages[p as usize]
    }

    /// Re-stamp page `p`'s CRC (to make injected damage CRC-valid).
    pub(crate) fn restamp(&mut self, p: u32) {
        crc::add_crc32_nonzero(&mut self.pages[p as usize]);
    }

    /// Check the page accounting after a commit (no private or freed pages pending): the pages
    /// reachable from `roots`, the retired pages and the pool are disjoint and together are every
    /// tree page `2..page_count`.
    pub(crate) fn assert_accounting(&self, roots: &[TreeRoot]) {
        let mut all: Vec<u32> = roots.iter().flat_map(|r| self.reachable_pgnos(r)).collect();
        all.extend(self.retired.iter().map(|&(_, p)| p));
        all.extend_from_slice(self.alloc.pool());
        all.sort_unstable();
        let expect: Vec<u32> = (2..self.alloc.page_count()).collect();
        assert_eq!(all, expect, "reachable + retired + pool must be every page, once");
    }

    /// Every page reachable from `root` with its bytes, in walk order.
    pub(crate) fn reachable(&self, root: &TreeRoot) -> Vec<(u32, Vec<u8>)> {
        self.reachable_pgnos(root).into_iter().map(|p| (p, self.raw(p).to_vec())).collect()
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
            |p| self.pages.get(p as usize).map(Vec::as_slice),
        )
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        checked_child(
            pgno,
            want_txn,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private(pgno),
            |p| self.pages.get(p as usize).map(Vec::as_slice),
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
        self.pages
            .get(pgno as usize)
            .map(Vec::as_slice)
            .ok_or_else(|| Error::Corrupt(format!("private page {pgno} is not stored")))
    }

    fn epoch(&self) -> u64 {
        self.alloc.epoch()
    }

    fn alloc(&mut self) -> Result<u32> {
        let (pages, size) = (&mut self.pages, self.geometry.page_size());
        let p = self.alloc.allocate(|count| {
            if pages.len() < count as usize {
                pages.resize(count as usize, vec![0; size]);
            }
            Ok(())
        })?;
        self.pages[p as usize].fill(0);
        Ok(p)
    }

    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]> {
        if !self.alloc.is_private(pgno) {
            return Err(Error::Failed(format!("page {pgno} is not private: copy it first")));
        }
        Ok(&mut self.pages[pgno as usize])
    }

    fn free(&mut self, pgno: u32) {
        self.alloc.free(pgno);
    }

    fn take_scratch(&mut self) -> Scratch {
        std::mem::take(&mut self.scratch)
    }

    fn put_scratch(&mut self, scratch: Scratch) {
        self.scratch = scratch;
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
            self.store.pages.get(p as usize).map(Vec::as_slice)
        })
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        checked_child(pgno, want_txn, self.store.alloc.page_count(), self.txn, false, |p| {
            self.store.pages.get(p as usize).map(Vec::as_slice)
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
}

/// A walk of one tree, checking its structure (see [`walk_tree`]).
struct Walker<'s, S: ?Sized> {
    store: &'s S,
    geometry: Geometry,
    height: u32,
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
/// separators give it and keys ascend strictly across the tree; and the entry count matches.
pub(crate) fn walk_tree<S: PageRead + ?Sized>(store: &S, root: &TreeRoot) -> Walk {
    root.check().expect("root shape");
    let mut walker = Walker {
        store,
        geometry: store.geometry(),
        height: root.height,
        seen: HashSet::new(),
        walk: Walk::default(),
    };
    if !root.is_empty() {
        walker.visit(root.root, root.root_txn, 1, None, None);
    }
    assert_eq!(walker.walk.entries.len() as u64, root.entries, "the root's entry count");
    walker.walk
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
