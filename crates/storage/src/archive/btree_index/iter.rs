//! Sorted iteration over a [`BtreeIndex`]: forward, reverse, bounded ranges, and key prefixes.
//!
//! The scan state is a [`BtreeCursor`]: the path from the root to the current leaf (a fixed stack
//! of `(page, slot)`), detached from the tree — each step takes a [`PageSource`] as an argument.
//! Leaves are not linked (a copy-on-write tree cannot keep sibling links: copying a leaf would
//! force copying its neighbors), so a cursor moves between leaves through their parents. The same
//! cursor runs over the writer's tree ([`BtreeIter`], bound to a `&BtreeIndex`) and over a
//! published, immutable snapshot read without a lock. Keys come back borrowed from the page source;
//! a fetch/CRC failure is a terminal `Err`.

use std::ops::{Bound, RangeBounds};

use crate::archive::{
    btree_index::{index::BtreeIndex, page::Node},
    error::fetch::FetchError,
};

/// Hard cap on tree height (a cursor's path length and every descent), a corruption tripwire:
/// real heights are tiny (at a branching factor of dozens, even 2^48 keys stay well under this).
pub(crate) const MAX_DEPTH: usize = 48;

/// Where a cursor reads pages from: the writer's tree or a published snapshot.
pub(crate) trait PageSource {
    /// The page geometry (key size) used to decode pages.
    fn node(&self) -> Node;
    /// The tree's root page.
    fn root(&self) -> u32;
    /// Borrow page `p` (bounds- and corruption-checked by the source).
    fn page(&self, p: u32) -> Result<&[u8], FetchError>;
}

/// The position of a sorted scan over a B-tree: the path from the root to the current leaf, as
/// `(page, slot)` entries. For an internal level the slot is the child being visited; for the leaf
/// it is the next entry to yield (forward) or the count of entries still to yield (reverse).
#[derive(Debug)]
pub(crate) struct BtreeCursor {
    path: [(u32, usize); MAX_DEPTH],
    /// Entries in `path` in use; 0 once the scan is exhausted or failed.
    depth: usize,
    reverse: bool,
    /// Inclusive/exclusive lower bound (the stop bound in reverse, start bound in forward).
    lower: Bound<Vec<u8>>,
    /// Inclusive/exclusive upper bound (the stop bound in forward, start bound in reverse).
    upper: Bound<Vec<u8>>,
}

/// The tree is deeper than [`MAX_DEPTH`] or a child pointer loops: corruption, not a miss.
fn too_deep() -> FetchError {
    FetchError::CorruptIndex("btree descent exceeded max depth".to_string())
}

impl BtreeCursor {
    /// Position a cursor at the first entry, in scan order, within the bounds.
    pub(crate) fn new<S: PageSource + ?Sized>(
        src: &S,
        reverse: bool,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Result<Self, FetchError> {
        let mut cursor = Self { path: [(0, 0); MAX_DEPTH], depth: 0, reverse, lower, upper };
        cursor.start(src)?;
        Ok(cursor)
    }

    fn push(&mut self, page: u32, slot: usize) -> Result<(), FetchError> {
        if self.depth == MAX_DEPTH {
            return Err(too_deep());
        }
        self.path[self.depth] = (page, slot);
        self.depth += 1;
        Ok(())
    }

    /// Descend from the root to the leaf holding the start of the scan.
    fn start<S: PageSource + ?Sized>(&mut self, src: &S) -> Result<(), FetchError> {
        let node = src.node();
        let bound = if self.reverse { &self.upper } else { &self.lower };
        let bound = match bound {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
            Bound::Unbounded => None,
        };
        let mut p = src.root();
        loop {
            let buf = src.page(p)?;
            if node.is_leaf(buf) {
                let n = node.entry_count(buf);
                let slot = if self.reverse {
                    // The count of entries <= (or <) the upper bound, still to yield.
                    match &self.upper {
                        Bound::Unbounded => n,
                        Bound::Included(hi) => match node.leaf_search(buf, hi) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        },
                        Bound::Excluded(hi) => match node.leaf_search(buf, hi) {
                            Ok(i) | Err(i) => i,
                        },
                    }
                } else {
                    // The first entry >= (or >) the lower bound.
                    match &self.lower {
                        Bound::Unbounded => 0,
                        Bound::Included(lo) => match node.leaf_search(buf, lo) {
                            Ok(i) | Err(i) => i,
                        },
                        Bound::Excluded(lo) => match node.leaf_search(buf, lo) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        },
                    }
                };
                return self.push(p, slot);
            }
            let ci = match &bound {
                Some(k) => node.internal_child_index(buf, k),
                None if self.reverse => node.entry_count(buf),
                None => 0,
            };
            let child = node.internal_child(buf, ci);
            self.push(p, ci)?;
            p = child;
        }
    }

    /// Descend from `p` along the leftmost (forward) or rightmost (reverse) edge to a leaf.
    fn descend_edge<S: PageSource + ?Sized>(
        &mut self,
        src: &S,
        mut p: u32,
    ) -> Result<(), FetchError> {
        let node = src.node();
        loop {
            let buf = src.page(p)?;
            let n = node.entry_count(buf);
            if node.is_leaf(buf) {
                return self.push(p, if self.reverse { n } else { 0 });
            }
            let ci = if self.reverse { n } else { 0 };
            let child = node.internal_child(buf, ci);
            self.push(p, ci)?;
            p = child;
        }
    }

    /// Move to the next leaf in scan order through the parents; `false` once there is none.
    fn next_leaf<S: PageSource + ?Sized>(&mut self, src: &S) -> Result<bool, FetchError> {
        let node = src.node();
        self.depth -= 1; // leave the exhausted leaf
        while self.depth > 0 {
            let (p, ci) = self.path[self.depth - 1];
            let buf = src.page(p)?;
            let next = if self.reverse {
                ci.checked_sub(1)
            } else {
                (ci < node.entry_count(buf)).then_some(ci + 1)
            };
            match next {
                Some(ci) => {
                    self.path[self.depth - 1].1 = ci;
                    let child = node.internal_child(buf, ci);
                    self.descend_edge(src, child)?;
                    return Ok(true);
                }
                None => self.depth -= 1,
            }
        }
        Ok(false)
    }

    /// The next `(key, position)` in scan order (the key borrowed from the page source), `None`
    /// once the scan is exhausted, or a terminal `Err` on a fetch failure.
    pub(crate) fn next<'i, S: PageSource + ?Sized>(
        &mut self,
        src: &'i S,
    ) -> Option<Result<(&'i [u8], u64), FetchError>> {
        match self.step(src) {
            Ok(item) => item.map(Ok),
            Err(e) => {
                self.depth = 0;
                Some(Err(e))
            }
        }
    }

    fn step<'i, S: PageSource + ?Sized>(
        &mut self,
        src: &'i S,
    ) -> Result<Option<(&'i [u8], u64)>, FetchError> {
        let node = src.node();
        loop {
            if self.depth == 0 {
                return Ok(None);
            }
            let (leaf, slot) = self.path[self.depth - 1];
            let buf = src.page(leaf)?;
            let n = node.entry_count(buf);
            let i = if self.reverse { slot.checked_sub(1) } else { (slot < n).then_some(slot) };
            let Some(i) = i.filter(|&i| i < n) else {
                if !self.next_leaf(src)? {
                    return Ok(None);
                }
                continue;
            };
            let key = node.leaf_key(buf, i);
            let stop = if self.reverse {
                match &self.lower {
                    Bound::Unbounded => false,
                    Bound::Included(lo) => key < lo.as_slice(),
                    Bound::Excluded(lo) => key <= lo.as_slice(),
                }
            } else {
                match &self.upper {
                    Bound::Unbounded => false,
                    Bound::Included(hi) => key > hi.as_slice(),
                    Bound::Excluded(hi) => key >= hi.as_slice(),
                }
            };
            if stop {
                self.depth = 0;
                return Ok(None);
            }
            self.path[self.depth - 1].1 = if self.reverse { i } else { i + 1 };
            return Ok(Some((key, node.leaf_value(buf, i))));
        }
    }
}

/// A sorted iterator over `(key, position)` entries of a [`BtreeIndex`]: a [`BtreeCursor`] bound
/// to a borrow of the index (which keeps the tree unchanged for the iterator's life). It reads the
/// writer's current tree, including writes not yet published.
///
/// Created by [`BtreeIndex::iter`], [`BtreeIndex::rev_iter`], [`BtreeIndex::range`],
/// [`BtreeIndex::rev_range`], and [`BtreeIndex::prefix`].
#[derive(Debug)]
pub struct BtreeIter<'a> {
    index: &'a BtreeIndex,
    cursor: BtreeCursor,
}

impl<'a> BtreeIter<'a> {
    fn new(
        index: &'a BtreeIndex,
        reverse: bool,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Result<Self, FetchError> {
        Ok(Self { index, cursor: BtreeCursor::new(index, reverse, lower, upper)? })
    }
}

impl Iterator for BtreeIter<'_> {
    type Item = Result<(Vec<u8>, u64), FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next(self.index).map(|item| item.map(|(key, pos)| (key.to_vec(), pos)))
    }
}

impl BtreeIndex {
    /// Ascending iterator over all `(key, position)` entries.
    pub fn iter(&self) -> Result<BtreeIter<'_>, FetchError> {
        BtreeIter::new(self, false, Bound::Unbounded, Bound::Unbounded)
    }

    /// Descending iterator over all `(key, position)` entries.
    pub fn rev_iter(&self) -> Result<BtreeIter<'_>, FetchError> {
        BtreeIter::new(self, true, Bound::Unbounded, Bound::Unbounded)
    }

    /// Ascending iterator over the entries whose keys fall within `bounds`.
    ///
    /// The bound element may be any `AsRef<[u8]>` (e.g. `[u8; 32]` or `Vec<u8>`).  A fully
    /// unbounded `..` needs the element type spelled out (e.g. `range::<[u8; 32], _>(..)`) or use
    /// [`BtreeIndex::iter`].
    pub fn range<T: AsRef<[u8]>, R: RangeBounds<T>>(
        &self,
        bounds: R,
    ) -> Result<BtreeIter<'_>, FetchError> {
        let (lower, upper) = clone_bounds(&bounds);
        BtreeIter::new(self, false, lower, upper)
    }

    /// Descending iterator over the entries whose keys fall within `bounds`.
    pub fn rev_range<T: AsRef<[u8]>, R: RangeBounds<T>>(
        &self,
        bounds: R,
    ) -> Result<BtreeIter<'_>, FetchError> {
        let (lower, upper) = clone_bounds(&bounds);
        BtreeIter::new(self, true, lower, upper)
    }

    /// Ascending iterator over all keys sharing the given byte `prefix` (a prefix longer than
    /// `ksize()` is truncated to `ksize()`).
    pub fn prefix(&mut self, prefix: &[u8]) -> Result<BtreeIter<'_>, FetchError> {
        let ksize = self.ksize();
        let plen = prefix.len().min(ksize);
        // Lower bound: the prefix padded with zero bytes (smallest key with this prefix).
        let mut lo = vec![0_u8; ksize];
        lo[..plen].copy_from_slice(&prefix[..plen]);
        // Upper bound: increment the last non-0xFF prefix byte; all-0xFF means unbounded.
        let mut hi = vec![0_u8; ksize];
        hi[..plen].copy_from_slice(&prefix[..plen]);
        let mut i = plen;
        let upper = loop {
            if i == 0 {
                break Bound::Unbounded;
            }
            i -= 1;
            if hi[i] != 0xFF {
                hi[i] += 1;
                for b in hi.iter_mut().take(plen).skip(i + 1) {
                    *b = 0;
                }
                break Bound::Excluded(hi);
            }
        };
        BtreeIter::new(self, false, Bound::Included(lo), upper)
    }
}

/// Copy the (possibly borrowed) bounds of a range into owned `Bound<Vec<u8>>` values.
fn clone_bounds<T: AsRef<[u8]>, R: RangeBounds<T>>(bounds: &R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let map = |b: Bound<&T>| match b {
        Bound::Included(k) => Bound::Included(k.as_ref().to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (map(bounds.start_bound()), map(bounds.end_bound()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::archive::pack::{DataHeader, PackCompression};

    /// 32-byte big-endian key so lexicographic order equals numeric order.
    fn bkey(i: u64) -> [u8; 32] {
        let mut k = [0_u8; 32];
        k[24..32].copy_from_slice(&i.to_be_bytes());
        k
    }

    /// 32-byte key in prefix group `g` (first byte) with numeric suffix `i`.
    fn gkey(g: u8, i: u64) -> [u8; 32] {
        let mut k = [0_u8; 32];
        k[0] = g;
        k[24..32].copy_from_slice(&i.to_be_bytes());
        k
    }

    fn open(dir: &std::path::Path) -> BtreeIndex {
        let data_header = DataHeader::new(0, PackCompression::ZStd, 0);
        BtreeIndex::open_btx_file(dir, &data_header, 32, false).expect("open")
    }

    #[test]
    fn test_archive_btx_iteration_sorted() {
        let tmp = TempDir::with_prefix("test_archive_btx_iter").expect("temp dir");
        let mut idx = open(&tmp.path().join("idx"));

        // Insert in reverse order to prove ordering is a tree invariant, not insertion luck.
        let n = 10_000u64;
        for i in (0..n).rev() {
            idx.save(&bkey(i), i).expect("save");
        }
        assert_eq!(idx.len() as u64, n);

        // Forward iteration yields every entry in ascending key order.
        let forward: Vec<(u64, u64)> = idx
            .iter()
            .expect("iter")
            .map(|r| {
                let (k, v) = r.expect("item");
                (u64::from_be_bytes(k[24..32].try_into().unwrap()), v)
            })
            .collect();
        assert_eq!(forward.len() as u64, n);
        for (i, (k, v)) in forward.iter().enumerate() {
            assert_eq!(*k, i as u64, "key out of order at {i}");
            assert_eq!(*v, i as u64, "value mismatch at {i}");
        }

        // Reverse iteration yields the same entries in descending order.
        let reverse: Vec<u64> = idx
            .rev_iter()
            .expect("rev_iter")
            .map(|r| u64::from_be_bytes(r.expect("item").0[24..32].try_into().unwrap()))
            .collect();
        assert_eq!(reverse.len() as u64, n);
        for (i, k) in reverse.iter().enumerate() {
            assert_eq!(*k, n - 1 - i as u64, "reverse key out of order at {i}");
        }
    }

    #[test]
    fn test_archive_btx_range() {
        let tmp = TempDir::with_prefix("test_archive_btx_range").expect("temp dir");
        let mut idx = open(&tmp.path().join("idx"));
        let n = 5_000u64;
        for i in 0..n {
            idx.save(&bkey(i), i * 10).expect("save");
        }

        let collect = |it: BtreeIter<'_>| -> Vec<u64> {
            it.map(|r| u64::from_be_bytes(r.expect("item").0[24..32].try_into().unwrap())).collect()
        };

        // Half-open [a, b): crosses many leaves.
        let got = collect(idx.range(bkey(100)..bkey(4900)).expect("range"));
        assert_eq!(got, (100..4900).collect::<Vec<_>>());

        // Open-ended: ..b and a..
        assert_eq!(collect(idx.range(..bkey(3)).expect("range")), vec![0, 1, 2]);
        let tail = collect(idx.range(bkey(4997)..).expect("range"));
        assert_eq!(tail, vec![4997, 4998, 4999]);

        // Full range equals iter() (the element type must be named for a bare `..`).
        assert_eq!(collect(idx.range::<[u8; 32], _>(..).expect("range")).len() as u64, n);

        // Empty range (b <= a) yields nothing.
        assert!(collect(idx.range(bkey(500)..bkey(500)).expect("range")).is_empty());

        // Inclusive end via a RangeInclusive.
        assert_eq!(collect(idx.range(bkey(10)..=bkey(12)).expect("range")), vec![10, 11, 12]);

        // Reverse range is the descending mirror of [a, b).
        let rev = collect(idx.rev_range(bkey(10)..bkey(15)).expect("rev_range"));
        assert_eq!(rev, vec![14, 13, 12, 11, 10]);
    }

    #[test]
    fn test_archive_btx_prefix() {
        let tmp = TempDir::with_prefix("test_archive_btx_prefix").expect("temp dir");
        let mut idx = open(&tmp.path().join("idx"));

        // Groups 0..4 plus the all-0xFF group, each with several members.
        for g in [0u8, 1, 2, 3, 0xFF] {
            for i in 0..50u64 {
                idx.save(&gkey(g, i), (g as u64) * 1000 + i).expect("save");
            }
        }

        let collect =
            |it: BtreeIter<'_>| -> Vec<Vec<u8>> { it.map(|r| r.expect("item").0).collect() };

        for g in [0u8, 2, 3] {
            let got = collect(idx.prefix(&[g]).expect("prefix"));
            assert_eq!(got.len(), 50, "group {g} size");
            assert!(got.iter().all(|k| k[0] == g), "group {g} all match prefix");
            assert!(got.windows(2).all(|w| w[0] < w[1]), "group {g} ascending");
        }

        // The all-0xFF prefix exercises the unbounded-upper edge.
        let last = collect(idx.prefix(&[0xFF]).expect("prefix"));
        assert_eq!(last.len(), 50);
        assert!(last.iter().all(|k| k[0] == 0xFF));
    }

    #[test]
    fn test_archive_btx_iter_empty() {
        let tmp = TempDir::with_prefix("test_archive_btx_iter_empty").expect("temp dir");
        let idx = open(&tmp.path().join("idx"));
        assert_eq!(idx.iter().expect("iter").count(), 0);
        assert_eq!(idx.rev_iter().expect("rev_iter").count(), 0);
        assert_eq!(idx.range(bkey(0)..bkey(10)).expect("range").count(), 0);
    }
}
