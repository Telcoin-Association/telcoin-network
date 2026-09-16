//! Sorted iteration over a [`BtreeIndex`]: forward, reverse, bounded ranges, and key prefixes.
//!
//! All iterators walk the doubly-linked leaf chain, so once positioned they advance one leaf at a
//! time with no re-descent.  Each step yields `Result<(Vec<u8>, u64), FetchError>` (the key is a
//! fresh `ksize`-byte `Vec`) and streams (holding `&mut BtreeIndex` to fetch successive leaves); a
//! fetch/CRC failure is surfaced as a terminal `Err`.

use std::ops::{Bound, RangeBounds};

use crate::archive::{
    btree_index::{
        index::BtreeIndex,
        page::{Node, NULL_PAGE},
    },
    error::fetch::FetchError,
};

/// Marker `pos` value (for reverse scans) meaning "step to the previous leaf on the next call".
const NEED_PREV: usize = usize::MAX;

/// A sorted iterator over `(key, position)` entries of a [`BtreeIndex`].
///
/// Created by [`BtreeIndex::iter`], [`BtreeIndex::rev_iter`], [`BtreeIndex::range`],
/// [`BtreeIndex::rev_range`], and [`BtreeIndex::prefix`].
#[derive(Debug)]
pub struct BtreeIter<'a> {
    index: &'a mut BtreeIndex,
    /// Page geometry (key size) copied from the index, to decode leaf buffers.
    node: Node,
    /// Owned copy of the current leaf page (empty until initialized).
    buf: Vec<u8>,
    /// Current leaf page number, or [`NULL_PAGE`] when exhausted.
    leaf: u32,
    /// Next slot to yield within `buf` (or [`NEED_PREV`] in reverse mode).
    pos: usize,
    reverse: bool,
    /// Inclusive/exclusive lower bound (the stop bound in reverse, start bound in forward).
    lower: Bound<Vec<u8>>,
    /// Inclusive/exclusive upper bound (the stop bound in forward, start bound in reverse).
    upper: Bound<Vec<u8>>,
}

impl<'a> BtreeIter<'a> {
    fn new(
        index: &'a mut BtreeIndex,
        reverse: bool,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Result<Self, FetchError> {
        let node = index.node();
        let mut it =
            Self { index, node, buf: Vec::new(), leaf: NULL_PAGE, pos: 0, reverse, lower, upper };
        if reverse {
            it.reverse_start()?;
        } else {
            it.forward_start()?;
        }
        Ok(it)
    }

    /// Position the cursor at the first entry `>= lower` (ascending).
    fn forward_start(&mut self) -> Result<(), FetchError> {
        match self.lower.clone() {
            Bound::Unbounded => {
                let sl = self.index.first_leaf();
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                self.pos = 0;
            }
            Bound::Included(lo) => {
                let sl = self.index.find_leaf(&lo)?;
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                self.pos = match self.node.leaf_search(&self.buf, &lo) {
                    Ok(i) => i,
                    Err(i) => i,
                };
            }
            Bound::Excluded(lo) => {
                let sl = self.index.find_leaf(&lo)?;
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                self.pos = match self.node.leaf_search(&self.buf, &lo) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
            }
        }
        Ok(())
    }

    /// Position the cursor at the greatest entry `<= upper` (descending).
    fn reverse_start(&mut self) -> Result<(), FetchError> {
        let sp: isize = match self.upper.clone() {
            Bound::Unbounded => {
                let sl = self.index.last_leaf();
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                self.node.entry_count(&self.buf) as isize - 1
            }
            Bound::Included(hi) => {
                let sl = self.index.find_leaf(&hi)?;
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                match self.node.leaf_search(&self.buf, &hi) {
                    Ok(i) => i as isize,
                    Err(i) => i as isize - 1,
                }
            }
            Bound::Excluded(hi) => {
                let sl = self.index.find_leaf(&hi)?;
                self.buf = self.index.fetch_page(sl)?;
                self.leaf = sl;
                match self.node.leaf_search(&self.buf, &hi) {
                    Ok(i) => i as isize - 1,
                    Err(i) => i as isize - 1,
                }
            }
        };
        // A negative start means the greatest matching entry is in an earlier leaf.
        self.pos = if sp >= 0 { sp as usize } else { NEED_PREV };
        Ok(())
    }

    fn key_at(&self, i: usize) -> Vec<u8> {
        self.node.leaf_key(&self.buf, i).to_vec()
    }

    fn next_forward(&mut self) -> Option<Result<(Vec<u8>, u64), FetchError>> {
        loop {
            if self.leaf == NULL_PAGE {
                return None;
            }
            let n = self.node.entry_count(&self.buf);
            if self.pos >= n {
                let nx = self.node.leaf_next(&self.buf);
                if nx == NULL_PAGE {
                    self.leaf = NULL_PAGE;
                    return None;
                }
                match self.index.fetch_page(nx) {
                    Ok(b) => {
                        self.buf = b;
                        self.leaf = nx;
                        self.pos = 0;
                    }
                    Err(e) => {
                        self.leaf = NULL_PAGE;
                        return Some(Err(e));
                    }
                }
                continue;
            }
            let key = self.key_at(self.pos);
            let stop = match &self.upper {
                Bound::Unbounded => false,
                Bound::Included(hi) => key > *hi,
                Bound::Excluded(hi) => key >= *hi,
            };
            if stop {
                self.leaf = NULL_PAGE;
                return None;
            }
            let val = self.node.leaf_value(&self.buf, self.pos);
            self.pos += 1;
            return Some(Ok((key, val)));
        }
    }

    fn next_reverse(&mut self) -> Option<Result<(Vec<u8>, u64), FetchError>> {
        loop {
            if self.leaf == NULL_PAGE {
                return None;
            }
            if self.pos == NEED_PREV || self.node.entry_count(&self.buf) == 0 {
                let pv = self.node.leaf_prev(&self.buf);
                if pv == NULL_PAGE {
                    self.leaf = NULL_PAGE;
                    return None;
                }
                match self.index.fetch_page(pv) {
                    Ok(b) => {
                        let n = self.node.entry_count(&b);
                        self.buf = b;
                        self.leaf = pv;
                        self.pos = if n > 0 { n - 1 } else { NEED_PREV };
                    }
                    Err(e) => {
                        self.leaf = NULL_PAGE;
                        return Some(Err(e));
                    }
                }
                continue;
            }
            let key = self.key_at(self.pos);
            let stop = match &self.lower {
                Bound::Unbounded => false,
                Bound::Included(lo) => key < *lo,
                Bound::Excluded(lo) => key <= *lo,
            };
            if stop {
                self.leaf = NULL_PAGE;
                return None;
            }
            let val = self.node.leaf_value(&self.buf, self.pos);
            // Advance to the previous entry for the next call.
            self.pos = if self.pos == 0 { NEED_PREV } else { self.pos - 1 };
            return Some(Ok((key, val)));
        }
    }
}

impl Iterator for BtreeIter<'_> {
    type Item = Result<(Vec<u8>, u64), FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reverse {
            self.next_reverse()
        } else {
            self.next_forward()
        }
    }
}

impl BtreeIndex {
    /// Ascending iterator over all `(key, position)` entries.
    pub fn iter(&mut self) -> Result<BtreeIter<'_>, FetchError> {
        BtreeIter::new(self, false, Bound::Unbounded, Bound::Unbounded)
    }

    /// Descending iterator over all `(key, position)` entries.
    pub fn rev_iter(&mut self) -> Result<BtreeIter<'_>, FetchError> {
        BtreeIter::new(self, true, Bound::Unbounded, Bound::Unbounded)
    }

    /// Ascending iterator over the entries whose keys fall within `bounds`.
    ///
    /// The bound element may be any `AsRef<[u8]>` (e.g. `[u8; 32]` or `Vec<u8>`).  A fully
    /// unbounded `..` needs the element type spelled out (e.g. `range::<[u8; 32], _>(..)`) or use
    /// [`BtreeIndex::iter`].
    pub fn range<T: AsRef<[u8]>, R: RangeBounds<T>>(
        &mut self,
        bounds: R,
    ) -> Result<BtreeIter<'_>, FetchError> {
        let (lower, upper) = clone_bounds(&bounds);
        BtreeIter::new(self, false, lower, upper)
    }

    /// Descending iterator over the entries whose keys fall within `bounds`.
    pub fn rev_range<T: AsRef<[u8]>, R: RangeBounds<T>>(
        &mut self,
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
        let mut idx = open(&tmp.path().join("idx"));
        assert_eq!(idx.iter().expect("iter").count(), 0);
        assert_eq!(idx.rev_iter().expect("rev_iter").count(), 0);
        assert_eq!(idx.range(bkey(0)..bkey(10)).expect("range").count(), 0);
    }
}
