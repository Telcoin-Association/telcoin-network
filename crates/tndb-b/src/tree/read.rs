//! The read path: the checks every page fetch makes, the checked descent from a root to a leaf,
//! and point lookups.

use super::{PageRead, TreeRoot};
use crate::{
    crc,
    error::{Error, Result},
    page::{self, Find, Kind, Route},
};

/// The checks every page fetch makes, shared by every [`PageRead`] store. `slice` maps a page
/// number (already checked to be below `page_count`) to its bytes. The page must be a tree page
/// (`2 <= pgno < page_count`: pages 0 and 1 are the metas), carry a non-zero CRC trailer unless
/// it is `private` to the writer (a published page was stamped when it was committed; only the
/// writer's own pages are dirty), name `pgno` in its header, and have been written no later than
/// the reading snapshot's `snapshot_txn` (so a page reused too early surfaces as damage). Any
/// failure is [`Error::Corrupt`].
///
/// The CRC itself is not recomputed here (that costs a pass over the page on every read):
/// copy-on-write verifies a page before copying it, and the root of each tree is verified when
/// the file is opened.
pub(crate) fn checked_page<'a>(
    pgno: u32,
    page_count: u32,
    snapshot_txn: u64,
    private: bool,
    slice: impl FnOnce(u32) -> Option<&'a [u8]>,
) -> Result<&'a [u8]> {
    if pgno < 2 || pgno >= page_count {
        return Err(Error::Corrupt(format!(
            "page {pgno} is not a tree page of a file of {page_count} pages"
        )));
    }
    let page = slice(pgno)
        .ok_or_else(|| Error::Corrupt(format!("page {pgno} is not mapped ({page_count} pages)")))?;
    if !private && crc::crc_is_zero(page) {
        return Err(Error::Corrupt(format!("published page {pgno} was never stamped (zero CRC)")));
    }
    let header = page::pgno(page);
    if header != pgno {
        return Err(Error::Corrupt(format!("page {pgno} holds page {header}")));
    }
    let txn = page::txn(page);
    if txn > snapshot_txn {
        return Err(Error::Corrupt(format!(
            "page {pgno} was written by txn {txn}, after the reading snapshot's txn {snapshot_txn}"
        )));
    }
    Ok(page)
}

/// The leaf a [`descend`] reached, with the key range `[lo, hi)` its ancestors' separators give
/// it (`None`: unbounded).
#[derive(Debug)]
pub(crate) struct Leaf<'s> {
    /// The leaf's page number.
    pub(crate) pgno: u32,
    /// The leaf's bytes.
    pub(crate) page: &'s [u8],
    /// The greatest lower bound on the way down.
    pub(crate) lo: Option<&'s [u8]>,
    /// The least upper bound on the way down.
    pub(crate) hi: Option<&'s [u8]>,
}

/// Descend from `root` to the leaf whose range holds `key` (`None` for the empty tree). Every
/// page on the way must be the one its parent names (header page number and txn equal to the
/// branch cell's `child` and `child_txn`, or the root's), a branch above the tree's height and a
/// leaf at it, and only the root leaf may be empty; else [`Error::Corrupt`].
///
/// `visit(pgno, page, lo, hi)` sees each page before the descent reads its cells, with the range
/// its ancestors give it, and may refuse it. With `path`, each branch passed is pushed as
/// `(pgno, child index taken)`, root first.
///
/// The range kept is the intersection of every ancestor's, not the nearest one's: in a sound tree
/// the ranges nest, but a pointer misrouted to another branch brings that branch's separators,
/// which can be wider.
pub(crate) fn descend<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    key: &[u8],
    mut visit: impl FnMut(u32, &'s [u8], Option<&'s [u8]>, Option<&'s [u8]>) -> Result<()>,
    mut path: Option<&mut Vec<(u32, usize)>>,
) -> Result<Option<Leaf<'s>>> {
    root.check()?;
    if root.is_empty() {
        return Ok(None);
    }
    let (mut pgno, mut want_txn) = (root.root, root.root_txn);
    let (mut lo, mut hi): (Option<&'s [u8]>, Option<&'s [u8]>) = (None, None);
    for depth in 1..=root.height {
        let page = store.page(pgno)?;
        let txn = page::txn(page);
        if page::pgno(page) != pgno || txn as u32 != want_txn {
            return Err(Error::Corrupt(format!(
                "page {pgno} (txn {txn}) is not the copy its parent names (txn {want_txn}): a \
                 redirected or stale pointer"
            )));
        }
        let at_leaf_level = depth == root.height;
        let kind = page::kind(page)?;
        if (kind == Kind::Leaf) != at_leaf_level {
            return Err(Error::Corrupt(format!(
                "page {pgno} is a {kind:?} at depth {depth} of a tree of height {}",
                root.height
            )));
        }
        visit(pgno, page, lo, hi)?;
        if at_leaf_level {
            if depth > 1 && page::nkeys(page) == 0 {
                return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
            }
            return Ok(Some(Leaf { pgno, page, lo, hi }));
        }
        let route = page::branch_route(page, key)?;
        tighten(&route, &mut lo, &mut hi);
        if let Some(path) = path.as_deref_mut() {
            path.push((pgno, route.index));
        }
        (pgno, want_txn) = (route.child, route.child_txn);
    }
    // The loop returns at depth == height (checked to be >= 1 by `root.check`).
    Err(Error::Corrupt(format!("descent of a tree of height {} found no leaf", root.height)))
}

/// Intersect `[lo, hi)` (`None`: unbounded) with the range `route`'s branch gives the child it
/// takes.
pub(crate) fn tighten<'s>(route: &Route<'s>, lo: &mut Option<&'s [u8]>, hi: &mut Option<&'s [u8]>) {
    if let Some(k) = route.lo {
        if lo.is_none_or(|lo| page::cmp_keys(k, lo).is_gt()) {
            *lo = Some(k);
        }
    }
    if let Some(k) = route.hi {
        if hi.is_none_or(|hi| page::cmp_keys(k, hi).is_lt()) {
            *hi = Some(k);
        }
    }
}

/// Point lookup of `key` in the tree `root` names. The leaf reached must be the one its parents
/// name (see [`descend`]) and, for a miss, hold the range the separators on the way down give it:
/// a child pointer damaged at rest that leads to another page of the tree is then
/// [`Error::Corrupt`], not a wrong `None`.
pub(crate) fn get<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    key: &[u8],
) -> Result<Option<&'s [u8]>> {
    let Some(leaf) = descend(store, root, key, |_, _, _, _| Ok(()), None)? else {
        return Ok(None);
    };
    match page::leaf_find(leaf.page, key)? {
        // The descent keeps `key` inside the separators, so only a leaf whose range overlaps them
        // can hold it: a hit needs no check.
        Find::Found { value, .. } => Ok(Some(value)),
        // A miss must come from the leaf whose range holds `key`: the keys either side of where it
        // would go must lie inside the separators. Any other leaf holds a disjoint range, so one
        // of them is outside.
        Find::NotFound { before, after, .. } => {
            let below = match (before, leaf.lo) {
                (Some(k), Some(lo)) => page::cmp_keys(k, lo).is_ge(),
                _ => true,
            };
            let above = match (after, leaf.hi) {
                (Some(k), Some(hi)) => page::cmp_keys(k, hi).is_lt(),
                _ => true,
            };
            if below && above {
                Ok(None)
            } else {
                Err(Error::Corrupt(format!(
                    "leaf {} is not where its parents say (keys outside their separators)",
                    leaf.pgno
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{
        test_harness::{walk_tree, MemStore},
        write::put,
        PageWrite,
    };

    /// A 200-byte key (wide, so a tree of a few thousand entries is three levels deep).
    fn key_of(i: u64) -> Vec<u8> {
        let mut k = vec![0x5A; 200];
        k[..8].copy_from_slice(&i.to_be_bytes());
        k
    }

    /// A tree of keys `0..n` (inserted out of order, 100-byte values), committed once.
    fn committed_tree(n: u64) -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..n {
            put(&mut store, &mut root, &key_of(i * 7919 % n), &[i as u8; 100]).expect("put");
        }
        store.commit_like();
        (store, root)
    }

    /// A lookup through a reader of the last commit, the value copied out.
    fn read(store: &MemStore, root: &TreeRoot, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(get(&store.reader(), root, key)?.map(<[u8]>::to_vec))
    }

    /// The checks of every fetch: a meta or out-of-range page number, a published page with a
    /// zero (never stamped) CRC, a page whose header names another page, a page written after
    /// the reading snapshot, and a non-root empty leaf are all `Corrupt`; a private page needs
    /// no CRC.
    #[test]
    fn fetch_checks_reject_damaged_pages() {
        let (mut store, mut root) = committed_tree(200);
        let walk = walk_tree(&store, &root);
        let (leaf, other) = (walk.leaves[0], walk.leaves[1]);
        let corrupt = |r: Result<&[u8]>| matches!(r, Err(Error::Corrupt(_)));
        let count = store.alloc.page_count();
        let original = store.raw(leaf).to_vec();
        let p = original.len();
        // Pages that would pass every other check: the metas, and a page in the slack past the
        // page count.
        for meta in [0, 1] {
            page::set_pgno_txn(store.raw_mut(meta), meta, 0);
            store.restamp(meta);
        }
        store.raw_mut(count).copy_from_slice(&original);
        page::set_pgno_txn(store.raw_mut(count), count, 1);
        store.restamp(count);
        for p in [0, 1, count, u32::MAX] {
            assert!(corrupt(store.reader().page(p)), "page {p}");
        }
        assert!(store.reader().page(leaf).is_ok());

        store.raw_mut(leaf)[p - 4..].fill(0);
        assert!(corrupt(store.reader().page(leaf)), "a published page without a CRC");

        let copy = store.raw(other).to_vec();
        store.raw_mut(leaf).copy_from_slice(&copy);
        assert!(corrupt(store.reader().page(leaf)), "a page that names another page");

        store.raw_mut(leaf).copy_from_slice(&original);
        let txn = store.alloc.txn() - 1;
        page::set_pgno_txn(store.raw_mut(leaf), leaf, txn + 1);
        store.restamp(leaf);
        assert!(corrupt(store.reader().page(leaf)), "a page written after the snapshot");

        // An emptied non-root leaf (CRC-valid) would answer every miss in its range with `None`.
        store.raw_mut(leaf).copy_from_slice(&original);
        while page::nkeys(store.raw(leaf)) > 0 {
            page::leaf_delete(store.raw_mut(leaf), 0).expect("delete");
        }
        store.restamp(leaf);
        assert!(matches!(get(&store.reader(), &root, &key_of(0)), Err(Error::Corrupt(_))));
        store.raw_mut(leaf).copy_from_slice(&original);

        // The writer's own pages are dirty (zero CRC) by design and read fine.
        put(&mut store, &mut root, &key_of(0), b"rewritten").expect("put");
        assert!(store.is_private(root.root) && crc::crc_is_zero(store.raw(root.root)));
        assert!(store.page(root.root).is_ok());
    }

    /// A branch cell redirected to an older, still CRC-valid copy of its child (same page range,
    /// stale contents) passes every range check; only the cell's `child_txn` exposes it. A cell
    /// redirected to another page is caught too. Reads and writes both refuse.
    #[test]
    fn stale_copies_and_redirected_children_are_corrupt() {
        let (mut store, mut root) = committed_tree(60);
        assert_eq!(root.height, 2, "leaves under the root");
        let old = walk_tree(&store, &root);
        let stale_leaf = old.leaves[0];
        // Rewrite the smallest key: its leaf and the root are copied in the next txn.
        put(&mut store, &mut root, &key_of(0), b"rewritten").expect("put");
        store.commit_like();
        let new = walk_tree(&store, &root);
        assert_ne!(new.leaves[0], stale_leaf, "the leaf was copied");
        let (_, child_txn) = page::branch_child(store.raw(root.root), 0).expect("child");
        let original = store.raw(root.root).to_vec();

        for (case, target) in [("stale copy", stale_leaf), ("another page", old.leaves[1])] {
            page::branch_set_child(store.raw_mut(root.root), 0, target, child_txn).expect("set");
            store.restamp(root.root);
            for k in [key_of(0), key_of(1)] {
                let got = read(&store, &root, &k);
                assert!(matches!(got, Err(Error::Corrupt(_))), "{case}: {got:?}");
            }
            let mut writer_root = root;
            let put = put(&mut store, &mut writer_root, &key_of(0), b"again");
            assert!(matches!(put, Err(Error::Corrupt(_))), "{case}: {put:?}");
            assert_eq!(writer_root, root, "{case}: the failed put changed nothing");
            store.raw_mut(root.root).copy_from_slice(&original);
        }
        assert_eq!(read(&store, &root, &key_of(0)).expect("get"), Some(b"rewritten".to_vec()));
    }

    /// A root cell misrouted to its sibling's subtree (both cell fields copied, so the page and txn
    /// checks pass) is caught on a miss by the intersection of the ancestors' ranges: the
    /// sibling's own separators are wider than the range the root gives the lost subtree.
    #[test]
    fn range_intersection_catches_a_misrouted_pointer() {
        let (mut store, root) = committed_tree(1_000);
        assert_eq!(root.height, 3, "the root's children are branches");
        let original = store.raw(root.root).to_vec();
        for (from, to) in [(0, 1), (1, 0)] {
            // A key inside the lost subtree: the first key of its middle leaf.
            let lost = {
                let reader = store.reader();
                let (sub, _) = page::branch_child(store.raw(root.root), from).expect("child");
                let sub = reader.page(sub).expect("subtree");
                let (leaf, _) = page::branch_child(sub, page::nkeys(sub) / 2).expect("leaf");
                page::leaf_key(reader.page(leaf).expect("leaf"), 0).expect("key").to_vec()
            };
            let target = page::branch_child(&original, to).expect("child");
            page::branch_set_child(store.raw_mut(root.root), from, target.0, target.1)
                .expect("set");
            store.restamp(root.root);
            let got = read(&store, &root, &lost);
            assert!(
                matches!(got, Err(Error::Corrupt(_))),
                "child {from} misrouted to {to}: {got:?}"
            );
            // A write's descent checks every page's keys against the range: no misplaced insert.
            let mut writer_root = root;
            let put = put(&mut store, &mut writer_root, &lost, b"misplaced");
            assert!(matches!(put, Err(Error::Corrupt(_))), "put through {from} -> {to}: {put:?}");
            store.raw_mut(root.root).copy_from_slice(&original);
            assert!(get(&store.reader(), &root, &lost).expect("get").is_some());
        }
    }
}
