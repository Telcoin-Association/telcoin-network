//! The read path: the checks every page fetch makes, the checked descent from a root to a leaf,
//! and point lookups.

use super::{PageRead, TreeRoot};
use crate::{
    crc,
    error::{Error, Result},
    page::{self, Find, Kind, Route, Val},
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
    checked(pgno, None, page_count, snapshot_txn, private, slice)
}

/// [`checked_page`] for a page reached through a pointer that names it as `(pgno, want_txn)` (a
/// branch cell's `child` and `child_txn`, or a root's): after those checks, the low 32 bits of
/// the header's txn must equal `want_txn`, so a pointer to a stale copy or redirected to another
/// page is [`Error::Corrupt`]. The header's page number and txn are read once for every check.
pub(crate) fn checked_child<'a>(
    pgno: u32,
    want_txn: u32,
    page_count: u32,
    snapshot_txn: u64,
    private: bool,
    slice: impl FnOnce(u32) -> Option<&'a [u8]>,
) -> Result<&'a [u8]> {
    checked(pgno, Some(want_txn), page_count, snapshot_txn, private, slice)
}

/// The checks of [`checked_page`] and, with `want_txn`, [`checked_child`], in that order.
#[inline(always)]
fn checked<'a>(
    pgno: u32,
    want_txn: Option<u32>,
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
    let (header, txn) = (page::pgno(page), page::txn(page));
    if header != pgno {
        return Err(Error::Corrupt(format!("page {pgno} holds page {header}")));
    }
    if txn > snapshot_txn {
        return Err(Error::Corrupt(format!(
            "page {pgno} was written by txn {txn}, after the reading snapshot's txn {snapshot_txn}"
        )));
    }
    if let Some(want_txn) = want_txn {
        check_identity(pgno, txn, want_txn)?;
    }
    Ok(page)
}

/// The identity check of a page reached through a pointer naming `(pgno, want_txn)` whose header
/// holds `pgno` and `txn`: the low 32 bits of `txn` must be `want_txn`.
#[inline]
pub(crate) fn check_identity(pgno: u32, txn: u64, want_txn: u32) -> Result<()> {
    if txn as u32 != want_txn {
        return Err(Error::Corrupt(format!(
            "page {pgno} (txn {txn}) is not the copy its parent names (txn {want_txn}): a \
             redirected or stale pointer"
        )));
    }
    Ok(())
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
        let page = store.child(pgno, want_txn)?;
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
    find(store, root, key)?.map(|v| store.value(v)).transpose()
}

/// [`get`] of the value as its leaf cell holds it: the bytes, or the reference to its overflow
/// run, unread. The catalog and the free list take only inline values ([`Val::inline`]).
pub(crate) fn find<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    key: &[u8],
) -> Result<Option<Val<'s>>> {
    find_in(store, root, key, None)
}

/// [`find`] in a dup table's outer tree, every key of which has the table's width, `key.len()`
/// bytes: the keys either side of a miss must have it too, else [`Error::Corrupt`] naming the
/// leaf. A record of another width is then refused at first use, not a wrong `None` (or a new
/// cell of that width beside the old ones). A hit needs no check: the key found is `key`.
pub(crate) fn find_outer<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    key: &[u8],
) -> Result<Option<Val<'s>>> {
    find_in(store, root, key, Some(key.len()))
}

/// [`find`], a miss's neighbours in the leaf held to `width` bytes when it is given.
fn find_in<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    key: &[u8],
    width: Option<usize>,
) -> Result<Option<Val<'s>>> {
    let Some(leaf) = descend(store, root, key, |_, _, _, _| Ok(()), None)? else {
        return Ok(None);
    };
    match page::leaf_find(leaf.page, key)? {
        // The descent keeps `key` inside the separators, so only a leaf whose range overlaps them
        // can hold it: a hit needs no check.
        Find::Found { value, .. } => Ok(Some(value)),
        Find::NotFound { before, after, .. } => {
            check_miss(leaf.pgno, before, after, leaf.lo, leaf.hi)?;
            let wide = |k: &&[u8]| width.is_some_and(|n| k.len() != n);
            if let Some(k) = [before, after].into_iter().flatten().find(wide) {
                return Err(Error::Corrupt(format!(
                    "page {}: an outer key of {} bytes in a dup table of width {}",
                    leaf.pgno,
                    k.len(),
                    key.len()
                )));
            }
            Ok(None)
        }
    }
}

/// The last key of the tree `root` names (`None` for the empty tree): the rightmost descent, with
/// [`descend`]'s checks of every page, to the last leaf, whose last key must lie inside the range
/// its ancestors' separators give it (a misrouted pointer to another leaf of the tree is
/// [`Error::Corrupt`], not a wrong key). An append's order rule over a dup table's outer keys
/// asks it.
pub(crate) fn last_key<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
) -> Result<Option<&'s [u8]>> {
    root.check()?;
    if root.is_empty() {
        return Ok(None);
    }
    let (mut pgno, mut want_txn) = (root.root, root.root_txn);
    let (mut lo, mut hi): (Option<&'s [u8]>, Option<&'s [u8]>) = (None, None);
    for depth in 1..=root.height {
        let page = store.child(pgno, want_txn)?;
        let at_leaf_level = depth == root.height;
        let kind = page::kind(page)?;
        if (kind == Kind::Leaf) != at_leaf_level {
            return Err(Error::Corrupt(format!(
                "page {pgno} is a {kind:?} at depth {depth} of a tree of height {}",
                root.height
            )));
        }
        let n = page::nkeys(page);
        if at_leaf_level {
            let Some(last) = n.checked_sub(1) else {
                if depth > 1 {
                    return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
                }
                return Ok(None);
            };
            let key = page::leaf_key(page, last)?;
            check_miss(pgno, Some(key), None, lo, hi)?;
            return Ok(Some(key));
        }
        let Some(last) = n.checked_sub(1) else {
            return Err(Error::Corrupt(format!("branch {pgno} has no child")));
        };
        let route = page::BranchView::new(page)?.route_at(last)?;
        tighten(&route, &mut lo, &mut hi);
        (pgno, want_txn) = (route.child, route.child_txn);
    }
    // The loop returns at depth == height (checked to be >= 1 by `root.check`).
    Err(Error::Corrupt(format!("descent of a tree of height {} found no leaf", root.height)))
}

/// The check of a miss in leaf `leaf`, whose search put the key between `before` and `after`
/// ([`Find::NotFound`]), against the range `[lo, hi)` its ancestors' separators give it (`None`:
/// unbounded). A miss must come from the leaf whose range holds the key: the keys either side of
/// where it would go must lie inside the separators. Any other leaf holds a disjoint range, so one
/// of them is outside, and the miss is [`Error::Corrupt`], not a wrong `None`.
pub(crate) fn check_miss(
    leaf: u32,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Result<()> {
    let below = match (before, lo) {
        (Some(k), Some(lo)) => page::cmp_keys(k, lo).is_ge(),
        _ => true,
    };
    let above = match (after, hi) {
        (Some(k), Some(hi)) => page::cmp_keys(k, hi).is_lt(),
        _ => true,
    };
    if below && above {
        Ok(())
    } else {
        Err(Error::Corrupt(format!(
            "leaf {leaf} is not where its parents say (keys outside their separators)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{
        test_harness::{walk_tree, MemStore},
        write::plain::put,
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

    /// [`last_key`] against a sorted model, through the writer's view (private pages included)
    /// after every put and delete, and through a reader after each commit: the empty tree's is
    /// `None`, and the trees grow to three levels and shrink again. A rightmost pointer misrouted
    /// to the first subtree (both cell fields copied, so the page and txn checks pass) is
    /// `Corrupt`: the last key it reaches lies below the root's last separator.
    #[test]
    fn last_key_is_the_trees_last_key() {
        use crate::tree::{test_harness::Rng, write::plain::delete};
        let mut rng = Rng(0x1A57_0F7E_E5ED);
        let (mut store, mut root) = (MemStore::new(4096), TreeRoot::EMPTY);
        let mut model = std::collections::BTreeSet::new();
        assert_eq!(last_key(&store, &root).expect("empty"), None);
        let mut height = 0;
        for op in 0..3_000 {
            let key = key_of(rng.below(2_000));
            if op < 2_000 || rng.below(3) == 0 {
                put(&mut store, &mut root, &key, &[op as u8; 8]).expect("put");
                model.insert(key);
            } else {
                delete(&mut store, &mut root, &key).expect("delete");
                model.remove(&key);
            }
            let want = model.last().map(Vec::as_slice);
            assert_eq!(last_key(&store, &root).expect("last key"), want, "op {op}");
            if rng.below(50) == 0 {
                store.commit_like();
                assert_eq!(last_key(&store.reader(), &root).expect("reader"), want, "op {op}");
            }
            height = height.max(root.height);
        }
        assert_eq!(height, 3, "the trees reached three levels");

        let (mut store, root) = committed_tree(1_000);
        assert_eq!(root.height, 3);
        let original = store.raw(root.root).to_vec();
        let last = page::nkeys(&original) - 1;
        let first = page::branch_child(&original, 0).expect("child");
        page::branch_set_child(store.raw_mut(root.root), last, first.0, first.1).expect("set");
        store.restamp(root.root);
        let got = last_key(&store.reader(), &root).map(|k| k.map(<[u8]>::to_vec));
        assert!(matches!(got, Err(Error::Corrupt(_))), "misrouted: {got:?}");
        store.raw_mut(root.root).copy_from_slice(&original);
        let want = key_of(999);
        assert_eq!(last_key(&store.reader(), &root).expect("restored"), Some(&want[..]));
    }
}
