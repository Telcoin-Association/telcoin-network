//! The write path: copy-on-write of the root-to-leaf path, insert and replace with leaf and
//! branch splits, and a new root when the old one splits; delete, with emptied pages unlinked,
//! pages under a quarter full merged with their siblings ([`plan_merges`]) and a single-child
//! root collapsed; and freeing a whole tree.
//!
//! A put first plans from a read-only descent (checking every page on the path, and verifying
//! each published page it will copy), then allocates every page it can need — the copies of the
//! published pages on the path and, if the leaf overflows, the worst case of the split cascade —
//! and only then changes anything. A failed allocation (a full disk, say) therefore leaves the
//! tree exactly as it was: the same root handle and every reachable page byte for byte. A delete
//! plans and reserves the same way ([`delete`]).
//!
//! **Overflow runs.** A value too large for its leaf ([`Geometry::place`]) goes to a run of
//! contiguous pages ([`crate::overflow`]), which the put allocates with its tree pages and writes
//! before any leaf names it; the leaf holds a 12-byte reference. A run is immutable: a copy of
//! the leaf shares it, and a replace or a delete frees the old run only once the change is
//! applied. A run leaving a tree is checked first (header and identity) and must be one this
//! transaction can free: a run private to this transaction named by a leaf it did not write is
//! [`Error::Corrupt`], as a private page under an untrusted parent is ([`descend_write`]).

use std::time::Instant;

use super::{
    read::{check_miss, tighten},
    PageRead, PageWrite, TreeRoot, MAX_HEIGHT,
};
use crate::{
    crc::{self, CrcState},
    error::{Error, Result},
    overflow::{self, Ovf, RunCrc, OVF_CELL},
    page::{self, Find, Fit, Geometry, Kind, Place, Val},
    page_set::PageSet,
    stats::elapsed_ns,
};

// Test-only counters, debug builds only: the benchmark is compiled with the test configuration,
// and a thread-local update on a timed path would show in its release timings.
#[cfg(all(test, debug_assertions))]
thread_local! {
    /// Test-only: how many published pages copy-on-write verified (CRC and structure).
    pub(crate) static COW_VERIFIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Test-only: how many puts [`put_cached`] did in the cached leaf.
    pub(crate) static CACHED_PUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Test-only: how many deletes [`delete_cached`] did in the cached leaf.
    pub(crate) static CACHED_DELETES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What a put does to the leaf it reaches.
#[derive(Clone, Copy, Debug)]
enum LeafChange {
    /// Replace entry `i`'s value in place.
    Replace(usize),
    /// Insert the entry at `i`.
    Insert(usize),
    /// The leaf overflows: split it, inserting the entry at `at` (after deleting the old entry
    /// there first when `replace`), in append mode when the entry goes past the end of the tree's
    /// rightmost leaf.
    Split { at: usize, replace: bool, append: bool },
}

/// Which put a write is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PutKind {
    /// Insert the entry, or replace the value of a present key.
    Upsert,
    /// Insert an entry whose key sorts strictly after the tree's last key (any key into an empty
    /// tree); any other key is [`Error::AppendOutOfOrder`], refused before anything changes.
    Append,
    /// Insert an entry whose key is not present; a present key is [`Error::KeyExists`], refused
    /// where the put finds it (in the cached leaf, or by the plan after the descent) before
    /// anything changes.
    Insert,
}

/// Where a put left its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Placed {
    /// True if it replaced a present key's value.
    pub(crate) replaced: bool,
    /// The entry's slot in the leaf the table's [`PathCache`] then names; `None` after a split
    /// that left the cache empty (every split but an append split through a cache, which fills
    /// it with the new path: see [`put_from_root`]).
    pub(crate) slot: Option<usize>,
}

/// What a delete through a table's [`PathCache`] did ([`delete_cached`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Removed {
    /// True if the key was there (and is now gone).
    pub(crate) found: bool,
    /// The deleted entry's old slot in the leaf the table's [`PathCache`] then names, which is
    /// now its follower's slot (the leaf's entry count if it was the last); `None` when the cache
    /// is empty after the delete (a miss, or a delete that unlinked, collapsed, emptied or
    /// merged).
    pub(crate) slot: Option<usize>,
}

/// A broken invariant of this module (not damage on disk): the writer must stop.
#[cold]
fn bug(what: impl std::fmt::Display) -> Error {
    Error::Failed(format!("b+tree write invariant broken: {what}"))
}

/// Insert `(key, value)` into the tree `root` names, or replace the value if `key` is present
/// (returning true). Entries over the size limits are refused ([`Error::KeyTooLarge`],
/// [`Error::ValueTooLarge`]) before anything changes; then as [`put_sized`]. The catalog and the
/// free list write through this checked entry point.
pub(crate) fn put<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    key: &[u8],
    value: &[u8],
) -> Result<bool> {
    store.geometry().check_entry(key.len(), value.len())?;
    put_sized(store, root, trusted, key, value)
}

/// [`put`] of an entry whose sizes the caller checked ([`Geometry::check_entry`]). Damage found
/// on the path ([`Error::Corrupt`]) and a failed allocation are refused before anything changes.
/// `root` is updated: a new root page when the old one is copied or splits, the height, the entry
/// count.
///
/// `trusted` is the root's provenance (see [`descend_write`]): true only while `root` names a
/// page this transaction's writer wrote for this same holder. A successful put leaves it true:
/// the whole path is then private, built here from checked pages.
pub(crate) fn put_sized<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    key: &[u8],
    value: &[u8],
) -> Result<bool> {
    Ok(put_from_root(store, root, trusted, None, key, value, PutKind::Upsert)?.replaced)
}

/// [`put_sized`], or an append or an insert (`kind`), through a table's write-path `cache`
/// ([`PathCache`]): a put whose key lands in the cached leaf and fits there changes that leaf in
/// place with no descent, exactly as the descent's put would (the same leaf, change, entry count
/// and bytes). Any other put descends ([`put_sized`]) and refills the cache if it did not split,
/// or did an append split (with the new path to the entry: see [`put_from_root`]). An append takes
/// the cached leaf only if it is the tree's rightmost; elsewhere, its descent refuses a key that
/// does not sort after the tree's last key. An insert of a present key is refused where it is
/// found: in the cached leaf (the cache kept), else by the descent's plan (the cache left empty). A
/// value stored in an overflow run, and a replace of one, always descend: the run is allocated, or
/// the old one freed, with the tree's other pages.
pub(crate) fn put_cached<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    cache: &mut PathCache,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
) -> Result<Placed> {
    if *trusted
        && cache.epoch == store.epoch()
        && value.len() <= store.geometry().max_value(key.len())
    {
        if let Some(placed) = put_in_cached_leaf(store, root, cache, key, value, kind)? {
            return Ok(placed);
        }
    }
    put_from_root(store, root, trusted, Some(cache), key, value, kind)
}

/// [`put_sized`], or an append or an insert (`kind`): descend from the root, plan, reserve,
/// apply; then fill `cache`, if given, with the entry's path (private and trusted once the put
/// is done) when the put did not split, or did an append split. After an append split whose
/// separator the leaf's parent took, the most common one, the new path is the descent's with the
/// parent's step moved to its new last cell and the new right leaf, which holds only the entry;
/// after one that split a branch or grew a new root, a second, trusted descent finds it.
fn put_from_root<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    cache: Option<&mut PathCache>,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
) -> Result<Placed> {
    root.check()?;
    let txn = store.txn();
    let run_pages = match store.geometry().place(key.len(), value.len())? {
        Place::Inline => None,
        Place::Overflow { npages } => {
            Some(u32::try_from(npages).map_err(|_| bug("a run of over 2^32 pages"))?)
        }
    };
    if root.is_empty() {
        let p = store.alloc()?;
        let val = match run_pages {
            None => Val::Inline(value),
            Some(n) => match write_run(store, n, value) {
                Ok(ovf) => Val::Overflow(ovf),
                Err(e) => {
                    store.free(p);
                    return Err(e);
                }
            },
        };
        let leaf = store.page_mut(p)?;
        page::init_leaf_header(leaf, p, txn);
        if page::LeafMut::new(leaf)?.insert(0, key, val)? == Fit::Full {
            return Err(bug("an entry overflows an empty leaf"));
        }
        *root = TreeRoot {
            root: p,
            root_txn: txn as u32,
            height: 1,
            overflow_pages: run_pages.unwrap_or(0),
            entries: 1,
            leaf_pages: 1,
            ..TreeRoot::EMPTY
        };
        *trusted = true;
        if let Some(cache) = cache {
            let (path, mut tail) = cache.lend_path();
            tail.set_range((None, None));
            tail.fill(store, p, path)?;
        }
        return Ok(Placed { replaced: false, slot: Some(0) });
    }
    // With a cache, the descent records its path straight into the cache's buffer.
    let mut own_path = None;
    let (path, mut tail) = match cache {
        Some(cache) => {
            let (path, tail) = cache.lend_path();
            (path, Some(tail))
        }
        None => (own_path.insert(Path::new()), None),
    };
    let vlen = if run_pages.is_some() { OVF_CELL } else { value.len() };
    let planned = plan(&*store, root, *trusted, key, vlen, kind, path)?;
    let Planned { leaf, change, needed, range, old_run } = planned;
    if let Some(tail) = tail.as_mut() {
        tail.set_range(range);
    }
    // The run counter after the put, refused before anything changes if the handle is damaged.
    let mut counted = *root;
    if let Some((_, n)) = old_run {
        counted.remove_run(n)?;
    }
    if let Some(n) = run_pages {
        counted.add_run(n)?;
    }
    // A put that needs no page (a private path, no split) needs no scratch either.
    let mut scratch = if needed == 0 { Scratch::default() } else { store.take_scratch() };
    let val = match reserve_put(store, needed, &mut scratch.reserved, run_pages, value) {
        Ok(ovf) => ovf.map_or(Val::Inline(value), Val::Overflow),
        Err(e) => {
            if needed != 0 {
                store.put_scratch(scratch);
            }
            return Err(e);
        }
    };
    let result = make_path_writable(store, root, path, leaf, &mut scratch).and_then(|leaf| {
        let applied = apply(store, root, path, leaf, key, val, change, &mut scratch)?;
        Ok((leaf, applied))
    });
    // The split's worst case may not have happened: the pages it did not use are reusable.
    for p in scratch.reserved.drain(..) {
        store.free(p);
    }
    if needed != 0 {
        store.put_scratch(scratch);
    }
    let (leaf, Applied { replaced, right_leaf }) = result?;
    // The leaf no longer names the old run: it leaves the tree.
    if let Some((p, n)) = old_run {
        store.free_run(p, n);
    }
    root.overflow_pages = counted.overflow_pages;
    *trusted = true;
    let (leaf, slot) = match (change, tail.as_mut()) {
        (LeafChange::Replace(i) | LeafChange::Insert(i), _) => (leaf, Some(i)),
        // An append split changed the path, and the next put of an ascending run goes to the new
        // right leaf. With a cache, cache the entry's new path, so neither that put nor a cursor
        // placed at the entry descends again. Any other split leaves the cache empty: a second
        // descent there would rarely be used.
        (LeafChange::Split { append: true, .. }, Some(tail)) => match right_leaf {
            // The leaf's parent took the separator, so `path` leads to the new right leaf
            // ([`Applied::right_leaf`]): the parent's last child, on the rightmost path, holding
            // only the entry. Its range starts at the separator, which an append split takes from
            // the key itself ([`page::SplitBuf::split_leaf`]), and has no upper bound: what a
            // descent of the new path finds, without the descent.
            Some(right) => {
                #[cfg(debug_assertions)]
                assert_found_again(&*store, root, key, path, right);
                tail.set_range((Some(key), None));
                (right, Some(0))
            }
            // A branch split or a new root: find the entry again on the new path (every page on
            // it is private now, so the trusted descent checks only identity, kind and
            // non-emptiness).
            None => {
                path.len = 0;
                let (found, _) = descend_write(&*store, root, true, key, Verify::Now, path)?;
                let Find::Found { index, .. } = found.view.find(key)? else {
                    return Err(bug("a split lost the entry it put"));
                };
                tail.set_range(found.range);
                (found.pgno, Some(index))
            }
        },
        (LeafChange::Split { .. }, _) => (leaf, None),
    };
    if let (Some(mut tail), Some(_)) = (tail, slot) {
        tail.fill(store, leaf, path)?;
    }
    Ok(Placed { replaced, slot })
}

/// Debug builds: the path [`put_from_root`] caches after an append split without a descent is
/// what a trusted descent of the new path finds: the branches `path`, the leaf `right` with `key`
/// at slot 0, and the range from `key` up.
#[cfg(debug_assertions)]
fn assert_found_again<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    key: &[u8],
    path: &[(u32, usize)],
    right: u32,
) {
    let mut again = Path::new();
    let found = descend_write(store, root, true, key, Verify::Now, &mut again)
        .and_then(|(found, _)| Ok((found.pgno, found.range, found.view.find(key)?)));
    let same = match found {
        Ok((pgno, (Some(lo), None), Find::Found { index: 0, .. })) => {
            pgno == right && lo == key && *again == *path
        }
        _ => false,
    };
    assert!(same, "an append split cached another path than a descent finds: {found:?}");
}

/// A [`PathCache`]'s fields other than its path, borrowed beside the path buffer a put's descent
/// records into ([`PathCache::lend_path`]).
struct CacheTail<'c> {
    epoch: &'c mut u64,
    leaf: &'c mut u32,
    lo: &'c mut Bound,
    hi: &'c mut Bound,
}

impl CacheTail<'_> {
    /// Copy the planned leaf's key range ([`WriteLeaf::range`]) into the cache, before the put
    /// changes anything (the range borrows the pages). The put then either fills the rest
    /// ([`Self::fill`]) or splits, and a split allocates: either way the cache never pairs this
    /// range with an older leaf in the current epoch.
    fn set_range(&mut self, (lo, hi): Bounds<'_>) {
        self.lo.set(lo);
        self.hi.set(hi);
    }

    /// Fill the cache: `leaf`, reached by the trusted `path` the put left (already in the cache's
    /// buffer), with the range [`Self::set_range`] copied, in the current epoch.
    fn fill<S: PageWrite + ?Sized>(
        &mut self,
        store: &S,
        leaf: u32,
        path: &[(u32, usize)],
    ) -> Result<()> {
        debug_assert_eq!(
            path_bounds(store, path)?,
            (self.lo.get(), self.hi.get()),
            "a leaf's range is the intersection of its ancestors' separators"
        );
        *self.leaf = leaf;
        *self.epoch = store.epoch();
        Ok(())
    }
}

/// A table's write-path cache: the leaf its latest write reached, the branches above it, and the
/// key range `[lo, hi)` that routes to that leaf (the intersection of every ancestor's
/// separators, as [`read::descend`] keeps it), so the next put or delete landing there skips the
/// descent ([`put_cached`], [`delete_cached`]). The bounds are owned copies in buffers the cache
/// reuses.
///
/// It is filled after a put that ended on a trusted path (every page private, built here: see
/// [`descend_write`]) without a split, or after an append split with the new path (from the split
/// itself when the leaf's parent took the separator, else from a second descent: see
/// [`put_from_root`]); after a delete that only took the entry out of its leaf, and after a miss
/// on a trusted path ([`delete_from_root`]). It holds only while the allocator's epoch
/// ([`PageWrite::epoch`]) is the one it was filled in. Every change to a tree's shape or separators
/// allocates or frees a page (a copy, a split, a new root, an unlink or a collapse; a cleared or
/// dropped tree frees its pages), and a commit or abort ends the transaction's private pages, each
/// with a new epoch. An in-place insert, replace or delete changes only its leaf and no separator.
/// So within the epoch the root still names the cached path, every page on it is still private, and
/// exactly the keys in `[lo, hi)` route to the leaf. The catalog and the free list write through
/// [`put`] and [`delete`] and have no cache.
#[derive(Debug)]
pub(crate) struct PathCache {
    /// The epoch it was filled in; 0 (never an epoch) while empty.
    epoch: u64,
    /// The leaf.
    leaf: u32,
    /// The branches above the leaf, root first, as [`descend_write`] records them.
    path: Path,
    /// The leaf's lower bound (`None`: the leftmost leaf).
    lo: Bound,
    /// The leaf's upper bound (`None`: the rightmost leaf).
    hi: Bound,
}

impl Default for PathCache {
    fn default() -> Self {
        Self { epoch: 0, leaf: 0, path: Path::new(), lo: Bound::default(), hi: Bound::default() }
    }
}

impl PathCache {
    /// Empty the cache and lend its path buffer to a put's descent, which records its path there
    /// (no fresh [`Path`] to zero, no copy at the fill), beside the rest of the cache
    /// ([`CacheTail`]), which the put fills once it is done. Emptied first, the cache holds again
    /// only once [`CacheTail::fill`] sets its epoch.
    fn lend_path(&mut self) -> (&mut Path, CacheTail<'_>) {
        let Self { epoch, leaf, path, lo, hi } = self;
        *epoch = 0;
        path.len = 0;
        (path, CacheTail { epoch, leaf, lo, hi })
    }

    /// The branches above the cached leaf, root first, as `(pgno, child index taken)`: the path a
    /// descent for any key of the leaf's range takes. Meaningful only right after a put or a
    /// delete that returned a slot ([`Placed::slot`], [`Removed::slot`]).
    pub(crate) fn branches(&self) -> &[(u32, usize)] {
        &self.path
    }

    /// The cached leaf (see [`Self::branches`]).
    pub(crate) fn leaf(&self) -> u32 {
        self.leaf
    }

    /// Test-only: empty the cache, so the next put descends (and refills it).
    #[cfg(test)]
    pub(crate) fn forget(&mut self) {
        self.epoch = 0;
    }
}

/// An optional key bound held in a reused buffer.
#[derive(Debug, Default)]
struct Bound {
    key: Vec<u8>,
    set: bool,
}

impl Bound {
    fn get(&self) -> Option<&[u8]> {
        self.set.then_some(self.key.as_slice())
    }

    fn set(&mut self, key: Option<&[u8]>) {
        self.key.clear();
        self.key.extend_from_slice(key.unwrap_or_default());
        self.set = key.is_some();
    }
}

/// [`put_cached`]'s put in the cached leaf, which the caller found valid in this epoch for a
/// trusted root: done if the key lands in the leaf's `[lo, hi)` and the entry fits there
/// (`Some`); else `None`, having changed nothing. The bounds are compared before the leaf is
/// read, so a key elsewhere costs at most two compares; in the leaf, a key past its last key (an
/// ascending run) needs no search. An append needs the rightmost leaf (no `hi`), whose last key
/// is the tree's: a key not past it is [`Error::AppendOutOfOrder`]. An insert of a key the leaf
/// holds is [`Error::KeyExists`].
fn put_in_cached_leaf<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    cache: &PathCache,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
) -> Result<Option<Placed>> {
    let top = cache.path.first().map_or(cache.leaf, |&(p, _)| p);
    let names_root = top == root.root && cache.path.len() + 1 == root.height as usize;
    debug_assert!(names_root, "a path cache of this epoch names another tree");
    let append = kind == PutKind::Append;
    if !names_root
        || (append && cache.hi.get().is_some())
        || cache.hi.get().is_some_and(|hi| page::cmp_keys(key, hi).is_ge())
        || cache.lo.get().is_some_and(|lo| page::cmp_keys(key, lo).is_lt())
    {
        return Ok(None);
    }
    let (leaf, txn) = (cache.leaf, store.txn());
    let page = store.page_mut(leaf)?;
    if page::pgno(page) != leaf || page::txn(page) != txn {
        return Err(Error::Corrupt(format!(
            "cached leaf {leaf} (txn {}) is not the page this txn wrote",
            page::txn(page)
        )));
    }
    // One header parse for the search and the change.
    let page = page::LeafMut::new(page)?;
    let change = {
        let view = page.view();
        let Some(last) = view.nkeys().checked_sub(1) else {
            return Ok(None);
        };
        let found = if page::cmp_keys(key, view.key(last)?).is_gt() {
            Find::NotFound { index: last + 1, before: None, after: None }
        } else if append {
            return Err(Error::AppendOutOfOrder);
        } else {
            view.find(key)?
        };
        match found {
            Find::Found { .. } if kind == PutKind::Insert => return Err(Error::KeyExists),
            // The old run must be freed with the tree's other changes: descend.
            Find::Found { value: Val::Overflow(_), .. } => return Ok(None),
            Find::Found { value: Val::Nested(root), .. } => return Err(page::nested_error(root)),
            Find::Found { index, value: Val::Inline(old) } => {
                let old = old.len();
                if value.len() > old && value.len() - old > view.free_space() {
                    return Ok(None);
                }
                LeafChange::Replace(index)
            }
            Find::NotFound { index, .. } => {
                if !view.fits(page::cell_size_leaf(key.len(), value.len())) {
                    return Ok(None);
                }
                LeafChange::Insert(index)
            }
        }
    };
    let (fit, replaced, slot) = match change {
        LeafChange::Replace(i) => (page.replace_value(i, value)?, true, i),
        LeafChange::Insert(i) => (page.insert(i, key, value)?, false, i),
        LeafChange::Split { .. } => return Err(bug("a split planned in a cached leaf")),
    };
    if fit == Fit::Full {
        return Err(bug("an entry did not fit its cached leaf as planned"));
    }
    if !replaced {
        root.entries += 1;
    }
    #[cfg(all(test, debug_assertions))]
    CACHED_PUTS.with(|n| n.set(n.get() + 1));
    Ok(Some(Placed { replaced, slot: Some(slot) }))
}

/// [`delete`] through a table's write-path `cache` ([`PathCache`]), the twin of [`put_cached`]: a
/// delete whose key lies in the cached leaf's range and whose entry leaves that leaf as it is (an
/// inline value, another entry left, no merge due: [`merge_due`]) changes the leaf in place with no
/// descent, exactly as the descent's delete would (the same leaf, change, entry count and bytes;
/// nothing allocated or freed, so the cache stays valid). A key in the range that the leaf does not
/// hold is a miss with no descent. Any other delete descends ([`delete_from_root`]) and refills the
/// cache after a delete that only took the entry out of its leaf (or a miss on a path of this
/// transaction's own pages). The returned slot places a cursor at the deleted entry's follower
/// ([`Removed::slot`]).
pub(crate) fn delete_cached<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    cache: &mut PathCache,
    key: &[u8],
) -> Result<Removed> {
    if *trusted && cache.epoch == store.epoch() {
        if let Some(removed) = delete_in_cached_leaf(store, root, cache, key)? {
            return Ok(removed);
        }
    }
    delete_from_root(store, root, trusted, Some(cache), key)
}

/// [`delete_cached`]'s delete in the cached leaf, which the caller found valid in this epoch for a
/// trusted root: done if the key lies in the leaf's `[lo, hi)` and its delete changes only the
/// leaf (`Some`); else `None`, having changed nothing. The descent's plan of such a delete is a
/// [`DeleteShape::Leaf`] with no merge and no copy (every page on a cached path is private), so
/// its change is this one: the cell out of the leaf, one entry fewer on the handle. It descends
/// instead for a value in an overflow run (freed with the tree's other changes), the leaf's last
/// entry (an unlink or a collapse), a leaf the delete would leave due a merge, or a handle that
/// does not count the entry and another (the descent's plan reports it).
fn delete_in_cached_leaf<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    cache: &PathCache,
    key: &[u8],
) -> Result<Option<Removed>> {
    let top = cache.path.first().map_or(cache.leaf, |&(p, _)| p);
    let names_root = top == root.root && cache.path.len() + 1 == root.height as usize;
    debug_assert!(names_root, "a path cache of this epoch names another tree");
    if !names_root
        || root.entries < 2
        || cache.hi.get().is_some_and(|hi| page::cmp_keys(key, hi).is_ge())
        || cache.lo.get().is_some_and(|lo| page::cmp_keys(key, lo).is_lt())
    {
        return Ok(None);
    }
    // The leaf's index on the path, for the merge rule; a root leaf never merges.
    let (depth, merge) = (cache.path.len(), merging(store));
    let (usable, leaf, txn) = (store.geometry().usable(), cache.leaf, store.txn());
    let page = store.page_mut(leaf)?;
    if page::pgno(page) != leaf || page::txn(page) != txn {
        return Err(Error::Corrupt(format!(
            "cached leaf {leaf} (txn {}) is not the page this txn wrote",
            page::txn(page)
        )));
    }
    // One header parse for the search and the change.
    let page = page::LeafMut::new(page)?;
    let index = {
        let view = page.view();
        let (index, value) = match view.find(key)? {
            Find::Found { index, value } => (index, value),
            // The key routes to this leaf only, and the leaf does not hold it.
            Find::NotFound { .. } => return Ok(Some(Removed { found: false, slot: None })),
        };
        match value {
            Val::Inline(_) => {}
            // The run must be freed with the tree's other changes: descend.
            Val::Overflow(_) => return Ok(None),
            Val::Nested(root) => return Err(page::nested_error(root)),
        }
        if view.nkeys() < 2 {
            return Ok(None);
        }
        if merge && merge_due(depth, leaf_used_after_delete(&view, key, value), usable) {
            return Ok(None);
        }
        index
    };
    page.delete(index)?;
    root.entries -= 1;
    #[cfg(all(test, debug_assertions))]
    CACHED_DELETES.with(|n| n.set(n.get() + 1));
    Ok(Some(Removed { found: true, slot: Some(index) }))
}

/// Test-only forms of [`put`] and [`delete`] for trees whose holder is not tracked: the root is
/// taken as trusted exactly when it is private, which holds in a test that never forges a root.
#[cfg(test)]
pub(crate) mod plain {
    use super::*;

    /// [`super::put_sized`] with a table's size checks ([`Geometry::place`]: a large value goes
    /// to an overflow run), trusting a private root.
    pub(crate) fn put<S: PageWrite + ?Sized>(
        store: &mut S,
        root: &mut TreeRoot,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        store.geometry().place(key.len(), value.len())?;
        let mut trusted = !root.is_empty() && store.is_private(root.root);
        super::put_sized(store, root, &mut trusted, key, value)
    }

    /// [`super::delete`], trusting a private root.
    pub(crate) fn delete<S: PageWrite + ?Sized>(
        store: &mut S,
        root: &mut TreeRoot,
        key: &[u8],
    ) -> Result<bool> {
        let mut trusted = !root.is_empty() && store.is_private(root.root);
        super::delete(store, root, &mut trusted, key)
    }

    /// [`super::clear`], trusting a private root.
    pub(crate) fn clear<S: PageWrite + ?Sized>(store: &mut S, root: &mut TreeRoot) -> Result<()> {
        let trusted = !root.is_empty() && store.is_private(root.root);
        super::clear(store, root, trusted)
    }
}

/// The deepest path a write records: a step per branch of the tallest tree accepted. A branch
/// has two children or more, so a file of at most 2^32 pages holds trees of height 33 or less;
/// [`TreeRoot::check`] refuses a height over [`MAX_HEIGHT`].
const MAX_PATH: usize = MAX_HEIGHT as usize;

/// The branches of a root-to-leaf path, root first, as `(pgno, child index taken)`: a fixed
/// array, so a write allocates nothing for it.
#[derive(Debug)]
struct Path {
    steps: [(u32, usize); MAX_PATH],
    len: usize,
}

impl Path {
    fn new() -> Self {
        Self { steps: [(0, 0); MAX_PATH], len: 0 }
    }

    /// Add the next step down; [`Error::Corrupt`] past [`MAX_PATH`].
    fn push(&mut self, step: (u32, usize)) -> Result<()> {
        let slot = self.steps.get_mut(self.len).ok_or_else(|| {
            Error::Corrupt(format!("a tree path deeper than {MAX_PATH} branches"))
        })?;
        *slot = step;
        self.len += 1;
        Ok(())
    }

    fn pop(&mut self) -> Option<(u32, usize)> {
        self.len = self.len.checked_sub(1)?;
        Some(self.steps[self.len])
    }

    fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }
}

impl std::ops::Deref for Path {
    type Target = [(u32, usize)];

    fn deref(&self) -> &Self::Target {
        &self.steps[..self.len]
    }
}

impl std::ops::DerefMut for Path {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.steps[..self.len]
    }
}

#[cfg(all(test, debug_assertions))]
thread_local! {
    /// Test-only: how many `keys_in_range` checks the writer's descent made.
    pub(crate) static RANGE_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A key range `[lo, hi)` from separators (`None`: unbounded).
type Bounds<'s> = (Option<&'s [u8]>, Option<&'s [u8]>);

/// The leaf a [`descend_write`] reached.
struct WriteLeaf<'s> {
    pgno: u32,
    page: &'s [u8],
    view: page::LeafView<'s>,
    /// The keys that route to the leaf, `[lo, hi)`: the intersection of every ancestor's
    /// separators. Once the descent reached an untrusted page it kept that intersection (as
    /// [`read::descend`] does); on a path of trusted pages it is the nearest separator either
    /// side, which a trusted page's keys lie within by construction. On a path the descent
    /// accepts the two agree: an untrusted page's keys lie within its ancestors' intersection
    /// (`keys_in_range`).
    range: Bounds<'s>,
    /// [`Verify::Later`] skipped a published page on the path that this writer did not stamp:
    /// [`verify_path`] verifies it before the write trusts or copies it.
    unverified: bool,
}

/// When [`descend_write`] verifies the published pages on its path that this writer did not
/// stamp ([`verify_published`] and [`check_cells_naming_this_txn`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verify {
    /// As it reaches each one, before it reads the page's cells: a put copies every page on its
    /// path.
    Now,
    /// Not at all: the caller runs [`verify_path`] once it knows the write goes ahead. A delete
    /// that misses copies nothing, so it verifies nothing either.
    Later,
}

/// The writer's descent from `root` to the leaf for `key`, recording the branches on `path`;
/// returns the leaf and how many published pages are on the path (each one the write copies).
/// `verify` says when the published pages are verified ([`Verify`]).
///
/// **Provenance trust.** A page is trusted when this transaction's writer built it from checked
/// pages: `trusted(root) = root_trusted && is_private(root)`, and `trusted(child) =
/// trusted(parent) && is_private(child)`. A private page named by an untrusted parent (or an
/// untrusted root handle) is [`Error::Corrupt`]: in a sound tree a published page never names a
/// page this transaction allocated, and only the writer's own pages and holders do. Without the
/// rule a forged cell, or an unchanged root record, naming a page that this transaction allocated
/// for another tree would let the write change that tree's page.
///
/// Every page gets the identity check (header page number and txn equal to its parent's cell's
/// `child` and `child_txn`, or the root's), kind-versus-depth, and non-root-leaf-not-empty. An
/// untrusted page also gets [`read::checked_page`]'s checks, [`verify_published`] unless this
/// writer stamped it (with [`Verify::Later`], [`verify_path`] does that), and `keys_in_range`
/// against the intersection of every ancestor's separators (as [`read::descend`] keeps it),
/// computed only once the descent first reaches an untrusted page: until then the separators are
/// not needed. A page [`verify_published`] checks also gets [`check_cells_naming_this_txn`], so
/// its private copy names no page or run by this transaction's id that the copy-time trust would
/// not vouch for.
fn descend_write<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    verify: Verify,
    path: &mut Path,
) -> Result<(WriteLeaf<'s>, usize)> {
    let geometry = store.geometry();
    let (mut pgno, mut want_txn) = (root.root, root.root_txn);
    let mut parent_trusted = root_trusted;
    let (mut copies, mut unverified) = (0, false);
    // The intersected separators, once an untrusted page needed them.
    let mut bounds: Option<Bounds<'s>> = None;
    let mut range: Bounds<'s> = (None, None);
    for depth in 1..=root.height {
        let trusted = store.is_private(pgno);
        if trusted && !parent_trusted {
            return Err(Error::Corrupt(format!(
                "page {pgno} is private to this transaction but named by a page it did not write"
            )));
        }
        let page = if trusted {
            store.private_page(pgno)?
        } else {
            copies += 1;
            store.page(pgno)?
        };
        let txn = page::txn(page);
        if page::pgno(page) != pgno || txn as u32 != want_txn {
            return Err(Error::Corrupt(format!(
                "page {pgno} (txn {txn}) is not the copy its parent names (txn {want_txn}): a \
                 redirected or stale pointer"
            )));
        }
        let at_leaf_level = depth == root.height;
        let view = page::View::new(page)?;
        if matches!(view, page::View::Leaf(_)) != at_leaf_level {
            return Err(Error::Corrupt(format!(
                "page {pgno} is a {:?} at depth {depth} of a tree of height {}",
                page::kind(page)?,
                root.height
            )));
        }
        if !trusted {
            if !store.is_stamped(pgno) {
                match verify {
                    Verify::Now => {
                        verify_published(store, page, &geometry)?;
                        check_cells_naming_this_txn(store, pgno, &view)?;
                    }
                    Verify::Later => unverified = true,
                }
            }
            let (lo, hi) = match bounds {
                Some(b) => b,
                None => *bounds.insert(path_bounds(store, path)?),
            };
            #[cfg(all(test, debug_assertions))]
            RANGE_CHECKS.with(|n| n.set(n.get() + 1));
            if !view.keys_in_range(lo, hi)? {
                return Err(Error::Corrupt(format!(
                    "page {pgno} holds keys outside its parents' separators"
                )));
            }
        }
        match view {
            page::View::Leaf(view) => {
                if depth > 1 && view.nkeys() == 0 {
                    return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
                }
                let range = bounds.unwrap_or(range);
                return Ok((WriteLeaf { pgno, page, view, range, unverified }, copies));
            }
            page::View::Branch(view) => {
                let route = view.route(key)?;
                if let Some((lo, hi)) = bounds.as_mut() {
                    tighten(&route, lo, hi);
                }
                range = (route.lo.or(range.0), route.hi.or(range.1));
                path.push((pgno, route.index))?;
                (pgno, want_txn) = (route.child, route.child_txn);
            }
        }
        parent_trusted = trusted;
    }
    // The loop returns at depth == height (checked to be >= 1 by `root.check`).
    Err(Error::Corrupt(format!("descent of a tree of height {} found no leaf", root.height)))
}

/// A published branch the write will copy must not name a child by this transaction's `child_txn`
/// unless the child is a live published page: its header names the cell's page number and a txn
/// with the cell's low 32 bits but below this transaction's (a child from 2^32 transactions ago).
/// The copy is private, so trusted, and a trusted page's private children are trusted
/// ([`descend_write`]); without this rule a forged cell naming `(X, txn)`, where `X` is a free page
/// this transaction allocates later for another tree, would pass the copy (`X` is not private
/// yet) and let a later write through it change that tree's page. It refuses a free page's stale
/// bytes (another txn) and any page already private here (this txn). Only a cell whose
/// `child_txn` matches reads its child, so the usual cost is one compare per cell.
///
/// A published leaf obeys the same rule for its overflow cells: one naming a run by this
/// transaction's id must name a live published run, whose header txn is below this
/// transaction's. Otherwise the private copy is trusted, and a replace or delete through the
/// cell would free a run this transaction wrote for another entry (a private run named by a
/// private leaf is one this writer may free).
///
/// It runs where [`verify_published`] does: a page this writer stamped holds only cells it wrote
/// from checked pages (the trust that skips the CRC and [`page::validate`] there), and a forged
/// cell is damage at rest.
fn check_cells_naming_this_txn<S: PageWrite + ?Sized>(
    store: &S,
    pgno: u32,
    view: &page::View<'_>,
) -> Result<()> {
    let txn = store.txn();
    let forged = |what: String, header_txn: u64| {
        Error::Corrupt(format!(
            "published page {pgno} names {what} with this txn's id {}, but it holds txn \
             {header_txn}: a forged or stale pointer",
            txn as u32
        ))
    };
    match view {
        page::View::Branch(branch) => {
            for i in 0..branch.nkeys() {
                let (child, child_txn) = branch.child(i)?;
                if child_txn != txn as u32 {
                    continue;
                }
                let header_txn = page::txn(store.page(child)?);
                if header_txn as u32 != child_txn || header_txn >= txn {
                    return Err(forged(format!("page {child}"), header_txn));
                }
            }
        }
        page::View::Leaf(leaf) => {
            for i in 0..leaf.nkeys() {
                let ovf = match leaf.entry(i)?.1 {
                    Val::Inline(_) => continue,
                    Val::Overflow(ovf) => ovf,
                    Val::Nested(root) => return Err(page::nested_error(root)),
                };
                if ovf.txn != txn as u32 {
                    continue;
                }
                // The run's checks include its identity (`ovf.txn`) against its header.
                let header_txn = overflow::run_txn(store.run(ovf, RunCrc::Skip)?);
                if header_txn >= txn {
                    return Err(forged(format!("overflow run {}", ovf.pgno), header_txn));
                }
            }
        }
    }
    Ok(())
}

/// The intersection of the separators of every step of `path`, as [`read::descend`] keeps it:
/// each step's [`page::Route`] read again with [`page::BranchView::route_at`], which gives the
/// bounds `route` gave. The steps are trusted pages (the first untrusted page asks for this).
fn path_bounds<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    path: &[(u32, usize)],
) -> Result<Bounds<'s>> {
    let (mut lo, mut hi) = (None, None);
    for &(p, ci) in path {
        let route = page::BranchView::new(store.private_page(p)?)?.route_at(ci)?;
        tighten(&route, &mut lo, &mut hi);
    }
    Ok((lo, hi))
}

/// The verification [`Verify::Later`] left out of a [`descend_write`] to `leaf` along `path`:
/// [`verify_published`] and [`check_cells_naming_this_txn`] of each published page on it that
/// this writer did not stamp, root first. The descent made the other checks of every page.
fn verify_path<S: PageWrite + ?Sized>(store: &S, path: &[(u32, usize)], leaf: u32) -> Result<()> {
    let geometry = store.geometry();
    for p in path.iter().map(|&(p, _)| p).chain([leaf]) {
        if store.is_private(p) || store.is_stamped(p) {
            continue;
        }
        let page = store.page(p)?;
        verify_published(store, page, &geometry)?;
        check_cells_naming_this_txn(store, p, &page::View::new(page)?)?;
    }
    Ok(())
}

/// What [`plan`] decided for a put.
struct Planned<'s> {
    /// The leaf the entry goes to.
    leaf: u32,
    /// The change to it.
    change: LeafChange,
    /// How many tree pages the put can need (copies and the split's worst case).
    needed: usize,
    /// The leaf's key range ([`WriteLeaf::range`]).
    range: Bounds<'s>,
    /// The overflow run of the value a replace takes out of the tree, `(first page, pages)`,
    /// checked by [`leaving_run`].
    old_run: Option<(u32, u32)>,
}

/// The read-only first half of a put of a `vlen`-byte cell value (the value, or an overflow
/// reference): descend to the leaf for `key` with the writer's checks ([`descend_write`]), and
/// decide the change ([`Planned`]). A replace of an overflow value checks the run it frees
/// ([`leaving_run`]).
///
/// An insert ([`PutKind::Insert`]) of a key the leaf holds is refused ([`Error::KeyExists`]).
/// An append ([`PutKind::Append`]) is refused ([`Error::AppendOutOfOrder`]) unless the key lands
/// past the last key of the rightmost leaf (no upper bound). A key past the tree's last key routes
/// there, since no separator exceeds the keys to its right; a key that lands in another leaf lies
/// below that leaf's upper bound, so below the keys of the (non-empty) leaves to its right.
fn plan<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    vlen: usize,
    kind: PutKind,
    path: &mut Path,
) -> Result<Planned<'s>> {
    let (leaf, copies) = descend_write(store, root, root_trusted, key, Verify::Now, path)?;
    let view = leaf.view;
    let found = match kind {
        PutKind::Upsert => view.find(key)?,
        PutKind::Insert => match view.find(key)? {
            Find::Found { .. } => return Err(Error::KeyExists),
            missing => missing,
        },
        PutKind::Append => {
            let n = view.nkeys();
            let past_last = match n.checked_sub(1) {
                Some(last) => page::cmp_keys(key, view.key(last)?).is_gt(),
                None => true,
            };
            if leaf.range.1.is_some() || !past_last {
                return Err(Error::AppendOutOfOrder);
            }
            Find::NotFound { index: n, before: None, after: None }
        }
    };
    let mut old_run = None;
    let change = match found {
        Find::Found { index: i, value: old } => {
            match old {
                Val::Inline(_) => {}
                Val::Overflow(ovf) => {
                    old_run = Some((ovf.pgno, leaving_run(store, leaf.pgno, ovf)?));
                }
                Val::Nested(root) => return Err(page::nested_error(root)),
            }
            let old = old.cell_len();
            if vlen <= old || vlen - old <= view.free_space() {
                LeafChange::Replace(i)
            } else {
                LeafChange::Split { at: i, replace: true, append: false }
            }
        }
        Find::NotFound { index: at, .. } => {
            if view.fits(page::cell_size_leaf(key.len(), vlen)) {
                LeafChange::Insert(at)
            } else {
                // An append's leaf is the rightmost, with no need to read the path again: it was
                // refused above unless its range has no upper bound, which the descent leaves
                // exactly when every branch routed to its last child ([`page::BranchView::route`]).
                debug_assert!(
                    kind != PutKind::Append || matches!(is_rightmost(store, path), Ok(true)),
                    "an append's unbounded leaf is not on the rightmost path"
                );
                let append =
                    at == view.nkeys() && (kind == PutKind::Append || is_rightmost(store, path)?);
                LeafChange::Split { at, replace: false, append }
            }
        }
    };
    let split = match change {
        LeafChange::Split { append, .. } => {
            split_pages(store, root, path, leaf.page, key.len(), append)?
        }
        _ => 0,
    };
    Ok(Planned { leaf: leaf.pgno, change, needed: copies + split, range: leaf.range, old_run })
}

/// The page count of the overflow run `ovf`, named by leaf `leaf`, which a replace or a delete
/// takes out of the tree (or a free of the tree, [`tree_pages`]): checked as a free reads a run
/// (its header and identity, [`PageRead::run`] without the CRC: the value is not read), and by
/// provenance. A run this transaction wrote may leave only through a leaf it wrote too (a
/// private leaf); one named by a leaf it did not write, or one overlapping its private pages
/// without being one of its runs, is [`Error::Corrupt`]: freeing it would free pages this
/// transaction still uses.
fn leaving_run<S: PageWrite + ?Sized>(store: &S, leaf: u32, ovf: Ovf) -> Result<u32> {
    store.run(ovf, RunCrc::Skip)?;
    // The check bounds the length, so the count fits.
    let n = overflow::run_pages(ovf.len as usize, store.geometry().page_size()) as u32;
    match store.run_privacy(ovf.pgno, n) {
        Some(false) => Ok(n),
        Some(true) if store.is_private(leaf) => Ok(n),
        Some(true) => Err(Error::Corrupt(format!(
            "overflow run {} is private to this transaction but named by leaf {leaf}, which it \
             did not write",
            ovf.pgno
        ))),
        None => Err(Error::Corrupt(format!(
            "overflow run {} of {n} pages overlaps pages private to this transaction",
            ovf.pgno
        ))),
    }
}

/// A published page about to be copied must pass its CRC and [`page::validate`]: the copy is
/// stamped afresh at commit, so copying a page damaged at rest would make the damage CRC-valid
/// and invisible, and the mutators keep a page valid only if it was valid. The check and its
/// time are counted on `store` ([`PageWrite::count_verify`]).
fn verify_published<S: PageWrite + ?Sized>(
    store: &S,
    page: &[u8],
    geometry: &Geometry,
) -> Result<()> {
    let started = Instant::now();
    let checked = check_published(page, geometry);
    store.count_verify(elapsed_ns(started));
    checked
}

/// [`verify_published`]'s checks.
fn check_published(page: &[u8], geometry: &Geometry) -> Result<()> {
    #[cfg(all(test, debug_assertions))]
    COW_VERIFIES.with(|n| n.set(n.get() + 1));
    if crc::crc_state(page) != CrcState::Valid {
        return Err(Error::Corrupt(format!(
            "page {} fails its CRC (at-rest corruption); refusing to copy it",
            page::pgno(page)
        )));
    }
    page::validate(page, geometry)
}

/// True if `path` is the tree's rightmost path (every step takes the last child).
fn is_rightmost<S: PageRead + ?Sized>(store: &S, path: &[(u32, usize)]) -> Result<bool> {
    for &(p, ci) in path {
        if ci + 1 != page::nkeys(store.page(p)?) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The length of the longest key of a page (one header parse).
fn longest_key(page: &[u8]) -> Result<usize> {
    let mut longest = 0;
    match page::View::new(page)? {
        page::View::Leaf(view) => {
            for i in 0..view.nkeys() {
                longest = longest.max(view.key(i)?.len());
            }
        }
        page::View::Branch(view) => {
            for i in 0..view.nkeys() {
                longest = longest.max(view.key(i)?.len());
            }
        }
    }
    Ok(longest)
}

/// The worst-case page count of splitting the full `leaf` on `path`: the new right leaf, a new
/// right branch for each ancestor the pending separator may not fit (the split climbs while it
/// does not), and a new root if the root splits. Fullness is by bytes, so it depends on the
/// separator's length, unknown until the split picks it; but a separator is a key of the page
/// that splits or the entry it receives, so the longest of those bounds it at every level. An
/// `append` split's leaf separator is the new key itself ([`page::split_leaf`]).
fn split_pages<S: PageRead + ?Sized>(
    store: &S,
    root: &TreeRoot,
    path: &[(u32, usize)],
    leaf: &[u8],
    klen: usize,
    append: bool,
) -> Result<usize> {
    let mut sep_bound = if append { klen } else { klen.max(longest_key(leaf)?) };
    let mut needed = 1;
    for &(p, _) in path.iter().rev() {
        let branch = store.page(p)?;
        if page::fits(branch, page::cell_size_branch(sep_bound))? {
            return Ok(needed);
        }
        needed += 1;
        sep_bound = sep_bound.max(longest_key(branch)?);
    }
    if root.height >= MAX_HEIGHT {
        return Err(Error::Failed(format!("the tree is at the height limit of {MAX_HEIGHT}")));
    }
    Ok(needed + 1)
}

/// A writer's buffers for a put or delete that copies or splits pages, reused from one to the
/// next ([`PageWrite::take_scratch`]) so such a write allocates no memory once they have grown.
#[derive(Debug, Default)]
pub(crate) struct Scratch {
    /// The pages [`reserve`] took for the write; empty between writes.
    reserved: Vec<u32>,
    /// The splits' buffers.
    split: page::SplitBuf,
}

/// Allocate `n` pages for a put into `pages` (empty), or none: on a failure the pages taken so
/// far go back (they are private, so straight to the pool) and nothing else has changed.
fn reserve<S: PageWrite + ?Sized>(store: &mut S, n: usize, pages: &mut Vec<u32>) -> Result<()> {
    debug_assert!(pages.is_empty(), "reserve into {} pages already reserved", pages.len());
    for _ in 0..n {
        match store.alloc() {
            Ok(p) => pages.push(p),
            Err(e) => {
                while let Some(p) = pages.pop() {
                    store.free(p);
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Allocate a put's pages: `n` tree pages into `pages` ([`reserve`]), then, for a value stored in
/// an overflow run of `run_pages` pages, the run, with `value` written into it before any leaf
/// names it ([`write_run`]). Returns the run's reference. On a failure everything taken goes back
/// and nothing else has changed.
fn reserve_put<S: PageWrite + ?Sized>(
    store: &mut S,
    n: usize,
    pages: &mut Vec<u32>,
    run_pages: Option<u32>,
    value: &[u8],
) -> Result<Option<Ovf>> {
    reserve(store, n, pages)?;
    let Some(run_pages) = run_pages else {
        return Ok(None);
    };
    match write_run(store, run_pages, value) {
        Ok(ovf) => Ok(Some(ovf)),
        Err(e) => {
            while let Some(p) = pages.pop() {
                store.free(p);
            }
            Err(e)
        }
    }
}

/// A new overflow run of `npages` pages holding `value` ([`overflow::encode_run`]), written in
/// full now, before any leaf names it; its CRC is stamped at commit. A failure changes nothing.
fn write_run<S: PageWrite + ?Sized>(store: &mut S, npages: u32, value: &[u8]) -> Result<Ovf> {
    let len = u32::try_from(value.len()).map_err(|_| bug("a value of over 4 GiB in a run"))?;
    let p = store.alloc_run(npages)?;
    let (txn, page_size) = (store.txn(), store.geometry().page_size());
    match store.run_mut(p, npages) {
        Ok(run) => overflow::encode_run(run, page_size, p, txn, value),
        Err(e) => {
            store.free_run(p, npages);
            return Err(e);
        }
    }
    Ok(Ovf { pgno: p, txn: txn as u32, len })
}

/// The writable version of page `p`: `p` itself if private, else a copy in a reserved page
/// ([`PageWrite::copy_published`], one copy), re-stamped with its new page number and this
/// transaction's id and its trailer zeroed (dirty); the original is freed. (The planning descent
/// verified it.)
fn writable<S: PageWrite + ?Sized>(store: &mut S, p: u32, scratch: &mut Scratch) -> Result<u32> {
    if store.is_private(p) {
        return Ok(p);
    }
    let q = scratch.reserved.pop().ok_or_else(|| bug("copy-on-write ran out of reserved pages"))?;
    let txn = store.txn();
    let copy = store.copy_published(p, q)?;
    page::set_pgno_txn(copy, q, txn);
    crc::zero_crc(copy);
    store.free(p);
    Ok(q)
}

/// Copy page `src` into private page `to`'s bytes `dst` ([`PageWrite::copy_published`]): both
/// are one page, else a broken invariant ([`Error::Failed`]), never a panic.
pub(crate) fn copy_page(src: &[u8], to: u32, dst: &mut [u8]) -> Result<()> {
    if dst.len() != src.len() {
        return Err(bug(format!("page {to} of {} bytes, not {}", dst.len(), src.len())));
    }
    dst.copy_from_slice(src);
    Ok(())
}

/// Make the planned root-to-leaf path writable, top-down: a copied child is re-pointed in its
/// (already writable) parent with its new `child_txn`, and a copied root becomes the tree's root.
/// `path` is updated to the writable branches; returns the writable leaf.
fn make_path_writable<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut [(u32, usize)],
    leaf: u32,
    scratch: &mut Scratch,
) -> Result<u32> {
    let txn = store.txn() as u32;
    let mut parent: Option<(u32, usize)> = None;
    let mut current = leaf;
    for depth in 0..=path.len() {
        let old = path.get(depth).map_or(leaf, |&(p, _)| p);
        current = writable(store, old, scratch)?;
        if current != old {
            match parent {
                None => (root.root, root.root_txn) = (current, txn),
                Some((p, ci)) => page::branch_set_child(store.page_mut(p)?, ci, current, txn)?,
            }
        }
        if let Some(step) = path.get_mut(depth) {
            step.0 = current;
            parent = Some(*step);
        }
    }
    Ok(current)
}

/// What [`apply`] did.
struct Applied {
    /// It replaced a present key's value.
    replaced: bool,
    /// After a split whose separator the leaf's parent took (no branch split, no new root): the
    /// new right leaf, which `path` then leads to (the parent's step moved to the leaf's new cell,
    /// the steps above it unchanged). `None` after any other change.
    right_leaf: Option<u32>,
}

/// Apply the planned change to the writable `leaf` with the cell value `value` (the value, or the
/// reference to its run), splitting up the writable `path` with pages from `scratch.reserved`,
/// and count each split's new page and a new root in `root`.
#[allow(clippy::too_many_arguments)]
fn apply<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut Path,
    leaf: u32,
    key: &[u8],
    value: Val<'_>,
    change: LeafChange,
    scratch: &mut Scratch,
) -> Result<Applied> {
    let fit = |fit: Fit, what: &str| match fit {
        Fit::Done => Ok(()),
        Fit::Full => Err(bug(format!("{what} did not fit as planned"))),
    };
    let (at, replace, append) = match change {
        LeafChange::Replace(i) => {
            fit(page::LeafMut::new(store.page_mut(leaf)?)?.replace_value(i, value)?, "a value")?;
            return Ok(Applied { replaced: true, right_leaf: None });
        }
        LeafChange::Insert(i) => {
            fit(page::LeafMut::new(store.page_mut(leaf)?)?.insert(i, key, value)?, "an entry")?;
            root.entries += 1;
            return Ok(Applied { replaced: false, right_leaf: None });
        }
        LeafChange::Split { at, replace, append } => (at, replace, append),
    };
    if replace {
        page::leaf_delete(store.page_mut(leaf)?, at)?;
    }
    let txn = store.txn();
    let txn32 = txn as u32;
    let Scratch { reserved, split, .. } = scratch;
    let mut take = || reserved.pop().ok_or_else(|| bug("a split ran out of reserved pages"));

    // Split the leaf: plan from it, then write the right half into its new page (whatever bytes
    // the allocation left: the layout writes all but the free gap) and the left half over the
    // leaf.
    let mut right = take()?;
    split.split_leaf(store.page_mut(leaf)?, at, key, value, append)?;
    split.write_right(store.page_mut(right)?, right, txn)?;
    split.write_left(store.page_mut(leaf)?)?;
    root.add_page(Kind::Leaf)?;

    // Insert the separator up the path, splitting each branch it does not fit; `placed` is the
    // step of the branch that took it, at the new cell.
    let new_leaf = right;
    let mut placed = None;
    while let Some((p, ci)) = path.pop() {
        if page::branch_insert(store.page_mut(p)?, ci + 1, split.sep(), right, txn32)? == Fit::Done
        {
            placed = Some((p, ci + 1));
            break;
        }
        let q = take()?;
        split.split_branch(store.page_mut(p)?, ci + 1, right, txn32, append)?;
        split.write_right(store.page_mut(q)?, q, txn)?;
        split.write_left(store.page_mut(p)?)?;
        root.add_page(Kind::Branch)?;
        right = q;
    }
    let right_leaf = match placed {
        // The leaf's parent took it (no branch split moved `right` on): the path above it is
        // unchanged, and the parent's step at the new cell leads to the new leaf.
        Some(step) if right == new_leaf => {
            path.push(step)?;
            Some(new_leaf)
        }
        Some(_) => None,
        // The root split: grow a new root one level up.
        None => {
            let q = take()?;
            let sep = split.sep();
            page::init_root(store.page_mut(q)?, q, txn, (root.root, txn32), sep, (right, txn32))?;
            (root.root, root.root_txn) = (q, txn32);
            root.height += 1;
            root.add_page(Kind::Branch)?;
            None
        }
    };
    if !replace {
        root.entries += 1;
    }
    Ok(Applied { replaced: replace, right_leaf })
}

/// What a delete does to the tree once the entry is gone from its leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeleteShape {
    /// The leaf keeps other entries: only the leaf changes.
    Leaf,
    /// The leaf empties: path step `d` is the deepest branch with another child, and loses the
    /// child the path took; the emptied pages below it leave the tree.
    Unlink(usize),
    /// The leaf empties and the root is left with one child: the tree below that child (past
    /// any single-child branches) becomes the tree; the root and every page passed leave it.
    Collapse { root: u32, root_txn: u32, height: u32 },
    /// The leaf held the tree's last entry: the tree becomes [`TreeRoot::EMPTY`], every page on
    /// the path (the whole tree) leaving it.
    Empty,
}

/// What [`plan_delete`] decided.
#[derive(Debug)]
struct DeletePlan {
    /// The leaf holding the key.
    leaf: u32,
    /// The entry's index in the leaf.
    at: usize,
    /// The change to the tree.
    shape: DeleteShape,
    /// The pages that leave the tree (as they are now: published or private).
    dropped: Vec<u32>,
    /// The overflow run of the deleted value, `(first page, pages)`, checked by [`leaving_run`].
    run: Option<(u32, u32)>,
    /// How many published pages the delete copies.
    copies: usize,
    /// The handle the delete leaves: its height, entry count and page counters, and, for a
    /// collapse or an empty tree, its root (copy-on-write of a kept path gives the root page
    /// and txn of the others).
    after: TreeRoot,
    /// The siblings the change's page and its ancestors absorb, bottom-up ([`plan_merges`]);
    /// each is also in `dropped`.
    merges: Vec<Absorb>,
    /// The merges leave the root branch with one child, the page on the path below it, which
    /// becomes the root: the old root leaves the tree (it is in `dropped`, never copied) and the
    /// height drops by one.
    merged_root: bool,
    /// A sibling was read for a merge and did not fit (the test-only [`MergeReport`]).
    #[cfg_attr(not(test), allow(dead_code))]
    refused: bool,
}

/// Test-only: what a delete's merges did ([`PageWrite::merges_applied`]), for the model tests'
/// coverage.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MergeReport {
    /// Leaves absorbed from the left.
    pub(crate) leaf_left: usize,
    /// Leaves absorbed from the right.
    pub(crate) leaf_right: usize,
    /// Branches absorbed.
    pub(crate) branches: usize,
    /// Levels that merged.
    pub(crate) levels: usize,
    /// The root gave way to its single child after the merges.
    pub(crate) root: bool,
    /// A sibling was read and did not fit.
    pub(crate) refused: bool,
}

#[cfg(test)]
impl MergeReport {
    /// The report of `plan`.
    fn of(plan: &DeletePlan) -> Self {
        let m = &plan.merges;
        let leaf = |side| m.iter().filter(|a| a.leaf && a.side == side).count();
        // The absorptions come level by level, bottom-up; no allocation on the bench's timed path.
        let levels = m.iter().enumerate().filter(|&(i, a)| i == 0 || m[i - 1].level != a.level);
        Self {
            leaf_left: leaf(Side::Left),
            leaf_right: leaf(Side::Right),
            branches: m.iter().filter(|a| !a.leaf).count(),
            levels: levels.count(),
            root: plan.merged_root,
            refused: plan.refused,
        }
    }
}

/// Which neighbour of a page on a delete's path a merge absorbs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    /// The sibling just left of it under the same parent.
    Left,
    /// The sibling just right of it.
    Right,
}

/// One sibling a delete's merge absorbs into the page on its path ([`plan_merges`]).
#[derive(Clone, Copy, Debug)]
struct Absorb {
    /// The parent's path step: the page that absorbs is the one at path index `level + 1`.
    level: usize,
    /// The sibling, freed as it is once its cells are in the page on the path.
    sibling: u32,
    /// Where the sibling is.
    side: Side,
    /// Whether the pair are leaves (else branches).
    leaf: bool,
}

/// The pages a delete's merges take out of the tree, for [`handle_after`].
#[derive(Clone, Copy, Debug, Default)]
struct Merged {
    /// Leaves absorbed.
    leaves: u32,
    /// Branches absorbed.
    branches: u32,
    /// The root gave way to its single child ([`DeletePlan::merged_root`]).
    root: bool,
}

/// Delete `key` from the tree `root` names, returning whether it was present. One read-only
/// descent ([`plan_delete`]) finds the key, each page on the path checked as a put checks it. A
/// miss ends there, after the check a lookup makes of a miss ([`get`]): it verifies, copies,
/// allocates and frees nothing.
///
/// A hit verifies the published pages on the path before the delete trusts or copies them,
/// plans the change, reserves the copies of the published pages that stay in the tree, and only
/// then changes anything, so a failed allocation leaves the tree exactly as it was. The entry
/// leaves its leaf; a leaf it empties is unlinked from its parent, and a parent left with no child
/// from its own, up the path; a root branch left with a single child gives way to it
/// ([`DeleteShape::Collapse`]). A delete of the last entry leaves [`TreeRoot::EMPTY`] (no pages),
/// as an empty tree always is. Emptied pages are never copied: they leave the tree as they are, a
/// published one freed and a private one reusable at once.
///
/// A page the change leaves under a quarter full merges with its same-parent siblings while they
/// fit, and a branch that loses a cell to a merge or an unlink gets the same test, up the path
/// ([`plan_merges`]). The page on the path survives (it is copied anyway); a sibling's cells move
/// into it and the sibling leaves the tree as it is, so a merge allocates nothing and the
/// reservation is the path's copies. A root branch the merges leave with one child gives way to
/// it. A deleted value's overflow run leaves the tree with the emptied pages; a merge moves an
/// overflow cell, never its run. `root` is updated (root page, height, entry count, page
/// counters).
pub(crate) fn delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    key: &[u8],
) -> Result<bool> {
    Ok(delete_from_root(store, root, trusted, None, key)?.found)
}

/// [`delete`], with a table's `cache` if given: the descent records its path straight into the
/// cache's buffer ([`PathCache::lend_path`], which empties the cache), and the cache is filled
/// again with the entry's leaf and the path to it (private and trusted once the delete is done)
/// after a [`DeleteShape::Leaf`] delete that merged nothing, with the epoch its copies left; a
/// miss fills it when every page on its path is this transaction's own (nothing copied, nothing
/// verified, the epoch unchanged). Any other delete (an unlink, a collapse, an empty tree, a
/// merge) frees pages and leaves the cache empty.
fn delete_from_root<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    cache: Option<&mut PathCache>,
    key: &[u8],
) -> Result<Removed> {
    const MISS: Removed = Removed { found: false, slot: None };
    root.check()?;
    if root.is_empty() {
        return Ok(MISS);
    }
    let mut own_path = None;
    let (path, mut tail) = match cache {
        Some(cache) => {
            let (path, tail) = cache.lend_path();
            (path, Some(tail))
        }
        None => (own_path.insert(Path::new()), None),
    };
    let Some(plan) = plan_delete(&*store, root, *trusted, key, path, tail.as_mut())? else {
        return Ok(MISS);
    };
    let buffers = plan.copies != 0 || !plan.merges.is_empty();
    let mut scratch = if buffers { store.take_scratch() } else { Scratch::default() };
    // A failed reservation gives its pages back itself; the buffers go back either way.
    let result = reserve(store, plan.copies, &mut scratch.reserved)
        .and_then(|()| apply_delete(store, root, path, &plan, &mut scratch));
    // Every reserved page is used, so this frees nothing unless the apply failed part way.
    for p in scratch.reserved.drain(..) {
        store.free(p);
    }
    if buffers {
        store.put_scratch(scratch);
    }
    let changed = result?;
    // Only a delete that merged or read a sibling reports: on the release benchmark's timed path
    // (compiled with the test configuration) the hook costs a branch.
    #[cfg(test)]
    if !plan.merges.is_empty() || plan.refused {
        store.merges_applied(MergeReport::of(&plan));
    }
    // A kept path is now private and built here (a merge's survivor that became the root too); a
    // collapse's new root is trusted exactly when it is private (`collapse_to` refused a private
    // page below an untrusted one).
    *trusted = match plan.shape {
        DeleteShape::Leaf | DeleteShape::Unlink(_) => true,
        DeleteShape::Collapse { root: new_root, .. } => store.is_private(new_root),
        DeleteShape::Empty => false,
    };
    // The entry left its leaf and nothing else changed: `path` leads to the leaf, private now,
    // whose range `plan_delete` copied into the cache. Its follower is at the entry's old slot.
    let slot = match (plan.shape, changed, tail) {
        (DeleteShape::Leaf, Some(leaf), Some(mut tail)) if plan.merges.is_empty() => {
            tail.fill(store, leaf, path)?;
            Some(plan.at)
        }
        _ => None,
    };
    Ok(Removed { found: true, slot })
}

/// The read-only first half of a delete of `key` from a non-empty tree: descend to its leaf with
/// the writer's checks ([`descend_write`]) and look the key up there. A miss is `None`, once the
/// keys either side of where it would go lie inside the leaf's separators ([`check_miss`], as a
/// lookup's miss is checked), with nothing verified ([`Verify::Later`]). A hit verifies the
/// published pages on the path ([`verify_path`]) before anything reads them further, and decides
/// the change.
///
/// With a table's cache (`tail`, its path being `path`), a hit copies the leaf's range into it
/// before anything changes, for [`delete_from_root`] to fill it once the delete is done; a miss
/// fills it at once when the descent met only this transaction's own pages from a trusted root
/// (the path a put's descent caches), since a miss changes nothing.
fn plan_delete<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    path: &mut Path,
    mut tail: Option<&mut CacheTail<'_>>,
) -> Result<Option<DeletePlan>> {
    let geometry = store.geometry();
    let (leaf, published) = descend_write(store, root, root_trusted, key, Verify::Later, path)?;
    if let Some(tail) = &mut tail {
        tail.set_range(leaf.range);
    }
    let (at, value) = match leaf.view.find(key)? {
        Find::Found { index, value } => (index, value),
        Find::NotFound { before, after, .. } => {
            check_miss(leaf.pgno, before, after, leaf.range.0, leaf.range.1)?;
            if let Some(tail) = tail.filter(|_| root_trusted && published == 0) {
                tail.fill(store, leaf.pgno, path)?;
            }
            return Ok(None);
        }
    };
    if leaf.unverified {
        verify_path(store, path, leaf.pgno)?;
    }
    let run = match value {
        Val::Inline(_) => None,
        Val::Overflow(ovf) => Some((ovf.pgno, leaving_run(store, leaf.pgno, ovf)?)),
        Val::Nested(root) => return Err(page::nested_error(root)),
    };
    let mut dropped = Vec::new();
    let shape = if leaf.view.nkeys() > 1 {
        DeleteShape::Leaf
    } else {
        let mut keeper = None;
        for (d, &(p, _)) in path.iter().enumerate().rev() {
            if page::nkeys(store.page(p)?) > 1 {
                keeper = Some(d);
                break;
            }
        }
        match keeper {
            None => DeleteShape::Empty,
            Some(0) if page::nkeys(store.page(path[0].0)?) == 2 => {
                let (r, ci) = path[0];
                let (child, child_txn) = page::branch_child(store.page(r)?, 1 - ci)?;
                // The root is trusted exactly when private (`descend_write` refused otherwise).
                let trusted = store.is_private(r);
                collapse_to(
                    store,
                    &geometry,
                    child,
                    child_txn,
                    root.height - 1,
                    trusted,
                    &mut dropped,
                )?
            }
            Some(d) => DeleteShape::Unlink(d),
        }
    };
    // The handle's count must agree with the tree: the last entry goes only with an `Empty`, and
    // every other shape leaves an entry (a non-root leaf is never empty).
    let count_agrees = match shape {
        DeleteShape::Empty => root.entries == 1,
        _ => root.entries >= 2,
    };
    if !count_agrees {
        return Err(Error::Corrupt(format!(
            "tree root {root:?} does not count the entries its pages hold ({shape:?})"
        )));
    }
    // The page the change leaves, with the bytes of cells and slots it then holds: the leaf
    // without the entry, or the keeper without the child (a first child takes the next one's
    // key with it, [`page::branch_delete`]).
    let changed = match shape {
        DeleteShape::Leaf => Some((path.len(), leaf_used_after_delete(&leaf.view, key, value))),
        DeleteShape::Unlink(d) => {
            let (p, ci) = path[d];
            let keeper = page::BranchView::new(path_page(store, p)?)?;
            let mut gone = page::cell_size_branch(keeper.key(ci)?.len()) + page::SLOT;
            if ci == 0 && keeper.nkeys() > 1 {
                gone += keeper.key(1)?.len();
            }
            Some((d, keeper.used().saturating_sub(gone)))
        }
        DeleteShape::Collapse { .. } | DeleteShape::Empty => None,
    };
    let (mut merges, mut merged_root, mut refused) = (Vec::new(), false, false);
    if let Some((k, used)) = changed.filter(|_| merging(store)) {
        (merged_root, refused) = plan_merges(store, path, k, used, &mut merges)?;
    }
    let kept = match shape {
        DeleteShape::Leaf => path.len() + 1,
        DeleteShape::Unlink(d) => d + 1,
        DeleteShape::Collapse { .. } | DeleteShape::Empty => 0,
    };
    // A root the merges leave with one child is dropped as it is, not copied.
    let top = usize::from(merged_root);
    let pages = path.iter().map(|&(p, _)| p).chain([leaf.pgno]);
    let copies = pages.clone().take(kept).skip(top).filter(|&p| !store.is_private(p)).count();
    dropped.extend(pages.clone().skip(kept));
    let emptied = dropped.len();
    dropped.extend(pages.take(top));
    dropped.extend(merges.iter().map(|m| m.sibling));
    let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
    let leaves = merges.iter().filter(|m| m.leaf).count();
    let merged =
        Merged { leaves: count(leaves), branches: count(merges.len() - leaves), root: merged_root };
    let run_pages = run.map_or(0, |(_, n)| n);
    let after = handle_after(root, shape, emptied, run_pages, merged)?;
    Ok(Some(DeletePlan {
        leaf: leaf.pgno,
        at,
        shape,
        dropped,
        run,
        copies,
        after,
        merges,
        merged_root,
        refused,
    }))
}

/// The bytes of cells and slots ([`page::LeafView::used`]) a leaf holds once the entry `key`, with
/// `value` as its cell holds it, leaves it. The entry's key is `key`, which its search compared
/// equal.
#[inline]
fn leaf_used_after_delete(view: &page::LeafView<'_>, key: &[u8], value: Val<'_>) -> usize {
    let gone = page::cell_size_leaf(key.len(), value.cell_len()) + page::SLOT;
    view.used().saturating_sub(gone)
}

/// Whether a delete that leaves the page at path index `k` (the leaf when `k` is the path's
/// length, else a branch) holding `used` bytes of cells and slots looks for a sibling to absorb
/// ([`plan_merges`]): the page is not the root and holds under a quarter of `U` (`usable`). The
/// one rule of both delete paths: [`delete_cached`] changes a leaf in place only when it is false.
#[inline]
fn merge_due(k: usize, used: usize, usable: usize) -> bool {
    k >= 1 && 4 * used < usable
}

/// Whether deletes merge under-full pages: always, but for the test-only knob
/// ([`PageWrite::no_merge`]).
#[inline]
fn merging<S: PageWrite + ?Sized>(store: &S) -> bool {
    #[cfg(test)]
    let on = !store.no_merge();
    #[cfg(not(test))]
    let on = {
        let _ = store;
        true
    };
    on
}

/// A page on a write's path, which [`descend_write`] checked: a private page's bytes, or a
/// published page through the checked fetch.
fn path_page<S: PageWrite + ?Sized>(store: &S, p: u32) -> Result<&[u8]> {
    if store.is_private(p) {
        store.private_page(p)
    } else {
        store.page(p)
    }
}

/// A branch's key 1, its least key (`None` for a single child).
fn key1(page: &[u8]) -> Result<Option<&[u8]>> {
    let branch = page::BranchView::new(page)?;
    if branch.nkeys() < 2 {
        return Ok(None);
    }
    Ok(Some(branch.key(1)?))
}

/// Plan the merges of a delete whose change leaves the page at path index `k` (the leaf when `k
/// == path.len()`, else a branch that lost a child) holding `used` bytes of cells and slots;
/// returns whether the root gives way ([`DeletePlan::merged_root`]) and whether a sibling read
/// did not fit ([`DeletePlan::refused`]).
///
/// While that page is not the root and holds under a quarter of `U`, it absorbs a same-parent
/// sibling that fits: the left neighbour first, while one fits, then the right one. A pair fits
/// when the merged page's cells and slots are within `U`: `used + used(S)`, plus for branches the
/// parent's key for the right page of the pair, which comes down as that page's cell-0 key
/// ([`page::SplitBuf::merge`]). It keeps absorbing until it reaches a quarter or no neighbour
/// fits, so the page the change leaves is never under a quarter beside a sibling it would fit
/// into. Each absorption takes the right page's cell out of the parent, which then gets the same
/// test one level up; at the root, a branch left with one child gives way to it (returns true).
///
/// Each sibling is read with a descent's cheap checks before its fit test ([`sibling_view`]),
/// and the one absorbed gets the rest ([`check_sibling`]), plus the key order across the seam:
/// its cells enter a page this transaction trusts. Pushes the absorptions on `merges`,
/// bottom-up. Damage is [`Error::Corrupt`], found before anything changes.
fn plan_merges<S: PageWrite + ?Sized>(
    store: &S,
    path: &[(u32, usize)],
    mut k: usize,
    mut used: usize,
    merges: &mut Vec<Absorb>,
) -> Result<(bool, bool)> {
    let usable = store.geometry().usable();
    let mut refused = false;
    while merge_due(k, used, usable) {
        let (p, ci) = path[k - 1];
        let parent_trusted = store.is_private(p);
        let parent = page::BranchView::new(path_page(store, p)?)?;
        let n = parent.nkeys();
        let leaf_level = k == path.len();
        // Key 1 of the absorbing page's leftmost part: a key coming down from its left must sort
        // below it. (A branch the change shrank keeps key 1 or a greater one.)
        let mut x_key1 = if leaf_level { None } else { key1(path_page(store, path[k].0)?)? };
        let (mut lefts, mut rights, mut lost) = (0, 0, 0);
        let mut side = Side::Left;
        while merge_due(k, used, usable) {
            let si = match side {
                Side::Left => ci.checked_sub(lefts + 1),
                Side::Right => Some(ci + rights + 1).filter(|&i| i < n),
            };
            // The parent's cell for the right page of the pair: its key comes down, the cell goes.
            let candidate = match si {
                Some(si) => {
                    let sep_i = if side == Side::Left { si + 1 } else { si };
                    let sep = parent.key(sep_i)?;
                    let (s, s_txn) = parent.child(si)?;
                    let (page, view) = sibling_view(store, s, s_txn, parent_trusted, leaf_level)?;
                    let size = used + view.used() + if leaf_level { 0 } else { sep.len() };
                    refused |= size > usable;
                    (size <= usable).then_some((si, sep, s, page, view, size))
                }
                None => None,
            };
            let Some((si, sep, s, page, view, size)) = candidate else {
                if side == Side::Right {
                    break;
                }
                side = Side::Right;
                continue;
            };
            check_sibling(store, path, k - 1, si, s, page, &view)?;
            if let page::View::Branch(_) = view {
                let right_key1 = match side {
                    Side::Left => x_key1,
                    Side::Right => key1(page)?,
                };
                if right_key1.is_some_and(|k1| page::cmp_keys(k1, sep).is_le()) {
                    return Err(Error::Corrupt(format!(
                        "a merge of page {s} would bring its parent's key down out of order"
                    )));
                }
                if side == Side::Left {
                    x_key1 = Some(key1(page)?.unwrap_or(sep));
                }
            }
            used = size;
            lost += page::cell_size_branch(sep.len()) + page::SLOT;
            merges.push(Absorb { level: k - 1, sibling: s, side, leaf: leaf_level });
            match side {
                Side::Left => lefts += 1,
                Side::Right => rights += 1,
            }
        }
        if lefts + rights == 0 {
            break;
        }
        if k == 1 {
            return Ok((n - lefts - rights == 1, refused));
        }
        used = parent.used().saturating_sub(lost);
        k -= 1;
    }
    Ok((false, refused))
}

/// A same-parent sibling `s` of a page on a delete's path, named by its parent's cell with
/// `s_txn`, read for a merge's fit test with the checks a descent makes of every page: a private
/// page under an untrusted parent is [`Error::Corrupt`] (as in [`descend_write`]); its header
/// names `s` and the cell's txn; it is a leaf exactly at the leaf level (`leaf_level`); it is not
/// empty.
fn sibling_view<S: PageWrite + ?Sized>(
    store: &S,
    s: u32,
    s_txn: u32,
    parent_trusted: bool,
    leaf_level: bool,
) -> Result<(&[u8], page::View<'_>)> {
    let private = store.is_private(s);
    if private && !parent_trusted {
        return Err(Error::Corrupt(format!(
            "page {s} is private to this transaction but named by a page it did not write"
        )));
    }
    let page = if private { store.private_page(s)? } else { store.page(s)? };
    let txn = page::txn(page);
    if page::pgno(page) != s || txn as u32 != s_txn {
        return Err(Error::Corrupt(format!(
            "page {s} (txn {txn}) is not the copy its parent names (txn {s_txn}): a redirected \
             or stale pointer"
        )));
    }
    let view = page::View::new(page)?;
    let (is_leaf, n) = match view {
        page::View::Leaf(v) => (true, v.nkeys()),
        page::View::Branch(v) => (false, v.nkeys()),
    };
    if is_leaf != leaf_level {
        return Err(Error::Corrupt(format!(
            "page {s} is a {:?} beside a {} on a delete's path",
            page::kind(page)?,
            if leaf_level { "leaf" } else { "branch" }
        )));
    }
    if n == 0 {
        return Err(Error::Corrupt(format!("non-root page {s} is empty")));
    }
    Ok((page, view))
}

/// The checks a descent makes of an untrusted page, for the sibling `s` (`page`, `view`) at child
/// `si` of the parent at path step `level`, which a merge absorbs: [`verify_published`] and
/// [`check_cells_naming_this_txn`] unless this writer stamped it, and its keys within the
/// intersection of its ancestors' separators. A private sibling, under a trusted parent
/// ([`sibling_view`]), is trusted, as in [`descend_write`].
///
/// A branch sibling's keys bound its children but say nothing of a single child's subtree, and
/// [`sibling_view`]'s kind check tells a leaf from a branch, not one level from another. So the
/// branch's child-0 chain is descended to the leaf level ([`check_sibling_chain`]) with a
/// descent's checks, which a pointer redirected to a single-child branch from elsewhere, or to a
/// branch of another level, fails. Two cells naming one sibling cannot both pass: their ranges
/// are disjoint, and the chain ends at a non-empty leaf whose keys must lie in each.
fn check_sibling<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    path: &[(u32, usize)],
    level: usize,
    si: usize,
    s: u32,
    page: &'s [u8],
    view: &page::View<'s>,
) -> Result<()> {
    if store.is_private(s) {
        return Ok(());
    }
    if !store.is_stamped(s) {
        verify_published(store, page, &store.geometry())?;
        check_cells_naming_this_txn(store, s, view)?;
    }
    let (mut lo, mut hi) = (None, None);
    let steps = path[..level].iter().copied().chain([(path[level].0, si)]);
    for (p, ci) in steps {
        tighten(&page::BranchView::new(path_page(store, p)?)?.route_at(ci)?, &mut lo, &mut hi);
    }
    if !view.keys_in_range(lo, hi)? {
        return Err(Error::Corrupt(format!("page {s} holds keys outside its parents' separators")));
    }
    if let page::View::Branch(branch) = view {
        // The sibling is at depth `level + 2` of a tree whose leaves are at `path.len() + 1`.
        check_sibling_chain(store, s, branch, (lo, hi), level + 2, path.len() + 1)?;
    }
    Ok(())
}

/// The child-0 chain below the published branch sibling `s` (`branch`, at `depth` of a tree of
/// `height`, its keys within `bounds`), down to the leaf level, each page with the checks
/// [`descend_write`] makes of an untrusted page: not private (a published page names none), the
/// copy its parent names, a branch above the leaf level and a leaf at it, not empty, verified
/// unless this writer stamped it, and its keys within the bounds tightened by each step. A merge
/// is rare, so the few page reads cost little.
fn check_sibling_chain<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    s: u32,
    branch: &page::BranchView<'s>,
    bounds: Bounds<'s>,
    mut depth: usize,
    height: usize,
) -> Result<()> {
    let geometry = store.geometry();
    let (mut lo, mut hi) = bounds;
    let mut route = branch.route_at(0)?;
    while depth < height {
        depth += 1;
        tighten(&route, &mut lo, &mut hi);
        let (pgno, want_txn) = (route.child, route.child_txn);
        if store.is_private(pgno) {
            return Err(Error::Corrupt(format!(
                "page {pgno} is private to this transaction but named by a page it did not write"
            )));
        }
        let page = store.page(pgno)?;
        let txn = page::txn(page);
        if page::pgno(page) != pgno || txn as u32 != want_txn {
            return Err(Error::Corrupt(format!(
                "page {pgno} (txn {txn}) is not the copy its parent names (txn {want_txn}): a \
                 redirected or stale pointer"
            )));
        }
        let view = page::View::new(page)?;
        let (is_leaf, n) = match view {
            page::View::Leaf(v) => (true, v.nkeys()),
            page::View::Branch(v) => (false, v.nkeys()),
        };
        if is_leaf != (depth == height) {
            return Err(Error::Corrupt(format!(
                "page {pgno} below merge sibling {s} is a {:?} at depth {depth} of a tree of \
                 height {height}",
                page::kind(page)?
            )));
        }
        if n == 0 {
            return Err(Error::Corrupt(format!("non-root page {pgno} is empty")));
        }
        if !store.is_stamped(pgno) {
            verify_published(store, page, &geometry)?;
        }
        if !view.keys_in_range(lo, hi)? {
            return Err(Error::Corrupt(format!(
                "page {pgno} holds keys outside its parents' separators"
            )));
        }
        match view {
            page::View::Leaf(_) => return Ok(()),
            page::View::Branch(b) => route = b.route_at(0)?,
        }
    }
    Err(Error::Corrupt(format!("merge sibling {s} is a branch at the leaves' depth {depth}")))
}

/// The handle a delete of shape `shape` that drops `dropped` pages and a run of `run_pages`
/// pages (0 for an inline value) leaves behind `root` (whose entry count [`plan_delete`]
/// checked): one entry fewer, the run uncounted and, when the leaf empties, the dropped pages
/// uncounted. Those are the emptied leaf and branches only: an unlink drops the single-child
/// branches between the keeper and the leaf, a collapse the root, the branches on the path and
/// the single-child branches [`collapse_to`] passed. Each sibling a merge absorbs is one leaf or
/// branch fewer, and a root that gives way to its single child after merges ([`Merged`]) one
/// branch and one level fewer. A handle whose counters do not allow it, or that would leave an
/// impossible shape, is [`Error::Corrupt`], refused before anything changes.
fn handle_after(
    root: &TreeRoot,
    shape: DeleteShape,
    dropped: usize,
    run_pages: u32,
    merged: Merged,
) -> Result<TreeRoot> {
    let entries = root.entries - 1;
    let overflow_pages = root.overflow_pages.checked_sub(run_pages);
    let unlinked = |root: &TreeRoot| -> Option<TreeRoot> {
        let branches = u32::try_from(dropped.checked_sub(1)?).ok()?;
        Some(TreeRoot {
            entries,
            overflow_pages: overflow_pages?,
            branch_pages: root.branch_pages.checked_sub(branches)?,
            leaf_pages: root.leaf_pages.checked_sub(1)?,
            ..*root
        })
    };
    let after = match shape {
        DeleteShape::Leaf => {
            overflow_pages.map(|o| TreeRoot { entries, overflow_pages: o, ..*root })
        }
        DeleteShape::Unlink(_) => unlinked(root),
        DeleteShape::Collapse { root: new_root, root_txn, height } => {
            unlinked(&TreeRoot { root: new_root, root_txn, height, ..*root })
        }
        DeleteShape::Empty => (overflow_pages == Some(0)).then_some(TreeRoot::EMPTY),
    };
    let after = after.and_then(|after| {
        let root_gone = u32::from(merged.root);
        Some(TreeRoot {
            height: after.height.checked_sub(root_gone)?,
            leaf_pages: after.leaf_pages.checked_sub(merged.leaves)?,
            branch_pages: after
                .branch_pages
                .checked_sub(merged.branches.checked_add(root_gone)?)?,
            ..after
        })
    });
    let Some(after) = after else {
        return Err(Error::Corrupt(format!(
            "tree root {root:?} does not count the {dropped} pages and the run of {run_pages} a \
             delete drops ({shape:?}, {merged:?})"
        )));
    };
    after.check()?;
    Ok(after)
}

/// Where a collapse ends: from the root's remaining child `(pgno, txn)` at `height`, follow each
/// branch with a single child (pushed on `passed`: they leave the tree too) down to the first page
/// that is a leaf or has two children or more, the new root. Each page whose pointer the collapse
/// follows, and the one it ends at, gets a descent's checks (the page its parent names, a branch
/// above the leaf level and a leaf at it, a non-empty leaf) and, if published and not stamped here,
/// is verified ([`verify_published`]): a pointer damaged at rest must not become the tree's root.
/// A private page below an untrusted one (`parent_trusted` is the root's trust) is
/// [`Error::Corrupt`], as in [`descend_write`].
fn collapse_to<S: PageWrite + ?Sized>(
    store: &S,
    geometry: &Geometry,
    mut pgno: u32,
    mut txn: u32,
    mut height: u32,
    mut parent_trusted: bool,
    passed: &mut Vec<u32>,
) -> Result<DeleteShape> {
    loop {
        let private = store.is_private(pgno);
        if private && !parent_trusted {
            return Err(Error::Corrupt(format!(
                "page {pgno} is private to this transaction but named by a page it did not write"
            )));
        }
        parent_trusted = private;
        let page = store.page(pgno)?;
        let header_txn = page::txn(page);
        if page::pgno(page) != pgno || header_txn as u32 != txn {
            return Err(Error::Corrupt(format!(
                "page {pgno} (txn {header_txn}) is not the copy its parent names (txn {txn}): a \
                 redirected or stale pointer"
            )));
        }
        let kind = page::kind(page)?;
        if (kind == Kind::Leaf) != (height == 1) {
            return Err(Error::Corrupt(format!(
                "page {pgno} is a {kind:?} {height} levels above the leaves' level"
            )));
        }
        if !store.is_private(pgno) && !store.is_stamped(pgno) {
            verify_published(store, page, geometry)?;
        }
        if kind == Kind::Leaf && page::nkeys(page) == 0 {
            return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
        }
        if kind == Kind::Leaf || page::nkeys(page) > 1 {
            return Ok(DeleteShape::Collapse { root: pgno, root_txn: txn, height });
        }
        passed.push(pgno);
        (pgno, txn) = page::branch_child(page, 0)?;
        height -= 1;
    }
}

/// Apply a planned delete: make the pages that stay on the path writable (with pages from
/// `scratch.reserved`; a root the merges leave with one child is not among them), take the entry
/// or the emptied child out, apply the merges ([`apply_merges`]), free the dropped pages (the
/// absorbed siblings among them) and update the handle to the plan's (`DeletePlan::after`).
/// Returns the page the change left, writable now (the leaf without the entry, or the branch
/// without the child), with `path` leading to it; `None` for a collapse or an empty tree.
fn apply_delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut Path,
    plan: &DeletePlan,
    scratch: &mut Scratch,
) -> Result<Option<u32>> {
    let top = usize::from(plan.merged_root);
    let changed = match plan.shape {
        DeleteShape::Leaf => {
            let leaf = make_path_writable(store, root, &mut path[top..], plan.leaf, scratch)?;
            page::leaf_delete(store.page_mut(leaf)?, plan.at)?;
            Some(leaf)
        }
        DeleteShape::Unlink(d) => {
            let (keeper, ci) = path[d];
            path.truncate(d);
            let keeper = make_path_writable(store, root, &mut path[top..], keeper, scratch)?;
            page::branch_delete(store.page_mut(keeper)?, ci)?;
            Some(keeper)
        }
        DeleteShape::Collapse { .. } | DeleteShape::Empty => None,
    };
    if let Some(changed) = changed.filter(|_| !plan.merges.is_empty()) {
        apply_merges(store, path, changed, plan, &mut scratch.split)?;
    }
    for &p in &plan.dropped {
        store.free(p);
    }
    if let Some((p, n)) = plan.run {
        store.free_run(p, n);
    }
    *root = match (plan.shape, changed) {
        (DeleteShape::Leaf | DeleteShape::Unlink(_), Some(changed)) if plan.merged_root => {
            // The page on the path below the old root, writable and absorbing its siblings.
            let survivor = path.get(1).map_or(changed, |&(p, _)| p);
            TreeRoot { root: survivor, root_txn: store.txn() as u32, ..plan.after }
        }
        (DeleteShape::Leaf | DeleteShape::Unlink(_), _) => {
            TreeRoot { root: root.root, root_txn: root.root_txn, ..plan.after }
        }
        (DeleteShape::Collapse { .. } | DeleteShape::Empty, _) => plan.after,
    };
    Ok(changed)
}

/// Apply a planned delete's merges ([`DeletePlan::merges`]) bottom-up along the writable `path`,
/// which ends above `changed`, the page the change left. For each absorption the sibling's cells
/// and the page's own go into the page, in key order, laid out over it in place
/// ([`page::SplitBuf::merge`]); a branch pair takes the parent's key for its right page as that
/// page's cell-0 key. The parent then points its left cell of the pair at the page (when the
/// sibling was on the left) and drops the right cell. The sibling is not changed (it is in
/// `DeletePlan::dropped`). A root the merges leave with one child is read, never changed: it
/// leaves the tree.
fn apply_merges<S: PageWrite + ?Sized>(
    store: &mut S,
    path: &[(u32, usize)],
    changed: u32,
    plan: &DeletePlan,
    split: &mut page::SplitBuf,
) -> Result<()> {
    let txn32 = store.txn() as u32;
    let mut merges = plan.merges.iter().peekable();
    while let Some(&&Absorb { level, .. }) = merges.peek() {
        let x = path.get(level + 1).map_or(changed, |&(p, _)| p);
        let (p, ci) = path[level];
        let writable = !(plan.merged_root && level == 0);
        let (mut lefts, mut rights) = (0, 0);
        while let Some(m) = merges.next_if(|m| m.level == level) {
            // The sibling's cell and the right page's cell (its key comes down) in the parent,
            // which drops the absorbed cells as it goes unless it is a root that leaves the tree.
            let (si, sep_i) = match m.side {
                Side::Left => (ci - lefts - 1, ci - lefts),
                Side::Right if writable => (ci - lefts + 1, ci - lefts + 1),
                Side::Right => (ci + rights + 1, ci + rights + 1),
            };
            let parent = store.page(p)?;
            if page::branch_child(parent, si)?.0 != m.sibling {
                return Err(bug(format!("merge sibling {} is not child {si} of {p}", m.sibling)));
            }
            let sep = if m.leaf { &[][..] } else { page::branch_key(parent, sep_i)? };
            let (l, r) = match m.side {
                Side::Left => (m.sibling, x),
                Side::Right => (x, m.sibling),
            };
            split.merge(store.page(l)?, store.page(r)?, sep)?;
            split.write_merged(store.page_mut(x)?)?;
            if writable {
                let parent = store.page_mut(p)?;
                if m.side == Side::Left {
                    page::branch_set_child(parent, si, x, txn32)?;
                }
                page::branch_delete(parent, sep_i)?;
            }
            match m.side {
                Side::Left => lefts += 1,
                Side::Right => rights += 1,
            }
        }
    }
    Ok(())
}

/// Every page of the tree `root` names, root first, level by level, collected without changing
/// anything (so a caller freeing them refuses damage before it frees a page). Each branch is read
/// with a descent's checks (the page its parent names, a branch above the leaf level) and, if
/// published and not stamped here, verified ([`verify_published`]) before its child pointers are
/// followed: they decide which pages leave the tree, so a pointer damaged at rest must not free a
/// live page. Every child must be a tree page of the file reached once.
///
/// **Provenance**, as in [`descend_write`]: a page private to this transaction named by a page it
/// did not write, or by an untrusted root handle (`root_trusted` is the handle's provenance), is
/// [`Error::Corrupt`], and a private child must be the copy its parent names (the identity check).
/// Freed, a private page goes back to the pool at once, so without these rules a forged pointer
/// naming a page this transaction allocated for another tree (a tree page, or a page of one of
/// its runs) would hand that page out again while the other tree holds it.
///
/// A tree counting no overflow page has its leaves taken from their parents' pointers: only the
/// first is read (it must be a leaf: the tree is balanced, so that checks the height), and the
/// private ones' headers (in memory) for the identity check. A tree with runs has
/// every leaf read, with a descent's checks and verified unless private or stamped here, and each
/// run its overflow cells name taken after its header and identity checks and its provenance
/// ([`leaving_run`]); their pages, each reached once, must add up to the handle's counter
/// ([`TreePages`]). Damage is [`Error::Corrupt`].
pub(crate) fn tree_pages<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    root_trusted: bool,
) -> Result<TreePages> {
    root.check()?;
    if root.is_empty() {
        return Ok(TreePages::default());
    }
    let geometry = store.geometry();
    let page_count = store.page_count();
    let corrupt = |what: String| Error::Corrupt(format!("tree walk: {what}"));
    let check_header = |pgno: u32, txn: u32| -> Result<&[u8]> {
        let page = store.page(pgno)?;
        if page::pgno(page) != pgno || page::txn(page) as u32 != txn {
            return Err(corrupt(format!(
                "page {pgno} is not the copy its parent names (txn {txn})"
            )));
        }
        Ok(page)
    };
    // Range-check the root as the children are, before it sizes the dense set: a damaged root
    // number must not allocate a bitset of up to 2^32 bits before it is refused.
    if root.root < 2 || root.root >= page_count {
        return Err(corrupt(format!(
            "root {} is outside the file's {page_count} pages",
            root.root
        )));
    }
    let untrusted = |p: u32| {
        corrupt(format!(
            "page {p} is private to this transaction but named by a page it did not write"
        ))
    };
    if store.is_private(root.root) && !root_trusted {
        return Err(untrusted(root.root));
    }
    let mut seen = PageSet::default();
    seen.insert(root.root);
    let mut pages = vec![root.root];
    let mut level = vec![(root.root, root.root_txn)];
    for _ in 1..root.height {
        let mut next = Vec::new();
        for &(p, txn) in &level {
            let page = check_header(p, txn)?;
            if page::kind(page)? != Kind::Branch {
                return Err(corrupt(format!("page {p} is a leaf above the leaves' level")));
            }
            let private = store.is_private(p);
            if !private && !store.is_stamped(p) {
                verify_published(store, page, &geometry)?;
            }
            for i in 0..page::nkeys(page) {
                let (child, child_txn) = page::branch_child(page, i)?;
                if child < 2 || child >= page_count || seen.contains(child) {
                    return Err(corrupt(format!("page {p} has a bad child {child}")));
                }
                if store.is_private(child) {
                    if !private {
                        return Err(untrusted(child));
                    }
                    check_header(child, child_txn)?;
                }
                seen.insert(child);
                next.push((child, child_txn));
            }
        }
        pages.extend(next.iter().map(|&(p, _)| p));
        level = next;
    }
    let Some(&(first, txn)) = level.first() else {
        return Err(corrupt(format!("a level of the tree of height {} is empty", root.height)));
    };
    if root.overflow_pages == 0 {
        if page::kind(check_header(first, txn)?)? != Kind::Leaf {
            return Err(corrupt(format!("height {} does not reach the leaves", root.height)));
        }
        return Ok(TreePages { pages, runs: Vec::new() });
    }
    let mut runs = Vec::new();
    let mut run_pages = 0_u64;
    for &(p, txn) in &level {
        let page = check_header(p, txn)?;
        let page::View::Leaf(view) = page::View::new(page)? else {
            return Err(corrupt(format!("height {} does not reach the leaves", root.height)));
        };
        if !store.is_private(p) && !store.is_stamped(p) {
            verify_published(store, page, &geometry)?;
        }
        for i in 0..view.nkeys() {
            let ovf = match view.entry(i)?.1 {
                Val::Inline(_) => continue,
                Val::Overflow(ovf) => ovf,
                Val::Nested(root) => return Err(page::nested_error(root)),
            };
            let n = leaving_run(store, p, ovf)?;
            for q in ovf.pgno..ovf.pgno + n {
                if seen.contains(q) {
                    return Err(corrupt(format!(
                        "page {q} of overflow run {} is reached twice",
                        ovf.pgno
                    )));
                }
                seen.insert(q);
            }
            runs.push((ovf.pgno, n));
            run_pages += u64::from(n);
        }
    }
    if run_pages != u64::from(root.overflow_pages) {
        return Err(corrupt(format!(
            "the leaves name {run_pages} overflow pages, the handle counts {}",
            root.overflow_pages
        )));
    }
    Ok(TreePages { pages, runs })
}

/// What [`tree_pages`] collected.
#[derive(Debug, Default)]
pub(crate) struct TreePages {
    /// The tree's pages, root first, level by level.
    pub(crate) pages: Vec<u32>,
    /// Its overflow runs as `(first page, pages)`.
    pub(crate) runs: Vec<(u32, u32)>,
}

/// Free every page of the tree `root` names and every overflow run: all collected first
/// ([`tree_pages`], `root_trusted` the handle's provenance), so damage is refused before anything
/// is freed. A published page is freed
/// once no reader or durable meta can reach it, a private one is reusable at once; a run leaves
/// as single pages. The handle is the caller's to drop or reset.
pub(crate) fn free_subtree<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &TreeRoot,
    root_trusted: bool,
) -> Result<()> {
    let TreePages { pages, runs } = tree_pages(&*store, root, root_trusted)?;
    for p in pages {
        store.free(p);
    }
    for (p, n) in runs {
        store.free_run(p, n);
    }
    Ok(())
}

/// Empty the tree `root` names: free every page ([`free_subtree`], `root_trusted` the handle's
/// provenance) and reset `root` to [`TreeRoot::EMPTY`]. Published snapshots keep reading the old
/// tree. On damage nothing is freed and `root` is unchanged.
pub(crate) fn clear<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    root_trusted: bool,
) -> Result<()> {
    free_subtree(store, root, root_trusted)?;
    *root = TreeRoot::EMPTY;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use super::*;
    use crate::tree::{
        read::{descend, find, get},
        test_harness::{
            leaf_fill, level_keys, merge_open_pages, page_fill, walk_tree, MemStore, ReportSeed,
            Rng,
        },
    };
    use plain::{clear, delete, put};

    /// The deletes [`delete_cached`] did in a cached leaf on this thread so far
    /// ([`CACHED_DELETES`]); 0 in a release build, whose callers then check nothing.
    fn cached_deletes() -> u64 {
        #[cfg(debug_assertions)]
        return CACHED_DELETES.with(std::cell::Cell::get);
        #[cfg(not(debug_assertions))]
        0
    }

    /// [`put`], through a table's path cache when given ([`put_cached`]).
    fn put_through(
        store: &mut MemStore,
        root: &mut TreeRoot,
        cache: Option<&mut PathCache>,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        let Some(cache) = cache else { return put(store, root, key, value) };
        store.geometry().place(key.len(), value.len())?;
        let mut trusted = !root.is_empty() && store.is_private(root.root);
        Ok(put_cached(store, root, &mut trusted, cache, key, value, PutKind::Upsert)?.replaced)
    }

    /// [`delete`], through a table's path cache when given ([`delete_cached`]).
    fn delete_through(
        store: &mut MemStore,
        root: &mut TreeRoot,
        cache: Option<&mut PathCache>,
        key: &[u8],
    ) -> Result<bool> {
        let Some(cache) = cache else { return delete(store, root, key) };
        let mut trusted = !root.is_empty() && store.is_private(root.root);
        Ok(delete_cached(store, root, &mut trusted, cache, key)?.found)
    }

    /// A random entry: a fresh key (mostly short, sometimes up to the key limit) or, a third of the
    /// time, one of `keys` again; a value mostly short, sometimes up to the limit for its key.
    fn random_entry(rng: &mut Rng, g: &Geometry, keys: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
        let key = if !keys.is_empty() && rng.below(3) == 0 {
            keys[rng.below(keys.len() as u64) as usize].clone()
        } else {
            let klen = match rng.below(10) {
                0 => 1 + rng.len_upto(g.max_key() - 1),
                1 | 2 => 1 + rng.len_upto(g.max_key() / 8),
                _ => 1 + rng.len_upto(24),
            };
            (0..klen).map(|_| rng.next() as u8).collect()
        };
        let max = g.max_value(key.len());
        let vlen = match rng.below(10) {
            0 => max - rng.len_upto(max.min(16)),
            1 | 2 => rng.len_upto(max / 4),
            _ => rng.len_upto(max.min(64)),
        };
        (key, (0..vlen).map(|_| rng.next() as u8).collect())
    }

    /// Random puts (inserts and replaces, sizes up to the limits) checked against a `BTreeMap`
    /// after every op: the walker passes and the entries and lookups match. Commits, reuse of
    /// freed pages and reopens (published pages verified before their first copy) come at random.
    /// Returns the final height.
    fn random_puts_match_a_model(page_size: u32, seed: u64, ops: usize) -> u32 {
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let mut store = MemStore::new(page_size);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        let mut keys = Vec::new();
        for op in 0..ops {
            let (k, v) = random_entry(&mut rng, &g, &keys);
            let replaced = put(&mut store, &mut root, &k, &v).expect("put");
            assert_eq!(replaced, model.insert(k.clone(), v.clone()).is_some(), "op {op}");
            if !replaced {
                keys.push(k.clone());
            }
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "op {op}: entries");
            assert_eq!(get(&store, &root, &k).expect("get"), Some(v.as_slice()), "op {op}");
            if rng.below(25) == 0 {
                if rng.below(4) == 0 {
                    store.reopen();
                } else {
                    store.commit_like();
                }
                if rng.below(3) == 0 {
                    store.recycle();
                }
                store.assert_accounting(&[root]);
                // A reader of the commit sees every entry, through published pages only.
                let reader = store.reader();
                walk_tree(&reader, &root);
                for (k, v) in &model {
                    assert_eq!(get(&reader, &root, k).expect("get"), Some(v.as_slice()));
                }
                for _ in 0..20 {
                    let (k, _) = random_entry(&mut rng, &g, &[]);
                    let got = get(&reader, &root, &k).expect("get");
                    assert_eq!(got, model.get(&k).map(Vec::as_slice), "op {op}: absent key");
                }
            }
        }
        root.height
    }

    #[test]
    fn random_puts_match_a_btreemap_model() {
        assert!(
            random_puts_match_a_model(4096, 0x2545_F491_4F6C_DD1D, 1_000) >= 3,
            "deep at 4 KiB"
        );
        random_puts_match_a_model(4096, 0x9E37_79B9_7F4A_7C15, 300);
        assert!(random_puts_match_a_model(8192, 0x1234_5678_9ABC_DEF1, 300) >= 2);
        assert!(random_puts_match_a_model(65536, 0x0F0F_0F0F_1234_5679, 150) >= 2, "64 KiB splits");
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn random_puts_match_a_btreemap_model_heavy() {
        for seed in 1..=8_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for page_size in [4096, 8192, 16384, 32768, 65536] {
                random_puts_match_a_model(page_size, seed, 3_000);
            }
        }
    }

    /// Insert and grow-replace wide entries (a few per page, so splits cascade several levels),
    /// failing each allocation of each put in turn: a failed put leaves the root handle, every
    /// reachable page (byte for byte) and the freed list exactly as they were, and every page it
    /// took is back in the pool. One put in five stores its value in an overflow run of 1 to 4
    /// pages, allocated with the tree pages, so its failures leave the tree identical too.
    #[test]
    fn a_failed_allocation_leaves_the_tree_identical() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut root = TreeRoot::EMPTY;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut failures = 0;
        for n in 0..150_u64 {
            let key = match model.keys().nth(rng.below(model.len() as u64 + 1) as usize) {
                Some(k) if n % 4 == 3 => k.clone(),
                _ => {
                    let id = if n % 3 == 0 { n } else { rng.below(100_000) };
                    let mut k = vec![0xA5; g.max_key() - rng.len_upto(g.max_key() / 2)];
                    k[..8].copy_from_slice(&id.to_be_bytes());
                    k
                }
            };
            let mut value = vec![n as u8; rng.len_upto(g.max_value(key.len()))];
            if n % 5 == 4 && g.max_table_value(key.len()) > g.max_value(key.len()) {
                value.resize(g.max_value(key.len()) + 1 + (n as usize * 7919) % (3 * 4096), 1);
            }
            for fail_at in 0.. {
                let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
                let (pool, count) = (store.alloc.pool().len(), store.alloc.page_count());
                store.alloc.fail_allocs_after = Some(fail_at);
                let result = put(&mut store, &mut root, &key, &value);
                store.alloc.fail_allocs_after = None;
                match result {
                    Ok(_) => break,
                    Err(Error::Io(_)) => failures += 1,
                    Err(e) => panic!("put {n}, failing allocation {fail_at}: {e}"),
                }
                let case = format!("put {n}, failing allocation {fail_at}");
                assert_eq!(root, before.0, "{case}: the root handle changed");
                assert!(store.reachable(&root) == before.1, "{case}: a reachable page changed");
                assert_eq!(store.alloc.freed(), before.2, "{case}: pages were freed");
                let grown = (store.alloc.page_count() - count) as usize;
                assert_eq!(store.alloc.pool().len(), pool + grown, "{case}: pages taken were kept");
            }
            model.insert(key, value);
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "put {n}");
            if n % 10 == 9 {
                store.commit_like();
                store.assert_accounting(&[root]);
            }
        }
        assert!(root.height >= 4, "the test reaches deep cascades (height {})", root.height);
        assert!(failures > 150, "{failures} failed allocations");
    }

    /// Ascending keys split in append mode, filling each leaf before starting the next; random
    /// keys leave leaves part full.
    #[test]
    fn ascending_inserts_fill_leaves() {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..20_000_u64 {
            put(&mut store, &mut root, &i.to_be_bytes(), &i.to_le_bytes()).expect("put");
        }
        let walk = walk_tree(&store, &root);
        let fill = leaf_fill(&store, &walk.leaves);
        assert!(fill > 0.95, "ascending fill {fill}");

        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for _ in 0..20_000 {
            let x = rng.next();
            put(&mut store, &mut root, &x.to_be_bytes(), &x.to_le_bytes()).expect("put");
        }
        let walk = walk_tree(&store, &root);
        let fill = leaf_fill(&store, &walk.leaves);
        assert!((0.55..0.9).contains(&fill), "random fill {fill}");
    }

    /// An ascending run inside the key space (not at its end) splits at the byte midpoint: append
    /// mode is only for the rightmost path, where nothing is inserted behind the run.
    #[test]
    fn only_the_rightmost_run_appends() {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..10_000_u64 {
            for run in [b'a', b'b'] {
                let mut k = vec![run];
                k.extend_from_slice(&i.to_be_bytes());
                put(&mut store, &mut root, &k, &i.to_le_bytes()).expect("put");
            }
        }
        let walk = walk_tree(&store, &root);
        let run_fill = |run: u8| {
            let leaves: Vec<u32> = walk
                .leaves
                .iter()
                .copied()
                .filter(|&p| page::leaf_key(store.raw(p), 0).expect("key")[0] == run)
                .collect();
            leaf_fill(&store, &leaves)
        };
        let (inner, rightmost) = (run_fill(b'a'), run_fill(b'b'));
        assert!((0.4..0.75).contains(&inner), "inner run fill {inner}");
        assert!(rightmost > 0.95, "rightmost run fill {rightmost}");
    }

    /// A leaf of `entries` (in order), private to the store's transaction.
    fn leaf_page(store: &mut MemStore, entries: &[&[u8]]) -> u32 {
        let (p, txn) = (store.alloc().expect("alloc"), store.txn());
        let page = store.page_mut(p).expect("page");
        page::init_leaf(page, p, txn);
        for (i, k) in entries.iter().enumerate() {
            assert_eq!(page::leaf_insert(page, i, k, b"").expect("insert"), Fit::Done);
        }
        p
    }

    /// A branch over `children` (`(lower-bound key, child)`, the first key empty).
    fn branch_page(store: &mut MemStore, children: &[(Vec<u8>, u32)]) -> u32 {
        let (p, txn) = (store.alloc().expect("alloc"), store.txn());
        let page = store.page_mut(p).expect("page");
        page::init_branch(page, p, txn);
        for (i, (k, child)) in children.iter().enumerate() {
            let fit = page::branch_insert(page, i, k, *child, txn as u32).expect("insert");
            assert_eq!(fit, Fit::Done, "branch cell {i}");
        }
        p
    }

    /// Fullness is by bytes, so whether a split climbs depends on the separator's length. Here a
    /// full leaf of short keys splits; its parent, packed with short separators (and one long key
    /// at its byte midpoint), cannot take even a short one and splits, sending the long key up;
    /// the root has room for a short separator but not that long one, so it splits too. The
    /// up-front reservation must bound each level's separator by the keys of the page below,
    /// not by the leaf's keys alone.
    #[test]
    fn a_long_separator_climbing_a_split_is_reserved_for() {
        let mut store = MemStore::new(4096);
        let long_key = |first: u8, second: u8, len: usize| {
            let mut k = vec![first, second];
            k.resize(len, 0xFF);
            k
        };
        // The parent: 217 two-byte separators and one 1000-byte key after the 145th, over
        // one-entry leaves; its last child is the full leaf (keys [1, 217, j]).
        let full = {
            let (p, txn) = (store.alloc().expect("alloc"), store.txn());
            let page = store.page_mut(p).expect("page");
            page::init_leaf(page, p, txn);
            let mut j = 0_u8;
            for vlen in [100, 0] {
                while page::leaf_insert(page, page::nkeys(page), &[1, 217, j], &vec![0; vlen])
                    .expect("insert")
                    == Fit::Done
                {
                    j += 1;
                }
            }
            p
        };
        let mut children = vec![(vec![], leaf_page(&mut store, &[&[1, 0]]))];
        for i in 1..=216_u8 {
            children.push((vec![1, i], leaf_page(&mut store, &[&[1, i]])));
            if i == 145 {
                let k = long_key(1, 145, 1000);
                children.push((k.clone(), leaf_page(&mut store, &[&k])));
            }
        }
        children.push((vec![1, 217], full));
        let parent = branch_page(&mut store, &children);
        assert!(page::free_space(store.raw(parent)).expect("free") < 14, "the parent is full");
        // The root: the parent, then three 1000-byte and one 101-byte separators.
        let mut root_children = vec![(vec![], parent)];
        for (first, len) in [(2, 1000), (3, 1000), (4, 1000), (5, 101)] {
            let k = long_key(first, 0, len);
            let leaf = leaf_page(&mut store, &[&k]);
            root_children.push((k, branch_page(&mut store, &[(vec![], leaf)])));
        }
        let top = branch_page(&mut store, &root_children);
        let free = page::free_space(store.raw(top)).expect("free");
        assert!((14..1012).contains(&free), "room for a short separator only ({free})");
        let entries = 218 + 4 + page::nkeys(store.raw(full)) as u64;
        let txn = store.txn() as u32;
        let mut root = store.counted(TreeRoot {
            root: top,
            root_txn: txn,
            height: 3,
            entries,
            ..TreeRoot::EMPTY
        });
        walk_tree(&store, &root);
        store.commit_like();

        let key = [1, 217, 0xFF, 0xFF];
        put(&mut store, &mut root, &key, b"v").expect("the reservation covers the cascade");
        assert_eq!(root.height, 4, "the root split");
        walk_tree(&store, &root);
        assert_eq!(get(&store, &root, &key).expect("get"), Some(&b"v"[..]));
    }

    /// A published tree of `n` entries (8-byte keys, 100-byte values) as a reopened writer sees it:
    /// nothing stamped by this writer.
    fn reopened_tree(n: u64) -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..n {
            put(&mut store, &mut root, &(i * 7919 % n).to_be_bytes(), &[i as u8; 100])
                .expect("put");
        }
        store.reopen();
        (store, root)
    }

    /// Copy-on-write refuses a published page that fails its CRC (a flipped value bit) or, with a
    /// valid CRC, fails `validate` (a reserved header byte set): the copy would be stamped valid
    /// at the next commit. Nothing changes.
    #[test]
    fn copy_on_write_refuses_a_damaged_page() {
        for structural in [false, true] {
            let (mut store, mut root) = reopened_tree(2_000);
            let leaf = walk_tree(&store, &root).leaves[0];
            let p = store.geometry().page_size();
            if structural {
                store.raw_mut(leaf)[20] = 1;
                store.restamp(leaf);
            } else {
                store.raw_mut(leaf)[p - 5] ^= 0x01;
            }
            let before = (root, store.reachable(&root));
            let put = put(&mut store, &mut root, &0_u64.to_be_bytes(), b"new");
            assert!(matches!(put, Err(Error::Corrupt(_))), "structural {structural}: {put:?}");
            assert_eq!((root, store.reachable(&root)), before, "structural {structural}");
            assert!(store.alloc.freed().is_empty() && store.alloc.pool().is_empty());
        }
    }

    /// Copy-on-write verifies each published page from disk once, before its first copy, and
    /// neither the private copies nor pages this writer stamped itself.
    #[test]
    #[cfg(debug_assertions)]
    fn copy_on_write_verifies_only_pages_it_did_not_stamp() {
        let (mut store, mut root) = reopened_tree(2_000);
        assert!(root.height >= 2, "a path to copy");
        let verifies = || COW_VERIFIES.with(std::cell::Cell::get);
        COW_VERIFIES.with(|n| n.set(0));
        put(&mut store, &mut root, &0_u64.to_be_bytes(), b"one").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "each page on the path, verified once");
        put(&mut store, &mut root, &1_u64.to_be_bytes(), b"two").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "private copies are not verified");
        store.commit_like();
        put(&mut store, &mut root, &0_u64.to_be_bytes(), b"three").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "pages it stamped are not re-verified");
    }

    /// A put along a path this transaction built makes no separator range check; the first put
    /// through published pages checks each of them.
    #[test]
    #[cfg(debug_assertions)]
    fn a_trusted_path_skips_the_range_checks() {
        let (mut store, mut root) = reopened_tree(2_000);
        assert!(root.height >= 2, "a path of several pages");
        let checks = || RANGE_CHECKS.with(std::cell::Cell::get);
        let mut trusted = false;
        RANGE_CHECKS.with(|n| n.set(0));
        super::put(&mut store, &mut root, &mut trusted, &0_u64.to_be_bytes(), b"a").expect("put");
        assert_eq!(checks(), u64::from(root.height), "each published page on the path, once");
        assert!(trusted, "a put leaves its root trusted");
        RANGE_CHECKS.with(|n| n.set(0));
        super::put(&mut store, &mut root, &mut trusted, &1_u64.to_be_bytes(), b"b").expect("put");
        assert_eq!(checks(), 0, "a fully trusted path makes no range check");
    }

    /// A private page named by a page this writer did not write is damage: a published branch
    /// this writer stamped (whose cells copy-on-write does not re-check), forged to name a page
    /// this transaction allocated for another tree, or an untrusted root handle naming that page.
    /// A put through the forged cell, a put from the handle and a delete that collapses the root
    /// onto the forged cell (`collapse_to`) each refuse it, and the other tree's page is
    /// unchanged.
    #[test]
    fn a_published_cell_naming_a_private_page_is_refused() {
        for case in ["put through the cell", "put from the root handle", "collapse onto the cell"] {
            let mut store = MemStore::new(4096);
            let left = leaf_page(&mut store, &[b"a"]);
            let right = leaf_page(&mut store, &[b"m", b"n"]);
            let r = branch_page(&mut store, &[(Vec::new(), left), (b"m".to_vec(), right)]);
            let mut root = TreeRoot {
                root: r,
                root_txn: store.txn() as u32,
                height: 2,
                entries: 3,
                branch_pages: 1,
                leaf_pages: 2,
                overflow_pages: 0,
            };
            store.commit_like();
            assert!(store.is_stamped(r), "the root is stamped by this writer: not re-verified");
            let (mut other, mut other_trusted) = (TreeRoot::EMPTY, false);
            super::put(&mut store, &mut other, &mut other_trusted, b"other", b"tree").expect("put");
            let x = other.root;
            assert!(store.is_private(x), "X is private to this transaction");
            let x_before = store.raw(x).to_vec();
            // Redirect the root's cell 1 to X with this txn's id, and restamp it CRC-valid.
            let txn32 = store.txn() as u32;
            page::branch_set_child(store.raw_mut(r), 1, x, txn32).expect("redirect");
            store.restamp(r);
            let mut trusted = false;
            let result = match case {
                "put through the cell" => {
                    super::put(&mut store, &mut root, &mut trusted, b"z", b"v").map(drop)
                }
                "put from the root handle" => {
                    let mut handle = other;
                    super::put(&mut store, &mut handle, &mut trusted, b"z", b"v").map(drop)
                }
                _ => super::delete(&mut store, &mut root, &mut trusted, b"a").map(drop),
            };
            let Err(Error::Corrupt(msg)) = &result else { panic!("{case}: {result:?}") };
            assert!(msg.contains("named by a page it did not write"), "{case}: {msg}");
            assert_eq!(store.raw(x), &x_before[..], "{case}: the other tree's page is unchanged");
        }
    }

    /// A published branch, restamped CRC-valid with cell 1 redirected to cell 2's real leaf
    /// (identity and all), becomes private through a put under another cell. A put through the
    /// redirected cell then meets a published leaf below a trusted parent, holding keys outside
    /// the cell's separators: the range check still runs there, against the trusted ancestors'
    /// separators, so the put is `Corrupt` and the leaf is unchanged.
    #[test]
    fn a_redirect_below_a_copied_parent_is_refused() {
        let (mut store, mut root) = reopened_tree(2_000);
        assert_eq!(root.height, 2, "a published root branch over the leaves");
        let r = root.root;
        assert!(page::nkeys(store.raw(r)) > 3, "cells 1 and 2 are not the last cell");
        let (sibling, sibling_txn) = page::branch_child(store.raw(r), 2).expect("child");
        let k1 = page::branch_key(store.raw(r), 1).expect("key").to_vec();
        page::branch_set_child(store.raw_mut(r), 1, sibling, sibling_txn).expect("redirect");
        store.restamp(r);
        let sibling_before = store.raw(sibling).to_vec();
        let mut trusted = false;
        let last = u64::MAX.to_be_bytes();
        super::put(&mut store, &mut root, &mut trusted, &last, b"a").expect("under the last cell");
        assert!(trusted && root.root != r && store.is_private(root.root), "a private root copy");
        let result = super::put(&mut store, &mut root, &mut trusted, &k1, b"b");
        assert!(matches!(result, Err(Error::Corrupt(_))), "{result:?}");
        assert_eq!(store.raw(sibling), &sibling_before[..], "the redirect's target is unchanged");
    }

    /// A forged cell in a published branch naming `(X, txn)`, where `X` is a free page this
    /// transaction allocates for another tree, is refused when a put copies the branch, whether
    /// `X` is allocated after the copy or before it. Otherwise the private copy keeps the cell,
    /// `X` is private for the other tree, and a put through the cell trusts `X` and writes into
    /// the other tree's page.
    #[test]
    fn a_copied_cell_naming_a_page_allocated_later_is_refused() {
        for x_first in [false, true] {
            let (mut store, mut root) = reopened_tree(2_000);
            assert_eq!(root.height, 2, "a published root branch over the leaves");
            let last = u64::MAX.to_be_bytes();
            // Free the root and two leaves (two puts copy them) and make them reusable. The pool
            // is a stack: its last page goes to the next allocation, its first after the two
            // pages a put copies into.
            put(&mut store, &mut root, &0_u64.to_be_bytes(), b"a").expect("put");
            put(&mut store, &mut root, &last, b"b").expect("put");
            store.reopen();
            store.recycle();
            let pool = store.alloc.pool();
            assert_eq!(pool.len(), 3);
            let x = if x_first { pool[2] } else { pool[0] };
            let x_free = store.raw(x).to_vec();
            // Forge the published root's cell 1 to name X with this txn's id, CRC-valid.
            assert!(page::nkeys(store.raw(root.root)) > 2, "cell 1 is not the last cell");
            let k1 = page::branch_key(store.raw(root.root), 1).expect("key").to_vec();
            let txn32 = store.txn() as u32;
            page::branch_set_child(store.raw_mut(root.root), 1, x, txn32).expect("forge");
            store.restamp(root.root);

            let (mut trusted, mut other, mut other_trusted) = (false, TreeRoot::EMPTY, false);
            let mut x_other = None;
            let mut steps = || -> Result<()> {
                for step in 0..2 {
                    if (step == 0) == x_first {
                        // Another tree's first put allocates X.
                        super::put(&mut store, &mut other, &mut other_trusted, b"other", b"t")?;
                        assert_eq!(other.root, x, "the other tree's root is X");
                        x_other = Some(store.raw(x).to_vec());
                    } else {
                        // Copy the forged branch: a put under its last cell.
                        super::put(&mut store, &mut root, &mut trusted, &last, b"c")?;
                    }
                }
                // A put through the forged cell.
                super::put(&mut store, &mut root, &mut trusted, &k1, b"d")?;
                Ok(())
            };
            let result = steps();
            assert!(matches!(result, Err(Error::Corrupt(_))), "x_first {x_first}: {result:?}");
            let x_expected = x_other.unwrap_or(x_free);
            assert_eq!(store.raw(x), &x_expected[..], "x_first {x_first}: X is unchanged");
        }
    }

    /// The copy rule accepts a live child whose txn id wrapped: a cell's `child_txn` is the low
    /// 32 bits of the child's header txn, so a child written 2^32 transactions ago carries this
    /// transaction's `child_txn`.
    #[test]
    fn a_child_whose_txn_wrapped_is_a_live_child() {
        let mut store = MemStore::new(4096);
        let left = leaf_page(&mut store, &[b"a", b"b"]);
        let right = leaf_page(&mut store, &[b"m", b"n"]);
        let r = branch_page(&mut store, &[(Vec::new(), left), (b"m".to_vec(), right)]);
        let txn = store.txn();
        let mut root = TreeRoot {
            root: r,
            root_txn: txn as u32,
            height: 2,
            entries: 4,
            branch_pages: 1,
            leaf_pages: 2,
            overflow_pages: 0,
        };
        store.commit_like();
        // The writer 2^32 transactions on, with nothing stamped by it: the pages are verified.
        store.alloc = crate::alloc::Allocator::new(txn + (1 << 32), store.alloc.page_count());
        assert_eq!(store.txn() as u32, txn as u32, "every cell names this txn's low 32 bits");
        let mut trusted = false;
        super::put(&mut store, &mut root, &mut trusted, b"c", b"new").expect("a live child");
        super::put(&mut store, &mut root, &mut trusted, b"o", b"new").expect("a live child");
        assert_eq!(get(&store, &root, b"c").expect("get"), Some(&b"new"[..]));
        assert_eq!(get(&store, &root, b"o").expect("get"), Some(&b"new"[..]));
    }

    /// A one-leaf tree of keys `a`, `b`, `c` whose cell `b` is forged, CRC-valid, to name `x`
    /// with the run counter to match: the leaf as the tree's handle sees it.
    fn forge_run_cell(store: &mut MemStore, root: &mut TreeRoot, x: Ovf) {
        let leaf = root.root;
        let fit = page::LeafMut::new(store.raw_mut(leaf))
            .expect("leaf")
            .replace_value(1, Val::Overflow(x))
            .expect("forge");
        assert_eq!(fit, Fit::Done);
        store.restamp(leaf);
        root.overflow_pages = overflow::run_pages(x.len as usize, 4096) as u32;
    }

    /// A leaf of keys `a`, `b`, `c` (one-byte values) as a tree.
    fn abc_tree(store: &mut MemStore) -> TreeRoot {
        let mut root = TreeRoot::EMPTY;
        for k in [b"a", b"b", b"c"] {
            put(store, &mut root, k, b"v").expect("put");
        }
        root
    }

    /// The run a tree's only overflow value lives in.
    fn run_of(store: &MemStore, root: &TreeRoot, key: &[u8]) -> Ovf {
        match find(store, root, key).expect("find") {
            Some(Val::Overflow(ovf)) => ovf,
            other => panic!("{key:?} is not in a run: {other:?}"),
        }
    }

    /// A private run named by a leaf this writer did not write is damage: a published leaf this
    /// writer stamped (whose cells copy-on-write does not re-check), forged to name the run this
    /// transaction wrote for another tree's value, with a handle that counts it. A replace and a
    /// delete through the forged cell each refuse it, and the other tree keeps its run.
    #[test]
    fn a_published_leaf_naming_a_private_run_is_refused() {
        for case in ["replace", "delete"] {
            let mut store = MemStore::new(4096);
            let mut root = abc_tree(&mut store);
            store.commit_like();
            assert!(store.is_stamped(root.root), "stamped by this writer: not re-verified");
            let (big, mut other) = (vec![9; 3 * 4096], TreeRoot::EMPTY);
            put(&mut store, &mut other, b"other", &big).expect("put");
            let x = run_of(&store, &other, b"other");
            assert!(store.alloc.is_private_run(x.pgno), "the run is private to this transaction");
            forge_run_cell(&mut store, &mut root, x);
            let mut trusted = false;
            let result = match case {
                "replace" => super::put_sized(&mut store, &mut root, &mut trusted, b"b", b"new"),
                _ => super::delete(&mut store, &mut root, &mut trusted, b"b"),
            };
            let Err(Error::Corrupt(msg)) = &result else { panic!("{case}: {result:?}") };
            assert!(msg.contains("which it did not write"), "{case}: {msg}");
            assert!(store.alloc.is_private_run(x.pgno), "{case}: the run was not freed");
            assert_eq!(get(&store, &other, b"other").expect("get"), Some(big.as_slice()));
        }
    }

    /// A forged overflow cell in a published leaf naming `(X, txn)`, where `X` is the run this
    /// transaction allocates for another tree's value, is refused when a put copies the leaf,
    /// whether `X` is allocated after the copy (out of the file then) or before it (this txn's
    /// run). Otherwise the private copy is trusted, and a replace through the cell frees the
    /// other tree's run.
    #[test]
    fn a_copied_leaf_naming_a_run_allocated_later_is_refused() {
        for x_first in [false, true] {
            let mut store = MemStore::new(4096);
            let mut root = abc_tree(&mut store);
            store.reopen();
            assert!(store.alloc.pool().is_empty(), "every page comes from the file's end");
            let big = vec![9; 3 * 4096];
            // The other tree's first put takes a leaf, then its run; the copy takes one page.
            let count = store.alloc.page_count();
            let pgno = if x_first { count + 1 } else { count + 2 };
            let x = Ovf { pgno, txn: store.txn() as u32, len: big.len() as u32 };
            forge_run_cell(&mut store, &mut root, x);

            let (mut trusted, mut other) = (false, TreeRoot::EMPTY);
            let mut steps = || -> Result<()> {
                for step in 0..2 {
                    if (step == 0) == x_first {
                        put(&mut store, &mut other, b"other", &big)?;
                        assert_eq!(run_of(&store, &other, b"other"), x, "the other run is X");
                    } else {
                        // Copy the forged leaf: a put of another of its keys.
                        super::put_sized(&mut store, &mut root, &mut trusted, b"a", b"w")?;
                    }
                }
                super::put_sized(&mut store, &mut root, &mut trusted, b"b", b"new")?;
                Ok(())
            };
            let result = steps();
            assert!(matches!(result, Err(Error::Corrupt(_))), "x_first {x_first}: {result:?}");
            if !other.is_empty() {
                assert!(store.alloc.is_private_run(pgno), "x_first {x_first}: X was freed");
                assert_eq!(get(&store, &other, b"other").expect("get"), Some(big.as_slice()));
            }
        }
    }

    /// A put through the path cache takes the cached leaf only for an inline value into an
    /// inline cell: an overflow-sized value the leaf has room for, and an inline value replacing
    /// an overflow one, descend, so the run is allocated or freed with the tree. The tree then
    /// holds no over-limit inline cell, counts its runs, and its pages add up.
    #[test]
    fn the_path_cache_leaves_overflow_puts_to_the_descent() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let (mut root, mut trusted, mut cache) = (TreeRoot::EMPTY, false, PathCache::default());
        let mut put_via_cache = |store: &mut MemStore, root: &mut TreeRoot, k: &[u8], v: &[u8]| {
            put_cached(store, root, &mut trusted, &mut cache, k, v, PutKind::Upsert).expect("put")
        };
        put_via_cache(&mut store, &mut root, b"k1", b"small");
        #[cfg(debug_assertions)]
        let cached = || CACHED_PUTS.with(std::cell::Cell::get);
        #[cfg(debug_assertions)]
        let before = cached();
        put_via_cache(&mut store, &mut root, b"k0", b"cached");
        let big = vec![7; g.max_value(2) + 1];
        put_via_cache(&mut store, &mut root, b"k2", &big);
        assert_eq!((root.overflow_pages, root.leaf_pages), (1, 1), "one leaf and a one-page run");
        let walk = walk_tree(&store, &root);
        assert_eq!(walk.entries[2], (b"k2".to_vec(), big.clone()));
        put_via_cache(&mut store, &mut root, b"k2", b"tiny");
        assert_eq!(root.overflow_pages, 0, "the run was freed");
        walk_tree(&store, &root);
        #[cfg(debug_assertions)]
        assert_eq!(cached() - before, 1, "only the inline put took the cached leaf");
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// A tree of several leaves with merging off (an emptied leaf unlinks), its path cache filled
    /// by a delete in a middle leaf, then the rest of that leaf deleted without the cache: the
    /// leaf unlinked and its page freed, so the epoch moved while the root and the height stayed.
    /// Returns the store, the tree, its trust, that stale cache and the key the cached delete took.
    fn stale_cache_over_an_unlinked_leaf() -> (MemStore, TreeRoot, bool, PathCache, Vec<u8>) {
        let mut store = MemStore::new(4096);
        store.no_merge = true;
        let mut root = TreeRoot::EMPTY;
        let key = |i: u32| format!("key{i:04}").into_bytes();
        for i in 0..60 {
            put(&mut store, &mut root, &key(i), &[7; 300]).expect("put");
        }
        assert!(root.height == 2 && root.leaf_pages >= 4, "a root over several leaves");
        let (mut trusted, mut cache) = (store.is_private(root.root), PathCache::default());
        let removed = delete_cached(&mut store, &mut root, &mut trusted, &mut cache, &key(30))
            .expect("delete");
        assert!(removed.found && removed.slot.is_some(), "the delete filled the cache");
        let in_leaf = |k: &[u8]| {
            cache.lo.get().is_none_or(|lo| page::cmp_keys(k, lo).is_ge())
                && cache.hi.get().is_none_or(|hi| page::cmp_keys(k, hi).is_lt())
        };
        let rest: Vec<u32> = (0..60).filter(|&i| i != 30 && in_leaf(&key(i))).collect();
        let (top, height, epoch) = (root.root, root.height, cache.epoch);
        for &i in &rest {
            assert!(delete(&mut store, &mut root, &key(i)).expect("delete"), "key {i}");
        }
        assert_eq!((root.root, root.height), (top, height), "an unlink, the root kept");
        assert_ne!(store.epoch(), epoch, "the unlink freed the cached leaf");
        (store, root, trusted, cache, key(30))
    }

    /// A put or a delete through a path cache takes the cached leaf only in the epoch the cache
    /// was filled in: through a cache whose leaf was unlinked since by writes that bypassed it
    /// ([`stale_cache_over_an_unlinked_leaf`]), a put of a key in the leaf's old range lands in
    /// the tree, and a delete of such a key, put back without the cache, finds it.
    #[test]
    fn a_path_cache_of_another_epoch_is_never_used() {
        let (mut store, mut root, mut trusted, mut cache, k) = stale_cache_over_an_unlinked_leaf();
        let entries = root.entries;
        put_cached(&mut store, &mut root, &mut trusted, &mut cache, &k, b"back", PutKind::Upsert)
            .expect("put");
        assert_eq!(get(&store, &root, &k).expect("get"), Some(&b"back"[..]));
        assert_eq!(root.entries, entries + 1);
        store.commit_like();
        store.assert_accounting(&[root]);

        let (mut store, mut root, mut trusted, mut cache, k) = stale_cache_over_an_unlinked_leaf();
        put(&mut store, &mut root, &k, b"back").expect("put");
        let removed =
            delete_cached(&mut store, &mut root, &mut trusted, &mut cache, &k).expect("delete");
        assert!(removed.found, "the key was found where it lives");
        assert_eq!(get(&store, &root, &k).expect("get"), None);
        assert_eq!(root.entries, entries);
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// Twin stores holding the same tree of several leaves (60 keys of 300 B: a root over five
    /// leaves) with merging off, one for writes through a path cache and one for the same writes
    /// without it, and the tree's keys.
    fn twin_trees() -> ([(MemStore, TreeRoot); 2], impl Fn(u32) -> Vec<u8>) {
        let key = |i: u32| format!("key{i:04}").into_bytes();
        let build = || {
            let mut store = MemStore::new(4096);
            store.no_merge = true;
            let mut root = TreeRoot::EMPTY;
            for i in 0..60 {
                put(&mut store, &mut root, &key(i), &[7; 300]).expect("put");
            }
            assert!(root.height == 2 && root.leaf_pages >= 4, "a root over several leaves");
            (store, root)
        };
        ([build(), build()], key)
    }

    /// Whether `key` lies in the cached leaf's range `[lo, hi)`.
    fn in_cached_range(cache: &PathCache, key: &[u8]) -> bool {
        cache.lo.get().is_none_or(|lo| page::cmp_keys(key, lo).is_ge())
            && cache.hi.get().is_none_or(|hi| page::cmp_keys(key, hi).is_lt())
    }

    /// With merging off, deletes through the path cache take a middle leaf's entries down to its
    /// last one in the leaf, and the delete of that last one unlinks the leaf, as the same deletes
    /// without the cache do: after each, both trees' handles agree, and the leaf is gone (a
    /// non-root leaf is never empty).
    #[test]
    fn a_cached_delete_leaves_a_leafs_last_entry_to_the_descent() {
        let ([(mut store, mut root), (mut plain, mut plain_root)], key) = twin_trees();
        let mut cache = PathCache::default();
        assert!(delete_through(&mut store, &mut root, Some(&mut cache), &key(30)).expect("delete"));
        assert!(delete(&mut plain, &mut plain_root, &key(30)).expect("delete"));
        assert!(cache.lo.get().is_some() && cache.hi.get().is_some(), "a middle leaf is cached");
        let rest: Vec<u32> =
            (0..60).filter(|&i| i != 30 && in_cached_range(&cache, &key(i))).collect();
        assert!(rest.len() >= 2, "the leaf holds more entries");
        let (leaves, before) = (root.leaf_pages, cached_deletes());
        for &i in &rest {
            assert!(
                delete_through(&mut store, &mut root, Some(&mut cache), &key(i)).expect("delete")
            );
            assert!(delete(&mut plain, &mut plain_root, &key(i)).expect("delete"));
            assert_eq!(
                root, plain_root,
                "key {i}: the cached delete changed what the descent does"
            );
        }
        assert_eq!(root.leaf_pages, leaves - 1, "the emptied leaf was unlinked");
        let cached = cached_deletes() - before;
        assert!(
            cached == rest.len() as u64 - 1 || !cfg!(debug_assertions),
            "{cached} deletes in the cached leaf: all but its last entry's"
        );
        assert_eq!(walk_tree(&store, &root).entries, walk_tree(&plain, &plain_root).entries);
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// A delete's miss refills the path cache only on a path of this transaction's own pages:
    /// after a commit, a put through the cache in one leaf copies its path (the root is then
    /// private and trusted), a miss in another leaf, still published, descends from that root, and
    /// a put into the published leaf's range through the cache then copies its path, as the same
    /// writes without the cache do.
    #[test]
    fn a_delete_miss_over_a_published_leaf_leaves_the_path_cache_empty() {
        let ([(mut store, mut root), (mut plain, mut plain_root)], key) = twin_trees();
        store.commit_like();
        plain.commit_like();
        let mut cache = PathCache::default();
        assert!(put_through(&mut store, &mut root, Some(&mut cache), &key(5), b"a").expect("put"));
        assert!(put(&mut plain, &mut plain_root, &key(5), b"a").expect("put"));
        assert!(store.is_private(root.root) && in_cached_range(&cache, &key(5)), "leaf A cached");
        // Absent, between two keys of a leaf the put did not copy; then put there.
        let missing = b"key0040x".to_vec();
        assert!(!in_cached_range(&cache, &missing), "another leaf");
        assert!(!delete_through(&mut store, &mut root, Some(&mut cache), &missing).expect("delete"));
        assert!(!delete(&mut plain, &mut plain_root, &missing).expect("delete"));
        assert!(!put_through(&mut store, &mut root, Some(&mut cache), &missing, b"b").expect("put"));
        assert!(!put(&mut plain, &mut plain_root, &missing, b"b").expect("put"));
        assert_eq!(root, plain_root, "the put through the cache changed what the descent does");
        assert_eq!(get(&store, &root, &missing).expect("get"), Some(&b"b"[..]));
        assert_eq!(walk_tree(&store, &root).entries, walk_tree(&plain, &plain_root).entries);
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// After a put through `cache` that returned `slot`: with a slot, the cache holds in this
    /// epoch and names what a fresh trusted descent for `key` finds (the branches, the leaf, its
    /// range, and `key` at the slot), which is where a cursor's put places the cursor; without
    /// one, the cache is empty.
    fn assert_cache_found(
        store: &MemStore,
        root: &TreeRoot,
        cache: &PathCache,
        key: &[u8],
        slot: Option<usize>,
        what: &str,
    ) {
        let Some(slot) = slot else {
            assert_eq!(cache.epoch, 0, "{what}: a put that placed no entry left a cache");
            return;
        };
        let mut path = Path::new();
        let (found, _) =
            descend_write(store, root, true, key, Verify::Now, &mut path).expect("descent");
        assert_eq!(cache.epoch, store.epoch(), "{what}: the cache holds");
        assert_eq!(cache.branches(), &*path, "{what}: the branches");
        assert_eq!(cache.leaf(), found.pgno, "{what}: the leaf");
        assert_eq!((cache.lo.get(), cache.hi.get()), found.range, "{what}: the range");
        let at = found.view.find(key).expect("find");
        assert!(matches!(at, Find::Found { index, .. } if index == slot), "{what}: slot {slot}");
    }

    /// Ascending puts through a table's path cache (appends, and upserts and inserts past the last
    /// key, which split in append mode too) across leaf, branch and root splits, with replaces of
    /// present keys (growing some until they split), inserts and upserts of new keys between
    /// present keys (some past the last key of a full leaf, which split it as any other) and
    /// commits (the next put copies its path) between them: after every put, the cache names
    /// what a fresh descent finds ([`assert_cache_found`]), and the tree holds the model. Every
    /// shape of split happens: past the last key, the leaf's parent takes the separator (the
    /// cache filled from the split, on a private path and on one the put copied), a branch
    /// below the root splits, or a new root grows (both filled by a descent); elsewhere, the
    /// cache is left empty.
    #[test]
    fn an_append_split_caches_the_path_a_descent_finds() {
        let mut store = MemStore::new(4096);
        let mut rng = Rng(0x5DEE_CE66_D1CE_4E5B);
        let (mut root, mut trusted, mut cache) = (TreeRoot::EMPTY, false, PathCache::default());
        let mut model = BTreeMap::new();
        // Leaf splits by shape: past the last key, the separator taken by the leaf's parent (0),
        // by a branch below the root that split (1), or by a new root (2); elsewhere (3); and
        // shape 0 again where the put copied its path from published pages (4).
        let mut splits = [0u32; 5];
        // 200-byte keys: about 19 children a branch, so the tree reaches height 3 and splits
        // branches below its root within a few hundred leaves.
        let key = |id: u64| id_key(id, 200);
        let mut next = 0u64;
        for op in 0..2_500u64 {
            let (k, kind) = match rng.below(10) {
                0 => (key(2 * next), PutKind::Upsert),
                1 => (key(2 * next), PutKind::Insert),
                2 if next > 0 => (key(2 * rng.below(next)), PutKind::Upsert),
                3 if next > 0 => match key(2 * rng.below(next) + 1) {
                    k if model.contains_key(&k) || rng.below(2) == 0 => (k, PutKind::Upsert),
                    k => (k, PutKind::Insert),
                },
                _ => (key(2 * next), PutKind::Append),
            };
            let past_last = model.last_key_value().is_none_or(|(last, _)| k > *last);
            if past_last {
                next += 1;
            }
            let v = vec![op as u8; 40 + rng.below(160) as usize];
            let (before, copied) = (root, !trusted);
            let placed = put_cached(&mut store, &mut root, &mut trusted, &mut cache, &k, &v, kind)
                .expect("put");
            assert_eq!(placed.replaced, model.insert(k.clone(), v).is_some(), "op {op}");
            // A put into the empty tree adds the first leaf: no split.
            if before.height > 0 && root.leaf_pages > before.leaf_pages {
                let shape = if !past_last {
                    3
                } else if root.height > before.height {
                    2
                } else if root.branch_pages > before.branch_pages {
                    1
                } else {
                    0
                };
                splits[shape] += 1;
                if shape == 0 && copied {
                    splits[4] += 1;
                }
            }
            assert_cache_found(&store, &root, &cache, &k, placed.slot, &format!("op {op}"));
            if op % 23 == 22 {
                store.commit_like();
                trusted = false;
            }
        }
        assert!(splits.iter().all(|&n| n > 0), "splits by shape {splits:?}");
        assert!(root.height >= 3, "height {}", root.height);
        assert_holds(&store, &root, &model);
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// Over-limit entries are refused before anything changes.
    #[test]
    fn over_limit_entries_are_refused() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let key = vec![1; g.max_key() + 1];
        assert!(matches!(put(&mut store, &mut root, &key, b""), Err(Error::KeyTooLarge { .. })));
        // A table's limits: 16 MiB, or the inline limit for a key too long for an overflow cell.
        let value = vec![1; overflow::MAX_VALUE + 1];
        let put_value = put(&mut store, &mut root, b"four", &value);
        assert!(matches!(put_value, Err(Error::ValueTooLarge { .. })));
        let long = vec![2; g.max_key() - OVF_CELL + 3];
        let put_value = put(&mut store, &mut root, &long, &vec![1; g.max_value(long.len()) + 1]);
        assert!(matches!(put_value, Err(Error::ValueTooLarge { .. })));
        // The catalog's and the free list's entry point takes inline values only.
        let value = vec![1; g.max_value(4) + 1];
        let mut trusted = false;
        let put_value = super::put(&mut store, &mut root, &mut trusted, b"four", &value);
        assert!(matches!(put_value, Err(Error::ValueTooLarge { .. })));
        assert_eq!(root, TreeRoot::EMPTY);
        assert_eq!(store.alloc.page_count(), 2, "nothing allocated");
    }

    /// A `klen`-byte key that sorts by `id`.
    fn id_key(id: u64, klen: usize) -> Vec<u8> {
        let mut k = vec![0x5A; klen.max(8)];
        k[..8].copy_from_slice(&id.to_be_bytes());
        k
    }

    /// A tree of keys `id_key(0..n, klen)` (inserted out of order, 100-byte values), with the
    /// model, committed and reopened (nothing stamped by the writer).
    fn wide_tree(n: u64, klen: usize) -> (MemStore, TreeRoot, BTreeMap<Vec<u8>, Vec<u8>>) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        for i in 0..n {
            let (k, v) = (id_key(i * 7919 % n, klen), vec![i as u8; 100]);
            put(&mut store, &mut root, &k, &v).expect("put");
            model.insert(k, v);
        }
        store.reopen();
        (store, root, model)
    }

    /// The tree holds exactly the model.
    fn assert_holds(store: &MemStore, root: &TreeRoot, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let walk = walk_tree(store, root);
        assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "entries");
    }

    /// A delete that misses (below, between and above the keys, and a key over the limit) runs
    /// only the lookup: it verifies, copies, allocates and frees nothing, and leaves the root and
    /// every reachable page byte for byte as they were, published or private.
    #[test]
    fn a_delete_miss_copies_nothing() {
        let (mut store, mut root, _) = wide_tree(600, 200);
        assert!(root.height >= 3);
        let g = store.geometry();
        let misses = [
            vec![0],
            id_key(17, 201),
            id_key(300, 199),
            id_key(u64::MAX, 200),
            vec![0xFF; g.max_key() + 1],
        ];
        for private in [false, true] {
            if private {
                // Copy a path: the misses must leave private pages as they are too.
                assert!(delete(&mut store, &mut root, &id_key(17, 200)).expect("hit"));
            }
            let before = (root, store.reachable(&root), store.alloc.page_count());
            let freed = (store.alloc.freed().to_vec(), store.alloc.pool().to_vec());
            #[cfg(debug_assertions)]
            COW_VERIFIES.with(|n| n.set(0));
            for key in &misses {
                assert!(!delete(&mut store, &mut root, key).expect("miss"), "{private}");
            }
            #[cfg(debug_assertions)]
            assert_eq!(COW_VERIFIES.with(std::cell::Cell::get), 0, "a miss verifies nothing");
            assert_eq!(root, before.0, "{private}: the root handle changed");
            assert!(store.reachable(&root) == before.1, "{private}: a reachable page changed");
            assert_eq!(store.alloc.page_count(), before.2, "{private}: a page was allocated");
            assert_eq!(
                (store.alloc.freed().to_vec(), store.alloc.pool().to_vec()),
                freed,
                "{private}: pages were freed"
            );
        }
    }

    /// A delete that misses ends at its one descent: it leaves the allocator's epoch (so the path
    /// cache), the root's provenance, the page count and every reachable page as they were, and
    /// verifies nothing, whether the path is published or this transaction's copy. A hit through
    /// published pages then verifies each one on its path once, before it copies them.
    #[test]
    fn a_delete_miss_moves_no_epoch_and_only_a_hit_verifies() {
        let (mut store, mut root, _) = wide_tree(600, 200);
        let misses = [vec![0], id_key(17, 201), id_key(300, 199), id_key(u64::MAX, 200)];
        let mut trusted = false;
        for private in [false, true] {
            if private {
                assert!(super::delete(&mut store, &mut root, &mut trusted, &id_key(17, 200))
                    .expect("hit"));
                assert!(trusted, "the copied path is this transaction's");
            }
            let before = (root, trusted, store.alloc.epoch(), store.alloc.page_count());
            let pages = store.reachable(&root);
            #[cfg(debug_assertions)]
            COW_VERIFIES.with(|n| n.set(0));
            for key in &misses {
                let found = super::delete(&mut store, &mut root, &mut trusted, key);
                assert!(!found.expect("miss"), "{private}");
            }
            #[cfg(debug_assertions)]
            assert_eq!(COW_VERIFIES.with(std::cell::Cell::get), 0, "{private}: a miss verified");
            let after = (root, trusted, store.alloc.epoch(), store.alloc.page_count());
            assert_eq!(after, before, "{private}: (root, trust, epoch, page count)");
            assert!(store.reachable(&root) == pages, "{private}: a reachable page changed");
        }
        store.reopen();
        let mut trusted = false;
        #[cfg(debug_assertions)]
        COW_VERIFIES.with(|n| n.set(0));
        assert!(super::delete(&mut store, &mut root, &mut trusted, &id_key(300, 200)).expect("hit"));
        assert_eq!(store.merge_report, MergeReport::default(), "no merge: only the path is read");
        #[cfg(debug_assertions)]
        assert_eq!(COW_VERIFIES.with(std::cell::Cell::get), u64::from(root.height), "a hit");
    }

    /// A delete that misses keeps a lookup's checks: a published path damaged at rest is
    /// `Corrupt`, with nothing changed. A leaf whose middle key (its search's first probe) was
    /// rewritten past the leaf's range under a stale CRC: the keys either side of the miss must
    /// lie within the separators (`check_miss`), though the miss verifies no CRC. A root cell
    /// redirected, identity and all, to the next subtree (`keys_in_range`, then `check_miss`). A
    /// root cell naming its child with a stale txn (the identity check). A hit in the damaged leaf
    /// is `Corrupt` too: its CRC fails (`verify_path`).
    #[test]
    fn a_delete_miss_on_a_damaged_path_is_corrupt() {
        let id = |page: &[u8], i: usize| {
            let k = page::leaf_key(page, i).expect("key");
            u64::from_be_bytes(k[..8].try_into().expect("id"))
        };
        for case in ["key past the range", "redirect", "stale txn", "hit in the damaged leaf"] {
            let (mut store, mut root, _) = wide_tree(600, 200);
            assert!(root.height >= 3);
            let walk = walk_tree(&store, &root);
            let leaf = walk.leaves[walk.leaves.len() / 2];
            let n = page::nkeys(store.raw(leaf));
            assert!(n >= 3);
            let r = root.root;
            let (key, msg) = match case {
                "key past the range" | "hit in the damaged leaf" => {
                    // The search's first probe is cell n / 2: the miss just past its old key
                    // lands next to it.
                    let (mid, first) = (id(store.raw(leaf), n / 2), id(store.raw(leaf), 0));
                    let at = page::leaf_key(store.raw(leaf), n / 2).expect("key").as_ptr() as usize
                        - store.raw(leaf).as_ptr() as usize;
                    store.raw_mut(leaf)[at..at + 8].copy_from_slice(&u64::MAX.to_be_bytes());
                    match case {
                        "key past the range" => (id_key(mid, 201), "is not where its parents say"),
                        _ => (id_key(first, 200), "fails its CRC"),
                    }
                }
                "redirect" => {
                    // A miss in the middle of subtree 0, whose cell now names subtree 1.
                    let sub = page::branch_child(store.raw(r), 0).expect("child").0;
                    let sub_leaf = page::branch_child(store.raw(sub), 1).expect("child").0;
                    let key = id_key(id(store.raw(sub_leaf), 0), 201);
                    let (next, next_txn) = page::branch_child(store.raw(r), 1).expect("child");
                    set_child_restamped(&mut store, r, 0, next, next_txn);
                    (key, "outside its parents' separators")
                }
                _ => {
                    let (c, c_txn) = page::branch_child(store.raw(r), 0).expect("child");
                    set_child_restamped(&mut store, r, 0, c, c_txn.wrapping_sub(1));
                    (id_key(0, 201), "is not the copy its parent names")
                }
            };
            assert_delete_refused(&mut store, &mut root, &key, msg);
        }
    }

    /// Deleting every key (in scrambled order) empties leaves, unlinks them, collapses the root
    /// level by level and ends at the empty tree, which holds no page: after the commit every page
    /// is retired or pooled, and the tree refills. A snapshot of the full tree keeps reading every
    /// entry throughout. Three rounds, so later ones delete from trees built over reused pages.
    #[test]
    fn deleting_every_key_collapses_to_the_empty_tree() {
        let n = 600_u64;
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for round in 0..3_u64 {
            let mut model = BTreeMap::new();
            for i in 0..n {
                let (k, v) = (id_key(i * 7 % n, 200), vec![(i + round) as u8; 100]);
                put(&mut store, &mut root, &k, &v).expect("put");
                model.insert(k, v);
            }
            store.commit_like();
            let (full, full_txn) = (root, store.alloc.txn() - 1);
            assert!(full.height >= 3, "round {round}: height {}", full.height);
            let tree: Vec<u32> = store.reachable(&full).into_iter().map(|(p, _)| p).collect();
            let mut heights = vec![root.height];
            for (j, i) in (0..n).map(|i| i * 11 % n).enumerate() {
                let k = id_key(i, 200);
                assert!(delete(&mut store, &mut root, &k).expect("delete"), "round {round}");
                model.remove(&k);
                if heights.last() != Some(&root.height) {
                    heights.push(root.height);
                }
                if j % 50 == 0 || model.len() < 30 {
                    assert_holds(&store, &root, &model);
                }
            }
            assert_eq!(root, TreeRoot::EMPTY, "round {round}");
            let expect: Vec<u32> = (0..=full.height).rev().collect();
            assert_eq!(heights, expect, "round {round}: the root collapses one level at a time");
            // Every page of the published tree was freed once, as it was (no copy).
            let mut freed = store.alloc.freed().to_vec();
            freed.sort_unstable();
            let mut want = tree.clone();
            want.sort_unstable();
            assert_eq!(freed, want, "round {round}: the published tree's pages are freed");
            store.commit_like();
            store.assert_accounting(&[root]);
            let reader = store.reader_at(full_txn);
            assert_eq!(walk_tree(&reader, &full).entries.len() as u64, n, "round {round}");
            store.recycle();
        }
    }

    /// Ascending keys with the oldest deleted (a sliding window), committing and reusing freed
    /// pages as it goes: emptied leaves are unlinked and their pages reused, so the file stays the
    /// size of the window, not of every key ever written.
    #[test]
    fn a_sliding_window_of_deletes_stays_bounded() {
        const WINDOW: u64 = 1_000;
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..40_000_u64 {
            put(&mut store, &mut root, &i.to_be_bytes(), &i.to_le_bytes()).expect("put");
            if i >= WINDOW {
                assert!(delete(&mut store, &mut root, &(i - WINDOW).to_be_bytes()).expect("del"));
            }
            if i % 100 == 99 {
                store.commit_like();
                store.recycle();
            }
        }
        let walk = walk_tree(&store, &root);
        let expect: Vec<(Vec<u8>, Vec<u8>)> = (40_000 - WINDOW..40_000)
            .map(|i: u64| (i.to_be_bytes().to_vec(), i.to_le_bytes().to_vec()))
            .collect();
        assert_eq!(walk.entries, expect);
        let pages = store.alloc.page_count();
        assert!(
            pages < 25,
            "{pages} pages for a {WINDOW}-key window of {} pages",
            walk.pages.len()
        );
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// The first child of a branch emptying takes its cell 0 with it: the next child becomes
    /// cell 0, its key dropped, so it now takes every key below its old lower bound (as far down
    /// as the parent's own range). Keys below every remaining one then go to it, and the tree
    /// stays sound.
    #[test]
    fn unlinking_a_first_child_widens_the_next_to_the_parents_range() {
        let (mut store, mut root, mut model) = wide_tree(600, 200);
        let leftmost = walk_tree(&store, &root).leaves[0];
        let first_keys: Vec<Vec<u8>> = (0..page::nkeys(store.raw(leftmost)))
            .map(|i| page::leaf_key(store.raw(leftmost), i).expect("key").to_vec())
            .collect();
        for k in &first_keys {
            assert!(delete(&mut store, &mut root, k).expect("delete"));
            model.remove(k);
        }
        let walk = walk_tree(&store, &root);
        assert!(!walk.pages.contains(&leftmost), "the emptied leaf left the tree");
        assert_holds(&store, &root, &model);
        // Below every key left: into the leaf that became the leftmost.
        let leaf_of = |store: &MemStore, root: &TreeRoot, k: &[u8]| {
            descend(store, root, k, |_, _, _, _| Ok(()), None).expect("descend").expect("leaf").pgno
        };
        let smallest = model.keys().next().expect("a key").clone();
        assert_eq!(leaf_of(&store, &root, &[0]), walk.leaves[0]);
        assert_eq!(leaf_of(&store, &root, &smallest), walk.leaves[0]);
        for k in [vec![0], id_key(0, 8), first_keys[0].clone()] {
            put(&mut store, &mut root, &k, b"low").expect("put");
            model.insert(k.clone(), b"low".to_vec());
            assert_eq!(get(&store, &root, &k).expect("get"), Some(&b"low"[..]));
        }
        assert_holds(&store, &root, &model);
    }

    /// Delete wide entries (four per leaf, so emptied leaves unlink and roots collapse often)
    /// from a deep published tree, failing each allocation of each delete in turn: a failed delete
    /// leaves the root handle, every reachable page (byte for byte) and the freed list exactly as
    /// they were, and gives back every page it took.
    #[test]
    fn a_failed_allocation_during_a_delete_leaves_the_tree_identical() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for i in 0..400_u64 {
            let k = id_key(i * 7919 % 400, 600 + rng.len_upto(g.max_key() - 600));
            let v = vec![i as u8; g.max_value(k.len())];
            put(&mut store, &mut root, &k, &v).expect("put");
            model.insert(k, v);
        }
        store.reopen();
        assert!(root.height >= 4, "height {}", root.height);
        let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
        let (mut failures, mut unlinks, mut collapses) = (0, 0, 0);
        for (n, k) in (0..keys.len()).map(|i| &keys[i * 13 % keys.len()]).enumerate() {
            let pages = store.reachable(&root).len();
            let height = root.height;
            for fail_at in 0.. {
                let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
                let (pool, count) = (store.alloc.pool().len(), store.alloc.page_count());
                store.alloc.fail_allocs_after = Some(fail_at);
                let result = delete(&mut store, &mut root, k);
                store.alloc.fail_allocs_after = None;
                match result {
                    Ok(true) => break,
                    Err(Error::Io(_)) => failures += 1,
                    other => panic!("delete {n}, failing allocation {fail_at}: {other:?}"),
                }
                let case = format!("delete {n}, failing allocation {fail_at}");
                assert_eq!(root, before.0, "{case}: the root handle changed");
                assert!(store.reachable(&root) == before.1, "{case}: a reachable page changed");
                assert_eq!(store.alloc.freed(), before.2, "{case}: pages were freed");
                let grown = (store.alloc.page_count() - count) as usize;
                assert_eq!(store.alloc.pool().len(), pool + grown, "{case}: pages taken were kept");
            }
            model.remove(k);
            unlinks += usize::from(store.reachable(&root).len() < pages);
            collapses += usize::from(root.height < height && !root.is_empty());
            if n % 8 == 7 {
                assert_holds(&store, &root, &model);
                if n % 32 == 31 {
                    store.reopen();
                } else {
                    store.commit_like();
                }
                store.assert_accounting(&[root]);
            }
        }
        assert_eq!(root, TreeRoot::EMPTY);
        assert!(failures > 300, "{failures} failed allocations");
        assert!(unlinks > 50 && collapses >= 3, "{unlinks} unlinks, {collapses} collapses");
    }

    /// A merging delete reserves the copies of its path before it changes anything, and its
    /// merges allocate nothing (a sibling's cells move into the page on the path; the sibling
    /// leaves the tree as it is). Every key is deleted from a three-level published tree of long
    /// keys, whose deletes merge leaves, merge branches and leave a root with one child (which
    /// gives way), failing each allocation of each delete in turn: a failed delete leaves the root
    /// handle (its counters included), every reachable page byte for byte and the freed list as
    /// they were, and gives back exactly the pages it took (the pool is the old one plus the pages
    /// the file grew by). Every delete starts from a published path: a commit after each, a reopen
    /// after every third (nothing stamped, so each sibling is verified), and the retired pages
    /// recycled after every fifth (copies then come from the pool, not the file's end).
    #[test]
    fn a_failed_allocation_during_a_merging_delete_leaves_the_tree_identical() {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let n = 400_u64;
        for i in 0..n {
            let k = id_key(i * 7919 % n, 300 + rng.len_upto(500));
            let v = vec![i as u8; rng.len_upto(60)];
            put(&mut store, &mut root, &k, &v).expect("put");
            model.insert(k, v);
        }
        store.reopen();
        assert!(root.height >= 3, "height {}", root.height);
        let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
        // Deletes that merged leaves, merged branches or gave the root way, each counted when a
        // failed allocation came first.
        let (mut failures, mut leaf, mut branch, mut merged_root) = (0, 0, 0, 0);
        for (d, k) in (0..keys.len()).map(|i| &keys[i * 37 % keys.len()]).enumerate() {
            let mut failed = 0;
            for fail_at in 0.. {
                let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
                let (pool, count) = (store.alloc.pool().to_vec(), store.alloc.page_count());
                store.alloc.fail_allocs_after = Some(fail_at);
                let result = delete(&mut store, &mut root, k);
                store.alloc.fail_allocs_after = None;
                match result {
                    Ok(true) => break,
                    Err(Error::Io(_)) => failed += 1,
                    other => panic!("delete {d}, failing allocation {fail_at}: {other:?}"),
                }
                let case = format!("delete {d}, failing allocation {fail_at}");
                assert_eq!(root, before.0, "{case}: the root handle changed");
                assert!(store.reachable(&root) == before.1, "{case}: a reachable page changed");
                assert_eq!(store.alloc.freed(), before.2, "{case}: pages were freed");
                let mut given_back = store.alloc.pool().to_vec();
                given_back.sort_unstable();
                let mut want: Vec<u32> =
                    pool.into_iter().chain(count..store.alloc.page_count()).collect();
                want.sort_unstable();
                assert_eq!(given_back, want, "{case}: the pages taken, and only those, came back");
            }
            let report = store.merge_report;
            if failed > 0 {
                failures += failed;
                leaf += usize::from(report.leaf_left + report.leaf_right > 0);
                branch += usize::from(report.branches > 0);
                merged_root += usize::from(report.root);
            }
            model.remove(k);
            if d % 3 == 2 {
                store.reopen();
            } else {
                store.commit_like();
            }
            if d % 5 == 4 {
                store.recycle();
            }
            if d % 16 == 15 {
                assert_holds(&store, &root, &model);
                store.assert_accounting(&[root]);
            }
        }
        assert_eq!(root, TreeRoot::EMPTY);
        let counts = format!(
            "{failures} failed allocations; with one: {leaf} leaf merges, {branch} branch \
             merges, {merged_root} roots giving way"
        );
        assert!(failures > 1000 && leaf > 50 && branch > 10 && merged_root >= 2, "{counts}");
    }

    /// A tree of `n` entries whose first version's pages are retired, intact (a pool of stray
    /// pages a damaged pointer can name), reopened.
    fn tree_with_strays(n: u64) -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for round in 0..2_u64 {
            for i in 0..n {
                put(&mut store, &mut root, &id_key(i, 200), &[(i + round) as u8; 100])
                    .expect("put");
            }
            store.commit_like();
        }
        store.reopen();
        (store, root)
    }

    /// The free walk takes the old tree's pages from its child pointers, so each branch read from
    /// disk must pass its CRC first and every pointer must name a tree page not reached before:
    /// a pointer redirected to a stray page with a stale CRC, or (CRC-valid) to a meta page, past
    /// the page count or to a page the tree already reaches, refuses the clear before anything is
    /// freed, leaving the root handle as it was.
    #[test]
    fn the_free_walk_refuses_a_damaged_pointer_and_frees_nothing() {
        for damage in 0..4 {
            let (mut store, mut root) = tree_with_strays(600);
            assert!(root.height >= 3, "height {}", root.height);
            let walk = walk_tree(&store, &root);
            let tree: HashSet<u32> = walk.pages.iter().copied().collect();
            let stray = (2..store.alloc.page_count()).find(|p| !tree.contains(p)).expect("stray");
            // The last branch above the leaves, so the walk has read others before reaching it.
            let branch = *walk
                .pages
                .iter()
                .rev()
                .find(|&&p| page::kind(store.raw(p)).expect("kind") == Kind::Branch)
                .expect("a branch");
            let (target, restamp) = match damage {
                0 => (stray, false),
                1 => (1, true),
                2 => (store.alloc.page_count(), true),
                _ => (walk.leaves[0], true),
            };
            let target_txn = page::txn(store.raw(target.min(store.alloc.page_count() - 1))) as u32;
            let last = page::nkeys(store.raw(branch)) - 1;
            page::branch_set_child(store.raw_mut(branch), last, target, target_txn).expect("set");
            if restamp {
                store.restamp(branch);
            }
            let before = root;
            let cleared = clear(&mut store, &mut root);
            assert!(matches!(cleared, Err(Error::Corrupt(_))), "damage {damage}: {cleared:?}");
            assert_eq!(root, before, "damage {damage}: the root handle changed");
            assert!(store.alloc.freed().is_empty(), "damage {damage}: pages were freed");
            assert!(store.alloc.pool().is_empty(), "damage {damage}: pages were pooled");
        }
    }

    /// The free walk also refuses a CRC-valid pointer that names the right page but a stale txn
    /// (the page is not the copy its parent names), and a handle whose height is off by one
    /// either way (a leaf read as a branch, or the walk ending above the leaves).
    #[test]
    fn the_free_walk_refuses_a_stale_pointer_or_a_wrong_height() {
        for damage in 0..3 {
            let (mut store, root) = tree_with_strays(600);
            let mut handle = root;
            match damage {
                0 => {
                    let (child, txn) = page::branch_child(store.raw(root.root), 0).expect("child");
                    page::branch_set_child(store.raw_mut(root.root), 0, child, txn ^ 1)
                        .expect("set");
                    store.restamp(root.root);
                }
                1 => handle.height += 1,
                _ => handle.height -= 1,
            }
            let before = handle;
            let cleared = clear(&mut store, &mut handle);
            assert!(matches!(cleared, Err(Error::Corrupt(_))), "damage {damage}: {cleared:?}");
            assert_eq!(handle, before, "damage {damage}: the root handle changed");
            assert!(store.alloc.freed().is_empty(), "damage {damage}: pages were freed");
        }
    }

    /// A page this transaction allocated for another tree (that tree's root leaf, or a middle page
    /// of its overflow run) named by the tree a clear frees: by a published branch's cell, by an
    /// untrusted root handle, or by a cell that a forged published branch passed on to its private
    /// copy (the cell's `child_txn` an older txn's, so the copy-time rule does not read it). The
    /// free walk refuses each before anything is freed (`tree_pages`: the provenance rule, and the
    /// identity check of a private child): freed, the page would go back to the pool at once while
    /// the other tree holds it.
    #[test]
    fn the_free_walk_refuses_a_page_private_to_another_tree() {
        for run in [false, true] {
            for case in ["published cell", "root handle", "copied cell"] {
                let what = format!("{case}, run {run}");
                let (mut store, root) = reopened_tree(2_000);
                assert_eq!(root.height, 2, "a published root branch over the leaves");
                let other_value = vec![7_u8; if run { 10_000 } else { 100 }];
                let mut other = TreeRoot::EMPTY;
                put(&mut store, &mut other, b"other", &other_value).expect("put");
                let x = if run { store.run_pgnos(&other)[1] } else { other.root };
                assert!(store.is_private(x), "{what}: X is private to this transaction");
                let x_before = store.raw(x).to_vec();
                let r = root.root;
                let last = page::nkeys(store.raw(r)) - 1;
                let (_, last_txn) = page::branch_child(store.raw(r), last).expect("child");
                let txn32 = store.txn() as u32;
                let (mut handle, mut trusted) = (root, false);
                match case {
                    "published cell" => {
                        page::branch_set_child(store.raw_mut(r), last, x, txn32).expect("forge");
                        store.restamp(r);
                    }
                    "root handle" => {
                        handle =
                            TreeRoot { root: x, root_txn: txn32, height: 1, ..TreeRoot::EMPTY };
                        handle.entries = 1;
                        handle.leaf_pages = 1;
                    }
                    _ => {
                        page::branch_set_child(store.raw_mut(r), last, x, last_txn).expect("forge");
                        store.restamp(r);
                        super::put(
                            &mut store,
                            &mut handle,
                            &mut trusted,
                            &0_u64.to_be_bytes(),
                            b"a",
                        )
                        .expect("a put under cell 0 copies the root");
                        assert!(trusted && store.is_private(handle.root), "{what}: a private copy");
                    }
                }
                let lists =
                    |store: &MemStore| (store.alloc.pool().to_vec(), store.alloc.freed().to_vec());
                let before = (handle, lists(&store));
                let result = super::clear(&mut store, &mut handle, trusted);
                // A run's middle page has no header: its first bytes are the value's, which the
                // fetch refuses as another page number before the identity check compares them.
                let want = match case {
                    "copied cell" if run => "holds page",
                    "copied cell" => "is not the copy its parent names",
                    _ => "is private to this transaction but named by a page it did not write",
                };
                let Err(Error::Corrupt(msg)) = &result else { panic!("{what}: {result:?}") };
                assert!(msg.contains(&format!("page {x} ")) && msg.contains(want), "{what}: {msg}");
                assert!((handle, lists(&store)) == before, "{what}: nothing freed");
                assert_eq!(store.raw(x), &x_before[..], "{what}: X is unchanged");
                let got = get(&store, &other, b"other").expect("get");
                assert_eq!(got, Some(&other_value[..]), "{what}: the other tree reads back");
            }
        }
    }

    /// A two-leaf tree of 200-byte keys `0..n` (appended, so the left leaf is full), its first
    /// version's pages retired intact, reopened; with the right leaf.
    fn two_leaf_tree(n: u64) -> (MemStore, TreeRoot, u32) {
        let (mut store, mut root) = (MemStore::new(4096), TreeRoot::EMPTY);
        for round in 0..2_u8 {
            for i in 0..n {
                put(&mut store, &mut root, &id_key(i, 200), &[round; 100]).expect("put");
            }
            store.commit_like();
        }
        store.reopen();
        let walk = walk_tree(&store, &root);
        assert_eq!((root.height, walk.leaves.len()), (2, 2), "two leaves under a root");
        (store, root, walk.leaves[1])
    }

    /// A collapse makes the root's remaining child the root without a descent reaching it, so it
    /// checks that page itself: a sibling damaged at rest (a flipped bit: its CRC fails), or a
    /// CRC-valid root pointer redirected to a stale copy of it (an older txn), is `Corrupt`, and
    /// the failed delete changes nothing. Merging is off (`MemStore::no_merge`): the left leaf
    /// would merge into the right one long before it empties, and no collapse would come.
    #[test]
    fn a_collapse_checks_the_page_it_promotes() {
        for damage in 0..2 {
            let (mut store, mut root, right) = two_leaf_tree(20);
            store.no_merge = true;
            let left_keys: Vec<Vec<u8>> =
                (0..20).map(|i| id_key(i, 200)).filter(|k| k < &right_key(&store, right)).collect();
            let (last, rest) = left_keys.split_last().expect("left keys");
            for k in rest {
                assert!(delete(&mut store, &mut root, k).expect("delete"));
            }
            if damage == 0 {
                let p = store.geometry().page_size();
                store.raw_mut(right)[p - 5] ^= 1;
            } else {
                // The old version of the right leaf: retired, intact, CRC-valid.
                let stale = (2..store.alloc.page_count())
                    .find(|&p| {
                        p != right
                            && page::kind(store.raw(p)).ok() == Some(Kind::Leaf)
                            && page::leaf_key(store.raw(p), 0).ok()
                                == Some(right_key(&store, right).as_slice())
                    })
                    .expect("a stale copy");
                let txn = page::branch_child(store.raw(root.root), 1).expect("child").1;
                page::branch_set_child(store.page_mut(root.root).expect("private"), 1, stale, txn)
                    .expect("set");
            }
            let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
            let pool = store.alloc.pool().to_vec();
            let deleted = delete(&mut store, &mut root, last);
            assert!(matches!(deleted, Err(Error::Corrupt(_))), "damage {damage}: {deleted:?}");
            assert_eq!(root, before.0, "damage {damage}: the root handle changed");
            assert!(store.reachable(&root) == before.1, "damage {damage}: a page changed");
            assert_eq!(store.alloc.freed(), before.2, "damage {damage}: pages were freed");
            assert_eq!(store.alloc.pool(), pool, "damage {damage}: pages were pooled");
        }
    }

    /// The first key of leaf `p`.
    fn right_key(store: &MemStore, p: u32) -> Vec<u8> {
        page::leaf_key(store.raw(p), 0).expect("key").to_vec()
    }

    /// A leaf of `entries` (`(key, value length)`, in order), private to the store's transaction.
    /// Each value is nonzero bytes, and cell 0 lies at the page's end: a flipped byte just before
    /// the trailer lands in its value, where only the CRC sees it.
    fn leaf_with(store: &mut MemStore, entries: &[(&[u8], usize)]) -> u32 {
        let (p, txn) = (store.alloc().expect("alloc"), store.txn());
        let page = store.page_mut(p).expect("page");
        page::init_leaf(page, p, txn);
        for (i, &(k, len)) in entries.iter().enumerate() {
            assert_eq!(page::leaf_insert(page, i, k, &vec![0xA5; len]).expect("insert"), Fit::Done);
        }
        p
    }

    /// A leaf of `keys` with 8-byte values ([`leaf_with`]).
    fn leaf_of(store: &mut MemStore, keys: &[&[u8]]) -> u32 {
        let entries: Vec<(&[u8], usize)> = keys.iter().map(|&k| (k, 8)).collect();
        leaf_with(store, &entries)
    }

    /// The handle of the tree a test built page by page under root branch `r` (private to the
    /// store's transaction), counted from its pages.
    fn built_root(store: &MemStore, r: u32, height: u32, entries: u64) -> TreeRoot {
        let root =
            TreeRoot { root: r, root_txn: store.txn() as u32, height, entries, ..TreeRoot::EMPTY };
        store.counted(root)
    }

    /// Publish the tree a test built: reopened (nothing stamped by this writer: each page verified
    /// before its first use), or, with `stamped`, committed by this writer (its cells trusted, as
    /// copy-on-write trusts a page it stamped).
    fn publish(store: &mut MemStore, stamped: bool) {
        if stamped {
            store.commit_like();
        } else {
            store.reopen();
        }
    }

    /// A published two-level tree ([`publish`]) of leaves `[a, b]`, `[m, n]` and `[t, u]` under
    /// separators `m` and `t`; with the leaves. Every page is far under a quarter full, so a
    /// delete from a leaf merges it with its neighbours: a delete of `n` absorbs the left leaf
    /// first, a delete of `b` the middle one.
    fn three_leaf_tree(stamped: bool) -> (MemStore, TreeRoot, [u32; 3]) {
        let mut store = MemStore::new(4096);
        let leaves = [
            leaf_of(&mut store, &[b"a", b"b"]),
            leaf_of(&mut store, &[b"m", b"n"]),
            leaf_of(&mut store, &[b"t", b"u"]),
        ];
        let cells =
            [(Vec::new(), leaves[0]), (b"m".to_vec(), leaves[1]), (b"t".to_vec(), leaves[2])];
        let r = branch_page(&mut store, &cells);
        let root = built_root(&store, r, 2, 6);
        publish(&mut store, stamped);
        (store, root, leaves)
    }

    /// A branch over a leaf of each of `leaves` ([`leaf_of`]), its cells keyed by each leaf's
    /// first key but cell 0's, private to the store's transaction.
    fn branch_of(store: &mut MemStore, leaves: &[&[&[u8]]]) -> u32 {
        let cells: Vec<(Vec<u8>, u32)> = leaves
            .iter()
            .enumerate()
            .map(|(i, keys)| {
                let key = if i == 0 { Vec::new() } else { keys[0].to_vec() };
                (key, leaf_of(store, keys))
            })
            .collect();
        branch_page(store, &cells)
    }

    /// A published three-level tree ([`publish`]) of branches `B0`, `B1`, `B2` under separators `m`
    /// and `t`, over leaves `[a, b]`, `[c, d]`, `[e]` (`B0`), `[m, n]`, `[p, q]`, `[s]` (`B1`) and
    /// `[t, u]`, `[v, w]` (`B2`); with the branches. A delete of `s` unlinks its leaf and leaves
    /// `B1` far under a quarter full, so it absorbs `B0` (left) first; a delete of `e` does the
    /// same to `B0`, which absorbs `B1` (right) first.
    fn three_branch_tree(stamped: bool) -> (MemStore, TreeRoot, [u32; 3]) {
        let mut store = MemStore::new(4096);
        let b0 = branch_of(&mut store, &[&[b"a", b"b"], &[b"c", b"d"], &[b"e"]]);
        let b1 = branch_of(&mut store, &[&[b"m", b"n"], &[b"p", b"q"], &[b"s"]]);
        let b2 = branch_of(&mut store, &[&[b"t", b"u"], &[b"v", b"w"]]);
        let r =
            branch_page(&mut store, &[(Vec::new(), b0), (b"m".to_vec(), b1), (b"t".to_vec(), b2)]);
        let root = built_root(&store, r, 3, 14);
        publish(&mut store, stamped);
        (store, root, [b0, b1, b2])
    }

    /// Point cell `i` of page `branch` at `(child, txn)` and stamp its CRC again: damage only the
    /// checks under test can find.
    fn set_child_restamped(store: &mut MemStore, branch: u32, i: usize, child: u32, txn: u32) {
        page::branch_set_child(store.raw_mut(branch), i, child, txn).expect("set");
        store.restamp(branch);
    }

    /// Delete `key` and assert the delete is `Corrupt`, its message holding `msg`, and changed
    /// nothing: the handle, every reachable page byte for byte, the freed list, the pool and the
    /// page count are as they were.
    fn assert_delete_refused(store: &mut MemStore, root: &mut TreeRoot, key: &[u8], msg: &str) {
        let before = (*root, store.reachable(root), store.alloc.freed().to_vec());
        let (pool, count) = (store.alloc.pool().to_vec(), store.alloc.page_count());
        let mut trusted = false;
        let result = super::delete(store, root, &mut trusted, key);
        let Err(Error::Corrupt(m)) = &result else { panic!("{msg}: {result:?}") };
        assert!(m.contains(msg), "expected {msg:?}: {m}");
        assert_eq!(*root, before.0, "{msg}: the root handle changed");
        assert!(store.reachable(root) == before.1, "{msg}: a reachable page changed");
        assert_eq!(store.alloc.freed(), before.2, "{msg}: pages were freed");
        assert_eq!((store.alloc.pool(), store.alloc.page_count()), (&pool[..], count), "{msg}");
    }

    /// A merge moves a sibling's cells into a page this transaction trusts, so the absorbed leaf
    /// gets a descent's checks first, from either side (the left neighbour of the page on the
    /// path, or the right one): a flipped value byte under a stale CRC (`verify_published`); a
    /// CRC-valid parent cell naming the sibling with a stale txn, or naming an older retired copy
    /// of it (`sibling_view`'s identity check); a parent cell redirected, identity and all, to a
    /// real leaf whose keys fall outside the cell's separators (the range check); a parent cell
    /// naming a page private to this transaction (`sibling_view`'s privacy rule); and an
    /// overflow cell naming a run this transaction allocated (`check_cells_naming_this_txn`).
    /// Each is `Corrupt`, found before anything changes. Undamaged, each delete merges the
    /// sibling (then the third leaf, and the root gives way).
    #[test]
    fn a_merge_refuses_a_damaged_leaf_sibling() {
        for (key, lefts) in [(b"n", 1), (b"b", 0)] {
            let (mut store, mut root, _) = three_leaf_tree(false);
            assert!(delete(&mut store, &mut root, key).expect("delete"));
            let report = store.merge_report;
            assert_eq!(
                (report.leaf_left, report.leaf_right, report.root),
                (lefts, 2 - lefts, true)
            );
        }
        let cases =
            ["flipped byte", "stale txn", "older copy", "redirect", "private", "this txn's run"];
        for (side, case) in
            [Side::Left, Side::Right].into_iter().flat_map(|s| cases.map(|c| (s, c)))
        {
            // A page private to this transaction must be named by a cell this writer stamped:
            // the copy-time rule finds it first in a page read from disk.
            let (mut store, mut root, leaves) = three_leaf_tree(case == "private");
            // The key deleted, and the sibling's cell in the root.
            let (key, si): (&[u8], usize) = match side {
                Side::Left => (b"n", 0),
                Side::Right => (b"b", 1),
            };
            if case == "older copy" {
                // A rewrite of one of the sibling's values copies its page; the old one is retired.
                let k: &[u8] = [b"a", b"m"][si];
                put(&mut store, &mut root, k, &[0x5A; 8]).expect("put");
                store.reopen();
            }
            let r = root.root;
            let (sib, sib_txn) = page::branch_child(store.raw(r), si).expect("child");
            let txn32 = store.txn() as u32;
            let mut other = TreeRoot::EMPTY;
            let big = vec![9; 3 * 4096];
            let msg = match case {
                "flipped byte" => {
                    let p = store.geometry().page_size();
                    store.raw_mut(sib)[p - 5] ^= 1;
                    "fails its CRC"
                }
                "stale txn" => {
                    set_child_restamped(&mut store, r, si, sib, sib_txn.wrapping_sub(1));
                    "is not the copy its parent names"
                }
                "older copy" => {
                    assert_ne!(leaves[si], sib, "a new copy");
                    set_child_restamped(&mut store, r, si, leaves[si], sib_txn);
                    "is not the copy its parent names"
                }
                "redirect" => {
                    let (c, c_txn) = page::branch_child(store.raw(r), 2).expect("child");
                    set_child_restamped(&mut store, r, si, c, c_txn);
                    "outside its parents' separators"
                }
                "private" => {
                    put(&mut store, &mut other, b"other", b"tree").expect("put");
                    assert!(store.is_private(other.root));
                    set_child_restamped(&mut store, r, si, other.root, txn32);
                    "named by a page it did not write"
                }
                _ => {
                    put(&mut store, &mut other, b"other", &big).expect("put");
                    let x = run_of(&store, &other, b"other");
                    let fit = page::LeafMut::new(store.raw_mut(sib))
                        .expect("leaf")
                        .replace_value(0, Val::Overflow(x))
                        .expect("forge");
                    assert_eq!(fit, Fit::Done);
                    store.restamp(sib);
                    root.overflow_pages = overflow::run_pages(x.len as usize, 4096) as u32;
                    "with this txn's id"
                }
            };
            assert_delete_refused(&mut store, &mut root, key, msg);
            if case == "this txn's run" {
                let x = run_of(&store, &other, b"other");
                assert!(
                    store.alloc.is_private_run(x.pgno),
                    "{side:?}: the other tree keeps its run"
                );
                assert_eq!(get(&store, &other, b"other").expect("get"), Some(big.as_slice()));
            }
        }
    }

    /// The branch twin of [`a_merge_refuses_a_damaged_leaf_sibling`], whose merges an unlink
    /// starts: a flipped child txn byte under a stale CRC, a stale txn in the parent's cell, a
    /// redirect to a real branch outside the cell's separators, a page private to this
    /// transaction, and a cell of the sibling naming a page this transaction allocated are each
    /// `Corrupt`. So is a right page of the pair (the sibling, or the page on the path when the
    /// sibling is on its left) whose key 1 equals the parent's key coming down: the range check
    /// passes it (a branch's key 1 may equal its lower bound, as cell 1 of a branch may start
    /// where its parent's cell does), and the merged page would hold that key twice. On the path
    /// only the seam check finds it; in the sibling, the descent of its child-0 chain
    /// (`check_sibling_chain`) finds it first, since child 0's range is then empty. Undamaged,
    /// each delete merges the sibling (then the third branch, and the root gives way).
    #[test]
    fn a_merge_refuses_a_damaged_branch_sibling() {
        for key in [b"s", b"e"] {
            let (mut store, mut root, _) = three_branch_tree(false);
            assert!(delete(&mut store, &mut root, key).expect("delete"));
            let report = store.merge_report;
            assert_eq!((report.branches, report.levels, report.root), (2, 1, true), "{report:?}");
        }
        let cases = ["flipped byte", "stale txn", "redirect", "private", "this txn's page", "seam"];
        for (side, case) in
            [Side::Left, Side::Right].into_iter().flat_map(|s| cases.map(|c| (s, c)))
        {
            let (mut store, mut root, branches) = three_branch_tree(case == "private");
            let (key, si): (&[u8], usize) = match side {
                Side::Left => (b"s", 0),
                Side::Right => (b"e", 1),
            };
            let r = root.root;
            let (sib, sib_txn) = page::branch_child(store.raw(r), si).expect("child");
            let txn32 = store.txn() as u32;
            let mut other = TreeRoot::EMPTY;
            let msg = match case {
                "flipped byte" => {
                    // Cell 0 lies at the page's end: its child txn's low byte.
                    let p = store.geometry().page_size();
                    store.raw_mut(sib)[p - 10] ^= 0x40;
                    "fails its CRC"
                }
                "stale txn" => {
                    set_child_restamped(&mut store, r, si, sib, sib_txn.wrapping_sub(1));
                    "is not the copy its parent names"
                }
                "redirect" => {
                    let (c, c_txn) = page::branch_child(store.raw(r), 2).expect("child");
                    set_child_restamped(&mut store, r, si, c, c_txn);
                    "outside its parents' separators"
                }
                "private" => {
                    put(&mut store, &mut other, b"other", b"tree").expect("put");
                    set_child_restamped(&mut store, r, si, other.root, txn32);
                    "named by a page it did not write"
                }
                "this txn's page" => {
                    put(&mut store, &mut other, b"other", b"tree").expect("put");
                    set_child_restamped(&mut store, sib, 1, other.root, txn32);
                    "with this txn's id"
                }
                _ => {
                    // `B1`'s key 1, `p`, becomes `m`: the root's key for it.
                    let b1 = store.raw_mut(branches[1]);
                    let slot = page::HEADER + page::SLOT;
                    let cell = usize::from(u16::from_le_bytes([b1[slot], b1[slot + 1]]));
                    assert_eq!(&b1[cell + 8..cell + 11], &[1, 0, b'p'], "klen 1, key p");
                    b1[cell + 10] = b'm';
                    store.restamp(branches[1]);
                    match side {
                        Side::Left => "would bring its parent's key down out of order",
                        Side::Right => "holds keys outside its parents' separators",
                    }
                }
            };
            assert_delete_refused(&mut store, &mut root, key, msg);
        }
    }

    /// [`three_branch_tree`], reopened, with a single-child branch `T` over leaf `[x, y]` built
    /// beside it (named by nothing); with `T`.
    fn single_child_beside() -> (MemStore, TreeRoot, u32) {
        let mut store = MemStore::new(4096);
        let b0 = branch_of(&mut store, &[&[b"a", b"b"], &[b"c", b"d"], &[b"e"]]);
        let b1 = branch_of(&mut store, &[&[b"m", b"n"], &[b"p", b"q"], &[b"s"]]);
        let b2 = branch_of(&mut store, &[&[b"t", b"u"], &[b"v", b"w"]]);
        let r =
            branch_page(&mut store, &[(Vec::new(), b0), (b"m".to_vec(), b1), (b"t".to_vec(), b2)]);
        let t = branch_of(&mut store, &[&[b"x", b"y"]]);
        let root = built_root(&store, r, 3, 14);
        publish(&mut store, false);
        (store, root, t)
    }

    /// Point cell `i` of `root`'s root page at the single-child branch `t`, its identity and all
    /// (CRC stamped again); returns `t`'s leaf.
    fn redirect_to(store: &mut MemStore, root: &TreeRoot, i: usize, t: u32) -> u32 {
        let (leaf, _) = page::branch_child(store.raw(t), 0).expect("child");
        let t_txn = page::txn(store.raw(t)) as u32;
        set_child_restamped(store, root.root, i, t, t_txn);
        leaf
    }

    /// A root cell redirected, identity and all, to a single-child branch `T` over `[x, y]`,
    /// outside the cell's separators, from either side: `T`'s one cell has no key to range-check,
    /// so only the descent of its child-0 chain to the leaves (`check_sibling_chain`) finds the
    /// leaf outside the range. `Corrupt`, and the delete changes nothing; undamaged, each delete
    /// merges two branches and the root gives way.
    #[test]
    fn a_merge_refuses_a_single_child_branch_outside_the_cell() {
        for key in [b"s", b"e"] {
            let (mut store, mut root, _) = single_child_beside();
            assert!(delete(&mut store, &mut root, key).expect("delete"));
            let r = store.merge_report;
            assert_eq!((r.branches, r.levels, r.root), (2, 1, true), "{r:?}");
        }
        for (key, si) in [(b"s", 0), (b"e", 1)] {
            let (mut store, mut root, t) = single_child_beside();
            let leaf = redirect_to(&mut store, &root, si, t);
            let msg = format!("page {leaf} holds keys outside its parents' separators");
            assert_delete_refused(&mut store, &mut root, key, &msg);
        }
    }

    /// A reopened four-level tree: `B0` over `D0` (`[a]`, `[b]`) and `D1` (`[c]`, `[d]`), `B1` over
    /// `D2` (`[m]`, `[n]`) and `D3` (`[p]`, `[s]`), `B2` over `D4` (`[t]`, `[u]`) and `D5` (`[v]`,
    /// `[w]`), under separators `m` and `t`. A delete of `s` merges `D3` with `D2`, and then `B1`
    /// absorbs `B0` and `B2`; a delete of `d` merges `D1` with `D0`, and then `B0` absorbs `B1` and
    /// `B2`.
    fn four_level_tree() -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut cells = Vec::new();
        for (lo, pair) in
            [(&b""[..], [b"ab", b"cd"]), (b"m", [b"mn", b"ps"]), (b"t", [b"tu", b"vw"])]
        {
            let d: Vec<u32> =
                pair.iter().map(|ks| branch_of(&mut store, &[&[&ks[..1]], &[&ks[1..]]])).collect();
            let b = branch_page(&mut store, &[(Vec::new(), d[0]), (pair[1][..1].to_vec(), d[1])]);
            cells.push((lo.to_vec(), b));
        }
        let r = branch_page(&mut store, &cells);
        let root = built_root(&store, r, 4, 12);
        publish(&mut store, false);
        (store, root)
    }

    /// In a four-level tree, a root cell redirected, identity and all, to a branch of the level
    /// below whose keys lie inside the cell's separators (`D0` for `B0`'s cell, `D2` for `B1`'s),
    /// from either side: `sibling_view` tells a leaf from a branch, not one level from another,
    /// and the range check passes, so only the descent of its child-0 chain finds a leaf above
    /// the leaves' level. `Corrupt`, and the delete changes nothing.
    #[test]
    fn a_merge_refuses_a_branch_sibling_of_another_level() {
        for key in [b"s", b"d"] {
            let (mut store, mut root) = four_level_tree();
            assert!(delete(&mut store, &mut root, key).expect("delete"));
            let r = store.merge_report;
            assert_eq!((r.branches, r.levels, r.root), (3, 2, true), "{r:?}");
        }
        for (key, si) in [(b"s", 0), (b"d", 1)] {
            let (mut store, mut root) = four_level_tree();
            let (b, _) = page::branch_child(store.raw(root.root), si).expect("child");
            let (d, d_txn) = page::branch_child(store.raw(b), 0).expect("child");
            set_child_restamped(&mut store, root.root, si, d, d_txn);
            assert_delete_refused(
                &mut store,
                &mut root,
                key,
                "Leaf at depth 3 of a tree of height 4",
            );
        }
    }

    /// A reopened root `(B0, T, B1, B2)` under separators `f`, `m`, `t`, where `T` is a
    /// single-child branch over `[g, h]`; with `T`. A delete of `s` leaves `B1` far under a
    /// quarter, so it absorbs `T`, `B0` and `B2`, and the root gives way.
    fn root_with_single_child() -> (MemStore, TreeRoot, u32) {
        let mut store = MemStore::new(4096);
        let b0 = branch_of(&mut store, &[&[b"a", b"b"], &[b"c", b"d"]]);
        let t = branch_of(&mut store, &[&[b"g", b"h"]]);
        let b1 = branch_of(&mut store, &[&[b"m", b"n"], &[b"p", b"q"], &[b"s"]]);
        let b2 = branch_of(&mut store, &[&[b"t", b"u"], &[b"v", b"w"]]);
        let cells =
            [(Vec::new(), b0), (b"f".to_vec(), t), (b"m".to_vec(), b1), (b"t".to_vec(), b2)];
        let r = branch_page(&mut store, &cells);
        let root = built_root(&store, r, 3, 15);
        publish(&mut store, false);
        (store, root, t)
    }

    /// A root naming one single-child branch `T` by two cells (`T`'s own, and the cell before it
    /// redirected to `T`, identity and all): the merge absorbs `T` through its own cell, then
    /// meets `T` again through the other, whose range `T`'s leaf `[g, h]` lies outside, so the
    /// descent of `T`'s child-0 chain refuses it and `T` is never absorbed, and freed, twice.
    /// `Corrupt`, and the delete changes nothing.
    #[test]
    fn a_merge_refuses_a_branch_named_by_two_cells() {
        let (mut store, mut root, _) = root_with_single_child();
        assert!(delete(&mut store, &mut root, b"s").expect("delete"));
        let r = store.merge_report;
        assert_eq!((r.branches, r.levels, r.root), (3, 1, true), "{r:?}");
        let (mut store, mut root, t) = root_with_single_child();
        let leaf = redirect_to(&mut store, &root, 0, t);
        let msg = format!("page {leaf} holds keys outside its parents' separators");
        assert_delete_refused(&mut store, &mut root, b"s", &msg);
    }

    /// A neighbour read for a merge and refused (too full to absorb) still gets `sibling_view`'s
    /// cheap checks, made before the fit test: a stale txn in the parent's cell, a leaf whose
    /// header counts no cell, or a parent cell naming a page private to this transaction is
    /// `Corrupt` and the delete changes nothing. Damage only the CRC finds (a flipped value byte)
    /// is not looked for in a refused neighbour, which is read no further than its header: the
    /// delete succeeds without using it (the neighbour stays as it was), and the damage is found
    /// at its first real use, a delete that copies it.
    #[test]
    fn a_refused_neighbour_gets_only_the_cheap_checks() {
        // Four entries of the largest inline value fill a leaf: nothing fits beside it.
        let full: Vec<(&[u8], usize)> =
            [b"a", b"b", b"c", b"d"].iter().map(|k| (&k[..], 1006)).collect();
        for case in ["flipped byte", "stale txn", "empty", "private"] {
            let mut store = MemStore::new(4096);
            assert_eq!(store.geometry().max_value(1), 1006);
            let a = leaf_with(&mut store, &full);
            let m = leaf_of(&mut store, &[b"m", b"n"]);
            let r = branch_page(&mut store, &[(Vec::new(), a), (b"m".to_vec(), m)]);
            let mut root = built_root(&store, r, 2, 6);
            publish(&mut store, case == "private");
            assert_eq!(page::free_space(store.raw(a)).expect("free"), 0, "the left leaf is full");
            let (_, a_txn) = page::branch_child(store.raw(r), 0).expect("child");
            match case {
                "flipped byte" => {
                    let p = store.geometry().page_size();
                    store.raw_mut(a)[p - 5] ^= 1;
                    let before = store.raw(a).to_vec();
                    assert!(delete(&mut store, &mut root, b"n").expect("delete"));
                    let report = store.merge_report;
                    assert!(report.refused && report.levels == 0, "refused, no merge: {report:?}");
                    assert_eq!(store.raw(a), &before[..], "the neighbour is unchanged");
                    assert!(
                        !store.alloc.freed().contains(&a),
                        "the neighbour is still in the tree"
                    );
                    let used = delete(&mut store, &mut root, b"a");
                    let Err(Error::Corrupt(msg)) = &used else { panic!("{used:?}") };
                    assert!(msg.contains("fails its CRC"), "{msg}");
                }
                "stale txn" => {
                    set_child_restamped(&mut store, r, 0, a, a_txn.wrapping_sub(1));
                    assert_delete_refused(&mut store, &mut root, b"n", "is not the copy");
                }
                "empty" => {
                    store.raw_mut(a)[6..8].copy_from_slice(&0_u16.to_le_bytes());
                    assert_delete_refused(&mut store, &mut root, b"n", "is empty");
                }
                _ => {
                    // A full leaf private to this transaction, the root of another tree.
                    let mut other = TreeRoot::EMPTY;
                    for (k, len) in &full {
                        put(&mut store, &mut other, k, &vec![1; *len]).expect("put");
                    }
                    assert_eq!(page::free_space(store.raw(other.root)).expect("free"), 0);
                    let txn32 = store.txn() as u32;
                    set_child_restamped(&mut store, r, 0, other.root, txn32);
                    assert_delete_refused(&mut store, &mut root, b"n", "did not write");
                }
            }
        }
    }

    /// A delete checks the handle's entry count against the tree before changing anything: a
    /// count of one for a tree with more entries, or of more than one for the tree's last entry,
    /// is `Corrupt` (the count is kept exact, never wrapped).
    #[test]
    fn a_delete_refuses_a_wrong_entry_count() {
        let (mut store, root, _) = two_leaf_tree(20);
        let mut handle = TreeRoot { entries: 1, ..root };
        let deleted = delete(&mut store, &mut handle, &id_key(3, 200));
        assert!(matches!(deleted, Err(Error::Corrupt(_))), "{deleted:?}");
        assert_eq!(handle, TreeRoot { entries: 1, ..root });

        let mut handle = TreeRoot::EMPTY;
        put(&mut store, &mut handle, b"only", b"one").expect("put");
        handle.entries = 2;
        let deleted = delete(&mut store, &mut handle, b"only");
        assert!(matches!(deleted, Err(Error::Corrupt(_))), "{deleted:?}");
        assert_eq!(handle.entries, 2);
        handle.entries = 1;
        assert!(delete(&mut store, &mut handle, b"only").expect("delete"));
        assert_eq!(handle, TreeRoot::EMPTY);
    }

    /// A delete checks the handle it leaves before changing anything: a two-leaf tree whose
    /// handle counts one leaf or one branch too many would collapse to a one-leaf tree counting
    /// two leaves, or a branch, which is `Corrupt`; the handle, the pages and the free lists stay
    /// as they were. With the counts right, the collapse leaves exactly one leaf. Merging is off
    /// (`MemStore::no_merge`), so the deletes reach the collapse rather than a merge.
    #[test]
    fn a_delete_refuses_a_handle_miscounting_its_pages() {
        for miscount in 0..3 {
            let (mut store, root, right) = two_leaf_tree(20);
            store.no_merge = true;
            assert_eq!((root.branch_pages, root.leaf_pages), (1, 2));
            let mut handle = match miscount {
                0 => TreeRoot { leaf_pages: 3, ..root },
                1 => TreeRoot { branch_pages: 2, ..root },
                _ => root,
            };
            let left: Vec<Vec<u8>> =
                (0..20).map(|i| id_key(i, 200)).filter(|k| k < &right_key(&store, right)).collect();
            let (last, rest) = left.split_last().expect("left keys");
            for k in rest {
                assert!(delete(&mut store, &mut handle, k).expect("delete"));
            }
            let before = (handle, store.reachable(&handle), store.alloc.freed().to_vec());
            let pool = store.alloc.pool().to_vec();
            let deleted = delete(&mut store, &mut handle, last);
            if miscount == 2 {
                assert!(deleted.expect("delete"), "the collapse");
                assert_eq!((handle.height, handle.branch_pages, handle.leaf_pages), (1, 0, 1));
                walk_tree(&store, &handle);
                continue;
            }
            assert!(matches!(deleted, Err(Error::Corrupt(_))), "miscount {miscount}: {deleted:?}");
            assert_eq!(handle, before.0, "miscount {miscount}: the handle changed");
            assert!(store.reachable(&handle) == before.1, "miscount {miscount}: a page changed");
            assert_eq!(store.alloc.freed(), before.2, "miscount {miscount}: pages were freed");
            assert_eq!(store.alloc.pool(), pool, "miscount {miscount}: pages were pooled");
        }
    }

    /// A clear frees exactly the tree's pages: published ones to the freed list, private ones
    /// straight back to the pool; the root becomes empty and the tree refills.
    #[test]
    fn clear_frees_every_page_of_the_tree() {
        let (mut store, mut root) = tree_with_strays(600);
        let mut tree: Vec<u32> = store.reachable(&root).into_iter().map(|(p, _)| p).collect();
        tree.sort_unstable();
        clear(&mut store, &mut root).expect("clear");
        assert_eq!(root, TreeRoot::EMPTY);
        let mut freed = store.alloc.freed().to_vec();
        freed.sort_unstable();
        assert_eq!(freed, tree, "every published page freed, once");
        assert!(store.alloc.pool().is_empty());

        // A tree built in this transaction: its pages are loose, reusable at once.
        let freed = freed.len();
        for i in 0..600 {
            put(&mut store, &mut root, &id_key(i, 200), b"again").expect("put");
        }
        let private = store.reachable(&root).len();
        assert!(private > 20, "{private} pages");
        let pool = store.alloc.pool().len();
        clear(&mut store, &mut root).expect("clear");
        assert_eq!(root, TreeRoot::EMPTY);
        assert_eq!(store.alloc.freed().len(), freed, "no published page among them");
        assert_eq!(store.alloc.pool().len(), pool + private, "the private pages are pooled");
        store.commit_like();
        store.assert_accounting(&[root]);
        assert!(clear(&mut store, &mut root).is_ok(), "clearing the empty tree");
    }

    /// Fill a tree with random keys (no ascending run, so no append splits), then delete 90% of
    /// them at random: with merging, every non-root page under a quarter of `U` fits into neither
    /// same-parent neighbour (`merge_open_pages` with no `before`), and the fill stays far above
    /// what the same deletes leave without merging. Returns the fill (all pages, leaves) after the
    /// fill and after the deletes, and the page count after.
    fn fill_then_delete_90(seed: u64, n: usize, merge: bool) -> ([f64; 4], usize, Vec<u32>) {
        let _seed = ReportSeed(seed);
        let mut rng = Rng(seed);
        let mut store = MemStore::new(4096);
        store.no_merge = !merge;
        let mut root = TreeRoot::EMPTY;
        let mut keys = Vec::new();
        while keys.len() < n {
            let k: Vec<u8> = (0..8 + rng.len_upto(24)).map(|_| rng.next() as u8).collect();
            let v: Vec<u8> = (0..rng.len_upto(160)).map(|_| rng.next() as u8).collect();
            if !put(&mut store, &mut root, &k, &v).expect("put") {
                keys.push(k);
            }
            if keys.len() % 500 == 0 {
                store.commit_like();
                store.recycle();
            }
        }
        let before = [page_fill(&store, &root, false), page_fill(&store, &root, true)];
        for i in (1..keys.len()).rev() {
            keys.swap(i, rng.below(i as u64 + 1) as usize);
        }
        for (j, k) in keys.iter().take(n * 9 / 10).enumerate() {
            assert!(delete(&mut store, &mut root, k).expect("delete"));
            if j % 500 == 0 {
                store.commit_like();
                store.recycle();
            }
        }
        let walk = walk_tree(&store, &root);
        assert_eq!(walk.entries.len(), n - n * 9 / 10);
        let after = [page_fill(&store, &root, false), page_fill(&store, &root, true)];
        store.commit_like();
        store.assert_accounting(&[root]);
        let open = merge_open_pages(&store, &root, &[]);
        ([before[0], before[1], after[0], after[1]], walk.pages.len(), open)
    }

    /// The fill bound: after random fills each followed by deleting 90% of the keys at random,
    /// no non-root page under a quarter of `U` fits with a neighbour, and the pages stay well
    /// filled (about 0.69 after the fill, 0.40–0.47 after the deletes at 4 KiB pages; without
    /// merging the same deletes leave about 0.08 in six times the pages).
    #[test]
    fn deleting_90_percent_leaves_no_page_merge_open() {
        for seed in 1..=3_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let (fill, pages, open) = fill_then_delete_90(seed, 8_000, true);
            let (fill_off, pages_off, _) = fill_then_delete_90(seed, 8_000, false);
            eprintln!(
                "fill (pages, leaves) before {:.3} {:.3}, after {:.3} {:.3} in {pages} pages; \
                 without merging {:.3} {:.3} in {pages_off} pages",
                fill[0], fill[1], fill[2], fill[3], fill_off[2], fill_off[3]
            );
            assert!(open.is_empty(), "seed {seed:#x}: pages {open:?} are merge-open");
            assert!(fill[3] > 0.3 && fill[3] > 3.0 * fill_off[3], "seed {seed:#x}: {fill:?}");
        }
    }

    /// One randomized run of [`random_batches`].
    #[derive(Clone, Copy, Debug)]
    struct BatchRun {
        page_size: u32,
        /// Random keys are drawn from ids `0..key_space`; ascending puts go above it, descending
        /// ones down from four times it.
        key_space: u64,
        rounds: usize,
        /// Rounds per phase: phases alternate between growing (mostly puts) and shrinking
        /// (mostly deletes), so the tree deepens, empties out and refills.
        phase: usize,
        seed: u64,
        /// The height the tree must reach.
        min_height: u32,
        /// Whether deletes merge under-full pages (else `MemStore::no_merge`).
        merge: bool,
        /// With `merge`, whether every merge shape must run (the coverage gate). Large pages
        /// rarely refuse a merge, and a small run there stays two levels high.
        gate_merges: bool,
    }

    /// How often each operation and tree change ran: every one must have run.
    #[derive(Debug, Default)]
    struct Coverage {
        inserts: usize,
        replaces: usize,
        deletes: usize,
        missed_deletes: usize,
        clears: usize,
        splits: usize,
        unlinks: usize,
        collapses: usize,
        empties: usize,
        refills: usize,
        aborts: usize,
        held: usize,
        dropped_out_of_order: usize,
        reopens: usize,
        inline_to_overflow: usize,
        overflow_to_inline: usize,
        overflow_to_overflow: usize,
        overflow_deletes: usize,
        runs_from_the_pool: usize,
        runs_from_the_file_end: usize,
        leaf_merges_left: usize,
        leaf_merges_right: usize,
        branch_merges: usize,
        merge_collapses: usize,
        refused_merges: usize,
        cascades: usize,
    }

    impl Coverage {
        /// The operations and changes that never ran. With `merge` a leaf merges before it empties,
        /// so unlinks are rare and not required; `gate` requires every merge shape.
        fn missing(&self, merge: bool, gate: bool) -> Vec<&'static str> {
            let Self {
                inserts,
                replaces,
                deletes,
                missed_deletes,
                clears,
                splits,
                unlinks,
                collapses,
                empties,
                refills,
                aborts,
                held,
                dropped_out_of_order,
                reopens,
                inline_to_overflow,
                overflow_to_inline,
                overflow_to_overflow,
                overflow_deletes,
                runs_from_the_pool,
                runs_from_the_file_end,
                leaf_merges_left,
                leaf_merges_right,
                branch_merges,
                merge_collapses,
                refused_merges,
                cascades,
            } = *self;
            let merges = [
                ("leaf_merges_left", leaf_merges_left),
                ("leaf_merges_right", leaf_merges_right),
                ("branch_merges", branch_merges),
                ("merge_collapses", merge_collapses),
                ("refused_merges", refused_merges),
                ("cascades", cascades),
            ];
            [
                ("inserts", inserts),
                ("replaces", replaces),
                ("deletes", deletes),
                ("missed_deletes", missed_deletes),
                ("clears", clears),
                ("splits", splits),
                ("unlinks", if merge { 1 } else { unlinks }),
                ("collapses", collapses),
                ("empties", empties),
                ("refills", refills),
                ("aborts", aborts),
                ("held", held),
                ("dropped_out_of_order", dropped_out_of_order),
                ("reopens", reopens),
                ("inline_to_overflow", inline_to_overflow),
                ("overflow_to_inline", overflow_to_inline),
                ("overflow_to_overflow", overflow_to_overflow),
                ("overflow_deletes", overflow_deletes),
                ("runs_from_the_pool", runs_from_the_pool),
                ("runs_from_the_file_end", runs_from_the_file_end),
            ]
            .into_iter()
            .chain(merges.into_iter().filter(|_| gate))
            .filter_map(|(what, n)| (n == 0).then_some(what))
            .collect()
        }
    }

    /// The key of `id`: its 8 big-endian bytes (so keys sort by id), padded to a length fixed by
    /// the id, mostly short, sometimes up to the key limit.
    fn batch_key(id: u64, g: &Geometry) -> Vec<u8> {
        let h = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 8;
        let len = match h % 10 {
            0 => g.max_key() - (h >> 8) as usize % (g.max_key() / 2),
            1 | 2 => 8 + (h >> 8) as usize % (g.max_key() / 8),
            _ => 8 + (h >> 8) as usize % 24,
        };
        let mut k = vec![(h >> 40) as u8; len];
        k[..8].copy_from_slice(&id.to_be_bytes());
        k
    }

    /// A value for one of the few keys [`random_batches`] gives large values: a third of the time
    /// inline ([`batch_value`]), a third in a one-page overflow run, a third in a run of 2 to 10
    /// pages (inline whenever the key is too long for an overflow cell).
    fn large_batch_value(rng: &mut Rng, g: &Geometry, key: &[u8]) -> Vec<u8> {
        let (max, p) = (g.max_value(key.len()), g.page_size());
        let one_page = p - overflow::RUN_HEADER - page::TRAILER;
        let vlen = match rng.below(3) {
            _ if g.max_table_value(key.len()) == max => return batch_value(rng, g, key),
            0 => return batch_value(rng, g, key),
            1 => max + 1 + rng.len_upto(one_page - max - 1),
            _ => {
                let n = 2 + rng.below(9) as usize;
                (n - 1) * p + 1 - overflow::RUN_HEADER - page::TRAILER + rng.len_upto(p - 1)
            }
        };
        let seed = rng.next();
        (0..vlen).map(|i| (seed.wrapping_add(i as u64 * 0x9E37) >> 8) as u8).collect()
    }

    /// A value for `key`: mostly short, sometimes up to the limit for its key.
    fn batch_value(rng: &mut Rng, g: &Geometry, key: &[u8]) -> Vec<u8> {
        let max = g.max_value(key.len());
        let vlen = match rng.below(10) {
            0 => max - rng.len_upto(max.min(16)),
            1 | 2 => rng.len_upto(max / 4),
            _ => rng.len_upto(max.min(64)),
        };
        (0..vlen).map(|_| rng.next() as u8).collect()
    }

    /// Writer-shaped batches against a `BTreeMap` model. Each round is a batch of random,
    /// ascending and descending puts (inserts and replaces, sizes up to the limits), deletes (the
    /// smallest, largest or a random present key, or a likely miss) and clears; one in four
    /// batches has an allocation failure injected, and the batch then aborts (the private pages
    /// dropped, the published root restored), the others commit. After every operation the tree
    /// passes [`walk_tree`] and holds exactly the model, and a failed operation changed nothing.
    /// After every commit the page accounting is exact and a reader of the commit sees the model.
    /// A few keys (16 at 4 KiB pages, fewer at larger ones, so a walk reads a bounded volume)
    /// take large values ([`large_batch_value`]): inline, or in overflow runs of 1 to 10 pages,
    /// which every change above allocates, replaces, frees and reuses.
    /// Reader snapshots are held and dropped out of order; freed pages are reused once no held
    /// snapshot can reach them, and every held snapshot keeps reading the state it saw (a page
    /// reused too early is `Corrupt` to it). Reopens (stamped pages forgotten, readers gone) come
    /// at random and at the end, followed by writes. A coverage gate proves each tree change ran:
    /// splits, unlinks, collapses, empties and refills; replaces from inline to overflow, back
    /// and between runs; overflow deletes; and runs taken from the pool and from the file's end.
    ///
    /// With `merge` (else `MemStore::no_merge`), after every delete the pages it changed are
    /// merge-closed (`merge_open_pages`), and the gate adds leaves merged from the left and from
    /// the right, branch merges, a root giving way after merges, a refused merge (a sibling that
    /// does not fit) and a cascade over two levels or more. Without it, no delete merges.
    fn random_batches(run: BatchRun) {
        let deletes_before = cached_deletes();
        let _seed = ReportSeed(run.seed);
        let BatchRun { page_size, key_space, rounds, phase, seed, min_height, merge, gate_merges } =
            run;
        let mut rng = Rng(seed);
        let mut store = MemStore::new(page_size);
        store.no_merge = !merge;
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut published = (model.clone(), root);
        // Held reader snapshots: the committed txn, the root and the model at that commit.
        let mut held = Vec::<(u64, TreeRoot, BTreeMap<Vec<u8>, Vec<u8>>)>::new();
        let (mut ascending, mut descending) = (key_space, key_space * 4);
        let (mut max_height, mut emptied) = (0, false);
        let mut seen = Coverage::default();
        let mut pages = 0;
        // The ids of the keys that take large values.
        let large = (16 * 4096 / u64::from(page_size)).max(4);
        let large_id = |i: u64| i * (key_space / large);
        let mut large_rng = Rng(seed ^ 0x5DEE_CE66_D1CE_4E5B);
        for round in 0..rounds {
            let ops = 1 + rng.below(12);
            let inject = rng.below(4) == 0;
            if inject {
                store.alloc.fail_allocs_after = Some(rng.below(6));
            }
            let grow = (round / phase) % 2 == 0;
            let mut failed = false;
            // Every other batch writes through a path cache, as a table does; a batch is one
            // transaction, so the cache starts empty.
            let mut cache = PathCache::default();
            let cached = round % 2 == 1;
            for op in 0..ops {
                let r = rng.below(100);
                let (put_below, delete_below) = if grow { (85, 99) } else { (5, 100) };
                let height = root.height;
                let was_empty = root.is_empty();
                let what = format!("round {round} op {op}");
                // The pages the op's merges took out of the tree.
                let mut merged = 0;
                let result = if r < put_below {
                    let id = match rng.below(10) {
                        0 | 1 => {
                            ascending += 1;
                            ascending
                        }
                        2 => {
                            descending = descending.saturating_sub(1);
                            descending
                        }
                        _ => rng.below(key_space),
                    };
                    let k = batch_key(id, &g);
                    let v = batch_value(&mut rng, &g, &k);
                    // One put in 20 goes to a large key instead, drawn from its own stream.
                    let (k, v) = if large_rng.below(20) == 0 {
                        let k = batch_key(large_id(large_rng.below(large)), &g);
                        let v = large_batch_value(&mut large_rng, &g, &k);
                        (k, v)
                    } else {
                        (k, v)
                    };
                    let is_run = |v: &[u8], k: &[u8]| v.len() > g.max_value(k.len());
                    let old_run = model.get(&k).map(|old| is_run(old, &k));
                    let new_run = is_run(&v, &k);
                    let count = store.alloc.page_count();
                    let cache = cached.then_some(&mut cache);
                    put_through(&mut store, &mut root, cache, &k, &v).map(|replaced| {
                        if new_run {
                            let Some(Val::Overflow(ovf)) = find(&store, &root, &k).expect("find")
                            else {
                                panic!("{what}: a large value is not in a run");
                            };
                            if ovf.pgno >= count {
                                seen.runs_from_the_file_end += 1;
                            } else {
                                seen.runs_from_the_pool += 1;
                            }
                        }
                        match (old_run, new_run) {
                            (Some(false), true) => seen.inline_to_overflow += 1,
                            (Some(true), false) => seen.overflow_to_inline += 1,
                            (Some(true), true) => seen.overflow_to_overflow += 1,
                            _ => {}
                        }
                        assert_eq!(replaced, model.insert(k, v).is_some(), "{what}: put");
                        if replaced {
                            seen.replaces += 1;
                        } else {
                            seen.inserts += 1;
                        }
                        if was_empty && emptied {
                            seen.refills += 1;
                            emptied = false;
                        }
                    })
                } else if r < delete_below || rng.below(4) != 0 {
                    let k = match rng.below(8) {
                        0 => model.keys().next().cloned(),
                        1 => model.keys().next_back().cloned(),
                        2 => None,
                        _ if !model.is_empty() => {
                            model.keys().nth(rng.below(model.len() as u64) as usize).cloned()
                        }
                        _ => None,
                    }
                    .unwrap_or_else(|| batch_key(rng.below(key_space), &g));
                    let hit = model.contains_key(&k);
                    let keys_before =
                        if merge && hit { level_keys(&store, &root) } else { Vec::new() };
                    store.merge_report = MergeReport::default();
                    let cache = cached.then_some(&mut cache);
                    delete_through(&mut store, &mut root, cache, &k).map(|removed| {
                        let old = model.remove(&k);
                        assert_eq!(removed, old.is_some(), "{what}: delete");
                        let Some(old) = old else {
                            seen.missed_deletes += 1;
                            return;
                        };
                        let report = store.merge_report;
                        assert!(merge || report == MergeReport::default(), "{what}: {report:?}");
                        if merge {
                            let open = merge_open_pages(&store, &root, &keys_before);
                            assert!(open.is_empty(), "{what}: pages {open:?} merge-open");
                        }
                        seen.leaf_merges_left += report.leaf_left;
                        seen.leaf_merges_right += report.leaf_right;
                        seen.branch_merges += report.branches;
                        seen.merge_collapses += usize::from(report.root);
                        seen.refused_merges += usize::from(report.refused);
                        seen.cascades += usize::from(report.levels >= 2);
                        merged = report.leaf_left + report.leaf_right + report.branches;
                        merged += usize::from(report.root);
                        seen.deletes += 1;
                        seen.overflow_deletes += usize::from(old.len() > g.max_value(k.len()));
                        if root.is_empty() {
                            seen.empties += 1;
                            emptied = true;
                        } else if root.height < height {
                            seen.collapses += 1;
                        }
                    })
                } else {
                    if !was_empty {
                        seen.clears += 1;
                        emptied = true;
                    }
                    clear(&mut store, &mut root).map(|()| model.clear())
                };
                let walk = walk_tree(&store, &root);
                assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "{what}");
                match result {
                    Ok(()) => {}
                    Err(Error::Io(_)) if inject => {
                        assert_eq!(walk.pages.len(), pages, "{what}: a failed op changed the tree");
                        failed = true;
                        break;
                    }
                    Err(e) => panic!("{what}: {e}"),
                }
                if walk.pages.len() > pages && r < put_below {
                    seen.splits += usize::from(!was_empty && root.height >= height);
                } else if walk.pages.len() + merged < pages && !root.is_empty() && r >= put_below {
                    seen.unlinks += 1;
                }
                pages = walk.pages.len();
                max_height = max_height.max(root.height);
            }
            store.alloc.fail_allocs_after = None;
            if failed {
                seen.aborts += 1;
                store.abort_like();
                (model, root) = (published.0.clone(), published.1);
                pages = walk_tree(&store, &root).pages.len();
                store.assert_accounting(&[root]);
                continue;
            }
            store.commit_like();
            store.assert_accounting(&[root]);
            let reader = store.reader();
            let walk = walk_tree(&reader, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "round {round}");
            published = (model.clone(), root);
            if rng.below(3) == 0 {
                seen.held += 1;
                held.push((store.alloc.txn() - 1, root, model.clone()));
            }
            if rng.below(4) == 0 && !held.is_empty() {
                let i = rng.below(held.len() as u64) as usize;
                if i + 1 != held.len() {
                    seen.dropped_out_of_order += 1;
                }
                held.remove(i);
            }
            if rng.below(40) == 0 {
                seen.reopens += 1;
                held.clear();
                store.reopen();
            }
            // Reuse what no held snapshot can reach: pages freed by txns up to the oldest one.
            store.recycle_upto(held.iter().map(|&(t, _, _)| t).min().unwrap_or(u64::MAX));
            for (t, r, m) in &held {
                let reader = store.reader_at(*t);
                for (k, v) in m.iter().take(4).chain(m.iter().rev().take(4)) {
                    let got = get(&reader, r, k).expect("held snapshot get");
                    assert_eq!(got, Some(v.as_slice()), "round {round}: held snapshot {t}");
                }
            }
            if !held.is_empty() {
                let (t, r, m) = &held[round % held.len()];
                let walk = walk_tree(&store.reader_at(*t), r);
                assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(m.iter()), "held {t}");
            }
        }
        eprintln!(
            "page size {page_size}: max height {max_height}, {} keys, {} pages, {seen:?}",
            model.len(),
            store.alloc.page_count()
        );
        assert!(max_height >= min_height, "the tree reached height {max_height}, not {min_height}");
        let missing = seen.missing(merge, merge && gate_merges);
        assert!(missing.is_empty(), "never ran: {missing:?} ({seen:?})");
        assert!(seen.aborts > 10, "too few aborted batches: {seen:?}");
        let cached = cached_deletes() - deletes_before;
        assert!(cached > 0 || !cfg!(debug_assertions), "no delete in a cached leaf: {seen:?}");
        for (t, r, m) in &held {
            let walk = walk_tree(&store.reader_at(*t), r);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(m.iter()), "held {t} at the end");
        }

        // A reopen serves the same tree; writes after it (verifying every published page before
        // its first copy, reusing the pages the reopen pooled) keep it sound.
        held.clear();
        store.reopen();
        store.recycle();
        for op in 0..64 {
            let k = batch_key(rng.below(key_space), &g);
            if rng.below(2) == 0 {
                let v = batch_value(&mut rng, &g, &k);
                put(&mut store, &mut root, &k, &v).expect("put after the reopen");
                model.insert(k, v);
            } else {
                let had = model.remove(&k).is_some();
                assert_eq!(
                    delete(&mut store, &mut root, &k).expect("delete after the reopen"),
                    had
                );
            }
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "reopen op {op}");
        }
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    #[test]
    fn random_batches_match_a_btreemap_model() {
        // Walks of 64 KiB pages (a CRC of each page) dominate: fewer, shorter rounds there. Each
        // seed reaches its height and every change of the coverage gate. Merging is off: the
        // shapes of a tree without it (unlinks of emptied leaves, single-child branches).
        for (page_size, key_space, rounds, seed, min_height) in [
            (4096, 2_000, 400, 0x2545_F491_4F6C_DD21, 3),
            (8192, 2_000, 400, 0x2545_F491_4F6C_DD21, 3),
            (65536, 2_000, 160, 0x1234_5678_9ABC_DEF2, 2),
        ] {
            random_batches(BatchRun {
                page_size,
                key_space,
                rounds,
                phase: rounds / 4,
                seed,
                min_height,
                merge: false,
                gate_merges: false,
            });
        }
    }

    /// [`random_batches`] with merging: the trees stay smaller, so the run takes more keys and
    /// rounds to reach height 3, where every merge shape of the gate runs. The heavy variant
    /// merges at every page size.
    #[test]
    fn random_batches_with_merging_match_a_btreemap_model() {
        random_batches(BatchRun {
            page_size: 4096,
            key_space: 4_000,
            rounds: 600,
            phase: 150,
            seed: 0x2545_F491_4F6C_DD21,
            min_height: 3,
            merge: true,
            gate_merges: true,
        });
    }

    /// The key whose cell [`private_tree_with_a_nested_cell`] makes a nested cell.
    const NESTED_KEY: [u8; 4] = 150u32.to_be_bytes();

    /// A tree private to the store's transaction (300 entries of 4-byte keys and 32-byte values,
    /// plus one value in an overflow run), with its trusted handle and a path cache filled at
    /// [`NESTED_KEY`]'s leaf, whose cell for that key was then made a nested cell in place (flags
    /// `CELL_NESTED`, a valid encoded root). A private page carries no CRC, so only the cell rules
    /// can find it, and the edit leaves the cache's epoch valid.
    fn private_tree_with_a_nested_cell() -> (MemStore, TreeRoot, PathCache) {
        let (mut store, mut root, mut cache) =
            (MemStore::new(4096), TreeRoot::EMPTY, PathCache::default());
        for i in 0..300u32 {
            put(&mut store, &mut root, &i.to_be_bytes(), &[i as u8; 32]).expect("put");
        }
        put(&mut store, &mut root, &u32::MAX.to_be_bytes(), &[7; 3 * 4096]).expect("run");
        assert!(root.height >= 2 && root.overflow_pages > 0, "{root:?}");
        put_through(&mut store, &mut root, Some(&mut cache), &NESTED_KEY, &[150; 32])
            .expect("fill");
        let leaf = descend(&store, &root, &NESTED_KEY, |_, _, _, _| Ok(()), None)
            .expect("descend")
            .expect("leaf")
            .pgno;
        let nested = TreeRoot {
            root: 9,
            root_txn: 7,
            height: 1,
            entries: 3,
            leaf_pages: 1,
            ..TreeRoot::EMPTY
        }
        .encode();
        let page = store.page_mut(leaf).expect("a private leaf");
        let Find::Found { index, .. } = page::leaf_find(page, &NESTED_KEY).expect("find") else {
            panic!("the key is in its leaf");
        };
        let fit =
            page::LeafMut::new(page).expect("leaf").replace_value(index, Val::Nested(&nested));
        assert_eq!(fit.expect("a nested cell"), Fit::Done);
        (store, root, cache)
    }

    /// `result` is the [`page::nested_error`] refusal.
    #[track_caller]
    fn assert_nested_refused<T: std::fmt::Debug>(what: &str, result: Result<T>) {
        const NONE: &str = "a nested cell in a tree that holds none";
        assert!(
            matches!(&result, Err(Error::Corrupt(m)) if m.contains(NONE)),
            "{what}: {result:?}"
        );
    }

    /// A nested cell in a plain tree's private leaf is damage to every write that reads the old
    /// value or walks the leaf ([`page::nested_error`]): a put over it from the root ([`plan`]) and
    /// in the cached leaf ([`put_cached`]), its delete in the cached leaf ([`delete_cached`]) and
    /// from the root ([`plan_delete`]), and a clear or free of the tree ([`tree_pages`]). Each is
    /// refused with the handle unchanged.
    #[test]
    fn a_nested_cell_in_a_private_leaf_is_corrupt_at_every_write() {
        let value = [1; 32];
        let (mut store, mut root, _) = private_tree_with_a_nested_cell();
        let before = root;
        assert_nested_refused("put", put(&mut store, &mut root, &NESTED_KEY, &value));
        assert_eq!(root, before, "put");

        let (mut store, mut root, mut cache) = private_tree_with_a_nested_cell();
        let r = put_through(&mut store, &mut root, Some(&mut cache), &NESTED_KEY, &value);
        assert_nested_refused("cached put", r);
        assert_eq!(root, before, "cached put");

        let (mut store, mut root, mut cache) = private_tree_with_a_nested_cell();
        let r = delete_through(&mut store, &mut root, Some(&mut cache), &NESTED_KEY);
        assert_nested_refused("cached delete", r);
        assert_eq!(root, before, "cached delete");

        let (mut store, mut root, _) = private_tree_with_a_nested_cell();
        assert_nested_refused("delete", delete(&mut store, &mut root, &NESTED_KEY));
        assert_eq!(root, before, "delete");

        let (mut store, mut root, _) = private_tree_with_a_nested_cell();
        assert_nested_refused("clear", clear(&mut store, &mut root));
        assert_eq!(root, before, "clear");
        assert_nested_refused("free_subtree", free_subtree(&mut store, &root, true));
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn random_batches_match_a_btreemap_model_heavy() {
        for seed in 1..=6_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for (page_size, key_space) in
                [(4096, 2_000), (8192, 3_000), (16384, 4_000), (32768, 6_000), (65536, 8_000)]
            {
                for merge in [false, true] {
                    random_batches(BatchRun {
                        page_size,
                        key_space,
                        rounds: 3_000,
                        phase: 60,
                        seed,
                        min_height: 2,
                        merge,
                        gate_merges: page_size == 4096,
                    });
                }
            }
        }
    }

    /// Copy-on-write of published page `p` through [`writable`], against the two-step copy it
    /// replaced (the page into a buffer, the buffer into the reserved page, then the stamp): the
    /// same bytes, the new page number and this txn in the header, the trailer zeroed.
    fn assert_one_copy_is_the_two_step_copy<S: PageWrite + ?Sized>(store: &mut S, p: u32) {
        let txn = store.txn();
        let q = store.alloc().expect("alloc");
        let mut want = store.page(p).expect("a published page").to_vec();
        page::set_pgno_txn(&mut want, q, txn);
        crc::zero_crc(&mut want);
        let mut scratch = Scratch::default();
        scratch.reserved.push(q);
        assert_eq!(writable(store, p, &mut scratch).expect("copy"), q);
        let got = store.private_page(q).expect("the copy");
        assert!(got == want.as_slice(), "page {p} copied to {q}: not the two-step copy");
        assert_eq!((page::pgno(got), page::txn(got)), (q, txn), "page {p}: the stamp");
        assert!(crc::crc_is_zero(got), "page {p}: the trailer");
    }

    /// [`PageWrite::copy_published`] of page `p` into private page `q` returns what
    /// [`PageRead::page`] of `p` returns: its bytes, or its error, message and all.
    fn assert_copy_fails_as_page_fails<S: PageWrite + ?Sized>(store: &mut S, p: u32, q: u32) {
        let want = store.page(p).map(<[u8]>::to_vec).map_err(|e| e.to_string());
        let got = store.copy_published(p, q).map(|c| c.to_vec()).map_err(|e| e.to_string());
        assert_eq!(got, want, "copy of page {p}");
    }

    /// The damage cases of [`assert_copy_fails_as_page_fails`] on published page `p`, each
    /// undone after: a zero trailer, a header naming another page, a txn after the writer's;
    /// and the pages either side of the tree pages. Then the two guards: a private source is
    /// refused, and so is a published destination (`other`, a published page other than `p`),
    /// whose bytes stay as they were. `raw` gives a page's bytes unchecked.
    fn assert_copies_fail_as_pages_fail<S: PageWrite + ?Sized>(
        store: &mut S,
        p: u32,
        other: u32,
        raw: fn(&mut S, u32) -> &mut [u8],
    ) {
        let q = store.alloc().expect("alloc");
        let (txn, saved) = (store.txn(), raw(store, p).to_vec());
        let page_txn = page::txn(&saved);
        type Damage<'d> = &'d dyn Fn(&mut [u8]);
        let damages: [Damage<'_>; 4] =
            [&|_| {}, &|b| crc::zero_crc(b), &|b| page::set_pgno_txn(b, p + 1, page_txn), &|b| {
                page::set_pgno_txn(b, p, txn + 1)
            }];
        for damage in damages {
            damage(raw(store, p));
            assert_copy_fails_as_page_fails(store, p, q);
            raw(store, p).copy_from_slice(&saved);
        }
        let count = store.page_count();
        for outside in [0, 1, count, count + 1] {
            assert_copy_fails_as_page_fails(store, outside, q);
        }
        let err = store.copy_published(q, q).expect_err("a private source");
        assert!(matches!(&err, Error::Failed(m) if m.contains("is private")), "{err}");

        assert_ne!(other, p, "a destination other than the source");
        let before = raw(store, other).to_vec();
        let err = store.copy_published(p, other).map(|_| ()).expect_err("a published destination");
        let want = format!("page {other} is not private: copy it first");
        assert!(matches!(&err, Error::Failed(m) if *m == want), "{err}");
        assert!(raw(store, other) == before.as_slice(), "the destination {other} changed");
    }

    /// A [`MemStore`] holding a committed tree of a few levels, and its handle.
    fn committed_mem_store() -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..600u32 {
            put(&mut store, &mut root, &i.to_be_bytes(), &[i as u8; 40]).expect("put");
        }
        store.commit_like();
        assert!(root.height >= 2, "a tree with branches");
        (store, root)
    }

    /// An environment (4 KiB pages, a 16 KiB reservation, so its mapping has moved) holding a
    /// committed table of a few levels.
    fn committed_env() -> (tempfile::TempDir, crate::Env) {
        let dir = tempfile::TempDir::with_prefix("tndb_b_cow").expect("temp dir");
        let options = crate::EnvOptions { map_reserve: 16 << 10, ..Default::default() };
        let env = crate::Env::open(dir.path().join("db"), options).expect("open");
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table("t").expect("table");
        for i in 0..600u32 {
            txn.put(&table, &i.to_be_bytes(), &[i as u8; 40]).expect("put");
        }
        txn.commit().expect("commit");
        (dir, env)
    }

    /// The pages of the writer's newest committed state that its trees reach: the catalog's, the
    /// free list's and every table's.
    fn published_tree_pages(w: &crate::env::Writer) -> Vec<u32> {
        let (catalog, free) = (w.last.catalog, w.last.free);
        let tables = walk_tree(w, &catalog).entries.into_iter().map(|(_, record)| {
            crate::catalog::TableRecord::decode(&record).expect("a table record").root
        });
        [catalog, free].into_iter().chain(tables).flat_map(|r| walk_tree(w, &r).pages).collect()
    }

    /// Copy-on-write takes one copy, through [`PageWrite::copy_published`], and gives the bytes
    /// the two-step copy gave, on a [`MemStore`] (`copy_within`) and on the environment's writer
    /// (a published page through the readers' view into a private page).
    #[test]
    fn copy_on_write_is_the_two_step_copy() {
        let (mut store, root) = committed_mem_store();
        for p in store.reachable_pgnos(&root) {
            assert_one_copy_is_the_two_step_copy(&mut store, p);
        }

        let (_dir, env) = committed_env();
        let mut txn = env.begin_rw().expect("begin");
        let w = txn.writer_mut();
        let pages = published_tree_pages(w);
        assert!(pages.len() > 5, "{} tree pages", pages.len());
        for p in pages {
            assert_one_copy_is_the_two_step_copy(w, p);
        }
    }

    /// A page published before the file grew past the writer's mapping, in the same transaction,
    /// is copied from the new mapping with the right bytes: the writer's view is the file's own
    /// [`crate::file::MapView`], which the remap moved.
    #[test]
    fn copy_on_write_reads_a_page_published_before_a_remap() {
        let (_dir, env) = committed_env();
        let mut txn = env.begin_rw().expect("begin");
        let w = txn.writer_mut();
        let size = w.geometry.page_size();
        let p = *published_tree_pages(w).last().expect("a tree page");
        let before = w.file.page(p, size).expect("mapped").as_ptr();
        let mut grown = 0;
        while w.file.page(p, size).expect("mapped").as_ptr() == before {
            w.alloc().expect("alloc");
            grown += 1;
            assert!(grown < 4096, "the mapping never moved");
        }
        let view = w.view.slice(u64::from(p) * size as u64, size).expect("in the view");
        assert_eq!(view.as_ptr(), w.file.page(p, size).expect("mapped").as_ptr(), "the view moved");
        assert_one_copy_is_the_two_step_copy(w, p);
    }

    /// A copy-on-write fetches its source with exactly [`PageRead::page`]'s checks: a damaged or
    /// out-of-range page fails with the same error, and a page past the published bytes that
    /// this transaction freed (where the writer's view ends) is copied as `page` reads it. A
    /// private source, and a published destination, are refused, and the destination is left
    /// as it was.
    #[test]
    fn copy_on_write_fails_as_the_page_fetch_fails() {
        let (mut store, root) = committed_mem_store();
        let pages = store.reachable_pgnos(&root);
        let (root_page, leaf) = (pages[0], *pages.last().expect("a page"));
        assert_copies_fail_as_pages_fail(&mut store, leaf, root_page, MemStore::raw_mut);

        fn raw(w: &mut crate::env::Writer, p: u32) -> &mut [u8] {
            let size = w.geometry.page_size();
            w.file.page_mut(p, size).expect("in the file")
        }
        let (_dir, env) = committed_env();
        let mut txn = env.begin_rw().expect("begin");
        let w = txn.writer_mut();
        let size = w.geometry.page_size();
        let pages = published_tree_pages(w);
        let (first, leaf) = (pages[0], *pages.last().expect("a tree page"));
        assert_copies_fail_as_pages_fail(w, leaf, first, raw);

        // A freed page past the published bytes, forged to pass the checks, then not.
        let src = w.page(leaf).expect("leaf").to_vec();
        let freed = loop {
            let p = w.alloc().expect("alloc");
            if p >= w.last.page_count {
                break p;
            }
        };
        let (txn_id, page) = (w.txn(), raw(w, freed));
        page.copy_from_slice(&src);
        page::set_pgno_txn(page, freed, txn_id);
        crc::add_crc32_nonzero(page);
        let q = w.alloc().expect("alloc");
        w.free(freed);
        assert!(w.view.slice(u64::from(freed) * size as u64, size).is_none(), "past the view");
        assert!(w.page(freed).is_ok(), "page reads it");
        assert_copy_fails_as_page_fails(w, freed, q);
        crc::zero_crc(raw(w, freed));
        assert_copy_fails_as_page_fails(w, freed, q);
    }
}
