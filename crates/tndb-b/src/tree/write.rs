//! The write path: copy-on-write of the root-to-leaf path, insert and replace with leaf and
//! branch splits, and a new root when the old one splits; delete, with emptied pages unlinked and
//! a single-child root collapsed; and freeing a whole tree.
//!
//! A put first plans from a read-only descent (checking every page on the path, and verifying
//! each published page it will copy), then allocates every page it can need — the copies of the
//! published pages on the path and, if the leaf overflows, the worst case of the split cascade —
//! and only then changes anything. A failed allocation (a full disk, say) therefore leaves the
//! tree exactly as it was: the same root handle and every reachable page byte for byte. A delete
//! plans and reserves the same way ([`delete`]).

use super::{
    read::{get, tighten},
    PageRead, PageWrite, TreeRoot, MAX_HEIGHT,
};
use crate::{
    crc::{self, CrcState},
    error::{Error, Result},
    page::{self, Find, Fit, Geometry, Kind, Search},
    page_set::PageSet,
};

// Test-only counters, debug builds only: the benchmark is compiled with the test configuration,
// and a thread-local update on a timed path would show in its release timings.
#[cfg(all(test, debug_assertions))]
thread_local! {
    /// Test-only: how many published pages copy-on-write verified (CRC and structure).
    pub(crate) static COW_VERIFIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Test-only: how many puts [`put_cached`] did in the cached leaf.
    pub(crate) static CACHED_PUTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
}

/// Where a put left its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Placed {
    /// True if it replaced a present key's value.
    pub(crate) replaced: bool,
    /// The entry's slot in the leaf the table's [`PathCache`] then names; `None` after a split
    /// that left the cache empty (every split but an append split through a cache, which fills
    /// it from a second descent of the new path).
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

/// [`put_sized`], or an append (`kind`), through a table's write-path `cache` ([`PathCache`]): a
/// put whose key lands in the cached leaf and fits there changes that leaf in place with no
/// descent, exactly as the descent's put would (the same leaf, change, entry count and bytes).
/// Any other put descends ([`put_sized`]) and refills the cache if it did not split, or did an
/// append split (by descending the new path again for the entry). An append takes the cached
/// leaf only if it is the tree's rightmost; elsewhere, its descent refuses a key that does not
/// sort after the tree's last key.
pub(crate) fn put_cached<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    cache: &mut PathCache,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
) -> Result<Placed> {
    if *trusted && cache.epoch == store.epoch() {
        if let Some(placed) = put_in_cached_leaf(store, root, cache, key, value, kind)? {
            return Ok(placed);
        }
    }
    put_from_root(store, root, trusted, Some(cache), key, value, kind)
}

/// [`put_sized`], or an append (`kind`): descend from the root, plan, reserve, apply; then fill
/// `cache`, if given, with the entry's path (private and trusted once the put is done) when the
/// put did not split, or did an append split (the path then found by a second descent).
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
    if root.is_empty() {
        let p = store.alloc()?;
        let leaf = store.page_mut(p)?;
        page::init_leaf_zeroed(leaf, p, txn);
        if page::leaf_insert(leaf, 0, key, value)? == Fit::Full {
            return Err(bug("an entry overflows an empty leaf"));
        }
        *root = TreeRoot { root: p, root_txn: txn as u32, height: 1, entries: 1 };
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
    let (leaf, change, needed, range) = plan(&*store, root, *trusted, key, value, kind, path)?;
    if let Some(tail) = tail.as_mut() {
        tail.set_range(range);
    }
    // A put that needs no page (a private path, no split) needs no scratch either.
    let mut scratch = if needed == 0 { Scratch::default() } else { store.take_scratch() };
    reserve(store, needed, &mut scratch.reserved)?;
    let result = make_path_writable(store, root, path, leaf, &mut scratch).and_then(|leaf| {
        let replaced = apply(store, root, path, leaf, key, value, change, &mut scratch)?;
        Ok((leaf, replaced))
    });
    // The split's worst case may not have happened: the pages it did not use are reusable.
    for p in scratch.reserved.drain(..) {
        store.free(p);
    }
    if needed != 0 {
        store.put_scratch(scratch);
    }
    let (leaf, replaced) = result?;
    *trusted = true;
    let (leaf, slot) = match (change, tail.as_mut()) {
        (LeafChange::Replace(i) | LeafChange::Insert(i), _) => (leaf, Some(i)),
        // An append split changed the path, and the next put of an ascending run goes to the new
        // right leaf. With a cache, find the entry again on the new path (every page on it is
        // private now, so the trusted descent checks only identity, kind and non-emptiness) and
        // cache it, so neither that put nor a cursor placed at the entry descends again. Any other
        // split leaves the cache empty: a second descent there would rarely be used.
        (LeafChange::Split { append: true, .. }, Some(tail)) => {
            path.len = 0;
            let (found, _) = descend_write(&*store, root, true, key, path)?;
            let Find::Found { index, .. } = found.view.find(key)? else {
                return Err(bug("a split lost the entry it put"));
            };
            tail.set_range(found.range);
            (found.pgno, Some(index))
        }
        (LeafChange::Split { .. }, _) => (leaf, None),
    };
    if let (Some(tail), Some(_)) = (tail, slot) {
        tail.fill(store, leaf, path)?;
    }
    Ok(Placed { replaced, slot })
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
        self,
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

/// A table's write-path cache: the leaf its latest put reached, the branches above it, and the
/// key range `[lo, hi)` that routes to that leaf (the intersection of every ancestor's
/// separators, as [`read::descend`] keeps it), so the next put landing there skips the descent
/// ([`put_cached`]). The bounds are owned copies in buffers the cache reuses.
///
/// It is filled after a put that ended on a trusted path (every page private, built here: see
/// [`descend_write`]) without a split, or after an append split from a second descent of the new
/// path, and holds only while the allocator's epoch ([`PageWrite::epoch`]) is the one it was
/// filled in. Every change to a tree's shape or separators allocates or frees a page (a copy, a
/// split, a new root, an unlink or a collapse; a cleared or dropped tree frees its pages), and a
/// commit or abort ends the transaction's private pages, each with a new epoch. An in-place
/// insert, replace or delete changes only its leaf and no separator. So within the epoch the root
/// still names the cached path, every page on it is still private, and exactly the keys in
/// `[lo, hi)` route to the leaf. The catalog and the free list write through [`put`] and
/// [`delete`] and have no cache.
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
    /// descent for any key of the leaf's range takes. Meaningful only right after a put that
    /// returned a slot ([`Placed::slot`]).
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
/// is the tree's: a key not past it is [`Error::AppendOutOfOrder`].
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
            Find::Found { index, value: old } => {
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

/// Test-only forms of [`put`] and [`delete`] for trees whose holder is not tracked: the root is
/// taken as trusted exactly when it is private, which holds in a test that never forges a root.
#[cfg(test)]
pub(crate) mod plain {
    use super::*;

    /// [`super::put`], trusting a private root.
    pub(crate) fn put<S: PageWrite + ?Sized>(
        store: &mut S,
        root: &mut TreeRoot,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        let mut trusted = !root.is_empty() && store.is_private(root.root);
        super::put(store, root, &mut trusted, key, value)
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
    /// The keys that route to the leaf, `[lo, hi)`: the nearest separator either side on the
    /// path. On a path the descent accepts this is the intersection of every ancestor's
    /// separators: an untrusted page's keys lie within its ancestors' intersection
    /// (`keys_in_range`), and a trusted page's do by construction.
    range: Bounds<'s>,
}

/// The writer's descent from `root` to the leaf for `key`, recording the branches on `path`;
/// returns the leaf and how many published pages are on the path (each one the write copies).
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
/// writer stamped it, and `keys_in_range` against the intersection of every ancestor's separators
/// (as [`read::descend`] keeps it), computed only once the descent first reaches an untrusted
/// page: until then the separators are not needed. A branch [`verify_published`] checks also
/// gets [`check_cells_naming_this_txn`], so its private copy names no page by this transaction's
/// id that the copy-time trust would not vouch for.
fn descend_write<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    path: &mut Path,
) -> Result<(WriteLeaf<'s>, usize)> {
    let geometry = store.geometry();
    let (mut pgno, mut want_txn) = (root.root, root.root_txn);
    let mut parent_trusted = root_trusted;
    let mut copies = 0;
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
                verify_published(page, &geometry)?;
                if let page::View::Branch(branch) = &view {
                    check_cells_naming_this_txn(store, pgno, branch)?;
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
                return Ok((WriteLeaf { pgno, page, view, range }, copies));
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
/// It runs where [`verify_published`] does: a page this writer stamped holds only cells it wrote
/// from checked pages (the trust that skips the CRC and [`page::validate`] there), and a forged
/// cell is damage at rest.
fn check_cells_naming_this_txn<S: PageWrite + ?Sized>(
    store: &S,
    pgno: u32,
    branch: &page::BranchView<'_>,
) -> Result<()> {
    let txn = store.txn();
    for i in 0..branch.nkeys() {
        let (child, child_txn) = branch.child(i)?;
        if child_txn != txn as u32 {
            continue;
        }
        let header_txn = page::txn(store.page(child)?);
        if header_txn as u32 != child_txn || header_txn >= txn {
            return Err(Error::Corrupt(format!(
                "published page {pgno} names page {child} with this txn's id {child_txn}, but it \
                 holds txn {header_txn}: a forged or stale pointer"
            )));
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

/// The read-only first half of a put: descend to the leaf for `key` with the writer's checks
/// ([`descend_write`]), and decide the change. Returns the leaf, the change, how many pages it can
/// need, and the leaf's key range ([`WriteLeaf::range`]).
///
/// An append ([`PutKind::Append`]) is refused ([`Error::AppendOutOfOrder`]) unless the key lands
/// past the last key of the rightmost leaf (no upper bound). A key past the tree's last key routes
/// there, since no separator exceeds the keys to its right; a key that lands in another leaf lies
/// below that leaf's upper bound, so below the keys of the (non-empty) leaves to its right.
fn plan<'s, S: PageWrite + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
    path: &mut Path,
) -> Result<(u32, LeafChange, usize, Bounds<'s>)> {
    let (leaf, copies) = descend_write(store, root, root_trusted, key, path)?;
    let view = leaf.view;
    let found = match kind {
        PutKind::Upsert => view.find(key)?,
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
    let change = match found {
        Find::Found { index: i, value: old } => {
            let old = old.len();
            if value.len() <= old || value.len() - old <= view.free_space() {
                LeafChange::Replace(i)
            } else {
                LeafChange::Split { at: i, replace: true, append: false }
            }
        }
        Find::NotFound { index: at, .. } => {
            if view.fits(page::cell_size_leaf(key.len(), value.len())) {
                LeafChange::Insert(at)
            } else {
                let append = at == view.nkeys() && is_rightmost(store, path)?;
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
    Ok((leaf.pgno, change, copies + split, leaf.range))
}

/// A published page about to be copied must pass its CRC and [`page::validate`]: the copy is
/// stamped afresh at commit, so copying a page damaged at rest would make the damage CRC-valid
/// and invisible, and the mutators keep a page valid only if it was valid.
fn verify_published(page: &[u8], geometry: &Geometry) -> Result<()> {
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
    /// [`writable`]'s copy of a published page.
    copy: Vec<u8>,
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

/// The writable version of page `p`: `p` itself if private, else a copy in a reserved page,
/// re-stamped with its new page number and this transaction's id and its trailer zeroed (dirty);
/// the original is freed. (The planning descent verified it.)
fn writable<S: PageWrite + ?Sized>(store: &mut S, p: u32, scratch: &mut Scratch) -> Result<u32> {
    if store.is_private(p) {
        return Ok(p);
    }
    let q = scratch.reserved.pop().ok_or_else(|| bug("copy-on-write ran out of reserved pages"))?;
    let txn = store.txn();
    let buf = &mut scratch.copy;
    buf.clear();
    buf.extend_from_slice(store.page(p)?);
    let copy = store.page_mut(q)?;
    if copy.len() != buf.len() {
        return Err(bug(format!("page {q} of {} bytes, not {}", copy.len(), buf.len())));
    }
    copy.copy_from_slice(buf);
    page::set_pgno_txn(copy, q, txn);
    crc::zero_crc(copy);
    store.free(p);
    Ok(q)
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

/// Apply the planned change to the writable `leaf`, splitting up the writable `path` with pages
/// from `scratch.reserved`. Returns true for a replaced value.
#[allow(clippy::too_many_arguments)]
fn apply<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut Path,
    leaf: u32,
    key: &[u8],
    value: &[u8],
    change: LeafChange,
    scratch: &mut Scratch,
) -> Result<bool> {
    let fit = |fit: Fit, what: &str| match fit {
        Fit::Done => Ok(()),
        Fit::Full => Err(bug(format!("{what} did not fit as planned"))),
    };
    let (at, replace, append) = match change {
        LeafChange::Replace(i) => {
            fit(page::leaf_replace_value(store.page_mut(leaf)?, i, value)?, "a value")?;
            return Ok(true);
        }
        LeafChange::Insert(i) => {
            fit(page::leaf_insert(store.page_mut(leaf)?, i, key, value)?, "an entry")?;
            root.entries += 1;
            return Ok(false);
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

    // Split the leaf: plan from it, then write the right half into its new page (all zeros from
    // the allocation) and the left half over the leaf.
    let mut right = take()?;
    split.split_leaf(store.page_mut(leaf)?, at, key, value, append)?;
    split.write_right(store.page_mut(right)?, right, txn)?;
    split.write_left(store.page_mut(leaf)?)?;

    // Insert the separator up the path, splitting each branch it does not fit.
    let mut placed = false;
    while let Some((p, ci)) = path.pop() {
        if page::branch_insert(store.page_mut(p)?, ci + 1, split.sep(), right, txn32)? == Fit::Done
        {
            placed = true;
            break;
        }
        let q = take()?;
        split.split_branch(store.page_mut(p)?, ci + 1, right, txn32, append)?;
        split.write_right(store.page_mut(q)?, q, txn)?;
        split.write_left(store.page_mut(p)?)?;
        right = q;
    }
    // The root split: grow a new root one level up.
    if !placed {
        let q = take()?;
        let sep = split.sep();
        page::init_root(store.page_mut(q)?, q, txn, (root.root, txn32), sep, (right, txn32))?;
        (root.root, root.root_txn) = (q, txn32);
        root.height += 1;
    }
    if !replace {
        root.entries += 1;
    }
    Ok(replace)
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
    /// How many published pages the delete copies.
    copies: usize,
}

/// Delete `key` from the tree `root` names, returning whether it was present. The key is looked
/// up first, with a lookup's checks ([`get`]): a miss copies, verifies and frees nothing.
///
/// A hit plans from a read-only descent (each page on the path checked as a put checks it,
/// published pages verified before the delete trusts or copies them), reserves the copies of the
/// published pages that stay in the tree, and only then changes anything, so a failed allocation
/// leaves the tree exactly as it was. The entry leaves its leaf; a leaf it empties is unlinked from
/// its parent, and a parent left with no child from its own, up the path; a root branch left with
/// a single child gives way to it ([`DeleteShape::Collapse`]). A delete of the last entry leaves
/// [`TreeRoot::EMPTY`] (no pages), as an empty tree always is. Emptied pages are never copied:
/// they leave the tree as they are, a published one freed and a private one reusable at once.
///
/// Pages that are merely sparse are not merged or rebalanced: only empty pages leave the tree, so
/// a tree never keeps (or scans) pages that hold nothing, but a tree shrunk by scattered deletes
/// keeps part-full pages. `root` is updated (root page, height, entry count).
pub(crate) fn delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    trusted: &mut bool,
    key: &[u8],
) -> Result<bool> {
    if get(&*store, root, key)?.is_none() {
        return Ok(false);
    }
    let mut path = Path::new();
    let plan = plan_delete(&*store, root, *trusted, key, &mut path)?;
    let mut scratch = if plan.copies == 0 { Scratch::default() } else { store.take_scratch() };
    reserve(store, plan.copies, &mut scratch.reserved)?;
    let result = apply_delete(store, root, &mut path, &plan, &mut scratch);
    // Every reserved page is used, so this frees nothing unless the apply failed part way.
    for p in scratch.reserved.drain(..) {
        store.free(p);
    }
    if plan.copies != 0 {
        store.put_scratch(scratch);
    }
    result?;
    // A kept path is now private and built here; a collapse's new root is trusted exactly when
    // it is private (`collapse_to` refused a private page below an untrusted one).
    *trusted = match plan.shape {
        DeleteShape::Leaf | DeleteShape::Unlink(_) => true,
        DeleteShape::Collapse { root: new_root, .. } => store.is_private(new_root),
        DeleteShape::Empty => false,
    };
    Ok(true)
}

/// The read-only first half of a delete of `key` (present: [`delete`] looked it up): descend to
/// its leaf with the writer's checks ([`descend_write`]), and decide the change.
fn plan_delete<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    root_trusted: bool,
    key: &[u8],
    path: &mut Path,
) -> Result<DeletePlan> {
    let geometry = store.geometry();
    let (leaf, _) = descend_write(store, root, root_trusted, key, path)?;
    let Search::Found(at) = leaf.view.search(key)? else {
        return Err(bug("a key found by the lookup is missing from its leaf"));
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
    let kept = match shape {
        DeleteShape::Leaf => path.len() + 1,
        DeleteShape::Unlink(d) => d + 1,
        DeleteShape::Collapse { .. } | DeleteShape::Empty => 0,
    };
    let pages = path.iter().map(|&(p, _)| p).chain([leaf.pgno]);
    let copies = pages.clone().take(kept).filter(|&p| !store.is_private(p)).count();
    dropped.extend(pages.skip(kept));
    Ok(DeletePlan { leaf: leaf.pgno, at, shape, dropped, copies })
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
            verify_published(page, geometry)?;
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
/// `scratch.reserved`), take the entry or the emptied child out, free the dropped pages and update
/// the handle.
fn apply_delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut Path,
    plan: &DeletePlan,
    scratch: &mut Scratch,
) -> Result<()> {
    match plan.shape {
        DeleteShape::Leaf => {
            let leaf = make_path_writable(store, root, path, plan.leaf, scratch)?;
            page::leaf_delete(store.page_mut(leaf)?, plan.at)?;
        }
        DeleteShape::Unlink(d) => {
            let (keeper, ci) = path[d];
            path.truncate(d);
            let keeper = make_path_writable(store, root, path, keeper, scratch)?;
            page::branch_delete(store.page_mut(keeper)?, ci)?;
        }
        DeleteShape::Collapse { root: new_root, root_txn, height } => {
            *root = TreeRoot { root: new_root, root_txn, height, entries: root.entries };
        }
        DeleteShape::Empty => {}
    }
    for &p in &plan.dropped {
        store.free(p);
    }
    if plan.shape == DeleteShape::Empty {
        *root = TreeRoot::EMPTY;
    } else {
        root.entries -= 1;
    }
    Ok(())
}

/// Every page of the tree `root` names, root first, level by level, collected without changing
/// anything (so a caller freeing them refuses damage before it frees a page). Each branch is read
/// with a descent's checks (the page its parent names, a branch above the leaf level) and, if
/// published and not stamped here, verified ([`verify_published`]) before its child pointers are
/// followed: they decide which pages leave the tree, so a pointer damaged at rest must not free a
/// live page. Every child must be a tree page of the file reached once. Leaves are taken from
/// their parents' pointers and only the first is read (it must be a leaf: the tree is balanced,
/// so that checks the height). Damage is [`Error::Corrupt`].
pub(crate) fn tree_pages<S: PageWrite + ?Sized>(store: &S, root: &TreeRoot) -> Result<Vec<u32>> {
    root.check()?;
    if root.is_empty() {
        return Ok(Vec::new());
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
            if !store.is_private(p) && !store.is_stamped(p) {
                verify_published(page, &geometry)?;
            }
            for i in 0..page::nkeys(page) {
                let (child, child_txn) = page::branch_child(page, i)?;
                if child < 2 || child >= page_count || seen.contains(child) {
                    return Err(corrupt(format!("page {p} has a bad child {child}")));
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
    if page::kind(check_header(first, txn)?)? != Kind::Leaf {
        return Err(corrupt(format!("height {} does not reach the leaves", root.height)));
    }
    Ok(pages)
}

/// Free every page of the tree `root` names: all collected first ([`tree_pages`]), so damage is
/// refused before anything is freed. A published page is freed once no reader or durable meta can
/// reach it, a private one is reusable at once. The handle is the caller's to drop or reset.
pub(crate) fn free_subtree<S: PageWrite + ?Sized>(store: &mut S, root: &TreeRoot) -> Result<()> {
    for p in tree_pages(&*store, root)? {
        store.free(p);
    }
    Ok(())
}

/// Empty the tree `root` names: free every page ([`free_subtree`]) and reset `root` to
/// [`TreeRoot::EMPTY`]. Published snapshots keep reading the old tree. On damage nothing is
/// freed and `root` is unchanged.
pub(crate) fn clear<S: PageWrite + ?Sized>(store: &mut S, root: &mut TreeRoot) -> Result<()> {
    free_subtree(store, root)?;
    *root = TreeRoot::EMPTY;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use super::*;
    use crate::tree::{
        read::{descend, get},
        test_harness::{leaf_fill, walk_tree, MemStore, ReportSeed, Rng},
    };
    use plain::{delete, put};

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
    /// reachable page (byte for byte) and the freed list exactly as they were.
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
            let value = vec![n as u8; rng.len_upto(g.max_value(key.len()))];
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
        let mut root = TreeRoot { root: top, root_txn: txn, height: 3, entries };
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
            let mut root =
                TreeRoot { root: r, root_txn: store.txn() as u32, height: 2, entries: 3 };
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
        let mut root = TreeRoot { root: r, root_txn: txn as u32, height: 2, entries: 4 };
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

    /// Over-limit entries are refused before anything changes.
    #[test]
    fn over_limit_entries_are_refused() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let key = vec![1; g.max_key() + 1];
        assert!(matches!(put(&mut store, &mut root, &key, b""), Err(Error::KeyTooLarge { .. })));
        let value = vec![1; g.max_value(4) + 1];
        let put_value = put(&mut store, &mut root, b"four", &value);
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
    /// the failed delete changes nothing.
    #[test]
    fn a_collapse_checks_the_page_it_promotes() {
        for damage in 0..2 {
            let (mut store, mut root, right) = two_leaf_tree(20);
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
    }

    impl Coverage {
        /// The operations and changes that never ran.
        fn missing(&self) -> Vec<&'static str> {
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
            } = *self;
            [
                ("inserts", inserts),
                ("replaces", replaces),
                ("deletes", deletes),
                ("missed_deletes", missed_deletes),
                ("clears", clears),
                ("splits", splits),
                ("unlinks", unlinks),
                ("collapses", collapses),
                ("empties", empties),
                ("refills", refills),
                ("aborts", aborts),
                ("held", held),
                ("dropped_out_of_order", dropped_out_of_order),
                ("reopens", reopens),
            ]
            .into_iter()
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
    /// Reader snapshots are held and dropped out of order; freed pages are reused once no held
    /// snapshot can reach them, and every held snapshot keeps reading the state it saw (a page
    /// reused too early is `Corrupt` to it). Reopens (stamped pages forgotten, readers gone) come
    /// at random and at the end, followed by writes. A coverage gate proves each tree change ran:
    /// splits, unlinks, collapses, empties and refills.
    fn random_batches(run: BatchRun) {
        let _seed = ReportSeed(run.seed);
        let BatchRun { page_size, key_space, rounds, phase, seed, min_height } = run;
        let mut rng = Rng(seed);
        let mut store = MemStore::new(page_size);
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
        for round in 0..rounds {
            let ops = 1 + rng.below(12);
            let inject = rng.below(4) == 0;
            if inject {
                store.alloc.fail_allocs_after = Some(rng.below(6));
            }
            let grow = (round / phase) % 2 == 0;
            let mut failed = false;
            for op in 0..ops {
                let r = rng.below(100);
                let (put_below, delete_below) = if grow { (85, 99) } else { (5, 100) };
                let height = root.height;
                let was_empty = root.is_empty();
                let what = format!("round {round} op {op}");
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
                    put(&mut store, &mut root, &k, &v).map(|replaced| {
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
                    delete(&mut store, &mut root, &k).map(|removed| {
                        assert_eq!(removed, model.remove(&k).is_some(), "{what}: delete");
                        if !removed {
                            seen.missed_deletes += 1;
                            return;
                        }
                        seen.deletes += 1;
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
                } else if walk.pages.len() < pages && !root.is_empty() && r >= put_below {
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
        assert!(seen.missing().is_empty(), "never ran: {:?} ({seen:?})", seen.missing());
        assert!(seen.aborts > 10, "too few aborted batches: {seen:?}");
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
        // Walks of 64 KiB pages (a CRC of each page) dominate: fewer, shorter rounds there.
        for (page_size, key_space, rounds, seed, min_height) in [
            (4096, 2_000, 400, 0x2545_F491_4F6C_DD1D, 3),
            (8192, 2_000, 400, 0x9E37_79B9_7F4A_7C15, 3),
            (65536, 2_000, 160, 0x1234_5678_9ABC_DEF1, 2),
        ] {
            random_batches(BatchRun {
                page_size,
                key_space,
                rounds,
                phase: rounds / 4,
                seed,
                min_height,
            });
        }
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn random_batches_match_a_btreemap_model_heavy() {
        for seed in 1..=6_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for (page_size, key_space) in
                [(4096, 2_000), (8192, 3_000), (16384, 4_000), (32768, 6_000), (65536, 8_000)]
            {
                random_batches(BatchRun {
                    page_size,
                    key_space,
                    rounds: 3_000,
                    phase: 60,
                    seed,
                    min_height: 2,
                });
            }
        }
    }
}
