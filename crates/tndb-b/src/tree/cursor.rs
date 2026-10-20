//! Bidirectional cursors over one tree.
//!
//! A cursor's position is the path from the root to the current leaf, as `(page, slot)`
//! entries: for a branch the slot is the child being visited, for the leaf it is the current
//! entry. The path is detached from the tree ([`TreeCursor`]): each move takes the page store and
//! the tree's root as arguments, so the same cursor runs over a reader's snapshot and over the
//! writer's working tree, and a write transaction's cursor can re-seek after changing the tree.
//! Leaves are not linked (a copy-on-write tree cannot keep sibling links: copying a leaf would
//! force copying its neighbours), so a cursor moves between leaves through their parents.
//!
//! Every page a cursor enters gets the checks of a descent ([`super::read::descend`]): the
//! checked fetch, the copy its parent names (header page number and txn), a branch above the
//! tree's height and a leaf at it, no empty non-root leaf, and every key inside the intersection
//! of the ranges its ancestors' separators give it. A child pointer damaged at rest that leads to
//! another page of the tree is then [`Error::Corrupt`], not a scan that skips or repeats entries.

use super::{read::tighten, PageRead, TreeRoot, MAX_HEIGHT};
use crate::{
    error::{Error, Result},
    latch::Latch,
    page::{self, Kind, Search},
};

/// A `(key, value)` entry borrowed from a page store.
type Entry<'s> = (&'s [u8], &'s [u8]);

/// A key range `[lo, hi)` borrowed from a page store (`None`: unbounded).
type Bounds<'s> = (Option<&'s [u8]>, Option<&'s [u8]>);

/// Where a move landed: the entry the cursor is now at, with the page of the leaf holding it
/// (`None`: not at an entry).
type Landing<'s> = Option<(Entry<'s>, &'s [u8])>;

/// The entry of a [`Landing`].
fn entry(landing: Landing<'_>) -> Option<Entry<'_>> {
    landing.map(|(entry, _)| entry)
}

/// Where a cursor is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    /// Not positioned: `next` starts at the first entry, `prev` at the last.
    Unset,
    /// At the entry the leaf slot of the path names.
    At,
    /// Past the last entry: `next` stays here, `prev` goes to the last entry.
    AfterEnd,
    /// Before the first entry: `prev` stays here, `next` goes to the first entry.
    BeforeStart,
}

/// Which child a descent takes at each branch, and where in the leaf it lands.
#[derive(Clone, Copy, Debug)]
enum Edge<'k> {
    /// The first child; the leaf's first entry.
    First,
    /// The last child; the leaf's last entry.
    Last,
    /// The child whose range holds the key; in the leaf, the first entry `>=` the key (one past
    /// the last entry if there is none).
    Key(&'k [u8]),
}

/// The position of a cursor over one tree, detached from it: each move takes the page store and
/// the tree's root (see the module docs). An error leaves it [`State::Unset`].
#[derive(Clone, Debug)]
pub(crate) struct TreeCursor {
    /// The path from the root: `(branch, child taken)` entries, then `(leaf, current entry)`.
    path: [(u32, usize); MAX_HEIGHT as usize],
    /// Entries of `path` in use (the tree's height while [`State::At`]).
    depth: usize,
    state: State,
}

impl Default for TreeCursor {
    fn default() -> Self {
        Self { path: [(0, 0); MAX_HEIGHT as usize], depth: 0, state: State::Unset }
    }
}

impl TreeCursor {
    /// Where the cursor is.
    pub(crate) fn state(&self) -> State {
        self.state
    }

    /// Move one entry forward or backward inside `leaf`, the page of the leaf the cursor is at
    /// (fetched and checked when the cursor entered it). `None`, the cursor unmoved, when the
    /// move would leave the leaf or the cursor is not at an entry.
    pub(crate) fn step_in_leaf<'s>(
        &mut self,
        leaf: &'s [u8],
        forward: bool,
    ) -> Result<Option<Entry<'s>>> {
        if self.state != State::At {
            return Ok(None);
        }
        let slot = self.path[self.depth - 1].1;
        let to = if forward {
            (slot + 1 < page::nkeys(leaf)).then_some(slot + 1)
        } else {
            slot.checked_sub(1)
        };
        let Some(to) = to else { return Ok(None) };
        self.guarded(|c| {
            c.path[c.depth - 1].1 = to;
            Ok(Some(page::leaf_entry(leaf, to)?))
        })
    }

    /// Move to the first entry (after the end of an empty tree).
    pub(crate) fn first<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
    ) -> Result<Option<Entry<'s>>> {
        self.guarded(|c| c.start(store, root, Edge::First)).map(entry)
    }

    /// Move to the last entry (before the start of an empty tree).
    pub(crate) fn last<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
    ) -> Result<Option<Entry<'s>>> {
        self.guarded(|c| c.start(store, root, Edge::Last)).map(entry)
    }

    /// Move to the next entry: from an unset cursor or before the start, the first entry; past
    /// the last entry, after the end (and stay there).
    pub(crate) fn next<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
    ) -> Result<Option<Entry<'s>>> {
        self.guarded(|c| c.advance(store, root, true)).map(entry)
    }

    /// Move to the previous entry: from an unset cursor or after the end, the last entry; before
    /// the first entry, before the start (and stay there).
    pub(crate) fn prev<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
    ) -> Result<Option<Entry<'s>>> {
        self.guarded(|c| c.advance(store, root, false)).map(entry)
    }

    /// The entry the cursor is at, if it is at one.
    pub(crate) fn current<'s, S: PageRead + ?Sized>(
        &self,
        store: &'s S,
    ) -> Result<Option<Entry<'s>>> {
        if self.state != State::At {
            return Ok(None);
        }
        let (pgno, slot) = self.path[self.depth - 1];
        Ok(Some(page::leaf_entry(store.page(pgno)?, slot)?))
    }

    /// Move to the first entry `>= key`; after the end if there is none.
    pub(crate) fn seek<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
    ) -> Result<Option<Entry<'s>>> {
        self.guarded(|c| c.start(store, root, Edge::Key(key))).map(entry)
    }

    /// Move to the entry `key` and return its value; unset if there is none.
    pub(crate) fn seek_exact<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        Ok(self.guarded(|c| c.find(store, root, key))?.map(|((_, value), _)| value))
    }

    /// Run a move; an error leaves the cursor unset.
    fn guarded<T>(&mut self, op: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let result = op(self);
        if result.is_err() {
            self.reset(State::Unset);
        }
        result
    }

    /// Forget the path.
    fn reset(&mut self, state: State) {
        self.depth = 0;
        self.state = state;
    }

    /// [`Self::seek_exact`]: land on the entry `key`, or leave the cursor unset.
    fn find<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
    ) -> Result<Landing<'s>> {
        self.reset(State::Unset);
        root.check()?;
        if root.is_empty() {
            return Ok(None);
        }
        let leaf = self.descend(store, root, root.root, root.root_txn, Edge::Key(key))?;
        let slot = self.path[self.depth - 1].1;
        if slot < page::nkeys(leaf) {
            let (k, v) = page::leaf_entry(leaf, slot)?;
            if k == key {
                self.state = State::At;
                return Ok(Some(((k, v), leaf)));
            }
        }
        self.reset(State::Unset);
        Ok(None)
    }

    /// [`Self::next`] (`forward`) or [`Self::prev`].
    fn advance<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
    ) -> Result<Landing<'s>> {
        match (self.state, forward) {
            (State::Unset | State::BeforeStart, true) => self.start(store, root, Edge::First),
            (State::Unset | State::AfterEnd, false) => self.start(store, root, Edge::Last),
            (State::AfterEnd, true) | (State::BeforeStart, false) => Ok(None),
            (State::At, _) => self.step(store, root, forward),
        }
    }

    /// Descend from the root along `edge` and settle on an entry: forward from where a
    /// [`Edge::First`] or [`Edge::Key`] descent lands, backward for [`Edge::Last`].
    fn start<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        edge: Edge<'_>,
    ) -> Result<Landing<'s>> {
        self.reset(State::Unset);
        root.check()?;
        let forward = !matches!(edge, Edge::Last);
        if root.is_empty() {
            self.state = if forward { State::AfterEnd } else { State::BeforeStart };
            return Ok(None);
        }
        let leaf = self.descend(store, root, root.root, root.root_txn, edge)?;
        self.land(store, root, leaf, forward)
    }

    /// From the entry the cursor is at, move one entry forward or backward, crossing to the
    /// adjacent leaf through the parents at the end of this one.
    fn step<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
    ) -> Result<Landing<'s>> {
        let leaf = store.page(self.path[self.depth - 1].0)?;
        if let Some(entry) = self.step_in_leaf(leaf, forward)? {
            return Ok(Some((entry, leaf)));
        }
        self.cross(store, root, forward)
    }

    /// At the end of the current leaf in the direction of travel: cross to the nearest entry
    /// of the leaves beyond it, else after the end (forward) or before the start (backward).
    fn cross<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
    ) -> Result<Landing<'s>> {
        match self.adjacent_leaf(store, root, forward)? {
            Some(leaf) => self.land(store, root, leaf, forward),
            None => {
                self.reset(if forward { State::AfterEnd } else { State::BeforeStart });
                Ok(None)
            }
        }
    }

    /// Settle on the leaf just entered: at its slot if that names an entry, else at the nearest
    /// entry of the leaves beyond it in the direction of travel, else after the end (forward) or
    /// before the start (backward).
    fn land<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        mut leaf: &'s [u8],
        forward: bool,
    ) -> Result<Landing<'s>> {
        loop {
            let slot = self.path[self.depth - 1].1;
            if slot < page::nkeys(leaf) {
                self.state = State::At;
                return Ok(Some((page::leaf_entry(leaf, slot)?, leaf)));
            }
            match self.adjacent_leaf(store, root, forward)? {
                Some(next) => leaf = next,
                None => {
                    self.reset(if forward { State::AfterEnd } else { State::BeforeStart });
                    return Ok(None);
                }
            }
        }
    }

    /// Leave the current leaf for the next (forward) or previous leaf through the parents,
    /// landing on its first or last entry; `None` (the path emptied) if there is none.
    fn adjacent_leaf<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        self.depth -= 1;
        while self.depth > 0 {
            let (pgno, ci) = self.path[self.depth - 1];
            let branch = store.page(pgno)?;
            let to = if forward {
                (ci + 1 < page::nkeys(branch)).then_some(ci + 1)
            } else {
                ci.checked_sub(1)
            };
            if let Some(ci) = to {
                self.path[self.depth - 1].1 = ci;
                let (child, child_txn) = page::branch_child(branch, ci)?;
                let edge = if forward { Edge::First } else { Edge::Last };
                return self.descend(store, root, child, child_txn, edge).map(Some);
            }
            self.depth -= 1;
        }
        Ok(None)
    }

    /// Descend from page `pgno` (`want_txn` the low 32 bits of its txn) — the root when the path
    /// is empty, else the child the path's last branch names — to a leaf, taking the child `edge`
    /// picks at each branch and pushing every page entered with its slot. Each page gets the
    /// checks of the module docs; damage is [`Error::Corrupt`]. Returns the leaf.
    fn descend<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        mut pgno: u32,
        mut want_txn: u32,
        edge: Edge<'_>,
    ) -> Result<&'s [u8]> {
        let (mut lo, mut hi) = self.bounds(store)?;
        loop {
            let depth = self.depth + 1;
            if depth > self.path.len() || depth as u32 > root.height {
                return Err(Error::Corrupt(format!(
                    "cursor descent past the height {} of the tree",
                    root.height
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
            let at_leaf_level = depth as u32 == root.height;
            let kind = page::kind(page)?;
            if (kind == Kind::Leaf) != at_leaf_level {
                return Err(Error::Corrupt(format!(
                    "page {pgno} is a {kind:?} at depth {depth} of a tree of height {}",
                    root.height
                )));
            }
            if !page::keys_in_range(page, lo, hi)? {
                return Err(Error::Corrupt(format!(
                    "page {pgno} holds keys outside its parents' separators (a misrouted pointer)"
                )));
            }
            let n = page::nkeys(page);
            if at_leaf_level {
                if depth > 1 && n == 0 {
                    return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
                }
                let slot = match edge {
                    Edge::First => 0,
                    Edge::Last => n.saturating_sub(1),
                    Edge::Key(key) => match page::leaf_search(page, key)? {
                        Search::Found(i) | Search::NotFound(i) => i,
                    },
                };
                self.push(pgno, slot);
                return Ok(page);
            }
            if n == 0 {
                return Err(Error::Corrupt(format!("branch page {pgno} has no children")));
            }
            let route = match edge {
                Edge::First => page::branch_route_at(page, 0)?,
                Edge::Last => page::branch_route_at(page, n - 1)?,
                Edge::Key(key) => page::branch_route(page, key)?,
            };
            tighten(&route, &mut lo, &mut hi);
            self.push(pgno, route.index);
            (pgno, want_txn) = (route.child, route.child_txn);
        }
    }

    /// The range `[lo, hi)` the path's branches give the page below them (`None`: unbounded):
    /// the intersection of each branch's range for the child taken. In a sound tree the nearest
    /// branch's range is the tightest, but a pointer misrouted to another branch brings that
    /// branch's separators, which can be wider.
    fn bounds<'s, S: PageRead + ?Sized>(&self, store: &'s S) -> Result<Bounds<'s>> {
        let (mut lo, mut hi): Bounds<'s> = (None, None);
        for &(pgno, ci) in &self.path[..self.depth] {
            tighten(&page::branch_route_at(store.page(pgno)?, ci)?, &mut lo, &mut hi);
        }
        Ok((lo, hi))
    }

    /// Append `(pgno, slot)` to the path (the caller checked the depth).
    fn push(&mut self, pgno: u32, slot: usize) {
        self.path[self.depth] = (pgno, slot);
        self.depth += 1;
    }
}

/// A cursor over one table in a transaction's state: ordered access to its entries in both
/// directions, borrowed from the transaction (and the entries it returns with it). Created by
/// [`crate::ReadTxn::cursor`]; a write transaction's cursor sees its own changes.
///
/// A cursor is at an entry, or unset, after the end, or before the start. A new cursor is unset.
/// [`Self::next`] from an unset cursor or before the start moves to the first entry, and
/// [`Self::prev`] from an unset cursor or after the end to the last; stepping past the last entry
/// leaves it after the end, and past the first entry before the start. A [`Self::seek`] that finds
/// nothing leaves it after the end, a [`Self::seek_exact`] that finds nothing leaves it unset, and
/// on an empty table [`Self::first`] leaves it after the end and [`Self::last`] before the start.
///
/// Damage a move finds is [`Error::Corrupt`], leaves the cursor unset and latches the environment
/// (later writes fail; reads continue).
pub struct Cursor<'t> {
    /// The transaction's page store.
    store: &'t (dyn PageRead + Sync),
    /// The table's tree in the transaction's state.
    root: TreeRoot,
    /// The environment's failure latch.
    latch: &'t Latch,
    tree: TreeCursor,
    /// The page of the leaf `tree` is at, while it is at an entry: a step inside that leaf skips
    /// the fetch. The page passed every check when the cursor entered it, and the transaction's
    /// pages cannot change while the cursor borrows it.
    leaf: Option<&'t [u8]>,
}

impl std::fmt::Debug for Cursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
            .field("state", &self.tree.state)
            .field("entries", &self.root.entries)
            .finish_non_exhaustive()
    }
}

impl<'t> Cursor<'t> {
    /// A new, unset cursor over the tree `root` in `store`, latching damage on `latch`.
    pub(crate) fn new(store: &'t (dyn PageRead + Sync), root: TreeRoot, latch: &'t Latch) -> Self {
        Self { store, root, latch, tree: TreeCursor::default(), leaf: None }
    }

    /// Test-only: where the cursor is.
    #[cfg(test)]
    pub(crate) fn state(&self) -> State {
        self.tree.state()
    }

    /// Latch damage (or any other failure) `result` carries.
    fn latched<T>(&self, result: Result<T>) -> Result<T> {
        result.inspect_err(|e| self.latch.latch_error(e))
    }

    /// Finish a move of the tree cursor: latch a failure, and remember the leaf it landed in
    /// (the move fetched it with every check).
    fn track(&mut self, moved: Result<Landing<'t>>) -> Result<Option<Entry<'t>>> {
        self.leaf = None;
        let landing = self.latched(moved)?;
        self.leaf = landing.map(|(_, leaf)| leaf);
        Ok(entry(landing))
    }

    /// Move one entry forward or backward: inside the remembered leaf without a fetch, else
    /// through the tree cursor (crossing leaves through their parents; from the remembered
    /// leaf's end straight to the crossing).
    fn step(&mut self, forward: bool) -> Result<Option<(&'t [u8], &'t [u8])>> {
        let (store, root) = (self.store, &self.root);
        let moved = match self.leaf {
            Some(leaf) => match self.tree.step_in_leaf(leaf, forward) {
                Ok(Some(entry)) => return Ok(Some(entry)),
                Ok(None) => self.tree.guarded(|c| c.cross(store, root, forward)),
                Err(e) => Err(e),
            },
            None => self.tree.guarded(|c| c.advance(store, root, forward)),
        };
        self.track(moved)
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&'t [u8], &'t [u8])>> {
        let (store, root) = (self.store, &self.root);
        let moved = self.tree.guarded(|c| c.start(store, root, Edge::First));
        self.track(moved)
    }

    /// Move to the last entry and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&'t [u8], &'t [u8])>> {
        let (store, root) = (self.store, &self.root);
        let moved = self.tree.guarded(|c| c.start(store, root, Edge::Last));
        self.track(moved)
    }

    /// Move to the next entry and return it (see the type docs for an unset cursor and the
    /// ends); `None` past the last entry.
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step(true)
    }

    /// Move to the previous entry and return it (see the type docs for an unset cursor and the
    /// ends); `None` before the first entry.
    pub fn prev(&mut self) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step(false)
    }

    /// The entry the cursor is at, if it is at one. The cursor does not move.
    pub fn current(&self) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.latched(self.tree.current(self.store))
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none.
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&'t [u8], &'t [u8])>> {
        let (store, root) = (self.store, &self.root);
        let moved = self.tree.guarded(|c| c.start(store, root, Edge::Key(key)));
        self.track(moved)
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&'t [u8]>> {
        let (store, root) = (self.store, &self.root);
        let moved = self.tree.guarded(|c| c.find(store, root, key));
        Ok(self.track(moved)?.map(|(_, value)| value))
    }
}

#[cfg(test)]
mod tests;
