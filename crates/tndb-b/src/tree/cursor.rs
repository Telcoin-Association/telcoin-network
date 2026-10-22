//! Bidirectional cursors over one tree.
//!
//! A cursor's position is the path from the root to the current leaf, as a page number and a slot
//! per level: for a branch the slot is the child being visited, for the leaf it is the current
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
//!
//! The descent parses each page's header once ([`page::View`]), after the fetch checks and the
//! kind check, and makes the range check, the empty-page checks and the search through that one
//! view, in that order. It hands the leaf's view to the move that settles there, which reads the
//! entry through it: an exact seek's hit is the search's own result ([`Find::Found`]), not a
//! second read of the cell.

use super::{read::tighten, PageRead, TreeRoot, MAX_HEIGHT};
use crate::{
    error::{Error, Result},
    latch::Latch,
    page::{self, Find, Kind, LeafView, Search, Val, View},
};

/// A `(key, value)` entry borrowed from a page store.
pub(crate) type Entry<'s> = (&'s [u8], &'s [u8]);

/// Entry `i` of `leaf` with its value as a reader sees it: an overflow value read from its run
/// ([`PageRead::value`]).
#[inline]
fn leaf_entry<'s, S: PageRead + ?Sized>(
    store: &'s S,
    leaf: &'s [u8],
    i: usize,
) -> Result<Entry<'s>> {
    let (k, v) = page::leaf_entry(leaf, i)?;
    Ok((k, store.value(v)?))
}

/// [`leaf_entry`] through the view of a leaf whose header a descent parsed. Forced inline, as
/// [`leaf_entry`]'s page read is: an out-of-line call here costs a leaf crossing more than the
/// header parse it saves.
#[inline(always)]
fn view_entry<'s, S: PageRead + ?Sized>(
    store: &'s S,
    leaf: &LeafView<'s>,
    i: usize,
) -> Result<Entry<'s>> {
    let (k, v) = leaf.entry(i)?;
    Ok((k, store.value(v)?))
}

/// A cursor entry's value with the last bytes of its key before it, as a `current_with_key_tail`
/// returns it ([`Cursor::current_with_key_tail`], [`crate::RoCursor::current_with_key_tail`],
/// [`crate::TxnCursor::current_with_key_tail`]). A leaf cell holds its value right after its key,
/// so for an inline value the key's tail and the value are one slice of the page, read without a
/// copy; a value in an overflow run is not next to its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyTail<'a> {
    /// An inline value: the key's last `t` bytes followed by the value, one slice.
    Joined(&'a [u8]),
    /// A value in an overflow run: the value alone, for the caller to join to the key's tail.
    Apart(&'a [u8]),
}

/// Entry `i` of `leaf` as its key and its value joined to the key's last `t` bytes
/// ([`KeyTail`]; `t` past the key's length is the key's length).
#[inline]
fn leaf_entry_with_key_tail<'s, S: PageRead + ?Sized>(
    store: &'s S,
    leaf: &'s [u8],
    i: usize,
    t: usize,
) -> Result<(&'s [u8], KeyTail<'s>)> {
    key_tail(store, page::leaf_entry_with_key_tail(leaf, i, t)?)
}

/// An entry as [`page::LeafView::entry_with_key_tail`] gives it, with an overflow value read from
/// its run ([`KeyTail`]).
#[inline]
fn key_tail<'s, S: PageRead + ?Sized>(
    store: &'s S,
    (key, val): (&'s [u8], Val<'s>),
) -> Result<(&'s [u8], KeyTail<'s>)> {
    let tail = match val {
        Val::Inline(joined) => KeyTail::Joined(joined),
        Val::Overflow(ovf) => KeyTail::Apart(store.overflow(ovf)?),
    };
    Ok((key, tail))
}

/// A key range `[lo, hi)` borrowed from a page store (`None`: unbounded).
type Bounds<'s> = (Option<&'s [u8]>, Option<&'s [u8]>);

/// Where a move landed: the entry the cursor is now at, with the page of the leaf holding it
/// (`None`: not at an entry).
pub(crate) type Landing<'s> = Option<(Entry<'s>, &'s [u8])>;

/// The entry of a [`Landing`].
pub(crate) fn entry(landing: Landing<'_>) -> Option<Entry<'_>> {
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
    /// [`Edge::Key`], and in the leaf the entry equal to the key, whose value the descent
    /// returns ([`Reached`]).
    Exact(&'k [u8]),
}

/// The leaf a descent reached: its view, the one header parse its checks and the search went
/// through, and for an [`Edge::Exact`] descent that found the key the entry's value as its cell
/// holds it.
type Reached<'s> = (LeafView<'s>, Option<Val<'s>>);

/// The position of a cursor over one tree, detached from it: each move takes the page store and
/// the tree's root (see the module docs). An error leaves it [`State::Unset`].
///
/// The path is two arrays, not one of pairs, so it has no padding: every new cursor zeroes it and
/// its creation moves it, so its size is a cursor's cost (`tree_cursor_stays_small`).
#[derive(Clone, Debug)]
pub(crate) struct TreeCursor {
    /// The path from the root, as page numbers: the branches entered, then the leaf.
    pages: [u32; MAX_HEIGHT as usize],
    /// The slot taken at each page of `pages`: for a branch the child visited, for the leaf the
    /// current entry. A slot is at most its page's cell count, which the header holds as a `u16`
    /// ([`page::nkeys`]; a search's insertion point is at most that count), so it fits in one.
    slots: [u16; MAX_HEIGHT as usize],
    /// Levels of the path in use (the tree's height while [`State::At`]).
    depth: usize,
    state: State,
}

impl Default for TreeCursor {
    fn default() -> Self {
        Self {
            pages: [0; MAX_HEIGHT as usize],
            slots: [0; MAX_HEIGHT as usize],
            depth: 0,
            state: State::Unset,
        }
    }
}

/// A slot as the path holds it (see [`TreeCursor`]'s `slots`).
#[inline]
fn narrow(slot: usize) -> u16 {
    debug_assert!(u16::try_from(slot).is_ok(), "slot {slot}");
    slot as u16
}

impl TreeCursor {
    /// Where the cursor is.
    pub(crate) fn state(&self) -> State {
        self.state
    }

    /// The last step of the path: the leaf and the entry while the cursor is at one.
    #[inline]
    fn top(&self) -> (u32, usize) {
        let i = self.depth - 1;
        (self.pages[i], usize::from(self.slots[i]))
    }

    /// Set the slot of the path's last step.
    #[inline]
    fn set_top_slot(&mut self, slot: usize) {
        self.slots[self.depth - 1] = narrow(slot);
    }

    /// Move one entry forward or backward inside `leaf`, the page of the leaf the cursor is at
    /// (fetched and checked when the cursor entered it). `None`, the cursor unmoved, when the
    /// move would leave the leaf or the cursor is not at an entry.
    #[inline]
    pub(crate) fn step_in_leaf<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        leaf: &'s [u8],
        forward: bool,
    ) -> Result<Option<Entry<'s>>> {
        if self.state != State::At {
            return Ok(None);
        }
        let slot = self.top().1;
        let to = if forward {
            (slot + 1 < page::nkeys(leaf)).then_some(slot + 1)
        } else {
            slot.checked_sub(1)
        };
        let Some(to) = to else { return Ok(None) };
        self.guarded(|c| {
            c.set_top_slot(to);
            Ok(Some(leaf_entry(store, leaf, to)?))
        })
    }

    /// Place the cursor at entry `slot` of `leaf`, below `branches` (root first, as `(pgno, child
    /// index taken)`), without reading a page: the caller is the writer that just put that entry
    /// there, on that path of its own pages, so a descent for the entry's key would take exactly
    /// this path.
    pub(crate) fn place(&mut self, branches: &[(u32, usize)], leaf: u32, slot: usize) {
        let depth = branches.len() + 1;
        let steps = self.pages.iter_mut().zip(&mut self.slots);
        for ((page, step), &(pgno, ci)) in steps.zip(branches) {
            (*page, *step) = (pgno, narrow(ci));
        }
        (self.pages[branches.len()], self.slots[branches.len()]) = (leaf, narrow(slot));
        self.depth = depth;
        self.state = State::At;
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
        let (pgno, slot) = self.top();
        Ok(Some(leaf_entry(store, store.page(pgno)?, slot)?))
    }

    /// The entry the cursor is at, if it is at one, as its key and its value joined to the key's
    /// last `t` bytes ([`KeyTail`]). `leaf` is the page of the leaf the cursor is at, when the
    /// caller holds it (else it is fetched, as [`Self::current`] fetches it).
    #[inline]
    pub(crate) fn current_with_key_tail<'s, S: PageRead + ?Sized>(
        &self,
        store: &'s S,
        leaf: Option<&'s [u8]>,
        t: usize,
    ) -> Result<Option<(&'s [u8], KeyTail<'s>)>> {
        if self.state != State::At {
            return Ok(None);
        }
        let (pgno, slot) = self.top();
        let leaf = match leaf {
            Some(leaf) => leaf,
            None => store.page(pgno)?,
        };
        Ok(Some(leaf_entry_with_key_tail(store, leaf, slot, t)?))
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

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`]: move to the first entry `>=
    /// key` (after the end if there is none) and return it as its key and its value joined to the
    /// key's last `t` bytes ([`KeyTail`]). The entry is read through the view the descent parsed,
    /// so the leaf is fetched and its header parsed once, and an overflow value read once.
    pub(crate) fn seek_with_key_tail<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&'s [u8], KeyTail<'s>)>> {
        let read = |leaf: &LeafView<'s>, i| key_tail(store, leaf.entry_with_key_tail(i, t)?);
        let landed = self.guarded(|c| c.start_with(store, root, Edge::Key(key), read))?;
        Ok(landed.map(|(entry, _)| entry))
    }

    /// Move to the entry `key` and return its value; unset if there is none.
    pub(crate) fn seek_exact<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        Ok(self.guarded(|c| c.find(store, root, key))?.map(|(value, _)| value))
    }

    /// Move one entry forward (`forward`) or backward only if the entry there shares the first
    /// `n` bytes of the current entry's key, returning it with its leaf's page; else `None`, the
    /// cursor unmoved (see [`Cursor::next_in_prefix`] for the edge rules). `leaf` is the page of
    /// the leaf the cursor is at, when the caller holds it (else it is fetched). Inside the leaf
    /// the follower's key is compared before its value is read; at the leaf's end a copy of the
    /// cursor crosses, and the cursor takes its place only if the entry it lands on shares the
    /// prefix.
    pub(crate) fn step_in_prefix<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        leaf: Option<&'s [u8]>,
        n: usize,
        forward: bool,
    ) -> Result<Landing<'s>> {
        if self.state != State::At {
            return Ok(None);
        }
        self.guarded(|c| {
            let (pgno, slot) = c.top();
            let leaf = match leaf {
                Some(leaf) => leaf,
                None => store.page(pgno)?,
            };
            let Some(prefix) = page::leaf_key(leaf, slot)?.get(..n) else {
                return Ok(None);
            };
            let to = if forward {
                (slot + 1 < page::nkeys(leaf)).then_some(slot + 1)
            } else {
                slot.checked_sub(1)
            };
            match to {
                Some(to) => {
                    let (k, v) = page::leaf_entry(leaf, to)?;
                    if !k.starts_with(prefix) {
                        return Ok(None);
                    }
                    let value = store.value(v)?;
                    c.set_top_slot(to);
                    Ok(Some(((k, value), leaf)))
                }
                None => c.move_if(prefix, |peek| peek.cross(store, root, forward)),
            }
        })
    }

    /// Move to the next (`forward`) or previous entry, as [`Self::next`] and [`Self::prev`] do,
    /// only if its key starts with `prefix`; else `None`, the cursor unmoved. An error leaves the
    /// cursor unset.
    pub(crate) fn step_if<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        prefix: &[u8],
        forward: bool,
    ) -> Result<Landing<'s>> {
        self.guarded(|c| c.move_if(prefix, |peek| peek.advance(store, root, forward)))
    }

    /// Make the move `op` on a copy of the cursor, and take the copy's place only if it landed
    /// on an entry whose key starts with `prefix`.
    fn move_if<'s>(
        &mut self,
        prefix: &[u8],
        op: impl FnOnce(&mut Self) -> Result<Landing<'s>>,
    ) -> Result<Landing<'s>> {
        let mut peek = self.clone();
        match op(&mut peek)? {
            Some(landing) if landing.0 .0.starts_with(prefix) => {
                *self = peek;
                Ok(Some(landing))
            }
            _ => Ok(None),
        }
    }

    /// The page number of the leaf the cursor is at, if it is at an entry.
    pub(crate) fn leaf_pgno(&self) -> Option<u32> {
        (self.state == State::At).then(|| self.pages[self.depth - 1])
    }

    /// [`Self::next`] (`forward`) or [`Self::prev`] for a cursor at an entry, from `leaf`: the page
    /// of the leaf it is at, as the cursor fetched it with every check when it entered it.
    pub(crate) fn step_from<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        leaf: &'s [u8],
        forward: bool,
    ) -> Result<Option<Entry<'s>>> {
        if let Some(entry) = self.step_in_leaf(store, leaf, forward)? {
            return Ok(Some(entry));
        }
        self.guarded(|c| c.cross(store, root, forward)).map(entry)
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

    /// [`Self::seek_exact`]: land on the entry `key` and return its value with the page of its
    /// leaf, or leave the cursor unset. The value is the one the descent's search found.
    fn find<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        key: &[u8],
    ) -> Result<Option<(&'s [u8], &'s [u8])>> {
        self.reset(State::Unset);
        root.check()?;
        if root.is_empty() {
            return Ok(None);
        }
        match self.descend(store, root, root.root, root.root_txn, Edge::Exact(key))? {
            (leaf, Some(value)) => {
                self.state = State::At;
                Ok(Some((store.value(value)?, leaf.page())))
            }
            (_, None) => {
                self.reset(State::Unset);
                Ok(None)
            }
        }
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
    /// [`Edge::First`] or [`Edge::Key`] descent lands, backward for [`Edge::Last`]. An
    /// [`Edge::Exact`] descent never comes here: [`Self::find`] makes it, as a miss leaves the
    /// cursor unset rather than settled on the next entry, and a hit takes the search's value.
    fn start<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        edge: Edge<'_>,
    ) -> Result<Landing<'s>> {
        self.start_with(store, root, edge, |leaf, i| view_entry(store, leaf, i))
    }

    /// [`Self::start`], the entry settled on given by `read(leaf, slot)`.
    fn start_with<'s, S: PageRead + ?Sized, T>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        edge: Edge<'_>,
        read: impl Fn(&LeafView<'s>, usize) -> Result<T>,
    ) -> Result<Option<(T, &'s [u8])>> {
        self.reset(State::Unset);
        root.check()?;
        let forward = !matches!(edge, Edge::Last);
        if root.is_empty() {
            self.state = if forward { State::AfterEnd } else { State::BeforeStart };
            return Ok(None);
        }
        let (leaf, _) = self.descend(store, root, root.root, root.root_txn, edge)?;
        self.land(store, root, leaf, forward, read)
    }

    /// From the entry the cursor is at, move one entry forward or backward, crossing to the
    /// adjacent leaf through the parents at the end of this one.
    fn step<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
    ) -> Result<Landing<'s>> {
        let leaf = store.page(self.pages[self.depth - 1])?;
        if let Some(entry) = self.step_in_leaf(store, leaf, forward)? {
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
        self.land_beyond(store, root, forward, |leaf, i| view_entry(store, leaf, i))
    }

    /// Settle on the leaf just entered, `leaf` the view its descent parsed: at its slot if that
    /// names an entry, else as [`Self::land_beyond`]. The entry settled on is `read(leaf, slot)`,
    /// returned with the leaf's page.
    fn land<'s, S: PageRead + ?Sized, T>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        leaf: LeafView<'s>,
        forward: bool,
        read: impl Fn(&LeafView<'s>, usize) -> Result<T>,
    ) -> Result<Option<(T, &'s [u8])>> {
        let slot = self.top().1;
        if slot < leaf.nkeys() {
            self.state = State::At;
            return Ok(Some((read(&leaf, slot)?, leaf.page())));
        }
        self.land_beyond(store, root, forward, read)
    }

    /// Settle on the nearest entry of the leaves beyond the current one in the direction of
    /// travel, `read(leaf, slot)`, returned with the leaf's page; else after the end (forward) or
    /// before the start (backward). Each leaf entered is the next (forward) or previous leaf
    /// through the parents ([`Self::climb`]), at its first or last entry.
    ///
    /// The view goes from the descent's result straight to its reads, never through another
    /// `Option` or `Result`: repacking it costs a stalled copy per crossing.
    fn land_beyond<'s, S: PageRead + ?Sized, T>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        forward: bool,
        read: impl Fn(&LeafView<'s>, usize) -> Result<T>,
    ) -> Result<Option<(T, &'s [u8])>> {
        let edge = if forward { Edge::First } else { Edge::Last };
        loop {
            let Some((child, child_txn)) = self.climb(store, forward)? else {
                self.reset(if forward { State::AfterEnd } else { State::BeforeStart });
                return Ok(None);
            };
            let (leaf, _) = self.descend(store, root, child, child_txn, edge)?;
            let slot = self.top().1;
            if slot < leaf.nkeys() {
                self.state = State::At;
                return Ok(Some((read(&leaf, slot)?, leaf.page())));
            }
        }
    }

    /// Leave the current leaf through the parents: pop it, climb to the nearest branch with a
    /// child beyond the path's in the direction of travel, take that child and return its pointer
    /// `(pgno, child_txn)` for the descent to the next (forward) or previous leaf; `None` (the
    /// path emptied) if there is none.
    fn climb<S: PageRead + ?Sized>(
        &mut self,
        store: &S,
        forward: bool,
    ) -> Result<Option<(u32, u32)>> {
        self.depth -= 1;
        while self.depth > 0 {
            let (pgno, ci) = self.top();
            let branch = store.page(pgno)?;
            let to = if forward {
                (ci + 1 < page::nkeys(branch)).then_some(ci + 1)
            } else {
                ci.checked_sub(1)
            };
            if let Some(ci) = to {
                self.set_top_slot(ci);
                return page::branch_child(branch, ci).map(Some);
            }
            self.depth -= 1;
        }
        Ok(None)
    }

    /// Descend from page `pgno` (`want_txn` the low 32 bits of its txn) — the root when the path
    /// is empty, else the child the path's last branch names — to a leaf, taking the child `edge`
    /// picks at each branch and pushing every page entered with its slot. Each page gets the
    /// checks of the module docs; damage is [`Error::Corrupt`]. Returns the leaf's view, with the
    /// value an [`Edge::Exact`] search found.
    ///
    /// Per page, in this order: the fetch checks and the identity check ([`PageRead::child`]),
    /// the kind its level asks for, the one header parse ([`View::new`]), the separator range,
    /// the empty-page checks, and the search, each read after the parse going through its view.
    fn descend<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        root: &TreeRoot,
        mut pgno: u32,
        mut want_txn: u32,
        edge: Edge<'_>,
    ) -> Result<Reached<'s>> {
        let (mut lo, mut hi) = self.bounds(store)?;
        loop {
            let depth = self.depth + 1;
            if depth > self.pages.len() || depth as u32 > root.height {
                return Err(Error::Corrupt(format!(
                    "cursor descent past the height {} of the tree",
                    root.height
                )));
            }
            let page = store.child(pgno, want_txn)?;
            let at_leaf_level = depth as u32 == root.height;
            let kind = page::kind(page)?;
            if (kind == Kind::Leaf) != at_leaf_level {
                return Err(Error::Corrupt(format!(
                    "page {pgno} is a {kind:?} at depth {depth} of a tree of height {}",
                    root.height
                )));
            }
            // The kind byte passed above, so the view is a leaf's exactly at the leaf level.
            let view = View::new(page)?;
            if !view.keys_in_range(lo, hi)? {
                return Err(Error::Corrupt(format!(
                    "page {pgno} holds keys outside its parents' separators (a misrouted pointer)"
                )));
            }
            let branch = match view {
                View::Leaf(leaf) => {
                    let n = leaf.nkeys();
                    if depth > 1 && n == 0 {
                        return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
                    }
                    let (slot, hit) = match edge {
                        Edge::First => (0, None),
                        Edge::Last => (n.saturating_sub(1), None),
                        Edge::Key(key) => match leaf.search(key)? {
                            Search::Found(i) | Search::NotFound(i) => (i, None),
                        },
                        Edge::Exact(key) => match leaf.find(key)? {
                            Find::Found { index, value } => (index, Some(value)),
                            Find::NotFound { index, .. } => (index, None),
                        },
                    };
                    self.push(pgno, slot);
                    return Ok((leaf, hit));
                }
                View::Branch(branch) => branch,
            };
            let n = branch.nkeys();
            if n == 0 {
                return Err(Error::Corrupt(format!("branch page {pgno} has no children")));
            }
            let route = match edge {
                Edge::First => branch.route_at(0)?,
                Edge::Last => branch.route_at(n - 1)?,
                Edge::Key(key) | Edge::Exact(key) => branch.route(key)?,
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
        for (&pgno, &ci) in self.pages[..self.depth].iter().zip(&self.slots[..self.depth]) {
            let route = page::branch_route_at(store.page(pgno)?, usize::from(ci))?;
            tighten(&route, &mut lo, &mut hi);
        }
        Ok((lo, hi))
    }

    /// Append `(pgno, slot)` to the path (the caller checked the depth). Forced inline: a call per
    /// level of every descent otherwise.
    #[inline(always)]
    fn push(&mut self, pgno: u32, slot: usize) {
        (self.pages[self.depth], self.slots[self.depth]) = (pgno, narrow(slot));
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
    fn track<T>(&mut self, moved: Result<Option<(T, &'t [u8])>>) -> Result<Option<T>> {
        self.leaf = None;
        let landing = self.latched(moved)?;
        Ok(landing.map(|(found, leaf)| {
            self.leaf = Some(leaf);
            found
        }))
    }

    /// Move one entry forward or backward: inside the remembered leaf without a fetch, else
    /// through the tree cursor (crossing leaves through their parents; from the remembered
    /// leaf's end straight to the crossing).
    fn step(&mut self, forward: bool) -> Result<Option<(&'t [u8], &'t [u8])>> {
        let (store, root) = (self.store, &self.root);
        let moved = match self.leaf {
            Some(leaf) => match self.tree.step_in_leaf(store, leaf, forward) {
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

    /// The entry the cursor is at, if it is at one, as its key and its value with the key's last
    /// `t` bytes before it: for an inline value one slice of the page, `key[key.len() − t..] ‖
    /// value` ([`KeyTail::Joined`]), for a value in an overflow run the value alone
    /// ([`KeyTail::Apart`]). A `t` past the key's length is the key's length (the slice is then
    /// `key ‖ value`). The cursor does not move.
    pub fn current_with_key_tail(&self, t: usize) -> Result<Option<(&'t [u8], KeyTail<'t>)>> {
        self.latched(self.tree.current_with_key_tail(self.store, self.leaf, t))
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
        self.track(moved)
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay at the current entry. For keys built as
    /// `key ‖ subkey`, `next_in_prefix(key.len())` steps through one key's subkeys.
    ///
    /// - Not at an entry (unset, after the end, before the start): `None`, the cursor unmoved.
    /// - `n` longer than the current entry's key: that prefix has no other entry, so `None`.
    /// - `n == 0`: every key shares the empty prefix, so this is [`Self::next`], except that at the
    ///   last entry it returns `None` and stays there.
    ///
    /// At the end of a leaf the move peeks into the next leaf without losing the position.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(n, true)
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay at the current entry. The edge
    /// rules are [`Self::next_in_prefix`]'s, backward: with `n == 0` this is [`Self::prev`],
    /// except that at the first entry it returns `None` and stays there.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`], from the remembered
    /// leaf. A `None` leaves the cursor, so its leaf, where it was.
    fn step_in_prefix(&mut self, n: usize, forward: bool) -> Result<Option<Entry<'t>>> {
        let (store, root) = (self.store, &self.root);
        match self.tree.step_in_prefix(store, root, self.leaf, n, forward) {
            Ok(Some((entry, leaf))) => {
                self.leaf = Some(leaf);
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            moved @ Err(_) => self.track(moved),
        }
    }
}

#[cfg(test)]
mod tests;
