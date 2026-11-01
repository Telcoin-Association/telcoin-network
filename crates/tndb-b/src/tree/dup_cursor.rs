//! The walker of a dup table's cursors: one position over the table's rows, the logical keys
//! `k ‖ s` in order, made of two tree positions ([`TreeCursor`]): the outer tree's, at the outer
//! key `k` whose nested cell names `k`'s nested tree, and that nested tree's, at the subkey `s`.
//! The rows are those of the composite twin (a plain table keyed `k ‖ s`), and every move gives
//! what the same move gives on the twin (the dup cursors' type docs list the exceptions).
//!
//! The walker is detached as [`TreeCursor`] is: each move takes the page store, the outer tree's
//! root and the source of the nested roots ([`NestedRoots`]: a snapshot's cells, or a write
//! transaction's working map before its cells), so one walker serves a reader's cursor and a write
//! transaction's. The outer tree is read through [`Cells`] (an outer cell as it is), and the
//! walker checks every outer key it lands on before it reads the key's cell: the table's key
//! width, then the cell, a nested cell ([`nested_cell`]) or, in a table that packs
//! ([`NestedRoots::packed_max`] not 0), an inline cell holding the key's packed set
//! ([`crate::packed`]). The nested trees are plain trees ([`super::cursor::Values`]).
//!
//! **Packed mode.** At a row of a packed key the nested path is unset and the walker holds the
//! set's row count and the row's index; each move re-reads the cell from the outer leaf it is at
//! and decodes it again (O(1), [`PackedView::new`]), so the walker borrows no page. A step inside
//! the set compares the new row's subkey with the previous one, so a walk over a key checks the
//! order of every row it passes; a landing (an edge row, a seek's) needs no compare. A nested
//! key's moves pay one compare of the row count for this mode, an absent key none.
//!
//! A write through a dup cursor at a row of the written key passes the walker's paths to the
//! write ([`DupWalk::hints`]): both paths' slots as routing hints, each followed only where the
//! write's descent finds it holds, and the outer leaf, where the write reads the key's cell when
//! the transaction's working map holds no root for the key.
//!
//! The logical key is not contiguous in any page (`k` sits in an outer leaf, `s` in a nested
//! leaf), so the walker keeps it in a buffer, rewritten past `k` on each move inside a key and
//! whole on a move to another key; its capacity is reused, so a move allocates nothing once the
//! buffer has grown to the longest key.

use super::{
    cursor::{Cells, KeyTail, State, TreeCursor},
    write, PageRead, TreeRoot,
};
use crate::{
    error::{Error, Result},
    packed::{packed_error, PackedView},
    page::{self, cmp_keys, LeafView, Search, Val},
};
use std::cmp::Ordering;

/// Where a dup walker gets the nested root of each outer key it lands on.
pub(crate) trait NestedRoots {
    /// The dup table's name, for the messages of damage.
    fn name(&self) -> &str;

    /// The nested root of outer key `k`, whose outer cell holds `cell`: a reader's from the cell
    /// itself, a write transaction's from its working map when the map holds `k`, never from a
    /// cell the map supersedes. A cell naming an empty tree is [`Error::Corrupt`].
    fn root(&mut self, k: &[u8], cell: &[u8; page::NESTED_CELL]) -> Result<TreeRoot>;

    /// The table's packed-set limit, its record's resolved `packed_max`, fixed for a cursor's
    /// life: an inline outer cell is a packed set of at most this many bytes when it is not 0, and
    /// [`Error::Corrupt`] at 0.
    fn packed_max(&self) -> usize;
}

/// The nested root bytes of outer key `k`'s cell `val` in the dup table `name`: any other cell is
/// [`Error::Corrupt`] (this build writes only nested cells into an outer tree).
pub(crate) fn nested_cell<'s>(
    name: &str,
    k: &[u8],
    val: Val<'s>,
) -> Result<&'s [u8; page::NESTED_CELL]> {
    match val {
        Val::Nested(b) => Ok(b),
        val => Err(refused_cell(name, k, val)),
    }
}

/// The error of outer key `k`'s cell `val` in the dup table `name` when it is not a cell the
/// table holds: an overflow cell in any dup table, an inline cell in one that does not pack.
#[cold]
#[inline(never)]
fn refused_cell(name: &str, k: &[u8], val: Val<'_>) -> Error {
    match val {
        Val::Overflow(_) => Error::Corrupt(format!(
            "dup table {name}: an overflow cell in a dup table's outer tree (key {k:02x?})"
        )),
        _ => Error::Corrupt(format!(
            "dup table {name}: the outer cell of key {k:02x?} holds no nested tree"
        )),
    }
}

/// How the walker is inside the key it is at: by the nested tree (with the page of the nested
/// leaf it is at, when a caller holds it), or by the key's packed set.
enum Inside<'s> {
    Nested(Option<&'s [u8]>),
    Packed(PackedView<'s>),
}

/// The error of an outer key of `len` bytes in the dup table `name` of key width `kl`: a damaged
/// record or tree, since no write makes a key of another width.
#[cold]
fn width_error(name: &str, len: usize, kl: usize) -> Error {
    Error::Corrupt(format!(
        "dup table {name}: an outer key of {len} bytes, not the table's width {kl}"
    ))
}

/// The page of the leaf `tree` is at, from `again` (a shell's fetch of a page its cursor entered
/// with every check), for a move from it; `None` when `tree` is not at an entry or `again` holds
/// no page.
#[inline]
fn held<'s>(
    tree: &TreeCursor,
    again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
) -> Result<Option<&'s [u8]>> {
    match tree.leaf_pgno() {
        Some(pgno) => again(pgno),
        None => Ok(None),
    }
}

/// A dup write's routing hints from a walker at a row of the written outer key
/// ([`DupWalk::hints`]): the outer path's slots for the write's outer descents, the nested path's
/// for its nested ones (each a [`write::Hint`]: slots only, each checked where it is followed),
/// and the outer leaf the walker is at, whose cell of the key the write may read there (the
/// working map first, always). [`NO_DUP_HINT`] for none.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DupHint<'h> {
    /// The outer path's slots, root first, the key's cell's slot last.
    pub(crate) outer: write::Hint<'h>,
    /// The page of the outer leaf the walker is at: the leaf of the outer tree holding the key's
    /// cell, entered with a descent's checks.
    pub(crate) outer_leaf: Option<u32>,
    /// The nested path's slots, root first; none at a packed key.
    pub(crate) nested: write::Hint<'h>,
}

/// No hints: every descent of a dup write searches, and its outer cell is found from the root.
pub(crate) const NO_DUP_HINT: DupHint<'static> =
    DupHint { outer: write::NO_HINT, outer_leaf: None, nested: write::NO_HINT };

impl DupHint<'_> {
    /// The outer leaf the walker is at and the slot of the key's cell in it, if it holds one.
    #[inline]
    pub(crate) fn held(&self) -> Option<(u32, usize)> {
        self.outer_leaf.zip(self.outer.last().map(|&slot| usize::from(slot)))
    }
}

/// A position in a dup table: the outer tree's path, the current outer key's nested tree and its
/// path, and the logical key `k ‖ s` of the row it is at (see the module docs).
///
/// Its state is the nested path's while the outer path is at an entry of a nested key, at a row
/// while it is at a packed key's, else the outer path's. After every move an outer path at an
/// entry sits over a nested path at an entry or over a packed row: the walker never stops at a
/// nested tree's or a packed set's end, it steps the outer path on. An error leaves both paths
/// unset.
#[derive(Clone, Debug)]
pub(crate) struct DupWalk {
    outer: TreeCursor,
    inner: TreeCursor,
    /// The current outer key's nested root (meaningful while the outer path is at an entry).
    nested: TreeRoot,
    /// `k ‖ s` while at a row.
    key: Vec<u8>,
    /// The table's key width `K`: every outer key's length.
    key_len: u16,
    /// The longest subkey a row of the table has ([`crate::page::Geometry::max_subkey`]).
    max_subkey: u16,
    /// The rows of the packed set of the key the walker is at; 0 while the key is nested (or the
    /// outer path at none). A set's row count is a `u16` of its encoding.
    packed_rows: u16,
    /// The packed row the walker is at (meaningful while `packed_rows` is not 0).
    packed_at: u16,
}

impl DupWalk {
    /// An unset walker over a dup table of outer keys of `key_len` bytes whose rows' subkeys have
    /// 1 to `max_subkey` bytes (a page's limit, held to `u16`: no page size has a longer key).
    pub(crate) fn new(key_len: u16, max_subkey: usize) -> Self {
        Self {
            outer: TreeCursor::default(),
            inner: TreeCursor::default(),
            nested: TreeRoot::EMPTY,
            key: Vec::new(),
            key_len,
            max_subkey: u16::try_from(max_subkey).unwrap_or(u16::MAX),
            packed_rows: 0,
            packed_at: 0,
        }
    }

    /// The key width `K`.
    #[inline]
    pub(crate) fn kl(&self) -> usize {
        usize::from(self.key_len)
    }

    /// `k` has the key width, else [`Error::KeyTooShort`] or [`Error::KeyTooLarge`]: a bounded
    /// seek's refusal, made before anything moves.
    #[inline]
    pub(crate) fn check_key(&self, k: &[u8]) -> Result<()> {
        let kl = self.kl();
        if k.len() < kl {
            return Err(Error::KeyTooShort { len: k.len(), min: kl });
        }
        if k.len() > kl {
            return Err(Error::KeyTooLarge { len: k.len(), max: kl });
        }
        Ok(())
    }

    /// Where the walker is.
    #[inline]
    pub(crate) fn state(&self) -> State {
        match self.outer.state() {
            State::At if self.packed_rows != 0 => State::At,
            State::At => self.inner.state(),
            outer => outer,
        }
    }

    /// The logical key of the row the walker is at (meaningful while [`State::At`]).
    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    /// The routing hints of a write into outer key `k` of the outer tree `outer` (the walker's,
    /// up to date): while the walker is at a row of `k`, the outer path's slots and leaf, and at
    /// a nested key the nested path's slots ([`TreeCursor::hint`]); else none. The caller vouches
    /// only that the paths may be the trees': the write's descents check every slot they follow.
    #[inline]
    pub(crate) fn hints(&self, k: &[u8], outer: &TreeRoot) -> DupHint<'_> {
        if self.state() != State::At || self.key.get(..self.kl()) != Some(k) {
            return NO_DUP_HINT;
        }
        let nested = if self.packed_rows == 0 {
            self.inner.hint(self.nested.height)
        } else {
            write::NO_HINT
        };
        DupHint { outer: self.outer.hint(outer.height), outer_leaf: self.outer.leaf_pgno(), nested }
    }

    /// Run a move; an error leaves both paths unset.
    #[inline]
    fn guarded<T>(&mut self, op: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let result = op(self);
        if result.is_err() {
            self.unset();
        }
        result
    }

    /// Forget both paths.
    #[inline]
    pub(crate) fn unset(&mut self) {
        self.outer.unset();
        self.inner.unset();
        self.packed_rows = 0;
    }

    /// Rewrite the logical key's subkey with the key of the nested row just landed on: 1 to
    /// `max_subkey` bytes, else [`Error::Corrupt`] (a damaged leaf: no write stores another
    /// length).
    #[inline]
    fn set_sub(&mut self, s: &[u8]) -> Result<()> {
        if !(1..=usize::from(self.max_subkey)).contains(&s.len()) {
            return Err(self.bad_subkey(s.len()));
        }
        self.key.truncate(self.kl());
        self.key.extend_from_slice(s);
        Ok(())
    }

    /// The error of a nested row whose subkey has `len` bytes, outside 1 to `max_subkey`.
    #[cold]
    fn bad_subkey(&self, len: usize) -> Error {
        Error::Corrupt(format!(
            "a subkey of {len} bytes under outer key {:02x?}, not 1 to {}",
            &self.key[..self.kl().min(self.key.len())],
            self.max_subkey
        ))
    }

    /// The outer path just landed on key `k` with cell `val`: check the key's width and its cell,
    /// write `k` into the logical key and say how the walker is inside the key: a nested key's
    /// root taken, the nested path left for the caller to land; or a packed key's set decoded, the
    /// nested path unset and the row left for the caller to pick. The nested arm comes first, so
    /// a nested key pays the match it paid before packing and one store.
    fn enter<'s, N: NestedRoots>(
        &mut self,
        roots: &mut N,
        k: &[u8],
        val: Val<'s>,
    ) -> Result<Inside<'s>> {
        self.check_width(roots, k)?;
        let inside = match val {
            Val::Nested(cell) => {
                self.nested = roots.root(k, cell)?;
                self.packed_rows = 0;
                Inside::Nested(None)
            }
            Val::Inline(b) if roots.packed_max() != 0 => self.enter_packed(roots, k, b)?,
            val => return Err(refused_cell(roots.name(), k, val)),
        };
        if self.key.capacity() == 0 {
            self.key.reserve(self.kl() + 64);
        }
        self.key.clear();
        self.key.extend_from_slice(k);
        Ok(inside)
    }

    /// [`Self::enter`]'s packed arm: decode outer key `k`'s set `b` with [`PackedView::new`]'s
    /// checks and unset the nested path.
    #[inline(never)]
    fn enter_packed<'s, N: NestedRoots>(
        &mut self,
        roots: &N,
        k: &[u8],
        b: &'s [u8],
    ) -> Result<Inside<'s>> {
        let view =
            PackedView::new(b, roots.packed_max()).map_err(|e| packed_error(roots.name(), k, e))?;
        self.inner.unset();
        // Lossless: the row count is a u16 of the encoding.
        self.packed_rows = view.rows() as u16;
        Ok(Inside::Packed(view))
    }

    /// Land on the first (`forward`) or last row of the key just entered (`inside`) and return
    /// its value.
    #[inline]
    fn land_edge_of<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        roots: &N,
        inside: Inside<'s>,
        forward: bool,
    ) -> Result<&'s [u8]> {
        match inside {
            Inside::Nested(_) => self.land_edge(store, forward),
            Inside::Packed(view) => {
                let i = if forward { 0 } else { view.rows() - 1 };
                self.land_packed(roots.name(), &view, i)
            }
        }
    }

    /// The packed set of the key the walker is at, read again from the outer leaf it is at
    /// (`again`'s page, else fetched) and decoded with [`PackedView::new`]'s checks. A cell no
    /// longer inline, or a set of another row count than the landing's, is [`Error::Corrupt`]: a
    /// snapshot does not change, and a change a write transaction makes to the table refreshes
    /// its cursors before they move.
    #[inline(never)]
    fn packed<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &self,
        store: &'s S,
        roots: &N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
    ) -> Result<PackedView<'s>> {
        let name = roots.name();
        let leaf = held(&self.outer, again)?;
        let view = match self.outer.current_in::<Cells, S>(store, leaf)? {
            Some((_, Val::Inline(b))) => {
                PackedView::new(b, roots.packed_max()).map_err(|e| self.packed_err(name, e))?
            }
            _ => return Err(self.packed_changed(name)),
        };
        if view.rows() != usize::from(self.packed_rows) {
            return Err(self.packed_changed(name));
        }
        Ok(view)
    }

    /// The packed row beside the one the walker is at, the next (`forward`) or the previous;
    /// `None` at the set's edge.
    #[inline]
    fn packed_neighbour(&self, forward: bool) -> Option<usize> {
        let i = usize::from(self.packed_at);
        if forward {
            (i + 1 < usize::from(self.packed_rows)).then_some(i + 1)
        } else {
            i.checked_sub(1)
        }
    }

    /// Row `i` of the current key's set `view`: its subkey and value, with [`PackedView::row`]'s
    /// checks.
    #[inline]
    fn packed_row<'s>(
        &self,
        name: &str,
        view: &PackedView<'s>,
        i: usize,
    ) -> Result<(&'s [u8], &'s [u8])> {
        view.row(i, usize::from(self.max_subkey)).map_err(|e| self.packed_err(name, e))
    }

    /// Land on packed row `i` of the current key's set `view` and return its value: an edge row
    /// or a search's, which needs no order compare (the next step compares against it).
    fn land_packed<'s>(&mut self, name: &str, view: &PackedView<'s>, i: usize) -> Result<&'s [u8]> {
        let (s, v) = self.packed_row(name, view, i)?;
        self.set_sub(s)?;
        // Lossless: `i` is below the set's u16 row count.
        self.packed_at = i as u16;
        Ok(v)
    }

    /// Step to packed row `to`, the next (`forward`) or the previous of the one the walker is at,
    /// whose subkey is `s`: above (below) the current subkey, else [`Error::Corrupt`] (a set's
    /// subkeys strictly ascend, so an equal pair is damage too).
    fn packed_move(&mut self, name: &str, s: &[u8], to: usize, forward: bool) -> Result<()> {
        let want = if forward { Ordering::Greater } else { Ordering::Less };
        if cmp_keys(s, &self.key[self.kl()..]) != want {
            return Err(
                self.packed_err(name, Error::Corrupt(format!("subkeys out of order at row {to}")))
            );
        }
        self.set_sub(s)?;
        // Lossless: `to` is below the set's u16 row count.
        self.packed_at = to as u16;
        Ok(())
    }

    /// The first row of the current key's set `view` whose subkey is `>= s`; `None` past its last.
    fn packed_find(&self, name: &str, view: &PackedView<'_>, s: &[u8]) -> Result<Option<usize>> {
        let (Search::Found(j) | Search::NotFound(j)) =
            view.find(s, usize::from(self.max_subkey)).map_err(|e| self.packed_err(name, e))?;
        Ok((j < view.rows()).then_some(j))
    }

    /// [`Self::step`] at a packed row: to the set's neighbouring row (the order compared), else
    /// across to the next (previous) key.
    #[inline(never)]
    fn step_packed<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        if let Some(to) = self.packed_neighbour(forward) {
            let view = self.packed(store, roots, again)?;
            let (s, v) = self.packed_row(roots.name(), &view, to)?;
            self.packed_move(roots.name(), s, to, forward)?;
            return Ok(Some(v));
        }
        self.cross_key(store, outer_root, roots, again, forward)
    }

    /// [`Self::step_in_prefix`] at a packed row with a neighbouring row `to` in the set: move
    /// there (the order compared) when the logical keys share their first `n` bytes, else `None`,
    /// the walker unmoved.
    #[inline(never)]
    fn step_in_prefix_packed<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        roots: &N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        to: usize,
        n: usize,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        let (kl, name) = (self.kl(), roots.name());
        let view = self.packed(store, roots, again)?;
        let (s, v) = self.packed_row(name, &view, to)?;
        // The rows of one key share its `K` bytes, so only a prefix past them can differ.
        if n > kl && !s.starts_with(&self.key[kl..n]) {
            return Ok(None);
        }
        self.packed_move(name, s, to, forward)?;
        Ok(Some(v))
    }

    /// Move to the first row `>= s` of the current key's set `view` (an empty `s`: its first),
    /// else across to the next key's first row.
    #[inline(never)]
    fn seek_packed<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        view: &PackedView<'s>,
        s: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        match self.packed_find(roots.name(), view, s)? {
            Some(j) => self.land_packed(roots.name(), view, j).map(Some),
            None => self.cross_key(store, outer_root, roots, again, true),
        }
    }

    /// The value of the packed row the walker is at with the logical key's last `t` bytes: the
    /// row is `s ‖ v` in one slice of the outer leaf, so a tail inside the subkey comes joined.
    #[inline(never)]
    fn packed_tail<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &self,
        store: &'s S,
        roots: &N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        t: usize,
    ) -> Result<KeyTail<'s>> {
        let name = roots.name();
        let view = self.packed(store, roots, again)?;
        let (start, slen, end) = view
            .row_span(usize::from(self.packed_at), usize::from(self.max_subkey))
            .map_err(|e| self.packed_err(name, e))?;
        let blob = view.blob();
        Ok(if t <= slen {
            KeyTail::Joined(&blob[start + slen - t..end])
        } else {
            KeyTail::Apart(&blob[start + slen..end])
        })
    }

    /// A packed-set error `e` of the codec, naming the table `name` and the current outer key.
    #[cold]
    #[inline(never)]
    fn packed_err(&self, name: &str, e: Error) -> Error {
        packed_error(name, &self.key[..self.kl().min(self.key.len())], e)
    }

    /// The error of a packed set that is not the one the walker landed on.
    #[cold]
    #[inline(never)]
    fn packed_changed(&self, name: &str) -> Error {
        Error::Corrupt(format!(
            "dup table {name}: the packed set of key {:02x?} changed under a cursor",
            &self.key[..self.kl().min(self.key.len())]
        ))
    }

    /// An outer key the outer path landed on has the table's width, else [`Error::Corrupt`] (a
    /// damaged record or tree: no write makes a key of another width).
    #[inline]
    fn check_width<N: NestedRoots>(&self, roots: &N, k: &[u8]) -> Result<()> {
        if k.len() == self.kl() {
            return Ok(());
        }
        Err(width_error(roots.name(), k.len(), self.kl()))
    }

    /// Land the nested path on the first (`forward`) or last row of the nested tree just entered
    /// and return its value. A nested tree with no row there is [`Error::Corrupt`]: a cell names
    /// a non-empty tree.
    fn land_edge<'s, S: PageRead + ?Sized>(
        &mut self,
        store: &'s S,
        forward: bool,
    ) -> Result<&'s [u8]> {
        let landed = if forward {
            self.inner.first(store, &self.nested)?
        } else {
            self.inner.last(store, &self.nested)?
        };
        let Some((s, v)) = landed else {
            return Err(Error::Corrupt(format!(
                "a nested tree of {} entries under outer key {:02x?} holds no row",
                self.nested.entries,
                &self.key[..self.kl()]
            )));
        };
        self.set_sub(s)?;
        Ok(v)
    }

    /// Settle on the outer path's landing: on a key, enter it and land on its first (`forward`)
    /// or last row; on none, the outer path's end is the walker's.
    fn settle<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        roots: &mut N,
        landed: Option<(&'s [u8], Val<'s>)>,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        match landed {
            Some((k, val)) => {
                let inside = self.enter(roots, k, val)?;
                self.land_edge_of(store, roots, inside, forward).map(Some)
            }
            None => {
                self.inner.unset();
                Ok(None)
            }
        }
    }

    /// Move to the first (`forward`) or last row: after the end (before the start) of an empty
    /// table.
    pub(crate) fn start<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        self.guarded(|w| {
            w.inner.unset();
            let landed = if forward {
                w.outer.first_in::<Cells, S>(store, outer_root)?
            } else {
                w.outer.last_in::<Cells, S>(store, outer_root)?
            };
            w.settle(store, roots, landed, forward)
        })
    }

    /// Move to the next (`forward`) or previous row, as a plain cursor's `next` and `prev` do on
    /// the twin: from a row, inside its key from the nested leaf the walker is at, else to the
    /// next key's first row (the previous key's last); from unset or before the start (after the
    /// end), the first (last) row; at the end it stays there. `again` gives the page of a leaf
    /// the walker is at ([`held`]).
    pub(crate) fn step<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        match (self.state(), forward) {
            (State::Unset | State::BeforeStart, true) | (State::Unset | State::AfterEnd, false) => {
                self.start(store, outer_root, roots, forward)
            }
            (State::AfterEnd, true) | (State::BeforeStart, false) => Ok(None),
            (State::At, _) => self.guarded(|w| {
                if w.packed_rows != 0 {
                    return w.step_packed(store, outer_root, roots, again, forward);
                }
                let stepped = match held(&w.inner, again)? {
                    Some(leaf) => w.inner.step_from(store, &w.nested, leaf, forward)?,
                    None if forward => w.inner.next(store, &w.nested)?,
                    None => w.inner.prev(store, &w.nested)?,
                };
                if let Some((s, v)) = stepped {
                    w.set_sub(s)?;
                    return Ok(Some(v));
                }
                w.cross_key(store, outer_root, roots, again, forward)
            }),
        }
    }

    /// From the edge of the current key's nested tree (the nested path past it), step the outer
    /// path to the next (`forward`) or previous key and land on its first (last) row; at the outer
    /// tree's end, the walker's end.
    fn cross_key<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        let landed = match held(&self.outer, again)? {
            Some(leaf) => self.outer.step_from_in::<Cells, S>(store, outer_root, leaf, forward)?,
            None if forward => self.outer.next_in::<Cells, S>(store, outer_root)?,
            None => self.outer.prev_in::<Cells, S>(store, outer_root)?,
        };
        self.settle(store, roots, landed, forward)
    }

    /// Move to the first row `>= x` (any length of `x`); after the end if there is none. From a
    /// row of the key `x` starts with, the outer path does not move and the nested seek starts
    /// from the nested leaf the walker is at; from a row of another key the outer seek starts
    /// from the outer leaf the walker is at ([`TreeCursor::seek`]'s rule, `again` giving the
    /// pages); else from the roots.
    pub(crate) fn seek<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        x: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        let kl = self.kl();
        let (xk, xs) = x.split_at(kl.min(x.len()));
        self.guarded(|w| {
            let at = w.state() == State::At;
            if at && x.len() >= kl && w.key[..kl] == *xk {
                if w.packed_rows != 0 {
                    let view = w.packed(store, roots, again)?;
                    return w.seek_packed(store, outer_root, roots, again, &view, xs);
                }
                let leaf = held(&w.inner, again)?;
                return match w.inner.seek(store, &w.nested, leaf, xs)? {
                    Some((s, v)) => {
                        w.set_sub(s)?;
                        Ok(Some(v))
                    }
                    None => w.cross_key(store, outer_root, roots, again, true),
                };
            }
            let leaf = if at { held(&w.outer, again)? } else { None };
            w.inner.unset();
            let Some((k, val)) = w.outer.seek_in::<Cells, S>(store, outer_root, leaf, xk)? else {
                return Ok(None);
            };
            let inside = w.enter(roots, k, val)?;
            // Every row of a key above `xk`, and of `xk` itself when `x` has no subkey, is `> x`.
            if k != xk || x.len() <= kl {
                return w.land_edge_of(store, roots, inside, true).map(Some);
            }
            if let Inside::Packed(view) = &inside {
                return w.seek_packed(store, outer_root, roots, again, view, xs);
            }
            match w.inner.seek(store, &w.nested, None, xs)? {
                Some((s, v)) => {
                    w.set_sub(s)?;
                    Ok(Some(v))
                }
                None => w.cross_key(store, outer_root, roots, again, true),
            }
        })
    }

    /// Move to the row `x` and return its value; unset if there is none. A key of at most the
    /// key width has no row and reads nothing. The seek starts as [`Self::seek`]'s does.
    pub(crate) fn seek_exact<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        x: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        if x.len() <= self.kl() {
            self.unset();
            return Ok(None);
        }
        let (xk, xs) = x.split_at(self.kl());
        self.bounded(store, outer_root, roots, again, xk, |w, store, name, inside| match inside {
            Inside::Nested(leaf) => {
                let found = w.inner.seek_exact(store, &w.nested, leaf, xs)?;
                if found.is_some() {
                    w.set_sub(xs)?;
                }
                Ok(found)
            }
            Inside::Packed(view) => {
                match view.find(xs, usize::from(w.max_subkey)).map_err(|e| w.packed_err(name, e))? {
                    Search::Found(j) => w.land_packed(name, &view, j).map(Some),
                    Search::NotFound(_) => Ok(None),
                }
            }
        })
    }

    /// Move to the first row `k ‖ s'` of the outer key `k` with `s' >= s` (an empty `s`: `k`'s
    /// first row) and return its value; unset if there is none in `k`, never a row of the next
    /// key: no other key's cell or tree is read. `k` of another length than the key width is
    /// [`Error::KeyTooShort`] or [`Error::KeyTooLarge`], the walker unmoved.
    pub(crate) fn seek_dup<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        k: &[u8],
        s: &[u8],
    ) -> Result<Option<&'s [u8]>> {
        self.check_key(k)?;
        self.bounded(store, outer_root, roots, again, k, |w, store, name, inside| {
            let leaf = match inside {
                Inside::Nested(leaf) => leaf,
                Inside::Packed(view) => {
                    let j = if s.is_empty() { Some(0) } else { w.packed_find(name, &view, s)? };
                    return j.map(|j| w.land_packed(name, &view, j)).transpose();
                }
            };
            let landed = if s.is_empty() {
                w.inner.first(store, &w.nested)?
            } else {
                w.inner.seek(store, &w.nested, leaf, s)?
            };
            match landed {
                Some((s, v)) => {
                    w.set_sub(s)?;
                    Ok(Some(v))
                }
                None => Ok(None),
            }
        })
    }

    /// A seek bounded to the outer key `k` (of the key width): onto `k`'s nested tree, the outer
    /// path unmoved when the walker is at a row of `k`, else found from the outer leaf the walker
    /// is at or the root (an exact search, as a plain `seek_exact`'s); then `inner(walker, store,
    /// table name, inside)` settles the row inside the key (the nested leaf held, or the packed
    /// set), a miss leaving both paths unset. An outer
    /// search that misses `k` reads one key beside it, the leaf's key after `k` (its last when
    /// `k` is past it), only for its width, and leaves the walker unset.
    fn bounded<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        k: &[u8],
        inner: impl FnOnce(&mut Self, &'s S, &str, Inside<'s>) -> Result<Option<&'s [u8]>>,
    ) -> Result<Option<&'s [u8]>> {
        self.guarded(|w| {
            let at = w.state() == State::At;
            if at && w.key[..w.kl()] == *k {
                let inside = if w.packed_rows != 0 {
                    Inside::Packed(w.packed(store, roots, again)?)
                } else {
                    Inside::Nested(held(&w.inner, again)?)
                };
                let found = inner(w, store, roots.name(), inside)?;
                if found.is_none() {
                    w.unset();
                }
                return Ok(found);
            }
            let leaf = if at { held(&w.outer, again)? } else { None };
            w.inner.unset();
            let (kl, name) = (w.kl(), roots.name());
            // The key after `k` in the leaf searched, else its last (`k` past it).
            let beside = |leaf: &LeafView<'s>, slot: usize| {
                let n = leaf.nkeys();
                let Some(i) = (slot < n).then_some(slot).or(n.checked_sub(1)) else {
                    return Ok(());
                };
                match leaf.key(i)?.len() {
                    len if len == kl => Ok(()),
                    len => Err(width_error(name, len, kl)),
                }
            };
            let Some(val) =
                w.outer.seek_exact_beside_in::<Cells, S>(store, outer_root, leaf, k, &beside)?
            else {
                return Ok(None);
            };
            let inside = w.enter(roots, k, val)?;
            let found = inner(w, store, roots.name(), inside)?;
            if found.is_none() {
                w.unset();
            }
            Ok(found)
        })
    }

    /// Move to the next (`forward`) or previous row only if its logical key shares the first `n`
    /// bytes of the current row's; else `None`, the walker unmoved (a plain cursor's prefix step
    /// on the twin). `n` of at least the key width stays inside the key's nested tree or packed
    /// set; a shorter `n` crosses to the neighbouring key when the key's rows end, comparing outer
    /// keys only.
    pub(crate) fn step_in_prefix<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        n: usize,
        forward: bool,
    ) -> Result<Option<&'s [u8]>> {
        if self.state() != State::At || n > self.key.len() {
            return Ok(None);
        }
        let kl = self.kl();
        self.guarded(|w| {
            if w.packed_rows != 0 {
                if let Some(to) = w.packed_neighbour(forward) {
                    return w.step_in_prefix_packed(store, roots, again, to, n, forward);
                }
            } else {
                let leaf = held(&w.inner, again)?;
                let within = n.saturating_sub(kl);
                if let Some(((s, v), _)) =
                    w.inner.step_in_prefix(store, &w.nested, leaf, within, forward)?
                {
                    w.set_sub(s)?;
                    return Ok(Some(v));
                }
            }
            if n >= kl {
                return Ok(None);
            }
            let leaf = held(&w.outer, again)?;
            match w.outer.step_in_prefix_in::<Cells, S>(store, outer_root, leaf, n, forward)? {
                Some(((k, val), _)) => {
                    let inside = w.enter(roots, k, val)?;
                    w.land_edge_of(store, roots, inside, forward).map(Some)
                }
                None => Ok(None),
            }
        })
    }

    /// Move to the next (`forward`) or previous row, as [`Self::step`] does, only if its logical
    /// key starts with `prefix`; else `None`, the walker unmoved. `spare` holds the logical key
    /// meanwhile (a buffer the caller reuses). An error leaves both paths unset.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn step_if<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        prefix: &[u8],
        forward: bool,
        spare: &mut Vec<u8>,
    ) -> Result<Option<&'s [u8]>> {
        let (outer, inner, nested) = (self.outer.clone(), self.inner.clone(), self.nested);
        let packed = (self.packed_rows, self.packed_at);
        spare.clear();
        spare.extend_from_slice(&self.key);
        match self.step(store, outer_root, roots, again, forward)? {
            Some(v) if self.key.starts_with(prefix) => Ok(Some(v)),
            _ => {
                (self.outer, self.inner, self.nested) = (outer, inner, nested);
                (self.packed_rows, self.packed_at) = packed;
                std::mem::swap(&mut self.key, spare);
                Ok(None)
            }
        }
    }

    /// Place the walker on the row `s` its writer just put into the current outer key's nested
    /// tree, now `nested`, at entry `slot` of `leaf` below `branches` (as [`TreeCursor::place`]
    /// takes them), reading no page. The outer path stays: the caller's put changed only that
    /// nested tree, and the walker was at a row of its key on the outer tree as it still is.
    pub(crate) fn place_in_key(
        &mut self,
        nested: TreeRoot,
        branches: &[(u32, usize)],
        leaf: u32,
        slot: usize,
        s: &[u8],
    ) -> Result<()> {
        self.guarded(|w| {
            w.nested = nested;
            w.packed_rows = 0;
            w.inner.place(branches, leaf, slot);
            w.set_sub(s)
        })
    }

    /// Land on the follower of the row its writer just deleted from the current outer key's
    /// nested tree, which the delete kept, now `nested`, with the follower's slot `slot` in
    /// `leaf` below `branches` ([`TreeCursor::place_after_delete`]), and return its value: that
    /// tree's next row, else the next key's first row, else after the end. The outer path must
    /// still be a path of the outer tree `outer_root` (a nested-only delete leaves it so).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn place_after_nested_delete<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        nested: TreeRoot,
        (branches, leaf, slot): (&[(u32, usize)], u32, usize),
    ) -> Result<Option<&'s [u8]>> {
        self.guarded(|w| {
            w.nested = nested;
            w.packed_rows = 0;
            match w.inner.place_after_delete(store, &w.nested, branches, leaf, slot)? {
                Some((s, v)) => {
                    w.set_sub(s)?;
                    Ok(Some(v))
                }
                None => w.cross_key(store, outer_root, roots, again, true),
            }
        })
    }

    /// Land on the follower of an outer key whose cell its writer just removed from the outer
    /// tree `outer_root` (the key's last row deleted), with the follower's slot `slot` in `leaf`
    /// below `branches` (the outer tree's path cache), and return its value: the next key's first
    /// row, else after the end.
    pub(crate) fn place_after_outer_delete<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        (branches, leaf, slot): (&[(u32, usize)], u32, usize),
    ) -> Result<Option<&'s [u8]>> {
        self.guarded(|w| {
            w.inner.unset();
            let landed = w
                .outer
                .place_after_delete_in::<Cells, S>(store, outer_root, branches, leaf, slot)?;
            w.settle(store, roots, landed, true)
        })
    }

    /// The value of the row the walker is at, if it is at one.
    pub(crate) fn current<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &self,
        store: &'s S,
        roots: &N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
    ) -> Result<Option<&'s [u8]>> {
        Ok(self.current_with_key_tail(store, roots, again, 0)?.map(|tail| match tail {
            KeyTail::Joined(v) | KeyTail::Apart(v) => v,
        }))
    }

    /// The value of the row the walker is at, if it is at one, with the logical key's last `t`
    /// bytes: a tail inside the subkey (`t <= |s|`) and an inline value come joined from the nested
    /// leaf, or a packed row from the outer leaf ([`KeyTail::Joined`]); a value in a run, or a tail
    /// reaching into the outer key, come apart ([`KeyTail::Apart`]: the value alone, for the
    /// caller to join to the key's tail).
    pub(crate) fn current_with_key_tail<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &self,
        store: &'s S,
        roots: &N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        t: usize,
    ) -> Result<Option<KeyTail<'s>>> {
        if self.state() != State::At {
            return Ok(None);
        }
        if self.packed_rows != 0 {
            return self.packed_tail(store, roots, again, t).map(Some);
        }
        let leaf = held(&self.inner, again)?;
        let sub = self.key.len() - self.kl();
        let Some((_, tail)) = self.inner.current_with_key_tail(store, leaf, t.min(sub))? else {
            return Ok(None);
        };
        Ok(Some(match tail {
            KeyTail::Joined(joined) if t > sub => KeyTail::Apart(&joined[sub..]),
            tail => tail,
        }))
    }

    /// Test-only: the first and last keys of the outer leaf and of the nested leaf the walker is
    /// at, read from `store` (each `None` unless it is at a row and the page reads): where a seek
    /// from the row may start without a root descent.
    #[cfg(test)]
    pub(crate) fn leaf_bounds_for_test<S: PageRead + ?Sized>(&self, store: &S) -> [LeafBounds; 2] {
        let bounds = |tree: &TreeCursor| {
            let leaf = page::LeafView::new(store.page(tree.leaf_pgno()?).ok()?).ok()?;
            let last = leaf.nkeys().checked_sub(1)?;
            Some((leaf.key(0).ok()?.to_vec(), leaf.key(last).ok()?.to_vec()))
        };
        if self.state() != State::At {
            return [None, None];
        }
        [bounds(&self.outer), bounds(&self.inner)]
    }

    /// Test-only: where the walker is in the trees while it is at a row (`None` otherwise), as a
    /// seek leaves it: the outer path, and inside the key the nested root and path of a nested
    /// key or the row count and row of a packed one.
    #[cfg(test)]
    pub(crate) fn place_for_test(&self) -> Option<WalkPlace> {
        if self.state() != State::At {
            return None;
        }
        let inside = if self.packed_rows != 0 {
            InKey::Packed { rows: self.packed_rows, at: self.packed_at }
        } else {
            InKey::Nested(self.nested, self.inner.path())
        };
        Some((self.outer.path(), inside))
    }
}

/// Test-only: where a walker at a row is ([`DupWalk::place_for_test`]).
#[cfg(test)]
pub(crate) type WalkPlace = (Vec<(u32, u16)>, InKey);

/// Test-only: where a walker at a row is inside its key ([`DupWalk::place_for_test`]).
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InKey {
    /// A nested key: its root and the nested path.
    Nested(TreeRoot, Vec<(u32, u16)>),
    /// A packed key: its row count and the row.
    Packed {
        /// The set's row count.
        rows: u16,
        /// The row the walker is at.
        at: u16,
    },
}

/// Test-only: a leaf's first and last keys ([`DupWalk::leaf_bounds_for_test`]).
#[cfg(test)]
pub(crate) type LeafBounds = Option<(Vec<u8>, Vec<u8>)>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{test_harness::MemStore, write};

    /// The root source of an outer tree of packed sets alone, of up to 512 bytes each.
    struct Packing;

    impl NestedRoots for Packing {
        fn name(&self) -> &str {
            "t"
        }

        fn root(&mut self, _: &[u8], _: &[u8; page::NESTED_CELL]) -> Result<TreeRoot> {
            Err(Error::Corrupt("no nested tree here".into()))
        }

        fn packed_max(&self) -> usize {
            512
        }
    }

    /// No held leaf: every move fetches its page.
    fn no_leaf<'s>(_: u32) -> Result<Option<&'s [u8]>> {
        Ok(None)
    }

    /// A packed set of `rows` in the order given, laid out as the codec lays one out (a forger's
    /// encoder: no order check).
    fn forge(rows: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut out = (rows.len() as u16).to_le_bytes().to_vec();
        let mut end = 2 + 4 * rows.len();
        for (s, v) in rows {
            end += s.len() + v.len();
            out.extend((end as u16).to_le_bytes());
            out.extend((s.len() as u16).to_le_bytes());
        }
        for (s, v) in rows {
            out.extend(*s);
            out.extend(*v);
        }
        out
    }

    /// Every step inside a packed set compares the new row's subkey with the one before: a set
    /// whose last two rows are swapped (which passes the O(1) checks) is `Corrupt` at the step
    /// onto the out-of-order row, forward and backward, by the exact message, the walker unset;
    /// the set's edge rows land with no compare.
    #[test]
    fn the_walk_order_check_catches_a_forged_swap() {
        let mut store = MemStore::new(4096);
        let (mut root, mut trusted) = (TreeRoot::EMPTY, false);
        let blob = forge(&[(b"a", b"1"), (b"c", b"3"), (b"b", b"2")]);
        write::put(&mut store, &mut root, &mut trusted, &[0, 0, 0, 1], &blob).expect("put");
        let refused = |row: usize| {
            format!(
                "dup table t: the packed set of key [00, 00, 00, 01]: subkeys out of order at row \
                 {row}"
            )
        };
        let mut w = DupWalk::new(4, 64);
        let walk = &mut w;
        assert_eq!(walk.start(&store, &root, &mut Packing, true).expect("row 0"), Some(&b"1"[..]));
        let step = walk.step(&store, &root, &mut Packing, &no_leaf, true).expect("row 1");
        assert_eq!((step, walk.key()), (Some(&b"3"[..]), &b"\0\0\0\x01c"[..]));
        let step = walk.step(&store, &root, &mut Packing, &no_leaf, true);
        assert!(matches!(&step, Err(Error::Corrupt(m)) if *m == refused(2)), "{step:?}");
        assert_eq!(walk.state(), State::Unset);

        assert_eq!(walk.start(&store, &root, &mut Packing, false).expect("row 2"), Some(&b"2"[..]));
        let step = walk.step(&store, &root, &mut Packing, &no_leaf, false);
        assert!(matches!(&step, Err(Error::Corrupt(m)) if *m == refused(1)), "{step:?}");
        assert_eq!(walk.state(), State::Unset);
    }

    /// A walker is two tree paths, a nested root and the key buffer: nothing else rides along.
    /// The packed mode's two `u16`s took the padding after the key width and `max_subkey`: the
    /// exact size is the size before them, so a later field cannot hide in slack.
    #[test]
    fn dup_walk_stays_small() {
        let most = 2 * size_of::<TreeCursor>() + size_of::<TreeRoot>() + 32;
        assert!(size_of::<DupWalk>() <= most, "{} > {most}", size_of::<DupWalk>());
        assert_eq!(size_of::<DupWalk>(), 672);
    }
}
