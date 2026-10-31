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
//! width, then a nested cell ([`nested_cell`]). The nested trees are plain trees
//! ([`super::cursor::Values`]).
//!
//! The logical key is not contiguous in any page (`k` sits in an outer leaf, `s` in a nested
//! leaf), so the walker keeps it in a buffer, rewritten past `k` on each move inside a key and
//! whole on a move to another key; its capacity is reused, so a move allocates nothing once the
//! buffer has grown to the longest key.

use super::{
    cursor::{Cells, KeyTail, State, TreeCursor},
    PageRead, TreeRoot,
};
use crate::{
    error::{Error, Result},
    page::{self, Val},
};

/// Where a dup walker gets the nested root of each outer key it lands on.
pub(crate) trait NestedRoots {
    /// The dup table's name, for the messages of damage.
    fn name(&self) -> &str;

    /// The nested root of outer key `k`, whose outer cell holds `cell`: a reader's from the cell
    /// itself, a write transaction's from its working map when the map holds `k`, never from a
    /// cell the map supersedes. A cell naming an empty tree is [`Error::Corrupt`].
    fn root(&mut self, k: &[u8], cell: &[u8; page::NESTED_CELL]) -> Result<TreeRoot>;
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
        Val::Inline(_) => Err(Error::Corrupt(format!(
            "dup table {name}: the outer cell of key {k:02x?} holds no nested tree"
        ))),
        Val::Overflow(_) => Err(Error::Corrupt(format!(
            "dup table {name}: an overflow cell in a dup table's outer tree (key {k:02x?})"
        ))),
    }
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

/// A position in a dup table: the outer tree's path, the current outer key's nested tree and its
/// path, and the logical key `k ‖ s` of the row it is at (see the module docs).
///
/// Its state is the nested path's while the outer path is at an entry, else the outer path's.
/// After every move an outer path at an entry sits over a nested path at an entry: the walker
/// never stops at a nested tree's end, it steps the outer path on. An error leaves both paths
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
        }
    }

    /// The key width `K`.
    #[inline]
    fn kl(&self) -> usize {
        usize::from(self.key_len)
    }

    /// Where the walker is.
    #[inline]
    pub(crate) fn state(&self) -> State {
        match self.outer.state() {
            State::At => self.inner.state(),
            outer => outer,
        }
    }

    /// The logical key of the row the walker is at (meaningful while [`State::At`]).
    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
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
    fn unset(&mut self) {
        self.outer.unset();
        self.inner.unset();
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
    /// take its nested root and write `k` into the logical key. The nested path is left for the
    /// caller to land.
    fn enter<N: NestedRoots>(&mut self, roots: &mut N, k: &[u8], val: Val<'_>) -> Result<()> {
        self.check_width(roots, k)?;
        let cell = nested_cell(roots.name(), k, val)?;
        self.nested = roots.root(k, cell)?;
        if self.key.capacity() == 0 {
            self.key.reserve(self.kl() + 64);
        }
        self.key.clear();
        self.key.extend_from_slice(k);
        Ok(())
    }

    /// An outer key the outer path landed on has the table's width, else [`Error::Corrupt`] (a
    /// damaged record or tree: no write makes a key of another width).
    #[inline]
    fn check_width<N: NestedRoots>(&self, roots: &N, k: &[u8]) -> Result<()> {
        if k.len() == self.kl() {
            return Ok(());
        }
        Err(Error::Corrupt(format!(
            "dup table {}: an outer key of {} bytes, not the table's width {}",
            roots.name(),
            k.len(),
            self.kl()
        )))
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
                self.enter(roots, k, val)?;
                self.land_edge(store, forward).map(Some)
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
            w.enter(roots, k, val)?;
            // Every row of a key above `xk`, and of `xk` itself when `x` has no subkey, is `> x`.
            if k != xk || x.len() <= kl {
                return w.land_edge(store, true).map(Some);
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
        self.bounded(store, outer_root, roots, again, xk, |w, store, leaf| {
            let found = w.inner.seek_exact(store, &w.nested, leaf, xs)?;
            if found.is_some() {
                w.set_sub(xs)?;
            }
            Ok(found)
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
        let kl = self.kl();
        if k.len() < kl {
            return Err(Error::KeyTooShort { len: k.len(), min: kl });
        }
        if k.len() > kl {
            return Err(Error::KeyTooLarge { len: k.len(), max: kl });
        }
        self.bounded(store, outer_root, roots, again, k, |w, store, leaf| {
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
    /// is at or the root; then `inner(walker, store, nested leaf held)` settles the nested path,
    /// a miss leaving both paths unset. An outer seek that lands on another key reads only that
    /// key (its width, no cell) and leaves the walker unset.
    fn bounded<'s, S: PageRead + ?Sized, N: NestedRoots>(
        &mut self,
        store: &'s S,
        outer_root: &TreeRoot,
        roots: &mut N,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        k: &[u8],
        inner: impl FnOnce(&mut Self, &'s S, Option<&'s [u8]>) -> Result<Option<&'s [u8]>>,
    ) -> Result<Option<&'s [u8]>> {
        self.guarded(|w| {
            let at = w.state() == State::At;
            if at && w.key[..w.kl()] == *k {
                let leaf = held(&w.inner, again)?;
                let found = inner(w, store, leaf)?;
                if found.is_none() {
                    w.unset();
                }
                return Ok(found);
            }
            let leaf = if at { held(&w.outer, again)? } else { None };
            w.inner.unset();
            match w.outer.seek_in::<Cells, S>(store, outer_root, leaf, k)? {
                Some((found, val)) if found == k => {
                    w.enter(roots, found, val)?;
                    let found = inner(w, store, None)?;
                    if found.is_none() {
                        w.unset();
                    }
                    Ok(found)
                }
                Some((other, _)) => {
                    w.check_width(roots, other)?;
                    w.unset();
                    Ok(None)
                }
                None => {
                    w.unset();
                    Ok(None)
                }
            }
        })
    }

    /// Move to the next (`forward`) or previous row only if its logical key shares the first `n`
    /// bytes of the current row's; else `None`, the walker unmoved (a plain cursor's prefix step
    /// on the twin). `n` of at least the key width stays inside the key's nested tree; a shorter
    /// `n` crosses to the neighbouring key when the key's tree ends, comparing outer keys only.
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
            let leaf = held(&w.inner, again)?;
            let within = n.saturating_sub(kl);
            if let Some(((s, v), _)) =
                w.inner.step_in_prefix(store, &w.nested, leaf, within, forward)?
            {
                w.set_sub(s)?;
                return Ok(Some(v));
            }
            if n >= kl {
                return Ok(None);
            }
            let leaf = held(&w.outer, again)?;
            match w.outer.step_in_prefix_in::<Cells, S>(store, outer_root, leaf, n, forward)? {
                Some(((k, val), _)) => {
                    w.enter(roots, k, val)?;
                    w.land_edge(store, forward).map(Some)
                }
                None => Ok(None),
            }
        })
    }

    /// The value of the row the walker is at, if it is at one.
    pub(crate) fn current<'s, S: PageRead + ?Sized>(
        &self,
        store: &'s S,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
    ) -> Result<Option<&'s [u8]>> {
        Ok(self.current_with_key_tail(store, again, 0)?.map(|tail| match tail {
            KeyTail::Joined(v) | KeyTail::Apart(v) => v,
        }))
    }

    /// The value of the row the walker is at, if it is at one, with the logical key's last `t`
    /// bytes: a tail inside the subkey (`t <= |s|`) and an inline value come joined from the nested
    /// leaf ([`KeyTail::Joined`]); a value in a run, or a tail reaching into the outer key, come
    /// apart ([`KeyTail::Apart`]: the value alone, for the caller to join to the key's tail).
    pub(crate) fn current_with_key_tail<'s, S: PageRead + ?Sized>(
        &self,
        store: &'s S,
        again: &impl Fn(u32) -> Result<Option<&'s [u8]>>,
        t: usize,
    ) -> Result<Option<KeyTail<'s>>> {
        if self.state() != State::At {
            return Ok(None);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A walker is two tree paths, a nested root and the key buffer: nothing else rides along.
    #[test]
    fn dup_walk_stays_small() {
        let most = 2 * size_of::<TreeCursor>() + size_of::<TreeRoot>() + 32;
        assert!(size_of::<DupWalk>() <= most, "{} > {most}", size_of::<DupWalk>());
    }
}
