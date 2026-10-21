//! reth's cursors: [`TndbCursor`] over a read-only transaction and [`TndbCursorMut`] over a
//! read-write one.
//!
//! Both follow MDBX's rules, which the engine's cursors share: `seek` lands on the first key at or
//! after its key, and after `delete_current` the cursor is on the deleted row's follower, as
//! MDBX's is: `next` and `current` return the follower, `prev` the row before it, and another
//! `delete_current` deletes it. After `last` a cursor ends the table: `next` finds nothing and the
//! cursor stays on the row, even once another cursor appends past it. A missed `seek_exact` or
//! `seek_by_key_subkey` leaves the cursor unset, as MDBX's: every relative move (`next`, `prev`,
//! `current` and the dup moves) then finds nothing and a delete has nothing to delete, until an
//! absolute move or a write places it again. After `upsert`, `insert` or `append` the cursor is on
//! the row written; after a refused `insert` it is on the present key, after a refused `append` on
//! the table's last row, and after an `append` that replaces the last key's value it ends the
//! table as after `last`, as MDBX leaves it. A write refused for its size leaves the cursor where
//! it was, in whatever state, as MDBX's refusal does.
//!
//! On a dup-sort table (stored as composite keys, see [`crate::dup`]) each row read is read again
//! once the move is done, with its value (subkey, then the rest) as one slice of the page, which
//! the engine's `current_with_key_tail` gives since a leaf cell holds its value right after its
//! key: no copy, unless the value is in an overflow run, whose rest is joined to the subkey in a
//! buffer of the cursor's. The dup moves are the engine's prefix
//! steps over the key's width: `next_dup` and `prev_dup` stay within the current key's rows, from
//! a fresh cursor `next_dup`, `next_no_dup` and `next_dup_val` go to the first row and `prev_dup`
//! to the last, as MDBX's do. Where MDBX's cursor ends a key's rows, the cursor does too (its
//! `Pos::DupEnd` state): after `last`, and after a delete of a key's last row that leaves the key
//! earlier rows, which puts the cursor on the row before the deleted one. There `next_dup` finds
//! nothing and `next` goes to the next key; right after that delete `current`, `prev` and
//! `prev_dup` return the row before (now the key's last) and `delete_current` is `MDBX_ENODATA`,
//! as MDBX's do. Right after a delete that leaves a key no rows (`Pos::KeyDeleted`) the cursor is
//! on the next key's first row, where `prev_dup` finds nothing, as MDBX's does.

use crate::{
    dup::DupLayout,
    error::{self, Mdbx},
    tx::{compressed, no_data, SharedTxn},
};
use reth_db_api::{
    common::{IterPairResult, PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Decode, Decompress, DupSort, Encode, Table},
    DatabaseError,
};
use reth_storage_errors::db::DatabaseWriteOperation;
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};
use tndb_b::{KeyTail, ReadTxn, RoCursor, RwTxn, Table as EngineTable, TxnCursor};

/// A stored row: its key and value as the engine holds them.
type Entry<'a> = (&'a [u8], &'a [u8]);

/// A stored row with its value joined to its key's last bytes ([`KeyTail`]).
type Tailed<'a> = (&'a [u8], KeyTail<'a>);

/// The moves of an engine cursor: a read-only one's, or a write one's with its transaction locked
/// ([`Locked`]). The adapter's moves ([`Rows`]) are written once over these.
trait Moves {
    fn first(&mut self) -> tndb_b::Result<Option<Entry<'_>>>;
    fn last(&mut self) -> tndb_b::Result<Option<Entry<'_>>>;
    fn next(&mut self) -> tndb_b::Result<Option<Entry<'_>>>;
    fn prev(&mut self) -> tndb_b::Result<Option<Entry<'_>>>;
    fn current(&mut self) -> tndb_b::Result<Option<Entry<'_>>>;
    fn seek(&mut self, key: &[u8]) -> tndb_b::Result<Option<Entry<'_>>>;
    fn seek_exact(&mut self, key: &[u8]) -> tndb_b::Result<Option<&[u8]>>;
    fn next_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>>;
    fn prev_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>>;
    fn current_with_key_tail(&mut self, t: usize) -> tndb_b::Result<Option<Tailed<'_>>>;
}

impl Moves for RoCursor {
    fn first(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::first(self)
    }
    fn last(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::last(self)
    }
    fn next(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::next(self)
    }
    fn prev(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::prev(self)
    }
    fn current(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::current(self)
    }
    fn seek(&mut self, key: &[u8]) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::seek(self, key)
    }
    fn seek_exact(&mut self, key: &[u8]) -> tndb_b::Result<Option<&[u8]>> {
        Self::seek_exact(self, key)
    }
    fn next_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::next_in_prefix(self, n)
    }
    fn prev_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>> {
        Self::prev_in_prefix(self, n)
    }
    fn current_with_key_tail(&mut self, t: usize) -> tndb_b::Result<Option<Tailed<'_>>> {
        Self::current_with_key_tail(self, t)
    }
}

/// A write cursor with its transaction locked, for one call.
struct Locked<'c, 't> {
    cursor: &'c mut TxnCursor,
    txn: &'t RwTxn,
}

impl Moves for Locked<'_, '_> {
    fn first(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.first(self.txn)
    }
    fn last(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.last(self.txn)
    }
    fn next(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.next(self.txn)
    }
    fn prev(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.prev(self.txn)
    }
    fn current(&mut self) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.current(self.txn)
    }
    fn seek(&mut self, key: &[u8]) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.seek(self.txn, key)
    }
    fn seek_exact(&mut self, key: &[u8]) -> tndb_b::Result<Option<&[u8]>> {
        self.cursor.seek_exact(self.txn, key)
    }
    fn next_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.next_in_prefix(self.txn, n)
    }
    fn prev_in_prefix(&mut self, n: usize) -> tndb_b::Result<Option<Entry<'_>>> {
        self.cursor.prev_in_prefix(self.txn, n)
    }
    fn current_with_key_tail(&mut self, t: usize) -> tndb_b::Result<Option<Tailed<'_>>> {
        self.cursor.current_with_key_tail(self.txn, t)
    }
}

/// Where a cursor is, beyond its engine cursor's state: MDBX tells a fresh cursor from one a
/// missed lookup left unset, which the engine's cursors (unset in both cases) do not, and a
/// dup-sort cursor at the end of its key's rows after a delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pos {
    /// Made and not moved: `next_dup`, `next_no_dup` and `next_dup_val` go to the first row and
    /// `prev_dup` to the last.
    Fresh,
    /// Left by a missed `seek_exact` or `seek_by_key_subkey`: every relative move finds nothing
    /// and a delete has nothing to delete (the engine cursor may sit anywhere).
    Missed,
    /// The engine cursor's state is the cursor's.
    Engine,
    /// Right after its own delete of a dup key's last row that left the key no rows (or its
    /// `delete_current_duplicates`): the engine cursor is on the follower, the next key's first
    /// row, in its deleted state. `prev_dup` finds nothing, as MDBX's does, even once another
    /// writer has put rows of the deleted key again (the engine's step back would take them, as
    /// it compares with the deleted key); every other call is as from [`Pos::Engine`].
    KeyDeleted,
    /// A dup-sort cursor at the end of its key's rows, where MDBX's cursor stops stepping
    /// through them: left by its own delete of the key's last row while the key keeps earlier
    /// rows (the engine cursor is then on the row before the deleted one), and with `soft` by
    /// `last` (and the moves that take the last row as `last` does). `current` returns the row;
    /// `next` and `next_no_dup` the next key's first, by a seek past the key's rows (so rows put
    /// since after the row are skipped, as MDBX skips them); `next_dup` and `next_dup_val`
    /// nothing, and set `soft`; `last_dup`, and while not `soft` `prev` and `prev_dup`, the key's
    /// last row; while `soft`, `prev` and `prev_dup` step back from the row. `delete_current`
    /// deletes the row once `soft`, and before is `MDBX_ENODATA`.
    DupEnd { soft: bool },
}

/// The part of a cursor that knows its table's rows: their layout, the cursor's [`Pos`], and the
/// buffers a dup-sort row's value is rebuilt in and a `seek_by_key_subkey` key is built in. Its
/// moves take the engine cursor as [`Moves`].
#[derive(Debug)]
struct Rows {
    dup: Option<DupLayout>,
    pos: Pos,
    value: Vec<u8>,
    target: Vec<u8>,
}

/// The row of table `T` that a move of the engine cursor `$m` landed on, `$entry` (its stored
/// form, which borrows `$m`), decoded: from the entry on a table without duplicates ([`plain`]);
/// on a dup-sort table read again from `$m` ([`Rows::dup_pair`]) once the entry, so the borrow,
/// is done with.
macro_rules! pair {
    ($rows:ident, $m:ident, $entry:ident) => {
        match $rows.dup {
            None => plain::<T>($entry),
            Some(dup) => {
                let found = $entry.is_some();
                $rows.dup_pair::<T>($m, dup, found)
            }
        }
    };
}

/// The row of table `T`, which has no duplicates, stored as `entry`, if any.
fn plain<T: Table>(entry: Option<Entry<'_>>) -> PairResult<T> {
    let Some((key, value)) = entry else { return Ok(None) };
    Ok(Some((T::Key::decode(key)?, T::Value::decompress(value)?)))
}

impl Rows {
    const fn new(dup: Option<DupLayout>) -> Self {
        Self { dup, pos: Pos::Fresh, value: Vec::new(), target: Vec::new() }
    }

    /// Where a write through the cursor that ended with `result` leaves it. A write the engine
    /// took, or one refused where MDBX's refusal places the cursor too, which the write paths
    /// place it for (an insert of a present key: on the key, its first row on a dup-sort table;
    /// an append out of order: on the last row, for `append_dup` the key's last or first), leaves
    /// it at the engine cursor's place ([`Pos::Engine`]). A size the engine refuses changes
    /// nothing, the engine cursor included, so the cursor keeps its state, as MDBX's does; so it
    /// does after damage, which fails the environment.
    fn wrote(&mut self, result: &tndb_b::Result<()>) {
        if matches!(
            result,
            Ok(()) | Err(tndb_b::Error::KeyExists | tndb_b::Error::AppendOutOfOrder)
        ) {
            self.pos = Pos::Engine;
        }
    }

    /// The layout for a dup cursor call on table `T`; a table without duplicates has none.
    fn layout<T: Table>(&self) -> Result<DupLayout, DatabaseError> {
        self.dup.ok_or_else(|| error::dup_sort_unsupported(T::NAME))
    }

    /// The row of the dup-sort table the engine cursor `m` is at, if a move just `found` it, as
    /// its stored key and its full value: the stored key's subkey and the stored value read as one
    /// slice of the page, or for a value in an overflow run joined in the cursor's buffer
    /// ([`DupLayout::joined`]).
    #[inline(always)]
    fn dup_row<'a>(
        &'a mut self,
        m: &'a mut impl Moves,
        dup: DupLayout,
        found: bool,
    ) -> Result<Option<Entry<'a>>, DatabaseError> {
        if !found {
            return Ok(None);
        }
        let Some((key, tail)) = m.current_with_key_tail(dup.subkey).map_err(error::read)? else {
            return Ok(None);
        };
        let value = dup.joined(key, tail, &mut self.value).ok_or(DatabaseError::Decode)?;
        Ok(Some((key, value)))
    }

    /// The row of dup-sort table `T` the engine cursor `m` is at, if a move just `found` it
    /// ([`Rows::dup_row`]).
    fn dup_pair<T: Table>(
        &mut self,
        m: &mut impl Moves,
        dup: DupLayout,
        found: bool,
    ) -> PairResult<T> {
        let Some((key, value)) = self.dup_row(m, dup, found)? else { return Ok(None) };
        let (key_part, _) = dup.split(key).ok_or(DatabaseError::Decode)?;
        Ok(Some((T::Key::decode(key_part)?, T::Value::decompress(value)?)))
    }

    /// The value of the row of dup-sort table `T` the engine cursor `m` is at, if a move just
    /// `found` it ([`Rows::dup_row`]).
    fn dup_value<T: Table>(
        &mut self,
        m: &mut impl Moves,
        dup: DupLayout,
        found: bool,
    ) -> ValueOnlyResult<T> {
        self.dup_row(m, dup, found)?.map(|(_, value)| T::Value::decompress(value)).transpose()
    }

    fn first<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        self.pos = Pos::Engine;
        let entry = m.first().map_err(error::read)?;
        pair!(self, m, entry)
    }

    fn last<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let entry = m.last().map_err(error::read)?;
        self.pos = self.ended(entry.is_some());
        pair!(self, m, entry)
    }

    /// The position after a move that took the table's last row as MDBX's `last` does (`found`:
    /// there was one): on a dup-sort table, the end of its key's rows.
    const fn ended(&self, found: bool) -> Pos {
        if found && self.dup.is_some() {
            Pos::DupEnd { soft: true }
        } else {
            Pos::Engine
        }
    }

    fn seek<T: Table>(&mut self, m: &mut impl Moves, key: &[u8]) -> PairResult<T> {
        self.pos = Pos::Engine;
        let entry = m.seek(key).map_err(error::read)?;
        pair!(self, m, entry)
    }

    /// The row `key` (on a dup-sort table, its first): a `seek`, then a check that the row found
    /// is the key's. A miss leaves the cursor unset ([`Pos::Missed`]).
    fn seek_exact<T: Table>(&mut self, m: &mut impl Moves, key: &[u8]) -> PairResult<T> {
        let found = match self.dup {
            None => m.seek_exact(key).map_err(error::read)?.map(|value| (key, value)),
            Some(dup) => {
                m.seek(key).map_err(error::read)?.filter(|&(stored, _)| dup.is_row_of(stored, key))
            }
        };
        self.pos = if found.is_some() { Pos::Engine } else { Pos::Missed };
        pair!(self, m, found)
    }

    fn next<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let entry = match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Missed => {
                self.pos = Pos::Missed;
                return Ok(None);
            }
            Pos::DupEnd { .. } => past_dup_end(m, self.layout::<T>()?, false),
            Pos::Fresh | Pos::Engine | Pos::KeyDeleted => m.next(),
        };
        let entry = entry.map_err(error::read)?;
        pair!(self, m, entry)
    }

    fn prev<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let entry = match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Missed => {
                self.pos = Pos::Missed;
                return Ok(None);
            }
            Pos::DupEnd { soft: false } => past_dup_end(m, self.layout::<T>()?, true),
            // From a fresh cursor the last row, which MDBX takes by its `last`.
            Pos::Fresh => {
                let entry = m.prev();
                self.pos = self.ended(matches!(entry, Ok(Some(_))));
                entry
            }
            Pos::DupEnd { soft: true } | Pos::Engine | Pos::KeyDeleted => m.prev(),
        };
        let entry = entry.map_err(error::read)?;
        pair!(self, m, entry)
    }

    fn current<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        if self.pos == Pos::Missed {
            return Ok(None);
        }
        let entry = m.current().map_err(error::read)?;
        pair!(self, m, entry)
    }

    /// Move to the first row of `key` whose subkey is at or after `subkey` (MDBX's
    /// `get_both_range`): a `seek` of `key ‖ subkey`, never past the key's rows. `true` if there
    /// is one; a miss leaves the cursor unset.
    fn seek_by_key_subkey_row(
        &mut self,
        m: &mut impl Moves,
        dup: DupLayout,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<bool, DatabaseError> {
        self.target.clear();
        self.target.extend_from_slice(key);
        self.target.extend_from_slice(subkey);
        let found = m
            .seek(&self.target)
            .map_err(error::read)?
            .is_some_and(|(stored, _)| dup.is_row_of(stored, key));
        self.pos = if found { Pos::Engine } else { Pos::Missed };
        Ok(found)
    }

    /// The next row of the current key.
    fn next_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tndb_b::Result<Option<Entry<'m>>> {
        match self.pos {
            Pos::Fresh => {
                self.pos = Pos::Engine;
                m.first()
            }
            Pos::Missed => Ok(None),
            Pos::DupEnd { .. } => {
                self.pos = Pos::DupEnd { soft: true };
                Ok(None)
            }
            Pos::Engine => m.next_in_prefix(dup.key),
            Pos::KeyDeleted => {
                let entry = m.next_in_prefix(dup.key);
                if matches!(entry, Ok(Some(_))) {
                    self.pos = Pos::Engine;
                }
                entry
            }
        }
    }

    /// The previous row of the current key.
    fn prev_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tndb_b::Result<Option<Entry<'m>>> {
        match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Fresh => {
                let entry = m.last();
                self.pos = self.ended(matches!(entry, Ok(Some(_))));
                entry
            }
            pos @ (Pos::Missed | Pos::KeyDeleted) => {
                self.pos = pos;
                Ok(None)
            }
            Pos::DupEnd { soft: false } => past_dup_end(m, dup, true),
            Pos::DupEnd { soft: true } | Pos::Engine => m.prev_in_prefix(dup.key),
        }
    }

    /// The first row of the next key: past the current key's rows by a `seek` of
    /// [`DupLayout::past`] when another row of the key follows, else the next row (which is also
    /// right after a delete, past either end, and before the first row); from a fresh cursor, the
    /// first row.
    fn next_no_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tndb_b::Result<Option<Entry<'m>>> {
        match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Fresh => return m.first(),
            Pos::Missed => {
                self.pos = Pos::Missed;
                return Ok(None);
            }
            Pos::DupEnd { .. } => return past_dup_end(m, dup, false),
            Pos::Engine | Pos::KeyDeleted => {}
        }
        match m.next_in_prefix(dup.key)?.map(|(stored, _)| dup.past(stored)) {
            Some(past) => m.seek(&past),
            None => m.next(),
        }
    }

    /// The last row of the current key: past the key's rows and back one when another row of the
    /// key follows, else the current row. Right after a delete the current row is the follower,
    /// whose key MDBX takes too. The cursor then ends the key's rows, as MDBX's `last_dup` does
    /// ([`Pos::DupEnd`], soft): `next_dup` finds nothing, even after another writer put a row
    /// after it in the key.
    fn last_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tndb_b::Result<Option<Entry<'m>>> {
        let entry = match self.pos {
            Pos::Fresh | Pos::Missed => return Ok(None),
            Pos::DupEnd { .. } => past_dup_end(m, dup, true)?,
            Pos::Engine | Pos::KeyDeleted => {
                if m.current()?.is_none() {
                    return Ok(None);
                }
                match m.next_in_prefix(dup.key)?.map(|(stored, _)| dup.past(stored)) {
                    Some(past) => {
                        m.seek(&past)?;
                        m.prev()?
                    }
                    None => m.current()?,
                }
            }
        };
        self.pos = if entry.is_some() { Pos::DupEnd { soft: true } } else { Pos::Engine };
        Ok(entry)
    }

    fn next_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let found = self.next_dup_row(m, dup).map_err(error::read)?.is_some();
        self.dup_pair::<T>(m, dup, found)
    }

    fn prev_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let found = self.prev_dup_row(m, dup).map_err(error::read)?.is_some();
        self.dup_pair::<T>(m, dup, found)
    }

    fn next_no_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let found = self.next_no_dup_row(m, dup).map_err(error::read)?.is_some();
        self.dup_pair::<T>(m, dup, found)
    }

    fn last_dup<T: Table>(&mut self, m: &mut impl Moves) -> ValueOnlyResult<T> {
        let dup = self.layout::<T>()?;
        let found = self.last_dup_row(m, dup).map_err(error::read)?.is_some();
        self.dup_value::<T>(m, dup, found)
    }

    fn next_dup_val<T: Table>(&mut self, m: &mut impl Moves) -> ValueOnlyResult<T> {
        let dup = self.layout::<T>()?;
        let found = self.next_dup_row(m, dup).map_err(error::read)?.is_some();
        self.dup_value::<T>(m, dup, found)
    }
}

/// From a [`Pos::DupEnd`] row, past its key's rows by a `seek` of [`DupLayout::past`] (which also
/// steps past the end an engine cursor's `last` sets): the next key's first row, or with `back`
/// the key's last row.
fn past_dup_end(
    m: &mut impl Moves,
    dup: DupLayout,
    back: bool,
) -> tndb_b::Result<Option<Entry<'_>>> {
    let Some(past) = m.current()?.map(|(stored, _)| dup.past(stored)) else { return Ok(None) };
    if back {
        m.seek(&past)?;
        m.prev()
    } else {
        m.seek(&past)
    }
}

/// The first row of a range walk over `range`: the first key at or after an included start, past
/// every row of an excluded one (MDBX's cursor panics on an excluded start), or the table's first.
fn range_start<T: Table, C: DbCursorRO<T>>(
    cursor: &mut C,
    range: &impl RangeBounds<T::Key>,
) -> IterPairResult<T> {
    match range.start_bound() {
        Bound::Included(key) => cursor.seek(key.clone()),
        Bound::Excluded(start) => {
            let mut row = cursor.seek(start.clone());
            while matches!(&row, Ok(Some((key, _))) if key == start) {
                row = cursor.next();
            }
            row
        }
        Bound::Unbounded => cursor.first(),
    }
    .transpose()
}

/// reth's `DbCursorRO` for a cursor with a `with` method that runs a closure on its [`Rows`] and
/// its engine cursor as [`Moves`].
macro_rules! reads {
    () => {
        fn first(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.first::<T>(m))
        }

        fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
            let key = key.encode();
            self.with(|rows, m| rows.seek_exact::<T>(m, key.as_ref()))
        }

        fn seek(&mut self, key: T::Key) -> PairResult<T> {
            let key = key.encode();
            self.with(|rows, m| rows.seek::<T>(m, key.as_ref()))
        }

        fn next(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.next::<T>(m))
        }

        fn prev(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.prev::<T>(m))
        }

        fn last(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.last::<T>(m))
        }

        fn current(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.current::<T>(m))
        }

        fn walk(
            &mut self,
            start_key: Option<T::Key>,
        ) -> Result<Walker<'_, T, Self>, DatabaseError> {
            let start = match start_key {
                Some(key) => self.seek(key),
                None => self.first(),
            }
            .transpose();
            Ok(Walker::new(self, start))
        }

        fn walk_range(
            &mut self,
            range: impl RangeBounds<T::Key>,
        ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
            let start = range_start(self, &range);
            Ok(RangeWalker::new(self, start, range.end_bound().cloned()))
        }

        /// Walks back from the first key at or after `start_key` (as MDBX's cursor does), or
        /// from the last.
        fn walk_back(
            &mut self,
            start_key: Option<T::Key>,
        ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
            let start = match start_key {
                Some(key) => self.seek(key),
                None => self.last(),
            }
            .transpose();
            Ok(ReverseWalker::new(self, start))
        }
    };
}

/// reth's `DbDupCursorRO`, from the same `with` as [`reads!`]. On a table without duplicates
/// each call is refused with a [`DatabaseError::Other`].
macro_rules! dup_reads {
    () => {
        fn prev_dup(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.prev_dup::<T>(m))
        }

        fn next_dup(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.next_dup::<T>(m))
        }

        fn last_dup(&mut self) -> ValueOnlyResult<T> {
            self.with(|rows, m| rows.last_dup::<T>(m))
        }

        fn next_no_dup(&mut self) -> PairResult<T> {
            self.with(|rows, m| rows.next_no_dup::<T>(m))
        }

        fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
            self.with(|rows, m| rows.next_dup_val::<T>(m))
        }

        /// The first value of `key` whose subkey is at or after `subkey`, never one of another
        /// key (MDBX's `get_both_range`). A miss leaves the cursor unset.
        fn seek_by_key_subkey(&mut self, key: T::Key, subkey: T::SubKey) -> ValueOnlyResult<T> {
            let (key, subkey) = (key.encode(), subkey.encode());
            self.with(|rows, m| {
                let dup = rows.layout::<T>()?;
                let found = rows.seek_by_key_subkey_row(m, dup, key.as_ref(), subkey.as_ref())?;
                rows.dup_value::<T>(m, dup, found)
            })
        }

        /// Starts as MDBX's does: at the first row (no key, no subkey), the key's first row (a
        /// key), the first row of `key` at or after `subkey` (both), or of the table's first key
        /// (a subkey only: on an empty table the walk yields MDBX's `NOTFOUND` error). The walk
        /// then steps through that key's rows.
        fn walk_dup(
            &mut self,
            key: Option<T::Key>,
            subkey: Option<T::SubKey>,
        ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
            self.rows.layout::<T>()?;
            let from_subkey = |this: &mut Self, key: T::Key, subkey: T::SubKey| {
                let (key, subkey) = (key.encode(), subkey.encode());
                this.with(|rows, m| {
                    let dup = rows.layout::<T>()?;
                    let found =
                        rows.seek_by_key_subkey_row(m, dup, key.as_ref(), subkey.as_ref())?;
                    rows.dup_pair::<T>(m, dup, found)
                })
            };
            let start = match (key, subkey) {
                (Some(key), Some(subkey)) => from_subkey(self, key, subkey),
                (Some(key), None) => self.seek_exact(key),
                (None, Some(subkey)) => match self.first() {
                    Ok(Some((key, _))) => from_subkey(self, key, subkey),
                    Ok(None) => Err(DatabaseError::Read(Mdbx::NOT_FOUND.info())),
                    Err(e) => Err(e),
                },
                (None, None) => self.first(),
            }
            .transpose();
            Ok(DupWalker { cursor: self, start })
        }
    };
}

/// A cursor over one table of a read-only transaction ([`crate::TndbTx`]): the engine's
/// [`RoCursor`], which owns a share of the transaction.
#[derive(Debug)]
pub struct TndbCursor<T: Table> {
    cursor: RoCursor,
    rows: Rows,
    _table: PhantomData<T>,
}

impl<T: Table> TndbCursor<T> {
    pub(crate) const fn new(cursor: RoCursor, dup: Option<DupLayout>) -> Self {
        Self { cursor, rows: Rows::new(dup), _table: PhantomData }
    }

    /// Run `f` on the cursor's rows and its engine cursor.
    fn with<R>(
        &mut self,
        f: impl FnOnce(&mut Rows, &mut RoCursor) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        f(&mut self.rows, &mut self.cursor)
    }
}

impl<T: Table> DbCursorRO<T> for TndbCursor<T> {
    reads!();
}

impl<T: DupSort> DbDupCursorRO<T> for TndbCursor<T> {
    dup_reads!();
}

/// A cursor over one table of a read-write transaction ([`crate::TndbTxMut`]): the engine's
/// detached [`TxnCursor`], with a share of the transaction. Each call locks the transaction,
/// moves or writes, decodes, and unlocks. Several cursors and the transaction's own writes see
/// each other's changes: a cursor whose row another one deleted is on the row's follower, as if it
/// had deleted the row itself, as an MDBX cursor is (the crate docs list the two cases where it is
/// not: another writer's change between the row and its follower before the cursor's next call,
/// and, on a dup-sort table, another writer's delete of a key's last row).
#[derive(Debug)]
pub struct TndbCursorMut<T: Table> {
    txn: SharedTxn,
    /// The engine's handle on the table, for a lookup apart from the cursor.
    table: EngineTable,
    cursor: TxnCursor,
    rows: Rows,
    /// The compressed value of the last write (reused).
    buf: Vec<u8>,
    /// The stored key of the last write to a dup-sort table (reused).
    stored: Vec<u8>,
    _table: PhantomData<T>,
}

impl<T: Table> TndbCursorMut<T> {
    pub(crate) const fn new(
        txn: SharedTxn,
        table: EngineTable,
        cursor: TxnCursor,
        dup: Option<DupLayout>,
    ) -> Self {
        Self {
            txn,
            table,
            cursor,
            rows: Rows::new(dup),
            buf: Vec::new(),
            stored: Vec::new(),
            _table: PhantomData,
        }
    }

    /// Run `f` on the cursor's rows and its engine cursor, with the transaction locked; a
    /// finished transaction is `MDBX_BAD_TXN`.
    fn with<R>(
        &mut self,
        f: impl FnOnce(&mut Rows, &mut Locked<'_, '_>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut guard = self.txn.lock();
        let txn = live(&mut guard, error::read)?;
        f(&mut self.rows, &mut Locked { cursor: &mut self.cursor, txn })
    }
}

/// The transaction behind `slot`, else `MDBX_BAD_TXN` through `on_finished`: it committed or
/// aborted.
fn live<E>(
    slot: &mut Option<RwTxn>,
    on_finished: impl FnOnce(tndb_b::Error) -> E,
) -> Result<&mut RwTxn, E> {
    slot.as_mut().ok_or_else(|| on_finished(error::finished()))
}

impl<T: Table> DbCursorRO<T> for TndbCursorMut<T> {
    reads!();
}

impl<T: Table> TndbCursorMut<T> {
    /// A write through the cursor of `key` and `value` by `operation`; see [`Rows::wrote`] for
    /// where it leaves the cursor.
    fn write(
        &mut self,
        key: T::Key,
        value: &T::Value,
        operation: DatabaseWriteOperation,
    ) -> Result<(), DatabaseError> {
        let Self { txn, table, cursor, rows, buf, stored, .. } = self;
        let key = key.encode();
        let key = key.as_ref();
        let value = compressed(value, buf);
        let fail = |e: tndb_b::Error| error::write(&e, operation, T::NAME, key);
        let mut guard = txn.lock();
        let txn = live(&mut guard, fail)?;
        let Some(dup) = rows.dup else {
            let result = match operation {
                DatabaseWriteOperation::CursorInsert => match cursor.insert(txn, key, value) {
                    // MDBX leaves a cursor whose insert it refused on the present key; the engine
                    // leaves it where it was. Damage the seek meets is the call's error.
                    Err(tndb_b::Error::KeyExists) => {
                        cursor.seek(txn, key).and(Err(tndb_b::Error::KeyExists))
                    }
                    inserted => inserted,
                },
                DatabaseWriteOperation::CursorAppend => match cursor.append(txn, key, value) {
                    // MDBX's append takes the table's last key again, replacing its value, and
                    // leaves the cursor ending the table as after `last` (`next` finds nothing,
                    // even once another writer adds a later key); it leaves a cursor whose append
                    // it refused on the last row. The engine's refuses both and leaves the cursor
                    // where it was.
                    Err(tndb_b::Error::AppendOutOfOrder) => {
                        let last_key =
                            cursor.last(txn).map_err(fail)?.is_some_and(|(found, _)| found == key);
                        if last_key {
                            cursor.put(txn, key, value).and_then(|()| cursor.last(txn).map(drop))
                        } else {
                            Err(tndb_b::Error::AppendOutOfOrder)
                        }
                    }
                    appended => appended,
                },
                _ => cursor.put(txn, key, value),
            };
            rows.wrote(&result);
            return result.map_err(fail);
        };
        let rest =
            dup.join(key, value, stored).ok_or_else(|| error::bad_size(operation, T::NAME, key))?;
        let stored = stored.as_slice();
        let result = match operation {
            DatabaseWriteOperation::CursorUpsert => cursor.put(txn, stored, rest),
            DatabaseWriteOperation::CursorInsert => {
                // MDBX refuses a key that has any row, and leaves the cursor on its first. The
                // key's rows are looked for apart from the cursor, so a row the engine refuses (by
                // its size) leaves the cursor where it was.
                let row_of_key =
                    |found: Option<Entry<'_>>| found.is_some_and(|(f, _)| dup.is_row_of(f, key));
                let taken = txn
                    .cursor(table)
                    .and_then(|mut rows_of| rows_of.seek(key).map(row_of_key))
                    .map_err(fail)?;
                if taken {
                    cursor.seek(txn, key).and(Err(tndb_b::Error::KeyExists))
                } else {
                    cursor.insert(txn, stored, rest)
                }
            }
            DatabaseWriteOperation::CursorAppend => match cursor.append(txn, stored, rest) {
                // MDBX's append to a dup-sort table also takes the last key again, with a value
                // anywhere among its rows; a smaller key is refused with the cursor on the last
                // row, as after `last`.
                Err(tndb_b::Error::AppendOutOfOrder) => {
                    let last_key = cursor
                        .last(txn)
                        .map_err(fail)?
                        .is_some_and(|(found, _)| dup.is_row_of(found, key));
                    if last_key {
                        cursor.put(txn, stored, rest)
                    } else {
                        rows.pos = Pos::DupEnd { soft: true };
                        return Err(fail(tndb_b::Error::AppendOutOfOrder));
                    }
                }
                appended => appended,
            },
            // MDBX's `APPEND_DUP`: the value must sort after the key's last, and a key without
            // rows takes any value, wherever it sorts. A refused one leaves the cursor on the
            // key's last row, or on its first when the value is exactly the first's (MDBX finds
            // the key's first value before it appends).
            _ => match cursor.append(txn, stored, rest) {
                Err(tndb_b::Error::AppendOutOfOrder) => {
                    cursor.seek(txn, &dup.past(key)).map_err(fail)?;
                    let after_last = cursor
                        .prev(txn)
                        .map_err(fail)?
                        .filter(|&(found, _)| dup.is_row_of(found, key))
                        .map(|(found, _)| stored > found);
                    match after_last {
                        Some(false) => {
                            let first = cursor.seek(txn, key).map_err(fail)?;
                            if first != Some((stored, rest)) {
                                cursor.seek(txn, &dup.past(key)).map_err(fail)?;
                                cursor.prev(txn).map_err(fail)?;
                            }
                            Err(tndb_b::Error::AppendOutOfOrder)
                        }
                        _ => cursor.put(txn, stored, rest),
                    }
                }
                appended => appended,
            },
        };
        rows.wrote(&result);
        result.map_err(fail)
    }
}

impl<T: Table> DbCursorRW<T> for TndbCursorMut<T> {
    /// On a dup-sort table this stores the row of `value`'s subkey, replacing one with the same
    /// subkey (MDBX would add a second value for that subkey).
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.write(key, value, DatabaseWriteOperation::CursorUpsert)
    }

    /// A present key is refused with MDBX's `MDBX_KEYEXIST`, and nothing changes but the cursor,
    /// which moves to the key, as MDBX's does; on a dup-sort table a key with any row is refused,
    /// and the cursor moves to its first row.
    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.write(key, value, DatabaseWriteOperation::CursorInsert)
    }

    /// A key that sorts before the table's last is refused with MDBX's `MDBX_EKEYMISMATCH`, and
    /// leaves the cursor on the last row. The last key itself is taken again, as MDBX takes it:
    /// its value replaced (the cursor then ends the table, as after `last`), or on a dup-sort
    /// table with a value anywhere among its rows.
    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.write(key, value, DatabaseWriteOperation::CursorAppend)
    }

    /// Right after a delete this deletes the follower, as MDBX's does. With the cursor not at a
    /// row (fresh, unset, past either end) this is MDBX's `MDBX_ENODATA`, and on a dup-sort table
    /// it is right after a delete of a key's last row that left the key earlier rows (the cursor
    /// then ends the key's rows, on the row before, as MDBX's does).
    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        let Self { txn, cursor, rows, .. } = self;
        let dup = match (rows.pos, rows.dup) {
            (Pos::Missed | Pos::DupEnd { soft: false }, _) => return Err(no_data()),
            (_, dup) => dup,
        };
        let mut guard = txn.lock();
        let txn = live(&mut guard, error::delete)?;
        let Some(dup) = dup else {
            return if cursor.delete_current(txn).map_err(error::delete)? {
                Ok(())
            } else {
                Err(no_data())
            };
        };
        // On a dup-sort table: whether the row is its key's last (on the row first: right after
        // a delete, the follower; at a key's end after `last`, the row found again by key, which
        // ends the engine cursor's end), and after the delete whether the key keeps an earlier
        // row.
        let Some((stored, _)) = cursor.current(txn).map_err(error::delete)? else {
            return Err(no_data());
        };
        if rows.pos == (Pos::DupEnd { soft: true }) {
            let stored = stored.to_vec();
            cursor.seek_exact(txn, &stored).map_err(error::delete)?;
        }
        let key_last = cursor.next_in_prefix(txn, dup.key).map_err(error::delete)?.is_none();
        if !key_last {
            cursor.prev(txn).map_err(error::delete)?;
        }
        if !cursor.delete_current(txn).map_err(error::delete)? {
            return Err(no_data());
        }
        rows.pos = if !key_last {
            Pos::Engine
        } else if cursor.prev_in_prefix(txn, dup.key).map_err(error::delete)?.is_some() {
            Pos::DupEnd { soft: false }
        } else {
            Pos::KeyDeleted
        };
        Ok(())
    }
}

impl<T: DupSort> DbDupCursorRO<T> for TndbCursorMut<T> {
    dup_reads!();
}

impl<T: DupSort> DbDupCursorRW<T> for TndbCursorMut<T> {
    /// Deletes every row of the current row's key (right after a delete, of the follower's key,
    /// as MDBX's does), and leaves the cursor on the next key's first row as after a delete
    /// (`next` and `current` return it, `prev_dup` finds nothing). With the cursor not at a row
    /// (fresh, unset, past either end) this is MDBX's `MDBX_ENODATA`.
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        let dup = self.rows.layout::<T>()?;
        if matches!(self.rows.pos, Pos::Fresh | Pos::Missed) {
            return Err(no_data());
        }
        self.rows.pos = Pos::Engine;
        let mut guard = self.txn.lock();
        let txn = live(&mut guard, error::delete)?;
        let key = match self.cursor.current(txn).map_err(error::delete)? {
            Some((stored, _)) => dup.split(stored).ok_or(DatabaseError::Decode)?.0.to_vec(),
            None => return Err(no_data()),
        };
        self.cursor.seek(txn, &key).map_err(error::delete)?;
        while self.cursor.delete_current(txn).map_err(error::delete)? {
            if self.cursor.next_in_prefix(txn, dup.key).map_err(error::delete)?.is_none() {
                break;
            }
        }
        self.rows.pos = Pos::KeyDeleted;
        Ok(())
    }

    /// MDBX's `APPEND_DUP`: `value` must sort after the key's last value (by subkey), else it is
    /// refused with `MDBX_EKEYMISMATCH` and the cursor is left on the key's last row (on its
    /// first when `value` is exactly the first's, as MDBX's is). A key without rows takes any
    /// value, wherever the key sorts.
    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.rows.layout::<T>()?;
        self.write(key, &value, DatabaseWriteOperation::CursorAppendDup)
    }
}
