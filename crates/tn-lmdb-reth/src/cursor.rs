//! reth's cursors: [`TnLmdbCursor`] over a read-only transaction and [`TnLmdbCursorMut`] over a
//! read-write one.
//!
//! Both follow MDBX's rules, most of which are the engine cursors' own: `seek` lands on the first
//! key at or after its key, and after `delete_current` the cursor is on the deleted row's
//! follower, as MDBX's is: `next` and `current` return the follower, `prev` the row before it, and
//! another `delete_current` deletes it. After `last` the cursor ends the table, as MDBX's does (its
//! `Pos::End` state; the engine's cursor is simply on the row): `next` finds nothing and the cursor
//! stays on the row, even once another cursor appends past it. A missed `seek_exact` or
//! `seek_by_key_subkey` leaves the cursor unset, as MDBX's: every relative move (`next`, `prev`,
//! `current` and the dup moves) then finds nothing and a delete has nothing to delete, until an
//! absolute move or a write places it again. After `upsert`, `insert` or `append` the cursor is on
//! the row written; after a refused `insert` it is on the present key, after a refused `append` on
//! the table's last row as after `last`, and after an `append` that replaces the last key's value
//! it ends the table as after `last`, as MDBX leaves it. A write refused for its size leaves the
//! cursor where it was, in whatever state, as MDBX's refusal does.
//!
//! On a dup-sort table (an engine dup table or composite keys, see [`crate::dup`]) a row's value
//! (subkey, then the rest) is read as one slice of the page, since a leaf cell holds its value
//! right after its key: no copy, unless the value is in an overflow run, whose rest is joined to
//! the subkey in a buffer of the cursor's. A move reads the row it lands on in the move itself,
//! by its tail form ([`Moves::first_with_key_tail`] and the others, chosen by [`PickTail`]): on an
//! engine dup table the engine's own tail move, one call that reads the row from the leaf the
//! move fetched; on composite keys the plain engine cursor's move, then its
//! `current_with_key_tail`. `current` is `current_with_key_tail`; `seek_exact`,
//! `seek_by_key_subkey`, and a `walk_dup` started from a key or a subkey, read the row in the seek
//! itself ([`Moves::seek_dup_with_key_tail`]). The moves on the way, whose rows are not returned
//! (the read of the key a seek past its rows starts from, that seek before a step back, the probe
//! step of `next_no_dup` and `last_dup`), stay plain ones. The dup moves are the engine's prefix
//! steps over the key's width: `next_dup` and `prev_dup` stay within
//! the current key's rows, from a fresh cursor `next_dup`, `next_no_dup` and `next_dup_val` go to
//! the first row and `prev_dup` to the last, as MDBX's do. Where MDBX's cursor ends a key's rows,
//! the cursor does too (its `Pos::DupEnd` state): after `last`, and after a delete of a key's last
//! row that leaves the key earlier rows, which puts the cursor on the row before the deleted one.
//! There `next_dup` finds nothing and `next` goes to the next key; right after that delete
//! `current`, `prev` and `prev_dup` return the row before (now the key's last) and `delete_current`
//! is `MDBX_ENODATA`, as MDBX's do. Right after a delete that leaves a key no rows
//! (`Pos::KeyDeleted`) the cursor is on the next key's first row, where `prev_dup` finds nothing,
//! as MDBX's does.
//!
//! A write cursor's `delete_current` on a dup-sort table is held ([`Deferred`], the crate docs' dup
//! rewrite): the cursor's next call runs it first, or with an `upsert` of the same row makes the
//! two one engine `put` through the cursor; any other accessor of the transaction runs it before
//! its own call, and leaves the cursor the engine cursor and [`Pos`] the delete left ([`Parked`]).

use crate::{
    dup::{DupLayout, DUP_LAYOUTS},
    error::{self, Mdbx},
    tx::{compressed, no_data, SharedTxn, TxnState},
};
use parking_lot::Mutex;
use reth_db_api::{
    common::{IterPairResult, PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Decode, Decompress, DupSort, Encode, Table},
    DatabaseError,
};
use reth_storage_errors::db::{DatabaseErrorInfo, DatabaseWriteOperation};
use std::{
    marker::PhantomData,
    num::NonZeroU64,
    ops::{Bound, RangeBounds},
    sync::Arc,
};
use tn_lmdb::{
    DupRoCursor, DupTxnCursor, KeyTail, ReadTxn, RoCursor, RoTxn, RwTxn, Table as EngineTable,
    TxnCursor,
};

/// A stored row: its key and value as the engine holds them.
type Entry<'a> = (&'a [u8], &'a [u8]);

/// A stored row with its value joined to its key's last bytes ([`KeyTail`]).
type Tailed<'a> = (&'a [u8], KeyTail<'a>);

/// The moves of an engine cursor: a read-only one's ([`ReadCursor`]), or a write one's with its
/// transaction locked ([`Locked`]). The adapter's moves ([`Rows`]) are written once over these.
trait Moves {
    fn first(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn last(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn next(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn prev(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn current(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn seek(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn seek_exact(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<&[u8]>>;
    fn next_in_prefix(&mut self, n: usize) -> tn_lmdb::Result<Option<Entry<'_>>>;
    fn current_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    /// The tail forms of the moves: the move, and the row it lands on with its value joined to
    /// the stored key's last `t` bytes ([`KeyTail`]), as `current_with_key_tail(t)` would read it
    /// right after. On a dup engine cursor this is the engine's own tail move, the row read from
    /// the move's leaf access (one engine call); on a plain one the move, then
    /// `current_with_key_tail(t)` once the move landed on a row. A prefix step that does not move
    /// is `None`, as the plain one is.
    fn first_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn last_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn next_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn prev_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn seek_with_key_tail(&mut self, key: &[u8], t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn next_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    fn prev_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>>;
    /// The first row of `key` whose subkey is at or after `subkey`, with its value joined to the
    /// stored key's last `t` bytes, from the seek's own leaf access; never a row of another key on
    /// a dup cursor. `key` is the table's key width (the caller checks). On a plain engine cursor
    /// this is the engine's `seek_with_key_tail(key ‖ subkey, t)` ([`flat_seek_dup`]), which may
    /// land on a later key's row: the caller keeps the row only if it is `key`'s.
    fn seek_dup_with_key_tail(
        &mut self,
        key: &[u8],
        subkey: &[u8],
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>>;
}

/// The engine cursor under a reth read cursor, chosen at open from the table's [`DupLayout`]: a
/// plain one (plain tables and dup-sort tables stored as composite keys) or a dup one (dup-sort
/// tables stored as engine dup tables, [`DupLayout::nested`]).
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the dup arm is the hot one, and the cursor is already boxed: boxing the arm would \
              add a second allocation per new dup cursor and an indirection per call"
)]
pub(crate) enum ReadCursor {
    Flat(RoCursor),
    Dup(DupRoCursor),
}

/// The engine cursor under a reth write cursor, chosen as [`ReadCursor`]'s is.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the dup arm is the hot one; boxing it would allocate for every fresh cursor"
)]
pub(crate) enum WriteCursor {
    Flat(TxnCursor),
    Dup(DupTxnCursor),
}

/// `$call` on the engine cursor `$c` of the [`ReadCursor`] or [`WriteCursor`] `$cursor` (named by
/// its type, `$kind`), whichever its arm: each arm's call has the same signature.
macro_rules! on_cursor {
    ($kind:ident, $cursor:expr, $c:ident => $call:expr) => {
        match $cursor {
            $kind::Flat($c) => $call,
            $kind::Dup($c) => $call,
        }
    };
}

/// A tail move ([`Moves::first_with_key_tail`] and the others) on the engine cursor `$c` of the
/// [`ReadCursor`] or [`WriteCursor`] `$cursor` (named by its type, `$kind`): on a dup one its own
/// tail move `$tail`; on a plain one the move `$plain`, then `current_with_key_tail($at)` once the
/// move landed on a row.
macro_rules! tail_move {
    ($kind:ident, $cursor:expr, $c:ident => $plain:expr, $tail:expr, current($($at:expr),+)) => {
        match $cursor {
            $kind::Flat($c) => {
                if $plain?.is_none() {
                    return Ok(None);
                }
                $c.current_with_key_tail($($at),+)
            }
            $kind::Dup($c) => $tail,
        }
    };
}

impl ReadCursor {
    /// A new, unset cursor over `table` of the read-only transaction `txn`: a dup one if `dup`
    /// says the table is an engine dup table, else a plain one.
    pub(crate) fn open(
        txn: &Arc<RoTxn>,
        table: &EngineTable,
        dup: Option<DupLayout>,
    ) -> tn_lmdb::Result<Self> {
        Ok(if dup.is_some_and(|dup| dup.nested) {
            Self::Dup(txn.dup_cursor_owned(table)?)
        } else {
            Self::Flat(txn.cursor_owned(table)?)
        })
    }

    /// Forget the position: the cursor is then as [`Self::open`] made it (the engine's `reset`).
    #[inline]
    fn reset(&mut self) {
        on_cursor!(ReadCursor, self, c => c.reset());
    }
}

/// The engine read cursors that a read-only transaction's dropped [`TnLmdbCursor`]s gave back, for
/// its next cursors on the same tables: [`crate::TnLmdbTx`]'s `cursor_read` takes one of its table
/// and resets it before it opens a new one, which saves the open (the table's lookup in the
/// snapshot, the transaction's share, and the copies of a new engine cursor's bytes) on every
/// cursor after a table's first. The pool keeps [`Self::PER_TABLE`] cursors a table and drops the
/// others.
///
/// It is shared by the transaction and its cursors, which may outlive it: a cursor dropped after
/// its transaction gives its engine cursor to a pool nobody takes from, freed with the pool's
/// last share. A cursor is keyed by its table's id ([`EngineTable::id`]) and reth's name for the
/// table: every handle that opens a cursor in one transaction names a committed table of its
/// snapshot, and the engine tells two tables apart by id and name, so a damaged catalog that gives
/// two tables one id still never hands one table's cursor to the other. The lock is
/// held for a scan of the pool and a push or a removal; a cursor the pool refuses drops after the
/// lock is released.
#[derive(Default)]
pub(crate) struct ReadPool(Mutex<Vec<(u32, &'static str, Box<ReadCursor>)>>);

impl std::fmt::Debug for ReadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadPool").finish_non_exhaustive()
    }
}

impl ReadPool {
    /// The most cursors of one table the pool keeps: as many as reth's code holds open at once on
    /// one table of a read-only transaction, about.
    pub(crate) const PER_TABLE: usize = 4;

    /// A cursor of the table of `id` and reth's `name` the pool holds, reset, or `None`.
    #[inline]
    pub(crate) fn take(&self, id: u32, name: &'static str) -> Option<Box<ReadCursor>> {
        let mut cursor = {
            let mut pool = self.0.lock();
            let at = pool.iter().rposition(|(table, n, _)| *table == id && *n == name)?;
            pool.swap_remove(at).2
        };
        cursor.reset();
        Some(cursor)
    }

    /// Keep `cursor`, of the table of `id` and `name`, unless the pool holds [`Self::PER_TABLE`] of
    /// that table.
    #[inline]
    pub(crate) fn give_back(&self, id: u32, name: &'static str, cursor: Box<ReadCursor>) {
        let refused = {
            let mut pool = self.0.lock();
            let held = pool.iter().filter(|(table, n, _)| *table == id && *n == name).count();
            if held < Self::PER_TABLE {
                pool.push((id, name, cursor));
                None
            } else {
                Some(cursor)
            }
        };
        drop(refused);
    }

    /// Test-only: the cursors the pool holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0.lock().len()
    }
}

impl Moves for ReadCursor {
    fn first(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.first())
    }
    fn last(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.last())
    }
    fn next(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.next())
    }
    fn prev(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.prev())
    }
    fn current(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.current())
    }
    fn seek(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.seek(key))
    }
    fn seek_exact(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<&[u8]>> {
        on_cursor!(ReadCursor, self, c => c.seek_exact(key))
    }
    fn next_in_prefix(&mut self, n: usize) -> tn_lmdb::Result<Option<Entry<'_>>> {
        on_cursor!(ReadCursor, self, c => c.next_in_prefix(n))
    }
    fn current_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        on_cursor!(ReadCursor, self, c => c.current_with_key_tail(t))
    }
    fn first_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(ReadCursor, self, c => c.first(), c.first_with_key_tail(t), current(t))
    }
    fn last_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(ReadCursor, self, c => c.last(), c.last_with_key_tail(t), current(t))
    }
    fn next_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(ReadCursor, self, c => c.next(), c.next_with_key_tail(t), current(t))
    }
    fn prev_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(ReadCursor, self, c => c.prev(), c.prev_with_key_tail(t), current(t))
    }
    fn seek_with_key_tail(&mut self, key: &[u8], t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(ReadCursor, self, c => c.seek(key), c.seek_with_key_tail(key, t), current(t))
    }
    fn next_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(
            ReadCursor, self,
            c => c.next_in_prefix(n), c.next_in_prefix_with_key_tail(n, t), current(t)
        )
    }
    fn prev_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        tail_move!(
            ReadCursor, self,
            c => c.prev_in_prefix(n), c.prev_in_prefix_with_key_tail(n, t), current(t)
        )
    }
    fn seek_dup_with_key_tail(
        &mut self,
        key: &[u8],
        subkey: &[u8],
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        match self {
            Self::Flat(c) => {
                let mut stack = [0; SEEK_KEY_STACK];
                let mut heap = Vec::new();
                c.seek_with_key_tail(flat_seek_dup(&mut stack, &mut heap, key, subkey), t)
            }
            Self::Dup(c) => c.seek_dup_with_key_tail(key, subkey, t),
        }
    }
}

/// The seek key of a `seek_dup` on a plain engine cursor over composite rows, `key ‖ subkey`:
/// built in `stack` ([`SEEK_KEY_STACK`] bytes), or when longer (only a raw key or subkey can be)
/// in `heap`: the same seek either way.
fn flat_seek_dup<'b>(
    stack: &'b mut [u8; SEEK_KEY_STACK],
    heap: &'b mut Vec<u8>,
    key: &[u8],
    subkey: &[u8],
) -> &'b [u8] {
    if let Some(target) = stack.get_mut(..key.len() + subkey.len()) {
        let (key_part, subkey_part) = target.split_at_mut(key.len());
        key_part.copy_from_slice(key);
        subkey_part.copy_from_slice(subkey);
        target
    } else {
        heap.extend_from_slice(key);
        heap.extend_from_slice(subkey);
        heap
    }
}

/// The engine calls of a [`WriteCursor`], each its arm's call: the moves, which take the
/// transaction shared (and return rows borrowing the cursor and the transaction, as a dup
/// cursor's do), and the writes, which take it exclusively.
impl WriteCursor {
    /// A new, unset cursor over `table` of the write transaction `txn`, detached from it: a dup
    /// one if `dup` says the table is an engine dup table, else a plain one.
    pub(crate) fn open(
        txn: &RwTxn,
        table: &EngineTable,
        dup: Option<DupLayout>,
    ) -> tn_lmdb::Result<Self> {
        Ok(if dup.is_some_and(|dup| dup.nested) {
            Self::Dup(txn.dup_cursor_detached(table)?)
        } else {
            Self::Flat(txn.cursor_detached(table)?)
        })
    }

    fn first<'c>(&'c mut self, txn: &'c RwTxn) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.first(txn))
    }
    fn last<'c>(&'c mut self, txn: &'c RwTxn) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.last(txn))
    }
    fn next<'c>(&'c mut self, txn: &'c RwTxn) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.next(txn))
    }
    fn prev<'c>(&'c mut self, txn: &'c RwTxn) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.prev(txn))
    }
    fn current<'c>(&'c mut self, txn: &'c RwTxn) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.current(txn))
    }
    fn seek<'c>(&'c mut self, txn: &'c RwTxn, key: &[u8]) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.seek(txn, key))
    }
    fn seek_exact<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        key: &[u8],
    ) -> tn_lmdb::Result<Option<&'c [u8]>> {
        on_cursor!(WriteCursor, self, c => c.seek_exact(txn, key))
    }
    fn next_in_prefix<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
    ) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.next_in_prefix(txn, n))
    }
    fn prev_in_prefix<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
    ) -> tn_lmdb::Result<Option<Entry<'c>>> {
        on_cursor!(WriteCursor, self, c => c.prev_in_prefix(txn, n))
    }
    fn current_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        on_cursor!(WriteCursor, self, c => c.current_with_key_tail(txn, t))
    }
    /// The tail moves: see [`Moves::first_with_key_tail`].
    fn first_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self,
            c => c.first(txn), c.first_with_key_tail(txn, t), current(txn, t)
        )
    }
    fn last_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self,
            c => c.last(txn), c.last_with_key_tail(txn, t), current(txn, t)
        )
    }
    fn next_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self,
            c => c.next(txn), c.next_with_key_tail(txn, t), current(txn, t)
        )
    }
    fn prev_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self,
            c => c.prev(txn), c.prev_with_key_tail(txn, t), current(txn, t)
        )
    }
    fn seek_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        key: &[u8],
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self,
            c => c.seek(txn, key), c.seek_with_key_tail(txn, key, t), current(txn, t)
        )
    }
    fn next_in_prefix_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self, c => c.next_in_prefix(txn, n),
            c.next_in_prefix_with_key_tail(txn, n, t), current(txn, t)
        )
    }
    fn prev_in_prefix_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        tail_move!(
            WriteCursor, self, c => c.prev_in_prefix(txn, n),
            c.prev_in_prefix_with_key_tail(txn, n, t), current(txn, t)
        )
    }
    /// See [`Moves::seek_dup_with_key_tail`].
    fn seek_dup_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        key: &[u8],
        subkey: &[u8],
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'c>>> {
        match self {
            Self::Flat(c) => {
                let mut stack = [0; SEEK_KEY_STACK];
                let mut heap = Vec::new();
                c.seek_with_key_tail(txn, flat_seek_dup(&mut stack, &mut heap, key, subkey), t)
            }
            Self::Dup(c) => c.seek_dup_with_key_tail(txn, key, subkey, t),
        }
    }
    fn follower_shares_prefix(&mut self, txn: &RwTxn, n: usize) -> tn_lmdb::Result<bool> {
        on_cursor!(WriteCursor, self, c => c.follower_shares_prefix(txn, n))
    }

    fn put(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> tn_lmdb::Result<()> {
        on_cursor!(WriteCursor, self, c => c.put(txn, key, value))
    }
    fn insert(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> tn_lmdb::Result<()> {
        on_cursor!(WriteCursor, self, c => c.insert(txn, key, value))
    }
    fn append(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> tn_lmdb::Result<()> {
        on_cursor!(WriteCursor, self, c => c.append(txn, key, value))
    }
    fn delete_current(&mut self, txn: &mut RwTxn) -> tn_lmdb::Result<bool> {
        on_cursor!(WriteCursor, self, c => c.delete_current(txn))
    }
}

/// A write cursor with its transaction locked, for one call.
struct Locked<'c, 't> {
    cursor: &'c mut WriteCursor,
    txn: &'t RwTxn,
}

impl Moves for Locked<'_, '_> {
    fn first(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.first(self.txn)
    }
    fn last(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.last(self.txn)
    }
    fn next(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.next(self.txn)
    }
    fn prev(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.prev(self.txn)
    }
    fn current(&mut self) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.current(self.txn)
    }
    fn seek(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.seek(self.txn, key)
    }
    fn seek_exact(&mut self, key: &[u8]) -> tn_lmdb::Result<Option<&[u8]>> {
        self.cursor.seek_exact(self.txn, key)
    }
    fn next_in_prefix(&mut self, n: usize) -> tn_lmdb::Result<Option<Entry<'_>>> {
        self.cursor.next_in_prefix(self.txn, n)
    }
    fn current_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.current_with_key_tail(self.txn, t)
    }
    fn first_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.first_with_key_tail(self.txn, t)
    }
    fn last_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.last_with_key_tail(self.txn, t)
    }
    fn next_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.next_with_key_tail(self.txn, t)
    }
    fn prev_with_key_tail(&mut self, t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.prev_with_key_tail(self.txn, t)
    }
    fn seek_with_key_tail(&mut self, key: &[u8], t: usize) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.seek_with_key_tail(self.txn, key, t)
    }
    fn next_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.next_in_prefix_with_key_tail(self.txn, n, t)
    }
    fn prev_in_prefix_with_key_tail(
        &mut self,
        n: usize,
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.prev_in_prefix_with_key_tail(self.txn, n, t)
    }
    fn seek_dup_with_key_tail(
        &mut self,
        key: &[u8],
        subkey: &[u8],
        t: usize,
    ) -> tn_lmdb::Result<Option<Tailed<'_>>> {
        self.cursor.seek_dup_with_key_tail(self.txn, key, subkey, t)
    }
}

/// Where a cursor is, beyond its engine cursor's state: MDBX tells a fresh cursor from one a
/// missed lookup left unset, which the engine's cursors (unset in both cases) do not, a cursor
/// that ends the table after `last`, and a dup-sort cursor at the end of its key's rows.
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
    /// A cursor over a table without duplicates on the table's last row, where MDBX's cursor ends
    /// the table: left by `last`, a fresh `prev`, and an `append` of the last key (taken
    /// again) or refused for its order. `next` finds nothing and the cursor stays on the row, even
    /// once another writer has put a later key, until another move or a write. The row's key is
    /// kept ([`Rows::end_key`]): once the row is deleted (by another writer, or by the cursor's
    /// own `delete_current`), the engine cursor is on its follower in its deleted state, which
    /// `next` and `current` return, as from [`Pos::Engine`], and the cursor is then as there.
    /// Every other call is as from [`Pos::Engine`].
    End,
    /// Right after its own delete of a dup key's last row that left the key no rows (or its
    /// `delete_current_duplicates`): the engine cursor is on the follower, the next key's first
    /// row, in its deleted state. `prev_dup` finds nothing, as MDBX's does, even once another
    /// writer has put rows of the deleted key again (the engine's step back would take them, as
    /// it compares with the deleted key); every other call is as from [`Pos::Engine`].
    KeyDeleted,
    /// A dup-sort cursor at the end of its key's rows, where MDBX's cursor stops stepping
    /// through them: left by its own delete of the key's last row while the key keeps earlier
    /// rows (the engine cursor is then on the row before the deleted one), and with `soft` by
    /// `last` (and the moves that take the last row as `last` does). The row's key is kept
    /// ([`Rows::end_key`], as for [`Pos::End`]): the steps past the key's rows are built from it,
    /// not from the engine cursor's row, which another accessor's delete of the row moves to the
    /// follower (the next key's first row, which a step from there would skip with its key).
    /// `current` returns the row; `next` and `next_no_dup` the next key's first, by a seek past
    /// the kept key's rows (so rows put since after the row are skipped, as MDBX skips them);
    /// `next_dup` and `next_dup_val` nothing, and set `soft`; `last_dup`, and while not `soft`
    /// `prev` and `prev_dup`, the key's last row; while `soft`, `prev` and `prev_dup` step back
    /// from the row. `delete_current` deletes the row once `soft`, and before is `MDBX_ENODATA`.
    DupEnd { soft: bool },
}

/// The part of a cursor that knows its table's rows: their layout, the cursor's [`Pos`], and the
/// buffer a dup-sort row's value is rebuilt in. Its moves take the engine cursor as [`Moves`].
#[derive(Debug)]
struct Rows {
    dup: Option<DupLayout>,
    pos: Pos,
    value: Vec<u8>,
    /// The stored key of the row a [`Pos::End`] or [`Pos::DupEnd`] cursor is on (reused).
    end_key: Vec<u8>,
}

/// The width of the stack buffer a `seek_by_key_subkey` key, `key ‖ subkey`, is built in on a
/// plain engine cursor ([`flat_seek_dup`]). Every layout's `key + subkey` fits (`StoragesTrie`'s
/// 97 bytes is the widest), so building the seek key from reth's encoded keys allocates nothing;
/// only a longer raw input takes a `Vec`. The encodes themselves may allocate: `StoragesTrie`'s
/// subkey, `StoredNibblesSubKey`, encodes to a `Vec`.
const SEEK_KEY_STACK: usize = 128;

// Every layout's `key ‖ subkey` fits the stack buffer, and its key width the engine's `u16`
// `DupOptions::key_len` ([`DupLayout::options`]), checked at compile time.
const _: () = {
    let mut i = 0;
    while i < DUP_LAYOUTS.len() {
        let layout = DUP_LAYOUTS[i].1;
        assert!(layout.key + layout.subkey <= SEEK_KEY_STACK);
        assert!(layout.key <= u16::MAX as usize);
        i += 1;
    }
};

/// How the moves [`Rows`] makes on both kinds of table give the row they land on: as its stored
/// entry ([`PickEntry`], a table without duplicates), or with its value joined to the stored
/// key's last `t` bytes by the move itself ([`PickTail`], a dup-sort table, `t` the subkey's
/// width: [`Rows::dup_row`]). Such a move is written once, over a pick ([`pair!`]).
trait Pick: Copy {
    /// A row's value as the pick gives it.
    type Value<'a>;
    fn first<'m>(self, m: &'m mut impl Moves) -> Picked<'m, Self>;
    fn last<'m>(self, m: &'m mut impl Moves) -> Picked<'m, Self>;
    fn next<'m>(self, m: &'m mut impl Moves) -> Picked<'m, Self>;
    fn prev<'m>(self, m: &'m mut impl Moves) -> Picked<'m, Self>;
    fn current<'m>(self, m: &'m mut impl Moves) -> Picked<'m, Self>;
    fn seek<'m>(self, m: &'m mut impl Moves, key: &[u8]) -> Picked<'m, Self>;
}

/// The row a [`Pick`]'s move landed on, if any: its stored key and its value as `P` gives it.
type Picked<'m, P> = tn_lmdb::Result<Option<(&'m [u8], <P as Pick>::Value<'m>)>>;

/// The plain moves: a row as its stored entry.
#[derive(Clone, Copy)]
struct PickEntry;

/// The tail moves of `t` bytes: a row with its value joined to the stored key's last `t` bytes.
#[derive(Clone, Copy)]
struct PickTail(usize);

impl Pick for PickEntry {
    type Value<'a> = &'a [u8];
    #[inline(always)]
    fn first<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.first()
    }
    #[inline(always)]
    fn last<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.last()
    }
    #[inline(always)]
    fn next<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.next()
    }
    #[inline(always)]
    fn prev<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.prev()
    }
    #[inline(always)]
    fn current<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.current()
    }
    #[inline(always)]
    fn seek<'m>(self, m: &'m mut impl Moves, key: &[u8]) -> tn_lmdb::Result<Option<Entry<'m>>> {
        m.seek(key)
    }
}

impl Pick for PickTail {
    type Value<'a> = KeyTail<'a>;
    #[inline(always)]
    fn first<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.first_with_key_tail(self.0)
    }
    #[inline(always)]
    fn last<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.last_with_key_tail(self.0)
    }
    #[inline(always)]
    fn next<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.next_with_key_tail(self.0)
    }
    #[inline(always)]
    fn prev<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.prev_with_key_tail(self.0)
    }
    #[inline(always)]
    fn current<'m>(self, m: &'m mut impl Moves) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.current_with_key_tail(self.0)
    }
    #[inline(always)]
    fn seek<'m>(self, m: &'m mut impl Moves, key: &[u8]) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        m.seek_with_key_tail(key, self.0)
    }
}

/// The row of table `T` that the move `$moved` lands on, decoded. `$moved` is written once over
/// the [`Pick`] `$pick` and gives the row it landed on (borrowing the engine cursor), if any: on a
/// table without duplicates it runs with [`PickEntry`] and the row is the entry ([`plain`]); on a
/// dup-sort table with [`PickTail`] of the subkey's width, so the move itself reads the row's
/// whole value ([`Rows::dup_pair`]).
macro_rules! pair {
    ($rows:ident, $pick:ident => $moved:expr) => {
        match $rows.dup {
            None => {
                let $pick = PickEntry;
                plain::<T>($moved)
            }
            Some(dup) => {
                let $pick = PickTail(dup.subkey);
                let row = $moved;
                $rows.dup_pair::<T>(dup, row)
            }
        }
    };
}

/// The row of table `T`, which has no duplicates, stored as `entry`, if any.
fn plain<T: Table>(entry: Option<Entry<'_>>) -> PairResult<T> {
    let Some((key, value)) = entry else { return Ok(None) };
    Ok(Some((T::Key::decode(key)?, T::Value::decompress(value)?)))
}

/// The row of dup-sort table `T` read as its stored key and its full value ([`Rows::dup_row`]),
/// if any, decoded.
#[inline(always)]
fn dup_decoded<T: Table>(dup: DupLayout, row: Option<Entry<'_>>) -> PairResult<T> {
    let Some((key, value)) = row else { return Ok(None) };
    let (key_part, _) = dup.split(key).ok_or(DatabaseError::Decode)?;
    Ok(Some((T::Key::decode(key_part)?, T::Value::decompress(value)?)))
}

/// The value of the row of dup-sort table `T` read as its stored key and its full value
/// ([`Rows::dup_row`]), if any, decoded.
#[inline(always)]
fn dup_value_of<T: Table>(row: Option<Entry<'_>>) -> ValueOnlyResult<T> {
    row.map(|(_, value)| T::Value::decompress(value)).transpose()
}

impl Rows {
    const fn new(dup: Option<DupLayout>) -> Self {
        Self { dup, pos: Pos::Fresh, value: Vec::new(), end_key: Vec::new() }
    }

    /// Where a write through the cursor that ended with `result` leaves it. A write the engine
    /// took, or one refused where MDBX's refusal places the cursor too, which the write paths
    /// place it for (an insert of a present key: on the key, its first row on a dup-sort table;
    /// an append out of order: on the last row, for `append_dup` the key's last or first), leaves
    /// it at the engine cursor's place ([`Pos::Engine`]). A size the engine refuses changes
    /// nothing, the engine cursor included, so the cursor keeps its state, as MDBX's does. A
    /// failure that latches the environment (damage, I/O, a broken invariant) leaves the engine
    /// cursor unset and this one where it was: the transaction can no longer commit.
    fn wrote(&mut self, result: &tn_lmdb::Result<()>) {
        if matches!(
            result,
            Ok(()) | Err(tn_lmdb::Error::KeyExists | tn_lmdb::Error::AppendOutOfOrder)
        ) {
            self.pos = Pos::Engine;
        }
    }

    /// The layout for a dup cursor call on table `T`; a table without duplicates has none.
    fn layout<T: Table>(&self) -> Result<DupLayout, DatabaseError> {
        self.dup.ok_or_else(|| error::dup_sort_unsupported(T::NAME))
    }

    /// The row of the dup-sort table a move landed on, `row` as the move's tail form gave it
    /// ([`PickTail`] of the subkey's width: the engine call that moved), as its stored key and its
    /// full value: the stored key's subkey and the stored value read as one slice of the page, or
    /// for a value in an overflow run joined in the cursor's buffer ([`DupLayout::joined`]).
    #[inline(always)]
    fn dup_row<'a>(
        &'a mut self,
        dup: DupLayout,
        row: Option<Tailed<'a>>,
    ) -> Result<Option<Entry<'a>>, DatabaseError> {
        let Some((key, tail)) = row else { return Ok(None) };
        let value = dup.joined(key, tail, &mut self.value).ok_or(DatabaseError::Decode)?;
        Ok(Some((key, value)))
    }

    /// The row of dup-sort table `T` a move landed on, `row` ([`Rows::dup_row`]).
    fn dup_pair<T: Table>(&mut self, dup: DupLayout, row: Option<Tailed<'_>>) -> PairResult<T> {
        dup_decoded::<T>(dup, self.dup_row(dup, row)?)
    }

    /// The value of the row of dup-sort table `T` a move landed on, `row` ([`Rows::dup_row`]).
    fn dup_value<T: Table>(
        &mut self,
        dup: DupLayout,
        row: Option<Tailed<'_>>,
    ) -> ValueOnlyResult<T> {
        dup_value_of::<T>(self.dup_row(dup, row)?)
    }

    fn first<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        self.pos = Pos::Engine;
        pair!(self, pick => pick.first(m).map_err(error::read)?)
    }

    fn last<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        pair!(self, pick => {
            let row = pick.last(m).map_err(error::read)?;
            self.pos = self.ended(row.as_ref().map(|&(key, _)| key));
            row
        })
    }

    /// The position after a move that took the table's last row as MDBX's `last` does, the row
    /// of the stored key `landed` (`None`: the table is empty): on a dup-sort table, the end of
    /// its key's rows; on another table, the end of the table. The key is kept either way.
    fn ended(&mut self, landed: Option<&[u8]>) -> Pos {
        match landed {
            Some(key) => {
                self.keep_end_key(key);
                if self.dup.is_some() {
                    Pos::DupEnd { soft: true }
                } else {
                    Pos::End
                }
            }
            None => Pos::Engine,
        }
    }

    /// Keep `key`, the stored key of the row an end position is on ([`Rows::end_key`]).
    fn keep_end_key(&mut self, key: &[u8]) {
        self.end_key.clear();
        self.end_key.extend_from_slice(key);
    }

    /// Whether the engine cursor is still on the row a [`Pos::DupEnd`] or [`Pos::End`] cursor
    /// ends on ([`Rows::end_key`]): once another accessor has deleted it, the engine cursor is on
    /// its follower, and the kept key says where MDBX's cursor is.
    fn still_on_end_row(&self, m: &mut impl Moves) -> tn_lmdb::Result<bool> {
        Ok(m.current()?.is_some_and(|(key, _)| key == self.end_key))
    }

    /// Whether `row`, the engine cursor's current row as a [`Pos::End`] cursor's call found it,
    /// is still the row the cursor ends the table at: else another writer deleted it.
    fn on_end_row<V>(&self, row: &Option<(&[u8], V)>) -> bool {
        row.as_ref().is_some_and(|&(key, _)| key == self.end_key)
    }

    fn seek<T: Table>(&mut self, m: &mut impl Moves, key: &[u8]) -> PairResult<T> {
        self.pos = Pos::Engine;
        pair!(self, pick => pick.seek(m, key).map_err(error::read)?)
    }

    /// The row `key` (on a dup-sort table, its first: [`Rows::seek_by_key_subkey_row`] with an
    /// empty subkey). A miss leaves the cursor unset ([`Pos::Missed`]).
    fn seek_exact<T: Table>(&mut self, m: &mut impl Moves, key: &[u8]) -> PairResult<T> {
        let Some(dup) = self.dup else {
            let found = m.seek_exact(key).map_err(error::read)?.map(|value| (key, value));
            self.pos = if found.is_some() { Pos::Engine } else { Pos::Missed };
            return plain::<T>(found);
        };
        let row = self.seek_by_key_subkey_row(m, dup, key, &[])?;
        dup_decoded::<T>(dup, row)
    }

    fn next<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        pair!(self, pick => {
            let row = match std::mem::replace(&mut self.pos, Pos::Engine) {
                Pos::Missed => {
                    self.pos = Pos::Missed;
                    return Ok(None);
                }
                Pos::DupEnd { .. } => {
                    past_dup_end(pick, m, self.layout::<T>()?, &self.end_key, false)
                }
                // Nothing, the cursor staying on the row; once another writer has deleted it,
                // the follower, which `current` returns as `next` would.
                Pos::End => {
                    let row = pick.current(m).map_err(error::read)?;
                    if self.on_end_row(&row) {
                        self.pos = Pos::End;
                        return Ok(None);
                    }
                    Ok(row)
                }
                Pos::Fresh | Pos::Engine | Pos::KeyDeleted => pick.next(m),
            };
            row.map_err(error::read)?
        })
    }

    fn prev<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        pair!(self, pick => {
            let row = match std::mem::replace(&mut self.pos, Pos::Engine) {
                Pos::Missed => {
                    self.pos = Pos::Missed;
                    return Ok(None);
                }
                Pos::DupEnd { soft } => match self.still_on_end_row(m) {
                    Ok(true) if soft => pick.prev(m),
                    Ok(_) => past_dup_end(pick, m, self.layout::<T>()?, &self.end_key, true),
                    Err(e) => Err(e),
                },
                // From a fresh cursor the last row, which MDBX takes by its `last`.
                Pos::Fresh => {
                    let row = pick.prev(m);
                    self.pos = self.ended(landed(&row));
                    row
                }
                Pos::Engine | Pos::End | Pos::KeyDeleted => pick.prev(m),
            };
            row.map_err(error::read)?
        })
    }

    fn current<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        if self.pos == Pos::Missed {
            return Ok(None);
        }
        if matches!(self.pos, Pos::End | Pos::DupEnd { .. })
            && !self.still_on_end_row(m).map_err(error::read)?
        {
            if self.pos == Pos::End {
                self.pos = Pos::Engine;
            } else {
                // Another accessor deleted the row: MDBX's cursor is past the key's rows, and
                // reports the key's last row, or the follower once the key has none.
                let dup = self.layout::<T>()?;
                let row = past_dup_end_gone(PickTail(dup.subkey), m, dup, &self.end_key)
                    .map_err(error::read)?;
                return self.dup_pair::<T>(dup, row);
            }
        }
        pair!(self, pick => pick.current(m).map_err(error::read)?)
    }

    /// Move to the first row of `key` whose subkey is at or after `subkey` (MDBX's
    /// `get_both_range`) and return it, as [`Rows::dup_row`] does: never a row of another key, read
    /// from the leaf access of the seek itself ([`Moves::seek_dup_with_key_tail`]). A miss, and a
    /// `key` of another width than the table's (only a raw key can be: no row's, so no engine
    /// call), leaves the cursor unset ([`Pos::Missed`]).
    fn seek_by_key_subkey_row<'a>(
        &'a mut self,
        m: &'a mut impl Moves,
        dup: DupLayout,
        key: &[u8],
        subkey: &[u8],
    ) -> Result<Option<Entry<'a>>, DatabaseError> {
        if key.len() != dup.key {
            self.pos = Pos::Missed;
            return Ok(None);
        }
        let row = m
            .seek_dup_with_key_tail(key, subkey, dup.subkey)
            .map_err(error::read)?
            .filter(|&(stored, _)| dup.is_row_of(stored, key));
        self.pos = if row.is_some() { Pos::Engine } else { Pos::Missed };
        self.dup_row(dup, row)
    }

    /// The next row of the current key, from its move's tail form ([`Rows::dup_row`]).
    fn next_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        match self.pos {
            Pos::Fresh => {
                self.pos = Pos::Engine;
                m.first_with_key_tail(dup.subkey)
            }
            Pos::Missed => Ok(None),
            Pos::DupEnd { .. } => {
                self.pos = Pos::DupEnd { soft: true };
                Ok(None)
            }
            Pos::Engine | Pos::End => m.next_in_prefix_with_key_tail(dup.key, dup.subkey),
            Pos::KeyDeleted => {
                let row = m.next_in_prefix_with_key_tail(dup.key, dup.subkey);
                if matches!(row, Ok(Some(_))) {
                    self.pos = Pos::Engine;
                }
                row
            }
        }
    }

    /// The previous row of the current key, from its move's tail form ([`Rows::dup_row`]).
    fn prev_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Fresh => {
                let row = m.last_with_key_tail(dup.subkey);
                self.pos = self.ended(landed(&row));
                row
            }
            pos @ (Pos::Missed | Pos::KeyDeleted) => {
                self.pos = pos;
                Ok(None)
            }
            Pos::DupEnd { soft } => match self.still_on_end_row(m) {
                Ok(true) if soft => m.prev_in_prefix_with_key_tail(dup.key, dup.subkey),
                Ok(_) => past_dup_end(PickTail(dup.subkey), m, dup, &self.end_key, true),
                Err(e) => Err(e),
            },
            Pos::Engine | Pos::End => m.prev_in_prefix_with_key_tail(dup.key, dup.subkey),
        }
    }

    /// The first row of the next key: past the current key's rows by a `seek` of
    /// [`DupLayout::past`] when another row of the key follows, else the next row (which is also
    /// right after a delete, past either end, and before the first row); from a fresh cursor, the
    /// first row. The last move is a tail form ([`Rows::dup_row`]).
    fn next_no_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        match std::mem::replace(&mut self.pos, Pos::Engine) {
            Pos::Fresh => return m.first_with_key_tail(dup.subkey),
            Pos::Missed => {
                self.pos = Pos::Missed;
                return Ok(None);
            }
            Pos::DupEnd { .. } => {
                return past_dup_end(PickTail(dup.subkey), m, dup, &self.end_key, false)
            }
            Pos::Engine | Pos::End | Pos::KeyDeleted => {}
        }
        match m.next_in_prefix(dup.key)?.map(|(stored, _)| dup.past(stored)) {
            Some(past) => m.seek_with_key_tail(&past, dup.subkey),
            None => m.next_with_key_tail(dup.subkey),
        }
    }

    /// The last row of the current key: past the key's rows and back one when another row of the
    /// key follows, else the current row. Right after a delete the current row is the follower,
    /// whose key MDBX takes too. The cursor then ends the key's rows, as MDBX's `last_dup` does
    /// ([`Pos::DupEnd`], soft): `next_dup` finds nothing, even after another writer put a row
    /// after it in the key. The last move is a tail form, or `current_with_key_tail`
    /// ([`Rows::dup_row`]).
    fn last_dup_row<'m>(
        &mut self,
        m: &'m mut impl Moves,
        dup: DupLayout,
    ) -> tn_lmdb::Result<Option<Tailed<'m>>> {
        let row = match self.pos {
            Pos::Fresh | Pos::Missed => return Ok(None),
            Pos::DupEnd { .. } => {
                // The key's last row, wherever the engine cursor is. Once another accessor has
                // emptied the key, MDBX's cursor is on the follower, and this is its key's last.
                let key = &self.end_key[..dup.key];
                let on_key = {
                    let row = past_dup_end(PickEntry, m, dup, &self.end_key, true)?;
                    row.is_some_and(|(stored, _)| dup.is_row_of(stored, key))
                };
                if on_key {
                    m.current_with_key_tail(dup.subkey)?
                } else if m.seek(&dup.past(&self.end_key))?.is_none() {
                    None
                } else {
                    last_dup_of_current_key(m, dup)?
                }
            }
            Pos::Engine | Pos::End | Pos::KeyDeleted => {
                if m.current()?.is_none() {
                    return Ok(None);
                }
                last_dup_of_current_key(m, dup)?
            }
        };
        self.pos = match &row {
            Some((key, _)) => {
                let key = *key;
                self.keep_end_key(key);
                Pos::DupEnd { soft: true }
            }
            None => Pos::Engine,
        };
        Ok(row)
    }

    fn next_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let row = self.next_dup_row(m, dup).map_err(error::read)?;
        self.dup_pair::<T>(dup, row)
    }

    fn prev_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let row = self.prev_dup_row(m, dup).map_err(error::read)?;
        self.dup_pair::<T>(dup, row)
    }

    fn next_no_dup<T: Table>(&mut self, m: &mut impl Moves) -> PairResult<T> {
        let dup = self.layout::<T>()?;
        let row = self.next_no_dup_row(m, dup).map_err(error::read)?;
        self.dup_pair::<T>(dup, row)
    }

    fn last_dup<T: Table>(&mut self, m: &mut impl Moves) -> ValueOnlyResult<T> {
        let dup = self.layout::<T>()?;
        let row = self.last_dup_row(m, dup).map_err(error::read)?;
        self.dup_value::<T>(dup, row)
    }

    fn next_dup_val<T: Table>(&mut self, m: &mut impl Moves) -> ValueOnlyResult<T> {
        let dup = self.layout::<T>()?;
        let row = self.next_dup_row(m, dup).map_err(error::read)?;
        self.dup_value::<T>(dup, row)
    }
}

/// From a [`Pos::DupEnd`] row of stored key `end_key` ([`Rows::end_key`]), past its key's rows by
/// a `seek` of [`DupLayout::past`] (which also steps past the end an engine cursor's `last`
/// sets): the next key's first row, or with `back` the key's last row, its last move `pick`'s.
/// The kept key, not the engine cursor's row: another accessor's delete of the row leaves the
/// engine cursor on the follower, the next key's first row, whose key a step past would skip.
fn past_dup_end<'m, P: Pick>(
    pick: P,
    m: &'m mut impl Moves,
    dup: DupLayout,
    end_key: &[u8],
    back: bool,
) -> Picked<'m, P> {
    let past = dup.past(end_key);
    if back {
        m.seek(&past)?;
        pick.prev(m)
    } else {
        pick.seek(m, &past)
    }
}

/// From a [`Pos::DupEnd`] row of stored key `end_key` that another accessor deleted, where
/// MDBX's cursor is past the key's rows: the key's last row, or, the key left no row, the next
/// key's first (the follower), its last move `pick`'s.
fn past_dup_end_gone<'m, P: Pick>(
    pick: P,
    m: &'m mut impl Moves,
    dup: DupLayout,
    end_key: &[u8],
) -> Picked<'m, P> {
    let key = &end_key[..dup.key];
    let on_key = {
        let row = past_dup_end(PickEntry, m, dup, end_key, true)?;
        row.is_some_and(|(stored, _)| dup.is_row_of(stored, key))
    };
    if on_key {
        pick.current(m)
    } else {
        pick.seek(m, &dup.past(end_key))
    }
}

/// The last row of the key of the row the engine cursor is on (there is one): past the key's
/// rows and back one when another row of the key follows, else the current row. The last move is
/// a tail form, or `current_with_key_tail`.
fn last_dup_of_current_key<'m>(
    m: &'m mut impl Moves,
    dup: DupLayout,
) -> tn_lmdb::Result<Option<Tailed<'m>>> {
    match m.next_in_prefix(dup.key)?.map(|(stored, _)| dup.past(stored)) {
        Some(past) => {
            m.seek(&past)?;
            m.prev_with_key_tail(dup.subkey)
        }
        None => m.current_with_key_tail(dup.subkey),
    }
}

/// The stored key of the row a move landed on, if it landed on one.
fn landed<'a, V>(moved: &tn_lmdb::Result<Option<(&'a [u8], V)>>) -> Option<&'a [u8]> {
    match moved {
        Ok(Some((key, _))) => Some(key),
        _ => None,
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
                let row = rows.seek_by_key_subkey_row(m, dup, key.as_ref(), subkey.as_ref())?;
                dup_value_of::<T>(row)
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
                    let row = rows.seek_by_key_subkey_row(m, dup, key.as_ref(), subkey.as_ref())?;
                    dup_decoded::<T>(dup, row)
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

/// A cursor over one table of a read-only transaction ([`crate::TnLmdbTx`]): the engine's
/// [`RoCursor`] or, on an engine dup table, [`DupRoCursor`] (`ReadCursor`), which owns a share
/// of the transaction.
#[derive(Debug)]
pub struct TnLmdbCursor<T: Table> {
    /// The engine cursor, held until the cursor drops and gives it back to `pool`.
    cursor: Option<Box<ReadCursor>>,
    rows: Rows,
    /// The transaction's pool, which takes the engine cursor back on drop.
    pool: Arc<ReadPool>,
    /// The engine table's id, with `T::NAME` the cursor's key in `pool`.
    table: u32,
    _table: PhantomData<T>,
}

impl<T: Table> TnLmdbCursor<T> {
    pub(crate) const fn new(
        cursor: Box<ReadCursor>,
        dup: Option<DupLayout>,
        pool: Arc<ReadPool>,
        table: u32,
    ) -> Self {
        Self { cursor: Some(cursor), rows: Rows::new(dup), pool, table, _table: PhantomData }
    }

    /// Run `f` on the cursor's rows and its engine cursor.
    #[inline]
    fn with<R>(
        &mut self,
        f: impl FnOnce(&mut Rows, &mut ReadCursor) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let cursor = self.cursor.as_deref_mut().expect("the engine cursor is held until drop");
        f(&mut self.rows, cursor)
    }
}

impl<T: Table> Drop for TnLmdbCursor<T> {
    fn drop(&mut self) {
        if let Some(cursor) = self.cursor.take() {
            self.pool.give_back(self.table, T::NAME, cursor);
        }
    }
}

impl<T: Table> DbCursorRO<T> for TnLmdbCursor<T> {
    reads!();
}

impl<T: DupSort> DbDupCursorRO<T> for TnLmdbCursor<T> {
    dup_reads!();
}

/// A dup-sort cursor's `delete_current` held back from the engine, in the write transaction's
/// shared state ([`TxnState`]). reth rewrites a dup row as `seek_by_key_subkey`, `delete_current`,
/// then `upsert` of the same subkey; held back, the delete and the upsert become one engine `put`
/// through the cursor, a replace of the row. Any other call runs the delete first, so no call sees
/// the row and every cursor is where the delete would have left it:
/// - the cursor's own next call, through its engine cursor, which is still on the row (no accessor
///   came between), with the steps the delete takes on a dup-sort table ([`after_delete`]);
/// - any other accessor's call ([`Self::flush`]), through an engine cursor of its own placed on the
///   row, which then equals the cursor's: it deletes the row and takes those steps at once, and
///   leaves itself and the [`Pos`] it found for the cursor ([`Parked`]), which takes them in place
///   of its own at its next call. The delete's steps so happen before any later write, as the
///   delete's own would (an engine cursor finds its place again only at its next call, so the
///   cursor's own, left on the row, would land on whatever a later write put after the row).
///
/// A failure of a held delete is the error of the call that runs it (the commit's, if that runs
/// it), and is kept ([`Self::failure`]): the transaction's commit refuses it, since the cursor's
/// `delete_current` reported the delete done. So is a refused wipe of a key on an engine dup table
/// ([`TnLmdbCursorMut::delete_current_duplicates`]), which may have deleted rows.
#[derive(Debug, Default)]
pub(crate) struct Deferred {
    /// The held delete, if any (at most one a transaction).
    pending: Option<Pending>,
    /// The held row's stored key, `k ‖ s` (reused).
    key: Vec<u8>,
    /// The engine cursors and positions other accessors left after running a cursor's held delete.
    parked: Vec<Parked>,
    /// The first failure of a held delete, or a refused wipe's: the commit refuses it.
    failed: Option<DatabaseErrorInfo>,
    /// The last held delete's number; each cursor knows its own by it.
    last: u64,
    /// Test-only: the rewrites made as one engine put ([`FUSED`]), and the held deletes run as
    /// deletes by their cursors ([`BY_OWNER`]) and by other accessors ([`BY_OTHER`]).
    #[cfg(test)]
    counts: [u64; 3],
    /// Test-only: the next held delete to run fails, with an error that does not latch the
    /// environment ([`Self::fail_next`]).
    #[cfg(test)]
    fail_next: bool,
    /// Test-only: the next [`Self::flush`] does not run the held delete, as a call that broke the
    /// ordering would ([`Self::skip_next_flush`]).
    #[cfg(test)]
    skip_next_flush: bool,
    /// Test-only: the next wipe of a key on an engine dup table
    /// ([`TnLmdbCursorMut::delete_current_duplicates`]) reads as this fault
    /// ([`Self::break_next_wipe`]).
    #[cfg(test)]
    wipe_fault: Option<WipeFault>,
}

/// Test-only: how [`Deferred::break_next_wipe`] makes a wipe of a key on an engine dup table read,
/// as a tree and a cursor that disagree would.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum WipeFault {
    /// The wipe removed no row.
    NoRowRemoved,
    /// The wipe left a row of the key after the cursor.
    RowLeft,
}

/// The test-only counts of [`Deferred`]: the rewrites made as one engine put, the held deletes
/// run by their cursors (a call of the cursor, its drop, or its refused rewrite), and those run by
/// other accessors ([`Deferred::flush`]).
const FUSED: usize = 0;
const BY_OWNER: usize = 1;
const BY_OTHER: usize = 2;

/// A held delete: its number, the cursor's table and layout.
#[derive(Debug)]
struct Pending {
    number: NonZeroU64,
    table: EngineTable,
    dup: DupLayout,
}

/// What another accessor that ran a held delete leaves for its cursor: the engine cursor it ran it
/// with, on the deleted row's follower, and the cursor's [`Pos`] after it.
#[derive(Debug)]
struct Parked {
    number: NonZeroU64,
    cursor: WriteCursor,
    pos: Pos,
    /// The row's key when `pos` is [`Pos::DupEnd`] ([`Rows::end_key`]).
    end_key: Vec<u8>,
}

impl Deferred {
    /// Hold the delete of the row of stored key `key` in `table` (of layout `dup`), the row a
    /// cursor is on, and return its number: the cursor's own name for it. Every call runs a held
    /// delete before its own work ([`settle`]), so none is held here; if one were, holding a second
    /// would drop it and a row reth deleted would survive the commit, so this refuses, and the
    /// commit refuses the transaction ([`Self::fail`]).
    fn hold(
        &mut self,
        table: &EngineTable,
        dup: DupLayout,
        key: &[u8],
    ) -> tn_lmdb::Result<NonZeroU64> {
        if self.pending.is_some() {
            let what = "a second delete was held before the first ran";
            return Err(self.fail(invariant_broken(table, what)));
        }
        self.last += 1;
        let number = NonZeroU64::new(self.last).expect("numbered from 1");
        self.key.clear();
        self.key.extend_from_slice(key);
        self.pending = Some(Pending { number, table: table.clone(), dup });
        Ok(number)
    }

    /// The held delete `number`, taken, if it is still held (no other accessor ran it).
    fn take(&mut self, number: NonZeroU64) -> Option<Pending> {
        self.pending.take_if(|p| p.number == number)
    }

    /// The held delete `number`, taken, if it is still held and of the row of stored key `key`.
    fn take_rewrite(&mut self, number: NonZeroU64, key: &[u8]) -> Option<Pending> {
        let held = &self.key;
        self.pending.take_if(|p| p.number == number && held.as_slice() == key)
    }

    /// What another accessor left for the cursor of held delete `number`, taken.
    fn unpark(&mut self, number: NonZeroU64) -> Option<Parked> {
        let at = self.parked.iter().position(|p| p.number == number)?;
        Some(self.parked.swap_remove(at))
    }

    /// Run the held delete, if any, as every accessor but its cursor does before its call: through
    /// an engine cursor of its own, left for the cursor ([`Parked`]). A failure is kept
    /// ([`Self::failure`]) and returned.
    #[inline]
    pub(crate) fn flush(&mut self, txn: &mut RwTxn) -> tn_lmdb::Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.skip_next_flush) {
            return Ok(());
        }
        if self.pending.is_none() {
            return Ok(());
        }
        self.flush_held(txn)
    }

    #[inline(never)]
    fn flush_held(&mut self, txn: &mut RwTxn) -> tn_lmdb::Result<()> {
        let Some(Pending { number, table, dup }) = self.pending.take() else { return Ok(()) };
        let placed = WriteCursor::open(txn, &table, Some(dup)).and_then(|mut cursor| {
            let on_row = cursor.seek_exact(txn, &self.key)?.is_some();
            if on_row {
                Ok(cursor)
            } else {
                Err(held_row_missing(&table))
            }
        });
        let mut cursor = placed.map_err(|e| self.fail(e))?;
        self.count(BY_OTHER);
        let mut end_key = Vec::new();
        let pos = self.run(&mut cursor, txn, &table, dup, &mut end_key)?;
        self.parked.push(Parked { number, cursor, pos, end_key });
        Ok(())
    }

    /// Run a held delete in `table` (of layout `dup`) through `cursor`, an engine cursor on its
    /// row, and return where the reth cursor then is ([`after_delete`], the row's key in
    /// `end_key` when it ends its key's rows); a failure is kept ([`Self::failure`]).
    fn run(
        &mut self,
        cursor: &mut WriteCursor,
        txn: &mut RwTxn,
        table: &EngineTable,
        dup: DupLayout,
        end_key: &mut Vec<u8>,
    ) -> tn_lmdb::Result<Pos> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next) {
            return Err(self.fail(tn_lmdb::Error::StaleTable(table.name().into())));
        }
        let ran = match cursor.delete_current(txn) {
            Ok(true) => after_delete(cursor, txn, dup, end_key),
            Ok(false) => Err(held_row_missing(table)),
            Err(e) => Err(e),
        };
        ran.map_err(|e| self.fail(e))
    }

    /// Keep the failure `e` of a held delete or a wipe (the first one), for the commit to refuse.
    fn fail(&mut self, e: tn_lmdb::Error) -> tn_lmdb::Error {
        if self.failed.is_none() {
            self.failed = Some(error::info(&e));
        }
        e
    }

    /// The first failure of a held delete or a wipe, if one failed: the transaction must not
    /// commit.
    pub(crate) fn failure(&self) -> Option<DatabaseErrorInfo> {
        self.failed.clone()
    }

    /// Drop the held delete, as the transaction aborts.
    pub(crate) fn discard(&mut self) {
        self.pending = None;
    }

    /// Count `what` ([`FUSED`], [`BY_OWNER`] or [`BY_OTHER`]) in the test-only counts.
    #[inline(always)]
    fn count(&mut self, what: usize) {
        #[cfg(test)]
        {
            self.counts[what] += 1;
        }
        #[cfg(not(test))]
        let _ = what;
    }

    /// Test-only: the rewrites made as one engine put, and the held deletes run as deletes by
    /// their cursors and by other accessors.
    #[cfg(test)]
    pub(crate) const fn counts(&self) -> [u64; 3] {
        self.counts
    }

    /// Test-only: make the next held delete to run fail, as an engine error that does not latch
    /// the environment would.
    #[cfg(test)]
    pub(crate) fn fail_next(&mut self) {
        self.fail_next = true;
    }

    /// Test-only: make the next [`Self::flush`] leave the held delete held, as a call that broke
    /// the ordering would.
    #[cfg(test)]
    pub(crate) fn skip_next_flush(&mut self) {
        self.skip_next_flush = true;
    }

    /// Test-only: make the next wipe of a key on an engine dup table read as `fault`.
    #[cfg(test)]
    pub(crate) fn break_next_wipe(&mut self, fault: WipeFault) {
        self.wipe_fault = Some(fault);
    }
}

/// Whether `result` is a failure the engine latches the environment on
/// ([`tn_lmdb::Error::latches`]: damage, I/O, a full file, a broken invariant), after which its
/// write cursors are unset; a refusal (a size, `KeyExists`, ...) is not.
fn latches(result: &tn_lmdb::Result<()>) -> bool {
    result.as_ref().is_err_and(tn_lmdb::Error::latches)
}

/// A held delete's row was not where its cursor found it: no accessor writes before running it,
/// so the table is damaged or the adapter wrong.
fn held_row_missing(table: &EngineTable) -> tn_lmdb::Error {
    invariant_broken(table, "the row of a held delete is missing")
}

/// The error for a write that broke the adapter's invariant (`what` happened in `table`): a held
/// delete, or a wipe of a key on an engine dup table. The engine's `Failed` (`MDBX_PANIC`), with a
/// message saying what it is, since nothing latched: the commit refuses this write transaction
/// ([`Deferred::fail`]), and the environment and later transactions carry on.
fn invariant_broken(table: &EngineTable, what: &str) -> tn_lmdb::Error {
    tn_lmdb::Error::Failed(format!(
        "tn-lmdb-reth refuses this write transaction (the environment did not latch): {what} in \
         table {:?}, damage or an adapter bug",
        table.name()
    ))
}

/// Where a cursor over a dup-sort table of layout `dup` is right after its engine cursor deleted
/// a row: the key keeps later rows if the follower is the key's (the engine tells from the keys it
/// kept, without a step: [`Pos::Engine`]), else earlier ones if a step back finds one (the cursor
/// then on it, ending the key's rows: [`Pos::DupEnd`], the row's key kept in `end_key`), else
/// none ([`Pos::KeyDeleted`]).
fn after_delete(
    cursor: &mut WriteCursor,
    txn: &RwTxn,
    dup: DupLayout,
    end_key: &mut Vec<u8>,
) -> tn_lmdb::Result<Pos> {
    if cursor.follower_shares_prefix(txn, dup.key)? {
        return Ok(Pos::Engine);
    }
    Ok(match cursor.prev_in_prefix(txn, dup.key)? {
        Some((stored, _)) => {
            end_key.clear();
            end_key.extend_from_slice(stored);
            Pos::DupEnd { soft: false }
        }
        None => Pos::KeyDeleted,
    })
}

/// Before a call of a cursor with the transaction locked (`state`): its held delete `held`, if
/// any, run through its engine cursor `cursor` (still on the row) with its [`Pos`] set in `rows`,
/// or, once another accessor ran it, the engine cursor and `Pos` that accessor left taken in place
/// of its own; then another cursor's held delete run ([`Deferred::flush`]). A failure is the
/// call's, as a delete error.
#[inline]
fn settle(
    state: &mut TxnState,
    held: &mut Option<NonZeroU64>,
    cursor: &mut WriteCursor,
    rows: &mut Rows,
) -> Result<(), DatabaseError> {
    let TxnState { txn, deferred } = state;
    if let Some(number) = held.take() {
        match (deferred.take(number), txn.as_mut()) {
            (Some(Pending { table, dup, .. }), Some(txn)) => {
                deferred.count(BY_OWNER);
                rows.pos = deferred
                    .run(cursor, txn, &table, dup, &mut rows.end_key)
                    .map_err(error::delete)?;
            }
            _ => {
                if let Some(parked) = deferred.unpark(number) {
                    *cursor = parked.cursor;
                    rows.pos = parked.pos;
                    rows.end_key = parked.end_key;
                }
            }
        }
    }
    match txn {
        Some(txn) => deferred.flush(txn).map_err(error::delete),
        None => Ok(()),
    }
}

/// A cursor over one table of a read-write transaction ([`crate::TnLmdbTxMut`]): the engine's
/// detached [`TxnCursor`] or, on an engine dup table, [`DupTxnCursor`] (`WriteCursor`), with a
/// share of the transaction. Each call locks the transaction, runs a held `delete_current` (its
/// own or another cursor's, `Deferred`), moves or writes, decodes, and unlocks. Several cursors and
/// the transaction's own writes see each other's changes: a cursor whose row another one deleted is
/// on the row's follower, as if it had deleted the row itself, as an MDBX cursor is (the crate
/// docs' cursor positions list where it is not: another writer's delete of the cursor's row, then a
/// put between the row and its follower or of the row's key, before the cursor's next call; and, on
/// a dup-sort table, another writer's put or delete of a row of the cursor's dup key).
#[derive(Debug)]
pub struct TnLmdbCursorMut<T: Table> {
    txn: SharedTxn,
    /// The engine's handle on the table, for a lookup apart from the cursor.
    table: EngineTable,
    cursor: WriteCursor,
    rows: Rows,
    /// The compressed value of the last write (reused).
    buf: Vec<u8>,
    /// The stored key of the last write to a dup-sort table (reused).
    stored: Vec<u8>,
    /// The number of the cursor's held `delete_current` ([`Deferred`]), until a call of the
    /// cursor runs it (or makes it part of a rewrite) or finds that another accessor ran it.
    held: Option<NonZeroU64>,
    _table: PhantomData<T>,
}

impl<T: Table> TnLmdbCursorMut<T> {
    pub(crate) const fn new(
        txn: SharedTxn,
        table: EngineTable,
        cursor: WriteCursor,
        dup: Option<DupLayout>,
    ) -> Self {
        Self {
            txn,
            table,
            cursor,
            rows: Rows::new(dup),
            buf: Vec::new(),
            stored: Vec::new(),
            held: None,
            _table: PhantomData,
        }
    }

    /// Run `f` on the cursor's rows and its engine cursor, with the transaction locked and a held
    /// delete run ([`settle`]); a finished transaction is `MDBX_BAD_TXN`.
    fn with<R>(
        &mut self,
        f: impl FnOnce(&mut Rows, &mut Locked<'_, '_>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let Self { txn, cursor, rows, held, .. } = self;
        let mut guard = txn.lock();
        let state = &mut *guard;
        settle(state, held, cursor, rows)?;
        let txn = live(&mut state.txn, error::read)?;
        f(rows, &mut Locked { cursor, txn })
    }
}

/// A cursor dropped with its delete held runs it, as its next call would; it cannot report a
/// failure, which is kept for the commit to refuse (`Deferred::failure`).
impl<T: Table> Drop for TnLmdbCursorMut<T> {
    fn drop(&mut self) {
        let Some(number) = self.held.take() else { return };
        let mut guard = self.txn.lock();
        let TxnState { txn, deferred } = &mut *guard;
        match (deferred.take(number), txn.as_mut()) {
            (Some(Pending { table, dup, .. }), Some(txn)) => {
                // A failure is kept by `run`; where the cursor lands no longer matters.
                deferred.count(BY_OWNER);
                drop(deferred.run(&mut self.cursor, txn, &table, dup, &mut self.rows.end_key));
            }
            _ => drop(deferred.unpark(number)),
        }
    }
}

/// The transaction behind `slot`, else `MDBX_BAD_TXN` through `on_finished`: it committed or
/// aborted.
fn live<E>(
    slot: &mut Option<RwTxn>,
    on_finished: impl FnOnce(tn_lmdb::Error) -> E,
) -> Result<&mut RwTxn, E> {
    slot.as_mut().ok_or_else(|| on_finished(error::finished()))
}

impl<T: Table> DbCursorRO<T> for TnLmdbCursorMut<T> {
    reads!();
}

impl<T: Table> TnLmdbCursorMut<T> {
    /// A write through the cursor of `key` and `value` by `operation`; see [`Rows::wrote`] for
    /// where it leaves the cursor. A second value for a dup-sort row's subkey is refused before
    /// that ([`insert_dup_at`]), so it leaves the cursor as it was. An `upsert` of the row whose
    /// delete the cursor holds ([`Deferred`]) is one engine `put` through the cursor, a replace of
    /// the row, which leaves the cursor on it as the insert after the delete would.
    fn write(
        &mut self,
        key: T::Key,
        value: &T::Value,
        operation: DatabaseWriteOperation,
    ) -> Result<(), DatabaseError> {
        let Self { txn, table, cursor, rows, buf, stored, held, .. } = self;
        let key = key.encode();
        let key = key.as_ref();
        let value = compressed(value, buf);
        let fail = |e: tn_lmdb::Error| error::write(&e, operation, T::NAME, key);
        let mut guard = txn.lock();
        let state = &mut *guard;
        // On a dup-sort table, the row's stored value (its stored key in `stored`); `None` for a
        // value shorter than the subkey, refused below.
        let rest = rows.dup.and_then(|dup| dup.join(key, value, stored));
        if let (Some(number), Some(rest), Some(txn), DatabaseWriteOperation::CursorUpsert) =
            (*held, rest, state.txn.as_mut(), operation)
        {
            if let Some(Pending { table, dup, .. }) = state.deferred.take_rewrite(number, stored) {
                *held = None;
                let put = cursor.put(txn, stored, rest);
                if put.is_ok() {
                    state.deferred.count(FUSED);
                } else if !latches(&put) {
                    // A refused put changes nothing, the cursor included: the delete runs as it
                    // would have before the upsert (a failure of it is kept for the commit). A
                    // put that latched the environment left the engine cursor unset, and no
                    // commit can follow: the delete does not run, and the put's error is the
                    // call's.
                    state.deferred.count(BY_OWNER);
                    if let Ok(pos) = state.deferred.run(cursor, txn, &table, dup, &mut rows.end_key)
                    {
                        rows.pos = pos;
                    }
                }
                rows.wrote(&put);
                return put.map_err(fail);
            }
        }
        settle(state, held, cursor, rows)?;
        let txn = live(&mut state.txn, fail)?;
        let Some(dup) = rows.dup else {
            // Where an append that met the table's last key leaves the cursor ([`Pos::End`]).
            let mut ends = None;
            let result = match operation {
                DatabaseWriteOperation::CursorInsert => match cursor.insert(txn, key, value) {
                    // MDBX leaves a cursor whose insert it refused on the present key; the engine
                    // leaves it where it was. Damage the seek meets is the call's error.
                    Err(tn_lmdb::Error::KeyExists) => {
                        cursor.seek(txn, key).and(Err(tn_lmdb::Error::KeyExists))
                    }
                    inserted => inserted,
                },
                DatabaseWriteOperation::CursorAppend => match cursor.append(txn, key, value) {
                    // MDBX's append takes the table's last key again, replacing its value, and
                    // leaves the cursor ending the table as after `last` (`next` finds nothing,
                    // even once another writer adds a later key); it leaves a cursor whose append
                    // it refused on the last row, ending the table too. The engine's refuses both
                    // and leaves the cursor where it was.
                    Err(tn_lmdb::Error::AppendOutOfOrder) => {
                        let last = cursor.last(txn).map_err(fail)?;
                        let last_key = last.is_some_and(|(found, _)| found == key);
                        ends = Some(rows.ended(last.map(|(found, _)| found)));
                        if last_key {
                            cursor.put(txn, key, value).and_then(|()| cursor.last(txn).map(drop))
                        } else {
                            Err(tn_lmdb::Error::AppendOutOfOrder)
                        }
                    }
                    appended => appended,
                },
                _ => cursor.put(txn, key, value),
            };
            rows.wrote(&result);
            if let (Some(pos), Ok(()) | Err(tn_lmdb::Error::AppendOutOfOrder)) = (ends, &result) {
                rows.pos = pos;
            }
            return result.map_err(fail);
        };
        let rest = rest.ok_or_else(|| error::bad_size(operation, T::NAME, key))?;
        let stored = stored.as_slice();
        // `Ok(false)`: a second value for a subkey the key holds, refused ([`insert_dup_at`]).
        let result = match operation {
            DatabaseWriteOperation::CursorUpsert => insert_dup_at(cursor, txn, table, stored, rest),
            DatabaseWriteOperation::CursorInsert => {
                // MDBX refuses a key that has any row, and leaves the cursor on its first. The
                // key's rows are looked for apart from the cursor ([`has_rows`]), so a row the
                // engine refuses (by its size) leaves the cursor where it was.
                if has_rows(txn, table, dup, key).map_err(fail)? {
                    cursor.seek(txn, key).and(Err(tn_lmdb::Error::KeyExists))
                } else {
                    cursor.insert(txn, stored, rest).map(|()| true)
                }
            }
            DatabaseWriteOperation::CursorAppend => match cursor.append(txn, stored, rest) {
                // MDBX's append to a dup-sort table also takes the last key again, with a value
                // anywhere among its rows; a smaller key is refused with the cursor on the last
                // row, as after `last`. The last row is looked for apart from the cursor
                // ([`last_is_row_of`]), so a refused second value leaves the cursor where it was.
                Err(tn_lmdb::Error::AppendOutOfOrder) => {
                    if last_is_row_of(txn, table, dup, key).map_err(fail)? {
                        insert_dup_at(cursor, txn, table, stored, rest)
                    } else {
                        if let Some((stored, _)) = cursor.last(txn).map_err(fail)? {
                            rows.keep_end_key(stored);
                        }
                        rows.pos = Pos::DupEnd { soft: true };
                        return Err(fail(tn_lmdb::Error::AppendOutOfOrder));
                    }
                }
                appended => appended.map(|()| true),
            },
            // MDBX's `APPEND_DUP`: the value must sort after the key's last, and a key without
            // rows takes any value, wherever it sorts. A refused one leaves the cursor on the
            // key's last row, or on its first when the value is exactly the first's (MDBX finds
            // the key's first value before it appends).
            _ => match cursor.append(txn, stored, rest) {
                Err(tn_lmdb::Error::AppendOutOfOrder) => {
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
                            Err(tn_lmdb::Error::AppendOutOfOrder)
                        }
                        _ => insert_dup_at(cursor, txn, table, stored, rest),
                    }
                }
                appended => appended.map(|()| true),
            },
        };
        let result = match result {
            // Nothing changed, the cursor's state included.
            Ok(false) => return Err(error::second_dup_value(operation, T::NAME, key)),
            result => result.map(drop),
        };
        rows.wrote(&result);
        result.map_err(fail)
    }
}

/// [`crate::tx::insert_dup`] through `cursor`: a row new or held already (`Ok(true)`) leaves the
/// cursor on it, as MDBX's put of a pair it holds does; a second value for the subkey (`Ok(false)`)
/// leaves it where it was, since a refused insert does not move the engine's cursor.
fn insert_dup_at(
    cursor: &mut WriteCursor,
    txn: &mut RwTxn,
    table: &EngineTable,
    stored: &[u8],
    rest: &[u8],
) -> tn_lmdb::Result<bool> {
    match cursor.insert(txn, stored, rest) {
        Err(tn_lmdb::Error::KeyExists) => {
            if txn.get(table, stored)? != Some(rest) {
                return Ok(false);
            }
            cursor.seek_exact(txn, stored).map(|_| true)
        }
        inserted => inserted.map(|()| true),
    }
}

/// Whether `key` (the table's key width) has a row in the dup-sort table `table`, looked for by
/// an engine cursor of its own, so the caller's is not moved: on an engine dup table a detached
/// dup cursor's `seek_key`, else a plain cursor's `seek` and [`DupLayout::is_row_of`].
pub(crate) fn has_rows(
    txn: &RwTxn,
    table: &EngineTable,
    dup: DupLayout,
    key: &[u8],
) -> tn_lmdb::Result<bool> {
    if dup.nested {
        return Ok(txn.dup_cursor_detached(table)?.seek_key(txn, key)?.is_some());
    }
    Ok(txn.cursor(table)?.seek(key)?.is_some_and(|(found, _)| dup.is_row_of(found, key)))
}

/// Whether the last row of the dup-sort table `table` is a row of `key`, looked for as
/// [`has_rows`] looks, with `last`.
pub(crate) fn last_is_row_of(
    txn: &RwTxn,
    table: &EngineTable,
    dup: DupLayout,
    key: &[u8],
) -> tn_lmdb::Result<bool> {
    let row_of_key =
        |last: Option<Entry<'_>>| last.is_some_and(|(found, _)| dup.is_row_of(found, key));
    if dup.nested {
        return Ok(row_of_key(txn.dup_cursor_detached(table)?.last(txn)?));
    }
    Ok(row_of_key(txn.cursor(table)?.last()?))
}

impl<T: Table> DbCursorRW<T> for TnLmdbCursorMut<T> {
    /// On a dup-sort table this stores the row of `value`'s subkey. The value that row holds
    /// already changes nothing and moves the cursor to the row, as MDBX's re-put of a pair does;
    /// another value for that subkey (MDBX would store a second) is refused with `MDBX_KEYEXIST`
    /// and this crate's message, changing nothing, the cursor included.
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
    /// table with a value anywhere among its rows, stored as by `upsert` (another value for a
    /// subkey the key holds is refused).
    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.write(key, value, DatabaseWriteOperation::CursorAppend)
    }

    /// Right after a delete this deletes the follower, as MDBX's does. With the cursor not at a
    /// row (fresh, unset, past either end) this is MDBX's `MDBX_ENODATA`, and on a dup-sort table
    /// it is right after a delete of a key's last row that left the key earlier rows (the cursor
    /// then ends the key's rows, on the row before, as MDBX's does).
    ///
    /// On a dup-sort table the delete of a row the cursor is on is held (the crate docs' dup
    /// rewrite): the cursor's next call runs it first, or, if it is an `upsert` of the same row,
    /// makes the two one engine `put`; any other accessor runs it before its own call. A failure
    /// of the delete is so the error of the call that runs it, not this one's, and the commit
    /// refuses it.
    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        let Self { txn, table, cursor, rows, held, .. } = self;
        let mut guard = txn.lock();
        let state = &mut *guard;
        settle(state, held, cursor, rows)?;
        let dup = match (rows.pos, rows.dup) {
            (Pos::Missed | Pos::DupEnd { soft: false }, _) => return Err(no_data()),
            (_, dup) => dup,
        };
        let txn = live(&mut state.txn, error::delete)?;
        // At a dup key's end after `last`, the row is found again by its kept key first, which
        // ends the engine cursor's end; a row another accessor deleted since is `MDBX_ENODATA`
        // (never the follower, which the engine cursor is on then).
        if rows.pos == (Pos::DupEnd { soft: true }) {
            if cursor.seek_exact(txn, &rows.end_key).map_err(error::delete)?.is_none() {
                return Err(no_data());
            }
        } else if let Some(dup) = dup {
            // The row the cursor is on, held; none is `MDBX_ENODATA` now, as the engine's delete
            // would find none.
            let Some((stored, _)) = cursor.current(txn).map_err(error::delete)? else {
                return Err(no_data());
            };
            *held = Some(state.deferred.hold(table, dup, stored).map_err(error::delete)?);
            return Ok(());
        }
        if !cursor.delete_current(txn).map_err(error::delete)? {
            return Err(no_data());
        }
        let Some(dup) = dup else { return Ok(()) };
        rows.pos = after_delete(cursor, txn, dup, &mut rows.end_key).map_err(error::delete)?;
        Ok(())
    }
}

impl<T: DupSort> DbDupCursorRO<T> for TnLmdbCursorMut<T> {
    dup_reads!();
}

impl<T: DupSort> DbDupCursorRW<T> for TnLmdbCursorMut<T> {
    /// Deletes every row of the current row's key (right after a delete, of the follower's key,
    /// as MDBX's does), and leaves the cursor on the next key's first row as after a delete
    /// (`next` and `current` return it, `prev_dup` finds nothing). With the cursor not at a row
    /// (fresh, unset, past either end) this is MDBX's `MDBX_ENODATA`. On an engine dup table, a
    /// wipe that removed no row or left one of its key (damage, or a bug) is refused with the
    /// engine's `Failed` (`MDBX_PANIC`), and the commit refuses the transaction.
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        let dup = self.rows.layout::<T>()?;
        let mut guard = self.txn.lock();
        let state = &mut *guard;
        settle(state, &mut self.held, &mut self.cursor, &mut self.rows)?;
        if matches!(self.rows.pos, Pos::Fresh | Pos::Missed) {
            return Err(no_data());
        }
        self.rows.pos = Pos::Engine;
        let txn = live(&mut state.txn, error::delete)?;
        let key = match self.cursor.current(txn).map_err(error::delete)? {
            Some((stored, _)) => dup.split(stored).ok_or(DatabaseError::Decode)?.0.to_vec(),
            None => return Err(no_data()),
        };
        if dup.nested {
            // The key's tree in one walk. The cursor is stale after it, and finds its place again
            // by its row's key: the next key's first row in the deleted state, where the loop
            // below leaves it. It does so now (any call that keeps the deleted state), not at its
            // next call, which another write may come before: by then a row put again in the key
            // at or after the cursor's subkey would be found instead, where the loop's cursor
            // stays on the next key's.
            let deleted = txn.delete_prefix(&self.table, &key).map_err(error::delete)?;
            let shares = self.cursor.follower_shares_prefix(txn, dup.key).map_err(error::delete)?;
            #[cfg(test)]
            let (deleted, shares) = match state.deferred.wipe_fault.take() {
                Some(WipeFault::NoRowRemoved) => (0, shares),
                Some(WipeFault::RowLeft) => (deleted, true),
                None => (deleted, shares),
            };
            // The walk took the cursor's row and left no row of its key, else the tree and the
            // cursor disagree (damage the engine did not detect, or a bug): a wipe reported done
            // would keep rows reth deleted, or leave the cursor blind to them. Refused, and the
            // commit refuses the transaction ([`Deferred::fail`]).
            if deleted == 0 || shares {
                let what = if deleted == 0 {
                    "a wipe of the cursor's key removed no row"
                } else {
                    "a wipe of the cursor's key left a row of it"
                };
                let broken = invariant_broken(&self.table, what);
                return Err(error::delete(state.deferred.fail(broken)));
            }
        } else {
            self.cursor.seek(txn, &key).map_err(error::delete)?;
            while self.cursor.delete_current(txn).map_err(error::delete)? {
                if self.cursor.next_in_prefix(txn, dup.key).map_err(error::delete)?.is_none() {
                    break;
                }
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

#[cfg(test)]
mod tests {
    use super::{error, Moves, Table, TnLmdbCursor, TnLmdbCursorMut};
    use crate::{tests::open_env, TnLmdbArgs, TnLmdbEnvKind};
    use alloy::primitives::{Address, B256, U256};
    use reth_db_api::{
        cursor::{DbCursorRO, DbDupCursorRO},
        database::Database,
        tables::PlainStorageState,
        transaction::{DbTx, DbTxMut},
    };
    use reth_primitives_traits::StorageEntry;

    /// The stored key of the row the engine cursor under `c` is at, if any.
    fn engine_row<T: Table>(c: &mut TnLmdbCursor<T>) -> Option<Vec<u8>> {
        c.with(|_, m| Ok(m.current().map_err(error::read)?.map(|(k, _)| k.to_vec()))).unwrap()
    }

    /// [`engine_row`] for a write cursor.
    fn engine_row_mut<T: Table>(c: &mut TnLmdbCursorMut<T>) -> Option<Vec<u8>> {
        c.with(|_, m| Ok(m.current().map_err(error::read)?.map(|(k, _)| k.to_vec()))).unwrap()
    }

    /// A missed `seek_exact` of a key between two present ones, and a `seek_by_key_subkey` past
    /// a key's last subkey, find nothing on both forms, as on MDBX; on an engine dup table the
    /// seek is bounded by the key, so it leaves the engine cursor unset, where on composite keys it
    /// leaves it on the next key's first row. This is the one sign of the bounded seek: every
    /// reth call after the miss answers the same either way (`Pos::Missed`).
    #[test]
    fn bounded_seeks_never_leave_their_key() {
        let a = Address::with_last_byte;
        let s = B256::with_last_byte;
        let e = |n: u8| StorageEntry { key: s(n), value: U256::from(n) };
        for nested in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let args = TnLmdbArgs::default();
            let args = if nested { args } else { args.composite_dup_tables() };
            let mut db = open_env(dir.path(), TnLmdbEnvKind::RW, args).unwrap();
            db.create_tables().unwrap();
            let tx = db.tx_mut().unwrap();
            for (k, sk) in [(1, 1), (1, 3), (3, 2)] {
                tx.put::<PlainStorageState>(a(k), e(sk)).unwrap();
            }
            tx.commit().unwrap();
            let next_key_row = [a(3).as_slice(), s(2).as_slice()].concat();
            let left = (!nested).then_some(next_key_row);
            let form = if nested { "nested" } else { "composite" };

            let tx = db.tx().unwrap();
            let mut c = tx.cursor_dup_read::<PlainStorageState>().unwrap();
            assert_eq!(c.seek_exact(a(2)).unwrap(), None);
            assert_eq!(engine_row(&mut c), left, "{form}: seek_exact");
            assert_eq!(c.seek_by_key_subkey(a(1), s(4)).unwrap(), None);
            assert_eq!(engine_row(&mut c), left, "{form}: seek_by_key_subkey");
            assert_eq!(c.seek_by_key_subkey(a(1), s(3)).unwrap(), Some(e(3)), "{form}");
            assert_eq!(engine_row(&mut c), Some([a(1).as_slice(), s(3).as_slice()].concat()));

            let tx = db.tx_mut().unwrap();
            let mut c = tx.cursor_dup_write::<PlainStorageState>().unwrap();
            assert_eq!(c.seek_exact(a(2)).unwrap(), None);
            assert_eq!(engine_row_mut(&mut c), left, "{form}: seek_exact, write cursor");
            assert_eq!(c.seek_by_key_subkey(a(1), s(4)).unwrap(), None);
            assert_eq!(engine_row_mut(&mut c), left, "{form}: seek_by_key_subkey, write cursor");
        }
    }
}
