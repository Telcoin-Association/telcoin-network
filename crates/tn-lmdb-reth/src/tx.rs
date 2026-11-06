//! reth's transactions: [`TnLmdbTx`] (read-only) and [`TnLmdbTxMut`] (read-write), whose engine
//! transaction its cursors share ([`TxnState`]) with a dup cursor's held delete
//! ([`Deferred`]), which every call runs before its own work.

use crate::{
    cursor::{
        last_is_row_of, Deferred, ReadCursor, ReadPool, TnLmdbCursor, TnLmdbCursorMut, WriteCursor,
    },
    dup::DupLayout,
    env::{resolve, ReaderWatch, TableMap},
    error::{self, Mdbx},
};
use parking_lot::Mutex;
use reth_db_api::{
    table::{Compress, Decompress, DupSort, Encode, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_storage_errors::db::DatabaseWriteOperation;
use std::sync::Arc;
use tn_lmdb::{KeyTail, ReadTxn, RoTxn, RwTxn, Table as EngineTable};

/// A write transaction shared by its handle and its cursors ([`TxnState`]).
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// What a write transaction's handle and cursors share, behind one mutex.
#[derive(Debug)]
pub(crate) struct TxnState {
    /// The transaction; `None` once it committed or aborted: a cursor that outlives it then gets
    /// `MDBX_BAD_TXN`.
    pub(crate) txn: Option<RwTxn>,
    /// A dup-sort cursor's `delete_current` not run yet ([`Deferred`]): every accessor but that
    /// cursor runs it before its own call ([`Deferred::flush`]), and the commit before the
    /// engine's, refusing a transaction one of whose held deletes failed.
    pub(crate) deferred: Deferred,
}

impl TxnState {
    /// Abort: a held delete is dropped with the transaction.
    fn abort(&mut self) {
        self.deferred.discard();
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
    }
}

/// `value` as stored: its bytes when it has an uncompressed form, else compressed into `buf`.
pub(crate) fn compressed<'b, V: Compress>(value: &'b V, buf: &'b mut Vec<u8>) -> &'b [u8] {
    if let Some(bytes) = value.uncompressable_ref() {
        return bytes;
    }
    buf.clear();
    value.compress_to_buf(buf);
    buf
}

/// The value of table `T` stored as `bytes`, if any.
fn decompress<T: Table>(bytes: Option<&[u8]>) -> Result<Option<T::Value>, DatabaseError> {
    bytes.map(T::Value::decompress).transpose()
}

/// The value of `key` in table `T` (on a dup-sort table, the key's first, as MDBX's `get`
/// returns), read through `txn`.
fn get_value<T: Table>(
    txn: &impl DupLookup,
    (table, dup): (&EngineTable, Option<DupLayout>),
    key: &[u8],
) -> Result<Option<T::Value>, DatabaseError> {
    let Some(dup) = dup else {
        return decompress::<T>(txn.reads().get(table, key).map_err(error::read)?);
    };
    // A key of another width is no row's: a miss with no seek (a nested table's engine would
    // refuse it).
    if key.len() != dup.key {
        return Ok(None);
    }
    let mut value = Vec::new();
    if dup.nested {
        return txn
            .first_row_of(table, key, dup.subkey, |row| {
                let Some((stored, tail)) = row.filter(|&(stored, _)| dup.is_row_of(stored, key))
                else {
                    return Ok(None);
                };
                decompress::<T>(dup.joined(stored, tail, &mut value))
            })
            .map_err(error::read)?;
    }
    let mut cursor = txn.reads().cursor(table).map_err(error::read)?;
    if !cursor.seek(key).map_err(error::read)?.is_some_and(|(stored, _)| dup.is_row_of(stored, key))
    {
        return Ok(None);
    }
    let Some((stored, tail)) = cursor.current_with_key_tail(dup.subkey).map_err(error::read)?
    else {
        return Ok(None);
    };
    decompress::<T>(dup.joined(stored, tail, &mut value))
}

/// A transaction [`get_value`] reads through: its [`ReadTxn`] calls, and the lookup of an outer
/// key's first row in an engine dup table, by a dup cursor of the transaction's kind (a read-only
/// transaction's owns a share of it, a write transaction's is detached).
trait DupLookup {
    /// The transaction's [`ReadTxn`].
    type Reads: ReadTxn;

    fn reads(&self) -> &Self::Reads;

    /// `f` of the first row of the outer key `key` (the table's key width) in the engine dup
    /// table `table`, with its value joined to its stored key's last `t` bytes; `None` if `key`
    /// has no row.
    fn first_row_of<R>(
        &self,
        table: &EngineTable,
        key: &[u8],
        t: usize,
        f: impl FnOnce(Option<(&[u8], KeyTail<'_>)>) -> R,
    ) -> tn_lmdb::Result<R>;
}

impl DupLookup for Arc<RoTxn> {
    type Reads = RoTxn;

    fn reads(&self) -> &RoTxn {
        self
    }

    fn first_row_of<R>(
        &self,
        table: &EngineTable,
        key: &[u8],
        t: usize,
        f: impl FnOnce(Option<(&[u8], KeyTail<'_>)>) -> R,
    ) -> tn_lmdb::Result<R> {
        let mut cursor = self.dup_cursor_owned(table)?;
        Ok(f(cursor.seek_dup_with_key_tail(key, &[], t)?))
    }
}

impl DupLookup for RwTxn {
    type Reads = Self;

    fn reads(&self) -> &Self {
        self
    }

    fn first_row_of<R>(
        &self,
        table: &EngineTable,
        key: &[u8],
        t: usize,
        f: impl FnOnce(Option<(&[u8], KeyTail<'_>)>) -> R,
    ) -> tn_lmdb::Result<R> {
        let mut cursor = self.dup_cursor_detached(table)?;
        Ok(f(cursor.seek_dup_with_key_tail(self, key, &[], t)?))
    }
}

/// A read-only transaction: one committed state, unaffected by later commits. Its cursors
/// ([`TnLmdbCursor`]) own a share of it, so they live on after it drops. A dropped cursor gives its
/// engine cursor back to the transaction's pool (`ReadPool`, up to four a table), and a new
/// cursor on the same table takes it, reset, in place of a new one.
#[derive(Debug)]
pub struct TnLmdbTx {
    txn: Arc<RoTxn>,
    tables: Arc<TableMap>,
    pool: Arc<ReadPool>,
}

impl TnLmdbTx {
    pub(crate) fn new(txn: RoTxn, tables: Arc<TableMap>) -> Self {
        Self { txn: Arc::new(txn), tables, pool: Arc::default() }
    }

    /// Test-only: the engine cursors the transaction's pool holds.
    #[cfg(test)]
    pub(crate) fn pooled_cursors(&self) -> usize {
        self.pool.len()
    }

    /// Test-only: a weak handle on the transaction's pool, to see when it is freed.
    #[cfg(test)]
    pub(crate) fn pool_weak(&self) -> std::sync::Weak<ReadPool> {
        Arc::downgrade(&self.pool)
    }
}

impl DbTx for TnLmdbTx {
    type Cursor<T: Table> = TnLmdbCursor<T>;
    type DupCursor<T: DupSort> = TnLmdbCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        self.get_by_encoded_key::<T>(&key.encode())
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let entry = resolve::<T>(&self.tables, &*self.txn)?;
        get_value::<T>(&self.txn, entry.check::<T>()?, key.as_ref())
    }

    /// Ends the transaction; a read-only one has nothing to commit.
    fn commit(self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn abort(self) {}

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        let entry = resolve::<T>(&self.tables, &*self.txn)?;
        let (table, dup) = entry.check::<T>()?;
        let id = table.id();
        let cursor = match self.pool.take(id, T::NAME) {
            Some(cursor) => cursor,
            None => Box::new(
                ReadCursor::open(&self.txn, table, dup)
                    .map_err(|e| DatabaseError::InitCursor(error::info(&e)))?,
            ),
        };
        Ok(TnLmdbCursor::new(cursor, dup, Arc::clone(&self.pool), id))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.cursor_read::<T>()
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let entry = resolve::<T>(&self.tables, &*self.txn)?;
        let stat =
            self.txn.table_stat(&entry.table).map_err(|e| DatabaseError::Stats(error::info(&e)))?;
        Ok(stat.entries as usize)
    }

    /// Does nothing: tn-lmdb has no read timeout to disable. A long read transaction only holds
    /// back page reuse, so the file grows until it ends; a commit made while it has held a
    /// superseded snapshot for over a minute logs a warning instead (at most once a minute).
    fn disable_long_read_transaction_safety(&mut self) {}
}

/// A read-write transaction. It and its cursors ([`TnLmdbCursorMut`]) share the engine's
/// transaction behind a mutex, which each call holds while it runs (entries are decoded before
/// it is released), after running a cursor's held `delete_current` (`Deferred`). Its reads see
/// its own uncommitted writes. Dropping it without [`DbTx::commit`] aborts it, and its cursors
/// then fail with `MDBX_BAD_TXN`.
#[derive(Debug)]
pub struct TnLmdbTxMut {
    txn: SharedTxn,
    tables: Arc<TableMap>,
    /// The environment's long-reader warning, run after a successful commit.
    watch: Arc<ReaderWatch>,
}

impl TnLmdbTxMut {
    pub(crate) fn new(txn: RwTxn, tables: Arc<TableMap>, watch: Arc<ReaderWatch>) -> Self {
        let state = TxnState { txn: Some(txn), deferred: Deferred::default() };
        Self { txn: Arc::new(Mutex::new(state)), tables, watch }
    }

    /// Run `f` on the transaction, once a cursor's held delete has run ([`Deferred::flush`]; its
    /// failure is the call's, as a delete error); `on_finished` makes the error for one already
    /// finished (a handle cannot see that, since commit and abort consume it, but its cursors
    /// can).
    fn with<R>(
        &self,
        on_finished: impl FnOnce(tn_lmdb::Error) -> DatabaseError,
        f: impl FnOnce(&mut RwTxn) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut guard = self.txn.lock();
        let TxnState { txn, deferred } = &mut *guard;
        let txn = txn.as_mut().ok_or_else(|| on_finished(error::finished()))?;
        deferred.flush(txn).map_err(error::delete)?;
        f(txn)
    }

    /// Test-only: the dup rewrites the transaction's cursors made as one engine put, and the
    /// held deletes ([`Deferred`]) that ran as deletes, by their cursors and by other accessors.
    #[cfg(test)]
    pub(crate) fn deferred_counts(&self) -> [u64; 3] {
        self.txn.lock().deferred.counts()
    }

    /// Test-only: make the transaction's next held delete to run fail ([`Deferred`]), with an
    /// error that does not latch the environment.
    #[cfg(test)]
    pub(crate) fn fail_next_held_delete(&self) {
        self.txn.lock().deferred.fail_next();
    }

    /// Test-only: make the next call that would run a cursor's held delete ([`Deferred`]) before
    /// its own work leave it held, as a call that broke that ordering would.
    #[cfg(test)]
    pub(crate) fn skip_next_held_delete_run(&self) {
        self.txn.lock().deferred.skip_next_flush();
    }

    /// `tx.put` or `tx.append` (`append`) of `key` and `value` into `T`. An append follows
    /// MDBX's rule: the table's last key is taken again, its value replaced (on a dup-sort table,
    /// with a value anywhere among its rows). On a dup-sort table both store the row of `value`'s
    /// subkey as [`insert_dup`] does: a second value for a subkey the key holds is refused.
    fn put_kind<T: Table>(
        &self,
        key: T::Key,
        value: &T::Value,
        operation: DatabaseWriteOperation,
    ) -> Result<(), DatabaseError> {
        let key = key.encode();
        let mut buf = Vec::new();
        let value = compressed(value, &mut buf);
        let key = key.as_ref();
        let fail = |e: tn_lmdb::Error| error::write(&e, operation, T::NAME, key);
        self.with(fail, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            let (table, dup) = entry.check::<T>()?;
            let append = operation == DatabaseWriteOperation::PutAppend;
            let Some(dup) = dup else {
                if !append {
                    return txn.put(table, key, value).map_err(fail);
                }
                return match txn.append(table, key, value) {
                    Err(tn_lmdb::Error::AppendOutOfOrder) => {
                        let last = txn.cursor(table).map_err(fail)?.last().map_err(fail)?;
                        if last.is_some_and(|(found, _)| found == key) {
                            txn.put(table, key, value)
                        } else {
                            Err(tn_lmdb::Error::AppendOutOfOrder)
                        }
                    }
                    appended => appended,
                }
                .map_err(fail);
            };
            let mut stored = Vec::new();
            let rest = dup
                .join(key, value, &mut stored)
                .ok_or_else(|| error::bad_size(operation, T::NAME, key))?;
            let stored_row = if append {
                match txn.append(table, &stored, rest) {
                    Err(tn_lmdb::Error::AppendOutOfOrder) => {
                        if last_is_row_of(txn, table, dup, key).map_err(fail)? {
                            insert_dup(txn, table, &stored, rest)
                        } else {
                            Err(tn_lmdb::Error::AppendOutOfOrder)
                        }
                    }
                    appended => appended.map(|()| true),
                }
            } else {
                insert_dup(txn, table, &stored, rest)
            };
            if stored_row.map_err(fail)? {
                Ok(())
            } else {
                Err(error::second_dup_value(operation, T::NAME, key))
            }
        })
    }
}

/// Insert the dup-sort row `stored` (its stored key, `key ‖ subkey`) with stored value `rest` into
/// `table`, as MDBX puts a (key, subkey) pair: `Ok(true)` when the row is new and stored, and when
/// it holds `rest` already (MDBX's put of a pair it holds, which changes nothing). `Ok(false)`
/// when it holds another value: a second value for the subkey, which this crate refuses, having
/// changed nothing (the crate docs' differences from MDBX). The present row is found by the
/// insert's own search, so a new row costs what a put does.
pub(crate) fn insert_dup(
    txn: &mut RwTxn,
    table: &EngineTable,
    stored: &[u8],
    rest: &[u8],
) -> tn_lmdb::Result<bool> {
    match txn.insert(table, stored, rest) {
        Err(tn_lmdb::Error::KeyExists) => Ok(txn.get(table, stored)? == Some(rest)),
        inserted => inserted.map(|()| true),
    }
}

impl Drop for TnLmdbTxMut {
    fn drop(&mut self) {
        self.txn.lock().abort();
    }
}

impl DbTx for TnLmdbTxMut {
    type Cursor<T: Table> = TnLmdbCursorMut<T>;
    type DupCursor<T: DupSort> = TnLmdbCursorMut<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        self.get_by_encoded_key::<T>(&key.encode())
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        self.with(error::read, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            get_value::<T>(txn, entry.check::<T>()?, key.as_ref())
        })
    }

    /// A cursor's held delete runs first; its failure, or an earlier held delete's (one its
    /// cursor's `delete_current` had reported done), fails the commit and aborts the transaction.
    fn commit(self) -> Result<(), DatabaseError> {
        let commit_error = |e: tn_lmdb::Error| DatabaseError::Commit(error::info(&e));
        let txn = {
            let mut guard = self.txn.lock();
            let TxnState { txn, deferred } = &mut *guard;
            let mut txn = txn.take().ok_or_else(|| commit_error(error::finished()))?;
            let refused = match deferred.flush(&mut txn) {
                Ok(()) => deferred.failure().map(DatabaseError::Commit),
                Err(e) => Some(commit_error(e)),
            };
            if let Some(refused) = refused {
                deferred.discard();
                txn.abort();
                return Err(refused);
            }
            txn
        };
        txn.commit().map_err(commit_error)?;
        self.watch.after_commit();
        Ok(())
    }

    fn abort(self) {
        self.txn.lock().abort();
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.cursor_write::<T>()
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.cursor_write::<T>()
    }

    /// Counts the transaction's own uncommitted writes too.
    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let stats_error = |e: tn_lmdb::Error| DatabaseError::Stats(error::info(&e));
        self.with(stats_error, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            Ok(txn.table_stat(&entry.table).map_err(stats_error)?.entries as usize)
        })
    }

    /// Does nothing: a write transaction has no timeout either.
    fn disable_long_read_transaction_safety(&mut self) {}
}

impl DbTxMut for TnLmdbTxMut {
    type CursorMut<T: Table> = TnLmdbCursorMut<T>;
    type DupCursorMut<T: DupSort> = TnLmdbCursorMut<T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.put_kind::<T>(key, &value, DatabaseWriteOperation::PutUpsert)
    }

    /// The engine's append: a key that does not sort after the table's last is refused with
    /// MDBX's `MDBX_EKEYMISMATCH`, and nothing changes.
    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.put_kind::<T>(key, &value, DatabaseWriteOperation::PutAppend)
    }

    /// Deletes `key`'s entry and returns `true`, or `false` if there is none. With a value,
    /// only an entry holding exactly that value is deleted, as MDBX checks the value given to a
    /// delete on a table without duplicates. On a dup-sort table, `key` with a value deletes the
    /// row of that value (its subkey, holding exactly the rest), and without one every row of
    /// `key`.
    fn delete<T: Table>(
        &self,
        key: T::Key,
        value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let key = key.encode();
        let key = key.as_ref();
        let mut buf = Vec::new();
        let value = value.as_ref().map(|v| compressed(v, &mut buf));
        self.with(error::delete, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            let (table, dup) = entry.check::<T>()?;
            let Some(dup) = dup else {
                if let Some(value) = value {
                    if txn.get(table, key).map_err(error::delete)? != Some(value) {
                        return Ok(false);
                    }
                }
                return txn.delete(table, key).map_err(error::delete);
            };
            let Some(value) = value else { return delete_rows_of(txn, table, dup, key) };
            let mut stored = Vec::new();
            // A value shorter than the subkey is no row's.
            let Some(rest) = dup.join(key, value, &mut stored) else { return Ok(false) };
            if txn.get(table, &stored).map_err(error::delete)? != Some(rest) {
                return Ok(false);
            }
            txn.delete(table, &stored).map_err(error::delete)
        })
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        self.with(error::delete, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            txn.clear_table(&entry.table).map_err(error::delete)
        })
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        let init_error = |e: tn_lmdb::Error| DatabaseError::InitCursor(error::info(&e));
        let (table, cursor, dup) = self.with(init_error, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            let (table, dup) = entry.check::<T>()?;
            Ok((table.clone(), WriteCursor::open(txn, table, dup).map_err(init_error)?, dup))
        })?;
        Ok(TnLmdbCursorMut::new(Arc::clone(&self.txn), table, cursor, dup))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        self.cursor_write::<T>()
    }
}

impl TableImporter for TnLmdbTxMut {}

/// Deletes every row of `key` in a dup-sort table; `true` if there was one. On an engine dup
/// table the key's tree goes in one walk (a key of another width is no row's).
fn delete_rows_of(
    txn: &mut RwTxn,
    table: &EngineTable,
    dup: DupLayout,
    key: &[u8],
) -> Result<bool, DatabaseError> {
    if dup.nested {
        if key.len() != dup.key {
            return Ok(false);
        }
        return Ok(txn.delete_prefix(table, key).map_err(error::delete)? > 0);
    }
    let mut cursor = txn.cursor_mut(table).map_err(error::delete)?;
    let first = cursor.seek(key).map_err(error::delete)?;
    if !first.is_some_and(|(stored, _)| dup.is_row_of(stored, key)) {
        return Ok(false);
    }
    while cursor.delete_current().map_err(error::delete)? {
        if cursor.next_in_prefix(dup.key).map_err(error::delete)?.is_none() {
            break;
        }
    }
    Ok(true)
}

/// The error of a cursor delete with nothing under the cursor: `MDBX_ENODATA`.
pub(crate) fn no_data() -> DatabaseError {
    DatabaseError::Delete(Mdbx::NO_DATA.info())
}
