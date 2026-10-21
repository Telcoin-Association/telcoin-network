//! reth's transactions: [`TndbTx`] (read-only) and [`TndbTxMut`] (read-write).

use crate::{
    cursor::{TndbCursor, TndbCursorMut},
    dup::DupLayout,
    env::{resolve, TableMap},
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
use tndb_b::{ReadTxn, RoTxn, RwTxn, Table as EngineTable};

/// A write transaction shared by its handle and its cursors. `None` once it committed or
/// aborted: a cursor that outlives it then gets `MDBX_BAD_TXN`.
pub(crate) type SharedTxn = Arc<Mutex<Option<RwTxn>>>;

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
    txn: &impl ReadTxn,
    (table, dup): (&EngineTable, Option<DupLayout>),
    key: &[u8],
) -> Result<Option<T::Value>, DatabaseError> {
    let Some(dup) = dup else {
        return decompress::<T>(txn.get(table, key).map_err(error::read)?);
    };
    let mut cursor = txn.cursor(table).map_err(error::read)?;
    if !cursor.seek(key).map_err(error::read)?.is_some_and(|(stored, _)| dup.is_row_of(stored, key))
    {
        return Ok(None);
    }
    let Some((stored, tail)) = cursor.current_with_key_tail(dup.subkey).map_err(error::read)?
    else {
        return Ok(None);
    };
    let mut value = Vec::new();
    decompress::<T>(dup.joined(stored, tail, &mut value))
}

/// A read-only transaction: one committed state, unaffected by later commits. Its cursors
/// ([`TndbCursor`]) own a share of it, so they live on after it drops.
#[derive(Debug)]
pub struct TndbTx {
    txn: Arc<RoTxn>,
    tables: Arc<TableMap>,
}

impl TndbTx {
    pub(crate) fn new(txn: RoTxn, tables: Arc<TableMap>) -> Self {
        Self { txn: Arc::new(txn), tables }
    }
}

impl DbTx for TndbTx {
    type Cursor<T: Table> = TndbCursor<T>;
    type DupCursor<T: DupSort> = TndbCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        self.get_by_encoded_key::<T>(&key.encode())
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let entry = resolve::<T>(&self.tables, &*self.txn)?;
        get_value::<T>(&*self.txn, entry.check::<T>()?, key.as_ref())
    }

    /// Ends the transaction; a read-only one has nothing to commit.
    fn commit(self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn abort(self) {}

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        let entry = resolve::<T>(&self.tables, &*self.txn)?;
        let (table, dup) = entry.check::<T>()?;
        let cursor =
            self.txn.cursor_owned(table).map_err(|e| DatabaseError::InitCursor(error::info(&e)))?;
        Ok(TndbCursor::new(cursor, dup))
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

    /// Does nothing: tndb-b never times a read transaction out. A long one only holds back page
    /// reuse, so the file grows instead.
    fn disable_long_read_transaction_safety(&mut self) {}
}

/// A read-write transaction. It and its cursors ([`TndbCursorMut`]) share the engine's
/// transaction behind a mutex, which each call holds while it runs (entries are decoded before
/// it is released). Its reads see its own uncommitted writes. Dropping it without
/// [`DbTx::commit`] aborts it, and its cursors then fail with `MDBX_BAD_TXN`.
#[derive(Debug)]
pub struct TndbTxMut {
    txn: SharedTxn,
    tables: Arc<TableMap>,
}

impl TndbTxMut {
    pub(crate) fn new(txn: RwTxn, tables: Arc<TableMap>) -> Self {
        Self { txn: Arc::new(Mutex::new(Some(txn))), tables }
    }

    /// Run `f` on the transaction; `on_finished` makes the error for one already finished
    /// (a handle cannot see that, since commit and abort consume it, but its cursors can).
    fn with<R>(
        &self,
        on_finished: impl FnOnce(tndb_b::Error) -> DatabaseError,
        f: impl FnOnce(&mut RwTxn) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut guard = self.txn.lock();
        let txn = guard.as_mut().ok_or_else(|| on_finished(error::finished()))?;
        f(txn)
    }

    /// `tx.put` or `tx.append` (`append`) of `key` and `value` into `T`. An append follows
    /// MDBX's rule: the table's last key is taken again, its value replaced (on a dup-sort table,
    /// with a value anywhere among its rows). On a dup-sort table a put stores the row of
    /// `value`'s subkey (replacing one with the same subkey).
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
        let fail = |e: tndb_b::Error| error::write(&e, operation, T::NAME, key);
        self.with(fail, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            let (table, dup) = entry.check::<T>()?;
            let append = operation == DatabaseWriteOperation::PutAppend;
            let Some(dup) = dup else {
                if !append {
                    return txn.put(table, key, value).map_err(fail);
                }
                return match txn.append(table, key, value) {
                    Err(tndb_b::Error::AppendOutOfOrder) => {
                        let last = txn.cursor(table).map_err(fail)?.last().map_err(fail)?;
                        if last.is_some_and(|(found, _)| found == key) {
                            txn.put(table, key, value)
                        } else {
                            Err(tndb_b::Error::AppendOutOfOrder)
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
            if !append {
                return txn.put(table, &stored, rest).map_err(fail);
            }
            match txn.append(table, &stored, rest) {
                Err(tndb_b::Error::AppendOutOfOrder) => {
                    let last = txn.cursor(table).map_err(fail)?.last().map_err(fail)?;
                    if last.is_some_and(|(found, _)| dup.is_row_of(found, key)) {
                        txn.put(table, &stored, rest)
                    } else {
                        Err(tndb_b::Error::AppendOutOfOrder)
                    }
                }
                appended => appended,
            }
            .map_err(fail)
        })
    }
}

impl Drop for TndbTxMut {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.lock().take() {
            txn.abort();
        }
    }
}

impl DbTx for TndbTxMut {
    type Cursor<T: Table> = TndbCursorMut<T>;
    type DupCursor<T: DupSort> = TndbCursorMut<T>;

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

    fn commit(self) -> Result<(), DatabaseError> {
        let commit_error = |e: tndb_b::Error| DatabaseError::Commit(error::info(&e));
        let txn = self.txn.lock().take().ok_or_else(|| commit_error(error::finished()))?;
        txn.commit().map(drop).map_err(commit_error)
    }

    fn abort(self) {
        if let Some(txn) = self.txn.lock().take() {
            txn.abort();
        }
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.cursor_write::<T>()
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.cursor_write::<T>()
    }

    /// Counts the transaction's own uncommitted writes too.
    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let stats_error = |e: tndb_b::Error| DatabaseError::Stats(error::info(&e));
        self.with(stats_error, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            Ok(txn.table_stat(&entry.table).map_err(stats_error)?.entries as usize)
        })
    }

    /// Does nothing: a write transaction has no timeout either.
    fn disable_long_read_transaction_safety(&mut self) {}
}

impl DbTxMut for TndbTxMut {
    type CursorMut<T: Table> = TndbCursorMut<T>;
    type DupCursorMut<T: DupSort> = TndbCursorMut<T>;

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
        let init_error = |e: tndb_b::Error| DatabaseError::InitCursor(error::info(&e));
        let (table, cursor, dup) = self.with(init_error, |txn| {
            let entry = resolve::<T>(&self.tables, txn)?;
            let (table, dup) = entry.check::<T>()?;
            Ok((table.clone(), txn.cursor_detached(table).map_err(init_error)?, dup))
        })?;
        Ok(TndbCursorMut::new(Arc::clone(&self.txn), table, cursor, dup))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        self.cursor_write::<T>()
    }
}

impl TableImporter for TndbTxMut {}

/// Deletes every row of `key` in a dup-sort table; `true` if there was one.
fn delete_rows_of(
    txn: &mut RwTxn,
    table: &EngineTable,
    dup: DupLayout,
    key: &[u8],
) -> Result<bool, DatabaseError> {
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
