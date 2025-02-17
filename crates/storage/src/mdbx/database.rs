//! Impl db traits for mdbx.

use std::{
    marker::PhantomData,
    path::Path,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    time::Duration,
};

use reth_libmdbx::{
    ffi::MDBX_dbi, Cursor, DatabaseFlags, Environment, Geometry, PageSize, Transaction, WriteFlags,
    RO, RW,
};
use tn_types::{
    decode, decode_key, encode, encode_key, DBIter, Database, DbTx, DbTxMut, KeyT, Table, ValueT,
};

use crate::mdbx::metrics::MdbxMetrics;

/// Wrapper for the libmdbx transaction.
#[derive(Debug)]
pub struct MdbxTx {
    /// Libmdbx-sys transaction.
    inner: Transaction<RO>,
}

impl MdbxTx {
    /// Gets a table database handle if it exists, otherwise creates it.
    fn get_dbi<T: Table>(&self) -> eyre::Result<MDBX_dbi> {
        Ok(self.inner.open_db(Some(T::NAME)).map(|db| db.dbi())?)
    }

    fn cursor<T: Table>(&self) -> eyre::Result<Cursor<RO>> {
        Ok(self.inner.cursor_with_dbi(self.get_dbi::<T>()?)?)
    }
}

impl DbTx for MdbxTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let key_buf = encode_key(key);
        let v = self
            .inner
            .get::<Vec<u8>>(self.get_dbi::<T>()?, &key_buf[..])
            .map(|res| res.map(|bytes| decode::<T::Value>(&bytes)))?;
        Ok(v)
    }
}

/// Wrapper for the libmdbx transaction.
#[derive(Debug)]
pub struct MdbxTxMut {
    /// Libmdbx-sys transaction.
    inner: Transaction<RW>,
}

impl MdbxTxMut {
    /// Gets a table database handle if it exists, otherwise creates it.
    fn get_dbi<T: Table>(&self) -> eyre::Result<MDBX_dbi> {
        Ok(self.inner.open_db(Some(T::NAME)).map(|db| db.dbi())?)
    }
}

impl DbTx for MdbxTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let key_buf = encode_key(key);
        let v = self
            .inner
            .get::<Vec<u8>>(self.get_dbi::<T>()?, &key_buf[..])
            .map(|res| res.map(|bytes| decode::<T::Value>(&bytes)))?;
        Ok(v)
    }
}

impl DbTxMut for MdbxTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let key_buf = encode_key(key);
        let value_buf = encode(value);
        self.inner.put(self.get_dbi::<T>()?, key_buf, value_buf, WriteFlags::UPSERT)?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let key_buf = encode_key(key);
        self.inner.del(self.get_dbi::<T>()?, key_buf, None)?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        Ok(self.inner.clear_db(self.get_dbi::<T>()?)?)
    }

    fn commit(self) -> eyre::Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}

/// Wrapper for the libmdbx environment: [Environment]
#[derive(Debug, Clone)]
pub struct MdbxDatabase {
    /// Libmdbx-sys environment.
    inner: Environment,
    shutdown_tx: Arc<SyncSender<()>>,
}

impl Drop for MdbxDatabase {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shutdown_tx) <= 1 {
            tracing::info!(target: "telcoin::mdbx", "MDBX Dropping, shutting down metrics thread");
            // shutdown_tx is a sync sender with no buffer so this should block until the thread
            // reads it and shuts down.
            if let Err(e) = self.shutdown_tx.send(()) {
                tracing::error!(target: "telcoin::mdbx", "Error while trying to send shutdown to MDBX metrics thread {e}");
            }
        }
    }
}

const GIGABYTE: usize = 1024 * 1024 * 1024;
const TERABYTE: usize = GIGABYTE * 1024;

/// Returns the default page size that can be used in this OS.
fn default_page_size() -> usize {
    let os_page_size = page_size::get();

    // source: https://gitflic.ru/project/erthink/libmdbx/blob?file=mdbx.h#line-num-821
    let libmdbx_max_page_size = 0x10000;

    // May lead to errors if it's reduced further because of the potential size of the
    // data.
    let min_page_size = 4096;

    os_page_size.clamp(min_page_size, libmdbx_max_page_size)
}

impl MdbxDatabase {
    /// Creates a new database at the specified path if it doesn't exist. Does NOT create tables.
    /// Check [`init_db`].
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        let env = Environment::builder()
            .set_max_dbs(32)
            .write_map()
            .set_geometry(Geometry {
                // Maximum database size of 4 terabytes
                size: Some(0..(4 * TERABYTE)),
                // We grow the database in increments of 4 gigabytes
                growth_step: Some(4 * GIGABYTE as isize),
                // The database never shrinks
                shrink_threshold: Some(0),
                page_size: Some(PageSize::Set(default_page_size())),
            })
            .open(path.as_ref())?;

        let (shutdown_tx, rx) = mpsc::sync_channel::<()>(0);

        let db_cloned = env.clone();
        // Spawn thread to update metrics from MDBX stats every 2 seconds.
        std::thread::spawn(move || {
            tracing::info!(target: "telcoin::mdbx", "Starting MDBX metrics thread");
            let metrics = MdbxMetrics::default();
            while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(2))
            {
                match db_cloned.stat() {
                    Ok(status) => {
                        tracing::trace!(target: "telcoin::mdbx", "MDBX metrics thread {status:?}");
                        metrics.page_size.set(status.page_size().into());
                        metrics.depth.set(status.depth().into());
                        metrics.branch_pages.set(status.branch_pages().try_into().unwrap_or(-1));
                        metrics.leaf_pages.set(status.leaf_pages().try_into().unwrap_or(-1));
                        metrics
                            .overflow_pages
                            .set(status.overflow_pages().try_into().unwrap_or(-1));
                        metrics.entries.set(status.entries().try_into().unwrap_or(-1));
                    }
                    Err(e) => {
                        tracing::error!(target: "telcoin::mdbx", "Error while trying to get MDBX status: {e}");
                    }
                }
            }
            tracing::info!(target: "telcoin::mdbx", "Ending MDBX metrics thread");
        });

        Ok(MdbxDatabase { inner: env, shutdown_tx: Arc::new(shutdown_tx) })
    }

    pub fn open_table<T: Table>(&self) -> eyre::Result<()> {
        let txn = self.inner.begin_rw_txn()?;
        txn.create_db(Some(T::NAME), DatabaseFlags::default())?;
        txn.commit()?;
        Ok(())
    }
}

impl Database for MdbxDatabase {
    type TX<'txn>
        = MdbxTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = MdbxTxMut
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(MdbxTx { inner: self.inner.begin_ro_txn()? })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MdbxTxMut { inner: self.inner.begin_rw_txn()? })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.read_txn()?.get::<T>(key)?.is_some())
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.read_txn()?.get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let mut txn = self.write_txn()?;
        txn.insert::<T>(key, value)?;
        txn.commit()?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        let mut txn = self.write_txn()?;
        txn.remove::<T>(key)?;
        txn.commit()?;
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        let mut txn = self.write_txn()?;
        txn.clear_table::<T>()?;
        txn.commit()?;
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.iter::<T>().next().is_none()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let cursor = self
            .read_txn()
            .expect("Failed to get cursor!")
            .cursor::<T>()
            .expect("Failed to get cursor!");
        Box::new(MdbxIter { cursor, _key: PhantomData, _val: PhantomData })
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let cursor = self
            .read_txn()
            .expect("Failed to get cursor!")
            .cursor::<T>()
            .expect("Failed to get cursor!");
        let i = MdbxIter { cursor, _key: PhantomData, _val: PhantomData };
        let key = key.clone();
        Ok(Box::new(i.skip_while(move |(k, _)| k < &key)))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let cursor = self
            .read_txn()
            .expect("Failed to get cursor!")
            .cursor::<T>()
            .expect("Failed to get cursor!");
        Box::new(MdbxRevIter { cursor, started: false, _key: PhantomData, _val: PhantomData })
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let mut last = None;
        for (k, v) in self.iter::<T>() {
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.read_txn()
            .ok()?
            .cursor::<T>()
            .ok()?
            .last::<Vec<u8>, Vec<u8>>()
            .ok()?
            .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
    }
}

pub struct MdbxIter<K, V>
where
    K: KeyT,
    V: ValueT,
{
    cursor: Cursor<RO>,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K, V> Iterator for MdbxIter<K, V>
where
    K: KeyT,
    V: ValueT,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(result) = self.cursor.next::<Vec<u8>, Vec<u8>>() {
            result.map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)))
        } else {
            None
        }
    }
}

pub struct MdbxRevIter<K, V>
where
    K: KeyT,
    V: ValueT,
{
    cursor: Cursor<RO>,
    started: bool,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K, V> Iterator for MdbxRevIter<K, V>
where
    K: KeyT,
    V: ValueT,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            return self
                .cursor
                .last::<Vec<u8>, Vec<u8>>()
                .ok()?
                .map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)));
        }
        if let Ok(result) = self.cursor.prev::<Vec<u8>, Vec<u8>>() {
            result.map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::MdbxDatabase;
    use crate::test::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn open_db(path: &Path) -> MdbxDatabase {
        let db = MdbxDatabase::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    #[test]
    fn test_mdbx_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_contains_key(db)
    }

    #[test]
    fn test_mdbx_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_get(db)
    }

    #[test]
    fn test_mdbx_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_get(db)
    }

    #[test]
    fn test_mdbx_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_skip(db)
    }

    #[test]
    fn test_mdbx_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_skip_to_previous_simple(db)
    }

    #[test]
    fn test_mdbx_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter_skip_to_previous_gap(db)
    }

    #[test]
    fn test_mdbx_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_remove(db)
    }

    #[test]
    fn test_mdbx_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter(db)
    }

    #[test]
    fn test_mdbx_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter_reverse(db)
    }

    #[test]
    fn test_mdbx_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_clear(db)
    }

    #[test]
    fn test_mdbx_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_is_empty(db)
    }

    #[test]
    fn test_mdbx_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_insert(db)
    }

    #[test]
    fn test_mdbx_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_remove(db)
    }

    #[test]
    fn test_mdbx_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db_simp_bench(db, "MDBX");
    }
}
