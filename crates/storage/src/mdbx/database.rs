//! Impl db traits for mdbx.

use std::{marker::PhantomData, path::Path};

use reth_libmdbx::{
    ffi::MDBX_dbi, Cursor, DatabaseFlags, Environment, Geometry, PageSize, Transaction, WriteFlags,
    RO, RW,
};
use tn_types::{
    decode, decode_key, encode, encode_key, DBIter, Database, DbTx, DbTxMut, KeyT, Table, ValueT,
};

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
}

pub const MEGABYTE: usize = 1024 * 1024;
pub const GIGABYTE: usize = MEGABYTE * 1024;
pub const TERABYTE: usize = GIGABYTE * 1024;

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

/// The MDBX sync mode compiled into `test` and `test-utils` builds. `SafeNoSync` removes the
/// hot-path `fsync` while still surviving a process kill+restart (see [`MdbxDatabase::open`]);
/// it must never be `UtterlyNoSync`, which risks whole-DB corruption on an OS/power crash. This
/// is the single source of truth `open` applies, so a test can pin it exactly. Production builds
/// enable neither cfg and this const does not exist for them (the env stays `Durable`).
#[cfg(any(test, feature = "test-utils"))]
const BUILD_SYNC_MODE: reth_libmdbx::SyncMode = reth_libmdbx::SyncMode::SafeNoSync;

impl MdbxDatabase {
    /// Creates a new database at the specified path if it doesn't exist. Does NOT create tables.
    /// Check [`init_db`].
    pub fn open<P: AsRef<Path>>(
        path: P,
        max_tables: usize,
        max_size: usize,
        growth_step: usize,
    ) -> eyre::Result<Self> {
        let mut builder = Environment::builder();
        builder.set_max_dbs(max_tables).write_map().set_geometry(Geometry {
            // Maximum database size
            size: Some(0..max_size),
            // We grow the database in increments of 1 gigabyte
            growth_step: Some(growth_step as isize),
            // The database never shrinks
            shrink_threshold: Some(0),
            page_size: Some(PageSize::Set(default_page_size())),
        });

        // Test and `test-utils` builds trade fsync durability for write speed: they open the
        // env in `SafeNoSync` instead of the default `Durable`, which removes the meta+data
        // `fsync` that MDBX performs at every `txn.commit()` on the consensus hot path.
        //
        // This is coverage-preserving. With `write_map()` + `SafeNoSync`, a committed
        // transaction survives a *process* crash/kill+restart -- the only recovery the tests
        // ever exercise (they restart a node by relaunching the process against the same
        // on-disk datadir) -- because the data lives in the file-backed mmap / OS page cache
        // and a relaunched process re-maps it. Durability is lost only on an *OS/power* crash,
        // which no test induces.
        //
        // It must be `SafeNoSync`, never `UtterlyNoSync`. `SafeNoSync` keeps the last steady
        // commit's pages untouched, so it is corruption-proof on *any* crash (it can always roll
        // back to that steady commit) and merely loses the last transactions on an OS/power
        // crash. `UtterlyNoSync` discards that steady commit for marginally faster writes and can
        // corrupt the whole database on an OS/power crash -- a needless risk given our access
        // pattern. Note the e2e suite would NOT reliably catch a drift to `UtterlyNoSync`: an
        // OS-alive process restart recovers under either mode, so the `BUILD_SYNC_MODE` const and
        // its `assert_eq!` are what actually enforce the choice, not the restart tests.
        //
        // The `feature = "test-utils"` arm is what reaches the e2e nodes: they are spawned as
        // separate processes built with `--features tn-storage/test-utils`, where `cfg(test)`
        // is not live, so no CLI plumbing is required to select the mode. Production builds
        // enable neither cfg, so this block is compiled out and the default `Durable` stands.
        #[cfg(any(test, feature = "test-utils"))]
        {
            use reth_libmdbx::{EnvironmentFlags, Mode};
            builder.set_flags(EnvironmentFlags {
                mode: Mode::ReadWrite { sync_mode: BUILD_SYNC_MODE },
                ..Default::default()
            });
        }

        let env = builder.open(path.as_ref())?;

        Ok(MdbxDatabase { inner: env })
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

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        let txn = self.inner.begin_rw_txn()?;
        txn.create_db(Some(T::NAME), DatabaseFlags::default())?;
        txn.commit()?;
        Ok(())
    }

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

#[derive(Debug)]
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

#[derive(Debug)]
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
    use crate::{mdbx::database::MEGABYTE, test::*};
    use std::path::Path;
    use tempfile::tempdir;
    use tn_types::Database as _;

    fn open_db(path: &Path) -> MdbxDatabase {
        let db =
            MdbxDatabase::open(path, 4, 16 * MEGABYTE, 8 * MEGABYTE).expect("Cannot open database");
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

    /// #917: under the `test`/`test-utils` cfg the env must open in `SafeNoSync` (no hot-path
    /// `fsync`) -- specifically not `Durable` (would keep the fsync) and, critically, not
    /// `UtterlyNoSync` (risks whole-DB corruption on an OS/power crash). The e2e restart suite
    /// cannot tell the two no-sync modes apart, so this const-assert is the real guard.
    #[test]
    fn test_open_uses_safe_no_sync_under_test_cfg() {
        use super::BUILD_SYNC_MODE;
        use reth_libmdbx::{Mode, SyncMode};

        // Pin the exact compiled mode. `SyncMode` is `PartialEq`, so this catches a drift to
        // `Durable` (fsync back on) or to `UtterlyNoSync` (recovery broken) -- the latter is the
        // one the readback below cannot see, so this assert is what guards the hard constraint.
        assert_eq!(BUILD_SYNC_MODE, SyncMode::SafeNoSync);

        // And prove the flag actually reached MDBX: the opened env reports a non-`Durable` mode,
        // so the hot-path fsync is genuinely gone. NOTE: MDBX defines
        // `MDBX_UTTERLY_NOSYNC = MDBX_SAFE_NOSYNC | <extra bit>`, and reth-libmdbx's `mode()`
        // tests the UtterlyNoSync mask before the SafeNoSync one, so it reports `UtterlyNoSync`
        // for a genuine `SafeNoSync` env. We therefore assert only "not Durable / not read-only"
        // from the readback; the exact-mode guarantee is pinned by the const assert above. Do not
        // "fix" this into `assert_eq!(mode, ..SafeNoSync)` -- it cannot pass by construction.
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        let mode = db.inner.info().expect("read env info").mode();
        assert!(
            matches!(mode, Mode::ReadWrite { sync_mode } if sync_mode != SyncMode::Durable),
            "test-build MDBX env must not be Durable (hot-path fsync must be off), got {mode:?}"
        );
    }
}
