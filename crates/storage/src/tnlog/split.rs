//! [`SplitDatabase`]: one tnlog database (one shared log, write lock and commit thread) per
//! production environment, as `open_db` splits MDBX, routed by each table's [`TableHint`] with no
//! layer in between. The batch caches (`Cache`) then never share a write lock with consensus
//! (`Epoch`): a large batch append holds up only other cache writes.
//!
//! A write transaction is atomic within each environment it writes (its part in each commits as
//! one batch there); `persist` covers the table's environment, as with MDBX today (every
//! production barrier is on consensus tables).
//!
//! The cache environment compacts automatically only once its log is at least
//! [`CACHE_AUTO_MIN_BYTES`]: it churns at the batch write rate, and each compaction's flushes slow
//! the other logs' syncs while it runs.

use std::path::Path;

use tn_types::{DBIter, Database, DbTx, DbTxMut, Table, TableHint};

use super::database::{CompactionConfig, TnLogDatabase, TnLogOptions, TnLogTxMut};

/// The least log the cache environment compacts automatically at (a larger configured threshold
/// is kept). The batch caches churn at the batch write rate: at the default 64 MiB, a heavy batch
/// stream keeps their log compacting most of the time, and each compaction's flushes queue ahead
/// of the epoch log's syncs (see the tnlog README, "Cost to other logs"). Four times the default
/// makes those compactions four times rarer, for up to that much more cache log on disk.
pub const CACHE_AUTO_MIN_BYTES: u64 = 256 << 20;

/// The environments, in [`SplitDatabase::dbs`] order.
const EPOCH: usize = 0;
const KAD: usize = 1;
const CACHE: usize = 2;

/// The options for environment `env`: `options`, with the cache's automatic threshold raised to
/// at least [`CACHE_AUTO_MIN_BYTES`] (automatic compaction left off if it is off).
fn env_options(options: TnLogOptions, env: usize) -> TnLogOptions {
    if env != CACHE {
        return options;
    }
    let auto_min_bytes = options.compaction.auto_min_bytes.map(|min| min.max(CACHE_AUTO_MIN_BYTES));
    TnLogOptions {
        compaction: CompactionConfig { auto_min_bytes, ..options.compaction },
        ..options
    }
}

fn env(hint: TableHint) -> usize {
    match hint {
        TableHint::Epoch => EPOCH,
        TableHint::Kad => KAD,
        TableHint::Cache => CACHE,
    }
}

/// Three tnlog databases (epoch, kad, cache) behind one `Database`, each table in its hint's.
#[derive(Clone, Debug)]
pub struct SplitDatabase {
    dbs: [TnLogDatabase; 3],
}

impl SplitDatabase {
    /// Open (creating if absent) the three databases under `root` (`epoch/`, `kad/`, `cache/`),
    /// committing and compacting as `options` say (the cache compacting automatically at no less
    /// than [`CACHE_AUTO_MIN_BYTES`]).
    pub fn open_with<P: AsRef<Path>>(root: P, options: TnLogOptions) -> eyre::Result<Self> {
        let root = root.as_ref();
        Ok(Self {
            dbs: [
                TnLogDatabase::open_with(root.join("epoch"), env_options(options, EPOCH))?,
                TnLogDatabase::open_with(root.join("kad"), env_options(options, KAD))?,
                TnLogDatabase::open_with(root.join("cache"), env_options(options, CACHE))?,
            ],
        })
    }

    /// Block until every write so far, in every environment, is durable (see
    /// [`TnLogDatabase::wait_durable_blocking`]).
    #[cfg(any(test, feature = "tnlog"))]
    pub(crate) fn wait_durable_blocking(&self) -> eyre::Result<()> {
        for db in &self.dbs {
            db.wait_durable_blocking()?;
        }
        Ok(())
    }

    fn db<T: Table>(&self) -> &TnLogDatabase {
        &self.dbs[env(T::HINT)]
    }
}

/// A read transaction: each read goes to its table's environment.
#[derive(Debug)]
pub struct SplitTx {
    db: SplitDatabase,
}

impl DbTx for SplitTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.db.db::<T>().get::<T>(key)
    }
}

/// A write transaction: one buffered tnlog transaction per environment it writes, each committed
/// (or, dropped, written) as one batch in its environment.
#[derive(Debug)]
pub struct SplitTxMut {
    db: SplitDatabase,
    txns: [Option<TnLogTxMut>; 3],
}

impl SplitTxMut {
    fn txn<T: Table>(&mut self) -> eyre::Result<&mut TnLogTxMut> {
        let env = env(T::HINT);
        if self.txns[env].is_none() {
            self.txns[env] = Some(self.db.dbs[env].write_txn()?);
        }
        Ok(self.txns[env].as_mut().expect("just created"))
    }
}

impl DbTx for SplitTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        match &self.txns[env(T::HINT)] {
            Some(txn) => txn.get::<T>(key),
            None => self.db.db::<T>().get::<T>(key),
        }
    }
}

impl DbTxMut for SplitTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.txn::<T>()?.insert::<T>(key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.txn::<T>()?.remove::<T>(key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.txn::<T>()?.clear_table::<T>()
    }

    fn commit(mut self) -> eyre::Result<()> {
        let mut result = Ok(());
        for txn in self.txns.iter_mut().filter_map(Option::take) {
            let committed = txn.commit();
            if result.is_ok() {
                result = committed;
            }
        }
        result
    }
}

impl Database for SplitDatabase {
    type TX<'txn>
        = SplitTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = SplitTxMut
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.db::<T>().open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(SplitTx { db: self.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(SplitTxMut { db: self.clone(), txns: [None, None, None] })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.db::<T>().contains_key::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.db::<T>().get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.db::<T>().insert::<T>(key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.db::<T>().remove::<T>(key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.db::<T>().clear_table::<T>()
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.db::<T>().is_empty::<T>()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        self.db::<T>().iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        self.db::<T>().skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.db::<T>().reverse_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.db::<T>().record_prior_to::<T>(key)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.db::<T>().last_record::<T>()
    }

    /// Wait until every earlier write in `T`'s environment is durable.
    fn persist<T: Table>(&self) -> impl std::future::Future<Output = eyre::Result<()>> + Send {
        self.db::<T>().persist::<T>()
    }

    /// Ask every environment's database to compact its log now (in the background).
    fn compact(&self) -> eyre::Result<()> {
        for db in &self.dbs {
            db.compact()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;
    use tn_types::{Database as _, DbTx as _, DbTxMut as _};

    use super::*;
    use crate::{test::*, tndb::CommitMode};

    /// An epoch-hinted table, beside the shared suite's `TestTable` (`Cache`).
    #[derive(Debug)]
    struct EpochTable;
    impl Table for EpochTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "EpochTable";
        const HINT: TableHint = TableHint::Epoch;
    }

    fn open(path: &Path) -> SplitDatabase {
        let options = TnLogOptions { commit: CommitMode::Group, ..Default::default() };
        let db = SplitDatabase::open_with(path, options).expect("open split");
        db.open_table::<TestTable>().expect("open table");
        db.open_table::<EpochTable>().expect("open table");
        db
    }

    #[test]
    fn test_tnlog_split_shared_suite() {
        let run = |test: fn(SplitDatabase)| {
            let tmp = TempDir::with_prefix("tnlog_split_suite").expect("temp dir");
            test(open(tmp.path()));
        };
        run(test_contains_key);
        run(test_get);
        run(test_multi_get);
        run(test_skip);
        run(test_skip_to_previous_simple);
        run(test_iter_skip_to_previous_gap);
        run(test_remove);
        run(test_iter);
        run(test_iter_reverse);
        run(test_clear);
        run(test_is_empty);
        run(test_multi_insert);
        run(test_multi_remove);
    }

    /// The cache environment's automatic threshold is raised to [`CACHE_AUTO_MIN_BYTES`] (a larger
    /// one kept, an off one left off); the other environments keep the configured one.
    #[test]
    fn test_tnlog_split_cache_compacts_less_often() {
        let with = |auto_min_bytes| TnLogOptions {
            commit: CommitMode::Group,
            compaction: CompactionConfig { auto_min_bytes, bytes_per_sec: 7 },
        };
        let auto = |options: TnLogOptions, env| env_options(options, env).compaction;
        let default = with(Some(64 << 20));
        assert_eq!(auto(default, EPOCH), default.compaction);
        assert_eq!(auto(default, KAD), default.compaction);
        assert_eq!(
            auto(default, CACHE),
            CompactionConfig { auto_min_bytes: Some(CACHE_AUTO_MIN_BYTES), bytes_per_sec: 7 }
        );
        assert_eq!(auto(with(Some(1 << 30)), CACHE).auto_min_bytes, Some(1 << 30));
        assert_eq!(auto(with(None), CACHE).auto_min_bytes, None, "off stays off");
    }

    /// Each table lives in its hint's database; a transaction over both environments writes its
    /// part in each, and its own reads see both.
    #[test]
    fn test_tnlog_split_routes_by_hint() {
        let tmp = TempDir::with_prefix("tnlog_split_route").expect("temp dir");
        let db = open(tmp.path());
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<TestTable>(&1, &"cache".to_string()).expect("insert");
        txn.insert::<EpochTable>(&1, &"epoch".to_string()).expect("insert");
        assert_eq!(txn.get::<EpochTable>(&1).expect("get"), Some("epoch".to_string()));
        txn.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&1).expect("get"), Some("cache".to_string()));
        assert_eq!(db.get::<EpochTable>(&1).expect("get"), Some("epoch".to_string()));
        drop(db);
        let manifest =
            |env: &str| std::fs::read(tmp.path().join(env).join("tables")).expect("manifest");
        let names = |env: &str| String::from_utf8_lossy(&manifest(env)).into_owned();
        assert!(names("cache").contains("TestTable") && !names("cache").contains("EpochTable"));
        assert!(names("epoch").contains("EpochTable") && !names("epoch").contains("TestTable"));
    }
}
