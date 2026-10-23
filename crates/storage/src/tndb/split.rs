//! [`SplitDatabase`]: one tndb database (one shared log, write lock and commit thread) per
//! production environment, routed by each table's [`TableHint`] with no layer in between. The
//! batch caches (`Cache`) then never share a write lock with consensus (`Epoch`): a large batch
//! append holds up only other cache writes.
//!
//! A write transaction is atomic within each environment it writes (its part in each commits as
//! one batch there); `persist` covers the table's environment. The anti-equivocation barriers are
//! on consensus tables; the node's epoch-end check (`ensure_stores_durable`) persists one table in
//! each environment, and [`Database::failed`] names whichever environment latched a failure.
//!
//! The cache environment compacts automatically only once its log is at least
//! [`CACHE_AUTO_MIN_BYTES`]: it churns at the batch write rate, and each compaction's flushes slow
//! the other logs' syncs while it runs.

use std::path::Path;

use eyre::{bail, WrapErr as _};
use tn_types::{DBIter, Database, DbTx, DbTxMut, Table, TableHint};

use super::database::{CompactionConfig, TnDatabase, TnDbOptions, TnDbTxMut};

/// The `env` label (metrics) of the environment holding epoch-scoped tables.
pub(crate) const EPOCH_ENV: &str = "epoch";

/// The `env` label of the environment holding kademlia records.
pub(crate) const KAD_ENV: &str = "kad";

/// The `env` label of the environment holding cache tables, including the batch cache.
pub(crate) const CACHE_ENV: &str = "cache";

/// The least log the cache environment compacts automatically at (a larger configured threshold
/// is kept). The batch caches churn at the batch write rate: at the default 64 MiB, a heavy batch
/// stream keeps their log compacting most of the time, and each compaction's flushes queue ahead
/// of the epoch log's syncs (see the tndb README, "Cost to other logs"). Four times the default
/// makes those compactions four times rarer, for up to that much more cache log on disk.
pub const CACHE_AUTO_MIN_BYTES: u64 = 256 << 20;

/// The least log the epoch environment compacts automatically at mid-epoch: a safety net only.
/// The epoch tables are all cleared at every epoch's end, which compacts the log at once (nothing
/// live to copy; see the tndb README's Compaction section), so consensus writes see no
/// compaction stalls mid-epoch.
pub const EPOCH_AUTO_MIN_BYTES: u64 = 2 << 30;

/// The environments' directories under a `SplitDatabase`'s root, in [`EPOCH`], [`KAD`],
/// [`CACHE`] order (the order of `SplitDatabase::dbs`).
pub(super) const ENVIRONMENTS: [&str; 3] = ["epoch", "kad", "cache"];

/// The file in a `SplitDatabase`'s root recording that the node's MDBX consensus tables were
/// copied in (see `migrate_from_mdbx` in the crate root).
pub const MDBX_MIGRATED: &str = "mdbx-migrated";

const EPOCH: usize = 0;
const KAD: usize = 1;
const CACHE: usize = 2;

/// Refuse a `SplitDatabase` at `root` that lost an environment's directory. [`MDBX_MIGRATED`] is
/// written only after the first open has created all three (`open_db` opens them before
/// `migrate_from_mdbx` runs), so with it present a missing environment was deleted or not
/// restored, and opening it would start that environment empty: for `epoch`, with no record of
/// this node's votes and proposals. Before the marker exists (a first start), the environments
/// are created as usual. A stat error is an error, never read as absent.
fn check_environments(root: &Path) -> eyre::Result<()> {
    let marker = root.join(MDBX_MIGRATED);
    let stat = |path: &Path| {
        path.try_exists().wrap_err_with(|| format!("tndb: check for {}", path.display()))
    };
    if !stat(&marker)? {
        return Ok(());
    }
    for env in ENVIRONMENTS {
        let dir = root.join(env);
        if !stat(&dir)? {
            bail!(
                "tndb: environment {} is missing beside {} (its log is lost); refusing to open \
                 it empty",
                dir.display(),
                marker.display()
            );
        }
    }
    Ok(())
}

/// The options for environment `env`: `options`, with the cache's automatic threshold raised to
/// at least [`CACHE_AUTO_MIN_BYTES`] and the epoch's to at least [`EPOCH_AUTO_MIN_BYTES`]
/// (automatic compaction left off if it is off).
fn env_options(options: TnDbOptions, env: usize) -> TnDbOptions {
    let floor = match env {
        CACHE => CACHE_AUTO_MIN_BYTES,
        EPOCH => EPOCH_AUTO_MIN_BYTES,
        _ => return options,
    };
    let auto_min_bytes = options.compaction.auto_min_bytes.map(|min| min.max(floor));
    TnDbOptions { compaction: CompactionConfig { auto_min_bytes, ..options.compaction }, ..options }
}

fn env(hint: TableHint) -> usize {
    match hint {
        TableHint::Epoch => EPOCH,
        TableHint::Kad => KAD,
        TableHint::Cache => CACHE,
    }
}

/// Three tndb databases (epoch, kad, cache) behind one `Database`, each table in its hint's.
#[derive(Clone, Debug)]
pub struct SplitDatabase {
    dbs: [TnDatabase; 3],
}

impl SplitDatabase {
    /// Open (creating if absent) the three databases under `root` (`epoch/`, `kad/`, `cache/`),
    /// committing and compacting as `options` say (the cache compacting automatically at no less
    /// than [`CACHE_AUTO_MIN_BYTES`]). Once [`MDBX_MIGRATED`] is in `root`, every environment
    /// has been created, so a missing one is refused (see `check_environments`).
    pub fn open_with<P: AsRef<Path>>(root: P, options: TnDbOptions) -> eyre::Result<Self> {
        let root = root.as_ref();
        check_environments(root)?;
        Ok(Self {
            dbs: [
                TnDatabase::open_named(
                    root.join(ENVIRONMENTS[EPOCH]),
                    env_options(options, EPOCH),
                    EPOCH_ENV,
                )?,
                TnDatabase::open_named(
                    root.join(ENVIRONMENTS[KAD]),
                    env_options(options, KAD),
                    KAD_ENV,
                )?,
                TnDatabase::open_named(
                    root.join(ENVIRONMENTS[CACHE]),
                    env_options(options, CACHE),
                    CACHE_ENV,
                )?,
            ],
        })
    }

    /// Block until every write so far, in every environment, is durable (see
    /// [`TnDatabase::wait_durable_blocking`]).
    #[cfg(any(test, feature = "reth-libmdbx"))]
    pub(crate) fn wait_durable_blocking(&self) -> eyre::Result<()> {
        for db in &self.dbs {
            db.wait_durable_blocking()?;
        }
        Ok(())
    }

    fn db<T: Table>(&self) -> &TnDatabase {
        &self.dbs[env(T::HINT)]
    }

    /// Test-only: latch a durability failure in the database holding `T` (see
    /// [`TnDatabase::fail_durability_for_test`]); the other two are unaffected.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_durability_for_test<T: Table>(&self) {
        self.db::<T>().fail_durability_for_test();
    }

    /// [`TnDatabase::scan_raw`] in the database holding `T`.
    pub(crate) fn scan_raw<T: Table>(&self, visit: impl FnMut(&[u8], &[u8])) -> eyre::Result<()> {
        self.db::<T>().scan_raw::<T>(visit)
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

/// A write transaction: one buffered tndb transaction per environment it writes, each committed
/// (or, dropped, written) as one batch in its environment.
#[derive(Debug)]
pub struct SplitTxMut {
    db: SplitDatabase,
    txns: [Option<TnDbTxMut>; 3],
}

impl SplitTxMut {
    fn txn<T: Table>(&mut self) -> eyre::Result<&mut TnDbTxMut> {
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

    /// Resolve once any of the three environments has latched a failure, naming it.
    fn failed(&self) -> impl std::future::Future<Output = String> + Send + 'static {
        use std::{future::Future as _, task::Poll};
        let mut envs: Vec<_> = ENVIRONMENTS
            .into_iter()
            .zip(self.dbs.iter().map(|db| Box::pin(db.failure())))
            .collect();
        std::future::poll_fn(move |cx| {
            for (env, failure) in &mut envs {
                if let Poll::Ready(cause) = failure.as_mut().poll(cx) {
                    return Poll::Ready(format!("tndb: the {env} environment failed: {cause}"));
                }
            }
            Poll::Pending
        })
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

    /// A kad-hinted table.
    #[derive(Debug)]
    struct KadTable;
    impl Table for KadTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "KadTable";
        const HINT: TableHint = TableHint::Kad;
    }

    fn open(path: &Path) -> SplitDatabase {
        let options = TnDbOptions { commit: CommitMode::Group, ..Default::default() };
        let db = SplitDatabase::open_with(path, options).expect("open split");
        db.open_table::<TestTable>().expect("open table");
        db.open_table::<EpochTable>().expect("open table");
        db.open_table::<KadTable>().expect("open table");
        db
    }

    /// Simulate a crash of every environment: their commit threads stop and their locks are
    /// released, as a dead process's are, and nothing is synced or sealed on the way out.
    fn crash(db: SplitDatabase) {
        for env in &db.dbs {
            env.release_locks_for_crash();
        }
        std::mem::forget(db);
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().build().expect("runtime").block_on(f)
    }

    #[test]
    fn test_tndb_split_shared_suite() {
        let run = |test: fn(SplitDatabase)| {
            let tmp = TempDir::with_prefix("tndb_split_suite").expect("temp dir");
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

    /// The cache environment's automatic threshold is raised to [`CACHE_AUTO_MIN_BYTES`] and the
    /// epoch environment's to [`EPOCH_AUTO_MIN_BYTES`] (a larger one kept, an off one left off);
    /// kad keeps the configured one.
    #[test]
    fn test_tndb_split_cache_compacts_less_often() {
        let with = |auto_min_bytes| TnDbOptions {
            commit: CommitMode::Group,
            compaction: CompactionConfig { auto_min_bytes, bytes_per_sec: 7 },
        };
        let auto = |options: TnDbOptions, env| env_options(options, env).compaction;
        let default = with(Some(64 << 20));
        assert_eq!(auto(default, KAD), default.compaction);
        assert_eq!(
            auto(default, CACHE),
            CompactionConfig { auto_min_bytes: Some(CACHE_AUTO_MIN_BYTES), bytes_per_sec: 7 }
        );
        assert_eq!(auto(with(Some(1 << 30)), CACHE).auto_min_bytes, Some(1 << 30));
        assert_eq!(auto(with(None), CACHE).auto_min_bytes, None, "off stays off");
        // The epoch log compacts at the epoch boundary; mid-epoch only past a safety threshold.
        assert_eq!(
            auto(default, EPOCH),
            CompactionConfig { auto_min_bytes: Some(EPOCH_AUTO_MIN_BYTES), bytes_per_sec: 7 }
        );
        assert_eq!(auto(with(None), EPOCH).auto_min_bytes, None, "off stays off");
    }

    /// Each table lives in its hint's database; a transaction over both environments writes its
    /// part in each, and its own reads see both.
    #[test]
    fn test_tndb_split_routes_by_hint() {
        let tmp = TempDir::with_prefix("tndb_split_route").expect("temp dir");
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

    /// A power loss (the disk model keeps only what was flushed) and reopen of all three
    /// environments keeps every persisted row (a persist covers its table's environment).
    #[test]
    fn test_tndb_split_power_loss_reopens_every_environment() {
        use crate::archive::data_file::disk_model;
        let tmp = TempDir::with_prefix("tndb_split_crash").expect("temp dir");
        let db = open(tmp.path());
        let logs = ENVIRONMENTS.map(|env| tmp.path().join(env).join("gen-0").join("log"));
        for log in &logs {
            disk_model::track(log);
        }
        for i in 0..50_u64 {
            db.insert::<EpochTable>(&i, &format!("e{i}")).expect("insert");
            db.insert::<KadTable>(&i, &format!("k{i}")).expect("insert");
            db.insert::<TestTable>(&i, &format!("c{i}")).expect("insert");
        }
        block_on(db.persist::<EpochTable>()).expect("persist epoch");
        block_on(db.persist::<KadTable>()).expect("persist kad");
        block_on(db.persist::<TestTable>()).expect("persist cache");
        crash(db);
        for log in &logs {
            disk_model::power_loss(log);
        }
        let db = open(tmp.path());
        assert_eq!(db.iter::<EpochTable>().count(), 50);
        assert_eq!(db.iter::<KadTable>().count(), 50);
        assert_eq!(db.iter::<TestTable>().count(), 50);
        assert_eq!(db.get::<KadTable>(&7).expect("get"), Some("k7".to_string()));
        drop(db);
        for log in &logs {
            disk_model::untrack(log);
        }
    }

    /// Once the MDBX migration's marker is written, every environment exists: a missing one (an
    /// operator's deletion, a partial restore) is refused by name rather than recreated empty, and
    /// the refused open creates nothing. Before the marker (a first start), a missing
    /// environment is created as usual.
    #[test]
    fn test_tndb_split_refuses_a_missing_environment_after_the_migration() {
        for lost in ENVIRONMENTS {
            let tmp = TempDir::with_prefix("tndb_split_lost_env").expect("temp dir");
            {
                let db = open(tmp.path());
                db.insert::<EpochTable>(&1, &"epoch".to_string()).expect("insert");
                db.insert::<KadTable>(&1, &"kad".to_string()).expect("insert");
                db.insert::<TestTable>(&1, &"cache".to_string()).expect("insert");
            }
            // No marker yet: a first start (or one before the migration finished) creates it.
            std::fs::remove_dir_all(tmp.path().join(lost)).expect("lose an environment");
            drop(open(tmp.path()));
            assert!(tmp.path().join(lost).is_dir(), "{lost}: created before the marker");

            std::fs::write(tmp.path().join(MDBX_MIGRATED), b"").expect("marker");
            drop(open(tmp.path()));
            std::fs::remove_dir_all(tmp.path().join(lost)).expect("lose an environment");
            let err = SplitDatabase::open_with(tmp.path(), TnDbOptions::default())
                .err()
                .unwrap_or_else(|| panic!("{lost}: a missing environment must not open empty"));
            let err = format!("{err:#}");
            assert!(
                err.contains(&format!("environment {}", tmp.path().join(lost).display()))
                    && err.contains("missing beside"),
                "{lost}: the refusal names it: {err}"
            );
            assert!(!tmp.path().join(lost).exists(), "{lost}: the refused open created nothing");
        }
    }

    /// A transaction over two environments commits each part in its own log: when one part
    /// fails, the commit fails and that part is rolled back, while the other is committed (atomic
    /// per environment, as documented) and the failed environment carries on.
    #[test]
    fn test_tndb_split_txn_with_a_failing_environment() {
        let tmp = TempDir::with_prefix("tndb_split_partial").expect("temp dir");
        let db = open(tmp.path());
        db.dbs[CACHE].core().fail_next_batch();
        let mut txn = db.write_txn().expect("txn");
        txn.insert::<EpochTable>(&1, &"epoch".to_string()).expect("insert");
        txn.insert::<TestTable>(&1, &"cache".to_string()).expect("insert");
        assert!(txn.commit().is_err(), "the cache part failed");
        assert_eq!(db.get::<EpochTable>(&1).expect("get"), Some("epoch".to_string()));
        assert_eq!(db.get::<TestTable>(&1).expect("get"), None, "rolled back");
        db.insert::<TestTable>(&2, &"later".to_string()).expect("the cache carries on");
        block_on(db.persist::<EpochTable>()).expect("persist");
        block_on(db.persist::<TestTable>()).expect("persist");
        crash(db);
        let db = open(tmp.path());
        assert_eq!(db.get::<EpochTable>(&1).expect("get"), Some("epoch".to_string()));
        assert_eq!(db.get::<TestTable>(&1).expect("get"), None);
        assert_eq!(db.get::<TestTable>(&2).expect("get"), Some("later".to_string()));
    }

    /// `failed` stays pending while every environment is sound, and resolves promptly once one
    /// latches a failure (here from another thread, as a sync round's would), naming it; the
    /// other environments carry on.
    #[tokio::test]
    async fn test_tndb_split_failed_names_the_environment() {
        use std::time::Duration;
        let tmp = TempDir::with_prefix("tndb_split_failed").expect("temp dir");
        let db = open(tmp.path());
        let mut failed = std::pin::pin!(db.failed());
        let early = tokio::time::timeout(Duration::from_millis(50), failed.as_mut()).await;
        assert!(early.is_err(), "pending while nothing failed: {early:?}");
        let failing = db.clone();
        let fail = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            failing.fail_durability_for_test::<KadTable>();
        });
        let cause = tokio::time::timeout(Duration::from_secs(5), failed).await.expect("promptly");
        fail.join().expect("thread");
        assert!(cause.contains("the kad environment failed"), "{cause}");
        assert!(cause.contains("injected by a test"), "{cause}");
        db.insert::<EpochTable>(&1, &"epoch".to_string()).expect("the epoch store carries on");
        db.persist::<EpochTable>().await.expect("persist");
        assert!(db.persist::<KadTable>().await.is_err(), "the kad store stays failed");
    }
}
