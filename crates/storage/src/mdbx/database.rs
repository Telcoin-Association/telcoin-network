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
/// is the default `open` applies when [`TN_TEST_MDBX_SYNC_ENV`] is unset, so a test can pin it
/// exactly. Production builds enable neither cfg and this const does not exist for them (the
/// env stays `Durable`).
#[cfg(any(test, feature = "test-utils"))]
const BUILD_SYNC_MODE: reth_libmdbx::SyncMode = reth_libmdbx::SyncMode::SafeNoSync;

/// The environment variable a `test`/`test-utils` build reads at `open` to override
/// [`BUILD_SYNC_MODE`] with no rebuild (#1149). `durable` restores the production per-commit
/// fsync regime; `safe-no-sync` selects the compiled default explicitly. Any other value is a
/// hard error from [`MdbxDatabase::open`]: a silent fallback could green-run a "Durable" e2e
/// lane that never ran `Durable`. Production builds compile the read out with the rest of the
/// cfg block.
#[cfg(any(test, feature = "test-utils"))]
const TN_TEST_MDBX_SYNC_ENV: &str = "TN_TEST_MDBX_SYNC";

/// Resolves the sync mode a `test`/`test-utils` build opens the environment with: the parsed
/// [`TN_TEST_MDBX_SYNC_ENV`] value when the variable is set, otherwise [`BUILD_SYNC_MODE`]. A
/// set-but-invalid value (not UTF-8, or a spelling `SyncMode::from_str` rejects) is an error,
/// never a fallback. Every spelling the parser accepts maps to `Durable` or `SafeNoSync`, so
/// no environment value can reach `UtterlyNoSync`.
#[cfg(any(test, feature = "test-utils"))]
fn resolve_sync_mode(raw: Option<std::ffi::OsString>) -> eyre::Result<reth_libmdbx::SyncMode> {
    raw.map(|value| {
        value
            .into_string()
            .map_err(|value| eyre::eyre!("{TN_TEST_MDBX_SYNC_ENV} is not valid UTF-8: {value:?}"))
            .and_then(|value| {
                value
                    .parse::<reth_libmdbx::SyncMode>()
                    .map_err(|err| eyre::eyre!("invalid {TN_TEST_MDBX_SYNC_ENV} value: {err}"))
            })
    })
    .transpose()
    .map(|parsed| parsed.unwrap_or(BUILD_SYNC_MODE))
}

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
        //
        // #1149: `TN_TEST_MDBX_SYNC` overrides the compiled default at `open`. `durable`
        // restores the production per-commit fsync regime for the scheduled Durable e2e lane
        // (and for A/B timing runs like #1142's) with no rebuild; an invalid value fails
        // `open` outright. The e2e nodes inherit the variable through `std::process::Command`
        // (the harness never clears the child env), so a CI-level export reaches every
        // spawned node.
        #[cfg(any(test, feature = "test-utils"))]
        {
            use reth_libmdbx::{EnvironmentFlags, Mode};
            let sync_mode = resolve_sync_mode(std::env::var_os(TN_TEST_MDBX_SYNC_ENV))?;
            builder.set_flags(EnvironmentFlags {
                mode: Mode::ReadWrite { sync_mode },
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

    /// The harness-visible name of a test in this module: the module path minus the leading
    /// crate-name segment, as libtest prints (and `--exact` matches) it.
    fn child_test_name(fn_name: &str) -> String {
        module_path!()
            .split_once("::")
            .map_or_else(|| fn_name.to_string(), |(_, module)| format!("{module}::{fn_name}"))
    }

    /// Runs one `#[ignore]` child test of THIS binary in a fresh process, with
    /// `TN_TEST_MDBX_SYNC` set to `value` or explicitly removed (`None`). Env mutation in a
    /// child process cannot race sibling tests in this one. The child's harness output must
    /// report exactly one passed test: a drifted name would match nothing and still exit 0,
    /// so exit status alone would be a vacuous pass.
    fn run_child_test(fn_name: &str, value: Option<&str>) {
        let exe = std::env::current_exe().expect("test binary path");
        let name = child_test_name(fn_name);
        let mut command = std::process::Command::new(exe);
        command.args(["--exact", name.as_str(), "--ignored"]);
        // The variable is never inherited: it is removed outright, then set only when this
        // regime supplies a value, so ambient state cannot leak into any child.
        command.env_remove(super::TN_TEST_MDBX_SYNC_ENV);
        value.into_iter().for_each(|value| {
            command.env(super::TN_TEST_MDBX_SYNC_ENV, value);
        });
        let output = command.output().expect("spawn child test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "child test {name} did not pass exactly once; status {:?}, stdout:\n{stdout}",
            output.status
        );
    }

    /// Child of [`test_open_uses_safe_no_sync_under_test_cfg`], spawned with
    /// `TN_TEST_MDBX_SYNC` removed from its env: the compiled default must reach MDBX, so the
    /// opened env reports a non-`Durable` mode (no hot-path fsync). NOTE: MDBX defines
    /// `MDBX_UTTERLY_NOSYNC = MDBX_SAFE_NOSYNC | <extra bit>`, and reth-libmdbx's `mode()`
    /// tests the UtterlyNoSync mask before the SafeNoSync one, so it reports `UtterlyNoSync`
    /// for a genuine `SafeNoSync` env. We therefore assert only "not Durable / not read-only"
    /// from the readback; the exact-mode guarantee is pinned by the parent's const assert. Do
    /// not "fix" this into `assert_eq!(mode, ..SafeNoSync)` -- it cannot pass by construction.
    #[test]
    #[ignore = "spawned by test_open_uses_safe_no_sync_under_test_cfg with a controlled env"]
    fn child_open_default_safe_no_sync() {
        use reth_libmdbx::{Mode, SyncMode};
        assert!(
            std::env::var_os(super::TN_TEST_MDBX_SYNC_ENV).is_none(),
            "this child expects TN_TEST_MDBX_SYNC absent from its env"
        );
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        let mode = db.inner.info().expect("read env info").mode();
        assert!(
            matches!(mode, Mode::ReadWrite { sync_mode } if sync_mode != SyncMode::Durable),
            "test-build MDBX env must not be Durable (hot-path fsync must be off), got {mode:?}"
        );
    }

    /// Child of [`test_open_uses_safe_no_sync_under_test_cfg`], spawned with
    /// `TN_TEST_MDBX_SYNC=durable`: the override must force `Durable` at `open`. `Durable`
    /// sets no no-sync bits, so the mask caveat on the default child does not apply and the
    /// read-back is exact.
    #[test]
    #[ignore = "spawned by test_open_uses_safe_no_sync_under_test_cfg with a controlled env"]
    fn child_open_durable_override() {
        use reth_libmdbx::{Mode, SyncMode};
        assert_eq!(
            std::env::var_os(super::TN_TEST_MDBX_SYNC_ENV).as_deref(),
            Some(std::ffi::OsStr::new("durable")),
            "this child expects TN_TEST_MDBX_SYNC=durable in its env"
        );
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        let mode = db.inner.info().expect("read env info").mode();
        assert!(
            matches!(mode, Mode::ReadWrite { sync_mode: SyncMode::Durable }),
            "TN_TEST_MDBX_SYNC=durable must open the env Durable, got {mode:?}"
        );
    }

    /// Child of [`test_open_uses_safe_no_sync_under_test_cfg`], spawned with an invalid
    /// `TN_TEST_MDBX_SYNC`: `open` must fail outright, and the error must name the variable
    /// so the failure is attributable to the resolver rather than the environment setup. A
    /// silent fallback here could green-run a "Durable" e2e lane that never ran Durable.
    #[test]
    #[ignore = "spawned by test_open_uses_safe_no_sync_under_test_cfg with a controlled env"]
    fn child_open_invalid_value_errors() {
        assert!(
            std::env::var_os(super::TN_TEST_MDBX_SYNC_ENV).is_some(),
            "this child expects an invalid TN_TEST_MDBX_SYNC in its env"
        );
        let temp_dir = tempdir().expect("failed to create temp dir");
        let result = MdbxDatabase::open(temp_dir.path(), 4, 16 * MEGABYTE, 8 * MEGABYTE);
        let error = result.err().map(|error| error.to_string()).unwrap_or_default();
        assert!(
            error.contains(super::TN_TEST_MDBX_SYNC_ENV),
            "an invalid TN_TEST_MDBX_SYNC must be a hard error naming the variable, got: \
             {error:?}"
        );
    }

    /// #917/#1149: under the `test`/`test-utils` cfg the env must open in `SafeNoSync` (no
    /// hot-path `fsync`) -- specifically not `Durable` (would keep the fsync) and, critically,
    /// not `UtterlyNoSync` (risks whole-DB corruption on an OS/power crash) -- unless
    /// `TN_TEST_MDBX_SYNC` overrides the mode at runtime: `durable` restores the production
    /// fsync regime with no rebuild, and an invalid value is a hard error from `open`, never a
    /// fallback. The e2e restart suite cannot tell the two no-sync modes apart, so the
    /// const-assert is the real guard for the compiled default.
    ///
    /// Each live read-back runs in a CHILD process of this binary (`--exact` plus a controlled
    /// child env), so no phase ever mutates this process's environment: sibling tests opening
    /// databases on other threads (plain `cargo test`) can never observe a half-set variable,
    /// a panicking phase cannot leak a poisoned value (it dies with its child process), and
    /// the spawn path exercises the same env inheritance the Durable e2e lane relies on.
    #[test]
    fn test_open_uses_safe_no_sync_under_test_cfg() {
        use super::{resolve_sync_mode, BUILD_SYNC_MODE, TN_TEST_MDBX_SYNC_ENV};
        use reth_libmdbx::SyncMode;

        // Pin the exact compiled mode. `SyncMode` is `PartialEq`, so this catches a drift to
        // `Durable` (fsync back on) or to `UtterlyNoSync` (recovery broken) -- the latter is the
        // one the readback below cannot see, so this assert is what guards the hard constraint.
        assert_eq!(BUILD_SYNC_MODE, SyncMode::SafeNoSync);

        // Pin the documented variable NAME too: the CI lane and the docs reference the
        // literal, so a drift of the const away from it must fail here.
        assert_eq!(TN_TEST_MDBX_SYNC_ENV, "TN_TEST_MDBX_SYNC");

        // The resolver, exercised pure (no process-env mutation): unset keeps the compiled
        // default; `durable` and `safe-no-sync` parse to their modes; garbage, the empty
        // string, and a non-UTF-8 value are hard errors. `UtterlyNoSync` has no accepted
        // spelling at all, so no environment value can reach it.
        assert_eq!(resolve_sync_mode(None).expect("unset env resolves"), BUILD_SYNC_MODE);
        assert_eq!(
            resolve_sync_mode(Some("durable".into())).expect("durable resolves"),
            SyncMode::Durable
        );
        assert_eq!(
            resolve_sync_mode(Some("safe-no-sync".into())).expect("safe-no-sync resolves"),
            SyncMode::SafeNoSync
        );
        assert!(resolve_sync_mode(Some("utterly-no-sync".into())).is_err());
        assert!(resolve_sync_mode(Some("".into())).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let non_utf8 = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
            assert!(resolve_sync_mode(Some(non_utf8)).is_err());
        }

        // Live read-backs against a real opened environment, one child process per regime.
        run_child_test("child_open_default_safe_no_sync", None);
        run_child_test("child_open_durable_override", Some("durable"));
        run_child_test("child_open_invalid_value_errors", Some("utterly-no-sync"));
    }
}
