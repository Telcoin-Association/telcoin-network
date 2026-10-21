//! [`TndbEnv`]: the environment, its table map, and reth's `Database` and `DatabaseMetrics`.

use crate::{
    dup::{dup_layout, DupLayout},
    error,
    tx::{TndbTx, TndbTxMut},
};
use metrics::Label;
use reth_db_api::{
    database::Database,
    database_metrics::DatabaseMetrics,
    table::Table as RethTable,
    tables::{TableSet, Tables},
    DatabaseError,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    path::Path,
    sync::Arc,
};
use tndb_b::{Env, EnvOptions, ReadTxn, SyncMode, Table};

/// The name of the page file inside the environment's directory.
pub const DATA_FILE: &str = "tndb.dat";

/// How an environment is opened: as reth's `DatabaseEnvKind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TndbEnvKind {
    /// Read-only: an existing file no writer holds (the node stopped). Never creates, writes or
    /// syncs; `tx_mut` and `create_tables` fail with `MDBX_EACCESS`.
    RO,
    /// Read-write: creates the directory and the file if missing.
    RW,
}

impl TndbEnvKind {
    /// `true` for [`Self::RW`].
    pub const fn is_rw(&self) -> bool {
        matches!(self, Self::RW)
    }
}

/// Settings for [`TndbEnv::open`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TndbArgs {
    /// The page size of a new file: a power of two in 4 KiB..=64 KiB (default 4096). An existing
    /// file keeps its own.
    pub page_size: u32,
    /// Address space the mapping reserves, in bytes (default: the engine's).
    pub map_reserve: u64,
    /// When commits become durable (default [`SyncMode::Durable`]).
    pub sync: SyncMode,
}

impl Default for TndbArgs {
    fn default() -> Self {
        let engine = EnvOptions::default();
        Self { page_size: 4096, map_reserve: engine.map_reserve, sync: SyncMode::Durable }
    }
}

impl TndbArgs {
    /// Set the page size of a new file.
    pub const fn with_page_size(mut self, page_size: u32) -> Self {
        self.page_size = page_size;
        self
    }

    /// Set when commits become durable.
    pub const fn with_sync_mode(mut self, sync: SyncMode) -> Self {
        self.sync = sync;
        self
    }

    /// Set the address space the mapping reserves.
    pub const fn with_map_reserve(mut self, map_reserve: u64) -> Self {
        self.map_reserve = map_reserve;
        self
    }

    const fn options(&self) -> EnvOptions {
        EnvOptions { page_size: self.page_size, map_reserve: self.map_reserve, sync: self.sync }
    }
}

/// One table of the map: its engine handle and how its rows are laid out.
#[derive(Clone, Debug)]
pub(crate) struct TableEntry {
    /// The engine's handle.
    pub(crate) table: Table,
    /// For a dup-sort table stored as composite keys, its layout ([`DupLayout`]); `None` for a
    /// plain table, and for a dup-sort table this crate does not store (each access to its rows
    /// is refused, see [`TableEntry::check`]).
    pub(crate) dup: Option<DupLayout>,
}

impl TableEntry {
    fn new(table: Table) -> Self {
        let dup = dup_layout(table.name());
        Self { table, dup }
    }

    /// The handle and row layout for reading and writing the rows of `T` through this entry: a
    /// plain table, or a dup-sort table with a known layout. Another dup-sort table is refused
    /// with a [`DatabaseError::Other`]. The layout is the table's, whatever `T` says: a raw view
    /// of a dup-sort table reads and writes whole values, as on MDBX. Counting and clearing need
    /// no layout and skip this check.
    pub(crate) fn check<T: RethTable>(&self) -> Result<(&Table, Option<DupLayout>), DatabaseError> {
        if T::DUPSORT && self.dup.is_none() {
            return Err(error::dup_sort_unsupported(T::NAME));
        }
        Ok((&self.table, self.dup))
    }
}

/// The tables by name, built at open and extended by [`TndbEnv::create_tables`]. Every
/// transaction call looks its table up here, so the map hashes with [`NameHasher`].
pub(crate) type TableMap = HashMap<&'static str, TableEntry, BuildHasherDefault<NameHasher>>;

/// The [`TableMap`]'s hasher: a word at a time, rotate, xor and multiply (FxHash's step), several
/// times faster than the default SipHash on a table name. The keys are table names from reth's
/// table sets, which this crate's own code supplies, so the map needs no defence against chosen
/// collisions.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NameHasher(u64);

impl NameHasher {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for NameHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut words = bytes.chunks_exact(8);
        for word in &mut words {
            self.add(u64::from_le_bytes(word.try_into().expect("8 bytes")));
        }
        let rest = words.remainder();
        if !rest.is_empty() {
            let mut word = [0; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word));
        }
    }

    fn write_u8(&mut self, byte: u8) {
        self.add(u64::from(byte));
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// The entry of table `T`: from the map, else looked up in the transaction's state (a table
/// this environment did not create or find at open), else MDBX's `NOTFOUND` as
/// [`DatabaseError::Open`], as an MDBX transaction reports a table it cannot open.
pub(crate) fn resolve<'m, T: RethTable>(
    tables: &'m TableMap,
    txn: &impl ReadTxn,
) -> Result<Cow<'m, TableEntry>, DatabaseError> {
    if let Some(entry) = tables.get(T::NAME) {
        return Ok(Cow::Borrowed(entry));
    }
    match txn.open_table(T::NAME) {
        Ok(Some(table)) => Ok(Cow::Owned(TableEntry::new(table))),
        Ok(None) => Err(error::table_not_found()),
        Err(e) => Err(DatabaseError::Open(error::info(&e))),
    }
}

/// reth's database on tndb-b: one page file, `<dir>/tndb.dat` ([`DATA_FILE`]), and a map from
/// each table's name to its engine handle. It implements reth's [`Database`] (transactions
/// [`TndbTx`] and [`TndbTxMut`]) and [`DatabaseMetrics`].
#[derive(Debug)]
pub struct TndbEnv {
    env: Env,
    tables: Arc<TableMap>,
    kind: TndbEnvKind,
}

impl TndbEnv {
    /// Open the environment in directory `dir` (see [`TndbEnvKind`]). The table map holds every
    /// one of reth's [`Tables`] the file has; [`Self::create_tables`] creates the rest.
    ///
    /// A file another handle holds is `MDBX_BUSY`; a read-only open of a missing file is an I/O
    /// error (`ENOENT`). Both are [`DatabaseError::Open`].
    pub fn open(dir: &Path, kind: TndbEnvKind, args: TndbArgs) -> Result<Self, DatabaseError> {
        let open_error = |e: tndb_b::Error| DatabaseError::Open(error::info(&e));
        let path = dir.join(DATA_FILE);
        let env = match kind {
            TndbEnvKind::RW => {
                std::fs::create_dir_all(dir).map_err(|e| open_error(e.into()))?;
                Env::open(&path, args.options())
            }
            TndbEnvKind::RO => Env::open_read_only(&path, args.options()),
        }
        .map_err(open_error)?;
        let txn = env.begin_ro().map_err(open_error)?;
        let mut tables = TableMap::default();
        for name in Tables::ALL.iter().map(Tables::name) {
            if let Some(table) = txn.open_table(name).map_err(open_error)? {
                tables.insert(name, TableEntry::new(table));
            }
        }
        Ok(Self { env, tables: Arc::new(tables), kind })
    }

    /// Create every one of reth's [`Tables`] the file lacks, and add them to the table map, as
    /// reth's `DatabaseEnv::create_tables` does.
    pub fn create_tables(&mut self) -> Result<(), DatabaseError> {
        self.create_tables_for::<Tables>()
    }

    /// Create every table of `TS` the file lacks, and add them to the table map.
    pub fn create_tables_for<TS: TableSet>(&mut self) -> Result<(), DatabaseError> {
        let mut txn = self.env.begin_rw().map_err(|e| DatabaseError::InitTx(error::info(&e)))?;
        let mut created = Vec::new();
        for info in TS::tables() {
            let name = info.name();
            let table =
                txn.create_table(name).map_err(|e| DatabaseError::CreateTable(error::info(&e)))?;
            created.push((name, TableEntry::new(table)));
        }
        txn.commit().map_err(|e| DatabaseError::Commit(error::info(&e)))?;
        Arc::make_mut(&mut self.tables).extend(created);
        Ok(())
    }

    /// The engine environment.
    pub const fn env(&self) -> &Env {
        &self.env
    }

    /// How the environment was opened.
    pub const fn kind(&self) -> TndbEnvKind {
        self.kind
    }

    /// The first failure the environment latched, with the engine's detail (a `Corrupt` error's
    /// MDBX information carries only `MDBX_CORRUPTED`), if any.
    pub fn failure(&self) -> Option<String> {
        self.env.failure()
    }

    /// The figures of each of reth's [`Tables`] in the newest committed state, as metrics.
    fn table_metrics(
        &self,
        metrics: &mut Vec<(&'static str, f64, Vec<Label>)>,
    ) -> tndb_b::Result<()> {
        let txn = self.env.begin_ro()?;
        let page_size = f64::from(self.env.stat().page_size);
        for name in Tables::ALL.iter().map(Tables::name) {
            let stat = match self.tables.get(name) {
                Some(entry) => txn.table_stat(&entry.table)?,
                None => match txn.open_table(name)? {
                    Some(table) => txn.table_stat(&table)?,
                    // MDBX's metrics stop at a table they cannot open; these skip it.
                    None => continue,
                },
            };
            let (leaf, branch, overflow) = (
                f64::from(stat.leaf_pages),
                f64::from(stat.branch_pages),
                f64::from(stat.overflow_pages),
            );
            let table = || Label::new("table", name);
            metrics.push(("db.table_size", page_size * (leaf + branch + overflow), vec![table()]));
            metrics.push(("db.table_pages", leaf, vec![table(), Label::new("type", "leaf")]));
            metrics.push(("db.table_pages", branch, vec![table(), Label::new("type", "branch")]));
            metrics.push((
                "db.table_pages",
                overflow,
                vec![table(), Label::new("type", "overflow")],
            ));
            metrics.push(("db.table_entries", stat.entries as f64, vec![table()]));
        }
        Ok(())
    }
}

impl Database for TndbEnv {
    type TX = TndbTx;
    type TXMut = TndbTxMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        let txn = self.env.begin_ro().map_err(|e| DatabaseError::InitTx(error::info(&e)))?;
        Ok(TndbTx::new(txn, Arc::clone(&self.tables)))
    }

    /// Waits while another write transaction is open, as MDBX does, holding no lock of this
    /// crate's while it waits.
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        let txn = self.env.begin_rw().map_err(|e| DatabaseError::InitTx(error::info(&e)))?;
        Ok(TndbTxMut::new(txn, Arc::clone(&self.tables)))
    }
}

impl DatabaseMetrics for TndbEnv {
    /// The gauges reth's MDBX environment reports, with the same names and labels: for each of
    /// reth's tables `db.table_size` (page size times its branch, leaf and overflow pages),
    /// `db.table_pages` (`type` = `leaf`, `branch`, `overflow`) and `db.table_entries`; then
    /// `db.freelist` (the pages the free list holds), `db.page_size`, and
    /// `db.timed_out_not_aborted_transactions` (always 0: tndb-b times no transaction out).
    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        let mut metrics = Vec::new();
        if let Err(error) = self.table_metrics(&mut metrics) {
            tracing::error!(%error, "Failed to read db table stats");
        }
        match self.env.free_pages() {
            Ok(pages) => metrics.push(("db.freelist", pages as f64, vec![])),
            Err(error) => tracing::error!(%error, "Failed to read db.freelist"),
        }
        metrics.push(("db.page_size", f64::from(self.env.stat().page_size), vec![]));
        metrics.push(("db.timed_out_not_aborted_transactions", 0.0, vec![]));
        metrics
    }
}
