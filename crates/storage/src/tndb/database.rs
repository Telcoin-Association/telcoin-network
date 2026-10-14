//! [`TnDatabase`]: the `Database` trait over the shared log, with buffered write transactions.

use std::{cell::RefCell, collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use serde::Serialize;
use tn_types::{
    decode, decode_key, encode_into_buffer, encode_key, encode_key_into, DBIter, Database, DbTx,
    DbTxMut, Table,
};

use super::{
    commit::{Committer, Durability},
    compact::Compactor,
    metrics::{TnDbMetrics, UNNAMED_ENV},
    store::{BatchOp, Core, ScanKind, TableShared},
    TableId,
};

/// When a write is committed (made durable).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CommitMode {
    /// Each write (an autocommit op, or a transaction's commit) waits for the sync covering it
    /// before it returns; [`Database::persist`] only reports a failed sync.
    #[default]
    Sync,
    /// Writes return without waiting, readable at once; the commit thread syncs the log on
    /// demand or after a short delay (see the README's Durability section), and
    /// [`Database::persist`] waits for the sync covering every earlier write. Callers never wait
    /// on the disk unless they ask to.
    Group,
}

/// When and how fast the log compacts (see the README's Compaction section).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionConfig {
    /// Compact automatically once the log is at least this many bytes, at least twice its length
    /// right after the last compaction, and may hold a dead row (or at once when a batch leaves
    /// every table empty); `None` compacts only on request ([`tn_types::Database::compact`]).
    pub auto_min_bytes: Option<u64>,
    /// The pace of a compaction's copy, in bytes per second (0: unpaced). The catch-up after it
    /// is never paced.
    pub bytes_per_sec: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self { auto_min_bytes: Some(64 << 20), bytes_per_sec: 64 << 20 }
    }
}

/// How a [`TnDatabase`] commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TnDbOptions {
    /// When writes are durable: `Sync`, each write (or transaction commit) waits for the commit
    /// thread's sync covering it before it returns; `Group`, writes return at once and
    /// [`Database::persist`] waits. Both share one sync per round among every writer waiting.
    pub commit: CommitMode,
    /// When the log compacts automatically, and how fast a compaction copies (see the
    /// compaction docs in the README).
    pub compaction: CompactionConfig,
}

/// A [`Database`] whose tables share one log (see the module docs). A cheap `Clone` handle; the
/// commit thread stops (after a last sync) when the last handle drops, and the files close when
/// the last handle, transaction and scan are gone.
#[derive(Clone, Debug)]
pub struct TnDatabase {
    /// Held for its `Drop`: the last handle's drop stops the compaction thread (cancelling a
    /// running compaction) before the commit thread's last sync. Declared first, so it drops
    /// first.
    #[cfg_attr(not(test), allow(dead_code))]
    compactor: Arc<Compactor>,
    core: Arc<Core>,
    /// Held for its `Drop`: the last handle's drop stops the commit thread after a last sync.
    #[cfg_attr(not(test), allow(dead_code))]
    committer: Arc<Committer>,
    mode: CommitMode,
}

thread_local! {
    /// Encode buffers for point reads and bare writes: per thread, so nothing is shared or
    /// allocated per op.
    static BUFS: RefCell<(Vec<u8>, Vec<u8>)> = const { RefCell::new((Vec::new(), Vec::new())) };
}

/// The most capacity an encode buffer keeps between uses: one that grew past it for a large
/// value is let go, so a thread does not hold the largest value it ever wrote.
const BUF_KEEP: usize = 4 << 20;

/// Let `buf` go if a large value grew it past [`BUF_KEEP`].
fn trim(buf: &mut Vec<u8>) {
    if buf.capacity() > BUF_KEEP {
        *buf = Vec::new();
    }
}

/// Encode `key` into this thread's key buffer and run `f` on the bytes. `f` must not re-enter
/// tndb's encoding on this thread.
fn with_key<K: Serialize, R>(key: &K, f: impl FnOnce(&[u8]) -> eyre::Result<R>) -> eyre::Result<R> {
    BUFS.with_borrow_mut(|(buf, _)| {
        buf.clear();
        encode_key_into(buf, key)?;
        let out = f(buf);
        trim(buf);
        out
    })
}

/// Encode `key` and `value` into this thread's buffers and run `f` on the bytes.
fn with_key_value<K: Serialize, V: Serialize, R>(
    key: &K,
    value: &V,
    f: impl FnOnce(&[u8], &[u8]) -> eyre::Result<R>,
) -> eyre::Result<R> {
    BUFS.with_borrow_mut(|(kbuf, vbuf)| {
        kbuf.clear();
        encode_key_into(kbuf, key)?;
        vbuf.clear();
        encode_into_buffer(vbuf, value)?;
        let out = f(kbuf, vbuf);
        trim(kbuf);
        trim(vbuf);
        out
    })
}

/// Test-only: the capacities of this thread's encode buffers (key, value).
#[cfg(test)]
pub(super) fn buf_capacities() -> (usize, usize) {
    BUFS.with_borrow(|(kbuf, vbuf)| (kbuf.capacity(), vbuf.capacity()))
}

impl TnDatabase {
    /// Open (creating if absent) the database at `path`, in [`CommitMode::Sync`].
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        Self::open_with(path, TnDbOptions::default())
    }

    /// Open (creating if absent) the database at `path`, committing as `options` say.
    pub fn open_with<P: AsRef<Path>>(path: P, options: TnDbOptions) -> eyre::Result<Self> {
        Self::open_named(path, options, UNNAMED_ENV)
    }

    /// [`Self::open_with`], its metrics labeled `env` (a [`super::SplitDatabase`] names each of
    /// its environments).
    pub(super) fn open_named<P: AsRef<Path>>(
        path: P,
        options: TnDbOptions,
        env: &'static str,
    ) -> eyre::Result<Self> {
        let metrics = TnDbMetrics::new(env);
        let durability = Durability::new(metrics.failed_gauge());
        let core = Arc::new(Core::open(path.as_ref(), durability, options.compaction, metrics)?);
        let committer = Arc::new(Committer::start(Arc::clone(&core))?);
        let rate = options.compaction.bytes_per_sec;
        let compactor = Arc::new(Compactor::start(Arc::clone(&core), rate)?);
        Ok(Self { compactor, core, committer, mode: options.commit })
    }

    fn table<T: Table>(&self) -> eyre::Result<Arc<TableShared>> {
        table::<T>(&self.core)
    }

    /// Write `ops` as one batch and, in `Sync` mode, wait until it is durable.
    fn write(&self, ops: &[BatchOp<'_>]) -> eyre::Result<()> {
        write(&self.core, self.mode, ops)
    }

    /// Block until every write so far is durable, running the sync here if none is in flight: for
    /// a caller with no async runtime to await [`Database::persist`] on (e.g. a migration at open).
    #[cfg(any(test, feature = "tndb"))]
    pub(crate) fn wait_durable_blocking(&self) -> eyre::Result<()> {
        super::commit::wait_durable(&self.core, self.core.durability.last_seq())
    }

    /// Test-only, for a simulated crash (a leaked handle): stop the commit thread at once and
    /// release the database's lock, as a dead process's are.
    #[cfg(test)]
    pub(crate) fn release_locks_for_crash(&self) {
        self.compactor.stop();
        self.committer.abort_for_crash();
        self.core.release_lock_for_crash();
    }

    /// Test-only: stop the commit thread for good (a database left without it).
    #[cfg(test)]
    pub(super) fn stop_commit_thread(&self) {
        self.committer.abort_for_crash();
    }

    /// Test-only: the core.
    #[cfg(test)]
    pub(super) fn core(&self) -> &Arc<Core> {
        &self.core
    }

    /// Test-only: compact now, whatever the log's size, and wait for it: whether it switched, or
    /// why it was abandoned.
    #[cfg(test)]
    pub(super) fn compact_now(&self) -> Result<bool, String> {
        self.core.compact.compact_now()
    }
}

/// Table `T`, which must be open.
fn table<T: Table>(core: &Core) -> eyre::Result<Arc<TableShared>> {
    core.table(T::NAME).ok_or_else(|| eyre::eyre!("tndb: table {} is not open", T::NAME))
}

/// Write `ops` as one batch and wait until it is durable in `Sync` mode, or, in `Group` mode,
/// only when the log holds more unsynced bytes than its cap (backpressure: writes nobody waits on
/// cannot outrun the disk without bound). Either wait may run the sync itself.
fn write(core: &Core, mode: CommitMode, ops: &[BatchOp<'_>]) -> eyre::Result<()> {
    let seq = match core.write(ops)? {
        Some(seq) => seq,
        // Nothing changed, but in `Sync` mode the state the write saw (an earlier batch, perhaps
        // not yet durable) must be durable before it returns.
        None if mode == CommitMode::Sync => core.durability.last_seq(),
        None => return Ok(()),
    };
    if mode == CommitMode::Sync || core.unsynced() > core.durability.max_unsynced() {
        super::commit::wait_durable(core, seq)?;
    }
    Ok(())
}

/// A lazy, key-ordered [`DBIter`] over table `T`'s published snapshot.
fn scan<T: Table>(core: &Arc<Core>, kind: ScanKind) -> DBIter<'static, T> {
    match core.table(T::NAME) {
        Some(table) => {
            let mut scan = table.scan(kind, Arc::clone(core));
            Box::new(std::iter::from_fn(move || {
                scan.next_with(|key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
            }))
        }
        None => Box::new(std::iter::empty()),
    }
}

/// The single `(key, value)` a one-shot scan of table `T` lands on.
fn first_of<T: Table>(core: &Core, kind: ScanKind) -> Option<(T::Key, T::Value)> {
    core.table(T::NAME)?
        .first_with(kind, |key, value| (decode_key::<T::Key>(key), decode::<T::Value>(value)))
}

fn get<T: Table>(core: &Core, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    let table = table::<T>(core)?;
    with_key(key, |key| table.get_with(key, |bytes| decode::<T::Value>(bytes)))
}

fn contains_key<T: Table>(core: &Core, key: &T::Key) -> eyre::Result<bool> {
    let table = table::<T>(core)?;
    with_key(key, |key| table.contains(key))
}

/// A read transaction: reads the tables' published snapshots.
#[derive(Debug)]
pub struct TnDbTx {
    core: Arc<Core>,
}

impl DbTx for TnDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.core, key)
    }
}

/// A write transaction's changes to one table: cleared first (if `cleared`), then its writes in
/// order (a later write of a key replaces an earlier one).
#[derive(Debug)]
struct TxTable {
    shared: Arc<TableShared>,
    cleared: bool,
    writes: Vec<TxWrite>,
}

/// One write of a transaction: its key and value (`None`: a removal) as ranges of the
/// transaction's byte arena.
#[derive(Clone, Debug)]
struct TxWrite {
    key: Range<usize>,
    value: Option<Range<usize>>,
}

/// A write transaction, buffered: its writes stay in memory (its own `get` sees them) and are
/// written as one batch when it commits, or when it is dropped without a commit (as
/// `LayeredDatabase`'s dropped transactions are: kept, never rolled back), unless it is dropped
/// while a panic unwinds: then it was cut off part way, and nothing of it is written. A batch is
/// atomic across tables: after a crash, all of it is there or none. Other readers and transactions
/// see none of it until then.
///
/// Every key and value is encoded into one byte arena (no allocation per write); at commit each
/// table's writes are sorted by key, so they land in key order, and only a key's last write is
/// kept.
#[derive(Debug)]
pub struct TnDbTxMut {
    core: Arc<Core>,
    mode: CommitMode,
    tables: BTreeMap<TableId, TxTable>,
    bytes: Vec<u8>,
    /// Written (or failed to write): nothing more to do on drop.
    done: bool,
}

impl TnDbTxMut {
    fn table_mut(&mut self, shared: Arc<TableShared>) -> &mut TxTable {
        self.tables.entry(shared.id).or_insert_with(|| TxTable {
            shared,
            cleared: false,
            writes: Vec::new(),
        })
    }

    /// The last write of `key` to table `id` in this transaction: `Some(Some(value))` a put,
    /// `Some(None)` a removal (or the table was cleared since), `None` untouched.
    fn last_write(&self, id: TableId, key: &[u8]) -> Option<Option<&[u8]>> {
        let tx = self.tables.get(&id)?;
        match tx.writes.iter().rev().find(|write| &self.bytes[write.key.clone()] == key) {
            Some(write) => Some(write.value.clone().map(|value| &self.bytes[value])),
            None if tx.cleared => Some(None),
            None => None,
        }
    }

    /// Write the buffered changes as one batch (fixing each new table's key size first, outside
    /// the write lock) and, in `Sync` mode, wait until it is durable.
    fn finish(&mut self) -> eyre::Result<()> {
        self.done = true;
        let tables = std::mem::take(&mut self.tables);
        let bytes = std::mem::take(&mut self.bytes);
        let mut ops = Vec::new();
        for (&id, tx) in &tables {
            // The last write of each key, in key order.
            let mut order: Vec<usize> = (0..tx.writes.len()).collect();
            order.sort_by(|&a, &b| {
                bytes[tx.writes[a].key.clone()]
                    .cmp(&bytes[tx.writes[b].key.clone()])
                    .then(a.cmp(&b))
            });
            let mut last: Vec<&TxWrite> = Vec::with_capacity(order.len());
            for i in order {
                let write = &tx.writes[i];
                match last.last_mut() {
                    Some(prev) if bytes[prev.key.clone()] == bytes[write.key.clone()] => {
                        *prev = write
                    }
                    _ => last.push(write),
                }
            }
            if let Some(put) = last.iter().find(|write| write.value.is_some()) {
                self.core.ensure_ksize(&tx.shared, put.key.len())?;
            }
            if tx.cleared {
                ops.push(BatchOp::Clear(id));
            }
            for write in last {
                let key = &bytes[write.key.clone()];
                ops.push(match &write.value {
                    Some(value) => BatchOp::Put(id, key, &bytes[value.clone()]),
                    None => BatchOp::Remove(id, key),
                });
            }
        }
        write(&self.core, self.mode, &ops)
    }
}

impl Drop for TnDbTxMut {
    fn drop(&mut self) {
        if self.done || self.tables.is_empty() {
            return;
        }
        if std::thread::panicking() {
            tracing::error!(
                target: "tndb",
                "a write transaction dropped while panicking: its writes are discarded"
            );
            return;
        }
        tracing::warn!(target: "tndb", "a write transaction dropped without commit; writing it");
        if let Err(e) = self.finish() {
            tracing::error!(target: "tndb", "a dropped write transaction failed to write: {e:#}");
        }
    }
}

impl DbTx for TnDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let table = table::<T>(&self.core)?;
        let encoded = encode_key(key);
        match self.last_write(table.id, &encoded) {
            Some(Some(value)) => Ok(Some(decode::<T::Value>(value))),
            Some(None) => Ok(None),
            None => table.get_with(&encoded, |bytes| decode::<T::Value>(bytes)),
        }
    }
}

impl DbTxMut for TnDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let shared = table::<T>(&self.core)?;
        let start = self.bytes.len();
        encode_key_into(&mut self.bytes, key)?;
        let key_end = self.bytes.len();
        let fixed = shared.ksize();
        if fixed != 0 && key_end - start != fixed as usize {
            self.bytes.truncate(start);
            eyre::bail!(
                "tndb: a {}-byte key in table {} of {fixed}-byte keys",
                key_end - start,
                T::NAME
            );
        }
        if let Err(e) = encode_into_buffer(&mut self.bytes, value) {
            self.bytes.truncate(start);
            return Err(e.into());
        }
        let write = TxWrite { key: start..key_end, value: Some(key_end..self.bytes.len()) };
        self.table_mut(shared).writes.push(write);
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let shared = table::<T>(&self.core)?;
        let start = self.bytes.len();
        encode_key_into(&mut self.bytes, key)?;
        let write = TxWrite { key: start..self.bytes.len(), value: None };
        self.table_mut(shared).writes.push(write);
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        let shared = table::<T>(&self.core)?;
        let tx = self.table_mut(shared);
        tx.cleared = true;
        tx.writes.clear();
        Ok(())
    }

    fn commit(mut self) -> eyre::Result<()> {
        self.finish()
    }
}

impl Database for TnDatabase {
    type TX<'txn>
        = TnDbTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = TnDbTxMut
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.core.open_table(T::NAME).map(drop)
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(TnDbTx { core: Arc::clone(&self.core) })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(TnDbTxMut {
            core: Arc::clone(&self.core),
            mode: self.mode,
            tables: BTreeMap::new(),
            bytes: Vec::new(),
            done: false,
        })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        contains_key::<T>(&self.core, key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.core, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let table = self.table::<T>()?;
        with_key_value(key, value, |key, value| {
            self.core.ensure_ksize(&table, key.len())?;
            self.write(&[BatchOp::Put(table.id, key, value)])
        })
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        let table = self.table::<T>()?;
        with_key(key, |key| self.write(&[BatchOp::Remove(table.id, key)]))
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        let table = self.table::<T>()?;
        self.write(&[BatchOp::Clear(table.id)])
    }

    /// True if the table is open and empty (an absent table reads as not empty).
    fn is_empty<T: Table>(&self) -> bool {
        self.core.table(T::NAME).is_some_and(|table| table.len() == 0)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        scan::<T>(&self.core, ScanKind::Forward)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        Ok(scan::<T>(&self.core, ScanKind::From(encode_key(key))))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        scan::<T>(&self.core, ScanKind::Reverse)
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        first_of::<T>(&self.core, ScanKind::RevFrom(encode_key(key)))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        first_of::<T>(&self.core, ScanKind::Reverse)
    }

    /// Wait until every write made before the call (in any table) is durable; an error once any
    /// write or sync has failed. In [`CommitMode::Sync`] every write was durable when it returned.
    fn persist<T: Table>(&self) -> impl std::future::Future<Output = eyre::Result<()>> + Send {
        let durability = Arc::clone(&self.core.durability);
        let mode = self.mode;
        // The writes so far, as of the call (not the first poll).
        let target = durability.last_seq();
        async move {
            match mode {
                CommitMode::Group => durability.persist(target).await,
                CommitMode::Sync => durability.check(),
            }
        }
    }

    /// Ask the compaction thread to compact the log now (in the background), unless it is
    /// under 1 MiB.
    fn compact(&self) -> eyre::Result<()> {
        self.core.compact.request();
        Ok(())
    }
}
