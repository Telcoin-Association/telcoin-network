//! A single tndb table — its logs plus a sorted [`BtreeIndex`] over them — as a cheap `Clone`
//! [`TnTable`] handle.  This file is byte-oriented (byte-slice keys and values in, borrowed bytes
//! out); the typed `encode`/`decode` stays in `database.rs`.
//!
//! Readers take no lock and write no shared memory. Writers serialize on a writer lock and change a
//! copy-on-write working tree (a published index page is never modified); [`TnTable::flush`] makes
//! the logs durable and then installs a new [`Published`] snapshot — the index snapshot plus the
//! published extent of the data log — in an `ArcSwap`. A reader loads the current snapshot through
//! a per-thread slot, looks the key up in its immutable pages and reads the value straight from the
//! log, which never changes once appended. Both files keep one mapping that does not move (a
//! reserved range; see [`MmapFileOptions::reserve`]), and a mapping replaced on the rare
//! reservation overflow stays mapped until the file closes, so a snapshot's pages stay readable.
//!
//! Writes are visible to readers from the next flush (snapshot isolation). The writer's own
//! uncommitted writes are read through [`TnTable::get_working_with`].
//!
//! A scan ([`TableScan`]) owns a snapshot and a [`BtreeCursor`] over it and holds no lock, so a
//! table can be written (and published) while it is being scanned, from any thread; the scan keeps
//! reading the snapshot it started with. Index pages a live snapshot can reach are not reused, so
//! a long-lived scan lets the index file grow until it is dropped.
//!
//! ## Logs and recovery
//!
//! The data log is the table's write-ahead log (see [`super::layout`] for the files):
//! - a put appends `[key | value]`, or just `[value]` in a derived-key table, whose key function
//!   recomputes the key from the value;
//! - a remove appends `[key | data log length]` to the separate removal log: every put of that key
//!   below that length is gone;
//! - a flush appends an empty record to the data log, the commit point, after syncing the removal
//!   log, and then syncs the data log.
//!
//! The index is derived: it is synced only by a clean close. Opening a table whose files were not
//! closed cleanly (or whose index is unreadable, or behind its log) rebuilds the index by replaying
//! the logs up to the last commit point, so a crash keeps every committed write and no part of an
//! uncommitted one (see [`recover`]). Recovery fails closed: it changes nothing until both logs
//! have been read and checked, a tear in a sealed log or below a log's commit marker is an error,
//! and a failed recovery leaves the logs as they were. Each write is all or nothing: a put or
//! remove whose index step fails takes its log record back out.
//!
//! One writer per table: an open holds the table's `LOCK` (`flock`) for the writer's life, so a
//! second open of the same table (this process or another) fails. A clear that fails after the
//! next open would pick its new generation stops the writer: later writes and commits fail until a
//! restart, rather than commit to a generation the restart discards.
//!
//! Clearing a table starts a new, empty generation and deletes the old one's files, so cleared
//! data leaves the disk. Snapshots of the old generation keep its files open (its mappings valid)
//! until the last of them drops. The new generation is a spare a background thread prepared (its
//! files created and synced) after the table opened or was last cleared, so a clear only renames it
//! into place and syncs the table directory.
//!
//! Compaction rewrites a table's live rows into its next generation on a background thread and
//! switches to it at a commit, after which the removed and overwritten records are gone from disk
//! (see [`compact`]). It starts on its own once a table's log is large and mostly dead puts
//! ([`CompactionConfig`]), or on request.

mod compact;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use eyre::{bail, WrapErr as _};
use parking_lot::Mutex;

pub use compact::CompactionConfig;
use compact::{
    replay_delta, Committed, Compacted, Compaction, Job, AUTO_BACKOFF_MAX, AUTO_BACKOFF_MIN,
    DISK_HEADROOM, REQUESTED_MIN_BYTES,
};

use super::{
    commit::GroupCommit,
    layout::{
        available_bytes, compact_dir, gen_dir, list_gens, lock_table, remove_meta_tmp,
        remove_spares, spare_dir, sync_dir, KeyMode, TableMeta,
    },
};
use crate::archive::{
    btree_index::{
        index::IndexSnapshot,
        iter::{BtreeCursor, PageSource},
        BtreeIndex,
    },
    data_file::{MapView, MmapFileOptions, SyncTicket},
    error::{fetch::FetchError, load_header::LoadHeaderError},
    pack::{Pack, PackCompression, DATA_HEADER_BYTES},
};

/// Pack/index format version for tndb tables (matches `tndb::database`).
const PACK_VERSION: u16 = 1;

/// Address space reserved for a table's value log mapping (see [`MmapFileOptions::reserve`]):
/// virtual only, sized so a table's log never outgrows it in practice.
const TNDB_MAP_RESERVE: u64 = crate::archive::btree_index::index::BTX_MAP_RESERVE;

/// First size of a removal log: removals are few, and the log is only read on a rebuild.
const REMOVED_INITIAL_SIZE: u64 = 64 << 10;

/// The data log's commit record: an empty payload (no put record is empty).
const COMMIT: &[u8] = &[];

/// Derives a derived-key table's encoded key from an encoded value.
pub(crate) type KeyFn = Arc<dyn Fn(&[u8]) -> eyre::Result<Vec<u8>> + Send + Sync>;

/// Which ordered scan to run over a table's index.
pub(crate) enum ScanKind {
    /// Every entry in ascending key order.
    Forward,
    /// Every entry in descending key order.
    Reverse,
    /// Ascending from the given key bytes (inclusive) to the end.
    From(Vec<u8>),
    /// Descending over the entries whose keys are strictly less than the given key bytes.
    RevFrom(Vec<u8>),
}

impl ScanKind {
    /// A cursor positioned for this scan over the tree `src` reads.
    fn cursor<S: PageSource + ?Sized>(self, src: &S) -> Result<BtreeCursor, FetchError> {
        let (reverse, lower, upper) = match self {
            Self::Forward => (false, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Self::Reverse => (true, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Self::From(key) => (false, std::ops::Bound::Included(key), std::ops::Bound::Unbounded),
            Self::RevFrom(key) => {
                (true, std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key))
            }
        };
        BtreeCursor::new(src, reverse, lower, upper)
    }
}

/// The value bytes of a data record: past the key in a keyed table (`value_offset` = key size),
/// the whole record in a derived-key one (0).
fn record_value(record: &[u8], value_offset: usize) -> Result<&[u8], FetchError> {
    record
        .get(value_offset..)
        .ok_or_else(|| FetchError::CorruptIndex("a data record is shorter than its key".into()))
}

/// A byte log: the data log or the removal log.
type Log = Pack<Vec<u8>>;

/// One generation's open files.
#[derive(Debug)]
struct GenFiles {
    data: Log,
    removed: Log,
    idx: Option<BtreeIndex>,
}

/// Keeps a retired (cleared or compacted away) generation's files open, and so its mappings
/// valid, while snapshots of it are alive: each [`Published`] holds its generation's `GenAlive`,
/// and retiring the generation moves its files into it, so they close when the last old snapshot
/// (or compaction thread) drops.
#[derive(Debug, Default)]
struct GenAlive {
    retired: Mutex<Option<GenFiles>>,
}

impl Drop for GenAlive {
    /// Close a retired generation's files (unmapping them) on a short-lived thread, off the path
    /// of whichever writer or reader dropped the last snapshot; if no thread can be started, here.
    fn drop(&mut self) {
        if let Some(files) = self.retired.get_mut().take() {
            let _ = std::thread::Builder::new().name("tndb-reap".to_string()).spawn(move || {
                drop(files);
            });
        }
    }
}

/// Delete a retired generation's directory on a short-lived thread (its files are unlinked now;
/// their space returns once no mapping of them is left), or here if no thread can be started. A
/// failure leaves it for the next open to remove.
fn remove_in_background(dir: PathBuf) {
    let remove = |dir: &Path| {
        if let Err(e) = fs::remove_dir_all(dir) {
            tracing::warn!(target: "tndb", "remove retired generation {}: {e}", dir.display());
        }
    };
    let thread_dir = dir.clone();
    let spawned = std::thread::Builder::new()
        .name("tndb-reap".to_string())
        .spawn(move || remove(&thread_dir));
    if spawned.is_err() {
        remove(&dir);
    }
}

/// Open (creating if absent) a generation's data and removal logs.
fn open_logs(dir: &Path) -> eyre::Result<(Log, Log)> {
    // The data log keeps one reserved mapping for its whole open life (it never moves on growth),
    // as the B-tree index does, so lock-free readers can read it.
    let data_opts = MmapFileOptions { reserve: TNDB_MAP_RESERVE, ..Default::default() };
    let data = Pack::open_with(
        dir.join("data"),
        0,
        false,
        PackCompression::None,
        PACK_VERSION,
        data_opts,
    )?;
    let removed_opts = MmapFileOptions { initial_size: REMOVED_INITIAL_SIZE, ..Default::default() };
    let removed = Pack::open_with(
        dir.join("removed"),
        0,
        false,
        PackCompression::None,
        PACK_VERSION,
        removed_opts,
    )?;
    Ok((data, removed))
}

/// Create generation `n` of `table`: its directory and empty logs, durable before it is used (the
/// log headers, the generation's entries, and the generation itself).
fn create_gen(table: &Path, n: u64) -> eyre::Result<(Log, Log)> {
    let dir = gen_dir(table, n);
    fs::create_dir(&dir).wrap_err_with(|| format!("tndb: create {}", dir.display()))?;
    let (data, removed) = open_logs(&dir)?;
    data.commit()?;
    removed.commit()?;
    sync_dir(&dir)?;
    sync_dir(table)?;
    Ok((data, removed))
}

/// Prepare a spare generation in `dir` (run on a background thread): an empty generation's logs,
/// and its index when the key size is known, created and synced. A spare directory is never
/// mistaken for a generation (only `gen-<N>` directories are). First deletes `cleared`, the
/// directory of the generation the last clear replaced, if any.
fn prepare_spare(dir: &Path, ksize: u16, cleared: Option<&Path>) -> eyre::Result<GenFiles> {
    if let Some(cleared) = cleared {
        // Unlinked now (its space returns once no mapping of it is left); a failure leaves it for
        // the next open to remove.
        if let Err(e) = fs::remove_dir_all(cleared) {
            tracing::warn!(target: "tndb", "remove cleared generation {}: {e}", cleared.display());
        }
    }
    fs::create_dir(dir).wrap_err_with(|| format!("tndb: create {}", dir.display()))?;
    let (data, removed) = open_logs(dir)?;
    data.commit()?;
    removed.commit()?;
    let idx = match ksize {
        0 => None,
        ksize => Some(BtreeIndex::open_btx_file(dir.join("btx"), data.header(), ksize, false)?),
    };
    sync_dir(dir)?;
    Ok(GenFiles { data, removed, idx })
}

/// What replaying a generation's logs found: the live rows and where each log's committed records
/// end.
struct Replay {
    rows: BTreeMap<Vec<u8>, u64>,
    /// Committed puts, live or not.
    puts: u64,
    data_end: u64,
    removed_end: u64,
}

/// One data record, read during a replay.
enum DataRecord {
    Commit,
    Put(Vec<u8>),
}

/// Replay a generation's logs (read through cloned file handles; nothing is changed): the rows
/// the committed records leave, and the end of each log's committed records. Every check happens
/// here, before recovery changes anything:
/// - a put becomes live only at the commit record after it, and a removal applies only when it was
///   made before the last commit (its data length is at most that commit record's position), so a
///   crash keeps no part of an uncommitted flush;
/// - a sealed (cleanly closed) log must read whole (no tear); any log may end in uncommitted
///   records (a close that could not commit), which are dropped; an unclean log may also end in a
///   tear, but not below its commit marker (that is damage to committed data, not a crash tail);
/// - a malformed record, or a key function failure, is an error.
fn replay(files: &GenFiles, meta: TableMeta, key_fn: Option<&KeyFn>) -> eyre::Result<Replay> {
    let ksize = meta.ksize as usize;
    let header = DATA_HEADER_BYTES as u64;

    // The data log: puts, made live by the commit record after them.
    let mut rows = BTreeMap::new();
    let mut puts = 0;
    let mut pending: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut last_commit = None;
    let mut data_end = header;
    let mut torn = false;
    let mut iter = files.data.raw_iter()?;
    loop {
        let start = iter.logical_position();
        let record = match iter.next_raw() {
            None => break,
            Some(Err(_)) => {
                torn = true;
                break;
            }
            Some(Ok([])) => DataRecord::Commit,
            Some(Ok(payload)) => DataRecord::Put(match meta.mode {
                KeyMode::Keyed if ksize > 0 && payload.len() >= ksize => payload[..ksize].to_vec(),
                KeyMode::Keyed => bail!("tndb: a data record at {start} is shorter than its key"),
                KeyMode::Derived => {
                    let key_fn = key_fn.ok_or_else(|| eyre::eyre!("tndb: no key function"))?;
                    let key = key_fn(payload).wrap_err_with(|| {
                        format!("tndb: derive the key of the record at {start}")
                    })?;
                    if key.len() != ksize {
                        bail!("tndb: the derived key at {start} is not {ksize} bytes");
                    }
                    key
                }
            }),
        };
        match record {
            DataRecord::Put(key) => pending.push((key, start)),
            DataRecord::Commit => {
                puts += pending.len() as u64;
                rows.extend(pending.drain(..));
                last_commit = Some(start);
                data_end = iter.logical_position();
            }
        }
    }
    check_log_end("data", &files.data, torn, data_end)?;

    // The removal log: committed removals, in order (their data lengths never decrease).
    let mut removed_end = header;
    let mut torn = false;
    let mut uncommitted = false;
    let mut iter = files.removed.raw_iter()?;
    loop {
        let start = iter.logical_position();
        let (key, data_len) = match iter.next_raw() {
            None => break,
            Some(Err(_)) => {
                torn = true;
                break;
            }
            Some(Ok(payload)) if payload.len() == ksize + 8 && ksize > 0 => {
                let data_len = u64::from_le_bytes(payload[ksize..].try_into().expect("8 bytes"));
                (payload[..ksize].to_vec(), data_len)
            }
            Some(Ok(_)) => bail!("tndb: a removal record at {start} is malformed"),
        };
        if last_commit.is_some_and(|commit| data_len <= commit) {
            if uncommitted {
                bail!("tndb: the removal log is out of order at {start}");
            }
            if rows.get(&key).is_some_and(|&pos| pos < data_len) {
                rows.remove(&key);
            }
            removed_end = iter.logical_position();
        } else {
            uncommitted = true;
        }
    }
    check_log_end("removal", &files.removed, torn, removed_end)?;

    Ok(Replay { rows, puts, data_end, removed_end })
}

/// Fail closed on a log whose replay hit a bad frame (`torn`) when that cannot be a crash tail:
/// the log was sealed by a clean close; or whose committed records stop below its commit marker.
fn check_log_end(name: &str, log: &Log, torn: bool, end: u64) -> eyre::Result<()> {
    if torn && !log.opened_unclean() {
        bail!("tndb: the {name} log was closed cleanly but does not replay to its end (corrupt)");
    }
    if log.opened_unclean() && log.committed_end().is_some_and(|committed| end < committed) {
        bail!("tndb: the {name} log's committed records end at {end}, below its commit marker");
    }
    Ok(())
}

/// The writer's state: the current generation's files and the working (copy-on-write) index,
/// changed only under the writer lock.
struct Writer {
    /// Table directory (`meta` and the generations).
    table_dir: PathBuf,
    meta: TableMeta,
    /// A derived-key table's key function.
    key_fn: Option<KeyFn>,
    /// The current generation's number, directory, files and keepalive.
    gen: u64,
    gen_dir: PathBuf,
    files: GenFiles,
    data_view: Arc<MapView>,
    alive: Arc<GenAlive>,
    /// The next generation, being prepared in the background in the given directory (see
    /// [`prepare_spare`]).
    spare: Option<(PathBuf, std::thread::JoinHandle<eyre::Result<GenFiles>>)>,
    /// Records appended since the last commit record.
    uncommitted: bool,
    /// Removal records appended since the removal log was last synced.
    removals_unsynced: bool,
    /// Group-commit mode (see `super::commit`): the sequence number of the last write published
    /// here, the first one not yet durable (if any), and the open write transactions that wrote
    /// this table (the committer leaves the table alone while any is open, so a transaction never
    /// becomes durable in part).
    applied_seq: u64,
    first_pending: Option<u64>,
    open_txns: u32,
    /// Test-only: the committer's next round on this table reports reaching the point between its
    /// unlocked removal-log sync and its commit record, then waits to be released.
    #[cfg(test)]
    commit_gate: Option<(CommitPoint, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    /// When and how fast this table compacts.
    config: CompactionConfig,
    /// Puts in the current generation made dead (overwritten or removed): the automatic
    /// compaction's measure of garbage. Kept in the index header across a clean close
    /// ([`BtreeIndex::set_owner_value`]), recounted by a rebuild.
    dead: u64,
    /// The running compaction, if any.
    compaction: Option<Compaction>,
    /// After a failed automatic compaction, when the automatic trigger may try again, and how long
    /// it waits after the next failure (see [`AUTO_BACKOFF_MIN`]).
    auto_retry_at: Option<Instant>,
    auto_backoff: Duration,
    /// Compactions cancelled by a clear, still stopping: joined (and their results discarded)
    /// before the next compaction starts or the table closes.
    cancelled: Vec<std::thread::JoinHandle<eyre::Result<Compacted>>>,
    /// Test-only: the next compaction's thread waits at each checkpoint for this channel, and
    /// reports reaching each one on the other.
    #[cfg(test)]
    compact_gate: Option<std::sync::mpsc::Receiver<()>>,
    #[cfg(test)]
    compact_arrived: Option<std::sync::mpsc::Sender<u32>>,
    /// Test-only: every compaction thread fails before it builds anything.
    #[cfg(test)]
    fail_compactions: bool,
    /// Test-only: compactions started, and the bytes the last switch replayed under the lock.
    #[cfg(test)]
    compactions_started: u32,
    #[cfg(test)]
    last_switch_tail: u64,
    /// Test-only: the free disk space the compaction guard sees.
    #[cfg(test)]
    free_space_for_test: Option<u64>,
    /// Test-only: the next compaction switch's directory sync after its rename fails.
    #[cfg(test)]
    fail_next_switch_sync: bool,
    /// Set when this open rebuilt the index (for tests).
    #[cfg(test)]
    rebuilt: bool,
    /// Clears that renamed a prepared spare into place (for tests).
    #[cfg(test)]
    clears_from_spare: u32,
    /// Test-only: the next clear's directory sync after its rename fails.
    #[cfg(test)]
    fail_next_clear_sync: bool,
    /// Test-only: the next commit fails before writing anything.
    #[cfg(test)]
    fail_next_flush: bool,
    /// Set when a clear or a compaction switch failed after it may have changed which generation
    /// the next open picks: writing on here would commit to a generation a restart discards, so
    /// every later write and commit fails (restart to recover).
    failed: Option<String>,
    /// The table's `flock` (see [`lock_table`]), released when the writer drops. Declared last so
    /// it is released after the files close. `None` only after a test's simulated crash.
    _lock: Option<fs::File>,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("table_dir", &self.table_dir)
            .field("gen", &self.gen)
            .finish()
    }
}

impl Writer {
    /// Bytes before a data record's value: its key in a keyed table, nothing in a derived one.
    fn value_offset(&self) -> usize {
        match self.meta.mode {
            KeyMode::Keyed => self.meta.ksize as usize,
            KeyMode::Derived => 0,
        }
    }

    /// The index, created on first use.
    fn index_mut(&mut self) -> eyre::Result<&mut BtreeIndex> {
        if self.files.idx.is_none() {
            let idx = BtreeIndex::open_btx_file(
                self.gen_dir.join("btx"),
                self.files.data.header(),
                self.meta.ksize,
                false,
            )?;
            self.files.idx = Some(idx);
        }
        Ok(self.files.idx.as_mut().expect("index just created"))
    }

    /// Check `key`'s size against the table's, recording it (once, durably) on the first write.
    fn check_ksize(&mut self, key: &[u8]) -> eyre::Result<()> {
        if key.len() == self.meta.ksize as usize {
            return Ok(());
        }
        if self.meta.ksize != 0 || key.is_empty() || key.len() > u16::MAX as usize {
            bail!("tndb: a {}-byte key in a table of {}-byte keys", key.len(), self.meta.ksize);
        }
        let meta = TableMeta { ksize: key.len() as u16, ..self.meta };
        meta.write(&self.table_dir)?;
        self.meta = meta;
        Ok(())
    }

    /// Refuse writes once a failed clear or compaction switch stopped this writer (see `failed`).
    fn check_failed(&self) -> eyre::Result<()> {
        match &self.failed {
            Some(cause) => bail!(
                "tndb: table {} stopped after a failed generation change ({cause}); restart to \
                 recover",
                self.table_dir.display()
            ),
            None => Ok(()),
        }
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
        self.check_failed()?;
        self.check_ksize(key)?;
        let pos = match &self.key_fn {
            None => self.files.data.append_raw_parts(&[key, value])?,
            Some(key_fn) => {
                if value.is_empty() {
                    bail!("tndb: a derived-key table cannot store an empty value");
                }
                debug_assert_eq!(
                    key_fn(value).ok().as_deref(),
                    Some(key),
                    "tndb: a derived-key table's key must be the key derived from its value"
                );
                self.files.data.append_raw(value)?
            }
        };
        // All or nothing: a failed index step takes its record back out of the log, or a later
        // commit would make durable a put the index (and the caller) never had.
        let saved = self.index_mut().and_then(|idx| {
            let live = idx.len();
            idx.save(key, pos)?;
            Ok(idx.len() == live)
        });
        match saved {
            // An overwrite leaves the row count as it was, and the old put dead.
            Ok(overwrote) => self.dead += overwrote as u64,
            Err(e) => {
                self.files.data.rewind_to(pos);
                return Err(e);
            }
        }
        self.uncommitted = true;
        Ok(())
    }

    /// Read `key` from the working tree (including writes not yet published).
    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
        let pos = match self.files.idx.as_ref() {
            Some(idx) => match idx.load(key) {
                Ok(pos) => pos,
                Err(FetchError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            None => return Ok(None),
        };
        let record = self.files.data.record_bytes(pos)?;
        Ok(Some(decode(record_value(record, self.value_offset())?)))
    }

    /// Remove `key`, logging the removal first. The record is taken back out when the key is
    /// absent (only a present key stays logged) or the index step fails (all or nothing).
    fn remove(&mut self, key: &[u8]) -> eyre::Result<bool> {
        self.check_failed()?;
        let Some(idx) = self.files.idx.as_mut() else { return Ok(false) };
        let record = self.files.removed.file_len();
        let data_len = self.files.data.file_len().to_le_bytes();
        self.files.removed.append_raw_parts(&[key, &data_len])?;
        match idx.remove(key) {
            Ok(true) => {
                self.removals_unsynced = true;
                self.uncommitted = true;
                self.dead += 1;
                Ok(true)
            }
            Ok(false) => {
                self.files.removed.rewind_to(record);
                Ok(false)
            }
            Err(e) => {
                self.files.removed.rewind_to(record);
                Err(e.into())
            }
        }
    }

    /// Start preparing the spare for the next generation on a background thread, after deleting
    /// `cleared` (the generation a clear just replaced). Without a thread, the deletion happens
    /// here and the next clear creates its generation itself.
    fn prepare_next_spare(&mut self, cleared: Option<PathBuf>) {
        let dir = spare_dir(&self.table_dir, self.gen + 1);
        let (thread_dir, ksize) = (dir.clone(), self.meta.ksize);
        let thread_cleared = cleared.clone();
        self.spare = std::thread::Builder::new()
            .name("tndb-spare".to_string())
            .spawn(move || prepare_spare(&thread_dir, ksize, thread_cleared.as_deref()))
            .map_err(|e| {
                tracing::warn!(target: "tndb", "spawn the spare generation thread: {e}");
                if let Some(cleared) = &cleared {
                    let _ = fs::remove_dir_all(cleared);
                }
            })
            .ok()
            .map(|handle| (dir, handle));
    }

    /// The prepared spare and its directory, waited for if still being prepared, or `None` if
    /// there is none or it failed (logged; its directory is removed).
    fn take_spare(&mut self) -> Option<(PathBuf, GenFiles)> {
        let (dir, handle) = self.spare.take()?;
        let result = handle
            .join()
            .unwrap_or_else(|_| Err(eyre::eyre!("tndb: the spare generation thread panicked")));
        match result {
            Ok(files) => Some((dir, files)),
            Err(e) => {
                tracing::warn!(target: "tndb", "spare generation unavailable: {e}");
                let _ = fs::remove_dir_all(&dir);
                None
            }
        }
    }

    /// Start a new, empty generation and delete the current one's files (see the module docs).
    fn clear(&mut self) -> eyre::Result<()> {
        self.check_failed()?;
        // A compaction of the generation being cleared is moot.
        self.cancel_compaction();
        let next = self.gen + 1;
        let next_dir = gen_dir(&self.table_dir, next);
        let files = match self.take_spare() {
            // Its files are already synced: renaming it into place, durably, is the clear.
            Some((dir, files)) => {
                fs::rename(&dir, &next_dir)?;
                // From here the next open picks the new generation, so a failure stops this
                // writer rather than let it commit to the generation a restart discards.
                #[cfg(test)]
                if std::mem::take(&mut self.fail_next_clear_sync) {
                    return Err(self.stop("injected directory sync failure".into()));
                }
                if let Err(e) = sync_dir(&self.table_dir) {
                    return Err(self.stop(format!("sync the renamed generation: {e}")));
                }
                #[cfg(test)]
                {
                    self.clears_from_spare += 1;
                }
                files
            }
            None => match create_gen(&self.table_dir, next) {
                Ok((data, removed)) => GenFiles { data, removed, idx: None },
                // A partly created generation the next open would pick stops this writer too.
                Err(e) if next_dir.exists() => {
                    return Err(self.stop(format!("create the new generation: {e}")));
                }
                Err(e) => return Err(e),
            },
        };
        let old_dir = self.install_gen(next, files);
        self.dead = 0;
        // The old generation's directory is deleted, and the next spare prepared, in the
        // background.
        self.prepare_next_spare(Some(old_dir));
        Ok(())
    }

    /// Make `files` (generation `next`, already in place on disk) the current generation and
    /// retire the old one: its files are deleted, not sealed, once the last snapshot of them
    /// drops. Returns the old generation's directory, for the caller to delete.
    fn install_gen(&mut self, next: u64, files: GenFiles) -> PathBuf {
        let old_dir = std::mem::replace(&mut self.gen_dir, gen_dir(&self.table_dir, next));
        let mut old = std::mem::replace(&mut self.files, files);
        let old_alive = std::mem::take(&mut self.alive);
        self.data_view = self.files.data.view();
        self.gen = next;
        self.uncommitted = false;
        self.removals_unsynced = false;
        old.data.set_remove_on_drop();
        old.removed.set_remove_on_drop();
        if let Some(idx) = old.idx.as_mut() {
            idx.set_remove_on_drop();
        }
        *old_alive.retired.lock() = Some(old);
        old_dir
    }

    /// Stop this writer (see `failed`) and return the error saying why.
    fn stop(&mut self, cause: String) -> eyre::Report {
        tracing::error!(
            target: "tndb",
            "table {}: {cause}; writes stop until a restart",
            self.table_dir.display()
        );
        let report = eyre::eyre!("tndb: {cause}");
        self.failed = Some(cause);
        report
    }

    /// Commit: sync the removal log (if it changed), append the commit record, and sync the data
    /// log. A flush with nothing to commit does nothing.
    fn flush(&mut self) -> eyre::Result<()> {
        self.check_failed()?;
        if !self.uncommitted {
            return Ok(());
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_flush) {
            eyre::bail!("injected commit failure");
        }
        // The removals are durable before the commit record that makes them count.
        let removals = self.removals_unsynced;
        if removals {
            self.files.removed.commit()?;
        }
        self.files.data.append_raw(COMMIT)?;
        self.files.data.commit()?;
        // The commit markers (best-effort floors for recovery) move only once the commit is
        // durable, so neither ever claims an uncommitted record.
        self.files.data.stamp_commit_marker();
        if removals {
            self.files.removed.stamp_commit_marker();
        }
        self.removals_unsynced = false;
        self.uncommitted = false;
        let data_len = self.files.data.file_len();
        if let Some(idx) = self.files.idx.as_mut() {
            idx.set_data_file_length(data_len);
        }
        Ok(())
    }

    /// Make every write so far readable: publish the index (its new pages become immutable) and
    /// the data log's current extent.
    fn publish(&mut self) -> Published {
        let index = self.files.idx.as_mut().map(BtreeIndex::publish);
        let committed =
            Committed { data: self.files.data.file_len(), removed: self.files.removed.file_len() };
        self.data_view.publish_len(committed.data);
        if let Some(compaction) = &self.compaction {
            compaction.publish(committed);
        }
        Published {
            index,
            data_view: Arc::clone(&self.data_view),
            value_offset: self.value_offset(),
            gen: self.gen,
            committed,
            alive: Arc::clone(&self.alive),
        }
    }

    /// True when the automatic compaction should start (see [`CompactionConfig`]): none is
    /// running, the data log is large enough, and at least half the puts are dead. O(1), checked
    /// at every commit.
    fn wants_compaction(&self) -> bool {
        self.compaction.is_none()
            && self.config.auto_min_bytes.is_some_and(|min| self.files.data.file_len() >= min)
            && self.dead > 0
            && self.dead >= self.files.idx.as_ref().map_or(0, BtreeIndex::len) as u64
            && self.auto_retry_at.is_none_or(|at| Instant::now() >= at)
    }

    /// Hold the automatic trigger off after a failure, doubling the wait each time (a persistent
    /// cause would otherwise restart a full compaction at every commit).
    fn back_off(&mut self) {
        self.auto_retry_at = Some(Instant::now() + self.auto_backoff);
        self.auto_backoff = (self.auto_backoff * 2).min(AUTO_BACKOFF_MAX);
    }

    /// Whether the filesystem has room for a compaction's new generation beside the current one:
    /// the data log's length (the most the live rows can take) plus [`DISK_HEADROOM`]. Logged when
    /// not; a probe that fails does not block the compaction.
    fn has_room_to_compact(&self) -> bool {
        #[cfg(test)]
        let available =
            self.free_space_for_test.map_or_else(|| available_bytes(&self.table_dir), Ok);
        #[cfg(not(test))]
        let available = available_bytes(&self.table_dir);
        let needed = self.files.data.file_len() + DISK_HEADROOM;
        match available {
            Ok(available) if available < needed => {
                tracing::warn!(
                    target: "tndb",
                    "table {}: compaction skipped: {available} bytes free, {needed} needed",
                    self.table_dir.display()
                );
                false
            }
            Ok(_) => true,
            Err(e) => {
                tracing::debug!(target: "tndb", "free space unknown ({e}); compacting anyway");
                true
            }
        }
    }

    /// Start compacting the current generation from `snapshot`, the last publish (see
    /// [`compact`]), unless one is already running. Returns whether a compaction is running: not
    /// when the writer stopped, the snapshot is of an older generation (a clear not yet
    /// published), or no thread could be started.
    fn start_compaction(&mut self, snapshot: Arc<Published>) -> bool {
        if self.compaction.is_some() {
            return true;
        }
        if self.failed.is_some() || snapshot.gen != self.gen || !self.has_room_to_compact() {
            return false;
        }
        self.reap_cancelled(false);
        let job = Job {
            dir: compact_dir(&self.table_dir, self.gen + 1),
            alive: Arc::clone(&snapshot.alive),
            data_view: Arc::clone(&snapshot.data_view),
            removed_view: self.files.removed.view(),
            snapshot,
            meta: self.meta,
            key_fn: self.key_fn.clone(),
            rate: self.config.bytes_per_sec,
            #[cfg(test)]
            gate: self.compact_gate.take(),
            #[cfg(test)]
            arrived: self.compact_arrived.take(),
            #[cfg(test)]
            fail_at_start: self.fail_compactions,
        };
        self.compaction = Compaction::start(self.gen, job);
        #[cfg(test)]
        {
            self.compactions_started += u32::from(self.compaction.is_some());
        }
        self.compaction.is_some()
    }

    /// Right after a commit (nothing is uncommitted): if the compaction thread has finished,
    /// catch its generation up on the commits since its last round and switch to it. A compaction
    /// that failed is dropped (logged; its files are deleted) and the table carries on as it was.
    /// Returns whether the table switched; an error means the switch failed after its rename,
    /// which stops the writer (see `failed`).
    fn switch_if_compacted(&mut self) -> eyre::Result<bool> {
        if !self.compaction.as_ref().is_some_and(Compaction::is_finished) {
            return Ok(false);
        }
        let compaction = self.compaction.take().expect("checked above");
        debug_assert_eq!(compaction.gen, self.gen, "a clear cancels its generation's compaction");
        let next = self.gen + 1;
        let next_dir = gen_dir(&self.table_dir, next);
        let renamed = compaction
            .join()
            .and_then(|compacted| {
                #[cfg(test)]
                {
                    let (data, removed) = (&self.files.data, &self.files.removed);
                    self.last_switch_tail = (data.file_len() - compacted.done.data)
                        + (removed.file_len() - compacted.done.removed);
                }
                self.catch_up(compacted)
            })
            .and_then(|builder| Ok(fs::rename(builder.dir(), &next_dir).map(|()| builder)?));
        let builder = match renamed {
            Ok(builder) => builder,
            Err(e) => {
                let table = self.table_dir.display();
                if compact::is_corruption(&e) {
                    tracing::error!(target: "tndb", "table {table}: compaction abandoned: {e:#}");
                } else {
                    tracing::warn!(target: "tndb", "table {table}: compaction abandoned: {e:#}");
                }
                self.back_off();
                return Ok(false);
            }
        };
        let (files, dead) = builder.finish();
        // From here the next open picks the new generation, which holds every committed row (as
        // the old one does); a failure stops this writer rather than let it commit to the old one.
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_switch_sync) {
            return Err(self.stop("injected directory sync failure".into()));
        }
        if let Err(e) = sync_dir(&self.table_dir) {
            return Err(self.stop(format!("sync the compacted generation: {e}")));
        }
        let old_dir = self.install_gen(next, files);
        self.dead = dead;
        (self.auto_retry_at, self.auto_backoff) = (None, AUTO_BACKOFF_MIN);
        remove_in_background(old_dir);
        Ok(true)
    }

    /// The compaction's final catch-up, under the writer lock: replay what was committed since
    /// the thread's last round (all of it: this runs right after a commit), then commit and sync
    /// it. The new generation must have exactly the table's rows.
    fn catch_up(&self, compacted: Compacted) -> eyre::Result<compact::Builder> {
        let Compacted { mut builder, done } = compacted;
        let (data, removed) = (&self.files.data, &self.files.removed);
        let pending = (done.data..data.file_len(), done.removed..removed.file_len());
        replay_delta(&mut builder, data, pending.0, removed, pending.1)?;
        builder.commit_record()?;
        builder.sync()?;
        let live = self.files.idx.as_ref().map_or(0, BtreeIndex::len);
        if builder.len() != live {
            bail!("tndb: the compacted generation has {} rows, the table {live}", builder.len());
        }
        Ok(builder)
    }

    /// Stop the running compaction, if any (a clear makes it moot). One that has already finished
    /// is discarded at once (its complete copy must not outlive the clear); a running one stops at
    /// its next check and is joined later.
    fn cancel_compaction(&mut self) {
        if let Some(compaction) = self.compaction.take() {
            let finished = compaction.is_finished();
            let handle = compaction.cancel();
            if finished {
                compact::discard(handle);
            } else {
                self.cancelled.push(handle);
            }
        }
    }

    /// Discard the cancelled compactions whose threads have stopped (in the background; see
    /// [`compact::discard`]), or with `wait` (a close) join every one here.
    fn reap_cancelled(&mut self, wait: bool) {
        for handle in std::mem::take(&mut self.cancelled) {
            if wait {
                drop(compact::join(handle));
            } else if handle.is_finished() {
                compact::discard(handle);
            } else {
                self.cancelled.push(handle);
            }
        }
    }

    /// Rebuild the index from the logs (see [`recover`]).
    fn recover(&mut self) -> eyre::Result<()> {
        let replay = replay(&self.files, self.meta, self.key_fn.as_ref())?;
        self.dead = replay.puts.saturating_sub(replay.rows.len() as u64);
        // Every check passed: now cut the logs back to their committed records.
        if replay.data_end < self.files.data.file_len() {
            self.files.data.rewind_to(replay.data_end);
        }
        if replay.removed_end < self.files.removed.file_len() {
            self.files.removed.rewind_to(replay.removed_end);
        }
        if replay.rows.is_empty() && self.files.idx.is_none() {
            let _ = fs::remove_dir_all(self.gen_dir.join("btx"));
        } else {
            if self.meta.ksize == 0 {
                bail!("tndb: the table has rows but no recorded key size");
            }
            let idx = self.index_mut()?;
            idx.rebuild_from(replay.rows)?;
        }
        let (data_len, dead) = (self.files.data.file_len(), self.dead);
        if let Some(idx) = self.files.idx.as_mut() {
            idx.set_data_file_length(data_len);
            idx.set_owner_value(dead);
            idx.sync()?;
            idx.mark_consistent();
        }
        self.files.data.mark_consistent();
        self.files.removed.mark_consistent();
        #[cfg(test)]
        {
            self.rebuilt = true;
        }
        Ok(())
    }
}

impl Drop for Writer {
    /// A clean close commits whatever is uncommitted, the ordinary way, and records the log length
    /// the index matches, so the next open needs no rebuild; a close that cannot commit makes the
    /// next open rebuild instead, which drops the uncommitted records.
    fn drop(&mut self) {
        // Let a spare being prepared finish (so no thread writes in the table directory after it
        // closes), then discard it: the next open prepares its own.
        if let Some((dir, mut spare)) = self.take_spare() {
            spare.data.set_remove_on_drop();
            spare.removed.set_remove_on_drop();
            if let Some(idx) = spare.idx.as_mut() {
                idx.set_remove_on_drop();
            }
            drop(spare);
            let _ = fs::remove_dir_all(dir);
        }
        // Commit what is uncommitted the ordinary way (the removal log synced before the commit
        // record that makes its removals count).
        let committed = self.flush().is_ok();
        // Keep a compaction that has finished (switch to it); stop one still running. Wait for
        // every compaction thread, so none writes in the table directory after it closes.
        if committed {
            let _ = self.switch_if_compacted();
        }
        self.cancel_compaction();
        self.reap_cancelled(true);
        if !committed {
            // The close could not commit: make the next open rebuild from the logs (a length the
            // log never has), which drops the uncommitted tail.
            if let Some(idx) = self.files.idx.as_mut() {
                idx.set_data_file_length(u64::MAX);
            }
            return;
        }
        let (data_len, dead) = (self.files.data.file_len(), self.dead);
        if let Some(idx) = self.files.idx.as_mut() {
            idx.set_data_file_length(data_len);
            idx.set_owner_value(dead);
        }
    }
}

/// What readers see: the last published index snapshot and a view of the data log (published up
/// to the log's extent at that publish). Immutable; replaced as a whole by each publish.
#[derive(Debug)]
struct Published {
    index: Option<IndexSnapshot>,
    data_view: Arc<MapView>,
    /// Bytes before each record's value (see [`Writer::value_offset`]).
    value_offset: usize,
    /// The generation this snapshot reads, and where its logs' committed records end (a
    /// compaction starts from these).
    gen: u64,
    committed: Committed,
    /// Keeps this snapshot's generation open if it is cleared or compacted away. Declared last so
    /// it drops after the snapshot.
    alive: Arc<GenAlive>,
}

impl Published {
    /// The value bytes of the record at `pos`.
    fn value_at(&self, pos: u64) -> Result<&[u8], FetchError> {
        record_value(Pack::<Vec<u8>>::record_bytes_in(&self.data_view, pos)?, self.value_offset)
    }

    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
        let Some(index) = &self.index else { return Ok(None) };
        let pos = match index.load(key) {
            Ok(pos) => pos,
            Err(FetchError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(Some(decode(self.value_at(pos)?)))
    }

    fn contains(&self, key: &[u8]) -> Result<bool, FetchError> {
        let Some(index) = &self.index else { return Ok(false) };
        match index.load(key) {
            Ok(_) => Ok(true),
            Err(FetchError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn len(&self) -> usize {
        self.index.as_ref().map_or(0, IndexSnapshot::len)
    }
}

/// A table's state: the writer side and the published snapshot readers load. Field order makes
/// the writer (and so the files) drop before the snapshot that points into their mappings.
#[derive(Debug)]
struct Inner {
    writer: Mutex<Writer>,
    published: ArcSwap<Published>,
    /// The table directory, for error logs.
    label: String,
}

/// A cheap `Clone` handle to a table.  Reads take no lock (they load the published snapshot);
/// writes take the writer lock and become readable at the next [`Self::flush`].  The table closes
/// cleanly when the last handle — or the last live [`TableScan`], which holds the table — is
/// dropped.
#[derive(Clone, Debug)]
pub(crate) struct TnTable {
    inner: Arc<Inner>,
}

/// The current generation of the table at `dir`, opened: the newest generation (an error if it
/// does not open), with every older one deleted (more than one exists only after a crash during a
/// clear); a new table gets generation 0.
fn open_current_gen(dir: &Path) -> eyre::Result<(u64, Log, Log)> {
    let gens = list_gens(dir)?;
    let Some(&newest) = gens.last() else {
        let (data, removed) = create_gen(dir, 0)?;
        return Ok((0, data, removed));
    };
    // The newest generation is current. One that does not open is an error, never a reason to
    // delete it or to fall back to an older one: a clear renames a complete generation into place,
    // and a partly created one opens as empty (a missing log is created, a zeroed one reset), so a
    // failure here is damage or an environment error, and older data must not come back.
    let current = newest;
    let logs = open_logs(&gen_dir(dir, current))
        .wrap_err_with(|| format!("tndb: open generation {current} of {}", dir.display()))?;
    // An older generation is never opened again (the newest is current), so one that cannot be
    // deleted now (logged) is only garbage for a later open to remove.
    for &old in gens.iter().filter(|&&n| n < current) {
        if let Err(e) = fs::remove_dir_all(gen_dir(dir, old)) {
            tracing::warn!(target: "tndb", "remove old generation {old} of {}: {e}", dir.display());
        }
    }
    Ok((current, logs.0, logs.1))
}

impl TnTable {
    /// Open (creating if needed) the table rooted at `dir`: keyed, or with `key_fn` a derived-key
    /// table (the mode is fixed when the table is created; opening it in the other mode is an
    /// error). Rebuilds the index from the logs when the table was not closed cleanly. Compacts
    /// as [`CompactionConfig::default`].
    #[cfg(test)]
    pub(crate) fn open(dir: PathBuf, key_fn: Option<KeyFn>) -> eyre::Result<Self> {
        Self::open_with(dir, key_fn, CompactionConfig::default())
    }

    /// [`Self::open`], compacting as `config` says.
    pub(crate) fn open_with(
        dir: PathBuf,
        key_fn: Option<KeyFn>,
        config: CompactionConfig,
    ) -> eyre::Result<Self> {
        fs::create_dir_all(&dir)?;
        let lock = lock_table(&dir)?;
        remove_meta_tmp(&dir);
        let mode = if key_fn.is_some() { KeyMode::Derived } else { KeyMode::Keyed };
        let meta = match TableMeta::read(&dir)? {
            Some(meta) if meta.mode == mode => meta,
            Some(meta) => {
                bail!("tndb: table {} is {:?} but was opened {:?}", dir.display(), meta.mode, mode)
            }
            None => {
                if !list_gens(&dir)?.is_empty() {
                    bail!("tndb: table {} has data but no meta file", dir.display());
                }
                let meta = TableMeta { mode, ksize: 0 };
                meta.write(&dir)?;
                meta
            }
        };
        remove_spares(&dir)?;
        let (gen, data, removed) = open_current_gen(&dir)?;
        let gen_path = gen_dir(&dir, gen);

        // The index, if the generation has one: a header or geometry the open rejects marks it for
        // a rebuild; an io error is surfaced, not papered over by a rebuild.
        let mut index_broken = false;
        let idx = if gen_path.join("btx").exists() && meta.ksize > 0 {
            match BtreeIndex::open_btx_file(gen_path.join("btx"), data.header(), meta.ksize, false)
            {
                Ok(idx) => Some(idx),
                Err(LoadHeaderError::IO(e)) => return Err(e.into()),
                Err(_) => {
                    index_broken = true;
                    None
                }
            }
        } else {
            index_broken = gen_path.join("btx").exists();
            None
        };
        let data_view = data.view();
        let has_records = data.file_len() > DATA_HEADER_BYTES as u64;
        let must_rebuild = index_broken
            || data.opened_unclean()
            || removed.opened_unclean()
            || match &idx {
                Some(idx) => idx.opened_unclean() || idx.data_file_length() != data.file_len(),
                None => has_records,
            };
        if index_broken {
            // A rejected index is derived data: discard it (after the logs are checked, the
            // rebuild creates it fresh).
            let _ = fs::remove_dir_all(gen_path.join("btx"));
        }

        let mut writer = Writer {
            table_dir: dir,
            meta,
            key_fn,
            gen,
            gen_dir: gen_path,
            files: GenFiles { data, removed, idx },
            data_view,
            alive: Arc::default(),
            spare: None,
            uncommitted: false,
            removals_unsynced: false,
            applied_seq: 0,
            first_pending: None,
            open_txns: 0,
            #[cfg(test)]
            commit_gate: None,
            config,
            dead: 0,
            compaction: None,
            auto_retry_at: None,
            auto_backoff: AUTO_BACKOFF_MIN,
            cancelled: Vec::new(),
            #[cfg(test)]
            compact_gate: None,
            #[cfg(test)]
            compact_arrived: None,
            #[cfg(test)]
            fail_compactions: false,
            #[cfg(test)]
            compactions_started: 0,
            #[cfg(test)]
            last_switch_tail: 0,
            #[cfg(test)]
            free_space_for_test: None,
            #[cfg(test)]
            fail_next_switch_sync: false,
            #[cfg(test)]
            rebuilt: false,
            #[cfg(test)]
            clears_from_spare: 0,
            #[cfg(test)]
            fail_next_clear_sync: false,
            #[cfg(test)]
            fail_next_flush: false,
            failed: None,
            _lock: Some(lock),
        };
        if must_rebuild {
            writer.recover()?;
        } else {
            // A clean close kept the count in the index header.
            writer.dead = writer.files.idx.as_ref().map_or(0, BtreeIndex::owner_value);
        }
        writer.prepare_next_spare(None);
        let published = writer.publish();
        let label = writer.table_dir.display().to_string();
        Ok(Self {
            inner: Arc::new(Inner {
                writer: Mutex::new(writer),
                published: ArcSwap::from_pointee(published),
                label,
            }),
        })
    }

    /// True if this open rebuilt the index from the logs.
    #[cfg(test)]
    pub(crate) fn rebuilt_on_open(&self) -> bool {
        self.inner.writer.lock().rebuilt
    }

    /// Test-only: make the next commit fail before it writes anything.
    #[cfg(test)]
    pub(crate) fn fail_next_flush(&self) {
        self.inner.writer.lock().fail_next_flush = true;
    }

    /// Test-only, for a simulated crash (a leaked handle): release the table's lock, as a crashed
    /// process would, so the table can be reopened.
    #[cfg(test)]
    pub(crate) fn release_lock_for_crash(&self) {
        self.inner.writer.lock()._lock = None;
    }

    /// Test-only: start a compaction whose thread waits at each checkpoint (after the copy, and
    /// after each catch-up round) for a message on the returned channel, or for its drop.
    #[cfg(test)]
    pub(crate) fn start_compaction_gated(&self) -> std::sync::mpsc::Sender<()> {
        let (gate, wait) = std::sync::mpsc::channel();
        let mut writer = self.inner.writer.lock();
        writer.compact_gate = Some(wait);
        assert!(writer.start_compaction(self.inner.published.load_full()), "compaction started");
        gate
    }

    /// Wait (not holding the writer lock) until the running compaction's thread, if any, has
    /// finished.
    pub(crate) fn wait_compaction_thread(&self) {
        while self.inner.writer.lock().compaction.as_ref().is_some_and(|c| !c.is_finished()) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Test-only: make the next compaction switch's directory sync (after its rename) fail.
    #[cfg(test)]
    pub(crate) fn fail_next_switch_sync(&self) {
        self.inner.writer.lock().fail_next_switch_sync = true;
    }

    /// Test-only: the table's generation, log lengths, dead puts, and running compaction.
    #[cfg(test)]
    pub(crate) fn compaction_state(&self) -> CompactionState {
        let writer = self.inner.writer.lock();
        CompactionState {
            gen: writer.gen,
            data_len: writer.files.data.file_len(),
            removed_len: writer.files.removed.file_len(),
            dead: writer.dead,
            running: writer.compaction.is_some(),
            started: writer.compactions_started,
            last_switch_tail: writer.last_switch_tail,
        }
    }

    /// Test-only: [`Self::start_compaction_gated`], also returning the channel the thread reports
    /// each checkpoint it reaches on.
    #[cfg(test)]
    pub(crate) fn start_compaction_observed(
        &self,
    ) -> (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<u32>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        self.inner.writer.lock().compact_arrived = Some(arrived);
        (self.start_compaction_gated(), arrivals)
    }

    /// Test-only: make every compaction thread fail before it builds anything (or stop).
    #[cfg(test)]
    pub(crate) fn fail_compactions(&self, fail: bool) {
        self.inner.writer.lock().fail_compactions = fail;
    }

    /// Test-only: the free disk space the compaction guard sees (`None`: the real probe).
    #[cfg(test)]
    pub(crate) fn set_free_space_for_test(&self, bytes: Option<u64>) {
        self.inner.writer.lock().free_space_for_test = bytes;
    }

    /// Test-only: lift the automatic compaction's backoff after a failure.
    #[cfg(test)]
    pub(crate) fn clear_compaction_backoff(&self) {
        self.inner.writer.lock().auto_retry_at = None;
    }

    /// Insert (or overwrite) `key → value`; readable from the next flush.
    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
        self.inner.writer.lock().insert(key, value)
    }

    /// Remove `key`; returns whether it was present. Readable from the next flush.
    pub(crate) fn remove(&self, key: &[u8]) -> eyre::Result<bool> {
        self.inner.writer.lock().remove(key)
    }

    /// Reset the table to empty: a new, empty generation (durable on return); the old one's files
    /// are deleted. Readable from the next flush.
    pub(crate) fn clear(&self) -> eyre::Result<()> {
        self.inner.writer.lock().clear()
    }

    /// Group commit (see `super::commit`): insert `key → value` and publish it at once, without a
    /// commit, numbering it from `seq` for the committer. Visible to readers on return; durable at
    /// the committer's next round.
    pub(crate) fn insert_published(
        &self,
        key: &[u8],
        value: &[u8],
        group: &GroupCommit,
        name: &'static str,
    ) -> eyre::Result<()> {
        let mut writer = self.inner.writer.lock();
        writer.insert(key, value)?;
        self.publish_pending(&mut writer, group, name);
        Ok(())
    }

    /// Group commit: [`Self::remove`], published at once (see [`Self::insert_published`]).
    pub(crate) fn remove_published(
        &self,
        key: &[u8],
        group: &GroupCommit,
        name: &'static str,
    ) -> eyre::Result<bool> {
        let mut writer = self.inner.writer.lock();
        let removed = writer.remove(key)?;
        if removed {
            self.publish_pending(&mut writer, group, name);
        }
        Ok(removed)
    }

    /// Group commit: [`Self::clear`] (itself durable on return), published at once.
    pub(crate) fn clear_published(
        &self,
        group: &GroupCommit,
        name: &'static str,
    ) -> eyre::Result<()> {
        let mut writer = self.inner.writer.lock();
        writer.clear()?;
        self.publish_pending(&mut writer, group, name);
        Ok(())
    }

    /// Group commit: a write transaction is about to write this table for the first time. Until
    /// it ends ([`Self::txn_end`]) the committer leaves the table uncommitted.
    pub(crate) fn txn_begin(&self) {
        self.inner.writer.lock().open_txns += 1;
    }

    /// Group commit: a write transaction that wrote this table ended (committed or dropped):
    /// publish its writes and hand them to the committer.
    pub(crate) fn txn_end(&self, group: &GroupCommit, name: &'static str) {
        let mut writer = self.inner.writer.lock();
        writer.open_txns = writer.open_txns.saturating_sub(1);
        self.publish_pending(&mut writer, group, name);
    }

    /// Publish every write applied so far (no commit) and hand the table to the committer with
    /// the next sequence number, under the writer lock (so sequence order is log order).
    ///
    /// The table is marked dirty *before* the number is taken: a committer round reads its target
    /// number before it takes the dirty tables, so any write numbered at or below the target is
    /// in that round (or an earlier one), and the round never counts a write it did not commit.
    fn publish_pending(&self, writer: &mut Writer, group: &GroupCommit, name: &'static str) {
        self.inner.published.store(Arc::new(writer.publish()));
        group.mark_dirty(name);
        let n = group.seq.fetch_add(1, Ordering::AcqRel) + 1;
        writer.applied_seq = n;
        writer.first_pending.get_or_insert(n);
    }

    /// The committer's step for this table (see `super::commit`): commit every write published so
    /// far, syncing outside the writer lock so writers never wait on the disk. Returns the first
    /// sequence number still not durable here, if any: a table with an open write transaction is
    /// left as it is, and writes published during the sync wait for the next round.
    ///
    /// The ordering is [`Writer::flush`]'s: removals are durable before the commit record that
    /// makes them count. So the removal log is synced first (unlocked), any removal appended
    /// meanwhile is synced under the lock, and only then is the commit record appended.
    pub(crate) fn group_commit(&self) -> eyre::Result<Option<u64>> {
        // (a) What is pending; the removal log's unsynced range, if any.
        #[cfg(test)]
        let gate;
        let removal: Option<SyncTicket> = {
            // Mutated only to take the test gate.
            #[cfg_attr(not(test), allow(unused_mut))]
            let mut writer = self.inner.writer.lock();
            if writer.open_txns > 0 {
                return Ok(writer.first_pending);
            }
            if writer.first_pending.is_none() && !writer.uncommitted {
                return Ok(None);
            }
            writer.check_failed()?;
            #[cfg(test)]
            {
                gate = writer.commit_gate.take();
            }
            match writer.removals_unsynced {
                true => Some(writer.files.removed.sync_ticket()?),
                false => None,
            }
        };
        // (b) The removal log, unlocked.
        let removal_outcome = removal.as_ref().map(SyncTicket::sync);
        #[cfg(test)]
        let gate = commit_gate_wait(gate, CommitPoint::AfterRemovalSync);
        // (c) Under the lock: finish the removal log, append the commit record, take the data
        // log's range.
        let (data, removals_synced_to, covered) = {
            let mut writer = self.inner.writer.lock();
            if let (Some(ticket), Some(outcome)) = (&removal, &removal_outcome) {
                writer.files.removed.complete_sync(ticket, outcome)?;
            }
            if writer.open_txns > 0 {
                return Ok(writer.first_pending);
            }
            #[cfg(test)]
            if std::mem::take(&mut writer.fail_next_flush) {
                bail!("injected commit failure");
            }
            let removals = writer.removals_unsynced.then(|| writer.files.removed.file_len());
            if removals.is_some() {
                // Removals appended during the unlocked sync (rare, and few).
                writer.files.removed.commit()?;
            }
            if writer.uncommitted {
                writer.files.data.append_raw(COMMIT)?;
            }
            writer.uncommitted = false;
            writer.removals_unsynced = false;
            // Nothing is uncommitted and no transaction is open: the moment a finished compaction
            // can switch in (its catch-up replays through the commit record just appended and
            // syncs the new generation before renaming it into place). Waiting for a round that
            // ends idle instead would never come while writes keep arriving.
            let removals = match writer.switch_if_compacted()? {
                true => {
                    self.inner.published.store(Arc::new(writer.publish()));
                    None // the removal log just synced belongs to the retired generation
                }
                false => removals,
            };
            (writer.files.data.sync_ticket()?, removals, writer.applied_seq)
        };
        #[cfg(test)]
        commit_gate_wait(gate, CommitPoint::AfterCommitRecord);
        // (d) The data log, unlocked; writes appended meanwhile lie past the ticket.
        let outcome = data.sync();
        // (e) Record it; start a compaction if one is due.
        let mut writer = self.inner.writer.lock();
        writer.files.data.complete_sync(&data, &outcome)?;
        writer.files.data.stamp_commit_marker_at(data.end());
        if let Some(removed_end) = removals_synced_to {
            writer.files.removed.stamp_commit_marker_at(removed_end);
        }
        writer.first_pending = (writer.applied_seq != covered).then_some(covered + 1);
        if writer.wants_compaction() && !writer.start_compaction(self.inner.published.load_full()) {
            writer.back_off();
        }
        Ok(writer.first_pending)
    }

    /// Test-only: the committer's next round on this table stops at `point`, reports it on the
    /// first channel, and waits for a message (or the sender's drop) on the second.
    #[cfg(test)]
    pub(crate) fn gate_next_group_commit(
        &self,
        point: CommitPoint,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (arrived, arrivals) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        self.inner.writer.lock().commit_gate = Some((point, arrived, released));
        (arrivals, release)
    }

    /// Commit (see [`Writer::flush`]), then publish every write so far: install a new snapshot for
    /// readers. Readers are never blocked by it (they take no lock); writers wait for it.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        self.commit().map(drop)
    }

    /// [`Self::flush`], switching to a compaction that has finished between the commit and the
    /// publish, and starting the automatic compaction when it is due. Returns whether the table
    /// switched to a compacted generation.
    fn commit(&self) -> eyre::Result<bool> {
        let mut writer = self.inner.writer.lock();
        writer.flush()?;
        // A switch that fails after its rename stops the writer; the commit is still published.
        let switched = writer.switch_if_compacted();
        // Stored under the writer lock, so snapshots are installed in publish order.
        self.inner.published.store(Arc::new(writer.publish()));
        if writer.wants_compaction() && !writer.start_compaction(self.inner.published.load_full()) {
            writer.back_off();
        }
        if !writer.cancelled.is_empty() {
            writer.reap_cancelled(false);
        }
        switched
    }

    /// Start compacting the table in the background if it holds dead puts and its data log is at
    /// least [`REQUESTED_MIN_BYTES`] (see [`compact`]); the table switches to the compacted
    /// generation at a later commit. Returns whether a compaction is running.
    pub(crate) fn compact(&self) -> bool {
        let mut writer = self.inner.writer.lock();
        if writer.dead == 0 || writer.files.data.file_len() < REQUESTED_MIN_BYTES {
            return writer.compaction.is_some();
        }
        writer.start_compaction(self.inner.published.load_full())
    }

    /// Compact the table now, whatever its garbage: start a compaction (or take the running one),
    /// wait for its thread, and commit to switch to it. Blocks the caller for the whole copy (the
    /// table stays readable and writable).
    pub(crate) fn compact_now(&self) -> eyre::Result<()> {
        let started = self.inner.writer.lock().start_compaction(self.inner.published.load_full());
        if !started {
            bail!("tndb: table {} cannot start a compaction", self.inner.label);
        }
        self.finish_compaction()
    }

    /// Wait for the running compaction's thread, then commit to switch to it.
    pub(crate) fn finish_compaction(&self) -> eyre::Result<()> {
        self.wait_compaction_thread();
        if !self.commit()? {
            bail!("tndb: table {}: the compaction did not complete (logged)", self.inner.label);
        }
        Ok(())
    }

    /// Read the published value for `key` and map its bytes with `decode`, or `None` if absent. No
    /// lock: `decode` borrows the value straight from the log's mapping.
    pub(crate) fn get_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.inner.published.load().get_with(key, decode)
    }

    /// Read `key` from the working tree, including writes not yet flushed (a write transaction's
    /// own reads). Takes the writer lock.
    pub(crate) fn get_working_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.inner.writer.lock().get_with(key, decode)
    }

    /// True if `key` is present in the published snapshot. A damaged index is an error, not
    /// "absent".
    pub(crate) fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.inner.published.load().contains(key)?)
    }

    /// True if the published snapshot has no entries.
    pub(crate) fn is_empty(&self) -> eyre::Result<bool> {
        Ok(self.inner.published.load().len() == 0)
    }

    /// Number of entries in the published snapshot. (Part of the table API; not currently used by
    /// `TnDatabase`.)
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> eyre::Result<usize> {
        Ok(self.inner.published.load().len())
    }

    /// A lazy, key-ordered scan over `(key_bytes, value_bytes)` of the published snapshot. Takes no
    /// lock (see the module docs); a table with no index yet scans empty.
    pub(crate) fn scan(&self, kind: ScanKind) -> TableScan {
        let published = self.inner.published.load_full();
        let cursor = published.index.as_ref().and_then(|index| match kind.cursor(index) {
            Ok(cursor) => Some(cursor),
            Err(e) => {
                scan_error(&self.inner.label, &e);
                None
            }
        });
        TableScan { published, cursor, _table: Arc::clone(&self.inner) }
    }

    /// Map the single `(key_bytes, value_bytes)` a scan of `kind` lands on first with `f`, or
    /// `None` if it lands on nothing — a direct seek in the published snapshot.
    pub(crate) fn first_with<R>(
        &self,
        kind: ScanKind,
        f: impl FnOnce(&[u8], &[u8]) -> R,
    ) -> Option<R> {
        let published = self.inner.published.load();
        let index = published.index.as_ref()?;
        let found = kind.cursor(index).and_then(|mut cursor| {
            let Some(item) = cursor.next(index) else { return Ok(None) };
            let (key, pos) = item?;
            Ok(Some((key, published.value_at(pos)?)))
        });
        match found {
            Ok(Some((key, value))) => Some(f(key, value)),
            Ok(None) => None,
            Err(e) => {
                scan_error(&self.inner.label, &e);
                None
            }
        }
    }
}

/// Test-only: where a table's group commit stops (see [`TnTable::gate_next_group_commit`]).
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitPoint {
    /// After the unlocked removal-log sync, before the commit record.
    AfterRemovalSync,
    /// After the commit record (and a compaction's switch), before the unlocked data-log sync.
    AfterCommitRecord,
}

/// Test-only: wait at `gate` if it is set for `point`; otherwise hand it on.
#[cfg(test)]
fn commit_gate_wait(
    gate: Option<(CommitPoint, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    point: CommitPoint,
) -> Option<(CommitPoint, std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)> {
    match gate {
        Some((at, arrived, release)) if at == point => {
            let _ = arrived.send(());
            let _ = release.recv_timeout(Duration::from_secs(30));
            None
        }
        other => other,
    }
}

/// Test-only: see [`TnTable::compaction_state`].
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct CompactionState {
    pub(crate) gen: u64,
    pub(crate) data_len: u64,
    pub(crate) removed_len: u64,
    pub(crate) dead: u64,
    pub(crate) running: bool,
    pub(crate) started: u32,
    pub(crate) last_switch_tail: u64,
}

/// Log a scan or seek ended by an error. `DBIter` (and a seek's `Option`) cannot carry it, but a
/// damaged table must not pass for a shorter one in silence.
fn scan_error(table: &str, e: &FetchError) {
    tracing::error!(target: "tndb", "scan of table {table} ended by an error: {e}");
}

/// A lazy, key-ordered scan of a published snapshot (see [`TnTable::scan`]), stepped with
/// [`Self::next_with`]. Holds no lock; it keeps the table (and so the mappings the snapshot reads)
/// open until dropped.
pub(crate) struct TableScan {
    published: Arc<Published>,
    /// `None` once the scan is exhausted or failed (or the table had no index).
    cursor: Option<BtreeCursor>,
    /// Keeps the table's files open, so the snapshot's mappings stay valid. Declared last so it
    /// drops after the snapshot.
    _table: Arc<Inner>,
}

impl TableScan {
    /// Map the next `(key_bytes, value_bytes)` with `f` — the key borrowed from the index page, the
    /// value from the log — or `None` when the scan is done.  A fetch failure ends the scan.
    pub(crate) fn next_with<R>(&mut self, f: impl FnOnce(&[u8], &[u8]) -> R) -> Option<R> {
        let published = &*self.published;
        let index = published.index.as_ref()?;
        let row = self.cursor.as_mut()?.next(index).map(|item| {
            let (key, pos) = item?;
            Ok((key, published.value_at(pos)?))
        });
        match row {
            Some(Ok((key, value))) => Some(f(key, value)),
            Some(Err(e)) => {
                scan_error(&self._table.label, &e);
                self.cursor = None;
                None
            }
            None => {
                self.cursor = None;
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;

    /// 8-byte big-endian key (so byte order equals numeric order) and a small value.
    fn kv(i: u64) -> (Vec<u8>, Vec<u8>) {
        (i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
    }

    fn key_u64(key: &[u8]) -> u64 {
        u64::from_be_bytes(key.try_into().expect("8-byte key"))
    }

    fn keys_of(mut scan: TableScan) -> Vec<u64> {
        std::iter::from_fn(|| scan.next_with(|k, _| key_u64(k))).collect()
    }

    #[test]
    fn test_tntable_ops_and_scans() {
        let tmp = TempDir::with_prefix("tntable").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");

        assert!(table.is_empty().expect("is_empty"));
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        assert!(!table.is_empty().expect("is_empty"));
        assert_eq!(table.len().expect("len"), 100);

        // Point reads.
        for i in 0..100u64 {
            let (k, v) = kv(i);
            assert_eq!(table.get_with(&k, |b| b.to_vec()).expect("get"), Some(v));
            assert!(table.contains(&k).expect("contains"));
        }
        assert_eq!(table.get_with(&kv(999).0, |b| b.to_vec()).expect("get miss"), None);
        assert!(!table.contains(&kv(999).0).expect("contains miss"));

        // Ordered scans.
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), (0..100).collect::<Vec<_>>());
        assert_eq!(keys_of(table.scan(ScanKind::Reverse)), (0..100).rev().collect::<Vec<_>>());
        assert_eq!(
            keys_of(table.scan(ScanKind::From(50u64.to_be_bytes().to_vec()))),
            (50..100).collect::<Vec<_>>()
        );

        // Early-terminate a scan: take a few, drop the rest.
        let mut scan = table.scan(ScanKind::Forward);
        let head: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, _| key_u64(k))).take(3).collect();
        assert_eq!(head, vec![0, 1, 2]);
        drop(scan);

        // Remove + clear.
        assert!(table.remove(&kv(0).0).expect("remove"));
        assert!(!table.remove(&kv(0).0).expect("remove again"));
        table.flush().expect("flush");
        assert_eq!(table.len().expect("len after remove"), 99);
        table.clear().expect("clear");
        table.flush().expect("flush");
        assert!(table.is_empty().expect("is_empty after clear"));
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), Vec::<u64>::new());
    }

    /// Writes are invisible to readers until a flush publishes them, but visible to the writer's
    /// own working reads at once.
    #[test]
    fn test_tntable_writes_visible_at_flush() {
        let tmp = TempDir::with_prefix("tntable_visibility").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
        let (k1, v1) = kv(1);
        table.insert(&k1, &v1).expect("insert");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), None, "not yet published");
        assert!(!table.contains(&k1).expect("contains"));
        assert_eq!(table.get_working_with(&k1, |b| b.to_vec()).expect("working"), Some(v1.clone()));
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1.clone()));

        // An unpublished overwrite and remove leave the published value in place.
        table.insert(&k1, b"new").expect("overwrite");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1.clone()));
        assert!(table.remove(&k1).expect("remove"));
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1));
        assert_eq!(table.get_working_with(&k1, |b| b.to_vec()).expect("working"), None);
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), None);
    }

    #[test]
    fn test_tntable_persists_across_reopen() {
        let tmp = TempDir::with_prefix("tntable_persist").expect("temp dir");
        let dir = tmp.path().join("t");
        {
            let table = TnTable::open(dir.clone(), None).expect("open");
            for i in 0..50u64 {
                let (k, v) = kv(i);
                table.insert(&k, &v).expect("insert");
            }
            assert!(table.remove(&kv(7).0).expect("remove"));
            table.flush().expect("flush");
        } // drop -> actor thread joins, clean close (index synced, log sealed)

        // Reopen: the on-disk index reopens on the first same-width insert, then old+new are
        // visible.
        let table = TnTable::open(dir, None).expect("reopen");
        let (k100, v100) = kv(100);
        table.insert(&k100, &v100).expect("insert after reopen");
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k100, |b| b.to_vec()).expect("get new"), Some(v100));
        assert_eq!(
            table.get_with(&kv(20).0, |b| b.to_vec()).expect("get old"),
            Some(kv(20).1),
            "old value persisted"
        );
        assert_eq!(
            table.get_with(&kv(7).0, |b| b.to_vec()).expect("get removed"),
            None,
            "removal persisted"
        );
    }

    /// A scan holds no lock: the same thread can write and flush the table mid-scan, and the scan
    /// keeps yielding the snapshot it started on (overwrites, removes and inserts made after it
    /// started are not seen) while a new scan sees the new state.
    #[test]
    fn test_tntable_write_during_scan_keeps_the_snapshot() {
        let tmp = TempDir::with_prefix("tntable_scan_write").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
        for i in 0..3_000u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        let mut scan = table.scan(ScanKind::Forward);
        assert_eq!(scan.next_with(|k, v| (key_u64(k), v.to_vec())), Some((0, kv(0).1)));

        for i in 0..3_000u64 {
            table.insert(&kv(i).0, b"changed").expect("overwrite during the scan");
        }
        for i in 0..1_000u64 {
            assert!(table.remove(&kv(i).0).expect("remove during the scan"));
        }
        table.insert(&kv(9_999).0, &kv(9_999).1).expect("insert during the scan");
        table.flush().expect("flush during the scan");

        let rest: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, v| (key_u64(k), v.to_vec()))).collect();
        assert_eq!(rest, (1..3_000u64).map(|i| (i, kv(i).1)).collect::<Vec<_>>());
        assert_eq!(table.len().expect("len"), 2_001);
        assert_eq!(
            table.get_with(&kv(2_000).0, |b| b.to_vec()).expect("get"),
            Some(b"changed".to_vec())
        );
    }

    /// Readers on several threads against a writer that keeps overwriting and committing: every
    /// read sees a complete committed value (all its keys from one commit generation or later,
    /// never a torn or uncommitted value), across index and log growth.
    #[test]
    fn test_tntable_readers_against_committing_writer() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        const KEYS: u64 = 500;
        let value = |i: u64, generation: u64| -> Vec<u8> {
            let mut v = vec![(i ^ generation) as u8; 1_024];
            v[..8].copy_from_slice(&generation.to_le_bytes());
            v[8..16].copy_from_slice(&i.to_le_bytes());
            v
        };
        let tmp = TempDir::with_prefix("tntable_concurrent").expect("temp dir");
        // No automatic compaction: a switch to a new index would change the page count below.
        let config = CompactionConfig { auto_min_bytes: None, ..Default::default() };
        let table = TnTable::open_with(tmp.path().join("t"), None, config).expect("open");
        for i in 0..KEYS {
            table.insert(&kv(i).0, &value(i, 0)).expect("insert");
        }
        table.flush().expect("flush");
        let committed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let readers: Vec<_> = (0..4u64)
            .map(|t| {
                let (table, committed, stop) = (table.clone(), committed.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut x = t + 1;
                    let mut reads = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let floor = committed.load(Ordering::Acquire);
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let i = (x >> 33) % KEYS;
                        let v = table.get_with(&kv(i).0, |b| b.to_vec()).expect("get");
                        let v = v.expect("every key is always present");
                        let generation = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                        assert!(generation >= floor, "read generation {generation} < {floor}");
                        assert_eq!(v, value(i, generation), "a torn or foreign value");
                        if reads.is_multiple_of(64) {
                            // A whole scan reads one committed state: every key, all from one
                            // generation (each commit overwrites every key). Reader 0 dawdles
                            // mid-scan, holding its snapshot across commits while pages the
                            // writer replaced are reclaimed and reused.
                            let mut scan = table.scan(ScanKind::Forward);
                            let mut gens = Vec::new();
                            while let Some((ok, g)) = scan.next_with(|k, v| {
                                let g = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                                (v == value(key_u64(k), g).as_slice(), g)
                            }) {
                                assert!(ok, "a torn value in a scan");
                                gens.push(g);
                                if t == 0 && gens.len() == KEYS as usize / 2 {
                                    std::thread::sleep(std::time::Duration::from_millis(2));
                                }
                            }
                            assert_eq!(gens.len(), KEYS as usize);
                            assert!(gens.iter().all(|&g| g == gens[0]), "a scan mixed states");
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        let commit = |generation: u64| {
            for i in 0..KEYS {
                table.insert(&kv(i).0, &value(i, generation)).expect("overwrite");
            }
            table.flush().expect("commit");
        };
        for generation in 1..=200u64 {
            commit(generation);
            committed.store(generation, Ordering::Release);
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            assert!(reader.join().expect("reader") > 0);
        }

        // With the readers gone every replaced index page is reusable: more commits do not grow
        // the index file.
        let index_pages = |table: &TnTable| {
            table.inner.writer.lock().files.idx.as_ref().expect("index").page_count()
        };
        commit(201);
        let pages = index_pages(&table);
        for generation in 202..=260u64 {
            commit(generation);
        }
        assert_eq!(index_pages(&table), pages, "replaced index pages are reused");
    }

    /// A live scan keeps the table open (it shares the table's state) even after the last handle
    /// is dropped; the table then closes cleanly when the scan ends.
    #[test]
    fn test_tntable_scan_outlives_its_handle() {
        let tmp = TempDir::with_prefix("tntable_scan_outlives").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        for i in 0..10u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        let scan = table.scan(ScanKind::Reverse);
        drop(table);
        assert_eq!(keys_of(scan), (0..10).rev().collect::<Vec<_>>());
        let table = TnTable::open(dir, None).expect("reopen after the scan closed the table");
        table.insert(&kv(10).0, &kv(10).1).expect("insert after reopen"); // reopens the index
        table.flush().expect("flush");
        assert_eq!(table.len().expect("len"), 11);
    }

    /// `first_with` seeks directly: the last entry, the greatest entry below a key, and the first
    /// entry at or above a key.
    #[test]
    fn test_tntable_first_with_seeks() {
        let tmp = TempDir::with_prefix("tntable_first_with").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
        assert_eq!(table.first_with(ScanKind::Reverse, |k, _| key_u64(k)), None, "no index yet");
        for i in (0..100u64).map(|i| i * 2) {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        let first = |kind| table.first_with(kind, |k, v| (key_u64(k), v.to_vec()));
        assert_eq!(first(ScanKind::Reverse), Some((198, kv(198).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(50).0)), Some((48, kv(48).1)), "strictly below");
        assert_eq!(first(ScanKind::RevFrom(kv(51).0)), Some((50, kv(50).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(0).0)), None, "nothing below the smallest key");
        assert_eq!(first(ScanKind::From(kv(51).0)), Some((52, kv(52).1)));
        assert_eq!(first(ScanKind::Forward), Some((0, kv(0).1)));
    }

    /// A scan takes no lock, so a flush of the same table completes while the scan is alive.
    #[test]
    fn test_tntable_flush_does_not_wait_for_readers() {
        use std::{sync::mpsc, time::Duration};

        let tmp = TempDir::with_prefix("tntable_flush_shared").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        let mut scan = table.scan(ScanKind::Forward);
        assert!(scan.next_with(|_, _| ()).is_some());

        // Flush on another thread so a regression fails the test instead of hanging it.
        let (done_tx, done_rx) = mpsc::channel();
        let flusher = table.clone();
        let flush = std::thread::spawn(move || {
            done_tx.send(flusher.flush().is_ok()).expect("report flush");
        });
        let flushed = done_rx.recv_timeout(Duration::from_secs(10));
        drop(scan);
        flush.join().expect("flush thread");
        assert_eq!(flushed, Ok(true), "a flush must not wait for a live scan of its table");
    }

    /// A crash during a clear leaves two generations. If the newer one was fully created, the
    /// clear happened: the table opens empty and the older generation is deleted.
    #[test]
    fn test_tntable_crash_mid_clear_after_new_generation() {
        let tmp = TempDir::with_prefix("tntable_mid_clear_done").expect("temp dir");
        let dir = tmp.path().join("t");
        {
            let table = TnTable::open(dir.clone(), None).expect("open");
            for i in 0..10 {
                let (k, v) = kv(i);
                table.insert(&k, &v).expect("insert");
            }
            table.flush().expect("flush");
        }
        drop(create_gen(&dir, 1).expect("the clear's new generation"));

        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert!(table.is_empty().expect("is_empty"), "the clear took effect");
        assert!(!gen_dir(&dir, 0).exists(), "the older generation is deleted");
        let (k, v) = kv(3);
        table.insert(&k, &v).expect("insert");
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k, |v| v.to_vec()).expect("get"), Some(v));
    }

    /// A newest generation that does not open is an error, never a reason to delete it: nothing
    /// is removed, and the older generation is not brought back.
    #[test]
    fn test_tntable_unopenable_newest_generation_fails_closed() {
        let tmp = TempDir::with_prefix("tntable_mid_clear_torn").expect("temp dir");
        let dir = tmp.path().join("t");
        {
            let table = TnTable::open(dir.clone(), None).expect("open");
            for i in 0..10 {
                let (k, v) = kv(i);
                table.insert(&k, &v).expect("insert");
            }
            table.flush().expect("flush");
        }
        fs::create_dir(gen_dir(&dir, 1)).expect("mkdir");
        fs::write(gen_dir(&dir, 1).join("data"), [0xAB_u8; 64]).expect("damaged header");

        assert!(TnTable::open(dir.clone(), None).is_err(), "the open fails closed");
        assert!(gen_dir(&dir, 1).exists(), "the newest generation is not deleted");
        assert!(gen_dir(&dir, 0).exists(), "the older generation is left as it was");
    }

    /// A clear renames the spare prepared in the background into place, and the spare prepared
    /// once the key size is known already holds an empty index.
    #[test]
    fn test_tntable_clear_activates_prepared_spare() {
        let tmp = TempDir::with_prefix("tntable_spare").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let (k, v) = kv(1);
        table.insert(&k, &v).expect("insert");
        table.flush().expect("flush");

        table.clear().expect("first clear");
        table.flush().expect("publish");
        {
            let writer = table.inner.writer.lock();
            assert_eq!(writer.clears_from_spare, 1, "the clear used the prepared spare");
            assert!(writer.files.idx.is_none(), "prepared before the key size was known");
        }
        table.clear().expect("second clear");
        table.flush().expect("publish");
        {
            let writer = table.inner.writer.lock();
            assert_eq!(writer.clears_from_spare, 2);
            assert!(writer.files.idx.is_some(), "a spare prepared later has its index ready");
        }
        // Each cleared generation is deleted in the background, by the next spare's thread.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while list_gens(&dir).expect("list") != vec![2] {
            assert!(std::time::Instant::now() < deadline, "cleared generations are deleted");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(table.is_empty().expect("is_empty"));

        table.insert(&k, &v).expect("insert after clears");
        table.flush().expect("flush");
        drop(table);
        let spares = fs::read_dir(&dir)
            .expect("table dir")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("spare-"))
            .count();
        assert_eq!(spares, 0, "a close discards its spare");
        let table = TnTable::open(dir, None).expect("reopen");
        assert!(!table.rebuilt_on_open());
        assert_eq!(table.get_with(&k, |v| v.to_vec()).expect("get"), Some(v));
    }

    /// A spare left by a crash (complete or not) is deleted on open; the table is untouched.
    #[test]
    fn test_tntable_leftover_spare_removed_on_open() {
        let tmp = TempDir::with_prefix("tntable_leftover_spare").expect("temp dir");
        let dir = tmp.path().join("t");
        {
            let table = TnTable::open(dir.clone(), None).expect("open");
            for i in 0..5 {
                let (k, v) = kv(i);
                table.insert(&k, &v).expect("insert");
            }
            table.flush().expect("flush");
        }
        let leftover = spare_dir(&dir, 7);
        fs::create_dir(&leftover).expect("mkdir");
        fs::write(leftover.join("data"), b"partial").expect("junk");

        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert!(!leftover.exists(), "the leftover spare is deleted");
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), (0..5).collect::<Vec<_>>());
    }

    /// A clear that fails after renaming the new generation into place (its directory sync
    /// failed) stops the writer: the next open will pick the new generation, so writing on in the
    /// old one would lose every later commit at restart. A reopen sees the clear.
    #[test]
    fn test_tntable_clear_failing_after_rename_latches() {
        let tmp = TempDir::with_prefix("tntable_clear_latch").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let (k, v) = kv(1);
        table.insert(&k, &v).expect("insert");
        table.flush().expect("flush");

        table.inner.writer.lock().fail_next_clear_sync = true;
        assert!(table.clear().is_err(), "the injected sync failure surfaces");
        let (k2, v2) = kv(2);
        assert!(table.insert(&k2, &v2).is_err(), "writes after the failed clear are refused");
        assert!(table.flush().is_err(), "and so are commits");
        drop(table);

        let table = TnTable::open(dir, None).expect("reopen");
        assert!(table.is_empty().expect("is_empty"), "the reopen picks the cleared generation");
    }

    /// A put or remove whose index step fails leaves nothing behind in the logs: after a later
    /// commit and a crash rebuild, the failed put is absent and the failed remove's key present.
    #[test]
    fn test_tntable_failed_index_step_leaves_no_log_record() {
        let tmp = TempDir::with_prefix("tntable_index_fail").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        for i in 0..10 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");

        // After a publish every write copies the root first, so it allocates a page.
        let fail_next = |table: &TnTable| {
            let mut writer = table.inner.writer.lock();
            writer.files.idx.as_mut().expect("index").fail_allocations_after(Some(0));
        };
        let (k_new, v_new) = kv(100);
        fail_next(&table);
        assert!(table.insert(&k_new, &v_new).is_err(), "the index step fails");
        let (k_old, _) = kv(5);
        assert!(table.remove(&k_old).is_err(), "the index step fails");
        table.inner.writer.lock().files.idx.as_mut().expect("index").fail_allocations_after(None);
        // An unrelated write commits whatever the failed calls left in the logs.
        let (k_other, v_other) = kv(200);
        table.insert(&k_other, &v_other).expect("insert");
        table.flush().expect("flush");
        // Crash (the lock released, as a dead process's is): the next open rebuilds from the logs.
        table.release_lock_for_crash();
        std::mem::forget(table);

        let table = TnTable::open(dir, None).expect("reopen");
        assert!(table.rebuilt_on_open());
        assert_eq!(table.get_with(&k_new, |v| v.to_vec()).expect("get"), None, "failed put");
        assert!(table.contains(&k_old).expect("contains"), "failed remove");
        assert!(table.contains(&k_other).expect("contains"));
    }

    // ---- compaction ----

    type Model = BTreeMap<Vec<u8>, Vec<u8>>;

    /// Simulate a crash: release the table's lock (as a dead process's is) and leak the handle,
    /// so nothing is committed or sealed on the way out.
    fn crash(table: TnTable) {
        table.release_lock_for_crash();
        std::mem::forget(table);
    }

    fn put(table: &TnTable, model: &mut Model, i: u64, tag: &str) {
        let (k, v) = (i.to_be_bytes().to_vec(), format!("{tag}{i}").into_bytes());
        table.insert(&k, &v).expect("insert");
        model.insert(k, v);
    }

    fn del(table: &TnTable, model: &mut Model, i: u64) {
        let k = i.to_be_bytes().to_vec();
        assert_eq!(table.remove(&k).expect("remove"), model.remove(&k).is_some(), "key {i}");
    }

    /// Every published read of `table` agrees with `model`: a forward scan, point reads and the
    /// length.
    fn assert_matches(table: &TnTable, model: &Model) {
        let mut scan = table.scan(ScanKind::Forward);
        let rows: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, v| (k.to_vec(), v.to_vec()))).collect();
        let expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(rows, expected, "scan");
        for (k, v) in model {
            assert_eq!(table.get_with(k, |b| b.to_vec()).expect("get").as_ref(), Some(v), "get");
        }
        assert_eq!(table.len().expect("len"), model.len(), "len");
    }

    fn compact_dirs(dir: &Path) -> usize {
        fs::read_dir(dir)
            .expect("table dir")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
            .count()
    }

    /// Wait for the background deletion of retired generations: only `gen` is left.
    fn wait_for_only_gen(dir: &Path, gen: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while list_gens(dir).expect("list") != vec![gen] {
            assert!(std::time::Instant::now() < deadline, "retired generations are deleted");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// A compaction keeps exactly the live rows and drops the dead records: puts, overwrites and
    /// removes committed while it copies, while it catches up, and after its last round (left to
    /// the switch) all land, as does a write still uncommitted when the switch's commit runs. The
    /// compacted generation is smaller, survives a crash (its logs replay to the same rows) and a
    /// clean reopen (no rebuild).
    #[test]
    fn test_tntable_compaction_keeps_live_rows_and_drops_dead_ones() {
        let tmp = TempDir::with_prefix("tntable_compact").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..2_000 {
            put(&table, &mut model, i, "a");
        }
        for i in (0..2_000).step_by(2) {
            put(&table, &mut model, i, "b");
        }
        for i in (0..2_000).step_by(3) {
            del(&table, &mut model, i);
        }
        table.flush().expect("flush");
        let before = table.compaction_state();
        assert_eq!(before.dead, 1_000 + 667, "overwrites and removes are dead puts");

        // The thread copies the snapshot, then waits: these commits are for its catch-up.
        let gate = table.start_compaction_gated();
        for i in 2_000..2_100 {
            put(&table, &mut model, i, "c");
        }
        for i in (1..600).step_by(5) {
            put(&table, &mut model, i, "d");
        }
        for i in (0..300).step_by(7) {
            del(&table, &mut model, i);
        }
        del(&table, &mut model, 1);
        put(&table, &mut model, 1, "e"); // removed, then put again in one commit
        put(&table, &mut model, 5_000, "f");
        del(&table, &mut model, 5_000); // put, then removed in one commit
        table.flush().expect("flush");
        gate.send(()).expect("resume"); // one catch-up round, then it waits again
        assert_matches(&table, &model);

        // Committed after its last round: left to the switch's own catch-up.
        for i in 100..200 {
            put(&table, &mut model, i, "g");
        }
        for i in (200..400).step_by(11) {
            del(&table, &mut model, i);
        }
        table.flush().expect("flush");
        // Uncommitted when the switch runs: its commit comes first.
        put(&table, &mut model, 7_000, "h");
        del(&table, &mut model, 2);
        drop(gate);
        table.finish_compaction().expect("switch");

        let after = table.compaction_state();
        assert_eq!(after.gen, before.gen + 1, "switched to the next generation");
        assert!(!after.running);
        assert!(after.data_len < before.data_len * 2 / 3, "{after:?} vs {before:?}");
        assert!(after.removed_len < before.removed_len / 4, "{after:?} vs {before:?}");
        assert_matches(&table, &model);
        wait_for_only_gen(&dir, after.gen);
        assert_eq!(compact_dirs(&dir), 0);

        crash(table);
        let table = TnTable::open(dir.clone(), None).expect("reopen after a crash");
        assert!(table.rebuilt_on_open(), "an unclean compacted generation replays");
        assert_matches(&table, &model);
        drop(table);
        let table = TnTable::open(dir, None).expect("clean reopen");
        assert!(!table.rebuilt_on_open());
        assert_matches(&table, &model);
    }

    /// A scan (or any snapshot) taken before the switch keeps reading the old generation, whose
    /// files stay open until it drops; a new scan reads the compacted generation.
    #[test]
    fn test_tntable_compaction_readers_keep_the_old_generation() {
        let tmp = TempDir::with_prefix("tntable_compact_readers").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..1_000 {
            put(&table, &mut model, i, "a");
        }
        let old = model.clone();
        table.flush().expect("flush");
        let mut scan = table.scan(ScanKind::Forward);
        assert_eq!(scan.next_with(|k, _| key_u64(k)), Some(0));

        for i in 0..1_000 {
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        table.compact_now().expect("compact");
        assert_eq!(table.compaction_state().gen, 1);
        wait_for_only_gen(&dir, 1);

        let rest: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, v| (k.to_vec(), v.to_vec()))).collect();
        assert_eq!(rest, old.into_iter().skip(1).collect::<Vec<_>>(), "the old generation");
        assert_matches(&table, &model);
    }

    /// A crash after the compaction's thread finished but before the switch leaves the table as
    /// it was; the reopen deletes the unfinished compaction.
    #[test]
    fn test_tntable_compaction_crash_before_switch() {
        let tmp = TempDir::with_prefix("tntable_compact_crash").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..500 {
            put(&table, &mut model, i, "a");
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        drop(table.start_compaction_gated());
        table.wait_compaction_thread();
        assert_eq!(compact_dirs(&dir), 1);
        crash(table);

        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert_eq!(compact_dirs(&dir), 0, "the unfinished compaction is deleted");
        assert_eq!(list_gens(&dir).expect("list"), vec![0]);
        assert_matches(&table, &model);
    }

    /// A clear cancels a running compaction: its thread stops and deletes its directory, and
    /// the table keeps the cleared generation.
    #[test]
    fn test_tntable_clear_cancels_compaction() {
        let tmp = TempDir::with_prefix("tntable_compact_clear").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..500 {
            put(&table, &mut model, i, "a");
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        let gate = table.start_compaction_gated(); // waits after its copy
        table.clear().expect("clear");
        model.clear();
        put(&table, &mut model, 7, "c");
        table.flush().expect("flush");
        assert!(!table.compaction_state().running, "the clear cancelled it");
        drop(gate);
        drop(table); // joins the cancelled thread

        assert_eq!(compact_dirs(&dir), 0, "the cancelled compaction deleted its directory");
        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert_eq!(table.compaction_state().gen, 1);
        assert_matches(&table, &model);
    }

    /// A derived-key table compacts too: its catch-up derives each put's key from its value.
    #[test]
    fn test_tntable_compaction_derived_keys() {
        let tmp = TempDir::with_prefix("tntable_compact_derived").expect("temp dir");
        let dir = tmp.path().join("t");
        let key_fn: KeyFn = Arc::new(|value: &[u8]| Ok(value[..8].to_vec()));
        let table = TnTable::open(dir.clone(), Some(Arc::clone(&key_fn))).expect("open");
        let mut model = Model::new();
        let put = |table: &TnTable, model: &mut Model, i: u64, tag: &str| {
            let k = i.to_be_bytes().to_vec();
            let v = [k.as_slice(), tag.as_bytes()].concat();
            table.insert(&k, &v).expect("insert");
            model.insert(k, v);
        };
        for i in 0..500 {
            put(&table, &mut model, i, "a");
        }
        for i in (0..500).step_by(2) {
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        let gate = table.start_compaction_gated();
        for i in (0..500).step_by(3) {
            put(&table, &mut model, i, "c");
        }
        for i in (0..500).step_by(5) {
            del(&table, &mut model, i);
        }
        put(&table, &mut model, 900, "d");
        table.flush().expect("flush");
        drop(gate);
        table.finish_compaction().expect("switch");
        assert_matches(&table, &model);

        crash(table);
        let table = TnTable::open(dir, Some(key_fn)).expect("reopen");
        assert!(table.rebuilt_on_open());
        assert_matches(&table, &model);
    }

    /// The automatic compaction starts at a commit once the log is large enough and at least
    /// half its puts are dead, not before.
    #[test]
    fn test_tntable_automatic_compaction_trigger() {
        let tmp = TempDir::with_prefix("tntable_compact_auto").expect("temp dir");
        let dir = tmp.path().join("t");
        let config = CompactionConfig { auto_min_bytes: Some(64 << 10), bytes_per_sec: 0 };
        let table = TnTable::open_with(dir, None, config).expect("open");
        let mut model = Model::new();
        let tag = "x".repeat(100);
        for i in 0..500 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        for i in 0..499 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        let state = table.compaction_state();
        assert!(state.data_len >= 64 << 10 && state.dead == 499);
        assert!(!state.running, "fewer dead puts than live rows");

        put(&table, &mut model, 499, &tag);
        table.flush().expect("flush");
        assert!(table.compaction_state().running, "half the puts are dead");
        table.finish_compaction().expect("switch");
        let state = table.compaction_state();
        assert_eq!((state.gen, state.dead), (1, 0));
        assert!(state.data_len < 64 << 10);
        assert_matches(&table, &model);
    }

    /// A requested compaction skips a table without dead puts.
    #[test]
    fn test_tntable_requested_compaction_needs_garbage() {
        let tmp = TempDir::with_prefix("tntable_compact_requested").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 };
        let table = TnTable::open_with(tmp.path().join("t"), None, config).expect("open");
        let mut model = Model::new();
        let tag = "x".repeat(1_000);
        for i in 0..1_500 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        assert!(!table.compact(), "no dead puts");
        put(&table, &mut model, 3, "y");
        table.flush().expect("flush");
        assert!(table.compact(), "a dead put");
        table.finish_compaction().expect("switch");
        assert_eq!(table.compaction_state().gen, 1);
        assert_matches(&table, &model);
    }

    /// The copy is paced: copying ~3 MiB at 4 MiB/s waits for at least two batches' worth.
    #[test]
    fn test_tntable_compaction_is_paced() {
        let tmp = TempDir::with_prefix("tntable_compact_paced").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 4 << 20 };
        let table = TnTable::open_with(tmp.path().join("t"), None, config).expect("open");
        let mut model = Model::new();
        let tag = "x".repeat(1_000);
        for i in 0..3_000 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        let started = std::time::Instant::now();
        table.compact_now().expect("compact");
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(450),
            "{:?}",
            started.elapsed()
        );
        assert_matches(&table, &model);
    }

    /// Only the copy is paced: the catch-up replays what was committed during it at full speed (a
    /// catch-up paced below the writer's rate would never finish). The ~2 MiB committed here
    /// during the copy would take minutes to replay at 16 KiB/s.
    #[test]
    fn test_tntable_compaction_catch_up_is_not_paced() {
        let tmp = TempDir::with_prefix("tntable_compact_catch_up").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 16 << 10 };
        let table = Arc::new(TnTable::open_with(tmp.path().join("t"), None, config).expect("open"));
        let mut model = Model::new();
        let tag = "x".repeat(1_000);
        for i in 0..10 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        let (gate, arrivals) = table.start_compaction_observed();
        assert_eq!(arrivals.recv_timeout(Duration::from_secs(30)), Ok(1), "the copy is done");
        for i in 10..2_100 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        drop(gate);

        let (done, finished) = std::sync::mpsc::channel();
        let finishing = Arc::clone(&table);
        std::thread::spawn(move || {
            let _ = done.send(finishing.finish_compaction());
        });
        let switched =
            finished.recv_timeout(Duration::from_secs(10)).expect("the catch-up was paced");
        switched.expect("switch");
        assert_eq!(table.compaction_state().gen, 1);
        assert_matches(&table, &model);
    }

    /// A switch that fails after its rename stops the writer (the next open picks the compacted
    /// generation, which holds every committed row); the reopen has them all.
    #[test]
    fn test_tntable_compaction_switch_failing_after_rename_latches() {
        let tmp = TempDir::with_prefix("tntable_compact_latch").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..300 {
            put(&table, &mut model, i, "a");
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        table.fail_next_switch_sync();
        assert!(table.compact_now().is_err(), "the injected sync failure surfaces");
        assert!(table.insert(&kv(1).0, &kv(1).1).is_err(), "writes are refused");
        drop(table);

        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert_eq!(list_gens(&dir).expect("list"), vec![1]);
        assert_matches(&table, &model);
    }

    /// Random puts, removes, clears, commits, compactions (started, and switched to at a later
    /// commit), crashes and clean reopens: the table always reads its committed state.
    #[test]
    fn test_tntable_compaction_random_against_model() {
        use rand::{rngs::StdRng, Rng as _, SeedableRng as _};

        let tmp = TempDir::with_prefix("tntable_compact_random").expect("temp dir");
        let dir = tmp.path().join("t");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 };
        let open = || TnTable::open_with(dir.clone(), None, config).expect("open");
        let mut rng = StdRng::seed_from_u64(0xC0_4AC7);
        let mut table = open();
        let (mut committed, mut working) = (Model::new(), Model::new());
        let mut switches = 0;
        for step in 0..600 {
            match rng.random_range(0..100) {
                0..45 => {
                    let i = rng.random_range(0..300);
                    let tag = "v".repeat(rng.random_range(1..200));
                    put(&table, &mut working, i, &tag);
                }
                45..65 => del(&table, &mut working, rng.random_range(0..300)),
                65..67 => {
                    // A clear is durable at once (it changes generation); the writes before it
                    // that were never committed are gone with the old generation.
                    table.clear().expect("clear");
                    committed.clear();
                    working.clear();
                }
                67..80 => {
                    table.flush().expect("flush");
                    committed = working.clone();
                    assert_matches(&table, &committed);
                }
                80..86 => {
                    table.inner.writer.lock().start_compaction(table.inner.published.load_full());
                }
                86..92 => {
                    if table.compaction_state().running {
                        let gen = table.compaction_state().gen;
                        table.finish_compaction().unwrap_or_else(|e| panic!("step {step}: {e}"));
                        assert_eq!(table.compaction_state().gen, gen + 1);
                        switches += 1;
                        committed = working.clone();
                        assert_matches(&table, &committed);
                    }
                }
                92..96 => {
                    // No compaction thread may read the logs the reopen recovers.
                    table.wait_compaction_thread();
                    crash(table);
                    table = open();
                    working = committed.clone();
                    assert_matches(&table, &committed);
                }
                _ => {
                    drop(table); // commits what is uncommitted
                    committed = working.clone();
                    table = open();
                    assert_matches(&table, &committed);
                }
            }
        }
        assert!(switches > 5, "{switches} switches");
    }

    // ---- compaction: failure, timing and lifetime edges ----

    /// A churned table under a small automatic threshold: every key overwritten once, so the
    /// trigger is due at the next commit.
    fn churned_table(dir: &Path, config: CompactionConfig) -> (TnTable, Model) {
        let table = TnTable::open_with(dir.to_path_buf(), None, config).expect("open");
        let mut model = Model::new();
        let tag = "x".repeat(200);
        for round in 0..2 {
            for i in 0..400 {
                put(&table, &mut model, i, &format!("{round}{tag}"));
            }
        }
        (table, model)
    }

    /// A compaction that fails is not retried at every following commit: the automatic trigger
    /// backs off, and retries once the backoff is lifted.
    #[test]
    fn test_tntable_failed_automatic_compaction_backs_off() {
        let tmp = TempDir::with_prefix("tntable_compact_backoff").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: Some(64 << 10), bytes_per_sec: 0 };
        let (table, mut model) = churned_table(&tmp.path().join("t"), config);
        table.fail_compactions(true);
        for i in 0..5 {
            // Overwrites: the trigger stays due (dead puts keep pace with live rows).
            table.wait_compaction_thread();
            put(&table, &mut model, i, "y");
            table.flush().expect("flush");
        }
        assert_eq!(table.compaction_state().started, 1, "one attempt, then the backoff");

        table.fail_compactions(false);
        table.clear_compaction_backoff();
        table.flush().expect("flush");
        assert_eq!(table.compaction_state().started, 2, "retried once the backoff is lifted");
        table.finish_compaction().expect("switch");
        assert_eq!(table.compaction_state().gen, 1);
        assert_matches(&table, &model);
    }

    /// What is committed while the thread syncs its bulk copy is caught up by the thread, not left
    /// to the switch under the writer lock.
    #[test]
    fn test_tntable_compaction_switch_tail_excludes_bulk_sync() {
        let tmp = TempDir::with_prefix("tntable_compact_tail").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
        let mut model = Model::new();
        for i in 0..500 {
            put(&table, &mut model, i, "a");
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        let (gate, arrivals) = table.start_compaction_observed();
        let wait = |n| assert_eq!(arrivals.recv_timeout(Duration::from_secs(30)), Ok(n));
        wait(1); // copied
        gate.send(()).expect("resume");
        wait(2); // one (empty) catch-up round
        gate.send(()).expect("resume");
        wait(3); // the bulk sync is done: commits land here
        for i in 0..200 {
            put(&table, &mut model, i, "c");
        }
        table.flush().expect("flush");
        drop(gate);
        table.finish_compaction().expect("switch");
        assert_eq!(table.compaction_state().last_switch_tail, 0, "nothing left to the lock");
        assert_matches(&table, &model);
    }

    /// A compaction that finished just before a clear is discarded at once, not kept on disk
    /// until the next compaction or close.
    #[test]
    fn test_tntable_clear_discards_finished_compaction() {
        let tmp = TempDir::with_prefix("tntable_compact_finished_clear").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone(), None).expect("open");
        let mut model = Model::new();
        for i in 0..500 {
            put(&table, &mut model, i, "a");
            put(&table, &mut model, i, "b");
        }
        table.flush().expect("flush");
        drop(table.start_compaction_gated());
        table.wait_compaction_thread();
        assert_eq!(compact_dirs(&dir), 1);
        table.clear().expect("clear");
        table.flush().expect("flush");

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while compact_dirs(&dir) != 0 {
            assert!(std::time::Instant::now() < deadline, "the finished compaction is discarded");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(table.is_empty().expect("is_empty"));
    }

    /// The dead-put count survives a clean close: a reopened table full of dead records still
    /// compacts on its own.
    #[test]
    fn test_tntable_dead_count_survives_clean_reopen() {
        let tmp = TempDir::with_prefix("tntable_dead_count").expect("temp dir");
        let dir = tmp.path().join("t");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 0 };
        let (table, model) = churned_table(&dir, config);
        table.flush().expect("flush");
        let dead = table.compaction_state().dead;
        assert_eq!(dead, 400);
        drop(table);

        let config = CompactionConfig { auto_min_bytes: Some(64 << 10), bytes_per_sec: 0 };
        let table = TnTable::open_with(dir, None, config).expect("reopen");
        assert!(!table.rebuilt_on_open());
        assert_eq!(table.compaction_state().dead, dead, "the count is kept");
        table.flush().expect("flush");
        assert!(table.compaction_state().running, "the automatic trigger fires");
        table.finish_compaction().expect("switch");
        assert_matches(&table, &model);
    }

    /// A compaction does not start without room for the new generation beside the old one.
    #[test]
    fn test_tntable_compaction_needs_disk_space() {
        let tmp = TempDir::with_prefix("tntable_compact_space").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: Some(64 << 10), bytes_per_sec: 0 };
        let (table, model) = churned_table(&tmp.path().join("t"), config);
        table.set_free_space_for_test(Some(1 << 20));
        table.flush().expect("flush");
        assert!(!table.compaction_state().running, "automatic: no room");
        assert!(!table.compact(), "requested: no room");
        assert!(table.compact_now().is_err(), "immediate: no room");

        table.set_free_space_for_test(None);
        table.compact_now().expect("room again");
        assert_eq!(table.compaction_state().gen, 1);
        assert_matches(&table, &model);
    }

    /// A close does not wait out a paced compaction's sleep: it stops the thread promptly.
    #[test]
    fn test_tntable_close_interrupts_paced_compaction() {
        let tmp = TempDir::with_prefix("tntable_compact_close").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: None, bytes_per_sec: 1 << 10 };
        let table = TnTable::open_with(tmp.path().join("t"), None, config).expect("open");
        let tag = "x".repeat(1_000);
        let mut model = Model::new();
        for i in 0..1_500 {
            put(&table, &mut model, i, &tag);
        }
        table.flush().expect("flush");
        assert!(table.inner.writer.lock().start_compaction(table.inner.published.load_full()));
        // The first megabyte copies at once; then the thread sleeps toward 1 KiB/s.
        std::thread::sleep(Duration::from_millis(300));
        let (closed, done) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(table);
            let _ = closed.send(());
        });
        assert!(done.recv_timeout(Duration::from_secs(3)).is_ok(), "the close stopped the thread");
    }

    /// Readers on other threads keep reading consistent values (and each scan one committed
    /// state) while automatic compactions switch generations under them.
    #[test]
    fn test_tntable_readers_across_compaction_switches() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        const KEYS: u64 = 400;
        let value = |i: u64, generation: u64| {
            let mut v = generation.to_le_bytes().to_vec();
            v.extend_from_slice(&i.to_le_bytes());
            v.resize(256, (i % 251) as u8);
            v
        };
        let tmp = TempDir::with_prefix("tntable_compact_readers_mt").expect("temp dir");
        let config = CompactionConfig { auto_min_bytes: Some(256 << 10), bytes_per_sec: 0 };
        let table = TnTable::open_with(tmp.path().join("t"), None, config).expect("open");
        for i in 0..KEYS {
            table.insert(&kv(i).0, &value(i, 0)).expect("insert");
        }
        table.flush().expect("flush");
        let committed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4u64)
            .map(|t| {
                let (table, committed, stop) = (table.clone(), committed.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut x = t + 1;
                    let mut reads = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let floor = committed.load(Ordering::Acquire);
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let i = (x >> 33) % KEYS;
                        let v = table.get_with(&kv(i).0, |b| b.to_vec()).expect("get");
                        let v = v.expect("every key is always present");
                        let generation = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                        assert!(generation >= floor, "read generation {generation} < {floor}");
                        assert_eq!(v, value(i, generation), "a torn or foreign value");
                        if reads.is_multiple_of(32) {
                            let mut scan = table.scan(ScanKind::Forward);
                            let mut gens = Vec::new();
                            while let Some((ok, g)) = scan.next_with(|k, v| {
                                let g = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                                (v == value(key_u64(k), g).as_slice(), g)
                            }) {
                                assert!(ok, "a torn value in a scan");
                                gens.push(g);
                                if t == 0 && gens.len() == KEYS as usize / 2 {
                                    std::thread::sleep(Duration::from_millis(2));
                                }
                            }
                            assert_eq!(gens.len(), KEYS as usize);
                            assert!(gens.iter().all(|&g| g == gens[0]), "a scan mixed states");
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        let mut generation = 0;
        while table.compaction_state().gen < 3 {
            generation += 1;
            assert!(generation < 5_000, "three switches");
            for i in 0..KEYS {
                table.insert(&kv(i).0, &value(i, generation)).expect("overwrite");
            }
            table.flush().expect("commit");
            committed.store(generation, Ordering::Release);
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            assert!(reader.join().expect("reader") > 0);
        }
    }
}
