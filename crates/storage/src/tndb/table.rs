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
//! and a failed recovery leaves the logs as they were.
//!
//! Clearing a table starts a new, empty generation and deletes the old one's files, so cleared
//! data leaves the disk. Snapshots of the old generation keep its files open (its mappings valid)
//! until the last of them drops. The new generation is a spare a background thread prepared (its
//! files created and synced) after the table opened or was last cleared, so a clear only renames it
//! into place and syncs the table directory.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use eyre::{bail, WrapErr as _};
use parking_lot::Mutex;

use super::layout::{gen_dir, list_gens, remove_spares, spare_dir, sync_dir, KeyMode, TableMeta};
use crate::archive::{
    btree_index::{
        index::IndexSnapshot,
        iter::{BtreeCursor, PageSource},
        BtreeIndex,
    },
    data_file::{MapView, MmapFileOptions},
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
    record.get(value_offset..).ok_or(FetchError::CrcFailed)
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

/// Keeps a cleared generation's files open (and so its mappings valid) while snapshots of it are
/// alive: each [`Published`] holds its generation's `GenAlive`, and a clear moves the old files
/// into it, so they close when the last old snapshot drops.
#[derive(Debug, Default)]
struct GenAlive {
    retired: Mutex<Option<GenFiles>>,
}

impl Drop for GenAlive {
    /// Close a cleared generation's files (unmapping them) on a short-lived thread, off the path
    /// of whichever writer or reader dropped the last snapshot; if no thread can be started, here.
    fn drop(&mut self) {
        if let Some(files) = self.retired.get_mut().take() {
            let _ = std::thread::Builder::new().name("tndb-reap".to_string()).spawn(move || {
                drop(files);
            });
        }
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
/// - a sealed (cleanly closed) log must read whole and end committed; an unclean log may end in a
///   tear or uncommitted records, but not below its commit marker (that is damage to committed
///   data, not a crash tail);
/// - a malformed record, or a key function failure, is an error.
fn replay(files: &GenFiles, meta: TableMeta, key_fn: Option<&KeyFn>) -> eyre::Result<Replay> {
    let ksize = meta.ksize as usize;
    let header = DATA_HEADER_BYTES as u64;

    // The data log: puts, made live by the commit record after them.
    let mut rows = BTreeMap::new();
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
                rows.extend(pending.drain(..));
                last_commit = Some(start);
                data_end = iter.logical_position();
            }
        }
    }
    check_log_end("data", &files.data, torn || !pending.is_empty(), data_end)?;

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
    check_log_end("removal", &files.removed, torn || uncommitted, removed_end)?;

    Ok(Replay { rows, data_end, removed_end })
}

/// Fail closed on a log whose replay stopped short of its end (a tear, or records past the last
/// commit) when that cannot be a crash tail: the log was sealed by a clean close, or the stop lies
/// below the log's commit marker.
fn check_log_end(name: &str, log: &Log, short: bool, end: u64) -> eyre::Result<()> {
    if short && !log.opened_unclean() {
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
    /// Set when this open rebuilt the index (for tests).
    #[cfg(test)]
    rebuilt: bool,
    /// Clears that renamed a prepared spare into place (for tests).
    #[cfg(test)]
    clears_from_spare: u32,
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

    fn insert(&mut self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
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
        self.uncommitted = true;
        self.index_mut()?.save(key, pos)?;
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

    /// Remove `key`, logging the removal first (only a present key is logged).
    fn remove(&mut self, key: &[u8]) -> eyre::Result<bool> {
        let Some(idx) = self.files.idx.as_ref() else { return Ok(false) };
        match idx.load(key) {
            Ok(_) => {}
            Err(FetchError::NotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        let data_len = self.files.data.file_len().to_le_bytes();
        self.files.removed.append_raw_parts(&[key, &data_len])?;
        self.removals_unsynced = true;
        self.uncommitted = true;
        self.index_mut()?.remove(key)?;
        Ok(true)
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
        let next = self.gen + 1;
        let files = match self.take_spare() {
            // Its files are already synced: renaming it into place, durably, is the clear.
            Some((dir, files)) => {
                fs::rename(&dir, gen_dir(&self.table_dir, next))?;
                sync_dir(&self.table_dir)?;
                #[cfg(test)]
                {
                    self.clears_from_spare += 1;
                }
                files
            }
            None => {
                let (data, removed) = create_gen(&self.table_dir, next)?;
                GenFiles { data, removed, idx: None }
            }
        };
        let old_dir = std::mem::replace(&mut self.gen_dir, gen_dir(&self.table_dir, next));
        let mut old = std::mem::replace(&mut self.files, files);
        let old_alive = std::mem::take(&mut self.alive);
        self.data_view = self.files.data.view();
        self.gen = next;
        self.uncommitted = false;
        self.removals_unsynced = false;
        // The old files are deleted, not sealed, once the last snapshot of them drops.
        old.data.set_remove_on_drop();
        old.removed.set_remove_on_drop();
        if let Some(idx) = old.idx.as_mut() {
            idx.set_remove_on_drop();
        }
        *old_alive.retired.lock() = Some(old);
        drop(old_alive);
        // The old generation's directory is deleted, and the next spare prepared, in the
        // background.
        self.prepare_next_spare(Some(old_dir));
        Ok(())
    }

    /// Commit: sync the removal log (if it changed), append the commit record, and sync the data
    /// log. A flush with nothing to commit does nothing.
    fn flush(&mut self) -> eyre::Result<()> {
        if !self.uncommitted {
            return Ok(());
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
        self.data_view.publish_len(self.files.data.file_len());
        Published {
            index,
            data_view: Arc::clone(&self.data_view),
            value_offset: self.value_offset(),
            _alive: Arc::clone(&self.alive),
        }
    }

    /// Rebuild the index from the logs (see [`recover`]).
    fn recover(&mut self) -> eyre::Result<()> {
        let replay = replay(&self.files, self.meta, self.key_fn.as_ref())?;
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
        let data_len = self.files.data.file_len();
        if let Some(idx) = self.files.idx.as_mut() {
            idx.set_data_file_length(data_len);
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
    /// A clean close commits whatever is uncommitted (it is sealed into the log either way) and
    /// records the log length the index matches, so the next open needs no rebuild.
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
        if self.uncommitted {
            let _ = self.files.data.append_raw(COMMIT);
        }
        let data_len = self.files.data.file_len();
        if let Some(idx) = self.files.idx.as_mut() {
            idx.set_data_file_length(data_len);
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
    /// Keeps this snapshot's generation open if it is cleared. Declared last so it drops after
    /// the snapshot.
    _alive: Arc<GenAlive>,
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

    fn contains(&self, key: &[u8]) -> bool {
        self.index.as_ref().is_some_and(|index| index.load(key).is_ok())
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
}

/// A cheap `Clone` handle to a table.  Reads take no lock (they load the published snapshot);
/// writes take the writer lock and become readable at the next [`Self::flush`].  The table closes
/// cleanly when the last handle — or the last live [`TableScan`], which holds the table — is
/// dropped.
#[derive(Clone, Debug)]
pub(crate) struct TnTable {
    inner: Arc<Inner>,
}

/// The current generation of the table at `dir`, opened: the newest generation whose logs open,
/// with every older one deleted (more than one exists only after a crash during a clear, which then
/// either finished or never happened); a new table gets generation 0.
fn open_current_gen(dir: &Path) -> eyre::Result<(u64, Log, Log)> {
    let mut gens = list_gens(dir)?;
    let Some(&newest) = gens.last() else {
        let (data, removed) = create_gen(dir, 0)?;
        return Ok((0, data, removed));
    };
    let (current, logs) = match open_logs(&gen_dir(dir, newest)) {
        Ok(logs) => (newest, logs),
        // A newest generation that does not open, beside an older one, is a clear that crashed
        // while creating it: the older generation is still current.
        Err(_) if gens.len() > 1 => {
            fs::remove_dir_all(gen_dir(dir, newest))?;
            gens.pop();
            let current = *gens.last().expect("an older generation");
            (current, open_logs(&gen_dir(dir, current))?)
        }
        Err(e) => return Err(e),
    };
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
    /// error). Rebuilds the index from the logs when the table was not closed cleanly.
    pub(crate) fn open(dir: PathBuf, key_fn: Option<KeyFn>) -> eyre::Result<Self> {
        fs::create_dir_all(&dir)?;
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
            #[cfg(test)]
            rebuilt: false,
            #[cfg(test)]
            clears_from_spare: 0,
        };
        if must_rebuild {
            writer.recover()?;
        }
        writer.prepare_next_spare(None);
        let published = writer.publish();
        Ok(Self {
            inner: Arc::new(Inner {
                writer: Mutex::new(writer),
                published: ArcSwap::from_pointee(published),
            }),
        })
    }

    /// True if this open rebuilt the index from the logs.
    #[cfg(test)]
    pub(crate) fn rebuilt_on_open(&self) -> bool {
        self.inner.writer.lock().rebuilt
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

    /// Commit (see [`Writer::flush`]), then publish every write so far: install a new snapshot for
    /// readers. Readers are never blocked by it (they take no lock); writers wait for it.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        let mut writer = self.inner.writer.lock();
        writer.flush()?;
        // Stored under the writer lock, so snapshots are installed in publish order.
        self.inner.published.store(Arc::new(writer.publish()));
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

    /// True if `key` is present in the published snapshot.
    pub(crate) fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.inner.published.load().contains(key))
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
        let cursor = published.index.as_ref().and_then(|index| kind.cursor(index).ok());
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
        let (key, pos) = kind.cursor(index).ok()?.next(index)?.ok()?;
        Some(f(key, published.value_at(pos).ok()?))
    }
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
        let row = self.cursor.as_mut()?.next(index).and_then(|item| {
            let (key, pos) = item.ok()?;
            Some((key, published.value_at(pos).ok()?))
        });
        match row {
            Some((key, value)) => Some(f(key, value)),
            None => {
                // Exhausted, or a fetch failure ended the scan.
                self.cursor = None;
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
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
        let table = TnTable::open(tmp.path().join("t"), None).expect("open");
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

    /// If the crash interrupted the new generation's creation (its logs do not open), the clear
    /// never happened: the older generation is current and the broken one is deleted.
    #[test]
    fn test_tntable_crash_mid_clear_before_new_generation() {
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
        fs::write(gen_dir(&dir, 1).join("data"), b"half a header").expect("torn header");

        let table = TnTable::open(dir.clone(), None).expect("reopen");
        assert!(!gen_dir(&dir, 1).exists(), "the broken generation is deleted");
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), (0..10).collect::<Vec<_>>());
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
}
