//! The environment: one page file opened, locked and recovered ([`Env::open`]), the single
//! writer's state handed from one write transaction to the next, the snapshots readers start
//! from, and the commit protocol's durable steps.
//!
//! ## Open and recovery
//!
//! A missing or empty file is created atomically: both metas (txn 0, no tables) are written to
//! `<path>.creating`, synced, and renamed over the path. The file is then locked
//! (`flock(LOCK_EX | LOCK_NB)`, so a second open fails with [`Error::Locked`], in this process
//! too), and the newest valid meta chosen ([`choose_meta`]). Open never writes to an existing
//! file: a file that is shorter than the chosen meta's page count, not a tndb-b file, or whose
//! catalog or free-tree root fails its checks fails closed, unchanged.
//!
//! ## Commits
//!
//! A commit (see `RwTxn::commit`) stamps the CRC of every page the transaction wrote, makes
//! them durable (`msync` over coalesced ranges, or one `fdatasync` of the file when the pages are
//! scattered, then an `fdatasync` if the file grew and it is still needed), and only
//! then writes the new meta over the slot holding the lower txn and syncs that one page. A crash
//! before the meta's sync recovers the previous state; after it, the new one. In
//! [`SyncMode::NoSync`] a commit only stamps and publishes in memory; [`Env::sync`] (or the
//! environment's drop) runs the durable steps for the newest state.
//!
//! ## Readers
//!
//! Each commit publishes an immutable [`Snapshot`] (its meta and the mapping's reader view);
//! [`Env::begin_ro`] clones the current one, so a reader sees one consistent state of every
//! table for as long as it lives. The writer keeps a weak reference to every snapshot it
//! published, oldest first, to learn the oldest state a reader may still be reading
//! ([`Writer::oldest_reader_txn`]): a page freed after that state must not be reused yet.
//!
//! ## Page reuse
//!
//! Pages a commit freed are listed in the free tree ([`crate::freelist`]) and reused once no
//! durable meta and no live reader can reach them ([`Writer::reuse_gate`]).

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io,
    ops::Range,
    os::unix::fs::{FileExt as _, MetadataExt as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{fence, Ordering},
        Arc, Weak,
    },
};

use parking_lot::{Condvar, Mutex, RwLock};

use crate::{
    alloc::Allocator,
    check::{self, CheckReport},
    crc::{self, CrcState},
    error::{Error, Result},
    file::{MapView, PageFile, DEFAULT_RESERVE},
    freelist,
    latch::{Latch, UnwindLatch},
    meta::{choose_meta, slot_to_write, Meta, META_LEN},
    page::{self, Geometry, Kind},
    page_set::PageSet,
    sys,
    tree::{
        read::{checked_child, checked_page},
        write::Scratch,
        PageRead, PageWrite, TreeRoot,
    },
    txn::{RoTxn, RwTxn},
};

/// Runs of dirty pages separated by at most this many clean pages are synced by one `msync` (the
/// clean pages between cost the kernel a page-table walk, not a write).
const MERGE_GAP: u32 = 64;

/// A commit whose dirty pages form more runs of adjacent pages than this makes them durable with
/// one `fdatasync` of the file instead of one `msync` per range ([`data_sync`]).
const MAX_RANGES: usize = 16;

/// How step 4 makes a commit's dirty pages durable ([`data_sync`]).
#[derive(Debug, PartialEq, Eq)]
enum DataSync {
    /// One `msync(MS_SYNC)` per byte range.
    Ranges(Vec<Range<u64>>),
    /// One `fdatasync` of the whole file.
    File,
}

/// When a commit becomes durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// A commit is durable when it returns: its pages, then its meta page, are synced.
    #[default]
    Durable,
    /// A commit is visible in this process at once, and durable at the next [`Env::sync`] (or
    /// when the environment drops). A crash, even of the process alone, rolls back to the last
    /// sync; the file stays consistent, since the durable state's pages are never overwritten.
    NoSync,
}

/// How [`Env::open`] opens (or creates) a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvOptions {
    /// The page size of a new file: a power of two in 4 KiB..=64 KiB (default 4096). An
    /// existing file keeps the page size it was created with.
    pub page_size: u32,
    /// Address space the mapping reserves, in bytes (default 1 GiB). The file grows inside it
    /// without remapping; past it the mapping moves to a larger reservation.
    pub map_reserve: u64,
    /// When commits become durable (default [`SyncMode::Durable`]).
    pub sync: SyncMode,
}

impl Default for EnvOptions {
    fn default() -> Self {
        Self { page_size: 4096, map_reserve: DEFAULT_RESERVE, sync: SyncMode::Durable }
    }
}

/// Figures about the environment's newest committed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvStat {
    /// The page size.
    pub page_size: u32,
    /// The id of the newest committed transaction (durable or, in [`SyncMode::NoSync`], not
    /// yet).
    pub txn_id: u64,
    /// The pages allocated: `0..page_count`.
    pub page_count: u32,
    /// The number of tables.
    pub tables: u64,
}

/// An open database file (see the module docs). Cheap to clone: clones share one environment,
/// which closes when the last clone and the last transaction drop.
#[derive(Clone)]
pub struct Env {
    inner: Arc<EnvInner>,
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env").field("stat", &self.stat()).finish_non_exhaustive()
    }
}

/// One committed state as readers see it: its meta (txn, catalog and free-tree roots, page
/// count) and the mapping's reader view.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// The state's meta.
    pub(crate) meta: Meta,
    /// The page geometry.
    pub(crate) geometry: Geometry,
    /// The mapping, read without a lock.
    pub(crate) view: Arc<MapView>,
}

impl PageRead for Snapshot {
    fn geometry(&self) -> Geometry {
        self.geometry
    }

    fn page_count(&self) -> u32 {
        self.meta.page_count
    }

    fn page(&self, pgno: u32) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_page(pgno, self.meta.page_count, self.meta.txn, false, |p| {
            self.view.slice(u64::from(p) * size as u64, size)
        })
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_child(pgno, want_txn, self.meta.page_count, self.meta.txn, false, |p| {
            self.view.slice(u64::from(p) * size as u64, size)
        })
    }
}

/// What every handle and transaction of one environment shares.
#[derive(Debug)]
pub(crate) struct EnvInner {
    /// The page geometry.
    pub(crate) geometry: Geometry,
    /// When commits become durable.
    pub(crate) sync_mode: SyncMode,
    /// The failure latch: once set, writes and commits fail.
    pub(crate) latch: Latch,
    /// The newest committed state, which a new reader starts from.
    current: RwLock<Arc<Snapshot>>,
    /// The writer's state while no write transaction holds it.
    writer: Mutex<Option<Box<Writer>>>,
    /// Signalled whenever the writer's state comes back.
    writer_back: Condvar,
    /// Test-only: drop as a crash would, without the final sync.
    #[cfg(test)]
    pub(crate) crash_on_drop: std::sync::atomic::AtomicBool,
}

impl EnvInner {
    /// The newest committed state.
    pub(crate) fn current(&self) -> Arc<Snapshot> {
        Arc::clone(&self.current.read())
    }

    /// Take the writer's state, waiting while a write transaction holds it (with `wait`), else
    /// `None` if it is held.
    fn take_writer(&self, wait: bool) -> Option<Box<Writer>> {
        let mut slot = self.writer.lock();
        loop {
            if let Some(writer) = slot.take() {
                return Some(writer);
            }
            if !wait {
                return None;
            }
            self.writer_back.wait(&mut slot);
        }
    }

    /// Run `f` on the writer's state, waiting while a write transaction holds it. The state goes
    /// back to its slot even if `f` panics: the page file inside must outlive every reader.
    fn with_writer<T>(&self, f: impl FnOnce(&mut Writer) -> T) -> T {
        struct Back<'a>(&'a EnvInner, Option<Box<Writer>>);
        impl Drop for Back<'_> {
            fn drop(&mut self) {
                if let Some(writer) = self.1.take() {
                    self.0.return_writer(writer);
                }
            }
        }
        let mut back = Back(self, self.take_writer(true));
        f(back.1.as_deref_mut().expect("a waiting take returns the writer"))
    }

    /// Give the writer's state back and wake one waiter.
    pub(crate) fn return_writer(&self, writer: Box<Writer>) {
        *self.writer.lock() = Some(writer);
        self.writer_back.notify_one();
    }
}

impl Drop for EnvInner {
    fn drop(&mut self) {
        #[cfg(test)]
        if self.crash_on_drop.load(Ordering::Relaxed) {
            return;
        }
        if self.sync_mode != SyncMode::NoSync || self.latch.is_failed() {
            return;
        }
        // Every transaction holds a reference to this, so the writer's state is home.
        if let Some(writer) = self.writer.get_mut().as_mut() {
            if let Err(e) = writer.sync(&self.latch) {
                tracing::error!(target: "tndb_b", "the final sync failed: {e}");
            }
        }
    }
}

/// The single writer's state, held by one write transaction at a time: the page file, the
/// page allocator, the newest committed state, what the meta slots hold on disk, and the
/// snapshots published to readers.
#[derive(Debug)]
pub(crate) struct Writer {
    /// The page file (only the writer changes it).
    pub(crate) file: PageFile,
    /// The page geometry.
    pub(crate) geometry: Geometry,
    /// The page allocator of the writing transaction.
    pub(crate) alloc: Allocator,
    /// The newest committed state (durable or not): the next transaction's base.
    pub(crate) last: Meta,
    /// The txn of the meta each slot holds on disk (`None`: invalid).
    slots: [Option<u64>; 2],
    /// [`SyncMode::NoSync`]: the pages commits stamped since the last sync.
    unsynced: PageSet,
    /// The snapshots published, oldest first (see [`Self::oldest_reader_txn`]).
    readers: VecDeque<(u64, Weak<Snapshot>)>,
    /// The queue's length after its last full pass (see [`Self::prune_readers`]).
    readers_kept: usize,
    /// The tree writes' reused buffers ([`PageWrite::take_scratch`]).
    scratch: Scratch,
    /// Test-only: the most rounds a commit's free-list save took.
    #[cfg(test)]
    pub(crate) max_save_rounds: u32,
    /// Test-only: the commit point at which the next commit or sync stops (see
    /// [`Self::crash_point`]).
    #[cfg(test)]
    pub(crate) crash_at: Option<CrashAt>,
    /// Test-only: the data syncs (msynced ranges, or the one `fdatasync`) of the latest durable
    /// steps.
    #[cfg(test)]
    pub(crate) data_msyncs: usize,
    /// Test-only: the latest durable steps synced the data with one `fdatasync` of the file.
    #[cfg(test)]
    pub(crate) data_by_file: bool,
    /// Test-only (a benchmark comparison): step 4 makes the data pages durable with one
    /// `fdatasync` of the whole file for every commit, not only those [`data_sync`] picks it for.
    #[cfg(test)]
    pub(crate) data_by_fdatasync: bool,
    /// Test-only: table puts take the descent every time: each put empties its table's
    /// write-path cache (`write::PathCache`) first, so none is used.
    #[cfg(test)]
    pub(crate) no_path_cache: bool,
}

/// Test-only: a point of the commit protocol (see the env module docs) at which
/// [`Writer::crash_point`] stops a commit or sync as a process death there would: no further
/// I/O, an error that latches the environment.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CrashAt {
    /// Before step 1 (the catalog's roots and the free list's save).
    BeforeSave,
    /// After the free list's save.
    AfterSave,
    /// After step 3 (the pages' CRCs stamped): the last point of a `SyncMode::NoSync` commit.
    AfterStamp,
    /// After the msync of the `k`-th data range (from 1) of step 4, or (`k` = 1) after the one
    /// `fdatasync` that replaces them.
    DataMsync(usize),
    /// After step 5 (the size sync, whether or not the file grew).
    AfterSizeSync,
    /// After the meta's copy into its slot, before its msync (inside step 6).
    AfterMetaCopy,
    /// After the meta's msync (step 6 complete), before publication.
    AfterMetaMsync,
}

impl Writer {
    /// Stamp the CRC of every page the transaction wrote (commit step 3) and return them,
    /// ascending; they then count as stamped by this writer.
    pub(crate) fn stamp_private(&mut self) -> Result<Vec<u32>> {
        let size = self.geometry.page_size();
        let pages = self.alloc.stamp_private();
        for &p in &pages {
            crc::add_crc32_nonzero(self.file.page_mut(p, size)?);
        }
        Ok(pages)
    }

    /// Record pages stamped by a [`SyncMode::NoSync`] commit, for the next sync.
    pub(crate) fn defer_sync(&mut self, pages: &[u32]) {
        for &p in pages {
            self.unsynced.insert(p);
        }
    }

    /// The durable steps of a commit (4–6): sync `pages` (ascending), the file's size if it
    /// grew, then write `meta` over the slot holding the lower txn and sync that page.
    pub(crate) fn make_durable(&mut self, pages: &[u32], meta: &Meta) -> Result<()> {
        let size = self.geometry.page_size();
        #[cfg(test)]
        {
            self.data_msyncs = 0;
            self.data_by_file = false;
        }
        let plan = data_sync(pages, size as u64);
        #[cfg(test)]
        let plan = if self.data_by_fdatasync { DataSync::File } else { plan };
        match plan {
            DataSync::Ranges(ranges) => {
                for range in ranges {
                    self.file.msync_ranges(std::slice::from_ref(&range))?;
                    #[cfg(test)]
                    {
                        self.data_msyncs += 1;
                        self.crash_point(CrashAt::DataMsync(self.data_msyncs))?;
                    }
                }
            }
            DataSync::File => {
                self.file.fdatasync_all()?;
                #[cfg(test)]
                {
                    self.data_by_file = true;
                    self.data_msyncs = 1;
                    self.crash_point(CrashAt::DataMsync(1))?;
                }
            }
        }
        self.file.sync_size_if_grown()?;
        #[cfg(test)]
        self.crash_point(CrashAt::AfterSizeSync)?;
        let slot = slot_to_write(&self.slots);
        self.file.page_mut(slot as u32, size)?[..META_LEN].copy_from_slice(&meta.encode());
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaCopy)?;
        self.file.msync_page(slot as u32, size)?;
        self.slots[slot] = Some(meta.txn);
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaMsync)?;
        Ok(())
    }

    /// Test-only: stop here if [`Self::crash_at`] names `at` (once): the caller returns the
    /// error, so the commit does no further I/O and latches the environment, as if the process
    /// had died at `at`.
    #[cfg(test)]
    pub(crate) fn crash_point(&mut self, at: CrashAt) -> Result<()> {
        if self.crash_at != Some(at) {
            return Ok(());
        }
        self.crash_at = None;
        Err(Error::Failed(format!("injected crash at {at:?}")))
    }

    /// Make the newest committed state durable if a [`SyncMode::NoSync`] commit left it in
    /// memory only. A failure latches. The caller checks the latch first.
    fn sync(&mut self, latch: &Latch) -> Result<()> {
        let durable = self.slots.iter().flatten().copied().max();
        if durable.is_some_and(|txn| txn >= self.last.txn) {
            return Ok(());
        }
        let _guard = UnwindLatch(latch, "a sync panicked");
        let pages: Vec<u32> = self.unsynced.drain().map(|p| p as u32).collect();
        let last = self.last;
        self.make_durable(&pages, &last).inspect_err(|e| latch.fail(format!("sync: {e}")))
    }

    /// Publish `meta` as the newest committed state (commit step 7): readers that begin from
    /// now on see it.
    pub(crate) fn publish(&mut self, inner: &EnvInner, meta: Meta) {
        let view = self.file.view();
        view.publish_len(u64::from(meta.page_count) * self.geometry.page_size() as u64);
        let snapshot = Arc::new(Snapshot { meta, geometry: self.geometry, view });
        self.readers.push_back((meta.txn, Arc::downgrade(&snapshot)));
        self.last = meta;
        *inner.current.write() = snapshot;
        self.prune_readers();
    }

    /// Drop the snapshots no reader holds from the front of the queue. A reader stuck on an old
    /// snapshot keeps the front, so the dead entries behind it are also dropped, in a pass each
    /// time the queue doubles: it stays bounded by the live snapshots.
    fn prune_readers(&mut self) {
        while self.readers.front().is_some_and(|(_, r)| r.strong_count() == 0) {
            self.readers.pop_front();
        }
        if self.readers.len() > 2 * self.readers_kept.max(16) {
            self.readers.retain(|(_, r)| r.strong_count() > 0);
            self.readers_kept = self.readers.len();
        }
    }

    /// The txn of the oldest snapshot a reader may still be reading (the current one at least:
    /// the environment holds it). A page freed by a later transaction is unreachable from every
    /// live reader's snapshot.
    pub(crate) fn oldest_reader_txn(&mut self) -> u64 {
        self.prune_readers();
        let txn = self.readers.front().map_or(self.last.txn, |&(txn, _)| txn);
        // Pairs with the `Release` decrement of each snapshot's last `Arc` drop, so a reader's
        // final reads of a page happen before this writer reuses it.
        fence(Ordering::Acquire);
        txn
    }

    /// The reuse gate: the latest txn whose free records may be reused, the lowest of the txns
    /// the valid meta slots hold on disk and of the oldest snapshot a reader may hold. A page
    /// freed by a transaction no later than it is unreachable from both durable states (whichever
    /// a crash recovers) and from every live snapshot. In [`SyncMode::NoSync`] the slots move
    /// only at a sync.
    pub(crate) fn reuse_gate(&mut self) -> u64 {
        let durable = self.slots.iter().flatten().copied().min().unwrap_or(0);
        durable.min(self.oldest_reader_txn())
    }

    /// Refill the empty pool from the free list of the transaction's base state: records are
    /// taken smallest first while their txn passes [`Self::reuse_gate`], until one lists a page
    /// (records may be empty). The first record past the gate, or the end of the list, stops
    /// loading for the rest of the transaction. A damaged record is [`Error::Corrupt`] and none
    /// of its pages is taken.
    fn refill(&mut self) -> Result<()> {
        let gate = self.reuse_gate();
        let (free, page_count) = (self.last.free, self.last.page_count);
        while self.alloc.wants_refill() {
            let after = self.alloc.last_consumed();
            match freelist::next_record(&*self, &free, after)? {
                Some((key, value)) if key.txn <= gate => {
                    let pages = freelist::decode_pages(value, page_count)?;
                    self.alloc.load_record(key, pages)?;
                }
                _ => self.alloc.stop_loading(),
            }
        }
        Ok(())
    }

    /// Test-only: the txns the meta slots hold on disk.
    #[cfg(test)]
    pub(crate) fn slots(&self) -> [Option<u64>; 2] {
        self.slots
    }
}

impl PageRead for Writer {
    fn geometry(&self) -> Geometry {
        self.geometry
    }

    fn page_count(&self) -> u32 {
        self.alloc.page_count()
    }

    fn page(&self, pgno: u32) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_page(
            pgno,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private(pgno),
            |p| self.file.page(p, size),
        )
    }

    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_child(
            pgno,
            want_txn,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private(pgno),
            |p| self.file.page(p, size),
        )
    }
}

impl PageWrite for Writer {
    fn txn(&self) -> u64 {
        self.alloc.txn()
    }

    fn is_private(&self, pgno: u32) -> bool {
        self.alloc.is_private(pgno)
    }

    fn is_stamped(&self, pgno: u32) -> bool {
        self.alloc.is_stamped(pgno)
    }

    fn private_page(&self, pgno: u32) -> Result<&[u8]> {
        self.file
            .page(pgno, self.geometry.page_size())
            .ok_or_else(|| Error::Corrupt(format!("private page {pgno} is not mapped")))
    }

    fn epoch(&self) -> u64 {
        self.alloc.epoch()
    }

    fn alloc(&mut self) -> Result<u32> {
        if self.alloc.wants_refill() {
            self.refill()?;
        }
        let size = self.geometry.page_size();
        let file = &mut self.file;
        let p = self.alloc.allocate(|count| file.ensure_len(u64::from(count) * size as u64))?;
        self.file.page_mut(p, size)?.fill(0);
        Ok(p)
    }

    #[inline]
    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]> {
        if !self.alloc.is_private(pgno) {
            return Err(not_private(pgno));
        }
        self.file.page_mut(pgno, self.geometry.page_size())
    }

    fn free(&mut self, pgno: u32) {
        self.alloc.free(pgno);
    }

    fn take_scratch(&mut self) -> Scratch {
        std::mem::take(&mut self.scratch)
    }

    fn put_scratch(&mut self, scratch: Scratch) {
        self.scratch = scratch;
    }
}

/// [`Writer::page_mut`]'s error for a page that is not private.
#[cold]
#[inline(never)]
fn not_private(pgno: u32) -> Error {
    Error::Failed(format!("page {pgno} is not private: copy it first"))
}

/// How step 4 syncs dirty `pages` (ascending). Pages forming at most [`MAX_RANGES`] runs of
/// adjacent pages get one `msync` per range, runs separated by at most [`MERGE_GAP`] clean pages
/// merged. More runs (a commit whose copies took pages freed all over the file) get one
/// `fdatasync` of the file: an msync writes its range's dirty pages one cluster at a time on
/// macOS, about three times the cost of fdatasync's single pass for a commit of a thousand
/// scattered pages, and on Linux every msync runs the file system's fsync path, device flush
/// included, so one fdatasync is one flush instead of many. Either way the data is durable before
/// the meta is written. `fdatasync` also writes any other dirty page of the file, which at a
/// commit can only be a page of an aborted transaction: a free page, so writing it is harmless.
fn data_sync(pages: &[u32], page_size: u64) -> DataSync {
    let adjacent_runs = 1 + pages.windows(2).filter(|w| w[1] != w[0] + 1).count();
    if adjacent_runs > MAX_RANGES {
        return DataSync::File;
    }
    let mut runs: Vec<Range<u32>> = Vec::new();
    for &p in pages {
        match runs.last_mut() {
            Some(run) if p - run.end <= MERGE_GAP => run.end = p + 1,
            _ => runs.push(p..p + 1),
        }
    }
    DataSync::Ranges(
        runs.into_iter()
            .map(|run| u64::from(run.start) * page_size..u64::from(run.end) * page_size)
            .collect(),
    )
}

impl Env {
    /// Open the database file at `path`, creating it if it is missing or empty, and recover the
    /// newest committed state (see the module docs). Errors: [`Error::Locked`] if another handle
    /// holds it open; [`Error::NotTndbB`], [`Error::UnsupportedVersion`] or [`Error::Corrupt`]
    /// for a file that cannot be opened safely (left unchanged); [`Error::InvalidPageSize`] for
    /// an invalid `options.page_size`; [`Error::Io`].
    pub fn open(path: impl AsRef<Path>, options: EnvOptions) -> Result<Env> {
        let path = path.as_ref();
        Geometry::new(options.page_size)?;
        let file = open_locked(path, options.page_size)?;
        let chosen = choose_meta(|offset| read_record(&file, offset))?;
        let meta = chosen.meta;
        let geometry = Geometry::new(meta.page_size)?;
        if meta.txn == u64::MAX {
            return Err(Error::Corrupt("open: the meta's txn id leaves no next txn".to_string()));
        }
        let needed = u64::from(meta.page_count) * u64::from(meta.page_size);
        let len = file.metadata()?.len();
        if len < needed {
            return Err(Error::Corrupt(format!(
                "open: the file is {len} bytes, shorter than the {} pages of its newest meta (txn \
                 {})",
                meta.page_count, meta.txn
            )));
        }
        // A crash may have left a meta in the page cache whose msync never finished: make what
        // this open reads durable before anything is built on it.
        sys::fsync_file(&file)?;
        let file = PageFile::from_file(file, options.map_reserve)?;
        let view = file.view();
        view.publish_len(needed);
        let snapshot = Arc::new(Snapshot { meta, geometry, view });
        check_root(&snapshot, &meta.catalog, "catalog")?;
        check_root(&snapshot, &meta.free, "free tree")?;
        let writer = Writer {
            file,
            geometry,
            alloc: Allocator::new(meta.txn + 1, meta.page_count),
            last: meta,
            slots: chosen.slots,
            unsynced: PageSet::default(),
            readers: VecDeque::from([(meta.txn, Arc::downgrade(&snapshot))]),
            readers_kept: 1,
            scratch: Scratch::default(),
            #[cfg(test)]
            max_save_rounds: 0,
            #[cfg(test)]
            crash_at: None,
            #[cfg(test)]
            data_msyncs: 0,
            #[cfg(test)]
            data_by_file: false,
            #[cfg(test)]
            data_by_fdatasync: false,
            #[cfg(test)]
            no_path_cache: false,
        };
        Ok(Env {
            inner: Arc::new(EnvInner {
                geometry,
                sync_mode: options.sync,
                latch: Latch::default(),
                current: RwLock::new(snapshot),
                writer: Mutex::new(Some(Box::new(writer))),
                writer_back: Condvar::new(),
                #[cfg(test)]
                crash_on_drop: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }

    /// Begin a read-only transaction on the newest committed state. It sees that state of every
    /// table for as long as it lives, whatever commits meanwhile.
    pub fn begin_ro(&self) -> Result<RoTxn> {
        Ok(RoTxn::new(Arc::clone(&self.inner), self.inner.current()))
    }

    /// Begin the write transaction, waiting while another one is open (from any thread: a
    /// thread that holds one and calls this waits forever). [`Error::Failed`] once the
    /// environment has latched a failure.
    pub fn begin_rw(&self) -> Result<RwTxn> {
        self.inner.latch.check()?;
        let writer = self.inner.take_writer(true).expect("a waiting take returns the writer");
        Ok(RwTxn::new(Arc::clone(&self.inner), writer))
    }

    /// Begin the write transaction if no other one is open, else `None`. [`Error::Failed`] once
    /// the environment has latched a failure.
    pub fn try_begin_rw(&self) -> Result<Option<RwTxn>> {
        self.inner.latch.check()?;
        Ok(self.inner.take_writer(false).map(|w| RwTxn::new(Arc::clone(&self.inner), w)))
    }

    /// Make every commit durable: in [`SyncMode::NoSync`], sync the pages committed since the
    /// last sync and write the newest state's meta (a no-op when nothing is pending, and always
    /// in [`SyncMode::Durable`]). Waits while a write transaction is open, as
    /// [`Self::begin_rw`] does. A failure latches the environment.
    pub fn sync(&self) -> Result<()> {
        let latch = &self.inner.latch;
        self.inner.with_writer(|writer| {
            latch.check()?;
            writer.sync(latch)
        })
    }

    /// Check the newest committed state in full: every page of every tree passes its CRC and
    /// structure checks, is the copy its parent names, lies at the depth its kind says, and holds
    /// keys inside its ancestors' separators; every tree holds the entries its root counts; every
    /// free record is well formed; and the pages reachable from the trees and those the free list
    /// lists are disjoint and cover `2..page_count` exactly. Any failure is [`Error::Corrupt`]
    /// naming the damage, or the pages leaked (used by nothing) and used twice, and latches the
    /// environment. It reads through a snapshot, alongside readers and the writer.
    pub fn check(&self) -> Result<CheckReport> {
        let inner = &self.inner;
        check::check_snapshot(&inner.current()).inspect_err(|e| inner.latch.latch_error(e))
    }

    /// Figures about the newest committed state.
    pub fn stat(&self) -> EnvStat {
        let meta = self.inner.current().meta;
        EnvStat {
            page_size: meta.page_size,
            txn_id: meta.txn,
            page_count: meta.page_count,
            tables: meta.catalog.entries,
        }
    }

    /// The first failure the environment latched, if any: from then on writes and commits
    /// return [`Error::Failed`] (reads continue) until the file is reopened.
    pub fn failure(&self) -> Option<String> {
        self.inner.latch.failure()
    }

    /// The longest key this page size accepts.
    pub fn max_key_len(&self) -> usize {
        self.inner.geometry.max_key()
    }

    /// The longest value this page size accepts with a `klen`-byte key, or 0 when `klen` is over
    /// [`max_key_len`](Self::max_key_len): such a key is refused with any value. Every accepted
    /// key leaves room for at least 2 bytes, so 0 always means the key is refused.
    pub fn max_value_len(&self, klen: usize) -> usize {
        self.inner.geometry.max_value(klen)
    }

    /// Test-only: the environment's shared state.
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<EnvInner> {
        &self.inner
    }

    /// Test-only: run `f` on the writer's state (waiting for an open write transaction).
    #[cfg(test)]
    pub(crate) fn with_writer<T>(&self, f: impl FnOnce(&mut Writer) -> T) -> T {
        self.inner.with_writer(f)
    }

    /// Test-only: drop this handle as a crash would, skipping the final sync (the caller holds
    /// no other handle or transaction).
    #[cfg(test)]
    pub(crate) fn crash(self) {
        self.inner.crash_on_drop.store(true, Ordering::Relaxed);
    }
}

/// Lock the database file at `path`, creating it first if it is missing or empty (open steps
/// 2–3). The lock must end up on the file the path names, and that file must not be empty: a
/// creator may rename a new file over the path between this open and its lock, so the open is
/// retried (a few times; then [`Error::Locked`]). A creator still building its file is not waited
/// for: [`create`] returns [`Error::Locked`] at once, that creator holding the database.
fn open_locked(path: &Path, page_size: u32) -> Result<File> {
    for _ in 0..3 {
        if needs_create(path)? {
            create(path, page_size)?;
        }
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if !sys::flock_exclusive(&file)? {
            return Err(Error::Locked);
        }
        let locked = file.metadata()?;
        match fs::metadata(path) {
            Ok(named) if named.ino() == locked.ino() && named.dev() == locked.dev() => {
                if locked.len() > 0 {
                    return Ok(file);
                }
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Err(Error::Locked)
}

/// True if the file at `path` is missing or empty: open creates it.
fn needs_create(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(meta) => Ok(meta.len() == 0),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}

/// Create the file at `path` atomically (open step 2): both metas of a new file (txn 0, no
/// tables) are written to `<path>.creating` (truncating any leftover), synced, and renamed over
/// the path, and the directory is synced. The temporary file is locked while it is built: one
/// another creator holds locked is [`Error::Locked`] at once. A file another handle created
/// meanwhile is kept, and the temporary file is then removed if unused ([`remove_unused`]).
fn create(path: &Path, page_size: u32) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".creating");
    let tmp = PathBuf::from(tmp);
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&tmp)?;
    if !sys::flock_exclusive(&file)? {
        return Err(Error::Locked);
    }
    if !needs_create(path)? {
        remove_unused(&tmp, &file);
        return Ok(());
    }
    file.set_len(0)?;
    let p = page_size as usize;
    let record = Meta::initial(page_size).encode();
    let mut metas = vec![0; 2 * p];
    metas[..META_LEN].copy_from_slice(&record);
    metas[p..p + META_LEN].copy_from_slice(&record);
    file.write_all_at(&metas, 0)?;
    sys::fdatasync_file(&file)?;
    fs::rename(&tmp, path)?;
    let dir = path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."));
    sys::fsync_directory(dir)?;
    Ok(())
}

/// Remove the temporary file `tmp` that [`create`] opened and locked as `file` but did not use,
/// if the name still holds that inode and it is empty; else leave it (best effort: a leftover is
/// harmless, and the next creator truncates it). The check cannot go stale before the unlink:
/// the name changes only by a creator's rename or this unlink, each made under the lock of the
/// inode the name holds, which is held here. (A creator holding an inode the name no longer
/// holds builds, and renames, only while the database is missing or empty, and it is not.)
fn remove_unused(tmp: &Path, file: &File) {
    let (Ok(named), Ok(held)) = (fs::metadata(tmp), file.metadata()) else { return };
    if named.ino() == held.ino() && named.dev() == held.dev() && held.len() == 0 {
        let _ = fs::remove_file(tmp);
    }
}

/// The meta record at `offset` of `file`, or `None` if the file ends before it.
fn read_record(file: &File, offset: u64) -> io::Result<Option<[u8; META_LEN]>> {
    let mut record = [0; META_LEN];
    match file.read_exact_at(&mut record, offset) {
        Ok(()) => Ok(Some(record)),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Check a tree root named by the opened meta (open step 9): the page is a tree page of the
/// file passing its full CRC and structure checks, its header names the root and its txn is the
/// root's and no later than the meta's, and it is a leaf exactly when the tree has one level.
fn check_root(snapshot: &Snapshot, root: &TreeRoot, what: &str) -> Result<()> {
    if root.is_empty() {
        return Ok(());
    }
    let corrupt = |why: String| Error::Corrupt(format!("open: the {what} root: {why}"));
    let page = snapshot.page(root.root).map_err(|e| corrupt(e.to_string()))?;
    if crc::crc_state(page) != CrcState::Valid {
        return Err(corrupt(format!("page {} fails its CRC", root.root)));
    }
    page::validate(page, &snapshot.geometry).map_err(|e| corrupt(e.to_string()))?;
    let txn = page::txn(page);
    if txn as u32 != root.root_txn {
        return Err(corrupt(format!("page {} has txn {txn}, not {}", root.root, root.root_txn)));
    }
    if (page::kind(page)? == Kind::Leaf) != (root.height == 1) {
        return Err(corrupt(format!(
            "page {} is the wrong kind for a tree of height {}",
            root.root, root.height
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
