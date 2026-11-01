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
//! [`Env::open_read_only`] opens an existing file no writer holds (`O_RDONLY`, a shared
//! `flock`), chooses the meta and checks the roots exactly as [`Env::open`] does, and maps the
//! file read-only. It never creates, writes or syncs anything; it has no writer, so its snapshot
//! is the one it opened at.
//!
//! ## Commits
//!
//! A commit (see `RwTxn::commit`) stamps the CRC of every page the transaction wrote, makes
//! them durable (one `msync` when they form one range, else one `fdatasync` of the file; see
//! [`data_sync`]), then the file's size if it grew and that sync did not already cover it, and
//! only then writes the new meta over the slot holding the lower txn and syncs that one page. A
//! crash before the meta's sync recovers the previous state; after it, the new one. A durable
//! commit of many pages stamps them in slices and starts each slice's writeback once its CRCs
//! are stored ([`Writer::stamp_sliced`]), so the drive writes while the rest is stamped; the
//! data's durability is still that one sync's. In [`SyncMode::NoSync`] a commit only stamps and
//! publishes in memory; [`Env::sync`] (or the environment's drop) runs the durable steps for the
//! newest state.
//!
//! ## Readers
//!
//! Each commit publishes an immutable [`Snapshot`] (its meta and the mapping's reader view);
//! [`Env::begin_ro`] clones the current one, so a reader sees one consistent state of every
//! table for as long as it lives. A snapshot also remembers the tree roots of the tables its
//! readers resolved ([`TableRoots`]), so each table's catalog lookup runs once per snapshot,
//! not once per transaction. The writer keeps a weak reference to every snapshot it
//! published, oldest first, to learn the oldest state a reader may still be reading
//! ([`Writer::oldest_reader_txn`]): a page freed after that state must not be reused yet.
//! Each snapshot in that queue records when the next commit superseded it (one clock read per
//! commit), and each commit stores the time the oldest live reader's snapshot was superseded,
//! which [`Env::oldest_reader_age`] reads without a lock: a long reader keeps freed pages from
//! reuse, so the file grows, and this lets a caller notice. Readers pay nothing for it.
//!
//! ## Page reuse
//!
//! Pages a commit freed are listed in the free tree ([`crate::freelist`]) and reused once no
//! durable meta and no live reader can reach them ([`Writer::reuse_gate`]).
//!
//! ## I/O counters
//!
//! The writer counts what it does in plain fields of its state ([`Writer::stats`]): each commit
//! step's pages, syncs and time, the copies, checks and allocations of a transaction, and the
//! refills of its pool. [`EnvInner::return_writer`], where the state comes back after every
//! commit, abort and sync, copies them out for [`Env::io_stats`] (see [`IoStats`]).

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io,
    ops::Range,
    os::unix::fs::{FileExt as _, MetadataExt as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{fence, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex, RwLock};

use crate::{
    alloc::{Allocator, PrivatePages, RUN_LOAD_RECORDS},
    check::{self, CheckReport},
    crc::{self, CrcState},
    error::{Error, Result},
    file::{MapView, PageFile, ReadOnlyFile, DEFAULT_RESERVE},
    freelist,
    latch::{Latch, UnwindLatch},
    meta::{choose_meta, slot_to_write, Chosen, Meta, META_LEN},
    overflow::{checked_run, Ovf, RunCrc},
    page::{self, Geometry, Kind},
    page_set::PageSet,
    stamp,
    stats::{elapsed_ns, lap, IoStats},
    sys,
    tree::{
        read::{checked_child, checked_page},
        write::{copy_page, Scratch},
        PageRead, PageWrite, TreeRoot,
    },
    txn::{RoTxn, RwTxn, TableRoots},
};

/// Runs of dirty pages separated by at most this many clean pages merge into one range, which one
/// `msync` can sync (the clean pages between cost the kernel a page-table walk, not a write).
const MERGE_GAP: u32 = 64;

/// A commit whose dirty pages form more runs of adjacent pages than this makes them durable with
/// one `fdatasync` of the file even when the runs merge into one range ([`data_sync`]).
const MAX_RUNS: usize = 16;

/// The bytes of units each slice of a sliced step 3 takes ([`Writer::stamp_sliced`]): a slice
/// closes at the first unit boundary from this many bytes.
const WRITEBACK_SLICE: usize = 16 << 20;

/// The bytes of units from which a [`SyncMode::Durable`] commit's step 3 is sliced
/// ([`Writer::slicing`]): two slices, as one alone would leave its writeback nothing to overlap.
const WRITEBACK_MIN: usize = 2 * WRITEBACK_SLICE;

#[cfg(test)]
thread_local! {
    /// Test-only: the [`Writer::writeback_slice`] each writer opened on this thread starts with
    /// (see [`with_writeback_slice`]).
    static WRITEBACK_SLICE_ON_OPEN: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Test-only: run `f` with every environment it opens on this thread slicing each durable
/// commit's step 3 at `slice` bytes ([`Writer::writeback_slice`]), so a suite that opens its own
/// environments (the crash matrix, the multi-table model) runs the sliced path.
#[cfg(test)]
pub(crate) fn with_writeback_slice<T>(slice: usize, f: impl FnOnce() -> T) -> T {
    /// Puts the thread's previous choice back, also when `f` panics.
    struct Reset(Option<usize>);
    impl Drop for Reset {
        fn drop(&mut self) {
            WRITEBACK_SLICE_ON_OPEN.set(self.0);
        }
    }
    let _reset = Reset(WRITEBACK_SLICE_ON_OPEN.replace(Some(slice)));
    f()
}

/// Test-only: one writeback start of a sliced step 3, as [`Writer::writeback_log`] records it.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WritebackSpan {
    /// The bytes of the file it started.
    pub(crate) bytes: Range<u64>,
    /// The slice's units, `(first page, pages)`, ascending.
    pub(crate) units: Vec<(u32, u32)>,
    /// Every unit of the slice held its CRC when the start was made (`crc::crc_state` valid).
    pub(crate) stamped: bool,
}

/// How step 4 makes a commit's dirty pages durable ([`data_sync`]).
#[derive(Debug, PartialEq, Eq)]
enum DataSync {
    /// No page to sync.
    Nothing,
    /// One `msync(MS_SYNC)` of this byte range.
    Range(Range<u64>),
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

/// How [`Env::open`] opens (or creates) a file. [`Env::open_read_only`] validates `page_size` as
/// [`Env::open`] does and uses `readahead`, and no other field: it maps the file at its length and
/// commits nothing.
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
    /// Let the kernel read ahead on the file's page faults (default `false` on Linux, `true`
    /// elsewhere). With `false` every mapping of the file is advised `MADV_RANDOM`. On Linux a
    /// B+tree's reads are random, so readahead mostly reads pages nothing asks for, and the large
    /// page-cache folios it builds make each page a commit dirties write back its whole folio
    /// (measured on Linux 7.1, ext4: 4.0 KiB written per 4 KiB page dirtied with the advice, 115
    /// KiB after a full cold read with readahead). On macOS a dirtied page writes back its VM page
    /// either way, and the advice measured slower first writes into new file space, so it is off
    /// there. `true` gives no advice, for a workload that scans whole tables cold.
    pub readahead: bool,
}

impl Default for EnvOptions {
    fn default() -> Self {
        Self {
            page_size: 4096,
            map_reserve: DEFAULT_RESERVE,
            sync: SyncMode::Durable,
            readahead: !cfg!(target_os = "linux"),
        }
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
/// count), the mapping's reader view, and the tree roots of the tables its readers resolved.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// The state's meta.
    pub(crate) meta: Meta,
    /// The page geometry.
    pub(crate) geometry: Geometry,
    /// The mapping, read without a lock.
    pub(crate) view: Arc<MapView>,
    /// The tree roots of the tables resolved by the readers of this snapshot (empty when it is
    /// published; a commit publishes a new snapshot).
    pub(crate) tables: TableRoots,
}

impl Snapshot {
    /// The snapshot of the state `meta` names, read through `view`, with no table resolved.
    fn new(meta: Meta, geometry: Geometry, view: Arc<MapView>) -> Self {
        Self { meta, geometry, view, tables: TableRoots::default() }
    }

    /// Page `pgno` again, without the per-fetch checks, for a cursor that fetched it with them
    /// before: the snapshot's pages do not change while it lives. `None` if it is not mapped.
    #[inline]
    pub(crate) fn page_again(&self, pgno: u32) -> Option<&[u8]> {
        let size = self.geometry.page_size();
        self.view.slice(u64::from(pgno) * size as u64, size)
    }
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

    fn run(&self, ovf: Ovf, verify: RunCrc) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_run(ovf, size, self.meta.page_count, self.meta.txn, false, verify, |p, len| {
            self.view.slice(u64::from(p) * size as u64, len)
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
    /// The writer's state while no write transaction holds it (always empty in a read-only
    /// environment, which [`Self::writable`] refuses first).
    writer: Mutex<Option<Box<Writer>>>,
    /// Signalled whenever the writer's state comes back.
    writer_back: Condvar,
    /// A read-only environment's mapping ([`Env::open_read_only`]); `None` in one with a writer,
    /// whose page file the writer's state holds.
    read_only: Option<ReadOnlyFile>,
    /// The base of the writer's clock ([`Self::clock`]).
    clock_base: Instant,
    /// When the snapshot of the oldest live reader was superseded, as of the last commit
    /// ([`Writer::publish`]), on [`Self::clock`]; [`NOT_SUPERSEDED`] when that reader holds the
    /// current snapshot. Read by [`Env::oldest_reader_age`].
    oldest_superseded: AtomicU64,
    /// The writer's counters as of the last time its state came back ([`Self::return_writer`]),
    /// which [`Env::io_stats`] copies: all zero in a read-only environment.
    io_stats: Mutex<IoStats>,
    /// Test-only: drop as a crash would, without the final sync.
    #[cfg(test)]
    pub(crate) crash_on_drop: std::sync::atomic::AtomicBool,
}

impl EnvInner {
    /// A new environment's shared state: `snapshot` is its current state, and `writer` its
    /// writer's state, or `None` with `read_only` set.
    fn new(
        geometry: Geometry,
        sync_mode: SyncMode,
        snapshot: Arc<Snapshot>,
        writer: Option<Box<Writer>>,
        read_only: Option<ReadOnlyFile>,
    ) -> Self {
        Self {
            geometry,
            sync_mode,
            latch: Latch::default(),
            current: RwLock::new(snapshot),
            writer: Mutex::new(writer),
            writer_back: Condvar::new(),
            read_only,
            clock_base: Instant::now(),
            oldest_superseded: AtomicU64::new(NOT_SUPERSEDED),
            io_stats: Mutex::new(IoStats::default()),
            #[cfg(test)]
            crash_on_drop: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The newest committed state.
    pub(crate) fn current(&self) -> Arc<Snapshot> {
        Arc::clone(&self.current.read())
    }

    /// The time since [`Self::clock_base`], in nanoseconds: the clock of the reader queue's
    /// superseded-at times.
    fn clock(&self) -> u64 {
        u64::try_from(self.clock_base.elapsed().as_nanos()).unwrap_or(NOT_SUPERSEDED - 1)
    }

    /// `Err(ReadOnly)` in a read-only environment: it has no writer, so whatever needs one
    /// checks this before waiting for it.
    fn writable(&self) -> Result<()> {
        if self.read_only.is_some() {
            return Err(Error::ReadOnly);
        }
        Ok(())
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

    /// Give the writer's state back and wake one waiter, first publishing its counters
    /// ([`Writer::io_stats`]). Every commit, abort and sync ends here, so the published
    /// counters include each one's work.
    pub(crate) fn return_writer(&self, writer: Box<Writer>) {
        *self.io_stats.lock() = writer.io_stats();
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
        // Every transaction holds a reference to this, so the writer's state is home (a
        // read-only environment has none, and is never `NoSync`).
        if let Some(writer) = self.writer.get_mut().as_mut() {
            if let Err(e) = writer.sync(&self.latch) {
                tracing::error!(target: "tndb_b", "the final sync failed: {e}");
            }
        }
    }
}

/// [`Published::superseded_at`] of the current snapshot, and [`EnvInner::oldest_superseded`]
/// when no live reader holds a superseded one.
const NOT_SUPERSEDED: u64 = u64::MAX;

/// A snapshot the writer published, in its reader queue ([`Writer::readers`]).
#[derive(Debug)]
struct Published {
    /// The snapshot's txn.
    txn: u64,
    /// The snapshot, alive while the environment or a reader holds it.
    snapshot: Weak<Snapshot>,
    /// When the next commit published over it, on [`EnvInner::clock`] ([`NOT_SUPERSEDED`] while
    /// it is the current snapshot).
    superseded_at: u64,
}

impl Published {
    /// The current snapshot's entry.
    fn current(snapshot: &Arc<Snapshot>) -> Self {
        let txn = snapshot.meta.txn;
        Self { txn, snapshot: Arc::downgrade(snapshot), superseded_at: NOT_SUPERSEDED }
    }
}

/// The single writer's state, held by one write transaction at a time: the page file, the
/// page allocator, the newest committed state, what the meta slots hold on disk, and the
/// snapshots published to readers.
#[derive(Debug)]
pub(crate) struct Writer {
    /// The page file (only the writer changes it).
    pub(crate) file: PageFile,
    /// The readers' view of [`Self::file`]'s mapping ([`PageFile::view`]): the one [`MapView`]
    /// the file updates in place whenever its mapping moves, so it never needs refreshing. A
    /// field apart from `file`, so [`PageWrite::copy_published`] borrows a published page through
    /// it while it writes a private page through `file`.
    pub(crate) view: Arc<MapView>,
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
    readers: VecDeque<Published>,
    /// The queue's length after its last full pass (see [`Self::prune_readers`]).
    readers_kept: usize,
    /// The tree writes' reused buffers ([`PageWrite::take_scratch`]).
    scratch: Scratch,
    /// The running totals of the I/O and allocation counters, but for those [`Self::io_stats`]
    /// takes from elsewhere ([`Self::verifies`], [`Self::verify_ns`], the file's growth).
    pub(crate) stats: IoStats,
    /// [`IoStats::verifies`]: counted by [`PageWrite::count_verify`], whose callers hold the store
    /// by shared reference (and the store is `Sync`), so an atomic. Only the writing
    /// transaction's thread changes it, by a relaxed load and store, never a read-modify-write.
    verifies: AtomicU64,
    /// [`IoStats::verify_ns`], kept as [`Self::verifies`] is.
    verify_ns: AtomicU64,
    /// Test-only: the most rounds a commit's free-list save took.
    #[cfg(test)]
    pub(crate) max_save_rounds: u32,
    /// Test-only: the commit point at which the next commit or sync stops (see
    /// [`Self::crash_point`]).
    #[cfg(test)]
    pub(crate) crash_at: Option<CrashAt>,
    /// Test-only: the latest durable steps synced the data with one `fdatasync` of the file.
    #[cfg(test)]
    pub(crate) data_by_file: bool,
    /// Test-only (a benchmark comparison): step 4 makes the data pages durable with one
    /// `fdatasync` of the whole file for every commit, not only those [`data_sync`] picks it for.
    #[cfg(test)]
    pub(crate) data_by_fdatasync: bool,
    /// Test-only: step 4 makes the data pages durable with one `msync` of the range from the
    /// first dirty page to the last, whatever [`data_sync`] picks, so the crash matrix runs the
    /// `msync` path (and the size sync after it) on a commit whose pages form several ranges.
    /// [`Self::data_by_fdatasync`] wins over it.
    #[cfg(test)]
    pub(crate) data_by_msync: bool,
    /// Test-only: table puts take the descent every time: each put empties its table's
    /// write-path cache (`write::PathCache`) first, so none is used.
    #[cfg(test)]
    pub(crate) no_path_cache: bool,
    /// Test-only: deletes do not merge under-full pages (`PageWrite::no_merge`).
    #[cfg(test)]
    pub(crate) no_merge: bool,
    /// Test-only: the deletes whose merges absorbed a sibling (`PageWrite::merges_applied`).
    #[cfg(test)]
    pub(crate) merging_deletes: u64,
    /// Test-only: the most pages one free record lists, in place of
    /// [`freelist::record_capacity`], so a commit writes many small records.
    #[cfg(test)]
    pub(crate) record_pages: Option<usize>,
    /// Test-only: the threads commit step 3 computes the CRCs on, in place of
    /// [`stamp::threads`]' choice (1: the serial loop), so a small commit can take either path.
    #[cfg(test)]
    pub(crate) stamp_threads: Option<usize>,
    /// Test-only: the most threads one CRC computation of the latest commit's step 3 took (1:
    /// the serial loop): the whole commit's, or each slice's of a sliced step 3.
    #[cfg(test)]
    pub(crate) stamped_on: usize,
    /// Test-only: step 3 of every [`SyncMode::Durable`] commit is sliced
    /// ([`Self::stamp_sliced`]) at this many bytes, in place of [`Self::slicing`]'s choice, so a
    /// small commit takes many slices.
    #[cfg(test)]
    pub(crate) writeback_slice: Option<usize>,
    /// Test-only: when `Some`, every writeback start of a sliced step 3, in order. A test sets it
    /// to `Some(Vec::new())` to record; the starts run while other threads read the file, so a
    /// lock.
    #[cfg(test)]
    pub(crate) writeback_log: Mutex<Option<Vec<WritebackSpan>>>,
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
    /// After step 4 (the data sync: the one `msync`, the one `fdatasync`, or nothing).
    AfterDataSync,
    /// After step 5 (the size sync, whether or not the file grew or step 4 already synced it).
    AfterSizeSync,
    /// After the meta's copy into its slot, before its msync (inside step 6).
    AfterMetaCopy,
    /// After the meta's msync (step 6 complete), before publication.
    AfterMetaMsync,
}

impl Writer {
    /// Stamp the CRCs of what the transaction wrote (commit step 3): each tree page's, and one
    /// over each overflow run ([`crate::alloc::PrivatePages::units`]). Returns every page written,
    /// ascending, for the data sync; the tree pages then count as stamped by this writer.
    ///
    /// Units of many bytes have their CRCs computed on several threads first, which this stores
    /// in unit order; else each unit is stamped in turn ([`Self::stamp_units`]). A `durable`
    /// commit of many bytes is stamped in slices, each slice's writeback started once it is
    /// stamped ([`Self::stamp_sliced`]). Every way writes the same bytes in the same order, and
    /// fails at the same unit with the same error.
    pub(crate) fn stamp_private(&mut self, durable: bool) -> Result<Vec<u32>> {
        let started = Instant::now();
        let size = self.geometry.page_size();
        let private = self.alloc.stamp_private();
        let bytes = private.pages.len() * size;
        #[cfg(test)]
        {
            self.stamped_on = 0;
        }
        match self.slicing(durable, bytes) {
            None => self.stamp_units(private.units(), bytes, &[])?,
            Some(slice) => self.stamp_sliced(&private, slice)?,
        }
        self.stats.stamp_ns += elapsed_ns(started);
        Ok(private.pages)
    }

    /// The slice size step 3 cuts a commit of `bytes` bytes of units into
    /// ([`Self::stamp_sliced`]): [`WRITEBACK_SLICE`] for a `durable` commit of at least
    /// [`WRITEBACK_MIN`], else `None`, the units in one piece and no writeback started. A
    /// [`SyncMode::NoSync`] commit leaves its pages to a later sync, so it starts no I/O; a
    /// smaller commit has too little to overlap. In tests [`Self::writeback_slice`], when set,
    /// slices every durable commit.
    fn slicing(&self, durable: bool, bytes: usize) -> Option<usize> {
        #[cfg(test)]
        if let (true, Some(slice)) = (durable, self.writeback_slice) {
            return Some(slice);
        }
        (durable && bytes >= WRITEBACK_MIN).then_some(WRITEBACK_SLICE)
    }

    /// Stamp `units` (`(first page, pages)`, ascending; `bytes` their size), as step 3 stamps a
    /// commit's units: from [`stamp::threads`]' threshold, their CRCs computed on several threads
    /// ([`Self::unit_crcs`]) and then stored in unit order ([`stamp::put_crc`]); else, or when a
    /// unit lies past the end of the file, each unit stamped in turn, the serial loop, which then
    /// raises that unit's error. Meanwhile the writeback of `prev`, the slice stamped just before
    /// (empty: none), is started ([`Self::start_writeback`]): on this thread while the others
    /// compute, else before the serial loop. A failed start returns before any unit is stamped.
    fn stamp_units(
        &mut self,
        units: impl Iterator<Item = (u32, u32)> + Clone,
        bytes: usize,
        prev: &[(u32, u32)],
    ) -> Result<()> {
        let size = self.geometry.page_size();
        let threads = stamp::threads(bytes);
        #[cfg(test)]
        let threads = self.stamp_threads.unwrap_or(threads);
        let (crcs, started) =
            match self.unit_crcs(units.clone(), threads, || self.start_writeback(prev)) {
                Some((crcs, started)) => (Some(crcs), started),
                None => (None, self.start_writeback(prev)),
            };
        #[cfg(test)]
        {
            self.stamped_on = self.stamped_on.max(if crcs.is_some() { threads } else { 1 });
        }
        self.count_writeback(started?);
        match crcs {
            Some(crcs) => {
                for ((p, n), crc) in units.zip(crcs) {
                    stamp::put_crc(self.file.pages_mut(p, size, n as usize * size)?, crc);
                }
            }
            None => {
                for (p, n) in units {
                    crc::add_crc32_nonzero(self.file.pages_mut(p, size, n as usize * size)?);
                }
            }
        }
        Ok(())
    }

    /// The CRC of each of `units`, in unit order, computed on `threads` threads (the count
    /// [`stamp::threads`] gives the units' bytes) from read-only slices of the units
    /// ([`PageFile::pages`]), with `meanwhile`'s result: [`stamp::crcs`] runs it on this thread
    /// as the others compute. `None`, `meanwhile` not run, for one thread, which leaves the units
    /// to the serial loop, and when a unit lies past the end of the file, whose error the serial
    /// loop then raises.
    fn unit_crcs<R>(
        &self,
        units: impl Iterator<Item = (u32, u32)>,
        threads: usize,
        meanwhile: impl FnOnce() -> R,
    ) -> Option<(Vec<u32>, R)> {
        let size = self.geometry.page_size();
        if threads < 2 {
            return None;
        }
        let units: Vec<&[u8]> = units
            .map(|(p, n)| self.file.pages(p, size, n as usize * size))
            .collect::<Option<_>>()?;
        Some(stamp::crcs(&units, threads, meanwhile))
    }

    /// Step 3 of a durable commit of many bytes, in slices: `private`'s units in page order, cut
    /// into slices that close at the first unit boundary from `slice` bytes, each stamped as a
    /// whole commit's units are ([`Self::stamp_units`]) and then its pages' writeback started
    /// ([`Self::start_writeback`]), so the drive writes the stamped slices while the rest is
    /// stamped: a slice's start runs while the next slice's CRCs are computed, the last slice's
    /// once it is stamped. The bytes stamped, their order and the errors are those of the units
    /// stamped in one piece; the starts make nothing durable, which stays step 4's. No page is
    /// written in this commit after its slice's start, so no store waits on that writeback.
    fn stamp_sliced(&mut self, private: &PrivatePages, slice: usize) -> Result<()> {
        let size = self.geometry.page_size();
        let mut units = private.units();
        let (mut prev, mut next) = (Vec::new(), Vec::new());
        loop {
            let mut bytes = 0;
            for (p, n) in units.by_ref() {
                next.push((p, n));
                bytes += n as usize * size;
                if bytes >= slice {
                    break;
                }
            }
            if next.is_empty() {
                break;
            }
            self.stamp_units(next.iter().copied(), bytes, &prev)?;
            std::mem::swap(&mut prev, &mut next);
            next.clear();
        }
        let started = self.start_writeback(&prev)?;
        self.count_writeback(started);
        Ok(())
    }

    /// Start the writeback of `slice`'s pages (units `(first page, pages)`, ascending, each one
    /// stamped): one [`PageFile::start_writeback`] of the bytes from its first page to the end of
    /// its last, the clean pages between included (the kernel writes only dirty pages; in a
    /// durable environment those are this commit's, or free pages an aborted transaction wrote).
    /// Returns the call's time when it started a writeback; `None` for an empty slice and on a
    /// target without the call. A failure poisons the file.
    fn start_writeback(&self, slice: &[(u32, u32)]) -> Result<Option<u64>> {
        let (Some(&(first, _)), Some(&(last, n))) = (slice.first(), slice.last()) else {
            return Ok(None);
        };
        let size = self.geometry.page_size() as u64;
        let span = u64::from(first) * size..(u64::from(last) + u64::from(n)) * size;
        #[cfg(test)]
        self.log_writeback(slice, &span);
        let started = Instant::now();
        Ok(self.file.start_writeback(span)?.then(|| elapsed_ns(started)))
    }

    /// Count a writeback start [`Self::start_writeback`] made in `ns` nanoseconds (`None`: none).
    fn count_writeback(&mut self, ns: Option<u64>) {
        if let Some(ns) = ns {
            self.stats.writeback_starts += 1;
            self.stats.writeback_ns += ns;
        }
    }

    /// Test-only: record the start of `slice`'s writeback over the bytes `span` in
    /// [`Self::writeback_log`], if a test asked for the log, with whether every unit of the slice
    /// held its CRC then.
    #[cfg(test)]
    fn log_writeback(&self, slice: &[(u32, u32)], span: &Range<u64>) {
        let mut log = self.writeback_log.lock();
        let Some(log) = log.as_mut() else {
            return;
        };
        let size = self.geometry.page_size();
        let stamped = slice.iter().all(|&(p, n)| {
            self.file
                .pages(p, size, n as usize * size)
                .is_some_and(|unit| crc::crc_state(unit) == CrcState::Valid)
        });
        log.push(WritebackSpan { bytes: span.clone(), units: slice.to_vec(), stamped });
    }

    /// Record pages stamped by a [`SyncMode::NoSync`] commit, for the next sync.
    pub(crate) fn defer_sync(&mut self, pages: &[u32]) {
        self.stats.nosync_commits += 1;
        for &p in pages {
            self.unsynced.insert(p);
        }
    }

    /// The durable steps of a commit (4–6): sync `pages` (ascending) by the one `msync` or the
    /// one `fdatasync` [`data_sync`] picks, then the file's size if it grew (a no-op after the
    /// `fdatasync`, which syncs the size too), then write `meta` over the slot holding the lower
    /// txn and sync that page.
    pub(crate) fn make_durable(&mut self, pages: &[u32], meta: &Meta) -> Result<()> {
        let size = self.geometry.page_size();
        let plan = data_sync(pages, size as u64);
        #[cfg(test)]
        let plan = match (pages.first(), pages.last()) {
            _ if self.data_by_fdatasync => DataSync::File,
            (Some(&first), Some(&last)) if self.data_by_msync => {
                DataSync::Range(u64::from(first) * size as u64..(u64::from(last) + 1) * size as u64)
            }
            _ => plan,
        };
        #[cfg(test)]
        {
            self.data_by_file = plan == DataSync::File;
        }
        let grown = self.file.size_unsynced();
        let mut clock = Instant::now();
        match plan {
            DataSync::Nothing => {}
            DataSync::Range(range) => {
                let span = (range.end - range.start) / size as u64;
                self.file.msync_range(range)?;
                self.stats.msync_syncs += 1;
                self.stats.msync_span_pages += span;
            }
            DataSync::File => {
                self.file.fdatasync_all()?;
                self.stats.fdatasync_syncs += 1;
            }
        }
        self.stats.pages_synced += pages.len() as u64;
        self.stats.data_sync_ns += lap(&mut clock);
        #[cfg(test)]
        self.crash_point(CrashAt::AfterDataSync)?;
        self.file.sync_size_if_grown()?;
        self.stats.size_sync_ns += lap(&mut clock);
        #[cfg(test)]
        self.crash_point(CrashAt::AfterSizeSync)?;
        let slot = slot_to_write(&self.slots);
        self.file.page_mut(slot as u32, size)?[..META_LEN].copy_from_slice(&meta.encode());
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaCopy)?;
        self.file.msync_page(slot as u32, size)?;
        self.slots[slot] = Some(meta.txn);
        self.stats.meta_sync_ns += lap(&mut clock);
        self.stats.commits += 1;
        self.stats.grown_commits += u64::from(grown);
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaMsync)?;
        Ok(())
    }

    /// The I/O and allocation counters' totals so far ([`Env::io_stats`]).
    pub(crate) fn io_stats(&self) -> IoStats {
        IoStats {
            verifies: self.verifies.load(Ordering::Relaxed),
            verify_ns: self.verify_ns.load(Ordering::Relaxed),
            bytes_grown: self.file.bytes_grown(),
            ..self.stats
        }
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
    /// now on see it. The snapshot it supersedes records the time, and the superseded-at time of
    /// the oldest live reader's snapshot goes to [`EnvInner::oldest_superseded`].
    pub(crate) fn publish(&mut self, inner: &EnvInner, meta: Meta) {
        let view = self.file.view();
        view.publish_len(u64::from(meta.page_count) * self.geometry.page_size() as u64);
        let snapshot = Arc::new(Snapshot::new(meta, self.geometry, view));
        if let Some(previous) = self.readers.back_mut() {
            previous.superseded_at = inner.clock();
        }
        self.readers.push_back(Published::current(&snapshot));
        self.last = meta;
        *inner.current.write() = snapshot;
        self.prune_readers();
        let oldest = self.readers.front().map_or(NOT_SUPERSEDED, |p| p.superseded_at);
        inner.oldest_superseded.store(oldest, Ordering::Relaxed);
    }

    /// Drop the snapshots no reader holds from the front of the queue. A reader stuck on an old
    /// snapshot keeps the front, so the dead entries behind it are also dropped, in a pass each
    /// time the queue doubles: it stays bounded by the live snapshots.
    fn prune_readers(&mut self) {
        while self.readers.front().is_some_and(|p| p.snapshot.strong_count() == 0) {
            self.readers.pop_front();
        }
        if self.readers.len() > 2 * self.readers_kept.max(16) {
            self.readers.retain(|p| p.snapshot.strong_count() > 0);
            self.readers_kept = self.readers.len();
        }
    }

    /// The txn of the oldest snapshot a reader may still be reading (the current one at least:
    /// the environment holds it). A page freed by a later transaction is unreachable from every
    /// live reader's snapshot.
    pub(crate) fn oldest_reader_txn(&mut self) -> u64 {
        self.prune_readers();
        let txn = self.readers.front().map_or(self.last.txn, |p| p.txn);
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

    /// Refill the empty pool from the free list of the transaction's base state ([`Self::load`]),
    /// until a record lists a page (records may be empty). Out of line: the allocations call it
    /// only when the pool is empty.
    #[cold]
    #[inline(never)]
    fn refill(&mut self) -> Result<()> {
        self.load(|alloc, _| !alloc.wants_refill())
    }

    /// Run allocation's step 2 (see the `alloc` module docs): load more free records until the
    /// pool holds `n` consecutive pages or [`RUN_LOAD_RECORDS`] records are loaded.
    fn load_for_run(&mut self, n: u32) -> Result<()> {
        self.load(|alloc, records| records >= RUN_LOAD_RECORDS || alloc.pool_fits(n))
    }

    /// Load records of the free list of the transaction's base state into the pool, smallest
    /// first while their txn passes [`Self::reuse_gate`], until `enough(allocator, records
    /// loaded)`. The first record past the gate, or the end of the list, stops loading for the
    /// rest of the transaction. A damaged record is [`Error::Corrupt`] and none of its pages is
    /// taken.
    fn load(&mut self, mut enough: impl FnMut(&mut Allocator, u32) -> bool) -> Result<()> {
        let started = Instant::now();
        let gate = self.reuse_gate();
        let lag = self.alloc.txn().saturating_sub(gate);
        self.stats.gate_lag_sum += lag;
        self.stats.gate_lag_samples += 1;
        self.stats.max_gate_lag = self.stats.max_gate_lag.max(lag);
        let (free, page_count) = (self.last.free, self.last.page_count);
        let mut records = 0;
        while self.alloc.loading() && !enough(&mut self.alloc, records) {
            let after = self.alloc.last_consumed();
            match freelist::next_record(&*self, &free, after)? {
                Some((key, value)) if key.txn <= gate => {
                    let pages = freelist::decode_pages(value, page_count)?;
                    records += 1;
                    self.alloc.load_record(key, pages)?;
                    self.stats.records_loaded += 1;
                }
                _ => self.alloc.stop_loading(),
            }
        }
        self.stats.refill_ns += elapsed_ns(started);
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

    fn run(&self, ovf: Ovf, verify: RunCrc) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_run(
            ovf,
            size,
            self.alloc.page_count(),
            self.alloc.txn(),
            self.alloc.is_private_run(ovf.pgno),
            verify,
            |p, len| self.file.pages(p, size, len),
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
        let end = self.alloc.page_count();
        let file = &mut self.file;
        let p = self.alloc.allocate(|count| file.ensure_len(u64::from(count) * size as u64))?;
        let from_end = p >= end;
        self.stats.pages_from_end += u64::from(from_end);
        self.stats.pages_from_pool += u64::from(!from_end);
        if from_end {
            // The page's first write, in the order pages are allocated: a put reserves its pages
            // before it writes them and may write one after pages past it, and on macOS first
            // writes into preallocated space after an fsync are slow, slower out of that order
            // (README, Platform notes). One byte is enough; the layout writes the page anyway.
            self.file.page_mut(p, size)?[0] = 0;
        }
        #[cfg(debug_assertions)]
        crate::tree::poison(self.file.page_mut(p, size)?);
        Ok(p)
    }

    #[inline]
    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]> {
        if !self.alloc.is_private(pgno) {
            return Err(not_private(pgno));
        }
        self.file.page_mut(pgno, self.geometry.page_size())
    }

    /// The source is borrowed through [`Self::view`], as a reader borrows a page, and the
    /// destination through `file`'s [`PageFile::page_mut`]: each slice covers exactly its own
    /// page, and they are disjoint because `from` is not private and `to` is. A page past the
    /// published bytes, where the view ends, is no page of a committed tree, only one this
    /// transaction freed: [`copy_unpublished`] copies it as the two-step copy did, so its checks
    /// stay exactly [`PageRead::page`]'s.
    #[inline]
    fn copy_published(&mut self, from: u32, to: u32) -> Result<&mut [u8]> {
        if self.alloc.is_private(from) {
            return Err(private_source(from));
        }
        self.stats.cow_copies += 1;
        let size = self.geometry.page_size();
        let Some(src) = self.view.slice(u64::from(from) * size as u64, size) else {
            return copy_unpublished(self, from, to);
        };
        let src =
            checked_page(from, self.alloc.page_count(), self.alloc.txn(), false, |_| Some(src))?;
        if !self.alloc.is_private(to) {
            return Err(not_private(to));
        }
        let dst = self.file.page_mut(to, size)?;
        copy_page(src, to, dst)?;
        Ok(dst)
    }

    fn free(&mut self, pgno: u32) {
        self.alloc.free(pgno);
    }

    fn run_privacy(&self, pgno: u32, npages: u32) -> Option<bool> {
        self.alloc.run_privacy(pgno, npages)
    }

    /// The run allocation policy (see the `alloc` module docs): step 2 here, the others in
    /// [`Allocator::allocate_run`]. Records loaded stay loaded if the allocation then fails, as a
    /// refill's do; the failure latches, and the abort returns them.
    fn alloc_run(&mut self, npages: u32) -> Result<u32> {
        if self.alloc.wants_refill() {
            self.refill()?;
        }
        let loads = self.alloc.loading() && !self.alloc.pool_fits(npages);
        if loads {
            self.load_for_run(npages)?;
        }
        let size = self.geometry.page_size();
        let file = &mut self.file;
        let end = self.alloc.page_count();
        let p = self
            .alloc
            .allocate_run(npages, |count| file.ensure_len(u64::from(count) * size as u64))?;
        // The run starts at or below the old end (tail reuse, or new pages from it).
        let from_end = p.saturating_add(npages).saturating_sub(end);
        let step = match (from_end == 0, p < end) {
            (true, _) => usize::from(loads),
            (false, true) => 2,
            (false, false) => 3,
        };
        self.stats.runs_by_step[step] += 1;
        self.stats.pages_from_end += u64::from(from_end);
        self.stats.pages_from_pool += u64::from(npages - from_end);
        #[cfg(test)]
        {
            self.alloc.run_stats.steps[step] += 1;
        }
        #[cfg(debug_assertions)]
        crate::tree::poison(self.file.pages_mut(p, size, npages as usize * size)?);
        Ok(p)
    }

    fn run_mut(&mut self, pgno: u32, npages: u32) -> Result<&mut [u8]> {
        if self.alloc.run_privacy(pgno, npages) != Some(true) {
            return Err(not_private(pgno));
        }
        let size = self.geometry.page_size();
        self.file.pages_mut(pgno, size, npages as usize * size)
    }

    fn free_run(&mut self, pgno: u32, npages: u32) {
        self.alloc.free_run(pgno, npages);
    }

    fn take_scratch(&mut self) -> Scratch {
        std::mem::take(&mut self.scratch)
    }

    fn put_scratch(&mut self, scratch: Scratch) {
        self.scratch = scratch;
    }

    fn count_verify(&self, ns: u64) {
        let add = |counter: &AtomicU64, n: u64| {
            counter.store(counter.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
        };
        add(&self.verifies, 1);
        add(&self.verify_ns, ns);
    }

    #[cfg(test)]
    fn no_merge(&self) -> bool {
        self.no_merge
    }

    #[cfg(test)]
    fn merges_applied(&mut self, report: crate::tree::write::MergeReport) {
        let crate::tree::write::MergeReport { leaf_left, leaf_right, branches, .. } = report;
        self.merging_deletes += u64::from(leaf_left + leaf_right + branches > 0);
    }
}

/// [`Writer::page_mut`]'s error for a page that is not private.
#[cold]
#[inline(never)]
fn not_private(pgno: u32) -> Error {
    Error::Failed(format!("page {pgno} is not private: copy it first"))
}

/// [`Writer::copy_published`]'s error for a source page that is private.
#[cold]
#[inline(never)]
fn private_source(pgno: u32) -> Error {
    Error::Failed(format!("page {pgno} is private: it needs no copy"))
}

/// [`Writer::copy_published`] for a page past the published bytes (see there): fetched through
/// the file with [`PageRead::page`]'s checks and copied through a buffer of its own. No sound
/// tree reaches it.
#[cold]
#[inline(never)]
fn copy_unpublished(writer: &mut Writer, from: u32, to: u32) -> Result<&mut [u8]> {
    let src = writer.page(from)?.to_vec();
    let dst = writer.page_mut(to)?;
    copy_page(&src, to, dst)?;
    Ok(dst)
}

/// How step 4 syncs dirty `pages` (ascending). Runs of adjacent pages separated by at most
/// [`MERGE_GAP`] clean pages merge into one range. Pages that form exactly one range, in at most
/// [`MAX_RUNS`] runs, get one `msync` of it; any other set gets one `fdatasync` of the file.
///
/// - **Two or more ranges:** on Linux every `msync(MS_SYNC)` runs the file system's fsync path,
///   drive-cache flush included, so k ranges cost k flushes where one `fdatasync` costs one (small
///   commits on macOS sync faster by one `fdatasync` too).
/// - **One range:** one `msync` is one flush on Linux too, and on macOS it beats an `fdatasync` of
///   the file for a large commit (a bulk load's one range), which `fdatasync` writes slower.
/// - **More than [`MAX_RUNS`] runs** (a commit whose copies took pages freed all over the file) get
///   the `fdatasync` even when they merge into one range: an `msync` writes its range's dirty pages
///   one cluster at a time on macOS, about three times the cost of `fdatasync`'s single pass for a
///   commit of a thousand scattered pages.
///
/// Either way the data is durable before the meta is written. `fdatasync` also writes any other
/// dirty page of the file, which at a commit can only be a page of an aborted transaction: a free
/// page, so writing it is harmless.
fn data_sync(pages: &[u32], page_size: u64) -> DataSync {
    let (Some(&first), Some(&last)) = (pages.first(), pages.last()) else {
        return DataSync::Nothing;
    };
    let (mut runs, mut ranges) = (1, 1);
    for w in pages.windows(2) {
        let clean = w[1] - w[0] - 1;
        runs += usize::from(clean > 0);
        ranges += usize::from(clean > MERGE_GAP);
    }
    if runs > MAX_RUNS || ranges > 1 {
        return DataSync::File;
    }
    DataSync::Range(u64::from(first) * page_size..(u64::from(last) + 1) * page_size)
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
        let (chosen, geometry) = choose_state(&file)?;
        let meta = chosen.meta;
        // A crash may have left a meta in the page cache whose msync never finished: make what
        // this open reads durable before anything is built on it.
        sys::fsync_file(&file)?;
        let file = PageFile::from_file(file, options.map_reserve, options.readahead)?;
        let snapshot = checked_snapshot(meta, geometry, file.view())?;
        let writer = Writer {
            view: file.view(),
            file,
            geometry,
            alloc: Allocator::new(meta.txn + 1, meta.page_count),
            last: meta,
            slots: chosen.slots,
            unsynced: PageSet::default(),
            readers: VecDeque::from([Published::current(&snapshot)]),
            readers_kept: 1,
            scratch: Scratch::default(),
            stats: IoStats::default(),
            verifies: AtomicU64::new(0),
            verify_ns: AtomicU64::new(0),
            #[cfg(test)]
            max_save_rounds: 0,
            #[cfg(test)]
            crash_at: None,
            #[cfg(test)]
            data_by_file: false,
            #[cfg(test)]
            data_by_fdatasync: false,
            #[cfg(test)]
            data_by_msync: false,
            #[cfg(test)]
            no_path_cache: false,
            #[cfg(test)]
            no_merge: false,
            #[cfg(test)]
            merging_deletes: 0,
            #[cfg(test)]
            record_pages: None,
            #[cfg(test)]
            stamp_threads: None,
            #[cfg(test)]
            stamped_on: 0,
            #[cfg(test)]
            writeback_slice: WRITEBACK_SLICE_ON_OPEN.get(),
            #[cfg(test)]
            writeback_log: Mutex::new(None),
        };
        let inner = EnvInner::new(geometry, options.sync, snapshot, Some(Box::new(writer)), None);
        Ok(Env { inner: Arc::new(inner) })
    }

    /// Open the existing database file at `path` read-only, for a process that reads it while no
    /// writer has it open (a tool run while the node is stopped). It takes a shared lock, so
    /// read-only opens coexist and exclude [`Self::open`]; chooses the meta and checks the roots
    /// as [`Self::open`] does; and maps the file read-only. It never creates, writes or syncs
    /// anything, and has no writer: [`Self::begin_rw`] and [`Self::try_begin_rw`] return
    /// [`Error::ReadOnly`], [`Self::sync`] does nothing, and every reader sees the state opened
    /// at. Reads, [`Self::check`] and [`Self::stat`] work as in any environment, and damage a
    /// read finds latches it (see the module docs).
    ///
    /// Errors: as [`Self::open`]'s, except that a missing file is [`Error::Io`] (`NotFound`) and
    /// an empty one [`Error::NotTndbB`]; [`Error::Locked`] if a handle with a writer holds it.
    /// `options.page_size` is validated as [`Self::open`] validates it and `options.readahead`
    /// advises the mapping as [`Self::open`]'s; no other field applies.
    pub fn open_read_only(path: impl AsRef<Path>, options: EnvOptions) -> Result<Env> {
        Geometry::new(options.page_size)?;
        let file = open_shared(path.as_ref())?;
        let (chosen, geometry) = choose_state(&file)?;
        // No sync, unlike `open`: nothing is built on what this open reads.
        let file = ReadOnlyFile::from_file(file, options.readahead)?;
        let snapshot = checked_snapshot(chosen.meta, geometry, file.view())?;
        let inner = EnvInner::new(geometry, SyncMode::Durable, snapshot, None, Some(file));
        Ok(Env { inner: Arc::new(inner) })
    }

    /// Begin a read-only transaction on the newest committed state. It sees that state of every
    /// table for as long as it lives, whatever commits meanwhile.
    pub fn begin_ro(&self) -> Result<RoTxn> {
        Ok(RoTxn::new(Arc::clone(&self.inner), self.inner.current()))
    }

    /// How long the oldest live reader has held a superseded snapshot: the time since a commit
    /// published a newer state over the oldest snapshot a reader still holds. `None` when every
    /// live reader holds the current snapshot (or none is open), and always in an environment
    /// opened read-only, which never commits. While such a reader lives, the pages freed after
    /// its snapshot are not reused, so the file grows instead; they become reusable once it
    /// ends.
    ///
    /// Lock-free, and free for readers: each commit records the time and refreshes the value
    /// after it publishes, so between commits a reader that has ended still counts until the
    /// next commit.
    pub fn oldest_reader_age(&self) -> Option<Duration> {
        let since = self.inner.oldest_superseded.load(Ordering::Relaxed);
        (since != NOT_SUPERSEDED)
            .then(|| Duration::from_nanos(self.inner.clock().saturating_sub(since)))
    }

    /// The writer's I/O and allocation counters, cumulative since this environment opened (see
    /// [`IoStats`]): in memory only, published at the end of every commit, abort and
    /// [`Self::sync`], so between those they stay put. All zero in an environment opened
    /// read-only. One short lock of the published copy: a caller never waits for an open write
    /// transaction.
    pub fn io_stats(&self) -> IoStats {
        *self.inner.io_stats.lock()
    }

    /// Begin the write transaction, waiting while another one is open (from any thread: a
    /// thread that holds one and calls this waits forever). [`Error::ReadOnly`] in an
    /// environment opened read-only, latched or not; [`Error::Failed`] once the environment has
    /// latched a failure.
    pub fn begin_rw(&self) -> Result<RwTxn> {
        self.inner.writable()?;
        self.inner.latch.check()?;
        let writer = self.inner.take_writer(true).expect("a waiting take returns the writer");
        Ok(RwTxn::new(Arc::clone(&self.inner), writer))
    }

    /// Begin the write transaction if no other one is open, else `None`. [`Error::ReadOnly`] in
    /// an environment opened read-only, latched or not; [`Error::Failed`] once the environment
    /// has latched a failure.
    pub fn try_begin_rw(&self) -> Result<Option<RwTxn>> {
        self.inner.writable()?;
        self.inner.latch.check()?;
        Ok(self.inner.take_writer(false).map(|w| RwTxn::new(Arc::clone(&self.inner), w)))
    }

    /// Make every commit durable: in [`SyncMode::NoSync`], sync the pages committed since the
    /// last sync and write the newest state's meta (a no-op when nothing is pending, and always
    /// in [`SyncMode::Durable`] and in an environment opened read-only). Waits while a write
    /// transaction is open, as [`Self::begin_rw`] does. A failure latches the environment.
    pub fn sync(&self) -> Result<()> {
        if self.inner.writable().is_err() {
            return Ok(());
        }
        let latch = &self.inner.latch;
        self.inner.with_writer(|writer| {
            latch.check()?;
            writer.sync(latch)
        })
    }

    /// Check the newest committed state in full: every page of every tree passes its CRC and
    /// structure checks, is the copy its parent names, lies at the depth its kind says, and holds
    /// keys inside its ancestors' separators; every overflow run a leaf names passes its header,
    /// identity and full CRC checks with a zero pad; every tree holds the entries and the pages of
    /// each kind its root counts; every free record is well formed; and the pages reachable from
    /// the trees (their runs' included) and those the free list lists are disjoint and cover
    /// `2..page_count` exactly. Any failure is [`Error::Corrupt`] naming the damage, or the pages
    /// leaked (used by nothing) and used twice, and latches the environment. It reads through a
    /// snapshot, alongside readers and the writer.
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

    /// The pages the free list of the newest committed state holds: pages a later write may reuse,
    /// once no reader needs them. It reads every free record, with the checks the allocator's reads
    /// make; damage is [`Error::Corrupt`] and latches the environment.
    pub fn free_pages(&self) -> Result<u64> {
        let inner = &self.inner;
        let snapshot = inner.current();
        let mut pages = 0;
        let mut after = None;
        let counted = loop {
            match freelist::next_record(&*snapshot, &snapshot.meta.free, after) {
                Ok(Some((key, value))) => {
                    match freelist::decode_pages(value, snapshot.meta.page_count) {
                        Ok(listed) => pages += listed.len() as u64,
                        Err(e) => break Err(e),
                    }
                    after = Some(key);
                }
                Ok(None) => break Ok(pages),
                Err(e) => break Err(e),
            }
        };
        counted.inspect_err(|e| inner.latch.latch_error(e))
    }

    /// The first failure the environment latched, if any: from then on writes and commits
    /// return [`Error::Failed`] (reads continue) until the file is reopened. A read-only
    /// environment latches damage its reads find too, though it has no writer to stop.
    pub fn failure(&self) -> Option<String> {
        self.inner.latch.failure()
    }

    /// The longest key this page size accepts.
    pub fn max_key_len(&self) -> usize {
        self.inner.geometry.max_key()
    }

    /// The longest value a table accepts with a `klen`-byte key: 16 MiB (stored in an overflow
    /// run) when the key leaves room for the run's 12-byte reference in its leaf, which every key
    /// up to 10 bytes short of [`max_key_len`](Self::max_key_len) does; else the longest value its
    /// leaf holds; 0 when `klen` is over [`max_key_len`](Self::max_key_len): such a key is refused
    /// with any value. Every accepted key leaves room for at least 2 bytes, so 0 always means the
    /// key is refused.
    pub fn max_value_len(&self, klen: usize) -> usize {
        self.inner.geometry.max_table_value(klen)
    }

    /// Test-only: the environment's shared state.
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<EnvInner> {
        &self.inner
    }

    /// Test-only: run `f` on the writer's state (waiting for an open write transaction).
    #[cfg(test)]
    pub(crate) fn with_writer<T>(&self, f: impl FnOnce(&mut Writer) -> T) -> T {
        assert!(self.inner.read_only.is_none(), "a read-only environment has no writer");
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

/// Open and lock the existing database file at `path` read-only (a read-only open's steps 2–3):
/// `O_RDONLY`, then a shared lock, else [`Error::Locked`] (a handle with a writer holds the file).
/// As in [`open_locked`], the lock must end up on the file the path names: a creator may rename a
/// new file over a missing or empty path between this open and its lock, so the open is retried
/// (a few times; then [`Error::Locked`]). A missing file is [`Error::Io`] (`NotFound`), and an
/// empty one is left to [`choose_meta`], which finds no meta in it: [`Error::NotTndbB`].
fn open_shared(path: &Path) -> Result<File> {
    for _ in 0..3 {
        let file = OpenOptions::new().read(true).open(path)?;
        if !sys::flock_shared(&file)? {
            return Err(Error::Locked);
        }
        let locked = file.metadata()?;
        match fs::metadata(path) {
            Ok(named) if named.ino() == locked.ino() && named.dev() == locked.dev() => {
                return Ok(file);
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

/// Choose the state an open recovers from `file`, locked (open steps 4–6): the newest valid meta
/// ([`choose_meta`]) and its geometry, refusing a txn id with no next one and a file shorter than
/// the meta's pages. Reads only.
fn choose_state(file: &File) -> Result<(Chosen, Geometry)> {
    let chosen = choose_meta(|offset| read_record(file, offset))?;
    let meta = chosen.meta;
    let geometry = Geometry::new(meta.page_size)?;
    if meta.txn == u64::MAX {
        return Err(Error::Corrupt("open: the meta's txn id leaves no next txn".to_string()));
    }
    let needed = u64::from(meta.page_count) * u64::from(meta.page_size);
    let len = file.metadata()?.len();
    if len < needed {
        return Err(Error::Corrupt(format!(
            "open: the file is {len} bytes, shorter than the {} pages of its newest meta (txn {})",
            meta.page_count, meta.txn
        )));
    }
    Ok((chosen, geometry))
}

/// The snapshot of the state `meta` names, read through `view` (the file mapped by open step 8),
/// once both its tree roots pass [`check_root`] (open step 9).
fn checked_snapshot(meta: Meta, geometry: Geometry, view: Arc<MapView>) -> Result<Arc<Snapshot>> {
    view.publish_len(u64::from(meta.page_count) * u64::from(meta.page_size));
    let snapshot = Arc::new(Snapshot::new(meta, geometry, view));
    check_root(&snapshot, &meta.catalog, "catalog")?;
    check_root(&snapshot, &meta.free, "free tree")?;
    Ok(snapshot)
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
mod io_stats_tests;
#[cfg(test)]
pub(crate) mod tests;
