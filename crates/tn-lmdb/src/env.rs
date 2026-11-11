//! The environment: one page file opened, locked and recovered ([`Env::open`]), the single
//! writer's state handed from one write transaction to the next, the snapshots readers start
//! from, and the commit protocol's durable steps.
//!
//! ## Open and recovery
//!
//! A missing or empty file is created atomically: both metas (txn 0, no tables) are written to
//! `<path>.creating`, synced, and renamed over the path. A symbolic link at the path is followed
//! only to a file holding bytes: a dangling one, or one to an empty file, is refused (the rename
//! would replace the link, not create its target). The file is then locked
//! (`flock(LOCK_EX | LOCK_NB)`, so a second open fails with [`Error::Locked`], in this process
//! too), and the meta chosen ([`choose_meta`]: the newest valid one, but a weak one only in the
//! boot that wrote it, and only once the pages it names since the last sync pass
//! [`check::check_since`], else the last sync: [`opened_snapshot`]). Open never writes to an
//! existing file: a file that is shorter than the chosen meta's page count, not a tn-lmdb file, or
//! whose catalog or free-tree root fails its checks fails closed, unchanged.
//!
//! [`Env::open_read_only`] opens an existing file no writer holds (`O_RDONLY`, a shared
//! `flock`), chooses the meta and checks the roots exactly as [`Env::open`] does, and maps the
//! file read-only. It never creates, writes or syncs anything; it has no writer, so its snapshot
//! is the one it opened at.
//!
//! ## Commits
//!
//! A commit (see `RwTxn::commit`) stamps the CRC of every page the transaction wrote, makes
//! them durable (one `msync` when they form one range, else one `fdatasync` of the file, or on
//! macOS an `msync` of each range; see [`data_sync`]), then the file's size if it grew and that
//! sync did not already cover it, and only then writes the new meta into the slot that does not
//! hold the newest steady meta (with both steady, the one holding the lower txn; beside a weak
//! record, the weak one: [`slot_to_write`]) and syncs that one page. A crash before the meta is
//! copied recovers the previous state; after its sync, the new one; in between, the new one if
//! its record survived intact (a process crash keeps it in the page cache, which a reopen reads;
//! a power loss, only if it reached the disk), else the one the other slot holds, the last
//! durable state (README, Commit protocol). In [`SyncMode::NoSync`] a
//! commit stamps, copies its meta as a weak record into the slot that does not hold the newest
//! steady one (no sync: [`Writer::defer_sync`]) and publishes; [`Env::sync`] (or the
//! environment's drop) runs the durable steps for the newest state, and so does a commit past
//! [`EnvOptions::sync_period`] or [`EnvOptions::sync_bytes`] for its own ([`Writer::sync_due`]),
//! as a durable commit does, and the first commit that changes something after each open, and one
//! [`meta::MAX_WEAK_RUN`] txns past the newest steady meta. A reopen in the same boot trusts a weak
//! head once its pages since the last sync pass their check; after a reboot it opens at the newest
//! steady meta ([`choose_meta`]). Each open starts its writer [`meta::MAX_WEAK_RUN`] + 1 txns past
//! the newest steady meta ([`Chosen::next_txn`]), so no txn a dead or rolled-back writer stamped
//! pages with is given to a commit that does not sync.
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
//! meta slot, weak or steady, and no live reader can reach them ([`Writer::reuse_gate`]).
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
    os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{fence, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex, RwLock};

use self::gap_auto::{AutoGap, Choice, Gaps, Outcome};
use crate::{
    alloc::{Allocator, PrivatePages, RUN_LOAD_RECORDS},
    check::{self, CheckReport},
    crc::CrcState,
    error::{Error, Result},
    file::{MapView, PageFile, ReadOnlyFile, Resident, DEFAULT_RESERVE, MAX_GROWTH, MIN_GROWTH},
    freelist,
    latch::{Latch, UnwindLatch},
    meta::{self, choose_meta, slot_to_write, Chosen, Meta, Slot, Tag, META_LEN},
    overflow::{checked_run, Ovf, RunCrc},
    page::{self, Geometry, Kind},
    page_set::PageSet,
    stamp,
    stats::{elapsed_ns, lap, IoStats},
    sys::{self, BootId},
    tree::{
        read::{checked_child, checked_page},
        write::Scratch,
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

/// The largest [`EnvOptions::gap_fill`], in pages: the widest gap of clean pages step 4 merges
/// into one synced range (`MERGE_GAP`), so every gap a fill rewrites lies inside a range step 4
/// syncs whichever way it syncs (the one `msync`'s range, the `fdatasync`'s whole file, or on
/// macOS a merged range of `DataSync::Ranges`).
pub const MAX_GAP_FILL: u32 = 64;

const _: () = assert!(MAX_GAP_FILL <= MERGE_GAP, "a filled gap must stay inside a synced range");

/// How many more of a commit's gaps may miss the page cache than it filled before its gap fill
/// stops trying ([`Writer::fill_gaps`]): on Linux a miss starts a read of the gap's pages, so the
/// reads a commit's fill starts exceed the writes it saved by at most this many.
const GAP_MISS_SLACK: u64 = 16;

mod gap_auto;

/// [`EnvOptions::gap_fill`]: how wide a gap of clean pages step 4 may rewrite before its data
/// sync (README, "Gap fill"). Parsed from `auto`, `off` or a page count ([`std::str::FromStr`]),
/// and shown the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GapFill {
    /// Every gap of at most this many pages; 0 (the default) is off. At most [`MAX_GAP_FILL`],
    /// else [`Env::open`] refuses it ([`Error::InvalidGapFill`]).
    Pages(u32),
    /// A width per scattered durable step, from the disk's measured costs: the width that
    /// predicts the least time for step 4, from a cost per write and a cost per page fitted to
    /// the recent steps' syncs, and the fill's own timed cost (README, "Automatic width"). It
    /// settles at 0 on a drive whose writes are cheap beside its bytes (local NVMe), and fills
    /// on one that caps its writes per second (cloud block storage). [`IoStats`]'s `gap_auto_*`
    /// fields report its choice and its fit.
    Auto,
}

impl GapFill {
    /// Off: no gap filled.
    pub const OFF: GapFill = GapFill::Pages(0);

    /// Whether any gap may be filled: a width of at least 1 page, or [`GapFill::Auto`].
    pub const fn is_on(self) -> bool {
        !matches!(self, GapFill::Pages(0))
    }
}

impl Default for GapFill {
    fn default() -> Self {
        GapFill::OFF
    }
}

impl std::fmt::Display for GapFill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GapFill::Pages(pages) => write!(f, "{pages}"),
            GapFill::Auto => f.write_str("auto"),
        }
    }
}

impl std::str::FromStr for GapFill {
    type Err = ParseGapFillError;

    /// `auto`, `off` (0) or a page count of at most [`MAX_GAP_FILL`] (0 is off).
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "auto" => Ok(GapFill::Auto),
            "off" => Ok(GapFill::OFF),
            _ => match s.parse::<u32>() {
                Ok(pages) if pages <= MAX_GAP_FILL => Ok(GapFill::Pages(pages)),
                _ => Err(ParseGapFillError(s.to_owned())),
            },
        }
    }
}

/// A gap fill that is not `auto`, `off` or a page count of at most [`MAX_GAP_FILL`]
/// ([`GapFill`]'s `FromStr`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid gap fill {0:?}: expected auto, off, or a page count of at most {MAX_GAP_FILL}")]
pub struct ParseGapFillError(String);

/// The writer's gap fill ([`EnvOptions::gap_fill`]) as it runs.
#[derive(Debug)]
enum GapWidth {
    /// Every gap of at most this many pages; 0: off.
    Fixed(u32),
    /// Chosen per scattered step ([`GapFill::Auto`]).
    Auto(Box<AutoGap>),
}

impl GapWidth {
    /// Whether any gap may be filled.
    fn is_on(&self) -> bool {
        !matches!(self, GapWidth::Fixed(0))
    }
}

/// What one gap fill did ([`Writer::fill_gaps`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Fill {
    /// Gaps rewritten.
    pub(crate) filled: u64,
    /// Gaps skipped as not in memory.
    pub(crate) missed: u64,
    /// The pages of the gaps rewritten.
    pub(crate) pages: u64,
}

/// How step 4 makes a commit's dirty pages durable ([`data_sync`]).
#[derive(Debug, PartialEq, Eq)]
enum DataSync {
    /// No page to sync.
    Nothing,
    /// One `msync(MS_SYNC)` of this byte range.
    Range(Range<u64>),
    /// macOS, where Linux takes [`DataSync::File`]: one `msync(MS_ASYNC)` of each range the pages
    /// merge into, then one `msync(MS_SYNC)` of each ([`merged_ranges`],
    /// `PageFile::msync_ranges`). An `fsync` there leaves some pages dirtied through the mapping
    /// unwritten, and only `msync` writes them (README, Platform notes).
    #[cfg(target_os = "macos")]
    Ranges,
    /// One `fdatasync` of the whole file (on macOS only the test-only `Writer::data_by_fdatasync`
    /// picks it, and `PageFile::fdatasync_all` msyncs the whole mapping first there).
    #[cfg_attr(all(target_os = "macos", not(test)), allow(dead_code))]
    File,
}

/// When a commit becomes durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// A commit is durable when it returns: its pages, then its meta page, are synced.
    #[default]
    Durable,
    /// A commit is visible in this process at once, and durable at the next sync: an
    /// [`Env::sync`], the close (which runs in the drop of the environment's last handle, on
    /// that thread: a reader's or cursor's drop can be the one that syncs, and a handle alive
    /// when the process exits skips it), or a commit past [`EnvOptions::sync_period`] or
    /// [`EnvOptions::sync_bytes`], which commits durably as a [`Self::Durable`] commit does.
    /// So does the first commit that changes something after each open, whatever the bounds
    /// (the open is not a durable state: a weak head it trusted stays weak until then), and a
    /// commit whose txn is more than 2^20 past the last durable state's, so a writer never
    /// runs further than that past it: no txn a writer stamped pages with is then given to a
    /// later writer's commit that does not sync (`meta::Chosen::next_txn`).
    ///
    /// A commit that does not sync still writes its meta: unsynced, with no system call, *weak*
    /// (tagged with the running boot's id), into the slot that does not hold the last sync's meta.
    /// So a crash of the process alone loses nothing, but for one inside the 128-byte copy of a
    /// meta (a commit's weak one, or a sync's steady one over the weak slot), which leaves that
    /// slot torn and rolls back to the last sync (README, Commit protocol): a reopen in the same
    /// boot trusts the weak meta, whose pages the page cache holds
    /// ([`EnvOptions::trust_unsynced`]). A system crash, a power loss or any other reboot rolls
    /// back to the last sync. Neither leaves the file corrupt: the last sync's pages are never
    /// overwritten and its meta is never rewritten before the next sync. A host whose boot id
    /// cannot be read trusts no weak meta, so there a process crash too rolls back to the last
    /// sync. A volume lost without a reboot (pulled, then plugged back in the same boot) keeps the
    /// boot id but can leave a weak meta naming pages that never reached the disk, or reached it
    /// torn: so open trusts a weak meta only once the pages it names that were written since the
    /// last sync pass a full check (their CRCs among it), and opens at the last sync, with a
    /// warning, if any fails. That check costs about one CRC pass over those pages, and only after
    /// an unclean end (README, Open and recovery).
    ///
    /// Pages freed meanwhile wait for a sync (README, Free list and page reuse), so without
    /// either bound the file grows until one.
    NoSync,
}

/// How [`Env::open`] opens (or creates) a file. [`Env::open_read_only`] validates `page_size` as
/// [`Env::open`] does and uses `readahead` and `trust_unsynced`, and no other field: it maps the
/// file at its length and commits nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvOptions {
    /// The page size of a new file: a power of two in 4 KiB..=64 KiB (default 4096). An
    /// existing file keeps the page size it was created with. It also caps the file: `u32` page
    /// numbers hold at most 2^32 - 1 pages (16 TiB at 4 KiB, 256 TiB at 64 KiB).
    pub page_size: u32,
    /// Address space the mapping reserves, in bytes (default 1 GiB). The file grows inside it
    /// without remapping; past it the mapping moves to a larger reservation. More than
    /// `isize::MAX` bytes can never be mapped: [`Env::open`] refuses it
    /// ([`Error::InvalidMapReserve`]) before it touches the file. A reservation the host refuses
    /// (past the address space, or past `ulimit -v` on Linux: `ENOMEM`) fails the open with
    /// [`Error::OutOfMemory`] after a missing file was created.
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
    /// The largest growth step in bytes (default 1 GiB): the file grows by doubling up to it, then
    /// linearly by it, each step preallocated. At least [`MIN_GROWTH`](crate::MIN_GROWTH), else
    /// [`Env::open`] refuses it ([`Error::InvalidMaxGrowth`]). A smaller step bounds the
    /// preallocated tail past the pages in use while the environment runs; a clean close gives
    /// that tail back.
    pub max_growth: u64,
    /// [`SyncMode::NoSync`]: a commit that changes something commits durably once at least this
    /// long has passed since the last durable state (the last run of the durable steps: a durable
    /// commit, an [`Env::sync`]) (default `None`: never by time). The open is not one: the first
    /// commit that changes something after it commits durably whatever this says, and with it the
    /// commits of a weak head the open trusted ([`SyncMode::NoSync`]). Checked at each commit, so
    /// no thread or timer syncs: commits that then stop wait for the next commit, an [`Env::sync`]
    /// or the close. A reboot then loses only commits made less than this after the last durable
    /// state (a crash of the process alone none, but inside a meta's copy: [`SyncMode::NoSync`]),
    /// and freed pages come back within about one period and a commit. Ignored in
    /// [`SyncMode::Durable`], where every commit is durable, and by [`Env::open_read_only`].
    pub sync_period: Option<Duration>,
    /// [`SyncMode::NoSync`]: a commit that changes something commits durably once the pages stamped
    /// since the last durable state, its own included, hold at least this many bytes (pages times
    /// the page size) (default `None`: never by size). The open is not a durable state: the first
    /// commit that changes something after it commits durably whatever this says, and with it the
    /// commits of a weak head the open trusted ([`SyncMode::NoSync`]). A reboot then loses less
    /// than this many bytes of pages (a crash of the process alone, none, but inside a meta's
    /// copy: [`SyncMode::NoSync`]), and freed pages come back within about this many bytes of
    /// commits, plus a commit. Ignored in [`SyncMode::Durable`], where every commit is
    /// durable, and by [`Env::open_read_only`].
    pub sync_bytes: Option<u64>,
    /// Trust a weak meta, the meta a [`SyncMode::NoSync`] commit writes with no sync, at open when
    /// the running boot wrote it (default `true`): a crash of the process alone then loses no
    /// commit (but for one inside a meta's copy: [`SyncMode::NoSync`]), once open's check of the
    /// pages the weak meta names that were written since the last sync passes (a volume lost
    /// without a reboot, an external drive pulled and plugged back in the same boot, may have lost
    /// them; a failed check opens at the last sync, with a warning). `false` opens at the last
    /// sync whatever the slots hold, unchecked, as a reboot does. It does not discard the
    /// commits since that sync: their weak meta stays in its slot until this environment's
    /// first commit that changes something writes its steady meta over it (an [`Env::sync`] or
    /// the close with nothing committed leaves it), so a later open with `true` in the same
    /// boot can open at them again, once they pass its check (README, Open and recovery).
    /// Applies to [`Env::open_read_only`] too, which never writes that slot.
    pub trust_unsynced: bool,
    /// Gap fill (default [`GapFill::OFF`]): the most clean pages between two runs of a durable
    /// step's dirty pages that step 4 rewrites, with the bytes they already hold, before its data
    /// sync, so that writeback finds one run where it found two and the disk takes one write
    /// for both (README, "Gap fill"). A commit's pages are scattered (nearly all are reused from
    /// the free list), and Linux writes back each run of adjacent dirty pages as its own I/O, so
    /// a durable commit costs about one write per run: fewer, larger writes for more bytes. It
    /// adds no file space and changes no byte.
    ///
    /// A fixed width ([`GapFill::Pages`]) suits a disk whose I/Os per second, not its bytes per
    /// second, bound a durable commit: network block storage with an IOPS cap (cloud disks). On a
    /// local NVMe drive, whose IOPS are plentiful, the extra bytes and the copies (two system
    /// calls per gap filled) only cost. [`GapFill::Auto`] chooses the width per scattered step
    /// from the syncs' measured costs, so one setting serves both (README, "Automatic width").
    /// A width past [`MAX_GAP_FILL`] is refused by [`Env::open`] ([`Error::InvalidGapFill`]).
    ///
    /// Linux reads a gap with `RWF_NOWAIT` and fills it only when the page cache holds it whole:
    /// a gap not in memory is skipped (its read, which the kernel starts, is not waited for), and
    /// a commit stops trying once its gaps not in memory outnumber those it filled by 16. A
    /// kernel or file system that refuses `RWF_NOWAIT` turns it off for the environment, with a
    /// warning. macOS reads a gap with a plain `pread`, so a gap page not in memory costs a
    /// synchronous read there: it serves tests and measurement, not production, at a fixed width
    /// and at auto alike. A fill rewrites
    /// pages of the last durable state too, which no commit otherwise writes: it relies on the
    /// drive writing each logical block atomically (README, "Gap fill"). Applies to every run of
    /// the durable steps ([`SyncMode::Durable`] commits, [`SyncMode::NoSync`]'s syncs); ignored
    /// by [`Env::open_read_only`].
    pub gap_fill: GapFill,
}

impl Default for EnvOptions {
    fn default() -> Self {
        Self {
            page_size: 4096,
            map_reserve: DEFAULT_RESERVE,
            sync: SyncMode::Durable,
            readahead: !cfg!(target_os = "linux"),
            max_growth: MAX_GROWTH,
            sync_period: None,
            sync_bytes: None,
            trust_unsynced: true,
            gap_fill: GapFill::OFF,
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
/// which closes when the last clone and the last transaction drop, inside that last drop and on
/// its thread (README, Close).
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
///
/// Aligned to 128 bytes (a cache line on Apple silicon, the pair of lines x86-64 prefetches
/// together), so that in its `Arc` the reference counts have a line of their own. Every read
/// transaction's begin and end write those counts, and every page fetch reads the meta and the
/// view: on one line, readers on other threads would take a cache miss on each fetch after a
/// begin or end elsewhere. The cost is the padding, under 128 bytes per live snapshot.
#[derive(Debug)]
#[repr(align(128))]
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
    /// before: the snapshot's pages do not change while it lives. Never a meta page, which the
    /// writer rewrites in place: those checks refused page numbers below 2 (`MapView::slice`).
    /// `None` if it is not mapped.
    #[inline]
    pub(crate) fn page_again(&self, pgno: u32) -> Option<&[u8]> {
        debug_assert!(pgno >= 2, "page_again of meta page {pgno}");
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

    #[inline]
    fn page(&self, pgno: u32) -> Result<&[u8]> {
        let size = self.geometry.page_size();
        checked_page(pgno, self.meta.page_count, self.meta.txn, false, |p| {
            self.view.slice(u64::from(p) * size as u64, size)
        })
    }

    #[inline]
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
    /// The id of the process that opened the environment. A child it forks (without exec) holds a
    /// copy of this state, of the file's descriptor and lock, and of the mapping, all the
    /// parent's: the child's drop closes nothing ([`Drop`]).
    pid: u32,
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
            pid: std::process::id(),
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

/// The close: the final sync (in [`SyncMode::Durable`] a no-op, unless the open trusted a weak
/// head that no commit has replaced since: [`Writer::sync`]), then the trim of the file's
/// preallocated tail ([`Writer::trim_tail`]). A latched environment does neither (its newest state
/// may not be durable, and the slots' bookkeeping may not match the disk), nor does a read-only
/// one, which has no writer, nor a copy in a process the opener forked, whose file it is not (its
/// state is the parent's as of the fork: its sync could write a stale meta, its trim cut the
/// parent's slack under the parent's next writes). A failed sync skips the trim; a failed trim is
/// logged and changes nothing that matters: the file keeps its tail, which is slack.
impl Drop for EnvInner {
    fn drop(&mut self) {
        if std::process::id() != self.pid {
            return;
        }
        #[cfg(test)]
        if self.crash_on_drop.load(Ordering::Relaxed) {
            return;
        }
        if self.latch.is_failed() {
            return;
        }
        // Every transaction holds a reference to this, so the writer's state is home (a
        // read-only environment has none).
        let Some(writer) = self.writer.get_mut().as_mut() else {
            return;
        };
        if let Err(e) = writer.sync(&self.latch) {
            tracing::error!(target: "tn_lmdb", "the final sync failed: {e}");
            return;
        }
        if let Err(e) = writer.trim_tail() {
            tracing::warn!(
                target: "tn_lmdb",
                "the close did not trim the file's preallocated tail: {e}; the file keeps it"
            );
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
/// page allocator, the newest committed state, what the meta slots hold, and the snapshots
/// published to readers.
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
    /// What each meta slot holds in the file (`None`: invalid), weak or steady: in the page cache,
    /// which a weak record has not left.
    slots: [Option<Slot>; 2],
    /// The boot this writer runs in (`sys::boot_id`, zero when unknown), the tag of every record
    /// it writes.
    boot_id: BootId,
    /// [`SyncMode::NoSync`]: the pages commits stamped since the last sync.
    unsynced: PageSet,
    /// The pages [`Self::unsynced`] holds: each commit's stamped pages counted as it defers them,
    /// distinct from those already there, because a page a `NoSync` commit stamped is reused only
    /// once the reuse gate has passed the txn that frees it, which takes a sync after it (the gate
    /// is at most the newest steady slot's txn).
    unsynced_pages: u64,
    /// [`SyncMode::NoSync`]: the newest txn a commit may leave weak, [`meta::MAX_WEAK_RUN`] past
    /// the newest steady slot's ([`meta::weak_limit`]): a commit past it commits durably
    /// ([`Self::sync_due`]). Set at open, below the writer's first txn ([`Chosen::next_txn`]), so
    /// the first commit that changes something after any open is durable, and after each run of
    /// the durable steps over a state this writer committed: not after an [`Env::sync`] of the
    /// state the open found (a trusted weak head), which would raise it past the first txn.
    weak_limit: u64,
    /// [`SyncMode::NoSync`]'s automatic syncs ([`Self::sync_due`]): `None` in
    /// [`SyncMode::Durable`] and when neither bound is set, so a commit then tests
    /// [`Self::weak_limit`] alone.
    auto_sync: Option<AutoSync>,
    /// When the last run of the durable steps ended ([`Self::make_durable`], which takes it from
    /// its last clock read), and the open's time before the first: no period is counted from it,
    /// as the writer's first commit that changes something is durable anyway
    /// ([`Self::weak_limit`]).
    pub(crate) durable_at: Instant,
    /// The txn ids the open skipped ([`Chosen::skipped_txns`]), which the reuse gate's lag does
    /// not count ([`Self::load`]); it ends at the writer's first txn ([`Self::weak_limit`]).
    skipped: Range<u64>,
    /// The snapshots published, oldest first (see [`Self::oldest_reader_txn`]).
    readers: VecDeque<Published>,
    /// The queue's length after its last full pass (see [`Self::prune_readers`]).
    readers_kept: usize,
    /// The tree writes' reused buffers ([`PageWrite::take_scratch`]), boxed at the first write
    /// that takes them.
    scratch: Option<Box<Scratch>>,
    /// [`EnvOptions::gap_fill`]: `Fixed(0)` when off, and from a commit on whose gap fill met a
    /// kernel or file system that refuses `RWF_NOWAIT` ([`Self::fill_gaps`]).
    gap_fill: GapWidth,
    /// [`Self::fill_gaps`]' bounce buffer: a gap's bytes, read from the file and written back to
    /// it, never through the mapping. Grown to the largest gap filled, empty while off.
    gap_buf: Vec<u8>,
    /// The running totals of the I/O and allocation counters, but for those [`Self::io_stats`]
    /// takes from elsewhere ([`Self::verifies`], [`Self::verify_ns`], the file's growth).
    pub(crate) stats: IoStats,
    /// [`IoStats::verifies`]: counted by [`PageWrite::count_verify`], whose callers hold the store
    /// by shared reference (and the store is `Sync`), so an atomic. Every caller is inside a
    /// write, which holds the `RwTxn` by `&mut`, so one thread changes it at a time, by a
    /// relaxed load and store, never a read-modify-write. The types do not enforce that: a
    /// caller reached through a shared `&RwTxn` could lose counts, harmless in a statistic.
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
    /// Test-only: the latest durable steps synced the data with one `fdatasync` of the file (on
    /// macOS also by the `msync`s of `DataSync::Ranges`, which take its place there).
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
    /// Test-only: a cursor's writes pass no routing hint (`write::Hint`), so every write's
    /// descent searches each level, as before the hint.
    #[cfg(test)]
    pub(crate) no_write_hint: bool,
    /// Test-only: deletes do not merge under-full pages (`PageWrite::no_merge`).
    #[cfg(test)]
    pub(crate) no_merge: bool,
    /// Test-only: copy-on-write of a leaf for its change takes the two-step path
    /// (`PageWrite::no_fused_copy`).
    #[cfg(test)]
    pub(crate) no_fused_copy: bool,
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
    /// Test-only: the threads the latest commit's step 3 computed its CRCs on (1: the serial
    /// loop).
    #[cfg(test)]
    pub(crate) stamped_on: usize,
    /// Test-only: the time [`Self::sync_due`]'s period check reads in place of the clock, so a
    /// test reaches `EnvOptions::sync_period` without waiting for it.
    #[cfg(test)]
    pub(crate) clock_at: Option<Instant>,
}

/// [`SyncMode::NoSync`]'s bounds on the commits a sync waits for: [`EnvOptions::sync_period`] and
/// [`EnvOptions::sync_bytes`], at least one of them set.
#[derive(Clone, Copy, Debug)]
struct AutoSync {
    /// [`EnvOptions::sync_period`].
    period: Option<Duration>,
    /// [`EnvOptions::sync_bytes`].
    bytes: Option<u64>,
}

impl AutoSync {
    /// The automatic syncs `options` ask for: `None` in [`SyncMode::Durable`], whose commits are
    /// all durable, and when neither bound is set.
    fn new(options: &EnvOptions) -> Option<Self> {
        let (period, bytes) = (options.sync_period, options.sync_bytes);
        (options.sync == SyncMode::NoSync && (period.is_some() || bytes.is_some()))
            .then_some(Self { period, bytes })
    }
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
    /// After step 3 (the pages' CRCs stamped): the last point of a `SyncMode::NoSync` commit that
    /// defers its sync.
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
    /// Stamp the CRCs of what the transaction wrote (commit step 3): each tree page's, outside its
    /// free gap ([`page::crc`]), and one over each overflow run
    /// ([`crate::alloc::PrivatePages::units`]). Returns every page written,
    /// ascending, for the data sync; the tree pages then count as stamped by this writer.
    ///
    /// Units of many bytes have their CRCs computed on several threads first ([`Self::unit_crcs`]),
    /// which this stores in unit order; else each unit is stamped in turn. Both write the same
    /// bytes in the same order, and fail at the same unit with the same error.
    pub(crate) fn stamp_private(&mut self) -> Result<Vec<u32>> {
        let started = Instant::now();
        let size = self.geometry.page_size();
        let private = self.alloc.stamp_private();
        let threads = stamp::threads(private.pages.len() * size);
        #[cfg(test)]
        let threads = self.stamp_threads.unwrap_or(threads);
        let crcs = self.unit_crcs(&private, threads);
        #[cfg(test)]
        {
            self.stamped_on = if crcs.is_some() { threads } else { 1 };
        }
        match crcs {
            Some(crcs) => {
                for ((p, n, _), crc) in private.units().zip(crcs) {
                    stamp::put_crc(self.file.pages_mut(p, size, n as usize * size)?, crc);
                }
            }
            None => {
                for (p, n, run) in private.units() {
                    stamp::stamp(self.file.pages_mut(p, size, n as usize * size)?, run);
                }
            }
        }
        self.stats.stamp_ns += elapsed_ns(started);
        Ok(private.pages)
    }

    /// The CRC of each of `private`'s units, in unit order, computed on `threads` threads (the
    /// count [`stamp::threads`] gives the units' bytes) from read-only slices of the units
    /// ([`PageFile::pages`]). `None` for one thread, which leaves the units to the serial loop,
    /// and when a unit lies past the end of the file, whose error the serial loop then raises.
    fn unit_crcs(&self, private: &PrivatePages, threads: usize) -> Option<Vec<u32>> {
        let size = self.geometry.page_size();
        if threads < 2 {
            return None;
        }
        let units: Vec<stamp::Unit<'_>> = private
            .units()
            .map(|(p, n, run)| {
                let unit = self.file.pages(p, size, n as usize * size)?;
                Some(if run { stamp::Unit::Run(unit) } else { stamp::Unit::Page(unit) })
            })
            .collect::<Option<_>>()?;
        Some(stamp::crcs(&units, threads))
    }

    /// Whether a [`SyncMode::NoSync`] commit of `txn` that stamped `pages` pages commits durably
    /// ([`Self::sync_with`]) rather than deferring them ([`Self::defer_sync`]): when `txn` is past
    /// [`Self::weak_limit`], more than [`meta::MAX_WEAK_RUN`] txns past the newest steady slot's
    /// (the writer's first commit after an open always is: [`Chosen::next_txn`]); or when the
    /// pages stamped since the last durable state, these included, hold at least
    /// [`EnvOptions::sync_bytes`] (pages times the page size), or at least
    /// [`EnvOptions::sync_period`] has passed since that state ([`Self::durable_at`]). With
    /// neither bound set, a test of two fields; the clock is read only with a period set.
    #[inline]
    pub(crate) fn sync_due(&self, txn: u64, pages: usize) -> bool {
        if txn > self.weak_limit {
            return true;
        }
        let Some(auto) = &self.auto_sync else {
            return false;
        };
        let page_size = self.geometry.page_size() as u64;
        auto.bytes.is_some_and(|bytes| {
            (self.unsynced_pages + pages as u64).saturating_mul(page_size) >= bytes
        }) || auto.period.is_some_and(|period| self.since_durable() >= period)
    }

    /// The time since the last durable state ([`Self::durable_at`]); in tests from
    /// [`Self::clock_at`] when it is set.
    fn since_durable(&self) -> Duration {
        #[cfg(test)]
        if let Some(now) = self.clock_at {
            return now.saturating_duration_since(self.durable_at);
        }
        self.durable_at.elapsed()
    }

    /// A [`SyncMode::NoSync`] commit that defers its sync: record the `pages` it stamped for the
    /// next sync, then copy its `meta` into its slot ([`slot_to_write`]: the one that does not hold
    /// the newest steady meta) as a weak record, through the mapping: no msync, no system call. A
    /// process crash from then on recovers this state in the same boot, whose page cache holds its
    /// pages; a reboot recovers the newest steady one, which this never touches.
    pub(crate) fn defer_sync(&mut self, pages: &[u32], meta: &Meta) -> Result<()> {
        self.stats.nosync_commits += 1;
        self.add_unsynced(pages);
        let slot = self.copy_meta(meta, Tag::weak(self.boot_id))?;
        self.slots[slot] = Some(Slot { txn: meta.txn, weak: true });
        Ok(())
    }

    /// Copy `meta`'s record, tagged `tag`, into the slot [`slot_to_write`] picks, and return the
    /// slot. Nothing is synced, and the caller records the slot's new record.
    fn copy_meta(&mut self, meta: &Meta, tag: Tag) -> Result<usize> {
        let slot = slot_to_write(&self.slots);
        let size = self.geometry.page_size();
        self.file.page_mut(slot as u32, size)?[..META_LEN].copy_from_slice(&meta.encode(tag));
        Ok(slot)
    }

    /// Add `pages`, stamped by a [`SyncMode::NoSync`] commit, to [`Self::unsynced`].
    fn add_unsynced(&mut self, pages: &[u32]) {
        self.unsynced_pages += pages.len() as u64;
        for &p in pages {
            self.unsynced.insert(p);
        }
    }

    /// Empty [`Self::unsynced`]: its pages, ascending.
    fn take_unsynced(&mut self) -> Vec<u32> {
        let pages: Vec<u32> = self.unsynced.drain().map(|p| p as u32).collect();
        debug_assert_eq!(
            pages.len() as u64,
            self.unsynced_pages,
            "a page a NoSync commit stamped was stamped again before a sync"
        );
        self.unsynced_pages = 0;
        pages
    }

    /// A [`SyncMode::NoSync`] commit past a bound of [`Self::sync_due`], committing durably: the
    /// durable steps (4–6, [`Self::make_durable`]) over every page stamped since the last sync and
    /// `pages`, the commit's own, with its `meta`, exactly as a [`SyncMode::Durable`] commit runs
    /// them. The pages stay ascending for [`data_sync`], and distinct: a page a `NoSync` commit
    /// stamped is not reused before a sync. An error leaves the commit unpublished, and the
    /// caller latches, as for a durable commit.
    pub(crate) fn sync_with(&mut self, pages: &[u32], meta: &Meta) -> Result<()> {
        self.add_unsynced(pages);
        let pages = self.take_unsynced();
        self.make_durable(&pages, meta)?;
        self.stats.auto_syncs += 1;
        Ok(())
    }

    /// The durable steps of a commit (4–6): sync `pages` (ascending) by the one `msync` or the
    /// one `fdatasync` [`data_sync`] picks (on macOS the `msync`s of `DataSync::Ranges` instead
    /// of the `fdatasync`), then the file's size if it grew (a no-op after the `fdatasync`, which
    /// syncs the size too), then write `meta` as a steady record into the slot that does not hold
    /// the newest steady one ([`slot_to_write`]: with both steady, the lower txn; beside a weak
    /// record, the weak one) and sync that page.
    pub(crate) fn make_durable(&mut self, pages: &[u32], meta: &Meta) -> Result<()> {
        let size = self.geometry.page_size();
        let (plan, runs) = data_sync_runs(pages, size as u64);
        // Auto chooses for a scattered step only, one whose pages `data_sync` syncs by the
        // `fdatasync` (on macOS the `msync`s of `DataSync::Ranges`), and learns from those
        // alone: one range of a few runs has a few writes to save. The plan's, not the tests'.
        let scattered = !matches!(plan, DataSync::Nothing | DataSync::Range(_));
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
            #[cfg(target_os = "macos")]
            {
                self.data_by_file |= plan == DataSync::Ranges;
            }
        }
        let grown = self.file.size_unsynced();
        let mut clock = Instant::now();
        self.stats.sync_runs += runs as u64;
        let choice = match &mut self.gap_fill {
            GapWidth::Auto(auto) if scattered => Some(auto.choose(&Gaps::of(pages))),
            _ => None,
        };
        let choose_ns = if choice.is_some() { lap(&mut clock) } else { 0 };
        let width = match (&self.gap_fill, choice) {
            (GapWidth::Fixed(width), _) => *width,
            (GapWidth::Auto(_), choice) => choice.map_or(0, |c| c.width),
        };
        let fill = if width > 0 && plan != DataSync::Nothing {
            Some(self.fill_gaps(pages, width)?)
        } else {
            None
        };
        let fill_ns = if fill.is_some() { lap(&mut clock) } else { 0 };
        self.stats.gap_fill_ns += choose_ns + fill_ns;
        match plan {
            DataSync::Nothing => {}
            DataSync::Range(range) => {
                let span = (range.end - range.start) / size as u64;
                self.file.msync_range(range)?;
                self.stats.msync_syncs += 1;
                self.stats.msync_span_pages += span;
            }
            #[cfg(target_os = "macos")]
            DataSync::Ranges => {
                self.file.msync_ranges(merged_ranges(pages, size as u64))?;
                self.stats.fdatasync_syncs += 1;
            }
            DataSync::File => {
                self.file.fdatasync_all()?;
                self.stats.fdatasync_syncs += 1;
            }
        }
        self.stats.pages_synced += pages.len() as u64;
        let sync_ns = lap(&mut clock);
        self.stats.data_sync_ns += sync_ns;
        #[cfg(test)]
        self.crash_point(CrashAt::AfterDataSync)?;
        self.file.sync_size_if_grown()?;
        self.stats.size_sync_ns += lap(&mut clock);
        #[cfg(test)]
        self.crash_point(CrashAt::AfterSizeSync)?;
        let slot = self.copy_meta(meta, Tag::steady(self.boot_id))?;
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaCopy)?;
        self.file.msync_page(slot as u32, size)?;
        self.slots[slot] = Some(Slot { txn: meta.txn, weak: false });
        if meta.txn >= self.skipped.end {
            // A state this writer committed: its later commits may run weak from it. A sync of
            // the state the open found leaves the limit below the writer's first txn, so its
            // first commit is still durable.
            self.weak_limit = meta::weak_limit(&self.slots);
        }
        self.stats.meta_sync_ns += lap(&mut clock);
        self.durable_at = clock;
        self.stats.commits += 1;
        self.stats.grown_commits += u64::from(grown);
        if let Some(choice) = choice {
            // After the meta's sync, so the fit's time never delays the state's durability.
            let fill = fill.unwrap_or_default();
            let outcome = Outcome {
                runs: runs as u64 - fill.filled,
                pages: pages.len() as u64 + fill.pages,
                sync_ns,
                fill_ns,
                tried: fill.filled + fill.missed,
                filled: fill.filled,
            };
            self.observe_auto(choice, outcome);
        }
        #[cfg(test)]
        self.crash_point(CrashAt::AfterMetaMsync)?;
        Ok(())
    }

    /// Step 4's gap fill ([`EnvOptions::gap_fill`]), before the data sync: rewrite each gap of
    /// at most `width` clean pages between two of `pages` (ascending: the pages the sync makes
    /// durable) with the bytes it holds, so the sync writes the runs on either side and the gap
    /// as one run (README, "Gap fill"). Returns what it did, which [`IoStats`] counts too.
    ///
    /// Each gap is read into [`Self::gap_buf`] through the file, not the mapping
    /// (`PageFile::read_resident`: on Linux only a gap the page cache holds whole), then written
    /// back from that buffer at its own offset (`PageFile::rewrite`). Never through the mapping:
    /// readers may hold slices of any page but this transaction's own, and a store through the
    /// mapping would be a write under their shared borrows. The kernel's copy into the cached
    /// page stores each byte's own value, so no load, a reader's or the writer's, can see any
    /// other, whenever it runs.
    ///
    /// **Why rewriting a page with its own bytes is safe, whatever the page is.** Only the writer
    /// changes pages, on this thread, and nothing changes one between this read and this write:
    /// the gap holds no page of this commit (those are `pages`), and the metas (pages 0 and 1)
    /// are never between two of `pages`. So the write stores the bytes the page cache already
    /// holds, and a clean cached page holds what the disk holds. A gap page is one of: a page of
    /// a state a slot or a reader names (the last durable state's, freed by this commit or
    /// earlier and not yet past the reuse gate, or still live); a free page; a page this
    /// transaction took and gave back; or a page an aborted transaction or a dead writer wrote,
    /// which no slot names. Each keeps its bytes. A crash at any point leaves each sector of the
    /// gap either not rewritten or rewritten, and both hold the same bytes, so a torn write
    /// cannot change a page, on a drive that writes each logical block atomically (NVMe, cloud
    /// block storage). A drive that can garble a block it was writing when the power failed
    /// could now damage a page of the last durable state, which without gap fill no commit
    /// writes (README, "Gap fill"); the page CRCs would report it as `Corrupt`, never read it
    /// as data.
    ///
    /// A gap not in memory is skipped and counted ([`IoStats::gap_misses`]); once a commit's
    /// misses exceed its fills by [`GAP_MISS_SLACK`] it stops trying. A kernel or file system that
    /// refuses the residency check turns gap fill off for the environment, with a warning: the
    /// gaps filled so far keep their bytes. An I/O error reading or writing a gap fails the
    /// commit and poisons the file (a failed write leaves each byte as it was or rewritten).
    fn fill_gaps(&mut self, pages: &[u32], width: u32) -> Result<Fill> {
        let size = self.geometry.page_size();
        let (mut filled, mut missed, mut filled_pages) = (0u64, 0u64, 0u64);
        for w in pages.windows(2) {
            let clean = w[1] - w[0] - 1;
            if clean == 0 || clean > width {
                continue;
            }
            if missed > filled + GAP_MISS_SLACK {
                break;
            }
            let offset = u64::from(w[0] + 1) * size as u64;
            let len = clean as usize * size;
            if self.gap_buf.len() < len {
                self.gap_buf.resize(len, 0);
            }
            let buf = &mut self.gap_buf[..len];
            match self.file.read_resident(offset, buf)? {
                Resident::Whole => {
                    self.file.rewrite(offset, buf)?;
                    filled += 1;
                    filled_pages += u64::from(clean);
                }
                Resident::NotWhole => missed += 1,
                Resident::Unsupported(err) => {
                    tracing::warn!(
                        "tn-lmdb: gap fill turned off: the file refuses reads with RWF_NOWAIT \
                         ({err})"
                    );
                    self.gap_fill = GapWidth::Fixed(0);
                    self.gap_buf = Vec::new();
                    break;
                }
            }
        }
        self.stats.gaps_filled += filled;
        self.stats.gap_pages += filled_pages;
        self.stats.gap_misses += missed;
        Ok(Fill { filled, missed, pages: filled_pages })
    }

    /// File a scattered step's `outcome` with the automatic width ([`GapFill::Auto`]) that chose
    /// `choice` for it, and publish its choice and fit in [`IoStats`]'s `gap_auto_*` fields. Does
    /// nothing once gap fill turned itself off ([`Self::fill_gaps`]).
    fn observe_auto(&mut self, choice: Choice, outcome: Outcome) {
        let GapWidth::Auto(auto) = &mut self.gap_fill else { return };
        let readapted = auto.observe(outcome);
        let model = auto.model();
        let s = &mut self.stats;
        s.gap_auto_steps += 1;
        s.gap_auto_probes += u64::from(choice.probe);
        s.gap_auto_readapts += u64::from(readapted);
        s.gap_auto_width_sum += u64::from(choice.width);
        s.gap_auto_width = u64::from(choice.width);
        s.gap_auto_run_ns = model.per_run.round() as u64;
        s.gap_auto_page_ns = model.per_page.round() as u64;
        s.gap_auto_gap_ns = auto.fill_cost_ns().round() as u64;
        s.gap_auto_capped = u64::from(model.capped);
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

    /// Make the newest committed state durable unless a steady slot already holds it: after
    /// [`SyncMode::NoSync`] commits that deferred their sync, or an open that trusted a weak head
    /// (in either mode) and has committed nothing since (its first commit is durable). The check is
    /// against the newest *steady* slot: a weak slot may hold the newest state's txn, and its
    /// pages may be only in the page cache. A failure latches. The caller checks the latch
    /// first.
    fn sync(&mut self, latch: &Latch) -> Result<()> {
        if self.steady_txn().is_some_and(|txn| txn >= self.last.txn) {
            return Ok(());
        }
        let guard = UnwindLatch(latch, "a sync panicked");
        let pages = self.take_unsynced();
        let last = self.last;
        let synced =
            self.make_durable(&pages, &last).inspect_err(|e| latch.fail(format!("sync: {e}")));
        guard.disarm();
        synced
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

    /// The txn of the newest steady meta the slots hold: the last state made durable.
    fn steady_txn(&self) -> Option<u64> {
        meta::newest_steady(&self.slots)
    }

    /// The reuse gate: the latest txn whose free records may be reused, the lowest of the txns
    /// the valid meta slots hold, weak or steady, and of the oldest snapshot a reader may hold. A
    /// page freed by a transaction no later than it is unreachable from every state a crash can
    /// recover (the newest steady meta's, a weak head's in the same boot) and from every live
    /// snapshot: each is the writer's base or an earlier state of its chain, at or past the gate,
    /// and the base's free list still lists the page (README, Free list and page reuse). With a
    /// weak slot the gate is the steady slot's txn, the lower one, so a `SyncMode::NoSync`
    /// commit's freed pages wait for a sync.
    pub(crate) fn reuse_gate(&mut self) -> u64 {
        let recoverable = self.slots.iter().flatten().map(|slot| slot.txn).min().unwrap_or(0);
        recoverable.min(self.oldest_reader_txn())
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
        // A gate from before the open is behind by the txn ids the open skipped too, which no
        // commit took: the lag counts commits.
        let skipped =
            if gate < self.skipped.start { self.skipped.end - self.skipped.start } else { 0 };
        let lag = self.alloc.txn().saturating_sub(gate).saturating_sub(skipped);
        self.stats.gate_lag_sum = self.stats.gate_lag_sum.saturating_add(lag);
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

    /// Give back the file's preallocated tail at a clean close ([`EnvInner::drop`], after the
    /// final sync): truncate the file to [`Self::trim_len`] when that is shorter
    /// ([`PageFile::trim_to`]). An error leaves the file as it was, or longer than its pages: never
    /// shorter than a state a meta slot names.
    fn trim_tail(&mut self) -> Result<()> {
        let len = self.trim_len()?;
        if len >= self.file.file_len() {
            return Ok(());
        }
        self.file.trim_to(len)
    }

    /// The length a clean close may truncate the file to: [`Self::used_len`] rounded up to the OS
    /// page.
    fn trim_len(&self) -> Result<u64> {
        let used = self.used_len()?;
        used.checked_next_multiple_of(sys::os_page_size())
            .ok_or_else(|| Error::Failed(format!("no length holds {used} bytes")))
    }

    /// The bytes of the file's pages in use: the most pages any state a meta slot holds names,
    /// weak or steady, and never fewer than the newest state's ([`Self::last`]), times the page
    /// size. A file cut to it still opens to every state its slots hold: the clean close's trim
    /// ([`Self::trim_len`]), and open's cut of an inherited tail ([`PageFile::distrust_tail`]). A
    /// torn or invalid slot (`None`) names no state an open can recover, so it is left out; a
    /// valid slot whose record in the file does not decode to its txn and kind is one this writer
    /// cannot account for, and is [`Error::Failed`].
    ///
    /// The page count never shrinks from one committed state to the next (the allocator's next
    /// page number only grows within a transaction, and an abort puts it back), so the newest
    /// state's count is the largest; the slots' are read anyway, which costs an open or a close
    /// nothing.
    fn used_len(&self) -> Result<u64> {
        let size = self.geometry.page_size();
        let mut pages = self.last.page_count;
        for (i, slot) in self.slots.iter().enumerate() {
            let Some(Slot { txn, weak }) = *slot else { continue };
            let record = self.file.page(i as u32, size).and_then(|page| page.first_chunk());
            match record.map(Meta::decode) {
                Some(Ok((meta, tag))) if meta.txn == txn && tag.weak == weak => {
                    pages = pages.max(meta.page_count)
                }
                _ => {
                    return Err(Error::Failed(format!(
                        "meta slot {i} does not hold the state of txn {txn} (weak: {weak})"
                    )))
                }
            }
        }
        Ok(u64::from(pages) * size as u64)
    }

    /// Test-only: what the meta slots hold.
    #[cfg(test)]
    pub(crate) fn slots(&self) -> [Option<Slot>; 2] {
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

    fn base_page_count(&self) -> u32 {
        self.alloc.base_page_count()
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
    /// stay exactly [`PageRead::page`]'s. Forced inline: `f` is a leaf's change on the write path,
    /// whose outcome stays in registers (`page::LeafMut`'s docs).
    #[inline(always)]
    fn copy_published_with<F>(&mut self, from: u32, to: u32, f: F) -> Result<&mut [u8]>
    where
        F: FnOnce(&[u8], &mut [u8]) -> Result<()>,
    {
        if self.alloc.is_private(from) {
            return Err(private_source(from));
        }
        self.stats.cow_copies += 1;
        let size = self.geometry.page_size();
        let Some(src) = self.view.slice(u64::from(from) * size as u64, size) else {
            return copy_unpublished(self, from, to, f);
        };
        let src =
            checked_page(from, self.alloc.page_count(), self.alloc.txn(), false, |_| Some(src))?;
        if !self.alloc.is_private(to) {
            return Err(not_private(to));
        }
        let dst = self.file.page_mut(to, size)?;
        f(src, dst)?;
        Ok(dst)
    }

    #[cfg(test)]
    fn no_fused_copy(&self) -> bool {
        self.no_fused_copy
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

    fn take_scratch(&mut self) -> Box<Scratch> {
        self.scratch.take().unwrap_or_default()
    }

    fn put_scratch(&mut self, scratch: Box<Scratch>) {
        self.scratch = Some(scratch);
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

/// [`Writer::copy_published_with`]'s error for a source page that is private.
#[cold]
#[inline(never)]
fn private_source(pgno: u32) -> Error {
    Error::Failed(format!("page {pgno} is private: it needs no copy"))
}

/// [`Writer::copy_published_with`] for a page past the published bytes (see there): fetched
/// through the file with [`PageRead::page`]'s checks into a buffer of its own, which `f` then
/// copies from. No sound tree reaches it.
#[cold]
#[inline(never)]
fn copy_unpublished<F>(writer: &mut Writer, from: u32, to: u32, f: F) -> Result<&mut [u8]>
where
    F: FnOnce(&[u8], &mut [u8]) -> Result<()>,
{
    let src = writer.page(from)?.to_vec();
    let dst = writer.page_mut(to)?;
    f(&src, dst)?;
    Ok(dst)
}

/// How step 4 syncs dirty `pages` (ascending). Runs of adjacent pages separated by at most
/// [`MERGE_GAP`] clean pages merge into one range. Pages that form exactly one range, in at most
/// [`MAX_RUNS`] runs, get one `msync` of it; any other set gets one `fdatasync` of the file (on
/// macOS the `msync`s of `DataSync::Ranges`: see below).
///
/// - **Two or more ranges:** on Linux every `msync(MS_SYNC)` runs the file system's fsync path,
///   drive-cache flush included, so k ranges cost k flushes where one `fdatasync` costs one.
/// - **One range:** one `msync` is one flush on Linux too, and on macOS it beats an `fdatasync` of
///   the file for a large commit (a bulk load's one range), which `fdatasync` writes slower.
/// - **More than [`MAX_RUNS`] runs** (a commit whose copies took pages freed all over the file) get
///   the `fdatasync` (on macOS the two passes) even when they merge into one range: one
///   `msync(MS_SYNC)` writes its range's dirty pages one cluster at a time on macOS, three to four
///   times the cost of starting every write first, as the `MS_ASYNC` pass does.
///
/// Either way the data is durable before the meta is written. `fdatasync` also writes any other
/// dirty page of the file, which at a commit can only be a page of an aborted transaction: a free
/// page, so writing it is harmless.
///
/// **macOS** takes `DataSync::Ranges` where Linux takes the `fdatasync`: a plain `fsync` there
/// (`sys::fdatasync_file`) does not write every page dirtied through a shared mapping, and only
/// `msync` does (README, Platform notes; do-not-flag item 30). The `MS_ASYNC` pass starts the
/// writes of every range at once, and the `MS_SYNC` pass after it waits for them (as it waits for
/// any write in flight) and writes whatever is still dirty: one `MS_SYNC` per range alone writes
/// one range at a time, about three times slower on a commit of a few thousand scattered pages,
/// and one `MS_SYNC` of the span from the first page to the last walks every resident page in it,
/// a cost that grows with the file. A scattered commit of a mature file of 1–4 GiB syncs this way
/// three to five times faster than by the `fsync`, which also skipped pages.
#[cfg(test)]
fn data_sync(pages: &[u32], page_size: u64) -> DataSync {
    data_sync_runs(pages, page_size).0
}

/// [`data_sync`]'s choice for `pages`, with the runs of adjacent pages they form (0 for none),
/// which its loop counts anyway ([`IoStats::sync_runs`]).
fn data_sync_runs(pages: &[u32], page_size: u64) -> (DataSync, usize) {
    let (Some(&first), Some(&last)) = (pages.first(), pages.last()) else {
        return (DataSync::Nothing, 0);
    };
    let (mut runs, mut ranges) = (1, 1);
    for w in pages.windows(2) {
        let clean = w[1] - w[0] - 1;
        runs += usize::from(clean > 0);
        ranges += usize::from(clean > MERGE_GAP);
    }
    if runs > MAX_RUNS || ranges > 1 {
        #[cfg(target_os = "macos")]
        return (DataSync::Ranges, runs);
        #[cfg(not(target_os = "macos"))]
        return (DataSync::File, runs);
    }
    (DataSync::Range(u64::from(first) * page_size..(u64::from(last) + 1) * page_size), runs)
}

/// The byte ranges dirty `pages` (ascending) merge into, as [`data_sync`] counts them: runs of
/// adjacent pages separated by at most [`MERGE_GAP`] clean pages form one range.
#[cfg(target_os = "macos")]
fn merged_ranges(pages: &[u32], page_size: u64) -> impl Iterator<Item = Range<u64>> + Clone + '_ {
    pages.chunk_by(|a, b| b - a - 1 <= MERGE_GAP).map(move |range| {
        let (first, last) = (range[0], range[range.len() - 1]);
        u64::from(first) * page_size..(u64::from(last) + 1) * page_size
    })
}

impl Env {
    /// Open the database file at `path`, creating it if it is missing or empty, and recover the
    /// newest committed state (see the module docs). Errors: [`Error::Locked`] if another handle
    /// holds it open; [`Error::NotTnLmdb`], [`Error::UnsupportedVersion`] or [`Error::Corrupt`]
    /// for a file that cannot be opened safely (left unchanged); [`Error::InvalidPageSize`] for
    /// an invalid `options.page_size`; [`Error::InvalidMaxGrowth`] for an `options.max_growth`
    /// under [`MIN_GROWTH`](crate::MIN_GROWTH); [`Error::InvalidMapReserve`] for an
    /// `options.map_reserve` past `isize::MAX` (all three refused before the file is touched);
    /// [`Error::OutOfMemory`] for a reservation the host refuses (a missing file is created
    /// first); [`Error::Io`] `NotFound` for a symbolic link at `path` to a missing or empty file,
    /// and `InvalidInput` naming it for a `<path>.creating` that is a symbolic link or not a
    /// regular file when the open would create the file (each left as it is).
    pub fn open(path: impl AsRef<Path>, options: EnvOptions) -> Result<Env> {
        let path = path.as_ref();
        Geometry::new(options.page_size)?;
        if options.max_growth < MIN_GROWTH {
            return Err(Error::InvalidMaxGrowth(options.max_growth));
        }
        if options.map_reserve > isize::MAX as u64 {
            return Err(Error::InvalidMapReserve(options.map_reserve));
        }
        if let GapFill::Pages(pages) = options.gap_fill {
            if pages > MAX_GAP_FILL {
                return Err(Error::InvalidGapFill(pages));
            }
        }
        let file = open_locked(path, options.page_size)?;
        let boot_id = sys::boot_id();
        let trusted = options.trust_unsynced.then_some(boot_id).flatten();
        let (chosen, geometry) = choose_state(&file, trusted)?;
        // A crash may have left a meta in the page cache whose msync never finished, a weak head
        // whose pages a dead writer left there, or a created file whose name never reached the
        // disk (a `create` that died, or failed, before its directory sync): make what this open
        // reads, and the name it found it by, durable before anything is built on them (a
        // directory that cannot be synced is skipped, with a warning: `sys::fsync_directory`).
        // A weak head stays weak on disk: the next sync writes its steady meta.
        sys::fsync_file(&file)?;
        sys::fsync_directory(parent_dir(path))?;
        let file =
            PageFile::from_file(file, options.map_reserve, options.readahead, options.max_growth)?;
        // macOS: that fsync leaves pages a crashed writer dirtied through its mapping unwritten (a
        // trusted weak head's among them), and only an msync writes them (README, Platform
        // notes). Every mapping of the file shares its pages, so an msync of this one does, once,
        // before anything is built on them.
        #[cfg(target_os = "macos")]
        file.msync_range(0..file.file_len())?;
        let snapshot = opened_snapshot(&chosen, geometry, file.view())?;
        let meta = snapshot.meta;
        let skipped = chosen.skipped_txns().expect("choose_state refuses a slot with no next txn");
        let mut writer = Writer {
            view: file.view(),
            file,
            geometry,
            alloc: Allocator::new(skipped.end, meta.page_count),
            last: meta,
            slots: chosen.slots,
            boot_id: boot_id.unwrap_or_default(),
            unsynced: PageSet::default(),
            unsynced_pages: 0,
            weak_limit: meta::weak_limit(&chosen.slots),
            auto_sync: AutoSync::new(&options),
            durable_at: Instant::now(),
            skipped,
            readers: VecDeque::from([Published::current(&snapshot)]),
            readers_kept: 1,
            scratch: None,
            gap_fill: match options.gap_fill {
                GapFill::Pages(pages) => GapWidth::Fixed(pages),
                GapFill::Auto => GapWidth::Auto(Box::default()),
            },
            gap_buf: Vec::new(),
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
            no_write_hint: false,
            #[cfg(test)]
            no_merge: false,
            #[cfg(test)]
            no_fused_copy: false,
            #[cfg(test)]
            merging_deletes: 0,
            #[cfg(test)]
            record_pages: None,
            #[cfg(test)]
            stamp_threads: None,
            #[cfg(test)]
            stamped_on: 0,
            #[cfg(test)]
            clock_at: None,
        };
        // A tail past the pages in use is an earlier process's, maybe sparse: the first growth
        // past those pages cuts it and grows again, preallocated. Nothing is written here.
        let used = writer.used_len()?;
        writer.file.distrust_tail(used);
        if writer.gap_fill.is_on() {
            // A gap fill's read that misses the page cache starts a read of the gap: with this
            // advice, of the gap's pages only. The engine reads the file only through the
            // mapping otherwise, which the advice does not touch.
            writer.file.advise_random_reads();
        }
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
    /// an empty one [`Error::NotTnLmdb`]; [`Error::Locked`] if a handle with a writer holds it.
    /// `options.page_size` is validated as [`Self::open`] validates it, `options.readahead`
    /// advises the mapping as [`Self::open`]'s, and `options.trust_unsynced` chooses the meta as
    /// in [`Self::open`]; no other field applies.
    pub fn open_read_only(path: impl AsRef<Path>, options: EnvOptions) -> Result<Env> {
        Geometry::new(options.page_size)?;
        let file = open_shared(path.as_ref())?;
        let trusted = options.trust_unsynced.then(sys::boot_id).flatten();
        let (chosen, geometry) = choose_state(&file, trusted)?;
        // No sync, unlike `open`: nothing is built on what this open reads.
        let file = ReadOnlyFile::from_file(file, options.readahead)?;
        let snapshot = opened_snapshot(&chosen, geometry, file.view())?;
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
    /// last sync and write the newest state's steady meta (a no-op when no state is pending: a
    /// steady meta holds the newest one; and in [`SyncMode::Durable`] but after an open that
    /// trusted a weak head, which this makes steady). Waits while a write transaction is open, as
    /// [`Self::begin_rw`] does, in every mode. A failure latches the environment, and once it is
    /// latched this is [`Error::Failed`] in every mode. In an environment opened read-only it
    /// does nothing and returns `Ok(())`, latched or not.
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
    /// leaked (used by nothing) and used twice (the first 16 runs and pages named, the rest
    /// counted), and latches the environment. It keeps 4 bytes per page of the state's page count
    /// in an anonymous mapping, of which only the pages the walk touches cost memory; a host that
    /// refuses that mapping is [`Error::OutOfMemory`], which latches as any failed check does. It
    /// reads through a snapshot, alongside readers and the writer.
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
    /// run when it is longer than its leaf holds inline) for every key up to
    /// [`max_key_len`](Self::max_key_len), whose leaf entry always leaves room for the run's
    /// 12-byte reference; 0 when `klen` is over [`max_key_len`](Self::max_key_len): such a key is
    /// refused with any value.
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
        if !locked.is_file() {
            // Another type put at the path after `needs_create` refused none.
            return Err(not_regular(path));
        }
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
/// empty one is left to [`choose_meta`], which finds no meta in it: [`Error::NotTnLmdb`]. A path
/// naming a file of another type than a regular file (followed through symbolic links) is
/// refused before it is opened ([`not_regular`]): a FIFO's open would block until a writer
/// opened it. The open is non-blocking too, so a FIFO put there after the check is refused as
/// well, without waiting.
fn open_shared(path: &Path) -> Result<File> {
    for _ in 0..3 {
        if fs::metadata(path).is_ok_and(|named| !named.is_file()) {
            return Err(not_regular(path));
        }
        let file = OpenOptions::new().read(true).custom_flags(libc::O_NONBLOCK).open(path)?;
        let locked = file.metadata()?;
        if !locked.is_file() {
            return Err(not_regular(path));
        }
        if !sys::flock_shared(&file)? {
            return Err(Error::Locked);
        }
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

/// True if the file at `path` is missing or empty: open creates it. A symbolic link at `path` is
/// followed only to a file holding bytes: one whose target is missing or empty is refused,
/// [`Error::Io`] (`NotFound`) naming the link, and left as it is. [`create`]'s rename would
/// replace the link itself with a new, empty database, leaving the one the link names (on a disk
/// not mounted, say) behind. A file of another type than a regular file at `path`, or at the
/// link's end (a FIFO, a socket or a device, whose length is 0, or a directory), is refused
/// ([`not_regular`]) and left as it is, never replaced by a new database.
fn needs_create(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {}
        Ok(meta) if meta.is_file() => return Ok(meta.len() == 0),
        Ok(_) => return Err(not_regular(path)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(e.into()),
    }
    let refused = |target: &str| {
        Error::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "the database path {} is a symbolic link to {target}: refused, as creating a \
                 database there would replace the link",
                path.display()
            ),
        ))
    };
    match fs::metadata(path) {
        Ok(target) if !target.is_file() => Err(not_regular(path)),
        Ok(target) if target.len() > 0 => Ok(false),
        Ok(_) => Err(refused("an empty file")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(refused("a missing file")),
        Err(e) => Err(e.into()),
    }
}

/// The refusal of a database path naming a file of another type than a regular file (a FIFO, a
/// socket, a device, a directory), [`Error::Io`] (`InvalidInput`) naming it, as [`create`]
/// refuses such a temporary file: an open neither replaces it nor reads it.
#[cold]
fn not_regular(path: &Path) -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("the database path {} is not a regular file: refused", path.display()),
    ))
}

/// Create the file at `path` atomically (open step 2): both metas of a new file (txn 0, no
/// tables) are written to `<path>.creating` (truncating any leftover; a symbolic link or any
/// other file that is not a regular file there is refused, [`Error::Io`], and left as it is),
/// synced, and renamed over the path, and the directory is synced. The temporary file is locked
/// while it is built: one another creator holds locked is [`Error::Locked`] at once. A file
/// another handle created meanwhile is kept, and a path [`needs_create`] now refuses is refused:
/// either way the temporary file is then removed if unused ([`remove_unused`]).
fn create(path: &Path, page_size: u32) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".creating");
    let tmp = PathBuf::from(tmp);
    // Never through a symbolic link (`O_NOFOLLOW`), whose target the truncate and the writes
    // below would overwrite, and only into a regular file.
    let refused = |what: &str| {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is {what}: refused as a new database's temporary file", tmp.display()),
        ))
    };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&tmp)
        .map_err(|e| match e.raw_os_error() {
            Some(libc::ELOOP) => refused("a symbolic link"),
            Some(libc::EISDIR) => refused("not a regular file"),
            _ => e.into(),
        })?;
    if !file.metadata()?.is_file() {
        return Err(refused("not a regular file"));
    }
    if !sys::flock_exclusive(&file)? {
        return Err(Error::Locked);
    }
    if !needs_create(path).inspect_err(|_| remove_unused(&tmp, &file))? {
        remove_unused(&tmp, &file);
        return Ok(());
    }
    file.set_len(0)?;
    let p = page_size as usize;
    let record = Meta::initial(page_size).encode(Tag::steady(sys::boot_id().unwrap_or_default()));
    let mut metas = vec![0; 2 * p];
    metas[..META_LEN].copy_from_slice(&record);
    metas[p..p + META_LEN].copy_from_slice(&record);
    file.write_all_at(&metas, 0)?;
    sys::fdatasync_file(&file)?;
    fs::rename(&tmp, path)?;
    sys::fsync_directory(parent_dir(path))?;
    Ok(())
}

/// The directory holding the entry `path` names (`.` for a bare file name): the one a [`create`]
/// renames into and [`Env::open`] step 7 syncs.
fn parent_dir(path: &Path) -> &Path {
    path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."))
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

/// Choose the state an open recovers from `file`, locked (open steps 4–6): the meta
/// [`choose_meta`] picks, trusting weak metas of the boot `trusted` (`None`: none), and its
/// geometry, refusing a slot's txn id with no next one ([`Chosen::next_txn`]) and a file shorter
/// than the meta's pages. A trusted weak head the file is too short for lost its growth with the
/// volume: the open falls back to the last sync beside it ([`fall_back`]), whose pages the file
/// must hold. Reads only.
fn choose_state(file: &File, trusted: Option<BootId>) -> Result<(Chosen, Geometry)> {
    let mut chosen = choose_meta(|offset| read_record(file, offset), trusted)?;
    let geometry = Geometry::new(chosen.meta.page_size)?;
    if chosen.next_txn().is_none() {
        return Err(Error::Corrupt("open: a meta's txn id leaves no next txn".to_string()));
    }
    let len = file.metadata()?.len();
    let needed = |meta: &Meta| u64::from(meta.page_count) * u64::from(meta.page_size);
    if let Some(last_sync) = chosen.last_sync.filter(|_| len < needed(&chosen.meta)) {
        let why =
            format!("the file is {len} bytes, shorter than its {} pages", chosen.meta.page_count);
        chosen =
            Chosen { meta: fall_back(chosen.meta, last_sync, &why), last_sync: None, ..chosen };
    }
    let meta = chosen.meta;
    if len < needed(&meta) {
        return Err(Error::Corrupt(format!(
            "open: the file is {len} bytes, shorter than the {} pages of its newest meta (txn {})",
            meta.page_count, meta.txn
        )));
    }
    Ok((chosen, geometry))
}

/// The snapshot an open recovers, read through `view` (the file mapped by open step 8): the
/// state `chosen` names once its roots pass [`check_root`] (open step 9). A trusted weak head
/// ([`Chosen::last_sync`]) must also pass [`check::check_since`], the check of every page it names
/// that was written since the last sync; if it fails in any way (its roots too), the open falls
/// back to the last sync, with a warning, as for a torn newest record: the volume may have been
/// lost in this boot, and those pages with it (README, Open and recovery). Reads only.
fn opened_snapshot(
    chosen: &Chosen,
    geometry: Geometry,
    view: Arc<MapView>,
) -> Result<Arc<Snapshot>> {
    let Some(last_sync) = chosen.last_sync else {
        return checked_snapshot(chosen.meta, geometry, view);
    };
    let head = chosen.meta;
    let checked = checked_snapshot(head, geometry, Arc::clone(&view))
        .and_then(|snapshot| Ok((check::check_since(&snapshot, last_sync.txn)?, snapshot)));
    match checked {
        Ok((_pages, snapshot)) => {
            #[cfg(test)]
            WEAK_HEAD_CHECK.with(|last| last.set(Some(Ok(_pages))));
            Ok(snapshot)
        }
        Err(e) => checked_snapshot(fall_back(head, last_sync, &e), geometry, view),
    }
}

/// Fall back from the trusted weak head `head`, whose state failed open's check (`why`), to the
/// last sync beside it, with a warning: the meta to open at.
fn fall_back(head: Meta, last_sync: Meta, why: &dyn std::fmt::Display) -> Meta {
    tracing::warn!(
        target: "tn_lmdb",
        "open: the weak meta of txn {} names pages written since the last sync (txn {}) that \
         fail their check, so the open falls back to the last sync: {why}",
        head.txn,
        last_sync.txn
    );
    #[cfg(test)]
    WEAK_HEAD_CHECK.with(|last| last.set(Some(Err(()))));
    last_sync
}

#[cfg(test)]
thread_local! {
    /// Test-only: what this thread's last open found when it checked a trusted weak head
    /// ([`opened_snapshot`]): the pages it checked, or `Err` when it fell back to the last sync.
    /// Read and cleared by [`take_weak_head_check`].
    static WEAK_HEAD_CHECK: std::cell::Cell<Option<std::result::Result<u64, ()>>> =
        const { std::cell::Cell::new(None) };
}

/// Test-only: what this thread's last open found when it checked a trusted weak head since the
/// last sync (`Some(Ok(pages))`: it passed, `Some(Err(()))`: it fell back), and `None` if no open
/// checked one since the last call.
#[cfg(test)]
pub(crate) fn take_weak_head_check() -> Option<std::result::Result<u64, ()>> {
    WEAK_HEAD_CHECK.with(std::cell::Cell::take)
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
    if page::crc_state(page) != CrcState::Valid {
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
mod gap_fill_tests;
#[cfg(test)]
mod io_stats_tests;
#[cfg(test)]
pub(crate) mod tests;
