//! Random-access advice for the execution database's memory map, and the storage counters each
//! persist reports.
//!
//! # The advice
//!
//! libmdbx maps the whole upper size limit of `mdbx.dat` once, when the environment opens, and
//! advises the kernel that the map is read at random (`MADV_RANDOM`) only over the pages in use at
//! that moment. It widens the advised range only when the file grows a step. A node that re-opens
//! its database and never crosses a growth step therefore allocates every new page in a range
//! without the advice, where a page-cache miss reads around the faulting page into large folios.
//! Under `WRITEMAP` the kernel writes back a whole folio for every page dirtied in it: bench 5 (run
//! r20261107-1504) wrote 4.3 device bytes for every byte MDBX dirtied.
//!
//! With the `mdbx_random_advice` switch on, [`MdbxAdvice::open`] advises every mapping of this
//! environment's `mdbx.dat` right after the open. Growth inside the upper limit adds no mapping, so
//! one call covers it. Only a change of the upper limit remaps, so after each persist
//! [`MdbxAdvice::finish_persist`] compares the map size with the one it advised and, when it
//! changed, re-applies the advice to any mapping it has not advised yet. The mappings are found in
//! `/proc/self/maps` by device and inode, taken from the mapping that holds one of this database's
//! own records, never by path: one process can hold many environments (tests do).
//!
//! The advice closes the mmap read-around and nothing else. Folios already in the page cache keep
//! their size, `read()`/`write()` on the file ignore it, and libmdbx's multi-page prefault (one
//! `pwritev` of up to 64 pages for a multi-page allocation) still gets a folio sized to the write.
//! Turning prefault off would close that route too, and is safe for folio size only with the
//! whole-map advice in place; this module does not do it.
//!
//! # The counters
//!
//! Each successful persist reports a [`PersistStorage`] with the switch on or off: libmdbx's
//! page-operation deltas and the process's `write_bytes` delta over the persist, the used size, the
//! last page number, the advised bytes, the GC table's record and large-page counts, and the bytes
//! the persist's two writing threads passed to `write()`. Together they show whether prefault is on
//! (Δ`page_prefault` ≈ Δ`mdbx_last_pgno` while the database grows) and how many device bytes each
//! dirtied page costs.
//!
//! Nothing here opens the environment a second time or runs an MDBX tool. An open without
//! `MDBX_NORDAHEAD` sets a bit in the shared lock file that switches the node's prefault writes off
//! for every allocation, which would cause the write amplification these counters measure. The GC
//! counts come from a read transaction that ends before the call returns.
//!
//! Linux only: elsewhere no mapping is found, nothing is advised, the `/proc` reads find nothing,
//! and every field that needs them is left off the line.

use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use reth_db::mdbx::Info;
use tracing::{debug, info, warn};

use crate::{metrics, RethDb};

/// The name libmdbx-rs gives the thread that begins and commits every read-write transaction of
/// one environment (`txn_manager.rs`). The kernel keeps 15 bytes of a thread name, which this name
/// fits exactly.
const TXN_MANAGER_THREAD: &str = "mdbx-rs-txn-mgr";

/// The GC table's handle (`FREE_DBI` in libmdbx): always open, in every environment.
const GC_DBI: u32 = 0;

/// The execution database's map advice and the counters that show whether it works.
///
/// Built once per [`crate::RethEnv`], before the env writes genesis or serves anything. With the
/// switch off it advises nothing and only reads the counters.
#[derive(Debug)]
pub(crate) struct MdbxAdvice {
    /// The environment this advice and these counters belong to.
    db: RethDb,
    /// Whether to advise the map (`mdbx_random_advice`).
    random_advice: bool,
    /// The database page size in bytes, read once at open; `None` if the read failed.
    page_size: Option<u64>,
    /// Device and inode of this env's `mdbx.dat` as the map prints them; `None` off Linux or if
    /// unknown.
    file: Option<FileId>,
    /// `/proc/self/task/<tid>/io` of this environment's transaction-manager thread, which runs
    /// every commit; `None` unless exactly one such thread existed at open.
    txn_manager_io: Option<PathBuf>,
    /// What the advice covered when it was last applied.
    advised: Mutex<Advised>,
    /// Set once the first advice failure has been logged.
    warned: AtomicBool,
}

impl MdbxAdvice {
    /// Read the environment's fixed facts and, with `random_advice`, advise every mapping of its
    /// `mdbx.dat`.
    ///
    /// Sets `tn_reth_mdbx_random_advice_bytes`, `tn_reth_mdbx_used_bytes` and
    /// `tn_reth_mdbx_page_ops_total`. Never fails: an advice that cannot be applied is logged once
    /// at warn and leaves the advised bytes short of the mapping.
    pub(crate) fn open(db: &RethDb, random_advice: bool) -> Self {
        metrics::describe_mdbx_series();
        // the open created and recorded tables through write transactions, which this env's
        // transaction-manager thread ran, so the thread already carries its name
        let txn_manager_io = txn_manager_io();
        if txn_manager_io.is_none() {
            info!(
                target: "tn::reth",
                "no single mdbx transaction-manager thread; prefault_wchar_bytes is left off"
            );
        }
        let advice = Self {
            db: Arc::clone(db),
            random_advice,
            page_size: db.stat().ok().map(|stat| u64::from(stat.page_size())),
            file: file_id(db),
            txn_manager_io,
            advised: Mutex::new(Advised::default()),
            warned: AtomicBool::new(false),
        };
        let info = advice.db.info().ok();
        if random_advice {
            let map_size = info.as_ref().map_or(0, Info::map_size);
            // with no mapping found, `apply` has already warned
            let mappings = advice.apply(map_size);
            if mappings > 0 {
                info!(
                    target: "tn::reth",
                    mappings,
                    advised_bytes = advice.advised_bytes(),
                    "mdbx random-access advice applied"
                );
            }
        }
        metrics::record_mdbx_random_advice_bytes(advice.advised_bytes());
        if let Some(info) = &info {
            metrics::record_mdbx_page_ops(PageOpCounts::of(info).labelled());
            if let Some(used) = advice.used_bytes(info) {
                metrics::record_mdbx_used_bytes(used);
            }
        }
        advice
    }

    /// Bytes of `mdbx.dat` mappings this env advised successfully; zero with the switch off.
    pub(crate) fn advised_bytes(&self) -> u64 {
        self.advised.lock().bytes
    }

    /// The environment, for tests that read its info.
    #[cfg(test)]
    pub(super) fn db(&self) -> &RethDb {
        &self.db
    }

    /// Start the counters of one persist. Call it on the persisting thread once the write
    /// transaction is open, so the deltas leave out other writers (except one that starts in the
    /// moment between the commit and [`MdbxAdvice::finish_persist`]'s first read).
    pub(crate) fn begin_persist(&self) -> PersistProbe {
        PersistProbe {
            page_ops: self.db.info().ok().map(|info| PageOpCounts::of(&info)),
            write_bytes: read_io(Path::new("/proc/self/io")).map(|io| io.write_bytes),
            persist_wchar: read_io(Path::new("/proc/thread-self/io")).map(|io| io.wchar),
            persist_wchar_bytes: None,
            txn_manager_wchar: self.txn_manager_wchar(),
        }
    }

    /// Close the counters of one persist after its commit returned, update the gauges and the
    /// page-operation counter, and re-apply the advice if the map changed.
    pub(crate) fn finish_persist(&self, probe: PersistProbe) -> PersistStorage {
        let info = self.db.info().ok();
        let write_bytes = read_io(Path::new("/proc/self/io")).map(|io| io.write_bytes);
        let txn_manager_wchar = self.txn_manager_wchar();
        let gc = self.gc_stat();

        let page_ops = info.as_ref().map(PageOpCounts::of);
        let used = info.as_ref().and_then(|info| self.used_bytes(info));
        if let Some(info) = &info {
            self.recheck(info.map_size());
        }
        if let Some(totals) = page_ops {
            metrics::record_mdbx_page_ops(totals.labelled());
        }
        if let Some(used) = used {
            metrics::record_mdbx_used_bytes(used);
        }

        let commit_wchar = txn_manager_wchar
            .zip(probe.txn_manager_wchar)
            .map(|(now, then)| now.saturating_sub(then));
        PersistStorage {
            page_ops: page_ops.zip(probe.page_ops).map(|(now, then)| now.since(&then)),
            proc_write_bytes: write_bytes
                .zip(probe.write_bytes)
                .map(|(now, then)| now.saturating_sub(then)),
            mdbx_used_bytes: used,
            mdbx_advised_bytes: self.advised_bytes(),
            mdbx_last_pgno: info.as_ref().map(|info| to_u64(info.last_pgno())),
            gc,
            prefault_wchar_bytes: probe
                .persist_wchar_bytes
                .zip(commit_wchar)
                .map(|(persist, commit)| persist.saturating_add(commit)),
        }
    }

    /// The used size: pages `0..=last_pgno` times the page size.
    fn used_bytes(&self, info: &Info) -> Option<u64> {
        self.page_size
            .map(|page_size| to_u64(info.last_pgno()).saturating_add(1).saturating_mul(page_size))
    }

    /// Cumulative `wchar` of the transaction-manager thread, if it is known.
    fn txn_manager_wchar(&self) -> Option<u64> {
        self.txn_manager_io.as_deref().and_then(read_io).map(|io| io.wchar)
    }

    /// The GC table's record count and large pages, from a read transaction that ends before this
    /// returns. `mdbx_dbi_stat` reads the table's tree header: no walk, whatever the GC holds.
    fn gc_stat(&self) -> Option<GcStat> {
        let txn = self.db.begin_ro_txn().ok()?;
        let stat = txn.db_stat_with_dbi(GC_DBI).ok()?;
        Some(GcStat { entries: to_u64(stat.entries()), large_pages: to_u64(stat.overflow_pages()) })
    }

    /// Advise every mapping of `mdbx.dat` and record what was covered at `map_size`. Returns the
    /// number of mappings found.
    fn apply(&self, map_size: usize) -> usize {
        let Some(file) = self.file else {
            self.warn_once(&io::Error::other("the open mdbx.dat could not be identified"));
            return 0;
        };
        let ranges = mapped_ranges(file);
        if ranges.is_empty() {
            self.warn_once(&io::Error::other("no mapping of mdbx.dat in /proc/self/maps"));
        }
        let mut bytes = 0u64;
        for &(start, end) in &ranges {
            match advise_random(start, end) {
                Ok(()) => bytes = bytes.saturating_add(end.saturating_sub(start)),
                Err(error) => self.warn_once(&error),
            }
        }
        // adjacent mappings with equal flags merge once advised: keep the set as it is now, so the
        // next comparison sees no change
        *self.advised.lock() = Advised { map_size, ranges: mapped_ranges(file), bytes };
        metrics::record_mdbx_random_advice_bytes(bytes);
        ranges.len()
    }

    /// Re-apply the advice if the map was remapped since it was last applied.
    ///
    /// libmdbx remaps only when the upper limit changes, which changes the map size; the map size
    /// is read anyway, so the common case costs one comparison and no `/proc` read.
    fn recheck(&self, map_size: usize) {
        if !self.random_advice {
            return;
        }
        let Some(file) = self.file else { return };
        let mut advised = self.advised.lock();
        if advised.map_size == map_size {
            return;
        }
        let ranges = mapped_ranges(file);
        if ranges == advised.ranges {
            advised.map_size = map_size;
            return;
        }
        drop(advised);
        let mappings = self.apply(map_size);
        debug!(
            target: "tn::reth",
            map_size,
            mappings,
            advised_bytes = self.advised_bytes(),
            "mdbx map changed, random-access advice re-applied"
        );
    }

    /// Log the first advice failure at warn; later failures are silent.
    fn warn_once(&self, error: &io::Error) {
        if !self.warned.swap(true, Ordering::Relaxed) {
            warn!(
                target: "tn::reth",
                %error,
                "mdbx random-access advice not applied; the database works without it"
            );
        }
    }
}

/// The counters of one persist between [`MdbxAdvice::begin_persist`] and
/// [`MdbxAdvice::finish_persist`].
#[derive(Debug)]
pub(crate) struct PersistProbe {
    /// libmdbx's cumulative page operations when the write transaction was open.
    page_ops: Option<PageOpCounts>,
    /// The process's cumulative `write_bytes` at the same point.
    write_bytes: Option<u64>,
    /// The persisting thread's cumulative `wchar` at the same point.
    persist_wchar: Option<u64>,
    /// What the persisting thread passed to `write()` between the open transaction and the commit.
    persist_wchar_bytes: Option<u64>,
    /// The transaction-manager thread's cumulative `wchar` at the same point.
    txn_manager_wchar: Option<u64>,
}

impl PersistProbe {
    /// Close the persisting thread's `write()` window just before the commit. The commit finalizes
    /// the static files on this thread, so the window must end here; the commit's own MDBX writes
    /// run on the transaction-manager thread and are counted there. Call it on the thread that
    /// called [`MdbxAdvice::begin_persist`].
    pub(crate) fn before_commit(&mut self) {
        self.persist_wchar_bytes = read_io(Path::new("/proc/thread-self/io"))
            .zip(self.persist_wchar)
            .map(|(io, then)| io.wchar.saturating_sub(then));
    }
}

/// What one persist did to the execution database, for the `persisted output` line.
///
/// Deltas cover the window from the open write transaction to the returned commit. A field is
/// `None` when its source could not be read (always the `/proc` ones off Linux); the line then
/// leaves it out.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistStorage {
    /// libmdbx page-operation deltas (`page_newly` through `page_msync`).
    pub(crate) page_ops: Option<PageOpCounts>,
    /// Δ`write_bytes` of `/proc/self/io` (`proc_write_bytes`): bytes the whole process caused to
    /// be written to storage, counted when a page cache folio is dirtied, so a dirtied 4 KiB page
    /// inside a large folio counts the whole folio.
    pub(crate) proc_write_bytes: Option<u64>,
    /// The used size after the commit (`mdbx_used_bytes`).
    pub(crate) mdbx_used_bytes: Option<u64>,
    /// Bytes of `mdbx.dat` mappings advised, zero with the switch off (`mdbx_advised_bytes`).
    pub(crate) mdbx_advised_bytes: u64,
    /// The last used page number after the commit (`mdbx_last_pgno`).
    pub(crate) mdbx_last_pgno: Option<u64>,
    /// The GC table after the commit (`gc_entries`, `gc_large_pages`).
    pub(crate) gc: Option<GcStat>,
    /// `wchar` the persisting thread added over `save_blocks` and the marker writes, plus what the
    /// transaction-manager thread added over the commit (`prefault_wchar_bytes`). Under `WRITEMAP`
    /// libmdbx writes to `mdbx.dat` with `write()` only to prefault new pages, so this is the
    /// prefault volume, plus any log line those threads wrote synchronously in the window.
    pub(crate) prefault_wchar_bytes: Option<u64>,
}

/// libmdbx's page-operation counters the persist line reports, cumulative or as a delta.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageOpCounts {
    /// Pages added.
    pub(crate) newly: u64,
    /// Pages copied for update.
    pub(crate) cow: u64,
    /// Dirty pages spilled.
    pub(crate) spill: u64,
    /// Spilled pages reloaded.
    pub(crate) unspill: u64,
    /// Prefault writes (a multi-page prefault counts once).
    pub(crate) prefault: u64,
    /// Explicit write operations (zero under `WRITEMAP`).
    pub(crate) wops: u64,
    /// Explicit `msync` calls.
    pub(crate) msync: u64,
}

impl PageOpCounts {
    /// The cumulative counters in `info`.
    fn of(info: &Info) -> Self {
        let ops = info.page_ops();
        Self {
            newly: ops.newly,
            cow: ops.cow,
            spill: ops.spill,
            unspill: ops.unspill,
            prefault: ops.prefault,
            wops: ops.wops,
            msync: ops.msync,
        }
    }

    /// The operations since `earlier`.
    fn since(&self, earlier: &Self) -> Self {
        Self {
            newly: self.newly.saturating_sub(earlier.newly),
            cow: self.cow.saturating_sub(earlier.cow),
            spill: self.spill.saturating_sub(earlier.spill),
            unspill: self.unspill.saturating_sub(earlier.unspill),
            prefault: self.prefault.saturating_sub(earlier.prefault),
            wops: self.wops.saturating_sub(earlier.wops),
            msync: self.msync.saturating_sub(earlier.msync),
        }
    }

    /// Each counter under its `op` label of `tn_reth_mdbx_page_ops_total`.
    fn labelled(&self) -> [(&'static str, u64); 7] {
        [
            ("newly", self.newly),
            ("cow", self.cow),
            ("spill", self.spill),
            ("unspill", self.unspill),
            ("prefault", self.prefault),
            ("wops", self.wops),
            ("msync", self.msync),
        ]
    }
}

/// The GC table's size after a commit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcStat {
    /// GC records (`gc_entries`).
    pub(crate) entries: u64,
    /// Large pages holding GC records (`gc_large_pages`); more than `entries` means some record
    /// spans several pages.
    pub(crate) large_pages: u64,
}

/// What the advice covered when it was last applied.
#[derive(Debug, Default)]
struct Advised {
    /// The map size (the upper limit) at the time.
    map_size: usize,
    /// `(start, end)` of each `mdbx.dat` mapping afterwards.
    ranges: Vec<(u64, u64)>,
    /// Bytes advised successfully.
    bytes: u64,
}

/// A mapped file's identity as `/proc/self/maps` prints it: device number (in the encoding `stat`
/// uses) and inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    /// `st_dev`.
    dev: u64,
    /// `st_ino`.
    ino: u64,
}

/// One line of `/proc/self/maps`: the mapping's range and the file behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MapsEntry {
    /// First address.
    start: u64,
    /// One past the last address.
    end: u64,
    /// The mapped file (device 0, inode 0 for an anonymous mapping).
    file: FileId,
}

/// The two `/proc/<pid>/io` counters the persist line uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcIo {
    /// Bytes passed to `write()` and its relatives, whatever their target (`wchar`).
    wchar: u64,
    /// Bytes caused to be written to storage, counted when page cache is dirtied (`write_bytes`).
    write_bytes: u64,
}

/// Device and inode of this environment's `mdbx.dat` as `/proc/self/maps` prints them.
///
/// Taken from the mapping that holds a record of this very database: the first key of its main
/// table, which a read transaction returns as a pointer into the map (the open wrote the table
/// records). Not from `stat`: on btrfs `stat` reports a per-subvolume device and on overlayfs the
/// overlay's, while the map prints the device of the file actually mapped. Off Linux there is no
/// `/proc/self/maps`, so the file is not identified and nothing is advised.
fn file_id(db: &RethDb) -> Option<FileId> {
    let txn = db.begin_ro_txn().ok()?;
    let main = txn.open_db(None).ok()?;
    let mut cursor = txn.cursor(main.dbi()).ok()?;
    let (key, _) = cursor.first::<Cow<'_, [u8]>, Cow<'_, [u8]>>().ok()??;
    // a read transaction never copies, so the key lies in the map
    let Cow::Borrowed(key) = key else { return None };
    let address = to_u64(key.as_ptr().addr());
    std::fs::read_to_string("/proc/self/maps")
        .ok()?
        .lines()
        .filter_map(parse_maps_line)
        .find(|entry| entry.start <= address && address < entry.end)
        .map(|entry| entry.file)
}

/// Advise the kernel that `[start, end)` is read at random.
#[cfg(target_os = "linux")]
fn advise_random(start: u64, end: u64) -> io::Result<()> {
    let addr = usize::try_from(start).map_err(io::Error::other)?;
    let len = usize::try_from(end.saturating_sub(start)).map_err(io::Error::other)?;
    // SAFETY: `[start, end)` is a whole mapping of `mdbx.dat` as this process's own
    // `/proc/self/maps` listed it just before, so it is page-aligned and was mapped. `MADV_RANDOM`
    // only sets the mapping's random-read flag and never changes, frees or discards its contents,
    // so a range unmapped or reused since the read loses nothing: the call fails or hints the new
    // mapping
    let rc = unsafe { libc::madvise(addr as *mut libc::c_void, len, libc::MADV_RANDOM) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Off Linux nothing is mapped by identity, so this is never reached with a range.
#[cfg(not(target_os = "linux"))]
fn advise_random(_start: u64, _end: u64) -> io::Result<()> {
    Ok(())
}

/// `(start, end)` of every mapping of `file` in this process, in address order.
fn mapped_ranges(file: FileId) -> Vec<(u64, u64)> {
    std::fs::read_to_string("/proc/self/maps")
        .map(|maps| {
            maps.lines()
                .filter_map(parse_maps_line)
                .filter(|entry| entry.file == file)
                .map(|entry| (entry.start, entry.end))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse one `/proc/self/maps` line: `start-end perms offset major:minor inode [path]`.
fn parse_maps_line(line: &str) -> Option<MapsEntry> {
    let mut fields = line.split_ascii_whitespace();
    let (start, end) = fields.next()?.split_once('-')?;
    let _perms = fields.next()?;
    let _offset = fields.next()?;
    let (major, minor) = fields.next()?.split_once(':')?;
    let ino = fields.next()?.parse().ok()?;
    Some(MapsEntry {
        start: u64::from_str_radix(start, 16).ok()?,
        end: u64::from_str_radix(end, 16).ok()?,
        file: FileId {
            dev: makedev(
                u64::from_str_radix(major, 16).ok()?,
                u64::from_str_radix(minor, 16).ok()?,
            ),
            ino,
        },
    })
}

/// A device number in the encoding `stat` reports (glibc's `gnu_dev_makedev`); `/proc/self/maps`
/// prints the major and minor numbers instead.
fn makedev(major: u64, minor: u64) -> u64 {
    ((major & 0xffff_f000) << 32)
        | ((major & 0x0000_0fff) << 8)
        | ((minor & 0xffff_ff00) << 12)
        | (minor & 0x0000_00ff)
}

/// Read a `/proc/<pid>/io` file.
fn read_io(path: &Path) -> Option<ProcIo> {
    parse_proc_io(&std::fs::read_to_string(path).ok()?)
}

/// Parse the `wchar` and `write_bytes` lines of a `/proc/<pid>/io` file.
fn parse_proc_io(text: &str) -> Option<ProcIo> {
    let mut wchar = None;
    let mut write_bytes = None;
    for (key, value) in text.lines().filter_map(|line| line.split_once(':')) {
        match key.trim() {
            "wchar" => wchar = value.trim().parse().ok(),
            "write_bytes" => write_bytes = value.trim().parse().ok(),
            _ => {}
        }
    }
    Some(ProcIo { wchar: wchar?, write_bytes: write_bytes? })
}

/// `/proc/self/task/<tid>/io` of the one thread named [`TXN_MANAGER_THREAD`], or `None` when there
/// is none or more than one (another environment's could not be told apart from this one's).
fn txn_manager_io() -> Option<PathBuf> {
    let mut found = None;
    for task in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        let path = task.path();
        // a thread can exit between the listing and the read
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else { continue };
        if comm.trim_end() == TXN_MANAGER_THREAD {
            if found.is_some() {
                return None;
            }
            found = Some(path.join("io"));
        }
    }
    found
}

/// `usize` to `u64`, saturating (lossless on every target the node builds for).
fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Helpers the env and persist tests share: the `mdbx.dat` mappings as `/proc/self/smaps` shows
/// them, and the fields of captured `persisted output` lines.
#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use tracing::{
        field::{Field, Visit},
        span, Event, Metadata, Subscriber,
    };

    /// The fields of one captured event, as `(name, value)` in the order the callsite declares
    /// them.
    pub(crate) type Fields = Vec<(String, String)>;

    /// One mapping of a file as `/proc/self/smaps` shows it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Mapping {
        /// First address.
        pub(crate) start: u64,
        /// One past the last address.
        pub(crate) end: u64,
        /// Whether `VmFlags` carries `rr` (`VM_RAND_READ`, set by `MADV_RANDOM`).
        pub(crate) random: bool,
    }

    /// Every mapping of the file at `path` in this process.
    ///
    /// The file's identity is taken from the first mapping whose line ends in the file's path (a
    /// test knows its own temporary path), so it is found independently of [`file_id`] and on any
    /// file system.
    #[cfg(target_os = "linux")]
    pub(crate) fn mappings_of(path: &Path) -> Vec<Mapping> {
        let path = std::fs::canonicalize(path).expect("canonical path of the mapped file");
        let path = path.to_str().expect("utf-8 temporary path");
        let smaps = std::fs::read_to_string("/proc/self/smaps").expect("read smaps");
        let Some(file) = smaps
            .lines()
            .filter(|line| line.ends_with(path))
            .find_map(parse_maps_line)
            .map(|entry| entry.file)
        else {
            return Vec::new();
        };
        let mut mappings = Vec::new();
        let mut current = None;
        for line in smaps.lines() {
            if let Some(flags) = line.strip_prefix("VmFlags:") {
                if let Some((start, end)) = current.take() {
                    let random = flags.split_ascii_whitespace().any(|flag| flag == "rr");
                    mappings.push(Mapping { start, end, random });
                }
            } else if let Some(entry) = parse_maps_line(line) {
                current = (entry.file == file).then_some((entry.start, entry.end));
            }
        }
        mappings
    }

    /// Run `f` and return its result with the fields of every `persisted output` line it logged on
    /// this thread.
    pub(crate) fn persisted_output_lines<T>(f: impl FnOnce() -> T) -> (T, Vec<Fields>) {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let out = tracing::subscriber::with_default(Capture(Arc::clone(&lines)), f);
        let lines = std::mem::take(&mut *lines.lock());
        (out, lines)
    }

    /// The value of `name` in `fields` as a number, `None` if the line left it off.
    pub(crate) fn number(fields: &Fields, name: &str) -> Option<u64> {
        fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.parse().expect("numeric field"))
    }

    /// A subscriber that keeps the fields of `persisted output` events.
    struct Capture(Arc<Mutex<Vec<Fields>>>);

    impl Subscriber for Capture {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            if event.metadata().target() != "engine" {
                return;
            }
            let mut fields = FieldList::default();
            event.record(&mut fields);
            let persisted = fields
                .0
                .iter()
                .any(|(name, value)| name == "message" && value == "persisted output");
            if persisted {
                self.0.lock().push(fields.0);
            }
        }

        fn enter(&self, _span: &span::Id) {}

        fn exit(&self, _span: &span::Id) {}
    }

    /// Collects an event's fields in order.
    #[derive(Default)]
    struct FieldList(Fields);

    impl Visit for FieldList {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push((field.name().to_owned(), format!("{value:?}")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The parser reads both counters from a real `/proc/<pid>/io` layout and refuses a file
    /// missing either.
    #[test]
    fn test_parse_proc_io() {
        let text = "rchar: 323934931\nwchar: 323929600\nsyscr: 632687\nsyscw: 632675\n\
                    read_bytes: 0\nwrite_bytes: 323932160\ncancelled_write_bytes: 0\n";
        assert_eq!(
            parse_proc_io(text),
            Some(ProcIo { wchar: 323_929_600, write_bytes: 323_932_160 })
        );
        assert_eq!(parse_proc_io("rchar: 1\nwchar: 2\n"), None);
        assert_eq!(parse_proc_io(""), None);
    }

    /// File, anonymous and pathless lines parse; a malformed line does not.
    #[test]
    fn test_parse_maps_line() {
        let line = "7f0e2c000000-7f0e2d000000 rw-s 00000000 fd:01 1572874      \
                    /data/tn node/db/mdbx.dat";
        assert_eq!(
            parse_maps_line(line),
            Some(MapsEntry {
                start: 0x7f0e_2c00_0000,
                end: 0x7f0e_2d00_0000,
                file: FileId { dev: makedev(0xfd, 0x01), ino: 1_572_874 },
            })
        );
        let anon = parse_maps_line("7ffd1a2b3000-7ffd1a2d4000 rw-p 00000000 00:00 0  [stack]")
            .expect("anonymous mapping");
        assert_eq!(anon.file, FileId { dev: 0, ino: 0 });
        assert!(parse_maps_line("55d4c0e00000-55d4c0e21000 rw-p 00000000 00:00 0").is_some());
        assert_eq!(parse_maps_line("not a maps line"), None);
        assert_eq!(parse_maps_line(""), None);
    }

    /// The device encoding matches the kernel's `new_encode_dev` for small and large numbers.
    #[test]
    fn test_makedev_matches_stat_encoding() {
        assert_eq!(makedev(0xfd, 0x01), 0xfd01);
        assert_eq!(makedev(259, 2), (259 << 8) | 2);
        // minor numbers above 255 move their high bits up by 12
        assert_eq!(makedev(8, 0x1234), (0x12 << 20) | (8 << 8) | 0x34);
    }

    /// `/proc/self/io` and this thread's io file parse on Linux.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_proc_io_files_parse() {
        assert!(read_io(Path::new("/proc/self/io")).is_some());
        assert!(read_io(Path::new("/proc/thread-self/io")).is_some());
    }

    /// A changed map size triggers a rescan, and a mapping set that differs from the one advised
    /// is advised again; an unchanged map size reads nothing. With the switch off the check does
    /// nothing at all.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_recheck_reapplies_when_the_mappings_changed() -> eyre::Result<()> {
        let tm = tn_types::TaskManager::new("recheck");
        let dir = tempfile::TempDir::new()?;
        let chain: Arc<reth_chainspec::ChainSpec> = Arc::new(tn_types::test_genesis().into());
        let env = crate::RethEnv::new_for_temp_chain(chain, dir.path(), &tm, None)?;
        let db = &env.inner.mdbx.db;
        let map_size = db.info()?.map_size();

        let off = &env.inner.mdbx;
        off.recheck(map_size + 1);
        assert_eq!(off.advised.lock().map_size, 0, "the switch is off: nothing is checked");
        assert_eq!(off.advised_bytes(), 0);

        let advice = MdbxAdvice::open(db, true);
        let advised = std::mem::take(&mut *advice.advised.lock());
        assert!(!advised.ranges.is_empty(), "the env's own mdbx.dat was identified and mapped");
        let total: u64 = advised.ranges.iter().map(|(start, end)| end - start).sum();
        assert_eq!(total, u64::try_from(map_size)?, "the mappings span the whole map");
        assert_eq!(advised.map_size, map_size);
        assert_eq!(advised.bytes, total, "every byte of every mapping was advised");
        // the identity found through a record of the database selects the mappings of the file at
        // the env's path
        let mappings = test_support::mappings_of(&dir.path().join("mdbx.dat"));
        let by_path: Vec<_> = mappings.iter().map(|mapping| (mapping.start, mapping.end)).collect();
        assert_eq!(by_path, advised.ranges);
        assert!(mappings.iter().all(|mapping| mapping.random), "{mappings:?}");

        // as if the map had been remapped: the recorded set no longer matches
        advice.recheck(map_size);
        assert_eq!(advice.advised_bytes(), total, "the advice was applied again");
        assert_eq!(advice.advised.lock().ranges, advised.ranges);

        // a map size seen before costs nothing, even when the record is stale
        advice.advised.lock().bytes = 1;
        advice.recheck(map_size);
        assert_eq!(advice.advised_bytes(), 1);
        Ok(())
    }

    /// A probe's deltas cover what happened between its ends: a committed write shows page
    /// operations, an idle window shows none, and the commit thread's writes are counted exactly
    /// when that thread is known.
    #[tokio::test]
    async fn test_probe_deltas_cover_the_window() -> eyre::Result<()> {
        use reth_db::{tables, transaction::DbTxMut as _};
        use reth_provider::{DBProvider as _, DatabaseProviderFactory as _};

        let tm = tn_types::TaskManager::new("probe");
        let dir = tempfile::TempDir::new()?;
        let chain: Arc<reth_chainspec::ChainSpec> = Arc::new(tn_types::test_genesis().into());
        let env = crate::RethEnv::new_for_temp_chain(chain, dir.path(), &tm, None)?;
        let advice = &env.inner.mdbx;

        let mut probe = advice.begin_persist();
        probe.before_commit();
        let idle = advice.finish_persist(probe);
        assert_eq!(idle.page_ops, Some(PageOpCounts::default()), "{idle:?}");
        assert!(idle.prefault_wchar_bytes.is_none_or(|bytes| bytes == 0), "{idle:?}");

        let provider = env.blockchain_provider().database_provider_rw()?;
        let mut probe = advice.begin_persist();
        for index in 0..64u8 {
            let code = reth_primitives_traits::Bytecode::new_raw(vec![index; 8 * 1024].into());
            provider.tx_ref().put::<tables::Bytecodes>([index; 32].into(), code)?;
        }
        probe.before_commit();
        provider.commit()?;
        let wrote = advice.finish_persist(probe);
        let ops = wrote.page_ops.expect("page operations are readable");
        assert!(ops.newly + ops.cow > 0, "{wrote:?}");
        assert!(wrote.mdbx_last_pgno > idle.mdbx_last_pgno, "{idle:?} {wrote:?}");
        assert!(wrote.gc.is_some());
        assert_eq!(
            wrote.prefault_wchar_bytes.is_some(),
            cfg!(target_os = "linux") && advice.txn_manager_io.is_some(),
            "{wrote:?}"
        );
        Ok(())
    }
}
