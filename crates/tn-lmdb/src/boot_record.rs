//! The boot record: a small file beside the database file, `<path>.boot`, that tells a writer
//! open whether the page cache may hold a page of the file the device lacks. Open step 4 drops
//! the file's cached pages (`sys::drop_cached_pages`) only then, so a reopen after a clean close,
//! in the same boot or after a reboot, keeps the cache and does not read every page again from
//! the device (README, "Open and recovery", step 4).
//!
//! **Why the drop.** A page whose writeback failed can stay in the cache, clean, its bytes never
//! on the device, the error reported to the writer that is gone. A later open in the same boot
//! would read it from the cache: the metas, or step 9's check of a weak head, would pass on bytes
//! the device does not hold, and the next sync would write a steady meta naming them. The drop
//! makes every read of the open the device's.
//!
//! **When the cache can be stale.** Any write to the file whose writeback failed can leave such
//! a page, in the boot it ran in: a writer's, and a write in place that keeps no record of its
//! own (a copy over the file, a writer that reaches it through another path name, whose record is
//! another file). A writer whose clean close synced everything (its final sync and the trim
//! succeeded) left a cache equal to the device: a failed writeback would have failed that sync,
//! and a latched or failed close never writes "clean". Its record keeps the file's ctime, which
//! any later write moves. So each writer open writes "open" with its boot id, durably, before its
//! writer can commit; its clean close writes "clean" with the ctime. At the next writer open
//! ([`decide`]):
//!
//! | record | decision |
//! |---|---|
//! | missing, unreadable, torn, another version, another file's | drop ([`Cache::NoRecord`]) |
//! | this boot's id, or the record's, unknown | drop ([`Cache::UnknownBoot`]) |
//! | "clean", but the file changed since (its ctime moved) | drop ([`Cache::Changed`]) |
//! | "open", of any boot: its writer crashed, was killed or closed latched | drop ([`Cache::Unclean`]) |
//! | another boot's "clean" | keep ([`Cache::OtherBoot`]) |
//! | this boot's "clean" | keep ([`Cache::Clean`]) |
//!
//! "Open" drops whichever boot wrote it, since it holds no ctime: a write in place since it was
//! written goes unseen. A file restored over its own inode after a crash, whose writeback failed,
//! is the case: the first open's `fsync` reports the error and fails, leaving the record as it
//! was, and the next open's `fsync` has nothing left to report. The drop costs little in a new
//! boot, whose cache holds only what that boot read or wrote of the file.
//!
//! **Two path names.** The record is kept beside the path the open was given, so a file reached
//! through two names (a hard link, or a symbolic link to it) has a record per name. A writer
//! through one name writes the file, which moves the ctime the other name's "clean" holds, and
//! the other name's "open" drops anyway, so neither keeps a cache the other's writer left stale
//! (within the ctime's resolution, below).
//!
//! **Another file's record.** The record names the data file by its device and inode. A record
//! whose file is not the one open locked (a datadir copied to another host or beside itself, a
//! file restored from a backup) is no record: the copy filled that host's page cache, perhaps in
//! large folios, and the drop gives the open a cache of the device's bytes in the mapping's
//! small pages, as at any first open. A rename of the directory keeps both. A device number that
//! changes across a reboot (some volume managers number devices at mount) costs a drop in a new
//! boot, whose cache holds nothing of the file anyway. A record a removed file left, whose inode
//! number a new file reuses, is caught too: a "clean" one by the ctime, an "open" one drops
//! anyway.
//!
//! **Crash safety.** The record is advisory: every way it can go wrong drops the cache, which
//! costs time, never data, except a record that says "clean" wrongly, which nothing can write:
//! - "open" is written and synced (the directory too when the record is created) before the writer
//!   can commit, and a failure there fails the open. Were it only in the cache, a failed writeback
//!   of the record could put the device's older "clean" of this boot back.
//! - A crash after "open" and before the first commit leaves "open": the next open drops, which
//!   costs only its reads.
//! - "clean" is written after the close's final sync and trim, with no sync of its own: a power
//!   loss before its writeback, or a failed writeback of it, leaves the device's "open", which
//!   drops.
//! - A record torn by a crash fails its CRC (a write of [`RECORD_LEN`] bytes at offset 0 lands
//!   whole or not in the cache; a torn sector on the device has the old or the new bytes): no
//!   record, a drop.
//! - Two processes: the record is written only by a writer open and its close, under the data
//!   file's exclusive lock, so no two writers race on it; a read-only open neither reads nor writes
//!   it.
//! - No clock is read. The boot id is the kernel's random id of the running boot (`sys::boot_id`),
//!   the same in every container on the host, which shares its page cache. An unreadable id is
//!   unknown: zeros in the record, and a drop at every open.
//! - The "clean" record also holds the file's ctime as the close left it: a write to the file since
//!   (a build that keeps no record, a copy over the file, a `truncate`, a writer through another
//!   path name) moves it, and the open drops. A file system whose timestamps are coarser than a
//!   write could fall inside would miss a write within one tick of the close; this build's writers
//!   rely on it only for a file reached through two path names.
//!
//! **Read-only opens** leave the record alone and drop nothing: they write nothing and build
//! nothing on what they read, so a stale cached page can mislead only their own readers (the
//! dead writer's readers saw the same state), and the next writer open still drops, since the
//! record still says "open".
//!
//! **Platforms.** Linux only, where the drop is real; in tests on every host, whose disk model
//! mirrors the drop ([`KEPT`]). Elsewhere open keeps no record and its drop is a no-op.
//!
//! The format: [`RECORD_LEN`] bytes, little-endian: an 8-byte magic, the version (`u32`), the
//! state (`u32`: 1 open, 2 clean), the boot id (16 bytes), the data file's device and inode
//! (`u64` each), its ctime in seconds (`i64`) and nanoseconds (`u32`; both 0 in "open"), and the
//! CRC-32 of the bytes before it.

use std::{
    fs::{File, Metadata, OpenOptions},
    io,
    os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _},
    path::{Path, PathBuf},
};

use crate::{crc, sys, sys::BootId};

/// Whether this build keeps the boot record: on Linux, where open's drop of the cached pages is
/// real, and in tests on every host, whose disk model mirrors the drop.
const KEPT: bool = cfg!(any(target_os = "linux", test));

/// The record's length in bytes.
pub(crate) const RECORD_LEN: usize = 64;

/// The record's first bytes.
const MAGIC: [u8; 8] = *b"tnlmboot";

/// The record format's version.
const VERSION: u32 = 1;

/// Where the CRC starts.
const CRC_AT: usize = RECORD_LEN - 4;

/// A writer's state in the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    /// A writer had the file open, from its open until its clean close.
    Open,
    /// The writer closed cleanly: its final sync and trim succeeded.
    Clean,
}

impl State {
    fn code(self) -> u32 {
        match self {
            State::Open => 1,
            State::Clean => 2,
        }
    }

    fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(State::Open),
            2 => Some(State::Clean),
            _ => None,
        }
    }
}

/// One record (see the module docs for the format).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BootRecord {
    pub(crate) state: State,
    /// The writer's boot id (zeros: unknown).
    pub(crate) boot_id: BootId,
    /// The data file's device.
    pub(crate) dev: u64,
    /// The data file's inode.
    pub(crate) ino: u64,
    /// The data file's ctime as the clean close left it: seconds and nanoseconds (0 in "open").
    pub(crate) ctime: (i64, u32),
}

impl BootRecord {
    /// The record of a writer in `state` on the file `meta` describes, in the boot `boot_id`.
    /// Only a clean one keeps the ctime.
    pub(crate) fn new(state: State, boot_id: BootId, meta: &Metadata) -> Self {
        let ctime = match state {
            State::Open => (0, 0),
            State::Clean => (meta.ctime(), meta.ctime_nsec() as u32),
        };
        Self { state, boot_id, dev: meta.dev(), ino: meta.ino(), ctime }
    }

    pub(crate) fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0; RECORD_LEN];
        out[..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&self.state.code().to_le_bytes());
        out[16..32].copy_from_slice(&self.boot_id);
        out[32..40].copy_from_slice(&self.dev.to_le_bytes());
        out[40..48].copy_from_slice(&self.ino.to_le_bytes());
        out[48..56].copy_from_slice(&self.ctime.0.to_le_bytes());
        out[56..60].copy_from_slice(&self.ctime.1.to_le_bytes());
        let crc = crc::crc32(&out[..CRC_AT]);
        out[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// The record in `bytes`, or `None` for bytes of another length, magic, version or state, or
    /// that fail the CRC.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; RECORD_LEN] = bytes.try_into().ok()?;
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4"));
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8"));
        if bytes[..8] != MAGIC
            || u32_at(8) != VERSION
            || crc::crc32(&bytes[..CRC_AT]) != u32_at(CRC_AT)
        {
            return None;
        }
        Some(Self {
            state: State::from_code(u32_at(12))?,
            boot_id: bytes[16..32].try_into().expect("16"),
            dev: u64_at(32),
            ino: u64_at(40),
            ctime: (u64_at(48) as i64, u32_at(56)),
        })
    }
}

/// What a writer open does with the data file's cached pages, and why (see the module docs'
/// table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cache {
    /// Drop: no record this open can use (missing, unreadable, torn, of another version, or
    /// naming another file), or none kept on this platform.
    NoRecord,
    /// Drop: this boot's id or the record's is unknown.
    UnknownBoot,
    /// Drop: the record is a clean close's, but the file changed since.
    Changed,
    /// Drop: the record is "open", of this boot or another: its writer ended without its clean
    /// close, and a write in place since would go unseen (no ctime).
    Unclean,
    /// Keep: a clean close of another boot, the file unchanged since.
    OtherBoot,
    /// Keep: a clean close of this boot synced everything, and the file is unchanged since.
    Clean,
}

impl Cache {
    /// Whether the open drops the file's cached pages.
    pub(crate) fn drops(self) -> bool {
        !matches!(self, Cache::OtherBoot | Cache::Clean)
    }
}

/// The decision for the record `found` (`None`: no usable one) beside the file `meta` describes,
/// in the boot `boot_id` (`None`: unknown). See the module docs' table.
pub(crate) fn decide(found: Option<BootRecord>, meta: &Metadata, boot_id: Option<BootId>) -> Cache {
    let Some(record) = found.filter(|r| r.dev == meta.dev() && r.ino == meta.ino()) else {
        return Cache::NoRecord;
    };
    let Some(boot_id) = boot_id.filter(|_| record.boot_id != [0; 16]) else {
        return Cache::UnknownBoot;
    };
    if record.state == State::Clean && record.ctime != (meta.ctime(), meta.ctime_nsec() as u32) {
        return Cache::Changed;
    }
    match (record.state, record.boot_id == boot_id) {
        (State::Open, _) => Cache::Unclean,
        (State::Clean, false) => Cache::OtherBoot,
        (State::Clean, true) => Cache::Clean,
    }
}

/// The boot record's file beside a database file.
#[derive(Debug)]
pub(crate) struct BootFile {
    path: PathBuf,
}

/// What a read of the record found.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Found {
    /// No file there.
    Missing,
    /// A file that is not a record: unreadable, torn, of another length, version or kind.
    Unusable,
    /// A record.
    Record(BootRecord),
}

#[cfg(test)]
thread_local! {
    /// Test-only: every boot record write on this thread fails with `EACCES`, as a directory
    /// that refuses the record's creation fails it for any process but root's.
    pub(crate) static REFUSE_WRITES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl BootFile {
    /// The record of the database file at `path`: `<path>.boot`, or `None` on a platform that
    /// keeps none ([`KEPT`]).
    pub(crate) fn beside(path: &Path) -> Option<Self> {
        let mut name = path.as_os_str().to_owned();
        name.push(".boot");
        KEPT.then(|| Self { path: PathBuf::from(name) })
    }

    /// Test-only: the record's path.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Read the record. Never through a symbolic link, and never blocking on a FIFO: anything
    /// but a regular file of [`RECORD_LEN`] bytes holding a valid record is
    /// [`Found::Unusable`], as is a file that cannot be read.
    pub(crate) fn read(&self) -> Found {
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Found::Missing,
            Err(_) => return Found::Unusable,
        };
        let mut bytes = [0; RECORD_LEN];
        let read = file.metadata().is_ok_and(|m| m.is_file() && m.len() == RECORD_LEN as u64)
            && file.read_exact_at(&mut bytes, 0).is_ok();
        match BootRecord::decode(&bytes).filter(|_| read) {
            Some(record) => Found::Record(record),
            None => Found::Unusable,
        }
    }

    /// Write `record` over the record's file, creating it if `created` says it is missing; with
    /// `durable`, sync it, and the directory too when it was created. Never through a symbolic
    /// link; a file there that is not a regular file is refused, [`io::ErrorKind::InvalidInput`]
    /// naming it. Every error names the file, and keeps the OS error as its source
    /// ([`sys::Context`]), so the caller still finds its code.
    pub(crate) fn write(
        &self,
        record: &BootRecord,
        created: bool,
        durable: bool,
    ) -> io::Result<()> {
        let named = |e: io::Error| {
            sys::Context::wrap(format!("the boot record {}", self.path.display()), e)
        };
        #[cfg(test)]
        if REFUSE_WRITES.get() {
            return Err(named(io::Error::from_raw_os_error(libc::EACCES)));
        }
        let refused = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the boot record {} is not a regular file: refused", self.path.display()),
            )
        };
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.path)
            .map_err(|e| match e.raw_os_error() {
                Some(libc::ELOOP | libc::EISDIR | libc::ENXIO) => refused(),
                _ => named(e),
            })?;
        let len = file.metadata().map_err(named)?;
        if !len.is_file() {
            return Err(refused());
        }
        file.write_all_at(&record.encode(), 0).map_err(named)?;
        if len.len() != RECORD_LEN as u64 {
            file.set_len(RECORD_LEN as u64).map_err(named)?;
        }
        if durable {
            sys::fsync_file(&file).map_err(named)?;
            if created {
                sys::fsync_directory(parent_dir(&self.path))?;
            }
        }
        Ok(())
    }

    /// Test-only: write `bytes` as the record's file, as damage or another writer would.
    #[cfg(test)]
    pub(crate) fn plant(&self, bytes: &[u8]) {
        std::fs::write(&self.path, bytes).expect("write the boot record");
    }

    /// Test-only: the record, if the file holds one.
    #[cfg(test)]
    pub(crate) fn record(&self) -> Option<BootRecord> {
        match self.read() {
            Found::Record(record) => Some(record),
            Found::Missing | Found::Unusable => None,
        }
    }
}

/// The directory holding the entry `path` names (`.` for a bare file name).
fn parent_dir(path: &Path) -> &Path {
    path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."))
}

/// Read the record beside the data file `file` and decide what open does with its cached pages:
/// the decision and what the read found (a writer open then writes "open" over it).
pub(crate) fn read_and_decide(
    boot_file: Option<&BootFile>,
    file: &File,
    boot_id: Option<BootId>,
) -> io::Result<(Cache, Found)> {
    let Some(boot_file) = boot_file else {
        return Ok((Cache::NoRecord, Found::Missing));
    };
    let found = boot_file.read();
    let record = match found {
        Found::Record(record) => Some(record),
        Found::Missing | Found::Unusable => None,
    };
    Ok((decide(record, &file.metadata()?, boot_id), found))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Run `change` (a change to a file, returning the file's metadata after it) until the file's
    /// ctime differs from `before`'s, for up to 5 s, and return the metadata it last returned: on
    /// a file system with coarse timestamps (1 s) a change may keep the ctime it had.
    pub(crate) fn until_ctime_moves(
        before: &Metadata,
        mut change: impl FnMut() -> Metadata,
    ) -> Metadata {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let now = change();
            let moved = (now.ctime(), now.ctime_nsec()) != (before.ctime(), before.ctime_nsec());
            if moved || std::time::Instant::now() > deadline {
                return now;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn meta_of(dir: &tempfile::TempDir) -> (PathBuf, Metadata) {
        let path = dir.path().join("db");
        std::fs::write(&path, b"data").expect("write");
        let meta = std::fs::metadata(&path).expect("metadata");
        (path, meta)
    }

    /// A record round-trips; any flipped bit, another length, magic, version or state is no
    /// record.
    #[test]
    fn a_boot_record_round_trips_and_refuses_damage() {
        let dir = tempfile::TempDir::with_prefix("tn_lmdb_boot").expect("temp dir");
        let (_, meta) = meta_of(&dir);
        for state in [State::Open, State::Clean] {
            let record = BootRecord::new(state, [7; 16], &meta);
            let bytes = record.encode();
            assert_eq!(BootRecord::decode(&bytes), Some(record));
            for bit in 0..RECORD_LEN * 8 {
                let mut flipped = bytes;
                flipped[bit / 8] ^= 1 << (bit % 8);
                assert_eq!(BootRecord::decode(&flipped), None, "bit {bit}");
            }
            assert_eq!(BootRecord::decode(&bytes[..RECORD_LEN - 1]), None);
            let mut longer = bytes.to_vec();
            longer.push(0);
            assert_eq!(BootRecord::decode(&longer), None);
            for (at, value) in [(8, VERSION + 1), (12, 3)] {
                let mut other = bytes;
                other[at..at + 4].copy_from_slice(&value.to_le_bytes());
                let crc = crc::crc32(&other[..CRC_AT]);
                other[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
                assert_eq!(BootRecord::decode(&other), None, "{at}: {value}");
            }
        }
        assert_eq!(BootRecord::new(State::Open, [7; 16], &meta).ctime, (0, 0), "open: no ctime");
    }

    /// Every row of the decision table, from records built over a real file.
    #[test]
    fn the_cache_decision_follows_the_table() {
        let dir = tempfile::TempDir::with_prefix("tn_lmdb_boot").expect("temp dir");
        let (path, meta) = meta_of(&dir);
        let (this, other) = (Some([1; 16]), [2; 16]);
        let record = |state, boot| Some(BootRecord::new(state, boot, &meta));
        assert_eq!(decide(None, &meta, this), Cache::NoRecord);
        let mut moved = BootRecord::new(State::Clean, [1; 16], &meta);
        moved.ino ^= 1;
        assert_eq!(decide(Some(moved), &meta, this), Cache::NoRecord, "another inode");
        moved.ino ^= 1;
        moved.dev ^= 1;
        assert_eq!(decide(Some(moved), &meta, this), Cache::NoRecord, "another device");
        assert_eq!(decide(record(State::Clean, [1; 16]), &meta, None), Cache::UnknownBoot);
        assert_eq!(decide(record(State::Clean, [0; 16]), &meta, this), Cache::UnknownBoot);
        assert_eq!(decide(record(State::Open, [1; 16]), &meta, this), Cache::Unclean);
        assert_eq!(decide(record(State::Clean, [1; 16]), &meta, this), Cache::Clean);
        assert_eq!(decide(record(State::Open, other), &meta, this), Cache::Unclean);
        assert_eq!(decide(record(State::Clean, other), &meta, this), Cache::OtherBoot);
        let mut touched = BootRecord::new(State::Clean, [1; 16], &meta);
        touched.ctime.1 ^= 1;
        assert_eq!(decide(Some(touched), &meta, this), Cache::Changed, "this boot");
        touched.boot_id = other;
        assert_eq!(decide(Some(touched), &meta, this), Cache::Changed, "another boot");
        // A write to the file moves its ctime past the clean record's (written until it does: a
        // file system with coarse timestamps keeps one ctime for a while).
        let clean = BootRecord::new(State::Clean, [1; 16], &meta);
        let now = until_ctime_moves(&meta, || {
            std::fs::write(&path, b"other").expect("write");
            std::fs::metadata(&path).expect("metadata")
        });
        assert_eq!(decide(Some(clean), &now, this), Cache::Changed, "written since");
        for (cache, drops) in [
            (Cache::NoRecord, true),
            (Cache::UnknownBoot, true),
            (Cache::Changed, true),
            (Cache::Unclean, true),
            (Cache::OtherBoot, false),
            (Cache::Clean, false),
        ] {
            assert_eq!(cache.drops(), drops, "{cache:?}");
        }
    }

    /// The file: missing, a record, anything else unusable (damaged, a directory, a symbolic
    /// link, a FIFO, without blocking); a write over a symbolic link or a directory is refused,
    /// naming the file, and leaves the link's target alone.
    #[test]
    fn a_boot_file_reads_only_a_regular_record() {
        let dir = tempfile::TempDir::with_prefix("tn_lmdb_boot").expect("temp dir");
        let (path, meta) = meta_of(&dir);
        let boot_file = BootFile::beside(&path).expect("kept in tests");
        assert_eq!(boot_file.path(), dir.path().join("db.boot"));
        assert!(matches!(boot_file.read(), Found::Missing));
        let record = BootRecord::new(State::Open, [3; 16], &meta);
        boot_file.write(&record, true, true).expect("create");
        assert!(matches!(boot_file.read(), Found::Record(r) if r == record));
        let clean = BootRecord::new(State::Clean, [3; 16], &meta);
        boot_file.write(&clean, false, false).expect("overwrite");
        assert_eq!(boot_file.record(), Some(clean));
        let mut bytes = clean.encode().to_vec();
        bytes.extend_from_slice(&[0; 8]);
        boot_file.plant(&bytes);
        assert!(matches!(boot_file.read(), Found::Unusable), "too long");
        boot_file.write(&record, false, true).expect("overwrite a longer file");
        assert_eq!(boot_file.record(), Some(record), "cut to the record");
        boot_file.plant(&record.encode()[..10]);
        assert!(matches!(boot_file.read(), Found::Unusable), "torn");

        std::fs::remove_file(boot_file.path()).expect("remove");
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep").expect("target");
        std::os::unix::fs::symlink(&target, boot_file.path()).expect("symlink");
        assert!(matches!(boot_file.read(), Found::Unusable), "a symbolic link");
        let e = boot_file.write(&record, false, true).expect_err("a symbolic link");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(e.to_string().contains("db.boot"), "{e}");
        assert_eq!(std::fs::read(&target).expect("target"), b"keep");

        std::fs::remove_file(boot_file.path()).expect("remove");
        std::fs::create_dir(boot_file.path()).expect("a directory");
        assert!(matches!(boot_file.read(), Found::Unusable), "a directory");
        let e = boot_file.write(&record, false, true).expect_err("a directory");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{e}");

        std::fs::remove_dir(boot_file.path()).expect("remove");
        let made = std::process::Command::new("mkfifo").arg(boot_file.path()).status();
        assert!(made.expect("mkfifo").success(), "mkfifo");
        assert!(matches!(boot_file.read(), Found::Unusable), "a FIFO, read without blocking");
        let e = boot_file.write(&record, false, true).expect_err("a FIFO, without blocking");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{e}");
    }

    /// A write the OS refuses fails with the OS error under the file's name: the original error
    /// is its source, so a caller that walks the sources for the code (the reth adapter, which
    /// reports it) finds `EACCES` (or `ENOENT`), not a bare kind. The `ENOENT` of a directory that
    /// is gone holds for every user; root writes a read-only file anyway, so a run as root skips
    /// the `EACCES` case, saying so.
    #[test]
    fn a_refused_write_keeps_the_os_error() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::TempDir::with_prefix("tn_lmdb_boot").expect("temp dir");
        let (path, meta) = meta_of(&dir);
        let os_error = |e: &io::Error| {
            let source = std::error::Error::source(e).and_then(|s| s.downcast_ref::<io::Error>());
            source.and_then(io::Error::raw_os_error)
        };
        let record = BootRecord::new(State::Open, [3; 16], &meta);
        let gone = BootFile::beside(&dir.path().join("gone").join("db")).expect("kept in tests");
        let e = gone.write(&record, true, false).expect_err("a directory that is gone");
        assert_eq!(e.kind(), io::ErrorKind::NotFound, "{e}");
        assert!(e.to_string().contains("db.boot"), "names the file: {e}");
        assert_eq!(os_error(&e), Some(libc::ENOENT), "{e:?}");

        let boot_file = BootFile::beside(&path).expect("kept in tests");
        boot_file.write(&record, true, false).expect("create");
        let read_only = std::fs::Permissions::from_mode(0o444);
        std::fs::set_permissions(boot_file.path(), read_only).expect("chmod");
        if OpenOptions::new().write(true).open(boot_file.path()).is_ok() {
            eprintln!(
                "a_refused_write_keeps_the_os_error: SKIPPED the EACCES case: this process writes \
                 a read-only file (root)"
            );
            return;
        }
        let e = boot_file.write(&record, false, true).expect_err("a read-only record");
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied, "{e}");
        assert!(e.to_string().contains("db.boot"), "names the file: {e}");
        assert_eq!(os_error(&e), Some(libc::EACCES), "{e:?}");
    }
}
