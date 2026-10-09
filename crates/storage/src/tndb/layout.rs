//! The on-disk layout of a tndb table directory:
//!
//! ```text
//! <table>/LOCK             held (flock) by the table's one open writer
//! <table>/meta             the key mode (keyed | derived) and the encoded key size
//! <table>/gen-<N>/data     the value log: keyed records [key | value], derived records [value],
//!                          and an empty record at each commit
//! <table>/gen-<N>/removed  the removal log: [key | data log length at the removal]
//! <table>/gen-<N>/btx/     the B-tree index over the current generation (rebuildable)
//! <table>/spare-<N+1>-<id>/ the next generation, prepared empty in the background
//! <table>/compact-<N+1>-<id>/ a compaction's new generation, being built in the background
//! ```
//!
//! A table's data lives in one generation directory. Clearing the table starts a new, empty
//! generation and deletes the old one, so cleared data leaves the disk (once no reader still maps
//! it). The new generation is a spare prepared ahead of time, so a clear only renames it into
//! place; a spare is never a generation (only `gen-<N>` directories are), and a leftover one is
//! deleted on open. A compaction builds the next generation in a `compact-*` directory and renames
//! it into place when done; a leftover one is deleted on open too. `meta` is per table, outside
//! the generations: its key mode is fixed when the table is created, and its key size once the
//! first row is written.

use std::{
    fs,
    io::{self, Write as _},
    os::fd::AsRawFd as _,
    path::{Path, PathBuf},
};

use eyre::{bail, WrapErr as _};

use crate::archive::crc::crc32;

/// `fsync(2)` an open file or directory: its contents, and for a directory its entries.
///
/// On Linux this is what `File::sync_all` does. On macOS `sync_all` is `F_FULLFSYNC`, a flush of
/// the whole drive cache, which tndb does not use: its commits are `msync`, which on macOS does not
/// flush the drive's cache either, so a full flush for its directory entries alone would add no
/// durability its data has, at a far higher cost (tens of milliseconds under write load).
pub(crate) fn sync_file(file: &fs::File) -> io::Result<()> {
    // SAFETY: `fsync` on a valid, open file descriptor borrowed for the call.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// [`sync_file`] a directory, making its entries (created, renamed or removed) durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    sync_file(&fs::File::open(dir)?)
}

/// How a table's rows are keyed in its log, fixed when the table is created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyMode {
    /// Each data record stores its key before the value.
    Keyed,
    /// Data records store only the value; the key is derived from it (on rebuild) by the
    /// table's key function.
    Derived,
}

/// A table's `meta` file: the key mode and the encoded key size (0 until the first row).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableMeta {
    pub(crate) mode: KeyMode,
    pub(crate) ksize: u16,
}

const META_FILE: &str = "meta";
const META_TMP: &str = "meta.tmp";
const META_MAGIC: &[u8; 8] = b"TNDBMETA";
const META_VERSION: u8 = 1;
/// magic (8) | version (1) | mode (1) | ksize (2, LE) | crc32 of the preceding 12 bytes (4, LE).
const META_LEN: usize = 16;

impl TableMeta {
    fn to_bytes(self) -> [u8; META_LEN] {
        let mut bytes = [0_u8; META_LEN];
        bytes[..8].copy_from_slice(META_MAGIC);
        bytes[8] = META_VERSION;
        bytes[9] = match self.mode {
            KeyMode::Keyed => 0,
            KeyMode::Derived => 1,
        };
        bytes[10..12].copy_from_slice(&self.ksize.to_le_bytes());
        let crc = crc32(&bytes[..12]);
        bytes[12..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> eyre::Result<Self> {
        if bytes.len() != META_LEN
            || &bytes[..8] != META_MAGIC
            || bytes[8] != META_VERSION
            || crc32(&bytes[..12]).to_le_bytes() != bytes[12..16]
        {
            bail!("tndb: table meta file is corrupt");
        }
        let mode = match bytes[9] {
            0 => KeyMode::Keyed,
            1 => KeyMode::Derived,
            other => bail!("tndb: table meta file has an unknown key mode {other}"),
        };
        Ok(Self { mode, ksize: u16::from_le_bytes([bytes[10], bytes[11]]) })
    }

    /// Read the table's meta file, or `None` if the table has none yet.
    pub(crate) fn read(table: &Path) -> eyre::Result<Option<Self>> {
        match fs::read(table.join(META_FILE)) {
            Ok(bytes) => Self::from_bytes(&bytes).map(Some),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).wrap_err("tndb: read table meta"),
        }
    }

    /// Durably replace the table's meta file: write a temporary file, sync it, rename it over
    /// `meta`, then sync the directory. A crash leaves the old file or the new one, never a mix.
    pub(crate) fn write(self, table: &Path) -> eyre::Result<()> {
        let tmp = table.join(META_TMP);
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&self.to_bytes())?;
            sync_file(&file)?;
        }
        fs::rename(&tmp, table.join(META_FILE))?;
        sync_dir(table)?;
        Ok(())
    }
}

/// Lock table directory `table` for one writer: an exclusive, non-blocking `flock` on
/// `<table>/LOCK`, held until the returned file is dropped (a crash releases it with the process).
/// A second open of the table, from this process or another, fails instead of becoming a second
/// writer on the same logs.
pub(crate) fn lock_table(table: &Path) -> eyre::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(table.join("LOCK"))
        .wrap_err("tndb: open the table lock file")?;
    // SAFETY: `flock` on a valid, open file descriptor borrowed for the call.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock {
            bail!("tndb: table {} is already open (its LOCK is held)", table.display());
        }
        return Err(e).wrap_err("tndb: lock the table");
    }
    Ok(file)
}

/// The directory of generation `n`.
pub(crate) fn gen_dir(table: &Path, n: u64) -> PathBuf {
    table.join(format!("gen-{n}"))
}

/// A fresh directory to prepare a spare for generation `n` in. Unique (by process and a
/// per-process counter), so no two preparations ever share files, and the files of a spare that
/// became a generation never share a path with a later spare.
pub(crate) fn spare_dir(table: &Path, n: u64) -> PathBuf {
    scratch_dir(table, "spare", n)
}

/// A fresh directory to build a compaction's generation `n` in (unique, as [`spare_dir`]).
pub(crate) fn compact_dir(table: &Path, n: u64) -> PathBuf {
    scratch_dir(table, "compact", n)
}

fn scratch_dir(table: &Path, kind: &str, n: u64) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    table.join(format!("{kind}-{n}-{}-{id}", std::process::id()))
}

/// Delete every spare and unfinished compaction in `table` (left by a close or a crash).
/// Best-effort: neither is ever read, so one that cannot be deleted now (logged) is only garbage
/// for a later open to remove.
pub(crate) fn remove_spares(table: &Path) -> eyre::Result<()> {
    for entry in fs::read_dir(table)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("spare-") || name.starts_with("compact-"))
        {
            if let Err(e) = fs::remove_dir_all(entry.path()) {
                tracing::warn!(target: "tndb", "remove spare {}: {e}", entry.path().display());
            }
        }
    }
    Ok(())
}

/// The generation numbers present in `table`, ascending. Removes a leftover `meta.tmp` (a meta
/// write interrupted before its rename).
pub(crate) fn list_gens(table: &Path) -> eyre::Result<Vec<u64>> {
    let _ = fs::remove_file(table.join(META_TMP));
    let mut gens = Vec::new();
    for entry in fs::read_dir(table)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(n) =
            name.to_str().and_then(|n| n.strip_prefix("gen-")).and_then(|n| n.parse().ok())
        {
            gens.push(n);
        }
    }
    gens.sort_unstable();
    Ok(gens)
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tndb_meta_roundtrip_and_corruption() {
        let tmp = TempDir::with_prefix("tndb_meta").expect("temp dir");
        assert_eq!(TableMeta::read(tmp.path()).expect("read"), None, "no meta yet");
        let meta = TableMeta { mode: KeyMode::Derived, ksize: 40 };
        meta.write(tmp.path()).expect("write");
        assert_eq!(TableMeta::read(tmp.path()).expect("read"), Some(meta));

        let path = tmp.path().join(META_FILE);
        let mut bytes = fs::read(&path).expect("read meta");
        bytes[10] ^= 1; // the key size, under the CRC
        fs::write(&path, bytes).expect("write meta");
        assert!(TableMeta::read(tmp.path()).is_err(), "a corrupt meta file is an error");
    }

    #[test]
    fn test_tndb_list_gens_sorted() {
        let tmp = TempDir::with_prefix("tndb_gens").expect("temp dir");
        for n in [10_u64, 2, 0] {
            fs::create_dir(gen_dir(tmp.path(), n)).expect("mkdir");
        }
        fs::create_dir(tmp.path().join("gen-x")).expect("mkdir");
        fs::write(tmp.path().join(META_TMP), b"partial").expect("tmp");
        assert_eq!(list_gens(tmp.path()).expect("list"), vec![0, 2, 10]);
        assert!(!tmp.path().join(META_TMP).exists(), "a leftover meta.tmp is removed");
    }
}
