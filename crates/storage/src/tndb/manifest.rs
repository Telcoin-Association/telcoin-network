//! The database's table list, `<root>/tables`: each table's id (the one its log records carry,
//! never reused) and name, and its encoded key size once its first row has fixed it.
//!
//! The file is small and rewritten whole, atomically (a temporary file, `fsync`, rename, directory
//! `fsync`), when a table is first opened or its key size is fixed, always before the log holds a
//! record of that table: a record's table id is therefore always in the manifest, and one that is
//! not is damage.
//!
//! ```text
//! magic "TNDBTABL" (8) | version (1) | count (u16 LE)
//!   count x [ id (u16 LE) | key size (u16 LE, 0 = not yet fixed) | name length (u8) | name ]
//! crc32 (u32 LE) of everything before it
//! ```

use std::{fs, io::Write as _, path::Path};

use eyre::{bail, WrapErr as _};

use crate::{
    archive::{
        crc::crc32,
        data_file::{fsync_directory, fsync_file},
    },
    tndb::TableId,
};

#[cfg(test)]
thread_local! {
    /// Test-only: this thread's next [`Manifest::write`] fails, as a full disk or an I/O error
    /// would.
    pub(super) static FAIL_NEXT_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) const FILE: &str = "tables";
const TMP: &str = "tables.tmp";
const MAGIC: &[u8; 8] = b"TNDBTABL";
const VERSION: u8 = 1;

/// One table of the database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) id: TableId,
    pub(super) name: String,
    /// The table's encoded key size, or 0 until its first row fixes it.
    pub(super) ksize: u16,
}

/// The database's tables, in id order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Manifest {
    pub(super) tables: Vec<Entry>,
}

impl Manifest {
    /// The table named `name`, if the database has one.
    #[cfg(test)]
    pub(super) fn entry(&self, name: &str) -> Option<&Entry> {
        self.tables.iter().find(|entry| entry.name == name)
    }

    /// Add table `name` (key size not yet fixed) under the next unused id, and return the id.
    pub(super) fn add(&mut self, name: &str) -> eyre::Result<TableId> {
        if name.len() > u8::MAX as usize {
            bail!("tndb: table name {name} is longer than {} bytes", u8::MAX);
        }
        let id = match self.tables.iter().map(|entry| entry.id).max() {
            None => 0,
            Some(last) => {
                last.checked_add(1).ok_or_else(|| eyre::eyre!("tndb: no table id is left"))?
            }
        };
        self.tables.push(Entry { id, name: name.to_string(), ksize: 0 });
        Ok(id)
    }

    /// Fix table `id`'s key size.
    pub(super) fn set_ksize(&mut self, id: TableId, ksize: u16) {
        if let Some(entry) = self.tables.iter_mut().find(|entry| entry.id == id) {
            entry.ksize = ksize;
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + self.tables.len() * 40);
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(&(self.tables.len() as u16).to_le_bytes());
        for entry in &self.tables {
            bytes.extend_from_slice(&entry.id.to_le_bytes());
            bytes.extend_from_slice(&entry.ksize.to_le_bytes());
            bytes.push(entry.name.len() as u8);
            bytes.extend_from_slice(entry.name.as_bytes());
        }
        let crc = crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> eyre::Result<Self> {
        let corrupt = || eyre::eyre!("tndb: the table manifest is corrupt");
        let (body, crc) =
            bytes.split_at_checked(bytes.len().saturating_sub(4)).ok_or_else(corrupt)?;
        if body.len() < 11 || &body[..8] != MAGIC || crc32(body).to_le_bytes() != crc {
            return Err(corrupt());
        }
        if body[8] != VERSION {
            bail!(
                "tndb: the table manifest has format version {}, this build reads {VERSION}",
                body[8]
            );
        }
        let count = u16::from_le_bytes([body[9], body[10]]) as usize;
        let mut tables = Vec::with_capacity(count);
        let mut rest = &body[11..];
        for _ in 0..count {
            let (head, tail) = rest.split_at_checked(5).ok_or_else(corrupt)?;
            let len = head[4] as usize;
            let (name, tail) = tail.split_at_checked(len).ok_or_else(corrupt)?;
            let name = std::str::from_utf8(name).map_err(|_| corrupt())?.to_string();
            let id = u16::from_le_bytes([head[0], head[1]]);
            if tables.iter().any(|entry: &Entry| entry.id == id || entry.name == name) {
                return Err(corrupt());
            }
            tables.push(Entry { id, name, ksize: u16::from_le_bytes([head[2], head[3]]) });
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(corrupt());
        }
        Ok(Self { tables })
    }

    /// Read the manifest at `root`, or `None` if the database has none yet.
    pub(super) fn read(root: &Path) -> eyre::Result<Option<Self>> {
        match fs::read(root.join(FILE)) {
            Ok(bytes) => Self::from_bytes(&bytes).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).wrap_err("tndb: read the table manifest"),
        }
    }

    /// Durably replace the manifest at `root`: a crash leaves the old file or the new one.
    pub(super) fn write(&self, root: &Path) -> eyre::Result<()> {
        #[cfg(test)]
        if FAIL_NEXT_WRITE.with(|fail| fail.replace(false)) {
            bail!("a manifest write failure injected by a test");
        }
        let tmp = root.join(TMP);
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&self.to_bytes())?;
            fsync_file(&file)?;
        }
        fs::rename(&tmp, root.join(FILE))?;
        fsync_directory(root)?;
        Ok(())
    }

    /// Remove a leftover temporary file (a write interrupted before its rename). At open only,
    /// under the database's lock.
    pub(super) fn remove_tmp(root: &Path) {
        let _ = fs::remove_file(root.join(TMP));
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tndb_manifest_roundtrip_and_corruption() {
        let tmp = TempDir::with_prefix("tndb_manifest").expect("temp dir");
        assert_eq!(Manifest::read(tmp.path()).expect("read"), None, "no manifest yet");
        let mut manifest = Manifest::default();
        assert_eq!(manifest.add("Votes").expect("add"), 0);
        assert_eq!(manifest.add("Certificates").expect("add"), 1);
        manifest.set_ksize(1, 32);
        manifest.write(tmp.path()).expect("write");
        let read = Manifest::read(tmp.path()).expect("read").expect("present");
        assert_eq!(read, manifest);
        assert_eq!(read.entry("Certificates").map(|entry| entry.ksize), Some(32));

        let path = tmp.path().join(FILE);
        let mut bytes = fs::read(&path).expect("read file");
        bytes[12] ^= 1;
        fs::write(&path, &bytes).expect("write file");
        assert!(Manifest::read(tmp.path()).is_err(), "a flipped bit is caught");
    }
}
