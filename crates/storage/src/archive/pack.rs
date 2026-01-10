//! A data/log file of archival data.  Once written is only indended to be read and shared with
//! other nodes.

use serde::{de::DeserializeOwned, Serialize};
use tn_types::{encode_into_buffer, try_decode};

use crate::archive::{
    error::{
        commit::CommitError, fetch::FetchError, flush::FlushError, insert::AppendError,
        load_header::LoadHeaderError, open::OpenError, rename::RenameError,
    },
    fxhasher::FxHasher,
    pack_iter::PackIter,
};

use super::{crc::add_crc32, data_file::DataFile};
use std::{
    fmt::Debug,
    fs,
    hash::Hasher as _,
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
    time,
    time::UNIX_EPOCH,
};

/// An instance of a DB.
/// Will consist of a data file (.dat), hash index (.hdx) and hash bucket overflow file (.odx).
#[derive(Debug)]
pub struct Pack<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    inner: PackInner<V>,
}

impl<V> Pack<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    /// Open a new or reopen an existing database.
    pub fn open<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, OpenError> {
        Ok(Self { inner: PackInner::open(path, read_only)? })
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, pos: u64) -> Result<V, FetchError> {
        self.inner.fetch(pos)
    }

    /// Insert a new key/value pair in Db.
    ///
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    ///
    /// For the erros IndexCrcError, IndexOverflow, WriteDataError or KeyError the DB will move to a
    /// failed state and become read only.  These errors all indicate serious underlying issues that
    /// can not be trivially fixed, a reopen/repair might help.
    pub fn append(&mut self, value: &V) -> Result<u64, AppendError> {
        self.inner.append(value)
    }

    /// Return the DB version.
    pub fn version(&self) -> u16 {
        self.inner.version()
    }

    /// Return the DB application number (set at creation).
    pub fn appnum(&self) -> u64 {
        self.inner.appnum()
    }

    /// Return the DB uid (generated at creation).
    pub fn uid(&self) -> u64 {
        self.inner.uid()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    /// Note this is an expensive call (syncing to disk is not cheap).
    pub fn commit(&mut self) -> Result<(), CommitError> {
        self.inner.commit()
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&mut self) -> Result<(), FlushError> {
        self.inner.flush()
    }

    /// Close and destroy the Pack (remove it's file).
    /// If it can not remove a file it will silently ignore this.
    pub fn destroy(self) {
        self.inner.destroy();
    }

    /// Rename the pack file to name.
    pub fn rename<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RenameError> {
        self.inner.rename(path)
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    pub fn raw_iter(&self) -> Result<PackIter<V>, LoadHeaderError> {
        self.inner.raw_iter()
    }
}

/// An instance of a DB.
/// Will consist of a data file (.dat), hash index (.hdx) and hash bucket overflow file (.odx).
/// This is synchronous and single threaded.  It is intended to keep the algorithms clearer and
/// to be wrapped for async or multi-threaded synchronous use.
/// This is the private inner type, this protects the io (Read, Write, Sync) traits from external
/// use).
#[derive(Debug)]
struct PackInner<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    header: DataHeader,
    data_file: DataFile,
    value_buffer: Vec<u8>,
    failed: bool,
    read_only: bool,
    _value: PhantomData<V>,
}

impl<V> Drop for PackInner<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        if !self.read_only {
            let _ = self.commit();
        }
    }
}

impl<V> PackInner<V>
where
    V: Debug + Serialize + DeserializeOwned,
{
    /// Open a new or reopen an existing database.
    fn open<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, OpenError> {
        let (data_file, header) =
            Self::open_data_file(path, read_only).map_err(OpenError::DataFileOpen)?;
        // XXXX need to have the last valid pos from somewhere.
        Ok(Self {
            header,
            data_file,
            value_buffer: Vec::new(),
            failed: false,
            read_only,
            _value: PhantomData,
        })
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    fn fetch(&mut self, pos: u64) -> Result<V, FetchError> {
        self.read_record(pos)
    }

    /// Do the actual insert so the public function can rollback easily on an error.
    fn append_inner(&mut self, value: &V) -> Result<u64, AppendError> {
        let record_pos = self.data_file.len();
        self.value_buffer.clear();
        encode_into_buffer(&mut self.value_buffer, value)
            .map_err(|e| AppendError::SerializeValue(e.to_string()))?;

        let mut crc32_hasher = crc32fast::Hasher::new();
        // Once we have written to write_buffer, it needs to be rolled back before returning an
        // error. Space for the value length.
        let value_size = (self.value_buffer.len() as u32).to_le_bytes();
        self.data_file.write_all(&value_size)?;
        crc32_hasher.update(&value_size);

        self.data_file.write_all(&self.value_buffer)?;
        crc32_hasher.update(&self.value_buffer);
        let crc32 = crc32_hasher.finalize();
        self.data_file.write_all(&crc32.to_le_bytes())?;

        Ok(record_pos)
    }

    /// Insert a new key/value pair in Db.
    ///
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    ///
    /// For the errors IndexCrcError, IndexOverflow, WriteDataError or KeyError the DB will move to
    /// a failed state and become read only.  These errors all indicate serious underlying
    /// issues that can not be trivially fixed, a reopen/repair might help.
    fn append(&mut self, value: &V) -> Result<u64, AppendError> {
        if self.read_only || self.failed {
            return Err(AppendError::ReadOnly);
        }
        let result = self.append_inner(value);
        if let Err(err) = &result {
            match err {
                // These errors all indicate a failed DB that can no longer be inserted too.
                AppendError::WriteDataError(_io_err) => self.failed = true,
                // These errors do not indicate a failed DB.
                AppendError::SerializeValue(_)
                | AppendError::ReadOnly
                | AppendError::CrcError
                | AppendError::DuplicateKey => {}
            }
        }
        result
    }

    /// Return the DB version.
    fn version(&self) -> u16 {
        self.header.version()
    }

    /// Return the DB application number (set at creation).
    fn appnum(&self) -> u64 {
        self.header.appnum()
    }

    /// Return the DB uid (generated at creation).
    fn uid(&self) -> u64 {
        self.header.uid()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    /// Note this is a very expensive call (syncing to disk is not cheap).
    fn commit(&mut self) -> Result<(), CommitError> {
        if self.read_only || self.failed {
            return Err(CommitError::ReadOnly);
        }
        self.flush().map_err(CommitError::Flush)?;
        self.data_file.sync_all().map_err(CommitError::DataFileSync)?;
        Ok(())
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    fn flush(&mut self) -> Result<(), FlushError> {
        self.data_file.flush().map_err(FlushError::WriteData)?;
        //XXXXlet file_end = self.data_file.data_file_end();
        Ok(())
    }

    fn open_data_file<P: AsRef<Path>>(
        path: P,
        ro: bool,
    ) -> Result<(DataFile, DataHeader), LoadHeaderError> {
        let mut data_file = DataFile::open(path, ro)?;
        let file_end = data_file.data_file_end();

        let header = if file_end == 0 {
            let header = DataHeader::new();
            header.write_header(&mut data_file)?;
            header
        } else {
            let header = DataHeader::load_header(&mut data_file)?;
            if header.version() != 0 {
                return Err(LoadHeaderError::InvalidVersion);
            }
            if header.appnum() != 1 {
                return Err(LoadHeaderError::InvalidAppNum);
            }
            header
        };
        data_file.flush()?;
        Ok((data_file, header))
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn read_record(&mut self, position: u64) -> Result<V, FetchError> {
        self.data_file.seek(SeekFrom::Start(position))?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        let mut val_size_buf = [0_u8; 4];
        self.data_file.read_exact(&mut val_size_buf)?;
        crc32_hasher.update(&val_size_buf);
        let val_size = u32::from_le_bytes(val_size_buf);
        self.value_buffer.resize(val_size as usize, 0);
        self.data_file.read_exact(&mut self.value_buffer[..])?;
        crc32_hasher.update(&self.value_buffer);
        let calc_crc32 = crc32_hasher.finalize();
        let mut buf_u32 = [0_u8; 4];
        self.data_file.read_exact(&mut buf_u32)?;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        let val = try_decode::<V>(&self.value_buffer[..])
            .map_err(|e| FetchError::DeserializeValue(e.to_string()))?;
        Ok(val)
    }

    /// Close and destroy the Pack (remove it's file).
    /// If it can not remove a file it will silently ignore this.
    fn destroy(self) {
        let path = self.data_file.path().to_owned();
        drop(self);
        let _ = fs::remove_file(&path);
    }

    /// Rename the pack file to name.
    fn rename<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RenameError> {
        self.data_file.rename(path)
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    fn raw_iter(&self) -> Result<PackIter<V>, LoadHeaderError> {
        let dat_file = { self.data_file.try_clone()? };
        PackIter::with_file(dat_file)
    }
}

/// Size of the data file header.
pub const DATA_HEADER_BYTES: usize = 28;

/// Struct that contains the header for a pack file.
/// This data is immutable was written, the data file is an append only log file and will only be
/// truncated to maintain consistency.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct DataHeader {
    type_id: [u8; 6], // The characters "telnet"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
}

impl DataHeader {
    fn new() -> Self {
        let mut hasher = FxHasher::default();
        let now = time::SystemTime::now();
        let now_millis = now
            .duration_since(UNIX_EPOCH)
            // Time went backwards, WTF?  This could lead to common uid I guess if using
            // machines with broken time (i.e. clocks set before the Unix epoch)...
            .unwrap_or_else(|_| time::Duration::from_millis(66))
            .as_millis();
        hasher.write_u128(now_millis);
        // This is pretty basic, just has the current millis with FX to get a UID.
        // This is just to make sure sets of files belong together so not going crazy here.
        let uid = hasher.finish();
        Self { type_id: *b"telnet", version: 0, uid, appnum: 1 }
    }

    /// Load a DataHeader from source.
    pub(crate) fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        source.rewind()?;
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer[..(DATA_HEADER_BYTES - 4)]);
        let calc_crc32 = crc32_hasher.finalize();
        buf32.copy_from_slice(&buffer[(DATA_HEADER_BYTES - 4)..]);
        let read_crc32 = u32::from_le_bytes(buf32);
        if calc_crc32 != read_crc32 {
            return Err(LoadHeaderError::CrcFailed);
        }
        let mut type_id = [0_u8; 6];
        type_id.copy_from_slice(&buffer[0..6]);
        pos += 6;
        if &type_id != b"telnet" {
            return Err(LoadHeaderError::InvalidType);
        }
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let version = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let uid = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let appnum = u64::from_le_bytes(buf64);
        let header = Self { type_id, version, uid, appnum };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
        let mut pos = 0;
        buffer[pos..6].copy_from_slice(&self.type_id);
        pos += 6;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 8;
        add_crc32(&mut buffer);
        pos += 4;
        assert_eq!(pos, DATA_HEADER_BYTES);
        sync.write_all(&buffer)?;
        Ok(())
    }

    /// Version of the DB file.
    pub(crate) fn version(&self) -> u16 {
        self.version
    }

    /// Generated uid for this DB.
    pub(crate) fn uid(&self) -> u64 {
        self.uid
    }

    /// User defined appnum.
    pub(crate) fn appnum(&self) -> u64 {
        self.appnum
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestRec {
        idx: u64,
        name: String,
    }
    type TestPack = Pack<TestRec>;

    #[test]
    fn test_archive_pack_one() {
        let tmp_path = TempDir::with_prefix("test_archive_pack_one").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_test_one"), false).expect("open pack");
        let pos_1 = db.append(&TestRec { idx: 1, name: "Value One".to_string() }).expect("append");
        let pos_2 = db.append(&TestRec { idx: 2, name: "Value Two".to_string() }).expect("append");
        let pos_3 =
            db.append(&TestRec { idx: 3, name: "Value Three".to_string() }).expect("append");
        let pos_4 = db.append(&TestRec { idx: 4, name: "Value Four".to_string() }).expect("append");
        let pos_5 = db.append(&TestRec { idx: 5, name: "Value Five".to_string() }).expect("append");

        let v = db.fetch(pos_5).unwrap();
        assert_eq!(v.idx, 5);
        assert_eq!(v.name, "Value Five");
        let v = db.fetch(pos_1).unwrap();
        assert_eq!(v.idx, 1);
        assert_eq!(v.name, "Value One");
        let v = db.fetch(pos_3).unwrap();
        assert_eq!(v.idx, 3);
        assert_eq!(v.name, "Value Three");
        let v = db.fetch(pos_2).unwrap();
        assert_eq!(v.idx, 2);
        assert_eq!(v.name, "Value Two");
        let v = db.fetch(pos_4).unwrap();
        assert_eq!(v.idx, 4);
        assert_eq!(v.name, "Value Four");

        db.flush().unwrap();
        let iter = db.raw_iter().unwrap().map(|r| r.unwrap());
        assert_eq!(iter.count(), 5);
        let mut iter = db.raw_iter().unwrap().map(|r| r.unwrap());
        let v = iter.next().unwrap();
        assert_eq!(v.idx, 1);
        assert_eq!(v.name, "Value One");
        let v = iter.next().unwrap();
        assert_eq!(v.idx, 2);
        assert_eq!(v.name, "Value Two");
        let v = iter.next().unwrap();
        assert_eq!(v.idx, 3);
        assert_eq!(v.name, "Value Three");
        let v = iter.next().unwrap();
        assert_eq!(v.idx, 4);
        assert_eq!(v.name, "Value Four");
        let v = iter.next().unwrap();
        assert_eq!(v.idx, 5);
        assert_eq!(v.name, "Value Five");
        assert!(iter.next().is_none());
        drop(db);

        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_test_one"), false).expect("open pack");
        let pos_1_2 =
            db.append(&TestRec { idx: 6, name: "Value One2".to_string() }).expect("append");
        let pos_2_2 =
            db.append(&TestRec { idx: 7, name: "Value Two2".to_string() }).expect("append");
        let pos_3_2 =
            db.append(&TestRec { idx: 8, name: "Value Three2".to_string() }).expect("append");
        db.commit().unwrap();
        let v = db.fetch(pos_1_2).unwrap();
        assert_eq!(v.idx, 6);
        assert_eq!(v.name, "Value One2");
        let v = db.fetch(pos_2_2).unwrap();
        assert_eq!(v.idx, 7);
        assert_eq!(v.name, "Value Two2");
        let v = db.fetch(pos_3_2).unwrap();
        assert_eq!(v.idx, 8);
        assert_eq!(v.name, "Value Three2");
        drop(db);

        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_test_one"), true).expect("open pack");
        let v = db.fetch(pos_1_2).unwrap();
        assert_eq!(v.idx, 6);
        assert_eq!(v.name, "Value One2");
        let v = db.fetch(pos_2_2).unwrap();
        assert_eq!(v.idx, 7);
        assert_eq!(v.name, "Value Two2");
        let v = db.fetch(pos_3_2).unwrap();
        assert_eq!(v.idx, 8);
        assert_eq!(v.name, "Value Three2");
        drop(db);

        let mut iter =
            PackIter::open(tmp_path.path().join("pack_test_one")).unwrap().map(|r| r.unwrap());
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 1);
        assert_eq!(v.name, "Value One");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 2);
        assert_eq!(v.name, "Value Two");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 3);
        assert_eq!(v.name, "Value Three");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 4);
        assert_eq!(v.name, "Value Four");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 5);
        assert_eq!(v.name, "Value Five");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 6);
        assert_eq!(v.name, "Value One2");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 7);
        assert_eq!(v.name, "Value Two2");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 8);
        assert_eq!(v.name, "Value Three2");
        assert!(iter.next().is_none());

        let db: TestPack =
            Pack::open(tmp_path.path().join("pack_test_one"), true).expect("open pack");
        let mut iter = db.raw_iter().unwrap().map(|r| r.unwrap());
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 1);
        assert_eq!(v.name, "Value One");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 2);
        assert_eq!(v.name, "Value Two");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 3);
        assert_eq!(v.name, "Value Three");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 4);
        assert_eq!(v.name, "Value Four");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 5);
        assert_eq!(v.name, "Value Five");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 6);
        assert_eq!(v.name, "Value One2");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 7);
        assert_eq!(v.name, "Value Two2");
        let v: TestRec = iter.next().unwrap();
        assert_eq!(v.idx, 8);
        assert_eq!(v.name, "Value Three2");
        assert!(iter.next().is_none());
    }
}
