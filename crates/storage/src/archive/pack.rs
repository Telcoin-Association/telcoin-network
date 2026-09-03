//! A data/log file of archival data.  Once written is only indended to be read and shared with
//! other nodes.

use serde::{de::DeserializeOwned, Serialize};
use tn_types::{encode_into_buffer, try_decode};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::archive::{
    error::{
        commit::CommitError, fetch::FetchError, flush::FlushError, insert::AppendError,
        load_header::LoadHeaderError, open::OpenError, rename::RenameError,
    },
    fxhasher::FxHasher,
    pack_iter::{PackIter, MAX_RECORD_SIZE},
};

use super::{crc::add_crc32, data_file::MmapDataFile};
use std::{
    fmt::Debug,
    fs::{self, File},
    hash::Hasher as _,
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
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
    /// Open a new or reopen an existing database. The data file is memory-mapped
    /// ([`MmapDataFile`]).
    pub fn open<P: AsRef<Path>>(
        path: P,
        uid_idx: u64,
        read_only: bool,
        compression: PackCompression,
        version: u16,
    ) -> Result<Self, OpenError> {
        Ok(Self { inner: PackInner::open(path, uid_idx, read_only, compression, version)? })
    }

    /// Length of the Pack file.
    pub fn file_len(&self) -> u64 {
        self.inner.file_len()
    }

    /// Reconcile the physical data file down to its logical length, dropping any capacity padding
    /// the mmap backend leaves past `end` (the mapping must be >= the file). Afterwards a separate
    /// handle reading the file to EOF observes exactly `[0, end)` with no trailing zero-padding.
    pub fn reconcile_len(&self) -> io::Result<()> {
        self.inner.reconcile_len()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, pos: u64) -> Result<V, FetchError> {
        self.inner.fetch(pos)
    }

    /// Read raw bytes from the file.  Will return an error if not able to read all the bytes.
    pub fn read_bytes(&mut self, start_pos: u64, end_pos: u64) -> Result<Vec<u8>, FetchError> {
        self.inner.read_bytes(start_pos, end_pos)
    }

    /// Read the record size (with crc32) at position.
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    pub fn record_size(&mut self, pos: u64) -> Result<u32, FetchError> {
        self.inner.record_size(pos)
    }

    /// Return a refernce to the pack files header.
    pub fn header(&self) -> &DataHeader {
        &self.inner.header
    }

    /// Insert a new key/value pair in Db.
    ///
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    ///
    /// A WriteDataError moves the DB to a failed state.  While the DB is failed, each append
    /// and each commit returns a copy of the error that caused the failed state.  This error
    /// indicates a serious underlying issue that can not be trivially fixed, a reopen/repair
    /// might help.
    pub fn append(&mut self, value: &V) -> Result<u64, AppendError> {
        self.inner.append(value)
    }

    /// Test-only failure injector: make the next append fail with
    /// [`AppendError::WriteDataError`], the same classification a real io write failure gets.
    /// The append path then marks the pack failed, which is the poisoned state the queued-save
    /// regression tests start from.
    #[cfg(test)]
    pub(crate) fn fail_next_append_for_test(&mut self) {
        self.inner.fail_next_append = true;
    }

    /// Return the DB version.
    pub fn version(&self) -> u16 {
        self.inner.version()
    }

    /// Return the DB application number (set at creation).
    pub fn appnum(&self) -> u32 {
        self.inner.appnum()
    }

    /// Return the DB uid (generated at creation).
    pub fn uid(&self) -> u64 {
        self.inner.uid()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    /// Note this is an expensive call (syncing to disk is not cheap).
    /// On a pack in the failed state this returns [`CommitError::Failed`] with a copy of the
    /// error that caused the failed state.
    pub fn commit(&mut self) -> Result<(), CommitError> {
        self.inner.commit()
    }

    /// Is this pack read only?
    pub fn read_only(&self) -> bool {
        self.inner.read_only
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

    /// Truncate the pack file.  Use this get back to known good state.
    pub fn truncate(&mut self, new_len: u64) -> Result<(), io::Error> {
        self.inner.truncate(new_len)
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    pub fn raw_iter(&self) -> Result<PackIter<V, File>, LoadHeaderError> {
        self.inner.raw_iter()
    }
}

/// An instance of a DB append only log.
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
    data_file: MmapDataFile,
    value_buffer: Vec<u8>,
    /// Used as a second buffer for compress and decompress operations on records.
    compression_buffer: Vec<u8>,
    /// Root cause of the failed state: a copy of the io error from the append that failed
    /// the pack. While this is `Some`, each append and each commit returns a copy of this
    /// error so callers see the root cause and not a generic guard error.
    failed: Option<io::Error>,
    read_only: bool,
    uid_idx: u64, // Store for opening an iterator.
    /// Test-only: when set, the next append fails as if the data write hit an io error.
    #[cfg(test)]
    fail_next_append: bool,
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
    fn open<P: AsRef<Path>>(
        path: P,
        uid_idx: u64,
        read_only: bool,
        compression: PackCompression,
        version: u16,
    ) -> Result<Self, OpenError> {
        let (data_file, header) =
            Self::open_data_file(path, uid_idx, read_only, compression, version)
                .map_err(OpenError::DataFileOpen)?;
        Ok(Self {
            header,
            data_file,
            value_buffer: Vec::new(),
            compression_buffer: Vec::new(),
            failed: None,
            read_only,
            uid_idx,
            #[cfg(test)]
            fail_next_append: false,
            _value: PhantomData,
        })
    }

    /// Length of the Pack file.
    fn file_len(&self) -> u64 {
        self.data_file.len()
    }

    /// Reconcile the physical file to the logical length (flush `[0, end)`, then truncate any
    /// capacity padding, exactly as a clean close does) so an external consumer that reads the raw
    /// bytes to EOF sees exactly the written bytes.
    fn reconcile_len(&self) -> io::Result<()> {
        self.data_file.reconcile_to_end()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    fn fetch(&mut self, pos: u64) -> Result<V, FetchError> {
        self.read_record(pos)
    }

    /// Read raw bytes from the file.  Will return an error if not able to read all the bytes.
    fn read_bytes(&mut self, start_pos: u64, end_pos: u64) -> Result<Vec<u8>, FetchError> {
        // Validate the range against the file length before allocating so a corrupt or
        // oversized bound (the position index has no per-record CRC) errors instead of
        // triggering a huge up-front allocation that would only fail at read_exact.
        if start_pos > end_pos || end_pos > self.data_file.len() {
            return Err(FetchError::IO(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_bytes range out of bounds",
            )));
        }
        let mut bytes = vec![0; (end_pos - start_pos) as usize];
        self.data_file.seek(SeekFrom::Start(start_pos))?;
        self.data_file.read_exact(&mut bytes[..])?;
        Ok(bytes)
    }

    /// Test-only injection point: fail the append the way a real io write failure fails.
    /// Armed by [`Pack::fail_next_append_for_test`]; disarms after one use. The injected
    /// error carries the StorageFull kind, a sentinel that is not the Other default, so
    /// tests can assert that a replayed copy keeps the kind.
    #[cfg(test)]
    fn injected_append_failure(&mut self) -> Result<(), AppendError> {
        std::mem::take(&mut self.fail_next_append)
            .then(|| {
                AppendError::WriteDataError(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "injected write failure",
                ))
            })
            .map_or(Ok(()), Err)
    }

    /// Do the actual insert so the public function can rollback easily on an error.
    fn append_inner(&mut self, value: &V) -> Result<u64, AppendError> {
        let record_pos = self.data_file.len();

        #[cfg(test)]
        self.injected_append_failure()?;

        write_value(
            value,
            &mut self.data_file,
            &mut self.value_buffer,
            &mut self.compression_buffer,
            self.header.compression,
        )?;
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
    /// A WriteDataError moves the DB to a failed state.  While the DB is failed, each append
    /// and each commit returns a copy of the error that caused the failed state.  This error
    /// indicates a serious underlying issue that can not be trivially fixed, a reopen/repair
    /// might help.
    fn append(&mut self, value: &V) -> Result<u64, AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.failed_cause().map_err(AppendError::WriteDataError)?;
        let result = self.append_inner(value);
        if let Err(err) = &result {
            match err {
                // These errors all indicate a failed DB that can no longer be inserted too.
                AppendError::WriteDataError(io_err) => {
                    self.failed = Some(Self::copy_io_error(io_err))
                }
                // These errors do not indicate a failed DB.
                AppendError::SerializeValue(_)
                | AppendError::ReadOnly
                | AppendError::CrcError
                | AppendError::CorruptIndex(_)
                | AppendError::DuplicateKey => {}
            }
        }
        result
    }

    /// Copy an io error: io::Error is not Clone, so the copy keeps the error kind and the
    /// message of the original.
    fn copy_io_error(cause: &io::Error) -> io::Error {
        io::Error::new(cause.kind(), cause.to_string())
    }

    /// When the pack is in the failed state, return a copy of the io error that caused it.
    /// The copy keeps the error kind and the message of the first failure, so every later
    /// append or commit reports the root cause of the failed state.
    fn failed_cause(&self) -> Result<(), io::Error> {
        self.failed.as_ref().map_or(Ok(()), |cause| Err(Self::copy_io_error(cause)))
    }

    /// Return the DB version.
    fn version(&self) -> u16 {
        self.header.version()
    }

    /// Return the DB application number (set at creation).
    fn appnum(&self) -> u32 {
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
        if self.read_only {
            return Err(CommitError::ReadOnly);
        }
        self.failed_cause().map_err(CommitError::Failed)?;
        self.flush().map_err(CommitError::Flush)?;
        self.data_file.sync_all().map_err(CommitError::DataFileSync)?;
        Ok(())
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    fn flush(&mut self) -> Result<(), FlushError> {
        self.data_file.flush().map_err(FlushError::WriteData)?;
        Ok(())
    }

    fn open_data_file<P: AsRef<Path>>(
        path: P,
        uid_idx: u64,
        ro: bool,
        compression: PackCompression,
        version: u16,
    ) -> Result<(MmapDataFile, DataHeader), LoadHeaderError> {
        let mut data_file = MmapDataFile::open(path, ro)?;
        let header = Self::init_header(&mut data_file, uid_idx, compression, version)?;
        Ok((data_file, header))
    }

    /// Write a fresh [`DataHeader`] to an empty file, or load and validate an existing one, then
    /// flush.
    fn init_header(
        data_file: &mut MmapDataFile,
        uid_idx: u64,
        compression: PackCompression,
        version: u16,
    ) -> Result<DataHeader, LoadHeaderError> {
        let file_end = data_file.data_file_end();
        let header = if file_end == 0 {
            let header = DataHeader::new(uid_idx, compression, version);
            header.write_header(data_file)?;
            header
        } else {
            let header = DataHeader::load_header(data_file, uid_idx)?;
            if header.version() > version {
                // Do not allow a newer version than we request but allow an older.
                return Err(LoadHeaderError::InvalidVersion);
            }
            if header.appnum() != 1 {
                return Err(LoadHeaderError::InvalidAppNum);
            }
            header
        };
        data_file.flush()?;
        Ok(header)
    }

    fn record_size_bytes<'a>(
        &'a mut self,
        position: u64,
        crc32_hasher: &mut crc32fast::Hasher,
    ) -> Result<(usize, &'a [u8]), FetchError> {
        if let Some(bytes) = self.data_file.slice(position, 4) {
            let mut val_size_buf = [0_u8; 4];
            val_size_buf.copy_from_slice(&bytes[0..4]);
            crc32_hasher.update(&val_size_buf);
            let val_size = u32::from_le_bytes(val_size_buf);
            if val_size > MAX_RECORD_SIZE {
                return Err(FetchError::RequestedSizeTooLarge(val_size, MAX_RECORD_SIZE));
            }
            if let Some(bytes) = self.data_file.slice(position + 4, val_size as usize + 4) {
                Ok((val_size as usize, bytes))
            } else {
                Err(FetchError::IO(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unable to read the full record and CRC",
                )))
            }
        } else {
            self.data_file.seek(SeekFrom::Start(position))?;
            let mut val_size_buf = [0_u8; 4];
            self.data_file.read_exact(&mut val_size_buf)?;
            crc32_hasher.update(&val_size_buf);
            let val_size = u32::from_le_bytes(val_size_buf);
            if val_size > MAX_RECORD_SIZE {
                return Err(FetchError::RequestedSizeTooLarge(val_size, MAX_RECORD_SIZE));
            }
            self.value_buffer.resize(val_size as usize + 4, 0);
            self.data_file.read_exact(&mut self.value_buffer[..])?;
            Ok((val_size as usize, &self.value_buffer[..]))
        }
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn read_record(&mut self, position: u64) -> Result<V, FetchError> {
        // The record `bytes` (in `read_record_into`) borrow `self` on the zero-copy path, so the
        // reusable decompression buffer cannot be borrowed from `self` during the decode. Move it
        // out and back; `mem::take` preserves its capacity, so there is still no per-read
        // allocation.
        let mut compression_buffer = std::mem::take(&mut self.compression_buffer);
        let result = self.read_record_into(position, &mut compression_buffer);
        self.compression_buffer = compression_buffer;
        result
    }

    /// Body of [`Self::read_record`], with the reusable decompression buffer passed in (see the
    /// note there) so the record `bytes` can be decoded/decompressed straight from where they
    /// were read (the mmap map for the zero-copy path) without a borrow conflict.
    fn read_record_into(
        &mut self,
        position: u64,
        compression_buffer: &mut Vec<u8>,
    ) -> Result<V, FetchError> {
        let mut crc32_hasher = crc32fast::Hasher::new();
        let compression = self.header.compression;
        let (val_size, bytes) = self.record_size_bytes(position, &mut crc32_hasher)?;
        crc32_hasher.update(&bytes[0..val_size]);
        let calc_crc32 = crc32_hasher.finalize();
        let mut buf_u32 = [0_u8; 4];
        buf_u32.copy_from_slice(&bytes[val_size..val_size + 4]);
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        let buffer: &[u8] = match compression {
            // The value bytes only — `bytes` is `[value | crc]`, so drop the trailing 4-byte CRC.
            PackCompression::None => &bytes[0..val_size],
            PackCompression::ZStd => {
                let mut decoder = zstd::stream::read::Decoder::new(&bytes[0..val_size])?;
                decoder.window_log_max(24)?;
                compression_buffer.clear();
                // +1 lets us detect overflow vs. natural EOF
                let mut limited = decoder.take(MAX_RECORD_SIZE as u64 + 1);
                limited.read_to_end(compression_buffer)?;
                if compression_buffer.len() as u64 > MAX_RECORD_SIZE as u64 {
                    return Err(FetchError::RequestedDecompressSizeTooLarge(MAX_RECORD_SIZE));
                }
                &compression_buffer[..]
            }
        };
        let val =
            try_decode::<V>(buffer).map_err(|e| FetchError::DeserializeValue(e.to_string()))?;
        Ok(val)
    }

    /// Read the record size (with crc32) at position.
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn record_size(&mut self, position: u64) -> Result<u32, FetchError> {
        let mut crc32_hasher = crc32fast::Hasher::new();
        let (val_size, bytes) = self.record_size_bytes(position, &mut crc32_hasher)?;
        crc32_hasher.update(&bytes[0..val_size]);
        let calc_crc32 = crc32_hasher.finalize();
        let mut buf_u32 = [0_u8; 4];
        buf_u32.copy_from_slice(&bytes[val_size..val_size + 4]);
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        Ok(val_size as u32 + 8)
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
        self.data_file.rename(path.as_ref())
    }

    /// Truncate the pack file.  Use this get back to known good state.
    fn truncate(&mut self, new_len: u64) -> Result<(), io::Error> {
        self.data_file.set_len(new_len)
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    fn raw_iter(&self) -> Result<PackIter<V, File>, LoadHeaderError> {
        // `try_clone` does NOT truncate the capacity padding, so read to the logical `end` it
        // returns rather than physical EOF — otherwise a concurrent append that re-grows and
        // re-pads the file would feed the iterator trailing zeros (a 0-size record → CRC failure).
        let (dat_file, end) = self.data_file.try_clone()?;
        PackIter::open(dat_file, self.uid_idx, end)
    }
}

/// Do the actual insert so the public function can rollback easily on an error.
pub fn write_value<V, W>(
    value: &V,
    writer: &mut W,
    value_buffer: &mut Vec<u8>,
    mut compression_buffer: &mut Vec<u8>,
    compression: PackCompression,
) -> Result<(), std::io::Error>
where
    V: Debug + Serialize,
    W: ?Sized + std::io::Write,
{
    value_buffer.clear();
    encode_into_buffer(value_buffer, value).map_err(|e| std::io::Error::other(e.to_string()))?;
    let buffer = match compression {
        PackCompression::None => value_buffer,
        PackCompression::ZStd => {
            compression_buffer.clear();
            {
                let mut compressor = zstd::stream::write::Encoder::new(&mut compression_buffer, 0)?;
                compressor.write_all(value_buffer)?;
                compressor.finish()?;
            }
            compression_buffer
        }
    };

    let mut crc32_hasher = crc32fast::Hasher::new();
    // Once we have written to write_buffer, it needs to be rolled back before returning an
    // error. Space for the value length.
    let value_size = (buffer.len() as u32).to_le_bytes();
    writer.write_all(&value_size)?;
    crc32_hasher.update(&value_size);

    writer.write_all(buffer)?;
    crc32_hasher.update(buffer);
    let crc32 = crc32_hasher.finalize();
    writer.write_all(&crc32.to_le_bytes())?;

    Ok(())
}

/// Size of the data file header.
pub const DATA_HEADER_BYTES: usize = 28;

/// Struct that contains the header for a pack file.
/// This data is immutable was written, the data file is an append only log file and will only be
/// truncated to maintain consistency.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
pub struct DataHeader {
    /// The characters "telnet"
    type_id: [u8; 6],
    /// Holds the version number
    version: u16,
    /// Unique ID generated on creation
    uid: u64,
    /// Application defined constant
    appnum: u32,
    /// Define compression used.
    compression: PackCompression,
}

impl DataHeader {
    pub(crate) fn new(uid_idx: u64, compression: PackCompression, version: u16) -> Self {
        let uid = Self::gen_uid(uid_idx);
        Self { type_id: *b"telnet", version, uid, appnum: 1, compression }
    }

    /// Load a DataHeader from source.
    pub(crate) fn load_header<R: Read + Seek>(
        source: &mut R,
        uid_idx: u64,
    ) -> Result<Self, LoadHeaderError> {
        source.rewind()?;
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
        source.read_exact(&mut buffer[..])?;
        Self::load_header_from_buffer(buffer, uid_idx)
    }

    /// Load a DataHeader from source.
    /// Note the read position must be at the header (this does not seek first).
    pub(crate) async fn load_header_async<R: AsyncRead + Unpin>(
        source: &mut R,
        uid_idx: u64,
    ) -> Result<Self, LoadHeaderError> {
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
        source.read_exact(&mut buffer[..]).await?;
        Self::load_header_from_buffer(buffer, uid_idx)
    }

    /// Load a DataHeader from source.
    pub(crate) fn load_header_from_buffer(
        buffer: [u8; DATA_HEADER_BYTES],
        uid_idx: u64,
    ) -> Result<Self, LoadHeaderError> {
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
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
        if uid != Self::gen_uid(uid_idx) {
            return Err(LoadHeaderError::InvalidDataUID);
        }
        pos += 8;
        buf32.copy_from_slice(&buffer[pos..(pos + 4)]);
        let appconst = u32::from_le_bytes(buf32);
        pos += 4;
        buf32.copy_from_slice(&buffer[pos..(pos + 4)]);
        let compression = u32::from_le_bytes(buf32);
        let compression = PackCompression::from_u32(compression)?;
        let header = Self { type_id, version, uid, appnum: appconst, compression };
        Ok(header)
    }

    /// Generate a unique (simple not cryptographic) "uid" for a file.
    /// Use uid_idx for uniqueness.
    fn gen_uid(uid_idx: u64) -> u64 {
        let mut hasher = FxHasher::default();
        hasher.write(b"telcoin-network-epoch-");
        hasher.write_u64(uid_idx);
        // This is pretty basic, just use a string and provided u64.
        // this is just to make sure sets of files belong together so not going crazy here.
        hasher.finish()
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
        buffer[pos..(pos + 4)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 4;
        buffer[pos..(pos + 4)].copy_from_slice(&self.compression.to_u32().to_le_bytes());
        pos += 4;
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
    pub(crate) fn appnum(&self) -> u32 {
        self.appnum
    }

    /// Compression for records.
    pub(crate) fn compression(&self) -> PackCompression {
        self.compression
    }
}

/// Set the pack file record level compression.
#[derive(Debug, Copy, Clone)]
pub enum PackCompression {
    /// No compression
    None,
    /// ZStd compression
    ZStd,
}

impl PackCompression {
    /// Create a PackCompression enum from a u32.
    pub fn from_u32(v: u32) -> Result<Self, LoadHeaderError> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::ZStd),
            _ => Err(LoadHeaderError::InvalidCompression),
        }
    }

    /// Convert a PackCompression enum into a u32.
    pub fn to_u32(&self) -> u32 {
        match self {
            PackCompression::None => 0,
            PackCompression::ZStd => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestRec {
        idx: u64,
        name: String,
    }
    type TestPack = Pack<TestRec>;

    /// Regression test for the failed-state guard: a failed pack replays the error that
    /// caused the failed state, on both the append and the commit path, instead of the
    /// read-only guard error it returned before. The replayed copy keeps the error kind
    /// and the message of the root cause.
    #[test]
    fn failed_pack_replays_the_root_cause() {
        let tmp_path = TempDir::with_prefix("test_failed_pack_replay").expect("temp dir");
        let mut db: TestPack = Pack::open(
            tmp_path.path().join("pack_failed_replay"),
            0,
            false,
            PackCompression::None,
            0,
        )
        .expect("open pack");

        // Arm the injector: the next append fails the way a real io write failure fails and
        // moves the pack to the failed state.
        db.fail_next_append_for_test();
        let root_cause = db
            .append(&TestRec { idx: 1, name: "Value One".to_string() })
            .expect_err("armed append must fail");
        assert!(
            root_cause.to_string().contains("injected write failure"),
            "unexpected root cause: {root_cause}"
        );
        // Positive control for the negative assertions below: the root cause does not
        // render as the guard error.
        assert!(!root_cause.to_string().contains("read only"), "got: {root_cause}");
        // Positive control for the kind assertions below: the injected root cause really
        // carries the StorageFull sentinel kind, not the Other default a degenerate copy
        // would produce.
        assert!(
            matches!(&root_cause, AppendError::WriteDataError(io_err) if io_err.kind() == io::ErrorKind::StorageFull),
            "unexpected root cause shape: {root_cause}"
        );

        // Each later append replays a copy of the root cause, not the read-only guard error.
        let replayed = db
            .append(&TestRec { idx: 2, name: "Value Two".to_string() })
            .expect_err("a failed pack rejects appends");
        assert_eq!(
            replayed.to_string(),
            root_cause.to_string(),
            "append must replay the root cause"
        );
        assert!(
            matches!(&replayed, AppendError::WriteDataError(io_err) if io_err.kind() == io::ErrorKind::StorageFull),
            "the append replay must keep the error kind, got: {replayed}"
        );

        // Commit reports the failed state with its own discriminant and the same root cause.
        let commit_err = db.commit().expect_err("a failed pack rejects commits");
        assert!(
            matches!(&commit_err, CommitError::Failed(io_err) if io_err.kind() == io::ErrorKind::StorageFull),
            "commit must report the failed state and keep the error kind, got: {commit_err:?}"
        );
        assert!(
            commit_err.to_string().contains("injected write failure"),
            "commit must carry the root cause, got: {commit_err}"
        );

        // A genuinely read-only pack still reports the read-only guard error. The injected
        // failure fired before any record write, so the file reopens cleanly.
        drop(db);
        let mut ro: TestPack = Pack::open(
            tmp_path.path().join("pack_failed_replay"),
            0,
            true,
            PackCompression::None,
            0,
        )
        .expect("reopen read only");
        let ro_err = ro
            .append(&TestRec { idx: 3, name: "Value Three".to_string() })
            .expect_err("a read-only pack rejects appends");
        assert_eq!(ro_err.to_string(), "read only");
    }

    fn archive_pack_(compression: PackCompression) {
        let tmp_path = TempDir::with_prefix("test_archive_pack_one").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_test_one"), 0, false, compression, 0)
                .expect("open pack");
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
            Pack::open(tmp_path.path().join("pack_test_one"), 0, false, compression, 0)
                .expect("open pack");
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
            Pack::open(tmp_path.path().join("pack_test_one"), 0, true, compression, 0)
                .expect("open pack");
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

        let data_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(tmp_path.path().join("pack_test_one"))
            .unwrap();
        // The pack was cleanly closed (Drop truncated to the logical end), so the physical file
        // length is the logical end to bound the iterator at.
        let end = data_file.metadata().unwrap().len();
        let mut iter = PackIter::open(data_file, 0, end).unwrap().map(|r| r.unwrap());
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
            Pack::open(tmp_path.path().join("pack_test_one"), 0, true, compression, 0)
                .expect("open pack");
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

    /// A `raw_iter` snapshots the pack at its clone-time logical `end`. A concurrent append that
    /// re-grows and re-pads the physical mmap file underneath the already-cloned reader must not
    /// feed the snapshot iterator the later records or the trailing zero padding (which would
    /// decode as a 0-size, CRC-failing record). Regression for the `try_clone` EOF-contract
    /// hazard: under the old truncate-at-clone behavior the post-clone append re-padded the
    /// file and the stale reader ran off the end into the padding.
    #[test]
    fn raw_iter_stops_at_clone_time_end_despite_concurrent_append() {
        let tmp_path = TempDir::with_prefix("pack_iter_bound").expect("temp dir");
        let path = tmp_path.path().join("pack_bound");
        let mut db: TestPack =
            Pack::open(&path, 0, false, PackCompression::None, 0).expect("open pack");

        // Append the first three records and snapshot an iterator (captures the logical end now).
        for i in 1..=3u64 {
            db.append(&TestRec { idx: i, name: format!("v{i}") }).expect("append");
        }
        db.flush().expect("flush");
        let iter = db.raw_iter().expect("raw_iter");

        // Append three MORE records to the same live pack, re-growing and re-padding the physical
        // file under the already-cloned reader.
        for i in 4..=6u64 {
            db.append(&TestRec { idx: i, name: format!("v{i}") }).expect("append");
        }
        db.flush().expect("flush");

        // The snapshot iterator yields EXACTLY the three clone-time records and terminates cleanly,
        // never decoding the later appends or the mmap padding.
        let got: Vec<u64> =
            iter.map(|r| r.expect("no read/CRC error past the logical end").idx).collect();
        assert_eq!(got, vec![1, 2, 3], "iterator is bounded to the clone-time end");
    }

    #[test]
    fn test_archive_pack_zstd() {
        archive_pack_(PackCompression::ZStd);
    }

    #[test]
    fn test_archive_pack_none() {
        archive_pack_(PackCompression::None);
    }

    /// Builds a zstd pack file containing a single hand-crafted record whose compressed
    /// payload is small (so val_size <= MAX_RECORD_SIZE) but decompresses to
    /// MAX_RECORD_SIZE + 1 bytes. The outer CRC32 is computed over the same bytes the
    /// production read path hashes, so the record reaches the zstd decoder with the
    /// integrity check passing — exercising the in-memory cap added at pack.rs:362-368.
    ///
    /// Returns the temp dir handle (drop = cleanup) and the byte position at which the
    /// crafted record starts, suitable for `Pack::fetch` or as the iterator's first
    /// post-header read.
    fn build_pack_with_decompression_bomb() -> (TempDir, u64) {
        let tmp_path = TempDir::with_prefix("test_zstd_bomb").expect("temp dir");
        let path = tmp_path.path().join("pack_bomb");
        {
            let _pack: TestPack =
                Pack::open(&path, 0, false, PackCompression::ZStd, 0).expect("open pack");
        }
        let pos = fs::metadata(&path).expect("metadata").len();

        let payload = vec![0u8; (MAX_RECORD_SIZE as usize) + 1];
        let mut compressed = Vec::new();
        {
            let mut encoder =
                zstd::stream::write::Encoder::new(&mut compressed, 0).expect("zstd encoder");
            encoder.write_all(&payload).expect("zstd write");
            encoder.finish().expect("zstd finish");
        }

        let val_size = compressed.len() as u32;
        let val_size_bytes = val_size.to_le_bytes();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&val_size_bytes);
        hasher.update(&compressed);
        let crc = hasher.finalize();

        let mut file = OpenOptions::new().append(true).open(&path).expect("open for append");
        file.write_all(&val_size_bytes).expect("write val_size");
        file.write_all(&compressed).expect("write compressed");
        file.write_all(&crc.to_le_bytes()).expect("write crc");
        file.flush().expect("flush");

        (tmp_path, pos)
    }

    /// Builds a zstd pack file containing one valid record and then mutates a byte deep
    /// in the zstd frame body, recomputing the outer CRC32 so the corruption survives
    /// the integrity check. The decoder must surface an io-error or a deserialization
    /// failure rather than silently returning bad bytes.
    ///
    /// Returns the temp dir and the byte position of the corrupted record.
    fn build_pack_with_corrupt_zstd_frame() -> (TempDir, u64) {
        let tmp_path = TempDir::with_prefix("test_zstd_corrupt").expect("temp dir");
        let path = tmp_path.path().join("pack_corrupt");
        let pos = {
            let mut pack: TestPack =
                Pack::open(&path, 0, false, PackCompression::ZStd, 0).expect("open pack");
            pack.append(&TestRec { idx: 1, name: "f4 fixture".to_string() }).expect("append")
        };

        let mut file = OpenOptions::new().read(true).write(true).open(&path).expect("open for rw");

        file.seek(SeekFrom::Start(pos)).expect("seek val_size");
        let mut val_size_bytes = [0_u8; 4];
        file.read_exact(&mut val_size_bytes).expect("read val_size");
        let val_size = u32::from_le_bytes(val_size_bytes);

        let mut compressed = vec![0u8; val_size as usize];
        file.read_exact(&mut compressed).expect("read compressed");

        // Flip every byte past the 4-byte zstd magic. A single-byte corruption is too narrow:
        // for small payloads zstd may emit a Raw_Block where a one-byte flip is silently
        // absorbed into the output, and bincode is permissive enough to decode the resulting
        // bytes as a valid-but-wrong record. Corrupting the entire post-magic span makes the
        // decoder either reject the frame structurally (frame header / block header parse
        // failure) or produce enough garbage that bincode fails to deserialize.
        let corruption_start = 4_usize.min(compressed.len());
        for byte in compressed[corruption_start..].iter_mut() {
            *byte ^= 0xFF;
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&val_size_bytes);
        hasher.update(&compressed);
        let new_crc = hasher.finalize();

        file.seek(SeekFrom::Start(pos + 4)).expect("seek body");
        file.write_all(&compressed).expect("write corrupted");
        file.seek(SeekFrom::Start(pos + 4 + val_size as u64)).expect("seek crc");
        file.write_all(&new_crc.to_le_bytes()).expect("write crc");
        file.flush().expect("flush");

        (tmp_path, pos)
    }

    // F3: the in-memory MAX_RECORD_SIZE cap on decompressed output must fire at every
    // decompression site (sync fetch, sync iterator, async iterator). Parity tests across
    // the three sites guard against drift if one site is refactored without the others.

    #[test]
    fn test_zstd_decompression_bomb_fetch() {
        let (tmp_dir, pos) = build_pack_with_decompression_bomb();
        let path = tmp_dir.path().join("pack_bomb");
        let mut pack: TestPack =
            Pack::open(&path, 0, true, PackCompression::ZStd, 0).expect("open pack");
        match pack.fetch(pos) {
            Err(FetchError::RequestedDecompressSizeTooLarge(max)) => {
                assert_eq!(max, MAX_RECORD_SIZE);
            }
            other => panic!("expected RequestedSizeTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn test_zstd_decompression_bomb_pack_iter() {
        let (tmp_dir, _pos) = build_pack_with_decompression_bomb();
        let path = tmp_dir.path().join("pack_bomb");
        let file = File::open(&path).expect("open file");
        let end = file.metadata().expect("metadata").len();
        let mut iter = PackIter::<TestRec, _>::open(file, 0, end).expect("iter open");
        match iter.next() {
            Some(Err(FetchError::RequestedDecompressSizeTooLarge(max))) => {
                assert_eq!(max, MAX_RECORD_SIZE);
            }
            other => panic!("expected RequestedSizeTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_zstd_decompression_bomb_async_pack_iter() {
        use crate::archive::pack_iter::AsyncPackIter;

        let (tmp_dir, _pos) = build_pack_with_decompression_bomb();
        let path = tmp_dir.path().join("pack_bomb");
        let file = tokio::fs::File::open(&path).await.expect("open file");
        let mut iter = AsyncPackIter::<TestRec, _>::open(file, 0).await.expect("iter open");
        match iter.next().await {
            Some(Err(FetchError::RequestedDecompressSizeTooLarge(max))) => {
                assert_eq!(max, MAX_RECORD_SIZE);
            }
            other => panic!("expected RequestedSizeTooLarge, got {other:?}"),
        }
    }

    // F4: a CRC-valid but internally corrupt zstd frame must surface as an error rather
    // than silently passing through. Either FetchError::IO (zstd decode error) or
    // FetchError::DeserializeValue (zstd produced different bytes that bincode rejects)
    // is acceptable — both signal that the frame did not round-trip cleanly.

    #[test]
    fn test_zstd_corrupt_frame_fetch() {
        let (tmp_dir, pos) = build_pack_with_corrupt_zstd_frame();
        let path = tmp_dir.path().join("pack_corrupt");
        let mut pack: TestPack =
            Pack::open(&path, 0, true, PackCompression::ZStd, 0).expect("open pack");
        match pack.fetch(pos) {
            Err(FetchError::IO(_)) | Err(FetchError::DeserializeValue(_)) => {}
            other => panic!("expected IO or DeserializeValue error, got {other:?}"),
        }
    }

    #[test]
    fn test_zstd_corrupt_frame_pack_iter() {
        let (tmp_dir, _pos) = build_pack_with_corrupt_zstd_frame();
        let path = tmp_dir.path().join("pack_corrupt");
        let file = File::open(&path).expect("open file");
        let end = file.metadata().expect("metadata").len();
        let mut iter = PackIter::<TestRec, _>::open(file, 0, end).expect("iter open");
        match iter.next() {
            Some(Err(FetchError::IO(_))) | Some(Err(FetchError::DeserializeValue(_))) => {}
            other => panic!("expected IO or DeserializeValue error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_zstd_corrupt_frame_async_pack_iter() {
        use crate::archive::pack_iter::AsyncPackIter;

        let (tmp_dir, _pos) = build_pack_with_corrupt_zstd_frame();
        let path = tmp_dir.path().join("pack_corrupt");
        let file = tokio::fs::File::open(&path).await.expect("open file");
        let mut iter = AsyncPackIter::<TestRec, _>::open(file, 0).await.expect("iter open");
        match iter.next().await {
            Some(Err(FetchError::IO(_))) | Some(Err(FetchError::DeserializeValue(_))) => {}
            other => panic!("expected IO or DeserializeValue error, got {other:?}"),
        }
    }

    #[test]
    fn test_read_bytes_rejects_out_of_range() {
        let tmp_path = TempDir::with_prefix("test_read_bytes_oob").expect("temp dir");
        let path = tmp_path.path().join("pack_oob");
        let mut pack: TestPack =
            Pack::open(&path, 0, false, PackCompression::None, 0).expect("open pack");
        pack.append(&TestRec { idx: 1, name: "x".to_string() }).expect("append");
        pack.commit().expect("commit");

        // An end far past EOF must error without attempting a giant allocation.
        assert!(pack.read_bytes(0, u64::MAX).is_err());
        // An inverted range must error.
        assert!(pack.read_bytes(100, 10).is_err());
        // A valid in-range request still works.
        let len = pack.file_len();
        assert!(pack.read_bytes(0, len).is_ok());
    }
}
