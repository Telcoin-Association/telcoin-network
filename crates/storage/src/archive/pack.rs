//! A data/log file of archival data.  Once written is only indended to be read and shared with
//! other nodes.

use serde::{de::DeserializeOwned, Serialize};
use tn_types::{encode_into_buffer, try_decode, try_decode_from_read};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::archive::{
    error::{
        commit::CommitError, fetch::FetchError, flush::FlushError, insert::AppendError,
        load_header::LoadHeaderError, open::OpenError, rename::RenameError,
    },
    fxhasher::FxHasher,
    pack_iter::{PackIter, MAX_RECORD_SIZE},
};

use super::{
    crc::add_crc32,
    data_file::{DataFileReader, MapView, MmapDataFile, MmapFileOptions},
};
use std::{
    fmt::Debug,
    fs,
    hash::Hasher as _,
    io::{self, Read, Seek, Write},
    marker::PhantomData,
    path::Path,
};

/// The sequential record iterator [`Pack::raw_iter`] returns.
pub type RawIter<V> = PackIter<V, DataFileReader>;

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
        Self::open_with(path, uid_idx, read_only, compression, version, MmapFileOptions::default())
    }

    /// [`Self::open`] with explicit options for the data file's mapping (for example a
    /// [`reserve`](MmapFileOptions::reserve) so the mapping never moves while the pack is open).
    pub fn open_with<P: AsRef<Path>>(
        path: P,
        uid_idx: u64,
        read_only: bool,
        compression: PackCompression,
        version: u16,
        opts: MmapFileOptions,
    ) -> Result<Self, OpenError> {
        Ok(Self { inner: PackInner::open(path, uid_idx, read_only, compression, version, opts)? })
    }

    /// Length of the Pack file.
    pub fn file_len(&self) -> u64 {
        self.inner.file_len()
    }

    /// True when the backing data file was opened without a valid clean-close sentinel — it was not
    /// sealed by a clean shutdown and is most likely still padded/torn, so a consistency check
    /// should treat the pack as needing recovery.
    pub fn opened_unclean(&self) -> bool {
        self.inner.opened_unclean()
    }

    /// Clear the backing data file's "opened unclean" flag after a successful recovery/heal, so a
    /// clean `Drop` re-seals the pack (and a reopen reports it clean) instead of replaying the WAL
    /// on every restart. Callers invoke this once recovery has made the log + its derived
    /// indexes self-consistent. See
    /// [`MmapDataFile::mark_consistent`](crate::archive::data_file::MmapDataFile::mark_consistent).
    pub fn mark_consistent(&mut self) {
        self.inner.mark_consistent();
    }

    /// Clamp a read-only pack's read bound down to `logical_end` (the index-attested record end),
    /// so reads never touch bytes above the committed data even if the underlying file were
    /// physically padded. Defense-in-depth against the read-only-mmap SIGBUS window; no-op on a
    /// writable pack.
    pub fn set_read_bound(&mut self, logical_end: u64) {
        self.inner.data_file.set_read_bound(logical_end);
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&self, pos: u64) -> Result<V, FetchError> {
        self.inner.read_record(pos)
    }

    /// Read raw bytes from the file.  Will return an error if not able to read all the bytes.
    pub fn read_bytes(&self, start_pos: u64, end_pos: u64) -> Result<&[u8], FetchError> {
        self.inner.read_bytes(start_pos, end_pos)
    }

    /// The CRC-checked, zero-copy value bytes of the record at `pos`, borrowed from the mmap -- the
    /// raw byte-log read paired with [`Self::append_raw`]. Unlike [`Self::fetch`] it does not
    /// decode through the `V` codec or allocate a `Vec`, so a byte-oriented caller can decode
    /// straight from the map. Valid for an uncompressed pack (`PackCompression::None`).
    pub fn record_bytes(&self, pos: u64) -> Result<&[u8], FetchError> {
        self.inner.record_bytes(pos)
    }

    /// The lock-free reader view of the data file's mapping (see [`MapView`]). The owner publishes
    /// how much of the log readers may see ([`MapView::publish_len`]).
    pub(crate) fn view(&self) -> std::sync::Arc<MapView> {
        self.inner.data_file.view()
    }

    /// [`Self::record_bytes`] through a lock-free [`MapView`]: CRC-check the record at `pos` within
    /// the view's published bytes and borrow its payload. Log records never change once appended,
    /// so a published record is safe to read with no lock.
    pub(crate) fn record_bytes_in(view: &MapView, pos: u64) -> Result<&[u8], FetchError> {
        checked_payload_in(view.tail(pos))
    }

    /// Read the record size (with crc32) at position.
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    pub fn record_size(&self, pos: u64) -> Result<u32, FetchError> {
        self.inner.record_size(pos)
    }

    /// True iff the record-length-prefix region at `pos` has been written — any of the up-to-4
    /// prefix bytes within the logical data `[pos, min(pos + 4, len()))` is non-zero.
    ///
    /// A real record's 4-byte length prefix is non-zero, whereas freshly-grown mmap capacity
    /// padding reads as zeros. This checks every one of the up-to-4 prefix bytes (not just the
    /// leading one — a valid length that is a multiple of 256 has a zero low byte), so it
    /// distinguishes "a record (even a torn one) was written here" — which a caller must not
    /// silently overwrite — from unwritten zero padding or an empty tail. Bounded by the
    /// logical end, so a cleanly-sealed file with nothing at `pos` (`pos >= len()`) returns
    /// false, and a crash-grown, zero-padded file with no record written past `pos` also
    /// returns false.
    pub(crate) fn record_present_at(&self, pos: u64) -> bool {
        let avail = self.inner.data_file.len().saturating_sub(pos).min(4) as usize;
        avail != 0
            && self.inner.data_file.slice(pos, avail).is_some_and(|b| b.iter().any(|&x| x != 0))
    }

    /// True iff any logical byte at or after `pos` is non-zero — i.e. real record bytes (a meta or
    /// an output) were written past `pos`, as opposed to unwritten, zero-filled mmap capacity
    /// padding. Reads a zero-copy view of the mapped bytes and short-circuits at the first non-zero
    /// byte, so an occupied pack returns immediately while a genuinely empty, all-zero-padded tail
    /// is fully scanned.
    ///
    /// Complements [`Self::record_present_at`], which inspects only the 4-byte length prefix: a
    /// prefix corrupted to zero reads there as "no record", but real content can still sit past
    /// `pos`. `open_append` uses this to tell a genuinely header-only file (safe to initialize)
    /// from an occupied pack whose meta length-prefix was zeroed (which a blind re-initialize would
    /// erase).
    pub(crate) fn any_content_after(&self, pos: u64) -> bool {
        let span = self.inner.data_file.len().saturating_sub(pos) as usize;
        span != 0
            && self.inner.data_file.slice(pos, span).is_some_and(|b| b.iter().any(|&x| x != 0))
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

    /// Append already-serialized `value` bytes as one record, returning its position. Sibling of
    /// [`Self::append`] that skips the value serialize step -- for a caller that has already
    /// produced the final bytes (e.g. `tndb`, which serializes at its typed layer), so re-encoding
    /// them through the `V` codec would be a redundant copy. Read the bytes back with
    /// [`Self::record_bytes`] (not [`Self::fetch`], which would decode them through the `V` codec).
    pub fn append_raw(&mut self, value: &[u8]) -> Result<u64, AppendError> {
        self.inner.append_raw(value)
    }

    /// Test-only failure injector: make the next append fail with
    /// [`AppendError::WriteDataError`], the same classification a real io write failure gets.
    /// The append path then marks the pack failed, which is the poisoned state the queued-save
    /// regression tests start from.
    #[cfg(test)]
    pub(crate) fn fail_next_append_for_test(&mut self) {
        self.fail_next_append_with_kind_for_test(io::ErrorKind::StorageFull);
    }

    /// Test-only failure injector like [`Self::fail_next_append_for_test`], with the io error
    /// kind of the injected failure chosen by the caller.
    #[cfg(test)]
    pub(crate) fn fail_next_append_with_kind_for_test(&mut self, kind: io::ErrorKind) {
        self.inner.fail_next_append = Some(kind);
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
    ///
    /// A pure durability barrier: it changes nothing but the data file's own (atomic) flush
    /// bookkeeping, so it needs only shared access. An owner that serializes appends behind a lock
    /// can commit under that lock's shared mode, leaving readers unblocked during the sync.
    pub fn commit(&self) -> Result<(), CommitError> {
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

    /// Mark the underlying data file to be removed (not sealed) when this handle drops. Used to
    /// abandon a partial/failed build cheaply, skipping the clean-close
    /// msync+truncate+sentinel+fsync.
    pub fn set_remove_on_drop(&mut self) {
        self.inner.data_file.set_remove_on_drop();
    }

    /// Rename the pack file to name.
    pub fn rename<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RenameError> {
        self.inner.rename(path)
    }

    /// Roll the log's logical end back to `new_len`, zeroing the abandoned region, WITHOUT a
    /// physical truncate/remap (see [`MmapDataFile::rewind_to`]). Used to atomically undo a partial
    /// append without opening a read-only-mmap SIGBUS window; a later append lands at `new_len`.
    pub fn rewind_to(&mut self, new_len: u64) {
        self.inner.data_file.rewind_to(new_len);
    }

    /// The durable acked-data watermark recovered from the tail commit marker of an unclean pack,
    /// if present (see [`MmapDataFile::committed_end`]). `None` on a clean/fresh open. Recovery
    /// uses it as an index-free way to detect at-rest corruption of the last committed record.
    pub fn committed_end(&self) -> Option<u64> {
        self.inner.data_file.committed_end()
    }

    /// Stamp the tail commit marker (`committed_end == file_len()`) — a best-effort, no-extra-sync
    /// record of the acked frontier (see [`MmapDataFile::stamp_commit_marker`]). Call AFTER
    /// [`Self::commit`] so the marker can never be ahead of durable data.
    pub fn stamp_commit_marker(&mut self) {
        self.inner.data_file.stamp_commit_marker();
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    /// Each iterator reads at its own position, so several can be live over one pack at once.
    pub fn raw_iter(&self) -> Result<RawIter<V>, LoadHeaderError> {
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
    /// The zstd context every compressed append reuses, created on the first one.
    zstd_ctx: Option<ZstdCtx>,
    /// The reused buffer every append encodes its value into (see [`write_payload`]).
    stage: Vec<u8>,
    /// Root cause of the failed state: a copy of the io error from the append that failed
    /// the pack. While this is `Some`, each append and each commit returns a copy of this
    /// error so callers see the root cause and not a generic guard error.
    failed: Option<io::Error>,
    read_only: bool,
    uid_idx: u64, // Store for opening an iterator.
    /// Test-only: when set, the next append fails as if the data write hit an io error of this
    /// kind.
    #[cfg(test)]
    fail_next_append: Option<io::ErrorKind>,
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
        opts: MmapFileOptions,
    ) -> Result<Self, OpenError> {
        let (data_file, header) =
            Self::open_data_file(path, uid_idx, read_only, compression, version, opts)
                .map_err(OpenError::DataFileOpen)?;
        Ok(Self {
            header,
            data_file,
            zstd_ctx: None,
            stage: Vec::new(),
            failed: None,
            read_only,
            uid_idx,
            #[cfg(test)]
            fail_next_append: None,
            _value: PhantomData,
        })
    }

    /// Length of the Pack file.
    fn file_len(&self) -> u64 {
        self.data_file.len()
    }

    /// True when the data file was opened without a valid clean-close sentinel (not cleanly
    /// sealed).
    fn opened_unclean(&self) -> bool {
        self.data_file.opened_unclean()
    }

    /// Clear the backing data file's "opened unclean" flag (see
    /// [`MmapDataFile::mark_consistent`](crate::archive::data_file::MmapDataFile::mark_consistent)).
    fn mark_consistent(&mut self) {
        self.data_file.mark_consistent();
    }

    /// Read raw bytes from the file.  Will return an error if not able to read all the bytes.
    fn read_bytes(&self, start_pos: u64, end_pos: u64) -> Result<&[u8], FetchError> {
        // Validate the range against the file length so a corrupt or oversized bound (a
        // caller-supplied out-of-range end offset) errors instead of reading past the log.
        if start_pos > end_pos || end_pos > self.data_file.len() {
            return Err(FetchError::IO(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_bytes range out of bounds",
            )));
        }
        let bytes = self.data_file.slice(start_pos, (end_pos - start_pos) as usize).unwrap_or(&[]);
        Ok(bytes)
    }

    /// Test-only injection point: fail the append the way a real io write failure fails.
    /// Armed by [`Pack::fail_next_append_for_test`] (StorageFull, a sentinel kind that is not the
    /// Other default, so tests can assert that a replayed copy keeps the kind) or
    /// [`Pack::fail_next_append_with_kind_for_test`]; disarms after one use.
    #[cfg(test)]
    fn injected_append_failure(&mut self) -> Result<(), AppendError> {
        self.fail_next_append.take().map_or(Ok(()), |kind| {
            Err(AppendError::WriteDataError(io::Error::new(kind, "injected write failure")))
        })
    }

    /// Do the actual insert so the public function can rollback easily on an error.
    fn append_inner(&mut self, value: &V) -> Result<u64, AppendError> {
        let record_pos = self.data_file.len();

        #[cfg(test)]
        self.injected_append_failure()?;

        self.write_record(Payload::Value(value))?;
        Ok(record_pos)
    }

    /// Raw-bytes sibling of [`Self::append_inner`]: frame `value` as one record without the
    /// serialize step (the bytes are already final).
    fn append_raw_inner(&mut self, value: &[u8]) -> Result<u64, AppendError> {
        let record_pos = self.data_file.len();

        #[cfg(test)]
        self.injected_append_failure()?;

        self.write_record(Payload::Raw(value))?;
        Ok(record_pos)
    }

    /// Append `payload` as one framed record (see [`frame_record`]), compressing it with this
    /// pack's reused zstd context when the pack is compressed. A record that fails part-way is
    /// rolled back: whatever of it reached the file is zeroed and the logical end moves back to
    /// where the record began, so a refused or failed append leaves the log as it was.
    fn write_record(&mut self, payload: Payload<'_, V>) -> Result<(), AppendError> {
        let record_pos = self.data_file.len();
        let zstd = match self.header.compression {
            PackCompression::None => None,
            PackCompression::ZStd => Some(match &mut self.zstd_ctx {
                Some(ctx) => ctx,
                slot @ None => slot.insert(ZstdCtx::new()?),
            }),
        };
        let result = frame_record(&mut self.data_file, zstd, &mut self.stage, payload);
        if result.is_err() {
            self.data_file.rewind_to(record_pos);
        }
        result
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
        self.classify_append(result)
    }

    /// Raw-bytes sibling of [`Self::append`]: append already-serialized `value` bytes as one record
    /// (no codec re-encode), sharing the same read-only guard, failed-state guard, and
    /// poison classification. Used by the byte-oriented `tndb` value log.
    fn append_raw(&mut self, value: &[u8]) -> Result<u64, AppendError> {
        if self.read_only {
            return Err(AppendError::ReadOnly);
        }
        self.failed_cause().map_err(AppendError::WriteDataError)?;
        let result = self.append_raw_inner(value);
        self.classify_append(result)
    }

    /// Poison the pack on a write io error, shared between [`Self::append`] and
    /// [`Self::append_raw`]. A write io error, whatever its kind, moves the pack to the failed
    /// state. A record too large to ever be read back (`RecordTooLarge`) is rolled back where it
    /// stopped ([`Self::write_record`]), so the log is left as it was and the pack stays healthy:
    /// neither it nor the other non-write errors poison the pack.
    fn classify_append(&mut self, result: Result<u64, AppendError>) -> Result<u64, AppendError> {
        if let Err(err) = &result {
            match err {
                // A write io error, whatever its kind, indicates a failed DB that can no longer be
                // inserted to.
                AppendError::WriteDataError(io_err) => {
                    self.failed = Some(Self::copy_io_error(io_err));
                }
                // These errors do not indicate a failed DB. A `RecordTooLarge` record is rolled
                // back where it stopped, so the log is left as it was (the read path likewise
                // rejects an oversize record without failing the pack).
                AppendError::RecordTooLarge { .. }
                | AppendError::SerializeValue(_)
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
    fn commit(&self) -> Result<(), CommitError> {
        if self.read_only {
            return Err(CommitError::ReadOnly);
        }
        self.failed_cause().map_err(CommitError::Failed)?;
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
        opts: MmapFileOptions,
    ) -> Result<(MmapDataFile, DataHeader), LoadHeaderError> {
        let mut data_file = MmapDataFile::open_with(path, ro, opts)?;
        let header = Self::init_header(&mut data_file, uid_idx, compression, version, ro)?;
        Ok((data_file, header))
    }

    /// Write a fresh [`DataHeader`] to an empty (or never-written) file, or load and validate an
    /// existing one, then flush.
    fn init_header(
        data_file: &mut MmapDataFile,
        uid_idx: u64,
        compression: PackCompression,
        version: u16,
        read_only: bool,
    ) -> Result<DataHeader, LoadHeaderError> {
        let file_end = data_file.data_file_end();
        // A file with a physical size but all-zero logical bytes was sized by a first write
        // (`grow_to`'s preallocate + ftruncate) that crashed before the header reached disk. It is
        // semantically unwritten — the same "all-zero == unwritten" rule the pack applies to
        // records — so treat it as fresh rather than feeding zeros to `load_header` (which fails
        // with a bare CRC error no door can classify). `is_unwritten` is gated on `opened_unclean`
        // since a clean close always leaves a non-zero trailing sentinel.
        let never_written = file_end != 0 && data_file.opened_unclean() && data_file.is_unwritten();
        if file_end == 0 || (never_written && !read_only) {
            if never_written {
                // Reset the sized-but-unwritten file to empty; this discards only zeros, so it is a
                // fresh initialization, not a repair of any committed data.
                data_file.truncate(0)?;
            }
            let header = DataHeader::new(uid_idx, compression, version);
            header.write_header(data_file)?;
            // Make the header durable (msync + fsync) NOW, before anything can observe the pack.
            // Otherwise a crash after `grow_to` sized the file but before the meta commit leaves an
            // empty or all-zero file; syncing here narrows that window and keeps a fresh header on
            // disk.
            data_file.sync_disk()?;
            return Ok(header);
        } else if never_written {
            // Read-only door: it cannot re-initialize, so surface a classifiable, actionable error
            // instead of a bare CRC failure.
            return Err(LoadHeaderError::Unwritten);
        }
        let header = DataHeader::load_header(data_file, uid_idx)?;
        if header.version() > version {
            // Do not allow a newer version than we request but allow an older.
            return Err(LoadHeaderError::InvalidVersion);
        }
        if header.appnum() != 1 {
            return Err(LoadHeaderError::InvalidAppNum);
        }
        data_file.flush()?;
        Ok(header)
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn read_record(&self, position: u64) -> Result<V, FetchError> {
        self.read_record_into(position)
    }

    /// CRC-check the record at `position` and return the zero-copy `&[u8]` payload -- the raw
    /// stored value bytes (before any decompression), borrowed straight from the mmap. Shared
    /// by [`Self::read_record_into`] and [`Self::record_bytes`].
    fn checked_payload(&self, position: u64) -> Result<&[u8], FetchError> {
        checked_payload_in(self.data_file.tail(position))
    }

    /// Decode the record at `position`, decompressing first if the pack is compressed. The value
    /// `bytes` are decoded straight from where they were read (the mmap, for the zero-copy path).
    fn read_record_into(&self, position: u64) -> Result<V, FetchError> {
        let payload = self.checked_payload(position)?;
        match self.header.compression {
            PackCompression::None => {
                try_decode(payload).map_err(|e| FetchError::DeserializeValue(e.to_string()))
            }
            PackCompression::ZStd => {
                // Stream-decode straight from the decompressor, capping decompressed bytes at
                // MAX_RECORD_SIZE with `take` -- no interim buffer (this is the `&self`
                // random-access read, where a reusable buffer would mean a per-read alloc or a
                // `&mut self` field). A record that decompresses past the cap is rejected by
                // *failing to decode* (the reader hits the cap mid-value); unlike the iterator
                // paths it does not surface `RequestedDecompressSizeTooLarge`.
                let mut decoder = zstd::stream::read::Decoder::new(payload)?;
                decoder.window_log_max(24)?;
                let limited = decoder.take(MAX_RECORD_SIZE as u64);
                try_decode_from_read(limited)
                    .map_err(|e| FetchError::DeserializeValue(e.to_string()))
            }
        }
    }

    /// The CRC-checked, zero-copy value bytes of the record at `position`, borrowed from the mmap
    /// -- the raw byte-log read that skips the `V` codec (and the `Vec` [`Self::read_record`]
    /// would allocate). Returns the raw stored payload: valid for an uncompressed pack
    /// (`PackCompression::None`); on a compressed pack the bytes are still compressed.
    fn record_bytes(&self, position: u64) -> Result<&[u8], FetchError> {
        self.checked_payload(position)
    }

    /// Read the record size (with crc32) at position.
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn record_size(&self, position: u64) -> Result<u32, FetchError> {
        Ok(self.checked_payload(position)?.len() as u32 + 8)
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

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    fn raw_iter(&self) -> Result<RawIter<V>, LoadHeaderError> {
        // `try_clone` does NOT truncate the capacity padding, so read to the logical `end` it
        // returns rather than physical EOF — otherwise a concurrent append that re-grows and
        // re-pads the file would feed the iterator trailing zeros (a 0-size record → CRC failure).
        let (dat_file, end) = self.data_file.try_clone()?;
        PackIter::open(dat_file, self.uid_idx, end)
    }
}

/// CRC-check the record at the start of `tail` (every readable byte from the record's position on:
/// the data file's own mapping, or a lock-free [`MapView`]) and return its payload (the raw stored
/// value bytes, before any decompression) borrowed in place.
///
/// A record is `[len u32 | payload | crc u32]` with the CRC over the contiguous `[len | payload]`,
/// so it is checked with one bounds-checked borrow and one CRC call over those bytes.
fn checked_payload_in(tail: Option<&[u8]>) -> Result<&[u8], FetchError> {
    let Some(tail) = tail.filter(|tail| tail.len() >= 4) else {
        return Err(FetchError::IO(io::Error::other("Unable to get mmap slice.")));
    };
    let val_size = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
    if val_size > MAX_RECORD_SIZE {
        return Err(FetchError::RequestedSizeTooLarge(val_size, MAX_RECORD_SIZE));
    }
    let crc_at = 4 + val_size as usize;
    let Some(stored) = tail.get(crc_at..crc_at + 4) else {
        return Err(FetchError::IO(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Unable to read the full record and CRC",
        )));
    };
    if crc32fast::hash(&tail[..crc_at])
        != u32::from_le_bytes([stored[0], stored[1], stored[2], stored[3]])
    {
        return Err(FetchError::CrcFailed);
    }
    Ok(&tail[4..crc_at])
}

/// What one append writes: a value to encode through the pack's codec, or bytes the caller has
/// already serialized (the raw byte-log path, e.g. `tndb`, which serializes at its typed layer).
enum Payload<'a, V> {
    Value(&'a V),
    Raw(&'a [u8]),
}

/// The zstd compression context a pack's appends reuse, instead of allocating a fresh one per
/// record. Configured like `zstd::stream::write::Encoder::new(_, 0)` (zstd's default level), so the
/// frames it produces are byte-identical to that encoder's.
struct ZstdCtx(zstd::zstd_safe::CCtx<'static>);

impl Debug for ZstdCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ZstdCtx")
    }
}

impl ZstdCtx {
    fn new() -> io::Result<Self> {
        let mut ctx = zstd::zstd_safe::CCtx::create();
        ctx.set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(0)).map_err(zstd_error)?;
        Ok(Self(ctx))
    }
}

/// A zstd library error code as an io error.
fn zstd_error(code: usize) -> io::Error {
    io::Error::other(zstd::zstd_safe::get_error_name(code))
}

/// The payload end of a record write: everything written goes straight into the data file (the
/// frame's length and CRC are taken from the written bytes afterwards).
struct RecordSink<'a> {
    file: &'a mut MmapDataFile,
}

impl Write for RecordSink<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    /// A no-op: the bytes are in the mapping as soon as they are written, and durability is the
    /// pack's commit, not a per-record flush (the zstd encoder flushes its writer on `finish`).
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Write `payload` to `out` as one write, mapping a failure to the append error it means: a value
/// the codec rejects is [`AppendError::SerializeValue`], a payload past the decoded cap is
/// [`AppendError::RecordTooLarge`] (refused before any of it is written), and a write that failed
/// below is [`AppendError::WriteDataError`].
///
/// A value is encoded whole into `stage`, a buffer the pack reuses for every append. The codec
/// writes a byte vector one byte per call (serde has no byte specialization and `bcs` writes each
/// element on its own), which compiles to a tight loop into a plain `Vec` but costs a full call per
/// byte through any other writer, so the encode stays in the `Vec` and only its result moves on.
///
/// The cap: every read path caps the decompressed payload at [`MAX_RECORD_SIZE`], so a payload that
/// compresses to <= the cap but decodes above it would be appended and acked yet could never be
/// fetched, iterated, replayed, or served to a peer (and an unclean reopen would then see
/// `CorruptPack`).
fn write_payload<V: Serialize, W: Write>(
    mut out: W,
    stage: &mut Vec<u8>,
    payload: Payload<'_, V>,
) -> Result<(), AppendError> {
    let bytes = match payload {
        Payload::Value(value) => {
            stage.clear();
            encode_into_buffer(stage, value)
                .map_err(|e| AppendError::SerializeValue(e.to_string()))?;
            &stage[..]
        }
        Payload::Raw(bytes) => bytes,
    };
    if bytes.len() > MAX_RECORD_SIZE as usize {
        return Err(AppendError::RecordTooLarge { size: bytes.len(), max: MAX_RECORD_SIZE });
    }
    out.write_all(bytes).map_err(AppendError::WriteDataError)
}

/// Write one framed record `[u32 len | payload | u32 crc]` (little-endian) at the end of `file`,
/// writing the payload straight into the mapping: a value is encoded into the reused `stage` buffer
/// (see [`write_payload`]) and, with `zstd`, compressed from there directly into the file, so the
/// compressed bytes are never staged. The length is known only once the
/// payload is written, so its prefix is reserved as zeros and patched in after; the CRC over the
/// then-contiguous `len | payload` is one pass over those written bytes in the mapping.
///
/// Until the prefix is patched the record reads as nothing (a zero length prefix, like capacity
/// padding), and the append is acked only after the CRC is written, so an interrupted append is an
/// unacked tail. On an error the caller rolls the record back ([`PackInner::write_record`]).
fn frame_record<V: Serialize>(
    file: &mut MmapDataFile,
    zstd: Option<&mut ZstdCtx>,
    stage: &mut Vec<u8>,
    payload: Payload<'_, V>,
) -> Result<(), AppendError> {
    let record_pos = file.len();
    file.write_all(&[0; 4])?;
    let mut sink = RecordSink { file };
    match zstd {
        None => write_payload(&mut sink, stage, payload)?,
        Some(ctx) => {
            // A record that failed part-way left its frame unfinished; start every record clean.
            ctx.0.reset(zstd::zstd_safe::ResetDirective::SessionOnly).map_err(zstd_error)?;
            let mut encoder = zstd::stream::write::Encoder::with_context(&mut sink, &mut ctx.0);
            write_payload(&mut encoder, stage, payload)?;
            encoder.finish()?;
        }
    }
    let RecordSink { file } = sink;
    // Pack data files append, so the payload is everything written past the prefix.
    let len = file.len() - record_pos - 4;
    // Every read path refuses a framed record larger than `MAX_RECORD_SIZE` (and a payload past
    // `u32::MAX` would silently truncate the size prefix), so such a record is refused here.
    if len > MAX_RECORD_SIZE as u64 {
        return Err(AppendError::RecordTooLarge { size: len as usize, max: MAX_RECORD_SIZE });
    }
    file.slice_mut(record_pos, 4)
        .ok_or_else(|| io::Error::other("record length prefix is not mapped"))?
        .copy_from_slice(&(len as u32).to_le_bytes());
    let crc = crc32fast::hash(
        file.slice(record_pos, 4 + len as usize)
            .ok_or_else(|| io::Error::other("record is not mapped"))?,
    );
    file.write_all(&crc.to_le_bytes())?;
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
    use std::{
        fs::{File, OpenOptions},
        io::SeekFrom,
    };

    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestRec {
        idx: u64,
        name: String,
    }
    type TestPack = Pack<TestRec>;

    /// The raw byte-log path used by `tndb`: `append_raw` stores value bytes verbatim (no `V`
    /// codec) and `record_bytes` reads them back zero-copy, CRC-checked. Covers empty, small, and
    /// larger payloads, and confirms a flipped payload byte surfaces as `CrcFailed`.
    #[test]
    fn append_raw_and_record_bytes_roundtrip() {
        let tmp = TempDir::with_prefix("pack_append_raw").expect("temp dir");
        let path = tmp.path().join("raw");
        let mut pack: Pack<Vec<u8>> =
            Pack::open(&path, 0, false, PackCompression::None, 1).expect("open");

        let empty: &[u8] = b"";
        let small: &[u8] = b"hello raw world";
        let large: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
        let pos_empty = pack.append_raw(empty).expect("append empty");
        let pos_small = pack.append_raw(small).expect("append small");
        let pos_large = pack.append_raw(&large).expect("append large");

        assert_eq!(pack.record_bytes(pos_empty).expect("read empty"), empty);
        assert_eq!(pack.record_bytes(pos_small).expect("read small"), small);
        assert_eq!(pack.record_bytes(pos_large).expect("read large"), large.as_slice());

        // Records are `[u32 len | payload | u32 crc]`, so the payload starts at pos + 4. Flip one
        // byte and the recomputed CRC no longer matches the stored one.
        let payload = pack.inner.data_file.slice_mut(pos_small + 4, 1).expect("payload slice");
        payload[0] ^= 0xFF;
        assert!(matches!(pack.record_bytes(pos_small), Err(FetchError::CrcFailed)));
    }

    /// A damaged length prefix never yields a payload: a small change mis-frames the CRC
    /// (`CrcFailed`), a huge one trips the size cap, and one reaching past the end is an EOF.
    #[test]
    fn record_bytes_rejects_damaged_length() {
        let tmp = TempDir::with_prefix("pack_damaged_len").expect("temp dir");
        let mut pack: Pack<Vec<u8>> =
            Pack::open(tmp.path().join("raw"), 0, false, PackCompression::None, 1).expect("open");
        let first = pack.append_raw(b"first record").expect("append");
        let last = pack.append_raw(b"last record").expect("append");
        let mut set_len = |pos: u64, len: u32| {
            pack.inner
                .data_file
                .slice_mut(pos, 4)
                .expect("prefix")
                .copy_from_slice(&len.to_le_bytes());
        };
        set_len(first, 11); // was 12
        set_len(last, 11 + 8); // past the end of the log
        assert!(matches!(pack.record_bytes(first), Err(FetchError::CrcFailed)));
        assert!(
            matches!(pack.record_bytes(last), Err(FetchError::IO(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
        let mut set_len = |pos: u64, len: u32| {
            pack.inner
                .data_file
                .slice_mut(pos, 4)
                .expect("prefix")
                .copy_from_slice(&len.to_le_bytes());
        };
        set_len(first, u32::MAX);
        assert!(matches!(pack.record_bytes(first), Err(FetchError::RequestedSizeTooLarge(..))));
        assert!(pack.record_bytes(pack.file_len()).is_err(), "no record at the end of the log");
    }

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

    /// A record whose framed size exceeds `MAX_RECORD_SIZE` is rejected on write (it could
    /// never be read back — the read paths cap at the same size), and because the refused record
    /// is rolled back the pack is NOT poisoned: a later append still succeeds and reads back, with
    /// no partial bytes from the rejected record.
    #[test]
    fn append_rejects_oversized_record_without_poisoning() {
        let tmp_path = TempDir::with_prefix("test_pack_oversize").expect("temp dir");
        let path = tmp_path.path().join("pack_oversize");
        let mut db: TestPack =
            Pack::open(&path, 0, false, PackCompression::None, 0).expect("open pack");

        // With no compression the framed size is the encoded size, so a name past the cap pushes
        // the record over `MAX_RECORD_SIZE`.
        let oversized = TestRec { idx: 1, name: "x".repeat(MAX_RECORD_SIZE as usize + 1) };
        let err = db.append(&oversized).expect_err("an oversized record must be rejected on write");
        assert!(
            matches!(&err, AppendError::RecordTooLarge { max: MAX_RECORD_SIZE, .. }),
            "expected a RecordTooLarge rejection, got: {err:?}"
        );

        // Not poisoned: a normal append and commit still succeed (mirrors the read path rejecting
        // an oversize record without failing the pack).
        db.append(&TestRec { idx: 2, name: "ok".to_string() }).expect("pack must not be poisoned");
        db.commit().expect("commit must succeed");

        // Only the good record was written; the rejected one left no partial bytes behind.
        drop(db);
        let db: TestPack =
            Pack::open(&path, 0, false, PackCompression::None, 0).expect("reopen pack");
        let recs: Vec<TestRec> =
            db.raw_iter().expect("raw iter").map(|r| r.expect("decode")).collect();
        assert_eq!(recs.len(), 1, "only the non-oversized record should be present");
        assert_eq!(recs[0].idx, 2);
        assert_eq!(recs[0].name, "ok");
    }

    /// The buffered framing appends used before they streamed (encode the whole value, compress it
    /// whole with a fresh encoder, then frame it): the oracle the streamed write must match byte
    /// for byte, so the on-disk format is unchanged.
    fn buffered_record(payload: &[u8], compression: PackCompression) -> Vec<u8> {
        let body = match compression {
            PackCompression::None => payload.to_vec(),
            PackCompression::ZStd => {
                let mut out = Vec::new();
                let mut encoder = zstd::stream::write::Encoder::new(&mut out, 0).expect("encoder");
                encoder.write_all(payload).expect("compress");
                encoder.finish().expect("finish");
                out
            }
        };
        let len = (body.len() as u32).to_le_bytes();
        let mut crc = crc32fast::Hasher::new();
        crc.update(&len);
        crc.update(&body);
        let mut record = len.to_vec();
        record.extend_from_slice(&body);
        record.extend_from_slice(&crc.finalize().to_le_bytes());
        record
    }

    /// Streaming a record straight into the file (encode into the compressor into the mapping, the
    /// length prefix patched in after) writes exactly the bytes the buffered framing wrote, for
    /// values and raw bytes, compressed or not: small, empty, and past 128 KiB so a compressed
    /// payload spans several zstd blocks.
    #[test]
    fn streamed_records_match_the_buffered_framing() {
        // Pseudo-random, so zstd has real work to do.
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let noisy: String = (0..300_000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(b'a' + (seed % 26) as u8)
            })
            .collect();
        let values = [
            TestRec { idx: 1, name: "small".to_string() },
            TestRec { idx: 2, name: noisy.clone() },
            TestRec { idx: 3, name: String::new() },
        ];
        let raws: [&[u8]; 3] = [b"", b"raw bytes", noisy.as_bytes()];
        for compression in [PackCompression::None, PackCompression::ZStd] {
            let tmp = TempDir::with_prefix("pack_streamed_framing").expect("temp dir");
            let mut db: TestPack =
                Pack::open(tmp.path().join("pack"), 0, false, compression, 1).expect("open pack");
            for value in &values {
                let start = db.file_len();
                db.append(value).expect("append");
                let expected = buffered_record(&tn_types::encode(value), compression);
                assert_eq!(
                    db.read_bytes(start, db.file_len()).expect("record bytes"),
                    expected.as_slice(),
                    "{compression:?}: value record {} differs from the buffered framing",
                    value.idx
                );
            }
            for raw in raws {
                let start = db.file_len();
                db.append_raw(raw).expect("append raw");
                assert_eq!(
                    db.read_bytes(start, db.file_len()).expect("record bytes"),
                    buffered_record(raw, compression).as_slice(),
                    "{compression:?}: raw record of {} bytes differs from the buffered framing",
                    raw.len()
                );
            }
        }
    }

    /// A value or raw payload past the decoded cap is refused part-way through its streamed write
    /// and rolled back: the log ends where it did, the pack is not poisoned, and the next
    /// record lands exactly where the refused ones began.
    #[test]
    fn oversized_records_roll_back_without_poisoning() {
        for compression in [PackCompression::None, PackCompression::ZStd] {
            let tmp = TempDir::with_prefix("pack_oversize_rollback").expect("temp dir");
            let mut db: TestPack =
                Pack::open(tmp.path().join("pack"), 0, false, compression, 1).expect("open pack");
            db.append(&TestRec { idx: 1, name: "first".to_string() }).expect("append");
            let end = db.file_len();

            let oversized = TestRec { idx: 2, name: "x".repeat(MAX_RECORD_SIZE as usize + 1) };
            let err = db.append(&oversized).expect_err("an oversized value must be refused");
            assert!(
                matches!(err, AppendError::RecordTooLarge { max: MAX_RECORD_SIZE, .. }),
                "{compression:?}: got {err:?}"
            );
            assert_eq!(db.file_len(), end, "{compression:?}: the refused value is rolled back");
            let err = db
                .append_raw(&vec![0_u8; MAX_RECORD_SIZE as usize + 1])
                .expect_err("an oversized raw payload must be refused");
            assert!(
                matches!(err, AppendError::RecordTooLarge { max: MAX_RECORD_SIZE, .. }),
                "{compression:?}: got {err:?}"
            );
            assert_eq!(db.file_len(), end, "{compression:?}: the refused payload is rolled back");

            let pos = db
                .append(&TestRec { idx: 3, name: "after".to_string() })
                .expect("the pack is not poisoned");
            assert_eq!(pos, end, "{compression:?}: the next record starts where the refused began");
            assert_eq!(db.fetch(pos).expect("fetch").name, "after");
        }
    }

    /// An append interrupted after its payload reached the file but before its length prefix was
    /// patched leaves that prefix as the reserved zeros. It must never read as a record: the
    /// records before it read back, and nothing after them does.
    #[test]
    fn interrupted_append_reads_as_no_record() {
        let tmp = TempDir::with_prefix("pack_interrupted_append").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp.path().join("pack"), 0, false, PackCompression::ZStd, 1)
                .expect("open pack");
        db.append(&TestRec { idx: 1, name: "kept".to_string() }).expect("append");
        let pos = db.append(&TestRec { idx: 2, name: "interrupted".to_string() }).expect("append");
        db.inner.data_file.slice_mut(pos, 4).expect("prefix is mapped").fill(0);

        let records: Vec<_> = db.raw_iter().expect("raw iter").collect();
        assert!(
            matches!(records.first(), Some(Ok(rec)) if rec.idx == 1),
            "the record before the interrupted one reads back: {records:?}"
        );
        assert!(
            records.iter().skip(1).all(|r| r.is_err()),
            "the interrupted append must not read as a record: {records:?}"
        );
    }

    /// A writer that records the writes it receives.
    #[derive(Default)]
    struct WriteLog {
        writes: usize,
        bytes: Vec<u8>,
    }

    impl Write for WriteLog {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// The codec writes a byte vector one byte per call. A value must reach the record's writer (a
    /// zstd stream step or a data file write per call) as one write of its encoded bytes, never one
    /// call per byte.
    #[test]
    fn value_payload_reaches_the_writer_in_one_write() {
        let mut stage = Vec::new();
        for size in [10_usize << 10, 200 << 10] {
            let value: Vec<u8> = (0..size).map(|i| i as u8).collect();
            let mut log = WriteLog::default();
            write_payload(&mut log, &mut stage, Payload::Value(&value)).expect("write");
            assert_eq!(log.bytes, tn_types::encode(&value), "{size}-byte value: bytes unchanged");
            assert_eq!(log.writes, 1, "a {size}-byte value must reach the writer in one write");
        }
    }

    /// A writer that fails every write the way a full disk does.
    struct DiskFullWriter;

    impl Write for DiskFullWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::StorageFull, "disk full"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A failed write keeps its io error kind through the streamed encode. The codec reports a
    /// writer's error by message only, so without carrying the original back a full disk would
    /// surface as a generic error.
    #[test]
    fn streamed_write_failure_keeps_its_io_kind() {
        let value = TestRec { idx: 1, name: "value".to_string() };
        for payload in [Payload::Value(&value), Payload::<TestRec>::Raw(b"raw bytes")] {
            match write_payload(DiskFullWriter, &mut Vec::new(), payload) {
                Err(AppendError::WriteDataError(e)) => {
                    assert_eq!(e.kind(), io::ErrorKind::StorageFull, "got {e}")
                }
                other => panic!("expected a WriteDataError, got {other:?}"),
            }
        }
    }

    /// Several `raw_iter`s over one pack each read at their own position: interleaving their
    /// `next()` calls must not make either skip, repeat, or misframe a record. The records total
    /// well past the iterator's read buffer, so the two cursors really do interleave on the file.
    #[test]
    fn concurrent_raw_iters_do_not_share_a_cursor() {
        let tmp_path = TempDir::with_prefix("test_pack_two_iters").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_two_iters"), 0, false, PackCompression::None, 0)
                .expect("open pack");
        let names: Vec<String> = (0..200).map(|idx| format!("{idx}:{}", "x".repeat(200))).collect();
        for (idx, name) in names.iter().enumerate() {
            db.append(&TestRec { idx: idx as u64, name: name.clone() }).expect("append");
        }
        db.commit().expect("commit");

        let mut a = db.raw_iter().expect("iter a");
        let mut b = db.raw_iter().expect("iter b");
        let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
        loop {
            let next_a = a.next().map(|r| r.expect("iter a decodes"));
            let next_b = b.next().map(|r| r.expect("iter b decodes"));
            if next_a.is_none() && next_b.is_none() {
                break;
            }
            got_a.extend(next_a.map(|r| r.name));
            got_b.extend(next_b.map(|r| r.name));
        }
        assert_eq!(got_a, names, "iterator a must see every record in order");
        assert_eq!(got_b, names, "iterator b must see every record in order");
    }

    /// Poisoning follows the error variant, not the io error kind: a write failure whose kind
    /// happens to be `InvalidInput` (e.g. `EINVAL` from growing the data file) is a failed write
    /// like any other and must move the pack to its failed state.
    #[test]
    fn invalid_input_write_failure_poisons_the_pack() {
        let tmp_path = TempDir::with_prefix("test_pack_einval_poisons").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_einval"), 0, false, PackCompression::None, 0)
                .expect("open pack");

        db.fail_next_append_with_kind_for_test(io::ErrorKind::InvalidInput);
        let err = db
            .append(&TestRec { idx: 1, name: "one".to_string() })
            .expect_err("armed append must fail");
        assert!(
            matches!(&err, AppendError::WriteDataError(io_err) if io_err.kind() == io::ErrorKind::InvalidInput),
            "unexpected root cause shape: {err:?}"
        );
        let replayed = db
            .append(&TestRec { idx: 2, name: "two".to_string() })
            .expect_err("an InvalidInput write failure must poison the pack");
        assert_eq!(replayed.to_string(), err.to_string(), "append must replay the root cause");
        assert!(
            matches!(db.commit(), Err(CommitError::Failed(_))),
            "commit must report the failed state"
        );
    }

    #[test]
    fn append_rejects_zstd_record_oversized_when_decoded() {
        // With ZStd the framed-size guard sees the COMPRESSED size, so a highly-compressible
        // value that decodes past `MAX_RECORD_SIZE` but compresses under it would (without
        // the decoded-size check) be appended and acked yet be unreadable (every read path
        // caps the decompressed size). The decoded-size check rejects it at append instead.
        let tmp_path = TempDir::with_prefix("test_pack_oversize_zstd").expect("temp dir");
        let path = tmp_path.path().join("pack_oversize_zstd");
        let mut db: TestPack =
            Pack::open(&path, 0, false, PackCompression::ZStd, 0).expect("open pack");

        // All-'x' compresses to a few hundred bytes, but decodes to > 16 MiB.
        let oversized = TestRec { idx: 1, name: "x".repeat(MAX_RECORD_SIZE as usize + 1) };
        let err =
            db.append(&oversized).expect_err("a ZStd record oversized when decoded is rejected");
        assert!(
            matches!(&err, AppendError::RecordTooLarge { max: MAX_RECORD_SIZE, .. }),
            "expected a RecordTooLarge rejection, got: {err:?}"
        );

        // Not poisoned: a normal append still succeeds and reads back.
        db.append(&TestRec { idx: 2, name: "ok".to_string() }).expect("pack must not be poisoned");
        db.commit().expect("commit must succeed");
        drop(db);
        let db: TestPack =
            Pack::open(&path, 0, false, PackCompression::ZStd, 0).expect("reopen pack");
        let recs: Vec<TestRec> =
            db.raw_iter().expect("raw iter").map(|r| r.expect("decode")).collect();
        assert_eq!(recs.len(), 1, "only the readable record should be present");
        assert_eq!(recs[0].idx, 2);
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

        let db: TestPack =
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
        // The pack was cleanly closed, so the physical file is the logical data plus an 8-byte
        // clean-close sentinel; strip the sentinel to get the logical end to bound the iterator at
        // (the real reopen path does this in `MmapDataFile::open`).
        let end = data_file.metadata().unwrap().len() - crate::archive::data_file::SENTINEL_LEN;
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

    /// A frame that straddles the logical end is torn, not a record: the iterator must report it
    /// rather than finish reading it from bytes past `end`.
    #[test]
    fn raw_iter_rejects_a_frame_straddling_the_logical_end() {
        let tmp_path = TempDir::with_prefix("pack_iter_straddle").expect("temp dir");
        let mut db: TestPack =
            Pack::open(tmp_path.path().join("pack_straddle"), 0, false, PackCompression::None, 0)
                .expect("open pack");
        for i in 1..=3u64 {
            db.append(&TestRec { idx: i, name: format!("v{i}") }).expect("append");
        }
        db.commit().expect("commit");

        // Bound the scan one byte short of the last frame's end: its bytes are all physically
        // present, but its CRC lies past the logical end.
        let (reader, end) = db.inner.data_file.try_clone().expect("clone");
        let mut iter = PackIter::<TestRec, _>::open(reader, 0, end - 1).expect("open iter");
        assert_eq!(iter.next().expect("record 1").expect("decodes").idx, 1);
        assert_eq!(iter.next().expect("record 2").expect("decodes").idx, 2);
        let torn = iter.next().expect("a straddling frame is reported, not skipped");
        assert!(
            matches!(&torn, Err(FetchError::IO(e)) if e.kind() == io::ErrorKind::UnexpectedEof),
            "expected a torn-frame error, got {torn:?}"
        );
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
        // The clean close appended an 8-byte sentinel past the header; strip it so the crafted
        // record lands at the logical end (right after the header) rather than after the sentinel.
        let pos =
            fs::metadata(&path).expect("metadata").len() - crate::archive::data_file::SENTINEL_LEN;

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
        // Drop the clean-close sentinel so the appended record starts at `pos` (the logical end).
        file.set_len(pos).expect("truncate sentinel");
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

    // The in-memory MAX_RECORD_SIZE cap bounds decompressed output at every decompression site.
    // The iterator sites (sync + async) buffer the decompressed bytes and report the precise
    // `RequestedDecompressSizeTooLarge`; the `fetch` site streams the decode with no buffer (a
    // deliberate no-alloc optimization) and so rejects a bomb by *failing to decode* -- a
    // `DeserializeValue`, not the size-cap error. These parity tests pin that intended split.

    #[test]
    fn test_zstd_decompression_bomb_fetch() {
        let (tmp_dir, pos) = build_pack_with_decompression_bomb();
        let path = tmp_dir.path().join("pack_bomb");
        let pack: TestPack =
            Pack::open(&path, 0, true, PackCompression::ZStd, 0).expect("open pack");
        match pack.fetch(pos) {
            // Streaming fetch caps decompressed bytes with `take` and rejects the bomb by failing
            // to decode (bcs sees data past the record) rather than reporting the size cap.
            Err(FetchError::DeserializeValue(_)) => {}
            other => panic!("expected the bomb to fail decoding, got {other:?}"),
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
        let mut iter =
            AsyncPackIter::<TestRec, _>::open(file, 0, u16::MAX).await.expect("iter open");
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
        let pack: TestPack =
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
        let mut iter =
            AsyncPackIter::<TestRec, _>::open(file, 0, u16::MAX).await.expect("iter open");
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
