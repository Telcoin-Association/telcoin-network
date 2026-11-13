//! Changeset offset sidecar file I/O.
//!
//! Provides append-only writing and O(1) random-access reading for changeset offsets.
//! The file format is fixed-width 16-byte records: `[offset: u64 LE][num_changes: u64 LE]`.
//!
//! TN(TN-0121): a checksummed jar's sidecar (`reth_nippy_jar::NippyJar::has_checksums`) has
//! 20-byte records, `[offset][num_changes][crc: u32 LE]`: the CRC-32 (ISO-HDLC) of the first 16
//! bytes, started from [`record_seed`] of the record's index in the file (the block's distance
//! from the file's first block) instead of the standard initial value. The seed binds a record to
//! its block, so a record shifted or copied to another block fails as well as a changed byte. The
//! jar's configuration says which format its sidecar has; every reader and writer is told.

use crate::ChangesetOffset;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, OnceLock, PoisonError,
    },
};

/// Size of a record without a checksum.
const PLAIN_RECORD_SIZE: usize = 16;
/// Size of a checksummed record.
const CHECKSUMMED_RECORD_SIZE: usize = PLAIN_RECORD_SIZE + 4;

/// TN(TN-0121): the size in bytes of one record of a sidecar with or without checksums.
pub const fn changeset_offset_record_size(checksums: bool) -> u64 {
    if checksums {
        CHECKSUMMED_RECORD_SIZE as u64
    } else {
        PLAIN_RECORD_SIZE as u64
    }
}

/// TN(TN-0121): the initial value of the checksum of the record at `index`, in crc32fast's
/// convention (`crc32fast::Hasher::new_with_initial`, where 0 gives the standard CRC-32):
/// `0xC2B2_AE35 ^ (index as u32) ^ ((index >> 32) as u32).rotate_left(16)`. Distinct for every
/// index below 2^32, and distinct from the value seeds' constant.
const fn record_seed(index: u64) -> u32 {
    0xC2B2_AE35 ^ (index as u32) ^ ((index >> 32) as u32).rotate_left(16)
}

/// The checksum of the record at `index`, over its first 16 bytes.
fn record_checksum(index: u64, record: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new_with_initial(record_seed(index));
    hasher.update(&record[..PLAIN_RECORD_SIZE]);
    hasher.finalize()
}

/// TN(TN-0121): a changeset offset record that fails its checksum: the sidecar is damaged.
///
/// Returned as the inner error of an [`io::ErrorKind::InvalidData`] error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetOffsetChecksumMismatch {
    /// The sidecar file.
    pub path: PathBuf,
    /// The record's index in the file.
    pub index: u64,
}

impl core::fmt::Display for ChangesetOffsetChecksumMismatch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "changeset offsets {} are damaged: record {} fails its checksum",
            self.path.display(),
            self.index
        )
    }
}

impl std::error::Error for ChangesetOffsetChecksumMismatch {}

/// Decodes the record at `index`, verifying its checksum if it has one.
fn decode_record(
    record: &[u8],
    index: u64,
    checksums: bool,
    path: &Path,
) -> io::Result<ChangesetOffset> {
    if checksums &&
        record_checksum(index, record).to_le_bytes() !=
            record[PLAIN_RECORD_SIZE..CHECKSUMMED_RECORD_SIZE]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ChangesetOffsetChecksumMismatch { path: path.to_path_buf(), index },
        ));
    }
    let offset = u64::from_le_bytes(record[..8].try_into().unwrap());
    let num_changes = u64::from_le_bytes(record[8..PLAIN_RECORD_SIZE].try_into().unwrap());
    Ok(ChangesetOffset::new(offset, num_changes))
}

/// Writer for appending changeset offsets to a sidecar file.
#[derive(Debug)]
pub struct ChangesetOffsetWriter {
    file: File,
    /// Number of records written.
    records_written: u64,
    /// TN(TN-0121): whether records carry checksums.
    checksums: bool,
    /// Record size in bytes.
    record_size: usize,
}

impl ChangesetOffsetWriter {
    /// Opens or creates the changeset offset file for appending.
    ///
    /// The file is healed to match `committed_len` (from the segment header):
    /// - Partial records (from crash mid-write) are truncated to record boundary
    /// - Extra complete records (from crash after sidecar sync but before header commit) are
    ///   truncated to match the committed length
    /// - If the file has fewer records than committed, returns an error (data corruption)
    ///
    /// This mirrors `NippyJar`'s healing behavior where config/header is the commit boundary.
    ///
    /// TN(TN-0121): `checksums` is the jar's [`changeset_offset_record_size`] format. The heal
    /// works on lengths only and never reads a record, so it cannot act on a damaged one.
    pub fn new(path: impl AsRef<Path>, committed_len: u64, checksums: bool) -> io::Result<Self> {
        let record_size = changeset_offset_record_size(checksums) as usize;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let file_len = file.metadata()?.len();
        let remainder = file_len % record_size as u64;

        // First, truncate any partial record from crash mid-write
        let aligned_len = if remainder != 0 {
            let truncated_len = file_len - remainder;
            tracing::warn!(
                target: "reth::static_file",
                path = %path.as_ref().display(),
                original_len = file_len,
                truncated_len,
                "Truncating partial changeset offset record"
            );
            file.set_len(truncated_len)?;
            // TN(TN-0117): a plain `fsync` on macOS (no `F_FULLFSYNC`), here and below.
            reth_fs_util::fsync_file(&file)?; // Sync required for crash safety
            truncated_len
        } else {
            file_len
        };

        let records_in_file = aligned_len / record_size as u64;

        // Heal sidecar to match committed header length
        match records_in_file.cmp(&committed_len) {
            std::cmp::Ordering::Greater => {
                // Sidecar has uncommitted records from a crash - truncate them
                let target_len = committed_len * record_size as u64;
                tracing::warn!(
                    target: "reth::static_file",
                    path = %path.as_ref().display(),
                    sidecar_records = records_in_file,
                    committed_len,
                    "Truncating uncommitted changeset offset records after crash recovery"
                );
                file.set_len(target_len)?;
                reth_fs_util::fsync_file(&file)?; // Sync required for crash safety
            }
            std::cmp::Ordering::Less => {
                // INVARIANT VIOLATION: This should be impossible if healing ran correctly.
                //
                // All code paths call `heal_changeset_sidecar()` before this function, which
                // validates the sidecar against NippyJar state and corrects the header to match
                // the actual file size. Therefore, `committed_len` should always equal or exceed
                // `records_in_file` when this function is called.
                //
                // If we reach this error, it indicates:
                // - A bug in the healing logic (header not corrected properly)
                // - This function was called directly without going through healing
                // - External corruption occurred between healing and opening (extremely unlikely)
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "INVARIANT VIOLATION: Changeset offset sidecar has {} records but header expects {} \
                         (healing should have prevented this - possible bug in healing logic): {}",
                        records_in_file,
                        committed_len,
                        path.as_ref().display()
                    ),
                ));
            }
            std::cmp::Ordering::Equal => {}
        }

        let records_written = committed_len;
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self { file, records_written, checksums, record_size })
    }

    /// Appends a single changeset offset record.
    pub fn append(&mut self, offset: &ChangesetOffset) -> io::Result<()> {
        let mut buf = [0u8; CHECKSUMMED_RECORD_SIZE];
        buf[..8].copy_from_slice(&offset.offset().to_le_bytes());
        buf[8..PLAIN_RECORD_SIZE].copy_from_slice(&offset.num_changes().to_le_bytes());
        if self.checksums {
            let checksum = record_checksum(self.records_written, &buf);
            buf[PLAIN_RECORD_SIZE..].copy_from_slice(&checksum.to_le_bytes());
        }
        self.file.write_all(&buf[..self.record_size])?;
        self.records_written += 1;
        Ok(())
    }

    /// Appends multiple changeset offset records.
    pub fn append_many(&mut self, offsets: &[ChangesetOffset]) -> io::Result<()> {
        for offset in offsets {
            self.append(offset)?;
        }
        Ok(())
    }

    /// Syncs all data to disk. Must be called before committing the header.
    pub fn sync(&mut self) -> io::Result<()> {
        reth_fs_util::fsync_file(&self.file)
    }

    /// Truncates the file to contain exactly `len` records and syncs to disk.
    /// Used after prune operations to reclaim space.
    ///
    /// The sync is required for crash safety - without it, a crash could
    /// resurrect the old file length.
    pub fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len * self.record_size as u64)?;
        reth_fs_util::fsync_file(&self.file)?;
        self.records_written = len;
        Ok(())
    }

    /// Returns the number of records in the file.
    pub const fn len(&self) -> u64 {
        self.records_written
    }

    /// Returns true if the file is empty.
    pub const fn is_empty(&self) -> bool {
        self.records_written == 0
    }
}

/// TN(TN-0144): the records of one chunk of [`ChangesetOffsetCache`], 1280 B with checksums.
///
/// A miss reads the whole chunk, and at this size that costs about one record's `pread` (Mac:
/// 0.48-0.61 us against 0.43-0.52 us), so a working set larger than the cache reads as fast as
/// reading every record from the file did.
const CHUNK_RECORDS: u64 = 64;

/// TN(TN-0144): the bytes of `.csoff` records [`ChangesetOffsetCache::shared`] keeps in memory, for
/// every changeset jar of the process together (both changeset segments, every file, every
/// snapshot): the records of about 105k blocks of both segments.
pub const CHANGESET_OFFSET_CACHE_BYTES: usize = 4 << 20;

/// TN(TN-0144): the chunks of one [`ChangesetOffsetCache`] set.
const CACHE_WAYS: usize = 8;

/// TN(TN-0144): the process-unique id of the next [`ChangesetOffsetReader`]; never reused, so a
/// new reader never meets the chunks of one it replaced.
static NEXT_READER_ID: AtomicU64 = AtomicU64::new(0);

/// TN(TN-0144): a fixed-size, set-associative cache of `.csoff` record chunks, shared by every
/// [`ChangesetOffsetReader`].
///
/// A chunk is keyed by its reader's id and its index, so a reader only ever sees chunks it read
/// itself: each reader is one jar snapshot of fixed length, and an append, prune, truncation,
/// heal or rotation loads a new reader, which starts with nothing cached. A key maps to one of the
/// sets; a full set evicts its least recently used chunk. A chunk keeps its records as stored,
/// checksums included, and every lookup verifies its record, whether the chunk was cached or just
/// read again after an eviction. The chunks of a dropped reader are never used again, so their
/// sets evict them first.
pub struct ChangesetOffsetCache {
    sets: Box<[Mutex<CacheSet>]>,
}

/// TN(TN-0144): the chunks of one set, at most [`CACHE_WAYS`].
#[derive(Default)]
struct CacheSet {
    chunks: Vec<CachedChunk>,
    /// The use count of the set, which stamps a chunk on every use.
    clock: u64,
}

/// TN(TN-0144): one chunk of one reader.
struct CachedChunk {
    reader: u64,
    chunk: u64,
    /// The set's clock at the chunk's last use.
    used: u64,
    /// The chunk's records, as stored.
    records: Box<[u8]>,
}

impl ChangesetOffsetCache {
    /// A cache of at most `bytes` of records (at least one set), in chunks of [`CHUNK_RECORDS`]
    /// checksummed records.
    pub fn new(bytes: usize) -> Self {
        let chunk_bytes = CHUNK_RECORDS as usize * CHECKSUMMED_RECORD_SIZE;
        let sets = (bytes / chunk_bytes / CACHE_WAYS).max(1);
        Self { sets: (0..sets).map(|_| Mutex::default()).collect() }
    }

    /// The process's cache of [`CHANGESET_OFFSET_CACHE_BYTES`], which
    /// [`ChangesetOffsetReader::new`] uses.
    pub fn shared() -> &'static Arc<Self> {
        static SHARED: OnceLock<Arc<ChangesetOffsetCache>> = OnceLock::new();
        SHARED.get_or_init(|| Arc::new(Self::new(CHANGESET_OFFSET_CACHE_BYTES)))
    }

    /// The most bytes of records the cache holds.
    pub fn capacity_bytes(&self) -> usize {
        self.sets.len() * CACHE_WAYS * CHUNK_RECORDS as usize * CHECKSUMMED_RECORD_SIZE
    }

    /// The bytes of records the cache holds now.
    pub fn cached_bytes(&self) -> usize {
        self.sets
            .iter()
            .map(|set| {
                Self::lock(set).chunks.iter().map(|chunk| chunk.records.len()).sum::<usize>()
            })
            .sum()
    }

    fn lock(set: &Mutex<CacheSet>) -> MutexGuard<'_, CacheSet> {
        // A set is consistent between any two of its statements, so a panic elsewhere while
        // holding it leaves nothing to repair.
        set.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set(&self, reader: u64, chunk: u64) -> &Mutex<CacheSet> {
        let hash = (reader.rotate_left(32) ^ chunk).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        &self.sets[((hash >> 32) % self.sets.len() as u64) as usize]
    }

    /// Copies the bytes at `at` of the reader's chunk into `record`, if the chunk is cached.
    fn get(&self, reader: u64, chunk: u64, at: usize, record: &mut [u8]) -> bool {
        let mut set = Self::lock(self.set(reader, chunk));
        set.clock += 1;
        let clock = set.clock;
        let Some(cached) =
            set.chunks.iter_mut().find(|cached| cached.reader == reader && cached.chunk == chunk)
        else {
            return false
        };
        cached.used = clock;
        record.copy_from_slice(&cached.records[at..at + record.len()]);
        true
    }

    /// Caches the reader's chunk, evicting its set's least recently used chunk when the set is
    /// full, and copies the bytes at `at` into `record`. A chunk another lookup cached meanwhile
    /// is kept and answers instead.
    fn insert(&self, reader: u64, chunk: u64, records: Box<[u8]>, at: usize, record: &mut [u8]) {
        let mut set = Self::lock(self.set(reader, chunk));
        set.clock += 1;
        let used = set.clock;
        let slot = match set
            .chunks
            .iter()
            .position(|cached| cached.reader == reader && cached.chunk == chunk)
        {
            Some(slot) => slot,
            None if set.chunks.len() < CACHE_WAYS => {
                set.chunks.push(CachedChunk { reader, chunk, used, records });
                set.chunks.len() - 1
            }
            None => {
                let (slot, _) =
                    set.chunks.iter().enumerate().min_by_key(|(_, cached)| cached.used).unwrap();
                set.chunks[slot] = CachedChunk { reader, chunk, used, records };
                slot
            }
        };
        let cached = &mut set.chunks[slot];
        cached.used = used;
        record.copy_from_slice(&cached.records[at..at + record.len()]);
    }

    #[cfg(test)]
    fn contains(&self, reader: u64, chunk: u64) -> bool {
        Self::lock(self.set(reader, chunk))
            .chunks
            .iter()
            .any(|cached| cached.reader == reader && cached.chunk == chunk)
    }
}

impl core::fmt::Debug for ChangesetOffsetCache {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChangesetOffsetCache").field("sets", &self.sets.len()).finish()
    }
}

/// Reader for changeset offsets with O(1) random access.
pub struct ChangesetOffsetReader {
    file: File,
    /// Cached file length in records.
    len: u64,
    /// TN(TN-0121): whether records carry checksums, which every read verifies.
    checksums: bool,
    /// The sidecar's path, for checksum errors.
    path: PathBuf,
    /// TN(TN-0144): the cache [`Self::get`] reads a record's whole chunk into on the chunk's first
    /// lookup, and answers the chunk's later lookups from while it stays cached, instead of one
    /// `pread` a lookup.
    cache: Arc<ChangesetOffsetCache>,
    /// TN(TN-0144): this reader's key in `cache`.
    id: u64,
    /// TN(TN-0144): the records the chunks cover: those below `len` that the file held at the
    /// open; the rest are read one at a time.
    chunked_len: u64,
}

impl ChangesetOffsetReader {
    /// Opens the changeset offset file for reading with an explicit length.
    ///
    /// The `len` parameter (from header metadata) bounds the reader - any records
    /// beyond this length are ignored. This ensures we only read committed data.
    ///
    /// TN(TN-0121): `checksums` is the jar's [`changeset_offset_record_size`] format.
    /// TN(TN-0144): lookups go through [`ChangesetOffsetCache::shared`].
    pub fn new(path: impl AsRef<Path>, len: u64, checksums: bool) -> io::Result<Self> {
        Self::with_cache(path, len, checksums, Arc::clone(ChangesetOffsetCache::shared()))
    }

    /// TN(TN-0144): [`Self::new`], with its lookups through `cache`.
    pub fn with_cache(
        path: impl AsRef<Path>,
        len: u64,
        checksums: bool,
        cache: Arc<ChangesetOffsetCache>,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        // TN(TN-0144): chunks cover only the records the file holds below `len`, so a `len` past
        // the file (a damaged header) caches nothing past it.
        let file_records = file.metadata()?.len() / changeset_offset_record_size(checksums);
        let chunked_len = len.min(file_records);
        let id = NEXT_READER_ID.fetch_add(1, Ordering::Relaxed);
        Ok(Self { file, len, checksums, path: path.to_path_buf(), cache, id, chunked_len })
    }

    /// Reads a single changeset offset by block index.
    /// Returns None if index is out of bounds.
    ///
    /// TN(TN-0144): from the cached copy of the record's chunk, read into the cache on a miss. The
    /// record's checksum is verified on every lookup, as when each lookup read it from the file. A
    /// chunk that cannot be read is not cached, and the record is then read alone.
    pub fn get(&self, block_index: u64) -> io::Result<Option<ChangesetOffset>> {
        if block_index >= self.len {
            return Ok(None);
        }

        let record_size = changeset_offset_record_size(self.checksums) as usize;
        let mut buf = [0u8; CHECKSUMMED_RECORD_SIZE];
        if block_index < self.chunked_len {
            let chunk = block_index / CHUNK_RECORDS;
            let at = (block_index % CHUNK_RECORDS) as usize * record_size;
            let record = &mut buf[..record_size];
            let found = self.cache.get(self.id, chunk, at, record) ||
                self.read_chunk(chunk)
                    .map(|records| self.cache.insert(self.id, chunk, records, at, record))
                    .is_ok();
            if found {
                return decode_record(&buf, block_index, self.checksums, &self.path).map(Some);
            }
        }

        let byte_pos = block_index * record_size as u64;
        self.file.read_exact_at(&mut buf[..record_size], byte_pos)?;

        decode_record(&buf, block_index, self.checksums, &self.path).map(Some)
    }

    /// TN(TN-0144): the stored records of chunk `chunk`, up to [`CHUNK_RECORDS`] of them, in one
    /// read; the last chunk ends at `chunked_len`.
    fn read_chunk(&self, chunk: u64) -> io::Result<Box<[u8]>> {
        let record_size = changeset_offset_record_size(self.checksums);
        let start = chunk * CHUNK_RECORDS;
        let records = (self.chunked_len - start).min(CHUNK_RECORDS);
        let mut buf = vec![0u8; (records * record_size) as usize].into_boxed_slice();
        self.file.read_exact_at(&mut buf, start * record_size)?;
        Ok(buf)
    }

    /// Reads a range of changeset offsets.
    pub fn get_range(&self, start: u64, end: u64) -> io::Result<Vec<ChangesetOffset>> {
        let end = end.min(self.len);
        if start >= end {
            return Ok(Vec::new());
        }

        let count = (end - start) as usize;
        let record_size = changeset_offset_record_size(self.checksums) as usize;

        // TN(TN-0121): whole chunks of records a read, not one `pread` a record: the open-time
        // heal reads every committed record (up to one a block, 500k a file).
        const CHUNK_RECORDS: usize = 4096;
        // TN(TN-0134): reserve no more than the file holds; a `len` past it (a damaged header)
        // ends in the read error below, not in an allocation sized by the header.
        let file_records = self.file.metadata()?.len() / record_size as u64;
        let mut result = Vec::with_capacity(count.min(file_records as usize));
        let mut buf = vec![0u8; count.min(CHUNK_RECORDS) * record_size];
        let mut index = start;
        while index < end {
            let records = ((end - index) as usize).min(CHUNK_RECORDS);
            let chunk = &mut buf[..records * record_size];
            self.file.read_exact_at(chunk, index * record_size as u64)?;
            for record in chunk.chunks_exact(record_size) {
                result.push(decode_record(record, index, self.checksums, &self.path)?);
                index += 1;
            }
        }

        Ok(result)
    }

    /// Returns the number of valid records.
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Returns true if there are no records.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// TN(TN-0144): the cache by its sets, not by its chunks.
impl core::fmt::Debug for ChangesetOffsetReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChangesetOffsetReader")
            .field("file", &self.file)
            .field("len", &self.len)
            .field("checksums", &self.checksums)
            .field("path", &self.path)
            .field("cache", &self.cache)
            .field("id", &self.id)
            .field("chunked_len", &self.chunked_len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// TN(TN-0134): the record seed's high-half term separates indexes 2^32 apart.
    #[test]
    fn test_record_seed_high_half() {
        for index in [0, 1, 499_999, u32::MAX as u64] {
            assert_ne!(record_seed(index), record_seed(index + (1 << 32)));
            assert_ne!(record_seed(index), record_seed(index + (1 << 48)));
        }
    }

    /// TN(TN-0144): lookups answer from the in-memory chunks exactly as from the file, across
    /// chunk boundaries and a short last chunk, in both formats; a record damaged before its chunk
    /// is read fails its checksum on every lookup, the chunk's other records still answer; a
    /// `len` past the file answers the records the file holds and fails past them.
    #[test]
    fn test_chunked_lookups_match_the_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let records = 2 * CHUNK_RECORDS + 7;
        let expected: Vec<_> = (0..records).map(|i| ChangesetOffset::new(i * 3, i % 5)).collect();
        for checksums in [false, true] {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
            writer.append_many(&expected).unwrap();
            writer.sync().unwrap();
            drop(writer);

            let cache = Arc::new(ChangesetOffsetCache::new(1 << 20));
            let reader =
                ChangesetOffsetReader::with_cache(&path, records, checksums, Arc::clone(&cache))
                    .unwrap();
            for _ in 0..2 {
                for (i, offset) in expected.iter().enumerate().rev() {
                    assert_eq!(reader.get(i as u64).unwrap().as_ref(), Some(offset), "record {i}");
                }
            }
            assert!((0..3).all(|chunk| cache.contains(reader.id, chunk)));
            assert!(!cache.contains(reader.id, 3));
            assert_eq!(reader.get(records).unwrap(), None);

            // A damaged record, before its chunk is read.
            let damaged = CHUNK_RECORDS + 3;
            let record_size = changeset_offset_record_size(checksums);
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            file.write_all_at(&[0xff], damaged * record_size + 1).unwrap();
            let reader = ChangesetOffsetReader::new(&path, records, checksums).unwrap();
            for _ in 0..2 {
                let got = reader.get(damaged);
                if checksums {
                    let err = got.unwrap_err();
                    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
                } else {
                    assert_ne!(got.unwrap().as_ref(), Some(&expected[damaged as usize]));
                }
                assert_eq!(
                    reader.get(damaged + 1).unwrap().as_ref(),
                    Some(&expected[damaged as usize + 1])
                );
            }

            // A length past the file.
            let reader = ChangesetOffsetReader::new(&path, records + 10, checksums).unwrap();
            assert_eq!(
                reader.get(records - 1).unwrap().as_ref(),
                Some(&expected[records as usize - 1])
            );
            let err = reader.get(records).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");
            std::fs::remove_file(&path).unwrap();
        }
    }

    /// TN(TN-0144): many distinct lookups over three files (both formats, as both changeset
    /// segments' jars) keep the cache within its bound, whose capacity is at most the bytes asked
    /// for, while every lookup answers its own file's record; the shared cache is within
    /// [`CHANGESET_OFFSET_CACHE_BYTES`].
    #[test]
    fn test_cache_stays_within_its_bound() {
        assert!(ChangesetOffsetCache::shared().capacity_bytes() <= CHANGESET_OFFSET_CACHE_BYTES);
        let dir = tempdir().unwrap();
        let records = 200 * CHUNK_RECORDS;
        // Each file its own records, so a lookup answered from the other's chunk fails.
        let expected = |file: u64| -> Vec<_> {
            (0..records).map(|i| ChangesetOffset::new(i * 3 + file, i % 5)).collect()
        };
        let bytes = 4 * CACHE_WAYS * CHUNK_RECORDS as usize * CHECKSUMMED_RECORD_SIZE;
        let cache = Arc::new(ChangesetOffsetCache::new(bytes + 100));
        assert_eq!(cache.capacity_bytes(), bytes);
        let readers: Vec<_> = [(0, false), (1, true), (2, true)]
            .into_iter()
            .map(|(file, checksums)| {
                let path = dir.path().join(format!("{file}.csoff"));
                let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
                let expected = expected(file);
                writer.append_many(&expected).unwrap();
                writer.sync().unwrap();
                let reader = ChangesetOffsetReader::with_cache(
                    &path,
                    records,
                    checksums,
                    Arc::clone(&cache),
                )
                .unwrap();
                (reader, expected)
            })
            .collect();
        let mut most = 0;
        for step in [1, 7, 61] {
            for i in (0..records).step_by(step) {
                for (reader, expected) in &readers {
                    assert_eq!(reader.get(i).unwrap().as_ref(), Some(&expected[i as usize]));
                }
                most = most.max(cache.cached_bytes());
            }
        }
        assert!(most <= cache.capacity_bytes(), "{most} > {}", cache.capacity_bytes());
        assert!(most > cache.capacity_bytes() / 2, "{most}");
    }

    /// TN(TN-0144): a lookup in a chunk the cache evicted reads the chunk again and verifies its
    /// record: a record damaged while its chunk was cached answers from the cache, and after the
    /// eviction fails its checksum (or, without checksums, answers what the file now holds),
    /// while the chunk's other records answer. A later reader of the file reads it from the file,
    /// not from the earlier reader's chunk in the same set.
    #[test]
    fn test_lookup_after_eviction_rereads_and_verifies() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let records = (CACHE_WAYS as u64 + 2) * CHUNK_RECORDS;
        let expected: Vec<_> = (0..records).map(|i| ChangesetOffset::new(i * 3, i % 5)).collect();
        for checksums in [false, true] {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
            writer.append_many(&expected).unwrap();
            writer.sync().unwrap();
            drop(writer);
            // One set: the chunks of `CACHE_WAYS` other lookups evict the first.
            let cache = Arc::new(ChangesetOffsetCache::new(0));
            let reader =
                ChangesetOffsetReader::with_cache(&path, records, checksums, Arc::clone(&cache))
                    .unwrap();
            assert_eq!(reader.get(0).unwrap().as_ref(), Some(&expected[0]));

            let damaged = 5;
            // The damaged record as read from the file: a checksum error, or without checksums
            // its changed offset.
            let assert_damaged = |got: io::Result<Option<ChangesetOffset>>| {
                if checksums {
                    let err = got.unwrap_err();
                    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
                    let mismatch = err
                        .get_ref()
                        .and_then(|inner| inner.downcast_ref::<ChangesetOffsetChecksumMismatch>())
                        .unwrap();
                    assert_eq!(mismatch.index, damaged);
                } else {
                    let got = got.unwrap().unwrap();
                    assert_ne!(got, expected[damaged as usize]);
                    assert_eq!(got.num_changes(), expected[damaged as usize].num_changes());
                }
            };
            let record_size = changeset_offset_record_size(checksums);
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            file.write_all_at(&[0xff], damaged * record_size + 1).unwrap();
            assert_eq!(reader.get(damaged).unwrap().as_ref(), Some(&expected[damaged as usize]));
            // A later snapshot of the file, in the same set, never meets the first's chunk.
            let later =
                ChangesetOffsetReader::with_cache(&path, records, checksums, Arc::clone(&cache))
                    .unwrap();
            assert_damaged(later.get(damaged));
            assert_eq!(reader.get(damaged).unwrap().as_ref(), Some(&expected[damaged as usize]));
            drop(later);

            for chunk in 1..=CACHE_WAYS as u64 {
                let i = chunk * CHUNK_RECORDS;
                assert_eq!(reader.get(i).unwrap().as_ref(), Some(&expected[i as usize]));
            }
            assert!(!cache.contains(reader.id, 0));
            for _ in 0..2 {
                assert_damaged(reader.get(damaged));
                assert_eq!(reader.get(0).unwrap().as_ref(), Some(&expected[0]));
            }
            assert!(cache.contains(reader.id, 0));
            std::fs::remove_file(&path).unwrap();
        }
    }

    /// TN(TN-0134): a range read whose length (from a damaged header) runs past the file is a read
    /// error, after reserving only what the file holds, not an allocation sized by the length.
    #[test]
    fn test_get_range_past_the_file_is_an_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        for checksums in [false, true] {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
            writer.append_many(&[ChangesetOffset::new(0, 1), ChangesetOffset::new(1, 1)]).unwrap();
            writer.sync().unwrap();
            drop(writer);
            let reader = ChangesetOffsetReader::new(&path, 1 << 60, checksums).unwrap();
            let err = reader.get_range(0, 1 << 60).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");
            std::fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        // Write (new file, committed_len=0)
        {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, false).unwrap();
            writer.append(&ChangesetOffset::new(0, 5)).unwrap();
            writer.append(&ChangesetOffset::new(5, 3)).unwrap();
            writer.append(&ChangesetOffset::new(8, 10)).unwrap();
            writer.sync().unwrap();
            assert_eq!(writer.len(), 3);
        }

        // Read
        {
            let reader = ChangesetOffsetReader::new(&path, 3, false).unwrap();
            assert_eq!(reader.len(), 3);

            let entry = reader.get(0).unwrap().unwrap();
            assert_eq!(entry.offset(), 0);
            assert_eq!(entry.num_changes(), 5);

            let entry = reader.get(1).unwrap().unwrap();
            assert_eq!(entry.offset(), 5);
            assert_eq!(entry.num_changes(), 3);

            let entry = reader.get(2).unwrap().unwrap();
            assert_eq!(entry.offset(), 8);
            assert_eq!(entry.num_changes(), 10);

            assert!(reader.get(3).unwrap().is_none());
        }
    }

    #[test]
    fn test_truncate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        let mut writer = ChangesetOffsetWriter::new(&path, 0, false).unwrap();
        writer.append(&ChangesetOffset::new(0, 1)).unwrap();
        writer.append(&ChangesetOffset::new(1, 2)).unwrap();
        writer.append(&ChangesetOffset::new(3, 3)).unwrap();
        writer.sync().unwrap();

        writer.truncate(2).unwrap();
        assert_eq!(writer.len(), 2);

        let reader = ChangesetOffsetReader::new(&path, 2, false).unwrap();
        assert_eq!(reader.len(), 2);
        assert!(reader.get(2).unwrap().is_none());
    }

    #[test]
    fn test_partial_record_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        // Write 1 full record (16 bytes) + 8 trailing bytes (partial record)
        {
            let mut file = std::fs::File::create(&path).unwrap();
            // Full record: offset=100, num_changes=5
            file.write_all(&100u64.to_le_bytes()).unwrap();
            file.write_all(&5u64.to_le_bytes()).unwrap();
            // Partial record: only 8 bytes (incomplete)
            file.write_all(&200u64.to_le_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        // Verify file has 24 bytes before opening with writer
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 24);

        // Open with writer, committed_len=1 (header committed 1 record)
        // Should truncate partial record and match committed length
        let writer = ChangesetOffsetWriter::new(&path, 1, false).unwrap();
        assert_eq!(writer.len(), 1);

        // Verify file was truncated to 16 bytes
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 16);

        // Verify the complete record is readable
        let reader = ChangesetOffsetReader::new(&path, 1, false).unwrap();
        assert_eq!(reader.len(), 1);
        let entry = reader.get(0).unwrap().unwrap();
        assert_eq!(entry.offset(), 100);
        assert_eq!(entry.num_changes(), 5);
    }

    #[test]
    fn test_len_bounds_reads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        // Write 3 records
        {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, false).unwrap();
            writer.append(&ChangesetOffset::new(0, 10)).unwrap();
            writer.append(&ChangesetOffset::new(10, 20)).unwrap();
            writer.append(&ChangesetOffset::new(30, 30)).unwrap();
            writer.sync().unwrap();
            assert_eq!(writer.len(), 3);
        }

        // Open with len=2, ignoring the 3rd record
        let reader = ChangesetOffsetReader::new(&path, 2, false).unwrap();
        assert_eq!(reader.len(), 2);

        // First two records should be readable
        let entry0 = reader.get(0).unwrap().unwrap();
        assert_eq!(entry0.offset(), 0);
        assert_eq!(entry0.num_changes(), 10);

        let entry1 = reader.get(1).unwrap().unwrap();
        assert_eq!(entry1.offset(), 10);
        assert_eq!(entry1.num_changes(), 20);

        // Third record should be out of bounds (due to len=2)
        assert!(reader.get(2).unwrap().is_none());

        // get_range should also respect the len bound
        let range = reader.get_range(0, 5).unwrap();
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn test_truncate_uncommitted_records_on_open() {
        // Simulates crash recovery where sidecar has more records than committed header length.
        // ChangesetOffsetWriter::new() should automatically truncate to committed_len.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        // Simulate: wrote 3 records, synced sidecar, but header only committed len=2
        {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, false).unwrap();
            writer.append(&ChangesetOffset::new(0, 5)).unwrap();
            writer.append(&ChangesetOffset::new(5, 10)).unwrap();
            writer.append(&ChangesetOffset::new(15, 7)).unwrap(); // uncommitted
            writer.sync().unwrap();
            assert_eq!(writer.len(), 3);
        }

        // On "restart", new() heals by truncating to committed length
        let committed_len = 2u64;
        {
            let writer = ChangesetOffsetWriter::new(&path, committed_len, false).unwrap();
            assert_eq!(writer.len(), 2); // Healed to committed length
        }

        // Verify file is now correct length and new appends go to the right place
        {
            let mut writer = ChangesetOffsetWriter::new(&path, 2, false).unwrap();
            assert_eq!(writer.len(), 2);

            // Append a new record - should be at index 2, not index 3
            writer.append(&ChangesetOffset::new(15, 20)).unwrap();
            writer.sync().unwrap();
            assert_eq!(writer.len(), 3);
        }

        // Verify the records are correct
        {
            let reader = ChangesetOffsetReader::new(&path, 3, false).unwrap();
            assert_eq!(reader.len(), 3);

            let entry0 = reader.get(0).unwrap().unwrap();
            assert_eq!(entry0.offset(), 0);
            assert_eq!(entry0.num_changes(), 5);

            let entry1 = reader.get(1).unwrap().unwrap();
            assert_eq!(entry1.offset(), 5);
            assert_eq!(entry1.num_changes(), 10);

            // This should be the NEW record, not the old uncommitted one
            let entry2 = reader.get(2).unwrap().unwrap();
            assert_eq!(entry2.offset(), 15);
            assert_eq!(entry2.num_changes(), 20); // Not 7 from the old uncommitted record
        }
    }

    #[test]
    fn test_sidecar_shorter_than_committed_errors() {
        // If sidecar has fewer records than committed, it's data corruption - should error.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");

        // Write 1 record
        {
            let mut writer = ChangesetOffsetWriter::new(&path, 0, false).unwrap();
            writer.append(&ChangesetOffset::new(0, 5)).unwrap();
            writer.sync().unwrap();
        }

        // Try to open with committed_len=3 (header claims more than file has)
        let result = ChangesetOffsetWriter::new(&path, 3, false);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Writes `offsets` to a new checksummed sidecar at `path`.
    fn write_checksummed(path: &Path, offsets: &[ChangesetOffset]) {
        let mut writer = ChangesetOffsetWriter::new(path, 0, true).unwrap();
        writer.append_many(offsets).unwrap();
        writer.sync().unwrap();
        assert_eq!(writer.len(), offsets.len() as u64);
    }

    /// Asserts that `result` is the checksum error for record `index` of `path`.
    fn assert_mismatch<T: core::fmt::Debug>(result: io::Result<T>, path: &Path, index: u64) {
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let inner = err.get_ref().and_then(|e| e.downcast_ref::<ChangesetOffsetChecksumMismatch>());
        assert_eq!(
            inner,
            Some(&ChangesetOffsetChecksumMismatch { path: path.to_path_buf(), index }),
            "{err}"
        );
    }

    /// TN(TN-0121): a checksummed sidecar has 20-byte records that read back as written, and its
    /// open-time heal works in whole 20-byte records: a partial record and records past the
    /// committed length are cut, and appends after the heal land at the right index.
    #[test]
    fn test_checksummed_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let offsets =
            [ChangesetOffset::new(0, 5), ChangesetOffset::new(5, 0), ChangesetOffset::new(5, 9)];
        write_checksummed(&path, &offsets);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 60);
        assert_eq!(changeset_offset_record_size(true), 20);
        assert_eq!(changeset_offset_record_size(false), 16);

        let reader = ChangesetOffsetReader::new(&path, 3, true).unwrap();
        for (index, expected) in offsets.iter().enumerate() {
            assert_eq!(reader.get(index as u64).unwrap().as_ref(), Some(expected));
        }
        assert!(reader.get(3).unwrap().is_none());
        assert_eq!(reader.get_range(0, 5).unwrap(), offsets);
        assert_eq!(reader.get_range(1, 3).unwrap(), offsets[1..]);

        // A partial record after the committed two: the heal cuts both.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[7; 9]).unwrap();
        }
        let mut writer = ChangesetOffsetWriter::new(&path, 2, true).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 40);
        writer.append(&ChangesetOffset::new(5, 2)).unwrap();
        writer.sync().unwrap();
        let reader = ChangesetOffsetReader::new(&path, 3, true).unwrap();
        assert_eq!(reader.get(2).unwrap(), Some(ChangesetOffset::new(5, 2)));
        assert_eq!(reader.get_range(0, 3).unwrap().len(), 3);

        writer.truncate(1).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 20);
    }

    /// TN(TN-0121): a changed byte anywhere in a checksummed record, including its checksum,
    /// fails reads of that record with an error naming the file and record, single and range
    /// reads alike, while the other records still read.
    #[test]
    fn test_checksummed_record_damage_is_an_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let offsets: Vec<_> = (0..4).map(|i| ChangesetOffset::new(i * 10, 10)).collect();
        write_checksummed(&path, &offsets);
        let clean = std::fs::read(&path).unwrap();

        for pos in 20..40 {
            let mut damaged = clean.clone();
            damaged[pos] ^= 0x04;
            std::fs::write(&path, &damaged).unwrap();
            let reader = ChangesetOffsetReader::new(&path, 4, true).unwrap();
            assert_mismatch(reader.get(1), &path, 1);
            assert_mismatch(reader.get_range(0, 4), &path, 1);
            for index in [0, 2, 3] {
                assert_eq!(reader.get(index).unwrap(), Some(offsets[index as usize].clone()));
            }
            assert_eq!(reader.get_range(2, 4).unwrap(), offsets[2..]);
        }
    }

    /// TN(TN-0121): range reads go by whole chunks of records; across chunk boundaries they read
    /// every record at its own index, and a damaged record past the first chunk fails the range.
    #[test]
    fn test_range_reads_across_chunks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let offsets: Vec<_> = (0..10_000).map(|i| ChangesetOffset::new(i * 3, 3)).collect();
        for checksums in [false, true] {
            {
                let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
                writer.truncate(0).unwrap();
                writer.append_many(&offsets).unwrap();
                writer.sync().unwrap();
            }
            let reader = ChangesetOffsetReader::new(&path, 10_000, checksums).unwrap();
            assert_eq!(reader.get_range(0, 10_000).unwrap(), offsets);
            assert_eq!(reader.get_range(4095, 8193).unwrap(), offsets[4095..8193]);
            assert_eq!(reader.get(9_000).unwrap(), Some(offsets[9_000].clone()));
        }

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[9_000 * 20 + 3] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let reader = ChangesetOffsetReader::new(&path, 10_000, true).unwrap();
        assert_mismatch(reader.get_range(0, 10_000), &path, 9_000);
        assert_eq!(reader.get_range(0, 9_000).unwrap(), offsets[..9_000]);
    }

    /// TN(TN-0121): a record moved to another block's index fails, with its own valid checksum:
    /// the index binds it. A plain sidecar returns the moved record as the block's.
    #[test]
    fn test_checksummed_record_bound_to_its_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csoff");
        let offsets: Vec<_> = (0..3).map(|i| ChangesetOffset::new(i * 10, 10)).collect();

        for checksums in [true, false] {
            let size = changeset_offset_record_size(checksums) as usize;
            {
                let mut writer = ChangesetOffsetWriter::new(&path, 0, checksums).unwrap();
                writer.truncate(0).unwrap();
                writer.append_many(&offsets).unwrap();
                writer.sync().unwrap();
            }
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.copy_within(0..size, 2 * size);
            std::fs::write(&path, &bytes).unwrap();

            let reader = ChangesetOffsetReader::new(&path, 3, checksums).unwrap();
            if checksums {
                assert_mismatch(reader.get(2), &path, 2);
            } else {
                assert_eq!(reader.get(2).unwrap(), Some(offsets[0].clone()));
            }
        }
    }
}
