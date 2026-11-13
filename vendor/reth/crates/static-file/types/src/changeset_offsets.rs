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
    sync::OnceLock,
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

/// TN(TN-0144): the records of one chunk of [`ChangesetOffsetReader`]'s in-memory copy.
const CHUNK_RECORDS: u64 = 256;

/// Reader for changeset offsets with O(1) random access.
pub struct ChangesetOffsetReader {
    file: File,
    /// Cached file length in records.
    len: u64,
    /// TN(TN-0121): whether records carry checksums, which every read verifies.
    checksums: bool,
    /// The sidecar's path, for checksum errors.
    path: PathBuf,
    /// TN(TN-0144): the records read so far, as stored (checksums included), one slot a chunk of
    /// [`CHUNK_RECORDS`]. [`Self::get`] reads a record's whole chunk on its first lookup and
    /// answers the chunk's later lookups from memory, instead of one `pread` a lookup. It covers
    /// the records below `len` that the file held at the open; the rest are read one at a time.
    chunks: Box<[OnceLock<Box<[u8]>>]>,
    /// TN(TN-0144): the records `chunks` covers.
    chunked_len: u64,
}

impl ChangesetOffsetReader {
    /// Opens the changeset offset file for reading with an explicit length.
    ///
    /// The `len` parameter (from header metadata) bounds the reader - any records
    /// beyond this length are ignored. This ensures we only read committed data.
    ///
    /// TN(TN-0121): `checksums` is the jar's [`changeset_offset_record_size`] format.
    pub fn new(path: impl AsRef<Path>, len: u64, checksums: bool) -> io::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        // TN(TN-0144): one chunk slot for every chunk of records the file holds below `len`, so
        // a `len` past the file (a damaged header) sizes nothing by itself.
        let file_records = file.metadata()?.len() / changeset_offset_record_size(checksums);
        let chunked_len = len.min(file_records);
        let chunks = (0..chunked_len.div_ceil(CHUNK_RECORDS)).map(|_| OnceLock::new()).collect();
        Ok(Self { file, len, checksums, path: path.to_path_buf(), chunks, chunked_len })
    }

    /// Reads a single changeset offset by block index.
    /// Returns None if index is out of bounds.
    ///
    /// TN(TN-0144): from the in-memory copy of the record's chunk, read on the chunk's first
    /// lookup. The record's checksum is verified on every lookup, as when each lookup read it from
    /// the file. A chunk that cannot be read is not kept, and the record is then read alone.
    pub fn get(&self, block_index: u64) -> io::Result<Option<ChangesetOffset>> {
        if block_index >= self.len {
            return Ok(None);
        }

        let record_size = changeset_offset_record_size(self.checksums) as usize;
        if block_index < self.chunked_len {
            let slot = &self.chunks[(block_index / CHUNK_RECORDS) as usize];
            let chunk = match slot.get() {
                Some(chunk) => Some(chunk),
                None => self
                    .read_chunk(block_index / CHUNK_RECORDS)
                    .ok()
                    .map(|chunk| slot.get_or_init(|| chunk)),
            };
            if let Some(chunk) = chunk {
                let at = (block_index % CHUNK_RECORDS) as usize * record_size;
                let record = &chunk[at..at + record_size];
                return decode_record(record, block_index, self.checksums, &self.path).map(Some);
            }
        }

        let byte_pos = block_index * record_size as u64;
        let mut buf = [0u8; CHECKSUMMED_RECORD_SIZE];
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

/// TN(TN-0144): the in-memory chunks by count, not by their bytes.
impl core::fmt::Debug for ChangesetOffsetReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChangesetOffsetReader")
            .field("file", &self.file)
            .field("len", &self.len)
            .field("checksums", &self.checksums)
            .field("path", &self.path)
            .field("chunked_len", &self.chunked_len)
            .field("chunks_read", &self.chunks.iter().filter(|chunk| chunk.get().is_some()).count())
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

            let reader = ChangesetOffsetReader::new(&path, records, checksums).unwrap();
            assert_eq!(reader.chunks.len(), 3);
            for _ in 0..2 {
                for (i, offset) in expected.iter().enumerate().rev() {
                    assert_eq!(reader.get(i as u64).unwrap().as_ref(), Some(offset), "record {i}");
                }
            }
            assert!(reader.chunks.iter().all(|chunk| chunk.get().is_some()));
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
