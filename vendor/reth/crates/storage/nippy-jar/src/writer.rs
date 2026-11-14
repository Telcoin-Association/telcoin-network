use crate::{
    compression::{Compression, Compressors},
    value_checksum, ColumnResult, NippyJar, NippyJarChecker, NippyJarError, NippyJarHeader,
    CHECKSUM_SIZE,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
};
use zstd::{bulk::Compressor, zstd_safe};

/// Size of one offset in bytes.
pub(crate) const OFFSET_SIZE_BYTES: u8 = 8;

/// Writer of [`NippyJar`]. Handles table data and offsets only.
///
/// Table data is written directly to disk, while offsets and configuration need to be flushed by
/// calling `commit()`.
///
/// ## Offset file layout
/// The first byte is the size of a single offset in bytes, `m`.
/// Then, the file contains `n` entries, each with a size of `m`. Each entry represents an offset,
/// except for the last entry, which represents both the total size of the data file, as well as the
/// next offset to write new data to.
///
/// ## Data file layout
/// The data file is represented just as a sequence of bytes of data without any delimiters
///
/// TN(TN-0120): in a checksummed jar ([`NippyJar::with_checksums`]) each value is followed by its
/// [`value_checksum`], and the offsets count those bytes as part of the value.
#[derive(Debug)]
pub struct NippyJarWriter<H: NippyJarHeader = ()> {
    /// Associated [`NippyJar`], containing all necessary configurations for data
    /// handling.
    jar: NippyJar<H>,
    /// File handle to where the data is stored.
    data_file: BufWriter<File>,
    /// File handle to where the offsets are stored.
    offsets_file: BufWriter<File>,
    /// Temporary buffer to reuse when compressing data.
    tmp_buf: Vec<u8>,
    /// TN(TN-0146): the compressors of a dictionary zstd jar, kept for the writer's life.
    dictionary_compressors: Option<DictionaryCompressors>,
    /// Used to find the maximum uncompressed size of a row in a jar.
    uncompressed_row_size: usize,
    /// Partial offset list which hasn't been flushed to disk.
    offsets: Vec<u64>,
    /// Column where writer is going to write next.
    column: usize,
    /// Whether the writer has changed data that needs to be committed.
    dirty: bool,
    /// TN(TN-0161): set when a sync, an offsets write or a prune failed. Its files and its rows
    /// and offsets in memory may then disagree (a failed offsets write has drained the offsets it
    /// did not write, a failed prune has cut its rows in memory only, and a retried `fsync` can
    /// report success for pages the failed one lost), so it writes no configuration again:
    /// [`Self::finalize`] and [`Self::prune_rows`] refuse. A new writer of the jar heals its files
    /// back to the last configuration.
    failed: bool,
}

impl<H: NippyJarHeader> NippyJarWriter<H> {
    /// Creates a [`NippyJarWriter`] from [`NippyJar`].
    ///
    /// It will **always** attempt to heal any inconsistent state when called.
    pub fn new(jar: NippyJar<H>) -> Result<Self, NippyJarError> {
        // TN(TN-0149): a compressor `load` refuses is refused before any file is created.
        jar.check_compression()?;
        // TN(TN-0132): a jar whose configuration counts rows has no file to create: one missing
        // is damage, not a new jar.
        if jar.rows > 0 {
            for path in [jar.data_path().to_path_buf(), jar.offsets_path()] {
                if !path.exists() {
                    return Err(NippyJarError::MissingFile(path))
                }
            }
        }

        let (data_file, offsets_file, is_created) = Self::create_or_open_files(&jar)?;

        let (jar, data_file, offsets_file) = if is_created {
            (jar, BufWriter::new(data_file), BufWriter::new(offsets_file))
        } else {
            // If we are opening a previously created jar, we need to check its consistency, and
            // make changes if necessary.
            let mut checker = NippyJarChecker::new(jar);
            checker.ensure_consistency()?;

            let NippyJarChecker { jar, data_file, offsets_file } = checker;

            // Calling ensure_consistency, will fill data_file and offsets_file
            (jar, data_file.expect("qed"), offsets_file.expect("qed"))
        };

        let dictionary_compressors = DictionaryCompressors::new(&jar)?;
        let mut writer = Self {
            jar,
            data_file,
            offsets_file,
            tmp_buf: Vec::with_capacity(1_000_000),
            dictionary_compressors,
            uncompressed_row_size: 0,
            offsets: Vec::with_capacity(1_000_000),
            column: 0,
            dirty: false,
            failed: false,
        };

        if !is_created {
            // Commit any potential heals done above.
            writer.commit()?;
        }

        Ok(writer)
    }

    /// Returns a reference to `H` of [`NippyJar`]
    pub const fn user_header(&self) -> &H {
        &self.jar.user_header
    }

    /// Returns a mutable reference to `H` of [`NippyJar`].
    ///
    /// Since there's no way of knowing if `H` has been actually changed, this sets `self.dirty` to
    /// true.
    pub const fn user_header_mut(&mut self) -> &mut H {
        self.dirty = true;
        &mut self.jar.user_header
    }

    /// Returns whether there are changes that need to be committed.
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Sets writer as dirty.
    pub const fn set_dirty(&mut self) {
        self.dirty = true
    }

    /// Gets total writer rows in jar.
    pub const fn rows(&self) -> usize {
        self.jar.rows()
    }

    /// Whether the jar's values, configuration and changeset offsets carry checksums
    /// ([`NippyJar::has_checksums`]).
    pub const fn has_checksums(&self) -> bool {
        self.jar.has_checksums()
    }

    /// Consumes the writer and returns the associated [`NippyJar`].
    pub fn into_jar(self) -> NippyJar<H> {
        self.jar
    }

    fn create_or_open_files(jar: &NippyJar<H>) -> Result<(File, File, bool), NippyJarError> {
        let (data, offsets) = (jar.data_path(), &jar.offsets_path());
        let is_created = !data.exists() || !offsets.exists();

        // TN(TN-0150): a new jar's configuration (no rows) is written before its files exist.
        // Jars are found by their data file (`iter_static_files`), which loads the configuration
        // beside it, so a creation stopped at any point leaves a configuration no listing sees, or
        // a jar without rows, which the next open resets (`NippyJarChecker`). Written after the
        // files, a stop in between left a data file with no configuration, which no start loads.
        if is_created {
            jar.freeze_config()?;
        }

        if !data.exists() {
            // File::create is write-only (no reading possible)
            File::create(data)?;
        }

        let mut data_file = OpenOptions::new().read(true).write(true).open(data)?;
        data_file.seek(SeekFrom::End(0))?;

        if !offsets.exists() {
            // File::create is write-only (no reading possible)
            File::create(offsets)?;
        }

        let mut offsets_file = OpenOptions::new().read(true).write(true).open(offsets)?;
        if is_created {
            // TN(TN-0132): whichever file was not missing is left over from a deletion that
            // stopped part way (`NippyJar::delete`) and holds nothing of this jar.
            data_file.set_len(0)?;
            data_file.seek(SeekFrom::Start(0))?;
            offsets_file.set_len(0)?;

            let mut buf = Vec::with_capacity(1 + OFFSET_SIZE_BYTES as usize);

            // First byte of the offset file is the size of one offset in bytes
            buf.write_all(&[OFFSET_SIZE_BYTES])?;

            // The last offset should always represent the data file len, which is 0 on
            // creation.
            buf.write_all(&[0; OFFSET_SIZE_BYTES as usize])?;

            offsets_file.write_all(&buf)?;
            offsets_file.seek(SeekFrom::End(0))?;

            // TN(TN-0150): the new files' directory entries are durable before the writer
            // returns, as the configuration's directory sync made them when it came last.
            if let Some(dir) = data.parent() {
                reth_fs_util::fsync_file(&File::open(dir)?)?;
            }
        }

        Ok((data_file, offsets_file, is_created))
    }

    /// Appends rows to data file.  `fn commit()` should be called to flush offsets and config to
    /// disk.
    ///
    /// `column_values_per_row`: A vector where each element is a column's values in sequence,
    /// corresponding to each row. The vector's length equals the number of columns.
    pub fn append_rows(
        &mut self,
        column_values_per_row: Vec<impl IntoIterator<Item = ColumnResult<impl AsRef<[u8]>>>>,
        num_rows: u64,
    ) -> Result<(), NippyJarError> {
        let mut column_iterators = column_values_per_row
            .into_iter()
            .map(|v| v.into_iter())
            .collect::<Vec<_>>()
            .into_iter();

        for _ in 0..num_rows {
            let mut iterators = Vec::with_capacity(self.jar.columns);

            for mut column_iter in column_iterators {
                self.append_column(column_iter.next())?;

                iterators.push(column_iter);
            }

            column_iterators = iterators.into_iter();
        }

        Ok(())
    }

    /// Appends a column to data file. `fn commit()` should be called to flush offsets and config to
    /// disk.
    pub fn append_column(
        &mut self,
        column: Option<ColumnResult<impl AsRef<[u8]>>>,
    ) -> Result<(), NippyJarError> {
        self.dirty = true;

        match column {
            Some(Ok(value)) => {
                if self.offsets.is_empty() {
                    // Represents the offset of the soon to be appended data column
                    self.offsets.push(self.data_file.stream_position()?);
                }

                let written = self.write_column(value.as_ref())?;

                // Last offset represents the size of the data file if no more data is to be
                // appended. Otherwise, represents the offset of the next data item.
                self.offsets.push(self.offsets.last().expect("qed") + written as u64);
            }
            None => {
                return Err(NippyJarError::UnexpectedMissingValue(
                    self.jar.rows as u64,
                    self.column as u64,
                ))
            }
            Some(Err(err)) => return Err(err.into()),
        }

        Ok(())
    }

    /// Writes column to data file. If it's the last column of the row, call `finalize_row()`
    fn write_column(&mut self, value: &[u8]) -> Result<usize, NippyJarError> {
        self.uncompressed_row_size += value.len();
        let (stored, len) = if let Some(compressors) = &mut self.dictionary_compressors {
            let stored = compressors.compress(self.column, value, &mut self.tmp_buf)?;
            (stored, stored.len())
        } else if let Some(compression) = &self.jar.compressor {
            let before = self.tmp_buf.len();
            let len = compression.compress_to(value, &mut self.tmp_buf)?;
            (&self.tmp_buf[before..before + len], len)
        } else {
            (value, value.len())
        };
        self.data_file.write_all(stored)?;

        // TN(TN-0120): `jar.rows` counts the uncommitted rows too, so it is this row's index.
        let len = if self.jar.has_checksums() {
            let checksum = value_checksum(self.jar.rows as u64, self.column, stored);
            self.data_file.write_all(&checksum.to_le_bytes())?;
            len + CHECKSUM_SIZE
        } else {
            len
        };

        self.column += 1;

        if self.jar.columns == self.column {
            self.finalize_row();
        }

        Ok(len)
    }

    /// Prunes rows from data and offsets file and updates its configuration on disk
    ///
    /// TN(TN-0130): the configuration with the new row count is committed before either file
    /// shrinks, so a prune stopped part way leaves files longer than their configuration: an
    /// uncommitted tail, which the open-time heal cuts as it cuts an interrupted append. Files
    /// shorter than their configuration are then damage, which a checksummed jar's heal refuses
    /// ([`NippyJarChecker`]). A finished prune leaves the files it left before.
    ///
    /// TN(TN-0161): a failed prune leaves the writer failed.
    pub fn prune_rows(&mut self, num_rows: usize) -> Result<(), NippyJarError> {
        self.check_not_failed()?;
        let result = self.prune_rows_inner(num_rows);
        self.fail_on(result)
    }

    fn prune_rows_inner(&mut self, num_rows: usize) -> Result<(), NippyJarError> {
        self.dirty = true;

        self.offsets_file.flush()?;
        self.data_file.flush()?;

        // Each column of a row is one offset
        let num_offsets = num_rows * self.jar.columns;

        // Calculate the number of offsets to prune from in-memory list
        let offsets_prune_count = num_offsets.min(self.offsets.len().saturating_sub(1)); // last element is the expected size of the data file
        let remaining_to_prune = num_offsets.saturating_sub(offsets_prune_count);

        // TN(TN-0130): the lengths the files are cut to once the configuration is committed.
        let mut data_len = None;
        let mut offsets_len = None;

        // Prune in-memory offsets if needed
        if offsets_prune_count > 0 {
            // Determine new length based on the offset to prune up to
            let new_len = self.offsets[(self.offsets.len() - 1) - offsets_prune_count]; // last element is the expected size of the data file
            self.offsets.truncate(self.offsets.len() - offsets_prune_count);

            // The data file is cut to the new length
            data_len = Some(new_len);
        }

        // Prune from on-disk offset list if there are still rows left to prune
        if remaining_to_prune > 0 {
            // Get the current length of the on-disk offset file
            let length = self.offsets_file.get_ref().metadata()?.len();

            // Handle non-empty offset file
            if length > 1 {
                // first byte is reserved for `bytes_per_offset`, which is 8 initially.
                let num_offsets = (length - 1) / OFFSET_SIZE_BYTES as u64;

                if remaining_to_prune as u64 > num_offsets {
                    return Err(NippyJarError::InvalidPruning(
                        num_offsets,
                        remaining_to_prune as u64,
                    ))
                }

                let new_num_offsets = num_offsets.saturating_sub(remaining_to_prune as u64);

                // If all rows are to be pruned
                if new_num_offsets <= 1 {
                    // <= 1 because the one offset would actually be the expected file data size
                    //
                    // When no rows remain, keep the offset size byte and the final offset (data
                    // file size = 0). This maintains the same structure as when
                    // a file is initially created.
                    // See `NippyJarWriter::create_or_open_files` for the initial file format.
                    offsets_len = Some(1 + OFFSET_SIZE_BYTES as u64);
                    data_len = Some(0);
                } else {
                    // Calculate the new length for the on-disk offset list
                    let new_len = 1 + new_num_offsets * OFFSET_SIZE_BYTES as u64;
                    // Seek to the position of the last offset
                    self.offsets_file
                        .seek(SeekFrom::Start(new_len.saturating_sub(OFFSET_SIZE_BYTES as u64)))?;
                    // Read the last offset value
                    let mut last_offset = [0u8; OFFSET_SIZE_BYTES as usize];
                    self.offsets_file.get_ref().read_exact(&mut last_offset)?;
                    let last_offset = u64::from_le_bytes(last_offset);

                    // The lengths of both the offsets and data files
                    offsets_len = Some(new_len);
                    data_len = Some(last_offset);
                }
            } else {
                return Err(NippyJarError::InvalidPruning(0, remaining_to_prune as u64))
            }
        }

        if remaining_to_prune > 0 {
            // Every uncommitted row was pruned, so the remaining in-memory offset (the previous
            // data file end) is stale: the on-disk prune shrinks the data file below it.
            self.offsets.clear();
        } else {
            // The config written below counts the uncommitted rows that were not pruned, so their
            // offsets and data must be on disk first. If we crash before the config is written,
            // the offsets file is longer than the config expects and the checker heals it as an
            // interrupted append.
            self.commit_offsets_inner()?;
            reth_fs_util::fsync_file(self.offsets_file.get_ref())?;
            reth_fs_util::fsync_file(self.data_file.get_ref())?;
        }

        self.jar.rows = self.jar.rows.saturating_sub(num_rows);
        if self.jar.rows == 0 {
            self.jar.max_row_size = 0;
        }
        self.jar.freeze_config()?;

        // TN(TN-0130): the files shrink only now, after the configuration that no longer counts
        // the pruned rows.
        if let Some(len) = offsets_len {
            self.offsets_file.get_mut().set_len(len)?;
        }
        if let Some(len) = data_len {
            self.data_file.get_mut().set_len(len)?;
        }
        reth_fs_util::fsync_file(self.offsets_file.get_ref())?;
        reth_fs_util::fsync_file(self.data_file.get_ref())?;

        self.offsets_file.seek(SeekFrom::End(0))?;
        self.data_file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    /// Updates [`NippyJar`] with the new row count and maximum uncompressed row size, while
    /// resetting internal fields.
    fn finalize_row(&mut self) {
        self.jar.max_row_size = self.jar.max_row_size.max(self.uncompressed_row_size);
        self.jar.rows += 1;

        self.tmp_buf.clear();
        self.uncompressed_row_size = 0;
        self.column = 0;
    }

    /// Commits configuration and offsets to disk. It drains the internal offset list.
    pub fn commit(&mut self) -> Result<(), NippyJarError> {
        self.sync_all()?;
        self.finalize()?;
        Ok(())
    }

    /// Syncs data and offsets to disk.
    ///
    /// This does NOT commit the configuration. Call [`Self::finalize`] after to write the
    /// configuration and mark the writer as clean.
    pub fn sync_all(&mut self) -> Result<(), NippyJarError> {
        self.sync_data()?;
        self.commit_offsets()?;
        Ok(())
    }

    /// Flushes and syncs the data file, the first half of [`Self::sync_all`];
    /// [`Self::commit_offsets`] is the second.
    ///
    /// TN(TN-0119): split out, and `commit_offsets` made public, so the static-file writer can
    /// time the two syncs apart.
    pub fn sync_data(&mut self) -> Result<(), NippyJarError> {
        let result = self.sync_jobs().0.run();
        self.fail_on(result)
    }

    /// The two halves of [`Self::sync_all`], [`Self::sync_data`] and [`Self::commit_offsets`], as
    /// jobs that borrow disjoint parts of the writer, so they can run on two threads at once.
    ///
    /// TN(TN-0138): the static-file writer runs them beside its `.csoff` sync. Their order does
    /// not matter: the configuration, written only after both, bounds what a reopen keeps.
    /// TN(TN-0161): a caller whose job failed calls [`Self::mark_failed`].
    pub const fn sync_jobs(&mut self) -> (DataSync<'_>, OffsetsSync<'_>) {
        (
            DataSync { data_file: &mut self.data_file },
            OffsetsSync { offsets_file: &mut self.offsets_file, offsets: &mut self.offsets },
        )
    }

    /// Commits configuration to disk and marks the writer as clean.
    ///
    /// Must be called after [`Self::sync_all`] to complete the commit.
    pub fn finalize(&mut self) -> Result<(), NippyJarError> {
        self.check_not_failed()?;
        // Flushes `max_row_size` and total `rows` to disk.
        self.jar.freeze_config()?;
        self.dirty = false;

        Ok(())
    }

    /// Commits changes to the data file and offsets without synchronizing all data to disk.
    ///
    /// This function flushes the buffered data to the data file and commits the offsets,
    /// but it does not guarantee that all data is synchronized to persistent storage.
    #[cfg(feature = "test-utils")]
    pub fn commit_without_sync_all(&mut self) -> Result<(), NippyJarError> {
        self.check_not_failed()?;
        self.data_file.flush()?;

        self.commit_offsets_without_sync_all()?;

        // Flushes `max_row_size` and total `rows` to disk.
        self.jar.freeze_config()?;
        self.dirty = false;

        Ok(())
    }

    /// Flushes offsets to disk.
    pub fn commit_offsets(&mut self) -> Result<(), NippyJarError> {
        let result = self.sync_jobs().1.run();
        self.fail_on(result)
    }

    #[cfg(feature = "test-utils")]
    fn commit_offsets_without_sync_all(&mut self) -> Result<(), NippyJarError> {
        self.commit_offsets_inner()
    }

    /// Flushes offsets to disk.
    ///
    /// CAUTION: Does not sync the offsets file and requires a manual call to
    /// `reth_fs_util::fsync_file(self.offsets_file.get_ref())`.
    fn commit_offsets_inner(&mut self) -> Result<(), NippyJarError> {
        let result = self.sync_jobs().1.write();
        self.fail_on(result)
    }

    /// TN(TN-0161): marks the writer failed ([`Self::has_failed`]): it writes no configuration
    /// again. For a caller whose [`Self::sync_jobs`] job, or sync of a file of its own that the
    /// configuration counts, failed.
    pub const fn mark_failed(&mut self) {
        self.failed = true;
    }

    /// TN(TN-0161): whether a sync, offsets write or prune of this writer failed, after which it
    /// writes no configuration.
    pub const fn has_failed(&self) -> bool {
        self.failed
    }

    /// TN(TN-0161): `result`, with the writer marked failed if it is an error.
    const fn fail_on<T>(&mut self, result: Result<T, NippyJarError>) -> Result<T, NippyJarError> {
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// TN(TN-0161): refuses to write a configuration after a failure.
    fn check_not_failed(&self) -> Result<(), NippyJarError> {
        if self.failed {
            return Err(NippyJarError::WriterFailed(self.jar.data_path().to_path_buf()))
        }
        Ok(())
    }

    /// Returns the maximum row size for the associated [`NippyJar`].
    #[cfg(test)]
    pub const fn max_row_size(&self) -> usize {
        self.jar.max_row_size
    }

    /// Returns the column index of the current checker instance.
    #[cfg(test)]
    pub const fn column(&self) -> usize {
        self.column
    }

    /// Returns a reference to the offsets vector.
    #[cfg(test)]
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    /// Returns a mutable reference to the offsets vector.
    #[cfg(test)]
    pub const fn offsets_mut(&mut self) -> &mut Vec<u64> {
        &mut self.offsets
    }

    /// Returns the path to the offsets file for the associated [`NippyJar`].
    #[cfg(test)]
    pub fn offsets_path(&self) -> std::path::PathBuf {
        self.jar.offsets_path()
    }

    /// Returns the path to the data file for the associated [`NippyJar`].
    #[cfg(test)]
    pub fn data_path(&self) -> &std::path::Path {
        self.jar.data_path()
    }

    /// Returns a mutable reference to the buffered writer for the data file.
    #[cfg(any(test, feature = "test-utils"))]
    pub const fn data_file(&mut self) -> &mut BufWriter<File> {
        &mut self.data_file
    }

    /// Returns a reference to the associated [`NippyJar`] instance.
    #[cfg(any(test, feature = "test-utils"))]
    pub const fn jar(&self) -> &NippyJar<H> {
        &self.jar
    }
}

/// TN(TN-0146): the compressors of a dictionary zstd jar, one per column from the configuration's
/// dictionaries. Made once per writer: a context per value would cost more than compressing a
/// small value. The writer used to compress every value without the dictionaries the cursor
/// decompresses with.
struct DictionaryCompressors(Vec<Compressor<'static>>);

impl std::fmt::Debug for DictionaryCompressors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DictionaryCompressors").field(&self.0.len()).finish()
    }
}

impl DictionaryCompressors {
    /// The compressors of `jar` if it compresses with dictionaries.
    fn new<H: NippyJarHeader>(jar: &NippyJar<H>) -> Result<Option<Self>, NippyJarError> {
        let Some(Compressors::Zstd(zstd)) = jar.compressor() else { return Ok(None) };
        if !zstd.use_dict {
            return Ok(None)
        }
        let compressors = zstd.compressors()?.ok_or(NippyJarError::CompressorNotReady)?;
        if compressors.len() != jar.columns {
            return Err(NippyJarError::ColumnLenMismatch(jar.columns, compressors.len()))
        }
        Ok(Some(Self(compressors)))
    }

    /// Replaces `buf` with `value` compressed with `column`'s dictionary and returns it.
    fn compress<'b>(
        &mut self,
        column: usize,
        value: &[u8],
        buf: &'b mut Vec<u8>,
    ) -> Result<&'b [u8], NippyJarError> {
        buf.clear();
        buf.reserve(zstd_safe::compress_bound(value.len()));
        self.0[column].compress_to_buffer(value, buf)?;
        Ok(buf)
    }
}

/// The data file's half of [`NippyJarWriter::sync_all`], from [`NippyJarWriter::sync_jobs`]:
/// writes out the buffered rows, then syncs the file.
#[derive(Debug)]
pub struct DataSync<'a> {
    data_file: &'a mut BufWriter<File>,
}

impl DataSync<'_> {
    /// Flushes and syncs the data file ([`NippyJarWriter::sync_data`]).
    pub fn run(self) -> Result<(), NippyJarError> {
        self.data_file.flush()?;
        // TN(TN-0117): a plain `fsync` on macOS (no `F_FULLFSYNC`), as at every sync of the
        // writer.
        reth_fs_util::fsync_file(self.data_file.get_ref())?;
        Ok(())
    }
}

/// The offsets file's half of [`NippyJarWriter::sync_all`], from [`NippyJarWriter::sync_jobs`]:
/// appends the offsets not yet on disk, then syncs the file.
#[derive(Debug)]
pub struct OffsetsSync<'a> {
    offsets_file: &'a mut BufWriter<File>,
    offsets: &'a mut Vec<u64>,
}

impl OffsetsSync<'_> {
    /// Appends and syncs the offsets ([`NippyJarWriter::commit_offsets`]).
    pub fn run(mut self) -> Result<(), NippyJarError> {
        self.write()?;
        reth_fs_util::fsync_file(self.offsets_file.get_ref())?;
        Ok(())
    }

    /// Appends the offsets, draining them, and flushes the file without syncing it.
    fn write(&mut self) -> Result<(), NippyJarError> {
        // The last offset on disk can be the first offset of `self.offsets` given how
        // `append_column()` works alongside commit. So we need to skip it.
        let mut last_offset_ondisk = if self.offsets_file.get_ref().metadata()?.len() > 1 {
            self.offsets_file.seek(SeekFrom::End(-(OFFSET_SIZE_BYTES as i64)))?;
            let mut buf = [0u8; OFFSET_SIZE_BYTES as usize];
            self.offsets_file.get_ref().read_exact(&mut buf)?;
            Some(u64::from_le_bytes(buf))
        } else {
            None
        };

        self.offsets_file.seek(SeekFrom::End(0))?;

        // Appends new offsets to disk
        for offset in self.offsets.drain(..) {
            if let Some(last_offset_ondisk) = last_offset_ondisk.take() &&
                last_offset_ondisk == offset
            {
                continue
            }
            self.offsets_file.write_all(&offset.to_le_bytes())?;
        }
        self.offsets_file.flush()?;

        Ok(())
    }
}
