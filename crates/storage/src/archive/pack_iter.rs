//! Raw iterator over a sldb data file.  This will work without an index file and can be used to
//! open the file directly.

use std::{
    fmt::Debug,
    io::{self, BufReader, Read, Seek},
    marker::PhantomData,
};

use serde::{de::DeserializeOwned, Serialize};
use tn_types::try_decode;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::archive::{
    crc::{crc32, crc32_update},
    error::{fetch::FetchError, load_header::LoadHeaderError},
    pack::{DataHeader, PackCompression, DATA_HEADER_BYTES},
};

/// Provide an upper bound on a record size.
/// This should be large enough for any record but provide
/// an upper bound on memory allocations for a record.
pub const MAX_RECORD_SIZE: u32 = 16 * 1024 * 1024;

/// A zstd failure on a CRC-valid record is a fault in the record's bytes, not in reading them.
fn zstd_decode_error(e: io::Error) -> FetchError {
    FetchError::DeserializeValue(format!("zstd: {e}"))
}

/// Decompress a zstd record `compressed` payload into `out`, enforcing the in-memory
/// `MAX_RECORD_SIZE` cap on the decompressed size, so an oversized record is reported as
/// `RequestedDecompressSizeTooLarge`. Shared by every local decompression site: the iterators
/// (sync, and async unless [`AsyncPackIter::bounded_for_peer`] set the bound), which reuse their
/// buffer, and the `&self` `fetch` path (`PackInner::read_record_into`), which decompresses into
/// a fresh buffer and then decodes the slice. Peer-facing sites go through
/// [`decompress_peer_record`], which also bounds the stored size (#1787). Clears `out` first and
/// returns a borrow of it.
///
/// The record's frame passed its CRC before this runs, so a payload zstd rejects is what the writer
/// produced, not a read failure: it is reported as a decode fault, never as `IO` (a peer stream's
/// importer treats `IO` as a transport failure that says nothing about the bytes).
pub(crate) fn decompress_checked<'a>(
    compressed: &[u8],
    out: &'a mut Vec<u8>,
) -> Result<&'a [u8], FetchError> {
    let mut decoder = zstd::stream::read::Decoder::new(compressed).map_err(zstd_decode_error)?;
    decoder.window_log_max(24).map_err(zstd_decode_error)?;
    out.clear();
    // +1 lets us detect overflow vs. natural EOF
    let mut limited = decoder.take(MAX_RECORD_SIZE as u64 + 1);
    limited.read_to_end(out).map_err(zstd_decode_error)?;
    if out.len() as u64 > MAX_RECORD_SIZE as u64 {
        return Err(FetchError::RequestedDecompressSizeTooLarge(MAX_RECORD_SIZE));
    }
    Ok(out)
}

/// zstd's worst-case stored size for one frame of `content_len` bytes of content
/// (`zstd_safe::compress_bound`), plus headroom for the frame header and checksum. The pack
/// writer emits one frame per record, so every honest record meets this bound; a record above it
/// carries bytes that are not content (#1787). Checked by [`decompress_peer_record`].
pub(crate) fn stored_bound(content_len: usize) -> usize {
    zstd::zstd_safe::compress_bound(content_len).saturating_add(64)
}

/// Decompress a zstd record received from a peer as [`decompress_checked`] does, then refuse it
/// when its stored size is above [`stored_bound`] of its content. This refuses skippable frames,
/// empty frames, empty blocks and any other padding: the sync rate meter counts stored bytes, so
/// padding would feed the meter bytes that carry no content (GHSA-8fj9-xc3h-gmxm, #1787). The
/// refusal is `DeserializeValue`, which the peer paths map to `PackError::UndecodableRecord`
/// (`record_read_error`), so the probe fails and the peer is marked for the epoch.
///
/// Local reads keep [`decompress_checked`]: imports store a peer's bytes verbatim, so a pack
/// imported before this bound may hold such records, and they must still read.
pub(crate) fn decompress_peer_record<'a>(
    compressed: &[u8],
    out: &'a mut Vec<u8>,
) -> Result<&'a [u8], FetchError> {
    let content = decompress_checked(compressed, out)?;
    let content_len = content.len();
    let bound = stored_bound(content_len);
    (compressed.len() <= bound).then_some(content).ok_or_else(|| {
        FetchError::DeserializeValue(format!(
            "zstd: {} stored bytes for {content_len} bytes of content (bound {bound})",
            compressed.len()
        ))
    })
}

/// Iterate over a pack's records (values) in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
#[derive(Debug)]
pub struct PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: Read,
{
    _val: PhantomData<V>,
    reader: BufReader<R>,
    buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
    compression: PackCompression,
    version: u16,
    /// Logical data length: the sequential scan stops here rather than at physical EOF. The mmap
    /// backend pads the physical file past the data, and a `try_clone`d handle can see that
    /// padding re-grow under a concurrent append, so reading to EOF would decode trailing
    /// zeros as a (CRC-failing) record. `end` is the exact byte length of the header + all
    /// complete records.
    end: u64,
    /// Absolute file offset of the next record to read, tracked so the scan can stop at `end`.
    pos: u64,
    /// The largest record read (a longer one is refused, never allocated for).
    max_record: u32,
}

impl<V, R> PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: Read + Seek,
{
    /// Open the iterator using reader as a data source.
    /// Produces an iterator over all the records, in insert order.
    ///
    /// `end` is the logical data length (header + all complete records); the scan stops there
    /// instead of at physical EOF so mmap capacity padding is never decoded as a record.
    pub fn open(mut reader: R, uid_idx: u64, end: u64) -> Result<Self, LoadHeaderError> {
        let header = DataHeader::load_header(&mut reader, uid_idx)?;
        if header.appnum() != 1 {
            return Err(LoadHeaderError::InvalidAppNum);
        }
        let reader = BufReader::new(reader);
        Ok(PackIter {
            _val: PhantomData,
            reader,
            buffer: Vec::new(),
            decompress_buffer: Vec::new(),
            compression: header.compression(),
            version: header.version(),
            end,
            // `load_header` consumed exactly the fixed-size header, so the first record starts
            // here.
            pos: DATA_HEADER_BYTES as u64,
            max_record: MAX_RECORD_SIZE,
        })
    }

    /// Read records up to `max` bytes instead of [`MAX_RECORD_SIZE`] (a pack opened with a larger
    /// cap, see [`MmapFileOptions::max_record_size`](crate::archive::data_file::MmapFileOptions)).
    pub(crate) fn with_max_record(mut self, max: u32) -> Self {
        self.max_record = max;
        self
    }

    /// Return the version of the underlying data.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Byte offset just past the last record `next()` returned (the header + all whole record
    /// frames consumed so far). After a successful `next()` this is a record boundary — the
    /// safe point to truncate a torn log back to. It is NOT a valid boundary after `next()`
    /// returns an `Err`: `pos` advances by the frame's claimed length before the CRC/decode is
    /// checked, so on a decode failure it can sit past an incomplete (or, on a corrupt size
    /// prefix, out-of-range) frame. Recovery callers capture it *before* each `next()` and
    /// never read it after an `Err`.
    pub fn logical_position(&self) -> u64 {
        self.pos
    }

    /// Sets the current position of the data file.
    pub fn set_position(&mut self, position: u64) -> io::Result<()> {
        self.reader.seek(io::SeekFrom::Start(position))?;
        self.pos = position;
        Ok(())
    }

    /// Read the next record's payload without decoding it: CRC-checked and decompressed bytes,
    /// borrowed until the next call. The raw counterpart of `next`, for byte logs written with
    /// [`Pack::append_raw`](crate::archive::pack::Pack::append_raw). `None` at the logical end; an
    /// `Err` is a torn or corrupt frame (see [`Self::logical_position`] for where to cut).
    pub fn next_raw(&mut self) -> Option<Result<&[u8], FetchError>> {
        match Self::read_frame(
            &mut self.reader,
            &mut self.buffer,
            &mut self.decompress_buffer,
            self.compression,
            self.end,
            &mut self.pos,
            self.max_record,
        ) {
            Ok(payload) => Some(Ok(payload)),
            Err(FetchError::NotFound) => None,
            Err(err) => Some(Err(err)),
        }
    }

    /// Read and decode the next record.
    /// This expects the file cursor to be positioned at the record's first byte.
    fn read_record_file<R2: Read + Seek>(
        file: &mut R2,
        buffer: &mut Vec<u8>,
        decompress_buffer: &mut Vec<u8>,
        compression: PackCompression,
        end: u64,
        pos: &mut u64,
        max: u32,
    ) -> Result<V, FetchError> {
        let payload =
            Self::read_frame(file, buffer, decompress_buffer, compression, end, pos, max)?;
        try_decode::<V>(payload).map_err(|e| FetchError::DeserializeValue(e.to_string()))
    }

    /// Read the next record's frame and return its CRC-checked (and decompressed) payload.
    /// This expects the file cursor to be positioned at the record's first byte.
    ///
    /// Stops at the logical `end` (returning `NotFound`) before reading past the data into any mmap
    /// capacity padding; `pos` is advanced by the on-disk frame size of each record read.
    fn read_frame<'b, R2: Read + Seek>(
        file: &mut R2,
        buffer: &'b mut Vec<u8>,
        decompress_buffer: &'b mut Vec<u8>,
        compression: PackCompression,
        end: u64,
        pos: &mut u64,
        max: u32,
    ) -> Result<&'b [u8], FetchError> {
        // At (or past) the logical data end: stop cleanly rather than decode trailing padding.
        if *pos >= end {
            return Err(FetchError::NotFound);
        }
        #[cfg(test)]
        if FAIL_READ_AT.with(|at| at.get() == Some(*pos))
            && FAIL_READ_SKIP.with(|skip| skip.replace(skip.get().saturating_sub(1))) == 0
        {
            FAIL_READ_AT.with(|at| at.set(None));
            if FAIL_READ_TORN.with(|torn| torn.replace(false)) {
                return Err(FetchError::IO(io::ErrorKind::UnexpectedEof.into()));
            }
            return Err(FetchError::IO(io::Error::from_raw_os_error(5)));
        }
        let mut val_size_buf = [0_u8; 4];
        if let Err(err) = file.read_exact(&mut val_size_buf) {
            // An EOF here should be caused by no more records although it is possible there
            // was a bit of garbage at the end of the file, not worrying about that now (maybe
            // ever).
            if let io::ErrorKind::UnexpectedEof = err.kind() {
                return Err(FetchError::NotFound);
            }
            return Err(FetchError::IO(err));
        }
        let val_size = u32::from_le_bytes(val_size_buf);
        if val_size > max {
            return Err(FetchError::RequestedSizeTooLarge(val_size, max));
        }
        // On-disk frame: 4-byte size prefix + payload + 4-byte CRC. `checked_add` mirrors the
        // offset-arithmetic hardening in `MmapDataFile::write`; the `*pos >= end` guard above plus
        // real mmap file-size limits already make an overflow impossible, so this only formalizes
        // it.
        let frame_end = pos.checked_add(4 + val_size as u64 + 4).ok_or_else(|| {
            FetchError::IO(io::Error::new(
                io::ErrorKind::InvalidData,
                "pack record position overflow",
            ))
        })?;
        // The whole frame must end within the logical data. A frame that straddles `end` would
        // read its tail from bytes past the data (capacity padding, or bytes appended after the
        // iterator was opened), so it is a torn frame here, not a record.
        if frame_end > end {
            return Err(FetchError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record frame extends past the logical data end",
            )));
        }
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer)?;
        // The record CRC covers `len | payload`.
        let calc_crc32 = crc32_update(crc32(&val_size_buf), buffer);
        let mut buf_u32 = [0_u8; 4];
        file.read_exact(&mut buf_u32)?;
        *pos = frame_end;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        match compression {
            PackCompression::None => Ok(&buffer[..]),
            PackCompression::ZStd => decompress_checked(&buffer[..], decompress_buffer),
        }
    }
}

impl FetchError {
    /// Is this a read the disk failed, as opposed to a frame that is short, runs past the end or
    /// fails its checks? [`PackIter::read_frame`] defines the split: a tear (a frame past the
    /// logical end, a short read) is an `IO` error of kind `UnexpectedEof`; any other `IO` error is
    /// a disk failure; CRC, decompression and decode failures are not `IO` at all. A log walk must
    /// stop on a read failure with an error, never take it for a torn tail: acked records may
    /// follow it, and the bytes may read on a retry.
    pub(crate) fn is_read_failure(&self) -> bool {
        matches!(self, FetchError::IO(e) if e.kind() != io::ErrorKind::UnexpectedEof)
    }
}

impl<V, R: Read + Seek> Iterator for PackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
{
    type Item = Result<V, FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        match Self::read_record_file(
            &mut self.reader,
            &mut self.buffer,
            &mut self.decompress_buffer,
            self.compression,
            self.end,
            &mut self.pos,
            self.max_record,
        ) {
            Ok(val) => Some(Ok(val)),
            Err(err) => match err {
                FetchError::NotFound => None,
                _ => Some(Err(err)),
            },
        }
    }
}

/// Async iteration over a pack's records (values) in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
///
/// Unlike [`PackIter`], this has **no logical-end bound** — it reads to reader EOF and stops only
/// when the next record's size prefix hits EOF. Callers must therefore feed it exactly the logical
/// bytes: a sealed file, or a framed/length-bounded network stream (state-sync bounds its copy to
/// `data_file_len()`). It must **not** be handed a live, capacity-padded mmap data file: the
/// trailing zero padding would decode as a 0-size record and surface a recoverable padded tail as a
/// `CrcFailed` error instead of a clean end.
#[derive(Debug)]
pub struct AsyncPackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: AsyncRead,
{
    _val: PhantomData<V>,
    reader: R,
    buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
    compression: PackCompression,
    version: u16,
    /// When set, zstd records are decoded with [`decompress_peer_record`] (see
    /// [`AsyncPackIter::bounded_for_peer`]); otherwise with [`decompress_checked`].
    bound_stored: bool,
}

impl<V, R> AsyncPackIter<V, R>
where
    V: Debug + Serialize + DeserializeOwned,
    R: AsyncRead + Unpin,
{
    /// Open the iterator using reader as a data source.
    /// Produces an iterator over all the records, in insert order.
    pub async fn open(
        mut reader: R,
        uid_idx: u64,
        max_version: u16,
    ) -> Result<Self, LoadHeaderError> {
        let header = DataHeader::load_header_async(&mut reader, uid_idx).await?;
        // This is a peer-facing parser: reject a foreign `appnum`, and reject a pack-format version
        // newer than the caller understands, UP FRONT — so no caller can forget to bound the
        // version and end up parsing an unknown future format with current-format logic.
        // `max_version` is the caller's supported ceiling (e.g. `PACK_VERSION`); pass
        // `u16::MAX` to accept any version.
        if header.appnum() != 1 {
            return Err(LoadHeaderError::InvalidAppNum);
        }
        if header.version() > max_version {
            return Err(LoadHeaderError::InvalidVersion);
        }
        Ok(AsyncPackIter {
            _val: PhantomData,
            reader,
            buffer: Vec::new(),
            decompress_buffer: Vec::new(),
            compression: header.compression(),
            version: header.version(),
            bound_stored: false,
        })
    }

    /// Open the iterator using reader as a data source.
    /// Produces an iterator over all the records, in insert order.
    /// This version only expects a chunk of records not a complete pack file (no header for
    /// instance).
    pub async fn open_partial(
        reader: R,
        compression: PackCompression,
        version: u16,
    ) -> Result<Self, LoadHeaderError> {
        Ok(AsyncPackIter {
            _val: PhantomData,
            reader,
            buffer: Vec::new(),
            decompress_buffer: Vec::new(),
            compression,
            version,
            bound_stored: false,
        })
    }

    /// Decode every zstd record with [`decompress_peer_record`], not [`decompress_checked`]: a
    /// record stored in more than [`stored_bound`] of its content is refused as
    /// `DeserializeValue` (#1787). For streams received from a peer (`bytes_to_verified_output`,
    /// `stream_import`). A local read keeps the unbounded decode, since a pack imported before
    /// the bound may hold padded records.
    pub(crate) fn bounded_for_peer(self) -> Self {
        Self { bound_stored: true, ..self }
    }

    /// Return the version of the underlying data.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Return the compression of the underlying data.
    pub fn compression(&self) -> PackCompression {
        self.compression
    }

    /// Read the next record's frame, leaving its CRC-checked payload in `buffer` as stored
    /// (compressed when the data is). A clean end of the reader at a record boundary is
    /// `NotFound`.
    async fn read_frame(file: &mut R, buffer: &mut Vec<u8>) -> Result<(), FetchError> {
        let mut val_size_buf = [0_u8; 4];
        if let Err(err) = file.read_exact(&mut val_size_buf).await {
            // An EOF here should be caused by no more records although it is possible there
            // was a bit of garbage at the end of the file, not worrying about that now (maybe
            // ever).
            if let io::ErrorKind::UnexpectedEof = err.kind() {
                return Err(FetchError::NotFound);
            }
            return Err(FetchError::IO(err));
        }
        let val_size = u32::from_le_bytes(val_size_buf);
        if val_size > MAX_RECORD_SIZE {
            return Err(FetchError::RequestedSizeTooLarge(val_size, MAX_RECORD_SIZE));
        }
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer).await?;
        // The record CRC covers `len | payload`.
        let calc_crc32 = crc32_update(crc32(&val_size_buf), buffer);
        let mut buf_u32 = [0_u8; 4];
        file.read_exact(&mut buf_u32).await?;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        Ok(())
    }

    /// Return the next V when available. A zstd record is decoded with `decompress_peer_record`
    /// once `bounded_for_peer` set the bound (a peer's stream, #1787), else with
    /// `decompress_checked`.
    pub async fn next(&mut self) -> Option<Result<V, FetchError>> {
        let decoded = match Self::read_frame(&mut self.reader, &mut self.buffer).await {
            Ok(()) => match self.compression {
                PackCompression::None => Ok(&self.buffer[..]),
                PackCompression::ZStd => {
                    if self.bound_stored {
                        decompress_peer_record(&self.buffer[..], &mut self.decompress_buffer)
                    } else {
                        decompress_checked(&self.buffer[..], &mut self.decompress_buffer)
                    }
                }
            },
            Err(err) => Err(err),
        };
        match decoded.and_then(|payload| {
            try_decode::<V>(payload).map_err(|e| FetchError::DeserializeValue(e.to_string()))
        }) {
            Ok(val) => Some(Ok(val)),
            Err(FetchError::NotFound) => None,
            Err(err) => Some(Err(err)),
        }
    }

    /// Return the next record's CRC-checked payload without decompressing or decoding it: the
    /// bytes as stored, compressed when [`Self::compression`] says so (see
    /// [`decompress_checked`]). `None` at a clean end. Lets a caller do a record's CPU work (the
    /// decompress and the decode) elsewhere than where it reads the stream.
    pub async fn next_frame(&mut self) -> Option<Result<Vec<u8>, FetchError>> {
        let mut frame = Vec::new();
        match Self::read_frame(&mut self.reader, &mut frame).await {
            Ok(()) => Some(Ok(frame)),
            Err(FetchError::NotFound) => None,
            Err(err) => Some(Err(err)),
        }
    }

    /// Consume the iter and return the underlying reader.
    pub fn into_reader(self) -> R {
        self.reader
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: this thread's next read of a record frame starting at this position fails once,
    /// as a disk read can (`EIO`).
    pub(crate) static FAIL_READ_AT: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    /// Test-only: how many reads at [`FAIL_READ_AT`] succeed before the one that fails (so a test
    /// can fail a later pass over the same log).
    pub(crate) static FAIL_READ_SKIP: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Test-only: the injected failure is a short read (a torn frame), not `EIO`.
    pub(crate) static FAIL_READ_TORN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// One zstd frame of `content` at level 0, as the pack writer (`ZstdCtx`) stores a record.
    fn compress(content: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 0).unwrap();
        encoder.write_all(content).unwrap();
        encoder.finish().unwrap()
    }

    /// A zstd skippable frame (RFC 8878 3.1.2) that carries `n` zero bytes.
    fn skippable(n: usize) -> Vec<u8> {
        0x184D_2A50_u32
            .to_le_bytes()
            .into_iter()
            .chain(u32::try_from(n).unwrap().to_le_bytes())
            .chain(vec![0_u8; n])
            .collect()
    }

    /// `n` bytes of an xorshift64 stream from a fixed seed: content that zstd cannot shrink.
    fn pseudo_random(n: usize) -> Vec<u8> {
        std::iter::successors(Some(0x9E37_79B9_7F4A_7C15_u64), |&x| {
            let x = x ^ (x << 13);
            let x = x ^ (x >> 7);
            Some(x ^ (x << 17))
        })
        .flat_map(u64::to_le_bytes)
        .take(n)
        .collect()
    }

    /// Every honest record (one level-0 frame, as the pack writer stores it) fits
    /// [`stored_bound`] and passes [`decompress_peer_record`]: empty, tiny, block-edge and
    /// multi-block sizes of zeros, incompressible bytes and repeated text (#1787).
    #[test]
    fn stored_bound_admits_every_honest_record() {
        let sizes = [0, 1, 100, 4096, 131_071, 131_072, 1 << 20, 4 << 20];
        sizes
            .iter()
            .flat_map(|&n| {
                [
                    vec![0_u8; n],
                    pseudo_random(n),
                    b"telcoin pack record ".iter().copied().cycle().take(n).collect(),
                ]
            })
            .for_each(|content| {
                let stored = compress(&content);
                assert!(
                    stored.len() <= stored_bound(content.len()),
                    "{} stored bytes for {} bytes of content",
                    stored.len(),
                    content.len()
                );
                let mut out = Vec::new();
                let decoded = decompress_peer_record(&stored, &mut out).expect("honest record");
                assert_eq!(decoded, &content[..]);
            });
    }

    /// A skippable frame (RFC 8878 3.1.2) before or after a record's real frame is padding. The
    /// multi-frame decoder of [`decompress_checked`] skips it and yields the content (pinned here:
    /// it is why the peer paths need the bound), and [`decompress_peer_record`] refuses the
    /// record as `DeserializeValue` (GHSA-8fj9-xc3h-gmxm, #1787). The plain real frame passes.
    #[test]
    fn decompress_peer_record_rejects_skippable_frame_padding() {
        let payload = pseudo_random(64);
        let real = compress(&payload);
        let mut out = Vec::new();
        assert_eq!(decompress_peer_record(&real, &mut out).expect("plain frame"), &payload[..]);
        let pad = skippable(4096);
        let padded = [
            pad.iter().chain(real.iter()).copied().collect::<Vec<u8>>(),
            real.iter().chain(pad.iter()).copied().collect::<Vec<u8>>(),
        ];
        padded.iter().for_each(|stored| {
            let mut out = Vec::new();
            assert_eq!(
                decompress_checked(stored, &mut out).expect("the decoder skips the padding"),
                &payload[..]
            );
            assert!(matches!(
                decompress_peer_record(stored, &mut out),
                Err(FetchError::DeserializeValue(_))
            ));
        });
    }
}
