//! Frame codec for the primary's typed epoch-pack sync protocol.
//!
//! Step 6 of #739 cuts the full epoch-pack transfer over to the typed
//! [`SyncFrame`] layer: the request travels in the opening
//! [`SyncFrame::Req`](tn_network_libp2p::SyncFrame::Req) of a `/tn-primary-sync`
//! stream and the responder answers [`SyncFrame::Ack`] then streams the raw pack
//! bytes as a sequence of [`SyncFrame::Data`] frames terminated by
//! [`SyncFrame::End`] (or a [`SyncFrame::Deny`] when the pack is not held).
//!
//! Unlike the worker's batch path — where each `Data` frame is one whole
//! decodable [`Batch`](tn_types::Batch) — an epoch pack is a single opaque byte
//! stream (`EpochMeta` followed by interleaved batch and consensus records). The
//! responder therefore chunks the pack file into fixed-size `Data` frames and the
//! requester reassembles them into a contiguous [`tokio::io::AsyncRead`] so the
//! existing [`ConsensusChain::stream_import`](tn_storage::consensus::ConsensusChain::stream_import)
//! verifier consumes it unchanged.

use crate::error::PrimaryNetworkResult;
use futures::{
    io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite},
    AsyncWriteExt as _, FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
};
use std::time::Duration;
use tn_network_libp2p::{read_frame, write_frame, DenyReason, PrimarySyncRequest, SyncFrame};
use tn_storage::consensus::ConsensusChain;
use tn_types::{encode, encoded_size, try_decode, BlsPublicKey, Certificate, Epoch};
use tokio::io::{AsyncRead as TokioAsyncRead, AsyncReadExt as _};
use tracing::debug;

/// The raw pack bytes carried by a single [`SyncFrame::Data`] frame.
///
/// A larger chunk means fewer frames (and fewer flushes) at the cost of a larger
/// per-frame buffer on each side; 256 KiB keeps a full 512 MiB pack to a few
/// thousand frames.
const SYNC_PACK_CHUNK_SIZE: usize = 256 * 1024;

/// Headroom added to [`SYNC_PACK_CHUNK_SIZE`] for the `SyncFrame` envelope (the
/// enum tag and the `Data` length prefix) when bounding a decoded frame.
const SYNC_PACK_FRAME_OVERHEAD: usize = 1024;

/// The largest sync frame accepted on the primary epoch-pack stream: one `Data`
/// frame carrying a pack chunk, plus envelope headroom. Unlike the worker's
/// epoch-keyed bound, the pack is chunked at a fixed size, so this is constant.
pub(crate) const MAX_SYNC_PACK_FRAME_SIZE: usize = SYNC_PACK_CHUNK_SIZE + SYNC_PACK_FRAME_OVERHEAD;

/// Per-frame read timeout while reassembling the pack on the requester side. The
/// whole import is also bounded by the caller's `record_timeout`; this guards a
/// single stalled frame.
const EPOCH_PACK_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// Target encoded size of one missing-certificates [`SyncFrame::Data`] frame: the
/// responder accumulates certificates into a batch and flushes a frame once the
/// batch reaches this size, so a large catch-up is a few hundred frames rather
/// than one frame per certificate. A certificate is ~0.3-3.5 KiB, so a batch holds
/// many.
const SYNC_CERT_BATCH_TARGET_SIZE: usize = 256 * 1024;

/// The largest missing-certificates sync frame accepted on either side: one batch
/// `Data` frame plus headroom for the certificate that pushed the batch over the
/// target and the `SyncFrame` envelope. Bounds the per-frame allocation a peer can
/// force.
pub(crate) const MAX_SYNC_CERT_FRAME_SIZE: usize = SYNC_CERT_BATCH_TARGET_SIZE * 2;

/// Honest-responder cap on the total encoded certificate bytes streamed in one
/// missing-certificates exchange. The responder stops streaming (writes `End`) once
/// it has emitted this many bytes; the streaming collector is otherwise bounded only
/// by its processing-time limit, so this keeps a single exchange to a sane size
/// (64x the 1 MiB default legacy single-response cap) while still delivering far
/// more than one request-response message per round.
const MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Requester-side cap on the total encoded certificate bytes accepted in one
/// missing-certificates exchange. One [`MAX_SYNC_CERT_FRAME_SIZE`] above the honest
/// responder cap so the batch that carries an honest responder past
/// [`MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES`] is still accepted, while a responder
/// that ignores `End` and streams unboundedly is cut off.
///
/// This bounds a *single* exchange. The requester (`fetch_from_peers`) staggers several
/// fetches and cancels only on the first success, so a catch-up round can hold multiple
/// exchanges open at once; the process-wide requester peak is that concurrency times this
/// cap. See `fetch_from_peers` for the aggregate envelope (issue #822, finding 2).
const MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES: usize =
    MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES + MAX_SYNC_CERT_FRAME_SIZE;

/// Per-frame read timeout while reading the missing-certificates response on the
/// requester side. The whole exchange is also bounded by the caller's fetch
/// timeout; this guards a single stalled frame.
const MISSING_CERTS_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve an accepted epoch-pack sync exchange over `stream`.
///
/// `stop_number` selects the transfer: `None` serves the complete epoch pack,
/// `Some(last_consensus_number)` serves the verifiable PREFIX up to that consensus
/// output (the partial in-progress epoch, [`ConsensusChain::get_partial_epoch_stream`]).
/// Either way the transfer is all-or-nothing: if the requested pack (or its prefix)
/// is not held, the responder sheds with
/// [`SyncFrame::Deny`]`(`[`DenyReason::Unavailable`]`)` so the requester retries
/// another peer immediately instead of waiting out its ack timeout. Otherwise it
/// writes [`SyncFrame::Ack`], streams the pack as [`SyncFrame::Data`] frames, and
/// ends with [`SyncFrame::End`]. A partial transfer bounds the source reader to the
/// prefix length so it never streams past the verifiable point. Each frame write is
/// bounded by `buffer_timeout` so a peer that stops reading cannot stall the
/// responder task on an unbounded write. The caller has already admitted the
/// exchange against the concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_epoch_pack_over_stream<S>(
    stream: &mut S,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
    stop_number: Option<u64>,
    buffer_timeout: Duration,
    peer: BlsPublicKey,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // resolve the source reader and its byte cap through the `Option`'s combinator
    // rather than a Some/None branch: a full pack streams to EOF (`u64::MAX` never
    // bounds the `take` below), a partial prefix is capped at its `[0, end)`
    // verifiable length. Both arms normalize to one boxed future so `map_or_else`
    // can pick either; a missing pack (or an out-of-range partial stop point) sheds
    // cleanly so the requester retries elsewhere.
    let source = stop_number
        .map_or_else(
            || {
                consensus_chain
                    .get_epoch_stream(epoch)
                    .map_ok(|reader| (reader, u64::MAX))
                    .map_err(drop)
                    .boxed()
            },
            |number| consensus_chain.get_partial_epoch_stream(epoch, number).map_err(drop).boxed(),
        )
        .await;
    let Ok((epoch_stream, cap)) = source else {
        debug!(target: "primary::network", %peer, epoch, ?stop_number, "epoch pack unavailable; denying sync request");
        write_one_frame(
            stream,
            &SyncFrame::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
            buffer_timeout,
        )
        .await?;
        return Ok(());
    };

    // accept and flush immediately so the requester sees the `Ack` within its ack
    // timeout even when the first pack reads are slow
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;

    // a partial transfer caps the reader at the prefix length; a full transfer
    // resolved `cap` to `u64::MAX`, which never bounds the read, so this streams to
    // EOF.
    write_pack_data_frames(&mut epoch_stream.take(cap), stream, buffer_timeout).await?;
    Ok(())
}

/// Stream a pack reader as [`SyncFrame::Data`] frames terminated by
/// [`SyncFrame::End`]. Does not write the opening `Ack` (the caller decides
/// whether to accept); split out so the wire framing can be round-tripped in
/// tests against [`sync_pack_reader`] without a [`ConsensusChain`] fixture.
pub(crate) async fn write_pack_data_frames<R, S>(
    reader: &mut R,
    stream: &mut S,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    R: TokioAsyncRead + Unpin + Send,
    S: FuturesAsyncWrite + Unpin + Send,
{
    // Chunk the reader into fixed-capacity `Data` frames by folding a `ReaderStream`
    // (yielding `Ok(<=SYNC_PACK_CHUNK_SIZE bytes)`, ending at EOF, surfacing a read
    // error). The writer and the two scratch buffers ride the accumulator: this
    // reuses the buffers across frames and keeps the writer's `&mut` out of the
    // `FnMut` closure, whose returned future cannot borrow per-call captures. Each
    // chunk's backing allocation is reclaimed into the `Data` frame without a copy.
    let (stream, mut encode_buffer, mut compressed_buffer) =
        tokio_util::io::ReaderStream::with_capacity(reader, SYNC_PACK_CHUNK_SIZE)
            .try_fold(
                (stream, Vec::new(), Vec::new()),
                |(stream, mut encode_buffer, mut compressed_buffer), chunk| async move {
                    write_one_frame(
                        stream,
                        &SyncFrame::Data(chunk.into()),
                        &mut encode_buffer,
                        &mut compressed_buffer,
                        MAX_SYNC_PACK_FRAME_SIZE,
                        buffer_timeout,
                    )
                    .await?;
                    Ok((stream, encode_buffer, compressed_buffer))
                },
            )
            .await?;

    write_one_frame(
        stream,
        &SyncFrame::End,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await
}

/// Write a single frame and flush, bounding both on `buffer_timeout` so a
/// non-reading peer cannot pin the writer on an unbounded write. `max_frame_size`
/// bounds the encoded frame (pack chunks and cert batches use different caps).
async fn write_one_frame<S>(
    stream: &mut S,
    frame: &SyncFrame<PrimarySyncRequest>,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_frame_size: usize,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    tokio::time::timeout(buffer_timeout, async {
        write_frame(stream, frame, encode_buffer, compressed_buffer, max_frame_size).await?;
        stream.flush().await
    })
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout writing sync frame")
    })?
}

/// Reassemble the [`SyncFrame::Data`] frames of an accepted epoch-pack exchange
/// into a contiguous [`tokio::io::AsyncRead`] terminating at [`SyncFrame::End`].
///
/// The caller has already read the opening [`SyncFrame::Ack`]; this reads the
/// `Data`/`End` stream. An [`SyncFrame::Err`] frame or any out-of-place frame
/// surfaces as a read error so the importer aborts. The returned reader is fed
/// directly to
/// [`ConsensusChain::stream_import`](tn_storage::consensus::ConsensusChain::stream_import).
pub(crate) fn sync_pack_reader<S>(stream: S) -> impl TokioAsyncRead + Unpin + Send
where
    S: FuturesAsyncRead + Unpin + Send + 'static,
{
    let frames = futures::stream::unfold(
        (stream, Vec::new(), Vec::new()),
        |(mut stream, mut decode_buffer, mut compressed_buffer)| async move {
            let chunk =
                next_pack_chunk(&mut stream, &mut decode_buffer, &mut compressed_buffer).await;
            let state = (stream, decode_buffer, compressed_buffer);
            // Result<Option<bytes>> -> Option<Result<bytes>>: `End` stops the
            // stream, `Data` yields a chunk, an error yields one error item
            chunk.transpose().map(|item| (item.map(std::io::Cursor::new), state))
        },
    );
    tokio_util::io::StreamReader::new(Box::pin(frames))
}

/// Read the next pack chunk from an accepted exchange: `Ok(Some(bytes))` for a
/// `Data` frame, `Ok(None)` for `End`, `Err` for an aborted or malformed stream.
async fn next_pack_chunk<S>(
    stream: &mut S,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>>
where
    S: FuturesAsyncRead + Unpin + Send,
{
    let frame = tokio::time::timeout(
        EPOCH_PACK_FRAME_TIMEOUT,
        read_frame::<_, PrimarySyncRequest>(
            stream,
            decode_buffer,
            compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
        ),
    )
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout reading epoch pack sync frame")
    })??;

    match frame {
        SyncFrame::Data(bytes) => Ok(Some(bytes)),
        SyncFrame::End => Ok(None),
        SyncFrame::Err(err) => {
            Err(std::io::Error::other(format!("peer aborted epoch pack sync stream: {err:?}")))
        }
        // a well-behaved responder never sends these once streaming
        SyncFrame::Ack | SyncFrame::Deny(_) | SyncFrame::Req(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected sync frame during epoch pack stream",
        )),
    }
}

/// Serve an accepted single-consensus-output sync exchange over `stream`.
///
/// `bytes` is the output's pack-file-encoded bytes when the responder holds it, or
/// `None` when it is unavailable (an unknown number, or one not yet built during
/// catch-up). An unavailable output sheds with
/// [`SyncFrame::Deny`]`(`[`DenyReason::Unavailable`]`)` so the requester retries
/// another peer immediately instead of waiting out its ack timeout. Otherwise it
/// writes [`SyncFrame::Ack`], streams the bytes as [`SyncFrame::Data`] frames, and
/// ends with [`SyncFrame::End`]. Reuses the epoch-pack chunk framing
/// ([`write_pack_data_frames`]) and its [`MAX_SYNC_PACK_FRAME_SIZE`] bound, so the
/// requester reassembles it with [`read_sync_consensus_output`]. Each frame write
/// is bounded by `buffer_timeout` so a peer that stops reading cannot pin the
/// responder task. The caller has already admitted the exchange against the
/// concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_consensus_output_over_stream<S>(
    stream: &mut S,
    bytes: Option<Vec<u8>>,
    buffer_timeout: Duration,
    peer: BlsPublicKey,
    number: u64,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // an unavailable output sheds with `Deny(Unavailable)` so the requester retries
    // elsewhere without waiting out its ack timeout, exactly as the epoch-pack serve
    // does on a `get_epoch_stream` miss.
    let Some(bytes) = bytes else {
        debug!(target: "primary::network", %peer, number, "consensus output unavailable; denying sync request");
        write_one_frame(
            stream,
            &SyncFrame::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
            buffer_timeout,
        )
        .await?;
        return Ok(());
    };

    // accept and flush immediately so the requester sees the `Ack` within its ack
    // timeout, then stream the output bytes as fixed-size `Data` frames.
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;
    write_pack_data_frames(&mut std::io::Cursor::new(bytes), stream, buffer_timeout).await?;
    Ok(())
}

/// Reassemble the `Data`/`End` frames of an accepted consensus-output sync
/// exchange into a contiguous byte vector, bounded to `max_bytes`.
///
/// The caller has already read the opening [`SyncFrame::Ack`]; this reads the
/// `Data`/`End` stream through [`sync_pack_reader`] (so an [`SyncFrame::Err`] or an
/// out-of-place frame surfaces as a read error). Each frame is bounded by the
/// per-frame [`EPOCH_PACK_FRAME_TIMEOUT`] inside the reader (the analog of the
/// legacy per-read timeout), and a stream exceeding `max_bytes` is rejected with
/// [`std::io::ErrorKind::InvalidData`] rather than silently truncated, matching the
/// legacy size guard.
pub(crate) async fn read_sync_consensus_output<S>(
    stream: S,
    max_bytes: usize,
) -> std::io::Result<Vec<u8>>
where
    S: FuturesAsyncRead + Unpin + Send + 'static,
{
    let mut out = Vec::new();
    // read one byte past the cap so an over-cap stream is detected here rather than
    // silently truncated by `take`.
    sync_pack_reader(stream).take(max_bytes as u64 + 1).read_to_end(&mut out).await?;
    (out.len() <= max_bytes).then_some(out).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "consensus output stream exceeded maximum size",
        )
    })
}

/// Serve an accepted missing-certificates sync exchange over `stream`.
///
/// Writes [`SyncFrame::Ack`], then streams the certificates yielded by `certs` as
/// batched [`SyncFrame::Data`] frames (each one BCS-encoded `Vec<Certificate>`
/// targeting [`SYNC_CERT_BATCH_TARGET_SIZE`]), and ends with [`SyncFrame::End`].
///
/// Unlike the legacy request-response reply there is no aggregate cap declared by
/// the requester: the responder streams until `certs` is exhausted, its iterator
/// stops yielding (the streaming collector's processing-time limit), or the honest
/// byte cap [`MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES`] is reached. Each frame write
/// is bounded by `buffer_timeout` so a peer that stops reading cannot pin the
/// responder task. The caller has already admitted the exchange against the
/// concurrency caps and read the opening request frame.
///
/// A storage read error yielded by `certs` aborts with that error; the caller then
/// signals [`SyncFrame::Err`]. Certificates already written stay valid on the wire
/// because the requester only commits the response on [`SyncFrame::End`].
pub(crate) async fn send_sync_certificates_over_stream<S, I>(
    stream: &mut S,
    certs: I,
    buffer_timeout: Duration,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
    I: IntoIterator<Item = PrimaryNetworkResult<Certificate>>,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // accept and flush immediately so the requester sees the `Ack` within its ack timeout
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_CERT_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;

    // Fold the certificates into target-sized `Data` frames: each step appends the
    // certificate to the in-progress batch and flushes a frame once the batch reaches
    // `SYNC_CERT_BATCH_TARGET_SIZE`; `finish` then writes the trailing partial batch
    // and the closing `End`.
    //
    // `scan` enforces the honest response cap on the cumulative encoded size, ending
    // the stream once `MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES` is reached, including the
    // certificate that crosses it, matching the requester's one-frame acceptance
    // margin. Bounding the stream rather than the fold stops the fold from draining the
    // time-bounded collector past what it will send (at the cost of one boundary read).
    // A storage error yielded by `certs` short-circuits the fold and propagates so the
    // caller signals `SyncFrame::Err`.
    futures::stream::iter(certs)
        .map(|cert| {
            cert.and_then(|cert| encoded_size(&cert).map(|size| (size, cert)).map_err(Into::into))
        })
        .scan(0usize, |sent, sized| {
            let within_cap = *sent < MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES;
            futures::future::ready(within_cap.then(|| sized.inspect(|(size, _)| *sent += *size)))
        })
        .try_fold(
            CertBatch::new(stream, encode_buffer, compressed_buffer, buffer_timeout),
            |batch, (size, cert)| batch.push(cert, size),
        )
        .await?
        .finish()
        .await
}

/// Encode `batch` into one [`SyncFrame::Data`] frame (a BCS `Vec<Certificate>`) and
/// write it, bounded by `buffer_timeout`. Serializing the slice by reference
/// produces the same bytes the requester decodes as a `Vec<Certificate>`.
async fn write_cert_batch_frame<S>(
    stream: &mut S,
    batch: &[Certificate],
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    write_one_frame(
        stream,
        &SyncFrame::Data(encode(&batch)),
        encode_buffer,
        compressed_buffer,
        MAX_SYNC_CERT_FRAME_SIZE,
        buffer_timeout,
    )
    .await
}

/// The in-progress state threaded through [`send_sync_certificates_over_stream`]'s
/// fold: the certificates gathered for the next [`SyncFrame::Data`] frame and the
/// reusable encode buffers, carried alongside the stream so each fold step can flush
/// a full frame in place.
struct CertBatch<'s, S> {
    stream: &'s mut S,
    encode_buffer: Vec<u8>,
    compressed_buffer: Vec<u8>,
    batch: Vec<Certificate>,
    batch_bytes: usize,
    buffer_timeout: Duration,
}

impl<'s, S> CertBatch<'s, S>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    fn new(
        stream: &'s mut S,
        encode_buffer: Vec<u8>,
        compressed_buffer: Vec<u8>,
        buffer_timeout: Duration,
    ) -> Self {
        Self {
            stream,
            encode_buffer,
            compressed_buffer,
            batch: Vec::new(),
            batch_bytes: 0,
            buffer_timeout,
        }
    }

    /// Append one certificate (encoded size `size`), flushing a full
    /// [`SyncFrame::Data`] frame once the batch reaches [`SYNC_CERT_BATCH_TARGET_SIZE`].
    async fn push(mut self, cert: Certificate, size: usize) -> PrimaryNetworkResult<Self> {
        self.batch.push(cert);
        self.batch_bytes += size;
        match () {
            () if self.batch_bytes >= SYNC_CERT_BATCH_TARGET_SIZE => self.flush().await?,
            () => (),
        }
        Ok(self)
    }

    /// Write the gathered batch as one [`SyncFrame::Data`] frame and reset.
    async fn flush(&mut self) -> std::io::Result<()> {
        write_cert_batch_frame(
            self.stream,
            &self.batch,
            &mut self.encode_buffer,
            &mut self.compressed_buffer,
            self.buffer_timeout,
        )
        .await?;
        self.batch.clear();
        self.batch_bytes = 0;
        Ok(())
    }

    /// Flush the trailing partial batch, if any, then close the exchange with
    /// [`SyncFrame::End`].
    async fn finish(mut self) -> PrimaryNetworkResult<()> {
        match () {
            () if !self.batch.is_empty() => self.flush().await?,
            () => (),
        }
        write_one_frame(
            self.stream,
            &SyncFrame::End,
            &mut self.encode_buffer,
            &mut self.compressed_buffer,
            MAX_SYNC_CERT_FRAME_SIZE,
            self.buffer_timeout,
        )
        .await
        .map_err(Into::into)
    }
}

/// Read the certificate [`SyncFrame::Data`] frames of an accepted
/// missing-certificates exchange into a `Vec`, terminating at [`SyncFrame::End`].
///
/// The caller has already read the opening [`SyncFrame::Ack`]; this reads the
/// `Data`/`End` stream. Each `Data` frame is a BCS-encoded `Vec<Certificate>` — an
/// untrusted peer payload, so it is decoded fallibly. An [`SyncFrame::Err`] frame,
/// any out-of-place frame, a frame larger than [`MAX_SYNC_CERT_FRAME_SIZE`], a
/// malformed payload, a stalled frame, or a response whose total encoded size
/// exceeds [`MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES`] surfaces as an error so the
/// caller drops the exchange and tries another peer. The returned certificates are
/// unverified; the caller validates them.
pub(crate) async fn read_sync_certificates<S>(stream: &mut S) -> std::io::Result<Vec<Certificate>>
where
    S: FuturesAsyncRead + Unpin + Send,
{
    let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // Unfold the `Data`/`End` stream into the payload of each `Data` frame: the
    // generator reads one frame per step (each bounded by `MISSING_CERTS_FRAME_TIMEOUT`),
    // yields the `Data` bytes, ends the stream on `End`, and surfaces an aborted (`Err`)
    // or out-of-place frame as an error that short-circuits the fold below. The stream
    // threads the `&mut` stream and reusable decode buffers through its state so each
    // step reuses them, mirroring the loop's single buffer pair.
    let data_frames = futures::stream::try_unfold(
        (stream, &mut decode_buffer, &mut compressed_buffer),
        |(stream, decode_buffer, compressed_buffer)| async move {
            let frame = tokio::time::timeout(
                MISSING_CERTS_FRAME_TIMEOUT,
                read_frame::<_, PrimarySyncRequest>(
                    stream,
                    decode_buffer,
                    compressed_buffer,
                    MAX_SYNC_CERT_FRAME_SIZE,
                ),
            )
            .await
            .map_err(|_elapsed| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout reading missing certificates sync frame",
                )
            })??;

            match frame {
                SyncFrame::Data(bytes) => {
                    Ok(Some((bytes, (stream, decode_buffer, compressed_buffer))))
                }
                SyncFrame::End => Ok(None),
                SyncFrame::Err(err) => Err(std::io::Error::other(format!(
                    "peer aborted missing certificates sync stream: {err:?}"
                ))),
                // a well-behaved responder never sends these once streaming
                SyncFrame::Ack | SyncFrame::Deny(_) | SyncFrame::Req(_) => {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected sync frame during missing certificates stream",
                    ))
                }
            }
        },
    );

    // Fold the `Data` payloads into the certificate vector: each step decodes one
    // untrusted frame and extends the gathered certificates. The running byte total
    // bounds the cumulative encoded size by `MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES`
    // (counting the frame that crosses it, as the loop's `total_bytes` check did) and
    // short-circuits with an error so the caller drops the exchange and tries another
    // peer.
    data_frames
        .try_fold((Vec::new(), 0usize), |(mut certificates, total_bytes), bytes| async move {
            let total_bytes = total_bytes + bytes.len();
            (total_bytes <= MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES).then_some(()).ok_or_else(
                || std::io::Error::other("missing certificates sync response exceeded size cap"),
            )?;
            let batch: Vec<Certificate> = try_decode(&bytes).map_err(std::io::Error::other)?;
            certificates.extend(batch);
            Ok((certificates, total_bytes))
        })
        .await
        .map(|(certificates, _total_bytes)| certificates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_network_libp2p::SyncFrameError;

    /// Frame `bytes` through [`write_pack_data_frames`] into an in-memory buffer.
    async fn frame_pack(bytes: &[u8]) -> Vec<u8> {
        let mut reader = std::io::Cursor::new(bytes.to_vec());
        let mut out = Vec::new();
        write_pack_data_frames(&mut reader, &mut out, Duration::from_secs(5))
            .await
            .expect("frame pack");
        out
    }

    /// Reassemble framed bytes through [`sync_pack_reader`].
    async fn read_back(framed: Vec<u8>) -> std::io::Result<Vec<u8>> {
        let mut reader = sync_pack_reader(futures::io::Cursor::new(framed));
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await?;
        Ok(out)
    }

    /// The responder's `Data`+`End` framing round-trips through the requester's
    /// reassembly reader across multiple chunk boundaries: the reconstructed pack
    /// bytes are byte-for-byte the original. This is the cross-version wire
    /// contract between an item-6 responder and an item-6 requester.
    #[tokio::test]
    async fn sync_pack_round_trip_multi_chunk() {
        // two full chunks plus a partial one, so the reader spans frame boundaries
        let original: Vec<u8> =
            (0..(SYNC_PACK_CHUNK_SIZE * 2 + 1234)).map(|i| (i % 251) as u8).collect();
        let framed = frame_pack(&original).await;
        let received = read_back(framed).await.expect("read back");
        assert_eq!(received, original);
    }

    /// An empty pack streams only `End`; the reader yields zero bytes cleanly.
    #[tokio::test]
    async fn sync_pack_round_trip_empty() {
        let framed = frame_pack(&[]).await;
        let received = read_back(framed).await.expect("read back");
        assert!(received.is_empty());
    }

    /// An `Err` frame mid-stream surfaces as a read error so `stream_import` aborts.
    #[tokio::test]
    async fn sync_pack_reader_rejects_err_frame() {
        let mut framed = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Data(vec![1, 2, 3]),
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write data");
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write err");
        assert!(read_back(framed).await.is_err(), "an Err frame must surface as a read error");
    }

    /// An out-of-place control frame (e.g. a second `Ack`) is a protocol
    /// violation and surfaces as a read error.
    #[tokio::test]
    async fn sync_pack_reader_rejects_unexpected_frame() {
        let mut framed = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Ack,
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write ack");
        assert!(
            read_back(framed).await.is_err(),
            "an unexpected control frame mid-stream must surface as a read error"
        );
    }

    /// The consensus-output serve writes `Ack` then the exact output bytes as
    /// `Data`+`End`, and `read_sync_consensus_output` reassembles them
    /// byte-for-byte across chunk boundaries. This is the wire contract between an
    /// item-9b responder and an item-9b requester.
    #[tokio::test]
    async fn sync_consensus_output_round_trip() {
        // one full chunk plus a partial one, so the reader spans a frame boundary
        let original: Vec<u8> =
            (0..(SYNC_PACK_CHUNK_SIZE + 777)).map(|i| (i % 251) as u8).collect();
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            Some(original.clone()),
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serve consensus output");

        // consume the opening `Ack`, then reassemble the remaining `Data`/`End` frames
        let mut cursor = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack = read_frame::<_, PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "first frame must be Ack");
        let received = read_sync_consensus_output(cursor, SYNC_PACK_CHUNK_SIZE * 8)
            .await
            .expect("reassemble output");
        assert_eq!(received, original, "reassembled bytes must equal the served output");
    }

    /// An empty served output streams `Ack` then only `End`; the reader yields zero
    /// bytes cleanly (distinct from the `None`/`Deny` unavailable case below).
    #[tokio::test]
    async fn sync_consensus_output_round_trip_empty() {
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            Some(Vec::new()),
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serve empty consensus output");
        let mut cursor = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack = read_frame::<_, PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "first frame must be Ack");
        let received = read_sync_consensus_output(cursor, SYNC_PACK_CHUNK_SIZE * 8)
            .await
            .expect("reassemble output");
        assert!(received.is_empty(), "an empty served output reassembles to zero bytes");
    }

    /// An unavailable output (`None`) is shed with `Deny(Unavailable)` and no `Ack`,
    /// so a sync requester retries another peer immediately.
    #[tokio::test]
    async fn sync_consensus_output_unavailable_denies() {
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            None,
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serving an unavailable output sheds cleanly");
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let frame = read_frame::<_, PrimarySyncRequest>(
            &mut futures::io::Cursor::new(wire),
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read deny");
        assert!(
            matches!(frame, SyncFrame::Deny(DenyReason::Unavailable)),
            "an unavailable output must deny"
        );
    }

    /// A served stream exceeding `max_bytes` is rejected rather than silently
    /// truncated, mirroring the legacy size guard.
    #[tokio::test]
    async fn read_sync_consensus_output_rejects_oversize() {
        let original: Vec<u8> = (0..(SYNC_PACK_CHUNK_SIZE + 10)).map(|i| (i % 251) as u8).collect();
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            Some(original),
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serve consensus output");
        let mut cursor = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        read_frame::<_, PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack");
        // cap below the served size: reassembly must error, not truncate
        assert!(
            read_sync_consensus_output(cursor, SYNC_PACK_CHUNK_SIZE).await.is_err(),
            "an over-cap stream must be rejected"
        );
    }

    /// Serve `certs` through [`send_sync_certificates_over_stream`] (`Ack` + `Data`* +
    /// `End`), consume the opening `Ack`, then reassemble through
    /// [`read_sync_certificates`]. Returns the reconstructed certificates.
    async fn round_trip_certs(certs: Vec<Certificate>) -> std::io::Result<Vec<Certificate>> {
        let mut wire = Vec::new();
        send_sync_certificates_over_stream(
            &mut wire,
            certs.into_iter().map(Ok),
            Duration::from_secs(5),
        )
        .await
        .expect("serve certs");

        let mut reader = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack: SyncFrame<PrimarySyncRequest> =
            read_frame(&mut reader, &mut dec, &mut comp, MAX_SYNC_CERT_FRAME_SIZE)
                .await
                .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "responder opens with Ack");
        read_sync_certificates(&mut reader).await
    }

    /// A handful of certificates round-trip through one `Data` batch frame.
    #[tokio::test]
    async fn sync_certs_round_trip_single_batch() {
        let certs = vec![Certificate::default(), Certificate::default(), Certificate::default()];
        let received = round_trip_certs(certs.clone()).await.expect("round trip");
        assert_eq!(received, certs);
    }

    /// An empty response streams only `Ack` + `End`; the reader yields zero certs cleanly.
    #[tokio::test]
    async fn sync_certs_round_trip_empty() {
        let received = round_trip_certs(Vec::new()).await.expect("round trip");
        assert!(received.is_empty());
    }

    /// Enough certificates to cross the batch-flush threshold, so the response spans
    /// multiple `Data` frames and the reader reassembles across frame boundaries. This
    /// is the cross-version wire contract between an item-7 responder and requester.
    #[tokio::test]
    async fn sync_certs_round_trip_multi_batch() {
        let one = encode(&Certificate::default()).len().max(1);
        // strictly more than two full batches so at least three `Data` frames are sent
        let count = (SYNC_CERT_BATCH_TARGET_SIZE * 2) / one + 5;
        let certs = vec![Certificate::default(); count];
        let received = round_trip_certs(certs.clone()).await.expect("round trip");
        assert_eq!(received.len(), count);
        assert_eq!(received, certs);
    }

    /// An `Err` frame after some `Data` surfaces as a read error so the fetch aborts.
    #[tokio::test]
    async fn read_sync_certificates_rejects_err_frame() {
        let mut wire = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Data(encode(&vec![Certificate::default()])),
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write data");
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write err");
        let mut reader = futures::io::Cursor::new(wire);
        assert!(
            read_sync_certificates(&mut reader).await.is_err(),
            "an Err frame must surface as a read error"
        );
    }

    /// An out-of-place control frame (a stray `Ack`) mid-stream is a protocol violation
    /// and surfaces as a read error.
    #[tokio::test]
    async fn read_sync_certificates_rejects_unexpected_frame() {
        let mut wire = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Ack,
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write stray ack");
        let mut reader = futures::io::Cursor::new(wire);
        assert!(
            read_sync_certificates(&mut reader).await.is_err(),
            "an unexpected control frame mid-stream must surface as a read error"
        );
    }
}
