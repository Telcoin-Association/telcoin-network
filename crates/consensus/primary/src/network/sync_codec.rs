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
    AsyncWriteExt as _, TryStreamExt as _,
};
use std::time::Duration;
use tn_network_libp2p::{read_frame, write_frame, DenyReason, PrimarySyncRequest, SyncFrame};
use tn_storage::consensus::ConsensusChain;
use tn_types::{BlsPublicKey, Epoch};
use tokio::io::AsyncRead as TokioAsyncRead;
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

/// Serve an accepted epoch-pack sync exchange over `stream`.
///
/// A full epoch pack is all-or-nothing: if the complete, verifiable pack is not
/// held, the responder sheds with [`SyncFrame::Deny`]`(`[`DenyReason::Unavailable`]`)`
/// so the requester retries another peer immediately instead of waiting out its
/// ack timeout. Otherwise it writes [`SyncFrame::Ack`], streams the pack as
/// [`SyncFrame::Data`] frames, and ends with [`SyncFrame::End`]. Each frame write
/// is bounded by `buffer_timeout` so a peer that stops reading cannot stall the
/// responder task on an unbounded write. The caller has already admitted the
/// exchange against the concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_epoch_pack_over_stream<S>(
    stream: &mut S,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
    buffer_timeout: Duration,
    peer: BlsPublicKey,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // the complete pack is not held: shed cleanly so the requester retries elsewhere
    let Ok(mut epoch_stream) = consensus_chain.get_epoch_stream(epoch).await else {
        debug!(target: "primary::network", %peer, epoch, "epoch pack unavailable; denying sync request");
        write_one_frame(
            stream,
            &SyncFrame::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
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
        buffer_timeout,
    )
    .await?;

    write_pack_data_frames(&mut epoch_stream, stream, buffer_timeout).await?;
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
        buffer_timeout,
    )
    .await
}

/// Write a single frame and flush, bounding both on `buffer_timeout` so a
/// non-reading peer cannot pin the writer on an unbounded write.
async fn write_one_frame<S>(
    stream: &mut S,
    frame: &SyncFrame<PrimarySyncRequest>,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    tokio::time::timeout(buffer_timeout, async {
        write_frame(stream, frame, encode_buffer, compressed_buffer, MAX_SYNC_PACK_FRAME_SIZE)
            .await?;
        stream.flush().await
    })
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout writing epoch pack sync frame")
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

#[cfg(test)]
mod tests {
    use super::*;
    use tn_network_libp2p::SyncFrameError;
    use tokio::io::AsyncReadExt as _;

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
}
