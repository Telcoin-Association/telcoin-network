//! Stream codec utilities for batch sync.
//!
//! Reuses patterns from tn-network-libp2p's TNCodec for consistent
//! length-prefixed, snappy-compressed BCS encoding.
//!
//! To start stream, peers exchange `B256` (32-byte) digest at the
//! beginning of the stream.

use crate::batch_fetcher::get_batches_local;

use super::error::{WorkerNetworkError, WorkerNetworkResult};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::collections::BTreeSet;
use tn_network_libp2p::{write_frame, SyncFrame, WorkerSyncRequest};
use tn_storage::consensus::ConsensusChain;
use tn_types::{encode, max_batch_size, Batch, Database, Epoch, B256};

/// Max number of batch digests per chunk.
/// Batch db reads are chunked to limit the amount of batches in memory.
///
/// SAFETY: current max batch size is 1MB.
pub(crate) const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

/// Read a single length-prefixed, snappy-compressed batch from a stream.
///
/// Returns the decoded batch or an error if:
/// - The length prefix exceeds max_batch_size
/// - Decompression fails
/// - BCS deserialization fails
pub(crate) async fn read_batch<T>(
    io: &mut T,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    epoch: Epoch,
) -> WorkerNetworkResult<Batch>
where
    T: AsyncRead + Unpin + Send,
{
    let max_batch_size = max_batch_size(epoch);
    tn_network_libp2p::decode_message(io, decode_buffer, compressed_buffer, max_batch_size)
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                WorkerNetworkError::StreamClosed
            } else {
                WorkerNetworkError::StdIo(e)
            }
        })
}

/// Write a single batch as length-prefixed, snappy-compressed data.
pub(crate) async fn write_batch<T>(
    io: &mut T,
    batch: &Batch,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    epoch: Epoch,
) -> WorkerNetworkResult<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let max_batch_size = max_batch_size(epoch);
    tn_network_libp2p::encode_message(io, batch, encode_buffer, compressed_buffer, max_batch_size)
        .await?;
    Ok(())
}

/// Read batch count header from stream.
pub(crate) async fn read_chunk_count<T>(io: &mut T) -> WorkerNetworkResult<u32>
where
    T: AsyncRead + Unpin + Send,
{
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            WorkerNetworkError::StreamClosed
        } else {
            WorkerNetworkError::StdIo(e)
        }
    })?;

    let count = u32::from_le_bytes(buf);
    Ok(count)
}

/// Send batches over stream, looking up from database.
pub(crate) async fn send_batches_over_stream<DB, S>(
    stream: &mut S,
    store: &DB,
    consensus_chain: &ConsensusChain,
    batch_digests: &BTreeSet<B256>,
    epoch: Epoch,
) -> WorkerNetworkResult<()>
where
    DB: Database,
    S: AsyncWrite + Unpin + Send,
{
    let max_size = max_batch_size(epoch);

    // allocate reusable buffers
    let mut encode_buffer = Vec::with_capacity(max_size);
    let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

    // loop through all batch digests (chunked)
    let digests: Vec<_> = batch_digests.iter().copied().collect();
    for chunk in digests.chunks(BATCH_DIGESTS_READ_CHUNK_SIZE) {
        // look up batches from db
        let batches: Vec<_> = get_batches_local(epoch, chunk, store, consensus_chain).await?;

        // write batch count for this chunk
        let chunk_size = batches.len() as u32;
        stream.write_all(&chunk_size.to_le_bytes()).await?;

        // write each batch
        for batch in &batches {
            write_batch(stream, batch, &mut encode_buffer, &mut compressed_buffer, epoch).await?;
        }

        // flush per chunk
        stream.flush().await?;
    }

    Ok(())
}

/// Serve an accepted sync batch exchange over `stream`.
///
/// Writes a [`SyncFrame::Ack`], then one [`SyncFrame::Data`] frame per batch
/// found in storage (looked up in chunks), terminated by a [`SyncFrame::End`].
/// Each `Data` payload is the BCS-encoded [`Batch`]; the frame layer adds its own
/// length prefix and compression, bounded by `max_frame`.
///
/// Mirrors [`send_batches_over_stream`]'s partial-DB behaviour: only batches
/// present in storage are streamed, so a requester tolerant of partial results
/// fetches the rest elsewhere. The caller has already admitted the exchange
/// against the concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_batches_over_stream<DB, S>(
    stream: &mut S,
    store: &DB,
    consensus_chain: &ConsensusChain,
    batch_digests: &BTreeSet<B256>,
    epoch: Epoch,
    max_frame: usize,
) -> WorkerNetworkResult<()>
where
    DB: Database,
    S: AsyncWrite + Unpin + Send,
{
    let max_size = max_batch_size(epoch);

    // reusable buffers for the frame layer
    let mut encode_buffer = Vec::with_capacity(max_size);
    let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

    // accept the exchange before streaming any data, flushing so the requester
    // receives the `Ack` within its ack timeout even when the first chunk's DB
    // reads are slow or no batches match (an empty response flushes only at `End`)
    write_frame(
        stream,
        &SyncFrame::<WorkerSyncRequest>::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        max_frame,
    )
    .await?;
    stream.flush().await?;

    // stream each found batch as its own Data frame (chunked DB reads)
    let digests: Vec<_> = batch_digests.iter().copied().collect();
    for chunk in digests.chunks(BATCH_DIGESTS_READ_CHUNK_SIZE) {
        let batches: Vec<_> = get_batches_local(epoch, chunk, store, consensus_chain).await?;
        for batch in &batches {
            let frame = SyncFrame::<WorkerSyncRequest>::Data(encode(batch));
            write_frame(stream, &frame, &mut encode_buffer, &mut compressed_buffer, max_frame)
                .await?;
        }

        // flush per chunk
        stream.flush().await?;
    }

    // orderly end of the response stream
    write_frame(
        stream,
        &SyncFrame::<WorkerSyncRequest>::End,
        &mut encode_buffer,
        &mut compressed_buffer,
        max_frame,
    )
    .await?;
    stream.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        test_utils::{create_test_batches, setup_batch_db},
        WorkerNetworkHandle,
    };

    use super::*;
    use futures::io::Cursor;
    use snap::read::FrameDecoder;
    use std::io::Read;
    use tempfile::TempDir;
    use tn_network_libp2p::read_frame;
    use tn_types::{max_batch_size, Committee, TaskManager};

    /// Read the opening [`SyncFrame::Ack`] a sync responder writes, asserting it
    /// is in fact an `Ack`, leaving the cursor positioned at the first `Data`
    /// frame for [`WorkerNetworkHandle::read_sync_batches`].
    async fn read_sync_ack(cursor: &mut Cursor<Vec<u8>>, max_frame: usize) {
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let first = read_frame::<_, WorkerSyncRequest>(cursor, &mut dec, &mut comp, max_frame)
            .await
            .expect("read ack frame");
        assert!(matches!(first, SyncFrame::Ack), "responder opens with Ack");
    }

    /// Helper to write batch to buffer and return it
    async fn encode_batch_to_vec(batch: &Batch) -> Vec<u8> {
        let max_size = max_batch_size(0);
        let mut output = Vec::new();
        let mut encode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer, 0)
            .await
            .expect("write batch");
        output
    }

    /// An [`AsyncRead`] that serves a fixed prefix of bytes and then parks
    /// forever (`Poll::Pending`) without ever closing the stream. Models a peer
    /// that streams one complete chunk and then stalls before the next
    /// chunk-count header, the GHSA-hh6m-5822-6w25 inter-chunk stall.
    struct StallAfterPrefix {
        data: Vec<u8>,
        pos: usize,
    }

    impl futures::AsyncRead for StallAfterPrefix {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if this.pos >= this.data.len() {
                // prefix exhausted: never make progress, never signal EOF
                return std::task::Poll::Pending;
            }
            let remaining = &this.data[this.pos..];
            let n = remaining.len().min(buf.len());
            buf[..n].copy_from_slice(&remaining[..n]);
            this.pos += n;
            std::task::Poll::Ready(Ok(n))
        }
    }

    /// Regression for GHSA-hh6m-5822-6w25: a peer that streams one complete
    /// chunk and then stalls before the next chunk-count header must not park
    /// the requester forever. The inter-chunk count read is now timeout-bounded,
    /// so the reader returns [`NetworkError::Timeout`] instead of hanging. The
    /// paused clock advances to the timeout deterministically, so the test does
    /// not wait real time.
    #[tokio::test(start_paused = true)]
    async fn test_read_and_validate_times_out_on_inter_chunk_stall() {
        use tn_network_libp2p::error::NetworkError;

        // one batch, so the responder emits exactly one chunk: [count=1][batch].
        let batches = create_test_batches(1);
        let db = setup_batch_db(&batches);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();

        // serialize that single chunk, then never send more and never close it.
        let mut prefix = Vec::new();
        send_batches_over_stream(&mut prefix, &db, &consensus_chain, &digests, 0)
            .await
            .expect("send batches");
        let mut stream = StallAfterPrefix { data: prefix, pos: 0 };

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        // Reads chunk 0 immediately, then parks at the chunk-1 count read. The
        // stalling reader never registers a waker, so it is the inter-chunk
        // timeout's own Sleep that keeps a timer live; under the paused clock the
        // runtime auto-advances to that (nearest) timer, which fires and yields
        // Ok(Err(Timeout)) without waiting real time. The outer 1h guard keeps a
        // future revert from hanging CI.
        //
        // cq2q's whole-exchange BATCH_READ_TOTAL_TIMEOUT (210s) also wraps this
        // read and maps to the same NetworkError::Timeout, so asserting on the
        // error alone would still pass if the inter-chunk fix were reverted to a
        // bare await (the 210s outer bound would fire instead). Pin the elapsed
        // paused time to the inner INTER_CHUNK_STREAM_TIMEOUT (200s) window,
        // strictly below the 210s outer deadline, so a revert fails here rather
        // than passing on the outer bound.
        let start = tokio::time::Instant::now();
        let guarded = tokio::time::timeout(
            std::time::Duration::from_secs(3600),
            handle.read_and_validate_batches_with_timeout(&mut stream, &digests),
        )
        .await;
        let elapsed = start.elapsed();
        assert!(
            matches!(guarded, Ok(Err(NetworkError::Timeout))),
            "inter-chunk stall must time out (not hang), got {guarded:?}"
        );
        assert!(
            elapsed >= std::time::Duration::from_secs(200)
                && elapsed < std::time::Duration::from_secs(210),
            "the 200s inter-chunk bound (not the 210s whole-exchange deadline) must fire; \
             elapsed {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_write_read_batch_roundtrip() {
        let batch =
            Batch { transactions: vec![vec![1, 2, 3], vec![4, 5, 6]], ..Default::default() };

        // write batch
        let encoded = encode_batch_to_vec(&batch).await;

        // read batch back
        let max_size = max_batch_size(0);
        let mut cursor = Cursor::new(encoded);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0)
            .await
            .expect("read batch");

        assert_eq!(batch, decoded);
    }

    #[tokio::test]
    async fn test_write_read_empty_batch_roundtrip() {
        let batch = Batch::default();

        // write batch
        let encoded = encode_batch_to_vec(&batch).await;

        // read batch back
        let max_size = max_batch_size(0);
        let mut cursor = Cursor::new(encoded);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0)
            .await
            .expect("read batch");

        assert_eq!(batch, decoded);
    }

    #[tokio::test]
    async fn test_write_read_multiple_batches() {
        let batches = vec![
            Batch { transactions: vec![vec![1, 2, 3]], ..Default::default() },
            Batch { transactions: vec![vec![4, 5, 6]], ..Default::default() },
            Batch { transactions: vec![vec![7, 8, 9]], ..Default::default() },
        ];

        // write all batches to buffer
        let max_size = max_batch_size(0);
        let mut output = Vec::new();
        let mut encode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        for batch in &batches {
            write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer, 0)
                .await
                .expect("write batch");
        }

        // read all batches back
        let mut cursor = Cursor::new(output);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        for expected in &batches {
            let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0)
                .await
                .expect("read batch");
            assert_eq!(*expected, decoded);
        }
    }

    #[tokio::test]
    async fn test_read_batch_stream_closed() {
        // empty buffer - should fail with StreamClosed
        let max_size = max_batch_size(0);
        let mut cursor = Cursor::new(Vec::new());
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let result = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0).await;
        assert!(matches!(result, Err(WorkerNetworkError::StreamClosed)));
    }

    #[tokio::test]
    async fn test_read_batch_exceeds_max_size() {
        // Create a buffer with an oversized length prefix
        let max_size = max_batch_size(0);
        let oversized_len = (max_size + 1) as u32;
        let mut buffer = oversized_len.to_le_bytes().to_vec();
        // Add a dummy compressed length
        buffer.extend_from_slice(&100u32.to_le_bytes());

        let mut cursor = Cursor::new(buffer);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let result = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0).await;
        assert!(matches!(result, Err(WorkerNetworkError::StdIo(_))));
    }

    #[tokio::test]
    async fn test_read_chunk_count() {
        let count = 42u32;
        let buffer = count.to_le_bytes().to_vec();
        let mut cursor = Cursor::new(buffer);

        let result = read_chunk_count(&mut cursor).await.expect("read chunk count");
        assert_eq!(result, count);
    }

    #[tokio::test]
    async fn test_read_chunk_count_stream_closed() {
        let mut cursor = Cursor::new(Vec::new());
        let result = read_chunk_count(&mut cursor).await;
        assert!(matches!(result, Err(WorkerNetworkError::StreamClosed)));
    }

    #[tokio::test]
    async fn test_wire_format_structure() {
        // Verify the wire format is: [4-byte uncompressed len][4-byte compressed len][compressed
        // data]
        let batch = Batch { transactions: vec![vec![1, 2, 3]], ..Default::default() };
        let encoded = encode_batch_to_vec(&batch).await;

        // Read the length prefixes
        let uncompressed_len = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
        let compressed_len = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;

        // Verify total length
        assert_eq!(encoded.len(), 8 + compressed_len);

        // Verify we can decompress the data
        let compressed_data = &encoded[8..];
        assert_eq!(compressed_data.len(), compressed_len);

        // Decompress and verify size matches
        let mut decoder = FrameDecoder::new(std::io::Cursor::new(compressed_data));
        let mut decompressed = vec![0u8; uncompressed_len];
        decoder.read_exact(&mut decompressed).expect("decompress");

        // Verify BCS decoding works
        let decoded: Batch = bcs::from_bytes(&decompressed).expect("bcs decode");
        assert_eq!(batch, decoded);
    }

    #[tokio::test]
    async fn test_send_batches_over_stream_roundtrip() {
        let batches = create_test_batches(3);
        let db = setup_batch_db(&batches);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");

        // collect digests
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();

        // send batches over a Vec<u8> buffer
        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0)
            .await
            .expect("send batches");

        // read back: chunk_count then each batch
        let mut cursor = Cursor::new(output);
        let chunk_count = read_chunk_count(&mut cursor).await.expect("read count");
        assert_eq!(chunk_count as usize, batches.len());

        let max_size = max_batch_size(0);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let mut received = Vec::new();
        for _ in 0..chunk_count {
            let batch = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer, 0)
                .await
                .expect("read batch");
            received.push(batch);
        }

        // verify all batches present
        let received_digests: BTreeSet<B256> = received.iter().map(|b| b.digest()).collect();
        assert_eq!(received_digests, digests);
    }

    #[tokio::test]
    async fn test_send_batches_over_stream_partial_db() {
        let batches = create_test_batches(3);
        // only insert first 2 batches into DB
        let db = setup_batch_db(&batches[..2]);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        consensus_chain.persist_current().await.expect("clean open");

        // request all 3 digests
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();

        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0)
            .await
            .expect("send batches");

        // chunk count should reflect only found batches (2)
        let mut cursor = Cursor::new(output);
        let chunk_count = read_chunk_count(&mut cursor).await.expect("read count");
        assert_eq!(chunk_count, 2);
    }

    #[tokio::test]
    async fn test_send_batches_over_stream_empty_digests() {
        let db = setup_batch_db(&[]);
        let digests: BTreeSet<B256> = BTreeSet::new();
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");

        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0)
            .await
            .expect("send batches");

        // empty digests → no output (no chunks written)
        assert!(output.is_empty());
    }

    /// Test that send/receive works correctly with >200 batches,
    /// which produces multiple chunks (BATCH_DIGESTS_READ_CHUNK_SIZE = 200).
    #[tokio::test]
    async fn test_send_receive_multi_chunk_roundtrip() {
        // 250 batches → 2 chunks (200 + 50)
        let batch_count = BATCH_DIGESTS_READ_CHUNK_SIZE + 50;
        let batches = create_test_batches(batch_count);
        let db = setup_batch_db(&batches);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");

        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();
        assert_eq!(digests.len(), batch_count);

        // send batches (will chunk into 200 + 50)
        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0)
            .await
            .expect("send batches");

        // read back using the handle's multi-chunk reader
        let mut cursor = Cursor::new(output);
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let result = handle
            .read_and_validate_batches_with_timeout(&mut cursor, &digests)
            .await
            .expect("should read all chunks");

        assert_eq!(result.len(), batch_count);
        let received_digests: BTreeSet<B256> = result.iter().map(|(d, _)| *d).collect();
        assert_eq!(received_digests, digests);
    }

    /// A peer that answers with a run of zero-count chunk headers exceeding the
    /// request's chunk span must be rejected, not spun on (regression for the
    /// zero-count spin, GHSA-cq2q-p534-cmmg). `read_and_validate_batches_with_timeout`
    /// caps the number of chunk headers at `ceil(requested / 200)`.
    ///
    /// The input is a FINITE buffer of zero-count headers so the test terminates
    /// deterministically under either behaviour: with the cap it errors at the
    /// first header past the span; if the cap were ever removed the reader would
    /// instead drain the buffer, hit EOF, and return `Ok` — failing this assertion
    /// cleanly rather than hanging. (An infinite reader could not test this: a
    /// no-progress busy-loop never yields, so no `tokio::time::timeout` guard could
    /// fire to catch a regression — it would hang the test instead.)
    #[tokio::test]
    async fn test_read_and_validate_rejects_zero_count_flood() {
        // one digest -> chunk span of 1, so the second zero-count header is already
        // one past the cap. Send several so a removed cap would still terminate (EOF).
        let digests: BTreeSet<B256> = create_test_batches(1).iter().map(|b| b.digest()).collect();
        assert!(!digests.is_empty());
        let zero_count_headers = vec![0u8; 8 * std::mem::size_of::<u32>()];
        let mut cursor = Cursor::new(zero_count_headers);

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let outcome = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;

        assert!(
            outcome.is_err(),
            "a run of zero-count headers past the chunk-span cap must error, got {outcome:?}"
        );
    }

    /// The sync responder's `Ack`+`Data`+`End` framing round-trips through the
    /// requester's `read_sync_batches` reader: every served batch is received,
    /// validated, and matched to its digest. This is the cross-version contract
    /// between an item-5 responder and an item-5 requester.
    #[tokio::test]
    async fn test_send_receive_sync_roundtrip() {
        let batches = create_test_batches(3);
        let db = setup_batch_db(&batches);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();
        let max_frame = crate::network::handle::max_sync_frame_size(0);

        // responder serializes Ack + one Data frame per batch + End
        let mut output = Vec::new();
        send_sync_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0, max_frame)
            .await
            .expect("serve sync batches");

        // requester reads the opening Ack, then the Data/End stream
        let mut cursor = Cursor::new(output);
        read_sync_ack(&mut cursor, max_frame).await;

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let result =
            handle.read_sync_batches(&mut cursor, &digests).await.expect("read sync batches");

        assert_eq!(result.len(), batches.len());
        let received: BTreeSet<B256> = result.iter().map(|(d, _)| *d).collect();
        assert_eq!(received, digests);
    }

    /// A responder that holds only a subset of the requested batches serves those
    /// it has and still ends the stream cleanly; the requester accepts the
    /// partial result without error (mirrors the legacy partial-DB behaviour).
    #[tokio::test]
    async fn test_send_receive_sync_partial_db() {
        let batches = create_test_batches(3);
        // only the first two batches are in the DB
        let db = setup_batch_db(&batches[..2]);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        consensus_chain.persist_current().await.expect("clean open");

        // request all three digests
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();
        let max_frame = crate::network::handle::max_sync_frame_size(0);

        let mut output = Vec::new();
        send_sync_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0, max_frame)
            .await
            .expect("serve sync batches");

        let mut cursor = Cursor::new(output);
        read_sync_ack(&mut cursor, max_frame).await;

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let result =
            handle.read_sync_batches(&mut cursor, &digests).await.expect("read sync batches");

        // only the two batches present in the DB are served
        let expected: BTreeSet<B256> = batches[..2].iter().map(|b| b.digest()).collect();
        let received: BTreeSet<B256> = result.iter().map(|(d, _)| *d).collect();
        assert_eq!(received, expected);
    }

    /// Multi-chunk sync serve (>200 batches): the responder emits one Data frame
    /// per batch across chunk boundaries and the requester reassembles them all.
    #[tokio::test]
    async fn test_send_receive_sync_multi_chunk() {
        let batch_count = BATCH_DIGESTS_READ_CHUNK_SIZE + 50;
        let batches = create_test_batches(batch_count);
        let db = setup_batch_db(&batches);
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        let digests: BTreeSet<B256> = batches.iter().map(|b| b.digest()).collect();
        assert_eq!(digests.len(), batch_count);
        let max_frame = crate::network::handle::max_sync_frame_size(0);

        let mut output = Vec::new();
        send_sync_batches_over_stream(&mut output, &db, &consensus_chain, &digests, 0, max_frame)
            .await
            .expect("serve sync batches");

        let mut cursor = Cursor::new(output);
        read_sync_ack(&mut cursor, max_frame).await;

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let result =
            handle.read_sync_batches(&mut cursor, &digests).await.expect("read sync batches");

        assert_eq!(result.len(), batch_count);
        let received: BTreeSet<B256> = result.iter().map(|(d, _)| *d).collect();
        assert_eq!(received, digests);
    }
}
