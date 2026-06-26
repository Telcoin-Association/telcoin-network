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
const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

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
    use tn_types::{max_batch_size, TaskManager};

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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");

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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");
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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");

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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");

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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");
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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");
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
            ConsensusChain::new(temp_dir.path().to_path_buf()).expect("consensus chain");
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
