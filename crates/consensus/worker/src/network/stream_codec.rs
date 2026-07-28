//! Stream codec utilities for batch sync.
//!
//! Reuses patterns from tn-network-libp2p's TNCodec for consistent
//! length-prefixed, snappy-compressed BCS encoding.
//!
//! To start stream, peers exchange `B256` (32-byte) digest at the
//! beginning of the stream.

use crate::batch_fetcher::get_batches_local;

use super::error::WorkerNetworkResult;
use futures::{AsyncWrite, AsyncWriteExt};
use std::collections::BTreeSet;
use tn_network_libp2p::{write_frame, SyncFrame, WorkerSyncRequest};
use tn_storage::consensus::ConsensusChain;
use tn_types::{encode, max_batch_size, Database, Epoch, B256};
// `Batch` is only named by the test-only `write_batch` encoder below.
#[cfg(any(test, feature = "test-utils"))]
use tn_types::Batch;

/// Max number of batch digests per chunk.
/// Batch db reads are chunked to limit the amount of batches in memory.
///
/// SAFETY: current max batch size is 1MB.
pub(crate) const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

/// Write a single batch as length-prefixed, snappy-compressed data.
///
/// Only used by the `test-utils` batch-stream encoder now that the legacy
/// chunked responder is gone; the live sync responder frames batches via
/// [`send_sync_batches_over_stream`].
#[cfg(any(test, feature = "test-utils"))]
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

/// Serve an accepted sync batch exchange over `stream`.
///
/// Writes a [`SyncFrame::Ack`], then one [`SyncFrame::Data`] frame per batch
/// found in storage (looked up in chunks), terminated by a [`SyncFrame::End`].
/// Each `Data` payload is the BCS-encoded [`Batch`]; the frame layer adds its own
/// length prefix and compression, bounded by `max_frame`.
///
/// Only batches present in storage are streamed, so a requester tolerant of
/// partial results fetches the rest elsewhere. The caller has already admitted the
/// exchange against the concurrency caps and read the opening request frame.
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
    use tempfile::TempDir;
    use tn_network_libp2p::read_frame;
    use tn_types::{Committee, TaskManager};

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
