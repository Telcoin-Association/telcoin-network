//! Stream codec utilities for batch sync.
//!
//! Reuses patterns from tn-network-libp2p's TNCodec for consistent
//! length-prefixed, snappy-compressed BCS encoding.
//!
//! To start stream, peers exchange `B256` (32-byte) digest at the
//! beginning of the stream.

use super::error::{WorkerNetworkError, WorkerNetworkResult};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::collections::HashSet;
use tn_storage::tables::Batches;
use tn_types::{max_batch_size, Batch, Database, B256};

// chunk batch digest reads to limit amount of batches in memory
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
) -> WorkerNetworkResult<Batch>
where
    T: AsyncRead + Unpin + Send,
{
    // TODO: Use epoch from context when available
    let max_batch_size = max_batch_size(0);
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
) -> WorkerNetworkResult<()>
where
    T: AsyncWrite + Unpin + Send,
{
    // TODO: Use epoch from context when available
    let max_batch_size = max_batch_size(0);
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

    // TODO:
    //
    // assert chunk count is expected/within limit
    if count as usize > BATCH_DIGESTS_READ_CHUNK_SIZE {
        todo!() // error
    }

    Ok(count)
}

/// Send batches over stream, looking up from database.
pub(crate) async fn send_batches_over_stream<DB, S>(
    stream: &mut S,
    store: &DB,
    batch_digests: &HashSet<B256>,
) -> WorkerNetworkResult<()>
where
    DB: Database,
    S: AsyncWrite + Unpin + Send,
{
    // TODO: Use epoch from context when available
    let max_size = max_batch_size(0);

    // allocate reusable buffers
    let mut encode_buffer = Vec::with_capacity(max_size);
    let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

    // loop through all batch digests (chunked)
    let digests: Vec<_> = batch_digests.iter().copied().collect();
    for chunk in digests.chunks(BATCH_DIGESTS_READ_CHUNK_SIZE) {
        // look up batches from db
        let batches: Vec<_> = store
            .multi_get::<Batches>(chunk.iter())
            .map_err(|e| WorkerNetworkError::Internal(format!("DB error: {e}")))?
            .into_iter()
            .flatten() // removes `None`
            .collect();

        // write batch count for this chunk
        let chunk_size = batches.len() as u32;
        stream.write_all(&chunk_size.to_le_bytes()).await?;

        // write each batch
        for batch in &batches {
            write_batch(stream, batch, &mut encode_buffer, &mut compressed_buffer).await?;
        }

        // flush per chunk
        stream.flush().await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use snap::read::FrameDecoder;
    use std::io::Read;
    use tn_types::max_batch_size;

    /// Helper to write batch to buffer and return it
    async fn encode_batch_to_vec(batch: &Batch) -> Vec<u8> {
        let max_size = max_batch_size(0);
        let mut output = Vec::new();
        let mut encode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer)
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

        let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer)
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

        let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer)
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
            write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer)
                .await
                .expect("write batch");
        }

        // read all batches back
        let mut cursor = Cursor::new(output);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        for expected in &batches {
            let decoded = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer)
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

        let result = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer).await;
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

        let result = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer).await;
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
        use crate::test_utils::{create_test_batches, setup_batch_db};

        let batches = create_test_batches(3);
        let db = setup_batch_db(&batches);

        // collect digests
        let digests: HashSet<B256> = batches.iter().map(|b| b.digest()).collect();

        // send batches over a Vec<u8> buffer
        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &digests).await.expect("send batches");

        // read back: chunk_count then each batch
        let mut cursor = Cursor::new(output);
        let chunk_count = read_chunk_count(&mut cursor).await.expect("read count");
        assert_eq!(chunk_count as usize, batches.len());

        let max_size = max_batch_size(0);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        let mut received = Vec::new();
        for _ in 0..chunk_count {
            let batch = read_batch(&mut cursor, &mut decode_buffer, &mut compressed_buffer)
                .await
                .expect("read batch");
            received.push(batch);
        }

        // verify all batches present (order may differ due to HashSet iteration)
        let received_digests: HashSet<B256> = received.iter().map(|b| b.digest()).collect();
        assert_eq!(received_digests, digests);
    }

    #[tokio::test]
    async fn test_send_batches_over_stream_partial_db() {
        use crate::test_utils::{create_test_batches, setup_batch_db};

        let batches = create_test_batches(3);
        // only insert first 2 batches into DB
        let db = setup_batch_db(&batches[..2]);

        // request all 3 digests
        let digests: HashSet<B256> = batches.iter().map(|b| b.digest()).collect();

        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &digests).await.expect("send batches");

        // chunk count should reflect only found batches (2)
        let mut cursor = Cursor::new(output);
        let chunk_count = read_chunk_count(&mut cursor).await.expect("read count");
        assert_eq!(chunk_count, 2);
    }

    #[tokio::test]
    async fn test_send_batches_over_stream_empty_digests() {
        use crate::test_utils::setup_batch_db;

        let db = setup_batch_db(&[]);
        let digests: HashSet<B256> = HashSet::new();

        let mut output = Vec::new();
        send_batches_over_stream(&mut output, &db, &digests).await.expect("send batches");

        // empty digests → no output (no chunks written)
        assert!(output.is_empty());
    }
}
