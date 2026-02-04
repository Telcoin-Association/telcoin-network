//! Stream codec utilities for batch sync.
//!
//! Reuses patterns from tn-network-libp2p's TNCodec for consistent
//! length-prefixed, snappy-compressed BCS encoding.

use super::error::{WorkerNetworkError, WorkerNetworkResult};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use snap::{read::FrameDecoder, write::FrameEncoder};
use std::io::{Read, Write};
use tn_storage::tables::Batches;
use tn_types::{encode_into_buffer, max_batch_size, Batch, Database, B256};

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
    // clear buffers
    decode_buffer.clear();
    compressed_buffer.clear();

    // read 4-byte uncompressed length prefix first
    let mut uncompressed_prefix = [0u8; 4];
    io.read_exact(&mut uncompressed_prefix).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            WorkerNetworkError::StreamClosed
        } else {
            WorkerNetworkError::StdIo(e)
        }
    })?;

    let uncompressed_len = u32::from_le_bytes(uncompressed_prefix) as usize;

    // SECURITY: Validate length before allocation
    //
    //
    //
    //
    //
    //
    //
    // TODO: Use epoch from context when available
    let max_batch_size = max_batch_size(0);
    if uncompressed_len > max_batch_size {
        return Err(WorkerNetworkError::InvalidRequest(format!(
            "uncompressed batch size {uncompressed_len} exceeds max {max_batch_size}"
        )));
    }

    // read 4-byte compressed length prefix next
    let mut compressed_prefix = [0u8; 4];
    io.read_exact(&mut compressed_prefix).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            WorkerNetworkError::StreamClosed
        } else {
            WorkerNetworkError::StdIo(e)
        }
    })?;

    let compressed_len = u32::from_le_bytes(compressed_prefix) as usize;
    let max_compress_len = snap::raw::max_compress_len(uncompressed_len);
    if compressed_len > max_compress_len {
        return Err(WorkerNetworkError::InvalidRequest(format!(
            "compressed batch size {compressed_len} exceeds max {max_compress_len}"
        )));
    }

    // resize buffers to reported size
    decode_buffer.resize(uncompressed_len, 0);
    compressed_buffer.resize(compressed_len, 0);

    // read compressed data
    io.read_exact(compressed_buffer).await?;

    // decompress
    let reader = std::io::Cursor::new(&compressed_buffer);
    let mut decoder = FrameDecoder::new(reader);
    decoder.read_exact(decode_buffer)?;

    // deserialize
    bcs::from_bytes(decode_buffer).map_err(Into::into)
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
    // clear buffers
    encode_buffer.clear();
    compressed_buffer.clear();

    // encode batch
    encode_into_buffer(encode_buffer, batch)?;

    // compress into buffer
    {
        let mut encoder = FrameEncoder::new(&mut *compressed_buffer);
        encoder.write_all(encode_buffer)?;
        // FrameEncoder flushes on drop
    }

    // write lengths:
    // [4-byte uncompressed][4-byte compressed][compressed data]
    let uncompressed_len = (encode_buffer.len() as u32).to_le_bytes();
    let compressed_len = (compressed_buffer.len() as u32).to_le_bytes();

    io.write_all(&uncompressed_len).await?;
    io.write_all(&compressed_len).await?;
    io.write_all(compressed_buffer).await?;

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
    batch_digests: &[B256],
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
    for chunk in batch_digests.chunks(BATCH_DIGESTS_READ_CHUNK_SIZE) {
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
