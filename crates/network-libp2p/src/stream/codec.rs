// SPDX-License-Identifier: MIT or Apache-2.0
//! Codec for streaming protocol frame/chunk read/write operations.
//!
//! Reuses snappy compression and BCS serialization from the existing TNCodec.

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use serde::{de::DeserializeOwned, Serialize};
use snap::read::FrameDecoder;
use std::io::{Read as _, Write as _};
use tn_types::encode_into_buffer;

/// Codec for reading and writing frames on libp2p streams.
///
/// Wire format:
/// - Request/single response: `| length (4 bytes LE) | compressed_data |`
/// - Bulk response chunks: `| length (4 bytes LE) | compressed_chunk | ... | 0x00000000 |`
///
/// The length prefix indicates uncompressed size. Zero-length (4 zero bytes) marks end of stream.
#[derive(Debug)]
pub struct StreamCodec {
    /// Maximum allowed frame size in bytes (uncompressed).
    max_frame_size: usize,
    /// Reusable buffer for compressed data.
    compress_buf: Vec<u8>,
    /// Reusable buffer for decompressed/decoded data.
    decompress_buf: Vec<u8>,
}

impl StreamCodec {
    /// Create a new stream codec with the given maximum frame size.
    pub fn new(max_frame_size: usize) -> Self {
        let max_compress_len = snap::raw::max_compress_len(max_frame_size);
        Self {
            max_frame_size,
            compress_buf: Vec::with_capacity(max_compress_len),
            decompress_buf: Vec::with_capacity(max_frame_size),
        }
    }

    /// Write a single frame containing a serialized item.
    ///
    /// Used for requests and single-item responses.
    pub async fn write_frame<W, T>(&mut self, writer: &mut W, item: &T) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
        T: Serialize,
    {
        self.compress_buf.clear();
        self.decompress_buf.clear();

        // BCS encode into buffer
        encode_into_buffer(&mut self.decompress_buf, item)
            .map_err(|e| std::io::Error::other(format!("BCS encode failed: {e}")))?;

        // Check size limit
        if self.decompress_buf.len() > self.max_frame_size {
            return Err(std::io::Error::other("frame exceeds max size"));
        }

        // Write length prefix (uncompressed size)
        let prefix = (self.decompress_buf.len() as u32).to_le_bytes();
        writer.write_all(&prefix).await?;

        // Compress and write
        let mut encoder = snap::write::FrameEncoder::new(&mut self.compress_buf);
        encoder.write_all(&self.decompress_buf)?;
        encoder.flush()?;
        writer.write_all(encoder.get_ref()).await?;

        Ok(())
    }

    /// Read a single frame and deserialize it.
    ///
    /// Returns an error if the frame exceeds max size or deserialization fails.
    pub async fn read_frame<R, T>(&mut self, reader: &mut R) -> std::io::Result<T>
    where
        R: AsyncRead + Unpin,
        T: DeserializeOwned,
    {
        self.compress_buf.clear();
        self.decompress_buf.clear();

        // Read length prefix
        let mut prefix = [0u8; 4];
        reader.read_exact(&mut prefix).await?;
        let length = u32::from_le_bytes(prefix) as usize;

        // Validate size
        if length > self.max_frame_size {
            return Err(std::io::Error::other(format!(
                "frame size {length} exceeds max {}",
                self.max_frame_size
            )));
        }

        // Zero length is end marker (not expected in single-frame reads)
        if length == 0 {
            return Err(std::io::Error::other("unexpected end-of-stream marker"));
        }

        // Prepare buffer for decompressed data
        self.decompress_buf.resize(length, 0);

        // Read compressed data (limit by max possible compression size)
        let max_compress_len = snap::raw::max_compress_len(length);
        reader.take(max_compress_len as u64).read_to_end(&mut self.compress_buf).await?;

        // Decompress
        let cursor = std::io::Cursor::new(&self.compress_buf);
        let mut decoder = FrameDecoder::new(cursor);
        decoder.read_exact(&mut self.decompress_buf)?;

        // Deserialize
        bcs::from_bytes(&self.decompress_buf).map_err(std::io::Error::other)
    }

    /// Write a chunk of items for bulk streaming.
    ///
    /// Each chunk is written as: `| length (4 bytes LE) | compressed Vec<T> |`
    pub async fn write_chunk<W, T>(&mut self, writer: &mut W, items: &[T]) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
        T: Serialize + Clone,
    {
        // Serialize Vec<T> as the chunk payload
        self.write_frame(writer, &items.to_vec()).await
    }

    /// Write the end-of-stream marker (4 zero bytes).
    pub async fn write_end<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&[0u8; 4]).await
    }

    /// Read a chunk from a bulk stream.
    ///
    /// Returns `Ok(Some(items))` for a chunk, `Ok(None)` at end of stream.
    pub async fn read_chunk<R, T>(&mut self, reader: &mut R) -> std::io::Result<Option<Vec<T>>>
    where
        R: AsyncRead + Unpin,
        T: DeserializeOwned,
    {
        self.compress_buf.clear();
        self.decompress_buf.clear();

        // Read length prefix
        let mut prefix = [0u8; 4];
        reader.read_exact(&mut prefix).await?;
        let length = u32::from_le_bytes(prefix) as usize;

        // Zero length = end of stream
        if length == 0 {
            return Ok(None);
        }

        // Validate size
        if length > self.max_frame_size {
            return Err(std::io::Error::other(format!(
                "chunk size {length} exceeds max {}",
                self.max_frame_size
            )));
        }

        // Prepare buffer
        self.decompress_buf.resize(length, 0);

        // Read compressed data
        let max_compress_len = snap::raw::max_compress_len(length);
        reader.take(max_compress_len as u64).read_to_end(&mut self.compress_buf).await?;

        // Decompress
        let cursor = std::io::Cursor::new(&self.compress_buf);
        let mut decoder = FrameDecoder::new(cursor);
        decoder.read_exact(&mut self.decompress_buf)?;

        // Deserialize as Vec<T>
        let items: Vec<T> = bcs::from_bytes(&self.decompress_buf).map_err(std::io::Error::other)?;
        Ok(Some(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestMessage {
        id: u64,
        data: Vec<u8>,
    }

    #[tokio::test]
    async fn test_frame_round_trip() {
        let mut codec = StreamCodec::new(1024 * 1024);
        let msg = TestMessage { id: 42, data: vec![1, 2, 3, 4, 5] };

        // Write to buffer
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        codec.write_frame(&mut cursor, &msg).await.unwrap();

        // Read back
        let mut read_cursor = Cursor::new(&buf);
        let decoded: TestMessage = codec.read_frame(&mut read_cursor).await.unwrap();

        assert_eq!(msg, decoded);
    }

    #[tokio::test]
    async fn test_chunk_round_trip() {
        let mut codec = StreamCodec::new(1024 * 1024);
        let items = vec![
            TestMessage { id: 1, data: vec![1] },
            TestMessage { id: 2, data: vec![2] },
            TestMessage { id: 3, data: vec![3] },
        ];

        // Write chunk and end marker
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        codec.write_chunk(&mut cursor, &items).await.unwrap();
        codec.write_end(&mut cursor).await.unwrap();

        // Read back
        let mut read_cursor = Cursor::new(&buf);
        let chunk1: Option<Vec<TestMessage>> = codec.read_chunk(&mut read_cursor).await.unwrap();
        let chunk2: Option<Vec<TestMessage>> = codec.read_chunk(&mut read_cursor).await.unwrap();

        assert_eq!(chunk1, Some(items));
        assert_eq!(chunk2, None);
    }

    #[tokio::test]
    async fn test_max_frame_size_exceeded() {
        let mut codec = StreamCodec::new(10); // Very small limit
        let msg = TestMessage { id: 42, data: vec![0; 100] };

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        let result = codec.write_frame(&mut cursor, &msg).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max size"));
    }
}
