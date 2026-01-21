//! Stream codec for encoding/decoding framed messages.
//!
//! Reuses the snappy compression and BCS encoding from the existing TNCodec,
//! but with a different framing format suitable for multiplexed streams.

use super::protocol::{FrameHeader, InvalidMessageType, FRAME_HEADER_SIZE};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use snap::read::FrameDecoder;
use std::io::{Read as _, Write as _};

/// Maximum payload size for a single frame (default: 10 MiB).
/// This is a safety limit to prevent memory exhaustion from malicious peers.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 10 * 1024 * 1024;

/// Codec for reading and writing framed messages on streams.
///
/// Uses the same compression (snappy) and serialization (BCS) as TNCodec,
/// but with a different header format that includes request IDs for correlation.
#[derive(Debug)]
pub struct StreamCodec {
    /// Buffer for compressed data during encoding/decoding.
    compress_buffer: Vec<u8>,
    /// Buffer for uncompressed payload data.
    payload_buffer: Vec<u8>,
    /// Maximum allowed payload size.
    max_frame_size: usize,
}

impl StreamCodec {
    /// Create a new stream codec with the given maximum frame size.
    pub fn new(max_frame_size: usize) -> Self {
        let max_compress_len = snap::raw::max_compress_len(max_frame_size);
        Self {
            compress_buffer: Vec::with_capacity(max_compress_len),
            payload_buffer: Vec::with_capacity(max_frame_size),
            max_frame_size,
        }
    }

    /// Create a codec with default settings.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_FRAME_SIZE)
    }

    /// Read a frame header from the stream.
    pub async fn read_header<R>(&mut self, reader: &mut R) -> std::io::Result<FrameHeader>
    where
        R: AsyncRead + Unpin,
    {
        let mut header_buf = [0u8; FRAME_HEADER_SIZE];
        reader.read_exact(&mut header_buf).await?;

        FrameHeader::decode(&header_buf).map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Read a complete frame (header + payload) from the stream.
    ///
    /// Returns the header and the decompressed payload bytes.
    pub async fn read_frame<R>(&mut self, reader: &mut R) -> std::io::Result<(FrameHeader, Vec<u8>)>
    where
        R: AsyncRead + Unpin,
    {
        let header = self.read_header(reader).await?;

        // Validate payload size before allocating
        if header.payload_len as usize > self.max_frame_size {
            return Err(std::io::Error::other(format!(
                "frame payload too large: {} > {}",
                header.payload_len, self.max_frame_size
            )));
        }

        // Read the compressed payload
        self.compress_buffer.clear();
        self.compress_buffer.resize(header.payload_len as usize, 0);
        reader.read_exact(&mut self.compress_buffer).await?;

        // Decompress the payload
        let payload = self.decompress(&self.compress_buffer)?;

        Ok((header, payload))
    }

    /// Read raw bytes from the stream without decompression.
    ///
    /// Used for streaming pack file data that doesn't use our framing.
    pub async fn read_raw<R>(&mut self, reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize>
    where
        R: AsyncRead + Unpin,
    {
        reader.read(buf).await
    }

    /// Read exact number of raw bytes from the stream.
    pub async fn read_raw_exact<R>(&mut self, reader: &mut R, buf: &mut [u8]) -> std::io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        reader.read_exact(buf).await
    }

    /// Write a frame (header + compressed payload) to the stream.
    pub async fn write_frame<W>(
        &mut self,
        writer: &mut W,
        header: &FrameHeader,
        payload: &[u8],
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        // Compress the payload
        self.compress_buffer.clear();
        self.compress(payload)?;

        // Create header with actual compressed size
        let mut actual_header = header.clone();
        actual_header.payload_len = self.compress_buffer.len() as u32;

        // Write header
        let header_bytes = actual_header.encode();
        writer.write_all(&header_bytes).await?;

        // Write compressed payload
        writer.write_all(&self.compress_buffer).await?;

        Ok(())
    }

    /// Write a frame header only (no payload).
    pub async fn write_header<W>(
        &mut self,
        writer: &mut W,
        header: &FrameHeader,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let header_bytes = header.encode();
        writer.write_all(&header_bytes).await
    }

    /// Write raw bytes to the stream without compression.
    ///
    /// Used for streaming pack file data that doesn't use our framing.
    pub async fn write_raw<W>(&mut self, writer: &mut W, data: &[u8]) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(data).await
    }

    /// Flush the writer.
    pub async fn flush<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.flush().await
    }

    /// Encode a serializable value into compressed bytes.
    pub fn encode_payload<T: serde::Serialize>(&mut self, value: &T) -> std::io::Result<Vec<u8>> {
        // Serialize with BCS
        self.payload_buffer.clear();
        tn_types::encode_into_buffer(&mut self.payload_buffer, value)
            .map_err(|e| std::io::Error::other(format!("serialization error: {e}")))?;

        // Compress using inline encoder to avoid borrow issues
        self.compress_buffer.clear();
        {
            let mut encoder = snap::write::FrameEncoder::new(&mut self.compress_buffer);
            encoder.write_all(&self.payload_buffer)?;
            encoder.flush()?;
        }

        Ok(self.compress_buffer.clone())
    }

    /// Decode a decompressed payload into a value.
    pub fn decode_payload<T: serde::de::DeserializeOwned>(
        &self,
        payload: &[u8],
    ) -> std::io::Result<T> {
        tn_types::try_decode(payload)
            .map_err(|e| std::io::Error::other(format!("deserialization error: {e}")))
    }

    /// Compress data using snappy frame encoding.
    fn compress(&mut self, data: &[u8]) -> std::io::Result<()> {
        let mut encoder = snap::write::FrameEncoder::new(&mut self.compress_buffer);
        encoder.write_all(data)?;
        encoder.flush()?;
        Ok(())
    }

    /// Decompress snappy frame-encoded data.
    fn decompress(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        let cursor = std::io::Cursor::new(data);
        let mut decoder = FrameDecoder::new(cursor);
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    }
}

impl Default for StreamCodec {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Clone for StreamCodec {
    fn clone(&self) -> Self {
        // Don't clone the buffers, just create new ones with same capacity
        Self::new(self.max_frame_size)
    }
}

/// Error type for codec operations.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// IO error during read/write.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid frame header.
    #[error("invalid frame header: {0}")]
    InvalidHeader(#[from] InvalidMessageType),

    /// Payload too large.
    #[error("payload too large: {size} > {max}")]
    PayloadTooLarge {
        /// Actual payload size.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::protocol::{FrameFlags, StreamMessageType};

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let mut codec = StreamCodec::with_defaults();

        // Create a test payload
        let payload = b"Hello, World! This is a test message.";

        // Create header
        let header = FrameHeader::new(
            42,
            StreamMessageType::TypedRequest,
            FrameFlags::NONE,
            0, // Will be set during write
        );

        // Write to buffer
        let mut buffer = Vec::new();
        codec.write_frame(&mut buffer, &header, payload).await.unwrap();

        // Read back
        let mut cursor = std::io::Cursor::new(buffer);
        let (read_header, read_payload) = codec.read_frame(&mut cursor).await.unwrap();

        assert_eq!(read_header.request_id, 42);
        assert_eq!(read_header.message_type, StreamMessageType::TypedRequest);
        assert_eq!(read_payload, payload);
    }

    #[tokio::test]
    async fn test_payload_encoding() {
        let mut codec = StreamCodec::with_defaults();

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct TestStruct {
            value: u64,
            name: String,
        }

        let original = TestStruct { value: 12345, name: "test".to_string() };

        // Encode
        let encoded = codec.encode_payload(&original).unwrap();

        // Decode (need to decompress first)
        let decompressed = codec.decompress(&encoded).unwrap();
        let decoded: TestStruct = codec.decode_payload(&decompressed).unwrap();

        assert_eq!(decoded, original);
    }

    #[tokio::test]
    async fn test_max_frame_size_enforcement() {
        let mut codec = StreamCodec::new(100); // Small max size

        // Create a header claiming a large payload
        let header = FrameHeader::new(1, StreamMessageType::TypedRequest, FrameFlags::NONE, 1000);

        // Write the header only
        let mut buffer = Vec::new();
        codec.write_header(&mut buffer, &header).await.unwrap();

        // Try to read - should fail due to size
        let mut cursor = std::io::Cursor::new(buffer);
        let result = codec.read_frame(&mut cursor).await;

        assert!(result.is_err());
    }
}
