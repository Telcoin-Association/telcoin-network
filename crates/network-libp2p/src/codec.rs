//! Codec for encoding/decoding consensus network messages.

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use serde::{de::DeserializeOwned, Serialize};
use snap::read::FrameDecoder;
use std::{
    fmt,
    io::{Read as _, Write as _},
    marker::PhantomData,
};
use tn_types::encode_into_buffer;

#[cfg(test)]
#[path = "tests/codec_tests.rs"]
mod codec_tests;

/// Convenience type for all traits implemented for messages used for TN request-response codec.
pub trait TNMessage: Send + Serialize + DeserializeOwned + Clone + fmt::Debug + 'static {}

impl TNMessage for tn_types::Certificate {}

/// The Telcoin Network request/response codec for consensus messages between peers.
///
/// The codec reuses pre-allocated buffers to asynchronously read messages per the libp2p [Codec]
/// trait. All messages include a 4-byte prefix that indicates the message's uncompressed length.
/// Peers use this prefix to safely decompress and decode messages from peers.
///
/// TODO:
/// - handle peer scores when messages are malicious
#[derive(Clone, Debug)]
pub struct TNCodec<Req, Res> {
    /// The fixed-size buffer for compressed messages.
    compressed_buffer: Vec<u8>,
    /// The fixed-size buffer for requests.
    decode_buffer: Vec<u8>,
    /// The maximum size (bytes) for a single message.
    ///
    /// The 4-byte message prefix does not count towards this value.
    max_chunk_size: usize,
    /// Phantom data for codec that indicates network message type.
    _phantom: PhantomData<(Req, Res)>,
}

impl<Req, Res> TNCodec<Req, Res> {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        // create buffer from max possible compression length based on max request size
        let max_compress_len = snap::raw::max_compress_len(max_chunk_size);
        let compressed_buffer = Vec::with_capacity(max_compress_len);
        // allocate capacity for decoding max message size
        let decode_buffer = Vec::with_capacity(max_chunk_size);

        Self {
            compressed_buffer,
            decode_buffer,
            max_chunk_size,
            _phantom: PhantomData::<(Req, Res)>,
        }
    }

    /// Convenience method to keep READ logic DRY.
    ///
    /// This method is used to read requests and responses from peers.
    #[inline]
    pub async fn decode_message<T, M>(&mut self, io: &mut T) -> std::io::Result<M>
    where
        T: AsyncRead + Unpin + Send,
        M: TNMessage,
    {
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // retrieve prefix for uncompressed message length
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix).await?;

        // NOTE: cast u32 to usize is safe
        let length = u32::from_le_bytes(prefix) as usize;

        // ensure message length within bounds
        if length > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "prefix indicates message size is too large",
            ));
        }

        // resize buffer to reported message size
        //
        // NOTE: this should not reallocate
        self.decode_buffer.resize(length, 0);

        // take max possible compression size based on reported length
        // this is used to limit the amount read in case peer used malicious prefix
        //
        // NOTE: usize -> u64 won't lose precision (even on 32bit system)
        let max_compress_len = snap::raw::max_compress_len(length);
        io.take(max_compress_len as u64).read_to_end(&mut self.compressed_buffer).await?;

        // decompress bytes
        let reader = std::io::Cursor::new(&mut self.compressed_buffer);
        let mut snappy_decoder = FrameDecoder::new(reader);
        snappy_decoder.read_exact(&mut self.decode_buffer)?;

        // decode bytes
        bcs::from_bytes(&self.decode_buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Convenience method to keep WRITE logic DRY.
    ///
    /// This method is used to write requests and responses from peers.
    #[inline]
    pub async fn encode_message<T, M>(&mut self, io: &mut T, msg: M) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
        M: TNMessage,
    {
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // encode into allocated buffer
        encode_into_buffer(&mut self.decode_buffer, &msg).map_err(|e| {
            let error = format!("encode into buffer: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, error)
        })?;

        // ensure encoded bytes are within bounds
        if self.decode_buffer.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (self.decode_buffer.len() as u32).to_le_bytes();
        io.write_all(&prefix).await?;

        // compress data using allocated buffer
        let mut encoder = snap::write::FrameEncoder::new(&mut self.compressed_buffer);
        encoder.write_all(&self.decode_buffer)?;
        encoder.flush()?;

        // add compressed bytes to prefix
        io.write_all(encoder.get_ref()).await?;

        Ok(())
    }
}

impl<Req, Res> Default for TNCodec<Req, Res> {
    fn default() -> Self {
        Self::new(MAX_REQUEST_SIZE)
    }
}

/// Max request size in bytes
///
/// batches are capped at 1MB worth of transactions.
/// This should be more than enough for snappy-compressed messages.
///
/// TODO: add the message overhead as the max request size
pub const MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Max gossip size in bytes.
///
/// Gossip messages must be small because peers are likely to receive the same message multiple
/// times. The protocol is optimized for latency, not throughput.
/// See github discussion for more info: https://github.com/libp2p/specs/issues/118#issuecomment-499688869
///
/// The max is based on `Certificate` sizes. Certificate signatures are encoded as roaring bitmaps.
/// Certificates are small and okay to gossip uncompressed:
/// - 3 signatures ~= 0.3kb
/// - 99 signatures ~= 3.5kb
///
/// The max is based on `ConsensusHeader`.
///
/// Consensus headers are created based on subdag commits which can be several rounds deep.
/// More benchmarking is needed, but this should be a safe number for a 4-member committee.
/// - 6 round max commit
/// - 4 certificate max per round
/// - 300 bytes size per empty certificate
/// - 5 batch digests max per certificate (32 bytes)
/// - (6 * 4)(300 + (5 * 32)) = 11,040
pub const MAX_GOSSIP_SIZE: usize = 12_000;

#[async_trait]
impl<Req, Res> Codec for TNCodec<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    type Protocol = StreamProtocol;
    type Request = Req;
    type Response = Res;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, res).await
    }
}
