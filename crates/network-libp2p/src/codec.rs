//! Codec for encoding/decoding consensus network messages.

use crate::PeerExchangeMap;
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
pub trait TNMessage:
    Send + Serialize + DeserializeOwned + Clone + fmt::Debug + From<PeerExchangeMap> + 'static
{
}

/// The Telcoin Network request/response codec for consensus messages between peers.
///
/// The codec reuses pre-allocated buffers to asynchronously read messages per the libp2p [Codec]
/// trait. All messages include a 4-byte prefix that indicates the message's uncompressed length.
/// Peers use this prefix to safely decompress and decode messages from peers.
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
    async fn decode_message<T, M>(&mut self, io: &mut T) -> std::io::Result<M>
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
            return Err(std::io::Error::other("prefix indicates message size is too large"));
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
        bcs::from_bytes(&self.decode_buffer).map_err(std::io::Error::other)
    }

    /// Convenience method to keep WRITE logic DRY.
    ///
    /// This method is used to write requests and responses from peers.
    #[inline]
    async fn encode_message<T, M>(&mut self, io: &mut T, msg: M) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
        M: TNMessage,
    {
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // encode into allocated buffer
        encode_into_buffer(&mut self.decode_buffer, &msg).map_err(|e| {
            let error = format!("encode into buffer: {e}");
            std::io::Error::other(error)
        })?;

        // ensure encoded bytes are within bounds
        if self.decode_buffer.len() > self.max_chunk_size {
            return Err(std::io::Error::other("encode data > max_chunk_size"));
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
