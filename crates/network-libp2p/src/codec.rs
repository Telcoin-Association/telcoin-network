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

/// Decode a single length-prefixed, snappy-compressed BCS message from an async reader.
///
/// Wire format: `[4-byte uncompressed_len][4-byte compressed_len][compressed_data]`
///
/// The caller provides reusable buffers to avoid repeated allocation.
pub async fn decode_message<T, M>(
    io: &mut T,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_message_size: usize,
) -> std::io::Result<M>
where
    T: AsyncRead + Unpin + Send,
    M: DeserializeOwned,
{
    // clear buffers
    decode_buffer.clear();
    compressed_buffer.clear();

    // read 4-byte uncompressed length prefix
    let mut uncompressed_prefix = [0u8; 4];
    io.read_exact(&mut uncompressed_prefix).await?;
    let uncompressed_len = u32::from_le_bytes(uncompressed_prefix) as usize;

    // validate uncompressed length
    if uncompressed_len > max_message_size {
        return Err(std::io::Error::other("prefix indicates message size is too large"));
    }

    // read 4-byte compressed length prefix
    let mut compressed_prefix = [0u8; 4];
    io.read_exact(&mut compressed_prefix).await?;
    let compressed_len = u32::from_le_bytes(compressed_prefix) as usize;

    // validate compressed length against max possible compression size
    let max_compress_len = snap::raw::max_compress_len(uncompressed_len);
    if compressed_len > max_compress_len {
        return Err(std::io::Error::other(
            "compressed size exceeds max for reported uncompressed size",
        ));
    }

    // resize buffers to reported sizes
    decode_buffer.resize(uncompressed_len, 0);
    compressed_buffer.resize(compressed_len, 0);

    // read compressed data
    io.read_exact(compressed_buffer).await?;

    // decompress
    let reader = std::io::Cursor::new(&*compressed_buffer);
    let mut decoder = FrameDecoder::new(reader);
    decoder.read_exact(decode_buffer)?;

    // deserialize
    bcs::from_bytes(decode_buffer).map_err(std::io::Error::other)
}

/// Encode a single BCS message with snappy compression and length prefixes to an async writer.
///
/// Wire format: `[4-byte uncompressed_len][4-byte compressed_len][compressed_data]`
///
/// The caller provides reusable buffers to avoid repeated allocation.
pub async fn encode_message<T, M>(
    io: &mut T,
    msg: &M,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_message_size: usize,
) -> std::io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
    M: Serialize + Sync,
{
    // clear buffers
    encode_buffer.clear();
    compressed_buffer.clear();

    // encode into buffer
    encode_into_buffer(encode_buffer, msg).map_err(|e| {
        let error = format!("encode into buffer: {e}");
        std::io::Error::other(error)
    })?;

    // validate encoded size
    if encode_buffer.len() > max_message_size {
        return Err(std::io::Error::other("encode data > max_message_size"));
    }

    // compress
    {
        let mut encoder = snap::write::FrameEncoder::new(&mut *compressed_buffer);
        encoder.write_all(encode_buffer)?;
        encoder.flush()?;
    }

    // write [uncompressed_len][compressed_len][compressed_data]
    let uncompressed_len = (encode_buffer.len() as u32).to_le_bytes();
    let compressed_len = (compressed_buffer.len() as u32).to_le_bytes();

    io.write_all(&uncompressed_len).await?;
    io.write_all(&compressed_len).await?;
    io.write_all(compressed_buffer).await?;

    Ok(())
}

#[cfg(test)]
#[path = "tests/codec_tests.rs"]
mod codec_tests;

/// Convenience type for all traits implemented for messages used for TN request-response codec.
pub trait TNMessage:
    Send + Sync + Serialize + DeserializeOwned + Clone + fmt::Debug + From<PeerExchangeMap> + 'static
{
    /// Function to intercept peer exchange messages at the network layer before passing to the
    /// application layer. Only the network layer needs peer exchange messages.
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap>;
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
        decode_message(
            io,
            &mut self.decode_buffer,
            &mut self.compressed_buffer,
            self.max_chunk_size,
        )
        .await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        decode_message(
            io,
            &mut self.decode_buffer,
            &mut self.compressed_buffer,
            self.max_chunk_size,
        )
        .await
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
        encode_message(
            io,
            &req,
            &mut self.decode_buffer,
            &mut self.compressed_buffer,
            self.max_chunk_size,
        )
        .await
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
        encode_message(
            io,
            &res,
            &mut self.decode_buffer,
            &mut self.compressed_buffer,
            self.max_chunk_size,
        )
        .await
    }
}
