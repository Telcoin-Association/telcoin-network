//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use async_trait::async_trait;
use futures::AsyncWriteExt;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite};
use libp2p::bytes::BytesMut;
use libp2p::{
    request_response::{self, Codec},
    StreamProtocol, Swarm,
};
use serde::{de::DeserializeOwned, Serialize};
use snap::read::FrameDecoder;
use std::{
    io::{Read as _, Write as _},
    marker::PhantomData,
};
use tn_types::encode;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ConsensusNetwork<C: Codec + Send + Clone + 'static> {
    /// The gossip network for flood publishing sealed worker blocks.
    swarm: Swarm<request_response::Behaviour<C>>,
    /// The sender for network handles.
    handle: Sender<()>,
    /// The receiver for processing network handle requests.
    commands: Receiver<()>,
}

/// The Telcoin Network request/response codec for consensus messages between peers.
pub struct TNCodec<T, U> {
    /// The max possible compression length based on the max uncompressed message size.
    max_compress_len: u64,
    /// The fixed-size buffer for compressed messages.
    compressed_buffer: Vec<u8>,
    /// The fixed-size buffer for requests.
    decode_buffer: Vec<u8>,
    /// The fixed-size buffer for responses.
    response_buffer: BytesMut,
    /// The maximum size (bytes) in a chunked request/response.
    max_chunk_size: usize,
    /// Phantom data for trait
    _phantom: PhantomData<(T, U)>,
}

impl<T, U> TNCodec<T, U> {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        // create buffer from max possible compression length based on max request size
        let max_compress_len = snap::raw::max_compress_len(MAX_REQUEST_SIZE);
        let compressed_buffer = Vec::with_capacity(max_compress_len);

        let decode_buffer = Vec::with_capacity(MAX_REQUEST_SIZE);
        let response_buffer = BytesMut::with_capacity(MAX_RESPONSE_SIZE);

        Self {
            // NOTE: usize -> u64 won't lose precision (even on 32bit systems)
            max_compress_len: max_compress_len as u64,
            compressed_buffer,
            decode_buffer,
            response_buffer,
            max_chunk_size,
            _phantom: PhantomData::<(T, U)>,
        }
    }
}

/// Max request size in bytes
///
/// Worker blocks are capped at 1MB worth of transactions.
/// This should be more than enough for snappy-compressed messages.
///
/// TODO: add the message overhead as the max request size
const MAX_REQUEST_SIZE: usize = 1024 * 1024;
/// Max response size in bytes
///
/// TODO: syncing max size? This is 10 MB
const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;

#[async_trait]
impl<Req, Res> Codec for TNCodec<Req, Res>
where
    Req: Send + Serialize + DeserializeOwned + 'static,
    Res: Send + Serialize + DeserializeOwned + 'static,
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
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // retrieve prefix for uncompressed message length
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix).await?;

        // NOTE: cast usize to u32 is safe
        let length = u32::from_le_bytes(prefix) as usize;

        // ensure message length within bounds
        if length > MAX_REQUEST_SIZE {
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

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // retrieve prefix for uncompressed message length
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix).await?;

        // NOTE: cast usize to u32 is safe
        let length = u32::from_le_bytes(prefix) as usize;

        // ensure message length within bounds
        if length > MAX_REQUEST_SIZE {
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

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // global encode
        let bytes = encode(&req);

        // ensure encoded bytes are within bounds
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (bytes.len() as u32).to_le_bytes();
        io.write_all(&prefix).await?;

        // compress data
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(&bytes)?;
        encoder.flush()?;

        // add compressed bytes to prefix
        io.write_all(encoder.get_ref()).await?;
        Ok(())
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
        // global encode
        let bytes = encode(&res);

        // ensure encoded bytes are within bounds
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (bytes.len() as u32).to_le_bytes();
        io.write_all(&prefix).await?;

        // compress data
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(&bytes)?;
        encoder.flush()?;

        // add compressed bytes to prefix
        io.write_all(encoder.get_ref()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tn_types::{BlockHash, WorkerBlock};

    #[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
    struct TestData {
        timestamp: u64,
        base_fee_per_gas: Option<u64>,
        hash: BlockHash,
    }

    #[tokio::test]
    async fn test_encode_decode() {
        let max_chunk_size = 10 * 1024 * 1024; // 10mb
        let mut codec = TNCodec::<TestData, TestData>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");
        // let mut encoded = futures::io::Cursor::new(Vec::new());
        let mut encoded = Vec::new();
        let block =
            TestData { timestamp: 12345, base_fee_per_gas: Some(54321), hash: BlockHash::random() };
        codec
            .write_request(&protocol, &mut encoded, block.clone())
            .await
            .expect("write request valid");
        println!("encoded:\n{encoded:?}");
        let decoded =
            codec.read_request(&protocol, &mut encoded.as_ref()).await.expect("read request valid");
        println!("decoded??:\n{decoded:?}");
        assert_eq!(decoded, block);
    }
}
