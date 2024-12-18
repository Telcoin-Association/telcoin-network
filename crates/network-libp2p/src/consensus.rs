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
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snap::read::FrameDecoder;
use std::{
    io::{Read as _, Write as _},
    marker::PhantomData,
};
use tn_types::encode;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Decoder, Encoder};

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
    compressed_buffer: BytesMut,
    /// The fixed-size buffer for requests.
    request_buffer: BytesMut,
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
        let compressed_buffer = BytesMut::with_capacity(max_compress_len);

        let request_buffer = BytesMut::with_capacity(MAX_REQUEST_SIZE);
        let response_buffer = BytesMut::with_capacity(MAX_RESPONSE_SIZE);

        // NOTE: usize -> u64 won't lose precision (even on 32bit systems)
        Self {
            max_compress_len: max_compress_len as u64,
            compressed_buffer,
            request_buffer,
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
        // allocate reusable buffer on `self`
        // set max to 1mb uncompressed (worker block)
        // clear reusable buffer at the beginning
        // take + read_to_end
        // ditch the prefix
        // async read bytes
        //

        // self.fixed_buffer.

        // let mut compressed = Vec::new();
        // io.take(MAX_COMPRESSED_REQUEST_SIZE).read_to_end(&mut compressed).await?;

        // clear buffers
        self.compressed_buffer.clear();
        self.request_buffer.clear();
        // let compressed_size = self.request_buffer.remaining();
        // println!("remaining after clear: {compressed_size:?}");

        let comp_length = self.compressed_buffer.len();
        println!("compressed len before: {comp_length:?}");

        // take max request size so request buffer doesn't reallocate
        let mut vec = Vec::new();
        io.take(self.max_compress_len).read_to_end(&mut vec).await?;
        println!("vec?:\n{vec:?}");

        // NOTE: this returns early with 0 bytes - no guarantees this reads all bytes,
        // so try using read_exact or read_to_end:
        //
        // io.take(self.max_compress_len).read(&mut self.compressed_buffer).await?;
        // println!("compressed_buffer?:\n{:?}", self.compressed_buffer);

        // let compressed_size = self.request_buffer.remaining();
        // println!("remaining after read?? {compressed_size:?}");

        // if compressed_size == 0 {
        //     // TODO: should this be an error?
        //     println!("ZERO BYTES!!!");
        // }

        // decompress bytes
        // read message up to the max possible compression length
        let length = self.request_buffer.len();
        println!("request buffer length before: {length:?}");
        println!("max compress len: {:?}", self.max_compress_len);

        let comp_length = self.compressed_buffer.len();
        println!("compressed len after read: {comp_length:?}");
        println!("compressed_buffer:\n{:?}", self.compressed_buffer);
        println!("request_buffer\n{:?}", self.request_buffer);
        // determine max compression length possible based on msg prefix
        //
        // NOTE: usize -> u64 won't lose precision (even on 32)
        // let max_compressed_len = snap::raw::max_compress_len(length) as u64;

        let length = snap::raw::max_compress_len(vec.len());
        let reader_limited = std::io::Cursor::new(vec); //.take(self.max_compress_len);
                                                        // std::io::Cursor::new(self.compressed_buffer.as_ref()).take(self.max_compress_len);
        let mut snappy_decoder = FrameDecoder::new(reader_limited);

        let mut request_buffer = vec![0; length];
        snappy_decoder.read_to_end(&mut request_buffer)?;

        bcs::from_bytes(&self.request_buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))

        // // determine max compression length possible based on msg prefix
        // //
        // // NOTE: usize -> u64 won't lose precision
        // let max_compressed_len = snap::raw::max_compress_len(prefix) as u64;

        // // read message up to the max possible compression length
        // let limited_reader = std::io::Cursor::new(&compressed).take(max_compressed_len);
        // let mut reader = snap::read::FrameDecoder::new(limited_reader);
        // let mut decoded_buffer = vec![0; prefix];

        // reader.read_exact(&mut decoded_buffer)?;
        // let req = Self::Request::from_ssz_bytes(&decoded_buffer[..]).map_err(|_| {
        //     std::io::Error::new(std::io::ErrorKind::Other, "failed to cast request from ssz bytes")
        // })?;
        // Ok(req)
        // todo!()
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // async read bytes
        let mut compressed = Vec::new();
        // io.take(MAX_COMPRESSED_REQUEST_SIZE).read_to_end(&mut compressed).await?;

        // spawn blocking task
        //
        // NOTE: io limited by max allowable size
        tokio::task::spawn_blocking(move || {
            let mut decoder = snap::read::FrameDecoder::new(&compressed[..]);
            let mut uncompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut uncompressed)?;
            bcs::from_bytes(&uncompressed)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
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
        // let bytes = req.as_ssz_bytes();
        let bytes = encode(&req);

        // encoded bytes must be within `max_chunk_size`
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        // encode uncompressed bytes length prefix using unsigned varint
        // self.prefix.encode(bytes.len(), &mut out)?;

        // compress data
        let mut writer = snap::write::FrameEncoder::new(Vec::new());
        writer.write_all(&bytes)?;
        writer.flush()?;

        // add compressed bytes to prefix
        // out.extend_from_slice(writer.get_ref());

        // io.write_all(out.as_ref()).await?;
        io.write_all(writer.get_ref()).await?;
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
        // let bytes = res.as_ssz_bytes();
        let bytes = Vec::new();

        // encoded bytes must be within `max_chunk_size`
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        let mut out = BytesMut::new();

        // encode uncompressed bytes length prefix using unsigned varint
        // self.prefix.encode(bytes.len(), &mut out)?;

        // compress data
        let mut writer = snap::write::FrameEncoder::new(Vec::new());
        writer.write_all(&bytes)?;
        writer.flush()?;

        // add compressed bytes to prefix
        out.extend_from_slice(writer.get_ref());

        io.write_all(out.as_ref()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
