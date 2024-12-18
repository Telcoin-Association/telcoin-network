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
use ssz::{Decode, Encode};
use std::{
    io::{Read as _, Write as _},
    marker::PhantomData,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Decoder, Encoder};
use unsigned_varint::codec::Uvi;

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
    /// Encoder for message length prefix (unsigned protobuf varint).
    prefix: Uvi<usize>,
    /// The temporary length of decoded bytes while framing via buffers.
    ///
    /// See [tokio_util::codec::decoder::Decoder] for more information.
    temp_len: Option<usize>,
    /// The maximum size (bytes) in a chunked request/response.
    max_chunk_size: usize,
    /// Phantom data for trait
    _phantom: PhantomData<(T, U)>,
}

impl<T, U> TNCodec<T, U> {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        let prefix = Uvi::default();
        Self { prefix, temp_len: None, max_chunk_size, _phantom: PhantomData::<(T, U)> }
    }

    /// Process the length prefix for messages using an unsigned protobuf varint.
    fn process_length_prefix(
        &mut self,
        bytes: &mut libp2p::bytes::BytesMut,
    ) -> Result<Option<usize>, std::io::Error> {
        if let Some(length) = self.temp_len {
            println!("some length!! {length}");
            Ok(Some(length))
        } else {
            println!("no length...");
            // decode the prefix and update the tracked temp length
            //
            // NOTE: length of > 10 bytes is decoding error (uint64)
            let res = self
                .prefix
                .decode(bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                .inspect(|len| {
                    // update temp length if some
                    self.temp_len = Some(*len);
                });

            Ok(res)
        }
    }
}

/// Max request size in bytes
///
/// Worker blocks are capped at 1MB worth of transactions.
/// This should be more than enough for snappy-compressed messages.
///
/// TODO: add the message overhead as the max request size
const MAX_COMPRESSED_REQUEST_SIZE: u64 = 1024 * 1024;
/// Max response size in bytes
///
/// TODO: syncing max size? This is 10 MB
const MAX_RESPONSE_SIZE: u64 = 10 * 1024 * 1024;

#[async_trait]
impl<Req, Res> Codec for TNCodec<Req, Res>
where
    Req: Send + Serialize + DeserializeOwned + Encode + Decode + 'static,
    Res: Send + Serialize + DeserializeOwned + Encode + Decode + 'static,
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
        let mut compressed = Vec::new();
        io.take(MAX_COMPRESSED_REQUEST_SIZE).read_to_end(&mut compressed).await?;

        // decode message's reported length in prefix
        let mut bytes_mut = BytesMut::from(&compressed[..]);
        let prefix = self.process_length_prefix(&mut bytes_mut)?;
        if prefix.is_none() {
            println!("prefix is none");
        } else {
            println!("prefix is some!");
        }
        let prefix = prefix.expect("prefix included");

        // determine max compression length possible based on msg prefix
        //
        // NOTE: usize -> u64 won't lose precision
        let max_compressed_len = snap::raw::max_compress_len(prefix) as u64;

        // read message up to the max possible compression length
        let limited_reader = std::io::Cursor::new(&compressed).take(max_compressed_len);
        let mut reader = snap::read::FrameDecoder::new(limited_reader);
        let mut decoded_buffer = vec![0; prefix];

        reader.read_exact(&mut decoded_buffer)?;
        let req = Self::Request::from_ssz_bytes(&decoded_buffer[..]).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "failed to cast request from ssz bytes")
        })?;
        Ok(req)
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
        io.take(MAX_COMPRESSED_REQUEST_SIZE).read_to_end(&mut compressed).await?;

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
        let bytes = req.as_ssz_bytes();

        // encoded bytes must be within `max_chunk_size`
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        let mut out = BytesMut::new();

        // encode uncompressed bytes length prefix using unsigned varint
        self.prefix.encode(bytes.len(), &mut out)?;

        // compress data
        let mut writer = snap::write::FrameEncoder::new(Vec::new());
        writer.write_all(&bytes)?;
        writer.flush()?;

        // add compressed bytes to prefix
        out.extend_from_slice(writer.get_ref());

        io.write_all(out.as_ref()).await?;
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
        let bytes = res.as_ssz_bytes();

        // encoded bytes must be within `max_chunk_size`
        if bytes.len() > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        let mut out = BytesMut::new();

        // encode uncompressed bytes length prefix using unsigned varint
        self.prefix.encode(bytes.len(), &mut out)?;

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
    use fastcrypto::hash::Blake2b256;
    use ssz_derive::{Decode, Encode};
    use tn_types::{BlockHash, WorkerBlock};

    #[derive(Encode, Decode, Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
    struct TestData {
        timestamp: u64,
        base_fee_per_gas: Option<u64>,
        hash: BlockHash,
    }

    #[tokio::test]
    async fn test_uvi_decode() {
        let max_chunk_size = 10 * 1024 * 1024; // 10mb
        let mut codec = TNCodec::<TestData, TestData>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");
        let mut encoded = Vec::new();
        let block =
            TestData { timestamp: 12345, base_fee_per_gas: Some(54321), hash: BlockHash::random() };
        codec
            .write_request(&protocol, &mut encoded, block.clone())
            .await
            .expect("write request valid");
        println!("encoded:\n{encoded:?}");
        let decode_buffer = Vec::new();
        let decoded = codec
            .read_request(&protocol, &mut decode_buffer.as_ref())
            .await
            .expect("read request valid");
        println!("decoded??:\n{decoded:?}");
        assert_eq!(decoded, block);
    }
}
