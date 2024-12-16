//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite};
use libp2p::{
    request_response::{self, Codec},
    StreamProtocol, Swarm,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{io::Read, marker::PhantomData};
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
#[derive(Debug, Clone)]
pub struct TNCodec<T, U>(PhantomData<(T, U)>);

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
    Req: Send + DeserializeOwned,
    Res: Send + Serialize,
{
    type Protocol = StreamProtocol;

    #[doc = " The type of inbound and outbound requests."]
    type Request = Req;

    #[doc = " The type of inbound and outbound responses."]
    type Response = Res;

    #[doc = " Reads a request from the given I/O stream according to the"]
    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        // async read bytes
        let mut compressed = Vec::new();
        io.take(MAX_COMPRESSED_REQUEST_SIZE).read_to_end(&mut compressed).await?;

        // spawn blocking task
        tokio::task::spawn_blocking(move || {
            let mut decoder = snap::read::FrameDecoder::new(&compressed[..]);
            let mut uncompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut uncompressed)?;
            bcs::from_bytes(&uncompressed)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))??

        // // create cursor for sync snappy decoding
        // let mut decoded_buffer = Vec::new();
        // let mut limit_reader = std::io::Cursor::new(&mut bytes);
        // let mut snappy_decoder = snap::read::FrameDecoder::new(&mut limit_reader);

        // // now decode
        // snappy_decoder.read_to_end(&mut decoded_buffer)?;
        // bcs::from_bytes(decoded_buffer.as_slice())
        //     .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))

        // let result: T = tokio::task::spawn_blocking(move || {
        //         let mut decoder = FrameDecoder::new(&compressed[..]);
        //         let mut decompressed = Vec::new();
        //         std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
        //         bcs::from_bytes(&decompressed).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        //     })
        //     .await
        //     .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Join error: {}", e)))??;
    }

    #[doc = " Reads a response from the given I/O stream according to the"]
    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // TODO: create Vec with capacity
        // let mut buf = Vec::new();
        // let mut snappy_encoder = snap::write::FrameEncoder::new(&mut buf);
        // bcs::serialize_into(&mut snappy_encoder, io)?;
        // drop(snappy_encoder);
        // buf.into().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        todo!()
    }

    #[doc = " Writes a request to the given I/O stream according to the"]
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }

    #[doc = " Writes a response to the given I/O stream according to the"]
    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }
}
