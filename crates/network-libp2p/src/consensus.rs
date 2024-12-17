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
    Req: Send + Serialize + DeserializeOwned + 'static,
    Res: Send + Serialize + DeserializeOwned + 'static,
{
    type Protocol = StreamProtocol;

    #[doc = " The type of inbound and outbound requests."]
    type Request = Req;

    #[doc = " The type of inbound and outbound responses."]
    type Response = Res;

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
        // tokio::task::spawn_blocking(|| {
        //     let mut snappy_encoder = snap::write::FrameEncoder::new(&mut io);
        //     bcs::serialize_into(&mut snappy_encoder, &req)?;
        // })
        // .await
        // .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
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
