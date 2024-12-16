//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use std::{marker::PhantomData, pin::Pin};

use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use libp2p::{
    request_response::{self, Codec},
    StreamProtocol, Swarm,
};
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

#[async_trait]
impl<Req, Res> Codec for TNCodec<Req, Res>
where
    Req: Send,
    Res: Send,
{
    type Protocol = StreamProtocol;

    #[doc = " The type of inbound and outbound requests."]
    type Request = Req;

    #[doc = " The type of inbound and outbound responses."]
    type Response = Res;

    #[doc = " Reads a request from the given I/O stream according to the"]
    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!()
    }

    #[doc = " Reads a response from the given I/O stream according to the"]
    async fn read_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        todo!()
    }

    #[doc = " Writes a request to the given I/O stream according to the"]
    async fn write_request<T>(
        &mut self,
        protocol: &Self::Protocol,
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
        protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        todo!()
    }
}
