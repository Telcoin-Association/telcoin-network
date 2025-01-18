//! Constants and trait implementations for network compatibility.

use crate::{codec::TNMessage, error::NetworkError};
use libp2p::{
    core::transport::ListenerId,
    gossipsub::{IdentTopic, MessageId, PublishError, SubscriptionError, TopicHash},
    request_response::ResponseChannel,
    Multiaddr, PeerId, TransportError,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

/// The result for network operations.
pub type NetworkResult<T> = Result<T, NetworkError>;

/// The topic for NVVs to subscribe to for published batches.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// Events created from network activity.
#[derive(Debug)]
pub enum NetworkEvent<Req, Res> {
    /// Direct request from peer.
    Request {
        /// The network request type.
        request: Req,
        /// The network response channel.
        channel: ResponseChannel<Res>,
    },
    /// Gossip message received.
    Gossip(Vec<u8>),
}

/// Commands for the swarm.
#[derive(Debug)]
pub enum NetworkCommand<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Update the list of authorized publishers.
    ///
    /// This list is used to verify messages came from an authorized source.
    /// Only valid for Subscriber implementations.
    UpdateAuthorizedPublishers {
        /// The unique set of authorized peers.
        authorities: HashSet<PeerId>,
        /// The acknowledgement that the set was updated.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Start listening on the provided multiaddr.
    ///
    /// Return the result to caller.
    StartListening {
        /// The [Multiaddr] for the swarm to connect.
        multiaddr: Multiaddr,
        /// Oneshot channel for reply.
        reply: oneshot::Sender<Result<ListenerId, TransportError<std::io::Error>>>,
    },
    /// Listeners
    GetListener { reply: oneshot::Sender<Vec<Multiaddr>> },
    /// Add explicit peer to add.
    ///
    /// This adds to the swarm's peers and the gossipsub's peers.
    AddExplicitPeer {
        /// The peer's id.
        peer_id: PeerId,
        /// The peer's address.
        addr: Multiaddr,
    },
    /// Dial a peer to establish a connection.
    Dial {
        /// The peer's id.
        peer_id: PeerId,
        /// The peer's address.
        peer_addr: Multiaddr,
        /// Oneshot for reply
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Return an owned copy of this node's [PeerId].
    LocalPeerId { reply: oneshot::Sender<PeerId> },
    /// Send a request to a peer.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequest {
        /// The destination peer.
        peer: PeerId,
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: oneshot::Sender<NetworkResult<Res>>,
    },
    /// Send response to a peer's request.
    SendResponse {
        /// The encoded message data.
        response: Res,
        /// The libp2p response channel.
        channel: ResponseChannel<Res>,
        /// Oneshot channel for returning result.
        reply: oneshot::Sender<Result<(), Res>>,
    },
    /// Subscribe to a topic.
    Subscribe { topic: IdentTopic, reply: oneshot::Sender<Result<bool, SubscriptionError>> },
    /// Publish a message to topic subscribers.
    Publish {
        topic: IdentTopic,
        msg: Vec<u8>,
        reply: oneshot::Sender<Result<MessageId, PublishError>>,
    },
    /// Map of all known peers and their associated subscribed topics.
    AllPeers { reply: oneshot::Sender<HashMap<PeerId, Vec<TopicHash>>> },
    /// Collection of this node's connected peers.
    ConnectedPeers { reply: oneshot::Sender<Vec<PeerId>> },
    /// Collection of all mesh peers.
    AllMeshPeers { reply: oneshot::Sender<Vec<PeerId>> },
    /// Collection of all mesh peers by a certain topic hash.
    MeshPeers { topic: TopicHash, reply: oneshot::Sender<Vec<PeerId>> },
    /// The peer's score, if it exists.
    PeerScore { peer_id: PeerId, reply: oneshot::Sender<Option<f64>> },
    /// Set peer's application score.
    ///
    /// Peer's application score is P₅ of the peer scoring system.
    SetApplicationScore { peer_id: PeerId, new_score: f64, reply: oneshot::Sender<bool> },
}

/// Network handle.
///
/// The type that sends commands to the running network (swarm) task.
#[derive(Clone)]
pub struct NetworkHandle<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Sending channel to the network to process commands.
    sender: mpsc::Sender<NetworkCommand<Req, Res>>,
}

impl<Req, Res> NetworkHandle<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Create a new instance of Self.
    pub fn new(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self { sender }
    }

    /// Update the list of authorized publishers.
    pub async fn update_authorized_publishers(
        &self,
        authorities: HashSet<PeerId>,
    ) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::UpdateAuthorizedPublishers { authorities, reply }).await?;
        ack.await?
    }

    /// Start swarm listening on the given address. Returns an error if the address is not
    /// supported.
    ///
    /// Return swarm error to caller.
    pub async fn start_listening(&self, multiaddr: Multiaddr) -> NetworkResult<ListenerId> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::StartListening { multiaddr, reply }).await?;
        let res = ack.await?;
        res.map_err(Into::into)
    }

    /// Request listeners from the swarm.
    pub async fn listeners(&self) -> NetworkResult<Vec<Multiaddr>> {
        let (reply, listeners) = oneshot::channel();
        self.sender.send(NetworkCommand::GetListener { reply }).await?;
        listeners.await.map_err(Into::into)
    }

    /// Add explicit peer.
    pub async fn add_explicit_peer(&self, peer_id: PeerId, addr: Multiaddr) -> NetworkResult<()> {
        self.sender
            .send(NetworkCommand::AddExplicitPeer { peer_id, addr })
            .await
            .map_err(Into::into)
    }

    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Dial { peer_id, peer_addr, reply }).await?;
        let res = ack.await?;
        res.map_err(Into::into)
    }

    /// Get local peer id.
    pub async fn local_peer_id(&self) -> NetworkResult<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::LocalPeerId { reply }).await?;
        peer_id.await.map_err(Into::into)
    }

    /// Subscribe to a topic.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe(&self, topic: IdentTopic) -> NetworkResult<bool> {
        let (reply, already_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Subscribe { topic, reply }).await?;
        let res = already_subscribed.await?;
        res.map_err(Into::into)
    }

    /// Publish a message on a certain topic.
    ///
    /// TODO: make this <M> generic to prevent accidental publishing of incorrect messages?
    pub async fn publish(&self, topic: IdentTopic, msg: Vec<u8>) -> NetworkResult<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender.send(NetworkCommand::Publish { topic, msg, reply }).await?;
        published.await?.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeers { reply }).await?;
        peers.await.map_err(Into::into)
    }

    /// Map of all known peers and their associated subscribed topics.
    pub async fn all_peers(&self) -> NetworkResult<HashMap<PeerId, Vec<TopicHash>>> {
        let (reply, all_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::AllPeers { reply }).await?;
        all_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers.
    pub async fn all_mesh_peers(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, all_mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::AllMeshPeers { reply }).await?;
        all_mesh_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers by a certain topic hash.
    pub async fn mesh_peers(&self, topic: TopicHash) -> NetworkResult<Vec<PeerId>> {
        let (reply, mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::MeshPeers { topic, reply }).await?;
        mesh_peers.await.map_err(Into::into)
    }

    /// Retrieve a specific peer's score, if it exists.
    pub async fn peer_score(&self, peer_id: PeerId) -> NetworkResult<Option<f64>> {
        let (reply, score) = oneshot::channel();
        self.sender.send(NetworkCommand::PeerScore { peer_id, reply }).await?;
        score.await.map_err(Into::into)
    }

    /// Set the peer's application score.
    ///
    /// This is useful for reporting messages from a peer that fails decoding.
    pub async fn set_application_score(
        &self,
        peer_id: PeerId,
        new_score: f64,
    ) -> NetworkResult<bool> {
        let (reply, score) = oneshot::channel();
        self.sender.send(NetworkCommand::SetApplicationScore { peer_id, new_score, reply }).await?;
        score.await.map_err(Into::into)
    }

    /// Send a request to a peer.
    ///
    /// Returns a handle for the caller to await the peer's response.
    pub async fn send_request(
        &self,
        request: Req,
        peer: PeerId,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<Res>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequest { peer, request, reply }).await?;
        Ok(to_caller)
    }

    /// Respond to a peer's request.
    pub async fn send_response(
        &self,
        response: Res,
        channel: ResponseChannel<Res>,
    ) -> NetworkResult<()> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::SendResponse { response, channel, reply }).await?;
        res.await?.map_err(|_| NetworkError::SendResponse)
    }
}
