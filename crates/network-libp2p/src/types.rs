//! Constants and trait implementations for network compatibility.

use crate::error::NetworkError;
use libp2p::{
    gossipsub::{IdentTopic, MessageId, PublishError, SubscriptionError, TopicHash},
    swarm::{dial_opts::DialOpts, DialError},
    Multiaddr, PeerId,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

/// The result for network operations.
pub type NetworkResult<T> = Result<T, NetworkError>;

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// Commands for the swarm.
#[derive(Debug)]
pub enum NetworkCommand {
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
    /// Commands to manage the network's swarm.
    Swarm(SwarmCommand),
}

/// Commands for the swarm.
#[derive(Debug)]
//TODO: add <M> generic here so devs can only publish correct messages?
pub enum SwarmCommand {
    // TODO: add `StartListening` command to start listening on multiaddr
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
        /// The peer's address and peer id both impl Into<DialOpts>.
        ///
        /// However, it seems best to use the peer's [Multiaddr].
        dial_opts: DialOpts,
        /// Oneshot for reply
        reply: oneshot::Sender<Result<(), DialError>>,
    },
    /// Return an owned copy of this node's [PeerId].
    LocalPeerId { reply: oneshot::Sender<PeerId> },
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
pub struct GossipNetworkHandle {
    /// Sending channel to the network to process commands.
    sender: mpsc::Sender<NetworkCommand>,
}

impl GossipNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(sender: mpsc::Sender<NetworkCommand>) -> Self {
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

    /// Request listeners from the swarm.
    pub async fn listeners(&self) -> NetworkResult<Vec<Multiaddr>> {
        let (reply, listeners) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::GetListener { reply })).await?;
        listeners.await.map_err(Into::into)
    }

    /// Add explicit peer.
    pub async fn add_explicit_peer(&self, peer_id: PeerId, addr: Multiaddr) -> NetworkResult<()> {
        self.sender
            .send(NetworkCommand::Swarm(SwarmCommand::AddExplicitPeer { peer_id, addr }))
            .await
            .map_err(Into::into)
    }

    /// Dial a peer.
    pub async fn dial(&self, dial_opts: DialOpts) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::Dial { dial_opts, reply })).await?;
        let res = ack.await?;
        res.map_err(Into::into)
    }

    /// Get local peer id.
    pub async fn local_peer_id(&self) -> NetworkResult<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::LocalPeerId { reply })).await?;
        peer_id.await.map_err(Into::into)
    }

    /// Subscribe to a topic.
    pub async fn subscribe(&self, topic: IdentTopic) -> NetworkResult<bool> {
        let (reply, already_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::Subscribe { topic, reply })).await?;
        let res = already_subscribed.await?;
        res.map_err(Into::into)
    }

    /// Publish a message on a certain topic.
    ///
    /// TODO: make this <M> generic to prevent accidental publishing of incorrect messages.
    pub async fn publish(&self, topic: IdentTopic, msg: Vec<u8>) -> NetworkResult<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender
            .send(NetworkCommand::Swarm(SwarmCommand::Publish { topic, msg, reply }))
            .await?;

        published.await?.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::ConnectedPeers { reply })).await?;
        peers.await.map_err(Into::into)
    }

    /// Map of all known peers and their associated subscribed topics.
    pub async fn all_peers(&self) -> NetworkResult<HashMap<PeerId, Vec<TopicHash>>> {
        let (reply, all_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::AllPeers { reply })).await?;
        all_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers.
    pub async fn all_mesh_peers(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, all_mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::AllMeshPeers { reply })).await?;
        all_mesh_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers by a certain topic hash.
    pub async fn mesh_peers(&self, topic: TopicHash) -> NetworkResult<Vec<PeerId>> {
        let (reply, mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::MeshPeers { topic, reply })).await?;
        mesh_peers.await.map_err(Into::into)
    }

    /// Retrieve a specific peer's score, if it exists.
    pub async fn peer_score(&self, peer_id: PeerId) -> NetworkResult<Option<f64>> {
        let (reply, score) = oneshot::channel();
        self.sender.send(NetworkCommand::Swarm(SwarmCommand::PeerScore { peer_id, reply })).await?;
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
        self.sender
            .send(NetworkCommand::Swarm(SwarmCommand::SetApplicationScore {
                peer_id,
                new_score,
                reply,
            }))
            .await?;
        score.await.map_err(Into::into)
    }
}
