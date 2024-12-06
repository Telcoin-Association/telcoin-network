//! Constants and trait implementations for network compatibility.

use fastcrypto::hash::Hash as _;
use libp2p::{
    gossipsub::{self, IdentTopic, MessageId, PublishError, SubscriptionError},
    swarm::{dial_opts::DialOpts, DialError},
    Multiaddr, PeerId,
};
use tn_types::{decode, BlockHash, Certificate, ConsensusHeader, SealedWorkerBlock};
use tokio::sync::{mpsc, oneshot};

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// Convenience trait to make publish network generic over message types.
///
/// The function decodes the `[libp2p::Message]` data field and returns the digest. Using the digest
/// for published message topics makes it easier for peers to recover missing data through the
/// gossip network because the message id is the same as the data type's digest used to reach
/// consensus.
pub trait GossipNetworkMessage: TryFrom<Vec<u8>> {
    /// Create a message id for a published message to the gossip network.
    ///
    /// Lifetimes are preferred for easier maintainability.
    /// ie) encoding/decoding logic is defined in the type's impl of `From`
    fn message_id(msg: &gossipsub::Message) -> BlockHash;

    /// Decode self from bytes.
    fn decode(data: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        Self::try_from(data)
    }
}

// Implementation for worker gossip network.
impl GossipNetworkMessage for SealedWorkerBlock {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let sealed_block = decode::<Self>(&msg.data);
        sealed_block.digest()
    }
}

// Implementation for primary gossip network.
impl GossipNetworkMessage for Certificate {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let certificate = decode::<Self>(&msg.data);
        certificate.digest().into()
    }
}

// Implementation for consensus gossip network.
impl GossipNetworkMessage for ConsensusHeader {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let header = decode::<Self>(&msg.data);
        header.digest()
    }
}

/// Commands for the swarm.
#[derive(Debug)]
//TODO: add <M> generic here so devs can only publish correct messages?
pub enum NetworkCommand {
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
        reply: oneshot::Sender<std::result::Result<(), DialError>>,
    },
    /// Return an owned copy of this node's [PeerId].
    LocalPeerId { reply: oneshot::Sender<PeerId> },
    /// Subscribe to a topic.
    Subscribe {
        topic: IdentTopic,
        reply: oneshot::Sender<std::result::Result<bool, SubscriptionError>>,
    },
    /// Publish a message to topic subscribers.
    Publish {
        topic: IdentTopic,
        msg: Vec<u8>,
        reply: oneshot::Sender<std::result::Result<MessageId, PublishError>>,
    },
    /// Collection of this node's connected peers.
    ConnectedPeers { reply: oneshot::Sender<Vec<PeerId>> },
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

    /// Request listeners from the swarm.
    pub async fn listeners(&self) -> eyre::Result<Vec<Multiaddr>> {
        let (reply, listeners) = oneshot::channel();
        self.sender.send(NetworkCommand::GetListener { reply }).await?;
        Ok(listeners.await?)
    }

    /// Add explicit peer.
    pub async fn add_explicit_peer(&self, peer_id: PeerId, addr: Multiaddr) -> eyre::Result<()> {
        self.sender.send(NetworkCommand::AddExplicitPeer { peer_id, addr }).await?;
        Ok(())
    }

    /// Dial a peer.
    pub async fn dial(&self, dial_opts: DialOpts) -> eyre::Result<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Dial { dial_opts, reply }).await?;
        let res = ack.await?;
        Ok(res?)
    }

    /// Get local peer id.
    pub async fn local_peer_id(&self) -> eyre::Result<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::LocalPeerId { reply }).await?;
        Ok(peer_id.await?)
    }

    /// Subscribe to a topic.
    pub async fn subscribe(&self, topic: IdentTopic) -> eyre::Result<bool> {
        let (reply, already_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Subscribe { topic, reply }).await?;
        let res = already_subscribed.await?;
        Ok(res?)
    }

    /// Publish a message on a certain topic.
    ///
    /// TODO: make this <M> generic to prevent accidental publishing of incorrect messages.
    pub async fn publish(&self, topic: IdentTopic, msg: Vec<u8>) -> eyre::Result<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender.send(NetworkCommand::Publish { topic, msg, reply }).await?;
        let res = published.await?;
        Ok(res?)
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> eyre::Result<Vec<PeerId>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeers { reply }).await?;
        Ok(peers.await?)
    }
}
