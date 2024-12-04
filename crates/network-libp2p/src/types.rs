//! Constants and trait implementations for network compatibility.

use eyre::eyre;
use fastcrypto::hash::Hash as _;
use libp2p::{
    gossipsub::{self, IdentTopic, MessageId, PublishError, SubscriptionError},
    swarm::{dial_opts::DialOpts, DialError},
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use std::time::Duration;
use tn_types::{BlockHash, Certificate, ConsensusHeader, SealedWorkerBlock};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

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
pub trait PublishMessageId<'a>: From<&'a Vec<u8>> {
    /// Create a message id for a published message to the gossip network.
    ///
    /// Lifetimes are preferred for easier maintainability.
    /// ie) encoding/decoding logic is defined in the type's impl of `From`
    fn message_id(msg: &gossipsub::Message) -> BlockHash;
}

// Implementation for worker gossip network.
impl<'a> PublishMessageId<'a> for SealedWorkerBlock {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        // tn_types::decode::<Self>(&msg.data).digest()
        Self::from(&msg.data).digest()
    }
}

// Implementation for primary gossip network.
impl<'a> PublishMessageId<'a> for Certificate {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let certificate = Self::from(&msg.data);
        certificate.digest().into()
    }
}

// Implementation for consensus gossip network.
impl<'a> PublishMessageId<'a> for ConsensusHeader {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let certificate = Self::from(&msg.data);
        certificate.digest().into()
    }
}

/// Generate a swarm type for use with gossip network and start listening.
///
/// This is a convenience function to keep publisher/subscriber network DRY.
///
/// NOTE: the swarm tries to connect to the provided multiaddr.
pub fn start_swarm<'a, M>(multiaddr: Multiaddr) -> eyre::Result<Swarm<gossipsub::Behaviour>>
where
    M: PublishMessageId<'a>,
{
    // generate a random ed25519 key
    let mut swarm = SwarmBuilder::with_new_identity()
        // tokio runtime
        .with_tokio()
        // quic protocol
        .with_quic()
        // custom behavior
        .with_behaviour(|keypair| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let message_id = M::message_id(message);
                gossipsub::MessageId::new(message_id.as_ref())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // this is default - enforce message signing
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be
                // propagated.
                .build()
                .map_err(|e| {
                    error!(?e, "gossipsub publish network");
                    eyre!("failed to build gossipsub config for primary")
                })?;

            // build a gossipsub network behaviour
            let network = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )?;

            Ok(network)
        })
        .expect("worker publish swarm behavior valid")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // start listening
    swarm.listen_on(multiaddr)?;

    Ok(swarm)
}

/// Commands for the swarm.
#[derive(Debug)]
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
}

/// Network handle.
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
    pub async fn publish(&self, topic: IdentTopic, msg: Vec<u8>) -> eyre::Result<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender.send(NetworkCommand::Publish { topic, msg, reply }).await?;
        let res = published.await?;
        Ok(res?)
    }
}

/// Helper function for processing network commands.
///
/// This function calls methods on the swarm.
pub(crate) fn process_network_command(
    command: NetworkCommand,
    network: &mut Swarm<gossipsub::Behaviour>,
) {
    match command {
        NetworkCommand::GetListener { reply } => {
            let addrs = network.listeners().cloned().collect();
            if let Err(e) = reply.send(addrs) {
                error!(target: "gossip-network", ?e, "GetListeners command failed");
            }
        }
        NetworkCommand::AddExplicitPeer { peer_id, addr } => {
            // TODO: need to understand what `add_explicit_peer` actually does
            //
            // DO NOT MERGE
            network.add_peer_address(peer_id, addr);
            network.behaviour_mut().add_explicit_peer(&peer_id);
        }
        NetworkCommand::Dial { dial_opts, reply } => {
            let res = network.dial(dial_opts);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "AddExplicitPeer command failed");
            }
        }
        NetworkCommand::LocalPeerId { reply } => {
            let peer_id = network.local_peer_id().clone();
            if let Err(e) = reply.send(peer_id) {
                error!(target: "gossip-network", ?e, "LocalPeerId command failed");
            }
        }
        NetworkCommand::Publish { topic, msg, reply } => {
            let res = network.behaviour_mut().publish(topic, msg);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Publish command failed");
            }
        }
        NetworkCommand::Subscribe { topic, reply } => {
            let res = network.behaviour_mut().subscribe(&topic);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Subscribe command failed");
            }
        }
    }
}
