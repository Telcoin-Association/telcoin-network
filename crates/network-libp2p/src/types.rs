//! Constants and trait implementations for network compatibility.

use crate::{
    codec::TNMessage, error::NetworkError, peers::Penalty, GossipMessage, PeerExchangeMap,
};
use futures::stream::FuturesUnordered;
pub use libp2p::gossipsub::MessageId;
use libp2p::{
    core::transport::ListenerId,
    gossipsub::{PublishError, SubscriptionError, TopicHash},
    request_response::ResponseChannel,
    Multiaddr, PeerId, TransportError,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tn_types::{encode, BlsPublicKey, BlsSignature, NetworkPublicKey};
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
#[path = "tests/types.rs"]
mod network_types;

/// The result for network operations.
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Helper trait to cast lib-specific results into RPC messages.
pub trait IntoResponse<M> {
    /// Convert a [Result] into a [TNMessage] type.
    fn into_response(self) -> M;
}

impl<M, E> IntoResponse<M> for Result<M, E>
where
    M: TNMessage + IntoRpcError<E>,
{
    fn into_response(self) -> M {
        match self {
            Ok(msg) => msg,
            Err(e) => M::into_error(e),
        }
    }
}

/// Convenience trait for casting lib-specific error types to RPC application-layer error messages.
pub trait IntoRpcError<E> {
    /// Convert application-layer error into message.
    fn into_error(error: E) -> Self;
}

/// The topic for NVVs to subscribe to for published worker batches.
pub const WORKER_BATCH_TOPIC: &str = "tn_batches";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// Events created from network activity.
#[derive(Debug)]
pub enum NetworkEvent<Req, Res> {
    /// Direct request from peer.
    Request {
        /// The peer that made the request.
        peer: PeerId,
        /// The network request type.
        request: Req,
        /// The network response channel.
        channel: ResponseChannel<Res>,
        /// The oneshot channel if the request gets cancelled at the network level.
        cancel: oneshot::Receiver<()>,
    },
    /// Gossip message received and propagation source.
    Gossip(GossipMessage, PeerId),
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
        /// The unique set of authorized peers by topic.
        authorities: HashMap<String, Option<HashSet<PeerId>>>,
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
    GetListener {
        /// The reply to caller.
        reply: oneshot::Sender<Vec<Multiaddr>>,
    },
    /// Add explicit peer to add.
    ///
    /// This adds to the swarm's peers and the gossipsub's peers.
    AddExplicitPeer {
        /// The peer's id.
        peer_id: PeerId,
        /// The peer's address.
        addr: Multiaddr,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
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
    LocalPeerId {
        /// Reply to caller.
        reply: oneshot::Sender<PeerId>,
    },
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
    /// Send a request to any connected peer.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequestAny {
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
    Subscribe {
        /// The topic to subscribe to.
        topic: String,
        /// Authorized publishers.
        publishers: Option<HashSet<PeerId>>,
        /// The reply to caller.
        reply: oneshot::Sender<Result<bool, SubscriptionError>>,
    },
    /// Publish a message to topic subscribers.
    Publish {
        /// The topic to publish the message on.
        topic: String,
        /// The encoded message to publish.
        msg: Vec<u8>,
        /// The reply to caller.
        reply: oneshot::Sender<Result<MessageId, PublishError>>,
    },
    /// Map of all known peers and their associated subscribed topics.
    AllPeers {
        /// Reply to caller.
        reply: oneshot::Sender<HashMap<PeerId, Vec<TopicHash>>>,
    },
    /// Collection of this node's connected peers.
    ConnectedPeers {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// Collection of all mesh peers.
    AllMeshPeers {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// Collection of all mesh peers by a certain topic hash.
    MeshPeers {
        /// The topic to filter peers.
        topic: String,
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// The peer's score, if it exists.
    PeerScore {
        /// The peer's id.
        peer_id: PeerId,
        /// Reply to caller.
        reply: oneshot::Sender<Option<f64>>,
    },
    /// Report penalty for peer.
    ReportPenalty {
        /// The peer's id.
        peer_id: PeerId,
        /// The penalty to apply to the peer.
        penalty: Penalty,
    },
    /// Return the number of pending outbound requests.
    PendingRequestCount {
        /// Reply to caller.
        reply: oneshot::Sender<usize>,
    },
    /// Disconnect a peer by [PeerId]. The oneshot returns a result if the peer
    /// was connected or not.
    DisconnectPeer {
        /// The peer's id.
        peer_id: PeerId,
        /// Reply to caller.
        reply: oneshot::Sender<Result<(), ()>>,
    },
    /// Process peer information and possibly discover new peers.
    PeerExchange {
        /// Peers for discovery.
        peers: PeerExchangeMap,
        /// The libp2p response channel to send back an ack.
        channel: ResponseChannel<Res>,
    },
    /// Retrieve peers from peer manager to share with a requesting peer.
    PeersForExchange {
        /// The reply to caller.
        reply: oneshot::Sender<PeerExchangeMap>,
    },
    /// Start a new epoch.
    NewEpoch {
        /// The epoch committee.
        committee: HashMap<PeerId, Multiaddr>,
        /// The new sender for events.
        new_event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    },
    /// Find authorities for a future committee by bls key and return to sender.
    FindAuthorities {
        /// The collection of requests.
        requests: Vec<AuthorityInfoRequest>,
    },
}

/// Network handle.
///
/// The type that sends commands to the running network (swarm) task.
#[derive(Clone, Debug)]
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

    /// Create a handle to no where for test setup.
    pub fn new_for_test() -> Self {
        let (sender, _) = mpsc::channel(100);
        Self { sender }
    }

    /// Update the list of authorized publishers.
    pub async fn update_authorized_publishers(
        &self,
        authorities: HashMap<String, Option<HashSet<PeerId>>>,
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
    pub async fn add_explicit_peer(
        &self,
        peer_id: PeerId,
        addr: Multiaddr,
        reply: oneshot::Sender<NetworkResult<()>>,
    ) -> NetworkResult<()> {
        self.sender
            .send(NetworkCommand::AddExplicitPeer { peer_id, addr, reply })
            .await
            .map_err(Into::into)
    }

    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Dial { peer_id, peer_addr, reply }).await?;
        ack.await?
    }

    /// Get local peer id.
    pub async fn local_peer_id(&self) -> NetworkResult<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::LocalPeerId { reply }).await?;
        peer_id.await.map_err(Into::into)
    }

    /// Subscribe to a topic with valid publishers.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe_with_publishers(
        &self,
        topic: String,
        publishers: HashSet<PeerId>,
    ) -> NetworkResult<bool> {
        let (reply, already_subscribed) = oneshot::channel();
        self.sender
            .send(NetworkCommand::Subscribe { topic, publishers: Some(publishers), reply })
            .await?;
        let res = already_subscribed.await?;
        res.map_err(Into::into)
    }

    /// Subscribe to a topic, any publisher valid.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe(&self, topic: String) -> NetworkResult<bool> {
        let (reply, already_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Subscribe { topic, publishers: None, reply }).await?;
        let res = already_subscribed.await?;
        res.map_err(Into::into)
    }

    /// Publish a message on a certain topic.
    pub async fn publish(&self, topic: String, msg: Vec<u8>) -> NetworkResult<MessageId> {
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
    pub async fn mesh_peers(&self, topic: String) -> NetworkResult<Vec<PeerId>> {
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

    /// Send a request to a peer- any peer will do.
    ///
    /// Returns a handle for the caller to await the peer's response.
    pub async fn send_request_any(
        &self,
        request: Req,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<Res>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequestAny { request, reply }).await?;
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

    /// Return the number of pending requests.
    ///
    /// Mostly helpful for testing, but could be useful for managing outbound requests.
    pub async fn get_pending_request_count(&self) -> NetworkResult<usize> {
        let (reply, count) = oneshot::channel();
        self.sender.send(NetworkCommand::PendingRequestCount { reply }).await?;
        count.await.map_err(Into::into)
    }

    /// Disconnect from the peer.
    ///
    /// This method closes all connections to the peer without waiting for handlers
    /// to complete.
    pub async fn disconnect_peer(&self, peer_id: PeerId) -> NetworkResult<()> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::DisconnectPeer { peer_id, reply }).await?;
        res.await?.map_err(|_| NetworkError::DisconnectPeer)
    }

    /// Process peer exchange message.
    ///
    /// This is a side-effect of generic `ConsensusNetwork`. Primary and Workers
    /// receive peer exchange requests and pass them back to the peer manager.
    pub async fn process_peer_exchange(
        &self,
        peers: PeerExchangeMap,
        channel: ResponseChannel<Res>,
    ) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::PeerExchange { peers, channel }).await?;
        Ok(())
    }

    /// Report a penalty to the peer manager.
    pub async fn report_penalty(&self, peer_id: PeerId, penalty: Penalty) {
        let _ = self.sender.send(NetworkCommand::ReportPenalty { peer_id, penalty }).await;
    }

    /// Create a [PeerExchangeMap] for exchanging peers.
    pub async fn peers_for_exchange(&self) -> NetworkResult<PeerExchangeMap> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::PeersForExchange { reply }).await?;
        res.await.map_err(Into::into)
    }

    /// Create a [PeerExchangeMap] for exchanging peers.
    pub async fn new_epoch(
        &self,
        committee: HashMap<PeerId, Multiaddr>,
        new_event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    ) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::NewEpoch { committee, new_event_stream }).await?;
        Ok(())
    }

    /// Return network information for authorities by bls pubkey on kad.
    pub async fn find_authorities(
        &self,
        bls_keys: Vec<BlsPublicKey>,
    ) -> NetworkResult<
        FuturesUnordered<oneshot::Receiver<NetworkResult<(BlsPublicKey, NetworkInfo)>>>,
    > {
        // let mut results = Vec::with_capacity(bls_keys.len());
        let results = FuturesUnordered::new();
        let requests = bls_keys
            .into_iter()
            .map(|bls_key| {
                let (reply, rx) = oneshot::channel();
                results.push(rx);
                // create the request
                AuthorityInfoRequest { bls_key, reply }
            })
            .collect();

        self.sender.send(NetworkCommand::FindAuthorities { requests }).await?;
        Ok(results)
    }
}

/// List of addresses for a node, signature will be the nodes BLS signature
/// over the addresses to verify they are from the node in question.
/// Used to publish this to kademlia.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The network information contained within the record.
    pub info: NetworkInfo,
    /// Signature of the info field with the node's BLS key.
    /// This is part of a kademlia record keyed on a BLS public key
    /// that can be used for verifiction.  Intended to stop malicious
    /// nodes from poisoning the routing table.
    pub signature: BlsSignature,
}

impl NodeRecord {
    /// Helper method to build a signed node record.
    pub fn build<F>(
        pubkey: NetworkPublicKey,
        multiaddr: Multiaddr,
        hostname: String,
        signer: F,
    ) -> NodeRecord
    where
        F: FnOnce(&[u8]) -> BlsSignature,
    {
        let info = NetworkInfo { pubkey, multiaddr, hostname };
        let data = encode(&info);
        let signature = signer(&data);
        Self { info, signature }
    }

    /// Verify if a signature matches the record.
    pub fn verify(self, pubkey: &BlsPublicKey) -> Option<(BlsPublicKey, NodeRecord)> {
        let data = encode(&self.info);
        if self.signature.verify_raw(&data, pubkey) {
            Some((*pubkey, self))
        } else {
            None
        }
    }

    /// Return a reference to the record's [NetworkInfo].
    pub fn info(&self) -> &NetworkInfo {
        &self.info
    }
}

/// The network information needed for consensus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInfo {
    /// The node's [NetworkPublicKey].
    pub pubkey: NetworkPublicKey,
    /// Network address for node.
    pub multiaddr: Multiaddr,
    /// The hostname.
    pub hostname: String,
}

/// The request from the application layer to lookup a validator's network information
/// using their BLS key.
#[derive(Debug)]
pub struct AuthorityInfoRequest {
    /// The [BlsPublicKey] for the authority (on-chain).
    pub bls_key: BlsPublicKey,
    /// The reply to requestor with authority information.
    pub reply: oneshot::Sender<NetworkResult<(BlsPublicKey, NetworkInfo)>>,
}

/// Helper macro for sending oneshot replies and logging errors.
///
/// The arguments are:
/// 1) oneshot::Sender
/// 2) value to send through oneshot channel
/// 3) string error message
/// 4) `key = value` for additional logging (Optional)
#[macro_export]
macro_rules! send_or_log_error {
    // basic case: Takes a result expression and an error message string
    ($reply:expr, $result:expr, $error_msg:expr) => {
        if let Err(e) = $reply.send($result) {
            error!(target: "network", ?e, $error_msg);
        }
    };

    // optional case that allows specifying additional error context
    ($reply:expr, $result:expr, $error_msg:expr, $($field:ident = $value:expr),+ $(,)?) => {
        if let Err(e) = $reply.send($result) {
            error!(target: "network", ?e, $($field = ?$value,)+ $error_msg);
        }
    };
}
