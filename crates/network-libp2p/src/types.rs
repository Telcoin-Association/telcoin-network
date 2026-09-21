//! Constants and trait implementations for network compatibility.

use crate::{
    codec::TNMessage, error::NetworkError, peers::Penalty, GossipMessage, PeerExchangeMap,
};
pub use libp2p::gossipsub::MessageId;
use libp2p::{
    core::transport::ListenerId,
    gossipsub::{PublishError, SubscriptionError, TopicHash},
    request_response::ResponseChannel as Libp2pResponseChannel,
    Multiaddr, PeerId, Stream, StreamProtocol, TransportError,
};
use std::collections::{BTreeMap, HashMap, HashSet};
pub use tn_node_record::{
    gossip_protocol_id_prefix, NetworkInfo, NetworkType, NodeRecord, RecordDomain, RpcInfo,
};
use tn_types::{BlsPublicKey, NetworkPublicKey, P2pNode};
use tokio::sync::{mpsc, oneshot};

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

/// Adapt shared protocol names to the node's fallible stream protocol interface.
pub(crate) trait NetworkTypeExt {
    /// The request-response protocol for this chain and role.
    fn req_res_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol>;
    /// The Kademlia protocol for this chain and role.
    fn kad_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol>;
    /// The bulk-sync protocol for this chain and role.
    fn sync_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol>;
    /// The peer-exchange protocol for this chain and role.
    fn peer_exchange_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol>;
}

impl NetworkTypeExt for NetworkType {
    fn req_res_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol> {
        owned_protocol(self.req_res_protocol_name(chain_id))
    }

    fn kad_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol> {
        owned_protocol(self.kad_protocol_name(chain_id))
    }

    fn sync_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol> {
        owned_protocol(self.sync_protocol_name(chain_id))
    }

    fn peer_exchange_protocol(&self, chain_id: u64) -> NetworkResult<StreamProtocol> {
        owned_protocol(self.peer_exchange_protocol_name(chain_id))
    }
}

/// The chain-namespaced stream protocol a node advertises on the stream
/// behaviour: the per-role sync protocol the typed
/// [`SyncFrame`](crate::sync::SyncFrame) layer rides on. It carries the chain id,
/// so the stream subsystem isolates chains the same way req-res and kad do.
pub(crate) fn stream_protocol(
    network_type: NetworkType,
    chain_id: u64,
) -> NetworkResult<StreamProtocol> {
    network_type.sync_protocol(chain_id)
}

/// Build an owned [`StreamProtocol`] from a runtime name, surfacing a malformed
/// name (one that does not start with `/`) as a [`NetworkError`] instead of a panic.
///
/// The names this crate builds are always well formed, so the error path is not
/// expected to fire; returning a result keeps the construction panic-free.
fn owned_protocol(name: String) -> NetworkResult<StreamProtocol> {
    StreamProtocol::try_from_owned(name).map_err(|e| NetworkError::ProtocolError(e.to_string()))
}

/// A channel for sending the response to an inbound RPC, bound to the peer that
/// opened the exchange.
///
/// This is the crate-owned equivalent of libp2p's
/// [`ResponseChannel`](libp2p::request_response::ResponseChannel): it wraps the
/// same response sink but also carries the [`PeerId`] captured when the inbound
/// request was accepted, so the requesting peer's identity travels with the
/// channel instead of being re-derived from a side map at response time. Keeping the
/// type crate-owned also keeps libp2p's request-response types out of the public
/// API, so a libp2p upgrade does not ripple into the consumer crates.
#[derive(Debug)]
pub struct ResponseChannel<Res> {
    /// The peer that opened this exchange.
    peer_id: PeerId,
    /// The underlying libp2p response sink.
    inner: Libp2pResponseChannel<Res>,
}

impl<Res> ResponseChannel<Res> {
    /// Bind a libp2p response channel to the peer that opened the exchange.
    pub(crate) fn new(peer_id: PeerId, inner: Libp2pResponseChannel<Res>) -> Self {
        Self { peer_id, inner }
    }

    /// The peer that opened this exchange.
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Consume the wrapper, returning the underlying libp2p response sink so the
    /// swarm task can deliver the response.
    pub(crate) fn into_inner(self) -> Libp2pResponseChannel<Res> {
        self.inner
    }
}

/// Events created from network activity.
#[derive(Debug)]
pub enum NetworkEvent<Req, Res> {
    /// Direct request from peer.
    Request {
        /// The peer that made the request.
        peer: BlsPublicKey,
        /// The network request type.
        request: Req,
        /// The network response channel.
        channel: ResponseChannel<Res>,
        /// The oneshot channel if the request gets cancelled at the network level.
        cancel: oneshot::Receiver<()>,
    },
    /// Gossip message received, with the relaying peer's and the author's BLS identities
    /// when they have resolved.
    ///
    /// Both identities are `Option` because a peer is `Connected` — and can relay or author
    /// gossip — before its signed `NodeRecord` (which carries the BLS key) has been resolved.
    /// The payload is delivered regardless, because the message author is already authenticated
    /// during gossip verification; the identities are used only for penalty attribution and a
    /// log label, never for the payload itself.
    ///
    /// `relayer` (the forwarding `propagation_source`) and `author` (the publishing
    /// `GossipMessage::source`) are distinct peers, and charging a content fault to the wrong
    /// one bans honest peers (see issues #801/#819); they are named rather than positional so
    /// the two cannot be transposed.
    ///
    /// The payload is boxed as [`GossipPayload`] so this variant does not size the whole enum.
    Gossip(Box<GossipPayload>),
    /// Send an error back the requester.
    Error(String, ResponseChannel<Res>),
    /// An inbound stream was established by a peer.
    ///
    /// Every stream negotiates the per-role sync protocol and carries the typed
    /// [`SyncFrame`](crate::sync::SyncFrame) layer, with the request in its first
    /// frame.
    InboundStream {
        /// The peer that opened the stream.
        peer: BlsPublicKey,
        /// The established raw p2p stream for reading data.
        stream: Stream,
    },
}

/// Boxed payload of [`NetworkEvent::Gossip`].
///
/// Held behind a `Box` in the variant so `NetworkEvent` is not sized to two inline
/// `BlsPublicKey`s (288 B each); that indirection is what lets `NetworkEvent` carry no
/// `#[allow(clippy::large_enum_variant)]`. See [`NetworkEvent::Gossip`] for the relayer
/// and author identity semantics.
#[derive(Debug)]
pub struct GossipPayload {
    /// The gossip message payload.
    pub message: GossipMessage,
    /// BLS identity of the relaying peer (`propagation_source`), when resolved.
    pub relayer: Option<BlsPublicKey>,
    /// BLS identity of the message author (`GossipMessage::source`), when resolved.
    pub author: Option<BlsPublicKey>,
}

// ============================================================================
// Network Commands
// ============================================================================

/// Commands for the swarm.
#[derive(Debug)]
pub enum NetworkCommand<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
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
    /// Add explicit peer and then dial it.
    ///
    /// This adds to the swarm's peers and the gossipsub's peers.
    AddTrustedPeerAndDial {
        /// The Bls public key for this record.
        bls_pubkey: BlsPublicKey,
        /// The peer's id.
        network_pubkey: NetworkPublicKey,
        /// The peer's address.
        addr: Multiaddr,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Add explicit peer to internal bls to peer cache.
    AddExplicitPeer {
        /// The Bls public key for this record.
        bls_pubkey: BlsPublicKey,
        /// The peer's id.
        network_pubkey: NetworkPublicKey,
        /// The peer's address.
        addr: Multiaddr,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Add explicit peers to internal bls to peer cache.
    /// Don't overwrite existing records.
    AddBootstrapPeers {
        /// The Bls public key for t.
        peers: BTreeMap<BlsPublicKey, P2pNode>,
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
    /// Dial a peer by bls key to establish a connection.
    DialBls {
        /// The peer's bls public key.
        bls_key: BlsPublicKey,
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
        peer: BlsPublicKey,
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: NetworkResponseSender<Res>,
    },
    /// Send a request to a peer by PeerId.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequestDirect {
        /// The destination peer.
        peer: PeerId,
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: NetworkResponseSender<Res>,
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
        reply: NetworkResponseSender<Res>,
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
        publishers: Option<HashSet<BlsPublicKey>>,
        /// The reply to caller.
        reply: oneshot::Sender<Result<bool, SubscriptionError>>,
    },
    /// Unsubscribe from a topic.
    Unsubscribe {
        /// The topic to unsubscribe from.
        topic: String,
        /// The reply to caller: true if this node was subscribed.
        reply: oneshot::Sender<bool>,
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
    /// Peer IDs connected or currently being dialed by the peer manager.
    ConnectedPeerIds {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// Number of established peers available to `SendRequestAny`.
    EstablishedPeerCount {
        /// Reply to caller, excluding pending dials.
        reply: oneshot::Sender<usize>,
    },
    /// Collection of this node's connected peers.
    ConnectedPeers {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<BlsPublicKey>>,
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
        peer: BlsPublicKey,
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
    /// Retrieve peers from peer manager to share with a requesting peer.
    PeersForExchange {
        /// The reply to caller.
        reply: oneshot::Sender<PeerExchangeMap>,
    },
    /// Set the previous/current/next committees directly from authoritative state, every epoch.
    UpdateCommittees {
        /// The previous epoch committee.
        previous: HashSet<BlsPublicKey>,
        /// The current epoch committee.
        current: HashSet<BlsPublicKey>,
        /// The next epoch committee.
        next: HashSet<BlsPublicKey>,
    },
    /// Pre-dial recovery: forgive bans for a committee so it can be dialed, without mutating the
    /// committee slots.
    PrepareCommitteeDial {
        /// The committee whose peers should be unbanned for dialing.
        committee: HashSet<BlsPublicKey>,
    },
    /// Find authorities for a future committee by bls key and return to sender.
    FindAuthorities {
        /// The collection of bls public keys associated with authorities to find.
        bls_keys: Vec<BlsPublicKey>,
    },
    /// Open a raw stream to a peer for bulk data transfer.
    ///
    /// The open negotiates the per-role sync protocol and the caller writes a
    /// [`SyncFrame`](crate::sync::SyncFrame) request as the first frame. An open
    /// that fails negotiation is penalty-exempt (honest version/role skew), so
    /// the caller can retry another peer.
    OpenStream {
        /// The peer to open the stream to.
        peer: BlsPublicKey,
        /// Channel for returning the established stream to application layer.
        reply: oneshot::Sender<NetworkResult<Stream>>,
    },
    /// Look up the most-recently-fetched RPC info for a known authority.
    GetValidatorRpc {
        /// The authority's BLS public key.
        bls_key: BlsPublicKey,
        /// The reply to caller.
        reply: oneshot::Sender<Option<RpcInfo>>,
    },
    /// Snapshot of the current committee's advertised RPCs; triggers kad discovery
    /// for members with no known record.
    GetAllValidatorRpcs {
        /// The reply to caller.
        reply: oneshot::Sender<Vec<(BlsPublicKey, RpcInfo)>>,
    },
    /// Read a single record from the local kad store by BLS key.
    ///
    /// Test-only observation hook used to assert local-store eviction
    /// behavior from a spawned-network test. See `kad_store_get`.
    #[cfg(test)]
    KadStoreGet {
        /// The BLS public key the record is stored under.
        key: BlsPublicKey,
        /// Reply with the record if one exists in the local store.
        reply: oneshot::Sender<Option<libp2p::kad::Record>>,
    },
}

/// Wrap a network response.
/// Adds the Bls key of the peer that sent the response.
#[derive(Clone, Debug)]
pub struct NetworkResponseMessage<Res: TNMessage> {
    /// The BLS public key of the node that sent this response.
    pub peer: BlsPublicKey,
    /// The actual response.
    pub result: Res,
}

/// Type for the result channel for a network request.
pub type NetworkResponseSender<Res> = oneshot::Sender<NetworkResult<NetworkResponseMessage<Res>>>;

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

    /// Add explicit "trusted" peer.
    ///
    /// These peers are considered "trusted" and do not receive penalties.
    /// This does not unban ips and should only be called during initialization.
    pub async fn add_trusted_peer_and_dial(
        &self,
        bls_pubkey: BlsPublicKey,
        network_pubkey: NetworkPublicKey,
        addr: Multiaddr,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender
            .send(NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, network_pubkey, addr, reply })
            .await?;
        rx.await?
    }

    /// Add explicit peer.
    pub async fn add_explicit_peer(
        &self,
        bls_pubkey: BlsPublicKey,
        network_pubkey: NetworkPublicKey,
        addr: Multiaddr,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender
            .send(NetworkCommand::AddExplicitPeer { bls_pubkey, network_pubkey, addr, reply })
            .await?;
        rx.await?
    }

    /// Add explicit bootstrap peers.
    pub async fn add_bootstrap_peers(
        &self,
        peers: BTreeMap<BlsPublicKey, P2pNode>,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::AddBootstrapPeers { peers, reply }).await?;
        rx.await?
    }

    /// Dial a peer by Bls public key.
    ///
    /// Return swarm error to caller.
    pub async fn dial_by_bls(&self, bls_key: BlsPublicKey) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::DialBls { bls_key, reply }).await?;
        ack.await?
    }

    /// Subscribe to a topic with valid publishers.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe_with_publishers(
        &self,
        topic: String,
        publishers: HashSet<BlsPublicKey>,
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

    /// Unsubscribe from a topic.
    ///
    /// Gossipsub subscriptions live on the process-lifetime swarm, so a topic subscribed for one
    /// epoch stays subscribed until it is explicitly dropped. This also clears the topic's
    /// authorized-publisher entry, restoring the "not subscribed here" state that
    /// `verify_gossip` expects for an absent entry.
    ///
    /// Return `true` if this node was subscribed to the topic.
    pub async fn unsubscribe(&self, topic: String) -> NetworkResult<bool> {
        let (reply, was_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Unsubscribe { topic, reply }).await?;
        Ok(was_subscribed.await?)
    }

    /// Publish a message on a certain topic.
    pub async fn publish(&self, topic: String, msg: Vec<u8>) -> NetworkResult<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender.send(NetworkCommand::Publish { topic, msg, reply }).await?;
        published.await?.map_err(Into::into)
    }

    /// Count peers connected or currently being dialed by the peer manager.
    ///
    /// Pending dials may not support requests yet. Use [`Self::established_peer_count`] for
    /// readiness checks.
    pub async fn connected_peer_count(&self) -> NetworkResult<usize> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeerIds { reply }).await?;
        Ok(peers.await?.len())
    }

    /// Count established peers in the queue used by [`Self::send_request_any`], excluding dials
    /// that have not established a connection yet.
    ///
    /// This is a snapshot: a peer can disconnect before a subsequent request is sent.
    pub async fn established_peer_count(&self) -> NetworkResult<usize> {
        let (reply, count) = oneshot::channel();
        self.sender.send(NetworkCommand::EstablishedPeerCount { reply }).await?;
        count.await.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> NetworkResult<Vec<BlsPublicKey>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeers { reply }).await?;
        peers.await.map_err(Into::into)
    }

    /// Send a request to a peer.
    ///
    /// Returns a handle for the caller to await the peer's response.
    pub async fn send_request(
        &self,
        request: Req,
        peer: BlsPublicKey,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<NetworkResponseMessage<Res>>>> {
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
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<NetworkResponseMessage<Res>>>> {
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
    pub(crate) async fn disconnect_peer(&self, peer_id: PeerId) -> NetworkResult<()> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::DisconnectPeer { peer_id, reply }).await?;
        res.await?.map_err(|_| NetworkError::DisconnectPeer)
    }

    /// Report a penalty to the peer manager.
    pub async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        let _ = self.sender.send(NetworkCommand::ReportPenalty { peer, penalty }).await;
    }

    /// Create a [PeerExchangeMap] for exchanging peers.
    pub async fn peers_for_exchange(&self) -> NetworkResult<PeerExchangeMap> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::PeersForExchange { reply }).await?;
        res.await.map_err(Into::into)
    }

    /// Set the previous/current/next committees directly from authoritative state, every epoch.
    pub async fn update_committees(
        &self,
        previous: HashSet<BlsPublicKey>,
        current: HashSet<BlsPublicKey>,
        next: HashSet<BlsPublicKey>,
    ) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::UpdateCommittees { previous, current, next }).await?;
        Ok(())
    }

    /// Forgive bans for a committee so it can be dialed, without mutating the committee slots.
    ///
    /// Used by the deadlock-breaker pre-dial path; the real slot update follows via
    /// [`Self::update_committees`].
    pub async fn prepare_committee_dial(
        &self,
        committee: HashSet<BlsPublicKey>,
    ) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::PrepareCommitteeDial { committee }).await?;
        Ok(())
    }

    /// Return network information for authorities by bls pubkey on kad.
    pub async fn find_authorities(&self, bls_keys: Vec<BlsPublicKey>) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::FindAuthorities { bls_keys }).await?;
        Ok(())
    }

    /// Open a raw stream to a peer for bulk data transfer.
    ///
    /// The open negotiates the per-role sync protocol; the caller then writes a
    /// [`SyncFrame`](crate::sync::SyncFrame) request as the first frame. An open
    /// that fails negotiation returns an error without penalizing the peer
    /// (honest version/role skew), so the caller can retry another peer.
    pub async fn open_stream(&self, peer: BlsPublicKey) -> NetworkResult<NetworkResult<Stream>> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::OpenStream { peer, reply }).await?;
        rx.await.map_err(Into::into)
    }

    /// Look up the most-recently-fetched RPC info for a known authority.
    ///
    /// Returns `None` if the authority is unknown or has not advertised RPC info.
    /// Callers that need fresh data should call [`Self::find_authorities`] first
    /// and wait for discovery to complete.
    ///
    /// RPC info is advertised only on worker [`NodeRecord`]s, so this is meaningful
    /// on a worker network handle; a primary handle always returns `None`. Together
    /// with [`Self::get_all_validator_rpcs`] this backs a worker gateway: a load
    /// balancer that discovers validators' advertised worker RPC endpoints and
    /// routes client traffic across them.
    pub async fn get_validator_rpc(&self, bls_key: BlsPublicKey) -> NetworkResult<Option<RpcInfo>> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::GetValidatorRpc { bls_key, reply }).await?;
        rx.await.map_err(Into::into)
    }

    /// Snapshot of the current committee's advertised RPCs.
    ///
    /// Returns the RPC info for every current-committee validator that has
    /// advertised it. Pinned operator peers and previous/next committee members
    /// never appear. The call itself (re)triggers kad discovery for current
    /// members whose node records are still unknown, so a polling caller picks
    /// up their entries on a later call once the records arrive.
    ///
    /// Only worker [`NodeRecord`]s carry RPC info, so this returns entries on a
    /// worker network handle and an empty list on a primary handle. This is the
    /// discovery primitive behind a worker gateway that maps client traffic across
    /// every validator's advertised worker RPC endpoint.
    pub async fn get_all_validator_rpcs(&self) -> NetworkResult<Vec<(BlsPublicKey, RpcInfo)>> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::GetAllValidatorRpcs { reply }).await?;
        rx.await.map_err(Into::into)
    }
}

/// Outbound kad query from this node.
#[derive(Debug)]
pub struct KadQuery {
    /// The [BlsPublicKey] for the requested authority record.
    pub request: BlsPublicKey,
    /// The best result so far.
    pub result: Option<NodeRecord>,
}

impl From<BlsPublicKey> for KadQuery {
    fn from(request: BlsPublicKey) -> Self {
        Self { request, result: None }
    }
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

/// Some PeerId specific code only used for in-crate testing.
#[cfg(test)]
impl<Req, Res> NetworkHandle<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub(crate) async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Dial { peer_id, peer_addr, reply }).await?;
        ack.await?
    }

    /// Map of all known peers and their associated subscribed topics.
    pub(crate) async fn all_peers(&self) -> NetworkResult<HashMap<PeerId, Vec<TopicHash>>> {
        let (reply, all_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::AllPeers { reply }).await?;
        all_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers by a certain topic hash.
    pub(crate) async fn mesh_peers(&self, topic: String) -> NetworkResult<Vec<PeerId>> {
        let (reply, mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::MeshPeers { topic, reply }).await?;
        mesh_peers.await.map_err(Into::into)
    }

    /// Retrieve a specific peer's score, if it exists.
    pub(crate) async fn peer_score(&self, peer_id: PeerId) -> NetworkResult<Option<f64>> {
        let (reply, score) = oneshot::channel();
        self.sender.send(NetworkCommand::PeerScore { peer_id, reply }).await?;
        score.await.map_err(Into::into)
    }

    /// Get local peer id.
    pub(crate) async fn local_peer_id(&self) -> NetworkResult<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::LocalPeerId { reply }).await?;
        peer_id.await.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub(crate) async fn connected_peer_ids(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeerIds { reply }).await?;
        peers.await.map_err(Into::into)
    }

    /// Send a request to a peer by peer id.
    ///
    /// Returns a handle for the caller to await the peer's response.
    /// For internal network use.
    pub(crate) async fn send_request_direct(
        &self,
        request: Req,
        peer: PeerId,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<NetworkResponseMessage<Res>>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequestDirect { peer, request, reply }).await?;
        Ok(to_caller)
    }

    /// Read a single record from the local kad store by BLS key.
    ///
    /// Test-only observation hook so a spawned-network test can assert on
    /// kad store contents that are otherwise unreachable once the network is
    /// running on its own task.
    #[cfg(test)]
    pub(crate) async fn kad_store_get(
        &self,
        key: BlsPublicKey,
    ) -> NetworkResult<Option<libp2p::kad::Record>> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::KadStoreGet { key, reply }).await?;
        rx.await.map_err(Into::into)
    }
}
