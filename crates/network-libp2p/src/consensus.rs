//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{PeerExchangeCodec, TNCodec, TNMessage},
    error::NetworkError,
    kad::KadStore,
    metrics::{PeerManagerMetrics, SwarmMetrics},
    peers::{self, PeerEvent, PeerManager, Penalty},
    send_or_log_error,
    stream::{StreamBehavior, StreamEvent},
    types::{
        KadQuery, NetworkCommand, NetworkEvent, NetworkHandle, NetworkInfo, NetworkResponseMessage,
        NetworkResponseSender, NetworkResult, NetworkType, NodeRecord, ResponseChannel, RpcInfo,
    },
    PeerExchangeMap,
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance, Topic,
        TopicHash,
    },
    kad::{self, store::RecordStore, Mode, QueryId},
    request_response::{
        self, Codec, Event as ReqResEvent, InboundFailure as ReqResInboundFailure,
        InboundRequestId, OutboundFailure as ReqResOutboundFailure, OutboundRequestId,
        ProtocolSupport,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::ErrorKind,
    time::Duration,
};
use tn_config::{KeyConfig, LibP2pConfig, NetworkConfig, PeerConfig};
use tn_types::{
    encode, now, BlsPublicKey, BlsSigner, Database, NetworkKeypair, NetworkPublicKey, TaskSpawner,
    TnSender, WorkerId,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tracing::{debug, error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
mod network_tests;

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior composes multiple sub-behaviors:
/// - `peer_manager`: Connection management and peer scoring
/// - `gossipsub`: Flood publishing for certificates and batches
/// - `req_res`: Point-to-point request-response messages
/// - `peer_exchange`: Dedicated request-response protocol for the goodbye exchange
/// - `kademlia`: Distributed hash table for peer discovery
/// - `stream`: Stream-based bulk data transfer for state sync
///
/// **Field order matters**: `NetworkBehaviour` derive calls `handle_established_*_connection`
/// on sub-behaviors in declaration order, short-circuiting on `Err(ConnectionDenied)`.
/// `peer_manager` must be first so banned-peer denials fire before other behaviors
/// (e.g. `req_res`) register the connection in their internal state.
#[derive(NetworkBehaviour)]
pub(crate) struct TNBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
{
    /// The peer manager — first so banned-peer denials short-circuit
    /// before other behaviors register the connection.
    pub(crate) peer_manager: peers::PeerManager,
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// Dedicated request-response behavior for the peer-exchange goodbye.
    ///
    /// Preferred over the [`PeerExchangeMap`] variants embedded in the consensus
    /// request enums; goodbyes fall back to the embedded variant when the peer
    /// has not upgraded yet. The embedded variants stay on the wire until the
    /// coordinated `/0.0.2` protocol bump.
    pub(crate) peer_exchange: request_response::Behaviour<PeerExchangeCodec>,
    /// Used for peer discovery.
    pub(crate) kademlia: kad::Behaviour<KadStore<DB>>,
    /// Stream-based sync behavior for bulk data transfer.
    pub(crate) stream: StreamBehavior,
}

impl<C, DB> TNBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
    DB: Database,
{
    /// Create a new instance of Self.
    ///
    /// The request-response behaviours arrive as a `(consensus, peer_exchange)`
    /// pair: the main consensus RPC behaviour and the dedicated goodbye behaviour.
    pub(crate) fn new(
        local_peer_id: PeerId,
        gossipsub: gossipsub::Behaviour,
        req_res: (request_response::Behaviour<C>, request_response::Behaviour<PeerExchangeCodec>),
        kademlia: kad::Behaviour<KadStore<DB>>,
        peer_config: &PeerConfig,
        metrics: PeerManagerMetrics,
        stream_protocols: (StreamProtocol, StreamProtocol),
    ) -> Self {
        let peer_manager = PeerManager::new(local_peer_id, peer_config, metrics);
        let (req_res, peer_exchange) = req_res;
        let (stream_legacy, stream_sync) = stream_protocols;
        let stream = StreamBehavior::new(stream_legacy, stream_sync);
        Self { peer_manager, gossipsub, req_res, peer_exchange, kademlia, stream }
    }
}

/// A goodbye dispatched on the dedicated peer-exchange protocol, awaiting the ack.
///
/// Holds everything needed to fall back to the legacy embedded exchange if the
/// peer turns out not to support the dedicated protocol.
#[derive(Debug)]
struct PendingGoodbye {
    /// The exchange map, retained so an `UnsupportedProtocols` failure can resend
    /// it as the embedded legacy variant.
    exchange: PeerExchangeMap,
    /// Notifies the disconnect-deadline task how the goodbye resolved.
    ///
    /// Dropping the sender wakes the task, which disconnects: the correct default
    /// for every resolution except a legacy fallback.
    notify: oneshot::Sender<GoodbyeOutcome>,
}

/// How a goodbye on the dedicated peer-exchange protocol resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GoodbyeOutcome {
    /// The peer acked the exchange: safe to disconnect immediately.
    Acked,
    /// The peer does not support the dedicated protocol; the goodbye was re-sent
    /// on the legacy embedded path, which owns the disconnect from here.
    FellBack,
}

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
pub struct ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>>,
{
    /// The gossip network for flood publishing sealed batches.
    swarm: Swarm<TNBehavior<TNCodec<Req, Res>, DB>>,
    /// The stream for forwarding network events.
    event_stream: Events,
    /// The sender for network handles.
    handle: Sender<NetworkCommand<Req, Res>>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand<Req, Res>>,
    /// The collection of authorized publishers per topic.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify messages
    /// published on certain topics. These are updated when the caller subscribes to a topic.
    authorized_publishers: HashMap<String, Option<HashSet<BlsPublicKey>>>,
    /// The collection of pending _graceful_ disconnects.
    ///
    /// This node disconnects from new peers if it already has the target number of peers.
    /// For these types of "peer exchange / discovery disconnects", the node shares peer records
    /// before disconnecting. This keeps track of the number of disconnects to ensure resources
    /// aren't starved while waiting for the peer's ack.
    pending_px_disconnects: HashMap<OutboundRequestId, PeerId>,
    /// The collection of pending goodbyes on the dedicated peer-exchange protocol.
    ///
    /// Tracked separately from `pending_px_disconnects`: request ids are scoped to
    /// the behaviour that issued them, so ids from the dedicated protocol could
    /// collide with the legacy req-res ids. Each entry retains the exchange map so
    /// a goodbye that fails with `UnsupportedProtocols` can fall back to the
    /// legacy variant embedded in the consensus request enum.
    pending_goodbyes: HashMap<OutboundRequestId, PendingGoodbye>,
    /// The collection of pending outbound requests.
    ///
    /// Callers include a oneshot channel for the network to return response. The caller is
    /// responsible for decoding message bytes and reporting peers who return bad data. Peers that
    /// send messages that fail to decode must receive an application score penalty.
    outbound_requests: HashMap<(PeerId, OutboundRequestId), NetworkResponseSender<Res>>,
    /// The collection of pending inbound requests.
    ///
    /// Callers include a oneshot channel for the network to return a cancellation notice. The
    /// caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    inbound_requests: HashMap<InboundRequestId, oneshot::Sender<()>>,
    /// The collection of kademlia record requests.
    ///
    /// When the application layer makes a request, the swarm stores the kad::QueryId and the
    /// the bls key associated with the desired authority's [NodeRecord]. The query runs until
    /// the last step. During this time, results are tracked and compared to one another to
    /// ensure the latest valid record is used for the peer's info.
    kad_record_queries: HashMap<QueryId, KadQuery>,
    /// The configurables for the libp2p consensus network implementation.
    config: LibP2pConfig,
    /// Track peers we have a connection with.
    ///
    /// This explicitly tracked and is a VecDeque so we can use to round robin requests without an
    /// explicit peer.
    connected_peers: VecDeque<PeerId>,
    /// Key manager, provide the BLS public key and sign peer records published to kademlia.
    key_config: KeyConfig,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The signed [NodeRecord].
    ///
    /// The external address is self-reported and unconfirmed.
    node_record: NodeRecord,
    /// Peers we have already pushed our [NodeRecord] to.
    ///
    /// A peer connecting for the first time needs our record before it can resolve
    /// our BLS key, so we push it on `PeerConnected`. A peer that reconnects (or that
    /// flaps repeatedly, as observed with banned peers in adiri testnet) should already have
    /// the record in their persistent kad store. This list is per-process-lifetime in case nodes
    /// restart.
    published_to_peers: HashSet<PeerId>,
    /// Prometheus metrics for swarm-level events (gossip, requests).
    metrics: SwarmMetrics,
}

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Convenience method for spawning a primary network instance.
    pub fn new_for_primary(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
    ) -> NetworkResult<Self> {
        let network_key = key_config.primary_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            NetworkType::Primary,
            external_addr,
            None,
        )
    }

    /// Convenience method for spawning a worker network instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_worker(
        worker_id: WorkerId,
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
        rpc: Option<RpcInfo>,
    ) -> NetworkResult<Self> {
        let network_key = key_config.worker_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            NetworkType::Worker(worker_id),
            external_addr,
            rpc,
        )
    }

    /// Create a new instance of Self.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        keypair: NetworkKeypair,
        db: DB,
        task_spawner: TaskSpawner,
        network_type: NetworkType,
        external_addr: Multiaddr,
        rpc: Option<RpcInfo>,
    ) -> NetworkResult<Self> {
        // Namespace every wire protocol by the genesis chain id so nodes on
        // different chains never negotiate a connection. The id is stamped onto
        // the network config from genesis at node startup; see
        // `NetworkConfig::set_chain_id`.
        let chain_id = network_config.libp2p_config().chain_id;

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // TN specific: filter against authorized_publishers for certain topics
            .validate_messages()
            // Gossipsub negotiates its own `/meshsub` protocol, independent of the
            // req-res/kad/stream names below, so without this it is the one wire
            // protocol two chains still share: namespacing the topics keeps their
            // messages apart but still lets cross-chain peers negotiate a gossip
            // substream. Folding the chain id into the protocol id closes that gap.
            // The builder appends `/1.1.0` and `/1.0.0`, yielding
            // `/tn-meshsub-{chain_id}/1.1.0` and `/tn-meshsub-{chain_id}/1.0.0`.
            .protocol_id_prefix(crate::types::gossip_protocol_id_prefix(chain_id))
            .build()?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(NetworkError::GossipBehavior)?;

        let tn_codec =
            TNCodec::<Req, Res>::new(network_config.libp2p_config().max_rpc_message_size);

        let req_res = request_response::Behaviour::with_codec(
            tn_codec,
            vec![(network_type.req_res_protocol(chain_id)?, ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        // Dedicated goodbye protocol: the same hardened codec under its own wire
        // name, so the peer-exchange map no longer has to ride inside the consensus
        // request enums. The embedded variants remain as the fallback for
        // not-yet-upgraded peers until the coordinated `/0.0.2` bump.
        let px_codec = PeerExchangeCodec::new(network_config.libp2p_config().max_rpc_message_size);
        let peer_exchange = request_response::Behaviour::with_codec(
            px_codec,
            vec![(network_type.peer_exchange_protocol(chain_id)?, ProtocolSupport::Full)],
            request_response::Config::default(),
        );
        let peer_id: PeerId = keypair.public().into();
        let mut kad_config = libp2p::kad::Config::new(network_type.kad_protocol(chain_id)?);
        // manually add peers
        kad_config.set_kbucket_inserts(kad::BucketInserts::Manual);
        let libp2p = network_config.libp2p_config();
        kad_config.set_kbucket_size(libp2p.k_bucket_size);
        kad_config
            .set_record_ttl(Some(libp2p.kad_record_ttl))
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            .set_publication_interval(Some(libp2p.kad_publication_interval))
            .set_query_timeout(Duration::from_secs(60))
            .set_provider_record_ttl(Some(libp2p.kad_record_ttl));
        let mut kad_store = KadStore::new(db.clone(), &key_config, network_type);

        // Load the Kad records from DB for the local peer cache, decoding with a legacy
        // fallback so records persisted by pre-upgrade software still load. Collect
        // corrupt entries (undecodable values or broken keys) for removal.
        let mut known = Vec::new();
        let mut corrupt = Vec::new();
        for record in kad_store.records() {
            match BlsPublicKey::from_literal_bytes(record.key.as_ref()) {
                Ok(key) => match NodeRecord::try_decode_compat(record.value.as_ref()) {
                    Some(node_record) => known.push((key, node_record.info)),
                    None => corrupt.push(record.key.clone()),
                },
                // How did we get a KAD record with a broken key?
                Err(error) => {
                    error!(target: "network-kad", ?error, "Invalid/corrupt KAD DB store!");
                    corrupt.push(record.key.clone());
                }
            }
        }

        // Purge corrupt records before the store is cloned into the kademlia behaviour
        // so its record accounting stays accurate.
        for key in corrupt {
            warn!(target: "network-kad", ?key, "removing undecodable record from kad store");
            kad_store.remove(&key);
        }

        let kademlia = kad::Behaviour::with_config(peer_id, kad_store.clone(), kad_config);

        // create custom behavior
        let stream_protocols = crate::types::stream_protocols(network_type, chain_id)?;
        let mut behavior = TNBehavior::new(
            peer_id,
            gossipsub,
            (req_res, peer_exchange),
            kademlia,
            network_config.peer_config(),
            PeerManagerMetrics::new_for(&network_type),
            stream_protocols,
        );

        // Promote the surviving records into the local peer cache.
        for (key, info) in known {
            behavior.peer_manager.add_known_peer(key, info);
        }

        let network_pubkey = keypair.public().into();

        // create swarm
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic_config(|mut config| {
                config.handshake_timeout = network_config.quic_config().handshake_timeout;
                config.max_idle_timeout = network_config.quic_config().max_idle_timeout;
                config.keep_alive_interval = network_config.quic_config().keep_alive_interval;
                config.max_concurrent_stream_limit =
                    network_config.quic_config().max_concurrent_stream_limit;
                config.max_stream_data = network_config.quic_config().max_stream_data;
                config.max_connection_data = network_config.quic_config().max_connection_data;
                config
            })
            .with_behaviour(|_| behavior)
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(
                    network_config.libp2p_config().max_idle_connection_timeout,
                )
            })
            .build();

        // set external address
        swarm.add_external_address(external_addr.clone());

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let config = network_config.libp2p_config().clone();
        let pending_px_disconnects = HashMap::with_capacity(config.max_px_disconnects);
        let pending_goodbyes = HashMap::with_capacity(config.max_px_disconnects);
        let node_record = Self::create_node_record(external_addr, &key_config, network_pubkey, rpc);

        Ok(Self {
            swarm,
            handle,
            commands,
            event_stream,
            authorized_publishers: Default::default(),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            kad_record_queries: Default::default(),
            config,
            connected_peers: VecDeque::new(),
            pending_px_disconnects,
            pending_goodbyes,
            key_config,
            task_spawner,
            node_record,
            published_to_peers: HashSet::new(),
            metrics: SwarmMetrics::new_for(&network_type),
        })
    }

    /// Return a [NetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Create and sign this node's [NodeRecord].
    fn create_node_record(
        external_addr: Multiaddr,
        key_config: &KeyConfig,
        network_pubkey: NetworkPublicKey,
        rpc: Option<RpcInfo>,
    ) -> NodeRecord {
        NodeRecord::build(network_pubkey, external_addr, rpc, |data| {
            key_config.request_signature_direct(data)
        })
    }

    /// Return a kademlia record keyed on our BlsPublicKey with our peer_id and network addresses.
    /// Return None if we don't have any confirmed external addresses yet.
    fn get_peer_record(&self) -> kad::Record {
        let key = kad::RecordKey::new(&self.key_config.primary_public_key());
        // Leave `expires: None` for our OWN record so libp2p's PutRecordJob
        // recomputes a fresh `now + kad_record_ttl` on every replication snapshot
        // (see libp2p-kad jobs.rs:217-221). The configured `kad_record_ttl` still
        // drives the wire-level expiry that remote peers store.
        kad::Record {
            key: key.clone(),
            value: encode(&self.node_record),
            publisher: Some(*self.swarm.local_peer_id()),
            expires: None,
        }
    }

    /// Verify the address list in Record was signed by the key and the kad record's publisher
    /// matches the network key.
    fn peer_record_valid(&self, record: &kad::Record) -> Option<(BlsPublicKey, NodeRecord)> {
        let key = BlsPublicKey::from_literal_bytes(record.key.as_ref()).ok()?;

        // decode (with legacy fallback for pre-upgrade peers) and verify bls signature
        let (pubkey, node_record) = NodeRecord::decode_and_verify(record.value.as_ref(), &key)?;

        // verify publisher matches the network public key in the record
        // this prevents replay attacks where malicious nodes republish outdated records
        let expected_peer_id: PeerId = node_record.info.pubkey.clone().into();
        if record.publisher != Some(expected_peer_id) {
            warn!(
                target: "network-kad",
                "NodeRecord validation failed: publisher {:?} doesn't match network key (expected {:?})",
                record.publisher, expected_peer_id
            );
            return None;
        }

        Some((pubkey, node_record))
    }

    /// Publish and provide our network addresses and peer id under our BLS public key for
    /// discovery.
    fn provide_our_data(&mut self) {
        let record = self.get_peer_record();
        info!(target: "network-kad", ?record, "Providing our record to kademlia for peer {:?}", self.swarm.local_peer_id());
        let key = record.key.clone();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            match &err {
                kad::store::Error::ValueTooLarge => error!(
                    target: "network-kad",
                    "node record exceeds kad value-size limit; RPC endpoint NOT advertised to peers ({err})"
                ),
                _ => error!(target: "network-kad", "Failed to store record locally: {err}"),
            }
        }
        if let Err(err) = self.swarm.behaviour_mut().kademlia.start_providing(key) {
            error!(target: "network-kad", "Failed to start providing key: {err}");
        }
    }

    /// Push our [NodeRecord] directly to a newly-connected peer.
    ///
    /// Used on first-time connections so the remote peer can resolve our BLS key
    /// without waiting for the kad publication interval (12h). Callers must
    /// short-circuit on reconnects - see [`Self::published_to_peers`]
    fn publish_our_data_to_peer(&mut self, peer: PeerId) {
        let record = self.get_peer_record();
        info!(target: "network-kad", "Publishing our record to peer {peer:?}");
        let _ = self.swarm.behaviour_mut().kademlia.put_record_to(
            record,
            vec![peer].into_iter(),
            kad::Quorum::One,
        );
    }

    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        // add peer record if address confirmed
        self.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
        self.provide_our_data();

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => if let Err(e) = self.process_event(event).await {
                    error!(target: "network", ?e, "network event error");
                    if let NetworkError::AllListenersClosed = e {
                        // In this case go ahead and kill the node.
                        return Err(e);
                    }
                },
                command = self.commands.recv() => match command {
                    Some(c) => if let Err(e) = self.process_command(c) {
                        error!(target: "network", ?e, "network command error")
                    },
                    None => {
                        info!(target: "network", "network shutting down...");
                        return Ok(())
                    }
                },
            }

            // refresh in-flight gauges once per loop iteration (scrape-interval freshness)
            self.metrics.set_pending(self.goodbyes_in_flight(), self.outbound_requests.len());
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.authorized_publishers.keys()))]
    async fn process_event(
        &mut self,
        event: SwarmEvent<TNBehaviorEvent<TNCodec<Req, Res>, DB>>,
    ) -> NetworkResult<()> {
        match event {
            SwarmEvent::Behaviour(behavior) => match behavior {
                TNBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                TNBehaviorEvent::ReqRes(event) => self.process_reqres_event(event)?,
                TNBehaviorEvent::PeerExchange(event) => self.process_peer_exchange_event(event)?,
                TNBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
                TNBehaviorEvent::Kademlia(event) => self.process_kad_event(event)?,
                TNBehaviorEvent::Stream(event) => self.process_stream_event(event)?,
            },
            SwarmEvent::ExternalAddrConfirmed { address } => {
                // protocol expects static IP address
                // emit warning if peers report different external address
                let expected = &self.node_record.info().multiaddrs;
                if !expected.contains(&address) {
                    warn!(target: "network", ?expected, reported=?address, "peer reporting different external addr:")
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                debug!(
                    target: "network",
                    ?address,
                    "listener address expired"
                );
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                // log listener errors
                error!(
                    target: "network",
                    ?listener_id,
                    ?error,
                    "listener error"
                );
            }
            SwarmEvent::ListenerClosed { addresses, reason, .. } => {
                // log errors
                if let Err(e) = reason {
                    error!(target: "network", ?e, "listener unexpectedly closed");
                }

                // critical failure
                if self.swarm.listeners().count() == 0 {
                    error!(target: "network", ?addresses, "no listeners for swarm - network shutting down");
                    return Err(NetworkError::AllListenersClosed);
                }
            }
            // other events handled by peer manager and other behaviors
            _ => {}
        }
        Ok(())
    }

    /// Process commands for the network.
    fn process_command(&mut self, command: NetworkCommand<Req, Res>) -> NetworkResult<()> {
        match command {
            NetworkCommand::StartListening { multiaddr, reply } => {
                let res = self.swarm.listen_on(multiaddr);
                send_or_log_error!(reply, res, "StartListening");
            }
            NetworkCommand::GetListener { reply } => {
                let addrs = self.swarm.listeners().cloned().collect();
                send_or_log_error!(reply, addrs, "GetListeners");
            }
            NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, network_pubkey, addr, reply } => {
                // update peer manager
                self.swarm.behaviour_mut().peer_manager.add_trusted_peer_and_dial(
                    bls_pubkey,
                    NetworkInfo {
                        pubkey: network_pubkey,
                        multiaddrs: vec![addr],
                        timestamp: now(),
                        rpc: None,
                    },
                    reply,
                );
            }
            NetworkCommand::AddExplicitPeer { bls_pubkey, network_pubkey, addr, reply } => {
                // update peer manager
                self.swarm.behaviour_mut().peer_manager.add_known_peer(
                    bls_pubkey,
                    NetworkInfo {
                        pubkey: network_pubkey,
                        multiaddrs: vec![addr],
                        timestamp: now(),
                        rpc: None,
                    },
                );
                let _ = reply.send(Ok(()));
            }
            NetworkCommand::AddBootstrapPeers { peers, reply } => {
                // update peer manager
                let peer = &mut self.swarm.behaviour_mut().peer_manager;
                for (bls, info) in peers {
                    if peer.auth_to_peer(bls).is_none() {
                        peer.add_known_peer(
                            bls,
                            NetworkInfo {
                                pubkey: info.network_key,
                                multiaddrs: vec![info.network_address],
                                timestamp: now(),
                                rpc: None,
                            },
                        );
                    }
                }
                let _ = reply.send(Ok(()));
            }
            NetworkCommand::Dial { peer_id, peer_addr, reply } => {
                self.swarm.behaviour_mut().peer_manager.dial_peer(
                    peer_id,
                    vec![peer_addr],
                    Some(reply),
                );
            }
            NetworkCommand::DialBls { bls_key, reply } => {
                debug!(target: "network", "command for dial bls {bls_key}");
                if let Some((peer_id, peer_addr)) =
                    self.swarm.behaviour().peer_manager.auth_to_peer(bls_key)
                {
                    self.swarm.behaviour_mut().peer_manager.dial_peer(
                        peer_id,
                        peer_addr,
                        Some(reply),
                    );
                } else {
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::LocalPeerId { reply } => {
                let peer_id = *self.swarm.local_peer_id();
                send_or_log_error!(reply, peer_id, "LocalPeerId");
            }
            NetworkCommand::Publish { topic, msg, reply } => {
                let res =
                    self.swarm.behaviour_mut().gossipsub.publish(TopicHash::from_raw(topic), msg);
                if res.is_ok() {
                    self.metrics.record_gossip_published();
                }
                send_or_log_error!(reply, res, "Publish");
            }
            NetworkCommand::Subscribe { topic, publishers, reply } => {
                let sub: IdentTopic = Topic::new(&topic);
                let res = self.swarm.behaviour_mut().gossipsub.subscribe(&sub);
                self.authorized_publishers.insert(topic, publishers);
                send_or_log_error!(reply, res, "Subscribe");
            }
            NetworkCommand::ConnectedPeerIds { reply } => {
                let res = self.swarm.behaviour().peer_manager.connected_or_dialing_peers();
                debug!(target: "network", ?res, "peer manager connected peers:");
                send_or_log_error!(reply, res, "ConnectedPeers");
            }
            NetworkCommand::ConnectedPeers { reply } => {
                let peers = self
                    .swarm
                    .behaviour()
                    .peer_manager
                    .connected_or_dialing_peers()
                    .iter()
                    .flat_map(|id| self.swarm.behaviour().peer_manager.peer_to_bls(id))
                    .collect();
                debug!(target: "network", ?peers, "peer manager connected peers:");
                send_or_log_error!(reply, peers, "ConnectedPeers");
            }
            NetworkCommand::PeerScore { peer_id, reply } => {
                let opt_score = self.swarm.behaviour().peer_manager.peer_score(&peer_id);
                send_or_log_error!(reply, opt_score, "PeerScore");
            }
            NetworkCommand::AllPeers { reply } => {
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .all_peers()
                    .map(|(peer_id, vec)| (*peer_id, vec.into_iter().cloned().collect()))
                    .collect();

                send_or_log_error!(reply, collection, "AllPeers");
            }
            NetworkCommand::MeshPeers { topic, reply } => {
                let topic: IdentTopic = Topic::new(&topic);
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .mesh_peers(&topic.into())
                    .cloned()
                    .collect();
                send_or_log_error!(reply, collection, "MeshPeers");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                debug!(target: "network", "send request for bls {peer}");
                if let Some((peer, addr)) = self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
                    debug!(target: "network", "trying to send to {peer} at {addr:?}");
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .req_res
                        .send_request_with_addresses(&peer, request, addr);
                    self.outbound_requests.insert((peer, request_id), reply);
                } else {
                    // Best effort to return an error to caller.
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::SendRequestDirect { peer, request, reply } => {
                let request_id = self.swarm.behaviour_mut().req_res.send_request(&peer, request);
                self.outbound_requests.insert((peer, request_id), reply);
            }
            NetworkCommand::SendRequestAny { request, reply } => {
                // Rotating an empty list will panic...
                if !self.connected_peers.is_empty() {
                    self.connected_peers.rotate_left(1);
                }
                if let Some(peer) = self.connected_peers.front() {
                    let request_id = self.swarm.behaviour_mut().req_res.send_request(peer, request);
                    self.outbound_requests.insert((*peer, request_id), reply);
                } else {
                    // Ignore error since this means other end lost interest and we don't really
                    // care.
                    let _ = reply.send(Err(NetworkError::NoPeers));
                }
            }
            NetworkCommand::SendResponse { response, channel, reply } => {
                let res = self
                    .swarm
                    .behaviour_mut()
                    .req_res
                    .send_response(channel.into_inner(), response);
                send_or_log_error!(reply, res, "SendResponse");
            }
            NetworkCommand::PendingRequestCount { reply } => {
                let count = self.outbound_requests.len();
                send_or_log_error!(reply, count, "SendResponse");
            }
            NetworkCommand::ReportPenalty { peer, penalty } => {
                debug!(target: "network", "penalty reported for peer {peer}");
                if let Some((peer, _)) = self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, penalty);
                } else {
                    warn!(target: "peer-manager", ?peer, "unable to assess penalty for peer");
                }
            }
            NetworkCommand::DisconnectPeer { peer_id, reply } => {
                // this is called after timeout for disconnected peer exchanges
                let res = self.swarm.disconnect_peer_id(peer_id);
                send_or_log_error!(reply, res, "DisconnectPeer");
            }
            NetworkCommand::PeersForExchange { reply } => {
                let peers = self.swarm.behaviour_mut().peer_manager.peers_for_exchange();
                send_or_log_error!(reply, peers, "PeersForExchange");
            }
            NetworkCommand::UpdateCommittees { previous, current, next } => {
                // The network mirrors three of the on-chain registry's committees: previous,
                // current, and next. Peers in any of the three count as validators so the
                // just-completed committee is not pruned while late gossip may still arrive and
                // next-epoch peers are protected before they begin voting. (NVV support and
                // late-gossip acceptance remain future work.)
                //
                // All three slots are set directly from authoritative state every epoch (no
                // positional rotation), so current/previous self-correct against on-chain state and
                // any peer that exits the three-slot window is demoted.
                info!(target: "network", this_node=?self.swarm.local_peer_id(), "updating previous/current/next committees");
                self.swarm.behaviour_mut().peer_manager.update_committees(previous, current, next);
            }
            NetworkCommand::PrepareCommitteeDial { committee } => {
                // Deadlock-breaker pre-dial: forgive bans so the committee can be dialed without
                // mutating the committee slots (the real slot update follows shortly after).
                self.swarm.behaviour_mut().peer_manager.prepare_committee_dial(committee);
            }
            NetworkCommand::FindAuthorities { bls_keys } => {
                // this will trigger a PeerEvent to fetch records through kad if not in the peer map
                self.swarm.behaviour_mut().peer_manager.find_authorities(bls_keys);
            }
            NetworkCommand::GetValidatorRpc { bls_key, reply } => {
                let rpc = self.swarm.behaviour().peer_manager.get_rpc(&bls_key);
                send_or_log_error!(reply, rpc, "GetValidatorRpc");
            }
            NetworkCommand::GetAllValidatorRpcs { reply } => {
                let rpcs = self.swarm.behaviour().peer_manager.all_rpcs();
                send_or_log_error!(reply, rpcs, "GetAllValidatorRpcs");
            }
            NetworkCommand::OpenStream { peer, kind, reply } => {
                // Look up the peer's PeerId from their BLS key
                let (peer_id, addrs) = match self.swarm.behaviour().peer_manager.auth_to_peer(peer)
                {
                    Some((id, addrs)) => (id, addrs),
                    None => {
                        debug!(
                            target: "network",
                            ?peer,
                            "OpenStream: peer not found"
                        );
                        let _ = reply.send(Err(NetworkError::PeerMissing));
                        return Ok(());
                    }
                };

                debug!(
                    target: "network",
                    ?peer_id,
                    "opening stream to peer"
                );

                // Pass the reply channel directly to the stream behavior.
                // The stream (or error) will be returned to the caller via oneshot
                // without any intermediate tracking.
                self.swarm.behaviour_mut().stream.open_stream(peer_id, kind, addrs, reply);
            }
            #[cfg(test)]
            NetworkCommand::KadStoreGet { key, reply } => {
                let record_key = kad::RecordKey::new(&key);
                let record = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .get(&record_key)
                    .map(|cow| cow.into_owned());
                let _ = reply.send(record);
            }
        }

        Ok(())
    }

    /// Process gossip events.
    fn process_gossip_event(&mut self, event: GossipEvent) -> NetworkResult<()> {
        match event {
            GossipEvent::Message { propagation_source, message_id, message } => {
                trace!(target: "network", topic=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?message, "message received from publisher");
                self.metrics.record_gossip_received();
                // verify message was published by authorized node
                let msg_acceptance = self.verify_gossip(&message);
                let valid = msg_acceptance.is_accepted();
                trace!(target: "network", ?msg_acceptance, "gossip message verification status");

                // report message validation results to propagate valid messages
                if !self.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    msg_acceptance.into(),
                ) {
                    error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, "error reporting message validation result");
                }

                // process gossip in application layer
                if valid {
                    // A peer is `Connected` before its `NodeRecord` resolves its BLS
                    // identity, so a live mesh neighbor can relay a message before
                    // `peer_to_bls` can resolve it. Deliver the accepted payload
                    // regardless and carry the relayer as `Option`: the author is
                    // already authenticated by `verify_gossip`, the relayer identity
                    // is only used for penalty attribution, and dropping here would
                    // lose the message for good because gossipsub has already cached
                    // `message_id` and will not re-deliver it once the identity
                    // resolves. The consumer skips the (unattributable) penalty while
                    // the relayer is unresolved.
                    let relayer =
                        self.swarm.behaviour().peer_manager.peer_to_bls(&propagation_source);
                    if relayer.is_none() {
                        debug!(
                            target: "network",
                            ?propagation_source,
                            ?message_id,
                            "delivering accepted gossip with unresolved relayer identity; consensus-layer penalty skipped"
                        );
                    }
                    // forward gossip to handler
                    if let Err(e) =
                        self.event_stream.try_send(accepted_gossip_event(message, relayer))
                    {
                        error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?e, "failed to forward gossip!");
                        // ignore failures at the epoch boundary
                        // During epoch change the event_stream reciever can be closed.
                        return Ok(());
                    }
                } else {
                    self.metrics.record_gossip_rejected();
                    let GossipMessage { source, topic, .. } = message;
                    warn!(
                        target: "network",
                        author = ?source,
                        ?topic,
                        "received invalid gossip - applying fatal penalty to propagation source: {:?}",
                        propagation_source
                    );
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .process_penalty(propagation_source, Penalty::Fatal);
                }
            }
            GossipEvent::Subscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?topic, "gossipsub event - subscribed")
            }
            GossipEvent::Unsubscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?topic, "gossipsub event - unsubscribed")
            }
            GossipEvent::GossipsubNotSupported { peer_id } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, "gossipsub event - not supported");
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Fatal);
            }
            GossipEvent::SlowPeer { peer_id, failed_messages } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?failed_messages, "gossipsub event - slow peer");
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Mild);
            }
        }

        Ok(())
    }

    /// Process req/res events.
    fn process_reqres_event(&mut self, event: ReqResEvent<Req, Res>) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id: _ } => {
                match message {
                    request_response::Message::Request { request_id, request, channel } => {
                        debug!(target: "network", ?peer, ?request, "request received");
                        // intercept peer exchange messages
                        if let Some(peers) = request.peer_exchange_msg() {
                            debug!(target: "network", ?peers, "processing peer exchange");
                            self.swarm.behaviour_mut().peer_manager.process_peer_exchange(peers);
                            // send empty ack and ignore errors
                            let ack = PeerExchangeMap::default().into();
                            let _ = self.swarm.behaviour_mut().req_res.send_response(channel, ack);

                            // initiate disconnect from this peer to prevent redial attempts
                            debug!(target: "peer-manager", ?peer, "initiating reciprocal disconnect after px");
                            self.swarm.behaviour_mut().peer_manager.disconnect_peer(peer, false);
                            return Ok(());
                        }

                        // We should not be able to recieve a message from an unknown peer so this
                        // should always work. It is possible (mostly in
                        // testing) to have a race where we don't know the requester YET.
                        // If so send an error back but this should be so infrequent on a real
                        // network that we can ignore and it should not
                        // cause any lasting damage if triggered.
                        if let Some(bls) = self.swarm.behaviour().peer_manager.peer_to_bls(&peer) {
                            let (notify, cancel) = oneshot::channel();
                            // forward request to handler without blocking other events
                            if let Err(e) = self.event_stream.try_send(NetworkEvent::Request {
                                peer: bls,
                                request,
                                channel: ResponseChannel::new(peer, channel),
                                cancel,
                            }) {
                                error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                                // ignore failures at the epoch boundary
                                // During epoch change the event_stream reciever can be closed.
                                return Ok(());
                            }

                            // store the request and cancel duplicate requests
                            //
                            // NOTE: the request id is internally generated, so this should not
                            // happen
                            if let Some(channel) = self.inbound_requests.insert(request_id, notify)
                            {
                                // cancel if this is a duplicate request
                                warn!(target: "network", ?peer, "duplicate request id from peer");
                                let _ = channel.send(());
                            }
                        } else if let Err(e) = self.event_stream.try_send(NetworkEvent::Error(
                            format!("requesting peer unknown: {peer:?}"),
                            ResponseChannel::new(peer, channel),
                        )) {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                            // ignore failures at the epoch boundary
                            // During epoch change the event_stream reciever can be closed.
                            return Ok(());
                        }
                    }
                    request_response::Message::Response { request_id, response } => {
                        // check if response associated with PX disconnect
                        if self.pending_px_disconnects.remove(&request_id).is_some() {
                            let _ = self.swarm.disconnect_peer_id(peer);
                        }

                        // try to forward response to original caller
                        let _ = self.outbound_requests.remove(&(peer, request_id)).map(|ack| {
                            // The response payload is genuine (we still hold the
                            // matching outbound request). If the responder's BLS
                            // identity has not resolved yet, report a transient
                            // `PeerUnresolved` rather than a misleading `PeerMissing`
                            // so the caller does not retry a request that succeeded.
                            let resolved = self.swarm.behaviour().peer_manager.peer_to_bls(&peer);
                            let _ = ack.send(resolve_response(resolved, response));
                        });
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, "Outbound failure for req/res");
                // handle px disconnects
                //
                // px attempts to support peer discovery, but failures are okay
                // this node disconnects after a px timeout
                if self.pending_px_disconnects.remove(&request_id).is_some() {
                    debug!(target: "network", "outbound failure expected because of px disconnect");
                    return Ok(());
                }

                let failure_kind = match &error {
                    ReqResOutboundFailure::DialFailure => "dial",
                    ReqResOutboundFailure::ConnectionClosed => "connection",
                    ReqResOutboundFailure::Io(_) => "io",
                    ReqResOutboundFailure::Timeout => "timeout",
                    ReqResOutboundFailure::UnsupportedProtocols => "unsupported",
                };
                self.metrics.record_outbound_failure(failure_kind);

                // Differentiate transport-level failures (peer disconnect, dial fail) from
                // protocol-level violations. Transport failures are common on WAN and should
                // not contribute to ban score; otherwise N in-flight requests at disconnect
                // time cause N * Medium = instant ban.
                match &error {
                    ReqResOutboundFailure::DialFailure
                    | ReqResOutboundFailure::ConnectionClosed => {
                        // transport-level: no penalty
                    }
                    ReqResOutboundFailure::Io(e) => match e.kind() {
                        ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::BrokenPipe
                        | ErrorKind::Interrupted => {
                            // transport flap on WAN — no penalty
                        }
                        _ => {
                            warn!(
                                target: "network",
                                ?e, ?peer, ?request_id,
                                "outbound IO failure (likely codec violation)"
                            );
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer, Penalty::Medium);
                        }
                    },
                    ReqResOutboundFailure::Timeout => {
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Mild);
                    }
                    // Not penalized. Failing to negotiate a common protocol is honest
                    // version/role skew (the peer runs a different/older/role-distinct
                    // protocol set), not misbehavior — the same not-the-peer's-fault
                    // class as `DialFailure`/`ConnectionClosed` above. Penalizing it
                    // bans not-yet-upgraded peers during rolling upgrades and would turn
                    // the #765 chain-id protocol split into a network partition. Warn for
                    // operator visibility only.
                    ReqResOutboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, "outbound failure: unsupported protocol (not penalized)");
                    }
                }

                // try to forward error to original caller
                let _ = self.outbound_requests.remove(&(peer, request_id)).map(|ack| {
                    let _ = ack.send(Err(NetworkError::Outbound(error.into())));
                });
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, pending=?self.inbound_requests, "Inbound failure for req/res");
                debug!(target: "network", my_id=?self.swarm.local_peer_id(), "this node");
                match &error {
                    ReqResInboundFailure::Io(e) => match e.kind() {
                        ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::BrokenPipe
                        | ErrorKind::Interrupted => {
                            // transport flap on WAN — no penalty
                        }
                        _ => {
                            warn!(
                                target: "network",
                                ?e, ?peer, ?request_id,
                                "inbound IO failure (likely codec violation)"
                            );
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer, Penalty::Medium);
                        }
                    },
                    // Not penalized. The local peer supports none of the protocols the
                    // remote requested: honest version/role skew, not misbehavior (the
                    // inbound mirror of the outbound arm above). Penalizing it bans
                    // not-yet-upgraded peers during rolling upgrades and is a prerequisite
                    // blocker for the #765 chain-id protocol split. Warn for operator
                    // visibility only.
                    ReqResInboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, ?error, "inbound failure: unsupported protocol (not penalized)");
                    }
                    ReqResInboundFailure::Timeout | ReqResInboundFailure::ConnectionClosed => {
                        // peer dropped or stalled mid-request — expected on WAN, no penalty
                    }
                    ReqResInboundFailure::ResponseOmission => { /* ignore local error */ }
                }

                // forward cancelation to handler and ignore errors
                if let Some(channel) = self.inbound_requests.remove(&request_id) {
                    let _ = channel.send(());
                }
            }

            ReqResEvent::ResponseSent { request_id, .. } => {
                if let Some(channel) = self.inbound_requests.remove(&request_id) {
                    let _ = channel.send(());
                }
            }
        }

        Ok(())
    }

    /// Process events from the dedicated peer-exchange goodbye protocol.
    ///
    /// Mirrors the legacy embedded peer-exchange handling in
    /// [`Self::process_reqres_event`]: an inbound exchange updates the peer manager,
    /// receives an empty ack, and triggers a reciprocal disconnect. Failures are
    /// never penalized: a goodbye precedes a disconnect, so there is no
    /// relationship left to protect. The one failure that changes course is
    /// outbound `UnsupportedProtocols` (honest version skew, penalty-exempt): the
    /// exchange is re-sent as the legacy variant embedded in the consensus request
    /// enum so not-yet-upgraded peers still receive it.
    fn process_peer_exchange_event(
        &mut self,
        event: ReqResEvent<PeerExchangeMap, PeerExchangeMap>,
    ) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id: _ } => match message {
                request_response::Message::Request { request_id: _, request, channel } => {
                    debug!(target: "network", ?peer, ?request, "processing peer exchange (dedicated protocol)");
                    self.swarm.behaviour_mut().peer_manager.process_peer_exchange(request);
                    // send empty ack and ignore errors
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .peer_exchange
                        .send_response(channel, PeerExchangeMap::default());

                    // initiate disconnect from this peer to prevent redial attempts
                    debug!(target: "peer-manager", ?peer, "initiating reciprocal disconnect after px");
                    self.swarm.behaviour_mut().peer_manager.disconnect_peer(peer, false);
                }
                request_response::Message::Response { request_id, response: _ } => {
                    // goodbye acked: disconnect immediately (the ack payload is
                    // reserved for a future reciprocal exchange and ignored today)
                    if let Some(pending) = self.pending_goodbyes.remove(&request_id) {
                        let _ = pending.notify.send(GoodbyeOutcome::Acked);
                        let _ = self.swarm.disconnect_peer_id(peer);
                    }
                }
            },
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, "Outbound failure for peer exchange");
                if let Some(pending) = self.pending_goodbyes.remove(&request_id) {
                    match &error {
                        // Not penalized: honest version skew, the same class the main
                        // req-res handler exempts. The peer predates the dedicated
                        // protocol, so re-send the exchange as the embedded legacy
                        // variant, which owns the disconnect from here.
                        ReqResOutboundFailure::UnsupportedProtocols => {
                            debug!(
                                target: "peer-manager",
                                ?peer,
                                "peer exchange protocol unsupported - falling back to embedded exchange"
                            );
                            self.send_legacy_goodbye(peer, pending.exchange);
                            let _ = pending.notify.send(GoodbyeOutcome::FellBack);
                        }
                        // Any other failure means no ack is coming: dropping the
                        // notify sender wakes the deadline task, which disconnects.
                        // No penalty: px supports discovery and failures are okay.
                        ReqResOutboundFailure::DialFailure
                        | ReqResOutboundFailure::ConnectionClosed
                        | ReqResOutboundFailure::Io(_)
                        | ReqResOutboundFailure::Timeout => {}
                    }
                }
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                // never penalized: the exchange is best-effort and both sides
                // disconnect afterwards regardless
                debug!(target: "network", ?peer, ?request_id, ?error, "Inbound failure for peer exchange");
            }
            ReqResEvent::ResponseSent { peer, .. } => {
                trace!(target: "network", ?peer, "peer exchange ack sent");
            }
        }

        Ok(())
    }

    /// The number of graceful goodbyes currently awaiting resolution, across the
    /// dedicated peer-exchange protocol and the embedded legacy path.
    ///
    /// Both paths share the `max_px_disconnects` budget so the combined pending
    /// count keeps the original bound.
    fn goodbyes_in_flight(&self) -> usize {
        self.pending_goodbyes.len() + self.pending_px_disconnects.len()
    }

    /// Send a goodbye on the dedicated peer-exchange protocol and schedule the
    /// disconnect.
    ///
    /// The spawned task disconnects once the goodbye resolves or after
    /// `px_disconnect_timeout`, whichever comes first, unless the goodbye fell
    /// back to the embedded legacy path, which schedules its own disconnect.
    fn send_goodbye(&mut self, peer_id: PeerId, exchange: PeerExchangeMap) {
        let (notify, done) = oneshot::channel();
        let request_id =
            self.swarm.behaviour_mut().peer_exchange.send_request(&peer_id, exchange.clone());
        self.pending_goodbyes.insert(request_id, PendingGoodbye { exchange, notify });

        let timeout = self.config.px_disconnect_timeout;
        let handle = self.network_handle();

        // spawn task
        let task_name = format!("goodbye-{peer_id}");
        self.task_spawner.spawn_task(task_name, async move {
            // disconnect after the goodbye resolves (ack / failure / deadline)
            // unless the legacy fallback took over the disconnect
            let fell_back = tokio::time::timeout(timeout, done)
                .await
                .ok()
                .and_then(|resolved| resolved.ok())
                .is_some_and(|outcome| outcome == GoodbyeOutcome::FellBack);
            if !fell_back {
                let _ = handle.disconnect_peer(peer_id).await;
            }
            Ok(())
        });
    }

    /// Send a goodbye as the [`PeerExchangeMap`] variant embedded in the legacy
    /// consensus request enum.
    ///
    /// The fallback for peers that do not support the dedicated peer-exchange
    /// protocol yet; removal is coordinated with the `/0.0.2` protocol bump.
    fn send_legacy_goodbye(&mut self, peer_id: PeerId, peer_exchange: PeerExchangeMap) {
        // guard: skip PX if peer already disconnected
        if !self.swarm.is_connected(&peer_id) {
            debug!(target: "peer-manager", ?peer_id, "peer already disconnected, skipping PX");
        } else if self.goodbyes_in_flight() < self.config.max_px_disconnects {
            // attempt to exchange peer information if limits allow
            let (reply, done) = oneshot::channel();
            let request_id =
                self.swarm.behaviour_mut().req_res.send_request(&peer_id, peer_exchange.into());
            self.outbound_requests.insert((peer_id, request_id), reply);

            let timeout = self.config.px_disconnect_timeout;
            let handle = self.network_handle();

            // spawn task
            let task_name = format!("peer-exchange-{peer_id}");
            self.task_spawner.spawn_task(task_name, async move {
                // ignore errors and disconnect after px attempt
                let _res = tokio::time::timeout(timeout, done).await;
                let _ = handle.disconnect_peer(peer_id).await;
                Ok(())
            });

            // insert to pending px disconnects
            self.pending_px_disconnects.insert(request_id, peer_id);
        } else {
            // too many px disconnects pending so disconnect without px
            let _ = self.swarm.disconnect_peer_id(peer_id);
        }
    }

    /// Specific logic to accept gossip messages.
    ///
    /// Messages are only published by current committee nodes and must be within max size.
    fn verify_gossip(&self, gossip: &GossipMessage) -> GossipAcceptance {
        // verify message size
        if gossip.data.len() > self.config.max_gossip_message_size {
            return GossipAcceptance::Reject;
        }

        let GossipMessage { topic, .. } = gossip;

        // ensure publisher is authorized
        if gossip.source.is_some_and(|id| {
            let bls_key = self.swarm.behaviour().peer_manager.peer_to_bls(&id);
            self.authorized_publishers.get(topic.as_str()).is_some_and(|auth| {
                auth.is_none()
                    || (bls_key.is_some()
                        && auth.as_ref().expect("is some").contains(&bls_key.expect("is some")))
            })
        }) {
            GossipAcceptance::Accept
        } else {
            GossipAcceptance::Reject
        }
    }

    /// Process an event from the peer manager.
    fn process_peer_manager_event(&mut self, event: PeerEvent) -> NetworkResult<()> {
        match event {
            PeerEvent::DisconnectPeer(peer_id) => {
                debug!(target: "network", ?peer_id, "peer manager: disconnect peer");
                // remove from request-response
                // NOTE: gossipsub handle `FromSwarm::ConnectionClosed`
                let _ = self.swarm.disconnect_peer_id(peer_id);

                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
            }
            PeerEvent::PeerDisconnected(peer_id) => {
                debug!(target: "network", ?peer_id, "peer disconnected event from peer manager");

                // Check if there are any connections still in the pool
                if self.swarm.is_connected(&peer_id) {
                    warn!(
                        target: "network",
                        ?peer_id,
                        "PeerDisconnected event but swarm still has connections - forcing disconnect"
                    );
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);

                let keys = self
                    .outbound_requests
                    .iter()
                    .filter_map(
                        |((p_id, req_id), _)| {
                            if *p_id == peer_id {
                                Some((*p_id, *req_id))
                            } else {
                                None
                            }
                        },
                    )
                    .collect::<Vec<_>>();

                // remove from outbound_requests and send error
                for k in keys {
                    let _ = self.outbound_requests.remove(&k).map(|ack| {
                        let _ = ack.send(Err(NetworkError::Disconnected));
                    });
                }
            }
            PeerEvent::DisconnectPeerX(peer_id, peer_exchange) => {
                debug!(target: "peer-manager", this_node=?self.swarm.local_peer_id(), ?peer_id, "disconnecting from peer with exchange info");

                // guard: skip PX if peer already disconnected
                if !self.swarm.is_connected(&peer_id) {
                    debug!(target: "peer-manager", ?peer_id, "peer already disconnected, skipping PX");
                } else if self.goodbyes_in_flight() < self.config.max_px_disconnects {
                    // attempt to exchange peer information if limits allow,
                    // preferring the dedicated protocol (falls back to the
                    // embedded legacy variant on `UnsupportedProtocols`)
                    self.send_goodbye(peer_id, peer_exchange);
                } else {
                    // too many px disconnects pending so disconnect without px
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove peer from kad - will redial if necessary
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);
            }
            PeerEvent::PeerConnected(peer_id, addr) => {
                // Defense in depth: even if the peer-manager `handle_established_*_connection`
                // path lets a banned peer reach this event (observed in adiri testnet logs),
                // refuse to register the connection with kademlia/gossipsub. Otherwise the
                // banned peer ends up in the kad routing table and triggers a redial loop.
                if self.swarm.behaviour().peer_manager.peer_banned(&peer_id) {
                    debug!(
                        target: "network",
                        ?peer_id,
                        "PeerConnected for banned peer — refusing to register"
                    );
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                    return Ok(());
                }

                // register peer for request-response behaviour
                // NOTE: gossipsub handles `FromSwarm::ConnectionEstablished`
                self.swarm.add_peer_address(peer_id, addr.clone());
                // add as a kademlia peer
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);

                // First-time connections need a direct record push so the peer can resolve
                // our BLS key without waiting for the next kad publication interval. Skip
                // on reconnects to avoid amplifying the local kad store on flapping peers
                if self.published_to_peers.insert(peer_id) {
                    self.publish_our_data_to_peer(peer_id);
                }

                // manage connected peers for
                self.connected_peers.push_back(peer_id);

                // if this is a trusted/validator (important) peer, mark it as explicit in gossipsub
                if self.swarm.behaviour().peer_manager.peer_is_important(&peer_id) {
                    self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }
            PeerEvent::Banned(peer_id) => {
                warn!(target: "network", ?peer_id, "peer banned");
                // blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
            }
            PeerEvent::Unbanned(peer_id) => {
                debug!(target: "network", ?peer_id, "peer unbanned");
                // remove blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&peer_id);
            }
            PeerEvent::MissingAuthorities(missing) => {
                for bls_key in missing {
                    let key = kad::RecordKey::new(&bls_key);
                    let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
                    self.kad_record_queries.insert(query_id, bls_key.into());
                }
            }
            PeerEvent::Discovery => {
                let peer_id = PeerId::random();
                self.swarm.behaviour_mut().kademlia.get_closest_peers(peer_id);
            }
        }

        Ok(())
    }

    /// Process events from the stream behavior.
    ///
    /// This handles inbound and outbound stream events for bulk data transfer.
    fn process_stream_event(&mut self, event: StreamEvent) -> NetworkResult<()> {
        match event {
            StreamEvent::InboundStream { peer, kind, stream } => {
                debug!(
                    target: "network",
                    ?peer,
                    ?kind,
                    "inbound stream received"
                );
                // Forward raw stream to application layer, tagged with the
                // negotiated protocol so it routes to the sync or legacy reader.
                if let Some(bls) = self.swarm.behaviour().peer_manager.peer_to_bls(&peer) {
                    if let Err(e) = self.event_stream.try_send(NetworkEvent::InboundStream {
                        peer: bls,
                        kind,
                        stream,
                    }) {
                        error!(target: "network", ?e, "failed to forward inbound stream");
                    }
                } else {
                    warn!(target: "network", ?peer, "received inbound stream from unknown peer");
                }
            }
            StreamEvent::OutboundFailure { peer, failure }
            | StreamEvent::InboundFailure { peer, failure } => {
                // Classified for scoring but reported metrics-only until telemetry
                // confirms the classification does not fire on healthy peers (see
                // #739). Once confirmed, the matching penalty is enforced via
                // `peer_manager.process_penalty(peer, penalty)`.
                failure.penalty().map_or_else(
                    || trace!(target: "network", ?peer, ?failure, "stream failure (no penalty)"),
                    |penalty| {
                        debug!(
                            target: "network",
                            ?peer, ?failure, ?penalty,
                            "stream failure classified (metrics-only, not enforced)"
                        )
                    },
                );
            }
        }
        Ok(())
    }

    /// Process event from kademlia behavior.
    fn process_kad_event(&mut self, event: kad::Event) -> NetworkResult<()> {
        match event {
            kad::Event::InboundRequest { request } => {
                trace!(target: "network-kad", "inbound {request:?}");
                match request {
                    kad::InboundRequest::FindNode { num_closer_peers: _ } => {}
                    kad::InboundRequest::GetProvider {
                        num_closer_peers: _,
                        num_provider_peers: _,
                    } => {}
                    kad::InboundRequest::AddProvider { record } => {
                        if let Some(record) = record {
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .store_mut()
                                .add_provider(record)
                                .map_err(|e| NetworkError::StoreKademliaRecord(e.to_string()))?;
                        }
                    }
                    kad::InboundRequest::GetRecord { num_closer_peers: _, present_locally: _ } => {}
                    kad::InboundRequest::PutRecord { source, connection: _, record } => {
                        if let Some(record) = record {
                            self.process_kad_put_request(source, record)?;
                        }
                    }
                }
            }
            kad::Event::OutboundQueryProgressed { id: query_id, result, stats: _, step } => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        key,
                        providers,
                        ..
                    })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            ?providers,
                            "kad::GetProviders::Ok"
                        );
                    }
                    kad::QueryResult::GetProviders(Err(err)) => {
                        error!(target: "network-kad", "Failed to get providers: {err:?}");
                    }
                    kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                        kad::PeerRecord { record, peer },
                    ))) => {
                        if let Some((key, node_record)) = self.peer_record_valid(&record) {
                            trace!(target: "network-kad", "Got record {key} {node_record:?}");
                            self.process_kad_query_result(
                                &query_id,
                                key,
                                node_record,
                                peer,
                                step.last,
                            );
                        } else {
                            trace!(target: "network-kad", "Received invalid peer record!");

                            // assess penalty for invalid peer record
                            if let Some(peer_id) = peer {
                                self.swarm
                                    .behaviour_mut()
                                    .peer_manager
                                    .process_penalty(peer_id, Penalty::Fatal);
                            }

                            // ensure query cleaned up
                            if step.last {
                                self.close_kad_query(&query_id);
                            }
                        }
                    }
                    kad::QueryResult::GetRecord(Ok(
                        kad::GetRecordOk::FinishedWithNoAdditionalRecord { cache_candidates },
                    )) => {
                        debug!(target: "network-kad", ?cache_candidates, "FinishedWithNoAdditionalRecord - failed to find record");
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::GetRecord(Err(err)) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::GetRecord::Err"
                        );
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::PutRecordOk"
                        );
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        debug!(target: "network-kad", "Failed to put record: {err:?}");
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::StartProviding::Ok"
                        );
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        warn!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::StartProviding::Err"
                        );
                    }
                    kad::QueryResult::GetClosestPeers(Ok(result)) => {
                        // process peers for potential discovery attempts
                        debug!(target: "network-kad", ?result, "GetClosestPeers for discovery");
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_peers_for_discovery(result.peers);
                    }
                    kad::QueryResult::GetClosestPeers(Err(err)) => {
                        // A timed-out query still carries the peers it located before
                        // expiring. Recover them for discovery instead of letting the
                        // catch-all discard the whole query: discovery only runs when
                        // the node is short on peers, and that same low-connectivity
                        // state is what makes queries slow enough to time out, so
                        // dropping the partial results starves discovery exactly when
                        // it is most needed.
                        let peers = partial_peers_from_get_closest_timeout(err);
                        debug!(
                            target: "network-kad",
                            recovered = peers.len(),
                            "GetClosestPeers timed out; recovering partial discovery results"
                        );
                        self.swarm.behaviour_mut().peer_manager.process_peers_for_discovery(peers);
                    }
                    _ => {}
                }
            }
            kad::Event::RoutingUpdated { peer, is_new_peer, addresses, bucket_range, old_peer } => {
                debug!(target: "network-kad", "routing updated peer {peer:?} new {is_new_peer} addrs {addresses:?} bucketr {bucket_range:?} old {old_peer:?}");

                // update newly added peer
                if is_new_peer {
                    self.swarm.behaviour_mut().peer_manager.update_routing_for_peer(&peer, true);

                    // update old peer if evicted from routing table
                    if let Some(old) = old_peer {
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .update_routing_for_peer(&old, false);
                    }
                }
            }
            kad::Event::UnroutablePeer { peer } => {
                // unknown peer queried a record - noop
                trace!(target: "network-kad", "unroutable peer {peer:?}")
            }
            kad::Event::RoutablePeer { peer, address } => {
                // kad discovered a new peer - peer is added to table on `PeerEvent::Connected`
                trace!(target: "network-kad", "routable peer {peer:?}/{address:?}");
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                trace!(target: "network-kad", "pending routable peer {peer:?}/{address:?}")
            }
            kad::Event::ModeChanged { new_mode } => {
                trace!(target: "network-kad", "mode changed {new_mode:?}")
            }
        }
        Ok(())
    }

    /// Process an inbound kad put request.
    fn process_kad_put_request(
        &mut self,
        source: PeerId,
        record: kad::Record,
    ) -> NetworkResult<()> {
        // check if source or publisher are banned
        let publisher_is_banned = record
            .publisher
            .map(|peer| self.swarm.behaviour().peer_manager.peer_banned(&peer))
            .unwrap_or(true); // reject records without publisher
        let source_is_banned = self.swarm.behaviour().peer_manager.peer_banned(&source);

        // reject record
        if publisher_is_banned || source_is_banned {
            error!(target: "network-kad", ?publisher_is_banned, ?source_is_banned, ?source, publisher=?record.publisher, "rejecting put request for record");
            // handle race condition with PM
            self.swarm.behaviour_mut().kademlia.remove_record(&record.key);

            // assess penalty for pushing record without publisher
            if record.publisher.is_none() {
                trace!(target: "network-kad", ?source, "processing fatal penalty for missing publisher");
                self.swarm.behaviour_mut().peer_manager.process_penalty(source, Penalty::Fatal);
            }

            // return early
            return Ok(());
        }

        // verify record signature and ensure publisher matches record's network
        // key
        if let Some((key, value)) = self.peer_record_valid(&record) {
            // store latest node records
            if self.is_newer_record(&record) {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .put(record)
                    .map_err(|e| NetworkError::StoreKademliaRecord(e.to_string()))?;
                trace!(target: "network-kad", "Got record {key} {value:?}");
                self.swarm.behaviour_mut().peer_manager.add_known_peer(key, value.info);
            } else {
                // A peer republishing a slightly stale (but signature-valid) record is
                // expected after restarts and benign — the local store keeps the newer
                // version. Log only; no penalty.
                trace!(target: "network-kad", ?source, "ignoring stale but valid kad record");
            }
        } else {
            warn!(target: "network-kad", "Received invalid peer record!");

            // assess penalty for invalid peer record
            trace!(target: "network-kad", ?source, "processing fatal penalty for invalid peer record");
            self.swarm.behaviour_mut().peer_manager.process_penalty(source, Penalty::Fatal);
        }

        Ok(())
    }

    /// Check the local kad store to compare record timestamps.
    ///
    /// This method compares timestamps for verified records to ensure the latest record
    /// is stored (prevents replay attacks). Also returns `true` if the record is not found.
    /// It is the caller's responsibility to ensure records are verified and valid.
    fn is_newer_record(&mut self, record: &kad::Record) -> bool {
        let store = self.swarm.behaviour_mut().kademlia.store_mut();

        if let Some(existing) = store.get(&record.key) {
            match (
                NodeRecord::try_decode_compat(&existing.value),
                NodeRecord::try_decode_compat(&record.value),
            ) {
                (Some(existing_record), Some(new_record)) => {
                    // return true if the new record is newer
                    existing_record.info.timestamp < new_record.info.timestamp
                }
                _ => false,
            }
        } else {
            // return true if record is not in local store
            true
        }
    }

    /// Logic to process a kad record query result.
    ///
    /// The record arrives pre-validated — the caller already checked the signature
    /// and publisher via [`Self::peer_record_valid`]. This method checks:
    /// - the returned key matches the request
    /// - the latest node record is used
    fn process_kad_query_result(
        &mut self,
        query_id: &QueryId,
        key: BlsPublicKey,
        new_record: NodeRecord,
        peer: Option<PeerId>,
        is_last_step: bool,
    ) {
        // return if query id unknown - should not happen
        let Some(query) = self.kad_record_queries.get_mut(query_id) else { return };

        // ensure returned value matches request
        if query.request == key {
            match &mut query.result {
                None => query.result = Some(new_record),
                Some(tracked) if tracked.info.timestamp < new_record.info.timestamp => {
                    *tracked = new_record
                }
                Some(_) => {} // keep existing record
            }
        } else {
            // assess penalty for returning record that doesn't match key
            if let Some(peer_id) = peer {
                trace!(target: "network-kad", ?peer_id, "processing fatal penalty for query record key mismatch");
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Fatal);
            }
        }

        // handle last step
        if is_last_step {
            self.close_kad_query(query_id);
        }
    }

    /// Cleanup kad record queries (called on last step).
    fn close_kad_query(&mut self, query_id: &QueryId) {
        if let Some(query) = self.kad_record_queries.remove(query_id) {
            if let Some(node_record) = query.result {
                self.swarm
                    .behaviour_mut()
                    .peer_manager
                    .add_known_peer(query.request, node_record.info);
            }
        }
    }
}

/// Enum if the received gossip is initially accepted for further processing.
///
/// This is necessary because libp2p does not impl `PartialEq` on [MessageAcceptance].
/// This impl does not map to `MessageAcceptance::Ignore`.
#[derive(Debug, PartialEq)]
enum GossipAcceptance {
    /// The message is considered valid, and it should be delivered and forwarded to the network.
    Accept,
    /// The message is considered invalid, and it should be rejected and trigger the P₄ penalty.
    Reject,
}

impl GossipAcceptance {
    /// Helper method indicating if the gossip message was accepted.
    fn is_accepted(&self) -> bool {
        *self == GossipAcceptance::Accept
    }
}

impl From<GossipAcceptance> for MessageAcceptance {
    fn from(value: GossipAcceptance) -> Self {
        match value {
            GossipAcceptance::Accept => MessageAcceptance::Accept,
            GossipAcceptance::Reject => MessageAcceptance::Reject,
        }
    }
}

impl<Req, Res, DB, Events> std::fmt::Debug for ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusNetwork")
            .field("authorized_publishers", &self.authorized_publishers)
            .field("pending_px_disconnects", &self.pending_px_disconnects)
            .field("pending_goodbyes", &self.pending_goodbyes)
            .field("outbound_requests", &self.outbound_requests.len())
            .field("inbound_requests", &self.inbound_requests.len())
            .field("config", &self.config)
            .field("connected_peers", &self.connected_peers)
            .field("swarm", &"<swarm>") // Skip detailed debug for swarm
            .finish()
    }
}

/// Peers a kademlia `GetClosestPeers` query located before it timed out.
///
/// Kademlia reports a timed-out query as [`kad::GetClosestPeersError::Timeout`],
/// whose payload carries the closest peers found so far. Those peers are still
/// valid discovery candidates, so they are recovered for the discovery pool
/// rather than discarded along with the failed query.
pub(crate) fn partial_peers_from_get_closest_timeout(
    err: kad::GetClosestPeersError,
) -> Vec<kad::PeerInfo> {
    let kad::GetClosestPeersError::Timeout { peers, .. } = err;
    peers
}

/// Pair a response payload with the responding peer's resolved BLS identity.
///
/// The payload is genuine whenever this node still holds the matching outbound
/// request, so a peer whose identity has not resolved yet (it connected before
/// its `NodeRecord` populated the confirmed-identity index) is reported as a
/// transient [`NetworkError::PeerUnresolved`] rather than the misleading
/// [`NetworkError::PeerMissing`].
fn resolve_response<Res: TNMessage>(
    resolved: Option<BlsPublicKey>,
    response: Res,
) -> NetworkResult<NetworkResponseMessage<Res>> {
    resolved
        .map(|peer| NetworkResponseMessage { peer, result: response })
        .ok_or(NetworkError::PeerUnresolved)
}

/// Build the application event for an accepted gossip message.
///
/// `relayer` is the relaying peer's BLS identity, or `None` while its
/// `NodeRecord` has not yet resolved. The accepted payload is delivered in
/// either case: the author is already authenticated during gossip verification,
/// and dropping an unresolved-relayer message would lose it permanently because
/// gossipsub has already cached its `message_id`. The relayer is carried so the
/// consumer can attribute a penalty only when the identity is known.
fn accepted_gossip_event<Req, Res>(
    message: GossipMessage,
    relayer: Option<BlsPublicKey>,
) -> NetworkEvent<Req, Res> {
    NetworkEvent::Gossip(message, relayer)
}
