//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::TNMessage,
    error::NetworkError,
    kad::{KadStore, KadStoreType},
    peers::{self, PeerEvent, PeerExchangeMap, PeerManager, Penalty},
    send_or_log_error,
    stream::{
        spawn_stream_tasks, FrameHeader, ReadEvent, StreamError, StreamErrorCode, StreamHandle,
        StreamHandlerConfig, TN_STREAM_PROTOCOL,
    },
    types::{
        KadQuery, NetworkCommand, NetworkEvent, NetworkHandle, NetworkInfo, NetworkResult,
        NodeRecord, StreamResponseChannel,
    },
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance, Topic,
        TopicHash,
    },
    kad::{self, store::RecordStore, Mode, QueryId},
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use libp2p_stream as stream;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tn_config::{KeyConfig, LibP2pConfig, NetworkConfig, PeerConfig};
use tn_types::{
    decode, encode, encode_into_buffer, now, try_decode, BlsPublicKey, BlsSigner, Database,
    NetworkKeypair, NetworkPublicKey, TaskSpawner, TnSender,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tracing::{debug, error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
mod network_tests;

/// Kademlia stream protocol.
const DEFAULT_KAD_PROTO_NAME: StreamProtocol = StreamProtocol::new("/tn/kad/1.0.0");

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior includes gossipsub, kademlia, peer management, and stream-based messaging.
#[derive(NetworkBehaviour)]
pub(crate) struct TNBehavior<DB>
where
    DB: Database,
{
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The peer manager.
    pub(crate) peer_manager: peers::PeerManager,
    /// Used for peer discovery.
    pub(crate) kademlia: kad::Behaviour<KadStore<DB>>,
    /// Stream-based messaging for large data transfers.
    pub(crate) stream: stream::Behaviour,
}

impl<DB> TNBehavior<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        gossipsub: gossipsub::Behaviour,
        kademlia: kad::Behaviour<KadStore<DB>>,
        peer_config: &PeerConfig,
    ) -> Self {
        let peer_manager = PeerManager::new(peer_config);
        let stream = stream::Behaviour::new();
        Self { gossipsub, peer_manager, kademlia, stream }
    }

    /// Get a control handle for opening streams.
    pub(crate) fn stream_control(&self) -> stream::Control {
        self.stream.new_control()
    }
}

/// Events from background stream tasks back to ConsensusNetwork.
#[derive(Debug)]
enum StreamTaskEvent {
    /// Received an inbound request from a peer.
    InboundRequest { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Received a response to an outbound request.
    Response { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Received an error response from a peer.
    ErrorResponse { peer_id: PeerId, request_id: u64, error: String },
    /// Stream closed or errored.
    StreamClosed { peer_id: PeerId, error: Option<String> },
    /// Error sending request.
    SendError { peer_id: PeerId, request_id: u64, error: String },
    /// Response bytes ready to send (from application layer).
    SendResponse { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Stream successfully opened - store the handle and receiver.
    StreamOpened { peer_id: PeerId, handle: StreamHandle, events_rx: mpsc::Receiver<ReadEvent> },
    /// Failed to open stream.
    StreamOpenFailed { peer_id: PeerId, request_id: u64, error: String },
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
    swarm: Swarm<TNBehavior<DB>>,
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
    /// Stream control for opening new streams.
    stream_control: stream::Control,
    /// Active stream handles per peer (long-lived).
    stream_handles: HashMap<PeerId, StreamHandle>,
    /// Next request ID counter for stream requests.
    next_request_id: Arc<AtomicU64>,
    /// Pending outbound requests: (PeerId, request_id) -> reply channel.
    outbound_requests: HashMap<(PeerId, u64), oneshot::Sender<NetworkResult<Res>>>,
    /// Pending inbound requests: (PeerId, request_id) -> cancel channel.
    inbound_requests: HashMap<(PeerId, u64), oneshot::Sender<()>>,
    /// Channel to receive events from background stream tasks.
    stream_events_tx: mpsc::Sender<StreamTaskEvent>,
    stream_events_rx: mpsc::Receiver<StreamTaskEvent>,
    /// Stream handler configuration.
    stream_handler_config: StreamHandlerConfig,
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
    /// Phantom marker for request/response types.
    _phantom: std::marker::PhantomData<(Req, Res)>,
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
            KadStoreType::Primary,
            external_addr,
        )
    }

    /// Convenience method for spawning a worker network instance.
    pub fn new_for_worker(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
    ) -> NetworkResult<Self> {
        let network_key = key_config.worker_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            KadStoreType::Worker,
            external_addr,
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
        kad_type: KadStoreType,
        external_addr: Multiaddr,
    ) -> NetworkResult<Self> {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // TN specific: filter against authorized_publishers for certain topics
            .validate_messages()
            .build()?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(NetworkError::GossipBehavior)?;

        let peer_id: PeerId = keypair.public().into();
        let mut kad_config = libp2p::kad::Config::new(DEFAULT_KAD_PROTO_NAME);
        // manually add peers
        kad_config.set_kbucket_inserts(kad::BucketInserts::Manual);
        kad_config.set_kbucket_size(network_config.libp2p_config().k_bucket_size);
        let two_days = Some(Duration::from_secs(48 * 60 * 60));
        let twelve_hours = Some(Duration::from_secs(12 * 60 * 60));
        kad_config
            .set_record_ttl(two_days)
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            .set_publication_interval(twelve_hours)
            .set_query_timeout(Duration::from_secs(60))
            .set_provider_record_ttl(two_days);
        let kad_store = KadStore::new(db.clone(), &key_config, kad_type);
        let kademlia = kad::Behaviour::with_config(peer_id, kad_store.clone(), kad_config);

        // create custom behavior
        let mut behavior = TNBehavior::new(gossipsub, kademlia, network_config.peer_config());

        // Load the Kad records from DB into the local peer cache.
        for record in kad_store.records() {
            match BlsPublicKey::from_literal_bytes(record.key.as_ref()) {
                Ok(key) => {
                    let record: NodeRecord = decode(&record.value);
                    behavior.peer_manager.add_known_peer(key, record.info);
                }
                // How did we get a KAD record with a broken key?
                Err(error) => {
                    error!(target: "network-kad", ?error, "Invalid/corrupt KAD DB store!");
                }
            }
        }

        let network_pubkey = keypair.public().into();

        // get stream control before moving behavior into swarm
        let stream_control = behavior.stream_control();

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
        let node_record = Self::create_node_record(external_addr, &key_config, network_pubkey);

        // create stream event channels
        let (stream_events_tx, stream_events_rx) = mpsc::channel(256);

        // create stream handler config
        let stream_config = network_config.stream_config();
        let stream_handler_config = StreamHandlerConfig {
            max_frame_size: stream_config.max_frame_size,
            read_timeout: stream_config.request_timeout,
            write_buffer_size: 64,
        };

        Ok(Self {
            swarm,
            handle,
            commands,
            event_stream,
            authorized_publishers: Default::default(),
            stream_control,
            stream_handles: Default::default(),
            next_request_id: Arc::new(AtomicU64::new(1)),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            stream_events_tx,
            stream_events_rx,
            stream_handler_config,
            kad_record_queries: Default::default(),
            config,
            connected_peers: VecDeque::new(),
            key_config,
            task_spawner,
            node_record,
            _phantom: std::marker::PhantomData,
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
    ) -> NodeRecord {
        NodeRecord::build(network_pubkey, external_addr, |data| {
            key_config.request_signature_direct(data)
        })
    }

    /// Return a kademlia record keyed on our BlsPublicKey with our peer_id and network addresses.
    /// Return None if we don't have any confirmed external addresses yet.
    fn get_peer_record(&self) -> kad::Record {
        let key = kad::RecordKey::new(&self.key_config.primary_public_key());
        kad::Record {
            key: key.clone(),
            value: encode(&self.node_record),
            publisher: Some(*self.swarm.local_peer_id()),
            expires: None, // never expire
        }
    }

    /// Verify the address list in Record was signed by the key and the kad record's publisher
    /// matches the network key.
    fn peer_record_valid(&self, record: &kad::Record) -> Option<(BlsPublicKey, NodeRecord)> {
        let key = BlsPublicKey::from_literal_bytes(record.key.as_ref()).ok()?;
        let node_record = try_decode::<NodeRecord>(record.value.as_ref()).ok()?;

        // verify bls signature
        let verified = node_record.verify(&key)?;

        // verify publisher matches the network public key in the record
        // this prevents replay attacks where malicious nodes republish outdated records
        let expected_peer_id: PeerId = verified.1.info.pubkey.clone().into();
        if record.publisher != Some(expected_peer_id) {
            warn!(
                target: "network-kad",
                "NodeRecord validation failed: publisher {:?} doesn't match network key (expected {:?})",
                record.publisher, expected_peer_id
            );
            return None;
        }

        Some(verified)
    }

    /// Publish and provide our network addresses and peer id under our BLS public key for
    /// discovery.
    fn provide_our_data(&mut self) {
        let record = self.get_peer_record();
        info!(target: "network-kad", ?record, "Providing our record to kademlia for peer {:?}", self.swarm.local_peer_id());
        let key = record.key.clone();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            error!(target: "network-kad", "Failed to store record locally: {err}");
        }
        if let Err(err) = self.swarm.behaviour_mut().kademlia.start_providing(key) {
            error!(target: "network-kad", "Failed to start providing key: {err}");
        }
    }

    /// Publish our network addresses and peer id AND to the network under our BLS public key for
    /// discovery.
    fn publish_our_data_to_peer(&mut self, peer: PeerId) {
        let record = self.get_peer_record();
        info!(target: "network-kad", "Publishing our record to kademlia");
        // Publish to the specified peer.
        let _ = self.swarm.behaviour_mut().kademlia.put_record_to(
            record.clone(),
            vec![peer].into_iter(),
            kad::Quorum::One,
        );

        // Also publish our record locally and to the network.
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            error!(target: "network-kad", "Failed to publish record: {err}");
        }
    }

    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        // add peer record if address confirmed
        self.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
        self.provide_our_data();

        // accept incoming streams
        let mut incoming_streams = self
            .stream_control
            .accept(TN_STREAM_PROTOCOL)
            .map_err(|e| NetworkError::Stream(format!("protocol already registered: {e:?}")))?;

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.process_event(event).await.inspect_err(|e| {
                    error!(target: "network", ?e, "network event error")
                })?,
                command = self.commands.recv() => match command {
                    Some(c) => self.process_command(c).inspect_err(|e| {
                        error!(target: "network", ?e, "network command error")
                    })?,
                    None => {
                        info!(target: "network", "network shutting down...");
                        return Ok(())
                    }
                },
                // handle incoming streams from peers
                Some((peer_id, stream)) = incoming_streams.next() => {
                    self.handle_incoming_stream(peer_id, stream);
                }
                // handle events from background stream tasks
                Some(event) = self.stream_events_rx.recv() => {
                    self.process_stream_task_event(event)?;
                }
            }
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.authorized_publishers.keys()))]
    async fn process_event(&mut self, event: SwarmEvent<TNBehaviorEvent<DB>>) -> NetworkResult<()> {
        match event {
            SwarmEvent::Behaviour(behavior) => match behavior {
                TNBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                TNBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
                TNBehaviorEvent::Kademlia(event) => self.process_kad_event(event)?,
                TNBehaviorEvent::Stream(event) => self.process_stream_event(event)?,
            },
            SwarmEvent::ExternalAddrConfirmed { address: _ } => {
                // New confirmed address so lets publish/update or kademlia address rocord.
                self.provide_our_data();
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
            NetworkCommand::UpdateAuthorizedPublishers { authorities, reply } => {
                // this value should be updated at the start of each epoch
                self.authorized_publishers = authorities;
                send_or_log_error!(reply, Ok(()), "UpdateAuthorizedPublishers");
            }
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
                if let Some((peer_id, _addr)) =
                    self.swarm.behaviour().peer_manager.auth_to_peer(peer)
                {
                    debug!(target: "network", "trying to send to {peer_id}");
                    let request_id = self.next_request_id();
                    let payload = Self::encode_message(&request);

                    // store the reply channel
                    self.outbound_requests.insert((peer_id, request_id), reply);

                    // spawn send request task
                    self.spawn_send_request_task(peer_id, request_id, payload);
                } else {
                    // best effort to return an error to caller
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::SendRequestDirect { peer, request, reply } => {
                let request_id = self.next_request_id();
                let payload = Self::encode_message(&request);

                // store the reply channel
                self.outbound_requests.insert((peer, request_id), reply);

                // spawn send request task
                self.spawn_send_request_task(peer, request_id, payload);
            }
            NetworkCommand::SendRequestAny { request, reply } => {
                // rotating an empty list will panic...
                if !self.connected_peers.is_empty() {
                    self.connected_peers.rotate_left(1);
                }
                if let Some(peer) = self.connected_peers.front().copied() {
                    let request_id = self.next_request_id();
                    let payload = Self::encode_message(&request);

                    // store the reply channel
                    self.outbound_requests.insert((peer, request_id), reply);

                    // spawn send request task
                    self.spawn_send_request_task(peer, request_id, payload);
                } else {
                    // ignore error since this means other end lost interest
                    let _ = reply.send(Err(NetworkError::NoPeers));
                }
            }
            NetworkCommand::SendResponse { response, channel, reply } => {
                // encode the response and send it through the channel
                let payload = Self::encode_message(&response);
                match channel.response_tx.send(payload) {
                    Ok(()) => {
                        send_or_log_error!(reply, Ok(()), "SendResponse");
                    }
                    Err(_) => {
                        // channel was closed, return error response
                        send_or_log_error!(reply, Err(response), "SendResponse");
                    }
                }
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
            NetworkCommand::NewEpoch { committee } => {
                // at the start of a new epoch, each node needs to know:
                // - the current committee
                // - all staked nodes who will vote at the end of the epoch
                //      - only synced nodes can vote
                //
                // once a node stakes and tries to sync, it would be nice
                // if it could receive priority on the network for syncing
                // state
                //
                // for now, this only supports the current committee for the epoch

                info!(target: "network", this_node=?self.swarm.local_peer_id(), "network update for next committee - ensuring no committee members are banned");
                // ensure that the next committee isn't banned
                self.swarm.behaviour_mut().peer_manager.new_epoch(committee);
            }
            NetworkCommand::FindAuthorities { bls_keys } => {
                // this will trigger a PeerEvent to fetch records through kad if not in the peer map
                self.swarm.behaviour_mut().peer_manager.find_authorities(bls_keys);
            }
        }

        Ok(())
    }

    /// Process gossip events.
    fn process_gossip_event(&mut self, event: GossipEvent) -> NetworkResult<()> {
        match event {
            GossipEvent::Message { propagation_source, message_id, message } => {
                trace!(target: "network", topic=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?message, "message received from publisher");
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
                    // We should not be able to recieve a message from an unknown peer so this
                    // should always work.
                    if let Some(bls) =
                        self.swarm.behaviour().peer_manager.peer_to_bls(&propagation_source)
                    {
                        // forward gossip to handler
                        if let Err(e) =
                            self.event_stream.try_send(NetworkEvent::Gossip(message, bls))
                        {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?e, "failed to forward gossip!");
                            // ignore failures at the epoch boundary
                            // During epoch change the event_stream reciever can be closed.
                            return Ok(());
                        }
                    }
                } else {
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

                // clean up stream handle
                self.stream_handles.remove(&peer_id);

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
                    let _ = self
                        .outbound_requests
                        .remove(&k)
                        .map(|ack| ack.send(Err(NetworkError::Disconnected)));
                }

                // cancel pending inbound requests from this peer
                let inbound_keys: Vec<_> = self
                    .inbound_requests
                    .keys()
                    .filter(|(pid, _)| *pid == peer_id)
                    .cloned()
                    .collect();

                for key in inbound_keys {
                    if let Some(cancel_tx) = self.inbound_requests.remove(&key) {
                        let _ = cancel_tx.send(());
                    }
                }
            }
            PeerEvent::DisconnectPeerX(peer_id, peer_exchange) => {
                debug!(target: "peer-manager", this_node=?self.swarm.local_peer_id(), ?peer_id, "disconnecting from peer with exchange info");

                let timeout = self.config.px_disconnect_timeout;
                let network_handle = self.network_handle();

                // encode peer exchange as a request
                let request_id = self.next_request_id();
                let payload = Self::encode_message(&Req::from(peer_exchange));
                let header = FrameHeader::typed_request(request_id, payload.len() as u32);

                // attempt to send peer exchange info via stream before disconnecting
                if let Some(handle) = self.stream_handles.get(&peer_id).cloned() {
                    // spawn task to send px and then disconnect
                    let task_name = format!("peer-exchange-{peer_id}");
                    self.task_spawner.spawn_task(task_name, async move {
                        // best effort send with timeout - ignore errors
                        let send_fut = handle.send_frame(header, payload);
                        let _ = tokio::time::timeout(timeout, send_fut).await;

                        // disconnect after px attempt
                        let _ = network_handle.disconnect_peer(peer_id).await;
                    });
                } else {
                    // no stream exists - need to open one first
                    let mut stream_control = self.stream_control.clone();
                    let stream_handler_config = self.stream_handler_config.clone();

                    let task_name = format!("peer-exchange-open-{peer_id}");
                    self.task_spawner.spawn_task(task_name, async move {
                        // try to open stream with timeout
                        let open_result = tokio::time::timeout(
                            timeout,
                            stream_control.open_stream(peer_id, TN_STREAM_PROTOCOL),
                        )
                        .await;

                        if let Ok(Ok(stream)) = open_result {
                            // spawn read/write tasks
                            let (handle, _events_rx) =
                                spawn_stream_tasks(stream, peer_id, stream_handler_config);

                            // best effort send with timeout - ignore errors
                            let _ =
                                tokio::time::timeout(timeout, handle.send_frame(header, payload))
                                    .await;
                        }

                        // disconnect after px attempt (regardless of success)
                        let _ = network_handle.disconnect_peer(peer_id).await;
                    });
                }

                // remove peer from kad - will redial if necessary
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);
            }
            PeerEvent::PeerConnected(peer_id, addr) => {
                // register peer for request-response behaviour
                // NOTE: gossipsub handles `FromSwarm::ConnectionEstablished`
                self.swarm.add_peer_address(peer_id, addr.clone());
                // add as a kademlia peer
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                self.publish_our_data_to_peer(peer_id);

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
                        if let Some((key, value)) = self.peer_record_valid(&record) {
                            trace!(target: "network-kad", "Got record {key} {value:?}");
                            self.process_kad_query_result(&query_id, record, peer, step.last);
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
                        // TODO: configure caching and see issue #301
                        // self.swarm.behaviour_mut().kademlia.put_record_to(record, peers, quorum);

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
                        error!(target: "network-kad", "Failed to put record: {err:?}");
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

    /// Process an event from the stream behaviour.
    ///
    /// Note: The stream::Behaviour has `ToSwarm = ()` (void event type).
    /// Actual stream handling is done through the Control handle, not through events.
    fn process_stream_event(&mut self, _event: ()) -> NetworkResult<()> {
        // The stream behaviour doesn't emit meaningful events.
        // Incoming streams are handled through the Control::accept() mechanism.
        Ok(())
    }

    /// Handle an incoming stream from a peer.
    fn handle_incoming_stream(&mut self, peer_id: PeerId, stream: libp2p::swarm::Stream) {
        debug!(target: "network", ?peer_id, "handling incoming stream");

        // spawn read/write tasks
        let (handle, events_rx) =
            spawn_stream_tasks(stream, peer_id, self.stream_handler_config.clone());

        // spawn task to forward events to main loop
        let events_tx = self.stream_events_tx.clone();
        self.task_spawner.spawn_task(format!("stream-read-{peer_id}"), async move {
            Self::stream_read_loop(peer_id, events_rx, events_tx).await;
        });

        // store the handle (may replace existing)
        self.stream_handles.insert(peer_id, handle);
    }

    /// Background task that reads from a peer's stream and forwards events.
    async fn stream_read_loop(
        peer_id: PeerId,
        mut events_rx: mpsc::Receiver<ReadEvent>,
        events_tx: mpsc::Sender<StreamTaskEvent>,
    ) {
        debug!(target: "network", ?peer_id, "stream_read_loop started");
        while let Some(event) = events_rx.recv().await {
            debug!(target: "network", ?peer_id, ?event, "stream_read_loop received event");
            let task_event = match event {
                ReadEvent::Request { request_id, payload } => {
                    StreamTaskEvent::InboundRequest { peer_id, request_id, payload }
                }
                ReadEvent::Response { request_id, payload } => {
                    StreamTaskEvent::Response { peer_id, request_id, payload }
                }
                ReadEvent::Closed => StreamTaskEvent::StreamClosed { peer_id, error: None },
                ReadEvent::ReadError { error } => {
                    StreamTaskEvent::StreamClosed { peer_id, error: Some(error.to_string()) }
                }
                ReadEvent::Error { request_id, error } => {
                    StreamTaskEvent::ErrorResponse { peer_id, request_id, error: error.message }
                }
                // For now, ignore raw stream events (not used for request-response)
                ReadEvent::RawStreamBegin { .. }
                | ReadEvent::RawStreamEnd { .. }
                | ReadEvent::RawData { .. }
                | ReadEvent::Cancelled { .. } => continue,
            };

            if events_tx.send(task_event).await.is_err() {
                // main loop shut down
                debug!(target: "network", ?peer_id, "stream_read_loop: main loop shut down");
                break;
            }
        }
        debug!(target: "network", ?peer_id, "stream_read_loop ended (channel closed)");
    }

    /// Process events from background stream tasks.
    fn process_stream_task_event(&mut self, event: StreamTaskEvent) -> NetworkResult<()> {
        match event {
            StreamTaskEvent::InboundRequest { peer_id, request_id, payload } => {
                // forward to application layer
                if let Some(bls) = self.swarm.behaviour().peer_manager.peer_to_bls(&peer_id) {
                    // decode the request - handle errors gracefully
                    let request: Req = match try_decode(&payload) {
                        Ok(req) => req,
                        Err(e) => {
                            warn!(target: "network", ?peer_id, ?e, "failed to decode inbound request");
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer_id, Penalty::Medium);
                            // send error response to the sender
                            if let Some(handle) = self.stream_handles.get(&peer_id).cloned() {
                                let error =
                                    StreamError::new(StreamErrorCode::BadRequest, e.to_string());
                                let error_payload = encode(&error);
                                let header =
                                    FrameHeader::error(request_id, error_payload.len() as u32);
                                self.task_spawner.spawn_task(
                                    format!("send-error-{peer_id}-{request_id}"),
                                    async move {
                                        let _ = handle.send_frame(header, error_payload).await;
                                    },
                                );
                            }
                            return Ok(());
                        }
                    };

                    // intercept peer exchange messages - handle at network layer
                    if let Some(peers) = request.peer_exchange_msg() {
                        debug!(target: "network", ?peers, "processing peer exchange");
                        self.swarm.behaviour_mut().peer_manager.process_peer_exchange(peers);

                        // send empty ack response
                        if let Some(handle) = self.stream_handles.get(&peer_id).cloned() {
                            let ack = PeerExchangeMap::default();
                            let ack_payload = Self::encode_message(&Res::from(ack));
                            let header =
                                FrameHeader::typed_response(request_id, ack_payload.len() as u32);
                            self.task_spawner.spawn_task(
                                format!("px-ack-{peer_id}-{request_id}"),
                                async move {
                                    let _ = handle.send_frame(header, ack_payload).await;
                                },
                            );
                        }

                        // temporarily ban the sender to prevent reconnection attempts
                        // the sender already rejected us, so we should not try to reconnect
                        self.swarm.behaviour_mut().peer_manager.temporarily_ban_peer(peer_id);

                        return Ok(());
                    }

                    // create response channel
                    let (response_tx, response_rx) = oneshot::channel();
                    let channel = StreamResponseChannel::new(peer_id, request_id, response_tx);

                    // create cancel channel
                    let (notify, cancel) = oneshot::channel();

                    // spawn task to wait for response and send it back
                    let stream_events_tx = self.stream_events_tx.clone();
                    self.task_spawner.spawn_task(
                        format!("stream-response-{peer_id}-{request_id}"),
                        async move {
                            if let Ok(response_bytes) = response_rx.await {
                                let _ = stream_events_tx
                                    .send(StreamTaskEvent::SendResponse {
                                        peer_id,
                                        request_id,
                                        payload: response_bytes,
                                    })
                                    .await;
                            }
                        },
                    );

                    // forward request to handler
                    if let Err(e) = self.event_stream.try_send(NetworkEvent::Request {
                        peer: bls,
                        request,
                        channel,
                        cancel,
                    }) {
                        error!(target: "network", ?e, "failed to forward request!");
                        // ignore failures at the epoch boundary
                        return Ok(());
                    }

                    // store the request and cancel duplicate requests
                    if let Some(cancel_tx) =
                        self.inbound_requests.insert((peer_id, request_id), notify)
                    {
                        // cancel if this is a duplicate request
                        warn!(target: "network", ?peer_id, "duplicate request id from peer");
                        let _ = cancel_tx.send(());
                    }
                } else {
                    warn!(target: "network", ?peer_id, "request from unknown peer");
                }
            }
            StreamTaskEvent::Response { peer_id, request_id, payload } => {
                if let Some(reply) = self.outbound_requests.remove(&(peer_id, request_id)) {
                    // decode the response - handle errors gracefully
                    match try_decode::<Res>(&payload) {
                        Ok(response) => {
                            let _ = reply.send(Ok(response));
                        }
                        Err(e) => {
                            // return error to caller instead of panicking
                            let _ =
                                reply.send(Err(NetworkError::Stream(format!("decode error: {e}"))));
                            // apply penalty for malformed response
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer_id, Penalty::Medium);
                        }
                    }
                }
            }
            StreamTaskEvent::ErrorResponse { peer_id, request_id, error } => {
                // received an error response from the peer (e.g., failed to decode our request)
                if let Some(reply) = self.outbound_requests.remove(&(peer_id, request_id)) {
                    let _ = reply.send(Err(NetworkError::Stream(error)));
                }
            }
            StreamTaskEvent::StreamClosed { peer_id, error } => {
                debug!(target: "network", ?peer_id, ?error, "stream closed event received");
                self.handle_stream_closed(peer_id);
            }
            StreamTaskEvent::SendError { peer_id, request_id, error } => {
                if let Some(reply) = self.outbound_requests.remove(&(peer_id, request_id)) {
                    let _ = reply.send(Err(NetworkError::Stream(error)));
                }
            }
            StreamTaskEvent::SendResponse { peer_id, request_id, payload } => {
                // send response through the stream
                if let Some(handle) = self.stream_handles.get(&peer_id) {
                    let handle = handle.clone();
                    self.task_spawner.spawn_task(
                        format!("send-response-{peer_id}-{request_id}"),
                        async move {
                            let header =
                                FrameHeader::typed_response(request_id, payload.len() as u32);
                            if let Err(e) = handle.send_frame(header, payload).await {
                                warn!(target: "network", ?peer_id, ?e, "failed to send response");
                            }
                        },
                    );
                }
            }
            StreamTaskEvent::StreamOpened { peer_id, handle, events_rx } => {
                debug!(target: "network", ?peer_id, "stream opened - storing handle and spawning read loop");
                // store the handle if not already present (another task may have added it)
                self.stream_handles.entry(peer_id).or_insert(handle);

                // spawn task to forward events from this stream to main loop
                let events_tx = self.stream_events_tx.clone();
                self.task_spawner.spawn_task(format!("stream-read-{peer_id}"), async move {
                    Self::stream_read_loop(peer_id, events_rx, events_tx).await;
                });
            }
            StreamTaskEvent::StreamOpenFailed { peer_id, request_id, error } => {
                debug!(target: "network", ?peer_id, ?error, "failed to open stream");
                if let Some(reply) = self.outbound_requests.remove(&(peer_id, request_id)) {
                    let _ = reply.send(Err(NetworkError::Stream(error)));
                }
            }
        }
        Ok(())
    }

    /// Handle stream closed - clean up handles and fail pending requests.
    fn handle_stream_closed(&mut self, peer_id: PeerId) {
        self.stream_handles.remove(&peer_id);

        // fail pending outbound requests to this peer
        let to_fail: Vec<_> =
            self.outbound_requests.keys().filter(|(pid, _)| *pid == peer_id).cloned().collect();

        for key in to_fail {
            if let Some(reply) = self.outbound_requests.remove(&key) {
                let _ = reply.send(Err(NetworkError::Disconnected));
            }
        }

        // cancel pending inbound requests from this peer
        let to_cancel: Vec<_> =
            self.inbound_requests.keys().filter(|(pid, _)| *pid == peer_id).cloned().collect();

        for key in to_cancel {
            if let Some(cancel_tx) = self.inbound_requests.remove(&key) {
                let _ = cancel_tx.send(());
            }
        }
    }

    /// Generate the next request ID.
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Spawn a task to send a request to a peer.
    ///
    /// If a stream handle already exists, the request is sent immediately.
    /// Otherwise, a new stream is opened first.
    fn spawn_send_request_task(&mut self, peer_id: PeerId, request_id: u64, payload: Vec<u8>) {
        let header = FrameHeader::typed_request(request_id, payload.len() as u32);
        let events_tx = self.stream_events_tx.clone();

        if let Some(handle) = self.stream_handles.get(&peer_id).cloned() {
            // stream already exists, just send
            self.task_spawner.spawn_task(
                format!("send-request-{peer_id}-{request_id}"),
                async move {
                    if let Err(e) = handle.send_frame(header, payload).await {
                        let _ = events_tx
                            .send(StreamTaskEvent::SendError {
                                peer_id,
                                request_id,
                                error: e.to_string(),
                            })
                            .await;
                    }
                },
            );
        } else {
            // need to open stream first
            let mut stream_control = self.stream_control.clone();
            let stream_handler_config = self.stream_handler_config.clone();

            self.task_spawner.spawn_task(
                format!("open-stream-send-{peer_id}-{request_id}"),
                async move {
                    // try to open stream
                    match stream_control.open_stream(peer_id, TN_STREAM_PROTOCOL).await {
                        Ok(stream) => {
                            // spawn read/write tasks
                            let (handle, events_rx) =
                                spawn_stream_tasks(stream, peer_id, stream_handler_config);

                            // notify main loop about the new stream
                            let _ = events_tx
                                .send(StreamTaskEvent::StreamOpened {
                                    peer_id,
                                    handle: handle.clone(),
                                    events_rx,
                                })
                                .await;

                            // send the request
                            if let Err(e) = handle.send_frame(header, payload).await {
                                let _ = events_tx
                                    .send(StreamTaskEvent::SendError {
                                        peer_id,
                                        request_id,
                                        error: e.to_string(),
                                    })
                                    .await;
                            }
                        }
                        Err(e) => {
                            let _ = events_tx
                                .send(StreamTaskEvent::StreamOpenFailed {
                                    peer_id,
                                    request_id,
                                    error: e.to_string(),
                                })
                                .await;
                        }
                    }
                },
            );
        }
    }

    /// Encode a message to bytes using BCS and compression.
    fn encode_message<M: TNMessage>(msg: &M) -> Vec<u8> {
        let mut buffer = Vec::new();
        encode_into_buffer(&mut buffer, msg).expect("encoding should not fail");
        buffer
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
                // mild punishment for old record
                trace!(target: "network-kad", ?source, "processing mild penalty for old record");
                self.swarm.behaviour_mut().peer_manager.process_penalty(source, Penalty::Mild);
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
                try_decode::<NodeRecord>(&existing.value),
                try_decode::<NodeRecord>(&record.value),
            ) {
                (Ok(existing_record), Ok(new_record)) => {
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

    /// Logic to process a kad record request.
    ///
    /// This method checks:
    /// - the peer record is signed
    /// - the returned key matches the request
    /// - the latest node record is used
    fn process_kad_query_result(
        &mut self,
        query_id: &QueryId,
        record: kad::Record,
        peer: Option<PeerId>,
        is_last_step: bool,
    ) {
        // ensure returned record is valid, otherwise assess penalty
        if let Some((key, new_record)) = self.peer_record_valid(&record) {
            trace!(target: "network-kad", "Got record {key} {new_record:?}");
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
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .process_penalty(peer_id, Penalty::Fatal);
                }
            }
        } else {
            // record signature invalid
            warn!(target: "network-kad", "Received invalid peer record!");

            // assess penalty for invalid peer record
            if let Some(peer_id) = peer {
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
            .field("stream_handles", &self.stream_handles.len())
            .field("outbound_requests", &self.outbound_requests.len())
            .field("inbound_requests", &self.inbound_requests.len())
            .field("config", &self.config)
            .field("connected_peers", &self.connected_peers)
            .field("swarm", &"<swarm>")
            .finish()
    }
}
