//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{TNCodec, TNMessage},
    error::NetworkError,
    peers::{self, PeerEvent, PeerManager, Penalty},
    send_or_log_error,
    types::{NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult},
    PeerExchangeMap,
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance, Topic,
        TopicHash,
    },
    kad::{self, store::MemoryStore, Mode},
    request_response::{
        self, Codec, Event as ReqResEvent, InboundFailure as ReqResInboundFailure,
        InboundRequestId, OutboundRequestId,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};
use tn_config::{ConsensusConfig, KeyConfig, LibP2pConfig};
use tn_types::{
    encode, try_decode, BlsPublicKey, BlsSignature, BlsSigner, Database, NetworkKeypair,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tracing::{debug, error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
mod network_tests;

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior includes gossipsub and request-response.
#[derive(NetworkBehaviour)]
pub(crate) struct TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// The peer manager.
    pub(crate) peer_manager: peers::PeerManager,
    /// Used for peer discovery.
    pub(crate) kademlia: kad::Behaviour<MemoryStore>,
}

impl<C> TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// Create a new instance of Self.
    pub(crate) fn new<DB: Database>(
        gossipsub: gossipsub::Behaviour,
        req_res: request_response::Behaviour<C>,
        consensus_config: &ConsensusConfig<DB>,
        kademlia: kad::Behaviour<MemoryStore>,
    ) -> Self {
        let peer_manager = PeerManager::new(consensus_config);
        Self { gossipsub, req_res, peer_manager, kademlia }
    }
}

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
pub struct ConsensusNetwork<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// The gossip network for flood publishing sealed batches.
    swarm: Swarm<TNBehavior<TNCodec<Req, Res>>>,
    /// The stream for forwarding network events.
    event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    /// The sender for network handles.
    handle: Sender<NetworkCommand<Req, Res>>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand<Req, Res>>,
    /// The collection of authorized publishers per topic.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify messages
    /// published on certain topics. These are updated when the caller subscribes to a topic.
    authorized_publishers: HashMap<String, HashSet<PeerId>>,
    /// The collection of pending _graceful_ disconnects.
    ///
    /// This node disconnects from new peers if it already has the target number of peers.
    /// For these types of "peer exchange / discovery disconnects", the node shares peer records
    /// before disconnecting. This keeps track of the number of disconnects to ensure resources
    /// aren't starved while waiting for the peer's ack.
    pending_px_disconnects: HashMap<OutboundRequestId, PeerId>,
    /// The collection of pending outbound requests.
    ///
    /// Callers include a oneshot channel for the network to return response. The caller is
    /// responsible for decoding message bytes and reporting peers who return bad data. Peers that
    /// send messages that fail to decode must receive an application score penalty.
    outbound_requests: HashMap<(PeerId, OutboundRequestId), oneshot::Sender<NetworkResult<Res>>>,
    /// The collection of pending inbound requests.
    ///
    /// Callers include a oneshot channel for the network to return a cancellation notice. The
    /// caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    inbound_requests: HashMap<InboundRequestId, oneshot::Sender<()>>,
    /// The configurables for the libp2p consensus network implementation.
    config: LibP2pConfig,
    /// Track peers we have a connection with.
    ///
    /// This explicitly tracked and is a VecDeque so we can use to round robin requests without an
    /// explicit peer.
    connected_peers: VecDeque<PeerId>,
    /// Key manager, provide the BLS public key and sign peer records published to kademlia.
    key_config: KeyConfig,
    /// If true then add peers to kademlia- useful for testing to set false.
    kad_add_peers: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AddrList {
    signature: BlsSignature,
    value: Vec<Multiaddr>,
}

impl<Req, Res> ConsensusNetwork<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Convenience method for spawning a primary network instance.
    pub fn new_for_primary<DB>(
        config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    ) -> NetworkResult<Self>
    where
        DB: tn_types::database_traits::Database,
    {
        let network_key = config.key_config().primary_network_keypair().clone();
        Self::new(config, event_stream, network_key)
    }

    /// Convenience method for spawning a worker network instance.
    pub fn new_for_worker<DB>(
        config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    ) -> NetworkResult<Self>
    where
        DB: tn_types::database_traits::Database,
    {
        let network_key = config.key_config().worker_network_keypair().clone();
        Self::new(config, event_stream, network_key)
    }

    /// Create a new instance of Self.
    pub fn new<DB>(
        consensus_config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
        keypair: NetworkKeypair,
    ) -> NetworkResult<Self>
    where
        DB: tn_types::database_traits::Database,
    {
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

        let tn_codec = TNCodec::<Req, Res>::new(
            consensus_config.network_config().libp2p_config().max_rpc_message_size,
        );

        let req_res = request_response::Behaviour::with_codec(
            tn_codec,
            consensus_config.network_config().libp2p_config().supported_req_res_protocols.clone(),
            request_response::Config::default(),
        );
        let peer_id: PeerId = keypair.public().into();
        let kademlia = kad::Behaviour::new(peer_id, MemoryStore::new(peer_id));

        // create custom behavior
        let behavior = TNBehavior::new(gossipsub, req_res, consensus_config, kademlia);

        // create swarm
        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic_config(|mut config| {
                config.handshake_timeout =
                    consensus_config.network_config().quic_config().handshake_timeout;
                config.max_idle_timeout =
                    consensus_config.network_config().quic_config().max_idle_timeout;
                config.keep_alive_interval =
                    consensus_config.network_config().quic_config().keep_alive_interval;
                config.max_concurrent_stream_limit =
                    consensus_config.network_config().quic_config().max_concurrent_stream_limit;
                config.max_stream_data =
                    consensus_config.network_config().quic_config().max_stream_data;
                config.max_connection_data =
                    consensus_config.network_config().quic_config().max_connection_data;
                config
            })
            .with_behaviour(|_| behavior)
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(
                    consensus_config.network_config().libp2p_config().max_idle_connection_timeout,
                )
            })
            .build();

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let config = consensus_config.network_config().libp2p_config().clone();
        let pending_px_disconnects = HashMap::with_capacity(config.max_px_disconnects);

        Ok(Self {
            swarm,
            handle,
            commands,
            event_stream,
            authorized_publishers: Default::default(),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            config,
            connected_peers: VecDeque::new(),
            pending_px_disconnects,
            key_config: consensus_config.key_config().clone(),
            kad_add_peers: true,
        })
    }

    /// After this call peers will not be added to kademlia, for testing.
    pub fn no_kad_peers_for_test(&mut self) {
        self.kad_add_peers = false;
    }

    /// Return a [NetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Return a kademlia record keyed on our BlsPublicKey with our peer_id and network addresses.
    fn get_peer_record(&self) -> kad::Record {
        let key = kad::RecordKey::new(&encode(&self.key_config.primary_public_key()));
        let value: Vec<Multiaddr> = self.swarm.external_addresses().cloned().collect();
        let peer_id = *self.swarm.local_peer_id();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        let signature = self.key_config.request_signature_direct(&encode(&value));
        let addr_list = AddrList { signature, value };
        kad::Record {
            key: key.clone(),
            value: encode(&addr_list),
            publisher: Some(peer_id),
            expires,
        }
    }

    /// Verify the addres list in Record was signed by the key.
    fn peer_record_valid(&self, record: &kad::Record) -> Option<(BlsPublicKey, AddrList)> {
        let key = try_decode::<BlsPublicKey>(record.key.as_ref()).ok()?;
        let addr_list = try_decode::<AddrList>(record.value.as_ref()).ok()?;
        if addr_list.signature.verify_raw(&encode(&addr_list.value), &key) {
            Some((key, addr_list))
        } else {
            None
        }
    }

    /// Publish and provide our network addresses and peer id under our BLS public key for
    /// discovery.
    fn provide_our_data(&mut self) {
        let record = self.get_peer_record();
        let key = record.key.clone();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            error!(target: "network-kad", "Failed to store record locally: {err}");
        }
        if let Err(err) = self.swarm.behaviour_mut().kademlia.start_providing(key) {
            error!(target: "network-kad", "Failed to start providing key: {err}");
        }
    }

    /// Publish our network addresses and peer id under our BLS public key for discovery.
    fn publish_our_data(&mut self) {
        let record = self.get_peer_record();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            error!(target: "network-kad", "Failed to publish record: {err}");
        }
    }

    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        self.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
        self.provide_our_data();
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.process_event(event).await?,
                command = self.commands.recv() => match command {
                    Some(c) => self.process_command(c),
                    None => {
                        info!(target: "network", "network shutting down...");
                        return Ok(())
                    }
                },
            }
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.authorized_publishers.keys()))]
    async fn process_event(
        &mut self,
        event: SwarmEvent<TNBehaviorEvent<TNCodec<Req, Res>>>,
    ) -> NetworkResult<()> {
        match event {
            SwarmEvent::Behaviour(behavior) => match behavior {
                TNBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                TNBehaviorEvent::ReqRes(event) => self.process_reqres_event(event)?,
                TNBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
                TNBehaviorEvent::Kademlia(event) => self.process_kad_event(event)?,
            },
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
                    target: "network::events",
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
    fn process_command(&mut self, command: NetworkCommand<Req, Res>) {
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
            NetworkCommand::AddExplicitPeer { peer_id, addr, reply } => {
                // update peer manager
                self.swarm.behaviour_mut().peer_manager.add_explicit_peer(
                    peer_id,
                    addr.clone(),
                    reply,
                );
            }
            NetworkCommand::Dial { peer_id, peer_addr, reply } => {
                self.swarm.behaviour_mut().peer_manager.dial_peer(peer_id, peer_addr, reply);
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
            NetworkCommand::ConnectedPeers { reply } => {
                let res = self.swarm.connected_peers().cloned().collect();
                send_or_log_error!(reply, res, "ConnectedPeers");
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
            NetworkCommand::AllMeshPeers { reply } => {
                let collection =
                    self.swarm.behaviour_mut().gossipsub.all_mesh_peers().cloned().collect();
                send_or_log_error!(reply, collection, "AllMeshPeers");
            }
            NetworkCommand::MeshPeers { topic, reply } => {
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .mesh_peers(&TopicHash::from_raw(topic))
                    .cloned()
                    .collect();
                send_or_log_error!(reply, collection, "MeshPeers");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                let request_id = self.swarm.behaviour_mut().req_res.send_request(&peer, request);
                self.outbound_requests.insert((peer, request_id), reply);
            }
            NetworkCommand::SendRequestAny { request, reply } => {
                self.connected_peers.rotate_left(1);
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
                let res = self.swarm.behaviour_mut().req_res.send_response(channel, response);
                send_or_log_error!(reply, res, "SendResponse");
            }
            NetworkCommand::PendingRequestCount { reply } => {
                let count = self.outbound_requests.len();
                send_or_log_error!(reply, count, "SendResponse");
            }
            NetworkCommand::ReportPenalty { peer_id, penalty } => {
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, penalty);
            }
            NetworkCommand::DisconnectPeer { peer_id, reply } => {
                // this is called after timeout for disconnected peer exchanges
                let res = self.swarm.disconnect_peer_id(peer_id);
                send_or_log_error!(reply, res, "DisconnectPeer");
            }
            NetworkCommand::PeerExchange { peers, channel } => {
                self.swarm.behaviour_mut().peer_manager.process_peer_exchange(peers);
                // send empty ack and ignore errors
                let ack = PeerExchangeMap::default().into();
                let _ = self.swarm.behaviour_mut().req_res.send_response(channel, ack);
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

                self.swarm.behaviour_mut().peer_manager.new_epoch(committee);
            }
        }
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
                    // TODO: Issue #253
                    //
                    // forward gossip to handler
                    if let Err(e) = self
                        .event_stream
                        .try_send(NetworkEvent::Gossip(message, propagation_source))
                    {
                        error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?e, "failed to forward gossip!");
                        // fatal - unable to process gossip messages
                        return Err(e.into());
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

    /// Process req/res events.
    fn process_reqres_event(&mut self, event: ReqResEvent<Req, Res>) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id: _ } => {
                match message {
                    request_response::Message::Request { request_id, request, channel } => {
                        let (notify, cancel) = oneshot::channel();
                        // forward request to handler without blocking other events
                        if let Err(e) = self.event_stream.try_send(NetworkEvent::Request {
                            peer,
                            request,
                            channel,
                            cancel,
                        }) {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                            // fatal - unable to process requests
                            return Err(e.into());
                        }

                        self.inbound_requests.insert(request_id, notify);
                    }
                    request_response::Message::Response { request_id, response } => {
                        // check if response associated with PX disconnect
                        if self.pending_px_disconnects.remove(&request_id).is_some() {
                            let _ = self.swarm.disconnect_peer_id(peer);
                        }

                        // try to forward response to original caller
                        let _ = self
                            .outbound_requests
                            .remove(&(peer, request_id))
                            .ok_or(NetworkError::PendingRequestChannelLost)?
                            .send(Ok(response));
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                // handle px disconnects
                //
                // px attempts to support peer discovery, but failures are okay
                // this node disconnects after a px timeout
                if self.pending_px_disconnects.remove(&request_id).is_some() {
                    return Ok(());
                }

                // log errors for other outbound failures
                warn!(target: "network", ?peer, ?error, "outbound failure");

                // apply penalty
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer, Penalty::Medium);

                // try to forward error to original caller
                let _ = self
                    .outbound_requests
                    .remove(&(peer, request_id))
                    .ok_or(NetworkError::PendingRequestChannelLost)?
                    .send(Err(error.into()));
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                match error {
                    ReqResInboundFailure::Io(e) => {
                        // penalize peer since this is an attack surface
                        warn!(target: "network", ?e, ?peer, ?request_id, "inbound IO failure");
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Medium);
                    }
                    ReqResInboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, ?error, "inbound failure: unsupported protocol");

                        // the local peer supports none of the protocols requested by the remote
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Fatal);
                    }
                    ReqResInboundFailure::Timeout | ReqResInboundFailure::ConnectionClosed => {
                        // penalty for potentially malicious request
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Mild);
                    }
                    ReqResInboundFailure::ResponseOmission => { /* ignore local error */ }
                }

                // forward cancelation to handler
                let _ = self
                    .inbound_requests
                    .remove(&request_id)
                    .ok_or(NetworkError::PendingRequestChannelLost)?
                    .send(());
            }
            ReqResEvent::ResponseSent { .. } => {}
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
        //
        // NOTE: expand on this based on gossip::topic - not all topics need to be permissioned
        if gossip.source.is_some_and(|id| {
            self.authorized_publishers.get(topic.as_str()).is_some_and(|auth| auth.contains(&id))
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
                // NOTE: gossipsub handles `FromSwarm::ConnectionClosed`
                let _ = self.swarm.disconnect_peer_id(peer_id);
            }
            PeerEvent::PeerDisconnected(peer_id) => {
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
                        .ok_or(NetworkError::PendingRequestChannelLost)?
                        .send(Err(NetworkError::Disconnected));
                }
            }
            PeerEvent::DisconnectPeerX(peer_id, peer_exchange) => {
                // attempt to exchange peer information if limits allow
                if self.pending_px_disconnects.len() < self.config.max_px_disconnects {
                    let (reply, done) = oneshot::channel();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .req_res
                        .send_request(&peer_id, peer_exchange.into());
                    self.outbound_requests.insert((peer_id, request_id), reply);

                    let timeout = self.config.px_disconnect_timeout;
                    let handle = self.network_handle();

                    // spawn task
                    tokio::spawn(async move {
                        // ignore errors and disconnect after px attempt
                        let _res = tokio::time::timeout(timeout, done).await;
                        let _ = handle.disconnect_peer(peer_id).await;
                    });

                    // insert to pending px disconnects
                    self.pending_px_disconnects.insert(request_id, peer_id);
                } else {
                    // too many px disconnects pending so disconnect without px
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);
            }
            PeerEvent::PeerConnected(peer_id, addr) => {
                // register peer for request-response behaviour
                // NOTE: gossipsub handles `FromSwarm::ConnectionEstablished`
                self.swarm.add_peer_address(peer_id, addr.clone());
                if self.kad_add_peers {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    self.publish_our_data();
                }

                // manage connected peers for
                self.connected_peers.push_back(peer_id);

                // if this is a trusted/validator (important) peer, mark it as explicit in gossipsub
                if self.swarm.behaviour().peer_manager.peer_is_important(&peer_id) {
                    self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }
            PeerEvent::Banned(peer_id) => {
                debug!(target: "network", ?peer_id, "peer banned");
                // blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
            }
            PeerEvent::Unbanned(peer_id) => {
                debug!(target: "network", ?peer_id, "peer unbanned");
                // remove blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&peer_id);
            }
        }

        Ok(())
    }

    fn process_kad_event(&mut self, event: kad::Event) -> NetworkResult<()> {
        match event {
            kad::Event::InboundRequest { request } => {
                info!(target: "network-kad", "inbound {request:?}")
            }
            kad::Event::OutboundQueryProgressed { id: _, result, stats: _, step: _ } => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        key,
                        providers,
                        ..
                    })) => match try_decode::<BlsPublicKey>(key.as_ref()) {
                        Ok(key) => {
                            for peer in providers {
                                info!(target: "network-kad",
                                    "Peer {peer:?} provides key {:?}",
                                    key,
                                );
                            }
                        }
                        Err(err) => {
                            error!(target: "network-kad", "Failed to decode a kad Key: {err}")
                        }
                    },
                    kad::QueryResult::GetProviders(Err(err)) => {
                        error!(target: "network-kad", "Failed to get providers: {err:?}");
                    }
                    kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                        kad::PeerRecord { record, .. },
                    ))) => {
                        if let Some((key, value)) = self.peer_record_valid(&record) {
                            info!(target: "network-kad", "Got record {key} {value:?}");
                        } else {
                            error!(target: "network-kad", "Recieved invalid peer record!");
                        }
                    }
                    kad::QueryResult::GetRecord(Ok(_)) => {}
                    kad::QueryResult::GetRecord(Err(err)) => {
                        error!(target: "network-kad", "Failed to get record: {err:?}");
                    }
                    kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                        match try_decode::<BlsPublicKey>(key.as_ref()) {
                            Ok(key) => {
                                info!(target: "network-kad", "Successfully put record {key}")
                            }
                            Err(err) => {
                                error!(target: "network-kad", "Failed to decode a kad Key: {err}")
                            }
                        }
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        error!(target: "network-kad", "Failed to put record: {err:?}");
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                        match try_decode::<BlsPublicKey>(key.as_ref()) {
                            Ok(key) => {
                                info!(target: "network-kad", "Successfully put provider record {:?}", key)
                            }
                            Err(err) => {
                                error!(target: "network-kad", "Failed to decode a kad Key: {err}")
                            }
                        }
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        error!(target: "network-kad", "Failed to put provider record: {err:?}");
                    }
                    _ => {}
                }
            }
            kad::Event::RoutingUpdated { peer, is_new_peer, addresses, bucket_range, old_peer } => {
                info!(target: "network-kad", "routing updated peer {peer:?} new {is_new_peer} addrs {addresses:?} bucketr {bucket_range:?} old {old_peer:?}")
            }
            kad::Event::UnroutablePeer { peer } => {
                info!(target: "network-kad", "unroutable peer {peer:?}")
            }
            kad::Event::RoutablePeer { peer, address } => {
                info!(target: "network-kad", "routable peer {peer:?}/{address:?}")
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                info!(target: "network-kad", "pending routable peer {peer:?}/{address:?}")
            }
            kad::Event::ModeChanged { new_mode } => {
                info!(target: "network-kad", "mode changed {new_mode:?}")
            }
        }
        Ok(())
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

impl<Req, Res> std::fmt::Debug for ConsensusNetwork<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusNetwork")
            .field("authorized_publishers", &self.authorized_publishers)
            .field("pending_px_disconnects", &self.pending_px_disconnects)
            .field("outbound_requests", &self.outbound_requests.len())
            .field("inbound_requests", &self.inbound_requests.len())
            .field("config", &self.config)
            .field("connected_peers", &self.connected_peers)
            .field("swarm", &"<swarm>") // Skip detailed debug for swarm
            .finish()
    }
}
