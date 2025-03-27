//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{TNCodec, TNMessage},
    error::NetworkError,
    peers::{self, PeerEvent, PeerManager},
    send_or_log_error,
    types::{NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult},
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance,
    },
    multiaddr::Protocol,
    request_response::{
        self, Codec, Event as ReqResEvent, InboundFailure as ReqResInboundFailure,
        InboundRequestId, OutboundRequestId,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId, Swarm, SwarmBuilder,
};
use std::{
    collections::{hash_map, HashMap, HashSet, VecDeque},
    time::Duration,
};
use tn_config::{ConsensusConfig, LibP2pConfig};
use tn_types::{network_public_key_to_libp2p, Database};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tracing::{error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
mod network_tests;

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior includes gossipsub and request-response.
#[derive(NetworkBehaviour)]
pub struct TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// The peer manager.
    pub(crate) peer_manager: peers::PeerManager,
}

impl<C> TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// Create a new instance of Self.
    pub fn new<DB: Database>(
        gossipsub: gossipsub::Behaviour,
        req_res: request_response::Behaviour<C>,
        consensus_config: &ConsensusConfig<DB>,
    ) -> Self {
        let peer_manager = PeerManager::new(consensus_config);
        Self { gossipsub, req_res, peer_manager }
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
    /// The subscribed gossip network topics.
    topics: Vec<IdentTopic>,
    /// The stream for forwarding network events.
    event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    /// The sender for network handles.
    handle: Sender<NetworkCommand<Req, Res>>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand<Req, Res>>,
    /// The collection of staked validators.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify message sources
    /// are from validators.
    authorized_publishers: HashSet<PeerId>,
    /// The collection of pending dials.
    pending_dials: HashMap<PeerId, oneshot::Sender<NetworkResult<()>>>,
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
    /// This explicitly tracked and is a VecDeque so we can use to round robin requests without an
    /// explicit peer.
    connected_peers: VecDeque<PeerId>,
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
        let topics = vec![IdentTopic::new("tn-primary")];
        let network_key = config.key_config().primary_network_keypair().as_ref().to_vec();
        let authorized_publishers = config.committee_peer_ids();
        Self::new(config, event_stream, topics, network_key, authorized_publishers)
    }

    /// Convenience method for spawning a worker network instance.
    pub fn new_for_worker<DB>(
        config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
    ) -> NetworkResult<Self>
    where
        DB: tn_types::database_traits::Database,
    {
        let topics = vec![IdentTopic::new("tn-worker")];
        let network_key = config.key_config().worker_network_keypair().as_ref().to_vec();
        let authorized_publishers = config
            .worker_cache()
            .all_workers()
            .iter()
            .map(|(id, _)| network_public_key_to_libp2p(id))
            .collect();
        Self::new(config, event_stream, topics, network_key, authorized_publishers)
    }

    /// Create a new instance of Self.
    pub fn new<DB>(
        consensus_config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
        topics: Vec<IdentTopic>,
        mut ed25519_private_key_bytes: Vec<u8>,
        authorized_publishers: HashSet<PeerId>,
    ) -> NetworkResult<Self>
    where
        DB: tn_types::database_traits::Database,
    {
        // create libp2p keypair from ed25519 secret bytes
        let keypair =
            libp2p::identity::Keypair::ed25519_from_bytes(&mut ed25519_private_key_bytes)?;

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // support peer exchange
            .do_px()
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

        // create custom behavior
        let behavior = TNBehavior::new(gossipsub, req_res, consensus_config);

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
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let config = consensus_config.network_config().libp2p_config().clone();

        Ok(Self {
            swarm,
            topics,
            handle,
            commands,
            event_stream,
            authorized_publishers,
            pending_dials: Default::default(),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            config,
            connected_peers: VecDeque::new(),
        })
    }

    /// Return a [NetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.process_event(event).await?,
                command = self.commands.recv() => match command {
                    Some(c) => self.process_command(c),
                    None => {
                        info!(target: "network", topics=?self.topics, "subscriber shutting down...");
                        return Ok(())
                    }
                }
            }
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.topics))]
    async fn process_event(
        &mut self,
        event: SwarmEvent<TNBehaviorEvent<TNCodec<Req, Res>>>,
    ) -> NetworkResult<()> {
        match event {
            SwarmEvent::Behaviour(behavior) => match behavior {
                TNBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                TNBehaviorEvent::ReqRes(event) => self.process_reqres_event(event)?,
                TNBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                concurrent_dial_errors,
                established_in,
            } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        send_or_log_error!(sender, Ok(()), "ConnectionEstablished", peer = peer_id);
                    }
                }
                if !self.connected_peers.contains(&peer_id) {
                    self.connected_peers.push_back(peer_id);
                }

                // Log successful connection establishment
                info!(
                    target: "network::events",
                    ?peer_id,
                    ?connection_id,
                    ?num_established,
                    ?established_in,
                    ?concurrent_dial_errors,
                    "new connection established"
                );

                // TODO: manage connnections?
                // - better to manage after `IncomingConnection` event?
                // if num_established > MAX_CONNECTIONS_PER_PEER {
                //     warn!(
                //         target: "network",
                //         ?peer_id,
                //         connections = num_established,
                //         "excessive connections from peer"
                //     );
                //     // close excess connections
                // }
            }
            SwarmEvent::ConnectionClosed {
                peer_id, connection_id, num_established, cause, ..
            } => {
                // Log connection closure with cause
                info!(
                    target: "network",
                    ?peer_id,
                    ?connection_id,
                    ?cause,
                    remaining = num_established,
                    "connection closed"
                );
                // TODO: moved to peer manager disconnected event
                //
                // self.connected_peers.retain(|peer| *peer != peer_id);

                // // handle complete peer disconnect
                // if num_established == 0 {
                //     tracing::debug!(target:"network::events", pending=?self.outbound_requests.len());
                //     // clean up any pending requests for this peer
                //     //
                //     // NOTE: self.outbound_requests are removed by `OutboundFailure`
                //     // but only if the Option<PeerId> is included. This is a
                //     // sanity check to prevent the HashMap from growing indefinitely when peers
                //     // disconnect after a request is made and the PeerId is lost.
                //     self.outbound_requests.retain(|_, sender| !sender.is_closed());

                //     // TODO: schedule reconnection attempt?
                //     if self.authorized_publishers.contains(&peer_id) {
                //         warn!(target: "network::events", ?peer_id, "authorized peer disconnected");
                //     }
                // }
            }
            SwarmEvent::OutgoingConnectionError { peer_id: Some(peer_id), error, .. } => {
                if let Some(sender) = self.pending_dials.remove(&peer_id) {
                    send_or_log_error!(sender, Err(error.into()), "OutgoingConnectionError");
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                // log listening addr
                info!(
                    target: "network",
                    address = ?address.with(Protocol::P2p(*self.swarm.local_peer_id())),
                    "network listening"
                );
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                // Log listener errors
                error!(
                    target: "network::events",
                    ?listener_id,
                    ?error,
                    "listener error"
                );
            }
            // These events are included here because they will likely become useful in near-future
            // PRs
            SwarmEvent::IncomingConnection { .. }
            | SwarmEvent::IncomingConnectionError { .. }
            | SwarmEvent::NewListenAddr { .. }
            | SwarmEvent::ListenerClosed { .. }
            | SwarmEvent::Dialing { .. }
            | SwarmEvent::NewExternalAddrCandidate { .. }
            | SwarmEvent::ExternalAddrConfirmed { .. }
            | SwarmEvent::ExternalAddrExpired { .. }
            | SwarmEvent::NewExternalAddrOfPeer { .. } => {}
            _ => {}
        }
        Ok(())
    }

    /// Process commands for the network.
    fn process_command(&mut self, command: NetworkCommand<Req, Res>) {
        match command {
            NetworkCommand::UpdateAuthorizedPublishers { authorities, reply } => {
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
            NetworkCommand::AddExplicitPeer { peer_id, addr } => {
                self.swarm.add_peer_address(peer_id, addr);
                self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
            NetworkCommand::Dial { peer_id, peer_addr, reply } => {
                if let hash_map::Entry::Vacant(entry) = self.pending_dials.entry(peer_id) {
                    // Add the peer we are dialing so we can easily reconnect after a timeout, etc.
                    // Can use "peer_addr.with(Protocol::P2p(peer_id))})" as the dial parameter
                    // without adding the peer but libp2p won't remember it.
                    self.swarm.add_peer_address(peer_id, peer_addr);
                    match self.swarm.dial(peer_id) {
                        Ok(()) => {
                            entry.insert(reply);
                        }
                        Err(e) => {
                            send_or_log_error!(
                                reply,
                                Err(e.into()),
                                "AddExplicitPeer",
                                peer = peer_id,
                            );
                        }
                    }
                } else {
                    // return error - dial attempt already tracked for peer
                    //
                    // may be necessary to update entry in future, but for now assume only one dial
                    // attempt
                    send_or_log_error!(reply, Err(NetworkError::RedialAttempt), "AddExplicitPeer");
                }
            }
            NetworkCommand::LocalPeerId { reply } => {
                let peer_id = *self.swarm.local_peer_id();
                send_or_log_error!(reply, peer_id, "LocalPeerId");
            }
            NetworkCommand::Publish { topic, msg, reply } => {
                let res = self.swarm.behaviour_mut().gossipsub.publish(topic, msg);
                send_or_log_error!(reply, res, "Publish");
            }
            NetworkCommand::Subscribe { topic, reply } => {
                let res = self.swarm.behaviour_mut().gossipsub.subscribe(&topic);
                send_or_log_error!(reply, res, "Subscribe");
            }
            NetworkCommand::ConnectedPeers { reply } => {
                let res = self.swarm.connected_peers().cloned().collect();
                send_or_log_error!(reply, res, "ConnectedPeers");
            }
            NetworkCommand::PeerScore { peer_id, reply } => {
                let opt_score = self.swarm.behaviour_mut().gossipsub.peer_score(&peer_id);
                send_or_log_error!(reply, opt_score, "PeerScore");
            }
            NetworkCommand::SetApplicationScore { peer_id, new_score, reply } => {
                let bool =
                    self.swarm.behaviour_mut().gossipsub.set_application_score(&peer_id, new_score);
                send_or_log_error!(reply, bool, "SetApplicationScore");
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
                let collection =
                    self.swarm.behaviour_mut().gossipsub.mesh_peers(&topic).cloned().collect();
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
        }
    }

    /// Process gossip events.
    fn process_gossip_event(&mut self, event: GossipEvent) -> NetworkResult<()> {
        match event {
            GossipEvent::Message { propagation_source, message_id, message } => {
                trace!(target: "network", topic=?self.topics, ?propagation_source, ?message_id, ?message, "message received from publisher");
                // verify message was published by authorized node
                let msg_acceptance = self.verify_gossip(&message);

                if msg_acceptance.is_accepted() {
                    // forward gossip to handler
                    if let Err(e) = self.event_stream.try_send(NetworkEvent::Gossip(message)) {
                        error!(target: "network", topics=?self.topics, ?propagation_source, ?message_id, ?e, "failed to forward gossip!");
                        // fatal - unable to process gossip messages
                        return Err(e.into());
                    }
                }
                trace!(target: "network", ?msg_acceptance, "gossip message verification status");

                // report message validation results
                if let Err(e) =
                    self.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                        &message_id,
                        &propagation_source,
                        msg_acceptance.into(),
                    )
                {
                    error!(target: "network", topics=?self.topics, ?propagation_source, ?message_id, ?e, "error reporting message validation result");
                }
            }
            GossipEvent::Subscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.topics, ?peer_id, ?topic, "gossipsub event - subscribed")
            }
            GossipEvent::Unsubscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.topics, ?peer_id, ?topic, "gossipsub event - unsubscribed")
            }
            GossipEvent::GossipsubNotSupported { peer_id } => {
                // TODO: remove peer at self point?
                trace!(target: "network", topics=?self.topics, ?peer_id, "gossipsub event - not supported")
            }
        }

        Ok(())
    }

    /// Process req/res events.
    fn process_reqres_event(&mut self, event: ReqResEvent<Req, Res>) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message } => {
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
                            error!(target: "network", topics=?self.topics, ?request_id, ?e, "failed to forward request!");
                            // fatal - unable to process requests
                            return Err(e.into());
                        }

                        self.inbound_requests.insert(request_id, notify);
                    }
                    request_response::Message::Response { request_id, response } => {
                        // try to forward response to original caller
                        let _ = self
                            .outbound_requests
                            .remove(&(peer, request_id))
                            .ok_or(NetworkError::PendingRequestChannelLost)?
                            .send(Ok(response));
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error } => {
                error!(target: "network", ?peer, ?error, "outbound failure");
                // try to forward error to original caller
                let _ = self
                    .outbound_requests
                    .remove(&(peer, request_id))
                    .ok_or(NetworkError::PendingRequestChannelLost)?
                    .send(Err(error.into()));
            }
            ReqResEvent::InboundFailure { peer, request_id, error } => {
                match error {
                    ReqResInboundFailure::Io(e) => {
                        // TODO: update peer score - could be malicious
                        warn!(target: "network", ?e, ?peer, ?request_id, "inbound IO failure");
                    }
                    ReqResInboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, ?error, "inbound failure: unsupported protocol");
                    }
                    _ => { /* ignore timeout, connection closed, and response ommission */ }
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

        // ensure publisher is authorized
        //
        // NOTE: expand on this based on gossip::topic - not all topics need to be permissioned
        if gossip.source.is_some_and(|id| self.authorized_publishers.contains(&id)) {
            GossipAcceptance::Accept
        } else {
            GossipAcceptance::Reject
        }
    }

    /// Process an event from the peer manager.
    fn process_peer_manager_event(&mut self, event: PeerEvent) -> NetworkResult<()> {
        // TODO: remove this!!!!!!
        println!("Peer Manager Event: {event:?}");

        match event {
            PeerEvent::PeerDisconnected(peer_id) => {
                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);

                // TODO: schedule reconnection attempt?
                if self.authorized_publishers.contains(&peer_id) {
                    warn!(target: "network::events", ?peer_id, "authorized peer disconnected");
                }

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
            _ => (),
            // PeerEvent::PeerConnectedIncoming(peer_id) => todo!(),
            // PeerEvent::PeerConnectedOutgoing(peer_id) => todo!(),
            // PeerEvent::DisconnectPeer(peer_id) => todo!(),
            // PeerEvent::Banned(peer_id, ip_addrs) => todo!(),
            // PeerEvent::Unbanned(peer_id, ip_addrs) => todo!(),
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
