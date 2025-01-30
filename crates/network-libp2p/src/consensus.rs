//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{TNCodec, TNMessage},
    error::NetworkError,
    send_or_log_error,
    types::{NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult},
    MAX_GOSSIP_SIZE,
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance,
    },
    multiaddr::Protocol,
    request_response::{
        self, Codec, Event as ReqResEvent, InboundFailure as ReqResInboundFailure,
        OutboundRequestId, ProtocolSupport,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::{
    collections::{hash_map, HashMap, HashSet},
    time::Duration,
};
use tn_config::ConsensusConfig;
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tracing::{error, info, instrument, trace, warn};

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior includes gossipsub, request-response, and identify.
/// TODO: possibly KAD?
#[derive(NetworkBehaviour)]
pub struct TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
}

impl<C> TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// Create a new instance of Self.
    pub fn new(gossipsub: gossipsub::Behaviour, req_res: request_response::Behaviour<C>) -> Self {
        Self { gossipsub, req_res }
    }
}

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
///
/// TODO: Primaries gossip signatures of final execution state at epoch boundaries and workers
/// gossip transactions? Publishers usually broadcast to several peers, so this may not be efficient
/// (multiple txs submitted).
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
    event_stream: Sender<NetworkEvent<Req, Res>>,
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
    /// The collection of pending requests.
    ///
    /// Callers include a oneshot channel for the network to return response. The caller is
    /// responsible for decoding message bytes and reporting peers who return bad data. Peers that
    /// send messages that fail to decode must receive an application score penalty.
    pending_requests: HashMap<OutboundRequestId, oneshot::Sender<NetworkResult<Res>>>,
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
        DB: tn_storage::traits::Database,
    {
        let topics = vec![IdentTopic::new("tn-primary")];
        Self::new(config, event_stream, topics)
    }

    /// Create a new instance of Self.
    pub fn new<DB>(
        config: &ConsensusConfig<DB>,
        event_stream: mpsc::Sender<NetworkEvent<Req, Res>>,
        topics: Vec<IdentTopic>,
    ) -> NetworkResult<Self>
    where
        // TODO: need to import tn-storage just for this trait?
        DB: tn_storage::traits::Database,
    {
        // TODO: pass keypair as arg so this function stays agnostic to primary/worker
        // - don't put helper method on key config bc that is TN-specific, and this is required by
        //   libp2p
        // - need to separate worker/primary network signatures
        let mut key_bytes = config.key_config().primary_network_keypair().as_ref().to_vec();
        let keypair = libp2p::identity::Keypair::ed25519_from_bytes(&mut key_bytes)?;

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // support peer exchange
            .do_px()
            // TN specific: filter against authorized_publishers
            .validate_messages()
            .build()?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(NetworkError::GossipBehavior)?;

        // TODO: use const as default and read from config
        let tn_codec = TNCodec::<Req, Res>::new(1024 * 1024); // 1mb

        // TODO: take this from configuration through CLI
        // - ex) "/telcoin-network/mainnet/0.0.1"
        let protocols = [(StreamProtocol::new("/telcoin-network/0.0.0"), ProtocolSupport::Full)];
        let req_res = request_response::Behaviour::with_codec(
            tn_codec,
            protocols,
            request_response::Config::default(),
        );

        // create custom behavior
        let behavior = TNBehavior::new(gossipsub, req_res);

        // create swarm
        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_behaviour(|_| behavior)
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let authorized_publishers = config.authorized_publishers();

        Ok(Self {
            swarm,
            topics,
            handle,
            commands,
            event_stream,
            authorized_publishers,
            pending_dials: Default::default(),
            pending_requests: Default::default(),
        })
    }

    /// Return a [NetworkHandle] to send commands to this network.
    ///
    /// TODO: this should just be `NetworkHandle`
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Run the network loop to process incoming gossip.
    pub fn run(mut self) -> JoinHandle<NetworkResult<()>> {
        tokio::spawn(async move {
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
        })
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
                peer_id,
                connection_id,
                endpoint,
                num_established,
                cause,
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

                // handle complete peer disconnect
                if num_established == 0 {
                    tracing::debug!(target:"network::events", pending=?self.pending_requests.len());
                    // clean up any pending requests for this peer
                    //
                    // NOTE: self.pending_requests are removed by `OutboundFailure`
                    // but only if the Option<PeerId> is included. This is mostly a
                    // sanity check to prevent the HashMap from growing indefinitely when peers
                    // disconnect after a request is made
                    self.pending_requests.retain(
                        |_, sender| {
                            if sender.is_closed() {
                                false
                            } else {
                                true
                            }
                        },
                    );

                    // TODO: schedule reconnection attempt?
                    if self.authorized_publishers.contains(&peer_id) {
                        warn!(target: "network::events", ?peer_id, "authorized peer disconnected");
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        send_or_log_error!(sender, Err(error.into()), "OutgoingConnectionError");
                    }
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
            // These events are included here because they will likely become useful in near-future PRs
            SwarmEvent::IncomingConnection { .. }
            | SwarmEvent::IncomingConnectionError { .. }
            | SwarmEvent::NewListenAddr { .. }
            | SwarmEvent::ListenerClosed { .. }
            | SwarmEvent::Dialing { .. }
            | SwarmEvent::NewExternalAddrCandidate { .. }
            | SwarmEvent::ExternalAddrConfirmed { .. }
            | SwarmEvent::ExternalAddrExpired { .. }
            | SwarmEvent::NewExternalAddrOfPeer { .. } => {}
            _e => {}
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
                    match self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id))) {
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
                self.pending_requests.insert(request_id, reply);
            }
            NetworkCommand::SendResponse { response, channel, reply } => {
                let res = self.swarm.behaviour_mut().req_res.send_response(channel, response);
                send_or_log_error!(reply, res, "SendResponse");
            }
            NetworkCommand::PendingRequestCount { reply } => {
                let count = self.pending_requests.len();
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
                    if let Err(e) = self.event_stream.try_send(NetworkEvent::Gossip(message.data)) {
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
                        // TODO: create hashmap for inbound requests to track max requests
                        // process if channel dropped for inbound failures

                        // forward request to handler without blocking other events
                        if let Err(e) = self.event_stream.try_send(NetworkEvent::Request {
                            peer,
                            request,
                            channel,
                        }) {
                            error!(target: "network", topics=?self.topics, ?request_id, ?e, "failed to forward request!");
                            // fatal - unable to process requests
                            return Err(e.into());
                        }
                    }
                    request_response::Message::Response { request_id, response } => {
                        // try to forward response to original caller
                        let _ = self
                            .pending_requests
                            .remove(&request_id)
                            .ok_or(NetworkError::PendingRequestChannelLost)?
                            .send(Ok(response));
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error } => {
                error!(target: "network", ?peer, ?error, "outbound failure");
                // try to forward error to original caller
                let _ = self
                    .pending_requests
                    .remove(&request_id)
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
            }
            ReqResEvent::ResponseSent { peer, request_id } => {}
        }

        Ok(())
    }

    /// Specific logic to accept gossip messages.
    ///
    /// Messages are only published by current committee nodes and must be within max size.
    fn verify_gossip(&self, gossip: &GossipMessage) -> GossipAcceptance {
        // verify message size
        if gossip.data.len() > MAX_GOSSIP_SIZE {
            return GossipAcceptance::Reject;
        }

        // ensure publisher is authorized
        if gossip.source.is_some_and(|id| self.authorized_publishers.contains(&id)) {
            GossipAcceptance::Accept
        } else {
            GossipAcceptance::Reject
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use serde::{Deserialize, Serialize};
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::{fixture_batch_with_transactions, CommitteeFixture};
    use tn_types::{BlockHash, Certificate, CertificateDigest, Header, SealedBatch, Vote};
    use tokio::time::timeout;

    // TODO: consolidate these to be common for all unit tests
    // impl TNMessage trait for types
    impl TNMessage for WorkerRequest {}
    impl TNMessage for WorkerResponse {}
    impl TNMessage for PrimaryRequest {}
    impl TNMessage for PrimaryResponse {}

    /// Requests between workers.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum WorkerRequest {
        /// Broadcast a newly produced worker block.
        ///
        /// NOTE: expect no response
        NewBatch(SealedBatch),
        /// The collection of missing [BlockHash]es for this peer.
        MissingBatches(Vec<BlockHash>),
    }

    /// Response to worker requests.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum WorkerResponse {
        /// Return the missing blocks requested by the peer.
        ///
        /// TODO: anemo included `size_limit_reached: bool` field
        /// but this should be trustless. See `RequestBlocksResponse` message.
        MissingBatches {
            /// The collection of requested blocks.
            batches: Vec<SealedBatch>,
        },
    }

    /// Requests from Primary.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum PrimaryRequest {
        NewCertificate { certificate: Certificate },
        Vote { header: Header, parents: Vec<Certificate> },
    }

    /// Response to primary requests.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum PrimaryResponse {
        Vote(Vote),
        MissingCertificates(Vec<Certificate>),
        MissingParents(Vec<CertificateDigest>),
    }

    /// A peer on TN
    struct NetworkPeer<DB, Req, Res>
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        /// Peer's node config.
        config: ConsensusConfig<DB>,
        /// Receiver for network events.
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        /// Network handle to send commands.
        network_handle: NetworkHandle<Req, Res>,
        /// The network task.
        network: ConsensusNetwork<Req, Res>,
    }

    /// The type for holding testng components.
    struct TestTypes<Req, Res, DB = MemDatabase>
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        /// The first authority in the committee.
        peer1: NetworkPeer<DB, Req, Res>,
        /// The second authority in the committee.
        peer2: NetworkPeer<DB, Req, Res>,
    }

    /// Helper function to create an instance of [RequestHandler] for the first authority in the
    /// committee.
    fn create_test_types<Req, Res>() -> TestTypes<Req, Res>
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
        let mut authorities = all_nodes.authorities();
        let authority_1 = authorities.next().expect("first authority");
        let authority_2 = authorities.next().expect("second authority");
        let config_1 = authority_1.consensus_config();
        let config_2 = authority_2.consensus_config();
        let (tx1, network_events_1) = mpsc::channel(1);
        let (tx2, network_events_2) = mpsc::channel(1);
        let topics = vec![IdentTopic::new("test-topic")];

        // peer1
        let peer1_network = ConsensusNetwork::<Req, Res>::new(&config_1, tx1, topics.clone())
            .expect("peer1 network created");
        let network_handle_1 = peer1_network.network_handle();
        let peer1 = NetworkPeer {
            config: config_1,
            network_events: network_events_1,
            network_handle: network_handle_1,
            network: peer1_network,
        };

        // peer2
        let peer2_network = ConsensusNetwork::<Req, Res>::new(&config_2, tx2, topics.clone())
            .expect("peer2 network created");
        let network_handle_2 = peer2_network.network_handle();
        let peer2 = NetworkPeer {
            config: config_2,
            network_events: network_events_2,
            network_handle: network_handle_2,
            network: peer2_network,
        };

        TestTypes { peer1, peer2 }
    }

    #[tokio::test]
    async fn test_valid_req_res() -> eyre::Result<()> {
        // start honest peer1 network
        let TestTypes { peer1, peer2 } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
        network.run();

        // start honest peer2 network
        let NetworkPeer {
            config: config_2,
            network_handle: peer2,
            network_events: mut network_events_2,
            network,
        } = peer2;
        network.run();

        // start swarm listening on default any address
        peer1.start_listening(config_1.authority().primary_network_address().inner()).await?;
        peer2.start_listening(config_2.authority().primary_network_address().inner()).await?;
        let peer2_id = peer2.local_peer_id().await?;
        let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

        let missing_block = fixture_batch_with_transactions(3).seal_slow();
        let digests = vec![missing_block.digest()];
        let batch_req = WorkerRequest::MissingBatches(digests);
        let batch_res = WorkerResponse::MissingBatches { batches: vec![missing_block] };

        // dial peer2
        peer1.dial(peer2_id, peer2_addr).await?;

        // send request and wait for response
        let max_time = Duration::from_secs(5);
        let response_from_peer = peer1.send_request(batch_req.clone(), peer2_id).await?;
        let event = timeout(max_time, network_events_2.recv())
            .await?
            .expect("first network event received");

        // expect network event
        if let NetworkEvent::Request { request, channel, .. } = event {
            assert_eq!(request, batch_req);

            // send response
            peer2.send_response(batch_res.clone(), channel).await?;
        } else {
            panic!("unexpected network event received");
        }

        // expect response
        let response = timeout(max_time, response_from_peer).await?.expect("outbound id recv")?;
        assert_eq!(response, batch_res);

        Ok(())
    }

    #[tokio::test]
    async fn test_valid_req_res_connection_closed_cleanup() -> eyre::Result<()> {
        // start honest peer1 network
        let TestTypes { peer1, peer2 } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
        network.run();

        // start honest peer2 network
        let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
        let peer2_network_task = network.run();

        // start swarm listening on default any address
        peer1.start_listening(config_1.authority().primary_network_address().inner()).await?;
        peer2.start_listening(config_2.authority().primary_network_address().inner()).await?;
        let peer2_id = peer2.local_peer_id().await?;
        let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

        let missing_block = fixture_batch_with_transactions(3).seal_slow();
        let digests = vec![missing_block.digest()];
        let batch_req = WorkerRequest::MissingBatches(digests);

        // dial peer2
        peer1.dial(peer2_id, peer2_addr).await?;

        // expect no pending requests yet
        let count = peer1.get_pending_request_count().await?;
        assert_eq!(count, 0);

        // send request and wait for response
        let _ = peer1.send_request(batch_req.clone(), peer2_id).await?;

        // peer1 has a pending_request now
        let count = peer1.get_pending_request_count().await?;
        assert_eq!(count, 1);

        // another sanity check
        let connected_peers = peer1.connected_peers().await?;
        assert_eq!(connected_peers.len(), 1);

        // simulate crashed peer 2
        peer2_network_task.abort();
        assert!(peer2_network_task.await.unwrap_err().is_cancelled());

        // allow peer1 to process disconnect
        tokio::time::sleep(Duration::from_millis(500)).await;

        // assert peer is disconnected
        let connected_peers = peer1.connected_peers().await?;
        assert_eq!(connected_peers.len(), 0);

        // peer1 removes pending requests
        let count = peer1.get_pending_request_count().await?;
        assert_eq!(count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_valid_req_res_inbound_failure() -> eyre::Result<()> {
        tn_test_utils::init_test_tracing();
        // start honest peer1 network
        let TestTypes { peer1, peer2 } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;

        let peer1_network_task = network.run();

        // start honest peer2 network
        let NetworkPeer {
            config: config_2,
            network_handle: peer2,
            network_events: mut network_events_2,
            network,
        } = peer2;
        network.run();

        // start swarm listening on default any address
        peer1.start_listening(config_1.authority().primary_network_address().inner()).await?;
        peer2.start_listening(config_2.authority().primary_network_address().inner()).await?;
        let peer2_id = peer2.local_peer_id().await?;
        let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

        let missing_block = fixture_batch_with_transactions(3).seal_slow();
        let digests = vec![missing_block.digest()];
        let batch_req = WorkerRequest::MissingBatches(digests);

        // dial peer2
        peer1.dial(peer2_id, peer2_addr).await?;

        // expect no pending requests yet
        let count = peer1.get_pending_request_count().await?;
        assert_eq!(count, 0);

        // send request and wait for response
        let max_time = Duration::from_secs(5);
        let response_from_peer = peer1.send_request(batch_req.clone(), peer2_id).await?;

        // peer1 has a pending_request now
        let count = peer1.get_pending_request_count().await?;
        assert_eq!(count, 1);

        // another sanity check
        let connected_peers = peer1.connected_peers().await?;
        assert_eq!(connected_peers.len(), 1);

        // wait for peer2 to receive req
        let event = timeout(max_time, network_events_2.recv())
            .await?
            .expect("first network event received");

        // expect network event
        if let NetworkEvent::Request { request, .. } = event {
            assert_eq!(request, batch_req);

            // peer 1 crashes after making request
            peer1_network_task.abort();
            assert!(peer1_network_task.await.unwrap_err().is_cancelled());

            tokio::task::yield_now().await;
        } else {
            panic!("unexpected network event received");
        }

        let res = timeout(Duration::from_secs(2), response_from_peer)
            .await?
            .expect("first network event received");

        println!("res: {res:?}");

        // InboundFailure::Io(Kind(UnexpectedEof))
        assert_matches!(res, Err(NetworkError::Outbound(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_outbound_failure_malicious_request() -> eyre::Result<()> {
        // start malicious peer1 network
        //
        // although these are valid req/res types, they are incorrect for the honest peer's
        // "worker" network
        let TestTypes { peer1, .. } = create_test_types::<PrimaryRequest, PrimaryResponse>();
        let NetworkPeer { config: config_1, network_handle: malicious_peer, network, .. } = peer1;
        network.run();

        // start honest peer2 network
        let TestTypes { peer2, .. } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_2, network_handle: honest_peer, network, .. } = peer2;
        network.run();

        // start swarm listening on default any address
        malicious_peer
            .start_listening(config_1.authority().primary_network_address().inner())
            .await?;
        honest_peer.start_listening(config_2.authority().primary_network_address().inner()).await?;

        let honest_peer_id = honest_peer.local_peer_id().await?;
        let honest_peer_addr =
            honest_peer.listeners().await?.first().expect("honest_peer listen addr").clone();

        // this type already impl `TNMessage` but this could be incorrect message type
        let malicious_msg = PrimaryRequest::Vote {
            header: Header::default(),
            parents: vec![Certificate::default()],
        };

        // dial honest peer
        malicious_peer.dial(honest_peer_id, honest_peer_addr).await?;

        // honest peer returns `OutboundFailure` error
        //
        // TODO: this should affect malicious peer's local score
        // - how can honest peer limit malicious requests?
        let response_from_peer = malicious_peer.send_request(malicious_msg, honest_peer_id).await?;
        let res = timeout(Duration::from_secs(2), response_from_peer)
            .await?
            .expect("first network event received");

        // OutboundFailure::Io(Kind(UnexpectedEof))
        assert_matches!(res, Err(NetworkError::Outbound(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_outbound_failure_malicious_response() -> eyre::Result<()> {
        // honest peer 1
        let TestTypes { peer1, .. } = create_test_types::<PrimaryRequest, PrimaryResponse>();
        let NetworkPeer { config: config_1, network_handle: honest_peer, network, .. } = peer1;
        network.run();

        // malicious peer2
        //
        // although these are honest req/res types, they are incorrect for the honest peer's
        // "primary" network this allows the network to receive "correct" messages and
        // respond with bad messages
        let TestTypes { peer2, .. } = create_test_types::<PrimaryRequest, WorkerResponse>();
        let NetworkPeer {
            config: config_2,
            network_handle: malicious_peer,
            network,
            network_events: mut network_events_2,
        } = peer2;
        network.run();

        // start swarm listening on default any address
        honest_peer.start_listening(config_1.authority().primary_network_address().inner()).await?;
        malicious_peer
            .start_listening(config_2.authority().primary_network_address().inner())
            .await?;
        let malicious_peer_id = malicious_peer.local_peer_id().await?;
        let malicious_peer_addr =
            malicious_peer.listeners().await?.first().expect("malicious_peer listen addr").clone();

        // dial malicious_peer
        honest_peer.dial(malicious_peer_id, malicious_peer_addr).await?;

        // send request and wait for malicious response
        let max_time = Duration::from_secs(2);
        let honest_req = PrimaryRequest::Vote {
            header: Header::default(),
            parents: vec![Certificate::default()],
        };
        let response_from_peer =
            honest_peer.send_request(honest_req.clone(), malicious_peer_id).await?;
        let event = timeout(max_time, network_events_2.recv())
            .await?
            .expect("first network event received");

        // expect network event
        if let NetworkEvent::Request { request, channel, .. } = event {
            assert_eq!(request, honest_req);
            // send response
            let block = fixture_batch_with_transactions(1).seal_slow();
            let malicious_reply = WorkerResponse::MissingBatches { batches: vec![block] };
            malicious_peer.send_response(malicious_reply, channel).await?;
        } else {
            panic!("unexpected network event received");
        }

        // expect response
        let res =
            timeout(max_time, response_from_peer).await?.expect("response received within time");

        // OutboundFailure::Io(Custom { kind: Other, error: Custom("Invalid value was given to the
        // function") })
        assert_matches!(res, Err(NetworkError::Outbound(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_publish_to_one_peer() -> eyre::Result<()> {
        // start honest cvv network
        let TestTypes { peer1, peer2 } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
        network.run();

        // start honest nvv network
        let NetworkPeer {
            config: config_2,
            network_handle: nvv,
            network_events: mut nvv_network_events,
            network,
        } = peer2;
        network.run();

        // start swarm listening on default any address
        cvv.start_listening(config_1.authority().primary_network_address().inner()).await?;
        nvv.start_listening(config_2.authority().primary_network_address().inner()).await?;
        let cvv_id = cvv.local_peer_id().await?;
        let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

        // topics for pubsub
        let test_topic = IdentTopic::new("test-topic");

        // subscribe
        nvv.subscribe(test_topic.clone()).await?;

        // dial cvv
        nvv.dial(cvv_id, cvv_addr).await?;

        // publish random block
        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);

        // sleep for gossip connection time lapse
        tokio::time::sleep(Duration::from_millis(500)).await;

        // publish on wrong topic - no peers
        let expected_failure =
            cvv.publish(IdentTopic::new("WRONG_TOPIC"), expected_result.clone()).await;
        assert!(expected_failure.is_err());

        // publish correct message and wait to receive
        let _message_id = cvv.publish(test_topic, expected_result.clone()).await?;
        let event = timeout(Duration::from_secs(2), nvv_network_events.recv())
            .await?
            .expect("batch received");

        // assert gossip message
        if let NetworkEvent::Gossip(msg) = event {
            assert_eq!(msg, expected_result);
        } else {
            panic!("unexpected network event received");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_msg_verification_ignores_unauthorized_publisher() -> eyre::Result<()> {
        // start honest cvv network
        let TestTypes { peer1, peer2 } = create_test_types::<WorkerRequest, WorkerResponse>();
        let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
        network.run();

        // start honest nvv network
        let NetworkPeer {
            config: config_2,
            network_handle: nvv,
            network_events: mut nvv_network_events,
            network,
        } = peer2;
        network.run();

        // start swarm listening on default any address
        cvv.start_listening(config_1.authority().primary_network_address().inner()).await?;
        nvv.start_listening(config_2.authority().primary_network_address().inner()).await?;
        let cvv_id = cvv.local_peer_id().await?;
        let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

        // topics for pubsub
        let test_topic = IdentTopic::new("test-topic");

        // subscribe
        nvv.subscribe(test_topic.clone()).await?;

        // dial cvv
        nvv.dial(cvv_id, cvv_addr).await?;

        // publish random block
        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);

        // sleep for gossip connection time lapse
        tokio::time::sleep(Duration::from_millis(500)).await;

        // publish correct message and wait to receive
        let _message_id = cvv.publish(test_topic.clone(), expected_result.clone()).await?;
        let event = timeout(Duration::from_secs(2), nvv_network_events.recv())
            .await?
            .expect("batch received");

        // assert gossip message
        if let NetworkEvent::Gossip(msg) = event {
            assert_eq!(msg, expected_result);
        } else {
            panic!("unexpected network event received");
        }

        // remove cvv from whitelist and try to publish again
        nvv.update_authorized_publishers(HashSet::with_capacity(0)).await?;

        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);
        let _message_id = cvv.publish(test_topic, expected_result.clone()).await?;

        // message should never be forwarded
        let timeout = timeout(Duration::from_secs(2), nvv_network_events.recv()).await;
        assert!(timeout.is_err());

        // TODO: assert peer score after bad message

        Ok(())
    }

    #[test]
    fn test_peer_id_to_from_fastcrypto() {
        let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
        let mut authorities = all_nodes.authorities();
        let authority = authorities.next().expect("first authority");
        let config = authority.consensus_config();

        // converts fastcrypto -> libp2p or panics
        let fastcrypto_to_libp2p = config.authorized_publishers();
        // assert libp2p -> fastcrypto works
        for key in fastcrypto_to_libp2p.iter() {
            let fc_key =
                config.ed25519_libp2p_to_fastcrypto(key).expect("libp2p to fastcrypto ed25519 key");
            let libp2p_key_again = config
                .ed25519_fastcrypto_to_libp2p(&fc_key)
                .expect("fastcrypto to libp2p ed25519 key");
            // sanity check - cast back to original type
            assert_eq!(fc_key.as_ref(), &libp2p_key_again.as_ref().digest()[4..]);
        }
    }

    // test peer floods requests?
    //
}
