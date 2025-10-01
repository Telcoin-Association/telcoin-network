//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::{sync::Arc, time::Duration};

use crate::{proposer::OurDigestMessage, state_sync::StateSynchronizer, ConsensusBus};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    types::{IntoResponse as _, NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, PeerExchangeMap, Penalty, ResponseChannel,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::PayloadStore;
use tn_types::{
    encode, BlockHash, BlsPublicKey, Certificate, CertificateDigest, ConsensusHeader, Database,
    Epoch, EpochCertificate, EpochRecord, EpochVote, Header, TaskSpawner, TnReceiver, TnSender,
    Vote,
};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;
pub mod handler;
mod message;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
pub(crate) type Req = PrimaryRequest;
/// Convenience type for Primary network.
pub(crate) type Res = PrimaryResponse;

/// Primary network specific handle.
#[derive(Clone, Debug)]
pub struct PrimaryNetworkHandle {
    handle: NetworkHandle<Req, Res>,
}

impl From<NetworkHandle<Req, Res>> for PrimaryNetworkHandle {
    fn from(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle }
    }
}

impl PrimaryNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle }
    }

    //// Convenience method for creating a new Self for tests.
    pub fn new_for_test(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self { handle: NetworkHandle::new(sender) }
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<PrimaryRequest, PrimaryResponse> {
        &self.handle
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(Box::new(certificate)));
        self.handle.publish(tn_config::LibP2pConfig::primary_topic(), data).await?;
        Ok(())
    }

    /// Publish a consensus block number and hash of the header.
    pub async fn publish_consensus(
        &self,
        consensus_block_num: u64,
        consensus_header_hash: BlockHash,
    ) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Consensus(consensus_block_num, consensus_header_hash));
        self.handle.publish(tn_config::LibP2pConfig::primary_topic(), data).await?;
        Ok(())
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_epoch_vote(&self, vote: EpochVote) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
        self.handle.publish(tn_config::LibP2pConfig::epoch_vote_topic(), data).await?;
        Ok(())
    }

    /// Request a vote for header from the peer.
    /// Can return a response of Vote or MissingParents, other responses will be an error.
    pub async fn request_vote(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
    ) -> NetworkResult<RequestVoteResult> {
        let header = Arc::new(header);
        let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
        let res = self.handle.send_request(request, peer).await?;
        let mut res = res.await??;
        let mut tries = 0;
        while let PrimaryResponse::RecoverableError(PrimaryRPCError(s)) = res {
            warn!(target: "primary::network", "Got recoverable error {s}, retrying");
            tokio::time::sleep(Duration::from_millis(250)).await;
            let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
            let res_raw = self.handle.send_request(request, peer).await?;
            res = res_raw.await??;
            tries += 1;
            if tries > 5 {
                break;
            }
        }
        match res {
            PrimaryResponse::Vote(vote) => Ok(RequestVoteResult::Vote(vote)),
            PrimaryResponse::RecoverableError(PrimaryRPCError(s))
            | PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            PrimaryResponse::RequestedCertificates(_vec) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is requested certificates!".to_string(),
            )),
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::ConsensusHeader(_consensus_header) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is consensus header!".to_string(),
            )),
            PrimaryResponse::EpochRecord { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is epoch record!".to_string(),
            )),
            PrimaryResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is peer exchange!".to_string(),
            )),
            PrimaryResponse::NodeRecord(_) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is node record!".to_string(),
            )),
        }
    }

    pub async fn fetch_certificates(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        let request = PrimaryRequest::MissingCertificates { inner: request };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::RequestedCertificates(certs) => Ok(certs),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError("Got wrong response, not a certificate!".to_string())),
        }
    }

    /// Request consensus header from specific peer.
    pub async fn request_consensus_from_peer(
        &self,
        peer: BlsPublicKey,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::ConsensusHeader(header) => Ok(Arc::unwrap_or_clone(header)),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError(
                "Got wrong response, not a consensus header!".to_string(),
            )),
        }
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    pub async fn request_consensus(
        &self,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            let res = res.await?;
            if let Ok(PrimaryResponse::ConsensusHeader(header)) = res {
                return Ok(Arc::unwrap_or_clone(header));
            }
        }
        Err(NetworkError::RPCError("Could not get the consensus header!".to_string()))
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    pub async fn request_epoch_cert(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<(EpochRecord, EpochCertificate)> {
        let request = PrimaryRequest::EpochRecord { epoch, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            let res = res.await?;
            if let Ok(PrimaryResponse::EpochRecord { record, certificate }) = res {
                return Ok((record, certificate));
            }
        }
        Err(NetworkError::RPCError("Could not get the epoch record!".to_string()))
    }

    /// Report a penalty to the network's peer manager.
    async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Notify peer manager of peer exchange information.
    pub(crate) async fn process_peer_exchange(
        &self,
        peers: PeerExchangeMap,
        channel: ResponseChannel<PrimaryResponse>,
    ) {
        if let Err(e) = self.handle.process_peer_exchange(peers, channel).await {
            warn!(target: "primary::network", ?e, "process peer exchange failed");
        }
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }
}

/// Handle inter-node communication between primaries.
pub struct PrimaryNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: PrimaryNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
}

impl<DB, Events> PrimaryNetwork<DB, Events>
where
    DB: Database,
    Events: TnReceiver<NetworkEvent<Req, Res>> + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: Events,
        network_handle: PrimaryNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        task_spawner: TaskSpawner,
    ) -> Self {
        let request_handler =
            RequestHandler::new(consensus_config, consensus_bus, state_sync.clone());
        Self { network_events, network_handle, request_handler, task_spawner }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("primary network events", async move {
            while let Some(event) = self.network_events.recv().await {
                self.process_network_event(event)
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&mut self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                PrimaryRequest::Vote { header, parents } => {
                    self.process_vote_request(
                        peer,
                        Arc::unwrap_or_clone(header),
                        parents,
                        channel,
                        cancel,
                    );
                }
                PrimaryRequest::MissingCertificates { inner } => {
                    self.process_request_for_missing_certs(peer, inner, channel, cancel)
                }
                PrimaryRequest::ConsensusHeader { number, hash } => {
                    self.process_consensus_output_request(peer, number, hash, channel, cancel)
                }
                PrimaryRequest::PeerExchange { peers } => {
                    self.process_peer_exchange(peers, channel)
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
                PrimaryRequest::NodeRecord => {
                    // this should be intercepted by network layer
                    warn!(target: "primary::network", "request for primary's node record received in application layer");
                }
            },
            NetworkEvent::Gossip(msg, propagation_source) => {
                self.process_gossip(msg, propagation_source);
            }
            NetworkEvent::Error(msg, channel) => {
                let err = PrimaryResponse::Error(PrimaryRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.task_spawner.spawn_task("report request error", async move {
                    let _ = network_handle.handle.send_response(err, channel).await;
                });
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("VoteRequest-{}", header.digest());

        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                vote = request_handler.vote(peer, header, parents) => {
                    let response = vote.into_response();
                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve certificates for a peer that's missing them.
    fn process_request_for_missing_certs(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("MissingCertsReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                result = request_handler.retrieve_missing_certs(request) => {
                    // report penalty if any
                    if let Err(ref e) = result {
                        if let Some(penalty) = e.into() {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                    }

                    let response = result.into_response();
                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_consensus_output_request(
        &self,
        peer: BlsPublicKey,
        number: Option<u64>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ConsensusOutputReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                header =
                    request_handler.retrieve_consensus_header(number, hash) => {
                        let response = header.into_response();
                        // TODO: penalize peer's reputation for bad request
                        // if response.is_err() { }
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_epoch_record_request(
        &self,
        peer: BlsPublicKey,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ConsensusOutputReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                header =
                    request_handler.retrieve_epoch_record(epoch, hash) => {
                        let response = header.into_response();
                        // TODO: penalize peer's reputation for bad request
                        // if response.is_err() { }
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from committee.
    fn process_gossip(&self, msg: GossipMessage, propagation_source: BlsPublicKey) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ProcessGossip-{propagation_source}");
        // spawn task to process gossip
        self.task_spawner.spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "primary::network", ?e, "process_gossip");
                // convert error into penalty to lower peer score
                if let Some(penalty) = (&e).into() {
                    network_handle.report_penalty(propagation_source, penalty).await;
                }
            }
        });
    }

    /// Process peer exchange.
    fn process_peer_exchange(
        &self,
        peers: PeerExchangeMap,
        channel: ResponseChannel<PrimaryResponse>,
    ) {
        let network_handle = self.network_handle.clone();
        // notify peer manager and respond with ack
        self.task_spawner.spawn_task("ProcessPeerExchange", async move {
            network_handle.process_peer_exchange(peers, channel).await;
        });
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    consensus_bus: ConsensusBus,
    payload_store: DB,
}

impl<DB: PayloadStore> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self.
    pub fn new(consensus_bus: ConsensusBus, payload_store: DB) -> Self {
        Self { consensus_bus, payload_store }
    }
}

#[async_trait::async_trait]
impl<DB: Database> WorkerToPrimaryClient for WorkerReceiverHandler<DB> {
    async fn report_own_batch(&self, message: WorkerOwnBatchMessage) -> eyre::Result<()> {
        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                timestamp: message.timestamp,
                ack_channel: tx_ack,
            })
            .await?;

        // If we are ok, then wait for the ack
        rx_ack.await?;

        Ok(response)
    }

    async fn report_others_batch(&self, message: WorkerOthersBatchMessage) -> eyre::Result<()> {
        self.payload_store.write_payload(&message.digest, &message.worker_id)?;
        Ok(())
    }
}

/// Responses to a vote request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestVoteResult {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<CertificateDigest>),
}
