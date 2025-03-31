//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::sync::Arc;

use crate::{proposer::OurDigestMessage, state_sync::StateSynchronizer, ConsensusBus};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    types::{
        IdentTopic, IntoResponse as _, NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult,
    },
    GossipMessage, Multiaddr, PeerId, ResponseChannel,
};
use tn_network_types::{
    FetchCertificatesRequest, WorkerOthersBatchMessage, WorkerOwnBatchMessage,
    WorkerToPrimaryClient,
};
use tn_storage::PayloadStore;
use tn_types::{
    encode, BlockHash, Certificate, CertificateDigest, ConsensusHeader, Database, Header, Noticer,
    TaskManager, TnSender, Vote,
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
#[derive(Clone)]
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

    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        self.handle.dial(peer_id, peer_addr).await
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(Box::new(certificate)));
        self.handle.publish(IdentTopic::new("tn-primary"), data).await?;
        Ok(())
    }

    /// Publish a onsensus block number and hash of the header.
    pub async fn publish_consensus(
        &self,
        consensus_block_num: u64,
        consensus_header_hash: BlockHash,
    ) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Consenus(consensus_block_num, consensus_header_hash));
        self.handle.publish(IdentTopic::new("tn-primary"), data).await?;
        Ok(())
    }

    /// Request a vote for header from the peer.
    /// Can return a response of Vote or MissingParents, other responses will be an error.
    pub async fn request_vote(
        &self,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
    ) -> NetworkResult<RequestVoteResult> {
        let request = PrimaryRequest::Vote { header: Arc::new(header), parents };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::Vote(vote) => Ok(RequestVoteResult::Vote(vote)),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            PrimaryResponse::RequestedCertificates(_vec) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is requested certificates!".to_string(),
            )),
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::ConsensusHeader(_consensus_header) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is consensus header!".to_string(),
            )),
        }
    }

    pub async fn fetch_certificates(
        &self,
        peer: PeerId,
        request: FetchCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        let FetchCertificatesRequest { exclusive_lower_bound, skip_rounds, max_items } = request;
        let request = PrimaryRequest::MissingCertificates {
            inner: MissingCertificatesRequest { exclusive_lower_bound, skip_rounds, max_items },
        };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::RequestedCertificates(certs) => Ok(certs),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError("Got wrong response, not a certificate!".to_string())),
        }
    }

    pub async fn request_consensus_from_peer(
        &self,
        peer: PeerId,
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
}

/// Handle inter-node communication between primaries.
pub struct PrimaryNetwork<DB> {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: PrimaryNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// Shutdown notification.
    shutdown_rx: Noticer,
}

impl<DB> PrimaryNetwork<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: PrimaryNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
    ) -> Self {
        let shutdown_rx = consensus_config.shutdown().subscribe();
        let request_handler =
            RequestHandler::new(consensus_config, consensus_bus, state_sync.clone());
        Self { network_events, network_handle, request_handler, shutdown_rx }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network.
    pub fn spawn(mut self, task_manager: &TaskManager) {
        task_manager.spawn_task("primary network events", async move {
            loop {
                tokio::select!(
                    _ = &self.shutdown_rx => break,
                    event = self.network_events.recv() => {
                        match event {
                            Some(e) => self.process_network_event(e),
                            None => break,
                        }
                    }
                )
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
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
            },
            NetworkEvent::Gossip(msg) => {
                self.process_gossip(msg);
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
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
        _peer: PeerId,
        request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                certs = request_handler.retrieve_missing_certs(request) => {
                    let response = certs.into_response();

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
    fn process_consensus_output_request(
        &self,
        _peer: PeerId,
        number: Option<u64>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
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

    /// Process gossip from committee.
    fn process_gossip(&self, msg: GossipMessage) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        // let network_handle = self.network_handle.clone();

        // commented out to prevent CertificateError::TooNew from forcing disconnect when peers
        // are trying to resync
        tokio::spawn(async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "primary::network", ?e, "process_gossip");
                // TODO: peers don't track reputation yet
                //
                // NOTE: the network ensures the peer id is present before forwarding the msg
                // if let Some(peer_id) = msg.source {
                //     if let Err(e) =
                //         network_handle.handle.set_application_score(peer_id, -100.0).await
                //     {
                //         error!(target: "primary::network", ?e, "failed to penalize malicious peer")
                //     }
                // }

                // match on error to lower peer score
                //todo!();
            }
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
