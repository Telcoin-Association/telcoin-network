//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    error::PrimaryNetworkResult, proposer::OurDigestMessage, state_sync::StateSynchronizer,
    ConsensusBus, ConsensusBusApp,
};
use futures::{AsyncReadExt as _, AsyncWriteExt as _};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use parking_lot::Mutex;
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    types::{IntoResponse as _, NetworkCommand, NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, Penalty, ResponseChannel, Stream,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::{ConsensusChain, ConsensusChainError},
    consensus_pack::PackError,
    PayloadStore,
};
use tn_types::{
    encode, BlockHash, BlsPublicKey, BlsSignature, Certificate, CertificateDigest, ConsensusHeader,
    Database, Epoch, EpochCertificate, EpochRecord, EpochVote, Header, Round, TaskSpawner,
    TnReceiver, TnSender, Vote, B256,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{debug, info, warn};
pub mod handler;
mod message;
pub use message::ConsensusResult;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
pub(crate) type Req = PrimaryRequest;
/// Convenience type for Primary network.
pub(crate) type Res = PrimaryResponse;

/// Interval for pruning pending epoch pack requests (awaiting peer to open stream).
const PENDING_REQUEST_PRUNE_INTERVAL: Duration = Duration::from_secs(15);

/// Timeout for pending epoch pack requests before cleanup.
pub const PENDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of concurrent epoch stream operations (pending + active).
///
/// A semaphore permit is held from RPC acceptance through stream completion,
/// so this bounds the true concurrent count—not just the pending map size.
pub const MAX_CONCURRENT_EPOCH_STREAMS: usize = 5;

/// Maximum number of concurrent pending batch requests from a single peer.
///
/// Prevents a single malicious peer from filling all global slots.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Tracks a pending epoch pack stream request awaiting stream establishment.
// pub for IT
#[derive(Debug)]
pub struct PendingEpochStream {
    /// The epoch which produced these batches.
    epoch: Epoch,
    /// When this request was created (for timeout cleanup).
    created_at: Instant,
    /// Semaphore permit held for the lifetime of this request (pending + active).
    /// Dropping the permit frees a global concurrency slot.
    _permit: OwnedSemaphorePermit,
}

/// Key for pending requests: (peer_bls, request_digest)
type PendingEpochRequestKey = (BlsPublicKey, B256);

impl PendingEpochStream {
    /// Create a new pending batch stream.
    pub fn new(epoch: Epoch, permit: OwnedSemaphorePermit) -> Self {
        Self { epoch, created_at: Instant::now(), _permit: permit }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl PendingEpochStream {
    /// Create a pending batch stream with a custom `created_at` for testing stale cleanup.
    pub fn new_with_created_at(
        epoch: Epoch,
        permit: OwnedSemaphorePermit,
        created_at: Instant,
    ) -> Self {
        Self { epoch, created_at, _permit: permit }
    }
}

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
        epoch: Epoch,
        round: Round,
        consensus_block_num: u64,
        consensus_header_hash: BlockHash,
        key: BlsPublicKey,
        signature: BlsSignature,
    ) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Consensus(Box::new(ConsensusResult {
            epoch,
            round,
            number: consensus_block_num,
            hash: consensus_header_hash,
            validator: key,
            signature,
        })));
        self.handle.publish(tn_config::LibP2pConfig::consensus_output_topic(), data).await?;
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
            PrimaryResponse::RequestEpochStream { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is epoch stream!".to_string(),
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
    /// Will verify the returned header matches hash if provided (strong) or number if not (weak).
    pub async fn request_consensus_from_peer(
        &self,
        peer: BlsPublicKey,
        number: u64,
        hash: BlockHash,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::ConsensusHeader(header) => {
                if header.digest() == hash && header.number == number {
                    Ok(Arc::unwrap_or_clone(header))
                } else {
                    Err(NetworkError::RPCError(format!(
                        "Returned header does not match number {number}/{} or hash {hash}/{}!",
                        header.number,
                        header.digest()
                    )))
                }
            }
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError(
                "Got wrong response, not a consensus header!".to_string(),
            )),
        }
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    /// Will verify the returned header matches hash if provided (strong) or number if not (weak).
    pub async fn request_consensus(
        &self,
        number: u64,
        hash: BlockHash,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            let res = res.await?;
            if let Ok(PrimaryResponse::ConsensusHeader(header)) = res {
                if header.digest() == hash && header.number == number {
                    return Ok(Arc::unwrap_or_clone(header));
                } else {
                    return Err(NetworkError::RPCError(format!(
                        "Returned header does not match number {number}/{} or hash {hash}/{}!",
                        header.number,
                        header.digest()
                    )));
                }
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
            if let Ok(Ok(PrimaryResponse::EpochRecord { record, certificate })) = res.await {
                return Ok((record, certificate));
            }
        }
        Err(NetworkError::RPCError("Could not get the epoch record!".to_string()))
    }

    /// Report a penalty to the network's peer manager.
    async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }

    /// Attempt to get an epoch pack file from any peer via stream.
    ///
    /// This method:
    /// 1. Sends a `RequestEpochStream` request to negotiate
    /// 2. If accepted, opens a stream with the request digest for correlation
    /// 3. Reads and validates batches from the stream in real-time
    pub async fn request_epoch_pack(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        let epoch = epoch_record.epoch;
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        // send request to negotiate stream
        let request = PrimaryRequest::StreamEpoch { epoch };
        let mut hasher = tn_types::DefaultHashFunction::new();
        hasher.update(&epoch.to_le_bytes());
        let request_digest = B256::from_slice(hasher.finalize().as_bytes());

        for _ in 0..3 {
            // send request and await response from peer
            //
            // SAFETY: network layer handles request timeout
            let dispatch = match self.handle.send_request_any(request.clone()).await {
                Ok(rx) => rx,
                Err(e) => {
                    warn!(target: "primary::network", ?e, "send_request_any failed; retrying");
                    continue;
                }
            };
            let resp = match dispatch.await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    warn!(target: "primary::network", ?e, "peer responded with error; retrying");
                    continue;
                }
                Err(e) => {
                    warn!(target: "primary::network", ?e, "peer dropped response channel; retrying");
                    continue;
                }
            };
            if let PrimaryResponse::RequestEpochStream { ack, peer } = resp {
                // continue if denied to try next peer
                if !ack {
                    continue;
                }

                debug!(
                    target: "primary::network",
                    %peer,
                    ?ack,
                    "peer ack for stream request"
                );

                // open raw stream then write request_digest for correlation
                let mut stream = self.handle.open_stream(peer).await??;
                stream.write_all(request_digest.as_slice()).await.map_err(|e| {
                    NetworkError::RPCError(format!("failed to write request digest: {e}"))
                })?;
                stream.flush().await.map_err(|e| {
                    NetworkError::RPCError(format!("failed to flush request digest: {e}"))
                })?;

                info!(
                    target: "primary::network",
                    %peer,
                    epoch,
                    "stream opened - reading and validating epoch pack file..."
                );

                if let Err(err) = consensus_chain
                    .stream_import(stream.compat(), epoch_record, previous_epoch, record_timeout)
                    .await
                {
                    if let Some(penalty) = Self::consensus_chain_error_to_penalty(&err) {
                        self.report_penalty(peer, penalty).await;
                    }
                    warn!(
                        target: "primary::network",
                        %peer,
                        epoch,
                        "FAILED to streamed epoch pack file from peer"
                    );
                    continue;
                }
                info!(
                    target: "primary::network",
                    %peer,
                    epoch,
                    "streamed epoch pack file"
                );

                return Ok(());
            }
        }
        Err(NetworkError::RPCError("Could not get the epoch pack file!".to_string()))
    }

    /// Helper to convert a consensus chain error to penalty.
    /// This error does not have network knowledge so do it here for the
    /// streaming case vs with the error.
    fn consensus_chain_error_to_penalty(error: &ConsensusChainError) -> Option<Penalty> {
        match error {
            ConsensusChainError::PackError(pack_error) => match pack_error {
                PackError::MissingBatch
                | PackError::NotConsensus
                | PackError::NotBatch
                | PackError::NotEpoch => Some(Penalty::Medium),
                PackError::InvalidConsensusChain
                | PackError::ExtraBatches
                | PackError::MissingBatches
                | PackError::CorruptPack
                | PackError::InvalidEpoch => Some(Penalty::Severe),
                PackError::IO(_)
                | PackError::BatchLoad(_)
                | PackError::EpochLoad(_)
                | PackError::Append(_)
                | PackError::IndexAppend(_)
                | PackError::Fetch(_)
                | PackError::Open(_)
                | PackError::ReadOnly
                | PackError::ReadError(_)
                | PackError::MissingAuthority
                | PackError::SendFailed
                | PackError::ReceiveFailed
                | PackError::PersistError(_)
                | PackError::InvalidConsensusNumber(_, _)
                | PackError::ConsensusNumberAlreadyAdded
                | PackError::ConsensusNumberTooLow
                | PackError::ConsensusNumberTooHigh => None,
            },
            ConsensusChainError::EpochMismatch
            | ConsensusChainError::PrevCommitteeEpochMismatch
            | ConsensusChainError::CrcError => Some(Penalty::Mild),
            ConsensusChainError::EmptyImport | ConsensusChainError::InvalidImport => {
                Some(Penalty::Severe)
            }
            ConsensusChainError::StreamUnavailable
            | ConsensusChainError::NoCurrentEpoch
            | ConsensusChainError::EpochDbError(_)
            | ConsensusChainError::IO(_) => None,
        }
    }
}

/// Handle inter-node communication between primaries.
#[derive(Debug)]
pub struct PrimaryNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: PrimaryNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// Hold a reference to the consensus chain.
    consensus_chain: ConsensusChain,
    /// Semaphore bounding total concurrent epoch pack stream operations (pending + active).
    epoch_stream_semaphore: Arc<Semaphore>,
    /// Pending epoch pack requests awaiting stream from requestor.
    ///
    /// Wrapped in `Arc<Mutex>` so spawned stream tasks can look up the matching
    /// request after reading the correlation digest from the stream.
    pending_epoch_requests: Arc<Mutex<HashMap<PendingEpochRequestKey, PendingEpochStream>>>,
    /// My node's BLS public key.
    bls_public_key: BlsPublicKey,
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
        consensus_bus: ConsensusBusApp,
        state_sync: StateSynchronizer<DB>,
        task_spawner: TaskSpawner,
        consensus_chain: ConsensusChain,
    ) -> Self {
        let bls_public_key = consensus_config.key_config().primary_public_key();
        let request_handler = RequestHandler::new(
            consensus_config,
            consensus_bus,
            state_sync.clone(),
            consensus_chain.clone(),
        );
        let epoch_stream_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_EPOCH_STREAMS));
        let pending_batch_requests = Arc::new(Mutex::new(HashMap::default()));
        Self {
            network_events,
            network_handle,
            request_handler,
            task_spawner,
            consensus_chain,
            epoch_stream_semaphore,
            pending_epoch_requests: pending_batch_requests,
            bls_public_key,
        }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("primary network events", async move {
            // start interval for pruning stale stream requests
            let mut prune_requests = tokio::time::interval(PENDING_REQUEST_PRUNE_INTERVAL);
            loop {
                tokio::select! {
                    // process network events
                    next = self.network_events.recv() => {
                        match next {
                            Some(event) => {
                                self.process_network_event(event);
                            }
                            None => {
                                warn!(target: "primary::network", "critical worker network events channel dropped");
                                break;
                            }
                        }
                    }
                    // periodically prune stale stream requests
                    _ = prune_requests.tick() => {
                        self.cleanup_stale_pending_requests();
                    }
                }
            }
        });
    }

    /// Clean up stale pending requests that have timed out.
    fn cleanup_stale_pending_requests(&mut self) {
        let now = Instant::now();
        self.pending_epoch_requests
            .lock()
            .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
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
                PrimaryRequest::PeerExchange { .. } => {
                    warn!(target: "primary::network", "primary application received unexpected peer exchange message");
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
                PrimaryRequest::StreamEpoch { epoch } => {
                    self.process_epoch_stream(peer, epoch, channel, cancel)
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
            NetworkEvent::InboundStream { peer, stream } => {
                self.process_inbound_stream(peer, stream);
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
        number: u64,
        hash: BlockHash,
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
                        // Penalize peer's reputation for bad request.
                        // This could happen now and then and not be malicious so use a Mild only
                        // to close any DOS attack.
                        if let Err(e) = &header {
                            let my_number = request_handler.consensus_chain().latest_consensus_number();
                            tracing::warn!(target: "primary::network", ?e, ?my_number, ?number, ?hash, ?peer,  "applying penalty for failed consesus header request");
                            network_handle.handle.report_penalty(peer, Penalty::Mild).await;
                        }
                        let response = header.into_response();
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
                        // penalize peer's reputation for bad request
                        if let Err(err) = &header {
                            if let Some(penalty) = err.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                        }
                        let response = header.into_response();
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process a request to stream an epoch pack file.
    fn process_epoch_stream(
        &self,
        peer: BlsPublicKey,
        epoch: Epoch,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // acquire semaphore permit (non-blocking) for global concurrency
        let ack = match self.epoch_stream_semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let mut pending_map = self.pending_epoch_requests.lock();

                // check per-peer capacity
                let peer_count = pending_map.keys().filter(|(p, _)| *p == peer).count();
                if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
                    info!(
                        target: "primary::network",
                        %peer,
                        peer_count,
                        "rejecting batch stream request: per-peer limit reached"
                    );
                    // permit drops here, freeing the slot
                    false
                } else {
                    let mut hasher = tn_types::DefaultHashFunction::new();
                    hasher.update(&epoch.to_le_bytes());
                    let request_digest = B256::from_slice(hasher.finalize().as_bytes());
                    // If the same peer re-requests the same epoch while a prior entry
                    // is still pending, preserve the original `created_at` so the
                    // cleanup timer is not rearmed. Without this, a peer could hold a
                    // slot indefinitely by re-requesting before the 30s timeout.
                    // A second stream open is still punished as a protocol violation.
                    let created_at = pending_map
                        .get(&(peer, request_digest))
                        .map(|p| p.created_at)
                        .unwrap_or_else(Instant::now);
                    let pending = PendingEpochStream { epoch, created_at, _permit: permit };
                    if pending_map.insert((peer, request_digest), pending).is_some() {
                        debug!(
                            target: "primary::network",
                            %peer,
                            ?request_digest,
                            epoch,
                            "pending epoch stream request replaced with identical epoch request"
                        );
                    }
                    debug!(
                        target: "primary::network",
                        %peer,
                        ?request_digest,
                        epoch,
                        "pending epoch stream request accepted"
                    );
                    true
                }
            }
            Err(_) => false,
        };

        let response: PrimaryNetworkResult<PrimaryResponse> =
            Ok(PrimaryResponse::RequestEpochStream { ack, peer: self.bls_public_key });
        // send response
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-request-epoch-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            let msg = match response {
                Ok(msg) => msg,
                Err(err) => {
                    let error = err.to_string();
                    if let Some(penalty) = (&err).into() {
                        network_handle.report_penalty(peer, penalty).await;
                    }

                    PrimaryResponse::Error(message::PrimaryRPCError(error))
                }
            };

            // send response or cancel
            tokio::select! {
                _ = network_handle.inner_handle().send_response(msg, channel) => (),
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from committee.
    fn process_gossip(&self, msg: GossipMessage, propagation_source: BlsPublicKey) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ProcessGossip-{}-{propagation_source}", msg.topic);
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

    /// Process an inbound stream for epoch pack transfer.
    ///
    /// Reads the request digest from the stream and validates against pending requests.
    fn process_inbound_stream(&self, peer: BlsPublicKey, mut stream: Stream) {
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let pending_map = self.pending_epoch_requests.clone();
        let task_name = format!("stream-requested-epoch-{peer}");
        let consensus_chain = self.consensus_chain.clone();
        self.task_spawner.spawn_task(task_name, async move {
            // read the request digest (32-bytes) from the stream with timeout
            let mut digest_buf = [0u8; tn_types::DIGEST_LENGTH];
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.read_exact(&mut digest_buf),
            ).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(target: "primary::network", %peer, ?e, "failed to read request digest from stream");
                    return;
                }
                Err(_) => {
                    warn!(target: "primary::network", %peer, "timeout reading request digest from stream");
                    return;
                }
            }
            let request_digest = B256::from(digest_buf);

            // look up and remove the matching pending request
            let opt_pending_req = pending_map
                .lock()
                .remove(&(peer, request_digest));

            // process stream
            if let Err(err) = request_handler
                .process_request_epoch_stream(peer, opt_pending_req, stream, request_digest, &consensus_chain)
                .await {
                    // apply applicable penalty for error
                    warn!(target: "primary::network", ?err, "error processing request batches stream");
                    if let Some(penalty) = (&err).into() {
                        network_handle.report_penalty(peer, penalty).await;
                    }
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
    pub(crate) fn new(consensus_bus: ConsensusBus, payload_store: DB) -> Self {
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
