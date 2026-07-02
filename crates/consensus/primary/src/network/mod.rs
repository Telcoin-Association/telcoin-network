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
    proposer::OurDigestMessage, state_sync::StateSynchronizer, ConsensusBus, ConsensusBusApp,
};
use futures::{AsyncReadExt as _, AsyncWriteExt as _, StreamExt as _};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use parking_lot::Mutex;
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{
        IntoResponse as _, NetworkCommand, NetworkEvent, NetworkHandle, NetworkResponseMessage,
        NetworkResult,
    },
    write_frame, DenyReason, GossipMessage, Penalty, PrimarySyncRequest, ResponseChannel, Stream,
    StreamError, StreamKind, SyncFrame, SyncFrameError,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::{ConsensusChain, ConsensusChainError},
    consensus_pack::PackError,
    PayloadStore,
};
use tn_types::{
    encode, BlsPublicKey, BlsSignature, Certificate, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusResult, Database, Epoch, EpochCertificate, EpochDigest, EpochRecord, EpochVote,
    Header, HeaderDigest, Round, TaskError, TaskSpawner, TnReceiver, TnSender, Vote, B256,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{debug, info, warn};
pub mod handler;
mod message;
mod sync_codec;

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

/// Maximum bytes the client will read from a single consensus-output stream.
///
/// Generous upper bound: comfortably above any realistic single output, but caps an
/// unbounded or malicious stream.
/// TODO- replace with a size from consensus.  See Issue 782.
const MAX_CONSENSUS_OUTPUT_STREAM_BYTES: usize = 512 * 1024 * 1024;

/// Per-read timeout while draining a consensus-output stream (slow-peer guard).
const CONSENSUS_OUTPUT_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for the responder's first sync frame (`Ack`/`Deny`) after the epoch-pack
/// request frame is written. A peer that negotiated the sync protocol but does not
/// answer (a pre-cutover node that registered the protocol but reads the stream on
/// the legacy digest path) trips this and is cached unsyncable this epoch, so the
/// probe moves on to the next peer.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for reading the opening request frame of an inbound sync stream, and the
/// bound on every best-effort trailing write (shed `Deny`, malformed `Err`). A peer
/// that opens a sync stream but never sends its request, or applies receive
/// backpressure and stops reading, cannot hold an admission slot indefinitely.
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of network sync probe attempts per [`request_epoch_pack`] call
/// before the full-pack fetch gives up (returns `Err`) for this call.
///
/// This caps probe *attempts* (stream opens), not peers examined: a peer cached
/// unsyncable this epoch is skipped with only a cache lookup (no network I/O) and
/// does not count against the budget, so successive calls keep discovering newly
/// upgraded peers without an all-unsupported fleet ever spending more than this many
/// sync opens per call.
///
/// [`request_epoch_pack`]: PrimaryNetworkHandle::request_epoch_pack
const MAX_EPOCH_SYNC_PROBES: usize = 3;

/// What a [`PendingStreamRequest`] should stream once the peer opens the stream.
#[derive(Debug, Clone, Copy)]
pub enum StreamRequestKind {
    /// Stream the full (finished) pack file for an epoch.
    EpochPack(Epoch),
    /// Stream a verifiable PREFIX of an epoch's pack file, up to and including the consensus
    /// output with `last_consensus_number` (used for the in-progress current epoch).
    EpochPackPartial {
        /// The epoch we are streaming consensus data for.
        epoch: Epoch,
        /// The final (inclusive) consensus header number to stream up to.
        last_consensus_number: u64,
    },
    /// Stream the raw bytes for a single consensus output (by consensus chain number).
    ConsensusOutput(u64),
}

/// Correlation digest for a single consensus-output stream request.
///
/// Each kind is domain-separated so distinct requests never collide in the pending map:
/// - `EpochPack(epoch)` hashes only the epoch — byte-for-byte the original full-stream scheme, so
///   existing full-stream clients are unaffected.
/// - `EpochPackPartial { epoch, n }` additionally mixes in the stop number, giving it a distinct
///   digest from the full-epoch request.
/// - `ConsensusOutput(n)` is tagged so output `N` never collides with epoch `N`.
fn stream_request_digest(kind: &StreamRequestKind) -> B256 {
    let mut hasher = tn_types::DefaultHashFunction::new();
    match kind {
        StreamRequestKind::EpochPack(epoch) => {
            hasher.update(b"epoch-pack");
            hasher.update(&epoch.to_le_bytes());
        }
        StreamRequestKind::EpochPackPartial { epoch, last_consensus_number } => {
            hasher.update(b"epoch-pack-partial");
            hasher.update(&epoch.to_le_bytes());
            hasher.update(&last_consensus_number.to_le_bytes());
        }
        StreamRequestKind::ConsensusOutput(number) => {
            hasher.update(b"consensus-output");
            hasher.update(&number.to_le_bytes());
        }
    }
    B256::from_slice(hasher.finalize().as_bytes())
}

/// Tracks a pending stream request (epoch pack or single consensus output) awaiting stream
/// establishment.
// pub for IT
#[derive(Debug)]
pub struct PendingStreamRequest {
    /// What to stream once the peer opens the correlated stream.
    kind: StreamRequestKind,
    /// When this request was created (for timeout cleanup).
    created_at: Instant,
    /// Semaphore permit held for the lifetime of this request (pending + active).
    /// Dropping the permit frees a global concurrency slot.
    _permit: OwnedSemaphorePermit,
}

/// Key for pending requests: (peer_bls, request_digest)
type PendingEpochRequestKey = (BlsPublicKey, B256);

impl PendingStreamRequest {
    /// Create a new pending stream request.
    pub fn new(kind: StreamRequestKind, permit: OwnedSemaphorePermit) -> Self {
        Self { kind, created_at: Instant::now(), _permit: permit }
    }

    /// What this pending request will stream.
    pub fn kind(&self) -> StreamRequestKind {
        self.kind
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl PendingStreamRequest {
    /// Create a pending stream request with a custom `created_at` for testing stale cleanup.
    pub fn new_with_created_at(
        kind: StreamRequestKind,
        permit: OwnedSemaphorePermit,
        created_at: Instant,
    ) -> Self {
        Self { kind, created_at, _permit: permit }
    }
}

/// The outcome of an epoch-pack fetch attempt over the typed sync protocol.
enum EpochPackAttempt {
    /// The peer served and the pack was imported into the consensus chain.
    Imported,
    /// The peer did not answer the sync exchange; cache it unsyncable this epoch
    /// and skip it on later probes (it must upgrade to be syncable again).
    Unsupported,
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// RAII guard for an admitted inbound sync epoch-pack stream.
///
/// Holds a global concurrency permit and counts toward the peer's per-peer
/// in-flight total. Dropping it releases the global slot and decrements the
/// per-peer count, so a finished or aborted exchange frees capacity for both the
/// sync and legacy responder paths.
#[derive(Debug)]
struct SyncStreamPermit {
    /// Global concurrency permit, released on drop.
    _permit: OwnedSemaphorePermit,
    /// Shared per-peer in-flight counter, decremented on drop.
    peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// The peer whose count this permit holds.
    peer: BlsPublicKey,
}

impl Drop for SyncStreamPermit {
    fn drop(&mut self) {
        let mut peers = self.peers.lock();
        if let Some(count) = peers.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                peers.remove(&self.peer);
            }
        }
    }
}

/// Try to admit one inbound sync epoch-pack stream for `peer`.
///
/// Acquires a global permit and admits only if the peer's combined in-flight
/// count (legacy pending requests plus sync streams) is below
/// [`MAX_PENDING_REQUESTS_PER_PEER`], so the per-peer cap holds across both paths.
/// Returns `None` (shedding the global permit) when either cap is hit.
///
/// Locks `pending_epoch_requests` before `sync_stream_peers`; the legacy admission
/// path takes the same order, so the two never deadlock.
fn try_admit_sync(
    semaphore: &Arc<Semaphore>,
    pending: &Arc<Mutex<HashMap<PendingEpochRequestKey, PendingStreamRequest>>>,
    sync_peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<SyncStreamPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let pending_guard = pending.lock();
    let legacy_count = pending_guard.keys().filter(|(p, _)| *p == peer).count();
    let mut sync_guard = sync_peers.lock();
    let sync_count = sync_guard.get(&peer).copied().unwrap_or(0);
    (legacy_count + sync_count < MAX_PENDING_REQUESTS_PER_PEER).then(|| {
        *sync_guard.entry(peer).or_insert(0) += 1;
        SyncStreamPermit { _permit: permit, peers: sync_peers.clone(), peer }
    })
}

/// Primary network specific handle.
#[derive(Clone, Debug)]
pub struct PrimaryNetworkHandle {
    handle: NetworkHandle<Req, Res>,
    /// The genesis chain id, used to namespace gossip topics this handle publishes.
    chain_id: u64,
    /// Per-peer sync-protocol capability for epoch-pack fetch, learned by probing.
    ///
    /// Absent means not yet probed (try the sync protocol); `false` means the peer
    /// did not answer a sync open (a pre-cutover peer that fails negotiation, or one
    /// that negotiated but never `Ack`ed), so it is skipped on later probes this epoch
    /// (full-pack fetch has no legacy fallback); `true` means the peer served (or is
    /// serving) the sync protocol. Cleared each epoch via
    /// [`Self::clear_sync_capability`] so a peer upgraded over the rotation boundary
    /// is re-probed.
    sync_capability: Arc<Mutex<HashMap<BlsPublicKey, bool>>>,
}

// Test-only conversion that defaults the chain id to 0. Gated to tests so the only
// way to build a production handle is `new`, with the genesis chain id supplied
// explicitly: a 0 here would publish on the `-0` topic suffix while validators read
// the real id, a silent gossip mismatch.
#[cfg(test)]
impl From<NetworkHandle<Req, Res>> for PrimaryNetworkHandle {
    fn from(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle, chain_id: 0, sync_capability: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl PrimaryNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(handle: NetworkHandle<Req, Res>, chain_id: u64) -> Self {
        Self { handle, chain_id, sync_capability: Arc::new(Mutex::new(HashMap::new())) }
    }

    //// Convenience method for creating a new Self for tests.
    pub fn new_for_test(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self {
            handle: NetworkHandle::new(sender),
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Clear the per-peer epoch-pack sync-capability cache.
    ///
    /// Called at each epoch boundary: committees rotate and binaries are upgraded
    /// there, so a peer that did not answer the sync protocol last epoch is
    /// re-probed for it this epoch.
    pub fn clear_sync_capability(&self) {
        self.sync_capability.lock().clear();
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<PrimaryRequest, PrimaryResponse> {
        &self.handle
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(Box::new(certificate)));
        self.handle.publish(tn_config::LibP2pConfig::primary_topic(self.chain_id), data).await?;
        Ok(())
    }

    /// Publish a consensus block number and hash of the header.
    pub async fn publish_consensus(
        &self,
        epoch: Epoch,
        round: Round,
        consensus_block_num: u64,
        consensus_header_hash: ConsensusHeaderDigest,
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
        self.handle
            .publish(tn_config::LibP2pConfig::consensus_output_topic(self.chain_id), data)
            .await?;
        Ok(())
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_epoch_vote(&self, vote: EpochVote) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
        self.handle.publish(tn_config::LibP2pConfig::epoch_vote_topic(self.chain_id), data).await?;
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
        let mut res = res.await??.result;
        let mut tries = 0;
        while let PrimaryResponse::RecoverableError(PrimaryRPCError(s)) = res {
            warn!(target: "primary::network", "Got recoverable error {s}, retrying");
            tokio::time::sleep(Duration::from_millis(250)).await;
            let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
            let res_raw = self.handle.send_request(request, peer).await?;
            res = res_raw.await??.result;
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
            PrimaryResponse::StreamRequestAck { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is stream ack!".to_string(),
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
        let res = res.await??.result;
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
        hash: ConsensusHeaderDigest,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??.result;
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
        hash: ConsensusHeaderDigest,
    ) -> NetworkResult<ConsensusHeader> {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            let res = match tokio::time::timeout(TIMEOUT, res).await {
                Ok(r) => r,
                Err(_) => {
                    tracing::warn!(target: "primary::network", ?number, "request_consensus timed out waiting for peer response");
                    continue;
                }
            };
            let res = res?;
            if let Ok(NetworkResponseMessage {
                peer,
                result: PrimaryResponse::ConsensusHeader(header),
            }) = res
            {
                if header.digest() == hash && header.number == number {
                    return Ok(Arc::unwrap_or_clone(header));
                } else {
                    tracing::warn!(target: "primary::network", "Returned header does not match number {number}/{} or hash {hash}/{}, try again!",
                        header.number,
                        header.digest()
                    );
                    // Give the naughty peer a penalty.
                    self.report_penalty(peer, Penalty::Medium).await;
                }
            }
        }
        Err(NetworkError::RPCError("Could not get the consensus header!".to_string()))
    }

    /// Request the raw (serialized) consensus output bytes for `number` from a specific peer.
    ///
    /// Returns the pack-file encoded output (batches + consensus header). A single output can
    /// exceed the request/response message-size limit, so the bytes are streamed: this negotiates
    /// the stream via RPC, then opens a stream and reads the bytes. The caller is responsible for
    /// deserializing the result (with the epoch's committee) and verifying it; the bytes cannot be
    /// cheaply validated at the network layer.
    pub async fn request_consensus_output_from_peer(
        &self,
        peer: BlsPublicKey,
        number: u64,
    ) -> NetworkResult<Vec<u8>> {
        let request = PrimaryRequest::StreamConsensusOutput { number };
        let request_digest = stream_request_digest(&StreamRequestKind::ConsensusOutput(number));
        let resp = self.handle.send_request(request, peer).await?.await??;
        let PrimaryResponse::StreamRequestAck { ack } = resp.result else {
            return Err(NetworkError::RPCError(
                "Got wrong response, not a stream ack!".to_string(),
            ));
        };
        if !ack {
            return Err(NetworkError::RPCError(
                "peer declined consensus output stream request".to_string(),
            ));
        }
        let bytes = self.stream_consensus_output(peer, request_digest).await?;
        if bytes.is_empty() {
            return Err(NetworkError::RPCError(
                "peer streamed an empty consensus output".to_string(),
            ));
        }
        Ok(bytes)
    }

    /// Request the raw (serialized) consensus output bytes for `number` from a random peer,
    /// trying up to three times from three different peers.
    ///
    /// Returns the pack-file encoded output (batches + consensus header). A single output can
    /// exceed the request/response message-size limit, so the bytes are streamed (see
    /// [`Self::request_consensus_output_from_peer`]). The caller is responsible for deserializing
    /// the result (with the epoch's committee) and verifying it; the bytes cannot be cheaply
    /// validated at the network layer.
    pub async fn request_consensus_output(&self, number: u64) -> NetworkResult<Vec<u8>> {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let request = PrimaryRequest::StreamConsensusOutput { number };
        let request_digest = stream_request_digest(&StreamRequestKind::ConsensusOutput(number));
        // Try up to three times (from three peers) to get the output.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let dispatch = match self.handle.send_request_any(request.clone()).await {
                Ok(rx) => rx,
                Err(e) => {
                    warn!(target: "primary::network", ?e, ?number, "send_request_any failed; retrying");
                    continue;
                }
            };
            let resp = match tokio::time::timeout(TIMEOUT, dispatch).await {
                Ok(Ok(Ok(r))) => r,
                Ok(Ok(Err(e))) => {
                    warn!(target: "primary::network", ?e, ?number, "peer responded with error; retrying");
                    continue;
                }
                Ok(Err(e)) => {
                    warn!(target: "primary::network", ?e, ?number, "peer dropped response channel; retrying");
                    continue;
                }
                Err(_) => {
                    warn!(target: "primary::network", ?number, "request_consensus_output timed out waiting for peer response");
                    continue;
                }
            };
            let PrimaryResponse::StreamRequestAck { ack } = resp.result else {
                continue;
            };
            if !ack {
                continue;
            }
            match self.stream_consensus_output(resp.peer, request_digest).await {
                Ok(bytes) if !bytes.is_empty() => return Ok(bytes),
                Ok(_) => {
                    warn!(target: "primary::network", ?number, peer = %resp.peer, "peer streamed an empty consensus output; retrying");
                }
                Err(e) => {
                    warn!(target: "primary::network", ?e, ?number, peer = %resp.peer, "failed to stream consensus output; retrying");
                }
            }
        }
        Err(NetworkError::RPCError("Could not get the consensus output!".to_string()))
    }

    /// Open a stream to `peer`, write the correlation digest, and read the streamed consensus
    /// output bytes to EOF.
    async fn stream_consensus_output(
        &self,
        peer: BlsPublicKey,
        request_digest: B256,
    ) -> NetworkResult<Vec<u8>> {
        let mut stream = self.handle.open_stream(peer, StreamKind::Legacy).await??;
        stream
            .write_all(request_digest.as_slice())
            .await
            .map_err(|e| NetworkError::RPCError(format!("failed to write request digest: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| NetworkError::RPCError(format!("failed to flush request digest: {e}")))?;

        let mut out = Vec::new();
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = match tokio::time::timeout(
                CONSENSUS_OUTPUT_STREAM_READ_TIMEOUT,
                stream.read(&mut buf[..]),
            )
            .await
            {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    return Err(NetworkError::RPCError(format!("stream read failed: {e}")))
                }
                Err(_) => return Err(NetworkError::RPCError("stream read timed out".to_string())),
            };
            if n == 0 {
                break;
            }
            if out.len() + n > MAX_CONSENSUS_OUTPUT_STREAM_BYTES {
                return Err(NetworkError::RPCError(
                    "consensus output stream exceeded maximum size".to_string(),
                ));
            }
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    pub async fn request_epoch_cert(
        &self,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
    ) -> NetworkResult<(EpochRecord, EpochCertificate)> {
        let request = PrimaryRequest::EpochRecord { epoch, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            if let Ok(Ok(NetworkResponseMessage {
                peer: _,
                result: PrimaryResponse::EpochRecord { record, certificate },
            })) = res.await
            {
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

    /// Attempt to get a complete epoch pack file from any peer over the typed
    /// `/tn-primary-sync` protocol.
    ///
    /// Full-pack fetch is typed-only (no legacy fallback): this returns `Err`, and
    /// the caller retries, when no connected peer successfully serves a pack over
    /// the sync protocol (no peers, all unsupported, or all failed to import). See
    /// [`Self::request_epoch_pack_sync`] for the per-peer probe behavior.
    pub async fn request_epoch_pack(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        self.request_epoch_pack_inner(
            epoch_record,
            previous_epoch,
            consensus_chain,
            None,
            record_timeout,
        )
        .await
    }

    /// Attempt to get a verifiable PREFIX of an epoch's pack file from any peer via stream,
    /// stopping after consensus number `last_consensus_number`.
    ///
    /// Behaves exactly like [`Self::request_epoch_pack`] but negotiates a partial transfer. The
    /// caller must supply an `epoch_record` whose `final_consensus` matches the partial stop point
    /// so the streamed prefix verifies. Used to fetch the in-progress current epoch up to a
    /// known point.
    pub async fn request_partial_epoch_pack(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        last_consensus_number: u64,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        self.request_epoch_pack_inner(
            epoch_record,
            previous_epoch,
            consensus_chain,
            Some(last_consensus_number),
            record_timeout,
        )
        .await
    }

    /// Shared implementation for full ([`Self::request_epoch_pack`]) and partial
    /// ([`Self::request_partial_epoch_pack`]) epoch pack streaming. A full pack
    /// (`last_consensus_number` is `None`) is fetched over the typed sync protocol
    /// only; a partial prefix (`Some`) still negotiates over the legacy stream path,
    /// which has no typed sync request variant yet.
    async fn request_epoch_pack_inner(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        last_consensus_number: Option<u64>,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        let epoch = epoch_record.epoch;

        // Full epoch pack is typed-only (#739, step 6): the legacy `StreamEpoch`
        // fallback was removed so that a full-pack fetch hard-fails (and the caller
        // retries) when no connected peer serves the `/tn-primary-sync` protocol,
        // rather than silently syncing over the legacy stream path. This forces
        // peers to upgrade. The partial-prefix transfer has no typed sync request
        // variant yet, so it still negotiates over the legacy path below.
        let Some(last_consensus_number) = last_consensus_number else {
            return self
                .request_epoch_pack_sync(
                    epoch,
                    epoch_record,
                    previous_epoch,
                    consensus_chain,
                    record_timeout,
                )
                .await;
        };

        // Partial prefix: negotiate the stream over the legacy path. Try up to three
        // times (from three peers) to get consensus. This could be a lot more
        // complicated but this KISS method should work fine.
        let (request, kind) = (
            PrimaryRequest::StreamEpochPartial { epoch, last_consensus_number },
            StreamRequestKind::EpochPackPartial { epoch, last_consensus_number },
        );
        let request_digest = stream_request_digest(&kind);

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
            if let NetworkResponseMessage {
                peer,
                result: PrimaryResponse::StreamRequestAck { ack },
            } = resp
            {
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

                // open raw stream then write request_digest for correlation. The
                // partial-prefix transfer still uses the legacy path (no typed sync
                // request variant yet).
                let mut stream = self.handle.open_stream(peer, StreamKind::Legacy).await??;
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

                // This is the partial-prefix path (the full-request path returned early above), so
                // import into a side "staging" dir where it cannot race the in-order build of the
                // current epoch's main pack.
                let import_result = consensus_chain
                    .import_partial_to_staging(
                        stream.compat(),
                        epoch_record,
                        previous_epoch,
                        record_timeout,
                    )
                    .await;
                if let Err(err) = import_result {
                    if let Some(penalty) = Self::consensus_chain_error_to_penalty(&err) {
                        self.report_penalty(peer, penalty).await;
                    }
                    warn!(
                        target: "primary::network",
                        %peer,
                        epoch,
                        ?err,
                        "FAILED to stream epoch pack file from peer"
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

    /// Attempt to fetch and import a full epoch pack over the typed sync protocol.
    ///
    /// Probes up to [`MAX_EPOCH_SYNC_PROBES`] connected peers that are not cached
    /// unsyncable this epoch, opening a `/tn-primary-sync` stream whose opening
    /// frame carries the request. Returns `Ok(())` as soon as one peer serves a pack
    /// that imports; otherwise `Err` (full-pack fetch has no legacy fallback, so the
    /// caller retries). A peer that does not answer is cached unsyncable (skipping
    /// its probe next time); the probe is penalty-exempt either way.
    async fn request_epoch_pack_sync(
        &self,
        epoch: Epoch,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        let peers = self.handle.connected_peers().await?;

        // Probe candidate peers one at a time (the async analog of a short-circuiting
        // fold): `filter` drops peers cached unsyncable this epoch for free, `take`
        // bounds the network probe attempts, `then` runs each exchange and records its
        // verdict in the capability cache, and `any` stops at the first peer whose pack
        // imports (so no peer past the first success is probed).
        let imported = futures::stream::iter(peers)
            .filter(move |peer| {
                let known_unsyncable = self.sync_capability.lock().get(peer) == Some(&false);
                futures::future::ready(!known_unsyncable)
            })
            .take(MAX_EPOCH_SYNC_PROBES)
            .then(move |peer| async move {
                match self
                    .sync_epoch_pack_from_peer(
                        peer,
                        epoch,
                        epoch_record,
                        previous_epoch,
                        consensus_chain,
                        record_timeout,
                    )
                    .await
                {
                    EpochPackAttempt::Imported => {
                        self.sync_capability.lock().insert(peer, true);
                        info!(
                            target: "primary::network",
                            %peer,
                            epoch,
                            "streamed epoch pack file over sync protocol"
                        );
                        true
                    }
                    EpochPackAttempt::Unsupported => {
                        self.sync_capability.lock().insert(peer, false);
                        debug!(
                            target: "primary::network",
                            %peer,
                            "peer did not answer epoch pack sync; caching unsyncable this epoch"
                        );
                        false
                    }
                    EpochPackAttempt::Failed(e) => {
                        // the peer speaks sync but this exchange failed; keep it
                        // sync-capable and try the next peer
                        self.sync_capability.lock().insert(peer, true);
                        warn!(
                            target: "primary::network",
                            %peer,
                            epoch,
                            ?e,
                            "epoch pack sync exchange failed; trying next peer"
                        );
                        false
                    }
                }
            })
            .any(futures::future::ready)
            .await;

        imported.then_some(()).ok_or_else(|| {
            NetworkError::RPCError(
                "no peer served the epoch pack over the sync protocol".to_string(),
            )
        })
    }

    /// Run one epoch-pack sync exchange against `peer`, flattening the classified
    /// outcome into a single [`EpochPackAttempt`].
    async fn sync_epoch_pack_from_peer(
        &self,
        peer: BlsPublicKey,
        epoch: Epoch,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> EpochPackAttempt {
        self.try_sync_epoch_pack_exchange(
            peer,
            epoch,
            epoch_record,
            previous_epoch,
            consensus_chain,
            record_timeout,
        )
        .await
        .map_or_else(|attempt| attempt, |()| EpochPackAttempt::Imported)
    }

    /// Open a `/tn-primary-sync` stream, write the [`PrimarySyncRequest::EpochPack`]
    /// request in the opening frame, and stream the pack through
    /// [`ConsensusChain::stream_import`].
    ///
    /// `Err(EpochPackAttempt::Unsupported)` means the peer did not answer the
    /// protocol (negotiation failed, or it negotiated but never `Ack`ed), so it is
    /// cached unsyncable and skipped on later probes this epoch.
    /// `Err(EpochPackAttempt::Failed(_))` means a transient or exchange-level error
    /// once the peer has proved sync-capable, so the caller keeps it sync-capable and
    /// tries the next peer. A transport I/O
    /// error during the open (`UpgradeIo`) is transient rather than a protocol
    /// mismatch, so it maps to `Failed` instead of poisoning the capability cache.
    async fn try_sync_epoch_pack_exchange(
        &self,
        peer: BlsPublicKey,
        epoch: Epoch,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> Result<(), EpochPackAttempt> {
        // open the sync stream, flattening the command-channel and stream-open
        // results. Only a genuine negotiation failure (`UpgradeFailed`) is a
        // pre-cutover peer that does not advertise the protocol -> Unsupported
        // (penalty-exempt, cached unsyncable this epoch). A transient upgrade I/O
        // error or any other open error is not proof the peer lacks sync -> try next
        // peer.
        let mut stream =
            self.handle.open_stream(peer, StreamKind::Sync).await.and_then(|s| s).map_err(|e| {
                match () {
                    () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                        EpochPackAttempt::Unsupported
                    }
                    () => EpochPackAttempt::Failed(e),
                }
            })?;

        // write the request in the opening frame. Negotiation already succeeded, so
        // the peer is sync-capable; a write failure is transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        let request = SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch });
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(&mut stream, &request, &mut encode_buffer, &mut compressed_buffer, max_frame)
            .await
            .and(stream.flush().await)
            .map_err(|e| {
                EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to write epoch pack sync request frame: {e}"
                )))
            })?;

        // read the responder's first frame. A timeout (outer `Err`) means no `Ack`
        // -> a peer that negotiated sync but does not serve it, so cache it
        // unsyncable and try the next peer. A read I/O error (inner `Err`) after
        // negotiation is transient -> try the next peer, unless it is a clean close
        // (a pre-cutover peer that shut the misread stream), which is also an
        // Unsupported signal.
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, PrimarySyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut compressed_buffer,
                max_frame,
            ),
        )
        .await
        .map_err(|_elapsed| EpochPackAttempt::Unsupported)?
        .map_err(|e| {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ) {
                EpochPackAttempt::Unsupported
            } else {
                EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to read epoch pack sync ack frame: {e}"
                )))
            }
        })?;

        match first {
            // accepted: reassemble the `Data`/`End` frames into a contiguous reader
            // and import it. A bad pack is the peer's fault on the legacy path, but
            // the sync path is metrics-only during rollout: classify it `Failed`
            // (try next peer) without a penalty.
            SyncFrame::Ack => consensus_chain
                .stream_import(
                    sync_codec::sync_pack_reader(stream),
                    epoch_record,
                    previous_epoch,
                    record_timeout,
                )
                .await
                .map_err(|e| {
                    EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                        "failed to import epoch pack over sync stream: {e}"
                    )))
                }),
            // sync-capable, but shedding load or lacking the pack: try next peer
            SyncFrame::Deny(reason) => Err(EpochPackAttempt::Failed(NetworkError::RPCRetryable(
                format!("peer denied epoch pack sync request: {reason:?}"),
            ))),
            SyncFrame::Err(err) => Err(EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                "peer aborted epoch pack sync exchange: {err:?}"
            )))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => {
                Err(EpochPackAttempt::Failed(NetworkError::ProtocolError(
                    "unexpected opening sync frame from peer".to_string(),
                )))
            }
        }
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
                | PackError::TooManyBatches(_)
                | PackError::CorruptPack
                | PackError::InvalidEpoch(_, _) => Some(Penalty::Severe),
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
            | ConsensusChainError::InvalidPackEpoch(_, _)
            | ConsensusChainError::CantSaveAndNotAvailable(_)
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
    /// Semaphore bounding total concurrent stream operations (pending + active), shared by epoch
    /// pack and single consensus-output streams.
    epoch_stream_semaphore: Arc<Semaphore>,
    /// Pending stream requests (epoch pack or single consensus output) awaiting stream from
    /// requestor.
    ///
    /// Wrapped in `Arc<Mutex>` so spawned stream tasks can look up the matching
    /// request after reading the correlation digest from the stream.
    pending_epoch_requests: Arc<Mutex<HashMap<PendingEpochRequestKey, PendingStreamRequest>>>,
    /// Per-peer count of in-flight sync epoch-pack streams.
    ///
    /// The legacy per-peer count comes from `pending_epoch_requests`; this counts
    /// the sync path's in-flight exchanges. Admission on either path checks the sum
    /// against [`MAX_PENDING_REQUESTS_PER_PEER`], so the per-peer cap is the combined
    /// limit across both paths.
    sync_stream_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
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
            sync_stream_peers: Arc::new(Mutex::new(HashMap::default())),
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
                                break Err(TaskError::from_message("critical worker network events channel dropped"));
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
                    self.process_epoch_stream(peer, epoch, None, channel, cancel)
                }
                PrimaryRequest::StreamEpochPartial { epoch, last_consensus_number } => self
                    .process_epoch_stream(
                        peer,
                        epoch,
                        Some(last_consensus_number),
                        channel,
                        cancel,
                    ),
                PrimaryRequest::StreamConsensusOutput { number } => {
                    self.process_consensus_output_stream(peer, number, channel, cancel)
                }
            },
            NetworkEvent::Gossip { message, relayer, author } => {
                self.process_gossip(message, relayer, author);
            }
            NetworkEvent::Error(msg, channel) => {
                let err = PrimaryResponse::Error(PrimaryRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.task_spawner.spawn_task("report request error", async move {
                    let _ = network_handle.handle.send_response(err, channel).await;
                    Ok(())
                });
            }
            NetworkEvent::InboundStream { peer, kind, stream } => match kind {
                StreamKind::Legacy => self.process_inbound_stream(peer, stream),
                StreamKind::Sync => self.process_inbound_sync_stream(peer, stream),
            },
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
            Ok(())
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
            Ok(())
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_consensus_output_request(
        &self,
        peer: BlsPublicKey,
        number: u64,
        hash: ConsensusHeaderDigest,
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
                        // Route through the central PrimaryNetworkError → Penalty mapping
                        // so every handler in this file applies penalties consistently.
                        // The only reachable variant from this path is
                        // UnknownConsensusHeaderDigest, which the central table now maps
                        // to None — observers legitimately request not-yet-served headers.
                        if let Err(ref e) = header {
                            if let Some(penalty) = e.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                            let my_number = request_handler.consensus_chain().latest_consensus_number();
                            tracing::debug!(
                                target: "primary::network",
                                ?e, ?my_number, ?number, ?hash, ?peer,
                                "consensus header request could not be served"
                            );
                        }
                        let response = header.into_response();
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
            Ok(())
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_epoch_record_request(
        &self,
        peer: BlsPublicKey,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
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
            Ok(())
        });
    }

    /// Reserve a global concurrency slot for a stream request and register it in the pending map.
    ///
    /// Returns whether the request was accepted. Acceptance requires both an available semaphore
    /// permit (global concurrency) and the peer being under its per-peer pending limit. The permit
    /// is moved into the [`PendingStreamRequest`] and held for the lifetime of the request.
    fn accept_stream_request(
        &self,
        peer: BlsPublicKey,
        request_digest: B256,
        kind: StreamRequestKind,
    ) -> bool {
        // acquire semaphore permit (non-blocking) for global concurrency
        let Ok(permit) = self.epoch_stream_semaphore.clone().try_acquire_owned() else {
            return false;
        };

        let mut pending_map = self.pending_epoch_requests.lock();

        // check per-peer capacity. The cap is the combined limit across the legacy
        // and sync paths, so add this peer's in-flight sync streams to its legacy
        // pending count. Locks pending then sync_stream_peers, matching
        // try_admit_sync's order so the two admission paths never deadlock.
        let legacy_count = pending_map.keys().filter(|(p, _)| *p == peer).count();
        let sync_count = self.sync_stream_peers.lock().get(&peer).copied().unwrap_or(0);
        let peer_count = legacy_count + sync_count;
        if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
            info!(
                target: "primary::network",
                %peer,
                peer_count,
                "rejecting stream request: per-peer limit reached"
            );
            // permit drops here, freeing the slot
            return false;
        }

        // If the same peer re-requests the same data while a prior entry is still
        // pending, preserve the original `created_at` so the cleanup timer is not
        // rearmed. Without this, a peer could hold a slot indefinitely by re-requesting
        // before the 30s timeout. A second stream open is still punished as a protocol
        // violation.
        let created_at = pending_map
            .get(&(peer, request_digest))
            .map(|p| p.created_at)
            .unwrap_or_else(Instant::now);
        let pending = PendingStreamRequest { kind, created_at, _permit: permit };
        if pending_map.insert((peer, request_digest), pending).is_some() {
            debug!(
                target: "primary::network",
                %peer,
                ?request_digest,
                ?kind,
                "pending stream request replaced with identical request"
            );
        }
        debug!(
            target: "primary::network",
            %peer,
            ?request_digest,
            ?kind,
            "pending stream request accepted"
        );
        true
    }

    /// Spawn a task to send a [`PrimaryResponse::StreamRequestAck`] for a stream negotiation.
    fn send_stream_ack(
        &self,
        ack: bool,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
        task_name: String,
    ) {
        let msg = PrimaryResponse::StreamRequestAck { ack };
        let network_handle = self.network_handle.clone();
        self.task_spawner.spawn_task(task_name, async move {
            // send response or cancel
            tokio::select! {
                _ = network_handle.inner_handle().send_response(msg, channel) => (),
                _ = cancel => (),
            }
            Ok(())
        });
    }

    /// Process a request to stream an epoch pack file.
    ///
    /// `last_consensus_number` is `None` for a full-epoch transfer and `Some(n)` to stream only the
    /// verifiable prefix up to consensus number `n` (the in-progress current epoch).
    fn process_epoch_stream(
        &self,
        peer: BlsPublicKey,
        epoch: Epoch,
        last_consensus_number: Option<u64>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        let kind = match last_consensus_number {
            None => StreamRequestKind::EpochPack(epoch),
            Some(last_consensus_number) => {
                StreamRequestKind::EpochPackPartial { epoch, last_consensus_number }
            }
        };
        let request_digest = stream_request_digest(&kind);
        let ack = self.accept_stream_request(peer, request_digest, kind);
        self.send_stream_ack(ack, channel, cancel, format!("process-request-epoch-{peer}"));
    }

    /// Process a request to stream a single consensus output's raw bytes.
    fn process_consensus_output_stream(
        &self,
        peer: BlsPublicKey,
        number: u64,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        let kind = StreamRequestKind::ConsensusOutput(number);
        let request_digest = stream_request_digest(&kind);
        let ack = self.accept_stream_request(peer, request_digest, kind);
        self.send_stream_ack(ack, channel, cancel, format!("process-request-output-{peer}"));
    }

    /// Process gossip from committee.
    fn process_gossip(
        &self,
        msg: GossipMessage,
        relayer: Option<BlsPublicKey>,
        author: Option<BlsPublicKey>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let relayer_label =
            relayer.as_ref().map_or_else(|| "unresolved".to_string(), |bls| bls.to_string());
        let task_name = format!("ProcessGossip-{}-{relayer_label}", msg.topic);
        // spawn task to process gossip
        self.task_spawner.spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "primary::network", ?e, "process_gossip");
                // Charge the accountable peer, and only once its BLS identity has resolved. A
                // content-determined fault (malformed payload / mis-topic) is the message
                // author's: the network layer forwarded it after a shallow check, so an honest
                // relayer could not have screened it, and banning the relayer lets a Byzantine
                // author partition the mesh (issues #801/#819). Every other fault is the relaying
                // peer's, as before. `zip` skips the penalty when that peer is unresolved or the
                // error carries no penalty.
                let charged = if e.is_author_content_fault() { author } else { relayer };
                if let Some((peer, penalty)) = charged.zip((&e).into()) {
                    network_handle.report_penalty(peer, penalty).await;
                }
                Err(e.into())
            } else {
                Ok(())
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
                    return Err(e.into());
                }
                Err(e) => {
                    warn!(target: "primary::network", %peer, "timeout reading request digest from stream");
                    return Err(e.into());
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
                    Err(err.into())
                } else {
                    Ok(())
                }
        });
    }

    /// Process an inbound sync epoch-pack stream.
    ///
    /// Admission against the shared concurrency caps happens here on stream open:
    /// the request rides in the opening frame, so there is no `(peer, digest)`
    /// pending map for this path. A shedding responder writes
    /// [`DenyReason::AtCapacity`] without reading so the requester retries elsewhere
    /// immediately. Once admitted, the opening request frame is read (bounded by
    /// [`SYNC_REQUEST_READ_TIMEOUT`]) and an `EpochPack` request is served by
    /// [`RequestHandler::process_sync_epoch_pack_stream`]. The admission permit is
    /// held for the lifetime of the spawned task.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        // admit against the shared caps before spawning; the permit (if any) moves
        // into the task and frees capacity on drop
        let permit = try_admit_sync(
            &self.epoch_stream_semaphore,
            &self.pending_epoch_requests,
            &self.sync_stream_peers,
            peer,
        );
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let task_name = format!("sync-epoch-pack-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            let mut stream = stream;
            let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
            let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

            // shed load: deny without reading so the requester retries elsewhere
            let Some(_permit) = permit else {
                debug!(target: "primary::network", %peer, "denying inbound sync stream: at capacity");
                // bound the best-effort shed write: a peer that applies receive
                // backpressure and never reads must not stall this task.
                let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                    let _ = write_frame(
                        &mut stream,
                        &SyncFrame::<PrimarySyncRequest>::Deny(DenyReason::AtCapacity),
                        &mut encode_buffer,
                        &mut compressed_buffer,
                        max_frame,
                    )
                    .await;
                    let _ = stream.close().await;
                })
                .await;
                return Ok(());
            };

            // read the opening request frame; a peer that never sends one (timeout)
            // or sends a malformed one (io error) is dropped after releasing the
            // permit. Collapse the timeout/io results rather than nesting matches.
            let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
            let request = tokio::time::timeout(
                SYNC_REQUEST_READ_TIMEOUT,
                read_frame::<_, PrimarySyncRequest>(
                    &mut stream,
                    &mut decode_buffer,
                    &mut decompress_buffer,
                    max_frame,
                ),
            )
            .await
            .ok()
            .and_then(Result::ok);
            let Some(request) = request else {
                warn!(target: "primary::network", %peer, "no readable sync request frame");
                let _ = stream.close().await;
                return Ok(());
            };

            match request {
                SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch }) => {
                    request_handler
                        .process_sync_epoch_pack_stream(peer, stream, epoch, &consensus_chain)
                        .await?;
                }
                // `MissingCertificates` over sync is item 7; the primary registers
                // the request type but does not serve it yet. Decline cleanly.
                SyncFrame::Req(PrimarySyncRequest::MissingCertificates { .. }) => {
                    debug!(target: "primary::network", %peer, "declining unimplemented sync request: missing certificates (item 7)");
                    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                        let _ = write_frame(
                            &mut stream,
                            &SyncFrame::<PrimarySyncRequest>::Deny(DenyReason::Unavailable),
                            &mut encode_buffer,
                            &mut compressed_buffer,
                            max_frame,
                        )
                        .await;
                        let _ = stream.close().await;
                    })
                    .await;
                }
                // a well-behaved requester always opens with `Req`; anything else is
                // malformed. Signal it and drop (metrics-only, no penalty).
                SyncFrame::Ack
                | SyncFrame::Deny(_)
                | SyncFrame::Data(_)
                | SyncFrame::End
                | SyncFrame::Err(_) => {
                    warn!(target: "primary::network", %peer, "unexpected opening sync frame from requester");
                    // bound the best-effort error write so a non-reading peer cannot
                    // pin the held admission permit on an unbounded write.
                    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                        let _ = write_frame(
                            &mut stream,
                            &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Malformed),
                            &mut encode_buffer,
                            &mut compressed_buffer,
                            max_frame,
                        )
                        .await;
                        let _ = stream.close().await;
                    })
                    .await;
                }
            }
            Ok(())
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
    MissingParents(Vec<HeaderDigest>),
}
