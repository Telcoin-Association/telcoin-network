//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    proposer::OurDigestMessage, state_sync::StateSynchronizer, ConsensusBus, ConsensusBusApp,
};
use futures::{AsyncWriteExt as _, FutureExt as _, StreamExt as _, TryFutureExt as _};
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
    StreamError, SyncFrame, SyncFrameError,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::{ConsensusChain, ConsensusChainError},
    consensus_pack::PackError,
    PayloadStore,
};
use tn_types::{
    encode, BlsPublicKey, BlsSignature, Certificate, ConsensusHeaderDigest, ConsensusOutput,
    ConsensusResult, Database, Epoch, EpochCertificate, EpochDigest, EpochRecord, EpochVote,
    Header, HeaderDigest, Round, TaskError, TaskSpawner, TnReceiver, TnSender, Vote,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
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

/// Maximum number of concurrent epoch stream operations (pending + active).
///
/// A semaphore permit is held from RPC acceptance through stream completion,
/// so this bounds the true concurrent count—not just the pending map size.
pub const MAX_CONCURRENT_EPOCH_STREAMS: usize = 5;

/// Maximum number of concurrent `EpochRecord` request-response serves across all peers.
///
/// The `EpochRecord` request-response arm is reachable by any peer whose identity resolves
/// (not committee membership), and each serve does real work: a database lookup plus, for the
/// in-progress epoch, an up-to-1.5s certificate-wait. Without a bound a peer could spawn
/// unbounded concurrent, penalty-free serve tasks. A dedicated semaphore (separate from
/// `MAX_CONCURRENT_EPOCH_STREAMS` so an epoch-record flood and the stream-serving paths never
/// starve one another) caps the concurrent serve count and, with it, the total in-flight
/// certificate-wait budget. See GHSA-vc2r-9cp2-w74j.
pub const MAX_CONCURRENT_EPOCH_RECORD_REQUESTS: usize = 5;

/// Hard cap on the number of distinct pending consensus results tracked in the
/// handler's `consensus_certs` signature-tally map at any time.
///
/// `consensus_certs` is a transient tally that is wiped wholesale the moment any
/// consensus result reaches quorum, so under honest operation only a handful of
/// entries (typically one, for the next consensus number) are ever live at once.
/// Capping the map bounds its memory against a gossip flood of validly-signed but
/// non-quorum `ConsensusResult`s from Byzantine committee members: no matter how
/// many members collude, they cannot grow the map past this cap. See the
/// `PrimaryGossip::Consensus` handler and GHSA-2r5c-c4h7-gp5h.
pub(crate) const MAX_CONSENSUS_CERTS: usize = 20;

/// Hard cap on the number of distinct epoch votes deduplicated at ingress.
///
/// The handler's `epoch_votes_seen` map records one entry per `(author, epoch, epoch-record
/// hash)` once a vote's signature has verified, so a replayed or flooded vote is dropped before the
/// next (expensive) BLS verify. Sized well above a single committee's worth of votes across a
/// few candidate epoch records. Eviction is least-recently-seen, so an over-flood only costs
/// redundant verifies (never a dropped honest vote — the collector dedups per signer). See the
/// `PrimaryGossip::EpochVote` handler and issue #898.
pub(crate) const MAX_EPOCH_VOTES: usize = 1024;

/// Maximum number of distinct in-flight `consensus_certs` tallies any single committee member
/// may be a signer of for one consensus number at a time. An honest validator signs exactly one
/// hash per consensus number, so a signer present in this many distinct live tallies for the same
/// number is equivocating; its further fresh digests for that number are dropped. Per
/// `(signer, number)`, not per signer. See the handler and GHSA-pvhw-9pmg-q2hg.
pub(crate) const MAX_TALLIES_PER_SIGNER_PER_NUMBER: usize = 2;
/// Maximum number of concurrent pending batch requests from a single peer.
///
/// Prevents a single malicious peer from filling all global slots.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Timeout for the responder's first sync frame (`Ack`/`Deny`) after the epoch-pack
/// request frame is written. A peer that negotiated the sync protocol but does not
/// answer (a pre-cutover node that registered the protocol but reads the stream on
/// the legacy digest path) trips this and is cached unsyncable this epoch, so the
/// probe moves on to the next peer.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Overall bound on reading a streamed missing-certificates response, on top of the
/// per-frame timeout. Without it a Byzantine peer that `Ack`s and then drips a tiny
/// valid `Data` frame just inside the per-frame timeout could keep the requester's
/// read loop (and its open substream) alive far longer than any honest transfer;
/// this caps the whole post-`Ack` read so such a peer is dropped and the fan-out
/// moves on. Comfortably covers a realistic catch-up response (a few MiB) while
/// keeping an abandoned per-peer fetch task's lifetime in the same ballpark as the
/// legacy request-response path's network timeout; a peer too slow to finish in this
/// window is treated as failed and another peer (or the legacy path) is tried.
const MISSING_CERTS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Bound on the opening `Req` frame of an inbound sync stream (and on the tiny
/// best-effort control writes that follow). The `EpochPack` request is a few bytes,
/// but a `MissingCertificates` request carries one skip-round bitmap per committee
/// authority, so this is sized well above the per-pack-chunk bound to admit a large
/// (but still bounded) request. An oversized request is rejected at read, after
/// which the requester simply falls back to the legacy path, so this is a safety
/// bound rather than a hard compatibility limit.
const MAX_SYNC_REQUEST_FRAME_SIZE: usize = 4 * 1024 * 1024;

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

/// The outcome of a single-consensus-output fetch attempt over the typed sync
/// protocol.
///
/// Mirrors [`EpochPackAttempt`] but carries the fully decoded, header-hash-verified
/// output on success: the exchange stream-decodes the reassembled pack bytes and
/// verifies the header digest against the requested hash BEFORE buffering batches, so
/// only a verified output ever reaches the caller (a wrong-hash peer becomes `Failed`
/// and the probe falls through to the next peer instead of wedging the walk).
enum ConsensusOutputAttempt {
    /// The peer served a header-hash-verified output.
    Fetched(ConsensusOutput),
    /// The peer did not answer the sync exchange. Unlike a full-pack `Unsupported`
    /// this is NOT cached unsyncable, because a peer that speaks `/tn-primary-sync`
    /// but cannot decode this newer `ConsensusOutput` variant still serves full epoch
    /// packs and must not be hidden from the full-pack path.
    Unsupported,
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// One epoch-pack fetch over the typed sync protocol, as threaded unchanged
/// through every per-peer probe (`request_epoch_pack_sync` down to the stream
/// exchange). Bundles what the exchange needs so each probe hop takes the peer
/// plus this instead of six loose parameters.
#[derive(Clone, Copy)]
struct EpochPackSyncRequest<'a> {
    /// The epoch whose pack is requested.
    epoch: Epoch,
    /// `None` requests the full pack; `Some(n)` requests a verifiable prefix
    /// stopping after consensus number `n`.
    last_consensus_number: Option<u64>,
    /// Record of the epoch being fetched; the streamed pack verifies against it.
    epoch_record: &'a EpochRecord,
    /// The preceding epoch's record, anchoring verification.
    previous_epoch: &'a EpochRecord,
    /// Destination chain the served pack imports into.
    consensus_chain: &'a ConsensusChain,
    /// Timeout applied when importing the streamed records.
    record_timeout: Duration,
}

/// One single-consensus-output fetch over the typed sync protocol, threaded through
/// each per-peer probe (`request_consensus_output` down to the stream exchange).
/// Bundles what the exchange needs — the destination chain (for the epoch committee)
/// and the already-verified header hash — so the reassembled bytes are stream-decoded
/// and header-hash-verified in place instead of round-tripping an unverified `Vec<u8>`
/// back to the caller.
#[derive(Clone, Copy)]
struct ConsensusOutputSyncRequest<'a> {
    /// Consensus number requested.
    number: u64,
    /// Epoch that `number` falls in; selects the committee for decoding.
    epoch: Epoch,
    /// The already-verified header digest for `number` (from validated gossip or a
    /// verified descendant's `parent_hash`). The decoded header MUST hash to this.
    expected_hash: ConsensusHeaderDigest,
    /// Chain holding the epoch's committee, used to stream-decode and verify.
    consensus_chain: &'a ConsensusChain,
}

/// RAII guard for one admitted in-flight request holding a global concurrency permit
/// and a per-peer in-flight slot.
///
/// Holds a semaphore permit and counts toward the peer's entry in a per-peer in-flight
/// counter. Dropping it releases the global slot and decrements the per-peer count, so a
/// finished or aborted request frees capacity again. Reused by the inbound sync epoch-pack
/// path and the `EpochRecord` request-response path, each with its own semaphore and counter.
#[derive(Debug)]
pub(crate) struct PeerSlotPermit {
    /// Global concurrency permit, released on drop.
    _permit: OwnedSemaphorePermit,
    /// Shared per-peer in-flight counter, decremented on drop.
    peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// The peer whose count this permit holds.
    peer: BlsPublicKey,
}

impl Drop for PeerSlotPermit {
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
/// Acquires a global permit and admits only if the peer's in-flight sync-stream
/// count is below [`MAX_PENDING_REQUESTS_PER_PEER`]. Returns `None` (shedding the
/// global permit) when either cap is hit.
fn try_admit_sync(
    semaphore: &Arc<Semaphore>,
    sync_peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<PeerSlotPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let mut sync_guard = sync_peers.lock();
    let sync_count = sync_guard.get(&peer).copied().unwrap_or(0);
    (sync_count < MAX_PENDING_REQUESTS_PER_PEER).then(|| {
        *sync_guard.entry(peer).or_insert(0) += 1;
        PeerSlotPermit { _permit: permit, peers: sync_peers.clone(), peer }
    })
}

/// Try to admit one `EpochRecord` request-response serve for `peer`.
///
/// Acquires a global permit from the dedicated epoch-record semaphore and admits only while the
/// peer has fewer than [`MAX_PENDING_REQUESTS_PER_PEER`] in-flight epoch-record serves. Returns
/// `None` (shedding the global permit) when either cap is hit, so the caller sheds the request
/// without spawning work. The returned [`PeerSlotPermit`] is held for the serve's lifetime and
/// frees both caps on drop. Unlike [`try_admit_sync`] this path has its own semaphore and
/// counter, so it neither consults nor contends the stream pending map.
pub(crate) fn try_admit_epoch_record(
    semaphore: &Arc<Semaphore>,
    peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<PeerSlotPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let mut guard = peers.lock();
    let count = guard.get(&peer).copied().unwrap_or(0);
    (count < MAX_PENDING_REQUESTS_PER_PEER).then(|| {
        *guard.entry(peer).or_insert(0) += 1;
        PeerSlotPermit { _permit: permit, peers: peers.clone(), peer }
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
    #[allow(clippy::too_many_arguments)]
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

    /// Fetch missing certificates from `peer` over the typed sync protocol.
    ///
    /// Item 7 (#739) cut the certificate fetch fully over to `/tn-primary-sync`: there is
    /// no legacy request-response fallback. The exchange opens a stream, writes the
    /// request in the opening frame, reads the `Ack`, and reassembles the streamed
    /// certificate `Data` frames; any failure (negotiation, shed, transport, or protocol)
    /// returns an error so the staggered fan-out tries the next peer. The exchange is
    /// penalty-exempt, and cancelling this future (the fan-out's cancel-on-first-success)
    /// drops the in-flight substream, resetting it. The returned certificates are
    /// unverified; the caller validates them.
    pub async fn fetch_certificates(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        // open the sync stream, flattening the command-channel and stream-open results. A
        // peer that does not advertise the protocol surfaces an error here; with no
        // fallback it is simply a failed peer and the fan-out moves on.
        let mut stream = self.handle.open_stream(peer).await.and_then(|s| s)?;

        // write the request in the opening frame
        let request_frame = SyncFrame::Req(PrimarySyncRequest::MissingCertificates {
            exclusive_lower_bound: request.exclusive_lower_bound,
            skip_rounds: request.skip_rounds.clone(),
        });
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(
            &mut stream,
            &request_frame,
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        )
        .await
        .and(stream.flush().await)
        .map_err(|e| {
            NetworkError::RPCRetryable(format!(
                "failed to write missing certificates sync request frame: {e}"
            ))
        })?;

        // read the responder's first frame, bounded by the ack timeout
        let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, PrimarySyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut decompress_buffer,
                sync_codec::MAX_SYNC_CERT_FRAME_SIZE,
            ),
        )
        .await
        .map_err(|_elapsed| {
            NetworkError::RPCRetryable(
                "timed out awaiting missing certificates sync ack".to_string(),
            )
        })?
        .map_err(|e| {
            NetworkError::RPCRetryable(format!(
                "failed to read missing certificates sync ack frame: {e}"
            ))
        })?;

        match first {
            // accepted: reassemble the streamed certificate `Data` frames under an overall
            // read timeout (on top of the per-frame timeout) so a peer that `Ack`s then
            // drips frames cannot pin the read loop and its substream
            SyncFrame::Ack => tokio::time::timeout(
                MISSING_CERTS_RESPONSE_TIMEOUT,
                sync_codec::read_sync_certificates(&mut stream),
            )
            .await
            .map_err(|_elapsed| {
                NetworkError::RPCRetryable(
                    "timed out reading missing certificates over sync stream".to_string(),
                )
            })?
            .map_err(|e| {
                NetworkError::RPCError(format!(
                    "failed to read missing certificates over sync stream: {e}"
                ))
            }),
            // the responder is shedding load or declines the request: try another peer
            SyncFrame::Deny(reason) => Err(NetworkError::RPCRetryable(format!(
                "peer denied missing certificates sync request: {reason:?}"
            ))),
            SyncFrame::Err(err) => Err(NetworkError::RPCError(format!(
                "peer aborted missing certificates sync exchange: {err:?}"
            ))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => Err(
                NetworkError::ProtocolError("unexpected opening sync frame from peer".to_string()),
            ),
        }
    }

    /// Fetch, decode, and header-hash-verify the consensus output for `number` from any
    /// connected peer over the typed `/tn-primary-sync` protocol.
    ///
    /// `expected_hash` is the already-verified header digest for `number` (from validated
    /// gossip or a verified descendant's `parent_hash`); `consensus_chain` supplies the epoch
    /// committee. The v1 pack is header-first, so each per-peer exchange stream-decodes the
    /// reassembled bytes and checks the header digest against `expected_hash` BEFORE buffering
    /// any batch — bounding the unverified buffer to a single header record and returning only
    /// a verified [`ConsensusOutput`]. Probes up to `MAX_EPOCH_SYNC_PROBES` peers not cached
    /// unsyncable and returns the first VERIFIED output, otherwise `Err` (the caller retries).
    /// A peer that serves proves it speaks the sync protocol (cached `true`); a peer that does
    /// not answer is NOT cached (an ambiguous `Unsupported`; see `ConsensusOutputAttempt`). A
    /// peer that streams a well-formed but wrong-hash output is penalized (Severe) and skipped,
    /// so the probe falls through to the next peer instead of wedging the caller's retry loop.
    pub async fn request_consensus_output(
        &self,
        number: u64,
        consensus_chain: &ConsensusChain,
        expected_hash: ConsensusHeaderDigest,
    ) -> NetworkResult<ConsensusOutput> {
        let peers = self.handle.connected_peers().await?;
        let epoch = consensus_chain.epochs().number_to_epoch(number);
        if !consensus_chain.contains_decode_epoch(epoch).await {
            // We do not have a pack file for epoch and will not be able to decode so bail now.
            // This should never happen, normally we would downloading consensus for the current
            // pack file, anything else will not be savable and would not make sense.
            // This check will just short circuit early if in an invalid state vs loop
            // over peers continually.
            return Err(NetworkError::RPCError(
                "we do not contain a pack file to decode this consensus output".to_string(),
            ));
        }
        // Resolve the committee-selecting epoch once and bundle the request so every probe hop
        // carries the chain and the verified hash needed to stream-decode + verify in place.
        let request = ConsensusOutputSyncRequest { number, epoch, expected_hash, consensus_chain };

        // Probe candidate peers one at a time (the async analog of a short-circuiting
        // fold): `filter` drops peers cached unsyncable for free, `take` bounds the
        // network probes, `then` runs each exchange and records its verdict, and
        // `filter_map` + `next` stop at the first peer that serves a VERIFIED output (so no
        // peer past the first success is probed, and a wrong-hash peer is skipped).
        let probes = futures::stream::iter(peers)
            .filter(move |peer| {
                let known_unsyncable = self.sync_capability.lock().get(peer) == Some(&false);
                futures::future::ready(!known_unsyncable)
            })
            .take(MAX_EPOCH_SYNC_PROBES)
            .then(move |peer| async move {
                match self.sync_consensus_output_from_peer(peer, request).await {
                    ConsensusOutputAttempt::Fetched(output) => {
                        self.sync_capability.lock().insert(peer, true);
                        debug!(
                            target: "primary::network",
                            %peer,
                            number,
                            "fetched verified consensus output over sync protocol"
                        );
                        Some(output)
                    }
                    ConsensusOutputAttempt::Unsupported => {
                        // Ambiguous: the peer may serve full epoch packs but not decode
                        // this newer `ConsensusOutput` variant, so do NOT cache `false`
                        // and hide it from the full-pack path.
                        debug!(
                            target: "primary::network",
                            %peer,
                            number,
                            "peer did not answer consensus output sync; skipping this probe"
                        );
                        None
                    }
                    ConsensusOutputAttempt::Failed(e) => {
                        // the peer speaks sync but this exchange failed; keep it
                        // sync-capable and try the next peer
                        self.sync_capability.lock().insert(peer, true);
                        warn!(
                            target: "primary::network",
                            %peer,
                            number,
                            ?e,
                            "consensus output sync exchange failed; trying next peer"
                        );
                        None
                    }
                }
            })
            .filter_map(futures::future::ready);

        // `next` needs the stream `Unpin`, which the `then(async move { .. })`
        // combinator is not, so pin the probe chain on the stack before pulling the
        // first success.
        let mut probes = std::pin::pin!(probes);
        probes.next().await.ok_or_else(|| {
            NetworkError::RPCError(
                "no peer served the consensus output over the sync protocol".to_string(),
            )
        })
    }

    /// Run one consensus-output sync exchange against `peer`, flattening the classified
    /// outcome into a single [`ConsensusOutputAttempt`].
    async fn sync_consensus_output_from_peer(
        &self,
        peer: BlsPublicKey,
        request: ConsensusOutputSyncRequest<'_>,
    ) -> ConsensusOutputAttempt {
        self.try_sync_consensus_output_exchange(peer, request)
            .await
            .map_or_else(|attempt| attempt, ConsensusOutputAttempt::Fetched)
    }

    /// Open a `/tn-primary-sync` stream, write the consensus-output request in the
    /// opening frame, read the `Ack`, then stream-decode and header-hash-verify the output.
    ///
    /// `Err(ConsensusOutputAttempt::Unsupported)` means the peer did not answer the
    /// protocol (negotiation failed, or it negotiated but never `Ack`ed - e.g. a peer
    /// that speaks `/tn-primary-sync` but cannot decode the newer `ConsensusOutput`
    /// variant and shut the stream). `Err(ConsensusOutputAttempt::Failed(_))` means a
    /// transient or exchange-level error (including a decode/verification failure, which
    /// is also penalized) once the peer has proved sync-capable. A transport I/O error
    /// during the open (`UpgradeIo`) is transient rather than a protocol mismatch, so it
    /// maps to `Failed`. `Ok` is always a header-hash-verified output.
    async fn try_sync_consensus_output_exchange(
        &self,
        peer: BlsPublicKey,
        request: ConsensusOutputSyncRequest<'_>,
    ) -> Result<ConsensusOutput, ConsensusOutputAttempt> {
        // open the sync stream, flattening the command-channel and stream-open results.
        // Only a genuine negotiation failure (`UpgradeFailed`) is a peer that does not
        // advertise the protocol -> Unsupported; a transient upgrade I/O error or any
        // other open error is not proof the peer lacks sync -> try next.
        let mut stream =
            self.handle.open_stream(peer).await.and_then(|s| s).map_err(|e| match () {
                () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                    ConsensusOutputAttempt::Unsupported
                }
                () => ConsensusOutputAttempt::Failed(e),
            })?;

        // write the request in the opening frame. Negotiation already succeeded, so a
        // write failure is transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        let req_frame =
            SyncFrame::Req(PrimarySyncRequest::ConsensusOutput { number: request.number });
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(&mut stream, &req_frame, &mut encode_buffer, &mut compressed_buffer, max_frame)
            .await
            .and(stream.flush().await)
            .map_err(|e| {
                ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to write consensus output sync request frame: {e}"
                )))
            })?;

        // read the responder's first frame. A timeout (outer `Err`) means no `Ack` -> a
        // peer that negotiated sync but does not serve this variant, so try the next
        // peer. A clean close on the read (inner `Err`) is the same Unsupported signal;
        // any other read error after negotiation is transient -> Failed.
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
        .map_err(|_elapsed| ConsensusOutputAttempt::Unsupported)?
        .map_err(|e| {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ) {
                ConsensusOutputAttempt::Unsupported
            } else {
                ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to read consensus output sync ack frame: {e}"
                )))
            }
        })?;

        match first {
            // accepted: stream-decode the reassembled `Data`/`End` frames straight into the
            // pack decoder, verifying the header digest against the requested (already-
            // verified) hash the instant the header record is read — before any batch is
            // buffered. A wrong-hash or malformed output fails here (and is penalized below),
            // so the probe falls through to the next peer instead of returning unverified
            // bytes. An empty stream yields `NotConsensus`, i.e. a miss -> next peer.
            SyncFrame::Ack => {
                let reader = sync_codec::sync_pack_reader(stream);
                match request
                    .consensus_chain
                    .stream_decode_consensus_output(request.epoch, reader, request.expected_hash)
                    .await
                {
                    Ok(output) => Ok(output),
                    Err(e) => {
                        // A classified protocol/verification violation (a header-digest
                        // mismatch -> Severe) scores the peer down; transient decode/IO errors
                        // carry no penalty. Either way, drop this peer and try the next.
                        if let Some(penalty) = Self::consensus_chain_error_to_penalty(&e) {
                            self.report_penalty(peer, penalty).await;
                        }
                        Err(ConsensusOutputAttempt::Failed(NetworkError::RPCError(format!(
                            "failed to decode/verify consensus output over sync stream: {e}"
                        ))))
                    }
                }
            }
            // sync-capable, but shedding load or lacking the output: try next peer
            SyncFrame::Deny(reason) => {
                Err(ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "peer denied consensus output sync request: {reason:?}"
                ))))
            }
            SyncFrame::Err(err) => Err(ConsensusOutputAttempt::Failed(NetworkError::RPCError(
                format!("peer aborted consensus output sync exchange: {err:?}"),
            ))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => {
                Err(ConsensusOutputAttempt::Failed(NetworkError::ProtocolError(
                    "unexpected opening sync frame from peer".to_string(),
                )))
            }
        }
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
    /// [`Self::request_epoch_pack_sync`] for the per-peer probe behavior. The partial
    /// prefix ([`Self::request_partial_epoch_pack`]) reuses the same probe over the
    /// sync protocol but keeps a legacy fallback.
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
    /// only; a partial prefix (`Some`) is fetched over the sync protocol first and
    /// falls back to the legacy stream path for peers that do not yet serve the
    /// `EpochPackPartial` sync variant.
    async fn request_epoch_pack_inner(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        last_consensus_number: Option<u64>,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        let epoch = epoch_record.epoch;

        // Both full and partial epoch packs are typed-sync-only (#739, step 9c): the
        // legacy `StreamEpoch`/`StreamEpochPartial` raw-stream fallback was removed, so
        // a fetch hard-fails (and the caller retries) when no connected peer serves the
        // `/tn-primary-sync` protocol, rather than syncing over the legacy stream path.
        // This forces peers to upgrade. `request_epoch_pack_sync` selects the full pack
        // for `None` and the verifiable prefix for `Some(n)`.
        self.request_epoch_pack_sync(EpochPackSyncRequest {
            epoch,
            last_consensus_number,
            epoch_record,
            previous_epoch,
            consensus_chain,
            record_timeout,
        })
        .await
    }

    /// Attempt to fetch and import a full epoch pack over the typed sync protocol.
    ///
    /// Probes up to [`MAX_EPOCH_SYNC_PROBES`] connected peers that are not cached
    /// unsyncable this epoch, opening a `/tn-primary-sync` stream whose opening
    /// frame carries the request. Returns `Ok(())` as soon as one peer serves a pack
    /// that imports; otherwise `Err` (full-pack fetch has no legacy fallback, so the
    /// caller retries). A peer that does not answer is cached unsyncable (skipping
    /// its probe next time); the probe is penalty-exempt either way.
    async fn request_epoch_pack_sync(&self, sync: EpochPackSyncRequest<'_>) -> NetworkResult<()> {
        let EpochPackSyncRequest { epoch, last_consensus_number, .. } = sync;
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
                match self.sync_epoch_pack_from_peer(peer, sync).await {
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
                        // Only a FULL-pack `Unsupported` authoritatively proves the peer
                        // does not speak `/tn-primary-sync`. A PARTIAL `Unsupported` is
                        // ambiguous: the peer may be on an earlier item that serves full
                        // packs but cannot decode the newer `EpochPackPartial` variant, so
                        // it must not cache `false` and hide that peer from full-pack sync.
                        // (Partial still honors a `false` a full probe wrote and caches
                        // `true` on success, so it dedups without poisoning.)
                        if last_consensus_number.is_none() {
                            self.sync_capability.lock().insert(peer, false);
                        }
                        debug!(
                            target: "primary::network",
                            %peer,
                            partial = last_consensus_number.is_some(),
                            "peer did not answer epoch pack sync; skipping this probe"
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
        sync: EpochPackSyncRequest<'_>,
    ) -> EpochPackAttempt {
        self.try_sync_epoch_pack_exchange(peer, sync)
            .await
            .map_or_else(|attempt| attempt, |()| EpochPackAttempt::Imported)
    }

    /// Open a `/tn-primary-sync` stream, write the epoch-pack request in the opening
    /// frame, and stream the pack into the consensus chain. `last_consensus_number`
    /// selects the transfer: `None` requests the full pack ([`PrimarySyncRequest::EpochPack`])
    /// and imports it in place via [`ConsensusChain::stream_import`]; `Some(number)`
    /// requests a verifiable prefix ([`PrimarySyncRequest::EpochPackPartial`]) and imports
    /// it into a side staging dir via [`ConsensusChain::import_partial_to_staging`].
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
        sync: EpochPackSyncRequest<'_>,
    ) -> Result<(), EpochPackAttempt> {
        let EpochPackSyncRequest {
            epoch,
            last_consensus_number,
            epoch_record,
            previous_epoch,
            consensus_chain,
            record_timeout,
        } = sync;
        // open the sync stream, flattening the command-channel and stream-open
        // results. Only a genuine negotiation failure (`UpgradeFailed`) is a
        // pre-cutover peer that does not advertise the protocol -> Unsupported
        // (penalty-exempt, cached unsyncable this epoch). A transient upgrade I/O
        // error or any other open error is not proof the peer lacks sync -> try next
        // peer.
        let mut stream =
            self.handle.open_stream(peer).await.and_then(|s| s).map_err(|e| match () {
                () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                    EpochPackAttempt::Unsupported
                }
                () => EpochPackAttempt::Failed(e),
            })?;

        // write the request in the opening frame. Negotiation already succeeded, so
        // the peer is sync-capable; a write failure is transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        // full pack vs a verifiable partial prefix: select the request variant through
        // the `Option`'s combinator (no Some/None branch).
        let request = SyncFrame::Req(
            last_consensus_number.map_or(PrimarySyncRequest::EpochPack { epoch }, |number| {
                PrimarySyncRequest::EpochPackPartial { epoch, last_consensus_number: number }
            }),
        );
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(&mut stream, &request, &mut encode_buffer, &mut compressed_buffer, max_frame)
            .await
            .and(stream.flush().await)
            .map_err(|e| {
                EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to write epoch pack sync request frame: {e}"
                )))
            })?;

        // read the responder's first frame. Negotiation already succeeded, so the
        // peer IS sync-capable: a missing `Ack` (timeout) or any read error is a
        // transient exchange failure, not a capability signal. Both map to `Failed`
        // (try the next peer, keep the peer sync-capable) rather than `Unsupported`,
        // which for a full pack would cache the peer unsyncable for the whole epoch
        // with no fallback (#739 completed the sync-only cutover). Only a negotiation
        // failure at open time (`UpgradeFailed`, above) proves the peer lacks sync.
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
        .map_err(|_elapsed| {
            EpochPackAttempt::Failed(NetworkError::RPCRetryable(
                "timed out reading epoch pack sync ack frame".to_string(),
            ))
        })?
        .map_err(|e| {
            EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to read epoch pack sync ack frame: {e}"
            )))
        })?;

        match first {
            // accepted: reassemble the `Data`/`End` frames into a contiguous reader
            // and import it. A full pack imports in place; a partial prefix imports to
            // a side staging dir so it cannot race the in-order build of the current
            // epoch's main pack (matching the legacy responder split). A bad pack is
            // the peer's fault on the legacy path, but the sync path is metrics-only
            // during rollout: classify it `Failed` (try next peer) without a penalty.
            SyncFrame::Ack => {
                let reader = sync_codec::sync_pack_reader(stream);
                // pick the import target on `last_consensus_number.is_some()` (a bool,
                // not an `Option` pattern-match); each branch normalizes to
                // `Result<(), EpochPackAttempt>` then boxes so the `if` unifies their
                // distinct future types.
                let import = if last_consensus_number.is_some() {
                    consensus_chain
                        .import_partial_to_staging(
                            reader,
                            epoch_record,
                            previous_epoch,
                            record_timeout,
                        )
                        .map_err(|e| {
                            EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                                "failed to import partial epoch pack over sync stream: {e}"
                            )))
                        })
                        .boxed()
                } else {
                    consensus_chain
                        .stream_import(reader, epoch_record, previous_epoch, record_timeout)
                        .map_err(|e| {
                            EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                                "failed to import epoch pack over sync stream: {e}"
                            )))
                        })
                        .boxed()
                };
                import.await
            }
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
                | PackError::UnexpectedConsensusDigest { .. }
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
                | PackError::InvalidVersion(_, _)
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
    /// Per-peer count of in-flight sync epoch-pack streams.
    ///
    /// Admission checks this count against [`MAX_PENDING_REQUESTS_PER_PEER`], the
    /// sole per-peer cap now that every bulk path rides the typed sync protocol.
    sync_stream_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// Semaphore bounding concurrent `EpochRecord` request-response serves.
    ///
    /// Separate from `epoch_stream_semaphore` so an epoch-record flood and the stream-serving
    /// paths never starve one another. See [`MAX_CONCURRENT_EPOCH_RECORD_REQUESTS`].
    epoch_record_semaphore: Arc<Semaphore>,
    /// Per-peer count of in-flight `EpochRecord` request-response serves, capped at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`].
    epoch_record_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
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
        Self {
            network_events,
            network_handle,
            request_handler,
            task_spawner,
            consensus_chain,
            epoch_stream_semaphore,
            sync_stream_peers: Arc::new(Mutex::new(HashMap::default())),
            epoch_record_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_EPOCH_RECORD_REQUESTS)),
            epoch_record_peers: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("primary network events", async move {
            loop {
                match self.network_events.recv().await {
                    Some(event) => {
                        self.process_network_event(event);
                    }
                    None => {
                        warn!(target: "primary::network", "critical worker network events channel dropped");
                        break Err(TaskError::from_message("critical worker network events channel dropped"));
                    }
                }
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
                PrimaryRequest::PeerExchange { .. } => {
                    warn!(target: "primary::network", "primary application received unexpected peer exchange message");
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
                PrimaryRequest::StreamEpoch { .. } => {
                    self.process_epoch_stream(peer, channel, cancel)
                }
                PrimaryRequest::StreamEpochPartial { .. } => {
                    self.process_epoch_stream(peer, channel, cancel)
                }
                PrimaryRequest::StreamConsensusOutput { .. } => {
                    self.process_consensus_output_stream(peer, channel, cancel)
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
            NetworkEvent::InboundStream { peer, stream } => {
                self.process_inbound_sync_stream(peer, stream)
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
                    // report penalty if any
                    //
                    // votes are consensus-critical, so a peer that returns a penalizable
                    // error must pay the same reputational cost as on every other request
                    // handler. Route the error through the central PrimaryNetworkError →
                    // Penalty table before collapsing it into a response, instead of
                    // silently dropping it.
                    if let Err(ref e) = vote {
                        if let Some(penalty) = e.into() {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                    }

                    let response = vote.into_response();
                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
            Ok(())
        });
    }

    /// Answer a legacy request-response `MissingCertificates` request.
    ///
    /// Item 7 (#739) cut the certificate fetch fully over to the `/tn-primary-sync`
    /// protocol, so this request-response path is no longer served: peers fetch
    /// certificates over sync. The request variant is retained for wire compatibility
    /// (BCS variant indices stay until the coordinated `/0.0.2` bump, item 9); a legacy
    /// requester is answered with a typed error rather than served.
    fn process_request_for_missing_certs(
        &self,
        peer: BlsPublicKey,
        _request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        let network_handle = self.network_handle.clone();
        let task_name = format!("MissingCertsDeprecated-{peer}");
        let response = PrimaryResponse::Error(PrimaryRPCError(
            "missing certificates are served over the sync protocol only".to_string(),
        ));
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                _ = network_handle.handle.send_response(response, channel) => (),
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
        // Admission control: bound concurrent epoch-record serving globally and per peer.
        //
        // This request-response arm is reachable by any peer whose identity resolves (not
        // committee membership) and each serve does real work (a database read plus, for the
        // in-progress epoch, an up-to-1.5s certificate-wait) while returning no penalty for an
        // unavailable epoch. Without a bound a peer could spawn unbounded, penalty-free serve
        // tasks. The permit is held for the serve's lifetime, so the concurrency cap also bounds
        // the total in-flight certificate-wait budget. See GHSA-vc2r-9cp2-w74j.
        let Some(permit) =
            try_admit_epoch_record(&self.epoch_record_semaphore, &self.epoch_record_peers, peer)
        else {
            // At capacity: shed in O(1) by dropping the response channel rather than spawning
            // work, so a flood beyond the cap costs only a permit try. Dropping the channel sends
            // no rejection over the wire, so the caller (`request_epoch_cert`) rotates to another
            // peer only after its request-response timeout elapses; that is added catch-up latency
            // on a background path, not a hang, and only while this primary is at capacity.
            // Replying "at capacity" instead would reintroduce a per-request task spawn, which is
            // exactly what this bound removes.
            return;
        };

        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ConsensusOutputReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            // hold the admission permit for the serve's lifetime; dropping it on completion (or
            // cancel) frees the global slot and decrements the per-peer count
            let _permit = permit;
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

    /// Answer a legacy request-response `StreamEpoch` / `StreamEpochPartial` request.
    ///
    /// Item 9c (#739) removed the raw `/tn-stream` epoch-pack path, so peers fetch full
    /// and partial epoch packs over the `/tn-primary-sync` protocol. The request variants
    /// are retained for wire compatibility (BCS variant indices stay until the coordinated
    /// `/0.0.2` bump); a legacy requester is answered with a typed error rather than served.
    fn process_epoch_stream(
        &self,
        peer: BlsPublicKey,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        let network_handle = self.network_handle.clone();
        let task_name = format!("EpochPackDeprecated-{peer}");
        let response = PrimaryResponse::Error(PrimaryRPCError(
            "epoch packs are served over the sync protocol only".to_string(),
        ));
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                _ = network_handle.handle.send_response(response, channel) => (),
                // cancel notification from network layer
                _ = cancel => (),
            }
            Ok(())
        });
    }

    /// Answer a legacy request-response `StreamConsensusOutput` request.
    ///
    /// Item 9b (#739) cut the consensus-output fetch fully over to the
    /// `/tn-primary-sync` protocol, so this request-response path is no longer served:
    /// peers fetch a single consensus output over sync. The request variant is retained
    /// for wire compatibility (BCS variant indices stay until the coordinated `/0.0.2`
    /// bump, item 9c); a legacy requester is answered with a typed error rather than
    /// served.
    fn process_consensus_output_stream(
        &self,
        peer: BlsPublicKey,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        let network_handle = self.network_handle.clone();
        let task_name = format!("ConsensusOutputDeprecated-{peer}");
        let response = PrimaryResponse::Error(PrimaryRPCError(
            "consensus output is served over the sync protocol only".to_string(),
        ));
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                _ = network_handle.handle.send_response(response, channel) => (),
                // cancel notification from network layer
                _ = cancel => (),
            }
            Ok(())
        });
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

    /// Process an inbound sync epoch-pack stream.
    ///
    /// Admission against the per-peer concurrency cap happens here on stream open:
    /// the request rides in the opening frame, so there is no `(peer, digest)`
    /// pending map for this path. A shedding responder writes
    /// [`DenyReason::AtCapacity`] without reading so the requester retries elsewhere
    /// immediately. Once admitted, the opening request frame is read (bounded by
    /// [`SYNC_REQUEST_READ_TIMEOUT`]) and an `EpochPack` request is served by
    /// [`RequestHandler::process_sync_epoch_pack_stream`]. The admission permit is
    /// held for the lifetime of the spawned task.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        // admit against the per-peer cap before spawning; the permit (if any) moves
        // into the task and frees capacity on drop
        let permit = try_admit_sync(&self.epoch_stream_semaphore, &self.sync_stream_peers, peer);
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let task_name = format!("sync-epoch-pack-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            let mut stream = stream;
            // bounds the opening `Req` read and the tiny control writes below; the
            // per-request-type data bounds live in each serve function.
            let max_frame = MAX_SYNC_REQUEST_FRAME_SIZE;
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
                        .process_sync_epoch_pack_stream(peer, stream, epoch, None, &consensus_chain)
                        .await?;
                }
                // `EpochPackPartial` over sync (item 9): serve the verifiable prefix of
                // an in-progress epoch, replacing the legacy `StreamEpochPartial`
                // ack-plus-digest path. Same serve as the full pack, bounded to the
                // prefix length by `Some(last_consensus_number)`.
                SyncFrame::Req(PrimarySyncRequest::EpochPackPartial {
                    epoch,
                    last_consensus_number,
                }) => {
                    request_handler
                        .process_sync_epoch_pack_stream(
                            peer,
                            stream,
                            epoch,
                            Some(last_consensus_number),
                            &consensus_chain,
                        )
                        .await?;
                }
                // `MissingCertificates` over sync (item 7): serve the matching
                // certificates as a streamed `Ack` + `Data`* + `End`, replacing the
                // legacy request-response `RequestedCertificates` reply.
                SyncFrame::Req(PrimarySyncRequest::MissingCertificates {
                    exclusive_lower_bound,
                    skip_rounds,
                }) => {
                    request_handler
                        .process_sync_missing_certs_stream(
                            peer,
                            stream,
                            exclusive_lower_bound,
                            skip_rounds,
                        )
                        .await?;
                }
                // `ConsensusOutput` over sync (item 9b): serve one output's raw bytes
                // as a streamed `Ack` + `Data`* + `End`, replacing the legacy
                // `StreamConsensusOutput` ack-plus-digest path.
                SyncFrame::Req(PrimarySyncRequest::ConsensusOutput { number }) => {
                    request_handler
                        .process_sync_consensus_output_stream(peer, stream, number)
                        .await?;
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
