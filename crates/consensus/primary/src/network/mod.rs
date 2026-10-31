//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    proposer::OurDigestMessage,
    state_sync::StateSynchronizer,
    ConsensusBus, ConsensusBusApp,
};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt as _, FutureExt as _, StreamExt as _};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use parking_lot::Mutex;
use rand::{seq::SliceRandom as _, Rng};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{
        IntoResponse as _, NetworkCommand, NetworkEvent, NetworkHandle, NetworkResponseMessage,
        NetworkResult,
    },
    write_frame, AdmissionShed, DenyReason, GossipMessage, PeerSlot, PeerSlots, PeerTier, Penalty,
    PrimarySyncRequest, ReservedPeers, ReservedPool, ResponseChannel, ServiceClass, Stream,
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
    Header, HeaderDigest, Round, TaskError, TaskSpawner, TnReceiver, TnSender, Vote, WorkerId,
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

/// Public share of each inbound sync serve class.
///
/// The primary serves inbound sync requests from one serve pool per class (epoch pack, missing
/// certificates, consensus output; see `SyncServePools`), so a flood of one class cannot
/// starve another. Any peer may take one of these permits. Each pool also holds
/// `COMMITTEE_RESERVED_SYNC_STREAMS` permits per committee key, and only that key of the
/// current or the next committee may take them. A serve permit is held only while its request
/// is served, never during the opening read (see `MAX_CONCURRENT_OPENING_READS`).
pub const MAX_CONCURRENT_EPOCH_STREAMS: usize = 5;

/// Reserve of each committee key in each inbound sync serve class.
///
/// Each serve pool holds this many permits for each key of the current or the next committee,
/// on top of its public share ([`MAX_CONCURRENT_EPOCH_STREAMS`]). Only that key may take them,
/// and no other key can take from its reserve. So peers outside the committee that fill the
/// public share, and committee keys that hold streams without progress, cannot stop a different
/// committee key from catching up (issue #1750). The per-peer cap
/// ([`MAX_PENDING_REQUESTS_PER_PEER`], 2) lets a committee key open one more stream from the
/// public share. See GHSA-9h5w-9282-pc3g.
pub(crate) const COMMITTEE_RESERVED_SYNC_STREAMS: usize = 1;

/// Public share of the opening-read budget for inbound sync streams.
///
/// A permit is held only while the responder waits up to [`SYNC_REQUEST_READ_TIMEOUT`] for the
/// opening request frame of a stream. It is released before the request is served, when that
/// read times out, or when the task ends. In the worst case this many opening reads (plus the
/// reserve of each committee key) are in flight, each bounded by [`MAX_SYNC_REQUEST_FRAME_SIZE`].
pub(crate) const MAX_CONCURRENT_OPENING_READS: usize = 8;

/// Reserve of each committee key in the opening-read budget for inbound sync streams.
///
/// The budget holds this many permits for each key of the current or the next committee, on top
/// of its public share ([`MAX_CONCURRENT_OPENING_READS`]). Only that key may take them, and no
/// other key can take from its reserve. So silent streams from peers outside the committee, and
/// committee keys that hold streams without progress, cannot stop a different committee key from
/// sending its request and catching up (issue #1750). The per-peer cap
/// ([`MAX_PENDING_REQUESTS_PER_PEER`], 2) lets a committee key open one more stream from the
/// public share.
pub(crate) const COMMITTEE_RESERVED_OPENING_READS: usize = 1;

/// Maximum number of concurrent `EpochRecord` request-response serves across all peers.
///
/// The `EpochRecord` request-response arm is reachable by any peer whose identity resolves
/// (not committee membership), and each serve does real work: a database lookup. A record
/// without its certificate is answered as unavailable at once (no wait), so a permit is held only
/// for the lookup and the response. Without a bound a peer could spawn unbounded concurrent,
/// penalty-free serve tasks. A dedicated semaphore (separate from `MAX_CONCURRENT_EPOCH_STREAMS`
/// so an epoch-record flood and the stream-serving paths never starve one another) caps the
/// concurrent serve count. See GHSA-vc2r-9cp2-w74j.
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

/// Maximum number of concurrent shed tasks for denied inbound sync streams.
///
/// A denied stream still gets a short-lived task that writes
/// [`DenyReason::AtCapacity`] so the requester fails fast instead of waiting out
/// a timeout. Without a budget on that spawn, a burst of opened substreams
/// beyond the admission caps fans out to one task each, bounded only by the
/// stream behavior's per-peer inbound rate limit (#1308). Past this budget the
/// stream is dropped without spawning (the requester sees a reset and retries
/// elsewhere). Admitted tasks are bounded by the opening-read budget
/// ([`MAX_CONCURRENT_OPENING_READS`]), the serve pools ([`SyncServePools`]) and the per-peer
/// cap ([`MAX_PENDING_REQUESTS_PER_PEER`]), so the primary's total sync-task fan-out stays
/// bounded by those budgets plus this many shed tasks.
pub(crate) const MAX_CONCURRENT_SHED_TASKS: usize = 8;

/// Longest peer-supplied error text, in characters, that a vote request keeps.
///
/// A voter chooses the text of its error responses, bounded only by the RPC message size, and
/// the requester logs it and hands it to the certifier, which retries until the proposal is
/// superseded. Clipping keeps a Byzantine voter from turning each response into a megabyte-long
/// log line.
const MAX_PEER_ERROR_LEN: usize = 256;

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

/// Deadline for writing and flushing the opening request frame of an outbound sync stream on
/// every primary client path (epoch pack, consensus output, missing certificates). The
/// responder reads that frame under [`SYNC_REQUEST_READ_TIMEOUT`] and drops the stream when it
/// does not arrive in time, so a write still pending after the same window can no longer be
/// served and is cut here instead. Without this bound a peer that completes stream negotiation
/// and then grants no further receive credit holds the request write pending for the life of
/// the connection, the serial probe walk never reaches the next peer, and the state-sync
/// fetcher that drives the walk stalls until restart.
const SYNC_REQUEST_WRITE_TIMEOUT: Duration = SYNC_REQUEST_READ_TIMEOUT;

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
/// finished or aborted request frees capacity again. Used by the `EpochRecord`
/// request-response path ([`try_admit_epoch_record`]); the inbound sync path admits with
/// [`PeerSlots`] and [`ReservedPool`] instead.
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

/// Try to reserve a slot in the bounded shed-task budget.
///
/// Returns `None` when [`MAX_CONCURRENT_SHED_TASKS`] shed tasks are already in
/// flight; the caller then drops the stream without spawning. Dropping the
/// permit frees the slot for the next denied stream.
fn try_admit_shed(semaphore: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
    semaphore.clone().try_acquire_owned().ok()
}

/// Try to admit one `EpochRecord` request-response serve for `peer`.
///
/// Acquires a global permit from the dedicated epoch-record semaphore and admits only while the
/// peer has fewer than [`MAX_PENDING_REQUESTS_PER_PEER`] in-flight epoch-record serves. Returns
/// `None` (shedding the global permit) when either cap is hit, so the caller sheds the request
/// without spawning work. The returned [`PeerSlotPermit`] is held for the serve's lifetime and
/// frees both caps on drop. This path has its own semaphore and counter, separate from the
/// inbound sync budgets, so it neither consults nor contends them.
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

/// The serve class of an inbound sync request. Each class has its own serve pool in
/// [`SyncServePools`], so a flood of one class cannot starve another class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncClass {
    /// A full or a partial epoch pack (`EpochPack` or `EpochPackPartial`).
    EpochPack,
    /// The certificates that a lagging peer is missing (`MissingCertificates`).
    MissingCertificates,
    /// One consensus output (`ConsensusOutput`).
    ConsensusOutput,
}

impl SyncClass {
    /// The serve class of `request`. A partial epoch pack shares the class of the full pack.
    pub(crate) fn of(request: &PrimarySyncRequest) -> Self {
        match request {
            PrimarySyncRequest::EpochPack { .. } | PrimarySyncRequest::EpochPackPartial { .. } => {
                Self::EpochPack
            }
            PrimarySyncRequest::MissingCertificates { .. } => Self::MissingCertificates,
            PrimarySyncRequest::ConsensusOutput { .. } => Self::ConsensusOutput,
        }
    }
}

/// The serve pools of the inbound sync path: one [`ReservedPool`] for each [`SyncClass`].
///
/// Each pool has [`MAX_CONCURRENT_EPOCH_STREAMS`] public permits plus
/// [`COMMITTEE_RESERVED_SYNC_STREAMS`] permits per committee key, which only that key may take.
/// A clone shares the same permits.
#[derive(Clone, Debug)]
pub(crate) struct SyncServePools {
    /// The pool for full and partial epoch-pack serves.
    epoch_pack: ReservedPool,
    /// The pool for missing-certificates serves.
    missing_certificates: ReservedPool,
    /// The pool for consensus-output serves.
    consensus_output: ReservedPool,
}

impl SyncServePools {
    /// Create the three serve pools, each with its public share and a reserve per committee key.
    pub(crate) fn new() -> Self {
        let pool =
            || ReservedPool::new(MAX_CONCURRENT_EPOCH_STREAMS, COMMITTEE_RESERVED_SYNC_STREAMS);
        Self { epoch_pack: pool(), missing_certificates: pool(), consensus_output: pool() }
    }

    /// The pool that serves requests of `class`.
    pub(crate) fn pool(&self, class: SyncClass) -> &ReservedPool {
        match class {
            SyncClass::EpochPack => &self.epoch_pack,
            SyncClass::MissingCertificates => &self.missing_certificates,
            SyncClass::ConsensusOutput => &self.consensus_output,
        }
    }
}

/// How an inbound sync serve ended, as the admission layer sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncServeOutcome {
    /// The serve finished without an error. A `Deny(Unavailable)` answer is a completed serve.
    Completed,
    /// A write to the requester timed out: the requester stopped reading, read below the
    /// throughput floor, or held the epoch-pack serve past its total deadline. Only this outcome
    /// earns a penalty, and only a public-tier peer pays it.
    Stalled,
    /// The serve failed for a different reason (a local error, a reset stream, a malformed
    /// request, or an overall serve timeout). No penalty.
    Failed,
}

impl SyncServeOutcome {
    /// Classify the result of a serve. A [`PrimaryNetworkError::StdIo`] error of kind
    /// [`std::io::ErrorKind::TimedOut`] is [`Self::Stalled`]; a different error is
    /// [`Self::Failed`].
    pub(crate) fn of(served: &PrimaryNetworkResult<()>) -> Self {
        served.as_ref().map_or_else(
            |error| {
                let stalled = matches!(
                    error,
                    PrimaryNetworkError::StdIo(io) if io.kind() == std::io::ErrorKind::TimedOut
                );
                if stalled {
                    Self::Stalled
                } else {
                    Self::Failed
                }
            },
            |&()| Self::Completed,
        )
    }
}

/// The grants that admit one inbound sync stream, taken before its task is spawned.
#[derive(Debug)]
pub(crate) struct SyncAdmission {
    /// The peer that opened the stream.
    peer: BlsPublicKey,
    /// The admission tier of the peer.
    tier: PeerTier,
    /// The per-peer slot, held for the whole life of the task.
    slot: PeerSlot,
    /// The opening-read permit, released before the serve starts, when the opening read times
    /// out, or when the task ends.
    opening: OwnedSemaphorePermit,
}

/// The next step for an admitted sync stream once its opening read ends.
#[derive(Debug)]
enum SyncOpening {
    /// No frame arrived before [`SYNC_REQUEST_READ_TIMEOUT`].
    Silent,
    /// The opening frame could not be read (oversized, undecodable or a reset stream).
    Unreadable,
    /// The opening frame was not `Req`.
    Unexpected,
    /// The serve class of the request has no free permit for the peer's tier.
    AtCapacity(SyncClass),
    /// The request holds a serve permit for its class.
    Admitted {
        /// The request to serve.
        request: PrimarySyncRequest,
        /// The serve class of the request.
        class: SyncClass,
        /// The serve permit, held only while the serve runs.
        permit: OwnedSemaphorePermit,
    },
}

impl SyncOpening {
    /// Classify a readable opening frame and, for a `Req`, try to take a serve permit of its
    /// class for `peer`, a peer of `tier`. A committee-tier `peer` takes a permit from its own
    /// reserve first and then from the public share.
    fn of_frame(
        frame: SyncFrame<PrimarySyncRequest>,
        pools: &SyncServePools,
        peer: &BlsPublicKey,
        tier: PeerTier,
    ) -> Self {
        match frame {
            SyncFrame::Req(request) => {
                let class = SyncClass::of(&request);
                let admitted = |permit| Self::Admitted { request, class, permit };
                pools.pool(class).try_acquire(peer, tier).map_or(Self::AtCapacity(class), admitted)
            }
            SyncFrame::Ack
            | SyncFrame::Deny(_)
            | SyncFrame::Data(_)
            | SyncFrame::End
            | SyncFrame::Err(_) => Self::Unexpected,
        }
    }
}

/// Drive one admitted inbound sync stream: read the opening request, take a serve permit for its
/// class, and serve it.
///
/// The per-peer slot is held for the whole task. The opening-read permit is held while the
/// opening request is read and is released before `serve` runs or when that read times out (a
/// stream that ends with a bounded control frame keeps it until the task ends). The serve permit
/// is held only while `serve` runs. A public-tier peer is charged [`Penalty::Mild`] when its
/// opening read times out or when `serve` reports [`SyncServeOutcome::Stalled`].
pub(crate) async fn admit_and_serve_sync_stream<S, F, Fut>(
    mut stream: S,
    admission: SyncAdmission,
    serve_pools: SyncServePools,
    network_handle: PrimaryNetworkHandle,
    serve: F,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(S, PrimarySyncRequest) -> Fut + Send,
    Fut: Future<Output = SyncServeOutcome> + Send,
{
    let SyncAdmission { peer, tier, slot: _slot, opening } = admission;
    let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
    let step = tokio::time::timeout(
        SYNC_REQUEST_READ_TIMEOUT,
        read_frame::<_, PrimarySyncRequest>(
            &mut stream,
            &mut decode_buffer,
            &mut decompress_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        ),
    )
    .await
    .map_or(SyncOpening::Silent, |read| {
        read.map_or_else(
            |e| {
                warn!(target: "primary::network", %peer, ?e, "unreadable sync request frame");
                SyncOpening::Unreadable
            },
            |frame| SyncOpening::of_frame(frame, &serve_pools, &peer, tier),
        )
    });
    match step {
        SyncOpening::Silent => {
            drop(opening);
            warn!(target: "primary::network", %peer, ?tier, "no sync request before timeout");
            charge_public_peer(&network_handle, peer, tier).await;
            let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
        }
        SyncOpening::Unreadable => {
            let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
        }
        SyncOpening::Unexpected => {
            warn!(target: "primary::network", %peer, "unexpected opening sync frame");
            write_control_frame_and_close(&mut stream, SyncFrame::Err(SyncFrameError::Malformed))
                .await;
        }
        SyncOpening::AtCapacity(class) => {
            debug!(target: "primary::network", %peer, ?class, ?tier, "sync serve class full");
            write_control_frame_and_close(&mut stream, SyncFrame::Deny(DenyReason::AtCapacity))
                .await;
        }
        SyncOpening::Admitted { request, class, permit } => {
            drop(opening);
            let outcome = serve(stream, request).await;
            drop(permit);
            match outcome {
                SyncServeOutcome::Stalled => {
                    warn!(target: "primary::network", %peer, ?class, ?tier, "sync serve stalled");
                    charge_public_peer(&network_handle, peer, tier).await;
                }
                SyncServeOutcome::Completed | SyncServeOutcome::Failed => {}
            }
        }
    }
}

/// Charge a public-tier peer [`Penalty::Mild`] for holding a sync budget without progress. A
/// committee-tier peer is never charged. Fifty mild penalties reach the default ban threshold.
async fn charge_public_peer(handle: &PrimaryNetworkHandle, peer: BlsPublicKey, tier: PeerTier) {
    match tier {
        PeerTier::Public => handle.report_penalty(peer, Penalty::Mild).await,
        PeerTier::Committee => {}
    }
}

/// Best-effort write of one control frame (`Deny` or `Err`), then a close. The write and the
/// close share one [`SYNC_REQUEST_READ_TIMEOUT`] bound, so a peer that never reads cannot pin the
/// task and the budgets that it holds.
async fn write_control_frame_and_close<S: AsyncWrite + Unpin + Send>(
    stream: &mut S,
    frame: SyncFrame<PrimarySyncRequest>,
) {
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
        let _ = write_frame(
            &mut *stream,
            &frame,
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        )
        .await;
        let _ = stream.close().await;
    })
    .await;
}

/// Serve one inbound sync request through the matching request-handler path.
///
/// `EpochPack` and `EpochPackPartial` stream the full pack or its verifiable prefix,
/// `MissingCertificates` streams the matching certificates, and `ConsensusOutput` streams one
/// output's raw bytes, each as `Ack` + `Data`* + `End`.
async fn serve_sync_request<DB: Database>(
    handler: &RequestHandler<DB>,
    peer: BlsPublicKey,
    stream: Stream,
    request: PrimarySyncRequest,
    consensus_chain: &ConsensusChain,
) -> SyncServeOutcome {
    match request {
        PrimarySyncRequest::EpochPack { epoch } => {
            handler.process_sync_epoch_pack_stream(peer, stream, epoch, None, consensus_chain).await
        }
        PrimarySyncRequest::EpochPackPartial { epoch, last_consensus_number } => {
            handler
                .process_sync_epoch_pack_stream(
                    peer,
                    stream,
                    epoch,
                    Some(last_consensus_number),
                    consensus_chain,
                )
                .await
        }
        PrimarySyncRequest::MissingCertificates { exclusive_lower_bound, skip_rounds } => {
            handler
                .process_sync_missing_certs_stream(peer, stream, exclusive_lower_bound, skip_rounds)
                .await
        }
        PrimarySyncRequest::ConsensusOutput { number } => {
            handler.process_sync_consensus_output_stream(peer, stream, number).await
        }
    }
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
    /// Per-peer memory of the last epoch a sync probe to that peer failed or timed out.
    ///
    /// The probe loops shuffle their candidate peers and then move any peer whose recorded epoch
    /// equals the epoch being fetched to the BACK: a peer that failed once for this epoch is
    /// probed after every fresh peer (de-prioritised, not skipped) on the retries for the same
    /// epoch, so a "drip"/slow peer or a Byzantine peer in stable `HashMap` order can no longer
    /// stall the front of every attempt. Stale entries for older epochs are ignored (the compare
    /// is against the current epoch) and the whole map is cleared on rotation via
    /// [`Self::clear_sync_capability`].
    epoch_sync_failed: Arc<Mutex<HashMap<BlsPublicKey, Epoch>>>,
    /// The last peer that served a full pack, a partial pack or a verified consensus output.
    ///
    /// The probe loops try this peer first unless it already failed a probe for the epoch being
    /// fetched, so the header-by-header output walk returns to the peer that just served instead
    /// of reshuffling across the whole peer set for every number. Set only by a pack import that
    /// returned `Ok` or by a hash-verified output, never by a failed or unanswered probe.
    /// [`Self::clear_sync_capability`] deliberately leaves it in place.
    last_sync_server: Arc<Mutex<Option<BlsPublicKey>>>,
}

// Test-only conversion that defaults the chain id to 0. Gated to tests so the only
// way to build a production handle is `new`, with the genesis chain id supplied
// explicitly: a 0 here would publish on the `-0` topic suffix while validators read
// the real id, a silent gossip mismatch.
#[cfg(test)]
impl From<NetworkHandle<Req, Res>> for PrimaryNetworkHandle {
    fn from(handle: NetworkHandle<Req, Res>) -> Self {
        Self {
            handle,
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashMap::new())),
            last_sync_server: Arc::new(Mutex::new(None)),
        }
    }
}

impl PrimaryNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(handle: NetworkHandle<Req, Res>, chain_id: u64) -> Self {
        Self {
            handle,
            chain_id,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashMap::new())),
            last_sync_server: Arc::new(Mutex::new(None)),
        }
    }

    //// Convenience method for creating a new Self for tests.
    pub fn new_for_test(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self {
            handle: NetworkHandle::new(sender),
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashMap::new())),
            last_sync_server: Arc::new(Mutex::new(None)),
        }
    }

    /// Clear the per-peer epoch-pack sync-capability cache.
    ///
    /// Called at each epoch boundary: committees rotate and binaries are upgraded
    /// there, so a peer that did not answer the sync protocol last epoch is
    /// re-probed for it this epoch. The last peer that served a sync request stays
    /// preferred: a peer that served the previous epoch very likely serves this one,
    /// and a wrong guess costs one probe, after which the preference is dropped
    /// (see `forget_sync_server`).
    pub fn clear_sync_capability(&self) {
        self.sync_capability.lock().clear();
        self.epoch_sync_failed.lock().clear();
    }

    /// Record that `peer` served a full pack, a partial pack or a verified consensus output.
    ///
    /// The peer is cached sync-capable, any failure it recorded this epoch is cleared so a peer
    /// that recovered is no longer de-prioritised, and it becomes the peer the next probe walk
    /// tries first. The preference lasts until another peer serves or until `peer` fails a
    /// probe (`forget_sync_server`), so a peer that served once and then stalls does not stay
    /// at the front of every later walk.
    fn record_sync_served(&self, peer: BlsPublicKey) {
        self.sync_capability.lock().insert(peer, true);
        self.epoch_sync_failed.lock().remove(&peer);
        *self.last_sync_server.lock() = Some(peer);
    }

    /// Forget `peer` as the preferred sync server when it is the one on record. Every probe of
    /// `peer` that does not serve calls this, so a peer that served once and then stalls or fails
    /// does not stay at the front of every later walk.
    fn forget_sync_server(&self, peer: BlsPublicKey) {
        let mut preferred = self.last_sync_server.lock();
        *preferred = (*preferred).filter(|recorded| *recorded != peer);
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
            debug!(
                target: "primary::network",
                %peer,
                error = %clip_peer_error(s),
                "recoverable vote error, retrying"
            );
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
            // still recoverable after the retries above: report it as retryable so the caller
            // backs off and asks again rather than giving up on this peer for the header
            PrimaryResponse::RecoverableError(PrimaryRPCError(s)) => {
                Err(NetworkError::RPCRetryable(clip_peer_error(s)))
            }
            PrimaryResponse::Error(PrimaryRPCError(s)) => {
                Err(NetworkError::RPCError(clip_peer_error(s)))
            }
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::EpochRecord { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is epoch record!".to_string(),
            )),
            PrimaryResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is peer exchange!".to_string(),
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

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT` so a
        // peer that grants no receive credit cannot hold this fetch pending for the life of
        // the connection (a cut write is a failed peer and the fan-out moves on)
        let request_frame = SyncFrame::Req(PrimarySyncRequest::MissingCertificates {
            exclusive_lower_bound: request.exclusive_lower_bound,
            skip_rounds: request.skip_rounds.clone(),
        });
        write_sync_request(&mut stream, &request_frame, MAX_SYNC_REQUEST_FRAME_SIZE)
            .await
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
    /// a verified [`ConsensusOutput`]. Probes every connected peer not cached unsyncable, one at a
    /// time, and returns the first VERIFIED output, otherwise `Err` (the caller retries).
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
        // Shuffle, put the last peer that served first, and de-prioritise peers that already
        // failed a probe for this epoch (same ordering as the full-pack path). A preferred peer
        // that does not serve a probe loses the preference (`forget_sync_server`), and every
        // probe's opening write is bounded by `SYNC_REQUEST_WRITE_TIMEOUT`, so a peer that
        // served once and then stalls costs one bounded probe, not the walk. The preference is
        // read in its own statement so its lock guard never overlaps the failure map's.
        let preferred = *self.last_sync_server.lock();
        let peers = order_probe_peers(
            peers,
            &self.epoch_sync_failed.lock(),
            epoch,
            preferred,
            &mut rand::rng(),
        );

        // Probe candidate peers one at a time (the async analog of a short-circuiting
        // fold): `filter` drops peers cached unsyncable for free, `then` runs each exchange
        // and records its verdict, and `filter_map` + `next` stop at the first peer that
        // serves a VERIFIED output (so no peer past the first success is probed, and a
        // wrong-hash peer is skipped).
        let probes = futures::stream::iter(peers)
            .filter(move |peer| {
                let known_unsyncable = self.sync_capability.lock().get(peer) == Some(&false);
                futures::future::ready(!known_unsyncable)
            })
            .then(move |peer| async move {
                match self.sync_consensus_output_from_peer(peer, request).await {
                    ConsensusOutputAttempt::Fetched(output) => {
                        self.record_sync_served(peer);
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
                        // and hide it from the full-pack path. It did not serve, so it
                        // loses the probe-first preference if it held it.
                        self.forget_sync_server(peer);
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
                        // sync-capable, drop its probe-first preference, and try the
                        // next peer
                        self.sync_capability.lock().insert(peer, true);
                        self.epoch_sync_failed.lock().insert(peer, epoch);
                        self.forget_sync_server(peer);
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

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT`.
        // Negotiation already succeeded, so a write failure or a write still pending at the
        // deadline (a peer that grants no receive credit) is transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        let req_frame =
            SyncFrame::Req(PrimarySyncRequest::ConsensusOutput { number: request.number });
        write_sync_request(&mut stream, &req_frame, max_frame).await.map_err(|e| {
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
                    .stream_decode_consensus_output(
                        request.epoch,
                        reader,
                        request.expected_hash,
                        sync_codec::SYNC_RECORD_TIMEOUT,
                    )
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

    /// Attempt to fetch and import a full or partial epoch pack over the typed sync protocol.
    ///
    /// Probes every connected peer not cached unsyncable this epoch, one at a time,
    /// opening a `/tn-primary-sync` stream whose opening frame carries the request.
    /// Returns `Ok(())` as soon as one peer serves a pack that imports; otherwise `Err`
    /// (there is no legacy fallback, so the caller retries). A peer that does not answer
    /// a full-pack request is cached unsyncable (skipping its probe next time); a
    /// partial-pack `Unsupported` is ambiguous and is not cached. A probe that never
    /// reaches `Ack` is penalty-exempt; an import fault after `Ack` that is attributable
    /// to the peer's bytes is penalised. Every phase of a probe is bounded (stream open,
    /// the opening request write under `SYNC_REQUEST_WRITE_TIMEOUT`, the `Ack` read, and
    /// the reader's per-frame timeout plus throughput floor after it), and the last peer
    /// that served is probed first only until it fails a probe, so one stalled peer costs
    /// one bounded probe per walk rather than the walk.
    async fn request_epoch_pack_sync(&self, sync: EpochPackSyncRequest<'_>) -> NetworkResult<()> {
        let EpochPackSyncRequest { epoch, last_consensus_number, .. } = sync;
        let peers = self.handle.connected_peers().await?;
        // Shuffle, put the last peer that served first, and de-prioritise peers that already
        // failed a probe for this epoch: a peer that failed once for this epoch is probed after
        // every fresh peer, so a Byzantine peer fixed in stable `HashMap` order cannot stall the
        // front of every one of state-sync's retries for the same epoch (see
        // `order_probe_peers`). A preferred peer that does not serve a probe loses the
        // preference (`forget_sync_server`), and every probe's opening write is bounded by
        // `SYNC_REQUEST_WRITE_TIMEOUT`. The preference is read in its own statement so its
        // lock guard never overlaps the failure map's.
        let preferred = *self.last_sync_server.lock();
        let peers = order_probe_peers(
            peers,
            &self.epoch_sync_failed.lock(),
            epoch,
            preferred,
            &mut rand::rng(),
        );

        // Probe candidate peers one at a time (the async analog of a short-circuiting
        // fold): `filter` drops peers cached unsyncable this epoch for free, `then` runs
        // each exchange and records its verdict in the capability cache, and `any` stops
        // at the first peer whose pack imports (so no peer past the first success is
        // probed).
        let imported = futures::stream::iter(peers)
            .filter(move |peer| {
                let known_unsyncable = self.sync_capability.lock().get(peer) == Some(&false);
                futures::future::ready(!known_unsyncable)
            })
            .then(move |peer| async move {
                match self.sync_epoch_pack_from_peer(peer, sync).await {
                    EpochPackAttempt::Imported => {
                        self.record_sync_served(peer);
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
                        // it did not serve, so it loses the probe-first preference if it held it
                        self.forget_sync_server(peer);
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
                        // sync-capable, drop its probe-first preference, and try the
                        // next peer
                        self.sync_capability.lock().insert(peer, true);
                        // Remember it failed THIS epoch so the retries for the same epoch probe it
                        // last (a drip/slow peer no longer starves every attempt).
                        self.epoch_sync_failed.lock().insert(peer, epoch);
                        self.forget_sync_server(peer);
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

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT`.
        // Negotiation already succeeded, so the peer is sync-capable; a write failure or a
        // write still pending at the deadline (a peer that grants no receive credit) is
        // transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        // full pack vs a verifiable partial prefix: select the request variant through
        // the `Option`'s combinator (no Some/None branch).
        let request = SyncFrame::Req(
            last_consensus_number.map_or(PrimarySyncRequest::EpochPack { epoch }, |number| {
                PrimarySyncRequest::EpochPackPartial { epoch, last_consensus_number: number }
            }),
        );
        write_sync_request(&mut stream, &request, max_frame).await.map_err(|e| {
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
                // The reader's own per-frame timeout and throughput floor cut a stalled or
                // dribbling peer; the per-record timeout must not cut a transfer the floor admits.
                let record_timeout = record_timeout.max(sync_codec::SYNC_RECORD_TIMEOUT);
                // Pick the import target on `last_consensus_number.is_some()`; both resolve to
                // `Result<(), ConsensusChainError>`, so box to unify the future types while keeping
                // the TYPED error (a peer-caused stream fault must be penalised before it is
                // flattened into `Failed`). A partial prefix imports to a side
                // staging dir so it cannot race the in-order build of the current
                // epoch's main pack; a full pack imports in place.
                let import: futures::future::BoxFuture<'_, Result<(), ConsensusChainError>> =
                    if last_consensus_number.is_some() {
                        consensus_chain
                            .import_partial_to_staging(
                                reader,
                                epoch_record,
                                previous_epoch,
                                record_timeout,
                            )
                            .boxed()
                    } else {
                        consensus_chain
                            .stream_import(reader, epoch_record, previous_epoch, record_timeout)
                            .boxed()
                    };
                // No fixed overall wall-clock cap: the reader (`sync_pack_reader`) enforces a
                // throughput floor over a rolling window plus a per-frame timeout, so a large
                // honest pack finishes at any sustained rate at/above the floor
                // while a drip/stalled peer is cut quickly. On such an error the
                // future resolves `Err`, which unwinds `stream_import` so its
                // `ImportPath` guard removes the import dir, releases the epoch's claim, and the
                // epoch is retryable against another peer.
                match import.await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Charge the peer only for a fault attributable SOLELY to its streamed
                        // bytes (never our local storage/IO).
                        // Committee peers stay score-exempt for liveness. Operator trust alone
                        // does not exempt these protocol penalties. Still classified `Failed`
                        // so the probe moves to the next peer, regardless of scoring policy.
                        if Self::import_fault_is_peer_caused(&e) {
                            if let Some(penalty) = Self::consensus_chain_error_to_penalty(&e) {
                                self.report_penalty(peer, penalty).await;
                            }
                        }
                        Err(EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                            "failed to import epoch pack over sync stream: {e}"
                        ))))
                    }
                }
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
                | PackError::NotEpoch
                // A record out of place or not what the header declares, or one that frames but
                // fails its CRC/decode: the sender's bytes are bad. Medium, not Severe: an honest
                // peer serving a pack damaged at rest on its own disk produces the same bytes.
                | PackError::UnexpectedRecord(_)
                | PackError::UndecodableRecord(_) => Some(Penalty::Medium),
                PackError::InvalidConsensusChain
                | PackError::ExtraBatches
                | PackError::MissingBatches
                | PackError::TooManyBatches(_)
                | PackError::CorruptPack(_)
                | PackError::UnexpectedConsensusDigest { .. }
                | PackError::EmptySubDag
                // An oversized batch is peer misbehavior (the batch validator caps legitimate
                // batches at `max_batch_size`); charge Severe.
                | PackError::BatchTooLarge { .. }
                // A crafted output whose buffered batches exceed the per-output decoded-memory budget
                // is peer misbehavior (a memory-exhaustion attempt); charge Severe.
                | PackError::OutputTooLarge { .. }
                // A non-advancing / gapped / over-`final` consensus number is peer misbehavior (a peer
                // trying to wedge a fetch/import with a non-advancing chain); charge Severe.
                | PackError::InvalidConsensusNumber(_, _)
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
                // The caller's committee or previous record is for another epoch: local state.
                | PackError::LocalEpochMismatch { .. }
                | PackError::SendFailed
                | PackError::ReceiveFailed
                | PackError::PersistError(_)
                | PackError::ConsensusNumberAlreadyAdded
                | PackError::ConflictingOutput { .. }
                | PackError::ConsensusNumberTooLow
                | PackError::InvalidVersion(_, _)
                | PackError::ConsensusNumberTooHigh => None,
            },
            ConsensusChainError::EpochMismatch
            | ConsensusChainError::PrevCommitteeEpochMismatch
            | ConsensusChainError::CrcError => Some(Penalty::Mild),
            ConsensusChainError::StreamUnavailable
            | ConsensusChainError::NoCurrentEpoch
            | ConsensusChainError::EpochDbError(_)
            | ConsensusChainError::InvalidPackEpoch(_, _)
            | ConsensusChainError::CantSaveAndNotAvailable(_)
            | ConsensusChainError::LiveEpochImport(_)
            // The documented cause is LOCAL state (a resume that collapsed this node's
            // "latest"), not peer misbehavior, so no penalty is charged to the peer.
            | ConsensusChainError::NonMonotonicConsensusNumber { .. }
            | ConsensusChainError::IO(_) => None,
        }
    }

    /// Whether an epoch-pack IMPORT failure can be attributed SOLELY to the peer's streamed bytes,
    /// so it is safe to charge the peer a penalty (via
    /// [`Self::consensus_chain_error_to_penalty`]).
    ///
    /// The import builds a fresh pack from a single peer's stream, so a structural/framing fault in
    /// that stream is unambiguously the peer's fault. But the same error types are ALSO produced by
    /// local storage/IO during the import (a full disk, a failed mmap, an index write), and those
    /// must never ban an honest peer. This whitelist admits only the peer-stream-caused
    /// variants and excludes every local/ambiguous one — notably `CorruptPack` (also produced
    /// by local recovery), all IO/Append/Persist/Open/EpochDb variants, and the
    /// local-chain-state mismatches. Exhaustive so a new error variant forces a deliberate
    /// classification here.
    fn import_fault_is_peer_caused(error: &ConsensusChainError) -> bool {
        match error {
            ConsensusChainError::PackError(pack_error) => match pack_error {
                // The peer's streamed pack is structurally malformed / mis-framed / over a resource
                // bound: solely attributable to the bytes it sent.
                PackError::MissingBatch
                | PackError::NotConsensus
                | PackError::NotBatch
                | PackError::NotEpoch
                | PackError::UnexpectedRecord(_)
                | PackError::UndecodableRecord(_)
                | PackError::InvalidConsensusChain
                | PackError::ExtraBatches
                | PackError::MissingBatches
                | PackError::TooManyBatches(_)
                | PackError::UnexpectedConsensusDigest { .. }
                | PackError::EmptySubDag
                | PackError::BatchTooLarge { .. }
                | PackError::OutputTooLarge { .. }
                | PackError::InvalidConsensusNumber(_, _)
                | PackError::InvalidEpoch(_, _) => true,
                // Local storage / IO / decode-of-our-own-state, or ambiguous: never charge the
                // peer. `CorruptPack` is excluded deliberately — it is also
                // produced by local recovery, so on the import path it is treated
                // as possibly-local.
                PackError::CorruptPack(_)
                | PackError::IO(_)
                | PackError::BatchLoad(_)
                | PackError::EpochLoad(_)
                | PackError::Append(_)
                | PackError::IndexAppend(_)
                | PackError::Fetch(_)
                | PackError::Open(_)
                | PackError::ReadOnly
                | PackError::ReadError(_)
                | PackError::MissingAuthority
                // The caller's committee or previous record is for another epoch: local state.
                | PackError::LocalEpochMismatch { .. }
                | PackError::SendFailed
                | PackError::ReceiveFailed
                | PackError::PersistError(_)
                | PackError::ConsensusNumberAlreadyAdded
                | PackError::ConflictingOutput { .. }
                | PackError::ConsensusNumberTooLow
                | PackError::InvalidVersion(_, _)
                | PackError::ConsensusNumberTooHigh => false,
            },
            // Local chain state, our own storage, or a benign mismatch: never charge the peer.
            ConsensusChainError::EpochMismatch
            | ConsensusChainError::PrevCommitteeEpochMismatch
            | ConsensusChainError::CrcError
            | ConsensusChainError::StreamUnavailable
            | ConsensusChainError::NoCurrentEpoch
            | ConsensusChainError::EpochDbError(_)
            | ConsensusChainError::InvalidPackEpoch(_, _)
            | ConsensusChainError::CantSaveAndNotAvailable(_)
            | ConsensusChainError::LiveEpochImport(_)
            | ConsensusChainError::NonMonotonicConsensusNumber { .. }
            | ConsensusChainError::IO(_) => false,
        }
    }
}

/// Why the opening request frame of an outbound sync stream was not delivered.
#[derive(Debug)]
enum SyncRequestWriteError {
    /// The write or the flush did not finish within [`SYNC_REQUEST_WRITE_TIMEOUT`].
    TimedOut,
    /// The write or the flush failed.
    Io(std::io::Error),
}

impl std::fmt::Display for SyncRequestWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out after {SYNC_REQUEST_WRITE_TIMEOUT:?}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

/// Write and flush `frame` as the opening request of an outbound sync stream, bounded by
/// [`SYNC_REQUEST_WRITE_TIMEOUT`]. A timeout drops the in-flight write; the caller drops the
/// stream, which resets it, and treats the peer as a failed probe.
async fn write_sync_request<S, R>(
    stream: &mut S,
    frame: &SyncFrame<R>,
    max_frame: usize,
) -> Result<(), SyncRequestWriteError>
where
    S: AsyncWrite + Unpin + Send,
    R: serde::Serialize + Sync,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    tokio::time::timeout(SYNC_REQUEST_WRITE_TIMEOUT, async {
        write_frame(stream, frame, &mut encode_buffer, &mut compressed_buffer, max_frame).await?;
        stream.flush().await
    })
    .await
    .map_err(|_elapsed| SyncRequestWriteError::TimedOut)
    .and_then(|written| written.map_err(SyncRequestWriteError::Io))
}

/// Order the candidate peers for a sync probe: shuffle for an unbiased base order, then stably
/// sort them into four groups, each keeping its shuffled order: the `preferred` peer (the last one
/// that served) if it is fresh, the other fresh peers, the `preferred` peer if it already
/// failed/timed-out a probe for THIS `epoch`, and the other peers that failed this epoch. A
/// `preferred` peer that is not in `peers` changes nothing. Pure so it is unit-testable with a
/// seeded rng.
///
/// The shuffle defeats the stable-`HashMap`-order starvation (a Byzantine peer fixed at the front
/// of every walk), and the de-prioritisation keeps a slow/drip peer that failed one call from being
/// probed first again on the retries for the same epoch — without dropping it (a small network
/// whose only peer had a transient failure is still retried, just last). A stale `preferred` peer
/// costs at most one probe per walk until another peer serves and takes the preference; once a
/// probe to it fails it also sorts behind every fresh peer for the rest of the epoch.
fn order_probe_peers(
    mut peers: Vec<BlsPublicKey>,
    failed_this_epoch: &HashMap<BlsPublicKey, Epoch>,
    epoch: Epoch,
    preferred: Option<BlsPublicKey>,
    rng: &mut impl Rng,
) -> Vec<BlsPublicKey> {
    peers.shuffle(rng);
    // `sort_by_key` is stable and `false < true`, so the groups come out as fresh preferred,
    // fresh, failed preferred, failed, each keeping its shuffled order.
    peers.sort_by_key(|p| (failed_this_epoch.get(p) == Some(&epoch), Some(*p) != preferred));
    peers
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
    /// Opening-read budget for inbound sync streams: [`MAX_CONCURRENT_OPENING_READS`] public
    /// permits plus [`COMMITTEE_RESERVED_OPENING_READS`] permits per committee key, which only
    /// that key may take. A permit is held only while the opening request frame is read (bounded
    /// by [`SYNC_REQUEST_READ_TIMEOUT`]).
    sync_opening: ReservedPool,
    /// Serve pools for inbound sync requests, one per [`SyncClass`]. A permit is held only while
    /// its request is served.
    sync_serve: SyncServePools,
    /// Per-peer cap on in-flight inbound sync streams across every phase, at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`]. A slot is held for the whole life of the stream task.
    sync_peer_slots: PeerSlots,
    /// BLS keys of the current and the next committee, captured when the network is built for
    /// the epoch. A peer in this set may take the committee reserve of every sync budget.
    reserved_peers: ReservedPeers,
    /// Semaphore bounding concurrent shed tasks for denied sync streams.
    ///
    /// A permit is held for the lifetime of a shed task (the bounded
    /// `Deny(AtCapacity)` write), so [`MAX_CONCURRENT_SHED_TASKS`] caps the
    /// spawn fan-out from over-cap substream bursts.
    shed_task_semaphore: Arc<Semaphore>,
    /// Semaphore bounding concurrent `EpochRecord` request-response serves.
    ///
    /// Separate from the inbound sync budgets (`sync_opening`, `sync_serve`) so an epoch-record
    /// flood and the stream-serving paths never starve one another. See
    /// [`MAX_CONCURRENT_EPOCH_RECORD_REQUESTS`].
    epoch_record_semaphore: Arc<Semaphore>,
    /// Per-peer count of in-flight `EpochRecord` request-response serves, capped at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`].
    epoch_record_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// Counts the `EpochRecord` requests dropped at the admission cap, in the primary network's
    /// `inbound_requests_shed_total{class="epoch_record", reason="admission"}` series.
    epoch_record_admission_shed: AdmissionShed,
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
        // capture the committee tier before `consensus_config` moves into the request handler:
        // the current and the next committee may take the reserve of every sync budget
        let committee_keys = consensus_config.committee().bls_keys();
        let next_keys = consensus_config.next_committee_keys().iter().copied();
        let reserved_peers = ReservedPeers::new(committee_keys.into_iter().chain(next_keys));
        let request_handler = RequestHandler::new(
            consensus_config,
            consensus_bus,
            state_sync.clone(),
            consensus_chain.clone(),
        );
        Self {
            network_events,
            network_handle,
            request_handler,
            task_spawner,
            consensus_chain,
            sync_opening: ReservedPool::new(
                MAX_CONCURRENT_OPENING_READS,
                COMMITTEE_RESERVED_OPENING_READS,
            ),
            sync_serve: SyncServePools::new(),
            sync_peer_slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
            reserved_peers,
            shed_task_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS)),
            epoch_record_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_EPOCH_RECORD_REQUESTS)),
            epoch_record_peers: Arc::new(Mutex::new(HashMap::default())),
            epoch_record_admission_shed: AdmissionShed::epoch_record(),
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
                PrimaryRequest::PeerExchange { .. } => {
                    warn!(target: "primary::network", "primary application received unexpected peer exchange message");
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
            },
            NetworkEvent::Gossip(gossip) => {
                self.process_gossip(gossip.message, gossip.relayer, gossip.author);
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
        // Serve budget: offer the request to the vote class before any work for it. `None` means
        // the class has no free permit; the shed is already counted under `serve_budget`.
        // Dropping the response channel and the cancel receiver sends no reply, so the swarm
        // also counts the request as an `omitted` failure, exactly as an admission shed does.
        // The default budget is unbounded, so this only observes until an operator sets
        // `serve_budget.vote`. An enforcing vote class also caps each peer at
        // `MAX_PERMITS_PER_PEER`, so one committee peer cannot hold the whole pool while its
        // requests wait on its author lock in `RequestHandler::vote`.
        if let Some(serve_permit) =
            self.network_handle.handle.serve_permits().try_acquire(ServiceClass::Vote, peer)
        {
            // clone for spawned tasks
            let request_handler = self.request_handler.clone();
            let network_handle = self.network_handle.clone();
            let task_name = format!("VoteRequest-{}", header.digest());

            self.task_spawner.spawn_task(task_name, async move {
                // hold the serve permit for the task's lifetime, so every exit releases it
                let _serve_permit = serve_permit;
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
        // committee membership) and each serve does real work (a database read) while returning
        // no penalty for an unavailable epoch. Without a bound a peer could spawn unbounded,
        // penalty-free serve tasks. The permit is held for the serve's lifetime. A record without
        // its certificate is answered as unavailable at once (no wait), so that lifetime is only
        // the lookup and the response. See GHSA-vc2r-9cp2-w74j.
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
            self.epoch_record_admission_shed.record();
            return;
        };

        // Serve budget: after the admission cap, offer the request to the epoch-record class.
        // `None` sheds it the same way as the admission cap does; the shed is already counted
        // under `serve_budget`, and the admission permit releases on return. The default budget
        // is unbounded, so this only observes until an operator sets `serve_budget.epoch_record`.
        if let Some(serve_permit) =
            self.network_handle.handle.serve_permits().try_acquire(ServiceClass::EpochRecord, peer)
        {
            // clone for spawned tasks
            let request_handler = self.request_handler.clone();
            let network_handle = self.network_handle.clone();
            let task_name = format!("ConsensusOutputReq-{peer}");
            self.task_spawner.spawn_task(task_name, async move {
                // hold the admission permit for the serve's lifetime; dropping it on completion
                // (or cancel) frees the global slot and decrements the per-peer count
                let _permit = permit;
                // hold the serve permit for the task's lifetime, so every exit releases it
                let _serve_permit = serve_permit;
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

    /// Process an inbound sync stream.
    ///
    /// Admission runs on stream open, ahead of any task spawn (#1308). The stream takes a slot
    /// under the per-peer cap ([`MAX_PENDING_REQUESTS_PER_PEER`]) and a permit from the
    /// opening-read budget ([`MAX_CONCURRENT_OPENING_READS`], plus
    /// [`COMMITTEE_RESERVED_OPENING_READS`] per committee key). A stream that misses
    /// either takes the bounded shed path ([`Self::shed_inbound_sync_stream`]), which writes
    /// [`DenyReason::AtCapacity`] without reading so the requester retries elsewhere at once.
    ///
    /// An admitted stream runs [`admit_and_serve_sync_stream`] on its own task. The task reads
    /// the opening request frame (bounded by [`SYNC_REQUEST_READ_TIMEOUT`]), releases the
    /// opening-read permit, and takes a permit from the serve pool of the request's class
    /// ([`SyncServePools`]) before [`RequestHandler`] serves it. The per-peer slot is held for the
    /// life of the task and the serve permit only while the serve runs, so a silent peer holds no
    /// serve capacity. Each key of the current or the next committee may take its own reserve
    /// in each budget, and no other key can take from it. So neither peers outside the committee
    /// nor other committee keys can stop a lagging validator from syncing. See
    /// GHSA-9h5w-9282-pc3g and issue #1750.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        let tier = self.reserved_peers.tier(&peer);
        // take both admission grants before spawning; a stream that misses either takes the
        // bounded shed path instead, so over-cap substream bursts cannot fan out past the shed
        // budget (#1308)
        let Some((slot, opening)) =
            self.sync_peer_slots.try_take(peer).zip(self.sync_opening.try_acquire(&peer, tier))
        else {
            self.shed_inbound_sync_stream(peer, stream);
            return;
        };
        let admission = SyncAdmission { peer, tier, slot, opening };
        let serve_pools = self.sync_serve.clone();
        let handle = self.network_handle.clone();
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let task_name = format!("sync-epoch-pack-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            let serve = move |stream, request| async move {
                serve_sync_request(&request_handler, peer, stream, request, &consensus_chain).await
            };
            admit_and_serve_sync_stream(stream, admission, serve_pools, handle, serve).await;
            Ok(())
        });
    }

    /// Shed a denied inbound sync stream on a budgeted task.
    ///
    /// Spawns a short-lived task that writes [`DenyReason::AtCapacity`] and
    /// closes, so the requester fails fast and tries elsewhere. The spawn is
    /// gated by [`MAX_CONCURRENT_SHED_TASKS`]: past that budget the stream is
    /// dropped without spawning (the requester sees a reset), so a burst of
    /// over-cap substreams cannot fan out to unbounded tasks (#1308).
    fn shed_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        try_admit_shed(&self.shed_task_semaphore).map_or_else(
            || {
                debug!(target: "primary::network", %peer, "dropping inbound sync stream: shed budget exhausted");
            },
            |shed_permit| {
                let task_name = format!("shed-sync-epoch-pack-{peer}");
                self.task_spawner.spawn_task(task_name, async move {
                    // hold the shed budget slot for the lifetime of the task
                    let _shed_permit = shed_permit;
                    let mut stream = stream;
                    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
                    debug!(target: "primary::network", %peer, "denying inbound sync stream: at capacity");
                    // bound the best-effort shed write: a peer that applies receive
                    // backpressure and never reads must not pin the shed budget slot.
                    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                        let _ = write_frame(
                            &mut stream,
                            &SyncFrame::<PrimarySyncRequest>::Deny(DenyReason::AtCapacity),
                            &mut encode_buffer,
                            &mut compressed_buffer,
                            MAX_SYNC_REQUEST_FRAME_SIZE,
                        )
                        .await;
                        let _ = stream.close().await;
                    })
                    .await;
                    Ok(())
                });
            },
        )
    }
}

/// Defines how the network receiver handles incoming workers messages.
///
/// One instance is bound to each worker's `LocalNetwork` with that worker's id: the binding is
/// what establishes the reporting worker's identity. A message whose stamped id disagrees with
/// the instance it arrived on is rejected before it reaches the proposer's digest channel or
/// the payload store, so a mis-wired (or, once workers are remote, misbehaving) worker cannot
/// impersonate another worker's lane.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    /// The worker id this instance is bound to.
    worker_id: WorkerId,
    consensus_bus: ConsensusBus,
    payload_store: DB,
}

impl<DB: PayloadStore> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self bound to `worker_id`.
    pub(crate) fn new(worker_id: WorkerId, consensus_bus: ConsensusBus, payload_store: DB) -> Self {
        Self { worker_id, consensus_bus, payload_store }
    }

    /// Reject a message stamped with a different worker id than this instance is bound to.
    fn check_worker_id(&self, message_worker_id: WorkerId) -> eyre::Result<()> {
        if message_worker_id == self.worker_id {
            Ok(())
        } else {
            Err(eyre::eyre!(
                "message stamped with worker id {message_worker_id} arrived on the local network \
                 instance bound to worker id {}",
                self.worker_id
            ))
        }
    }
}

#[async_trait::async_trait]
impl<DB: Database> WorkerToPrimaryClient for WorkerReceiverHandler<DB> {
    async fn report_own_batch(&self, message: WorkerOwnBatchMessage) -> eyre::Result<()> {
        self.check_worker_id(message.worker_id())?;
        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest(),
                worker_id: message.worker_id(),
                ack_channel: tx_ack,
            })
            .await?;

        // If we are ok, then wait for the ack
        rx_ack.await?;

        Ok(response)
    }

    async fn report_others_batch(&self, message: WorkerOthersBatchMessage) -> eyre::Result<()> {
        self.check_worker_id(message.worker_id())?;
        self.payload_store.write_payload(&message.digest(), &message.worker_id())?;
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

/// Clip peer-supplied error text to its first [`MAX_PEER_ERROR_LEN`] characters.
///
/// Cuts on a character boundary, so multi-byte text never splits mid-character.
fn clip_peer_error(mut error: String) -> String {
    if let Some((end, _)) = error.char_indices().nth(MAX_PEER_ERROR_LEN) {
        error.truncate(end);
    }
    error
}

#[cfg(test)]
mod clip_peer_error_tests {
    use super::{clip_peer_error, MAX_PEER_ERROR_LEN};

    /// Text at or under the limit is kept whole; longer text keeps exactly the first
    /// [`MAX_PEER_ERROR_LEN`] characters, cut on a character boundary when they are multi-byte.
    #[test]
    fn clips_to_the_limit_on_a_char_boundary() {
        let short = "x".repeat(MAX_PEER_ERROR_LEN);
        assert_eq!(clip_peer_error(short.clone()), short);

        let long = "x".repeat(1 << 20);
        assert_eq!(clip_peer_error(long), "x".repeat(MAX_PEER_ERROR_LEN));

        // three bytes per char, so a byte-count cut would land mid-character
        let wide = "€".repeat(MAX_PEER_ERROR_LEN + 1);
        let clipped = clip_peer_error(wide);
        assert_eq!(clipped.chars().count(), MAX_PEER_ERROR_LEN);
        assert_eq!(clipped, "€".repeat(MAX_PEER_ERROR_LEN));
    }
}

/// Tests of the inbound sync admission budgets: the per-peer cap, the opening-read budget, and
/// the per-class serve pools with their committee reserves (GHSA-9h5w-9282-pc3g).
#[cfg(test)]
mod sync_admission_tests {
    use super::{
        admit_and_serve_sync_stream, PrimaryNetworkHandle, Req, Res, SyncAdmission, SyncClass,
        SyncServeOutcome, SyncServePools, COMMITTEE_RESERVED_OPENING_READS,
        COMMITTEE_RESERVED_SYNC_STREAMS, MAX_CONCURRENT_EPOCH_STREAMS,
        MAX_CONCURRENT_OPENING_READS, MAX_PENDING_REQUESTS_PER_PEER,
    };
    use futures::{AsyncRead, AsyncWrite, StreamExt as _};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::{
        io,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::Duration,
    };
    use tn_network_libp2p::{
        types::NetworkCommand, PeerSlots, PeerTier, Penalty, PrimarySyncRequest, ReservedPeers,
        ReservedPool,
    };
    use tn_types::{BlsKeypair, BlsPublicKey};
    use tokio::{sync::mpsc, task::JoinHandle};

    /// Every serve class.
    const CLASSES: [SyncClass; 3] =
        [SyncClass::EpochPack, SyncClass::MissingCertificates, SyncClass::ConsensusOutput];

    /// A deterministic BLS public key for `seed`.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// True when the serve pool of `class` still has its whole public share.
    fn public_share_whole(pools: &SyncServePools, class: SyncClass) -> bool {
        pools.pool(class).available_public() == MAX_CONCURRENT_EPOCH_STREAMS
    }

    /// True when every serve pool still has all of its public permits and all of the reserved
    /// permits of `committee`.
    fn serve_pools_untouched(pools: &SyncServePools, committee: &BlsPublicKey) -> bool {
        let whole = |class: SyncClass| {
            let pool = pools.pool(class);
            pool.available_public() == MAX_CONCURRENT_EPOCH_STREAMS
                && pool.available_reserved(committee) == COMMITTEE_RESERVED_SYNC_STREAMS
        };
        CLASSES.into_iter().all(whole)
    }

    /// The opening-read budget at its production sizes.
    fn opening_pool() -> ReservedPool {
        ReservedPool::new(MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS)
    }

    /// Let the spawned tasks run until each one waits on the paused clock or finishes.
    async fn settle() {
        futures::stream::iter(0..16).for_each(|_| tokio::task::yield_now()).await;
    }

    /// The peer of a mild penalty report, or `None` for a different command.
    fn mild_penalty_peer(command: NetworkCommand<Req, Res>) -> Option<BlsPublicKey> {
        if let NetworkCommand::ReportPenalty { peer, penalty: Penalty::Mild } = command {
            Some(peer)
        } else {
            None
        }
    }

    /// A requester that opens a sync stream and never sends its request frame. Reads stay
    /// pending; writes and the close succeed at once.
    #[derive(Debug)]
    struct SilentStream;

    impl AsyncRead for SilentStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for SilentStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The inbound sync admission state of one responder, wired the way the primary network
    /// wires it.
    struct Responder {
        /// The committee tier.
        reserved: ReservedPeers,
        /// The per-peer cap.
        slots: PeerSlots,
        /// The opening-read budget.
        opening: ReservedPool,
        /// The per-class serve pools.
        pools: SyncServePools,
        /// The handle that receives penalty reports.
        handle: PrimaryNetworkHandle,
        /// Set when any stream reaches its serve.
        served: Arc<AtomicBool>,
    }

    impl Responder {
        /// A responder whose committee tier is `committee`, and the receiver of its commands.
        fn new(committee: BlsPublicKey) -> (Self, mpsc::Receiver<NetworkCommand<Req, Res>>) {
            let (sender, commands) = mpsc::channel(64);
            let responder = Self {
                reserved: ReservedPeers::new([committee]),
                slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
                opening: opening_pool(),
                pools: SyncServePools::new(),
                handle: PrimaryNetworkHandle::new_for_test(sender),
                served: Arc::new(AtomicBool::new(false)),
            };
            (responder, commands)
        }

        /// Admit a silent stream from `peer` the way the primary network does and drive it on a
        /// task. `None` when the stream would be shed.
        fn open_silent_stream(&self, peer: BlsPublicKey) -> Option<JoinHandle<()>> {
            let tier = self.reserved.tier(&peer);
            let grants = self.slots.try_take(peer).zip(self.opening.try_acquire(&peer, tier));
            grants.map(|(slot, opening)| {
                let grant = SyncAdmission { peer, tier, slot, opening };
                let (pools, net) = (self.pools.clone(), self.handle.clone());
                let served = self.served.clone();
                let serve = move |_stream: SilentStream, _request: PrimarySyncRequest| {
                    served.store(true, Ordering::SeqCst);
                    std::future::ready(SyncServeOutcome::Completed)
                };
                tokio::spawn(admit_and_serve_sync_stream(SilentStream, grant, pools, net, serve))
            })
        }

        /// The in-flight stream count of each of `peers`.
        fn in_flight(&self, peers: &[BlsPublicKey]) -> Vec<usize> {
            peers.iter().map(|peer| self.slots.in_flight(peer)).collect()
        }
    }

    /// Open a silent stream from `peer`, let the responder task start, then let one second pass.
    async fn open_then_wait(responder: &Responder, peer: BlsPublicKey) -> Option<JoinHandle<()>> {
        let task = responder.open_silent_stream(peer);
        settle().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        task
    }

    /// Public keys that fill the public share of a serve class cannot lock out a committee key,
    /// and one committee key cannot take the reserve of another (issue #1750). Three public keys
    /// at the per-peer cap (two, two and one streams) fill the share of five, and a fourth public
    /// key is refused. A committee key is served from its own reserve until that reserve is empty,
    /// and then it is refused. A second committee key still has its whole reserve and is served.
    /// The other classes keep their whole share, and released permits refill the public share.
    #[test]
    fn committee_key_is_served_when_public_keys_fill_a_serve_class() {
        let (committee, other) = (key(1), key(4));
        let reserved = ReservedPeers::new([committee, other]);
        let slots = PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER);
        let pools = SyncServePools::new();
        let pool = pools.pool(SyncClass::EpochPack);
        let held: Option<Vec<_>> = [(key(10), 2), (key(11), 2), (key(12), 1)]
            .into_iter()
            .flat_map(|(peer, count)| std::iter::repeat_n(peer, count))
            .map(|peer| slots.try_take(peer).zip(pool.try_acquire(&peer, reserved.tier(&peer))))
            .collect();
        assert_eq!(held.as_ref().map(Vec::len), Some(MAX_CONCURRENT_EPOCH_STREAMS));
        assert_eq!(pool.available_public(), 0);
        // a third stream from a public key at the per-peer cap is refused by its slot
        assert!(slots.try_take(key(10)).is_none());
        // a fourth public key has a free slot but no serve permit
        assert!(slots.try_take(key(13)).is_some());
        assert!(pool.try_acquire(&key(13), reserved.tier(&key(13))).is_none());
        // the committee key is served from its own reserve until that reserve is empty
        assert_eq!(reserved.tier(&committee), PeerTier::Committee);
        let committee_permits: Option<Vec<_>> =
            std::iter::repeat_with(|| pool.try_acquire(&committee, reserved.tier(&committee)))
                .take(COMMITTEE_RESERVED_SYNC_STREAMS)
                .collect();
        assert!(committee_permits.is_some());
        assert_eq!(pool.available_reserved(&committee), 0);
        assert!(pool.try_acquire(&committee, reserved.tier(&committee)).is_none());
        // a second committee key keeps its whole reserve and is still served
        assert_eq!(pool.available_reserved(&other), COMMITTEE_RESERVED_SYNC_STREAMS);
        let other_permit = pool.try_acquire(&other, reserved.tier(&other));
        assert!(other_permit.is_some());
        assert_eq!(
            pool.available_reserved(&other),
            COMMITTEE_RESERVED_SYNC_STREAMS.saturating_sub(1)
        );
        // a flood of one class does not touch the other classes
        assert!(public_share_whole(&pools, SyncClass::MissingCertificates));
        assert!(public_share_whole(&pools, SyncClass::ConsensusOutput));
        // released permits refill the public share
        drop(held);
        assert_eq!(pool.available_public(), MAX_CONCURRENT_EPOCH_STREAMS);
        assert!(pool.try_acquire(&key(13), reserved.tier(&key(13))).is_some());
    }

    /// Opening reads draw only on the opening-read budget. Four public keys at the per-peer cap
    /// fill its public share of eight, and a fifth public key is refused. A committee key opens
    /// from its own reserve until that reserve is empty, and then it is refused. A second
    /// committee key still has its whole reserve and opens from it (issue #1750). Every serve
    /// pool stays whole.
    #[test]
    fn opening_reads_leave_the_serve_pools_untouched() {
        let (committee, other) = (key(2), key(5));
        let reserved = ReservedPeers::new([committee, other]);
        let slots = PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER);
        let opening = opening_pool();
        let pools = SyncServePools::new();
        let held: Option<Vec<_>> = (20..24)
            .flat_map(|seed| std::iter::repeat_n(key(seed), MAX_PENDING_REQUESTS_PER_PEER))
            .map(|peer| slots.try_take(peer).zip(opening.try_acquire(&peer, reserved.tier(&peer))))
            .collect();
        assert_eq!(held.as_ref().map(Vec::len), Some(MAX_CONCURRENT_OPENING_READS));
        assert_eq!(opening.available_public(), 0);
        assert!(opening.try_acquire(&key(24), reserved.tier(&key(24))).is_none());
        // the committee key opens from its own reserve until that reserve is empty
        let committee_opening: Option<Vec<_>> =
            std::iter::repeat_with(|| opening.try_acquire(&committee, reserved.tier(&committee)))
                .take(COMMITTEE_RESERVED_OPENING_READS)
                .collect();
        assert!(committee_opening.is_some());
        assert_eq!(opening.available_reserved(&committee), 0);
        assert!(opening.try_acquire(&committee, reserved.tier(&committee)).is_none());
        // a second committee key keeps its whole reserve and still opens
        assert_eq!(opening.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
        let other_opening = opening.try_acquire(&other, reserved.tier(&other));
        assert!(other_opening.is_some());
        assert!(serve_pools_untouched(&pools, &committee));
        assert!(serve_pools_untouched(&pools, &other));
        drop((held, committee_opening, other_opening));
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.available_reserved(&committee), COMMITTEE_RESERVED_OPENING_READS);
        assert_eq!(opening.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
    }

    /// Silent streams hold only the opening-read budget, and only until the opening read times
    /// out. The serve pools stay whole, each stream frees its opening permit and its peer slot at
    /// the timeout, nothing is served, and each public peer (but not the committee peer) is
    /// charged one mild penalty.
    #[tokio::test(start_paused = true)]
    async fn silent_streams_hold_only_the_opening_budget_and_are_charged() {
        let (committee, first, second, third) = (key(3), key(30), key(31), key(32));
        let peers = [committee, first, second, third];
        let (open_cap, open_reserve) =
            (MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS);
        let (responder, mut commands) = Responder::new(committee);
        // the committee stream and the first public stream open at 0 s, the others at 1 s and 2 s
        let opened = [
            responder.open_silent_stream(committee),
            open_then_wait(&responder, first).await,
            open_then_wait(&responder, second).await,
            open_then_wait(&responder, third).await,
        ];
        let tasks: Option<Vec<JoinHandle<()>>> = opened.into_iter().collect();
        assert!(tasks.is_some());

        // 3 s: four opening reads are in flight, and no serve permit is taken
        assert_eq!(responder.opening.available_public(), open_cap.saturating_sub(3));
        assert_eq!(
            responder.opening.available_reserved(&committee),
            open_reserve.saturating_sub(1)
        );
        assert_eq!(responder.in_flight(&peers), vec![1, 1, 1, 1]);
        assert!(serve_pools_untouched(&responder.pools, &committee));

        // 5.5 s: the committee read and the first public read timed out
        tokio::time::advance(Duration::from_millis(2_500)).await;
        settle().await;
        assert_eq!(responder.opening.available_public(), open_cap.saturating_sub(2));
        assert_eq!(responder.opening.available_reserved(&committee), open_reserve);
        assert_eq!(responder.in_flight(&peers), vec![0, 0, 1, 1]);
        assert!(serve_pools_untouched(&responder.pools, &committee));

        // 7.5 s: every read timed out, and every budget is whole again
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert_eq!(responder.opening.available_public(), open_cap);
        assert_eq!(responder.in_flight(&peers), vec![0, 0, 0, 0]);
        assert!(tasks.is_some_and(|tasks| tasks.iter().all(JoinHandle::is_finished)));
        assert!(serve_pools_untouched(&responder.pools, &committee));
        assert!(!responder.served.load(Ordering::SeqCst));

        // one mild penalty per public peer, in timeout order, and none to the committee peer
        let reports = std::iter::from_fn(|| commands.try_recv().ok());
        let charged: Vec<_> = reports.map(mild_penalty_peer).collect();
        assert_eq!(charged, vec![Some(first), Some(second), Some(third)]);
    }
}

/// Tests of the outbound sync request write deadline (GHSA-qf73-5cfc-3jgc): the opening request
/// frame of every client path is written under [`SYNC_REQUEST_WRITE_TIMEOUT`].
#[cfg(test)]
mod sync_request_write_tests {
    use super::{
        read_frame, write_sync_request, PrimarySyncRequest, SyncFrame, SyncRequestWriteError,
        SYNC_REQUEST_WRITE_TIMEOUT,
    };
    use futures::AsyncWrite;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    /// A responder that negotiated the stream and then grants no receive credit: every write
    /// and every flush stays pending forever.
    #[derive(Debug)]
    struct StalledStream;

    impl AsyncWrite for StalledStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The request frame every test writes.
    fn request() -> SyncFrame<PrimarySyncRequest> {
        SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: 1 })
    }

    /// A write that never completes is cut after exactly [`SYNC_REQUEST_WRITE_TIMEOUT`] and
    /// reported as a timeout, so the probe walk moves on instead of waiting on the connection.
    #[tokio::test(start_paused = true)]
    async fn stalled_stream_times_out_the_request_write() {
        let start = tokio::time::Instant::now();
        let mut stream = StalledStream;
        let written = write_sync_request(
            &mut stream,
            &request(),
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await;
        assert!(matches!(written, Err(SyncRequestWriteError::TimedOut)));
        assert_eq!(start.elapsed(), SYNC_REQUEST_WRITE_TIMEOUT);
    }

    /// A ready writer receives the frame inside the deadline, and the bytes decode back to the
    /// same request.
    #[tokio::test]
    async fn ready_stream_delivers_the_request_frame() {
        let mut stream = futures::io::Cursor::new(Vec::new());
        let written = write_sync_request(
            &mut stream,
            &request(),
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await;
        assert!(written.is_ok());
        let bytes = stream.into_inner();
        assert!(!bytes.is_empty());

        let mut reader = futures::io::Cursor::new(bytes);
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let decoded = read_frame::<_, PrimarySyncRequest>(
            &mut reader,
            &mut decode_buffer,
            &mut compressed_buffer,
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("frame decodes");
        assert_eq!(decoded, request());
    }
}

/// Tests of the preferred sync server bookkeeping (GHSA-qf73-5cfc-3jgc): the last peer that
/// served is probed first only until it fails a probe.
#[cfg(test)]
mod sync_server_preference_tests {
    use super::{order_probe_peers, PrimaryNetworkHandle};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::collections::HashMap;
    use tn_types::{BlsKeypair, BlsPublicKey};
    use tokio::sync::mpsc;

    /// A deterministic BLS public key for `seed`.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// A handle whose command channel is never read; these tests only touch its caches.
    fn handle() -> PrimaryNetworkHandle {
        let (sender, _receiver) = mpsc::channel(1);
        PrimaryNetworkHandle::new_for_test(sender)
    }

    /// Forgetting a peer other than the recorded one keeps the preference; forgetting the
    /// recorded one clears it, and forgetting with nothing on record stays empty.
    #[test]
    fn forget_sync_server_drops_only_the_recorded_peer() {
        let handle = handle();
        let (served, other) = (key(1), key(2));
        handle.record_sync_served(served);
        assert_eq!(*handle.last_sync_server.lock(), Some(served));

        handle.forget_sync_server(other);
        assert_eq!(*handle.last_sync_server.lock(), Some(served));

        handle.forget_sync_server(served);
        assert_eq!(*handle.last_sync_server.lock(), None);

        handle.forget_sync_server(served);
        assert_eq!(*handle.last_sync_server.lock(), None);
    }

    /// With a preference the preferred peer leads the walk; without one the walk is the seeded
    /// shuffle alone, so no peer is pinned to the front.
    #[test]
    fn order_probe_peers_puts_no_peer_first_without_a_preference() {
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let preferred = key(3);
        let failed = HashMap::new();

        let with_preference = order_probe_peers(
            peers.clone(),
            &failed,
            0,
            Some(preferred),
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(with_preference.first(), Some(&preferred));
        assert_eq!(with_preference.len(), peers.len());
        assert!(peers.iter().all(|p| with_preference.contains(p)));

        let without =
            order_probe_peers(peers.clone(), &failed, 0, None, &mut StdRng::seed_from_u64(7));
        let shuffle_only =
            order_probe_peers(peers.clone(), &failed, 0, None, &mut StdRng::seed_from_u64(7));
        assert_eq!(without, shuffle_only);
        assert_eq!(without.len(), peers.len());
        assert!(peers.iter().all(|p| without.contains(p)));
    }
}
