//! Worker network implementation.

use crate::metrics::{SyncShedReason, WorkerMetrics};
use error::{WorkerNetworkError, WorkerNetworkResult};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use handle::max_sync_frame_size;
pub use handle::WorkerNetworkHandle;
use handler::RequestHandler;
pub use message::{WorkerRequest, WorkerResponse};
use std::{collections::BTreeSet, future::Future, io::ErrorKind, sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    read_frame,
    types::{GossipValidation, NetworkEvent},
    write_frame, DenyReason, GossipMessage, HoldBudget, HoldLease, PeerSlot, PeerSlots, PeerTier,
    Penalty, ReservedPeers, ReservedPool, ResponseChannel, ServiceClass, Stream, SyncFrame,
    SyncFrameError, WorkerSyncRequest,
};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BatchValidation, BlsPublicKey, Database, Epoch, SealedBatch, TaskError, TaskSpawner,
    TnReceiver, WorkerId, B256,
};
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tracing::{debug, warn};

pub(crate) mod error;
pub(crate) mod handle;
pub(crate) mod handler;
pub(crate) mod message;
pub(crate) mod primary;
pub(crate) mod stream_codec;

/// Convenience type for Worker network.
pub(crate) type Req = WorkerRequest;

/// Convenience type for Worker network.
pub(crate) type Res = WorkerResponse;

/// Public share of the batch-serve pool for inbound sync streams.
///
/// A permit is taken after the opening request frame is read and held until the
/// serve ends, so this bounds the count of concurrent serves for public peers.
/// Each committee key (the current and the next committee) draws first on its own
/// reserve of `COMMITTEE_RESERVED_BATCH_STREAMS` permits that no other key can
/// take, so public peers that fill this share cannot block a committee request.
/// A public serve is also bounded by `PUBLIC_BATCH_SERVE_TIMEOUT` and budgeted per
/// key by `PUBLIC_BATCH_SERVE_HOLD_BUDGET`.
pub const MAX_CONCURRENT_BATCH_STREAMS: usize = 5;

/// Batch-serve permits reserved for each committee key.
///
/// Each key of the current or the next committee has its own reserve of this many
/// permits. That key takes a permit from its reserve first and then from the
/// [`MAX_CONCURRENT_BATCH_STREAMS`] public share. A public peer never takes one,
/// and no other committee key can take from this reserve. So committee keys that
/// hold streams without progress cannot stop a different committee key from
/// catching up (issue #1750). The per-peer cap ([`MAX_PENDING_REQUESTS_PER_PEER`],
/// 2) lets a committee key open one more stream from the public share.
pub(crate) const COMMITTEE_RESERVED_BATCH_STREAMS: usize = 1;

/// Public share of the opening-read pool for inbound sync streams.
///
/// A permit is taken when the stream opens and held while the opening request
/// frame is read, which [`opening_read_bound`] bounds for the peer's tier. The
/// permit is released as soon as the opening read ends, whatever its result, so no
/// serve, control write or close holds it. A public peer takes a lease from its
/// opening hold budget ([`PUBLIC_OPENING_HOLD_BUDGET`] per
/// [`PUBLIC_OPENING_HOLD_WINDOW`]) before the permit, and the time its opening read
/// took is charged to that budget when the read ends, so a few public keys cannot
/// hold this share by re-opening below the late charge (GHSA-pg7j-w22m-3qj5).
pub(crate) const MAX_CONCURRENT_OPENING_READS: usize = 8;

/// Opening-read permits reserved for each committee key.
///
/// Each key of the current or the next committee has its own reserve of this many
/// permits. That key takes a permit from its reserve first and then from the
/// [`MAX_CONCURRENT_OPENING_READS`] public share. A public peer never takes one,
/// and no other committee key can take from this reserve. So committee keys that
/// hold streams without progress cannot stop a different committee key from
/// catching up (issue #1750). The per-peer cap ([`MAX_PENDING_REQUESTS_PER_PEER`],
/// 2) lets a committee key open one more stream from the public share.
pub(crate) const COMMITTEE_RESERVED_OPENING_READS: usize = 1;

/// Maximum batch digests allowed per sync batch request (`WorkerSyncRequest::Batches`).
///
/// Derivation: 10 committee nodes * 6 max commit rounds * 5 batches per cert = 300.
/// We use 500 for forward-compatibility headroom (committee growth, parameter changes).
/// This is 66x smaller than the ~33k digests that fit in the 1MB RPC limit.
pub const MAX_BATCH_DIGESTS_PER_REQUEST: usize = 500;

/// Maximum number of concurrent in-flight sync batch streams from a single peer.
///
/// A peer holds one slot per sync stream through both admission phases (the
/// opening read and the batch serve), so no single peer can fill either pool
/// and starve its peers. A public key may serve on only
/// `PUBLIC_BATCH_SERVES_PER_PEER` of its streams at a time. The same cap bounds
/// the batch reports that one peer has in validation at a time (see
/// `WorkerNetwork::process_report_batch`). A cancelled report does not release
/// its slot early: the slot is held until the validation task ends.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Maximum number of concurrent gossip-triggered batch prefetches.
///
/// When a committee worker gossips an accepted batch digest, a subscribed committee validator
/// that is missing the batch prefetches it, and that fetch fans out to every connected peer (see
/// [`handle::WorkerNetworkHandle::request_batches`]). Prefetching is a best-effort
/// optimization: a genuinely needed batch is still fetched by the vote path itself
/// (`PrimaryReceiverHandler::synchronize` -> `request_batches`), and observers never take this
/// path at all — they receive batch bodies with the consensus output they follow. Capping
/// the number in flight (and deduplicating by digest) stops a Byzantine author from
/// amplifying bandwidth/connection load across the worker mesh by gossiping many
/// distinct (possibly forged) digests. Load beyond the cap is shed, not queued.
pub const MAX_CONCURRENT_GOSSIP_PREFETCHES: usize = 8;

/// Maximum number of concurrent shed tasks for denied inbound sync streams.
///
/// A denied stream still gets a short-lived task that writes
/// [`DenyReason::AtCapacity`] so the requester fails fast instead of waiting out
/// a timeout. Without a budget on that spawn, a burst of opened substreams
/// beyond the admission caps fans out to one task each, bounded only by the
/// libp2p per-connection substream limit (#1254). Past this budget the stream
/// is dropped without spawning (the requester sees a reset and retries
/// elsewhere), so the worker's total sync-task fan-out stays bounded by the
/// admitted tasks plus this many shed tasks. Each admitted task holds a per-peer
/// slot for its life, and an opening-read permit while it reads or a batch-serve
/// permit while it serves (see [`MAX_PENDING_REQUESTS_PER_PEER`],
/// [`MAX_CONCURRENT_OPENING_READS`] and [`MAX_CONCURRENT_BATCH_STREAMS`], each
/// pool with its committee reserve).
pub(crate) const MAX_CONCURRENT_SHED_TASKS: usize = 8;

/// Upper bound on the opening read of a committee-tier sync stream, and on each
/// control write (`Ack`, `Deny`, `End`) and the close.
///
/// A committee peer that opens a sync stream but never sends its request frame
/// trips this and the stream is dropped, so it cannot hold an admission slot
/// indefinitely. Public peers get the shorter [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`]
/// for the opening read.
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on the opening read of a public-tier sync stream.
///
/// A public opening request is small (an honest batches request carries at most
/// [`MAX_BATCH_DIGESTS_PER_REQUEST`] 32-byte digests), so one second is ample on a
/// working link. A public opening that uses more than half of this bound is charged
/// [`Penalty::Mild`] whatever frame it carried, so a peer cannot sit on an opening
/// permit for most of the window at no cost (GHSA-vr4r-x9mm-6x9g).
/// Committee peers keep [`SYNC_REQUEST_READ_TIMEOUT`].
const PUBLIC_SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Upper bound on one public-tier batch serve.
///
/// A fixed, size-independent cap of two frame bounds (`SEND_STREAM_FRAME_TIMEOUT`,
/// 30 s). At the 20 MB/s design rate that `SEND_STREAM_TIMEOUT` assumes, a full
/// request of [`MAX_BATCH_DIGESTS_PER_REQUEST`] 1 MiB batches completes in about
/// 25 s, so an honest public reader keeps more than 2x headroom; a reader that
/// drains at the frame floor gets about 2 MiB per serve. A serve that runs past
/// this bound is dropped, classed [`SyncServeOutcome::Stalled`] and charged
/// [`Penalty::Mild`], so a slow public reader cannot hold a batch-serve permit
/// for the whole 200 s stream bound at no cost (GHSA-pg7j-w22m-3qj5). Committee
/// serves keep `SEND_STREAM_TIMEOUT` only.
pub(crate) const PUBLIC_BATCH_SERVE_TIMEOUT: Duration = Duration::from_secs(60);

/// The batch-serve hold time one public key may use per [`PUBLIC_BATCH_SERVE_HOLD_WINDOW`].
///
/// One full serve at [`PUBLIC_BATCH_SERVE_TIMEOUT`], or several short ones. A key
/// is leased serve time only while it has a full [`PUBLIC_BATCH_SERVE_TIMEOUT`] of
/// this budget left, so an admitted serve can always run to its limit inside the
/// budget and a fresh key gets one full hold per refill, not two. A key with less
/// than that left is refused with `Deny(AtCapacity)` and not charged, like any
/// other at-capacity answer, until its charge has leaked: a key that completes a
/// serve waits 5 s for every second it was served before its next serve, and a key
/// that drains a serve just inside the hold limit is refused for the whole window
/// (GHSA-pg7j-w22m-3qj5).
pub(crate) const PUBLIC_BATCH_SERVE_HOLD_BUDGET: Duration = PUBLIC_BATCH_SERVE_TIMEOUT;

/// The window [`PUBLIC_BATCH_SERVE_HOLD_BUDGET`] refills over.
///
/// The same 300 s the peer score decays over (`ScoreConfig::score_halflife`), so a
/// key that just spent its budget is refused for about as long as a penalty would
/// take to fade. One key can hold the public share for at most 1/5 of the time, so
/// filling the 5 public permits needs 25 keys that are each refused for 4/5 of the
/// time, in the first window and in every window after it, instead of 3 keys with
/// no limit (GHSA-pg7j-w22m-3qj5).
pub(crate) const PUBLIC_BATCH_SERVE_HOLD_WINDOW: Duration = Duration::from_secs(300);

/// The number of batch serves one public key may hold on a worker at a time.
///
/// The per-peer stream cap ([`MAX_PENDING_REQUESTS_PER_PEER`], 2) still lets a
/// public key hold a second stream in its opening phase; a second serve is
/// refused with `Deny(AtCapacity)`, uncharged. Committee keys keep the stream cap
/// (GHSA-pg7j-w22m-3qj5).
pub(crate) const PUBLIC_BATCH_SERVES_PER_PEER: usize = 1;

/// The opening-read hold time one public key may use per [`PUBLIC_OPENING_HOLD_WINDOW`].
///
/// An honest opening takes about one round trip, so ten seconds covers hundreds of
/// openings in a window. A public key is leased opening time only while it has a full
/// [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`] of this budget left, and the time its opening
/// read took is charged when that read ends. A key outside the budget takes the shed
/// path, uncharged, like any other at-capacity opening. So a key that sits on an opening
/// permit for just under half the read bound, below the late charge, gets about twenty
/// such openings per window instead of an unbounded number, and filling the
/// [`MAX_CONCURRENT_OPENING_READS`] public share needs about 240 keys instead of 4
/// (GHSA-pg7j-w22m-3qj5; the opening-phase residual of GHSA-vr4r-x9mm-6x9g).
pub(crate) const PUBLIC_OPENING_HOLD_BUDGET: Duration = Duration::from_secs(10);

/// The window [`PUBLIC_OPENING_HOLD_BUDGET`] refills over.
///
/// The same 300 s as [`PUBLIC_BATCH_SERVE_HOLD_WINDOW`], on a budget of its own, so
/// openings never draw down the serve time of an honest key.
pub(crate) const PUBLIC_OPENING_HOLD_WINDOW: Duration = Duration::from_secs(300);

/// How a served sync batch exchange ended.
///
/// The admission layer uses this to decide on a penalty: only a stall, where the
/// peer stopped reading and kept a batch-serve permit busy until a time limit
/// expired, or the requester stopped or closed the stream after the serve began,
/// is charged to a public peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncServeOutcome {
    /// Every frame was written and the stream ended with `End`.
    Completed,
    /// A write time limit expired (a per-frame limit or the whole-stream limit), or
    /// the requester stopped or closed the stream after the serve began.
    Stalled,
    /// The serve failed for a reason that is not a stall, such as a storage error
    /// or an I/O error of another kind (for example, a broken pipe).
    Failed,
}

impl SyncServeOutcome {
    /// Classify the result of a sync batch serve.
    ///
    /// A whole-stream timeout (`Timeout(Elapsed)`) and a per-frame timeout (an
    /// `StdIo` error of kind [`ErrorKind::TimedOut`]) are stalls. A write error of
    /// kind [`ErrorKind::ConnectionReset`] or [`ErrorKind::NotConnected`] is a stall
    /// too: these are the kinds quinn 0.11 reports when the requester stops or
    /// closes the stream after the serve began (its `From<WriteError> for
    /// io::Error` maps `Stopped` and `ZeroRttRejected` to `ConnectionReset`, and
    /// `ConnectionLost` and `ClosedStream` to `NotConnected`). An honest requester
    /// never does that, because its read tolerance is at or above the whole-stream
    /// cap. Every other error is a failure.
    pub(crate) fn of(served: &WorkerNetworkResult<()>) -> Self {
        served.as_ref().map_or_else(Self::of_error, |_| Self::Completed)
    }

    /// Classify one serve error as a stall or a failure, as [`Self::of`] describes.
    fn of_error(error: &WorkerNetworkError) -> Self {
        match error {
            WorkerNetworkError::Timeout(_) => Self::Stalled,
            WorkerNetworkError::StdIo(io)
                if matches!(
                    io.kind(),
                    ErrorKind::TimedOut | ErrorKind::ConnectionReset | ErrorKind::NotConnected
                ) =>
            {
                Self::Stalled
            }
            WorkerNetworkError::StdIo(_)
            | WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::BatchValidation(_)
            | WorkerNetworkError::NonCommitteeBatch
            | WorkerNetworkError::RejectedBatchRepeat(_)
            | WorkerNetworkError::Internal(_)
            | WorkerNetworkError::Network(_)
            | WorkerNetworkError::InvalidRequest(_)
            | WorkerNetworkError::InvalidTopic
            | WorkerNetworkError::TooManyBatches { .. }
            | WorkerNetworkError::UnexpectedBatch(_)
            | WorkerNetworkError::DuplicateBatch(_)
            | WorkerNetworkError::StreamClosed
            | WorkerNetworkError::UnknownStreamRequest(_)
            | WorkerNetworkError::RequestHashMismatch
            | WorkerNetworkError::DBInsert(_)
            | WorkerNetworkError::DBCommit(_)
            | WorkerNetworkError::DBRead(_)
            | WorkerNetworkError::BatchEpochMismatch(..) => Self::Failed,
        }
    }
}

/// The budgets that admit the opening phase of one inbound sync stream.
#[derive(Clone, Debug)]
struct OpeningAdmission {
    /// The per-peer in-flight stream cap, held through both phases.
    slots: PeerSlots,
    /// The opening-read pool: a public share and a reserve for each committee key.
    pool: ReservedPool,
    /// The opening-read hold time each public key has used, capped at
    /// [`PUBLIC_OPENING_HOLD_BUDGET`] per [`PUBLIC_OPENING_HOLD_WINDOW`].
    public_hold: HoldBudget,
}

impl OpeningAdmission {
    /// The budgets at the sizes the live network uses.
    fn new() -> Self {
        Self {
            slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
            pool: ReservedPool::new(MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS),
            public_hold: HoldBudget::new(
                PUBLIC_SYNC_REQUEST_READ_TIMEOUT,
                PUBLIC_OPENING_HOLD_BUDGET,
                PUBLIC_OPENING_HOLD_WINDOW,
            ),
        }
    }

    /// Try to take the opening-read grants for `peer` of `tier`, without waiting.
    ///
    /// A committee opening takes its pool permit only. A public opening takes its hold lease,
    /// then its pool permit, so a key outside its budget never holds a permit.
    fn try_grant(&self, peer: &BlsPublicKey, tier: PeerTier, now: Instant) -> Option<OpeningGrant> {
        match tier {
            PeerTier::Committee => self.pool.try_acquire(peer, tier).map(OpeningGrant::Committee),
            PeerTier::Public => self.public_hold.try_lease(*peer, now).and_then(|lease| {
                self.pool.try_acquire(peer, tier).map(|permit| {
                    OpeningGrant::Public(Box::new(PublicOpeningGrant { lease, permit }))
                })
            }),
        }
    }
}

/// The opening-read grants one admitted stream holds while it reads its request frame.
#[derive(Debug)]
enum OpeningGrant {
    /// A committee opening holds its pool permit only.
    Committee(OwnedSemaphorePermit),
    /// A public opening also holds its hold-budget lease, which charges the read time when it
    /// drops. Boxed: the lease alone is about 340 bytes next to a 16-byte permit.
    Public(Box<PublicOpeningGrant>),
}

/// The grants one admitted public opening holds, declared in release order so an aborted task
/// drops them the same way `OpeningGrant::release` does: the lease books its charge before the
/// permit frees.
#[derive(Debug)]
struct PublicOpeningGrant {
    /// The hold-budget lease, which charges the read time when it drops.
    lease: HoldLease,
    /// The pool permit.
    permit: OwnedSemaphorePermit,
}

impl OpeningGrant {
    /// Release every grant: the lease first, so its charge is on the books before the permit
    /// frees, then the permit.
    fn release(self) {
        match self {
            Self::Committee(permit) => drop(permit),
            Self::Public(grant) => {
                let PublicOpeningGrant { lease, permit } = *grant;
                drop(lease);
                drop(permit);
            }
        }
    }
}

/// An inbound sync stream admitted to the opening phase.
///
/// Holds the peer's [`PeerSlot`] for the whole exchange and its opening-read
/// grants: a pool permit, and for a public peer the hold lease the permit was
/// taken under. The grants are released as soon as the opening read ends, whatever
/// its result, so no serve, control write or close holds opening-read capacity.
#[derive(Debug)]
struct SyncAdmission {
    /// The peer that opened the stream.
    peer: BlsPublicKey,
    /// The admission tier of the peer.
    tier: PeerTier,
    /// The per-peer in-flight slot, held through both phases.
    slot: PeerSlot,
    /// The opening-read grants, released as soon as the opening read ends, whatever its result.
    opening: OpeningGrant,
}

/// Try to admit the opening phase of one inbound sync stream for `peer`.
///
/// Takes a per-peer slot first, so a peer at [`MAX_PENDING_REQUESTS_PER_PEER`]
/// never holds a pool permit, then for a public peer a lease from its opening hold
/// budget, then an opening-read permit for `tier`. A committee peer draws on its
/// own reserve before the public share; a public peer draws only on the public
/// share. Returns `None` when any budget is full, and a grant taken before a
/// refusal is released on drop; the refused stream takes the shed path, uncharged.
fn try_admit_opening(
    opening: &OpeningAdmission,
    peer: BlsPublicKey,
    tier: PeerTier,
) -> Option<SyncAdmission> {
    let now = Instant::now();
    opening.slots.try_take(peer).and_then(|slot| {
        opening.try_grant(&peer, tier, now).map(|opening| SyncAdmission {
            peer,
            tier,
            slot,
            opening,
        })
    })
}

/// The budgets that admit one batch serve, taken together when the opening `Req` is read.
#[derive(Clone, Debug)]
struct BatchServeAdmission {
    /// The batch-serve pool: a public share and a reserve for each committee key.
    pool: ReservedPool,
    /// The number of batch serves each public key has in flight, capped at
    /// [`PUBLIC_BATCH_SERVES_PER_PEER`].
    public_serves: PeerSlots,
    /// The hold time each public key has used, capped at [`PUBLIC_BATCH_SERVE_HOLD_BUDGET`] per
    /// [`PUBLIC_BATCH_SERVE_HOLD_WINDOW`].
    public_hold: HoldBudget,
}

impl BatchServeAdmission {
    /// The budgets at the sizes the live network uses.
    fn new() -> Self {
        Self {
            pool: ReservedPool::new(MAX_CONCURRENT_BATCH_STREAMS, COMMITTEE_RESERVED_BATCH_STREAMS),
            public_serves: PeerSlots::new(PUBLIC_BATCH_SERVES_PER_PEER),
            public_hold: HoldBudget::new(
                PUBLIC_BATCH_SERVE_TIMEOUT,
                PUBLIC_BATCH_SERVE_HOLD_BUDGET,
                PUBLIC_BATCH_SERVE_HOLD_WINDOW,
            ),
        }
    }

    /// Try to take every grant a batch serve for `peer` of `tier` needs, without waiting.
    ///
    /// A committee serve needs its pool permit only. A public serve takes, in this order, its
    /// per-key serve slot, its hold lease and its pool permit; a grant taken before a refusal is
    /// released on drop.
    fn try_grant(
        &self,
        peer: &BlsPublicKey,
        tier: PeerTier,
        now: Instant,
    ) -> Result<ServeGrant, ServeRefusal> {
        match tier {
            PeerTier::Committee => self
                .pool
                .try_acquire(peer, tier)
                .map(ServeGrant::Committee)
                .ok_or(ServeRefusal::PoolFull),
            PeerTier::Public => self
                .public_serves
                .try_take(*peer)
                .ok_or(ServeRefusal::PeerCap)
                .and_then(|slot| {
                    self.public_hold
                        .try_lease(*peer, now)
                        .ok_or(ServeRefusal::HoldBudget)
                        .map(|lease| (slot, lease))
                })
                .and_then(|(slot, lease)| {
                    self.pool.try_acquire(peer, tier).ok_or(ServeRefusal::PoolFull).map(|permit| {
                        ServeGrant::Public(Box::new(PublicServeGrant { permit, lease, slot }))
                    })
                }),
        }
    }
}

/// The grants one admitted public batch serve holds, declared in release order so an aborted
/// task drops them the same way `ServeGrant::release` does.
#[derive(Debug)]
struct PublicServeGrant {
    /// The pool permit.
    permit: OwnedSemaphorePermit,
    /// The hold-budget lease, which charges the held time when it drops.
    lease: HoldLease,
    /// The per-key serve slot.
    slot: PeerSlot,
}

/// The grants one admitted batch serve holds until it ends.
#[derive(Debug)]
enum ServeGrant {
    /// A committee serve holds its pool permit only.
    Committee(OwnedSemaphorePermit),
    /// A public serve also holds its per-key serve slot and its hold-budget lease. Boxed: the
    /// public grants are about 650 bytes next to a 16-byte permit.
    Public(Box<PublicServeGrant>),
}

impl ServeGrant {
    /// Release every grant: the pool permit, then the lease, so its charge is on the books before
    /// the per-key serve slot frees and a second stream of the key cannot pass `try_lease`
    /// against books that miss the hold just ended, then the slot.
    fn release(self) {
        match self {
            Self::Committee(permit) => drop(permit),
            Self::Public(grant) => {
                let PublicServeGrant { permit, lease, slot } = *grant;
                drop(permit);
                drop(lease);
                drop(slot);
            }
        }
    }
}

/// Why a batch serve was refused. Every refusal answers `Deny(AtCapacity)` on the wire; the
/// reason is for the log only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServeRefusal {
    /// No pool permit the peer may take is free.
    PoolFull,
    /// The public key already serves on [`PUBLIC_BATCH_SERVES_PER_PEER`] streams.
    PeerCap,
    /// The public key has used its [`PUBLIC_BATCH_SERVE_HOLD_BUDGET`] for the window.
    HoldBudget,
}

/// The wall-clock bound the admission layer puts on one batch serve, by tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServeHoldLimit {
    /// The serve runs under its own bounds only (`SEND_STREAM_TIMEOUT` and the per-write bounds).
    Unbounded,
    /// The serve is dropped after this long and reported as [`SyncServeOutcome::Stalled`].
    Bounded(Duration),
}

impl ServeHoldLimit {
    /// The limit for a serve to a peer of `tier`: a public-tier serve is bounded by
    /// [`PUBLIC_BATCH_SERVE_TIMEOUT`]; a committee serve keeps `SEND_STREAM_TIMEOUT` only.
    fn of(tier: PeerTier) -> Self {
        match tier {
            PeerTier::Committee => Self::Unbounded,
            PeerTier::Public => Self::Bounded(PUBLIC_BATCH_SERVE_TIMEOUT),
        }
    }

    /// Run `serve` under this limit. A serve cut at the limit is dropped, which resets the stream
    /// for the requester, and is reported as [`SyncServeOutcome::Stalled`].
    async fn run<Fut: Future<Output = SyncServeOutcome>>(self, serve: Fut) -> SyncServeOutcome {
        match self {
            Self::Unbounded => serve.await,
            Self::Bounded(limit) => {
                tokio::time::timeout(limit, serve).await.unwrap_or(SyncServeOutcome::Stalled)
            }
        }
    }
}

/// The number of digests `request` names, before the responder caps it.
fn requested_digests(request: &WorkerSyncRequest) -> usize {
    let WorkerSyncRequest::Batches { batch_digests, .. } = request;
    batch_digests.len()
}

/// The result of the bounded read of an opening sync request frame.
#[derive(Debug)]
enum OpeningRead {
    /// No frame arrived within [`opening_read_bound`] for the peer's tier.
    Silent,
    /// The frame could not be read or decoded.
    Unreadable(std::io::Error),
    /// A request frame, with the grants that admit its serve.
    Admitted(WorkerSyncRequest, ServeGrant),
    /// A request frame, refused by one of the batch-serve budgets.
    AtCapacity(ServeRefusal),
    /// A readable frame that is not a request.
    Unexpected,
}

impl OpeningRead {
    /// Classify a decoded opening frame.
    ///
    /// A request takes every grant its serve needs from `admission` here, before
    /// the caller releases the opening-read permit, so an admitted stream always
    /// holds one of the two. A committee-tier `peer` takes its pool permit from its
    /// own reserve first and then from the public share.
    fn from_frame(
        frame: SyncFrame<WorkerSyncRequest>,
        admission: &BatchServeAdmission,
        peer: &BlsPublicKey,
        tier: PeerTier,
        now: Instant,
    ) -> Self {
        match frame {
            SyncFrame::Req(request) => admission
                .try_grant(peer, tier, now)
                .map_or_else(Self::AtCapacity, |grant| Self::Admitted(request, grant)),
            SyncFrame::Ack
            | SyncFrame::Deny(_)
            | SyncFrame::Data(_)
            | SyncFrame::End
            | SyncFrame::Err(_) => Self::Unexpected,
        }
    }
}

/// Opening-read bound for a peer tier: public peers get [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`],
/// committee peers keep [`SYNC_REQUEST_READ_TIMEOUT`].
fn opening_read_bound(tier: PeerTier) -> Duration {
    match tier {
        PeerTier::Committee => SYNC_REQUEST_READ_TIMEOUT,
        PeerTier::Public => PUBLIC_SYNC_REQUEST_READ_TIMEOUT,
    }
}

/// Run one inbound sync exchange through both admission phases.
///
/// The opening phase reads the request frame under the opening-read grants in
/// `admission`, bounded by [`opening_read_bound`] for the peer's tier. Those
/// grants are released as soon as that read ends, whatever its result, before any
/// serve, penalty, control write or close; for a public peer the lease then
/// charges the time the read took to the key's opening budget
/// ([`PUBLIC_OPENING_HOLD_BUDGET`] per [`PUBLIC_OPENING_HOLD_WINDOW`]).
///
/// The serve phase needs every grant in `batch_serve` for the peer's tier: a pool
/// permit for any peer, and for a public peer also a per-key serve slot (at most
/// [`PUBLIC_BATCH_SERVES_PER_PEER`] in flight) and a hold lease within
/// [`PUBLIC_BATCH_SERVE_HOLD_BUDGET`] per [`PUBLIC_BATCH_SERVE_HOLD_WINDOW`]. A
/// committee peer draws on the reserve before the public share, so public peers
/// cannot use up the capacity that a committee request needs. With every grant,
/// `serve` runs, cut for a public peer at [`PUBLIC_BATCH_SERVE_TIMEOUT`], and the
/// grants are held only while it serves. Without one, the stream gets a bounded
/// [`DenyReason::AtCapacity`] write.
///
/// A public-tier peer is charged [`Penalty::Mild`] for a silent, unreadable or
/// non-request opening, for a [`SyncServeOutcome::Stalled`] serve, which includes
/// a serve cut at [`PUBLIC_BATCH_SERVE_TIMEOUT`], and for a request that names
/// more than [`MAX_BATCH_DIGESTS_PER_REQUEST`] digests, which is served truncated.
/// It is also charged for a request that arrives in the second half of its
/// opening-read bound, whether the request is then admitted or refused. That
/// charge is sent once the stream holds no serve grant: after the serve for an
/// admitted request, before the deny for a refused one. A prompt
/// [`DenyReason::AtCapacity`] answer is not charged, whichever budget refused it:
/// an honest peer gets it when the public share is full, and the refused stream
/// holds no grant. A committee peer is never charged.
///
/// The per-peer slot is held until this returns. Generic over the stream and the
/// serve step, so unit tests drive both phases with in-memory streams and a paused
/// clock.
async fn admit_and_serve_sync_stream<S, F, Fut>(
    mut stream: S,
    admission: SyncAdmission,
    batch_serve: BatchServeAdmission,
    network_handle: WorkerNetworkHandle,
    max_frame: usize,
    serve: F,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FnOnce(S, WorkerSyncRequest) -> Fut,
    Fut: Future<Output = SyncServeOutcome>,
{
    let SyncAdmission { peer, tier, slot: _slot, opening } = admission;
    let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
    let read_bound = opening_read_bound(tier);
    let opened = tokio::time::Instant::now();
    let read = tokio::time::timeout(
        read_bound,
        read_frame::<_, WorkerSyncRequest>(
            &mut stream,
            &mut decode_buffer,
            &mut decompress_buffer,
            max_frame,
        ),
    )
    .await
    .map_or(OpeningRead::Silent, |frame| {
        frame.map_or_else(OpeningRead::Unreadable, |frame| {
            OpeningRead::from_frame(frame, &batch_serve, &peer, tier, Instant::now())
        })
    });
    let elapsed = opened.elapsed();
    let late = elapsed > read_bound / 2;

    // the opening read has ended: release its grants before any serve, penalty, write or close,
    // lease first so its charge is on the books before the permit frees
    opening.release();
    match read {
        OpeningRead::Silent => {
            warn!(
                target: "worker::network",
                %peer,
                ?tier,
                "no sync request frame within the read timeout"
            );
            charge_public_peer(&network_handle, peer, tier).await;
            close_stream_bounded(&mut stream).await;
        }
        OpeningRead::Unreadable(error) => {
            warn!(target: "worker::network", %peer, ?tier, ?error, "unreadable sync request frame");
            charge_public_peer(&network_handle, peer, tier).await;
            close_stream_bounded(&mut stream).await;
        }
        OpeningRead::Admitted(request, grant) => {
            let requested = requested_digests(&request);
            let outcome = ServeHoldLimit::of(tier).run(serve(stream, request)).await;
            // free every grant before the penalty send so a stall cannot hold one; a public
            // lease charges its hold time to the key here
            grant.release();
            match outcome {
                SyncServeOutcome::Stalled => {
                    warn!(
                        target: "worker::network",
                        %peer,
                        ?tier,
                        hold_limit = ?ServeHoldLimit::of(tier),
                        "sync batch serve stalled or was cut at the hold limit"
                    );
                    charge_public_peer(&network_handle, peer, tier).await
                }
                SyncServeOutcome::Completed | SyncServeOutcome::Failed => {}
            }
            // a request above the digest cap is served truncated and charged, so an oversized
            // request never costs less than a late one
            if requested > MAX_BATCH_DIGESTS_PER_REQUEST {
                warn!(
                    target: "worker::network",
                    %peer,
                    ?tier,
                    requested,
                    max = MAX_BATCH_DIGESTS_PER_REQUEST,
                    "oversized sync batch request"
                );
                charge_public_peer(&network_handle, peer, tier).await;
            }
            // a request that used more than half of the opening bound is charged on top of
            // the serve outcome, so a late opening never pays less than a silent one
            if late {
                warn!(
                    target: "worker::network", %peer, ?tier, ?elapsed, "late sync request opening"
                );
                charge_public_peer(&network_handle, peer, tier).await;
            }
        }
        OpeningRead::AtCapacity(refusal) => {
            debug!(
                target: "worker::network",
                %peer,
                ?tier,
                ?refusal,
                "batch-serve admission refused, denying inbound sync stream"
            );
            // a refusal is free only when the request was prompt; the refused stream holds
            // no serve permit, so the charge goes out before the deny
            if late {
                warn!(
                    target: "worker::network", %peer, ?tier, ?elapsed, "late sync request opening"
                );
                charge_public_peer(&network_handle, peer, tier).await;
            }
            write_control_frame_and_close(
                &mut stream,
                &SyncFrame::Deny(DenyReason::AtCapacity),
                max_frame,
            )
            .await;
        }
        OpeningRead::Unexpected => {
            // a well-behaved requester always opens with `Req`; anything else is
            // malformed. Charge a public peer, signal it, and drop.
            warn!(
                target: "worker::network",
                %peer,
                ?tier,
                "unexpected opening sync frame from requester"
            );
            charge_public_peer(&network_handle, peer, tier).await;
            write_control_frame_and_close(
                &mut stream,
                &SyncFrame::Err(SyncFrameError::Malformed),
                max_frame,
            )
            .await;
        }
    }
}

/// Charge `peer` a [`Penalty::Mild`] if it is a public peer.
///
/// Called for a silent, unreadable or non-request opening frame, for a request
/// that arrives in the second half of its opening-read bound (admitted or
/// refused), for a stalled serve (the peer stopped reading, or stopped or
/// closed the stream after the serve began), for a public serve cut at the hold
/// limit, and for a request above the digest cap. A prompt
/// [`DenyReason::AtCapacity`] refusal is not charged. A committee peer is never charged, because
/// load on either side can slow an honest committee peer.
async fn charge_public_peer(
    network_handle: &WorkerNetworkHandle,
    peer: BlsPublicKey,
    tier: PeerTier,
) {
    match tier {
        PeerTier::Public => network_handle.report_penalty(peer, Penalty::Mild).await,
        PeerTier::Committee => {}
    }
}

/// Write one best-effort control frame to `stream`, then close it.
///
/// The write and the close each have their own [`SYNC_REQUEST_READ_TIMEOUT`]
/// limit, so a peer that stops reading cannot pin the calling task, or the
/// permits it holds, on either step.
async fn write_control_frame_and_close<S>(
    stream: &mut S,
    frame: &SyncFrame<WorkerSyncRequest>,
    max_frame: usize,
) where
    S: AsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    let _ = tokio::time::timeout(
        SYNC_REQUEST_READ_TIMEOUT,
        write_frame(stream, frame, &mut encode_buffer, &mut compressed_buffer, max_frame),
    )
    .await;
    close_stream_bounded(stream).await;
}

/// Close `stream` gracefully, bounded by [`SYNC_REQUEST_READ_TIMEOUT`].
///
/// A peer that stops reading cannot pin the calling task, or the permits it
/// holds, on the FIN flush.
async fn close_stream_bounded<S>(stream: &mut S)
where
    S: AsyncWrite + Unpin + Send,
{
    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
}

/// Serve one admitted sync batch request on a live network stream.
///
/// `epoch` is the node's own epoch and `max_frame` the frame limit of that
/// epoch, so the requester cannot size the responder's buffers (#1718). A
/// request for an epoch outside the node's serve window (see
/// [`serves_sync_epoch`]) is denied with [`DenyReason::Unavailable`] before the
/// `Ack` and any storage read; that is not a peer fault, so it ends as a
/// completed serve. Otherwise the digest set is capped at
/// [`MAX_BATCH_DIGESTS_PER_REQUEST`], the batches are streamed through
/// [`RequestHandler::process_sync_batches_stream`], and the result says how
/// the serve ended.
async fn serve_sync_request<DB: Database>(
    request_handler: &RequestHandler<DB>,
    peer: BlsPublicKey,
    mut stream: Stream,
    request: WorkerSyncRequest,
    epoch: Epoch,
    max_frame: usize,
    consensus_chain: &ConsensusChain,
) -> SyncServeOutcome {
    let WorkerSyncRequest::Batches { batch_digests, epoch: req_epoch } = request;
    // serve only the node's epoch window: deny anything else before the Ack and
    // any storage read (#1718). Log only, no penalty.
    if serves_sync_epoch(epoch, req_epoch) {
        // cap to the node's max
        let batch_digests: BTreeSet<B256> = if batch_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST {
            debug!(
                target: "worker::network",
                %peer,
                requested = batch_digests.len(),
                max = MAX_BATCH_DIGESTS_PER_REQUEST,
                "truncating oversized sync batch request"
            );
            batch_digests.into_iter().take(MAX_BATCH_DIGESTS_PER_REQUEST).collect()
        } else {
            batch_digests
        };
        request_handler
            .process_sync_batches_stream(
                peer,
                stream,
                batch_digests,
                req_epoch,
                max_frame,
                consensus_chain,
            )
            .await
    } else {
        debug!(
            target: "worker::network",
            %peer,
            req_epoch,
            epoch,
            "denying sync batch request for an unserved epoch"
        );
        deny_unserved_sync_epoch(&mut stream, max_frame).await;
        SyncServeOutcome::Completed
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

/// Reserve a shed slot and record the refusal that follows from the outcome.
///
/// A reserved slot means the stream gets a deny task, so it counts as
/// [`SyncShedReason::Denied`]. No slot means the stream is dropped with no reply,
/// so it counts as [`SyncShedReason::BudgetExhausted`]. Recording here, at the
/// decision, keeps the count exact when the best-effort deny write later fails
/// or the task never runs (#1307).
fn admit_shed_and_record(
    semaphore: &Arc<Semaphore>,
    metrics: &WorkerMetrics,
) -> Option<OwnedSemaphorePermit> {
    let permit = try_admit_shed(semaphore);
    let reason =
        permit.as_ref().map_or(SyncShedReason::BudgetExhausted, |_| SyncShedReason::Denied);
    metrics.record_sync_stream_shed(reason);
    permit
}

/// Resolve the shed counter for batch reports that `worker_id` drops at the per-peer cap.
///
/// The series is `tn_worker.batch_reports_shed_total` with reason `peer_cap`. It is resolved
/// once, so a shed adds to it without a registry lookup.
fn report_shed_counter(worker_id: WorkerId) -> metrics::Counter {
    metrics::counter!(
        "tn_worker.batch_reports_shed_total",
        "worker" => worker_id.to_string(),
        "reason" => "peer_cap",
    )
}

/// Try to admit one inbound batch report from `peer` under the per-peer report cap.
///
/// Takes a slot from `slots`, which caps each peer at [`MAX_PENDING_REQUESTS_PER_PEER`]
/// reports in validation. A refusal adds one to `shed` at the decision, in the style of
/// [`admit_shed_and_record`]. The returned slot frees its count on drop.
fn admit_report_and_record(
    slots: &PeerSlots,
    shed: &metrics::Counter,
    peer: BlsPublicKey,
) -> Option<PeerSlot> {
    slots.try_take(peer).or_else(|| {
        shed.increment(1);
        None
    })
}

/// Shed a denied inbound sync stream on a budgeted task.
///
/// Spawns a short-lived task that writes [`DenyReason::AtCapacity`] and closes,
/// so the requester fails fast and tries elsewhere. The spawn is gated by the
/// budget in `shed_semaphore` ([`MAX_CONCURRENT_SHED_TASKS`] slots): past that
/// budget the stream is dropped without spawning (the requester sees a reset),
/// so a burst of over-cap substreams cannot fan out to unbounded tasks (#1254).
/// A spawned task holds its budget slot for its lifetime, and the best-effort
/// deny write is bounded by [`SYNC_REQUEST_READ_TIMEOUT`], so a peer that never
/// reads returns the slot at the timeout.
///
/// Both arms record `tn_worker.sync_streams_shed_total` by [`SyncShedReason`]
/// at the admission decision through [`admit_shed_and_record`] (#1307),
/// before the best-effort deny task runs or its write can fail.
///
/// Generic over the stream, in the style of `send_sync_batches_over_stream`, so
/// unit tests drive the full shed path with in-memory writers and a paused
/// clock (#1314).
fn shed_sync_stream<S>(
    shed_semaphore: &Arc<Semaphore>,
    metrics: &WorkerMetrics,
    spawner: &TaskSpawner,
    epoch: Epoch,
    peer: BlsPublicKey,
    stream: S,
) where
    S: AsyncWrite + Unpin + Send + 'static,
{
    admit_shed_and_record(shed_semaphore, metrics).map_or_else(
        || {
            debug!(target: "worker::network", %peer, "dropping inbound sync stream: shed budget exhausted");
        },
        |shed_permit| {
            let task_name = format!("shed-sync-batches-{peer}");
            spawner.spawn_task(task_name, async move {
                // hold the shed budget slot for the lifetime of the task
                let _shed_permit = shed_permit;
                let mut stream = stream;
                let max_frame = max_sync_frame_size(epoch);
                let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
                debug!(target: "worker::network", %peer, "denying inbound sync stream: at capacity");
                // bound the best-effort shed write: a peer that applies receive
                // backpressure and never reads must not pin the shed budget slot.
                let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                    let _ = write_frame(
                        &mut stream,
                        &SyncFrame::<WorkerSyncRequest>::Deny(DenyReason::AtCapacity),
                        &mut encode_buffer,
                        &mut compressed_buffer,
                        max_frame,
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

/// Whether the node serves a sync batch request for `req_epoch`.
///
/// An honest requester names its own current epoch. The node serves its own
/// epoch and the previous one, for a requester still finishing the prior epoch
/// at the boundary. Any other epoch is denied before the `Ack`, so a peer cannot
/// make the node open older retained epoch packs or churn the pack cache
/// (#1718). At epoch 0 only epoch 0 is served.
fn serves_sync_epoch(node_epoch: Epoch, req_epoch: Epoch) -> bool {
    req_epoch == node_epoch || req_epoch == node_epoch.saturating_sub(1)
}

/// Deny an admitted sync request for an epoch the node does not serve.
///
/// Writes [`DenyReason::Unavailable`] and closes, with no `Ack` and no storage
/// read, so the requester tries another peer. The caller only logs the request;
/// there is no penalty. The write and close are bounded by
/// [`SYNC_REQUEST_READ_TIMEOUT`], so a requester that stops reading cannot pin
/// the admission permit the caller holds.
async fn deny_unserved_sync_epoch<S>(stream: &mut S, max_frame: usize)
where
    S: AsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
        let _ = write_frame(
            &mut *stream,
            &SyncFrame::<WorkerSyncRequest>::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
            max_frame,
        )
        .await;
        let _ = stream.close().await;
    })
    .await;
}

/// Handle inter-node communication between primaries.
#[derive(Debug)]
pub struct WorkerNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: WorkerNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// Opening-read pool for inbound sync streams.
    ///
    /// A stream holds its per-peer slot (capped at [`MAX_PENDING_REQUESTS_PER_PEER`])
    /// through both admission phases, so one peer cannot fill either pool, and an
    /// opening-read permit while its opening request frame is read, which
    /// [`SYNC_REQUEST_READ_TIMEOUT`] bounds: [`MAX_CONCURRENT_OPENING_READS`] public
    /// permits plus [`COMMITTEE_RESERVED_OPENING_READS`] for each committee key,
    /// which only that key may take. A public key holds opening permits within
    /// [`PUBLIC_OPENING_HOLD_BUDGET`] per [`PUBLIC_OPENING_HOLD_WINDOW`].
    sync_opening: OpeningAdmission,
    /// The batch-serve budgets for inbound sync streams: the pool, the per-key
    /// public serve cap and the public hold budget.
    ///
    /// A stream takes its grants after its request frame is read and holds them
    /// until the serve ends. The pool has [`MAX_CONCURRENT_BATCH_STREAMS`] public
    /// permits plus [`COMMITTEE_RESERVED_BATCH_STREAMS`] for each committee key,
    /// which only that key may take; a public key serves on at most
    /// [`PUBLIC_BATCH_SERVES_PER_PEER`] streams at a time and within
    /// [`PUBLIC_BATCH_SERVE_HOLD_BUDGET`] per [`PUBLIC_BATCH_SERVE_HOLD_WINDOW`].
    batch_serve: BatchServeAdmission,
    /// Per-peer in-flight batch report validations, capped at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`].
    ///
    /// A report holds its slot for the life of its validation task. A cancelled request does
    /// not release the slot early: the task runs to its end and only skips the reply. The cap
    /// is a per-peer slot, not a shared class budget: a shared pool lets k colluding committee
    /// keys hold 2k permits and starve honest reporters, while a per-peer slot bounds each key
    /// at 2 concurrent validations and other peers cannot exhaust it. The cap applies whatever
    /// the value of `serve_budget.batch` (#1774).
    report_peer_slots: PeerSlots,
    /// The `tn_worker.batch_reports_shed_total` series of this worker, resolved once.
    ///
    /// A report refused at [`Self::report_peer_slots`] adds one.
    report_shed: metrics::Counter,
    /// The committee keys (current and next committee) that admission treats as
    /// [`PeerTier::Committee`]; every other peer is [`PeerTier::Public`].
    reserved_peers: ReservedPeers,
    /// Semaphore bounding concurrent shed tasks for denied sync streams.
    ///
    /// A permit is held for the lifetime of a shed task (the bounded
    /// `Deny(AtCapacity)` write), so [`MAX_CONCURRENT_SHED_TASKS`] caps the
    /// spawn fan-out from over-cap substream bursts.
    shed_task_semaphore: Arc<Semaphore>,
    /// Prometheus metrics for the inbound sync stream admission path.
    ///
    /// Built from the worker id, like the [`RequestHandler`]'s instance: the registry
    /// keys a series by name and labels, so both record into the same per-worker
    /// series and no handle is threaded through [`Self::new`].
    metrics: WorkerMetrics,
    /// Access to the consensus chain.
    consensus_chain: ConsensusChain,
}

impl<DB, Events> WorkerNetwork<DB, Events>
where
    DB: Database,
    Events: TnReceiver<NetworkEvent<Req, Res>> + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: Events,
        network_handle: WorkerNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        consensus_chain: ConsensusChain,
    ) -> Self {
        // committee peers (this epoch's and the next) admit through the reserves
        let committee = consensus_config.committee().bls_keys();
        let next_committee = consensus_config.next_committee_keys().iter().copied();
        let reserved_peers = ReservedPeers::new(committee.into_iter().chain(next_committee));
        let request_handler =
            RequestHandler::new(id, validator, consensus_config, network_handle.clone());
        Self {
            network_events,
            network_handle,
            request_handler,
            sync_opening: OpeningAdmission::new(),
            batch_serve: BatchServeAdmission::new(),
            report_peer_slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
            report_shed: report_shed_counter(id),
            reserved_peers,
            shed_task_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS)),
            metrics: WorkerMetrics::new_for_worker(id),
            consensus_chain,
        }
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("worker network events", async move {
            loop {
                match self.network_events.recv().await {
                    Some(event) => {
                        self.process_network_event(event);
                    }
                    None => {
                        warn!(target: "worker::network", "critical worker network events channel dropped");
                        break Err(TaskError::from_message("critical worker network events channel dropped"));
                    }
                }
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::PeerExchange { .. } => {
                    // expect this is intercepted by network layer
                    warn!(target: "worker::network", "worker application received unexpected peer exchange message");
                }
            },
            NetworkEvent::Gossip(gossip) => {
                self.process_gossip(
                    gossip.message,
                    gossip.relayer,
                    gossip.author,
                    gossip.validation,
                );
            }
            NetworkEvent::Error(msg, channel) => {
                let err = WorkerResponse::Error(message::WorkerRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.network_handle.get_task_spawner().spawn_task(
                    "report request error",
                    async move {
                        let _ = network_handle.inner_handle().send_response(err, channel).await;
                        Ok(())
                    },
                );
            }
            NetworkEvent::InboundStream { peer, stream } => {
                self.process_inbound_sync_stream(peer, stream)
            }
        }
    }

    /// Process a new reported batch.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    ///
    /// Each report costs a full batch validation, so admission runs before the spawn, and
    /// the cheaper check runs first. A per-peer slot in [`Self::report_peer_slots`] bounds
    /// each key at [`MAX_PENDING_REQUESTS_PER_PEER`] concurrent validations, whatever the
    /// value of `serve_budget.batch`. The cap is a per-peer slot, not a shared class budget:
    /// a shared pool lets k colluding committee keys hold 2k permits and starve honest
    /// reporters, while a per-peer slot cannot be exhausted by other peers. The batch serve
    /// permit is then offered as before (see [`Self::serve_report_batch`]). A report refused
    /// at either check gets no reply, so the quorum waiter of the reporter retries after its
    /// backoff (#1774).
    fn process_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // Per-peer cap: admit the report before any other work for it. The shed is counted at
        // the decision under `tn_worker.batch_reports_shed_total`. A shed drops the response
        // channel and the cancel receiver with no reply, so the reporter retries after its
        // backoff.
        let digest = sealed_batch.digest();
        admit_report_and_record(&self.report_peer_slots, &self.report_shed, peer).map_or_else(
            || {
                debug!(target: "worker::network", %peer, %digest, "dropping batch report: peer at its report cap");
            },
            |report_slot| self.serve_report_batch(peer, sealed_batch, channel, cancel, report_slot),
        )
    }

    /// Offer an admitted batch report to the batch serve class and spawn its validation.
    ///
    /// `report_slot` is the per-peer slot that [`Self::process_report_batch`] took. It moves
    /// into the spawned task with the serve permit, so every exit of the task frees both. The
    /// task runs the report to its end, also when the network layer cancels the request, so
    /// both cover the blocking validation and the bookkeeping of the handler; a cancel only
    /// skips the reply. A refused offer drops the slot here.
    fn serve_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
        report_slot: PeerSlot,
    ) {
        // Serve budget: offer the request to the batch class before any work for it. `None`
        // means the class has no free permit; the shed is already counted under `serve_budget`.
        // Dropping the response channel and the cancel receiver sends no reply, so the swarm
        // also counts the request as an `omitted` failure. The default budget is unbounded, so
        // the class only observes until an operator sets `serve_budget.batch`. The per-peer cap
        // in `process_report_batch` bounds reports regardless of `serve_budget.batch`.
        if let Some(serve_permit) = self
            .network_handle
            .inner_handle()
            .serve_permits()
            .try_acquire(ServiceClass::Batch, peer)
        {
            // clone for spawned tasks
            let request_handler = self.request_handler.clone();
            let network_handle = self.network_handle.clone();
            let task_name = format!("process-report-batch-{}", sealed_batch.digest());
            self.network_handle.get_task_spawner().spawn_task(task_name, async move {
                // hold the per-peer report slot and the serve permit for the task's lifetime,
                // so every exit releases both
                let _report_slot = report_slot;
                let _serve_permit = serve_permit;
                // run to the end, so the slot and the permit cover the blocking validation and
                // the bookkeeping of the handler; a cancel from the network layer only skips
                // the reply
                let (res, reply) =
                    request_handler.run_report_to_completion(&peer, sealed_batch, cancel).await;
                let response = match res {
                    Ok(()) => WorkerResponse::ReportBatch,
                    Err(err) => {
                        // classify transient responder-side conditions as
                        // recoverable so the requester retries instead of
                        // treating this as a permanent rejection
                        let response = WorkerResponse::into_error_ref(&err);
                        if let Some(penalty) = err.into() {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                        response
                    }
                };
                if reply {
                    let _ = network_handle.inner_handle().send_response(response, channel).await;
                }
                Ok(())
            });
        }
    }

    /// Process gossip from a worker.
    ///
    /// `validation` is the gossipsub verdict ticket for the message (#1772).
    fn process_gossip(
        &self,
        msg: GossipMessage,
        relayer: Option<BlsPublicKey>,
        author: Option<BlsPublicKey>,
        mut validation: GossipValidation<Req, Res>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let relayer_label =
            relayer.as_ref().map_or_else(|| "unresolved".to_string(), |bls| bls.to_string());
        let task_name = format!("process-gossip-{relayer_label}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            // The batch fetch can take longer than the about 12 s gossipsub validation window,
            // so the worker accepts at delivery (#1772). Batch gossip forwards as before.
            validation.accept().await;
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                // Charge the accountable peer, and only once its BLS identity has resolved: the
                // author for a content-determined fault (#819, guaranteed resolved on the
                // restricted batch topic), the relaying peer for every other fault (#801).
                // `is_author_content_fault` is the shared classifier; `zip` skips the penalty when
                // that peer is unresolved or the error carries no penalty.
                let charged = if e.is_author_content_fault() { author } else { relayer };
                if let Some((peer, penalty)) = charged.zip(e.penalty()) {
                    network_handle.report_penalty(peer, penalty).await;
                }
                Err(e.into())
            } else {
                Ok(())
            }
        });
    }

    /// Process an inbound sync-protocol batch stream.
    ///
    /// The request travels in the opening [`SyncFrame::Req`] frame (no prior
    /// request-response ack), so admission starts here on stream open, ahead of
    /// any task spawn (#1254). Admission has two committee-aware phases (see
    /// [`admit_and_serve_sync_stream`]): a per-peer slot and an opening-read permit
    /// cover the bounded read of the request frame, and a batch-serve permit covers
    /// the serve. Committee peers (the current and the next committee) draw on
    /// reserves that public peers cannot take. A stream refused at the opening
    /// phase takes the shed path, which writes [`DenyReason::AtCapacity`] on its own
    /// task gated by the [`MAX_CONCURRENT_SHED_TASKS`] budget (see
    /// [`Self::shed_inbound_sync_stream`]).
    ///
    /// A request for an epoch outside the serve window of the node (see
    /// [`serves_sync_epoch`]) is denied with [`DenyReason::Unavailable`] before
    /// the `Ack` and any storage read (#1718). A served request uses the frame
    /// limit of the epoch of the node, not of the request epoch.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        // admit the opening phase before spawning; the slot and permit move into
        // the task and free capacity on drop. A refused stream takes the bounded
        // shed path instead of spawning here, so over-cap substream bursts cannot
        // fan out past the shed budget (#1254).
        let tier = self.reserved_peers.tier(&peer);
        let Some(admission) = try_admit_opening(&self.sync_opening, peer, tier) else {
            self.shed_inbound_sync_stream(peer, stream);
            return;
        };
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let batch_serve = self.batch_serve.clone();
        let network_handle = self.network_handle.clone();
        let epoch = self.network_handle.epoch();
        let max_frame = max_sync_frame_size(epoch);
        let task_name = format!("sync-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            admit_and_serve_sync_stream(
                stream,
                admission,
                batch_serve,
                network_handle,
                max_frame,
                move |stream, request| async move {
                    serve_sync_request(
                        &request_handler,
                        peer,
                        stream,
                        request,
                        epoch,
                        max_frame,
                        &consensus_chain,
                    )
                    .await
                },
            )
            .await;
            Ok(())
        });
    }

    /// Shed a denied inbound sync stream on a budgeted task.
    ///
    /// Thin wrapper over [`shed_sync_stream`]: binds the live network's
    /// [`Self::shed_task_semaphore`], task spawner, and current epoch to the
    /// generic shed path (#1314). See the free function for the budget and
    /// timeout bounds (#1254).
    fn shed_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        shed_sync_stream(
            &self.shed_task_semaphore,
            &self.metrics,
            self.network_handle.get_task_spawner(),
            self.network_handle.epoch(),
            peer,
            stream,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{network::handler::SEND_STREAM_FRAME_TIMEOUT, test_utils::setup_batch_db};

    use super::*;
    use futures::io::Cursor;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use parking_lot::Mutex;
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::{
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    use tempfile::TempDir;
    use tn_network_libp2p::types::NetworkCommand;
    use tn_types::{BlsKeypair, Committee, TaskManager};
    use tokio::sync::mpsc;

    // A single fixed peer suffices: every case exercises the per-peer cap for one
    // peer. `BlsPublicKey::default()` is the same key the crate's other unit tests
    // use for this purpose.
    fn peer() -> BlsPublicKey {
        BlsPublicKey::default()
    }

    /// Opening admission up to the per-peer cap succeeds; the next attempt is
    /// refused without keeping an opening-read permit, and dropping the admissions
    /// frees both the opening-read permits and the peer's slots.
    #[test]
    fn sync_admit_enforces_per_peer_cap_and_frees_on_drop() {
        let p = peer();
        let (opening, _batch_serve) = admission_budgets();
        let held = MAX_CONCURRENT_OPENING_READS.saturating_sub(MAX_PENDING_REQUESTS_PER_PEER);

        // admit up to the per-peer cap
        let admissions: Vec<_> = (0..MAX_PENDING_REQUESTS_PER_PEER)
            .map(|_| {
                try_admit_opening(&opening, p, PeerTier::Public)
                    .expect("admit below the per-peer cap")
            })
            .collect();
        assert_eq!(opening.slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER);
        assert_eq!(opening.pool.available_public(), held);

        // at the cap: the next admit is refused and keeps no opening-read permit
        assert!(try_admit_opening(&opening, p, PeerTier::Public).is_none());
        assert_eq!(opening.pool.available_public(), held);

        // dropping the admissions frees the opening-read permits and the peer's slots
        drop(admissions);
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.slots.in_flight(&p), 0);
    }

    /// Report admission up to the per-peer cap succeeds; the next report from the same peer
    /// sheds and counts once, a second peer is unaffected, and dropping a slot frees it for
    /// the next report. The slots are built as `WorkerNetwork::new` builds the report cap.
    #[test]
    fn report_admit_enforces_per_peer_cap_and_frees_on_drop() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let (p, other) = (key(1), key(2));
            let slots = PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER);
            let shed = report_shed_counter(0);

            // admit up to the per-peer cap
            let held: Vec<_> = (0..MAX_PENDING_REQUESTS_PER_PEER)
                .map(|_| {
                    admit_report_and_record(&slots, &shed, p).expect("admit below the per-peer cap")
                })
                .collect();
            assert_eq!(slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER);

            // at the cap: the next report from the same peer sheds and holds no slot
            assert!(admit_report_and_record(&slots, &shed, p).is_none());
            assert_eq!(slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER);

            // a second peer is unaffected by the full slots of the first
            let other_slot =
                admit_report_and_record(&slots, &shed, other).expect("second peer admitted");
            assert_eq!(slots.in_flight(&other), 1);

            // dropping one guard frees one slot, and the next report from the peer is admitted
            let rest: Vec<_> = held.into_iter().skip(1).collect();
            assert_eq!(slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER.saturating_sub(1));
            let reclaimed =
                admit_report_and_record(&slots, &shed, p).expect("freed slot admits a report");
            assert_eq!(slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER);

            // dropping every guard frees every slot
            drop((rest, reclaimed, other_slot));
            assert_eq!(slots.in_flight(&p), 0);
            assert_eq!(slots.in_flight(&other), 0);
        });

        // only the refusal at the cap counts as a shed
        let snapshot = snapshotter.snapshot().into_vec();
        let shed_total = snapshot
            .iter()
            .find(|(k, ..)| k.key().name() == "tn_worker.batch_reports_shed_total")
            .map(|(_, _, _, value)| value);
        assert!(matches!(shed_total, Some(DebugValue::Counter(1))), "one shed is counted");
    }

    // The shed budget admits exactly MAX_CONCURRENT_SHED_TASKS concurrent shed
    // tasks, denies the next, and frees a slot when a shed permit drops.
    #[test]
    fn shed_admit_enforces_budget_and_frees_on_drop() {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS));

        // fill the shed budget
        let permits: Vec<_> = (0..MAX_CONCURRENT_SHED_TASKS)
            .map(|_| try_admit_shed(&semaphore).expect("admit below the shed budget"))
            .collect();
        assert_eq!(semaphore.available_permits(), 0);

        // at the budget: the next shed spawn is refused (stream dropped, no task)
        assert!(try_admit_shed(&semaphore).is_none());

        // dropping a shed permit frees its slot for the next denied stream
        drop(permits);
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_SHED_TASKS);
        assert!(try_admit_shed(&semaphore).is_some());
    }

    // Both shed arms record their own reason: every budgeted slot counts as
    // `denied`, and the refusal past the budget counts as `budget_exhausted`.
    // The full shed path records before any spawned task can run. Bypassing
    // the recording helper or swapping its reasons fails this test.
    #[tokio::test(start_paused = true)]
    async fn shed_admit_records_reason_per_arm() {
        let task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS));
            let metrics = WorkerMetrics::new_for_worker(0);
            (0..MAX_CONCURRENT_SHED_TASKS).for_each(|_| {
                shed_sync_stream(&semaphore, &metrics, &spawner, 0, peer(), PendingWriter);
            });
            assert_eq!(semaphore.available_permits(), 0, "every budgeted slot admits");
            shed_sync_stream(&semaphore, &metrics, &spawner, 0, peer(), PendingWriter);
            assert_eq!(semaphore.available_permits(), 0, "the request past the budget is refused");
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let counted = |reason: &str, expected: usize| {
            snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == "tn_worker.sync_streams_shed_total"
                    && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
                    && matches!(
                        value,
                        DebugValue::Counter(n) if usize::try_from(*n).ok() == Some(expected)
                    )
            })
        };
        assert!(
            counted("denied", MAX_CONCURRENT_SHED_TASKS),
            "every budgeted slot records `denied`"
        );
        assert!(
            counted("budget_exhausted", 1),
            "the refusal past the budget records `budget_exhausted`"
        );
    }

    // A writer whose polls never complete, modeling a peer that applies receive
    // backpressure and never reads its deny reply. `poll_close` pends too, so
    // only the SYNC_REQUEST_READ_TIMEOUT bound can end a shed task.
    struct PendingWriter;

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    // The full shed path: each denied stream below the budget spawns a task
    // that holds its slot while it runs (the budget bounds live tasks, not
    // spawn attempts), a denied stream past the budget spawns nothing without
    // corrupting the accounting, and the SYNC_REQUEST_READ_TIMEOUT bound on
    // the deny write returns every slot even when the peer never reads.
    #[tokio::test(start_paused = true)]
    async fn shed_sync_stream_bounds_live_tasks_and_times_out_stuck_writes() {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS));
        let task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        let metrics = WorkerMetrics::new_for_worker(0);

        // fill the budget: every denied stream below the cap spawns a shed task
        (0..MAX_CONCURRENT_SHED_TASKS).for_each(|_| {
            shed_sync_stream(&semaphore, &metrics, &spawner, 0, peer(), PendingWriter)
        });
        // let each task start and block on the pending deny write, inside the
        // SYNC_REQUEST_READ_TIMEOUT bound. The permit is taken at admit time
        // (before the spawn); the timeout step below proves the running task is
        // what holds it.
        tokio::task::yield_now().await;
        assert_eq!(
            semaphore.available_permits(),
            0,
            "every slot is taken by an admitted shed task"
        );

        // over budget: no spawn, no negative accounting
        shed_sync_stream(&semaphore, &metrics, &spawner, 0, peer(), PendingWriter);
        tokio::task::yield_now().await;
        assert_eq!(semaphore.available_permits(), 0);

        // the bounded write trips SYNC_REQUEST_READ_TIMEOUT and every slot returns
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_SHED_TASKS);
    }

    // A writer that accepts every byte into a shared buffer and completes, so
    // the deny write finishes without the timeout.
    #[derive(Clone, Default)]
    struct CapturingWriter {
        /// The bytes the shed task wrote, shared with the test body.
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl AsyncWrite for CapturingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.bytes.lock().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // A successful shed task writes exactly one Deny(AtCapacity) frame (the
    // requester-side contract: fail fast and try elsewhere, not a reset) and
    // returns its budget slot without needing the timeout bound.
    #[tokio::test(start_paused = true)]
    async fn shed_sync_stream_writes_deny_at_capacity_and_frees_slot() {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS));
        let task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        let metrics = WorkerMetrics::new_for_worker(0);
        let writer = CapturingWriter::default();
        let bytes = writer.bytes.clone();

        shed_sync_stream(&semaphore, &metrics, &spawner, 0, peer(), writer);
        tokio::task::yield_now().await;

        // the task completed with no clock advance: the slot is already back
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_SHED_TASKS);

        // the captured bytes decode to the deny frame
        let written = bytes.lock().clone();
        let frame = read_frame::<_, WorkerSyncRequest>(
            &mut written.as_slice(),
            &mut Vec::new(),
            &mut Vec::new(),
            max_sync_frame_size(0),
        )
        .await
        .expect("captured bytes decode to a sync frame");
        assert!(
            matches!(frame, SyncFrame::Deny(DenyReason::AtCapacity)),
            "shed task writes Deny(AtCapacity), got {frame:?}"
        );
    }

    // The serve window is the node's epoch and the one before it; every other
    // request epoch is denied, and epoch 0 serves only epoch 0 (#1718).
    #[test]
    fn serves_sync_epoch_window() {
        let cases = [
            (5, 5, true),
            (5, 4, true),
            (5, 3, false),
            (5, 6, false),
            (5, 0, false),
            (0, 0, true),
            (0, 1, false),
            (0, u32::MAX, false),
        ];
        cases.iter().for_each(|&(node_epoch, req_epoch, expected)| {
            assert_eq!(
                serves_sync_epoch(node_epoch, req_epoch),
                expected,
                "node epoch {node_epoch}, request epoch {req_epoch}"
            );
        });
    }

    // A denied unserved epoch gets exactly one Deny(Unavailable) frame: no Ack,
    // no Data, so the responder never touches storage for it (#1718).
    #[tokio::test]
    async fn deny_unserved_sync_epoch_writes_only_deny_unavailable() {
        let mut writer = CapturingWriter::default();
        let max_frame = max_sync_frame_size(0);
        deny_unserved_sync_epoch(&mut writer, max_frame).await;

        let written = writer.bytes.lock().clone();
        let mut reader = written.as_slice();
        let frame = read_frame::<_, WorkerSyncRequest>(
            &mut reader,
            &mut Vec::new(),
            &mut Vec::new(),
            max_frame,
        )
        .await
        .expect("captured bytes decode to a sync frame");
        assert!(
            matches!(frame, SyncFrame::Deny(DenyReason::Unavailable)),
            "unserved epoch writes Deny(Unavailable), got {frame:?}"
        );
        assert!(reader.is_empty(), "nothing follows the deny frame");
    }

    // A requester that stops reading cannot pin the batch-serve permit through
    // the unserved-epoch deny: the SYNC_REQUEST_READ_TIMEOUT bound ends the write
    // and the permit returns (#1718).
    #[tokio::test(start_paused = true)]
    async fn deny_unserved_sync_epoch_is_bounded_and_frees_permit() {
        let (_opening, batch_serve) = admission_budgets();
        let permit =
            batch_serve.pool.try_acquire(&key(1), PeerTier::Public).expect("admit below the caps");

        // hold the permit on a task blocked on the pending deny write
        let task = tokio::spawn(async move {
            let _permit = permit;
            let mut stream = PendingWriter;
            deny_unserved_sync_epoch(&mut stream, max_sync_frame_size(0)).await;
        });
        tokio::task::yield_now().await;
        assert_eq!(
            batch_serve.pool.available_public(),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(1),
            "the blocked deny task holds its permit"
        );

        // the bounded write trips SYNC_REQUEST_READ_TIMEOUT and the permit returns
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT + Duration::from_secs(1)).await;
        task.await.expect("deny task completes");
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
    }

    /// A deterministic BLS public key for `seed`, for tests that need several
    /// distinct peer identities.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// The opening and batch-serve budgets, at the sizes the live network uses.
    fn admission_budgets() -> (OpeningAdmission, BatchServeAdmission) {
        (OpeningAdmission::new(), BatchServeAdmission::new())
    }

    /// A network handle whose commands (such as reported penalties) arrive on the
    /// returned receiver.
    fn observed_handle(
        task_manager: &TaskManager,
    ) -> (WorkerNetworkHandle, mpsc::Receiver<NetworkCommand<Req, Res>>) {
        let (sender, commands) = mpsc::channel(16);
        let spawner = task_manager.get_spawner();
        (WorkerNetworkHandle::new_for_test_with_sender(sender, spawner), commands)
    }

    /// The penalized peer of every command received so far: `Some(peer)` for a
    /// `Mild` penalty and `None` for any other command.
    fn drain_mild_penalties(
        commands: &mut mpsc::Receiver<NetworkCommand<Req, Res>>,
    ) -> Vec<Option<BlsPublicKey>> {
        std::iter::from_fn(|| commands.try_recv().ok()).map(mild_penalty_peer).collect()
    }

    /// The penalized peer if `command` reports a `Mild` penalty.
    fn mild_penalty_peer(command: NetworkCommand<Req, Res>) -> Option<BlsPublicKey> {
        if let NetworkCommand::ReportPenalty { peer, penalty: Penalty::Mild } = command {
            Some(peer)
        } else {
            None
        }
    }

    /// The wire bytes of an opening `Req(Batches)` frame for `batch_digests`.
    async fn encoded_request(batch_digests: BTreeSet<B256>) -> Vec<u8> {
        let mut bytes = Vec::new();
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(
            &mut bytes,
            &SyncFrame::Req(WorkerSyncRequest::Batches { batch_digests, epoch: 0 }),
            &mut encode_buffer,
            &mut compressed_buffer,
            max_sync_frame_size(0),
        )
        .await
        .expect("encode the opening request frame");
        bytes
    }

    /// The wire bytes of an opening `Ack` frame: a readable frame that is not a
    /// request.
    async fn encoded_ack() -> Vec<u8> {
        let mut bytes = Vec::new();
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(
            &mut bytes,
            &SyncFrame::<WorkerSyncRequest>::Ack,
            &mut encode_buffer,
            &mut compressed_buffer,
            max_sync_frame_size(0),
        )
        .await
        .expect("encode the opening ack frame");
        bytes
    }

    /// A serve step that counts its call and never finishes, so its batch-serve
    /// permit stays held for the rest of the test.
    fn pending_serve<S>(
        served: Arc<AtomicUsize>,
    ) -> impl FnOnce(S, WorkerSyncRequest) -> std::future::Pending<SyncServeOutcome> {
        move |_stream: S, _request: WorkerSyncRequest| {
            served.fetch_add(1, Ordering::SeqCst);
            std::future::pending()
        }
    }

    /// Drive one public peer's sync stream whose peer sends `wire` and then EOF,
    /// and whose writes and close stay pending, against `batch_serve`.
    ///
    /// Checks that the opening-read permit is free as soon as the opening read
    /// ends, while the task is still stuck on its bounded control write or close;
    /// that the per-peer slot lasts for the task; and that no serve runs. Returns
    /// the peer of every `Mild` penalty charged.
    async fn stuck_opening(
        wire: Vec<u8>,
        batch_serve: BatchServeAdmission,
    ) -> Vec<Option<BlsPublicKey>> {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, _batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let peer = key(1);
        let admission =
            try_admit_opening(&opening, peer, PeerTier::Public).expect("opening read admitted");
        let task = tokio::spawn(admit_and_serve_sync_stream(
            RequestStream::new(wire, WriteCredit::Withheld),
            admission,
            batch_serve,
            handle,
            max_sync_frame_size(0),
            pending_serve(served.clone()),
        ));

        // the opening read has ended; the bounded control write or close is pending
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            opening.pool.available_public(),
            MAX_CONCURRENT_OPENING_READS,
            "the opening-read permit is free once the read ends"
        );
        assert!(!task.is_finished(), "the bounded control write or close is still pending");
        assert_eq!(opening.slots.in_flight(&peer), 1, "the per-peer slot lasts for the task");

        // past the write and close bounds: the task ends and frees the slot
        tokio::time::advance(
            SYNC_REQUEST_READ_TIMEOUT.saturating_mul(2).saturating_add(Duration::from_millis(1)),
        )
        .await;
        task.await.expect("stuck opening task");
        assert_eq!(opening.slots.in_flight(&peer), 0);
        assert_eq!(served.load(Ordering::SeqCst), 0, "no stuck opening reaches the serve phase");
        drain_mild_penalties(&mut commands)
    }

    /// Whether a test stream accepts the responder's writes.
    #[derive(Clone, Copy, Debug)]
    enum WriteCredit {
        /// Every write, flush, and close completes at once.
        Granted,
        /// Every write, flush, and close stays pending forever.
        Withheld,
    }

    impl WriteCredit {
        /// Complete an operation with `value` if credit is granted, else stay pending.
        fn complete<T>(self, value: T) -> Poll<std::io::Result<T>> {
            match self {
                Self::Granted => Poll::Ready(Ok(value)),
                Self::Withheld => Poll::Pending,
            }
        }
    }

    /// A sync stream whose peer has sent one opening request frame (then EOF) and
    /// either accepts or withholds credit for the responder's writes.
    struct RequestStream {
        /// The bytes the peer sent.
        request: Cursor<Vec<u8>>,
        /// Whether the responder's writes complete.
        credit: WriteCredit,
    }

    impl RequestStream {
        /// A stream that carries the encoded `request` bytes.
        fn new(request: Vec<u8>, credit: WriteCredit) -> Self {
            Self { request: Cursor::new(request), credit }
        }
    }

    impl AsyncRead for RequestStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.get_mut().request).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for RequestStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.credit.complete(buf.len())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.credit.complete(())
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.credit.complete(())
        }
    }

    /// A sync stream whose peer never sends its opening request frame; writes and
    /// closes complete at once.
    struct SilentStream;

    impl AsyncRead for SilentStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for SilentStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A sync stream whose peer sends its opening bytes only after a delay; writes
    /// and closes complete at once.
    struct DelayedStream {
        /// The delay before the peer's bytes become readable.
        delay: Pin<Box<tokio::time::Sleep>>,
        /// The bytes the peer sends once the delay has passed.
        wire: Cursor<Vec<u8>>,
    }

    impl DelayedStream {
        /// A stream that makes `wire` readable once `delay` has passed.
        fn new(delay: Duration, wire: Vec<u8>) -> Self {
            Self { delay: Box::pin(tokio::time::sleep(delay)), wire: Cursor::new(wire) }
        }
    }

    impl AsyncRead for DelayedStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            std::task::ready!(this.delay.as_mut().poll(cx));
            Pin::new(&mut this.wire).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DelayedStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Public peers can fill only the public share of the opening-read pool: past
    /// it a new public peer is refused. A committee key is still admitted from its
    /// own reserve until that reserve is empty, and then it is refused. A second
    /// committee key keeps its whole reserve and is still admitted (issue #1750).
    #[test]
    fn public_opening_reads_cannot_exhaust_the_committee_reserve() {
        let (opening, _batch_serve) = admission_budgets();
        let (committee, other) = (key(200), key(201));

        // one opening read each from as many public peers as the public share holds
        let public: Vec<_> = (0_u64..)
            .take(MAX_CONCURRENT_OPENING_READS)
            .map(|seed| {
                try_admit_opening(&opening, key(seed), PeerTier::Public)
                    .expect("public opening read within the public share")
            })
            .collect();
        assert_eq!(opening.pool.available_public(), 0);
        assert!(
            try_admit_opening(&opening, key(100), PeerTier::Public).is_none(),
            "a new public peer is refused once the public share is full"
        );
        assert_eq!(opening.pool.available_reserved(&committee), COMMITTEE_RESERVED_OPENING_READS);

        // one committee key opens streams from its own reserve until it is empty
        let drained: Vec<_> = std::iter::repeat_with(|| {
            try_admit_opening(&opening, committee, PeerTier::Committee)
                .expect("committee opening read from its own reserve")
        })
        .take(COMMITTEE_RESERVED_OPENING_READS)
        .collect();
        assert_eq!(opening.pool.available_reserved(&committee), 0);
        assert!(
            try_admit_opening(&opening, committee, PeerTier::Committee).is_none(),
            "a committee key is refused once its own reserve and the public share are full"
        );

        // a second committee key keeps its whole reserve and is still admitted
        assert_eq!(opening.pool.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
        let admitted = try_admit_opening(&opening, other, PeerTier::Committee)
            .expect("a second committee key opens from its own reserve");
        assert_eq!(
            opening.pool.available_reserved(&other),
            COMMITTEE_RESERVED_OPENING_READS.saturating_sub(1)
        );

        drop((public, drained, admitted));
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.pool.available_reserved(&committee), COMMITTEE_RESERVED_OPENING_READS);
        assert_eq!(opening.pool.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
    }

    /// Five public identities, each at the per-peer cap, open ten streams with
    /// readable requests while every serve stays pending. One stream of each
    /// identity is served and the five fill the public share of the batch-serve
    /// pool; the other stream of each is refused at the per-key serve cap. None of
    /// them can take a committee reserve, so a committee request is still
    /// admitted and served from the reserve of its key. A second committee key
    /// keeps its whole reserve, and its request is served too (issue #1750).
    #[tokio::test(start_paused = true)]
    async fn five_public_identities_cannot_block_a_committee_batches_request() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let (committee, other) = (key(0), key(6));
        let reserved = ReservedPeers::new([committee, other]);
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;

        // ten streams: two from each of five public identities; the public opening-read share
        // admits eight reads at once, so the fifth identity opens once the first four have read
        // their requests and released their opening-read permits
        let open_pair = |seed: u64| -> Vec<tokio::task::JoinHandle<()>> {
            std::iter::repeat_n(key(seed), MAX_PENDING_REQUESTS_PER_PEER)
                .map(|public| {
                    let admission = try_admit_opening(&opening, public, reserved.tier(&public))
                        .expect("public opening read admitted");
                    tokio::spawn(admit_and_serve_sync_stream(
                        RequestStream::new(request.clone(), WriteCredit::Granted),
                        admission,
                        batch_serve.clone(),
                        handle.clone(),
                        max_frame,
                        pending_serve(served.clone()),
                    ))
                })
                .collect()
        };
        let first_wave: Vec<_> = (1_u64..=4).flat_map(&open_pair).collect();
        tokio::time::sleep(Duration::from_millis(1)).await;
        let tasks: Vec<_> = first_wave.into_iter().chain(open_pair(5)).collect();
        // let every task read its request and reach its serve or its refusal
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(
            served.load(Ordering::SeqCst),
            MAX_CONCURRENT_BATCH_STREAMS,
            "the public share admits one serve per public key"
        );
        assert_eq!(batch_serve.pool.available_public(), 0, "the public share is full");
        assert_eq!(
            batch_serve.pool.available_reserved(&committee),
            COMMITTEE_RESERVED_BATCH_STREAMS,
            "public peers never take a committee reserve"
        );
        assert_eq!(
            tasks.iter().filter(|task| task.is_finished()).count(),
            MAX_CONCURRENT_BATCH_STREAMS,
            "the second stream of each public key is refused at the serve phase"
        );
        assert_eq!(
            opening.pool.available_public(),
            MAX_CONCURRENT_OPENING_READS,
            "served and refused streams hold no opening-read permit"
        );

        // the committee request is admitted and served from the reserve
        let admission = try_admit_opening(&opening, committee, reserved.tier(&committee))
            .expect("committee opening read admitted");
        let committee_task = tokio::spawn(admit_and_serve_sync_stream(
            RequestStream::new(request.clone(), WriteCredit::Granted),
            admission,
            batch_serve.clone(),
            handle.clone(),
            max_frame,
            pending_serve(served.clone()),
        ));
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(
            served.load(Ordering::SeqCst),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_add(1),
            "the committee request is served"
        );
        assert_eq!(
            batch_serve.pool.available_reserved(&committee),
            COMMITTEE_RESERVED_BATCH_STREAMS.saturating_sub(1),
            "the committee serve holds a permit from the reserve of its key"
        );
        assert!(!committee_task.is_finished(), "the committee serve is still in flight");

        // a second committee key keeps its whole reserve, so its request is served too
        assert_eq!(
            batch_serve.pool.available_reserved(&other),
            COMMITTEE_RESERVED_BATCH_STREAMS,
            "one committee key never takes the reserve of another"
        );
        let admission = try_admit_opening(&opening, other, reserved.tier(&other))
            .expect("second committee opening read admitted");
        let other_task = tokio::spawn(admit_and_serve_sync_stream(
            RequestStream::new(request, WriteCredit::Granted),
            admission,
            batch_serve.clone(),
            handle.clone(),
            max_frame,
            pending_serve(served.clone()),
        ));
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(
            served.load(Ordering::SeqCst),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_add(2),
            "the second committee request is served"
        );
        assert_eq!(
            batch_serve.pool.available_reserved(&other),
            COMMITTEE_RESERVED_BATCH_STREAMS.saturating_sub(1),
            "the second committee serve holds a permit from the reserve of its key"
        );
        assert!(!other_task.is_finished(), "the second committee serve is still in flight");
        assert!(drain_mild_penalties(&mut commands).is_empty(), "no stream was charged");
    }

    /// Three public peers each open a sync stream a quarter of the public
    /// opening-read bound apart and never send the request frame. Each stream's
    /// opening-read permit returns at `opening_read_bound(PeerTier::Public)` after
    /// that stream opened, each peer is charged one `Mild` penalty at that point,
    /// and no stream reaches the serve phase.
    #[tokio::test(start_paused = true)]
    async fn silent_public_streams_free_opening_permits_and_are_charged_once() {
        let step = opening_read_bound(PeerTier::Public) / 4;
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let (first_peer, second_peer, third_peer) = (key(1), key(2), key(3));
        let open = |peer: BlsPublicKey| {
            let admission = try_admit_opening(&opening, peer, PeerTier::Public)
                .expect("silent peer's opening read admitted");
            tokio::spawn(admit_and_serve_sync_stream(
                SilentStream,
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_sync_frame_size(0),
                pending_serve(served.clone()),
            ))
        };

        // open the three streams at t = 0, 1 and 2 steps (0, 250 and 500 ms)
        let first = open(first_peer);
        tokio::task::yield_now().await;
        tokio::time::advance(step).await;
        let second = open(second_peer);
        tokio::task::yield_now().await;
        tokio::time::advance(step).await;
        let third = open(third_peer);
        tokio::task::yield_now().await;
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS.saturating_sub(3));

        // t = 4.5 steps (1125 ms): only the first stream's read window has closed
        tokio::time::advance(step * 2 + step / 2).await;
        first.await.expect("first silent stream task");
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(first_peer)]);
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS.saturating_sub(2));

        // t = 6.5 steps (1625 ms): the other two read windows have closed
        tokio::time::advance(step * 2).await;
        second.await.expect("second silent stream task");
        third.await.expect("third silent stream task");
        let charged = drain_mild_penalties(&mut commands);
        assert_eq!(charged.len(), 2, "each remaining silent peer is charged once");
        assert!(charged.contains(&Some(second_peer)) && charged.contains(&Some(third_peer)));

        assert_eq!(served.load(Ordering::SeqCst), 0, "no silent stream reaches the serve phase");
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert!([first_peer, second_peer, third_peer]
            .iter()
            .all(|p| opening.slots.in_flight(p) == 0));
    }

    /// A peer that gives the stream no write credit cannot pin the `Ack` write: it
    /// fails with `TimedOut` at its bound, not before and not much after.
    #[tokio::test(start_paused = true)]
    async fn ack_write_with_zero_credit_times_out_within_bound() {
        let started = tokio::time::Instant::now();
        let error = stream_codec::write_ack_frame_bounded(
            &mut PendingWriter,
            max_sync_frame_size(0),
            SYNC_REQUEST_READ_TIMEOUT,
        )
        .await
        .expect_err("a writer with no credit never takes the ack");
        let elapsed = started.elapsed();

        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert!(
            elapsed >= SYNC_REQUEST_READ_TIMEOUT
                && elapsed < SYNC_REQUEST_READ_TIMEOUT.saturating_add(Duration::from_millis(10)),
            "the ack write fails at its bound, took {elapsed:?}"
        );
    }

    /// A public peer sends a readable request and then gives the stream no write
    /// credit. The serve's `Ack` write is bounded by `SYNC_REQUEST_READ_TIMEOUT`,
    /// so the batch-serve permit is still held just before that bound, returns
    /// just after it, and the peer is charged one `Mild` penalty for the stall.
    #[tokio::test(start_paused = true)]
    async fn stalled_ack_write_returns_serve_permit_at_bound() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        let db = setup_batch_db(&[]);
        let peer = key(1);
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let admission =
            try_admit_opening(&opening, peer, PeerTier::Public).expect("opening read admitted");
        let one_held = MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(1);

        let task = tokio::spawn(admit_and_serve_sync_stream(
            RequestStream::new(request, WriteCredit::Withheld),
            admission,
            batch_serve.clone(),
            handle,
            max_frame,
            move |mut stream, request| async move {
                let WorkerSyncRequest::Batches { batch_digests, epoch } = request;
                let served = stream_codec::send_sync_batches_over_stream(
                    &mut stream,
                    &db,
                    &consensus_chain,
                    &batch_digests,
                    epoch,
                    max_frame,
                    SEND_STREAM_FRAME_TIMEOUT,
                )
                .await;
                SyncServeOutcome::of(&served)
            },
        ));
        tokio::task::yield_now().await;
        assert_eq!(batch_serve.pool.available_public(), one_held, "the serve holds a permit");

        // just inside the ack bound: the permit is still held
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT.saturating_sub(Duration::from_millis(1)))
            .await;
        tokio::task::yield_now().await;
        assert_eq!(batch_serve.pool.available_public(), one_held, "held inside the ack bound");

        // just past the ack bound: the write fails and the permit returns
        tokio::time::advance(Duration::from_millis(2)).await;
        task.await.expect("serve task");
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(opening.slots.in_flight(&peer), 0);
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(peer)]);
    }

    /// A serve that ended on a timeout, either the whole-stream limit
    /// (`Timeout(Elapsed)`) or a per-frame limit (`StdIo` of kind `TimedOut`), or on
    /// a requester that stopped or closed the stream (`StdIo` of kind
    /// `ConnectionReset` or `NotConnected`), is a stall; every other error is a
    /// failure, and success is a completion.
    #[tokio::test(start_paused = true)]
    async fn sync_serve_outcome_classifies_stalls_by_variant_and_kind() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("a pending future times out");
        let io = |kind: ErrorKind| WorkerNetworkError::StdIo(std::io::Error::new(kind, "test"));
        let cases: [(WorkerNetworkResult<()>, SyncServeOutcome); 8] = [
            (Ok(()), SyncServeOutcome::Completed),
            (Err(WorkerNetworkError::Timeout(elapsed)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::TimedOut)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::ConnectionReset)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::NotConnected)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::BrokenPipe)), SyncServeOutcome::Failed),
            (Err(WorkerNetworkError::Internal("storage".to_string())), SyncServeOutcome::Failed),
            (Err(WorkerNetworkError::StreamClosed), SyncServeOutcome::Failed),
        ];
        cases.iter().for_each(|(served, expected)| {
            assert_eq!(SyncServeOutcome::of(served), *expected, "classifying {served:?}");
        });
    }

    /// A public peer whose opening frame cannot be read (here the stream ends
    /// before any frame) frees its opening-read permit as soon as the read ends,
    /// while the bounded close is still pending, and is charged one `Mild` penalty.
    #[tokio::test(start_paused = true)]
    async fn unreadable_openings_free_the_budget_and_are_charged() {
        let (_opening, batch_serve) = admission_budgets();
        assert_eq!(stuck_opening(Vec::new(), batch_serve).await, vec![Some(key(1))]);
    }

    /// A public peer that opens with a readable frame that is not a request (here
    /// `Ack`) frees its opening-read permit as soon as the read ends, while the
    /// bounded `Err(Malformed)` write and close are still pending, and is charged
    /// one `Mild` penalty.
    #[tokio::test(start_paused = true)]
    async fn unexpected_openings_free_the_budget_and_are_charged() {
        let (_opening, batch_serve) = admission_budgets();
        assert_eq!(stuck_opening(encoded_ack().await, batch_serve).await, vec![Some(key(1))]);
    }

    /// A public request refused with `Deny(AtCapacity)`, because the public share
    /// of the batch-serve pool is full, frees its opening-read permit as soon as
    /// the read ends and is not charged: an honest peer gets this answer when the
    /// share is full.
    #[tokio::test(start_paused = true)]
    async fn at_capacity_openings_free_the_budget_without_a_charge() {
        let (_opening, batch_serve) = admission_budgets();
        let held: Vec<_> = (0..MAX_CONCURRENT_BATCH_STREAMS)
            .map(|_| {
                batch_serve
                    .pool
                    .try_acquire(&key(1), PeerTier::Public)
                    .expect("public batch-serve permit")
            })
            .collect();
        assert_eq!(batch_serve.pool.available_public(), 0, "the public share is full");
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        assert!(
            stuck_opening(request, batch_serve.clone()).await.is_empty(),
            "an AtCapacity refusal is not charged"
        );
        // the held permits keep the public share full for the whole exchange
        drop(held);
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
    }

    /// A valid request that arrives in the second half of the public window is
    /// admitted and served, but charged. So four public identities, each at the
    /// per-peer cap, cannot park all eight public opening-read permits for free.
    #[tokio::test(start_paused = true)]
    async fn late_valid_public_openings_are_charged() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let late = PUBLIC_SYNC_REQUEST_READ_TIMEOUT - Duration::from_millis(1);
        let peers: Vec<_> = (1_u64..=4).map(key).collect();
        let committee = key(0);

        // eight streams: two from each of four public identities
        let tasks: Vec<_> = peers
            .iter()
            .flat_map(|peer| std::iter::repeat_n(*peer, MAX_PENDING_REQUESTS_PER_PEER))
            .map(|public| {
                let admission = try_admit_opening(&opening, public, PeerTier::Public)
                    .expect("public opening read admitted");
                let served = served.clone();
                tokio::spawn(admit_and_serve_sync_stream(
                    DelayedStream::new(late, request.clone()),
                    admission,
                    batch_serve.clone(),
                    handle.clone(),
                    max_frame,
                    move |_stream: DelayedStream, _request: WorkerSyncRequest| {
                        served.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(SyncServeOutcome::Completed)
                    },
                ))
            })
            .collect();
        assert_eq!(opening.pool.available_public(), 0, "the late openings fill the public share");

        // every request arrives just inside the public read bound
        tokio::task::yield_now().await;
        tokio::time::advance(late).await;
        futures::future::join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("late opening tasks");

        assert_eq!(served.load(Ordering::SeqCst), 8, "every late request is served");
        let charged = drain_mild_penalties(&mut commands);
        assert_eq!(charged.len(), 8, "every late request is charged once");
        peers.iter().for_each(|peer| {
            assert_eq!(
                charged.iter().filter(|charge| **charge == Some(*peer)).count(),
                MAX_PENDING_REQUESTS_PER_PEER,
                "each late stream of a peer is charged"
            );
        });
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(
            batch_serve.pool.available_reserved(&committee),
            COMMITTEE_RESERVED_BATCH_STREAMS
        );
        assert!(peers.iter().all(|peer| opening.slots.in_flight(peer) == 0));
    }

    /// A public serve that is still running at `PUBLIC_BATCH_SERVE_TIMEOUT` is cut
    /// there: its pool permit, serve slot and peer slot return, and the peer is
    /// charged one `Mild` penalty. A committee serve started at the same time keeps
    /// running on its reserve.
    #[tokio::test(start_paused = true)]
    async fn public_batch_serve_is_cut_at_the_hold_limit_and_charged() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let (public, committee) = (key(1), key(0));
        let reserved = ReservedPeers::new([committee]);
        let open = |peer: BlsPublicKey| {
            let admission = try_admit_opening(&opening, peer, reserved.tier(&peer))
                .expect("opening read admitted");
            tokio::spawn(admit_and_serve_sync_stream(
                RequestStream::new(request.clone(), WriteCredit::Granted),
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_frame,
                pending_serve(served.clone()),
            ))
        };

        let public_task = open(public);
        let committee_task = open(committee);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 2, "both serves are running");
        assert_eq!(
            batch_serve.pool.available_public(),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(1),
            "the public serve holds a public permit"
        );
        assert_eq!(batch_serve.public_serves.in_flight(&public), 1);

        // just inside the hold limit: the public serve still runs
        tokio::time::advance(PUBLIC_BATCH_SERVE_TIMEOUT.saturating_sub(Duration::from_millis(2)))
            .await;
        tokio::task::yield_now().await;
        assert!(!public_task.is_finished(), "the public serve runs inside the hold limit");

        // at the limit: the public serve is cut, every grant returns, and the key is charged
        tokio::time::advance(Duration::from_millis(2)).await;
        public_task.await.expect("public serve task");
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(batch_serve.public_serves.in_flight(&public), 0);
        assert_eq!(opening.slots.in_flight(&public), 0);
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(public)]);

        // the committee serve keeps running on its reserve
        assert!(!committee_task.is_finished(), "a committee serve has no hold limit");
        assert_eq!(
            batch_serve.pool.available_reserved(&committee),
            COMMITTEE_RESERVED_BATCH_STREAMS.saturating_sub(1),
            "the committee serve still holds its reserve permit"
        );
    }

    /// A public key at the per-peer stream cap opens two streams with readable
    /// requests: one is served and the other is refused at the serve phase without
    /// a charge, so one public key holds one batch-serve permit at a time. A
    /// committee key serves on both of its streams.
    #[tokio::test(start_paused = true)]
    async fn a_public_key_holds_one_batch_serve_permit_at_a_time() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let (public, committee) = (key(1), key(0));
        let reserved = ReservedPeers::new([committee]);
        let open_pair = |peer: BlsPublicKey| -> Vec<tokio::task::JoinHandle<()>> {
            (0..MAX_PENDING_REQUESTS_PER_PEER)
                .map(|_| {
                    let admission = try_admit_opening(&opening, peer, reserved.tier(&peer))
                        .expect("opening read admitted");
                    tokio::spawn(admit_and_serve_sync_stream(
                        RequestStream::new(request.clone(), WriteCredit::Granted),
                        admission,
                        batch_serve.clone(),
                        handle.clone(),
                        max_frame,
                        pending_serve(served.clone()),
                    ))
                })
                .collect()
        };

        let public_tasks = open_pair(public);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 1, "one stream of the public key is served");
        assert_eq!(
            public_tasks.iter().filter(|task| task.is_finished()).count(),
            1,
            "the other stream of the public key is refused at the serve phase"
        );
        assert_eq!(
            batch_serve.pool.available_public(),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(1),
            "the refused stream holds no pool permit"
        );
        assert_eq!(batch_serve.public_serves.in_flight(&public), PUBLIC_BATCH_SERVES_PER_PEER);
        assert!(drain_mild_penalties(&mut commands).is_empty(), "a per-key refusal is not charged");

        let committee_tasks = open_pair(committee);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 3, "a committee key serves on both streams");
        assert!(committee_tasks.iter().all(|task| !task.is_finished()));
        assert!(drain_mild_penalties(&mut commands).is_empty(), "no committee stream is charged");
    }

    /// A public key whose serve was cut at the hold limit has used its whole hold
    /// budget: its next request is refused `AtCapacity` without a charge while a
    /// fresh key is served, and it is served again once the window has refilled
    /// its budget.
    #[tokio::test(start_paused = true)]
    async fn a_public_key_that_used_its_hold_budget_is_refused_until_it_refills() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let (spent, fresh) = (key(1), key(2));
        let open = |peer: BlsPublicKey| {
            let admission =
                try_admit_opening(&opening, peer, PeerTier::Public).expect("opening read admitted");
            tokio::spawn(admit_and_serve_sync_stream(
                RequestStream::new(request.clone(), WriteCredit::Granted),
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_frame,
                pending_serve(served.clone()),
            ))
        };

        // the first serve runs to the hold limit and is cut there
        let first = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 1, "the first request is served");
        tokio::time::advance(PUBLIC_BATCH_SERVE_TIMEOUT).await;
        first.await.expect("first serve task");
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(spent)]);

        // the key has no hold budget left: refused at once, uncharged, and the pool is whole
        let refused = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(refused.is_finished(), "the spent key is refused at the serve phase");
        assert_eq!(served.load(Ordering::SeqCst), 1, "the refused request is not served");
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "a hold-budget refusal is not charged"
        );
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(opening.slots.in_flight(&spent), 0);

        // a fresh key is served
        let other = open(fresh);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 2, "a fresh key is served");
        assert!(!other.is_finished());

        // the window refills the budget and the spent key is served again
        tokio::time::advance(PUBLIC_BATCH_SERVE_HOLD_WINDOW.saturating_add(Duration::from_secs(1)))
            .await;
        let again = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 3, "the spent key is served after the window");
        assert!(!again.is_finished());
    }

    /// A public key whose serve completed just inside the hold limit is charged the time it was
    /// served, and with less than a full hold of budget left it is refused, uncharged, until that
    /// charge has leaked: 58 s served means 290 s refused. A fresh key is served meanwhile. So a
    /// key never gets two full holds back to back (GHSA-pg7j-w22m-3qj5, review round 2).
    #[tokio::test(start_paused = true)]
    async fn a_public_key_that_completed_a_serve_near_the_limit_waits_for_the_refill() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let (spent, fresh) = (key(1), key(2));
        let near_limit = PUBLIC_BATCH_SERVE_TIMEOUT.saturating_sub(Duration::from_secs(2));
        let open = |peer: BlsPublicKey| {
            let admission =
                try_admit_opening(&opening, peer, PeerTier::Public).expect("opening read admitted");
            let served = served.clone();
            tokio::spawn(admit_and_serve_sync_stream(
                RequestStream::new(request.clone(), WriteCredit::Granted),
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_frame,
                move |_stream: RequestStream, _request: WorkerSyncRequest| async move {
                    served.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(near_limit).await;
                    SyncServeOutcome::Completed
                },
            ))
        };

        // the first serve completes 2 s inside the hold limit and is not charged a penalty
        let first = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 1, "the first request is served");
        tokio::time::advance(near_limit).await;
        first.await.expect("first serve task");
        assert!(drain_mild_penalties(&mut commands).is_empty(), "a completed serve is free");

        // 2 s of budget is short of a full hold: refused at once, uncharged, and the pool is whole
        let refused = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(refused.is_finished(), "the spent key is refused at the serve phase");
        assert_eq!(served.load(Ordering::SeqCst), 1, "the refused request is not served");
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "a hold-budget refusal is not charged"
        );
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(opening.slots.in_flight(&spent), 0);

        // a fresh key is served
        let other = open(fresh);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 2, "a fresh key is served");
        assert!(!other.is_finished());

        // 289 s later the 58 s charge has not fully leaked: still refused
        tokio::time::advance(Duration::from_secs(289)).await;
        let early = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(early.is_finished(), "the spent key is refused before its charge has leaked");
        assert_eq!(served.load(Ordering::SeqCst), 2, "the early request is not served");

        // one more second (plus the milliseconds the checks took) and the key is served again
        tokio::time::advance(Duration::from_millis(1_100)).await;
        let again = open(spent);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 3, "the spent key is served after the refill");
        assert!(!again.is_finished());
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "nothing in this exchange is charged"
        );
    }

    /// Filling the public batch-serve share for a whole window needs 25 keys: five rounds of five
    /// fresh keys, each held to the hold limit, over 300 s. A sixth key in any round finds the
    /// share full, every key used so far is refused by its hold budget, and a key is granted
    /// again only 300 s after its own round (GHSA-pg7j-w22m-3qj5, review round 2).
    #[tokio::test(start_paused = true)]
    async fn filling_the_public_share_for_a_window_needs_twenty_five_keys() {
        /// Grant one round of fresh keys, hold them to the limit, release them, and check that
        /// every key used so far is refused. Returns every key used so far.
        async fn round(
            batch_serve: &BatchServeAdmission,
            used: Vec<BlsPublicKey>,
            seeds: std::ops::Range<u64>,
        ) -> Vec<BlsPublicKey> {
            let fresh: Vec<BlsPublicKey> = seeds.map(key).collect();
            let grants: Vec<ServeGrant> = fresh
                .iter()
                .map(|peer| {
                    batch_serve
                        .try_grant(peer, PeerTier::Public, Instant::now())
                        .expect("a fresh key is granted while the share has room")
                })
                .collect();
            assert_eq!(grants.len(), MAX_CONCURRENT_BATCH_STREAMS);
            assert!(
                matches!(
                    batch_serve.try_grant(&key(1_000), PeerTier::Public, Instant::now()),
                    Err(ServeRefusal::PoolFull)
                ),
                "a sixth fresh key finds the public share full"
            );
            tokio::time::advance(PUBLIC_BATCH_SERVE_TIMEOUT).await;
            grants.into_iter().for_each(ServeGrant::release);
            let used: Vec<BlsPublicKey> = used.into_iter().chain(fresh).collect();
            assert!(
                used.iter().all(|peer| matches!(
                    batch_serve.try_grant(peer, PeerTier::Public, Instant::now()),
                    Err(ServeRefusal::HoldBudget)
                )),
                "every key used so far is refused by its hold budget"
            );
            used
        }

        let batch_serve = BatchServeAdmission::new();
        let used = round(&batch_serve, Vec::new(), 1..6).await;
        let used = round(&batch_serve, used, 6..11).await;
        let used = round(&batch_serve, used, 11..16).await;
        let used = round(&batch_serve, used, 16..21).await;
        let used = round(&batch_serve, used, 21..26).await;
        assert_eq!(used.len(), 25, "five rounds of five keys fill the share for 300 s");

        // 300 s after their round the first five keys are back; the second five are not
        tokio::time::advance(PUBLIC_BATCH_SERVE_TIMEOUT).await;
        let regranted: Vec<ServeGrant> = used
            .iter()
            .take(5)
            .map(|peer| {
                batch_serve
                    .try_grant(peer, PeerTier::Public, Instant::now())
                    .expect("a key is granted again once its window has passed")
            })
            .collect();
        assert_eq!(regranted.len(), 5);
        assert!(used.iter().skip(5).take(5).all(|peer| matches!(
            batch_serve.try_grant(peer, PeerTier::Public, Instant::now()),
            Err(ServeRefusal::HoldBudget)
        )));
    }

    /// A public key that re-opens streams one after another, holding each opening permit for
    /// just under half the read bound (below the late charge), is admitted only while it has a
    /// full read bound of its opening budget left, and is then shed, uncharged. Its admitted
    /// opening time lands between the budget less one hold and the budget. A fresh public key
    /// and a committee key are still admitted, and the key is admitted again after the window
    /// (GHSA-pg7j-w22m-3qj5, review round 2; the opening-phase residual of GHSA-vr4r-x9mm-6x9g).
    #[tokio::test(start_paused = true)]
    async fn public_opening_holds_are_budgeted_per_key() {
        use futures::StreamExt as _;
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let spender = key(1);
        let hold = (PUBLIC_SYNC_REQUEST_READ_TIMEOUT / 2).saturating_sub(Duration::from_millis(10));
        let (opening_ref, batch_serve_ref, handle_ref, request_ref) =
            (&opening, &batch_serve, &handle, &request);
        // one opening: admitted or not, the task (if any) reads after `hold` and completes
        let open_once = move || async move {
            let task = try_admit_opening(opening_ref, spender, PeerTier::Public).map(|admission| {
                tokio::spawn(admit_and_serve_sync_stream(
                    DelayedStream::new(hold, request_ref.clone()),
                    admission,
                    batch_serve_ref.clone(),
                    handle_ref.clone(),
                    max_frame,
                    |_stream: DelayedStream, _request: WorkerSyncRequest| {
                        std::future::ready(SyncServeOutcome::Completed)
                    },
                ))
            });
            tokio::task::yield_now().await;
            tokio::time::advance(hold).await;
            futures::future::OptionFuture::from(task)
                .await
                .map(|done| done.expect("opening task"))
                .is_some()
        };

        // open until the opening budget sheds the key, at most 40 times
        let admitted = futures::stream::iter(0..40u32)
            .then(|_| open_once())
            .take_while(|admitted| std::future::ready(*admitted))
            .fold(0usize, |count, _| std::future::ready(count + 1))
            .await;
        assert!(admitted < 40, "the opening budget sheds the key");
        assert!(admitted >= 10, "the budget admits many honest-sized openings");
        let opening_time = hold.saturating_mul(u32::try_from(admitted).expect("count fits"));
        assert!(
            opening_time
                >= PUBLIC_OPENING_HOLD_BUDGET.saturating_sub(PUBLIC_SYNC_REQUEST_READ_TIMEOUT),
            "the key gets its budget less one reserved hold of opening time"
        );
        assert!(opening_time <= PUBLIC_OPENING_HOLD_BUDGET, "never more than the budget");
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "openings under half the bound are not late, and a shed opening is not charged"
        );
        assert_eq!(opening.pool.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.slots.in_flight(&spender), 0);

        // other keys are admitted at that instant
        let fresh = try_admit_opening(&opening, key(2), PeerTier::Public);
        let committee = try_admit_opening(&opening, key(3), PeerTier::Committee);
        assert!(fresh.is_some(), "a fresh public key is admitted");
        assert!(committee.is_some(), "a committee key is admitted");
        drop((fresh, committee));

        // the window refills the opening budget and the key is admitted again
        tokio::time::advance(PUBLIC_OPENING_HOLD_WINDOW).await;
        assert_eq!(
            opening.public_hold.remaining(&spender, Instant::now()),
            PUBLIC_OPENING_HOLD_BUDGET
        );
        assert!(try_admit_opening(&opening, spender, PeerTier::Public).is_some());
    }

    /// Three public keys, the GHSA-pg7j-w22m-3qj5 pattern, open five streams
    /// with readable requests while every serve stays pending: one serve per key
    /// runs and the other streams are refused, so the public share is never full.
    /// The three serves are cut at the hold limit and charged, and the keys cannot
    /// re-open at once: each is refused for the window while a fourth key is
    /// served.
    #[tokio::test(start_paused = true)]
    async fn slow_public_readers_cannot_hold_the_public_share_for_the_window() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let keys: Vec<_> = (1_u64..=3).map(key).collect();
        let open = |peer: BlsPublicKey| {
            let admission =
                try_admit_opening(&opening, peer, PeerTier::Public).expect("opening read admitted");
            tokio::spawn(admit_and_serve_sync_stream(
                RequestStream::new(request.clone(), WriteCredit::Granted),
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_frame,
                pending_serve(served.clone()),
            ))
        };

        // five streams: two from each of the first two keys and one from the third
        let tasks: Vec<_> = [1_u64, 1, 2, 2, 3].into_iter().map(key).map(&open).collect();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 3, "one serve per key runs");
        assert_eq!(
            tasks.iter().filter(|task| task.is_finished()).count(),
            2,
            "the second stream of a key is refused at the serve phase"
        );
        assert_eq!(
            batch_serve.pool.available_public(),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(3),
            "the public share is never full"
        );
        assert!(drain_mild_penalties(&mut commands).is_empty(), "no stream is charged yet");

        // the three serves are cut at the hold limit and each key is charged once
        tokio::time::advance(PUBLIC_BATCH_SERVE_TIMEOUT).await;
        futures::future::join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("serve tasks");
        let charged = drain_mild_penalties(&mut commands);
        assert_eq!(charged.len(), 3, "each cut serve is charged once");
        assert!(keys.iter().all(|peer| charged.contains(&Some(*peer))));
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);

        // the keys have used their hold budgets: re-opening at once is refused, uncharged
        let reopened: Vec<_> = keys.iter().map(|peer| open(*peer)).collect();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(reopened.iter().all(|task| task.is_finished()), "every spent key is refused");
        assert_eq!(served.load(Ordering::SeqCst), 3, "no spent key is served");
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "a hold-budget refusal is not charged"
        );
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);

        // a fourth key is served
        let fourth = open(key(4));
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(served.load(Ordering::SeqCst), 4, "a fresh key is served");
        assert!(!fourth.is_finished());
    }

    /// A request that names more digests than `MAX_BATCH_DIGESTS_PER_REQUEST` is
    /// handed to the serve whole, which truncates it, and a public requester is
    /// charged one `Mild` penalty for it; a committee requester is not.
    #[tokio::test(start_paused = true)]
    async fn oversized_public_requests_are_served_truncated_and_charged() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (opening, batch_serve) = admission_budgets();
        let max_frame = max_sync_frame_size(0);
        let digest = |seed: usize| {
            let mut bytes = [0_u8; 32];
            bytes.iter_mut().zip(seed.to_be_bytes()).for_each(|(byte, seed)| *byte = seed);
            B256::from(bytes)
        };
        let oversized = MAX_BATCH_DIGESTS_PER_REQUEST.saturating_add(1);
        let digests: BTreeSet<B256> = (0..oversized).map(digest).collect();
        assert_eq!(digests.len(), oversized, "every digest is distinct");
        let request = encoded_request(digests).await;
        let (public, committee) = (key(1), key(0));
        let reserved = ReservedPeers::new([committee]);
        let requested = Arc::new(AtomicUsize::new(0));
        let open = |peer: BlsPublicKey| {
            let admission = try_admit_opening(&opening, peer, reserved.tier(&peer))
                .expect("opening read admitted");
            let requested = requested.clone();
            tokio::spawn(admit_and_serve_sync_stream(
                RequestStream::new(request.clone(), WriteCredit::Granted),
                admission,
                batch_serve.clone(),
                handle.clone(),
                max_frame,
                move |_stream: RequestStream, sync_request: WorkerSyncRequest| {
                    requested.store(requested_digests(&sync_request), Ordering::SeqCst);
                    std::future::ready(SyncServeOutcome::Completed)
                },
            ))
        };

        // a public requester is served the whole request and charged once
        open(public).await.expect("public serve task");
        assert_eq!(
            requested.load(Ordering::SeqCst),
            oversized,
            "the serve sees every digest and truncates the request itself"
        );
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(public)]);
        assert_eq!(batch_serve.pool.available_public(), MAX_CONCURRENT_BATCH_STREAMS);

        // a committee requester is served and not charged
        open(committee).await.expect("committee serve task");
        assert_eq!(requested.load(Ordering::SeqCst), oversized);
        assert!(
            drain_mild_penalties(&mut commands).is_empty(),
            "a committee peer is never charged"
        );
    }

    /// The write errors quinn reports when the requester stops the stream
    /// (`ConnectionReset`) or closes it (`NotConnected`) after the serve began are
    /// stalls, as is a per-frame `TimedOut`; other I/O error kinds are failures.
    #[test]
    fn a_stopped_or_closed_stream_after_the_ack_is_a_stall() {
        let classify = |kind: ErrorKind| {
            SyncServeOutcome::of_error(&WorkerNetworkError::StdIo(std::io::Error::from(kind)))
        };
        [ErrorKind::ConnectionReset, ErrorKind::NotConnected, ErrorKind::TimedOut]
            .into_iter()
            .for_each(|kind| assert_eq!(classify(kind), SyncServeOutcome::Stalled, "{kind:?}"));
        [ErrorKind::BrokenPipe, ErrorKind::InvalidData]
            .into_iter()
            .for_each(|kind| assert_eq!(classify(kind), SyncServeOutcome::Failed, "{kind:?}"));
    }
}
