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
    read_frame, types::NetworkEvent, write_frame, DenyReason, GossipMessage, PeerSlot, PeerSlots,
    PeerTier, Penalty, ReservedPeers, ReservedPool, ResponseChannel, ServiceClass, Stream,
    SyncFrame, SyncFrameError, WorkerSyncRequest,
};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BatchValidation, BlsPublicKey, Database, Epoch, SealedBatch, TaskError, TaskSpawner,
    TnReceiver, WorkerId, B256,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
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
/// Committee peers (the current and the next committee) draw first on a separate
/// reserve of `COMMITTEE_RESERVED_BATCH_STREAMS` permits that public peers cannot
/// take, so public peers that fill this share cannot block a committee request.
pub const MAX_CONCURRENT_BATCH_STREAMS: usize = 5;

/// Batch-serve permits reserved for committee peers.
///
/// A committee peer takes a permit from this reserve first and then from the
/// [`MAX_CONCURRENT_BATCH_STREAMS`] public share. A public peer never takes one.
pub(crate) const COMMITTEE_RESERVED_BATCH_STREAMS: usize = 4;

/// Public share of the opening-read pool for inbound sync streams.
///
/// A permit is taken when the stream opens and held while the opening request
/// frame is read, which [`SYNC_REQUEST_READ_TIMEOUT`] bounds. A served stream
/// returns the permit before the serve phase starts; a stream that is not served
/// keeps it only for its bounded refusal or error write and close.
pub(crate) const MAX_CONCURRENT_OPENING_READS: usize = 8;

/// Opening-read permits reserved for committee peers.
///
/// A committee peer takes a permit from this reserve first and then from the
/// [`MAX_CONCURRENT_OPENING_READS`] public share. A public peer never takes one.
pub(crate) const COMMITTEE_RESERVED_OPENING_READS: usize = 4;

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
/// and starve its peers.
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
/// admitted tasks (each holds an opening-read or a batch-serve permit, see
/// [`MAX_CONCURRENT_OPENING_READS`] and [`MAX_CONCURRENT_BATCH_STREAMS`], each
/// with its committee reserve) plus this many shed tasks.
pub(crate) const MAX_CONCURRENT_SHED_TASKS: usize = 8;

/// Timeout for reading the opening request frame of an inbound sync stream.
///
/// A peer that opens a sync stream but never sends its request frame trips this
/// and the stream is dropped, so it cannot hold an admission slot indefinitely.
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How a served sync batch exchange ended.
///
/// The admission layer uses this to decide on a penalty: only a stall, where the
/// peer stopped reading and kept a batch-serve permit busy until a time limit
/// expired, is charged to a public peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncServeOutcome {
    /// Every frame was written and the stream ended with `End`.
    Completed,
    /// A write time limit expired: a per-frame limit or the whole-stream limit.
    Stalled,
    /// The serve failed for a reason that is not a stall, such as a storage error.
    Failed,
}

impl SyncServeOutcome {
    /// Classify the result of a sync batch serve.
    ///
    /// A whole-stream timeout (`Timeout(Elapsed)`) and a per-frame timeout (an
    /// `StdIo` error of kind [`ErrorKind::TimedOut`]) are stalls. Every other
    /// error is a failure.
    pub(crate) fn of(served: &WorkerNetworkResult<()>) -> Self {
        served.as_ref().map_or_else(Self::of_error, |_| Self::Completed)
    }

    /// Classify one serve error as a stall or a failure.
    fn of_error(error: &WorkerNetworkError) -> Self {
        match error {
            WorkerNetworkError::Timeout(_) => Self::Stalled,
            WorkerNetworkError::StdIo(io) if io.kind() == ErrorKind::TimedOut => Self::Stalled,
            WorkerNetworkError::StdIo(_)
            | WorkerNetworkError::Bcs(_)
            | WorkerNetworkError::BatchValidation(_)
            | WorkerNetworkError::NonCommitteeBatch
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

/// An inbound sync stream admitted to the opening phase.
///
/// Holds the peer's [`PeerSlot`] for the whole exchange and an opening-read
/// permit. The permit is released when the stream reaches the serve phase, so a
/// long serve does not hold opening-read capacity.
#[derive(Debug)]
struct SyncAdmission {
    /// The peer that opened the stream.
    peer: BlsPublicKey,
    /// The admission tier of the peer.
    tier: PeerTier,
    /// The per-peer in-flight slot, held through both phases.
    slot: PeerSlot,
    /// The opening-read permit, held until the serve phase or the end of the exchange.
    opening: OwnedSemaphorePermit,
}

/// Try to admit the opening phase of one inbound sync stream for `peer`.
///
/// Takes a per-peer slot first, so a peer at [`MAX_PENDING_REQUESTS_PER_PEER`]
/// never holds a pool permit, then an opening-read permit for `tier`. A committee
/// peer draws on the reserve before the public share; a public peer draws only on
/// the public share. Returns `None` when either budget is full, and a slot taken
/// before a refusal is released on drop.
fn try_admit_opening(
    slots: &PeerSlots,
    opening: &ReservedPool,
    peer: BlsPublicKey,
    tier: PeerTier,
) -> Option<SyncAdmission> {
    slots.try_take(peer).and_then(|slot| {
        opening.try_acquire(tier).map(|permit| SyncAdmission { peer, tier, slot, opening: permit })
    })
}

/// The result of the bounded read of an opening sync request frame.
#[derive(Debug)]
enum OpeningRead {
    /// No frame arrived within [`SYNC_REQUEST_READ_TIMEOUT`].
    Silent,
    /// The frame could not be read or decoded.
    Unreadable(std::io::Error),
    /// A request frame, with the batch-serve permit that admits it.
    Admitted(WorkerSyncRequest, OwnedSemaphorePermit),
    /// A request frame, refused because the batch-serve pool is full for the tier.
    AtCapacity,
    /// A readable frame that is not a request.
    Unexpected,
}

impl OpeningRead {
    /// Classify a decoded opening frame.
    ///
    /// A request takes a permit from `batch_serve` for `tier` here, before the
    /// caller releases the opening-read permit, so the stream always holds one of
    /// the two.
    fn from_frame(
        frame: SyncFrame<WorkerSyncRequest>,
        batch_serve: &ReservedPool,
        tier: PeerTier,
    ) -> Self {
        match frame {
            SyncFrame::Req(request) => batch_serve
                .try_acquire(tier)
                .map_or(Self::AtCapacity, |permit| Self::Admitted(request, permit)),
            SyncFrame::Ack
            | SyncFrame::Deny(_)
            | SyncFrame::Data(_)
            | SyncFrame::End
            | SyncFrame::Err(_) => Self::Unexpected,
        }
    }
}

/// Run one inbound sync exchange through both admission phases.
///
/// The opening phase reads the request frame under the opening-read permit in
/// `admission`, bounded by [`SYNC_REQUEST_READ_TIMEOUT`]. A public peer that sends
/// no frame in that time is charged [`Penalty::Mild`]; a committee peer is not.
///
/// The serve phase needs a permit from `batch_serve` for the peer's tier. A
/// committee peer draws on the reserve before the public share, so public peers
/// cannot use up the capacity that a committee request needs. With a permit, the
/// opening-read permit is released and `serve` runs; a
/// [`SyncServeOutcome::Stalled`] serve charges a public peer [`Penalty::Mild`].
/// Without one, the stream gets a bounded [`DenyReason::AtCapacity`] write.
///
/// The stream holds an opening-read or a batch-serve permit, and its per-peer
/// slot, until this returns. Generic over the stream and the serve step, so unit
/// tests drive both phases with in-memory streams and a paused clock.
async fn admit_and_serve_sync_stream<S, F, Fut>(
    mut stream: S,
    admission: SyncAdmission,
    batch_serve: ReservedPool,
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
    let read = tokio::time::timeout(
        SYNC_REQUEST_READ_TIMEOUT,
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
            OpeningRead::from_frame(frame, &batch_serve, tier)
        })
    });

    match read {
        OpeningRead::Silent => {
            // free the opening-read permit before the penalty send and the close
            drop(opening);
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
            warn!(target: "worker::network", %peer, ?error, "unreadable sync request frame");
            close_stream_bounded(&mut stream).await;
        }
        OpeningRead::Admitted(request, serve_permit) => {
            // the serve phase holds the batch-serve permit: free the opening read
            drop(opening);
            let outcome = serve(stream, request).await;
            // free the serve permit before the penalty send so a stall cannot hold it
            drop(serve_permit);
            match outcome {
                SyncServeOutcome::Stalled => charge_public_peer(&network_handle, peer, tier).await,
                SyncServeOutcome::Completed | SyncServeOutcome::Failed => {}
            }
        }
        OpeningRead::AtCapacity => {
            debug!(
                target: "worker::network",
                %peer,
                ?tier,
                "batch-serve pool is full, denying inbound sync stream"
            );
            write_control_frame_and_close(
                &mut stream,
                &SyncFrame::Deny(DenyReason::AtCapacity),
                max_frame,
            )
            .await;
        }
        OpeningRead::Unexpected => {
            // a well-behaved requester always opens with `Req`; anything else is
            // malformed. Signal it and drop (no penalty).
            warn!(
                target: "worker::network",
                %peer,
                "unexpected opening sync frame from requester"
            );
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
/// Called when a peer kept a sync permit busy without progress: it sent no
/// request frame in time, or it stopped reading during a serve. A committee peer
/// is not charged, because load on either side can slow an honest committee peer.
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
            warn!(
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
    /// A stream holds a permit while its opening request frame is read, which
    /// [`SYNC_REQUEST_READ_TIMEOUT`] bounds: [`MAX_CONCURRENT_OPENING_READS`] public
    /// permits plus [`COMMITTEE_RESERVED_OPENING_READS`] for committee peers only.
    sync_opening: ReservedPool,
    /// Batch-serve pool for inbound sync streams.
    ///
    /// A stream takes a permit after its request frame is read and holds it until
    /// the serve ends: [`MAX_CONCURRENT_BATCH_STREAMS`] public permits plus
    /// [`COMMITTEE_RESERVED_BATCH_STREAMS`] for committee peers only.
    batch_serve: ReservedPool,
    /// Per-peer in-flight sync streams, capped at [`MAX_PENDING_REQUESTS_PER_PEER`].
    ///
    /// A stream holds its slot through both admission phases, so one peer cannot
    /// fill either pool.
    sync_peer_slots: PeerSlots,
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
            sync_opening: ReservedPool::new(
                MAX_CONCURRENT_OPENING_READS,
                COMMITTEE_RESERVED_OPENING_READS,
            ),
            batch_serve: ReservedPool::new(
                MAX_CONCURRENT_BATCH_STREAMS,
                COMMITTEE_RESERVED_BATCH_STREAMS,
            ),
            sync_peer_slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
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
                self.process_gossip(gossip.message, gossip.relayer, gossip.author);
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
    fn process_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // Serve budget: offer the request to the batch class before any work for it. `None`
        // means the class has no free permit; the shed is already counted under `serve_budget`.
        // Dropping the response channel and the cancel receiver sends no reply, so the swarm
        // also counts the request as an `omitted` failure. The default budget is unbounded, so
        // this only observes until an operator sets `serve_budget.batch`.
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
                // hold the serve permit for the task's lifetime, so every exit releases it
                let _serve_permit = serve_permit;
                tokio::select! {
                    res = request_handler.process_report_batch(&peer, sealed_batch) => {
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
                        let _ = network_handle.inner_handle().send_response(response, channel).await;
                    },
                    // cancel notification from network layer
                    _ = cancel => (),
                }
                Ok(())
            });
        }
    }

    /// Process gossip from a worker.
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
        let task_name = format!("process-gossip-{relayer_label}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
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
        let Some(admission) =
            try_admit_opening(&self.sync_peer_slots, &self.sync_opening, peer, tier)
        else {
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
        let (slots, opening, _batch_serve) = admission_budgets();
        let held = MAX_CONCURRENT_OPENING_READS.saturating_sub(MAX_PENDING_REQUESTS_PER_PEER);

        // admit up to the per-peer cap
        let admissions: Vec<_> = (0..MAX_PENDING_REQUESTS_PER_PEER)
            .map(|_| {
                try_admit_opening(&slots, &opening, p, PeerTier::Public)
                    .expect("admit below the per-peer cap")
            })
            .collect();
        assert_eq!(slots.in_flight(&p), MAX_PENDING_REQUESTS_PER_PEER);
        assert_eq!(opening.available_public(), held);

        // at the cap: the next admit is refused and keeps no opening-read permit
        assert!(try_admit_opening(&slots, &opening, p, PeerTier::Public).is_none());
        assert_eq!(opening.available_public(), held);

        // dropping the admissions frees the opening-read permits and the peer's slots
        drop(admissions);
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(slots.in_flight(&p), 0);
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
        let (_slots, _opening, batch_serve) = admission_budgets();
        let permit = batch_serve.try_acquire(PeerTier::Public).expect("admit below the caps");

        // hold the permit on a task blocked on the pending deny write
        let task = tokio::spawn(async move {
            let _permit = permit;
            let mut stream = PendingWriter;
            deny_unserved_sync_epoch(&mut stream, max_sync_frame_size(0)).await;
        });
        tokio::task::yield_now().await;
        assert_eq!(
            batch_serve.available_public(),
            MAX_CONCURRENT_BATCH_STREAMS.saturating_sub(1),
            "the blocked deny task holds its permit"
        );

        // the bounded write trips SYNC_REQUEST_READ_TIMEOUT and the permit returns
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT + Duration::from_secs(1)).await;
        task.await.expect("deny task completes");
        assert_eq!(batch_serve.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
    }

    /// A deterministic BLS public key for `seed`, for tests that need several
    /// distinct peer identities.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// The per-peer slots, opening-read pool, and batch-serve pool, at the sizes
    /// the live network uses.
    fn admission_budgets() -> (PeerSlots, ReservedPool, ReservedPool) {
        (
            PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
            ReservedPool::new(MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS),
            ReservedPool::new(MAX_CONCURRENT_BATCH_STREAMS, COMMITTEE_RESERVED_BATCH_STREAMS),
        )
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

    /// Public peers can fill only the public share of the opening-read pool: past
    /// it a new public peer is refused, while committee peers are still admitted
    /// from their reserve until it is full too.
    #[test]
    fn public_opening_reads_cannot_exhaust_the_committee_reserve() {
        let (slots, opening, _batch_serve) = admission_budgets();

        // one opening read each from as many public peers as the public share holds
        let public: Vec<_> = (0_u64..)
            .take(MAX_CONCURRENT_OPENING_READS)
            .map(|seed| {
                try_admit_opening(&slots, &opening, key(seed), PeerTier::Public)
                    .expect("public opening read within the public share")
            })
            .collect();
        assert_eq!(opening.available_public(), 0);
        assert!(
            try_admit_opening(&slots, &opening, key(100), PeerTier::Public).is_none(),
            "a new public peer is refused once the public share is full"
        );
        assert_eq!(opening.available_reserved(), COMMITTEE_RESERVED_OPENING_READS);

        // committee peers still open streams, drawing on the reserve
        let committee: Vec<_> = (200_u64..)
            .take(COMMITTEE_RESERVED_OPENING_READS)
            .map(|seed| {
                try_admit_opening(&slots, &opening, key(seed), PeerTier::Committee)
                    .expect("committee opening read from the reserve")
            })
            .collect();
        assert_eq!(opening.available_reserved(), 0);
        assert!(
            try_admit_opening(&slots, &opening, key(300), PeerTier::Committee).is_none(),
            "a committee peer is refused once both the reserve and the public share are full"
        );

        drop((public, committee));
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.available_reserved(), COMMITTEE_RESERVED_OPENING_READS);
    }

    /// Three public identities, each at the per-peer cap, open six streams with
    /// readable requests while every serve stays pending. Five streams fill the
    /// public share of the batch-serve pool and the sixth is refused, but none of
    /// them can take the committee reserve, so a committee request is still
    /// admitted and served.
    #[tokio::test(start_paused = true)]
    async fn three_public_identities_cannot_block_a_committee_batches_request() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (slots, opening, batch_serve) = admission_budgets();
        let committee = key(0);
        let reserved = ReservedPeers::new([committee]);
        let served = Arc::new(AtomicUsize::new(0));
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;

        // six streams: two from each of three public identities
        let tasks: Vec<_> = (1_u64..=3)
            .flat_map(|seed| std::iter::repeat_n(key(seed), MAX_PENDING_REQUESTS_PER_PEER))
            .map(|public| {
                let admission = try_admit_opening(&slots, &opening, public, reserved.tier(&public))
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
            .collect();
        // let every task read its request and reach its serve or its refusal
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(
            served.load(Ordering::SeqCst),
            MAX_CONCURRENT_BATCH_STREAMS,
            "the public share admits five public serves"
        );
        assert_eq!(batch_serve.available_public(), 0, "the public share is full");
        assert_eq!(
            batch_serve.available_reserved(),
            COMMITTEE_RESERVED_BATCH_STREAMS,
            "public peers never take the committee reserve"
        );
        assert_eq!(
            tasks.iter().filter(|task| task.is_finished()).count(),
            1,
            "the sixth public stream is refused at the serve phase"
        );
        assert_eq!(
            opening.available_public(),
            MAX_CONCURRENT_OPENING_READS,
            "served and refused streams hold no opening-read permit"
        );

        // the committee request is admitted and served from the reserve
        let admission = try_admit_opening(&slots, &opening, committee, reserved.tier(&committee))
            .expect("committee opening read admitted");
        let committee_task = tokio::spawn(admit_and_serve_sync_stream(
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
            MAX_CONCURRENT_BATCH_STREAMS.saturating_add(1),
            "the committee request is served"
        );
        assert_eq!(
            batch_serve.available_reserved(),
            COMMITTEE_RESERVED_BATCH_STREAMS.saturating_sub(1),
            "the committee serve holds a reserved permit"
        );
        assert!(!committee_task.is_finished(), "the committee serve is still in flight");
        assert!(drain_mild_penalties(&mut commands).is_empty(), "no stream was charged");
    }

    /// Three public peers each open a sync stream one second apart and never
    /// send the request frame. Each stream's opening-read permit returns at
    /// `SYNC_REQUEST_READ_TIMEOUT` after that stream opened, each peer is charged
    /// one `Mild` penalty at that point, and no stream reaches the serve phase.
    #[tokio::test(start_paused = true)]
    async fn silent_public_streams_free_opening_permits_and_are_charged_once() {
        let task_manager = TaskManager::default();
        let (handle, mut commands) = observed_handle(&task_manager);
        let (slots, opening, batch_serve) = admission_budgets();
        let served = Arc::new(AtomicUsize::new(0));
        let (first_peer, second_peer, third_peer) = (key(1), key(2), key(3));
        let open = |peer: BlsPublicKey| {
            let admission = try_admit_opening(&slots, &opening, peer, PeerTier::Public)
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

        // open the three streams at t = 0 s, 1 s, and 2 s
        let first = open(first_peer);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let second = open(second_peer);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let third = open(third_peer);
        tokio::task::yield_now().await;
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS.saturating_sub(3));

        // t = 5.5 s: only the first stream's read window has closed
        tokio::time::advance(Duration::from_millis(3500)).await;
        first.await.expect("first silent stream task");
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(first_peer)]);
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS.saturating_sub(2));

        // t = 7.5 s: the other two read windows have closed
        tokio::time::advance(Duration::from_secs(2)).await;
        second.await.expect("second silent stream task");
        third.await.expect("third silent stream task");
        let charged = drain_mild_penalties(&mut commands);
        assert_eq!(charged.len(), 2, "each remaining silent peer is charged once");
        assert!(charged.contains(&Some(second_peer)) && charged.contains(&Some(third_peer)));

        assert_eq!(served.load(Ordering::SeqCst), 0, "no silent stream reaches the serve phase");
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(batch_serve.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert!([first_peer, second_peer, third_peer].iter().all(|p| slots.in_flight(p) == 0));
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
        let (slots, opening, batch_serve) = admission_budgets();
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain");
        let db = setup_batch_db(&[]);
        let peer = key(1);
        let max_frame = max_sync_frame_size(0);
        let request = encoded_request(BTreeSet::from([B256::default()])).await;
        let admission = try_admit_opening(&slots, &opening, peer, PeerTier::Public)
            .expect("opening read admitted");
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
        assert_eq!(batch_serve.available_public(), one_held, "the serve holds a permit");

        // just inside the ack bound: the permit is still held
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT.saturating_sub(Duration::from_millis(1)))
            .await;
        tokio::task::yield_now().await;
        assert_eq!(batch_serve.available_public(), one_held, "held inside the ack bound");

        // just past the ack bound: the write fails and the permit returns
        tokio::time::advance(Duration::from_millis(2)).await;
        task.await.expect("serve task");
        assert_eq!(batch_serve.available_public(), MAX_CONCURRENT_BATCH_STREAMS);
        assert_eq!(slots.in_flight(&peer), 0);
        assert_eq!(drain_mild_penalties(&mut commands), vec![Some(peer)]);
    }

    /// A serve that ended on a timeout, either the whole-stream limit
    /// (`Timeout(Elapsed)`) or a per-frame limit (`StdIo` of kind `TimedOut`), is
    /// a stall; every other error is a failure, and success is a completion.
    #[tokio::test(start_paused = true)]
    async fn sync_serve_outcome_classifies_stalls_by_variant_and_kind() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("a pending future times out");
        let io = |kind: ErrorKind| WorkerNetworkError::StdIo(std::io::Error::new(kind, "test"));
        let cases: [(WorkerNetworkResult<()>, SyncServeOutcome); 6] = [
            (Ok(()), SyncServeOutcome::Completed),
            (Err(WorkerNetworkError::Timeout(elapsed)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::TimedOut)), SyncServeOutcome::Stalled),
            (Err(io(ErrorKind::ConnectionReset)), SyncServeOutcome::Failed),
            (Err(WorkerNetworkError::Internal("storage".to_string())), SyncServeOutcome::Failed),
            (Err(WorkerNetworkError::StreamClosed), SyncServeOutcome::Failed),
        ];
        cases.iter().for_each(|(served, expected)| {
            assert_eq!(SyncServeOutcome::of(served), *expected, "classifying {served:?}");
        });
    }
}
