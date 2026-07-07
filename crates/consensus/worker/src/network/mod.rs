//! Worker network implementation.

use error::WorkerNetworkError;
use futures::{AsyncReadExt as _, AsyncWriteExt as _};
use handle::max_sync_frame_size;
pub use handle::WorkerNetworkHandle;
use handler::RequestHandler;
pub use message::{WorkerRequest, WorkerResponse};
use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    read_frame, types::NetworkEvent, write_frame, DenyReason, GossipMessage, ResponseChannel,
    Stream, StreamKind, SyncFrame, SyncFrameError, WorkerSyncRequest,
};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BatchValidation, BlockHash, BlsPublicKey, Database, Epoch, SealedBatch, TaskError, TaskSpawner,
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

/// Maximum number of concurrent batch stream operations (pending + active).
///
/// A semaphore permit is held from RPC acceptance through stream completion,
/// so this bounds the true concurrent count—not just the pending map size.
pub const MAX_CONCURRENT_BATCH_STREAMS: usize = 5;

/// Timeout for pending batch requests before cleanup.
pub const PENDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval for pruning pending batch requests (awaiting peer to open stream).
const PENDING_REQUEST_PRUNE_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum batch digests allowed per `RequestBatchesStream` request.
///
/// Derivation: 10 committee nodes * 6 max commit rounds * 5 batches per cert = 300.
/// We use 500 for forward-compatibility headroom (committee growth, parameter changes).
/// This is 66x smaller than the ~33k digests that fit in the 1MB RPC limit.
pub const MAX_BATCH_DIGESTS_PER_REQUEST: usize = 500;

/// Maximum number of concurrent pending batch requests from a single peer.
///
/// Prevents a single malicious peer from filling all global slots.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Timeout for reading the opening request frame of an inbound sync stream.
///
/// A peer that opens a sync stream but never sends its request frame trips this
/// and the stream is dropped, so it cannot hold an admission slot indefinitely.
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Tracks a pending batch stream request awaiting stream establishment.
// pub for IT
#[derive(Debug)]
pub struct PendingBatchStream {
    /// The batch digests requested (looked up from DB when stream arrives).
    batch_digests: BTreeSet<BlockHash>,
    /// The epoch which produced these batches.
    epoch: Epoch,
    /// When this request was created (for timeout cleanup).
    created_at: Instant,
    /// Semaphore permit held for the lifetime of this request (pending + active).
    /// Dropping the permit frees a global concurrency slot.
    _permit: OwnedSemaphorePermit,
}

/// Key for pending requests: (peer_bls, request_digest)
type PendingBatchRequestKey = (BlsPublicKey, B256);

impl PendingBatchStream {
    /// Create a new pending batch stream.
    pub fn new(
        batch_digests: BTreeSet<BlockHash>,
        epoch: Epoch,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self { batch_digests, epoch, created_at: Instant::now(), _permit: permit }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl PendingBatchStream {
    /// Create a pending batch stream with a custom `created_at` for testing stale cleanup.
    pub fn new_with_created_at(
        batch_digests: BTreeSet<BlockHash>,
        epoch: Epoch,
        permit: OwnedSemaphorePermit,
        created_at: Instant,
    ) -> Self {
        Self { batch_digests, epoch, created_at, _permit: permit }
    }

    /// Read the `created_at` timestamp for testing the cleanup / replacement behavior.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}

/// RAII guard for an admitted sync batch stream.
///
/// Holds a global concurrency permit and counts toward the peer's per-peer
/// in-flight total. Dropping it releases the global slot and decrements the
/// per-peer count, so a finished or aborted exchange frees capacity for both
/// the sync and legacy paths.
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

/// Try to admit one inbound sync batch stream for `peer`.
///
/// Acquires a global permit and admits only if the peer's combined in-flight
/// count (legacy pending requests plus sync streams) is below
/// [`MAX_PENDING_REQUESTS_PER_PEER`], so the per-peer cap holds across both
/// paths. Returns `None` (shedding the global permit) when either cap is hit.
///
/// Locks `pending_batch_requests` before `sync_stream_peers`; the legacy
/// admission path takes the same order, so the two never deadlock.
fn try_admit_sync(
    semaphore: &Arc<Semaphore>,
    pending: &Arc<Mutex<HashMap<PendingBatchRequestKey, PendingBatchStream>>>,
    sync_peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<SyncStreamPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let pending_guard = pending.lock();
    let legacy_count = pending_guard.keys().filter(|(p, _)| *p == peer).count();
    let mut sync_guard = sync_peers.lock();
    let sync_count = sync_guard.get(&peer).copied().unwrap_or(0);
    if legacy_count + sync_count >= MAX_PENDING_REQUESTS_PER_PEER {
        return None;
    }
    *sync_guard.entry(peer).or_insert(0) += 1;
    Some(SyncStreamPermit { _permit: permit, peers: sync_peers.clone(), peer })
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
    /// Pending batch requests awaiting stream from requestor.
    ///
    /// Wrapped in `Arc<Mutex>` so spawned stream tasks can look up the matching
    /// request after reading the correlation digest from the stream.
    pending_batch_requests: Arc<Mutex<HashMap<PendingBatchRequestKey, PendingBatchStream>>>,
    /// Semaphore bounding total concurrent batch stream operations (pending + active).
    ///
    /// Shared by the legacy and sync responder paths so the global concurrency cap
    /// of [`MAX_CONCURRENT_BATCH_STREAMS`] is the combined limit across both.
    batch_stream_semaphore: Arc<Semaphore>,
    /// Per-peer count of in-flight sync batch streams.
    ///
    /// The legacy per-peer count comes from `pending_batch_requests`; this counts
    /// the sync path's in-flight exchanges. Admission on either path checks the
    /// sum against [`MAX_PENDING_REQUESTS_PER_PEER`], so the per-peer cap is the
    /// combined limit across both paths.
    sync_stream_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
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
        let request_handler =
            RequestHandler::new(id, validator, consensus_config, network_handle.clone());
        Self {
            network_events,
            network_handle,
            request_handler,
            pending_batch_requests: Arc::new(Mutex::new(HashMap::new())),
            batch_stream_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS)),
            sync_stream_peers: Arc::new(Mutex::new(HashMap::new())),
            consensus_chain,
        }
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("worker network events", async move {
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
                                warn!(target: "worker::network", "critical worker network events channel dropped");
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

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::RequestBatchesStream { batch_digests, epoch } => {
                    self.process_request_batches_stream(
                        peer,
                        batch_digests,
                        epoch,
                        channel,
                        cancel,
                    );
                }
                WorkerRequest::PeerExchange { .. } => {
                    // expect this is intercepted by network layer
                    warn!(target: "worker::network", "worker application received unexpected peer exchange message");
                }
            },
            NetworkEvent::Gossip(msg, relayer) => {
                self.process_gossip(msg, relayer);
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
            NetworkEvent::InboundStream { peer, kind, stream } => match kind {
                StreamKind::Legacy => self.process_inbound_stream(peer, stream),
                StreamKind::Sync => self.process_inbound_sync_stream(peer, stream),
            },
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
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-report-batch-{}", sealed_batch.digest());
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
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

    /// Process gossip from a worker.
    fn process_gossip(&self, msg: GossipMessage, relayer: Option<BlsPublicKey>) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let relayer_label =
            relayer.as_ref().map_or_else(|| "unresolved".to_string(), |bls| bls.to_string());
        let task_name = format!("process-gossip-{relayer_label}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                // convert error into penalty to lower peer score; only attributable
                // when the relaying peer's BLS identity has resolved
                if let Some((relayer, penalty)) = relayer.zip(e.penalty()) {
                    network_handle.report_penalty(relayer, penalty).await;
                }
                Err(e.into())
            } else {
                Ok(())
            }
        });
    }

    /// Process a stream-based batch request.
    ///
    /// This negotiates a stream transfer. If we can fulfill the request,
    /// we store the pending request and return an ack. The requestor will
    /// then open a stream with the request digest for correlation.
    fn process_request_batches_stream(
        &self,
        peer: BlsPublicKey,
        batch_digests: BTreeSet<B256>,
        epoch: Epoch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // cap batch digests to node's max — process as many as possible
        let batch_digests: BTreeSet<B256> = if batch_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST {
            warn!(
                target: "worker::network",
                %peer,
                requested = batch_digests.len(),
                max = MAX_BATCH_DIGESTS_PER_REQUEST,
                "truncating oversized batch request"
            );
            batch_digests.into_iter().take(MAX_BATCH_DIGESTS_PER_REQUEST).collect()
        } else {
            batch_digests
        };

        // validate pending batch request
        let response = if batch_digests.is_empty() {
            debug!(target: "worker::network", "batch request empty");
            Err(WorkerNetworkError::InvalidRequest("Empty batch digests".into()))
        } else {
            // acquire semaphore permit (non-blocking) for global concurrency
            let ack = match self.batch_stream_semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    let mut pending_map = self.pending_batch_requests.lock();

                    // check per-peer capacity. The cap is the combined limit
                    // across the legacy and sync paths, so add this peer's
                    // in-flight sync streams to its legacy pending count. Locks
                    // pending then sync_stream_peers, matching try_admit_sync's
                    // order so the two admission paths never deadlock.
                    let legacy_count = pending_map.keys().filter(|(p, _)| *p == peer).count();
                    let sync_count = self.sync_stream_peers.lock().get(&peer).copied().unwrap_or(0);
                    let peer_count = legacy_count + sync_count;
                    if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
                        debug!(
                            target: "worker::network",
                            %peer,
                            peer_count,
                            "rejecting batch stream request: per-peer limit reached"
                        );
                        // permit drops here, freeing the slot
                        false
                    } else {
                        let request_digest =
                            self.network_handle.generate_batch_request_id(&batch_digests);
                        // If the same peer re-requests the same batch set while a prior
                        // entry is still pending, preserve the original `created_at` so
                        // the cleanup timer is not rearmed. Without this, a peer could
                        // hold a slot indefinitely by re-requesting before the 30s
                        // timeout. A second stream open is still punished as a protocol
                        // violation.
                        let created_at = pending_map
                            .get(&(peer, request_digest))
                            .map(|p| p.created_at)
                            .unwrap_or_else(Instant::now);
                        let pending = PendingBatchStream {
                            batch_digests,
                            epoch,
                            created_at,
                            _permit: permit,
                        };
                        if pending_map.insert((peer, request_digest), pending).is_some() {
                            debug!(
                                target: "worker::network",
                                %peer,
                                ?request_digest,
                                "pending batch stream request replaced with identical batch request"
                            );
                        }
                        debug!(
                            target: "worker::network",
                            %peer,
                            ?request_digest,
                            "pending batch stream request accepted"
                        );
                        true
                    }
                }
                Err(_) => false,
            };

            Ok(WorkerResponse::RequestBatchesStream { ack })
        };

        // send response
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-request-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            let msg = match response {
                Ok(msg) => msg,
                Err(err) => {
                    // classify transient responder-side conditions as recoverable
                    // so the requester retries instead of treating this as a
                    // permanent rejection
                    let response = WorkerResponse::into_error_ref(&err);
                    if let Some(penalty) = err.into() {
                        network_handle.report_penalty(peer, penalty).await;
                    }

                    response
                }
            };

            // send response or cancel
            tokio::select! {
                _ = network_handle.inner_handle().send_response(msg, channel) => (),
                _ = cancel => (),
            }
            Ok(())
        });
    }

    /// Process an inbound stream for batch transfer.
    ///
    /// Reads the request digest from the stream and validates against pending requests.
    fn process_inbound_stream(&self, peer: BlsPublicKey, mut stream: Stream) {
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let pending_map = self.pending_batch_requests.clone();
        let task_name = format!("stream-requested-batches-{peer}");
        let consensus_chain = self.consensus_chain.clone();
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            // read the request digest (32-bytes) from the stream with timeout
            let mut digest_buf = [0u8; tn_types::DIGEST_LENGTH];
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.read_exact(&mut digest_buf),
            ).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(target: "worker::network", %peer, ?e, "failed to read request digest from stream");
                    return Err(e.into());
                }
                Err(e) => {
                    warn!(target: "worker::network", %peer, "timeout reading request digest from stream");
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
                .process_request_batches_stream(peer, opt_pending_req, stream, request_digest, &consensus_chain)
                .await {
                    // apply applicable penalty for error
                    warn!(target: "worker::network", ?err, "error processing request batches stream");
                    if let Some(penalty) = err.penalty() {
                        network_handle.report_penalty(peer, penalty).await;
                    }
                    Err(err.into())
                } else {
                    Ok(())
                }
        });
    }

    /// Process an inbound sync-protocol batch stream.
    ///
    /// The request travels in the opening [`SyncFrame::Req`] frame (no prior
    /// request-response ack), so admission against the shared concurrency caps
    /// happens here on stream open. A shedding responder writes
    /// [`DenyReason::AtCapacity`] without reading, so the requester gives up
    /// immediately and tries elsewhere. Once admitted, the opening request frame
    /// is read (bounded by [`SYNC_REQUEST_READ_TIMEOUT`]) and the batches are
    /// served by [`RequestHandler::process_sync_batches_stream`]. The admission
    /// permit is held for the lifetime of the spawned task.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        // admit against the shared caps before spawning; the permit (if any) moves
        // into the task and frees capacity on drop
        let permit = try_admit_sync(
            &self.batch_stream_semaphore,
            &self.pending_batch_requests,
            &self.sync_stream_peers,
            peer,
        );
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let epoch = self.network_handle.epoch();
        let task_name = format!("sync-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            let mut stream = stream;
            let max_frame = max_sync_frame_size(epoch);
            let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

            // shed load: deny without reading so the requester retries elsewhere
            let Some(_permit) = permit else {
                debug!(target: "worker::network", %peer, "denying inbound sync stream: at capacity");
                // bound the best-effort shed write: a peer that applies receive
                // backpressure and never reads must not stall this task (no permit
                // is held here, but the spawned task would otherwise linger).
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
                return Ok(());
            };

            // read the opening request frame; a peer that never sends one (timeout)
            // or sends a malformed one (io error) is dropped after releasing the
            // permit. Collapse the timeout/io results rather than nesting matches.
            let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
            let request = tokio::time::timeout(
                SYNC_REQUEST_READ_TIMEOUT,
                read_frame::<_, WorkerSyncRequest>(
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
                warn!(target: "worker::network", %peer, "no readable sync request frame");
                let _ = stream.close().await;
                return Ok(());
            };

            match request {
                SyncFrame::Req(WorkerSyncRequest::Batches { batch_digests, epoch: req_epoch }) => {
                    // cap to the node's max, mirroring the legacy responder
                    let batch_digests: BTreeSet<B256> =
                        if batch_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST {
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
                            &consensus_chain,
                        )
                        .await?;
                }
                // a well-behaved requester always opens with `Req`; anything else
                // is malformed. Signal it and drop (metrics-only, no penalty).
                SyncFrame::Ack
                | SyncFrame::Deny(_)
                | SyncFrame::Data(_)
                | SyncFrame::End
                | SyncFrame::Err(_) => {
                    warn!(target: "worker::network", %peer, "unexpected opening sync frame from requester");
                    // bound the best-effort error write so a non-reading peer
                    // cannot pin the held admission permit on an unbounded write.
                    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                        let _ = write_frame(
                            &mut stream,
                            &SyncFrame::<WorkerSyncRequest>::Err(SyncFrameError::Malformed),
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

    /// Clean up stale pending requests that have timed out.
    fn cleanup_stale_pending_requests(&mut self) {
        let now = Instant::now();
        self.pending_batch_requests
            .lock()
            .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
    }
}
