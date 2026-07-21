//! Worker network implementation.

use futures::AsyncWriteExt as _;
use handle::max_sync_frame_size;
pub use handle::WorkerNetworkHandle;
use handler::RequestHandler;
pub use message::{WorkerRequest, WorkerResponse};
use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    read_frame, types::NetworkEvent, write_frame, DenyReason, GossipMessage, ResponseChannel,
    Stream, SyncFrame, SyncFrameError, WorkerSyncRequest,
};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BatchValidation, BlsPublicKey, Database, SealedBatch, TaskError, TaskSpawner, TnReceiver,
    WorkerId, B256,
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

/// Maximum batch digests allowed per sync batch request (`WorkerSyncRequest::Batches`).
///
/// Derivation: 10 committee nodes * 6 max commit rounds * 5 batches per cert = 300.
/// We use 500 for forward-compatibility headroom (committee growth, parameter changes).
/// This is 66x smaller than the ~33k digests that fit in the 1MB RPC limit.
pub const MAX_BATCH_DIGESTS_PER_REQUEST: usize = 500;

/// Maximum number of concurrent in-flight sync batch streams from a single peer.
///
/// Counts a peer's in-flight sync streams, so no single peer can fill all global
/// slots ([`MAX_CONCURRENT_BATCH_STREAMS`]) and starve its peers.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Maximum number of concurrent gossip-triggered batch prefetches.
///
/// When a committee worker gossips an accepted batch digest, a peer that is missing
/// the batch prefetches it, and that fetch fans out to every connected peer (see
/// [`handle::WorkerNetworkHandle::request_batches`]). Prefetching is a best-effort
/// optimization: a genuinely needed batch is still fetched on demand later. Capping
/// the number in flight (and deduplicating by digest) stops a Byzantine author from
/// amplifying bandwidth/connection load across the worker mesh by gossiping many
/// distinct (possibly forged) digests. Load beyond the cap is shed, not queued.
pub const MAX_CONCURRENT_GOSSIP_PREFETCHES: usize = 8;

/// Timeout for reading the opening request frame of an inbound sync stream.
///
/// A peer that opens a sync stream but never sends its request frame trips this
/// and the stream is dropped, so it cannot hold an admission slot indefinitely.
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// RAII guard for an admitted sync batch stream.
///
/// Holds a global concurrency permit and counts toward the peer's per-peer
/// in-flight total. Dropping it releases the global slot and decrements the
/// per-peer count, so a finished or aborted exchange frees capacity for the next
/// sync stream.
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
/// Acquires a global permit and admits only if the peer's in-flight sync-stream
/// count is below [`MAX_PENDING_REQUESTS_PER_PEER`]. Returns `None` (shedding the
/// global permit) when either cap is hit.
fn try_admit_sync(
    semaphore: &Arc<Semaphore>,
    sync_peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<SyncStreamPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let mut sync_guard = sync_peers.lock();
    let sync_count = sync_guard.get(&peer).copied().unwrap_or(0);
    (sync_count < MAX_PENDING_REQUESTS_PER_PEER).then(|| {
        *sync_guard.entry(peer).or_insert(0) += 1;
        SyncStreamPermit { _permit: permit, peers: sync_peers.clone(), peer }
    })
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
    /// Semaphore bounding total concurrent sync batch stream operations.
    ///
    /// A permit is held from admission through stream completion, so the global
    /// concurrency cap of [`MAX_CONCURRENT_BATCH_STREAMS`] bounds the true
    /// in-flight count.
    batch_stream_semaphore: Arc<Semaphore>,
    /// Per-peer count of in-flight sync batch streams.
    ///
    /// Admission checks this count against [`MAX_PENDING_REQUESTS_PER_PEER`], the
    /// sole per-peer cap now that every batch fetch rides the typed sync protocol.
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
            batch_stream_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS)),
            sync_stream_peers: Arc::new(Mutex::new(HashMap::new())),
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
            NetworkEvent::Gossip { message, relayer, author } => {
                self.process_gossip(message, relayer, author);
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
    /// request-response ack), so admission against the per-peer concurrency cap
    /// happens here on stream open. A shedding responder writes
    /// [`DenyReason::AtCapacity`] without reading, so the requester gives up
    /// immediately and tries elsewhere. Once admitted, the opening request frame
    /// is read (bounded by [`SYNC_REQUEST_READ_TIMEOUT`]) and the batches are
    /// served by [`RequestHandler::process_sync_batches_stream`]. The admission
    /// permit is held for the lifetime of the spawned task.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        // admit against the per-peer cap before spawning; the permit (if any) moves
        // into the task and frees capacity on drop
        let permit = try_admit_sync(&self.batch_stream_semaphore, &self.sync_stream_peers, peer);
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
                // bound the best-effort close so a peer that sent no readable frame
                // and then stops reading cannot pin the held admission permit on the
                // FIN flush; mirrors the shed/malformed bounded closes above.
                let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
                return Ok(());
            };

            match request {
                SyncFrame::Req(WorkerSyncRequest::Batches { batch_digests, epoch: req_epoch }) => {
                    // cap to the node's max
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
}

#[cfg(test)]
mod tests {
    use super::*;

    // A single fixed peer suffices: every case exercises the per-peer cap for one
    // peer. `BlsPublicKey::default()` is the same key the crate's other unit tests
    // use for this purpose.
    fn peer() -> BlsPublicKey {
        BlsPublicKey::default()
    }

    // Admitting sync streams up to the per-peer cap succeeds; the next is rejected
    // (shedding its global permit), and dropping an admitted permit frees both the
    // global slot and the per-peer count.
    #[test]
    fn sync_admit_enforces_per_peer_cap_and_frees_on_drop() {
        let p = peer();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_STREAMS));
        let sync = Arc::new(Mutex::new(HashMap::new()));

        // admit up to the per-peer cap
        let permits: Vec<_> = (0..MAX_PENDING_REQUESTS_PER_PEER)
            .map(|_| try_admit_sync(&semaphore, &sync, p).expect("admit below the per-peer cap"))
            .collect();
        assert_eq!(sync.lock().get(&p).copied(), Some(MAX_PENDING_REQUESTS_PER_PEER));
        assert_eq!(
            semaphore.available_permits(),
            MAX_CONCURRENT_BATCH_STREAMS - MAX_PENDING_REQUESTS_PER_PEER
        );

        // at the cap: the next admit is rejected and its global permit is shed
        assert!(try_admit_sync(&semaphore, &sync, p).is_none());
        assert_eq!(
            semaphore.available_permits(),
            MAX_CONCURRENT_BATCH_STREAMS - MAX_PENDING_REQUESTS_PER_PEER
        );

        // dropping the admitted permits frees the global slots and the per-peer count
        drop(permits);
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_BATCH_STREAMS);
        assert!(sync.lock().get(&p).is_none());
    }
}
