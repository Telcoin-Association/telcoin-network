//! Worker network implementation.

use error::WorkerNetworkError;
use futures::AsyncReadExt as _;
pub use handle::WorkerNetworkHandle;
use handler::RequestHandler;
pub use message::{WorkerRequest, WorkerResponse};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{types::NetworkEvent, GossipMessage, ResponseChannel, Stream};
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

/// Tracks a pending batch stream request awaiting stream establishment.
// pub for IT
#[derive(Debug)]
pub struct PendingBatchStream {
    /// The batch digests requested (looked up from DB when stream arrives).
    batch_digests: HashSet<BlockHash>,
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
        batch_digests: HashSet<BlockHash>,
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
        batch_digests: HashSet<BlockHash>,
        epoch: Epoch,
        permit: OwnedSemaphorePermit,
        created_at: Instant,
    ) -> Self {
        Self { batch_digests, epoch, created_at, _permit: permit }
    }
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
    batch_stream_semaphore: Arc<Semaphore>,
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
            NetworkEvent::Gossip(msg, propagation_source) => {
                self.process_gossip(msg, propagation_source);
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
                self.process_inbound_stream(peer, stream);
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
                            let error = err.to_string();
                            if let Some(penalty) = err.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                            WorkerResponse::Error(message::WorkerRPCError(error))
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
    fn process_gossip(&self, msg: GossipMessage, propagation_source: BlsPublicKey) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-gossip-{propagation_source}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                // convert error into penalty to lower peer score
                if let Some(penalty) = e.penalty() {
                    network_handle.report_penalty(propagation_source, penalty).await;
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
        batch_digests: HashSet<B256>,
        epoch: Epoch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // cap batch digests to node's max — process as many as possible
        let batch_digests: HashSet<B256> = if batch_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST {
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

                    // check per-peer capacity
                    let peer_count = pending_map.keys().filter(|(p, _)| *p == peer).count();
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
                        let pending = PendingBatchStream::new(batch_digests, epoch, permit);
                        pending_map.insert((peer, request_digest), pending);
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
                    let error = err.to_string();
                    if let Some(penalty) = err.into() {
                        network_handle.report_penalty(peer, penalty).await;
                    }

                    WorkerResponse::Error(message::WorkerRPCError(error))
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

    /// Clean up stale pending requests that have timed out.
    fn cleanup_stale_pending_requests(&mut self) {
        let now = Instant::now();
        self.pending_batch_requests
            .lock()
            .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
    }
}
