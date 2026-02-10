//! Worker network implementation.

use crate::batch_fetcher::BatchFetcher;
use error::WorkerNetworkError;
pub use handle::WorkerNetworkHandle;
use handler::RequestHandler;
pub use message::{WorkerRequest, WorkerResponse};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    stream::StreamHeader, types::NetworkEvent, GossipMessage, ResponseChannel,
};
use tn_network_types::{FetchBatchResponse, PrimaryToWorkerClient, WorkerSynchronizeMessage};
use tn_storage::tables::Batches;
use tn_types::{
    now, BatchValidation, BlockHash, BlsPublicKey, Database, DbTxMut, SealedBatch, TaskSpawner,
    TnReceiver, WorkerId, B256,
};
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};

pub(crate) mod error;
pub(crate) mod handle;
pub(crate) mod handler;
pub(crate) mod message;
pub(crate) mod stream_codec;

/// Convenience type for Worker network.
pub(crate) type Req = WorkerRequest;

/// Convenience type for Worker network.
pub(crate) type Res = WorkerResponse;

/// Maximum number of concurrent pending batch stream requests.
const MAX_PENDING_BATCH_REQUESTS: usize = 5;

/// Timeout for pending batch requests before cleanup.
const PENDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Tracks a pending batch stream request awaiting stream establishment.
// pub for IT
#[derive(Debug)]
pub struct PendingBatchStream {
    /// The batch digests requested (looked up from DB when stream arrives).
    batch_digests: HashSet<BlockHash>,
    /// When this request was created (for timeout cleanup).
    created_at: Instant,
}

/// Key for pending requests: (peer_bls, request_digest)
type PendingBatchRequestKey = (BlsPublicKey, B256);

impl PendingBatchStream {
    /// Create a new pending batch stream for testing.
    pub fn new(batch_digests: HashSet<BlockHash>) -> Self {
        Self { batch_digests, created_at: Instant::now() }
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
    pending_batch_requests: HashMap<PendingBatchRequestKey, PendingBatchStream>,
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
    ) -> Self {
        let request_handler =
            RequestHandler::new(id, validator, consensus_config, network_handle.clone());
        Self {
            network_events,
            network_handle,
            request_handler,
            pending_batch_requests: HashMap::new(),
        }
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("worker network events", async move {
            // start interval for pruning stale stream requests
            //
            //
            // TODO: Duration should be a const or value from config - is 15s correct?
            let mut prune_requests = tokio::time::interval(Duration::from_secs(15));
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

    /// Handle events concurrently.
    fn process_network_event(&mut self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::RequestBatches { batch_digests, max_response_size } => {
                    self.process_request_batches(
                        peer,
                        batch_digests,
                        max_response_size,
                        channel,
                        cancel,
                    );
                }
                WorkerRequest::RequestBatchesStream { batch_digests } => {
                    self.process_request_batches_stream(peer, batch_digests, channel, cancel);
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
                    },
                );
            }
            NetworkEvent::InboundStream { peer, stream, header } => {
                self.process_inbound_stream(peer, stream, header);
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
        });
    }

    /// Attempt to return requested batches.
    fn process_request_batches(
        &self,
        peer: BlsPublicKey,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-request-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_request_batches(batch_digests, max_response_size) => {
                    let response = match res {
                        Ok(r) => WorkerResponse::RequestBatches(r),
                        Err(err) => {
                            let error = err.to_string();
                            if let Some(penalty) = err.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }

                            WorkerResponse::Error(message::WorkerRPCError(error))
                        }
                    };

                    let _ = network_handle.inner_handle().send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
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
                if let Err(e) = request_handler.process_gossip(&msg).await {
                    warn!(target: "worker::network", ?e, "process_gossip");
                    // convert error into penalty to lower peer score
                    if let Some(penalty) = e.into() {
                        network_handle.report_penalty(propagation_source, penalty).await;
                    }
                }
            }
        });
    }

    /// Process a stream-based batch request.
    ///
    /// This negotiates a stream transfer. If we can fulfill the request,
    /// we store the pending request and return an ack. The requestor will
    /// then open a stream with the request digest for correlation.
    fn process_request_batches_stream(
        &mut self,
        peer: BlsPublicKey,
        batch_digests: HashSet<B256>,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // check if node has capacity to fulfill peer's request
        let ack = self.pending_batch_requests.len() < MAX_PENDING_BATCH_REQUESTS;

        // check if pending batch request is empty
        let response = if batch_digests.len() == 0 {
            debug!(target: "worker::network", "batch request empty");
            Err(WorkerNetworkError::InvalidRequest("Empty batch digests".into()))
        } else {
            // compute request digest for stream correlation
            let request_digest = self.network_handle.generate_batch_request_id(&batch_digests);

            // store the pending request
            let pending = PendingBatchStream::new(batch_digests);
            self.pending_batch_requests.insert((peer, request_digest), pending);
            debug!(
                target: "worker::network",
                %peer,
                ?request_digest,
                ?ack,
                "ack for batch stream request"
            );
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
        });
    }

    /// Process an inbound stream for batch transfer.
    ///
    /// Validates the stream header against pending requests and sends batches.
    fn process_inbound_stream(
        &mut self,
        peer: BlsPublicKey,
        stream: libp2p::Stream,
        header: StreamHeader,
    ) {
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("stream-requested-batches-{peer}");
        // check for request and spawn task
        let request_digest = B256::from(header.request_digest);
        let key = (peer, request_digest);
        let opt_pending_req = self.pending_batch_requests.remove(&key);
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_request_batches_stream(peer, opt_pending_req, stream, header) => {
                     if let Err(err) = res {
                        if let Some(penalty) = err.into() {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                    }
                }
            }
        });
    }

    /// Clean up stale pending requests that have timed out.
    fn cleanup_stale_pending_requests(&mut self) {
        let now = Instant::now();
        self.pending_batch_requests
            .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Debug)]
pub(super) struct PrimaryReceiverHandler<DB> {
    /// The batch store
    pub store: DB,
    /// Timeout on RequestBatches RPC.
    pub request_batches_timeout: Duration,
    /// Synchronize header payloads from other workers.
    pub network: Option<WorkerNetworkHandle>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher<DB>>,
    /// Validate incoming batches
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait::async_trait]
impl<DB: Database> PrimaryToWorkerClient for PrimaryReceiverHandler<DB> {
    async fn synchronize(&self, message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        let Some(network) = self.network.as_ref() else {
            return Err(eyre::eyre!(
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get::<Batches>(digest) {
                Ok(None) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(Some(_)) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(eyre::eyre!("failed to read from batch store: {e:?}"));
                }
            };
        }
        if missing.is_empty() {
            return Ok(());
        }

        let response = tokio::time::timeout(
            self.request_batches_timeout,
            network.request_batches(missing.iter().cloned().collect()),
        )
        .await??;

        let sealed_batches_from_response: Vec<SealedBatch> =
            response.into_iter().map(|b| b.seal_slow()).collect();

        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
                    return Err(eyre::eyre!("Invalid batch: {err}"));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());
                let mut tx = self.store.write_txn().map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to create batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.insert::<Batches>(&digest, &batch).map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.commit().map_err(|e| {
                    WorkerNetworkError::Internal(format!("failed to commit batch: {e:?}"))
                })?;
            } else {
                return Err(eyre::eyre!(format!(
                    "failed to synchronize batches- received a batch {digest} we did not request!"
                )));
            }
        }

        if missing.is_empty() {
            return Ok(());
        }
        Err(eyre::eyre!("failed to synchronize batches!".to_string()))
    }

    async fn fetch_batches(&self, digests: HashSet<BlockHash>) -> eyre::Result<FetchBatchResponse> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(eyre::eyre!(
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let batches = batch_fetcher.fetch(digests).await;
        Ok(FetchBatchResponse { batches })
    }
}
