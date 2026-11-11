//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{
    batch_fetcher::BatchFetcher,
    metrics::{ForwardDropReason, WorkerMetrics},
    network::primary::PrimaryReceiverHandler,
    quorum_waiter::{QuorumWaiter, QuorumWaiterTrait},
    WorkerNetworkHandle,
};
use lru::LruCache;
use parking_lot::Mutex;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::ConsensusChain,
    tables::{NodeBatchesCache, OurNodeBatchesCache},
};
use tn_types::{
    error::BlockSealError, BatchReceiver, BatchSender, BatchValidation, BlockHash, BlsPublicKey,
    Database, SealedBatch, ShutdownNotifier, TaskManager, TxnForwarder, WorkerId,
};
use tracing::{error, info, instrument, warn};

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Capacity of the per-epoch set of own batch digests whose report the primary acked (switch
/// `digest_dedupe`, issue #1807).
///
/// The set is a least-recently-used cache, so its memory stays bounded: at most 4096 digests of 32
/// bytes each, plus the overhead of the cache. When the set is full, a new digest replaces the
/// oldest one, and a later seal of the dropped digest is reported again as it would be with the
/// switch off; a batch is never dropped because its digest fell out of the set.
const REPORTED_DIGESTS_CAPACITY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4095);

/// Spawn the worker.
///
/// Create an instance of `Self` and start all tasks to participate in consensus.
pub fn new_worker<DB: Database>(
    id: WorkerId,
    validator: Arc<dyn BatchValidation>,
    consensus_config: ConsensusConfig<DB>,
    network_handle: WorkerNetworkHandle,
    forwarder: Arc<dyn TxnForwarder>,
    consensus_chain: ConsensusChain,
) -> eyre::Result<Worker<DB, QuorumWaiter>> {
    info!(target: "worker::worker", "Boot worker node with id {} key {:?}", id, consensus_config.key_config().primary_public_key());

    let batch_fetcher = BatchFetcher::new(
        network_handle.clone(),
        consensus_config.node_storage().clone(),
        consensus_chain,
        WorkerMetrics::new_for_worker(id),
    );
    // This worker's own local network instance: a worker id outside the committee's worker
    // set is a wiring bug, surfaced as an error rather than a fallback onto another
    // worker's instance.
    let local_network = consensus_config
        .local_network(id)
        .cloned()
        .ok_or_else(|| eyre::eyre!("no local network instance for worker id {id}"))?;
    local_network.set_primary_to_worker_local_handler(Arc::new(PrimaryReceiverHandler {
        store: consensus_config.node_storage().clone(),
        network: Some(network_handle.clone()),
        batch_fetcher,
        validator,
    }))?;
    let batch_provider = new_worker_internal(
        id,
        &consensus_config,
        local_network,
        network_handle.clone(),
        forwarder,
    );

    // NOTE: This log entry is used to compute performance.
    info!(target: "worker::worker",
        "Worker {} successfully booted on {}",
        id,
        consensus_config.worker_address(id).map_or_else(|| "<no address>".to_string(), |addr| addr.to_string())
    );

    Ok(batch_provider)
}

/// Builds a new batch provider responsible for handling client transactions.
fn new_worker_internal<DB: Database>(
    id: WorkerId,
    consensus_config: &ConsensusConfig<DB>,
    client: LocalNetwork,
    network_handle: WorkerNetworkHandle,
    forwarder: Arc<dyn TxnForwarder>,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Starting handler for transactions");

    // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the batch
    // before forwarding the batch to the `Processor`
    // Only have a quorum waiter if we are an authority (validator).
    let quorum_waiter = consensus_config.authority().clone().map(|authority| {
        QuorumWaiter::new(
            authority,
            consensus_config.committee().clone(),
            network_handle.clone(),
            WorkerMetrics::new_for_worker(id),
        )
    });

    // Committee BLS keys in slot order (index == committee slot). A non-committee ("observer")
    // worker forwards each transaction it accepts to the JSON-RPC endpoint advertised by the
    // validator whose slot owns the sender, matching `submit_txn_if_mine` so nonce ordering is
    // preserved (issue #804).
    let committee_slots: Vec<BlsPublicKey> = consensus_config
        .committee()
        .authorities()
        .iter()
        .map(|authority| *authority.protocol_key())
        .collect();

    Worker::new(
        id,
        quorum_waiter,
        client,
        consensus_config.node_storage().clone(),
        consensus_config.parameters().batch_vote_timeout,
        network_handle,
        forwarder,
        committee_slots,
    )
    .with_consensus_shutdown(consensus_config.shutdown().clone())
    .with_digest_dedupe(consensus_config.parameters().digest_dedupe)
}

/// Process batch from EL into sealed batches for CL.
pub struct Worker<DB, QW> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to batches.
    quorum_waiter: Option<QW>,
    /// The network client to send our batches to the primary.
    client: LocalNetwork,
    /// The batch store to store our own batches.
    store: DB,
    /// Channel sender for alternate batch submision if not calling seal directly.
    tx_batches: BatchSender,
    /// Channel receiver for alternate batch submision if not calling seal directly.
    /// This will be "taken" on batch spawn and become None.
    rx_batches: Option<BatchReceiver>,
    /// The amount of time to wait on a reply from peer before timing out.
    timeout: Duration,
    /// Worker network handle.
    network_handle: WorkerNetworkHandle,
    /// Forwards transactions this node accepts to committee validators over their advertised
    /// JSON-RPC endpoints when this node is not a committee voting validator (issue #804).
    forwarder: Arc<dyn TxnForwarder>,
    /// Committee BLS keys in slot order (index == committee slot).
    ///
    /// Populated once per epoch at construction: a non-CVV worker forwards each transaction it
    /// accepts to the validator whose slot owns the sender, so nonce ordering is preserved.
    committee_slots: Vec<BlsPublicKey>,
    /// Prometheus metrics for this worker.
    metrics: WorkerMetrics,
    /// This epoch's consensus shutdown signal, the same notifier the primary's proposer exits on.
    ///
    /// Once it fires, a batch sealed by quorum could never be reported, so the seal is refused
    /// before any peer is asked to vote. `None` disables the check.
    consensus_shutdown: Option<ShutdownNotifier>,
    /// Own batch digests whose latest report this epoch's primary acked, shared by clones.
    ///
    /// `Some` only while switch `digest_dedupe` is on (issue #1807): an own batch whose digest is
    /// already reported is then neither sealed nor reported again (see
    /// [`Self::reported_and_uncommitted`]). A digest is added after a successful report and
    /// removed when a new seal of it starts, so a mark always describes the latest attempt.
    /// Bounded by [`REPORTED_DIGESTS_CAPACITY`]; a new worker (one per epoch) starts empty.
    /// `None` keeps the previous path, where every seal is sent for quorum and reported.
    reported_digests: Option<Arc<Mutex<LruCache<BlockHash, ()>>>>,
}

// Need to implement clone directly because of the rx_batches field.
// This field is a use once field when spawning the batch manager so this is fine.
// Code will panic quickly if this is messed up.
impl<DB: Clone, QW: Clone> Clone for Worker<DB, QW> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            quorum_waiter: self.quorum_waiter.clone(),
            client: self.client.clone(),
            store: self.store.clone(),
            tx_batches: self.tx_batches.clone(),
            rx_batches: None,
            timeout: self.timeout,
            network_handle: self.network_handle.clone(),
            forwarder: self.forwarder.clone(),
            committee_slots: self.committee_slots.clone(),
            metrics: self.metrics.clone(),
            consensus_shutdown: self.consensus_shutdown.clone(),
            reported_digests: self.reported_digests.clone(),
        }
    }
}

impl<DB, QW> std::fmt::Debug for Worker<DB, QW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> Worker<DB, QW> {
    /// Create an instance of `Self`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: WorkerId,
        quorum_waiter: Option<QW>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
        network_handle: WorkerNetworkHandle,
        forwarder: Arc<dyn TxnForwarder>,
        committee_slots: Vec<BlsPublicKey>,
    ) -> Self {
        let (tx_batches, rx_batches) = tokio::sync::mpsc::channel(1000);
        Self {
            id,
            quorum_waiter,
            client,
            store,
            tx_batches,
            rx_batches: Some(rx_batches),
            timeout,
            network_handle,
            forwarder,
            committee_slots,
            metrics: WorkerMetrics::new_for_worker(id),
            consensus_shutdown: None,
            reported_digests: None,
        }
    }

    /// Refuse quorum seals once `shutdown` is notified.
    ///
    /// Pass this epoch's consensus shutdown notifier. After it fires the proposer is exiting and
    /// stops taking batch reports, so a quorum seal would make peers validate and store a batch
    /// whose report fails. Forwarding by a worker outside the committee is not affected.
    pub fn with_consensus_shutdown(mut self, shutdown: ShutdownNotifier) -> Self {
        self.consensus_shutdown = Some(shutdown);
        self
    }

    /// Set the `digest_dedupe` switch (issue #1807); a new worker starts with it off.
    ///
    /// When on, a seal whose digest this epoch's primary already acked, and whose batch is not
    /// yet committed, returns `Ok` without asking peers to vote or reporting the digest again.
    /// Off, the worker holds no marks and allocates nothing for them.
    pub fn with_digest_dedupe(mut self, enabled: bool) -> Self {
        self.reported_digests =
            enabled.then(|| Arc::new(Mutex::new(LruCache::new(REPORTED_DIGESTS_CAPACITY))));
        self
    }

    /// Spawn a little task to accept batches from a channel and seal them that way.
    /// Allows the engine to remain removed from the worker.
    pub fn spawn_batch_builder(&mut self, prefix: &str, task_manager: &TaskManager) {
        let this_clone = self.clone();
        let mut rx_batches = self.rx_batches.take().expect("have batch receive");
        task_manager.spawn_critical_task(format!("{prefix} batch-builder"), async move {
            while let Some((batch, tx)) = rx_batches.recv().await {
                let res = this_clone.seal(batch).await;
                if tx.send(res).is_err() {
                    error!(target: "worker::batch_provider", "Error sending result to channel caller!  Channel closed.");
                }
            }
            Ok(())
        });
    }

    /// Return worker's ID.
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// True if this worker seals batches by collecting a committee quorum.
    ///
    /// A quorum-sealed batch must then be reported to this node's proposer, so the batch builder
    /// feeding this worker is only useful while that proposer runs. A worker without a quorum
    /// waiter forwards its transactions to committee validators instead.
    pub fn seals_via_quorum(&self) -> bool {
        self.quorum_waiter.is_some()
    }

    /// Return the network handle for this worker.
    pub fn network_handle(&self) -> WorkerNetworkHandle {
        self.network_handle.clone()
    }

    /// The sender end of the batch submit channel.
    pub fn batches_tx(&self) -> BatchSender {
        self.tx_batches.clone()
    }

    /// Forward all the txns in `sealed_batch` to committee validators so they can be included
    /// in blocks. Use this when not a CVV so that transactions you accept can be included.
    ///
    /// Replaces the previous gossip broadcast (issue #804): each transaction is forwarded to the
    /// JSON-RPC endpoint the owning validator advertised on its worker record, discovered over
    /// kademlia. Admission is decided here, synchronously, because the `Ok` this method returns
    /// is what lets the batch builder evict these transactions from its pool as mined: a batch
    /// that was never handed to a forward task must report [`BlockSealError::NotValidator`] so
    /// the builder keeps its transactions pending and retries on a later build. Delivery stays
    /// best-effort on a background task, so batch production is never stalled by a slow or
    /// unreachable validator; discovery is a snapshot of already-fetched kademlia records, not
    /// a blocking network round-trip.
    pub async fn disburse_txns(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let transactions = sealed_batch.batch.transactions;
        if transactions.is_empty() {
            return Ok(());
        }

        // Whole-batch count for the discovery dead ends below (issue #1133).
        let num_txns = transactions.len();
        let validator_rpcs = self
            .network_handle
            .get_all_validator_rpcs()
            .await
            .inspect_err(|err| {
                warn!(
                    target: "worker::batch_provider",
                    ?err,
                    "failed to discover validator JSON-RPC endpoints for transaction forwarding"
                );
                self.metrics.record_forward_dropped(ForwardDropReason::DiscoveryFailed, num_txns);
            })
            .inspect(|rpcs| {
                // Only the discovery-succeeded-but-empty case earns this message; a
                // discovery failure already warned above with the actual error.
                if rpcs.is_empty() {
                    warn!(
                        target: "worker::batch_provider",
                        "no committee validator has advertised a JSON-RPC endpoint; \
                         cannot forward accepted transactions"
                    );
                    self.metrics
                        .record_forward_dropped(ForwardDropReason::NoEndpointAdvertised, num_txns);
                }
            })
            .unwrap_or_default();

        let admitted = !validator_rpcs.is_empty()
            && self.forwarder.forward_txns(
                transactions,
                self.committee_slots.clone(),
                validator_rpcs,
            );
        admitted.then_some(()).ok_or(BlockSealError::NotValidator)
    }

    /// Seal and broadcast the current batch, treating empty batches as a successful no-op.
    #[instrument(level = "debug", skip_all, fields(batch_size = sealed_batch.size(), num_txs = sealed_batch.batch.transactions.len()))]
    pub async fn seal(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        if sealed_batch.batch.transactions.is_empty() {
            Ok(())
        } else {
            self.seal_non_empty(sealed_batch).await
        }
    }

    /// Forward or attest a batch after `seal` has confirmed it contains transactions.
    async fn seal_non_empty(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let Some(quorum_waiter) = &self.quorum_waiter else {
            // We are not a validator so need to send any transactions out for a CVV to pickup.
            return self.disburse_txns(sealed_batch).await;
        };

        // the proposer exits on this signal, so a batch sealed now could never be reported;
        // refuse before asking peers to validate and store it
        if self.consensus_shutdown.as_ref().is_some_and(ShutdownNotifier::is_notified) {
            return Err(BlockSealError::ConsensusShuttingDown);
        }

        if let Some(marks) = &self.reported_digests {
            let digest = sealed_batch.digest();
            if self.reported_and_uncommitted(marks, &digest) {
                // this epoch's proposer holds the digest until it commits or the epoch closes, and
                // the cache row lets orphan recovery re-pool the transactions if it never commits
                info!(
                    target: "consensus::metrics",
                    worker_id = self.id,
                    digest = %digest,
                    num_txs = sealed_batch.batch.transactions.len(),
                    "batch digest already reported"
                );
                self.metrics.record_duplicate_digest_suppressed();
                return Ok(());
            }
            // this seal replaces any earlier attempt: only its own successful report below may
            // mark the digest again, so a failed report is never mistaken for a delivered one
            marks.lock().pop(&digest);
        }

        // the quorum round trip as the worker sees it: from handing the batch to the quorum
        // waiter (its requests leave at once) until the attest result, across the cache insert
        let quorum_start = Instant::now();
        let batch_attest_handle = quorum_waiter.verify_batch(
            sealed_batch.clone(),
            self.timeout,
            self.network_handle.get_task_spawner(),
        );

        let (batch, digest) = sealed_batch.split();
        if let Err(e) = self.store.insert::<OurNodeBatchesCache>(&digest, &batch) {
            // Cache the batch early, avoid race conditions.
            // Note the cache should be cleared every epoch after processing.
            error!(target: "worker::batch_provider", "Store failed (batch cache) with error: {:?}", e);
            return Err(BlockSealError::FatalDBFailure);
        }

        // Wait for our batch to reach quorum or fail to do so.
        match batch_attest_handle.await {
            Ok(res) => {
                match res {
                    Ok(()) => {
                        // batch reached quorum!
                        // logged for every seal that reaches quorum; the batch metrics wait for
                        // the report below, so a seal whose store or report fails is not counted
                        info!(
                            target: "consensus::metrics",
                            worker_id = self.id,
                            batch_size = batch.size(),
                            num_txs = batch.transactions.len(),
                            digest = %digest,
                            quorum_ms = millis(quorum_start.elapsed()),
                            "batch sealed"
                        );
                        // Publish the digest for the nodes subscribed to this gossip, i.e. the
                        // committee validators that consume individual current-epoch batches.
                        // Note, ignore error- this should not
                        // happen and should not cause an issue (except the
                        // underlying p2p network may be in trouble but that will manifest quickly).
                        let _ = self.network_handle.publish_batch(digest).await;
                    }
                    Err(e) => {
                        // On error the batch builder should leave the transactions in the pool for
                        // a future batch. So go ahead and remove so we
                        // don't try to re-inject them later.
                        let _ = self.store.remove::<OurNodeBatchesCache>(&digest);
                        return Err(match e {
                            crate::quorum_waiter::QuorumWaiterError::QuorumRejected => {
                                BlockSealError::QuorumRejected
                            }
                            crate::quorum_waiter::QuorumWaiterError::AntiQuorum => {
                                BlockSealError::AntiQuorum
                            }
                            crate::quorum_waiter::QuorumWaiterError::Timeout => {
                                BlockSealError::Timeout
                            }
                            crate::quorum_waiter::QuorumWaiterError::Network
                            | crate::quorum_waiter::QuorumWaiterError::DroppedReceiver
                            | crate::quorum_waiter::QuorumWaiterError::Rpc(_) => {
                                BlockSealError::FailedQuorum
                            }
                        });
                    }
                }
            }
            Err(e) => {
                error!(target: "worker::batch_provider", "Join error attempting batch quorum! {e}");
                // See remove comment above.
                let _ = self.store.remove::<OurNodeBatchesCache>(&digest);
                return Err(BlockSealError::FailedQuorum);
            }
        }

        // Save to live batch storage so other nodes can fetch it.
        // Keep OurNodeBatchesCache entry intact: if the epoch ends before this batch's cert is
        // committed, orphan_batches() will re-inject the transactions at the next epoch start.
        // Already-executed transactions are rejected by the pool (nonce too low), so re-injection
        // is always safe.
        if let Err(e) = self.store.insert::<NodeBatchesCache>(&digest, &batch) {
            error!(target: "worker::batch_provider", "Store failed with error: {:?}", e);
            return Err(BlockSealError::FatalDBFailure);
        }

        // Send the batch to the primary.
        let message = WorkerOwnBatchMessage::new(self.id, digest);
        let report_start = Instant::now();
        let reported = self.client.report_own_batch(message).await;
        let report_elapsed = report_start.elapsed();
        self.metrics.record_batch_report(report_elapsed);
        if let Err(err) = reported {
            error!(target: "worker::batch_provider", "Failed to report our batch: {err:?}");
            Err(BlockSealError::FailedToReport)
        } else {
            info!(
                target: "consensus::metrics",
                digest = %digest,
                report_us = micros(report_elapsed),
                "batch reported"
            );
            if let Some(marks) = &self.reported_digests {
                marks.lock().put(digest, ());
            }
            // recorded only after a successful report: a failed report leaves the transactions
            // in the pool for the builder to seal again, and an earlier record would count them
            // once per attempt (issue #1444)
            self.metrics.record_batch_sealed(batch.size(), batch.transactions.len());
            Ok(())
        }
    }

    /// True if `digest` counts as already reported for switch `digest_dedupe` (issue #1807).
    ///
    /// Both must hold:
    /// - this worker's latest seal of `digest` was acked by the primary: `report_own_batch` returns
    ///   `Ok` only after the proposer acks the digest, and the proposer then either queues it or
    ///   drops it because it already holds a copy; either way it keeps proposing the digest until
    ///   it is committed or the epoch ends;
    /// - the digest still has its [`OurNodeBatchesCache`] row, so the batch is not yet committed
    ///   (the row is evicted when its output is forwarded to the engine) and orphan recovery at the
    ///   next epoch's start can re-pool its transactions if the epoch closes first.
    ///
    /// The cache row alone is not enough: it is written before quorum and kept when the report
    /// fails, the store insert fails, the seal is cancelled midway, or the node restarts (the
    /// proposer's queue does not survive a restart). Treating any of those as reported would drop
    /// a batch the primary never received, so all of them seal and report again. A failed store
    /// read also answers `false`: the seal then follows the switch-off path, whose cache insert
    /// surfaces a broken store.
    ///
    /// One case suppresses a seal it need not: if evicting a committed batch's row fails (that
    /// failure is only logged), a re-seal of its digest is still suppressed. Its transactions are
    /// then delayed, not lost: the surviving row lets orphan recovery re-pool them at the next
    /// epoch's start.
    fn reported_and_uncommitted(
        &self,
        marks: &Mutex<LruCache<BlockHash, ()>>,
        digest: &BlockHash,
    ) -> bool {
        // the lock is released before the store read
        let acked = marks.lock().contains(digest);
        acked && self.store.contains_key::<OurNodeBatchesCache>(digest).unwrap_or(false)
    }
}

/// Whole milliseconds of `elapsed` for the `consensus::metrics` lines, saturating.
fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Whole microseconds of `elapsed`, saturating, for stages that are normally sub-millisecond.
fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    // ---- bench6 digest-dedupe ----

    use super::*;
    use crate::quorum_waiter::QuorumWaiterError;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tn_network_types::WorkerOthersBatchMessage;
    use tn_storage::mem_db::MemDatabase;
    use tn_types::{Batch, NoopTxnForwarder, TaskSpawner};
    use tokio::sync::oneshot;

    /// `tn_worker.duplicate_digest_suppressed_total`.
    const SUPPRESSED: &str = "tn_worker.duplicate_digest_suppressed_total";

    /// Quorum waiter that answers at once: it accepts unless `reject` is set, and counts calls.
    #[derive(Clone, Default)]
    struct ScriptedQuorum {
        /// Batches sent for quorum.
        calls: Arc<AtomicUsize>,
        /// Answer `QuorumRejected` while set.
        reject: Arc<AtomicBool>,
    }

    impl QuorumWaiterTrait for ScriptedQuorum {
        fn verify_batch(
            &self,
            _batch: SealedBatch,
            _timeout: Duration,
            _task_spawner: &TaskSpawner,
        ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = oneshot::channel();
            let answer = if self.reject.load(Ordering::SeqCst) {
                Err(QuorumWaiterError::QuorumRejected)
            } else {
                Ok(())
            };
            let _ = tx.send(answer);
            rx
        }
    }

    /// Primary side of the local network: records every own-batch report and fails it while
    /// `fail` is set, as a primary whose proposer stopped taking digests does.
    #[derive(Clone, Default)]
    struct ScriptedPrimary {
        /// Every own-batch report received, acked or not.
        attempts: Arc<parking_lot::Mutex<Vec<BlockHash>>>,
        /// Reports answered `Ok`.
        acked: Arc<AtomicUsize>,
        /// Fail reports while set.
        fail: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl WorkerToPrimaryClient for ScriptedPrimary {
        async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()> {
            self.attempts.lock().push(request.digest());
            if self.fail.load(Ordering::SeqCst) {
                return Err(eyre::eyre!("proposer stopped taking digests"));
            }
            self.acked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn report_others_batch(
            &self,
            _request: WorkerOthersBatchMessage,
        ) -> eyre::Result<()> {
            Ok(())
        }
    }

    /// A validator worker over a memory store, its scripted quorum and primary, and a snapshotter
    /// for the worker's metrics.
    struct Harness {
        /// The worker under test.
        worker: Worker<MemDatabase, ScriptedQuorum>,
        /// The worker's store.
        store: MemDatabase,
        /// The worker's quorum waiter.
        quorum: ScriptedQuorum,
        /// The primary the worker reports to.
        primary: ScriptedPrimary,
        /// Snapshots the recorder the worker's metric handles were created under.
        snapshotter: Snapshotter,
        /// Keeps the task spawner behind the worker's network handle alive.
        _task_manager: TaskManager,
    }

    impl Harness {
        /// A worker with switch `digest_dedupe` set to `dedupe`.
        fn new(dedupe: bool) -> Self {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let store = MemDatabase::default();
            let quorum = ScriptedQuorum::default();
            let primary = ScriptedPrimary::default();
            let client = LocalNetwork::new_with_empty_id();
            client
                .set_worker_to_primary_local_handler(Arc::new(primary.clone()))
                .expect("register scripted primary");
            let task_manager = TaskManager::default();
            // the metric handles bind to the recorder they are created under
            let worker = metrics::with_local_recorder(&recorder, || {
                Worker::new(
                    0,
                    Some(quorum.clone()),
                    client,
                    store.clone(),
                    Duration::from_secs(5),
                    WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
                    Arc::new(NoopTxnForwarder),
                    Vec::new(),
                )
                .with_digest_dedupe(dedupe)
            });
            Self { worker, store, quorum, primary, snapshotter, _task_manager: task_manager }
        }

        /// Quorum requests so far.
        fn quorum_calls(&self) -> usize {
            self.quorum.calls.load(Ordering::SeqCst)
        }

        /// Own-batch reports the primary received so far, acked or not.
        fn report_attempts(&self) -> Vec<BlockHash> {
            self.primary.attempts.lock().clone()
        }

        /// True if `digest` has an [`OurNodeBatchesCache`] row.
        fn cached(&self, digest: &BlockHash) -> bool {
            self.store.contains_key::<OurNodeBatchesCache>(digest).expect("read batch cache")
        }

        /// The suppression counter. A snapshot drains the recorder, so read it once per test.
        fn suppressed(&self) -> u64 {
            self.snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .find_map(|(key, _, _, value)| match value {
                    DebugValue::Counter(count) if key.key().name() == SUPPRESSED => Some(count),
                    _ => None,
                })
                .expect("suppression counter registered")
        }
    }

    /// A sealed batch whose single transaction is `seed` repeated; equal seeds give equal digests.
    fn batch(seed: u8) -> SealedBatch {
        Batch { transactions: vec![vec![seed; 32]], ..Default::default() }.seal_slow()
    }

    /// Switch on: a digest the primary acked is not sent for quorum or reported again while its
    /// batch is uncommitted, the seal still answers `Ok`, and the counter moves (bench-6 worker
    /// digest-dedupe test case S4, issue #1807).
    #[tokio::test]
    async fn dedupe_on_reports_an_acked_digest_once() {
        let h = Harness::new(true);
        let digest = batch(1).digest();

        h.worker.seal(batch(1)).await.expect("first seal");
        h.worker.seal(batch(1)).await.expect("repeat seal answers ok");

        assert_eq!(h.report_attempts(), vec![digest], "the primary hears the digest once");
        assert_eq!(h.quorum_calls(), 1, "the repeat asks no peer to vote");
        assert!(h.cached(&digest), "the cache row is kept for orphan recovery");
        assert_eq!(h.suppressed(), 1);
    }

    /// Switch on: consensus shutdown still refuses a seal before the dedupe check, so a marked,
    /// uncommitted digest sealed after shutdown answers `ConsensusShuttingDown`, not `Ok`.
    #[tokio::test]
    async fn dedupe_on_consensus_shutdown_wins_over_a_mark() {
        let h = Harness::new(true);
        let shutdown = ShutdownNotifier::new();
        let h = Harness { worker: h.worker.with_consensus_shutdown(shutdown.clone()), ..h };

        h.worker.seal(batch(7)).await.expect("seal before shutdown");
        shutdown.notify();
        let res = h.worker.seal(batch(7)).await;

        assert!(matches!(res, Err(BlockSealError::ConsensusShuttingDown)), "unexpected: {res:?}");
        assert_eq!(h.report_attempts().len(), 1);
        assert_eq!(h.suppressed(), 0);
    }

    /// Switch on: a seal whose report failed leaves no mark, so the retry of the same digest is
    /// sealed and reported; only after that report is acked is a further repeat suppressed.
    /// This is the bench-5 epoch-boundary shape (`FailedToReport`, then rebuilds of one digest).
    #[tokio::test]
    async fn dedupe_on_reports_again_after_a_failed_report() {
        let h = Harness::new(true);
        let digest = batch(2).digest();

        h.primary.fail.store(true, Ordering::SeqCst);
        let res = h.worker.seal(batch(2)).await;
        assert!(matches!(res, Err(BlockSealError::FailedToReport)), "unexpected: {res:?}");
        assert!(h.cached(&digest), "a failed report keeps its cache row, as with the switch off");

        h.primary.fail.store(false, Ordering::SeqCst);
        h.worker.seal(batch(2)).await.expect("retry is reported");
        assert_eq!(h.report_attempts(), vec![digest, digest], "the retry reached the primary");
        assert_eq!(h.primary.acked.load(Ordering::SeqCst), 1);
        assert_eq!(h.quorum_calls(), 2);

        h.worker.seal(batch(2)).await.expect("repeat after the ack answers ok");
        assert_eq!(h.report_attempts().len(), 2, "no report after the ack");
        assert_eq!(h.quorum_calls(), 2);
        assert_eq!(h.suppressed(), 1, "only the repeat after the ack is suppressed");
    }

    /// Switch on: an acked digest whose report later fails on a new seal is not trusted any more.
    /// The new attempt clears the old mark first, so a failed latest report is retried.
    #[tokio::test]
    async fn dedupe_on_latest_failed_report_clears_an_earlier_ack() {
        let h = Harness::new(true);
        let digest = batch(3).digest();

        h.worker.seal(batch(3)).await.expect("first seal");
        // the batch commits: its rows are evicted, so a later seal of the digest is new work
        h.store.remove::<OurNodeBatchesCache>(&digest).expect("evict committed row");

        h.primary.fail.store(true, Ordering::SeqCst);
        let res = h.worker.seal(batch(3)).await;
        assert!(matches!(res, Err(BlockSealError::FailedToReport)), "unexpected: {res:?}");

        h.primary.fail.store(false, Ordering::SeqCst);
        h.worker.seal(batch(3)).await.expect("retry is reported");
        assert_eq!(h.report_attempts(), vec![digest, digest, digest]);
        assert_eq!(h.primary.acked.load(Ordering::SeqCst), 2);
        assert_eq!(h.suppressed(), 0);
    }

    /// Switch on: once the batch commits and its cache row is evicted, a re-seal of the same
    /// digest is reported again. Bench 5's `0x9c01a8c5…` re-seal landed transactions the first,
    /// committed copy had skipped.
    #[tokio::test]
    async fn dedupe_on_reports_again_after_commit() {
        let h = Harness::new(true);
        let digest = batch(4).digest();

        h.worker.seal(batch(4)).await.expect("first seal");
        h.store.remove::<OurNodeBatchesCache>(&digest).expect("evict committed row");
        h.worker.seal(batch(4)).await.expect("re-seal after commit");

        assert_eq!(h.report_attempts(), vec![digest, digest]);
        assert_eq!(h.quorum_calls(), 2);
        assert_eq!(h.suppressed(), 0);
    }

    /// Switch off (the default): both seals of one digest go for quorum and are reported, as
    /// before the switch existed, and the worker keeps no marks.
    #[tokio::test]
    async fn dedupe_off_reports_every_seal() {
        let h = Harness::new(false);
        let digest = batch(5).digest();

        h.worker.seal(batch(5)).await.expect("first seal");
        h.worker.seal(batch(5)).await.expect("second seal");

        assert_eq!(h.report_attempts(), vec![digest, digest]);
        assert_eq!(h.quorum_calls(), 2);
        assert!(h.cached(&digest));
        assert!(h.worker.reported_digests.is_none(), "no marks with the switch off");
        assert_eq!(h.suppressed(), 0);
    }

    /// Switch on: the batch cache keeps its own eviction. A rejected quorum still removes the row
    /// and leaves no mark, so the retry is sealed and reported.
    #[tokio::test]
    async fn dedupe_on_quorum_failure_keeps_cache_eviction() {
        let h = Harness::new(true);
        let digest = batch(6).digest();

        h.quorum.reject.store(true, Ordering::SeqCst);
        let res = h.worker.seal(batch(6)).await;
        assert!(matches!(res, Err(BlockSealError::QuorumRejected)), "unexpected: {res:?}");
        assert!(!h.cached(&digest), "a failed quorum removes the row, as with the switch off");
        assert!(h.report_attempts().is_empty());

        h.quorum.reject.store(false, Ordering::SeqCst);
        h.worker.seal(batch(6)).await.expect("retry");
        assert_eq!(h.report_attempts(), vec![digest]);
        assert_eq!(h.suppressed(), 0);
    }

    /// Switch on: the marks are bounded. Past [`REPORTED_DIGESTS_CAPACITY`] reports the oldest
    /// mark is dropped, and a re-seal of that digest fails open: it is reported again rather than
    /// dropped. The batch cache holds one row per uncommitted batch, as with the switch off.
    #[tokio::test]
    async fn dedupe_on_marks_are_bounded_and_fail_open() {
        let h = Harness::new(true);
        let capacity = REPORTED_DIGESTS_CAPACITY.get();
        let sealed: Vec<SealedBatch> = (0..=capacity)
            .map(|i| {
                let seed = u32::try_from(i).expect("small index").to_be_bytes().to_vec();
                Batch { transactions: vec![seed], ..Default::default() }.seal_slow()
            })
            .collect();
        let oldest = sealed[0].clone();
        for sealed_batch in sealed {
            h.worker.seal(sealed_batch).await.expect("seal");
        }
        let marks = h.worker.reported_digests.as_ref().expect("switch on keeps marks");
        assert_eq!(marks.lock().len(), capacity);
        assert!(!marks.lock().contains(&oldest.digest()));
        assert_eq!(h.store.iter::<OurNodeBatchesCache>().count(), capacity + 1);

        h.worker.seal(oldest.clone()).await.expect("re-seal of the dropped mark");
        let attempts = h.report_attempts();
        assert_eq!(attempts.len(), capacity + 2, "the dropped mark's digest is reported again");
        assert_eq!(attempts.last(), Some(&oldest.digest()));
        assert_eq!(h.suppressed(), 0);
    }
}
