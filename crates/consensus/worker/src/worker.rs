//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{
    batch_fetcher::BatchFetcher,
    metrics::WorkerMetrics,
    network::primary::PrimaryReceiverHandler,
    quorum_waiter::{QuorumWaiter, QuorumWaiterTrait},
    WorkerNetworkHandle,
};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use std::{sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network_libp2p::error::NetworkError;
use tn_network_types::{local::LocalNetwork, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::ConsensusChain,
    tables::{NodeBatchesCache, OurNodeBatchesCache},
};
use tn_types::{
    error::BlockSealError, BatchReceiver, BatchSender, BatchValidation, BlsPublicKey, Database,
    SealedBatch, TaskManager, WorkerId,
};
use tracing::{error, info, instrument, warn};

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Maximum number of attempts to push transactions to a single committee member.
const MAX_TXN_PUSH_ATTEMPTS: u32 = 3;

/// Base backoff between transaction-push retries (doubles each attempt).
const TXN_PUSH_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Spawn the worker.
///
/// Create an instance of `Self` and start all tasks to participate in consensus.
pub fn new_worker<DB: Database>(
    id: WorkerId,
    validator: Arc<dyn BatchValidation>,
    consensus_config: ConsensusConfig<DB>,
    network_handle: WorkerNetworkHandle,
    consensus_chain: ConsensusChain,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Boot worker node with id {} key {:?}", id, consensus_config.key_config().primary_public_key());

    let batch_fetcher = BatchFetcher::new(
        network_handle.clone(),
        consensus_config.node_storage().clone(),
        consensus_chain,
        WorkerMetrics::new_for_worker(id),
    );
    consensus_config.local_network().set_primary_to_worker_local_handler(Arc::new(
        PrimaryReceiverHandler {
            store: consensus_config.node_storage().clone(),
            network: Some(network_handle.clone()),
            batch_fetcher,
            validator,
        },
    ));
    let batch_provider = new_worker_internal(
        id,
        &consensus_config,
        consensus_config.local_network().clone(),
        network_handle.clone(),
    );

    // NOTE: This log entry is used to compute performance.
    info!(target: "worker::worker",
        "Worker {} successfully booted on {}",
        id,
        consensus_config.config().node_info.p2p_info.worker.network_address
    );

    batch_provider
}

/// Builds a new batch provider responsible for handling client transactions.
fn new_worker_internal<DB: Database>(
    id: WorkerId,
    consensus_config: &ConsensusConfig<DB>,
    client: LocalNetwork,
    network_handle: WorkerNetworkHandle,
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

    // Committee validators to push accepted transactions to when this node is not a CVV.
    // A non-committee node (`authority()` is `None`) pushes to every committee member; a
    // committee member excludes itself.
    let committee_txn_targets = match consensus_config.authority() {
        Some(authority) => {
            consensus_config.committee().others_keys_except(authority.protocol_key())
        }
        None => consensus_config.committee().bls_keys().into_iter().collect(),
    };

    Worker::new(
        id,
        quorum_waiter,
        client,
        consensus_config.node_storage().clone(),
        consensus_config.parameters().batch_vote_timeout,
        network_handle,
        committee_txn_targets,
    )
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
    /// Committee validators to push accepted transactions to when this node is not a
    /// committee voting validator.
    ///
    /// Populated once per epoch at construction (issue #804): non-CVV workers push the
    /// transactions they accept to these peers over RPC instead of gossiping them.
    committee_txn_targets: Vec<BlsPublicKey>,
    /// Prometheus metrics for this worker.
    metrics: WorkerMetrics,
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
            committee_txn_targets: self.committee_txn_targets.clone(),
            metrics: self.metrics.clone(),
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
    pub fn new(
        id: WorkerId,
        quorum_waiter: Option<QW>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
        network_handle: WorkerNetworkHandle,
        committee_txn_targets: Vec<BlsPublicKey>,
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
            committee_txn_targets,
            metrics: WorkerMetrics::new_for_worker(id),
        }
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

    /// Return the network handle for this worker.
    pub fn network_handle(&self) -> WorkerNetworkHandle {
        self.network_handle.clone()
    }

    /// The sender end of the batch submit channel.
    pub fn batches_tx(&self) -> BatchSender {
        self.tx_batches.clone()
    }

    /// Send all the txns in `sealed_batch` to committee validators so they can be included
    /// in blocks. Use this when not a CVV so that transactions you accept can be included.
    ///
    /// Replaces the previous gossip broadcast with a direct RPC push to each committee
    /// validator (issue #804). The push is best-effort and runs on a background task so
    /// batch production is never stalled by a slow or unreachable committee member; each
    /// recipient independently keeps only the transactions in its own shard.
    pub async fn disburse_txns(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let transactions = sealed_batch.batch.transactions;
        if transactions.is_empty() || self.committee_txn_targets.is_empty() {
            return Ok(());
        }

        let network_handle = self.network_handle.clone();
        let targets = self.committee_txn_targets.clone();
        self.network_handle.get_task_spawner().spawn_task("disburse-txns", async move {
            Self::push_txns_to_committee(&network_handle, &targets, transactions).await;
            Ok(())
        });

        Ok(())
    }

    /// Push `transactions` to every committee validator in `targets` over RPC concurrently.
    /// Best-effort: a member that cannot be reached (after retries) simply does not receive
    /// the transactions this round.
    async fn push_txns_to_committee(
        network_handle: &WorkerNetworkHandle,
        targets: &[BlsPublicKey],
        transactions: Vec<Vec<u8>>,
    ) {
        let mut sends: FuturesUnordered<_> = targets
            .iter()
            .map(|&peer| {
                let network_handle = network_handle.clone();
                let transactions = transactions.clone();
                async move { Self::push_txns_to_peer(&network_handle, peer, transactions).await }
            })
            .collect();

        while sends.next().await.is_some() {}
    }

    /// Push `transactions` to a single committee member, retrying transient RPC errors with
    /// exponential backoff (mirroring the batch-report path so a peer that is briefly
    /// unreachable at an epoch boundary is not skipped). A permanent rejection
    /// ([`NetworkError::RPCError`]) is not retried. Both give-up outcomes only warn-log,
    /// since transaction disbursal is best-effort.
    async fn push_txns_to_peer(
        network_handle: &WorkerNetworkHandle,
        peer: BlsPublicKey,
        transactions: Vec<Vec<u8>>,
    ) {
        for attempt in 0..MAX_TXN_PUSH_ATTEMPTS {
            match network_handle.report_txns(peer, transactions.clone()).await {
                Ok(()) => return,
                // permanent rejection - do not retry
                Err(NetworkError::RPCError(msg)) => {
                    warn!(
                        target: "worker::batch_provider",
                        %peer,
                        %msg,
                        "committee member rejected transaction push"
                    );
                    return;
                }
                // transient error (RPCRetryable / transport) - retry with backoff
                Err(err) => {
                    if attempt + 1 < MAX_TXN_PUSH_ATTEMPTS {
                        tokio::time::sleep(TXN_PUSH_RETRY_BACKOFF * 2u32.pow(attempt)).await;
                    } else {
                        warn!(
                            target: "worker::batch_provider",
                            %peer,
                            ?err,
                            "failed to push transactions to committee member after retries"
                        );
                    }
                }
            }
        }
    }

    /// Seal and broadcast the current batch.
    #[instrument(level = "debug", skip_all, fields(batch_size = sealed_batch.size(), num_txs = sealed_batch.batch.transactions.len()))]
    pub async fn seal(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let Some(quorum_waiter) = &self.quorum_waiter else {
            // We are not a validator so need to send any transactions out for a CVV to pickup.
            return self.disburse_txns(sealed_batch).await;
        };

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
                        // Metric: batch_sealed - tracks successfully sealed batches
                        info!(
                            target: "consensus::metrics",
                            worker_id = self.id,
                            batch_size = batch.size(),
                            num_txs = batch.transactions.len(),
                            "batch sealed"
                        );
                        self.metrics.record_batch_sealed(batch.size(), batch.transactions.len());
                        // Publish the digest for any nodes listening to this gossip (non-committee
                        // members). Note, ignore error- this should not
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
        let message = WorkerOwnBatchMessage { worker_id: self.id, digest };
        if let Err(err) = self.client.report_own_batch(message).await {
            error!(target: "worker::batch_provider", "Failed to report our batch: {err:?}");
            Err(BlockSealError::FailedToReport)
        } else {
            Ok(())
        }
    }
}
