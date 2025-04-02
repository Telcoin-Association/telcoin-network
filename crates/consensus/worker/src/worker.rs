//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{
    batch_fetcher::BatchFetcher,
    metrics::{Metrics, WorkerMetrics},
    network::PrimaryReceiverHandler,
    quorum_waiter::{QuorumWaiter, QuorumWaiterTrait},
    WorkerNetworkHandle,
};
use std::{sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::tables::Batches;
use tn_types::{
    error::BlockSealError, network_public_key_to_libp2p, BatchSender, BatchValidation, Database,
    SealedBatch, WorkerId,
};
use tracing::{error, info};

#[cfg(test)]
#[path = "tests/batch_provider_tests.rs"]
pub mod batch_provider_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Spawn the worker.
///
/// Create an instance of `Self` and start all tasks to participate in consensus.
pub fn new_worker<DB: Database>(
    id: WorkerId,
    validator: Arc<dyn BatchValidation>,
    metrics: Metrics,
    consensus_config: ConsensusConfig<DB>,
    network_handle: WorkerNetworkHandle,
) -> Worker<DB, QuorumWaiter> {
    let worker_name = consensus_config.key_config().worker_network_public_key();
    let worker_peer_id = network_public_key_to_libp2p(&worker_name);
    info!(target: "worker::worker", "Boot worker node with id {} peer id {:?}", id, worker_peer_id,);

    let node_metrics = metrics.worker_metrics.clone();

    let batch_fetcher = BatchFetcher::new(
        network_handle.clone(),
        consensus_config.node_storage().clone(),
        node_metrics.clone(),
    );
    consensus_config.local_network().set_primary_to_worker_local_handler(Arc::new(
        PrimaryReceiverHandler {
            store: consensus_config.node_storage().clone(),
            request_batches_timeout: consensus_config.parameters().sync_retry_delay,
            network: Some(network_handle.clone()),
            batch_fetcher: Some(batch_fetcher),
            validator,
        },
    ));
    let batch_provider = new_worker_internal(
        id,
        &consensus_config,
        node_metrics,
        consensus_config.local_network().clone(),
        network_handle.clone(),
    );

    // NOTE: This log entry is used to compute performance.
    info!(target: "worker::worker",
        "Worker {} successfully booted on {}",
        id,
        consensus_config
            .worker_cache()
            .worker(consensus_config.authority().protocol_key(), &id)
            .expect("Our public key or worker id is not in the worker cache")
            .transactions
    );

    batch_provider
}

/// Builds a new batch provider responsible for handling client transactions.
fn new_worker_internal<DB: Database>(
    id: WorkerId,
    consensus_config: &ConsensusConfig<DB>,
    node_metrics: Arc<WorkerMetrics>,
    client: LocalNetwork,
    network_handle: WorkerNetworkHandle,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Starting handler for transactions");

    // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the batch
    // before forwarding the batch to the `Processor`
    let quorum_waiter = QuorumWaiter::new(
        consensus_config.authority().clone(),
        id,
        consensus_config.committee().clone(),
        consensus_config.worker_cache().clone(),
        network_handle.clone(),
        node_metrics.clone(),
    );

    Worker::new(
        id,
        quorum_waiter,
        node_metrics,
        client,
        consensus_config.node_storage().clone(),
        consensus_config.parameters().batch_vote_timeout,
        network_handle,
    )
}

/// Process batch from EL into sealed batches for CL.
#[derive(Clone)]
pub struct Worker<DB, QW> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to batches.
    quorum_waiter: QW,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The network client to send our batches to the primary.
    client: LocalNetwork,
    /// The batch store to store our own batches.
    store: DB,
    /// Channel sender for alternate batch submision if not calling seal directly.
    tx_batches: BatchSender,
    /// The amount of time to wait on a reply from peer before timing out.
    timeout: Duration,
    /// Worker network handle.
    network_handle: WorkerNetworkHandle,
}

impl<DB, QW> std::fmt::Debug for Worker<DB, QW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> Worker<DB, QW> {
    pub fn new(
        id: WorkerId,
        quorum_waiter: QW,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
        network_handle: WorkerNetworkHandle,
    ) -> Self {
        let (tx_batches, mut rx_batches) = tokio::sync::mpsc::channel(1000);
        let this = Self {
            id,
            quorum_waiter,
            node_metrics,
            client,
            store,
            tx_batches,
            timeout,
            network_handle,
        };
        let this_clone = this.clone();
        // Spawn a little task to accept batches from a channel and seal them that way.
        // Allows the engine to remain removed from the worker.
        tokio::spawn(async move {
            while let Some((batch, tx)) = rx_batches.recv().await {
                let res = this_clone.seal(batch).await;
                if tx.send(res).is_err() {
                    error!(target: "worker::batch_provider", "Error sending result to channel caller!  Channel closed.");
                }
            }
        });
        this
    }

    pub fn batches_tx(&self) -> BatchSender {
        self.tx_batches.clone()
    }

    /// Seal and broadcast the current batch.
    pub async fn seal(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let size = sealed_batch.size();

        self.node_metrics
            .created_batch_size
            .with_label_values(&["latest batch size"])
            .observe(size as f64);

        let batch_attest_handle =
            self.quorum_waiter.verify_batch(sealed_batch.clone(), self.timeout);

        // Wait for our batch to reach quorum or fail to do so.
        match batch_attest_handle.await {
            Ok(res) => {
                match res {
                    Ok(()) => {
                        // batch reached quorum!
                        // Publish the digest for any nodes listening to this gossip (non-committee
                        // members). Note, ignore error- this should not
                        // happen and should not cause an issue (except the
                        // underlying p2p network may be in trouble but that will manifest quickly).
                        let _ = self.network_handle.publish_batch(sealed_batch.digest()).await;
                    }
                    Err(e) => {
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
                            | crate::quorum_waiter::QuorumWaiterError::Rpc(_) => {
                                BlockSealError::FailedQuorum
                            }
                        })
                    }
                }
            }
            Err(e) => {
                error!(target: "worker::batch_provider", "Join error attempting batch quorum! {e}");
                return Err(BlockSealError::FailedQuorum);
            }
        }

        // Now save it to disk
        let (batch, digest) = sealed_batch.split();

        if let Err(e) = self.store.insert::<Batches>(&digest, &batch) {
            error!(target: "worker::batch_provider", "Store failed with error: {:?}", e);
            return Err(BlockSealError::FatalDBFailure);
        }

        // Send the batch to the primary.
        let message =
            WorkerOwnBatchMessage { worker_id: self.id, digest, timestamp: batch.created_at() };
        if let Err(err) = self.client.report_own_batch(message).await {
            error!(target: "worker::batch_provider", "Failed to report our batch: {err:?}");
            // Should we return an error here?  Doing so complicates some tests but also the batch
            // is sealed, etc. If we can not report our own batch is this a
            // showstopper?
        }

        Ok(())
    }
}
