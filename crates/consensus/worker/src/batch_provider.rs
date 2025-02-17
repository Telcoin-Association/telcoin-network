//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{metrics::WorkerMetrics, quorum_waiter::QuorumWaiterTrait};
use std::{sync::Arc, time::Duration};
use tn_network::{local::LocalNetwork, WorkerToPrimaryClient as _};
use tn_network_types::WorkerOwnBatchMessage;
use tn_storage::{tables::Batches};
use tn_types::{error::BlockSealError, BatchSender, Database, SealedBatch, WorkerId};
use tracing::error;

#[cfg(test)]
#[path = "tests/batch_provider_tests.rs"]
pub mod batch_provider_tests;

/// Process batch from EL into sealed batches for CL.
#[derive(Clone)]
pub struct BatchProvider<DB, QW> {
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
}

impl<DB, QW> std::fmt::Debug for BatchProvider<DB, QW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> BatchProvider<DB, QW> {
    pub fn new(
        id: WorkerId,
        quorum_waiter: QW,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
    ) -> Self {
        let (tx_batches, mut rx_batches) = tokio::sync::mpsc::channel(1000);
        let this = Self { id, quorum_waiter, node_metrics, client, store, tx_batches, timeout };
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
                    Ok(_) => {} // batch reached quorum!
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
