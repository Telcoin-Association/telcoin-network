//! Worker <-> Primary networking logic.
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tn_network_types::{PrimaryToWorkerClient, WorkerSynchronizeMessage};
use tn_storage::tables::NodeBatchesCache;
use tn_types::{now, Batch, BatchValidation, BlockHash, Database, DbTxMut as _, SealedBatch};
use tracing::{debug, trace};

use crate::{batch_fetcher::BatchFetcher, WorkerNetworkError, WorkerNetworkHandle};

/// Defines how the network receiver handles incoming primary messages.
#[derive(Debug)]
pub(crate) struct PrimaryReceiverHandler<DB> {
    /// The local batch store
    pub store: DB,
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
            match self.store.get::<NodeBatchesCache>(digest) {
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

        let response = network.request_batches(&mut missing).await?;

        // SAFETY: `request_batches` ensures the batch digest matches
        let sealed_batches_from_response: Vec<SealedBatch> =
            response.into_iter().map(|(digest, batch)| batch.seal(digest)).collect();

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
                tx.insert::<NodeBatchesCache>(&digest, &batch).map_err(|e| {
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

    async fn fetch_batches(
        &self,
        digests: HashSet<BlockHash>,
    ) -> eyre::Result<HashMap<BlockHash, Batch>> {
        // option approach required for startup - this should never happen
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(eyre::eyre!(
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };

        batch_fetcher.fetch_for_primary(digests).await
    }
}
