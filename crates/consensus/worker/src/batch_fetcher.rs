//! Fetch batches from peers

use crate::{metrics::WorkerMetrics, network::WorkerNetworkHandle};
use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tn_network_libp2p::error::NetworkError;
use tn_storage::tables::Batches;
use tn_types::{now, Batch, BlockHash, Database, DbTxMut};
use tokio::time::error::Elapsed;
use tracing::debug;

#[cfg(test)]
#[path = "tests/batch_fetcher.rs"]
pub mod batch_fetcher_tests;

pub struct BatchFetcher<DB> {
    network: Arc<dyn RequestBatchesNetwork>,
    batch_store: DB,
    metrics: Arc<WorkerMetrics>,
}

impl<DB: Database> BatchFetcher<DB> {
    pub fn new(network: WorkerNetworkHandle, batch_store: DB, metrics: Arc<WorkerMetrics>) -> Self {
        Self { network: Arc::new(network), batch_store, metrics }
    }

    /// Bulk fetches payload from local storage and remote workers.
    /// This function performs infinite retries and until all batches are available.
    pub async fn fetch(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, Batch> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", digests.len(),);

        let mut remaining_digests = digests;
        let mut fetched_batches = HashMap::new();

        loop {
            if remaining_digests.is_empty() {
                return fetched_batches;
            }

            // Fetch from local storage.
            let _timer = self.metrics.worker_local_fetch_latency.start_timer();
            fetched_batches.extend(self.fetch_local(remaining_digests.clone()).await);
            remaining_digests.retain(|d| !fetched_batches.contains_key(d));
            if remaining_digests.is_empty() {
                return fetched_batches;
            }
            drop(_timer);

            // Fetch from peers.
            let _timer = self.metrics.worker_remote_fetch_latency.start_timer();
            if let Ok(new_batches) =
                self.safe_request_batches(&remaining_digests, Duration::from_secs(10)).await
            {
                // Set received_at timestamp for remote batches.
                let mut updated_new_batches = HashMap::new();
                let mut txn =
                    self.batch_store.write_txn().expect("unable to create DB transaction!");
                for (digest, batch) in
                    new_batches.iter().filter(|(d, _)| remaining_digests.remove(*d))
                {
                    let mut batch = (*batch).clone();
                    batch.set_received_at(now());
                    updated_new_batches.insert(*digest, batch.clone());
                    // Also persist the batches, so they are available after restarts.
                    if let Err(e) = txn.insert::<Batches>(digest, &batch) {
                        tracing::error!(target: "batch_fetcher", "failed to insert batch! We can not continue.. {e}");
                        panic!("failed to insert batch! We can not continue.. {e}");
                    }
                }
                if let Err(e) = txn.commit() {
                    tracing::error!(target: "batch_fetcher", "failed to commit batch! We can not continue.. {e}");
                    panic!("failed to commit batch! We can not continue.. {e}");
                }
                fetched_batches.extend(updated_new_batches.iter().map(|(d, b)| (*d, (*b).clone())));

                if remaining_digests.is_empty() {
                    return fetched_batches;
                }
            }
        }
    }

    async fn fetch_local(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, Batch> {
        let mut fetched_batches = HashMap::new();
        if digests.is_empty() {
            return fetched_batches;
        }

        // Continue to bulk request from local worker until no remaining digests
        // are available.
        debug!(target: "batch_fetcher", "Local attempt to fetch {} digests", digests.len());
        if let Ok(local_batches) = self.batch_store.multi_get::<Batches>(digests.iter()) {
            for (digest, batch) in digests.into_iter().zip(local_batches.into_iter()) {
                if let Some(batch) = batch {
                    self.metrics.batch_fetch.with_label_values(&["local", "success"]).inc();
                    fetched_batches.insert(digest, batch);
                } else {
                    self.metrics.batch_fetch.with_label_values(&["local", "missing"]).inc();
                }
            }
        }

        fetched_batches
    }

    /// Issue request_batches RPC and verifies response integrity
    async fn safe_request_batches(
        &self,
        digests_to_fetch: &HashSet<BlockHash>,
        timeout: Duration,
    ) -> Result<HashMap<BlockHash, Batch>, RequestBatchesNetworkError> {
        let mut fetched_batches = HashMap::new();
        if digests_to_fetch.is_empty() {
            return Ok(fetched_batches);
        }

        let batches = self
            .network
            .request_batches_from_all(digests_to_fetch.clone().into_iter().collect(), timeout)
            .await?;
        for batch in batches {
            let batch_digest = batch.digest();
            // This batch is part of a certificate, so no need to validate it.
            fetched_batches.insert(batch_digest, batch);
        }

        Ok(fetched_batches)
    }
}

/// Possible errors when requesting batches.
#[derive(Debug, Error)]
enum RequestBatchesNetworkError {
    #[error(transparent)]
    Timeout(#[from] Elapsed),
    #[error(transparent)]
    Network(#[from] NetworkError),
}

// Utility trait to add a timeout to a batch request.
#[async_trait]
trait RequestBatchesNetwork: Send + Sync {
    async fn request_batches_from_all(
        &self,
        batch_digests: Vec<BlockHash>,
        timeout: Duration,
    ) -> Result<Vec<Batch>, RequestBatchesNetworkError>;
}

#[async_trait]
impl RequestBatchesNetwork for WorkerNetworkHandle {
    async fn request_batches_from_all(
        &self,
        batch_digests: Vec<BlockHash>,
        timeout: Duration,
    ) -> Result<Vec<Batch>, RequestBatchesNetworkError> {
        let res = tokio::time::timeout(timeout, self.request_batches(batch_digests)).await??;
        Ok(res)
    }
}
