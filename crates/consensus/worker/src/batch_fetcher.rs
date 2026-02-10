//! Fetch batches from peers

use crate::{network::WorkerNetworkHandle, WorkerNetworkError};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tn_storage::tables::Batches;
use tn_types::{now, Batch, BlockHash, Database, DbTxMut};
use tracing::{debug, instrument};

#[cfg(test)]
#[path = "tests/batch_fetcher.rs"]
mod batch_fetcher_tests;

#[derive(Debug)]
pub(crate) struct BatchFetcher<DB> {
    network: WorkerNetworkHandle,
    batch_store: DB,
}

impl<DB: Database> BatchFetcher<DB> {
    /// Create a new instance of `Self`.
    pub(crate) fn new(network: WorkerNetworkHandle, batch_store: DB) -> Self {
        Self { network, batch_store }
    }

    /// Bulk fetches payload from local storage and remote workers.
    /// This function performs infinite retries and until all batches are available.
    #[instrument(level = "debug", skip_all, fields(num_digests = digests.len()))]
    pub(crate) async fn fetch(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, Batch> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", digests.len(),);

        let mut remaining_digests = digests;
        let mut fetched_batches = HashMap::new();

        loop {
            debug!(target: "batch_fetcher", "looooop");
            if remaining_digests.is_empty() {
                return fetched_batches;
            }

            // Fetch from local storage.
            fetched_batches.extend(self.fetch_local(remaining_digests.clone()).await);
            remaining_digests.retain(|d| !fetched_batches.contains_key(d));
            if remaining_digests.is_empty() {
                return fetched_batches;
            }

            // Fetch from peers.
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
                    fetched_batches.insert(digest, batch);
                }
            }
        }

        fetched_batches
    }

    /// Issue request_batches RPC and verifies response integrity
    #[instrument(level = "debug", skip_all, fields(num_digests = digests_to_fetch.len()))]
    async fn safe_request_batches(
        &self,
        digests_to_fetch: &HashSet<BlockHash>,
        timeout: Duration,
    ) -> Result<HashMap<BlockHash, Batch>, WorkerNetworkError> {
        let mut fetched_batches = HashMap::new();
        if digests_to_fetch.is_empty() {
            return Ok(fetched_batches);
        }

        let batches = tokio::time::timeout(
            timeout,
            self.network.request_batches(digests_to_fetch.clone().into_iter().collect()),
        )
        .await??;
        for batch in batches {
            let batch_digest = batch.digest();
            // This batch is part of a certificate, so no need to validate it.
            fetched_batches.insert(batch_digest, batch);
        }

        Ok(fetched_batches)
    }
}
