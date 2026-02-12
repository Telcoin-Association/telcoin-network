//! Fetch batches from database or peers.
//!
//! This type handles requests from the primary. Requests sent to the `BatchFetcher` have already been certified.

use crate::network::{error::WorkerNetworkResult, WorkerNetworkHandle};
use std::collections::{HashMap, HashSet};
use tn_storage::tables::Batches;
use tn_types::{now, Batch, BlockHash, Database, DbTxMut};
use tracing::{debug, error, instrument};

#[cfg(test)]
#[path = "tests/batch_fetcher.rs"]
mod batch_fetcher_tests;

#[derive(Debug)]
pub(crate) struct BatchFetcher<DB> {
    /// The handle for the network.
    network: WorkerNetworkHandle,
    /// The local database instance.
    batch_store: DB,
}

impl<DB: Database> BatchFetcher<DB> {
    /// Create a new instance of `Self`.
    pub(crate) fn new(network: WorkerNetworkHandle, batch_store: DB) -> Self {
        Self { network, batch_store }
    }

    /// Bulk fetches payload from local storage and remote workers.
    ///
    /// This function performs infinite retries and until all batches are available. This is called by `Subscriber`
    /// for each consensus output. The number of batches in a committed subdag is expected to be 10s of kbs max.
    /// Returns an error if the database writes fail. Otherwise, tries all peers infinitely until all batches
    /// successfully fetched.
    ///
    /// SAFETY: 10-node committees * 6-round commit max * 5 batch max = 300 max batch digests possible
    /// 32bytes * 300 = 9.6 kb => well within 1MB max message size
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    pub(crate) async fn fetch_for_primary(
        &self,
        mut missing_digests: HashSet<BlockHash>,
    ) -> eyre::Result<HashMap<BlockHash, Batch>> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", missing_digests.len());

        // preallocate hashmap
        let mut fetched_batches = HashMap::with_capacity(missing_digests.len());

        // loop until `missing_digests` empty
        loop {
            debug!(target: "batch_fetcher", "loop start");
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // 1) fetch from local storage
            self.fetch_local(&mut missing_digests, &mut fetched_batches)?;

            // return if all batches recovered
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // 2) fetch from peers over network
            if let Ok(mut new_batches) = self.request_batches_from_peers(&mut missing_digests).await
            {
                // set received_at timestamp for remote batches
                let mut updated_new_batches = HashMap::new();
                let mut txn = self.batch_store.write_txn()?;

                // update batch timestamps and insert to db
                for (digest, batch) in
                    new_batches.iter_mut().filter(|(d, _)| missing_digests.remove(*d))
                {
                    batch.set_received_at(now());
                    updated_new_batches.insert(*digest, batch.clone());
                    // also persist the batches, so they are available after restarts
                    txn.insert::<Batches>(digest, &batch).inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to insert batch! Node shutting down..."))?;
                }

                // commit db after all inserts
                txn.commit().inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to commit batch! Node shutting down..."))?;

                // add recovered batches to final collection
                fetched_batches.extend(updated_new_batches.iter().map(|(d, b)| (*d, (*b).clone())));

                // return if done, otherwise try again
                if missing_digests.is_empty() {
                    return Ok(fetched_batches);
                }
            }
        }
    }

    /// Retrieve batches from the local database.
    fn fetch_local(
        &self,
        missing_digests: &mut HashSet<BlockHash>,
        fetched_batches: &mut HashMap<BlockHash, Batch>,
    ) -> eyre::Result<()> {
        // read from database
        debug!(target: "batch_fetcher", "Local attempt to fetch {} missing_digests", missing_digests.len());
        let local_batches = self.batch_store.multi_get::<Batches>(missing_digests.iter())?;
        for (digest, batch) in missing_digests.iter().zip(local_batches.into_iter()) {
            if let Some(batch) = batch {
                debug_assert_eq!(*digest, batch.digest());
                fetched_batches.insert(*digest, batch);
            }
        }

        // remove fetched batches from missing
        missing_digests.retain(|d| !fetched_batches.contains_key(d));

        Ok(())
    }

    /// Issue request_batches RPC and verifies response integrity
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    async fn request_batches_from_peers(
        &self,
        missing_digests: &mut HashSet<BlockHash>,
    ) -> WorkerNetworkResult<HashMap<BlockHash, Batch>> {
        // request batches and return to caller
        let recovered_batches =
            self.network.request_batches(missing_digests).await?.into_iter().collect();

        Ok(recovered_batches)
    }
}
