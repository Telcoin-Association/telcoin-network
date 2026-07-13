//! Worker <-> Primary networking logic.
use std::{
    collections::{BTreeSet, HashMap},
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
    pub batch_fetcher: BatchFetcher<DB>,
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
        let mut missing = BTreeSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.batch_fetcher.fetch_local_batch(*digest).await {
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

        // `request_batches` removes each successfully fetched digest from `missing` as it goes,
        // so snapshot the originally requested set before the call. Afterwards `missing` holds
        // only the digests that could not be fetched.
        let requested = missing.clone();
        let response = network.request_batches(&mut missing).await?;

        self.store_synced_batches(response, &requested, &missing, message.is_certified)
    }

    async fn fetch_batches(
        &self,
        digests: BTreeSet<BlockHash>,
    ) -> eyre::Result<HashMap<BlockHash, Batch>> {
        Ok(self.batch_fetcher.fetch_for_primary(digests).await?)
    }
}

impl<DB: Database> PrimaryReceiverHandler<DB> {
    /// Validate and persist the batches returned by
    /// [`WorkerNetworkHandle::request_batches`].
    ///
    /// `requested` is the set of digests originally requested, snapshotted *before* the fetch
    /// (which drains it), and `unfetched` is whatever digests remained afterwards. Each returned
    /// batch is validated (unless it belongs to a certificate), checked against `requested`, and
    /// written in a single transaction.
    ///
    /// Membership is checked against `requested` rather than the live set because
    /// `request_batches` already removes every fetched digest from the set it is given;
    /// re-checking the drained set here would spuriously reject every batch. Returns an error if
    /// a batch fails validation, an unrequested batch is returned, or any requested batch is
    /// still missing.
    fn store_synced_batches(
        &self,
        response: Vec<(BlockHash, Batch)>,
        requested: &BTreeSet<BlockHash>,
        unfetched: &BTreeSet<BlockHash>,
        is_certified: bool,
    ) -> eyre::Result<()> {
        // SAFETY: `request_batches` ensures the batch digest matches
        let sealed_batches_from_response: Vec<SealedBatch> =
            response.into_iter().map(|(digest, batch)| batch.seal(digest)).collect();

        // open db write tx
        let mut tx = self.store.write_txn().map_err(|e| {
            WorkerNetworkError::Internal(format!(
                "failed to create batch transaction to commit: {e:?}"
            ))
        })?;

        // loop through responses
        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
                    return Err(eyre::eyre!("Invalid batch: {err}"));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if requested.contains(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());

                // insert batch
                tx.insert::<NodeBatchesCache>(&digest, &batch).map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to batch transaction to commit: {e:?}"
                    ))
                })?;
            } else {
                return Err(eyre::eyre!(format!(
                    "failed to synchronize batches- received a batch {digest} we did not request!"
                )));
            }
        }

        // commit all batch db writes
        tx.commit()
            .map_err(|e| WorkerNetworkError::Internal(format!("failed to commit batch: {e:?}")))?;

        if unfetched.is_empty() {
            return Ok(());
        }

        Err(eyre::eyre!("failed to synchronize batches!".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_batches;
    use tempfile::TempDir;
    use tn_batch_validator::NoopBatchValidator;
    use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
    use tn_test_utils::CommitteeFixture;
    use tn_types::TaskManager;

    /// Build a [`PrimaryReceiverHandler`] backed by an in-memory store. Returns the handler, a
    /// clone of the store for assertions, and the temp dir backing the consensus chain (kept
    /// alive for the duration of the test).
    fn test_handler() -> (PrimaryReceiverHandler<MemDatabase>, MemDatabase, TempDir) {
        let store = MemDatabase::default();
        let temp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_path_buf(), committee).expect("consensus chain");
        let batch_fetcher = BatchFetcher::new(
            handle,
            store.clone(),
            consensus_chain,
            crate::metrics::WorkerMetrics::new_for_worker(0),
        );
        let handler = PrimaryReceiverHandler {
            store: store.clone(),
            network: None,
            batch_fetcher,
            validator: Arc::new(NoopBatchValidator),
        };
        (handler, store, temp_dir)
    }

    /// Regression test for issue #800: `request_batches` drains the requested set as it fetches,
    /// so the post-fetch membership check must be made against the pre-drain snapshot. Every
    /// requested batch that came back must be validated and persisted, and the sync must succeed.
    #[tokio::test]
    async fn store_synced_batches_happy_path_stores_all() {
        let (handler, store, _temp) = test_handler();
        let batches = create_test_batches(2);
        let requested: BTreeSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let response: Vec<(BlockHash, Batch)> =
            batches.iter().map(|b| (b.digest(), b.clone())).collect();
        // every requested digest was fetched, so nothing remains unfetched
        let unfetched = BTreeSet::new();

        // is_certified = false so the validation path is exercised as well
        handler
            .store_synced_batches(response, &requested, &unfetched, false)
            .expect("all requested batches should be stored");

        for batch in &batches {
            let stored = store
                .get::<NodeBatchesCache>(&batch.digest())
                .expect("read store")
                .expect("batch should be persisted");
            assert_eq!(stored.digest(), batch.digest());
        }
    }

    /// When some requested batches could not be fetched (`unfetched` is non-empty), the sync must
    /// report failure even though the batches that did arrive are still committed.
    #[tokio::test]
    async fn store_synced_batches_incomplete_fetch_errors() {
        let (handler, store, _temp) = test_handler();
        let batches = create_test_batches(2);
        let requested: BTreeSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let fetched = &batches[0];
        let still_missing = &batches[1];
        // only the first batch came back; the second remains unfetched
        let response = vec![(fetched.digest(), fetched.clone())];
        let unfetched = BTreeSet::from([still_missing.digest()]);

        let result = handler.store_synced_batches(response, &requested, &unfetched, true);
        assert!(result.is_err(), "incomplete fetch should error");

        // the fetched batch is committed; the missing one is absent
        let fetched_present =
            store.get::<NodeBatchesCache>(&fetched.digest()).expect("read store").is_some();
        let missing_absent =
            store.get::<NodeBatchesCache>(&still_missing.digest()).expect("read store").is_none();
        assert!(fetched_present, "the fetched batch should be committed");
        assert!(missing_absent, "the unfetched batch should not be present");
    }

    /// A returned batch whose digest was never requested is a protocol violation and must error.
    #[tokio::test]
    async fn store_synced_batches_unrequested_batch_errors() {
        let (handler, _store, _temp) = test_handler();
        let batches = create_test_batches(1);
        let batch = &batches[0];
        // the request set is empty, yet a batch comes back
        let requested = BTreeSet::new();
        let response = vec![(batch.digest(), batch.clone())];
        let unfetched = BTreeSet::new();

        let result = handler.store_synced_batches(response, &requested, &unfetched, true);
        assert!(result.is_err(), "an unrequested batch should error");
    }
}
