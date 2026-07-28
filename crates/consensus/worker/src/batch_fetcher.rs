//! Fetch batches from database or peers for this worker's primary.
//!
//! This type handles requests from the primary. Requests sent to the `BatchFetcher` have already
//! been certified.

use crate::{
    metrics::WorkerMetrics,
    network::{error::WorkerNetworkResult, WorkerNetworkHandle},
    WorkerNetworkError,
};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};
use tn_storage::{consensus::ConsensusChain, tables::NodeBatchesCache};
use tn_types::{now, Batch, BlockHash, Database, DbTxMut, Epoch, B256};
use tracing::{debug, error, instrument};

/// Minimum delay before retrying a [`BatchFetcher::fetch_for_primary`] pass that
/// recovered no batches (locally or from a peer).
///
/// The retry loop is otherwise unpaced: `request_batches` returns immediately,
/// without applying its own retry backoff, when the worker has no connected
/// peers, so an explicit floor here keeps a no-peer (or all-peers-failed)
/// condition from re-looping at the scheduler's full rate (see issue #865).
const FETCH_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(50);

/// Upper bound for the exponential backoff between no-progress retry passes.
///
/// Caps how long a persistent no-peer condition waits before re-checking, so a
/// worker peer that (re)connects is still picked up promptly.
const FETCH_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug)]
/// The type to fetch batches for the primary.
pub(crate) struct BatchFetcher<DB> {
    /// The handle for the network.
    network: WorkerNetworkHandle,
    /// The local database instance.
    batch_store: DB,
    /// The consensus chain.
    consensus_chain: ConsensusChain,
    /// Prometheus metrics for fetch operations.
    metrics: WorkerMetrics,
}

/// Retrieve a batch from local storage for batch_digest.
/// Use this as a helper to fetch a batch from the local cache or the
/// consensus chain.
pub(crate) async fn get_batch_local<DB>(
    epoch: Epoch,
    batch_digest: B256,
    store: &DB,
    consensus_chain: &ConsensusChain,
) -> WorkerNetworkResult<Option<Batch>>
where
    DB: Database,
{
    // read from database
    // look up batches from db
    if let Some(batch) = store
        .get::<NodeBatchesCache>(&batch_digest)
        .map_err(|e| WorkerNetworkError::Internal(format!("DB error: {e}")))?
    {
        Ok(Some(batch))
    } else {
        Ok(consensus_chain.get_batches(epoch, [batch_digest].iter()).await.into_iter().next())
    }
}

/// Retrieve a batch from the local cache for batch_digest.
/// Use this as a helper to fetch a batch from ONLY the local cache.
pub(crate) fn get_batch_local_cache<DB>(
    batch_digest: B256,
    store: &DB,
) -> WorkerNetworkResult<Option<Batch>>
where
    DB: Database,
{
    store
        .get::<NodeBatchesCache>(&batch_digest)
        .map_err(|e| WorkerNetworkError::Internal(format!("DB error: {e}")))
}

/// Retrieve batches from local storage for the list of batch_digests.
/// Use this as a helper to fetch batches from the local cache or the
/// consensus chain.
pub(crate) async fn get_batches_local<DB>(
    epoch: Epoch,
    batch_digests: &[B256],
    store: &DB,
    consensus_chain: &ConsensusChain,
) -> WorkerNetworkResult<Vec<Batch>>
where
    DB: Database,
{
    // read from database
    // look up batches from db
    let mut batches: Vec<_> = store
        .multi_get::<NodeBatchesCache>(batch_digests.iter())
        .map_err(|e| WorkerNetworkError::Internal(format!("DB error: {e}")))?
        .into_iter()
        .flatten() // removes `None`
        .collect();

    let batches = if batches.is_empty() {
        // Nothing in cache so get what we can from the consensus chain.
        consensus_chain.get_batches(epoch, batch_digests.iter()).await
    } else if batches.len() < batch_digests.len() {
        // Some not in cache so try to get the rest from the consensus chain.
        let found_digests: Vec<B256> = batches.iter().map(|b| b.digest()).collect();
        let mut missing = Vec::new();
        for digest in batch_digests.iter() {
            if !found_digests.contains(digest) {
                missing.push(*digest);
            }
        }
        batches.extend(consensus_chain.get_batches(epoch, missing.iter()).await);
        batches
    } else {
        // All the batches were in the cache.
        batches
    };

    Ok(batches)
}

impl<DB: Database> BatchFetcher<DB> {
    /// Create a new instance of `Self`.
    pub(crate) fn new(
        network: WorkerNetworkHandle,
        batch_store: DB,
        consensus_chain: ConsensusChain,
        metrics: WorkerMetrics,
    ) -> Self {
        Self { network, batch_store, consensus_chain, metrics }
    }

    /// Bulk fetches payload from local storage and remote workers.
    ///
    /// This function performs infinite retries and until all batches are available. This is called
    /// internally by `Subscriber` for each consensus output. The number of batches in a committed
    /// subdag is expected to be 10s of kbs max. Returns an error if the database writes fail.
    /// Otherwise, tries all peers infinitely until all batches successfully fetched.
    ///
    /// SAFETY: 10-node committees * 6-round commit max * 5 batch max = 300 max batch digests
    /// possible 32bytes * 300 = 9.6 kb => well within 1MB max message size
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    pub(crate) async fn fetch_for_primary(
        &self,
        missing_digests: BTreeSet<BlockHash>,
    ) -> WorkerNetworkResult<HashMap<BlockHash, Batch>> {
        let fetch_start = Instant::now();
        let result = self.fetch_for_primary_inner(missing_digests).await;
        self.metrics.record_batch_fetch_duration(fetch_start.elapsed());
        result
    }

    /// Inner fetch loop for [`Self::fetch_for_primary`] (split out so the wrapper can time it).
    async fn fetch_for_primary_inner(
        &self,
        mut missing_digests: BTreeSet<BlockHash>,
    ) -> WorkerNetworkResult<HashMap<BlockHash, Batch>> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", missing_digests.len());

        // preallocate hashmap
        let mut fetched_batches = HashMap::with_capacity(missing_digests.len());

        // delay before re-looping after a pass that recovers nothing, so a no-peer /
        // all-peers-failed condition backs off instead of spinning (see issue #865)
        let mut backoff = FETCH_RETRY_MIN_BACKOFF;

        // loop until `missing_digests` empty
        loop {
            debug!(target: "batch_fetcher", "loop start");
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // snapshot remaining digests so a pass that recovers none can back off
            let missing_before = missing_digests.len();

            // 1) fetch from local storage
            let local_before = fetched_batches.len();
            self.fetch_local(&mut missing_digests, &mut fetched_batches).await?;
            self.metrics.record_batches_fetched("local", fetched_batches.len() - local_before);

            // return if all batches recovered
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // 2) fetch from peers over network
            //
            // NOTE: `request_batches` performs bounded retries (3 attempts with 500ms backoff)
            // internally, so RPCError here means all retries were exhausted. The `if let Ok()`
            // intentionally swallows that error to retry the outer infinite loop, which will
            // re-check local storage and try peers again.
            if let Ok(mut new_batches) = self.request_batches_from_peers(&mut missing_digests).await
            {
                self.metrics.record_batches_fetched("remote", new_batches.len());

                // Only touch the batch store when peers actually returned something. An empty
                // response (no connected peers, or every peer failed) must not open and commit
                // an empty write transaction on every pass (see issue #865).
                if !new_batches.is_empty() {
                    // set received_at timestamp for remote batches
                    let mut updated_new_batches = HashMap::new();
                    let mut txn = self.batch_store.write_txn().map_err(|e| {
                        WorkerNetworkError::DBCommit(format!("Failed to retrieve write txn: {e}"))
                    })?;

                    // update batch timestamps and insert to db
                    //
                    // NOTE: `request_batches` already removed successful digests from
                    // `missing_digests`, so all returned batches are valid and should be stored.
                    for (digest, batch) in new_batches.iter_mut() {
                        batch.set_received_at(now());
                        updated_new_batches.insert(*digest, batch.clone());
                        // also persist the batches, so they are available after restarts
                        txn.insert::<NodeBatchesCache>(digest, batch)
                            .inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to insert batch! Node shutting down...")).map_err(|e| WorkerNetworkError::DBInsert(format!("Failed to insert: {e}")))?;
                    }

                    // commit db after all inserts
                    txn.commit()
                        .inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to commit batch! Node shutting down...")).map_err(|e| WorkerNetworkError::DBCommit(format!("Failed to commit: {e}")))?;

                    // add recovered batches to final collection
                    fetched_batches
                        .extend(updated_new_batches.iter().map(|(d, b)| (*d, (*b).clone())));

                    // return if done, otherwise try again
                    if missing_digests.is_empty() {
                        return Ok(fetched_batches);
                    }
                }
            }

            // Pace the retry loop: reset the backoff to the floor when this pass made progress
            // (some digests resolved), otherwise sleep for the current backoff and grow it toward
            // the cap. This keeps a persistent no-peer or all-peers-failed condition from spinning
            // without delaying a pass that is still fetching batches (see issue #865).
            if missing_digests.len() < missing_before {
                backoff = FETCH_RETRY_MIN_BACKOFF;
            } else {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(FETCH_RETRY_MAX_BACKOFF);
            }
        }
    }

    /// Retrieve batches from the local database.
    async fn fetch_local(
        &self,
        missing_digests: &mut BTreeSet<BlockHash>,
        fetched_batches: &mut HashMap<BlockHash, Batch>,
    ) -> WorkerNetworkResult<()> {
        // read from database
        debug!(target: "batch_fetcher", "Local attempt to fetch {} missing_digests", missing_digests.len());
        let missing_vec: Vec<B256> = missing_digests.iter().copied().collect();
        let local_batches = get_batches_local(
            self.network.epoch(),
            &missing_vec,
            &self.batch_store,
            &self.consensus_chain,
        )
        .await?;
        fetched_batches.extend(local_batches.into_iter().map(|b| (b.digest(), b)));

        // remove fetched batches from missing
        missing_digests.retain(|d| !fetched_batches.contains_key(d));

        Ok(())
    }

    /// Retrieve a batch from the local database.
    pub(crate) async fn fetch_local_batch(
        &self,
        digest: BlockHash,
    ) -> WorkerNetworkResult<Option<Batch>> {
        get_batch_local(self.network.epoch(), digest, &self.batch_store, &self.consensus_chain)
            .await
    }

    /// Issue request_batches RPC and verifies response integrity
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    async fn request_batches_from_peers(
        &self,
        missing_digests: &mut BTreeSet<BlockHash>,
    ) -> WorkerNetworkResult<HashMap<BlockHash, Batch>> {
        // request batches and return to caller
        let recovered_batches =
            self.network.request_batches(missing_digests).await?.into_iter().collect();

        Ok(recovered_batches)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        batch_fetcher::BatchFetcher,
        network::WorkerNetworkHandle,
        test_utils::{create_test_batches, setup_batch_db},
    };
    use std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tempfile::TempDir;
    use tn_network_libp2p::types::{NetworkCommand, NetworkHandle};
    use tn_storage::consensus::ConsensusChain;
    use tn_types::{BlockHash, Committee, TaskManager};
    use tokio::sync::mpsc;

    // ============================================================================
    // BatchFetcher Local-Only Tests
    // ============================================================================

    #[tokio::test]
    async fn test_fetch_for_primary_all_local() {
        let batches = create_test_batches(3);
        let db = setup_batch_db(&batches);
        let digests: BTreeSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let temp_dir = TempDir::new().expect("temp dir");

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fetcher = BatchFetcher::new(
            handle,
            db,
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain"),
            crate::metrics::WorkerMetrics::new_for_worker(0),
        );

        let result = fetcher.fetch_for_primary(digests.clone()).await.expect("fetch local batches");
        assert_eq!(result.len(), batches.len());
        for batch in &batches {
            assert!(result.contains_key(&batch.digest()));
        }
    }

    #[tokio::test]
    async fn test_fetch_for_primary_all_local_many() {
        let batches = create_test_batches(20);
        let db = setup_batch_db(&batches);
        let digests: BTreeSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let temp_dir = TempDir::new().expect("temp dir");

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fetcher = BatchFetcher::new(
            handle,
            db,
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain"),
            crate::metrics::WorkerMetrics::new_for_worker(0),
        );

        let result = fetcher.fetch_for_primary(digests.clone()).await.expect("fetch local batches");
        assert_eq!(result.len(), 20);
        for batch in &batches {
            assert!(result.contains_key(&batch.digest()));
        }
    }

    // ============================================================================
    // BatchFetcher No-Peer Backoff Test
    // ============================================================================

    /// Regression test for issue #865: with batches to fetch and no connected
    /// worker peers, `fetch_for_primary` must back off between attempts instead of
    /// spinning at the scheduler's full rate.
    ///
    /// A network handle answers every `ConnectedPeers` query with an empty peer
    /// set (the production no-peer path, where `request_batches` returns
    /// `Ok(empty)` without applying its own retry backoff) and counts the queries.
    /// The fetch never completes (the digests are never in the local db and no
    /// peer ever connects), so it runs in the background and is stopped after a
    /// fixed window; the number of `connected_peers` polls over that window must
    /// stay small. Without the per-pass backoff this loop polls thousands of times.
    #[tokio::test]
    async fn test_fetch_for_primary_backs_off_without_peers() {
        // digests that are never in the local db, so `fetch_local` cannot satisfy them
        let batches = create_test_batches(2);
        let digests: BTreeSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let db = setup_batch_db(&[]);
        let temp_dir = TempDir::new().expect("temp dir");

        // network handle whose `connected_peers` always reports "no peers" and counts calls
        let (tx, mut rx) = mpsc::channel(10);
        let task_manager = TaskManager::default();
        let handle =
            WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0, 0);

        let connected_peers_calls = Arc::new(AtomicUsize::new(0));
        let calls = connected_peers_calls.clone();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let NetworkCommand::ConnectedPeers { reply } = cmd {
                    calls.fetch_add(1, Ordering::Relaxed);
                    // no connected worker peers
                    let _ = reply.send(vec![]);
                }
            }
        });

        let fetcher = BatchFetcher::new(
            handle,
            db,
            ConsensusChain::new(temp_dir.path().to_path_buf(), Committee::default())
                .expect("consensus chain"),
            crate::metrics::WorkerMetrics::new_for_worker(0),
        );

        // `fetch_for_primary` retries forever while digests are missing and no peer
        // connects, so drive it in the background and stop it after a fixed window.
        let fetch = tokio::spawn(async move { fetcher.fetch_for_primary(digests).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        fetch.abort();

        let polls = connected_peers_calls.load(Ordering::Relaxed);
        assert!(polls > 0, "fetch loop should have attempted at least once");
        assert!(
            polls < 50,
            "no-peer fetch loop should back off, but polled connected_peers {polls} times in 500ms"
        );
    }
}
