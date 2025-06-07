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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkerRequest, WorkerResponse};
    use rand::{rngs::StdRng, RngCore};
    use tempfile::TempDir;
    use tn_network_libp2p::{
        types::{NetworkCommand, NetworkHandle},
        PeerId,
    };
    use tn_reth::test_utils::transaction;
    use tn_storage::open_db;
    use tn_types::{NetworkKeypair, TaskManager};
    use tokio::sync::{mpsc, Mutex};

    #[tokio::test]
    pub async fn test_fetchertt() {
        let mut network = TestRequestBatchesNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let batch_store = open_db(temp_dir.path());
        let batch1 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch2 = Batch { transactions: vec![transaction()], ..Default::default() };
        let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest()]);
        network.put(&[1, 2], batch1.clone()).await;
        network.put(&[2, 3], batch2.clone()).await;
        let fetcher = BatchFetcher {
            network: Arc::new(network.handle()),
            batch_store: batch_store.clone(),
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_batches = HashMap::from_iter(vec![
            (batch1.digest(), batch1.clone()),
            (batch2.digest(), batch2.clone()),
        ]);
        let mut fetched_batches = fetcher.fetch(digests).await;
        // Reset metadata from the fetched and expected batches
        for batch in fetched_batches.values_mut() {
            // assert received_at was set to some value before resetting.
            assert!(batch.received_at().is_some());
            batch.set_received_at(0);
        }
        for batch in expected_batches.values_mut() {
            batch.set_received_at(0);
        }
        assert_eq!(fetched_batches, expected_batches);
        assert_eq!(
            batch_store.get::<Batches>(&batch1.digest()).unwrap().unwrap().digest(),
            batch1.digest()
        );
        assert_eq!(
            batch_store.get::<Batches>(&batch2.digest()).unwrap().unwrap().digest(),
            batch2.digest()
        );
    }

    #[tokio::test]
    pub async fn test_fetcher_locally_with_remaining() {
        // Limit is set to two batches in test request_batches(). Request 3 batches
        // and ensure another request is sent to get the remaining batches.
        let mut network = TestRequestBatchesNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let batch_store = open_db(temp_dir.path());
        let batch1 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch2 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch3 = Batch { transactions: vec![transaction()], ..Default::default() };
        let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
        for batch in &[&batch1, &batch2, &batch3] {
            batch_store.insert::<Batches>(&batch.digest(), batch).unwrap();
        }
        network.put(&[1, 2], batch1.clone()).await;
        network.put(&[2, 3], batch2.clone()).await;
        network.put(&[3, 4], batch3.clone()).await;
        let fetcher = BatchFetcher {
            network: Arc::new(network.handle()),
            batch_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let expected_batches = HashMap::from_iter(vec![
            (batch1.digest(), batch1.clone()),
            (batch2.digest(), batch2.clone()),
            (batch3.digest(), batch3.clone()),
        ]);
        let fetched_batches = fetcher.fetch(digests).await;
        assert_eq!(fetched_batches, expected_batches);
    }

    #[tokio::test]
    pub async fn test_fetcher_remote_with_remaining() {
        // Limit is set to two batches in test request_batches(). Request 3 batches
        // and ensure another request is sent to get the remaining batches.
        let mut network = TestRequestBatchesNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let batch_store = open_db(temp_dir.path());
        let batch1 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch2 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch3 = Batch { transactions: vec![transaction()], ..Default::default() };
        let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
        network.put(&[3, 4], batch1.clone()).await;
        network.put(&[2, 3], batch2.clone()).await;
        network.put(&[2, 3, 4], batch3.clone()).await;
        let fetcher = BatchFetcher {
            network: Arc::new(network.handle()),
            batch_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_batches = HashMap::from_iter(vec![
            (batch1.digest(), batch1.clone()),
            (batch2.digest(), batch2.clone()),
            (batch3.digest(), batch3.clone()),
        ]);
        let mut fetched_batches = fetcher.fetch(digests).await;

        // Reset metadata from the fetched and expected batches
        for batch in fetched_batches.values_mut() {
            // assert received_at was set to some value before resetting.
            assert!(batch.received_at().is_some());
            batch.set_received_at(0);
        }
        for batch in expected_batches.values_mut() {
            batch.set_received_at(0);
        }

        assert_eq!(fetched_batches, expected_batches);
    }

    #[tokio::test]
    pub async fn test_fetcher_local_and_remote() {
        let mut network = TestRequestBatchesNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let batch_store = open_db(temp_dir.path());
        let batch1 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch2 = Batch { transactions: vec![transaction()], ..Default::default() };
        let batch3 = Batch { transactions: vec![transaction()], ..Default::default() };
        let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
        batch_store.insert::<Batches>(&batch1.digest(), &batch1).unwrap();
        network.put(&[1, 2, 3], batch1.clone()).await;
        network.put(&[2, 3, 4], batch2.clone()).await;
        network.put(&[1, 4], batch3.clone()).await;
        let fetcher = BatchFetcher {
            network: Arc::new(network.handle()),
            batch_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_batches = HashMap::from_iter(vec![
            (batch1.digest(), batch1.clone()),
            (batch2.digest(), batch2.clone()),
            (batch3.digest(), batch3.clone()),
        ]);
        let mut fetched_batches = fetcher.fetch(digests).await;

        // Reset metadata from the fetched and expected remote batches
        for batch in fetched_batches.values_mut() {
            if batch.digest() != batch1.digest() {
                // assert received_at was set to some value for remote batches before resetting.
                assert!(batch.received_at().is_some());
                batch.set_received_at(0);
            }
        }
        for batch in expected_batches.values_mut() {
            if batch.digest() != batch1.digest() {
                batch.set_received_at(0);
            }
        }

        assert_eq!(fetched_batches, expected_batches);
    }

    #[tokio::test]
    pub async fn test_fetcher_response_size_limit() {
        let mut network = TestRequestBatchesNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let batch_store = open_db(temp_dir.path());
        let num_digests = 12;
        let mut expected_batches = Vec::new();
        let mut local_digests = Vec::new();
        // 6 batches available locally with response size limit of 2
        for _i in 0..num_digests / 2 {
            let batch = Batch { transactions: vec![transaction()], ..Default::default() };
            local_digests.push(batch.digest());
            batch_store.insert::<Batches>(&batch.digest(), &batch).unwrap();
            network.put(&[1, 2, 3], batch.clone()).await;
            expected_batches.push(batch);
        }
        // 6 batches available remotely with response size limit of 2
        for _i in (num_digests / 2)..num_digests {
            let batch = Batch { transactions: vec![transaction()], ..Default::default() };
            network.put(&[1, 2, 3], batch.clone()).await;
            expected_batches.push(batch);
        }

        let mut expected_batches = HashMap::from_iter(
            expected_batches.iter().map(|batch| (batch.digest(), batch.clone())),
        );
        let digests = HashSet::from_iter(expected_batches.clone().into_keys());
        let fetcher = BatchFetcher {
            network: Arc::new(network.handle()),
            batch_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut fetched_batches = fetcher.fetch(digests).await;

        // Reset metadata from the fetched and expected remote batches
        for batch in fetched_batches.values_mut() {
            if !local_digests.contains(&batch.digest()) {
                // assert received_at was set to some value for remote batches before resetting.
                assert!(batch.received_at().is_some());
                batch.set_received_at(0);
            }
        }
        for batch in expected_batches.values_mut() {
            if !local_digests.contains(&batch.digest()) {
                batch.set_received_at(0);
            }
        }

        assert_eq!(fetched_batches, expected_batches);
    }

    // TODO: add test for timeouts, failures and retries.

    #[derive(Clone)]
    struct TestRequestBatchesNetwork {
        // Worker name -> batch digests it has -> batches.
        data: Arc<Mutex<HashMap<PeerId, HashMap<BlockHash, Batch>>>>,
        handle: WorkerNetworkHandle,
    }

    impl TestRequestBatchesNetwork {
        pub fn new() -> Self {
            let data: Arc<Mutex<HashMap<PeerId, HashMap<BlockHash, Batch>>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let data_clone = data.clone();
            let (tx, mut rx) = mpsc::channel(100);
            let task_manager = TaskManager::default();
            let handle =
                WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner());
            tokio::spawn(async move {
                let _owned = task_manager;
                while let Some(r) = rx.recv().await {
                    match r {
                        NetworkCommand::ConnectedPeers { reply } => {
                            reply.send(data_clone.lock().await.keys().copied().collect()).unwrap();
                        }
                        NetworkCommand::SendRequest {
                            peer,
                            request: WorkerRequest::RequestBatches { batch_digests: digests },
                            reply,
                        } => {
                            // Use this to simulate server side response size limit in
                            // RequestBlocks
                            const MAX_REQUEST_BATCHES_RESPONSE_SIZE: usize = 2;
                            const MAX_READ_BLOCK_DIGESTS: usize = 5;

                            let mut batches = Vec::new();
                            let mut total_size = 0;

                            let digests_chunks = digests
                                .chunks(MAX_READ_BLOCK_DIGESTS)
                                .map(|chunk| chunk.to_vec())
                                .collect::<Vec<_>>();
                            for digests_chunk in digests_chunks {
                                for digest in digests_chunk {
                                    if let Some(batch) =
                                        data_clone.lock().await.get(&peer).unwrap().get(&digest)
                                    {
                                        if total_size < MAX_REQUEST_BATCHES_RESPONSE_SIZE {
                                            batches.push(batch.clone());
                                            total_size += batch.size();
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }

                            reply.send(Ok(WorkerResponse::RequestBatches(batches))).unwrap();
                        }
                        _ => {}
                    }
                }
            });
            Self { data, handle }
        }

        pub async fn put(&mut self, keys: &[u8], batch: Batch) {
            for key in keys {
                let key = test_pk(*key);
                let mut guard = self.data.lock().await;
                let entry = guard.entry(key).or_default();
                entry.insert(batch.digest(), batch.clone());
            }
        }

        pub fn handle(&self) -> WorkerNetworkHandle {
            self.handle.clone()
        }
    }

    fn test_pk(i: u8) -> PeerId {
        use rand::SeedableRng;
        let mut rng = StdRng::from_seed([i; 32]);
        let mut bytes = [0_u8; 32];
        rng.fill_bytes(&mut bytes);
        NetworkKeypair::ed25519_from_bytes(bytes)
            .expect("invalid network key bytes")
            .public()
            .to_peer_id()
    }
}
