// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anemo::Network;
use async_trait::async_trait;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use itertools::Itertools;
use narwhal_network::WorkerRpc;
use narwhal_typed_store::{
    tables::WorkerBlocks,
    traits::{Database, DbTxMut},
};
use prometheus::IntGauge;
use rand::{rngs::ThreadRng, seq::SliceRandom};
use tn_types::NetworkPublicKey;

use narwhal_network_types::{RequestBlocksRequest, RequestBlocksResponse};
use tn_types::{now, BlockHash, WorkerBlock};
use tokio::{
    select,
    time::{sleep, sleep_until, Instant},
};
use tracing::debug;

use crate::metrics::WorkerMetrics;

const REMOTE_PARALLEL_FETCH_INTERVAL: Duration = Duration::from_secs(2);
const WORKER_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct WorkerBlockFetcher<DB> {
    name: NetworkPublicKey,
    network: Arc<dyn RequestBlocksNetwork>,
    block_store: DB,
    metrics: Arc<WorkerMetrics>,
}

impl<DB: Database> WorkerBlockFetcher<DB> {
    pub fn new(
        name: NetworkPublicKey,
        network: Network,
        block_store: DB,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        Self { name, network: Arc::new(RequestBlocksNetworkImpl { network }), block_store, metrics }
    }

    /// Bulk fetches payload from local storage and remote workers.
    /// This function performs infinite retries and blocks until all blocks are available.
    pub async fn fetch(
        &self,
        digests: HashSet<BlockHash>,
        known_workers: HashSet<NetworkPublicKey>,
    ) -> HashMap<BlockHash, WorkerBlock> {
        debug!(
            "Attempting to fetch {} digests from {} workers",
            digests.len(),
            known_workers.len()
        );

        let mut remaining_digests = digests;
        let mut fetched_blocks = HashMap::new();
        // TODO: verify known_workers meets quorum threshold, or just use all other workers.
        let known_workers =
            known_workers.into_iter().filter(|worker| worker != &self.name).collect_vec();

        loop {
            if remaining_digests.is_empty() {
                return fetched_blocks;
            }

            // Fetch from local storage.
            let _timer = self.metrics.worker_local_fetch_latency.start_timer();
            fetched_blocks.extend(self.fetch_local(remaining_digests.clone()).await);
            remaining_digests.retain(|d| !fetched_blocks.contains_key(d));
            if remaining_digests.is_empty() {
                return fetched_blocks;
            }
            drop(_timer);

            // Fetch from remote workers.
            // TODO: Can further parallelize this by target worker_id if necessary.
            let _timer = self.metrics.worker_remote_fetch_latency.start_timer();
            let mut known_workers: Vec<_> = known_workers.iter().collect();
            known_workers.shuffle(&mut ThreadRng::default());
            let mut known_workers = VecDeque::from(known_workers);
            let mut stagger = Duration::from_secs(0);
            let mut futures = FuturesUnordered::new();

            loop {
                assert!(!remaining_digests.is_empty());
                if let Some(worker) = known_workers.pop_front() {
                    let future = self.fetch_remote(worker.clone(), remaining_digests.clone());
                    futures.push(future.boxed());
                } else {
                    // No more worker to fetch from. This happens after sending requests to all
                    // workers and then another staggered interval has passed.
                    break;
                }
                stagger += REMOTE_PARALLEL_FETCH_INTERVAL;
                let mut interval = Box::pin(sleep(stagger));
                select! {
                    result = futures.next() => {
                        if let Some(remote_blocks) = result {
                            let new_blocks: HashMap<_, _> = remote_blocks.iter().filter(|(d, _)| remaining_digests.remove(*d)).collect();

                            // Set received_at timestamp for remote blocks.
                            let mut updated_new_blocks = HashMap::new();
                            let mut txn = self.block_store.write_txn().expect("unable to create DB transaction!");
                            for (digest, block) in new_blocks {
                                let mut block = (*block).clone();
                                block.set_received_at(now());
                                updated_new_blocks.insert(*digest, block.clone());
                                // Also persist the blocks, so they are available after restarts.
                                if let Err(e) = txn.insert::<WorkerBlocks>(digest, &block) {
                                    tracing::error!("failed to insert block! We can not continue.. {e}");
                                    panic!("failed to insert block! We can not continue.. {e}");
                                }
                            }
                            if let Err(e) = txn.commit() {
                                tracing::error!("failed to commit block! We can not continue.. {e}");
                                panic!("failed to commit block! We can not continue.. {e}");
                            }
                            fetched_blocks.extend(updated_new_blocks.iter().map(|(d, b)| (*d, (*b).clone())));

                            if remaining_digests.is_empty() {
                                return fetched_blocks;
                            }
                        }
                    }
                    _ = interval.as_mut() => {
                    }
                }
            }

            // After all known remote workers have been tried, restart the outer loop to fetch
            // from local storage then remote workers again.
            sleep(WORKER_RETRY_INTERVAL).await;
        }
    }

    async fn fetch_local(&self, digests: HashSet<BlockHash>) -> HashMap<BlockHash, WorkerBlock> {
        let mut fetched_blocks = HashMap::new();
        if digests.is_empty() {
            return fetched_blocks;
        }

        // Continue to bulk request from local worker until no remaining digests
        // are available.
        debug!("Local attempt to fetch {} digests", digests.len());
        if let Ok(local_blocks) = self.block_store.multi_get::<WorkerBlocks>(digests.iter()) {
            for (digest, block) in digests.into_iter().zip(local_blocks.into_iter()) {
                if let Some(block) = block {
                    self.metrics.worker_block_fetch.with_label_values(&["local", "success"]).inc();
                    fetched_blocks.insert(digest, block);
                } else {
                    self.metrics.worker_block_fetch.with_label_values(&["local", "missing"]).inc();
                }
            }
        }

        fetched_blocks
    }

    /// This future performs a fetch from a given remote worker
    /// This future performs infinite retries with exponential backoff
    /// You can specify stagger_delay before request is issued
    async fn fetch_remote(
        &self,
        worker: NetworkPublicKey,
        digests: HashSet<BlockHash>,
    ) -> HashMap<BlockHash, WorkerBlock> {
        // TODO: Make these config parameters
        let max_timeout = Duration::from_secs(60);
        let mut timeout = Duration::from_secs(10);
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            debug!("Remote attempt #{attempt} to fetch {} digests from {worker}", digests.len(),);
            let deadline = Instant::now() + timeout;
            let request_guard = PendingGuard::make_inc(&self.metrics.pending_remote_request_blocks);
            let response = self.safe_request_blocks(digests.clone(), worker.clone(), timeout).await;
            drop(request_guard);
            match response {
                Ok(remote_blocks) => {
                    self.metrics.worker_block_fetch.with_label_values(&["remote", "success"]).inc();
                    debug!("Found {} blocks remotely", remote_blocks.len());
                    return remote_blocks;
                }
                Err(err) => {
                    if err.to_string().contains("Timeout") {
                        self.metrics
                            .worker_block_fetch
                            .with_label_values(&["remote", "timeout"])
                            .inc();
                        debug!("Timed out retrieving payloads {digests:?} from {worker} attempt {attempt}: {err}");
                    } else if err.to_string().contains("[Protocol violation]") {
                        self.metrics
                            .worker_block_fetch
                            .with_label_values(&["remote", "fail"])
                            .inc();
                        debug!("Failed retrieving payloads {digests:?} from possibly byzantine {worker} attempt {attempt}: {err}");
                        // Do not bother retrying if the remote worker is byzantine.
                        return HashMap::new();
                    } else {
                        self.metrics
                            .worker_block_fetch
                            .with_label_values(&["remote", "fail"])
                            .inc();
                        debug!("Error retrieving payloads {digests:?} from {worker} attempt {attempt}: {err}");
                    }
                }
            }
            timeout += timeout / 2;
            timeout = std::cmp::min(max_timeout, timeout);
            // Since the call might have returned before timeout, we wait until originally planned
            // deadline
            sleep_until(deadline).await;
        }
    }

    /// Issue request_blocks RPC and verifies response integrity
    async fn safe_request_blocks(
        &self,
        digests_to_fetch: HashSet<BlockHash>,
        worker: NetworkPublicKey,
        timeout: Duration,
    ) -> eyre::Result<HashMap<BlockHash, WorkerBlock>> {
        let mut fetched_blocks = HashMap::new();
        if digests_to_fetch.is_empty() {
            return Ok(fetched_blocks);
        }

        let RequestBlocksResponse { blocks, is_size_limit_reached: _ } = self
            .network
            .request_blocks(digests_to_fetch.clone().into_iter().collect(), worker.clone(), timeout)
            .await?;
        for block in blocks {
            let block_digest = block.digest();
            if !digests_to_fetch.contains(&block_digest) {
                eyre::bail!(
                    "[Protocol violation] Worker {worker} returned block with digest \
                    {block_digest} which is not part of the requested digests: {digests_to_fetch:?}"
                );
            }
            // This block is part of a certificate, so no need to validate it.
            fetched_blocks.insert(block_digest, block);
        }

        Ok(fetched_blocks)
    }
}

// todo - make it generic so that other can reuse
struct PendingGuard<'a> {
    metric: &'a IntGauge,
}

impl<'a> PendingGuard<'a> {
    pub fn make_inc(metric: &'a IntGauge) -> Self {
        metric.inc();
        Self { metric }
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.metric.dec()
    }
}

// Trait for unit tests
// TODO: migrate this WorkerRpc.
#[async_trait]
pub trait RequestBlocksNetwork: Send + Sync {
    async fn request_blocks(
        &self,
        block_digests: Vec<BlockHash>,
        worker: NetworkPublicKey,
        timeout: Duration,
    ) -> eyre::Result<RequestBlocksResponse>;
}

struct RequestBlocksNetworkImpl {
    network: anemo::Network,
}

#[async_trait]
impl RequestBlocksNetwork for RequestBlocksNetworkImpl {
    async fn request_blocks(
        &self,
        block_digests: Vec<BlockHash>,
        worker: NetworkPublicKey,
        timeout: Duration,
    ) -> eyre::Result<RequestBlocksResponse> {
        let request =
            anemo::Request::new(RequestBlocksRequest { block_digests }).with_timeout(timeout);
        self.network.request_blocks(&worker, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastcrypto::traits::KeyPair;
    use narwhal_test_utils::transaction;
    use narwhal_typed_store::open_db;
    use rand::rngs::StdRng;
    use reth_primitives::{Header, SealedHeader};
    use tempfile::TempDir;
    use tn_types::NetworkKeypair;

    #[tokio::test]
    pub async fn test_fetcher() {
        let mut network = TestRequestBlocksNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let block_store = open_db(temp_dir.path());
        let header = Header { nonce: 1, ..Default::default() };
        let block1 = WorkerBlock::new(vec![transaction()], header.seal_slow());
        let header = Header { nonce: 2, ..Default::default() };
        let block2 = WorkerBlock::new(vec![transaction()], header.seal_slow());
        let (digests, known_workers) = (
            HashSet::from_iter(vec![block1.digest(), block2.digest()]),
            HashSet::from_iter(test_pks(&[1, 2])),
        );
        network.put(&[1, 2], block1.clone());
        network.put(&[2, 3], block2.clone());
        let fetcher = WorkerBlockFetcher {
            name: test_pk(0),
            network: Arc::new(network.clone()),
            block_store: block_store.clone(),
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_blocks = HashMap::from_iter(vec![
            (block1.digest(), block1.clone()),
            (block2.digest(), block2.clone()),
        ]);
        let mut fetched_blocks = fetcher.fetch(digests, known_workers).await;
        // Reset metadata from the fetched and expected blocks
        for block in fetched_blocks.values_mut() {
            // assert received_at was set to some value before resetting.
            assert!(block.received_at().is_some());
            block.set_received_at(0);
        }
        for block in expected_blocks.values_mut() {
            block.set_received_at(0);
        }
        assert_eq!(fetched_blocks, expected_blocks);
        assert_eq!(
            block_store.get::<WorkerBlocks>(&block1.digest()).unwrap().unwrap().digest(),
            block1.digest()
        );
        assert_eq!(
            block_store.get::<WorkerBlocks>(&block2.digest()).unwrap().unwrap().digest(),
            block2.digest()
        );
    }

    #[tokio::test]
    pub async fn test_fetcher_locally_with_remaining() {
        // Limit is set to two blocks in test request_blocks(). Request 3 blocks
        // and ensure another request is sent to get the remaining blocks.
        let mut network = TestRequestBlocksNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let block_store = open_db(temp_dir.path());
        let block1 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let block2 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let block3 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let (digests, known_workers) = (
            HashSet::from_iter(vec![block1.digest(), block2.digest(), block3.digest()]),
            HashSet::from_iter(test_pks(&[1, 2, 3])),
        );
        for block in &[&block1, &block2, &block3] {
            block_store.insert::<WorkerBlocks>(&block.digest(), block).unwrap();
        }
        network.put(&[1, 2], block1.clone());
        network.put(&[2, 3], block2.clone());
        network.put(&[3, 4], block3.clone());
        let fetcher = WorkerBlockFetcher {
            name: test_pk(0),
            network: Arc::new(network.clone()),
            block_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let expected_blocks = HashMap::from_iter(vec![
            (block1.digest(), block1.clone()),
            (block2.digest(), block2.clone()),
            (block3.digest(), block3.clone()),
        ]);
        let fetched_blocks = fetcher.fetch(digests, known_workers).await;
        assert_eq!(fetched_blocks, expected_blocks);
    }

    #[tokio::test]
    pub async fn test_fetcher_remote_with_remaining() {
        // Limit is set to two blocks in test request_blocks(). Request 3 blocks
        // and ensure another request is sent to get the remaining blocks.
        let mut network = TestRequestBlocksNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let block_store = open_db(temp_dir.path());
        let block1 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let block2 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let block3 = WorkerBlock::new(vec![transaction()], SealedHeader::default());
        let (digests, known_workers) = (
            HashSet::from_iter(vec![block1.digest(), block2.digest(), block3.digest()]),
            HashSet::from_iter(test_pks(&[2, 3, 4])),
        );
        network.put(&[3, 4], block1.clone());
        network.put(&[2, 3], block2.clone());
        network.put(&[2, 3, 4], block3.clone());
        let fetcher = WorkerBlockFetcher {
            name: test_pk(0),
            network: Arc::new(network.clone()),
            block_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_blocks = HashMap::from_iter(vec![
            (block1.digest(), block1.clone()),
            (block2.digest(), block2.clone()),
            (block3.digest(), block3.clone()),
        ]);
        let mut fetched_blocks = fetcher.fetch(digests, known_workers).await;

        // Reset metadata from the fetched and expected blocks
        for block in fetched_blocks.values_mut() {
            // assert received_at was set to some value before resetting.
            assert!(block.received_at().is_some());
            block.set_received_at(0);
        }
        for block in expected_blocks.values_mut() {
            block.set_received_at(0);
        }

        assert_eq!(fetched_blocks, expected_blocks);
    }

    #[tokio::test]
    pub async fn test_fetcher_local_and_remote() {
        let mut network = TestRequestBlocksNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let block_store = open_db(temp_dir.path());
        let header = Header { nonce: 1, ..Default::default() };
        let block1 = WorkerBlock::new(vec![transaction()], header.seal_slow());
        let header = Header { nonce: 2, ..Default::default() };
        let block2 = WorkerBlock::new(vec![transaction()], header.seal_slow());
        let header = Header { nonce: 3, ..Default::default() };
        let block3 = WorkerBlock::new(vec![transaction()], header.seal_slow());
        let (digests, known_workers) = (
            HashSet::from_iter(vec![block1.digest(), block2.digest(), block3.digest()]),
            HashSet::from_iter(test_pks(&[1, 2, 3, 4])),
        );
        block_store.insert::<WorkerBlocks>(&block1.digest(), &block1).unwrap();
        network.put(&[1, 2, 3], block1.clone());
        network.put(&[2, 3, 4], block2.clone());
        network.put(&[1, 4], block3.clone());
        let fetcher = WorkerBlockFetcher {
            name: test_pk(0),
            network: Arc::new(network.clone()),
            block_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut expected_blocks = HashMap::from_iter(vec![
            (block1.digest(), block1.clone()),
            (block2.digest(), block2.clone()),
            (block3.digest(), block3.clone()),
        ]);
        let mut fetched_blocks = fetcher.fetch(digests, known_workers).await;

        // Reset metadata from the fetched and expected remote blocks
        for block in fetched_blocks.values_mut() {
            if block.digest() != block1.digest() {
                // assert received_at was set to some value for remote blocks before resetting.
                assert!(block.received_at().is_some());
                block.set_received_at(0);
            }
        }
        for block in expected_blocks.values_mut() {
            if block.digest() != block1.digest() {
                block.set_received_at(0);
            }
        }

        assert_eq!(fetched_blocks, expected_blocks);
    }

    #[tokio::test]
    pub async fn test_fetcher_response_size_limit() {
        let mut network = TestRequestBlocksNetwork::new();
        let temp_dir = TempDir::new().unwrap();
        let block_store = open_db(temp_dir.path());
        let num_digests = 12;
        let mut expected_blocks = Vec::new();
        let mut local_digests = Vec::new();
        // 6 blocks available locally with response size limit of 2
        let mut nonce = 0;
        for _i in 0..num_digests / 2 {
            let header = Header { nonce, ..Default::default() };
            nonce += 1;
            let block = WorkerBlock::new(vec![transaction()], header.seal_slow());
            local_digests.push(block.digest());
            block_store.insert::<WorkerBlocks>(&block.digest(), &block).unwrap();
            network.put(&[1, 2, 3], block.clone());
            expected_blocks.push(block);
        }
        // 6 blocks available remotely with response size limit of 2
        for _i in (num_digests / 2)..num_digests {
            let header = Header { nonce, ..Default::default() };
            nonce += 1;
            let block = WorkerBlock::new(vec![transaction()], header.seal_slow());
            network.put(&[1, 2, 3], block.clone());
            expected_blocks.push(block);
        }

        let mut expected_blocks =
            HashMap::from_iter(expected_blocks.iter().map(|block| (block.digest(), block.clone())));
        let (digests, known_workers) = (
            HashSet::from_iter(expected_blocks.clone().into_keys()),
            HashSet::from_iter(test_pks(&[1, 2, 3])),
        );
        let fetcher = WorkerBlockFetcher {
            name: test_pk(0),
            network: Arc::new(network.clone()),
            block_store,
            metrics: Arc::new(WorkerMetrics::default()),
        };
        let mut fetched_blocks = fetcher.fetch(digests, known_workers).await;

        // Reset metadata from the fetched and expected remote blocks
        for block in fetched_blocks.values_mut() {
            if !local_digests.contains(&block.digest()) {
                // assert received_at was set to some value for remote blocks before resetting.
                assert!(block.received_at().is_some());
                block.set_received_at(0);
            }
        }
        for block in expected_blocks.values_mut() {
            if !local_digests.contains(&block.digest()) {
                block.set_received_at(0);
            }
        }

        assert_eq!(fetched_blocks, expected_blocks);
    }

    // TODO: add test for timeouts, failures and retries.

    #[derive(Clone)]
    struct TestRequestBlocksNetwork {
        // Worker name -> block digests it has -> blocks.
        data: HashMap<NetworkPublicKey, HashMap<BlockHash, WorkerBlock>>,
    }

    impl TestRequestBlocksNetwork {
        pub fn new() -> Self {
            Self { data: HashMap::new() }
        }

        pub fn put(&mut self, keys: &[u8], block: WorkerBlock) {
            for key in keys {
                let key = test_pk(*key);
                let entry = self.data.entry(key).or_default();
                entry.insert(block.digest(), block.clone());
            }
        }
    }

    #[async_trait]
    impl RequestBlocksNetwork for TestRequestBlocksNetwork {
        async fn request_blocks(
            &self,
            digests: Vec<BlockHash>,
            worker: NetworkPublicKey,
            _timeout: Duration,
        ) -> eyre::Result<RequestBlocksResponse> {
            // Use this to simulate server side response size limit in RequestBlocks
            const MAX_REQUEST_BLOCKS_RESPONSE_SIZE: usize = 2;
            const MAX_READ_BLOCK_DIGESTS: usize = 5;

            let mut is_size_limit_reached = false;
            let mut blocks = Vec::new();
            let mut total_size = 0;

            let digests_chunks =
                digests.chunks(MAX_READ_BLOCK_DIGESTS).map(|chunk| chunk.to_vec()).collect_vec();
            for digests_chunk in digests_chunks {
                for digest in digests_chunk {
                    if let Some(block) = self.data.get(&worker).unwrap().get(&digest) {
                        if total_size < MAX_REQUEST_BLOCKS_RESPONSE_SIZE {
                            blocks.push(block.clone());
                            total_size += block.size();
                        } else {
                            is_size_limit_reached = true;
                            break;
                        }
                    }
                }
            }

            Ok(RequestBlocksResponse { blocks, is_size_limit_reached })
        }
    }

    fn test_pk(i: u8) -> NetworkPublicKey {
        use rand::SeedableRng;
        let mut rng = StdRng::from_seed([i; 32]);
        NetworkKeypair::generate(&mut rng).public().clone()
    }

    fn test_pks(i: &[u8]) -> Vec<NetworkPublicKey> {
        i.iter().map(|i| test_pk(*i)).collect()
    }
}
