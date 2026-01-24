//! Batch fetcher tests

use super::*;
use crate::test_utils::TestRequestBatchesNetwork;
use tempfile::TempDir;
use tn_reth::test_utils::transaction;
use tn_storage::open_db;
use tn_types::test_chain_spec_arc;

#[tokio::test]
async fn test_fetchertt() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest()]);
    network.put(&[1, 2], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    let fetcher =
        BatchFetcher { network: Arc::new(network.handle()), batch_store: batch_store.clone() };
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
async fn test_fetcher_locally_with_remaining() {
    // Limit is set to two batches in test request_batches(). Request 3 batches
    // and ensure another request is sent to get the remaining batches.
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    for batch in &[&batch1, &batch2, &batch3] {
        batch_store.insert::<Batches>(&batch.digest(), batch).unwrap();
    }
    network.put(&[1, 2], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    network.put(&[3, 4], batch3.clone()).await;
    let fetcher = BatchFetcher { network: Arc::new(network.handle()), batch_store };
    let expected_batches = HashMap::from_iter(vec![
        (batch1.digest(), batch1.clone()),
        (batch2.digest(), batch2.clone()),
        (batch3.digest(), batch3.clone()),
    ]);
    let fetched_batches = fetcher.fetch(digests).await;
    assert_eq!(fetched_batches, expected_batches);
}

#[tokio::test]
async fn test_fetcher_remote_with_remaining() {
    // Limit is set to two batches in test request_batches(). Request 3 batches
    // and ensure another request is sent to get the remaining batches.
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    network.put(&[3, 4], batch1.clone()).await;
    network.put(&[2, 3], batch2.clone()).await;
    network.put(&[2, 3, 4], batch3.clone()).await;
    let fetcher = BatchFetcher { network: Arc::new(network.handle()), batch_store };
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
async fn test_fetcher_local_and_remote() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let chain = test_chain_spec_arc();
    let batch1 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch2 = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
    let batch3 = Batch { transactions: vec![transaction(chain)], ..Default::default() };
    let digests = HashSet::from_iter(vec![batch1.digest(), batch2.digest(), batch3.digest()]);
    batch_store.insert::<Batches>(&batch1.digest(), &batch1).unwrap();
    network.put(&[1, 2, 3], batch1.clone()).await;
    network.put(&[2, 3, 4], batch2.clone()).await;
    network.put(&[1, 4], batch3.clone()).await;
    let fetcher = BatchFetcher { network: Arc::new(network.handle()), batch_store };
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
async fn test_fetcher_response_size_limit() {
    let mut network = TestRequestBatchesNetwork::new();
    let temp_dir = TempDir::new().unwrap();
    let batch_store = open_db(temp_dir.path());
    let num_digests = 12;
    let mut expected_batches = Vec::new();
    let mut local_digests = Vec::new();
    // 6 batches available locally with response size limit of 2
    let chain = test_chain_spec_arc();
    for _i in 0..num_digests / 2 {
        let batch = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
        local_digests.push(batch.digest());
        batch_store.insert::<Batches>(&batch.digest(), &batch).unwrap();
        network.put(&[1, 2, 3], batch.clone()).await;
        expected_batches.push(batch);
    }
    // 6 batches available remotely with response size limit of 2
    for _i in (num_digests / 2)..num_digests {
        let batch = Batch { transactions: vec![transaction(chain.clone())], ..Default::default() };
        network.put(&[1, 2, 3], batch.clone()).await;
        expected_batches.push(batch);
    }

    let mut expected_batches =
        HashMap::from_iter(expected_batches.iter().map(|batch| (batch.digest(), batch.clone())));
    let digests = HashSet::from_iter(expected_batches.clone().into_keys());
    let fetcher = BatchFetcher { network: Arc::new(network.handle()), batch_store };
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
