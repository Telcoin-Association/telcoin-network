//! Unit tests for the worker's batch provider.
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_network_types::{local::LocalNetwork, MockWorkerToPrimary};
use tn_reth::test_utils::transaction;
use tn_storage::{open_db, tables::NodeBatchesCache};
use tn_types::{
    error::BlockSealError, test_chain_spec_arc, Batch, Database, NoopTxnForwarder, TaskManager,
};
use tn_worker::{test_utils::TestMakeBlockQuorumWaiter, Worker, WorkerNetworkHandle};

#[tokio::test]
async fn make_batch() {
    let client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // Spawn a `BatchProvider` instance.
    let id = 0;
    let qw = TestMakeBlockQuorumWaiter::new_test();
    let timeout = Duration::from_secs(5);
    let task_manager = TaskManager::default();
    let batch_provider = Worker::new(
        id,
        Some(qw.clone()),
        client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    );

    // Send enough transactions to seal a batch.
    let chain = test_chain_spec_arc();
    let tx = transaction(chain);
    let new_batch = Batch { transactions: vec![tx.clone(), tx.clone()], ..Default::default() };

    batch_provider.seal(new_batch.clone().seal_slow()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch { transactions: vec![tx.clone(), tx.clone()], ..Default::default() };

    assert_eq!(
        new_batch.transactions(),
        qw.0.lock()
            .unwrap()
            .as_ref()
            .expect("batch not sent to Quorum Waiter!")
            .batch()
            .transactions()
    );

    // Ensure the batch is stored
    assert!(store.get::<NodeBatchesCache>(&expected_batch.digest()).unwrap().is_some());
}

/// An observer worker (no quorum waiter) must refuse a non-empty seal when the batch is not
/// admitted to a forward task. The test network handle discovers no validator RPC endpoints
/// and `NoopTxnForwarder` admits nothing, so `seal` returns `NotValidator` and never writes
/// the batch cache. An empty batch stays a success because there is nothing to forward.
#[tokio::test]
async fn observer_seal_without_admission_returns_not_validator() {
    let client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // A `BatchProvider` instance without a quorum waiter: the observer path.
    let id = 0;
    let timeout = Duration::from_secs(5);
    let task_manager = TaskManager::default();
    let batch_provider = Worker::new(
        id,
        None::<TestMakeBlockQuorumWaiter>,
        client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    );

    // Seal a batch with transactions.
    let chain = test_chain_spec_arc();
    let tx = transaction(chain);
    let new_batch = Batch { transactions: vec![tx.clone(), tx], ..Default::default() };
    let digest = new_batch.digest();

    let res = batch_provider.seal(new_batch.seal_slow()).await;
    assert!(matches!(res, Err(BlockSealError::NotValidator)));

    // The observer path refuses before the batch cache write.
    assert!(store.get::<NodeBatchesCache>(&digest).unwrap().is_none());

    // An empty batch is still a success.
    let empty_batch = Batch { transactions: vec![], ..Default::default() };
    assert!(batch_provider.seal(empty_batch.seal_slow()).await.is_ok());
}
