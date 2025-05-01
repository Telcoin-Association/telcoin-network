//! Unit tests for the worker's batch provider.
use super::*;
use crate::quorum_waiter::QuorumWaiterError;
use std::sync::Mutex;
use tempfile::TempDir;
use tn_network_types::MockWorkerToPrimary;
use tn_storage::open_db;
use tn_test_utils::transaction;
use tn_types::Batch;

#[derive(Clone, Debug)]
struct TestMakeBlockQuorumWaiter(Arc<Mutex<Option<SealedBatch>>>);
impl TestMakeBlockQuorumWaiter {
    fn new_test() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}
impl QuorumWaiterTrait for TestMakeBlockQuorumWaiter {
    fn verify_batch(
        &self,
        batch: SealedBatch,
        _timeout: Duration,
    ) -> tokio::task::JoinHandle<Result<(), QuorumWaiterError>> {
        let data = self.0.clone();
        tokio::spawn(async move {
            *data.lock().unwrap() = Some(batch);
            Ok(())
        })
    }
}

#[tokio::test]
async fn make_batch() {
    let client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let node_metrics = WorkerMetrics::default();

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // Spawn a `BatchProvider` instance.
    let id = 0;
    let qw = TestMakeBlockQuorumWaiter::new_test();
    let timeout = Duration::from_secs(5);
    let mut task_manager = TaskManager::default();
    let batch_provider = Worker::new(
        id,
        Some(qw.clone()),
        Arc::new(node_metrics),
        client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(),
        &mut task_manager,
    );

    // Send enough transactions to seal a batch.
    let tx = transaction();
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
    assert!(store.get::<Batches>(&expected_batch.digest()).unwrap().is_some());
}
