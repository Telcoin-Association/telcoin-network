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
    let mut mock_server = MockWorkerToPrimary::new();
    mock_server.expect_report_own_batch().returning(|_| Ok(anemo::Response::new(())));
    client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // Spawn a `BatchProvider` instance.
    let id = 0;
    let qw = TestMakeBlockQuorumWaiter::new_test();
    let timeout = Duration::from_secs(5);
    let batch_provider =
        BatchProvider::new(id, qw.clone(), Arc::new(node_metrics), client, store.clone(), timeout);

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

// #[tokio::test]
// async fn batch_timeout() {
//     let client = create_network_client();
//     let store: Arc<dyn DBMap<BatchDigest, Batch>> = Arc::new(MemDB::open());
//     let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);
//     let (tx_batch_maker, rx_batch_maker) = tn_types::test_channel!(1);
//     let (tx_quorum_waiter, mut rx_quorum_waiter) = tn_types::test_channel!(1);
//     let node_metrics = WorkerMetrics::new(&Registry::new());

//     // Mock the primary client to always succeed.
//     let mut mock_server = MockWorkerToPrimary::new();
//     mock_server.expect_report_own_batch().returning(|_| Ok(anemo::Response::new(())));
//     client.set_worker_to_primary_local_handler(Arc::new(mock_server));

//     // Spawn a `BatchMaker` instance.
//     let id = 0;
//     let _batch_maker_handle = BatchMaker::spawn(
//         id,
//         /* max_batch_size */ 200,
//         /* max_batch_delay */
//         Duration::from_millis(50), // Ensure the timer is triggered.
//         tx_shutdown.subscribe(),
//         rx_batch_maker,
//         tx_quorum_waiter,
//         Arc::new(node_metrics),
//         client,
//         store.clone(),
//     );

//     // Do not send enough transactions to seal a batch.
//     let tx = transaction();
//     let (s0, r0) = tokio::sync::oneshot::channel();
//     tx_batch_maker.send((tx.clone(), s0)).await.unwrap();

//     // Ensure the batch is as expected.
//     let (batch, resp) = rx_quorum_waiter.recv().await.unwrap();
//     let expected_batch = Batch::new(vec![tx.clone()]);
//     assert_eq!(batch.transactions(), expected_batch.transactions());

//     // Eventually deliver message
//     assert!(resp.send(()).is_ok());

//     // Batch maker should finish creating the batch.
//     assert!(r0.await.is_ok());

//     // Ensure the batch is stored
//     assert!(store.get(&batch.digest()).unwrap().is_some());
// }
