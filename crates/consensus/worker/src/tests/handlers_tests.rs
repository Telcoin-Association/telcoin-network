//! Network synchronize tests
use super::*;
use std::vec;
use tempfile::TempDir;
use tn_batch_validator::NoopBatchValidator;
use tn_network_types::{MockWorkerToWorker, WorkerToWorkerServer};
use tn_storage::{mem_db::MemDatabase, open_db};

#[tokio::test]
async fn synchronize() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let id = 0;

    // Create a new test store.
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Create network with mock behavior to respond to RequestBatches request.
    let target_primary = fixture.authorities().nth(1).unwrap();
    let batch = batch();
    let digest = batch.digest();
    let message = WorkerSynchronizeMessage {
        digests: vec![digest],
        target: target_primary.id(),
        is_certified: false,
    };

    let mut mock_server = MockWorkerToWorker::new();
    let mock_batch_response = batch.clone();
    mock_server
        .expect_request_batches()
        .withf(move |request| request.body().batch_digests == vec![digest])
        .return_once(move |_| {
            Ok(anemo::Response::new(RequestBatchesResponse {
                batches: vec![mock_batch_response],
                is_size_limit_reached: false,
            }))
        });
    let routes = anemo::Router::new().add_rpc_service(WorkerToWorkerServer::new(mock_server));
    let target_worker = target_primary.worker();
    let _recv_network = target_worker.new_network(routes);
    let send_network = random_network();
    send_network
        .connect_with_peer_id(
            target_worker.info().worker_address.to_anemo_address().unwrap(),
            anemo::PeerId(target_worker.info().name.0.to_bytes()),
        )
        .await
        .unwrap();

    let handler = PrimaryReceiverHandler {
        id,
        committee,
        worker_cache,
        store: store.clone(),
        request_batches_timeout: Duration::from_secs(999),
        network: Some(send_network),
        batch_fetcher: None,
        validator: Arc::new(NoopBatchValidator),
    };

    // Verify the batch is not in store
    assert!(store.get::<Batches>(&digest).unwrap().is_none());

    // Send a sync request.
    let request = anemo::Request::new(message);
    handler.synchronize(request).await.unwrap();

    // Verify it is now stored
    assert!(store.get::<Batches>(&digest).unwrap().is_some());
}

#[tokio::test]
async fn synchronize_when_batch_exists() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let id = 0;

    // Create a new test store.
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Create network without mock behavior since it will not be needed.
    let send_network = random_network();

    let handler = PrimaryReceiverHandler {
        id,
        committee: committee.clone(),
        worker_cache,
        store: store.clone(),
        request_batches_timeout: Duration::from_secs(999),
        network: Some(send_network),
        batch_fetcher: None,
        validator: Arc::new(NoopBatchValidator),
    };

    // Store the batch.
    let batch = batch();
    let batch_id = batch.digest();
    let missing = vec![batch_id];
    store.insert::<Batches>(&batch_id, &batch).unwrap();

    // Send a sync request.
    let target_primary = fixture.authorities().nth(1).unwrap();
    let message = WorkerSynchronizeMessage {
        digests: missing.clone(),
        target: target_primary.id(),
        is_certified: false,
    };
    // The sync request should succeed.
    handler.synchronize(anemo::Request::new(message)).await.unwrap();
}
