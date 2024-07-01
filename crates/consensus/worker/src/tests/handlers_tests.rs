// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_network_types::{MockWorkerToWorker, WorkerToWorkerServer};
use std::vec;
use tn_types::test_utils::{batch, random_network, CommitteeFixture};

use super::*;
use tn_batch_validator::NoopBatchValidator;

#[tokio::test]
async fn synchronize() {
    reth_tracing::init_test_tracing();

    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let id = 0;

    // Create a new test store.
    let store = tn_types::test_utils::create_batch_store();

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
    let target_worker = target_primary.worker(id);
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
        validator: NoopBatchValidator,
    };

    // Verify the batch is not in store
    assert!(store.get(&digest).unwrap().is_none());

    // Send a sync request.
    let request = anemo::Request::new(message);
    handler.synchronize(request).await.unwrap();

    // Verify it is now stored
    assert!(store.get(&digest).unwrap().is_some());
}

// TODO: Remove once we have removed BatchV1 from the codebase.
#[tokio::test]
async fn synchronize_versioned_batches() {
    reth_tracing::init_test_tracing();

    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let id = 0;

    // Create a new test store.
    let store = tn_types::test_utils::create_batch_store();

    // Create network with mock behavior to respond to RequestBatches request.
    let target_primary = fixture.authorities().nth(1).unwrap();
    // protocol versions under 12 should only support BatchV1
    let batch_v1 = batch();
    let digest_v1 = batch_v1.digest();
    let message_v1 = WorkerSynchronizeMessage {
        digests: vec![digest_v1],
        target: target_primary.id(),
        is_certified: false,
    };

    let batch_v2 = batch();
    let digest_v2 = batch_v2.digest();
    let message_v2 = WorkerSynchronizeMessage {
        digests: vec![digest_v2],
        target: target_primary.id(),
        is_certified: false,
    };

    let mut mock_server = MockWorkerToWorker::new();
    let mock_batch_response_v1 = batch_v1.clone();
    mock_server
        .expect_request_batches()
        .withf(move |request| request.body().batch_digests == vec![digest_v1])
        .times(1)
        .returning(move |_| {
            Ok(anemo::Response::new(RequestBatchesResponse {
                batches: vec![mock_batch_response_v1.clone()],
                is_size_limit_reached: false,
            }))
        });
    let mock_batch_response_v2 = batch_v2.clone();
    mock_server
        .expect_request_batches()
        .withf(move |request| request.body().batch_digests == vec![digest_v2])
        .times(1)
        .returning(move |_| {
            Ok(anemo::Response::new(RequestBatchesResponse {
                batches: vec![mock_batch_response_v2.clone()],
                is_size_limit_reached: false,
            }))
        });
    let routes = anemo::Router::new().add_rpc_service(WorkerToWorkerServer::new(mock_server));
    let target_worker = target_primary.worker(id);
    let _recv_network = target_worker.new_network(routes);
    let send_network = random_network();
    send_network
        .connect_with_peer_id(
            target_worker.info().worker_address.to_anemo_address().unwrap(),
            anemo::PeerId(target_worker.info().name.0.to_bytes()),
        )
        .await
        .unwrap();

    let handler_latest_version = PrimaryReceiverHandler {
        id,
        committee,
        worker_cache,
        store: store.clone(),
        request_batches_timeout: Duration::from_secs(999),
        network: Some(send_network),
        batch_fetcher: None,
        validator: NoopBatchValidator,
    };

    // Case #1: Receive BatchV1 but network is upgraded past v11 so we fail because we expect
    // BatchV2
    let request = anemo::Request::new(message_v1);
    let response = handler_latest_version.synchronize(request).await;
    assert!(response.is_err());

    // Case #2: Receive BatchV2 and network is upgraded past v11 so we are okay
    assert!(store.get(&digest_v2).unwrap().is_none());
    let request = anemo::Request::new(message_v2.clone());
    handler_latest_version.synchronize(request).await.unwrap();
    assert!(store.get(&digest_v2).unwrap().is_some());
}

#[tokio::test]
async fn synchronize_when_batch_exists() {
    reth_tracing::init_test_tracing();

    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let id = 0;

    // Create a new test store.
    let store = tn_types::test_utils::create_batch_store();

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
        validator: NoopBatchValidator,
    };

    // Store the batch.
    let batch = batch();
    let batch_id = batch.digest();
    let missing = vec![batch_id];
    store.insert(&batch_id, &batch).unwrap();

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
