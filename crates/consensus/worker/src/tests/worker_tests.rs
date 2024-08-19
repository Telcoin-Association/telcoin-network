// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use async_trait::async_trait;
use fastcrypto::encoding::{Encoding, Hex};
use narwhal_primary::{
    consensus::{ConsensusRound, LeaderSchedule, LeaderSwapTable},
    Primary, CHANNEL_CAPACITY, NUM_SHUTDOWN_RECEIVERS,
};
use narwhal_storage::NodeStorage;

use prometheus::Registry;
use tn_batch_validator::NoopBatchValidator;
use tn_types::{
    test_utils::{temp_dir, CommitteeFixture},
    Batch, ChainIdentifier,
};
use tokio::sync::watch;

// A test validator that rejects every batch
#[derive(Clone)]
#[allow(dead_code)]
struct NilBatchValidator;
#[async_trait]
impl BatchValidation for NilBatchValidator {
    type Error = eyre::Report;

    async fn validate_batch(&self, _txs: &Batch) -> Result<(), Self::Error> {
        eyre::bail!("Invalid batch");
    }
}

// #[tokio::test]
// async fn reject_invalid_clients_transactions() {
//     let fixture = CommitteeFixture::builder().randomize_ports(true).build();
//     let committee = fixture.committee();
//     let worker_cache = fixture.worker_cache();

//     let worker_id = 0;
//     let my_primary = fixture.authorities().next().unwrap();
//     let myself = my_primary.worker(worker_id);
//     let public_key = my_primary.public_key();
//     let client = NetworkClient::new_from_keypair(&my_primary.network_keypair());

//     let parameters = Parameters {
//         batch_size: 200, // Two transactions.
//         ..Parameters::default()
//     };

//     // Create a new test store.
//     let batch_store = MemDB::<BatchDigest, Batch>::open().unwrap();

//     let registry = Registry::new();
//     let metrics = Metrics::new_with_registry(&registry);

//     let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

//     // Spawn a `Worker` instance with a reject-all validator.
//     Worker::spawn(
//         my_primary.authority().clone(),
//         myself.keypair(),
//         worker_id,
//         committee.clone(),
//         worker_cache.clone(),
//         parameters,
//         NilTxValidator,
//         client,
//         batch_store,
//         metrics,
//         &mut tx_shutdown,
//     );

//     // Wait till other services have been able to start up
//     tokio::task::yield_now().await;
//     // Send enough transactions to create a batch.
//     let address = worker_cache.worker(&public_key, &worker_id).unwrap().transactions;
//     let config = tn_types::Config::new();
//     let channel = config.connect_lazy(&address).unwrap();
//     let mut client = TransactionsClient::new(channel);
//     let tx = transaction();
//     let txn = TransactionProto { transaction: Bytes::from(tx.clone()) };

//     // Check invalid transactions are rejected
//     let res = client.submit_transaction(txn).await;
//     assert!(res.is_err());

//     let worker_pk = worker_cache.worker(&public_key, &worker_id).unwrap().name;

//     let batch = batch();
//     let batch_message = WorkerBatchMessage { batch: batch.clone() };

//     // setup network : impersonate a send from another worker
//     let another_primary = fixture.authorities().nth(2).unwrap();
//     let another_worker = another_primary.worker(worker_id);
//     let network = test_network(another_worker.keypair(), &another_worker.info().worker_address);
//     // ensure that the networks are connected
//     network.connect(myself.info().worker_address.to_anemo_address().unwrap()).await.unwrap();
//     let peer = network.peer(PeerId(worker_pk.0.to_bytes())).unwrap();

//     // Check invalid batches are rejected
//     let res = WorkerToWorkerClient::new(peer).report_batch(batch_message).await;
//     assert!(res.is_err());
// }

// /// TODO: test both RemoteNarwhalClient and LocalNarwhalClient in the same test case.
// #[tokio::test]
// async fn handle_remote_clients_transactions() {
//     let fixture = CommitteeFixture::builder().randomize_ports(true).build();
//     let committee = fixture.committee();
//     let worker_cache = fixture.worker_cache();

//     let worker_id = 0;
//     let my_primary = fixture.authorities().next().unwrap();
//     let myself = my_primary.worker(worker_id);
//     let authority_public_key = my_primary.public_key();
//     let client = NetworkClient::new_from_keypair(&my_primary.network_keypair());

//     let parameters = Parameters {
//         batch_size: 200, // Two transactions.
//         ..Parameters::default()
//     };

//     // Create a new test store.
//     let batch_store = MemDB::<BatchDigest, Batch>::open().unwrap();

//     let registry = Registry::new();
//     let metrics = Metrics::new_with_registry(&registry);

//     let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

//     // Spawn a `Worker` instance.
//     Worker::spawn(
//         my_primary.authority().clone(),
//         myself.keypair(),
//         worker_id,
//         committee.clone(),
//         worker_cache.clone(),
//         parameters,
//         TrivialTransactionValidator,
//         client.clone(),
//         batch_store,
//         metrics,
//         &mut tx_shutdown,
//     );

//     // Spawn a network listener to receive our batch's digest.
//     let mut peer_networks = Vec::new();

//     // Create batches
//     let batch = batch();
//     let batch_digest = batch.digest();

//     let (tx_await_batch, mut rx_await_batch) =
// tn_types::test_channel!(CHANNEL_CAPACITY);
// let mut mock_primary_server = MockWorkerToPrimary::new();
//     mock_primary_server
//         .expect_report_own_batch()
//         .withf(move |request| {
//             let message = request.body();
//
//             message.digest == batch_digest && message.worker_id == worker_id
//         })
//         .times(1)
//         .returning(move |_| {
//             tx_await_batch.try_send(()).unwrap();
//             Ok(anemo::Response::new(()))
//         });
//     client.set_worker_to_primary_local_handler(Arc::new(mock_primary_server));
//
//     // Spawn enough workers' listeners to acknowledge our batches.
//     for worker in fixture.authorities().skip(1).map(|a| a.worker(worker_id)) {
//         let mut mock_server = MockWorkerToWorker::new();
//         mock_server.expect_report_batch().returning(|_| Ok(anemo::Response::new(())));
//         let routes =
// anemo::Router::new().add_rpc_service(WorkerToWorkerServer::new(mock_server));
//         peer_networks.push(worker.new_network(routes));
//     }
//
//     // Wait till other services have been able to start up
//     tokio::task::yield_now().await;
//     // Send enough transactions to create a batch.
//     let address = worker_cache.worker(&authority_public_key, &worker_id).unwrap().transactions;
//     let config = tn_types::Config::new();
//     let channel = config.connect_lazy(&address).unwrap();
//     let client = TransactionsClient::new(channel);

//     let join_handle = tokio::task::spawn(async move {
//         let mut fut_list = FuturesOrdered::new();
//         for tx in batch.transactions() {
//             let txn = TransactionProto { transaction: Bytes::from(tx.clone()) };

//             // Calls to submit_transaction are now blocking, so we need to drive them
//             // all at the same time, rather than sequentially.
//             let mut inner_client = client.clone();
//             fut_list.push_back(async move {
//                 inner_client.submit_transaction(txn).await.unwrap();
//             });
//         }

//         // Drive all sending in parallel.
//         while fut_list.next().await.is_some() {}
//     });

//     // Ensure the primary received the batch's digest (ie. it did not panic).
//     rx_await_batch.recv().await.unwrap();

//     // Ensure sending ended.
//     assert!(join_handle.await.is_ok());
// }

// / TODO: test both RemoteNarwhalClient and LocalNarwhalClient in the same test case.
// /
// / Probably save to delete this since there is an integration test for EL batch maker
// #[tokio::test]
// async fn handle_local_clients_transactions() {
//     let fixture = CommitteeFixture::builder().randomize_ports(true).build();
//     let committee = fixture.committee();
//     let worker_cache = fixture.worker_cache();

//     let worker_id = 0;
//     let my_primary = fixture.authorities().next().unwrap();
//     let myself = my_primary.worker(worker_id);
//     let authority_public_key = my_primary.public_key();
//     let client = NetworkClient::new_from_keypair(&my_primary.network_keypair());

//     let parameters = Parameters {
//         batch_size: 200, // Two transactions.
//         ..Parameters::default()
//     };

//     // Create a new test store.
//     let batch_store = MemDB::<BatchDigest, Batch>::open().unwrap();

//     let registry = Registry::new();
//     let metrics = Metrics::new_with_registry(&registry);

//     let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

//     // For EL batch maker
//     let channel_metrics: Arc<WorkerChannelMetrics> =
//         Arc::new(metrics.clone().channel_metrics.unwrap());
//     let (_tx_batch_maker, rx_batch_maker) = channel_with_total(
//         CHANNEL_CAPACITY,
//         &channel_metrics.tx_batch_maker,
//         &channel_metrics.tx_batch_maker_total,
//     );

//     // Spawn a `Worker` instance.
//     Worker::spawn(
//         my_primary.authority().clone(),
//         myself.keypair(),
//         worker_id,
//         committee.clone(),
//         worker_cache.clone(),
//         parameters,
//         TrivialTransactionValidator,
//         client.clone(),
//         batch_store,
//         metrics,
//         &mut tx_shutdown,
//         channel_metrics,
//         rx_batch_maker,
//     );

//     // Spawn a network listener to receive our batch's digest.
//     let mut peer_networks = Vec::new();

//     // Create batches
//     let batch = batch();
//     let batch_digest = batch.digest();

//     let (tx_await_batch, mut rx_await_batch) = tn_types::test_channel!(CHANNEL_CAPACITY);
//     let mut mock_primary_server = MockWorkerToPrimary::new();
//     mock_primary_server
//         .expect_report_own_batch()
//         .withf(move |request| {
//             let message = request.body();
//             message.digest == batch_digest && message.worker_id == worker_id
//         })
//         .times(1)
//         .returning(move |_| {
//             tx_await_batch.try_send(()).unwrap();
//             Ok(anemo::Response::new(()))
//         });
//     client.set_worker_to_primary_local_handler(Arc::new(mock_primary_server));

//     // Spawn enough workers' listeners to acknowledge our batches.
//     for worker in fixture.authorities().skip(1).map(|a| a.worker(worker_id)) {
//         let mut mock_server = MockWorkerToWorker::new();
//         mock_server.expect_report_batch().returning(|_| Ok(anemo::Response::new(())));
//         let routes =
// anemo::Router::new().add_rpc_service(WorkerToWorkerServer::new(mock_server));
//         peer_networks.push(worker.new_network(routes));
//     }

//     // Wait till other services have been able to start up
//     tokio::task::yield_now().await;
//     // Send enough transactions to create a batch.
//     let address = worker_cache.worker(&authority_public_key, &worker_id).unwrap().transactions;
//     let client = LocalNarwhalClient::get_global(&address).unwrap().load();

//     let join_handle = tokio::task::spawn(async move {
//         let mut fut_list = FuturesOrdered::new();
//         for txn in batch.transactions() {
//             // Calls to submit_transaction are now blocking, so we need to drive them
//             // all at the same time, rather than sequentially.
//             let inner_client = client.clone();
//             fut_list.push_back(async move {
//                 inner_client.submit_transaction(txn.clone()).await.unwrap();
//             });
//         }

//         // Drive all sending in parallel.
//         while fut_list.next().await.is_some() {}
//     });

//     // Ensure the primary received the batch's digest (ie. it did not panic).
//     rx_await_batch.recv().await.unwrap();

//     // Ensure sending ended.
//     assert!(join_handle.await.is_ok());
// }

#[tokio::test]
async fn get_network_peers_from_admin_server() {
    // reth_tracing::init_test_tracing();
    let primary_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let authority_1 = fixture.authorities().next().unwrap();
    let signer_1 = authority_1.keypair().copy();
    let client_1 = NetworkClient::new_from_keypair(&authority_1.network_keypair());

    let worker_id = 0;
    let worker_1_keypair = authority_1.worker(worker_id).keypair().copy();

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir(), None);

    let (tx_new_certificates, _rx_new_certificates) =
        tn_types::test_new_certificates_channel!(CHANNEL_CAPACITY);
    let (_tx_feedback, rx_feedback) = tn_types::test_channel!(CHANNEL_CAPACITY);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) =
        watch::channel(ConsensusRound::default());
    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    // Spawn Primary 1
    Primary::spawn(
        authority_1.authority().clone(),
        signer_1,
        authority_1.network_keypair().copy(),
        committee.clone(),
        worker_cache.clone(),
        ChainIdentifier::unknown(),
        primary_1_parameters.clone(),
        client_1.clone(),
        store.certificate_store.clone(),
        store.proposer_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        tx_new_certificates,
        rx_feedback,
        rx_consensus_round_updates,
        &mut tx_shutdown,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        &narwhal_primary_metrics::Metrics::default(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_1 = Registry::new();
    let metrics_1 = Metrics::new_with_registry(&registry_1);
    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    let worker_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // For EL batch maker
    let channel_metrics: Arc<WorkerChannelMetrics> = metrics_1.channel_metrics.clone();
    let (_tx_batch_maker, rx_batch_maker) = channel_with_total(
        CHANNEL_CAPACITY,
        &channel_metrics.tx_batch_maker,
        &channel_metrics.tx_batch_maker_total,
    );

    // Spawn a `Worker` instance for primary 1.
    Worker::spawn(
        authority_1.authority().clone(),
        worker_1_keypair.copy(),
        worker_id,
        committee.clone(),
        worker_cache.clone(),
        worker_1_parameters.clone(),
        NoopBatchValidator,
        client_1.clone(),
        store.batch_store.clone(),
        metrics_1.clone(),
        &mut tx_shutdown,
        channel_metrics,
        rx_batch_maker,
    );

    let primary_1_peer_id = Hex::encode(authority_1.network_keypair().copy().public().0.as_bytes());
    let worker_1_peer_id = Hex::encode(worker_1_keypair.copy().public().0.as_bytes());

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test getting all known peers for worker 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (1 primary + 3 other workers)
    assert_eq!(4, resp.len());

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 1 peer (only worker's primary spawned)
    assert_eq!(1, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    let authority_2 = fixture.authorities().nth(1).unwrap();
    let signer_2 = authority_2.keypair().copy();
    let client_2 = NetworkClient::new_from_keypair(&authority_2.network_keypair());

    let worker_2_keypair = authority_2.worker(worker_id).keypair().copy();

    let primary_2_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    let (tx_new_certificates_2, _rx_new_certificates_2) =
        tn_types::test_new_certificates_channel!(CHANNEL_CAPACITY);
    let (_tx_feedback_2, rx_feedback_2) = tn_types::test_channel!(CHANNEL_CAPACITY);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) =
        watch::channel(ConsensusRound::default());

    let mut tx_shutdown_2 = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    // Spawn Primary 2
    Primary::spawn(
        authority_2.authority().clone(),
        signer_2,
        authority_2.network_keypair().copy(),
        committee.clone(),
        worker_cache.clone(),
        ChainIdentifier::unknown(),
        primary_2_parameters.clone(),
        client_2.clone(),
        store.certificate_store.clone(),
        store.proposer_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        tx_new_certificates_2,
        rx_feedback_2,
        rx_consensus_round_updates,
        &mut tx_shutdown_2,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        &narwhal_primary_metrics::Metrics::default(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_2 = Registry::new();
    let metrics_2 = Metrics::new_with_registry(&registry_2);

    let worker_2_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    let mut tx_shutdown_worker = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    // For EL batch maker
    let channel_metrics: Arc<WorkerChannelMetrics> = metrics_2.channel_metrics.clone();
    let (_tx_batch_maker, rx_batch_maker) = channel_with_total(
        CHANNEL_CAPACITY,
        &channel_metrics.tx_batch_maker,
        &channel_metrics.tx_batch_maker_total,
    );

    // Spawn a `Worker` instance for primary 2.
    Worker::spawn(
        authority_2.authority().clone(),
        worker_2_keypair.copy(),
        worker_id,
        committee.clone(),
        worker_cache.clone(),
        worker_2_parameters.clone(),
        NoopBatchValidator,
        client_2,
        store.batch_store,
        metrics_2.clone(),
        &mut tx_shutdown_worker,
        channel_metrics,
        rx_batch_maker,
    );

    // Wait for tasks to start. Sleeping longer here to ensure all primaries and workers
    // have  a chance to connect to each other.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let primary_2_peer_id = Hex::encode(authority_2.network_keypair().copy().public().0.as_bytes());
    let worker_2_peer_id = Hex::encode(worker_2_keypair.copy().public().0.as_bytes());

    // Test getting all known peers for worker 2 (worker at index 0 for primary 2)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        worker_2_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 4 peers (1 primary + 3 other workers)
    assert_eq!(4, resp.len());

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (2 primaries spawned + 1 other worker spawned)
    assert_eq!(3, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id, &primary_2_peer_id, &worker_2_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Test getting all connected peers for worker 2 (worker at index 0 for primary 2)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_2_parameters.network_admin_server.worker_network_admin_server_base_port + worker_id
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (2 primaries spawned  + 1 other worker spawned)
    assert_eq!(3, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id, &primary_2_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Assert network connectivity metrics are also set as expected
    let filters = vec![
        (primary_2_peer_id.as_str(), "our_primary"),
        (primary_1_peer_id.as_str(), "other_primary"),
        (worker_1_peer_id.as_str(), "other_worker"),
    ];

    for f in filters {
        let mut m = HashMap::new();
        m.insert("peer_id", f.0);
        m.insert("type", f.1);

        assert_eq!(
            1,
            metrics_2
                .clone()
                .network_connection_metrics
                .network_peer_connected
                .get_metric_with(&m)
                .unwrap()
                .get()
        );
    }
}
