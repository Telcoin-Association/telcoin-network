//! Primary tests

use super::{Primary, PrimaryReceiverHandler};
use crate::{
    consensus::{LeaderSchedule, LeaderSwapTable},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use fastcrypto::{
    encoding::{Encoding, Hex},
    hash::Hash,
    traits::KeyPair as _,
};
use itertools::Itertools;
use prometheus::Registry;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tn_batch_validator::NoopBatchValidator;
use tn_config::ConsensusConfig;
use tn_network_libp2p::ConsensusNetwork;
use tn_network_types::{
    FetchCertificatesRequest, MockPrimaryToWorker, PrimaryToPrimary, RequestVoteRequest,
};
use tn_storage::{mem_db::MemDatabase, traits::Database};
use tn_test_utils::{
    fixture_batch_with_transactions, make_optimal_signed_certificates, test_network,
    CommitteeFixture,
};
use tn_types::{
    now, AuthorityIdentifier, BlockHash, Certificate, Committee, ExecHeader, SealedHeader,
    SignatureVerificationState, TaskManager,
};
use tn_worker::{metrics::Metrics, Worker};
use tokio::{sync::mpsc, time::timeout};

fn get_bus_and_primary<DB: Database>(config: ConsensusConfig<DB>) -> (ConsensusBus, Primary<DB>) {
    let (event_stream, rx_event_stream) = mpsc::channel(1000);
    let consensus_bus = ConsensusBus::new_with_args(config.config().parameters.gc_depth);
    let consensus_network = ConsensusNetwork::new_for_primary(&config, event_stream)
        .expect("p2p network create failed!");
    let consensus_network_handle = consensus_network.network_handle();

    let primary = Primary::new(config, &consensus_bus, consensus_network_handle, rx_event_stream);
    (consensus_bus, primary)
}

#[tokio::test]
async fn test_get_network_peers_from_admin_server() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let authority_1 = fixture.authorities().next().unwrap();
    let config_1 = authority_1.consensus_config();
    let primary_1_parameters = config_1.config().parameters.clone();

    let worker_id = 0;
    let worker_1_keypair = authority_1.worker().keypair().copy();

    let (cb_1, mut primary_1) = get_bus_and_primary(config_1.clone());
    // Spawn Primary 1
    primary_1.spawn(
        config_1.clone(),
        &cb_1,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        &TaskManager::default(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    let registry_1 = Registry::new();
    let metrics_1 = Metrics::new_with_registry(&registry_1);

    // Spawn a `Worker` instance for primary 1.
    let _ = Worker::spawn(
        worker_id,
        Arc::new(NoopBatchValidator),
        metrics_1,
        config_1.clone(),
        &TaskManager::default(),
    );

    // Test getting all known peers for primary 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        primary_1_parameters.network_admin_server.primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 7 peers (3 other primaries + 1 workers + 1*3 other workers)
    assert_eq!(7, resp.len());

    // Test getting all connected peers for primary 1
    let mut resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_1_parameters.network_admin_server.primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    let mut i = 0;
    while i < 10 && resp.is_empty() {
        i += 1;
        std::thread::sleep(Duration::from_millis(1000));
        resp = reqwest::get(format!(
            "http://127.0.0.1:{}/peers",
            primary_1_parameters.network_admin_server.primary_network_admin_server_port
        ))
        .await
        .unwrap()
        .json::<Vec<String>>()
        .await
        .unwrap();
    }
    // Assert we returned 1 peers (only 1 worker spawned)
    assert_eq!(1, resp.len());

    let authority_2 = fixture.authorities().nth(1).unwrap();
    let config_2 = authority_2.consensus_config();
    let primary_2_parameters = config_2.config().parameters.clone();

    // Spawn Primary 2
    let (cb_2, mut primary_2) = get_bus_and_primary(config_2.clone());
    primary_2.spawn(
        config_2,
        &cb_2,
        LeaderSchedule::new(committee, LeaderSwapTable::default()),
        &TaskManager::default(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let primary_1_peer_id =
        Hex::encode(authority_1.primary_network_keypair().copy().public().0.as_bytes());
    let primary_2_peer_id =
        Hex::encode(authority_2.primary_network_keypair().copy().public().0.as_bytes());
    let worker_1_peer_id = Hex::encode(worker_1_keypair.copy().public().0.as_bytes());

    // Test getting all connected peers for primary 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_1_parameters.network_admin_server.primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 other primary spawned + 1 worker spawned)
    assert_eq!(2, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_2_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Test getting all connected peers for primary 2
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_2_parameters.network_admin_server.primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 other primary spawned + 1 other worker)
    assert_eq!(2, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = [&primary_1_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let network = test_network(target.primary_network_keypair(), target.network_address());

    let certificate_store = target.consensus_config().node_storage().certificate_store.clone();
    let payload_store = target.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(target.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        target.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(BlockHash::default()) // dummy_hash would be correct here but this is the test...
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, (worker_id, _)) in cert.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    // Trying to build on off of a missing execution block, will be an error.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await;
    let result = result.unwrap();
    assert!(result.is_err(), "{:?}", result);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_older_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let network = test_network(target.primary_network_keypair(), target.network_address());

    let certificate_store = target.consensus_config().node_storage().certificate_store.clone();
    let payload_store = target.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    // This will be an "older" execution block, test this still works.
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut dummy = ExecHeader { nonce: 110_u64.into(), ..Default::default() };
    dummy.nonce = 110_u64.into();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(SealedHeader::seal(dummy)));
    dummy = ExecHeader { nonce: 120_u64.into(), ..Default::default() };
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(SealedHeader::seal(dummy)));
    let synchronizer = StateSynchronizer::new(target.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        target.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(dummy_hash)
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, (worker_id, _)) in cert.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    // Trying to build on off of a missing execution block, will be an error.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await;
    let result = result.unwrap();
    assert!(result.is_ok(), "{:?}", result);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let network = test_network(target.primary_network_keypair(), target.network_address());

    let certificate_store = target.consensus_config().node_storage().certificate_store.clone();
    let payload_store = target.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(target.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        target.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(dummy_hash)
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, (worker_id, _)) in cert.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    // TEST PHASE 1: Handler should report missing parent certificates to caller.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());
    let result = handler.request_vote(request).await;

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = result.unwrap().into_body().missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // TEST PHASE 2: Handler should not return additional unknown digests.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());
    // No additional missing parents will be requested.
    let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await;
    assert!(result.is_err(), "{:?}", result);

    // TEST PHASE 3: Handler should return error if header is too old.
    // Increase round threshold.
    let _ = cb.primary_round_updates().send(100);
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());
    // Because round 1 certificates are not in store, the missing parents will not be accepted yet.
    let result = handler.request_vote(request).await;
    assert!(result.is_err(), "{:?}", result);
    assert_eq!(
        // Returned error should be unretriable.
        anemo::types::response::StatusCode::BadRequest,
        result.err().unwrap().status()
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_accept_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let network = test_network(target.primary_network_keypair(), target.network_address());

    let certificate_store = target.consensus_config().node_storage().certificate_store.clone();
    let payload_store = target.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(target.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        target.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());

    let all_certificates = certificates.into_iter().collect_vec();
    let round_1_certs = all_certificates[..NUM_PARENTS].to_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .latest_execution_block(dummy_hash)
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();

    // Populate all round 1 certificates and some round 2 certificates into the storage.
    // The new header will have some round 2 certificates missing as parents, but these parents
    // should be able to get accepted.
    for cert in round_1_certs {
        for (digest, (worker_id, _)) in cert.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    for cert in round_2_parents {
        for (digest, (worker_id, _)) in cert.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    // Populate new header payload so they don't have to be retrieved.
    for (digest, (worker_id, _)) in test_header.payload() {
        payload_store.write(digest, worker_id).unwrap();
    }

    // TEST PHASE 1: Handler should report missing parent certificates to caller.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());
    let result = handler.request_vote(request).await;

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = result.unwrap().into_body().missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // TEST PHASE 2: Handler should process missing parent certificates and succeed.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header,
        parents: round_2_missing.clone(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await.unwrap();
    assert!(result.is_ok(), "{:?}", result);
}

#[tokio::test]
async fn test_request_vote_missing_batches() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let authority_id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let network = test_network(primary.primary_network_keypair(), primary.network_address());
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();
    let payload_store = primary.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        primary.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
            .build()
            .unwrap();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, (worker_id, _)) in certificate.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
    }
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .latest_execution_block(dummy_hash)
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();
    let test_digests: HashSet<_> =
        test_header.payload().iter().map(|(digest, _)| digest).cloned().collect();

    // Set up mock worker.
    let author_id = author.id();
    let worker = primary.worker();
    let worker_address = &worker.info().worker_address;
    let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
    let mut mock_server = MockPrimaryToWorker::new();
    mock_server
        .expect_synchronize()
        .withf(move |request| {
            let digests: HashSet<_> = request.body().digests.iter().cloned().collect();
            digests == test_digests && request.body().target == author_id
        })
        .times(1)
        .return_once(|_| Ok(anemo::Response::new(())));

    client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

    let _worker_network = worker.new_network(anemo::Router::new());
    let address = worker_address.to_anemo_address().unwrap();
    network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

    // Verify Handler synchronizes missing batches and generates a Vote.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let response = handler.request_vote(request).await.unwrap();
    assert!(response.body().vote.is_some());
}

#[tokio::test]
async fn test_request_vote_already_voted() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let network = test_network(primary.primary_network_keypair(), primary.network_address());
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();
    let payload_store = primary.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);

    let handler = PrimaryReceiverHandler::new(
        primary.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
            .build()
            .unwrap();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, (worker_id, _)) in certificate.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let worker = primary.worker();
    let worker_address = &worker.info().worker_address;
    let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
    let mut mock_server = MockPrimaryToWorker::new();
    // Always Synchronize successfully.
    mock_server.expect_synchronize().returning(|_| Ok(anemo::Response::new(())));

    client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

    let _worker_network = worker.new_network(anemo::Router::new());
    let address = worker_address.to_anemo_address().unwrap();
    network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

    // Verify Handler generates a Vote.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(dummy_hash)
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let response = tokio::time::timeout(Duration::from_secs(10), handler.request_vote(request))
        .await
        .unwrap()
        .unwrap();
    assert!(response.body().vote.is_some());
    let vote = response.into_body().vote.unwrap();

    // Verify the same request gets the same vote back successfully.
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let response = handler.request_vote(request).await.unwrap();
    assert!(response.body().vote.is_some());
    assert_eq!(vote.digest(), response.into_body().vote.unwrap().digest());

    // Verify a different request for the same round receives an error.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(dummy_hash)
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();
    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let response = handler.request_vote(request).await;
    assert_eq!(
        // Returned error should not be retriable.
        anemo::types::response::StatusCode::BadRequest,
        response.err().unwrap().status()
    );
}

// NOTE: this is unit tested in primary::state_sync
#[tokio::test]
async fn test_fetch_certificates_handler() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

    let cb = ConsensusBus::new();
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        primary.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
        .into_iter()
        .map(|cert| cert.header().clone())
        .collect();
    let mut headers = vec![];
    let total_rounds = 4;
    for i in 0..total_rounds {
        let parents: BTreeSet<_> =
            current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }

    let total_authorities = fixture.authorities().count();
    let total_certificates = total_authorities * total_rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates);
    assert_eq!(16, total_certificates);

    // Populate certificate store such that each authority has the following rounds:
    // Authority 0: 1
    // Authority 1: 1 2
    // Authority 2: 1 2 3
    // Authority 3: 1 2 3 4
    // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
    // already in store. But this does not matter for testing here.
    let mut authorities = Vec::<AuthorityIdentifier>::new();
    for i in 0..total_authorities {
        authorities.push(certificates[i].header().author());
        for j in 0..=i {
            let mut cert = certificates[i + j * total_authorities].clone();
            assert_eq!(&cert.header().author(), authorities.last().unwrap());
            if i == 3 && j == 3 {
                // Simulating only 1 directly verified certificate (Auth 3 Round 4) being stored.
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedDirectly(
                        cert.aggregated_signature().expect("Invalid Signature").clone(),
                    ),
                );
            } else {
                // Simulating some indirectly verified certificates being stored.
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedIndirectly(
                        cert.aggregated_signature().expect("Invalid Signature").clone(),
                    ),
                );
            }
            certificate_store.write(cert).expect("Writing certificate to store failed");
        }
    }

    // Each test case contains (lower bound round, skip rounds, max items, expected output).
    let test_cases = vec![
        (0, vec![vec![], vec![], vec![], vec![]], 20, vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![1u32], vec![1], vec![], vec![]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![], vec![], vec![1], vec![1]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 4, vec![2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2, 3]),
        (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], 2, vec![2, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
        // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
        (1, vec![vec![], vec![], vec![3], vec![3]], 5, vec![2, 2, 2, 4]),
    ];
    for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in test_cases {
        let req = FetchCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                    .collect(),
            )
            .set_max_items(max_items);
        let resp =
            handler.fetch_certificates(anemo::Request::new(req.clone())).await.unwrap().into_body();
        assert_eq!(
            resp.certificates.iter().map(|cert| cert.round()).collect_vec(),
            expected_rounds
        );
    }
}

#[tokio::test]
async fn test_request_vote_created_at_in_future() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let network = test_network(primary.primary_network_keypair(), primary.network_address());
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();
    let payload_store = primary.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    let handler = PrimaryReceiverHandler::new(
        primary.consensus_config(),
        synchronizer.clone(),
        cb.clone(),
        Default::default(),
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
            .build()
            .unwrap();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, (worker_id, _)) in certificate.header().payload() {
            payload_store.write(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let worker = primary.worker();
    let worker_address = &worker.info().worker_address;
    let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
    let mut mock_server = MockPrimaryToWorker::new();
    // Always Synchronize successfully.
    mock_server.expect_synchronize().returning(|_| Ok(anemo::Response::new(())));

    client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

    let _worker_network = worker.new_network(anemo::Router::new());
    let address = worker_address.to_anemo_address().unwrap();
    network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

    // Verify Handler generates a Vote.

    // Set the creation time to be deep in the future (an hour)
    let created_at = now() + 60 * 60;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(dummy_hash)
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .created_at(created_at)
        .build()
        .unwrap();

    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    // For such a future header we get back an error
    assert!(handler.request_vote(request).await.is_err());

    // Verify Handler generates a Vote.

    // Set the creation time to be a bit in the future (1s)
    let created_at = now() + 1;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .latest_execution_block(dummy_hash)
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .created_at(created_at)
        .build()
        .unwrap();

    let mut request = anemo::Request::new(RequestVoteRequest {
        header: test_header.clone(),
        parents: Vec::new(),
    });
    assert!(request.extensions_mut().insert(network.downgrade()).is_none());
    assert!(request
        .extensions_mut()
        .insert(anemo::PeerId(author.primary_network_public_key().0.to_bytes()))
        .is_none());

    let response = handler.request_vote(request).await.unwrap();
    assert!(response.body().vote.is_some());
    assert!(created_at <= now());
}
