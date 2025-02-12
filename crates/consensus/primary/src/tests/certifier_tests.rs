//! Certifier tests

use super::*;
use crate::ConsensusBus;
use fastcrypto::traits::KeyPair;
use rand::{rngs::StdRng, SeedableRng};
use std::num::NonZeroUsize;
use tn_network_types::{MockPrimaryToPrimary, PrimaryToPrimaryServer, RequestVoteResponse};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsKeypair, Notifier, SignatureVerificationState, TnSender};

#[tokio::test(flavor = "current_thread")]
async fn propose_header_to_form_certificate() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network.
    let network = primary.new_network(anemo::Router::new());

    // Set up remote primaries responding with votes.
    let mut peer_networks = Vec::new();
    for peer in fixture.authorities().filter(|a| a.id() != id) {
        let address = committee.primary(&peer.primary_public_key()).unwrap();
        let name = peer.id();
        let vote = Vote::new(&proposed_header, &name, peer.consensus_config().key_config()).await;
        let mut mock_server = MockPrimaryToPrimary::new();
        let mut mock_seq = mockall::Sequence::new();
        // Verify errors are retried.
        mock_server.expect_request_vote().times(3).in_sequence(&mut mock_seq).returning(
            move |_request| {
                Err(anemo::rpc::Status::new(anemo::types::response::StatusCode::Unknown))
            },
        );
        mock_server.expect_request_vote().times(1).in_sequence(&mut mock_seq).return_once(
            move |_request| {
                Ok(anemo::Response::new(RequestVoteResponse {
                    vote: Some(vote),
                    missing: Vec::new(),
                }))
            },
        );
        let routes = anemo::Router::new().add_rpc_service(PrimaryToPrimaryServer::new(mock_server));
        peer_networks.push(peer.new_network(routes));

        let address = address.to_anemo_address().unwrap();
        let peer_id = anemo::PeerId(peer.primary_network_keypair().public().0.to_bytes());
        network.connect_with_peer_id(address, peer_id).await.unwrap();
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());

    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone(),
        &task_manager,
    );

    // Propose header and ensure that a certificate is formed by pulling it out of the
    // consensus channel.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();
    let certificate = tokio::time::timeout(Duration::from_secs(10), rx_new_certificates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.header().digest(), proposed_digest);
    assert!(matches!(
        certificate.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_header_failure() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let authority_id = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network.
    let own_address = committee.primary_by_id(&authority_id).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("tn-test")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();

    // Set up remote primaries responding with votes.
    let mut primary_networks = Vec::new();
    for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
        let address = committee.primary(&primary.primary_public_key()).unwrap();
        let mut mock_server = MockPrimaryToPrimary::new();
        mock_server.expect_request_vote().returning(move |_request| {
            Err(anemo::rpc::Status::new(
                anemo::types::response::StatusCode::BadRequest, // unretriable
            ))
        });
        let routes = anemo::Router::new().add_rpc_service(PrimaryToPrimaryServer::new(mock_server));
        primary_networks.push(primary.new_network(routes));

        let address = address.to_anemo_address().unwrap();
        let peer_id = anemo::PeerId(primary.primary_network_keypair().public().0.to_bytes());
        network.connect_with_peer_id(address, peer_id).await.unwrap();
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());

    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone(),
        &task_manager,
    );

    // Propose header and verify we get no certificate back.
    cb.headers().send(proposed_header).await.unwrap();
    if let Ok(result) =
        tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv()).await
    {
        panic!("expected no certificate to form; got {result:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_scenario_with_bad_sigs() {
    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(6, 0, true).await;
    run_vote_aggregator_with_param(6, 1, true).await;
    run_vote_aggregator_with_param(6, 2, false).await;

    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(4, 0, true).await;
    run_vote_aggregator_with_param(4, 1, true).await;
    run_vote_aggregator_with_param(4, 2, false).await;
}

async fn run_vote_aggregator_with_param(
    committee_size: usize,
    num_byzantine: usize,
    expect_cert: bool,
) {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(committee_size).unwrap())
        .randomize_ports(true)
        .build();

    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id: AuthorityIdentifier = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network.
    let network = primary.new_network(anemo::Router::new());

    // Set up remote primaries responding with votes.
    let mut peer_networks = Vec::new();
    for (i, peer) in fixture.authorities().filter(|a| a.id() != id).enumerate() {
        let address = committee.primary(&peer.primary_public_key()).unwrap();
        let name = peer.id();
        // Create bad signature for a number of byzantines.
        let vote = if i < num_byzantine {
            let bad_key = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            Vote::new_with_signer(&proposed_header, &name, &bad_key)
        } else {
            Vote::new(&proposed_header, &name, peer.consensus_config().key_config()).await
        };
        let mut mock_server = MockPrimaryToPrimary::new();
        let mut mock_seq = mockall::Sequence::new();
        mock_server.expect_request_vote().times(1).in_sequence(&mut mock_seq).return_once(
            move |_request| {
                Ok(anemo::Response::new(RequestVoteResponse {
                    vote: Some(vote),
                    missing: Vec::new(),
                }))
            },
        );
        let routes = anemo::Router::new().add_rpc_service(PrimaryToPrimaryServer::new(mock_server));
        peer_networks.push(peer.new_network(routes));

        let address = address.to_anemo_address().unwrap();
        let peer_id = anemo::PeerId(peer.primary_network_keypair().public().0.to_bytes());
        network.connect_with_peer_id(address, peer_id).await.unwrap();
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(primary.consensus_config(), cb.clone(), synchronizer, network, &task_manager);

    // Send a proposed header.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();

    if expect_cert {
        // A cert is expected, checks that the header digest matches.
        let certificate = tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(certificate.header().digest(), proposed_digest);
    } else {
        // A cert is not expected, checks that it times out without forming the cert.
        assert!(tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn test_shutdown_core() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let id: AuthorityIdentifier = primary.id();

    let cb = ConsensusBus::new();
    // Make a synchronizer for the core.
    let synchronizer = StateSynchronizer::new(primary.consensus_config(), cb.clone());

    let own_address = committee.primary_by_id(&id).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("conensus-test")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();

    // Spawn the core.
    let mut task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer.clone(),
        network.clone(),
        &task_manager,
    );

    // Shutdown the core.
    fixture.notify_shutdown();
    task_manager.join(Notifier::default()).await;
}
