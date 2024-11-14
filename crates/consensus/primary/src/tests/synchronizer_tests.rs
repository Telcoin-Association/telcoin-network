// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    certificate_fetcher::CertificateFetcherCommand,
    consensus::{gc_round, ConsensusRound},
    synchronizer::Synchronizer,
    ConsensusBus,
};
use consensus_network::client::PrimaryClient;
use fastcrypto::{hash::Hash, traits::KeyPair};
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use std::{
    collections::{BTreeSet, HashMap},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{
    fixture_batch_with_transactions, make_optimal_signed_certificates, mock_signed_certificate,
    CommitteeFixture,
};
use tn_types::{
    error::DagError, BlsAggregateSignatureBytes, Certificate, Committee, Round,
    SignatureVerificationState, TnReceiver, TnSender,
};

#[tokio::test]
async fn accept_certificates() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let authority_id = primary.id();
    let client = PrimaryClient::new_from_keypair(&primary.primary_network_keypair());

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    let mut rx_parents = cb.parents().subscribe();
    // Make a synchronizer.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let own_address = committee.primary_by_id(&authority_id).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();
    client.set_primary_network(network.clone());

    // Send 3 certificates to the Synchronizer.
    let certificates: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
    for cert in certificates.clone() {
        synchronizer.try_accept_certificate(cert).await.unwrap();
    }

    // Ensure the Synchronizer sends the parents of the certificates to the proposer.
    //
    // The first messages are the Synchronizer letting us know about the round of parent
    // certificates
    for _i in 0..3 {
        let received = rx_parents.recv().await.unwrap();
        assert_eq!(received, (vec![], 0));
    }
    // the next message actually contains the parents
    let received = rx_parents.recv().await.unwrap();
    assert_eq!(received, (certificates.clone(), 1));

    // Ensure the Synchronizer sends the certificates to the consensus.
    for x in certificates.clone() {
        let received = rx_new_certificates.recv().await.unwrap();
        assert_eq!(received, x);
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = certificate_store.read(x.digest()).unwrap();
        assert_eq!(stored, Some(x.clone()));
    }

    let mut m = HashMap::new();
    m.insert("source", "other");
    assert_eq!(
        cb.primary_metrics().node_metrics.certificates_processed.get_metric_with(&m).unwrap().get(),
        3
    );
}

#[tokio::test]
async fn accept_suspended_certificates() {
    const NUM_AUTHORITIES: usize = 4;
    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_AUTHORITIES).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    // Make fake certificates.
    let committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());
    let certificates = certificates.into_iter().collect_vec();

    // Try to accept certificates from round 2 to 5. All of them should be suspended.
    let accept = FuturesUnordered::new();
    for cert in &certificates[NUM_AUTHORITIES..] {
        match synchronizer.try_accept_certificate(cert.clone()).await {
            Ok(()) => panic!("Unexpected acceptance of {cert:?}"),
            Err(DagError::Suspended(notify)) => {
                accept.push(async move { notify.wait().await });
                continue;
            }
            Err(e) => panic!("Unexpected error {e}"),
        }
    }

    // Try to accept certificates from round 1. All of them should be accepted.
    for cert in &certificates[..NUM_AUTHORITIES] {
        match synchronizer.try_accept_certificate(cert.clone()).await {
            Ok(()) => continue,
            Err(e) => panic!("Unexpected error {e}"),
        }
    }

    // Wait for all notifications to arrive.
    accept.collect::<Vec<()>>().await;

    // Try to accept certificates from round 2 and above again. All of them should be accepted.
    for cert in &certificates[NUM_AUTHORITIES..] {
        match synchronizer.try_accept_certificate(cert.clone()).await {
            Ok(()) => continue,
            Err(e) => panic!("Unexpected error {e}"),
        }
    }

    // Create a certificate > 1000 rounds above the highest local round.
    let (_digest, cert) = mock_signed_certificate(
        keys.as_slice(),
        certificates.last().cloned().unwrap().origin(),
        2000,
        next_parents,
        &committee,
    );
    // The certificate should not be accepted or suspended.
    match synchronizer.try_accept_certificate(cert.clone()).await {
        Ok(()) => panic!("Unexpected success!"),
        Err(DagError::TooNew(_, _, _)) => {}
        Err(e) => panic!("Unexpected error {e}!"),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn synchronizer_recover_basic() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let client = PrimaryClient::new_from_keypair(&primary.primary_network_keypair());
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let name = primary.id();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

    let cb = ConsensusBus::new();
    // Make Synchronizer.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let own_address = committee.primary_by_id(&name).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();
    client.set_primary_network(network.clone());

    // Send 3 certificates to Synchronizer.
    let certificates: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
    for cert in certificates.clone() {
        synchronizer.try_accept_certificate(cert).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shutdown Synchronizer.
    drop(synchronizer);

    // Restart Synchronizer.

    let mut m = HashMap::new();
    m.insert("source", "other");
    assert_eq!(
        cb.primary_metrics().node_metrics.certificates_processed.get_metric_with(&m).unwrap().get(),
        3
    );

    let cb = ConsensusBus::new();
    let mut rx_parents = cb.parents().subscribe();
    let _synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));
    client.set_primary_network(network.clone());

    // Ensure the Synchronizer sends the parent certificates to the proposer.

    // the recovery flow sends message that contains the parents
    let received = rx_parents.recv().await.unwrap();
    assert_eq!(received.1, 1);
    assert_eq!(received.0.len(), certificates.len());
    for c in &certificates {
        assert!(received.0.contains(c));
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = certificate_store.read(x.digest()).unwrap();
        assert_eq!(stored, Some(x.clone()));
    }

    // New metrics, they should be zeroed out.
    assert_eq!(
        cb.primary_metrics().node_metrics.certificates_processed.get_metric_with(&m).unwrap().get(),
        0
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn synchronizer_recover_partial_certs() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let client = PrimaryClient::new_from_keypair(&primary.primary_network_keypair());
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let name = primary.id();

    let cb = ConsensusBus::new();
    // Make a synchronizer.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let own_address = committee.primary_by_id(&name).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();
    client.set_primary_network(network.clone());

    // Send 1 certificate.
    let certificates: Vec<Certificate> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
    let last_cert = certificates.clone().into_iter().last().unwrap();
    synchronizer.try_accept_certificate(last_cert).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown Synchronizer.
    drop(synchronizer);

    // Restart Synchronizer.

    let cb = ConsensusBus::new();
    let mut rx_parents = cb.parents().subscribe();
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));
    client.set_primary_network(network.clone());

    // Send remaining 2f certs.
    for cert in certificates.clone().into_iter().take(2) {
        synchronizer.try_accept_certificate(cert).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    for _ in 0..2 {
        let received = rx_parents.recv().await.unwrap();
        assert_eq!(received, (vec![], 0));
    }

    // the recovery flow sends message that contains the parents
    let received = rx_parents.recv().await.unwrap();
    assert_eq!(received.1, 1);
    assert_eq!(received.0.len(), certificates.len());
    for c in &certificates {
        assert!(received.0.contains(c));
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn synchronizer_recover_previous_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let client = PrimaryClient::new_from_keypair(&primary.primary_network_keypair());
    let network_key = primary.primary_network_keypair().copy().private().0.to_bytes();
    let name = primary.id();

    let cb = ConsensusBus::new();
    // Make a synchronizer.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let own_address = committee.primary_by_id(&name).unwrap().to_anemo_address().unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();
    client.set_primary_network(network.clone());

    // Send 3 certificates from round 1, and 2 certificates from round 2 to Synchronizer.
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys =
        fixture.authorities().map(|a| (a.id(), a.keypair().copy())).take(3).collect::<Vec<_>>();
    let (all_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=2, &genesis, &committee, &keys);
    let all_certificates: Vec<_> = all_certificates.into_iter().collect();
    let round_1_certificates = all_certificates[0..3].to_vec();
    let round_2_certificates = all_certificates[3..5].to_vec();
    for cert in round_1_certificates.iter().chain(round_2_certificates.iter()) {
        synchronizer.try_accept_certificate(cert.clone()).await.unwrap();
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown Synchronizer.
    drop(synchronizer);

    // Restart Synchronizer.

    let cb = ConsensusBus::new();
    let mut rx_parents = cb.parents().subscribe();
    let _synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));
    client.set_primary_network(network.clone());

    // the recovery flow sends message that contains the parents for the last round for which we
    // have a quorum of certificates, in this case is round 1.
    let received = rx_parents.recv().await.unwrap();
    assert_eq!(received.0.len(), round_1_certificates.len());
    assert_eq!(received.1, 1);
    for c in &round_1_certificates {
        assert!(received.0.contains(c));
    }
}

#[tokio::test]
async fn deliver_certificate_using_store() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let committee = fixture.committee();

    let certificates_store = primary.consensus_config().node_storage().certificate_store.clone();

    let cb = ConsensusBus::new();
    let synchronizer = Synchronizer::new(primary.consensus_config(), &cb);

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    let keys =
        fixture.authorities().map(|a| (a.id(), a.keypair().copy())).take(3).collect::<Vec<_>>();
    let (mut certificates, _next_parents) =
        make_optimal_signed_certificates(1..=4, &genesis, &committee, &keys);

    // insert the certificates in the DAG
    for certificate in certificates.clone() {
        certificates_store.write(certificate).unwrap();
    }

    // take the last one (top) and test for parents
    let test_certificate = certificates.pop_back().unwrap();

    // ensure that the certificate parents are found
    let parents_available =
        synchronizer.get_missing_parents(&test_certificate).await.unwrap().is_empty();
    assert!(parents_available);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn deliver_certificate_not_found_parents() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let committee = fixture.committee();

    let cb = ConsensusBus::new();
    let mut rx_certificate_fetcher = cb.certificate_fetcher().subscribe();
    let synchronizer = Synchronizer::new(primary.consensus_config(), &cb);

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    let keys = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect::<Vec<_>>();
    let (mut certificates, _next_parents) =
        make_optimal_signed_certificates(1..=4, &genesis, &committee, &keys);

    // take the last one (top) and test for parents
    let test_certificate = certificates.pop_back().unwrap();

    // we try to find the certificate's parents
    let parents_available =
        synchronizer.get_missing_parents(&test_certificate).await.unwrap().is_empty();

    // and we should fail
    assert!(!parents_available);

    let CertificateFetcherCommand::Ancestors(certificate) =
        rx_certificate_fetcher.recv().await.unwrap()
    else {
        panic!("Expected CertificateFetcherCommand::Ancestors");
    };

    // Be inactive would result in a kick signal from synchronizer to fetcher eventually.
    let CertificateFetcherCommand::Kick = rx_certificate_fetcher.recv().await.unwrap() else {
        panic!("Expected CertificateFetcherCommand::Kick");
    };

    assert_eq!(certificate, test_certificate);
}

#[tokio::test]
async fn sanitize_fetched_certificates() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let committee = fixture.committee();

    let cb = ConsensusBus::new();
    let synchronizer = Synchronizer::new(primary.consensus_config(), &cb);

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    let keys = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect::<Vec<_>>();
    let (verified_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=60, &genesis, &committee, &keys);

    const VERIFICATION_ROUND: u64 = 50;
    const LEAF_ROUND: u64 = 60;

    // Able to verify a batch of certificates with good signatures.
    synchronizer
        .sanitize_fetched_certificates(verified_certificates.iter().cloned().collect_vec())
        .await
        .unwrap();

    // Fail to verify a batch of certificates with good signatures for leaves,
    // but bad signatures at other rounds including the verification round.
    let mut certs = Vec::new();
    for cert in &verified_certificates {
        let r = cert.round();
        let mut cert = cert.clone();
        if r != LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsAggregateSignatureBytes::default(),
            ));
        }
        certs.push(cert);
    }
    synchronizer.sanitize_fetched_certificates(certs).await.unwrap_err();

    // Fail to verify a batch of certificates with bad signatures for leaves and the verification
    // round, but good signatures at other rounds.
    let mut certs = Vec::new();
    for cert in &verified_certificates {
        let r = cert.round();
        let mut cert = cert.clone();
        if r == VERIFICATION_ROUND || r == LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsAggregateSignatureBytes::default(),
            ));
        }
        certs.push(cert);
    }
    synchronizer.sanitize_fetched_certificates(certs).await.unwrap_err();

    // Able to verify a batch of certificates with good signatures for leaves and the verification
    // round, but bad signatures at other rounds.
    let mut certs = Vec::new();
    for cert in &verified_certificates {
        let r = cert.round();
        let mut cert = cert.clone();
        if r != VERIFICATION_ROUND && r != LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsAggregateSignatureBytes::default(),
            ));
        }
        certs.push(cert);
    }
    synchronizer.sanitize_fetched_certificates(certs).await.unwrap();

    // Able to verify a batch of certificates with good signatures, but leaves in more rounds.
    let mut certs = Vec::new();
    for cert in &verified_certificates {
        let r = cert.round();
        if r % 5 == 0 {
            continue;
        }
        certs.push(cert.clone());
    }
    synchronizer.sanitize_fetched_certificates(certs).await.unwrap();
}

#[tokio::test]
async fn sync_batches_drops_old() {
    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();

    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();
    let payload_store = primary.consensus_config().node_storage().payload_store.clone();

    let cb = ConsensusBus::new();
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let mut certificates = HashMap::new();
    for _ in 0..3 {
        let header = author
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
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
        .build()
        .unwrap();

    tokio::task::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = cb.consensus_round_updates().send(ConsensusRound::new(30, 0));
    });
    match synchronizer.sync_header_batches(&test_header, 10).await {
        Err(DagError::TooOld(_, _, _)) => (),
        result => panic!("unexpected result {result:?}"),
    }
}

#[tokio::test]
async fn gc_suspended_certificates() {
    const NUM_AUTHORITIES: usize = 4;
    const GC_DEPTH: Round = 5;

    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_AUTHORITIES).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    // Make 5 rounds of fake certificates.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());
    let certificates = certificates.into_iter().collect_vec();

    // Try to aceept certificates from round 2 and above. All of them should be suspended.
    let accept = FuturesUnordered::new();
    for cert in &certificates[NUM_AUTHORITIES..] {
        match synchronizer.try_accept_certificate(cert.clone()).await {
            Ok(()) => panic!("Unexpected acceptance of {cert:?}"),
            Err(DagError::Suspended(notify)) => {
                accept.push(async move { notify.wait().await });
                continue;
            }
            Err(e) => panic!("Unexpected error {e}"),
        }
    }
    // Round 2~5 certificates are suspended.
    // Round 1~4 certificates are missing and referenced as parents.
    assert_eq!(
        synchronizer.get_suspended_stats().await,
        (NUM_AUTHORITIES * 4, NUM_AUTHORITIES * 4)
    );

    // Re-insertion of missing certificate as fetched certificates should be suspended too.
    for (idx, cert) in certificates[NUM_AUTHORITIES * 2..NUM_AUTHORITIES * 4].iter().enumerate() {
        let mut verified_cert = cert.clone();
        // Simulate CertificateV2 fetched certificate leaf only verification
        if (idx + NUM_AUTHORITIES * 2) < NUM_AUTHORITIES * 3 {
            // Round 3 certs are parents of round 4 certs, so we mark them as verified indirectly
            verified_cert.set_signature_verification_state(
                SignatureVerificationState::VerifiedIndirectly(
                    verified_cert.aggregated_signature().expect("Invalid Signature").clone(),
                ),
            )
        } else {
            // Round 4 certs are leaf certs in this case and are verified directly
            verified_cert.set_signature_verification_state(
                SignatureVerificationState::VerifiedDirectly(
                    verified_cert.aggregated_signature().expect("Invalid Signature").clone(),
                ),
            )
        }
        match synchronizer.try_accept_fetched_certificate(verified_cert).await {
            Ok(()) => panic!("Unexpected acceptance of {cert:?}"),
            Err(DagError::Suspended(_)) => {
                continue;
            }
            Err(e) => panic!("Unexpected error {e}"),
        }
    }
    assert_eq!(
        synchronizer.get_suspended_stats().await,
        (NUM_AUTHORITIES * 4, NUM_AUTHORITIES * 4)
    );

    // At commit round 8, round 3 becomes the GC round.
    let _ = cb.consensus_round_updates().send(ConsensusRound::new(8, gc_round(8, GC_DEPTH)));

    // Wait for all notifications to arrive.
    accept.collect::<Vec<()>>().await;

    // Expected to receive:
    // Round 2~4 certificates will be accepted because of GC.
    // Round 5 certificates will be accepted because of no missing dependencies.
    let expected_certificates: HashMap<_, _> =
        certificates[NUM_AUTHORITIES..].iter().map(|cert| (cert.digest(), cert.clone())).collect();
    let mut received_certificates = HashMap::new();
    for _ in 0..expected_certificates.len() {
        let cert = rx_new_certificates.try_recv().unwrap();
        received_certificates.insert(cert.digest(), cert);
    }
    assert_eq!(expected_certificates, received_certificates);
    // Suspended and missing certificates are cleared.
    assert_eq!(synchronizer.get_suspended_stats().await, (0, 0));
}
