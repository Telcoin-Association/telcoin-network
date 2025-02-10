//! IT tests for the flow of certificates.
//!
//! Certificates are validated and sent to the [CertificateManager].
//! The [CertificateManager] tracks pending certificates and accepts certificates that are complete.

use super::{cert_manager::CertificateManager, cert_validator::CertificateValidator, AtomicRound};
use crate::{state_sync::CertificateManagerCommand, ConsensusBus};
use assert_matches::assert_matches;
use fastcrypto::{hash::Hash as _, traits::KeyPair};
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{make_optimal_signed_certificates, signed_cert_for_test, CommitteeFixture};
use tn_types::{
    error::CertificateError, BlsAggregateSignatureBytes, Certificate, CertificateDigest, Round,
    SignatureVerificationState, TaskManager, TnReceiver as _, TnSender,
};
use tokio::time::timeout;

struct TestTypes<DB = MemDatabase> {
    /// The CertificateValidator
    validator: CertificateValidator<DB>,
    /// The CertificateManager
    manager: CertificateManager<DB>,
    /// The consensus bus.
    cb: ConsensusBus,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
    /// The task manager.
    task_manager: TaskManager,
}

fn create_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();

    let config = primary.consensus_config();
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);
    let genesis: HashMap<CertificateDigest, Certificate> = Certificate::genesis(config.committee())
        .into_iter()
        .map(|cert| (cert.digest(), cert))
        .collect();

    // manager
    let manager = CertificateManager::new(
        config.clone(),
        cb.clone(),
        genesis.clone(),
        gc_round.clone(),
        highest_processed_round.clone(),
        highest_received_round.clone(),
    );

    // validator
    let validator = CertificateValidator::new(
        config,
        cb.clone(),
        genesis,
        gc_round,
        highest_processed_round,
        highest_received_round,
    );

    let task_manager = TaskManager::default();

    TestTypes { manager, validator, cb, fixture, task_manager }
}

#[tokio::test]
async fn test_accept_valid_certs() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager } = create_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    // receive parent updates (proposer)
    let mut rx_parents = cb.parents().subscribe();
    // receive new accepted certs (consensus)
    let mut rx_new_certificates = cb.new_certificates().subscribe();

    // create 3 certs
    // NOTE: test types uses the last authority
    let certs: Vec<_> = fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    // assert unverified certificates and process
    for cert in certs.clone() {
        assert!(!cert.is_verified());
        // try to accept
        validator.process_peer_certificate(cert).await?;
    }

    // assert proposer receives parents
    for _i in 0..3 {
        let received = rx_parents.recv().await.unwrap();
        assert_eq!(received, (vec![], 0));
    }

    // enough parents accepted should trigger cert manager to forward to proposer
    let received = rx_parents.recv().await.unwrap();
    assert_eq!(received, (certs.clone(), 1));

    // assert consensus receives accepted certs
    for cert in &certs {
        let received = rx_new_certificates.recv().await.unwrap();
        assert_eq!(&received, cert);
    }

    // assert certs were stored
    for cert in &certs {
        let stored = certificate_store.read(cert.digest()).unwrap();
        assert_eq!(stored, Some(cert.clone()));
    }

    Ok(())
}

#[tokio::test]
async fn test_accept_pending_certs() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager } = create_test_types();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    // try to process certs for rounds 2..5 before round 1
    // assert pending
    for cert in later_rounds {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertificateError::Pending(digest)) if digest == expected);
    }

    // assert no certs accepted
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    assert!(rx_new_certificates.try_recv().is_err()); // empty channel

    // process round 1
    for cert in first_round {
        let res = validator.process_peer_certificate(cert).await;
        assert_matches!(res, Ok(()));
    }

    // assert all certs accepted in causal order
    let mut causal_round = 0;
    for _ in &all_certificates {
        let received = rx_new_certificates.try_recv().expect("new cert");
        // cert rounds should only accend
        let cert_round = received.round();
        if cert_round > causal_round {
            causal_round = cert_round;
        }
        assert!(cert_round == causal_round);
    }

    // create a certificate too far in the future
    let wrong_round = 2000;
    let (digest, cert) = signed_cert_for_test(
        keys.as_slice(),
        all_certificates.iter().last().cloned().unwrap().origin(),
        wrong_round,
        next_parents,
        &committee,
    );

    // try to accept
    let err = validator.process_peer_certificate(cert).await;
    assert_matches!(err, Err(CertificateError::TooNew(d, wrong, correct))
        if d == digest && wrong == wrong_round && correct == 5);

    Ok(())
}

#[tokio::test]
async fn test_recover_basic() -> eyre::Result<()> {
    let TestTypes { validator, manager, fixture, task_manager, .. } = create_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    // create 3 certs
    // NOTE: test types uses the last authority
    let mut certs: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    for cert in certs.clone() {
        validator.process_peer_certificate(cert).await?;
    }

    // wait for certs to storage
    let last_digest = certs.last().expect("last cert").digest();
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    // crash
    task_manager.abort();

    // from create_test_types()
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);
    let genesis: HashMap<CertificateDigest, Certificate> =
        Certificate::genesis(primary.consensus_config().committee())
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();
    let new_cb = ConsensusBus::new();
    let manager = CertificateManager::new(
        primary.consensus_config().clone(),
        new_cb.clone(),
        genesis.clone(),
        gc_round.clone(),
        highest_processed_round.clone(),
        highest_received_round.clone(),
    );

    task_manager.spawn_task("recovered manager", manager.run());

    // assert proposer receives parents for round after recovery
    let mut rx_parents = new_cb.parents().subscribe();
    let (mut received_certs, round) = rx_parents.recv().await.unwrap();

    fn sort_by_digest(a: &Certificate, b: &Certificate) -> core::cmp::Ordering {
        let a = a.digest();
        let b = b.digest();
        a.cmp(&b)
    }

    // sort certs to ensure consistent order
    let received = received_certs.sort_by(|a, b| sort_by_digest(a, b));
    let expected = certs.sort_by(|a, b| sort_by_digest(a, b));
    assert_eq!(received, expected);
    assert_eq!(round, 1);

    Ok(())
}

#[tokio::test]
async fn test_recover_partial_certs() -> eyre::Result<()> {
    todo!()
}
