//! IT tests for the flow of certificates.
//!
//! Certificates are validated and sent to the [CertificateManager].
//! The [CertificateManager] tracks pending certificates and accepts certificates that are complete.

use super::{cert_manager::CertificateManager, cert_validator::CertificateValidator, AtomicRound};
use crate::{
    consensus::{gc_round, ConsensusRound},
    error::CertManagerError,
    state_sync::HeaderValidator,
    ConsensusBus,
};
use assert_matches::assert_matches;
use std::{collections::BTreeSet, time::Duration};
use tn_storage::{mem_db::MemDatabase, CertificateStore};
use tn_test_utils::{
    make_optimal_signed_certificates, signed_cert_for_test, AuthorityFixture, CommitteeFixture,
};
use tn_types::{
    error::CertificateError, Certificate, CertificateDigest, Database, Hash as _, Round,
    TaskManager, TnReceiver as _, TnSender,
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

fn create_all_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().last().unwrap();
    let (manager, validator, cb) = create_core_test_types(primary);
    let task_manager = TaskManager::default();

    TestTypes { manager, validator, cb, fixture, task_manager }
}

// reused in other tests
fn create_core_test_types<DB: Database>(
    primary: &AuthorityFixture<DB>,
) -> (CertificateManager<DB>, CertificateValidator<DB>, ConsensusBus) {
    let cb = ConsensusBus::new();
    let config = primary.consensus_config();
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);

    // manager
    let manager = CertificateManager::new(
        config.clone(),
        cb.clone(),
        gc_round.clone(),
        highest_processed_round.clone(),
        highest_received_round.clone(),
    );

    // validator
    let validator = CertificateValidator::new(
        config.clone(),
        cb.clone(),
        gc_round.clone(),
        highest_processed_round,
        highest_received_round,
    );

    (manager, validator, cb)
}

/// Helper to sort certificates by digest
fn sort_by_digest(a: &CertificateDigest, b: &CertificateDigest) -> core::cmp::Ordering {
    a.cmp(b)
}

#[tokio::test]
async fn test_accept_valid_certs() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

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
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();

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
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
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
        all_certificates.iter().last().cloned().unwrap().origin().clone(),
        wrong_round,
        next_parents,
        &committee,
    );

    // try to accept
    let err = validator.process_peer_certificate(cert).await;
    assert_matches!(err, Err(CertManagerError::Certificate(
        CertificateError::TooNew(d, wrong, correct))
    ) if d == digest && wrong == wrong_round && correct == 5);

    Ok(())
}

#[tokio::test]
async fn test_gc_pending_certs() -> eyre::Result<()> {
    const GC_DEPTH: Round = 5;

    // create test types
    let TestTypes { validator, manager, cb, fixture, task_manager } = create_all_test_types();

    // cert store
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make 5 rounds of certificates
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    // try to process certs for rounds 2..5 (before round 1)
    // assert pending
    for cert in later_rounds.clone() {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
    }

    // assert no certs accepted
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    assert!(rx_new_certificates.try_recv().is_err()); // empty channel

    // reinsert later rounds as if fetched from peers
    // and assert still pending
    let last_digest = later_rounds.back().expect("last certificate").digest();
    let err = validator.process_fetched_certificates_in_parallel(later_rounds.clone().into()).await;
    assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == last_digest);

    // update consensus rounds
    // commit at round 8, so round 3 becomes the GC round
    let commit_round = 8;
    cb.update_consensus_rounds(ConsensusRound::new(
        commit_round,
        gc_round(commit_round, GC_DEPTH),
    ))?;

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    // assert all certs accepted in causal order
    let mut causal_round = 0;
    for _ in &later_rounds {
        let received = rx_new_certificates.try_recv().expect("new cert");
        // cert rounds should only accend
        let cert_round = received.round();
        if cert_round > causal_round {
            causal_round = cert_round;
        }
        assert!(cert_round == causal_round);
    }

    Ok(())
}

#[tokio::test]
async fn test_node_restart_syncs_state() -> eyre::Result<()> {
    let TestTypes { validator, manager, fixture, task_manager, .. } = create_all_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    // create 3 certs
    // NOTE: test types uses the last authority
    let mut certs: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let last_cert = certs.last().cloned().expect("last certificate");
    let last_digest = last_cert.digest();

    // process 1 certificate
    validator.process_peer_certificate(last_cert).await?;

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    // crash
    task_manager.abort();

    //
    // recover from crash and submit last two certs
    // this should not forward to the proposer on startup because quorum wasn't reached
    // so the round hasn't advanced
    //

    let (manager_first_recovery, validator_first_recovery, cb_first_recovery) =
        create_core_test_types(primary);

    task_manager.spawn_task("recovered manager", manager_first_recovery.run());

    // assert proposer receives parents for round after recovery
    let mut rx_parents_first_recovery = cb_first_recovery.parents().subscribe();

    // proposer should not receive parents because quorum wasn't reached
    assert!(rx_parents_first_recovery.try_recv().is_err());

    // send remaining 2 certs to reach quorum
    let mut last_digest = CertificateDigest::default();
    for cert in certs.clone().into_iter().take(2) {
        last_digest = cert.digest();
        validator_first_recovery.process_peer_certificate(cert).await.unwrap();
    }

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    //crash
    task_manager.abort();

    //
    // recover - this should forward an update to the proposer because enough certs were
    // reached to advance the round before crash
    //

    let (manager_second_recovery, _validator, cb_second_recovery) = create_core_test_types(primary);

    task_manager.spawn_task("recovered manager", manager_second_recovery.run());

    // assert proposer receives parents for round after recovery
    let mut rx_parents_second_recovery = cb_second_recovery.parents().subscribe();

    let (mut received_certs, round) = rx_parents_second_recovery.recv().await.unwrap();

    // sort certs to ensure consistent order
    received_certs.sort_by(|a, b| {
        let a = a.digest();
        let b = b.digest();
        sort_by_digest(&a, &b)
    });
    certs.sort_by(|a, b| {
        let a = a.digest();
        let b = b.digest();
        sort_by_digest(&a, &b)
    });
    assert_eq!(round, 1);

    Ok(())
}

#[tokio::test]
async fn test_filter_unknown_parents() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();

    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();

    // spawn manager task
    task_manager.spawn_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    let header_validator = HeaderValidator::new(primary.consensus_config(), cb.clone());

    // assert all unknown
    let round5_cert = later_rounds.back().expect("header for round 2");
    // only round 4 should be unknown
    let mut expected: Vec<_> = all_certificates
        .iter()
        .filter_map(|c| if c.header().round() == 4 { Some(c.digest()) } else { None })
        .collect();
    expected.sort_by(sort_by_digest);
    // report unknown
    let mut unknown = header_validator.identify_unkown_parents(round5_cert.header()).await?;
    unknown.sort_by(sort_by_digest);

    assert_eq!(expected, unknown);

    // try to process certs for rounds 2..5 before round 1
    // assert pending
    for cert in later_rounds.clone() {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
    }

    // round 4 should no longer be "missing"
    let unknown = header_validator.identify_unkown_parents(round5_cert.header()).await?;
    assert!(unknown.is_empty());

    // assert pending aren't unknown
    let round2_cert = later_rounds.front().expect("header for round 2");
    let mut unknown = header_validator.identify_unkown_parents(round2_cert.header()).await?;
    unknown.sort_by(sort_by_digest);
    let mut expected: Vec<_> = first_round.iter().map(|c| c.digest()).collect();
    expected.sort_by(sort_by_digest);
    assert_eq!(expected, unknown);

    Ok(())
}
