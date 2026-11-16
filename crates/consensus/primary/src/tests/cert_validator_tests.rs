//! Certificate validator tests

use super::{verify_stored_certificate, CertificateValidator};
use crate::{state_sync::CertificateManagerCommand, ConsensusBus};
use assert_matches::assert_matches;
use std::collections::BTreeSet;
use tn_primary::test_utils::make_optimal_signed_certificates;
use tn_storage::{mem_db::MemDatabase, CertificateStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    BlsSignature, Certificate, Hash as _, Round, SignatureVerificationState, TaskManager,
    TnReceiver as _,
};

struct TestTypes<DB = MemDatabase> {
    /// The CertificateValidator
    validator: CertificateValidator<DB>,
    /// The consensus bus.
    cb: ConsensusBus,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
    /// Task manager controlling spawned tasks.
    task_manager: TaskManager,
}

fn create_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();

    // for validator
    let config = primary.consensus_config();

    let task_manager = TaskManager::default();
    let validator = CertificateValidator::new(config, cb.clone(), task_manager.get_spawner());

    TestTypes { validator, cb, fixture, task_manager }
}

#[tokio::test]
async fn test_certificates_verified() -> eyre::Result<()> {
    let TestTypes { validator, cb, fixture, task_manager: _task_manager } = create_test_types();

    // receive parent updates
    let mut parents_rx = cb.subscribe_parents();
    // receive verified certificates
    let mut certificate_manager_rx = cb.subscribe_certificate_manager();

    // create 3 certs
    // NOTE: test types uses the last authority
    let mut certs: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
    let cloned_certs = certs.clone();

    // spawn task to receive processed certificates
    let cert_manager = tokio::task::spawn(async move {
        // ensure certs are verified and sent to certificate manager
        for cert in cloned_certs {
            // receive cert
            let command = certificate_manager_rx.recv().await.unwrap();
            let received = match command {
                CertificateManagerCommand::ProcessVerifiedCertificates { certificates, .. } => {
                    certificates
                }
                other => {
                    panic!("unexpected command: {other:?}");
                }
            };
            assert_eq!(*received, vec![cert]);
            assert!(received[0].is_verified());
        }
    });

    // assert unverified certificates and process
    for cert in certs.iter_mut() {
        assert!(!cert.is_verified());
        // try to accept - ignore err for dropped oneshot
        let _ = validator.process_peer_certificate(cert).await;
    }

    // assert proposer receives parents
    for _i in 0..3 {
        let received = parents_rx.recv().await.unwrap();
        assert_eq!(received, (vec![], 0));
    }

    assert!(cert_manager.await.is_ok());

    Ok(())
}

#[tokio::test]
async fn test_process_fetched_certificates_in_parallel() -> eyre::Result<()> {
    let TestTypes { validator, cb, fixture, task_manager: _task_manager } = create_test_types();

    // receive verified certificates
    let mut certificate_manager_rx = cb.subscribe_certificate_manager();
    let committee = fixture.committee();

    // NOTE: test types uses the last authority
    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>();

    let keys = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect::<Vec<_>>();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=60, &genesis, &committee, &keys);

    let unverified_certificates = certificates.into_iter().collect::<Vec<_>>();

    const VERIFICATION_ROUND: Round = 50;
    const LEAF_ROUND: Round = 60;

    let _task = tokio::task::spawn(async move {
        loop {
            // receive cert
            let command = certificate_manager_rx.recv().await.unwrap();
            let received = match command {
                CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply } => {
                    // return ok
                    let _ = reply.send(Ok(()));
                    certificates
                }
                other => {
                    panic!("unexpected command: {other:?}");
                }
            };

            // ensure verified
            for cert in received {
                assert!(cert.is_verified());
            }
        }
    });

    // assert unverified certificates and process
    for cert in &unverified_certificates {
        assert!(!cert.is_verified());
    }

    // test success
    assert!(validator
        .process_fetched_certificates_in_parallel(unverified_certificates.clone())
        .await
        .is_ok());

    // Fail to verify a batch of certificates with good signatures for leaves,
    // but bad signatures at other rounds including the verification round.
    let mut certs = Vec::new();
    for cert in &unverified_certificates {
        let mut cert = cert.clone();
        if cert.round() != LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsSignature::default(),
            ));
        }
        certs.push(cert);
    }

    // expect error
    assert!(validator.process_fetched_certificates_in_parallel(certs).await.is_err());

    // fail to verify a batch of certificates with bad signatures for leaves and the verification
    // round, but good signatures at other rounds.
    let mut certs = Vec::new();
    for cert in &unverified_certificates {
        let round = cert.round();
        let mut cert = cert.clone();
        if round == VERIFICATION_ROUND || round == LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsSignature::default(),
            ));
        }
        certs.push(cert);
    }

    // expect error
    assert!(validator.process_fetched_certificates_in_parallel(certs).await.is_err());

    // Able to verify a batch of certificates with good signatures for leaves and the verification
    // round, but bad signatures at other rounds.
    let mut certs = Vec::new();
    for cert in &unverified_certificates {
        let r = cert.round();
        let mut cert = cert.clone();
        if r != VERIFICATION_ROUND && r != LEAF_ROUND {
            cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                BlsSignature::default(),
            ));
        }
        certs.push(cert);
    }

    // expect ok
    assert!(validator.process_fetched_certificates_in_parallel(certs).await.is_ok());

    // Able to verify a batch of certificates with good signatures, but leaves in more rounds.
    let mut certs = Vec::new();
    for cert in &unverified_certificates {
        let r = cert.round();
        if r % 5 == 0 {
            continue;
        }
        certs.push(cert.clone());
    }

    // expect ok
    assert!(validator.process_fetched_certificates_in_parallel(certs).await.is_ok());

    Ok(())
}

/// A certificate as a fetch stores it when another certificate in the response names it as a
/// parent: `VerifiedIndirectly`, with the bytes it arrived with as its aggregate signature (the
/// default signature when `corrupt`). Returns that certificate, as written to a fresh store, its
/// valid signature, the store, and the committee.
fn stored_indirect(corrupt: bool) -> (Certificate, BlsSignature, MemDatabase, tn_types::Committee) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let mut certificate = fixture.certificate(&fixture.headers()[0]);
    let valid = certificate.aggregated_signature().expect("fixture certificates are signed");
    let signature = if corrupt { BlsSignature::default() } else { valid };
    certificate.set_signature_verification_state(SignatureVerificationState::VerifiedIndirectly(
        signature,
    ));
    let store = MemDatabase::default();
    store.write(certificate.clone()).expect("write the indirect certificate");
    (certificate, valid, store, committee)
}

/// Read `certificate` back from `store`, as a restarted node reads it.
fn reread(store: &MemDatabase, certificate: &Certificate) -> Certificate {
    store.read(certificate.digest()).expect("store readable").expect("certificate stays stored")
}

/// A stored `VerifiedIndirectly` certificate with a valid signature is verified once: the check is
/// stored with it, and a stored `VerifiedDirectly` copy is returned without a second check.
#[tokio::test]
async fn test_verify_stored_indirect_certificate_once() -> eyre::Result<()> {
    let (certificate, signature, store, committee) = stored_indirect(false);

    let verified = verify_stored_certificate(&store, &committee, reread(&store, &certificate))
        .await?
        .expect("a valid signature verifies");
    assert_matches!(
        verified.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == signature
    );
    let stored = reread(&store, &certificate);
    assert_matches!(
        stored.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == signature,
        "the check is written back with the certificate"
    );

    // a copy in the stored `VerifiedDirectly` state comes back as it is: were it checked again,
    // these bytes would fail
    let mut unchecked = stored;
    unchecked.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    let returned = verify_stored_certificate(&store, &committee, unchecked)
        .await?
        .expect("a directly verified copy is returned without a check");
    assert_matches!(
        returned.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == BlsSignature::default()
    );

    Ok(())
}

/// A stored `VerifiedIndirectly` certificate whose signature does not verify is withheld, stays
/// stored, and the failure is stored with it: read back again, as after a restart, it is withheld
/// without a second check.
#[tokio::test]
async fn test_verify_stored_indirect_certificate_with_bad_signature() -> eyre::Result<()> {
    let (certificate, valid, store, committee) = stored_indirect(true);

    assert!(
        verify_stored_certificate(&store, &committee, reread(&store, &certificate))
            .await?
            .is_none(),
        "a signature that does not verify is withheld"
    );
    let stored = reread(&store, &certificate);
    assert_matches!(
        stored.signature_verification_state(),
        SignatureVerificationState::Unsigned(sig) if *sig == BlsSignature::default(),
        "the failed check is written back with the certificate"
    );
    assert!(
        verify_stored_certificate(&store, &committee, stored.clone()).await?.is_none(),
        "the copy read back after a restart is withheld"
    );

    // the stored failure is not checked again: even valid bytes in that state are withheld
    let mut failed = stored;
    failed.set_signature_verification_state(SignatureVerificationState::Unsigned(valid));
    assert!(verify_stored_certificate(&store, &committee, failed).await?.is_none());

    Ok(())
}
