//! Certificate validator tests

use super::CertificateValidator;
use crate::{
    state_sync::{AtomicRound, CertificateManagerCommand},
    ConsensusBus,
};
use std::collections::BTreeSet;
use tn_primary::test_utils::make_optimal_signed_certificates;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    BlsSignature, Certificate, Hash as _, Round, SignatureVerificationState, TnReceiver as _,
    TnSender,
};

struct TestTypes<DB = MemDatabase> {
    /// The CertificateValidator
    validator: CertificateValidator<DB>,
    /// The consensus bus.
    cb: ConsensusBus,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
}

fn create_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();

    // for validator
    let config = primary.consensus_config();
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);

    let validator = CertificateValidator::new(
        config,
        cb.clone(),
        gc_round,
        highest_processed_round,
        highest_received_round,
    );

    TestTypes { validator, cb, fixture }
}

#[tokio::test]
async fn test_certificates_verified() -> eyre::Result<()> {
    let TestTypes { validator, cb, fixture } = create_test_types();

    // receive parent updates
    let mut parents_rx = cb.parents().subscribe();
    // receive verified certificates
    let mut certificate_manager_rx = cb.certificate_manager().subscribe();

    // create 3 certs
    // NOTE: test types uses the last authority
    let certs: Vec<_> = fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();
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
    for cert in certs {
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
    let TestTypes { validator, cb, fixture } = create_test_types();

    // receive verified certificates
    let mut certificate_manager_rx = cb.certificate_manager().subscribe();
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
