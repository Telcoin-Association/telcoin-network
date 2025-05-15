//! Tests for the cert manager.

use super::CertificateManager;
use crate::{error::CertManagerError, state_sync::AtomicRound, ConsensusBus};
use assert_matches::assert_matches;
use std::collections::BTreeSet;
use tn_primary::test_utils::make_optimal_signed_certificates;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{Certificate, Hash as _, SignatureVerificationState};

struct TestTypes<DB = MemDatabase> {
    /// The CertificateManager
    manager: CertificateManager<DB>,
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

    let manager = CertificateManager::new(config, cb, gc_round, highest_processed_round);

    TestTypes { manager, fixture }
}

#[tokio::test]
async fn test_unverified_certificate_fails() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();

    let unverified = fixture.unverified_cert_from_last_authority();
    assert!(manager.process_verified_certificates(vec![unverified]).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_accept_pending_certs() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();
    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // all certs
    let certs: Vec<_> = certificates
        .into_iter()
        .map(|mut c| {
            c.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                c.aggregated_signature().expect("signature valid"),
            ));
            c
        })
        .collect();

    // separate first round (4 certs) and later rounds
    let mut first_round = certs; // for readability
    let later_rounds = first_round.split_off(num_authorities);
    let expected_pending_len = later_rounds.len();

    // try to process certs - all should be pending
    let expected_last_digest = later_rounds.last().expect("at least one cert").digest();
    let res = manager.process_verified_certificates(later_rounds).await;

    // expect all certs to process and error to reference last digest processed
    assert_matches!(res, Err(CertManagerError::Pending(digest)) if digest == expected_last_digest);

    // later_rounds should be pending
    assert_eq!(expected_pending_len, manager.pending.num_pending());
    Ok(())
}
