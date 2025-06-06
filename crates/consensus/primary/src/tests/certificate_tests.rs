//! Certificate tests

use rand::{rngs::StdRng, SeedableRng};
use std::num::NonZeroUsize;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{AuthorityIdentifier, BlsKeypair, Certificate, SignatureVerificationState, Vote};

#[test]
fn test_empty_certificate_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    let votes =
        fixture.authorities().take(3).map(|a| (a.id(), *a.vote(&header).signature())).collect();

    let certificate =
        Certificate::new_unsigned_for_test(&committee, header, votes).expect("new unsigned cert");
    assert!(certificate.verify(&committee, &fixture.worker_cache()).is_err());
}

#[test]
fn test_valid_certificate_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    for authority in fixture.authorities().take(3) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let certificate = Certificate::new_unverified(&committee, header, signatures).unwrap();
    let verified_certificate = certificate.verify(&committee, &fixture.worker_cache());

    assert!(verified_certificate.is_ok());
    assert!(matches!(
        verified_certificate.unwrap().signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[test]
fn test_certificate_insufficient_signatures() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 2 Signatures. This is less than 2F + 1 (3).
    for authority in fixture.authorities().take(2) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    assert!(Certificate::new_unverified(&committee, header.clone(), signatures.clone()).is_err());

    let certificate = Certificate::new_unsigned_for_test(&committee, header, signatures).unwrap();

    assert!(certificate.verify(&committee, &fixture.worker_cache()).is_err());
}

#[test]
fn test_certificate_validly_repeated_public_keys() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    for authority in fixture.authorities().take(3) {
        let vote = authority.vote(&header);
        // We double every (pk, signature) pair - these should be ignored when forming the
        // certificate.
        signatures.push((vote.author().clone(), *vote.signature()));
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let certificate_res = Certificate::new_unverified(&committee, header, signatures);
    assert!(certificate_res.is_ok());
    let certificate = certificate_res.unwrap();

    assert!(certificate.verify(&committee, &fixture.worker_cache()).is_ok());
}

#[test]
fn test_unknown_signature_in_certificate() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 2 Signatures. This is less than 2F + 1 (3).
    for authority in fixture.authorities().take(2) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let malicious_key = BlsKeypair::generate(&mut StdRng::from_os_rng());
    let malicious_id: AuthorityIdentifier = AuthorityIdentifier::dummy_for_test(50u8);

    let vote = Vote::new_with_signer(&header, malicious_id, &malicious_key);
    signatures.push((vote.author().clone(), *vote.signature()));

    assert!(Certificate::new_unverified(&committee, header, signatures).is_err());
}

proptest::proptest! {
    #[test]
    fn test_certificate_verification(
        committee_size in 4..35_usize
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();
        let header = fixture.header_from_last_authority();

        let mut signatures = Vec::new();

        let quorum_threshold = committee.quorum_threshold() as usize;

        for authority in fixture.authorities().take(quorum_threshold) {
            let vote = authority.vote(&header);
            signatures.push((vote.author().clone(), *vote.signature()));
        }

        let certificate = Certificate::new_unverified(&committee, header, signatures).unwrap();

        assert!(certificate
            .verify(&committee, &fixture.worker_cache())
            .is_ok());
    }
}
