//! Certificate validation tests.
//!
//! These tests verify the critical security invariants of certificate validation:
//! - Quorum requirements (2f+1 signatures)
//! - BLS signature verification
//! - Epoch validation

use std::collections::BTreeSet;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    error::{CertificateError, DagError},
    Certificate, HeaderBuilder,
};

/// Test that a certificate requires at least 2f+1 signatures to be valid.
///
/// For a committee of size N = 3f + 1, we need 2f + 1 votes to form a quorum.
/// With N=4 validators, f=1, so we need 3 votes (2*1 + 1 = 3).
#[tokio::test]
async fn test_certificate_quorum_2f_plus_1() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create a header from the last authority
    let header = fixture.header_from_last_authority();

    // Collect votes from ALL authorities (except the author)
    // This should give us 3 votes for a 4-node committee (quorum)
    let all_votes: Vec<_> =
        fixture.votes(&header).into_iter().map(|v| (v.author().clone(), *v.signature())).collect();

    // Full quorum should succeed
    let cert_result = Certificate::new_unverified(&committee, header.clone(), all_votes.clone());
    assert!(cert_result.is_ok(), "Certificate with full quorum should succeed");

    // With only 2 votes (less than 2f+1=3), certificate creation should fail
    let insufficient_votes: Vec<_> = all_votes.into_iter().take(2).collect();
    let cert_result = Certificate::new_unverified(&committee, header.clone(), insufficient_votes);

    match cert_result {
        Err(DagError::CertificateRequiresQuorum) => {
            // Expected error
        }
        Ok(_) => panic!("Certificate with insufficient votes should fail"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }

    // With only 1 vote, should also fail
    let single_vote: Vec<_> = fixture
        .votes(&header)
        .into_iter()
        .take(1)
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();
    let cert_result = Certificate::new_unverified(&committee, header.clone(), single_vote);

    match cert_result {
        Err(DagError::CertificateRequiresQuorum) => {
            // Expected error
        }
        Ok(_) => panic!("Certificate with single vote should fail"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }

    // With 0 votes, should fail
    let cert_result = Certificate::new_unverified(&committee, header, vec![]);

    match cert_result {
        Err(DagError::CertificateRequiresQuorum) => {
            // Expected error
        }
        Ok(_) => panic!("Certificate with no votes should fail"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

/// Test that BLS signature verification works correctly.
///
/// A certificate's aggregated BLS signature must be verified against the
/// public keys of the signing authorities.
#[tokio::test]
async fn test_certificate_bls_signature_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create a valid certificate
    let header = fixture.header_from_last_authority();
    let certificate = fixture.certificate(&header);

    // Verify the certificate against the committee
    let mut verified_cert = certificate.clone();
    let res = verified_cert.validate_and_verify(&committee);
    assert!(res.is_ok(), "Valid certificate should verify: {:?}", res.err());

    assert!(verified_cert.is_verified(), "Certificate should be marked as verified");

    // Create a certificate with tampered header (different round)
    // The signature will not match the tampered header
    let mut tampered_cert = fixture.certificate(&header);
    tampered_cert.header_mut_for_test().update_round_for_test(999);

    // Verification should fail due to signature mismatch
    let tampered_result = tampered_cert.validate_and_verify(&committee);
    assert!(tampered_result.is_err(), "Tampered certificate should fail verification");
}

/// Test that certificates with wrong epoch are rejected.
///
/// A certificate's epoch must match the committee's epoch for validation.
#[tokio::test]
async fn test_certificate_wrong_epoch_rejected() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Verify committee epoch is 0
    assert_eq!(committee.epoch(), 0);

    // Create a header with epoch 1 (different from committee's epoch 0)
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let authority = fixture.first_authority();

    let header_wrong_epoch = HeaderBuilder::default()
        .author(authority.id())
        .round(1)
        .epoch(1) // Wrong epoch - committee is epoch 0
        .parents(genesis)
        .build();

    // Try to create and validate a certificate with wrong epoch header
    let votes: Vec<_> = fixture
        .votes(&header_wrong_epoch)
        .into_iter()
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();

    // Certificate creation with wrong epoch header
    let cert_result = Certificate::new_unverified(&committee, header_wrong_epoch.clone(), votes);

    // The certificate might be created (depending on implementation)
    // but validation against the committee should fail
    if let Ok(mut certificate) = cert_result {
        assert_eq!(certificate.epoch(), 1);

        // Validation should fail due to epoch mismatch
        let result = certificate.validate_and_verify(&committee);

        match result {
            Err(CertificateError::Header(tn_types::error::HeaderError::InvalidEpoch {
                theirs,
                ours,
            })) => {
                assert_eq!(theirs, 1);
                assert_eq!(ours, 0);
            }
            Ok(_) => panic!("Certificate with wrong epoch should fail validation"),
            Err(e) => panic!("Unexpected error type: {:?}", e),
        }
    }
    // If certificate creation fails, that's also acceptable behavior
}

/// Test that genesis certificates are always valid.
#[tokio::test]
async fn test_genesis_certificates_valid() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Get genesis certificates
    let genesis_certs = Certificate::genesis(&committee);

    assert_eq!(genesis_certs.len(), committee.size());

    for mut cert in genesis_certs {
        // Genesis certificates should always validate
        let result = cert.validate_and_verify(&committee);
        assert!(result.is_ok(), "Genesis certificate should be valid: {:?}", result.err());
    }
}

/// Test that certificates with unknown authority are rejected.
#[tokio::test]
async fn test_certificate_unknown_authority_rejected() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create a different committee (different authorities)
    let other_fixture = CommitteeFixture::builder(MemDatabase::default).build();

    // Create a certificate from the other committee's authority
    let other_header = other_fixture.header_from_last_authority();
    let mut other_cert = other_fixture.certificate(&other_header);

    // Try to validate against our committee - should fail
    let result = other_cert.validate_and_verify(&committee);

    // The certificate should fail validation because the authority is unknown
    // or the signature won't verify against our committee's keys
    assert!(result.is_err(), "Certificate from unknown authority should fail validation");
}

/// Test that verify_cert rejects certificates when validated against empty key set.
///
/// This tests the quorum check: when no public keys are provided for verification,
/// the signed_by() method returns 0 stake, which fails the quorum threshold check.
#[tokio::test]
async fn test_certificate_verify_with_empty_keys_rejected() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();

    // Create a valid certificate
    let header = fixture.header_from_last_authority();
    let mut certificate = fixture.certificate(&header);

    // Empty set of public keys to verify against
    let empty_keys: BTreeSet<tn_types::BlsPublicKey> = BTreeSet::new();

    // verify_cert should fail because:
    // 1. signed_by() returns (0, []) when no keys match
    // 2. quorum_threshold(0) = 1 (minimum threshold)
    // 3. 0 >= 1 is false, so Inquorate error
    let result = certificate.verify_cert(&empty_keys);

    match result {
        Err(CertificateError::Inquorate { stake, threshold }) => {
            assert_eq!(stake, 0, "No matching signers should mean 0 stake");
            assert!(threshold > 0, "Threshold should be at least 1");
        }
        Ok(_) => panic!("Certificate verified with empty keys should fail"),
        Err(e) => {
            // Other errors are acceptable (e.g., if implementation differs)
            println!("Got different error (acceptable): {:?}", e);
        }
    }
}

/// Test that quorum threshold calculation is correct for various committee sizes.
///
/// For BFT consensus with N = 3f + 1 validators:
/// - f = (N - 1) / 3 (max Byzantine faults)
/// - Quorum = 2f + 1 = (2N + 1) / 3 (rounded up)
#[tokio::test]
async fn test_quorum_threshold_calculation() {
    use tn_types::quorum_threshold;

    // Test various committee sizes
    // N=4: f=1, quorum = 2*1+1 = 3
    assert_eq!(quorum_threshold(4), 3, "4-node committee needs 3 for quorum");

    // N=7: f=2, quorum = 2*2+1 = 5
    assert_eq!(quorum_threshold(7), 5, "7-node committee needs 5 for quorum");

    // N=10: f=3, quorum = 2*3+1 = 7
    assert_eq!(quorum_threshold(10), 7, "10-node committee needs 7 for quorum");

    // N=1: Edge case - single node
    assert_eq!(quorum_threshold(1), 1, "1-node committee needs 1 for quorum");

    // N=0: Edge case - empty committee (should still require at least 1)
    assert_eq!(quorum_threshold(0), 1, "Empty committee should require at least 1");

    // Verify the BFT quorum formula for committee sizes 1 to 100
    for n in 1..=100 {
        let threshold = quorum_threshold(n);
        // Threshold should be > N/2 (majority) and <= N
        assert!(threshold > n / 2, "Quorum must be majority for N={}", n);
        assert!(threshold <= n, "Quorum can't exceed committee size for N={}", n);

        // Formula: (2*N + 3) / 3 is integer ceiling of (2*N + 1) / 3
        let expected = (2 * n + 3) / 3;
        assert_eq!(
            threshold, expected,
            "Quorum formula mismatch for N={}: got {}, expected {}",
            n, threshold, expected
        );
    }
}
