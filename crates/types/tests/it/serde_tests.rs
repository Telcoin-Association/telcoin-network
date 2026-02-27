//! Serialization/deserialization tests for types.
//!
//! These tests verify that all types correctly round-trip
//! through serialization and deserialization without data loss.

use std::collections::BTreeSet;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{decode, encode, Certificate, CommittedSubDag, Hash as _, Header, ReputationScores};

/// Test that certificates correctly roundtrip through serialization.
#[test]
fn test_certificate_serde_roundtrip() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create a valid certificate
    let header = fixture.header_from_last_authority();
    let original_cert = fixture.certificate(&header);

    // Serialize
    let bytes: Vec<u8> = (&original_cert).into();

    // Deserialize
    let mut recovered_cert: Certificate = bytes.as_slice().into();

    // Compare key fields
    assert_eq!(original_cert.digest(), recovered_cert.digest());
    assert_eq!(original_cert.round(), recovered_cert.round());
    assert_eq!(original_cert.epoch(), recovered_cert.epoch());
    assert_eq!(original_cert.origin(), recovered_cert.origin());
    assert_eq!(original_cert.header().digest(), recovered_cert.header().digest());

    // Verify the recovered certificate is valid
    let verified = recovered_cert.validate_and_verify(&committee);
    assert!(verified.is_ok(), "Recovered certificate should still be valid: {:?}", verified.err());
}

/// Test that headers correctly roundtrip through serialization.
#[test]
fn test_header_serde_roundtrip() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();

    // Create a header
    let original_header = fixture.header_from_last_authority();

    // Serialize
    let bytes = encode(&original_header);

    // Deserialize
    let recovered_header: Header = decode(&bytes);

    // Compare
    assert_eq!(original_header.digest(), recovered_header.digest());
    assert_eq!(original_header.round(), recovered_header.round());
    assert_eq!(original_header.epoch(), recovered_header.epoch());
    assert_eq!(original_header.author(), recovered_header.author());
}

/// Test that CommittedSubDag correctly roundtrips through serialization.
#[test]
fn test_committed_subdag_serde_roundtrip() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create certificates for a subdag
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

    // Create a CommittedSubDag
    let leader = certificates.first().cloned().unwrap();
    let reputation = ReputationScores::new(&committee);

    let original_subdag =
        CommittedSubDag::new(certificates.clone(), leader.clone(), 1, reputation, None);

    // Serialize
    let bytes = encode(&original_subdag);

    // Deserialize
    let recovered_subdag: CommittedSubDag = decode(&bytes);

    // Compare leader and certificates
    assert_eq!(original_subdag.leader.digest(), recovered_subdag.leader.digest());
    assert_eq!(original_subdag.certificates.len(), recovered_subdag.certificates.len());

    for (orig, recov) in
        original_subdag.certificates.iter().zip(recovered_subdag.certificates.iter())
    {
        assert_eq!(orig.digest(), recov.digest());
    }
}
