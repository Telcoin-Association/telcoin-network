//! DAG State tests for consensus.
//!
//! These tests verify the critical invariants of DAG state management:
//! - Equivocation detection (same authority, same round, different certificate)
//! - Parent verification (parents must exist before child insertion)
//! - Garbage collection (old rounds are removed)
//! - Idempotent insertion (same certificate twice is OK)

use std::collections::BTreeSet;
use tn_primary::consensus::ConsensusState;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{Certificate, Hash as _, HeaderBuilder};

/// Test that equivocation is detected when the same authority creates
/// two different certificates for the same round.
#[tokio::test]
async fn test_dag_equivocation_same_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);

    // Get genesis parents
    let genesis: BTreeSet<_> = fixture.genesis().collect();

    // Create first certificate for round 1 from first authority
    let authority = fixture.first_authority();
    let header1 = HeaderBuilder::default()
        .author(authority.id())
        .round(1)
        .epoch(committee.epoch())
        .parents(genesis.clone())
        .build();
    let cert1 = fixture.certificate(&header1);

    // Insert first certificate - should succeed
    let result = state.try_insert(&cert1);
    assert!(result.is_ok(), "First certificate insertion should succeed");
    assert!(result.unwrap(), "Certificate should be inserted (not below committed round)");

    // Create a DIFFERENT certificate for the same round from the same authority
    // We create it with a different payload to make it different
    let header2 = HeaderBuilder::default()
        .author(authority.id())
        .round(1)
        .epoch(committee.epoch())
        .parents(genesis)
        .created_at(1) // Different timestamp makes different digest
        .build();
    let cert2 = fixture.certificate(&header2);

    // Verify they have different digests
    assert_ne!(cert1.digest(), cert2.digest(), "Certificates should have different digests");

    // Insert second certificate - should fail with equivocation error
    let result = state.try_insert(&cert2);
    assert!(result.is_err(), "Second certificate should fail due to equivocation");

    let err = result.unwrap_err();
    assert!(err.to_string().contains("equivocates"), "Error should mention equivocation: {}", err);
}

/// Test that parents must exist before a child certificate can be inserted.
#[tokio::test]
async fn test_dag_parent_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);

    // Get genesis and create round 1 certificates
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, round1_headers) = fixture.headers_round(0, &genesis);
    let round1_certs: Vec<_> = round1_headers.iter().map(|h| fixture.certificate(h)).collect();

    // Insert round 1 certificates
    for cert in &round1_certs {
        let result = state.try_insert(cert);
        assert!(result.is_ok(), "Round 1 certificate should be inserted");
    }

    // Create round 2 certificates with round 1 as parents
    let round1_digests: BTreeSet<_> = round1_certs.iter().map(|c| c.digest()).collect();
    let (_, round2_headers) = fixture.headers_round(1, &round1_digests);
    let round2_certs: Vec<_> = round2_headers.iter().map(|h| fixture.certificate(h)).collect();

    // Insert round 2 certificates - should succeed because parents exist
    for cert in &round2_certs {
        let result = state.try_insert(cert);
        assert!(result.is_ok(), "Round 2 certificate should be inserted when parents exist");
    }

    // Now create a round 3 certificate with FAKE parents (that don't exist)
    let authority = fixture.first_authority();
    let fake_parent = Certificate::default().digest();
    let header_with_fake_parent = HeaderBuilder::default()
        .author(authority.id())
        .round(3)
        .epoch(committee.epoch())
        .parents([fake_parent].into_iter().collect())
        .build();
    let cert_with_fake_parent = fixture.certificate(&header_with_fake_parent);

    // This should fail because the parent doesn't exist
    let result = state.try_insert(&cert_with_fake_parent);
    assert!(result.is_err(), "Certificate with missing parent should fail");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Parent") || err.to_string().contains("parent"),
        "Error should mention missing parent: {}",
        err
    );
}

/// Test that garbage collection removes old rounds based on gc_depth.
#[tokio::test]
async fn test_dag_gc_removes_old_rounds() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let _committee = fixture.committee();
    let gc_depth = 5; // Small gc_depth for testing
    let mut state = ConsensusState::new(gc_depth);

    // Build up DAG with multiple rounds
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let mut parents = genesis;

    // Create 10 rounds of certificates
    for round in 1..=10 {
        let (_, headers) = fixture.headers_round(round - 1, &parents);
        let certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

        for cert in &certs {
            let _ = state.try_insert(cert);
        }

        parents = certs.iter().map(|c| c.digest()).collect();

        // Simulate commit for each leader round (even rounds in Bullshark)
        if round % 2 == 0 {
            if let Some(leader_cert) = certs.first() {
                state.update(leader_cert);
            }
        }
    }

    // After committing round 10, gc_round should be 10 - gc_depth = 5
    // Rounds <= 5 should be garbage collected
    assert!(
        state.dag.keys().all(|&r| r > state.last_round.gc_round),
        "All remaining rounds should be above gc_round"
    );

    // Verify that old rounds are actually removed
    for round in 1..=state.last_round.gc_round {
        assert!(
            !state.dag.contains_key(&round),
            "Round {} should have been garbage collected",
            round
        );
    }
}

/// Test that inserting the same certificate twice is idempotent (no error).
#[tokio::test]
async fn test_dag_idempotent_insert() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let _committee = fixture.committee();
    let gc_depth = 50;
    let mut state = ConsensusState::new(gc_depth);

    // Create a certificate
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let cert = fixture.certificate(headers.first().unwrap());

    // Insert once
    let result1 = state.try_insert(&cert);
    assert!(result1.is_ok(), "First insertion should succeed");

    // Insert same certificate again - should NOT error
    let result2 = state.try_insert(&cert);
    assert!(result2.is_ok(), "Second insertion of same certificate should succeed (idempotent)");

    // The certificate should only appear once in the DAG
    if let Some(round_map) = state.dag.get(&cert.round()) {
        let count = round_map.values().filter(|(d, _)| *d == cert.digest()).count();
        assert_eq!(count, 1, "Certificate should appear exactly once in DAG");
    }
}

/// Test that certificates at or below gc_round are rejected.
#[tokio::test]
async fn test_dag_rejects_old_certificates() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let _committee = fixture.committee();
    let gc_depth = 5;
    let mut state = ConsensusState::new(gc_depth);

    // Build DAG and advance committed round
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let mut parents = genesis.clone();

    // Create rounds 1-10 and commit them
    for round in 1..=10 {
        let (_, headers) = fixture.headers_round(round - 1, &parents);
        let certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

        for cert in &certs {
            let _ = state.try_insert(cert);
        }

        parents = certs.iter().map(|c| c.digest()).collect();

        if round % 2 == 0 {
            if let Some(leader_cert) = certs.first() {
                state.update(leader_cert);
            }
        }
    }

    // Now try to insert a certificate from round 1 (which should be GC'd)
    let (_, old_headers) = fixture.headers_round(0, &genesis);
    let old_cert = fixture.certificate(old_headers.first().unwrap());
    assert_eq!(old_cert.round(), 1);

    // This should return Ok(false) - not an error, but not inserted
    let result = state.try_insert(&old_cert);
    assert!(result.is_ok(), "Old certificate should not error, just be ignored");
    assert!(!result.unwrap(), "Old certificate should not be inserted (below GC round)");
}
