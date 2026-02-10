//! Recovery tests for consensus state persistence.
//!
//! These tests verify that consensus state survives restarts:
//! - Subdags persist across restarts
//! - DAG can be reconstructed from storage

use std::collections::BTreeSet;
use tempfile::TempDir;
use tn_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{CertificateDigestByRound, Certificates, ConsensusBlocks},
    CertificateStore, ConsensusStore,
};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{CommittedSubDag, Database, Hash as _, ReputationScores};

/// Test that subdags are correctly persisted and retrieved after restart.
#[tokio::test]
async fn test_subdag_persists_restart() {
    let temp_dir = TempDir::new().unwrap();

    // Use MemDatabase for fixture (to generate certificates)
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create certificates for a subdag
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader_digest = certificates.first().unwrap().digest();

    // Phase 1: Write data
    {
        let store = open_db(temp_dir.path());

        // Create and persist subdags with sequential indices
        for idx in 0..5u64 {
            let leader = certificates.first().cloned().unwrap();
            let reputation = ReputationScores::new(&committee);
            let subdag = CommittedSubDag::new(certificates.clone(), leader, idx, reputation, None);

            store.write_subdag_for_test(idx, subdag);
        }
        store.persist::<ConsensusBlocks>().await;

        // Verify latest subdag exists
        let latest = store.get_latest_sub_dag();
        assert!(latest.is_some(), "Should have a latest subdag");

        drop(store); // Explicit drop to release DB lock
    }

    // Allow time for MDBX to fully release locks
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Phase 2: Simulate "restart" by opening a new handle to same storage
    {
        let store2 = open_db(temp_dir.path());
        let latest_after_restart = store2.get_latest_sub_dag();
        assert!(latest_after_restart.is_some(), "Should have latest subdag after restart");
        let latest_subdag = latest_after_restart.unwrap();

        // Verify subdag content persisted correctly
        assert_eq!(
            latest_subdag.leader.digest(),
            leader_digest,
            "Leader should persist across restart"
        );
        assert_eq!(
            latest_subdag.certificates.len(),
            certificates.len(),
            "Certificates should persist"
        );
    }
}

/// Test that subdags persist across epoch-like boundaries.
#[tokio::test]
async fn test_subdag_persists_multiple_writes() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Create fixture for epoch 0 (use MemDatabase for certificate generation)
    let fixture_epoch0 = CommitteeFixture::builder(MemDatabase::default).build();
    let committee_epoch0 = fixture_epoch0.committee();

    // Create certificates for epoch 0
    let genesis: BTreeSet<_> = fixture_epoch0.genesis().collect();
    let (_, headers) = fixture_epoch0.headers_round(0, &genesis);
    let certs_epoch0: Vec<_> = headers.iter().map(|h| fixture_epoch0.certificate(h)).collect();

    // Write subdags for epoch 0 with indices 0, 1, 2
    for idx in 0..3u64 {
        let leader = certs_epoch0.first().cloned().unwrap();
        let reputation = ReputationScores::new(&committee_epoch0);
        let subdag = CommittedSubDag::new(certs_epoch0.clone(), leader, idx, reputation, None);
        store.write_subdag_for_test(idx, subdag);
    }
    store.persist::<ConsensusBlocks>().await;

    // Verify state after first batch
    let latest_epoch0 = store.get_latest_sub_dag();
    assert!(latest_epoch0.is_some(), "Should have subdags after first batch");

    // Now simulate epoch transition - create new committee for epoch 1
    let fixture_epoch1 = CommitteeFixture::builder(MemDatabase::default).build();
    let committee_epoch1 = fixture_epoch1.committee();

    // Create certificates for "epoch 1"
    let genesis1: BTreeSet<_> = fixture_epoch1.genesis().collect();
    let (_, headers1) = fixture_epoch1.headers_round(0, &genesis1);
    let certs_epoch1: Vec<_> = headers1.iter().map(|h| fixture_epoch1.certificate(h)).collect();
    let epoch1_leader_digest = certs_epoch1.first().unwrap().digest();

    // Continue with indices 3, 4, 5
    for idx in 3..6u64 {
        let leader = certs_epoch1.first().cloned().unwrap();
        let reputation = ReputationScores::new(&committee_epoch1);
        let subdag = CommittedSubDag::new(certs_epoch1.clone(), leader, idx, reputation, None);
        store.write_subdag_for_test(idx, subdag);
    }
    store.persist::<ConsensusBlocks>().await;

    // Verify latest subdag is from the second batch
    let latest_after_epoch1 = store.get_latest_sub_dag().unwrap();
    assert_eq!(
        latest_after_epoch1.leader.digest(),
        epoch1_leader_digest,
        "Latest subdag should be from second batch"
    );
}

/// Test that certificates are persisted and can be read after restart.
#[tokio::test]
async fn test_certificate_store_persists_restart() {
    let temp_dir = TempDir::new().unwrap();

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();

    // Create certificates
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let digests: Vec<_> = certificates.iter().map(|c| c.digest()).collect();

    // Phase 1: Write certificates to storage
    {
        let store = open_db(temp_dir.path());
        store.write_all(certificates.clone()).unwrap();
        store.persist::<Certificates>().await;
        drop(store); // Explicit drop to release DB lock
    }

    // Allow time for MDBX to fully release locks
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Phase 2: "Restart" - open new handle to storage
    {
        let store2 = open_db(temp_dir.path());

        // Verify all certificates can be retrieved
        for digest in &digests {
            let recovered = store2.read(*digest).unwrap();
            assert!(recovered.is_some(), "Certificate should be recovered after restart");

            let recovered_cert = recovered.unwrap();
            assert_eq!(&recovered_cert.digest(), digest, "Recovered certificate should match");
        }

        // Verify multi_contains works
        let found = store2.multi_contains(digests.iter()).unwrap();
        assert!(found.iter().all(|&x| x), "All certificates should be found");
    }
}

/// Test that DAG reconstruction from storage produces correct state.
#[tokio::test]
async fn test_dag_reconstruction_from_store() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();

    // Build multiple rounds of certificates
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let mut all_certs = Vec::new();
    let mut parents = genesis;

    // Create 5 rounds of certificates
    for round in 1..=5 {
        let (_, headers) = fixture.headers_round(round - 1, &parents);
        let certs: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

        // Store certificates
        store.write_all(certs.clone()).unwrap();

        parents = certs.iter().map(|c| c.digest()).collect();
        all_certs.extend(certs);
    }
    store.persist::<Certificates>().await;
    store.persist::<CertificateDigestByRound>().await;

    // Verify we can retrieve certificates by round (simulates DAG reconstruction)
    let gc_round = 0;
    let recovered_certs = store.after_round(gc_round + 1).unwrap();

    assert_eq!(
        recovered_certs.len(),
        all_certs.len(),
        "Should recover all certificates after GC round"
    );

    // Verify certificates are returned in round order
    let mut last_round = 0;
    for cert in &recovered_certs {
        assert!(cert.round() >= last_round, "Certificates should be in non-decreasing round order");
        last_round = cert.round();
    }

    // Verify highest round
    let highest = store.highest_round_number();
    assert_eq!(highest, 5, "Highest round should be 5");
}

/// Test that last committed state is correctly persisted and retrieved.
#[tokio::test]
async fn test_last_committed_persists() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // Create and commit subdags
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

    // Create subdags with different leaders to track last committed per authority
    for (idx, cert) in certificates.iter().enumerate() {
        let reputation = ReputationScores::new(&committee);
        let subdag =
            CommittedSubDag::new(vec![cert.clone()], cert.clone(), idx as u64, reputation, None);
        store.write_subdag_for_test(idx as u64, subdag);
    }
    store.persist::<ConsensusBlocks>().await;

    // Read last committed state
    let last_committed = store.read_last_committed(committee.epoch());

    // Should have entries for authorities that were leaders
    assert!(!last_committed.is_empty(), "Should have last committed entries for authorities");

    // Verify the rounds are tracked
    for (authority, round) in &last_committed {
        assert!(*round >= 1, "Committed round for {:?} should be >= 1", authority);
    }
}
