//! State sync integration tests.
//!
//! These tests verify state synchronization behavior:
//! - ConsensusOutput is saved correctly
//! - Parent hash chain is validated

#![allow(unused_crate_dependencies)]

use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};
use tempfile::TempDir;
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase, open_db};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{CommittedSubDag, ConsensusHeader, ConsensusOutput, ReputationScores, B256};

/// Test that save_consensus correctly persists ConsensusOutput to database.
#[tokio::test]
async fn test_sync_save_consensus() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).unwrap();

    // Create certificates for a subdag
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.first().cloned().unwrap();

    // Create a CommittedSubDag
    let reputation = ReputationScores::new(&committee);
    let sub_dag =
        Arc::new(CommittedSubDag::new(certificates.clone(), leader.clone(), 0, reputation, None));

    // Create ConsensusOutput using struct initialization
    let output = ConsensusOutput::new(sub_dag, B256::ZERO, 1, false, VecDeque::new(), vec![]);

    // Save consensus using state_sync
    state_sync::save_consensus(&store, output, &None, &mut consensus_chain).await.unwrap();

    // Verify the consensus was saved
    let saved = consensus_chain.consensus_header_by_number(None, 1).await.unwrap();
    assert!(saved.is_some(), "Consensus should be saved to database");

    let saved_header = saved.unwrap();
    assert_eq!(saved_header.number, 1, "Consensus number should be 1");
    assert_eq!(
        saved_header.sub_dag.certificates.len(),
        certificates.len(),
        "Should have same number of certificates"
    );
}

/// Test that parent hash chain is properly formed.
#[tokio::test]
async fn test_sync_parent_hash_chain() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).unwrap();

    // Create certificates
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

    let mut parent_hash = B256::ZERO;
    let mut saved_digests: Vec<B256> = vec![B256::ZERO]; // Genesis parent

    for i in 1..=3u64 {
        let leader = certificates.first().cloned().unwrap();
        let reputation = ReputationScores::new(&committee);
        let sub_dag =
            Arc::new(CommittedSubDag::new(certificates.clone(), leader, i - 1, reputation, None));

        let output =
            ConsensusOutput::new(sub_dag.clone(), parent_hash, i, false, VecDeque::new(), vec![]);

        state_sync::save_consensus(&store, output, &None, &mut consensus_chain).await.unwrap();

        // Calculate the digest that was saved
        let digest = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, i);
        saved_digests.push(digest);
        parent_hash = digest;
    }

    // Verify we can read back the headers and parent hash chain is correct
    for i in 1..=3u64 {
        let header = consensus_chain.consensus_header_by_number(None, i).await.unwrap();
        assert!(header.is_some(), "Header {} should exist", i);

        let header = header.unwrap();
        // Verify parent_hash matches previous digest
        assert_eq!(
            header.parent_hash,
            saved_digests[(i - 1) as usize],
            "Header {} parent_hash should match previous digest",
            i
        );
        // Verify number is correct
        assert_eq!(header.number, i, "Header number should match");
    }
}

/// Test that consensus can be looked up by hash (digest index is populated).
#[tokio::test]
async fn test_sync_lookup_by_hash() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).unwrap();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.first().cloned().unwrap();

    let reputation = ReputationScores::new(&committee);
    let sub_dag = Arc::new(CommittedSubDag::new(certificates, leader, 0, reputation, None));

    let output =
        ConsensusOutput::new(sub_dag.clone(), B256::ZERO, 1, false, VecDeque::new(), vec![]);

    state_sync::save_consensus(&store, output, &None, &mut consensus_chain).await.unwrap();

    // Compute the expected digest
    let expected_digest = ConsensusHeader::digest_from_parts(B256::ZERO, &sub_dag, 1);

    // Verify we can look up by hash
    let by_hash = consensus_chain.consensus_header_by_digest(None, expected_digest).await.unwrap();
    assert!(by_hash.is_some(), "Should be able to look up by hash");
    assert_eq!(by_hash.unwrap().number, 1, "Looked up header should have correct number");
}

/// Test that digest computation is deterministic.
#[tokio::test]
async fn test_digest_determinism() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.first().cloned().unwrap();

    let reputation = ReputationScores::new(&committee);
    let sub_dag = CommittedSubDag::new(certificates, leader, 0, reputation, None);

    let parent_hash = B256::ZERO;
    let number = 1u64;

    // Compute digest multiple times - must be identical
    let digest1 = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
    let digest2 = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
    let digest3 = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);

    assert_eq!(digest1, digest2, "Digest must be deterministic");
    assert_eq!(digest2, digest3, "Digest must be deterministic");
}

/// Test that different inputs produce different digests (basic collision resistance).
#[tokio::test]
async fn test_digest_collision_resistance() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.first().cloned().unwrap();

    let reputation = ReputationScores::new(&committee);
    let sub_dag = CommittedSubDag::new(certificates, leader, 0, reputation, None);

    // Same subdag but different parent_hash
    let digest_parent_zero = ConsensusHeader::digest_from_parts(B256::ZERO, &sub_dag, 1);
    let mut different_parent = [0u8; 32];
    different_parent[31] = 1;
    let digest_parent_one =
        ConsensusHeader::digest_from_parts(B256::from(different_parent), &sub_dag, 1);

    assert_ne!(
        digest_parent_zero, digest_parent_one,
        "Different parent_hash must produce different digest"
    );

    // Same subdag and parent but different number
    let digest_num_1 = ConsensusHeader::digest_from_parts(B256::ZERO, &sub_dag, 1);
    let digest_num_2 = ConsensusHeader::digest_from_parts(B256::ZERO, &sub_dag, 2);

    assert_ne!(digest_num_1, digest_num_2, "Different number must produce different digest");
}

/// Test that wrong parent_hash in chain would be detected.
/// This simulates what catch_up_consensus_from_to checks.
#[tokio::test]
async fn test_digest_mismatch_detection() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).unwrap();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.first().cloned().unwrap();

    let reputation = ReputationScores::new(&committee);
    let sub_dag = Arc::new(CommittedSubDag::new(certificates, leader, 0, reputation, None));

    // Save block 1 with correct parent (ZERO)
    let output1 = ConsensusOutput::new(sub_dag, B256::ZERO, 1, false, VecDeque::new(), vec![]);
    state_sync::save_consensus(&store, output1, &None, &mut consensus_chain).await.unwrap();

    // Get block 1's digest
    let block1 = consensus_chain.consensus_header_by_number(None, 1).await.unwrap().unwrap();
    let block1_digest = block1.digest();

    // Now simulate verification: if we compute digest with wrong parent, it won't match
    let mut wrong_parent_bytes = [0u8; 32];
    wrong_parent_bytes[31] = 99;
    let wrong_parent = B256::from(wrong_parent_bytes);
    let computed_with_wrong_parent =
        ConsensusHeader::digest_from_parts(wrong_parent, &block1.sub_dag, 1);

    // This is the key security check: computed digest != stored digest means fork/corruption
    assert_ne!(
        computed_with_wrong_parent, block1_digest,
        "Computed digest with wrong parent must not match stored digest - this detects forks"
    );

    // Verify that correct parent produces matching digest
    let computed_with_correct_parent =
        ConsensusHeader::digest_from_parts(B256::ZERO, &block1.sub_dag, 1);
    assert_eq!(
        computed_with_correct_parent, block1_digest,
        "Computed digest with correct parent must match stored digest"
    );
}

fn main() {}
