use tn_types::{BlockNumHash, CommittedSubDag, ConsensusOutput, B256};

#[test]
fn consensus_output_num_hash_matches_cached_header_hash() {
    let output = ConsensusOutput::new_with_subdag(
        CommittedSubDag::default().into(),
        B256::from([7u8; 32]),
        42,
    );

    assert!(output.consensus_header_hash_cache.get().is_none());

    let hash = output.consensus_header_hash();
    assert_eq!(output.consensus_header_hash_cache.get().copied(), Some(hash));
    assert_eq!(output.num_hash(), BlockNumHash::new(42, hash));

    // Repeated calls should return the same cached digest.
    assert_eq!(output.consensus_header_hash(), hash);
}
