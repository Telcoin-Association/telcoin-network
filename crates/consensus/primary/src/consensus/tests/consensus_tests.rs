//! Consensus tests

use crate::{
    consensus::{
        Bullshark, Consensus, ConsensusError, ConsensusState, LeaderSchedule, LeaderSwapTable,
    },
    test_utils::{make_optimal_certificates, mock_certificate},
    ConsensusBus,
};
use std::collections::BTreeSet;
use tn_storage::{mem_db::MemDatabase, CertificateStore, ConsensusStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    BlockNumHash, Certificate, CertificateDigest, ExecHeader, Hash as _, ReputationScores,
    SealedHeader, TaskManager, TnReceiver, TnSender, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};

/// This test is trying to compare the output of the Consensus algorithm when:
/// (1) running without any crash for certificates processed from round 1 to 5 (inclusive)
/// (2) when a crash happens with last commit at round 2, and then consensus recovers
///
/// The output of (1) is compared to the output of (2) . The output of (2) is the combination
/// of the output before the crash and after the crash. What we expect to see is the output of
/// (1) & (2) be exactly the same. That will ensure:
/// * no certificates re-commit happens
/// * no certificates are skipped
/// * no forks created
#[tokio::test]
async fn test_consensus_recovery_with_bullshark() {
    // GIVEN
    let num_sub_dags_per_schedule = 3;

    // AND Setup consensus
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let certificate_store = config.node_storage().clone();

    // config.set_consensus_bad_nodes_stake_threshold(33);

    // AND make certificates for rounds 1 to 7 (inclusive)
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _next_parents) =
        make_optimal_certificates(&committee, 1..=7, &genesis, &ids);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| {
        blocks.push_latest(0, BlockNumHash::new(0, B256::default()), Some(dummy_parent))
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager);

    // WHEN we feed all certificates to the consensus.
    for certificate in certificates.iter() {
        // we store the certificates so we can enable the recovery
        // mechanism later.
        certificate_store.write(certificate.clone()).unwrap();
        cb.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // THEN we expect to have 2 leader election rounds (round = 2, and round = 4).
    // In total we expect to have the following certificates get committed:
    // * 4 certificates from round 1
    // * 4 certificates from round 2
    // * 4 certificates from round 3
    // * 4 certificates from round 4
    // * 4 certificates from round 5
    // * 1 certificates from round 6 (the leader of last round)
    //
    // In total we should see 21 certificates committed
    let mut consensus_index_counter = 2;

    // hold all the certificates that get committed when consensus runs
    // without any crash.
    let mut committed_output_no_crash: Vec<Certificate> = Vec::new();
    let mut score_no_crash: ReputationScores = ReputationScores::default();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_no_crash = sub_dag.reputation_score.clone();
        assert_eq!(sub_dag.leader.round(), consensus_index_counter);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());
        for output in sub_dag.certificates {
            assert!(output.round() <= 6);

            committed_output_no_crash.push(output.clone());

            // we received the leader of round 6, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 6 {
                break 'main;
            }
        }
        consensus_index_counter += 2;
    }

    // AND the last committed store should be updated correctly
    let last_committed = consensus_store.read_last_committed(config.epoch());

    for id in ids.clone() {
        let last_round = *last_committed.get(&id).unwrap();

        // For the leader of round 6 we expect to have last committed round of 6.
        if id == leader_schedule.leader(6).id() {
            assert_eq!(last_round, 6);
        } else {
            // For the others should be 5.
            assert_eq!(last_round, 5);
        }
    }

    // AND shutdown consensus
    task_manager.abort();
    drop(task_manager);

    certificate_store.clear().unwrap();
    consensus_store.clear_consensus_chain_for_test();

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| {
        blocks.push_latest(0, BlockNumHash::new(0, B256::default()), Some(dummy_parent))
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager);

    // WHEN we send same certificates but up to round 3 (inclusive)
    // Then we store all the certificates up to round 6 so we can let the recovery algorithm
    // restore the consensus.
    // We omit round 7 so we can feed those later after "crash" to trigger a new leader
    // election round and commit.
    for certificate in certificates.iter() {
        if certificate.header().round() <= 3 {
            cb.new_certificates().send(certificate.clone()).await.unwrap();
        }
        if certificate.header().round() <= 6 {
            certificate_store.write(certificate.clone()).unwrap();
        }
    }

    // THEN we expect to commit with a leader of round 2.
    // So in total we expect to have committed certificates:
    // * 4 certificates of round 1
    // * 1 certificate of round 2 (the leader)
    let mut consensus_index_counter = 2;
    let mut committed_output_before_crash: Vec<Certificate> = Vec::new();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        assert_eq!(sub_dag.leader.round(), consensus_index_counter);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());
        for output in sub_dag.certificates {
            assert!(output.round() <= 2);

            committed_output_before_crash.push(output.clone());

            // we received the leader of round 2, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 2 {
                break 'main;
            }
        }
        consensus_index_counter += 2;
    }

    // AND shutdown (crash) consensus
    task_manager.abort();
    drop(task_manager);

    let bad_nodes_stake_threshold = 0;
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        bad_nodes_stake_threshold,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| {
        blocks.push_latest(0, BlockNumHash::new(0, B256::default()), Some(dummy_parent))
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config, &cb, bullshark, &task_manager);

    // WHEN send the certificates of round >= 5 to trigger a leader election for round 4
    // and start committing.
    for certificate in certificates.iter() {
        if certificate.header().round() >= 5 {
            cb.new_certificates().send(certificate.clone()).await.unwrap();
        }
    }

    // AND capture the committed output
    let mut committed_output_after_crash: Vec<Certificate> = Vec::new();
    let mut score_with_crash: ReputationScores = ReputationScores::default();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_with_crash = sub_dag.reputation_score.clone();
        assert_eq!(score_with_crash.total_authorities(), 4);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());

        for output in sub_dag.certificates {
            assert!(output.round() >= 2);

            committed_output_after_crash.push(output.clone());

            // we received the leader of round 6, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 6 {
                break 'main;
            }
        }
    }

    // THEN compare the output from a non-Crashed consensus to the outputs produced by the
    // crash consensus events. Those two should be exactly the same and will ensure that we see:
    // * no certificate re-commits
    // * no skips
    // * no forks
    committed_output_before_crash.append(&mut committed_output_after_crash);

    let all_output_with_crash = committed_output_before_crash;

    assert_eq!(committed_output_no_crash, all_output_with_crash);

    // AND ensure that scores are exactly the same
    assert_eq!(score_with_crash.scores_per_authority.len(), 4);
    assert_eq!(score_with_crash, score_no_crash);
    assert_eq!(
        score_with_crash.scores_per_authority.into_iter().filter(|(_, score)| *score == 1).count(),
        4
    );
}

/// Test that certificates with missing parents are rejected.
///
/// SECURITY: This ensures DAG integrity. A certificate must reference parents that
/// exist in the DAG to prevent forks and ensure causal ordering.
#[tokio::test]
async fn test_dag_rejects_certificate_with_missing_parents() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let gc_depth = 10;
    let mut state = ConsensusState::new(gc_depth);

    // Get genesis digests
    let genesis: BTreeSet<CertificateDigest> =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect();

    // Create round 1 certificates with valid genesis parents
    let (round1_certs, round1_digests) =
        make_optimal_certificates(&committee, 1..=1, &genesis, &ids);

    // Insert round 1 certificates
    for cert in &round1_certs {
        assert!(state.try_insert(cert).unwrap(), "Round 1 certs should insert");
    }

    // Create a fake parent digest that doesn't exist
    let mut fake_parent_bytes = [0u8; 32];
    fake_parent_bytes[31] = 0xFF;
    let fake_parent = CertificateDigest::new(fake_parent_bytes);

    // Create certificate with fake parent (that doesn't exist in DAG)
    let mut fake_parents = round1_digests.clone();
    fake_parents.insert(fake_parent);

    let (_, bad_cert) = mock_certificate(&committee, ids[0].clone(), 2, fake_parents);

    // Insertion should fail with MissingParent
    let result = state.try_insert(&bad_cert);
    match result {
        Err(ConsensusError::MissingParent(missing_digest, _)) => {
            assert_eq!(missing_digest, fake_parent, "Should report the fake parent as missing");
        }
        Ok(_) => panic!("Should reject certificate with missing parent - DAG integrity violation!"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

/// Test that certificates at or below GC round are rejected.
///
/// SECURITY: This prevents old certificates from being reprocessed after garbage collection,
/// which could allow replaying old decisions or cause memory exhaustion.
#[tokio::test]
async fn test_dag_rejects_certificate_at_gc_boundary() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let gc_depth = 5;
    let mut state = ConsensusState::new(gc_depth);

    let genesis: BTreeSet<CertificateDigest> =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect();

    // Build DAG from round 1 to 10
    let (certificates, _) = make_optimal_certificates(&committee, 1..=10, &genesis, &ids);

    // Insert all certificates and simulate commits to advance GC round
    for cert in &certificates {
        let _ = state.try_insert(cert);
        // Simulate commit to advance GC
        state.update(cert);
    }

    // GC round should now be: committed_round - gc_depth = 10 - 5 = 5
    assert_eq!(state.last_round.gc_round, 5, "GC round should be 5");

    // Try to insert a certificate at round 4 (below GC round 5)
    let (_, old_cert) = mock_certificate(&committee, ids[0].clone(), 4, genesis.clone());

    let result = state.try_insert(&old_cert);
    assert!(result.is_ok(), "Should not error, but reject silently");
    assert!(!result.unwrap(), "Certificate at round 4 should be rejected (below GC round 5)");

    // Try to insert at exactly GC round (round 5)
    // Implementation uses `round <= gc_round` so round 5 is also rejected
    let (_, gc_boundary_cert) = mock_certificate(&committee, ids[1].clone(), 5, genesis.clone());

    let result = state.try_insert(&gc_boundary_cert);
    assert!(result.is_ok(), "Should not error for boundary case");
    assert!(!result.unwrap(), "Certificate at round <= gc_round should be rejected");
}

/// Test that equivocation is detected at the ConsensusState level.
///
/// SECURITY: Byzantine validators must not be able to submit two different certificates
/// for the same round. This is critical for consensus safety.
#[tokio::test]
async fn test_dag_detects_equivocation_same_round_different_cert() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let gc_depth = 10;
    let mut state = ConsensusState::new(gc_depth);

    let genesis: BTreeSet<CertificateDigest> =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect();

    // Create first certificate from authority 0 at round 1
    let (_, cert1) = mock_certificate(&committee, ids[0].clone(), 1, genesis.clone());

    // Insert first certificate
    assert!(state.try_insert(&cert1).unwrap(), "First cert should insert");

    // Create DIFFERENT certificate from same authority at same round
    // (different payload results in different digest)
    let mut different_parents = genesis.clone();
    // Remove one parent to create a different certificate
    if let Some(first) = different_parents.iter().next().cloned() {
        different_parents.remove(&first);
    }
    let (_, cert2) = mock_certificate(&committee, ids[0].clone(), 1, different_parents);

    // Verify the certificates are different
    assert_ne!(cert1.digest(), cert2.digest(), "Certificates should have different digests");
    assert_eq!(cert1.origin(), cert2.origin(), "Same authority");
    assert_eq!(cert1.round(), cert2.round(), "Same round");

    // Second insertion should fail with equivocation error
    let result = state.try_insert(&cert2);
    match result {
        Err(ConsensusError::CertificateEquivocation(new_cert, existing_cert)) => {
            assert_eq!(new_cert.digest(), cert2.digest());
            assert_eq!(existing_cert.digest(), cert1.digest());
        }
        Ok(_) => panic!("Should detect equivocation - Byzantine attack possible!"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

/// Test that reinserting the same certificate is idempotent.
///
/// This is important for network retries - receiving the same certificate twice
/// should not cause errors or state corruption.
#[tokio::test]
async fn test_dag_accepts_duplicate_certificate_insertion() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let gc_depth = 10;
    let mut state = ConsensusState::new(gc_depth);

    let genesis: BTreeSet<CertificateDigest> =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect();

    let (_, cert) = mock_certificate(&committee, ids[0].clone(), 1, genesis);

    // Insert once
    assert!(state.try_insert(&cert).unwrap(), "First insertion should succeed");

    // Insert same certificate again - should succeed (idempotent)
    let result = state.try_insert(&cert);
    assert!(result.is_ok(), "Duplicate insertion should not error");
    // Note: Returns false because it's already committed for this authority
}

/// Test that certificates with missing parent round are rejected.
///
/// SECURITY: If the entire parent round is missing from the DAG, the certificate
/// cannot be validated and must be rejected.
#[tokio::test]
async fn test_dag_rejects_certificate_with_missing_parent_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();

    let gc_depth = 10;
    let mut state = ConsensusState::new(gc_depth);

    let genesis: BTreeSet<CertificateDigest> =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect();

    // Create round 1 certificates
    let (round1_certs, round1_digests) =
        make_optimal_certificates(&committee, 1..=1, &genesis, &ids);

    // Insert round 1
    for cert in &round1_certs {
        state.try_insert(cert).unwrap();
    }

    // Create round 2 certificates
    let (round2_certs, round2_digests) =
        make_optimal_certificates(&committee, 2..=2, &round1_digests, &ids);

    // Insert round 2
    for cert in &round2_certs {
        state.try_insert(cert).unwrap();
    }

    // Try to insert a round 4 certificate that references round 3
    // but round 3 doesn't exist in the DAG
    let (_round3_certs, round3_digests) =
        make_optimal_certificates(&committee, 3..=3, &round2_digests, &ids);

    // DON'T insert round 3

    // Create round 4 certificate referencing round 3 parents
    let (_, round4_cert) = mock_certificate(&committee, ids[0].clone(), 4, round3_digests);

    // Should fail because round 3 is missing
    let result = state.try_insert(&round4_cert);
    match result {
        Err(ConsensusError::MissingParentRound(_)) => {
            // Expected - parent round 3 is missing
        }
        Ok(_) => panic!("Should reject certificate with missing parent round!"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}
