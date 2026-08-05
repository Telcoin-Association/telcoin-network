//! Consensus tests

use super::resolve_seed_chain_anchor;
use crate::{
    consensus::{
        Bullshark, Consensus, ConsensusError, ConsensusState, LeaderSchedule, LeaderSwapTable,
    },
    test_utils::{make_optimal_certificates, mock_certificate},
    ConsensusBus,
};
use std::collections::BTreeSet;
use tempfile::TempDir;
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase, CertificateStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    keccak256, BlsSignature, Certificate, CommittedSubDag, ConsensusHeaderDigest, ConsensusNumHash,
    Epoch, EpochSeedChainError, EpochSeedChainValue, ExecHeader, Hash as _, Header, HeaderDigest,
    ReputationScores, Round, SealedHeader, TaskManager, TnReceiver, TnSender, B256,
    DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use tokio::fs::create_dir_all;

/// The fold domain separator, duplicated here on purpose (see [`independent_fold`]).
const FOLD_DOMAIN: &[u8] = b"TN_EPOCH_SEED_FOLD_V1";

/// The root domain separator, duplicated here on purpose (see [`independent_fold`]).
const ROOT_DOMAIN: &[u8] = b"TN_EPOCH_SEED_ROOT_V1";

/// Recompute one seed chain fold step INDEPENDENTLY of [`EpochSeedChainValue::fold`].
///
/// Spelling the preimage out from literal domain bytes is what gives the chain assertions in
/// [`test_consensus_recovery_with_bullshark`] force: an expectation built by calling the production
/// helper would move with any mutation *inside* that helper and stay green.
fn independent_fold(anchor: B256, leader_round: Round, signature: &BlsSignature) -> B256 {
    let preimage: Vec<u8> = FOLD_DOMAIN
        .iter()
        .copied()
        .chain(anchor.as_slice().iter().copied())
        .chain(leader_round.to_le_bytes())
        .chain(signature.to_bytes())
        .collect();
    keccak256(preimage)
}

/// Recompute the per-epoch chain root INDEPENDENTLY of [`EpochSeedChainValue::epoch_root`], for the
/// same reason as [`independent_fold`].
fn independent_root(epoch: Epoch) -> B256 {
    let preimage: Vec<u8> = ROOT_DOMAIN.iter().copied().chain(epoch.to_le_bytes()).collect();
    keccak256(preimage)
}

/// Assert that `sub_dags` form an unbroken seed chain rooted at `epoch`'s root.
///
/// This is the pin for the PRODUCTION commit loop's chain wiring in
/// [`Bullshark::process_certificate`](crate::consensus::Bullshark), which nothing else exercises:
/// the sub-dags handed in must have come off the real `sequence` channel, in commit order.
///
/// Catches both ways the loop can collapse the chain back to a per-commit constant:
///
/// - deleting `state.set_seed_chain(sub_dag.seed_chain_value())`, so every commit folds the same
///   stale anchor - the `assert_eq!` for the second commit fails, and the trailing `assert_ne!`
///   names the resulting re-rooted value directly;
/// - replacing `state.seed_chain()` with a fixed value such as
///   [`EpochSeedChainValue::genesis_placeholder`], which breaks the first commit's fold over the
///   epoch root.
fn assert_seed_chain_advances(sub_dags: &[CommittedSubDag], epoch: Epoch) {
    assert!(
        sub_dags.len() >= 2,
        "the chain assertions need at least two commits, got {}",
        sub_dags.len()
    );

    let first = sub_dags.first().expect("checked non-empty above");
    assert_eq!(
        first.randomness(),
        independent_fold(
            independent_root(epoch),
            first.leader().round(),
            first.leader().seed_signature().expect("leader seed signature for fork-active epoch")
        ),
        "the epoch's first commit must fold the epoch root"
    );

    sub_dags.windows(2).for_each(|pair| {
        let (previous, current) = (&pair[0], &pair[1]);
        assert_eq!(
            current.randomness(),
            independent_fold(
                previous.randomness(),
                current.leader().round(),
                current
                    .leader()
                    .seed_signature()
                    .expect("leader seed signature for fork-active epoch")
            ),
            "commit at leader round {} must fold the previous commit's chain value",
            current.leader().round()
        );
        assert_ne!(
            current.randomness(),
            independent_fold(
                independent_root(epoch),
                current.leader().round(),
                current
                    .leader()
                    .seed_signature()
                    .expect("leader seed signature for fork-active epoch")
            ),
            "commit at leader round {} must not re-root the chain",
            current.leader().round()
        );
    });
}

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
    let certificate_store = config.node_storage().clone();

    // config.set_consensus_bad_nodes_stake_threshold(33);

    // AND make certificates for rounds 1 to 7 (inclusive)
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _next_parents) =
        make_optimal_certificates(&committee, 1..=7, &genesis, &ids);
    let temp_dir = TempDir::new().unwrap();
    let mut consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone()).await.unwrap();

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await
    .unwrap();
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager, &consensus_chain, None)
        .await
        .unwrap();

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
    let mut committed_output_no_crash: Vec<Header> = Vec::new();
    let mut score_no_crash: ReputationScores = ReputationScores::default();
    // Every sub-dag the PRODUCTION commit loop emitted, in commit order, for the seed chain pin
    // below. Collected here rather than in a separate test because nothing else in the suite drives
    // more than one commit through the real `Bullshark` loop.
    let mut committed_sub_dags_no_crash: Vec<CommittedSubDag> = Vec::new();

    let mut idx = 1;
    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_no_crash = sub_dag.reputation_scores().clone();
        assert_eq!(sub_dag.leader().round(), consensus_index_counter);
        consensus_chain.write_subdag_for_test(idx, sub_dag.clone()).await;
        committed_sub_dags_no_crash.push(sub_dag.clone());
        idx += 1;
        for output in sub_dag.headers() {
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

    // AND the seed chain advanced across those commits: every commit folded the PREVIOUS commit's
    // value, and the epoch's first commit folded the epoch root. This is the only assertion in the
    // suite that covers the chain wiring inside the real commit loop.
    assert_seed_chain_advances(&committed_sub_dags_no_crash, config.epoch());

    // AND the last committed store should be updated correctly
    let last_committed = consensus_chain.read_last_committed(config.epoch()).await.unwrap();

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
    // Make new chain DB to "clear" it.
    let path2 = temp_dir.path().join("2");
    create_dir_all(&path2).await.unwrap();
    let mut consensus_chain = ConsensusChain::new_for_test(path2, committee.clone()).await.unwrap();

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        &mut consensus_chain,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    )
    .await
    .unwrap();
    let bullshark = Bullshark::new(
        committee.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager, &consensus_chain, None)
        .await
        .unwrap();

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
    let mut pack_number = 1u64;
    let mut committed_output_before_crash: Vec<Header> = Vec::new();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        assert_eq!(sub_dag.leader().round(), consensus_index_counter);
        consensus_chain.write_subdag_for_test(pack_number, sub_dag.clone()).await;
        pack_number += 1;
        for output in sub_dag.headers() {
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
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let mut rx_output = cb.subscribe_sequence();
    let task_manager = TaskManager::default();
    Consensus::spawn(config, &cb, bullshark, &task_manager, &consensus_chain, None).await.unwrap();

    // WHEN send the certificates of round >= 5 to trigger a leader election for round 4
    // and start committing.
    for certificate in certificates.iter() {
        if certificate.header().round() >= 5 {
            cb.new_certificates().send(certificate.clone()).await.unwrap();
        }
    }

    // AND capture the committed output
    let mut committed_output_after_crash: Vec<Header> = Vec::new();
    let mut score_with_crash: ReputationScores = ReputationScores::default();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_with_crash = sub_dag.reputation_scores().clone();
        assert_eq!(score_with_crash.total_authorities(), 4);
        consensus_chain.write_subdag_for_test(pack_number, sub_dag.clone()).await;
        pack_number += 1;

        for output in sub_dag.headers() {
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
    let genesis: BTreeSet<HeaderDigest> =
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
    let fake_parent = HeaderDigest::new(fake_parent_bytes);

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

    let genesis: BTreeSet<HeaderDigest> =
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

    let genesis: BTreeSet<HeaderDigest> =
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

    let genesis: BTreeSet<HeaderDigest> =
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

    let genesis: BTreeSet<HeaderDigest> =
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

/// Startup recovery of the epoch seed chain anchor must FAIL LOUDLY on every inconsistent pair of
/// recovered cursors, never fall back to the epoch root.
///
/// This is the direct test of the fallible recovery function over all four
/// `(latest_sub_dag, last_committed_round > 0)` combinations. A genuine pack read-error injection
/// is impractical here: the reader is behind `ConsensusChainReader`, a 21-method trait with a
/// single implementor, so a failing mock would be pure boilerplate. The `(None, true)` case below
/// IS the read-error shape - the read produced no sub-dag while the commit cursor says this epoch
/// has committed - and it is the case the old `unwrap_or_default()` collapsed into "fresh epoch".
///
/// Catches: reintroducing any fallback on the three error arms. The final assertion shows why that
/// matters: the value the old code silently produced (the epoch root) is not the value the chain
/// is actually at, so a node taking that branch forks execution permanently.
#[tokio::test]
async fn test_seed_chain_recovery_fails_closed_on_inconsistent_cursors() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let epoch = committee.epoch();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.last().cloned().expect("fixture builds certificates");
    let leader_round = leader.round();
    let sub_dag = CommittedSubDag::new(
        certificates.clone(),
        leader,
        0,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::epoch_root(epoch),
    );

    // (Some, cursor agrees): the only sound recovery. Continue from the recovered commit.
    assert_eq!(
        resolve_seed_chain_anchor(Some(&sub_dag), leader_round, epoch),
        Ok(EpochSeedChainValue::from_committed(sub_dag.randomness())),
        "a consistent cursor pair must continue the chain from the recovered commit"
    );

    // (Some, cursor disagrees): one of the two cursors is stale.
    assert_eq!(
        resolve_seed_chain_anchor(Some(&sub_dag), leader_round + 1, epoch),
        Err(EpochSeedChainError::LeaderRoundMismatch {
            sub_dag_leader_round: leader_round,
            last_committed_round: leader_round + 1
        }),
        "a leader round that disagrees with the commit cursor must fail closed"
    );

    // (None, cursor > 0): the read-error shape. This is the branch the old `unwrap_or_default()`
    // turned into a silent re-root.
    assert_eq!(
        resolve_seed_chain_anchor(None, leader_round, epoch),
        Err(EpochSeedChainError::MissingSubDagForCommittedRound {
            last_committed_round: leader_round
        }),
        "an unreadable commit must fail closed, never re-root the chain"
    );

    // (Some, cursor == 0): a commit with no cursor pointing at it.
    assert_eq!(
        resolve_seed_chain_anchor(Some(&sub_dag), 0, epoch),
        Err(EpochSeedChainError::SubDagWithoutCommittedRound {
            sub_dag_leader_round: leader_round
        }),
        "a commit with no committed round must fail closed"
    );

    // (None, cursor == 0): the genuine first commit of an epoch starts from the epoch root.
    assert_eq!(
        resolve_seed_chain_anchor(None, 0, epoch),
        Ok(EpochSeedChainValue::epoch_root(epoch)),
        "an epoch that has committed nothing must start from its root"
    );

    // Why failing closed matters: the value the old fallback produced is NOT the value the chain
    // is at after a commit, so a node that took it would fold a different `prev` forever.
    assert_ne!(
        EpochSeedChainValue::epoch_root(epoch),
        EpochSeedChainValue::from_committed(sub_dag.randomness()),
        "re-rooting mid-epoch would diverge from the network"
    );
}

/// A node restarted mid-epoch recovers the identical seed chain value from its pack and produces
/// the identical next `randomness`.
///
/// The sub-dag is written to a real `ConsensusChain`, the handle is dropped, and a fresh handle is
/// opened on the same path - the same reader startup recovery uses. The next commit is then built
/// twice, once from the in-memory chain value and once from the recovered one, and the two must
/// agree byte for byte.
///
/// Catches: any recovery path that does not round-trip the chain value exactly - dropping
/// `randomness` from `CommittedSubDagInner`'s serialization, anchoring recovery on something other
/// than the recovered commit, or re-rooting at startup. Any of those makes the restarted node fold
/// a different `prev` into every later commit, and because the value reaches
/// `parent_beacon_block_root` the resulting execution fork is permanent.
#[tokio::test]
async fn test_seed_chain_survives_restart() {
    let temp_dir = TempDir::new().unwrap();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let epoch = committee.epoch();

    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
    let leader = certificates.last().cloned().expect("fixture builds certificates");
    let leader_round = leader.round();

    // The epoch's first commit, folded onto the epoch root.
    let first_commit = CommittedSubDag::new(
        certificates.clone(),
        leader,
        0,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::epoch_root(epoch),
    );

    // Persist it, then drop the handle: this is the crash.
    {
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();
        // Consensus output number 1: `save_consensus_output` requires each saved number to
        // strictly advance the chain's current number, and a fresh chain starts at 0.
        consensus_chain.write_subdag_for_test(1, first_commit.clone()).await;
        consensus_chain.persist_current().await.unwrap();
    }

    // Restart: a fresh handle on the same path, read the way startup recovery reads. `new` (not
    // `new_for_test`) is what a restarting node calls, so the existing epoch pack is reopened
    // rather than recreated.
    let recovered_sub_dag = {
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain
            .latest_consensus_header_from_pack(epoch)
            .await
            .expect("pack is readable after restart")
            .expect("the persisted commit is present after restart")
            .sub_dag
    };

    assert_eq!(
        recovered_sub_dag.randomness(),
        first_commit.randomness(),
        "the chain value must round-trip through the pack unchanged"
    );

    let recovered_anchor = resolve_seed_chain_anchor(Some(&recovered_sub_dag), leader_round, epoch)
        .expect("a consistent cursor pair recovers an anchor");
    assert_eq!(
        recovered_anchor,
        first_commit.seed_chain_value(),
        "recovery must resolve exactly the chain value the live node held"
    );

    // The next commit of the epoch, built from the live anchor and from the recovered one.
    let (_, next_headers) =
        fixture.headers_round(leader_round, &certificates.iter().map(|c| c.digest()).collect());
    let next_certificates: Vec<_> = next_headers.iter().map(|h| fixture.certificate(h)).collect();
    let next_leader = next_certificates.last().cloned().expect("fixture builds certificates");

    let without_restart = CommittedSubDag::new(
        next_certificates.clone(),
        next_leader.clone(),
        1,
        ReputationScores::new(&committee),
        Some(first_commit.clone()),
        first_commit.seed_chain_value(),
    );
    let after_restart = CommittedSubDag::new(
        next_certificates,
        next_leader,
        1,
        ReputationScores::new(&committee),
        Some(recovered_sub_dag),
        recovered_anchor,
    );

    assert_eq!(
        after_restart.randomness(),
        without_restart.randomness(),
        "a restarted node must produce the identical next randomness"
    );
    assert_ne!(
        after_restart.randomness(),
        first_commit.randomness(),
        "the chain must still advance after the restart"
    );
}
