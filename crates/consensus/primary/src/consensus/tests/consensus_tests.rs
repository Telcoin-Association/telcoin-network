//! Consensus tests

use crate::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule, LeaderSwapTable},
    test_utils::make_optimal_certificates,
    ConsensusBus,
};
use std::{collections::BTreeSet, sync::Arc};
use tn_storage::{mem_db::MemDatabase, CertificateStore, ConsensusStore};
use tn_test_utils::CommitteeFixture;
use tn_types::{
    Certificate, ExecHeader, Hash as _, ReputationScores, SealedHeader, TaskManager, TnReceiver,
    TnSender, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
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

    let metrics = Arc::new(ConsensusMetrics::default());
    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
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

    certificate_store.clear().unwrap();
    consensus_store.clear_consensus_chain_for_test();

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
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

    let bad_nodes_stake_threshold = 0;
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        bad_nodes_stake_threshold,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
    Consensus::spawn(config, &cb, bullshark, &TaskManager::default());

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
