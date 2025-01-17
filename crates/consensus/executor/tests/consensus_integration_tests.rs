//! IT tests

use fastcrypto::hash::Hash;
use std::{collections::BTreeSet, sync::Arc};
use tn_executor::get_restored_consensus_output;
use tn_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule, LeaderSwapTable},
    ConsensusBus,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{Certificate, TaskManager, TnReceiver, TnSender, DEFAULT_BAD_NODES_STAKE_THRESHOLD};

#[tokio::test]
async fn test_recovery() {
    // Setup consensus
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let config_1 = fixture.authorities().next().unwrap().consensus_config();
    let consensus_store = config_1.node_storage().consensus_store.clone();
    let certificate_store = config_1.node_storage().certificate_store.clone();
    let committee = fixture.committee();

    // Make certificates for rounds 1 up to 4.
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) =
        tn_test_utils::make_optimal_certificates(&committee, 1..=4, &genesis, &ids);

    // Make two certificate (f+1) with round 5 to trigger the commits.
    let (_, certificate) =
        tn_test_utils::mock_certificate(&committee, ids[0], 5, next_parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) = tn_test_utils::mock_certificate(&committee, ids[1], 5, next_parents);
    certificates.push_back(certificate);

    const NUM_SUB_DAGS_PER_SCHEDULE: u64 = 100;
    let metrics = Arc::new(ConsensusMetrics::default());
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        metrics.clone(),
        NUM_SUB_DAGS_PER_SCHEDULE,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let cb_clone = cb.clone();
    let mut rx_output = cb.sequence().subscribe();
    // pretend we are synced and ready to go so test can run...
    cb.node_mode().send(tn_primary::NodeMode::Cvv).unwrap();
    Consensus::spawn(config_1, &cb, bullshark, &TaskManager::default());
    tokio::spawn(async move {
        let mut rx_primary = cb_clone.committed_certificates().subscribe();
        while rx_primary.recv().await.is_some() {}
    });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        // we store the certificates so we can enable the recovery
        // mechanism later.
        certificate_store.write(certificate.clone()).unwrap();
        cb.new_certificates().send(certificate).await.unwrap();
    }

    let expected_committed_sub_dags = 2;
    for i in 1..=expected_committed_sub_dags {
        let sub_dag = rx_output.recv().await.unwrap();
        assert_eq!(sub_dag.sub_dag_index, i);
    }

    // Now assume that we want to recover from a crash. We are testing all the recovery cases
    // from restoring the executed sub dag index = 0 up to 2.
    for last_executed_certificate_index in 0..=expected_committed_sub_dags {
        let consensus_output = get_restored_consensus_output(
            consensus_store.clone(),
            certificate_store.clone(),
            // &execution_state,
            last_executed_certificate_index,
        )
        .await
        .expect("consensus output is restored from storage");

        assert_eq!(consensus_output.len(), (2 - last_executed_certificate_index) as usize);
    }
}
