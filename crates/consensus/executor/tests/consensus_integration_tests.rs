// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use fastcrypto::hash::Hash;
use narwhal_executor::get_restored_consensus_output;
use narwhal_primary::{
    consensus::{Bullshark, Consensus, ConsensusMetrics, LeaderSchedule, LeaderSwapTable},
    ConsensusBus,
};

use narwhal_test_utils::CommitteeFixture;
use tn_storage::mem_db::MemDatabase;
use tn_types::DEFAULT_BAD_NODES_STAKE_THRESHOLD;

use std::{collections::BTreeSet, sync::Arc};

use tn_types::{Certificate, TnReceiver, TnSender};

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
        narwhal_test_utils::make_optimal_certificates(&committee, 1..=4, &genesis, &ids);

    // Make two certificate (f+1) with round 5 to trigger the commits.
    let (_, certificate) =
        narwhal_test_utils::mock_certificate(&committee, ids[0], 5, next_parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) =
        narwhal_test_utils::mock_certificate(&committee, ids[1], 5, next_parents);
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
    let _consensus_handle = Consensus::spawn(config_1, &cb, bullshark);
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

// TODO: this needs to work again
// #[tokio::test]
// async fn test_internal_consensus_output() {
//     // Enabled debug tracing so we can easily observe the
//     // nodes logs.
//     let _guard = setup_test_tracing();

// let manager = TaskManager::current();
// let executor = manager.executor();
//     let mut cluster = Cluster::new(None, executor);

//     // start the cluster
//     cluster.start(Some(4), Some(1), None).await;

//     // get a client to send transactions
//     let worker_id = 0;

//     let authority = cluster.authority(0);
//     // let mut client = authority.new_transactions_client(&worker_id).await;

//     // Subscribe to the transaction confirmation channel
//     let mut receiver = authority.primary().await.tx_transaction_confirmation.subscribe();

//     // Create arbitrary transactions
//     // let mut transactions = Vec::new();

//     const NUM_OF_TRANSACTIONS: u32 = 10;
//     // for i in 0..NUM_OF_TRANSACTIONS {
//     //     let tx = string_transaction(i);

//     //     // serialise and send
//     //     let tr = bcs::to_bytes(&tx).unwrap();
//     //     let txn = TransactionProto { transaction: Bytes::from(tr) };
//     //     client.submit_transaction(txn).await.unwrap();

//     //     transactions.push(tx);
//     // }

//     // wait for transactions to complete
//     loop {
//         let result = receiver.recv().await.unwrap();

//         // deserialise transaction
//         let output_transaction = bcs::from_bytes::<String>(&result).unwrap();

//         // we always remove the first transaction and check with the one
//         // sequenced. We want the transactions to be sequenced in the
//         // same order as we post them.
//         let expected_transaction = transactions.remove(0);

//         assert_eq!(
//             expected_transaction, output_transaction,
//             "Expected to have received transaction with same id. Ordering is important"
//         );

//         if transactions.is_empty() {
//             break;
//         }
//     }
// }

// // fn string_transaction(id: u32) -> String {
// //     format!("test transaction:{id}")
// // }
