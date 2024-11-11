// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use narwhal_test_utils::cluster::Cluster;
use prometheus::core::Metric;
use reth::tasks::TaskManager;
use tn_storage::mem_db::MemDatabase;

use narwhal_test_utils::setup_test_tracing;
use std::time::Duration;
use tracing::info;

#[ignore]
#[tokio::test]
async fn test_restore_from_disk() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    setup_test_tracing();

    let manager = TaskManager::current();
    let executor = manager.executor();
    let mut cluster = Cluster::new(executor, MemDatabase::default);

    // start the cluster
    cluster.start(Some(4), Some(1), None).await;

    let _id = 0;

    // TODO: setup cluster with rpc / tx pool

    // let client = cluster.authority(0).new_transactions_client(&id).await;

    // // Subscribe to the transaction confirmation channel
    // let mut receiver =
    // cluster.authority(0).primary().await.tx_transaction_confirmation.subscribe();

    // // Create arbitrary transactions
    // let mut total_tx = 3;
    // for tx in [string_transaction(), string_transaction(), string_transaction()] {
    //     let mut c = client.clone();
    //     tokio::spawn(async move {
    //         let tr = bcs::to_bytes(&tx).unwrap();
    //         let txn = TransactionProto { transaction: Bytes::from(tr) };

    //         c.submit_transaction(txn).await.unwrap();
    //     });
    // }

    // // wait for transactions to complete
    // loop {
    //     if let Ok(_result) = receiver.recv().await {
    //         total_tx -= 1;
    //         if total_tx < 1 {
    //             break;
    //         }
    //     }
    // }

    // Now stop node 0
    cluster.stop_node(0).await;

    // Let other primaries advance and primary 0 releases its port.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Now start the node 0 again
    cluster.start_node(0, true, Some(1)).await.expect("cluster started node 0");

    // Let the node recover
    tokio::time::sleep(Duration::from_secs(2)).await;

    let node = cluster.authority(0);

    // Check the metrics to ensure the node was recovered from disk
    let primary = node.primary().await;

    let node_recovered_state =
        primary.consensus_metrics().await.recovered_consensus_state.get() > 0;

    assert!(node_recovered_state, "Node did not recover state from disk");
}

#[ignore]
#[tokio::test]
async fn test_read_causal_signed_certificates() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    setup_test_tracing();

    let manager = TaskManager::current();
    let executor = manager.executor();
    let mut cluster = Cluster::new(executor, MemDatabase::default);

    // start the cluster
    cluster.start(Some(4), Some(1), None).await;

    // Let primaries advance little bit
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Ensure all nodes advanced
    for authority in cluster.authorities().await {
        let value = authority
            .primary()
            .await
            .primary_metrics()
            .await
            .node_metrics
            .current_round
            .metric()
            .get_gauge()
            .get_value();
        info!("Metric -> {:?}", value);
        // If the current round is increasing then it means that the
        // node starts catching up and is proposing.
        assert!(value > 1.0, "Node didn't progress further than the round 1");
    }

    // Now stop node 0
    cluster.stop_node(0).await;

    // Let other primaries advance and primary 0 releases its port.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Now start the validator 0 again
    cluster.start_node(0, true, Some(1)).await.expect("cluster started node 0");

    // Now check that the current round advances. Give the opportunity with a few
    // iterations. If metric hasn't picked up then we know that node can't make
    // progress.
    let mut node_made_progress = false;
    let node = cluster.authority(0).primary().await;

    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let value = node
            .primary_metrics()
            .await
            .node_metrics
            .current_round
            .metric()
            .get_gauge()
            .get_value();
        info!("Metric -> {:?}", value);
        // If the current round is increasing then it means that the
        // node starts catching up and is proposing.
        if value > 1.0 {
            node_made_progress = true;
            break;
        }
    }

    assert!(node_made_progress, "Node 0 didn't make progress - causal completion didn't succeed");
}
