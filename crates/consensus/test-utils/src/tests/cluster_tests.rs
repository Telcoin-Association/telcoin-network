// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::cluster::Cluster;
use reth::{providers::BlockReader, tasks::TaskManager};
use std::time::Duration;
use tn_types::test_utils::ensure_test_environment;

#[tokio::test]
async fn basic_cluster_setup() {
    ensure_test_environment();
    reth_tracing::init_test_tracing();
    // handle to the current runtime
    let manager = TaskManager::current();
    let executor = manager.executor();

    let mut cluster = Cluster::new(None, executor);

    // start the cluster will all the possible nodes
    cluster.start(Some(4), Some(1), None).await;

    // give some time for nodes to bootstrap and build blocks
    tokio::time::sleep(Duration::from_secs(3)).await;

    // fetch all the running authorities
    let authorities = cluster.authorities().await;

    assert_eq!(authorities.len(), 4);

    let mut block_vec = Vec::new();

    // fetch their workers transactions address
    for authority in cluster.authorities().await {
        assert_eq!(authority.worker_transaction_addresses().await.len(), 1);
        // collect first 3 executed blocks
        let el = authority.execution_components().await.expect("execution layer running");
        let db = el.get_provider().await;
        let blocks = db.block_range(0..=3).expect("at least 3 blocks");

        println!("\n\n\nblocks for authority {:?}:", authority.id);
        for block in blocks.iter() {
            println!("\n{block:?}");
        }

        block_vec.push(blocks.clone());
    }

    // assert block_vec isn't empty
    assert!(!block_vec[0].is_empty());
    // assert first three blocks are the same for all authorities
    assert!(block_vec.windows(2).all(|w| w[0] == w[1]));

    // now stop all authorities
    for id in 0..4 {
        cluster.stop_node(id).await;
    }

    println!("sleeping...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // No authority should still run
    assert!(cluster.authorities().await.is_empty());
}

// #[tokio::test]
// async fn basic_cluster_recovery() {
//     ensure_test_environment();
//     reth_tracing::init_test_tracing();
// let manager = TaskManager::current();
// let executor = manager.executor();
//     let mut cluster = Cluster::new(None, executor);

//     // start the cluster will all the possible nodes
//     cluster.start(Some(4), Some(1), None).await;

//     // give some time for nodes to bootstrap
//     tokio::time::sleep(Duration::from_secs(2)).await;

//     // fetch all the running authorities
//     let authorities = cluster.authorities().await;

//     assert_eq!(authorities.len(), 4);

//     let mut block_vec = Vec::new();

//     // fetch their workers transactions address
//     for authority in cluster.authorities().await {
//         assert_eq!(authority.worker_transaction_addresses().await.len(), 1);
//         // collect first 3 executed blocks
//         let el = authority.execution_components().await.expect("execution layer running");
//         let db = el.get_provider().await;
//         let blocks = db.block_range(0..=3).expect("at least 3 blocks");

//         println!("{:?}\n\n\n", authority.id);
//         for block in blocks.iter() {
//             println!("\n{block:?}");
//         }

//         block_vec.push(blocks.clone());
//     }

//     // assert first three blocks are the same for all authorities
//     assert!(block_vec.windows(2).all(|w| w[0] == w[1]));

//     let last_finalized_block = {
//         let engine_0 = cluster.authority(0).execution_components().await.expect("validator0
// engine available");         let db0 = engine_0.get_provider().await;
//         db0.finalized_block_number().expect("finalized block number available");
//     };

//     println!("\nlast finalized block:\n\n{last_finalized_block:?}\n");

//     // stop validator 0
//     cluster.stop_node(0).await;

//     // ensure progress made by other validators
//     println!("sleeping...");
//     tokio::time::sleep(Duration::from_secs(3)).await;

//     cluster.start_node(0, preserve_store, workers_per_authority)

//     // No authority should still run
//     assert!(cluster.authorities().await.is_empty());
// }
