// SPDX-License-Identifier: Apache-2.0
//! Library for managing all components used by a full-node in a single process.

use crate::{primary::PrimaryNode, worker::WorkerNode};
use engine::{ExecutionNode, RethDB, TelcoinNode, TnBuilder};
use futures::StreamExt;
use reth_db::{
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    Database,
};
use reth_provider::{
    providers::{ProviderNodeTypes, TreeNodeTypes},
    CanonStateSubscriptions,
};
use tn_config::{ConsensusConfig, KeyConfig, TelcoinDirs};
use tn_primary::NodeMode;
use tn_storage::open_db;
pub use tn_storage::NodeStorage;
use tn_types::{BlockExt as _, TaskManager};
use tracing::{info, instrument};

pub mod dirs;
pub mod engine;
mod error;
pub mod metrics;
pub mod primary;
pub mod worker;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
#[instrument(level = "info", skip_all)]
pub async fn launch_node<DB, P>(mut builder: TnBuilder<DB>, tn_datadir: P) -> eyre::Result<()>
where
    DB: RethDB,
    P: TelcoinDirs + 'static,
{
    // config for validator keys
    let config = builder.tn_config.clone();
    // adjust rpc instance ports
    builder.node_config.adjust_instance_ports();
    let mut task_manager = TaskManager::new("Task Manager");
    let mut engine_task_manager = TaskManager::new("Engine Task Manager");
    let engine = ExecutionNode::<TelcoinNode<DB>>::new(builder, &engine_task_manager)?;

    info!(target: "telcoin::node", "execution engine created");

    let consensus_db_path = tn_datadir.consensus_db_path();

    tracing::info!(target: "telcoin::cli", "opening node storage at {:?}", consensus_db_path);

    // open storage for consensus
    // In case the DB dir does not yet exist.
    let _ = std::fs::create_dir_all(&consensus_db_path);
    let db = open_db(&consensus_db_path);
    let node_storage = NodeStorage::reopen(db);
    tracing::info!(target: "telcoin::cli", "node storage open");
    let key_config = KeyConfig::new(&tn_datadir)?;
    let consensus_config = ConsensusConfig::new(config, tn_datadir, node_storage, key_config)?;

    let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;
    let worker = WorkerNode::new(*worker_id, consensus_config.clone());
    let primary = PrimaryNode::new(consensus_config.clone());

    let mut engine_state = engine.get_provider().await.canonical_state_stream();
    let eng_bus = primary.consensus_bus().await;

    // Prime the recent_blocks watch with latest executed blocks.
    let block_capacity = eng_bus.recent_blocks().borrow().block_capacity();
    for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
        eng_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(recent_block));
    }

    if tn_executor::subscriber::can_cvv(
        eng_bus.clone(),
        consensus_config.clone(),
        primary.network().await,
    )
    .await
    {
        eng_bus.node_mode().send_modify(|v| *v = NodeMode::CvvActive);
    } else {
        eng_bus.node_mode().send_modify(|v| *v = NodeMode::CvvInactive);
    }

    // Spawn a task to update the consensus bus with new execution blocks as they are produced.
    let latest_block_shutdown = consensus_config.shutdown().subscribe();
    task_manager.spawn_task("latest block", async move {
        loop {
            tokio::select!(
                _ = &latest_block_shutdown => {
                    break;
                }
                latest = engine_state.next() => {
                    if let Some(latest) = latest {
                        eng_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(latest.tip().block.header.clone()));
                    } else {
                        break;
                    }
                }
            )
        }
    });

    // create receiving channel before spawning primary to ensure messages are not lost
    let consensus_output_rx = primary.consensus_bus().await.subscribe_consensus_output();

    // start the primary
    let mut primary_task_manager = primary.start().await?;

    let validator = engine.new_batch_validator().await;
    // start the worker
    let (mut worker_task_manager, block_provider) = worker.start(validator).await?;

    // start engine
    engine
        .start_engine(
            consensus_output_rx,
            &engine_task_manager,
            consensus_config.shutdown().subscribe(),
        )
        .await?;
    // spawn block maker for worker
    engine
        .start_batch_builder(
            *worker_id,
            block_provider.batches_tx(),
            &engine_task_manager,
            consensus_config.shutdown().subscribe(),
        )
        .await?;

    primary_task_manager.update_tasks();
    task_manager.add_task_manager(primary_task_manager);
    worker_task_manager.update_tasks();
    task_manager.add_task_manager(worker_task_manager);
    engine_task_manager.update_tasks();
    task_manager.add_task_manager(engine_task_manager);

    info!(target:"tn", tasks=?task_manager, "TASKS");

    task_manager.join_until_exit(consensus_config.shutdown().clone()).await;
    Ok(())
}
