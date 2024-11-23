// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::{primary::PrimaryNode, worker::WorkerNode};
use engine::{ExecutionNode, TnBuilder};
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_provider::CanonStateSubscriptions;
use tn_config::{ConsensusConfig, KeyConfig, TelcoinDirs};
use tn_network::inner_node::InnerNodeNetwork;
use tn_storage::open_db;
pub use tn_storage::NodeStorage;
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
pub async fn launch_node<DB, Evm, CE, P>(
    mut builder: TnBuilder<DB>,
    executor: Evm,
    evm_config: CE,
    tn_datadir: P,
) -> eyre::Result<()>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
    CE: ConfigureEvm,
    P: TelcoinDirs + 'static,
{
    // config for validator keys
    let config = builder.tn_config.clone();
    // adjust rpc instance ports
    builder.node_config.adjust_instance_ports();

    // TODO: impl trait for inner-node network to swap out later with WAN solution
    // trait InnerNetwork
    //
    // create the local network for engine <-> primary <-> worker
    let inner_network = InnerNodeNetwork::spawn();
    let (primary_handle, worker_handle, engine_handle) = inner_network.into_parts();

    let engine = ExecutionNode::new(builder, executor, evm_config, engine_handle)?;

    info!(target: "telcoin::node", "execution engine created");

    let narwhal_db_path = tn_datadir.narwhal_db_path();

    tracing::info!(target: "telcoin::cli", "opening node storage at {:?}", narwhal_db_path);

    // open storage for consensus
    // In case the DB dir does not yet exist.
    let _ = std::fs::create_dir_all(&narwhal_db_path);
    let db = open_db(&narwhal_db_path);
    let node_storage = NodeStorage::reopen(db);
    tracing::info!(target: "telcoin::cli", "node storage open");
    let key_config = KeyConfig::new(&tn_datadir)?;
    let consensus_config = ConsensusConfig::new(config, tn_datadir, node_storage, key_config)?;

    let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;
    let worker = WorkerNode::new(*worker_id, consensus_config.clone());
    let primary = PrimaryNode::new(consensus_config.clone());
    let mut engine_state = engine.get_provider().await.canonical_state_stream();
    let eng_bus = primary.consensus_bus().await;

    // Spawn a task to update the consensus bus with new execution blocks as they are produced.
    tokio::spawn(async move {
        while let Some(latest) = engine_state.next().await {
            let latest_num_hash = latest.tip().block.num_hash();
            eng_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(latest_num_hash));
        }
    });

    // start the primary
    primary.start(&engine).await?;

    // start the worker
    worker.start(&engine, worker_handle).await?;

    // TODO: use value from CLI
    let terminate_early = false;

    if terminate_early {
        Ok(())
    } else {
        // The pipeline has finished downloading blocks up to `--debug.tip` or
        // `--debug.max-block`. Keep other node components alive for further usage.
        futures::future::pending().await
    }
}
