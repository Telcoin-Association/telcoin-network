// SPDX-License-Identifier: Apache-2.0
//! Library for managing all components used by a full-node in a single process.

use crate::{primary::PrimaryNode, worker::WorkerNode};
use consensus_metrics::start_prometheus_server;
use engine::{ExecutionNode, TnBuilder};
use manager::EpochManager;
use std::{str::FromStr as _, sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{ConsensusNetwork, PeerId};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{RethDb, RethEnv};
use tn_storage::{open_db, tables::ConsensusBlocks, DatabaseType};
use tn_types::{BatchValidation, ConsensusHeader, Database as TNDatabase, Multiaddr, TaskManager};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::{runtime::Builder, sync::mpsc};
use tracing::{info, instrument, warn};

pub mod engine;
mod error;
mod manager;
pub mod primary;
pub mod worker;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
#[instrument(level = "info", skip_all)]
pub fn launch_node<P>(builder: TnBuilder, tn_datadir: P) -> eyre::Result<()>
where
    P: TelcoinDirs + 'static,
{
    let consensus_db_path = tn_datadir.consensus_db_path();

    tracing::info!(target: "telcoin::node", "opening node storage at {:?}", consensus_db_path);

    // open storage for consensus
    // In case the DB dir does not yet exist.
    let _ = std::fs::create_dir_all(&consensus_db_path);
    // // Create both of the MDBX DBs and hold onto them.
    // // MDBX seems to have issues occasionally if recreated on a relaunch...
    // let db = open_db(&consensus_db_path);
    // let reth_db = RethEnv::new_database(&builder.node_config, tn_datadir.reth_db_path())?;

    // create the epoch manager
    let mut epoch_manager = EpochManager::new(builder, tn_datadir)?;
    epoch_manager.run()
}
