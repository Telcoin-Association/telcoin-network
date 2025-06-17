// SPDX-License-Identifier: Apache-2.0
//! Library for managing all components used by a full-node in a single process.

#![warn(unused_crate_dependencies)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::TelcoinDirs;
use tn_primary::ConsensusBus;
use tn_rpc::EngineToPrimary;
use tn_storage::tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks};
use tn_types::{BlockHash, ConsensusHeader, Database};
use tokio::runtime::Builder;
use tracing::{instrument, warn};

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
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    passphrase: Option<String>,
) -> eyre::Result<()>
where
    P: TelcoinDirs + 'static,
{
    let runtime = Builder::new_multi_thread()
        .thread_name("telcoin-network")
        .enable_io()
        .enable_time()
        .build()?;

    // create the epoch manager
    let mut epoch_manager = EpochManager::new(builder, tn_datadir, passphrase)?;
    // run the node
    let res = runtime.block_on(async move { epoch_manager.run().await });

    // shutdown background tasks
    runtime.shutdown_background();

    // return result after shutdown
    res
}

pub struct EngineToPrimaryRpc<DB> {
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// Consensus DB
    db: DB,
}

impl<DB: Database> EngineToPrimaryRpc<DB> {
    pub fn new(consensus_bus: ConsensusBus, db: DB) -> Self {
        Self { consensus_bus, db }
    }
}

impl<DB: Database> EngineToPrimary for EngineToPrimaryRpc<DB> {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        self.consensus_bus.last_consensus_header().borrow().clone()
    }

    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        self.db.get::<ConsensusBlocks>(&number).ok().flatten()
    }

    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        let number = self.db.get::<ConsensusBlockNumbersByDigest>(&hash).ok().flatten()?;
        self.db.get::<ConsensusBlocks>(&number).ok().flatten()
    }
}
