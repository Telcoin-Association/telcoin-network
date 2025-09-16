// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![warn(unused_crate_dependencies)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::TelcoinDirs;
use tn_primary::ConsensusBus;
use tn_rpc::EngineToPrimary;
use tn_storage::tables::{
    ConsensusBlockNumbersByDigest, ConsensusBlocks, EpochCerts, EpochRecords, EpochRecordsIndex,
};
use tn_types::{BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord};
use tokio::runtime::Builder;
use tracing::{instrument, warn};

pub mod engine;
mod error;
mod manager;
pub mod primary;
pub mod worker;
pub use manager::catchup_accumulator;

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
    P: TelcoinDirs + Clone + 'static,
{
    let runtime = Builder::new_multi_thread()
        .thread_name("telcoin-network")
        .enable_io()
        .enable_time()
        .build()?;

    // run the node
    let res = runtime.block_on(async move {
        let consensus_db = manager::open_consensus_db(&tn_datadir)?;
        // create the epoch manager
        let mut epoch_manager = EpochManager::new(builder, tn_datadir, passphrase, consensus_db)?;
        epoch_manager.run().await
    });

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

    /// Retrieve the consensus header by number.
    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)> {
        let record = self.db.get::<EpochRecords>(&epoch).ok()??;
        let digest = record.digest();
        Some((record, self.db.get::<EpochCerts>(&digest).ok()??))
    }

    /// Retrieve the consensus header by hash
    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
        let epoch = self.db.get::<EpochRecordsIndex>(&hash).ok()??;
        let record = self.db.get::<EpochRecords>(&epoch).ok()??;
        let digest = record.digest();
        Some((record, self.db.get::<EpochCerts>(&digest).ok()??))
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

    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash),
            (Some(epoch), _) => self.get_epoch_by_number(epoch),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}
