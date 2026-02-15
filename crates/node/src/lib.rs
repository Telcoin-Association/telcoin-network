// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::ConsensusBus;
use tn_rpc::EngineToPrimary;
use tn_storage::EpochStore;
use tn_types::{BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord};
use tokio::task::JoinHandle;

pub mod engine;
mod error;
mod health;
mod manager;
pub mod primary;
pub mod worker;
pub use manager::catchup_accumulator;

#[cfg(test)]
use tempfile as _;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    key_config: KeyConfig,
) -> JoinHandle<eyre::Result<()>>
where
    P: TelcoinDirs + Clone + 'static,
{
    let consensus_db = manager::open_consensus_db(&tn_datadir);

    // create the epoch manager
    let mut epoch_manager = EpochManager::new(builder, tn_datadir, consensus_db, key_config);
    // run the node
    // Note this is the "entry task" for the node and the caller needs to wait on the JoinHandle
    // then exit.
    tokio::spawn(async move { epoch_manager.run().await })
}

#[derive(Debug)]
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
        if let Some((r, Some(c))) = self.db.get_epoch_by_number(epoch) {
            Some((r, c))
        } else {
            None
        }
    }

    /// Retrieve the consensus header by hash
    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.db.get_epoch_by_hash(hash) {
            Some((r, c))
        } else {
            None
        }
    }
}

impl<DB: Database> EngineToPrimary for EngineToPrimaryRpc<DB> {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        self.consensus_bus.last_consensus_header().borrow().clone().unwrap_or_default()
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
