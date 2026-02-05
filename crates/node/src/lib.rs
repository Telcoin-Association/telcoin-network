// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use std::sync::OnceLock;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::{ConsensusBus, NodeMode};
use tn_rpc::{CommitteeInfo, EngineToPrimary, EpochInfo, SyncProgress, SyncStatus, ValidatorInfo};
use tn_storage::{tables::EpochRecords, ConsensusStore, EpochStore};
use tn_types::{BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord};
use tokio::task::JoinHandle;

pub mod engine;
mod error;
mod health;
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
    /// The block number when sync started, captured on first syncing status check.
    /// Derived from the last EpochRecord's parent_state as specified in issue #530.
    sync_starting_block: OnceLock<u64>,
}

impl<DB: Database> EngineToPrimaryRpc<DB> {
    pub fn new(consensus_bus: ConsensusBus, db: DB) -> Self {
        Self { consensus_bus, db, sync_starting_block: OnceLock::new() }
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

    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_number(number)
    }

    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_hash(hash)
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

    fn sync_status(&self) -> SyncStatus {
        let mode = *self.consensus_bus.node_mode().borrow();
        if matches!(mode, NodeMode::CvvActive) {
            return SyncStatus::Synced;
        }

        // Block-level progress: compare executed blocks vs known consensus blocks.
        let current_block = self.consensus_bus.latest_block_num_hash().number;
        let highest_block = self
            .consensus_bus
            .last_consensus_header()
            .borrow()
            .as_ref()
            .map(|h| h.number)
            .unwrap_or(current_block);
        let execution_synced = current_block >= highest_block;

        // Epoch-based sync decision: compare network epoch (from committee) vs local epoch (from
        // DB). This follows the issue #530 spec: "A node is considered synced when its
        // execution state is within the current consensus epoch."
        let network_epoch =
            self.consensus_bus.current_committee().borrow().as_ref().map(|c| c.epoch());

        let local_epoch =
            self.db.last_record::<EpochRecords>().map(|(epoch, _)| epoch).unwrap_or(0);

        // Synced only when both epoch AND execution are caught up.
        // A node can have consensus data for the current epoch but execution may still
        // lag behind — we must wait for execution to complete before declaring synced.
        if network_epoch.is_some_and(|net| local_epoch >= net) && execution_synced {
            return SyncStatus::Synced;
        }

        // Fallback when committee info is unavailable (e.g. early startup):
        // use block numbers to determine sync status.
        if network_epoch.is_none() && execution_synced {
            return SyncStatus::Synced;
        }

        // Capture starting_block once from the last completed EpochRecord's
        // parent_state, as specified in issue #530.
        let starting_block = *self.sync_starting_block.get_or_init(|| {
            self.db
                .last_record::<EpochRecords>()
                .map(|(_, record)| record.parent_state.number)
                .unwrap_or(0)
        });

        let current_epoch = local_epoch as u64;
        let highest_epoch = network_epoch.unwrap_or(local_epoch) as u64;

        SyncStatus::Syncing(SyncProgress {
            starting_block,
            current_block,
            highest_block,
            current_epoch,
            highest_epoch,
        })
    }

    fn current_epoch_info(&self) -> Option<EpochInfo> {
        let (_, record) = self.db.last_record::<EpochRecords>()?;
        Some(record.into())
    }

    fn current_committee(&self) -> Option<CommitteeInfo> {
        let committee = self.consensus_bus.current_committee().borrow().clone()?;
        let epoch = committee.epoch();
        let validators = committee
            .authorities()
            .iter()
            .map(|a| ValidatorInfo {
                bls_public_key: *a.protocol_key(),
                address: a.execution_address(),
                voting_power: a.voting_power(),
            })
            .collect();
        Some(CommitteeInfo { epoch, validators })
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_primary::ConsensusBus;
    use tn_rpc::EngineToPrimary;
    use tn_storage::{mem_db::MemDatabase, tables::EpochRecords};
    use tn_types::{
        BlockNumHash, BlsKeypair, CommitteeBuilder, ConsensusHeader, Database, ExecHeader,
        SealedHeader, B256,
    };

    /// Helper to create a SealedHeader with a specific block number.
    fn sealed_header_with_number(number: u64) -> SealedHeader {
        let mut header = ExecHeader::default();
        header.number = number;
        SealedHeader::new(header, B256::default())
    }

    /// Helper to create a ConsensusHeader with a specific block number.
    fn consensus_header_with_number(number: u64) -> ConsensusHeader {
        ConsensusHeader { number, ..Default::default() }
    }

    /// Helper to create a Committee with a specific epoch.
    fn committee_with_epoch(epoch: u32) -> tn_types::Committee {
        let mut rng = rand::rng();
        let kp1 = BlsKeypair::generate(&mut rng);
        let kp2 = BlsKeypair::generate(&mut rng);
        let mut builder = CommitteeBuilder::new(epoch);
        builder.add_authority(*kp1.public(), 1, tn_types::Address::repeat_byte(0));
        builder.add_authority(*kp2.public(), 1, tn_types::Address::repeat_byte(1));
        builder.build()
    }

    /// Helper to build the RPC handler with specific state.
    fn setup_rpc(
        mode: NodeMode,
        committee_epoch: Option<u32>,
        exec_block: u64,
        consensus_block: Option<u64>,
        db_epoch: Option<(u32, u64)>, // (epoch_number, parent_state_block_number)
    ) -> EngineToPrimaryRpc<MemDatabase> {
        let bus = ConsensusBus::new();

        // set node mode
        let _ = bus.node_mode().send(mode);

        // set committee
        if let Some(epoch) = committee_epoch {
            let committee = committee_with_epoch(epoch);
            let _ = bus.current_committee().send(Some(committee));
        }

        // set execution block (via recent_blocks)
        if exec_block > 0 {
            let header = sealed_header_with_number(exec_block);
            bus.recent_blocks().send_modify(|blocks| blocks.push_latest(header));
        }

        // set consensus header
        if let Some(num) = consensus_block {
            let _ = bus.last_consensus_header().send(Some(consensus_header_with_number(num)));
        }

        // set up DB with epoch record
        let db = MemDatabase::default();
        if let Some((epoch, parent_block)) = db_epoch {
            let record = EpochRecord {
                epoch,
                parent_state: BlockNumHash { number: parent_block, hash: Default::default() },
                ..Default::default()
            };
            let _ = db.insert::<EpochRecords>(&epoch, &record);
        }

        EngineToPrimaryRpc::new(bus, db)
    }

    #[tokio::test]
    async fn sync_status_cvv_active_always_synced() {
        let rpc = setup_rpc(NodeMode::CvvActive, None, 0, None, None);
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_cvv_active_synced_even_when_behind() {
        // CvvActive should return Synced regardless of block/epoch state
        let rpc = setup_rpc(NodeMode::CvvActive, Some(10), 100, Some(500), Some((5, 50)));
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_epoch_and_execution_synced() {
        // local epoch >= network epoch AND execution caught up
        let rpc = setup_rpc(NodeMode::Observer, Some(5), 1000, Some(1000), Some((5, 500)));
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_epoch_synced_but_execution_behind() {
        // This is the bug fix: epoch matches but execution is behind consensus
        let rpc = setup_rpc(NodeMode::Observer, Some(5), 800, Some(1000), Some((5, 500)));
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 800);
                assert_eq!(progress.highest_block, 1000);
                assert_eq!(progress.current_epoch, 5);
                assert_eq!(progress.highest_epoch, 5);
            }
            SyncStatus::Synced => panic!("should be Syncing when execution is behind"),
        }
    }

    #[tokio::test]
    async fn sync_status_epoch_behind() {
        // local epoch < network epoch
        let rpc = setup_rpc(NodeMode::Observer, Some(10), 500, Some(1000), Some((7, 300)));
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 500);
                assert_eq!(progress.highest_block, 1000);
                assert_eq!(progress.current_epoch, 7);
                assert_eq!(progress.highest_epoch, 10);
            }
            SyncStatus::Synced => panic!("should be Syncing when epoch is behind"),
        }
    }

    #[tokio::test]
    async fn sync_status_no_committee_execution_synced() {
        // No committee info but execution caught up to consensus
        let rpc = setup_rpc(NodeMode::Observer, None, 1000, Some(1000), None);
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_no_committee_execution_behind() {
        // No committee info and execution behind consensus
        let rpc = setup_rpc(NodeMode::Observer, None, 500, Some(1000), None);
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 500);
                assert_eq!(progress.highest_block, 1000);
                assert_eq!(progress.current_epoch, 0);
                assert_eq!(progress.highest_epoch, 0);
            }
            SyncStatus::Synced => panic!("should be Syncing when execution is behind"),
        }
    }

    #[tokio::test]
    async fn sync_status_starting_block_from_epoch_record() {
        // starting_block should come from last EpochRecord's parent_state.number
        let rpc = setup_rpc(NodeMode::Observer, Some(10), 500, Some(1000), Some((7, 300)));
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.starting_block, 300);
            }
            SyncStatus::Synced => panic!("should be Syncing"),
        }
    }

    #[tokio::test]
    async fn sync_status_starting_block_zero_when_no_epoch_record() {
        // starting_block should be 0 when no EpochRecord exists
        let rpc = setup_rpc(NodeMode::Observer, Some(5), 500, Some(1000), None);
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.starting_block, 0);
            }
            SyncStatus::Synced => panic!("should be Syncing"),
        }
    }

    #[tokio::test]
    async fn sync_status_no_consensus_header_uses_current_block() {
        // When no consensus header exists, highest_block defaults to current_block
        let rpc = setup_rpc(NodeMode::Observer, None, 500, None, None);
        // No consensus header and no committee → execution_synced (500 >= 500) → Synced
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_cvv_inactive_can_be_syncing() {
        // CvvInactive should report syncing when behind
        let rpc = setup_rpc(NodeMode::CvvInactive, Some(5), 500, Some(1000), Some((3, 200)));
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 500);
                assert_eq!(progress.highest_block, 1000);
                assert_eq!(progress.current_epoch, 3);
                assert_eq!(progress.highest_epoch, 5);
                assert_eq!(progress.starting_block, 200);
            }
            SyncStatus::Synced => panic!("CvvInactive should be Syncing when behind"),
        }
    }
}
