// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use std::sync::OnceLock;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::{ConsensusBus, NodeMode};
use tn_reth::RethEnv;
use tn_rpc::{CommitteeInfo, EngineToPrimary, EpochInfo, SyncProgress, SyncStatus, ValidatorInfo};
use tn_storage::{tables::EpochRecords, ConsensusStore, EpochStore};
use tn_types::{BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord, B256};
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
    /// Access to execution canonical state.
    reth_env: RethEnv,
    /// The block number when sync started, captured on first syncing status check.
    /// Derived from the last EpochRecord's parent_consensus hash.
    sync_starting_block: OnceLock<u64>,
}

impl<DB: Database> EngineToPrimaryRpc<DB> {
    pub fn new(consensus_bus: ConsensusBus, db: DB, reth_env: RethEnv) -> Self {
        Self { consensus_bus, db, reth_env, sync_starting_block: OnceLock::new() }
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

        // Consensus progress:
        // - current_block tracks the latest consensus header we have locally.
        // - highest_block tracks the latest known consensus header across gossip/local state.
        let current_header = self.consensus_bus.last_consensus_header().borrow().clone();
        let current_block = current_header.as_ref().map(|h| h.number).unwrap_or_default();
        let highest_block = self.consensus_bus.published_consensus_num_hash().0.max(current_block);
        let consensus_synced = current_block >= highest_block;

        // Execution progress:
        // compare the last consensus round processed by execution against the latest known local
        // consensus header round. This remains valid even when consensus outputs skip EVM blocks.
        let current_consensus_round =
            current_header.as_ref().map(|h| h.sub_dag.leader_round()).unwrap_or_default();
        let processed_consensus_round = self.consensus_bus.last_consensus_round();
        let execution_synced = processed_consensus_round >= current_consensus_round;

        // Epoch progress:
        // - network_epoch: highest downloaded epoch record + 1.
        // - local_epoch: current epoch read from execution canonical tip.
        let network_epoch = self.db.last_record::<EpochRecords>().map_or(0, |(epoch, record)| {
            if epoch == 0 && record.parent_consensus == B256::default() {
                0
            } else {
                epoch.saturating_add(1)
            }
        });
        let local_epoch =
            self.reth_env.epoch_state_from_canonical_tip().map(|state| state.epoch).unwrap_or_else(
                |_| {
                    self.consensus_bus
                        .current_committee()
                        .borrow()
                        .as_ref()
                        .map(|committee| committee.epoch())
                        .unwrap_or_default()
                },
            );

        // Synced only when epoch, consensus, and execution progress are all caught up.
        if local_epoch >= network_epoch && consensus_synced && execution_synced {
            return SyncStatus::Synced;
        }

        // Capture starting_block once from the last completed EpochRecord parent_consensus.
        let starting_block = *self.sync_starting_block.get_or_init(|| {
            self.db
                .last_record::<EpochRecords>()
                .and_then(|(_, record)| {
                    self.db
                        .get_consensus_by_hash(record.parent_consensus)
                        .map(|header| header.number)
                })
                .unwrap_or_default()
        });

        let execution_block_height = self.reth_env.canonical_tip().number;
        let current_epoch = local_epoch as u64;
        let highest_epoch = network_epoch as u64;

        SyncStatus::Syncing(SyncProgress {
            starting_block,
            current_block,
            highest_block,
            execution_block_height,
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
    use tn_primary::{test_utils::temp_dir, ConsensusBus};
    use tn_reth::RethEnv;
    use tn_rpc::EngineToPrimary;
    use tn_storage::{mem_db::MemDatabase, tables::EpochRecords, ConsensusStore};
    use tn_types::{CommittedSubDag, ConsensusHeader, Database, TaskManager, B256};

    fn write_consensus_header(
        db: &MemDatabase,
        number: u64,
        epoch: u32,
        round: u32,
    ) -> ConsensusHeader {
        let mut sub_dag = CommittedSubDag::default();
        sub_dag.leader.header.epoch = epoch;
        sub_dag.leader.header.update_round_for_test(round);
        db.write_subdag_for_test(number, sub_dag);
        db.get_consensus_by_number(number).expect("consensus header should be present")
    }

    /// Build the RPC handler with configurable consensus and epoch-sync state.
    fn setup_rpc(
        mode: NodeMode,
        consensus_header: Option<(u64, u32, u32)>, // (number, epoch, round)
        published_consensus_number: Option<u64>,
        processed_consensus_round: u32,
        db_epoch: Option<u32>,
        db_parent_consensus_number: Option<u64>,
    ) -> EngineToPrimaryRpc<MemDatabase> {
        let bus = ConsensusBus::new();
        let db = MemDatabase::default();

        // set node mode
        bus.node_mode().send_replace(mode);

        let latest_consensus_hash = if let Some((number, epoch, round)) = consensus_header {
            let mut header = ConsensusHeader { number, ..Default::default() };
            header.sub_dag.leader.header.epoch = epoch;
            header.sub_dag.leader.header.update_round_for_test(round);
            bus.last_consensus_header().send_replace(Some(header.clone()));
            header.digest()
        } else {
            B256::default()
        };

        if let Some(number) = published_consensus_number {
            bus.last_published_consensus_num_hash().send_replace((number, B256::default()));
        }

        // Track execution progress via processed consensus rounds.
        bus.recent_blocks().send_modify(|blocks| {
            blocks.push_latest(processed_consensus_round, latest_consensus_hash, None)
        });

        // set up DB with epoch record
        if let Some(epoch) = db_epoch {
            let mut record = EpochRecord { epoch, ..Default::default() };
            if let Some(parent_consensus_number) = db_parent_consensus_number {
                let parent_header = write_consensus_header(
                    &db,
                    parent_consensus_number,
                    epoch.saturating_sub(1),
                    0,
                );
                record.parent_consensus = parent_header.digest();
            }
            db.insert::<EpochRecords>(&epoch, &record).expect("insert epoch record");
        }

        let tmp = temp_dir();
        let reth_env =
            RethEnv::new_for_test(tmp.path().join("reth"), &TaskManager::default(), None)
                .expect("reth env for test");

        EngineToPrimaryRpc::new(bus, db, reth_env)
    }

    fn fresh_local_epoch() -> u32 {
        let tmp = temp_dir();
        let reth_env =
            RethEnv::new_for_test(tmp.path().join("reth"), &TaskManager::default(), None)
                .expect("reth env for epoch probe");
        reth_env.epoch_state_from_canonical_tip().expect("epoch state from canonical tip").epoch
    }

    #[tokio::test]
    async fn sync_status_cvv_active_always_synced() {
        let rpc = setup_rpc(NodeMode::CvvActive, Some((100, 10, 10)), Some(200), 0, Some(10), None);
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_syncing_when_network_epoch_ahead() {
        let local_epoch = fresh_local_epoch();
        let rpc = setup_rpc(
            NodeMode::Observer,
            Some((100, local_epoch.saturating_add(2), 10)),
            Some(100),
            10,
            Some(local_epoch.saturating_add(1)),
            None,
        );
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 100);
                assert_eq!(progress.highest_block, 100);
                assert_eq!(progress.current_epoch, local_epoch as u64);
                assert_eq!(progress.highest_epoch, local_epoch.saturating_add(2) as u64);
            }
            SyncStatus::Synced => panic!("should be Syncing when network epoch is ahead"),
        }
    }

    #[tokio::test]
    async fn sync_status_syncing_when_execution_round_behind() {
        let local_epoch = fresh_local_epoch();
        let db_epoch = if local_epoch == 0 { 0 } else { local_epoch - 1 };
        let rpc = setup_rpc(
            NodeMode::Observer,
            Some((100, local_epoch, 10)),
            Some(100),
            9,
            Some(db_epoch),
            None,
        );
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 100);
                assert_eq!(progress.highest_block, 100);
                assert_eq!(progress.current_epoch, local_epoch as u64);
                assert_eq!(progress.highest_epoch, local_epoch as u64);
            }
            SyncStatus::Synced => panic!("should be Syncing when execution is behind"),
        }
    }

    #[tokio::test]
    async fn sync_status_syncing_when_published_consensus_is_ahead() {
        let local_epoch = fresh_local_epoch();
        let db_epoch = if local_epoch == 0 { 0 } else { local_epoch - 1 };
        let rpc = setup_rpc(
            NodeMode::Observer,
            Some((95, local_epoch, 9)),
            Some(100),
            9,
            Some(db_epoch),
            None,
        );
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.current_block, 95);
                assert_eq!(progress.highest_block, 100);
            }
            SyncStatus::Synced => panic!("should be Syncing when published consensus is ahead"),
        }
    }

    #[tokio::test]
    async fn sync_status_synced_on_dummy_epoch_zero_when_caught_up() {
        let rpc = setup_rpc(NodeMode::Observer, Some((100, 0, 10)), Some(100), 10, Some(0), None);
        assert!(matches!(rpc.sync_status(), SyncStatus::Synced));
    }

    #[tokio::test]
    async fn sync_status_starting_block_from_parent_consensus_record() {
        let local_epoch = fresh_local_epoch();
        let rpc = setup_rpc(
            NodeMode::Observer,
            Some((100, local_epoch.saturating_add(2), 10)),
            Some(100),
            0,
            Some(local_epoch.saturating_add(1)),
            Some(42),
        );
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.starting_block, 42);
            }
            SyncStatus::Synced => panic!("should be Syncing"),
        }
    }

    #[tokio::test]
    async fn sync_status_starting_block_zero_for_dummy_epoch_zero_record() {
        let rpc = setup_rpc(NodeMode::Observer, Some((100, 0, 10)), Some(100), 0, Some(0), None);
        match rpc.sync_status() {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.starting_block, 0);
            }
            SyncStatus::Synced => panic!("should be Syncing"),
        }
    }
}
