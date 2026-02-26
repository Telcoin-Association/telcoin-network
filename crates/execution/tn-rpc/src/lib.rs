// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{
    CommitteeInfo, EpochInfo, SyncProgress, SyncStatus, TelcoinNetworkRpcExt,
    TelcoinNetworkRpcExtApiServer, ValidatorInfo,
};
use tn_types::{BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord};

/// Trait used to get primary data for our RPC extension (tn namespace).
pub trait EngineToPrimary {
    /// Retrieve the latest consensus block.
    fn get_latest_consensus_block(&self) -> ConsensusHeader;
    /// Retrieve the consensus block by number.
    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader>;
    /// Retrieve the consensus block by hash.
    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;
    /// Get an epoch header if found.
    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)>;
    /// Return the node's sync status.
    fn sync_status(&self) -> SyncStatus;
    /// Return information about the current epoch, if available.
    fn current_epoch_info(&self) -> Option<EpochInfo>;
    /// Return information about the current committee, if available.
    fn current_committee(&self) -> Option<CommitteeInfo>;
}
