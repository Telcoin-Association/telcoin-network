// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use tn_types::{BlockHash, ConsensusHeader};

/// Trait used to get primary data for our RPC extension (tn namespace).
pub trait EngineToPrimary {
    fn get_latest_consensus_block(&self) -> ConsensusHeader;
    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader>;
    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;
}
