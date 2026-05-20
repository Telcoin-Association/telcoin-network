// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use serde::{Deserialize, Serialize};
use tn_types::{
    Address, AuthorityIdentifier, BlockHash, BlsPublicKey, ConsensusHeader, Epoch,
    EpochCertificate, EpochRecord, Multiaddr, NetworkPublicKey,
};

/// Contain the nodes identifying info to provide over RPC.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct RpcNodeInfo {
    /// The name for the validator. The default value
    /// is the hashed value of the validator's
    /// execution address. The operator can overwrite
    /// this value since it is not used when writing to file.
    ///
    /// Keccak256(Address)
    pub name: String,
    /// The nodes BLS public key.
    pub bls_public_key: BlsPublicKey,
    /// The nodes authority id (hash of BLS key).
    /// Used for some tables (like reputation).
    pub authority_id: AuthorityIdentifier,
    /// Address that will receive rewards if this node participates in consensus.
    pub execution_address: Address,
    /// Network public key for the primary network.
    pub primary_network_key: NetworkPublicKey,
    /// Network public key for the workes network.
    pub worker_network_key: NetworkPublicKey,
    /// Network external address for the primary network.
    pub primary_external_address: Multiaddr,
    /// Network external address for the worker network.
    pub worker_external_address: Multiaddr,
}

/// Trait used to get primary data for our RPC extension (tn namespace).
pub trait EngineToPrimary {
    /// Retrieve the latest consensus block.
    fn get_latest_consensus_block(&self) -> ConsensusHeader;
    /// Get an epoch header if found.
    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> impl std::future::Future<Output = Option<(EpochRecord, EpochCertificate)>> + Send;
    /// Return the nodes static information.
    fn node_info(&self) -> RpcNodeInfo;
}
