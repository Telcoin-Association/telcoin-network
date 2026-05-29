// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use serde::Serialize;
use tn_types::{
    Address, AuthorityIdentifier, BlockHash, BlsPublicKey, ConsensusHeader, Epoch,
    EpochCertificate, EpochRecord, Multiaddr, NetworkPublicKey,
};

/// Contain the node's identifying info to provide over RPC.
#[derive(PartialEq, Serialize, Clone, Debug)]
pub struct RpcNodeInfo {
    /// Chain id this node is part of.
    pub chain_id: u64,
    /// The version of the running software.
    pub version: &'static str,
    /// The name for the validator. The default value
    /// is the base58 encoding of the first 8 bytes of the BLS public key
    /// prepended with 'node-'. The operator can overwrite
    /// this value since it is not used when writing to file.
    pub name: String,
    /// The node's BLS public key.
    pub bls_public_key: BlsPublicKey,
    /// The node's authority id (hash of BLS key).
    /// Used for some tables (like reputation).
    pub authority_id: AuthorityIdentifier,
    /// Address that will receive rewards if this node participates in consensus.
    pub execution_address: Address,
    /// Network public key for the primary network.
    pub primary_network_key: NetworkPublicKey,
    /// Network public key for the workers network.
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
    /// Return the node's static information.
    fn node_info(&self) -> &RpcNodeInfo;
}
