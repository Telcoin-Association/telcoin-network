// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use serde::Serialize;
use std::future::Future;
use thiserror::Error;
use tn_types::{
    Address, AuthorityIdentifier, BlsPublicKey, ConsensusHeader, ConsensusHeaderDigest, Epoch,
    EpochCertificate, EpochDigest, EpochRecord, Multiaddr, NetworkPublicKey, NodeMode,
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
        hash: Option<EpochDigest>,
    ) -> impl Future<Output = Option<(EpochRecord, EpochCertificate)>> + Send;
    /// Get the consensus header with `digest` from `epoch`'s consensus pack.
    ///
    /// Returns `Ok(None)` when this node does not hold the header: the digest is unknown or the
    /// epoch's pack is absent. The RPC layer reports that as "not found" and asks again on the
    /// next request, so the header resolves once its pack arrives.
    ///
    /// Returns [`ConsensusStorageError`] when storage fails to answer, for example for a sealed
    /// epoch whose pack files are on disk but cannot be opened. The error carries no detail
    /// because RPC callers must never see storage internals, so implementations log the cause
    /// where it happens. The RPC layer reports the failure as an internal error and, for a short
    /// time afterwards, answers requests for that epoch without calling this method, so a caller
    /// that repeats the request does not reopen the pack and log the failure every time.
    fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: ConsensusHeaderDigest,
    ) -> impl Future<Output = Result<Option<ConsensusHeader>, ConsensusStorageError>> + Send;
    /// Return the node's static information.
    fn node_info(&self) -> &RpcNodeInfo;
    /// Return the node's current consensus participation mode.
    ///
    /// Read live so callers can observe transient modes (e.g. `CvvInactive` while a restarted node
    /// catches up); it is not part of the static [`RpcNodeInfo`].
    fn node_mode(&self) -> NodeMode;
}

/// Consensus storage failed to answer an [`EngineToPrimary`] lookup.
///
/// Carries no detail: the implementation that hit the failure logs its cause, and the RPC layer
/// only needs to tell a failed lookup apart from a header that is not there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("consensus storage failed to answer the lookup")]
pub struct ConsensusStorageError;
