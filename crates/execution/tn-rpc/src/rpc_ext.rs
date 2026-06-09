//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    EngineToPrimary, RpcNodeInfo,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use tn_reth::RethEnv;
use tn_types::{BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, Genesis};

/// Telcoin Network RPC namespace.
///
/// TN-specific RPC endpoints.
#[rpc(server, namespace = "tn")]
pub trait TelcoinNetworkRpcExtApi {
    /// Return the node's information.
    /// To include, names, ids, public keys, network addressed etc.
    /// This should be all the publicly available information to identify and connect to this node.
    #[method(name = "info")]
    async fn info(&self) -> TelcoinNetworkRpcResult<RpcNodeInfo>;
    /// Return the latest consensus header.
    #[method(name = "latestConsensusHeader")]
    async fn latest_consensus_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader>;
    /// Return the latest consensus header.
    ///
    /// Deprecated alias for `tn_latestConsensusHeader`.
    #[method(name = "latestHeader")]
    async fn latest_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader>;
    /// Return the chain genesis.
    #[method(name = "genesis")]
    async fn genesis(&self) -> TelcoinNetworkRpcResult<Genesis>;
    /// Get the header for epoch if available.
    #[method(name = "epochRecord")]
    async fn epoch_record(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)>;
    /// Get the header for epoch by hash if available.
    #[method(name = "epochRecordByHash")]
    async fn epoch_record_by_hash(
        &self,
        hash: BlockHash,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)>;
}

/// The type that implements `tn` namespace trait.
#[derive(Debug)]
pub struct TelcoinNetworkRpcExt<N: EngineToPrimary> {
    /// Type to interact with EVM state.
    evm_state: RethEnv,
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
}

#[async_trait]
impl<N: EngineToPrimary> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    async fn info(&self) -> TelcoinNetworkRpcResult<RpcNodeInfo> {
        Ok(self.inner_node_network.node_info().clone())
    }
    async fn latest_consensus_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader> {
        Ok(self.inner_node_network.get_latest_consensus_block())
    }

    async fn latest_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader> {
        Ok(self.inner_node_network.get_latest_consensus_block())
    }

    async fn genesis(&self) -> TelcoinNetworkRpcResult<Genesis> {
        Ok(self.evm_state.chainspec().genesis().clone())
    }

    async fn epoch_record(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(Some(epoch), None).await.ok_or(TNRpcError::NotFound)
    }

    async fn epoch_record_by_hash(
        &self,
        hash: BlockHash,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(None, Some(hash)).await.ok_or(TNRpcError::NotFound)
    }
}

impl<N: EngineToPrimary> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(evm_state: RethEnv, inner_node_network: N) -> Self {
        Self { evm_state, inner_node_network }
    }
}
