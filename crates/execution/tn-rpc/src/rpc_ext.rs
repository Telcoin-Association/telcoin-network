//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    EngineToPrimary,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};
use tn_reth::ChainSpec;
use tn_types::{
    Address, BlockHash, BlockNumHash, BlsPublicKey, ConsensusHeader, Epoch, EpochCertificate,
    EpochRecord, Genesis, B256,
};

/// Sync status returned by `tn_syncing`.
///
/// When the node is fully synced, serializes as `false`.
/// When syncing, serializes as an object with progress fields.
#[derive(Debug, Clone)]
pub enum SyncStatus {
    /// Node is fully synced (serializes as `false`).
    Synced,
    /// Node is syncing (serializes as an object with camelCase fields).
    Syncing(SyncProgress),
}

impl Serialize for SyncStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SyncStatus::Synced => serializer.serialize_bool(false),
            SyncStatus::Syncing(progress) => progress.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SyncStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Try to deserialize as a bool first (synced case), then as SyncProgress
        let value = serde_json::Value::deserialize(deserializer)?;
        match &value {
            serde_json::Value::Bool(false) => Ok(SyncStatus::Synced),
            serde_json::Value::Object(_) => {
                let progress: SyncProgress =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(SyncStatus::Syncing(progress))
            }
            _ => Err(serde::de::Error::custom("expected `false` or a sync progress object")),
        }
    }
}

/// Progress information when the node is syncing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncProgress {
    /// The consensus block number where sync started.
    pub starting_block: u64,
    /// The latest consensus block number processed by execution.
    pub current_block: u64,
    /// The highest known consensus block number.
    pub highest_block: u64,
    /// The current execution canonical tip height.
    pub execution_block_height: u64,
    /// The node's current local epoch.
    pub current_epoch: u64,
    /// The network's current epoch.
    pub highest_epoch: u64,
}

/// Information about a validator in the committee.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorInfo {
    /// The validator's BLS public key.
    pub bls_public_key: BlsPublicKey,
    /// The validator's execution-layer address.
    pub address: Address,
    /// The validator's voting power.
    pub voting_power: u64,
}

/// Information about the current committee.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitteeInfo {
    /// The epoch number for this committee.
    pub epoch: Epoch,
    /// The validators in the committee.
    pub validators: Vec<ValidatorInfo>,
}

/// Epoch information returned by `tn_epochInfo`.
///
/// Wraps [EpochRecord] fields with camelCase serialization for the JSON-RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochInfo {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    pub committee: Vec<BlsPublicKey>,
    /// The committee for the next epoch.
    pub next_committee: Vec<BlsPublicKey>,
    /// Hash of the previous EpochRecord.
    pub parent_hash: B256,
    /// The block number and hash of the last execution state of this epoch.
    pub parent_state: BlockNumHash,
    /// The hash of the last ConsensusHeader of this epoch.
    pub parent_consensus: B256,
}

impl From<EpochRecord> for EpochInfo {
    fn from(record: EpochRecord) -> Self {
        Self {
            epoch: record.epoch,
            committee: record.committee,
            next_committee: record.next_committee,
            parent_hash: record.parent_hash,
            parent_state: record.parent_state,
            parent_consensus: record.parent_consensus,
        }
    }
}

/// Telcoin Network RPC namespace.
///
/// TN-specific RPC endpoints.
#[rpc(server, namespace = "tn")]
pub trait TelcoinNetworkRpcExtApi {
    /// Return the latest consensus header.
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
    /// Return the sync status of the node.
    ///
    /// Returns `false` if fully synced, or an object with progress information if syncing.
    #[method(name = "syncing")]
    async fn syncing(&self) -> TelcoinNetworkRpcResult<SyncStatus>;
    /// Return information about the current epoch.
    #[method(name = "epochInfo")]
    async fn epoch_info(&self) -> TelcoinNetworkRpcResult<EpochInfo>;
    /// Return information about the current committee.
    #[method(name = "currentCommittee")]
    async fn current_committee(&self) -> TelcoinNetworkRpcResult<CommitteeInfo>;
}

/// The type that implements `tn` namespace trait.
#[derive(Debug)]
pub struct TelcoinNetworkRpcExt<N: EngineToPrimary> {
    /// The chain id for this node.
    chain: ChainSpec,
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
    async fn latest_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader> {
        Ok(self.inner_node_network.get_latest_consensus_block())
    }

    async fn genesis(&self) -> TelcoinNetworkRpcResult<Genesis> {
        Ok(self.chain.genesis().clone())
    }

    async fn epoch_record(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(Some(epoch), None).ok_or(TNRpcError::NotFound)
    }

    async fn epoch_record_by_hash(
        &self,
        hash: BlockHash,
    ) -> TelcoinNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(None, Some(hash)).ok_or(TNRpcError::NotFound)
    }

    async fn syncing(&self) -> TelcoinNetworkRpcResult<SyncStatus> {
        Ok(self.inner_node_network.sync_status())
    }

    async fn epoch_info(&self) -> TelcoinNetworkRpcResult<EpochInfo> {
        self.inner_node_network.current_epoch_info().ok_or(TNRpcError::NotFound)
    }

    async fn current_committee(&self) -> TelcoinNetworkRpcResult<CommitteeInfo> {
        self.inner_node_network.current_committee().ok_or(TNRpcError::NotFound)
    }
}

impl<N: EngineToPrimary> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(chain: ChainSpec, inner_node_network: N) -> Self {
        Self { chain, inner_node_network }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_status_synced_serializes_as_false() {
        let status = SyncStatus::Synced;
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json, serde_json::Value::Bool(false));
    }

    #[test]
    fn sync_status_syncing_serializes_as_object() {
        let status = SyncStatus::Syncing(SyncProgress {
            starting_block: 100,
            current_block: 5000,
            highest_block: 10000,
            execution_block_height: 4800,
            current_epoch: 3,
            highest_epoch: 5,
        });
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["startingBlock"], 100);
        assert_eq!(json["currentBlock"], 5000);
        assert_eq!(json["highestBlock"], 10000);
        assert_eq!(json["executionBlockHeight"], 4800);
        assert_eq!(json["currentEpoch"], 3);
        assert_eq!(json["highestEpoch"], 5);
        // Ensure camelCase field names
        assert!(json.get("starting_block").is_none());
        assert!(json.get("current_epoch").is_none());
    }

    #[test]
    fn sync_status_synced_roundtrip() {
        let status = SyncStatus::Synced;
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: SyncStatus = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, SyncStatus::Synced));
    }

    #[test]
    fn sync_status_syncing_roundtrip() {
        let status = SyncStatus::Syncing(SyncProgress {
            starting_block: 0,
            current_block: 500,
            highest_block: 1000,
            execution_block_height: 450,
            current_epoch: 2,
            highest_epoch: 4,
        });
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: SyncStatus = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncStatus::Syncing(progress) => {
                assert_eq!(progress.starting_block, 0);
                assert_eq!(progress.current_block, 500);
                assert_eq!(progress.highest_block, 1000);
                assert_eq!(progress.execution_block_height, 450);
                assert_eq!(progress.current_epoch, 2);
                assert_eq!(progress.highest_epoch, 4);
            }
            SyncStatus::Synced => panic!("expected Syncing variant"),
        }
    }

    #[test]
    fn sync_status_rejects_true() {
        let result: Result<SyncStatus, _> = serde_json::from_str("true");
        assert!(result.is_err());
    }

    #[test]
    fn sync_status_rejects_null() {
        let result: Result<SyncStatus, _> = serde_json::from_str("null");
        assert!(result.is_err());
    }

    #[test]
    fn sync_status_rejects_number() {
        let result: Result<SyncStatus, _> = serde_json::from_str("42");
        assert!(result.is_err());
    }
}
