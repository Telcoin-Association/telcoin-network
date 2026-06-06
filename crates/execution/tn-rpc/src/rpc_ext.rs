//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    EngineToPrimary, RpcNodeInfo,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};
use tn_reth::{error::RegistryReadError, system_calls::ConsensusRegistry, ChainSpec, RethEnv};
use tn_types::{
    Address, BlockHash, Bytes, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, Genesis,
    SolCall, SolType, SolValue, B256, U256,
};

/// Response for `tn_getBalanceBreakdown`.
///
/// The contract returns three unnamed `uint256` values; this struct names them for JSON clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceBreakdown {
    /// The validator's outstanding balance.
    pub outstanding_balance: U256,
    /// The initial stake amount for the validator's stake version.
    pub initial_stake: U256,
    /// The claimable rewards accrued for the validator.
    pub rewards: U256,
}

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
    /// Return the current epoch number from the on-chain [`ConsensusRegistry`].
    #[method(name = "getCurrentEpoch")]
    async fn get_current_epoch(&self) -> TelcoinNetworkRpcResult<Epoch>;
    /// Return the current epoch's on-chain info from the [`ConsensusRegistry`].
    ///
    /// This reads contract state (committee, issuance, block height, duration) — distinct from
    /// `tn_epochRecord`, which serves consensus-layer records from the node's database.
    #[method(name = "getCurrentEpochInfo")]
    async fn get_current_epoch_info(&self)
        -> TelcoinNetworkRpcResult<ConsensusRegistry::EpochInfo>;
    /// Return on-chain info for a specific epoch from the [`ConsensusRegistry`].
    ///
    /// The contract only retains a ring buffer of epochs around the current one; requests
    /// outside that window revert. Distinct from `tn_epochRecord`, which serves consensus-layer
    /// records from the node's database.
    #[method(name = "getEpochInfo")]
    async fn get_epoch_info(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<ConsensusRegistry::EpochInfo>;
    /// Return all validators with the requested status.
    ///
    /// Pass `"Any"` to return all validators; `"Undefined"` reverts on-chain.
    #[method(name = "getValidators")]
    async fn get_validators(
        &self,
        status: ConsensusRegistry::ValidatorStatus,
    ) -> TelcoinNetworkRpcResult<Vec<ConsensusRegistry::ValidatorInfo>>;
    /// Return the committee validators for the given epoch.
    #[method(name = "getCommitteeValidators")]
    async fn get_committee_validators(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<Vec<ConsensusRegistry::ValidatorInfo>>;
    /// Return the `ValidatorInfo` for a given validator address.
    #[method(name = "getValidator")]
    async fn get_validator(
        &self,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<ConsensusRegistry::ValidatorInfo>;
    /// Return the BLS public key for a given validator address.
    #[method(name = "getBlsPubkey")]
    async fn get_bls_pubkey(&self, validator_address: Address) -> TelcoinNetworkRpcResult<Bytes>;
    /// Return the BLS public keys for the committee of a given epoch.
    #[method(name = "getCommitteeBlsPubkeys")]
    async fn get_committee_bls_pubkeys(&self, epoch: Epoch) -> TelcoinNetworkRpcResult<Vec<Bytes>>;
    /// Return true if the BLS public key belongs to a known validator.
    #[method(name = "isValidator")]
    async fn is_validator(&self, bls_pubkey: Bytes) -> TelcoinNetworkRpcResult<bool>;
    /// Return true if the validator's stake originates from a delegator.
    #[method(name = "isDelegated")]
    async fn is_delegated(&self, validator_address: Address) -> TelcoinNetworkRpcResult<bool>;
    /// Return true if the validator is permanently retired.
    #[method(name = "isRetired")]
    async fn is_retired(&self, validator_address: Address) -> TelcoinNetworkRpcResult<bool>;
    /// Return the claimable rewards accrued for a given validator address.
    #[method(name = "getRewards")]
    async fn get_rewards(&self, validator_address: Address) -> TelcoinNetworkRpcResult<U256>;
    /// Return the balance breakdown (outstanding balance, initial stake, rewards) for a given
    /// validator address.
    #[method(name = "getBalanceBreakdown")]
    async fn get_balance_breakdown(
        &self,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<BalanceBreakdown>;
    /// Return the EIP-712 digest a validator signs to accept a delegation.
    #[method(name = "delegationDigest")]
    async fn delegation_digest(
        &self,
        bls_pubkey: Bytes,
        validator_address: Address,
        delegator: Address,
    ) -> TelcoinNetworkRpcResult<B256>;
    /// Return the BLS12-381 proof of possession message: `blsPubkey || validatorAddress`.
    ///
    /// Expects the 192-byte uncompressed BLS public key.
    #[method(name = "proofOfPossessionMessage")]
    async fn proof_of_possession_message(
        &self,
        bls_pubkey_uncompressed: Bytes,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<Bytes>;
    /// Return the committee size for the next epoch.
    #[method(name = "getNextCommitteeSize")]
    async fn get_next_committee_size(&self) -> TelcoinNetworkRpcResult<u16>;
    /// Return the current stake config version.
    #[method(name = "getCurrentStakeVersion")]
    async fn get_current_stake_version(&self) -> TelcoinNetworkRpcResult<u8>;
    /// Return the issuance not yet distributed to validators.
    #[method(name = "undistributedIssuance")]
    async fn undistributed_issuance(&self) -> TelcoinNetworkRpcResult<U256>;
}

/// The type that implements `tn` namespace trait.
#[derive(Debug)]
pub struct TelcoinNetworkRpcExt<N: EngineToPrimary> {
    /// The chain id for this node.
    chain: ChainSpec,
    /// The execution environment for reading on-chain [`ConsensusRegistry`] state.
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
        Ok(self.chain.genesis().clone())
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

    async fn get_current_epoch(&self) -> TelcoinNetworkRpcResult<Epoch> {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_current_epoch_info(
        &self,
    ) -> TelcoinNetworkRpcResult<ConsensusRegistry::EpochInfo> {
        let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_epoch_info(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<ConsensusRegistry::EpochInfo> {
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch }.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_validators(
        &self,
        status: ConsensusRegistry::ValidatorStatus,
    ) -> TelcoinNetworkRpcResult<Vec<ConsensusRegistry::ValidatorInfo>> {
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: status as u8 }.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_committee_validators(
        &self,
        epoch: Epoch,
    ) -> TelcoinNetworkRpcResult<Vec<ConsensusRegistry::ValidatorInfo>> {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_validator(
        &self,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<ConsensusRegistry::ValidatorInfo> {
        let calldata = ConsensusRegistry::getValidatorCall { validatorAddress: validator_address }
            .abi_encode()
            .into();
        self.registry_read(calldata).await
    }

    async fn get_bls_pubkey(&self, validator_address: Address) -> TelcoinNetworkRpcResult<Bytes> {
        let calldata = ConsensusRegistry::getBlsPubkeyCall { validatorAddress: validator_address }
            .abi_encode()
            .into();
        self.registry_read(calldata).await
    }

    async fn get_committee_bls_pubkeys(&self, epoch: Epoch) -> TelcoinNetworkRpcResult<Vec<Bytes>> {
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn is_validator(&self, bls_pubkey: Bytes) -> TelcoinNetworkRpcResult<bool> {
        let calldata =
            ConsensusRegistry::isValidatorCall { blsPubkey: bls_pubkey }.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn is_delegated(&self, validator_address: Address) -> TelcoinNetworkRpcResult<bool> {
        let calldata = ConsensusRegistry::isDelegatedCall { validatorAddress: validator_address }
            .abi_encode()
            .into();
        self.registry_read(calldata).await
    }

    async fn is_retired(&self, validator_address: Address) -> TelcoinNetworkRpcResult<bool> {
        let calldata = ConsensusRegistry::isRetiredCall { validatorAddress: validator_address }
            .abi_encode()
            .into();
        self.registry_read(calldata).await
    }

    async fn get_rewards(&self, validator_address: Address) -> TelcoinNetworkRpcResult<U256> {
        let calldata = ConsensusRegistry::getRewardsCall { validatorAddress: validator_address }
            .abi_encode()
            .into();
        self.registry_read(calldata).await
    }

    async fn get_balance_breakdown(
        &self,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<BalanceBreakdown> {
        let calldata =
            ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: validator_address }
                .abi_encode()
                .into();
        let (outstanding_balance, initial_stake, rewards) =
            self.registry_read::<(U256, U256, U256)>(calldata).await?;
        Ok(BalanceBreakdown { outstanding_balance, initial_stake, rewards })
    }

    async fn delegation_digest(
        &self,
        bls_pubkey: Bytes,
        validator_address: Address,
        delegator: Address,
    ) -> TelcoinNetworkRpcResult<B256> {
        let calldata = ConsensusRegistry::delegationDigestCall {
            blsPubkey: bls_pubkey,
            validatorAddress: validator_address,
            delegator,
        }
        .abi_encode()
        .into();
        self.registry_read(calldata).await
    }

    async fn proof_of_possession_message(
        &self,
        bls_pubkey_uncompressed: Bytes,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<Bytes> {
        let calldata = ConsensusRegistry::proofOfPossessionMessageCall {
            blsPubkey: bls_pubkey_uncompressed,
            validatorAddress: validator_address,
        }
        .abi_encode()
        .into();
        self.registry_read(calldata).await
    }

    async fn get_next_committee_size(&self) -> TelcoinNetworkRpcResult<u16> {
        let calldata = ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into();
        self.registry_read(calldata).await
    }

    async fn get_current_stake_version(&self) -> TelcoinNetworkRpcResult<u8> {
        let calldata = ConsensusRegistry::getCurrentStakeVersionCall {}.abi_encode().into();
        // alloy doesn't implement `SolValue` for `u8`; a `uint8` return occupies the same
        // left-padded word as `uint16`, so decode wide and narrow (value is <= u8::MAX).
        let version: u16 = self.registry_read(calldata).await?;
        Ok(version as u8)
    }

    async fn undistributed_issuance(&self) -> TelcoinNetworkRpcResult<U256> {
        let calldata = ConsensusRegistry::undistributedIssuanceCall {}.abi_encode().into();
        self.registry_read(calldata).await
    }
}

impl<N: EngineToPrimary> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(evm_state: RethEnv, inner_node_network: N) -> Self {
        let chain = evm_state.chainspec();
        Self { chain, evm_state, inner_node_network }
    }

    /// Execute a read-only [`ConsensusRegistry`] call at the canonical tip and decode the result.
    ///
    /// EVM reads are synchronous database/CPU work, so they run on a blocking task to avoid
    /// stalling the async runtime on a public endpoint.
    ///
    /// On-chain reverts surface to the client eth_call-style (code 3 with revert bytes in
    /// `data`); internal failures are logged server-side and return a generic error.
    async fn registry_read<T>(&self, calldata: Bytes) -> TelcoinNetworkRpcResult<T>
    where
        T: SolValue + Send + 'static,
        T: From<<<T as SolValue>::SolType as SolType>::RustType>,
    {
        let evm = self.evm_state.clone();
        tokio::task::spawn_blocking(move || evm.read_consensus_registry::<T>(calldata))
            .await
            .map_err(|e| {
                tracing::warn!(target: "tn::rpc", error = %e, "registry read task join error");
                TNRpcError::Internal
            })?
            .map_err(|report| match report.downcast_ref::<RegistryReadError>() {
                Some(RegistryReadError::Revert { output, .. }) => {
                    TNRpcError::Revert { message: report.to_string(), output: output.clone() }
                }
                _ => {
                    tracing::debug!(
                        target: "tn::rpc",
                        error = ?report,
                        "consensus registry read failed"
                    );
                    TNRpcError::Internal
                }
            })
    }
}
