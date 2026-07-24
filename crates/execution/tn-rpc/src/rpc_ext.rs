//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    EngineToPrimary, RpcNodeInfo,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tn_reth::{
    error::{EvmReadError, EvmReadResult},
    system_calls::ConsensusRegistry,
    RethEnv,
};
use tn_types::{
    construct_proof_of_possession_message, Address, BlsPublicKey, Bytes, ConsensusHeader, Epoch,
    EpochCertificate, EpochDigest, EpochRecord, Genesis, NodeMode, SolCall, SolType, SolValue,
    B256, U256,
};
use tokio::sync::{oneshot, Semaphore};

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
    /// Return the node's current consensus participation mode.
    ///
    /// Read live at call time (not part of the static `tn_info` response), so callers can observe
    /// transient modes such as `CvvInactive` while a restarted node catches up.
    #[method(name = "nodeMode")]
    async fn node_mode(&self) -> TelcoinNetworkRpcResult<NodeMode>;
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
        hash: EpochDigest,
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
    /// Return the BLS12-381 proof-of-possession message a validator signs:
    /// `intentPrefix(3) || compressedBlsPubkey(96) || validatorAddress(20)`.
    ///
    /// Expects the 96-byte compressed BLS public key.
    #[method(name = "proofOfPossessionMessage")]
    async fn proof_of_possession_message(
        &self,
        bls_pubkey: Bytes,
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

/// Maximum number of concurrent blocking [`ConsensusRegistry`] reads served by the `tn`
/// namespace.
///
/// Each public read builds a fresh EVM and state snapshot at the canonical tip - synchronous
/// database/CPU work on a blocking thread. A semaphore permit is held for the full lifetime of
/// the blocking work, so this bounds true blocking-pool occupancy, not just in-flight requests.
///
/// Mirrors reth's `eth_call` guard (`DEFAULT_MAX_BLOCKING_IO_REQUEST` = 256): tokio's blocking
/// pool defaults to 512 threads and grows unbounded under request load without a bound. Set
/// below reth's 256 because reth's own eth namespace shares the same pool; 64 keeps headroom
/// for BLS signing and engine blocking tasks. Lives at the RPC layer only, so consensus-critical
/// reads (`epoch_state_from_canonical_tip`) are never throttled by RPC traffic.
const MAX_CONCURRENT_REGISTRY_READS: usize = 64;

/// The type that implements `tn` namespace trait.
#[derive(Debug)]
pub struct TelcoinNetworkRpcExt<N: EngineToPrimary> {
    /// Type to interact with EVM state.
    evm_state: RethEnv,
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
    /// Bounds concurrent blocking [`ConsensusRegistry`] reads (see
    /// [`MAX_CONCURRENT_REGISTRY_READS`]). Acquired before spawning the blocking read and held
    /// until it completes, capping blocking-pool occupancy from RPC load.
    blocking_io_guard: Arc<Semaphore>,
}

#[async_trait]
impl<N: EngineToPrimary> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    async fn info(&self) -> TelcoinNetworkRpcResult<RpcNodeInfo> {
        Ok(self.inner_node_network.node_info().clone())
    }

    async fn node_mode(&self) -> TelcoinNetworkRpcResult<NodeMode> {
        Ok(self.inner_node_network.node_mode())
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
        hash: EpochDigest,
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
        use ConsensusRegistry::ValidatorStatus;

        // `getValidatorsInfo` returns exactly one status set and reverts on the `Undefined`/`Any`
        // sentinels (the registry no longer folds statuses on-chain). Emulate the documented
        // `"Any"` => "all validators" behavior by unioning every concrete status set. The five
        // reads run against ONE pinned canonical tip (`read_consensus_registry_batch`), so a
        // validator that changes status between block commits cannot be double-counted or dropped.
        // Each validator lives in exactly one set, so the union is a plain concatenation (no
        // dedup); retired validators intentionally appear in no set, matching the old scan. Any
        // other status (including `Undefined`) is passed straight through, so `Undefined` still
        // surfaces the on-chain revert that callers rely on.
        if status == ValidatorStatus::Any {
            let calldatas = [
                ValidatorStatus::Staked,
                ValidatorStatus::PendingActivation,
                ValidatorStatus::Active,
                ValidatorStatus::PendingExit,
                ValidatorStatus::Exited,
            ]
            .into_iter()
            .map(|s| {
                ConsensusRegistry::getValidatorsInfoCall { status: s as u8 }.abi_encode().into()
            })
            .collect::<Vec<Bytes>>();

            // one permit, one blocking task, one pinned EVM for all five status reads
            let sets: Vec<Vec<ConsensusRegistry::ValidatorInfo>> = self
                .spawn_registry_read(move |evm| evm.read_consensus_registry_batch(calldatas))
                .await?;
            Ok(sets.into_iter().flatten().collect())
        } else {
            let calldata = ConsensusRegistry::getValidatorsInfoCall { status: status as u8 }
                .abi_encode()
                .into();
            self.registry_read(calldata).await
        }
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
        bls_pubkey: Bytes,
        validator_address: Address,
    ) -> TelcoinNetworkRpcResult<Bytes> {
        // As of the compressed-bytes pivot the registry's `proofOfPossessionMessage` is an internal
        // helper (not externally callable), so build the message natively from the same `tn_types`
        // routine the validator signs and the registry reproduces on-chain byte-for-byte:
        // `intent || compressed pubkey || address`.
        let pubkey = BlsPublicKey::from_literal_bytes(&bls_pubkey).map_err(|e| {
            TNRpcError::InvalidParams(format!("invalid 96-byte compressed BLS pubkey: {e:?}"))
        })?;
        Ok(construct_proof_of_possession_message(&pubkey, &validator_address).into())
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
        let blocking_io_guard = Arc::new(Semaphore::new(MAX_CONCURRENT_REGISTRY_READS));
        Self { evm_state, inner_node_network, blocking_io_guard }
    }

    /// Execute a read-only [`ConsensusRegistry`] call at the canonical tip and decode the result.
    ///
    /// Thin wrapper over [`Self::spawn_registry_read`] for the common single-read endpoints.
    async fn registry_read<T>(&self, calldata: Bytes) -> TelcoinNetworkRpcResult<T>
    where
        T: SolValue + Send + 'static,
        T: From<<<T as SolValue>::SolType as SolType>::RustType>,
    {
        self.spawn_registry_read(move |evm| evm.read_consensus_registry::<T>(calldata)).await
    }

    /// Run a read-only [`ConsensusRegistry`] closure on the blocking pool and map its result to
    /// an RPC response.
    ///
    /// EVM reads are synchronous database/CPU work, so they run on the protocol [`TaskSpawner`]'s
    /// blocking pool to avoid stalling the async runtime on a public endpoint. A
    /// [`MAX_CONCURRENT_REGISTRY_READS`] permit is acquired before spawning and moved into the
    /// blocking task, so it is released only when the work finishes — bounding blocking-pool
    /// occupancy even if a client disconnects mid-read.
    ///
    /// The closure receives an owned [`RethEnv`] and may issue one read
    /// ([`RethEnv::read_consensus_registry`]) or several pinned reads
    /// ([`RethEnv::read_consensus_registry_batch`]); both single- and multi-read endpoints share
    /// this permit/spawn/revert-mapping path.
    ///
    /// On-chain reverts surface to the client eth_call-style (code 3 with revert bytes in
    /// `data`); internal failures are logged server-side and return a generic error.
    ///
    /// [`TaskSpawner`]: tn_types::TaskSpawner
    async fn spawn_registry_read<R, F>(&self, read: F) -> TelcoinNetworkRpcResult<R>
    where
        R: Send + 'static,
        F: FnOnce(RethEnv) -> EvmReadResult<R> + Send + 'static,
    {
        // bound concurrent blocking reads; queues (does not reject) at capacity, mirroring
        // reth's eth_call guard. acquire only fails if the semaphore is closed, which never
        // happens because it lives as long as the RPC server.
        let permit = self.blocking_io_guard.clone().acquire_owned().await.map_err(|e| {
            tracing::warn!(target: "tn::rpc", error = %e, "registry read semaphore closed");
            TNRpcError::Internal
        })?;

        let evm = self.evm_state.clone();
        let (tx, rx) = oneshot::channel();
        self.evm_state.get_task_spawner().spawn_blocking_task("tn-rpc-registry-read", move || {
            // hold the permit until the blocking read completes
            let _permit = permit;
            if tx.send(read(evm)).is_err() {
                tracing::debug!(target: "tn::rpc", "registry read receiver dropped before result");
            }
            Ok(())
        });

        rx.await
            .map_err(|e| {
                tracing::warn!(target: "tn::rpc", error = %e, "registry read result channel closed");
                TNRpcError::Internal
            })?
            .map_err(|err| match &err {
                EvmReadError::Revert { output, .. } => {
                    TNRpcError::Revert { message: err.to_string(), output: output.clone() }
                }
                EvmReadError::Internal(_) => {
                    tracing::debug!(
                        target: "tn::rpc",
                        error = ?err,
                        "consensus registry read failed"
                    );
                    TNRpcError::Internal
                }
            })
    }
}
