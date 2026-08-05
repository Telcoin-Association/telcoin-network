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
    TaskSpawner, B256, U256,
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
        deadline: U256,
    ) -> TelcoinNetworkRpcResult<B256>;
    /// Return the BLS12-381 proof-of-possession message a validator signs:
    /// `intentPrefix(3) || compressedBlsPubkey(96) || validatorAddress(20)`.
    ///
    /// Expects the 96-byte compressed BLS public key. The 192-byte uncompressed encoding is also
    /// accepted and normalized to the compressed form, so both yield the same message.
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

/// Maximum number of concurrent blocking tasks the `tn` namespace may have in flight.
///
/// Covers both kinds of synchronous work this namespace dispatches off the async runtime:
/// [`ConsensusRegistry`] reads, which build a fresh EVM and state snapshot at the canonical tip
/// (database plus CPU), and the BLS12-381 G2 decompress behind `proofOfPossessionMessage` (pure
/// CPU, tens of microseconds per call). A semaphore permit is held for the full lifetime of the
/// blocking work, so this bounds true blocking-pool occupancy, not just in-flight requests.
///
/// One bound covers both deliberately. The budget below is a share of a pool the whole process
/// contends for, so splitting it into a guard per endpoint would let their worst cases add up and
/// silently double the occupancy this number is chosen to permit.
///
/// Mirrors reth's `eth_call` guard (`DEFAULT_MAX_BLOCKING_IO_REQUEST` = 256): tokio's blocking
/// pool defaults to 512 threads and grows unbounded under request load without a bound. Set
/// below reth's 256 because reth's own eth namespace shares the same pool; 64 keeps headroom
/// for BLS signing and engine blocking tasks. Lives at the RPC layer only, so consensus-critical
/// reads (`epoch_state_from_canonical_tip`) are never throttled by RPC traffic.
const MAX_CONCURRENT_BLOCKING_RPC_WORK: usize = 64;

/// Run `work` on `spawner`'s blocking pool under a `guard` permit, and await its value.
///
/// This is how every synchronous endpoint in this namespace runs its work. Handler bodies are
/// polled on the shared runtime's worker threads, the same threads consensus tasks are scheduled
/// on, so synchronous work that does not yield holds a worker for its entire duration and nothing
/// else can be scheduled there until it returns. Dispatching it here keeps the worker free and puts
/// the work on a pool whose occupancy `guard` bounds.
///
/// The permit is moved into the blocking task rather than held across the await, so it is released
/// when the work actually finishes rather than when the caller stops waiting. A client that
/// disconnects mid-call therefore cannot release capacity that is still in use.
///
/// At capacity this queues rather than rejecting, so a burst of legitimate callers is served late
/// rather than failed.
///
/// `name` labels the task for the task manager's bookkeeping.
async fn spawn_bounded_blocking<R, F>(
    guard: &Arc<Semaphore>,
    spawner: &TaskSpawner,
    name: &'static str,
    work: F,
) -> TelcoinNetworkRpcResult<R>
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    // bound concurrent blocking work; queues (does not reject) at capacity, mirroring
    // reth's eth_call guard. acquire only fails if the semaphore is closed, which never
    // happens because it lives as long as the RPC server.
    let permit = guard.clone().acquire_owned().await.map_err(|e| {
        tracing::warn!(target: "tn::rpc", error = %e, task = name, "rpc blocking semaphore closed");
        TNRpcError::Internal
    })?;

    let (tx, rx) = oneshot::channel();
    spawner.spawn_blocking_task(name, move || {
        // hold the permit across the work itself and release it before handing the result back, so
        // capacity frees the moment the expensive part is done rather than after the handoff. The
        // ordering also means a caller woken by the send always observes the permit as returned.
        let value = {
            let _permit = permit;
            work()
        };
        if tx.send(value).is_err() {
            tracing::debug!(target: "tn::rpc", task = name, "blocking rpc receiver dropped before result");
        }
        Ok(())
    });

    rx.await.map_err(|e| {
        tracing::warn!(target: "tn::rpc", error = %e, task = name, "blocking rpc result channel closed");
        TNRpcError::Internal
    })
}

/// Build the proof-of-possession message `validator_address` signs with `bls_pubkey`.
///
/// Screening the encoding length happens here, on the async thread, because it is the one check
/// that costs nothing and it settles every input that could never parse before a permit or a pool
/// thread is committed to it. A flood of malformed keys therefore cannot occupy capacity that real
/// work needs. Whatever survives the screen still has to decompress a BLS12-381 G2 point, which
/// takes tens of microseconds and must not run where consensus tasks are scheduled, so it goes to
/// the blocking pool under a permit.
async fn proof_of_possession_message_bounded(
    guard: &Arc<Semaphore>,
    spawner: &TaskSpawner,
    bls_pubkey: Bytes,
    validator_address: Address,
) -> TelcoinNetworkRpcResult<Bytes> {
    BlsPublicKey::is_plausible_encoding(&bls_pubkey).then_some(()).ok_or_else(|| {
        TNRpcError::InvalidParams(format!(
            "invalid BLS pubkey: expected {} bytes (compressed) or {} bytes (uncompressed), got {}",
            BlsPublicKey::COMPRESSED_BYTES,
            BlsPublicKey::UNCOMPRESSED_BYTES,
            bls_pubkey.len(),
        ))
    })?;

    spawn_bounded_blocking(guard, spawner, "tn-rpc-proof-of-possession", move || {
        // As of the compressed-bytes pivot the registry's `proofOfPossessionMessage` is an internal
        // helper (not externally callable), so build the message natively from the same `tn_types`
        // routine the validator signs and the registry reproduces on-chain byte-for-byte:
        // `intent || compressed pubkey || address`.
        BlsPublicKey::from_literal_bytes(&bls_pubkey)
            .map(|pubkey| construct_proof_of_possession_message(&pubkey, &validator_address).into())
            .map_err(|e| {
                TNRpcError::InvalidParams(format!("invalid 96-byte compressed BLS pubkey: {e:?}"))
            })
    })
    .await?
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
    /// Bounds the blocking work this namespace dispatches (see
    /// [`MAX_CONCURRENT_BLOCKING_RPC_WORK`]). Acquired before spawning the blocking task and held
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
        deadline: U256,
    ) -> TelcoinNetworkRpcResult<B256> {
        let calldata = ConsensusRegistry::delegationDigestCall {
            blsPubkey: bls_pubkey,
            validatorAddress: validator_address,
            delegator,
            deadline,
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
        proof_of_possession_message_bounded(
            &self.blocking_io_guard,
            self.evm_state.get_task_spawner(),
            bls_pubkey,
            validator_address,
        )
        .await
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
        let blocking_io_guard = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_RPC_WORK));
        Self { evm_state, inner_node_network, blocking_io_guard }
    }

    /// Run `work` on the protocol blocking pool under a [`MAX_CONCURRENT_BLOCKING_RPC_WORK`]
    /// permit, and await its value.
    ///
    /// Binds [`spawn_bounded_blocking`] to this instance's guard and task spawner.
    async fn spawn_bounded_blocking<R, F>(
        &self,
        name: &'static str,
        work: F,
    ) -> TelcoinNetworkRpcResult<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        spawn_bounded_blocking(
            &self.blocking_io_guard,
            self.evm_state.get_task_spawner(),
            name,
            work,
        )
        .await
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
    /// EVM reads are synchronous database/CPU work, so they go through
    /// [`Self::spawn_bounded_blocking`] rather than running on the async runtime, under the shared
    /// [`MAX_CONCURRENT_BLOCKING_RPC_WORK`] bound.
    ///
    /// The closure receives an owned [`RethEnv`] and may issue one read
    /// ([`RethEnv::read_consensus_registry`]) or several pinned reads
    /// ([`RethEnv::read_consensus_registry_batch`]); both single- and multi-read endpoints share
    /// this permit/spawn/revert-mapping path.
    ///
    /// On-chain reverts surface to the client eth_call-style (code 3 with revert bytes in
    /// `data`); internal failures are logged server-side and return a generic error.
    async fn spawn_registry_read<R, F>(&self, read: F) -> TelcoinNetworkRpcResult<R>
    where
        R: Send + 'static,
        F: FnOnce(RethEnv) -> EvmReadResult<R> + Send + 'static,
    {
        let evm = self.evm_state.clone();
        self.spawn_bounded_blocking("tn-rpc-registry-read", move || read(evm)).await?.map_err(
            |err| match &err {
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
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future as _,
        pin::pin,
        task::{Context, Poll, Waker},
        thread,
    };
    use tn_types::{BlsKeypair, TaskManager};

    /// Fixed keypair so nothing here depends on an RNG. `[7u8; 32]` is a valid BLS12-381 scalar
    /// (well below the group order, whose first byte is `0x73`).
    fn test_keypair() -> BlsKeypair {
        BlsKeypair::from_bytes(&[7u8; 32]).expect("fixed test scalar is a valid bls private key")
    }

    /// A guard with every permit already taken, so any code path that acquires one cannot proceed.
    async fn exhausted_guard() -> (Arc<Semaphore>, tokio::sync::OwnedSemaphorePermit) {
        let guard = Arc::new(Semaphore::new(1));
        let held = guard.clone().acquire_owned().await.expect("semaphore is open");
        (guard, held)
    }

    /// Moving the decompress to the blocking pool must not change what callers receive: the
    /// response is still the message `construct_proof_of_possession_message` builds, for both
    /// encodings `from_literal_bytes` accepts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pop_message_matches_direct_construction() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let guard = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_RPC_WORK));
        let keypair = test_keypair();
        let address = Address::repeat_byte(0x11);
        let expected: Bytes =
            construct_proof_of_possession_message(keypair.public(), &address).into();

        let from_compressed = proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(keypair.public().to_bytes().to_vec()),
            address,
        )
        .await
        .expect("compressed key is accepted");
        // `BlsPublicKey` implements `Serialize`, so reach the blst inherent `serialize` through the
        // deref rather than letting serde's method win resolution
        let uncompressed = (**keypair.public()).serialize().to_vec();
        let from_uncompressed = proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(uncompressed),
            address,
        )
        .await
        .expect("uncompressed key is accepted");

        assert_eq!(from_compressed, expected);
        assert_eq!(
            from_uncompressed, expected,
            "uncompressed input normalizes to the same message"
        );
        assert_eq!(
            guard.available_permits(),
            MAX_CONCURRENT_BLOCKING_RPC_WORK,
            "permits are returned once the work completes"
        );
    }

    /// The length screen runs before any blocking capacity is committed. With every permit held,
    /// a wrong-length key must still be answered on the very first poll: a flood of malformed keys
    /// cannot queue behind, or displace, real work.
    ///
    /// Deleting the screen from `proof_of_possession_message_bounded` makes this poll return
    /// `Pending` instead, because the call then waits on the exhausted semaphore.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_key_is_rejected_before_taking_a_permit() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let (guard, held) = exhausted_guard().await;

        let mut call = pin!(proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(vec![0u8; BlsPublicKey::COMPRESSED_BYTES - 1]),
            Address::repeat_byte(0x11),
        ));
        let first_poll = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));

        assert!(
            matches!(first_poll, Poll::Ready(Err(TNRpcError::InvalidParams(_)))),
            "a wrong-length key must be rejected on the first poll, before acquiring a permit"
        );
        drop(held);
    }

    /// Only the expensive path consults the guard, which is what makes the screen worth having.
    ///
    /// A closed semaphore fails every acquisition immediately, so it separates the two paths
    /// without any timing: a well-formed key has to acquire, and surfaces `Internal`; a malformed
    /// key is answered by the screen and still surfaces `InvalidParams`.
    ///
    /// Removing the permit acquisition from `spawn_bounded_blocking` makes the well-formed call
    /// succeed here; removing the screen makes the malformed call return `Internal`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_the_expensive_path_consults_the_guard() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let guard = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_RPC_WORK));
        guard.close();
        let keypair = test_keypair();
        let address = Address::repeat_byte(0x11);

        let well_formed = proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(keypair.public().to_bytes().to_vec()),
            address,
        )
        .await;
        let malformed = proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(vec![0u8; BlsPublicKey::COMPRESSED_BYTES - 1]),
            address,
        )
        .await;

        assert!(
            matches!(well_formed, Err(TNRpcError::Internal)),
            "a well-formed key must acquire a permit, so a closed guard has to fail it"
        );
        assert!(
            matches!(malformed, Err(TNRpcError::InvalidParams(_))),
            "a malformed key is screened before the guard is ever consulted"
        );
    }

    /// Capacity is a queue, not a rejection. With the only permit held, a well-formed key parks,
    /// and it completes once capacity frees, so a burst of legitimate callers is served late rather
    /// than failed.
    ///
    /// Swapping the `acquire_owned().await` in `spawn_bounded_blocking` for a `try_acquire_owned()`
    /// makes the first poll return `Ready(Err(Internal))` here instead of `Pending`. Nothing else
    /// in this module distinguishes those two, because every other test either has a free
    /// permit or a closed semaphore, and a closed semaphore fails both forms identically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn at_capacity_a_well_formed_key_queues_rather_than_failing() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let (guard, held) = exhausted_guard().await;
        let keypair = test_keypair();
        let address = Address::repeat_byte(0x11);
        let expected: Bytes =
            construct_proof_of_possession_message(keypair.public(), &address).into();

        let mut call = pin!(proof_of_possession_message_bounded(
            &guard,
            &spawner,
            Bytes::from(keypair.public().to_bytes().to_vec()),
            address,
        ));
        let while_at_capacity = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert!(
            matches!(while_at_capacity, Poll::Pending),
            "at capacity a well-formed key must queue rather than be rejected"
        );

        drop(held);
        let served = call.await.expect("the queued call is served once capacity frees");
        assert_eq!(served, expected);
    }

    /// The work runs on the blocking pool, not on the async worker thread that polled the handler.
    /// This is the property that keeps a request flood from holding worker threads that consensus
    /// tasks are also scheduled on.
    ///
    /// Calling `work()` inline instead of dispatching it makes the two thread ids equal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_work_runs_off_the_async_thread() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let guard = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOCKING_RPC_WORK));
        let caller = thread::current().id();

        let worker =
            spawn_bounded_blocking(&guard, &spawner, "tn-rpc-test", move || thread::current().id())
                .await
                .expect("blocking task delivers its result");

        assert_ne!(caller, worker, "blocking work must not run on the polling thread");
    }

    /// The permit covers the whole blocking op, not just its dispatch. Capacity is only returned
    /// once the work is actually finished, so a caller that gives up early cannot free a slot that
    /// is still occupied.
    ///
    /// Dropping the permit at the await point instead of moving it into the task makes the
    /// mid-flight assertion see a free permit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permit_is_held_until_the_work_completes() {
        let task_manager = TaskManager::new("pop-test");
        let spawner = task_manager.get_spawner();
        let guard = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let call_guard = guard.clone();
        let call_spawner = spawner.clone();
        let call = tokio::spawn(async move {
            spawn_bounded_blocking(&call_guard, &call_spawner, "tn-rpc-test", move || {
                let _ = started_tx.send(());
                // park until the test releases us, so "mid-flight" is a fact, not a race
                let _ = release_rx.recv();
                7u8
            })
            .await
        });

        started_rx.await.expect("blocking work started");
        assert_eq!(guard.available_permits(), 0, "the permit is held while the work runs");

        release_tx.send(()).expect("release the blocking work");
        let result = call.await.expect("task joins").expect("blocking work delivers its result");
        assert_eq!(result, 7);
        assert_eq!(guard.available_permits(), 1, "the permit is returned once the work completes");
    }
}
