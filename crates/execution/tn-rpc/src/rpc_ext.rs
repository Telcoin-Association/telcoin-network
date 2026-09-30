//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    EngineToPrimary, RpcNodeInfo,
};
use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::U64,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, PoisonError},
};
use tn_reth::{
    error::{EvmReadError, EvmReadResult},
    system_calls::ConsensusRegistry,
    RethEnv,
};
use tn_types::{
    construct_proof_of_possession_message, forks::subsecond_timestamp_active, Address,
    BlsPublicKey, Bytes, ConsensusHeader, Epoch, EpochCertificate, EpochDigest, EpochRecord,
    Genesis, NodeMode, SealedHeader, SolCall, SolType, SolValue, TaskSpawner, TimestampMs, B256,
    U256,
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

/// Response for `tn_getBlockTimestampMillis`: an execution block's consensus commit time in
/// milliseconds.
///
/// Execution blocks keep a whole-second `timestamp`. The millisecond commit time lives in the
/// consensus header each block was executed from, which the block references through its
/// `parent_beacon_block_root`; this response pairs the two. Quantities are hex-encoded like the
/// `eth` namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockTimestampMillis {
    /// The execution block's number.
    pub block_number: U64,
    /// The execution block's hash.
    pub block_hash: B256,
    /// The execution block's `timestamp` in whole seconds, as the EVM reports it.
    pub timestamp: U64,
    /// The commit time of the block's consensus header, in milliseconds since the Unix epoch.
    ///
    /// Read from the consensus header's committed sub-dag, not computed from `timestamp`. The one
    /// exception is a block without a consensus header (genesis), which reports
    /// `timestamp * 1000`.
    ///
    /// Non-decreasing within an epoch; blocks executed from one consensus output share a value.
    /// Across an epoch boundary only `timestamp` is guaranteed not to decrease: the blocks of an
    /// epoch's first commit can report up to 998 ms less than the previous epoch's last block,
    /// within the same whole second.
    pub timestamp_millis: U64,
    /// Whether the consensus header's leader epoch commits with millisecond resolution.
    ///
    /// `false` for leader epochs before the sub-second timestamp fork, whose commit times are
    /// whole seconds (`timestamp_millis` is then a multiple of 1000), and for genesis.
    pub sub_second: bool,
    /// The number of the consensus header the block was executed from; absent for genesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consensus_number: Option<U64>,
    /// The digest of the consensus header the block was executed from, which is the block's
    /// `parent_beacon_block_root`; absent for genesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consensus_digest: Option<B256>,
}

impl BlockTimestampMillis {
    /// The response for a block with no consensus header (genesis): its execution timestamp in
    /// milliseconds.
    fn without_consensus(header: &SealedHeader) -> Self {
        Self {
            block_number: U64::from(header.number),
            block_hash: header.hash(),
            timestamp: U64::from(header.timestamp),
            timestamp_millis: U64::from(header.timestamp.saturating_mul(1000)),
            sub_second: false,
            consensus_number: None,
            consensus_digest: None,
        }
    }

    /// The response for a block executed from the consensus header `digest`, whose commit is
    /// `commit`.
    fn with_consensus(header: &SealedHeader, digest: B256, commit: ConsensusCommitTime) -> Self {
        Self {
            timestamp_millis: U64::from(commit.commit_ms.as_millis()),
            sub_second: subsecond_timestamp_active(commit.leader_epoch),
            consensus_number: Some(U64::from(commit.number)),
            consensus_digest: Some(digest),
            ..Self::without_consensus(header)
        }
    }
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
    /// Return the consensus commit time, in milliseconds, of the execution block `block`.
    ///
    /// Accepts a block number, hash, or tag; `pending` resolves as `latest`. Returns `null` for a
    /// block this node does not know: an unknown number or hash, `safe` and `finalized` before
    /// the first block is finalized, and heights below a snapshot-restored node's restored header
    /// window. A known block whose consensus header is missing from local storage (for example,
    /// its epoch's consensus pack is absent) is a "Not Found." error rather than `null`.
    ///
    /// Validators should not expose the `tn` namespace publicly. A request for a block from a
    /// sealed epoch can open that epoch's consensus pack, and the storage layer opens packs
    /// synchronously into a small cache it shares with state sync and peer epoch serving. The
    /// lookup runs off the async runtime and under a tight concurrency bound, but public callers
    /// can still churn that cache until storage opens and evicts packs without blocking; serve the
    /// namespace from non-validating nodes instead.
    #[method(name = "getBlockTimestampMillis")]
    async fn get_block_timestamp_millis(
        &self,
        block: BlockId,
    ) -> TelcoinNetworkRpcResult<Option<BlockTimestampMillis>>;
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
/// Covers the synchronous work this namespace dispatches off the async runtime:
/// [`ConsensusRegistry`] reads, which build a fresh EVM and state snapshot at the canonical tip
/// (database plus CPU), the execution header read behind `getBlockTimestampMillis` (database),
/// and the BLS12-381 G2 decompress behind `proofOfPossessionMessage` (pure CPU, tens of
/// microseconds per call). A semaphore permit is held for the full lifetime of the blocking work,
/// so this bounds true blocking-pool occupancy, not just in-flight requests. Consensus-pack
/// lookups also run on the blocking pool but under their own [`MAX_CONCURRENT_PACK_READS`] bound.
///
/// One bound covers them all deliberately. The budget below is a share of a pool the whole process
/// contends for, so splitting it into a guard per endpoint would let their worst cases add up and
/// silently double the occupancy this number is chosen to permit.
///
/// Mirrors reth's `eth_call` guard (`DEFAULT_MAX_BLOCKING_IO_REQUEST` = 256): tokio's blocking
/// pool defaults to 512 threads and grows unbounded under request load without a bound. Set
/// below reth's 256 because reth's own eth namespace shares the same pool; 64 keeps headroom
/// for BLS signing and engine blocking tasks. Lives at the RPC layer only, so consensus-critical
/// reads (`epoch_state_from_canonical_tip`) are never throttled by RPC traffic.
const MAX_CONCURRENT_BLOCKING_RPC_WORK: usize = 64;

/// Maximum number of consensus-pack lookups `getBlockTimestampMillis` may have in flight.
///
/// A lookup that misses the commit-time cache reaches consensus storage, whose resources it shares
/// with consensus and state sync rather than owning any of them:
///
/// - Each epoch's consensus pack is served by a single thread, and the current epoch's thread also
///   persists consensus output, so every lookup queues behind consensus writes or delays them.
/// - Sealed packs are opened into a ten-entry cache that state sync and peer epoch serving read
///   through as well. A lookup for an uncached sealed epoch opens its pack and evicts the oldest
///   entry, and an eviction that drops the last handle to a pack joins that pack's thread while
///   holding the cache lock every other sealed-pack reader takes.
///
/// The bound is therefore kept at two, which caps how fast public callers can churn that cache
/// and how long they can hold its lock. It is separate from [`MAX_CONCURRENT_BLOCKING_RPC_WORK`]
/// because it guards those storage resources, not the blocking pool: each lookup does occupy a
/// blocking-pool thread (see [`TelcoinNetworkRpcExt::consensus_header_from_pack`]), and this bound
/// adds at most two threads to that budget.
const MAX_CONCURRENT_PACK_READS: usize = 2;

/// Number of consensus headers whose commit time `getBlockTimestampMillis` keeps cached.
///
/// Entries are keyed by consensus header digest, so one entry answers every execution block
/// executed from the same consensus output. A committed header never changes, so entries never
/// need invalidation, only eviction.
const COMMIT_TIME_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(4096).expect("4096 is nonzero");

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
    /// The interface that handles primary <-> engine network communication. Shared so a
    /// consensus-pack lookup can run on the blocking pool (see
    /// [`Self::consensus_header_from_pack`]).
    inner_node_network: Arc<N>,
    /// Bounds the blocking work this namespace dispatches (see
    /// [`MAX_CONCURRENT_BLOCKING_RPC_WORK`]). Acquired before spawning the blocking task and held
    /// until it completes, capping blocking-pool occupancy from RPC load.
    blocking_io_guard: Arc<Semaphore>,
    /// Bounds concurrent consensus-pack lookups (see [`MAX_CONCURRENT_PACK_READS`]). Held by the
    /// lookup task until the pack answers.
    pack_read_guard: Arc<Semaphore>,
    /// Recently resolved consensus commit times, keyed by consensus header digest (see
    /// [`COMMIT_TIME_CACHE_CAPACITY`]).
    commit_times: Mutex<LruCache<B256, ConsensusCommitTime>>,
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

    async fn get_block_timestamp_millis(
        &self,
        block: BlockId,
    ) -> TelcoinNetworkRpcResult<Option<BlockTimestampMillis>> {
        let Some(header) = self.real_execution_header(block).await? else {
            return Ok(None);
        };
        self.block_timestamp_millis(&header).await.map(Some)
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
        let pack_read_guard = Arc::new(Semaphore::new(MAX_CONCURRENT_PACK_READS));
        let commit_times = Mutex::new(LruCache::new(COMMIT_TIME_CACHE_CAPACITY));
        Self {
            evm_state,
            inner_node_network: Arc::new(inner_node_network),
            blocking_io_guard,
            pack_read_guard,
            commit_times,
        }
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

    /// Resolve `block` to a real execution header, or `None` when it names no block this node
    /// can answer for.
    ///
    /// The read is in-memory-tip-aware ([`RethEnv::sealed_header_by_id`]), so blocks that are
    /// executed but not yet persisted resolve too. It may touch the database, so it runs on the
    /// blocking pool under the shared [`MAX_CONCURRENT_BLOCKING_RPC_WORK`] bound. A header that
    /// [`is_real_header`] rejects resolves to `None`.
    async fn real_execution_header(
        &self,
        block: BlockId,
    ) -> TelcoinNetworkRpcResult<Option<SealedHeader>> {
        let evm = self.evm_state.clone();
        let header = self
            .spawn_bounded_blocking("tn-rpc-block-header", move || evm.sealed_header_by_id(block))
            .await?
            .map_err(|err| {
                tracing::warn!(
                    target: "tn::rpc",
                    error = ?err,
                    ?block,
                    "execution header lookup failed"
                );
                TNRpcError::Internal
            })?;
        let real_header_floor = self.evm_state.real_header_floor();
        Ok(header.filter(|header| is_real_header(block, header, real_header_floor)))
    }

    /// Return the cached commit time of consensus header `digest`, if any.
    fn cached_commit_time(&self, digest: &B256) -> Option<ConsensusCommitTime> {
        self.commit_times.lock().unwrap_or_else(PoisonError::into_inner).get(digest).copied()
    }
}

impl<N> TelcoinNetworkRpcExt<N>
where
    N: EngineToPrimary + Send + Sync + 'static,
{
    /// Build the `tn_getBlockTimestampMillis` response for the resolved execution `header`.
    async fn block_timestamp_millis(
        &self,
        header: &SealedHeader,
    ) -> TelcoinNetworkRpcResult<BlockTimestampMillis> {
        // genesis precedes every consensus header. with cancun active from genesis it still
        // carries the field, zeroed as eip-4788 requires, so a zero root reads as absent
        let Some(digest) = header.parent_beacon_block_root.filter(|root| !root.is_zero()) else {
            return Ok(BlockTimestampMillis::without_consensus(header));
        };
        let epoch = RethEnv::extract_epoch_from_header(header.header());
        let commit = self.consensus_commit_time(epoch, digest).await?;
        Ok(BlockTimestampMillis::with_consensus(header, digest, commit))
    }

    /// Resolve the commit time of consensus header `digest` from `epoch`.
    ///
    /// Tries, in order: the commit-time cache; the latest consensus header, which costs no
    /// storage read; then `epoch`'s consensus pack. A digest the pack does not return is
    /// [`TNRpcError::NotFound`]. The execution block that references it exists, so the consensus
    /// header is missing or unreadable locally (for example, an epoch whose pack this node does
    /// not have), and the caller must be able to tell that apart from an unknown block. Failures
    /// are not cached, so the header resolves once its pack arrives.
    async fn consensus_commit_time(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> TelcoinNetworkRpcResult<ConsensusCommitTime> {
        if let Some(cached) = self.cached_commit_time(&digest) {
            return Ok(cached);
        }
        let latest = self.inner_node_network.get_latest_consensus_block();
        let header = if B256::from(latest.digest()) == digest {
            latest
        } else {
            self.consensus_header_from_pack(epoch, digest).await?.ok_or(TNRpcError::NotFound)?
        };
        let commit = ConsensusCommitTime::from(&header);
        self.commit_times.lock().unwrap_or_else(PoisonError::into_inner).put(digest, commit);
        Ok(commit)
    }

    /// Look up consensus header `digest` in `epoch`'s consensus pack on the blocking pool, under a
    /// [`MAX_CONCURRENT_PACK_READS`] permit.
    ///
    /// The storage call is async but does blocking work before it first yields. Reading a sealed
    /// epoch whose pack is not cached opens the pack synchronously (several file opens, about
    /// 4 MiB of bloom-filter reads and a thread spawn), and the cache eviction that makes room for
    /// it can join the evicted pack's thread. Awaited in an async task, all of that would run on
    /// one of the shared runtime's workers, where consensus tasks are scheduled. The lookup
    /// therefore runs on a blocking-pool thread, which drives the storage future to completion
    /// with [`tokio::runtime::Handle::block_on`].
    ///
    /// This is the one place the namespace blocks a thread on a future. `block_on` must never run
    /// on a runtime worker, but a blocking-pool thread is not one: it exists to be blocked, and its
    /// occupancy is bounded by the permit.
    ///
    /// The permit moves into the blocking task, as [`spawn_bounded_blocking`] does for every
    /// endpoint: once a request reaches the pack it is served whether or not the caller is still
    /// waiting. Returning the permit when a disconnecting caller stops waiting, rather than when
    /// the pack answers, would let abandoned lookups pile up past the bound.
    ///
    /// Moving the lookup off the workers does not isolate it from the storage layer's shared pack
    /// cache: a lookup still opens and caches packs that state sync and peer epoch serving also
    /// read through, and an eviction still holds the cache lock while it joins a pack thread.
    /// [`MAX_CONCURRENT_PACK_READS`] limits that interference; removing it needs the storage-side
    /// open and eviction to stop blocking.
    async fn consensus_header_from_pack(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> TelcoinNetworkRpcResult<Option<ConsensusHeader>> {
        let primary = Arc::clone(&self.inner_node_network);
        spawn_bounded_blocking(
            &self.pack_read_guard,
            self.evm_state.get_task_spawner(),
            "tn-rpc-consensus-header",
            move || {
                // blocking-pool threads carry the runtime's handle but are not workers, so
                // block_on is allowed here and keeps the synchronous pack open off the workers
                tokio::runtime::Handle::current()
                    .block_on(primary.consensus_header_by_digest(epoch, digest.into()))
            },
        )
        .await
    }
}

/// What `tn_getBlockTimestampMillis` reports from one consensus header, cached by its digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ConsensusCommitTime {
    /// The consensus header's number.
    number: u64,
    /// The committed sub-dag's commit time
    /// ([`tn_types::CommittedSubDag::commit_timestamp_ms`]).
    commit_ms: TimestampMs,
    /// The epoch of the sub-dag's leader, which decides the commit time's resolution.
    leader_epoch: Epoch,
}

impl From<&ConsensusHeader> for ConsensusCommitTime {
    fn from(header: &ConsensusHeader) -> Self {
        Self {
            number: header.number,
            commit_ms: header.sub_dag.commit_timestamp_ms(),
            leader_epoch: header.sub_dag.leader().epoch(),
        }
    }
}

/// Whether `header`, which `requested` resolved to, is a block this node can answer for.
///
/// A snapshot-restored datadir guarantees real headers only from [`RethEnv::real_header_floor`]
/// up. Below it, genesis aside, heights may hold scaffold placeholders: `ExecHeader::default()`
/// apart from their number, sealed under a zero hash. A placeholder has a zero timestamp and no
/// consensus reference, so answering for it would report a fabricated time of zero; every height
/// below the floor reads as unknown instead. Genesis (number 0) is always real. A by-number
/// request answered with a header of any other number did not find the block it asked for.
fn is_real_header(requested: BlockId, header: &SealedHeader, real_header_floor: u64) -> bool {
    let below_floor = header.number != 0 && header.number < real_header_floor;
    let other_height = matches!(
        requested,
        BlockId::Number(BlockNumberOrTag::Number(number)) if number != header.number
    );
    !below_floor && !other_height
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::core::{to_json_value, JsonValue};
    use std::{
        collections::BTreeMap,
        future::Future as _,
        pin::pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
        thread,
        time::Duration,
    };
    use tempfile::TempDir;
    use tn_types::{
        forks::{seed_signature_fork_epoch_override, subsecond_timestamp_fork_epoch_override},
        test_chain_spec_arc, BlsKeypair, BlsSignature, Certificate, CommittedSubDag,
        ConsensusHeaderDigest, EpochSeedChainValue, ExecHeader, ReputationScores,
        SignatureVerificationState, TaskManager,
    };

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

    /// First epoch of the sub-second timestamp fork in these tests: leaders of epoch 0 are
    /// pre-fork, every later epoch is post-fork.
    const SUBSECOND_FORK_EPOCH: Epoch = 1;

    /// Creation time, in milliseconds, of every fixture leader.
    const LEADER_MS: u64 = 1_700_000_000_123;

    /// Epoch commit floor every fixture sub-dag commits against. It is later than [`LEADER_MS`],
    /// so a post-fork commit is lifted to 1 ms past it: a time that differs from the leader's own
    /// and from any whole second.
    const COMMIT_FLOOR_MS: u64 = 1_700_000_000_456;

    /// Pins this test process's fork gates so every build flavor agrees on which fixture epochs
    /// are pre-fork: the sub-second fork at [`SUBSECOND_FORK_EPOCH`], and the seed-signature fork
    /// it requires at genesis.
    ///
    /// The gates read their `test-utils` environment overrides once per process, so this must run
    /// before anything consults a gate, including building the temp chain. nextest runs each test
    /// in its own process; a single-process `cargo test` run shares one latch, which is why every
    /// test here pins the same values. Reading the overrides back turns a value that latched
    /// before the pin into a named failure.
    fn pin_forks() {
        std::env::set_var("TN_SEED_SIGNATURE_FORK_EPOCH", "0");
        std::env::set_var("TN_SUBSECOND_TIMESTAMP_FORK_EPOCH", SUBSECOND_FORK_EPOCH.to_string());
        assert_eq!(
            seed_signature_fork_epoch_override(),
            Some(0),
            "TN_SEED_SIGNATURE_FORK_EPOCH latched to another value before this test pinned it"
        );
        assert_eq!(
            subsecond_timestamp_fork_epoch_override(),
            Some(SUBSECOND_FORK_EPOCH),
            "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH latched to another value before this test pinned it"
        );
    }

    /// How often each [`FakePrimary`] source was read.
    #[derive(Debug, Default)]
    struct Reads {
        /// Calls to `get_latest_consensus_block`.
        latest: AtomicUsize,
        /// Calls to `consensus_header_by_digest`.
        pack: AtomicUsize,
    }

    impl Reads {
        /// Reads of the latest consensus header so far.
        fn latest(&self) -> usize {
            self.latest.load(Ordering::SeqCst)
        }

        /// Consensus-pack lookups so far.
        fn pack(&self) -> usize {
            self.pack.load(Ordering::SeqCst)
        }
    }

    /// A gate a pack lookup parks on synchronously, without yielding, the way a sealed pack's
    /// synchronous open holds whatever thread polls it.
    #[derive(Debug, Default)]
    struct ThreadGate {
        /// `(entered, released)`: whether a lookup has parked, and whether it may continue.
        state: std::sync::Mutex<(bool, bool)>,
        /// Signalled on every state change.
        changed: std::sync::Condvar,
    }

    impl ThreadGate {
        /// Block the calling thread until the gate is released.
        fn park(&self) {
            let mut state = self.state.lock().expect("gate lock is never poisoned");
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).expect("gate lock is never poisoned");
            }
        }

        /// Whether a lookup parked on the gate within `timeout`.
        fn wait_entered(&self, timeout: Duration) -> bool {
            let state = self.state.lock().expect("gate lock is never poisoned");
            let (state, _) = self
                .changed
                .wait_timeout_while(state, timeout, |(entered, _)| !*entered)
                .expect("gate lock is never poisoned");
            state.0
        }

        /// Let every parked lookup continue.
        fn release(&self) {
            self.state.lock().expect("gate lock is never poisoned").1 = true;
            self.changed.notify_all();
        }
    }

    /// Releases a [`ThreadGate`] when dropped, so a failed assertion never leaves a thread parked
    /// and the runtime unable to shut down.
    struct ReleaseOnDrop(Arc<ThreadGate>);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    /// In-memory [`EngineToPrimary`]: a fixed latest consensus header, plus consensus packs held
    /// as a map from `(epoch, digest)` to header, counting every read.
    #[derive(Debug)]
    struct FakePrimary {
        /// Returned by `get_latest_consensus_block`.
        latest: ConsensusHeader,
        /// The headers `consensus_header_by_digest` can find.
        packs: BTreeMap<(Epoch, B256), ConsensusHeader>,
        /// When set, every pack lookup waits for a permit from this gate before it answers, so a
        /// test controls when the pack responds.
        pack_gate: Option<Arc<Semaphore>>,
        /// When set, every pack lookup blocks its thread on this gate before it answers, as a
        /// synchronous pack open does.
        thread_gate: Option<Arc<ThreadGate>>,
        /// Read counters, shared with the test.
        reads: Arc<Reads>,
    }

    impl FakePrimary {
        /// A primary whose latest consensus header is `latest` and whose packs hold `stored`,
        /// each under its leader's epoch.
        fn new(latest: ConsensusHeader, stored: impl IntoIterator<Item = ConsensusHeader>) -> Self {
            let packs = stored
                .into_iter()
                .map(|header| ((header.sub_dag.leader_epoch(), header.digest().into()), header))
                .collect();
            Self { latest, packs, pack_gate: None, thread_gate: None, reads: Arc::default() }
        }
    }

    impl EngineToPrimary for FakePrimary {
        fn get_latest_consensus_block(&self) -> ConsensusHeader {
            self.reads.latest.fetch_add(1, Ordering::SeqCst);
            self.latest.clone()
        }

        async fn epoch(
            &self,
            _epoch: Option<Epoch>,
            _hash: Option<EpochDigest>,
        ) -> Option<(EpochRecord, EpochCertificate)> {
            unreachable!("epoch records are not exercised by these tests")
        }

        async fn consensus_header_by_digest(
            &self,
            epoch: Epoch,
            digest: ConsensusHeaderDigest,
        ) -> Option<ConsensusHeader> {
            self.reads.pack.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.thread_gate {
                gate.park();
            }
            if let Some(gate) = &self.pack_gate {
                gate.acquire().await.expect("the pack gate is never closed").forget();
            }
            self.packs.get(&(epoch, digest.into())).cloned()
        }

        fn node_info(&self) -> &RpcNodeInfo {
            unreachable!("node info is not exercised by these tests")
        }

        fn node_mode(&self) -> NodeMode {
            unreachable!("node mode is not exercised by these tests")
        }
    }

    /// A `tn` namespace over a fresh temp chain at genesis, with the task manager and datadir the
    /// chain depends on. Fields drop in declaration order, so the chain closes before its datadir
    /// is removed.
    struct TestRpc {
        /// The namespace under test.
        ext: TelcoinNetworkRpcExt<FakePrimary>,
        /// The primary's read counters.
        reads: Arc<Reads>,
        /// Owns the chain's tasks.
        _task_manager: TaskManager,
        /// Holds the chain's database.
        _dir: TempDir,
    }

    impl TestRpc {
        /// Build the namespace over `primary` and a temp chain at genesis.
        fn new(name: &str, primary: FakePrimary) -> Self {
            let dir = TempDir::with_prefix(format!("tn-rpc-{name}")).expect("create test datadir");
            let task_manager = TaskManager::new(name);
            let evm_state =
                RethEnv::new_for_temp_chain(test_chain_spec_arc(), dir.path(), &task_manager, None)
                    .expect("temp chain at genesis");
            let reads = Arc::clone(&primary.reads);
            Self {
                ext: TelcoinNetworkRpcExt::new(evm_state, primary),
                reads,
                _task_manager: task_manager,
                _dir: dir,
            }
        }
    }

    /// Consensus header `number` whose epoch-`epoch` leader was created at [`LEADER_MS`],
    /// committed against the epoch commit floor [`COMMIT_FLOOR_MS`].
    ///
    /// Post-fork the commit lands 1 ms past the floor. Pre-fork the header builder drops the
    /// leader's sub-second part and the commit ignores the floor, so the commit is the leader's
    /// whole second.
    fn consensus_header(number: u64, epoch: Epoch) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.update_header_epoch_for_test(epoch);
        leader.update_header_round_for_test(2);
        leader.update_header_created_at_ms_for_test(TimestampMs::from_millis(LEADER_MS));
        let sub_dag = CommittedSubDag::new_with_commit_floor(
            vec![leader.clone()],
            leader,
            number,
            ReputationScores::default(),
            None,
            Some(TimestampMs::from_millis(COMMIT_FLOOR_MS)),
            EpochSeedChainValue::epoch_root(epoch),
        );
        ConsensusHeader {
            parent_hash: ConsensusHeader::default().digest(),
            sub_dag,
            number,
            extra: B256::ZERO,
        }
    }

    /// A JSON value serialized from `value`.
    fn to_json(value: impl Serialize) -> JsonValue {
        to_json_value(value).expect("test values serialize")
    }

    /// A JSON object with exactly `fields`, for comparing whole responses.
    fn json_object<const N: usize>(fields: [(&str, JsonValue); N]) -> JsonValue {
        to_json(BTreeMap::from(fields))
    }

    /// Execution block `number` executed from `consensus`, with its commit's whole second as the
    /// EVM `timestamp` and its leader's nonce, as the payload builder stamps them.
    fn executed_block(number: u64, consensus: &ConsensusHeader) -> SealedHeader {
        SealedHeader::seal_slow(ExecHeader {
            number,
            timestamp: consensus.sub_dag.commit_timestamp(),
            nonce: consensus.sub_dag.leader().nonce().into(),
            parent_beacon_block_root: Some(consensus.digest().into()),
            ..Default::default()
        })
    }

    /// A block executed from the latest consensus header is answered from that header, with no
    /// consensus-pack lookup.
    ///
    /// The pack holds the header too, so the answer is the same without the fast path; only the
    /// lookup count tells them apart. The reported time is the commit time, which differs from
    /// both the leader's creation time and the EVM timestamp times 1000.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn latest_consensus_header_is_served_without_a_pack_lookup() {
        pin_forks();
        let latest = consensus_header(7, SUBSECOND_FORK_EPOCH);
        let rpc = TestRpc::new("fast-path", FakePrimary::new(latest.clone(), [latest.clone()]));
        let block = executed_block(3, &latest);

        let response =
            rpc.ext.block_timestamp_millis(&block).await.expect("the latest header resolves");

        assert_eq!(rpc.reads.pack(), 0, "the latest consensus header needs no pack lookup");
        assert_eq!(rpc.reads.latest(), 1);
        assert_eq!(
            response,
            BlockTimestampMillis {
                block_number: U64::from(3),
                block_hash: block.hash(),
                timestamp: U64::from(block.timestamp),
                timestamp_millis: U64::from(COMMIT_FLOOR_MS + 1),
                sub_second: true,
                consensus_number: Some(U64::from(7)),
                consensus_digest: Some(latest.digest().into()),
            }
        );
    }

    /// A consensus header resolved once is cached by digest: repeating the request, or asking for
    /// another block executed from the same consensus output, reads neither the latest header nor
    /// the pack, and each block still reports its own number and hash.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cached_commit_time_answers_without_further_reads() {
        pin_forks();
        let stored = consensus_header(8, SUBSECOND_FORK_EPOCH);
        let latest = consensus_header(9, SUBSECOND_FORK_EPOCH);
        let rpc = TestRpc::new("cache", FakePrimary::new(latest, [stored.clone()]));
        let block = executed_block(4, &stored);
        let sibling_block = executed_block(5, &stored);

        let first = rpc.ext.block_timestamp_millis(&block).await.expect("stored header resolves");
        assert_eq!(
            (rpc.reads.latest(), rpc.reads.pack()),
            (1, 1),
            "a cold lookup checks the latest header, then the pack"
        );

        let repeat = rpc.ext.block_timestamp_millis(&block).await.expect("cached");
        let sibling = rpc.ext.block_timestamp_millis(&sibling_block).await.expect("cached");
        assert_eq!((rpc.reads.latest(), rpc.reads.pack()), (1, 1), "cache hits read nothing");

        assert_eq!(first.timestamp_millis, U64::from(COMMIT_FLOOR_MS + 1));
        assert_eq!(first.consensus_number, Some(U64::from(8)));
        assert_eq!(repeat, first);
        assert_eq!(
            sibling,
            BlockTimestampMillis {
                block_number: U64::from(5),
                block_hash: sibling_block.hash(),
                ..first
            }
        );
    }

    /// A block whose consensus header is in no local pack is `NotFound`, which clients can tell
    /// apart from the `null` of an unknown block. The miss is not cached, so the header resolves
    /// once its pack arrives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_consensus_header_is_not_found() {
        pin_forks();
        let missing = consensus_header(8, SUBSECOND_FORK_EPOCH);
        let latest = consensus_header(9, SUBSECOND_FORK_EPOCH);
        let rpc = TestRpc::new("missing-pack", FakePrimary::new(latest, Vec::new()));
        let block = executed_block(4, &missing);

        for attempt in 1..=2 {
            let result = rpc.ext.block_timestamp_millis(&block).await;
            assert!(matches!(result, Err(TNRpcError::NotFound)), "attempt {attempt}: {result:?}");
            assert_eq!(rpc.reads.pack(), attempt, "a miss is looked up again, never cached");
        }
    }

    /// The pack-read permit stays with the lookup task until the pack answers, even when the
    /// caller stops waiting first, so abandoned lookups cannot push the pack thread past
    /// [`MAX_CONCURRENT_PACK_READS`].
    ///
    /// Holding the permit in the caller instead of moving it into the task returns it the moment
    /// the caller is dropped, which fails the mid-flight assertion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pack_permit_is_held_until_the_pack_answers() {
        pin_forks();
        let stored = consensus_header(8, SUBSECOND_FORK_EPOCH);
        let pack_gate = Arc::new(Semaphore::new(0));
        let mut primary = FakePrimary::new(ConsensusHeader::default(), [stored.clone()]);
        primary.pack_gate = Some(Arc::clone(&pack_gate));
        let rpc = TestRpc::new("pack-permit", primary);
        let guard = Arc::clone(&rpc.ext.pack_read_guard);

        let mut call = Box::pin(
            rpc.ext.consensus_header_from_pack(SUBSECOND_FORK_EPOCH, stored.digest().into()),
        );
        assert!(
            call.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending(),
            "the pack has not answered yet"
        );
        // the caller gives up, as a disconnecting client's handler future does
        drop(call);
        assert_eq!(
            guard.available_permits(),
            MAX_CONCURRENT_PACK_READS - 1,
            "the abandoned lookup still holds its permit"
        );

        pack_gate.add_permits(1);
        let all_permits = u32::try_from(MAX_CONCURRENT_PACK_READS).expect("small constant");
        let returned =
            tokio::time::timeout(Duration::from_secs(10), guard.acquire_many(all_permits))
                .await
                .expect("the permit returns once the pack answers")
                .expect("the guard is never closed");
        drop(returned);
        assert_eq!(rpc.reads.pack(), 1);
    }

    /// A pack lookup never occupies a runtime worker: while one blocks its thread the way a
    /// sealed pack's synchronous open does, the runtime's only worker still runs other tasks, and
    /// the lookup answers once its thread is released.
    ///
    /// Awaiting the lookup in an async task instead parks that worker, so the probe task never
    /// runs and the probe assertion fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn pack_lookup_runs_off_the_runtime_workers() {
        pin_forks();
        let stored = consensus_header(8, SUBSECOND_FORK_EPOCH);
        let gate = Arc::new(ThreadGate::default());
        let mut primary = FakePrimary::new(ConsensusHeader::default(), [stored.clone()]);
        primary.thread_gate = Some(Arc::clone(&gate));
        let rpc = TestRpc::new("pack-off-worker", primary);
        let _release = ReleaseOnDrop(Arc::clone(&gate));

        let mut call = Box::pin(
            rpc.ext.consensus_header_from_pack(SUBSECOND_FORK_EPOCH, stored.digest().into()),
        );
        assert!(
            call.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending(),
            "the pack has not answered yet"
        );
        // the probe must be queued only once the lookup holds its thread, or it could run first
        // on the worker and pass however the lookup was scheduled
        assert!(gate.wait_entered(Duration::from_secs(10)), "the lookup reached the pack");

        // std channel: the test body runs outside the runtime's workers and may block on it
        let (probe_tx, probe_rx) = std::sync::mpsc::channel();
        rpc.ext.evm_state.get_task_spawner().spawn_task("tn-rpc-test-probe", async move {
            // a send error only means the receiver timed out and the test already failed
            let _ = probe_tx.send(());
            Ok(())
        });
        assert!(
            probe_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the runtime worker stayed free while the pack lookup blocked its thread"
        );

        gate.release();
        let header = call.await.expect("the lookup completes once released");
        assert_eq!(header, Some(stored));
        assert_eq!(rpc.reads.pack(), 1);
    }

    /// A leader epoch before the sub-second fork commits in whole seconds: `subSecond` is false
    /// and the reported time is the commit's whole second in milliseconds. A post-fork leader
    /// created at the same instant keeps its millisecond commit time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_fork_commit_reports_whole_seconds() {
        pin_forks();
        let pre_fork = consensus_header(5, SUBSECOND_FORK_EPOCH - 1);
        let post_fork = consensus_header(6, SUBSECOND_FORK_EPOCH);
        let primary =
            FakePrimary::new(ConsensusHeader::default(), [pre_fork.clone(), post_fork.clone()]);
        let rpc = TestRpc::new("pre-fork", primary);

        let pre = rpc
            .ext
            .block_timestamp_millis(&executed_block(2, &pre_fork))
            .await
            .expect("pre-fork header resolves");
        let post = rpc
            .ext
            .block_timestamp_millis(&executed_block(3, &post_fork))
            .await
            .expect("post-fork header resolves");

        assert!(!pre.sub_second, "a pre-fork leader commits in whole seconds");
        assert_eq!(pre.timestamp_millis, U64::from(LEADER_MS / 1000 * 1000));
        assert_eq!(pre.consensus_number, Some(U64::from(5)));
        assert!(post.sub_second, "a post-fork leader commits in milliseconds");
        assert_eq!(post.timestamp_millis, U64::from(COMMIT_FLOOR_MS + 1));
    }

    /// Genesis has no consensus header. Every id that selects it reports the genesis timestamp in
    /// milliseconds, `subSecond: false`, and no consensus fields, without consulting consensus.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn genesis_reports_its_execution_timestamp_in_millis() {
        pin_forks();
        let rpc = TestRpc::new("genesis", FakePrimary::new(ConsensusHeader::default(), Vec::new()));
        let genesis = rpc.ext.evm_state.chainspec().sealed_genesis_header();
        // the zero-root branch, not an absent field, is what answers genesis on this chain
        assert_eq!(genesis.parent_beacon_block_root, Some(B256::ZERO));
        let expected = json_object([
            ("blockNumber", to_json("0x0")),
            ("blockHash", to_json(genesis.hash())),
            ("timestamp", to_json(format!("{:#x}", genesis.timestamp))),
            ("timestampMillis", to_json(format!("{:#x}", genesis.timestamp * 1000))),
            ("subSecond", to_json(false)),
        ]);
        let module = rpc.ext.into_rpc();

        for id in [
            to_json("earliest"),
            to_json("latest"),
            to_json("pending"),
            to_json("0x0"),
            to_json(genesis.hash()),
        ] {
            let response: JsonValue = module
                .call("tn_getBlockTimestampMillis", [id.clone()])
                .await
                .expect("genesis resolves");
            assert_eq!(response, expected, "{id}");
        }
        assert_eq!((rpc.reads.latest(), rpc.reads.pack()), (0, 0), "genesis never reads consensus");
    }

    /// Ids that resolve to no block answer `null`, not an error: an unknown number or hash, and
    /// `safe`/`finalized` before the first block is finalized.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_blocks_resolve_to_null() {
        pin_forks();
        let rpc = TestRpc::new("unknown", FakePrimary::new(ConsensusHeader::default(), Vec::new()));
        let module = rpc.ext.into_rpc();

        for id in [
            to_json("0x1"),
            to_json(format!("{:#x}", u64::MAX)),
            to_json(B256::repeat_byte(0x42)),
            to_json("safe"),
            to_json("finalized"),
        ] {
            let response: JsonValue = module
                .call("tn_getBlockTimestampMillis", [id.clone()])
                .await
                .expect("an unknown block is not an error");
            assert_eq!(response, JsonValue::Null, "{id}");
        }
    }

    /// Snapshot-restore scaffolding below the real-header floor reads as unknown whichever id
    /// selects it, genesis never does, and a by-number request answered at another height did
    /// not find its block.
    #[test]
    fn scaffold_and_mismatched_headers_are_not_real() {
        let at = |number: u64| SealedHeader::seal_slow(ExecHeader { number, ..Default::default() });
        let floor = 100;

        assert!(is_real_header(BlockId::number(0), &at(0), floor), "genesis is always real");
        assert!(!is_real_header(BlockId::number(50), &at(50), floor), "scaffold height");
        assert!(!is_real_header(BlockId::number(99), &at(99), floor), "just below the floor");
        assert!(is_real_header(BlockId::number(100), &at(100), floor), "the floor is real");
        assert!(!is_real_header(BlockId::latest(), &at(50), floor), "tags are guarded too");
        assert!(is_real_header(BlockId::latest(), &at(150), floor));
        assert!(!is_real_header(BlockId::number(7), &at(8), 0), "answered at another height");
        assert!(is_real_header(BlockId::number(7), &at(7), 0), "no floor without a restore");
    }

    /// The response serializes with the camelCase keys and hex quantities clients rely on, omits
    /// the consensus fields when they are absent, and round-trips.
    #[test]
    fn response_serializes_camel_case_hex_quantities() {
        let full = BlockTimestampMillis {
            block_number: U64::from(42),
            block_hash: B256::repeat_byte(0xab),
            timestamp: U64::from(1_700_000_000u64),
            timestamp_millis: U64::from(COMMIT_FLOOR_MS + 1),
            sub_second: true,
            consensus_number: Some(U64::from(7)),
            consensus_digest: Some(B256::repeat_byte(0xcd)),
        };
        let genesis = BlockTimestampMillis {
            sub_second: false,
            consensus_number: None,
            consensus_digest: None,
            ..full.clone()
        };

        let full_json = to_json(&full);
        let genesis_json = to_json(&genesis);

        assert_eq!(
            full_json,
            json_object([
                ("blockNumber", to_json("0x2a")),
                ("blockHash", to_json(format!("0x{}", "ab".repeat(32)))),
                ("timestamp", to_json(format!("{:#x}", 1_700_000_000u64))),
                ("timestampMillis", to_json(format!("{:#x}", COMMIT_FLOOR_MS + 1))),
                ("subSecond", to_json(true)),
                ("consensusNumber", to_json("0x7")),
                ("consensusDigest", to_json(format!("0x{}", "cd".repeat(32)))),
            ])
        );
        assert_eq!(
            genesis_json,
            json_object([
                ("blockNumber", to_json("0x2a")),
                ("blockHash", to_json(format!("0x{}", "ab".repeat(32)))),
                ("timestamp", to_json(format!("{:#x}", 1_700_000_000u64))),
                ("timestampMillis", to_json(format!("{:#x}", COMMIT_FLOOR_MS + 1))),
                ("subSecond", to_json(false)),
            ])
        );
        assert_eq!(BlockTimestampMillis::deserialize(full_json).expect("round-trips"), full);
        assert_eq!(BlockTimestampMillis::deserialize(genesis_json).expect("round-trips"), genesis);
    }
}
