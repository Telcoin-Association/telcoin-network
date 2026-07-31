//! Module for solidity interface.
//!
//! These compile into types for interacting with smart contracts through
//! System Calls.
//!
//! The `sol!` blocks bind three contracts from the tn-contracts submodule:
//! - `ConsensusRegistry` — validator lifecycle (mint/stake/activate/exit/burn), epoch and committee
//!   views, and the three epoch-boundary mutators the protocol invokes as system calls in the
//!   closing block: `applyIncentives`, then `applySlashes`, then `concludeEpoch`.
//! - `LegacyConsensusRegistry` — the pre-fork registry's epoch-close surface, frozen so pre-fork
//!   epoch closes replay byte-identically (see the code-hash gate in `evm/block.rs`).
//! - `WorkerConfigs` — per-worker base-fee strategy used for base fee adjustment, plus the
//!   `setWorkerConfigsData` mutator the epoch-closing block invokes to record each EIP-1559
//!   worker's next-epoch base fee. Its `getAllWorkerConfigs` return is decoded in exactly one
//!   place, [`decode_worker_fee_configs`], shared by the pinned read path and the closing block.
//!
//! Two pinned addresses live here: `SYSTEM_ADDRESS` (0xfff...fffe), the reserved caller the EVM
//! uses for system transactions (the custom handler exempts it from fee/beneficiary
//! accounting), and `CONSENSUS_REGISTRY_ADDRESS`, the registry's fixed deployment address.
//!
//! NOTE: slashing is not live — the protocol calls `applySlashes` with an empty array. Do not
//! assume validators can currently be slashed in-protocol.

use alloy::{primitives::address, sol};
use tn_types::{
    gas_accumulator::{WorkerConfigEntry, WorkerFeeConfig},
    Address, Epoch,
};

/// The system address.
pub(super) const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

/// Precompile genesis bytecode.
///
/// This is intercepted by revm at runtime and prevents a reth issue where it skips calls with no
/// bytecode.
///
/// A single `0xfe` (INVALID) byte of code is used so the account is non-empty (never state-pruned)
/// and any call that bypasses precompile dispatch reverts instead of succeeding against an EOA.
pub const PRECOMPILE_GENESIS_BYTECODE: &[u8] = &[0xfe];

/// The address for consensus registry.
pub const CONSENSUS_REGISTRY_ADDRESS: Address =
    address!("07E17e17E17e17E17e17E17E17E17e17e17E17e1");

// ConsensusRegistry interface. See tn-contracts submodule.
sol!(
    /// Consensus registry.
    #[sol(rpc)]
    contract ConsensusRegistry {
        /// The validator's eligibility status for being
        /// considered in the next committee.
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        enum ValidatorStatus {
            /// Undefined status - default value.
            Undefined,
            /// The validator is staked but not eligible for participating
            /// in consensus.
            Staked,
            /// The validator is staked and has indicated it is ready
            /// to participate in committee to earn rewards.
            PendingActivation,
            /// The validator is actively participating in consensus.
            Active,
            /// The validator has indicated interest to exit the protocol.
            PendingExit,
            /// The validator is no longer participating in consensus.
            Exited,
            /// Match any status (also indicates `Retired`)
            Any
        }

        /// The validator's information.
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct ValidatorInfo {
            /// The address based on ECDSA public key.
            address validatorAddress;
            /// The epoch which the validator's status
            /// become "Active" and eligible to participate
            /// in a committee.
            uint32 activationEpoch;
            /// The epoch that the validator exited the protocol.
            uint32 exitEpoch;
            /// The current status of the validator.
            ValidatorStatus currentStatus;
            /// The validator is permanently disqualified from consensus.
            bool isRetired;
            /// The configuration for validators stake.
            ///
            /// This supports updating stake amount.
            uint8 stakeVersion;
            /// GSMA region identifier for geographic diversity.
            /// 0 = unspecified, 1-8 = assigned regions.
            uint8 region;
        }

        /// The epoch info stored on-chain.
        #[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
        struct EpochInfo {
            /// The committee of validators responsible for the epoch.
            address[] committee;
            /// The amount of TEL divied up between all leaders
            /// that produced blocks for the epochs.
            /// This amount is distributed based on the amount of blocks
            /// each leader produced.
            uint256 epochIssuance;
            /// The execution block height when the epoch started and the
            /// committee became active.
            uint64 blockHeight;
            /// The epoch's id.
            uint32 epochId;
            /// The duration for the epoch (in secs).
            ///
            /// NOTE: this is set at the start of each epoch based on the
            /// current value of the `StakeConfig`.
            uint32 epochDuration;
            /// The stake version to use for rewards calculations.
            uint8 stakeVersion;
        }

        /// The rewards applied right before concluding the epoch.
        /// This is provided by the protocol.
        ///
        /// NOTE: this is part of the StakeManager contract.
        #[derive(Debug)]
        struct RewardInfo {
            /// The validator to receive rewards.
            address validatorAddress;
            /// The number of consensus blocks for which they were the leader.
            uint256 consensusHeaderCount;
        }

        /// Slash information for system calls to decrement outstanding validator balances
        /// Currently disabled during MNO pilot.
        #[derive(Debug)]
        struct Slash {
            /// The validator to slash.
            address validatorAddress;
            /// The amount to slash.
            uint256 amount;
        }

        /// The configuration for consensus.
        #[derive(Debug)]
        struct StakeConfig {
            /// The fixed stake amount.
            uint256 stakeAmount;
            /// The min amount allowed to withdraw.
            uint256 minWithdrawAmount;
            /// The total amount issued per epoch.
            uint256 epochIssuance;
            /// The duration for the epoch (in secs).
            uint32 epochDuration;
        }

        /// Represents a validator's BLS12-381 proof of possession: a 48-byte compressed G1
        /// signature, verified by the native precompile against the separately-supplied 96-byte
        /// compressed `blsPubkey`.
        #[derive(Debug)]
        struct ProofOfPossession {
            bytes signature;
        }

        /// Initialize the contract.
        #[derive(Debug)]
        constructor(
            /// The configuration for staking.
            StakeConfig memory genesisConfig_,
            /// The initial validators with stake.
            ValidatorInfo[] memory initialValidators_,
            /// The initial validators' BLS12-381 public keys.
            bytes[] memory blsPubkeys_,
            /// The initial validators' proofs of possession
            ProofOfPossession[] memory proofsOfPossession,
            /// The address of the owner.
            address owner_
        ) external;

        /// Distribute the concluding epoch's issuance to leaders. First of the three
        /// epoch-boundary system calls; the protocol sequences it before `applySlashes` so
        /// weights reflect pre-slash collateral. May be empty while issuance is disabled.
        function applyIncentives(RewardInfo[] calldata rewardInfos) external;
        /// Apply the concluding epoch's slashes, ejecting validators slashed to zero. Second of
        /// the three boundary calls; MUST run before `concludeEpoch` so the committee the protocol
        /// then assembles, and the size it validates against, both reflect any ejections. Always
        /// empty while slashing is disabled.
        function applySlashes(Slash[] calldata slashes) external;
        /// Settle queued stake version changes and rotate the epoch, seating `newCommittee`. Final
        /// boundary call; the protocol reads `nextCommitteeSize` and assembles `newCommittee`
        /// after `applySlashes`, so the size check here cannot revert against a stale pre-slash
        /// view and settlement reads post-slash balances.
        function concludeEpoch(address[] calldata newCommittee) external;
        /// One-time in-protocol fork migration: back-fills the appended per-status `validatorSets`
        /// and the cached `eligibleValidatorCount` from the preserved `currentStatus` source of
        /// truth after the registry bytecode is swapped in place. System-gated and idempotent.
        function migrateValidatorSets() external;
        /// Number of committee-eligible validators (union of `PendingActivation`, `Active`,
        /// `PendingExit`). Single cached SLOAD; read back to confirm a successful migration.
        function getEligibleValidatorCount() external view returns (uint256);
        /// Return the current epoch.
        function getCurrentEpoch() public view returns (uint32) ;
        /// Helper function to get the epoch info from the current epoch.
        function getCurrentEpochInfo() external view returns (EpochInfo memory currentEpochInfo);
        /// Return committee epoch info for a specific epoch.
        function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory epochInfo);
        /// Return the validator addresses for exactly the given status's set. Reverts on `Undefined`
        /// (0) and `Any` (6); neither folds statuses. The committee-eligible pool is the off-chain
        /// union of `Active`, `PendingActivation`, and `PendingExit`.
        function getValidators(uint8 status) public view returns (address[] memory);
        /// Like `getValidators`, but returns the full `ValidatorInfo` structs for the status's set.
        function getValidatorsInfo(uint8 status) public view returns (ValidatorInfo[] memory);
        /// Fetch the committee for a given epoch.
        function getCommitteeValidators(uint32 epoch) external view returns (ValidatorInfo[] memory);
        /// Fetch the BLS pubkey for a given validator address.
        function getBlsPubkey(address validatorAddress) external view returns (bytes memory);
        /// Fetch the BLS pubkeys for the committee of a given epoch.
        function getCommitteeBlsPubkeys(uint32 epoch) external view returns (bytes[] memory);
        /// Fetch the `ValidatorInfo` for a give address.
        function getValidator(address validatorAddress) external view returns (ValidatorInfo memory);
        /// Mint an NFT for validator to stake.
        function mint(address validatorAddress) external override onlyOwner;
        /// Burn a validator's NFT, forcibly ejecting it from current and future committees.
        function burn(address validatorAddress) external override onlyOwner;
        /// Stake to the consensus registry.
        function stake(bytes calldata blsPubkey, ProofOfPossession calldata proofOfPossession) external override;
        /// Activate node for committee selection.
        /// Normally called by staker after node is synced.
        function activate() external override whenNotPaused;
        /// Initiate exit from protocol.
        function beginExit() external override whenNotPaused;
        /// Retrieve the claimable rewards accrued for a given validator address.
        function getRewards(address validatorAddress) public view virtual returns (uint256);
        /// Returns the next committee size
        function getNextCommitteeSize() external view returns (uint16);
        /// Returns the current stake config version.
        function getCurrentStakeVersion() external view returns (uint8);
        /// Returns true if the BLS pubkey belongs to a known validator.
        ///
        /// NOTE (post-`CONSENSUS_REGISTRY_FORK_EPOCH`): the upgraded contract keys its internal
        /// dedup map by a masked-x `_blsKeyId` rather than `keccak(full key)`, and the fork's
        /// `migrateValidatorSets()` does not re-key legacy entries. As an accepted, documented gap,
        /// `isValidator(legacyKey)` returns `false` for validators that staked before the fork. This
        /// is RPC-only and not consensus-critical (the protocol reads stored `blsPubkeys` via
        /// `getCommitteeBlsPubkeys`, never this map). See `tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH`.
        function isValidator(bytes calldata blsPubkey) external view returns (bool);
        /// Returns true if the validator's stake originates from a delegator.
        function isDelegated(address validatorAddress) external view returns (bool);
        /// Returns true if the validator is permanently retired.
        function isRetired(address validatorAddress) external view returns (bool);
        /// Returns the validator's (outstandingBalance, initialStake, rewards).
        function getBalanceBreakdown(address validatorAddress) external view returns (uint256, uint256, uint256);
        /// Returns the EIP-712 digest a validator signs to accept a delegation.
        function delegationDigest(bytes memory blsPubkey, address validatorAddress, address delegator, uint256 deadline) external view returns (bytes32);
        /// Issuance not yet distributed to validators (public variable getter).
        function undistributedIssuance() external view returns (uint256);
        /// Sets the GSMA region identifier for a validator (0=unspecified, 1-255=assigned regions).
        /// Only callable by governance (owner).
        function setValidatorRegion(address validatorAddress, uint8 region) external;
    }

    /// Worker fee configs for base fee adjustment.
    #[sol(rpc)]
    contract WorkerConfigs {
        /// Initialize with per-worker configs and owner.
        constructor(
            uint8[] memory strategies,
            uint64[] memory values,
            uint184[] memory datas,
            address owner_,
        );
        /// Get the stored fee config for a worker.
        function getWorkerConfig(uint16 workerId)
            external view
            returns (uint8 strategy, uint64 value, uint184 data);
        /// Get every worker's config in a single call.
        function getAllWorkerConfigs()
            external view
            returns (
                uint16 count,
                uint8[] memory strategies,
                uint64[] memory values,
                uint184[] memory datas,
            );
        /// Set the number of workers for the next epoch.
        function setNumWorkers(uint16 numWorkers_) external;
        /// Set the config for a worker by worker id.
        /// NOTE: this must be called before calling `setNumWorkers`.
        function setWorkerConfig(uint16 workerId, uint8 strategy, uint64 value, uint184 data) external;
        /// Update strategy-specific packed data for multiple workers. System call only;
        /// each worker's strategy and value are preserved, and only previously configured
        /// workers may be updated.
        function setWorkerConfigsData(uint16[] calldata workerIds, uint184[] calldata datas) external;
        /// Update config values for multiple workers. System call only; each worker's
        /// strategy and data are preserved, and only previously configured workers may be
        /// updated.
        function setWorkerConfigsValue(uint16[] calldata workerIds, uint64[] calldata values) external;
        /// Raise the highest strategy id the contract accepts. Owner only and strictly
        /// increasing, in lockstep with the protocol release that ships the new strategy —
        /// and only after every validator is confirmed running it (see
        /// [`decode_worker_fee_configs`] for why a mixed fleet diverges).
        function setMaxStrategy(uint8 newMaxStrategy) external;
        /// The highest strategy id the contract currently accepts.
        function MAX_STRATEGY() external view returns (uint8);
        /// Retrieve the number of workers for the protocol.
        function numWorkers() external view returns (uint16);
    }
);

// The epoch-close surface of the PRE-fork `ConsensusRegistry` deployment. While the deployed
// registry still carries the pre-fork code hash, epoch closes must replay this exact two-call
// sequence byte-for-byte — re-executed pre-fork blocks must derive identical state roots
// without leaning on contract-semantics arguments about which extra calls would no-op; the
// code-hash gate in `evm/block.rs` routes between the two. Frozen: these signatures must stay
// byte-identical to the calls the historical chain executed.
sol!(
    /// Pre-fork `ConsensusRegistry` epoch-close interface.
    contract LegacyConsensusRegistry {
        /// The rewards applied before concluding the epoch. Field layout is identical to
        /// `ConsensusRegistry::RewardInfo`.
        #[derive(Debug)]
        struct RewardInfo {
            /// The validator to receive rewards.
            address validatorAddress;
            /// The number of consensus blocks for which they were the leader.
            uint256 consensusHeaderCount;
        }

        /// Distribute the concluding epoch's issuance. Runs before `concludeEpoch`.
        function applyIncentives(RewardInfo[] calldata rewardInfos) external;
        /// Conclude the current epoch with the new committee.
        function concludeEpoch(address[] calldata newCommittee) external;
    }
);

/// The state of consensus retrieved from chain.
#[derive(Debug)]
pub struct EpochState {
    /// The epoch number.
    pub epoch: Epoch,
    /// The `EpochInfo` read from the registry.
    pub epoch_info: ConsensusRegistry::EpochInfo,
    /// The collection of validator info.
    pub validators: Vec<ConsensusRegistry::ValidatorInfo>,
    /// The BLS12-381 public keys for the committee validators.
    pub bls_pubkeys: Vec<tn_types::Bytes>,
    /// The timestamp for when the previous epoch closed.
    ///
    /// This time plus the `EpochInfo::epochDuration` creates the timestamp for the next epoch
    /// boundary.
    pub epoch_start: u64,
}

/// Decode a `getAllWorkerConfigs()` return payload into the on-chain worker count and one
/// [`WorkerConfigEntry`] per worker.
///
/// The single decode seam for the `WorkerConfigs` table: the pinned read path
/// (`RethEnv::worker_fee_configs_inner`) and the epoch-closing block's base-fee recording both
/// route through here, so the strategy mapping and the arity check cannot drift between the node
/// that reads a worker's fee strategy and the block that prices its next-epoch fee.
///
/// Errors are returned as strings for the caller to classify: every failure here is a
/// deterministic product of the return payload (identical on every node reading the same block),
/// so callers map them into their chain-global error variant.
///
/// Fail-open on unknown strategy ids: a strategy this node does not recognize (only possible when
/// a future contract version introduces one before this node is upgraded) is NOT an error — it
/// falls back to [`WorkerFeeConfig::Eip1559`] with a warning, preserving liveness instead of
/// halting all validators on the unrecognized id.
///
/// The fallback is deterministic per BUILD, not across builds — and this seam feeds a state
/// WRITE, not just the read path: the closing block's `setWorkerConfigsData` takes both its
/// membership (an unknown id maps to `Eip1559` and is therefore written; `Static` rows are
/// skipped) and its `data` values from these entries, so two builds that disagree about a
/// strategy id produce different closing-block state roots — and that block's hash is the
/// quorum-signed `EpochRecord::final_state`. LOCKSTEP FLEET-UPGRADE REQUIREMENT (same discipline
/// as `SYSTEM_CALL_GAS_LIMIT` in `evm/mod.rs` and the fork rollout in `tn_types::forks`): every
/// validator must be confirmed running the release that ships a new strategy id BEFORE governance
/// sends `setMaxStrategy(N)` and assigns it. A uniform old-build fleet does not split, but is not
/// benign either — every node identically reinterprets the row's `value` as a 1559 `target_gas`
/// and writes a silently wrong fee into consensus state.
pub(crate) fn decode_worker_fee_configs(
    bytes: &[u8],
) -> Result<(u16, Vec<WorkerConfigEntry>), String> {
    let ret =
        <WorkerConfigs::getAllWorkerConfigsCall as alloy::sol_types::SolCall>::abi_decode_returns(
            bytes,
        )
        .map_err(|e| {
            format!("worker configs return decode failed (contract absent at this block?): {e}")
        })?;

    let num_workers = ret.count as usize;
    if ret.strategies.len() != num_workers
        || ret.values.len() != num_workers
        || ret.datas.len() != num_workers
    {
        return Err(format!(
            "worker config arity mismatch: count={num_workers}, strategies={}, values={}, datas={}",
            ret.strategies.len(),
            ret.values.len(),
            ret.datas.len(),
        ));
    }

    let mut entries = Vec::with_capacity(num_workers);
    for (worker_id, ((&strategy, &value), &data)) in
        ret.strategies.iter().zip(ret.values.iter()).zip(ret.datas.iter()).enumerate()
    {
        let config = match strategy {
            0 => WorkerFeeConfig::Eip1559 { target_gas: value },
            1 => WorkerFeeConfig::Static { fee: value },
            s => {
                // The contract rejects unknown strategies, so this branch only fires when a
                // future contract version introduces a strategy this node hasn't been
                // updated to understand. Fall back to EIP-1559 to preserve liveness instead
                // of halting all validators.
                tracing::warn!(
                    target: "tn::reth",
                    worker_id,
                    strategy = s,
                    "unknown fee strategy; falling back to strategy 0 (Eip1559)"
                );
                WorkerFeeConfig::Eip1559 { target_gas: value }
            }
        };
        entries.push(WorkerConfigEntry { config, data });
    }

    Ok((ret.count, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        primitives::{aliases::U184, keccak256},
        sol_types::SolCall,
    };

    /// The hand-written `delegationDigest` binding must track the on-chain 4-argument selector. If
    /// the contract selector changes and this binding does not, `tn_delegationDigest` reverts
    /// at runtime instead of failing to compile, so pin the selector here.
    #[test]
    fn delegation_digest_binding_selector_is_current() {
        let expected: [u8; 4] =
            keccak256("delegationDigest(bytes,address,address,uint256)")[..4].try_into().unwrap();
        assert_eq!(ConsensusRegistry::delegationDigestCall::SELECTOR, expected);
    }

    /// An unknown strategy id must decode fail-open to `Eip1559` (deterministic per build), never
    /// `Err`: this seam feeds both the entry-time config read and the closing block's
    /// `setWorkerConfigsData` write set, so erroring here would halt every validator the moment a
    /// newer contract version introduces a strategy id this build predates. Pins the fallback arm
    /// (previously untested) alongside a known-strategy row to prove the mapping is per-row.
    #[test]
    fn unknown_strategy_decodes_fail_open_to_eip1559() {
        use tn_types::gas_accumulator::WorkerFeeConfig;

        let encoded = WorkerConfigs::getAllWorkerConfigsCall::abi_encode_returns(
            &WorkerConfigs::getAllWorkerConfigsReturn {
                count: 2,
                strategies: vec![2, 1],
                values: vec![777, 42],
                datas: vec![U184::from(5u64), U184::ZERO],
            },
        );

        let (count, entries) =
            decode_worker_fee_configs(&encoded).expect("unknown strategy is fail-open, not Err");
        assert_eq!(count, 2);
        assert_eq!(
            entries[0].config,
            WorkerFeeConfig::Eip1559 { target_gas: 777 },
            "unknown id 2 must fall back to Eip1559 over the row's value"
        );
        assert_eq!(entries[0].data, U184::from(5u64), "the row's data word rides along");
        assert_eq!(entries[1].config, WorkerFeeConfig::Static { fee: 42 });
        assert_eq!(entries[1].data, U184::ZERO);
    }
}
