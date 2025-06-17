//! Module for solidity interface.
//!
//! These compile into types for interacting with smart contracts through
//! System Calls.

use alloy::{primitives::address, sol};
use tn_types::{Address, Epoch};

/// The system address.
pub(super) const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

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
        #[derive(Debug)]
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
        #[derive(Debug)]
        struct ValidatorInfo {
            /// The BLS12-381 public key.
            bytes blsPubkey;
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
            /// The validator received stake through delegation.
            bool isDelegated;
            /// The configuration for validators stake.
            ///
            /// This supports updating stake amount.
            uint8 stakeVersion;
        }

        /// The epoch info stored on-chain.
        #[derive(PartialEq, Debug)]
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

        /// Initialize the contract.
        #[derive(Debug)]
        constructor(
            /// The configuration for staking.
            StakeConfig memory genesisConfig_,
            /// The initial validators with stake.
            ValidatorInfo[] memory initialValidators_,
            /// The address of the owner.
            address owner_
        ) external;

        /// Conclude the current epoch. Caller must pass a new committee of eligible validators.
        function concludeEpoch(address[] calldata newCommittee) external;
        /// Apply incentives for the epoch. This must be called before `concludeEpoch`.
        function applyIncentives(RewardInfo[] calldata rewardInfos) external;
        /// Apply negative incentives for the epoch. This must be called before `concludeEpoch`.
        function applySlashes(Slash[] calldata slashes) external;
        /// Return the current epoch.
        function getCurrentEpoch() public view returns (uint32) ;
        /// Helper function to get the epoch info from the current epoch.
        function getCurrentEpochInfo() external view returns (EpochInfo memory currentEpochInfo);
        /// Return committee epoch info for a specific epoch.
        function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory epochInfo);
        /// Return the validators by status. Pass `0` for status to return all validators.
        function getValidators(uint8 status) public view returns (ValidatorInfo[] memory);
        /// Fetch the committee for a given epoch.
        function getCommitteeValidators(uint32 epoch) external view returns (ValidatorInfo[] memory);
        /// Fetch the `ValidatorInfo` for a give address.
        function getValidator(address validatorAddress) external view returns (ValidatorInfo memory);

        #[cfg(any(feature = "test-utils", test))]
        /// Mint an NFT for validator to stake.
        function mint(address validatorAddress) external override onlyOwner;

        #[cfg(any(feature = "test-utils", test))]
        /// Stake to the consensus registry.
        function stake(bytes calldata blsPubkey) external override onlyOwner;

        #[cfg(any(feature = "test-utils", test))]
        /// Activate node for committee selection.
        /// Normally called by staker after node is synced.
        function activate() external override whenNotPaused;
    }

);

/// The state of consensus retrieved from chain.
#[derive(Debug)]
pub struct EpochState {
    /// The epoch number.
    pub epoch: Epoch,
    /// The [EpochInfo].
    pub epoch_info: ConsensusRegistry::EpochInfo,
    /// The collection of validator info.
    pub validators: Vec<ConsensusRegistry::ValidatorInfo>,
    /// The timestamp for when the previous epoch closed.
    ///
    /// This time plus the `EpochInfo::epochDuration` creates the timestamp for the next epoch
    /// boundary.
    pub epoch_start: u64,
}
