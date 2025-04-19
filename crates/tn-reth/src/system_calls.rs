//! Module for solidity interface.
//!
//! These compile into types for interacting with smart contracts through
//! System Calls.

use alloy::{primitives::address, sol};
use tn_types::Address;

/// The system address.
pub(super) const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

/// The address for consensus registry.
pub(super) const CONSENSUS_REGISTRY_ADDRESS: Address =
    address!("07E17e17E17e17E17e17E17E17E17e17e17E17e1");

// ConsensusRegistry interface. See tn-contracts submodule.
sol!(
    #[sol(rpc)]
    contract ConsensusRegistry {
        /// The validator's eligibility status for being
        /// considered in the next committee.
        #[derive(Debug)]
        enum ValidatorStatus {
            /// Match any status.
            Any,
            /// The validator is staked but has not indicated
            /// it is ready to participate in committee to earn
            /// rewards.
            PendingActivation,
            /// The validator is actively participating in consensus.
            Active,
            /// The validator has indicated interest to exit the protocol.
            PendingExit,
            /// The validator is no longer participating in consensus.
            Exited
        }

        /// The validator's information.
        #[derive(Debug)]
        struct ValidatorInfo {
            /// The BLS12-381 public key.
            bytes blsPubkey;
            /// TODO: remove this - keeping for now until tn-contracts upstream pr merged
            bytes32 ed25519Pubkey;
            /// The ECDSA public key.
            address ecdsaPubkey;
            /// The epoch which the validator's status
            /// become "Active" and eligible to participate
            /// in a committee.
            uint32 activationEpoch;
            /// The epoch that the validator exited the protocol.
            uint32 exitEpoch;
            /// The staking index of the validator.
            uint24 validatorIndex;
            /// The current status of the validator.
            ValidatorStatus currentStatus;
        }

        /// The epoch info stored on-chain.
        #[derive(PartialEq, Debug)]
        struct EpochInfo {
            /// The committee of validators responsible for the epoch.
            address[] committee;
            /// The block height when the epoch started and the
            /// committee became active.
            uint64 blockHeight;
        }

        /// Initialize the contract.
        function initialize(
            /// The rwTEL contract address.
            address rwTEL_,
            /// The stake amount required to be eligible as a validator.
            uint256 stakeAmount_,
            /// The min amount accumulated to withdraw.
            uint256 minWithdrawAmount_,
            /// The initial validators with stake.
            ValidatorInfo[] memory initialValidators_,
            /// The address of the owner.
            address owner_
        );

        /// Return the validators by status. Pass `0` for status to return all validators.
        function getValidators(uint8 status) public view returns (ValidatorInfo[] memory);
        /// Return committee epoch info for a specific epoch.
        function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory epochInfo);
        /// Return the current epoch.
        function getCurrentEpoch() public view returns (uint32) ;
        /// Conclude the current epoch. Caller must pass a new committee of eligible validators.
        function concludeEpoch(address[] calldata newCommittee) external;
    }
);
