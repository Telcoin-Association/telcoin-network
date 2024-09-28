//! Error types for building and executing output from consensus.

use reth_primitives::{BlockNumber, Bloom, B256, U256};
use reth_provider::ProviderError;
use thiserror::Error;
use tn_types::BlockHash;

/// Block validation error types
#[derive(Error, Debug)]
pub enum BlockValidationError {
    /// Invalid block hash.
    #[error("Invalid digest for worker block's sealed header: expected {expected:?} - received {peer_hash:?}")]
    BlockHash {
        /// The block hash computed from the payload.
        expected: B256,
        /// The block hash provided with the payload.
        peer_hash: B256,
    },
    /// Error when the block number does not match the parent block number.
    #[error(
        "Worker's block number {block_number} does not match parent block number {parent_block_number}"
    )]
    ParentBlockNumberMismatch {
        /// The parent block number.
        parent_block_number: BlockNumber,
        /// The block number.
        block_number: BlockNumber,
    },
    /// Attempt to retrieve the block's header from this worker's database.
    #[error("Error retrieving header from database provider: {0}")]
    Provider(#[from] ProviderError),
    /// Ensure proposed block is after parent.
    #[error("Peer's header proposed before parent block timestamp.")]
    TimestampIsInPast {
        /// The parent block's timestamp.
        parent_timestamp: u64,
        /// The block's timestamp.
        timestamp: u64,
    },
    /// Verify transaction root.
    #[error("Peer's transaction root mismatch: expected {expected:?} - received {peer_root:?}")]
    TransactionRootMismatch {
        /// The transaction root computed from the peer's transactions.
        expected: B256,
        /// The transaction root provided with the peer's block.
        peer_root: B256,
    },
    /// Canonical chain header cannot be found.
    #[error("Canonical chain header {block_hash} can't be found for peer block's parent")]
    CanonicalChain {
        /// The block hash of the missing canonical chain header.
        block_hash: BlockHash,
    },
    /// Error when the peer proposes a block with the wrong gas limit.
    #[error("Peer's gas limit mismatch: expected {expected} - received {received}")]
    InvalidGasLimit {
        /// This worker's configured block gas limit.
        expected: u64,
        /// The peer's gas limit.
        received: u64,
    },

    /// Error when the max gas included in the header exceeds the block's gas limit.
    #[error("Peer's block total possible gas ({total_possible_gas}) is greater than block's gas limit ({gas_limit})")]
    HeaderMaxGasExceedsGasLimit {
        /// The total possible gas used in the block header measured by included transactions max
        /// gas.
        total_possible_gas: u64,
        /// The gas limit in the block header.
        gas_limit: u64,
    },
    /// Error while calculating max possible gas from icluded transactions.
    #[error("Unable to reduce max possible gas limit for peer's block")]
    CalculateMaxPossibleGas,
    /// Error when the max gas included in the header does not match the block's gas used value.
    #[error("Peer's gas used mismatch: expected {expected} - received {received}")]
    HeaderGasUsedMismatch {
        /// This worker's calculated max possible gas.
        expected: u64,
        /// The peer's gas used included in the block header.
        received: u64,
    },
    /// Error while calculating size (in bytes) of icluded transactions.
    #[error("Unable to reduce size of transactions (in bytes) for peer's block")]
    CalculateTransactionByteSize,
    /// Error when peer's transaction list exceeds the maximum bytes allowed.
    #[error("Peer's transactions exceed max byte size: {0}")]
    HeaderTransactionBytesExceedsMax(usize),
    /// Non-empty value for peer's header.
    #[error("Non-empty ommers hash: {0:?}")]
    NonEmptyOmmersHash(B256),
    /// Non-empty value for peer's header.
    #[error("Non-empty state root: {0:?}")]
    NonEmptyStateRoot(B256),
    /// Non-empty value for peer's header.
    #[error("Non-empty receipts root: {0:?}")]
    NonEmptyReceiptsRoot(B256),
    /// Non-empty value for peer's header.
    #[error("Non-empty logs bloom: {0:?}")]
    NonEmptyLogsBloom(Bloom),
    /// Non-empty value for peer's header.
    #[error("Non-empty mix hash: {0:?}")]
    NonEmptyMixHash(B256),
    /// Non-empty value for peer's header.
    #[error("Non-zero nonce: {0}")]
    NonZeroNonce(u64),
    /// Non-empty value for peer's header.
    #[error("Non-zero difficulty: {0}")]
    NonZeroDifficulty(U256),
    /// Non-empty value for peer's header.
    #[error("Non-empty parent beacon block root: {0:?}")]
    NonEmptyBeaconRoot(Option<B256>),
    /// Non-empty value for peer's header.
    #[error("Non-empty blob gas: {0:?}")]
    NonEmptyBlobGas(Option<u64>),
    /// Non-empty value for peer's header.
    #[error("Non-empty excess blob gas: {0:?}")]
    NonEmptyExcessBlobGas(Option<u64>),
    /// Non-empty value for peer's header.
    #[error("Non-empty requests root: {0:?}")]
    NonEmptyRequestsRoot(Option<B256>),
}
