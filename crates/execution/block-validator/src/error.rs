//! Error types for building and executing output from consensus.

use reth_provider::ProviderError;
use thiserror::Error;
use tn_types::BlockHash;

/// Block validation error types
#[derive(Error, Debug)]
pub enum BlockValidationError {
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
    /// Canonical chain header cannot be found.
    #[error("Canonical chain header {block_hash} can't be found for peer block's parent")]
    CanonicalChain {
        /// The block hash of the missing canonical chain header.
        block_hash: BlockHash,
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
    /// Error while calculating size (in bytes) of icluded transactions.
    #[error("Unable to reduce size of transactions (in bytes) for peer's block")]
    CalculateTransactionByteSize,
    /// Error when peer's transaction list exceeds the maximum bytes allowed.
    #[error("Peer's transactions exceed max byte size: {0}")]
    HeaderTransactionBytesExceedsMax(usize),
}
