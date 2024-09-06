//! Error types for building and executing output from consensus.

use reth_blockchain_tree::error::BlockchainTreeError;
use reth_evm::execute::{BlockExecutionError, BlockValidationError};
use reth_primitives::{GotExpected, B256};
use reth_provider::ProviderError;
use thiserror::Error;
use tn_types::{BatchConversionError, ConsensusError};

/// Batch validation error types
#[derive(Error, Debug)]
pub enum BatchValidationError {
    /// Errors from BlockExecution
    #[error("Block execution error: {0}")]
    Execution(#[from] BlockExecutionError),
    /// Errors for converting batch into sealed block
    #[error(transparent)]
    IntoSealedBlock(#[from] BatchConversionError),
    /// Provider error.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Tree error
    #[error(transparent)]
    Tree(#[from] BlockchainTreeError),
    /// Tree error
    #[error(transparent)]
    BlockValidation(#[from] BlockValidationError),
    /// State root is invalid
    #[error(transparent)]
    BodyStateRootDiff(GotExpected<B256>),
    /// Error with beacon consensus validation
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
}
