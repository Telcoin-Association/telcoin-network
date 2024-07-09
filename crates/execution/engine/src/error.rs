//! Error types for Telcoin Network Engine.

use reth_blockchain_tree::error::InsertBlockError;
use reth_errors::{ProviderError, RethError};
use reth_revm::primitives::EVMError;
use tn_types::BatchConversionError;

/// Result alias for [`TNEngineError`].
pub(crate) type EngineResult<T> = Result<T, TnEngineError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum TnEngineError {
    /// Error from Reth
    #[error(transparent)]
    Reth(#[from] RethError),
    /// Error retrieving data from Provider.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Error during EVM execution.
    #[error("evm execution error: {0}")]
    EvmExecution(#[from] EVMError<ProviderError>),
    /// Error converting batch to `SealedBlockWithSenders`.
    #[error(transparent)]
    Batch(#[from] BatchConversionError),
    /// The next batch digest is missing.
    #[error("Missing next batch digest for recovered sealed block with senders.")]
    NextBatchDigestMissing,
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The block could not be inserted into the tree.
    #[error(transparent)]
    InsertNextCanonicalBlock(#[from] InsertBlockError),
}
