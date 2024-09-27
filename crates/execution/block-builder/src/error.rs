//! Error types for Telcoin Network Block Builder.

use reth_blockchain_tree::error::InsertBlockError;
use reth_errors::{CanonicalError, ProviderError, RethError};
use reth_revm::primitives::EVMError;
use tn_types::WorkerBlockConversionError;
use tokio::{sync::oneshot, task::JoinError};

/// Result alias for [`TNEngineError`].
pub(crate) type BlockBuilderResult<T> = Result<T, BlockBuilderError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum BlockBuilderError {
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
    Batch(#[from] WorkerBlockConversionError),
    /// The next batch digest is missing.
    #[error("Missing next batch digest for recovered sealed block with senders.")]
    NextBatchDigestMissing,
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The block could not be inserted into the tree.
    #[error(transparent)]
    InsertNextCanonicalBlock(#[from] InsertBlockError),
    /// The executed block failed to become part of the canonical chain.
    #[error("Blockchain tree failed to make_canonical: {0}")]
    Canonical(#[from] CanonicalError),
    /// The oneshot channel that receives the result from executing output on a blocking thread.
    #[error("Thread panicked while constructing the next worker block: {0}")]
    ThreadPanicked(#[from] JoinError),
}
