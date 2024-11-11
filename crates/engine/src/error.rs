//! Error types for Telcoin Network Engine.

use reth_blockchain_tree::error::InsertBlockError;
use reth_errors::{CanonicalError, ProviderError, RethError};
use reth_revm::primitives::EVMError;
use tn_types::WorkerBlockConversionError;
use tokio::sync::oneshot;

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
    /// Error converting block to `SealedBlockWithSenders`.
    #[error(transparent)]
    Block(#[from] WorkerBlockConversionError),
    /// The next block digest is missing.
    #[error("Missing next block digest for recovered sealed block with senders.")]
    NextBlockDigestMissing,
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
    #[error("The oneshot channel sender inside blocking task dropped during output execution.")]
    ChannelClosed,
    /// The queued output that triggered the engine build was not found.
    #[error("Engine trying to build from empty queue.")]
    EmptyQueue,
}

impl From<oneshot::error::RecvError> for TnEngineError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
