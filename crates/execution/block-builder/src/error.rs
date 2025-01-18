//! Error types for Telcoin Network Block Builder.

use reth_errors::{CanonicalError, ProviderError, RethError};
use tn_types::WorkerBlockConversionError;
use tokio::sync::{mpsc, oneshot};

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
    /// Error converting batch to `SealedBlockWithSenders`.
    #[error(transparent)]
    Batch(#[from] WorkerBlockConversionError),
    /// The next batch digest is missing.
    #[error("Missing next batch digest for recovered sealed block with senders.")]
    NextBatchDigestMissing,
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The executed block failed to become part of the canonical chain.
    #[error("Blockchain tree failed to make_canonical: {0}")]
    Canonical(#[from] CanonicalError),
    /// The oneshot channel that receives the ack that the block was persisted and being proposed.
    #[error("Fatal error: failed to receive ack reply that new block was built. Shutting down...")]
    AckChannelClosed,
    /// Failed to send to the worker.
    #[error("Fatal error: failed to send built block to worker.")]
    WorkerChannelClosed,
    /// Fatal db error with worker while trying to reach quorum.
    #[error("Fatal error: batch provider db error")]
    FatalDBFailure,
}

impl From<oneshot::error::RecvError> for BlockBuilderError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::AckChannelClosed
    }
}

impl<T> From<mpsc::error::SendError<T>> for BlockBuilderError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::WorkerChannelClosed
    }
}
