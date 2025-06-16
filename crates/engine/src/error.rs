//! Error types for Telcoin Network Engine.

use tn_reth::error::TnRethError;
use tokio::sync::oneshot;

/// Result alias for [`TNEngineError`].
pub(crate) type EngineResult<T> = Result<T, TnEngineError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum TnEngineError {
    /// Error from Reth
    #[error(transparent)]
    Reth(#[from] TnRethError),
    /// The next block digest is missing.
    #[error("Missing next block digest for recovered sealed block with senders.")]
    NextBlockDigestMissing,
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The oneshot channel that receives the result from executing output on a blocking thread.
    #[error("The oneshot channel sender inside blocking task dropped during output execution.")]
    ChannelClosed,
    /// The queued output that triggered the engine build was not found.
    #[error("Engine trying to build from empty queue.")]
    EmptyQueue,
    /// Failed to recover the signer for a transaction.
    #[error("Could not recover signer for transaction")]
    MissingSigner,
    /// Failed to find the block we need to finalize- forked?.
    #[error("Could not finalize execution block- forked?")]
    MissingFinalBlock,
    /// The consensus stream has closed.
    #[error("Consensus output stream closed.")]
    ConsensusOutputStreamClosed,
}

impl From<oneshot::error::RecvError> for TnEngineError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
