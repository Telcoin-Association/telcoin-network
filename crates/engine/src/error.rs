//! Error types for Telcoin Network Engine.

use tn_reth::error::TnRethError;
use tn_types::AuthorityIdentifier;
use tokio::sync::oneshot;

/// Result alias for [`TnEngineError`].
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
    /// The oneshot channel that receives the result from executing output on a blocking thread.
    #[error("The oneshot channel sender inside blocking task dropped during output execution.")]
    ChannelClosed,
    /// The queued output that triggered the engine build was not found.
    #[error("Engine trying to build from empty queue.")]
    EmptyQueue,
    /// The consensus stream has closed.
    #[error("Consensus output stream closed.")]
    ConsensusOutputStreamClosed,
    /// The output's leader is unknown.
    #[error("Unknown authority for block rewards {0}")]
    UnknownAuthority(AuthorityIdentifier),
    /// A ConsensusOutput produced a different number of batches and batch digests.
    /// This should not happen outside of a bug in ConsensusOutput production...
    #[error("Uneven number of sealed blocks from batches and batch digests, batches {0}, batch digests {1}")]
    ConsensusOutputUnevenBatches(usize, usize),
    /// The recovered signers of a ConsensusOutput do not have one entry per flattened batch.
    /// Both sides walk the same batches, so this indicates a bug in the engine.
    #[error("Recovered signers do not match the output's batches, batches {0}, recovered {1}")]
    RecoveredSignersMismatch(usize, usize),
    /// A pipelined-persist ordering check failed (an output not built on the in-memory tip, not
    /// dispatched on the durable tip, or a snapshot that does not match it). This is a bug
    /// signal: like every engine error it stops the node, and nothing out of order is persisted.
    #[error("pipelined persist ordering check failed: {0}")]
    PipelineInvariant(&'static str),
}

impl From<oneshot::error::RecvError> for TnEngineError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
