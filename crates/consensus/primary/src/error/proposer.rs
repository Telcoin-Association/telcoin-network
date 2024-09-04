//! Error types for primary's Proposer task.

use tokio::sync::watch;

/// Result alias for [`ProposerError`].
pub(crate) type ProposerResult<T> = Result<T, ProposerError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum ProposerError {
    /// The watch channel that receives the result from executing output on a blocking thread.
    #[error("The watch channel sender for TN engine dropped during output execution.")]
    ChannelClosed,
}

impl From<watch::error::RecvError> for ProposerError {
    fn from(_: watch::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
