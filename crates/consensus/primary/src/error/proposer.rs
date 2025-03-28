//! Error types for primary's Proposer task.

use tn_types::Header;
use tokio::sync::{oneshot, watch};

/// Result alias for [`ProposerError`].
pub(crate) type ProposerResult<T> = Result<T, ProposerError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProposerError {
    /// The watch channel that receives the result from executing output on a blocking thread.
    #[error(
        "The watch channel sender for primary's proposer dropped while building the next header."
    )]
    WatchChannelClosed,
    /// The oneshot channel that receives the result from executing output on a blocking thread.
    #[error(
        "The oneshot channel sender inside new header task dropped while builing the next header."
    )]
    OneshotChannelClosed,
    /// Sending error for the proposer to certifier.
    #[error("Proposer failed to send header to certifier.")]
    CertifierSender(#[from] Box<tn_types::SendError<Header>>),
    /// Error writing to the proposer store.
    #[error("Failed to write new header to proposer store: {0}")]
    StoreError(String),
}

impl From<watch::error::RecvError> for ProposerError {
    fn from(_: watch::error::RecvError) -> Self {
        Self::WatchChannelClosed
    }
}

impl From<oneshot::error::RecvError> for ProposerError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::OneshotChannelClosed
    }
}
