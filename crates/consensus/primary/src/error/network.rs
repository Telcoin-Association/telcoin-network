//! Error types for primary's network task.

use tn_types::error::HeaderError;

/// Result alias for results that possibly return [`PrimaryNetworkError`].
pub(crate) type PrimaryNetworkResult<T> = Result<T, PrimaryNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrimaryNetworkError {
    /// Error while processing a peer's request for vote.
    #[error("Error processing header vote request: {0}")]
    InvalidHeader(#[from] HeaderError),
}
