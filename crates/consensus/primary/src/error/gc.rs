//! Errors encountered during regular garbage collection.

use tokio::sync::watch;

/// Result alias for results that possibly return [`GarbageCollectorError`].
pub(crate) type GarbageCollectorResult<T> = Result<T, GarbageCollectorError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GarbageCollectorError {
    /// Error triggering certificate fetch after max round timer expires.
    #[error("Consensus bus failed to send to: {0}")]
    TNSend(String),
    /// The watch channel for consensus rounds returned an error.
    #[error("The watch channel for consensus rounds returned an error instead of a change notification.")]
    ConsensusRoundWatchChannel(#[from] watch::error::RecvError),
}
