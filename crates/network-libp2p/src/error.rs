//! Error types for TN network.

use libp2p::{
    gossipsub::{PublishError, SubscriptionError},
    swarm::DialError,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// The result for network operations.
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Networking error type.
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Swarm error dialing a peer.
    #[error(transparent)]
    Dial(#[from] DialError),
    /// Gossipsub error publishing message.
    #[error(transparent)]
    Publish(#[from] PublishError),
    /// Gossipsub error subscribing to topic.
    #[error(transparent)]
    Subscription(#[from] SubscriptionError),
    /// mpsc receiver dropped.
    #[error("mpsc error: {0}")]
    MpscSender(String),
    /// oneshot sender dropped.
    #[error("oneshot error: {0}")]
    AckChannelClosed(String),
}

impl From<oneshot::error::RecvError> for NetworkError {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::AckChannelClosed(e.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for NetworkError {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::MpscSender(e.to_string())
    }
}
