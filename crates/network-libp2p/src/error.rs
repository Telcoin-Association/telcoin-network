//! Error types for TN network.

use libp2p::{
    gossipsub::{ConfigBuilderError, PublishError, SubscriptionError},
    identity::DecodingError,
    request_response::OutboundFailure,
    swarm::DialError,
    TransportError,
};
use std::io;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

/// Networking error type.
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Swarm error dialing a peer.
    #[error(transparent)]
    Dial(#[from] DialError),
    /// Dial attempt currently ongoing for peer.
    #[error("Peer already dialed")]
    RedialAttempt,
    /// Gossipsub error publishing message.
    #[error(transparent)]
    Publish(#[from] PublishError),
    /// Gossipsub error subscribing to topic.
    #[error(transparent)]
    Subscription(#[from] SubscriptionError),
    /// mpsc try send
    #[error("mpsc try send error: {0}")]
    MpscTrySend(String),
    /// mpsc receiver dropped.
    #[error("mpsc error: {0}")]
    ChannelSender(String),
    /// oneshot sender dropped.
    #[error("oneshot error: {0}")]
    AckChannelClosed(String),
    /// Swarm failed to connect on listen address.
    #[error(transparent)]
    Listen(#[from] TransportError<io::Error>),
    /// Failed to build gossipsub config.
    #[error(transparent)]
    GossipsubConfig(#[from] ConfigBuilderError),
    /// Failed to build swarm with peer scoring enabled.
    #[error("{0}")]
    EnablePeerScoreBehavior(String),
    /// Error conversion from [std::io::Error]
    #[error(transparent)]
    StdIo(#[from] std::io::Error),
    /// Error converted from [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),
    /// Libp2p `ResponseChannel` already closed due to timeout or loss of connection.
    #[error("Response channel closed.")]
    SendResponse,
    /// The oneshot channel for a request was lost. This is not expected to happen.
    #[error("Pending request channel lost. Unable to return peer's response to original caller.")]
    PendingRequestChannelLost,
    /// Failed to send request/response outbound to peer.
    #[error("Outbound failure: {0}")]
    Outbound(#[from] OutboundFailure),
    /// Failed to create gossipsub behavior.
    #[error("{0}")]
    GossipBehavior(&'static str),
    /// Failed to build swarm with behavior.
    #[error("SwarmBuilder::with_behaviour failed somehow.")]
    BuildSwarm,
    /// Temporary error - used when converting fastcrypto -> libp2p key
    #[error(transparent)]
    Ed25519Decode(#[from] DecodingError),
}

impl From<oneshot::error::RecvError> for NetworkError {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::AckChannelClosed(e.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for NetworkError {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::ChannelSender(e.to_string())
    }
}

impl<T> From<broadcast::error::SendError<T>> for NetworkError {
    fn from(e: broadcast::error::SendError<T>) -> Self {
        Self::ChannelSender(e.to_string())
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for NetworkError {
    fn from(e: mpsc::error::TrySendError<T>) -> Self {
        Self::MpscTrySend(e.to_string())
    }
}
