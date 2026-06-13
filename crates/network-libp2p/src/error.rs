//! Error types for TN network.

use libp2p::{
    gossipsub::{ConfigBuilderError, PublishError, SubscriptionError},
    kad::GetRecordError,
    request_response::OutboundFailure,
    swarm::DialError,
    TransportError,
};
use std::io;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::StreamError;

/// Networking error type.
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Swarm error dialing a peer.
    #[error("{0}")]
    Dial(String),
    /// Redial attempt.
    #[error("Peer already dialed")]
    RedialAttempt,
    /// Dialing an banned peer.
    #[error("{0}")]
    DialBannedPeer(String),
    /// Dialing an already connected peer.
    #[error("{0}")]
    AlreadyConnected(String),
    /// The peer is already being dialed.
    #[error("{0}")]
    AlreadyDialing(String),
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
    /// Failed to send request/response outbound to peer.
    #[error("Outbound failure: {0}")]
    Outbound(#[from] RpcFailure),
    /// Failed to create gossipsub behavior.
    #[error("{0}")]
    GossipBehavior(&'static str),
    /// Failed to build swarm with behavior.
    #[error("SwarmBuilder::with_behaviour failed somehow.")]
    BuildSwarm,
    /// Request/response RPC Error
    #[error("{0}")]
    RPCError(String),
    /// If a request is made to "any" peer and no peers are currently connected.
    #[error("No connected peers")]
    NoPeers,
    /// Response violated the protocol.
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    /// A network operation timed out.
    #[error("Timed Out")]
    Timeout,
    /// This node disconnected from the peer.
    #[error("Disconnected from peer")]
    Disconnected,
    /// The peer was already disconnected.
    #[error("Peer already disconnected")]
    DisconnectPeer,
    /// Fatal error - the swarm is not connected to any listeners.
    #[error("All swarm listeners closed. Network shutting down...")]
    AllListenersClosed,
    /// The retrieved peer record is invalid.
    #[error("Invalid bls signature for peer record.")]
    InvalidPeerRecord,
    /// The requested peer is not on our local store.
    #[error("Requested peer is not in our local store.")]
    PeerMissing,
    /// Kademlia error.
    #[error("Failed to get kad record: {0}")]
    GetKademliaRecord(#[from] GetRecordError),
    /// Kademlia store write error.
    #[error("Failed to store kad record: {0}")]
    StoreKademliaRecord(String),
    /// Failed to open stream.
    #[error("Stream failed: {0}")]
    Stream(#[from] StreamError),
}

/// Reasons an outbound request-response RPC failed.
///
/// Crate-owned mirror of libp2p's [`OutboundFailure`] so the public
/// [`NetworkError`] surface does not name a libp2p type. The variants and their
/// `Display` text mirror the upstream enum exactly, so converting from
/// [`OutboundFailure`] is loss-free.
#[derive(Debug, Error)]
pub enum RpcFailure {
    /// The request could not be sent because dialing the peer failed.
    #[error("Failed to dial the requested peer")]
    DialFailure,
    /// The request timed out before a response was received.
    #[error("Timeout while waiting for a response")]
    Timeout,
    /// The connection closed before a response was received.
    #[error("Connection was closed before a response was received")]
    ConnectionClosed,
    /// The remote supports none of the requested protocols.
    #[error("The remote supports none of the requested protocols")]
    UnsupportedProtocols,
    /// An I/O failure happened on the outbound stream.
    #[error("IO error on outbound stream: {0}")]
    Io(std::io::Error),
}

impl From<OutboundFailure> for RpcFailure {
    fn from(failure: OutboundFailure) -> Self {
        match failure {
            OutboundFailure::DialFailure => Self::DialFailure,
            OutboundFailure::Timeout => Self::Timeout,
            OutboundFailure::ConnectionClosed => Self::ConnectionClosed,
            OutboundFailure::UnsupportedProtocols => Self::UnsupportedProtocols,
            OutboundFailure::Io(e) => Self::Io(e),
        }
    }
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

impl From<&DialError> for NetworkError {
    fn from(e: &DialError) -> Self {
        Self::Dial(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{NetworkError, RpcFailure};
    use libp2p::request_response::OutboundFailure;
    use std::io;

    /// Every `OutboundFailure` variant maps to the matching `RpcFailure`
    /// variant and preserves the upstream `Display` text, so wrapping it in
    /// `NetworkError::Outbound` stays identical to the previous direct
    /// `#[from] OutboundFailure`.
    #[test]
    fn rpc_failure_mirrors_outbound_failure() {
        let cases = [
            OutboundFailure::DialFailure,
            OutboundFailure::Timeout,
            OutboundFailure::ConnectionClosed,
            OutboundFailure::UnsupportedProtocols,
            OutboundFailure::Io(io::Error::other("boom")),
        ];

        for failure in cases {
            let expected = failure.to_string();
            let mapped = RpcFailure::from(failure);
            assert_eq!(
                mapped.to_string(),
                expected,
                "RpcFailure Display must match libp2p verbatim"
            );
            let wrapped = NetworkError::Outbound(mapped);
            assert_eq!(wrapped.to_string(), format!("Outbound failure: {expected}"));
        }
    }
}
