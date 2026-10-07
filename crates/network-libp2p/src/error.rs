//! Error types for TN network.

use libp2p::{
    core::upgrade::NegotiationError,
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
    /// `ResponseChannel` already closed due to timeout or loss of connection.
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
    /// Request/response RPC Error.
    ///
    /// A permanent, application-layer rejection from the responder. The
    /// requester must not retry: the responder will reject an identical request
    /// the same way (invalid payload, protocol violation, wrong response type).
    #[error("{0}")]
    RPCError(String),
    /// Retryable request/response RPC error.
    ///
    /// The responder hit a transient, recoverable condition (a momentary
    /// batch-store write failure, internal channel pressure during an epoch
    /// transition) rather than rejecting the request on its merits. A requester
    /// should retry with backoff instead of permanently giving up on the peer.
    /// This is the counterpart of [`NetworkError::RPCError`], which is a
    /// permanent rejection.
    #[error("{0}")]
    RPCRetryable(String),
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
    /// The peer's BLS identity has not been resolved yet.
    ///
    /// The peer is connected, but it connected before its `NodeRecord` populated
    /// the confirmed-identity index, so no `BlsPublicKey` can be attached to its
    /// message or response. This is a transient resolution gap, distinct from
    /// [`NetworkError::PeerMissing`] ("not in our local store"): the underlying
    /// payload is genuine, so a caller should treat it as a retryable race rather
    /// than a failed exchange.
    #[error("Peer identity not yet resolved.")]
    PeerUnresolved,
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

/// Whether an outbound request failed because the remote speaks none of the requested
/// protocols.
///
/// The swarm negotiates outbound substreams with multistream-select `V1Lazy`, so a
/// request-response dialer that proposes a single protocol does not wait for the
/// listener's confirmation and never sees the rejection during the upgrade. The
/// rejection surfaces when the response is read, as [`OutboundFailure::Io`] wrapping
/// [`NegotiationError::Failed`], instead of [`OutboundFailure::UnsupportedProtocols`].
/// Both shapes mean the peer runs a different protocol set (honest version, role, or
/// chain skew), so both return `true`.
pub(crate) fn is_unsupported_protocol(failure: &OutboundFailure) -> bool {
    match failure {
        OutboundFailure::UnsupportedProtocols => true,
        OutboundFailure::Io(e) => is_negotiation_failure(e),
        OutboundFailure::DialFailure
        | OutboundFailure::Timeout
        | OutboundFailure::ConnectionClosed => false,
    }
}

/// Map a lazily negotiated protocol rejection to [`OutboundFailure::UnsupportedProtocols`],
/// the shape strict negotiation reports, so failure handlers, metrics, and callers classify
/// it the same way under either negotiation version.
pub(crate) fn normalize_outbound_failure(failure: OutboundFailure) -> OutboundFailure {
    if is_unsupported_protocol(&failure) {
        OutboundFailure::UnsupportedProtocols
    } else {
        failure
    }
}

/// Whether `error` is multistream-select's rejection of every proposed protocol.
///
/// `Negotiated` reports the rejection on read as `io::Error::other(NegotiationError::Failed)`
/// (kind `Other`), so the typed error is the io error's inner error. Only `Failed` means the
/// listener refused the protocol: a `NegotiationError::ProtocolError` (malformed
/// negotiation) is converted into a plain io error, and the swarm reports it as
/// `StreamUpgradeError::Io` under strict negotiation too.
pub(crate) fn is_negotiation_failure(error: &io::Error) -> bool {
    std::iter::successors(
        error.get_ref().map(|inner| inner as &(dyn std::error::Error + 'static)),
        |err| err.source(),
    )
    .any(|err| matches!(err.downcast_ref::<NegotiationError>(), Some(NegotiationError::Failed)))
}

#[cfg(test)]
mod tests {
    use super::{is_unsupported_protocol, normalize_outbound_failure, NetworkError, RpcFailure};
    use libp2p::{
        core::upgrade::{NegotiationError, ProtocolError},
        request_response::OutboundFailure,
    };
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

    /// The io error multistream-select's `Negotiated` returns on read when the listener
    /// rejects a lazily negotiated protocol (`From<NegotiationError> for io::Error`).
    fn lazy_rejection() -> OutboundFailure {
        OutboundFailure::Io(io::Error::from(NegotiationError::Failed))
    }

    /// A lazy rejection and strict `UnsupportedProtocols` are both unsupported-protocol
    /// failures; transport, timeout, and malformed-negotiation failures are not.
    #[test]
    fn unsupported_protocol_covers_lazy_negotiation_rejection() {
        let OutboundFailure::Io(e) = lazy_rejection() else { unreachable!() };
        assert_eq!(e.kind(), io::ErrorKind::Other, "lazy rejection must keep its upstream shape");

        assert!(is_unsupported_protocol(&lazy_rejection()));
        assert!(is_unsupported_protocol(&OutboundFailure::UnsupportedProtocols));

        let not_unsupported = [
            OutboundFailure::Io(io::ErrorKind::ConnectionReset.into()),
            OutboundFailure::Io(io::Error::other("boom")),
            // a malformed negotiation is an io failure under strict negotiation too
            OutboundFailure::Io(io::Error::from(NegotiationError::ProtocolError(
                ProtocolError::InvalidMessage,
            ))),
            OutboundFailure::Io(io::Error::other(NegotiationError::ProtocolError(
                ProtocolError::InvalidProtocol,
            ))),
            OutboundFailure::Timeout,
            OutboundFailure::DialFailure,
            OutboundFailure::ConnectionClosed,
        ];
        for failure in not_unsupported {
            assert!(!is_unsupported_protocol(&failure), "{failure:?} is not a protocol rejection");
        }
    }

    /// Normalizing turns a lazy rejection into the strict shape, so callers see the same
    /// `RpcFailure` under either negotiation version, and leaves every other failure intact.
    #[test]
    fn normalize_maps_lazy_rejection_to_unsupported_protocols() {
        assert!(matches!(
            normalize_outbound_failure(lazy_rejection()),
            OutboundFailure::UnsupportedProtocols
        ));
        assert!(matches!(
            RpcFailure::from(normalize_outbound_failure(lazy_rejection())),
            RpcFailure::UnsupportedProtocols
        ));

        let reset =
            normalize_outbound_failure(OutboundFailure::Io(io::ErrorKind::ConnectionReset.into()));
        assert!(
            matches!(reset, OutboundFailure::Io(e) if e.kind() == io::ErrorKind::ConnectionReset)
        );
        assert!(matches!(
            normalize_outbound_failure(OutboundFailure::Timeout),
            OutboundFailure::Timeout
        ));
    }
}
