use libp2p::{core::UpgradeInfo, swarm::StreamProtocol, InboundUpgrade, OutboundUpgrade, Stream};
use std::{
    convert::Infallible,
    future::{ready, Ready},
};

use crate::{stream::behavior::TN_STREAM_PROTOCOL, Penalty};

/// Protocol upgrade for streaming data.
///
/// Both inbound and outbound upgrades simply return the raw stream.
/// Application-layer correlation (e.g. writing a request digest) is
/// handled by the caller after the stream is established.
#[derive(Debug, Clone)]
pub(crate) struct TNStreamProtocol;

impl UpgradeInfo for TNStreamProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(TN_STREAM_PROTOCOL)
    }
}

impl InboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // logic in application layer
    fn upgrade_inbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        ready(Ok(stream))
    }
}

impl OutboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // logic in application layer
    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        ready(Ok(stream))
    }
}

/// Errors returned to the caller of an outbound stream open.
#[derive(Debug)]
pub enum StreamError {
    /// The protocol upgrade failed during stream negotiation.
    UpgradeFailed,
    /// The peer was not connected and could not be dialed (no known address).
    NotConnected,
    /// The open attempt timed out before a stream was established.
    Timeout,
    /// Too many outbound stream opens are already pending.
    TooManyPending,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpgradeFailed => write!(f, "Protocol upgrade failed"),
            Self::NotConnected => write!(f, "Peer not connected and could not be dialed"),
            Self::Timeout => write!(f, "Timed out before a stream was established"),
            Self::TooManyPending => write!(f, "Too many pending stream opens"),
        }
    }
}

impl std::error::Error for StreamError {}

/// A classified stream failure used to score the remote peer.
///
/// Mirrors the request-response failure taxonomy so the stream path and the RPC
/// path penalize comparable misbehaviour comparably. Every variant is reported
/// for telemetry first; enforcement is enabled once telemetry confirms it does
/// not fire on healthy peers (see `process_stream_event`).
#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamFailure {
    /// Dialing the peer failed (transport-level; not necessarily the peer's fault).
    DialFailure,
    /// The open or negotiation timed out.
    Timeout,
    /// The peer supports none of our stream protocols.
    UnsupportedProtocol,
    /// An I/O error occurred while negotiating the stream.
    Io(std::io::ErrorKind),
    /// The peer exceeded the inbound stream rate limit.
    InboundRateLimited,
}

impl StreamFailure {
    /// The penalty this failure would incur, or `None` when it is not the
    /// peer's fault.
    ///
    /// Mirrors the request-response failure taxonomy so the stream path and the
    /// RPC path score comparable misbehaviour comparably. Reported metrics-only
    /// for now; see `process_stream_event`.
    pub(crate) fn penalty(&self) -> Option<Penalty> {
        match self {
            // transport-level: the peer went away or could not be reached
            Self::DialFailure => None,
            // stalled open
            Self::Timeout => Some(Penalty::Mild),
            // the peer speaks none of our stream protocols: honest version/role
            // skew, not a fault — not penalized, like `DialFailure` (mirrors the
            // request-response `UnsupportedProtocols` arm in `consensus.rs`).
            Self::UnsupportedProtocol => None,
            // transport flaps on WAN are not faults; other IO is likely a violation
            Self::Io(kind) => match kind {
                std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::Interrupted => None,
                _ => Some(Penalty::Medium),
            },
            // application-level abuse of the inbound stream path
            Self::InboundRateLimited => Some(Penalty::Medium),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StreamFailure;
    use crate::Penalty;
    use std::io::ErrorKind;

    /// The stream penalty taxonomy must mirror the request-response one: transport
    /// faults are not penalized, stalls are mild, and unsupported protocols are
    /// honest version/role skew (not penalized, like `DialFailure`).
    #[test]
    fn penalty_mapping_mirrors_reqres() {
        assert!(StreamFailure::DialFailure.penalty().is_none());
        assert!(matches!(StreamFailure::Timeout.penalty(), Some(Penalty::Mild)));
        assert!(StreamFailure::UnsupportedProtocol.penalty().is_none());
        assert!(matches!(StreamFailure::InboundRateLimited.penalty(), Some(Penalty::Medium)));
        // transport flaps on WAN are not the peer's fault
        assert!(StreamFailure::Io(ErrorKind::ConnectionReset).penalty().is_none());
        assert!(StreamFailure::Io(ErrorKind::BrokenPipe).penalty().is_none());
        // a genuine protocol/codec IO error is a fault
        assert!(matches!(
            StreamFailure::Io(ErrorKind::InvalidData).penalty(),
            Some(Penalty::Medium)
        ));
    }
}
