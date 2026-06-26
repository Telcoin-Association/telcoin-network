use libp2p::{core::UpgradeInfo, swarm::StreamProtocol, InboundUpgrade, OutboundUpgrade, Stream};
use std::{
    convert::Infallible,
    future::{ready, Ready},
};

use crate::{stream::StreamKind, Penalty};

/// Protocol upgrade for streaming data.
///
/// Advertises one or more chain-namespaced protocols for negotiation, in
/// dialer-preference order. An inbound listen advertises both the bulk-transfer
/// `/tn-stream-{chain}` and the per-role sync protocol and reports which one
/// negotiated, so the application can route a sync stream to the
/// [`SyncFrame`](crate::sync::SyncFrame) layer and a legacy stream to the
/// digest-correlation reader. An outbound open advertises a single chosen
/// protocol, so the caller already knows which kind it asked for. Both upgrades
/// return the raw stream; the framing is the caller's concern. The protocols
/// carry the chain id, so a node only ever establishes streams with peers on the
/// same chain.
#[derive(Debug, Clone)]
pub(crate) struct TNStreamProtocol {
    /// Protocols advertised for negotiation, in dialer-preference order. For an
    /// inbound listen this is `[bulk-transfer, sync]` — the chain-namespaced
    /// `/tn-stream-{chain}/0.0.1` first, so existing opens keep negotiating it,
    /// then the per-role sync protocol so a responder also accepts it; for an
    /// outbound open it is the single chosen protocol.
    protocols: Vec<StreamProtocol>,
    /// The per-role sync protocol, used to classify the negotiated protocol of
    /// an inbound stream as [`StreamKind::Sync`] versus [`StreamKind::Legacy`].
    sync: StreamProtocol,
}

impl TNStreamProtocol {
    /// Create an upgrade advertising `protocols`, in order (bulk-transfer first),
    /// with `sync` as the per-role sync protocol used to classify an inbound
    /// negotiation.
    pub(crate) fn new(protocols: Vec<StreamProtocol>, sync: StreamProtocol) -> Self {
        Self { protocols, sync }
    }
}

impl UpgradeInfo for TNStreamProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::vec::IntoIter<StreamProtocol>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocols.clone().into_iter()
    }
}

impl InboundUpgrade<Stream> for TNStreamProtocol {
    type Output = (Stream, StreamKind);
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // Classify the negotiated protocol so the application routes a sync-framed
    // stream to the `SyncFrame` layer and a legacy stream to the digest reader.
    // Only the legacy and sync protocols are ever advertised, so anything that
    // is not the sync protocol is the legacy one.
    fn upgrade_inbound(self, stream: Stream, protocol: Self::Info) -> Self::Future {
        let kind = if protocol == self.sync { StreamKind::Sync } else { StreamKind::Legacy };
        ready(Ok((stream, kind)))
    }
}

impl OutboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // The caller chose the single advertised protocol, so the negotiated info is
    // redundant; correlation and framing are the application layer's concern.
    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        ready(Ok(stream))
    }
}

/// Errors returned to the caller of an outbound stream open.
#[derive(Debug)]
pub enum StreamError {
    /// The protocol upgrade failed during stream negotiation: the peer advertised
    /// none of the offered protocols.
    UpgradeFailed,
    /// A transport I/O error occurred during stream negotiation. Distinct from
    /// [`StreamError::UpgradeFailed`]: the peer may well speak the protocol, so a
    /// caller probing protocol support should treat this as transient rather than
    /// as "unsupported".
    UpgradeIo,
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
            Self::UpgradeIo => write!(f, "I/O error during stream negotiation"),
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
    use super::{StreamFailure, TNStreamProtocol};
    use crate::Penalty;
    use libp2p::{core::UpgradeInfo, StreamProtocol};
    use std::io::ErrorKind;

    /// The upgrade advertises its protocols in the order given (legacy first),
    /// which is what keeps existing opens negotiating `/tn-stream` while a
    /// responder still accepts the registered sync protocol.
    #[test]
    fn protocol_info_preserves_order() {
        let legacy = StreamProtocol::new("/tn-stream/0.0.1");
        let sync = StreamProtocol::new("/tn-primary-sync/0.0.1");
        let upgrade = TNStreamProtocol::new(vec![legacy.clone(), sync.clone()], sync.clone());
        let advertised: Vec<_> = upgrade.protocol_info().collect();
        assert_eq!(advertised, vec![legacy, sync]);
    }

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
