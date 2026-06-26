use libp2p::{
    swarm::{
        handler::{
            ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
        },
        ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
    },
    Stream, StreamProtocol,
};
use std::{
    collections::VecDeque,
    convert::Infallible,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::oneshot;

use crate::{
    error::NetworkError,
    stream::{
        upgrade::{StreamError, StreamFailure, TNStreamProtocol},
        StreamKind,
    },
    types::NetworkResult,
};

/// Timeout for negotiating a single outbound stream substream.
///
/// Enforced by libp2p via [`SubstreamProtocol::with_timeout`]; on expiry the
/// handler receives a [`StreamUpgradeError::Timeout`].
pub(crate) const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on outbound opens a single connection handler will buffer before
/// shedding load. Streams normally drain in a single poll, so a non-trivial
/// backlog means the peer (or this node) is unhealthy.
const MAX_PENDING_OUTBOUND: usize = 256;

/// Upper bound on handler-to-behaviour events buffered before shedding load.
const MAX_HANDLER_EVENTS: usize = 256;

/// Commands from behavior to handler.
#[derive(Debug)]
pub(crate) enum HandlerCommand {
    /// Open an outbound stream, returning it through the provided channel.
    OpenStream {
        /// Which protocol the open negotiates ([`StreamKind::Legacy`] or
        /// [`StreamKind::Sync`]).
        kind: StreamKind,
        /// Channel for returning the established stream directly to the caller.
        reply: oneshot::Sender<NetworkResult<Stream>>,
    },
}

/// An in-flight outbound open, carried as the substream's open info so the
/// handler can answer the caller and classify a failure with the open's kind.
#[derive(Debug)]
pub(crate) struct OutboundOpen {
    /// Which protocol this open negotiates.
    kind: StreamKind,
    /// Channel for returning the established stream (or error) to the caller.
    reply: oneshot::Sender<NetworkResult<Stream>>,
}

/// Events from handler to behavior.
#[derive(Debug)]
pub(crate) enum StreamHandlerEvent {
    /// An inbound stream was successfully established.
    InboundStream {
        /// The protocol the inbound stream negotiated.
        kind: StreamKind,
        /// The established stream.
        stream: Stream,
    },
    /// An outbound stream open failed; classified for peer scoring.
    OutboundFailure {
        /// The classified failure.
        failure: StreamFailure,
    },
}

/// Connection handler for streaming data.
///
/// Manages streams on a single peer connection, processing inbound stream
/// requests and initiating outbound streams when commanded. Outbound streams
/// are returned directly to callers via oneshot channels passed through
/// `OutboundOpenInfo`, bypassing the behavior layer entirely. Open negotiation
/// is bounded by [`STREAM_OPEN_TIMEOUT`]; failures are classified and reported
/// to the behaviour for scoring.
pub(crate) struct StreamHandler {
    /// The chain-namespaced bulk-transfer `/tn-stream-{chain}` protocol,
    /// advertised first on inbound listen.
    legacy: StreamProtocol,
    /// The chain-namespaced per-role sync protocol, advertised second on inbound
    /// listen.
    sync: StreamProtocol,
    /// Pending outbound opens.
    pending_outbound: VecDeque<OutboundOpen>,
    /// Events to send to the behavior.
    events: VecDeque<StreamHandlerEvent>,
}

impl std::fmt::Debug for StreamHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandler")
            .field("pending_outbound_count", &self.pending_outbound.len())
            .field("events_count", &self.events.len())
            .finish()
    }
}

impl StreamHandler {
    /// Create a new stream handler advertising the legacy and sync protocols on
    /// this connection.
    pub(crate) fn new(legacy: StreamProtocol, sync: StreamProtocol) -> Self {
        Self { legacy, sync, pending_outbound: VecDeque::new(), events: VecDeque::new() }
    }

    /// The single protocol an outbound open of `kind` advertises.
    fn protocol_for(&self, kind: StreamKind) -> StreamProtocol {
        match kind {
            StreamKind::Legacy => self.legacy.clone(),
            StreamKind::Sync => self.sync.clone(),
        }
    }

    /// The upgrade an inbound listen advertises: both protocols (legacy first),
    /// carrying the sync protocol so a negotiated inbound stream is classified.
    fn listen_upgrade(&self) -> TNStreamProtocol {
        TNStreamProtocol::new(vec![self.legacy.clone(), self.sync.clone()], self.sync.clone())
    }

    /// Queue an event to the behaviour, dropping it if the buffer is saturated.
    fn push_event(&mut self, event: StreamHandlerEvent) {
        if self.events.len() < MAX_HANDLER_EVENTS {
            self.events.push_back(event);
        }
    }
}

/// Classify an outbound upgrade error into a scoring failure plus the
/// caller-facing error returned through the open's oneshot.
fn classify_outbound(error: StreamUpgradeError<Infallible>) -> (StreamFailure, StreamError) {
    match error {
        StreamUpgradeError::Timeout => (StreamFailure::Timeout, StreamError::Timeout),
        StreamUpgradeError::NegotiationFailed => {
            (StreamFailure::UnsupportedProtocol, StreamError::UpgradeFailed)
        }
        StreamUpgradeError::Io(e) => (StreamFailure::Io(e.kind()), StreamError::UpgradeIo),
        // `TNStreamProtocol`'s upgrade error is `Infallible`, so `Apply` is unconstructable.
        StreamUpgradeError::Apply(infallible) => match infallible {},
    }
}

impl ConnectionHandler for StreamHandler {
    type FromBehaviour = HandlerCommand;
    type ToBehaviour = StreamHandlerEvent;
    type InboundProtocol = TNStreamProtocol;
    type OutboundProtocol = TNStreamProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = OutboundOpen;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(self.listen_upgrade(), ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerCommand::OpenStream { kind, reply } => {
                if self.pending_outbound.len() >= MAX_PENDING_OUTBOUND {
                    let _ = reply.send(Err(NetworkError::Stream(StreamError::TooManyPending)));
                } else {
                    self.pending_outbound.push_back(OutboundOpen { kind, reply });
                }
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            '_,
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: (stream, kind),
                ..
            }) => {
                self.push_event(StreamHandlerEvent::InboundStream { kind, stream });
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                info: OutboundOpen { reply, .. },
                ..
            }) => {
                // Return the stream directly to the caller via oneshot. The
                // caller chose the protocol, so it already knows the kind.
                let _ = reply.send(Ok(stream));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError {
                info: OutboundOpen { kind, reply },
                error,
            }) => {
                // Return the error to the caller, which drives its fallback.
                let (failure, stream_error) = classify_outbound(error);
                let _ = reply.send(Err(NetworkError::Stream(stream_error)));
                // A sync open that fails negotiation only means the peer does not
                // speak the sync protocol yet; the caller falls back to legacy, so
                // the probe is penalty-exempt (the issue's "Severe suppressed for
                // new-protocol negotiation failure while a fallback exists"). Every
                // other failure, and every legacy failure, is still reported.
                let penalty_exempt_probe = matches!(kind, StreamKind::Sync)
                    && matches!(failure, StreamFailure::UnsupportedProtocol);
                if !penalty_exempt_probe {
                    self.push_event(StreamHandlerEvent::OutboundFailure { failure });
                }
            }
            _ => {}
        }
    }

    fn connection_keep_alive(&self) -> bool {
        !self.pending_outbound.is_empty() || !self.events.is_empty()
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        // Emit events to behavior first
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Request outbound streams, bounding negotiation with a timeout. The open
        // advertises only the chosen protocol, so a sync open negotiates sync (or
        // fails) instead of silently falling back to legacy at the wire level.
        if let Some(open) = self.pending_outbound.pop_front() {
            let upgrade =
                TNStreamProtocol::new(vec![self.protocol_for(open.kind)], self.sync.clone());
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(upgrade, open).with_timeout(STREAM_OPEN_TIMEOUT),
            });
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::classify_outbound;
    use crate::stream::upgrade::{StreamError, StreamFailure};
    use libp2p::swarm::StreamUpgradeError;
    use std::io;

    /// Outbound upgrade errors map to the right scoring failure and caller error.
    #[test]
    fn classify_outbound_maps_upgrade_errors() {
        let (failure, error) = classify_outbound(StreamUpgradeError::Timeout);
        assert!(matches!(failure, StreamFailure::Timeout));
        assert!(matches!(error, StreamError::Timeout));

        let (failure, error) = classify_outbound(StreamUpgradeError::NegotiationFailed);
        assert!(matches!(failure, StreamFailure::UnsupportedProtocol));
        assert!(matches!(error, StreamError::UpgradeFailed));

        let (failure, error) = classify_outbound(StreamUpgradeError::Io(io::Error::other("boom")));
        assert!(matches!(failure, StreamFailure::Io(_)));
        assert!(matches!(error, StreamError::UpgradeIo));
    }
}
