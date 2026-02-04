use libp2p::swarm::handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound};
use libp2p::swarm::{ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol};
use libp2p::Stream;
use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::stream::upgrade::{StreamError, StreamHeader, TNStreamProtocol};

/// Commands from behavior to handler.
///
/// These commands instruct the handler to perform operations on its connection.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Open an outbound stream with the given ID.
    OpenStream {
        /// The ID for tracking this stream.
        request_id: u64,
        /// The header to write to the stream after it's established.
        header: StreamHeader,
    },
}

/// Events from handler to behavior.
///
/// These events notify the behavior about stream-related occurrences on this connection.
#[derive(Debug)]
pub enum StreamHandlerEvent {
    /// An inbound stream was successfully established.
    InboundStream {
        /// The established stream.
        stream: Stream,
        /// The header read from the stream.
        header: StreamHeader,
    },
    /// An outbound stream was successfully established.
    OutboundStream {
        /// The established stream.
        stream: Stream,
        /// The ID for this stream.
        request_id: u64,
        /// The header to write to the stream.
        header: StreamHeader,
    },
    /// Failed to establish an outbound stream.
    OutboundFailure {
        /// The ID for the failed stream.
        request_id: u64,
        /// The error that occurred.
        error: StreamError,
    },
}

/// Pending outbound stream request with its associated header.
#[derive(Debug)]
struct PendingOutbound {
    /// The request ID for correlation.
    request_id: u64,
    /// The header to include when the stream is established.
    header: StreamHeader,
}

/// Connection handler for stream-based sync.
///
/// This handler manages streams on a single peer connection, processing
/// inbound stream requests and initiating outbound streams when commanded.
pub struct StreamHandler {
    /// Pending outbound stream requests with their headers.
    pending_outbound: VecDeque<PendingOutbound>,
    /// Events to send to the behavior.
    events: VecDeque<StreamHandlerEvent>,
    /// Keep connection alive while we have pending work.
    keep_alive: bool,
}

impl std::fmt::Debug for StreamHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandler")
            .field("pending_outbound_count", &self.pending_outbound.len())
            .field("events_count", &self.events.len())
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

impl StreamHandler {
    /// Create a new stream handler.
    pub fn new() -> Self {
        Self { pending_outbound: VecDeque::new(), events: VecDeque::new(), keep_alive: true }
    }
}

impl Default for StreamHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionHandler for StreamHandler {
    type FromBehaviour = HandlerCommand;
    type ToBehaviour = StreamHandlerEvent;
    type InboundProtocol = TNStreamProtocol;
    type OutboundProtocol = TNStreamProtocol;
    type InboundOpenInfo = ();
    /// Info passed with outbound streams: (request_id, header)
    type OutboundOpenInfo = (u64, StreamHeader);

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(TNStreamProtocol, ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerCommand::OpenStream { request_id, header } => {
                self.pending_outbound.push_back(PendingOutbound { request_id, header });
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
                protocol: (stream, header),
                ..
            }) => {
                self.events.push_back(StreamHandlerEvent::InboundStream { stream, header });
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                info: (request_id, header),
                ..
            }) => {
                self.events.push_back(StreamHandlerEvent::OutboundStream {
                    stream,
                    request_id,
                    header,
                });
            }
            ConnectionEvent::DialUpgradeError(e) => {
                self.events.push_back(StreamHandlerEvent::OutboundFailure {
                    request_id: e.info.0,
                    error: StreamError::UpgradeFailed,
                });
            }
            _ => {}
        }
    }

    fn connection_keep_alive(&self) -> bool {
        // TODO: logic here for pending disconnect?
        //
        // ???????
        //
        // Keep connection alive if we have pending work or events to emit
        self.keep_alive || !self.pending_outbound.is_empty() || !self.events.is_empty()
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

        // Request outbound streams
        if let Some(pending) = self.pending_outbound.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(
                    // TODO: should the stream protocol be specific for the request or network-wide?
                    TNStreamProtocol,
                    (pending.request_id, pending.header),
                ),
            });
        }

        Poll::Pending
    }
}
