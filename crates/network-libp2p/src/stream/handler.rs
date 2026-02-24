use libp2p::{
    swarm::{
        handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol,
    },
    Stream,
};
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

use crate::stream::upgrade::{StreamError, TNStreamProtocol};

/// Commands from behavior to handler.
#[derive(Debug)]
pub enum HandlerCommand {
    /// Open an outbound stream with the given request ID.
    OpenStream {
        /// The ID for tracking this stream.
        request_id: u64,
    },
}

/// Events from handler to behavior.
#[derive(Debug)]
pub enum StreamHandlerEvent {
    /// An inbound stream was successfully established.
    InboundStream {
        /// The established stream.
        stream: Stream,
    },
    /// An outbound stream was successfully established.
    OutboundStream {
        /// The established stream.
        stream: Stream,
        /// The ID for this stream.
        request_id: u64,
    },
    /// Failed to establish an outbound stream.
    OutboundFailure {
        /// The ID for the failed stream.
        request_id: u64,
        /// The error that occurred.
        error: StreamError,
    },
}

/// Connection handler for stream-based sync.
///
/// Manages streams on a single peer connection, processing inbound stream
/// requests and initiating outbound streams when commanded.
pub struct StreamHandler {
    /// Pending outbound request IDs.
    pending_outbound: VecDeque<u64>,
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
    /// Create a new stream handler.
    pub fn new() -> Self {
        Self { pending_outbound: VecDeque::new(), events: VecDeque::new() }
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
    type OutboundOpenInfo = u64;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(TNStreamProtocol, ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerCommand::OpenStream { request_id } => {
                self.pending_outbound.push_back(request_id);
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
                protocol: stream,
                ..
            }) => {
                self.events.push_back(StreamHandlerEvent::InboundStream { stream });
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                info: request_id,
                ..
            }) => {
                self.events.push_back(StreamHandlerEvent::OutboundStream { stream, request_id });
            }
            ConnectionEvent::DialUpgradeError(e) => {
                self.events.push_back(StreamHandlerEvent::OutboundFailure {
                    request_id: e.info,
                    error: StreamError::UpgradeFailed,
                });
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

        // Request outbound streams
        if let Some(request_id) = self.pending_outbound.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(TNStreamProtocol, request_id),
            });
        }

        Poll::Pending
    }
}
