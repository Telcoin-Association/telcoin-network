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
use tokio::sync::oneshot;

use crate::{
    error::NetworkError,
    stream::upgrade::{StreamError, TNStreamProtocol},
    types::NetworkResult,
};

/// Commands from behavior to handler.
#[derive(Debug)]
pub(crate) enum HandlerCommand {
    /// Open an outbound stream, returning it through the provided channel.
    OpenStream {
        /// Channel for returning the established stream directly to the caller.
        reply: oneshot::Sender<NetworkResult<Stream>>,
    },
}

/// Events from handler to behavior.
#[derive(Debug)]
pub(crate) enum StreamHandlerEvent {
    /// An inbound stream was successfully established.
    InboundStream {
        /// The established stream.
        stream: Stream,
    },
}

/// Connection handler for streaming data.
///
/// Manages streams on a single peer connection, processing inbound stream
/// requests and initiating outbound streams when commanded. Outbound streams
/// are returned directly to callers via oneshot channels passed through
/// `OutboundOpenInfo`, bypassing the behavior layer entirely.
#[derive(Default)]
pub(crate) struct StreamHandler {
    /// Pending outbound stream reply channels.
    pending_outbound: VecDeque<oneshot::Sender<NetworkResult<Stream>>>,
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
    pub(crate) fn new() -> Self {
        Self { pending_outbound: VecDeque::new(), events: VecDeque::new() }
    }
}

impl ConnectionHandler for StreamHandler {
    type FromBehaviour = HandlerCommand;
    type ToBehaviour = StreamHandlerEvent;
    type InboundProtocol = TNStreamProtocol;
    type OutboundProtocol = TNStreamProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = oneshot::Sender<NetworkResult<Stream>>;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(TNStreamProtocol, ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerCommand::OpenStream { reply } => {
                self.pending_outbound.push_back(reply);
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
                info: reply,
                ..
            }) => {
                // Return the stream directly to the caller via oneshot
                let _ = reply.send(Ok(stream));
            }
            ConnectionEvent::DialUpgradeError(e) => {
                // Return the error directly to the caller via oneshot
                let _ = e.info.send(Err(NetworkError::Stream(StreamError::UpgradeFailed)));
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
        if let Some(reply) = self.pending_outbound.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(TNStreamProtocol, reply),
            });
        }

        Poll::Pending
    }
}
