use libp2p::{
    swarm::{
        ConnectionHandler, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        ToSwarm,
    },
    PeerId, Stream, StreamProtocol,
};
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};
use tokio::sync::oneshot;

use crate::{
    stream::handler::{HandlerCommand, StreamHandler, StreamHandlerEvent},
    types::NetworkResult,
};

/// The protocol identifier for stream-based sync.
pub(crate) const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn-stream/1.0.0");

/// Events emitted by the stream behavior to the swarm/application layer.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// An inbound stream was accepted and is ready for the application to handle.
    InboundStream {
        /// The peer that opened the stream.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
    },
}

/// Internal command queued for dispatch to a connection handler.
struct PendingOpen {
    /// The peer to open the stream to.
    peer: PeerId,
    /// Channel for returning the established stream directly to the caller.
    reply: oneshot::Sender<NetworkResult<Stream>>,
}

/// The network behavior for stream-based sync.
///
/// Manages stream establishment across peer connections. After a successful
/// request-response negotiation, this behavior opens a raw stream for the
/// application to use for bulk data transfer.
///
/// Outbound streams are returned directly to the caller via oneshot channels,
/// without requiring correlation IDs or external tracking. The oneshot sender
/// is passed through to the connection handler as `OutboundOpenInfo`.
pub(crate) struct StreamBehavior {
    /// Events to emit to the swarm/application.
    events: VecDeque<StreamEvent>,
    /// Pending outbound stream requests to dispatch to handlers.
    pending_opens: VecDeque<PendingOpen>,
}

impl std::fmt::Debug for StreamBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamBehavior")
            .field("events_count", &self.events.len())
            .field("pending_opens_count", &self.pending_opens.len())
            .finish()
    }
}

impl StreamBehavior {
    /// Create a new instance of the stream behavior.
    pub(crate) fn new() -> Self {
        Self { events: VecDeque::new(), pending_opens: VecDeque::new() }
    }

    /// Initiate an outbound stream to a peer.
    ///
    /// The established stream (or error) will be sent directly through `reply`.
    pub(crate) fn open_stream(
        &mut self,
        peer: PeerId,
        reply: oneshot::Sender<NetworkResult<Stream>>,
    ) {
        self.pending_opens.push_back(PendingOpen { peer, reply });
    }
}

impl Default for StreamBehavior {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkBehaviour for StreamBehavior {
    type ConnectionHandler = StreamHandler;
    type ToSwarm = StreamEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &libp2p::Multiaddr,
        _: &libp2p::Multiaddr,
    ) -> Result<THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(StreamHandler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &libp2p::Multiaddr,
        _: libp2p::core::Endpoint,
        _: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(StreamHandler::new())
    }

    fn on_swarm_event(&mut self, _event: FromSwarm<'_>) {}

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        match event {
            StreamHandlerEvent::InboundStream { stream } => {
                self.events.push_back(StreamEvent::InboundStream { peer: peer_id, stream });
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // Emit pending events
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }

        // Dispatch pending stream open requests to handlers
        if let Some(PendingOpen { peer, reply }) = self.pending_opens.pop_front() {
            return Poll::Ready(ToSwarm::NotifyHandler {
                peer_id: peer,
                handler: libp2p::swarm::NotifyHandler::Any,
                event: HandlerCommand::OpenStream { reply },
            });
        }

        Poll::Pending
    }
}
