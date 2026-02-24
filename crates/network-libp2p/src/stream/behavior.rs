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

use crate::stream::{
    handler::{HandlerCommand, StreamHandler, StreamHandlerEvent},
    upgrade::StreamError,
};

/// The protocol identifier for stream-based sync.
pub const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn-stream/1.0.0");

/// Events emitted by the stream behavior to the swarm/application layer.
#[derive(Debug)]
pub enum StreamEvent {
    /// An inbound stream was accepted and is ready for the application to handle.
    InboundStream {
        /// The peer that opened the stream.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
    },
    /// An outbound stream was successfully established and is ready for use.
    OutboundStream {
        /// The peer the stream was opened to.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
        /// The ID linking this stream to the original request.
        request_id: u64,
    },
    /// Failed to establish an outbound stream.
    OutboundFailure {
        /// The peer the stream failed to open to.
        peer: PeerId,
        /// The ID linking this failure to the original request.
        request_id: u64,
        /// The specific error that occurred.
        error: StreamError,
    },
}

/// Commands from application to behavior.
#[derive(Debug)]
pub enum StreamCommand {
    /// Open an outbound stream to a peer.
    OpenStream {
        /// The peer to open the stream to.
        peer: PeerId,
        /// The ID for tracking this stream.
        request_id: u64,
    },
}

/// The network behavior for stream-based sync.
///
/// Manages stream establishment across peer connections. After a successful
/// request-response negotiation, this behavior opens a raw stream for the
/// application to use for bulk data transfer.
pub struct StreamBehavior {
    /// Events to emit to the swarm/application.
    events: VecDeque<StreamEvent>,
    /// Commands received from the application.
    commands: VecDeque<StreamCommand>,
}

impl std::fmt::Debug for StreamBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamBehavior")
            .field("events_count", &self.events.len())
            .field("commands_count", &self.commands.len())
            .finish()
    }
}

impl StreamBehavior {
    /// Create a new instance of the stream behavior.
    pub fn new() -> Self {
        Self { events: VecDeque::new(), commands: VecDeque::new() }
    }

    /// Initiate an outbound stream to a peer.
    ///
    /// The `request_id` is used to correlate the stream with the original request.
    pub fn open_stream(&mut self, peer: PeerId, request_id: u64) {
        self.commands.push_back(StreamCommand::OpenStream { peer, request_id });
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
            StreamHandlerEvent::OutboundStream { stream, request_id } => {
                self.events.push_back(StreamEvent::OutboundStream {
                    peer: peer_id,
                    stream,
                    request_id,
                });
            }
            StreamHandlerEvent::OutboundFailure { request_id, error } => {
                self.events.push_back(StreamEvent::OutboundFailure {
                    peer: peer_id,
                    request_id,
                    error,
                });
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

        // Process commands - tell handlers to open streams
        if let Some(command) = self.commands.pop_front() {
            match command {
                StreamCommand::OpenStream { peer, request_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: peer,
                        handler: libp2p::swarm::NotifyHandler::Any,
                        event: HandlerCommand::OpenStream { request_id },
                    });
                }
            }
        }

        Poll::Pending
    }
}
