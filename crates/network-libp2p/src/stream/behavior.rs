use libp2p::{
    swarm::{
        ConnectionHandler, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        ToSwarm,
    },
    PeerId, Stream, StreamProtocol,
};
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use crate::stream::{
    handler::{HandlerCommand, StreamHandler, StreamHandlerEvent},
    upgrade::{StreamHeader, StreamSyncError},
};

/// The protocol identifier for stream-based sync.
///
/// This protocol is used for bulk data transfer after successful request-response negotiation.
pub const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn-stream/1.0.0");

/// Events emitted by the stream behavior to the swarm/application layer.
///
/// These events notify the application when streams are established or fail,
/// enabling coordination between the request-response negotiation phase
/// and the subsequent data transfer phase.
#[derive(Debug)]
pub enum StreamEvent {
    /// An inbound stream was accepted and is ready for the application to handle.
    ///
    /// This occurs when a remote peer opens a stream to this node after
    /// successful request-response negotiation.
    InboundStream {
        /// The peer that opened the stream.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
        /// The header containing sync metadata (resource identifier, expected hash, etc.).
        header: StreamHeader,
    },
    /// An outbound stream was successfully established and is ready for use.
    ///
    /// This occurs after this node opens a stream to a peer following
    /// successful request-response negotiation. The header should be written
    /// to the stream before sending data.
    OutboundStream {
        /// The peer the stream was opened to.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
        /// The ID linking this stream to the original sync request.
        request_id: u64,
        /// The header to write to the stream before data transfer.
        header: StreamHeader,
    },
    /// Failed to establish an outbound stream.
    ///
    /// The application should handle this by notifying the original requester
    /// and potentially retrying with a different peer.
    OutboundFailure {
        /// The peer the stream failed to open to.
        peer: PeerId,
        /// The ID linking this failure to the original sync request.
        request_id: u64,
        /// The specific error that occurred.
        error: StreamSyncError,
    },
}

/// Commands from application to behavior.
///
/// These commands are used internally by the network layer to request
/// stream operations.
#[derive(Debug)]
pub enum StreamCommand {
    /// Open an outbound stream to a peer for data transfer.
    OpenStream {
        /// The peer to open the stream to.
        peer: PeerId,
        /// The ID for tracking this stream.
        request_id: u64,
        /// The header to write to the stream after it's established.
        header: StreamHeader,
    },
}

/// The network behavior for stream-based sync.
///
/// This behavior manages stream coordination across all peer connections,
/// handling both inbound streams (from peers requesting data from us) and
/// outbound streams (for requesting data from peers).
///
/// The behavior works in conjunction with request-response: after a successful
/// sync negotiation via req/res, this behavior opens a stream for bulk data transfer.
pub struct StreamBehavior {
    /// Pending outbound stream requests per peer.
    #[allow(dead_code)] // Will be used for tracking pending streams
    pending_outbound: HashMap<PeerId, VecDeque<u64>>,
    /// Events to emit to the swarm/application.
    events: VecDeque<StreamEvent>,
    /// Commands received from the application.
    commands: VecDeque<StreamCommand>,
}

impl std::fmt::Debug for StreamBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamBehavior")
            .field("pending_outbound_count", &self.pending_outbound.len())
            .field("events_count", &self.events.len())
            .field("commands_count", &self.commands.len())
            .finish()
    }
}

impl StreamBehavior {
    /// Create a new instance of the stream behavior.
    pub fn new() -> Self {
        Self {
            pending_outbound: HashMap::new(),
            events: VecDeque::new(),
            commands: VecDeque::new(),
        }
    }

    /// Initiate an outbound stream to a peer.
    ///
    /// This should be called after successful request-response negotiation.
    /// The `request_id` is used to correlate the stream with the original sync request.
    /// The `header` will be written to the stream after it's established.
    pub fn open_stream(&mut self, peer: PeerId, request_id: u64, header: StreamHeader) {
        self.commands.push_back(StreamCommand::OpenStream { peer, request_id, header });
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

    fn on_swarm_event(&mut self, _event: FromSwarm<'_>) {
        // Handle connection events if needed (currently no-op)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        match event {
            StreamHandlerEvent::InboundStream { stream, header } => {
                self.events.push_back(StreamEvent::InboundStream { peer: peer_id, stream, header });
            }
            StreamHandlerEvent::OutboundStream { stream, request_id, header } => {
                self.events.push_back(StreamEvent::OutboundStream {
                    peer: peer_id,
                    stream,
                    request_id,
                    header,
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
                StreamCommand::OpenStream { peer, request_id, header } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id: peer,
                        handler: libp2p::swarm::NotifyHandler::Any,
                        event: HandlerCommand::OpenStream { request_id, header },
                    });
                }
            }
        }

        Poll::Pending
    }
}
