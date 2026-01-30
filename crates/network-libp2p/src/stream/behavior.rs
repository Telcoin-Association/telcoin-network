use futures::channel::oneshot;
use libp2p::{
    swarm::{
        handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionId, FromSwarm, NetworkBehaviour, SubstreamProtocol, THandler,
        THandlerInEvent, ToSwarm,
    },
    PeerId, Stream, StreamProtocol,
};
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use crate::stream::{
    handler::{HandlerCommand, StreamEvent, StreamHandler},
    upgrade::{EpochSyncError, StreamHeader},
};

pub(super) const EPOCH_SYNC_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn/epoch-sync/1.0.0");

/// Events emitted by the behavior to the swarm/application
#[derive(Debug)]
pub enum EpochSyncEvent {
    /// Inbound stream accepted, ready for application to handle
    InboundStream { peer: PeerId, stream: Stream, header: StreamHeader },
    /// Outbound stream established, ready for application to use
    OutboundStream { peer: PeerId, stream: Stream, request_id: u64 },
    /// Stream failed to establish
    OutboundFailure { peer: PeerId, request_id: u64, error: EpochSyncError },
}

/// Commands from application to behavior
#[derive(Debug)]
pub enum StreamCommand {
    /// Open an outbound stream to peer for epoch sync
    OpenStream { peer: PeerId, request_id: u64 },
}

/// The network behavior for epoch sync streams
pub struct StreamBehaviour {
    /// Pending outbound stream requests
    pending_outbound: HashMap<PeerId, VecDeque<u64>>, // peer -> request_ids
    /// Events to emit
    events: VecDeque<EpochSyncEvent>,
    /// Commands received from application
    commands: VecDeque<StreamCommand>,
}

impl StreamBehaviour {
    pub fn new() -> Self {
        Self {
            pending_outbound: HashMap::new(),
            events: VecDeque::new(),
            commands: VecDeque::new(),
        }
    }

    /// Called by application to initiate outbound stream
    pub fn open_stream(&mut self, peer: PeerId, request_id: u64) {
        self.commands.push_back(StreamCommand::OpenStream { peer, request_id });
    }
}

impl NetworkBehaviour for StreamBehaviour {
    type ConnectionHandler = StreamHandler;
    type ToSwarm = EpochSyncEvent;

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

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        // Handle connection events if needed
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        match event {
            StreamEvent::InboundStream { stream, header } => {
                self.events.push_back(EpochSyncEvent::InboundStream {
                    peer: peer_id,
                    stream,
                    header,
                });
            }
            StreamEvent::OutboundStream { stream, request_id } => {
                self.events.push_back(EpochSyncEvent::OutboundStream {
                    peer: peer_id,
                    stream,
                    request_id,
                });
            }
            StreamEvent::OutboundFailure { request_id, error } => {
                self.events.push_back(EpochSyncEvent::OutboundFailure {
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
