use futures::{AsyncReadExt, AsyncWriteExt};
use libp2p::swarm::handler::{
    ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound, ListenUpgradeError,
    OutboundUpgradeSend,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
};
use libp2p::{Stream, StreamProtocol};
use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::stream::upgrade::{EpochSyncError, EpochSyncProtocol, StreamHeader};

/// Commands from behavior to handler
#[derive(Debug)]
pub enum HandlerCommand {
    OpenStream { request_id: u64 },
}

/// Events from handler to behavior
#[derive(Debug)]
pub enum StreamEvent {
    InboundStream { stream: Stream, header: StreamHeader },
    OutboundStream { stream: Stream, request_id: u64 },
    OutboundFailure { request_id: u64, error: EpochSyncError },
}

pub struct StreamHandler {
    /// Pending outbound stream requests
    pending_outbound: VecDeque<u64>,
    /// Events to send to behavior
    events: VecDeque<StreamEvent>,
    /// Keep connection alive while we have pending work
    keep_alive: bool,
}

impl StreamHandler {
    pub fn new() -> Self {
        Self { pending_outbound: VecDeque::new(), events: VecDeque::new(), keep_alive: true }
    }
}

impl ConnectionHandler for StreamHandler {
    type FromBehaviour = HandlerCommand;
    type ToBehaviour = StreamEvent;
    type InboundProtocol = EpochSyncProtocol;
    type OutboundProtocol = EpochSyncProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = u64; // request_id

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(EpochSyncProtocol, ())
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
                protocol: (stream, header),
                ..
            }) => {
                self.events.push_back(StreamEvent::InboundStream { stream, header });
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                info: request_id,
                ..
            }) => {
                self.events.push_back(StreamEvent::OutboundStream { stream, request_id });
            }
            ConnectionEvent::DialUpgradeError(e) => {
                self.events.push_back(StreamEvent::OutboundFailure {
                    request_id: e.info,
                    error: EpochSyncError::UpgradeFailed,
                });
            }
            _ => {}
        }
    }

    fn connection_keep_alive(&self) -> bool {
        // TODO: logic here for pending disconnect?
        //
        // ???????
        true // implement smarter logic
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        // Emit events to behavior
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Request outbound streams
        if let Some(request_id) = self.pending_outbound.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(EpochSyncProtocol, request_id),
            });
        }

        Poll::Pending
    }
}
