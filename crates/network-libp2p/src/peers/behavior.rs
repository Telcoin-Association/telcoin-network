//! Implement the libp2p network behavior to manage peers in the swarm.

use super::manager::{PeerEvent, PeerManager};
use libp2p::{
    core::{multiaddr::Protocol, transport::PortUse, ConnectedPoint, Endpoint},
    swarm::{
        behaviour::ConnectionEstablished, dummy::ConnectionHandler, ConnectionClosed,
        ConnectionDenied, ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use std::{
    net::IpAddr,
    task::{Context, Poll},
};
use tracing::{debug, error, info, trace};

impl NetworkBehaviour for PeerManager {
    type ConnectionHandler = ConnectionHandler;
    type ToSwarm = PeerEvent;

    // filter connections
    fn handle_pending_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        // only allow ipv4 and ipv6
        let ip = match remote_addr.iter().next() {
            Some(Protocol::Ip4(ip)) => IpAddr::V4(ip),
            Some(Protocol::Ip6(ip)) => IpAddr::V6(ip),
            _ => {
                return Err(ConnectionDenied::new(format!(
                    "Connection to peer rejected: invalid multiaddr: {remote_addr}"
                )))
            }
        };

        // ensure ip address is not banned
        if self.is_ip_banned(&ip) {
            return Err(ConnectionDenied::new(format!(
                "Connection to peer rejected: peer {ip} is banned"
            )));
        }

        Ok(())
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        trace!(target: "peer-manager", ?peer, ?remote_addr, "inbound connection established");
        // ensure banned peers are not accepted
        if self.connection_banned(&peer) {
            return Err(ConnectionDenied::new("peer is banned"));
        }

        Ok(ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        _role_override: Endpoint,
        _port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        trace!(target: "peer-manager", ?peer, ?addr, "outbound connection established");
        if self.connection_banned(&peer) {
            error!(target: "peer-manager", ?peer, ?addr, "established outbound connection with banned peer - disconnecting...");
            return Err(ConnectionDenied::new("peer is banned"));
        }

        Ok(ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id, endpoint, ..
            }) => {
                // NOTE: The ConnectionEstablished event must be handled because
                // NetworkBehaviour::handle_established_inbound_connection and
                // NetworkBehaviour::handle_established_outbound_connection are fallible.
                //
                // Another behaviour can terminate the connection early, making it unsafe to
                // assume a peer is connected until this event is received.
                self.on_connection_established(peer_id, endpoint)
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                endpoint,
                remaining_established,
                ..
            }) => self.on_connection_closed(peer_id, endpoint, remaining_established),
            FromSwarm::DialFailure(DialFailure { peer_id, error, connection_id: _ }) => {
                debug!(target: "peer-manager", ?peer_id, ?error, "failed to dial peer");
                self.on_dial_failure(peer_id);
            }
            FromSwarm::ExternalAddrConfirmed(_) => {
                // The external address was confirmed: possible to support NAT traversal
                //
                // TODO: update metrics here
            }
            _ => {
                // `FromSwarm` is non-exhaustive
                //
                // remaining events are handled by `SwarmEvent`s
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        // "dummy handler" - no events
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // poll heartbeat
        while self.heartbeat_ready(cx) {
            self.heartbeat();
        }

        // pass the next event to the swarm if the manager's events aren't empty
        if let Some(next_event) = self.poll_events() {
            return Poll::Ready(ToSwarm::GenerateEvent(next_event));
        }

        Poll::Pending
    }
}

impl PeerManager {
    /// Handle on connection established event from the swarm.
    ///
    /// The ConnectionEstablished event must be handled separately because
    /// NetworkBehaviour::handle_established_inbound_connection and
    /// NetworkBehaviour::handle_established_outbound_connection are fallible.
    ///
    /// Another behavior can terminate the connection early, making it unsafe to
    /// assume a peer is connected until this event is received.
    fn on_connection_established(&mut self, peer_id: PeerId, endpoint: &ConnectedPoint) {
        debug!(
            target: "peer-manager",
            ?peer_id,
            multiaddr = ?endpoint.get_remote_address(),
            "connection established"
        );

        // TODO: update metrics

        // check connection limits
        if self.peer_limit_reached(endpoint) && !self.is_validator(&peer_id) {
            // gracefully disconnect and indicate excess peers
            self.disconnect_peer(peer_id, true);
            return;
        }

        // do not register peers that were immediately disconnected - network service does not need to know about these peers
        match endpoint {
            ConnectedPoint::Listener { send_back_addr, .. } => {
                self.register_incoming_connection(&peer_id, send_back_addr.clone());
                self.push_event(PeerEvent::PeerConnectedIncoming(peer_id));
            }
            ConnectedPoint::Dialer { address, .. } => {
                self.register_outgoing_connection(&peer_id, address.clone());
                self.push_event(PeerEvent::PeerConnectedOutgoing(peer_id));
            }
        }

        // log successful connection establishment
        info!(
            target: "network",
            ?endpoint,
            "new connection established",
        );
    }

    /// Handle the connection closed event.
    fn on_connection_closed(
        &mut self,
        peer_id: PeerId,
        _endpoint: &ConnectedPoint,
        remaining_established: usize,
    ) {
        if remaining_established > 0 {
            return;
        }

        // there are no more connections
        if self.is_peer_connected_or_disconnecting(&peer_id) {
            // if the peer's connection status is either `Connected` or `Disconnecting`,
            // ensure the application layer is notified the peer has disconnected
            self.push_event(PeerEvent::PeerDisconnected(peer_id));
            debug!(target: "peer-manager", ?peer_id, "peer disconnected");
        }

        // if this node has too many peers, disconnect from the peer.
        // when this happens, the peer manager still needs to register this peer
        self.register_disconnected(&peer_id);

        // TODO: update metrics
    }

    /// Dial attempt failed.
    ///
    /// NOTE: `AllPeers` is only updated if the peer is not already connected. It's possible that
    /// an outgoing dial attempt fails because the peer connected during the dial.
    fn on_dial_failure(&mut self, peer_id: Option<PeerId>) {
        if let Some(peer_id) = peer_id {
            if !self.is_connected(&peer_id) {
                self.register_disconnected(&peer_id);
            }
        }
    }
}
