//! Implement the libp2p network behavior to manage peers in the swarm.

use super::{manager::PeerManager, types::DialRequest, PeerEvent};
use crate::peers::types::ConnectionType;
use libp2p::{
    core::{transport::PortUse, ConnectedPoint, Endpoint},
    swarm::{
        behaviour::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        dummy::ConnectionHandler,
        ConnectionClosed, ConnectionDenied, ConnectionId, DialError, DialFailure, FromSwarm,
        ListenError, ListenFailure, NetworkBehaviour, THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use std::task::{Context, Poll};
use tracing::{debug, error, info, trace};

/// A policy denial owned by the peer manager, preserved through libp2p's error wrapper.
#[derive(Debug, Clone, Copy)]
enum PeerAdmissionDenied {
    /// The remote identity is this swarm's own identity.
    LocalPeer,
    /// The authenticated remote peer is banned.
    BannedPeer,
    /// The remote address has no supported, unbanned IP address.
    InvalidIp,
}

impl PeerAdmissionDenied {
    /// The bounded metric reason for this policy denial.
    fn reason(&self) -> &'static str {
        match self {
            Self::LocalPeer => "peer_manager_local_peer",
            Self::BannedPeer => "peer_manager_banned_peer",
            Self::InvalidIp => "peer_manager_invalid_ip",
        }
    }
}

impl std::fmt::Display for PeerAdmissionDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::LocalPeer => "self-connection: remote peer id is our own",
            Self::BannedPeer => "peer is banned",
            Self::InvalidIp => "Connection denied: peer has no valid unbanned IP addresses",
        };
        f.write_str(message)
    }
}

impl std::error::Error for PeerAdmissionDenied {}

impl NetworkBehaviour for PeerManager {
    type ConnectionHandler = ConnectionHandler;
    type ToSwarm = PeerEvent;

    /// Apply the current policy before accepting either manager or Kademlia dials.
    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr], // kad may dial by PeerId only
        _effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.ensure_connection_authorized(maybe_peer.as_ref())?;
        // kademlia can initiate dial attempts
        //
        // ensure PeerId isn't banned if known and register dial attempt
        if let Some(peer_id) = maybe_peer {
            // refuse to dial our own identity. Kademlia can auto-dial an address
            // it learned for a peer id (e.g. our own record re-learned via a
            // hairpin address); if that id is ours, deny it here before a
            // self-connection is established.
            if self.is_local_peer(&peer_id) {
                debug!(target: "peer-manager", ?peer_id, "denying outbound connection to local peer id");
                return Err(ConnectionDenied::new("refusing to dial self"));
            }
            // PeerManager and Kad may initiate dials
            // intercept kad dial attempts, sanitize, and register
            if self.dial_attempt_already_registered(&peer_id) {
                if self.peer_banned(&peer_id) {
                    debug!(target: "peer-manager", ?peer_id, "rejecting in-flight dial for banned peer");
                    return Err(ConnectionDenied::new(
                        "Outbound connection to banned peer".to_string(),
                    ));
                }
                // peer manager has already approved this dial attempt
                return Ok(vec![]);
            }

            debug!(target: "peer-manager", ?peer_id, ?addresses, "kad initiated dial attempt for peer");
            // peer is not registered, ensure can be dialed
            if self.can_dial(&peer_id) {
                trace!(target: "peer-manager", ?peer_id, "can_dial success");
                self.register_dial_attempt(peer_id, None);
            } else {
                debug!(target: "peer-manager", ?peer_id, "can_dial failed");
                return Err(ConnectionDenied::new(
                    "Outbound connection to peer denied: peer cannot be dialed".to_string(),
                ));
            }
        }

        // do not check peer connection limits since kad may try to find better peers for routing
        // excess peers are pruned next heartbeat
        //
        // NOTE: kademlia extends addresses by default
        // See swarm `WithPeerId::build` -> DialOpts
        Ok(vec![])
    }

    // filter connections
    fn handle_pending_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.sanitize_ip_addr(remote_addr)
    }

    /// Recheck the authenticated inbound identity against the current policy.
    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        // drop a self-connection (loopback/hairpin back to our own id) without
        // scoring it. The inbound peer id is only known at this stage, so this is
        // the earliest point an inbound self-connection can be rejected.
        if self.is_local_peer(&peer) {
            return Err(ConnectionDenied::new(PeerAdmissionDenied::LocalPeer));
        }
        // ensure banned peers are not accepted
        self.ensure_connection_authorized(Some(&peer))?;
        if self.peer_banned(&peer) {
            return Err(ConnectionDenied::new(PeerAdmissionDenied::BannedPeer));
        }

        self.reserve_source(connection_id, peer, remote_addr, "in")?;
        Ok(ConnectionHandler)
    }

    /// Recheck the authenticated outbound identity after any intervening mode change.
    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        _role_override: Endpoint,
        _port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        trace!(target: "peer-manager", ?peer, ?addr, "outbound connection established");
        // drop a self-connection without scoring it (backstop for the pending
        // guard in case a self-dial still reaches the established stage).
        if self.is_local_peer(&peer) {
            debug!(target: "peer-manager", ?peer, ?addr, "denying outbound self-connection");
            return Err(ConnectionDenied::new("self-connection: remote peer id is our own"));
        }
        self.ensure_connection_authorized(Some(&peer))?;
        if self.peer_banned(&peer) {
            error!(target: "peer-manager", ?peer, ?addr, "established outbound connection with banned peer - disconnecting...");
            return Err(ConnectionDenied::new("peer is banned"));
        }

        // kad may dial peers by PeerId only, so always santize ban IPs after connection established
        self.sanitize_ip_addr(addr)?;

        self.reserve_source(connection_id, peer, addr, "out")?;
        Ok(ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        self.on_source_swarm_event(&event);
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
                self.on_dial_failure(peer_id, error);
            }
            FromSwarm::ListenFailure(ListenFailure { error, .. }) => {
                // Inbound hooks reserve no peer-manager state. Peers are registered only on
                // ConnectionEstablished, after every behaviour has accepted. The swarm and
                // connection_limits own pending slots and clean them up on this same event.
                // Do not disconnect the peer or complete a concurrent outbound dial here.
                // Counters replace per-attempt logs on this remotely driven failure path.
                let reason = match error {
                    ListenError::Denied { cause } => cause
                        .downcast_ref::<PeerAdmissionDenied>()
                        .map_or("other_behaviour_denied", PeerAdmissionDenied::reason),
                    ListenError::Transport(_) => "transport",
                    ListenError::WrongPeerId { .. } => "wrong_peer_id",
                    ListenError::LocalPeerId { .. } => "local_peer_id",
                    ListenError::Aborted => "aborted",
                };
                self.metrics.record_listen_failure(reason);
            }
            FromSwarm::ExternalAddrConfirmed(_) => {
                // The external address was confirmed: possible to support NAT traversal
                self.metrics.record_external_addr_confirmed();
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

    /// Remember the swarm waker so policy recovery can schedule fresh discovery immediately.
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.register_discovery_waker(cx);
        // poll heartbeat
        while self.heartbeat_ready(cx) {
            self.heartbeat();
        }

        // pass the next event to the swarm if the manager's events aren't empty
        if let Some(next_event) = self.poll_events() {
            return Poll::Ready(ToSwarm::GenerateEvent(next_event));
        }

        // process dial requests after all events drained
        if let Some(request) = self.next_dial_request() {
            let DialRequest { peer_id, multiaddrs, reply } = request;

            debug!(target: "network", ?peer_id, "network behavior processing next dial request");

            // register to send result back to caller
            self.register_dial_attempt(peer_id, reply);

            // swarm to dial peer
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer_id)
                    .condition(PeerCondition::Disconnected)
                    .addresses(multiaddrs)
                    // Try signed endpoints in order, with one connection attempt at a time.
                    .override_dial_concurrency_factor(std::num::NonZeroU8::MIN)
                    .build(),
            });
        }

        Poll::Pending
    }
}

impl PeerManager {
    /// Logic to ensure a pending connection supports ipv4 or ipv6, and that the ip address isn't
    /// banned.
    fn sanitize_ip_addr(&self, remote_addr: &Multiaddr) -> Result<(), ConnectionDenied> {
        // only support ipv4 and ipv6
        if !self.has_valid_unbanned_ips(std::slice::from_ref(remote_addr)) {
            return Err(ConnectionDenied::new(PeerAdmissionDenied::InvalidIp));
        }
        Ok(())
    }

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

        // register peers as connected by this point
        // even if the peer is to be immediately disconnected with peer-exchange (PX)
        let multiaddr = match endpoint {
            ConnectedPoint::Listener { send_back_addr, .. } => {
                self.register_peer_connection(
                    &peer_id,
                    ConnectionType::IncomingConnection { multiaddr: send_back_addr.clone() },
                );
                self.metrics.record_connection_established("in");
                send_back_addr.clone()
            }
            ConnectedPoint::Dialer { address, .. } => {
                self.register_peer_connection(
                    &peer_id,
                    ConnectionType::OutgoingConnection { multiaddr: address.clone() },
                );
                self.metrics.record_connection_established("out");
                address.clone()
            }
        };

        // check connection limits
        if self.peer_limit_reached(endpoint) && !self.peer_is_important(&peer_id) {
            debug!(target: "peer-manager", ?peer_id, "peer limit reached - disconnecting with PX");
            // gracefully disconnect and indicate excess peers
            self.disconnect_peer(peer_id, true);
            return;
        }

        self.push_event(PeerEvent::PeerConnected(peer_id, multiaddr));

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

        self.metrics.record_connection_closed();
    }

    /// Dial attempt failed.
    ///
    /// NOTE: `AllPeers` is only updated if the peer is _not_ already connected. It's possible that
    /// an outgoing dial attempt fails because the peer connected during the dial.
    pub(super) fn on_dial_failure(&mut self, peer_id: Option<PeerId>, error: &DialError) {
        self.metrics.record_dial_failure();
        if let Some(peer_id) = peer_id {
            if !self.is_connected(&peer_id) {
                self.register_disconnected(&peer_id);
            }

            // return the genuine dial error to the dialer. `register_disconnected` no longer
            // consumes the reply channel with a hardcoded cause, so the real `DialError`
            // (wrong key, refused, firewall, timeout) reaches the caller.
            self.notify_dial_result(&peer_id, Err(error.into()));
        }
    }
}
