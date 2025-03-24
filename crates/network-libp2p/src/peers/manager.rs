//! Manage peer connection status and reputation.

use crate::peers::status::ConnectionStatus;

use super::{
    score::Penalty, types::ConnectionType, AllPeers, ConnectionDirection, NewConnectionStatus,
    PeerAction,
};
use libp2p::{core::ConnectedPoint, Multiaddr, PeerId};
use std::{collections::VecDeque, net::IpAddr, task::Context};
use tn_config::ConsensusConfig;
use tn_types::Database;
use tracing::error;

/// The type to manage peers.
pub struct PeerManager<DB> {
    /// The local [PeerId] of this node.
    peer_id: PeerId,
    /// Config
    config: ConsensusConfig<DB>,
    /// The interval to perform maintenance.
    heartbeat: tokio::time::Interval,
    /// All peers for the manager.
    peers: AllPeers,
    /// A queue of events that the `PeerManager` is waiting to produce.
    events: VecDeque<PeerEvent>,
    //
    // TNR?
}

/// Events for the [PeerManager].
#[derive(Debug)]
pub enum PeerEvent {
    /// Received dial from peer.
    PeerConnectedIncoming(PeerId),
    /// Peer was dialed.
    PeerConnectedOutgoing(PeerId),
    /// Peer was disconnected.
    PeerDisconnected(PeerId),
    /// Peer is scheduled to be disconnected.
    DisconnectPeer(PeerId),
    /// Peer manager has identified a peer and associated ip addresses to ban.
    Banned(PeerId, Vec<IpAddr>),
    /// Peer manager has unbanned a peer and associated ip addresses.
    Unbanned(PeerId, Vec<IpAddr>),
}

impl<DB> PeerManager<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(peer_id: PeerId, config: ConsensusConfig<DB>) -> Self {
        let heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(
            config.network_config().peer_config().heartbeat_interval,
        ));

        // TODO: restore from backup?
        let peers = AllPeers::default();
        let events = VecDeque::new();

        Self { peer_id, config, heartbeat, peers, events }
    }

    /// Push a [PeerEvent].
    pub(super) fn push_event(&mut self, event: PeerEvent) {
        self.events.push_back(event);
    }

    /// Poll events.
    ///
    /// This method is called when the peer manager is `poll`ed by the swarm.
    /// The next event is returned, unless there are no events to pass to the swarm.
    /// When events are empty, the capacity of the vector is shrunk as much as possible.
    pub(super) fn poll_events(&mut self) -> Option<PeerEvent> {
        if self.events.is_empty() {
            // expect ~32 events
            if self.events.capacity() > 64 {
                self.events.shrink_to(32);
            }
            None
        } else {
            self.events.pop_front()
        }
    }

    /// Returns a boolean indicating if the next instant in the heartbeat interval was reached.
    pub(super) fn heartbeat_ready(&mut self, cx: &mut Context<'_>) -> bool {
        self.heartbeat.poll_tick(cx).is_ready()
    }

    /// Heeartbeat maintenance.
    ///
    /// The manager runs routine maintenance to decay penalties for peers. This method
    /// is routine and can not further penalize peers.
    pub(super) fn heartbeat(&mut self) {
        // TODO: maintain peer count

        // update peers
        let actions = self.peers.heartbeat_maintenance();
        for (peer_id, action) in actions {
            self.apply_reputation_updates(peer_id, action);
        }

        // TODO: update peer metrics

        self.prune_peers();

        // TODO: unban peers

        todo!()
    }

    // TODO: use RepuationUpdate instead of PeerAction
    fn apply_reputation_updates(&mut self, peer_id: PeerId, update: PeerAction) {
        match update {
            PeerAction::Ban(ip_addrs) => {
                // TODO: ensure TemporaryBan is sufficiently handled at the AllPeers level
                // - current approach is to issue PeerAction::Disconnect there and use empty
                // ip addresses here to intercept for self.temp_ban LRU cache

                self.process_ban(&peer_id, ip_addrs);
            }
            PeerAction::Disconnect => {
                self.push_event(PeerEvent::DisconnectPeer(peer_id));
            }
            PeerAction::Unban(ip_addrs) => {
                self.push_event(PeerEvent::Unbanned(peer_id, ip_addrs));
            }
            // no action
            PeerAction::NoAction => { /* nothing to do */ }
            // TODO: keep this?
            //
            // NOTE: disconnecting is not an option for `ScoreUpdateResult`
            //
            PeerAction::Disconnecting => todo!(),
        }
    }

    /// Returns a boolean indicating if a peer is already connected or disconnecting.
    ///
    /// Used when handling connection closed events from the swarm.
    pub(super) fn is_peer_connected_or_disconnecting(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_connected_or_disconnecting(peer_id)
    }

    /// Returns boolean if the ip address is banned.
    pub(super) fn is_ip_banned(&self, ip: &IpAddr) -> bool {
        self.peers.ip_banned(ip)
    }

    /// Returns a boolean if the peer is a known validator.
    pub(super) fn is_validator(&self, peer_id: &PeerId) -> bool {
        self.peers.is_validator(peer_id)
    }

    /// Returns a boolean if the peer is connected.
    pub(super) fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.peers.get_peer(peer_id).is_some_and(|peer| {
            matches!(peer.connection_status(), ConnectionStatus::Connected { .. })
        })
    }

    /// Check for banned peer ids and ip addresses and return true if present.
    ///
    /// This is called before accepting new connections.
    pub(super) fn connection_banned(&self, peer_id: &PeerId) -> bool {
        self.peers.peer_banned(peer_id)
    }

    /// Process new connection and return boolean indicating if the peer limit was reached.
    pub(super) fn peer_limit_reached(&self, endpoint: &ConnectedPoint) -> bool {
        if endpoint.is_dialer() {
            // this node dialed peer
            self.peers.connected_peer_ids().count()
                >= self.config.network_config().peer_config().max_outbound_dialing_peers()
        } else {
            // peer dialed this node
            self.connected_or_dialing_peers() >= self.max_peers()
        }
    }

    /// Return an iterator of peers that are connected or dialed.
    pub fn connected_or_dialing_peers(&self) -> usize {
        self.peers.connected_or_dialing_peers().count()
    }

    /// The maximum number of peers allowed to connect with this node.
    fn max_peers(&self) -> usize {
        self.config.network_config().peer_config().max_peers()
    }

    /// Disconnect from a peer.
    ///
    /// The application layer reports issues from peers that are processed here.
    /// Some reports are propagated to libp2p network layer.
    pub(crate) fn handle_reported_penalty(&mut self, peer_id: &PeerId, penalty: Penalty) {
        let action = self.peers.process_penalty(peer_id, penalty);

        match action {
            PeerAction::Ban(ips) => {
                // Process peer banning and associated IP addresses
                //
                // IP banning applies to the specific peer address.
                // Connected peers using the same IP remain connected.
                //
                // Any new connections or reconnection attempts from banned IPs
                // result in immediate disconnection and banning.
                self.process_ban(peer_id, ips)
            }
            PeerAction::NoAction => todo!(),
            PeerAction::Disconnect => todo!(),
            PeerAction::Disconnecting => todo!(),
            PeerAction::Unban(_) => todo!(), // impossible
        }
    }

    /// Process newly banned IP addresses.
    ///
    /// The peer is disconnected and can be banned from network layer.
    fn process_ban(&mut self, peer_id: &PeerId, ips: Vec<IpAddr>) {
        self.events.push_back(PeerEvent::Banned(*peer_id, ips));

        // ensure unbanned events are removed for this peer
        self.events.retain(|event| {
            if let PeerEvent::Unbanned(unbanned_peer_id, _) = event {
                unbanned_peer_id != peer_id
            } else {
                true
            }
        });
    }

    /// Disconnect from a peer.
    ///
    /// This is the recommended graceful disconnect method. Peers are not banned.
    // TODO: include reason? "DisconnectReason"
    pub(super) fn disconnect_peer(&mut self, peer_id: PeerId) {
        self.events.push_back(PeerEvent::DisconnectPeer(peer_id));
        self.peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { banned: false },
        );
    }

    /// Convenience method to register a connected peer if their reputation is sufficient.
    ///
    /// Returns a boolean if the peer was successfully registered.
    pub(super) fn register_incoming_connection(
        &mut self,
        peer_id: &PeerId,
        multiaddr: Multiaddr,
    ) -> bool {
        self.register_peer_connection(peer_id, ConnectionType::IncomingConnection { multiaddr })
    }

    /// Convenience method to register a connected peer if their reputation is sufficient.
    ///
    /// Returns a boolean if the peer was successfully registered.
    pub(super) fn register_outgoing_connection(
        &mut self,
        peer_id: &PeerId,
        multiaddr: Multiaddr,
    ) -> bool {
        self.register_peer_connection(peer_id, ConnectionType::OutgoingConnection { multiaddr })
    }

    /// Register a connected peer if their reputation is sufficient.
    ///
    /// Returns a boolean if the peer was successfully registered.
    fn register_peer_connection(&mut self, peer_id: &PeerId, connection: ConnectionType) -> bool {
        if self.peers.peer_banned(peer_id) {
            // log error if the peer is banned
            error!(target: "peer-manager", ?peer_id, "connected with banned peer");
        }

        match connection {
            ConnectionType::Dialing => {
                self.peers.update_connection_status(peer_id, NewConnectionStatus::Dialing);
                return true;
            }
            ConnectionType::IncomingConnection { multiaddr } => {
                self.peers.update_connection_status(
                    peer_id,
                    NewConnectionStatus::Connected {
                        multiaddr,
                        direction: ConnectionDirection::Incoming,
                    },
                );
            }
            ConnectionType::OutgoingConnection { multiaddr } => {
                self.peers.update_connection_status(
                    peer_id,
                    NewConnectionStatus::Connected {
                        multiaddr,
                        direction: ConnectionDirection::Outgoing,
                    },
                );
            }
        }

        true
    }

    pub(super) fn register_disconnected(&mut self, peer_id: &PeerId) {
        let (penalty, purged_peers) = self.peers.register_disconnected(peer_id);

        // The peer was awaiting a ban, continue to ban the peer.
        // TODO: this is not the right type
        // self.handle_reported_penalty(peer_id, penalty);

        self.events.extend(
            purged_peers
                .into_iter()
                .map(|(peer_id, unbanned_ips)| PeerEvent::Unbanned(peer_id, unbanned_ips)),
        );
    }

    /// Prune peers to reach target peer counts.
    ///
    /// Trusted peers and validators are ignored. Peers are sorted from lowest to highest score and removed until excess peer count reaches target.
    fn prune_peers(&mut self) {
        // obtain config for convenience
        let config = self.config.network_config().peer_config();

        // connected peers sorted from lowest to highest aggregate score
        let connected_peers = self.peers.connected_peers_by_score();
        let mut excess_peer_count = connected_peers.len().saturating_sub(config.target_num_peers);
        if excess_peer_count == 0 {
            // target num peers within range
            return;
        }

        // filter peers that are validators
        let ready_to_prune = connected_peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                if !self.peers.is_validator(peer_id) && !peer.is_trusted() {
                    Some(**peer_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // disconnect peers until excess_peer_count is 0
        for peer_id in ready_to_prune {
            if excess_peer_count > 0 {
                self.disconnect_peer(peer_id);
                excess_peer_count = excess_peer_count.saturating_sub(1);
                continue;
            }

            // excess peers 0 - finish pruning
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    // delete me
    #[test]
    fn saturating_sub() {
        let connected = vec!['a', 'b', 'c'];
        let max_peers = 5;

        let res = connected.len().saturating_sub(max_peers);
        println!("res {res}");
        assert_eq!(res, 0);

        let danger = connected.len() - max_peers;
        println!("danger {danger}");
    }
}
