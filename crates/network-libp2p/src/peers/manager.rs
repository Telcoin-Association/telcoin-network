//! Manage peer connection status and reputation.

use super::{
    cache::BannedPeerCache, init_peer_score_config, score::Penalty, types::ConnectionType,
    AllPeers, ConnectionDirection, NewConnectionStatus, PeerAction, PeerExchangeMap,
};
use crate::peers::status::ConnectionStatus;
use libp2p::{core::ConnectedPoint, Multiaddr, PeerId};
use std::{
    collections::{HashSet, VecDeque},
    net::IpAddr,
    task::Context,
};
use tn_config::{ConsensusConfig, PeerConfig, GENESIS_VALIDATORS_DIR};
use tn_types::Database;
use tracing::{debug, error};

/// The type to manage peers.
pub struct PeerManager {
    /// Config
    config: PeerConfig,
    /// The interval to perform maintenance.
    heartbeat: tokio::time::Interval,
    /// All peers for the manager.
    peers: AllPeers,
    /// A queue of events that the `PeerManager` is waiting to produce.
    events: VecDeque<PeerEvent>,
    /// Tracks temporarily banned peers to prevent immediate reconnection attempts.
    ///
    /// This LRU cache manages a time-based ban list that operates independently
    /// from the peer's state. Characteristics:
    ///
    /// - Prevents reconnection attempts at the network layer without affecting the
    ///   peer's stored state
    /// - Peers appear to be banned for connection purposes while still having a
    ///   non-banned state in the database
    /// - Ban records persist even after a peer is removed from the database, allowing
    ///   rejection of unknown peers based on previous temporary bans
    /// - Control the time-based LRU cache mechanism by leveraging the PeerManager's
    ///   heartbeat cycle for maintenance instead of requiring separate polling
    /// - The actual ban duration has a resolution limited by the heartbeat interval,
    ///   as cache cleanup occurs during heartbeat events
    ///
    /// The implementation uses `FnvHashSet` instead of the default Rust hasher `SipHash`
    /// for improved performance for short keys.
    temporarily_banned: BannedPeerCache<PeerId>,
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
    /// Disconnect from the peer without exchanging peer information.
    /// This is the event for disconnecting from penalized peers.
    DisconnectPeer(PeerId),
    /// Disconnect from the peer and share peer information for discovery.
    /// This is the event for disconnecting from excess peers with otherwise trusted reputations.
    DisconnectPeerX(PeerId, PeerExchangeMap),
    /// Peer manager has identified a peer and associated ip addresses to ban.
    Banned(PeerId, Vec<IpAddr>),
    /// Peer manager has unbanned a peer and associated ip addresses.
    Unbanned(PeerId, Vec<IpAddr>),
}

impl PeerManager {
    /// Create a new instance of Self.
    pub(crate) fn new<DB: Database>(consensus_config: &ConsensusConfig<DB>) -> Self {
        let config = consensus_config.network_config().peer_config();
        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(config.heartbeat_interval));

        // TODO: restore peers from backup?
        let validators = consensus_config.committee_peer_ids();
        let peers = AllPeers::new(validators, config.target_num_peers, config.dial_timeout);
        let events = VecDeque::new();
        let temporarily_banned = BannedPeerCache::new(config.excess_peers_reconnection_timeout);

        // initialize global score config
        init_peer_score_config(config.score_config);

        Self { config: *config, heartbeat, peers, events, temporarily_banned }
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
        // update peers
        let actions = self.peers.heartbeat_maintenance();
        for (peer_id, action) in actions {
            self.apply_peer_action(peer_id, action);
        }

        // TODO: update peer metrics

        self.prune_connected_peers();

        self.unban_temp_banned_peers();
    }

    /// Apply a [PeerAction].
    ///
    /// Actions on peers happen when their reputation or connection status changes.
    fn apply_peer_action(&mut self, peer_id: PeerId, action: PeerAction) {
        match action {
            PeerAction::Ban(ip_addrs) => {
                debug!(target: "peer-manager", ?peer_id, ?ip_addrs, "reputation update results in ban");
                self.process_ban(&peer_id, ip_addrs);
            }
            PeerAction::Disconnect => {
                debug!(target: "peer-manager", ?peer_id, "reputation update results in disconnect");
                self.push_event(PeerEvent::DisconnectPeer(peer_id));
            }
            PeerAction::DisconnectWithPX => {
                debug!(target: "peer-manager", ?peer_id, "reputation update results in temp ban");
                // prevent immediate reconnection attempts
                self.temporarily_banned.insert(peer_id);
                let exchange = self.peers.peer_exchange();
                self.events.push_back(PeerEvent::DisconnectPeerX(peer_id, exchange));
            }
            PeerAction::Unban(ip_addrs) => {
                debug!(target: "peer-manager", ?peer_id, ?ip_addrs, "reputation update results in unban");
                self.push_event(PeerEvent::Unbanned(peer_id, ip_addrs));
            }

            PeerAction::NoAction => { /* nothing to do */ }
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

    /// Check if the peer id is banned or associated with any banned ip addresses.
    ///
    /// This is called before accepting new connections. Also checks that the peer
    /// wasn't temporarily banned due to excess peers connections.
    pub(super) fn connection_banned(&self, peer_id: &PeerId) -> bool {
        self.temporarily_banned.contains(peer_id) || self.peers.peer_banned(peer_id)
    }

    /// Process new connection and return boolean indicating if the peer limit was reached.
    pub(super) fn peer_limit_reached(&self, endpoint: &ConnectedPoint) -> bool {
        if endpoint.is_dialer() {
            // this node dialed peer
            self.peers.connected_peer_ids().count() >= self.config.max_outbound_dialing_peers()
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
        self.config.max_peers()
    }

    /// Process a penalty from the application layer.
    ///
    /// The application layer reports issues from peers that are processed here.
    /// Some reports are propagated to libp2p network layer. Caller is responsible
    /// for specifying the severity of the penalty to apply.
    pub(crate) fn process_penalty(&mut self, peer_id: PeerId, penalty: Penalty) {
        let action = self.peers.process_penalty(&peer_id, penalty);

        debug!(target: "peer-manager", ?peer_id, ?action, "processed penalty");
        self.apply_peer_action(peer_id, action);
    }

    /// Process newly banned IP addresses.
    ///
    /// The peer is disconnected and is banned from network layer.
    fn process_ban(&mut self, peer_id: &PeerId, ips: Vec<IpAddr>) {
        // ensure unbanned events are removed for this peer
        self.events.retain(|event| {
            if let PeerEvent::Unbanned(unbanned_peer_id, _) = event {
                unbanned_peer_id != peer_id
            } else {
                true
            }
        });

        // push banned event
        self.events.push_back(PeerEvent::Banned(*peer_id, ips));
    }

    /// Disconnect from a peer.
    ///
    /// This is the recommended graceful disconnect method and is called when peers
    /// are penalized or if connecting with a dialing peer would result in excess peer
    /// count.
    ///
    /// The argument `support_discovery` indicates if the disconnect message should
    /// include additional connected peers to help the peer discovery other nodes.
    /// Peers that are disconnected because of excess peer limits support discovery.
    pub(super) fn disconnect_peer(&mut self, peer_id: PeerId, support_discovery: bool) {
        // include peer exchange or not
        let event = if support_discovery {
            let exchange = self.peers.peer_exchange();
            PeerEvent::DisconnectPeerX(peer_id, exchange)
        } else {
            PeerEvent::DisconnectPeer(peer_id)
        };

        self.events.push_back(event);
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

    /// Register disconnected peers.
    ///
    /// Some peers are disconnected with the intention to ban that peer.
    /// This method registers the peer as disconnected and ensures the list of banned/disconnected peers
    /// doesn't grow infinitely large. Peers may become "unbanned" if the limit for banned peers
    /// is reached.
    pub(super) fn register_disconnected(&mut self, peer_id: &PeerId) {
        let (action, pruned_peers) = self.peers.register_disconnected(peer_id);

        // banning is the only action that happens after disconnect
        // if the peer is banned then manager needs to apply the ban still
        // otherwise, there is no other action to take
        if action.is_ban() {
            self.apply_peer_action(*peer_id, action);
        }

        // process pruned peers
        self.events.extend(
            pruned_peers
                .into_iter()
                .map(|(peer_id, unbanned_ips)| PeerEvent::Unbanned(peer_id, unbanned_ips)),
        );
    }

    /// Prune peers to reach target peer counts.
    ///
    /// Trusted peers and validators are ignored. Peers are sorted from lowest to highest score and removed until excess peer count reaches target.
    fn prune_connected_peers(&mut self) {
        // connected peers sorted from lowest to highest aggregate score
        let connected_peers = self.peers.connected_peers_by_score();
        let mut excess_peer_count =
            connected_peers.len().saturating_sub(self.config.target_num_peers);
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
                self.disconnect_peer(peer_id, true);
                excess_peer_count = excess_peer_count.saturating_sub(1);
                continue;
            }

            // excess peers 0 - finish pruning
            break;
        }
    }

    /// Unban temporarily banned peers.
    ///
    /// Peers are temporarily "banned" when trying to connect while this node has excess peers.
    fn unban_temp_banned_peers(&mut self) {
        for peer_id in self.temporarily_banned.heartbeat() {
            self.push_event(PeerEvent::Unbanned(peer_id, Vec::new()));
        }
    }

    /// Process peer exchange for peer discovery.
    pub(crate) fn process_peer_exchange(&mut self, peers: PeerExchangeMap) {
        // peers.into_iter()
        todo!()
    }

    /// Create [PeerExchangeMap] for exchange with peers.
    pub(crate) fn peers_for_exchange(&self) -> PeerExchangeMap {
        self.peers.peer_exchange()
    }

    /// Test only
    ///
    /// Push an event to the swarm from `Self`.
    #[cfg(test)]
    pub(crate) fn push_test_event(&mut self, event: PeerEvent) {
        self.push_event(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::net::Ipv4Addr;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::CommitteeFixture;

    fn create_test_peer_manager() -> PeerManager {
        let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
        let mut authorities = all_nodes.authorities();
        let authority_1 = authorities.next().expect("first authority");
        let config = authority_1.consensus_config();
        PeerManager::new(&config)
    }

    fn create_test_multiaddr(id: u8) -> Multiaddr {
        format!("/ip4/{}/udp/45454/quic-v1", Ipv4Addr::new(127, 0, 0, id)).parse().unwrap()
    }

    /// Helper function to extract events of a certain type
    fn extract_events<'a>(
        events: &'a [PeerEvent],
        event_type: fn(&'a PeerEvent) -> bool,
    ) -> Vec<&'a PeerEvent> {
        events.iter().filter(|e| event_type(e)).collect()
    }

    #[test]
    fn test_register_disconnected_basic() {
        let mut peer_manager = create_test_peer_manager();
        let peer_id = PeerId::random();
        let multiaddr = create_test_multiaddr(1);

        // register connection
        assert!(peer_manager.register_incoming_connection(&peer_id, multiaddr));

        // register disconnection
        peer_manager.register_disconnected(&peer_id);

        // assert peer is no longer connected
        assert!(!peer_manager.is_connected(&peer_id));

        // assert no events from disconnect without ban
        assert!(peer_manager.poll_events().is_none());
    }

    #[tokio::test]
    async fn test_register_disconnected_with_banned_peer() {
        tn_test_utils::init_test_tracing();

        let mut peer_manager = create_test_peer_manager();
        let peer_id = PeerId::random();
        let multiaddr = create_test_multiaddr(2);

        // Register connection first
        assert!(peer_manager.register_incoming_connection(&peer_id, multiaddr));

        // Apply a severe penalty to trigger ban
        peer_manager.process_penalty(peer_id, Penalty::Fatal);

        // clear events from reported penalty
        let mut disconnect_events = Vec::new();
        while let Some(event) = peer_manager.poll_events() {
            disconnect_events.push(event);
        }

        // assert peer is set for disconnect
        let disconnect_event =
            extract_events(&disconnect_events, |e| matches!(e, PeerEvent::DisconnectPeer(_))).len();
        assert!(disconnect_event == 1, "Expect one disconnect event");
        assert_matches!(
            disconnect_events.first().unwrap(),
            PeerEvent::DisconnectPeer(id) if *id == peer_id
        );

        debug!(target: "peer-manager", ?disconnect_event, "made it here :D");

        // register disconnection
        peer_manager.register_disconnected(&peer_id);

        // There should be no additional ban events since the peer is already banned
        let mut banned_events = Vec::new();
        while let Some(event) = peer_manager.poll_events() {
            banned_events.push(event);
        }

        debug!(target: "peer-manager", ?banned_events, "made it here");

        let banned_event =
            extract_events(&banned_events, |e| matches!(e, PeerEvent::Banned(_, _))).len();
        assert!(banned_event == 1, "Expect one banned event");
        assert_matches!(
            banned_events.first().unwrap(),
            PeerEvent::Banned(id, _) if *id == peer_id
        );

        // assert peer is still banned after disconnection
        assert!(
            peer_manager.connection_banned(&peer_id),
            "Peer should remain banned after disconnection"
        );
    }

    // test temp ban from DisconnectWithPX
}
