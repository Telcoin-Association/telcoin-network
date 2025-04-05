//! Manage peer connection status and reputation.

use super::{
    cache::BannedPeerCache,
    init_peer_score_config,
    score::Penalty,
    types::{ConnectionType, DialRequest},
    AllPeers, ConnectionDirection, NewConnectionStatus, PeerAction, PeerExchangeMap,
};
use crate::{
    error::NetworkError, peers::status::ConnectionStatus, send_or_log_error, types::NetworkResult,
};
use libp2p::{core::ConnectedPoint, Multiaddr, PeerId};
use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    task::Context,
};
use tn_config::{ConsensusConfig, PeerConfig};
use tn_types::Database;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

#[cfg(test)]
#[path = "../tests/peer_manager.rs"]
mod peer_manager;

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
    /// A queue of peers to dial.
    dial_requests: VecDeque<DialRequest>,
    /// The collection of pending dials.
    pending_dials: HashMap<PeerId, oneshot::Sender<NetworkResult<()>>>,
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
    /// Connected with peer.
    PeerConnected(PeerId, Multiaddr),
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
        let temporarily_banned = BannedPeerCache::new(config.excess_peers_reconnection_timeout);

        // initialize global score config
        init_peer_score_config(config.score_config);

        Self {
            config: *config,
            heartbeat,
            peers,
            events: Default::default(),
            dial_requests: Default::default(),
            pending_dials: Default::default(),
            temporarily_banned,
        }
    }

    /// Process the request to dial a peer.
    pub(crate) fn dial_peer(
        &mut self,
        peer_id: PeerId,
        multiaddr: Multiaddr,
        reply: oneshot::Sender<NetworkResult<()>>,
    ) {
        // return early if peer is banned, connected, or currently being dialed
        if let Some(peer) = self.peers.get_peer(&peer_id) {
            match peer.connection_status() {
                ConnectionStatus::Banned { .. } => {
                    // report error - dial banned peer
                    let error = NetworkError::Dial(format!("Peer {} is banned", peer_id));
                    warn!(target: "peer-manager", ?error, "invalid dial request");
                    send_or_log_error!(reply, Err(error), "DialPeer", peer = peer_id);
                    return;
                }
                ConnectionStatus::Dialing { .. } => {
                    // report error - dialing already in progress
                    let error = NetworkError::Dial(format!("Already dialing {}", peer_id));
                    warn!(target: "peer-manager", ?error, "invalid dial request");
                    send_or_log_error!(reply, Err(error), "DialPeer", peer = peer_id);
                    return;
                }
                ConnectionStatus::Connected { .. } => {
                    // report error - dialing already connected
                    let error =
                        NetworkError::Dial(format!("Peer {} is already connected", peer_id));
                    warn!(target: "peer-manager", ?error, "invalid dial request");
                    send_or_log_error!(reply, Err(error), "DialPeer", peer = peer_id);
                    return;
                }
                _ => { /* ignore */ }
            }
        }
        // schedule swarm to dial peer
        debug!(target: "peer-manager", ?peer_id, "sending dial request to swarm");
        let request = DialRequest { peer_id, multiaddrs: vec![multiaddr], reply };
        self.dial_requests.push_back(request);
    }

    /// Push a [PeerEvent].
    pub(super) fn push_event(&mut self, event: PeerEvent) {
        self.events.push_back(event);
    }

    /// Register a dial attempt to return the result to caller.
    ///
    /// This method initializes the peer and sets the connection to `Dialing`.
    pub(super) fn register_dial_attempt(
        &mut self,
        peer_id: PeerId,
        reply: oneshot::Sender<NetworkResult<()>>,
    ) {
        // create the peer if it doesn't exist and register as dialing
        self.peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);
        self.pending_dials.insert(peer_id, reply);
    }

    /// Return the next dial request if it exists.
    pub(super) fn next_dial_request(&mut self) -> Option<DialRequest> {
        self.dial_requests.pop_front()
    }

    /// Return the oneshot sender for dial attempt if it exists.
    pub(super) fn reply_for_dial_attempt(
        &mut self,
        peer_id: &PeerId,
    ) -> Option<oneshot::Sender<NetworkResult<()>>> {
        self.pending_dials.remove(peer_id)
    }

    /// Notify the caller that a dial attempt was successful.
    pub(super) fn notify_dial_result(&mut self, peer_id: &PeerId, result: NetworkResult<()>) {
        // return result to caller
        if let Some(reply) = self.reply_for_dial_attempt(&peer_id) {
            send_or_log_error!(reply, result, "DialResult", peer = peer_id);
        }
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
            self.connected_or_dialing_peers() >= self.config.max_peers()
        }
    }

    /// Return an iterator of peers that are connected or dialed.
    pub fn connected_or_dialing_peers(&self) -> usize {
        self.peers.connected_or_dialing_peers().count()
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

    /// Register a connected peer if their reputation is sufficient.
    ///
    /// Returns a boolean if the peer was successfully registered. This is the initial
    /// method to call for registering a new peer through dialing or incoming connections.
    pub(super) fn register_peer_connection(
        &mut self,
        peer_id: &PeerId,
        connection: ConnectionType,
    ) -> bool {
        if self.peers.peer_banned(peer_id) {
            // log error if the peer is banned
            error!(target: "peer-manager", ?peer_id, "connected with banned peer");
            return false;
        }

        match connection {
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
                // this node dials for outgoing connections
                self.notify_dial_result(peer_id, Ok(()));
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
    ///
    /// This method is called when a peer disconnects immediately from this node due to having too many peers.
    /// The disconnecting peer shares information about other known peers to facilitate discovery.
    pub(crate) fn process_peer_exchange(&mut self, peers: PeerExchangeMap) {
        // // skip processing if there's already enough peers
        // if self.connected_or_dialing_peers() >= self.config.target_num_peers {
        //     debug!(target: "peer-manager", "Skipping peer exchange processing, already at target peer count");
        //     return;
        // }

        // let peers_to_dial = peers.into_iter().map(|| {

        // });

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
