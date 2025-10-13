//! Manage peer connection status and reputation.

use super::{
    all_peers::AllPeers,
    cache::BannedPeerCache,
    score::init_peer_score_config,
    status::NewConnectionStatus,
    types::{ConnectionDirection, ConnectionType, DialRequest, PeerAction},
    PeerEvent, PeerExchangeMap, Penalty,
};
use crate::{
    error::NetworkError,
    peers::status::ConnectionStatus,
    send_or_log_error,
    types::{NetworkInfo, NetworkResult},
};
use libp2p::{core::ConnectedPoint, kad::PeerInfo, multiaddr::Protocol, Multiaddr, PeerId};
use rand::seq::IteratorRandom as _;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::IpAddr,
    task::Context,
};
use tn_config::PeerConfig;
use tn_types::{now, BlsPublicKey};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

#[cfg(test)]
#[path = "../tests/peer_manager.rs"]
mod peer_manager;

/// The type to manage peers.
pub(crate) struct PeerManager {
    /// Config
    config: PeerConfig,
    /// The interval to perform maintenance.
    heartbeat: tokio::time::Interval,
    /// All peers for the manager.
    peers: AllPeers,
    /// The collection of bls public keys to known peers.
    /// This should incude the current and next couple of committee members network info.
    /// This is used for bootstrapping and to make sure we know the network settings of committee
    /// members.
    known_peers: HashMap<BlsPublicKey, NetworkInfo>,
    /// PeerId -> BlsPublicKey for know peers.
    known_peerids: HashMap<PeerId, BlsPublicKey>,
    /// A queue of events that the `PeerManager` is waiting to produce.
    events: VecDeque<PeerEvent>,
    /// A queue of peers to dial.
    dial_requests: VecDeque<DialRequest>,
    /// Tracks temporarily banned peers to prevent immediate reconnection attempts.
    ///
    /// This LRU cache manages a time-based ban list that operates independently
    /// from the peer's state. Characteristics:
    ///
    /// - Prevents reconnection attempts at the network layer without affecting the peer's stored
    ///   state
    /// - Peers appear to be banned for connection purposes while still having a non-banned state
    ///   in the database
    /// - Ban records persist even after a peer is removed from the database, allowing rejection of
    ///   unknown peers based on previous temporary bans
    /// - Control the time-based LRU cache mechanism by leveraging the PeerManager's heartbeat
    ///   cycle for maintenance instead of requiring separate polling
    /// - The actual ban duration has a resolution limited by the heartbeat interval, as cache
    ///   cleanup occurs during heartbeat events
    ///
    /// The implementation uses `FnvHashSet` instead of the default Rust hasher `SipHash`
    /// for improved performance for short keys.
    temporarily_banned: BannedPeerCache<PeerId>,
    /// Potential peers discovered through kad.
    ///
    /// These peers are not connected and reserved for dial attempts at heartbeat intervals if
    /// connections drop.
    discovery_peers: HashMap<PeerId, Vec<Multiaddr>>,
}

impl PeerManager {
    /// Create a new instance of Self.
    pub(crate) fn new(config: &PeerConfig) -> Self {
        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(config.heartbeat_interval));

        let peers = AllPeers::new(
            config.dial_timeout,
            config.max_banned_peers,
            config.max_disconnected_peers,
        );
        let temporarily_banned = BannedPeerCache::new(config.excess_peers_reconnection_timeout);

        // initialize global score config
        init_peer_score_config(config.score_config);

        Self {
            config: *config,
            heartbeat,
            peers,
            known_peers: Default::default(),
            known_peerids: Default::default(),
            events: Default::default(),
            dial_requests: Default::default(),
            temporarily_banned,
            discovery_peers: Default::default(),
        }
    }

    /// Explicitly add a "trusted" peer and dial it.
    ///
    /// These peers are considered "trusted" and do not receive penalties.
    /// This does not unban ips and should only be called during initialization.
    pub(crate) fn add_trusted_peer_and_dial(
        &mut self,
        bls_key: BlsPublicKey,
        info: NetworkInfo,
        reply: oneshot::Sender<NetworkResult<()>>,
    ) {
        let peer_id: PeerId = info.pubkey.clone().into();
        let multiaddr = info.multiaddrs.clone();
        self.peers.add_trusted_peer(bls_key, info.pubkey.clone(), multiaddr.clone());

        // remove from temporary banned and warn if peer was banned
        if self.temporarily_banned.remove(&peer_id) {
            warn!(target: "peer-manager", ?peer_id, "removed trusted peer from temporarily banned list");
        }
        let peer_id: PeerId = info.pubkey.clone().into();
        self.known_peerids.insert(peer_id, bls_key);
        self.known_peers.insert(bls_key, info);

        self.dial_peer(peer_id, multiaddr, Some(reply));
    }

    /// Process the request to dial a peer.
    pub(crate) fn dial_peer(
        &mut self,
        peer_id: PeerId,
        multiaddrs: Vec<Multiaddr>,
        reply: Option<oneshot::Sender<NetworkResult<()>>>,
    ) {
        // return early if peer is banned, connected, or currently being dialed
        if let Some(peer) = self.peers.get_peer(&peer_id) {
            match peer.connection_status() {
                ConnectionStatus::Banned { .. } => {
                    // report error - dial banned peer
                    let error = NetworkError::DialBannedPeer(format!("Peer {peer_id} is banned"));
                    warn!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Peer Banned",
                            peer = peer_id
                        );
                    }
                    return;
                }
                ConnectionStatus::Dialing { .. } => {
                    // report error - dialing already in progress
                    let error = NetworkError::AlreadyDialing(format!("Already dialing {peer_id}"));
                    debug!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Already dialing",
                            peer = peer_id
                        );
                    }
                    return;
                }
                ConnectionStatus::Connected { .. } => {
                    // report error - dialing already connected
                    let error =
                        NetworkError::AlreadyConnected(format!("Already connected {peer_id}"));
                    debug!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Already connected",
                            peer = peer_id
                        );
                    }
                    return;
                }
                _ => { /* ignore */ }
            }
        }
        // schedule swarm to dial peer
        debug!(target: "peer-manager", ?peer_id, "sending dial request to swarm");
        let request = DialRequest { peer_id, multiaddrs, reply };
        self.dial_requests.push_back(request);
    }

    /// Check if this peer is already registered as dialing.
    ///
    /// Self and kad behaviors can initiate dial attempts. This is used to filter pending outbound
    /// connections.
    pub(super) fn dial_attempt_already_registered(&self, peer_id: &PeerId) -> bool {
        self.peers.get_peer(peer_id).is_some_and(|peer| peer.connection_status().is_dialing())
    }

    /// Push a [PeerEvent].
    pub(super) fn push_event(&mut self, event: PeerEvent) {
        self.events.push_back(event);
    }

    /// Register a dial attempt to return the result to caller.
    ///
    /// This method initializes the peer and sets the connection to `Dialing`.
    /// If a dial attempt was already registered, the reply channel is updated.
    pub(super) fn register_dial_attempt(
        &mut self,
        peer_id: PeerId,
        reply: Option<oneshot::Sender<NetworkResult<()>>>,
    ) {
        self.peers.register_dial_attempt(peer_id, reply);
    }

    /// Return the next dial request if it exists.
    pub(super) fn next_dial_request(&mut self) -> Option<DialRequest> {
        self.dial_requests.pop_front()
    }

    /// Notify the caller that a dial attempt was successful.
    pub(super) fn notify_dial_result(&mut self, peer_id: &PeerId, result: NetworkResult<()>) {
        self.peers.notify_dial_result(peer_id, result);
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

    /// Heartbeat maintenance.
    ///
    /// The manager runs routine maintenance to decay penalties for peers. This method
    /// is routine and can not further penalize peers.
    pub(super) fn heartbeat(&mut self) {
        // update peers
        let actions = self.peers.heartbeat_maintenance();
        for (peer_id, action) in actions {
            self.apply_peer_action(peer_id, action);
        }

        // TODO: Issue #254 update metrics

        // enforce connection limits
        self.prune_connected_peers();

        // update timestamps
        self.unban_temp_banned_peers();

        // manage discovery peers
        self.discovery_heartbeat();
    }

    /// Apply a [PeerAction].
    ///
    /// Actions on peers happen when their reputation or connection status changes.
    fn apply_peer_action(&mut self, peer_id: PeerId, action: PeerAction) {
        match action {
            PeerAction::Ban(ip_addrs) => {
                debug!(target: "peer-manager", ?peer_id, ?ip_addrs, "reputation update results in ban");
                self.process_ban(&peer_id);
            }
            PeerAction::Disconnect => {
                debug!(target: "peer-manager", ?peer_id, "reputation update results in disconnect");
                self.temporarily_banned.insert(peer_id);
                self.push_event(PeerEvent::DisconnectPeer(peer_id));
            }
            PeerAction::DisconnectWithPX => {
                debug!(target: "peer-manager", ?peer_id, "reputation update results in temp ban with PX");
                // prevent immediate reconnection attempts
                self.temporarily_banned.insert(peer_id);
                let exchange = self.peers.peer_exchange();
                self.events.push_back(PeerEvent::DisconnectPeerX(peer_id, exchange));
            }
            PeerAction::Unban(ip_addrs) => {
                debug!(target: "peer-manager", ?peer_id, ?ip_addrs, "reputation update results in unban");
                self.push_event(PeerEvent::Unbanned(peer_id));
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
    ///
    /// `AllPeers` only tracks CVVs for now. (current voting validators)
    ///
    /// This method will be extended to support any staked validator.
    pub(super) fn is_peer_validator(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_validator(peer_id)
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
    pub(crate) fn peer_banned(&self, peer_id: &PeerId) -> bool {
        debug!(
            target: "peer-manager",
            ?peer_id,
            "checking if peer banned...current banned peers:\n{:?}",
            self.temporarily_banned
        );
        self.temporarily_banned.contains(peer_id) || self.peers.peer_banned(peer_id)
    }

    /// Process new connection and return boolean indicating if the peer limit was reached.
    pub(super) fn peer_limit_reached(&self, endpoint: &ConnectedPoint) -> bool {
        if endpoint.is_dialer() {
            // this node dialed peer
            self.peers.connected_peer_ids().count() >= self.config.max_outbound_dialing_peers()
        } else {
            // peer dialed this node
            self.connected_or_dialing_peers().len() >= self.config.max_peers()
        }
    }

    /// Return an iterator of peers that are connected or dialed.
    pub(crate) fn connected_or_dialing_peers(&self) -> Vec<PeerId> {
        trace!(target: "peer-manager", "all peers:\n{:?}", self.peers);
        self.peers.connected_or_dialing_peers()
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
    fn process_ban(&mut self, peer_id: &PeerId) {
        // ensure unbanned events are removed for this peer
        self.events.retain(|event| {
            if let PeerEvent::Unbanned(unbanned_peer_id) = event {
                unbanned_peer_id != peer_id
            } else {
                true
            }
        });

        // push banned event
        self.events.push_back(PeerEvent::Banned(*peer_id));
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
    pub(crate) fn disconnect_peer(&mut self, peer_id: PeerId, support_discovery: bool) {
        // include peer exchange or not
        let event = if support_discovery {
            let exchange = self.peers.peer_exchange();
            PeerEvent::DisconnectPeerX(peer_id, exchange)
        } else {
            PeerEvent::DisconnectPeer(peer_id)
        };

        self.events.push_back(event);
        let action = self.peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { banned: false },
        );

        debug!(target: "peer-manager", ?action, "disconnect peer results in:");
        self.apply_peer_action(peer_id, action);
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
    /// This method registers the peer as disconnected and ensures the list of banned/disconnected
    /// peers doesn't grow infinitely large. Peers may become "unbanned" if the limit for banned
    /// peers is reached.
    pub(super) fn register_disconnected(&mut self, peer_id: &PeerId) {
        let (action, pruned_peers) = self.peers.register_disconnected(peer_id);

        debug!(target: "peer-manager", ?action, ?pruned_peers, ?peer_id, "register disconnected");

        // banning is the only action that happens after disconnect
        // if the peer is banned then manager needs to apply the ban still
        // otherwise, there is no other action to take
        if action.is_ban() {
            debug!(target: "peer-manager", ?peer_id, "processing ban");
            self.apply_peer_action(*peer_id, action);
        }

        // process pruned peers
        self.events
            .extend(pruned_peers.into_iter().map(|(peer_id, _)| PeerEvent::Unbanned(peer_id)));
    }

    /// Prune peers to reach target peer counts.
    ///
    /// Trusted peers and validators are ignored. Peers are sorted from lowest to highest score and
    /// removed until excess peer count reaches target.
    fn prune_connected_peers(&mut self) {
        // connected peers sorted from lowest to highest aggregate score
        // peers that do not participate in the kad routing table are prioritized for disconnect
        let connected_peers = self.peers.connected_peers_by_score_and_routability();
        let mut excess_peer_count =
            connected_peers.len().saturating_sub(self.config.target_num_peers);
        if excess_peer_count == 0 {
            // no excess peers
            return;
        }

        // filter peers that are validators
        let ready_to_prune = connected_peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                if !self.is_peer_validator(peer_id) && !peer.is_trusted() {
                    Some(**peer_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // disconnect peers until excess_peer_count is 0 or no more peers
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
            self.push_event(PeerEvent::Unbanned(peer_id));
        }
    }

    /// Process peer exchange for peer discovery.
    ///
    /// This method is called when a peer disconnects immediately from this node due to having too
    /// many peers. The disconnecting peer shares information about other known peers to
    /// facilitate discovery.
    ///
    /// Peers should be weary of these reported peers (eclipse attacks).
    pub(crate) fn process_peer_exchange(&mut self, peers: PeerExchangeMap) {
        for (bls, (net_key, multiaddrs)) in peers.into_iter() {
            let multiaddrs: Vec<Multiaddr> = multiaddrs.into_iter().collect();
            let peer_id: PeerId = net_key.clone().into();
            if !self.known_peers.contains_key(&bls) {
                self.add_known_peer(
                    bls,
                    NetworkInfo {
                        pubkey: net_key.clone(),
                        multiaddrs: multiaddrs.clone(),
                        timestamp: now(),
                    },
                );
            }

            // dial peers that are unknown or can be dialed
            if self.peers.can_dial(&peer_id) {
                self.dial_peer(peer_id, multiaddrs, None);
            }
        }
    }

    /// Create [PeerExchangeMap] for exchange with peers.
    pub(crate) fn peers_for_exchange(&self) -> PeerExchangeMap {
        self.peers.peer_exchange()
    }

    /// Return the score for a peer if they exist.
    pub(crate) fn peer_score(&self, peer_id: &PeerId) -> Option<f64> {
        self.peers.get_peer(peer_id).map(|peer| peer.score().aggregate_score())
    }

    /// Bool indicating if the peer is trusted or a validator.
    pub(crate) fn peer_is_important(&self, peer_id: &PeerId) -> bool {
        self.is_peer_validator(peer_id)
            || self.peers.get_peer(peer_id).map(|p| p.is_trusted()).unwrap_or_default()
    }

    /// Update the committee for the new epoch.
    pub(crate) fn new_epoch(&mut self, committee: HashSet<BlsPublicKey>) {
        // remove from temporary banned and warn if validator was banned
        let mut exp_committee = Vec::default();
        for bls_key in &committee {
            if let Some(NetworkInfo { pubkey, multiaddrs: multiaddr, timestamp }) =
                self.known_peers.get(bls_key)
            {
                let peer_id: PeerId = pubkey.clone().into();
                info!(target: "peer-manager", "adding committee member {bls_key}/{peer_id}");
                if self.temporarily_banned.remove(&peer_id) {
                    warn!(target: "peer-manager", ?peer_id, "removed committee member from temporarily banned list");
                }
                exp_committee.push((
                    *bls_key,
                    NetworkInfo {
                        pubkey: pubkey.clone(),
                        multiaddrs: multiaddr.clone(),
                        timestamp: *timestamp,
                    },
                ));
            } else {
                warn!(target: "peer-manager", "unknown committee member with key {bls_key}");
            }
        }

        // add trusted peer record
        let unban_actions = self.peers.new_epoch(exp_committee);

        // apply unban for any banned validators
        for (peer_id, action) in unban_actions {
            self.apply_peer_action(peer_id, action);
        }
    }

    /// Add a known peer to the known list.
    /// Used for bootstrap servers or possibly committee members.
    pub(crate) fn add_known_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        self.peers.upsert_peer(bls_key, info.pubkey.clone(), info.multiaddrs.clone());
        self.known_peers.insert(bls_key, info.clone());
        let peer_id: PeerId = info.pubkey.into();
        self.known_peerids.insert(peer_id, bls_key);
    }

    /// Find authorities for the epoch manager.
    pub(crate) fn find_authorities(&mut self, authorities: Vec<BlsPublicKey>) {
        let mut missing = Vec::new();

        // check all peers for authority and track missing
        for bls_key in authorities {
            // identify missing authorities
            if !self.known_peers.contains(&bls_key) {
                missing.push(bls_key);
            }
        }

        // emit event for kad to try to discover
        trace!(target: "peer-manager", ?missing, "requesting kad records");
        self.events.push_back(PeerEvent::MissingAuthorities(missing));
    }

    /// Find the peer id for an authority.
    pub(crate) fn auth_to_peer(&self, bls_key: BlsPublicKey) -> Option<(PeerId, Vec<Multiaddr>)> {
        if let Some(NetworkInfo { pubkey, multiaddrs, .. }) = self.known_peers.get(&bls_key) {
            Some((pubkey.clone().into(), multiaddrs.clone()))
        } else {
            None
        }
    }

    /// Find the BlsPublicKey for a known PeerId.
    pub(crate) fn peer_to_bls(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.known_peerids.get(peer_id).copied()
    }

    /// Visibility helper for behavior to evaluate pending outbound connection attempts.
    pub(super) fn can_dial(&self, peer_id: &PeerId) -> bool {
        self.peers.can_dial(peer_id) && !self.temporarily_banned.contains(peer_id)
    }

    /// Extract IP addresses from multiaddrs and check if any are banned.
    ///
    /// Returns `true` if the peer has valid IP addresses and NONE are banned.
    /// Returns `false` if no valid IPs found OR any IP is banned.
    pub(super) fn has_valid_unbanned_ips(&self, multiaddrs: &[Multiaddr]) -> bool {
        let mut found_valid_ip = false;

        for addr in multiaddrs {
            if let Some(ip) = Self::extract_ip_from_multiaddr(addr) {
                found_valid_ip = true;
                if self.is_ip_banned(&ip) {
                    return false; // Early return on first banned IP
                }
            }
        }

        found_valid_ip
    }

    /// Extract IP address from a single multiaddr.
    ///
    /// Only supports IPv4 and IPv6.
    fn extract_ip_from_multiaddr(addr: &Multiaddr) -> Option<IpAddr> {
        addr.iter().find_map(|protocol| match protocol {
            Protocol::Ip4(ip) => Some(IpAddr::V4(ip)),
            Protocol::Ip6(ip) => Some(IpAddr::V6(ip)),
            _ => None,
        })
    }

    /// Check if peer is eligible for discovery.
    ///
    /// A peer is eligible if:
    /// - it has at least one valid ip address (ipv4/ipv6)
    /// - none of its ip addresses are banned
    /// - it can be dialed (not connected/dialing/banned)
    fn eligible_for_discovery(&self, info: &PeerInfo) -> bool {
        self.has_valid_unbanned_ips(&info.addrs) && self.peers.can_dial(&info.peer_id)
    }

    /// Process newly discovered peers for potential dial attempts.
    ///
    /// Only eligible peers are stored for dialing during heartbeat.
    pub(crate) fn process_peers_for_discovery(&mut self, mut peers: Vec<PeerInfo>) {
        peers.retain(|peer| self.eligible_for_discovery(peer));
        let peers: HashSet<_> = peers.into_iter().map(|info| (info.peer_id, info.addrs)).collect();
        trace!(target: "peer-manager", ?peers, "adding eligible peers to discovery map");
        self.discovery_peers.extend(peers);
    }

    /// Check peer counts and initiate dial attempts to maintain connection targets.
    fn discovery_heartbeat(&mut self) {
        // take discovery peers and filter ineligble peers
        let mut discovery_peers = std::mem::take(&mut self.discovery_peers);
        discovery_peers.retain(|peer_id, addrs| {
            let peer_info = PeerInfo { peer_id: *peer_id, addrs: addrs.clone() };
            self.eligible_for_discovery(&peer_info)
        });

        // calculate dial attempts needed for target connection limits
        let connected_or_dialing = self.connected_or_dialing_peers().len();
        let peers_needed = self.config.target_num_peers.saturating_sub(connected_or_dialing);

        // used for random selections
        let mut rng = rand::rng();

        // initiate dial attempts
        if peers_needed > 0 {
            // randomly select peers to dial
            let to_dial: Vec<(PeerId, Vec<Multiaddr>)> = discovery_peers
                .iter()
                .map(|(id, addrs)| (*id, addrs.clone()))
                .choose_multiple(&mut rng, peers_needed);

            // remove from discovery and dial discovery candidate
            for (peer, addrs) in to_dial {
                debug!(target: "peer-manager", ?peer, "dialing peer for discovery");
                discovery_peers.remove(&peer);
                self.dial_peer(peer, addrs, None);
            }
        }

        // manage target discovery peer counts
        let max_discovery_peers = self.config.max_discovery_peers();
        let current_count = discovery_peers.len();
        if current_count > max_discovery_peers {
            debug!(target: "peer-manager", "pruning discovery peers");
            // prune excess
            let excess = current_count - max_discovery_peers;
            let to_remove: Vec<PeerId> =
                discovery_peers.keys().copied().choose_multiple(&mut rng, excess);
            for peer in to_remove {
                discovery_peers.remove(&peer);
            }

            debug!(
                target: "peer-manager",
                pruned = excess,
                remaining = discovery_peers.len(),
                "pruned excess discovery peers"
            );
        } else if current_count < max_discovery_peers {
            // emit discovery event to find closest peers
            debug!(target: "peer-manager", "discovery peers low");
            self.events.push_back(PeerEvent::Discovery);
        }

        // store discovery peers
        self.discovery_peers = discovery_peers;
    }

    /// Update a peer's status in the routing table.
    pub(crate) fn update_routing_for_peer(&mut self, peer_id: &PeerId, routable: bool) {
        self.peers.update_routing_for_peer(peer_id, routable);
    }
}
