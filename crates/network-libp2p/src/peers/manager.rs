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
    metrics::PeerManagerMetrics,
    peers::status::ConnectionStatus,
    send_or_log_error,
    types::{NetworkInfo, NetworkResult, RpcInfo},
};
use libp2p::{core::ConnectedPoint, kad::PeerInfo, multiaddr::Protocol, Multiaddr, PeerId};
use rand::seq::{IteratorRandom as _, SliceRandom as _};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::IpAddr,
    task::Context,
};
use tn_config::PeerConfig;
use tn_types::BlsPublicKey;
use tokio::sync::oneshot;
use tracing::{debug, error, trace, warn};

#[cfg(test)]
#[path = "../tests/peer_manager.rs"]
mod peer_manager;

/// The type to manage peers.
pub(crate) struct PeerManager {
    /// This swarm's own peer id.
    ///
    /// Used to recognise and ignore the node's own identity on the dial,
    /// discovery, connection, and penalty paths so a self-connection (e.g. a
    /// learned hairpin address routed back to our own id) is never dialed or
    /// scored. Primary and each worker run separate swarms with distinct
    /// keypairs, so each `PeerManager` holds exactly one local id.
    local_peer_id: PeerId,
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
    /// BLS keys whose `known_peers` entry is pinned and never evicted by committee rotation.
    ///
    /// Populated only by the operator-provisioned insertion paths — trusted/bootstrap/explicit
    /// peers — whose count is bounded by node configuration. Records restored from persistence
    /// at startup are NOT pinned: the persisted kad store holds peer-fillable third-party records
    /// (stored as the node's DHT storage duty), so restored entries stay subject to
    /// committee-rotation pruning. Every other `known_peers` entry is a kad-discovered committee
    /// record kept only while its key sits in a tracked committee slot, so
    /// [`Self::prune_known_peers`] can drop rotated-out members without touching
    /// operator-provisioned peers.
    pinned_peers: HashSet<BlsPublicKey>,
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
    /// Prometheus metrics for peer lifecycle events.
    pub(super) metrics: PeerManagerMetrics,
}

impl PeerManager {
    /// Create a new instance of Self.
    pub(crate) fn new(
        local_peer_id: PeerId,
        config: &PeerConfig,
        metrics: PeerManagerMetrics,
    ) -> Self {
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
            local_peer_id,
            config: *config,
            heartbeat,
            peers,
            known_peers: Default::default(),
            pinned_peers: Default::default(),
            events: Default::default(),
            dial_requests: Default::default(),
            temporarily_banned,
            discovery_peers: Default::default(),
            metrics,
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
        self.peers.add_trusted_peer(bls_key, info.pubkey.clone());

        // remove from temporary banned and warn if peer was banned
        if self.temporarily_banned.remove(&peer_id) {
            warn!(target: "peer-manager", ?peer_id, "removed trusted peer from temporarily banned list");
        }
        // trusted peers are operator-provisioned; pin so committee rotation never evicts them
        self.pinned_peers.insert(bls_key);
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
        // never dial our own identity (e.g. a self entry that reached the
        // discovery/dial path via a learned hairpin address). Report success so
        // retrying callers (`dial_peer_bls`) treat it as a no-op, not a failure
        // to back off on.
        if self.is_local_peer(&peer_id) {
            debug!(target: "peer-manager", ?peer_id, "skipping dial request for local peer id");
            if let Some(reply) = reply {
                send_or_log_error!(reply, Ok(()), "DialPeer- Self", peer = peer_id);
            }
            return;
        }
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

        // Emit peer metrics via tracing for OpenTelemetry export.
        let connected_count = self.peers.connected_peer_ids().count();
        let connected_or_dialing = self.connected_or_dialing_peers().len();
        let banned_count = self.temporarily_banned.len();
        tracing::info!(
            target: "tn::network",
            connected_count,
            connected_or_dialing,
            banned_count,
            "peer metrics heartbeat"
        );
        self.metrics.set_peer_counts(
            connected_count,
            self.known_peers.len(),
            self.discovery_peers.len(),
            banned_count,
        );

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
                self.temporarily_banned.insert(peer_id);
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
    /// Membership spans the previous, current, and next committees tracked by `AllPeers`, so peers
    /// from the just-completed epoch and the upcoming epoch both count. (NVV support remains future
    /// work.)
    pub(super) fn is_peer_validator(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_validator(peer_id)
    }

    /// Returns a boolean if the peer is connected.
    pub(super) fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.peers.get_peer(peer_id).is_some_and(|peer| {
            matches!(peer.connection_status(), ConnectionStatus::Connected { .. })
        })
    }

    /// Whether `peer_id` is this swarm's own identity.
    ///
    /// A self-connection (the node dialing its own peer id, e.g. via a learned
    /// hairpin address) is not a real peer: it must never be dialed, registered
    /// for discovery, accepted as a connection, or penalized.
    pub(super) fn is_local_peer(&self, peer_id: &PeerId) -> bool {
        &self.local_peer_id == peer_id
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
        debug!(target: "peer-manager", connected_peers=?self.peers.connected_peer_ids().count(), "checking peer limits");
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
        // Never penalize our own identity. A self-connection (e.g. a learned
        // hairpin address routed back to our own peer id) must not feed the
        // score model and ban the node's own worker. Guard before recording the
        // metric so a self-event does not pollute penalty counts.
        if self.is_local_peer(&peer_id) {
            debug!(target: "peer-manager", ?peer_id, "skipping penalty for local peer id");
            return;
        }
        self.metrics.record_penalty(&penalty);
        let action = self.peers.process_penalty(&peer_id, penalty);

        debug!(target: "peer-manager", ?peer_id, ?action, "processed penalty");
        self.apply_peer_action(peer_id, action);
    }

    /// Process newly banned IP addresses.
    ///
    /// The peer is disconnected and is banned from network layer.
    fn process_ban(&mut self, peer_id: &PeerId) {
        self.metrics.record_peer_banned();
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
                if !self.is_peer_validator(peer_id) && !peer.is_operator_allowlisted() {
                    Some(*peer_id)
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
    /// Peers should be wary of these reported peers (eclipse attacks). Peers discovered through
    /// kademlia are prioritized over peer exchange by only processing up to the missing target
    /// number of discovery peers from exchange map.
    pub(crate) fn process_peer_exchange(&mut self, peers: PeerExchangeMap) {
        // check if discovery peers needed
        let max_discovery_peers = self.config.max_discovery_peers();
        let current_count = self.discovery_peers.len();

        // seed discovery peers from peer exchange
        if current_count < max_discovery_peers {
            // convert eligible peers to `PeerInfo` for processing
            let mut peers: Vec<_> = peers
                .into_iter()
                .filter_map(|(_, (net_key, addrs))| {
                    let info =
                        PeerInfo { peer_id: net_key.into(), addrs: addrs.into_iter().collect() };

                    // filter out ineligible peers
                    if self.eligible_for_discovery(&info) {
                        debug!(target: "peer-manager", ?info, "peer exchange eligible");
                        Some(info)
                    } else {
                        debug!(target: "peer-manager", peer=?self.peers.get_peer(&info.peer_id), ?info, "peer exchange ineligible");
                        None
                    }
                })
                .collect();

            debug!(target: "peer-manager", eligible=?peers, "processing peer exchange");

            // shuffle all peers
            let mut rng = rand::rng();
            peers.shuffle(&mut rng);

            // add target number of peers for discovery
            let peers_to_take = max_discovery_peers - current_count;
            for peer in peers.into_iter().take(peers_to_take) {
                debug!(target: "peer-manager", peer=?peer.peer_id, "added peer to discovery peers");
                self.discovery_peers.insert(peer.peer_id, peer.addrs);
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

    /// Bool indicating if the peer is operator-allowlisted or a validator.
    pub(crate) fn peer_is_important(&self, peer_id: &PeerId) -> bool {
        self.is_peer_validator(peer_id)
            || self.peers.get_peer(peer_id).map(|p| p.is_operator_allowlisted()).unwrap_or_default()
    }

    /// Set the previous/current/next committees directly from authoritative state, every epoch.
    ///
    /// The three committees are handed to the peer store keyed by [`BlsPublicKey`], which
    /// overwrites the three slots with the complete sets, demotes any member that fell out of
    /// the window, records every member as a validator (even those whose network identity is
    /// not yet known), and forgives any bans for members already discovered. Unknown keys in
    /// `current` and `next` trigger a kad lookup (see [`Self::trigger_missing_authorities`]) —
    /// but **not** `previous`, whose peers are rotating out. The `next` committee is the most
    /// likely source of peers we have never connected to, but `current` members may also be
    /// unknown when a restart seeds the committees late (the initial epoch returned early
    /// before network setup), so chase both.
    ///
    /// Members are also lifted out of the manager's temporary-ban cache so a follow-up dial loop
    /// can reach them; members discovered only later are forgiven lazily by
    /// [`Self::add_known_peer`].
    pub(crate) fn update_committees(
        &mut self,
        previous: HashSet<BlsPublicKey>,
        current: HashSet<BlsPublicKey>,
        next: HashSet<BlsPublicKey>,
    ) {
        self.trigger_missing_authorities(&current);
        self.trigger_missing_authorities(&next);
        // forgive manager-level temporary bans for any already-known members across all three slots
        self.forgive_temporarily_banned(&previous);
        self.forgive_temporarily_banned(&current);
        self.forgive_temporarily_banned(&next);
        let unban_actions = self.peers.update_committees(previous, current, next);
        self.apply_unban_actions(unban_actions);
        // bound `known_peers`: with the new committee slots in place, drop every entry that is
        // neither pinned nor a current committee member so rotated-out members and stale discovered
        // records cannot accumulate across epochs (issue #827).
        self.prune_known_peers();
    }

    /// Pre-dial recovery: forgive bans for a committee so a subsequent dial loop can connect,
    /// WITHOUT mutating the committee slots.
    ///
    /// Used by the deadlock-breaker path while waiting for an epoch record; the real slot update
    /// follows shortly after via [`Self::update_committees`].
    pub(crate) fn prepare_committee_dial(&mut self, committee: HashSet<BlsPublicKey>) {
        self.forgive_temporarily_banned(&committee);
        let unban_actions = self.peers.mark_committee_for_dial(committee);
        self.apply_unban_actions(unban_actions);
    }

    /// Drop cached network info for peers no longer worth tracking, bounding `known_peers`.
    ///
    /// Run after every committee rotation. An entry survives only if it is pinned (an
    /// operator-provisioned trusted/bootstrap/explicit peer) or its key still sits in a tracked
    /// committee slot, so members that rotated out — and any stale kad-discovered records — cannot
    /// accumulate across epochs. `known_peers` is thereby bounded by the operator-configured set
    /// plus the three committee slots. Evicting a still-relevant peer only forces a fresh kad
    /// lookup if it becomes a committee member again, which is the intended discovery flow.
    fn prune_known_peers(&mut self) {
        let pinned = &self.pinned_peers;
        let peers = &self.peers;
        self.known_peers
            .retain(|bls_key, _| pinned.contains(bls_key) || peers.is_committee_member(bls_key));
    }

    /// Lift any already-known committee members out of the manager's temporary-ban cache.
    ///
    /// The temporary-ban cache is a network-layer reconnection guard kept separate from peer state;
    /// committee members must bypass it so they can be (re)dialed. Members with no known network
    /// info yet are skipped here and forgiven lazily once discovered (see
    /// [`Self::add_known_peer`]).
    fn forgive_temporarily_banned(&mut self, committee: &HashSet<BlsPublicKey>) {
        for bls_key in committee {
            if let Some((peer_id, _)) = self.auth_to_peer(*bls_key) {
                if self.temporarily_banned.remove(&peer_id) {
                    warn!(target: "peer-manager", ?peer_id, "removed committee member from temporarily banned list");
                }
            }
        }
    }

    /// Emit a [`PeerEvent::MissingAuthorities`] for any committee keys with no known network info
    /// so kad discovery can chase them.
    fn trigger_missing_authorities(&mut self, committee: &HashSet<BlsPublicKey>) {
        let missing: Vec<BlsPublicKey> =
            committee.iter().filter(|k| !self.known_peers.contains_key(k)).copied().collect();
        if !missing.is_empty() {
            self.events.push_back(PeerEvent::MissingAuthorities(missing));
        }
    }

    /// Apply unban actions returned by the peer store.
    fn apply_unban_actions(&mut self, actions: Vec<(PeerId, PeerAction)>) {
        for (peer_id, action) in actions {
            self.apply_peer_action(peer_id, action);
        }
    }

    /// Add a locally provisioned known peer and pin it against eviction.
    ///
    /// Used for operator-driven entries only — trusted/explicit peers (bootstrap peers go through
    /// [`Self::add_bootstrap_peer`]) — whose count is bounded by node configuration. Pinned
    /// entries survive committee rotation (see [`Self::prune_known_peers`]). Records restored
    /// from local persistence at startup do NOT use this method precisely because they must not
    /// pin — they go through [`Self::add_restored_peer`]. The attacker-reachable kad discovery
    /// path must instead use [`Self::add_discovered_peer`], which is bounded to committee
    /// membership.
    pub(crate) fn add_known_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        self.pinned_peers.insert(bls_key);
        self.cache_known_peer(bls_key, info);
    }

    /// Add a peer record restored from the persisted kad store at startup, WITHOUT pinning it.
    ///
    /// Restored records are kad-discovered cache, not operator-provisioned peers: the persisted
    /// store's contents are peer-fillable because the node stores arbitrary signature-valid
    /// third-party records as its DHT storage duty. Pinning them would let a restart convert
    /// attacker-fillable store entries into permanently pinned `known_peers` entries, so restored
    /// entries are deliberately NOT pinned — they survive only until the first
    /// [`Self::update_committees`] prunes non-members, restoring the issue #827 committee bound.
    pub(crate) fn add_restored_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        self.cache_known_peer(bls_key, info);
    }

    /// Add an operator-configured bootstrap peer: always pin it, but never overwrite an existing
    /// `known_peers` record.
    ///
    /// Bootstrap peers are operator-provisioned and bounded by node configuration, so they must
    /// always be pinned — under unpinned restore, skipping keys that already resolve (the old
    /// handler pattern) would leave a bootstrap peer with a restored record unpinned, and it
    /// would be pruned at the first committee rotation. An existing `known_peers` entry (e.g. a
    /// richer record restored from persistence, which may carry fresher multiaddrs/rpc info) is
    /// not overwritten by the config-derived stub, preserving the don't-overwrite contract of
    /// the [`AddBootstrapPeers`](crate::types::NetworkCommand) command.
    pub(crate) fn add_bootstrap_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        self.pinned_peers.insert(bls_key);
        if !self.known_peers.contains_key(&bls_key) {
            self.cache_known_peer(bls_key, info);
        }
    }

    /// Add a peer learned from the kad discovery DHT, but only if it is a tracked committee member.
    ///
    /// Unlike [`Self::add_known_peer`], this path is reachable by any remote peer: a
    /// signature-valid kad record only proves the publisher owns the network key it advertises,
    /// not that the advertised [`BlsPublicKey`] belongs to any committee. Caching every such
    /// record would let a peer grow `known_peers` without bound by publishing records for endless
    /// fresh keys (issue #827). `known_peers` exists solely to resolve committee members' network
    /// info, so a record whose key is in no tracked committee slot is dropped: it is either stale
    /// (a member that already rotated out) or forged, and is never read. Legitimate discovery is
    /// unaffected because kad lookups are only ever triggered for current/next committee members
    /// whose info is missing (see [`Self::trigger_missing_authorities`]).
    pub(crate) fn add_discovered_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        if !self.peers.is_committee_member(&bls_key) {
            trace!(
                target: "peer-manager",
                ?bls_key,
                "dropping discovered peer record for non-committee key"
            );
            return;
        }
        if self.kad_record_is_stale(&bls_key, &info) {
            trace!(
                target: "peer-manager",
                ?bls_key,
                "dropping stale discovered peer record"
            );
            return;
        }
        self.cache_known_peer(bls_key, info);
    }

    /// Admit a peer record pushed to us over the peer's own authenticated connection (a kad PUT
    /// whose `source` is the sending peer).
    ///
    /// A committee member is cached in `known_peers` exactly as via [`Self::add_discovered_peer`].
    /// A non-committee peer that advertises its OWN record - the authenticated kad-put `source`
    /// equals the record's advertised network identity, so libp2p has already proven the sender
    /// owns that transport key - has only its `bls_key <-> peer_id` identity confirmed in the peer
    /// store, so a live connection (for example an nvv joining the gossip mesh) is retained. It is
    /// deliberately NOT inserted into the committee-only `known_peers` cache. Because
    /// [`AllPeers::upsert_peer`] re-keys by peer id, a peer can only ever hold ONE confirmed
    /// identity for its own connection, so this is bounded by the live connection count and keeps
    /// the issue #827 bound against unbounded record injection intact. A relayed record (`source`
    /// != the advertised identity) cannot confirm an identity the sender does not control and is
    /// dropped, so this never lets a peer displace another peer's identity.
    pub(crate) fn add_self_advertised_peer(
        &mut self,
        source: PeerId,
        bls_key: BlsPublicKey,
        info: NetworkInfo,
    ) {
        let advertised: PeerId = info.pubkey.clone().into();
        if self.peers.is_committee_member(&bls_key) {
            if self.kad_record_is_stale(&bls_key, &info) {
                trace!(
                    target: "peer-manager",
                    ?bls_key,
                    ?source,
                    "dropping stale self-advertised record for committee member"
                );
                return;
            }
            self.cache_known_peer(bls_key, info);
        } else if source == advertised {
            trace!(
                target: "peer-manager",
                ?bls_key,
                ?source,
                "confirming self-advertised connected peer identity"
            );
            self.peers.upsert_peer(bls_key, info.pubkey, info.multiaddrs);
        } else {
            trace!(
                target: "peer-manager",
                ?bls_key,
                ?source,
                "dropping non-committee relayed peer record"
            );
        }
    }

    /// Number of distinct multiaddrs currently retained for `peer_id`, if it is tracked.
    #[cfg(test)]
    pub(crate) fn peer_multiaddr_count(&self, peer_id: &PeerId) -> Option<usize> {
        self.peers.get_peer(peer_id).map(|peer| peer.multiaddr_count())
    }

    /// Check whether a kad-sourced record's timestamp fails to advance the cached entry.
    ///
    /// Mirrors the store-side `is_newer_record` monotonicity check (consensus.rs) so kad-sourced
    /// updates can never regress a fresher `known_peers` entry — `get_record` query results reach
    /// the cache without passing through the store, so the store-side check alone cannot protect
    /// it. EQUAL timestamps keep the existing entry (benign replay churn — the same rule as the
    /// store side). Operator-provisioned paths bypass this guard structurally: they all stamp
    /// `timestamp: now()` when building the [`NetworkInfo`] (the `AddTrustedPeerAndDial` /
    /// `AddExplicitPeer` / `AddBootstrapPeers` handlers in consensus.rs), and
    /// [`Self::add_bootstrap_peer`] never overwrites an existing entry at all.
    ///
    /// Pinned (operator-provisioned) entries are exempt from the comparison in the other
    /// direction too: their timestamp is a local provisioning stamp, not a peer-signed record
    /// timestamp, and node records are signed once at peer startup — so an operator stub stamped
    /// after the peer started would otherwise block the peer's real record (fresh multiaddrs,
    /// advertised rpc) forever. A signed record may therefore always refresh a pinned entry,
    /// which is the pre-existing upgrade flow for trusted/explicit peers.
    fn kad_record_is_stale(&self, bls_key: &BlsPublicKey, info: &NetworkInfo) -> bool {
        !self.pinned_peers.contains(bls_key)
            && self
                .known_peers
                .get(bls_key)
                .is_some_and(|existing| existing.timestamp >= info.timestamp)
    }

    /// Validate the advertised endpoint, register the peer's network identity, cache its info, and
    /// close the committee trust window if it belongs to a tracked slot.
    ///
    /// Shared body of the known-peer insertion paths; the caller decides whether the entry is
    /// pinned or admitted at all.
    fn cache_known_peer(&mut self, bls_key: BlsPublicKey, mut info: NetworkInfo) {
        // signature verification proves authenticity but not scheme correctness; drop a
        // malformed advertised endpoint so only well-formed RPC info is ever cached in
        // `known_peers`. the rest of the (signed, authentic) record is still usable.
        if let Some(rpc) = &info.rpc {
            if let Err(err) = rpc.validate() {
                warn!(
                    target: "peer-manager",
                    ?err,
                    ?bls_key,
                    "dropping malformed advertised RPC endpoint from peer record"
                );
                info.rpc = None;
            }
        }
        trace!(target: "peer-manager", ?bls_key, "adding known peer");
        self.peers.upsert_peer(bls_key, info.pubkey.clone(), info.multiaddrs.clone());
        self.known_peers.insert(bls_key, info);
        // if this newly-discovered peer belongs to a tracked committee, unban/trust it now
        // (closing the trust window) instead of waiting for the next epoch's `update_committees`.
        // `upsert_peer` just re-keyed it onto its `Confirmed` identity, so the trust pass can
        // resolve its peer id immediately.
        let unban_actions = self.peers.apply_membership_if_committee(bls_key);
        self.apply_unban_actions(unban_actions);
    }

    /// Find authorities for the epoch manager.
    pub(crate) fn find_authorities(&mut self, authorities: Vec<BlsPublicKey>) {
        let mut missing = Vec::new();

        // check all peers for authority and track missing
        for bls_key in authorities {
            // identify missing authorities
            if !self.known_peers.contains_key(&bls_key) {
                missing.push(bls_key);
            }
        }

        // emit event for kad to try to discover
        trace!(target: "peer-manager", ?missing, "requesting kad records");
        self.events.push_back(PeerEvent::MissingAuthorities(missing));
    }

    /// Return the most-recently-fetched [RpcInfo] for the given authority,
    /// if any has been advertised.
    pub(crate) fn get_rpc(&self, bls_key: &BlsPublicKey) -> Option<RpcInfo> {
        self.known_peers.get(bls_key).and_then(|info| info.rpc.clone())
    }

    /// Return the advertised [RpcInfo] for every current-committee validator, and
    /// chase node records for current members that are still unknown.
    ///
    /// Scoped to the current committee: pinned operator peers and previous/next
    /// committee members never appear, even if they advertised RPC info. For any
    /// current member with no known record, kad discovery is (re)triggered via
    /// [`PeerEvent::MissingAuthorities`] — the same path `update_committees` takes at
    /// epoch start — so a polling caller converges as records arrive. Members whose
    /// record is known but carries no RPC info did not advertise one and are skipped
    /// without a re-fetch.
    pub(crate) fn current_committee_rpcs(&mut self) -> Vec<(BlsPublicKey, RpcInfo)> {
        let current = self.peers.current_committee().clone();
        self.trigger_missing_authorities(&current);
        current
            .iter()
            .filter_map(|bls| {
                self.known_peers.get(bls).and_then(|info| info.rpc.clone()).map(|rpc| (*bls, rpc))
            })
            .collect()
    }

    /// Find the peer id for an authority.
    pub(crate) fn auth_to_peer(&self, bls_key: BlsPublicKey) -> Option<(PeerId, Vec<Multiaddr>)> {
        if let Some(NetworkInfo { pubkey, multiaddrs, .. }) = self.known_peers.get(&bls_key) {
            Some((pubkey.clone().into(), multiaddrs.clone()))
        } else {
            debug!(target: "peer-manager", ?bls_key, "unknown peer for bls key");
            None
        }
    }

    /// Find the BlsPublicKey for a known PeerId.
    ///
    /// Backed by the peer store's `Confirmed`-identity index, populated for connected/known peers.
    pub(crate) fn peer_to_bls(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.peers.bls_for_peer(peer_id)
    }

    /// Visibility helper for behavior to evaluate pending outbound connection attempts.
    pub(super) fn can_dial(&self, peer_id: &PeerId) -> bool {
        !self.peer_banned(peer_id) && self.peers.can_dial(peer_id)
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
    /// - it is not this node's own identity
    /// - it has at least one valid ip address (ipv4/ipv6)
    /// - none of its ip addresses are banned
    /// - it can be dialed (not connected/dialing/banned)
    fn eligible_for_discovery(&self, info: &PeerInfo) -> bool {
        // never add our own identity to the discovery feed; a self entry (learned
        // via kad closest-peers or peer exchange) would otherwise be selected for
        // a self-dial during the heartbeat.
        !self.is_local_peer(&info.peer_id)
            && self.has_valid_unbanned_ips(&info.addrs)
            && self.peers.can_dial(&info.peer_id)
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
