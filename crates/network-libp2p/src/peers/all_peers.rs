//! Peer service to track known peers.
//!
//! `AllPeers` is responsible for processing updates to peers and returns `PeerAction`s for the
//! manager to take. Some actions are propagated up to the swarm level and affect other behaviors.

use super::{
    banned::BannedPeers, peer::Peer, score::ReputationUpdate, status::ConnectionStatus,
    types::ConnectionDirection, PeerExchangeMap, Penalty,
};
use crate::{
    error::NetworkError,
    peers::{score::Reputation, status::NewConnectionStatus, types::PeerAction},
    send_or_log_error,
    types::{NetworkInfo, NetworkResult},
};
use libp2p::{Multiaddr, PeerId};
use rand::seq::SliceRandom as _;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use tn_types::BlsPublicKey;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
#[cfg(test)]
#[path = "../tests/peers.rs"]
mod peers;

/// State for known peers.
///
/// This keeps track of [Peer], [BannedPeers], and the number of disconnected peers.
#[derive(Debug)]
pub(super) struct AllPeers {
    /// The collection of known connected peers, their status and reputation
    peers: HashMap<PeerId, Peer>,
    /// The collection of staked current_committee at the beginning of each epoch.
    current_committee: HashSet<PeerId>,
    /// Information for peers that scored poorly enough to become banned.
    banned_peers: BannedPeers,
    /// The number of peers that have disconnected from this node.
    disconnected_peers: usize,
    /// The collection of pending dials.
    pending_dials: HashMap<PeerId, oneshot::Sender<NetworkResult<()>>>,
    /// The timeout for dialing peers.
    dial_timeout: Duration,
    /// The maximum number of banned peers to maintain before pruning.
    max_banned_peers: usize,
    /// The maximum number of disconnected peers to maintain before pruning.
    max_disconnected_peers: usize,
}

impl AllPeers {
    /// Create a new instance of Self.
    pub(super) fn new(
        dial_timeout: Duration,
        max_banned_peers: usize,
        max_disconnected_peers: usize,
    ) -> Self {
        Self {
            peers: Default::default(),
            current_committee: Default::default(),
            banned_peers: Default::default(),
            disconnected_peers: 0,
            pending_dials: Default::default(),
            dial_timeout,
            max_banned_peers,
            max_disconnected_peers,
        }
    }

    /// Create a peer that is "trusted".
    ///
    /// This overwrites peer records and unbans ips.
    pub(super) fn add_trusted_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        let trusted_peer = Peer::new_trusted(Some(addr));
        let _ = self.banned_peers.remove_banned_peer(trusted_peer.known_ip_addresses());
        self.peers.insert(peer_id, trusted_peer);
    }

    /// Handle reported action.
    ///
    /// This method is called when the application layer identifies a problem and reports a peer.
    pub(super) fn process_penalty(&mut self, peer_id: &PeerId, penalty: Penalty) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            let prior_reputation = peer.reputation();
            let new_reputation = peer.apply_penalty(penalty);
            debug!(target: "peer-manager", ?peer_id, ?prior_reputation, ?new_reputation);

            if new_reputation == prior_reputation {
                return PeerAction::NoAction;
            }

            let res = match new_reputation {
                Reputation::Banned => {
                    debug!(target: "peer-manager", ?peer_id, "penalty resulted in banning peer");

                    // update connection status for banned peer
                    self.update_connection_status(peer_id, NewConnectionStatus::Banned)
                }
                Reputation::Disconnected => {
                    if peer.connection_status().is_connected_or_dialing() {
                        self.update_connection_status(
                            peer_id,
                            NewConnectionStatus::Disconnecting { banned: true },
                        )
                    } else {
                        warn!(target: "peer-manager", ?peer_id, ?prior_reputation, "process_penalty for disconnected peer");
                        self.update_connection_status(peer_id, NewConnectionStatus::Disconnected)
                    }
                }
                Reputation::Trusted => {
                    // this should never happen
                    error!(target: "peer-manager", ?peer_id, "process_penalty resulted in peer becoming trusted");
                    PeerAction::NoAction
                }
            };

            return res;
        }

        warn!(target: "peer-manager", ?peer_id, "application layer reported an unknown peer");
        PeerAction::NoAction
    }

    /// Ensure a [Peer] exists.
    ///
    /// This method is called before updating a peer's status. If the peer is unknown, it is
    /// initialized with default data. The new status is used to ensure valid transitions for
    /// unknown peers.
    ///
    /// The method returns the peer's current [ConnectionStatus].
    fn ensure_peer_exists(
        &mut self,
        peer_id: &PeerId,
        new_status: &NewConnectionStatus,
    ) -> ConnectionStatus {
        if !self.peers.contains_key(peer_id) {
            // initialize unknown peer and log warning if status update is invalid for unknown peers
            if !new_status.valid_initial_state() {
                warn!(target: "peer-manager",
                    "Attempt to update {:?} for unknown peer {:?}. Current peers:\n{:?}",
                    new_status,
                    peer_id,
                    self.peers,
                );
            }

            // add default peer
            self.peers.insert(*peer_id, Peer::default());
        }

        // ensure peer is banned if the new state is Banned
        if matches!(new_status, &NewConnectionStatus::Banned) {
            if let Some(peer) = self.peers.get_mut(peer_id) {
                peer.ensure_banned(peer_id);
            } else {
                // unreachable
                error!(target: "peer-manager", ?peer_id, "impossible - peer was just created if it didn't already exist");
            }
        }

        self.peers
            .get(peer_id)
            .map(|peer| peer.connection_status().clone())
            .unwrap_or(ConnectionStatus::Unknown)
    }

    /// Heartbeat maintenance.
    ///
    /// Update scores and connection status for peers.
    ///
    /// Update peer connection status if dialing instant is greater than the timeout allowed.
    /// Peers that fail to connect within dial timeout are updated to
    /// `ConnectionStatus::Disconnected`. It's important these peers are disconnected because
    /// dialing peers are counted towards the limit on inbound connections.
    pub(super) fn heartbeat_maintenance(&mut self) -> Vec<(PeerId, PeerAction)> {
        let peers_to_disconnect: Vec<_> = self
            .peers
            .iter()
            .filter_map(|(peer_id, info)| {
                if let ConnectionStatus::Dialing { instant } = info.connection_status() {
                    if (*instant) + self.dial_timeout < Instant::now() {
                        return Some(*peer_id);
                    }
                }
                None
            })
            .collect();

        // disconnect peers
        for peer_id in peers_to_disconnect {
            self.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        }

        // update scores for all other peers
        self.update_peer_scores()
    }

    /// Update scores for heartbeat interval.
    ///
    /// Returns any subsequent actions the peer manager should take after the peer's score is
    /// updated. Peers are possibly unbanned, but penalties are not applied with this method.
    /// It's impossible for a peer to become banned during heartbeat maintenance.
    ///
    /// See [Self::apply_penalty] for ban logic.
    fn update_peer_scores(&mut self) -> Vec<(PeerId, PeerAction)> {
        // filter peers that are eligible to become unbanned
        let unbanned_peers = self.peers.iter_mut().filter_map(|(id, peer)| {
            let update = peer.heartbeat();
            match update {
                ReputationUpdate::Unbanned => {
                    Some(*id)
                },
                // filter other results and log error
                ReputationUpdate::Banned | ReputationUpdate::Disconnect => {
                    error!(
                        target: "peer-manager",
                        ?update,
                        ?id,
                        "peer reputation penalized during heartbeat - penalties only expected to decay"
                    );
                    None
                },
                ReputationUpdate::None => None,
            }
        }).collect::<Vec<_>>();

        // update peer connection status and return actions for manager
        unbanned_peers
            .iter()
            .map(|id| {
                let action = self.update_connection_status(id, NewConnectionStatus::Unbanned);
                (*id, action)
            })
            .collect()
    }

    /// Update the peer's connection status.
    ///
    /// This method ensures the collection of peers stays an appropriate size and in-sync with
    /// libp2p.
    pub(super) fn update_connection_status(
        &mut self,
        peer_id: &PeerId,
        new_status: NewConnectionStatus,
    ) -> PeerAction {
        let current_status = self.ensure_peer_exists(peer_id, &new_status);

        debug!(target: "peer-manager", ?peer_id, ?current_status, ?new_status, "update_connection_status");

        // Handle the state transition and return any necessary ban operations
        self.handle_status_transition(peer_id, current_status, new_status)
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
        // create the peer if it doesn't exist and register as dialing
        self.update_connection_status(&peer_id, NewConnectionStatus::Dialing);
        if let Some(reply) = reply {
            self.pending_dials.insert(peer_id, reply);
        }
    }

    /// Return the oneshot sender for dial attempt if it exists.
    pub(super) fn reply_for_dial_attempt(
        &mut self,
        peer_id: &PeerId,
    ) -> Option<oneshot::Sender<NetworkResult<()>>> {
        self.pending_dials.remove(peer_id)
    }

    /// Notify the caller about the result of a dial attempt.
    pub(super) fn notify_dial_result(&mut self, peer_id: &PeerId, result: NetworkResult<()>) {
        // return result to caller
        if let Some(reply) = self.reply_for_dial_attempt(peer_id) {
            send_or_log_error!(reply, result, "DialResult", peer = peer_id);
        }
    }

    /// Handle the state transition and return ban operations if needed
    ///
    /// WARNING: callers should call `Self::ensure_peer_exists` before handling the status
    /// transition
    fn handle_status_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: ConnectionStatus,
        new_status: NewConnectionStatus,
    ) -> PeerAction {
        match new_status {
            // Group transitions by the new status
            NewConnectionStatus::Connected { multiaddr, direction } => {
                let action = self.handle_connected_transition(
                    peer_id,
                    &current_status,
                    multiaddr,
                    direction,
                );
                // return ok to caller if dial attempt resulted in connection
                if current_status.is_dialing() {
                    self.notify_dial_result(peer_id, Ok(()));
                }
                action
            }
            NewConnectionStatus::Dialing => self.handle_dialing_transition(peer_id, current_status),
            NewConnectionStatus::Disconnected => {
                self.handle_disconnected_transition(peer_id, current_status)
            }
            NewConnectionStatus::Disconnecting { banned } => {
                self.handle_disconnecting_transition(peer_id, current_status, banned)
            }
            NewConnectionStatus::Banned => self.handle_banned_transition(peer_id, current_status),
            NewConnectionStatus::Unbanned => {
                self.handle_unbanned_transition(peer_id, current_status)
            }
        }
    }

    /// Handle transition to Connected state
    fn handle_connected_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: &ConnectionStatus,
        multiaddr: Multiaddr,
        direction: ConnectionDirection,
    ) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            // update counters based on previous state
            match current_status {
                ConnectionStatus::Disconnected { .. } => {
                    self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
                }
                ConnectionStatus::Banned { .. } => {
                    error!(target: "peer-manager", ?peer_id, "accepted a connection from a banned peer");
                    self.banned_peers.remove_banned_peer(peer.known_ip_addresses());
                }
                ConnectionStatus::Disconnecting { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "connected to a disconnecting peer")
                }
                ConnectionStatus::Unknown
                | ConnectionStatus::Connected { .. }
                | ConnectionStatus::Dialing { .. } => {}
            }

            // update connection status for peer
            match direction {
                ConnectionDirection::Incoming => peer.register_incoming(multiaddr),
                ConnectionDirection::Outgoing => peer.register_outgoing(multiaddr),
            }
        }

        PeerAction::NoAction
    }

    /// Handle transition to Dialing state
    fn handle_dialing_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: ConnectionStatus,
    ) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            match current_status {
                ConnectionStatus::Banned { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "dialing a banned peer");
                    self.banned_peers.remove_banned_peer(peer.known_ip_addresses());
                }
                ConnectionStatus::Disconnected { .. } => {
                    self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
                }
                ConnectionStatus::Connected { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "dialing an already connected peer")
                }
                ConnectionStatus::Dialing { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "dialing an already dialing peer")
                }
                ConnectionStatus::Disconnecting { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "dialing a disconnecting peer")
                }
                ConnectionStatus::Unknown => {} // default status
            }

            if let Err(e) = peer.register_dialing() {
                error!(target: "peer-manager", e, ?peer_id, "error updating peer to dialing");
            }
        }

        PeerAction::NoAction
    }

    /// Handle transition to Disconnected state
    fn handle_disconnected_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: ConnectionStatus,
    ) -> PeerAction {
        match current_status {
            ConnectionStatus::Banned { .. } => {}
            ConnectionStatus::Disconnected { .. } => {}
            ConnectionStatus::Disconnecting { banned } if banned => {
                return self.handle_disconnected_and_banned(peer_id);
            }
            ConnectionStatus::Disconnecting { .. } => {
                return self.handle_disconnected_normal(peer_id);
            }
            ConnectionStatus::Unknown
            | ConnectionStatus::Connected { .. }
            | ConnectionStatus::Dialing { .. } => {
                self.disconnected_peers += 1;
                if let Some(peer) = self.peers.get_mut(peer_id) {
                    peer.set_connection_status(ConnectionStatus::Disconnected {
                        instant: Instant::now(),
                    });
                }

                // notify caller of dial error if present
                self.notify_dial_result(
                    peer_id,
                    Err(NetworkError::Dial("dial attempt timedout".to_string())),
                );
            }
        }

        PeerAction::NoAction
    }

    /// Handle disconnected state for a peer that transitioned to disconnected with banned flag.
    fn handle_disconnected_and_banned(&mut self, peer_id: &PeerId) -> PeerAction {
        // filter these with newly banned peer
        let already_banned_ips = self.banned_peers.banned_ips();

        debug!(target: "peer-manager", ?already_banned_ips, "handle disconnected and banned");

        // update peer's status
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.set_connection_status(ConnectionStatus::Banned { instant: Instant::now() });
            self.banned_peers.add_banned_peer(peer);
            let banned_ips = peer
                .known_ip_addresses()
                .filter(|ip| already_banned_ips.contains(ip))
                .collect::<Vec<_>>();
            PeerAction::Ban(banned_ips)
        } else {
            // NOTE: this should never happen
            warn!(target: "peer-manager", ?peer_id, "failed to retrieve peer data for handling disconnect and ban");
            PeerAction::Ban(Vec::new())
        }
    }

    /// Handle disconnected state for a peer that was disconnected without being banned.
    fn handle_disconnected_normal(&mut self, peer_id: &PeerId) -> PeerAction {
        self.disconnected_peers += 1;
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.set_connection_status(ConnectionStatus::Disconnected { instant: Instant::now() });
        }

        PeerAction::NoAction
    }

    /// Handle transition to Disconnecting state
    fn handle_disconnecting_transition(
        &mut self,
        peer_id: &PeerId,
        current_state: ConnectionStatus,
        banned: bool,
    ) -> PeerAction {
        // set the peer to disconnecting state
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.set_connection_status(ConnectionStatus::Disconnecting { banned });
        }

        match current_state {
            ConnectionStatus::Disconnected { .. } => {
                // if the peer was previously disconnected and is now being disconnected,
                // decrease the disconnected_peers counter
                self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
            }
            ConnectionStatus::Banned { .. } => {
                // banned peers should already be disconnected
                error!(target: "peer-manager", ?peer_id, "disconnecting from a banned peer - banned peer should already be disconnected");
            }
            ConnectionStatus::Connected { .. } | ConnectionStatus::Dialing { .. } => {
                // support discovery with peer exchange if the target number of peers is reached
                let action =
                    if banned { PeerAction::Disconnect } else { PeerAction::DisconnectWithPX };
                return action;
            }
            _ => {}
        }

        PeerAction::NoAction
    }

    /// Handle transition to Banned state
    fn handle_banned_transition(
        &mut self,
        peer_id: &PeerId,
        current_state: ConnectionStatus,
    ) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            match current_state {
                ConnectionStatus::Disconnected { .. } => {
                    self.banned_peers.add_banned_peer(peer);
                    self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
                    let already_banned_ips = self.banned_peers.banned_ips();

                    // ensure the peer is banned
                    if !peer.connection_status().is_banned() {
                        peer.set_connection_status(ConnectionStatus::Banned {
                            instant: Instant::now(),
                        });
                    }

                    PeerAction::Ban(peer.filter_new_ips_to_ban(&already_banned_ips))
                }
                ConnectionStatus::Disconnecting { .. } => {
                    // ban the peer once the disconnection process completes
                    debug!(target: "peer-manager", ?peer_id, "banning peer that is currently disconnecting");
                    peer.set_connection_status(ConnectionStatus::Disconnecting { banned: true });
                    PeerAction::NoAction
                }
                ConnectionStatus::Banned { .. } => {
                    error!(target: "peer-manager", ?peer_id, "banning already banned peer");
                    let already_banned_ips = self.banned_peers.banned_ips();
                    PeerAction::Ban(peer.filter_new_ips_to_ban(&already_banned_ips))
                }
                ConnectionStatus::Connected { .. } | ConnectionStatus::Dialing { .. } => {
                    peer.set_connection_status(ConnectionStatus::Disconnecting { banned: true });
                    PeerAction::Disconnect
                }
                ConnectionStatus::Unknown => {
                    warn!(target: "peer-manager", ?peer_id, "banning an unknown peer");
                    self.banned_peers.add_banned_peer(peer);
                    peer.set_connection_status(ConnectionStatus::Banned {
                        instant: Instant::now(),
                    });
                    let already_banned_ips = self.banned_peers.banned_ips();
                    PeerAction::Ban(peer.filter_new_ips_to_ban(&already_banned_ips))
                }
            }
        } else {
            warn!(target: "peer-manager", ?peer_id, "failed to retrieve peer data for handling banned transition");
            PeerAction::NoAction
        }
    }

    /// Handle transition to Unbanned state
    fn handle_unbanned_transition(
        &mut self,
        peer_id: &PeerId,
        current_state: ConnectionStatus,
    ) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            if matches!(peer.reputation(), Reputation::Banned) {
                error!(target: "peer-manager", ?peer_id, "unbanning a banned peer");
            }

            // expected status is "banned", but there are possible edge cases
            match current_state {
                ConnectionStatus::Banned { instant } => {
                    // change the status to "disconnected" so the peer isn't registered as "banned"
                    // anymore
                    peer.set_connection_status(ConnectionStatus::Disconnected { instant });

                    // update counters
                    self.banned_peers.remove_banned_peer(peer.known_ip_addresses());
                    self.disconnected_peers = self.disconnected_peers.saturating_add(1);

                    return PeerAction::Unban(peer.known_ip_addresses().collect());
                }
                ConnectionStatus::Disconnecting { banned } => {
                    debug!(target: "peer-manager", ?peer_id, "unbanning disconnecting peer");
                    if banned {
                        // set disconnecting status false
                        peer.set_connection_status(ConnectionStatus::Disconnecting {
                            banned: false,
                        });
                    }
                }
                ConnectionStatus::Disconnected { .. } => {
                    debug!(target: "peer-manager", ?peer_id, "unbanning disconnected peer");
                }
                ConnectionStatus::Dialing { .. } => {
                    debug!(target: "peer-manager", ?peer_id, "unbanning dialing peer");
                }
                ConnectionStatus::Unknown | ConnectionStatus::Connected { .. } => {
                    // technically an error, but not fatal
                    error!(target: "peer-manager", ?peer_id, "unbanning a connected peer");
                }
            }
        }

        PeerAction::NoAction
    }

    /// Return the [Peer] by [PeerId] if it is known.
    pub(super) fn get_peer(&self, peer_id: &PeerId) -> Option<&Peer> {
        self.peers.get(peer_id)
    }

    /// Boolean indicating if this peer is a validator.
    /// This method will be updated to include nvvs as well.
    pub(super) fn is_validator(&self, peer_id: &PeerId) -> bool {
        self.is_cvv(peer_id)
    }

    /// Boolean indicating if this peer is in the current committee of voting validators.
    pub(super) fn is_cvv(&self, peer_id: &PeerId) -> bool {
        self.current_committee.contains(peer_id)
    }

    /// Boolean indicating if the ip address is associated with a banned peer.
    pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers.ip_banned(ip)
    }

    /// Boolean indicating if a peer id is banned or associated with any banned ip addresses.
    /// NOTE: the peer can still be in a connected status but pending a ban, so the connection
    /// status is not used.
    pub(super) fn peer_banned(&self, peer_id: &PeerId) -> bool {
        self.peers.get(peer_id).is_some_and(|peer| {
            peer.reputation().banned() || peer.known_ip_addresses().any(|ip| self.ip_banned(&ip))
        })
    }

    /// Gives the ids of all known connected peers.
    pub(super) fn connected_peer_ids(&self) -> impl Iterator<Item = &PeerId> {
        self.peers.iter().filter_map(|(peer_id, peer)| {
            peer.connection_status().is_connected().then_some(peer_id)
        })
    }

    /// Return an iterator of peers that are connected or dialed.
    pub(super) fn connected_or_dialing_peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter(|(_, peer)| {
                let status = peer.connection_status();
                status.is_connected() || status.is_dialing()
            })
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Returns a boolean indicating if a peer is already connected or disconnecting.
    ///
    /// Used when handling connection closed events from the swarm.
    pub(super) fn is_peer_connected_or_disconnecting(&self, peer_id: &PeerId) -> bool {
        self.peers.get(peer_id).is_some_and(|peer| {
            matches!(
                peer.connection_status(),
                ConnectionStatus::Connected { .. } | ConnectionStatus::Disconnecting { .. }
            )
        })
    }

    /// Collect connected peers to exchange with disconnecting peer.
    pub(super) fn peer_exchange(&self) -> PeerExchangeMap {
        self.peers
            .iter()
            .filter_map(|(id, peer)| {
                if peer.connection_status().is_connected() {
                    Some((*id, peer.exchange_info()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>()
            .into()
    }

    /// Sort connected peers from lowest to highest score.
    ///
    /// The shuffle ensures peers with equal scores are sorted in a random order.
    pub(super) fn connected_peers_by_score(&self) -> Vec<(&PeerId, &Peer)> {
        let mut connected_peers: Vec<_> =
            self.peers.iter().filter(|(_, peer)| peer.connection_status().is_connected()).collect();

        connected_peers.shuffle(&mut rand::rng());
        connected_peers.sort_by_key(|(_, peer)| peer.score());
        connected_peers
    }

    /// Register the peer as disconnected.
    ///
    /// It's possible that the peer's updated connection status results in the peer being banned.
    /// This method updates the connection status for the peer and ensures the number of banned
    /// peers doesn't exceed the allowable limit.
    pub(super) fn register_disconnected(
        &mut self,
        peer_id: &PeerId,
    ) -> (PeerAction, Vec<(PeerId, Vec<IpAddr>)>) {
        let action = self.update_connection_status(peer_id, NewConnectionStatus::Disconnected);
        // prune excess disconnected/banned peers
        self.prune_disconnected_peers();
        let pruned_peers = self.prune_banned_peers();
        (action, pruned_peers)
    }

    /// Filter peers based on connection status.
    ///
    /// This creates a min-heap with the excess number of peers.
    /// Used by Self::prune_banned_peers and Self::prune_disconnected_peers.
    fn collect_excess_peers<F>(
        &self,
        excess: usize,
        filter: F,
    ) -> BinaryHeap<(Reverse<Instant>, PeerId, Vec<IpAddr>)>
    where
        F: Fn(&ConnectionStatus) -> Option<Instant>,
    {
        // collection of peers to prune
        let mut excess_peers = BinaryHeap::with_capacity(excess);

        for (peer_id, peer) in &self.peers {
            if let Some(instant) = filter(peer.connection_status()) {
                // min-heap sorted by instant (oldest first)
                let entry =
                    (Reverse(instant), *peer_id, peer.known_ip_addresses().collect::<Vec<_>>());

                if excess_peers.len() < excess {
                    // fill the heap until `excess` elements
                    excess_peers.push(entry);
                } else if let Some(current_max) = excess_peers.peek() {
                    // if peer's banned instant is older, replace it
                    if entry.0 < current_max.0 {
                        excess_peers.pop();
                        excess_peers.push(entry);
                    }
                }
            }
        }

        excess_peers
    }

    /// Prune excess number of banned peers to prevent exhausting memory.
    fn prune_banned_peers(&mut self) -> Vec<(PeerId, Vec<IpAddr>)> {
        let excess = self.banned_peers.total().saturating_sub(self.max_banned_peers);
        let mut unbanned = Vec::with_capacity(excess);

        // remove excess peers from banned collection
        if excess > 0 {
            let excess_peers = self.collect_excess_peers(excess, |status| {
                if let ConnectionStatus::Banned { instant } = status {
                    Some(*instant)
                } else {
                    None
                }
            });

            for (_, peer_id, ip_addrs) in excess_peers {
                self.peers.remove(&peer_id);
                let ips = self.banned_peers.remove_banned_peer(ip_addrs.clone().into_iter());
                unbanned.push((peer_id, ips));
            }
        }

        unbanned
    }

    /// Prune excess number of disconnected peers to prevent exhausting memory.
    fn prune_disconnected_peers(&mut self) {
        let excess = self.disconnected_peers.saturating_sub(self.max_disconnected_peers);

        if excess > 0 {
            let excess_peers = self.collect_excess_peers(excess, |status| {
                if let ConnectionStatus::Disconnected { instant } = status {
                    Some(*instant)
                } else {
                    None
                }
            });

            // remove peer
            for (_, peer_id, _) in excess_peers {
                self.peers.remove(&peer_id);
                self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
            }
        }
    }

    /// Update committee for the new epoch.
    ///
    /// The committee is tracked to ensure priority on the network.
    /// The banned status of any committee peer is forgiven and IPs
    /// associated with the committee node are reset. The advertised
    /// listening addresses are updated and the peer is marked `trusted`
    /// so it won't incur any additional penalties.
    pub(super) fn new_epoch(
        &mut self,
        committee: HashMap<PeerId, Multiaddr>,
    ) -> Vec<(PeerId, PeerAction)> {
        // update current committee
        self.current_committee = committee.keys().cloned().collect();

        let mut actions = Vec::with_capacity(committee.len());
        for (peer_id, addr) in committee.into_iter() {
            // the NewConnectionStatus doesn't affect this call
            let status = self.ensure_peer_exists(&peer_id, &NewConnectionStatus::Unbanned);

            match status {
                ConnectionStatus::Disconnecting { banned } => {
                    // unban peer
                    if banned {
                        warn!(target: "peer-manager", ?peer_id, "unbanning committee member that was disconnecting pending ban");
                        let action =
                            self.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);
                        actions.push((peer_id, action));
                    }
                }
                ConnectionStatus::Banned { .. } => {
                    warn!(target: "peer-manager", ?peer_id, "unbanning committee member that was disconnecting pending ban");
                    let action =
                        self.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);
                    actions.push((peer_id, action));
                }
                ConnectionStatus::Disconnected { .. }
                | ConnectionStatus::Dialing { .. }
                | ConnectionStatus::Unknown
                | ConnectionStatus::Connected { .. } => { /* nothing to do */ }
            }

            // already ensured peer exists
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                // update peer regardless of connection status
                peer.make_trusted();
                peer.update_listening_addrs(addr);
                self.banned_peers.remove_validator_ip(&peer_id, peer.known_ip_addresses());
            }
        }

        // return any unban actions for committee peers
        actions
    }

    /// Find an authority to dial.
    ///
    /// Validators are unbanned at the beginning of each epoch when they are in the committee.
    /// This method is called at the end of each epoch to give the peer manager time (2 epochs)
    /// to find the committee peers.
    ///
    /// The peer manager tracks connections through kad and keeps track of peers of interest.
    pub(super) fn find_authority(&self, _: &BlsPublicKey) -> Option<NetworkInfo> {
        // TODO: see issue #301
        None
    }
}
