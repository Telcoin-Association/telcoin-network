//! Peer service to track known peers.

use banned::BannedPeers;
use libp2p::{Multiaddr, PeerId};
use peer::Peer;
use rand::seq::SliceRandom as _;
use score::{Penalty, Reputation, ReputationUpdate};
use status::{ConnectionStatus, NewConnectionStatus};
use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
    time::{Duration, Instant},
};
use tracing::{debug, error, warn};
use types::ConnectionDirection;
mod banned;
mod behavior;
mod manager;
mod peer;
mod score;
mod status;
mod types;

//
// TODO: move this to once-cell peer-config
// // // // //
const DIAL_TIMEOUT: u64 = 15;

/// State for known peers.
///
/// This keeps track of [Peer], [BannedPeers], and the number of disconnected peers.
// TODO: this was PeerDB
#[derive(Debug, Default)]
pub struct AllPeers {
    /// The collection of known connected peers, their status and reputation
    peers: HashMap<PeerId, Peer>,
    /// The collection of staked validators at the beginning of each epoch.
    validators: BTreeSet<PeerId>,
    /// Information for peers that scored poorly enough to become banned.
    banned_peers: BannedPeers,
    /// The number of peers that have disconnected from this node.
    disconnected_peers: usize,
}

/// The action to take based on the reported penalty and score update.
pub enum PeerAction {
    /// Ban the peer and the associated IP addresses.
    Ban(Vec<IpAddr>),
    /// No action needed.
    NoAction,
    /// Peer already disconnecting - nothing to do?
    /// TODO: can this also be NoAction?
    Disconnecting,
    /// Disconnect from peer.
    Disconnect,
    /// Unban the peer and it's known ip addresses.
    Unban(Vec<IpAddr>),
}

impl AllPeers {
    /// Handle reported action.
    ///
    /// This method is called when the application layer identifies a problem and reports a peer.
    // the PeerManager uses this result to:
    // - ban operation
    // - disconnect
    // - or unban
    //
    // TODO: review this logic before merge - heavily modified from PeerDB::report_peer
    pub(super) fn process_penalty(&mut self, peer_id: &PeerId, penalty: Penalty) -> PeerAction {
        // penalty was reported by application layer:
        // - if this results in a ban, ban the peer
        //  - consider previous status to ensure correct updates
        // - if this results in disconnecting, start disconnecting
        //  - consider previous status to ensure correct updates
        // - everything else is a noop

        if let Some(peer) = self.peers.get_mut(peer_id) {
            let prior_reputation = peer.reputation();
            let new_reputation = peer.apply_penalty(penalty);

            if new_reputation == prior_reputation {
                // TODO: NoAction
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
                            NewConnectionStatus::Disconnecting { banned: false },
                        )
                    } else {
                        // TODO: use peer's ip addresses
                        let ip_addr = vec![];
                        PeerAction::Unban(ip_addr)
                    }
                }
                Reputation::Trusted => {
                    match prior_reputation {
                        Reputation::Trusted => { /* no action */ }
                        Reputation::Disconnected => {
                            // no action
                        }
                        Reputation::Banned => { /* THIS SHOULD NEVER HAPPEN */ }
                    }

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
                    ?peer_id,
                    ?new_status,
                    "Attempt to update connection status for unknown peer"
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
            .unwrap_or(ConnectionStatus::Unknown) // impossible
    }

    /// Filter banned peer's ip addresses against already known banned ip addresses.
    fn filter_new_ips_to_ban(&self, peer: &Peer) -> Vec<IpAddr> {
        let already_banned_ips = self.banned_peers.banned_ips();
        peer.known_ip_addresses().filter(|ip| already_banned_ips.contains(ip)).collect::<Vec<_>>()
    }

    /// Heartbeat maintenance.
    ///
    /// Update scores and connection status for peers.
    ///
    /// Update peer connection status if dialing instant is greater than the timeout allowed.
    /// Peers that fail to connect within dial timeout are updated to `ConnectionStatus::Disconnected`.
    /// It's important these peers are disconnected because dialing peers are counted towards the limit
    /// on inbound connections.
    ///
    /// TODO: best way to handle committee peers with dial timeout?
    fn heartbeat_maintenance(&mut self) -> Vec<(PeerId, PeerAction)> {
        let peers_to_disconnect: Vec<_> = self
            .peers
            .iter()
            .filter_map(|(peer_id, info)| {
                if let ConnectionStatus::Dialing { instant } = info.connection_status() {
                    if (*instant) + Duration::from_secs(DIAL_TIMEOUT) < Instant::now() {
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
    /// Returns any subsequent actions the peer manager should take after the peer's score is updated.
    /// Peers are possibly unbanned, but penalties are not applied with this method. It's
    /// impossible for a peer to become banned during heartbeat maintenance.
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
                    error!(target: "peer-manager", ?update, "peer reputation penalized during heartbeat - penalties only expected to decay");
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
    fn update_connection_status(
        &mut self,
        peer_id: &PeerId,
        new_status: NewConnectionStatus,
    ) -> PeerAction {
        let current_status = self.ensure_peer_exists(peer_id, &new_status);

        // Handle the state transition and return any necessary ban operations
        self.handle_status_transition(peer_id, current_status, new_status)
    }

    /// Handle the state transition and return ban operations if needed
    ///
    /// WARNING: callers should call `Self::ensure_peer_exists` before handling the status transition
    fn handle_status_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: ConnectionStatus,
        new_status: NewConnectionStatus,
    ) -> PeerAction {
        match (current_status, new_status) {
            // Group transitions by the new status
            (current_status, NewConnectionStatus::Connected { multiaddr, direction }) => {
                self.handle_connected_transition(peer_id, current_status, multiaddr, direction)
            }
            (current_status, NewConnectionStatus::Dialing) => {
                self.handle_dialing_transition(peer_id, current_status)
            }
            (current_status, NewConnectionStatus::Disconnected) => {
                self.handle_disconnected_transition(peer_id, current_status)
            }
            (current_status, NewConnectionStatus::Disconnecting { banned }) => {
                self.handle_disconnecting_transition(peer_id, current_status, banned)
            }
            (current_status, NewConnectionStatus::Banned) => {
                self.handle_banned_transition(peer_id, current_status)
            }
            (current_status, NewConnectionStatus::Unbanned) => {
                self.handle_unbanned_transition(peer_id, current_status)
            }
        }
    }

    /// Handle transition to Connected state
    fn handle_connected_transition(
        &mut self,
        peer_id: &PeerId,
        current_status: ConnectionStatus,
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
            }
        }

        PeerAction::NoAction
    }

    /// Handle disconnected state for a peer that transitioned to disconnected with banned flag.
    fn handle_disconnected_and_banned(&mut self, peer_id: &PeerId) -> PeerAction {
        // filter these with newly banned peer
        let already_banned_ips = self.banned_peers.banned_ips();

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
            PeerAction::Ban(Vec::with_capacity(0))
        }
    }

    /// Handle disconnected state for a peer that was disconnected without being banned.
    fn handle_disconnected_normal(&mut self, peer_id: &PeerId) -> PeerAction {
        // The peer was disconnected but not banned.
        self.disconnected_peers += 1;
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.set_connection_status(ConnectionStatus::Disconnected { instant: Instant::now() });
        }

        PeerAction::Disconnect
    }

    /// Handle transition to Disconnecting state
    fn handle_disconnecting_transition(
        &mut self,
        peer_id: &PeerId,
        current_state: ConnectionStatus,
        banned: bool,
    ) -> PeerAction {
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
            _ => {} // Nothing else to do
        }

        // set the peer to disconnecting state
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.set_connection_status(ConnectionStatus::Disconnecting { banned });
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
                    PeerAction::Ban(peer.filter_new_ips_to_ban(&already_banned_ips))
                }
                ConnectionStatus::Disconnecting { .. } => {
                    // ban the peer once the disconnection process completes
                    debug!(target: "peer-manager", ?peer_id, "banning peer that is currently disconnecting");
                    PeerAction::Disconnecting
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
                    // change the status to "disconnected" so the peer isn't registered as "banned" anymore
                    peer.set_connection_status(ConnectionStatus::Disconnected { instant });

                    // update counters
                    self.banned_peers.remove_banned_peer(peer.known_ip_addresses());
                    self.disconnected_peers = self.disconnected_peers.saturating_add(1);

                    return PeerAction::Unban(peer.known_ip_addresses().collect());
                }
                ConnectionStatus::Disconnected { .. } | ConnectionStatus::Disconnecting { .. } => {
                    debug!(target: "peer-manager", ?peer_id, "unbanning disconnected or disconnecting peer");
                }
                ConnectionStatus::Dialing { .. } => {} // odd but acceptable
                ConnectionStatus::Unknown | ConnectionStatus::Connected { .. } => {
                    // technically an error, but not fatal
                    error!(target: "peer-manager", ?peer_id, "unbanning a connected peer");
                }
            }
        }

        PeerAction::NoAction
    }

    /*


    turtle



    */

    /// Return the [Peer] by [PeerId] if it is known.
    pub(super) fn get_peer(&self, peer_id: &PeerId) -> Option<&Peer> {
        self.peers.get(peer_id)
    }

    /// Boolean indicating if this peer is a staked validator.
    pub fn is_validator(&self, peer_id: &PeerId) -> bool {
        self.validators.contains(peer_id)
    }

    /// Boolean indicating if the ip address is associated with a banned peer.
    pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers.ip_banned(ip)
    }

    /// Boolean indicating if a peer id is banned or associated with a banned ip address.
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
    pub(super) fn connected_or_dialing_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peers
            .iter()
            .filter(|(_, peer)| {
                let status = peer.connection_status();
                status.is_connected() || status.is_dialing()
            })
            .map(|(peer_id, _)| peer_id)
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

    /// Sort connected peers from lowest to highest score.
    ///
    /// The shuffle ensures peers with equal scores are sorted in a random order.
    pub(super) fn connected_peers_by_score(&self) -> Vec<(&PeerId, &Peer)> {
        let mut connected_peers: Vec<_> =
            self.peers.iter().filter(|(_, peer)| peer.connection_status().is_connected()).collect();

        connected_peers.shuffle(&mut rand::thread_rng());
        connected_peers.sort_by_key(|(_, peer)| peer.score());
        connected_peers
    }

    pub(super) fn register_disconnected(
        &mut self,
        peer_id: &PeerId,
    ) -> (PeerAction, Vec<(PeerId, Vec<IpAddr>)>) {
        let action = self.update_connection_status(peer_id, NewConnectionStatus::Disconnected);
        todo!()
    }
}
