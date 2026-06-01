//! Peer service to track known peers.
//!
//! `AllPeers` is responsible for processing updates to peers and returns `PeerAction`s for the
//! manager to take. Some actions are propagated up to the swarm level and affect other behaviors.
//!
//! # `bls_index` invariant
//!
//! `AllPeers` maintains a secondary index `bls_index: HashMap<BlsPublicKey, PeerId>` that is the
//! inverse of `Peer::bls_public_key` for every non-`None` value. Concretely:
//!
//! `bls_index[bls] == pid`  iff  `peers[pid].bls_public_key == Some(bls)`
//!
//! Every mutation that sets, changes, or clears `Peer::bls_public_key` MUST go through
//! [`AllPeers::rebind_bls`] (for inserts/updates) or pair the `peers.remove(...)` call with a
//! `bls_index.remove(...)` (for evictions). Direct access to `Peer::bls_public_key` outside this
//! module is forbidden — that is enforced by `pub(super)` visibility.
//!
//! ## Committee lock exception
//!
//! When `rebind_bls` is called for a BLS key that is already promoted in
//! `current_committee_keys` with a *different* `PeerId`, the rebind is skipped to prevent
//! cross-contamination from worker-network kad records. The caller's `Peer` record still
//! gets `bls_public_key = Some(bls)` via `update_net`, creating a temporary state where two
//! peers claim the same BLS but `bls_index` only points to the promoted one. This exception
//! is cleared automatically when `new_epoch` resets `current_committee_keys`.
use super::{
    banned::BannedPeers, peer::Peer, score::ReputationUpdate, status::ConnectionStatus,
    types::ConnectionDirection, PeerExchangeMap, Penalty,
};
use crate::{
    error::NetworkError,
    peers::{score::Reputation, status::NewConnectionStatus, types::PeerAction},
    send_or_log_error,
    types::NetworkResult,
};
use libp2p::{Multiaddr, PeerId};
use rand::seq::SliceRandom as _;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use tn_types::{BlsPublicKey, NetworkPublicKey};
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
    /// Secondary index from BLS public key to PeerId.
    ///
    /// Maintained in lockstep with `peers[pid].bls_public_key`. See module docs for the
    /// invariant that callers must preserve.
    bls_index: HashMap<BlsPublicKey, PeerId>,
    /// The collection of staked current_committee at the beginning of each epoch.
    current_committee: HashSet<PeerId>,
    /// The collection of staked current_committee pub key to peerid at the beginning of each
    /// epoch.
    current_committee_keys: HashMap<BlsPublicKey, Option<PeerId>>,
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
            bls_index: Default::default(),
            current_committee: Default::default(),
            current_committee_keys: Default::default(),
            banned_peers: Default::default(),
            disconnected_peers: 0,
            pending_dials: Default::default(),
            dial_timeout,
            max_banned_peers,
            max_disconnected_peers,
        }
    }

    /// Update `bls_index` so that `new_bls` maps to `peer_id`, evicting any orphan
    /// forward/reverse entries the new association invalidates.
    ///
    /// Callers should invoke this BEFORE mutating `peers[peer_id].bls_public_key` so that any
    /// previously-associated BLS key is observable on the peer.
    ///
    /// Two orphan cases:
    ///   - The peer at `peer_id` already had a different BLS — drop that forward entry from the
    ///     index since the peer is being re-bound.
    ///   - The BLS key `new_bls` already pointed at a different `PeerId` (BLS rotation across
    ///     peers). The orphan peer's `bls_public_key` is cleared via [`Peer::clear_bls`] so the
    ///     peer record survives (it may still carry useful state) but stops being reachable via the
    ///     index until its next `update_net`.
    fn rebind_bls(&mut self, peer_id: PeerId, new_bls: BlsPublicKey) {
        // Layer 1: lock bls_index for promoted committee members.
        //
        // During staggered restarts, primary and worker networks both publish kad records
        // keyed on the same BLS public key but with different PeerIds. If a worker-network
        // record arrives after new_epoch promoted the primary PeerId, rebinding would corrupt
        // bls_index and leave the real committee member unprotected from penalties.
        //
        // Guard: if new_bls is already promoted (Some(existing_peer_id)) and the caller is
        // a *different* PeerId, skip the rebind entirely. The caller's Peer record still
        // gets bls_public_key set via update_net (a controlled invariant exception — two
        // peers claim the same BLS, but bls_index only points to the promoted one).
        if let Some(Some(existing_peer_id)) = self.current_committee_keys.get(&new_bls) {
            if *existing_peer_id != peer_id {
                warn!(
                    target: "peer-manager",
                    ?peer_id,
                    ?existing_peer_id,
                    "rebind_bls: skipping — BLS key is locked to promoted committee member"
                );
                return;
            }
        }

        if let Some(peer) = self.peers.get(&peer_id) {
            if let Some(old_bls) = peer.bls_public_key() {
                if old_bls != new_bls {
                    self.bls_index.remove(&old_bls);
                }
            }
        }
        if let Some(old_peer_id) = self.bls_index.insert(new_bls, peer_id) {
            if old_peer_id != peer_id {
                if let Some(orphan) = self.peers.get_mut(&old_peer_id) {
                    orphan.clear_bls();
                }
            }
        }
    }

    /// Create a peer that is "trusted".
    ///
    /// This overwrites peer records and unbans ips.
    pub(super) fn add_trusted_peer(
        &mut self,
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
        addr: Vec<Multiaddr>,
    ) {
        let peer_id: PeerId = network_key.clone().into();
        let trusted_peer = Peer::new_trusted(bls_public_key, network_key, addr);
        let _ = self.banned_peers.remove_banned_peer(trusted_peer.known_ip_addresses());
        self.rebind_bls(peer_id, bls_public_key);
        self.peers.insert(peer_id, trusted_peer);
    }

    /// Create or update a peer.
    ///
    /// If the peer's BLS key was recorded as unresolved by `new_epoch` (i.e.
    /// `current_committee_keys[bls] == None`), the peer is promoted into `current_committee`,
    /// marked trusted, and any pending ban is lifted. The returned `(PeerId, PeerAction)` carries
    /// the `Unban` action so the caller can flow it through `PeerManager::apply_peer_action`.
    /// `None` is returned when no promotion happened.
    pub(super) fn upsert_peer(
        &mut self,
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
        addrs: Vec<Multiaddr>,
    ) -> Option<(PeerId, PeerAction)> {
        let peer_id: PeerId = network_key.clone().into();
        self.rebind_bls(peer_id, bls_public_key);
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.update_net(bls_public_key, network_key, addrs);
        } else {
            let peer = Peer::new(bls_public_key, network_key, addrs);
            self.peers.insert(peer_id, peer);
        }

        // self-heal: promote an unresolved committee BLS when the kad lookup resolves it.
        let should_promote =
            matches!(self.current_committee_keys.get(&bls_public_key), Some(&None));

        if should_promote {
            self.promote_committee_member(bls_public_key, peer_id).map(|a| (peer_id, a))
        } else {
            None
        }
    }

    /// Handle reported action.
    ///
    /// This method is called when the application layer identifies a problem and reports a peer.
    pub(super) fn process_penalty(&mut self, peer_id: &PeerId, penalty: Penalty) -> PeerAction {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            // Layer 2: committee BLS keys are immune to penalties.
            //
            // During cross-contamination, the real committee member may connect with the
            // correct PeerId but not be the one promoted by new_epoch (bls_index points
            // elsewhere due to a worker-network kad record). The is_trusted flag on this
            // peer may be false, but its BLS key still identifies it as a committee member.
            if let Some(bls_key) = peer.bls_public_key() {
                if self.current_committee_keys.contains_key(&bls_key) {
                    if matches!(penalty, Penalty::Severe | Penalty::Fatal) {
                        warn!(
                            target: "peer-manager",
                            ?peer_id,
                            ?penalty,
                            "blocked penalty against committee member (BLS-based protection)"
                        );
                    }
                    return PeerAction::NoAction;
                }
            }

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
            .map(|peer| *peer.connection_status())
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
                .filter(|ip| !already_banned_ips.contains(ip))
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

    /// Return the BLS public key for a peer, if it is known.
    pub(super) fn peer_to_bls(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.peers.get(peer_id).and_then(|p| p.bls_public_key())
    }

    /// Resolve a BLS public key to its [`PeerId`] and dialable multiaddrs.
    ///
    /// Returns the advertised listening addresses for the peer (the addresses to dial),
    /// not the historical observed multiaddr set.
    pub(super) fn auth_to_peer(&self, bls_key: &BlsPublicKey) -> Option<(PeerId, Vec<Multiaddr>)> {
        let peer_id = self.bls_index.get(bls_key)?;
        let peer = self.peers.get(peer_id)?;
        Some((*peer_id, peer.listening_addrs_clone()))
    }

    /// True if a BLS public key is associated with a known peer.
    pub(super) fn contains_bls(&self, bls_key: &BlsPublicKey) -> bool {
        self.bls_index.contains_key(bls_key)
    }

    /// Boolean indicating if this peer is a validator.
    /// This method will be updated to include nvvs as well.
    pub(super) fn is_peer_validator(&self, peer_id: &PeerId) -> bool {
        self.is_peer_cvv(peer_id)
    }

    /// Boolean indicating if this peer is in the current committee of voting validators.
    fn is_peer_cvv(&self, peer_id: &PeerId) -> bool {
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
            .values()
            .filter_map(|peer| {
                if peer.connection_status().is_connected() {
                    peer.bls_public_key().and_then(|key| peer.exchange_info().map(|ei| (key, ei)))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>()
            .into()
    }

    /// Sort connected peers considering both score and Kademlia routing table status.
    ///
    /// The shuffle ensures peers with equal scores are sorted in a random order. Peers with the
    /// lowest score and are not part of the kademlia table are prioritized.
    pub(super) fn connected_peers_by_score_and_routability(&self) -> Vec<(&PeerId, &Peer)> {
        let mut connected_peers: Vec<_> =
            self.peers.iter().filter(|(_, peer)| peer.connection_status().is_connected()).collect();

        // shuffle here for unbiased tie-breakers
        connected_peers.shuffle(&mut rand::rng());
        // sort by (score, routable) - lowest score first, then non-routable first
        connected_peers.sort_by_key(|(_, peer)| (peer.score(), peer.is_routable()));
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

    /// True if a peer must be preserved by ban/disconnect pruning.
    ///
    /// Current committee validators and any trusted peer (operator-pinned or once-staked)
    /// are skipped by `collect_excess_peers`. This is the single source of truth for
    /// "do not evict" — pruning protection plus the persistent `is_trusted` flag give the
    /// "once-staked, never-forgotten" guarantee without any new permanent flag.
    pub(super) fn is_protected(&self, peer_id: &PeerId, peer: &Peer) -> bool {
        self.current_committee.contains(peer_id) || peer.is_trusted()
    }

    /// Filter peers based on connection status.
    ///
    /// This creates a min-heap with the excess number of peers. Protected peers
    /// (committee members and trusted peers) are skipped entirely so they cannot be
    /// evicted by `prune_banned_peers` or `prune_disconnected_peers`.
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
            // committee/trusted peers are never evicted
            if self.is_protected(peer_id, peer) {
                continue;
            }
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

    /// Remove a peer from `peers` and clean its `bls_index` entry if any.
    ///
    /// This is the only sanctioned way to drop a peer from the store; it preserves the
    /// `bls_index <-> peers` invariant documented at the top of the module.
    fn remove_peer(&mut self, peer_id: &PeerId) -> Option<Peer> {
        let peer = self.peers.remove(peer_id)?;
        if let Some(bls) = peer.bls_public_key() {
            // only drop if the index still points back at this peer; a prior `rebind_bls` may
            // already have detached the entry.
            if self.bls_index.get(&bls) == Some(peer_id) {
                self.bls_index.remove(&bls);
            }
        }
        Some(peer)
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

            if excess_peers.len() < excess {
                warn!(
                    target: "peer-manager",
                    requested = excess,
                    evicted = excess_peers.len(),
                    "banned peer prune capped by protected (committee/trusted) peers"
                );
            }

            for (_, peer_id, ip_addrs) in excess_peers {
                self.remove_peer(&peer_id);
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

            if excess_peers.len() < excess {
                warn!(
                    target: "peer-manager",
                    requested = excess,
                    evicted = excess_peers.len(),
                    "disconnected peer prune capped by protected (committee/trusted) peers"
                );
            }

            // remove peer
            for (_, peer_id, _) in excess_peers {
                self.remove_peer(&peer_id);
                self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
            }
        }
    }

    /// Promote a known peer to committee membership.
    ///
    /// Binds `bls_key → Some(peer_id)` in `current_committee_keys`, inserts the peer into
    /// `current_committee`, marks the peer trusted, drops its IPs from the banned set, and
    /// returns an `Unban` action if the peer was in `Banned` or `Disconnecting { banned: true }`.
    /// Shared between `new_epoch` (BLS resolved up-front) and `upsert_peer` (BLS resolved
    /// later via kad after an initial `new_epoch` miss).
    fn promote_committee_member(
        &mut self,
        bls_key: BlsPublicKey,
        peer_id: PeerId,
    ) -> Option<PeerAction> {
        self.current_committee.insert(peer_id);
        self.current_committee_keys.insert(bls_key, Some(peer_id));

        // the NewConnectionStatus doesn't affect this call
        let status = self.ensure_peer_exists(&peer_id, &NewConnectionStatus::Unbanned);

        let unban_action = match status {
            ConnectionStatus::Disconnecting { banned: true } => {
                warn!(target: "peer-manager", ?peer_id, "unbanning committee member that was disconnecting pending ban");
                Some(self.update_connection_status(&peer_id, NewConnectionStatus::Unbanned))
            }
            ConnectionStatus::Banned { .. } => {
                warn!(target: "peer-manager", ?peer_id, "unbanning banned committee member");
                Some(self.update_connection_status(&peer_id, NewConnectionStatus::Unbanned))
            }
            ConnectionStatus::Disconnecting { banned: false }
            | ConnectionStatus::Disconnected { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Unknown
            | ConnectionStatus::Connected { .. } => None,
        };

        // already ensured peer exists; addresses were populated when the peer was
        // first registered via `upsert_peer` (or earlier `add_known_peer`), so there
        // is no fresh address payload to fold in here — only the trusted flag and
        // the IP-ban cleanup.
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.make_trusted();
            self.banned_peers.remove_validator_ip(&peer_id, peer.known_ip_addresses());
        }

        unban_action
    }

    /// Update committee for the new epoch.
    ///
    /// The committee is tracked to ensure priority on the network. The banned status of any
    /// committee peer is forgiven and IPs associated with the committee node are reset. The
    /// advertised listening addresses are updated and the peer is marked `trusted` so it won't
    /// incur any additional penalties.
    ///
    /// Each committee member is resolved via the `bls_index`. Members not yet known to
    /// `AllPeers` are recorded in `current_committee_keys` with a `None` value and returned
    /// in the second element of the tuple — callers can then issue a kad lookup. When that
    /// lookup later resolves and `upsert_peer` runs, the same promotion logic runs there
    /// (see `promote_committee_member`).
    pub(super) fn new_epoch(
        &mut self,
        committee: HashSet<BlsPublicKey>,
    ) -> (Vec<(PeerId, PeerAction)>, Vec<BlsPublicKey>) {
        // update current committee
        self.current_committee.clear();
        self.current_committee_keys.clear();

        let mut actions = Vec::with_capacity(committee.len());
        let mut unresolved = Vec::new();
        for bls_key in committee {
            let Some(peer_id) = self.bls_index.get(&bls_key).copied() else {
                warn!(target: "peer-manager", ?bls_key, "unknown committee member");
                self.current_committee_keys.insert(bls_key, None);
                unresolved.push(bls_key);
                continue;
            };
            if let Some(action) = self.promote_committee_member(bls_key, peer_id) {
                actions.push((peer_id, action));
            }
        }

        (actions, unresolved)
    }

    /// Check if a peer is eligible for dial attempt.
    ///
    /// This method implicitly evaluates peers which are in the process
    /// of being banned (connected/disconnecting).
    pub(super) fn can_dial(&self, peer_id: &PeerId) -> bool {
        // unknown peers are eligible for dial attempts
        self.peers.get(peer_id).map(|peer| peer.can_dial()).unwrap_or(true)
    }

    /// Update a peer's status in the routing table.
    pub(super) fn update_routing_for_peer(&mut self, peer_id: &PeerId, routable: bool) {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.update_routability(routable)
        }
    }
}
