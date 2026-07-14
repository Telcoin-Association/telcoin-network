//! Peer service to track known peers.
//!
//! `AllPeers` is responsible for processing updates to peers and returns `PeerAction`s for the
//! manager to take. Some actions are propagated up to the swarm level and affect other behaviors.

use super::{
    banned::BannedPeers,
    peer::Peer,
    score::ReputationUpdate,
    status::ConnectionStatus,
    types::{ConnectionDirection, PeerIdentity, TrustBasis},
    PeerExchangeMap, Penalty,
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
    /// The collection of known peers, keyed by their domain [PeerIdentity].
    ///
    /// Committee/trusted/known peers are keyed by their [BlsPublicKey] (`Confirmed`); anonymous
    /// inbound or kad-dialed peers that have no known bls key yet are keyed by their libp2p
    /// [PeerId] (`Unidentified`).
    peers: HashMap<PeerIdentity, Peer>,
    /// Inbound boundary resolver: maps a libp2p [PeerId] to the [BlsPublicKey] of a `Confirmed`
    /// peer.
    ///
    /// libp2p connection events only carry a [PeerId], so this is the single point where a libp2p
    /// id is translated to the telcoin-domain identity that keys `peers`. It is kept in lockstep
    /// with the `Confirmed` entries in `peers`: an entry exists here iff the peer is stored under
    /// a [PeerIdentity::Confirmed] key.
    bls_by_peer_id: HashMap<PeerId, BlsPublicKey>,
    /// Members of the previous epoch's committee, keyed by [BlsPublicKey].
    ///
    /// Set every epoch from authoritative state so late gossip from the just-completed committee
    /// still counts as validator traffic and those peers are not pruned mid-rotation. Membership
    /// is a consensus-domain fact, so the complete set is stored here even for members whose
    /// libp2p [PeerId] is not yet known.
    previous_committee: HashSet<BlsPublicKey>,
    /// Members of the current epoch's committee, keyed by [BlsPublicKey].
    current_committee: HashSet<BlsPublicKey>,
    /// Members of the next epoch's committee, keyed by [BlsPublicKey].
    ///
    /// Pre-emptively tracked so they are not banned before they begin voting.
    next_committee: HashSet<BlsPublicKey>,
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
            bls_by_peer_id: Default::default(),
            previous_committee: Default::default(),
            current_committee: Default::default(),
            next_committee: Default::default(),
            banned_peers: Default::default(),
            disconnected_peers: 0,
            pending_dials: Default::default(),
            dial_timeout,
            max_banned_peers,
            max_disconnected_peers,
        }
    }

    /// Resolve a libp2p [PeerId] to the [PeerIdentity] used to key the peer collection.
    ///
    /// A peer is `Confirmed` once its bls key is known (committee/trusted/known peers); otherwise
    /// it is `Unidentified` and keyed by its libp2p id.
    fn identity_for(&self, peer_id: &PeerId) -> PeerIdentity {
        self.bls_by_peer_id
            .get(peer_id)
            .map_or(PeerIdentity::Unidentified(*peer_id), |bls_public_key| {
                PeerIdentity::Confirmed(*bls_public_key)
            })
    }

    /// Recover the libp2p [PeerId] for a stored peer.
    ///
    /// `Unidentified` peers carry their id in the key; `Confirmed` peers derive it from their
    /// network key, which is always set whenever a bls key is recorded.
    fn peer_id_for(identity: &PeerIdentity, peer: &Peer) -> Option<PeerId> {
        match identity {
            PeerIdentity::Unidentified(peer_id) => Some(*peer_id),
            PeerIdentity::Confirmed(_) => peer.peer_id(),
        }
    }

    /// Remove a peer from the collection, keeping the `bls_by_peer_id` resolution index in sync.
    fn evict(&mut self, identity: &PeerIdentity) -> Option<Peer> {
        let removed = self.peers.remove(identity);
        if let (PeerIdentity::Confirmed(_), Some(peer)) = (identity, removed.as_ref()) {
            if let Some(peer_id) = peer.peer_id() {
                self.bls_by_peer_id.remove(&peer_id);
            }
        }
        removed
    }

    /// Release the status-counter bookkeeping held by a record dropped outside the normal
    /// status-transition path (displaced by [`Self::add_trusted_peer`] or [`Self::upsert_peer`]).
    ///
    /// `disconnected_peers` counts records in `Disconnected` status and `banned_peers` tracks
    /// records in `Banned` status; a record removed without passing through
    /// [`Self::update_connection_status`] must release them here so pruning math stays accurate.
    fn release_displaced_record(&mut self, displaced: &Peer) {
        match displaced.connection_status() {
            ConnectionStatus::Disconnected { .. } => {
                self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
            }
            ConnectionStatus::Banned { .. } => {
                let _ = self.banned_peers.remove_banned_peer(displaced.known_ip_addresses());
            }
            ConnectionStatus::Connected { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Disconnecting { .. }
            | ConnectionStatus::Unknown => {}
        }
    }

    /// Normalize the transport-liveness status of a record carried across a network-key
    /// rotation.
    ///
    /// Reputation (score, trust, ban) is a property of the domain identity and survives the
    /// rotation, but `Connected`/`Dialing`/`Disconnecting` describe the rotated-away key's
    /// connection, which no longer belongs to this record. Left in place they wedge the new
    /// identity: `can_dial` reports false and dial attempts short-circuit with
    /// `AlreadyConnected` even though no connection exists for the new peer id, and no
    /// libp2p event for the new id ever corrects the record. Live transport states map onto
    /// `Disconnected` (or `Banned` when a ban was pending) with matching counter bookkeeping.
    fn normalize_carried_status(&mut self, peer: &mut Peer) {
        match *peer.connection_status() {
            ConnectionStatus::Connected { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Disconnecting { banned: false } => {
                peer.set_connection_status(ConnectionStatus::Disconnected {
                    instant: Instant::now(),
                });
                self.disconnected_peers = self.disconnected_peers.saturating_add(1);
            }
            ConnectionStatus::Disconnecting { banned: true } => {
                peer.set_connection_status(ConnectionStatus::Banned { instant: Instant::now() });
                self.banned_peers.add_banned_peer(peer);
            }
            ConnectionStatus::Disconnected { .. }
            | ConnectionStatus::Banned { .. }
            | ConnectionStatus::Unknown => {}
        }
    }

    /// Create a peer that is "trusted".
    ///
    /// This overwrites peer records and unbans ips.
    pub(super) fn add_trusted_peer(
        &mut self,
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
    ) {
        let peer_id: PeerId = network_key.clone().into();
        let confirmed = PeerIdentity::Confirmed(bls_public_key);
        let current = self.identity_for(&peer_id);
        // overwrite any prior record: anonymous under this peer id, confirmed under a different
        // bls key that previously presented this network key, or confirmed under a previous
        // network key; each displaced record is removed through `evict` / released so no
        // `bls_by_peer_id` entry or status counter goes stale
        if let Some(displaced) = (current != confirmed).then(|| self.evict(&current)).flatten() {
            self.release_displaced_record(&displaced);
        }
        if let Some(displaced) = self.evict(&confirmed) {
            self.release_displaced_record(&displaced);
        }
        self.bls_by_peer_id.insert(peer_id, bls_public_key);
        self.peers.insert(confirmed, Peer::new_trusted(bls_public_key, network_key));
    }

    /// Create a peer.
    pub(super) fn upsert_peer(
        &mut self,
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
        addrs: Vec<Multiaddr>,
    ) {
        let peer_id: PeerId = network_key.clone().into();
        let confirmed = PeerIdentity::Confirmed(bls_public_key);
        let current = self.identity_for(&peer_id);
        // network-key rotation: a record already stored under the confirmed identity is not
        // reachable from the new peer id, so displace it through `evict` to clear the
        // `bls_by_peer_id` entry derived from its previous network key
        let rotated = (current != confirmed).then(|| self.evict(&confirmed)).flatten();
        // migrate any record reachable from the new peer id (anonymous-inbound promotion or a
        // repeat upsert); a rotated record it displaces releases its counter bookkeeping
        // NOTE: when the anonymous record wins this merge it keeps its fresh score, so a banned
        // peer that reconnects anonymously before its kad record arrives sheds its ban here
        // (pre-existing semantics); carrying reputation across the merge is a possible follow-up
        let migrated = self.peers.remove(&current);
        if let (Some(_), Some(displaced)) = (migrated.as_ref(), rotated.as_ref()) {
            self.release_displaced_record(displaced);
        }
        // otherwise the rotated record carries forward (same domain peer, new transport key),
        // preserving its accumulated reputation; its transport status describes the old key's
        // connection, so it is normalized onto the new identity; a peer never seen before
        // starts fresh
        let carried = migrated.is_none();
        let mut peer = migrated
            .or(rotated)
            .unwrap_or_else(|| Peer::new(bls_public_key, network_key.clone(), Vec::new()));
        // apply the rotated-to keys and advertised addresses to the carried record. when the
        // record is mid-ban (`Disconnecting { banned: true }`), `normalize_carried_status` then
        // completes the ban through `add_banned_peer`, which reads `known_ip_addresses` - the IPs
        // this node has *observed* the peer connecting from. Those observed IPs live on the carried
        // record and survive the rotation regardless of this call; the addresses merged here are
        // self-advertised and feed dialing / peer exchange only, never the per-IP ban counter
        // (GHSA-6qcj-p42p-779j).
        peer.update_net(bls_public_key, network_key, addrs);
        if carried {
            self.normalize_carried_status(&mut peer);
        }
        self.bls_by_peer_id.insert(peer_id, bls_public_key);
        self.peers.insert(confirmed, peer);
    }

    /// Handle reported action.
    ///
    /// This method is called when the application layer identifies a problem and reports a peer.
    pub(super) fn process_penalty(&mut self, peer_id: &PeerId, penalty: Penalty) -> PeerAction {
        let id = self.identity_for(peer_id);
        let exemption = self.trust_basis_for(&id);
        if let Some(peer) = self.peers.get_mut(&id) {
            let prior_reputation = peer.reputation();
            let new_reputation = peer.apply_penalty(penalty, exemption);
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
        let resolved = self.identity_for(peer_id);
        // self-heal a stale resolution-index entry: a confirmed identity with no backing record
        // violates the `bls_by_peer_id` iff-invariant; repair the index and treat the peer as
        // unidentified so a keyless default record is never stored under a confirmed key (such
        // a record has no recoverable peer id, so `evict` could never clear its index entry)
        let id = if matches!(resolved, PeerIdentity::Confirmed(_))
            && !self.peers.contains_key(&resolved)
        {
            error!(target: "peer-manager", ?peer_id, "bls_by_peer_id entry found without a confirmed record - repairing index");
            self.bls_by_peer_id.remove(peer_id);
            PeerIdentity::Unidentified(*peer_id)
        } else {
            resolved
        };
        if !self.peers.contains_key(&id) {
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
            self.peers.insert(id, Peer::default());
        }

        // ensure peer is banned if the new state is Banned
        if matches!(new_status, &NewConnectionStatus::Banned) {
            let exemption = self.trust_basis(peer_id);
            if let Some(peer) = self.peers.get_mut(&id) {
                peer.ensure_banned(peer_id, exemption);
            } else {
                // unreachable
                error!(target: "peer-manager", ?peer_id, "impossible - peer was just created if it didn't already exist");
            }
        }

        self.peers
            .get(&id)
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
        let peers_to_disconnect: Vec<PeerId> = self
            .peers
            .iter()
            .filter_map(|(id, info)| {
                if let ConnectionStatus::Dialing { instant } = info.connection_status() {
                    if (*instant) + self.dial_timeout < Instant::now() {
                        return Self::peer_id_for(id, info);
                    }
                }
                None
            })
            .collect();

        // disconnect peers
        for peer_id in peers_to_disconnect {
            self.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

            // these peers exceeded `dial_timeout` while still `Dialing`, so the genuine cause
            // is a timeout. Report it to the caller (the disconnect transition no longer
            // notifies; the dialer's reply channel is only consumed here or in `on_dial_failure`).
            self.notify_dial_result(
                &peer_id,
                Err(NetworkError::Dial("dial attempt timedout".to_string())),
            );
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
        let previous_committee = &self.previous_committee;
        let current_committee = &self.current_committee;
        let next_committee = &self.next_committee;
        let unbanned_peers = self.peers.iter_mut().filter_map(|(id, peer)| {
            let exemption =
                Self::exemption(id, peer, previous_committee, current_committee, next_committee);
            let update = peer.heartbeat(exemption);
            match update {
                ReputationUpdate::Unbanned => {
                    Self::peer_id_for(id, peer)
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
                let id = self.identity_for(peer_id);
                if let Some(peer) = self.peers.get_mut(&id) {
                    peer.set_connection_status(ConnectionStatus::Disconnected {
                        instant: Instant::now(),
                    });
                }

                // NOTE: the dial result (if any) is notified by the caller that knows the real
                // cause: `on_dial_failure` delivers the genuine `DialError`, and
                // `heartbeat_maintenance` reports a timeout. This transition must not report a
                // hardcoded cause, which previously consumed the reply channel before the real
                // error could be sent.
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
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
        self.peers.get(&self.identity_for(peer_id))
    }

    /// Resolve a connected/known libp2p [PeerId] to its [BlsPublicKey], if confirmed.
    ///
    /// Backed by the `bls_by_peer_id` index, which is populated whenever a peer is stored under a
    /// `Confirmed` identity. Returns `None` for peers whose bls key has not been learned yet.
    pub(super) fn bls_for_peer(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.bls_by_peer_id.get(peer_id).copied()
    }

    /// Boolean indicating if this peer is a validator in the previous, current, or next committee.
    ///
    /// Membership spans all three tracked committees so peers from the just-completed epoch are not
    /// pruned while late gossip may still arrive, and next-epoch peers are protected before they
    /// begin voting. (NVV support remains future work.)
    pub(super) fn is_peer_validator(&self, peer_id: &PeerId) -> bool {
        match self.identity_for(peer_id) {
            PeerIdentity::Confirmed(bls_public_key) => {
                self.previous_committee.contains(&bls_public_key)
                    || self.current_committee.contains(&bls_public_key)
                    || self.next_committee.contains(&bls_public_key)
            }
            PeerIdentity::Unidentified(_) => false,
        }
    }

    /// The [TrustBasis] exempting `peer` from the score model this epoch, if any.
    ///
    /// Validator status is derived live from the three tracked committee slots (it is never
    /// stored on the peer, so it cannot drift out of sync with rotation); operator allowlisting
    /// is read from the peer. Validator takes precedence in the reported basis as it is the
    /// operationally significant signal when a penalty is suppressed.
    fn exemption(
        id: &PeerIdentity,
        peer: &Peer,
        previous_committee: &HashSet<BlsPublicKey>,
        current_committee: &HashSet<BlsPublicKey>,
        next_committee: &HashSet<BlsPublicKey>,
    ) -> Option<TrustBasis> {
        let in_committee = |bls_public_key: &BlsPublicKey| {
            previous_committee.contains(bls_public_key)
                || current_committee.contains(bls_public_key)
                || next_committee.contains(bls_public_key)
        };
        match id {
            PeerIdentity::Confirmed(bls_public_key) if in_committee(bls_public_key) => {
                Some(TrustBasis::Validator)
            }
            PeerIdentity::Confirmed(_) | PeerIdentity::Unidentified(_) => {
                peer.is_operator_allowlisted().then_some(TrustBasis::Operator)
            }
        }
    }

    /// The [TrustBasis] exempting the peer identified by `peer_id`, if it is known and exempt.
    ///
    /// `None` means the peer is subject to the normal score model.
    fn trust_basis(&self, peer_id: &PeerId) -> Option<TrustBasis> {
        let id = self.identity_for(peer_id);
        self.trust_basis_for(&id)
    }

    /// The [TrustBasis] exempting the peer with the already-resolved identity `id`, if it is
    /// known and exempt.
    fn trust_basis_for(&self, id: &PeerIdentity) -> Option<TrustBasis> {
        self.peers.get(id).and_then(|peer| {
            Self::exemption(
                id,
                peer,
                &self.previous_committee,
                &self.current_committee,
                &self.next_committee,
            )
        })
    }

    /// Boolean indicating if the ip address is associated with a banned peer.
    pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers.ip_banned(ip)
    }

    /// Boolean indicating if a peer id is banned or associated with any banned ip addresses.
    /// NOTE: the peer can still be in a connected status but pending a ban, so the connection
    /// status is not used.
    pub(super) fn peer_banned(&self, peer_id: &PeerId) -> bool {
        self.get_peer(peer_id).is_some_and(|peer| {
            peer.reputation().banned() || peer.known_ip_addresses().any(|ip| self.ip_banned(&ip))
        })
    }

    /// Gives the ids of all known connected peers.
    pub(super) fn connected_peer_ids(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(|(id, peer)| {
            peer.connection_status()
                .is_connected()
                .then_some(())
                .and_then(|()| Self::peer_id_for(id, peer))
        })
    }

    /// Return an iterator of peers that are connected or dialed.
    pub(super) fn connected_or_dialing_peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(id, peer)| {
                let status = peer.connection_status();
                (status.is_connected() || status.is_dialing())
                    .then_some(())
                    .and_then(|()| Self::peer_id_for(id, peer))
            })
            .collect()
    }

    /// Returns a boolean indicating if a peer is already connected or disconnecting.
    ///
    /// Used when handling connection closed events from the swarm.
    pub(super) fn is_peer_connected_or_disconnecting(&self, peer_id: &PeerId) -> bool {
        self.get_peer(peer_id).is_some_and(|peer| {
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
    pub(super) fn connected_peers_by_score_and_routability(&self) -> Vec<(PeerId, &Peer)> {
        let mut connected_peers: Vec<(PeerId, &Peer)> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.connection_status().is_connected())
            .filter_map(|(id, peer)| Self::peer_id_for(id, peer).map(|peer_id| (peer_id, peer)))
            .collect();

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

    /// Filter peers by connection status and return the `excess` OLDEST of them to prune.
    ///
    /// Builds a bounded max-heap keyed by `instant`, so the heap's top is the NEWEST collected
    /// peer. Once the heap is full, a candidate replaces that top only when the candidate is
    /// older, so the heap converges on the `excess` oldest peers. Callers evict exactly these
    /// entries, which keeps the freshest bans/disconnects and drops only stale ones (issue #799).
    /// Used by Self::prune_banned_peers and Self::prune_disconnected_peers.
    fn collect_excess_peers<F>(
        &self,
        excess: usize,
        filter: F,
    ) -> BinaryHeap<(Instant, PeerIdentity, Vec<IpAddr>)>
    where
        F: Fn(&ConnectionStatus) -> Option<Instant>,
    {
        // collection of peers to prune (the oldest `excess`)
        let mut excess_peers = BinaryHeap::with_capacity(excess);

        for (id, peer) in &self.peers {
            if let Some(instant) = filter(peer.connection_status()) {
                // max-heap by instant: the heap's top (peek) is the NEWEST collected peer
                let entry = (instant, *id, peer.known_ip_addresses().collect::<Vec<_>>());

                if excess_peers.len() < excess {
                    // fill the heap until `excess` elements
                    excess_peers.push(entry);
                } else if let Some(current_newest) = excess_peers.peek() {
                    // retain the OLDEST `excess`: drop the newest held entry whenever an older
                    // candidate appears, so recently banned/disconnected peers survive pruning
                    if entry.0 < current_newest.0 {
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

            for (_, id, ip_addrs) in excess_peers {
                let peer_id = self.evict(&id).and_then(|peer| Self::peer_id_for(&id, &peer));
                let ips = self.banned_peers.remove_banned_peer(ip_addrs.clone().into_iter());
                if let Some(peer_id) = peer_id {
                    unbanned.push((peer_id, ips));
                }
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
            for (_, id, _) in excess_peers {
                self.evict(&id);
                self.disconnected_peers = self.disconnected_peers.saturating_sub(1);
            }
        }
    }

    /// Set the previous/current/next committee slots directly from authoritative state.
    ///
    /// Called every epoch with the three committees read from the persisted epoch records. All
    /// three slots are overwritten (no positional rotation), so `current` and `previous` are always
    /// re-validated against authoritative state rather than derived from a prior prediction.
    ///
    /// The complete committee sets are stored directly, keyed by [BlsPublicKey]. Committee
    /// membership is a consensus-domain fact that is always known, so a member whose libp2p
    /// [PeerId] has not been discovered yet is still retained in its slot and counts as a
    /// validator; the unban pass for it then runs lazily once discovery confirms its network
    /// identity (see [`Self::apply_membership_if_committee`]).
    ///
    /// No trust flag is stored on peers: a member's validator exemption is derived live from the
    /// three committee slots (issue #715), so overwriting the slots is itself the demotion - a
    /// member absent from all three slots re-enters the normal score model immediately, while
    /// operator-allowlisted peers keep their exemption regardless. Members of the new committees
    /// whose network identity is known are forgiven any bans and have their scores primed to max;
    /// a member appearing in more than one committee is processed once.
    pub(super) fn update_committees(
        &mut self,
        previous: HashSet<BlsPublicKey>,
        current: HashSet<BlsPublicKey>,
        next: HashSet<BlsPublicKey>,
    ) -> Vec<(PeerId, PeerAction)> {
        // the union of the three new committees
        let new_union: HashSet<BlsPublicKey> =
            previous.iter().chain(current.iter()).chain(next.iter()).copied().collect();

        // store the complete sets directly; members whose PeerId is not yet known are retained
        // (the gap fix) and trusted lazily once discovery confirms their network identity
        self.previous_committee = previous;
        self.current_committee = current;
        self.next_committee = next;

        // unban every member with a known network identity once and prime its score; unknown
        // members are handled lazily on discovery. members that fell out of all three slots
        // need no demotion: their validator exemption derives from the slots just overwritten
        self.apply_committee_membership(new_union)
    }

    /// Forgive bans for a committee's members WITHOUT touching the committee slots.
    ///
    /// Used only by the deadlock-breaker pre-dial path: it unbans committee peers so a subsequent
    /// dial loop can connect, but leaves previous/current/next untouched because the real slot
    /// update follows shortly after via `update_committees`.
    ///
    /// Because validator exemption is derived from the slots, members not already in a slot are
    /// NOT penalty-exempt during this window (intentional: exemption only ever follows
    /// authoritative slot state). They are unbanned with scores primed to max, and the exemption
    /// begins when `update_committees` writes the slots.
    pub(super) fn mark_committee_for_dial(
        &mut self,
        committee: HashSet<BlsPublicKey>,
    ) -> Vec<(PeerId, PeerAction)> {
        self.apply_committee_membership(committee)
    }

    /// Whether `bls_key` sits in any tracked committee slot (previous, current, or next).
    ///
    /// Committee membership is set every epoch from authoritative consensus state, so this is the
    /// authority on whether a peer record is worth retaining: a key in no slot is neither a
    /// current, outgoing, nor incoming validator. Used to bound the peer manager's `known_peers`
    /// cache against records for arbitrary keys (see
    /// [`super::manager::PeerManager::add_discovered_peer`]).
    pub(super) fn is_committee_member(&self, bls_key: &BlsPublicKey) -> bool {
        self.previous_committee.contains(bls_key)
            || self.current_committee.contains(bls_key)
            || self.next_committee.contains(bls_key)
    }

    /// Lazily apply committee membership to a single member the moment its network identity is
    /// learned.
    ///
    /// Called from the discovery path ([`super::manager::PeerManager::add_known_peer`]) after a
    /// peer is re-keyed onto its `Confirmed` identity. If the member belongs to any tracked
    /// committee slot it is unbanned and its score primed immediately, closing the window for
    /// members that were tracked by [`Self::update_committees`] before their [PeerId] was known
    /// (the validator exemption itself derives from the slots, so it already applies). A no-op
    /// for peers that are not in any tracked committee.
    pub(super) fn apply_membership_if_committee(
        &mut self,
        bls_key: BlsPublicKey,
    ) -> Vec<(PeerId, PeerAction)> {
        if self.is_committee_member(&bls_key) {
            self.apply_committee_membership(std::iter::once(bls_key))
        } else {
            Vec::new()
        }
    }

    /// Forgive bans and prime the score of each committee member with a known network identity.
    ///
    /// Operates per [BlsPublicKey] against the existing `Confirmed` peer records: a member with no
    /// record yet (its libp2p [PeerId] has not been discovered) is skipped here and handled later
    /// via [`Self::apply_membership_if_committee`]. For members with a record, the banned status
    /// is forgiven, IPs associated with the committee node are reset, and the peer's score is
    /// reset to max. No trust flag is stored: the member's validator exemption is derived from
    /// the committee slots, so it incurs no penalties while tracked (members of the pre-dial
    /// path may not be in a slot yet; their exemption begins once `update_committees` lands).
    /// Returns the unban actions for the manager to apply; the committee slots are owned by the
    /// callers.
    fn apply_committee_membership(
        &mut self,
        members: impl IntoIterator<Item = BlsPublicKey>,
    ) -> Vec<(PeerId, PeerAction)> {
        let mut actions = Vec::new();
        for bls_key in members {
            let identity = PeerIdentity::Confirmed(bls_key);
            // only members whose network identity is already known have a confirmed record and a
            // recoverable peer id; others are trusted lazily on discovery
            let Some(peer_id) = self.peers.get(&identity).and_then(|peer| peer.peer_id()) else {
                continue;
            };

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
                    warn!(target: "peer-manager", ?peer_id, "unbanning banned committee member");
                    let action =
                        self.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);
                    actions.push((peer_id, action));
                }
                ConnectionStatus::Disconnected { .. }
                | ConnectionStatus::Dialing { .. }
                | ConnectionStatus::Unknown
                | ConnectionStatus::Connected { .. } => { /* nothing to do */ }
            }

            // update peer regardless of connection status; validator trust is derived from the
            // committee slots, so we only prime the score (no trust flag is stored)
            if let Some(peer) = self.peers.get_mut(&identity) {
                peer.reset_score_to_max();
                self.banned_peers.remove_validator_ip(&peer_id, peer.known_ip_addresses());
            }
        }

        // return any unban actions for committee peers
        actions
    }

    /// Check if a peer is eligible for dial attempt.
    ///
    /// This method implicitly evaluates peers which are in the process
    /// of being banned (connected/disconnecting).
    pub(super) fn can_dial(&self, peer_id: &PeerId) -> bool {
        // unknown peers are eligible for dial attempts
        self.get_peer(peer_id).map(|peer| peer.can_dial()).unwrap_or(true)
    }

    /// Update a peer's status in the routing table.
    pub(super) fn update_routing_for_peer(&mut self, peer_id: &PeerId, routable: bool) {
        let id = self.identity_for(peer_id);
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.update_routability(routable)
        }
    }

    /// Test-only: mutable access to a peer resolved by its libp2p [PeerId].
    #[cfg(test)]
    pub(super) fn get_peer_mut(&mut self, peer_id: &PeerId) -> Option<&mut Peer> {
        let id = self.identity_for(peer_id);
        self.peers.get_mut(&id)
    }

    /// Test-only: insert a peer keyed by its libp2p [PeerId] (an `Unidentified` peer).
    #[cfg(test)]
    pub(super) fn insert_unidentified(&mut self, peer_id: PeerId, peer: Peer) {
        self.peers.insert(PeerIdentity::Unidentified(peer_id), peer);
    }
}
