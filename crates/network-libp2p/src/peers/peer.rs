//! Information shared between peers.

use super::{
    score::{Reputation, ReputationUpdate, Score},
    status::ConnectionStatus,
    types::ConnectionDirection,
    Penalty,
};
use libp2p::{
    core::multiaddr::{Multiaddr, Protocol},
    PeerId,
};
use std::{collections::HashSet, net::IpAddr, time::Instant};
use tn_config::PeerConfig;
use tn_types::{BlsPublicKey, NetworkPublicKey};
use tracing::{error, warn};

/// Information about a given connected peer.
/// Note that bls_public_key and network_key are Optional.
/// It is possible we need to track a peer before we have network settings.
/// These are only used for peer exchange and if not set then this peer will not
/// be exchaged (which is fine since we don't have this info yet).
#[derive(Clone, Debug, Default)]
pub(super) struct Peer {
    /// The peers Bls public key.
    bls_public_key: Option<BlsPublicKey>,
    /// The peers network public key (libp2p public key).
    network_key: Option<NetworkPublicKey>,
    /// The config
    config: PeerConfig,
    /// The peer's score - used to derive [Reputation].
    score: Score,
    /// The multiaddrs this node has witnessed the peer using.
    ///
    /// These are used to manage the banning process and are exchanged with peers.
    multiaddrs: HashSet<Multiaddr>,
    /// Connection status of the peer.
    connection_status: ConnectionStatus,
    /// Trusted peers are specifically included by node operators.
    is_trusted: bool,
    /// Direction of the most recent connection with this peer.
    ///
    /// `None` if this peer was never connected.
    connection_direction: Option<ConnectionDirection>,
    /// Indicates if the peer is part of the node's kademlia routing table.
    ///
    /// Routable peers are used to query kad records and are prioritized connections. Peer manager
    /// prioritizes non-routable peers during connection limit pruning. If a peer is not in the
    /// routing table and this node needs to prune connections, then the peer may be disconnected.
    routable: bool,
}

impl Peer {
    /// Create a new trusted peer.
    pub(super) fn new_trusted(bls_public_key: BlsPublicKey, network_key: NetworkPublicKey) -> Peer {
        Self {
            bls_public_key: Some(bls_public_key),
            network_key: Some(network_key),
            score: Score::new_max(),
            is_trusted: true,
            config: Default::default(),
            multiaddrs: Default::default(),
            connection_status: Default::default(),
            connection_direction: Default::default(),
            routable: false,
        }
    }

    /// Create a new trusted peer.
    pub(super) fn new(bls_public_key: BlsPublicKey, network_key: NetworkPublicKey) -> Peer {
        Self {
            bls_public_key: Some(bls_public_key),
            network_key: Some(network_key),
            score: Score::default(),
            is_trusted: false,
            config: Default::default(),
            multiaddrs: Default::default(),
            connection_status: Default::default(),
            connection_direction: Default::default(),
            routable: false,
        }
    }

    #[cfg(test)]
    pub(super) fn default_for_test() -> Self {
        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{BlsKeypair, NetworkKeypair};
        let mut rng = StdRng::from_seed([0; 32]);
        let bls_public_key = *BlsKeypair::generate(&mut rng).public();
        let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        Self {
            bls_public_key: Some(bls_public_key),
            network_key: Some(network_key),
            score: Score::new_max(),
            is_trusted: false,
            config: Default::default(),
            multiaddrs: Default::default(),
            connection_status: Default::default(),
            connection_direction: Default::default(),
            routable: false,
        }
    }

    /// Update keys and network address.
    pub(super) fn update_net(
        &mut self,
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
        multiaddrs: Vec<Multiaddr>,
    ) {
        self.bls_public_key = Some(bls_public_key);
        self.network_key = Some(network_key);
        self.multiaddrs.extend(multiaddrs);
    }

    /// This peers Bls public key.
    pub(super) fn bls_public_key(&self) -> Option<BlsPublicKey> {
        self.bls_public_key
    }

    /// This peer's libp2p [PeerId], derived from its network public key.
    ///
    /// Returns `None` if the network key is not yet known. The derivation is a pure,
    /// total function of the network key, so any peer with a recorded bls key (which is
    /// always set alongside the network key) also has a recoverable [PeerId].
    pub(super) fn peer_id(&self) -> Option<PeerId> {
        self.network_key.as_ref().map(|network_key| network_key.clone().into())
    }

    /// Return a peer's reputation based on the aggregate score.
    pub(super) fn reputation(&self) -> Reputation {
        match self.score.aggregate_score() {
            score if score <= self.config.min_score_for_ban => Reputation::Banned,
            score if score <= self.config.min_score_for_disconnect => Reputation::Disconnected,
            _ => Reputation::Trusted,
        }
    }

    /// Return an iterator of known ip addresses for a peer.
    pub(super) fn known_ip_addresses(&self) -> impl Iterator<Item = IpAddr> + '_ {
        self.multiaddrs.iter().filter_map(|addr| {
            addr.iter().find_map(|protocol| {
                match protocol {
                    Protocol::Ip4(ip) => Some(ip.into()),
                    Protocol::Ip6(ip) => Some(ip.into()),
                    _ => None, // ignore others
                }
            })
        })
    }

    /// Apply a penalty to the peer's score.
    pub(super) fn apply_penalty(&mut self, penalty: Penalty) -> Reputation {
        if self.is_trusted {
            // Trusted peers (current-epoch committee, configured allowlist) bypass
            // the score model entirely. Severe/Fatal suppressions are operationally
            // significant: they hint that a committee member is misbehaving in ways
            // that would normally ban an untrusted peer. Surface as a warn! so ops
            // can correlate downstream issues with the original signal.
            if matches!(penalty, Penalty::Severe | Penalty::Fatal) {
                warn!(
                    target: "peer-manager",
                    ?penalty,
                    "skipping severe/fatal penalty for trusted peer"
                );
            }
        } else {
            self.score.apply_penalty(penalty);
        }

        // return new reputation
        self.reputation()
    }

    /// Ensure the peer's status is banned.
    pub(super) fn ensure_banned(&mut self, peer_id: &PeerId) {
        match self.reputation() {
            Reputation::Banned => {}
            _ => {
                // if the score isn't low enough to ban, this function has been called incorrectly.
                error!(target: "peer-manager", ?peer_id, "banning a peer with a good score");
                self.apply_penalty(Penalty::Fatal);
            }
        }
    }

    /// Sets the connection status.
    pub(super) fn set_connection_status(&mut self, connection_status: ConnectionStatus) {
        self.connection_status = connection_status
    }

    /// Return a reference to the peer's current connection status.
    pub(super) fn connection_status(&self) -> &ConnectionStatus {
        &self.connection_status
    }

    /// Return a reference to the peer's accumulated [Score].
    pub(super) fn score(&self) -> &Score {
        &self.score
    }

    /// Register the dialing peer as connected.
    ///
    /// This method also updates the number of incoming connections +1.
    pub(super) fn register_incoming(&mut self, multiaddr: Multiaddr) {
        self.multiaddrs.insert(multiaddr.clone());

        match &mut self.connection_status {
            ConnectionStatus::Connected { num_in, .. } => *num_in += 1,
            ConnectionStatus::Disconnected { .. }
            | ConnectionStatus::Banned { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Disconnecting { .. }
            | ConnectionStatus::Unknown => {
                self.connection_status = ConnectionStatus::Connected { num_in: 1, num_out: 0 };
                self.connection_direction = Some(ConnectionDirection::Incoming);
            }
        }
    }

    /// Register the dialed peer as connected.
    ///
    /// This method also updates the number of outgoing connections +1.
    pub(super) fn register_outgoing(&mut self, multiaddr: Multiaddr) {
        self.multiaddrs.insert(multiaddr.clone());

        match &mut self.connection_status {
            ConnectionStatus::Connected { num_out, .. } => *num_out += 1,
            ConnectionStatus::Disconnected { .. }
            | ConnectionStatus::Banned { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Disconnecting { .. }
            | ConnectionStatus::Unknown => {
                self.connection_status = ConnectionStatus::Connected { num_in: 0, num_out: 1 };
                self.connection_direction = Some(ConnectionDirection::Outgoing);
            }
        }
    }

    /// Register the peer's status as Dialing
    /// Returns an error if the current state is unexpected.
    pub(super) fn register_dialing(&mut self) -> Result<(), &'static str> {
        match &mut self.connection_status {
            ConnectionStatus::Connected { .. } => return Err("Dialing connected peer"),
            ConnectionStatus::Dialing { .. } => return Err("Dialing an already dialing peer"),
            ConnectionStatus::Disconnecting { .. } => return Err("Dialing a disconnecting peer"),
            ConnectionStatus::Disconnected { .. }
            | ConnectionStatus::Banned { .. }
            | ConnectionStatus::Unknown => {}
        }
        self.connection_status = ConnectionStatus::Dialing { instant: Instant::now() };
        Ok(())
    }

    /// True if this peer can be dialed in it's current state.
    ///
    /// This method implicitly evaluates peers which are in the process
    /// of being banned (connected/disconnecting).
    pub(super) fn can_dial(&self) -> bool {
        match self.connection_status {
            ConnectionStatus::Disconnecting { banned } => !banned,
            ConnectionStatus::Connected { .. }
            | ConnectionStatus::Dialing { .. }
            | ConnectionStatus::Banned { .. } => false,
            ConnectionStatus::Disconnected { .. } | ConnectionStatus::Unknown => true,
        }
    }

    /// Filter banned peer's ip addresses against already known banned ip addresses.
    pub(super) fn filter_new_ips_to_ban(
        &self,
        already_banned_ips: &HashSet<IpAddr>,
    ) -> Vec<IpAddr> {
        self.known_ip_addresses().filter(|ip| !already_banned_ips.contains(ip)).collect::<Vec<_>>()
    }

    /// Heartbeat maintenance applies decaying penalty rates to a non-trusted peer's score.
    ///
    /// The peer's reputation could change. This returns reputation update for the manager to react.
    pub(super) fn heartbeat(&mut self) -> ReputationUpdate {
        if !self.is_trusted {
            let prev_reputation = self.reputation();
            self.score.update();
            let new_reputation = self.reputation();

            match new_reputation {
                Reputation::Trusted => {
                    if prev_reputation.banned() {
                        return ReputationUpdate::Unbanned;
                    }
                }
                Reputation::Disconnected => {
                    if prev_reputation.banned() {
                        return ReputationUpdate::Unbanned;
                    } else if self.connection_status.is_connected_or_dialing() {
                        // disconnect if the peer is connected or dialing
                        return ReputationUpdate::Disconnect;
                    }
                    // otherwise, peer was healthy and disconnected now
                }
                Reputation::Banned => {
                    if !prev_reputation.banned() {
                        return ReputationUpdate::Banned;
                    }
                }
            }
        }

        // all other updates are no-op
        ReputationUpdate::None
    }

    /// Boolean indicating if the peer is trusted.
    pub(super) fn is_trusted(&self) -> bool {
        self.is_trusted
    }

    /// Extract relevant information for peer exchange.
    pub(super) fn exchange_info(&self) -> Option<(NetworkPublicKey, HashSet<Multiaddr>)> {
        self.network_key.as_ref().map(|network_key| (network_key.clone(), self.multiaddrs.clone()))
    }

    /// Update a peer record to make it trusted.
    pub(super) fn make_trusted(&mut self) {
        if !self.is_trusted {
            self.is_trusted = true;
            self.score = Score::new_max();
        }
    }

    /// Revoke a peer's trusted status, returning it to the normal score model.
    ///
    /// Called when a peer rotates out of all three committee slots. Only the flag is cleared;
    /// the score is left as-is so the demoted peer gets a soft landing (heartbeat decay and
    /// penalties resume immediately now that `is_trusted` is false).
    ///
    /// NOTE: `is_trusted` currently conflates committee-derived trust with operator-allowlist
    /// trust (`new_trusted`/`add_trusted_peer`); demoting clears both for a peer that was in a
    /// committee.
    pub(super) fn make_untrusted(&mut self) {
        self.is_trusted = false;
    }

    /// Update peer record to indicate participation in kad as a routable peer.
    pub(super) fn update_routability(&mut self, routable: bool) {
        self.routable = routable;
    }

    /// Bool indicating if the peer is a known participant in kademlia routing table.
    pub(super) fn is_routable(&self) -> bool {
        self.routable
    }
}
