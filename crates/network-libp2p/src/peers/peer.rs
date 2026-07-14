//! Information shared between peers.

use super::{
    score::{Reputation, ReputationUpdate, Score},
    status::ConnectionStatus,
    types::{ConnectionDirection, TrustBasis},
    Penalty,
};
use libp2p::{
    core::multiaddr::{Multiaddr, Protocol},
    PeerId,
};
use std::{collections::HashSet, net::IpAddr, time::Instant};
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
    /// The peer's score - used to derive [Reputation].
    score: Score,
    /// The multiaddrs associated with this peer: addresses observed on real connections plus any
    /// self-advertised addresses folded in via [`Self::update_net`].
    ///
    /// These drive dialing and are exchanged with peers. They are deliberately NOT the source for
    /// ban accounting: a self-advertised address is attacker-controlled, so counting it toward the
    /// per-IP ban would let a peer poison an unrelated IP. Ban accounting uses
    /// [`Self::observed_ip_addresses`] instead.
    multiaddrs: HashSet<Multiaddr>,
    /// IP addresses this node has actually observed the peer connecting from.
    ///
    /// Populated only by real inbound/outbound connection events ([`Self::register_incoming`] /
    /// [`Self::register_outgoing`]); self-advertised addresses folded in through
    /// [`Self::update_net`] never land here. This is the sole source for
    /// [`Self::known_ip_addresses`] and therefore for the per-IP ban counter, so a peer can only
    /// contribute an IP it genuinely presented on a connection. An attacker cannot get an honest
    /// peer's IP banned by advertising it in a signed record (GHSA-6qcj-p42p-779j).
    observed_ip_addresses: HashSet<IpAddr>,
    /// Connection status of the peer.
    connection_status: ConnectionStatus,
    /// Whether the node operator explicitly allowlisted this peer.
    ///
    /// This is *operator* trust only: it is set at construction and never altered by epoch
    /// rotation. Validator (committee) trust is NOT stored here - it is derived from the
    /// committee sets in `AllPeers` - so the two provenances can never be conflated (issue #715).
    operator_allowlisted: bool,
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
    /// Create a new operator-allowlisted peer.
    pub(super) fn new_trusted(bls_public_key: BlsPublicKey, network_key: NetworkPublicKey) -> Peer {
        Self {
            bls_public_key: Some(bls_public_key),
            network_key: Some(network_key),
            score: Score::new_max(),
            operator_allowlisted: true,
            multiaddrs: Default::default(),
            observed_ip_addresses: Default::default(),
            connection_status: Default::default(),
            connection_direction: Default::default(),
            routable: false,
        }
    }

    /// Create a new (non-allowlisted) peer with its known multiaddrs.
    pub(super) fn new(
        bls_public_key: BlsPublicKey,
        network_key: NetworkPublicKey,
        addrs: Vec<Multiaddr>,
    ) -> Peer {
        Self {
            bls_public_key: Some(bls_public_key),
            network_key: Some(network_key),
            score: Score::default(),
            operator_allowlisted: false,
            multiaddrs: addrs.into_iter().collect(),
            observed_ip_addresses: Default::default(),
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
            operator_allowlisted: false,
            multiaddrs: Default::default(),
            observed_ip_addresses: Default::default(),
            connection_status: Default::default(),
            connection_direction: Default::default(),
            routable: false,
        }
    }

    /// Update keys and merge advertised network addresses.
    ///
    /// The merged addresses are self-advertised (they arrive on a peer record, not on an observed
    /// connection). They are used for dialing and peer exchange only and are never treated as
    /// observed connection IPs, so they do not feed the per-IP ban counter
    /// ([`Self::observed_ip_addresses`] / GHSA-6qcj-p42p-779j).
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
        self.score.reputation()
    }

    /// Return an iterator of the IP addresses this node has observed the peer connecting from.
    ///
    /// Derived only from observed connection addresses ([`Self::observed_ip_addresses`]), never
    /// from self-advertised addresses, so it is safe to use as the per-IP ban-counter source: an
    /// attacker cannot inflate an honest peer's ban count by advertising its IP
    /// (GHSA-6qcj-p42p-779j).
    pub(super) fn known_ip_addresses(&self) -> impl Iterator<Item = IpAddr> + '_ {
        self.observed_ip_addresses.iter().copied()
    }

    /// Extract the IP address carried by a multiaddr, if any.
    fn ip_from_multiaddr(addr: &Multiaddr) -> Option<IpAddr> {
        addr.iter().find_map(|protocol| match protocol {
            Protocol::Ip4(ip) => Some(ip.into()),
            Protocol::Ip6(ip) => Some(ip.into()),
            _ => None, // ignore others
        })
    }

    /// Apply a penalty to the peer's score.
    ///
    /// `exemption` is the peer's [TrustBasis] for the current epoch, if any. Exempt peers
    /// (operator allowlist or committee validators) bypass the score model entirely.
    pub(super) fn apply_penalty(
        &mut self,
        penalty: Penalty,
        exemption: Option<TrustBasis>,
    ) -> Reputation {
        if let Some(basis) = exemption {
            // Exempt peers bypass the score model entirely. Severe/Fatal suppressions are
            // operationally significant: they hint that an exempt peer (committee member or
            // operator allowlist) is misbehaving in ways that would normally ban an untrusted
            // peer. Surface as a warn! so ops can correlate downstream issues with the signal.
            if matches!(penalty, Penalty::Severe | Penalty::Fatal) {
                warn!(
                    target: "peer-manager",
                    ?penalty,
                    ?basis,
                    "skipping severe/fatal penalty for exempt peer"
                );
            }
        } else {
            self.score.apply_penalty(penalty);
        }

        // return new reputation
        self.reputation()
    }

    /// Ensure the peer's status is banned.
    ///
    /// `exemption` is forwarded to [Self::apply_penalty]: an exempt peer (operator allowlist or
    /// committee validator) bypasses the score model, so the `Fatal` here is suppressed and the
    /// peer is not banned - the same protection exempt peers had before.
    pub(super) fn ensure_banned(&mut self, peer_id: &PeerId, exemption: Option<TrustBasis>) {
        match self.reputation() {
            Reputation::Banned => {}
            _ => {
                // if the score isn't low enough to ban, this function has been called incorrectly.
                error!(target: "peer-manager", ?peer_id, "banning a peer with a good score");
                self.apply_penalty(Penalty::Fatal, exemption);
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
        // an observed connection address: record its IP as one the peer genuinely presented, which
        // is the only kind of IP allowed to feed the per-IP ban counter (GHSA-6qcj-p42p-779j)
        if let Some(ip) = Self::ip_from_multiaddr(&multiaddr) {
            self.observed_ip_addresses.insert(ip);
        }
        self.multiaddrs.insert(multiaddr);

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
        // an observed connection address: record its IP as one the peer genuinely presented, which
        // is the only kind of IP allowed to feed the per-IP ban counter (GHSA-6qcj-p42p-779j)
        if let Some(ip) = Self::ip_from_multiaddr(&multiaddr) {
            self.observed_ip_addresses.insert(ip);
        }
        self.multiaddrs.insert(multiaddr);

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

    /// Heartbeat maintenance applies decaying penalty rates to a non-exempt peer's score.
    ///
    /// `exemption` is the peer's [TrustBasis] for the current epoch, if any; exempt peers skip
    /// score decay. The peer's reputation could change. This returns the reputation update for
    /// the manager to react to.
    pub(super) fn heartbeat(&mut self, exemption: Option<TrustBasis>) -> ReputationUpdate {
        if exemption.is_none() {
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

    /// Whether the node operator explicitly allowlisted this peer.
    ///
    /// This is operator trust only and is never affected by epoch rotation. Validator
    /// (committee) trust is derived from the committee sets in `AllPeers`, not stored here.
    pub(super) fn is_operator_allowlisted(&self) -> bool {
        self.operator_allowlisted
    }

    /// Extract relevant information for peer exchange.
    pub(super) fn exchange_info(&self) -> Option<(NetworkPublicKey, HashSet<Multiaddr>)> {
        self.network_key.as_ref().map(|network_key| (network_key.clone(), self.multiaddrs.clone()))
    }

    /// Reset the peer's score to the maximum.
    ///
    /// Called when a peer enters the committee. Trust is not stored on the peer (validator
    /// status is derived from the committee sets), but a committee member's score is primed to
    /// the maximum so that, should it later rotate out and re-enter the score model, it starts
    /// from a clean maximum rather than a stale value.
    pub(super) fn reset_score_to_max(&mut self) {
        self.score = Score::new_max();
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
