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

/// Maximum number of distinct multiaddrs retained for a single peer.
///
/// A peer's address set is fed both by witnessed connections (`register_incoming` /
/// `register_outgoing`) and by the peer's own self-advertised kad `NodeRecord`s (`update_net`).
/// A non-committee observer can republish its own signed record without bound (a record with a
/// fresher timestamp is always accepted), so the advertised path is attacker-sustained. Capping
/// the set keeps a single peer entry bounded in memory and keeps the peer-exchange payload built
/// from it (`exchange_info`) bounded (GHSA-29v6-gvv5-45gx).
///
/// The protocol assumes exactly one address per peer: a `NodeRecord` advertises one address
/// (`NodeRecord::build`), a committee entry carries one (`network_address`), and every consumer
/// acts on a single address for a peer. The set therefore holds only the address the peer most
/// recently presented. The set is keyed on exact `Multiaddr` equality and one endpoint appears
/// in two syntactic forms, with and without the `/p2p/<peer_id>` suffix (the advertised form is
/// whatever the operator configured, the dialed form always carries `/p2p` because libp2p-swarm
/// appends it to every dial, the inbound observed form never does); under this cap those forms
/// replace each other instead of accumulating, and either form dials the same endpoint. An
/// eviction only trims the peer-exchange payload (`exchange_info`): dialing reads `known_peers`,
/// kad and `discovery_peers`, and banning reads `observed_ip_addresses`, never this set. A cap of
/// one also bounds the dial fan-out one discovery entry can cause to a single address.
///
/// The discovery path reuses this value as the per-entry ceiling in `eligible_for_discovery`:
/// a PeerExchange entry with more addresses than this set can hold cannot come from an honest
/// peer's store, so it is rejected before it reaches `discovery_peers` (issue #1183). The record
/// validation cap `MAX_ADVERTISED_MULTIADDRS` is tied to this value, so validation and storage
/// agree on how many addresses one peer may present: a record can never carry more addresses than
/// the store keeps for a peer.
pub(crate) const MAX_MULTIADDRS_PER_PEER: usize = 1;

/// Maximum number of distinct observed connection IPs retained for a single peer.
///
/// `observed_ip_addresses` is fed only by genuine connection source IPs (`register_incoming` /
/// `register_outgoing`), never by self-advertised records, so growing it requires the peer to
/// really present each IP on a connection. One IPv6 /64 makes that cheap enough to matter:
/// without a bound one peer entry grows without limit (slow memory growth), and every recorded
/// IP later becomes one entry of the per-ban fan-out (`filter_new_ips_to_ban` yields one `Ban`
/// action per IP), so the set size is also the per-ban work factor (issue #1251).
///
/// This bound is deliberately independent of [`MAX_MULTIADDRS_PER_PEER`]. The two sets must stay
/// separate: the multiaddr set includes self-advertised addresses and is capped against a
/// republish flood (GHSA-29v6-gvv5-45gx), while this set must never admit an advertised address
/// at all, so an attacker cannot feed the per-IP ban counter by advertising an honest peer's IP
/// (GHSA-6qcj-p42p-779j). Routing this set through the multiaddr cap would collapse that
/// separation.
///
/// The cap refuses a new IP once the set is full; it never admits one by evicting another. Ban
/// accounting relies on the set only ever growing: per-IP ban counts are incremented from this
/// set when a ban starts (`add_banned_peer`) and decremented from it again when the ban ends
/// (`remove_banned_peer`), so the unban-time set must contain every IP the ban-time set held.
/// An eviction between those two reads would strand a count and keep the IP banned after its
/// peer is unbanned, penalizing any honest peer that shares the IP (the same harm class
/// GHSA-6qcj closed for advertised addresses). Refusal keeps growth monotonic, which closes
/// that stranding direction. The mirror direction (an IP admitted while a ban is in force is
/// decremented at unban without ever having been incremented) is a pre-existing hazard of
/// re-reading the live set at both moments; the cap neither causes nor fixes it. A ban-time
/// snapshot held by `BannedPeers` would close both directions and would then also permit
/// recency-biased eviction; that is a follow-up, not this change.
///
/// The retention trade-off is deliberate: once the cap is full a later connection contributes
/// no IP, so a ban fans out only to the first addresses the peer presented. This does not
/// change the containment class. The peer-identity ban is the primary control and the per-IP
/// fan-out is defense-in-depth: an attacker holding more addresses than any cap re-enters from
/// a fresh address under refusal and eviction alike, while an attacker holding at most the
/// cap's worth of addresses has every address recorded. Every retained IP is one the peer
/// genuinely connected from.
///
/// Sixteen leaves generous room for honest churn (dual-stack v4 + v6, DHCP renewal, mobile
/// roaming) while bounding both the per-peer memory and the per-ban fan-out.
pub(crate) const MAX_OBSERVED_IPS_PER_PEER: usize = 16;

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
    ///
    /// Bounded by [`MAX_OBSERVED_IPS_PER_PEER`] (issue #1251): once full, a new IP is refused,
    /// never admitted by evicting an old one, so the set grows monotonically and an unban can
    /// never strand a per-IP ban count behind an eviction.
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
            multiaddrs: addrs.into_iter().take(MAX_MULTIADDRS_PER_PEER).collect(),
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
        multiaddrs.into_iter().for_each(|multiaddr| self.note_multiaddr(multiaddr));
    }

    /// Record a multiaddr the peer is using, keeping the set within [`MAX_MULTIADDRS_PER_PEER`].
    ///
    /// A newly seen address is always admitted, so the most recent address a peer presents (a
    /// connection witnessed via `register_incoming` / `register_outgoing`, or the address it
    /// advertises on a rotated network key via `update_net`) is always part of the peer-exchange
    /// payload built by [`Self::exchange_info`]. If admitting it pushes the set over the cap, one
    /// of the other addresses is evicted to restore the bound. Re-recording an address already
    /// present is a no-op. A self-advertised republish flood therefore churns the set within the
    /// cap instead of growing it without bound (GHSA-29v6-gvv5-45gx). With the cap at one (see
    /// [`MAX_MULTIADDRS_PER_PEER`]) the set is the address the peer most recently presented, and
    /// the bare and `/p2p`-suffixed forms of one honest endpoint replace each other. An eviction
    /// can only ever trim that payload: the ban path reads [`Self::observed_ip_addresses`], not
    /// this set.
    fn note_multiaddr(&mut self, multiaddr: Multiaddr) {
        if self.multiaddrs.insert(multiaddr.clone())
            && self.multiaddrs.len() > MAX_MULTIADDRS_PER_PEER
        {
            if let Some(victim) = self.multiaddrs.iter().find(|addr| **addr != multiaddr).cloned() {
                self.multiaddrs.remove(&victim);
            }
        }
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

    /// Record the source IP of an observed connection, keeping the set within
    /// [`MAX_OBSERVED_IPS_PER_PEER`].
    ///
    /// Once the set is full a new IP is refused, never admitted by evicting an old one: ban
    /// accounting needs the set to grow monotonically so every per-IP count incremented at ban
    /// time can be decremented at unban time (see [`MAX_OBSERVED_IPS_PER_PEER`]). An IP already
    /// in the set stays recorded, so re-presenting a known IP at the cap is a no-op, not a loss.
    fn note_observed_ip(&mut self, multiaddr: &Multiaddr) {
        let has_capacity = self.observed_ip_addresses.len() < MAX_OBSERVED_IPS_PER_PEER;
        if let Some(ip) = Self::ip_from_multiaddr(multiaddr).filter(|_| has_capacity) {
            self.observed_ip_addresses.insert(ip);
        }
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

    /// Adopt `other`'s reputation if it is worse than this peer's own.
    ///
    /// Reputation (the accumulated [Score], and therefore any ban it encodes) is a property of the
    /// peer's confirmed domain identity, not of the transport key it currently presents. When an
    /// anonymous-inbound record is promoted over a record already stored under that identity
    /// (`AllPeers::upsert_peer`), the promoted record must not shed a ban - or any worse score -
    /// held by the record it displaces, or a peer could reset its own reputation simply by
    /// reconnecting under a fresh network key before its kad record arrives (issue #998). The whole
    /// [Score] is adopted, not just its aggregate, so the ban-decay lockout that keeps the ban in
    /// force also carries across; `reputation()` and `AllPeers::peer_banned` then keep reporting
    /// the ban after the rotation. This is a no-op when this peer's own score is already the
    /// worse (or equal) of the two, so a genuinely better-behaved displaced record never drags
    /// the promoted record down.
    pub(super) fn retain_worse_reputation(&mut self, other: &Peer) {
        if other.score < self.score {
            self.score = other.score.clone();
        }
    }

    /// Register the dialing peer as connected.
    ///
    /// This method also updates the number of incoming connections +1.
    pub(super) fn register_incoming(&mut self, multiaddr: Multiaddr) {
        // an observed connection address: record its IP as one the peer genuinely presented, which
        // is the only kind of IP allowed to feed the per-IP ban counter (GHSA-6qcj-p42p-779j);
        // the recorded set is itself bounded (issue #1251)
        self.note_observed_ip(&multiaddr);
        // keep the stored multiaddr set bounded (GHSA-29v6-gvv5-45gx); the observed IP recorded
        // above is independent of this set and is never evicted by the cap
        self.note_multiaddr(multiaddr);

        match &mut self.connection_status {
            ConnectionStatus::Connected { num_in, .. } => *num_in = num_in.saturating_add(1),
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
        // is the only kind of IP allowed to feed the per-IP ban counter (GHSA-6qcj-p42p-779j);
        // the recorded set is itself bounded (issue #1251)
        self.note_observed_ip(&multiaddr);
        // keep the stored multiaddr set bounded (GHSA-29v6-gvv5-45gx); the observed IP recorded
        // above is independent of this set and is never evicted by the cap
        self.note_multiaddr(multiaddr);

        match &mut self.connection_status {
            ConnectionStatus::Connected { num_out, .. } => *num_out = num_out.saturating_add(1),
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

    /// Number of distinct multiaddrs currently retained for this peer.
    #[cfg(test)]
    pub(super) fn multiaddr_count(&self) -> usize {
        self.multiaddrs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::create_multiaddr;
    use tn_config::ScoreConfig;

    /// Regression (GHSA-29v6-gvv5-45gx): a flood of distinct addresses must not grow the stored set
    /// past the cap, and the most recent address must always survive so the ban path keeps
    /// recording the address a peer is currently presenting (a rotated key or a live connection).
    #[test]
    fn note_multiaddr_caps_the_set_and_keeps_the_newest() {
        // constructing a `Peer` builds its `Score`, which reads the global score config
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();

        // far more distinct addresses than the cap
        (0..MAX_MULTIADDRS_PER_PEER * 8).for_each(|_| peer.note_multiaddr(create_multiaddr(None)));
        assert!(
            peer.multiaddrs.len() <= MAX_MULTIADDRS_PER_PEER,
            "the stored multiaddr set must stay within the cap under a republish flood"
        );

        // the newest address is admitted even when the set is already full, so the ban path keeps
        // seeing the address the peer is currently using rather than only stale ones
        let newest = create_multiaddr(None);
        peer.note_multiaddr(newest.clone());
        assert!(
            peer.multiaddrs.contains(&newest),
            "the most recent address must be recorded even at the cap boundary"
        );
        assert!(peer.multiaddrs.len() <= MAX_MULTIADDRS_PER_PEER);
    }

    /// The cap holds the address a peer most recently presented (see
    /// [`MAX_MULTIADDRS_PER_PEER`]). One honest endpoint reaches the set in two syntactic forms,
    /// with and without the `/p2p/<peer_id>` suffix (advertised bare, dialed with `/p2p`, seen bare
    /// again on an inbound connection). Each form replaces the previous one instead of
    /// accumulating, so the set never grows past one and always holds a form that dials the
    /// endpoint. The cap is exact: two forms of one endpoint would fill a cap of two.
    #[test]
    fn honest_address_forms_replace_each_other_within_the_cap() {
        // constructing a `Peer` builds its `Score`, which reads the global score config
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();
        // the suffix a dial carries is this peer's own id (libp2p-swarm appends it to every dial)
        let peer_id = peer.peer_id();
        let with_p2p = |bare: &Multiaddr| {
            peer_id.iter().fold(bare.clone(), |addr, id| addr.with(Protocol::P2p(*id)))
        };
        // a QUIC listen endpoint (TEST-NET-3), the shape this node advertises and dials
        let endpoint = Multiaddr::empty()
            .with(Protocol::Ip4([203, 0, 113, 10].into()))
            .with(Protocol::Udp(49_584))
            .with(Protocol::QuicV1);
        assert!(peer_id.is_some(), "the test peer carries a network key, so it has a peer id");

        // advertised bare (a record or committee entry)
        peer.note_multiaddr(endpoint.clone());
        assert_eq!(peer.multiaddrs.len(), MAX_MULTIADDRS_PER_PEER);
        assert!(peer.multiaddrs.contains(&endpoint), "the advertised form is stored");

        // then dialed: the dial always carries `/p2p`, and that form replaces the bare one
        peer.register_outgoing(with_p2p(&endpoint));
        assert_eq!(peer.multiaddrs.len(), 1, "one honest endpoint, one entry");
        assert!(peer.multiaddrs.contains(&with_p2p(&endpoint)), "the dialed form is stored");

        // then seen on an inbound connection, which presents the bare listen address again
        // because libp2p-quic dials from the listening socket; it replaces the dialed form
        peer.register_incoming(endpoint.clone());
        assert_eq!(peer.multiaddrs.len(), 1, "one honest endpoint, one entry");
        assert!(peer.multiaddrs.contains(&endpoint), "the most recently presented form is stored");
    }

    /// Regression (issue #1010, informational): the per-connection counters are `u8` and were
    /// incremented with a plain `*num_in += 1`. In a debug build (overflow checks on) the 256th
    /// inbound connection from one peer panicked; in a release build it wrapped `255 -> 0`.
    /// `saturating_add` makes the counter well-defined for a hostile peer: it clamps at `u8::MAX`
    /// instead of panicking or wrapping. This test would panic on the 256th call before the fix.
    #[test]
    fn register_incoming_saturates_and_never_panics() {
        // constructing a `Peer` builds its `Score`, which reads the global score config
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();

        // far more inbound connections than `u8::MAX`; a plain `+= 1` panics here in a debug build
        (0..300).for_each(|_| peer.register_incoming(create_multiaddr(None)));

        if let ConnectionStatus::Connected { num_in, num_out } = peer.connection_status {
            assert_eq!(num_in, u8::MAX, "inbound counter must saturate at u8::MAX, not wrap");
            assert_eq!(num_out, 0, "no outbound connections were registered");
        } else {
            panic!(
                "peer must be Connected after inbound registrations: {:?}",
                peer.connection_status
            );
        }
    }

    /// Regression (issue #1010, informational): the outbound counter mirror of
    /// [`register_incoming_saturates_and_never_panics`]. `register_outgoing` must clamp `num_out`
    /// at `u8::MAX` rather than panic (debug) or wrap (release).
    #[test]
    fn register_outgoing_saturates_and_never_panics() {
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();

        (0..300).for_each(|_| peer.register_outgoing(create_multiaddr(None)));

        if let ConnectionStatus::Connected { num_in, num_out } = peer.connection_status {
            assert_eq!(num_out, u8::MAX, "outbound counter must saturate at u8::MAX, not wrap");
            assert_eq!(num_in, 0, "no inbound connections were registered");
        } else {
            panic!(
                "peer must be Connected after outbound registrations: {:?}",
                peer.connection_status
            );
        }
    }

    /// Regression (issue #1251): `observed_ip_addresses` had no size cap. A peer that genuinely
    /// connects from many distinct source IPs (cheap from one IPv6 /64) grew its entry without
    /// bound, and every recorded IP later became one `Ban` action in the per-ban fan-out. The
    /// set must clamp at [`MAX_OBSERVED_IPS_PER_PEER`], and the fan-out source
    /// (`filter_new_ips_to_ban`) is bounded by the same cap.
    #[test]
    fn observed_ips_clamp_at_the_cap() {
        use std::net::Ipv4Addr;
        // constructing a `Peer` builds its `Score`, which reads the global score config
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();

        // far more distinct genuine source IPs than the cap (TEST-NET-3)
        (0u8..200).for_each(|i| {
            peer.register_incoming(create_multiaddr(Some(Ipv4Addr::new(203, 0, 113, i).into())));
        });
        assert_eq!(
            peer.observed_ip_addresses.len(),
            MAX_OBSERVED_IPS_PER_PEER,
            "the observed-IP set must clamp at the cap under a many-source-IP flood"
        );

        // the flood tail was refused, not admitted by evicting an earlier IP
        let tail: IpAddr = Ipv4Addr::new(203, 0, 113, 199).into();
        assert!(
            !peer.known_ip_addresses().any(|ip| ip == tail),
            "an IP past the cap must be refused"
        );

        // the other connection direction feeds the same bounded set (TEST-NET-2)
        peer.register_outgoing(create_multiaddr(Some(Ipv4Addr::new(198, 51, 100, 1).into())));
        assert_eq!(
            peer.observed_ip_addresses.len(),
            MAX_OBSERVED_IPS_PER_PEER,
            "outgoing registrations must respect the same cap"
        );

        // the per-ban IP fan-out is bounded by the same cap
        assert!(
            peer.filter_new_ips_to_ban(&HashSet::new()).len() <= MAX_OBSERVED_IPS_PER_PEER,
            "a ban must fan out to at most the cap"
        );
    }

    /// Refusal keeps ban accounting symmetric (issue #1251): every IP admitted before the cap is
    /// still present after an arbitrary flood - growth is monotonic, nothing is evicted - so the
    /// per-IP ban counts incremented from this set at ban time can all be decremented from it at
    /// unban time. Re-presenting an admitted IP at the cap stays a no-op, not a loss.
    #[test]
    fn observed_ips_refuse_new_entries_instead_of_evicting() {
        use std::net::Ipv4Addr;
        // constructing a `Peer` builds its `Score`, which reads the global score config
        super::super::score::init_peer_score_config(ScoreConfig::default());
        let mut peer = Peer::default_for_test();

        // admit exactly cap-many IPs (TEST-NET-3)
        let admitted = (0u8..)
            .take(MAX_OBSERVED_IPS_PER_PEER)
            .map(|i| IpAddr::from(Ipv4Addr::new(203, 0, 113, i)))
            .collect::<Vec<_>>();
        admitted.iter().for_each(|ip| peer.register_incoming(create_multiaddr(Some(*ip))));
        assert_eq!(peer.observed_ip_addresses.len(), MAX_OBSERVED_IPS_PER_PEER);

        // flood past the cap from a different range (TEST-NET-2)
        (0u8..100).for_each(|i| {
            peer.register_outgoing(create_multiaddr(Some(Ipv4Addr::new(198, 51, 100, i).into())));
        });

        // every admitted IP survives the flood
        admitted.iter().for_each(|ip| {
            assert!(
                peer.known_ip_addresses().any(|known| known == *ip),
                "an admitted IP must never be evicted; ban add/remove symmetry depends on it"
            );
        });

        // re-presenting an admitted IP at the cap is a no-op admit
        admitted.iter().take(1).for_each(|ip| peer.register_incoming(create_multiaddr(Some(*ip))));
        assert_eq!(
            peer.observed_ip_addresses.len(),
            MAX_OBSERVED_IPS_PER_PEER,
            "re-presenting a recorded IP at the cap must not change the set"
        );
    }
}
