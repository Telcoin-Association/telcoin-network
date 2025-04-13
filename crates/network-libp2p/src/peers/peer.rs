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
use tracing::error;

/// Information about a given connected peer.
#[derive(Clone, Debug, Default)]
pub(super) struct Peer {
    /// The config
    config: PeerConfig,
    /// The peer's score - used to derive [Reputation].
    score: Score,
    /// The multiaddrs this node has witnessed the peer using.
    ///
    /// These are used to manage the banning process and are exchanged with peers.
    multiaddrs: HashSet<Multiaddr>,
    /// The listening multiaddrs advertised by this peer.
    listening_addrs: Vec<Multiaddr>,
    /// Connection status of the peer.
    connection_status: ConnectionStatus,
    /// Trusted peers are specifically included by node operators.
    is_trusted: bool,
    /// Direction of the most recent connection with this peer.
    ///
    /// `None` if this peer was never connected.
    connection_direction: Option<ConnectionDirection>,
}

impl Peer {
    /// Create a new trusted peer.
    pub(super) fn new_trusted(addr: Option<Multiaddr>) -> Peer {
        let listening_addrs = addr.map(|multi| vec![multi]).unwrap_or_default();
        Self { listening_addrs, score: Score::new_max(), is_trusted: true, ..Default::default() }
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
        if !self.is_trusted {
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

    /// Filter banned peer's ip addresses against already known banned ip addresses.
    pub(super) fn filter_new_ips_to_ban(
        &self,
        already_banned_ips: &HashSet<IpAddr>,
    ) -> Vec<IpAddr> {
        self.known_ip_addresses().filter(|ip| already_banned_ips.contains(ip)).collect::<Vec<_>>()
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
    pub(super) fn exchange_info(&self) -> HashSet<Multiaddr> {
        self.multiaddrs.clone()
    }

    /// Update a peer record to make it trusted.
    pub(super) fn make_trusted(&mut self) {
        if !self.is_trusted {
            self.is_trusted = true;
            self.score = Score::new_max();
        }
    }

    /// Update multiaddrs for the peer.
    ///
    /// Returns a boolean indicating if the multiaddr was newly recorded.
    pub(super) fn update_listening_addrs(&mut self, multiaddr: Multiaddr) -> bool {
        if !self.listening_addrs.contains(&multiaddr) {
            self.listening_addrs.push(multiaddr);
            true
        } else {
            false
        }
    }
}
