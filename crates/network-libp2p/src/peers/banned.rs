//! Peer management for byzantine peers.
//!
//! Peers that score poorly are eventually banned.

use super::peer::Peer;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

#[cfg(test)]
#[path = "../tests/banned_peers.rs"]
mod banned_peers;

/// The threshold of banned peers before an IP address is blocked.
/// Currently set to 1, so ips are banned if more than one peer is banned.
const BANNED_PEERS_PER_IP_THRESHOLD: usize = 1;

/// The total number of banned peers and a collection of the number of bad peers by IP address.
#[derive(Debug, Default)]
pub(super) struct BannedPeers {
    /// The total number of banned peers for this node.
    total: usize,
    /// The number of banned peers by IP address.
    banned_peers_by_ip: HashMap<IpAddr, usize>,
}

impl BannedPeers {
    /// Remove banned peers by IP address.
    ///
    /// This method always reduces the total number of banned peers by 1. The method also attempts
    /// to reduce the number of banned peers by IP address. IP addresses that are no longer banned
    /// are returned to the sender.
    ///
    /// NOTE: it's possible to have multiple peers from a single IP address
    pub(super) fn remove_banned_peer(
        &mut self,
        ip_addresses: impl Iterator<Item = IpAddr>,
    ) -> Vec<IpAddr> {
        self.total = self.total.saturating_sub(1);

        ip_addresses
            .filter_map(|ip| {
                match self.banned_peers_by_ip.get_mut(&ip) {
                    Some(count) => {
                        // reduce count
                        *count = count.saturating_sub(1);
                        // return ip if no longer associated with a banned peer
                        if !self.ip_banned(&ip) {
                            Some(ip)
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            })
            .collect()
    }

    /// Add IP addresses to the banned peers collection and update counts.
    pub(super) fn add_banned_peer(&mut self, peer: &Peer) {
        self.total = self.total.saturating_add(1);
        for address in peer.known_ip_addresses() {
            tracing::debug!(target: "peer-manager", ?address, "known ip address for banned peer");
            *self.banned_peers_by_ip.entry(address).or_insert(0) += 1;
        }
    }

    /// Return the number of banned peers.
    pub(super) fn total(&self) -> usize {
        self.total
    }

    /// Return a [HashSet] of banned IP addresses.
    pub(super) fn banned_ips(&self) -> HashSet<IpAddr> {
        self.banned_peers_by_ip
            .iter()
            .filter_map(
                |(ip, count)| {
                    if *count > BANNED_PEERS_PER_IP_THRESHOLD {
                        Some(*ip)
                    } else {
                        None
                    }
                },
            )
            .collect()
    }

    /// Bool indicating an IP address is currently banned.
    ///
    /// IP addresses are banned if the number of banned peers exceeds the
    /// [BANNED_PEERS_PER_IP_THRESHOLD].
    pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers_by_ip
            .get(ip)
            .map_or(false, |count| *count > BANNED_PEERS_PER_IP_THRESHOLD)
    }
}
