//! Peer management for byzantine peers.
//!
//! Peers that score poorly are eventually banned.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

/// The threshold of banned peers before an IP address is blocked.
const BANNED_PEERS_PER_IP_THRESHOLD: usize = 5;

/// The total number of banned peers and a collection of the number of bad peers by IP address.
#[derive(Debug, Default)]
pub struct BannedPeers {
    /// The total number of banned peers for this node.
    banned_peers: usize,
    /// The number of banned peers by IP address.
    banned_peers_by_ip: HashMap<IpAddr, usize>,
}

impl BannedPeers {
    /// Remove banned peers by IP address.
    ///
    /// This method always reduces the total number of banned peers by 1. The method also attempts to reduce the number of banned peers by IP address.
    pub fn remove_banned_peer(&mut self, ip_addresses: impl Iterator<Item = IpAddr>) {
        self.banned_peers = self.banned_peers.saturating_sub(1);
        for address in ip_addresses {
            if let Some(count) = self.banned_peers_by_ip.get_mut(&address) {
                *count = count.saturating_sub(1);
            }
        }
    }

    /// Add IP addresses to the banned peers collection.
    pub fn add_banned_peer(&mut self, ip_addresses: impl Iterator<Item = IpAddr>) {
        self.banned_peers = self.banned_peers.saturating_add(1);
        for address in ip_addresses {
            *self.banned_peers_by_ip.entry(address).or_insert(0) += 1;
        }
    }

    /// Return the number of banned peers.
    pub fn banned_peers(&self) -> usize {
        self.banned_peers
    }

    /// Return a [HashSet] of banned IP addresses.
    pub fn banned_ips(&self) -> HashSet<IpAddr> {
        self.banned_peers_by_ip
            .iter()
            .filter(|(_ip, count)| **count > BANNED_PEERS_PER_IP_THRESHOLD)
            .map(|(ip, _count)| *ip)
            .collect()
    }

    /// Bool indicating an IP address is currently banned.
    ///
    /// IP addresses are banned if the number of banned peers exceeds the [BANNED_PEERS_PER_IP_THRESHOLD].
    pub fn ip_is_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers_by_ip
            .get(ip)
            .map_or(false, |count| *count > BANNED_PEERS_PER_IP_THRESHOLD)
    }
}
