//! Peer management for byzantine peers.
//!
//! Peers that score poorly are eventually banned.

use super::peer::Peer;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

/// The threshold of banned peers before an IP address is blocked.
// TODO: move to config
const BANNED_PEERS_PER_IP_THRESHOLD: usize = 5;

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

        // for address in ip_addresses {
        //     if let Some(count) = self.banned_peers_by_ip.get_mut(&address) {
        //         *count = count.saturating_sub(1);
        //     }
        // }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peers::score::GLOBAL_SCORE_CONFIG;
    use libp2p::{multiaddr::Protocol, Multiaddr};
    use rand::prelude::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::{Arc, Once};
    use tn_config::ScoreConfig;

    // TODO: make this visible to all unit tests
    // - maybe add in peers::types behind a mod test-utils?

    // ensure `init_peer_score_config` is only set once
    static INIT: Once = Once::new();

    // ensure `init_peer_score_config` is only set once
    pub fn ensure_score_config() {
        INIT.call_once(|| {
            // use default
            let config = ScoreConfig::default();
            // ignore result
            let _ = GLOBAL_SCORE_CONFIG.set(Arc::new(config));
        });
    }

    /// Helper function to create a random IPV4 address.
    fn random_ip_addr() -> IpAddr {
        let mut rng = rand::thread_rng();
        // random between IPv4 and IPv6 (80% v4, 20% v6)
        if rng.gen_bool(0.8) {
            // random IPv4
            let a = rng.gen_range(1..255);
            let b = rng.gen_range(0..255);
            let c = rng.gen_range(0..255);
            let d = rng.gen_range(1..255);
            IpAddr::V4(Ipv4Addr::new(a, b, c, d))
        } else {
            // random IPv6
            let random: Vec<u16> = [(); 8].iter().map(|_| rng.gen_range(0..255)).collect();
            IpAddr::V6(Ipv6Addr::new(
                random[0], random[1], random[2], random[3], random[4], random[5], random[6],
                random[7],
            ))
        }
    }

    /// Helper function to create a peer with specific IP addresses.
    fn create_peer_with_ips(ips: Vec<IpAddr>) -> Peer {
        ensure_score_config();

        let mut peer = Peer::default();

        // add multiaddrs with the specified IPs
        for ip in ips {
            let mut multiaddr = Multiaddr::empty();
            match ip {
                IpAddr::V4(ipv4) => multiaddr.push(Protocol::Ip4(ipv4)),
                IpAddr::V6(ipv6) => multiaddr.push(Protocol::Ip6(ipv6)),
            }
            multiaddr.push(Protocol::Tcp(8000));

            // add the multiaddr to the peer
            peer.register_outgoing(multiaddr);
        }

        peer
    }

    #[test]
    fn test_default() {
        let banned_peers = BannedPeers::default();
        assert_eq!(banned_peers.total(), 0);
        assert!(banned_peers.banned_ips().is_empty());
    }

    #[test]
    fn test_remove_banned_peer() {
        let mut banned_peers = BannedPeers::default();
        let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        let peer = create_peer_with_ips(vec![ip1, ip2]);
        banned_peers.add_banned_peer(&peer);
        assert_eq!(banned_peers.total(), 1);

        let unbanned_ips = banned_peers.remove_banned_peer(vec![ip1, ip2].into_iter());

        assert_eq!(banned_peers.total(), 0);
        assert_eq!(unbanned_ips.len(), 2);
        assert!(unbanned_ips.contains(&ip1));
        assert!(unbanned_ips.contains(&ip2));
        assert!(!banned_peers.ip_banned(&ip1));
        assert!(!banned_peers.ip_banned(&ip2));
        assert_eq!(banned_peers.banned_ips().len(), 0);
    }

    #[test]
    fn test_add_multiple_peers_same_ip() {
        let mut banned_peers = BannedPeers::default();
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // add multiple peers with the same IP
        for i in 0..BANNED_PEERS_PER_IP_THRESHOLD {
            let peer = create_peer_with_ips(vec![ip]);
            banned_peers.add_banned_peer(&peer);

            // check the total is incrementing
            assert_eq!(banned_peers.total(), i + 1);

            // check if IP is banned based on threshold
            if i + 1 > BANNED_PEERS_PER_IP_THRESHOLD {
                assert!(banned_peers.ip_banned(&ip), "IP should be banned after threshold");
                assert!(banned_peers.banned_ips().contains(&ip), "IP should be in banned_ips");
            } else {
                assert!(!banned_peers.ip_banned(&ip), "IP should not be banned before threshold");
                assert!(!banned_peers.banned_ips().contains(&ip), "IP should not be in banned_ips");
            }
        }

        // add one more peer to exceed threshold
        let peer = create_peer_with_ips(vec![ip]);
        banned_peers.add_banned_peer(&peer);

        // assert IP is now banned
        assert!(banned_peers.ip_banned(&ip));
        assert_eq!(banned_peers.banned_ips().len(), 1);
    }

    #[test]
    fn test_add_peer_no_ip() {
        ensure_score_config();
        let mut banned_peers = BannedPeers::default();

        // Create a peer with no IP addresses
        let peer = Peer::default();
        banned_peers.add_banned_peer(&peer);

        // total should increment but no IPs should be banned
        assert_eq!(banned_peers.total(), 1);
        assert_eq!(banned_peers.banned_ips().len(), 0);
    }

    #[test]
    fn test_remove_nonexistent_ip() {
        let mut banned_peers = BannedPeers::default();
        let ip1 = random_ip_addr();
        let ip2 = random_ip_addr();

        // Add a peer with ip1
        let peer = create_peer_with_ips(vec![ip1]);
        banned_peers.add_banned_peer(&peer);

        // Remove a peer with ip2, which doesn't exist in the collection
        let unbanned_ips = banned_peers.remove_banned_peer(vec![ip2].into_iter());

        // Total should decrease but no IPs should be unbanned
        assert_eq!(banned_peers.total(), 0);
        assert!(unbanned_ips.is_empty());
    }

    #[test]
    fn test_remove_banned_peer_partial() {
        let mut banned_peers = BannedPeers::default();
        let ip1 = random_ip_addr();
        let ip2 = random_ip_addr();

        // one above max so ip stays band after peer removed
        let banned_peers_threshold = BANNED_PEERS_PER_IP_THRESHOLD + 1;

        // add enough peers so ip is banned after removal
        for _ in 0..banned_peers_threshold {
            let peer = create_peer_with_ips(vec![ip1]);
            banned_peers.add_banned_peer(&peer);
        }

        // add one peer with both ips
        // this puts ip1 at threshold + 1 so removing the peer won't unban the IP
        let peer3_ips = vec![ip1, ip2];
        let peer3 = create_peer_with_ips(peer3_ips.clone());
        banned_peers.add_banned_peer(&peer3);

        // assert 1 extra peer past threshold
        assert_eq!(banned_peers.total(), banned_peers_threshold + 1);

        // remove one peer with both ip addresses
        let unbanned_ips = banned_peers.remove_banned_peer(peer3_ips.into_iter());

        // assert total decreased by 1
        assert_eq!(banned_peers.total(), banned_peers_threshold);

        // ip1 should still be banned because it had more than threshold
        // ip2 should be unbanned because it had less than threshold
        assert!(banned_peers.ip_banned(&ip1));
        assert!(!banned_peers.ip_banned(&ip2));

        // Only ip2 should be in the returned unbanned IPs
        assert_eq!(unbanned_ips.len(), 1);
        assert!(unbanned_ips.contains(&ip2));
    }

    #[test]
    fn test_multiple_ips_different_ban_status() {
        let mut banned_peers = BannedPeers::default();
        let ip1 = random_ip_addr();
        let ip2 = random_ip_addr();
        let ip3 = random_ip_addr();

        // one above max so ip stays band after peer removed
        let banned_peers_threshold = BANNED_PEERS_PER_IP_THRESHOLD + 1;

        // Add BANNED_PEERS_PER_IP_THRESHOLD+1 peers with ip1 (will be banned)
        for _ in 0..banned_peers_threshold {
            let peer = create_peer_with_ips(vec![ip1]);
            banned_peers.add_banned_peer(&peer);
        }

        // add just enough so at threshold, but not banned
        for _ in 0..banned_peers_threshold - 1 {
            let peer = create_peer_with_ips(vec![ip2]);
            banned_peers.add_banned_peer(&peer);
        }

        // add 1 peer with ip3 (below threshold)
        let peer = create_peer_with_ips(vec![ip3]);
        banned_peers.add_banned_peer(&peer);

        // assert banned ips
        assert!(banned_peers.ip_banned(&ip1));
        assert!(!banned_peers.ip_banned(&ip2));
        assert!(!banned_peers.ip_banned(&ip3));

        // assert only ip1 banned
        let banned_ips = banned_peers.banned_ips();
        assert_eq!(banned_ips.len(), 1);
        assert!(banned_ips.contains(&ip1));
    }

    #[test]
    fn test_remove_all_ips_for_peer() {
        let mut banned_peers = BannedPeers::default();

        // create multiple IPs
        let ips: Vec<IpAddr> = (0..10).map(|_| random_ip_addr()).collect();

        // add a peer with all IPs
        let peer = create_peer_with_ips(ips.clone());
        banned_peers.add_banned_peer(&peer);

        assert_eq!(banned_peers.total(), 1);

        // remove the peer
        let unbanned_ips = banned_peers.remove_banned_peer(ips.iter().cloned());

        // check all IPs were unbanned
        assert_eq!(unbanned_ips.len(), ips.len());
        for ip in &ips {
            assert!(unbanned_ips.contains(ip));
        }
    }

    #[test]
    fn test_saturating_operations() {
        let mut banned_peers = BannedPeers::default();

        // assert total doesn't underflow
        let unbanned_ips = banned_peers.remove_banned_peer(vec![].into_iter());
        assert_eq!(banned_peers.total(), 0);
        assert!(unbanned_ips.is_empty());

        let banned_peers_threshold = BANNED_PEERS_PER_IP_THRESHOLD;

        // add a large number of peers with the same IP
        let ip = random_ip_addr();
        for _ in 0..(banned_peers_threshold * 2) {
            let peer = create_peer_with_ips(vec![ip]);
            banned_peers.add_banned_peer(&peer);
        }

        // verify the IP is banned
        assert!(banned_peers.ip_banned(&ip));

        // remove more peers than added
        for _ in 0..(banned_peers_threshold * 3) {
            banned_peers.remove_banned_peer(vec![ip].into_iter());
        }

        // assert total does not underflow
        assert_eq!(banned_peers.total(), 0);
        // assert ip unbanned
        assert!(!banned_peers.ip_banned(&ip));
    }
}
