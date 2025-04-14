//! Unit tests for banned peers

use super::*;
use crate::common::{ensure_score_config, random_ip_addr};
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::net::Ipv4Addr;

/// Helper function to create a peer with specific IP addresses.
fn create_peer_with_ips(ips: Vec<IpAddr>) -> Peer {
    ensure_score_config(None);

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
    ensure_score_config(None);
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
