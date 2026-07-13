//! Unit tests for banned peers

use super::*;
use crate::common::{ensure_score_config, random_ip_addr};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::net::Ipv4Addr;

/// Helper function to create a peer with specific IP addresses.
fn create_peer_with_ips(ips: Vec<IpAddr>) -> Peer {
    ensure_score_config(None);

    let mut peer = Peer::default_for_test();

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
    let peer = Peer::default_for_test();
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

/// Regression for #809: `remove_validator_ip` is an IP-map cleanup and must not change the
/// peer-level `total`. This mirrors the committee-rotation path: a banned validator is first
/// unbanned by `remove_banned_peer` (the single, correct `-1`), then its lingering count-0 IP
/// entries are swept by `remove_validator_ip`. Previously that sweep decremented `total` once
/// per removed entry, over-counting the unban by the validator's IP count.
#[test]
fn test_remove_validator_ip_does_not_decrement_total() {
    let mut banned_peers = BannedPeers::default();

    // three unrelated background bans so a spurious decrement is visible rather than floored to
    // 0 by saturating_sub
    for _ in 0..3 {
        let background = create_peer_with_ips(vec![random_ip_addr()]);
        banned_peers.add_banned_peer(&background);
    }
    assert_eq!(banned_peers.total(), 3);

    // a validator whose two IPs are present in the banned-ip map
    let ip_a = random_ip_addr();
    let ip_b = random_ip_addr();
    let validator = create_peer_with_ips(vec![ip_a, ip_b]);
    banned_peers.add_banned_peer(&validator);
    assert_eq!(banned_peers.total(), 4);

    // the rotation path unbans the peer first; this owns the single, correct decrement and leaves
    // the two IP keys behind at count 0
    banned_peers.remove_banned_peer(vec![ip_a, ip_b].into_iter());
    assert_eq!(banned_peers.total(), 3, "remove_banned_peer owns the single -1");

    // then the validator-IP cleanup sweeps those count-0 entries. it must not touch `total`.
    // before the fix this decremented once per entry, driving total to 1.
    banned_peers.remove_validator_ip(&PeerId::random(), vec![ip_a, ip_b].into_iter());

    assert_eq!(banned_peers.total(), 3, "remove_validator_ip must not change the banned total");
    // and it still does its real job: the validator's IP entries are gone from the map (asserted
    // directly because a count-0 entry is indistinguishable from an absent one via ip_banned)
    assert!(!banned_peers.banned_peers_by_ip.contains_key(&ip_a));
    assert!(!banned_peers.banned_peers_by_ip.contains_key(&ip_b));
}

/// Regression for #809 (co-located variant): clearing a never-banned committee member's IP must
/// not decrement `total` even when that IP entry exists because a different, still-banned peer
/// shares it. This is why guarding only the call site is insufficient: the spurious decrement
/// also fires for members that were never banned.
#[test]
fn test_remove_validator_ip_for_never_banned_member_preserves_total() {
    let mut banned_peers = BannedPeers::default();
    let shared_ip = random_ip_addr();

    // a still-banned peer occupying the shared IP (below the per-IP block threshold)
    let attacker = create_peer_with_ips(vec![shared_ip]);
    banned_peers.add_banned_peer(&attacker);
    assert_eq!(banned_peers.total(), 1);

    // a never-banned committee member co-located on the same IP gets its validator IP cleared
    banned_peers.remove_validator_ip(&PeerId::random(), vec![shared_ip].into_iter());

    assert_eq!(
        banned_peers.total(),
        1,
        "clearing a never-banned validator IP must not change the banned total"
    );
}

#[test]
fn test_remove_banned_peer_prunes_zero_count_keys() {
    let mut banned_peers = BannedPeers::default();

    // ban a single peer on each of many distinct IPs
    let ips: Vec<IpAddr> = (0..64).map(|_| random_ip_addr()).collect();
    for ip in &ips {
        banned_peers.add_banned_peer(&create_peer_with_ips(vec![*ip]));
    }

    // every distinct IP now holds an entry
    assert_eq!(banned_peers.banned_peers_by_ip.len(), ips.len());

    // unban every peer again
    for ip in &ips {
        assert_eq!(banned_peers.remove_banned_peer(std::iter::once(*ip)), vec![*ip]);
    }

    // the map must shrink back to empty rather than leaking a permanent
    // zero-count entry per distinct banned IP
    assert!(
        banned_peers.banned_peers_by_ip.is_empty(),
        "zero-count keys should be pruned, found {} lingering entries",
        banned_peers.banned_peers_by_ip.len()
    );
}

#[test]
fn test_remove_banned_peer_keeps_entry_until_zero() {
    let mut banned_peers = BannedPeers::default();
    let ip = random_ip_addr();

    // two banned peers share the same IP
    for _ in 0..2 {
        banned_peers.add_banned_peer(&create_peer_with_ips(vec![ip]));
    }
    assert_eq!(banned_peers.banned_peers_by_ip.get(&ip), Some(&2));

    // first removal decrements but must keep the entry (count still above zero)
    banned_peers.remove_banned_peer(std::iter::once(ip));
    assert_eq!(banned_peers.banned_peers_by_ip.get(&ip), Some(&1));

    // second removal drives the count to zero and must prune the entry
    banned_peers.remove_banned_peer(std::iter::once(ip));
    assert!(!banned_peers.banned_peers_by_ip.contains_key(&ip));
}
