//! Unit tests for `AllPeers`

use super::*;
use crate::common::{create_multiaddr, ensure_score_config, random_ip_addr};
use libp2p::PeerId;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    net::IpAddr,
    time::{Duration, Instant},
};
use tn_config::{PeerConfig, ScoreConfig};
use tn_types::{BlsKeypair, NetworkKeypair};

/// Helper function to create a test AllPeers instance
fn create_all_peers(peer_config: Option<PeerConfig>) -> AllPeers {
    let config = peer_config.unwrap_or_default();
    ensure_score_config(Some(config.score_config));
    let dial_timeout = Duration::from_secs(5);
    AllPeers::new(dial_timeout, config.max_banned_peers, config.max_disconnected_peers)
}

#[test]
fn test_add_trusted_peer() {
    let config = ScoreConfig::default();
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);

    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    all_peers.add_trusted_peer(bls, net, vec![addr.clone()]);

    assert!(all_peers.peers.contains_key(&peer_id));
    let peer = all_peers.peers.get_mut(&peer_id).unwrap();
    assert_eq!(peer.reputation(), Reputation::Trusted);
    assert_eq!(peer.score().aggregate_score(), config.max_score);

    // assert that peer exchange doesn't include address for disconnected
    assert!(!peer.exchange_info().unwrap().1.iter().any(|a| a == &addr));

    // update connection and assert exchange info
    peer.register_outgoing(addr.clone());
    assert!(peer.exchange_info().unwrap().1.iter().any(|a| a == &addr));
}

#[test]
fn test_process_penalty() {
    let config = ScoreConfig::default();
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // add connected peer first and set as this node dialed
    let mut peer = Peer::default_for_test();
    peer.set_connection_status(ConnectionStatus::Connected { num_in: 0, num_out: 1 });
    all_peers.peers.insert(peer_id, peer);

    // test penalty that doesn't change reputation
    let action = all_peers.process_penalty(&peer_id, Penalty::Mild);
    assert!(matches!(action, PeerAction::NoAction));

    let mut action = PeerAction::NoAction;
    // test penalty that causes disconnection
    while all_peers.get_peer(&peer_id).map(|p| p.score().aggregate_score()).unwrap()
        > config.min_score_before_disconnect
    {
        action = all_peers.process_penalty(&peer_id, Penalty::Severe);
    }
    assert!(matches!(action, PeerAction::Disconnect));

    // test penalty that causes ban
    let action = all_peers.process_penalty(&peer_id, Penalty::Fatal);
    // assert no action for disconnecting peer
    assert!(matches!(action, PeerAction::NoAction));
    // assert ban applied after disconnect
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let score = all_peers.get_peer(&peer_id).map(|p| p.score().aggregate_score());
    assert_eq!(score, Some(config.min_score));

    // test penalty for unknown peer
    let unknown_peer_id = PeerId::random();
    let action = all_peers.process_penalty(&unknown_peer_id, Penalty::Fatal);
    assert!(matches!(action, PeerAction::NoAction));
}

#[test]
fn test_ensure_peer_exists() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Unknown peer, valid initial state
    let status = all_peers.ensure_peer_exists(&peer_id, &NewConnectionStatus::Dialing);
    assert!(matches!(status, ConnectionStatus::Unknown));
    assert!(all_peers.peers.contains_key(&peer_id));

    // now peer exists with default status
    all_peers.peers.clear();
    let status = all_peers.ensure_peer_exists(&peer_id, &NewConnectionStatus::Banned);
    assert!(matches!(status, ConnectionStatus::Unknown));

    // Check that peer is banned when new status is Banned
    let peer = all_peers.peers.get(&peer_id).unwrap();
    assert_eq!(peer.reputation(), Reputation::Banned);
}

#[test]
fn test_peer_exchange() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    for i in 1..4 {
        let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let peer_id: PeerId = network_key.clone().into();
        let addr = create_multiaddr(None);

        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Connected {
                multiaddr: addr.clone(),
                direction: ConnectionDirection::Incoming,
            },
        );
        let mut rng = StdRng::from_seed([i; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        all_peers.upsert_peer(bls, network_key, vec![addr]);
    }

    // Add a disconnected peer
    let disc_peer_id = PeerId::random();
    all_peers.update_connection_status(&disc_peer_id, NewConnectionStatus::Disconnected);

    let exchange = all_peers.peer_exchange();
    assert_eq!(exchange.0.len(), 3);
    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    assert!(!exchange.0.contains_key(&bls));
}

#[test]
fn test_connected_peers_by_score() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    let mut first_peer_id = PeerId::random();
    for i in 1..4 {
        let new_peer_id = PeerId::random();
        let addr = create_multiaddr(None);

        all_peers.update_connection_status(
            &new_peer_id,
            NewConnectionStatus::Connected {
                multiaddr: addr,
                direction: ConnectionDirection::Incoming,
            },
        );
        // track first peer id
        if i == 1 {
            first_peer_id = new_peer_id;
        }
    }

    // update last peer id as routable
    all_peers.update_routing_for_peer(&first_peer_id, true);

    let peers_by_score = all_peers.connected_peers_by_score_and_routability();
    assert_eq!(peers_by_score.len(), 3);

    // Ensure they're sorted by score
    for i in 1..peers_by_score.len() {
        assert!(peers_by_score[i - 1].1.score() <= peers_by_score[i].1.score());
    }

    // assert routable peer is last
    assert!(peers_by_score.last().map(|peer| peer.1.is_routable()).unwrap())
}

#[test]
fn test_heartbeat_maintenance() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Add a dialing peer
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    // Manually set the dialing time to be older than the timeout
    if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
        if peer.connection_status().is_dialing() {
            peer.set_connection_status(ConnectionStatus::Dialing {
                instant: Instant::now() - Duration::from_secs(10),
            }); // Older than the 5s timeout
        }
    }

    // Run heartbeat maintenance
    let _ = all_peers.heartbeat_maintenance();

    // The peer should now be disconnected
    let peer = all_peers.peers.get(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
    assert_eq!(all_peers.disconnected_peers, 1);
}

#[test]
fn test_pruning_logic() {
    let config = PeerConfig::default();
    let mut all_peers = create_all_peers(Some(config));

    // Add many disconnected peers
    let peer_num = 101;
    for i in 1..=peer_num {
        let peer_id = PeerId::random();

        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

        // Manually set disconnection time to be older for first peers
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            let disconnected =
                matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. });
            if disconnected {
                // set a deterministic order - earlier IDs are older
                peer.set_connection_status(ConnectionStatus::Disconnected {
                    instant: Instant::now() - Duration::from_secs(i as u64),
                });
            }
        }
    }

    assert_eq!(all_peers.disconnected_peers, peer_num);

    // Pruning happens in register_disconnected
    all_peers.register_disconnected(&PeerId::random());

    // Should be pruned to MAX_DISCONNECTED_PEERS (1000)
    assert_eq!(all_peers.disconnected_peers, config.max_disconnected_peers);
    assert_eq!(all_peers.peers.len(), config.max_disconnected_peers);

    // Now test banned peers pruning
    let mut all_peers = create_all_peers(None);

    // Add many banned peers
    for i in 1..=peer_num {
        let peer_id = PeerId::random();

        // Need to go through disconnecting first
        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { banned: true },
        );
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

        // Manually set banned time to be older for first peers
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            let banned = matches!(peer.connection_status(), ConnectionStatus::Banned { .. });
            if banned {
                // set a deterministic order - earlier IDs are older
                peer.set_connection_status(ConnectionStatus::Banned {
                    instant: Instant::now() - Duration::from_secs(i as u64),
                });
            }
        }
    }

    assert_eq!(all_peers.banned_peers.total(), peer_num);

    // Trigger pruning
    let (_, pruned) = all_peers.register_disconnected(&PeerId::random());

    // Should be pruned to MAX_BANNED_PEERS (1000)
    assert_eq!(all_peers.banned_peers.total(), config.max_banned_peers);
    let expected = peer_num - config.max_banned_peers;
    assert_eq!(pruned.len(), expected); // 1 peer should be pruned
}

#[test]
fn test_is_validator() {
    ensure_score_config(None);
    let validator_id = PeerId::random();
    let mut all_peers = AllPeers::new(Duration::from_secs(5), 10, 10);
    all_peers.current_committee.insert(validator_id);
    assert!(all_peers.is_peer_validator(&validator_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_ip_and_peer_banned() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let ip = IpAddr::V4("52.3.3.3".parse().unwrap());
    let addr = create_multiaddr(Some(ip));

    // Add a peer and ban it
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers
        .update_connection_status(&peer_id, NewConnectionStatus::Disconnecting { banned: true });
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let banned = all_peers.peer_banned(&peer_id);
    assert!(!banned);

    // check if IP is banned
    assert!(!all_peers.ip_banned(&ip));
    assert!(!all_peers.ip_banned(&random_ip_addr()));

    // new peer connects from same IP
    let new_peer = PeerId::random();
    all_peers.update_connection_status(
        &new_peer,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    all_peers
        .update_connection_status(&new_peer, NewConnectionStatus::Disconnecting { banned: true });
    let action = all_peers.update_connection_status(&new_peer, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));
    let banned = all_peers.peer_banned(&new_peer);
    assert!(banned);

    // Check if peer is banned
    assert!(all_peers.peer_banned(&peer_id));
    assert!(!all_peers.peer_banned(&PeerId::random()));
}

#[test]
fn test_connected_peer_methods() {
    let mut all_peers = create_all_peers(None);

    // Add some connected peers
    let connected_peer_id = PeerId::random();
    let dialing_peer_id = PeerId::random();
    let disconnected_peer_id = PeerId::random();
    let disconnecting_peer_id = PeerId::random();

    all_peers.update_connection_status(
        &connected_peer_id,
        NewConnectionStatus::Connected {
            multiaddr: create_multiaddr(None),
            direction: ConnectionDirection::Incoming,
        },
    );

    all_peers.update_connection_status(&dialing_peer_id, NewConnectionStatus::Dialing);

    all_peers.update_connection_status(&disconnected_peer_id, NewConnectionStatus::Disconnected);

    all_peers.update_connection_status(
        &disconnecting_peer_id,
        NewConnectionStatus::Disconnecting { banned: false },
    );

    // Test connected_peer_ids
    let connected_ids: Vec<_> = all_peers.connected_peer_ids().collect();
    assert_eq!(connected_ids.len(), 1);
    assert!(connected_ids.contains(&&connected_peer_id));

    // Test connected_or_dialing_peers
    let connected_or_dialing: Vec<_> = all_peers.connected_or_dialing_peers();
    assert_eq!(connected_or_dialing.len(), 2);
    assert!(connected_or_dialing.contains(&connected_peer_id));
    assert!(connected_or_dialing.contains(&dialing_peer_id));

    // Test is_peer_connected_or_disconnecting
    assert!(all_peers.is_peer_connected_or_disconnecting(&connected_peer_id));
    assert!(all_peers.is_peer_connected_or_disconnecting(&disconnecting_peer_id));
    assert!(!all_peers.is_peer_connected_or_disconnecting(&dialing_peer_id));
    assert!(!all_peers.is_peer_connected_or_disconnecting(&disconnected_peer_id));
}

#[test]
fn test_unknown_to_connected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Test Unknown -> Connected (incoming)
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    assert!(matches!(action, PeerAction::NoAction));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { num_in, .. }
            if *num_in == 1));
}

#[test]
fn test_connected_to_disconnecting_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    // Test Connected -> Disconnecting
    let action = all_peers
        .update_connection_status(&peer_id, NewConnectionStatus::Disconnecting { banned: false });

    assert!(matches!(action, PeerAction::DisconnectWithPX));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnecting { banned }
            if !(*banned)));
}

#[test]
fn test_disconnecting_to_disconnected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected -> Disconnecting
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );
    let action = all_peers
        .update_connection_status(&peer_id, NewConnectionStatus::Disconnecting { banned: false });
    assert!(matches!(action, PeerAction::DisconnectWithPX));

    // Test Disconnecting -> Disconnected
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(all_peers.disconnected_peers, 1);
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
}

#[test]
fn test_disconnected_to_dialing_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // set to Disconnected
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    assert_eq!(all_peers.disconnected_peers, 1);

    // Test Disconnected -> Dialing
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(all_peers.disconnected_peers, 0); // Counter should be decremented
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Dialing { .. }));
}

#[test]
fn test_dialing_to_connected_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Dialing
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Dialing);

    // Test Dialing -> Connected
    let action = all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Outgoing,
        },
    );

    assert!(matches!(action, PeerAction::NoAction));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { num_out, .. }
            if *num_out == 1));
}

#[test]
fn test_disconnected_to_banned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Setup: Set to Disconnected
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

    // Test Disconnected -> Banned
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);
    assert!(matches!(action, PeerAction::Ban(_)));
    assert_eq!(all_peers.disconnected_peers, 0); // counter should be decremented

    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Banned { .. }));
}

#[test]
fn test_banned_to_unbanned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // Setup: Disconnected -> Banned
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

    // Test Banned -> Unbanned
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);

    assert!(matches!(action, PeerAction::Unban(_)));
    assert_eq!(all_peers.disconnected_peers, 1); // Counter should be incremented
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
}

#[test]
fn test_connected_to_banned_transition() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // Setup: Unknown -> Connected
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );

    // Test Connected -> Banned (should go through Disconnecting)
    let action = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

    assert!(matches!(action, PeerAction::Disconnect));
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnecting { banned }
            if *banned));
}

#[test]
fn test_ban_action_returns_only_unbanned_ips() {
    let mut all_peers = create_all_peers(None);

    // create IPs
    let ip1 = IpAddr::V4("192.168.1.1".parse().unwrap());
    let ip2 = IpAddr::V4("192.168.1.2".parse().unwrap());

    // first peer with ip1
    let peer1 = PeerId::random();
    let addr1 = create_multiaddr(Some(ip1));
    all_peers.update_connection_status(
        &peer1,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer1, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&peer1, NewConnectionStatus::Disconnected);

    // second peer also from ip1 - this should trigger IP ban
    let peer2 = PeerId::random();
    all_peers.update_connection_status(
        &peer2,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer2, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&peer2, NewConnectionStatus::Disconnected);

    // at this point, ip1 should be banned (2 peers banned from this IP)
    assert!(all_peers.ip_banned(&ip1), "ip1 should be IP-banned after 2 peers banned");

    // now create a NEW peer that connects from ip2 but also has ip1 in its known addresses
    // NOTE: this should not happen in production, but this test is to ensure only new IP addresses
    // are returned from ban list
    let peer3 = PeerId::random();

    // connect from ip2
    let addr2 = create_multiaddr(Some(ip2));
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Connected {
            multiaddr: addr2.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    // disconnect and reconnect from ip1 to add it to known addresses
    all_peers
        .update_connection_status(&peer3, NewConnectionStatus::Disconnecting { banned: false });
    all_peers.update_connection_status(&peer3, NewConnectionStatus::Disconnected);
    all_peers.update_connection_status(
        &peer3,
        NewConnectionStatus::Connected {
            multiaddr: addr1.clone(), // connect from the already-banned IP
            direction: ConnectionDirection::Incoming,
        },
    );

    // now peer3 has both ip1 (banned) and ip2 (not banned) in its history
    // ban peer3
    all_peers.update_connection_status(&peer3, NewConnectionStatus::Disconnecting { banned: true });
    let action = all_peers.update_connection_status(&peer3, NewConnectionStatus::Disconnected);

    if let PeerAction::Ban(ips) = action {
        assert_eq!(ips.len(), 1, "Should only ban ip2 since ip1 is already IP-banned");
        assert!(ips.contains(&ip2), "Should ban ip2");
        assert!(!ips.contains(&ip1), "Should NOT include ip1 as it's already IP-banned");
    } else {
        panic!("Expected Ban action for peer3");
    }
}

// -----------------------------------------------------------------------------
// bls_index invariant tests
// -----------------------------------------------------------------------------

/// Helper: construct a fresh (bls, network_key, peer_id) triple from a deterministic seed.
fn bls_net_peer(seed: u8) -> (BlsPublicKey, NetworkPublicKey, PeerId) {
    let mut rng = StdRng::from_seed([seed; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    (bls, net, peer_id)
}

#[test]
fn bls_rotation_same_peer_id_evicts_old_index() {
    // Same PeerId, new BLS key — the old BLS must no longer resolve.
    //
    // NOTE: Validators can rotate peer ids but should not rotate BLS keys.
    // Observer nodes are discouraged from doing this but technically capable.
    // This test demonstrates how the protocol handles this behavior.
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);
    let (_bls1, net, peer_id) = bls_net_peer(1);

    let mut rng = StdRng::from_seed([2; 32]);
    let bls1 = *BlsKeypair::generate(&mut rng).public();
    let mut rng = StdRng::from_seed([3; 32]);
    let bls2 = *BlsKeypair::generate(&mut rng).public();

    all_peers.upsert_peer(bls1, net.clone(), vec![addr.clone()]);
    assert_eq!(all_peers.peer_to_bls(&peer_id), Some(bls1));
    assert!(all_peers.auth_to_peer(&bls1).is_some());

    // rotate BLS on the same PeerId
    all_peers.upsert_peer(bls2, net, vec![addr.clone()]);

    // bug B regression: old BLS must not resolve to a live-looking entry
    assert!(all_peers.auth_to_peer(&bls1).is_none(), "stale BLS must not resolve");
    assert!(!all_peers.contains_bls(&bls1));
    assert_eq!(all_peers.auth_to_peer(&bls2).map(|(pid, _)| pid), Some(peer_id));
    assert_eq!(all_peers.peer_to_bls(&peer_id), Some(bls2));
}

#[test]
fn peer_id_rotation_same_bls_orphans_old_peer() {
    // Same BLS key, new PeerId — the orphan peer record survives but loses its BLS.
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);

    let (_, net1, peer_id_1) = bls_net_peer(4);
    let (_, net2, peer_id_2) = bls_net_peer(5);
    let mut rng = StdRng::from_seed([6; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();

    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers.upsert_peer(bls, net2, vec![addr]);

    // index now points at the new peer
    assert_eq!(all_peers.auth_to_peer(&bls).map(|(pid, _)| pid), Some(peer_id_2));
    // orphan still exists in peers map but has no BLS
    assert!(all_peers.get_peer(&peer_id_1).is_some(), "orphan peer should survive");
    assert_eq!(all_peers.peer_to_bls(&peer_id_1), None, "orphan BLS must be cleared");
}

/// Regression rotation arm of `upsert_peer`: a sitting committee validator
/// that rotates its libp2p PeerId mid-epoch (operator-driven keypair seed change)
/// must have the *new* PeerId promoted into the committee, the *old* PeerId removed
/// from `current_committee`, and the orphan record retained with `is_trusted == true`
/// so INV-045 pruning protection still applies.
#[test]
fn committee_member_rotation_promotes_new_peer_id() {
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);

    let (_, net_a, peer_a) = bls_net_peer(70);
    let (_, net_b, peer_b) = bls_net_peer(71);
    let mut rng = StdRng::from_seed([72; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();

    // new_epoch with an unknown BLS — committee_keys[bls] = None.
    let mut committee = HashSet::new();
    committee.insert(bls);
    let (_actions, unresolved) = all_peers.new_epoch(committee);
    assert_eq!(unresolved, vec![bls], "BLS is unknown before any upsert");

    // First upsert promotes via the unresolved arm (existing behavior).
    all_peers.upsert_peer(bls, net_a, vec![addr.clone()]);
    assert_eq!(all_peers.auth_to_peer(&bls).map(|(pid, _)| pid), Some(peer_a));
    assert!(all_peers.is_peer_validator(&peer_a), "peer_a must be the sitting CVV");
    assert!(all_peers.get_peer(&peer_a).unwrap().is_trusted());
    assert_eq!(all_peers.current_committee_keys.get(&bls), Some(&Some(peer_a)));

    // Second upsert with the same BLS but a different PeerId — the rotation arm
    // must (a) promote peer_b, (b) evict peer_a from current_committee, and
    // (c) leave peer_a's record alive with is_trusted preserved.
    all_peers.upsert_peer(bls, net_b, vec![addr]);

    // peer_b is the new sitting CVV.
    assert_eq!(all_peers.auth_to_peer(&bls).map(|(pid, _)| pid), Some(peer_b));
    assert!(all_peers.is_peer_validator(&peer_b), "peer_b must be the sitting CVV after rotation");
    assert!(all_peers.get_peer(&peer_b).unwrap().is_trusted());
    assert_eq!(all_peers.current_committee_keys.get(&bls), Some(&Some(peer_b)));

    // peer_a is no longer in current_committee, but its record survives with
    // is_trusted == true and bls_public_key cleared (rebind_bls handled the index).
    assert!(
        !all_peers.is_peer_validator(&peer_a),
        "stale peer_a must be removed from current_committee"
    );
    let orphan = all_peers.get_peer(&peer_a).expect("orphan peer record must survive");
    assert!(orphan.is_trusted(), "INV-045: orphan keeps is_trusted so pruning protection holds");
    assert_eq!(all_peers.peer_to_bls(&peer_a), None, "orphan BLS must be cleared by rebind_bls");
}

#[test]
fn prune_disconnected_clears_bls_index() {
    let config = PeerConfig { max_disconnected_peers: 2, ..PeerConfig::default() };
    let mut all_peers = create_all_peers(Some(config));

    // pre-load three disconnected peers, each with a BLS in the index
    let mut bls_keys = Vec::new();
    let mut peer_ids = Vec::new();
    for i in 0..3 {
        let (_, net, peer_id) = bls_net_peer(10 + i);
        let mut rng = StdRng::from_seed([100 + i; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        let addr = create_multiaddr(None);
        all_peers.upsert_peer(bls, net, vec![addr]);
        // age the disconnect instant so prune ordering is deterministic
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            peer.set_connection_status(ConnectionStatus::Disconnected {
                instant: Instant::now() - Duration::from_secs(i as u64 + 1),
            });
        }
        bls_keys.push(bls);
        peer_ids.push(peer_id);
    }

    assert_eq!(all_peers.disconnected_peers, 3);
    // trigger prune via register_disconnected
    all_peers.register_disconnected(&PeerId::random());

    // the oldest peer should be pruned and its BLS no longer indexed
    assert_eq!(all_peers.disconnected_peers, config.max_disconnected_peers);
    let pruned = bls_keys.iter().filter(|b| !all_peers.contains_bls(b)).count();
    assert!(pruned >= 1, "at least one BLS must be evicted from the index");
}

#[test]
fn inbound_unknown_lifecycle() {
    // A peer that connects inbound before we know its BLS must remain BLS-less
    // and become indexable once upsert_peer is called.
    let mut all_peers = create_all_peers(None);
    let (_, net, peer_id) = bls_net_peer(42);
    let addr = create_multiaddr(None);

    // simulate inbound connection from an unknown peer (Peer::default created via ensure)
    all_peers.update_connection_status(
        &peer_id,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    // no BLS known yet, and no index entry for any BLS
    assert_eq!(all_peers.peer_to_bls(&peer_id), None);

    // learn the BLS via upsert_peer (e.g. from a NodeRecord); since the network key
    // hashes to peer_id, the same Peer entry is updated rather than replaced.
    let mut rng = StdRng::from_seed([43; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    all_peers.upsert_peer(bls, net, vec![addr]);

    assert_eq!(all_peers.peer_to_bls(&peer_id), Some(bls));
    assert_eq!(all_peers.auth_to_peer(&bls).map(|(pid, _)| pid), Some(peer_id));
}

#[test]
fn new_epoch_resolves_via_index() {
    let mut all_peers = create_all_peers(None);
    let (_, net, peer_id) = bls_net_peer(50);
    let mut rng = StdRng::from_seed([51; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let addr = create_multiaddr(None);

    // register the peer up-front
    all_peers.upsert_peer(bls, net, vec![addr]);
    assert!(!all_peers.get_peer(&peer_id).unwrap().is_trusted());

    let mut committee = HashSet::new();
    committee.insert(bls);
    let (_actions, unresolved) = all_peers.new_epoch(committee);
    assert!(unresolved.is_empty(), "BLS was in bls_index so committee resolves cleanly");

    // peer should now be trusted and present in current_committee
    let peer = all_peers.get_peer(&peer_id).unwrap();
    assert!(peer.is_trusted(), "committee member must be trusted after new_epoch");
    assert!(all_peers.is_peer_validator(&peer_id));
}

#[test]
fn new_epoch_warns_on_unknown_member() {
    let mut all_peers = create_all_peers(None);
    let mut rng = StdRng::from_seed([60; 32]);
    let unknown_bls = *BlsKeypair::generate(&mut rng).public();

    let mut committee = HashSet::new();
    committee.insert(unknown_bls);
    // should not panic and should produce no unban actions
    let (actions, unresolved) = all_peers.new_epoch(committee);
    assert!(actions.is_empty());
    assert_eq!(unresolved, vec![unknown_bls]);
    assert!(!all_peers.contains_bls(&unknown_bls));
}

/// Regression: a committee validator that gets banned mid-epoch must survive
/// `prune_banned_peers` (it is protected by `is_protected`), and the next `new_epoch`
/// must reaffirm it as a trusted committee member — no kad re-discovery required.
#[test]
fn test_banned_committee_member_survives_prune_and_remains_committee() {
    // Set max_banned_peers = 0 so that any unprotected banned peer would be pruned.
    let config = PeerConfig { max_banned_peers: 0, ..PeerConfig::default() };
    let mut all_peers = create_all_peers(Some(config));

    // Peer A: the validator we care about.
    let (_, net_a, peer_a) = bls_net_peer(80);
    let mut rng = StdRng::from_seed([81; 32]);
    let bls_a = *BlsKeypair::generate(&mut rng).public();
    let addr_a = create_multiaddr(Some(IpAddr::V4("10.0.0.10".parse().unwrap())));
    all_peers.upsert_peer(bls_a, net_a.clone(), vec![addr_a.clone()]);
    assert!(all_peers.contains_bls(&bls_a), "precondition: bls_a indexed");

    // Promote peer A to committee FIRST so it is `is_trusted == true` going into the
    // adversarial scenario. Penalties are now no-ops on this peer (INV-002), so the
    // only way it can hit `Banned` is via an explicit state-machine transition (an
    // operator-issued ban or a `Disconnecting { banned: true }` from the state engine).
    let mut committee = HashSet::new();
    committee.insert(bls_a);
    let (actions, unresolved) = all_peers.new_epoch(committee.clone());
    assert!(unresolved.is_empty(), "bls_a is in bls_index so committee resolves immediately");
    assert!(actions.is_empty(), "no unban needed: peer A was not previously banned");
    assert!(
        all_peers.get_peer(&peer_a).unwrap().is_trusted(),
        "precondition: peer A is trusted after new_epoch"
    );
    assert!(all_peers.is_peer_validator(&peer_a));

    // Drive peer A into Banned via explicit state transitions (simulates an
    // operator-issued ban or a non-score-driven state-machine ban).
    all_peers.update_connection_status(
        &peer_a,
        NewConnectionStatus::Connected {
            multiaddr: addr_a.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers
        .update_connection_status(&peer_a, NewConnectionStatus::Disconnecting { banned: true });
    let action_a = all_peers.update_connection_status(&peer_a, NewConnectionStatus::Disconnected);
    assert!(matches!(action_a, PeerAction::Ban(_)), "peer A must transition to Ban");
    assert_eq!(all_peers.banned_peers.total(), 1);
    assert!(matches!(
        all_peers.peers.get(&peer_a).map(|p| *p.connection_status()),
        Some(ConnectionStatus::Banned { .. })
    ));

    // Trigger pruning. The protection predicate must skip peer A even though
    // max_banned_peers = 0 and peer A is the only banned peer.
    let (_, pruned) = all_peers.register_disconnected(&PeerId::random());
    assert!(pruned.is_empty(), "fix: protected (committee) peer must not be pruned");

    // The peer record and bls_index entry survive pruning.
    assert!(
        all_peers.contains_bls(&bls_a),
        "fix: protected peer's bls_index entry survives pruning"
    );
    assert!(all_peers.get_peer(&peer_a).is_some(), "fix: protected peer's record survives pruning");

    // The next epoch boundary still resolves the BLS via bls_index and the
    // promotion helper unbans peer A.
    let (actions, unresolved) = all_peers.new_epoch(committee);
    assert!(unresolved.is_empty(), "BLS still indexed; nothing to ask kad about");
    assert_eq!(actions.len(), 1, "banned peer A produces an unban action on new_epoch");
    let (resolved_peer, action) = &actions[0];
    assert_eq!(*resolved_peer, peer_a);
    assert!(matches!(action, PeerAction::Unban(_)));

    // Final state: peer A is trusted, in the current committee, no longer banned.
    let peer = all_peers.get_peer(&peer_a).expect("peer record present");
    assert!(peer.is_trusted(), "fix: peer A remains trusted across the ban + new_epoch cycle");
    assert!(all_peers.is_peer_validator(&peer_a));
    assert!(!all_peers.peer_banned(&peer_a), "fix: peer A is unbanned by promote_committee_member");
    assert_eq!(
        all_peers.current_committee_keys.get(&bls_a),
        Some(&Some(peer_a)),
        "fix: BLS resolves back to peer A in current_committee_keys"
    );
}

#[test]
fn prune_skips_protected_peer_logs_warn() {
    // One trusted (banned) peer mixed with three non-trusted banned peers; with
    // max_banned_peers = 2 the prune must skip the trusted one and evict two
    // unprotected ones.
    let config = PeerConfig { max_banned_peers: 2, ..PeerConfig::default() };
    let mut all_peers = create_all_peers(Some(config));

    // Trusted banned peer.
    let (_, net_t, peer_t) = bls_net_peer(90);
    let mut rng = StdRng::from_seed([91; 32]);
    let bls_t = *BlsKeypair::generate(&mut rng).public();
    let addr_t = create_multiaddr(Some(IpAddr::V4("10.0.1.1".parse().unwrap())));
    all_peers.add_trusted_peer(bls_t, net_t, vec![addr_t.clone()]);
    all_peers
        .update_connection_status(&peer_t, NewConnectionStatus::Disconnecting { banned: true });
    let action = all_peers.update_connection_status(&peer_t, NewConnectionStatus::Disconnected);
    assert!(matches!(action, PeerAction::Ban(_)));

    // Three non-trusted banned peers, aged so prune ordering is deterministic.
    let mut unprotected = Vec::new();
    for i in 0..3u8 {
        let peer_id = PeerId::random();
        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { banned: true },
        );
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        // age the banned instant
        if let Some(peer) = all_peers.peers.get_mut(&peer_id) {
            peer.set_connection_status(ConnectionStatus::Banned {
                instant: Instant::now() - Duration::from_secs((i as u64) + 1),
            });
        }
        unprotected.push(peer_id);
    }

    assert_eq!(all_peers.banned_peers.total(), 4);

    // Trigger pruning.
    let (_, pruned) = all_peers.register_disconnected(&PeerId::random());

    // Trusted peer must survive; exactly two unprotected peers are pruned.
    assert!(all_peers.get_peer(&peer_t).is_some(), "trusted banned peer must survive prune");
    assert_eq!(pruned.len(), 2, "two unprotected peers should be pruned");
    for (pruned_peer, _) in &pruned {
        assert!(
            unprotected.iter().any(|id| id == pruned_peer),
            "every pruned peer must be from the unprotected set"
        );
    }
}

#[test]
fn prune_with_only_protected_peers_keeps_all() {
    // All banned peers are trusted; prune evicts nothing even though every peer is
    // technically excess. The banned-peer total stays above the limit by design.
    let config = PeerConfig { max_banned_peers: 0, ..PeerConfig::default() };
    let mut all_peers = create_all_peers(Some(config));

    // Add all trusted peers first — `add_trusted_peer` calls `remove_banned_peer`
    // which decrements `banned_peers.total` unconditionally, so interleaving with
    // bans would cancel them out. Adding upfront keeps the bookkeeping clean.
    let mut peer_ids = Vec::new();
    for seed in 0u8..3 {
        let mut rng = StdRng::from_seed([100 + seed; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let peer_id: PeerId = net.clone().into();
        let addr = create_multiaddr(None);
        all_peers.add_trusted_peer(bls, net, vec![addr]);
        peer_ids.push(peer_id);
    }

    // Now drive each trusted peer through the state machine into the Banned state.
    for peer_id in &peer_ids {
        all_peers
            .update_connection_status(peer_id, NewConnectionStatus::Disconnecting { banned: true });
        all_peers.update_connection_status(peer_id, NewConnectionStatus::Disconnected);
    }

    assert_eq!(all_peers.banned_peers.total(), 3);
    let (_, pruned) = all_peers.register_disconnected(&PeerId::random());
    assert!(pruned.is_empty(), "no unprotected peers means no eviction");
    assert!(
        all_peers.banned_peers.total() > config.max_banned_peers,
        "temporary excess is accepted to protect trusted peers"
    );
}

#[test]
fn new_epoch_reports_unresolved_bls_keys() {
    let mut all_peers = create_all_peers(None);
    let mut rng = StdRng::from_seed([110; 32]);
    let unknown_bls = *BlsKeypair::generate(&mut rng).public();

    let mut committee = HashSet::new();
    committee.insert(unknown_bls);
    let (actions, unresolved) = all_peers.new_epoch(committee);

    assert!(actions.is_empty(), "no actions for an unresolved BLS key");
    assert_eq!(unresolved, vec![unknown_bls]);
    assert_eq!(
        all_peers.current_committee_keys.get(&unknown_bls),
        Some(&None),
        "unresolved BLS is tracked with None peer_id"
    );
}

#[test]
fn upsert_peer_promotes_unresolved_committee_member() {
    let mut all_peers = create_all_peers(None);

    // new_epoch first — with bls_a NOT in bls_index, so it lands in
    // current_committee_keys[bls_a] = None.
    let (_, net_a, peer_a) = bls_net_peer(120);
    let mut rng = StdRng::from_seed([121; 32]);
    let bls_a = *BlsKeypair::generate(&mut rng).public();
    let addr_a = create_multiaddr(None);

    let mut committee = HashSet::new();
    committee.insert(bls_a);
    let (actions, unresolved) = all_peers.new_epoch(committee);
    assert!(actions.is_empty());
    assert_eq!(unresolved, vec![bls_a]);

    // upsert_peer with bls_a (simulating add_known_peer driven by a kad lookup result).
    // The peer is freshly inserted (no prior connection state), so no Unban action
    // is required — promote_committee_member returns None and upsert_peer reports None.
    // The promotion side effects must still land.
    let promoted = all_peers.upsert_peer(bls_a, net_a, vec![addr_a]);
    assert!(promoted.is_none(), "no unban action needed for a freshly-inserted committee peer");

    // promotion side-effects
    assert!(all_peers.current_committee.contains(&peer_a));
    assert!(all_peers.is_peer_validator(&peer_a));
    assert!(all_peers.get_peer(&peer_a).unwrap().is_trusted());
    assert_eq!(all_peers.current_committee_keys.get(&bls_a), Some(&Some(peer_a)));
}

#[test]
fn upsert_peer_promotes_banned_committee_member_via_kad() {
    // Variant of the above where the peer was already banned before `new_epoch` marked
    // its BLS as unresolved — `upsert_peer` must lift the ban as part of promotion.
    let mut all_peers = create_all_peers(None);
    let (_, net_a, peer_a) = bls_net_peer(125);
    let mut rng = StdRng::from_seed([126; 32]);
    let bls_a = *BlsKeypair::generate(&mut rng).public();
    let addr_a = create_multiaddr(Some(IpAddr::V4("10.0.2.5".parse().unwrap())));

    // First contact: peer is registered, then driven into the Banned state, then
    // forgotten by `bls_index` via a rotation orphan (or equivalent). Here we use a
    // direct BLS rotation so the original bls_a is no longer indexed, but the peer
    // record at peer_a survives in Banned state.
    all_peers.upsert_peer(bls_a, net_a.clone(), vec![addr_a.clone()]);
    all_peers.update_connection_status(
        &peer_a,
        NewConnectionStatus::Connected {
            multiaddr: addr_a.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers
        .update_connection_status(&peer_a, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&peer_a, NewConnectionStatus::Disconnected);
    assert!(matches!(
        all_peers.peers.get(&peer_a).map(|p| *p.connection_status()),
        Some(ConnectionStatus::Banned { .. })
    ));

    // Rotate the BLS so the prior bls_a entry is orphaned — bls_a is no longer in
    // bls_index, so the next new_epoch will treat it as unresolved.
    let mut rng2 = StdRng::from_seed([127; 32]);
    let other_bls = *BlsKeypair::generate(&mut rng2).public();
    all_peers.upsert_peer(other_bls, net_a.clone(), vec![addr_a.clone()]);
    assert!(!all_peers.contains_bls(&bls_a));

    // new_epoch records bls_a as unresolved.
    let mut committee = HashSet::new();
    committee.insert(bls_a);
    let (_, unresolved) = all_peers.new_epoch(committee);
    assert_eq!(unresolved, vec![bls_a]);

    // Simulate a kad result that re-binds bls_a back to peer_a — upsert_peer must
    // surface the Unban action AND apply the promotion side effects.
    let promoted = all_peers.upsert_peer(bls_a, net_a, vec![addr_a]);
    let (promoted_peer, action) =
        promoted.expect("promotion must surface an unban action for the banned committee peer");
    assert_eq!(promoted_peer, peer_a);
    assert!(matches!(action, PeerAction::Unban(_)));
    assert!(all_peers.current_committee.contains(&peer_a));
    assert!(all_peers.get_peer(&peer_a).unwrap().is_trusted());
    assert_eq!(all_peers.current_committee_keys.get(&bls_a), Some(&Some(peer_a)));
}
