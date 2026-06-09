//! Unit tests for `AllPeers`

use super::*;
use crate::common::{create_multiaddr, ensure_score_config, random_ip_addr};
use libp2p::PeerId;
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use std::{
    collections::HashSet,
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
    all_peers.add_trusted_peer(bls, net);

    assert!(all_peers.get_peer(&peer_id).is_some());
    let peer = all_peers.get_peer_mut(&peer_id).unwrap();
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
    all_peers.insert_unidentified(peer_id, peer);

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
    assert!(all_peers.get_peer(&peer_id).is_some());

    // now peer exists with default status
    all_peers.peers.clear();
    let status = all_peers.ensure_peer_exists(&peer_id, &NewConnectionStatus::Banned);
    assert!(matches!(status, ConnectionStatus::Unknown));

    // Check that peer is banned when new status is Banned
    let peer = all_peers.get_peer(&peer_id).unwrap();
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
    if let Some(peer) = all_peers.get_peer_mut(&peer_id) {
        if peer.connection_status().is_dialing() {
            peer.set_connection_status(ConnectionStatus::Dialing {
                instant: Instant::now() - Duration::from_secs(10),
            }); // Older than the 5s timeout
        }
    }

    // Run heartbeat maintenance
    let _ = all_peers.heartbeat_maintenance();

    // The peer should now be disconnected
    let peer = all_peers.get_peer(&peer_id).unwrap();
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
        if let Some(peer) = all_peers.get_peer_mut(&peer_id) {
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
        if let Some(peer) = all_peers.get_peer_mut(&peer_id) {
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
    let mut all_peers = AllPeers::new(Duration::from_secs(5), 10, 10);

    // a committee member is a confirmed peer (bls known) whose libp2p id resolves to its bls
    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let validator_id: PeerId = net.clone().into();
    all_peers.upsert_peer(bls, net, vec![]);
    all_peers.current_committee.insert(bls);

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
    assert!(connected_ids.contains(&connected_peer_id));

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

/// Build a distinct committee member: (bls key, network key, derived peer id).
fn committee_member(rng: &mut StdRng) -> (BlsPublicKey, NetworkPublicKey, PeerId) {
    let bls = *BlsKeypair::generate(rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    (bls, net, peer_id)
}

/// Simulate kad discovery of a member: record its network identity so its libp2p id resolves to its
/// bls key and a `Confirmed` peer record exists (the precondition for trust/validator-by-peer-id).
fn discover(all_peers: &mut AllPeers, bls: BlsPublicKey, net: &NetworkPublicKey) {
    all_peers.upsert_peer(bls, net.clone(), vec![create_multiaddr(None)]);
}

/// Build `size` distinct, already-discovered members and return them keyed by bls in a slot set,
/// alongside the set of their derived peer ids.
fn random_committee(
    all_peers: &mut AllPeers,
    rng: &mut StdRng,
    size: usize,
) -> (HashSet<BlsPublicKey>, HashSet<PeerId>) {
    let mut bls_set = HashSet::new();
    let mut ids = HashSet::new();
    for _ in 0..size {
        let (bls, net, id) = committee_member(rng);
        discover(all_peers, bls, &net);
        bls_set.insert(bls);
        ids.insert(id);
    }
    (bls_set, ids)
}

#[test]
fn test_update_committees_sets_all_three_slots() {
    let mut rng = StdRng::from_seed([10; 32]);
    let mut all_peers = create_all_peers(None);

    let (p_bls, p_net, p_peer_id) = committee_member(&mut rng);
    let (c_bls, c_net, c_peer_id) = committee_member(&mut rng);
    let (n_bls, n_net, n_peer_id) = committee_member(&mut rng);

    // discover each member so its peer id resolves to its bls
    discover(&mut all_peers, p_bls, &p_net);
    discover(&mut all_peers, c_bls, &c_net);
    discover(&mut all_peers, n_bls, &n_net);

    all_peers.update_committees(
        HashSet::from([p_bls]),
        HashSet::from([c_bls]),
        HashSet::from([n_bls]),
    );

    // slots hold the bls keys directly
    assert!(all_peers.previous_committee.contains(&p_bls));
    assert!(all_peers.current_committee.contains(&c_bls));
    assert!(all_peers.next_committee.contains(&n_bls));
    // discovered members resolve to validators by their peer id
    assert!(all_peers.is_peer_validator(&p_peer_id));
    assert!(all_peers.is_peer_validator(&c_peer_id));
    assert!(all_peers.is_peer_validator(&n_peer_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_update_committees_overwrites_slots_directly() {
    let mut rng = StdRng::from_seed([11; 32]);
    let mut all_peers = create_all_peers(None);

    let (a_bls, ..) = committee_member(&mut rng);
    let (b_bls, ..) = committee_member(&mut rng);
    let (c_bls, ..) = committee_member(&mut rng);
    let (d_bls, ..) = committee_member(&mut rng);
    let (e_bls, ..) = committee_member(&mut rng);
    let (f_bls, ..) = committee_member(&mut rng);

    // first update: previous={A}, current={B}, next={C}. The complete sets are stored directly, so
    // no discovery is required for slot-content assertions.
    all_peers.update_committees(
        HashSet::from([a_bls]),
        HashSet::from([b_bls]),
        HashSet::from([c_bls]),
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([a_bls]));
    assert_eq!(all_peers.current_committee, HashSet::from([b_bls]));
    assert_eq!(all_peers.next_committee, HashSet::from([c_bls]));

    // second update: each slot is set directly from its arg, NOT positionally shifted from the
    // prior round (otherwise current would be the prior next, {C}).
    all_peers.update_committees(
        HashSet::from([d_bls]),
        HashSet::from([e_bls]),
        HashSet::from([f_bls]),
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([d_bls]));
    assert_eq!(all_peers.current_committee, HashSet::from([e_bls]));
    assert_eq!(all_peers.next_committee, HashSet::from([f_bls]));
}

#[test]
fn test_update_committees_no_change() {
    let mut rng = StdRng::from_seed([13; 32]);
    let mut all_peers = create_all_peers(None);

    let (x_bls, x_net, x_id) = committee_member(&mut rng);
    discover(&mut all_peers, x_bls, &x_net);

    // X is in both current and next
    all_peers.update_committees(HashSet::new(), HashSet::from([x_bls]), HashSet::from([x_bls]));

    // repeat the same membership next epoch
    all_peers.update_committees(HashSet::new(), HashSet::from([x_bls]), HashSet::from([x_bls]));

    // full overlap keeps X a validator, present in both current and next, never demoted
    assert!(all_peers.is_peer_validator(&x_id));
    assert!(all_peers.current_committee.contains(&x_bls));
    assert!(all_peers.next_committee.contains(&x_bls));
    assert!(all_peers.get_peer(&x_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_full_turnover() {
    let mut rng = StdRng::from_seed([14; 32]);
    let mut all_peers = create_all_peers(None);

    let (a_bls, a_net, a_id) = committee_member(&mut rng);
    let (b_bls, b_net, b_id) = committee_member(&mut rng);
    let (c_bls, c_net, c_id) = committee_member(&mut rng);
    discover(&mut all_peers, a_bls, &a_net);
    discover(&mut all_peers, b_bls, &b_net);
    discover(&mut all_peers, c_bls, &c_net);

    // initialize with previous={A}, current={B}, next={C}
    all_peers.update_committees(
        HashSet::from([a_bls]),
        HashSet::from([b_bls]),
        HashSet::from([c_bls]),
    );

    // a fully disjoint set replaces every slot directly
    let (d_bls, d_net, _d_id) = committee_member(&mut rng);
    let (e_bls, e_net, _e_id) = committee_member(&mut rng);
    let (f_bls, f_net, _f_id) = committee_member(&mut rng);
    discover(&mut all_peers, d_bls, &d_net);
    discover(&mut all_peers, e_bls, &e_net);
    discover(&mut all_peers, f_bls, &f_net);
    all_peers.update_committees(
        HashSet::from([d_bls]),
        HashSet::from([e_bls]),
        HashSet::from([f_bls]),
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([d_bls]));
    assert_eq!(all_peers.current_committee, HashSet::from([e_bls]));
    assert_eq!(all_peers.next_committee, HashSet::from([f_bls]));

    // every member of the fully-replaced window is gone
    assert!(!all_peers.is_peer_validator(&a_id));
    assert!(!all_peers.is_peer_validator(&b_id));
    assert!(!all_peers.is_peer_validator(&c_id));
}

#[test]
fn test_is_peer_validator_returns_true_for_all_three_slots() {
    let mut rng = StdRng::from_seed([17; 32]);
    let mut all_peers = create_all_peers(None);

    let (prev_bls, prev_net, prev_id) = committee_member(&mut rng);
    let (curr_bls, curr_net, curr_id) = committee_member(&mut rng);
    let (next_bls, next_net, next_id) = committee_member(&mut rng);

    // discover each member so its peer id resolves to its bls
    discover(&mut all_peers, prev_bls, &prev_net);
    discover(&mut all_peers, curr_bls, &curr_net);
    discover(&mut all_peers, next_bls, &next_net);

    all_peers.previous_committee.insert(prev_bls);
    all_peers.current_committee.insert(curr_bls);
    all_peers.next_committee.insert(next_bls);

    // membership in ANY of the three slots makes a discovered peer a validator
    assert!(all_peers.is_peer_validator(&prev_id));
    assert!(all_peers.is_peer_validator(&curr_id));
    assert!(all_peers.is_peer_validator(&next_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_update_committees_tracks_undiscovered_member_then_trusts_on_discovery() {
    // GAP FIX: a committee member whose network identity is not yet known is still tracked in the
    // slot (membership is a bls-domain fact that is always known), but `is_peer_validator` by peer
    // id is false until the libp2p id is learned. The moment discovery confirms the network
    // identity, the lazy-trust hook (`apply_membership_if_committee`, driven by `add_known_peer`)
    // makes it a validator and trusts it -- without waiting for the next epoch's update.
    let mut rng = StdRng::from_seed([99; 32]);
    let mut all_peers = create_all_peers(None);

    let (bls, net, peer_id) = committee_member(&mut rng);

    // set committees with the member present by bls, but DO NOT discover its peer id yet
    all_peers.update_committees(HashSet::new(), HashSet::from([bls]), HashSet::new());

    // the slot retains the complete set (the bls is tracked)...
    assert!(all_peers.current_committee.contains(&bls));
    // ...but with no peer record the libp2p id does not yet resolve to a validator
    assert!(all_peers.get_peer(&peer_id).is_none());
    assert!(!all_peers.is_peer_validator(&peer_id));

    // discovery confirms the network identity (as `add_known_peer` would) ...
    all_peers.upsert_peer(bls, net, vec![create_multiaddr(None)]);
    // ... and the lazy-trust hook applies committee membership immediately
    let actions = all_peers.apply_membership_if_committee(bls);

    // now the member resolves: validator + trusted, the moment discovery completed
    assert!(all_peers.is_peer_validator(&peer_id));
    assert!(all_peers.get_peer(&peer_id).unwrap().is_trusted());
    // a freshly discovered, never-banned peer needs no unban action
    assert!(actions.is_empty());
}

#[test]
fn test_undiscovered_committee_member_banned_then_unbanned_on_discovery() {
    // Race-window scenario: a committee member is tracked by bls before its peer id is known, then
    // connects anonymously (`Unidentified`) and is banned during that window. When kad later
    // resolves its bls, `upsert_peer` re-keys the existing record onto its `Confirmed` identity
    // (retaining it) and the lazy-trust hook unbans + trusts it. No panic; the member is retained.
    let mut rng = StdRng::from_seed([55; 32]);
    let mut all_peers = create_all_peers(None);

    let (bls, net, peer_id) = committee_member(&mut rng);

    // committee set with the member present by bls, peer id still unknown
    all_peers.update_committees(HashSet::new(), HashSet::from([bls]), HashSet::new());
    assert!(!all_peers.is_peer_validator(&peer_id), "unidentified peer is not yet a validator");

    // the member connects anonymously and is banned during the window (Unidentified record)
    all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
    let ban = all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);
    assert!(matches!(ban, PeerAction::Ban(_)));
    assert!(all_peers.peer_banned(&peer_id), "member should be banned during the window");
    assert!(!all_peers.is_peer_validator(&peer_id), "still unidentified, still not a validator");

    // kad resolves the bls: re-key the Unidentified record onto Confirmed, then apply committee trust
    all_peers.upsert_peer(bls, net, vec![create_multiaddr(None)]);
    let actions = all_peers.apply_membership_if_committee(bls);

    // the member is retained, now resolves as a validator, is unbanned, and is trusted
    assert!(all_peers.get_peer(&peer_id).is_some(), "member record must be retained");
    assert!(all_peers.is_peer_validator(&peer_id), "discovered member is now a validator");
    assert!(!all_peers.peer_banned(&peer_id), "discovered committee member must be unbanned");
    assert!(
        all_peers.get_peer(&peer_id).unwrap().is_trusted(),
        "discovered member must be trusted"
    );
    assert!(
        actions.iter().any(|(id, action)| id == &peer_id && matches!(action, PeerAction::Unban(_))),
        "discovery must emit an unban action for the previously-banned member"
    );
}

#[test]
fn test_apply_membership_if_committee_is_noop_for_non_member() {
    // the lazy-trust hook must not trust an arbitrary discovered peer that is not in any committee.
    let mut rng = StdRng::from_seed([100; 32]);
    let mut all_peers = create_all_peers(None);

    let (bls, net, peer_id) = committee_member(&mut rng);
    all_peers.upsert_peer(bls, net, vec![create_multiaddr(None)]);

    let actions = all_peers.apply_membership_if_committee(bls);

    assert!(actions.is_empty());
    assert!(!all_peers.is_peer_validator(&peer_id));
    assert!(!all_peers.get_peer(&peer_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_in_window_peer_stays_trusted() {
    let mut rng = StdRng::from_seed([16; 32]);
    let mut all_peers = create_all_peers(None);

    // M starts in the current committee and is made trusted by update_committees
    let (m_bls, m_net, m_id) = committee_member(&mut rng);
    discover(&mut all_peers, m_bls, &m_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([m_bls]), HashSet::new());
    assert!(all_peers.get_peer(&m_id).unwrap().is_trusted());

    // next epoch: M moves to the previous slot (still in-window) alongside a disjoint current
    let (other_bls, other_net, _other_id) = committee_member(&mut rng);
    discover(&mut all_peers, other_bls, &other_net);
    all_peers.update_committees(HashSet::from([m_bls]), HashSet::from([other_bls]), HashSet::new());

    // M is still inside the three-slot window (now in previous), so it stays trusted -- not via a
    // one-way ratchet, but because it is still a tracked committee member.
    assert!(all_peers.previous_committee.contains(&m_bls));
    assert!(all_peers.is_peer_validator(&m_id));
    assert!(all_peers.get_peer(&m_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_demotes_peer_that_exits_window() {
    let mut rng = StdRng::from_seed([22; 32]);
    let mut all_peers = create_all_peers(None);

    // E starts trusted as a current-committee member
    let (e_bls, e_net, e_id) = committee_member(&mut rng);
    discover(&mut all_peers, e_bls, &e_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([e_bls]), HashSet::new());
    assert!(all_peers.get_peer(&e_id).unwrap().is_trusted());
    assert!(all_peers.is_peer_validator(&e_id));

    // next epoch's three slots do not include E at all
    let (f_bls, f_net, _f_id) = committee_member(&mut rng);
    discover(&mut all_peers, f_bls, &f_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([f_bls]), HashSet::new());

    // E fell out of the window: demoted to untrusted and no longer counts as a validator
    assert!(!all_peers.get_peer(&e_id).unwrap().is_trusted());
    assert!(!all_peers.is_peer_validator(&e_id));
}

#[test]
fn test_update_committees_does_not_demote_operator_trusted_peer_never_in_committee() {
    let mut rng = StdRng::from_seed([23; 32]);
    let mut all_peers = create_all_peers(None);

    // operator-allowlisted trusted peer that is never part of any committee
    let op_bls = *BlsKeypair::generate(&mut rng).public();
    let op_net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let op_id: PeerId = op_net.clone().into();
    all_peers.add_trusted_peer(op_bls, op_net);
    assert!(all_peers.get_peer(&op_id).unwrap().is_trusted());

    // a committee update that does not involve the operator peer
    let (c_bls, c_net, _c_id) = committee_member(&mut rng);
    discover(&mut all_peers, c_bls, &c_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([c_bls]), HashSet::new());

    // the operator peer was never tracked in a committee slot, so demotion does not touch it
    assert!(all_peers.get_peer(&op_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_current_is_authoritative() {
    // `current` is set directly from authoritative state, never carried over from the
    // previously-predicted `next`. When the new `current` differs from the prior `next`, the slot
    // must reflect the authoritative arg rather than the stale prediction.
    let mut rng = StdRng::from_seed([20; 32]);
    let mut all_peers = create_all_peers(None);

    let (predicted_bls, ..) = committee_member(&mut rng);
    let (actual_bls, ..) = committee_member(&mut rng);

    // epoch N predicts next = {predicted}
    all_peers.update_committees(HashSet::new(), HashSet::new(), HashSet::from([predicted_bls]));
    assert_eq!(all_peers.next_committee, HashSet::from([predicted_bls]));

    // epoch N+1's authoritative current is {actual}, which differs from the prediction
    all_peers.update_committees(HashSet::new(), HashSet::from([actual_bls]), HashSet::new());

    // current matches the authoritative arg, not the stale prediction
    assert_eq!(all_peers.current_committee, HashSet::from([actual_bls]));
    assert!(!all_peers.current_committee.contains(&predicted_bls));
}

#[test]
fn test_update_committees_populates_previous() {
    // a non-empty `previous` arg lands in the slot. After a restart the previous
    // committee is read from authoritative state rather than reset to empty, so just-rotated-out
    // peers keep validator protection for the duration of the window.
    let mut rng = StdRng::from_seed([21; 32]);
    let mut all_peers = create_all_peers(None);

    let (p_bls, p_net, p_id) = committee_member(&mut rng);
    let (c_bls, c_net, _c_id) = committee_member(&mut rng);
    discover(&mut all_peers, p_bls, &p_net);
    discover(&mut all_peers, c_bls, &c_net);

    all_peers.update_committees(HashSet::from([p_bls]), HashSet::from([c_bls]), HashSet::new());

    assert_eq!(all_peers.previous_committee, HashSet::from([p_bls]));
    assert!(all_peers.is_peer_validator(&p_id));
    assert!(all_peers.get_peer(&p_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_invariants_under_random_committees() {
    // Property-style coverage without a proptest dependency: across many epochs with random
    // committees in each slot, every slot must equal exactly the (bls) set it was given, every
    // discovered union member is trusted, and every peer tracked last round but absent this round
    // is demoted to untrusted.
    let mut rng = StdRng::from_seed([42; 32]);
    let mut all_peers = create_all_peers(None);

    let mut prev_union: HashSet<PeerId> = HashSet::new();

    for round in 0..32u64 {
        let prev_size = rng.random_range(0..=4usize);
        let (prev_bls, prev_ids) = random_committee(&mut all_peers, &mut rng, prev_size);
        let curr_size = rng.random_range(0..=4usize);
        let (curr_bls, curr_ids) = random_committee(&mut all_peers, &mut rng, curr_size);
        let next_size = rng.random_range(0..=4usize);
        let (next_bls, next_ids) = random_committee(&mut all_peers, &mut rng, next_size);

        all_peers.update_committees(prev_bls.clone(), curr_bls.clone(), next_bls.clone());

        // each slot equals exactly its input set (direct set, no positional shift)
        assert_eq!(all_peers.previous_committee, prev_bls, "previous slot after round {round}");
        assert_eq!(all_peers.current_committee, curr_bls, "current slot after round {round}");
        assert_eq!(all_peers.next_committee, next_bls, "next slot after round {round}");

        // every discovered member of the new window is trusted
        let union: HashSet<PeerId> =
            prev_ids.iter().chain(curr_ids.iter()).chain(next_ids.iter()).copied().collect();
        for id in &union {
            assert!(
                all_peers.get_peer(id).unwrap().is_trusted(),
                "union member {id} should be trusted after round {round}"
            );
        }

        // every peer tracked last round but absent this round is demoted (all ids are distinct
        // across rounds, so the entire prior union should now be untrusted)
        for id in prev_union.difference(&union) {
            assert!(
                !all_peers.get_peer(id).unwrap().is_trusted(),
                "exited peer {id} should be untrusted after round {round}"
            );
        }

        prev_union = union;
    }
}
