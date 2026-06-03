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

/// Build a distinct committee member: (bls key, network info, derived peer id).
fn committee_member(rng: &mut StdRng) -> (BlsPublicKey, NetworkInfo, PeerId) {
    let bls = *BlsKeypair::generate(rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    let info = NetworkInfo { pubkey: net, multiaddrs: vec![create_multiaddr(None)], timestamp: 0 };
    (bls, info, peer_id)
}

/// Build a committee of `size` distinct members plus the set of their derived peer ids.
fn random_committee(
    rng: &mut StdRng,
    size: usize,
) -> (Vec<(BlsPublicKey, NetworkInfo)>, HashSet<PeerId>) {
    let members: Vec<(BlsPublicKey, NetworkInfo, PeerId)> =
        (0..size).map(|_| committee_member(rng)).collect();
    let ids: HashSet<PeerId> = members.iter().map(|(_, _, id)| *id).collect();
    let committee = members.into_iter().map(|(b, i, _)| (b, i)).collect();
    (committee, ids)
}

#[test]
fn test_update_committees_sets_all_three_slots() {
    let mut rng = StdRng::from_seed([10; 32]);
    let mut all_peers = create_all_peers(None);

    let (p_bls, p_info, p_peer_id) = committee_member(&mut rng);
    let (c_bls, c_info, c_peer_id) = committee_member(&mut rng);
    let (n_bls, n_info, n_peer_id) = committee_member(&mut rng);

    all_peers.update_committees(
        vec![(p_bls, p_info)],
        vec![(c_bls, c_info)],
        vec![(n_bls, n_info)],
    );

    assert!(all_peers.previous_committee.contains(&p_peer_id));
    assert!(all_peers.current_committee.contains(&c_peer_id));
    assert!(all_peers.next_committee.contains(&n_peer_id));
    assert!(all_peers.is_peer_validator(&p_peer_id));
    assert!(all_peers.is_peer_validator(&c_peer_id));
    assert!(all_peers.is_peer_validator(&n_peer_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_update_committees_overwrites_slots_directly() {
    let mut rng = StdRng::from_seed([11; 32]);
    let mut all_peers = create_all_peers(None);

    let (a_bls, a_info, a_id) = committee_member(&mut rng);
    let (b_bls, b_info, b_id) = committee_member(&mut rng);
    let (c_bls, c_info, c_id) = committee_member(&mut rng);
    let (d_bls, d_info, d_id) = committee_member(&mut rng);
    let (e_bls, e_info, e_id) = committee_member(&mut rng);
    let (f_bls, f_info, f_id) = committee_member(&mut rng);

    // first update: previous={A}, current={B}, next={C}
    all_peers.update_committees(
        vec![(a_bls, a_info)],
        vec![(b_bls, b_info)],
        vec![(c_bls, c_info)],
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([a_id]));
    assert_eq!(all_peers.current_committee, HashSet::from([b_id]));
    assert_eq!(all_peers.next_committee, HashSet::from([c_id]));

    // second update: each slot is set directly from its arg, NOT positionally shifted from the
    // prior round (otherwise current would be the prior next, {C}).
    all_peers.update_committees(
        vec![(d_bls, d_info)],
        vec![(e_bls, e_info)],
        vec![(f_bls, f_info)],
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([d_id]));
    assert_eq!(all_peers.current_committee, HashSet::from([e_id]));
    assert_eq!(all_peers.next_committee, HashSet::from([f_id]));
}

#[test]
fn test_update_committees_no_change() {
    let mut rng = StdRng::from_seed([13; 32]);
    let mut all_peers = create_all_peers(None);

    let (x_bls, x_info, x_id) = committee_member(&mut rng);

    // X is in both current and next
    all_peers.update_committees(
        vec![],
        vec![(x_bls, x_info.clone())],
        vec![(x_bls, x_info.clone())],
    );

    // repeat the same membership next epoch
    all_peers.update_committees(
        vec![],
        vec![(x_bls, x_info.clone())],
        vec![(x_bls, x_info.clone())],
    );

    // full overlap keeps X a validator, present in both current and next, never demoted
    assert!(all_peers.is_peer_validator(&x_id));
    assert!(all_peers.current_committee.contains(&x_id));
    assert!(all_peers.next_committee.contains(&x_id));
    assert!(all_peers.get_peer(&x_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_full_turnover() {
    let mut rng = StdRng::from_seed([14; 32]);
    let mut all_peers = create_all_peers(None);

    let (a_bls, a_info, a_id) = committee_member(&mut rng);
    let (b_bls, b_info, b_id) = committee_member(&mut rng);
    let (c_bls, c_info, c_id) = committee_member(&mut rng);

    // initialize with previous={A}, current={B}, next={C}
    all_peers.update_committees(
        vec![(a_bls, a_info)],
        vec![(b_bls, b_info)],
        vec![(c_bls, c_info)],
    );

    // a fully disjoint set replaces every slot directly
    let (d_bls, d_info, d_id) = committee_member(&mut rng);
    let (e_bls, e_info, e_id) = committee_member(&mut rng);
    let (f_bls, f_info, f_id) = committee_member(&mut rng);
    all_peers.update_committees(
        vec![(d_bls, d_info)],
        vec![(e_bls, e_info)],
        vec![(f_bls, f_info)],
    );
    assert_eq!(all_peers.previous_committee, HashSet::from([d_id]));
    assert_eq!(all_peers.current_committee, HashSet::from([e_id]));
    assert_eq!(all_peers.next_committee, HashSet::from([f_id]));

    // every member of the fully-replaced window is gone
    assert!(!all_peers.is_peer_validator(&a_id));
    assert!(!all_peers.is_peer_validator(&b_id));
    assert!(!all_peers.is_peer_validator(&c_id));
}

#[test]
fn test_is_peer_validator_returns_true_for_all_three_slots() {
    let mut all_peers = create_all_peers(None);

    let prev_id = PeerId::random();
    let curr_id = PeerId::random();
    let next_id = PeerId::random();

    all_peers.previous_committee.insert(prev_id);
    all_peers.current_committee.insert(curr_id);
    all_peers.next_committee.insert(next_id);

    assert!(all_peers.is_peer_validator(&prev_id));
    assert!(all_peers.is_peer_validator(&curr_id));
    assert!(all_peers.is_peer_validator(&next_id));
    assert!(!all_peers.is_peer_validator(&PeerId::random()));
}

#[test]
fn test_update_committees_in_window_peer_stays_trusted() {
    let mut rng = StdRng::from_seed([16; 32]);
    let mut all_peers = create_all_peers(None);

    // M starts in the current committee and is made trusted by update_committees
    let (m_bls, m_info, m_id) = committee_member(&mut rng);
    all_peers.update_committees(vec![], vec![(m_bls, m_info.clone())], vec![]);
    assert!(all_peers.get_peer(&m_id).unwrap().is_trusted());

    // next epoch: M moves to the previous slot (still in-window) alongside a disjoint current
    let (other_bls, other_info, _other_id) = committee_member(&mut rng);
    all_peers.update_committees(vec![(m_bls, m_info)], vec![(other_bls, other_info)], vec![]);

    // M is still inside the three-slot window (now in previous), so it stays trusted -- not via a
    // one-way ratchet, but because it is still a tracked committee member.
    assert!(all_peers.previous_committee.contains(&m_id));
    assert!(all_peers.is_peer_validator(&m_id));
    assert!(all_peers.get_peer(&m_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_demotes_peer_that_exits_window() {
    let mut rng = StdRng::from_seed([22; 32]);
    let mut all_peers = create_all_peers(None);

    // E starts trusted as a current-committee member
    let (e_bls, e_info, e_id) = committee_member(&mut rng);
    all_peers.update_committees(vec![], vec![(e_bls, e_info)], vec![]);
    assert!(all_peers.get_peer(&e_id).unwrap().is_trusted());
    assert!(all_peers.is_peer_validator(&e_id));

    // next epoch's three slots do not include E at all
    let (f_bls, f_info, _f_id) = committee_member(&mut rng);
    all_peers.update_committees(vec![], vec![(f_bls, f_info)], vec![]);

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
    all_peers.add_trusted_peer(op_bls, op_net, vec![create_multiaddr(None)]);
    assert!(all_peers.get_peer(&op_id).unwrap().is_trusted());

    // a committee update that does not involve the operator peer
    let (c_bls, c_info, _c_id) = committee_member(&mut rng);
    all_peers.update_committees(vec![], vec![(c_bls, c_info)], vec![]);

    // the operator peer was never tracked in a committee slot, so demotion does not touch it
    assert!(all_peers.get_peer(&op_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_current_is_authoritative() {
    // Finding #2: `current` is set directly from authoritative state, never carried over from the
    // previously-predicted `next`. When the new `current` differs from the prior `next`, the slot
    // must reflect the authoritative arg rather than the stale prediction.
    let mut rng = StdRng::from_seed([20; 32]);
    let mut all_peers = create_all_peers(None);

    let (predicted_bls, predicted_info, predicted_id) = committee_member(&mut rng);
    let (actual_bls, actual_info, actual_id) = committee_member(&mut rng);

    // epoch N predicts next = {predicted}
    all_peers.update_committees(vec![], vec![], vec![(predicted_bls, predicted_info)]);
    assert_eq!(all_peers.next_committee, HashSet::from([predicted_id]));

    // epoch N+1's authoritative current is {actual}, which differs from the prediction
    all_peers.update_committees(vec![], vec![(actual_bls, actual_info)], vec![]);

    // current matches the authoritative arg, not the stale prediction
    assert_eq!(all_peers.current_committee, HashSet::from([actual_id]));
    assert!(!all_peers.current_committee.contains(&predicted_id));
}

#[test]
fn test_update_committees_populates_previous() {
    // Finding #4: a non-empty `previous` arg lands in the slot. After a restart the previous
    // committee is read from authoritative state rather than reset to empty, so just-rotated-out
    // peers keep validator protection for the duration of the window.
    let mut rng = StdRng::from_seed([21; 32]);
    let mut all_peers = create_all_peers(None);

    let (p_bls, p_info, p_id) = committee_member(&mut rng);
    let (c_bls, c_info, _c_id) = committee_member(&mut rng);

    all_peers.update_committees(vec![(p_bls, p_info)], vec![(c_bls, c_info)], vec![]);

    assert_eq!(all_peers.previous_committee, HashSet::from([p_id]));
    assert!(all_peers.is_peer_validator(&p_id));
    assert!(all_peers.get_peer(&p_id).unwrap().is_trusted());
}

#[test]
fn test_update_committees_invariants_under_random_committees() {
    // Property-style coverage without a proptest dependency: across many epochs with random
    // committees in each slot, every slot must equal exactly the set it was given, every union
    // member is trusted, and every peer tracked last round but absent this round is demoted to
    // untrusted.
    let mut rng = StdRng::from_seed([42; 32]);
    let mut all_peers = create_all_peers(None);

    let mut prev_union: HashSet<PeerId> = HashSet::new();

    for round in 0..32u64 {
        let prev_size = rng.random_range(0..=4usize);
        let (previous, prev_ids) = random_committee(&mut rng, prev_size);
        let curr_size = rng.random_range(0..=4usize);
        let (current, curr_ids) = random_committee(&mut rng, curr_size);
        let next_size = rng.random_range(0..=4usize);
        let (next, next_ids) = random_committee(&mut rng, next_size);

        all_peers.update_committees(previous, current, next);

        // each slot equals exactly its input set (direct set, no positional shift)
        assert_eq!(all_peers.previous_committee, prev_ids, "previous slot after round {round}");
        assert_eq!(all_peers.current_committee, curr_ids, "current slot after round {round}");
        assert_eq!(all_peers.next_committee, next_ids, "next slot after round {round}");

        // every member of the new window is trusted
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
