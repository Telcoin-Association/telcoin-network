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
fn test_upsert_peer_seeds_multiaddrs_for_new_peer() {
    let mut all_peers = create_all_peers(None);

    // upsert a previously-unknown peer with a known addr but no connection
    let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = network_key.clone().into();
    let addr = create_multiaddr(None);
    let mut rng = StdRng::from_seed([7; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    all_peers.upsert_peer(bls, network_key, vec![addr.clone()]);

    // the fresh record carries the addr through to peer exchange and dialing, even though the peer
    // has never connected
    let peer = all_peers.get_peer(&peer_id).expect("peer exists after upsert");
    assert!(peer.exchange_info().unwrap().1.iter().any(|a| a == &addr));

    // but a purely self-advertised address is never treated as an observed connection IP, so it
    // must not feed the per-IP ban counter: otherwise an attacker could advertise an honest peer's
    // IP and have it banned (GHSA-6qcj-p42p-779j)
    let advertised_ip = addr.iter().find_map(|p| match p {
        libp2p::multiaddr::Protocol::Ip4(ip) => Some(IpAddr::from(ip)),
        libp2p::multiaddr::Protocol::Ip6(ip) => Some(IpAddr::from(ip)),
        _ => None,
    });
    assert!(
        !peer.known_ip_addresses().any(|ip| Some(ip) == advertised_ip),
        "a self-advertised, never-observed IP must not enter the ban-counter source"
    );
}

/// Build a peer with a bls key and two network keys for rotation scenarios:
/// (bls, first network key, its peer id, rotated network key, its peer id).
fn rotation_keys(seed: u8) -> (BlsPublicKey, NetworkPublicKey, PeerId, NetworkPublicKey, PeerId) {
    let mut rng = StdRng::from_seed([seed; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net1: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id_1: PeerId = net1.clone().into();
    let net2: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id_2: PeerId = net2.clone().into();
    (bls, net1, peer_id_1, net2, peer_id_2)
}

#[test]
fn test_upsert_peer_network_key_rotation() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(21);
    let addr = create_multiaddr(None);

    // known peer via its first network key, driven to a non-default status so the counter
    // bookkeeping is observable
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.disconnected_peers, 1);

    // rotate the network key, same bls key
    all_peers.upsert_peer(bls, net2, vec![addr.clone()]);

    // iff-invariant: the old peer id no longer resolves, the new one does, and exactly one
    // confirmed record exists
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.bls_by_peer_id.len(), 1);
    assert_eq!(all_peers.peers.len(), 1);
    assert!(all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls)));
    assert!(all_peers.get_peer(&peer_id_1).is_none());

    // accumulated state survives the rotation: the record's recoverable peer id follows the
    // new key, its status is untouched, and the status counters are unchanged
    let peer = all_peers.get_peer(&peer_id_2).expect("rotated peer resolves");
    assert_eq!(peer.peer_id(), Some(peer_id_2));
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
    assert_eq!(all_peers.disconnected_peers, 1);
    assert_eq!(all_peers.banned_peers.total(), 0);

    // after the record is evicted (as pruning would), no orphaned index entry remains;
    // evict alone does not manage counters (the pruning paths decrement separately)
    all_peers.evict(&PeerIdentity::Confirmed(bls));
    assert!(all_peers.bls_by_peer_id.is_empty());
    assert_eq!(all_peers.disconnected_peers, 1);

    // a late event carrying the rotated-away peer id creates a normal unidentified record
    // instead of resurrecting a keyless record under the confirmed key
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Dialing);
    assert!(all_peers.peers.contains_key(&PeerIdentity::Unidentified(peer_id_1)));
    assert!(!all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls)));
    assert!(all_peers.bls_by_peer_id.is_empty());
}

#[test]
fn test_upsert_peer_network_key_rotation_banned_peer() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(22);
    let addr = create_multiaddr(None);

    // ban the peer under its first network key
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers
        .update_connection_status(&peer_id_1, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.banned_peers.total(), 1);

    // rotation does not reset the ban: the record carries forward under the new key with its
    // banned status and bookkeeping intact
    all_peers.upsert_peer(bls, net2, vec![addr]);
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.peers.len(), 1);
    let peer = all_peers.get_peer(&peer_id_2).expect("rotated peer resolves");
    assert!(matches!(peer.connection_status(), ConnectionStatus::Banned { .. }));
    assert_eq!(all_peers.banned_peers.total(), 1);
    assert_eq!(all_peers.disconnected_peers, 0);
}

#[test]
fn test_upsert_peer_rotation_releases_displaced_record_counters() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(23);
    let addr = create_multiaddr(None);

    // disconnected record under the confirmed identity from the first network key
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.disconnected_peers, 1);

    // the peer reconnects anonymously with its rotated key before publishing a kad record
    let addr2 = create_multiaddr(None);
    all_peers.update_connection_status(
        &peer_id_2,
        NewConnectionStatus::Connected {
            multiaddr: addr2.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    assert_eq!(all_peers.peers.len(), 2);

    // the kad record arrives: the anonymous record is promoted onto the confirmed identity and
    // the displaced (disconnected) record releases its counter as it is dropped
    all_peers.upsert_peer(bls, net2, vec![addr2]);
    assert_eq!(all_peers.peers.len(), 1);
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.bls_by_peer_id.len(), 1);
    let peer = all_peers.get_peer(&peer_id_2).expect("promoted peer resolves");
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { .. }));
    assert_eq!(all_peers.disconnected_peers, 0);
}

#[test]
fn test_add_trusted_peer_network_key_rotation() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(24);

    all_peers.add_trusted_peer(bls, net1);
    assert_eq!(all_peers.bls_for_peer(&peer_id_1), Some(bls));

    // rotate the network key, same bls key
    all_peers.add_trusted_peer(bls, net2);

    // iff-invariant: the old peer id no longer resolves, the new one does, and exactly one
    // confirmed record exists with a recoverable peer id for the new key
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.bls_by_peer_id.len(), 1);
    assert_eq!(all_peers.peers.len(), 1);
    let peer = all_peers.get_peer(&peer_id_2).expect("rotated trusted peer resolves");
    assert_eq!(peer.peer_id(), Some(peer_id_2));
    assert_eq!(peer.reputation(), Reputation::Trusted);
}

#[test]
fn test_add_trusted_peer_preserves_banned_total() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, _, _, _) = rotation_keys(25);

    // ban an unrelated peer
    let banned_peer = PeerId::random();
    all_peers.update_connection_status(
        &banned_peer,
        NewConnectionStatus::Disconnecting { banned: true },
    );
    all_peers.update_connection_status(&banned_peer, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.banned_peers.total(), 1);

    // adding a trusted peer displaces no record, so the banned total is untouched
    all_peers.add_trusted_peer(bls, net1);
    assert_eq!(all_peers.banned_peers.total(), 1);
}

#[test]
fn test_ensure_peer_exists_repairs_stale_index_entry() {
    let mut all_peers = create_all_peers(None);
    let (bls, _, peer_id_1, _, _) = rotation_keys(26);

    // manufacture an iff-invariant violation: an index entry with no confirmed record
    all_peers.bls_by_peer_id.insert(peer_id_1, bls);

    // an event for the stale peer id repairs the index instead of storing a keyless default
    // record under the confirmed key
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Dialing);
    assert!(all_peers.bls_by_peer_id.is_empty());
    assert!(all_peers.peers.contains_key(&PeerIdentity::Unidentified(peer_id_1)));
    assert!(!all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls)));
}

#[test]
fn test_upsert_peer_rotation_normalizes_carried_connected_status() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(27);
    let addr = create_multiaddr(None);

    // the peer is connected under its first network key when the rotated kad record arrives
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );

    all_peers.upsert_peer(bls, net2, vec![addr]);

    // the carried record's Connected status described the old key's transport; it is
    // normalized to Disconnected (with the counter incremented) so the new identity is
    // immediately dialable instead of wedged behind a phantom connection
    let peer = all_peers.get_peer(&peer_id_2).expect("rotated peer resolves");
    assert!(matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }));
    assert!(peer.can_dial());
    assert_eq!(all_peers.disconnected_peers, 1);
    assert_eq!(all_peers.banned_peers.total(), 0);
}

#[test]
fn test_upsert_peer_rotation_normalizes_carried_pending_ban_status() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(35);
    let addr = create_multiaddr(None);

    // the peer is mid-ban under its first network key (disconnecting with the ban pending, not
    // yet completed to Banned) when the rotated kad record arrives
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers
        .update_connection_status(&peer_id_1, NewConnectionStatus::Disconnecting { banned: true });
    assert_eq!(all_peers.banned_peers.total(), 0);

    all_peers.upsert_peer(bls, net2, vec![addr]);

    // the carried Disconnecting { banned: true } status normalizes onto the new identity as
    // Banned with the banned counter incremented, so the pending ban survives the rotation
    let peer = all_peers.get_peer(&peer_id_2).expect("rotated peer resolves");
    assert!(matches!(peer.connection_status(), ConnectionStatus::Banned { .. }));
    assert_eq!(all_peers.banned_peers.total(), 1);
    assert_eq!(all_peers.disconnected_peers, 0);

    // iff-invariant: the old peer id no longer resolves, the new one does, one confirmed record
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.peers.len(), 1);
}

#[test]
fn test_upsert_peer_rotation_pending_ban_records_observed_rotated_address() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, _peer_id_2) = rotation_keys(36);

    // a separate peer is already banned on the address the rotating peer will present, leaving
    // that ip one ban short of the per-ip block threshold
    let rotated_ip = IpAddr::V4("192.168.77.1".parse().unwrap());
    let rotated_addr = create_multiaddr(Some(rotated_ip));
    let other = PeerId::random();
    all_peers.update_connection_status(
        &other,
        NewConnectionStatus::Connected {
            multiaddr: rotated_addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&other, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&other, NewConnectionStatus::Disconnected);
    assert!(!all_peers.ip_banned(&rotated_ip), "a single ban is below the per-ip threshold");

    // the rotating peer is confirmed under its first network key and has actually connected from
    // the shared address (an observed connection IP recorded on the record), then goes mid-ban
    all_peers.upsert_peer(bls, net1, vec![]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: rotated_addr.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers
        .update_connection_status(&peer_id_1, NewConnectionStatus::Disconnecting { banned: true });

    // it rotates to a new network key: normalizing the carried pending ban records the *observed*
    // shared address carried on the record, pushing the shared ip over the threshold. an address
    // it had merely advertised (never connected from) would not count (GHSA-6qcj-p42p-779j).
    all_peers.upsert_peer(bls, net2, vec![create_multiaddr(None)]);
    assert!(
        all_peers.ip_banned(&rotated_ip),
        "the observed rotated-to address must be banned when the carried pending ban is normalized"
    );
}

#[test]
fn test_upsert_peer_rotation_pending_ban_ignores_advertised_rotated_address() {
    // Security regression (GHSA-6qcj-p42p-779j): the rotation-carry path must bank the address the
    // rotating peer was actually observed connecting from, but NOT an address it merely
    // self-advertised. The rotating peer connects from its own attacker IP and only advertises the
    // victim IP, so normalizing the carried pending ban must push the observed attacker IP over the
    // block threshold while leaving the advertised victim IP below it.
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, _peer_id_2) = rotation_keys(36);

    let victim_ip = IpAddr::V4("192.168.77.1".parse().unwrap());
    let victim_addr = create_multiaddr(Some(victim_ip));
    let attacker_ip = IpAddr::V4("10.0.0.1".parse().unwrap());
    let attacker_addr = create_multiaddr(Some(attacker_ip));

    // seed BOTH the victim IP and the attacker IP one ban short of the block threshold, each via a
    // separate peer that genuinely connected from it
    for (seed_peer, seed_addr) in
        [(PeerId::random(), &victim_addr), (PeerId::random(), &attacker_addr)]
    {
        all_peers.update_connection_status(
            &seed_peer,
            NewConnectionStatus::Connected {
                multiaddr: seed_addr.clone(),
                direction: ConnectionDirection::Incoming,
            },
        );
        all_peers.update_connection_status(
            &seed_peer,
            NewConnectionStatus::Disconnecting { banned: true },
        );
        all_peers.update_connection_status(&seed_peer, NewConnectionStatus::Disconnected);
    }
    assert!(!all_peers.ip_banned(&victim_ip), "a single ban is below the per-ip threshold");
    assert!(!all_peers.ip_banned(&attacker_ip), "a single ban is below the per-ip threshold");

    // the rotating peer connects from its OWN address (observed) and goes mid-ban, while merely
    // advertising the victim's address in its record
    all_peers.upsert_peer(bls, net1, vec![victim_addr.clone()]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: attacker_addr,
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers
        .update_connection_status(&peer_id_1, NewConnectionStatus::Disconnecting { banned: true });

    // rotate and keep advertising the victim address; normalizing the carried ban records only the
    // observed attacker IP
    all_peers.upsert_peer(bls, net2, vec![victim_addr]);

    // positive control: the carried ban really fired and counted the OBSERVED attacker IP, pushing
    // it over the threshold - so a green result cannot mean the ban path silently did nothing
    assert!(
        all_peers.ip_banned(&attacker_ip),
        "the observed attacker IP must be banned once the carried pending ban is normalized"
    );
    // the advertised-only victim IP stays one ban short of the threshold
    assert!(
        !all_peers.ip_banned(&victim_ip),
        "an address the rotating peer only advertised must not be banned"
    );
}

#[test]
fn test_upsert_peer_merge_releases_displaced_banned_record() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(28);
    let addr = create_multiaddr(None);

    // ban the peer under its first network key
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers
        .update_connection_status(&peer_id_1, NewConnectionStatus::Disconnecting { banned: true });
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.banned_peers.total(), 1);

    // the peer reconnects anonymously with its rotated key, then its kad record arrives;
    // the anonymous record wins the merge and the displaced banned record releases its
    // bookkeeping as it is dropped (the ban itself is shed: pre-existing merge semantics)
    let addr2 = create_multiaddr(None);
    all_peers.update_connection_status(
        &peer_id_2,
        NewConnectionStatus::Connected {
            multiaddr: addr2.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.upsert_peer(bls, net2, vec![addr2]);
    assert_eq!(all_peers.banned_peers.total(), 0);
    assert_eq!(all_peers.disconnected_peers, 0);
    assert_eq!(all_peers.peers.len(), 1);
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    let peer = all_peers.get_peer(&peer_id_2).expect("promoted peer resolves");
    assert!(matches!(peer.connection_status(), ConnectionStatus::Connected { .. }));
}

#[test]
fn test_add_trusted_peer_releases_displaced_anonymous_record_counters() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, _, _) = rotation_keys(29);
    let addr = create_multiaddr(None);

    // anonymous peer connects then disconnects under the peer id the operator later trusts
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.disconnected_peers, 1);

    // the displaced anonymous record releases its counter as it is overwritten
    all_peers.add_trusted_peer(bls, net1);
    assert_eq!(all_peers.disconnected_peers, 0);
    assert_eq!(all_peers.peers.len(), 1);
    assert_eq!(all_peers.bls_for_peer(&peer_id_1), Some(bls));
}

#[test]
fn test_add_trusted_peer_rotation_releases_displaced_record_counters() {
    let mut all_peers = create_all_peers(None);
    let (bls, net1, peer_id_1, net2, peer_id_2) = rotation_keys(30);
    let addr = create_multiaddr(None);

    // disconnected confirmed record under the first network key
    all_peers.upsert_peer(bls, net1, vec![addr.clone()]);
    all_peers.update_connection_status(
        &peer_id_1,
        NewConnectionStatus::Connected {
            multiaddr: addr,
            direction: ConnectionDirection::Incoming,
        },
    );
    all_peers.update_connection_status(&peer_id_1, NewConnectionStatus::Disconnected);
    assert_eq!(all_peers.disconnected_peers, 1);

    // trusting the peer under a rotated network key displaces the old record through the
    // proper path: counter released, index re-keyed
    all_peers.add_trusted_peer(bls, net2);
    assert_eq!(all_peers.disconnected_peers, 0);
    assert!(all_peers.bls_for_peer(&peer_id_1).is_none());
    assert_eq!(all_peers.bls_for_peer(&peer_id_2), Some(bls));
    assert_eq!(all_peers.peers.len(), 1);
}

#[test]
fn test_add_trusted_peer_rekeys_network_key_bound_to_other_bls() {
    let mut all_peers = create_all_peers(None);
    let (bls_x, net1, peer_id_1, _, _) = rotation_keys(31);
    let mut rng = StdRng::from_seed([32; 32]);
    let bls_y = *BlsKeypair::generate(&mut rng).public();
    let addr = create_multiaddr(None);

    // the network key is first bound to a different bls identity
    all_peers.upsert_peer(bls_x, net1.clone(), vec![addr]);
    assert_eq!(all_peers.bls_for_peer(&peer_id_1), Some(bls_x));

    // trusting bls_y with the same network key must displace the bls_x record, not orphan it
    // (an orphaned record's later eviction would delete the legitimate index entry)
    all_peers.add_trusted_peer(bls_y, net1);
    assert_eq!(all_peers.peers.len(), 1);
    assert!(!all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls_x)));
    assert!(all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls_y)));
    assert_eq!(all_peers.bls_by_peer_id.len(), 1);
    assert_eq!(all_peers.bls_for_peer(&peer_id_1), Some(bls_y));
}

#[test]
fn test_upsert_peer_rekeys_network_key_bound_to_other_bls() {
    let mut all_peers = create_all_peers(None);
    let (bls_x, net1, peer_id_1, _, _) = rotation_keys(33);
    let mut rng = StdRng::from_seed([34; 32]);
    let bls_y = *BlsKeypair::generate(&mut rng).public();
    let addr = create_multiaddr(None);

    // the network key is first bound to a different bls identity, then re-presented with a
    // new bls key; the record migrates onto the new confirmed identity and the index follows
    all_peers.upsert_peer(bls_x, net1.clone(), vec![addr.clone()]);
    all_peers.upsert_peer(bls_y, net1, vec![addr]);
    assert_eq!(all_peers.peers.len(), 1);
    assert!(!all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls_x)));
    assert!(all_peers.peers.contains_key(&PeerIdentity::Confirmed(bls_y)));
    assert_eq!(all_peers.bls_by_peer_id.len(), 1);
    assert_eq!(all_peers.bls_for_peer(&peer_id_1), Some(bls_y));
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

// Regression for #745: when a dial exceeds `dial_timeout` and is reaped by the heartbeat,
// the caller must still be told it was a timeout. This is the one path where "dial attempt
// timedout" is the genuine cause; the disconnect transition no longer hardcodes it, so the
// heartbeat is now responsible for notifying the dialer.
#[test]
fn test_heartbeat_dial_timeout_notifies_caller() {
    let mut all_peers = create_all_peers(None);
    let peer_id = PeerId::random();

    // register a dial with a live reply channel, then age it past the timeout
    let (sender, mut receiver) = oneshot::channel();
    all_peers.register_dial_attempt(peer_id, Some(sender));
    all_peers
        .get_peer_mut(&peer_id)
        .expect("peer must exist after register_dial_attempt")
        .set_connection_status(ConnectionStatus::Dialing {
            instant: Instant::now() - Duration::from_secs(10),
        }); // Older than the 5s timeout

    let _ = all_peers.heartbeat_maintenance();

    // the caller is notified of the genuine timeout
    let result = receiver.try_recv().expect("heartbeat must notify the dialer of the timeout");
    let err = result.expect_err("dial timeout must be reported as an error");
    assert_eq!(
        err.to_string(),
        NetworkError::Dial("dial attempt timedout".to_string()).to_string()
    );
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

/// Regression guard for issue #799: overflow pruning must evict the OLDEST bans and keep the
/// NEWEST. The heap previously stored `Reverse(instant)`, so it retained and then evicted the
/// freshest bans instead. That let a peer banned moments ago be dropped from the ban set and
/// reconnect (ban evasion). `test_pruning_logic` only asserts the pruned *count*, so it cannot
/// catch the inverted direction; this test pins down exactly *which* peers survive.
#[test]
fn test_prune_banned_evicts_oldest_not_newest() {
    let config = PeerConfig::default();
    let excess = 5;
    let peer_num = config.max_banned_peers + excess;
    let mut all_peers = create_all_peers(None);

    // rank 1..=peer_num: a larger rank is an OLDER ban (instant further in the past), so the
    // `excess` largest ranks are the oldest bans and must be the ones pruned.
    let mut banned = Vec::with_capacity(peer_num);
    for rank in 1..=peer_num {
        let peer_id = PeerId::random();
        all_peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { banned: true },
        );
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Banned);

        if let Some(peer) = all_peers.get_peer_mut(&peer_id) {
            if matches!(peer.connection_status(), ConnectionStatus::Banned { .. }) {
                peer.set_connection_status(ConnectionStatus::Banned {
                    instant: Instant::now() - Duration::from_secs(rank as u64),
                });
            }
        }
        banned.push((rank, peer_id));
    }
    assert_eq!(all_peers.banned_peers.total(), peer_num);

    // the triggering peer is disconnected (not banned), so it does not affect the banned count:
    // exactly `excess` bans are pruned, down to `max_banned_peers`.
    all_peers.register_disconnected(&PeerId::random());
    assert_eq!(all_peers.banned_peers.total(), config.max_banned_peers);

    // the `excess` OLDEST bans (rank > max_banned_peers) are gone; every newer ban survives.
    for (rank, peer_id) in &banned {
        let still_banned = all_peers.peer_banned(peer_id);
        if *rank > config.max_banned_peers {
            assert!(!still_banned, "oldest ban (rank {rank}) should have been pruned");
        } else {
            assert!(still_banned, "recent ban (rank {rank}) must survive pruning");
        }
    }
}

/// Companion to `test_prune_banned_evicts_oldest_not_newest`: the disconnected caller shares the
/// same `collect_excess_peers` helper, so it must likewise keep the newest disconnected peers and
/// evict the oldest. Guards against the two prune callers diverging in future refactors.
#[test]
fn test_prune_disconnected_evicts_oldest_not_newest() {
    let config = PeerConfig::default();
    let excess = 5;
    let peer_num = config.max_disconnected_peers + excess;
    let mut all_peers = create_all_peers(None);

    // rank 1..=peer_num: a larger rank is an OLDER disconnect.
    let mut disconnected = Vec::with_capacity(peer_num);
    for rank in 1..=peer_num {
        let peer_id = PeerId::random();
        all_peers.update_connection_status(&peer_id, NewConnectionStatus::Disconnected);

        if let Some(peer) = all_peers.get_peer_mut(&peer_id) {
            if matches!(peer.connection_status(), ConnectionStatus::Disconnected { .. }) {
                peer.set_connection_status(ConnectionStatus::Disconnected {
                    instant: Instant::now() - Duration::from_secs(rank as u64),
                });
            }
        }
        disconnected.push((rank, peer_id));
    }
    assert_eq!(all_peers.disconnected_peers, peer_num);

    // the triggering peer is itself disconnected (instant ~now, the newest), so it joins the set
    // and `excess + 1` of the oldest are pruned: ranks >= max_disconnected_peers.
    all_peers.register_disconnected(&PeerId::random());
    assert_eq!(all_peers.disconnected_peers, config.max_disconnected_peers);

    for (rank, peer_id) in &disconnected {
        let retained = all_peers.get_peer(peer_id).is_some();
        if *rank >= config.max_disconnected_peers {
            assert!(!retained, "oldest disconnect (rank {rank}) should have been pruned");
        } else {
            assert!(retained, "recent disconnect (rank {rank}) must survive pruning");
        }
    }
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

/// Regression guard for issue #715: a validator's exemption is derived from committee
/// membership, not a stored flag, so rotating out of the committee revokes the exemption
/// (and, because operator trust is separate, never touches an operator allowlist).
#[test]
fn test_committee_rotation_revokes_validator_exemption() {
    ensure_score_config(None);
    let mut all_peers = AllPeers::new(Duration::from_secs(5), 10, 10);

    // a committee validator that the operator did NOT allowlist
    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = net.clone().into();
    all_peers.upsert_peer(bls, net, vec![]);
    all_peers.current_committee.insert(bls);

    // while in the committee the peer is exempt: a fatal penalty is suppressed and its
    // reputation is unaffected
    let action = all_peers.process_penalty(&peer_id, Penalty::Fatal);
    assert!(matches!(action, PeerAction::NoAction));
    assert_eq!(all_peers.get_peer(&peer_id).unwrap().reputation(), Reputation::Trusted);

    // rotate the validator out of the committee
    all_peers.current_committee.clear();

    // the exemption is gone: the same fatal penalty now lands and the peer is banned
    let _ = all_peers.process_penalty(&peer_id, Penalty::Fatal);
    assert_eq!(all_peers.get_peer(&peer_id).unwrap().reputation(), Reputation::Banned);
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
    assert_eq!(all_peers.trust_basis(&x_id), Some(TrustBasis::Validator));
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

    // now the member resolves: validator + exempt, the moment discovery completed
    assert!(all_peers.is_peer_validator(&peer_id));
    assert_eq!(all_peers.trust_basis(&peer_id), Some(TrustBasis::Validator));
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

    // kad resolves the bls: re-key the Unidentified record onto Confirmed, then apply committee
    // trust
    all_peers.upsert_peer(bls, net, vec![create_multiaddr(None)]);
    let actions = all_peers.apply_membership_if_committee(bls);

    // the member is retained, now resolves as a validator, is unbanned, and is trusted
    assert!(all_peers.get_peer(&peer_id).is_some(), "member record must be retained");
    assert!(all_peers.is_peer_validator(&peer_id), "discovered member is now a validator");
    assert!(!all_peers.peer_banned(&peer_id), "discovered committee member must be unbanned");
    assert_eq!(
        all_peers.trust_basis(&peer_id),
        Some(TrustBasis::Validator),
        "discovered member must be exempt as a validator"
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
    assert_eq!(all_peers.trust_basis(&peer_id), None);
}

#[test]
fn test_update_committees_in_window_peer_stays_trusted() {
    let mut rng = StdRng::from_seed([16; 32]);
    let mut all_peers = create_all_peers(None);

    // M starts in the current committee and is exempt as a validator
    let (m_bls, m_net, m_id) = committee_member(&mut rng);
    discover(&mut all_peers, m_bls, &m_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([m_bls]), HashSet::new());
    assert_eq!(all_peers.trust_basis(&m_id), Some(TrustBasis::Validator));

    // next epoch: M moves to the previous slot (still in-window) alongside a disjoint current
    let (other_bls, other_net, _other_id) = committee_member(&mut rng);
    discover(&mut all_peers, other_bls, &other_net);
    all_peers.update_committees(HashSet::from([m_bls]), HashSet::from([other_bls]), HashSet::new());

    // M is still inside the three-slot window (now in previous), so it stays exempt -- not via a
    // one-way ratchet, but because the exemption derives from its tracked membership.
    assert!(all_peers.previous_committee.contains(&m_bls));
    assert!(all_peers.is_peer_validator(&m_id));
    assert_eq!(all_peers.trust_basis(&m_id), Some(TrustBasis::Validator));
}

#[test]
fn test_update_committees_demotes_peer_that_exits_window() {
    let mut rng = StdRng::from_seed([22; 32]);
    let mut all_peers = create_all_peers(None);

    // E starts exempt as a current-committee member
    let (e_bls, e_net, e_id) = committee_member(&mut rng);
    discover(&mut all_peers, e_bls, &e_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([e_bls]), HashSet::new());
    assert_eq!(all_peers.trust_basis(&e_id), Some(TrustBasis::Validator));
    assert!(all_peers.is_peer_validator(&e_id));

    // next epoch's three slots do not include E at all
    let (f_bls, f_net, _f_id) = committee_member(&mut rng);
    discover(&mut all_peers, f_bls, &f_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([f_bls]), HashSet::new());

    // E fell out of the window: the validator exemption is gone (derived from the slots, so no
    // demotion pass is needed) and E no longer counts as a validator
    assert_eq!(all_peers.trust_basis(&e_id), None);
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
    assert_eq!(all_peers.trust_basis(&op_id), Some(TrustBasis::Operator));

    // a committee update that does not involve the operator peer
    let (c_bls, c_net, _c_id) = committee_member(&mut rng);
    discover(&mut all_peers, c_bls, &c_net);
    all_peers.update_committees(HashSet::new(), HashSet::from([c_bls]), HashSet::new());

    // operator trust is stored at construction and never altered by epoch rotation, so the
    // committee update does not touch the operator peer's exemption
    assert_eq!(all_peers.trust_basis(&op_id), Some(TrustBasis::Operator));
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
    assert_eq!(all_peers.trust_basis(&p_id), Some(TrustBasis::Validator));
}

#[test]
fn test_update_committees_invariants_under_random_committees() {
    // Property-style coverage without a proptest dependency: across many epochs with random
    // committees in each slot, every slot must equal exactly the (bls) set it was given, every
    // discovered union member is exempt as a validator, and every peer tracked last round but
    // absent this round re-enters the normal score model.
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

        // every discovered member of the new window is exempt as a validator
        let union: HashSet<PeerId> =
            prev_ids.iter().chain(curr_ids.iter()).chain(next_ids.iter()).copied().collect();
        for id in &union {
            assert_eq!(
                all_peers.trust_basis(id),
                Some(TrustBasis::Validator),
                "union member {id} should be exempt after round {round}"
            );
        }

        // every peer tracked last round but absent this round loses its exemption (all ids are
        // distinct across rounds, so the entire prior union re-enters the score model)
        for id in prev_union.difference(&union) {
            assert_eq!(
                all_peers.trust_basis(id),
                None,
                "exited peer {id} should re-enter the score model after round {round}"
            );
        }

        prev_union = union;
    }
}
