//! Unit tests for peer manager

use super::*;
use crate::{
    common::{create_multiaddr, random_ip_addr},
    consensus::partial_peers_from_get_closest_timeout,
};
use assert_matches::assert_matches;
use libp2p::{
    core::Endpoint,
    kad::GetClosestPeersError,
    swarm::{ConnectionId, DialError, NetworkBehaviour as _},
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, Ipv6Addr},
    time::Duration,
};
use tn_config::{KeyConfig, NetworkConfig, ScoreConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{now, BlsKeypair, NetworkKeypair, NetworkPublicKey};
use tokio::time::{sleep, timeout};

fn create_test_peer_manager(network_config: Option<NetworkConfig>) -> PeerManager {
    let network_config = network_config.unwrap_or_default();
    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    PeerManager::new(
        PeerId::random(),
        config.network_config().peer_config(),
        crate::metrics::PeerManagerMetrics::new_for(&crate::types::NetworkType::Primary),
    )
}

/// Helper function to extract events of a certain type
fn extract_events<'a>(
    events: &'a [PeerEvent],
    event_type: fn(&'a PeerEvent) -> bool,
) -> Vec<&'a PeerEvent> {
    events.iter().filter(|e| event_type(e)).collect()
}

/// Helper to get all events from the peer manager
fn collect_all_events(peer_manager: &mut PeerManager) -> Vec<PeerEvent> {
    let mut events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        events.push(event);
    }
    events
}

/// Register a peer connection and return its PeerId
fn register_peer(peer_manager: &mut PeerManager, multiaddr: Option<Multiaddr>) -> PeerId {
    let peer_id = PeerId::random();
    let multiaddr = multiaddr.unwrap_or_else(|| create_multiaddr(None));
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));
    peer_id
}

#[tokio::test]
async fn test_register_disconnected_basic() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // register connection
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // assert peer is no longer connected
    assert!(!peer_manager.is_connected(&peer_id));

    // assert no events from disconnect without ban
    assert!(peer_manager.poll_events().is_none());
}

#[tokio::test]
async fn test_register_disconnected_with_banned_peer() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // register connection
    let connection = ConnectionType::IncomingConnection { multiaddr };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // Apply a severe penalty to trigger ban
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // clear events from reported penalty
    let mut disconnect_events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        disconnect_events.push(event);
    }

    // assert peer is set for disconnect
    let disconnect_event =
        extract_events(&disconnect_events, |e| matches!(e, PeerEvent::DisconnectPeer(_))).len();
    assert!(disconnect_event == 1, "Expect one disconnect event");
    assert_matches!(
        disconnect_events.first().unwrap(),
        PeerEvent::DisconnectPeer(id) if *id == peer_id
    );

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // There should be no additional ban events since the peer is already banned
    let mut banned_events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        banned_events.push(event);
    }

    let banned_event = extract_events(&banned_events, |e| matches!(e, PeerEvent::Banned(_))).len();
    assert!(banned_event == 1, "Expect one banned event");
    assert_matches!(
        banned_events.first().unwrap(),
        PeerEvent::Banned(id) if *id == peer_id
    );

    // assert peer is still banned after disconnection
    assert!(peer_manager.peer_banned(&peer_id), "Peer should remain banned after disconnection");
}

#[tokio::test]
async fn test_add_trusted_peer() {
    let config = ScoreConfig::default();
    let mut peer_manager = create_test_peer_manager(None);
    let keys = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let peer_bls = keys.primary_public_key();
    let peer_netkey = keys.primary_network_public_key();
    let peer_id: PeerId = peer_netkey.clone().into();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, _receiver) = oneshot::channel();

    // Add trusted peer
    peer_manager.add_trusted_peer_and_dial(
        peer_bls,
        NetworkInfo {
            pubkey: peer_netkey,
            multiaddrs: vec![multiaddr.clone()],
            timestamp: now(),
            rpc: None,
        },
        sender,
    );

    let score = peer_manager.peer_score(&peer_id).unwrap();
    assert_eq!(score, config.max_score);

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request().unwrap();
    assert_eq!(dial_request.peer_id, peer_id);
    assert_eq!(dial_request.multiaddrs, vec![multiaddr]);

    // assert penalty doesn't affect trusted peer
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    assert!(!peer_manager.peer_banned(&peer_id));
    let score = peer_manager.peer_score(&peer_id).unwrap();
    assert_eq!(score, config.max_score);
}

#[tokio::test]
async fn test_dial_peer_success() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, receiver) = oneshot::channel();

    // Dial peer
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request();
    assert!(dial_request.is_some());
    let request = dial_request.unwrap();
    assert_eq!(request.peer_id, peer_id);
    assert_eq!(request.multiaddrs, vec![multiaddr.clone()]);

    // Register the dial attempt
    peer_manager.register_dial_attempt(peer_id, request.reply);

    // update connection status to trigger dial result
    assert!(peer_manager
        .register_peer_connection(&peer_id, ConnectionType::IncomingConnection { multiaddr }));

    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dial_peer_already_dialing_error() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, _receiver) = oneshot::channel();

    // Dial peer for the first time
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request().unwrap();

    // Register the dial attempt
    peer_manager.register_dial_attempt(peer_id, dial_request.reply);

    // Create another oneshot channel
    let (sender2, receiver2) = oneshot::channel();

    // Try to dial the same peer again
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender2));

    // Verify no new dial request was created (since we already dialing)
    assert!(peer_manager.next_dial_request().is_none());

    // Verify the error from the oneshot channel
    let result = timeout(Duration::from_millis(500), receiver2).await.unwrap().unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn test_dial_peer_already_connected() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Register a connected peer
    let connection = ConnectionType::IncomingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));
    assert!(peer_manager.is_connected(&peer_id));

    // Create a oneshot channel
    let (sender, receiver) = oneshot::channel();

    // Try to dial the already connected peer
    peer_manager.dial_peer(peer_id, vec![multiaddr.clone()], Some(sender));

    // Verify no dial request was created
    assert!(peer_manager.next_dial_request().is_none());

    // Verify the error from the oneshot channel
    let result = timeout(Duration::from_millis(500), receiver).await;
    let channel_result = result.unwrap().unwrap();
    assert!(channel_result.is_err()); // Dial should have failed with an error
}

// Regression for #745: a dial failure must surface the *real* `DialError` to the
// caller, not the hardcoded "dial attempt timedout" string. Pre-fix, `on_dial_failure`
// -> `register_disconnected` consumed the reply channel with the timeout literal before
// the genuine cause could be delivered, so every distinct failure (wrong key, refused,
// firewall, timeout) looked identical to an operator onboarding a validator.
#[tokio::test]
async fn test_dial_failure_surfaces_real_error() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();

    // register an in-flight dial so a reply channel is pending
    let (sender, receiver) = oneshot::channel();
    peer_manager.register_dial_attempt(peer_id, Some(sender));

    // a concrete, non-timeout failure cause
    let error = DialError::Aborted;
    let expected = NetworkError::from(&error).to_string();
    peer_manager.on_dial_failure(Some(peer_id), &error);

    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    let err = result.expect_err("dial failure must report an error");
    assert_eq!(
        err.to_string(),
        expected,
        "the real DialError must reach the caller, not a hardcoded cause"
    );

    // and specifically not the old hardcoded timeout literal
    let old_timeout = NetworkError::Dial("dial attempt timedout".to_string()).to_string();
    assert_ne!(
        err.to_string(),
        old_timeout,
        "regression: the real dial error was erased by the hardcoded timeout string"
    );
}

#[tokio::test]
async fn test_process_penalty_mild() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply multiple mild penalties
    for _ in 0..5 {
        peer_manager.process_penalty(peer_id, Penalty::Mild);
    }

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty < config.default_score);

    let events = collect_all_events(&mut peer_manager);
    assert!(events.is_empty());

    // Verify peer is not banned after mild penalties
    assert!(!peer_manager.peer_banned(&peer_id));
}

#[tokio::test]
async fn test_process_penalty_medium() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply medium penalties
    for _ in 0..3 {
        peer_manager.process_penalty(peer_id, Penalty::Medium);
    }

    // Apply one more medium penalty which should trigger disconnection
    peer_manager.process_penalty(peer_id, Penalty::Medium);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after medium penalties"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);
}

#[tokio::test]
async fn test_process_penalty_severe() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply severe penalties
    peer_manager.process_penalty(peer_id, Penalty::Severe);

    // Clear events
    collect_all_events(&mut peer_manager);

    // Apply one more severe penalty which should trigger disconnection
    peer_manager.process_penalty(peer_id, Penalty::Severe);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after severe penalties"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);
}

#[tokio::test]
async fn test_process_penalty_fatal() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply a fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a disconnect event
    let disconnect_events = extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeer(_)));
    assert!(
        matches!(
            disconnect_events.first().unwrap(), PeerEvent::DisconnectPeer(id) if *id == peer_id
        ),
        "Expected disconnect event after fatal penalty"
    );

    let config = ScoreConfig::default();
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty <= config.min_score_before_disconnect);

    // Register disconnection
    peer_manager.register_disconnected(&peer_id);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // There should be a banned event
    let banned_events = extract_events(&events, |e| matches!(e, PeerEvent::Banned(_)));
    assert!(
        matches!(
            banned_events.first().unwrap(), PeerEvent::Banned(id) if *id == peer_id
        ),
        "Expected banned event after fatal penalty"
    );

    // Verify peer is banned
    assert!(peer_manager.peer_banned(&peer_id));
    let banned_score = peer_manager.peer_score(&peer_id).unwrap();
    assert!(banned_score <= config.min_score_before_ban);
}

#[tokio::test]
async fn test_heartbeat_maintenance() {
    // custom network config with short heartbeat interval for peer manager
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().score_config.score_halflife = 0.5;
    let default_score = network_config.peer_config().score_config.default_score;

    let mut peer_manager = create_test_peer_manager(Some(network_config));
    let peer_id = register_peer(&mut peer_manager, None);

    // Apply a mild penalty
    peer_manager.process_penalty(peer_id, Penalty::Mild);
    let score_after_penalty = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_penalty < default_score);

    // Clear events
    collect_all_events(&mut peer_manager);

    // halflife set to 0.5
    sleep(Duration::from_secs(1)).await;

    // trigger heartbeat for update
    peer_manager.heartbeat();

    // Verify the peer score increases after heartbeats (penalties decay)
    let score_after_heartbeat = peer_manager.peer_score(&peer_id).unwrap();
    assert!(score_after_heartbeat > score_after_penalty, "Score should increase after heartbeats");
}

#[tokio::test]
async fn test_temporarily_banned_peer() {
    let mut network_config = NetworkConfig::default();
    // make the temp ban very short
    let temp_ban_duration = Duration::from_millis(10);
    network_config.peer_config_mut().excess_peers_reconnection_timeout = temp_ban_duration;

    let mut peer_manager = create_test_peer_manager(Some(network_config));
    let peer_id = register_peer(&mut peer_manager, None);

    // Disconnect the peer with PX (this should temp ban the peer)
    peer_manager.disconnect_peer(peer_id, true);

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there's a disconnect with PX event
    let disconnect_px_events =
        extract_events(&events, |e| matches!(e, PeerEvent::DisconnectPeerX(_, _)));
    assert!(
        matches!(
            disconnect_px_events.first().unwrap(), PeerEvent::DisconnectPeerX(id, _) if *id == peer_id
        ),
        "Expected disconnect event with peer exchange"
    );

    // Verify peer is temporarily banned
    assert!(peer_manager.peer_banned(&peer_id), "Peer should be temporarily banned");

    // sleep for temp ban duration
    let _ = sleep(temp_ban_duration * 2).await;

    // Run heartbeat to clear temporary bans
    peer_manager.heartbeat();

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there's an unbanned event
    let unbanned_events = extract_events(&events, |e| matches!(e, PeerEvent::Unbanned(_)));
    assert!(
        matches!(
            unbanned_events.first().unwrap(), PeerEvent::Unbanned(id) if *id == peer_id
        ),
        "Expected peer is unbanned"
    );

    // Verify peer is no longer banned
    assert!(!peer_manager.peer_banned(&peer_id), "Peer should not be banned after heartbeat");
}

#[tokio::test]
async fn test_process_peer_exchange() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create peer exchange data
    let multiaddr1 = create_multiaddr(None);
    let multiaddr2 = create_multiaddr(None);

    let mut exchange_map = HashMap::new();
    let multiaddrs1 = HashSet::from([multiaddr1]);
    let multiaddrs2 = HashSet::from([multiaddr2]);
    let mut rng = StdRng::from_seed([0; 32]);
    let bls1 = *BlsKeypair::generate(&mut rng).public();
    let bls2 = *BlsKeypair::generate(&mut rng).public();
    let net1: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().clone().into();
    let net2: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().clone().into();
    let peer_id1 = net1.clone().into();
    let peer_id2 = net2.clone().into();
    exchange_map.insert(bls1, (net1, multiaddrs1));
    exchange_map.insert(bls2, (net2, multiaddrs2));

    let exchange = PeerExchangeMap::from(exchange_map);

    // Process the peer exchange
    peer_manager.process_peer_exchange(exchange);

    // verify peers were added to discovery
    assert!(peer_manager.discovery_peers.contains_key(&peer_id1));
    assert!(peer_manager.discovery_peers.contains_key(&peer_id2));

    // discovery heartbeat to initiate dial requests
    peer_manager.discovery_heartbeat();

    // verify dial requests were created for both peers
    let _ = peer_manager.next_dial_request().expect("peer 1 exchange");
    let _ = peer_manager.next_dial_request().expect("peer 2 exchange");

    // verify no more dial requests
    assert!(peer_manager.next_dial_request().is_none());
}

#[tokio::test]
async fn test_prune_connected_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // Register many peers
    let max_peers = peer_manager.config.max_peers();
    let mut peer_ids = Vec::new();
    for _ in 0..(max_peers * 2) {
        let peer_id = register_peer(&mut peer_manager, None);
        peer_ids.push(peer_id);
    }

    // Force pruning via heartbeat
    peer_manager.heartbeat();

    // Get events
    let events = collect_all_events(&mut peer_manager);

    // Verify there are disconnect events (from pruning)
    let disconnect_events = extract_events(&events, |e| {
        matches!(e, PeerEvent::DisconnectPeerX(_, _)) || matches!(e, PeerEvent::DisconnectPeer(_))
    });

    assert!(!disconnect_events.is_empty(), "Expected disconnect events from pruning");
}

#[tokio::test]
async fn test_is_peer_connected_or_disconnecting() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // Verify peer is considered connected
    assert!(peer_manager.is_peer_connected_or_disconnecting(&peer_id));

    // Disconnect the peer (but don't register disconnection yet)
    peer_manager.disconnect_peer(peer_id, false);

    // Peer should still be considered as connected or disconnecting
    assert!(peer_manager.is_peer_connected_or_disconnecting(&peer_id));

    // Register disconnection
    peer_manager.register_disconnected(&peer_id);

    // Peer should no longer be considered connected or disconnecting
    assert!(!peer_manager.is_peer_connected_or_disconnecting(&peer_id));
}

#[tokio::test]
async fn test_is_validator() {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    let mut peer_manager = PeerManager::new(
        PeerId::random(),
        config.network_config().peer_config(),
        crate::metrics::PeerManagerMetrics::new_for(&crate::types::NetworkType::Primary),
    );
    let validator = *authority_1.authority().protocol_key();
    let validator_peer_id: PeerId = config.key_config().primary_network_public_key().into();
    let random_peer_id = PeerId::random();

    let info = NetworkInfo {
        pubkey: config.key_config().primary_network_public_key(),
        multiaddrs: vec![config.primary_address()],
        timestamp: now(),
        rpc: None,
    };
    peer_manager.add_known_peer(validator, info);

    // set the current committee (no previous/next committee for this test)
    let committee = config.committee_pub_keys();
    peer_manager.update_committees(HashSet::new(), committee, HashSet::new());

    // Verify the registered committee member is a validator
    assert!(peer_manager.is_peer_validator(&validator_peer_id));

    // Verify random peer is not a validator
    assert!(!peer_manager.is_peer_validator(&random_peer_id));
}

#[tokio::test]
async fn test_update_committees_triggers_missing_authorities_for_unknown_next_keys() {
    let mut peer_manager = create_test_peer_manager(None);

    // generate a bls key that was never registered via add_known_peer
    let unknown_bls = *BlsKeypair::generate(&mut StdRng::from_seed([9; 32])).public();

    // update with a next committee that contains the unknown key
    peer_manager.update_committees(HashSet::new(), HashSet::new(), HashSet::from([unknown_bls]));

    // exactly one MissingAuthorities event referencing the unknown key should be emitted
    let events = collect_all_events(&mut peer_manager);
    let missing_events = extract_events(&events, |e| matches!(e, PeerEvent::MissingAuthorities(_)));
    assert!(missing_events.len() == 1, "Expect one missing authorities event");
    assert_matches!(
        missing_events.first().unwrap(),
        PeerEvent::MissingAuthorities(missing) if missing.contains(&unknown_bls)
    );
}

#[tokio::test]
async fn test_update_committees_does_not_trigger_for_unknown_previous() {
    let mut peer_manager = create_test_peer_manager(None);

    // generate a bls key that was never registered via add_known_peer
    let unknown_bls = *BlsKeypair::generate(&mut StdRng::from_seed([9; 32])).public();

    // the unknown key appears ONLY in the previous committee (peers rotating out are not chased)
    peer_manager.update_committees(HashSet::from([unknown_bls]), HashSet::new(), HashSet::new());

    // no MissingAuthorities event should be emitted for a previous-only unknown key
    let events = collect_all_events(&mut peer_manager);
    let missing_events = extract_events(&events, |e| matches!(e, PeerEvent::MissingAuthorities(_)));
    assert!(
        missing_events.is_empty(),
        "previous-committee-only unknown keys must not trigger MissingAuthorities"
    );
}

#[tokio::test]
async fn test_prepare_committee_dial_unbans_without_touching_slots() {
    let mut peer_manager = create_test_peer_manager(None);

    // build a known committee member
    let mut rng = StdRng::from_seed([3; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = netkey.clone().into();
    let info = NetworkInfo {
        pubkey: netkey,
        multiaddrs: vec![create_multiaddr(None)],
        timestamp: now(),
        rpc: None,
    };
    peer_manager.add_known_peer(bls, info);

    // manually temp-ban the peer
    peer_manager.temporarily_banned.insert(peer_id);
    assert!(peer_manager.peer_banned(&peer_id));

    // prepare the committee for dialing
    peer_manager.prepare_committee_dial(HashSet::from([bls]));

    // the peer is unbanned but the committee slots remain untouched
    assert!(!peer_manager.peer_banned(&peer_id), "prepare_committee_dial should unban the peer");
    assert!(
        !peer_manager.is_peer_validator(&peer_id),
        "prepare_committee_dial must not populate committee slots"
    );
}

#[tokio::test]
async fn test_add_known_peer_closes_validator_gap_on_discovery() {
    // GAP FIX (full): a committee member set by bls before its network identity is known is tracked
    // in the slot but does not yet resolve to a validator by its libp2p id. The moment kad
    // discovery confirms the identity via `add_known_peer`, the lazy-trust hook makes it a
    // validator and marks it important -- without waiting for the next epoch's
    // `update_committees`.
    let mut peer_manager = create_test_peer_manager(None);

    // a committee member whose network identity is not yet known to this node
    let mut rng = StdRng::from_seed([7; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let peer_id: PeerId = netkey.clone().into();
    let info = NetworkInfo {
        pubkey: netkey,
        multiaddrs: vec![create_multiaddr(None)],
        timestamp: now(),
        rpc: None,
    };

    // set the current committee with the member present by bls, BEFORE discovering its peer id
    peer_manager.update_committees(HashSet::new(), HashSet::from([bls]), HashSet::new());

    // GAP: the member is tracked by bls, but its libp2p id does not resolve to a validator yet
    assert!(!peer_manager.is_peer_validator(&peer_id), "unknown peer id must not resolve yet");

    // kad discovery confirms the network identity, which applies committee trust immediately
    peer_manager.add_known_peer(bls, info);

    // gap closed the instant discovery completed: validator + important (trusted)
    assert!(peer_manager.is_peer_validator(&peer_id), "discovered member must now be a validator");
    assert!(
        peer_manager.peer_is_important(&peer_id),
        "discovered member must be trusted/important"
    );
}

/// Build a well-formed [`NetworkInfo`] with a fresh, random network identity.
fn random_network_info() -> NetworkInfo {
    let netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    NetworkInfo {
        pubkey: netkey,
        multiaddrs: vec![create_multiaddr(None)],
        timestamp: now(),
        rpc: None,
    }
}

#[tokio::test]
async fn test_discovered_peers_bounded_to_committee_membership() {
    // Regression (issue #827): a flood of signature-valid kad records for fresh, non-committee keys
    // must not grow `known_peers`. Only records whose key is a tracked committee member are cached.
    let mut peer_manager = create_test_peer_manager(None);

    // a single legitimate current-committee member, tracked by bls before its identity is known
    let mut rng = StdRng::from_seed([11; 32]);
    let committee_bls = *BlsKeypair::generate(&mut rng).public();
    peer_manager.update_committees(HashSet::new(), HashSet::from([committee_bls]), HashSet::new());
    assert!(peer_manager.known_peers.is_empty(), "no records cached before discovery");

    // an attacker publishes many valid records for endless fresh keys it controls; none are
    // committee members, so every one is dropped and `known_peers` never grows
    let mut attacker_rng = StdRng::from_seed([42; 32]);
    for _ in 0..1_000 {
        let attacker_bls = *BlsKeypair::generate(&mut attacker_rng).public();
        peer_manager.add_discovered_peer(attacker_bls, random_network_info());
    }
    assert!(
        peer_manager.known_peers.is_empty(),
        "non-committee discovered records must all be dropped"
    );

    // the legitimate committee member's record, arriving via the same discovery path, IS cached
    peer_manager.add_discovered_peer(committee_bls, random_network_info());
    assert!(
        peer_manager.known_peers.contains_key(&committee_bls),
        "a committee member discovered via kad must be cached"
    );
    assert!(peer_manager.known_peers.len() == 1, "only the committee member is cached");
}

#[tokio::test]
async fn test_self_advertised_connected_peer_confirmed_not_cached_and_bounded() {
    // Issue #827 gate refinement: a non-committee peer that pushes its OWN record over its own
    // authenticated connection (kad-put `source` == the record's advertised network identity) has
    // its bls<->peer-id identity confirmed so a live connection (e.g. an nvv in the gossip mesh) is
    // retained, but it is NOT added to the committee-only `known_peers` cache. A relayed record
    // (source != identity) confirms nothing, and a flood of fresh keys over one connection stays
    // bounded because the peer store re-keys by peer id.
    let mut peer_manager = create_test_peer_manager(None);

    // a non-committee peer self-advertises its own record: source == the advertised network id
    let netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let self_peer_id: PeerId = netkey.clone().into();
    let info = NetworkInfo {
        pubkey: netkey,
        multiaddrs: vec![create_multiaddr(None)],
        timestamp: now(),
        rpc: None,
    };
    let bls = *BlsKeypair::generate(&mut StdRng::from_seed([7; 32])).public();
    peer_manager.add_self_advertised_peer(self_peer_id, bls, info);

    // identity confirmed in the peer store (resolvable by peer id) ...
    assert_eq!(
        peer_manager.peer_to_bls(&self_peer_id),
        Some(bls),
        "self-advertised connected peer must have its identity confirmed"
    );
    // ... but NOT cached in the committee-only known_peers
    assert!(
        peer_manager.known_peers.is_empty(),
        "self-advertised non-committee peer must not enter the committee-only known_peers cache"
    );

    // a relayed record (source is not the advertised identity) confirms nothing and is not cached,
    // so a peer can never displace an identity it does not control
    let relayed = random_network_info();
    let relayed_bls = *BlsKeypair::generate(&mut StdRng::from_seed([8; 32])).public();
    let unrelated_source = PeerId::random();
    peer_manager.add_self_advertised_peer(unrelated_source, relayed_bls, relayed);
    assert_eq!(
        peer_manager.peer_to_bls(&unrelated_source),
        None,
        "a relayed record must not confirm an identity the sender does not control"
    );
    assert!(peer_manager.known_peers.is_empty(), "relayed record must not be cached");

    // a flood of fresh bls keys all self-advertised over ONE connection stays bounded: the peer
    // store re-keys by peer id (one confirmed identity per connection) so only the latest survives,
    // and known_peers never grows
    let flood_netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    let flood_peer_id: PeerId = flood_netkey.clone().into();
    let mut attacker_rng = StdRng::from_seed([42; 32]);
    let last_bls = (0..1_000).fold(bls, |_, _| {
        let next_bls = *BlsKeypair::generate(&mut attacker_rng).public();
        let info = NetworkInfo {
            pubkey: flood_netkey.clone(),
            multiaddrs: vec![create_multiaddr(None)],
            timestamp: now(),
            rpc: None,
        };
        peer_manager.add_self_advertised_peer(flood_peer_id, next_bls, info);
        next_bls
    });
    assert!(
        peer_manager.known_peers.is_empty(),
        "a self-advertised flood must never grow the committee-only known_peers cache"
    );
    assert_eq!(
        peer_manager.peer_to_bls(&flood_peer_id),
        Some(last_bls),
        "the flooded connection retains exactly one (the latest) confirmed identity, not 1000"
    );
}

#[tokio::test]
async fn test_known_peers_pruned_on_rotation_but_pinned_survive() {
    // Regression (issue #827): committee members discovered in one epoch must not accumulate across
    // rotations, while operator-provisioned (pinned) peers must never be evicted.
    let mut peer_manager = create_test_peer_manager(None);
    let mut rng = StdRng::from_seed([13; 32]);

    // an operator/bootstrap peer added via `add_known_peer` is pinned even though it is never a
    // committee member
    let pinned_bls = *BlsKeypair::generate(&mut rng).public();
    peer_manager.add_known_peer(pinned_bls, random_network_info());

    // a committee member discovered this epoch via the kad path
    let member_bls = *BlsKeypair::generate(&mut rng).public();
    peer_manager.update_committees(HashSet::new(), HashSet::from([member_bls]), HashSet::new());
    peer_manager.add_discovered_peer(member_bls, random_network_info());
    assert!(peer_manager.known_peers.contains_key(&member_bls), "member cached while in committee");
    assert!(peer_manager.known_peers.contains_key(&pinned_bls), "pinned peer cached");

    // next epoch: `member_bls` falls out of every slot (a member still in `previous` would be kept
    // one more epoch); a fresh committee replaces it
    let mut next_rng = StdRng::from_seed([99; 32]);
    let new_member = *BlsKeypair::generate(&mut next_rng).public();
    peer_manager.update_committees(HashSet::new(), HashSet::from([new_member]), HashSet::new());

    // the rotated-out member is pruned; the pinned operator peer survives
    assert!(
        !peer_manager.known_peers.contains_key(&member_bls),
        "rotated-out member must be pruned"
    );
    assert!(
        peer_manager.known_peers.contains_key(&pinned_bls),
        "pinned operator peer must survive rotation"
    );
}

#[tokio::test]
async fn test_register_outgoing_connection() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel
    let (sender, receiver) = oneshot::channel();

    // Register dial attempt
    peer_manager.register_dial_attempt(peer_id, Some(sender));

    // Register outgoing connection
    let connection = ConnectionType::OutgoingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // Verify dial success was sent
    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    assert!(result.is_ok());

    // Verify peer is connected
    assert!(peer_manager.is_connected(&peer_id));
}

#[tokio::test]
async fn test_peer_limit_reached() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create many connected peers to reach the limit
    let mut peer_ids = Vec::new();
    for _ in 0..50 {
        let peer_id = register_peer(&mut peer_manager, None);
        peer_ids.push(peer_id);
    }

    // Create endpoint for inbound connection
    let multiaddr = create_multiaddr(None);
    let endpoint = ConnectedPoint::Listener {
        local_addr: multiaddr.clone(),
        send_back_addr: multiaddr.clone(),
    };

    // Check if peer limit is reached
    assert!(peer_manager.peer_limit_reached(&endpoint), "Peer limit should be reached");
}

#[tokio::test]
async fn test_peers_for_exchange() {
    let mut peer_manager = create_test_peer_manager(None);

    // Register some peers
    for i in 0..5 {
        let network_key: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let peer_id: PeerId = network_key.clone().into();
        let addr = create_multiaddr(None);

        let multiaddr = addr.clone();
        let connection = ConnectionType::IncomingConnection { multiaddr };
        assert!(peer_manager.register_peer_connection(&peer_id, connection));
        let mut rng = StdRng::from_seed([i; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        peer_manager.add_known_peer(
            bls,
            NetworkInfo {
                pubkey: network_key,
                multiaddrs: vec![addr],
                timestamp: now(),
                rpc: None,
            },
        );
    }

    // Get peers for exchange
    let exchange = peer_manager.peers_for_exchange();

    // Verify we have peers in the exchange
    assert!(!exchange.0.is_empty(), "Should have peers for exchange");

    // Each peer should have their multiaddr in the exchange
    for (_, (_, addrs)) in exchange.into_iter() {
        assert!(!addrs.is_empty(), "Each peer should have at least one multiaddr");
    }
}

#[tokio::test]
async fn test_banned_peer_dial_fails_and_ip_ban() {
    let mut peer_manager = create_test_peer_manager(None);
    let ip = random_ip_addr();
    let multiaddr = create_multiaddr(Some(ip));
    let peer_id = register_peer(&mut peer_manager, Some(multiaddr.clone()));

    // Initially IP is not banned
    assert!(!peer_manager.is_ip_banned(&ip));

    // Apply fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Clear events
    collect_all_events(&mut peer_manager);

    // Register disconnection to finalize the first ban for ip
    peer_manager.register_disconnected(&peer_id);
    assert!(!peer_manager.is_ip_banned(&ip));

    // Clear events
    let events = collect_all_events(&mut peer_manager);

    // there should be a banned event
    let banned_events = extract_events(&events, |e| matches!(e, PeerEvent::Banned(_)));
    assert!(
        matches!(
            banned_events.first().unwrap(), PeerEvent::Banned(id) if *id == peer_id
        ),
        "Expected banned event after fatal penalty"
    );

    // assert behavior stops connection
    let connection_id = ConnectionId::new_unchecked(0);
    let local = create_multiaddr(None);

    // handle banned peer id trying to reconnect
    let reconnect_attempt = peer_manager.handle_established_inbound_connection(
        connection_id,
        peer_id,
        &local,
        &multiaddr,
    );

    // assert inbound connection fails
    assert!(reconnect_attempt.is_err());

    // register different malicious peer id from same ip
    let peer_id = PeerId::random();
    let connection = ConnectionType::IncomingConnection { multiaddr: multiaddr.clone() };
    assert!(peer_manager.register_peer_connection(&peer_id, connection));

    // apply fatal penalty
    peer_manager.process_penalty(peer_id, Penalty::Fatal);

    // Register disconnection to finalize the first ban for ip
    peer_manager.register_disconnected(&peer_id);

    // verify IP is now banned after second peer banned from ip
    assert!(peer_manager.is_ip_banned(&ip));

    // assert pending dial attempt fails
    let dial_attempt =
        peer_manager.handle_pending_inbound_connection(connection_id, &local, &multiaddr);
    assert!(dial_attempt.is_err());
}

// Regression: a peer whose reputation is Banned but whose ConnectionStatus
// has been flipped back to Disconnected (via the Unbanned transition) must
// NOT be dialable. Pre-fix `can_dial` only consulted status + the
// temporarily_banned LRU, so it returned `true` here and `kad` was free to
// re-dial the banned peer.
#[tokio::test]
async fn test_can_dial_rejects_reputation_banned_disconnected_peer() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    // drive peer to reputation=Banned, ConnectionStatus=Banned
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);
    assert!(peer_manager.peer_banned(&peer_id));

    // flip ConnectionStatus Banned -> Disconnected without touching reputation
    let _ = peer_manager.peers.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);

    // clear temporarily_banned to isolate the rep-based ban check
    peer_manager.temporarily_banned.remove(&peer_id);

    // sanity: state is rep-banned + Disconnected + not temp-banned
    assert!(!peer_manager.temporarily_banned.contains(&peer_id));
    assert!(peer_manager.peer_banned(&peer_id), "rep+IP ban predicate still flags peer");
    assert!(
        peer_manager.peers.can_dial(&peer_id),
        "low-level can_dial returns true for Disconnected status"
    );

    // The unified can_dial must reject the dial because reputation is Banned.
    assert!(
        !peer_manager.can_dial(&peer_id),
        "can_dial must reject reputation-banned peer in Disconnected status"
    );
}

// Regression: `PeerAction::Ban` must arm `temporarily_banned`, matching the
// behavior of `Disconnect`/`DisconnectWithPX`. Without this, a peer that
// transitions directly into the banned state via `process_ban` would slip
// through the temp-ban LRU veto.
#[tokio::test]
async fn test_peer_action_ban_arms_temporarily_banned() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = register_peer(&mut peer_manager, None);

    assert!(!peer_manager.temporarily_banned.contains(&peer_id));

    peer_manager.apply_peer_action(peer_id, PeerAction::Ban(Vec::new()));

    assert!(
        peer_manager.temporarily_banned.contains(&peer_id),
        "PeerAction::Ban must arm temporarily_banned"
    );
}

// Regression: an in-flight dial whose peer became banned mid-dial must be
// rejected at `handle_pending_outbound_connection`. Pre-fix, the
// `dial_attempt_already_registered` short-circuit returned `Ok(vec![])` with
// no ban check, leaving the dial to fail later at the established stage and
// triggering a tight retry loop.
#[tokio::test]
async fn test_pending_outbound_rejected_when_dialing_state_and_banned() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();

    // put peer into Dialing state so `dial_attempt_already_registered` returns true
    peer_manager.register_dial_attempt(peer_id, None);
    assert!(peer_manager.dial_attempt_already_registered(&peer_id));

    // ban the peer mid-dial via temp-ban LRU
    peer_manager.temporarily_banned.insert(peer_id);
    assert!(peer_manager.peer_banned(&peer_id));

    let connection_id = ConnectionId::new_unchecked(0);
    let result = peer_manager.handle_pending_outbound_connection(
        connection_id,
        Some(peer_id),
        &[],
        Endpoint::Dialer,
    );

    assert!(
        result.is_err(),
        "pending outbound connection must be denied for banned peer in Dialing state"
    );
}

#[test]
fn test_extract_ip_from_multiaddr() {
    // test ipv4
    let ipv4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    let addr = Multiaddr::empty().with(ipv4.into()).with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), Some(ipv4));

    // test ipv6
    let ipv6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    let addr = Multiaddr::empty().with(ipv6.into()).with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), Some(ipv6));

    // test no ip (e.g., only tcp)
    let addr = Multiaddr::empty().with(Protocol::Tcp(8080));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), None);

    // test unsupported protocol
    let addr = Multiaddr::empty().with(Protocol::Dns("example.com".into()));
    assert_eq!(PeerManager::extract_ip_from_multiaddr(&addr), None);
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_valid_ips() {
    let peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);
    assert!(peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_no_valid_ips() {
    let peer_manager = create_test_peer_manager(None);
    let addr = Multiaddr::empty().with(Protocol::Tcp(8080)); // No IP

    assert!(!peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_with_banned_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);
    let peer_id = PeerId::random();

    // connect and ban the peer to ban the ip
    let connection = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);

    // create another peer with same ip (need 2 peers from same ip for ip ban)
    let peer_id2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id2, connection2);
    peer_manager.process_penalty(peer_id2, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id2);

    // IP should be banned
    assert!(!peer_manager.has_valid_unbanned_ips(&[addr]));
}

#[tokio::test]
async fn test_has_valid_unbanned_ips_early_return_on_banned() {
    let mut peer_manager = create_test_peer_manager(None);
    let banned_ip = random_ip_addr();
    let valid_ip = random_ip_addr();

    // ban the first ip
    let peer_id = PeerId::random();
    let addr1 = create_multiaddr(Some(banned_ip));
    let connection = ConnectionType::IncomingConnection { multiaddr: addr1.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);
    peer_manager.process_penalty(peer_id, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id);

    let peer_id2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr1.clone() };
    peer_manager.register_peer_connection(&peer_id2, connection2);
    peer_manager.process_penalty(peer_id2, Penalty::Fatal);
    peer_manager.register_disconnected(&peer_id2);

    // create multiaddrs with banned ip first, valid ip second
    let addr2 = create_multiaddr(Some(valid_ip));

    // should return false on first banned ip (early return)
    assert!(!peer_manager.has_valid_unbanned_ips(&[addr1, addr2]));
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_duplicates() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);
    let peer_info = PeerInfo { peer_id, addrs: vec![addr.clone()] };

    // add same peer twice
    peer_manager.process_peers_for_discovery(vec![peer_info.clone()]);
    peer_manager.process_peers_for_discovery(vec![peer_info.clone()]);

    // should only have one entry
    assert_eq!(peer_manager.discovery_peers.len(), 1);
    assert_eq!(peer_manager.discovery_peers.remove(&peer_id), Some(vec![addr]));
}

// Regression (issue #777 Part B): the node's own peer id must never be added to
// the discovery feed. A self entry (learned via kad closest-peers or peer
// exchange) would otherwise be selected for a self-dial during the heartbeat.
#[tokio::test]
async fn test_self_id_filtered_from_discovery() {
    let mut peer_manager = create_test_peer_manager(None);
    let local = peer_manager.local_peer_id;
    let other = PeerId::random();
    let addr = create_multiaddr(None);

    // our own id is not eligible; a normal peer is
    assert!(!peer_manager
        .eligible_for_discovery(&PeerInfo { peer_id: local, addrs: vec![addr.clone()] }));
    assert!(peer_manager
        .eligible_for_discovery(&PeerInfo { peer_id: other, addrs: vec![addr.clone()] }));

    // feeding self + a normal peer through the discovery sink registers only the
    // normal peer
    peer_manager.process_peers_for_discovery(vec![
        PeerInfo { peer_id: local, addrs: vec![addr.clone()] },
        PeerInfo { peer_id: other, addrs: vec![addr.clone()] },
    ]);
    assert!(!peer_manager.discovery_peers.contains_key(&local));
    assert_eq!(peer_manager.discovery_peers.remove(&other), Some(vec![addr]));
}

// Regression (issue #777 Part B): routing the node's own peer id through the
// dial path is a no-op (no dial request, no self-penalty) and reports success so
// retrying callers do not treat it as a failure.
#[tokio::test]
async fn test_self_dial_request_is_noop() {
    let mut peer_manager = create_test_peer_manager(None);
    let local = peer_manager.local_peer_id;
    // mirror the observed hairpin address (192.168.8.1)
    let hairpin = create_multiaddr(Some(IpAddr::V4(Ipv4Addr::new(192, 168, 8, 1))));
    let (sender, receiver) = oneshot::channel();

    peer_manager.dial_peer(local, vec![hairpin], Some(sender));

    // no dial request was queued
    assert!(peer_manager.next_dial_request().is_none());
    // the caller is told the no-op succeeded (so `dial_peer_bls` does not retry)
    let result = timeout(Duration::from_millis(500), receiver).await.unwrap().unwrap();
    assert!(result.is_ok());
    // the node never scored or tracked itself
    assert!(!peer_manager.peer_banned(&local));
    assert!(peer_manager.peer_score(&local).is_none());
}

// Regression (issue #777 Part B): a penalty targeting the node's own id is a
// no-op, even a `Fatal` one — a self-connection can never ban the node.
#[tokio::test]
async fn test_self_penalty_is_noop() {
    let mut peer_manager = create_test_peer_manager(None);
    let local = peer_manager.local_peer_id;

    peer_manager.process_penalty(local, Penalty::Fatal);

    assert!(!peer_manager.peer_banned(&local));
    assert!(peer_manager.peer_score(&local).is_none());
    let events = collect_all_events(&mut peer_manager);
    assert!(extract_events(&events, |e| matches!(e, PeerEvent::Banned(_))).is_empty());
}

// Regression (issue #777 Part B): a self-connection is denied without scoring at
// the connection-establishment handlers. The pending-outbound guard is the
// precise fix for kad auto-dialing our own re-learned record; the established
// handlers are the backstop. A normal peer is still accepted.
#[tokio::test]
async fn test_self_connection_denied() {
    let mut peer_manager = create_test_peer_manager(None);
    let local = peer_manager.local_peer_id;
    let other = PeerId::random();
    let connection_id = ConnectionId::new_unchecked(0);
    let addr = create_multiaddr(None);

    // kad-initiated dial of our own id is denied at the pending stage
    assert!(peer_manager
        .handle_pending_outbound_connection(connection_id, Some(local), &[], Endpoint::Dialer)
        .is_err());
    // a normal peer is allowed to be dialed
    assert!(peer_manager
        .handle_pending_outbound_connection(connection_id, Some(other), &[], Endpoint::Dialer)
        .is_ok());

    // an inbound self-connection (loopback/hairpin) is dropped
    assert!(peer_manager
        .handle_established_inbound_connection(connection_id, local, &addr, &addr)
        .is_err());

    // and no self-penalty was ever recorded by any of the above
    assert!(!peer_manager.peer_banned(&local));
    assert!(peer_manager.peer_score(&local).is_none());
}

#[tokio::test]
async fn test_get_closest_peers_timeout_recovers_partial_peers() {
    // Regression: a kademlia `GetClosestPeers` query that times out still reports
    // the peers it located before expiring, and those peers must reach the
    // discovery pool instead of being discarded along with the failed query.
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);
    let peer_info = PeerInfo { peer_id, addrs: vec![addr.clone()] };

    // kademlia surfaces a timeout carrying the closest peers found so far
    let err = GetClosestPeersError::Timeout { key: peer_id.to_bytes(), peers: vec![peer_info] };

    // the recovery helper must hand back exactly the peers the query located
    let recovered = partial_peers_from_get_closest_timeout(err);
    assert_eq!(recovered.len(), 1);

    // feeding them through the discovery sink registers the peer as a discovery
    // candidate, exactly as a successful query would have
    peer_manager.process_peers_for_discovery(recovered);
    assert_eq!(peer_manager.discovery_peers.remove(&peer_id), Some(vec![addr]));
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_no_valid_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let invalid_addr = Multiaddr::empty().with(Protocol::Tcp(8080)); // No IP
    let peer_info = PeerInfo { peer_id, addrs: vec![invalid_addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_banned_ip() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);

    // ban the ip first (need 2 peers from same ip)
    let banned_peer1 = PeerId::random();
    let connection1 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&banned_peer1, connection1);
    peer_manager.process_penalty(banned_peer1, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer1);

    let banned_peer2 = PeerId::random();
    let connection2 = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&banned_peer2, connection2);
    peer_manager.process_penalty(banned_peer2, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer2);

    // try to add a new peer with banned ip
    let new_peer = PeerId::random();
    let peer_info = PeerInfo { peer_id: new_peer, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_connected_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(None);

    // connect the peer first
    let connection = ConnectionType::IncomingConnection { multiaddr: addr.clone() };
    peer_manager.register_peer_connection(&peer_id, connection);

    // try to add to discovery
    let peer_info = PeerInfo { peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added (can't dial connected peers)
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_filters_dialing_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let peer_id = PeerId::random();
    let addr = create_multiaddr(Some(random_ip_addr()));

    // register dial attempt
    peer_manager.register_dial_attempt(peer_id, None);

    // try to add to discovery
    let peer_info = PeerInfo { peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![peer_info]);

    // should not be added (already dialing)
    assert_eq!(peer_manager.discovery_peers.len(), 0);
}

#[tokio::test]
async fn test_process_peers_for_discovery_accepts_valid_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // Create multiple valid peers
    let peer1 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };
    let peer2 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };
    let peer3 = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };

    peer_manager.process_peers_for_discovery(vec![peer1, peer2, peer3]);

    // all should be added
    assert_eq!(peer_manager.discovery_peers.len(), 3);
}

#[tokio::test]
async fn test_process_peers_for_discovery_mixed_valid_invalid() {
    let mut peer_manager = create_test_peer_manager(None);

    let valid_peer = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![create_multiaddr(Some(random_ip_addr()))],
    };

    let invalid_peer_no_ip = PeerInfo {
        peer_id: PeerId::random(),
        addrs: vec![Multiaddr::empty().with(Protocol::Tcp(8080))],
    };

    let connected_peer_id = PeerId::random();
    let addr = create_multiaddr(Some(random_ip_addr()));
    peer_manager.register_peer_connection(
        &connected_peer_id,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    let connected_peer = PeerInfo { peer_id: connected_peer_id, addrs: vec![addr] };

    peer_manager.process_peers_for_discovery(vec![valid_peer, invalid_peer_no_ip, connected_peer]);

    // only the valid peer should be added
    assert_eq!(peer_manager.discovery_peers.len(), 1);
}

#[tokio::test]
async fn test_discovery_heartbeat_filters_ineligible_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // add a valid peer to discovery
    let valid_peer = PeerId::random();
    let valid_addr = create_multiaddr(None);
    peer_manager.discovery_peers.insert(valid_peer, vec![valid_addr.clone()]);

    // add a peer with no valid ip
    let invalid_peer = PeerId::random();
    let invalid_addr = Multiaddr::empty().with(Protocol::Tcp(8080));
    peer_manager.discovery_peers.insert(invalid_peer, vec![invalid_addr]);

    // add a connected peer (ineligible)
    let connected_peer = PeerId::random();
    let connected_addr = create_multiaddr(None);
    peer_manager.register_peer_connection(
        &connected_peer,
        ConnectionType::IncomingConnection { multiaddr: connected_addr.clone() },
    );
    peer_manager.discovery_peers.insert(connected_peer, vec![connected_addr]);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // assert valid peer is dialing
    let valid_peer_dial_request =
        peer_manager.dial_requests.pop_front().expect("valid peer dial request");

    assert_matches!(
        valid_peer_dial_request,
        DialRequest { peer_id, multiaddrs, .. }
        if peer_id == valid_peer && multiaddrs == vec![valid_addr]
    );
    // assert all discoveries are empty
    assert!(!peer_manager.discovery_peers.contains_key(&valid_peer));
    assert!(!peer_manager.discovery_peers.contains_key(&invalid_peer));
    assert!(!peer_manager.discovery_peers.contains_key(&connected_peer));
    // assert discovery event emitted
    let event = peer_manager.events.pop_front().expect("discovery event");
    assert_matches!(event, PeerEvent::Discovery);
}

#[tokio::test]
async fn test_discovery_heartbeat_respects_target_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // connect enough peers to meet target
    let target = peer_manager.config.target_num_peers;
    for _ in 0..target {
        let peer_id = PeerId::random();
        let addr = create_multiaddr(None);
        peer_manager.register_peer_connection(
            &peer_id,
            ConnectionType::IncomingConnection { multiaddr: addr },
        );
    }

    // add max discovery peers
    let target = peer_manager.config.max_discovery_peers();
    for _ in 0..target {
        let discovery_peer = PeerId::random();
        peer_manager.discovery_peers.insert(discovery_peer, vec![create_multiaddr(None)]);
    }

    let initial_discovery_count = peer_manager.discovery_peers.len();

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // should not dial any peers (target reached)
    assert_eq!(peer_manager.dial_requests.len(), 0);
    // do not emit discovery event
    assert_eq!(peer_manager.events.len(), 0);
    // discovery peers should still be in the pool
    assert_eq!(peer_manager.discovery_peers.len(), initial_discovery_count);
}

#[tokio::test]
async fn test_discovery_heartbeat_prunes_excess_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let max_discovery = peer_manager.config.max_discovery_peers();

    // Add more than max discovery peers
    let excess_count = max_discovery + 10;
    for _ in 0..excess_count {
        let peer_id = PeerId::random();
        peer_manager.discovery_peers.insert(peer_id, vec![create_multiaddr(None)]);
    }

    // too many before heartbeat
    assert!(peer_manager.discovery_peers.len() > max_discovery);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // should prune down to max (or slightly less if some were dialed)
    assert!(peer_manager.discovery_peers.len() <= max_discovery);
}

#[tokio::test]
async fn test_discovery_heartbeat_removes_banned_ip_peers() {
    let mut peer_manager = create_test_peer_manager(None);
    let addr = create_multiaddr(None);

    // Ban the IP by banning 2 peers from that IP
    let banned_peer1 = PeerId::random();
    peer_manager.register_peer_connection(
        &banned_peer1,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    peer_manager.process_penalty(banned_peer1, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer1);

    let banned_peer2 = PeerId::random();
    peer_manager.register_peer_connection(
        &banned_peer2,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() },
    );
    peer_manager.process_penalty(banned_peer2, Penalty::Fatal);
    peer_manager.register_disconnected(&banned_peer2);

    // add a discovery peer with the banned ip
    let discovery_peer = PeerId::random();
    peer_manager.discovery_peers.insert(discovery_peer, vec![addr]);

    // run heartbeat
    peer_manager.discovery_heartbeat();

    // discovery peer with banned ip should be removed
    assert!(!peer_manager.discovery_peers.contains_key(&discovery_peer));
}
