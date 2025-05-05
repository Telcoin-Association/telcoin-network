//! Unit tests for peer manager

use super::*;
use crate::common::{create_multiaddr, random_ip_addr};
use assert_matches::assert_matches;
use libp2p::swarm::{ConnectionId, NetworkBehaviour as _};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tn_config::{NetworkConfig, ScoreConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tokio::time::{sleep, timeout};

fn create_test_peer_manager(network_config: Option<NetworkConfig>) -> PeerManager {
    let network_config = network_config.unwrap_or_default();
    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    PeerManager::new(config.network_config().peer_config())
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
    let peer_id = PeerId::random();
    let multiaddr = create_multiaddr(None);

    // Create a oneshot channel to simulate the reply channel
    let (sender, _receiver) = oneshot::channel();

    // Add trusted peer
    peer_manager.add_explicit_peer(peer_id, multiaddr.clone(), sender);

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
    peer_manager.dial_peer(peer_id, multiaddr.clone(), sender);

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
    peer_manager.dial_peer(peer_id, multiaddr.clone(), sender);

    // Verify a dial request was created
    let dial_request = peer_manager.next_dial_request().unwrap();

    // Register the dial attempt
    peer_manager.register_dial_attempt(peer_id, dial_request.reply);

    // Create another oneshot channel
    let (sender2, receiver2) = oneshot::channel();

    // Try to dial the same peer again
    peer_manager.dial_peer(peer_id, multiaddr.clone(), sender2);

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
    peer_manager.dial_peer(peer_id, multiaddr.clone(), sender);

    // Verify no dial request was created
    assert!(peer_manager.next_dial_request().is_none());

    // Verify the error from the oneshot channel
    let result = timeout(Duration::from_millis(500), receiver).await;
    let channel_result = result.unwrap().unwrap();
    assert!(channel_result.is_err()); // Dial should have failed with an error
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
    let peer_id1 = PeerId::random();
    let peer_id2 = PeerId::random();
    let multiaddr1 = create_multiaddr(None);
    let multiaddr2 = create_multiaddr(None);

    let mut exchange_map = HashMap::new();
    let multiaddrs1 = HashSet::from([multiaddr1]);
    let multiaddrs2 = HashSet::from([multiaddr2]);
    exchange_map.insert(peer_id1, multiaddrs1);
    exchange_map.insert(peer_id2, multiaddrs2);

    let exchange = PeerExchangeMap::from(exchange_map);

    // Process the peer exchange
    peer_manager.process_peer_exchange(exchange);

    // Verify dial requests were created for both peers
    let _ = peer_manager.next_dial_request().expect("peer 1 exchange");
    let _ = peer_manager.next_dial_request().expect("peer 2 exchange");

    // Verify no more dial requests
    assert!(peer_manager.next_dial_request().is_none());
}

#[tokio::test]
async fn test_prune_connected_peers() {
    let mut peer_manager = create_test_peer_manager(None);

    // Register many peers
    let mut peer_ids = Vec::new();
    for _ in 0..20 {
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
    let mut peer_manager = PeerManager::new(config.network_config().peer_config());
    let validator = authority_1.id().peer_id();
    let random_peer_id = PeerId::random();

    // update epoch with random multiaddr
    let committee =
        config.committee_peer_ids().into_iter().map(|id| (id, create_multiaddr(None))).collect();
    peer_manager.new_epoch(committee);

    // Verify validator peer is recognized
    assert!(peer_manager.is_validator(&validator));

    // Verify random peer is not a validator
    assert!(!peer_manager.is_validator(&random_peer_id));
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
    for _ in 0..5 {
        register_peer(&mut peer_manager, None);
    }

    // Get peers for exchange
    let exchange = peer_manager.peers_for_exchange();

    // Verify we have peers in the exchange
    assert!(!exchange.0.is_empty(), "Should have peers for exchange");

    // Each peer should have their multiaddr in the exchange
    for (_, addrs) in exchange.into_iter() {
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
