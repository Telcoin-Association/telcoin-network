//! Unit tests for peer manager

use super::*;
use assert_matches::assert_matches;
use std::net::Ipv4Addr;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;

fn create_test_peer_manager() -> PeerManager {
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config = authority_1.consensus_config();
    PeerManager::new(&config)
}

fn create_test_multiaddr(id: u8) -> Multiaddr {
    format!("/ip4/{}/udp/45454/quic-v1", Ipv4Addr::new(127, 0, 0, id)).parse().unwrap()
}

/// Helper function to extract events of a certain type
fn extract_events<'a>(
    events: &'a [PeerEvent],
    event_type: fn(&'a PeerEvent) -> bool,
) -> Vec<&'a PeerEvent> {
    events.iter().filter(|e| event_type(e)).collect()
}

#[test]
fn test_register_disconnected_basic() {
    let mut peer_manager = create_test_peer_manager();
    let peer_id = PeerId::random();
    let multiaddr = create_test_multiaddr(1);

    // register connection
    assert!(peer_manager.register_incoming_connection(&peer_id, multiaddr));

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // assert peer is no longer connected
    assert!(!peer_manager.is_connected(&peer_id));

    // assert no events from disconnect without ban
    assert!(peer_manager.poll_events().is_none());
}

#[tokio::test]
async fn test_register_disconnected_with_banned_peer() {
    tn_test_utils::init_test_tracing();

    let mut peer_manager = create_test_peer_manager();
    let peer_id = PeerId::random();
    let multiaddr = create_test_multiaddr(2);

    // Register connection first
    assert!(peer_manager.register_incoming_connection(&peer_id, multiaddr));

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

    debug!(target: "peer-manager", ?disconnect_event, "made it here :D");

    // register disconnection
    peer_manager.register_disconnected(&peer_id);

    // There should be no additional ban events since the peer is already banned
    let mut banned_events = Vec::new();
    while let Some(event) = peer_manager.poll_events() {
        banned_events.push(event);
    }

    debug!(target: "peer-manager", ?banned_events, "made it here");

    let banned_event =
        extract_events(&banned_events, |e| matches!(e, PeerEvent::Banned(_, _))).len();
    assert!(banned_event == 1, "Expect one banned event");
    assert_matches!(
        banned_events.first().unwrap(),
        PeerEvent::Banned(id, _) if *id == peer_id
    );

    // assert peer is still banned after disconnection
    assert!(
        peer_manager.connection_banned(&peer_id),
        "Peer should remain banned after disconnection"
    );
}

// test temp ban from DisconnectWithPX
