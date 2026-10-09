//! Regression coverage for population admission and independent ban owners (issue #1475).

use super::*;
use libp2p::{
    core::{transport::PortUse, ConnectedPoint},
    swarm::{
        behaviour::ConnectionEstablished, ConnectionClosed, ConnectionDenied, DialFailure,
        FromSwarm, ListenError, ListenFailure,
    },
};

/// A small ordinary population with additional outbound discovery headroom.
fn manager_with_capacity() -> PeerManager {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().target_num_peers = 2;
    config.peer_config_mut().peer_excess_factor = 0.0;
    config.peer_config_mut().priority_peer_excess = 1.0;
    create_test_peer_manager(Some(config))
}

/// Exercise authenticated admission through the behaviour callback in either direction.
fn admit(
    manager: &mut PeerManager,
    connection_id: ConnectionId,
    peer_id: PeerId,
    direction: Endpoint,
) -> Result<(), ConnectionDenied> {
    let addr = create_multiaddr(None);
    if direction == Endpoint::Listener {
        manager
            .handle_established_inbound_connection(connection_id, peer_id, &addr, &addr)
            .map(|_| ())
    } else {
        manager
            .handle_established_outbound_connection(
                connection_id,
                peer_id,
                &addr,
                Endpoint::Dialer,
                PortUse::Reuse,
            )
            .map(|_| ())
    }
}

/// Construct the actual swarm event that commits an admitted inbound connection.
fn establish(manager: &mut PeerManager, connection_id: ConnectionId, peer_id: PeerId) {
    let addr = create_multiaddr(None);
    let endpoint = ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr };
    manager.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id,
        connection_id,
        endpoint: &endpoint,
        failed_addresses: &[],
        other_established: 0,
    }));
}

/// Closing one connection must not release another connection to the same identity.
fn close(
    manager: &mut PeerManager,
    connection_id: ConnectionId,
    peer_id: PeerId,
    remaining: usize,
) {
    let addr = create_multiaddr(None);
    let endpoint = ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr };
    manager.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
        peer_id,
        connection_id,
        endpoint: &endpoint,
        cause: None,
        remaining_established: remaining,
    }));
}

/// The final slot is usable, reservations cannot oversubscribe, and duplicates share a slot.
#[tokio::test]
async fn test_admission_thresholds_and_duplicates() -> Result<(), ConnectionDenied> {
    [Endpoint::Listener, Endpoint::Dialer].into_iter().try_for_each(|direction| {
        let mut closed = manager_with_capacity();
        closed.config.target_num_peers = 0;
        assert!(admit(&mut closed, ConnectionId::new_unchecked(1), PeerId::random(), direction)
            .is_err());
        let mut manager = manager_with_capacity();
        let first = PeerId::random();
        let limit = if direction == Endpoint::Listener {
            manager.config.max_peers()
        } else {
            manager.config.max_outbound_dialing_peers()
        };
        admit(&mut manager, ConnectionId::new_unchecked(1), first, direction)?;
        (1..limit).try_for_each(|index| {
            admit(&mut manager, ConnectionId::new_unchecked(index + 1), PeerId::random(), direction)
        })?;
        assert!(!manager.is_connected(&first), "admission must not commit peer state");
        assert!(admit(
            &mut manager,
            ConnectionId::new_unchecked(limit + 1),
            PeerId::random(),
            direction
        )
        .is_err());
        admit(&mut manager, ConnectionId::new_unchecked(limit + 2), first, direction)?;
        assert!(admit(
            &mut manager,
            ConnectionId::new_unchecked(limit + 3),
            PeerId::random(),
            direction
        )
        .is_err());
        Ok(())
    })
}

/// Establishment publishes the final admitted slot; duplicates retain it until their last close.
#[tokio::test]
async fn test_admission_commit_close_and_replacement() -> Result<(), ConnectionDenied> {
    let mut manager = manager_with_capacity();
    let first = PeerId::random();
    let second = PeerId::random();
    let first_id = ConnectionId::new_unchecked(1);
    let duplicate_id = ConnectionId::new_unchecked(2);
    let second_id = ConnectionId::new_unchecked(3);
    admit(&mut manager, first_id, first, Endpoint::Listener)?;
    establish(&mut manager, first_id, first);
    admit(&mut manager, duplicate_id, first, Endpoint::Listener)?;
    establish(&mut manager, duplicate_id, first);
    admit(&mut manager, second_id, second, Endpoint::Listener)?;
    establish(&mut manager, second_id, second);
    assert!(manager.is_connected(&second));
    let events = collect_all_events(&mut manager);
    assert!(events
        .iter()
        .any(|event| matches!(event, PeerEvent::PeerConnected(peer, _) if *peer == second)));
    assert!(!events
        .iter()
        .any(|event| matches!(event, PeerEvent::DisconnectPeerX(peer, _) if *peer == second)));
    close(&mut manager, first_id, first, 1);
    assert!(manager.is_connected(&first));
    assert!(admit(
        &mut manager,
        ConnectionId::new_unchecked(4),
        PeerId::random(),
        Endpoint::Listener
    )
    .is_err());
    close(&mut manager, duplicate_id, first, 0);
    admit(&mut manager, ConnectionId::new_unchecked(5), PeerId::random(), Endpoint::Listener)
}

/// A later inbound behaviour denial releases only its reservation, exactly once.
#[tokio::test]
async fn test_admission_listen_failure_rollback() -> Result<(), ConnectionDenied> {
    let mut manager = manager_with_capacity();
    let first = PeerId::random();
    let second = PeerId::random();
    let first_id = ConnectionId::new_unchecked(1);
    admit(&mut manager, first_id, first, Endpoint::Listener)?;
    admit(&mut manager, ConnectionId::new_unchecked(2), second, Endpoint::Listener)?;
    let addr = create_multiaddr(None);
    let error = ListenError::Denied { cause: ConnectionDenied::new("later behaviour denial") };
    let failure = FromSwarm::ListenFailure(ListenFailure {
        connection_id: first_id,
        peer_id: Some(first),
        error: &error,
        local_addr: &addr,
        send_back_addr: &addr,
    });
    manager.on_swarm_event(failure);
    admit(&mut manager, ConnectionId::new_unchecked(3), PeerId::random(), Endpoint::Listener)?;
    manager.on_swarm_event(failure);
    assert!(admit(
        &mut manager,
        ConnectionId::new_unchecked(4),
        PeerId::random(),
        Endpoint::Listener
    )
    .is_err());
    assert!(!manager.is_connected(&first));
    assert!(!manager.is_connected(&second));
    assert!(collect_all_events(&mut manager)
        .iter()
        .all(|event| !matches!(event, PeerEvent::PeerConnected(_, _))));
    Ok(())
}

/// Pending outbound reservations survive until either establishment or DialFailure.
#[tokio::test]
async fn test_admission_dial_failure_rollback() -> Result<(), ConnectionDenied> {
    let mut manager = manager_with_capacity();
    let limit = manager.config.max_outbound_dialing_peers();
    let first = PeerId::random();
    let first_id = ConnectionId::new_unchecked(1);
    manager.handle_pending_outbound_connection(first_id, Some(first), &[], Endpoint::Dialer)?;
    (1..limit).try_for_each(|index| {
        manager
            .handle_pending_outbound_connection(
                ConnectionId::new_unchecked(index + 1),
                Some(PeerId::random()),
                &[],
                Endpoint::Dialer,
            )
            .map(|_| ())
    })?;
    assert!(manager
        .handle_pending_outbound_connection(
            ConnectionId::new_unchecked(limit + 1),
            Some(PeerId::random()),
            &[],
            Endpoint::Dialer
        )
        .is_err());
    let error = DialError::Denied { cause: ConnectionDenied::new("later behaviour denial") };
    let failure = FromSwarm::DialFailure(DialFailure {
        connection_id: first_id,
        peer_id: Some(first),
        error: &error,
    });
    manager.on_swarm_event(failure);
    manager.handle_pending_outbound_connection(
        ConnectionId::new_unchecked(limit + 2),
        Some(PeerId::random()),
        &[],
        Endpoint::Dialer,
    )?;
    manager.on_swarm_event(failure);
    assert!(manager
        .handle_pending_outbound_connection(
            ConnectionId::new_unchecked(limit + 3),
            Some(PeerId::random()),
            &[],
            Endpoint::Dialer
        )
        .is_err());
    Ok(())
}

/// Committee and trusted identities retain exemptions and reserve capacity against ordinary peers.
#[tokio::test]
async fn test_admission_privileged_reservations() -> Result<(), ConnectionDenied> {
    let mut manager = manager_with_capacity();
    (0..manager.config.max_peers()).try_for_each(|index| {
        admit(
            &mut manager,
            ConnectionId::new_unchecked(index + 1),
            PeerId::random(),
            Endpoint::Listener,
        )
    })?;
    let keys: Vec<_> =
        (0..3).map(|_| *BlsKeypair::generate(&mut StdRng::from_os_rng()).public()).collect();
    manager.update_committees(
        HashSet::new(),
        keys.get(1).copied().into_iter().collect(),
        HashSet::new(),
    );
    let peers: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let info = random_network_info();
            let peer: PeerId = info.pubkey.clone().into();
            if index == 1 {
                manager.add_discovered_peer(*key, info);
            } else {
                let (reply, _rx) = oneshot::channel();
                manager.add_trusted_peer_and_dial(*key, info, reply);
            }
            peer
        })
        .collect();
    peers.iter().enumerate().try_for_each(|(index, peer)| {
        admit(&mut manager, ConnectionId::new_unchecked(index + 3), *peer, Endpoint::Listener)
    })?;
    assert!(admit(
        &mut manager,
        ConnectionId::new_unchecked(6),
        PeerId::random(),
        Endpoint::Listener
    )
    .is_err());
    Ok(())
}

/// Reputation-cache turnover and ordinary disconnected-table churn must retain a live reconnect
/// ban.
#[tokio::test]
async fn test_admission_ban_table_turnover_preserves_live_ban() {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().max_banned_peers = 1;
    config.peer_config_mut().max_disconnected_peers = 1;
    let mut manager = create_test_peer_manager(Some(config));
    let first = register_peer(&mut manager, None);
    manager.process_penalty(first, Penalty::Fatal);
    manager.register_disconnected(&first);
    let second = register_peer(&mut manager, None);
    manager.process_penalty(second, Penalty::Fatal);
    manager.register_disconnected(&second);
    (0..3).for_each(|_| {
        let peer = register_peer(&mut manager, None);
        manager.register_disconnected(&peer);
    });
    assert!(!manager.peers.peer_banned(&first), "the reputation cache did evict this identity");
    assert!(manager.peer_banned(&first), "the reconnect cache still owns a live ban");
    assert!(
        admit(&mut manager, ConnectionId::new_unchecked(10), first, Endpoint::Listener).is_err()
    );
    assert!(!collect_all_events(&mut manager)
        .iter()
        .any(|event| matches!(event, PeerEvent::Unbanned(peer) if *peer == first)));
}

/// Reconnect-cache capacity eviction must retain a reputation owner's blacklist entry.
#[tokio::test]
async fn test_admission_reconnect_eviction_preserves_reputation_ban() {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().max_temporarily_banned_peers = 1;
    let mut manager = create_test_peer_manager(Some(config));
    let first = register_peer(&mut manager, None);
    manager.process_penalty(first, Penalty::Fatal);
    manager.register_disconnected(&first);
    manager.temporarily_ban(PeerId::random());
    assert!(!manager.temporarily_banned.contains(&first));
    assert!(manager.peer_banned(&first));
    assert!(!collect_all_events(&mut manager)
        .iter()
        .any(|event| matches!(event, PeerEvent::Unbanned(peer) if *peer == first)));
}

/// Expiring the reconnect cache must preserve a reputation owner's blacklist entry.
#[tokio::test]
async fn test_admission_reconnect_expiry_preserves_reputation_ban() {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().excess_peers_reconnection_timeout = Duration::ZERO;
    let mut manager = create_test_peer_manager(Some(config));
    let first = register_peer(&mut manager, None);
    manager.process_penalty(first, Penalty::Fatal);
    manager.register_disconnected(&first);
    manager.unban_temp_banned_peers();
    assert!(!manager.temporarily_banned.contains(&first));
    assert!(manager.peer_banned(&first));
    assert!(!collect_all_events(&mut manager)
        .iter()
        .any(|event| matches!(event, PeerEvent::Unbanned(peer) if *peer == first)));
}

/// A new ban invalidates a queued unban, while later unrelated events still reach the swarm.
#[tokio::test]
async fn test_admission_queued_unban_cannot_revoke_new_ban() {
    let mut manager = create_test_peer_manager(None);
    let peer = PeerId::random();
    manager.push_event(PeerEvent::Unbanned(peer));
    manager.temporarily_ban(peer);
    manager.push_event(PeerEvent::Discovery);
    let events = collect_all_events(&mut manager);
    assert!(events.iter().any(|event| matches!(event, PeerEvent::Discovery)));
    assert!(!events.iter().any(|event| matches!(event, PeerEvent::Unbanned(id) if *id == peer)));
}

/// Authoritative ways to forgive an identity whose reputation record was evicted.
enum Forgiveness {
    /// The operator explicitly adds an allowlisted identity.
    Trusted,
    /// Committee rotation promotes an identity whose network info is already known.
    KnownCommittee,
    /// Discovery resolves an identity after its committee membership was established.
    DiscoveredCommittee,
}

/// Last-owner release must reach gossip for operator, rotation and lazy-discovery forgiveness.
#[tokio::test]
async fn test_admission_authoritative_forgiveness() -> Result<(), ConnectionDenied> {
    [Forgiveness::Trusted, Forgiveness::KnownCommittee, Forgiveness::DiscoveredCommittee]
        .into_iter()
        .try_for_each(|forgiveness| {
            let mut config = NetworkConfig::default();
            config.peer_config_mut().max_banned_peers = 0;
            let mut manager = create_test_peer_manager(Some(config));
            let key = *BlsKeypair::generate(&mut StdRng::from_os_rng()).public();
            let info = random_network_info();
            let peer: PeerId = info.pubkey.clone().into();
            match forgiveness {
                Forgiveness::Trusted | Forgiveness::KnownCommittee => {
                    manager.add_bootstrap_peer(key, info.clone());
                }
                Forgiveness::DiscoveredCommittee => {}
            }
            assert!(manager.register_peer_connection(
                &peer,
                ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) },
            ));
            manager.process_penalty(peer, Penalty::Fatal);
            manager.register_disconnected(&peer);
            assert!(manager.temporarily_banned.contains(&peer));
            assert!(!manager.peers.peer_banned(&peer));
            let banned = collect_all_events(&mut manager);
            assert!(banned
                .iter()
                .any(|event| matches!(event, PeerEvent::Banned(id) if *id == peer)));
            assert!(!banned
                .iter()
                .any(|event| matches!(event, PeerEvent::Unbanned(id) if *id == peer)));
            match forgiveness {
                Forgiveness::Trusted => {
                    let (reply, _rx) = oneshot::channel();
                    manager.add_trusted_peer_and_dial(key, info, reply);
                }
                Forgiveness::KnownCommittee => {
                    manager.update_committees(HashSet::new(), HashSet::from([key]), HashSet::new());
                    manager.add_known_peer(key, info);
                }
                Forgiveness::DiscoveredCommittee => {
                    manager.update_committees(HashSet::new(), HashSet::from([key]), HashSet::new());
                    assert!(
                        manager.temporarily_banned.contains(&peer),
                        "defer until identity resolution"
                    );
                    manager.add_discovered_peer(key, info);
                }
            }
            assert!(!manager.peer_banned(&peer));
            let unbanned = collect_all_events(&mut manager);
            assert_eq!(
                unbanned
                    .iter()
                    .filter(|event| matches!(event, PeerEvent::Unbanned(id) if *id == peer))
                    .count(),
                1
            );
            admit(&mut manager, ConnectionId::new_unchecked(1), peer, Endpoint::Listener)
        })
}

/// The population of `manager_with_capacity` on a bootstrap node that hands off at capacity.
fn hand_off_manager() -> PeerManager {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().target_num_peers = 2;
    config.peer_config_mut().peer_excess_factor = 0.0;
    config.peer_config_mut().priority_peer_excess = 1.0;
    config.peer_config_mut().peer_exchange_at_capacity = true;
    create_test_peer_manager(Some(config))
}

/// The typed capacity cause of a refused admission, if the refusal carries it.
fn capacity_refusal(
    manager: &mut PeerManager,
    connection_id: ConnectionId,
    peer_id: PeerId,
    direction: Endpoint,
) -> Option<PeerCapacityReached> {
    admit(manager, connection_id, peer_id, direction)
        .err()
        .and_then(|denied| denied.downcast::<PeerCapacityReached>().ok())
}

/// Whether the peer's record holds a connection status that satisfies `expected`.
fn status_is(
    manager: &PeerManager,
    peer_id: &PeerId,
    expected: fn(&ConnectionStatus) -> bool,
) -> bool {
    manager.peers.get_peer(peer_id).is_some_and(|record| expected(record.connection_status()))
}

/// Whether the peer's reputation is at or past the ban threshold.
fn reputation_banned(manager: &PeerManager, peer_id: &PeerId) -> bool {
    manager.peers.get_peer(peer_id).is_some_and(|record| record.reputation().banned())
}

/// Count the `Unbanned` notifications for `peer_id` that reached the swarm.
fn unbanned_count(events: &[PeerEvent], peer_id: PeerId) -> usize {
    events.iter().filter(|event| matches!(event, PeerEvent::Unbanned(id) if *id == peer_id)).count()
}

/// Model heartbeat score recovery for a banned reputation.
///
/// The score decays on `std::time::Instant` behind a 30-minute ban lockout, so a unit test cannot
/// wait for it. The record's score is restored, then the `Unbanned` transition is applied exactly
/// as `AllPeers::update_peer_scores` and `PeerManager::heartbeat` apply it.
fn recover_score(manager: &mut PeerManager, peer_id: PeerId) {
    assert!(reputation_banned(manager, &peer_id), "recovery starts from a banned reputation");
    manager.peers.get_peer_mut(&peer_id).into_iter().for_each(|record| record.reset_score_to_max());
    assert!(!reputation_banned(manager, &peer_id), "the reputation must leave the ban threshold");
    let action = manager.peers.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);
    manager.apply_peer_action(peer_id, action);
}

/// A ban on a disconnected peer that outlives a failed queued dial is released exactly once,
/// after score recovery, and never at reconnect-cache expiry.
#[tokio::test]
async fn test_admission_unban_after_failed_dial_waits_for_score_recovery() {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().excess_peers_reconnection_timeout = Duration::ZERO;
    let mut manager = create_test_peer_manager(Some(config));
    let peer = register_peer(&mut manager, None);
    manager.register_disconnected(&peer);
    assert!(status_is(&manager, &peer, |status| matches!(
        status,
        ConnectionStatus::Disconnected { .. }
    )));

    // a ban lands while a dial for the peer is still queued
    manager.process_penalty(peer, Penalty::Fatal);
    assert!(status_is(&manager, &peer, |status| matches!(status, ConnectionStatus::Banned { .. })));
    let banned = collect_all_events(&mut manager);
    assert!(banned.iter().any(|event| matches!(event, PeerEvent::Banned(id) if *id == peer)));
    assert_eq!(unbanned_count(&banned, peer), 0);

    // the queued dial drains and fails, so the status leaves `Banned` before the score recovers
    manager.register_dial_attempt(peer, None);
    assert!(status_is(&manager, &peer, |status| matches!(
        status,
        ConnectionStatus::Dialing { .. }
    )));
    let error = DialError::Denied { cause: ConnectionDenied::new("queued dial refused") };
    manager.on_swarm_event(FromSwarm::DialFailure(DialFailure {
        connection_id: ConnectionId::new_unchecked(1),
        peer_id: Some(peer),
        error: &error,
    }));
    assert!(status_is(&manager, &peer, |status| matches!(
        status,
        ConnectionStatus::Disconnected { .. }
    )));
    assert!(reputation_banned(&manager, &peer));

    // reconnect-cache expiry must not release the reputation owner's blacklist entry
    manager.unban_temp_banned_peers();
    assert!(!manager.temporarily_banned.contains(&peer));
    assert!(manager.peer_banned(&peer));
    assert_eq!(unbanned_count(&collect_all_events(&mut manager), peer), 0);

    recover_score(&mut manager, peer);
    assert!(status_is(&manager, &peer, |status| matches!(
        status,
        ConnectionStatus::Disconnected { .. }
    )));
    assert_eq!(unbanned_count(&collect_all_events(&mut manager), peer), 1);
}

/// A connected peer banned in the common order is released exactly once, after score recovery,
/// and never at reconnect-cache expiry.
#[tokio::test]
async fn test_admission_unban_after_connected_ban_waits_for_score_recovery() {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().excess_peers_reconnection_timeout = Duration::ZERO;
    let mut manager = create_test_peer_manager(Some(config));
    let peer = register_peer(&mut manager, None);
    manager.process_penalty(peer, Penalty::Fatal);
    manager.register_disconnected(&peer);
    assert!(status_is(&manager, &peer, |status| matches!(status, ConnectionStatus::Banned { .. })));
    let banned = collect_all_events(&mut manager);
    assert!(banned.iter().any(|event| matches!(event, PeerEvent::Banned(id) if *id == peer)));
    assert_eq!(unbanned_count(&banned, peer), 0);

    manager.unban_temp_banned_peers();
    assert!(!manager.temporarily_banned.contains(&peer));
    assert!(manager.peer_banned(&peer));
    assert_eq!(unbanned_count(&collect_all_events(&mut manager), peer), 0);

    recover_score(&mut manager, peer);
    assert!(status_is(&manager, &peer, |status| matches!(
        status,
        ConnectionStatus::Disconnected { .. }
    )));
    assert_eq!(unbanned_count(&collect_all_events(&mut manager), peer), 1);
}

/// With peer exchange at capacity, a new inbound identity is a slotless hand-off that is
/// disconnected with peer exchange and temporarily banned once established.
#[tokio::test]
async fn test_admission_hand_off_holds_no_slot() -> Result<(), ConnectionDenied> {
    let mut manager = hand_off_manager();
    assert!(manager.config.peer_exchange_at_capacity());
    let first = PeerId::random();
    let second = PeerId::random();
    let newcomer = PeerId::random();
    let first_id = ConnectionId::new_unchecked(1);
    let second_id = ConnectionId::new_unchecked(2);
    let newcomer_id = ConnectionId::new_unchecked(3);
    let replacement_id = ConnectionId::new_unchecked(4);
    let overflow_id = ConnectionId::new_unchecked(5);
    admit(&mut manager, first_id, first, Endpoint::Listener)?;
    establish(&mut manager, first_id, first);
    admit(&mut manager, second_id, second, Endpoint::Listener)?;
    establish(&mut manager, second_id, second);
    admit(&mut manager, newcomer_id, newcomer, Endpoint::Listener)?;
    assert!(manager.is_hand_off(&newcomer_id));
    close(&mut manager, first_id, first, 0);
    admit(&mut manager, replacement_id, PeerId::random(), Endpoint::Listener)?;
    assert!(!manager.is_hand_off(&replacement_id), "a hand-off must not hold a population slot");
    admit(&mut manager, overflow_id, PeerId::random(), Endpoint::Listener)?;
    assert!(manager.is_hand_off(&overflow_id));
    establish(&mut manager, newcomer_id, newcomer);
    let events = collect_all_events(&mut manager);
    assert!(events
        .iter()
        .any(|event| matches!(event, PeerEvent::DisconnectPeerX(peer, _) if *peer == newcomer)));
    assert!(!events
        .iter()
        .any(|event| matches!(event, PeerEvent::PeerConnected(peer, _) if *peer == newcomer)));
    assert!(!manager.is_connected(&newcomer));
    assert!(manager.temporarily_banned.contains(&newcomer));
    // at capacity with the flag on, only the temporary ban can refuse this identity
    assert!(
        admit(&mut manager, ConnectionId::new_unchecked(6), newcomer, Endpoint::Listener).is_err()
    );
    Ok(())
}

/// Without peer exchange at capacity, a new inbound identity is refused with the typed cause and
/// is not temporarily banned.
#[tokio::test]
async fn test_admission_inbound_capacity_refusal_is_typed() -> Result<(), ConnectionDenied> {
    let mut manager = manager_with_capacity();
    assert!(!manager.config.peer_exchange_at_capacity());
    let limit = manager.config.max_peers();
    (0..limit).try_for_each(|index| {
        admit(
            &mut manager,
            ConnectionId::new_unchecked(index + 1),
            PeerId::random(),
            Endpoint::Listener,
        )
    })?;
    let refused = PeerId::random();
    let refused_id = ConnectionId::new_unchecked(limit + 1);
    assert_eq!(
        capacity_refusal(&mut manager, refused_id, refused, Endpoint::Listener),
        Some(PeerCapacityReached)
    );
    assert!(!manager.is_hand_off(&refused_id));
    assert!(!manager.temporarily_banned.contains(&refused));
    Ok(())
}

/// Peer exchange at capacity never hands off outbound connections: they are refused with the
/// typed cause, while an inbound newcomer at the same population is handed off.
#[tokio::test]
async fn test_admission_outbound_refused_with_hand_off_enabled() -> Result<(), ConnectionDenied> {
    let mut manager = hand_off_manager();
    let limit = manager.config.max_outbound_dialing_peers();
    (0..limit).try_for_each(|index| {
        admit(
            &mut manager,
            ConnectionId::new_unchecked(index + 1),
            PeerId::random(),
            Endpoint::Dialer,
        )
    })?;
    let refused_id = ConnectionId::new_unchecked(limit + 1);
    assert_eq!(
        capacity_refusal(&mut manager, refused_id, PeerId::random(), Endpoint::Dialer),
        Some(PeerCapacityReached)
    );
    assert!(!manager.is_hand_off(&refused_id));
    let inbound_id = ConnectionId::new_unchecked(limit + 2);
    admit(&mut manager, inbound_id, PeerId::random(), Endpoint::Listener)?;
    assert!(manager.is_hand_off(&inbound_id));
    Ok(())
}
