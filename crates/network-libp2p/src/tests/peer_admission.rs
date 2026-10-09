//! Regression coverage for population admission and independent ban owners (issue #1475).

use super::*;
use crate::types::NetworkType;
use futures::TryStreamExt as _;
use libp2p::{
    core::{transport::PortUse, ConnectedPoint},
    swarm::{
        behaviour::ConnectionEstablished, ConnectionClosed, ConnectionDenied, DialFailure,
        FromSwarm, ListenError, ListenFailure,
    },
};
use std::collections::BTreeMap;
use tn_config::{DaoObserverProfile, TrustedNode};
use tn_types::P2pNode;

/// Build a deployment inventory with primary and sparse worker identities.
fn dao_entry(keys: &KeyConfig) -> TrustedNode {
    let primary = P2pNode {
        network_key: keys.primary_network_public_key(),
        network_address: create_multiaddr(None),
        rpc: None,
    };
    let workers = [0, 7]
        .into_iter()
        .map(|id| {
            (
                id,
                P2pNode {
                    network_key: keys.worker_network_public_key(id),
                    network_address: create_multiaddr(None),
                    rpc: None,
                },
            )
        })
        .collect();
    TrustedNode::new(primary, workers)
}

/// A hub with two DAO reservations and one independent trusted connection.
fn dao_config(observers: &[KeyConfig], trusted: &KeyConfig) -> NetworkConfig {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().target_num_peers = 1;
    config.peer_config_mut().peer_excess_factor = 0.0;
    config.peer_config_mut().priority_peer_excess = 0.0;
    config.set_trusted_nodes(BTreeMap::from([(trusted.primary_public_key(), dao_entry(trusted))]));
    config.set_dao_observers(Some(DaoObserverProfile::new(
        observers.iter().map(|keys| (keys.primary_public_key(), dao_entry(keys))).collect(),
        4,
    )));
    config
}

/// Ordinary and unrelated trusted connections cannot consume either observer's finite allowance.
#[tokio::test]
async fn dao_observers_connect_at_capacity_and_remain_bounded() -> NetworkResult<()> {
    let keys: Vec<_> = (0..3)
        .map(|_| KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng())))
        .collect();
    let trusted = keys.first().ok_or(NetworkError::PeerMissing)?;
    let observers = keys.get(1..).ok_or(NetworkError::PeerMissing)?;
    let config = dao_config(observers, trusted);
    config
        .validate_operator_inventory(&BTreeMap::new(), [0, 7])
        .map_err(|error| NetworkError::ProtocolError(error.to_string()))?;
    [Endpoint::Listener, Endpoint::Dialer].into_iter().try_for_each(|direction| {
        let mut manager = create_test_peer_manager(Some(config.clone()));
        manager.configure_operator_peers(&config, NetworkType::Primary)?;
        let ordinary = PeerId::random();
        admit(&mut manager, ConnectionId::new_unchecked(1), ordinary, direction)
            .map_err(|error| NetworkError::ProtocolError(error.to_string()))?;
        establish(&mut manager, ConnectionId::new_unchecked(1), ordinary);
        assert!(admit(&mut manager, ConnectionId::new_unchecked(2), PeerId::random(), direction)
            .is_err());
        let trusted_peer: PeerId = trusted.primary_network_public_key().into();
        // Fill all remaining non-observer capacity with authenticated privileged identities.
        // Use distinct identities so the composed per-peer limit remains respected.
        let extra =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let extra_peer: PeerId = extra.primary_network_public_key().into();
        manager
            .peers
            .add_trusted_peer(extra.primary_public_key(), extra.primary_network_public_key());
        (2..=16).try_for_each(|id| {
            let peer = if id <= 9 { trusted_peer } else { ordinary };
            admit(&mut manager, ConnectionId::new_unchecked(id), peer, direction)
                .map_err(|error| NetworkError::ProtocolError(error.to_string()))
        })?;
        assert!(
            admit(&mut manager, ConnectionId::new_unchecked(17), extra_peer, direction).is_err()
        );
        observers.iter().enumerate().try_for_each(|(index, observer)| {
            let peer: PeerId = observer.primary_network_public_key().into();
            let start = 100 + index * 10;
            (start..start + 8).try_for_each(|id| {
                admit(&mut manager, ConnectionId::new_unchecked(id), peer, direction)
                    .map_err(|error| NetworkError::ProtocolError(error.to_string()))
            })?;
            assert!(admit(&mut manager, ConnectionId::new_unchecked(start + 8), peer, direction)
                .is_err());
            close(&mut manager, ConnectionId::new_unchecked(start), peer, 0);
            admit(&mut manager, ConnectionId::new_unchecked(start + 9), peer, direction)
                .map_err(|error| NetworkError::ProtocolError(error.to_string()))?;
            manager.process_penalty(peer, Penalty::Load(LoadPenalty::Timeout));
            assert!(!manager.peer_banned(&peer));
            manager.process_penalty(peer, Penalty::Fatal);
            assert!(manager.peer_banned(&peer));
            assert!(admit(&mut manager, ConnectionId::new_unchecked(start + 10), peer, direction)
                .is_err());
            Ok::<_, NetworkError>(())
        })?;
        Ok::<_, NetworkError>(())
    })
}

/// A long outage and committee rotation cannot stop retries or cross worker identities.
#[tokio::test(start_paused = true)]
async fn dao_observer_retries_survive_outage_rotation_and_worker_selection() -> NetworkResult<()> {
    let observer =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let trusted = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let config = dao_config(std::slice::from_ref(&observer), &trusted);
    let config_ref = &config;
    let observer_ref = &observer;
    futures::stream::iter(
        [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(7)]
            .into_iter()
            .map(Ok::<_, NetworkError>),
    )
    .try_for_each(|network| async move {
        let config = config_ref;
        let observer = observer_ref;
        let mut manager = create_test_peer_manager(Some(config.clone()));
        manager.configure_operator_peers(config, network)?;
        let expected: PeerId = match network {
            NetworkType::Primary => observer.primary_network_public_key(),
            NetworkType::Worker(id) => observer.worker_network_public_key(id),
        }
        .into();
        let dials: Vec<_> = std::iter::from_fn(|| manager.next_dial_request()).collect();
        assert!(dials.iter().any(|dial| dial.peer_id == expected));
        let configured_addrs = dials
            .iter()
            .find(|dial| dial.peer_id == expected)
            .map(|dial| dial.multiaddrs.clone())
            .ok_or(NetworkError::PeerMissing)?;
        let mut learned = manager
            .known_peers
            .get(&observer.primary_public_key())
            .cloned()
            .ok_or(NetworkError::PeerMissing)?;
        learned.multiaddrs.clear();
        manager.cache_known_peer(observer.primary_public_key(), learned);
        manager.retry_operator_peers();
        assert!(manager.next_dial_request().is_none());
        manager.register_disconnected(&expected);
        manager.update_committees(HashSet::new(), HashSet::new(), HashSet::new());
        let ordinary = PeerId::random();
        establish(&mut manager, ConnectionId::new_unchecked(1000), ordinary);
        tokio::time::advance(Duration::from_secs(3600)).await;
        manager.heartbeat();
        let dials: Vec<_> = std::iter::from_fn(|| manager.next_dial_request()).collect();
        assert_eq!(dials.iter().filter(|dial| dial.peer_id == expected).count(), 1);
        assert_eq!(
            dials.iter().find(|dial| dial.peer_id == expected).map(|dial| &dial.multiaddrs),
            Some(&configured_addrs)
        );
        manager.heartbeat();
        assert!(manager.next_dial_request().is_none());
        assert_eq!(manager.operator_retries.len(), 2);
        Ok(())
    })
    .await
}

/// Inventory replacement revokes only DAO-derived retention and reservation ownership.
#[tokio::test]
async fn dao_profile_replacement_preserves_unrelated_trust() -> NetworkResult<()> {
    let old = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let replacement =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let trusted = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let mut config = dao_config(std::slice::from_ref(&old), &trusted);
    config.set_dao_observers(Some(DaoObserverProfile::new(
        BTreeMap::from([(replacement.primary_public_key(), dao_entry(&replacement))]),
        4,
    )));
    let mut restarted = create_test_peer_manager(Some(config.clone()));
    restarted.configure_operator_peers(&config, NetworkType::Primary)?;
    assert!(!restarted.dao_peer_ids.contains(&old.primary_network_public_key().into()));
    assert!(restarted.dao_peer_ids.contains(&replacement.primary_network_public_key().into()));
    assert!(restarted.peer_is_important(&trusted.primary_network_public_key().into()));
    restarted.update_committees(HashSet::new(), HashSet::new(), HashSet::new());
    assert!(restarted.peer_is_important(&replacement.primary_network_public_key().into()));
    config.set_dao_observers(None);
    let mut revoked = create_test_peer_manager(Some(config.clone()));
    revoked.configure_operator_peers(&config, NetworkType::Primary)?;
    assert!(revoked.dao_peer_ids.is_empty());
    assert!(!revoked.peer_is_important(&replacement.primary_network_public_key().into()));
    assert!(revoked.peer_is_important(&trusted.primary_network_public_key().into()));
    // Independent operator membership survives removal from the DAO inventory.
    config.set_trusted_nodes(BTreeMap::from([
        (trusted.primary_public_key(), dao_entry(&trusted)),
        (replacement.primary_public_key(), dao_entry(&replacement)),
    ]));
    let mut overlapping = create_test_peer_manager(Some(config.clone()));
    overlapping.configure_operator_peers(&config, NetworkType::Primary)?;
    assert!(overlapping.dao_peer_ids.is_empty());
    assert!(overlapping.peer_is_important(&replacement.primary_network_public_key().into()));
    Ok(())
}

/// Learned records cannot rotate a configured identity or erase its protocol ban.
#[tokio::test]
async fn dao_observer_identity_pin_and_protocol_history_survive_records() -> NetworkResult<()> {
    let observer =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let trusted = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    let config = dao_config(std::slice::from_ref(&observer), &trusted);
    let mut manager = create_test_peer_manager(Some(config.clone()));
    manager.configure_operator_peers(&config, NetworkType::Primary)?;
    let key = observer.primary_public_key();
    manager.cache_known_peer(
        key,
        NetworkInfo {
            pubkey: trusted.primary_network_public_key(),
            multiaddrs: vec![create_multiaddr(None)],
            timestamp: now(),
            rpc: None,
        },
    );
    assert_eq!(
        manager.known_peers.get(&key).map(|info| info.pubkey.clone()),
        Some(observer.primary_network_public_key())
    );
    let peer: PeerId = observer.primary_network_public_key().into();
    let unrelated =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
    manager.cache_known_peer(
        unrelated.primary_public_key(),
        NetworkInfo {
            pubkey: observer.primary_network_public_key(),
            multiaddrs: vec![create_multiaddr(None)],
            timestamp: now(),
            rpc: None,
        },
    );
    assert_eq!(manager.peer_to_bls(&peer), Some(key));
    manager.process_penalty(peer, Penalty::Fatal);
    manager.configure_operator_peers(&config, NetworkType::Primary)?;
    assert!(manager.peer_banned(&peer));
    manager.retry_operator_peers();
    assert!(manager.peer_banned(&peer));
    Ok(())
}

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
