//! Closed discovery and admission regression tests using the production peer behavior.

use super::*;
use crate::types::NetworkType;
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{behaviour::ConnectionEstablished, ConnectionId, FromSwarm, NetworkBehaviour, ToSwarm},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tn_types::{now, BlsKeypair, NetworkKeypair};

/// Construct a peer manager for one independently identified primary or worker swarm.
fn manager(role: &NetworkType) -> PeerManager {
    PeerManager::new(PeerId::random(), &PeerConfig::default(), PeerManagerMetrics::new_for(role))
}

/// Create a peer record with a valid dial address without initiating network I/O.
fn record() -> eyre::Result<(BlsPublicKey, NetworkInfo)> {
    let key = BlsKeypair::generate(&mut rand::rng()).public().to_owned();
    let info = NetworkInfo {
        pubkey: NetworkKeypair::generate_ed25519().public().into(),
        multiaddrs: vec!["/ip4/8.8.8.8/udp/3000/quic-v1".parse()?],
        timestamp: now(),
        rpc: None,
    };
    Ok((key, info))
}

/// Closed consumes neither Kademlia contacts nor peer exchange and emits no discovery heartbeat.
#[tokio::test]
async fn closed_heartbeat_and_public_inputs_are_quiet() -> eyre::Result<()> {
    let (key, info) = record()?;
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)].iter().for_each(
        |role| {
            let mut peers = manager(role);
            peers.set_network_mode(NetworkMode::Closed);
            peers.process_peers_for_discovery(vec![PeerInfo {
                peer_id: info.pubkey.clone().into(),
                addrs: info.multiaddrs.clone(),
            }]);
            peers.process_peer_exchange(
                HashMap::from([(
                    key,
                    (info.pubkey.clone(), info.multiaddrs.clone().into_iter().collect()),
                )])
                .into(),
            );
            peers.heartbeat();
            assert!(peers.discovery_peers.is_empty());
            assert!(peers.poll_events().is_none());
            assert!(peers.next_dial_request().is_none());
        },
    );
    Ok(())
}

/// Closure cancels queued discovery and completes denied dial callers before acknowledging it.
#[tokio::test]
async fn closed_discards_queued_public_work() -> eyre::Result<()> {
    let mut peers = manager(&NetworkType::Primary);
    let (_, info) = record()?;
    let peer = info.pubkey.clone().into();
    peers.process_peers_for_discovery(vec![PeerInfo {
        peer_id: peer,
        addrs: info.multiaddrs.clone(),
    }]);
    peers.events.push_back(PeerEvent::Discovery);
    let (reply, mut ack) = oneshot::channel();
    peers.dial_peer(peer, info.multiaddrs, Some(reply));
    peers.set_network_mode(NetworkMode::Closed);
    assert!(peers.discovery_peers.is_empty());
    assert!(peers.poll_events().is_none());
    assert!(peers.next_dial_request().is_none());
    assert!(ack.try_recv()?.is_err());
    peers.set_network_mode(NetworkMode::Grace);
    assert!(peers.discovery_peers.is_empty());
    assert!(peers.next_dial_request().is_none());
    assert!(matches!(peers.poll_events(), Some(PeerEvent::Discovery)));
    Ok(())
}

/// Count actual task wakeups during policy recovery.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl std::task::Wake for WakeCount {
    /// Record a wake request from the behavior.
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Reopening an idle Closed swarm wakes it and starts fresh discovery in both recovery modes.
#[tokio::test]
async fn closed_recovery_wakes_and_restores_discovery() -> eyre::Result<()> {
    let (_, info) = record()?;
    [NetworkMode::Grace, NetworkMode::Open].into_iter().for_each(|mode| {
        let mut peers = manager(&NetworkType::Primary);
        peers.set_network_mode(NetworkMode::Closed);
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(peers.poll(&mut cx).is_pending());
        peers.set_network_mode(mode);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            peers.poll(&mut cx),
            std::task::Poll::Ready(ToSwarm::GenerateEvent(PeerEvent::Discovery))
        ));
        peers.process_peers_for_discovery(vec![PeerInfo {
            peer_id: info.pubkey.clone().into(),
            addrs: info.multiaddrs.clone(),
        }]);
        peers.discovery_heartbeat();
        assert!(peers.next_dial_request().is_some());
    });
    Ok(())
}

/// The same known-identity predicate protects both directions and all committee slots.
#[tokio::test]
async fn closed_authorizes_committee_and_configured_peers_on_every_swarm() -> eyre::Result<()> {
    let previous = record()?;
    let current = record()?;
    let next = record()?;
    let bootstrap = record()?;
    let trusted = record()?;
    let explicit = record()?;
    let outsider = record()?;
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)].iter().try_for_each(
        |role| -> eyre::Result<()> {
            let mut peers = manager(role);
            peers.update_committees([previous.0].into(), [current.0].into(), [next.0].into());
            [&previous, &current, &next]
                .into_iter()
                .for_each(|(key, info)| peers.add_discovered_peer(*key, info.clone()));
            peers.add_bootstrap_peer(bootstrap.0, bootstrap.1.clone());
            let (reply, _ack) = oneshot::channel();
            peers.add_trusted_peer_and_dial(trusted.0, trusted.1.clone(), reply);
            peers.add_known_peer(explicit.0, explicit.1.clone());
            peers.set_network_mode(NetworkMode::Closed);
            [&previous, &current, &next, &bootstrap, &trusted, &explicit]
                .into_iter()
                .try_for_each(|(_, info)| -> eyre::Result<()> {
                    let peer = info.pubkey.clone().into();
                    let addr = info
                        .multiaddrs
                        .first()
                        .ok_or_else(|| eyre::eyre!("missing test address"))?;
                    assert!(peers
                        .handle_pending_outbound_connection(
                            ConnectionId::new_unchecked(1),
                            Some(peer),
                            &[],
                            Endpoint::Dialer
                        )
                        .is_ok());
                    assert!(peers
                        .handle_established_outbound_connection(
                            ConnectionId::new_unchecked(1),
                            peer,
                            addr,
                            Endpoint::Dialer,
                            PortUse::New
                        )
                        .is_ok());
                    assert!(peers
                        .handle_established_inbound_connection(
                            ConnectionId::new_unchecked(2),
                            peer,
                            addr,
                            addr
                        )
                        .is_ok());
                    Ok(())
                })?;
            let peer = outsider.1.pubkey.clone().into();
            let addr =
                outsider.1.multiaddrs.first().ok_or_else(|| eyre::eyre!("missing test address"))?;
            assert!(peers
                .handle_pending_outbound_connection(
                    ConnectionId::new_unchecked(3),
                    None,
                    &[],
                    Endpoint::Dialer
                )
                .is_err());
            assert!(peers
                .handle_pending_outbound_connection(
                    ConnectionId::new_unchecked(3),
                    Some(peer),
                    &[],
                    Endpoint::Dialer
                )
                .is_err());
            assert!(peers
                .handle_established_outbound_connection(
                    ConnectionId::new_unchecked(3),
                    peer,
                    addr,
                    Endpoint::Dialer,
                    PortUse::New
                )
                .is_err());
            assert!(peers
                .handle_established_inbound_connection(
                    ConnectionId::new_unchecked(4),
                    peer,
                    addr,
                    addr
                )
                .is_err());
            assert!(!peers.is_connected(&peer));
            Ok(())
        },
    )?;
    Ok(())
}

/// Configured reconnects retain admission even when ordinary public discovery targets are full.
#[tokio::test]
async fn closed_configured_reconnect_is_not_rejected_by_public_targets() -> eyre::Result<()> {
    let config = PeerConfig { target_num_peers: 1, ..PeerConfig::default() };
    let mut peers = PeerManager::new(
        PeerId::random(),
        &config,
        PeerManagerMetrics::new_for(&NetworkType::Primary),
    );
    let (key, info) = record()?;
    let addr = info.multiaddrs.first().ok_or_else(|| eyre::eyre!("missing test address"))?;
    (0..config.max_peers()).for_each(|_| {
        assert!(peers.register_peer_connection(
            &PeerId::random(),
            ConnectionType::IncomingConnection { multiaddr: addr.clone() }
        ));
    });
    peers.add_bootstrap_peer(key, info.clone());
    peers.set_network_mode(NetworkMode::Closed);
    let peer = info.pubkey.clone().into();
    let endpoint =
        ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr.clone() };
    assert!(peers.peer_limit_reached(&endpoint));
    peers.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id: peer,
        connection_id: ConnectionId::new_unchecked(1),
        endpoint: &endpoint,
        failed_addresses: &[],
        other_established: 0,
    }));
    assert!(peers.is_connected(&peer));
    assert!(matches!(peers.poll_events(), Some(PeerEvent::PeerConnected(id, _)) if id == peer));
    Ok(())
}

/// A dial registered while Open cannot bypass closure at either outbound hook.
#[tokio::test]
async fn closed_rechecks_inflight_dials() -> eyre::Result<()> {
    let mut peers = manager(&NetworkType::Primary);
    let (_, info) = record()?;
    let peer = info.pubkey.clone().into();
    let addr = info.multiaddrs.first().ok_or_else(|| eyre::eyre!("missing test address"))?;
    peers.register_dial_attempt(peer, None);
    peers.set_network_mode(NetworkMode::Closed);
    assert!(peers
        .handle_pending_outbound_connection(
            ConnectionId::new_unchecked(1),
            Some(peer),
            &[],
            Endpoint::Dialer
        )
        .is_err());
    assert!(peers
        .handle_established_outbound_connection(
            ConnectionId::new_unchecked(1),
            peer,
            addr,
            Endpoint::Dialer,
            PortUse::New
        )
        .is_err());
    assert!(!peers.is_connected(&peer));
    Ok(())
}

/// Committee refresh requests and configured reconnect dials survive closure.
#[tokio::test]
async fn closed_preserves_record_refresh_and_configured_reconnects() -> eyre::Result<()> {
    let mut peers = manager(&NetworkType::Primary);
    let (committee, _) = record()?;
    let (bootstrap, info) = record()?;
    let peer = info.pubkey.clone().into();
    peers.update_committees(HashSet::new(), [committee].into(), HashSet::new());
    peers.add_bootstrap_peer(bootstrap, info.clone());
    peers.dial_peer(peer, info.multiaddrs.clone(), None);
    peers.set_network_mode(NetworkMode::Closed);
    assert_eq!(peers.next_dial_request().map(|request| request.peer_id), Some(peer));
    assert!(
        matches!(peers.poll_events(), Some(PeerEvent::MissingAuthorities(keys)) if keys.contains(&committee))
    );
    assert!(peers.record_query_authorized(&committee));
    assert!(peers.record_query_authorized(&bootstrap));
    assert!(!peers.record_query_authorized(&record()?.0));
    peers.register_disconnected(&peer);
    peers.dial_peer(peer, info.multiaddrs, None);
    assert_eq!(peers.next_dial_request().map(|request| request.peer_id), Some(peer));
    Ok(())
}

/// A queued committee dial is rechecked after rotation rather than replayed under stale trust.
#[tokio::test]
async fn closed_discards_rotated_out_queued_dials() -> eyre::Result<()> {
    let mut peers = manager(&NetworkType::Primary);
    let (key, info) = record()?;
    let peer = info.pubkey.clone().into();
    peers.update_committees(HashSet::new(), [key].into(), HashSet::new());
    peers.add_discovered_peer(key, info.clone());
    peers.set_network_mode(NetworkMode::Closed);
    let (reply, mut ack) = oneshot::channel();
    peers.dial_peer(peer, info.multiaddrs, Some(reply));
    peers.update_committees(HashSet::new(), HashSet::new(), HashSet::new());
    assert!(peers.next_dial_request().is_none());
    assert!(ack.try_recv()?.is_err());
    Ok(())
}
