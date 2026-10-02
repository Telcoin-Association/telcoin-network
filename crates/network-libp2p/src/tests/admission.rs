//! Admission matrices and deterministic fallback, renewal, and rotation regressions.

use super::*;
use crate::{common::create_multiaddr, types::NetworkType, AdmissionFallback};
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{ConnectionId, NetworkBehaviour as _},
};
use rand::{rngs::StdRng, SeedableRng as _};
use tn_types::{now, BlsKeypair, NetworkKeypair};

/// One independently generated authenticated identity and its verified record.
struct AdmissionPeer {
    /// BLS identity owning the record.
    bls: BlsPublicKey,
    /// Validated record supplied through the manager's internal cache path.
    info: NetworkInfo,
}

impl AdmissionPeer {
    /// Generate an independent record fixture.
    fn new() -> Self {
        Self {
            bls: *BlsKeypair::generate(&mut StdRng::from_os_rng()).public(),
            info: NetworkInfo {
                pubkey: NetworkKeypair::generate_ed25519().public().into(),
                multiaddrs: vec![create_multiaddr(None)],
                timestamp: now(),
                rpc: None,
            },
        }
    }
    /// Return the transport identity authenticated at establishment.
    fn id(&self) -> PeerId {
        self.info.pubkey.clone().into()
    }
}

/// Resolved admission window with every permitted peer class.
struct AdmissionFixture {
    /// Swarm manager under test.
    manager: PeerManager,
    /// Local BLS identity counted without a remote connection.
    local: BlsPublicKey,
    /// Previous committee peer.
    previous: AdmissionPeer,
    /// Current committee peer.
    current: AdmissionPeer,
    /// Next committee peer.
    next: AdmissionPeer,
    /// Configured bootstrap peer, with no reputation privilege.
    bootstrap: AdmissionPeer,
    /// Explicitly configured trusted peer.
    trusted: AdmissionPeer,
    /// Unrelated authenticated peer.
    ordinary: AdmissionPeer,
}

impl AdmissionFixture {
    /// Build the same policy on a primary or any worker ID.
    fn new(network: NetworkType, mode: AdmissionMode) -> Self {
        let local = *BlsKeypair::generate(&mut StdRng::from_os_rng()).public();
        let mut manager = PeerManager::new(
            PeerId::random(),
            &PeerConfig::default(),
            PeerManagerMetrics::new_for(&network),
        );
        manager.configure_admission(AdmissionConfig::new(mode, Duration::from_secs(300)), local);
        let previous = AdmissionPeer::new();
        let current = AdmissionPeer::new();
        let next = AdmissionPeer::new();
        [&previous, &current, &next].into_iter().for_each(|peer| {
            manager.cache_known_peer(peer.bls, peer.info.clone());
        });
        let bootstrap = AdmissionPeer::new();
        manager.add_bootstrap_peer(bootstrap.bls, bootstrap.info.clone());
        let trusted = AdmissionPeer::new();
        let (reply, _rx) = oneshot::channel();
        manager.add_trusted_peer_and_dial(trusted.bls, trusted.info.clone(), reply);
        let ordinary = AdmissionPeer::new();
        let mut fixture =
            Self { manager, local, previous, current, next, bootstrap, trusted, ordinary };
        fixture.renew(7);
        fixture
    }
    /// Renew the immutable authoritative window.
    fn renew(&mut self, epoch: u64) {
        self.manager.update_committees_at(
            epoch,
            HashSet::from([self.previous.bls]),
            HashSet::from([self.local, self.current.bls]),
            HashSet::from([self.next.bls]),
        );
    }
}

/// Apply both authenticated hooks and the known-identity pending hook to a peer.
fn assert_admission_hooks(manager: &mut PeerManager, peer: PeerId, permitted: bool) {
    let addr = create_multiaddr(None);
    assert_eq!(
        manager
            .handle_established_inbound_connection(
                ConnectionId::new_unchecked(1),
                peer,
                &addr,
                &addr
            )
            .is_ok(),
        permitted,
        "inbound"
    );
    assert_eq!(
        manager
            .handle_established_outbound_connection(
                ConnectionId::new_unchecked(2),
                peer,
                &addr,
                Endpoint::Dialer,
                PortUse::New
            )
            .is_ok(),
        permitted,
        "outbound"
    );
    assert_eq!(
        manager
            .handle_pending_outbound_connection(
                ConnectionId::new_unchecked(3),
                Some(peer),
                &[addr],
                Endpoint::Dialer
            )
            .is_ok(),
        permitted,
        "pending or Kademlia"
    );
}

/// All peer classes receive the same decision on the primary and multiple workers.
#[tokio::test]
async fn admission_peer_class_matrix_every_swarm() {
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(2)].into_iter().for_each(
        |network| {
            let mut fixture = AdmissionFixture::new(network, AdmissionMode::Closed);
            assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Closed);
            let permitted = [
                &fixture.previous,
                &fixture.current,
                &fixture.next,
                &fixture.bootstrap,
                &fixture.trusted,
            ]
            .map(AdmissionPeer::id);
            permitted
                .into_iter()
                .for_each(|peer| assert_admission_hooks(&mut fixture.manager, peer, true));
            fixture.manager.temporarily_banned.insert(fixture.current.id());
            assert_admission_hooks(&mut fixture.manager, fixture.current.id(), false);
            fixture.manager.temporarily_banned.remove(&fixture.current.id());
            let ordinary = fixture.ordinary.id();
            assert_admission_hooks(&mut fixture.manager, ordinary, false);
            assert!(fixture.manager.peers.get_peer(&ordinary).is_none());
            assert!(!fixture.manager.is_connected(&ordinary));
            assert!(!fixture.manager.peer_is_important(&fixture.bootstrap.id()));
            assert!(fixture.manager.admission_is_privileged(&fixture.bootstrap.id()));
            assert!(fixture
                .manager
                .handle_pending_outbound_connection(
                    ConnectionId::new_unchecked(4),
                    None,
                    &[],
                    Endpoint::Dialer
                )
                .is_err());
        },
    );
}

/// A policy installed during an in-flight dial is checked at pending and established hooks.
#[tokio::test]
async fn admission_inflight_and_resumed_dials_recheck_latest_policy() {
    let mut fixture = AdmissionFixture::new(NetworkType::Worker(2), AdmissionMode::Open);
    let ordinary = fixture.ordinary.id();
    fixture.manager.register_dial_attempt(ordinary, None);
    fixture.manager.configure_admission(
        AdmissionConfig::new(AdmissionMode::Closed, Duration::from_secs(300)),
        fixture.local,
    );
    assert_admission_hooks(&mut fixture.manager, ordinary, false);
    assert_admission_hooks(&mut fixture.manager, fixture.current.id(), true);
}

/// Missing, stale, conflicting, and unresolved inputs recover through authoritative renewal.
#[tokio::test(start_paused = true)]
async fn admission_faults_fallback_and_recover() {
    let mut fixture = AdmissionFixture::new(NetworkType::Primary, AdmissionMode::Closed);
    let ordinary = fixture.ordinary.id();
    fixture.manager.invalidate_admission();
    fixture.manager.update_committees(
        HashSet::new(),
        HashSet::from([fixture.ordinary.bls]),
        HashSet::new(),
    );
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Missing));
    assert_admission_hooks(&mut fixture.manager, ordinary, true);
    fixture.renew(7);
    assert!(fixture.manager.peers.is_committee_member(&fixture.current.bls));
    // Compatibility membership replacement prunes old records. Recovery stays in Grace
    // until the retried lookups return verified records for the authoritative window.
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Grace);
    [&fixture.previous, &fixture.current, &fixture.next].into_iter().for_each(|peer| {
        fixture.manager.cache_known_peer(peer.bls, peer.info.clone());
    });
    fixture.manager.update_committees_at(8, HashSet::new(), HashSet::new(), HashSet::new());
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Missing));
    assert_eq!(fixture.manager.admission_status().epoch(), Some(7));
    fixture.renew(7);
    fixture.manager.known_peers.remove(&fixture.previous.bls);
    fixture.manager.events.clear();
    fixture.renew(7);
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Grace);
    assert!(fixture.manager.events.iter().any(|event| matches!(event,
        PeerEvent::MissingAuthorities(keys) if keys.contains(&fixture.previous.bls))));
    fixture.manager.cache_known_peer(fixture.previous.bls, fixture.previous.info.clone());
    tokio::time::advance(Duration::from_secs(300)).await;
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Stale));
    assert_admission_hooks(&mut fixture.manager, ordinary, true);
    fixture.renew(7);
    fixture.renew(6);
    assert_eq!(fixture.manager.admission_status().epoch(), Some(7));
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Stale));
    fixture.renew(7);
    fixture.manager.update_committees_at(
        7,
        HashSet::new(),
        HashSet::from([fixture.local]),
        HashSet::new(),
    );
    assert_eq!(
        fixture.manager.admission_status().fallback(),
        Some(AdmissionFallback::Contradictory)
    );
    assert_admission_hooks(&mut fixture.manager, ordinary, true);
    fixture.renew(7);
    fixture.manager.known_peers.remove(&fixture.next.bls);
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Grace);
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Unresolved));
    assert_admission_hooks(&mut fixture.manager, ordinary, true);
    fixture.manager.cache_known_peer(fixture.next.bls, fixture.next.info.clone());
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Closed);
    assert_admission_hooks(&mut fixture.manager, ordinary, false);
}

/// Quorum counts ignore config stubs and connected peers; ambiguous identity maps fail open.
#[tokio::test]
async fn admission_resolution_is_separate_from_connections_and_grants() {
    let mut fixture = AdmissionFixture::new(NetworkType::Worker(0), AdmissionMode::Closed);
    assert_eq!(fixture.manager.admission_status().resolved_current(), 2);
    assert_eq!(fixture.manager.admission_status().required_current(), 2);
    assert_eq!(fixture.manager.admission_status().connected_current(), 0);
    fixture.manager.stub_records.insert(fixture.current.bls);
    assert_eq!(fixture.manager.admission_status().resolved_current(), 1);
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Grace);
    assert!(!fixture.manager.admission_is_privileged(&fixture.bootstrap.id()));
    fixture.manager.stub_records.remove(&fixture.current.bls);
    let contradictory =
        NetworkInfo { pubkey: fixture.current.info.pubkey.clone(), ..fixture.next.info.clone() };
    fixture.manager.cache_known_peer(fixture.next.bls, contradictory);
    assert_eq!(
        fixture.manager.admission_status().fallback(),
        Some(AdmissionFallback::Contradictory)
    );
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Open);
    fixture.manager.cache_known_peer(fixture.next.bls, fixture.next.info.clone());
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Closed);
}

/// Recovering Closed revokes ordinary live peers while preserving grants under discovery pressure.
#[tokio::test]
async fn admission_live_rotation_and_discovery_pressure() {
    let mut fixture = AdmissionFixture::new(NetworkType::Primary, AdmissionMode::Open);
    let ordinary = fixture.ordinary.id();
    fixture.manager.register_peer_connection(
        &ordinary,
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) },
    );
    fixture.manager.register_peer_connection(
        &fixture.bootstrap.id(),
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) },
    );
    fixture.manager.config.target_num_peers = 0;
    fixture.manager.configure_admission(
        AdmissionConfig::new(AdmissionMode::Closed, Duration::from_secs(300)),
        fixture.local,
    );
    fixture.renew(7);
    assert!(fixture
        .manager
        .events
        .iter()
        .any(|event| matches!(event, PeerEvent::DisconnectPeer(peer) if *peer == ordinary)));
    assert!(!fixture.manager.peer_banned(&ordinary));
    fixture.manager.prune_connected_peers();
    assert!(fixture.manager.is_connected(&fixture.bootstrap.id()));
    fixture.manager.register_peer_connection(
        &fixture.previous.id(),
        ConnectionType::IncomingConnection { multiaddr: create_multiaddr(None) },
    );
    fixture.manager.update_committees_at(
        8,
        HashSet::from([fixture.current.bls]),
        HashSet::from([fixture.local, fixture.next.bls]),
        HashSet::new(),
    );
    assert_admission_hooks(&mut fixture.manager, fixture.previous.id(), false);
    assert_admission_hooks(&mut fixture.manager, fixture.current.id(), true);
    assert_admission_hooks(&mut fixture.manager, fixture.next.id(), true);
}

/// Grace and Open preserve ordinary admission; fallback never grants a policy budget exemption.
#[tokio::test]
async fn admission_compatibility_modes_and_zero_lease() {
    [AdmissionMode::Open, AdmissionMode::Grace].into_iter().for_each(|mode| {
        let mut fixture = AdmissionFixture::new(NetworkType::Primary, mode);
        assert_admission_hooks(&mut fixture.manager, fixture.ordinary.id(), true);
        assert!(!fixture.manager.admission_is_privileged(&fixture.ordinary.id()));
        fixture.manager.temporarily_banned.insert(fixture.ordinary.id());
        assert_admission_hooks(&mut fixture.manager, fixture.ordinary.id(), false);
        let local_peer = fixture.manager.local_peer_id;
        assert_admission_hooks(&mut fixture.manager, local_peer, false);
    });
    let mut fixture = AdmissionFixture::new(NetworkType::Primary, AdmissionMode::Closed);
    fixture.manager.configure_admission(
        AdmissionConfig::new(AdmissionMode::Closed, Duration::ZERO),
        fixture.local,
    );
    assert_eq!(fixture.manager.admission_status().effective(), AdmissionMode::Open);
    assert_eq!(fixture.manager.admission_status().fallback(), Some(AdmissionFallback::Stale));
}
