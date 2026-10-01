//! Regressions for identity-scoped admission and collateral address penalties.

use super::{ConnectionType, NetworkInfo, NewConnectionStatus, PeerManager, Penalty};
use crate::{
    metrics::PeerManagerMetrics, peers::pending_inbound::PendingInbound, types::NetworkType,
};
use libp2p::{
    core::{transport::PortUse, Endpoint},
    multiaddr::Protocol,
    swarm::{ConnectionId, FromSwarm, ListenError, ListenFailure, NetworkBehaviour as _},
    Multiaddr, PeerId,
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};
use tn_config::PeerConfig;
use tn_types::{BlsKeypair, BlsPublicKey, NetworkKeypair, NetworkPublicKey};

/// Configured admission sources exercised independently, including every committee slot.
#[derive(Clone, Copy)]
enum AdmissionSource {
    /// Operator-allowlisted hub.
    Operator,
    /// Configured discovery/bootstrap identity.
    Bootstrap,
    /// Validator in the previous committee.
    Previous,
    /// Validator in the current committee.
    Current,
    /// Validator in the next committee.
    Next,
}

/// Build a peer manager without relying on committee-fixture defaults.
fn manager() -> PeerManager {
    PeerManager::new(
        PeerId::random(),
        &PeerConfig::default(),
        PeerManagerMetrics::new_for(&NetworkType::Primary),
    )
}

/// Build a QUIC multiaddr containing only the supplied actual source address.
fn address(ip: IpAddr) -> Multiaddr {
    let protocol = match ip {
        IpAddr::V4(ip) => Protocol::Ip4(ip),
        IpAddr::V6(ip) => Protocol::Ip6(ip),
    };
    Multiaddr::empty().with(protocol).with(Protocol::Udp(9000)).with(Protocol::QuicV1)
}

/// Generate separate BLS and transport identities, with an advertised address that grants no trust.
fn identity(addr: &Multiaddr) -> (BlsPublicKey, NetworkInfo) {
    let bls = *BlsKeypair::generate(&mut StdRng::from_seed([148; 32])).public();
    let pubkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
    (bls, NetworkInfo { pubkey, multiaddrs: vec![addr.clone()], timestamp: 1, rpc: None })
}

/// Record two authenticated offenders so the shared address crosses the collateral threshold.
fn ban_co_tenants(manager: &mut PeerManager, addr: &Multiaddr) -> [PeerId; 2] {
    std::array::from_fn(|_| {
        let peer = PeerId::random();
        assert!(manager.register_peer_connection(
            &peer,
            ConnectionType::IncomingConnection { multiaddr: addr.clone() }
        ));
        manager.process_penalty(peer, Penalty::Fatal);
        manager.register_disconnected(&peer);
        assert!(manager.peer_banned(&peer));
        peer
    })
}

/// Every configured trust basis permits reconnects behind an unrelated IPv4 or IPv6 offender.
#[tokio::test]
async fn admitted_reconnects_ignore_collateral_bans_but_keep_protocol_bans() {
    [
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
    ]
    .into_iter()
    .for_each(|ip| {
        [
            AdmissionSource::Operator,
            AdmissionSource::Bootstrap,
            AdmissionSource::Previous,
            AdmissionSource::Current,
            AdmissionSource::Next,
        ]
        .into_iter()
        .for_each(|basis| {
            let mut manager = manager();
            let addr = address(ip);
            let (bls, info) = identity(&addr);
            let peer: PeerId = info.pubkey.clone().into();
            manager.peers.upsert_peer(bls, info.pubkey.clone(), info.multiaddrs.clone());
            match basis {
                AdmissionSource::Operator => {
                    manager.peers.add_trusted_peer(bls, info.pubkey.clone())
                }
                AdmissionSource::Bootstrap => manager.add_known_peer(bls, info),
                AdmissionSource::Previous => {
                    manager.update_committees(HashSet::from([bls]), HashSet::new(), HashSet::new())
                }
                AdmissionSource::Current => {
                    manager.update_committees(HashSet::new(), HashSet::from([bls]), HashSet::new())
                }
                AdmissionSource::Next => {
                    manager.update_committees(HashSet::new(), HashSet::new(), HashSet::from([bls]))
                }
            }
            assert!(manager.register_peer_connection(
                &peer,
                ConnectionType::IncomingConnection { multiaddr: addr.clone() }
            ));
            manager.register_disconnected(&peer);
            ban_co_tenants(&mut manager, &addr);
            assert!(manager.is_ip_banned(&ip));
            assert!(!manager.peer_banned(&peer));
            assert!(manager.can_dial(&peer));
            assert!(manager.has_valid_peer_ips(&peer, std::slice::from_ref(&addr)));
            (0..2).for_each(|reconnect| {
                let id = ConnectionId::new_unchecked(10 + reconnect);
                assert!(manager.handle_pending_inbound_connection(id, &addr, &addr).is_ok());
                assert!(manager
                    .handle_established_inbound_connection(id, peer, &addr, &addr)
                    .is_ok());
                assert!(manager
                    .handle_established_outbound_connection(
                        id,
                        peer,
                        &addr,
                        Endpoint::Dialer,
                        PortUse::Reuse
                    )
                    .is_ok());
                assert!(manager.register_peer_connection(
                    &peer,
                    ConnectionType::IncomingConnection { multiaddr: addr.clone() }
                ));
                manager.register_disconnected(&peer);
            });
            manager.process_penalty(peer, Penalty::Fatal);
            manager.register_disconnected(&peer);
            assert!(manager.peers.peer_banned(&peer));
            assert!(manager.peer_banned(&peer));
            assert!(manager
                .handle_established_inbound_connection(
                    ConnectionId::new_unchecked(20),
                    peer,
                    &addr,
                    &addr
                )
                .is_err());
            assert!(manager
                .handle_established_outbound_connection(
                    ConnectionId::new_unchecked(21),
                    peer,
                    &addr,
                    Endpoint::Dialer,
                    PortUse::Reuse
                )
                .is_err());
        });
    });
}

/// A /p2p claim of a trusted identity grants neither policy privileges nor a larger pending quota.
#[tokio::test]
async fn claimed_trusted_address_grants_no_privileges() {
    let mut manager = manager();
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));
    let addr = address(ip);
    let (bls, info) = identity(&addr);
    let trusted: PeerId = info.pubkey.clone().into();
    manager.peers.add_trusted_peer(bls, info.pubkey);
    ban_co_tenants(&mut manager, &addr);
    let claimant = PeerId::random();
    let claimed = addr.clone().with(Protocol::P2p(trusted));
    assert!(!manager.peer_policy(&claimant).exempts_collateral_bans());
    manager.pending_inbound = PendingInbound::new(1, 1, 2);
    assert!(manager
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(1), &addr, &claimed)
        .is_ok());
    assert!(manager
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(2), &addr, &claimed)
        .is_err());
    assert!(manager
        .handle_established_inbound_connection(
            ConnectionId::new_unchecked(1),
            claimant,
            &addr,
            &claimed
        )
        .is_err());
    assert!(manager
        .handle_established_outbound_connection(
            ConnectionId::new_unchecked(3),
            claimant,
            &claimed,
            Endpoint::Dialer,
            PortUse::Reuse
        )
        .is_err());
    assert!(manager
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(4), &addr, &addr)
        .is_ok());
}

/// Rotation changes only the exemption; shared ban contributions and moved identities stay valid.
#[tokio::test]
async fn rotation_and_address_changes_preserve_ban_accounting() {
    let mut manager = manager();
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 3));
    let addr = address(ip);
    let moved = address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 4)));
    let (bls, info) = identity(&addr);
    let peer: PeerId = info.pubkey.clone().into();
    manager.peers.upsert_peer(bls, info.pubkey.clone(), info.multiaddrs.clone());
    assert!(manager.register_peer_connection(
        &peer,
        ConnectionType::IncomingConnection { multiaddr: addr.clone() }
    ));
    manager.register_disconnected(&peer);
    let offenders = ban_co_tenants(&mut manager, &addr);
    assert!(manager.is_ip_banned(&ip));
    manager.update_committees(HashSet::new(), HashSet::from([bls]), HashSet::new());
    assert!(manager.has_valid_peer_ips(&peer, std::slice::from_ref(&addr)));
    assert!(manager.is_ip_banned(&ip));
    manager.update_committees(HashSet::new(), HashSet::new(), HashSet::new());
    assert!(!manager.has_valid_peer_ips(&peer, std::slice::from_ref(&addr)));
    assert!(manager
        .handle_established_inbound_connection(ConnectionId::new_unchecked(1), peer, &moved, &moved)
        .is_ok());
    assert!(manager
        .handle_established_outbound_connection(
            ConnectionId::new_unchecked(2),
            peer,
            &moved,
            Endpoint::Dialer,
            PortUse::Reuse
        )
        .is_ok());
    assert!(manager.is_ip_banned(&ip));
    offenders.iter().for_each(|offender| {
        manager.peers.update_connection_status(offender, NewConnectionStatus::Unbanned);
    });
    assert!(!manager.is_ip_banned(&ip));
    ban_co_tenants(&mut manager, &addr);
    assert!(manager.is_ip_banned(&ip));
    manager.peers.add_trusted_peer(bls, info.pubkey);
    assert!(manager.has_valid_peer_ips(&peer, std::slice::from_ref(&addr)));
    assert!(manager.is_ip_banned(&ip));
}

/// Failed or denied handshakes free exactly their pending slots, including duplicate failures.
#[tokio::test]
async fn pending_slots_release_on_listen_failure_and_authentication() {
    let mut manager = manager();
    manager.pending_inbound = PendingInbound::new(1, 1, 2);
    let addr = address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)));
    let id = ConnectionId::new_unchecked(1);
    assert!(manager.handle_pending_inbound_connection(id, &addr, &addr).is_ok());
    assert!(manager
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(2), &addr, &addr)
        .is_err());
    manager.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
        local_addr: &addr,
        send_back_addr: &addr,
        error: &ListenError::Aborted,
        connection_id: id,
        peer_id: None,
    }));
    manager.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
        local_addr: &addr,
        send_back_addr: &addr,
        error: &ListenError::Aborted,
        connection_id: id,
        peer_id: None,
    }));
    let next = ConnectionId::new_unchecked(3);
    assert!(manager.handle_pending_inbound_connection(next, &addr, &addr).is_ok());
    assert!(manager
        .handle_established_inbound_connection(next, PeerId::random(), &addr, &addr)
        .is_ok());
    assert!(manager
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(4), &addr, &addr)
        .is_ok());
}
