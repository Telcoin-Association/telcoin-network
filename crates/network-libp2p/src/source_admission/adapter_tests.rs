//! Provenance and lease ownership using the pinned libp2p lifecycle events.

use super::*;
use libp2p::{
    core::ConnectedPoint,
    swarm::{
        ConnectionClosed, ConnectionDenied, DialError, DialFailure, ListenError, ListenFailure,
    },
};
use serde::Deserialize as _;
use std::net::{Ipv4Addr, Ipv6Addr};

/// Synthetic limits for adapter tests, with every configuration field explicit.
fn budget(connections: u64) -> Result<SourceAdmissionBudget, AdmissionError> {
    let fields = [
        ("max_connections", connections),
        ("max_connections_per_peer", connections),
        ("max_connections_per_address", connections),
        ("max_connections_per_prefix", connections),
        ("max_sources", connections),
        ("ipv4_prefix_length", 24),
        ("ipv6_prefix_length", 64),
    ];
    let config = SourceAdmissionConfig::deserialize(serde::de::value::MapDeserializer::<
        _,
        serde::de::value::Error,
    >::new(fields.into_iter()))
    .map_err(|_| AdmissionError::InvalidLimits)?;
    SourceAdmissionBudget::new(&config)
}

/// Direct observed QUIC endpoint, independent of advertised peer addresses.
fn endpoint(host: u8) -> Multiaddr {
    Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::new(192, 0, 2, host)))
        .with(Protocol::Udp(9000))
        .with(Protocol::QuicV1)
}

/// Construct one swarm's lease owner using the shared process instance.
fn swarm(budget: &SourceAdmissionBudget) -> SourceConnections {
    let mut connections = SourceConnections::default();
    connections.set_budget(Some(budget.clone()));
    connections
}

/// Reject advertised, relay, and non-QUIC address shapes at the established boundary.
#[test]
fn only_direct_observed_quic_endpoints_are_accounted() -> Result<(), AdmissionError> {
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    assert_eq!(observed_ip(&endpoint(1))?, ip);
    assert_eq!(observed_ip(&endpoint(1).with(Protocol::P2p(PeerId::random())))?, ip);
    let v6 = Multiaddr::empty()
        .with(Protocol::Ip6(Ipv6Addr::LOCALHOST))
        .with(Protocol::Udp(9000))
        .with(Protocol::QuicV1);
    assert_eq!(observed_ip(&v6)?, IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(
        observed_ip(&endpoint(1).with(Protocol::P2pCircuit)),
        Err(AdmissionError::UnsupportedAddress)
    );
    assert_eq!(
        observed_ip(&Multiaddr::empty().with(Protocol::Ip4(Ipv4Addr::LOCALHOST))),
        Err(AdmissionError::UnsupportedAddress)
    );
    assert_eq!(
        observed_ip(
            &Multiaddr::empty()
                .with(Protocol::Dns4("example.invalid".into()))
                .with(Protocol::Udp(9000))
                .with(Protocol::QuicV1)
        ),
        Err(AdmissionError::UnsupportedAddress)
    );
    Ok(())
}

/// Absent source configuration leaves admission disabled and retains no leases.
#[test]
fn disabled_admission_keeps_no_accounting_state() -> Result<(), AdmissionError> {
    let mut connections = SourceConnections::default();
    connections.reserve(ConnectionId::new_unchecked(1), PeerId::random(), &Multiaddr::empty())?;
    assert!(connections.leases.is_empty());
    Ok(())
}

/// A later inbound behaviour's denial releases exactly its reservation.
#[test]
fn inbound_denial_and_prevalidation_failure_release_only_owned_leases() -> Result<(), AdmissionError>
{
    let budget = budget(1)?;
    let mut primary = swarm(&budget);
    let mut worker = swarm(&budget);
    let peer = PeerId::random();
    let connection = ConnectionId::new_unchecked(1);
    let local_addr = endpoint(9);
    let send_back_addr = endpoint(1);
    let error = ListenError::Denied { cause: ConnectionDenied::new(AdmissionError::ProcessFull) };
    primary.on_swarm_event(&FromSwarm::ListenFailure(ListenFailure {
        local_addr: &local_addr,
        send_back_addr: &send_back_addr,
        error: &error,
        connection_id: connection,
        peer_id: None,
    }));
    primary.reserve(connection, peer, &send_back_addr)?;
    assert_eq!(
        primary.reserve(connection, peer, &send_back_addr),
        Err(AdmissionError::DuplicateConnection)
    );
    assert_eq!(
        worker.reserve(ConnectionId::new_unchecked(2), PeerId::random(), &endpoint(2)),
        Err(AdmissionError::ProcessFull)
    );
    primary.on_swarm_event(&FromSwarm::ListenFailure(ListenFailure {
        local_addr: &local_addr,
        send_back_addr: &send_back_addr,
        error: &error,
        connection_id: ConnectionId::new_unchecked(3),
        peer_id: None,
    }));
    assert_eq!(
        worker.reserve(ConnectionId::new_unchecked(2), PeerId::random(), &endpoint(2)),
        Err(AdmissionError::ProcessFull)
    );
    primary.on_swarm_event(&FromSwarm::ListenFailure(ListenFailure {
        local_addr: &local_addr,
        send_back_addr: &send_back_addr,
        error: &error,
        connection_id: connection,
        peer_id: Some(peer),
    }));
    worker.reserve(ConnectionId::new_unchecked(2), PeerId::random(), &endpoint(2))?;
    Ok(())
}

/// A later outbound behaviour's denial and swarm shutdown release reservations.
#[test]
fn outbound_denial_and_swarm_drop_release_occupancy() -> Result<(), AdmissionError> {
    let budget = budget(1)?;
    let mut primary = swarm(&budget);
    let mut worker = swarm(&budget);
    let peer = PeerId::random();
    let connection = ConnectionId::new_unchecked(1);
    primary.reserve(connection, peer, &endpoint(1))?;
    let error = DialError::Denied { cause: ConnectionDenied::new(AdmissionError::ProcessFull) };
    primary.on_swarm_event(&FromSwarm::DialFailure(DialFailure {
        peer_id: Some(peer),
        error: &error,
        connection_id: connection,
    }));
    worker.reserve(connection, peer, &endpoint(1))?;
    drop(worker);
    primary.reserve(connection, peer, &endpoint(1))?;
    Ok(())
}

/// Closing one connection releases its lease while another peer connection remains.
#[test]
fn closing_one_connection_does_not_require_peer_disconnect() -> Result<(), AdmissionError> {
    let budget = budget(2)?;
    let mut primary = swarm(&budget);
    let mut worker = swarm(&budget);
    let peer = PeerId::random();
    let first = ConnectionId::new_unchecked(1);
    primary.reserve(first, peer, &endpoint(1))?;
    primary.reserve(ConnectionId::new_unchecked(2), peer, &endpoint(1))?;
    let endpoint =
        ConnectedPoint::Listener { local_addr: endpoint(9), send_back_addr: endpoint(1) };
    primary.on_swarm_event(&FromSwarm::ConnectionClosed(ConnectionClosed {
        peer_id: peer,
        connection_id: first,
        endpoint: &endpoint,
        cause: None,
        remaining_established: 1,
    }));
    worker.reserve(first, PeerId::random(), endpoint.get_remote_address())?;
    assert_eq!(
        worker.reserve(
            ConnectionId::new_unchecked(3),
            PeerId::random(),
            endpoint.get_remote_address()
        ),
        Err(AdmissionError::ProcessFull)
    );
    Ok(())
}
