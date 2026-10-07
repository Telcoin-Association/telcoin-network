//! Deterministic lifecycle and admission regressions for the carried accounting patch.

use libp2p_identity::ParseError;
use libp2p_swarm::{DialError, ListenError};

use super::*;

/// Result of constructing the deterministic peer identities used by a test.
type TestResult = Result<(), ParseError>;

/// Construct a distinct identity multihash without randomness or key generation.
fn peer(identity: u8) -> Result<PeerId, ParseError> {
    PeerId::from_bytes(&[0, 1, identity])
}

/// Construct an endpoint without opening a transport or depending on wall-clock time.
fn endpoint(role: Endpoint) -> ConnectedPoint {
    match role {
        Endpoint::Listener => ConnectedPoint::Listener {
            local_addr: Multiaddr::empty(),
            send_back_addr: Multiaddr::empty(),
        },
        Endpoint::Dialer => ConnectedPoint::Dialer {
            address: Multiaddr::empty(),
            role_override: Endpoint::Dialer,
            port_use: PortUse::Reuse,
        },
    }
}

/// Exercise the admission callback that precedes an established-connection notification.
fn admit(
    behaviour: &mut Behaviour,
    peer: PeerId,
    connection: ConnectionId,
    endpoint: &ConnectedPoint,
) -> Result<dummy::ConnectionHandler, ConnectionDenied> {
    match endpoint {
        ConnectedPoint::Listener { local_addr, send_back_addr } => behaviour
            .handle_established_inbound_connection(connection, peer, local_addr, send_back_addr),
        ConnectedPoint::Dialer { address, role_override, port_use } => behaviour
            .handle_established_outbound_connection(
                connection,
                peer,
                address,
                *role_override,
                *port_use,
            ),
    }
}

/// Admit and notify the behaviour of a connection in the same order as the swarm.
fn establish(
    behaviour: &mut Behaviour,
    peer: PeerId,
    connection: ConnectionId,
    endpoint: &ConnectedPoint,
) {
    assert!(admit(behaviour, peer, connection, endpoint).is_ok());
    let other_established = behaviour.established_per_peer.get(&peer).map_or(0, HashSet::len);
    behaviour.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id: peer,
        connection_id: connection,
        endpoint,
        failed_addresses: &[],
        other_established,
    }));
}

/// Notify closure, including duplicate or unknown IDs, without inventing accounting state.
fn close(
    behaviour: &mut Behaviour,
    peer: PeerId,
    connection: ConnectionId,
    endpoint: &ConnectedPoint,
) {
    let remaining_established = behaviour
        .established_per_peer
        .get(&peer)
        .map_or(0, |connections| connections.iter().filter(|id| **id != connection).count());
    behaviour.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
        peer_id: peer,
        connection_id: connection,
        endpoint,
        cause: None,
        remaining_established,
    }));
}

/// Assert that all accounting collections agree and no peer entry is empty.
fn assert_counts(behaviour: &Behaviour, inbound: usize, outbound: usize, peers: usize) {
    assert_eq!(behaviour.established_inbound_connections.len(), inbound);
    assert_eq!(behaviour.established_outbound_connections.len(), outbound);
    assert_eq!(behaviour.established_per_peer.len(), peers);
    assert_eq!(
        behaviour.established_per_peer.values().map(HashSet::len).sum::<usize>(),
        inbound + outbound,
    );
    assert!(behaviour.established_per_peer.values().all(|connections| !connections.is_empty()));
}

/// Assert which bound rejected a connection, rather than accepting an unrelated denial.
fn assert_denied<T>(result: Result<T, ConnectionDenied>, kind: Kind) {
    assert!(result.err().and_then(|denied| denied.downcast::<Exceeded>().ok()).is_some_and(
        |exceeded| { std::mem::discriminant(&exceeded.kind) == std::mem::discriminant(&kind) }
    ));
}

/// Historical identities return to the active-peer baseline with either per-peer configuration.
#[test]
fn disconnected_identities_release_accounting() -> TestResult {
    [Some(2), None].into_iter().try_for_each(|per_peer_limit| {
        let mut behaviour = Behaviour::new(
            ConnectionLimits::default().with_max_established_per_peer(per_peer_limit),
        );
        let active_peer = peer(0)?;
        let active_connection = ConnectionId::new_unchecked(0);
        let inbound = endpoint(Endpoint::Listener);
        establish(&mut behaviour, active_peer, active_connection, &inbound);
        (1..=64).try_for_each(|identity| -> TestResult {
            let transient_peer = peer(identity)?;
            let connection = ConnectionId::new_unchecked(usize::from(identity));
            let transient_endpoint =
                endpoint(if identity % 2 == 0 { Endpoint::Listener } else { Endpoint::Dialer });
            establish(&mut behaviour, transient_peer, connection, &transient_endpoint);
            assert_eq!(behaviour.established_per_peer.len(), 2);
            close(&mut behaviour, transient_peer, connection, &transient_endpoint);
            assert_counts(&behaviour, 1, 0, 1);
            assert!(behaviour.established_per_peer.contains_key(&active_peer));
            Ok(())
        })?;
        close(&mut behaviour, active_peer, active_connection, &inbound);
        assert_counts(&behaviour, 0, 0, 0);
        Ok(())
    })
}

/// A mixed-direction peer remains accounted until its last connection closes and can reconnect.
#[test]
fn multiple_connections_retain_only_active_peer_entries() -> TestResult {
    [Some(2), None].into_iter().try_for_each(|per_peer_limit| {
        let mut behaviour = Behaviour::new(
            ConnectionLimits::default().with_max_established_per_peer(per_peer_limit),
        );
        let peer = peer(1)?;
        let inbound = endpoint(Endpoint::Listener);
        let outbound = endpoint(Endpoint::Dialer);
        let first = ConnectionId::new_unchecked(1);
        let second = ConnectionId::new_unchecked(2);
        establish(&mut behaviour, peer, first, &inbound);
        establish(&mut behaviour, peer, second, &outbound);
        assert_counts(&behaviour, 1, 1, 1);
        close(&mut behaviour, peer, first, &inbound);
        assert_counts(&behaviour, 0, 1, 1);
        assert!(behaviour.established_per_peer.get(&peer).is_some_and(|ids| ids.contains(&second)));
        close(&mut behaviour, peer, first, &inbound);
        assert_counts(&behaviour, 0, 1, 1);
        close(&mut behaviour, peer, second, &outbound);
        assert_counts(&behaviour, 0, 0, 0);
        close(&mut behaviour, peer, second, &outbound);
        assert_counts(&behaviour, 0, 0, 0);
        let reconnect = ConnectionId::new_unchecked(3);
        establish(&mut behaviour, peer, reconnect, &outbound);
        assert_counts(&behaviour, 0, 1, 1);
        close(&mut behaviour, peer, reconnect, &outbound);
        assert_counts(&behaviour, 0, 0, 0);
        Ok(())
    })
}

/// Rejections, failed handshakes and unknown closures leave existing counters intact.
#[test]
fn rejected_failed_and_unknown_connections_do_not_add_peer_entries() -> TestResult {
    let mut behaviour = Behaviour::new(
        ConnectionLimits::default()
            .with_max_pending_incoming(Some(1))
            .with_max_pending_outgoing(Some(1))
            .with_max_established_incoming(Some(1))
            .with_max_established_per_peer(Some(1)),
    );
    let active_peer = peer(1)?;
    let unknown_peer = peer(2)?;
    let inbound = endpoint(Endpoint::Listener);
    let outbound = endpoint(Endpoint::Dialer);
    let active = ConnectionId::new_unchecked(1);
    let rejected = ConnectionId::new_unchecked(2);
    establish(&mut behaviour, active_peer, active, &inbound);
    assert_denied(
        admit(&mut behaviour, unknown_peer, rejected, &inbound),
        Kind::EstablishedIncoming,
    );
    close(&mut behaviour, unknown_peer, rejected, &inbound);
    assert_counts(&behaviour, 1, 0, 1);
    assert_denied(
        admit(&mut behaviour, active_peer, rejected, &outbound),
        Kind::EstablishedPerPeer,
    );
    close(&mut behaviour, active_peer, rejected, &outbound);
    assert_counts(&behaviour, 1, 0, 1);

    let pending = ConnectionId::new_unchecked(3);
    let extra = ConnectionId::new_unchecked(4);
    let addr = Multiaddr::empty();
    assert!(behaviour.handle_pending_inbound_connection(pending, &addr, &addr).is_ok());
    assert_denied(
        behaviour.handle_pending_inbound_connection(extra, &addr, &addr),
        Kind::PendingIncoming,
    );
    assert!(
        behaviour
            .handle_pending_outbound_connection(pending, Some(unknown_peer), &[], Endpoint::Dialer)
            .is_ok()
    );
    assert_denied(
        behaviour.handle_pending_outbound_connection(
            extra,
            Some(unknown_peer),
            &[],
            Endpoint::Dialer,
        ),
        Kind::PendingOutgoing,
    );
    behaviour.on_swarm_event(FromSwarm::DialFailure(DialFailure {
        peer_id: Some(unknown_peer),
        error: &DialError::Aborted,
        connection_id: pending,
    }));
    behaviour.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
        local_addr: &addr,
        send_back_addr: &addr,
        error: &ListenError::Aborted,
        connection_id: pending,
        peer_id: Some(unknown_peer),
    }));
    assert!(behaviour.pending_inbound_connections.is_empty());
    assert!(behaviour.pending_outbound_connections.is_empty());
    close(&mut behaviour, unknown_peer, pending, &outbound);
    close(&mut behaviour, unknown_peer, extra, &inbound);
    assert_counts(&behaviour, 1, 0, 1);
    close(&mut behaviour, active_peer, active, &inbound);
    assert_counts(&behaviour, 0, 0, 0);
    Ok(())
}

/// Directional and per-peer ceilings still reject excess connections after slot reuse.
#[test]
fn configured_directional_and_per_peer_limits_are_preserved() -> TestResult {
    let mut behaviour = Behaviour::new(
        ConnectionLimits::default()
            .with_max_established_incoming(Some(1))
            .with_max_established_outgoing(Some(1))
            .with_max_established_per_peer(Some(1)),
    );
    let first_peer = peer(1)?;
    let second_peer = peer(2)?;
    let third_peer = peer(3)?;
    let first = ConnectionId::new_unchecked(1);
    let second = ConnectionId::new_unchecked(2);
    let extra = ConnectionId::new_unchecked(3);
    let inbound = endpoint(Endpoint::Listener);
    let outbound = endpoint(Endpoint::Dialer);
    establish(&mut behaviour, first_peer, first, &inbound);
    assert_denied(admit(&mut behaviour, first_peer, extra, &outbound), Kind::EstablishedPerPeer);
    establish(&mut behaviour, second_peer, second, &outbound);
    assert_denied(admit(&mut behaviour, third_peer, extra, &inbound), Kind::EstablishedIncoming);
    assert_denied(admit(&mut behaviour, third_peer, extra, &outbound), Kind::EstablishedOutgoing);
    close(&mut behaviour, first_peer, first, &inbound);
    establish(&mut behaviour, third_peer, extra, &inbound);
    assert_counts(&behaviour, 1, 1, 2);
    assert_denied(admit(&mut behaviour, first_peer, first, &inbound), Kind::EstablishedIncoming);
    close(&mut behaviour, second_peer, second, &outbound);
    close(&mut behaviour, third_peer, extra, &inbound);
    assert_counts(&behaviour, 0, 0, 0);
    Ok(())
}

/// The aggregate ceiling remains effective with the per-peer ceiling enabled or disabled.
#[test]
fn aggregate_limit_is_preserved_with_either_per_peer_configuration() -> TestResult {
    [Some(1), None].into_iter().try_for_each(|per_peer_limit| {
        let mut behaviour = Behaviour::new(
            ConnectionLimits::default()
                .with_max_established_incoming(Some(2))
                .with_max_established_outgoing(Some(2))
                .with_max_established(Some(2))
                .with_max_established_per_peer(per_peer_limit),
        );
        let first_peer = peer(1)?;
        let second_peer = peer(2)?;
        let third_peer = peer(3)?;
        let first = ConnectionId::new_unchecked(1);
        let second = ConnectionId::new_unchecked(2);
        let extra = ConnectionId::new_unchecked(3);
        let inbound = endpoint(Endpoint::Listener);
        let outbound = endpoint(Endpoint::Dialer);
        establish(&mut behaviour, first_peer, first, &inbound);
        establish(&mut behaviour, second_peer, second, &outbound);
        assert_denied(admit(&mut behaviour, third_peer, extra, &inbound), Kind::EstablishedTotal);
        assert_denied(admit(&mut behaviour, third_peer, extra, &outbound), Kind::EstablishedTotal);
        close(&mut behaviour, first_peer, first, &inbound);
        establish(&mut behaviour, third_peer, extra, &inbound);
        assert_counts(&behaviour, 1, 1, 2);
        close(&mut behaviour, second_peer, second, &outbound);
        close(&mut behaviour, third_peer, extra, &inbound);
        assert_counts(&behaviour, 0, 0, 0);
        Ok(())
    })
}

/// Disabling the per-peer ceiling allows multiple connections without disabling the total bound.
#[test]
fn disabled_per_peer_limit_still_enforces_global_limit() -> TestResult {
    let mut behaviour = Behaviour::new(ConnectionLimits::default().with_max_established(Some(3)));
    let peer = peer(1)?;
    let inbound = endpoint(Endpoint::Listener);
    let outbound = endpoint(Endpoint::Dialer);
    let first = ConnectionId::new_unchecked(1);
    let second = ConnectionId::new_unchecked(2);
    let third = ConnectionId::new_unchecked(3);
    let extra = ConnectionId::new_unchecked(4);
    establish(&mut behaviour, peer, first, &inbound);
    establish(&mut behaviour, peer, second, &outbound);
    establish(&mut behaviour, peer, third, &outbound);
    assert_counts(&behaviour, 1, 2, 1);
    assert_denied(admit(&mut behaviour, peer, extra, &inbound), Kind::EstablishedTotal);
    close(&mut behaviour, peer, second, &outbound);
    establish(&mut behaviour, peer, extra, &inbound);
    assert_counts(&behaviour, 2, 1, 1);
    close(&mut behaviour, peer, first, &inbound);
    close(&mut behaviour, peer, third, &outbound);
    close(&mut behaviour, peer, extra, &inbound);
    assert_counts(&behaviour, 0, 0, 0);
    Ok(())
}
