//! Regressions for the production established-connection budget behaviour.

use libp2p::{
    core::{transport::PortUse, ConnectedPoint, Endpoint},
    swarm::{
        behaviour::ConnectionEstablished, ConnectionClosed, ConnectionId, FromSwarm,
        NetworkBehaviour as _,
    },
    Multiaddr, PeerId,
};
use tn_config::NetworkProcessBudget;

/// Independent swarms enforce their allocation across identities and directions, then release it.
#[test]
fn process_budget_caps_each_swarm_and_releases_closed_connections() -> Result<(), std::io::Error> {
    let budget: NetworkProcessBudget = serde_json::from_value(serde_json::json!({
        "swarm_count": 2,
        "max_established_connections": 4,
        "max_established_connections_per_peer": 1,
        "max_inbound_streams": 40,
        "max_receive_credit_bytes": 4000
    }))?;
    let allocation = budget.allocate().map_err(std::io::Error::other)?;
    let mut primary = super::connection_limits_behaviour(Some(allocation));
    let mut worker = super::connection_limits_behaviour(Some(allocation));
    let peer = PeerId::random();
    let other_peer = PeerId::random();
    let addr = Multiaddr::empty();
    let listener =
        ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr.clone() };
    let dialer = ConnectedPoint::Dialer {
        address: addr.clone(),
        role_override: Endpoint::Dialer,
        port_use: PortUse::Reuse,
    };
    let first = ConnectionId::new_unchecked(1);
    let second = ConnectionId::new_unchecked(2);
    let third = ConnectionId::new_unchecked(3);

    assert!(primary.handle_established_inbound_connection(first, peer, &addr, &addr).is_ok());
    primary.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id: peer,
        connection_id: first,
        endpoint: &listener,
        failed_addresses: &[],
        other_established: 0,
    }));
    assert!(primary
        .handle_established_outbound_connection(
            second,
            peer,
            &addr,
            Endpoint::Dialer,
            PortUse::Reuse
        )
        .is_err());
    assert!(primary
        .handle_established_outbound_connection(
            second,
            other_peer,
            &addr,
            Endpoint::Dialer,
            PortUse::Reuse
        )
        .is_ok());
    primary.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id: other_peer,
        connection_id: second,
        endpoint: &dialer,
        failed_addresses: &[],
        other_established: 0,
    }));
    assert!(primary
        .handle_established_inbound_connection(third, PeerId::random(), &addr, &addr)
        .is_err());
    assert!(primary
        .handle_established_outbound_connection(
            third,
            PeerId::random(),
            &addr,
            Endpoint::Dialer,
            PortUse::Reuse
        )
        .is_err());
    assert!(worker.handle_established_inbound_connection(third, peer, &addr, &addr).is_ok());

    primary.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
        peer_id: peer,
        connection_id: first,
        endpoint: &listener,
        cause: None,
        remaining_established: 0,
    }));
    assert!(primary.handle_established_inbound_connection(third, peer, &addr, &addr).is_ok());
    Ok(())
}
