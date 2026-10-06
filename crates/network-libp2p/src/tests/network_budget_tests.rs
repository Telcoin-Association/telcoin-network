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
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BootstrapServer, P2pNode, TaskManager};
use tokio::sync::mpsc;

use crate::{
    common::{TestPrimaryRequest, TestPrimaryResponse},
    consensus::{ConsensusNetwork, NetworkEvent, NetworkType},
};

/// Admit and account for one transport in the order used by the pinned libp2p swarm.
fn establish_reserved(
    behaviour: &mut libp2p::connection_limits::Behaviour,
    peer: PeerId,
    id: usize,
    outbound: bool,
) {
    let connection_id = ConnectionId::new_unchecked(id);
    let addr = Multiaddr::empty();
    let endpoint = if outbound {
        assert!(behaviour
            .handle_established_outbound_connection(
                connection_id,
                peer,
                &addr,
                Endpoint::Dialer,
                PortUse::Reuse,
            )
            .is_ok());
        ConnectedPoint::Dialer {
            address: addr,
            role_override: Endpoint::Dialer,
            port_use: PortUse::Reuse,
        }
    } else {
        assert!(behaviour
            .handle_established_inbound_connection(connection_id, peer, &addr, &addr)
            .is_ok());
        ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr }
    };
    behaviour.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
        peer_id: peer,
        connection_id,
        endpoint: &endpoint,
        failed_addresses: &[],
        other_established: 0,
    }));
}

/// The actual constructor reserves each role's configured hubs before any swarm polling.
#[tokio::test]
async fn bootstrap_connection_reservations_survive_pressure_and_reconnect(
) -> Result<(), std::io::Error> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let configs = fixture
        .authorities()
        .take(3)
        .map(|authority| authority.consensus_config())
        .collect::<Vec<_>>();
    let client = configs.get(2).ok_or_else(|| std::io::Error::other("client fixture missing"))?;
    let addr: Multiaddr =
        "/ip4/127.0.0.1/udp/40000/quic-v1".parse().map_err(std::io::Error::other)?;
    let servers = configs
        .iter()
        .take(2)
        .map(|config| {
            let keys = config.key_config();
            (
                keys.primary_public_key(),
                BootstrapServer::new(
                    P2pNode {
                        network_address: addr.clone(),
                        network_key: keys.primary_network_public_key(),
                        rpc: None,
                    },
                    [0, 1]
                        .into_iter()
                        .map(|id| P2pNode {
                            network_address: addr.clone(),
                            network_key: keys.worker_network_public_key(id),
                            rpc: None,
                        })
                        .collect(),
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let config: tn_config::NetworkConfig = serde_json::from_value(serde_json::json!({
        "bootstrap_peers": servers,
        "process_budget": {
            "swarm_count": 3,
            "max_established_connections": 12,
            "max_established_connections_per_peer": 2,
            "max_inbound_streams": 192,
            "max_receive_credit_bytes": 50331648
        }
    }))?;
    let task_manager = TaskManager::default();
    // Worker 1 has its own derived identity, so this catches reserving worker 0 for every worker.
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)]
        .into_iter()
        .try_for_each(|role| -> Result<(), std::io::Error> {
            let keys = client.key_config();
            let network_key = match role {
                NetworkType::Primary => keys.primary_network_keypair().clone(),
                NetworkType::Worker(id) => keys.worker_network_keypair(id),
            };
            let (tx, _rx) = mpsc::channel(10);
            let mut network = ConsensusNetwork::<
                TestPrimaryRequest,
                TestPrimaryResponse,
                MemDatabase,
                mpsc::Sender<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>,
            >::new(
                &config,
                tx,
                keys.clone(),
                network_key,
                MemDatabase::default(),
                task_manager.get_spawner(),
                role,
                addr.clone(),
                None,
            )
            .map_err(std::io::Error::other)?;
            let required = config
                .bootstrap_peers()
                .values()
                .map(|server| match role {
                    NetworkType::Primary => Ok(PeerId::from(server.primary.network_key.clone())),
                    NetworkType::Worker(id) => server
                        .worker(id)
                        .map(|worker| PeerId::from(worker.network_key.clone()))
                        .ok_or_else(|| std::io::Error::other("configured worker missing")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(required.len(), 2);
            let a = required[0];
            let b = required[1];
            assert_ne!(a, b);
            let guests = [100, 101, 102]
                .into_iter()
                .map(|id| PeerId::from_bytes(&[0, 1, id]).map_err(std::io::Error::other))
                .collect::<Result<Vec<_>, _>>()?;
            let behaviour = &mut network.swarm.behaviour_mut().connection_limits;
            let empty = Multiaddr::empty();
            establish_reserved(behaviour, guests[0], 1, false);
            establish_reserved(behaviour, guests[1], 2, true);
            assert!(behaviour
                .handle_established_inbound_connection(
                    ConnectionId::new_unchecked(3),
                    guests[2],
                    &empty,
                    &empty
                )
                .is_err());
            establish_reserved(behaviour, a, 3, true);
            assert!(behaviour
                .handle_established_outbound_connection(
                    ConnectionId::new_unchecked(4),
                    a,
                    &empty,
                    Endpoint::Dialer,
                    PortUse::Reuse
                )
                .is_err());
            establish_reserved(behaviour, b, 4, false);
            assert!(behaviour
                .handle_established_inbound_connection(
                    ConnectionId::new_unchecked(5),
                    guests[2],
                    &empty,
                    &empty
                )
                .is_err());
            let endpoint = ConnectedPoint::Dialer {
                address: empty.clone(),
                role_override: Endpoint::Dialer,
                port_use: PortUse::Reuse,
            };
            behaviour.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id: a,
                connection_id: ConnectionId::new_unchecked(3),
                endpoint: &endpoint,
                cause: None,
                remaining_established: 0,
            }));
            assert!(behaviour
                .handle_established_inbound_connection(
                    ConnectionId::new_unchecked(5),
                    guests[2],
                    &empty,
                    &empty
                )
                .is_err());
            establish_reserved(behaviour, a, 5, true);
            Ok(())
        })?;
    // Configured identities do not turn an unbudgeted legacy constructor into a startup error.
    let legacy_config: tn_config::NetworkConfig =
        serde_json::from_value(serde_json::json!({ "bootstrap_peers": config.bootstrap_peers() }))?;
    let (tx, _rx) = mpsc::channel(10);
    assert!(ConsensusNetwork::<
        TestPrimaryRequest,
        TestPrimaryResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>,
    >::new(
        &legacy_config,
        tx,
        client.key_config().clone(),
        client.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        addr,
        None,
    )
    .is_ok());
    Ok(())
}

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
    let mut primary = super::connection_limits_behaviour(
        super::MAX_PENDING_INCOMING_CONNECTIONS,
        Some(allocation),
    );
    let mut worker = super::connection_limits_behaviour(
        super::MAX_PENDING_INCOMING_CONNECTIONS,
        Some(allocation),
    );
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
