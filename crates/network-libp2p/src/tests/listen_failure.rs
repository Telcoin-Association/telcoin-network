//! Inbound failure observations and ownership across the complete swarm lifecycle.

use super::*;
use crate::{metrics::network_label, types::NetworkType};
use eyre::OptionExt as _;
use futures::{future, stream, StreamExt as _, TryStreamExt as _};
use libp2p::{
    allow_block_list,
    connection_limits::{self, ConnectionLimits},
    core::{transport::TransportError, ConnectedPoint},
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, ListenError, ListenFailure, NetworkBehaviour,
        SwarmEvent,
    },
    Swarm, SwarmBuilder,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::{io, time::Duration};

/// Capture one group of assertions before the debugging recorder resets its counters.
fn snapshot_metrics(recorder: &DebuggingRecorder) -> Vec<(metrics::Key, DebugValue)> {
    recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| (key.key().clone(), value))
        .collect()
}

/// Read a metric with the requested fixed labels from a shared snapshot.
fn metric<'a>(
    snapshot: &'a [(metrics::Key, DebugValue)],
    name: &str,
    labels: &[(&str, &str)],
) -> Option<&'a DebugValue> {
    snapshot
        .iter()
        .find(|(key, _)| {
            key.name() == name
                && labels.iter().all(|(name, value)| {
                    key.labels().any(|label| label.key() == *name && label.value() == *value)
                })
        })
        .map(|(_, value)| value)
}

/// Every failure kind is counted once without changing unrelated peer or outbound dial state.
#[tokio::test(start_paused = true)]
async fn test_listen_failure_reasons_and_accounting() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let _guard = metrics::set_default_local_recorder(&recorder);
    [NetworkType::Primary, NetworkType::Worker(0)].into_iter().try_for_each(|network| {
        let local = PeerId::random();
        let live = PeerId::random();
        let dialing = PeerId::random();
        let failed = PeerId::random();
        let addr: Multiaddr = "/ip4/127.0.0.1/udp/12345/quic-v1".parse()?;
        let endpoint =
            ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr.clone() };
        let mut manager =
            PeerManager::new(local, &PeerConfig::default(), PeerManagerMetrics::new_for(&network));
        manager.register_peer_connection(
            &live,
            ConnectionType::IncomingConnection { multiaddr: addr.clone() },
        );
        let (reply, mut receive) = oneshot::channel();
        manager.register_dial_attempt(dialing, Some(reply));
        manager.temporarily_banned.insert(failed);
        let id = ConnectionId::new_unchecked(1);
        let invalid = manager
            .handle_pending_inbound_connection(id, &addr, &"/memory/1".parse()?)
            .err()
            .ok_or_eyre("unsupported address was admitted")?;
        let banned = manager
            .handle_established_inbound_connection(id, failed, &addr, &addr)
            .err()
            .ok_or_eyre("banned peer was admitted")?;
        let own = manager
            .handle_established_inbound_connection(id, local, &addr, &addr)
            .err()
            .ok_or_eyre("local peer was admitted")?;
        let errors = [
            (ListenError::Denied { cause: invalid }, "peer_manager_invalid_ip"),
            (ListenError::Denied { cause: banned }, "peer_manager_banned_peer"),
            (ListenError::Denied { cause: own }, "peer_manager_local_peer"),
            (
                ListenError::Denied { cause: ConnectionDenied::new("later behaviour") },
                "other_behaviour_denied",
            ),
            (
                ListenError::Transport(TransportError::Other(io::Error::other("failed"))),
                "transport",
            ),
            (
                ListenError::WrongPeerId { obtained: failed, endpoint: endpoint.clone() },
                "wrong_peer_id",
            ),
            (ListenError::LocalPeerId { address: addr.clone() }, "local_peer_id"),
            (ListenError::Aborted, "aborted"),
        ];
        errors.iter().for_each(|(error, reason)| {
            // The optional identity never owns an established connection or an outbound reply.
            [None, Some(failed), Some(live), Some(dialing)].into_iter().for_each(|peer_id| {
                manager.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
                    local_addr: &addr,
                    send_back_addr: &addr,
                    error,
                    connection_id: id,
                    peer_id,
                }));
                assert!(manager.is_connected(&live));
                assert_eq!(manager.peers.connected_peer_ids().count(), 1);
                assert!(manager.dial_attempt_already_registered(&dialing));
                assert!(manager.peers.get_peer(&failed).is_none());
                assert_eq!(manager.temporarily_banned.len(), 1);
                assert!(matches!(receive.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
            });
            let label = network_label(&network);
            let snapshot = snapshot_metrics(&recorder);
            assert_eq!(
                metric(
                    &snapshot,
                    "tn_network.listen_failures_total",
                    &[("network", &label), ("reason", reason)]
                ),
                Some(&DebugValue::Counter(4))
            );
        });
        manager.notify_dial_result(&dialing, Ok(()));
        assert!(receive.try_recv()?.is_ok());
        Ok::<_, eyre::Report>(())
    })?;
    let snapshot = snapshot_metrics(&recorder);
    let failures: Vec<_> = snapshot
        .iter()
        .filter(|(key, _)| key.name() == "tn_network.listen_failures_total")
        .collect();
    assert_eq!(failures.len(), 16);
    assert!(failures.iter().all(|(key, _)| key.labels().count() == 2));
    assert_eq!(
        metric(&snapshot, "tn_network.connections_closed_total", &[("network", "primary")]),
        Some(&DebugValue::Counter(0))
    );
    Ok(())
}

/// Production admission ordering with an independently denying later behaviour.
#[derive(NetworkBehaviour)]
struct AdmissionBehaviour {
    /// The peer manager runs first.
    manager: PeerManager,
    /// Libp2p owns the single pending slot used to detect leaks.
    limits: connection_limits::Behaviour,
    /// Refuse an authenticated peer after the manager has accepted it.
    later: allow_block_list::Behaviour<allow_block_list::BlockedPeers>,
}

/// Build a real QUIC swarm with primary/worker metric attribution and production admission order.
fn admission_swarm(network: &NetworkType) -> eyre::Result<Swarm<AdmissionBehaviour>> {
    Ok(SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| AdmissionBehaviour {
            manager: PeerManager::new(
                key.public().to_peer_id(),
                &PeerConfig::default(),
                PeerManagerMetrics::new_for(network),
            ),
            limits: connection_limits::Behaviour::new(
                ConnectionLimits::default().with_max_pending_incoming(Some(1)),
            ),
            later: allow_block_list::Behaviour::default(),
        })?
        .build())
}

/// Drive both endpoints until the listener produces the requested lifecycle event.
async fn observe(
    server: &mut Swarm<AdmissionBehaviour>,
    client: &mut Swarm<libp2p::swarm::dummy::Behaviour>,
    predicate: impl Fn(&SwarmEvent<AdmissionBehaviourEvent>) -> bool,
) -> eyre::Result<()> {
    stream::select(server.map(Some), client.map(|_| None))
        .filter_map(future::ready)
        .filter(|event| future::ready(predicate(event)))
        .next()
        .await
        .ok_or_eyre("swarm ended before the expected event")
        .map(|_| ())
}

/// Pending failures free only the connection-limit slot associated with their connection ID.
#[tokio::test(start_paused = true)]
async fn test_listen_failure_pending_owner_cleanup() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let addr: Multiaddr = "/ip4/127.0.0.1/udp/12345/quic-v1".parse()?;
    [NetworkType::Primary, NetworkType::Worker(0)].into_iter().try_for_each(|network| {
        let mut swarm = admission_swarm(&network)?;
        let behaviour = swarm.behaviour_mut();
        (0..8).try_for_each(|attempt| {
            let pending = ConnectionId::new_unchecked(attempt);
            let rejected = ConnectionId::new_unchecked(100 + attempt);
            behaviour.handle_pending_inbound_connection(pending, &addr, &addr)?;
            let denial = behaviour
                .handle_pending_inbound_connection(rejected, &addr, &addr)
                .err()
                .ok_or_eyre("the pending limit was not enforced")?;
            behaviour.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
                local_addr: &addr,
                send_back_addr: &addr,
                error: &ListenError::Denied { cause: denial },
                connection_id: rejected,
                peer_id: None,
            }));
            // Refusing a different connection must not free the occupied slot.
            assert!(behaviour.handle_pending_inbound_connection(rejected, &addr, &addr).is_err());
            behaviour.on_swarm_event(FromSwarm::ListenFailure(ListenFailure {
                local_addr: &addr,
                send_back_addr: &addr,
                error: &ListenError::Transport(TransportError::Other(io::Error::other(
                    "handshake failed",
                ))),
                connection_id: pending,
                peer_id: None,
            }));
            assert!(behaviour.manager.peers.connected_peer_ids().next().is_none());
            Ok::<_, eyre::Report>(())
        })?;
        behaviour.handle_pending_inbound_connection(
            ConnectionId::new_unchecked(999),
            &addr,
            &addr,
        )?;
        let label = network_label(&network);
        let snapshot = snapshot_metrics(&recorder);
        ["transport", "other_behaviour_denied"].iter().for_each(|reason| {
            assert_eq!(
                metric(
                    &snapshot,
                    "tn_network.listen_failures_total",
                    &[("network", &label), ("reason", reason)]
                ),
                Some(&DebugValue::Counter(8))
            );
        });
        Ok::<_, eyre::Report>(())
    })?;
    Ok(())
}

/// Denials free swarm and connection-limit slots; the next allowed connection succeeds and closes.
#[tokio::test]
async fn test_listen_failure_swarm_lifecycle() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let _guard = metrics::set_default_local_recorder(&recorder);
    tokio::time::timeout(Duration::from_secs(30), async {
        stream::iter([NetworkType::Primary, NetworkType::Worker(0)])
            .map(Ok::<_, eyre::Report>)
            .try_for_each(|network| {
                let recorder = &recorder;
                async move {
                    let mut server = admission_swarm(&network)?;
                    let client = SwarmBuilder::with_new_identity()
                        .with_tokio()
                        .with_quic()
                        .with_behaviour(|_| libp2p::swarm::dummy::Behaviour)?
                        .build();
                    server.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
                    let addr = server
                        .by_ref()
                        .filter_map(|event| {
                            future::ready(
                                if let SwarmEvent::NewListenAddr { address, .. } = event {
                                    Some(address)
                                } else {
                                    None
                                },
                            )
                        })
                        .next()
                        .await
                        .ok_or_eyre("listener ended before binding")?;
                    let remote = *client.local_peer_id();
                    // Three manager denials followed by three later-behaviour denials.
                    let (mut server, mut client) = stream::iter(0..6)
                        .map(Ok::<_, eyre::Report>)
                        .try_fold((server, client), |(mut server, mut client), attempt| {
                            let addr = addr.clone();
                            async move {
                                if attempt < 3 {
                                    server
                                        .behaviour_mut()
                                        .manager
                                        .temporarily_banned
                                        .insert(remote);
                                } else {
                                    server
                                        .behaviour_mut()
                                        .manager
                                        .temporarily_banned
                                        .remove(&remote);
                                    server.behaviour_mut().later.block_peer(remote);
                                }
                                client.dial(addr.clone())?;
                                observe(&mut server, &mut client, |event| {
                                    if let SwarmEvent::IncomingConnectionError { error, .. } = event
                                    {
                                        assert!(matches!(error, ListenError::Denied { .. }));
                                        true
                                    } else {
                                        false
                                    }
                                })
                                .await?;
                                assert_eq!(
                                    server
                                        .network_info()
                                        .connection_counters()
                                        .num_pending_incoming(),
                                    0
                                );
                                assert_eq!(
                                    server.network_info().connection_counters().num_established(),
                                    0
                                );
                                assert!(server
                                    .behaviour()
                                    .manager
                                    .peers
                                    .connected_peer_ids()
                                    .next()
                                    .is_none());
                                assert!(server
                                    .behaviour()
                                    .manager
                                    .peers
                                    .get_peer(&remote)
                                    .is_none());
                                // A new pending slot would fail if connection_limits retained the
                                // first.
                                let probe = ConnectionId::new_unchecked(100 + attempt);
                                server
                                    .behaviour_mut()
                                    .limits
                                    .handle_pending_inbound_connection(probe, &addr, &addr)?;
                                server.behaviour_mut().limits.on_swarm_event(
                                    FromSwarm::ListenFailure(ListenFailure {
                                        local_addr: &addr,
                                        send_back_addr: &addr,
                                        error: &ListenError::Aborted,
                                        connection_id: probe,
                                        peer_id: None,
                                    }),
                                );
                                Ok::<_, eyre::Report>((server, client))
                            }
                        })
                        .await?;
                    server.behaviour_mut().later.unblock_peer(remote);
                    client.dial(addr)?;
                    observe(&mut server, &mut client, |event| {
                        matches!(event, SwarmEvent::ConnectionEstablished { .. })
                    })
                    .await?;
                    assert!(server.behaviour().manager.is_connected(&remote));
                    server
                        .disconnect_peer_id(remote)
                        .map_err(|()| eyre::eyre!("peer was not connected"))?;
                    observe(&mut server, &mut client, |event| {
                        matches!(event, SwarmEvent::ConnectionClosed { .. })
                    })
                    .await?;
                    assert!(!server.behaviour().manager.is_connected(&remote));
                    let label = network_label(&network);
                    let snapshot = snapshot_metrics(recorder);
                    ["peer_manager_banned_peer", "other_behaviour_denied"].iter().for_each(
                        |reason| {
                            assert_eq!(
                                metric(
                                    &snapshot,
                                    "tn_network.listen_failures_total",
                                    &[("network", &label), ("reason", reason)]
                                ),
                                Some(&DebugValue::Counter(3))
                            );
                        },
                    );
                    assert_eq!(
                        metric(
                            &snapshot,
                            "tn_network.connections_established_total",
                            &[("network", &label), ("direction", "in")]
                        ),
                        Some(&DebugValue::Counter(1))
                    );
                    assert_eq!(
                        metric(
                            &snapshot,
                            "tn_network.connections_closed_total",
                            &[("network", &label)]
                        ),
                        Some(&DebugValue::Counter(1))
                    );
                    Ok::<_, eyre::Report>(())
                }
            })
            .await
    })
    .await??;
    Ok(())
}
