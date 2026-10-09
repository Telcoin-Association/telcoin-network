//! Public direct gossip preserves ordinary admission and disconnect lifecycles.

use super::*;
use futures::TryStreamExt as _;

type RelayTestPeer = TestPeer<TestWorkerRequest, TestWorkerResponse>;

fn relay_config() -> eyre::Result<NetworkConfig> {
    serde_json::from_value(serde_json::json!({
        "public_peer_limit": 2,
        "process_budget": {
            "swarm_count": 3,
            "max_established_connections": 12,
            "max_established_connections_per_peer": 2,
            "max_inbound_streams": 192,
            "max_receive_credit_bytes": 50331648
        }
    }))
    .map_err(Into::into)
}

fn receives_public_gossip(
    network: &ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    peer: &PeerId,
) -> bool {
    network.public_gossip_peers.as_ref().is_some_and(|peers| peers.ordinary.contains(peer))
}

/// Establish real authenticated transports without processing queued record advertisements.
async fn connected_fixture(
    config: NetworkConfig,
) -> eyre::Result<TestTypes<TestWorkerRequest, TestWorkerResponse>> {
    let mut fixture = create_test_types_with_config(config);
    let remote = *fixture.peer2.network.swarm.local_peer_id();
    let local = *fixture.peer1.network.swarm.local_peer_id();
    fixture.peer1.network.swarm.listen_on(fixture.peer1.config.primary_address())?;
    fixture.peer2.network.swarm.listen_on(fixture.peer2.config.primary_address())?;
    fixture.peer1.network.swarm.dial(fixture.peer2.config.primary_address())?;
    let progress = futures::stream::unfold(
        (&mut fixture.peer1.network.swarm, &mut fixture.peer2.network.swarm),
        move |(first, second)| async move {
            tokio::select! {
                _event = first.select_next_some() => {},
                _event = second.select_next_some() => {},
            }
            let connected = first.is_connected(&remote)
                && second.is_connected(&local)
                && first.behaviour().peer_manager.is_connected(&remote);
            Some((connected, (first, second)))
        },
    )
    .filter(|connected| futures::future::ready(*connected));
    let mut progress = Box::pin(progress);
    timeout(Duration::from_secs(5), progress.next())
        .await?
        .ok_or_else(|| eyre!("connection progress ended"))?;
    drop(progress);
    assert_eq!(fixture.peer1.network.swarm.behaviour().peer_manager.peer_to_bls(&remote), None);
    Ok(fixture)
}

async fn confirmed_fixture() -> eyre::Result<TestTypes<TestWorkerRequest, TestWorkerResponse>> {
    let mut fixture = connected_fixture(relay_config()?).await?;
    let source = *fixture.peer2.network.swarm.local_peer_id();
    fixture
        .peer1
        .network
        .process_kad_put_request(source, fixture.peer2.network.get_peer_record())?;
    assert!(receives_public_gossip(&fixture.peer1.network, &source));
    Ok(fixture)
}

/// Hold application reconciliation while gossipsub learns the recipient's subscription.
async fn advertise_subscription(
    first: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    second: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    topic: &IdentTopic,
) -> eyre::Result<()> {
    let source = *second.swarm.local_peer_id();
    second.swarm.behaviour_mut().gossipsub.subscribe(topic)?;
    let progress = futures::stream::unfold((first, second), move |(first, second)| async move {
        tokio::select! {
            _event = first.swarm.select_next_some() => {},
            _event = second.swarm.select_next_some() => {},
        }
        let subscribed = first
            .swarm
            .behaviour()
            .gossipsub
            .all_peers()
            .any(|(peer, topics)| *peer == source && !topics.is_empty());
        Some((subscribed, (first, second)))
    })
    .filter(|subscribed| futures::future::ready(*subscribed));
    let mut progress = Box::pin(progress);
    timeout(Duration::from_secs(5), progress.next())
        .await?
        .ok_or_else(|| eyre!("subscription progress ended"))?;
    Ok(())
}

#[tokio::test]
async fn public_gossip_requires_public_capacity_and_process_budget() -> eyre::Result<()> {
    let complete = serde_json::to_value(relay_config()?)?;
    let cases = [
        (serde_json::json!({}), false),
        (serde_json::json!({"public_peer_limit": 2}), false),
        (serde_json::json!({"process_budget": complete["process_budget"]}), false),
        (complete, true),
    ];
    cases.into_iter().try_for_each(|(value, enabled)| -> eyre::Result<()> {
        let TestTypes { peer1, peer2, _task_manager } = create_test_types_with_config::<
            TestWorkerRequest,
            TestWorkerResponse,
        >(serde_json::from_value(value)?);
        // Any public-capable node can serve peers, without a special hub-role condition.
        assert_eq!(peer1.network.public_gossip_peers.is_some(), enabled);
        assert_eq!(peer2.network.public_gossip_peers.is_some(), enabled);
        Ok(())
    })
}

#[tokio::test]
async fn public_gossip_confirmation_preserves_ordinary_admission() -> eyre::Result<()> {
    let TestTypes { mut peer1, peer2, _task_manager } = connected_fixture(relay_config()?).await?;
    let source = *peer2.network.swarm.local_peer_id();
    peer1.network.refresh_explicit_peer(&source);
    assert!(!receives_public_gossip(&peer1.network, &source), "unresolved peers are ineligible");
    let manager = &peer1.network.swarm.behaviour().peer_manager;
    let connected = manager.connected_peers();
    let score = manager.peer_score(&source);
    let addresses = manager.peer_multiaddr_count(&source);
    peer1.network.process_kad_put_request(source, peer2.network.get_peer_record())?;
    let manager = &peer1.network.swarm.behaviour().peer_manager;
    assert!(receives_public_gossip(&peer1.network, &source));
    assert!(manager.peer_is_confirmed_ordinary(&source));
    assert!(!manager.peer_is_important(&source));
    assert_eq!(manager.peer_to_bls(&source), Some(peer2.config.key_config().primary_public_key()));
    assert_eq!(manager.connected_peers(), connected);
    assert_eq!(manager.peer_score(&source), score);
    assert_eq!(manager.peer_multiaddr_count(&source), addresses);
    assert!(!peer1.network.swarm.behaviour().connection_limits.is_bypassed(&source));

    peer1.network.swarm.behaviour_mut().peer_manager.disconnect_peer(source, true);
    peer1.network.refresh_explicit_peers();
    assert!(!receives_public_gossip(&peer1.network, &source));
    let manager = &mut peer1.network.swarm.behaviour_mut().peer_manager;
    assert!(!manager.is_connected(&source));
    assert!(manager.simulate_temporary_ban_expiry(&source));
    assert!(!manager.peer_banned(&source));
    peer1.network.refresh_explicit_peer(&source);
    assert!(!receives_public_gossip(&peer1.network, &source), "pending PX cannot regain delivery");
    Ok(())
}

#[tokio::test]
async fn public_gossip_live_expired_record_confirms_delivery() -> eyre::Result<()> {
    let TestTypes { mut peer1, peer2, _task_manager } = connected_fixture(relay_config()?).await?;
    let source = *peer2.network.swarm.local_peer_id();
    let record = expired_kad_updated_record(&peer2)?;
    peer1.network.process_kad_put_request(source, record.clone())?;
    assert!(receives_public_gossip(&peer1.network, &source));
    assert!(peer1.network.swarm.behaviour().peer_manager.peer_is_confirmed_ordinary(&source));
    assert!(peer1.network.swarm.behaviour_mut().kademlia.store_mut().get(&record.key).is_none());
    Ok(())
}

#[derive(Debug)]
enum DisconnectPath {
    Command,
    Manager,
    PeerExchange,
    Disconnected,
    Banned,
}

#[tokio::test]
async fn public_gossip_disconnect_and_ban_cleanup_is_synchronous() -> eyre::Result<()> {
    futures::stream::iter([
        DisconnectPath::Command,
        DisconnectPath::Manager,
        DisconnectPath::PeerExchange,
        DisconnectPath::Disconnected,
        DisconnectPath::Banned,
    ])
    .map(Ok)
    .try_for_each(|path| async move {
        let TestTypes { mut peer1, mut peer2, _task_manager } = confirmed_fixture().await?;
        let source = *peer2.network.swarm.local_peer_id();
        let topic = IdentTopic::new("public-cleanup-mesh-oracle");
        advertise_subscription(&mut peer1.network, &mut peer2.network, &topic).await?;
        match path {
            DisconnectPath::Command => {
                let (reply, _response) = tokio::sync::oneshot::channel();
                peer1
                    .network
                    .process_command(NetworkCommand::DisconnectPeer { peer_id: source, reply })?;
            }
            DisconnectPath::Manager => {
                peer1.network.swarm.behaviour_mut().peer_manager.disconnect_peer(source, false);
                peer1.network.process_peer_manager_event(PeerEvent::DisconnectPeer(source))?;
            }
            DisconnectPath::PeerExchange => {
                peer1.network.swarm.behaviour_mut().peer_manager.disconnect_peer(source, true);
                peer1.network.process_peer_manager_event(PeerEvent::DisconnectPeerX(
                    source,
                    Default::default(),
                ))?;
            }
            DisconnectPath::Disconnected => {
                peer1.network.process_peer_manager_event(PeerEvent::PeerDisconnected(source))?;
            }
            DisconnectPath::Banned => {
                peer1
                    .network
                    .swarm
                    .behaviour_mut()
                    .peer_manager
                    .process_penalty(source, Penalty::Fatal);
                assert!(peer1.network.swarm.behaviour().peer_manager.peer_banned(&source));
                peer1.network.process_peer_manager_event(PeerEvent::Banned(source))?;
                peer1.network.refresh_explicit_peer(&source);
                // Remove only the gossip blacklist to isolate explicit-membership cleanup.
                // The manager ban remains effective for physical admission.
                peer1.network.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&source);
            }
        }
        assert!(!receives_public_gossip(&peer1.network, &source), "{path:?}");
        assert!(!peer1.network.swarm.behaviour().peer_manager.peer_is_important(&source));
        // Before another swarm poll closes the transport, a fresh mesh join can see this
        // healthy subscriber only if the actual gossipsub explicit entry was removed.
        peer1.network.swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
        assert!(
            peer1
                .network
                .swarm
                .behaviour()
                .gossipsub
                .mesh_peers(&topic.hash())
                .any(|peer| *peer == source),
            "explicit entry survived {path:?}"
        );
        Ok(())
    })
    .await
}

/// Observe a real last-connection-close before processing any manager reconciliation event.
async fn close_connection(
    first: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    second: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
) -> eyre::Result<()> {
    let source = *second.swarm.local_peer_id();
    first.swarm.disconnect_peer_id(source).map_err(|()| eyre!("source was disconnected"))?;
    let progress = futures::stream::unfold((&mut *first, second), |(first, second)| async move {
        let event = tokio::select! {
            event = first.swarm.select_next_some() => Some(event),
            _event = second.swarm.select_next_some() => None,
        };
        Some((event, (first, second)))
    })
    .filter_map(futures::future::ready)
    .filter(|event| {
        futures::future::ready(matches!(
            event,
            SwarmEvent::ConnectionClosed { num_established: 0, .. }
        ))
    });
    let mut progress = Box::pin(progress);
    let event = timeout(Duration::from_secs(5), progress.next())
        .await?
        .ok_or_else(|| eyre!("disconnect progress ended"))?;
    drop(progress);
    first.process_event(event).await.map_err(Into::into)
}

#[tokio::test]
async fn public_gossip_last_connection_close_cleans_up_before_reconciliation() -> eyre::Result<()> {
    let TestTypes { mut peer1, mut peer2, _task_manager } = confirmed_fixture().await?;
    let source = *peer2.network.swarm.local_peer_id();
    close_connection(&mut peer1.network, &mut peer2.network).await?;
    assert!(!receives_public_gossip(&peer1.network, &source));
    assert!(!peer1.network.swarm.is_connected(&source));
    peer1.network.refresh_explicit_peers();
    assert!(!receives_public_gossip(&peer1.network, &source));
    Ok(())
}

#[tokio::test]
async fn public_gossip_offline_committee_demotion_removes_reconnect_provenance() -> eyre::Result<()>
{
    let TestTypes { mut peer1, mut peer2, _task_manager } = confirmed_fixture().await?;
    let source = *peer2.network.swarm.local_peer_id();
    let key = peer2.config.key_config().primary_public_key();
    peer1.network.process_command(NetworkCommand::UpdateCommittees {
        previous: Default::default(),
        current: [key].into_iter().collect(),
        next: Default::default(),
    })?;
    assert!(!receives_public_gossip(&peer1.network, &source));
    assert!(peer1
        .network
        .public_gossip_peers
        .as_ref()
        .is_some_and(|peers| peers.promoted.contains(&source)));
    close_connection(&mut peer1.network, &mut peer2.network).await?;
    peer1.network.refresh_explicit_peers();
    assert!(peer1.network.swarm.behaviour().peer_manager.peer_is_important(&source));
    assert!(
        peer1
            .network
            .public_gossip_peers
            .as_ref()
            .is_some_and(|peers| peers.promoted.contains(&source)),
        "important peers keep reconnects while offline"
    );
    peer1.network.process_command(NetworkCommand::UpdateCommittees {
        previous: Default::default(),
        current: Default::default(),
        next: Default::default(),
    })?;
    assert!(!peer1.network.swarm.behaviour().peer_manager.peer_is_important(&source));
    assert!(!peer1
        .network
        .public_gossip_peers
        .as_ref()
        .is_some_and(|peers| peers.promoted.contains(&source)));
    assert!(!receives_public_gossip(&peer1.network, &source));
    let topic = IdentTopic::new("offline-public-demotion-mesh-oracle");
    peer1.network.swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
    peer1.network.swarm.dial(peer2.config.primary_address())?;
    advertise_subscription(&mut peer1.network, &mut peer2.network, &topic).await?;
    assert!(
        peer1
            .network
            .swarm
            .behaviour()
            .gossipsub
            .mesh_peers(&topic.hash())
            .any(|peer| *peer == source),
        "offline demotion must remove the actual explicit entry"
    );
    Ok(())
}

async fn connected_three_fixture(
) -> eyre::Result<(RelayTestPeer, RelayTestPeer, RelayTestPeer, TaskManager)> {
    connected_three_fixture_with_config(relay_config()?).await
}

async fn connected_three_fixture_with_config(
    config: NetworkConfig,
) -> eyre::Result<(RelayTestPeer, RelayTestPeer, RelayTestPeer, TaskManager)> {
    let (mut hub, mut others, task_manager) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(3).ok_or_else(|| eyre!("nonzero peer count"))?,
            Some(config),
        );
    let mut recipient = others.pop().ok_or_else(|| eyre!("recipient fixture missing"))?;
    let mut publisher = others.pop().ok_or_else(|| eyre!("publisher fixture missing"))?;
    let hub_network = hub.network.as_mut().ok_or_else(|| eyre!("relay network missing"))?;
    let recipient_network =
        recipient.network.as_mut().ok_or_else(|| eyre!("recipient network missing"))?;
    let publisher_network =
        publisher.network.as_mut().ok_or_else(|| eyre!("publisher network missing"))?;
    let recipient_id = *recipient_network.swarm.local_peer_id();
    let publisher_id = *publisher_network.swarm.local_peer_id();
    let hub_id = *hub_network.swarm.local_peer_id();
    hub_network.swarm.listen_on(hub.config.primary_address())?;
    recipient_network.swarm.listen_on(recipient.config.primary_address())?;
    publisher_network.swarm.listen_on(publisher.config.primary_address())?;
    hub_network.swarm.dial(recipient.config.primary_address())?;
    hub_network.swarm.dial(publisher.config.primary_address())?;
    let progress = futures::stream::unfold(
        (hub_network, recipient_network, publisher_network),
        move |(hub, recipient, publisher)| async move {
            tokio::select! {
                _event = hub.swarm.select_next_some() => {},
                _event = recipient.swarm.select_next_some() => {},
                _event = publisher.swarm.select_next_some() => {},
            }
            let connected = hub.swarm.behaviour().peer_manager.is_connected(&recipient_id)
                && hub.swarm.behaviour().peer_manager.is_connected(&publisher_id)
                && recipient.swarm.is_connected(&hub_id)
                && publisher.swarm.is_connected(&hub_id);
            Some((connected, (hub, recipient, publisher)))
        },
    )
    .filter(|connected| futures::future::ready(*connected));
    let mut progress = Box::pin(progress);
    timeout(Duration::from_secs(5), progress.next())
        .await?
        .ok_or_else(|| eyre!("connection progress ended"))?;
    drop(progress);
    Ok((hub, recipient, publisher, task_manager))
}

#[tokio::test]
async fn public_gossip_network_key_rotation_removes_displaced_peer() -> eyre::Result<()> {
    let (mut hub, mut previous, mut replacement, _task_manager) = connected_three_fixture().await?;
    let hub_network = hub.network.as_mut().ok_or_else(|| eyre!("relay network missing"))?;
    let previous_network =
        previous.network.as_mut().ok_or_else(|| eyre!("previous network missing"))?;
    let replacement_network =
        replacement.network.as_mut().ok_or_else(|| eyre!("replacement network missing"))?;
    let previous_id = *previous_network.swarm.local_peer_id();
    let replacement_id = *replacement_network.swarm.local_peer_id();
    let key = previous.config.key_config().primary_public_key();
    hub_network.process_kad_put_request(previous_id, previous_network.get_peer_record())?;
    assert!(receives_public_gossip(hub_network, &previous_id));
    let topic = IdentTopic::new("rotated-public-identity-mesh-oracle");
    advertise_subscription(hub_network, previous_network, &topic).await?;

    // The same BLS signer authenticates its replacement transport's own network identity.
    let mut info = replacement_network.node_record.info.clone();
    info.timestamp += 100;
    let chain_id = previous.config.network_config().libp2p_config().chain_id;
    let bytes = encode(&(b"telcoin-network/node-record/v1".as_slice(), chain_id, 0u8, 0u16, &info));
    let signature = previous.config.key_config().request_signature_direct(&bytes);
    let record = kad::Record {
        key: node_record_key(&key),
        value: encode(&NodeRecord { info, signature }),
        publisher: Some(replacement_id),
        expires: None,
    };
    hub_network.process_kad_put_request(replacement_id, record)?;
    assert!(hub_network.swarm.is_connected(&previous_id));
    assert!(!hub_network.swarm.behaviour().peer_manager.peer_is_confirmed_ordinary(&previous_id));
    assert!(!receives_public_gossip(hub_network, &previous_id));
    assert!(receives_public_gossip(hub_network, &replacement_id));
    assert_eq!(hub_network.swarm.behaviour().peer_manager.peer_to_bls(&replacement_id), Some(key));
    hub_network.swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
    assert!(
        hub_network
            .swarm
            .behaviour()
            .gossipsub
            .mesh_peers(&topic.hash())
            .any(|peer| *peer == previous_id),
        "displaced identity retained an explicit entry"
    );
    Ok(())
}

#[tokio::test]
async fn public_gossip_population_pruning_keeps_only_admitted_recipients() -> eyre::Result<()> {
    let mut value = serde_json::to_value(relay_config()?)?;
    value["public_peer_limit"] = serde_json::json!(1);
    let (mut hub, mut first, mut second, _task_manager) =
        connected_three_fixture_with_config(serde_json::from_value(value)?).await?;
    let hub_network = hub.network.as_mut().ok_or_else(|| eyre!("relay network missing"))?;
    let first_network = first.network.as_mut().ok_or_else(|| eyre!("first network missing"))?;
    let second_network = second.network.as_mut().ok_or_else(|| eyre!("second network missing"))?;
    let first_id = *first_network.swarm.local_peer_id();
    let second_id = *second_network.swarm.local_peer_id();
    hub_network.process_kad_put_request(first_id, first_network.get_peer_record())?;
    assert!(receives_public_gossip(hub_network, &first_id));
    hub_network.process_kad_put_request(second_id, second_network.get_peer_record())?;
    let manager = &hub_network.swarm.behaviour().peer_manager;
    assert_eq!(manager.connected_peers().len(), 1, "ordinary cap must still prune a peer");
    [first_id, second_id].into_iter().for_each(|peer| {
        assert!(
            hub_network.swarm.is_connected(&peer),
            "PX has not yet closed the physical transport"
        );
        assert!(!manager.peer_is_important(&peer));
        assert_eq!(receives_public_gossip(hub_network, &peer), manager.is_connected(&peer));
    });
    assert_eq!(hub_network.public_gossip_peers.as_ref().map(|peers| peers.ordinary.len()), Some(1));
    Ok(())
}

/// The publisher has no transport to the recipient, so accepted payloads must traverse the relay.
#[tokio::test]
async fn public_gossip_forwards_to_confirmed_ordinary_peer_outside_mesh() -> eyre::Result<()> {
    let (mut hub, mut recipient, mut publisher, _task_manager) = connected_three_fixture().await?;
    let hub_network = hub.network.as_mut().ok_or_else(|| eyre!("relay network missing"))?;
    let recipient_network =
        recipient.network.as_mut().ok_or_else(|| eyre!("recipient network missing"))?;
    let publisher_network =
        publisher.network.as_mut().ok_or_else(|| eyre!("publisher network missing"))?;
    let recipient_id = *recipient_network.swarm.local_peer_id();
    let publisher_id = *publisher_network.swarm.local_peer_id();
    let hub_id = *hub_network.swarm.local_peer_id();
    hub_network.process_kad_put_request(recipient_id, recipient_network.get_peer_record())?;
    hub_network.process_kad_put_request(publisher_id, publisher_network.get_peer_record())?;
    assert!(receives_public_gossip(hub_network, &recipient_id));
    assert!(hub_network.swarm.behaviour().peer_manager.peer_is_confirmed_ordinary(&recipient_id));
    assert!(!hub_network.swarm.behaviour().connection_limits.is_bypassed(&recipient_id));

    let topic = IdentTopic::new("ordinary-public-relay");
    hub_network.swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
    hub_network.authorized_publishers.insert(topic.hash().to_string(), None);
    recipient_network.swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
    recipient_network.authorized_publishers.insert(topic.hash().to_string(), None);
    let subscriptions = futures::stream::unfold(
        (&mut *hub_network, &mut *recipient_network, &mut *publisher_network),
        move |(hub, recipient, publisher)| async move {
            let outcome: NetworkResult<bool> = async {
                tokio::select! {
                    event = hub.swarm.select_next_some() => hub.process_event(event).await?,
                    event = recipient.swarm.select_next_some() => recipient.process_event(event).await?,
                    event = publisher.swarm.select_next_some() => publisher.process_event(event).await?,
                }
                let recipient_subscribed = hub.swarm.behaviour().gossipsub.all_peers()
                    .any(|(peer, topics)| *peer == recipient_id && !topics.is_empty());
                let hub_subscribed = publisher.swarm.behaviour().gossipsub.all_peers()
                    .any(|(peer, topics)| *peer == hub_id && !topics.is_empty());
                Ok(recipient_subscribed && hub_subscribed)
            }.await;
            Some((outcome, (hub, recipient, publisher)))
        },
    ).try_filter(|subscribed| futures::future::ready(*subscribed));
    let mut subscriptions = Box::pin(subscriptions);
    timeout(Duration::from_secs(5), subscriptions.try_next())
        .await??
        .ok_or_else(|| eyre!("subscription progress ended"))?;
    drop(subscriptions);
    assert!(!hub_network
        .swarm
        .behaviour()
        .gossipsub
        .mesh_peers(&topic.hash())
        .any(|peer| *peer == recipient_id));
    assert!(!publisher_network.swarm.is_connected(&recipient_id));
    let data = b"direct ordinary gossip payload".to_vec();
    publisher_network.swarm.behaviour_mut().gossipsub.publish(topic.clone(), data.clone())?;
    let delivery = futures::stream::unfold(
        (hub_network, recipient_network, publisher_network, &mut recipient._network_events),
        |(hub, recipient, publisher, events)| async move {
            let outcome: NetworkResult<Option<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>> = tokio::select! {
                event = events.recv() => event.map(Some).ok_or(NetworkError::Disconnected),
                event = hub.swarm.select_next_some() => hub.process_event(event).await.map(|()| None),
                event = recipient.swarm.select_next_some() => recipient.process_event(event).await.map(|()| None),
                event = publisher.swarm.select_next_some() => publisher.process_event(event).await.map(|()| None),
            };
            Some((outcome, (hub, recipient, publisher, events)))
        },
    ).filter_map(|event| futures::future::ready(event.transpose()));
    let mut delivery = Box::pin(delivery);
    let received = timeout(Duration::from_secs(5), delivery.try_next())
        .await??
        .ok_or_else(|| eyre!("delivery progress ended"))?;
    let payload = match received {
        NetworkEvent::Gossip(payload) => Ok(payload),
        NetworkEvent::Request { .. }
        | NetworkEvent::Error(..)
        | NetworkEvent::InboundStream { .. } => Err(eyre!("expected relayed gossip")),
    }?;
    assert_eq!(payload.message.data, data);
    assert_eq!(payload.message.source, Some(publisher_id));
    assert_eq!(payload.receipt.map(|receipt| receipt.propagation_source), Some(hub_id));
    Ok(())
}
