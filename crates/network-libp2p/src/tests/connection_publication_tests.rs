use super::*;

/// A fresh receiver with the same identity must get a signed record while its older physical
/// connection is still live. No periodic publication or application lookup can rescue this test.
#[tokio::test]
async fn restarted_identity_receives_record_with_old_connection_alive() -> eyre::Result<()> {
    use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};

    let TestTypes { mut peer1, mut peer2, _task_manager } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let publisher_id = *peer1.network.swarm.local_peer_id();
    let receiver_id = *peer2.network.swarm.local_peer_id();
    let publisher_key = peer1.config.key_config().primary_public_key();
    let record_key = node_record_key(&publisher_key);
    peer1.network.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
    peer2.network.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
    peer1.network.swarm.listen_on(peer1.config.primary_address())?;
    peer2.network.swarm.listen_on(peer2.config.primary_address())?;
    peer1.network.swarm.dial(peer2.config.primary_address())?;

    let first_delivery = futures::stream::unfold(
        (&mut peer1.network, &mut peer2.network),
        |(publisher, old)| {
            let record_key = record_key.clone();
            async move {
                let result = tokio::select! {
                    event = publisher.swarm.select_next_some() => publisher.process_event(event).await,
                    event = old.swarm.select_next_some() => old.process_event(event).await,
                };
                let ready = old.swarm.behaviour_mut().kademlia.store_mut().get(&record_key).is_some();
                Some((result.map(|()| ready), (publisher, old)))
            }
        },
    ).filter(|result| futures::future::ready(result.as_ref().map_or(true, |ready| *ready)));
    let mut first_delivery = Box::pin(first_delivery);
    timeout(Duration::from_secs(5), first_delivery.next())
        .await?
        .ok_or_else(|| eyre!("first record delivery ended"))??;
    drop(first_delivery);
    assert!(peer2.network.swarm.is_connected(&publisher_id));
    assert_eq!(peer1.network.swarm.network_info().connection_counters().num_established(), 1);

    let (sender, _events) = mpsc::channel(10);
    let mut restarted =
        ConsensusNetwork::<TestWorkerRequest, TestWorkerResponse, MemDatabase, _>::new(
            peer2.config.network_config(),
            sender,
            peer2.config.key_config().clone(),
            peer2.config.key_config().primary_network_keypair().clone(),
            MemDatabase::default(),
            _task_manager.get_spawner(),
            NetworkType::Primary,
            peer2.config.primary_address(),
            None,
        )?;
    assert_eq!(*restarted.swarm.local_peer_id(), receiver_id);
    assert!(restarted.swarm.behaviour_mut().kademlia.store_mut().get(&record_key).is_none());
    restarted.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
    restarted.swarm.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
    let listening = futures::stream::unfold(&mut restarted, |network| async move {
        let event = network.swarm.select_next_some().await;
        let address = if let SwarmEvent::NewListenAddr { address, .. } = &event {
            Some(address.clone())
        } else {
            None
        };
        let result = network.process_event(event).await.map(|()| address);
        Some((result, network))
    })
    .filter_map(|result| futures::future::ready(result.transpose()));
    let mut listening = Box::pin(listening);
    let new_address = timeout(Duration::from_secs(5), listening.next())
        .await?
        .ok_or_else(|| eyre!("replacement listener ended"))??;
    drop(listening);
    peer1.network.swarm.dial(
        DialOpts::peer_id(receiver_id)
            .addresses(vec![new_address])
            .condition(PeerCondition::Always)
            .build(),
    )?;

    let second_delivery = futures::stream::unfold(
        (&mut peer1.network, &mut peer2.network, &mut restarted),
        |(publisher, old, replacement)| {
            let record_key = record_key.clone();
            async move {
                let result = tokio::select! {
                    event = publisher.swarm.select_next_some() => publisher.process_event(event).await,
                    event = old.swarm.select_next_some() => old.process_event(event).await,
                    event = replacement.swarm.select_next_some() => replacement.process_event(event).await,
                };
                let record = replacement.swarm.behaviour_mut().kademlia.store_mut()
                    .get(&record_key).map(|record| record.into_owned());
                let valid = record.as_ref().and_then(|record| replacement.peer_record_valid(record))
                    .is_some_and(|(key, _record)| key == publisher_key);
                let ready = valid
                    && publisher.swarm.network_info().connection_counters().num_established() == 2
                    && old.swarm.is_connected(&publisher_id);
                Some((result.map(|()| ready), (publisher, old, replacement)))
            }
        },
    ).filter(|result| futures::future::ready(result.as_ref().map_or(true, |ready| *ready)));
    let mut second_delivery = Box::pin(second_delivery);
    timeout(Duration::from_secs(5), second_delivery.next())
        .await?
        .ok_or_else(|| eyre!("replacement record delivery ended"))??;
    drop(second_delivery);
    assert!(peer1.network.swarm.is_connected(&receiver_id));
    assert!(peer2.network.swarm.is_connected(&publisher_id));
    assert!(restarted.swarm.is_connected(&publisher_id));
    assert_eq!(peer1.network.swarm.network_info().connection_counters().num_established(), 2);
    Ok(())
}
