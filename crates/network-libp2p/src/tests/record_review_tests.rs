//! Regression coverage for record retrieval review feedback.

use super::*;

/// A solicited record shed by the shared PUT budget is retried after that window expires.
#[tokio::test(start_paused = true)]
async fn shed_record_response_remains_retryable() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let peer = *peer2.network.swarm.local_peer_id();
    let key = peer2.config.key_config().primary_public_key();
    // Retain the committee row without changing the source's ordinary PUT budget.
    network.swarm.behaviour_mut().kademlia.store_mut().retain_committees([key])?;
    (0..crate::peers::MAX_PUT_RECORDS_PER_WINDOW).for_each(|_| {
        assert!(matches!(
            network.swarm.behaviour_mut().peer_manager.put_record_rate_limited(peer),
            PutRecordRate::Allowed
        ));
    });
    assert!(network.record_exchange.allow_request(peer));
    let request = network.swarm.behaviour_mut().record_exchange.send_request(&peer, ());
    network.record_exchange.track(peer, request);
    network.process_record_response(
        peer,
        request,
        Some((key, peer2.network.node_record.clone())),
    )?;
    assert!(network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&node_record_key(&key))
        .is_none());
    assert_eq!(network.record_exchange.counts(), (0, 1));
    assert!(!network.swarm.behaviour().peer_manager.peer_banned(&peer));

    tokio::time::advance(Duration::from_secs(61)).await;
    assert!(network.record_exchange.take_deferred().contains(&peer));
    assert!(network.record_exchange.allow_request(peer));
    let retry = network.swarm.behaviour_mut().record_exchange.send_request(&peer, ());
    network.record_exchange.track(peer, retry);
    network.process_record_response(peer, retry, Some((key, peer2.network.node_record)))?;
    assert!(network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&node_record_key(&key))
        .is_some());
    assert_eq!(network.record_exchange.counts(), (0, 0));
    Ok(())
}

/// Codec violations are scored, while local stream capacity and transport errors only retry.
#[tokio::test]
async fn record_failures_score_only_invalid_data() -> eyre::Result<()> {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    [ErrorKind::Other, ErrorKind::UnexpectedEof, ErrorKind::InvalidData].into_iter().try_for_each(
        |kind| {
            let peer = register_untrusted_put_record_source(&mut network)?;
            let before = network.swarm.behaviour().peer_manager.peer_score(&peer);
            assert!(network.record_exchange.allow_request(peer));
            let request = network.swarm.behaviour_mut().record_exchange.send_request(&peer, ());
            network.record_exchange.track(peer, request);
            network.process_record_exchange_event(ReqResEvent::OutboundFailure {
                peer,
                connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                request_id: request,
                error: ReqResOutboundFailure::Io(std::io::Error::new(kind, "test failure")),
            })?;
            let after = network.swarm.behaviour().peer_manager.peer_score(&peer);
            let deferred = network.record_exchange.take_deferred().contains(&peer);
            if kind == ErrorKind::InvalidData {
                assert!(after.zip(before).is_some_and(|(after, before)| after < before));
                assert!(!deferred);
            } else {
                assert_eq!(after, before);
                assert!(deferred);
            }
            eyre::Ok(())
        },
    )
}

/// A confirmed legacy identity outside the tracked committees cannot start a wasted DHT query.
#[tokio::test]
async fn legacy_record_lookup_requires_committee_membership() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let peer = *peer2.network.swarm.local_peer_id();
    let key = peer2.config.key_config().primary_public_key();
    network.swarm.behaviour_mut().peer_manager.update_committees(
        Default::default(),
        Default::default(),
        Default::default(),
    );
    network.process_kad_put_request(peer, peer2.network.get_peer_record())?;
    assert_eq!(network.swarm.behaviour().peer_manager.peer_to_bls(&peer), Some(key));
    network.request_legacy_record(peer);
    assert!(network.kad_record_queries.is_empty());
    network.swarm.behaviour_mut().peer_manager.update_committees(
        [key].into_iter().collect(),
        Default::default(),
        Default::default(),
    );
    network.request_legacy_record(peer);
    assert_eq!(network.kad_record_queries.len(), 1);
    Ok(())
}

/// Byte equality with persisted data never substitutes for signature verification in this process.
#[tokio::test]
async fn persisted_record_does_not_prime_verification_cache() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let mut record = peer2.network.get_peer_record();
    // The direct write models a retained committee row loaded before this process verifies it.
    network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .retain_committees([peer2.config.key_config().primary_public_key()])?;
    let mut invalid = peer2.network.node_record.clone();
    invalid.info.timestamp = invalid.info.timestamp.saturating_add(1);
    record.value = encode(&invalid);
    network.swarm.behaviour_mut().kademlia.store_mut().put(record.clone())?;
    assert!(network.peer_record_valid(&record).is_none());
    assert!(network.verified_peer_records.is_empty());
    Ok(())
}

/// Cached verification still binds the exact bytes, BLS key and publisher on every reception.
#[tokio::test]
async fn cached_record_preserves_identity_and_signature_checks() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let peer = *peer2.network.swarm.local_peer_id();
    let record = peer2.network.get_peer_record();
    network.process_kad_put_request(peer, record.clone())?;
    assert!(network.peer_record_valid(&record).is_some());
    let mut wrong_publisher = record.clone();
    wrong_publisher.publisher = Some(PeerId::random());
    assert!(network.peer_record_valid(&wrong_publisher).is_none());
    let mut wrong_key = record.clone();
    wrong_key.key = node_record_key(&network.key_config.primary_public_key());
    assert!(network.peer_record_valid(&wrong_key).is_none());
    let mut changed = peer2.network.node_record.clone();
    changed.info.timestamp = changed.info.timestamp.saturating_add(1);
    let mut changed_bytes = record;
    changed_bytes.value = encode(&changed);
    assert!(network.peer_record_valid(&changed_bytes).is_none());
    Ok(())
}
