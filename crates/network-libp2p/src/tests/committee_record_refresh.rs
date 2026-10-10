//! Regression tests for cached committee record convergence and bounded endpoint recovery.

use super::*;
use futures::FutureExt;
use std::task::{Context, Poll};
use tn_config::KeyConfig;

/// Poll the production peer behavior's timer and collect only its generated application events.
fn peer_manager_events(
    network: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
) -> Vec<PeerEvent> {
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    std::iter::from_fn(|| match network.swarm.behaviour_mut().peer_manager.poll(&mut context) {
        Poll::Ready(action) => Some(action),
        Poll::Pending => None,
    })
    .fold(Vec::new(), |mut events, action| {
        let _ = action.map_out(|event| events.push(event));
        events
    })
}

/// Observe the actual library queries, including silent maintenance replication.
fn observe_publication_queries(
    network: &ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    own: &kad::RecordKey,
    third_party: &kad::RecordKey,
    own_queries: &mut HashSet<QueryId>,
    third_party_queries: &mut HashSet<QueryId>,
) {
    own_queries.extend(
        network
            .swarm
            .behaviour()
            .kademlia
            .iter_queries()
            .filter(|query| {
                matches!(query.info(), kad::QueryInfo::PutRecord { record, .. }
            if &record.key == own)
            })
            .map(|query| query.id()),
    );
    third_party_queries.extend(
        network
            .swarm
            .behaviour()
            .kademlia
            .iter_queries()
            .filter(|query| {
                matches!(query.info(), kad::QueryInfo::PutRecord { record, .. }
            if &record.key == third_party)
            })
            .map(|query| query.id()),
    );
}

/// Sign a candidate using the same domain encoding as the network's verifier.
fn signed_record(
    original: &kad::Record,
    info: NetworkInfo,
    domain: RecordDomain,
    key_config: &KeyConfig,
) -> kad::Record {
    let (role, worker) = match domain.network_type() {
        NetworkType::Primary => (0_u8, 0_u16),
        NetworkType::Worker(worker) => (1_u8, worker),
    };
    let bytes = encode(&(
        b"telcoin-network/node-record/v1".as_slice(),
        domain.chain_id(),
        role,
        worker,
        &info,
    ));
    let signature = key_config.request_signature_direct(&bytes);
    kad::Record {
        value: encode(&NodeRecord { info, signature }),
        expires: Some(std::time::Instant::now() + Duration::from_secs(120)),
        ..original.clone()
    }
}

/// Feed a terminal response through the production signature, publisher, and query gates.
fn found_record(
    network: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
    query_id: QueryId,
    record: kad::Record,
    peer: PeerId,
) -> eyre::Result<()> {
    network.process_kad_event(kad::Event::OutboundQueryProgressed {
        id: query_id,
        result: kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
            record,
            peer: Some(peer),
        }))),
        stats: kad::QueryStats::empty(),
        step: kad::ProgressStep { count: NonZeroUsize::MIN, last: true },
    })?;
    Ok(())
}

/// An observer with a cached URL resolves a replacement on this actual swarm role.
async fn cached_record_converges(network_type: NetworkType) -> eyre::Result<()> {
    let TestTypes { peer1, peer2, _task_manager } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let (events, _receiver) = mpsc::channel(10);
    let mut network = ConsensusNetwork::new(
        peer1.config.network_config(),
        events,
        peer1.config.key_config().clone(),
        peer1.config.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        _task_manager.get_spawner(),
        network_type,
        create_multiaddr(None),
        None,
    )?;
    let original = peer2.network.get_peer_record();
    let authority = BlsPublicKey::from_literal_bytes(original.key.as_ref())
        .map_err(|error| eyre!("record BLS key: {error:?}"))?;
    let publisher = original.publisher.ok_or_else(|| eyre!("record publisher"))?;
    let old_rpc = RpcInfo { http: "https://old.example:8545".parse()?, ws: None };
    let new_rpc = RpcInfo { http: "https://new.example:8545".parse()?, ws: None };
    let mut old_info = peer2.network.node_record.info.clone();
    old_info.rpc = Some(old_rpc.clone());
    old_info.timestamp = old_info.timestamp.saturating_sub(10);
    let old_record = signed_record(
        &original,
        old_info.clone(),
        network.record_domain,
        peer2.config.key_config(),
    );
    network.swarm.behaviour_mut().peer_manager.update_committees(
        HashSet::new(),
        HashSet::from([authority]),
        HashSet::new(),
    );
    // The store admits only owned keys, so the refreshed authority must own its record row.
    network.swarm.behaviour_mut().kademlia.store_mut().retain_committees([authority])?;
    network.process_kad_put_request(publisher, old_record.clone())?;
    assert_eq!(network.swarm.behaviour().peer_manager.get_rpc(&authority), Some(old_rpc.clone()));
    peer_manager_events(&mut network);

    tokio::time::advance(Duration::from_secs(60)).await;
    let events = peer_manager_events(&mut network);
    events.into_iter().try_for_each(|event| network.process_peer_manager_event(event))?;
    let query_id = *network
        .kad_record_queries
        .iter()
        .find(|(_, query)| query.query.request == authority)
        .map(|(id, _)| id)
        .ok_or_else(|| eyre!("cached member must be refreshed"))?;

    // Verification still precedes promotion: an unbound publisher cannot change the cache.
    let mut new_info = old_info.clone();
    new_info.timestamp += 1;
    new_info.rpc = Some(new_rpc.clone());
    let new_record =
        signed_record(&original, new_info, network.record_domain, peer2.config.key_config());
    let mut invalid = new_record.clone();
    invalid.publisher = Some(PeerId::random());
    found_record(&mut network, query_id, invalid, PeerId::random())?;
    assert_eq!(network.swarm.behaviour().peer_manager.get_rpc(&authority), Some(old_rpc));
    assert!(network.kad_record_queries.is_empty());

    tokio::time::advance(Duration::from_secs(30)).await;
    network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })?;
    let query_id =
        *network.kad_record_queries.keys().next().ok_or_else(|| eyre!("retry must be rearmed"))?;
    found_record(&mut network, query_id, new_record.clone(), publisher)?;
    assert_eq!(
        network.swarm.behaviour_mut().peer_manager.current_committee_rpcs(),
        vec![(authority, new_rpc.clone())]
    );
    let retained = network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&new_record.key)
        .ok_or_else(|| eyre!("verified query result must be retained"))?
        .into_owned();
    assert_eq!(retained.value, new_record.value);
    assert_eq!(retained.publisher, new_record.publisher);
    let retained_expiry = retained.expires.ok_or_else(|| eyre!("finite retained expiry"))?;
    let supplied_expiry = new_record.expires.ok_or_else(|| eyre!("finite supplied expiry"))?;
    // The database converts the monotonic deadline through a wall-clock timestamp.
    assert!(retained_expiry
        .checked_duration_since(supplied_expiry)
        .or_else(|| supplied_expiry.checked_duration_since(retained_expiry))
        .is_some_and(|drift| drift < Duration::from_millis(1)));

    // An older verified response cannot roll the mapping or stored bytes back.
    tokio::time::advance(Duration::from_secs(30)).await;
    network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })?;
    let query_id = *network
        .kad_record_queries
        .keys()
        .next()
        .ok_or_else(|| eyre!("second retry must be rearmed"))?;
    found_record(&mut network, query_id, old_record, publisher)?;
    assert_eq!(network.swarm.behaviour().peer_manager.get_rpc(&authority), Some(new_rpc));
    assert_eq!(
        network
            .swarm
            .behaviour_mut()
            .kademlia
            .store_mut()
            .get(&new_record.key)
            .ok_or_else(|| eyre!("retained record"))?
            .value,
        new_record.value
    );
    Ok(())
}

/// Cached primary records refresh without removing the old mapping or restarting.
#[tokio::test(start_paused = true)]
async fn committee_record_primary_converges() -> eyre::Result<()> {
    cached_record_converges(NetworkType::Primary).await
}

/// The first worker independently refreshes records in its worker domain and store.
#[tokio::test(start_paused = true)]
async fn committee_record_worker_zero_converges() -> eyre::Result<()> {
    cached_record_converges(NetworkType::Worker(0)).await
}

/// An additional worker independently refreshes records in its own domain and store.
#[tokio::test(start_paused = true)]
async fn committee_record_worker_one_converges() -> eyre::Result<()> {
    cached_record_converges(NetworkType::Worker(1)).await
}

/// Repeated failures coalesce during a lookup and cooldown, and rotation releases the budget.
#[tokio::test(start_paused = true)]
async fn committee_record_retry_is_bounded() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, _task_manager } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let authority = BlsPublicKey::from_literal_bytes(peer2.network.get_peer_record().key.as_ref())
        .map_err(|error| eyre!("record BLS key: {error:?}"))?;
    network.process_command(NetworkCommand::UpdateCommittees {
        previous: HashSet::new(),
        current: HashSet::from([authority]),
        next: HashSet::new(),
    })?;
    (0..100).try_for_each(|_| {
        network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })
    })?;
    assert_eq!(network.kad_record_queries.len(), 1);
    assert_eq!(network.committee_record_attempts.len(), 1);
    let query_id =
        *network.kad_record_queries.keys().next().ok_or_else(|| eyre!("pending query"))?;
    network.close_kad_query(&query_id);
    (0..100).try_for_each(|_| {
        network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })
    })?;
    assert!(network.kad_record_queries.is_empty(), "fast failures must respect cooldown");
    tokio::time::advance(Duration::from_secs(30)).await;
    network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })?;
    assert_eq!(network.kad_record_queries.len(), 1);
    network.process_command(NetworkCommand::UpdateCommittees {
        previous: HashSet::new(),
        current: HashSet::new(),
        next: HashSet::new(),
    })?;
    assert!(network.committee_record_attempts.is_empty());
    network.process_command(NetworkCommand::RefreshCommitteeRecord { authority })?;
    assert!(network.committee_record_attempts.is_empty(), "non-members cannot consume retry state");
    Ok(())
}

/// The dedicated timer publishes own records without periodically replicating committee data.
#[tokio::test]
async fn committee_record_retention_does_not_enable_third_party_replication() -> eyre::Result<()> {
    let mut config = NetworkConfig::default();
    config.libp2p_config_mut().kad_publication_interval = Duration::from_millis(50);
    config.libp2p_config_mut().kad_replication_interval = Duration::from_millis(10);
    let TestTypes { peer1, peer2, _task_manager } =
        create_test_types_with_config::<TestWorkerRequest, TestWorkerResponse>(config);
    let mut network = peer1.network;
    // Keep a routing target pending so silent maintenance queries remain observable.
    let blackhole = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let address: Multiaddr =
        format!("/ip4/127.0.0.1/udp/{}/quic-v1", blackhole.local_addr()?.port()).parse()?;
    network.swarm.behaviour_mut().kademlia.add_address(&PeerId::random(), address);
    network.provide_our_data();
    let own = network.get_peer_record().key;
    let record = peer2.network.get_peer_record();
    let third_party = record.key.clone();
    let authority = BlsPublicKey::from_literal_bytes(third_party.as_ref())
        .map_err(|error| eyre!("record BLS key: {error:?}"))?;
    network.swarm.behaviour_mut().peer_manager.update_committees(
        HashSet::new(),
        HashSet::from([authority]),
        HashSet::new(),
    );
    // The store admits only owned keys, so the refreshed authority must own its record row.
    network.swarm.behaviour_mut().kademlia.store_mut().retain_committees([authority])?;
    // Seed the retained third-party copy the way the GetRecord handler stores it: finite expiry.
    let expires = Some(std::time::Instant::now() + network.config.kad_record_ttl);
    libp2p::kad::store::RecordStore::put(
        network.swarm.behaviour_mut().kademlia.store_mut(),
        libp2p::kad::Record { expires, ..record },
    )?;
    let interval = network.config.kad_publication_interval;
    let mut own_refresh =
        tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    own_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let state = std::cell::RefCell::new((network, own_refresh, HashSet::new(), HashSet::new()));
    wait_until(Duration::from_secs(5), "own publication with library replication disabled", || {
        let mut state = state.borrow_mut();
        let (network, own_refresh, own_queries, third_party_queries) = &mut *state;
        std::iter::from_fn(|| own_refresh.tick().now_or_never())
            .for_each(|_| network.refresh_own_record());
        observe_publication_queries(network, &own, &third_party, own_queries, third_party_queries);
        std::iter::from_fn(|| {
            let event = network.swarm.next().now_or_never().flatten();
            observe_publication_queries(
                network,
                &own,
                &third_party,
                own_queries,
                third_party_queries,
            );
            event
        })
        .for_each(drop);
        let complete = own_queries.len() >= 2;
        async move { Ok(complete) }
    })
    .await?;
    let (_, _, own_queries, third_party_queries) = state.into_inner();
    assert!(own_queries.len() >= 2, "dedicated own-record publication must remain active");
    assert!(third_party_queries.is_empty(), "retention must not enlist a periodic replicator");
    Ok(())
}
