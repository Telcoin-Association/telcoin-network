//! Tests networking using libp2p between peers.

use super::*;
use crate::common::{
    create_multiaddr, TestPrimaryRequest, TestPrimaryResponse, TestWorkerRequest,
    TestWorkerResponse, TEST_HEARTBEAT_INTERVAL,
};
use assert_matches::assert_matches;
use eyre::eyre;
use rand::{rngs::StdRng, SeedableRng as _};
use std::num::NonZeroUsize;
use tn_config::{ConsensusConfig, NetworkConfig};
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::{BlsKeypair, Certificate, Header, TaskManager};
use tokio::{sync::mpsc, time::timeout};

/// Test topic for gossip.
const TEST_TOPIC: &str = "test-topic";

/// Helper function to create peers.
fn create_test_peers<Req: TNMessage, Res: TNMessage>(
    num_peers: NonZeroUsize,
    network_config: Option<NetworkConfig>,
) -> (TestPeer<Req, Res>, Vec<TestPeer<Req, Res>>, TaskManager) {
    let network_config = network_config.unwrap_or_default();

    let all_nodes = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(num_peers)
        .with_network_config(network_config)
        .build();
    let authorities = all_nodes.authorities();
    let task_manager = TaskManager::default();
    let mut peers: Vec<_> = authorities
        .map(|a| {
            let config = a.consensus_config();
            let (tx, network_events) = mpsc::channel(10);
            let network_key = config.key_config().primary_network_keypair().clone();
            let db = MemDatabase::default();
            let network = ConsensusNetwork::<
                Req,
                Res,
                MemDatabase,
                mpsc::Sender<NetworkEvent<Req, Res>>,
            >::new(
                config.network_config(),
                tx,
                config.key_config().clone(),
                network_key,
                db,
                task_manager.get_spawner(),
                NetworkType::Primary,
                config.primary_address(),
                None,
            )
            .expect("peer1 network created");

            let network_handle = network.network_handle();
            TestPeer {
                config,
                _network_events: network_events,
                network_handle,
                network: Some(network),
            }
        })
        .collect();

    let target = peers.remove(0);

    // return task manager to prevent drop
    (target, peers, task_manager)
}

/// ConsensusNetwork backed by in-memory kad store.
type ConsensusNetworkMemoryDB<Req, Res> =
    ConsensusNetwork<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>;

/// A peer on TN
struct TestPeer<Req, Res, DB = MemDatabase>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Peer's node config.
    config: ConsensusConfig<DB>,
    /// Receiver for network events.
    _network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// The network task.
    network: Option<ConsensusNetworkMemoryDB<Req, Res>>,
}
/// A peer on TN
struct NetworkPeer<Req, Res, DB = MemDatabase>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Peer's node config.
    config: ConsensusConfig<DB>,
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// The network task.
    network: ConsensusNetwork<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>,
}

/// The type for holding testng components.
struct TestTypes<Req, Res, DB = MemDatabase>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// The first authority in the committee.
    peer1: NetworkPeer<Req, Res, DB>,
    /// The second authority in the committee.
    peer2: NetworkPeer<Req, Res, DB>,
    /// The owned task manager to prevent dropping.
    _task_manager: TaskManager,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types<Req, Res>() -> TestTypes<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    create_test_types_with_config::<Req, Res>(NetworkConfig::default())
}

/// Variant of [`create_test_types`] that lets a caller customize [`NetworkConfig`]
/// (e.g. shrink `kad_record_ttl` for a regression test).
///
/// The heartbeat-interval override applied by `create_test_types` is preserved here
/// so callers can opt into a custom kad TTL without having to also remember the
/// short heartbeat the peer manager needs in tests.
fn create_test_types_with_config<Req, Res>(mut network_config: NetworkConfig) -> TestTypes<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    // custom network config with short heartbeat interval for peer manager
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let (tx1, network_events_1) = mpsc::channel(10);
    let (tx2, network_events_2) = mpsc::channel(10);
    let task_manager = TaskManager::default();

    // peer1
    let network_key_1 = config_1.key_config().primary_network_keypair().clone();
    let peer1_network =
        ConsensusNetwork::<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>::new(
            config_1.network_config(),
            tx1,
            config_1.key_config().clone(),
            network_key_1,
            MemDatabase::default(),
            task_manager.get_spawner(),
            NetworkType::Primary,
            config_1.primary_address(),
            None,
        )
        .expect("peer1 network created");
    let network_handle_1 = peer1_network.network_handle();
    let peer1 = NetworkPeer {
        config: config_1,
        network_events: network_events_1,
        network_handle: network_handle_1,
        network: peer1_network,
    };

    // peer2
    let network_key_2 = config_2.key_config().primary_network_keypair().clone();
    let peer2_network =
        ConsensusNetwork::<Req, Res, MemDatabase, mpsc::Sender<NetworkEvent<Req, Res>>>::new(
            config_2.network_config(),
            tx2,
            config_2.key_config().clone(),
            network_key_2,
            MemDatabase::default(),
            task_manager.get_spawner(),
            NetworkType::Primary,
            config_2.primary_address(),
            None,
        )
        .expect("peer2 network created");
    let network_handle_2 = peer2_network.network_handle();
    let peer2 = NetworkPeer {
        config: config_2,
        network_events: network_events_2,
        network_handle: network_handle_2,
        network: peer2_network,
    };

    TestTypes { peer1, peer2, _task_manager: task_manager }
}

/// Wait for a peer's BLS key to be discovered via kademlia.
///
/// Polls `connected_peers()` until the expected BLS key appears,
/// indicating the kademlia record exchange has completed and the
/// `peer_to_bls` mapping is established.
async fn wait_for_peer_discovery<Req: TNMessage, Res: TNMessage>(
    handle: &NetworkHandle<Req, Res>,
    expected_bls: BlsPublicKey,
    timeout_duration: Duration,
) -> eyre::Result<()> {
    wait_until(timeout_duration, "peer BLS key discovery via kademlia", move || async move {
        Ok(handle.connected_peers().await?.contains(&expected_bls))
    })
    .await
}

#[tokio::test]
async fn test_valid_req_restt() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network_events: mut network_events_2,
        network,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);
    let batch_res = TestWorkerResponse::MissingBatches { batches: vec![missing_block] };

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // send request and wait for response
    let max_time = Duration::from_secs(5);

    // wait for peer2 to discover peer1's BLS key via kademlia
    wait_for_peer_discovery(&peer2, config_1.key_config().primary_public_key(), max_time).await?;

    let response_from_peer =
        peer1.send_request(batch_req.clone(), config_2.key_config().primary_public_key()).await?;
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, channel, .. } = event {
        assert_eq!(request, batch_req);

        // send response
        peer2.send_response(batch_res.clone(), channel).await?;
    } else {
        panic!("unexpected network event received");
    }

    // expect response
    let NetworkResponseMessage { peer: _, result: response } =
        timeout(max_time, response_from_peer).await?.expect("outbound id recv")?;
    assert_eq!(response, batch_res);

    Ok(())
}

#[tokio::test]
async fn test_valid_req_res_connection_closed_cleanup() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    let peer2_network_task = tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;
    let peer2_id = peer2.local_peer_id().await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // wait for peer2 to discover peer1's BLS key via kademlia
    wait_for_peer_discovery(
        &peer2,
        config_1.key_config().primary_public_key(),
        Duration::from_secs(5),
    )
    .await?;

    // send request and wait for response
    let _reply = peer1.send_request_direct(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), 1);

    // simulate crashed peer 2
    peer2_network_task.abort();
    assert!(peer2_network_task.await.unwrap_err().is_cancelled());

    // wait for peer1 to observe the disconnect and drain its pending request,
    // instead of guessing at how long the teardown plus outbound-failure delivery takes
    let handle = &peer1;
    wait_until(
        Duration::from_secs(5),
        "peer1 clears its pending request after peer2 disconnects",
        move || async move { Ok(handle.get_pending_request_count().await? == 0) },
    )
    .await?;

    // peer1 removes pending requests
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test]
async fn test_valid_req_res_inbound_failure() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    let peer1_network_task = tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let NetworkPeer {
        config: config_2,
        network_handle: peer2,
        network_events: mut network_events_2,
        network,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;
    let peer2_id = peer2.local_peer_id().await?;

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.primary_networkkey(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // send request and wait for response
    let max_time = Duration::from_secs(5);

    // wait for peer2 to discover peer1's BLS key via kademlia
    wait_for_peer_discovery(&peer2, config_1.key_config().primary_public_key(), max_time).await?;

    let _response = peer1.send_request_direct(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), 1);

    // wait for peer2 to receive req
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, cancel, .. } = event {
        assert_eq!(request, batch_req);

        // peer 1 crashes after making request
        peer1_network_task.abort();
        assert!(peer1_network_task.await.unwrap_err().is_cancelled());

        tokio::task::yield_now().await;
        timeout(Duration::from_secs(2), cancel).await?.expect("first network event received");
        assert_matches!((), ());
    } else {
        panic!("unexpected network event received");
    }

    // InboundFailure::Io(Kind(UnexpectedEof))
    Ok(())
}

#[tokio::test]
async fn test_outbound_failure_malicious_request() -> eyre::Result<()> {
    // start malicious peer1 network
    //
    // although these are valid req/res types, they are incorrect for the honest peer's
    // "worker" network
    let TestTypes { peer1, .. } = create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();
    let NetworkPeer { config: config_1, network_handle: malicious_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest peer2 network
    let TestTypes { peer2, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_2, network_handle: honest_peer, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    malicious_peer.start_listening(config_1.primary_address()).await?;
    honest_peer.start_listening(config_2.primary_address()).await?;

    let honest_peer_id = honest_peer.local_peer_id().await?;
    let honest_peer_addr = config_2.primary_address();
    let honest_peer_net = config_2.primary_networkkey();

    // this type already impl `TNMessage` but this could be incorrect message type
    let malicious_msg = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };

    // dial honest peer
    let honest_bls = config_2.key_config().primary_public_key();
    malicious_peer.add_explicit_peer(honest_bls, honest_peer_net, honest_peer_addr.clone()).await?;
    malicious_peer.dial_by_bls(honest_bls).await?;

    // wait for honest peer to discover malicious peer's BLS key via kademlia
    wait_for_peer_discovery(
        &honest_peer,
        config_1.key_config().primary_public_key(),
        Duration::from_secs(5),
    )
    .await?;

    // sleep for heartbeat
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Capture the malicious peer's view of the honest responder before the request.
    //
    // The contract under test: when a malformed *request* arrives at the responder,
    // libp2p's `read_request` fails inside the codec and the responder drops the
    // substream WITHOUT emitting `InboundFailure::Io` (see libp2p request-response
    // 0.29 `lib.rs:1011-1014`). From the requester's side the failure surfaces as
    // either `OutboundFailure::ConnectionClosed` or `OutboundFailure::Io` with a
    // transport-flap `ErrorKind` (e.g. `UnexpectedEof`) — both are no-penalty
    // under the new policy.
    //
    // The `Penalty::Medium` codec-violation path requires the malformed bytes to
    // be a *response* (read_response on the requester returns `io::Error::other`).
    // That is exercised separately by `test_outbound_failure_malicious_response`.
    let malicious_view_before = malicious_peer.peer_score(honest_peer_id).await?.unwrap();

    // honest peer returns `OutboundFailure` error
    let response_from_peer = malicious_peer.send_request(malicious_msg, honest_bls).await?;
    let res = timeout(Duration::from_secs(2), response_from_peer)
        .await?
        .expect("first network event received");

    assert_matches!(res, Err(NetworkError::Outbound(_)));

    // Allow time for any penalty propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Responder's score (from the requester's view) is unchanged: a transport-level
    // OutboundFailure for a malformed-request scenario must not penalize the
    // responder. Otherwise a peer who cannot satisfy a request would be banned by
    // every requester — the ban-cascade that breaks observer joins on WAN.
    let malicious_view_after = malicious_peer.peer_score(honest_peer_id).await?.unwrap();
    assert_eq!(
        malicious_view_before, malicious_view_after,
        "requester must not penalize responder on transport-level OutboundFailure (before={malicious_view_before}, after={malicious_view_after})"
    );

    // Note: honest peer's penalty of the malicious requester via InboundFailure is
    // NOT exercised here. libp2p request-response 0.29 does not emit
    // InboundFailure::Io when read_request fails — see lib.rs:1011-1014.
    // Dispatch-layer coverage for that path is tracked in Issue #250.

    Ok(())
}

#[tokio::test]
async fn test_outbound_failure_malicious_response() -> eyre::Result<()> {
    // honest peer 1
    let TestTypes { peer1, .. } = create_test_types::<TestPrimaryRequest, TestPrimaryResponse>();
    let NetworkPeer { config: config_1, network_handle: honest_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // malicious peer2
    //
    // although these are honest req/res types, they are incorrect for the honest peer's
    // "primary" network this allows the network to receive "correct" messages and
    // respond with bad messages
    let TestTypes { peer2, .. } = create_test_types::<TestPrimaryRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: config_2,
        network_handle: malicious_peer,
        network,
        network_events: mut network_events_2,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    honest_peer.start_listening(config_1.primary_address()).await?;
    malicious_peer.start_listening(config_2.primary_address()).await?;
    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let malicious_peer_addr =
        malicious_peer.listeners().await?.first().expect("malicious_peer listen addr").clone();

    // dial malicious_peer
    let mal_bls = config_2.key_config().primary_public_key();
    honest_peer
        .add_explicit_peer(mal_bls, config_2.primary_networkkey(), malicious_peer_addr.clone())
        .await?;
    honest_peer.dial_by_bls(mal_bls).await?;

    // wait for malicious peer to discover honest peer's BLS key via kademlia
    let max_time = Duration::from_secs(2);
    wait_for_peer_discovery(&malicious_peer, config_1.key_config().primary_public_key(), max_time)
        .await?;

    // send request and wait for malicious response
    let honest_req = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let response_from_peer =
        honest_peer.send_request_direct(honest_req.clone(), malicious_peer_id).await?;
    let event =
        timeout(max_time, network_events_2.recv()).await?.expect("first network event received");

    // expect network event
    if let NetworkEvent::Request { request, channel, .. } = event {
        assert_eq!(request, honest_req);
        // send response
        let block = fixture_batch_with_transactions(1).seal_slow();
        let malicious_reply = TestWorkerResponse::MissingBatches { batches: vec![block] };
        malicious_peer.send_response(malicious_reply, channel).await?;
    } else {
        panic!("unexpected network event received");
    }

    // expect response
    let res = timeout(max_time, response_from_peer).await?.expect("response received within time");

    // OutboundFailure::Io(Custom { kind: Other, error: Custom("Invalid value was given to the
    // function") })
    assert_matches!(res, Err(NetworkError::Outbound(_)));

    Ok(())
}

/// Regression test for cross-role network isolation.
///
/// A single node runs a primary and a worker `ConsensusNetwork` in the same
/// process; they must never negotiate a working session with one another. Before
/// role-derived protocol names, both spoke `/telcoin-network/0.0.0` (req/res) and
/// `/tn-kad/1.0.0` (kad). Because kad records are keyed on the validator's BLS
/// pubkey — identical for that validator's worker and primary — a primary could
/// ingest a *worker's* `NodeRecord` and then dial/RPC it with primary protocols,
/// penalizing and banning otherwise-healthy peers.
///
/// With `NetworkType::Primary` → `/tn-primary-<chain>/*` and `NetworkType::Worker(0)`
/// → `/tn-worker-0-<chain>/*`, the cross-role substreams never negotiate: kad never exchanges
/// records (so the worker's pushed record never resolves on the primary) and a
/// req/res request surfaces an `OutboundFailure`. Both networks use IDENTICAL
/// req/res message types here, so the only thing that can differ is the
/// role-derived protocol name.
#[tokio::test]
async fn test_primary_worker_protocol_isolation() -> eyre::Result<()> {
    // build two networks from the same committee
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let config_1 = authorities.next().expect("first authority").consensus_config();
    let config_2 = authorities.next().expect("second authority").consensus_config();
    let (tx1, _events_1) = mpsc::channel(10);
    let (tx2, _events_2) = mpsc::channel(10);
    let task_manager = TaskManager::default();

    // peer1: PRIMARY role
    let primary_network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_1.network_config(),
        tx1,
        config_1.key_config().clone(),
        config_1.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        config_1.primary_address(),
        None,
    )
    .expect("primary network created");
    let primary = primary_network.network_handle();
    tokio::spawn(async move {
        primary_network.run().await.expect("primary network run failed!");
    });

    // peer2: WORKER role
    let worker_network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_2.network_config(),
        tx2,
        config_2.key_config().clone(),
        config_2.key_config().worker_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Worker(0),
        config_2.worker_address(),
        None,
    )
    .expect("worker network created");
    let worker = worker_network.network_handle();
    tokio::spawn(async move {
        worker_network.run().await.expect("worker network run failed!");
    });

    // start listening
    primary.start_listening(config_1.primary_address()).await?;
    worker.start_listening(config_2.worker_address()).await?;
    let worker_addr = worker.listeners().await?.first().expect("worker listen addr").clone();
    let primary_peer_id = primary.local_peer_id().await?;

    // the BLS key is identical for both of the validator's networks — exactly the
    // shared key that previously let a worker's record contaminate a primary store.
    let primary_bls = config_1.key_config().primary_public_key();
    let worker_bls = config_2.key_config().primary_public_key();

    // primary dials the worker across roles
    primary
        .add_explicit_peer(
            worker_bls,
            config_2.key_config().worker_network_public_key(),
            worker_addr,
        )
        .await?;
    primary.dial_by_bls(worker_bls).await?;

    // allow the QUIC transport to connect and the kad/req-res substreams to (fail
    // to) negotiate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // positive control: the transport connection IS established across roles — the
    // worker sees the primary's peer id — so the isolation below is protocol-level,
    // not a failed dial.
    assert!(
        worker.connected_peer_ids().await?.contains(&primary_peer_id),
        "transport connection across roles should establish"
    );

    // kad isolation: the primary pushes its record to the worker on connect, but
    // the worker speaks a different kad protocol and never ingests it, so the
    // worker never resolves the primary's BLS key. (The worker did NOT add the
    // primary as an explicit peer, so a resolved BLS key could only come from kad.)
    assert!(
        !worker.connected_peers().await?.contains(&primary_bls),
        "worker resolved primary's BLS key — kad records crossed roles"
    );

    // req/res isolation: a request to the worker cannot negotiate a protocol,
    // the worker speaks `/tn-worker-0-<chain>/*`, not the primary's
    // `/tn-primary-<chain>/*`. With identical message types, this can ONLY be a
    // protocol-name mismatch.
    let req = TestWorkerRequest::MissingBatches(vec![]);
    let reply = primary.send_request(req, worker_bls).await?;
    let res = timeout(Duration::from_secs(5), reply).await?.expect("reply channel");
    assert_matches!(
        res,
        Err(NetworkError::Outbound(_)),
        "cross-role req/res must fail to negotiate a protocol"
    );

    Ok(())
}

/// Issue #777 Part A: a peer whose only req/res failures are `UnsupportedProtocols`
/// must never be penalized or banned.
///
/// Failing to negotiate a common protocol is honest version/role skew, not
/// misbehavior. Reusing the cross-role setup from
/// `test_primary_worker_protocol_isolation`, the primary (`/tn-primary/*`) and the
/// worker (`/tn-worker-0/*`) connect at the transport but cannot negotiate a
/// req/res protocol, so every request surfaces `OutboundFailure::UnsupportedProtocols`.
/// Pre-fix each one applied `Penalty::Severe` (−10); the requests below would drive
/// the score past `min_score_before_ban` (−50) and ban an otherwise-healthy peer.
#[tokio::test]
async fn test_unsupported_protocol_does_not_penalize() -> eyre::Result<()> {
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let config_1 = authorities.next().expect("first authority").consensus_config();
    let config_2 = authorities.next().expect("second authority").consensus_config();
    let (tx1, _events_1) = mpsc::channel(10);
    let (tx2, _events_2) = mpsc::channel(10);
    let task_manager = TaskManager::default();

    // peer1: PRIMARY role
    let primary_network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_1.network_config(),
        tx1,
        config_1.key_config().clone(),
        config_1.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        config_1.primary_address(),
        None,
    )
    .expect("primary network created");
    let primary = primary_network.network_handle();
    tokio::spawn(async move {
        primary_network.run().await.expect("primary network run failed!");
    });

    // peer2: WORKER role
    let worker_network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_2.network_config(),
        tx2,
        config_2.key_config().clone(),
        config_2.key_config().worker_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Worker(0),
        config_2.worker_address(),
        None,
    )
    .expect("worker network created");
    let worker = worker_network.network_handle();
    tokio::spawn(async move {
        worker_network.run().await.expect("worker network run failed!");
    });

    primary.start_listening(config_1.primary_address()).await?;
    worker.start_listening(config_2.worker_address()).await?;
    let worker_addr = worker.listeners().await?.first().expect("worker listen addr").clone();
    let worker_peer_id = worker.local_peer_id().await?;
    let worker_bls = config_2.key_config().primary_public_key();

    // primary dials the worker across roles and establishes the transport connection
    primary
        .add_explicit_peer(
            worker_bls,
            config_2.key_config().worker_network_public_key(),
            worker_addr,
        )
        .await?;
    primary.dial_by_bls(worker_bls).await?;
    wait_until(Duration::from_secs(5), "transport connection across roles establishes", || async {
        Ok(primary.connected_peer_ids().await?.contains(&worker_peer_id))
    })
    .await?;
    assert!(
        primary.connected_peer_ids().await?.contains(&worker_peer_id),
        "transport connection across roles should establish"
    );

    // the primary's view of the worker's score before any failed requests
    let score_before = primary.peer_score(worker_peer_id).await?.expect("worker tracked");

    // fire enough cross-role requests that, pre-fix, 6 * Severe (−10) = −60 would
    // cross the ban threshold (−50)
    for _ in 0..6 {
        let reply =
            primary.send_request(TestWorkerRequest::MissingBatches(vec![]), worker_bls).await?;
        let res = timeout(Duration::from_secs(5), reply).await?.expect("reply channel");
        assert_matches!(
            res,
            Err(NetworkError::Outbound(_)),
            "cross-role req/res must fail to negotiate a protocol"
        );
    }

    // allow any (erroneous) penalty + a heartbeat to propagate
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL + 1)).await;

    // the worker is neither penalized nor banned: still connected, score unchanged
    assert!(
        primary.connected_peer_ids().await?.contains(&worker_peer_id),
        "peer must not be disconnected for unsupported-protocol failures"
    );
    let score_after = primary.peer_score(worker_peer_id).await?.expect("worker still tracked");
    assert_eq!(
        score_before, score_after,
        "unsupported-protocol failures must not change the peer's score (before={score_before}, after={score_after})"
    );

    Ok(())
}

#[tokio::test]
async fn test_publish_to_one_peer() -> eyre::Result<()> {
    // start honest cvv network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest nvv network
    let NetworkPeer {
        config: config_2,
        network_handle: nvv,
        network_events: mut nvv_network_events,
        network,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    cvv.start_listening(config_1.primary_address()).await?;
    nvv.start_listening(config_2.primary_address()).await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_pub_keys()).await?;

    // dial cvv
    nvv.add_trusted_peer_and_dial(
        config_1.key_config().primary_public_key(),
        config_1.key_config().primary_network_public_key(),
        cvv_addr,
    )
    .await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // wait until the publisher has observed a TEST_TOPIC subscriber before publishing.
    // cvv is not subscribed itself, so `publish` fans out only to peers whose
    // subscription has already propagated here; poll for that instead of guessing.
    let topic_hash = TopicHash::from_raw(TEST_TOPIC);
    let topic_ref = &topic_hash;
    let handle = &cvv;
    wait_until(
        Duration::from_secs(5),
        "publisher observes a TEST_TOPIC subscriber",
        move || async move {
            let peers = handle.all_peers().await?;
            Ok(peers.values().any(|topics| topics.contains(topic_ref)))
        },
    )
    .await?;

    // publish on wrong topic - no peers
    let expected_failure = cvv.publish("WRONG_TOPIC".into(), expected_result.clone()).await;
    assert!(expected_failure.is_err());

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(gossip) = event {
        assert_eq!(gossip.message.data, expected_result);
    } else {
        panic!("unexpected network event received");
    }

    Ok(())
}

#[tokio::test]
async fn test_msg_verification_ignores_unauthorized_publisher() -> eyre::Result<()> {
    // start honest cvv network
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: cvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start honest nvv network
    let NetworkPeer {
        config: config_2,
        network_handle: nvv,
        network_events: mut nvv_network_events,
        network,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    cvv.start_listening(config_1.primary_address()).await?;
    nvv.start_listening(config_2.primary_address()).await?;

    let target_peer_bls = config_1.key_config().primary_public_key();
    let target_peer_net = config_1.primary_networkkey();
    let cvv_id: PeerId = target_peer_net.clone().into();
    let target_addr = config_1.primary_address();
    nvv.add_explicit_peer(target_peer_bls, target_peer_net, target_addr).await?;
    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_pub_keys()).await?;

    // dial cvv
    nvv.dial_by_bls(target_peer_bls).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // wait until the publisher has observed a TEST_TOPIC subscriber before publishing.
    // cvv is not subscribed itself, so `publish` fans out only to peers whose
    // subscription has already propagated here; poll for that instead of guessing.
    let topic_hash = TopicHash::from_raw(TEST_TOPIC);
    let topic_ref = &topic_hash;
    let handle = &cvv;
    wait_until(
        Duration::from_secs(5),
        "publisher observes a TEST_TOPIC subscriber",
        move || async move {
            let peers = handle.all_peers().await?;
            Ok(peers.values().any(|topics| topics.contains(topic_ref)))
        },
    )
    .await?;

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message and that the resolved relayer identity is carried
    if let NetworkEvent::Gossip(gossip) = event {
        assert_eq!(gossip.message.data, expected_result);
        assert_eq!(
            gossip.relayer,
            Some(target_peer_bls),
            "resolved relayer's BLS identity must be attached to the delivered gossip"
        );
    } else {
        panic!("unexpected network event received");
    }

    // remove cvv from whitelist and try to publish again
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), HashSet::new()).await?;

    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;

    // message should never be forwarded
    let timeout = timeout(Duration::from_secs(2), nvv_network_events.recv()).await;
    assert!(timeout.is_err());

    // assert fatal score
    let score = nvv.peer_score(cvv_id).await?;
    assert_eq!(score, Some(config_2.network_config().peer_config().score_config.min_score));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_peer_exchange_with_excess_peers() -> eyre::Result<()> {
    tn_types::test_utils::init_test_tracing();
    // Create a custom config with very low peer limits for testing
    let target = NonZeroUsize::new(5).unwrap();
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().target_num_peers = 5; // entire committee + 1
    network_config.peer_config_mut().peer_excess_factor = 0.1;
    network_config.peer_config_mut().excess_peers_reconnection_timeout = Duration::from_secs(10);
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    network_config.libp2p_config_mut().k_bucket_size = target;

    // Set up peers with the custom config
    let (mut target_peer, mut other_peers, _) = create_test_peers::<
        TestWorkerRequest,
        TestWorkerResponse,
    >(target, Some(network_config.clone()));

    // spawn target network
    let target_network = target_peer.network.take().expect("target network is some");
    let target_id = target_peer.config.authority().as_ref().expect("authority").id();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?target_id, ?res, "target network shutdown");
    });

    // Start target peer listening
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;
    let target_addr = target_peer.config.primary_address();
    let target_peer_id = target_peer.network_handle.local_peer_id().await?;
    let target_peer_bls = target_peer.config.key_config().primary_public_key();
    let target_peer_net = target_peer.config.primary_networkkey();

    debug!(target: "network", ?target_peer_id, ?target_peer_bls, "target peer started");

    // start and connect the first few peers (more than target_num_peers)
    for peer in other_peers.iter_mut() {
        // spawn peer network
        let peer_network = peer.network.take().expect("peer network is some");
        let peer_id = peer.config.authority().as_ref().expect("authority").id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?peer_id, ?res, "peer network shutdown");
        });

        peer.network_handle.start_listening(peer.config.primary_address()).await?;

        // add peers to each other's known peers
        peer.network_handle
            .add_explicit_peer(target_peer_bls, target_peer_net.clone(), target_addr.clone())
            .await?;
        target_peer
            .network_handle
            .add_explicit_peer(
                peer.config.key_config().primary_public_key(),
                peer.config.primary_networkkey(),
                peer.config.primary_address(),
            )
            .await?;

        // subscribe to topic for gossip
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_pub_keys())
            .await?;

        // connect to target
        peer.network_handle.dial_by_bls(target_peer_bls).await?;

        // wait for the connection to establish and libp2p state to stabilize
        let peer_handle = &peer.network_handle;
        wait_until(Duration::from_secs(5), "peer connects to target", move || async move {
            Ok(peer_handle.connected_peers().await?.contains(&target_peer_bls))
        })
        .await?;
    }

    // allow heartbeat to trigger peer pruning - increased to give libp2p time to
    // stabilize internal connection state and avoid race conditions in CI
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 3)).await;

    // check that target has limited peers
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
    debug!(target: "network", count=connected_peers.len(), "target connected peers after pruning");

    // Should have at most max_peers connected
    let max_peers = network_config.peer_config().max_peers();
    assert!(
        connected_peers.len() <= max_peers,
        "Target should have at most {} peers, has {}",
        max_peers,
        connected_peers.len()
    );

    // create a new non-validator peer
    let TestTypes { peer1: nvv_peer, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    let NetworkPeer {
        config: peer2_config,
        network_handle: peer2,
        network: peer2_network,
        network_events: _,
    } = peer2;

    // connect peer2
    tokio::spawn(async move {
        if let Err(e) = peer2_network.run().await {
            error!(target: "network", ?e, "peer2 network run failed");
        }
    });

    peer2.start_listening(peer2_config.primary_address()).await?;

    // add peers to each other's known peers
    // add target as a bootstrap nodes
    peer2.add_explicit_peer(target_peer_bls, target_peer_net.clone(), target_addr.clone()).await?;

    // subscribe to topic for gossip
    peer2
        .subscribe_with_publishers(TEST_TOPIC.into(), vec![target_peer_bls].into_iter().collect())
        .await?;

    // connect to target
    peer2.dial_by_bls(target_peer_bls).await?;

    // give time for connection to establish and libp2p state to stabilize
    // target should be at max capacity
    tokio::time::sleep(Duration::from_millis(500)).await;

    // spawn nvv that goes through px
    let NetworkPeer {
        config: nvv_config,
        network_handle: nvv,
        network,
        network_events: mut nvv_events,
    } = nvv_peer;

    tokio::spawn(async move {
        if let Err(e) = network.run().await {
            error!(target: "network", ?e, "nvv network run failed");
        }
    });

    nvv.start_listening(nvv_config.primary_address()).await?;
    let nvv_peer_id = nvv.local_peer_id().await?;
    let nvv_peer_bls = nvv_config.key_config().primary_public_key();

    debug!(target: "network", ?nvv_peer_id, ?nvv_peer_bls, "nvv peer started");

    // add target as bootstrap peer
    nvv.add_explicit_peer(target_peer_bls, target_peer_net, target_addr.clone()).await?;
    // subscribe with target as authorized publisher
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), vec![target_peer_bls].into_iter().collect())
        .await?;

    // connect nvv to target (which already has too many peers)
    nvv.dial_by_bls(target_peer_bls).await?;

    // allow time for kademlia records to propagate and libp2p connection state to
    // stabilize after disconnection - increased to avoid race conditions in CI
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 5)).await;

    // assert target is disconnected from nvv
    assert!(!target_peer
        .network_handle
        .connected_peers()
        .await?
        .contains(nvv_config.config().primary_bls_key()));

    // assert nvv is disconnected from target
    let connected = nvv.connected_peer_ids().await?;
    error!(target: "network", ?connected, "nvv connected peers");
    assert!(!connected.contains(&target_peer_id));

    // publish from target
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_msg = Vec::from(&sealed_block);

    target_peer.network_handle.publish(TEST_TOPIC.into(), expected_msg.clone()).await?;

    // check if nvv receives the gossip (either directly or through mesh)
    // increased timeout for CI environments with higher latency
    let mut received = false;
    let timeout = Duration::from_secs(10);
    let start = tokio::time::Instant::now();

    while !received && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(500), nvv_events.recv()).await {
            Ok(Some(NetworkEvent::Gossip(gossip))) => {
                assert_eq!(gossip.message.data, expected_msg, "Gossip message data mismatch");
                let from = gossip.relayer;
                debug!(target: "network", ?from, "nvv received gossip from peer");
                received = true;
            }
            Ok(Some(NetworkEvent::Request {
                request: TestWorkerRequest::PeerExchange(_), ..
            })) => {
                // Ignore additional peer exchanges
                continue;
            }
            Ok(Some(other)) => {
                debug!(target: "network", ?other, "nvv received other event");
            }
            _ => {}
        }
    }

    assert!(received, "nvv MUST receive gossip message through mesh propagation");

    Ok(())
}

/// A pruned peer that does not support the dedicated peer-exchange protocol still
/// receives the goodbye.
///
/// The raw swarm below advertises only the legacy `/tn-primary-{chain}/0.0.1`
/// req-res protocol, exactly the wire surface of a not-yet-upgraded node. The
/// target's goodbye attempt on `/tn-primary-peer-exchange-{chain}/0.0.1` fails
/// with `UnsupportedProtocols` (penalty-exempt) and must fall back to the
/// `PeerExchange` variant embedded in the legacy request enum.
#[tokio::test(flavor = "multi_thread")]
async fn test_goodbye_falls_back_to_embedded_exchange_for_legacy_peer() -> eyre::Result<()> {
    tn_types::test_utils::init_test_tracing();
    // 4 committee peers fill the target to its limit; validators are protected from
    // pruning, so the raw legacy peer is deterministically the excess one
    let committee = NonZeroUsize::new(5).unwrap();
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().target_num_peers = 4;
    network_config.peer_config_mut().peer_excess_factor = 0.1;
    network_config.peer_config_mut().excess_peers_reconnection_timeout = Duration::from_secs(10);
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    network_config.libp2p_config_mut().k_bucket_size = committee;

    let (mut target_peer, mut other_peers, _task_manager) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            committee,
            Some(network_config.clone()),
        );

    // spawn target network
    let target_network = target_peer.network.take().expect("target network is some");
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?res, "target network shutdown");
    });
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;
    let target_addr = target_peer.config.primary_address();
    let target_peer_bls = target_peer.config.key_config().primary_public_key();
    let target_peer_net = target_peer.config.primary_networkkey();

    // fill the target with protected committee peers
    for peer in other_peers.iter_mut() {
        let peer_network = peer.network.take().expect("peer network is some");
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?res, "peer network shutdown");
        });
        peer.network_handle.start_listening(peer.config.primary_address()).await?;
        peer.network_handle
            .add_explicit_peer(target_peer_bls, target_peer_net.clone(), target_addr.clone())
            .await?;
        target_peer
            .network_handle
            .add_explicit_peer(
                peer.config.key_config().primary_public_key(),
                peer.config.primary_networkkey(),
                peer.config.primary_address(),
            )
            .await?;
        peer.network_handle.dial_by_bls(target_peer_bls).await?;

        // wait for the connection to establish and libp2p state to stabilize
        let peer_handle = &peer.network_handle;
        wait_until(Duration::from_secs(5), "peer connects to target", move || async move {
            Ok(peer_handle.connected_peers().await?.contains(&target_peer_bls))
        })
        .await?;
    }

    // raw legacy peer: the target's main req-res protocol only, no dedicated
    // peer-exchange protocol
    let chain_id = network_config.libp2p_config().chain_id;
    let raw_keypair = NetworkKeypair::generate_ed25519();
    let raw_peer_id: PeerId = raw_keypair.public().into();
    let legacy_codec = TNCodec::<TestWorkerRequest, TestWorkerResponse>::new(
        network_config.libp2p_config().max_rpc_message_size,
    );
    let legacy_req_res = request_response::Behaviour::with_codec(
        legacy_codec,
        vec![(NetworkType::Primary.req_res_protocol(chain_id)?, ProtocolSupport::Full)],
        request_response::Config::default(),
    );
    let mut raw_swarm = SwarmBuilder::with_existing_identity(raw_keypair)
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| legacy_req_res)
        .map_err(|e| eyre!("raw swarm behaviour: {e:?}"))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();

    // excess dial: accepted (max_peers = ceil(4 * 1.1) = 5), then pruned with px
    raw_swarm.dial(target_addr.clone())?;

    // drive the raw swarm: capture the legacy embedded goodbye and ack it
    let (goodbye_tx, goodbye_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut goodbye_tx = Some(goodbye_tx);
        loop {
            if let SwarmEvent::Behaviour(request_response::Event::Message {
                message: request_response::Message::Request { request, channel, .. },
                ..
            }) = raw_swarm.select_next_some().await
            {
                if let TestWorkerRequest::PeerExchange(map) = request {
                    let ack = TestWorkerResponse::from(PeerExchangeMap::default());
                    let _ = raw_swarm.behaviour_mut().send_response(channel, ack);
                    if let Some(tx) = goodbye_tx.take() {
                        let _ = tx.send(map);
                    }
                }
            }
        }
    });

    // the target prunes the excess raw peer on a heartbeat; the goodbye must reach
    // the legacy peer on the embedded path (which requires the dedicated-protocol
    // attempt to have failed over)
    let goodbye = timeout(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 10), goodbye_rx)
        .await
        .expect("legacy peer received the goodbye before timeout")?;
    assert!(
        !goodbye.0.is_empty(),
        "the fallback goodbye should carry the target's known peers for discovery"
    );

    // the ack resolves the pending exchange and the target disconnects promptly
    let target_handle = &target_peer.network_handle;
    wait_until(Duration::from_secs(10), "target disconnects the raw legacy peer", || async {
        Ok(!target_handle.connected_peer_ids().await?.contains(&raw_peer_id))
    })
    .await?;
    assert!(
        !target_peer.network_handle.connected_peer_ids().await?.contains(&raw_peer_id),
        "target should disconnect the raw legacy peer after the goodbye"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_score_decay_and_reconnection() -> eyre::Result<()> {
    // Create a custom config with short halflife for quicker testing
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().score_config.score_halflife = 0.1;
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let default_score = network_config.peer_config_mut().score_config.default_score;

    // Set up multiple peers with the custom config
    let (peer1, mut other_peers, _task_manager) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(4).unwrap(),
            Some(network_config.clone()),
        );

    let peer2 = other_peers.remove(0);
    let TestPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.expect("peer1 network available").run().await.expect("network run failed!");
    });

    let TestPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.expect("peer2 network available").run().await.expect("network run failed!");
    });

    // Start listeners and establish connection
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id: PeerId = config_2.primary_networkkey().into();
    let peer2_bls = config_2.key_config().primary_public_key();

    peer1
        .add_explicit_peer(peer2_bls, config_2.primary_networkkey(), config_2.primary_address())
        .await?;

    // Connect peers
    peer1.dial_by_bls(peer2_bls).await?;

    // Wait for peer2 to be connected (receives peer1 bls key).
    wait_until(Duration::from_secs(5), "peer2 is connected", || async {
        Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
    })
    .await?;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected");

    // Apply medium penalties to lower score but not ban
    for _ in 0..3 {
        peer1.report_penalty(peer2_bls, Penalty::Medium).await;
    }

    // Wait for the penalties to lower peer2's score below the default.
    wait_until(
        Duration::from_secs(5),
        "peer2 score drops below default after penalties",
        || async { Ok(peer1.peer_score(peer2_id).await?.is_some_and(|s| s < default_score)) },
    )
    .await?;

    // Check peer2's score is lower but still connected
    let score_after_penalty = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(
        score_after_penalty < default_score,
        "{score_after_penalty} not less than {default_score}"
    );

    // Wait for scores to recover (decay toward 0) through heartbeats.
    wait_until(
        Duration::from_secs(4 * TEST_HEARTBEAT_INTERVAL + 5),
        "peer2 score recovers above the post-penalty low",
        || async { Ok(peer1.peer_score(peer2_id).await?.is_some_and(|s| s > score_after_penalty)) },
    )
    .await?;

    // Check score improved (decayed toward 0)
    let score_after_decay = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(
        score_after_decay > score_after_penalty,
        "Score should decay toward 0. After penalty: {score_after_penalty}, after decay: {score_after_decay}"
    );

    // Peer should still be connected
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(
        connected_peers.contains(&peer2_id),
        "Peer2 should still be connected after score recovery"
    );

    Ok(())
}

#[tokio::test]
async fn test_banned_peer_reconnection_attempt() -> eyre::Result<()> {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    let NetworkPeer { config: config_1, network_handle: honest_peer, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: malicious_peer, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start listeners
    honest_peer.start_listening(config_1.primary_address()).await?;
    malicious_peer.start_listening(config_2.primary_address()).await?;

    let malicious_id: PeerId = config_2.primary_networkkey().into();
    let malicious_bls = config_2.key_config().primary_public_key();

    let malicious_addr = config_2.primary_address();

    // honest must resolve malicious_bls -> peer id to penalize it. the committee gate added in
    // #827 drops kad-discovered records for non-committee keys, so register the malicious peer
    // explicitly (an operator-provisioned, pinned path) instead. unlike committee membership this
    // does not make honest re-dial the peer, so the Fatal ban disconnects and stays disconnected.
    honest_peer
        .add_explicit_peer(malicious_bls, config_2.primary_networkkey(), malicious_addr.clone())
        .await?;

    // Connect malicious to honest
    malicious_peer
        .add_explicit_peer(
            config_1.key_config().primary_public_key(),
            config_1.primary_networkkey(),
            config_1.primary_address(),
        )
        .await?;
    malicious_peer.dial_by_bls(config_1.key_config().primary_public_key()).await?;

    // Wait for the connection to establish
    wait_until(Duration::from_secs(5), "malicious peer connects to honest", || async {
        Ok(honest_peer.connected_peer_ids().await?.contains(&malicious_id))
    })
    .await?;

    debug!(target: "peer-manager", ?malicious_id, ?malicious_bls, "assessing fatal penalty!!");
    // Report fatal penalty for malicious peer
    honest_peer.report_penalty(malicious_bls, Penalty::Fatal).await;

    // Wait for ban to take effect and disconnect (needs multiple heartbeat cycles on slow CI)
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 3)).await;

    // Verify malicious peer is disconnected
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&malicious_id), "Malicious peer should be disconnected");

    // Verify peer is banned
    let score = honest_peer.peer_score(malicious_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score_before_ban;
    assert!(score <= min_score, "Peer should have ban-level score");

    // Now try to reconnect from malicious peer
    let honest_bls = config_1.key_config().primary_public_key();
    malicious_peer
        .add_explicit_peer(honest_bls, config_1.primary_networkkey(), config_1.primary_address())
        .await?;
    let dial_result = malicious_peer.dial_by_bls(honest_bls).await;

    // The dial command should succeed at the API level (the swarm will try to dial)
    assert!(dial_result.is_ok());

    // Wait a moment to see if connection is rejected
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection is rejected
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(
        !connected_peers.contains(&malicious_id),
        "Banned peer should not be allowed to reconnect"
    );

    // Try direct connection from honest to malicious (should also be refused due to banned state)
    assert!(honest_peer.dial(malicious_id, malicious_addr).await.is_err());

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection still not established
    let connected_peers = honest_peer.connected_peer_ids().await?;
    assert!(
        !connected_peers.contains(&malicious_id),
        "Honest peer should not connect to banned peer"
    );

    Ok(())
}

#[tokio::test]
async fn test_dial_timeout_behavior() -> eyre::Result<()> {
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().dial_timeout = Duration::from_millis(100);

    let (mut peer1, _others, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
        NonZeroUsize::new(4).unwrap(),
        None,
    );
    let network = peer1.network.take().unwrap();
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start listener
    peer1.network_handle.start_listening(peer1.config.primary_address()).await?;

    // Create a peer ID that doesn't exist
    let nonexistent_peer = PeerId::random();

    // Create a valid but unreachable multiaddr (use a random high port)
    let unreachable_addr = create_multiaddr(None);

    let (tx, rx) = oneshot::channel();
    let handle = peer1.network_handle.clone();
    tokio::spawn(async move {
        // Start dial attempt
        let dial_result = handle.dial(nonexistent_peer, unreachable_addr).await;
        let _ = tx.send(dial_result);
    });

    // Wait for dial timeout
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    assert!(rx.await.unwrap().is_err());

    // Verify dialing peer has been cleaned up
    let connected_peers = peer1.network_handle.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&nonexistent_peer), "Failed dial should be cleaned up");

    Ok(())
}

#[tokio::test]
async fn test_multi_peer_mesh_formation() -> eyre::Result<()> {
    // Create multiple peers but with more realistic constraints
    let num_peers = NonZeroUsize::new(4).unwrap();
    let mut network_config = NetworkConfig::default();

    // Use default peer limits but with a faster heartbeat for testing
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;

    // committee network
    let (mut target_peer, _committee, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
        num_peers,
        Some(network_config.clone()),
    );
    // create other nvvs
    let (_, mut other_peers, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, Some(network_config));

    // Start target peer
    let target_network = target_peer.network.take().expect("target network is some");
    tokio::spawn(async move {
        let _ = target_network.run().await;
    });

    // Start target peer listening
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;

    let target_bls = *target_peer.config.config().primary_bls_key();
    let target_addr = target_peer.config.primary_address();
    let target_net_key = target_peer.config.primary_networkkey();

    // Subscribe target to test topic
    target_peer
        .network_handle
        .subscribe_with_publishers(
            TEST_TOPIC.into(),
            other_peers.first().unwrap().config.committee_pub_keys(),
        )
        .await?;

    // Start other peers and connect them all to the target (star topology)
    for peer in other_peers.iter_mut() {
        // Start peer network
        let peer_network = peer.network.take().expect("peer network is some");
        tokio::spawn(async move {
            let _ = peer_network.run().await;
        });

        // Start listener
        peer.network_handle.start_listening(peer.config.primary_address()).await?;

        // Connect to target peer
        peer.network_handle
            .add_explicit_peer(target_bls, target_net_key.clone(), target_addr.clone())
            .await?;
        peer.network_handle.dial_by_bls(target_bls).await?;

        // Wait for the connection to establish
        let peer_handle = &peer.network_handle;
        wait_until(Duration::from_secs(5), "peer connects to target", move || async move {
            Ok(peer_handle.connected_peers().await?.contains(&target_bls))
        })
        .await?;

        // subscribe to test topic with target peer as authorized publisher
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), vec![target_bls].into_iter().collect())
            .await?;
    }

    // Wait for every peer to connect to the target, instead of assuming a fixed
    // number of heartbeats is enough
    let handle = &target_peer.network_handle;
    let expected = other_peers.len();
    wait_until(Duration::from_secs(10), "all peers connected to target", move || async move {
        Ok(handle.connected_peer_ids().await?.len() == expected)
    })
    .await?;

    // Verify all peers are connected to target
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
    assert_eq!(connected_peers.len(), other_peers.len(), "All peers should be connected to target");

    // Check gossipsub mesh formation
    let mesh_peers = target_peer.network_handle.mesh_peers(TEST_TOPIC.into()).await?;
    debug!(target: "network", "Target has {} peers in its gossipsub mesh", mesh_peers.len());

    // Mesh formation takes time, we should have at least some peers in the mesh
    // Note: We can't guarantee all peers will be in the mesh due to gossipsub's internal behavior
    assert!(!mesh_peers.is_empty(), "Target should have at least some peers in its gossipsub mesh");

    // Test message propagation
    let test_data = Vec::from("test data for propagation".as_bytes());
    target_peer.network_handle.publish(TEST_TOPIC.into(), test_data.clone()).await?;

    // Wait for message propagation
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check mesh connectivity through gossipsub stats
    let gossip_peers = target_peer.network_handle.all_peers().await?;
    debug!(target: "network", "Gossipsub knows about {} peers", gossip_peers.len());
    assert!(!gossip_peers.is_empty(), "Target's gossipsub should know about its peers");

    // For each peer, check if they're subscribed to the topic
    for (peer_id, topics) in gossip_peers {
        assert!(
            topics.iter().any(|t| *t == TopicHash::from_raw(TEST_TOPIC)),
            "Peer {peer_id:?} should be subscribed to test topic"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_unbans_committee_members() -> eyre::Result<()> {
    // Start with two peers
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start swarm listening
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peers
    peer1
        .add_explicit_peer(
            config_2.key_config().primary_public_key(),
            config_2.key_config().primary_network_public_key(),
            peer2_addr.clone(),
        )
        .await?;
    peer1.dial_by_bls(config_2.key_config().primary_public_key()).await?;

    // Wait for the connection to establish
    wait_until(Duration::from_secs(5), "peer2 connects initially", || async {
        Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
    })
    .await?;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply fatal penalty to peer2 - should ban it
    peer1.report_penalty(config_2.key_config().primary_public_key(), Penalty::Fatal).await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2 is disconnected and banned
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&peer2_id), "Peer2 should be disconnected after ban");

    let score = peer1.peer_score(peer2_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score;
    assert_eq!(score, min_score, "Peer2 should have ban-level score");

    // Now simulate a new epoch where peer2 is in the committee
    let committee = vec![*config_2.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    // Seed peer1's committee with peer2
    let handle = peer1.clone();
    tokio::spawn(async move {
        handle
            .update_committees(Default::default(), committee, Default::default())
            .await
            .expect("Failed to send UpdateCommittees command");
    })
    .await?;

    // Wait for unban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2's score has improved and is no longer banned
    let score_after_epoch = peer1.peer_score(peer2_id).await?.unwrap();
    assert_eq!(
        score_after_epoch,
        config_1.network_config().peer_config().score_config.max_score,
        "Peer2 should have improved score after new epoch"
    );

    // peer2 should dial peer1 - but try dial to reconnecting peer2 and ignore `AlreadyConnectedErr`
    let _ = peer1.dial_by_bls(config_2.key_config().primary_public_key()).await;

    // Wait for the connection to reestablish
    wait_until(Duration::from_secs(5), "peer2 reconnects after unban", || async {
        Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
    })
    .await?;

    // Verify connection reestablished
    let connected_peers_after = peer1.connected_peer_ids().await?;
    assert!(connected_peers_after.contains(&peer2_id), "Peer2 should be reconnected after unban");

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_unbans_committee_member_ip() -> eyre::Result<()> {
    // Create multiple peers for this test
    let num_peers = NonZeroUsize::new(4).unwrap();
    let (mut target_peer, _, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, None);

    // create new committee
    let (_, mut other_peers, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(num_peers, None);

    // Start target peer network
    let target_network = target_peer.network.take().expect("target network is some");
    tokio::spawn(async move {
        target_network.run().await.expect("network run failed!");
    });

    // Start listening
    target_peer.network_handle.start_listening(target_peer.config.primary_address()).await?;

    // Take peer1 and peer2 from other_peers
    let mut peer1 = other_peers.remove(0);
    let mut peer2 = other_peers.remove(0);

    // Start peer1 network
    let peer1_network = peer1.network.take().expect("peer1 network is some");
    let peer1_network_task = tokio::spawn(async move {
        peer1_network.run().await.expect("network run failed!");
    });

    peer1.network_handle.start_listening(peer1.config.primary_address()).await?;
    let peer1_id = peer1.network_handle.local_peer_id().await?;
    let peer1_addr = peer1.config.primary_address();

    // Start peer2 network - this will be our future committee member
    // Use the SAME multiaddr as peer1 to simulate same IP
    let peer2_network = peer2.network.take().expect("peer2 network is some");
    tokio::spawn(async move {
        peer2_network.run().await.expect("network run failed!");
    });

    // For peer2, multiaddr has the same IP as peer1 (127.0.0.1)
    let peer2_addr = peer2.config.primary_address();
    // join network so kad record is available
    peer2.network_handle.start_listening(peer2_addr.clone()).await?;
    peer2.network_handle.dial(peer1_id, peer1_addr.clone()).await?;

    // Connect target to peer1
    target_peer
        .network_handle
        .add_explicit_peer(
            peer1.config.key_config().primary_public_key(),
            peer1.config.primary_networkkey(),
            peer1.config.primary_address(),
        )
        .await?;
    target_peer.network_handle.dial_by_bls(peer1.config.key_config().primary_public_key()).await?;
    // Explicitly add peer2 as well- we want to track it's score at the end.
    target_peer
        .network_handle
        .add_explicit_peer(
            peer2.config.key_config().primary_public_key(),
            peer2.config.primary_networkkey(),
            peer2.config.primary_address(),
        )
        .await?;

    // Wait for the connection to establish
    let target_handle = &target_peer.network_handle;
    wait_until(Duration::from_secs(5), "target connects to peer1", || async {
        Ok(target_handle.connected_peer_ids().await?.contains(&peer1_id))
    })
    .await?;

    // Apply fatal penalty to peer1 - should ban it and its IP
    target_peer
        .network_handle
        .report_penalty(peer1.config.key_config().primary_public_key(), Penalty::Fatal)
        .await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer1 is disconnected and banned
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;
    assert!(!connected_peers.contains(&peer1_id), "Peer1 should be disconnected after ban");

    // shutdown peer1 network
    peer1_network_task.abort();

    // allow os to make port available again
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Now simulate a new epoch where peer2 is in the committee with the same IP as banned peer1
    let committee = vec![*peer2.config.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    target_peer
        .network_handle
        .update_committees(Default::default(), committee, Default::default())
        .await?;

    // wait for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // verify connection established with peer2
    let peer2_score = target_peer
        .network_handle
        .peer_score(peer2.network_handle.local_peer_id().await.unwrap())
        .await?
        .unwrap();
    assert_eq!(
        peer2_score, 100.0,
        "Peer2 should have a high score despite sharing IP with banned peer1"
    );

    Ok(())
}

#[tokio::test]
async fn test_new_epoch_handles_disconnecting_pending_ban() -> eyre::Result<()> {
    // Start with two peers
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // Start swarm listening
    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2.local_peer_id().await?;

    let peer2_bls = config_2.key_config().primary_public_key();
    peer1
        .add_explicit_peer(
            peer2_bls,
            config_2.key_config().primary_network_public_key(),
            config_2.primary_address(),
        )
        .await?;
    // Connect peers
    peer1.dial_by_bls(peer2_bls).await?;

    // Wait for the connection to establish
    wait_until(Duration::from_secs(5), "peer2 connects initially", || async {
        Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
    })
    .await?;

    // Verify connection established
    let connected_peers = peer1.connected_peer_ids().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply severe penalties to put peer in a disconnecting state pending ban
    // We need to apply penalties but not enough to cause immediate ban
    // First apply medium penalties
    for _ in 0..3 {
        peer1.report_penalty(peer2_bls, Penalty::Medium).await;
    }

    // Then apply a severe penalty - should trigger disconnect pending ban
    peer1.report_penalty(peer2_bls, Penalty::Severe).await;

    // Wait for disconnect to begin but not complete
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now simulate a new epoch where peer2 is in the committee
    let committee = vec![*config_2.authority().as_ref().expect("authority").protocol_key()]
        .into_iter()
        .collect();

    // Seed peer1's committee with peer2
    let handle = peer1.clone();
    tokio::spawn(async move {
        handle
            .update_committees(Default::default(), committee, Default::default())
            .await
            .expect("Failed to send UpdateCommittees command");
    })
    .await?;

    // Wait for epoch processing to lift peer2's score above zero.
    wait_until(
        Duration::from_secs(TEST_HEARTBEAT_INTERVAL + 5),
        "peer2 score turns positive after new epoch",
        || async { Ok(peer1.peer_score(peer2_id).await?.is_some_and(|s| s > 0.0)) },
    )
    .await?;

    // Verify peer2's score has improved and is trusted
    let score_after_epoch = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_epoch > 0.0, "Peer2 should have a positive score after new epoch");

    // Try reconnecting peer2 if it was disconnected during the process
    if !peer1.connected_peer_ids().await?.contains(&peer2_id) {
        let dial_result = peer1.dial_by_bls(peer2_bls).await;
        assert!(dial_result.is_ok(), "Should be able to reconnect to peer2 after new epoch");

        // Wait for the connection to reestablish
        wait_until(Duration::from_secs(5), "peer2 reconnects after new epoch", || async {
            Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
        })
        .await?;
    }

    // Verify connection is established
    let connected_peers_after = peer1.connected_peer_ids().await?;
    assert!(connected_peers_after.contains(&peer2_id), "Peer2 should be connected after new epoch");

    Ok(())
}

#[tokio::test]
async fn test_rotate_does_not_disconnect_previous_committee() -> eyre::Result<()> {
    // Start with two peers
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer { config: config_2, network_handle: peer2, network, .. } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    peer1.start_listening(config_1.primary_address()).await?;
    peer2.start_listening(config_2.primary_address()).await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_bls = config_2.key_config().primary_public_key();

    // Connect peer1 -> peer2
    peer1
        .add_explicit_peer(
            peer2_bls,
            config_2.key_config().primary_network_public_key(),
            config_2.primary_address(),
        )
        .await?;
    peer1.dial_by_bls(peer2_bls).await?;
    wait_until(Duration::from_secs(5), "peer2 connects before rotation", || async {
        Ok(peer1.connected_peer_ids().await?.contains(&peer2_id))
    })
    .await?;
    assert!(
        peer1.connected_peer_ids().await?.contains(&peer2_id),
        "peer2 should be connected before rotation"
    );

    // peer2 is in the CURRENT committee for this epoch
    peer1
        .update_committees(
            Default::default(),
            vec![peer2_bls].into_iter().collect(),
            Default::default(),
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Update to the next epoch where peer2 has rotated out of `current` into `previous`: it is
    // placed explicitly in the previous slot and absent from current/next. peer2 must NOT be
    // disconnected, since it still counts as a validator (is_peer_validator spans the previous
    // committee).
    peer1
        .update_committees(
            vec![peer2_bls].into_iter().collect(),
            Default::default(),
            Default::default(),
        )
        .await?;

    // Let several heartbeats run so any pruning of non-validator peers would take effect.
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 3)).await;

    assert!(
        peer1.connected_peer_ids().await?.contains(&peer2_id),
        "peer2 must stay connected after rotating into the previous committee"
    );
    // peer2 stays trusted because it is still inside the three-slot window (now in `previous`), so
    // it keeps the validator (max) score it was given on entering the committee.
    let peer2_score = peer1.peer_score(peer2_id).await?.expect("peer2 score");
    assert_eq!(
        peer2_score,
        config_1.network_config().peer_config().score_config.max_score,
        "previous-committee peer should retain validator (max) score"
    );

    Ok(())
}

#[tokio::test]
async fn test_gossip_explicit_peer_includes_next_committee() -> eyre::Result<()> {
    // peer1 publishes; peer2 only ever appears in peer1's `next` committee and receives gossip.
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: publisher, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    let NetworkPeer {
        config: config_2,
        network_handle: next_peer,
        network_events: mut next_peer_events,
        network,
    } = peer2;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    publisher.start_listening(config_1.primary_address()).await?;
    next_peer.start_listening(config_2.primary_address()).await?;
    let publisher_addr =
        publisher.listeners().await?.first().expect("publisher listen addr").clone();

    let next_bls = config_2.key_config().primary_public_key();
    let next_id = next_peer.local_peer_id().await?;

    // Register peer2's network info with peer1, then place peer2 ONLY in peer1's `next` committee
    // (peer1 never had peer2 in `current`). When peer2 connects, peer1's PeerConnected handler must
    // treat it as important (is_peer_validator now spans `next`) and add it as a gossipsub explicit
    // peer, so gossip reaches it.
    publisher
        .add_explicit_peer(
            next_bls,
            config_2.key_config().primary_network_public_key(),
            config_2.primary_address(),
        )
        .await?;
    publisher
        .update_committees(
            Default::default(),
            Default::default(),
            vec![next_bls].into_iter().collect(),
        )
        .await?;

    // peer2 subscribes with peer1 as the authorized publisher and dials peer1.
    next_peer
        .subscribe_with_publishers(
            TEST_TOPIC.into(),
            vec![config_1.key_config().primary_public_key()].into_iter().collect(),
        )
        .await?;
    next_peer
        .add_trusted_peer_and_dial(
            config_1.key_config().primary_public_key(),
            config_1.key_config().primary_network_public_key(),
            publisher_addr,
        )
        .await?;

    // Allow the connection to establish and gossipsub to graft.
    wait_until(Duration::from_secs(5), "next-committee peer connects to publisher", || async {
        Ok(publisher.connected_peer_ids().await?.contains(&next_id))
    })
    .await?;

    assert!(
        publisher.connected_peer_ids().await?.contains(&next_id),
        "next-committee peer should be connected to the publisher"
    );

    // Publish and confirm the next-committee peer receives the gossip.
    let block = fixture_batch_with_transactions(10).seal_slow();
    let expected = Vec::from(&block);
    publisher.publish(TEST_TOPIC.into(), expected.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), next_peer_events.recv()).await?.expect("gossip received");
    if let NetworkEvent::Gossip(gossip) = event {
        assert_eq!(gossip.message.data, expected);
    } else {
        panic!("unexpected network event received");
    }

    Ok(())
}

/// Test kad records available to new node joining the network.
#[tokio::test]
async fn test_get_kad_records() -> eyre::Result<()> {
    // used later
    let num_network_peers = 5;

    // Set up multiple peers with the custom config
    let (mut target_peer, mut committee, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(num_network_peers).unwrap(),
            None,
        );

    // spawn target network
    let target_network = target_peer.network.take().expect("target network is some");
    let id = target_peer.config.authority().as_ref().expect("authority").id();
    let target_peer_bls = target_peer.config.key_config().primary_public_key();
    let target_peer_net = target_peer.config.primary_networkkey();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?id, ?res, "network shutdown");
    });

    // Start target peer listening
    let target_addr = target_peer.config.primary_address();
    target_peer.network_handle.start_listening(target_addr.clone()).await?;
    let target_peer_id: PeerId =
        target_peer.config.config().node_info.primary_network_key().clone().into();

    let mut peer_mapping = vec![(target_peer_bls, target_peer_net.clone(), target_addr.clone())];
    // Start other peers and connect them one by one to the target
    for peer in committee.iter_mut() {
        // spawn peer network
        let peer_network = peer.network.take().expect("peer network is some");
        let id = peer.config.authority().as_ref().expect("authority").id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?id, ?res, "network shutdown");
        });

        let peer_addr = peer.config.primary_address();
        peer.network_handle.start_listening(peer_addr).await?;

        peer_mapping.push((
            peer.config.key_config().primary_public_key(),
            peer.config.key_config().primary_network_public_key(),
            peer.config.config().node_info.primary_network_address().clone(),
        ));

        // Connect to target
        peer.network_handle
            .add_trusted_peer_and_dial(
                target_peer_bls,
                target_peer_net.clone(),
                target_addr.clone(),
            )
            .await?;

        // Wait for the connection to establish
        let peer_handle = &peer.network_handle;
        wait_until(Duration::from_secs(5), "peer connects to target", move || async move {
            Ok(peer_handle.connected_peers().await?.contains(&target_peer_bls))
        })
        .await?;

        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_pub_keys())
            .await?;
    }

    // Allow time for heartbeats to happen
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Check connected peers on target - should be limited based on config
    let connected_peers = target_peer.network_handle.connected_peer_ids().await?;

    // assert all peers connected (minus this node)
    assert_eq!(connected_peers.len(), num_network_peers - 1);

    // create non-validator peer
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: nvv_config,
        network_handle: nvv,
        network,
        network_events: mut nvv_events,
    } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    nvv.start_listening(nvv_config.primary_address()).await?;

    // connect to target
    nvv.add_trusted_peer_and_dial(target_peer_bls, target_peer_net.clone(), target_addr.clone())
        .await?;
    // subscribe to topic
    // add target peer as authorized publisher
    nvv.subscribe_with_publishers(
        TEST_TOPIC.into(),
        vec![*target_peer.config.authority().as_ref().expect("authority").protocol_key()]
            .into_iter()
            .collect(),
    )
    .await?;

    // give time for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // find other committee members through kad
    let authorities: Vec<BlsPublicKey> =
        committee.iter().map(|peer| peer.config.key_config().primary_public_key()).collect();
    nvv.find_authorities(authorities.clone()).await?;

    // allow dial attempts to be made
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 5)).await;

    // assert nvv is connected with other peers
    let connected = nvv.connected_peer_ids().await?;
    debug!(target: "network", ?connected, "nvv connected peers");
    assert!(connected.contains(&target_peer_id));
    for peer in committee.iter() {
        let id = peer.network_handle.local_peer_id().await?;
        debug!(target: "network", ?id, "checking connection for peer");
        assert!(connected.contains(&id));
    }

    // publish random batch
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_msg = Vec::from(&sealed_block);

    // assert gossip from disconnected target peer is received by nvv
    target_peer.network_handle.publish(TEST_TOPIC.into(), expected_msg.clone()).await?;

    // wait for gossip from disconnected peer
    match timeout(Duration::from_secs(5), nvv_events.recv()).await {
        Ok(Some(NetworkEvent::Gossip(gossip))) => {
            let GossipMessage { source, data, .. } = gossip.message;
            assert_eq!(source, Some(target_peer_id));
            assert_eq!(data, expected_msg);
        }
        Ok(None) => return Err(eyre!("Channel closed without receiving event")),
        Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
        e => return Err(eyre!("wrong event type: {:?}", e)),
    }

    Ok(())
}

#[tokio::test]
async fn test_node_record_validation() {
    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let network = peer1.network;

    // Create a kad::Record with correct publisher
    let mut peer_record = network.get_peer_record();
    assert!(network.peer_record_valid(&peer_record).is_some());
    assert!(peer2.network.peer_record_valid(&peer_record).is_some());

    // assert no publisher fails
    peer_record.publisher = None;
    // assert invalid peer record rejected with no publisher
    assert!(network.peer_record_valid(&peer_record).is_none());

    // assert publisher mismatch fails
    peer_record.publisher = Some(*peer2.network.swarm.local_peer_id());
    assert!(network.peer_record_valid(&peer_record).is_none());
}

#[tokio::test]
async fn test_local_record_has_no_expiry() {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let record = peer1.network.get_peer_record();
    // Must be `None`. libp2p's PutRecordJob::poll only refreshes `expires` via
    // `.or_else(...)`, so a `Some(_)` value here causes the local RecordIter to
    // filter our row after `kad_record_ttl` and the node goes invisible. See
    // get_peer_record() for the full reasoning.
    assert!(
        record.expires.is_none(),
        "local record must have expires: None (see nemesis-1 in security-eval-kad-table-bug.md)"
    );
}

/// End-to-end check on the nemesis-1 fix: peer 1's *own* record must survive
/// past `kad_record_ttl` (because `get_peer_record` sets `expires: None` and
/// libp2p's `PutRecordJob` keeps refreshing it), while peer 1's *copy of peer
/// 2's* record must drop off the read path once `expires: Some(_)` (filled in
/// by peer 2's libp2p before sending) lapses with no further republishes.
#[tokio::test]
async fn test_killed_peer_record_expires_local_record_survives() -> eyre::Result<()> {
    // Short TTL keeps the test fast. Publication interval is the libp2p check
    // cadence for record refresh — must be < TTL.
    let short_ttl = Duration::from_secs(2);
    let mut network_config = NetworkConfig::default();
    network_config.libp2p_config_mut().kad_record_ttl = short_ttl;
    network_config.libp2p_config_mut().kad_publication_interval = Duration::from_millis(500);

    let TestTypes { peer1, peer2, _task_manager } =
        create_test_types_with_config::<TestWorkerRequest, TestWorkerResponse>(network_config);

    let NetworkPeer {
        config: config_1, network_handle: peer1_handle, network: peer1_network, ..
    } = peer1;
    let NetworkPeer {
        config: config_2, network_handle: peer2_handle, network: peer2_network, ..
    } = peer2;

    let peer1_bls = config_1.key_config().primary_public_key();
    let peer2_bls = config_2.key_config().primary_public_key();
    let peer2_net = config_2.primary_networkkey();
    let peer2_addr = config_2.primary_address();

    let peer1_task = tokio::spawn(async move { peer1_network.run().await });
    let peer2_task = tokio::spawn(async move { peer2_network.run().await });

    peer1_handle.start_listening(config_1.primary_address()).await?;
    peer2_handle.start_listening(peer2_addr.clone()).await?;

    // Wire peer1 -> peer2 and wait for kad cross-publication.
    peer1_handle.add_trusted_peer_and_dial(peer2_bls, peer2_net, peer2_addr).await?;
    wait_for_peer_discovery(&peer1_handle, peer2_bls, Duration::from_secs(5)).await?;

    // Poll until peer1 has stored peer2's record. `publish_our_data_to_peer`
    // fires on PeerConnected, but the actual record-store write only lands
    // after a round trip.
    wait_until(Duration::from_secs(5), "peer1 receives peer2's kad record", || async {
        Ok(peer1_handle.kad_store_get(peer2_bls).await?.is_some())
    })
    .await?;

    // Sanity: peer1's own record also present before peer2 dies.
    assert!(
        peer1_handle.kad_store_get(peer1_bls).await?.is_some(),
        "own record present after startup"
    );

    // Kill peer2 — no more republishes can refresh peer1's stored copy.
    peer2_task.abort();
    let _ = peer2_task.await;

    // Wait past kad_record_ttl. Buffer covers SystemTime/Instant rounding.
    tokio::time::sleep(short_ttl + Duration::from_secs(1)).await;

    // Invariant 1: peer1's own record (expires: None) is still readable.
    assert!(
        peer1_handle.kad_store_get(peer1_bls).await?.is_some(),
        "local record must survive past kad_record_ttl (regression on nemesis-1 fix)"
    );

    // Invariant 2: peer1's copy of peer2's record (expires: Some(_)) is filtered on read.
    assert!(
        peer1_handle.kad_store_get(peer2_bls).await?.is_none(),
        "expired peer record must not be returned from kad store"
    );

    peer1_task.abort();
    let _ = peer1_task.await;
    drop(_task_manager);
    Ok(())
}

#[tokio::test]
async fn test_newer_kad_record_replaced() -> eyre::Result<()> {
    let TestTypes { peer1, mut peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let peer2_new_record = peer2.network.get_peer_record();
    // create valid peer2 record with old timestamp
    let mut peer2_info = peer2.network.node_record.info.clone();
    // timestamp in the past
    peer2_info.timestamp = now() - 10_000;
    // sign record
    let signature = peer2.config.key_config().request_signature_direct(&encode(&peer2_info));
    let old_record = NodeRecord { info: peer2_info, signature };
    // assert old record is valid
    let peer2_pubkey = peer2.config.key_config().primary_public_key();
    assert!(old_record.clone().verify(&peer2_pubkey).is_some());
    // store with peer2 to generate old kad record
    peer2.network.node_record = old_record;
    let old_kad_record = peer2.network.get_peer_record();
    // put old record in store
    network.swarm.behaviour_mut().kademlia.store_mut().put(old_kad_record.clone())?;
    // assert kad store is old
    let store_record = network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&peer2_new_record.key)
        .expect("peer2 record in local kad store");
    // Records carry an `Instant` `expires`, which loses precision across the
    // SystemTime <-> Instant round-trip in `KadRecord`. Compare the stable fields.
    assert_eq!(old_kad_record.key, store_record.key);
    assert_eq!(old_kad_record.value, store_record.value);
    assert_eq!(old_kad_record.publisher, store_record.publisher);

    // process new put request with newer record
    network
        .process_kad_put_request(*peer2.network.swarm.local_peer_id(), peer2_new_record.clone())?;
    // assert kad store is updated
    let store_record = network
        .swarm
        .behaviour_mut()
        .kademlia
        .store_mut()
        .get(&peer2_new_record.key)
        .expect("peer2 record in local kad store");
    assert_eq!(store_record.key, peer2_new_record.key);
    assert_eq!(store_record.value, peer2_new_record.value);
    assert_eq!(store_record.publisher, peer2_new_record.publisher);

    Ok(())
}

/// A validator that advertises an [`RpcInfo`] on its worker [`NodeRecord`] is
/// discoverable from a new node that joins the network and runs
/// `find_authorities`. The new node's `get_all_validator_rpcs` exposes exactly
/// the advertised entry, and lookups for non-advertising validators return
/// `None`.
#[tokio::test]
async fn test_advertise_rpc_via_kad() -> eyre::Result<()> {
    use crate::types::RpcInfo;

    let num_network_peers = 5;

    // Set up multiple peers with the default config
    let (mut target_peer, mut committee, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(num_network_peers).unwrap(),
            None,
        );

    // inject an RPC descriptor into target peer's signed node record before spawn —
    // simulates a validator that configured an `rpc` endpoint.
    let rpc = RpcInfo {
        http: "https://node1.example:8545/".parse().expect("http url"),
        ws: Some("wss://node1.example:8546/".parse().expect("ws url")),
    };
    let mut target_network = target_peer.network.take().expect("target network is some");
    let mut target_info = target_network.node_record.info.clone();
    target_info.rpc = Some(rpc.clone());
    let target_signature =
        target_peer.config.key_config().request_signature_direct(&encode(&target_info));
    target_network.node_record = NodeRecord { info: target_info, signature: target_signature };

    let target_peer_bls = target_peer.config.key_config().primary_public_key();
    let target_peer_net = target_peer.config.primary_networkkey();
    let target_peer_addr = target_peer.config.primary_address();
    let id = target_peer.config.authority().as_ref().expect("authority").id();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?id, ?res, "network shutdown");
    });
    target_peer.network_handle.start_listening(target_peer_addr.clone()).await?;

    // spawn the rest of the committee, connect each to target
    for peer in committee.iter_mut() {
        let peer_network = peer.network.take().expect("peer network is some");
        let id = peer.config.authority().as_ref().expect("authority").id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?id, ?res, "network shutdown");
        });
        peer.network_handle.start_listening(peer.config.primary_address()).await?;
        peer.network_handle
            .add_trusted_peer_and_dial(
                target_peer_bls,
                target_peer_net.clone(),
                target_peer_addr.clone(),
            )
            .await?;
        let peer_handle = &peer.network_handle;
        wait_until(Duration::from_secs(5), "peer connects to target", move || async move {
            Ok(peer_handle.connected_peers().await?.contains(&target_peer_bls))
        })
        .await?;
    }

    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // spawn the 6th non-validator peer
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: nvv_config, network_handle: nvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("nvv network run failed!");
    });
    nvv.start_listening(nvv_config.primary_address()).await?;
    // seed the nvv's committee (target + the rest) BEFORE it connects so target's rpc-bearing
    // record is retained the moment it arrives via kad; the committee gate added in #827 drops
    // records for non-committee keys, and a late seed misses the record's first delivery. an nvv
    // following the chain learns the committee from epoch state.
    let committee_keys = std::iter::once(target_peer_bls)
        .chain(committee.iter().map(|p| p.config.key_config().primary_public_key()))
        .collect();
    nvv.update_committees(Default::default(), committee_keys, Default::default()).await?;
    nvv.add_trusted_peer_and_dial(
        target_peer_bls,
        target_peer_net.clone(),
        target_peer_addr.clone(),
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // ask the network to locate every committee member via kad
    let authorities: Vec<BlsPublicKey> =
        committee.iter().map(|p| p.config.key_config().primary_public_key()).collect();
    nvv.find_authorities(authorities.clone()).await?;
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 5)).await;

    // snapshot the advertised RPCs known on the NVV; only the target advertised
    let mut all = nvv.get_all_validator_rpcs().await?;
    all.sort_by_key(|(bls, _)| *bls);
    assert_eq!(all.len(), 1, "exactly one advertised rpc expected; got {all:?}");
    let (key, advertised) = &all[0];
    assert_eq!(*key, target_peer_bls);
    assert_eq!(*advertised, rpc);

    // direct lookup for the advertising peer matches
    assert_eq!(nvv.get_validator_rpc(target_peer_bls).await?, Some(rpc));
    // and a non-advertising peer returns None
    let other_bls = committee[0].config.key_config().primary_public_key();
    assert_eq!(nvv.get_validator_rpc(other_bls).await?, None);

    Ok(())
}

/// Documents the rolling-upgrade contract: a pre-upgrade kad record (no `rpc`
/// field) decodes through the legacy fallback, verifies against the legacy
/// encoding it was signed over, and is promoted into `known_peers` with
/// `rpc: None`. The honest not-yet-upgraded sender is not penalized.
#[tokio::test]
async fn test_pre_upgrade_record_accepted_with_default_rpc() -> eyre::Result<()> {
    use libp2p::kad;
    use serde::{Deserialize, Serialize};
    use tn_types::{BlsSignature, Multiaddr, NetworkPublicKey, TimestampSec};

    /// Pre-upgrade NetworkInfo shape (no `rpc` field). Field order MUST mirror
    /// the historical layout so encoded bytes match what an old peer would
    /// have written to disk.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct OldNetworkInfo {
        pubkey: NetworkPublicKey,
        multiaddrs: Vec<Multiaddr>,
        timestamp: TimestampSec,
    }

    /// Pre-upgrade NodeRecord shape.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct OldNodeRecord {
        info: OldNetworkInfo,
        signature: BlsSignature,
    }

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let owner_bls = peer2.config.key_config().primary_public_key();
    let owner_peer_id = *peer2.network.swarm.local_peer_id();

    // build a valid pre-upgrade record signed with peer2's BLS key
    let old_info = OldNetworkInfo {
        pubkey: peer2.config.key_config().primary_network_public_key(),
        multiaddrs: vec![peer2.config.primary_address()],
        timestamp: now(),
    };
    let signature = peer2.config.key_config().request_signature_direct(&encode(&old_info));
    let old_record = OldNodeRecord { info: old_info, signature };

    let kad_record = kad::Record {
        key: kad::RecordKey::new(&owner_bls),
        value: encode(&old_record),
        publisher: Some(owner_peer_id),
        expires: None,
    };

    // peer_record_valid accepts pre-upgrade bytes via the legacy decode
    // fallback; the missing rpc field defaults to None.
    let (key, node_record) =
        network.peer_record_valid(&kad_record).expect("legacy record accepted");
    assert_eq!(key, owner_bls);
    assert!(node_record.info.rpc.is_none());
    assert_eq!(node_record.info.multiaddrs, vec![peer2.config.primary_address()]);

    // owner is a committee member so the gated discovery path (#827) retains its record;
    // production applies committee membership from epoch state before processing records.
    network.swarm.behaviour_mut().peer_manager.update_committees(
        Default::default(),
        std::iter::once(owner_bls).collect(),
        Default::default(),
    );

    // an inbound put request stores the record and promotes it into `known_peers`
    network.process_kad_put_request(owner_peer_id, kad_record.clone())?;
    assert!(network.swarm.behaviour_mut().kademlia.store_mut().get(&kad_record.key).is_some());

    let (peer_id, multiaddrs) = network
        .swarm
        .behaviour()
        .peer_manager
        .auth_to_peer(owner_bls)
        .expect("legacy record promoted into known_peers");
    assert_eq!(peer_id, owner_peer_id);
    assert_eq!(multiaddrs, vec![peer2.config.primary_address()]);

    // no rpc was advertised so lookups return None
    assert!(network.swarm.behaviour().peer_manager.get_rpc(&owner_bls).is_none());
    assert!(network.swarm.behaviour_mut().peer_manager.current_committee_rpcs().is_empty());

    // honest pre-upgrade sender is not penalized
    assert!(!network.swarm.behaviour().peer_manager.peer_banned(&owner_peer_id));

    Ok(())
}

/// Regression (GHSA-j24g-gqj4-9vj8): an inbound `PUT_VALUE` with `publisher =
/// None` and a `key` equal to the node's own discovery-record key must NOT
/// delete the node's own record from the local store.
///
/// `publisher = None` short-circuits to the reject path before any signature
/// check, and the pre-fix reject path called `remove_record(&record.key)` with
/// the sender-supplied key. Because the node's own record is the only
/// locally-published record, that call deleted it, and since we only re-provide
/// our record at startup, the deletion persisted until restart. The reject path
/// must perform no store mutation keyed on attacker-supplied input.
#[tokio::test]
async fn test_publisherless_put_cannot_delete_own_record() -> eyre::Result<()> {
    use libp2p::kad;

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    // Seed our own discovery record into the local store with the same record
    // `provide_our_data` publishes at startup: keyed on our BLS public key with
    // `publisher = Some(local_peer_id)`, the only record `remove_record` can touch.
    let own_record = network.get_peer_record();
    network.swarm.behaviour_mut().kademlia.store_mut().put(own_record.clone())?;
    assert!(
        network.swarm.behaviour_mut().kademlia.store_mut().get(&own_record.key).is_some(),
        "own discovery record present before the attack",
    );

    // An arbitrary connected peer sends a PUT keyed on our own record key with no
    // publisher. No signature and no committee membership are required.
    let attacker = *peer2.network.swarm.local_peer_id();
    let malicious = kad::Record {
        key: own_record.key.clone(),
        value: vec![0xde, 0xad, 0xbe, 0xef],
        publisher: None,
        expires: None,
    };
    network.process_kad_put_request(attacker, malicious)?;

    // The node's own authoritative record must survive intact: it is only
    // re-provided at startup, so a deletion here would persist until restart.
    // Assert it is still present AND unchanged, covering both the "delete" and
    // "mutate" halves of the advisory's acceptance criterion. (Compare the stable
    // fields; `expires` loses precision across the store's SystemTime<->Instant
    // round-trip, as noted in `test_newer_kad_record_replaced`.)
    let survived = network.swarm.behaviour_mut().kademlia.store_mut().get(&own_record.key).expect(
        "own discovery record must survive a publisher=None reject-path put (GHSA-j24g-gqj4-9vj8)",
    );
    assert_eq!(survived.key, own_record.key);
    assert_eq!(survived.value, own_record.value);
    assert_eq!(survived.publisher, own_record.publisher);

    Ok(())
}

/// A signed record advertising an RPC endpoint with a well-formed URL but the
/// wrong scheme is accepted (the signature is authentic) and promoted with the
/// malformed endpoint stripped, without penalizing the sender.
#[tokio::test]
async fn test_malformed_rpc_scheme_stripped_on_promotion() -> eyre::Result<()> {
    use libp2p::kad;

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let owner_bls = peer2.config.key_config().primary_public_key();
    let owner_peer_id = *peer2.network.swarm.local_peer_id();

    // parseable url with a scheme RpcInfo::validate rejects
    let rpc = RpcInfo { http: "ftp://validator.example.com:8545/".parse()?, ws: None };
    let key_config = peer2.config.key_config();
    let node_record = NodeRecord::build(
        key_config.primary_network_public_key(),
        peer2.config.primary_address(),
        Some(rpc),
        |data| key_config.request_signature_direct(data),
    );

    let kad_record = kad::Record {
        key: kad::RecordKey::new(&owner_bls),
        value: encode(&node_record),
        publisher: Some(owner_peer_id),
        expires: None,
    };

    // owner is a committee member so the gated discovery path (#827) retains its record;
    // production applies committee membership from epoch state before processing records.
    network.swarm.behaviour_mut().peer_manager.update_committees(
        Default::default(),
        std::iter::once(owner_bls).collect(),
        Default::default(),
    );

    network.process_kad_put_request(owner_peer_id, kad_record.clone())?;

    // the record was stored — the signature is authentic
    assert!(network.swarm.behaviour_mut().kademlia.store_mut().get(&kad_record.key).is_some());

    // promoted into known_peers with multiaddrs intact but the rpc stripped
    let (peer_id, multiaddrs) = network
        .swarm
        .behaviour()
        .peer_manager
        .auth_to_peer(owner_bls)
        .expect("record promoted into known_peers");
    assert_eq!(peer_id, owner_peer_id);
    assert_eq!(multiaddrs, vec![peer2.config.primary_address()]);
    assert!(network.swarm.behaviour().peer_manager.get_rpc(&owner_bls).is_none());
    assert!(network.swarm.behaviour_mut().peer_manager.current_committee_rpcs().is_empty());

    // the sender was not penalized
    assert!(!network.swarm.behaviour().peer_manager.peer_banned(&owner_peer_id));

    Ok(())
}

/// Startup over a kad store holding a pre-upgrade record and a corrupt record:
/// the node constructs cleanly (no panicking decode), the legacy peer is
/// promoted into `known_peers` with `rpc: None`, and the corrupt record is
/// purged from the persistent store. The legacy record's original signed bytes
/// are preserved.
#[tokio::test]
async fn test_startup_tolerates_legacy_and_corrupt_kad_records() -> eyre::Result<()> {
    use libp2p::kad;
    use serde::{Deserialize, Serialize};
    use tn_types::{BlsKeypair, BlsSignature, Multiaddr, NetworkPublicKey, TimestampSec};

    /// Pre-upgrade NetworkInfo shape (no `rpc` field).
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct OldNetworkInfo {
        pubkey: NetworkPublicKey,
        multiaddrs: Vec<Multiaddr>,
        timestamp: TimestampSec,
    }

    /// Pre-upgrade NodeRecord shape.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct OldNodeRecord {
        info: OldNetworkInfo,
        signature: BlsSignature,
    }

    let all_nodes = CommitteeFixture::builder(MemDatabase::default)
        .with_network_config(NetworkConfig::default())
        .build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let task_manager = TaskManager::default();

    let owner_bls = config_2.key_config().primary_public_key();
    let garbage_bls = *BlsKeypair::generate(&mut rand::rng()).public();
    let db = MemDatabase::default();

    // seed the DB before the network starts, simulating records that survived
    // a node restart from before the upgrade
    {
        let mut kad_store = KadStore::new(db.clone(), config_1.key_config(), NetworkType::Primary);

        // valid pre-upgrade record signed by authority 2 over the legacy encoding
        let old_info = OldNetworkInfo {
            pubkey: config_2.key_config().primary_network_public_key(),
            multiaddrs: vec![config_2.primary_address()],
            timestamp: now(),
        };
        let signature = config_2.key_config().request_signature_direct(&encode(&old_info));
        let old_record = OldNodeRecord { info: old_info, signature };
        kad_store.put(kad::Record {
            key: kad::RecordKey::new(&owner_bls),
            value: encode(&old_record),
            publisher: None,
            expires: None,
        })?;

        // garbage bytes under a valid BLS key — fails both decode layouts
        kad_store.put(kad::Record {
            key: kad::RecordKey::new(&garbage_bls),
            value: vec![0xde, 0xad, 0xbe, 0xef],
            publisher: None,
            expires: None,
        })?;
    }

    // construct the network over the seeded DB — this is the startup path that
    // panicked on pre-upgrade bytes before the compat decode
    let (tx, _network_events) = mpsc::channel(10);
    let network_key = config_1.key_config().primary_network_keypair().clone();
    let network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_1.network_config(),
        tx,
        config_1.key_config().clone(),
        network_key,
        db.clone(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        config_1.primary_address(),
        None,
    )
    .expect("network constructs over a store holding legacy + corrupt records");

    // the legacy peer was promoted with multiaddrs intact and rpc defaulted
    let (_, multiaddrs) = network
        .swarm
        .behaviour()
        .peer_manager
        .auth_to_peer(owner_bls)
        .expect("legacy peer in known_peers");
    assert_eq!(multiaddrs, vec![config_2.primary_address()]);
    assert!(network.swarm.behaviour().peer_manager.get_rpc(&owner_bls).is_none());

    // the corrupt record was purged from the persistent store; the legacy
    // record's original signed bytes were preserved
    let store = KadStore::new(db, config_1.key_config(), NetworkType::Primary);
    assert!(store.get(&kad::RecordKey::new(&garbage_bls)).is_none(), "corrupt record purged");
    assert!(store.get(&kad::RecordKey::new(&owner_bls)).is_some(), "legacy record preserved");

    Ok(())
}

/// Records restored from the persisted kad store at startup are UNPINNED: the store legitimately
/// holds arbitrary signature-valid third-party records (DHT storage duty), so a restart must not
/// convert them into permanently pinned `known_peers` entries. Restored records resolve until the
/// first committee rotation, which prunes every key outside a tracked slot — restoring the issue
/// #827 bound.
#[tokio::test]
async fn test_restored_records_survive_only_committee_rotation() -> eyre::Result<()> {
    use libp2p::kad;
    use tn_types::Signer as _;

    let all_nodes = CommitteeFixture::builder(MemDatabase::default)
        .with_network_config(NetworkConfig::default())
        .build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let task_manager = TaskManager::default();

    let authority_bls = config_2.key_config().primary_public_key();

    // a third party whose signature-valid record the node stored as its DHT storage duty
    let outsider_keypair = BlsKeypair::generate(&mut StdRng::from_seed([5; 32]));
    let outsider_bls = *outsider_keypair.public();

    let db = MemDatabase::default();

    // seed the DB before the network starts with two VALID records: one for a committee
    // authority and one for the non-committee outsider
    {
        let mut kad_store = KadStore::new(db.clone(), config_1.key_config(), NetworkType::Primary);

        let key_config_2 = config_2.key_config();
        let authority_record = NodeRecord::build(
            key_config_2.primary_network_public_key(),
            config_2.primary_address(),
            None,
            |data| key_config_2.request_signature_direct(data),
        );
        kad_store.put(kad::Record {
            key: kad::RecordKey::new(&authority_bls),
            value: encode(&authority_record),
            publisher: None,
            expires: None,
        })?;

        let outsider_netkey: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().into();
        let outsider_record =
            NodeRecord::build(outsider_netkey, create_multiaddr(None), None, |data| {
                outsider_keypair.sign(data)
            });
        kad_store.put(kad::Record {
            key: kad::RecordKey::new(&outsider_bls),
            value: encode(&outsider_record),
            publisher: None,
            expires: None,
        })?;
    }

    // construct the network over the seeded DB — the startup restore path
    let (tx, _network_events) = mpsc::channel(10);
    let network_key = config_1.key_config().primary_network_keypair().clone();
    let mut network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_1.network_config(),
        tx,
        config_1.key_config().clone(),
        network_key,
        db,
        task_manager.get_spawner(),
        NetworkType::Primary,
        config_1.primary_address(),
        None,
    )?;

    // both restored records resolve after startup
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(authority_bls).is_some(),
        "restored authority record resolvable at startup"
    );
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(outsider_bls).is_some(),
        "restored outsider record resolvable at startup"
    );

    // the first committee rotation tracks only the authority; production applies committee
    // membership from epoch state the same way
    network.swarm.behaviour_mut().peer_manager.update_committees(
        Default::default(),
        std::iter::once(authority_bls).collect(),
        Default::default(),
    );

    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(authority_bls).is_some(),
        "restored committee authority must survive rotation"
    );
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(outsider_bls).is_none(),
        "restored non-committee record must be pruned at the first rotation"
    );

    Ok(())
}

/// Startup restore skips the node's own persisted kad record: both primary and worker key their
/// record by the primary BLS key, and there is no point caching ourselves as a known peer.
#[tokio::test]
async fn test_restore_skips_own_record() -> eyre::Result<()> {
    use libp2p::kad;

    let all_nodes = CommitteeFixture::builder(MemDatabase::default)
        .with_network_config(NetworkConfig::default())
        .build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let config_1 = authority_1.consensus_config();
    let task_manager = TaskManager::default();

    let own_bls = config_1.key_config().primary_public_key();
    let db = MemDatabase::default();

    // seed the DB with a valid record for the node's OWN primary BLS key, built the same way
    // the node builds its own record
    {
        let mut kad_store = KadStore::new(db.clone(), config_1.key_config(), NetworkType::Primary);
        let key_config = config_1.key_config();
        let own_record = NodeRecord::build(
            key_config.primary_network_public_key(),
            config_1.primary_address(),
            None,
            |data| key_config.request_signature_direct(data),
        );
        kad_store.put(kad::Record {
            key: kad::RecordKey::new(&own_bls),
            value: encode(&own_record),
            publisher: None,
            expires: None,
        })?;
    }

    // construct the network over the seeded DB — the startup restore path
    let (tx, _network_events) = mpsc::channel(10);
    let network_key = config_1.key_config().primary_network_keypair().clone();
    let network = ConsensusNetwork::<
        TestWorkerRequest,
        TestWorkerResponse,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
    >::new(
        config_1.network_config(),
        tx,
        config_1.key_config().clone(),
        network_key,
        db,
        task_manager.get_spawner(),
        NetworkType::Primary,
        config_1.primary_address(),
        None,
    )?;

    // the restore loop skipped our own record
    assert!(
        network.swarm.behaviour().peer_manager.auth_to_peer(own_bls).is_none(),
        "own record must be skipped by the startup restore"
    );

    Ok(())
}

/// Regression for issue #743: a response from a peer whose BLS identity is not
/// yet resolved must surface as the transient `PeerUnresolved`, never the
/// misleading `PeerMissing`, because the response payload itself is genuine.
#[test]
fn unresolved_response_reports_peer_unresolved_not_peer_missing() -> eyre::Result<()> {
    let response = TestWorkerResponse::MissingBatches { batches: vec![] };
    let result = resolve_response(None, response);
    assert_matches!(result, Err(NetworkError::PeerUnresolved));
    Ok(())
}

/// A resolved responder identity is attached to the genuine response payload.
#[test]
fn resolved_response_attaches_identity_to_payload() -> eyre::Result<()> {
    let bls = *BlsKeypair::generate(&mut StdRng::from_seed([7; 32])).public();
    let response = TestWorkerResponse::MissingBatches { batches: vec![] };
    let NetworkResponseMessage { peer, result } = resolve_response(Some(bls), response.clone())?;
    assert_eq!(peer, bls);
    assert_eq!(result, response);
    Ok(())
}

/// A minimal accepted gossip payload for delivery-mapping tests.
fn test_gossip_message() -> GossipMessage {
    GossipMessage {
        source: None,
        data: Vec::from("gossip-payload".as_bytes()),
        sequence_number: None,
        topic: TopicHash::from_raw(TEST_TOPIC),
    }
}

/// Regression for issue #776: an accepted gossip message whose relaying peer's
/// BLS identity has not yet resolved must still be delivered (carried as
/// `None`), never dropped. The author is authenticated during gossip
/// verification, and gossipsub will not re-deliver the `message_id` once the
/// identity resolves, so dropping here would lose the message for good.
#[test]
fn accepted_gossip_with_unresolved_relayer_is_delivered() -> eyre::Result<()> {
    let message = test_gossip_message();
    let event: NetworkEvent<TestWorkerRequest, TestWorkerResponse> =
        accepted_gossip_event(message.clone(), None, None);
    assert_matches!(
        event,
        NetworkEvent::Gossip(payload)
            if payload.relayer.is_none()
                && payload.author.is_none()
                && payload.message.data == message.data
    );
    Ok(())
}

/// A resolved relayer identity is attached to the delivered gossip event.
#[test]
fn accepted_gossip_with_resolved_relayer_carries_identity() -> eyre::Result<()> {
    let bls = *BlsKeypair::generate(&mut StdRng::from_seed([9; 32])).public();
    let message = test_gossip_message();
    let event: NetworkEvent<TestWorkerRequest, TestWorkerResponse> =
        accepted_gossip_event(message, Some(bls), None);
    assert_matches!(event, NetworkEvent::Gossip(payload) if payload.relayer == Some(bls));
    Ok(())
}

/// Regression test for issue #828: `published_to_peers` is a per-process-lifetime de-dup gate that
/// is never cleaned on disconnect, so as an unbounded set it grew once per distinct `PeerId` ever
/// seen and would eventually OOM a RAM-capped node. It is now a capacity-bounded LRU: a flood of
/// fresh peer identities must pin it at [`MAX_PUBLISHED_TO_PEERS`] and never grow past it.
#[tokio::test]
async fn test_published_to_peers_is_bounded() {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    // The field is initialized at the intended cap and starts empty.
    assert_eq!(network.published_to_peers.cap(), MAX_PUBLISHED_TO_PEERS);
    assert_eq!(network.published_to_peers.len(), 0);

    // Each fresh, never-before-seen peer is a first-time push (this is exactly what the
    // `PeerEvent::PeerConnected` arm does when it sees a new `PeerId`). Overrun the cap and
    // assert the set never grows past it.
    let cap = MAX_PUBLISHED_TO_PEERS.get();
    for _ in 0..(cap + 100) {
        let peer_id = PeerId::random();
        assert!(
            network.mark_published_to_peer(peer_id),
            "a never-before-seen peer must register as a first-time push"
        );
        assert!(
            network.published_to_peers.len() <= cap,
            "published_to_peers must never exceed its LRU capacity"
        );
    }

    // The flood pinned the set at capacity rather than growing without bound.
    assert_eq!(
        network.published_to_peers.len(),
        cap,
        "a flood of distinct peers fills the LRU to exactly its capacity and stays there"
    );
}

/// Regression test for issue #828: bounding `published_to_peers` must not reintroduce the kad
/// re-push amplification the gate exists to prevent. An actively (re)connecting peer must stay
/// resident in the LRU - so it keeps de-duping and is never re-pushed to - even while a flood of
/// fresh peers churns the rest of the set.
#[tokio::test]
async fn test_published_to_peers_keeps_active_peer_resident() {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;

    // First sight of our sticky peer warrants a one-time push.
    let sticky = PeerId::random();
    assert!(
        network.mark_published_to_peer(sticky),
        "first sight of a peer must register as a first-time push"
    );

    // The sticky peer keeps reconnecting while enough fresh identities connect to overrun the
    // cap. Because every reconnect promotes it to most-recently-used, it is never the eviction
    // victim - unlike a plain insertion-order set, which would drop it after `cap` fresh inserts.
    for _ in 0..(MAX_PUBLISHED_TO_PEERS.get() + 100) {
        assert!(
            !network.mark_published_to_peer(sticky),
            "a reconnecting peer we have already pushed to must not be re-pushed"
        );
        network.mark_published_to_peer(PeerId::random());
    }

    // Despite the flood, the continually-active peer survived eviction and still de-dups.
    assert!(
        !network.mark_published_to_peer(sticky),
        "an actively reconnecting peer must survive LRU eviction (no re-push storm)"
    );
}

/// Regression for issue #819: a resolved author identity is carried on the delivered gossip
/// event so the application layer can charge an author-content fault to the author rather than
/// the forwarding relayer.
#[test]
fn accepted_gossip_carries_resolved_author_identity() -> eyre::Result<()> {
    let author = *BlsKeypair::generate(&mut StdRng::from_seed([11; 32])).public();
    let message = test_gossip_message();
    let event: NetworkEvent<TestWorkerRequest, TestWorkerResponse> =
        accepted_gossip_event(message, None, Some(author));
    assert_matches!(event, NetworkEvent::Gossip(payload) if payload.author == Some(author));
    Ok(())
}

/// Finding 1 (#819): the network-layer reject path must never Fatal-ban the relaying peer for an
/// author-attributable reject. An `UnauthorizedAuthor` reject charges the resolved author, and no
/// one when the author is unresolved (anonymous message or this node's view lag) — but never the
/// relayer, regardless of whether either identity has resolved.
#[test]
fn author_attributable_reject_charges_the_author_never_the_relayer() {
    for relayer_resolved in [true, false] {
        assert_eq!(
            RejectReason::UnauthorizedAuthor.penalty(relayer_resolved, true),
            RejectPenalty::FatalAuthor
        );
        assert_eq!(
            RejectReason::UnauthorizedAuthor.penalty(relayer_resolved, false),
            RejectPenalty::Skip
        );
    }
}

/// An oversized payload is the only relayer-attributable reject — the size bound is a compile-time
/// protocol constant (`MAX_GOSSIP_MESSAGE_SIZE`), identical on every honest node and enforced on
/// both the publish and receive paths, so an honest peer never originates or forwards one — and it
/// is charged to the forwarder only once its BLS identity has resolved (the author's resolution is
/// irrelevant), mirroring the Accept path's unresolved-relayer skip.
#[test]
fn oversized_reject_penalizes_only_a_resolved_relayer() {
    for author_resolved in [true, false] {
        assert_eq!(
            RejectReason::TooLarge.penalty(true, author_resolved),
            RejectPenalty::FatalRelayer
        );
        assert_eq!(RejectReason::TooLarge.penalty(false, author_resolved), RejectPenalty::Skip);
    }
}

/// #872: a node must never originate a gossip payload larger than `MAX_GOSSIP_MESSAGE_SIZE`. Honest
/// peers reject an oversized payload as `RejectReason::TooLarge` and Fatal-attribute it to the
/// relaying peer, which on the first hop is the originator — so the publish path enforces the same
/// bound as `verify_gossip`, refusing an oversized message locally with
/// `PublishError::MessageTooLarge` instead of letting the originator be banned by its peers.
#[tokio::test]
async fn oversized_publish_is_refused_locally() -> eyre::Result<()> {
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { network_handle: cvv, network, .. } = peer1;
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // one byte over the bound is refused at origination, before it can reach any peer
    let too_big = vec![0u8; MAX_GOSSIP_MESSAGE_SIZE + 1];
    let err = cvv.publish(TEST_TOPIC.into(), too_big).await.expect_err("oversized publish refused");
    assert_matches!(err, NetworkError::Publish(PublishError::MessageTooLarge));

    Ok(())
}

/// Regression (issue #1010): the swarm must cap the number of concurrent established connections a
/// single peer may hold. Without the `connection_limits::Behaviour` installed by `TNBehavior::new`,
/// one unbanned peer could open connections with no per-peer ceiling: the peer-count gate counts
/// *distinct* `PeerId`s, and the inbound admission callback rejects only self-connections and
/// banned peers.
///
/// This drives the exact behaviour the production path installs (`connection_limits_behaviour`)
/// through the swarm's admission + registration sequence and asserts the cap (1) fires at
/// `MAX_ESTABLISHED_CONNECTIONS_PER_PEER + 1`, (2) is per-peer (a distinct peer is unaffected), and
/// (3) counts *concurrent* connections (closing one frees a slot). Before the fix there is no such
/// behaviour, so no admission is ever denied on this basis.
#[test]
fn connection_limit_caps_established_connections_per_peer() {
    use libp2p::{
        core::ConnectedPoint,
        swarm::{
            behaviour::ConnectionEstablished, ConnectionClosed, ConnectionId, FromSwarm,
            NetworkBehaviour as _,
        },
    };

    // exactly the behaviour installed in production
    let mut limits = super::connection_limits_behaviour();
    let cap = usize::try_from(super::MAX_ESTABLISHED_CONNECTIONS_PER_PEER).expect("cap fits usize");

    let peer = PeerId::random();
    let addr = create_multiaddr(None);
    let listener =
        ConnectedPoint::Listener { local_addr: addr.clone(), send_back_addr: addr.clone() };

    // the first `cap` inbound connections from one peer are admitted and registered
    (0..cap).for_each(|i| {
        let id = ConnectionId::new_unchecked(i);
        assert!(
            limits.handle_established_inbound_connection(id, peer, &addr, &addr).is_ok(),
            "connection {i} from a peer within the cap must be admitted"
        );
        // production path: a successful admission is followed by
        // `FromSwarm::ConnectionEstablished`, which is what actually increments the
        // per-peer counter inside `connection_limits`
        limits.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id: peer,
            connection_id: id,
            endpoint: &listener,
            failed_addresses: &[],
            other_established: i,
        }));
    });

    // the next connection from the SAME peer is denied by the per-peer cap
    let over_cap = ConnectionId::new_unchecked(cap);
    assert!(
        limits.handle_established_inbound_connection(over_cap, peer, &addr, &addr).is_err(),
        "connection number {} from the same peer must be denied by the per-peer cap",
        cap + 1
    );

    // a distinct peer is unaffected: the cap is per-peer, not global
    let other_peer = PeerId::random();
    assert!(
        limits
            .handle_established_inbound_connection(
                ConnectionId::new_unchecked(cap + 1),
                other_peer,
                &addr,
                &addr,
            )
            .is_ok(),
        "a distinct peer must not be denied because another peer reached the cap"
    );

    // closing one of the first peer's connections frees a slot: the cap tracks *concurrent*
    // connections, and the decrement path must work
    limits.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
        peer_id: peer,
        connection_id: ConnectionId::new_unchecked(0),
        endpoint: &listener,
        cause: None,
        remaining_established: cap - 1,
    }));
    assert!(
        limits
            .handle_established_inbound_connection(
                ConnectionId::new_unchecked(cap + 2),
                peer,
                &addr,
                &addr,
            )
            .is_ok(),
        "after one of the peer's connections closes, a fresh connection from it must be admitted"
    );
}
