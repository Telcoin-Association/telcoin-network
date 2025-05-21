//! Tests networking using libp2p between peers.

use super::*;
use crate::common::{
    create_multiaddr, TestPrimaryRequest, TestPrimaryResponse, TestWorkerRequest,
    TestWorkerResponse, TEST_HEARTBEAT_INTERVAL,
};
use assert_matches::assert_matches;
use eyre::eyre;
use std::num::NonZeroUsize;
use tn_config::{ConsensusConfig, NetworkConfig};
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{Certificate, Header, TaskManager};
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
            let db = MemDatabase::new_for_network();
            let network = ConsensusNetwork::<Req, Res, MemDatabase>::new(
                config.network_config(),
                tx,
                config.key_config().clone(),
                network_key,
                db,
                task_manager.get_spawner(),
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
    network: Option<ConsensusNetwork<Req, Res, MemDatabase>>,
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
    network: ConsensusNetwork<Req, Res, MemDatabase>,
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
    // custom network config with short heartbeat interval for peer manager
    let mut network_config = NetworkConfig::default();
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
    let peer1_network = ConsensusNetwork::<Req, Res, MemDatabase>::new(
        config_1.network_config(),
        tx1,
        config_1.key_config().clone(),
        network_key_1,
        MemDatabase::new_for_network(),
        task_manager.get_spawner(),
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
    let peer2_network = ConsensusNetwork::<Req, Res, MemDatabase>::new(
        config_2.network_config(),
        tx2,
        config_2.key_config().clone(),
        network_key_2,
        MemDatabase::new_for_network(),
        task_manager.get_spawner(),
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

#[tokio::test]
async fn test_valid_req_res() -> eyre::Result<()> {
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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);
    let batch_res = TestWorkerResponse::MissingBatches { batches: vec![missing_block] };

    // dial peer2
    peer1.dial(peer2_id, peer2_addr).await?;

    // send request and wait for response
    let max_time = Duration::from_secs(5);
    let response_from_peer = peer1.send_request(batch_req.clone(), peer2_id).await?;
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
    let response = timeout(max_time, response_from_peer).await?.expect("outbound id recv")?;
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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1.dial(peer2_id, peer2_addr).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // send request and wait for response
    let _reply = peer1.send_request(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peers().await?;
    assert_eq!(connected_peers.len(), 1);

    // simulate crashed peer 2
    peer2_network_task.abort();
    assert!(peer2_network_task.await.unwrap_err().is_cancelled());

    // allow peer1 to process disconnect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // assert peer is disconnected
    let connected_peers = peer1.connected_peers().await?;
    assert_eq!(connected_peers.len(), 0);

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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    // dial peer2
    peer1.dial(peer2_id, peer2_addr).await?;

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    // send request and wait for response
    let max_time = Duration::from_secs(5);
    let _response = peer1.send_request(batch_req.clone(), peer2_id).await?;

    // peer1 has a pending_request now
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 1);

    // another sanity check
    let connected_peers = peer1.connected_peers().await?;
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
    malicious_peer
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    honest_peer
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let honest_peer_id = honest_peer.local_peer_id().await?;
    let honest_peer_addr =
        honest_peer.listeners().await?.first().expect("honest_peer listen addr").clone();

    // this type already impl `TNMessage` but this could be incorrect message type
    let malicious_msg = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };

    // dial honest peer
    malicious_peer.dial(honest_peer_id, honest_peer_addr).await?;

    // sleep for heartbeat
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    let peer_score_before_msg = honest_peer.peer_score(malicious_peer_id).await?.unwrap();

    // honest peer returns `OutboundFailure` error
    let response_from_peer = malicious_peer.send_request(malicious_msg, honest_peer_id).await?;
    let res = timeout(Duration::from_secs(2), response_from_peer)
        .await?
        .expect("first network event received");

    assert_matches!(res, Err(NetworkError::Outbound(_)));

    // Allow time for penalty to be applied
    tokio::time::sleep(Duration::from_millis(500)).await;

    // TODO: the honest peer penalize the malicious requestor. see Issue #250
    //
    // assert honest peer's score is lower - penalties are applied immediately
    // however, it should be the case that honest peer penalizes the malicious peer
    let peer_score_after_msg = malicious_peer.peer_score(honest_peer_id).await?.unwrap();
    assert!(peer_score_before_msg > peer_score_after_msg);

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
    honest_peer
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    malicious_peer
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let malicious_peer_addr =
        malicious_peer.listeners().await?.first().expect("malicious_peer listen addr").clone();

    // dial malicious_peer
    honest_peer.dial(malicious_peer_id, malicious_peer_addr).await?;

    // send request and wait for malicious response
    let max_time = Duration::from_secs(2);
    let honest_req = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let response_from_peer =
        honest_peer.send_request(honest_req.clone(), malicious_peer_id).await?;
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
    cvv.start_listening(
        config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;
    nvv.start_listening(
        config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;
    let cvv_id = cvv.local_peer_id().await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_peer_ids()).await?;

    // dial cvv
    nvv.dial(cvv_id, cvv_addr).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish on wrong topic - no peers
    let expected_failure = cvv.publish("WRONG_TOPIC".into(), expected_result.clone()).await;
    assert!(expected_failure.is_err());

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(msg, _) = event {
        assert_eq!(msg.data, expected_result);
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
    cvv.start_listening(
        config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;
    nvv.start_listening(
        config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;
    let cvv_id = cvv.local_peer_id().await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // subscribe
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), config_1.committee_peer_ids()).await?;

    // dial cvv
    nvv.dial(cvv_id, cvv_addr).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish correct message and wait to receive
    let _message_id = cvv.publish(TEST_TOPIC.into(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(msg, _) = event {
        assert_eq!(msg.data, expected_result);
    } else {
        panic!("unexpected network event received");
    }

    // remove cvv from whitelist and try to publish again
    nvv.update_authorized_publishers(HashMap::new()).await?;

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

/// Test peer exchanges when too many peers connect
#[tokio::test]
async fn test_peer_exchange_with_excess_peers() -> eyre::Result<()> {
    // Create a custom config with very low peer limits for testing
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().target_num_peers = 4;
    network_config.peer_config_mut().peer_excess_factor = 0.1; // Small excess factor
    network_config.peer_config_mut().excess_peers_reconnection_timeout = Duration::from_secs(10);

    // Set up multiple peers with the custom config
    let (mut target_peer, mut other_peers, _) =
        create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
            NonZeroUsize::new(5).unwrap(),
            Some(network_config.clone()),
        );

    // spawn target network
    let mut target_network = target_peer.network.take().expect("target network is some");
    // Need to disable kademlia peers or the various networks will connect on their own and trip up
    // the test...
    target_network.no_kad_peers_for_test();
    let id = target_peer.config.authority().as_ref().expect("authority").id().peer_id();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?id, ?res, "network shutdown");
    });

    // Start target peer listening
    target_peer
        .network_handle
        .start_listening(
            target_peer
                .config
                .authority()
                .as_ref()
                .expect("authority")
                .primary_network_address()
                .clone(),
        )
        .await?;
    let target_addr = target_peer
        .network_handle
        .listeners()
        .await?
        .first()
        .expect("target peer listen addr")
        .clone();
    let target_peer_id = target_peer.network_handle.local_peer_id().await?;

    debug!(target: "network", ?target_peer_id, "target peer");

    // Start other peers and connect them one by one to the target
    for peer in other_peers.iter_mut() {
        // spawn peer network
        let mut peer_network = peer.network.take().expect("peer network is some");
        peer_network.no_kad_peers_for_test();
        let id = peer.config.authority().as_ref().expect("authority").id().peer_id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?id, ?res, "network shutdown");
        });

        peer.network_handle
            .start_listening(
                peer.config
                    .authority()
                    .as_ref()
                    .expect("authority")
                    .primary_network_address()
                    .clone(),
            )
            .await?;

        // subscribe to topic
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_peer_ids())
            .await?;

        // Connect to target
        peer.network_handle.dial(target_peer_id, target_addr.clone()).await?;

        // Give time for connection to establish
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Allow time for excess peer disconnects to happen
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Check connected peers on target - should be limited based on config
    let connected_peers = target_peer.network_handle.connected_peers().await?;

    // assert more connected peers than max peers (4)
    assert!(connected_peers.len() <= network_config.peer_config().max_peers());

    // create non-validator peer
    let TestTypes { peer1, .. } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer {
        config: nvv_config,
        network_handle: nvv,
        mut network,
        network_events: mut nvv_events,
    } = peer1;
    network.no_kad_peers_for_test();
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    nvv.start_listening(
        nvv_config.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;

    // subscribe to topic
    // add target peer as authorized publisher
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), HashSet::from([target_peer_id])).await?;

    // connect to target
    nvv.dial(target_peer_id, target_addr.clone()).await?;

    // give time for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // wait for peer exchange event
    match timeout(Duration::from_secs(5), nvv_events.recv()).await {
        Ok(Some(NetworkEvent::Request {
            request: TestWorkerRequest::PeerExchange(map),
            channel,
            ..
        })) => {
            nvv.process_peer_exchange(map, channel).await?;
        }
        Ok(None) => return Err(eyre!("Channel closed without receiving event")),
        Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
        e => return Err(eyre!("wrong event type: {:?}", e)),
    }

    // allow dial attempts to be made
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 5)).await;

    // assert nvv is connected with other peers
    let connected = nvv.connected_peers().await?;
    assert!(!connected.contains(&target_peer_id));
    for peer in other_peers.iter() {
        let id = peer.config.authority().as_ref().expect("authority").id().peer_id();
        assert!(connected.contains(&id));
    }

    // publish random batch
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_msg = Vec::from(&sealed_block);

    // assert gossip from disconnected target peer is received by nvv
    target_peer.network_handle.publish(TEST_TOPIC.into(), expected_msg.clone()).await?;

    // wait for gossip from disconnected peer
    loop {
        match timeout(Duration::from_secs(5), nvv_events.recv()).await {
            Ok(Some(NetworkEvent::Gossip(msg, _))) => {
                let GossipMessage { source, data, .. } = msg;
                assert_eq!(source, Some(target_peer_id));
                assert_eq!(data, expected_msg);
                break;
            }
            Ok(Some(NetworkEvent::Request {
                request: TestWorkerRequest::PeerExchange(_), ..
            })) => {
                // ignore peer exchanges
                continue;
            }
            Ok(None) => return Err(eyre!("Channel closed without receiving event")),
            Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
            e => return Err(eyre!("wrong event type: {:?}", e)),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_score_decay_and_reconnection() -> eyre::Result<()> {
    // Create a custom config with short halflife for quicker testing
    let mut network_config = NetworkConfig::default();
    network_config.peer_config_mut().score_config.score_halflife = 0.1;
    network_config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let default_score = network_config.peer_config_mut().score_config.default_score;

    // Set up multiple peers with the custom config
    let (peer1, mut other_peers, _) = create_test_peers::<TestWorkerRequest, TestWorkerResponse>(
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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peers
    peer1.dial(peer2_id, peer2_addr.clone()).await?;

    // Verify connection established
    let connected_peers = peer1.connected_peers().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected");

    // Apply medium penalties to lower score but not ban
    for _ in 0..3 {
        peer1.report_penalty(peer2_id, Penalty::Medium).await;
    }

    // Check peer2's score is lower but still connected
    let score_after_penalty = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_penalty < default_score);

    // Wait for scores to recover through heartbeats
    tokio::time::sleep(Duration::from_secs(4 * TEST_HEARTBEAT_INTERVAL)).await;

    // Check score improved
    let score_after_decay = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_decay > score_after_penalty);

    // Peer should still be connected
    let connected_peers = peer1.connected_peers().await?;
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
    honest_peer
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    malicious_peer
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

    let honest_id = honest_peer.local_peer_id().await?;
    let malicious_id = malicious_peer.local_peer_id().await?;

    let honest_addr = honest_peer.listeners().await?.first().expect("honest listen addr").clone();
    let malicious_addr =
        malicious_peer.listeners().await?.first().expect("malicious listen addr").clone();

    // Connect malicious to honest
    malicious_peer.dial(honest_id, honest_addr.clone()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Report fatal penalty for malicious peer
    honest_peer.report_penalty(malicious_id, Penalty::Fatal).await;

    // Wait for ban to take effect and disconnect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    // Verify malicious peer is disconnected
    let connected_peers = honest_peer.connected_peers().await?;
    assert!(!connected_peers.contains(&malicious_id), "Malicious peer should be disconnected");

    // Verify peer is banned
    let score = honest_peer.peer_score(malicious_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score_before_ban;
    assert!(score <= min_score, "Peer should have ban-level score");

    // Now try to reconnect from malicious peer
    let dial_result = malicious_peer.dial(honest_id, honest_addr.clone()).await;

    // The dial command should succeed at the API level (the swarm will try to dial)
    assert!(dial_result.is_ok());

    // Wait a moment to see if connection is rejected
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection is rejected
    let connected_peers = honest_peer.connected_peers().await?;
    assert!(
        !connected_peers.contains(&malicious_id),
        "Banned peer should not be allowed to reconnect"
    );

    // Try direct connection from honest to malicious (should also be refused due to banned state)
    assert!(honest_peer.dial(malicious_id, malicious_addr).await.is_err());

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection still not established
    let connected_peers = honest_peer.connected_peers().await?;
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
    peer1
        .network_handle
        .start_listening(
            peer1.config.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

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
    let connected_peers = peer1.network_handle.connected_peers().await?;
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
    target_peer
        .network_handle
        .start_listening(
            target_peer
                .config
                .authority()
                .as_ref()
                .expect("authority")
                .primary_network_address()
                .clone(),
        )
        .await?;

    let target_addr = target_peer
        .network_handle
        .listeners()
        .await?
        .first()
        .expect("target peer listen addr")
        .clone();

    let target_peer_id = target_peer.network_handle.local_peer_id().await?;

    // Subscribe target to test topic
    target_peer
        .network_handle
        .subscribe_with_publishers(TEST_TOPIC.into(), target_peer.config.committee_peer_ids())
        .await?;

    // Start other peers and connect them all to the target (star topology)
    for peer in other_peers.iter_mut() {
        // Start peer network
        let peer_network = peer.network.take().expect("peer network is some");
        tokio::spawn(async move {
            let _ = peer_network.run().await;
        });

        // Start listener
        peer.network_handle
            .start_listening(
                peer.config
                    .authority()
                    .as_ref()
                    .expect("authority")
                    .primary_network_address()
                    .clone(),
            )
            .await?;

        // subscribe to test topic with target peer as authorized publisher
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), HashSet::from([target_peer_id]))
            .await?;

        // Connect to target peer
        peer.network_handle.dial(target_peer_id, target_addr.clone()).await?;

        // Give time for connection to establish
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for connections to stabilize
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL * 2)).await;

    // Verify all peers are connected to target
    let connected_peers = target_peer.network_handle.connected_peers().await?;
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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peers
    peer1.dial(peer2_id, peer2_addr.clone()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    let connected_peers = peer1.connected_peers().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply fatal penalty to peer2 - should ban it
    peer1.report_penalty(peer2_id, Penalty::Fatal).await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2 is disconnected and banned
    let connected_peers = peer1.connected_peers().await?;
    assert!(!connected_peers.contains(&peer2_id), "Peer2 should be disconnected after ban");

    let score = peer1.peer_score(peer2_id).await?.unwrap();
    let min_score = config_1.network_config().peer_config().score_config.min_score;
    assert_eq!(score, min_score, "Peer2 should have ban-level score");

    // Now simulate a new epoch where peer2 is in the committee
    let committee = HashMap::from([(peer2_id, peer2_addr.clone())]);

    // Send NewEpoch command to peer1
    let handle = peer1.clone();
    let (new_event_stream, _rx) = mpsc::channel(100);
    tokio::spawn(async move {
        handle
            .new_epoch(committee, new_event_stream)
            .await
            .expect("Failed to send NewEpoch command");
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

    // Try reconnecting peer2
    let dial_result = peer1.dial(peer2_id, peer2_addr).await;
    warn!(target: "network", ?dial_result, "dial result??");
    assert!(dial_result.is_ok(), "Should be able to reconnect to peer2 after unban");

    // Wait for connection to reestablish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection reestablished
    let connected_peers_after = peer1.connected_peers().await?;
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
    target_peer
        .network_handle
        .start_listening(
            target_peer
                .config
                .authority()
                .as_ref()
                .expect("authority")
                .primary_network_address()
                .clone(),
        )
        .await?;

    // Take peer1 and peer2 from other_peers
    let mut peer1 = other_peers.remove(0);
    let mut peer2 = other_peers.remove(0);

    // Start peer1 network
    let peer1_network = peer1.network.take().expect("peer1 network is some");
    let peer1_network_task = tokio::spawn(async move {
        peer1_network.run().await.expect("network run failed!");
    });

    peer1
        .network_handle
        .start_listening(
            peer1.config.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    let peer1_id = peer1.network_handle.local_peer_id().await?;
    let peer1_addr =
        peer1.network_handle.listeners().await?.first().expect("peer1 listen addr").clone();

    // Start peer2 network - this will be our future committee member
    // Use the SAME multiaddr as peer1 to simulate same IP
    let peer2_network = peer2.network.take().expect("peer2 network is some");
    tokio::spawn(async move {
        peer2_network.run().await.expect("network run failed!");
    });

    // For peer2, create a multiaddr with the same IP as peer1 but different port
    // Extract IP from peer1's multiaddr
    let peer2_addr = peer1_addr.clone();
    let peer2_id = peer2.network_handle.local_peer_id().await?;

    // Connect target to peer1
    target_peer.network_handle.dial(peer1_id, peer1_addr.clone()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Apply fatal penalty to peer1 - should ban it and its IP
    target_peer.network_handle.report_penalty(peer1_id, Penalty::Fatal).await;

    // Wait for ban to take effect
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer1 is disconnected and banned
    let connected_peers = target_peer.network_handle.connected_peers().await?;
    assert!(!connected_peers.contains(&peer1_id), "Peer1 should be disconnected after ban");

    // shutdown peer1 network
    peer1_network_task.abort();

    // allow os to make port available again
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    peer2.network_handle.start_listening(peer2_addr.clone()).await?;

    // Now simulate a new epoch where peer2 is in the committee with the same IP as banned peer1
    let committee = HashMap::from([(peer2_id, peer2_addr)]);
    let handle = target_peer.network_handle.clone();
    let (new_event_stream, _rx) = mpsc::channel(100);
    tokio::spawn(async move {
        handle
            .new_epoch(committee, new_event_stream)
            .await
            .expect("Failed to send NewEpoch command");
    })
    .await?;

    // Verify connection can be established with peer2 despite sharing IP with banned peer1
    target_peer.network_handle.dial(peer2_id, peer1_addr.clone()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established with peer2
    let connected_peers_after = target_peer.network_handle.connected_peers().await?;
    assert!(
        connected_peers_after.contains(&peer2_id),
        "Peer2 should be connected despite sharing IP with banned peer1"
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
    peer1
        .start_listening(
            config_1.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;
    peer2
        .start_listening(
            config_2.authority().as_ref().expect("authority").primary_network_address().clone(),
        )
        .await?;

    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    // Connect peers
    peer1.dial(peer2_id, peer2_addr.clone()).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    let connected_peers = peer1.connected_peers().await?;
    assert!(connected_peers.contains(&peer2_id), "Peer2 should be connected initially");

    // Apply severe penalties to put peer in a disconnecting state pending ban
    // We need to apply penalties but not enough to cause immediate ban
    // First apply medium penalties
    for _ in 0..3 {
        peer1.report_penalty(peer2_id, Penalty::Medium).await;
    }

    // Then apply a severe penalty - should trigger disconnect pending ban
    peer1.report_penalty(peer2_id, Penalty::Severe).await;

    // Wait for disconnect to begin but not complete
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now simulate a new epoch where peer2 is in the committee
    let committee = HashMap::from([(peer2_id, peer2_addr.clone())]);

    // Send NewEpoch command to peer1
    let handle = peer1.clone();
    let (new_event_stream, _rx) = mpsc::channel(100);
    tokio::spawn(async move {
        handle
            .new_epoch(committee, new_event_stream)
            .await
            .expect("Failed to send NewEpoch command");
    })
    .await?;

    // Wait for epoch processing
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Verify peer2's score has improved and is trusted
    let score_after_epoch = peer1.peer_score(peer2_id).await?.unwrap();
    assert!(score_after_epoch > 0.0, "Peer2 should have a positive score after new epoch");

    // Try reconnecting peer2 if it was disconnected during the process
    if !peer1.connected_peers().await?.contains(&peer2_id) {
        let dial_result = peer1.dial(peer2_id, peer2_addr).await;
        assert!(dial_result.is_ok(), "Should be able to reconnect to peer2 after new epoch");

        // Wait for connection to reestablish
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify connection is established
    let connected_peers_after = peer1.connected_peers().await?;
    assert!(connected_peers_after.contains(&peer2_id), "Peer2 should be connected after new epoch");

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
    let id = target_peer.config.authority().as_ref().expect("authority").id().peer_id();
    tokio::spawn(async move {
        let res = target_network.run().await;
        debug!(target: "network", ?id, ?res, "network shutdown");
    });

    // Start target peer listening
    target_peer
        .network_handle
        .start_listening(
            target_peer
                .config
                .authority()
                .as_ref()
                .expect("authority")
                .primary_network_address()
                .clone(),
        )
        .await?;
    let target_addr = target_peer
        .network_handle
        .listeners()
        .await?
        .first()
        .expect("target peer listen addr")
        .clone();
    let target_peer_id = target_peer.network_handle.local_peer_id().await?;

    // Start other peers and connect them one by one to the target
    for peer in committee.iter_mut() {
        // spawn peer network
        let peer_network = peer.network.take().expect("peer network is some");
        let id = peer.config.authority().as_ref().expect("authority").id().peer_id();
        tokio::spawn(async move {
            let res = peer_network.run().await;
            debug!(target: "network", ?id, ?res, "network shutdown");
        });

        peer.network_handle
            .start_listening(
                peer.config
                    .authority()
                    .as_ref()
                    .expect("authority")
                    .primary_network_address()
                    .clone(),
            )
            .await?;

        // subscribe to topic
        peer.network_handle
            .subscribe_with_publishers(TEST_TOPIC.into(), peer.config.committee_peer_ids())
            .await?;

        // Connect to target
        peer.network_handle.dial(target_peer_id, target_addr.clone()).await?;

        // Give time for connection to establish
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Allow time for heartbeats to happen
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // Check connected peers on target - should be limited based on config
    let connected_peers = target_peer.network_handle.connected_peers().await?;

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

    nvv.start_listening(
        nvv_config.authority().as_ref().expect("authority").primary_network_address().clone(),
    )
    .await?;

    // subscribe to topic
    // add target peer as authorized publisher
    nvv.subscribe_with_publishers(TEST_TOPIC.into(), HashSet::from([target_peer_id])).await?;

    // connect to target
    nvv.dial(target_peer_id, target_addr.clone()).await?;

    // give time for connection to establish
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // find other committee members through kad
    let authorities = committee
        .iter()
        .map(|peer| {
            peer.config
                .authority()
                .as_ref()
                .map(|a| *a.protocol_key())
                .expect("only authorities in committee")
        })
        .collect();
    let node_records = nvv.find_authorities(authorities).await?;

    for record in node_records {
        // wait for node records
        match timeout(Duration::from_secs(1), record).await {
            Ok(res) => {
                let info = res??;
                debug!(target: "network", ?info);
            }
            Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
        }
    }

    // allow dial attempts to be made
    tokio::time::sleep(Duration::from_secs(TEST_HEARTBEAT_INTERVAL)).await;

    // assert nvv is connected with other peers
    let connected = nvv.connected_peers().await?;
    debug!(target: "network", ?connected, "nvv connected peers");
    assert!(connected.contains(&target_peer_id));
    for peer in committee.iter() {
        let id = peer.config.authority().as_ref().expect("authority").id().peer_id();
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
        Ok(Some(NetworkEvent::Gossip(msg, _))) => {
            let GossipMessage { source, data, .. } = msg;
            assert_eq!(source, Some(target_peer_id));
            assert_eq!(data, expected_msg);
        }
        Ok(None) => return Err(eyre!("Channel closed without receiving event")),
        Err(_) => return Err(eyre!("Timeout waiting for peer exchange event")),
        e => return Err(eyre!("wrong event type: {:?}", e)),
    }

    Ok(())
}
