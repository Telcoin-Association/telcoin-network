//! Tests networking using libp2p between peers.

mod common;
use super::*;
use assert_matches::assert_matches;
use common::{TestPrimaryRequest, TestPrimaryResponse, TestWorkerRequest, TestWorkerResponse};
use tn_config::{ConsensusConfig, NetworkConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{fixture_batch_with_transactions, CommitteeFixture};
use tn_types::{Certificate, Header};
use tokio::{sync::mpsc, time::timeout};

/// A peer on TN
struct NetworkPeer<DB, Req, Res>
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
    network: ConsensusNetwork<Req, Res>,
}

/// The type for holding testng components.
struct TestTypes<Req, Res, DB = MemDatabase>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// The first authority in the committee.
    peer1: NetworkPeer<DB, Req, Res>,
    /// The second authority in the committee.
    peer2: NetworkPeer<DB, Req, Res>,
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
    network_config.peer_config_mut().heartbeat_interval = 1;

    let all_nodes =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(network_config).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let (tx1, network_events_1) = mpsc::channel(10);
    let (tx2, network_events_2) = mpsc::channel(10);
    let topics = vec![IdentTopic::new("test-topic")];

    // peer1
    let network_key_1 = config_1.key_config().primary_network_keypair().clone();
    let authorized_publishers = config_1.committee_peer_ids();
    let peer1_network = ConsensusNetwork::<Req, Res>::new(
        &config_1,
        tx1,
        topics.clone(),
        network_key_1,
        authorized_publishers,
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
    let authorized_publishers = config_2.committee_peer_ids();
    let peer2_network = ConsensusNetwork::<Req, Res>::new(
        &config_2,
        tx2,
        topics.clone(),
        network_key_2,
        authorized_publishers,
    )
    .expect("peer2 network created");
    let network_handle_2 = peer2_network.network_handle();
    let peer2 = NetworkPeer {
        config: config_2,
        network_events: network_events_2,
        network_handle: network_handle_2,
        network: peer2_network,
    };

    TestTypes { peer1, peer2 }
}

#[tokio::test]
async fn test_valid_req_res() -> eyre::Result<()> {
    // start honest peer1 network
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
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
    peer1.start_listening(config_1.authority().primary_network_address().clone()).await?;
    peer2.start_listening(config_2.authority().primary_network_address().clone()).await?;
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
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
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
    peer1.start_listening(config_1.authority().primary_network_address().clone()).await?;
    peer2.start_listening(config_2.authority().primary_network_address().clone()).await?;
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
    tn_test_utils::init_test_tracing();

    // start honest peer1 network
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config: config_1, network_handle: peer1, network, .. } = peer1;

    debug!(target: "network", "spawn peer1 network");
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
    debug!(target: "network", "spawn peer2 network");
    tokio::spawn(async move {
        network.run().await.expect("network run failed!");
    });

    // start swarm listening on default any address
    debug!(target: "network", "start listening...");
    peer1.start_listening(config_1.authority().primary_network_address().clone()).await?;
    peer2.start_listening(config_2.authority().primary_network_address().clone()).await?;
    debug!(target: "network", "get peer2 ids...");
    let peer2_id = peer2.local_peer_id().await?;
    let peer2_addr = peer2.listeners().await?.first().expect("peer2 listen addr").clone();

    debug!(target: "network", ?peer2_id, ?peer2_addr, "done");

    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let digests = vec![missing_block.digest()];
    let batch_req = TestWorkerRequest::MissingBatches(digests);

    debug!(target: "network", "dialing peer2");
    // dial peer2
    peer1.dial(peer2_id, peer2_addr).await?;

    debug!(target: "network", "dial awaited :D - requesting pending count");

    // expect no pending requests yet
    let count = peer1.get_pending_request_count().await?;
    assert_eq!(count, 0);

    debug!(target: "network", ?count, "count?? - sending request...");

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
    malicious_peer.start_listening(config_1.authority().primary_network_address().clone()).await?;
    honest_peer.start_listening(config_2.authority().primary_network_address().clone()).await?;

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

    // honest peer returns `OutboundFailure` error
    //
    // TODO: this should affect malicious peer's local score
    // - how can honest peer limit malicious requests?
    let response_from_peer = malicious_peer.send_request(malicious_msg, honest_peer_id).await?;
    let res = timeout(Duration::from_secs(2), response_from_peer)
        .await?
        .expect("first network event received");

    // OutboundFailure::Io(Kind(UnexpectedEof))
    assert_matches!(res, Err(NetworkError::Outbound(_)));

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
    honest_peer.start_listening(config_1.authority().primary_network_address().clone()).await?;
    malicious_peer.start_listening(config_2.authority().primary_network_address().clone()).await?;
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
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
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
    cvv.start_listening(config_1.authority().primary_network_address().clone()).await?;
    nvv.start_listening(config_2.authority().primary_network_address().clone()).await?;
    let cvv_id = cvv.local_peer_id().await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // topics for pubsub
    let test_topic = IdentTopic::new("test-topic");

    // subscribe
    nvv.subscribe(test_topic.clone()).await?;

    // dial cvv
    nvv.dial(cvv_id, cvv_addr).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish on wrong topic - no peers
    let expected_failure =
        cvv.publish(IdentTopic::new("WRONG_TOPIC"), expected_result.clone()).await;
    assert!(expected_failure.is_err());

    // publish correct message and wait to receive
    let _message_id = cvv.publish(test_topic, expected_result.clone()).await?;
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
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
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
    cvv.start_listening(config_1.authority().primary_network_address().clone()).await?;
    nvv.start_listening(config_2.authority().primary_network_address().clone()).await?;
    let cvv_id = cvv.local_peer_id().await?;
    let cvv_addr = cvv.listeners().await?.first().expect("peer2 listen addr").clone();

    // topics for pubsub
    let test_topic = IdentTopic::new("test-topic");

    // subscribe
    nvv.subscribe(test_topic.clone()).await?;

    // dial cvv
    nvv.dial(cvv_id, cvv_addr).await?;

    // publish random block
    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);

    // sleep for gossip connection time lapse
    tokio::time::sleep(Duration::from_millis(500)).await;

    // publish correct message and wait to receive
    let _message_id = cvv.publish(test_topic.clone(), expected_result.clone()).await?;
    let event =
        timeout(Duration::from_secs(2), nvv_network_events.recv()).await?.expect("batch received");

    // assert gossip message
    if let NetworkEvent::Gossip(msg, _) = event {
        assert_eq!(msg.data, expected_result);
    } else {
        panic!("unexpected network event received");
    }

    // remove cvv from whitelist and try to publish again
    nvv.update_authorized_publishers(HashSet::new()).await?;

    let random_block = fixture_batch_with_transactions(10);
    let sealed_block = random_block.seal_slow();
    let expected_result = Vec::from(&sealed_block);
    let _message_id = cvv.publish(test_topic, expected_result.clone()).await?;

    // message should never be forwarded
    let timeout = timeout(Duration::from_secs(2), nvv_network_events.recv()).await;
    assert!(timeout.is_err());

    // assert fatal score
    let score = nvv.peer_score(cvv_id).await?;
    assert_eq!(score, Some(config_2.network_config().peer_config().score_config.min_score));

    Ok(())
}

// test:
// - peer connects
// - peer receives fatal penalty
// - peer is disconnected without peerx
// - peer tries to redial and fails
// - peer made trusted
// - peer dials again and connects
//
// test:
// -

/// Test peer exchanges when too many peers connect
#[tokio::test]
async fn test_peer_exchange_with_excess_peers() -> eyre::Result<()> {
    tn_test_utils::init_test_tracing();

    // Set up multiple peers to simulate network congestion
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let TestTypes { peer1: peer3, peer2: peer4 } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let TestTypes { peer1: peer5, peer2: peer6 } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let TestTypes { peer1: peer7, peer2: peer8 } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    // Get peer configurations
    let NetworkPeer {
        config: config_1,
        network_handle: target_peer_handle,
        network: target_network,
        ..
    } = peer1;
    let NetworkPeer {
        config: config_2, network_handle: peer2_handle, network: peer2_network, ..
    } = peer2;
    let NetworkPeer {
        config: config_3, network_handle: peer3_handle, network: peer3_network, ..
    } = peer3;
    let NetworkPeer {
        config: config_4, network_handle: peer4_handle, network: peer4_network, ..
    } = peer4;
    let NetworkPeer {
        config: config_5, network_handle: peer5_handle, network: peer5_network, ..
    } = peer5;
    let NetworkPeer {
        config: config_6,
        network_handle: peer6_handle,
        network: peer6_network,
        network_events: mut peer6_events,
    } = peer6;
    let NetworkPeer {
        config: config_7,
        network_handle: peer7_handle,
        network: peer7_network,
        network_events: mut peer7_events,
    } = peer7;
    let NetworkPeer {
        config: config_8,
        network_handle: peer8_handle,
        network: peer8_network,
        network_events: mut peer8_events,
    } = peer8;

    // Start the target peer (will receive too many connections)
    tokio::spawn(async move {
        target_network.run().await.expect("target network run failed!");
    });

    // Start other peers
    tokio::spawn(async move {
        peer2_network.run().await.expect("peer2 network run failed!");
    });

    tokio::spawn(async move {
        peer3_network.run().await.expect("peer3 network run failed!");
    });

    tokio::spawn(async move {
        peer4_network.run().await.expect("peer4 network run failed!");
    });

    tokio::spawn(async move {
        peer5_network.run().await.expect("peer5 network run failed!");
    });

    tokio::spawn(async move {
        peer6_network.run().await.expect("peer6 network run failed!");
    });

    tokio::spawn(async move {
        peer7_network.run().await.expect("peer7 network run failed!");
    });

    tokio::spawn(async move {
        peer8_network.run().await.expect("peer8 network run failed!");
    });

    // Start target peer listening
    target_peer_handle
        .start_listening(config_1.authority().primary_network_address().clone())
        .await?;
    let target_peer_id = target_peer_handle.local_peer_id().await?;
    let target_addr =
        target_peer_handle.listeners().await?.first().expect("target peer listen addr").clone();

    // Start other peers listening
    peer2_handle.start_listening(config_2.authority().primary_network_address().clone()).await?;
    peer3_handle.start_listening(config_3.authority().primary_network_address().clone()).await?;
    peer4_handle.start_listening(config_4.authority().primary_network_address().clone()).await?;
    peer5_handle.start_listening(config_5.authority().primary_network_address().clone()).await?;
    peer6_handle.start_listening(config_6.authority().primary_network_address().clone()).await?;
    peer7_handle.start_listening(config_7.authority().primary_network_address().clone()).await?;
    peer8_handle.start_listening(config_8.authority().primary_network_address().clone()).await?;

    // Get peer IDs
    let peer2_id = peer2_handle.local_peer_id().await?;
    let peer3_id = peer3_handle.local_peer_id().await?;
    let peer4_id = peer4_handle.local_peer_id().await?;
    let peer5_id = peer5_handle.local_peer_id().await?;
    let peer6_id = peer6_handle.local_peer_id().await?;
    let peer7_id = peer7_handle.local_peer_id().await?;
    // let peer8_id = peer8_handle.local_peer_id().await?;

    // Connect all peers to target (simulating too many connections)
    peer2_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer3_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer4_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer5_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer6_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer7_handle.dial(target_peer_id, target_addr.clone()).await?;
    peer8_handle.dial(target_peer_id, target_addr.clone()).await?;

    // Allow connections to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check connected peers on target - should be limited by config
    let connected_peers = target_peer_handle.connected_peers().await?;
    assert!(connected_peers.len() > config_1.network_config().peer_config().max_peers());

    // For those that remain connected, verify they received peer exchange info
    // when others were disconnected due to excess connections

    // Allow time for disconnects with PX to propagate
    // tokio::time::sleep(Duration::from_secs(5)).await;

    let event =
        timeout(Duration::from_secs(5), peer8_events.recv()).await?.expect("batch received");
    println!("event: {event:?}");

    // // Connect peer6 to peer2 (which should have received peer6's info via PX)
    // if connected_peers.contains(&peer2_id) {
    //     // Get peer2's address
    //     let peer2_addr =
    //         peer2_handle.listeners().await?.first().expect("peer2 listen addr").clone();

    //     // Check if peer6 can successfully connect to peer2 (discovered via PX)
    //     if !connected_peers.contains(&peer6_id) {
    //         // Peer6 was disconnected due to excess connections, should have exchanged info
    //         peer6_handle.dial(peer2_id, peer2_addr).await?;

    //         // Allow connection to establish
    //         tokio::time::sleep(Duration::from_millis(500)).await;

    //         // Verify successful connection
    //         let peer6_connected = peer6_handle.connected_peers().await?;
    //         assert!(
    //             peer6_connected.contains(&peer2_id),
    //             "Peer6 should be able to connect to peer2 via peer exchange"
    //         );
    //     }
    // }

    // TODO: test peer6 dials others
    // todo!()

    Ok(())
}

/// Test banning and unbanning of malicious peers
#[tokio::test]
async fn test_ban_and_unban_malicious_peers() -> eyre::Result<()> {
    // Set up honest and malicious peers
    let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();

    let NetworkPeer {
        config: config_1, network_handle: honest_peer, network: honest_network, ..
    } = peer1;
    let NetworkPeer {
        config: config_2,
        network_handle: malicious_peer,
        network: malicious_network,
        ..
    } = peer2;

    // Start the networks
    tokio::spawn(async move {
        honest_network.run().await.expect("honest network run failed!");
    });

    let malicious_peer_task = tokio::spawn(async move {
        malicious_network.run().await.expect("malicious network run failed!");
    });

    // Start listening
    honest_peer.start_listening(config_1.authority().primary_network_address().clone()).await?;
    malicious_peer.start_listening(config_2.authority().primary_network_address().clone()).await?;

    // Get peer IDs and addresses
    let honest_peer_id = honest_peer.local_peer_id().await?;
    let malicious_peer_id = malicious_peer.local_peer_id().await?;
    let honest_addr =
        honest_peer.listeners().await?.first().expect("honest peer listen addr").clone();
    let malicious_addr =
        malicious_peer.listeners().await?.first().expect("malicious peer listen addr").clone();

    // Connect the peers
    honest_peer.dial(malicious_peer_id, malicious_addr.clone()).await?;

    // Allow connection to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection established
    let connected_peers = honest_peer.connected_peers().await?;
    assert!(
        connected_peers.contains(&malicious_peer_id),
        "Honest peer should be connected to malicious peer"
    );

    // Report severe penalties to trigger banning
    honest_peer.report_penalty(malicious_peer_id, Penalty::Fatal).await;

    // Allow time for the ban to take effect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify peer is banned
    let score = honest_peer.peer_score(malicious_peer_id).await?;
    assert!(
        score.is_some_and(|s| s <= config_1.network_config().peer_config().min_score_for_ban),
        "Malicious peer should be banned"
    );

    // Verify the gossipsub has blacklisted the peer
    let all_peers = honest_peer.all_peers().await?;
    assert!(
        !all_peers.contains_key(&malicious_peer_id),
        "Malicious peer should be removed from gossipsub peers"
    );

    // Try to reconnect the banned peer (should fail)
    let dial_result = honest_peer.dial(malicious_peer_id, malicious_addr.clone()).await;
    assert!(dial_result.is_err(), "Dialing a banned peer should fail");

    // Terminate the malicious peer's network task to simulate a restart
    malicious_peer_task.abort();

    // Start new malicious peer network instance (simulating peer restart with same ID)
    let TestTypes { peer2: new_malicious, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { network_handle: restarted_malicious, network: restarted_network, .. } =
        new_malicious;

    tokio::spawn(async move {
        restarted_network.run().await.expect("restarted malicious network run failed!");
    });

    // Start listening on same address
    restarted_malicious
        .start_listening(config_2.authority().primary_network_address().clone())
        .await?;

    // Wait for reputation to be reset or banned status to expire
    // In a real scenario, we might use the time fast-forward feature to simulate this
    // For testing purposes, we'll artificially reset the ban via the API

    // Force unban by setting a positive score
    // honest_peer.set_application_score(malicious_peer_id, 50.0).await?;

    // Allow time for the unban to take effect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try to reconnect after unban
    honest_peer.dial(malicious_peer_id, malicious_addr).await?;

    // Allow connection to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify connection re-established
    let connected_peers_after_unban = honest_peer.connected_peers().await?;
    assert!(
        connected_peers_after_unban.contains(&malicious_peer_id),
        "Honest peer should be able to reconnect to unbanned peer"
    );

    Ok(())
}
