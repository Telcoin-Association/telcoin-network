//! Tests networking using libp2p between peers.

mod common;
use super::*;
use assert_matches::assert_matches;
use common::{TestPrimaryRequest, TestPrimaryResponse, TestWorkerRequest, TestWorkerResponse};
use tn_config::ConsensusConfig;
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
    let all_nodes = CommitteeFixture::builder(MemDatabase::default).build();
    let mut authorities = all_nodes.authorities();
    let authority_1 = authorities.next().expect("first authority");
    let authority_2 = authorities.next().expect("second authority");
    let config_1 = authority_1.consensus_config();
    let config_2 = authority_2.consensus_config();
    let (tx1, network_events_1) = mpsc::channel(1);
    let (tx2, network_events_2) = mpsc::channel(1);
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
    if let NetworkEvent::Gossip(msg) = event {
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
    if let NetworkEvent::Gossip(msg) = event {
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

    // TODO: assert peer score after bad message

    Ok(())
}

// #[tokio::test]
// async fn test_pending_disconnects() -> eyre::Result<()> {
//     let TestTypes { peer1, peer2 } = create_test_types::<TestWorkerRequest, TestWorkerResponse>();
//     let TestTypes { peer1: peer3, peer2: peer4 } =
//         create_test_types::<TestWorkerRequest, TestWorkerResponse>();
//     let NetworkPeer { network_handle: cvv, network: mut network_1, .. } = peer1;
//     let NetworkPeer { config: config_2, .. } = peer2;
//     let NetworkPeer { config: config_3, .. } = peer3;
//     let NetworkPeer { config: config_4, network: network_4, network_handle: peer4_handle, .. } =
//         peer4;

//     // create px from peer1 for peer4
//     let expected_multi_1 = HashSet::from([config_2.authority().primary_network_address().clone()]);
//     let pk_2 = config_2.authority().network_key().into();
//     let expected_peer_id_1 = PeerId::from_public_key(&pk_2);
//     let expected_multi_2 = HashSet::from([config_3.authority().primary_network_address().clone()]);
//     let pk_3 = config_3.authority().network_key().into();
//     let expected_peer_id_2 = PeerId::from_public_key(&pk_3);
//     let exchange_info = HashMap::from([
//         (expected_peer_id_1, expected_multi_1.clone()),
//         (expected_peer_id_2, expected_multi_2.clone()),
//     ]);

//     // disconnect from peer 4
//     let pk_4 = config_4.authority().network_key().into();
//     let peer4_id = PeerId::from_public_key(&pk_4);
//     tokio::spawn(async move {
//         network_4.run().await.expect("network run failed!");
//     });

//     let peer4_multiaddr = peer4_handle.listeners().await?;

//     // insert connected event to dial peer
//     network_1
//         .swarm
//         .behaviour_mut()
//         .peer_manager
//         .push_test_event(PeerEvent::PeerConnected(peer4_id, peer4_multiaddr[0].clone()));

//     // insert px event
//     network_1
//         .swarm
//         .behaviour_mut()
//         .peer_manager
//         .push_test_event(PeerEvent::DisconnectPeerX(peer4_id, exchange_info.into()));

//     tokio::spawn(async move {
//         network_1.run().await.expect("network run failed!");
//     });

//     let pending_count = cvv.get_pending_request_count().await?;
//     assert_eq!(pending_count, 0);

//     let connected_peers = peer4_handle.connected_peers().await?;
//     assert_eq!(connected_peers.len(), 1);

//     Err(eyre!("finish test"))
// }
