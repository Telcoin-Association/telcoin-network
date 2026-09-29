//! End-to-end: a real `ConsensusNetwork` worker swarm serves its own signed record to a
//! [`KadClient`] that runs no node.
//!
//! The node publishes its record at startup (`run()` → `provide_our_data()` → `put_record`, which
//! stores locally first), so a single node answers a `GET_VALUE` for its own key with no other
//! peers in the DHT.

// an integration test links every dependency of the crate; the lint fires for the ones the test
// itself never names (see the other integration-test binaries in the workspace)
#![allow(unused_crate_dependencies)]

use serde::{Deserialize, Serialize};
use std::{num::NonZeroUsize, time::Duration};
use tn_config::NetworkConfig;
use tn_kad_client::{
    BlsPublicKey, KadClient, KadClientConfig, KadClientError, Multiaddr, NetworkType, PeerId,
    RpcInfo,
};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork, PeerExchangeMap, TNMessage};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsKeypair, TaskManager};
use tokio::sync::mpsc;

/// The chain id every swarm and client in this test is namespaced to.
const CHAIN_ID: u64 = 2017;

/// Minimal request type satisfying [`TNMessage`]; the test never sends one.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum TestReq {
    PeerExchange(PeerExchangeMap),
}

/// Minimal response type satisfying [`TNMessage`]; the test never sends one.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum TestRes {
    PeerExchange(PeerExchangeMap),
}

impl TNMessage for TestReq {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange(map) => Some(map.clone()),
        }
    }
}

impl TNMessage for TestRes {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange(map) => Some(map.clone()),
        }
    }
}

impl From<PeerExchangeMap> for TestReq {
    fn from(map: PeerExchangeMap) -> Self {
        Self::PeerExchange(map)
    }
}

impl From<PeerExchangeMap> for TestRes {
    fn from(map: PeerExchangeMap) -> Self {
        Self::PeerExchange(map)
    }
}

/// One running worker swarm and everything a client needs to read its record.
struct WorkerNode {
    /// The BLS key the record is published under.
    bls: BlsPublicKey,
    /// The worker swarm's libp2p identity.
    peer_id: PeerId,
    /// The `RpcInfo` the record was signed with.
    rpc: RpcInfo,
    /// The bound listen address with the `/p2p/<peer-id>` suffix a client dials.
    bootstrap: Multiaddr,
    /// Keeps the swarm's spawned tasks alive for the test's duration.
    _task_manager: TaskManager,
    /// Keeps the swarm's event stream open for the test's duration.
    _events: mpsc::Receiver<NetworkEvent<TestReq, TestRes>>,
}

/// Stand up one authority's worker-0 swarm with an advertised RPC endpoint and wait until it
/// listens.
async fn spawn_worker_node() -> eyre::Result<WorkerNode> {
    tn_types::test_utils::init_test_tracing();
    let mut network_config = NetworkConfig::default();
    network_config.set_chain_id(CHAIN_ID);
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(4).expect("nonzero"))
        .with_network_config(network_config)
        .build();
    let authority = fixture.authorities().next().expect("fixture yields an authority");
    let config = authority.consensus_config();
    assert_eq!(config.network_config().libp2p_config().chain_id, CHAIN_ID);

    let worker_addr = config.worker_address(0).expect("worker 0 is configured");
    let rpc = RpcInfo {
        http: "https://validator.example:8545/".parse()?,
        ws: Some("wss://validator.example:8546/".parse()?),
    };
    let task_manager = TaskManager::default();
    let (tx, events) = mpsc::channel(16);
    let network = ConsensusNetwork::<
        TestReq,
        TestRes,
        MemDatabase,
        mpsc::Sender<NetworkEvent<TestReq, TestRes>>,
    >::new_for_worker(
        0,
        config.network_config(),
        tx,
        config.key_config().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        worker_addr.clone(),
        Some(rpc.clone()),
    )?;
    let handle = network.network_handle();
    tokio::spawn(network.run());
    handle.start_listening(worker_addr).await?;

    // the fixture listens on port 0, so read the bound address back from the swarm
    let listen_addr = loop {
        if let Some(addr) = handle.listeners().await?.into_iter().next() {
            break addr;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let peer_id: PeerId = config.key_config().worker_network_public_key(0).into();
    let bootstrap = listen_addr.with_p2p(peer_id).expect("listen address carries no peer id yet");

    Ok(WorkerNode {
        bls: config.key_config().primary_public_key(),
        peer_id,
        rpc,
        bootstrap,
        _task_manager: task_manager,
        _events: events,
    })
}

#[tokio::test]
async fn fetch_worker_record_from_live_node() -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let node = spawn_worker_node().await?;

        let config =
            KadClientConfig::new(CHAIN_ID, NetworkType::Worker(0), vec![node.bootstrap.clone()]);
        let client = KadClient::spawn(config).await?;

        // spawn returns only once a bootstrap peer is connected
        assert_eq!(client.connected_bootstrap_peers().await?, vec![node.peer_id]);

        // the node's own record: signed for the worker domain, carrying rpc
        let found = client
            .get_node_record(node.bls)
            .await?
            .expect("the node serves its own record from its local store");
        assert_eq!(found.key, node.bls);
        assert_eq!(found.peer_id(), node.peer_id);
        assert_eq!(PeerId::from(found.info().pubkey.clone()), node.peer_id);
        assert_eq!(found.info().rpc, Some(node.rpc.clone()));
        assert_eq!(found.info().multiaddrs.len(), 1);
        assert_eq!(found.copies_seen, 1);

        // a key nobody published under is a clean miss, not an error
        let other = *BlsKeypair::generate(&mut rand::rng()).public();
        assert_eq!(client.get_node_record(other).await?.map(|r| r.key), None);

        // fan-out preserves input order and resolves each key independently
        let results = client
            .get_node_records(&[other, node.bls], NonZeroUsize::new(2).expect("nonzero"))
            .await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, other);
        assert!(matches!(results[0].1, Ok(None)));
        assert_eq!(results[1].0, node.bls);
        assert!(matches!(&results[1].1, Ok(Some(record)) if record.key == node.bls));

        // a clone outlives shutdown but can no longer query
        let clone = client.clone();
        client.shutdown().await;
        assert!(matches!(clone.get_node_record(node.bls).await, Err(KadClientError::Shutdown)));
        Ok::<(), eyre::Report>(())
    })
    .await??;
    Ok(())
}

/// A client configured for the primary DHT but pointed at a worker swarm connects at the QUIC
/// layer (the address and peer id are genuine) but can never negotiate kademlia: the protocol
/// names differ (`/tn-primary-kad-2017/0.0.1` vs `/tn-worker-0-kad-2017/0.0.1`), so every request
/// in the lookup fails at multistream-select. With plain libp2p that ends as `NotFound`, which is
/// indistinguishable from a clean miss; the client reads the query stats (zero successes) and
/// reports [`KadClientError::NoPeerAnswered`] instead. `spawn` itself succeeds, because a
/// transport-level connection is all it waits for. The node does not ban the client for the
/// mismatch: gossipsub is namespaced by chain only, so it negotiates fine, and a kademlia
/// negotiation failure is not penalized on either side.
#[tokio::test]
async fn primary_client_against_worker_swarm_reports_no_peer_answered() -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let node = spawn_worker_node().await?;

        let config =
            KadClientConfig::new(CHAIN_ID, NetworkType::Primary, vec![node.bootstrap.clone()]);
        let client = KadClient::spawn(config).await?;
        assert_eq!(client.connected_bootstrap_peers().await?, vec![node.peer_id]);

        let outcome = client.get_node_record(node.bls).await;
        assert!(
            matches!(
                &outcome,
                Err(KadClientError::NoPeerAnswered { protocol, requests })
                    if protocol == &NetworkType::Primary.kad_protocol_name(CHAIN_ID) && *requests >= 1
            ),
            "expected NoPeerAnswered for a role mismatch, got {outcome:?}"
        );

        client.shutdown().await;
        Ok::<(), eyre::Report>(())
    })
    .await??;
    Ok(())
}

/// A bootstrap address nobody listens on fails at spawn, not at the first lookup.
#[tokio::test]
async fn unreachable_bootstrap_fails_at_spawn() -> eyre::Result<()> {
    let peer = PeerId::random();
    // a loopback port with no listener: the QUIC dial gets no handshake and times out
    let dead: Multiaddr = format!("/ip4/127.0.0.1/udp/1/quic-v1/p2p/{peer}").parse()?;
    let config = KadClientConfig::new(CHAIN_ID, NetworkType::Worker(0), vec![dead])
        .with_query_timeout(Duration::from_secs(3));
    let outcome = tokio::time::timeout(Duration::from_secs(20), KadClient::spawn(config)).await?;
    assert!(
        matches!(outcome, Err(KadClientError::NoBootstrapPeerReachable)),
        "expected NoBootstrapPeerReachable, got {:?}",
        outcome.map(|_| ())
    );
    Ok(())
}
