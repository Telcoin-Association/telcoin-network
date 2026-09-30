//! End-to-end: one refresh cycle against a real `ConsensusNetwork` worker swarm fills the cache
//! with that node's signed record, RPC endpoint included.
//!
//! The swarm is stood up exactly as `crates/kad-client/tests/dht_roundtrip.rs` does (the
//! `spawn_worker_node` helper is copied from there). The node publishes its record at startup
//! (`run()` → `provide_our_data()` → `put_record`, which stores locally first), so a single node
//! answers a `GET_VALUE` for its own key with no other peers in the DHT.

// an integration test links every dependency of the crate; the lint fires for the ones the test
// itself never names (see the other integration-test binaries in the workspace)
#![allow(unused_crate_dependencies)]

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::Duration,
};
use tn_config::NetworkConfig;
use tn_kad_client::{BlsPublicKey, Multiaddr, NetworkType, PeerId, RpcInfo};
use tn_network_libp2p::{types::NetworkEvent, ConsensusNetwork, PeerExchangeMap, TNMessage};
use tn_node_record_api::{
    cache::{CacheConfig, RecordCache},
    keys::{KeySet, KeySource, StaticKeys},
    readiness,
    refresh::{run_cycle, RefreshConfig},
    telemetry::CycleOutcome,
};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{now, TaskManager};
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
async fn one_cycle_caches_the_live_node_record() -> eyre::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let node = spawn_worker_node().await?;

        let config = RefreshConfig {
            chain_id: CHAIN_ID,
            network_type: NetworkType::Worker(0),
            bootstrap: vec![node.bootstrap.clone()],
            query_timeout: Duration::from_secs(15),
            lookup_concurrency: NonZeroUsize::new(4).expect("nonzero"),
        };
        let mut keys = KeySet::new(vec![KeySource::Static(StaticKeys::new([node.bls]))]);
        let cache = Arc::new(RwLock::new(RecordCache::new(CacheConfig {
            record_ttl: Duration::from_secs(3_600),
            absent_cycles_before_evict: 3,
            refresh_interval: Duration::from_secs(300),
        })));

        // before the cycle the daemon is not ready
        let before = now();
        assert!(readiness::check(&cache.read().expect("lock"), before).is_err());

        let report = run_cycle(&config, &mut keys, &cache, 1).await;
        assert_eq!(report.cycle_no, 1);
        assert_eq!(report.keys, 1);
        assert_eq!(report.outcome, CycleOutcome::Ok, "{report:?}");
        assert_eq!(report.stats.found, 1);
        assert_eq!(report.stats.failed, 0);
        assert_eq!(report.stats.not_found, 0);
        assert!(report.epoch.is_none(), "a static source carries no epoch");

        // the cache now holds the node's record, rpc included, and the daemon is ready
        let after = now();
        let cache = cache.read().expect("lock");
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.keys_tracked(), 1);
        assert!(cache.last_refresh_at().is_some_and(|at| at >= before && at <= after));
        let view = cache.get(&node.bls, after).expect("record cached");
        assert_eq!(view.key, node.bls);
        assert_eq!(PeerId::from(view.cached.record.info.pubkey.clone()), node.peer_id);
        assert_eq!(view.cached.record.info.rpc, Some(node.rpc.clone()));
        assert_eq!(view.cached.record.info.multiaddrs.len(), 1);
        assert_eq!(view.cached.copies_seen, 1);
        assert!(!view.stale);
        let ready = readiness::check(&cache, after).expect("ready after one fresh record");
        assert_eq!(ready.records_cached, 1);
        assert_eq!(ready.cycles_completed, 1);

        // the snapshot is what the api serves
        let served: BTreeSet<_> = cache.snapshot(after).into_iter().map(|view| view.key).collect();
        assert_eq!(served, [node.bls].into_iter().collect());
        Ok::<(), eyre::Report>(())
    })
    .await??;
    Ok(())
}
