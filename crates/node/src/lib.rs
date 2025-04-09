// SPDX-License-Identifier: Apache-2.0
//! Library for managing all components used by a full-node in a single process.

use crate::{primary::PrimaryNode, worker::WorkerNode};
use consensus_metrics::start_prometheus_server;
use engine::{ExecutionNode, TnBuilder};
use futures::StreamExt;
use reth_db::{
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    Database,
};
use reth_provider::CanonStateSubscriptions;
use std::{
    str::FromStr as _,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tn_config::{ConsensusConfig, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{types::IdentTopic, ConsensusNetwork, PeerId};
use tn_node_traits::TelcoinNode;
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_storage::{open_db, tables::ConsensusBlocks, DatabaseType};
use tn_types::{BatchValidation, ConsensusHeader, Database as TNDatabase, Multiaddr, TaskManager};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::{runtime::Builder, sync::mpsc};
use tracing::{info, instrument, warn};

pub mod dirs;
pub mod engine;
mod error;
pub mod primary;
pub mod worker;

/// Spawn a task to dial a primary peer and to keep trying on failure.
fn dial_primary(
    handle: PrimaryNetworkHandle,
    peer_id: PeerId,
    peer_addr: tn_network_libp2p::Multiaddr,
    connected_count: Arc<AtomicU32>,
) {
    tokio::spawn(async move {
        let mut backoff = 1;
        while let Err(e) = handle.dial(peer_id, peer_addr.clone()).await {
            tracing::warn!(target: "telcoin::node", "failed to dial primary {peer_id} at {peer_addr}: {e}");
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            if backoff < 120 {
                backoff += backoff;
            }
        }
        connected_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Spawn a task to dial a worker peer and to keep trying on failure.
fn dial_worker(
    handle: WorkerNetworkHandle,
    peer_id: PeerId,
    peer_addr: tn_network_libp2p::Multiaddr,
    connected_count: Arc<AtomicU32>,
) {
    tokio::spawn(async move {
        let mut backoff = 1;
        while let Err(e) = handle.dial(peer_id, peer_addr.clone()).await {
            tracing::warn!(target: "telcoin::node", "failed to dial worker {peer_id} at {peer_addr}: {e}");
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            if backoff < 120 {
                backoff += backoff;
            }
        }
        connected_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Start up the primary and worker libp2p networks and return handles to use it.
/// This will also dial initial peers and the networks should be ready to use once it resolves.
async fn start_networks<DB: TNDatabase>(
    consensus_config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    task_manager: &TaskManager,
    worker_id: &u16,
    validator: Arc<dyn BatchValidation>,
    state_sync: StateSynchronizer<DB>,
) -> eyre::Result<(PrimaryNetworkHandle, WorkerNetworkHandle)> {
    let (event_stream, rx_event_stream) = mpsc::channel(1000);
    let (worker_event_stream, rx_worker_event_stream) = mpsc::channel(1000);
    let primary_network = ConsensusNetwork::new_for_primary(consensus_config, event_stream)
        .expect("primry p2p network create failed!");
    let worker_network = ConsensusNetwork::new_for_worker(consensus_config, worker_event_stream)
        .expect("worker p2p network create failed!");
    let primary_network_handle = primary_network.network_handle();
    let worker_network_handle = worker_network.network_handle();
    let rx_shutdown = consensus_config.shutdown().subscribe();
    task_manager.spawn_task("primary network run loop", async move {
        tokio::select!(
            _ = &rx_shutdown => {
                Ok(())
            }
            res = primary_network.run() => {
                res
            }
        )
    });
    let rx_shutdown = consensus_config.shutdown().subscribe();
    task_manager.spawn_task("worker network run loop", async move {
        tokio::select!(
            _ = &rx_shutdown => {
                Ok(())
            }
            res = worker_network.run() => {
               res
            }
        )
    });
    primary_network_handle.subscribe(IdentTopic::new("tn-primary")).await?;
    let my_authority = consensus_config.authority();

    let primary_multiaddr = get_multiaddr_from_env_or_config(
        "PRIMARY_MULTIADDR",
        my_authority.primary_network_address().clone(),
    );
    primary_network_handle.start_listening(primary_multiaddr).await?;

    let worker_address = consensus_config.worker_address(worker_id);
    let worker_multiaddr =
        get_multiaddr_from_env_or_config("WORKER_MULTIADDR", worker_address.clone());
    worker_network_handle.start_listening(worker_multiaddr).await?;
    let primary_network_handle = PrimaryNetworkHandle::new(primary_network_handle);
    let worker_network_handle = WorkerNetworkHandle::new(worker_network_handle);
    let peers_connected = Arc::new(AtomicU32::new(0));
    let workers_connected = Arc::new(AtomicU32::new(0));
    for (authority_id, addr, _) in
        consensus_config.committee().others_primaries_by_id(&consensus_config.authority().id())
    {
        let peer_id = authority_id.peer_id();
        dial_primary(primary_network_handle.clone(), peer_id, addr, peers_connected.clone());
    }
    for (peer_id, addr) in consensus_config.worker_cache().all_workers() {
        if addr != worker_address {
            dial_worker(worker_network_handle.clone(), peer_id, addr, workers_connected.clone());
        }
    }
    let quorum = consensus_config.committee().quorum_threshold() as u32;
    // Wait until we are connected to a quorum of peers (note this assumes we are a validator...).
    while peers_connected.load(Ordering::Relaxed) < quorum
        || workers_connected.load(Ordering::Relaxed) < quorum
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let primary_network = PrimaryNetwork::new(
        rx_event_stream,
        primary_network_handle.clone(),
        consensus_config.clone(),
        consensus_bus.clone(),
        state_sync,
    );
    primary_network.spawn(task_manager);

    // Receive incoming messages from other workers.
    WorkerNetwork::new(
        rx_worker_event_stream,
        worker_network_handle.clone(),
        consensus_config.clone(),
        *worker_id,
        validator,
    )
    .spawn(task_manager);

    Ok((primary_network_handle, worker_network_handle))
}

/// Inner working of launch_node().
///
/// This will bring up a tokio runtime and start the app within it.
/// It also will shutdown this runtime, potentially violently, to make
/// sure any lefteover tasks are ended.  This allows it to be called more
/// than once per program execution to support changing modes of the
/// running node.
/// If it returns Ok(true) this indicates a mode change occurred and a restart
/// is required.
pub fn launch_node_inner<DB, P>(
    builder: &TnBuilder<DB>,
    tn_datadir: &P,
    db: DatabaseType,
) -> eyre::Result<bool>
where
    DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static,
    P: TelcoinDirs + 'static,
{
    // Create a tokio runtime each time this is called.
    // This means we should be starting with a clean slate on a
    // relaunch (old tasks should be gone with relaunch).
    let runtime = Builder::new_multi_thread()
        .thread_name("telcoin-network")
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to build a tokio runtime");

    let res = runtime.block_on(async move {
        if let Some(metrics_socket) = builder.consensus_metrics {
            start_prometheus_server(metrics_socket);
        }

        // config for validator keys
        let config = builder.tn_config.clone();
        let mut task_manager = TaskManager::new("Task Manager");
        let mut engine_task_manager = TaskManager::new("Engine Task Manager");
        let engine = ExecutionNode::<TelcoinNode<DB>>::new(builder, &engine_task_manager)?;
        let validator = engine.new_batch_validator().await;

        info!(target: "telcoin::node", "execution engine created");

        let node_storage = db.clone();
        tracing::info!(target: "telcoin::cli", "node storage open");
        let key_config = KeyConfig::read_config(tn_datadir)?;
        let network_config = NetworkConfig::read_config(tn_datadir)?;
        let consensus_config = ConsensusConfig::new(config, tn_datadir, node_storage, key_config, network_config)?;

        let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;
        let worker = WorkerNode::new(*worker_id, consensus_config.clone());
        let consensus_bus =
                    ConsensusBus::new_with_args(consensus_config.config().parameters.gc_depth);
        let state_sync = StateSynchronizer::new(consensus_config.clone(), consensus_bus.clone());

        let (primary_network_handle, worker_network_handle) =
            start_networks(&consensus_config, &consensus_bus, &task_manager, worker_id, validator.clone(), state_sync.clone()).await?;

        let primary = PrimaryNode::new(
                consensus_config.clone(),
                consensus_bus.clone(),
                primary_network_handle,
                state_sync,
            );

        let mut engine_state = engine.get_provider().await.canonical_state_stream();

        // Prime the recent_blocks watch with latest executed blocks.
        let block_capacity = consensus_bus.recent_blocks().borrow().block_capacity();
        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            consensus_bus
                .recent_blocks()
                .send_modify(|blocks| blocks.push_latest(recent_block));
        }

        // Prime the last consensus header from the DB.
        let (_, last_db_block) = db
            .last_record::<ConsensusBlocks>()
            .unwrap_or_else(|| (0, ConsensusHeader::default()));
        consensus_bus.last_consensus_header().send(last_db_block)?;

        if builder.tn_config.observer {
            consensus_bus.node_mode().send_modify(|v| *v = NodeMode::Observer);
        } else  if state_sync::can_cvv(
            consensus_bus.clone(),
            consensus_config.clone(),
            primary.network_handle().await,
        )
        .await
        {
            consensus_bus.node_mode().send_modify(|v| *v = NodeMode::CvvActive);
        } else {
            consensus_bus.node_mode().send_modify(|v| *v = NodeMode::CvvInactive);
        }

        // Spawn a task to update the consensus bus with new execution blocks as they are produced.
        let latest_block_shutdown = consensus_config.shutdown().subscribe();
        let consensus_bus_clone = consensus_bus.clone();
        task_manager.spawn_task("latest block", async move {
            loop {
                tokio::select!(
                    _ = &latest_block_shutdown => {
                        break;
                    }
                    latest = engine_state.next() => {
                        if let Some(latest) = latest {
                            consensus_bus_clone.recent_blocks().send_modify(|blocks| blocks.push_latest(latest.tip().block.header.clone()));
                        } else {
                            break;
                        }
                    }
                )
            }
        });


        // create receiving channel before spawning primary to ensure messages are not lost
        let consensus_output_rx = consensus_bus.subscribe_consensus_output();

        // start the primary
        let mut primary_task_manager = primary.start().await?;

        // start the worker
        let batch_provider = worker.start(validator, worker_network_handle).await?;

        // start engine
        engine
            .start_engine(
                consensus_output_rx,
                &engine_task_manager,
                consensus_config.shutdown().subscribe(),
            )
            .await?;
        // spawn block maker for worker
        engine
            .start_batch_builder(
                *worker_id,
                batch_provider.batches_tx(),
                &engine_task_manager,
                consensus_config.shutdown().subscribe(),
            )
            .await?;

        primary_task_manager.update_tasks();
        task_manager.add_task_manager(primary_task_manager);
        engine_task_manager.update_tasks();
        task_manager.add_task_manager(engine_task_manager);

        info!(target:"telcoin::node", tasks=?task_manager, "TASKS");

        task_manager.join_until_exit(consensus_config.shutdown().clone()).await;
        let running = consensus_bus.restart();
        consensus_bus.clear_restart();
        info!(target:"tn", "TASKS complete, restart: {running}");
        Ok(running)
    });
    // Kick over the runtime- don't let errant tasks block the Drop.
    runtime.shutdown_background();
    res
}

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
#[instrument(level = "info", skip_all)]
pub fn launch_node<DB, P>(mut builder: TnBuilder<DB>, tn_datadir: P) -> eyre::Result<()>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    P: TelcoinDirs + 'static,
{
    // adjust rpc instance ports
    builder.node_config.adjust_instance_ports();

    let consensus_db_path = tn_datadir.consensus_db_path();

    tracing::info!(target: "telcoin::node", "opening node storage at {:?}", consensus_db_path);

    // open storage for consensus
    // In case the DB dir does not yet exist.
    let _ = std::fs::create_dir_all(&consensus_db_path);
    let db = open_db(&consensus_db_path);

    let mut running = true;
    while running {
        running = launch_node_inner(&builder, &tn_datadir, db.clone())?;
    }
    Ok(())
}

/// Check the environment to overwrite the host.
fn get_multiaddr_from_env_or_config(env_var: &str, fallback: Multiaddr) -> Multiaddr {
    let multiaddr = std::env::var(env_var)
        .ok()
        .and_then(|addr_str| Multiaddr::from_str(&addr_str).ok())
        .unwrap_or(fallback);
    info!(target: "telcoin::node", ?multiaddr, env_var);
    multiaddr
}
