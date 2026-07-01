//! Node/application-lifetime layer of the epoch manager.
//!
//! This file owns the [`EpochManager`] struct and the resources that live for the entire process:
//! the long-running primary/worker p2p networks, the execution engine, the consensus DB, the
//! consensus chain (epoch pack files), and the app-scoped fetch/collector tasks. It also drives
//! the epoch loop: `run` builds the process-lifetime components, then `run_epochs` repeatedly
//! invokes `run_epoch` until shutdown.
//!
//! Per-epoch orchestration lives in the sibling `run_epoch` module. Code here is concerned with
//! what survives across epochs; code there is concerned with setting up and tearing down a single
//! epoch's consensus components.

use crate::{
    engine::{ExecutionNode, TnBuilder},
    health::HealthcheckServer,
    manager::{
        exex::{run_critical_exex_future, run_isolated_exex_future},
        spawn_epoch_vote_collector,
    },
    metrics::EpochMetrics,
};
use eyre::{eyre, WrapErr as _};
use state_sync::{request_missing_packs, spawn_fetch_consensus, spawn_fetch_recent_consensus};
use std::{collections::BTreeMap, time::Duration};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{
    types::{NetworkEvent, NetworkHandle},
    ConsensusNetwork, TNMessage,
};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp, QueChannel};
use tn_reth::{system_calls::EpochState, RethDb, RethEnv};
use tn_storage::{consensus::ConsensusChain, open_db, DatabaseType};
use tn_types::{
    deconstruct_nonce, gas_accumulator::GasAccumulator, BlsPublicKey, BootstrapServer, Committee,
    ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput,
    Database as TNDatabase, EngineUpdate, Epoch, EpochRecord, Multiaddr, NetworkPublicKey,
    Notifier, TaskError, TaskManager, TaskSpawner, TimestampSec, DEFAULT_WORKER_ID,
    MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::{WorkerNetworkHandle, WorkerRequest, WorkerResponse};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod close_epoch;
mod run_epoch;
mod start_epoch;
pub(crate) use run_epoch::RunEpochMode;

/// Name of the process-lifetime [`TaskManager`] that owns tasks outliving any single epoch
/// (p2p networks, engine updates, consensus fetchers).
const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(crate) const WORKER_TASK_BASE: &str = "Worker Task";

/// Capacity of the ExEx → manager `events` channel.
///
/// `FinishedHeight` is latest-wins, so this only needs to absorb a short burst;
/// a small bound is enough and keeps a buggy ExEx from growing it without limit.
const EXEX_EVENT_CAPACITY: usize = 16;

/// How long app-level bring-up waits for a first connected peer before proceeding anyway.
///
/// Together with the record-sync deadline this bounds worst-case startup delay; both are
/// sized so existing e2e timing budgets hold.
const SOFT_PEER_WAIT: Duration = Duration::from_secs(10);

/// The long-running owner that oversees epoch transitions.
///
/// One instance exists for the lifetime of the process. It holds the resources that must survive
/// across epochs (p2p network handles, consensus DB, consensus bus, consensus chain) alongside the
/// small amount of cross-epoch carry-over state that the next epoch needs to start correctly -
/// notably [`last_consensus_header`](Self::last_consensus_header) and
/// [`last_forwarded_consensus_number`](Self::last_forwarded_consensus_number). Per-epoch consensus
/// components are built and dropped inside the epoch loop rather than stored here.
#[derive(Debug)]
pub(crate) struct EpochManager<P, DB> {
    /// The builder for node configuration
    builder: TnBuilder,
    /// The data directory
    tn_datadir: P,
    /// Primary network handle.
    primary_network_handle: Option<PrimaryNetworkHandle>,
    /// Worker network handle.
    worker_network_handle: Option<WorkerNetworkHandle>,
    /// Key config - loaded once for application lifetime.
    key_config: KeyConfig,
    /// The epoch manager's [Notifier] to shutdown all node processes.
    node_shutdown: Notifier,
    /// The timestamp to close the current epoch.
    ///
    /// The manager monitors leader timestamps for the epoch boundary.
    /// If the timestamp of the leader is >= the epoch_boundary then the
    /// manager closes the epoch after the engine executes all data.
    epoch_boundary: TimestampSec,
    /// Reth (MDBX) database handle. Held for the whole process so the execution engine can be
    /// recreated without reopening storage.
    reth_db: RethDb,
    /// Consensus (REDB) database handle. Held for the whole process; shared with the p2p networks
    /// and per-epoch consensus components.
    consensus_db: DB,
    /// Application-scoped consensus bus. Survives epoch boundaries and is reset between epochs via
    /// `reset_for_epoch`; carries `recent_blocks`, node mode, and other cross-component state.
    consensus_bus: ConsensusBusApp,
    /// Persistent event stream for the long-running worker network. Outlives any single epoch so
    /// the worker swarm does not have to be rebuilt on each transition.
    worker_event_stream: QueChannel<NetworkEvent<WorkerRequest, WorkerResponse>>,

    /// Final consensus header of the epoch that just closed, carried into the next epoch so it can
    /// be used as the starting point for the new epoch's chain.
    last_consensus_header: Option<ConsensusHeader>,

    /// Highest consensus number actually forwarded to the execution engine (not merely persisted
    /// to the DB). Carried across epochs to avoid waiting on consensus that was stored but never
    /// sent to the engine.
    last_forwarded_consensus_number: u64,

    /// Handle to the epoch pack files that durably store consensus data. Persisted on startup and
    /// at shutdown; read by the fetch tasks that backfill missing epochs.
    consensus_chain: ConsensusChain,

    /// Bootstrap servers used to seed peer discovery on the long-running networks.
    ///
    /// Seeded from the genesis committee file at construction as a fallback, then overridden
    /// in [`run`](Self::run) when the [`NetworkConfig`] provides a non-empty
    /// `bootstrap_peers` set. Bootstrap servers are independent of any epoch committee.
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,

    /// Static version string for the running node, reported by node-info surfaces.
    version_str: &'static str,

    /// Prometheus metrics for the epoch lifecycle.
    metrics: EpochMetrics,
}

/// Restore the [`GasAccumulator`] state after a mid-epoch restart.
///
/// This is the first of three recovery stages (see the module docs on
/// [`tn_types::gas_accumulator`] for the full picture). It runs once at startup, before
/// execution resumes, and performs the following:
///
/// 1. **Base fee** — sets worker 0's base fee from the finalized reth header. In a multi-worker
///    configuration this will need per-worker restoration.
/// 2. **Gas stats** — iterates every reth block from the epoch's start height through the finalized
///    tip, extracting the worker id from each block's `difficulty` field and calling
///    [`GasAccumulator::inc_block`] to rebuild per-worker gas totals.
/// 3. **Leader counts** — walks the consensus DB in reverse, counting each leader's committed
///    blocks for rounds that have already been executed (i.e. `leader_round <=
///    last_executed_round`). Rounds beyond the last executed round are intentionally skipped
///    because [`EpochManager::replay_missed_consensus`] will re-execute them, which increments
///    leader counts through the normal payload-builder path.
///
/// If there is no finalized header (fresh genesis), this is a no-op.
pub async fn catchup_accumulator(
    reth_env: RethEnv,
    gas_accumulator: &GasAccumulator,
    consensus_chain: &mut ConsensusChain,
) -> eyre::Result<()> {
    if let Some(block) = reth_env.finalized_header()? {
        let epoch_state = reth_env.epoch_state_from_canonical_tip()?;

        // Note WORKER: In a single worker world this should be suffecient to set the base fee.
        // In a multi-worker world (future) this will NOT work and needs updating.
        gas_accumulator
            .base_fee(0)
            .set_base_fee(block.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE));

        let nonce: u64 = block.nonce.into();
        let (last_executed_epoch, last_executed_round) = deconstruct_nonce(nonce);

        let blocks =
            reth_env.blocks_for_range(epoch_state.epoch_info.blockHeight..=block.number)?;

        // loop through blocks to accumulate gas stats
        for current in blocks {
            let gas = current.gas_used;
            let limit = current.gas_limit;

            // difficulty contains the worker id and batch index:
            // `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
            let lower64 = current.difficulty.into_limbs()[0];
            let worker_id = (lower64 & 0xffff) as u16;
            gas_accumulator.inc_block(worker_id, gas, limit);
        }

        // count leaders from consensus db for the current epoch
        // NOTE: replay_missed_consensus catches up rounds above last_executed_round.
        if last_executed_round > 0 && last_executed_epoch == epoch_state.epoch {
            consensus_chain
                .count_leaders(last_executed_round, gas_accumulator.rewards_counter().clone())
                .await?;
        }
    };

    Ok(())
}

/// Open the process-lifetime consensus DB, creating its directory if absent.
///
/// The returned handle is meant to be held for the whole process and shared across epochs; it is
/// not reopened per epoch.
pub(crate) fn open_consensus_db<P: TelcoinDirs + 'static>(tn_datadir: &P) -> DatabaseType {
    let consensus_db_path = tn_datadir.consensus_db_path();

    // ensure dir exists
    let _ = std::fs::create_dir_all(&consensus_db_path);
    let db = open_db(&consensus_db_path);

    info!(target: "epoch-manager", ?consensus_db_path, "opened consensus storage");

    db
}

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Construct the manager and its process-lifetime state.
    ///
    /// Opens the consensus chain, builds the application-scoped consensus bus, and seeds the
    /// fallback bootstrap servers from the genesis committee (overridden in
    /// [`run`](Self::run) when the network config supplies its own). Network handles are left
    /// `None` until [`run`](Self::run) spawns the networks. Panics if the consensus chain
    /// cannot be opened, since that is unrecoverable at startup.
    pub(crate) async fn new(
        builder: TnBuilder,
        tn_datadir: P,
        consensus_db: DB,
        key_config: KeyConfig,
        version_str: &'static str,
    ) -> eyre::Result<Self> {
        // Note this can only fail if the consensus DB is very broken (bad path for instance).
        // So we will panic for now, this will kill the node on startup for a critical error.
        let committee_zero = if let Ok(committee_zero) =
            Config::load_from_path::<Committee>(tn_datadir.committee_path(), ConfigFmt::YAML)
        {
            committee_zero
        } else {
            error!(target: "epoch-manager", "Unable to load committee zero from the genesis committee!");
            return Err(eyre::eyre!(
                "unable to load committee zero (genesis committee), this is fatal"
            ));
        };
        let epochs_db_path = tn_datadir.epochs_db_path();
        let _ = std::fs::create_dir_all(&epochs_db_path);
        // fallback bootstrap peers; `run` overrides these when the network config provides
        // a non-empty set of its own
        let bootstrap_servers = committee_zero.bootstrap_servers();
        let consensus_chain = ConsensusChain::new(epochs_db_path, committee_zero)?;
        // shutdown long-running node components
        let node_shutdown = Notifier::new();

        let reth_db = builder.reth_db.clone();

        let consensus_bus =
            ConsensusBusApp::new_with_recent_blocks(builder.tn_config.parameters.gc_depth);
        let worker_event_stream = QueChannel::new();

        Ok(Self {
            builder,
            tn_datadir,
            primary_network_handle: None,
            worker_network_handle: None,
            key_config,
            node_shutdown,
            epoch_boundary: Default::default(),
            reth_db,
            consensus_db,
            consensus_bus,
            worker_event_stream,
            last_consensus_header: None,
            last_forwarded_consensus_number: 0,
            consensus_chain,
            bootstrap_servers,
            version_str,
            metrics: EpochMetrics::default(),
        })
    }

    /// Build the process-lifetime components, then drive the epoch loop until shutdown.
    ///
    /// Startup proceeds in order: create the execution engine and start it, recover the
    /// [`GasAccumulator`] via [`catchup_accumulator`], backfill the dummy epoch-0 record on a
    /// fresh DB, spawn the long-running p2p networks
    /// ([`spawn_node_networks`](Self::spawn_node_networks)), and subscribe the primary to the
    /// epoch-vote and consensus-output gossip topics. The one-time network bring-up follows:
    /// register bootstrap peers on both swarms, bind their listeners, dial the bootstrap
    /// servers, and wait briefly (but never fail) for a first peer. With the network up, the
    /// epoch record chain is synced to the network tip
    /// ([`state_sync::sync_epoch_records_to_tip`]) so everything after reads current network
    /// state; only then are the epoch-record and vote collectors spawned, execution state
    /// restored ([`try_restore_state`](Self::try_restore_state)), and the engine-update task
    /// started. It then requests any missing epoch pack files and launches the app-scoped
    /// consensus fetch workers.
    ///
    /// Finally it selects over two futures: the node task manager running to exit, and the epoch
    /// loop ([`run_epochs`](Self::run_epochs)). Whichever resolves first ends the node; the
    /// consensus chain is persisted and remaining tasks are awaited before returning.
    pub(crate) async fn run(&mut self) -> eyre::Result<()> {
        // Surface any errors that may have been triggered on create.
        self.consensus_chain.persist_current().await?;
        // Main task manager that manages tasks across epochs.
        // Long-running tasks for the lifetime of the node.
        let mut node_task_manager = TaskManager::new(NODE_TASK_MANAGER);
        let node_task_spawner = node_task_manager.get_spawner();
        // Prime the last forwarded consensus number at startup.
        // Normally this is not needed but is a layer of safety in case
        // run_epoch() does not process any output for some reason.
        self.last_forwarded_consensus_number = self.consensus_chain.latest_consensus_number();

        info!(target: "epoch-manager", "starting node and launching first epoch");

        // create channels for engine that survive the lifetime of the node
        let (to_engine, for_engine) = mpsc::channel(1000);

        // Create our epoch gas accumulator, we currently have one worker.
        // All nodes have to agree on the worker count, do not change this for an existing chain.
        let gas_accumulator = GasAccumulator::new(1);
        // create channel for engine updates to consensus
        let (engine_update_tx, engine_update_rx) = mpsc::channel(64);

        // create the engine
        let engine = self.create_engine(&node_task_manager, &gas_accumulator)?;
        engine
            .start_engine(
                for_engine,
                self.node_shutdown.subscribe(),
                gas_accumulator.clone(),
                engine_update_tx,
            )
            .await?;

        // retrieve epoch information from canonical tip on startup
        let EpochState { epoch, .. } = engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch, "retrieved epoch state from canonical tip");
        catchup_accumulator(
            engine.get_reth_env().await,
            &gas_accumulator,
            &mut self.consensus_chain,
        )
        .await?;

        // Backfill a "dummy" epoch-0 record if missing so every startup path (record-sync
        // gate, epoch loop) can treat epoch 0 like any other epoch. It is unsigned and
        // overwritten by the real record when epoch 0 closes.
        if !self.consensus_chain.epochs().contains_epoch(0).await {
            if epoch != 0 {
                return Err(eyre::eyre!(
                    "We have epoch 0 in our database if we are past epoch 0, on {epoch}",
                ));
            }
            // on epoch 0 the tip yields the genesis committee - load it into the db too
            let (committee, _, _) = self.get_committee_with_epoch_start_info(&engine).await?;
            let committee: Vec<BlsPublicKey> = committee.bls_keys().iter().copied().collect();
            let next_committee = committee.clone();
            let epoch_rec =
                EpochRecord { epoch: 0, committee, next_committee, ..Default::default() };
            self.consensus_chain.epochs().save_dummy_epoch0(epoch_rec).await?;
        }

        // read the network config or use the default, then stamp the genesis chain id
        // onto it so every wire protocol and gossip topic is chain-namespaced (issue
        // #765). Genesis is the single source of truth; this one value is read by the
        // network builder, the gossip handles, and the gossip-validation handlers.
        let mut network_config = NetworkConfig::read_config(&self.tn_datadir)?;
        network_config.set_chain_id(self.builder.tn_config.genesis().config.chain_id);
        // operator-configured bootstrap peers take precedence over the genesis
        // committee's servers loaded in `new`
        if !network_config.bootstrap_peers().is_empty() {
            self.bootstrap_servers = network_config.bootstrap_peers().clone();
        }
        self.spawn_node_networks(node_task_spawner, &network_config, epoch).await?;
        let primary_network_handle =
            self.primary_network_handle.as_ref().expect("primary network").clone();
        primary_network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::epoch_vote_topic(network_config.chain_id()))
            .await?;
        primary_network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::consensus_output_topic(network_config.chain_id()))
            .await?;

        // One-time network bring-up at app scope: seed bootstrap peers into the known-peer
        // tables (this does not dial), bind the swarm listeners so peers can dial us back
        // during startup (load-bearing for simultaneous cohort restart — a node that isn't
        // listening cannot be reached by its restarting peers), then explicitly dial the
        // bootstrap servers so record sync below has peers to talk to.
        let worker_network_handle =
            self.worker_network_handle.as_ref().expect("worker network").clone();
        primary_network_handle
            .inner_handle()
            .add_bootstrap_peers(
                self.bootstrap_servers.iter().map(|(k, v)| (*k, v.primary.clone())).collect(),
            )
            .await?;
        worker_network_handle
            .inner_handle()
            .add_bootstrap_peers(
                self.bootstrap_servers.iter().map(|(k, v)| (*k, v.worker.clone())).collect(),
            )
            .await?;

        let node_info = &self.builder.tn_config.node_info;
        let primary_address = Self::parse_listener_address_for_swarm(
            "PRIMARY_LISTENER_MULTIADDR",
            node_info.p2p_info.primary.network_key.clone(),
            node_info.primary_network_address().clone(),
        )?;
        info!(target: "epoch-manager", ?primary_address, "primary network listening");
        primary_network_handle.inner_handle().start_listening(primary_address).await?;
        // the worker listener resolves its env override against the primary network key,
        // matching the per-epoch behavior this replaces
        let worker_address = Self::parse_listener_address_for_swarm(
            "WORKER_LISTENER_MULTIADDR",
            node_info.p2p_info.primary.network_key.clone(),
            node_info.worker_network_address().clone(),
        )?;
        worker_network_handle.inner_handle().start_listening(worker_address).await?;

        for bls_key in self.bootstrap_servers.keys() {
            self.dial_peer_bls(
                primary_network_handle.inner_handle().clone(),
                *bls_key,
                node_task_manager.get_spawner(),
            );
            self.dial_peer_bls(
                worker_network_handle.inner_handle().clone(),
                *bls_key,
                node_task_manager.get_spawner(),
            );
        }

        // Give discovery a moment to find a first peer, but never fail here: a lone node
        // (e.g. cold genesis) proceeds, and the per-epoch peer waits still enforce liveness.
        Self::wait_for_network_peers_soft(primary_network_handle.inner_handle(), "primary network")
            .await;

        // Sync the quorum-certified epoch record chain to the network tip before anything
        // else runs, so startup decisions read current network state rather than stale disk.
        // The maintenance collector is spawned after this to avoid two concurrent initial
        // collections.
        let synced_epoch = state_sync::sync_epoch_records_to_tip(
            &self.consensus_chain,
            &primary_network_handle,
            &self.consensus_bus,
            self.node_shutdown.subscribe(),
        )
        .await;
        info!(target: "epoch-manager", synced_epoch, "epoch records synced before starting the epoch loop");

        state_sync::spawn_epoch_record_collector(
            self.consensus_chain.clone(),
            primary_network_handle.clone(),
            self.consensus_bus.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        )
        .await?;

        spawn_epoch_vote_collector(
            self.consensus_chain.clone(),
            self.consensus_bus.clone(),
            self.key_config.clone(),
            primary_network_handle.clone(),
            node_task_manager.get_spawner(),
            self.node_shutdown.subscribe(),
        );

        self.try_restore_state(&engine).await?;
        // spawn task to update the latest execution results for consensus
        self.spawn_engine_update_task(engine_update_rx, &node_task_manager);

        // Spawn ExEx manager and ExEx tasks if any are registered
        if !self.builder.exex_fns.is_empty() {
            let reth_env = engine.get_reth_env().await;
            let canon_stream = reth_env.canonical_block_stream();

            // Subscribe to ConsensusBus broadcast channels for ExEx (fed from the
            // consensus-following path, not the validator hot path).
            let rx_certs = self.consensus_bus.subscribe_exex_certificates();
            let rx_consensus_output = self.consensus_bus.subscribe_exex_consensus_output();

            // Whether ExEx tasks + manager run as critical tasks (operator opt-in
            // via `Config::exex_critical`; default false → isolated, non-critical).
            let exex_critical = self.builder.tn_config.exex_critical;

            let mut exex_txs = Vec::new();
            let mut event_rxs = Vec::new();

            for (name, capacity, install_fn) in self.builder.exex_fns.drain(..) {
                // Clamp an operator-supplied `0` up to `1`: `mpsc::channel(0)`
                // panics, and the capacity from `install_exex_with_capacity` is
                // otherwise unvalidated (ExEx review finding #3).
                let (notif_tx, notif_rx) =
                    mpsc::channel(tn_exex::resolve_exex_channel_capacity(capacity));
                let (event_tx, event_rx) = mpsc::channel(EXEX_EVENT_CAPACITY);

                let ctx = tn_exex::TnExExContext::new(
                    notif_rx,
                    event_tx,
                    reth_env.clone(),
                    self.consensus_chain.clone(),
                );

                let exex_fut = install_fn(ctx);
                let label = format!("exex-{name}");
                let spawner = node_task_manager.get_spawner();
                if exex_critical {
                    // Operator opted in: a load-bearing ExEx. Spawn CRITICAL so a
                    // failure, panic, or clean exit propagates to the task manager
                    // and shuts the node down.
                    spawner.spawn_critical_task(
                        label.clone(),
                        run_critical_exex_future(label, exex_fut),
                    );
                } else {
                    // Default: optional, possibly third-party extension. Spawn
                    // NON-critical (a stop/error/panic must never shut the node
                    // down); panics are contained inside `run_isolated_exex_future`.
                    spawner.spawn_task(label.clone(), run_isolated_exex_future(label, exex_fut));
                }

                exex_txs.push((name, notif_tx));
                event_rxs.push(event_rx);
            }

            // NOTE: `_handle` exposes the minimum finished height across ExExes for
            // future pruning coordination. TN currently runs in archive mode (no
            // pruning), so there is no consumer yet and the handle is intentionally
            // dropped. See `tn_exex::TnExExEvent::FinishedHeight`.
            let (manager, _handle) = tn_exex::TnExExManager::new(
                canon_stream,
                rx_certs,
                rx_consensus_output,
                exex_txs,
                event_rxs,
            );
            // The manager follows the same policy as the ExEx tasks it serves.
            if exex_critical {
                node_task_manager.get_spawner().spawn_critical_task(
                    "exex-manager",
                    run_critical_exex_future("exex-manager".to_string(), manager),
                );
                info!(target: "epoch-manager", "ExEx manager and tasks spawned (critical)");
            } else {
                // Non-critical: if it dies, live ExEx delivery stops (logged
                // loudly) but the node — host to an optional subsystem — stays up.
                node_task_manager.get_spawner().spawn_task(
                    "exex-manager",
                    run_isolated_exex_future("exex-manager".to_string(), manager),
                );
                info!(target: "epoch-manager", "ExEx manager and tasks spawned (isolated, non-critical)");
            }
        }

        node_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?node_task_manager, "NODE TASKS\n");

        // spawn node healthcheck service if enabled
        if let Some(port) = self.builder.healthcheck {
            // probe worker 0's readiness per request; capture the engine handle
            let engine = engine.clone();
            let worker_ready = move || {
                let engine = engine.clone();
                async move { engine.is_worker_initialized(DEFAULT_WORKER_ID).await }
            };
            let _ =
                HealthcheckServer::spawn(node_task_manager.get_spawner(), port, worker_ready).await;
        }

        // spawn prometheus metrics endpoint if enabled
        //
        // bind errors are propagated (unlike healthcheck) - the operator explicitly
        // requested the endpoint, so failing to serve it should fail startup
        if let Some(addr) = self.builder.metrics {
            let db = self.reth_db.clone();
            let hooks = tn_metrics::MetricsHooks::default()
                .with_hook(move || tn_reth::report_db_metrics(&db));
            tn_metrics::start_metrics_server(
                addr,
                &node_task_manager.get_spawner(),
                self.version_str,
                hooks,
            )
            .await?;

            // mirror consensus watch channels (rounds, heights, node mode) into gauges
            tn_primary::spawn_bus_metrics_mirror(
                &self.consensus_bus,
                &node_task_manager.get_spawner(),
                self.node_shutdown.subscribe(),
            );
        }

        // Do a sanity check, request any pack files for complete epochs we are missing.
        request_missing_packs(&self.consensus_bus, &self.consensus_chain).await;
        // spawn three critical workers that will fetch epoch pack files from an epoch work queue.
        // Note, these workers will just go dormant once we have caught up- that's ok.
        for i in 0..3 {
            let shutdown = self.node_shutdown.subscribe();
            let consensus_bus = self.consensus_bus.clone();
            let primary_network_handle = primary_network_handle.clone();
            let consensus_chain = self.consensus_chain.clone();
            node_task_manager.spawn_critical_task(
                format!("epoch-consensus-worker-{i}"),
                async move {
                    spawn_fetch_consensus(
                        shutdown,
                        consensus_bus,
                        primary_network_handle,
                        i,
                        consensus_chain,
                    )
                    .await;
                    Ok(())
                },
            );
        }
        // Fire up a app scoped task to fetch rencent consensus.
        // This will not be used by CVVs but won't hurt anything and
        // will be used when not active or catching up and needs to
        // run with app scope (not epoch).
        let shutdown = self.node_shutdown.subscribe();
        let consensus_bus = self.consensus_bus.clone();
        let primary_network_handle = primary_network_handle.clone();
        let consensus_chain = self.consensus_chain.clone();
        let db = self.consensus_db.clone();
        let task_spawner = node_task_manager.get_spawner();
        let rx_consensus_request = consensus_bus.subscribe_consensus_request_queue();
        node_task_manager.spawn_critical_task("fetch-recent-consensus", async move {
            spawn_fetch_recent_consensus(
                db,
                consensus_bus,
                primary_network_handle,
                consensus_chain,
                shutdown,
                task_spawner,
                rx_consensus_request,
            )
            .await;
            Ok(())
        });

        // await all tasks on epoch-task-manager or node shutdown
        let result = tokio::select! {
            // run long-living node tasks
            res = node_task_manager.until_exit(self.node_shutdown.clone()) => {
                match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(eyre!("Node task shutdown: {e}")),
                }
            }

            // loop through short-term epochs
            epoch_result = self.run_epochs(&engine, network_config, to_engine, gas_accumulator) => epoch_result,
        };
        self.consensus_chain.persist_current().await?;
        node_task_manager.wait_for_task_shutdown().await;

        result
    }

    /// Spawn the process-lifetime primary and worker [`ConsensusNetwork`] swarms.
    ///
    /// Each swarm runs as a critical task until node shutdown. The resulting network handles are
    /// stored on the manager for use by every epoch; the worker handle is seeded with the starting
    /// `epoch` and its task spawner is refreshed on each epoch transition.
    async fn spawn_node_networks(
        &mut self,
        node_task_spawner: TaskSpawner,
        network_config: &NetworkConfig,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        //
        //=== PRIMARY
        //

        // create long-running network task for primary
        let primary_network = ConsensusNetwork::new_for_primary(
            network_config,
            self.consensus_bus.primary_network_events_cloned(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.tn_config.node_info.primary_network_address().clone(),
        )?;
        let primary_network_handle = primary_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Primary Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                },
                res = primary_network.run() => {
                    warn!(target: "epoch-manager", ?res, "primary network stopped");
                    Ok(res?)
                },
            )
        });

        // primary network handle
        self.primary_network_handle =
            Some(PrimaryNetworkHandle::new(primary_network_handle, network_config.chain_id()));

        // pass through the worker's RPC descriptor so peers can discover this
        // validator's JSON-RPC endpoint via kademlia. validators that did not
        // configure RPC leave the descriptor `None`. fail fast on a misconfigured
        // endpoint rather than advertising something peers will reject.
        let worker_rpc = self.builder.tn_config.node_info.p2p_info.worker.rpc.clone();
        if let Some(rpc) = &worker_rpc {
            rpc.validate()
                .wrap_err("invalid `node_info.p2p_info.worker.rpc` endpoint in node config")?;
        }

        // create long-running network task for worker
        let worker_network = ConsensusNetwork::new_for_worker(
            DEFAULT_WORKER_ID,
            network_config,
            self.worker_event_stream.clone(),
            self.key_config.clone(),
            self.consensus_db.clone(),
            node_task_spawner.clone(),
            self.builder.tn_config.node_info.worker_network_address().clone(),
            worker_rpc,
        )?;
        let worker_network_handle = worker_network.network_handle();
        let node_shutdown = self.node_shutdown.subscribe();

        // spawn long-running primary network task
        node_task_spawner.spawn_critical_task("Worker Network", async move {
            tokio::select!(
                _ = &node_shutdown => {
                    Ok(())
                }
                res = worker_network.run() => {
                    warn!(target: "epoch-manager", ?res, "worker network stopped");
                    Ok(res?)
                }
            )
        });

        // set temporary task spawner - this is updated with each epoch
        self.worker_network_handle = Some(WorkerNetworkHandle::new(
            worker_network_handle,
            node_task_spawner.clone(),
            epoch,
            network_config.chain_id(),
        ));

        Ok(())
    }

    /// Wait up to [`SOFT_PEER_WAIT`] for the network to report at least one connected peer.
    ///
    /// The soft counterpart to the per-epoch `wait_for_network_peers`: during app-level
    /// bring-up a node with zero peers is legitimate (the first node of a fresh network, or
    /// temporarily unreachable bootstrap servers), so this logs and proceeds on timeout
    /// instead of failing. Polls every 500ms.
    async fn wait_for_network_peers_soft<Req: TNMessage, Res: TNMessage>(
        handle: &NetworkHandle<Req, Res>,
        network_name: &str,
    ) {
        let deadline = tokio::time::Instant::now() + SOFT_PEER_WAIT;
        loop {
            if handle.connected_peer_count().await.unwrap_or(0) > 0 {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(target: "epoch-manager", "{network_name}: no peers connected after soft wait, proceeding");
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Resolve a swarm listener [`Multiaddr`] from an env var, falling back to a default.
    ///
    /// Lets cloud deployments override the primary/worker listen address (e.g. to bind a
    /// container's external address) without changing config. When the env var is set, the
    /// parsed address has the node's [`NetworkPublicKey`] appended as a `/p2p/` component to
    /// match the format produced by keytool generation; an unparseable value or one carrying a
    /// conflicting `/p2p/` key is an error. When unset, `fallback` is returned as-is.
    fn parse_listener_address_for_swarm(
        env_var: &str,
        network_pubkey: NetworkPublicKey,
        fallback: Multiaddr,
    ) -> eyre::Result<Multiaddr> {
        std::env::var(env_var)
            .map(|addr| {
                addr.parse()
                    .map_err(|e| {
                        eyre::eyre!(
                            "Failed to parse listener multiaddr from env {env_var} ({addr})\n{e}"
                        )
                    })
                    // add Protocol::P2p to multiaddr to maintain consistency with
                    // bin/telcoin-network/src/keytool/generate.rs
                    .and_then(|multi: Multiaddr| {
                        multi.with_p2p(network_pubkey.into()).map_err(|_| {
                            eyre::eyre!(
                                "{env_var} multiaddr contains a different P2P protocol {:?}",
                                std::env::var(env_var)
                            )
                        })
                    })
            })
            .unwrap_or(Ok(fallback))
    }

    /// Loop, starting a new epoch on each iteration until shutdown.
    ///
    /// Begins in [`RunEpochMode::Initial`]; each `run_epoch` call returns the [`RunEpochMode`] to
    /// carry into the next iteration, so the mode threads epoch-to-epoch state (e.g. whether this
    /// is a fresh start or a continuation). Any epoch error aborts the loop. After each epoch
    /// the consensus bus is reset and the task yields so the wrapping select can cancel it on
    /// shutdown; the loop also checks the shutdown notifier before starting the next epoch.
    async fn run_epochs(
        &mut self,
        engine: &ExecutionNode,
        network_config: NetworkConfig,
        to_engine: mpsc::Sender<ConsensusOutput>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        // initialize long-running components for node startup
        let mut run_epoch_mode = RunEpochMode::Initial;

        let node_ended_sub = self.node_shutdown.subscribe();

        // loop through epochs
        loop {
            let epoch_result = self
                .run_epoch(
                    engine,
                    &network_config,
                    &to_engine,
                    run_epoch_mode,
                    gas_accumulator.clone(),
                )
                .await;

            // ensure no errors
            run_epoch_mode = epoch_result.inspect_err(|e| {
                error!(target: "epoch-manager", ?e, "epoch returned error");
            })?;

            self.consensus_bus.reset_for_epoch();

            // Need a yield point so the task can be ended by the wrapping select when the node is
            // exiting.
            tokio::task::yield_now().await;

            // Make sure we don't start a new epoch when we are shutting down.
            if node_ended_sub.noticed() {
                break Ok(());
            }
            info!(target: "epoch-manager", "looping run epoch");
        }
    }

    /// Build the execution engine and its underlying reth environment.
    ///
    /// The reth env is wired to the shared `reth_db`, the configured base-fee address, and the
    /// accumulator's rewards counter so execution and reward accounting stay consistent.
    fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<ExecutionNode> {
        // create execution components (ie - reth env)
        let basefee_address = self.builder.tn_config.parameters.basefee_address;
        let reth_env = RethEnv::new(
            &self.builder.node_config,
            engine_task_manager,
            self.reth_db.clone(),
            basefee_address,
            gas_accumulator.rewards_counter(),
        )?;
        let engine = ExecutionNode::new(&self.builder, reth_env)?;

        Ok(engine)
    }

    /// Prime the consensus bus `recent_blocks` watch from the last executed blocks.
    ///
    /// On restart the in-memory `recent_blocks` history is empty; this backfills it (up to the
    /// watch's capacity) so consensus components can resolve recent consensus number/hash lookups.
    /// Each block's consensus hash is recovered from `parent_beacon_block_root`; round is set to 0
    /// because it is not persisted, which is sufficient for hash resolution during catch-up.
    async fn try_restore_state(&self, engine: &ExecutionNode) -> eyre::Result<()> {
        // prime the recent_blocks watch with latest executed blocks
        let block_capacity = self.consensus_bus.recent_blocks_capacity();

        for recent_block in engine.last_executed_output_blocks(block_capacity).await? {
            // On restore, use the block's consensus hash from parent_beacon_block_root.
            // Round is set to 0 since we don't persist it; consensus number/hash still allows
            // wait_for_consensus_execution to resolve hash lookups.
            let consensus_hash: ConsensusHeaderDigest =
                recent_block.parent_beacon_block_root.unwrap_or_default().into();
            let (epoch, round) = deconstruct_nonce(recent_block.nonce.into());
            let consensus_number = self
                .consensus_chain
                .consensus_header_by_digest(epoch, consensus_hash)
                .await?
                .map(|h| h.number)
                .unwrap_or_default();
            let consensus_num_hash = ConsensusNumHash::new(consensus_number, consensus_hash);
            self.consensus_bus.recent_blocks().send_modify(|blocks| {
                blocks.push_latest(round, consensus_num_hash, Some(recent_block))
            });
        }

        Ok(())
    }

    /// Spawn a task to update `ConsensusBus::recent_blocks` every time the engine processes a
    /// consensus output (with or without blocks).
    ///
    /// This is the live counterpart to [`try_restore_state`](Self::try_restore_state): the latter
    /// seeds `recent_blocks` once at startup, this keeps it current thereafter. If the engine
    /// update channel closes the engine is gone, so the task returns an error to bring the node
    /// down.
    fn spawn_engine_update_task(
        &self,
        mut engine_update: mpsc::Receiver<EngineUpdate>,
        task_manager: &TaskManager,
    ) {
        let consensus_bus = self.consensus_bus.clone();
        task_manager.spawn_critical_task("engine updates for consensus", async move {
            while let Some((latest_round, consensus_num_hash, latest_executed_block)) =
                engine_update.recv().await
            {
                consensus_bus.recent_blocks().send_modify(|blocks| {
                    blocks.push_latest(latest_round, consensus_num_hash, latest_executed_block)
                });
            }
            error!(target: "engine", "engine updates ended, node will exit");
            Err(TaskError::from_message("engine updates ended, node will exit"))
        });
    }
}
