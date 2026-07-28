//! Engine mod for TN Node
//!
//! This module contains all execution layer implementations for worker and primary nodes.
//!
//! The worker's execution components track the canonical tip to construct blocks for the worker to
//! propose. The execution state is also used to validate proposed blocks from other peers.
//!
//! The engine for the primary executes consensus output, extends the canonical tip, and updates the
//! final state of the chain.
//!
//! The methods in this module are thread-safe wrappers for the inner type that contains logic.

use self::inner::ExecutionNodeInner;
use builder::ExecutionNodeBuilder;
use std::{future::Future, net::SocketAddr, sync::Arc};
use tn_config::Config;
use tn_exex::ExExInstallFn;
use tn_reth::{
    system_calls::EpochState, CanonStateNotificationStream, RethConfig, RethDb, RethEnv,
    WorkerTxPool,
};
use tn_rpc::EngineToPrimary;
use tn_types::{
    gas_accumulator::GasAccumulator, BatchSender, BatchValidation, BlsPublicKey,
    ConsensusHeaderDigest, ConsensusOutput, EngineUpdate, Epoch, ExecHeader, Noticer, SealedHeader,
    TaskSpawner, WorkerId, B256,
};
use tn_worker::WorkerNetworkHandle;
use tokio::sync::{mpsc, RwLock};
mod builder;
mod inner;
pub use tn_reth::worker::*;

/// The struct used to build the execution nodes.
///
/// Used to build the node until upstream reth supports
/// broader node customization.
pub struct TnBuilder {
    /// The node configuration.
    pub node_config: RethConfig,
    /// Telcoin Network config.
    pub tn_config: Config,
    /// Enable Prometheus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    pub metrics: Option<SocketAddr>,
    /// Optional TCP port to start healthcheck service.
    /// If a port is provided, the healthcheck service will spawn. Otherwise, no healthcheck
    /// service starts.
    ///
    /// IMPORTANT: only enable healthcheck if the endpoint is protected by a firewall. The
    /// healthcheck service responds unconditionally. This reads from `HEALTHCHECK_TCP_PORT` env
    /// var.
    pub healthcheck: Option<u16>,
    /// Export each epoch's final execution state to a snapshot pack when set.
    pub enable_state_export: bool,
    /// A reference to the long lived reth DB for the node.
    pub reth_db: RethDb,
    /// Registered ExEx install functions.
    ///
    /// Each entry is a `(name, notification_channel_capacity, install_fn)` tuple.
    /// These are consumed during node startup to spawn ExEx tasks on the
    /// node-level task manager, each with its own bounded notification channel of
    /// the given capacity.
    pub exex_fns: Vec<(String, usize, ExExInstallFn)>,
}

impl TnBuilder {
    /// Register an Execution Extension (ExEx) plugin.
    ///
    /// ExExes are long-running tasks that receive notifications about the full
    /// transaction lifecycle: certificate accepted, consensus committed, and
    /// chain executed.
    ///
    /// The notification channel uses the default capacity
    /// ([`tn_exex::exex_channel_capacity`]); use [`install_exex_with_capacity`]
    /// to size it for a heavyweight ExEx on a high-throughput chain.
    ///
    /// [`install_exex_with_capacity`]: Self::install_exex_with_capacity
    pub fn install_exex<F, Fut>(&mut self, name: impl Into<String>, install_fn: F) -> &mut Self
    where
        F: FnOnce(tn_exex::TnExExContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        self.install_exex_with_capacity(name, tn_exex::exex_channel_capacity(), install_fn)
    }

    /// Register an ExEx plugin with an explicit notification channel capacity.
    ///
    /// A larger capacity lets a persistently-slightly-slow ExEx absorb bursts
    /// without dropping notifications (which would surface as
    /// [`TnExExNotification::Lagged`](tn_exex::TnExExNotification::Lagged) and
    /// force a replay). See [`install_exex`](Self::install_exex) for the default.
    pub fn install_exex_with_capacity<F, Fut>(
        &mut self,
        name: impl Into<String>,
        capacity: usize,
        install_fn: F,
    ) -> &mut Self
    where
        F: FnOnce(tn_exex::TnExExContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        self.exex_fns.push((name.into(), capacity, Box::new(|ctx| Box::pin(install_fn(ctx)))));
        self
    }
}

impl std::fmt::Debug for TnBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TnBuilder")
            .field("node_config", &self.node_config)
            .field("tn_config", &self.tn_config)
            .field("metrics", &self.metrics)
            .field("healthcheck", &self.healthcheck)
            .field("exex_count", &self.exex_fns.len())
            .finish_non_exhaustive()
    }
}

/// Wrapper for the inner execution node components.
#[derive(Clone, Debug)]
pub struct ExecutionNode {
    internal: Arc<RwLock<ExecutionNodeInner>>,
}

impl ExecutionNode {
    /// Create a new instance of `Self`.
    pub fn new(tn_builder: &TnBuilder, reth_env: RethEnv) -> eyre::Result<Self> {
        let inner = ExecutionNodeBuilder::new(tn_builder, reth_env).build()?;

        Ok(ExecutionNode { internal: Arc::new(RwLock::new(inner)) })
    }

    /// Execution engine to produce blocks after consensus.
    pub async fn start_engine(
        &self,
        rx_output: mpsc::Receiver<ConsensusOutput>,
        rx_shutdown: Noticer,
        gas_accumulator: GasAccumulator,
        engine_update_tx: mpsc::Sender<EngineUpdate>,
    ) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        guard.start_engine(rx_output, rx_shutdown, gas_accumulator, engine_update_tx).await
    }

    /// Initialize the worker's transaction pool and public RPC.
    ///
    /// This method should be called on node startup.
    pub async fn initialize_worker_components<EP>(
        &self,
        worker_id: WorkerId,
        network_handle: WorkerNetworkHandle,
        engine_to_primary: EP,
        base_fee: u64,
    ) -> eyre::Result<()>
    where
        EP: EngineToPrimary + Send + Sync + 'static,
    {
        let mut guard = self.internal.write().await;
        guard
            .initialize_worker_components(worker_id, network_handle, engine_to_primary, base_fee)
            .await
    }

    /// Update the pending base fee on a worker's transaction pool.
    ///
    /// Called every epoch so the pool charges the accumulator's current base fee for the worker,
    /// including on the respawn path where [`Self::initialize_worker_components`] is skipped.
    pub async fn set_worker_base_fee(
        &self,
        worker_id: WorkerId,
        base_fee: u64,
    ) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        guard.set_worker_base_fee(worker_id, base_fee)
    }

    /// Respawn any tasks on the worker network when we get a new epoch task manager.
    ///
    /// This method should be called on epoch rollover.
    pub async fn respawn_worker_network_tasks(&self, network_handle: WorkerNetworkHandle) {
        let guard = self.internal.write().await;
        guard.respawn_worker_network_tasks(network_handle).await
    }

    /// Returns true if worker components have already been initialized.
    pub async fn are_workers_initialized(&self) -> bool {
        !self.internal.read().await.workers.is_empty()
    }

    /// Returns true if the worker identified by `worker_id` has been initialized.
    ///
    /// A worker's components (RPC server + transaction pool) are created once on
    /// the node's first epoch and are not torn down across epoch transitions, so
    /// this reflects "this worker is up and accepting transactions" rather than
    /// any per-epoch state. Backs the `/health/workers` readiness endpoint.
    pub async fn is_worker_initialized(&self, worker_id: WorkerId) -> bool {
        self.internal.read().await.workers.get(worker_id as usize).is_some()
    }

    /// Batch maker
    pub async fn start_batch_builder(
        &self,
        worker_id: WorkerId,
        block_provider_sender: BatchSender,
        task_spawner: &TaskSpawner,
        base_fee: u64,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        let mut guard = self.internal.write().await;
        guard
            .start_batch_builder(worker_id, block_provider_sender, task_spawner, base_fee, epoch)
            .await
    }

    /// Batch validator
    pub async fn new_batch_validator(
        &self,
        worker_id: &WorkerId,
        base_fee: u64,
        epoch: Epoch,
    ) -> Arc<dyn BatchValidation> {
        let guard = self.internal.read().await;
        guard.new_batch_validator(worker_id, base_fee, epoch)
    }

    /// Retrieve the last executed block from the database to restore consensus.
    pub async fn last_executed_output(&self) -> eyre::Result<ConsensusHeaderDigest> {
        let guard = self.internal.read().await;
        guard.last_executed_output()
    }

    /// Return a vector of the last 'number' executed block headers.
    pub async fn last_executed_blocks(&self, number: u64) -> eyre::Result<Vec<ExecHeader>> {
        let guard = self.internal.read().await;
        guard.last_executed_blocks(number)
    }

    /// Return a vector of the last 'number' executed block headers.
    /// These are the execution blocks finalized after consensus output, i.e. it
    /// skips all the "intermediate" blocks and is just the final block from a consensus output.
    pub async fn last_executed_output_blocks(
        &self,
        number: u64,
    ) -> eyre::Result<Vec<SealedHeader>> {
        let guard = self.internal.read().await;
        guard.last_executed_output_blocks(number)
    }

    /// Return a receiver for canonical blocks.
    pub async fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        let guard = self.internal.read().await;
        let reth_env = guard.get_reth_env();
        reth_env.canonical_block_stream()
    }

    /// Return the reth execution env.
    pub async fn get_reth_env(&self) -> RethEnv {
        let guard = self.internal.read().await;
        guard.get_reth_env()
    }

    /// Return an HTTP client for submitting transactions to the RPC.
    pub async fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<jsonrpsee::http_client::HttpClient>> {
        let guard = self.internal.read().await;
        guard.worker_http_client(worker_id)
    }

    /// Return an owned instance of the worker's transaction pool.
    pub async fn get_worker_transaction_pool(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<WorkerTxPool> {
        let guard = self.internal.read().await;
        guard.get_worker_transaction_pool(worker_id)
    }

    /// Return an owned instance of all the worker's transaction pools.
    pub async fn get_all_worker_transaction_pools(&self) -> Vec<WorkerTxPool> {
        let guard = self.internal.read().await;
        guard.get_worker_transaction_pools()
    }

    /// Return an HTTP local address for submitting transactions to the RPC.
    pub async fn worker_http_local_address(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<SocketAddr>> {
        let guard = self.internal.read().await;
        guard.worker_http_local_address(worker_id)
    }

    /// Read [EpochState] from the canonical tip.
    pub async fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        let guard = self.internal.read().await;
        guard.epoch_state_from_canonical_tip()
    }

    /// Read the current epoch's [EpochState] pinned to the previous epoch's closing block
    /// (genesis for epoch 0), returning the pin header alongside it.
    pub async fn epoch_state_at_epoch_start(&self) -> eyre::Result<(EpochState, SealedHeader)> {
        let guard = self.internal.read().await;
        guard.epoch_state_at_epoch_start()
    }

    /// Read committee validator keys for epoch, pinned to the block identified by `block_hash`.
    ///
    /// This is deliberately the ONLY committee read the engine exposes: an unpinned
    /// (canonical-tip) variant would make the result depend on when the caller runs
    /// relative to a mid-epoch governance burn. Callers must choose their pin header
    /// explicitly (epoch-start for entry reads, the epoch-closing block for the record).
    pub async fn validators_for_epoch_at_block(
        &self,
        epoch: u32,
        block_hash: B256,
    ) -> eyre::Result<Vec<BlsPublicKey>> {
        let guard = self.internal.read().await;
        guard.validators_for_epoch_at_block(epoch, block_hash)
    }
}
