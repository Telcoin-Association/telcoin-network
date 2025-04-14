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
use std::{net::SocketAddr, sync::Arc};
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_reth::{RethConfig, RethEnv, WorkerTxPool};
use tn_types::{
    BatchSender, BatchValidation, ConsensusOutput, ExecHeader, Noticer, SealedHeader, TaskManager,
    WorkerId, B256,
};
use tokio::sync::{broadcast, mpsc, RwLock};
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
    ///
    /// TODO: consolidate configs
    pub tn_config: Config,
    /// TODO: temporary solution until upstream reth
    /// rpc hooks are publicly available.
    pub opt_faucet_args: Option<FaucetArgs>,
    /// Enable Prometheus consensus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    pub consensus_metrics: Option<SocketAddr>,
}

/// Wrapper for the inner execution node components.
#[derive(Clone)]
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
        from_consensus: broadcast::Receiver<ConsensusOutput>,
        task_manager: &TaskManager,
        rx_shutdown: Noticer,
    ) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        guard.start_engine(from_consensus, task_manager, rx_shutdown).await
    }

    /// Batch maker
    pub async fn start_batch_builder(
        &self,
        worker_id: WorkerId,
        block_provider_sender: BatchSender,
        task_manager: &TaskManager,
        rx_shutdown: Noticer,
    ) -> eyre::Result<()> {
        let mut guard = self.internal.write().await;
        guard.start_batch_builder(worker_id, block_provider_sender, task_manager, rx_shutdown).await
    }

    /// Batch validator
    pub async fn new_batch_validator(&self) -> Arc<dyn BatchValidation> {
        let guard = self.internal.read().await;
        guard.new_batch_validator()
    }

    /// Retrieve the last executed block from the database to restore consensus.
    pub async fn last_executed_output(&self) -> eyre::Result<B256> {
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

    pub async fn canonical_block_stream(&self) -> mpsc::Receiver<SealedHeader> {
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

    /// Return an HTTP local address for submitting transactions to the RPC.
    pub async fn worker_http_local_address(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<SocketAddr>> {
        let guard = self.internal.read().await;
        guard.worker_http_local_address(worker_id)
    }
}
