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

use consensus_metrics::metered_channel::Sender;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_node_builder::NodeConfig;
use std::sync::Arc;
mod inner;
mod pending_block;
mod worker;

use self::inner::ExecutionNodeInner;
use reth_provider::{providers::BlockchainProvider, ExecutionOutcome};
use reth_tasks::TaskExecutor;
use tn_batch_validator::BatchValidator;
use tn_faucet::FaucetArgs;
use tn_types::{Config, ConsensusOutput, NewBatch, WorkerId};
use tokio::sync::{broadcast, RwLock};
pub use worker::*;

/// The struct used to build the execution nodes.
///
/// Used to build the node until upstream reth supports
/// broader node customization.
pub struct TnBuilder<DB> {
    /// The database environment where all execution data is stored.
    pub database: DB,
    /// THe node configuration.
    pub node_config: NodeConfig,
    /// Task executor to spawn tasks for the node.
    ///
    /// The executor drops tasks when the CLI's TaskManager is dropped.
    pub task_executor: TaskExecutor,
    /// Telcoin Network config.
    ///
    /// TODO: consolidate configs
    pub tn_config: Config,
    /// TODO: temporary solution until upstream reth
    /// rpc hooks are publicly available.
    pub opt_faucet_args: Option<FaucetArgs>,
}

/// Wrapper for the inner execution node components.
#[derive(Clone)]
pub struct ExecutionNode<DB, Evm, CE>
where
    DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
    CE: ConfigureEvm,
{
    internal: Arc<RwLock<ExecutionNodeInner<DB, Evm, CE>>>,
}

impl<DB, Evm, CE> ExecutionNode<DB, Evm, CE>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
    CE: ConfigureEvm,
{
    /// Create a new instance of `Self`.
    pub fn new(tn_builder: TnBuilder<DB>, evm: Evm, evm_config: CE) -> eyre::Result<Self> {
        let inner = ExecutionNodeInner::new(tn_builder, evm, evm_config)?;

        Ok(ExecutionNode { internal: Arc::new(RwLock::new(inner)) })
    }

    /// Execution engine to produce blocks after consensus.
    pub async fn start_engine(
        &self,
        from_consensus: broadcast::Receiver<ConsensusOutput>,
    ) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        guard.start_engine(from_consensus).await
    }

    /// Batch maker
    pub async fn start_batch_maker(
        &self,
        to_worker: Sender<NewBatch>,
        worker_id: WorkerId,
    ) -> eyre::Result<()> {
        let mut guard = self.internal.write().await;
        guard.start_batch_maker(to_worker, worker_id).await
    }

    /// Batch validator
    pub async fn new_batch_validator(&self) -> BatchValidator<DB, Evm> {
        let guard = self.internal.read().await;
        guard.new_batch_validator()
    }

    /// Retrieve the last executed block from the database to restore consensus.
    pub async fn last_executed_output(&self) -> eyre::Result<u64> {
        let guard = self.internal.read().await;
        guard.last_executed_output()
    }

    /// Return an database provider.
    pub async fn get_provider(&self) -> BlockchainProvider<DB> {
        let guard = self.internal.read().await;
        guard.get_provider()
    }

    /// Return the node's EVM config.
    pub async fn get_evm_config(&self) -> CE {
        let guard = self.internal.read().await;
        guard.get_evm_config()
    }

    /// Return the node's evm-based block executor.
    pub async fn get_block_executor(&self) -> Evm {
        let guard = self.internal.read().await;
        guard.get_block_executor()
    }

    /// Return an HTTP client for submitting transactions to the RPC.
    pub async fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<jsonrpsee::http_client::HttpClient>> {
        let guard = self.internal.read().await;
        guard.worker_http_client(worker_id)
    }

    /// Return the worker's current pending block state.
    pub async fn worker_pending_block(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<ExecutionOutcome>> {
        let guard = self.internal.read().await;
        guard.worker_pending_block(worker_id)
    }
}
