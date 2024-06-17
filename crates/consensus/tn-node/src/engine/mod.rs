//! Engine mod for TN Node
//!
//! WIP

use std::sync::Arc;

use consensus_metrics::metered_channel::Sender;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::execute::BlockExecutorProvider;
use reth_node_builder::NodeConfig;
mod inner;
mod primary;
mod worker;

use self::inner::ExecutionNodeInner;
pub use primary::*;
use reth_provider::providers::BlockchainProvider;
use reth_tasks::TaskExecutor;
use tn_batch_validator::BatchValidator;
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_types::{ConsensusOutput, NewBatch, WorkerId};
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
pub struct ExecutionNode<DB, Evm>
where
    DB: Database + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
{
    internal: Arc<RwLock<ExecutionNodeInner<DB, Evm>>>,
}

impl<DB, Evm> ExecutionNode<DB, Evm>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
{
    /// Create a new instance of `Self`.
    pub fn new(tn_builder: TnBuilder<DB>, evm: Evm) -> eyre::Result<Self> {
        let inner = ExecutionNodeInner::new(tn_builder, evm)?;

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
        guard.last_executed_output().await
    }

    /// Return an database provider.
    pub async fn get_provider(&self) -> BlockchainProvider<DB> {
        let guard = self.internal.read().await;
        guard.get_provider()
    }

    /// Return an HTTP client for submitting transactions to the RPC.
    pub async fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<jsonrpsee::http_client::HttpClient>> {
        let guard = self.internal.read().await;
        guard.worker_http_client(worker_id)
    }
}
