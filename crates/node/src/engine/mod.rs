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
use reth_chainspec::ChainSpec;
use reth_db::{
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    Database,
};
use reth_evm::execute::{BasicBlockExecutor, BlockExecutorProvider};
use reth_node_builder::NodeConfig;
use reth_node_ethereum::{
    BasicBlockExecutorProvider, EthEvmConfig, EthExecutionStrategyFactory, EthExecutorProvider,
};
use reth_provider::providers::{BlockchainProvider, TreeNodeTypes};
use std::{net::SocketAddr, sync::Arc};
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_types::{
    BatchSender, BatchValidation, ConsensusOutput, ExecHeader, Noticer, TaskManager, WorkerId, B256,
};
use tokio::sync::{broadcast, RwLock};
pub use worker::*;
mod inner;
mod worker;

/// The struct used to build the execution nodes.
///
/// Used to build the node until upstream reth supports
/// broader node customization.
pub struct TnBuilder<DB> {
    /// The database environment where all execution data is stored.
    pub database: DB,
    /// The node configuration.
    pub node_config: NodeConfig<ChainSpec>,
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
pub struct ExecutionNode<N>
where
    N: TreeNodeTypes<ChainSpec = ChainSpec>,
    N::DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static,
{
    internal: Arc<
        RwLock<
            ExecutionNodeInner<
                N,
                BasicBlockExecutorProvider<EthExecutionStrategyFactory>,
                EthEvmConfig,
            >,
        >,
    >,
}

impl<N> ExecutionNode<N>
where
    N: TreeNodeTypes<ChainSpec = ChainSpec>,
    N::DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static,
{
    /// Create a new instance of `Self`.
    pub fn new(tn_builder: TnBuilder<N::DB>, task_manager: &TaskManager) -> eyre::Result<Self> {
        let evm_config = EthEvmConfig::new(Arc::clone(&tn_builder.node_config.chain));
        let executor = EthExecutorProvider::ethereum(Arc::clone(&tn_builder.node_config.chain));
        let inner = ExecutionNodeInner::new(tn_builder, executor, evm_config, task_manager)?;

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
    pub async fn last_executed_output_blocks(&self, number: u64) -> eyre::Result<Vec<ExecHeader>> {
        let guard = self.internal.read().await;
        guard.last_executed_output_blocks(number)
    }

    /// Return an database provider.
    pub async fn get_provider(&self) -> BlockchainProvider<DB> {
        let guard = self.internal.read().await;
        guard.get_provider()
    }

    /// Return the node's EVM config.
    /// Used for tests.
    pub async fn get_evm_config(&self) -> EthEvmConfig {
        let guard = self.internal.read().await;
        guard.get_evm_config()
    }

    //Evm: BlockExecutorProvider + Clone + 'static,
    /// Return the node's evm-based block executor.
    pub async fn get_batch_executor(&self) -> impl BlockExecutorProvider {
        let guard = self.internal.read().await;
        guard.get_batch_executor()
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
    ) -> eyre::Result<WorkerTxPool<DB>> {
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
