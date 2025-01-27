//! Builder for engine to mantain generics.

use super::{
    inner::ExecutionNodeInner, RethDB, TelcoinNode, TelcoinNodeTypes, TnBuilder, WorkerComponents,
    WorkerTxPool,
};
use crate::{engine::WorkerNetwork, error::ExecutionError};
use eyre::eyre;
use jsonrpsee::http_client::HttpClient;
use reth::{
    consensus::FullConsensus,
    primitives::EthPrimitives,
    rpc::{
        builder::{config::RethRpcServerConfig, RpcModuleBuilder, RpcServerHandle},
        eth::EthApi,
    },
};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_chainspec::{ChainSpec, EthereumHardforks};
use reth_db::{
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    Database,
};
use reth_db_common::init::init_genesis;
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm, ConfigureEvmEnv};
use reth_node_builder::{
    NodeConfig, NodeTypes, NodeTypesWithDB, NodeTypesWithEngine, RethTransactionPoolConfig,
};
use reth_node_ethereum::{BasicBlockExecutorProvider, EthEvmConfig, EthExecutorProvider};
use reth_provider::{
    providers::{
        BlockchainProvider, NodeTypesForProvider, ProviderNodeTypes, StaticFileProvider,
        TreeNodeTypes,
    },
    BlockIdReader, BlockReader, CanonStateSubscriptions as _, ChainSpecProvider,
    ChainStateBlockReader, DatabaseProviderFactory, EthStorage, HeaderProvider, ProviderFactory,
    TransactionVariant,
};
use reth_transaction_pool::{
    blobstore::DiskFileBlobStore, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tn_batch_builder::BatchBuilder;
use tn_batch_validator::BatchValidator;
use tn_config::Config;
use tn_engine::ExecutorEngine;
use tn_faucet::{FaucetArgs, FaucetRpcExtApiServer as _};
use tn_rpc::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use tn_types::{
    Address, BatchSender, BatchValidation, BlockBody, Consensus, ConsensusOutput, EnvKzgSettings,
    ExecHeader, LastCanonicalUpdate, Noticer, SealedBlock, SealedBlockWithSenders, TNExecution,
    TaskManager, TransactionSigned, WorkerId, B256, MIN_PROTOCOL_BASE_FEE,
};
use tokio::sync::{broadcast, mpsc::unbounded_channel};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info};

/// A builder that handles component initialization for the execution node.
/// Separates initialization concerns from runtime behavior.
pub struct ExecutionNodeBuilder<N>
where
    N: TelcoinNodeTypes,
    N::DB: RethDB,
{
    // Node configurations that drive component initialization
    node_config: NodeConfig<N::ChainSpec>,
    tn_config: Config,

    // Core initialized components
    database: N::DB,
    provider_factory: Option<ProviderFactory<N>>,
    blockchain_db: Option<BlockchainProvider<N>>,

    // EVM components
    evm_executor: Option<N::Executor>,
    evm_config: Option<N::EvmConfig>,

    // Optional components
    opt_faucet_args: Option<FaucetArgs>,
}

impl<N> ExecutionNodeBuilder<N>
where
    N: TelcoinNodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives, Storage = EthStorage>,
    N::DB: RethDB,
{
    /// Start the builder with required components
    pub fn new(tn_builder: TnBuilder<N::DB>) -> Self {
        let TnBuilder { database, node_config, tn_config, opt_faucet_args } = tn_builder;

        Self {
            node_config,
            tn_config,
            database,
            provider_factory: None,
            blockchain_db: None,
            evm_executor: None,
            evm_config: None,
            opt_faucet_args,
        }
    }

    /// Initialize the provider factory and related components
    pub fn init_provider_factory(mut self) -> eyre::Result<Self> {
        // Initialize provider factory with static files
        let datadir = self.node_config.datadir();
        let provider_factory = ProviderFactory::new(
            self.database.clone(),
            Arc::clone(&self.node_config.chain),
            StaticFileProvider::read_write(datadir.static_files())?,
        )
        .with_static_files_metrics();

        // Initialize genesis if needed
        let genesis_hash = init_genesis(&provider_factory)?;
        debug!(target: "tn::execution", chain=%self.node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        self.provider_factory = Some(provider_factory);
        Ok(self)
    }

    /// Initialize the blockchain provider and tree
    pub fn init_blockchain_provider(mut self, task_manager: &TaskManager) -> eyre::Result<Self> {
        let provider_factory = self
            .provider_factory
            .as_ref()
            .ok_or_else(|| eyre::eyre!("Provider factory must be initialized first"))?;

        // Set up metrics listener
        let (sync_metrics_tx, sync_metrics_rx) = unbounded_channel();
        let sync_metrics_listener = reth_stages::MetricsListener::new(sync_metrics_rx);
        task_manager.spawn_task("stages metrics listener task", sync_metrics_listener);

        // Initialize consensus implementation
        let tn_execution: Arc<dyn FullConsensus> = Arc::new(TNExecution);

        // Set up blockchain tree
        let tree_config = BlockchainTreeConfig::default();
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            tn_execution,
            self.evm_executor.as_ref().expect("EVM executor must be initialized first").clone(),
        );
        let tree =
            BlockchainTree::new(tree_externals, tree_config)?.with_sync_metrics_tx(sync_metrics_tx);

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));
        let blockchain_db = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)?;

        self.blockchain_db = Some(blockchain_db);
        Ok(self)
    }

    /// Initialize EVM components
    pub fn init_evm_components(mut self) -> Self {
        let evm_config = N::create_evm_config(Arc::clone(&self.node_config.chain));
        let evm_executor = N::create_executor(Arc::clone(&self.node_config.chain));

        self.evm_config = Some(evm_config);
        self.evm_executor = Some(evm_executor);
        self
    }

    /// Build the final ExecutionNodeInner
    pub fn build(self) -> eyre::Result<ExecutionNodeInner<N>> {
        // Ensure all required components are initialized
        let blockchain_db =
            self.blockchain_db.ok_or_else(|| eyre::eyre!("Blockchain provider not initialized"))?;
        let provider_factory =
            self.provider_factory.ok_or_else(|| eyre::eyre!("Provider factory not initialized"))?;
        let evm_config =
            self.evm_config.ok_or_else(|| eyre::eyre!("EVM config not initialized"))?;
        let evm_executor =
            self.evm_executor.ok_or_else(|| eyre::eyre!("EVM executor not initialized"))?;

        Ok(ExecutionNodeInner {
            address: *self.tn_config.execution_address(),
            node_config: self.node_config,
            blockchain_db,
            provider_factory,
            evm_config,
            evm_executor,
            opt_faucet_args: self.opt_faucet_args,
            tn_config: self.tn_config,
            workers: HashMap::default(),
        })
    }
}
