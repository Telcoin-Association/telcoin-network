//! Inner-execution node components for both Worker and Primary execution.
//!
//! This module contains the logic for execution.

use super::TnBuilder;
use crate::{
    engine::{WorkerNetwork, WorkerNode},
    error::ExecutionError,
};
use consensus_metrics::metered_channel::Sender;
use jsonrpsee::http_client::HttpClient;
use reth::rpc::{
    builder::{config::RethRpcServerConfig, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_auto_seal_consensus::AutoSealConsensus;
use reth_beacon_consensus::EthBeaconConsensus;
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_db_common::init::init_genesis;
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_node_builder::{common::WithConfigs, components::PoolBuilder, BuilderContext, NodeConfig};
use reth_node_ethereum::{node::EthereumPoolBuilder, EthEvmConfig};
use reth_primitives::Address;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    DatabaseProviderFactory, FinalizedBlockReader, HeaderProvider, ProviderFactory,
    StaticFileProviderFactory as _,
};
use reth_prune::PruneModes;
use reth_tasks::TaskExecutor;
use reth_transaction_pool::TransactionPool;
use std::{collections::HashMap, sync::Arc};
use tn_batch_maker::{BatchMakerBuilder, MiningMode};
use tn_batch_validator::BatchValidator;
use tn_engine::ExecutorEngine;
use tn_faucet::{FaucetArgs, FaucetRpcExtApiServer as _};
use tn_types::{Consensus, ConsensusOutput, NewBatch, WorkerId};
use tokio::sync::{broadcast, mpsc::unbounded_channel};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info};

/// Inner type for holding execution layer types.
pub(super) struct ExecutionNodeInner<DB, Evm, CE>
where
    DB: Database + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + 'static,
    CE: ConfigureEvm,
{
    /// The [Address] for the authority used as the suggested beneficiary.
    ///
    /// The address refers to the execution layer's address
    /// based on the authority's secp256k1 public key.
    address: Address,
    /// The type that holds all information needed to launch the node's engine.
    ///
    /// The [NodeConfig] is reth-specific and holds many helper functions that
    /// help TN stay in-sync with the Ethereum community.
    node_config: NodeConfig,
    /// Type that fetches data from the database.
    blockchain_db: BlockchainProvider<DB>,
    /// Provider factory is held by the blockchain db, but there isn't a publicly
    /// available way to get a cloned copy.
    /// TODO: add a method to `BlockchainProvider` in upstream reth
    provider_factory: ProviderFactory<DB>,
    /// The type to configure the EVM for execution.
    evm_config: CE,
    /// The Evm configuration type.
    evm_executor: Evm,
    /// The task executor is responsible for executing
    /// and spawning tasks to the runtime.
    ///
    /// This type is owned by the current runtime and facilitates
    /// a convenient way to spawn tasks that shutdown with the runtime.
    task_executor: TaskExecutor,
    /// TODO: temporary solution until upstream reth supports public rpc hooks
    opt_faucet_args: Option<FaucetArgs>,
    /// Collection of execution components by worker.
    workers: HashMap<WorkerId, RpcServerHandle>,
    // TODO: add Pool to self.workers for direct access (tests)
}

impl<DB, Evm, CE> ExecutionNodeInner<DB, Evm, CE>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + 'static,
    CE: ConfigureEvm,
{
    /// Create a new instance of `Self`.
    pub(super) fn new(
        tn_builder: TnBuilder<DB>,
        evm_executor: Evm,
        evm_config: CE,
    ) -> eyre::Result<Self> {
        // deconstruct the builder
        let TnBuilder { database, node_config, task_executor, tn_config, opt_faucet_args } =
            tn_builder;

        // resolve the node's datadir
        let datadir = node_config.datadir();

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        let _ = fdlimit::raise_fd_limit();

        let provider_factory = ProviderFactory::new(
            database.clone(),
            Arc::clone(&node_config.chain),
            StaticFileProvider::read_write(datadir.static_files())?,
        )
        .with_static_files_metrics();

        debug!(target: "tn::execution", chain=%node_config.chain.chain, genesis=?node_config.chain.genesis_hash(), "Initializing genesis");

        let genesis_hash = init_genesis(provider_factory.clone())?;

        info!(target: "tn::execution",  ?genesis_hash);
        info!(target: "tn::execution", "\n{}", node_config.chain.display_hardforks());

        let auto_consensus: Arc<dyn Consensus> =
            Arc::new(AutoSealConsensus::new(Arc::clone(&node_config.chain)));

        debug!(target: "tn::cli", "Spawning stages metrics listener task");
        let (sync_metrics_tx, sync_metrics_rx) = unbounded_channel();
        let sync_metrics_listener = reth_stages::MetricsListener::new(sync_metrics_rx);
        task_executor.spawn_critical("stages metrics listener task", sync_metrics_listener);

        // get config from file
        let prune_config = node_config.prune_config(); //.or(reth_config.prune.clone());
        let tree_config = BlockchainTreeConfig::default();
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            auto_consensus.clone(),
            evm_executor.clone(),
        );
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            prune_config.map(|config| config.segments.clone()).unwrap_or_else(PruneModes::none),
        )?
        .with_sync_metrics_tx(sync_metrics_tx.clone());

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));
        debug!(target: "tn::execution", "configured blockchain tree");

        // setup the blockchain provider
        let blockchain_db = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)?;
        let address = *tn_config.execution_address();

        Ok(Self {
            address,
            node_config,
            blockchain_db,
            provider_factory,
            evm_config,
            evm_executor,
            task_executor,
            opt_faucet_args,
            workers: HashMap::default(),
        })
    }

    /// Spawn tasks associated with executing output from consensus.
    ///
    /// The method is consumed by [PrimaryNodeInner::start].
    /// All tasks are spawned with the [ExecutionNodeInner]'s [TaskManager].
    pub(super) async fn start_engine(
        &self,
        from_consensus: broadcast::Receiver<ConsensusOutput>,
    ) -> eyre::Result<()> {
        // start metrics
        let prometheus_handle = self.node_config.install_prometheus_recorder()?;
        self.node_config
            .start_metrics_endpoint(
                prometheus_handle,
                self.provider_factory.db_ref().clone(),
                self.provider_factory.static_file_provider(),
                self.task_executor.clone(),
            )
            .await?;

        let head = self.node_config.lookup_head(self.provider_factory.clone())?;

        // TODO: call hooks?

        let parent_header = self.blockchain_db.sealed_header(head.number)?.expect("Failed to retrieve sealed header from head's block number while starting executor engine");

        // spawn execution engine to extend canonical tip
        let tn_engine = ExecutorEngine::new(
            self.blockchain_db.clone(),
            self.evm_config.clone(),
            self.node_config.debug.max_block,
            BroadcastStream::new(from_consensus),
            parent_header,
        );

        // spawn tn engine
        self.task_executor.spawn_critical_blocking("consensus engine", async move {
            let res = tn_engine.await;
            match res {
                Ok(_) => info!(target: "engine", "TN Engine exited gracefully"),
                Err(e) => error!(target: "engine", ?e, "TN Engine error"),
            }
            // TODO: return oneshot channel here?
        });

        // // TODO: TN needs to support event streams
        // // leaving this here as a reminder of possible events to stream
        // // with the understanding TN solution should be independent of reth
        // let events = stream_select!(
        //     network.event_listener().map(Into::into),
        //     beacon_engine_handle.event_listener().map(Into::into),
        //     pipeline_events_for_events_handler.map(Into::into),
        //     pruner_events.map(Into::into),
        //     static_file_producer_events.map(Into::into),
        // );

        // self.task_executor().spawn_critical(
        //     "events task",
        //     reth_node_events::node::handle_events(
        //         None, // network handle
        //         Some(head.number), // latest block
        //         events,
        //         self.provider_factory.db_ref().clone(),
        //     ),
        // );

        Ok(())
    }

    pub(super) async fn start_batch_maker(
        &mut self,
        to_worker: Sender<NewBatch>,
        worker_id: WorkerId,
    ) -> eyre::Result<()> {
        // TODO: both start_engine and start_batch_maker lookup head
        let head = self.node_config.lookup_head(self.provider_factory.clone())?;

        let ctx = BuilderContext::<WorkerNode<DB, Evm>>::new(
            head,
            self.blockchain_db.clone(),
            self.task_executor.clone(),
            WithConfigs {
                config: self.node_config.clone(),
                toml_config: reth_config::Config::default(), /* mostly peer / staging configs */
            },
        );

        // default tx pool
        let pool_builder = EthereumPoolBuilder::default();

        // taken from components_builder.build_components();
        let transaction_pool = pool_builder.build_pool(&ctx).await?;
        // TODO: this is basically noop and missing some functionality
        let network = WorkerNetwork::default();

        // build batch maker
        let max_transactions = 10;
        let mining_mode =
            MiningMode::instant(max_transactions, transaction_pool.pending_transactions_listener());
        let task = BatchMakerBuilder::new(
            Arc::clone(&self.node_config.chain),
            self.blockchain_db.clone(),
            transaction_pool.clone(),
            to_worker,
            mining_mode,
            self.address,
            self.evm_executor.clone(),
        )
        .build();

        // spawn batch maker mining task
        self.task_executor.spawn_critical("batch maker", task);

        // let mut hooks = EngineHooks::new();

        // let static_file_producer = StaticFileProducer::new(
        //     provider_factory.clone(),
        //     provider_factory.static_file_provider(),
        //     prune.node_config.clone().unwrap_or_default().segments,
        // );
        // let static_file_producer_events = static_file_producer.lock().events();
        // hooks.add(StaticFileHook::new(static_file_producer.clone(), Box::new(executor.clone())));
        // info!(target: "tn::batch_maker", "StaticFileProducer initialized");

        // TODO: adjust instance ports?
        //
        //.node_config.adjust_instance_ports();
        //

        // spawn RPC
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.blockchain_db.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network)
            .with_executor(self.task_executor.clone())
            .with_evm_config(EthEvmConfig::default()) // TODO: this should come from self
            .with_events(self.blockchain_db.clone());

        //.node_configure namespaces
        let modules_config = self.node_config.rpc.transport_rpc_module_config();
        let mut server = rpc_builder.build(modules_config, Box::new(EthApi::with_spawner));

        // TODO: rpc hook here
        // server.merge.node_configured(rpc_ext)?;

        if let Some(faucet_args) = self.opt_faucet_args.take() {
            // create extension from CLI args
            match faucet_args
                .create_rpc_extension(self.blockchain_db.clone(), transaction_pool.clone())
            {
                Ok(faucet_ext) => {
                    // add faucet module
                    if let Err(e) = server.merge_configured(faucet_ext.into_rpc()) {
                        error!(target: "faucet", "Error merging faucet rpc module: {e:?}");
                    }

                    info!(target: "tn::execution", "faucet rpc extension successfully merged");
                }
                Err(e) => {
                    error!(target: "faucet", "Error creating faucet rpc module: {e:?}");
                }
            }
        }

        // start the server
        let server_config = self.node_config.rpc.rpc_server_config();
        let rpc_handle = server_config.start(&server).await?;

        self.workers.insert(worker_id, rpc_handle);
        Ok(())
    }

    /// Create a new batch validator.
    pub(super) fn new_batch_validator(&self) -> BatchValidator<DB, Evm> {
        // validate batches using beaacon consensus
        // to ensure inner-chain compatibility
        let consensus: Arc<dyn Consensus> =
            Arc::new(EthBeaconConsensus::new(self.node_config.chain.clone()));

        // batch validator
        BatchValidator::<DB, Evm>::new(
            consensus,
            self.blockchain_db.clone(),
            self.evm_executor.clone(),
        )
    }

    /// Fetch the last executed state from the database.
    ///
    /// This method is called when the primary spawns to retrieve
    /// the last committed sub dag from it's database in the case
    /// of the node restarting.
    ///
    /// The primary adds +1 to this value for recovering output
    /// since the execution layer is confirming the last executing block.
    pub(super) fn last_executed_output(&self) -> eyre::Result<u64> {
        // NOTE: The payload_builder only extends canonical tip and sets finalized after
        // entire output is successfully executed. This ensures consistent recovery state.
        //
        // For example: consensus round 8 sends an output with 5 blocks, but only 2 blocks are
        // executed before the node restarts. The provider never finalized the round, so the
        // `finalized_block_number` would point to the last block of round 7. The primary
        // would then re-send consensus output for round 8.
        //
        // recover finalized block's nonce: this is the last subdag index from consensus (round)
        let finalized_block_num =
            self.blockchain_db.database_provider_ro()?.last_finalized_block_number()?;
        let last_round_of_consensus = self
            .blockchain_db
            .database_provider_ro()?
            .header_by_number(finalized_block_num)?
            .map(|opt| opt.nonce)
            .unwrap_or(0);

        Ok(last_round_of_consensus)
    }

    /// Return an database provider.
    pub(super) fn get_provider(&self) -> BlockchainProvider<DB> {
        self.blockchain_db.clone()
    }

    /// Return the node's EVM config.
    pub(super) fn get_evm_config(&self) -> CE {
        self.evm_config.clone()
    }

    /// Return the node's evm-based block executor
    pub(super) fn get_block_executor(&self) -> Evm {
        self.evm_executor.clone()
    }

    /// Return a worker's HttpClient if the RpcServer exists.
    pub(super) fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<HttpClient>> {
        let handle = self
            .workers
            .get(worker_id)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .http_client();
        Ok(handle)
    }
}
