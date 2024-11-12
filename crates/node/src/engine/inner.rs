//! Inner-execution node components for both Worker and Primary execution.
//!
//! This module contains the logic for execution.

use super::{TnBuilder, WorkerComponents, WorkerTxPool};
use crate::{
    engine::{WorkerNetwork, WorkerNode},
    error::ExecutionError,
};
use eyre::eyre;
use jsonrpsee::http_client::HttpClient;
use reth::rpc::{
    builder::{config::RethRpcServerConfig, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_auto_seal_consensus::AutoSealConsensus;
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_db_common::init::init_genesis;
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_node_builder::{common::WithConfigs, BuilderContext, NodeConfig};
use reth_node_ethereum::EthEvmConfig;
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, Address, BlockBody, SealedBlock, SealedBlockWithSenders,
};
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    BlockIdReader, BlockReader, CanonStateSubscriptions as _, DatabaseProviderFactory,
    FinalizedBlockReader, HeaderProvider, ProviderFactory, TransactionVariant,
};
use reth_prune::PruneModes;
use reth_tasks::TaskExecutor;
use reth_transaction_pool::{
    blobstore::DiskFileBlobStore, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tn_block_builder::BlockBuilder;
use tn_block_validator::BlockValidator;
use tn_config::Config;
use tn_engine::ExecutorEngine;
use tn_faucet::{FaucetArgs, FaucetRpcExtApiServer as _};
use tn_types::{Consensus, ConsensusOutput, LastCanonicalUpdate, WorkerBlockSender, WorkerId};
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
    /// The validator node config.
    tn_config: Config,
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
    workers: HashMap<WorkerId, WorkerComponents<DB>>,
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
            tn_config,
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
        });

        Ok(())
    }

    /// The worker's RPC, TX pool, and block builder
    pub(super) async fn start_block_builder(
        &mut self,
        worker_id: WorkerId,
        block_provider_sender: WorkerBlockSender,
    ) -> eyre::Result<()> {
        let head = self.node_config.lookup_head(self.provider_factory.clone())?;

        let ctx = BuilderContext::<WorkerNode<DB, Evm>>::new(
            head,
            self.blockchain_db.clone(),
            self.task_executor.clone(),
            WithConfigs {
                config: self.node_config.clone(),
                toml_config: reth_config::Config::default(), /* mostly unused peer and staging
                                                              * configs */
            },
        );

        // inspired by reth's default eth tx pool:
        // - `EthereumPoolBuilder::default()`
        // - `components_builder.build_components()`
        // - `pool_builder.build_pool(&ctx)`
        let transaction_pool = {
            let data_dir = ctx.config().datadir();
            let pool_config = ctx.pool_config();
            let blob_store = DiskFileBlobStore::open(data_dir.blobstore(), Default::default())?;
            let validator = TransactionValidationTaskExecutor::eth_builder(ctx.chain_spec())
                .with_head_timestamp(ctx.head().timestamp)
                .kzg_settings(ctx.kzg_settings()?)
                .with_local_transactions_config(pool_config.local_transactions_config.clone())
                .with_additional_tasks(ctx.config().txpool.additional_validation_tasks)
                .build_with_tasks(
                    ctx.provider().clone(),
                    ctx.task_executor().clone(),
                    blob_store.clone(),
                );

            let transaction_pool =
                reth_transaction_pool::Pool::eth_pool(validator, blob_store, pool_config);

            info!(target: "tn::execution", "Transaction pool initialized");

            let transactions_path = data_dir.txpool_transactions();
            let transactions_backup_config =
                reth_transaction_pool::maintain::LocalTransactionBackupConfig::with_local_txs_backup(transactions_path);

            // spawn task to backup local transaction pool in case of restarts
            ctx.task_executor().spawn_critical_with_graceful_shutdown_signal(
                "local transactions backup task",
                |shutdown| {
                    reth_transaction_pool::maintain::backup_local_transactions_task(
                        shutdown,
                        transaction_pool.clone(),
                        transactions_backup_config,
                    )
                },
            );

            transaction_pool
        };

        // TODO: WorkerNetwork is basically noop and missing some functionality
        let network = WorkerNetwork::default();
        use reth_transaction_pool::TransactionPoolExt as _;
        let mut tx_pool_latest = transaction_pool.block_info();
        tx_pool_latest.pending_basefee = MIN_PROTOCOL_BASE_FEE;
        tx_pool_latest.last_seen_block_hash = ctx
            .provider()
            .finalized_block_hash()?
            .unwrap_or_else(|| self.tn_config.chain_spec().sealed_genesis_header().hash());
        tx_pool_latest.last_seen_block_number =
            ctx.provider().finalized_block_number()?.unwrap_or_default();
        transaction_pool.set_block_info(tx_pool_latest);

        let tip = match tx_pool_latest.last_seen_block_number {
            // use genesis on startup
            0 => SealedBlockWithSenders::new(
                SealedBlock::new(
                    self.tn_config.chain_spec().sealed_genesis_header(),
                    BlockBody::default(),
                ),
                vec![],
            )
            .ok_or_else(|| eyre!("Failed to create genesis block for starting tx pool"))?,
            // retrieve from database
            _ => self
                .blockchain_db
                .sealed_block_with_senders(
                    tx_pool_latest.last_seen_block_hash.into(),
                    TransactionVariant::NoHash,
                )?
                .ok_or_else(|| {
                    eyre!(
                        "Failed to find sealed block during block builder startup! ({} - {:?}) ",
                        tx_pool_latest.last_seen_block_number,
                        tx_pool_latest.last_seen_block_hash,
                    )
                })?,
        };

        let latest_canon_state = LastCanonicalUpdate {
            tip: tip.block,
            pending_block_base_fee: tx_pool_latest.pending_basefee,
            pending_block_blob_fee: tx_pool_latest.pending_blob_fee,
        };

        let block_builder = BlockBuilder::new(
            self.blockchain_db.clone(),
            transaction_pool.clone(),
            self.blockchain_db.canonical_state_stream(),
            latest_canon_state,
            block_provider_sender,
            self.address,
            self.tn_config.parameters.max_worker_block_delay,
        );

        // spawn block builder task
        tokio::spawn(async move {
            let res = block_builder.await;
            info!(target: "tn::execution", ?res, "block builder task exited");
        });

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

        // start the RPC server
        let server_config = self.node_config.rpc.rpc_server_config();
        let rpc_handle = server_config.start(&server).await?;

        // take ownership of worker components
        let components = WorkerComponents::new(rpc_handle, transaction_pool);
        self.workers.insert(worker_id, components);

        Ok(())
    }

    /// Create a new block validator.
    pub(super) fn new_block_validator(&self) -> BlockValidator<DB> {
        // batch validator
        BlockValidator::<DB>::new(self.blockchain_db.clone())
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
            self.blockchain_db.database_provider_ro()?.last_finalized_block_number()?.unwrap_or(0);
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

    /// Return a worker's RpcServerHandle if the RpcServer exists.
    pub(super) fn worker_rpc_handle(&self, worker_id: &WorkerId) -> eyre::Result<&RpcServerHandle> {
        let handle = self
            .workers
            .get(worker_id)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .rpc_handle();
        Ok(handle)
    }

    /// Return a worker's HttpClient if the RpcServer exists.
    pub(super) fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<HttpClient>> {
        let handle = self.worker_rpc_handle(worker_id)?.http_client();
        Ok(handle)
    }

    /// Return a worker's transaction pool if it exists.
    pub(super) fn get_worker_transaction_pool(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<WorkerTxPool<DB>> {
        let tx_pool = self
            .workers
            .get(worker_id)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .pool();

        Ok(tx_pool)
    }

    /// Return a worker's local Http address if the RpcServer exists.
    pub(super) fn worker_http_local_address(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<SocketAddr>> {
        let addr = self.worker_rpc_handle(worker_id)?.http_local_addr();
        Ok(addr)
    }
}
