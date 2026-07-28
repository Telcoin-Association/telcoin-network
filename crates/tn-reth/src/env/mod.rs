//! The `RethEnv` for telcoin network internal API.

use std::{path::Path, sync::Arc};

use reth_chainspec::ChainSpec as RethChainSpec;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_node_builder::NodeConfig;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    ProviderFactory,
};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{gas_accumulator::RewardsCounter, Address, TaskManager, TaskSpawner};
use tracing::{debug, info};

use crate::{
    evm::TnEvmConfig, traits::TelcoinNode, RethConfig, RethDb, WorkerTxPool, BASEFEE_ADDRESS,
};

mod epoch;
mod execution;
mod genesis;
mod helpers;
mod rpc;

/// This is a wrapped abstraction around Reth.
///
/// It should allow the telcoin app to access the required functionality without
/// leaking Reth internals all over the codebase (this makes staying up to date
/// VERY time consuming).
///
/// `RethEnv` wraps its fields in an `Arc<RethEnvInner>` so that cloning is cheap
/// (just an `Arc` bump) and the type is trivially `Send + Sync`.
#[derive(Clone)]
pub struct RethEnv {
    /// The inner state wrapped in an Arc for cheap cloning and thread safety.
    inner: Arc<RethEnvInner>,
}

/// Inner state for [`RethEnv`].
///
/// This struct holds all the actual fields and is wrapped in an `Arc` by `RethEnv`.
struct RethEnvInner {
    /// The type that holds all information needed to launch the node's engine.
    ///
    /// The [NodeConfig] is reth-specific and holds many helper functions that
    /// help TN stay in-sync with the Ethereum community.
    node_config: NodeConfig<RethChainSpec>,
    /// Type that fetches data from the database.
    blockchain_provider: BlockchainProvider<TelcoinNode>,
    /// The type to configure the EVM for execution.
    evm_config: TnEvmConfig,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
}

impl std::fmt::Debug for RethEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RethEnv, config: {:?}", self.node_config())
    }
}

/// Set the basefee address.  This will only work on the first call and should be during program
/// initialization. Calling more than once will do nothing, not calling early can lead to an unset
/// basefee address and a chain fork.
fn set_basefee_address(address: Option<Address>) {
    // Ignore the error. Should probably panic on error but this will break some test environments.
    let _ = BASEFEE_ADDRESS.set(address.unwrap_or(GOVERNANCE_SAFE_ADDRESS));
}

impl RethEnv {
    /// Produce a new wrapped Reth environment from a config, DB path and task manager.
    ///
    /// This method MUST be called from within a tokio runtime.
    pub fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<Self> {
        let node_config = reth_config.0.clone();
        let evm_config = TnEvmConfig::new(reth_config.0.chain.clone(), rewards_counter);
        let provider_factory = Self::init_provider_factory(&node_config, database)?;
        let blockchain_provider = BlockchainProvider::new(provider_factory.clone())?;
        let task_spawner = task_manager.get_spawner();
        set_basefee_address(basefee_address);
        // baseline the block-building counters at zero now, while the recorder is known to be
        // installed, so a node that never drops a transaction still exports both series
        crate::metrics::init();

        Ok(Self {
            inner: Arc::new(RethEnvInner {
                node_config,
                blockchain_provider,
                evm_config,
                task_spawner,
            }),
        })
    }

    /// Create a new Reth DB.
    /// Break this out so this can be created upfront and used even on a
    /// restart (when catching up for instance).
    pub fn new_database<P: AsRef<Path>>(
        reth_config: &RethConfig,
        db_path: P,
    ) -> eyre::Result<RethDb> {
        let db_path = db_path.as_ref();
        info!(target: "tn::reth", path = ?db_path, "opening database");
        // with_metrics: record per-operation db latency metrics (noop unless the global
        // metrics recorder is installed, i.e. the node runs with `--metrics`)
        Ok(Arc::new(init_db(db_path, reth_config.0.db.database_args())?.with_metrics()))
    }

    /// Initialize the provider factory and related components
    fn init_provider_factory(
        node_config: &NodeConfig<RethChainSpec>,
        database: Arc<DatabaseEnv>,
    ) -> eyre::Result<ProviderFactory<TelcoinNode>> {
        // Initialize provider factory with static files
        let datadir = node_config.datadir();
        let rocksdb_provider = reth_provider::providers::RocksDBProvider::new(datadir.data_dir())?;
        let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
        let provider_factory = ProviderFactory::new(
            database,
            Arc::clone(&node_config.chain),
            StaticFileProvider::read_write(datadir.static_files())?,
            rocksdb_provider,
            runtime,
        )?;

        // Initialize genesis if needed
        let genesis_hash = init_genesis(&provider_factory)?;
        debug!(target: "tn::execution", chain=%node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        Ok(provider_factory)
    }

    /// Initialize a new transaction pool for worker.
    pub fn init_txn_pool(&self) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::new(
            self.node_config(),
            self.get_task_spawner(),
            self.blockchain_provider(),
            self.evm_config(),
        )
    }

    // TODO: doc comment
    pub(crate) fn node_config(&self) -> &NodeConfig<RethChainSpec> {
        &self.inner.node_config
    }

    // TODO: doc comment
    pub(crate) fn blockchain_provider(&self) -> &BlockchainProvider<TelcoinNode> {
        &self.inner.blockchain_provider
    }

    // TODO: doc comment
    pub(crate) fn evm_config(&self) -> &TnEvmConfig {
        &self.inner.evm_config
    }

    /// todo: doc comment
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.inner.task_spawner
    }
}
