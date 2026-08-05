//! The [`RethEnv`] for telcoin network internal API.
//!
//! This module owns the facade every other TN crate uses to reach reth. The facade's
//! methods are split across submodules: `epoch` (epoch boundaries and ConsensusRegistry
//! reads), `execution` (block building and atomic canonicalization of consensus
//! output), `genesis` (chain genesis and temp chains), `helpers` (read APIs), and
//! `rpc` (RPC server support).
//!
//! # Construction invariants
//!
//! - [`RethEnv::new`] MUST run inside a tokio runtime: `init_provider_factory` captures
//!   `tokio::runtime::Handle::current()`, which panics outside one.
//! - Opening an env initializes genesis in the database when absent (`init_genesis`), always from
//!   the LOCAL chain spec in the supplied config — genesis is never fetched from the network.
//! - [`RethEnv::new`] has two process-global side effects (base-fee address pinning and metrics
//!   registration) — see its docs.

use std::{path::Path, sync::Arc};

use reth_chainspec::ChainSpec as RethChainSpec;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_node_builder::NodeConfig;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    ProviderFactory, ProviderResult, StateProviderBox, StateProviderFactory as _,
};
use reth_revm::{database::StateProviderDatabase, State};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{gas_accumulator::GasAccumulator, Address, TaskManager, TaskSpawner, B256};
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
    /// This method MUST be called from within a tokio runtime: `init_provider_factory`
    /// captures `tokio::runtime::Handle::current()` for the provider's task runtime,
    /// which panics outside one. Construction also initializes genesis in the database
    /// when it is absent, from the LOCAL chain spec inside `reth_config` — never from
    /// the network.
    ///
    /// # Process-global side effects
    ///
    /// 1. Pins the base-fee recipient: the FIRST call in the process writes the `BASEFEE_ADDRESS`
    ///    `OnceLock` (passing `None` pins `GOVERNANCE_SAFE_ADDRESS`); on every later call
    ///    `basefee_address` is silently ignored. See the static's docs in `lib.rs` for why a
    ///    `None`-first ordering forks the node.
    /// 2. Registers this crate's block-building metric series (`crate::metrics::init`) with
    ///    whatever global metrics recorder is installed right now; the series bind to that recorder
    ///    on first use, so this must run after `tn_metrics::install_recorder` (see `metrics.rs`) or
    ///    the counters stay on the noop recorder.
    pub fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<Self> {
        // Fail fast on a pruning configuration before any database work happens. Every RethEnv in
        // the process funnels through here, and every pinned registry read this env goes on to
        // serve resolves historical state through `pinned_state_and_env`, which reth can only
        // answer from the pinned block while the history covering it is intact.
        reth_config.ensure_archive_mode()?;

        let node_config = reth_config.0.clone();
        let evm_config = TnEvmConfig::new(reth_config.0.chain.clone(), gas_accumulator);
        let provider_factory = Self::init_provider_factory(&node_config, database)?;
        let blockchain_provider = BlockchainProvider::new(provider_factory)?;
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

    /// Return the reth [`NodeConfig`] this env was built from: chain spec, datadir,
    /// and the database/RPC/pool settings reth derives its helpers from.
    pub(crate) fn node_config(&self) -> &NodeConfig<RethChainSpec> {
        &self.inner.node_config
    }

    /// Return the blockchain provider: the full read stack that layers the canonical
    /// in-memory state (recently executed blocks, canonical head, finalized/safe
    /// tracking) over the committed MDBX + static-file state.
    ///
    /// Contrast with reads through its `database_provider_ro()`, which see one
    /// consistent snapshot of COMMITTED state only and never the in-memory blocks.
    /// The read APIs in `env/helpers.rs` pick between the two per call (e.g. the
    /// canonical head comes from the in-memory state while `last_block_number` is
    /// committed-only), and `heal_finalized_to_persisted_tip` in `env/epoch.rs`
    /// depends on the committed-only view to read the persisted tip.
    pub(crate) fn blockchain_provider(&self) -> &BlockchainProvider<TelcoinNode> {
        &self.inner.blockchain_provider
    }

    /// Return the EVM config used to build and execute TN blocks: the chain spec plus
    /// the shared gas accumulator, wiring the TN handler (base-fee redirection, gas-limit
    /// penalty) and the TN precompiles.
    pub(crate) fn evm_config(&self) -> &TnEvmConfig {
        &self.inner.evm_config
    }

    /// Build the read-only database stack over the state at `block_hash`: state provider →
    /// [`StateProviderDatabase`] → [`State`] with bundle updates enabled.
    ///
    /// This is the shared construction for every pinned, non-committing contract read. Callers
    /// create their own EVM over the returned stack and keep their site-specific mapping of the
    /// [`ProviderResult`] error (node-local provider faults classify differently per caller).
    /// Block-building paths use a different, cached DB stack and must not switch to this one.
    pub(crate) fn read_only_state_db(
        &self,
        block_hash: B256,
    ) -> ProviderResult<State<StateProviderDatabase<StateProviderBox>>> {
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(block_hash)?;
        let state = StateProviderDatabase::new(state_provider);
        Ok(State::builder().with_database(state).with_bundle_update().build())
    }

    /// Return the task spawner taken from the `TaskManager` passed to
    /// [`RethEnv::new`]; components built on this env (e.g. the worker transaction
    /// pool) use it so their tasks run under the node's task manager.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.inner.task_spawner
    }
}
