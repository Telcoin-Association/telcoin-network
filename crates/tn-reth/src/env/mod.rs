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
//! - Opening an env loads the datadir's restored-state floor marker when one exists (a datadir
//!   bootstrapped from a snapshot); pinned state reads below the floor are refused — see
//!   `RethEnv::read_only_state_db` in this module.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use reth_chainspec::ChainSpec as RethChainSpec;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_node_builder::NodeConfig;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    ProviderError, ProviderFactory, ProviderResult, StateProviderBox, StateProviderFactory as _,
};
use reth_revm::{database::StateProviderDatabase, State};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{gas_accumulator::GasAccumulator, Address, SealedHeader, TaskManager, TaskSpawner};
use tracing::{debug, info};

use crate::{
    error::RestoredStateFloorError, evm::TnEvmConfig, traits::TelcoinNode, RethConfig, RethDb,
    WorkerTxPool, BASEFEE_ADDRESS,
};

mod epoch;
mod execution;
mod genesis;
mod helpers;
mod output_overlay;
mod rpc;

pub use output_overlay::OutputTrieOverlay;

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
    /// The snapshot's final block `B` — the first block whose state this datadir holds — when it
    /// was bootstrapped from a snapshot; `None` on a normally-synced node.
    ///
    /// Loaded once from the [`RESTORED_STATE_FLOOR_FILE`] marker at construction. The restore
    /// imports state ONLY at `B`: window headers below `B` carry real, resolvable hashes but no
    /// state and no history, and blocks below the window are zero-hash placeholders. So
    /// `read_only_state_db` (the shared state constructor every pinned read goes through)
    /// refuses every pin below the floor: reth's checkpoint-less history walk would otherwise
    /// answer such reads `Ok(None)` per account, silently reading every account as "never
    /// written".
    restored_state_floor: Option<u64>,
    /// TEST-ONLY: number of pending injected pre-commit persist faults.
    ///
    /// While non-zero, each call to `RethEnv::persist_executed_output` consumes one count
    /// and fails with a `TnRethError::Provider` before touching the database, simulating a
    /// node-local storage fault on the durable write. Armed via
    /// `RethEnv::inject_persist_provider_faults` (see `env/execution.rs`).
    #[cfg(any(feature = "test-utils", test))]
    persist_fault_injections: std::sync::atomic::AtomicU32,
    /// TEST-ONLY: number of pending injected LATE (post-`save_blocks`) persist faults.
    ///
    /// While non-zero, each call to `RethEnv::persist_executed_output` consumes one count
    /// and fails with a `TnRethError::Provider` AFTER `save_blocks` has advanced the
    /// process-wide static-file writers, immediately before the database commit, simulating
    /// a node-local fault on the marker writes or the commit itself. Armed via
    /// `RethEnv::inject_late_persist_provider_faults` (see `env/execution.rs`).
    #[cfg(any(feature = "test-utils", test))]
    persist_late_fault_injections: std::sync::atomic::AtomicU32,
    /// TEST-ONLY: how many times `RethEnv::persist_executed_output` has been entered.
    ///
    /// Incremented on entry, before any injected fault is consumed, so the value is the
    /// number of persist ATTEMPTS the engine's bounded retry actually made. Read through
    /// `RethEnv::persist_attempt_count` (see `env/execution.rs`).
    #[cfg(any(feature = "test-utils", test))]
    persist_attempts: std::sync::atomic::AtomicU32,
}

impl std::fmt::Debug for RethEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RethEnv, config: {:?}", self.node_config())
    }
}

/// Set the basefee address.  This will only work on the first call and should be during program
/// initialization. Calling more than once will do nothing, not calling early can lead to an unset
/// basefee address and a chain fork.
///
/// `None` pins the `GOVERNANCE_SAFE_ADDRESS` default. It is passed by the temp-chain and
/// snapshot-restore constructors (genesis tooling, `db load-state`, and tests) — see the
/// `BASEFEE_ADDRESS` docs in `lib.rs` for the first-write-wins hazard. The production node start
/// (`crates/node/src/manager/node.rs`) always supplies a configured address, because the config
/// layer declares `parameters.basefee_address` as a required key.
fn set_basefee_address(address: Option<Address>) {
    // Ignore the error. Should probably panic on error but this will break some test environments.
    let _ = BASEFEE_ADDRESS.set(address.unwrap_or(GOVERNANCE_SAFE_ADDRESS));
}

/// File name, relative to the datadir root, of the restored-state floor marker.
///
/// `SnapshotRestorer::import_chain_scaffold` (`snapshot.rs`) writes it — before any chain data
/// commits — with the snapshot's final block number (the first block whose state the datadir
/// holds), and [`RethEnv::new`] reads it on
/// every construction over the datadir. The file is absent on a normally-synced node. It sits at
/// the datadir root (beside `db` and `static_files`) so any whole-datadir copy carries it.
const RESTORED_STATE_FLOOR_FILE: &str = "restored-state-floor";

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
        // the process funnels through here, and every pinned read this env goes on to serve
        // resolves historical state through `read_only_state_db`, which reth can only answer from
        // the pinned block while the history covering it is intact.
        reth_config.ensure_archive_mode()?;

        let node_config = reth_config.0.clone();
        let evm_config = TnEvmConfig::new(reth_config.0.chain.clone(), gas_accumulator);
        let provider_factory = Self::init_provider_factory(&node_config, database)?;
        let blockchain_provider = BlockchainProvider::new(provider_factory)?;
        let task_spawner = task_manager.get_spawner();
        let restored_state_floor = Self::read_restored_state_floor(&node_config)?;
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
                restored_state_floor,
                #[cfg(any(feature = "test-utils", test))]
                persist_fault_injections: std::sync::atomic::AtomicU32::new(0),
                #[cfg(any(feature = "test-utils", test))]
                persist_late_fault_injections: std::sync::atomic::AtomicU32::new(0),
                #[cfg(any(feature = "test-utils", test))]
                persist_attempts: std::sync::atomic::AtomicU32::new(0),
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

    /// Absolute path of the restored-state floor marker inside `node_config`'s datadir root.
    pub(crate) fn restored_state_floor_path(node_config: &NodeConfig<RethChainSpec>) -> PathBuf {
        Self::restored_state_floor_marker(node_config.datadir().data_dir())
    }

    /// Absolute path of the restored-state floor marker under a TN datadir root, for callers
    /// outside this crate that hold only the datadir path: the CLI's failed-import cleanup
    /// removes the marker through this, so an aborted restore cannot leave a stale floor that
    /// poisons a later normal sync of the same datadir.
    pub fn restored_state_floor_marker(datadir: impl AsRef<Path>) -> PathBuf {
        datadir.as_ref().join(RESTORED_STATE_FLOOR_FILE)
    }

    /// Read the datadir's restored-state floor marker, if one exists.
    ///
    /// A missing file is the normal case (`Ok(None)`): the node was not bootstrapped from a
    /// snapshot. An unreadable or unparseable file is a hard error rather than `None` — treating
    /// a corrupt marker as "no floor" would silently re-admit the below-floor pinned reads the
    /// marker exists to refuse.
    fn read_restored_state_floor(
        node_config: &NodeConfig<RethChainSpec>,
    ) -> eyre::Result<Option<u64>> {
        let path = Self::restored_state_floor_path(node_config);
        std::fs::read_to_string(&path)
            .map(Some)
            .or_else(|e| (e.kind() == std::io::ErrorKind::NotFound).then_some(None).ok_or(e))
            .map_err(|e| {
                eyre::eyre!("failed to read restored-state floor marker {}: {e}", path.display())
            })?
            .map(|contents| {
                contents.trim().parse::<u64>().map_err(|e| {
                    eyre::eyre!(
                        "restored-state floor marker {} does not hold a block number: {e}; \
                         delete the file ONLY if this datadir was never bootstrapped from a \
                         snapshot — a restored datadir without its floor silently serves empty \
                         state for pre-snapshot reads",
                        path.display()
                    )
                })
            })
            .transpose()
    }

    /// Persist the datadir's restored-state floor marker: the snapshot's final block `B`, the
    /// only block whose state the restore imports — nothing below it is readable.
    ///
    /// Called by `SnapshotRestorer::import_chain_scaffold` (`snapshot.rs`) BEFORE any chain data
    /// commits, so no restored datadir can exist without its floor. That ordering only survives
    /// a crash if the marker is durable first: the scaffold's mdbx and static-file commits
    /// fsync, while a bare `std::fs::write` can sit in the page cache indefinitely. So this
    /// stages a temp file, fsyncs it, renames it over the final path (a torn marker can never be
    /// observed), and fsyncs the parent directory. The marker is read back by [`RethEnv::new`]
    /// on every later construction over this datadir — never by the env that wrote it:
    /// `SnapshotRestorer`'s own env was built over the pre-scaffold empty datadir and keeps
    /// `None`, and the restore's only pinned read targets the snapshot's final block, which the
    /// floor admits.
    pub(crate) fn write_restored_state_floor(&self, floor: u64) -> eyre::Result<()> {
        let path = Self::restored_state_floor_path(self.node_config());
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, format!("{floor}\n")).map_err(|e| {
            eyre::eyre!("failed to stage restored-state floor marker {}: {e}", tmp.display())
        })?;
        std::fs::File::open(&tmp).and_then(|f| f.sync_all()).map_err(|e| {
            eyre::eyre!("failed to fsync restored-state floor marker {}: {e}", tmp.display())
        })?;
        std::fs::rename(&tmp, &path).map_err(|e| {
            eyre::eyre!("failed to publish restored-state floor marker {}: {e}", path.display())
        })?;
        path.parent()
            .map(|dir| std::fs::File::open(dir).and_then(|d| d.sync_all()))
            .transpose()
            .map(|_| ())
            .map_err(|e| {
                eyre::eyre!(
                    "failed to fsync the datadir holding restored-state floor marker {}: {e}",
                    path.display()
                )
            })
    }

    /// Return the lowest block number whose header is guaranteed REAL on this datadir.
    ///
    /// `0` on a normally-synced node: every persisted header is real down to genesis. On a
    /// snapshot-restored datadir, real headers exist only inside the restore window; every block
    /// below it is a scaffold dummy (`ExecHeader::default()` with a zero hash and zero nonce,
    /// `SnapshotRestorer::import_chain_scaffold` in `snapshot.rs`). Backward header walks must
    /// stop here instead of genesis: below this bound the dummy nonces never change, so a walk
    /// keyed on nonce changes (`last_executed_output_blocks` in `tn-node`) would read every
    /// header down to block 0 (O(chain-height) reads inside startup) and hand its caller a
    /// synthetic header (issue #1321).
    ///
    /// Only the snapshot block `B` is persisted (the floor marker), not the window start, so the
    /// bound is derived: the restore admits a window only if it starts at or below
    /// `max(1, B - (BLOCKHASH_ANCESTORS - 1))` (`import_chain_scaffold`), making that expression
    /// the lowest block guaranteed real. The actual window may reach one block lower; stopping
    /// at the guarantee merely trims the walk by that block, while trusting an unpersisted
    /// window start could admit a dummy read.
    pub fn real_header_floor(&self) -> u64 {
        use tn_storage::exec_state_pack::BLOCKHASH_ANCESTORS;
        self.inner
            .restored_state_floor
            .map_or(0, |b| b.saturating_sub(BLOCKHASH_ANCESTORS - 1).max(1))
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

        // Reconcile the storage backends before anything reads or writes through the factory.
        // `persist_executed_output` (env/execution.rs) commits through reth's non-atomic
        // multi-backend commit: static files finalize, RocksDB commits, then MDBX commits last.
        // A crash inside that window durably leaves static files and/or RocksDB one block ahead
        // of the MDBX tip. `check_consistency` heals exactly that direction in place: it prunes
        // the leading backends down to the MDBX stage checkpoints, which `save_blocks` advances
        // inside the same MDBX transaction as the blocks. Without this call the divergence is
        // permanent: every restart re-persists the same block into static files that already
        // hold it, `save_blocks` fails with `UnexpectedStaticFileBlockNumber`, and the engine's
        // `retryable_persist_fault` correctly treats that as terminal (issue #1238).
        //
        // A returned unwind target is the opposite divergence: MDBX committed past a backend
        // that lost data. The commit ordering cannot produce that state; only corruption or
        // manual datadir changes can. Reth's launcher heals it by running an unwind pipeline,
        // but TN has no stage pipeline to unwind MDBX, so refuse to start instead of serving a
        // datadir whose backends disagree.
        //
        // If TN ever adopts reth's storage v2 settings, re-verify snapshot-restored datadirs
        // against this check first: v2 adds checked segments (transaction senders) that the
        // restore scaffold must leave exactly at the stage checkpoints, or startup refuses here.
        let (rocksdb_unwind, static_file_unwind) = provider_factory.check_consistency()?;
        [("RocksDB", rocksdb_unwind), ("the static files", static_file_unwind)]
            .into_iter()
            .filter_map(|(backend, target)| {
                target.map(|block| format!("{backend} (unwind target {block})"))
            })
            .reduce(|left, right| format!("{left}, {right}"))
            .map_or(Ok(()), |lagging| {
                Err(eyre::eyre!(
                    "storage consistency check found the database ahead of {lagging}. Startup \
                     cannot heal this direction: the backend lost data, which indicates \
                     corruption or manual datadir changes. Restore the datadir from a snapshot \
                     or backup before restarting"
                ))
            })?;

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

    /// Initialize a worker transaction pool WITHOUT its canonical-state maintenance task.
    ///
    /// Test seam for the broadcast-lag recovery path (issue #1236): a lagged pool is one
    /// whose maintenance task missed `Commit` notifications, and reproducing that state
    /// deterministically requires committing canonical blocks while no task is subscribed.
    #[cfg(test)]
    pub(crate) fn init_txn_pool_without_maintenance(&self) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::build(
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

    /// Build the read-only database stack over the state at `header`: state provider →
    /// [`StateProviderDatabase`] → [`State`] with bundle updates enabled.
    ///
    /// This is the shared construction for every pinned, non-committing contract read, and the
    /// ONE enforcement point for the restored-state floor: a pin below the floor is refused
    /// here, before any provider state is touched, as a [`RestoredStateFloorError`] carried in
    /// [`ProviderError::Other`], so every consumer inherits the refusal instead of silently
    /// reading every account as "never written" (the gap `read_contract_at_block` had, #1136).
    /// Taking the full [`SealedHeader`] rather than a bare hash removes the number/hash
    /// ambiguity of the hash-only signature: every production caller resolves the header from
    /// the provider, so the compared number and the resolved hash come from one record. A
    /// hand-built `SealedHeader` can still pair a number with a foreign hash (tests do), so the
    /// floor guards against API misuse, not against forged headers.
    ///
    /// ARCHIVE-MODE ASSUMPTION: the `state_by_block_hash` call below stays deterministic only
    /// because this node never constructs a pruner; with pruning enabled, reth's historical
    /// provider can silently fall back to TIP state for a pinned read. Revisit every consumer
    /// of this constructor before enabling pruning (the full note lives on
    /// `pinned_state_and_env` in `env/epoch.rs`).
    ///
    /// Callers create their own EVM over the returned stack and keep their site-specific mapping
    /// of the [`ProviderResult`] error (node-local provider faults classify differently per
    /// caller). Block-building paths use a different, cached DB stack and must not switch to
    /// this one.
    pub(crate) fn read_only_state_db(
        &self,
        header: &SealedHeader,
    ) -> ProviderResult<State<StateProviderDatabase<StateProviderBox>>> {
        // refuse pins below the restored-state floor before touching the provider: below it the
        // datadir holds headers but no state, and the read would resolve silently to "never
        // written" values instead of failing
        self.inner
            .restored_state_floor
            .filter(|floor| header.number < *floor)
            .map_or(Ok(()), |floor| {
                Err(ProviderError::other(RestoredStateFloorError::new(header.number, floor)))
            })?;
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(header.hash())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            test_genesis_with_consensus_registry,
        },
    };
    use reth_provider::{
        DatabaseProviderFactory as _, StageCheckpointReader as _, StaticFileProviderFactory as _,
        StaticFileSegment, StaticFileWriter as _,
    };
    use reth_stages_types::StageId;
    use tempfile::TempDir;
    use tn_types::TaskManager;

    /// Regression test for issue #1238: startup must reconcile static files left AHEAD of the
    /// database by a crash inside `persist_executed_output`'s commit window.
    ///
    /// `persist_executed_output` (env/execution.rs) commits through reth's non-atomic
    /// multi-backend commit: the static-file writers finalize first, the MDBX transaction
    /// commits last. A crash between the two durably leaves the headers static file one block
    /// ahead of the MDBX tip. Without `check_consistency` in `init_provider_factory` that
    /// divergence was permanent: every restart re-persisted the same block into static files
    /// that already hold it, `save_blocks` failed with
    /// `ProviderError::UnexpectedStaticFileBlockNumber`, and the engine's
    /// `retryable_persist_fault` correctly treated that as terminal: a crash loop.
    ///
    /// The test persists block 1 through the production path, then reproduces the crash window
    /// for block 2 by hand: `save_blocks` appends block 2 to both backends, ONLY the headers
    /// static-file writer commits, and the MDBX transaction is dropped uncommitted. On disk the
    /// headers static file is durably at 2 while MDBX is at 1, exactly the post-crash state.
    /// A "restart" (a second `RethEnv` over the same datadir) must then (a) construct Ok,
    /// (b) heal the headers static-file tip back to 1 (`check_consistency` prunes the leading
    /// static files down to the MDBX stage checkpoints), and (c) accept block 2 again through
    /// the normal persist path.
    #[tokio::test]
    async fn test_startup_heals_static_files_ahead_of_database() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp = TempDir::new().expect("create temp dir");
        let tm1 = TaskManager::new("consistency check env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm1, None)?;

        // block 1: execute + durably persist through the production path
        let output1 = consensus_output_for_tests(2, 0, 1, false);
        let payload1 = TNPayload::new_for_test(chain.sealed_genesis_header(), &output1);
        let block1 = execute_payload_and_update_canonical_chain(&env1, payload1, vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();
        assert_eq!(env1.last_block_number()?, 1, "block 1 must be committed");

        // block 2: build it, then simulate the crash inside the commit window. save_blocks
        // appends block 2 to the static-file writers and the MDBX transaction; committing ONLY
        // the headers static-file writer and then dropping the provider (aborting the MDBX
        // transaction) leaves the headers static file durably at 2 while MDBX stays at 1.
        let output2 = consensus_output_for_tests(2, 0, 2, false);
        let payload2 = TNPayload::new_for_test(header1.clone(), &output2);
        let no_txs: Vec<Vec<u8>> = Vec::new();
        let block2 =
            env1.build_block_from_batch_payload(payload2, &no_txs, &mut OutputTrieOverlay::new())?;
        {
            let provider_rw = env1.blockchain_provider().database_provider_rw()?;
            provider_rw.save_blocks(vec![block2], reth_provider::SaveBlocksMode::Full)?;
            let sf = provider_rw.static_file_provider();
            sf.latest_writer(StaticFileSegment::Headers)?.commit()?;
            // drop WITHOUT provider_rw.commit(): the MDBX transaction aborts
            drop(provider_rw);
        }
        assert_eq!(
            env1.blockchain_provider()
                .static_file_provider()
                .get_highest_static_file_block(StaticFileSegment::Headers),
            Some(2),
            "crash window must leave the headers static file durably at 2"
        );
        // the MDBX-side oracle is the Headers stage checkpoint: it is exactly what
        // `check_consistency` compares the static-file tip against, and unlike
        // `last_block_number` (which resolves through the headers static file, now at 2) it
        // only moves when the MDBX transaction commits
        let headers_checkpoint = env1
            .blockchain_provider()
            .database_provider_ro()?
            .get_stage_checkpoint(StageId::Headers)?
            .map_or(0, |checkpoint| checkpoint.block_number);
        assert_eq!(headers_checkpoint, 1, "the Headers checkpoint must still be 1 after the abort");
        drop(env1);
        drop(tm1);

        // "restart": reopen the same datadir; (a) construction must succeed
        let tm2 = TaskManager::new("consistency check env2");
        let env2 = RethEnv::new_for_temp_chain(chain, tmp.path(), &tm2, None)?;

        // (b) startup healed the headers static-file tip back to the MDBX tip
        assert_eq!(
            env2.blockchain_provider()
                .static_file_provider()
                .get_highest_static_file_block(StaticFileSegment::Headers),
            Some(1),
            "check_consistency must prune the headers static file back to the MDBX tip"
        );
        assert_eq!(env2.last_block_number()?, 1, "the committed tip must read 1 after the heal");

        // (c) block 2 re-persists cleanly through the production path, the exact step that
        // crash-looped with `UnexpectedStaticFileBlockNumber` before the fix
        let output2 = consensus_output_for_tests(2, 0, 2, false);
        let payload2 = TNPayload::new_for_test(header1, &output2);
        execute_payload_and_update_canonical_chain(&env2, payload2, vec![])?;
        assert_eq!(env2.last_block_number()?, 2, "block 2 must commit after the heal");

        Ok(())
    }
}
