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
use reth_node_core::node_config::NodeConfig;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    ProviderError, ProviderFactory, ProviderResult, StateProviderBox, StateProviderFactory as _,
};
use reth_revm::{database::StateProviderDatabase, State};
use reth_rpc_builder::config::RethRpcServerConfig as _;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    Address, ExecutionBacklog, SealedHeader, TaskManager, TaskSpawner,
};
use tracing::{debug, info, warn};

use self::mdbx_advice::MdbxAdvice;
use crate::{
    error::RestoredStateFloorError,
    evm::TnEvmConfig,
    rpc_tx_forward::{SanitizeRules, TxForwardConfig, TxForwarder},
    traits::TelcoinNode,
    RethConfig, RethDb, WorkerTxPool, BASEFEE_ADDRESS,
};

mod epoch;
mod execution;
mod genesis;
mod helpers;
mod mdbx_advice;
mod output_overlay;
mod rpc;

pub use execution::{BlockBuildTimings, OutputBuildScope};
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
    /// The `--forward-txs` relay, `None` when RPC submissions go to the local pool.
    ///
    /// Built once and shared by every worker's RPC server (`get_rpc_server`), so all lanes use
    /// one ordered target list, one connection pool per target and one metrics set.
    tx_forwarder: Option<Arc<TxForwarder>>,
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
    /// TEST-ONLY: the persist delay and the persist/build interleaving armed through
    /// `RethEnv::inject_persist_delay` and `RethEnv::interleave_next_persist` (see
    /// `env/execution.rs`).
    #[cfg(any(feature = "test-utils", test))]
    persist_holds: execution::PersistHolds,
    /// The node's runtime switches for this env, fixed at construction.
    switches: RethSwitches,
    /// The execution database's map advice (`mdbx_random_advice`) and the storage counters each
    /// persist reports.
    mdbx: MdbxAdvice,
}

/// The runtime switches from `parameters.yaml` that the node hands to its [`RethEnv`].
///
/// Each field's default keeps the behaviour from before the switch existed, and
/// [`RethEnv::new`] uses the defaults. The node builds this from its parameters and passes it to
/// [`RethEnv::new_with_switches`]; the env exposes each value through a getter of the same name.
#[derive(Clone, Debug, Default)]
pub struct RethSwitches {
    /// Advise the kernel that the execution database's memory map is read at random
    /// (`mdbx_random_advice`; default `false`).
    pub mdbx_random_advice: bool,
    /// Keep a sealed batch's transactions in the pool, marked, until they execute
    /// (`sealed_tx_keep`; default `false`).
    pub sealed_tx_keep: bool,
    /// Refuse RPC transaction submissions while execution is this many consensus outputs behind
    /// (`rpc_admission_backlog_outputs`; default `0`, no bound).
    pub rpc_admission_backlog_outputs: usize,
    /// The node's shared count of consensus outputs execution trails behind. Not a switch: the
    /// one handle the node, its engine and the RPC submission guard share. The default is a
    /// fresh handle that nothing else writes.
    pub execution_backlog: ExecutionBacklog,
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
    /// 3. With `--forward-txs`, registers the forwarding metric series the same way, and building
    ///    an `https://` target's client installs rustls's `ring` crypto provider as the process
    ///    default when none is installed yet.
    pub fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<Self> {
        Self::new_with_switches(
            reth_config,
            task_manager,
            database,
            basefee_address,
            gas_accumulator,
            RethSwitches::default(),
        )
    }

    /// Produce a new wrapped Reth environment, like [`RethEnv::new`], with the node's runtime
    /// switches.
    ///
    /// The node starts its env through here so the switches read from `parameters.yaml` reach the
    /// reth side; every other caller uses [`RethEnv::new`], which passes the defaults. The same
    /// runtime requirement, genesis initialization and process-global side effects apply.
    ///
    /// With `mdbx_random_advice` on, the env advises every mapping of the database's `mdbx.dat` as
    /// read at random before it writes genesis or anything else (`env/mdbx_advice.rs`).
    pub fn new_with_switches(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        gas_accumulator: GasAccumulator,
        switches: RethSwitches,
    ) -> eyre::Result<Self> {
        // Fail fast on a pruning configuration before any database work happens. Every RethEnv in
        // the process funnels through here, and every pinned read this env goes on to serve
        // resolves historical state through `read_only_state_db`, which reth can only answer from
        // the pinned block while the history covering it is intact.
        reth_config.ensure_archive_mode()?;

        // advise the map before this env writes genesis or anything else
        let mdbx = MdbxAdvice::open(&database, switches.mdbx_random_advice);
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
        let tx_forwarder = Self::new_tx_forwarder(reth_config.tx_forward(), &node_config)?;

        // read the e2e persist delay once at start (`None` without tn-types/test-utils): a node
        // that has it logs so before its first persist, which a run killed early never
        // reaches
        let _ = tn_types::test_hooks::persist_delay();
        Ok(Self {
            inner: Arc::new(RethEnvInner {
                node_config,
                blockchain_provider,
                evm_config,
                task_spawner,
                restored_state_floor,
                tx_forwarder,
                #[cfg(any(feature = "test-utils", test))]
                persist_fault_injections: std::sync::atomic::AtomicU32::new(0),
                #[cfg(any(feature = "test-utils", test))]
                persist_late_fault_injections: std::sync::atomic::AtomicU32::new(0),
                #[cfg(any(feature = "test-utils", test))]
                persist_attempts: std::sync::atomic::AtomicU32::new(0),
                #[cfg(any(feature = "test-utils", test))]
                persist_holds: execution::PersistHolds::default(),
                switches,
                mdbx,
            }),
        })
    }

    /// Whether the node advises the kernel that the execution database's memory map is read at
    /// random (`mdbx_random_advice` in `parameters.yaml`; `false` unless the node set it through
    /// [`RethEnv::new_with_switches`]).
    pub fn mdbx_random_advice(&self) -> bool {
        self.inner.switches.mdbx_random_advice
    }

    /// Whether the worker pools keep a sealed batch's transactions, marked, until they execute
    /// (`sealed_tx_keep` in `parameters.yaml`; `false` unless the node set it through
    /// [`RethEnv::new_with_switches`]).
    pub fn sealed_tx_keep(&self) -> bool {
        self.inner.switches.sealed_tx_keep
    }

    /// How many consensus outputs execution may trail before the RPC submission guard refuses
    /// new transactions (`rpc_admission_backlog_outputs` in `parameters.yaml`; zero, no bound,
    /// unless the node set it through [`RethEnv::new_with_switches`]).
    pub fn rpc_admission_backlog_outputs(&self) -> usize {
        self.inner.switches.rpc_admission_backlog_outputs
    }

    /// The node's shared [`ExecutionBacklog`], the count of consensus outputs execution trails
    /// behind. The engine built on this env records into it, and the RPC submission guard is the
    /// reader it exists for. An env built through [`RethEnv::new`] holds a fresh handle that
    /// nothing outside the env shares.
    pub fn execution_backlog(&self) -> &ExecutionBacklog {
        &self.inner.switches.execution_backlog
    }

    /// Build the shared `--forward-txs` relay, or `None` when forwarding is off.
    ///
    /// The clients connect lazily, so an unreachable target does not fail startup. The log
    /// line carries the target count only: the targets are private operator configuration.
    fn new_tx_forwarder(
        config: &TxForwardConfig,
        node_config: &NodeConfig<RethChainSpec>,
    ) -> eyre::Result<Option<Arc<TxForwarder>>> {
        config
            .targets
            .as_ref()
            .map(|targets| {
                let sanitize =
                    config.sanitize.then(|| SanitizeRules::new(node_config.chain.chain.id()));
                let forwarder = TxForwarder::new(
                    targets,
                    sanitize,
                    node_config.rpc.rpc_max_request_size_bytes(),
                )?;
                info!(
                    target: "tn::rpc",
                    targets = targets.as_slice().len(),
                    sanitize = config.sanitize,
                    "forwarding raw transaction submissions"
                );
                Ok(Arc::new(forwarder))
            })
            .transpose()
    }

    /// Whether `eth_sendRawTransaction` and `eth_sendRawTransactionSync` are forwarded to the
    /// `--forward-txs` targets instead of entering this node's pool.
    pub fn forwards_transactions(&self) -> bool {
        self.inner.tx_forwarder.is_some()
    }

    /// Warn, at most once per process, that `--forward-txs` is set on a committee member.
    ///
    /// The flag is meant for public RPC observers. On a committee member it still works, but
    /// transactions submitted to this node's RPC skip its own pool and batches. This is never an
    /// error. Called at the start of every epoch in which the node is a committee member, so a
    /// node promoted after startup is warned too. Returns whether this call logged the warning.
    pub fn warn_if_forwarding_on_committee_member(&self) -> bool {
        let first = self
            .inner
            .tx_forwarder
            .as_ref()
            .is_some_and(|forwarder| forwarder.claim_committee_warning());
        if first {
            warn!(
                target: "tn::rpc",
                "--forward-txs is set on a committee member: RPC submissions bypass this node's pool"
            );
        }
        first
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
    ///
    /// The `base_fee` container supplies the pool's pending base fee for the worker's current
    /// epoch (issue #1262). The pool's limits follow this env's `sealed_tx_keep` switch (see
    /// [`WorkerTxPool::new_with_sealed_tx_keep`]).
    pub fn init_txn_pool(&self, base_fee: BaseFeeContainer) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::new_with_sealed_tx_keep(
            self.node_config(),
            self.get_task_spawner(),
            self.blockchain_provider(),
            self.evm_config(),
            base_fee,
            self.sealed_tx_keep(),
        )
    }

    /// Initialize a worker transaction pool WITHOUT its canonical-state maintenance task.
    ///
    /// Test seam for the broadcast-lag recovery path (issue #1236): a lagged pool is one
    /// whose maintenance task missed `Commit` notifications, and reproducing that state
    /// deterministically requires committing canonical blocks while no task is subscribed.
    #[cfg(test)]
    pub(crate) fn init_txn_pool_without_maintenance(
        &self,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::build(
            self.node_config(),
            self.get_task_spawner(),
            self.blockchain_provider(),
            self.evm_config(),
            base_fee,
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

    /// A new datadir is written with reth's storage v1 layout, and a restart keeps it.
    ///
    /// The layout decides where receipts, change sets and history indices live, and historical
    /// reads (including consensus-registry reads at past blocks) depend on the history indices.
    /// A node on v2 would write no MDBX history indices and disagree with v1 nodes on those
    /// reads. TN pins v1 through one vendored line, `StorageSettings::base()` (TN-0014), which
    /// `init_genesis` writes into a new datadir; upstream main returns v2 there, so an upstream
    /// sync that takes that line would silently switch new nodes. This test fails instead.
    #[tokio::test]
    async fn test_new_datadir_uses_storage_v1() -> eyre::Result<()> {
        use reth_provider::{MetadataProvider as _, StorageSettings, StorageSettingsCache as _};

        assert_eq!(StorageSettings::base(), StorageSettings::v1());

        let genesis = test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp = TempDir::new().expect("create temp dir");
        for run in ["new datadir", "reopened datadir"] {
            let tm = TaskManager::new(run);
            let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None)?;
            let provider = env.blockchain_provider().database_provider_ro()?;
            assert_eq!(provider.storage_settings()?, Some(StorageSettings::v1()), "{run}");
            assert_eq!(provider.cached_storage_settings(), StorageSettings::v1(), "{run}");
        }
        Ok(())
    }

    // ---- bench6 sealed-keep ----

    /// The worker pool an env builds follows the env's `sealed_tx_keep`: with the switch on its
    /// pending sub-pool holds twice the limits of a switch-off env's pool (the in-flight sealed
    /// transactions stay pooled), so the switch reaches the pool the node really runs.
    #[tokio::test]
    async fn test_init_txn_pool_follows_sealed_tx_keep() -> eyre::Result<()> {
        let tm = TaskManager::new("pool limits follow the switch");
        let off_dir = TempDir::new()?;
        let off = env_with_switches(off_dir.path(), &tm, RethSwitches::default())?;
        let off_limit = off.init_txn_pool(BaseFeeContainer::default())?.pending_sub_pool_limit();
        assert!(off_limit.max_txs > 0 && off_limit.max_size > 0, "{off_limit:?}");

        let on_dir = TempDir::new()?;
        let switches = RethSwitches { sealed_tx_keep: true, ..Default::default() };
        let on = env_with_switches(on_dir.path(), &tm, switches)?;
        let on_limit = on.init_txn_pool(BaseFeeContainer::default())?.pending_sub_pool_limit();
        assert_eq!(on_limit.max_txs, off_limit.max_txs * 2);
        assert_eq!(on_limit.max_size, off_limit.max_size * 2);
        Ok(())
    }

    // ---- bench6 mdbx-advice ----

    /// The genesis time of every env [`env_with_switches`] opens, so that a second open of one
    /// datadir reads the genesis the first one wrote.
    const REOPEN_GENESIS_TIMESTAMP: u64 = 1_700_000_000;

    /// Open an env over a fresh datadir at `dir` through [`RethEnv::new_with_switches`], the way
    /// the node opens its own.
    fn env_with_switches(
        dir: &Path,
        task_manager: &TaskManager,
        switches: RethSwitches,
    ) -> eyre::Result<RethEnv> {
        use reth_node_core::{
            args::{DatabaseArgs, DatadirArgs},
            dirs::MaybePlatformPath,
        };
        crate::init_reth_defaults();
        // a fixed genesis time: `test_genesis()` stamps the current second, so a test that opens
        // the same datadir twice would see two genesis hashes whenever a second ticks in between
        let chain: Arc<RethChainSpec> =
            Arc::new(tn_types::test_genesis_at(REOPEN_GENESIS_TIMESTAMP).into());
        let config = RethConfig::from_node_config(NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(dir.to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            // a small map, like a temp chain's, so parallel tests do not exhaust address space
            db: DatabaseArgs {
                max_size: Some(2 * 1024 * 1024 * 1024),
                growth_step: Some(4 * 1024 * 1024),
                ..Default::default()
            },
            ..NodeConfig::default()
        });
        let database = RethEnv::new_database(&config, dir.join("db"))?;
        RethEnv::new_with_switches(
            &config,
            task_manager,
            database,
            None,
            GasAccumulator::default(),
            switches,
        )
    }

    /// Each switch handed to [`RethEnv::new_with_switches`] reads back from the env, and
    /// [`RethEnv::new`] leaves every switch at its default.
    #[tokio::test]
    async fn test_new_with_switches_hands_each_switch_to_the_env() -> eyre::Result<()> {
        let tm = TaskManager::new("switches");
        let on = TempDir::new()?;
        let backlog = ExecutionBacklog::new();
        let switches = RethSwitches {
            mdbx_random_advice: true,
            sealed_tx_keep: true,
            rpc_admission_backlog_outputs: 2,
            execution_backlog: backlog.clone(),
        };
        let env = env_with_switches(on.path(), &tm, switches)?;
        assert!(env.mdbx_random_advice());
        assert!(env.sealed_tx_keep());
        assert_eq!(env.rpc_admission_backlog_outputs(), 2);
        // the env holds the node's handle, not a copy of its counters
        backlog.record_forwarded(7);
        assert_eq!(env.execution_backlog().last_forwarded(), Some(7));
        env.execution_backlog().record_executed(5);
        assert_eq!(backlog.outputs_behind(), 2);

        let off = TempDir::new()?;
        let env = env_with_switches(off.path(), &tm, RethSwitches::default())?;
        assert!(!env.mdbx_random_advice());
        assert!(!env.sealed_tx_keep());
        assert_eq!(env.rpc_admission_backlog_outputs(), 0);

        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let temp = TempDir::new()?;
        let env = RethEnv::new_for_temp_chain(chain, temp.path(), &tm, None)?;
        assert!(!env.mdbx_random_advice(), "RethEnv::new passes the defaults");
        assert!(!env.sealed_tx_keep(), "RethEnv::new passes the defaults");
        assert_eq!(env.rpc_admission_backlog_outputs(), 0, "RethEnv::new passes the defaults");
        assert_eq!(env.execution_backlog().outputs_behind(), 0);
        Ok(())
    }

    /// [`env_with_switches`] with only `mdbx_random_advice` set.
    fn env_with_advice(
        dir: &Path,
        task_manager: &TaskManager,
        advice: bool,
    ) -> eyre::Result<RethEnv> {
        let switches = RethSwitches { mdbx_random_advice: advice, ..Default::default() };
        env_with_switches(dir, task_manager, switches)
    }

    /// Create the database under `dir` in an env that is then closed, as the deploy's stage-only
    /// start does, so the next open is a re-open: libmdbx then advises only the pages in use.
    fn create_and_close(dir: &Path) -> eyre::Result<()> {
        let task_manager = TaskManager::new("create");
        drop(env_with_advice(dir, &task_manager, false)?);
        Ok(())
    }

    /// The value of gauge `name` in `snapshot`.
    fn gauge_value(
        snapshot: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            metrics_util::debugging::DebugValue,
        )],
        name: &str,
    ) -> Option<f64> {
        snapshot.iter().find(|(key, ..)| key.key().name() == name).and_then(|(.., value)| {
            match value {
                metrics_util::debugging::DebugValue::Gauge(gauge) => Some(gauge.into_inner()),
                _ => None,
            }
        })
    }

    /// With the switch on, every `mdbx.dat` mapping of a re-opened database carries `rr`, and the
    /// advised bytes (on the env and in `tn_reth_mdbx_random_advice_bytes`) equal the whole map,
    /// not the file. `tn_reth_mdbx_used_bytes` is set at open.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_mdbx_random_advice_covers_every_mapping_of_a_reopened_database(
    ) -> eyre::Result<()> {
        use super::mdbx_advice::test_support::mappings_of;
        use metrics_util::debugging::DebuggingRecorder;

        let dir = TempDir::new()?;
        create_and_close(dir.path())?;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let tm = TaskManager::new("reopen with advice");
        let env =
            ::metrics::with_local_recorder(&recorder, || env_with_advice(dir.path(), &tm, true))?;

        let mappings = mappings_of(&dir.path().join("db").join("mdbx.dat"));
        assert!(!mappings.is_empty(), "the database is mapped");
        assert!(mappings.iter().all(|mapping| mapping.random), "{mappings:?}");
        let mapped: u64 = mappings.iter().map(|mapping| mapping.end - mapping.start).sum();
        assert_eq!(mapped, u64::try_from(env.inner.mdbx.db().info()?.map_size())?);
        assert_eq!(env.inner.mdbx.advised_bytes(), mapped);

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(gauge_value(&snapshot, "tn_reth.mdbx_random_advice_bytes"), Some(mapped as f64));
        assert!(gauge_value(&snapshot, "tn_reth.mdbx_used_bytes").is_some_and(|used| used > 0.0));
        Ok(())
    }

    /// With the switch off nothing is advised: past the pages libmdbx advised itself, a re-opened
    /// map carries no `rr` and the gauge reads zero. An env with the switch on in the same process
    /// advises its own file only.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_mdbx_random_advice_off_and_on_in_one_process() -> eyre::Result<()> {
        use super::mdbx_advice::test_support::mappings_of;
        use metrics_util::debugging::DebuggingRecorder;

        let off_dir = TempDir::new()?;
        create_and_close(off_dir.path())?;
        let off_dat = off_dir.path().join("db").join("mdbx.dat");
        let on_dir = TempDir::new()?;
        let tm = TaskManager::new("two envs");

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let off = ::metrics::with_local_recorder(&recorder, || {
            env_with_advice(off_dir.path(), &tm, false)
        })?;
        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(gauge_value(&snapshot, "tn_reth.mdbx_random_advice_bytes"), Some(0.0));
        assert!(gauge_value(&snapshot, "tn_reth.mdbx_used_bytes").is_some_and(|used| used > 0.0));
        assert_eq!(off.inner.mdbx.advised_bytes(), 0);
        let off_mappings = mappings_of(&off_dat);
        assert!(off_mappings.iter().any(|mapping| !mapping.random), "{off_mappings:?}");

        let on = env_with_advice(on_dir.path(), &tm, true)?;
        let on_mappings = mappings_of(&on_dir.path().join("db").join("mdbx.dat"));
        assert!(on_mappings.iter().all(|mapping| mapping.random), "{on_mappings:?}");
        assert!(on.inner.mdbx.advised_bytes() > 0);
        assert_eq!(mappings_of(&off_dat), off_mappings, "the other env's map is untouched");
        Ok(())
    }

    /// Growth inside the upper limit adds no mapping: after the database grows by several steps,
    /// the advised mappings are the same and all still carry `rr`.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_mdbx_random_advice_holds_while_the_database_grows() -> eyre::Result<()> {
        use super::mdbx_advice::test_support::mappings_of;
        use reth_db::{tables, transaction::DbTxMut as _};
        use reth_provider::DBProvider as _;

        let dir = TempDir::new()?;
        create_and_close(dir.path())?;
        let tm = TaskManager::new("growth");
        let env = env_with_advice(dir.path(), &tm, true)?;
        let mdbx_dat = dir.path().join("db").join("mdbx.dat");
        let before = mappings_of(&mdbx_dat);
        assert!(!before.is_empty(), "the database is mapped");
        let file_before = std::fs::metadata(&mdbx_dat)?.len();
        let last_pgno_before = env.inner.mdbx.db().info()?.last_pgno();

        // 16 MiB of bytecode against a 4 MiB growth step
        for round in 0..4u8 {
            let provider = env.blockchain_provider().database_provider_rw()?;
            for index in 0..256u16 {
                let mut key = [round; 32];
                key[30..].copy_from_slice(&index.to_be_bytes());
                let code = reth_primitives_traits::Bytecode::new_raw(vec![round; 16 * 1024].into());
                provider.tx_ref().put::<tables::Bytecodes>(key.into(), code)?;
            }
            provider.commit()?;
        }

        assert!(std::fs::metadata(&mdbx_dat)?.len() >= file_before + 8 * 1024 * 1024);
        assert!(env.inner.mdbx.db().info()?.last_pgno() > last_pgno_before);
        let after = mappings_of(&mdbx_dat);
        assert_eq!(after, before, "growth inside the limit adds no mapping");
        assert!(after.iter().all(|mapping| mapping.random), "{after:?}");
        Ok(())
    }

    /// A persist that writes a block reports non-zero page-operation deltas and the storage
    /// values after its commit; an empty persist reports zero deltas.
    #[tokio::test]
    async fn test_persisted_output_reports_the_storage_deltas() -> eyre::Result<()> {
        use super::mdbx_advice::test_support::{number, persisted_output_lines};

        const PAGE_FIELDS: [&str; 7] = [
            "page_newly",
            "page_cow",
            "page_spill",
            "page_unspill",
            "page_prefault",
            "page_wops",
            "page_msync",
        ];
        let genesis = test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp = TempDir::new()?;
        let tm = TaskManager::new("persist counters");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None)?;
        let output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
        let no_txs: Vec<Vec<u8>> = Vec::new();
        let block =
            env.build_block_from_batch_payload(payload, &no_txs, &mut OutputTrieOverlay::new())?;

        let (persisted, lines) = persisted_output_lines(|| env.persist_executed_output(&[block]));
        persisted?;
        let [line] = lines.as_slice() else { panic!("one persisted output line: {lines:?}") };
        for field in PAGE_FIELDS
            .iter()
            .chain(&["mdbx_used_bytes", "mdbx_advised_bytes", "mdbx_last_pgno"])
            .chain(&["gc_entries", "gc_large_pages"])
        {
            assert!(number(line, field).is_some(), "{field} missing: {line:?}");
        }
        let dirtied = number(line, "page_newly").unwrap_or_default()
            + number(line, "page_cow").unwrap_or_default();
        assert!(dirtied > 0, "the block dirtied pages: {line:?}");
        assert_eq!(number(line, "mdbx_advised_bytes"), Some(0), "the switch is off");
        let page_size = u64::from(env.inner.mdbx.db().stat()?.page_size());
        assert_eq!(
            number(line, "mdbx_used_bytes"),
            number(line, "mdbx_last_pgno").map(|last| (last + 1) * page_size)
        );
        if cfg!(target_os = "linux") {
            assert!(number(line, "proc_write_bytes").is_some(), "{line:?}");
        }

        let (persisted, lines) = persisted_output_lines(|| env.persist_executed_output(&[]));
        persisted?;
        let [line] = lines.as_slice() else { panic!("one persisted output line: {lines:?}") };
        for field in PAGE_FIELDS {
            assert_eq!(number(line, field), Some(0), "{field} after an empty persist: {line:?}");
        }
        assert!(number(line, "prefault_wchar_bytes").is_none_or(|bytes| bytes == 0), "{line:?}");
        Ok(())
    }
}
