//! Configure reth environment through CLI.
//!
//! This is the only reth CLI/config surface TN exposes: [`RethCommand`] narrows reth's full
//! argument set to RPC, transaction-pool, and database args, and [`RethConfig::new`] fills in
//! the rest of reth's `NodeConfig` with pinned values — reth networking/discovery fully
//! disabled (TN runs its own libp2p stack), no reth payload builder, no dev/debug options, and
//! reth's metrics server never launched (the prometheus endpoint is owned by `tn-metrics`).
//!
//! Security/operational policies enforced here:
//! - RPC namespace allowlist: only the modules in `ALL_MODULES` (eth, net, web3, debug, trace, rpc)
//!   can be enabled. `admin` and `txpool` are stripped from any selection with a warning, and a
//!   `--http.api all` style selection is rewritten to the allowlist (see
//!   `RethConfig::validate_rpc_modules`).
//! - Pruning is disabled: every `PruningArgs` field is off, so nodes keep full history (archive
//!   mode). Other code relies on this — e.g. ExEx replay treats missing receipts for an existing
//!   block as database corruption (`TnRethError::ReplayReceiptsMissing`), which is only sound
//!   because receipts are never pruned, and every pinned consensus-registry read resolves its state
//!   against fully indexed history. [`RethConfig::ensure_archive_mode`] enforces this at startup
//!   rather than leaving it to the defaults above, and `etc/archive-mode-guard.sh` fails the build
//!   if a pruner entry point is introduced.
//! - Reth's process-global defaults are seeded with TN's values via [`init_reth_defaults`], which
//!   must run before any [`TxPoolArgs`] or reth `RpcServerArgs` is parsed or default-constructed:
//!   the per-sender transaction-pool slot default is raised to
//!   [`TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER`] (256, vs reth's 16), and the default IPC endpoint
//!   becomes TN's [`DEFAULT_IPC_ENDPOINT`] (`tn.ipc`) rather than reth's `reth.ipc`.

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr},
    path::Path,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use reth::{
    args::{
        DatabaseArgs, DebugArgs, DefaultRpcServerArgs, DefaultTxPoolValues, DevArgs, DiscoveryArgs,
        EngineArgs, EraArgs, EraSourceArgs, MetricArgs, NetworkArgs, PayloadBuilderArgs,
        PruningArgs, StaticFilesArgs, StorageArgs, TxPoolArgs,
    },
    network::transactions::{
        config::TransactionPropagationKind, TransactionIngressPolicy, TransactionPropagationMode,
    },
    rpc::builder::{RethRpcModule, RpcModuleSelection},
};
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_discv4::NatResolver;
use reth_node_builder::{
    NodeConfig, DEFAULT_MEMORY_BLOCK_BUFFER_TARGET, DEFAULT_PERSISTENCE_THRESHOLD,
    DEFAULT_RESERVED_CPU_CORES, DEFAULT_SPARSE_TRIE_MAX_STORAGE_TRIES,
    DEFAULT_SPARSE_TRIE_PRUNE_DEPTH,
};
use reth_node_core::node_config::DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB;
use tn_types::ETHEREUM_BLOCK_GAS_LIMIT_30M;
use tracing::warn;

use crate::{
    dirs::path_to_datadir,
    rpc_server_args::{RpcServerArgs, DEFAULT_IPC_ENDPOINT},
};

/// A wrapper abstraction around a Reth node config.
#[derive(Clone, Debug)]
pub struct RethConfig(pub(crate) NodeConfig<RethChainSpec>);

/// Reth specific command line args.
#[derive(Debug, Parser, Clone)]
pub struct RethCommand {
    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,
}

const DEFAULT_UNUSED_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

/// All the rpc modules we allow.
/// Disallow admin, txpool.
const ALL_MODULES: [RethRpcModule; 6] = [
    RethRpcModule::Eth,
    RethRpcModule::Net,
    RethRpcModule::Web3,
    RethRpcModule::Debug,
    RethRpcModule::Trace,
    RethRpcModule::Rpc,
];

/// Telcoin Network's per-sender transaction pool slot default.
///
/// Reth's own default is 16 (`TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER`). The limit is an
/// admission-time anti-spam gate: once a sender already holds this many slots pool-wide, reth
/// rejects further externally-submitted transactions from it. TN raises the default because
/// its expected traffic is dominated by a small number of high-volume senders.
///
/// This is a default, not a floor. Operators override it with `--txpool.max-account-slots`,
/// including back down to reth's 16.
pub const TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER: usize = 256;

/// Seed reth's process-global defaults with TN's values.
///
/// Must run before any [`TxPoolArgs`] or reth `RpcServerArgs` is parsed or default-constructed.
/// Reth resolves those defaults out of process-wide `OnceLock`s, and the first read of each lock
/// fixes it for the life of the process, so a late call cannot take effect. The clap defaults and
/// the `Default` impls both read the locks.
///
/// Transaction pool: the per-sender slot default becomes
/// [`TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER`] (256, vs reth's 16). Seeding is what makes an
/// explicit `--txpool.max-account-slots 16` reachable: the value clap injects when the flag is
/// absent becomes 256, so an operator-supplied 16 is no longer indistinguishable from an unset
/// flag and needs no sentinel to detect.
///
/// RPC server: the default `ipcpath` becomes TN's [`DEFAULT_IPC_ENDPOINT`] (`tn.ipc`, vs reth's
/// `reth.ipc`). TN's own [`RpcServerArgs`] already carries that default through CLI parsing; the
/// seed covers the construction paths that never parse. Without it, `NodeConfig::default()` fills
/// a temp chain's config with reth's `ipcpath`, and every test env binds a socket at the fixed
/// global path `/tmp/reth.ipc` (issue #1165). TN's `From` impl also fills reth-side fields with
/// `..Default::default()`, which reads the same lock.
///
/// Idempotent, and safe to call from more than one entry point. A call that loses a race, or
/// that arrives after some other code has already read a lock, warns only when an effective
/// default actually differs from TN's, so repeat calls are silent.
pub fn init_reth_defaults() {
    if DefaultTxPoolValues::default()
        .with_max_account_slots(TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER)
        .try_init()
        .is_err()
    {
        // The lock is already taken, so reading it back cannot change the outcome.
        let effective = TxPoolArgs::default().max_account_slots;
        if effective != TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER {
            warn!(
                target: "tn::reth",
                effective,
                expected = TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER,
                "reth transaction pool defaults already initialized; TN per-sender slot default not applied"
            );
        }
    }
    if DefaultRpcServerArgs::default()
        .with_ipcpath(DEFAULT_IPC_ENDPOINT.to_string())
        .try_init()
        .is_err()
    {
        // The lock is already taken, so reading it back cannot change the outcome.
        let effective = reth::args::RpcServerArgs::default().ipcpath;
        if effective != DEFAULT_IPC_ENDPOINT {
            warn!(
                target: "tn::reth",
                effective = %effective,
                expected = DEFAULT_IPC_ENDPOINT,
                "reth RPC server defaults already initialized; TN IPC endpoint default not applied"
            );
        }
    }
}

impl RethConfig {
    /// Make sure that some modules are not selected, primarily they won't work as expected with TN
    /// (or at all).
    fn validate_rpc_modules(mods: &mut Option<RpcModuleSelection>) {
        match &mods {
            Some(RpcModuleSelection::All) => {
                *mods = Some(RpcModuleSelection::Selection(HashSet::from(ALL_MODULES)));
            }
            Some(RpcModuleSelection::Standard) => {}
            Some(RpcModuleSelection::Selection(hash_set)) => {
                let mut new_set = HashSet::new();
                for r in ALL_MODULES {
                    if hash_set.contains(&r) {
                        new_set.insert(r);
                    }
                }
                if hash_set.contains(&RethRpcModule::Admin) {
                    warn!(target: "tn::reth", "Attempted to configure unsupported admin RPC module!");
                }
                if hash_set.contains(&RethRpcModule::Txpool) {
                    warn!(target: "tn::reth", "Attempted to configure unsupported txpool RPC module!");
                }
                *mods = Some(RpcModuleSelection::Selection(new_set));
            }
            None => {}
        }
    }

    /// Create a new RethConfig wrapper.
    pub fn new<P: AsRef<Path>>(
        reth_config: RethCommand,
        instance: Option<u16>,
        datadir: P,
        with_unused_ports: bool,
        chain: Arc<RethChainSpec>,
    ) -> Self {
        // create a reth DatadirArgs from tn datadir
        let datadir = path_to_datadir(datadir.as_ref());
        // TN's per-sender slot default is seeded into reth's global pool defaults by
        // `init_reth_defaults` before parsing, so `txpool` already carries either that default
        // or the operator's explicit value. Nothing to override here.
        let RethCommand { mut rpc, txpool, db } = reth_config;

        Self::validate_rpc_modules(&mut rpc.http_api);
        Self::validate_rpc_modules(&mut rpc.ws_api);
        // We don't just use Default for these Reth args.
        // This will force us to look at new options and make sure they are good for our use.
        // We DO NOT use the Reth networking so these settings should reflect that.
        let network = NetworkArgs {
            discovery: DiscoveryArgs {
                disable_discovery: true,
                disable_nat: true,
                disable_dns_discovery: true,
                disable_discv4_discovery: true,
                enable_discv5_discovery: false,
                addr: DEFAULT_UNUSED_ADDR,
                port: 0,
                discv5_addr: None,
                discv5_addr_ipv6: None,
                discv5_port: 0,
                discv5_port_ipv6: 0,
                discv5_lookup_interval: 0,
                discv5_bootstrap_lookup_interval: 0,
                discv5_bootstrap_lookup_countdown: 0,
            },
            trusted_only: false,
            trusted_peers: vec![],
            bootnodes: None,
            dns_retries: 0,
            peers_file: None,
            identity: "Reth Null Network".to_string(),
            p2p_secret_key: None,
            no_persist_peers: true,
            nat: NatResolver::None,
            addr: DEFAULT_UNUSED_ADDR,
            port: 0,
            max_outbound_peers: None,
            max_inbound_peers: None,
            max_concurrent_tx_requests: 0,
            max_concurrent_tx_requests_per_peer: 0,
            max_seen_tx_history: 0,
            max_pending_pool_imports: 0,
            soft_limit_byte_size_pooled_transactions_response: 0,
            soft_limit_byte_size_pooled_transactions_response_on_pack_request: 0,
            max_capacity_cache_txns_pending_fetch: 0,
            net_if: None,
            tx_propagation_policy: TransactionPropagationKind::None,
            tx_ingress_policy: TransactionIngressPolicy::None,
            disable_tx_gossip: true,
            propagation_mode: TransactionPropagationMode::Max(0),
            required_block_hashes: vec![],
            network_id: None,
            enforce_enr_fork_id: false,
            max_peers: None,
            netrestrict: None,
            p2p_secret_key_hex: None,
        };

        // Not using the Reth payload builder.
        let builder = PayloadBuilderArgs {
            extra_data: "tn-reth-na".to_string(),
            gas_limit: Some(ETHEREUM_BLOCK_GAS_LIMIT_30M),
            interval: Duration::from_secs(1),
            deadline: Duration::from_secs(1),
            max_payload_tasks: 0,
            max_blobs_per_block: None,
        };
        let debug = DebugArgs {
            terminate: false,
            tip: None,
            max_block: None,
            etherscan: None,
            rpc_consensus_url: None,
            skip_fcu: None,
            skip_new_payload: None,
            reorg_frequency: None,
            reorg_depth: None,
            engine_api_store: None,
            invalid_block_hook: None,
            healthy_node_rpc_url: None,
            ethstats: None,
            startup_sync_state_idle: false,
        };
        // No Reth dev options.
        let dev = DevArgs {
            dev: false,
            block_max_transactions: None,
            block_time: None,
            dev_mnemonic: Default::default(),
        };
        // Ignore Reth pruning for now.
        let pruning = PruningArgs {
            full: false,
            minimal: false,
            block_interval: None,
            sender_recovery_full: false,
            sender_recovery_distance: None,
            sender_recovery_before: None,
            transaction_lookup_full: false,
            transaction_lookup_distance: None,
            transaction_lookup_before: None,
            receipts_full: false,
            receipts_distance: None,
            receipts_before: None,
            account_history_full: false,
            account_history_distance: None,
            account_history_before: None,
            storage_history_full: false,
            storage_history_distance: None,
            storage_history_before: None,
            receipts_log_filter: None,
            receipts_pre_merge: false,
            bodies_pre_merge: false,
            bodies_distance: None,
            bodies_before: None,
        };

        // Parameters for configuring the engine driver.
        #[allow(deprecated)] // deprecated fields
        let engine = EngineArgs {
            persistence_threshold: DEFAULT_PERSISTENCE_THRESHOLD,
            memory_block_buffer_target: DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
            legacy_state_root_task_enabled: false,
            state_root_task_compare_updates: false,
            state_cache_disabled: false,
            prewarming_disabled: false,
            state_provider_metrics: false,
            cross_block_cache_size: DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB,
            accept_execution_requests_hash: false,
            multiproof_chunking_enabled: false,
            multiproof_chunk_size: 0,
            reserved_cpu_cores: DEFAULT_RESERVED_CPU_CORES,
            state_root_fallback: false,
            precompile_cache_disabled: false,
            always_process_payload_attributes_on_canonical_head: false,
            allow_unwind_canonical_header: false,
            storage_worker_count: None,
            account_worker_count: None,
            disable_proof_v2: false,
            cache_metrics_disabled: false,
            disable_trie_cache: false,
            sparse_trie_prune_depth: DEFAULT_SPARSE_TRIE_PRUNE_DEPTH,
            sparse_trie_max_storage_tries: DEFAULT_SPARSE_TRIE_MAX_STORAGE_TRIES,
            disable_sparse_trie_cache_pruning: false,
            state_root_task_timeout: None,
            // deprecated fields
            caching_and_prewarming_enabled: true,
            parallel_sparse_trie_enabled: false,
            parallel_sparse_trie_disabled: true,
            precompile_cache_enabled: true,
        };

        // metrics args
        //
        // NOTE: dead config in TN's hand-rolled init path - the node never launches reth's
        // metric server. The prometheus endpoint is owned by `tn-metrics` (`--metrics` CLI arg).
        let metrics = MetricArgs {
            prometheus: None,
            push_gateway_url: None,
            push_gateway_interval: Duration::from_mins(5), // reth default
        };

        // static files
        let static_files = StaticFilesArgs {
            blocks_per_file_headers: None,
            blocks_per_file_transactions: None,
            blocks_per_file_receipts: None,
            blocks_per_file_transaction_senders: None,
            blocks_per_file_account_change_sets: None,
            blocks_per_file_storage_change_sets: None,
        };

        // storage
        let storage = StorageArgs { v2: false };

        // Parameters to configure block history syncing.
        let era = EraArgs { enabled: false, source: EraSourceArgs { path: None, url: None } };

        let mut this = NodeConfig {
            config: None,
            chain,
            metrics,
            instance,
            datadir,
            network,
            rpc: rpc.into(),
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            static_files,
            storage,
        };
        if with_unused_ports {
            this = this.with_unused_ports();
        }
        // adjust rpc instance ports
        this.adjust_instance_ports();

        Self(this)
    }

    /// Assert the archive-mode invariant that every pinned historical read depends on.
    ///
    /// Pinned reads (committee membership, epoch records, epoch state, worker fee configs, and
    /// the general pinned contract read) all resolve their state through
    /// `RethEnv::read_only_state_db` — the epoch reads via its `pinned_state_and_env` wrapper,
    /// `read_contract_inner` directly — and reth only serves that state from the requested block
    /// while the history indices covering it are intact.
    ///
    /// Reth catches part of the pruned case loudly: for a pinned block strictly BELOW the
    /// recorded watermark, `HistoricalStateProviderRef` returns
    /// `ProviderError::StateAtBlockPruned` rather than serving the wrong state. That failure is
    /// diagnosable, but it lands in the middle of a consensus read (committee construction, epoch
    /// entry, base-fee seeding), where the reachable outcomes are a stalled epoch transition or a
    /// halted node. Refusing to start is the same fact delivered at the one moment an operator
    /// can still act on it.
    ///
    /// The rest of the pruned case is silent, which is the stronger reason to refuse at startup:
    /// at or above the watermark, a key whose own history shards were pruned away classifies as
    /// `HistoryInfo::MaybeInPlainState` and the read serves PLAIN (tip) state with no error. That
    /// arm is reached only when a prune checkpoint exists, so pruning is the only thing that
    /// substitutes tip state for pinned state.
    ///
    /// Note what this does NOT protect against, since the distinction is easy to lose. Those
    /// watermarks come from the prune checkpoints, so history missing WITHOUT one (an externally
    /// truncated datadir, a snapshot-restored node with no pre-snapshot history) leaves them
    /// unset, and reth neither errors nor falls back to tip: the lookup classifies as
    /// `HistoryInfo::NotYetWritten` and returns `Ok(None)`, so the account or slot reads as never
    /// written. That is a datadir property rather than a configuration one, and this check cannot
    /// see it. For the snapshot-restore half the node now refuses the read instead:
    /// `pinned_state_and_env` rejects any pin below the restored-state floor the restorer
    /// persists (#1131). An externally truncated datadir remains invisible to both. See
    /// `pinned_state_and_env` in `env/epoch.rs` for the full breakdown.
    ///
    /// The check is deliberately conservative. `prune_config` returns `None` exactly when no
    /// pruning was requested, so any `Some` fails here: a per-segment mode, `--full`,
    /// `--minimal`, or a non-default block interval. A configuration that happens to leave
    /// account and storage history intact still fails, because deciding which segments a pinned
    /// read can survive is a review decision, not a startup one.
    ///
    /// Two limits are worth stating. This reads the configuration THIS node was built with, so
    /// it cannot detect a datadir some other binary already pruned; and `prune_config` reflects
    /// only the CLI-derived [`PruningArgs`], not a reth.toml `[prune]` section (TN never loads
    /// one, since `NodeConfig::config` stays `None` and the node hand-rolls its init path rather
    /// than using reth's launcher). The reth.toml residual is covered by
    /// `etc/archive-mode-guard.sh`, which fails the build if a pruner entry point or a reth.toml
    /// load is introduced; the foreign-datadir residual is beyond any check this repo can run.
    pub(crate) fn ensure_archive_mode(&self) -> eyre::Result<()> {
        self.0.prune_config().map_or(Ok(()), |prune_config| {
            Err(eyre::eyre!(
                "pruning is configured ({prune_config:?}) but pinned consensus-registry reads \
                 require archive mode: once history is pruned, reth fails a pinned read with \
                 StateAtBlockPruned, so committee, epoch-record, and epoch-state reads would \
                 break mid-consensus instead of here. Refusing to start"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth::rpc::builder::RpcModuleSelection;
    use tempfile::TempDir;
    use tn_types::test_genesis;

    /// reth's own per-sender slot default, spelled out rather than imported.
    ///
    /// hard-coding it keeps the assertion below honest if tn's default is ever changed to
    /// coincide with reth's, which is the condition that made 16 unreachable in the first place.
    const RETH_MAX_ACCOUNT_SLOTS_PER_SENDER: usize = 16;

    #[test]
    fn test_rpc_validator() {
        let mut mods: Option<RpcModuleSelection> = None;
        RethConfig::validate_rpc_modules(&mut mods);
        assert!(mods.is_none());
        let mut mods = Some(RpcModuleSelection::All);
        RethConfig::validate_rpc_modules(&mut mods);
        if let Some(RpcModuleSelection::Selection(mods)) = &mut mods {
            for r in ALL_MODULES {
                assert!(mods.remove(&r));
            }
        };
    }

    /// An operator-supplied per-sender slot limit must reach the node config untouched.
    ///
    /// [`RethConfig::new`] used to raise reth's default to TN's by sentinel equality against
    /// reth's constant, so an explicit `--txpool.max-account-slots 16` was indistinguishable from
    /// an absent flag and was silently rewritten to 256. The default now arrives through
    /// [`init_reth_defaults`], leaving nothing for this function to override. Restoring that
    /// guard fails this test.
    ///
    /// Only the explicit case is asserted here. What an *absent* flag resolves to depends on
    /// which test in this binary reached reth's process-wide `OnceLock` first, so it is pinned in
    /// the `telcoin-network-cli` integration test that owns its process instead.
    #[test]
    fn explicit_max_account_slots_survives_reth_config_new() {
        assert_ne!(TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER, RETH_MAX_ACCOUNT_SLOTS_PER_SENDER);

        let tmp_dir = TempDir::new().expect("tempdir created");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_command = RethCommand::parse_from([
            "tn",
            "--txpool.max-account-slots",
            &RETH_MAX_ACCOUNT_SLOTS_PER_SENDER.to_string(),
        ]);

        let config = RethConfig::new(reth_command, None, tmp_dir.path(), true, chain);

        assert_eq!(config.0.txpool.max_account_slots, RETH_MAX_ACCOUNT_SLOTS_PER_SENDER);
    }

    /// The `--instance` port offsets the CLI README documents, checked against the values reth's
    /// `adjust_instance_ports` resolves from TN's defaults: instance 1 keeps the defaults, each
    /// further instance moves HTTP down by one and WebSocket up by two, and the IPC suffix is
    /// appended to the configured path (`/tmp/tn.ipc-2`, not `/tmp/tn-2.ipc`).
    ///
    /// The CLI rejects instance 0 at parse time (`telcoin-network-cli`), so the smallest value
    /// it can hand this function is 1 and the `instance - 1` below never underflows on that path.
    #[test]
    fn instance_port_offsets_match_documented_table() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let resolve = |instance: u16| -> eyre::Result<(u16, u16, String)> {
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let reth_command = RethCommand::try_parse_from(["tn-reth"])?;
            let config =
                RethConfig::new(reth_command, Some(instance), tmp_dir.path(), false, chain);
            Ok((config.0.rpc.http_port, config.0.rpc.ws_port, config.0.rpc.ipcpath))
        };

        assert_eq!(resolve(1)?, (8545, 8546, format!("{DEFAULT_IPC_ENDPOINT}-1")));
        assert_eq!(resolve(2)?, (8544, 8548, format!("{DEFAULT_IPC_ENDPOINT}-2")));
        assert_eq!(resolve(3)?, (8543, 8550, format!("{DEFAULT_IPC_ENDPOINT}-3")));
        Ok(())
    }

    /// Build a config the way `new_for_temp_chain` does, then hand its [`PruningArgs`] to `enable`
    /// so a test can turn on exactly one prune knob.
    ///
    /// Seeds the pool defaults first for the same reason `new_for_temp_chain` does: reaching
    /// `NodeConfig::default` locks reth's process-wide per-sender slot default, and these tests
    /// should not be the thing that pins it at reth's 16 for the rest of the binary.
    fn config_with_pruning(enable: impl FnOnce(&mut PruningArgs)) -> RethConfig {
        init_reth_defaults();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut config = RethConfig(NodeConfig { chain, ..NodeConfig::default() });
        enable(&mut config.0.pruning);
        config
    }

    /// The config the node actually ships must satisfy the archive-mode invariant, or every node
    /// would refuse to start.
    #[test]
    fn test_archive_mode_accepts_production_config() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let reth_command = RethCommand::try_parse_from(["tn-reth"])?;
        RethConfig::new(reth_command, None, tmp_dir.path(), true, chain).ensure_archive_mode()
    }

    /// `new_for_temp_chain` fills its config from `NodeConfig::default()` rather than
    /// [`RethConfig::new`], so that second construction path needs its own coverage: every test env
    /// in the workspace is built through it and passes the same startup guard.
    #[test]
    fn test_archive_mode_accepts_default_node_config() -> eyre::Result<()> {
        config_with_pruning(|_| {}).ensure_archive_mode()
    }

    /// Pruning account history is the knob that directly unpins a pinned read: without the
    /// account-history shards, reth stops resolving the pinned block from its own state.
    #[test]
    fn test_archive_mode_rejects_account_history_pruning() {
        let err = config_with_pruning(|pruning| pruning.account_history_distance = Some(128))
            .ensure_archive_mode()
            .expect_err("account-history pruning must be rejected");
        assert!(err.to_string().contains("archive mode"), "unexpected error: {err}");
    }

    /// Storage history carries the `ConsensusRegistry` slots the committee is decoded from, so
    /// pruning it unpins the registry reads specifically.
    #[test]
    fn test_archive_mode_rejects_storage_history_pruning() {
        let err = config_with_pruning(|pruning| pruning.storage_history_distance = Some(128))
            .ensure_archive_mode()
            .expect_err("storage-history pruning must be rejected");
        assert!(err.to_string().contains("archive mode"), "unexpected error: {err}");
    }

    /// `--full` derives a whole prune-mode set from the chain spec rather than setting a single
    /// segment, so it exercises the composite path through `PruningArgs::prune_config`.
    #[test]
    fn test_archive_mode_rejects_full_node_pruning() {
        let err = config_with_pruning(|pruning| pruning.full = true)
            .ensure_archive_mode()
            .expect_err("full-node pruning must be rejected");
        assert!(err.to_string().contains("archive mode"), "unexpected error: {err}");
    }
}
