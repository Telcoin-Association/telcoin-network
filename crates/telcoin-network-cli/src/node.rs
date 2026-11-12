//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{
    builder::{BoolishValueParser, PathBufValueParser, PossibleValue, TypedValueParser},
    value_parser, Parser,
};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    thread::available_parallelism,
};
use tn_config::{Config, KeyConfig, PidLock, TelcoinDirs, BLS_KEYFILE};
use tn_node::{engine::TnBuilder, HealthcheckBind};
use tn_reth::{parse_socket_address, RethChainSpec, RethCommand, RethConfig, FAUCET_ENABLED};
use tn_types::{
    BlsPublicKey, BootstrapServer, Genesis, ADIRI_CHAIN_ID, B256, MAINNET_CHAIN_ID, MAINNET_GENESIS,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Parse bootstrap dial hints using the same map and legacy worker decoding as NetworkConfig.
fn parse_bootstrap_peers(
    raw: &str,
) -> Result<BTreeMap<BlsPublicKey, BootstrapServer>, serde_yaml::Error> {
    serde_yaml::from_str(raw)
}

/// Read a bootstrap dial hints file and parse it with [`parse_bootstrap_peers`].
///
/// Takes the path from [`PathBufValueParser`], so a path that is not valid UTF-8 reaches the
/// filesystem instead of failing clap's UTF-8 check.
fn parse_bootstrap_peers_file(
    path: PathBuf,
) -> Result<BTreeMap<BlsPublicKey, BootstrapServer>, String> {
    let raw = std::fs::read_to_string(path).map_err(|err| format!("cannot read file: {err}"))?;
    parse_bootstrap_peers(&raw).map_err(|err| err.to_string())
}

/// Avaliable "named" chains.
/// These will have embedded config files and can be joined after gereating keys.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum NamedChain {
    /// The adiri testnet (alias of `test-net`).
    Adiri,
    /// The adiri testnet.
    TestNet,
    /// Mainnet.
    Mainnet,
}

impl NamedChain {
    /// Load this chain's bundled config. Writes nothing to the datadir.
    pub(crate) fn load_config<P: TelcoinDirs>(
        self,
        tn_datadir: &P,
        version: &'static str,
    ) -> eyre::Result<Config> {
        match self {
            Self::Adiri | Self::TestNet => Config::load_adiri(tn_datadir, version),
            Self::Mainnet => Config::load_mainnet(tn_datadir, version),
        }
    }

    /// Write this chain's bundled genesis committee to the datadir unless one is already there.
    ///
    /// Callers run this after their startup checks pass, so a refused start leaves the datadir
    /// as it was.
    pub(crate) fn write_committee<P: TelcoinDirs>(self, tn_datadir: &P) -> eyre::Result<()> {
        match self {
            Self::Adiri | Self::TestNet => Config::write_adiri_committee(tn_datadir),
            Self::Mainnet => Config::write_mainnet_committee(tn_datadir),
        }
    }
}

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Join a named telcoin network (for instance test or main net).
    #[arg(long, value_name = "NAMED_TN_NETWORK", verbatim_doc_comment)]
    pub chain: Option<NamedChain>,

    /// Bootstrap dial hints as a YAML or JSON map keyed by BLS public key.
    ///
    /// Replaces network-config bootstrap_peers for this process only. A nonempty map replaces
    /// all genesis seeds; an explicit '{}' selects the genesis fallback. Each entry contains
    /// primary and workers (a list). Committee membership still comes from chain state.
    #[arg(long, value_name = "MAP", value_parser = parse_bootstrap_peers, allow_hyphen_values = true)]
    bootstrap_peers: Option<BTreeMap<BlsPublicKey, BootstrapServer>>,

    /// Read the bootstrap dial hints map from a file instead of the command line.
    ///
    /// The file holds the same YAML or JSON map as --bootstrap-peers, with the same meaning.
    /// It is read and checked while arguments are parsed, so a missing or invalid file stops
    /// the node before startup.
    #[arg(
        long,
        value_name = "PATH",
        value_parser = PathBufValueParser::new().try_map(parse_bootstrap_peers_file),
        conflicts_with = "bootstrap_peers"
    )]
    bootstrap_peers_file: Option<BTreeMap<BlsPublicKey, BootstrapServer>>,

    /// Enable Prometheus consensus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Consensus Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Instance numbers run from 1 to 200. The max is chosen in a way so that it's not possible
    /// to have port numbers that conflict with each other. 0 is rejected at parse time: the port
    /// arithmetic below subtracts `instance - 1`, which underflows on 0.
    ///
    /// Changes to the following port numbers:
    /// - `HTTP_RPC_PORT`: default - `instance` + 1
    /// - `WS_RPC_PORT`: default + `instance` * 2 - 2
    /// - `IPC_PATH`: default + `-instance`
    #[arg(long, value_name = "INSTANCE", global = true,  value_parser = value_parser!(u16).range(1..=200))]
    pub instance: Option<u16>,

    /// Export each epoch's final execution state to a snapshot pack under
    /// `consensus-db/state_exports/epoch-{N}/`.
    ///
    /// Each epoch writes a full execution-state copy. Retention is unlimited by default and can
    /// fill the data volume; use --state-export-keep N to limit the number of completed bundles.
    #[arg(long, global = true, default_value_t = false)]
    pub enable_state_export: bool,

    /// Keep the newest N completed state-export bundles (N must be at least 1).
    ///
    /// Only applies with --enable-state-export. Unset keeps all bundles. Each epoch exports the
    /// full execution state, so unlimited retention can fill the data volume. This limits bundle
    /// count, not bytes. Use N >= 2 to retain a previous bundle during overlapping exports.
    #[arg(long, global = true, value_name = "N")]
    state_export_keep: Option<NonZeroUsize>,

    /// Watch executed batches for cross-producer transaction re-packing (issue #1259).
    ///
    /// Opt-in telemetry: when set, execution keeps a bounded rolling window of recently
    /// executed transaction hashes and reports (a counter and a rate-bounded warning) batches
    /// that re-pack transactions first packed by another producer's batch. A node that does
    /// not opt in builds no window and hashes nothing.
    #[arg(long, global = true, default_value_t = false)]
    pub enable_repack_monitor: bool,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    /// Additional reth arguments
    #[clap(flatten)]
    pub reth: RethCommand,

    /// Healthcheck listener bind: a port or a socket address.
    ///
    /// A bare port (`8080`, the pre-#1689 form) binds `127.0.0.1:8080`. A socket address binds
    /// exactly that interface: `[::1]:8080`, `10.10.0.21:8080`, or `0.0.0.0:8080` to listen on
    /// every interface (write the wildcard out; it is never implied). The listener serves
    /// liveness, `/health/workers` and `/health/network`. Unset starts no listener. A bind
    /// failure stops startup; an unparsable value is rejected here.
    ///
    /// Each connection is handled synchronously in the accept loop with a read timeout; no
    /// connection limits or rate limiting, so keep the bind private.
    #[arg(long, value_name = "PORT|ADDR", global = true, env = "HEALTHCHECK_TCP_PORT")]
    pub healthcheck: Option<HealthcheckBind>,

    /// Assign this name to node.  Currently used to name it's opentracing service.
    #[arg(long, value_name = "NODE_NAME", global = true)]
    pub node_name: Option<String>,

    /// URL of an opentracing service (like jaeger) to send tracing data to (for example http://192.168.1.2:4317).
    #[arg(long, value_name = "URL", global = true, env = "TN_TRACING_URL")]
    pub tracing_url: Option<String>,

    /// Let a faucet build start on the loaded chain. Test networks only.
    ///
    /// A binary built with the `faucet` feature starts on the loaded chain only with this set,
    /// except on adiri (chain id 2017), which needs no flag. Mainnet (chain id 731) and the
    /// embedded mainnet genesis are refused with or without it.
    ///
    /// The environment variable accepts 1/0, true/false, yes/no, on/off, t/f and y/n in any case;
    /// an empty value leaves the flag off and any other value is an argument error.
    #[arg(
        long,
        env = "TN_ALLOW_FAUCET_ON_CHAIN",
        value_parser = OptInValueParser::default(),
        hide_short_help = true
    )]
    pub allow_faucet_on_chain: bool,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext,
}

impl<Ext: clap::Args + fmt::Debug> NodeCommand<Ext> {
    /// Execute `node` command
    pub fn execute<L>(
        mut self,
        tn_datadir: PathBuf,
        key_config: KeyConfig,
        launcher: L,
    ) -> eyre::Result<JoinHandle<eyre::Result<()>>>
    where
        L: FnOnce(TnBuilder, Ext, PathBuf, KeyConfig, &'static str) -> JoinHandle<eyre::Result<()>>,
    {
        info!(target: "cli", "telcoin-network {} starting", SHORT_VERSION);

        // Log the compiled fork schedule once per process start (#1086) so operators can diff it
        // across the fleet before a fork epoch arrives; several fork constants document this log
        // as their only in-protocol detection for a mismatched binary. Every epoch-gated adiri
        // constant belongs in the adiri line, and every fork belongs in the non-adiri sentence
        // (there is no non-adiri PREVRANDAO constant to log: that build's gate is a hardcoded
        // `true`, active from genesis like the rest).
        #[cfg(feature = "adiri")]
        info!(
            target: "cli",
            consensus_registry_fork_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH,
            seed_signature_fork_epoch = tn_types::forks::SEED_SIGNATURE_FORK_EPOCH,
            multi_workers_fork_epoch = tn_types::forks::MULTI_WORKERS_FORK_EPOCH,
            prevrandao_fork_epoch = tn_types::forks::PREVRANDAO_FORK_EPOCH,
            leader_seeded_ordering_fork_epoch = tn_types::forks::LEADER_SEEDED_ORDERING_FORK_EPOCH,
            subsecond_timestamp_fork_epoch = tn_types::forks::SUBSECOND_TIMESTAMP_FORK_EPOCH,
            governance_safe_fork_epoch = tn_types::forks::GOVERNANCE_SAFE_FORK_EPOCH,
            "fork schedule (adiri)"
        );
        #[cfg(not(feature = "adiri"))]
        info!(
            target: "cli",
            "fork schedule: seed_signature, multi_workers, prevrandao, \
             leader_seeded_ordering, and subsecond_timestamp active from genesis"
        );

        // Both lines above report compiled fork points, which a `test-utils` binary does not have
        // to obey: the e2e harness spawns nodes with all but the leader-seeded and sub-second
        // forks pinned dormant (the sub-second gate stays dormant anyway while the seed-signature
        // pin is dormant), and they log "active from genesis" while executing the legacy
        // derivations. Name the pins
        // that are actually in force so the startup log stays diffable there too. Empty and
        // silent in a production binary, where the overrides are compiled out of `tn-types`
        // entirely.
        for (var, fork_epoch) in tn_types::forks::fork_epoch_overrides() {
            warn!(
                target: "cli",
                var,
                fork_epoch,
                "fork schedule OVERRIDDEN by the environment; this is a test-utils build"
            );
        }

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit()?;

        // Install the global metrics recorder before any reth components are constructed
        // (in particular before `RethEnv::new_database`). Reth's derive-style metric
        // handles bind to whatever recorder is installed at construction time; anything
        // registered against the default noop recorder is silently lost. Nodes without
        // `--metrics` keep the zero-overhead noop recorder.
        if self.metrics.is_some() {
            tn_metrics::install_recorder()?;
        }

        // limit global rayon thread pool for batch validator
        //
        // ensure 2 cores are reserved unless the system only has 1 core
        let num_parallel_threads =
            available_parallelism().map_or(0, |num| num.get().saturating_sub(2).max(1));
        if let Err(err) = ThreadPoolBuilder::new()
            .num_threads(num_parallel_threads)
            .thread_name(|i| format!("tn-rayon-{i}"))
            .build_global()
        {
            error!(target: "cli", "Error: Failed to initialize global thread pool for rayon: {err}");
        }

        // overwrite all genesis if `genesis` was passed to CLI
        let named_chain = self.chain.take();
        let tn_config = named_chain.map_or_else(
            || Config::load(&tn_datadir, SHORT_VERSION),
            |chain| {
                info!(target: "cli", "Overwriting TN config with named chain: {chain:?}");
                chain.load_config(&tn_datadir, SHORT_VERSION)
            },
        )?;
        let chain_id = tn_config.genesis().config.chain_id;
        // built once here and handed to reth below; the genesis hash comes from the same spec the
        // node boots with
        let chain_spec = Arc::new(tn_config.chain_spec());
        // one fixed-format line naming the compiled features and the loaded chain, so operators
        // and deployment checks can confirm both from the log before any guard below refuses
        info!(target: "cli", "{}", node_build_line(chain_id, chain_spec.genesis_hash()));
        #[cfg(not(feature = "adiri"))]
        if chain_id == ADIRI_CHAIN_ID {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must compile with adiri feature flag in order to connect to adiri (testnet)!"
            ));
        }
        #[cfg(feature = "adiri")]
        if chain_id != ADIRI_CHAIN_ID {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must NOT compile with adiri feature flag when connecting to non-adiri (testnet) networks!"
            ));
        }
        validate_bls_key_protection(chain_id, key_config.bls_key_is_cleartext())?;
        warn_stale_cleartext_bls_key(
            chain_id,
            key_config.bls_key_is_cleartext(),
            &tn_datadir.node_keys_path(),
        );
        // Runtime value, not a #[cfg], so the check holds no matter which crate in the
        // build graph enabled the faucet feature.
        validate_faucet_build(FAUCET_ENABLED, tn_config.genesis(), self.allow_faucet_on_chain)?;
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            chain: _, // Used above
            bootstrap_peers,
            bootstrap_peers_file,
            metrics,
            enable_state_export,
            state_export_keep,
            enable_repack_monitor,
            instance,
            with_unused_ports,
            reth,
            healthcheck,
            ext,
            node_name: _,
            tracing_url: _,
            allow_faucet_on_chain: _, // Used above
        } = self;

        check_instance_ports(instance, &reth, with_unused_ports)?;
        // set up reth node config for engine components
        let node_config =
            RethConfig::new(reth, instance, &tn_datadir, with_unused_ports, chain_spec);

        // Refuse a second node on this datadir before opening the execution database a live node
        // may hold; the node keeps the lock for its lifetime (handed over in the builder).
        let pid_lock = PidLock::acquire(&tn_datadir)?;
        // the bundled committee is written only once every startup check above has passed
        if let Some(chain) = named_chain {
            chain.write_committee(&tn_datadir)?;
        }
        // create dbs to survive between sync state transitions
        let reth_db = open_execution_database(&node_config, &tn_datadir)?;
        let mut builder = TnBuilder::new(node_config, tn_config, reth_db)
            .with_state_export_keep(state_export_keep)
            // the two flags conflict, so at most one of them is set
            .with_bootstrap_peers(bootstrap_peers.or(bootstrap_peers_file))
            .with_pid_lock(pid_lock);
        builder.metrics = metrics;
        builder.healthcheck = healthcheck;
        builder.enable_state_export = enable_state_export;
        builder.enable_repack_monitor = enable_repack_monitor;

        Ok(launcher(builder, ext, tn_datadir, key_config, SHORT_VERSION))
    }
}

/// Open the node's reth execution database in `tn_datadir`, after refusing an unfinished
/// `db reset-execution` there: one stopped between its moves leaves part of the execution database
/// in its staging directory, and the open would create a database beside the rest.
fn open_execution_database(
    node_config: &RethConfig,
    tn_datadir: &Path,
) -> eyre::Result<tn_reth::RethDb> {
    crate::db::refuse_interrupted_reset(tn_datadir)?;
    tn_reth::RethEnv::new_database(node_config, tn_datadir.to_path_buf().reth_db_path())
}

/// Value parser for an opt-in flag that can also be set from an environment variable.
///
/// An empty value reads as unset (`false`), so a variable set to the empty string (`VAR=` in an
/// env file, or a `${VAR:-}` expansion) leaves the opt-in off instead of stopping the command.
/// Any other value parses like clap's [`BoolishValueParser`]: `1/0`, `true/false`, `yes/no`,
/// `on/off`, `t/f` and `y/n`, case-insensitive, and anything else is an argument error.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OptInValueParser(BoolishValueParser);

impl TypedValueParser for OptInValueParser {
    type Value = bool;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        if value.is_empty() {
            return Ok(false);
        }
        self.0.parse_ref(cmd, arg, value)
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        self.0.possible_values()
    }
}

/// Refuse to run a faucet-compiled binary on mainnet, and elsewhere without an opt-in.
///
/// The `faucet` cargo feature swaps the TEL precompile's `mint` dispatch table (the
/// instant, role-gated `mint(address,uint256)` replaces the timelocked, governance-only
/// `mint(uint256)`), so a faucet build joining mainnet computes different state roots
/// than the rest of the network for the same block: a consensus split, not a local
/// misconfiguration. Cargo feature unification can enable the feature transitively without
/// the operator ever typing it, so the caller passes the runtime `FAUCET_ENABLED` value.
///
/// A faucet build runs these checks, in this order:
///
/// 1. Chain id [`MAINNET_CHAIN_ID`] is refused. Nothing lifts this.
/// 2. A genesis whose block hash equals the embedded [`MAINNET_GENESIS`] is refused. The check
///    compares genesis block hashes rather than [`Genesis`] structs because the fork-config fields
///    (`config`, the chain id included) sit outside the genesis header preimage: a genesis
///    differing from mainnet only in such a field still computes mainnet's genesis block hash while
///    comparing unequal: it is the mainnet genesis under another config.
/// 3. Chain id [`ADIRI_CHAIN_ID`] passes. Unlike the mainnet refusal this keys on the chain id
///    alone: the genesis is not compared with the embedded adiri genesis. For `node`, only an
///    `adiri` build gets here on that chain id, because the `adiri` feature guard at the call site
///    in [`NodeCommand::execute`] refuses chain id 2017 in every other build, and `adiri` implies
///    `faucet`, so the faucet is intended there. `db load-state` has no feature guard of its own,
///    but a non-`adiri` build cannot start a node on the data it loads. The opt-in in step 4 is a
///    guard against starting a faucet build by mistake, not a trust boundary, so also matching the
///    adiri genesis hash would protect nothing. It would refuse the governance-Safe fork e2e lane
///    (`make test-e2e-governance-safe`), which runs an `adiri` binary on chain id 2017 with a
///    genesis generated for the test.
/// 4. Any other chain passes only with `allow_on_this_chain` set (`--allow-faucet-on-chain`, env
///    `TN_ALLOW_FAUCET_ON_CHAIN`). The e2e harness passes it to its in-process nodes. The in-tree
///    devnets (`etc/compose.yaml`, `etc/local-testnet.sh`) build without the faucet and need
///    nothing; a `make docker-devnet` faucet image on the chain id 487 genesis from
///    `etc/genesis.sh` must set `TN_ALLOW_FAUCET_ON_CHAIN=true` where it is deployed.
pub(crate) fn validate_faucet_build(
    faucet_enabled: bool,
    genesis: &Genesis,
    allow_on_this_chain: bool,
) -> eyre::Result<()> {
    if !faucet_enabled {
        return Ok(());
    }
    let chain_id = genesis.config.chain_id;
    if chain_id == MAINNET_CHAIN_ID {
        return Err(eyre::eyre!(
            "this binary was built with the faucet feature and refuses mainnet (chain id \
             {MAINNET_CHAIN_ID})"
        ));
    }
    let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
    if genesis_block_hash(genesis.clone()) == genesis_block_hash(mainnet) {
        return Err(eyre::eyre!(
            "Must NOT compile with faucet feature flag when connecting to Telcoin mainnet!"
        ));
    }
    // adiri by chain id only, not by genesis hash: see step 3 above
    if chain_id == ADIRI_CHAIN_ID || allow_on_this_chain {
        return Ok(());
    }
    Err(eyre::eyre!(
        "this binary was built with the faucet feature and starts on chain id {chain_id} only \
         with --allow-faucet-on-chain (env TN_ALLOW_FAUCET_ON_CHAIN=true)"
    ))
}

/// Refuse a cleartext BLS key on mainnet (chain id [`MAINNET_CHAIN_ID`]).
///
/// `cleartext_key` is [`KeyConfig::bls_key_is_cleartext`]: the key was read from `bls.key`
/// without a passphrase. That covers `--bls-passphrase-source no-passphrase`, the only source
/// that reaches the node without a passphrase, and any caller that loads keys without one.
/// Nothing lifts the refusal. Other chain ids are unaffected.
pub(crate) fn validate_bls_key_protection(chain_id: u64, cleartext_key: bool) -> eyre::Result<()> {
    if chain_id == MAINNET_CHAIN_ID && cleartext_key {
        return Err(eyre::eyre!(
            "refusing to start on mainnet (chain id {MAINNET_CHAIN_ID}) with a cleartext BLS key: \
             the key was read from node-keys/bls.key without a passphrase (for example with \
             --bls-passphrase-source no-passphrase); mainnet requires a passphrase-encrypted key \
             (bls.kw) and a passphrase source"
        ));
    }
    Ok(())
}

/// Warn about a cleartext `bls.key` left beside the encrypted key on mainnet (chain id
/// [`MAINNET_CHAIN_ID`]).
///
/// When the node loaded its key from `bls.kw` (`cleartext_key` false), a `bls.key` in the same
/// directory is a cleartext copy of a BLS private key that the node never reads. The node still
/// starts; the warning names the file so the operator can delete it. Other chain ids are not
/// checked. Returns the stale file's path when it warned.
pub(crate) fn warn_stale_cleartext_bls_key(
    chain_id: u64,
    cleartext_key: bool,
    keys_dir: &Path,
) -> Option<PathBuf> {
    if chain_id != MAINNET_CHAIN_ID || cleartext_key {
        return None;
    }
    let stale = keys_dir.join(BLS_KEYFILE);
    match std::fs::exists(&stale) {
        Ok(true) => {
            warn!(
                target: "cli",
                "cleartext BLS key file {} lies beside the encrypted key on mainnet (chain id \
                 {MAINNET_CHAIN_ID}); the node loaded bls.kw and does not read it, but it holds a \
                 BLS private key in cleartext. Delete it",
                stale.display()
            );
            Some(stale)
        }
        Ok(false) => None,
        Err(e) => {
            warn!(
                target: "cli",
                "cannot check for a stale cleartext BLS key file {}: {e}",
                stale.display()
            );
            None
        }
    }
}

/// The startup line naming the compiled build features and the loaded chain:
/// `node build: faucet=<bool> adiri=<bool> chain_id=<n> genesis_hash=<0x..>`.
///
/// `faucet` is the runtime `FAUCET_ENABLED` value, so it reports the faucet mint however the
/// feature reached tn-reth; `adiri` is this crate's `adiri` feature.
fn node_build_line(chain_id: u64, genesis_hash: B256) -> String {
    format!(
        "node build: faucet={FAUCET_ENABLED} adiri={} chain_id={chain_id} \
         genesis_hash={genesis_hash}",
        cfg!(feature = "adiri")
    )
}

/// The genesis block hash of `genesis`: the hash of the sealed genesis header, covering
/// the header fields plus the state root computed from `alloc`, built by the same
/// chain-spec construction the node itself boots with.
fn genesis_block_hash(genesis: Genesis) -> B256 {
    RethChainSpec::from(genesis).sealed_genesis_header().hash()
}

/// Refuse an `--instance` whose port shift leaves the port range.
///
/// reth's `adjust_instance_ports` moves the HTTP port down by `instance - 1` and the WebSocket
/// port up by `2 * (instance - 1)` with plain `u16` arithmetic: a release build wraps (an HTTP
/// port of 100 with instance 150 binds 65,387) and a build with overflow checks panics.
/// `--with-unused-ports` zeroes both ports before the shift, so there only instance 1 fits.
fn check_instance_ports(
    instance: Option<u16>,
    reth: &RethCommand,
    with_unused_ports: bool,
) -> eyre::Result<()> {
    let Some(instance) = instance else { return Ok(()) };
    let (http, ws) =
        if with_unused_ports { (0, 0) } else { (reth.rpc.http_port, reth.rpc.ws_port) };
    let shift = instance - 1;
    if http.checked_sub(shift).is_none() {
        eyre::bail!(
            "--instance {instance} moves the HTTP RPC port {http} down by {shift}, below port 0; \
             raise --http.port or lower --instance"
        );
    }
    if ws.checked_add(shift * 2).is_none() {
        eyre::bail!(
            "--instance {instance} moves the WebSocket RPC port {ws} up by {}, above port 65535; \
             lower --ws.port or --instance",
            shift * 2
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use tn_types::adiri_genesis;

    /// The node refuses to open its execution database while a `db reset-execution` there is
    /// unfinished (an `execution-reset-*.partial` directory), and creates nothing; once the reset
    /// directory has its final name, the node opens a new database.
    #[test]
    fn the_node_refuses_an_interrupted_reset_before_opening_its_database() -> eyre::Result<()> {
        let tmp = tempfile::TempDir::new()?;
        let datadir = tmp.path().to_path_buf();
        let partial = datadir.join("execution-reset-20261109T000000Z.partial");
        std::fs::create_dir_all(partial.join("db"))?;
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let config =
            RethConfig::new(RethCommand::try_parse_from(["tn-reth"])?, None, &datadir, true, chain);

        let err = open_execution_database(&config, &datadir).expect_err("an unfinished reset");
        assert!(err.to_string().contains("did not finish"), "{err}");
        assert!(!datadir.reth_db_path().try_exists()?, "no database is created");

        std::fs::rename(&partial, datadir.join("execution-reset-20261109T000000Z"))?;
        drop(open_execution_database(&config, &datadir)?);
        assert!(datadir.reth_db_path().try_exists()?, "a finished reset: a new database");
        Ok(())
    }

    /// The removed observer flag is rejected and absent from both help views.
    #[test]
    fn removed_observer_flag_is_rejected() -> eyre::Result<()> {
        use clap::CommandFactory as _;

        NodeCommand::<NoArgs>::try_parse_from(["node"])?;
        let legacy = NodeCommand::<NoArgs>::try_parse_from(["node", "--observer"]);
        assert_eq!(legacy.err().map(|error| error.kind()), Some(ErrorKind::UnknownArgument));
        let mut command = NodeCommand::<NoArgs>::command();
        assert!(!command.render_help().to_string().contains("--observer"));
        assert!(!command.render_long_help().to_string().contains("--observer"));
        Ok(())
    }

    /// The CLI accepts the same multi-worker map as network-config, in YAML or JSON form.
    #[test]
    fn bootstrap_peers_cli_parses_yaml_and_json() -> eyre::Result<()> {
        let committee: tn_types::Committee = serde_yaml::from_str(tn_types::MAINNET_COMMITTEE)?;
        let mut peers = committee.bootstrap_servers();
        assert!(!peers.is_empty());
        peers.values_mut().for_each(|server| server.workers.push(server.primary.clone()));
        [serde_yaml::to_string(&peers)?, serde_json::to_string(&peers)?].into_iter().try_for_each(
            |raw| -> eyre::Result<()> {
                let command = NodeCommand::<NoArgs>::try_parse_from([
                    "node",
                    "--bootstrap-peers",
                    raw.as_str(),
                ])?;
                assert_eq!(command.bootstrap_peers, Some(peers.clone()));
                Ok(())
            },
        )
    }

    /// An omitted option and an explicit empty override retain distinct precedence semantics.
    #[test]
    fn bootstrap_peers_cli_distinguishes_absent_and_empty() -> eyre::Result<()> {
        let absent = NodeCommand::<NoArgs>::try_parse_from(["node"])?;
        assert_eq!(absent.bootstrap_peers, None);
        let empty = NodeCommand::<NoArgs>::try_parse_from(["node", "--bootstrap-peers", "{}"])?;
        assert_eq!(empty.bootstrap_peers, Some(BTreeMap::new()));
        Ok(())
    }

    /// Invalid keys and empty worker lists fail at argument parsing, before node startup.
    #[test]
    fn bootstrap_peers_cli_rejects_invalid_entries() -> eyre::Result<()> {
        let committee: tn_types::Committee = serde_yaml::from_str(tn_types::MAINNET_COMMITTEE)?;
        let mut peers = committee.bootstrap_servers();
        peers.values_mut().for_each(|server| server.workers.clear());
        ["{invalid-key: {}}".to_owned(), serde_yaml::to_string(&peers)?].into_iter().for_each(
            |raw| {
                let result = NodeCommand::<NoArgs>::try_parse_from([
                    "node",
                    "--bootstrap-peers",
                    raw.as_str(),
                ])
                .map(|_| ())
                .map_err(|error| error.kind());
                assert_eq!(result, Err(ErrorKind::ValueValidation));
            },
        );
        Ok(())
    }

    /// `--healthcheck` accepts a bare port (loopback) or an explicit socket address and
    /// rejects anything else at parse time.
    #[test]
    fn healthcheck_flag_accepts_port_and_socket_address() -> Result<(), std::net::AddrParseError> {
        let parse = |args: &[&str]| {
            NodeCommand::<NoArgs>::try_parse_from(args)
                .map(|cmd| cmd.healthcheck)
                .map_err(|err| err.kind())
        };
        assert_eq!(
            parse(&["node", "--healthcheck", "8080"]),
            Ok(Some(HealthcheckBind::LegacyPort(8080)))
        );
        assert_eq!(
            parse(&["node", "--healthcheck", "[::1]:8080"]),
            Ok(Some(HealthcheckBind::Explicit("[::1]:8080".parse()?)))
        );
        assert_eq!(
            parse(&["node", "--healthcheck", "0.0.0.0:8080"]),
            Ok(Some(HealthcheckBind::Explicit("0.0.0.0:8080".parse()?)))
        );
        assert_eq!(parse(&["node", "--healthcheck", "127.0.0.1"]), Err(ErrorKind::ValueValidation));
        assert_eq!(parse(&["node", "--healthcheck", "nope"]), Err(ErrorKind::ValueValidation));
        assert_eq!(parse(&["node"]), Ok(None));
        Ok(())
    }

    /// A map read from `--bootstrap-peers-file` equals the same content passed inline.
    #[test]
    fn bootstrap_peers_file_matches_inline_map() -> eyre::Result<()> {
        let committee: tn_types::Committee = serde_yaml::from_str(tn_types::MAINNET_COMMITTEE)?;
        let peers = committee.bootstrap_servers();
        assert!(!peers.is_empty());
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("bootstrap-peers.yaml");
        [serde_yaml::to_string(&peers)?, serde_json::to_string(&peers)?, "{}".to_owned()]
            .into_iter()
            .try_for_each(|raw| -> eyre::Result<()> {
                std::fs::write(&path, &raw)?;
                let from_file = NodeCommand::<NoArgs>::try_parse_from([
                    "node".as_ref(),
                    "--bootstrap-peers-file".as_ref(),
                    path.as_os_str(),
                ])?;
                let inline =
                    NodeCommand::<NoArgs>::try_parse_from(["node", "--bootstrap-peers", &raw])?;
                assert!(from_file.bootstrap_peers_file.is_some());
                assert_eq!(from_file.bootstrap_peers_file, inline.bootstrap_peers);
                assert_eq!(from_file.bootstrap_peers, None);
                Ok(())
            })?;
        Ok(())
    }

    /// The inline map and the file form are mutually exclusive.
    #[test]
    fn bootstrap_peers_file_conflicts_with_inline_map() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("bootstrap-peers.yaml");
        std::fs::write(&path, "{}")?;
        let result = NodeCommand::<NoArgs>::try_parse_from([
            "node".as_ref(),
            "--bootstrap-peers".as_ref(),
            "{}".as_ref(),
            "--bootstrap-peers-file".as_ref(),
            path.as_os_str(),
        ])
        .map(|_| ())
        .map_err(|error| error.kind());
        assert_eq!(result, Err(ErrorKind::ArgumentConflict));
        Ok(())
    }

    /// A missing or unparsable file fails at argument parsing, before node startup.
    #[test]
    fn bootstrap_peers_file_rejects_missing_and_invalid_files() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let invalid = dir.path().join("invalid.yaml");
        std::fs::write(&invalid, "{invalid-key: {}}")?;
        [dir.path().join("absent.yaml"), invalid].iter().for_each(|path| {
            let result = NodeCommand::<NoArgs>::try_parse_from([
                "node".as_ref(),
                "--bootstrap-peers-file".as_ref(),
                path.as_os_str(),
            ])
            .map(|_| ())
            .map_err(|error| (error.kind(), error.to_string()));
            let (kind, message) = result.expect_err("bad file must not parse");
            assert_eq!(kind, ErrorKind::ValueValidation, "{path:?}");
            assert!(message.contains(path.to_str().expect("utf-8 temp path")), "{message}");
        });
        Ok(())
    }

    /// A file name that is not valid UTF-8 is read like any other, and a missing file under
    /// such a name still fails at argument parsing.
    ///
    /// A filesystem that only takes valid UTF-8 names (APFS on macOS refuses the name with
    /// `EILSEQ`) cannot hold the file, so the test ends after the missing-file half there.
    #[cfg(unix)]
    #[test]
    fn bootstrap_peers_file_accepts_non_utf8_path() -> eyre::Result<()> {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

        let committee: tn_types::Committee = serde_yaml::from_str(tn_types::MAINNET_COMMITTEE)?;
        let peers = committee.bootstrap_servers();
        assert!(!peers.is_empty());
        let raw = serde_yaml::to_string(&peers)?;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(OsStr::from_bytes(b"bootstrap-\xff.yaml"));
        assert!(path.to_str().is_none(), "the file name must not be valid UTF-8");
        let parse = |path: &std::path::Path| {
            NodeCommand::<NoArgs>::try_parse_from([
                "node".as_ref(),
                "--bootstrap-peers-file".as_ref(),
                path.as_os_str(),
            ])
        };

        let missing = parse(&path).map(|_| ()).map_err(|error| (error.kind(), error.to_string()));
        let (kind, message) = missing.expect_err("a missing file must not parse");
        assert_eq!(kind, ErrorKind::ValueValidation);
        assert!(message.contains(path.to_string_lossy().as_ref()), "{message}");

        match std::fs::write(&path, &raw) {
            Ok(()) => {}
            Err(error)
                if error.raw_os_error() == Some(libc::EILSEQ)
                    || error.kind() == std::io::ErrorKind::InvalidFilename =>
            {
                eprintln!("skipping the readable half: this filesystem refuses the name ({error})");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
        let from_file = parse(&path)?;
        let inline = NodeCommand::<NoArgs>::try_parse_from(["node", "--bootstrap-peers", &raw])?;
        assert!(from_file.bootstrap_peers_file.is_some());
        assert_eq!(from_file.bootstrap_peers_file, inline.bootstrap_peers);
        Ok(())
    }

    /// Parse `--instance <raw>` through the node command and return the parsed value, or the
    /// clap error kind that rejected it.
    fn parse_instance(raw: &str) -> Result<Option<u16>, ErrorKind> {
        NodeCommand::<NoArgs>::try_parse_from(["node", "--instance", raw])
            .map(|cmd| cmd.instance)
            .map_err(|err| err.kind())
    }

    /// `--instance 0` must fail at parse time. reth's `adjust_instance_ports` computes
    /// `instance - 1` on `u16`, so 0 panics under overflow checks and wraps in release (instance
    /// 0 then takes HTTP port 8546, instance 1's WebSocket port). The upper bound is reth's too.
    #[test]
    fn instance_rejects_zero_and_above_max_at_parse() {
        let rejected = ["0", "201"].map(parse_instance);
        assert_eq!(rejected, [Err(ErrorKind::ValueValidation), Err(ErrorKind::ValueValidation)]);
    }

    /// Both ends of the documented `1-200` range parse, and an absent flag stays `None`.
    #[test]
    fn instance_accepts_documented_bounds() {
        let accepted = ["1", "200"].map(parse_instance);
        assert_eq!(accepted, [Ok(Some(1)), Ok(Some(200))]);
        let absent = NodeCommand::<NoArgs>::try_parse_from(["node"]).map(|cmd| cmd.instance);
        assert!(matches!(absent, Ok(None)));
    }

    /// An instance whose shift would wrap a port is refused before the config is built; the
    /// shifts that stay in range pass, and so does `--with-unused-ports` with instance 1.
    #[test]
    fn instance_port_shift_is_checked() -> eyre::Result<()> {
        let parse = |args: &[&str]| NodeCommand::<NoArgs>::try_parse_from(args);
        let low_http = parse(&["node", "--instance", "150", "--http.port", "100"])?;
        let err = check_instance_ports(low_http.instance, &low_http.reth, false).unwrap_err();
        assert!(err.to_string().contains("below port 0"), "{err}");

        let high_ws = parse(&["node", "--instance", "200", "--ws.port", "65300"])?;
        let err = check_instance_ports(high_ws.instance, &high_ws.reth, false).unwrap_err();
        assert!(err.to_string().contains("above port 65535"), "{err}");

        let fits = parse(&["node", "--instance", "101", "--http.port", "100"])?;
        check_instance_ports(fits.instance, &fits.reth, false)?;
        let defaults = parse(&["node", "--instance", "200"])?;
        check_instance_ports(defaults.instance, &defaults.reth, false)?;
        check_instance_ports(Some(1), &defaults.reth, true)?;
        assert!(check_instance_ports(Some(2), &defaults.reth, true).is_err());
        check_instance_ports(None, &defaults.reth, true)
    }

    /// Retention defaults to unlimited, accepts positive counts, and rejects zero.
    ///
    /// A retention setting alone does not enable exports.
    #[test]
    fn state_export_keep_parses_optional_nonzero_limit() -> Result<(), clap::Error> {
        let command = NodeCommand::<NoArgs>::try_parse_from(["node"])?;
        assert_eq!(command.state_export_keep, None);
        assert!(!command.enable_state_export);

        let parsed = NodeCommand::<NoArgs>::try_parse_from(["node", "--state-export-keep", "0"])
            .map(|command| command.state_export_keep)
            .map_err(|error| error.kind());
        assert_eq!(parsed, Err(ErrorKind::ValueValidation));

        [("1", 1), ("2", 2), ("10", 10)].into_iter().try_for_each(|(raw, expected)| {
            let command =
                NodeCommand::<NoArgs>::try_parse_from(["node", "--state-export-keep", raw])?;
            assert_eq!(command.state_export_keep.map(NonZeroUsize::get), Some(expected));
            assert!(!command.enable_state_export);
            Ok::<(), clap::Error>(())
        })?;

        let enabled = NodeCommand::<NoArgs>::try_parse_from([
            "node",
            "--enable-state-export",
            "--state-export-keep",
            "2",
        ])?;
        assert_eq!(enabled.state_export_keep.map(NonZeroUsize::get), Some(2));
        assert!(enabled.enable_state_export);
        Ok(())
    }

    /// The chain id of the devnet scripts (`etc/genesis.sh`, `etc/local-testnet.sh`): a test
    /// chain that is not mainnet and not adiri.
    const DEVNET_CHAIN_ID: u64 = 487;

    /// A faucet build configured with the canonical mainnet genesis must refuse to start, with or
    /// without the opt-in, and the refusal names mainnet.
    #[test]
    fn faucet_build_rejects_canonical_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        for allow in [false, true] {
            let err = validate_faucet_build(true, &mainnet, allow).expect_err("mainnet genesis");
            assert!(err.to_string().contains("mainnet"), "{err}");
        }
        Ok(())
    }

    /// A default (non-faucet) build starts against the canonical mainnet genesis.
    #[test]
    fn default_build_accepts_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        assert!(validate_faucet_build(false, &mainnet, false).is_ok());
        Ok(())
    }

    /// A faucet build on the adiri/testnet genesis is allowed without the opt-in; that chain is
    /// faucet-intended and separately covered by the `adiri` feature guard. The exemption is by
    /// chain id: another genesis on chain id 2017 passes the same way.
    #[test]
    fn faucet_build_accepts_testnet_genesis() -> eyre::Result<()> {
        let testnet = adiri_genesis();
        assert_eq!(testnet.config.chain_id, ADIRI_CHAIN_ID);
        assert!(validate_faucet_build(true, &testnet, false).is_ok());
        let other = genesis_on_chain(ADIRI_CHAIN_ID)?;
        assert_ne!(genesis_block_hash(other.clone()), genesis_block_hash(testnet));
        assert!(validate_faucet_build(true, &other, false).is_ok());
        Ok(())
    }

    /// A genesis on `chain_id` that is not the mainnet genesis: the mainnet genesis with the next
    /// timestamp, so its block hash differs, and `chain_id` in its config.
    fn genesis_on_chain(chain_id: u64) -> eyre::Result<Genesis> {
        let base: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let mut genesis = Genesis { timestamp: base.timestamp.wrapping_add(1), ..base };
        genesis.config.chain_id = chain_id;
        Ok(genesis)
    }

    /// A faucet build refuses mainnet's chain id on any genesis, with or without the opt-in, and
    /// the refusal does not point at the flag.
    #[test]
    fn faucet_build_refuses_mainnet_chain_id_regardless_of_opt_in() -> eyre::Result<()> {
        let genesis = genesis_on_chain(MAINNET_CHAIN_ID)?;
        for allow in [false, true] {
            let err = validate_faucet_build(true, &genesis, allow).expect_err("mainnet chain id");
            let msg = err.to_string();
            assert!(msg.contains(&format!("mainnet (chain id {MAINNET_CHAIN_ID})")), "{msg}");
            assert!(!msg.contains("--allow-faucet-on-chain"), "{msg}");
        }
        Ok(())
    }

    /// A faucet build on the devnet chain id is refused without the opt-in, with an error naming
    /// the flag, its environment variable and the chain id, and starts with it.
    #[test]
    fn faucet_build_needs_opt_in_on_devnet_chain_id() -> eyre::Result<()> {
        let devnet = genesis_on_chain(DEVNET_CHAIN_ID)?;
        let err = validate_faucet_build(true, &devnet, false).expect_err("devnet without opt-in");
        let msg = err.to_string();
        assert!(msg.contains("--allow-faucet-on-chain"), "{msg}");
        assert!(msg.contains("TN_ALLOW_FAUCET_ON_CHAIN=true"), "{msg}");
        assert!(msg.contains(&format!("chain id {DEVNET_CHAIN_ID}")), "{msg}");
        assert!(validate_faucet_build(true, &devnet, true).is_ok());
        Ok(())
    }

    /// A default (non-faucet) build needs no opt-in, on the devnet chain id or on mainnet's.
    #[test]
    fn default_build_needs_no_opt_in() -> eyre::Result<()> {
        for chain_id in [DEVNET_CHAIN_ID, MAINNET_CHAIN_ID] {
            let genesis = genesis_on_chain(chain_id)?;
            assert!(validate_faucet_build(false, &genesis, false).is_ok());
        }
        Ok(())
    }

    /// A faucet build must be refused on a genesis whose fork config diverges from
    /// canonical mainnet while header and alloc stay identical: such a genesis computes
    /// mainnet's exact genesis block hash (the positive control below), so it is the
    /// mainnet genesis under another config. Struct equality would wrongly admit it; hash
    /// identity refuses it. The divergent genesis carries the devnet chain id, so the
    /// hash check and not the chain id check is what refuses it, with or without the opt-in.
    #[test]
    fn faucet_build_rejects_config_only_divergence_from_mainnet() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let mut divergent = mainnet.clone();
        divergent.config.dao_fork_support = !divergent.config.dao_fork_support;
        divergent.config.chain_id = DEVNET_CHAIN_ID;
        assert_ne!(divergent, mainnet);
        assert_eq!(genesis_block_hash(divergent.clone()), genesis_block_hash(mainnet));
        for allow in [false, true] {
            let err = validate_faucet_build(true, &divergent, allow).expect_err("mainnet hash");
            assert!(err.to_string().contains("Telcoin mainnet"), "{err}");
        }
        Ok(())
    }

    /// The compiled feature set gates the canonical mainnet genesis exactly: refused
    /// when the binary carries the faucet feature and accepted otherwise, whatever the
    /// opt-in. This exercises the same `FAUCET_ENABLED` value the [`NodeCommand::execute`]
    /// call site wires in, under whichever feature set the test build resolved.
    #[test]
    fn compiled_feature_set_gates_canonical_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        for allow in [false, true] {
            assert_eq!(
                validate_faucet_build(FAUCET_ENABLED, &mainnet, allow).is_err(),
                FAUCET_ENABLED
            );
        }
        Ok(())
    }

    /// The startup line has the fixed format deployment checks read, and its genesis hash is the
    /// genesis block hash the faucet guard compares.
    #[test]
    fn node_build_line_reports_features_chain_and_genesis_hash() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let hash = Arc::new(RethChainSpec::from(mainnet.clone())).genesis_hash();
        assert_eq!(hash, genesis_block_hash(mainnet));
        let line = node_build_line(MAINNET_CHAIN_ID, hash);
        assert_eq!(
            line,
            format!(
                "node build: faucet={FAUCET_ENABLED} adiri={} chain_id={MAINNET_CHAIN_ID} \
                 genesis_hash={hash:#x}",
                cfg!(feature = "adiri")
            )
        );
        let hex = line.rsplit("genesis_hash=0x").next().unwrap_or_default();
        assert!(hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()), "{line}");
        Ok(())
    }

    /// The faucet opt-in's argument id, the one node argument read from a `TN_ALLOW_*` variable.
    const FAUCET_OPT_IN: &str = "allow_faucet_on_chain";

    /// Parse `args` as the node command with the faucet opt-in's environment binding removed, so
    /// a `TN_ALLOW_FAUCET_ON_CHAIN` in the developer's shell cannot change the result.
    fn parse_node_without_env(args: &[&str]) -> Result<NodeCommand<NoArgs>, clap::Error> {
        use clap::{CommandFactory as _, FromArgMatches as _};

        let command = NodeCommand::<NoArgs>::command()
            .mut_arg(FAUCET_OPT_IN, |arg| arg.env(None::<&'static str>));
        NodeCommand::from_arg_matches(&command.try_get_matches_from(args)?)
    }

    /// `--allow-faucet-on-chain` defaults off, parses as a bare flag, reads its documented
    /// environment variable, and stays out of the short help.
    #[test]
    fn allow_faucet_flag_parses_and_is_hidden_from_short_help() -> eyre::Result<()> {
        use clap::CommandFactory as _;

        assert!(!parse_node_without_env(&["node"])?.allow_faucet_on_chain);
        let set = parse_node_without_env(&["node", "--allow-faucet-on-chain"])?;
        assert!(set.allow_faucet_on_chain);
        let mut command = NodeCommand::<NoArgs>::command();
        let arg = command
            .get_arguments()
            .find(|arg| arg.get_id() == FAUCET_OPT_IN)
            .ok_or_else(|| eyre::eyre!("flag registered"))?;
        assert_eq!(arg.get_env().and_then(|env| env.to_str()), Some("TN_ALLOW_FAUCET_ON_CHAIN"));
        assert!(!command.render_help().to_string().contains("--allow-faucet-on-chain"));
        assert!(command.render_long_help().to_string().contains("--allow-faucet-on-chain"));
        Ok(())
    }

    /// An empty value reads as unset; booleans parse as clap's boolish parser does, in any case;
    /// anything else is an argument error.
    #[test]
    fn opt_in_value_parser_reads_empty_as_unset() {
        let cmd = clap::Command::new("node");
        let parse = |raw: &str| {
            OptInValueParser::default()
                .parse_ref(&cmd, None, OsStr::new(raw))
                .map_err(|error| error.kind())
        };
        assert_eq!(parse(""), Ok(false));
        for raw in ["1", "true", "TRUE", "yes", "On", "t", "Y"] {
            assert_eq!(parse(raw), Ok(true), "{raw:?}");
        }
        for raw in ["0", "false", "No", "OFF", "f", "n"] {
            assert_eq!(parse(raw), Ok(false), "{raw:?}");
        }
        for raw in ["maybe", " ", "2"] {
            assert_eq!(parse(raw), Err(ErrorKind::ValueValidation), "{raw:?}");
        }
    }

    /// The faucet opt-in's environment value goes through [`OptInValueParser`] on both `node`
    /// and `db load-state`: an empty value leaves the flag off, a boolean sets it, and anything
    /// else stops the command at parse.
    #[test]
    fn faucet_opt_in_env_value_parses_on_node_and_load_state() {
        use clap::{Args as _, CommandFactory as _};

        // held to the end: `set_var` below changes the process environment, which the passphrase
        // tests read without std's lock
        let _env = crate::passphrase::env_lock();
        // a variable no other test reads, bound onto the flag in place of the real one so the
        // test runs clap's env path without depending on the developer's shell
        const VAR: &str = "TN_TEST_FAUCET_OPT_IN_ENV_VALUE";
        let cases = [
            ("", Ok(false)),
            ("TRUE", Ok(true)),
            ("0", Ok(false)),
            ("maybe", Err(ErrorKind::ValueValidation)),
        ];
        for (raw, expected) in cases {
            std::env::set_var(VAR, raw);
            let node = NodeCommand::<NoArgs>::command()
                .mut_arg(FAUCET_OPT_IN, |arg| arg.env(VAR))
                .try_get_matches_from(["node"]);
            let load_state =
                crate::db::DbLoadStateArgs::augment_args(clap::Command::new("load-state"))
                    .mut_arg(FAUCET_OPT_IN, |arg| arg.env(VAR))
                    .try_get_matches_from(["load-state", "pack"]);
            for (command, matches) in [("node", node), ("db load-state", load_state)] {
                let parsed =
                    matches.map(|m| m.get_flag(FAUCET_OPT_IN)).map_err(|error| error.kind());
                assert_eq!(parsed, expected, "{command} with {VAR}={raw:?}");
            }
        }
        std::env::remove_var(VAR);
    }

    /// A cleartext key on mainnet is refused, and the error names the source that loads one and
    /// the encrypted key file it wants instead; no flag is offered.
    #[test]
    fn cleartext_bls_key_refused_on_mainnet() {
        let err = validate_bls_key_protection(MAINNET_CHAIN_ID, true)
            .expect_err("cleartext key on mainnet");
        let msg = err.to_string();
        assert!(msg.contains("no-passphrase"), "{msg}");
        assert!(msg.contains("bls.kw"), "{msg}");
        assert!(!msg.contains("allow-cleartext"), "{msg}");
    }

    /// An encrypted key passes on mainnet, and other chain ids accept a cleartext key.
    #[test]
    fn bls_key_protection_accepts_encrypted_keys_and_other_chains() {
        assert!(validate_bls_key_protection(MAINNET_CHAIN_ID, false).is_ok());
        for chain_id in [ADIRI_CHAIN_ID, DEVNET_CHAIN_ID, 911_329] {
            assert!(validate_bls_key_protection(chain_id, true).is_ok());
        }
    }

    /// On mainnet a `bls.key` beside the `bls.kw` the node loaded is reported; without the file,
    /// with a cleartext-loaded key (the refusal covers that) or on another chain id nothing is
    /// reported.
    #[test]
    fn stale_cleartext_bls_key_is_reported_on_mainnet() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let keys_dir = dir.path();
        assert_eq!(warn_stale_cleartext_bls_key(MAINNET_CHAIN_ID, false, keys_dir), None);
        let stale = keys_dir.join(BLS_KEYFILE);
        std::fs::write(&stale, "stale")?;
        assert_eq!(warn_stale_cleartext_bls_key(MAINNET_CHAIN_ID, false, keys_dir), Some(stale));
        assert_eq!(warn_stale_cleartext_bls_key(MAINNET_CHAIN_ID, true, keys_dir), None);
        assert_eq!(warn_stale_cleartext_bls_key(ADIRI_CHAIN_ID, false, keys_dir), None);
        Ok(())
    }

    /// The removed `--allow-cleartext-bls-key` flag is rejected and absent from both help views:
    /// nothing lifts the mainnet cleartext key refusal.
    #[test]
    fn removed_cleartext_key_flag_is_rejected() -> eyre::Result<()> {
        use clap::CommandFactory as _;

        parse_node_without_env(&["node"])?;
        let legacy = parse_node_without_env(&["node", "--allow-cleartext-bls-key"]);
        assert_eq!(legacy.err().map(|error| error.kind()), Some(ErrorKind::UnknownArgument));
        let mut command = NodeCommand::<NoArgs>::command();
        assert!(!command.render_help().to_string().contains("--allow-cleartext-bls-key"));
        assert!(!command.render_long_help().to_string().contains("--allow-cleartext-bls-key"));
        Ok(())
    }

    /// The passphrase source arguments for `passphrase`: `no-passphrase` for `None`, otherwise
    /// the default `env` source, which [`Cli::resolve_bls_passphrase`] serves from its preloaded
    /// value, so the test never touches the process environment.
    #[cfg(not(feature = "adiri"))]
    fn passphrase_source_args(
        passphrase: &Option<zeroize::Zeroizing<String>>,
    ) -> &'static [&'static str] {
        if passphrase.is_none() {
            &["--bls-passphrase-source", "no-passphrase"]
        } else {
            &[]
        }
    }

    /// Generate validator keys in `datadir` as `keytool generate validator` does: a cleartext
    /// `bls.key` with `None`, a passphrase-encrypted `bls.kw` with `Some`.
    #[cfg(not(feature = "adiri"))]
    fn generate_keys(
        datadir: &str,
        passphrase: Option<zeroize::Zeroizing<String>>,
    ) -> eyre::Result<()> {
        use crate::cli::Cli;

        let args = ["tn"]
            .into_iter()
            .chain(passphrase_source_args(&passphrase).iter().copied())
            .chain(["keytool", "generate", "validator", "--workers", "1"])
            .chain(["--datadir", datadir, "--address", "0"]);
        let keygen = Cli::<NoArgs>::try_parse_args_from(args)?;
        let passphrase = keygen.resolve_bls_passphrase(passphrase)?;
        keygen.run(passphrase, |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
    }

    /// Run `node --chain mainnet` plus `extra` node arguments on `datadir`, through the same
    /// passphrase resolution and launcher plumbing as the binary: a cleartext key with `None`,
    /// the encrypted key decrypted with `Some`. The faucet opt-in's environment binding is
    /// removed, so the developer's shell cannot change the outcome.
    #[cfg(not(feature = "adiri"))]
    fn run_mainnet_node(
        datadir: &str,
        passphrase: Option<zeroize::Zeroizing<String>>,
        extra: &[&str],
    ) -> eyre::Result<()> {
        use crate::cli::Cli;
        use clap::{CommandFactory as _, FromArgMatches as _};

        let args = ["tn"]
            .into_iter()
            .chain(passphrase_source_args(&passphrase).iter().copied())
            .chain(["node", "--chain", "mainnet", "--datadir", datadir])
            .chain(extra.iter().copied());
        // seeded before the command is built, as `Cli::try_parse_args_from` does
        tn_reth::init_reth_defaults();
        let command = Cli::<NoArgs>::command().mut_subcommand("node", |node| {
            node.mut_arg(FAUCET_OPT_IN, |arg| arg.env(None::<&'static str>))
        });
        let node = Cli::<NoArgs>::from_arg_matches(&command.try_get_matches_from(args)?)?;
        let passphrase = node.resolve_bls_passphrase(passphrase)?;
        node.run(passphrase, |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
    }

    /// Every path under `dir`, sorted, so a test can compare a datadir before and after a run.
    #[cfg(not(feature = "adiri"))]
    fn datadir_listing(dir: &std::path::Path) -> eyre::Result<Vec<PathBuf>> {
        let mut listing = Vec::new();
        let mut pending = vec![dir.to_path_buf()];
        while let Some(next) = pending.pop() {
            for entry in std::fs::read_dir(&next)? {
                let path = entry?.path();
                if path.is_dir() {
                    pending.push(path.clone());
                }
                listing.push(path);
            }
        }
        listing.sort();
        Ok(listing)
    }

    /// `node --bls-passphrase-source no-passphrase` on mainnet loads the cleartext key and is
    /// refused, in every build (the key check runs before the faucet check), before it changes
    /// the datadir: no database and no `genesis/` committee file.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn node_refuses_cleartext_key_on_mainnet() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_str().ok_or_else(|| eyre::eyre!("utf-8 temp path"))?;
        generate_keys(datadir, None)?;
        let before = datadir_listing(dir.path())?;
        let err = run_mainnet_node(datadir, None, &[])
            .expect_err("a cleartext key on mainnet must be refused");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(&format!("refusing to start on mainnet (chain id {MAINNET_CHAIN_ID})")),
            "{msg}"
        );
        assert!(!dir.path().to_path_buf().reth_db_path().exists());
        assert!(!dir.path().to_path_buf().genesis_path().exists());
        assert_eq!(datadir_listing(dir.path())?, before);
        Ok(())
    }

    /// With a passphrase-encrypted key the same node passes the key check. A default build then
    /// starts and reaches the launcher. A build that carries the faucet mint is refused on
    /// mainnet with and without `--allow-faucet-on-chain`; neither refusal changes the datadir.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn node_on_mainnet_with_encrypted_key() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_str().ok_or_else(|| eyre::eyre!("utf-8 temp path"))?;
        let passphrase = || Some(zeroize::Zeroizing::new("mainnet test passphrase".to_owned()));
        generate_keys(datadir, passphrase())?;
        let keys_dir = dir.path().to_path_buf().node_keys_path();
        assert!(keys_dir.join(tn_config::BLS_WRAPPED_KEYFILE).exists());
        assert!(!keys_dir.join(BLS_KEYFILE).exists());
        if FAUCET_ENABLED {
            let before = datadir_listing(dir.path())?;
            for extra in [&[][..], &["--allow-faucet-on-chain"]] {
                let err = run_mainnet_node(datadir, passphrase(), extra)
                    .expect_err("a faucet build is refused on mainnet");
                let msg = format!("{err:#}");
                assert!(msg.contains("refuses mainnet"), "{msg}");
            }
            assert_eq!(datadir_listing(dir.path())?, before);
            return Ok(());
        }
        run_mainnet_node(datadir, passphrase(), &[])?;
        // past every startup guard: the bundled committee and the execution database were
        // written for the launcher
        assert!(dir.path().to_path_buf().committee_path().exists());
        assert!(dir.path().to_path_buf().reth_db_path().exists());
        Ok(())
    }
}
