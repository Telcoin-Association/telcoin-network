//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{
    builder::{BoolishValueParser, PossibleValue, TypedValueParser},
    value_parser, Parser,
};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{
    collections::BTreeMap, ffi::OsStr, net::SocketAddr, num::NonZeroUsize, path::PathBuf,
    sync::Arc, thread::available_parallelism,
};
use tn_config::{Config, KeyConfig, PidLock, TelcoinDirs};
use tn_node::engine::TnBuilder;
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

/// Avaliable "named" chains.
/// These will have embedded config files and can be joined after gereating keys.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum NamedChain {
    /// Adiri- alias for TestNet
    Adiri,
    /// TestNet or Adiri
    TestNet,
    /// MainNet
    MainNet,
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
            Self::MainNet => Config::load_mainnet(tn_datadir, version),
        }
    }

    /// Write this chain's bundled genesis committee to the datadir unless one is already there.
    ///
    /// Callers run this after their startup checks pass, so a refused start leaves the datadir
    /// as it was.
    pub(crate) fn write_committee<P: TelcoinDirs>(self, tn_datadir: &P) -> eyre::Result<()> {
        match self {
            Self::Adiri | Self::TestNet => Config::write_adiri_committee(tn_datadir),
            Self::MainNet => Config::write_mainnet_committee(tn_datadir),
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

    /// TCP health check endpoint port.
    ///
    /// When a port is specified, the node will spawn a TCP health check service
    /// on that port. The health check endpoint is useful for load balancers and
    /// monitoring systems to verify that the node process is running.
    ///
    /// If not specified, the health check service will not be started.
    ///
    /// WARNING: ensure the health endpoint is behind a firewall.
    /// Each connection is handled synchronously in the main accept loop.
    /// No connection limits or rate limiting are implemented.
    /// Connections are immediately closed after sending response.
    #[arg(long, value_name = "HEALTHCHECK_TCP_PORT", global = true, env = "HEALTHCHECK_TCP_PORT")]
    pub healthcheck: Option<u16>,

    /// Assign this name to node.  Currently used to name it's opentracing service.
    #[arg(long, value_name = "NODE_NAME", global = true)]
    pub node_name: Option<String>,

    /// URL of an opentracing service (like jaeger) to send tracing data to (for example http://192.168.1.2:4317).
    #[arg(long, value_name = "URL", global = true, env = "TN_TRACING_URL")]
    pub tracing_url: Option<String>,

    /// Let a faucet build start on chain id 487. Devnet only.
    ///
    /// A binary built with the `faucet` feature refuses chain id 487, the mainnet chain id,
    /// unless this is set. Devnets that reuse chain id 487 set it. A faucet build refuses the
    /// embedded mainnet genesis even with this set.
    ///
    /// The environment variable accepts 1/0, true/false, yes/no, on/off, t/f and y/n in any case;
    /// an empty value leaves the flag off and any other value is an argument error.
    #[arg(
        long,
        env = "TN_ALLOW_FAUCET_ON_CHAIN_487",
        value_parser = OptInValueParser::default(),
        hide_short_help = true
    )]
    pub allow_faucet_on_chain_487: bool,

    /// Let the node start on chain id 487 with a cleartext BLS key. Testing only.
    ///
    /// On chain id 487, the mainnet chain id, the node refuses a BLS key read from the
    /// cleartext `node-keys/bls.key` without a passphrase (for example with
    /// `--bls-passphrase-source no-passphrase`) and requires the passphrase-encrypted `bls.kw`.
    /// This flag lifts that refusal. It is read from the command line only, never from the
    /// environment.
    #[arg(long, hide_short_help = true)]
    pub allow_cleartext_bls_key: bool,

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
        validate_bls_key_protection(
            chain_id,
            key_config.bls_key_is_cleartext(),
            self.allow_cleartext_bls_key,
        )?;
        // Runtime value, not a #[cfg], so the check holds no matter which crate in the
        // build graph enabled the faucet feature.
        validate_faucet_build(FAUCET_ENABLED, tn_config.genesis(), self.allow_faucet_on_chain_487)?;
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            chain: _, // Used above
            bootstrap_peers,
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
            allow_faucet_on_chain_487: _, // Used above
            allow_cleartext_bls_key: _,   // Used above
        } = self;

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
        let reth_db = tn_reth::RethEnv::new_database(&node_config, tn_datadir.reth_db_path())?;
        let mut builder = TnBuilder::new(node_config, tn_config, reth_db)
            .with_state_export_keep(state_export_keep)
            .with_bootstrap_peers(bootstrap_peers)
            .with_pid_lock(pid_lock);
        builder.metrics = metrics;
        builder.healthcheck = healthcheck;
        builder.enable_state_export = enable_state_export;
        builder.enable_repack_monitor = enable_repack_monitor;

        Ok(launcher(builder, ext, tn_datadir, key_config, SHORT_VERSION))
    }
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

/// Refuse to run a faucet-compiled binary on the mainnet chain id or the mainnet genesis.
///
/// The `faucet` cargo feature swaps the TEL precompile's `mint` dispatch table (the
/// instant, role-gated `mint(address,uint256)` replaces the timelocked, governance-only
/// `mint(uint256)`), so a faucet build joining mainnet computes different state roots
/// than the rest of the network for the same block: a consensus split, not a local
/// misconfiguration. Cargo feature unification can enable the feature transitively without
/// the operator ever typing it, so the caller passes the runtime `FAUCET_ENABLED` value.
///
/// A faucet build runs two checks:
///
/// - Chain id: chain id [`MAINNET_CHAIN_ID`] is refused unless `allow_on_mainnet_chain_id` is set
///   (`--allow-faucet-on-chain-487`). Devnets that reuse that chain id (the docker devnet scripts
///   in `etc/genesis.sh`) opt in explicitly.
/// - Genesis identity: with the opt-in set, a genesis whose block hash equals the embedded
///   [`MAINNET_GENESIS`] is still refused. The check compares genesis block hashes rather than
///   [`Genesis`] structs because the fork-config fields (`config`) sit outside the genesis header
///   preimage: a genesis differing from mainnet only in such a field still computes mainnet's
///   genesis block hash, and could join mainnet consensus, while comparing unequal.
///
/// Adiri/testnet (2017) is faucet-intended and passes both checks; the `adiri` feature guard at
/// the call site in [`NodeCommand::execute`] covers it separately.
pub(crate) fn validate_faucet_build(
    faucet_enabled: bool,
    genesis: &Genesis,
    allow_on_mainnet_chain_id: bool,
) -> eyre::Result<()> {
    if !faucet_enabled {
        return Ok(());
    }
    if genesis.config.chain_id == MAINNET_CHAIN_ID && !allow_on_mainnet_chain_id {
        return Err(eyre::eyre!(
            "this binary was built with the faucet feature and refuses chain id \
             {MAINNET_CHAIN_ID}; a devnet that reuses chain id {MAINNET_CHAIN_ID} must pass \
             --allow-faucet-on-chain-487 (env TN_ALLOW_FAUCET_ON_CHAIN_487=true)"
        ));
    }
    let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
    if genesis_block_hash(genesis.clone()) == genesis_block_hash(mainnet) {
        return Err(eyre::eyre!(
            "Must NOT compile with faucet feature flag when connecting to Telcoin mainnet!"
        ));
    }
    Ok(())
}

/// Refuse a cleartext BLS key on chain id [`MAINNET_CHAIN_ID`] unless the operator opted in.
///
/// `cleartext_key` is [`KeyConfig::bls_key_is_cleartext`]: the key was read from `bls.key`
/// without a passphrase. That covers `--bls-passphrase-source no-passphrase`, the only source
/// that reaches the node without a passphrase, and any caller that loads keys without one.
/// Other chain ids are unaffected. With `allow_cleartext` set the node starts and logs a warning.
pub(crate) fn validate_bls_key_protection(
    chain_id: u64,
    cleartext_key: bool,
    allow_cleartext: bool,
) -> eyre::Result<()> {
    if chain_id != MAINNET_CHAIN_ID || !cleartext_key {
        return Ok(());
    }
    if allow_cleartext {
        warn!(
            target: "cli",
            "starting on chain id {MAINNET_CHAIN_ID} with a cleartext BLS key (bls.key) because \
             --allow-cleartext-bls-key is set"
        );
        return Ok(());
    }
    Err(eyre::eyre!(
        "refusing to start on chain id {MAINNET_CHAIN_ID} with a cleartext BLS key: the key was \
         read from node-keys/bls.key without a passphrase (for example with \
         --bls-passphrase-source no-passphrase). Use a passphrase-encrypted key (bls.kw) and a \
         passphrase source, or pass --allow-cleartext-bls-key on a test network"
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use tn_types::adiri_genesis;

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

    /// A faucet build configured with the canonical mainnet genesis must refuse to start, with or
    /// without the chain id opt-in: the genesis-hash check runs behind the chain id check.
    #[test]
    fn faucet_build_rejects_canonical_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        assert!(validate_faucet_build(true, &mainnet, false).is_err());
        let err = validate_faucet_build(true, &mainnet, true).expect_err("mainnet genesis");
        assert!(err.to_string().contains("Telcoin mainnet"), "{err}");
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
    /// faucet-intended and separately covered by the `adiri` feature guard.
    #[test]
    fn faucet_build_accepts_testnet_genesis() -> eyre::Result<()> {
        let testnet = adiri_genesis();
        assert_eq!(testnet.config.chain_id, ADIRI_CHAIN_ID);
        assert!(validate_faucet_build(true, &testnet, false).is_ok());
        Ok(())
    }

    /// A devnet genesis that reuses mainnet's chain id but is not the mainnet genesis.
    fn devnet_genesis_on_mainnet_chain_id() -> eyre::Result<Genesis> {
        let base: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let devnet = Genesis { timestamp: base.timestamp.wrapping_add(1), ..base };
        assert_eq!(devnet.config.chain_id, MAINNET_CHAIN_ID);
        Ok(devnet)
    }

    /// A faucet build refuses chain id 487 when the operator has not opted in, even on a genesis
    /// that is not the mainnet genesis, and the error names the opt-in flag.
    #[test]
    fn faucet_build_refuses_chain_487_without_opt_in() -> eyre::Result<()> {
        let devnet = devnet_genesis_on_mainnet_chain_id()?;
        let err = validate_faucet_build(true, &devnet, false).expect_err("chain 487 refused");
        assert!(err.to_string().contains("--allow-faucet-on-chain-487"), "{err}");
        Ok(())
    }

    /// A faucet build on a devnet genesis that reuses mainnet's chain id 487 (the documented
    /// docker-devnet flow) starts once the operator opts in with `--allow-faucet-on-chain-487`.
    #[test]
    fn faucet_build_accepts_devnet_genesis_with_mainnet_chain_id() -> eyre::Result<()> {
        let devnet = devnet_genesis_on_mainnet_chain_id()?;
        assert!(validate_faucet_build(true, &devnet, true).is_ok());
        Ok(())
    }

    /// A default (non-faucet) build needs no opt-in to start on a chain id 487 devnet.
    #[test]
    fn default_build_needs_no_opt_in_on_chain_487() -> eyre::Result<()> {
        let devnet = devnet_genesis_on_mainnet_chain_id()?;
        assert!(validate_faucet_build(false, &devnet, false).is_ok());
        Ok(())
    }

    /// A faucet build must be refused on a genesis whose fork config diverges from
    /// canonical mainnet while header and alloc stay identical: such a genesis computes
    /// mainnet's exact genesis block hash (the positive control below), so it could
    /// still join mainnet consensus. Struct equality would wrongly admit it; hash
    /// identity refuses it, with the chain id opt-in set.
    #[test]
    fn faucet_build_rejects_config_only_divergence_from_mainnet() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let mut divergent = mainnet.clone();
        divergent.config.dao_fork_support = !divergent.config.dao_fork_support;
        assert_ne!(divergent, mainnet);
        assert_eq!(genesis_block_hash(divergent.clone()), genesis_block_hash(mainnet));
        assert!(validate_faucet_build(true, &divergent, true).is_err());
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
                "node build: faucet={FAUCET_ENABLED} adiri={} chain_id=487 genesis_hash={hash:#x}",
                cfg!(feature = "adiri")
            )
        );
        let hex = line.rsplit("genesis_hash=0x").next().unwrap_or_default();
        assert!(hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()), "{line}");
        Ok(())
    }

    /// `--allow-faucet-on-chain-487` defaults off, parses as a bare flag, reads its documented
    /// environment variable, and stays out of the short help.
    #[test]
    fn allow_faucet_flag_parses_and_is_devnet_only() -> eyre::Result<()> {
        use clap::CommandFactory as _;

        assert!(!NodeCommand::<NoArgs>::try_parse_from(["node"])?.allow_faucet_on_chain_487);
        let set = NodeCommand::<NoArgs>::try_parse_from(["node", "--allow-faucet-on-chain-487"])?;
        assert!(set.allow_faucet_on_chain_487);
        let mut command = NodeCommand::<NoArgs>::command();
        let arg = command
            .get_arguments()
            .find(|arg| arg.get_id() == "allow_faucet_on_chain_487")
            .ok_or_else(|| eyre::eyre!("flag registered"))?;
        assert_eq!(
            arg.get_env().and_then(|env| env.to_str()),
            Some("TN_ALLOW_FAUCET_ON_CHAIN_487")
        );
        assert!(!command.render_help().to_string().contains("--allow-faucet-on-chain-487"));
        assert!(command.render_long_help().to_string().contains("--allow-faucet-on-chain-487"));
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

        // a variable no other test reads, bound onto the flag in place of the real one so the
        // test runs clap's env path without depending on the developer's shell
        const VAR: &str = "TN_TEST_FAUCET_OPT_IN_ENV_VALUE";
        let flag = "allow_faucet_on_chain_487";
        let cases = [
            ("", Ok(false)),
            ("TRUE", Ok(true)),
            ("0", Ok(false)),
            ("maybe", Err(ErrorKind::ValueValidation)),
        ];
        for (raw, expected) in cases {
            std::env::set_var(VAR, raw);
            let node = NodeCommand::<NoArgs>::command()
                .mut_arg(flag, |arg| arg.env(VAR))
                .try_get_matches_from(["node"]);
            let load_state =
                crate::db::DbLoadStateArgs::augment_args(clap::Command::new("load-state"))
                    .mut_arg(flag, |arg| arg.env(VAR))
                    .try_get_matches_from(["load-state", "pack"]);
            for (command, matches) in [("node", node), ("db load-state", load_state)] {
                let parsed = matches.map(|m| m.get_flag(flag)).map_err(|error| error.kind());
                assert_eq!(parsed, expected, "{command} with {VAR}={raw:?}");
            }
        }
        std::env::remove_var(VAR);
    }

    /// A cleartext key on chain id 487 is refused without the opt-in and the error names it;
    /// the opt-in lifts the refusal.
    #[test]
    fn cleartext_bls_key_refused_on_chain_487_without_opt_in() {
        let err = validate_bls_key_protection(MAINNET_CHAIN_ID, true, false)
            .expect_err("cleartext key on chain 487");
        let msg = err.to_string();
        assert!(msg.contains("--allow-cleartext-bls-key"), "{msg}");
        assert!(msg.contains("no-passphrase"), "{msg}");
        assert!(validate_bls_key_protection(MAINNET_CHAIN_ID, true, true).is_ok());
    }

    /// An encrypted key passes on chain id 487, and other chain ids accept a cleartext key with
    /// no opt-in.
    #[test]
    fn bls_key_protection_accepts_encrypted_keys_and_other_chains() {
        assert!(validate_bls_key_protection(MAINNET_CHAIN_ID, false, false).is_ok());
        for chain_id in [ADIRI_CHAIN_ID, 911_329] {
            assert!(validate_bls_key_protection(chain_id, true, false).is_ok());
        }
    }

    /// `--allow-cleartext-bls-key` defaults off, parses as a bare flag, has no environment
    /// variable, and stays out of the short help.
    #[test]
    fn allow_cleartext_flag_parses_and_is_hidden_from_short_help() -> eyre::Result<()> {
        use clap::CommandFactory as _;

        assert!(!NodeCommand::<NoArgs>::try_parse_from(["node"])?.allow_cleartext_bls_key);
        let set = NodeCommand::<NoArgs>::try_parse_from(["node", "--allow-cleartext-bls-key"])?;
        assert!(set.allow_cleartext_bls_key);
        let mut command = NodeCommand::<NoArgs>::command();
        let arg = command
            .get_arguments()
            .find(|arg| arg.get_id() == "allow_cleartext_bls_key")
            .ok_or_else(|| eyre::eyre!("flag registered"))?;
        assert_eq!(arg.get_env(), None, "the cleartext opt-in is command-line only");
        assert!(!command.render_help().to_string().contains("--allow-cleartext-bls-key"));
        assert!(command.render_long_help().to_string().contains("--allow-cleartext-bls-key"));
        Ok(())
    }

    /// Generate cleartext validator keys in `datadir`, as `keytool generate validator
    /// --bls-passphrase-source no-passphrase` does.
    #[cfg(not(feature = "adiri"))]
    fn generate_cleartext_keys(datadir: &str) -> eyre::Result<()> {
        use crate::cli::Cli;

        let keygen = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "--bls-passphrase-source",
            "no-passphrase",
            "keytool",
            "generate",
            "validator",
            "--workers",
            "1",
            "--datadir",
            datadir,
            "--address",
            "0",
        ])?;
        let passphrase = keygen.resolve_bls_passphrase(None)?;
        keygen.run(passphrase, |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
    }

    /// Run `node --chain main-net` (chain id 487) with `--bls-passphrase-source no-passphrase`
    /// plus `extra` node arguments on `datadir`, through the same passphrase resolution and
    /// launcher plumbing as the binary.
    #[cfg(not(feature = "adiri"))]
    fn run_mainnet_node_with_cleartext_key(datadir: &str, extra: &[&str]) -> eyre::Result<()> {
        use crate::cli::Cli;

        let args =
            ["tn", "node", "--bls-passphrase-source", "no-passphrase", "--chain", "main-net"]
                .into_iter()
                .chain(["--datadir", datadir])
                .chain(extra.iter().copied());
        let node = Cli::<NoArgs>::try_parse_args_from(args)?;
        let passphrase = node.resolve_bls_passphrase(None)?;
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

    /// `node --bls-passphrase-source no-passphrase` on chain id 487 loads the cleartext key and
    /// is refused before it changes the datadir: no database and no `genesis/` committee file.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn node_refuses_no_passphrase_on_chain_487() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_str().ok_or_else(|| eyre::eyre!("utf-8 temp path"))?;
        generate_cleartext_keys(datadir)?;
        let before = datadir_listing(dir.path())?;
        let err = run_mainnet_node_with_cleartext_key(datadir, &[])
            .expect_err("no-passphrase on chain 487 must be refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("refusing to start on chain id 487"), "{msg}");
        assert!(!dir.path().to_path_buf().reth_db_path().exists());
        assert!(!dir.path().to_path_buf().genesis_path().exists());
        assert_eq!(datadir_listing(dir.path())?, before);
        Ok(())
    }

    /// With `--allow-cleartext-bls-key` the same node starts and reaches the launcher. Skipped
    /// when the build carries the faucet mint, which refuses chain id 487 on its own.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn node_starts_on_chain_487_with_cleartext_opt_in() -> eyre::Result<()> {
        if FAUCET_ENABLED {
            return Ok(());
        }
        let dir = tempfile::tempdir()?;
        let datadir = dir.path().to_str().ok_or_else(|| eyre::eyre!("utf-8 temp path"))?;
        generate_cleartext_keys(datadir)?;
        run_mainnet_node_with_cleartext_key(datadir, &["--allow-cleartext-bls-key"])?;
        // past every startup guard: the bundled committee and the execution database were
        // written for the launcher
        assert!(dir.path().to_path_buf().committee_path().exists());
        assert!(dir.path().to_path_buf().reth_db_path().exists());
        Ok(())
    }
}
