//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Parser};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, thread::available_parallelism};
use tn_config::{Config, KeyConfig, TelcoinDirs as _};
use tn_node::engine::TnBuilder;
use tn_reth::{parse_socket_address, RethChainSpec, RethCommand, RethConfig, FAUCET_ENABLED};
use tn_types::{Genesis, B256, MAINNET_GENESIS};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

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

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Join a named telcoin network (for instance test or main net).
    #[arg(long, value_name = "NAMED_TN_NETWORK", verbatim_doc_comment)]
    pub chain: Option<NamedChain>,

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
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - `HTTP_RPC_PORT`: default - `instance` + 1
    /// - `WS_RPC_PORT`: default + `instance` * 2 - 2
    /// - `IPC_PATH`: default + `-instance`
    #[arg(long, value_name = "INSTANCE", global = true,  value_parser = value_parser!(u16).range(..=200))]
    pub instance: Option<u16>,

    /// Is this an observer node?  True if set, an observer will never be in the committee
    /// but will follow consensus and provide node RPC access.
    #[arg(long, value_name = "OBSERVER", global = true, default_value_t = false)]
    pub observer: bool,

    /// Export each epoch's final execution state to a snapshot pack under
    /// `consensus-db/state_exports/epoch-{N}/`.
    #[arg(long, global = true, default_value_t = false)]
    pub enable_state_export: bool,

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
        // across the fleet before a fork epoch arrives. Adiri builds carry epoch-gated forks;
        // every other build has the seed-signature (#1032) and multi-worker committee (#554)
        // wire formats and the batch-producer beneficiary rule (#1222) active from genesis.
        #[cfg(feature = "adiri")]
        info!(
            target: "cli",
            consensus_registry_fork_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH,
            seed_signature_fork_epoch = tn_types::forks::SEED_SIGNATURE_FORK_EPOCH,
            multi_workers_fork_epoch = tn_types::forks::MULTI_WORKERS_FORK_EPOCH,
            batch_producer_beneficiary_fork_epoch =
                tn_types::forks::BATCH_PRODUCER_BENEFICIARY_FORK_EPOCH,
            "fork schedule (adiri)"
        );
        #[cfg(not(feature = "adiri"))]
        info!(
            target: "cli",
            "fork schedule: seed_signature, multi_workers, and batch_producer_beneficiary \
             active from genesis"
        );

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
        let tn_config = if let Some(chain) = self.chain.take() {
            info!(target: "cli", "Overwriting TN config with named chain: {chain:?}");
            match chain {
                NamedChain::Adiri | NamedChain::TestNet => {
                    Config::load_adiri(&tn_datadir, self.observer, SHORT_VERSION)?
                }
                NamedChain::MainNet => {
                    Config::load_mainnet(&tn_datadir, self.observer, SHORT_VERSION)?
                }
            }
        } else {
            Config::load(&tn_datadir, self.observer, SHORT_VERSION)?
        };
        #[cfg(not(feature = "adiri"))]
        if tn_config.genesis().config.chain_id == 2017 {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must compile with adiri feature flag in order to connect to adiri (testnet)!"
            ));
        }
        #[cfg(feature = "adiri")]
        if tn_config.genesis().config.chain_id != 2017 {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must NOT compile with adiri feature flag when connecting to non-adiri (testnet) networks!"
            ));
        }
        // Runtime value, not a #[cfg], so the check holds no matter which crate in the
        // build graph enabled the faucet feature.
        validate_faucet_build(FAUCET_ENABLED, tn_config.genesis())?;
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            chain: _,    // Used above
            observer: _, // Used above
            metrics,
            enable_state_export,
            instance,
            with_unused_ports,
            reth,
            healthcheck,
            ext,
            node_name: _,
            tracing_url: _,
        } = self;

        // set up reth node config for engine components
        let node_config = RethConfig::new(
            reth,
            instance,
            &tn_datadir,
            with_unused_ports,
            Arc::new(tn_config.chain_spec()),
        );

        // create dbs to survive between sync state transitions
        let reth_db = tn_reth::RethEnv::new_database(&node_config, tn_datadir.reth_db_path())?;
        let builder = TnBuilder {
            node_config,
            tn_config,
            metrics,
            healthcheck,
            enable_state_export,
            reth_db,
            exex_fns: vec![],
        };

        Ok(launcher(builder, ext, tn_datadir, key_config, SHORT_VERSION))
    }
}

/// Refuse to run a faucet-compiled binary against canonical Telcoin mainnet.
///
/// The `faucet` cargo feature swaps the TEL precompile's `mint` dispatch table (the
/// instant, role-gated `mint(address,uint256)` replaces the timelocked, governance-only
/// `mint(uint256)`), so a faucet build joining mainnet computes different state roots
/// than the rest of the network for the same block: a consensus split, not a local
/// misconfiguration. No attacker is required; cargo feature unification can enable the
/// feature transitively without the operator ever typing it.
///
/// The check keys on the genesis block hash, the identity peers actually agree on,
/// rather than either alternative:
///
/// - Chain id: the guard the issue literally proposed (refuse `chain_id != 2017` in faucet builds)
///   would reject both supported faucet flows, the docker devnet scripts at chain id 487
///   (`etc/genesis.sh`) and the e2e faucet test at the default 911329; keying on mainnet's own 487
///   instead would still reject the devnet flow, which shares that id.
/// - Struct equality on [`Genesis`]: the fork-config fields (`config`) sit outside the genesis
///   header preimage, so a genesis differing from canonical mainnet only in such a field still
///   computes mainnet's genesis block hash (and could join mainnet consensus) while comparing
///   unequal.
///
/// Only the canonical embedded mainnet genesis is refused; adiri/testnet (2017) is
/// faucet-intended and separately covered by the `adiri` feature guard at the call
/// site in [`NodeCommand::execute`].
pub(crate) fn validate_faucet_build(faucet_enabled: bool, genesis: &Genesis) -> eyre::Result<()> {
    let joins_mainnet = faucet_enabled
        .then(|| serde_yaml::from_str::<Genesis>(MAINNET_GENESIS))
        .transpose()?
        .is_some_and(|mainnet| genesis_block_hash(genesis.clone()) == genesis_block_hash(mainnet));
    (!joins_mainnet).then_some(()).ok_or_else(|| {
        eyre::eyre!("Must NOT compile with faucet feature flag when connecting to Telcoin mainnet!")
    })
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
    use tn_types::adiri_genesis;

    /// A faucet build configured with the canonical mainnet genesis must refuse to start.
    #[test]
    fn faucet_build_rejects_canonical_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        assert!(validate_faucet_build(true, &mainnet).is_err());
        Ok(())
    }

    /// A default (non-faucet) build starts against the canonical mainnet genesis.
    #[test]
    fn default_build_accepts_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        assert!(validate_faucet_build(false, &mainnet).is_ok());
        Ok(())
    }

    /// A faucet build on the adiri/testnet genesis is allowed; that chain is
    /// faucet-intended and separately covered by the `adiri` feature guard.
    #[test]
    fn faucet_build_accepts_testnet_genesis() -> eyre::Result<()> {
        assert!(validate_faucet_build(true, &adiri_genesis()).is_ok());
        Ok(())
    }

    /// A faucet build on a devnet genesis that reuses mainnet's chain id 487 (the
    /// documented docker-devnet flow) must NOT be rejected: the guard keys on genesis
    /// identity, not chain id.
    #[test]
    fn faucet_build_accepts_devnet_genesis_with_mainnet_chain_id() -> eyre::Result<()> {
        let base: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let devnet = Genesis { timestamp: base.timestamp.wrapping_add(1), ..base };
        assert_eq!(devnet.config.chain_id, 487);
        assert!(validate_faucet_build(true, &devnet).is_ok());
        Ok(())
    }

    /// A faucet build must be refused on a genesis whose fork config diverges from
    /// canonical mainnet while header and alloc stay identical: such a genesis computes
    /// mainnet's exact genesis block hash (the positive control below), so it could
    /// still join mainnet consensus. Struct equality would wrongly admit it; hash
    /// identity refuses it.
    #[test]
    fn faucet_build_rejects_config_only_divergence_from_mainnet() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let mut divergent = mainnet.clone();
        divergent.config.dao_fork_support = !divergent.config.dao_fork_support;
        assert_ne!(divergent, mainnet);
        assert_eq!(genesis_block_hash(divergent.clone()), genesis_block_hash(mainnet));
        assert!(validate_faucet_build(true, &divergent).is_err());
        Ok(())
    }

    /// The compiled feature set gates the canonical mainnet genesis exactly: refused
    /// when the binary carries the faucet feature and accepted otherwise. This
    /// exercises the same `FAUCET_ENABLED` value the [`NodeCommand::execute`] call site
    /// wires in, under whichever feature set the test build resolved.
    #[test]
    fn compiled_feature_set_gates_canonical_mainnet_genesis() -> eyre::Result<()> {
        let mainnet: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        assert_eq!(validate_faucet_build(FAUCET_ENABLED, &mainnet).is_err(), FAUCET_ENABLED);
        Ok(())
    }
}
