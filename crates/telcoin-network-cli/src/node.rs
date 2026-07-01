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
use tn_reth::{parse_socket_address, RethCommand, RethConfig};
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
                    Config::load_adiri(&tn_datadir, SHORT_VERSION)?
                }
                NamedChain::MainNet => Config::load_mainnet(&tn_datadir, SHORT_VERSION)?,
            }
        } else {
            Config::load(&tn_datadir, SHORT_VERSION)?
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
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            chain: _, // Used above
            metrics,
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
        let builder =
            TnBuilder { node_config, tn_config, metrics, healthcheck, reth_db, exex_fns: vec![] };

        Ok(launcher(builder, ext, tn_datadir, key_config, SHORT_VERSION))
    }
}
