//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Parser};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, thread::available_parallelism};
use tn_config::{Config, ConfigFmt, ConfigTrait, TelcoinDirs as _};
use tn_node::engine::TnBuilder;
use tn_reth::{
    clap_genesis_parser,
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    parse_socket_address, MaybePlatformPath, RethChainSpec, RethCommand, RethConfig,
};
use tracing::*;

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// Overwrite the chain this node is running.
    ///
    /// The value parser matches either a known chain, the path
    /// to a json file, or a json formatted string in-memory. The json can be either
    /// a serialized [ChainSpec] or Genesis struct.
    #[arg(
        long,
        value_name = "GENESIS_OR_PATH",
        verbatim_doc_comment,
        value_parser = clap_genesis_parser,
    )]
    pub genesis: Option<Arc<RethChainSpec>>,

    /// Enable Prometheus consensus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Consensus Metrics")]
    pub consensus_metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - DISCOVERY_PORT: default + `instance` - 1
    /// - AUTH_PORT: default + `instance` * 100 - 100
    /// - HTTP_RPC_PORT: default - `instance` + 1
    /// - WS_RPC_PORT: default + `instance` * 2 - 2
    #[arg(long, value_name = "INSTANCE", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=200))]
    pub instance: u16,

    /// Is this an observer node?  True if set, an observer will never be in the committee
    /// but will follow consensus and provide node RPC access.
    #[arg(long, value_name = "OBSERVER", global = true, default_value_t = false)]
    pub observer: bool,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    /// The path to the data dir for all telcoin-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/telcoin-network/`
    /// - macOS: `$HOME/Library/Application Support/telcoin-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// Additional reth arguments
    #[clap(flatten)]
    pub reth: RethCommand,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext,
}

impl<Ext: clap::Args + fmt::Debug> NodeCommand<Ext> {
    /// Execute `node` command
    #[instrument(level = "info", skip_all)]
    pub fn execute<L>(
        mut self,
        passphrase: Option<String>,
        load_config: bool, /* If false will not attempt to load a previously saved config-
                            * useful for testing. */
        launcher: L,
    ) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder, Ext, DataDirChainPath, Option<String>) -> eyre::Result<()>,
    {
        info!(target: "tn::cli", "telcoin-network {} starting", SHORT_VERSION);

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit()?;

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
            error!("Failed to initialize global thread pool for rayon: {}", err)
        }

        // use TN-specific datadir for finding tn-config
        let default_args = default_datadir_args();
        let tn_datadir: DataDirChainPath = self
            .datadir
            .unwrap_or_chain_default(self.reth.chain.chain, default_args.clone())
            .into();

        // use config for chain spec
        let config_path = self.config.clone().unwrap_or(tn_datadir.node_config_path());
        let mut tn_config: Config = Config::load_from_path(&config_path, ConfigFmt::YAML)?;
        debug!(target: "cli", validator = ?tn_config.validator_info.name, "config path for node command: {config_path:?}");
        debug!(target: "cli", validator = ?tn_config.validator_info.name, "tn datadir for node command: {tn_datadir:?}");

        if load_config {
            // Make sure we are using the chain from config not just the default.
            self.reth.chain = Arc::new(tn_config.chain_spec());
            info!(target: "cli", validator = ?tn_config.validator_info.name, "config loaded");
        }

        // overwrite all genesis if `genesis` was passed to CLI
        if let Some(chain) = self.genesis.take() {
            info!(target: "cli", ?chain, "Overwriting TN config with specified chain");
            self.reth.chain = chain;
        }

        // get the worker's transaction address from the config
        let Self {
            datadir: _, // Used above
            config: _,  // Used above
            genesis: _, // Used above
            consensus_metrics,
            instance,
            with_unused_ports,
            reth,
            ext,
            observer,
        } = self;

        tn_config.observer = observer; // Set observer mode from the config.

        info!(target: "cli", "node command genesis: {:#?}", reth.chain.genesis());

        // set up reth node config for engine components
        let node_config = RethConfig::new(
            reth,
            instance,
            Some(config_path),
            tn_datadir.as_ref(),
            with_unused_ports,
        );

        let builder =
            TnBuilder { node_config, tn_config, opt_faucet_args: None, consensus_metrics };

        launcher(builder, ext, tn_datadir, passphrase)
    }
}
