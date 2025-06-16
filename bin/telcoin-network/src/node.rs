//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Parser};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, thread::available_parallelism};
use tn_config::Config;
use tn_node::engine::TnBuilder;
use tn_reth::{parse_socket_address, RethCommand, RethConfig};
use tracing::*;

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

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

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
        tn_datadir: PathBuf,
        passphrase: Option<String>,
        launcher: L,
    ) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder, Ext, PathBuf, Option<String>) -> eyre::Result<()>,
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
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            chain: _,    // Used above
            observer: _, // Used above
            metrics,
            instance,
            with_unused_ports,
            reth,
            ext,
        } = self;

        debug!(target: "cli", "node command genesis: {:#?}", tn_config.genesis());

        // set up reth node config for engine components
        let node_config = RethConfig::new(
            reth,
            instance,
            &tn_datadir,
            with_unused_ports,
            Arc::new(tn_config.chain_spec()),
        );

        let builder = TnBuilder { node_config, tn_config, opt_faucet_args: None, metrics };

        launcher(builder, ext, tn_datadir, passphrase)
    }
}
