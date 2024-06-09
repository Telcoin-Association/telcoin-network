//! Main node command
//!
//! Starts the client
use crate::{args::clap_genesis_parser, version::SHORT_VERSION};
use clap::{value_parser, Parser};
use core::fmt;
use fdlimit::raise_fd_limit;
use futures::Future;
use reth::{
    args::{
        utils::parse_socket_address, DatabaseArgs, DatadirArgs, DebugArgs, DevArgs, NetworkArgs,
        PayloadBuilderArgs, PruningArgs, RpcServerArgs, TxPoolArgs,
    },
    builder::NodeConfig,
    commands::node::NoArgs,
    dirs::{ChainPath, MaybePlatformPath},
    CliContext,
};
use reth_db::{init_db, DatabaseEnv};
use reth_primitives::ChainSpec;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tn_config::{traits::ConfigTrait, Config};
use tn_node::{
    dirs::{default_datadir_args, DataDirPath, TelcoinDirs as _},
    engine::TnBuilder,
};
use tracing::*;

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Defaults to the custom
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        default_value_if("dev", "true", "adiri"),
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<ChainSpec>,

    /// Enable Prometheus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Metrics")]
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
    /// - DISCOVERY_PORT: default + `instance` - 1
    /// - AUTH_PORT: default + `instance` * 100 - 100
    /// - HTTP_RPC_PORT: default - `instance` + 1
    /// - WS_RPC_PORT: default + `instance` * 2 - 2
    #[arg(long, value_name = "INSTANCE", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=200))]
    pub instance: u16,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    // TODO: this is painful to maintain
    // need a better way to overwrite reth DataDirPath
    /// The path to the data dir for all telcoin-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/telcoin-network/`
    /// - macOS: `$HOME/Library/Application Support/telcoin-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// All networking related arguments
    #[clap(flatten)]
    pub network: NetworkArgs,

    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All payload builder related arguments
    #[clap(flatten)]
    pub builder: PayloadBuilderArgs,

    /// All debug related arguments with --debug prefix
    #[clap(flatten)]
    pub debug: DebugArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,

    /// All dev related arguments with --dev prefix
    #[clap(flatten)]
    pub dev: DevArgs,

    /// All pruning related arguments
    #[clap(flatten)]
    pub pruning: PruningArgs,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext,
}

impl<Ext: clap::Args + fmt::Debug> NodeCommand<Ext> {
    /// Execute `node` command
    pub async fn execute<L, Fut>(self, ctx: CliContext, launcher: L) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder<Arc<DatabaseEnv>>, Ext, ChainPath<DataDirPath>) -> Fut,
        Fut: Future<Output = eyre::Result<()>>,
    {
        info!(target: "tn::cli", "telcoin-network {} starting", SHORT_VERSION);

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit()?;

        // use TN-specific datadir for finding tn-config
        let default_args = default_datadir_args();
        let tn_datadir =
            self.datadir.unwrap_or_chain_default(self.chain.chain, default_args.clone());

        // TODO: use config or CLI chain spec?
        let config_path = self.config.clone().unwrap_or(tn_datadir.node_config_path());
        let tn_config: Config = Config::load_from_path(config_path)?;
        info!(target: "telcoin::cli", validator = ?tn_config.validator_info.name, "config loaded");

        // get the worker's transaction address from the config
        let Self {
            // datadir
            // config,
            chain,
            metrics,
            instance,
            with_unused_ports,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ext,
            ..
        } = self;

        // create a reth DatadirArgs from tn datadir
        let datadir = DatadirArgs {
            datadir: MaybePlatformPath::from(PathBuf::from(tn_datadir.clone())),
            static_files_path: None,
        };

        // set up reth node config for engine components
        let mut node_config = NodeConfig {
            config: self.config,
            chain,
            metrics,
            instance,
            datadir,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
        };

        if with_unused_ports {
            node_config = node_config.with_unused_ports();
        }

        // create node builders for Primary and Worker
        //
        // Register the prometheus recorder before creating the database,
        // because database init needs it to register metrics.
        let _ = node_config.install_prometheus_recorder()?;

        let db_path = tn_datadir.db();
        info!(target: "tn::engine", path = ?db_path, "opening database");
        let database =
            Arc::new(init_db(db_path.clone(), node_config.db.database_args())?.with_metrics());

        // TODO: temporary solution until upstream reth supports public rpc hooks
        let builder = TnBuilder {
            database,
            node_config,
            task_executor: ctx.task_executor,
            tn_config,
            opt_faucet_args: None,
        };

        launcher(builder, ext, tn_datadir).await
    }
}
