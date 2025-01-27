//! CLI definition and entrypoint to executable
use crate::{
    args::clap_genesis_parser,
    genesis, keytool, node,
    version::{LONG_VERSION, SHORT_VERSION},
};
use clap::{value_parser, Parser, Subcommand};
use futures::Future;
use reth_chainspec::ChainSpec;
use reth_cli_commands::node::NoArgs;
use reth_db::DatabaseEnv;
use reth_node_core::args::LogArgs;
use reth_tracing::FileWorkerGuard;
use std::{ffi::OsString, fmt, sync::Arc};
use tn_node::{dirs::DataDirChainPath, engine::TnBuilder};

/// The main TN cli interface.
///
/// This is the entrypoint to the executable.
#[derive(Debug, Parser)]
#[command(author, version = SHORT_VERSION, long_version = LONG_VERSION, about = "Telcoin Network", long_about = None)]
pub struct Cli<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The command to run
    #[clap(subcommand)]
    pub command: Commands<Ext>,

    /// The chain this node is running.
    ///
    /// The value parser matches either a known chain, the path
    /// to a json file, or a json formatted string in-memory. The json can be either
    /// a serialized [ChainSpec] or Genesis struct.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        value_parser = clap_genesis_parser,
        global = true,
    )]
    pub chain: Arc<ChainSpec>,

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

    /// The log configuration.
    #[clap(flatten)]
    pub logs: LogArgs,
}

impl Cli {
    /// Parsers only the default CLI arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Parsers only the default CLI arguments from the given iterator
    pub fn try_parse_args_from<I, T>(itr: I) -> Result<Self, clap::error::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        Cli::try_parse_from(itr)
    }
}

impl<Ext: clap::Args + fmt::Debug> Cli<Ext> {
    /// Execute the configured cli command.
    ///
    /// This accepts a closure that is used to launch the node via the
    /// [NodeCommand](node::NodeCommand).
    ///
    ///
    /// # Example
    ///
    /// ```no_run
    /// use reth::cli::Cli;
    /// use reth_node_ethereum::EthereumNode;
    ///
    /// Cli::parse_args()
    ///     .run(|builder, _| async move {
    ///         let handle = builder.launch_node(EthereumNode::default()).await?;
    ///
    ///         handle.wait_for_node_exit().await
    ///     })
    ///     .unwrap();
    /// ```
    ///
    /// # Example
    ///
    /// Parse additional CLI arguments for the node command and use it to configure the node.
    ///
    /// ```no_run
    /// use clap::Parser;
    /// use telcoin_network::cli::Cli;
    /// use tn_node::launch_node;
    ///
    /// #[derive(Debug, Parser)]
    /// pub struct MyArgs {
    ///     pub enable: bool,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     if let Err(err) = telcoin_network::cli::Cli::<MyArgs>::parse()
    ///         .run(|builder, _, tn_datadir| async move { launch_node(builder, tn_datadir).await })
    ///         .await
    ///     {
    ///         eprintln!("Error: {err:?}");
    ///         std::process::exit(1);
    ///     }
    /// }
    /// ```
    pub async fn run<L, Fut>(mut self, launcher: L) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder<Arc<DatabaseEnv>>, Ext, DataDirChainPath) -> Fut,
        Fut: Future<Output = eyre::Result<()>>,
    {
        // add network name to logs dir
        self.logs.log_file_directory =
            self.logs.log_file_directory.join(self.chain.chain.to_string());

        let _guard = self.init_tracing()?;

        match self.command {
            Commands::Genesis(command) => command.execute().await,
            Commands::Node(command) => command.execute(true, launcher).await,
            Commands::Keytool(command) => command.execute().await,
        }
    }

    /// Initializes tracing with the configured options.
    ///
    /// If file logging is enabled, this function returns a guard that must be kept alive to ensure
    /// that all logs are flushed to disk.
    pub fn init_tracing(&self) -> eyre::Result<Option<FileWorkerGuard>> {
        let guard = self.logs.init_tracing()?;
        Ok(guard)
    }
}

/// Commands to be executed
#[derive(Debug, Subcommand)]
pub enum Commands<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Genesis ceremony for starting the network.
    #[command(name = "genesis")]
    Genesis(genesis::GenesisArgs),

    /// Key management.
    /// Generate or read keys for node management.
    #[command(name = "keytool")]
    Keytool(keytool::KeyArgs),

    /// Start the node
    #[command(name = "node")]
    Node(node::NodeCommand<Ext>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use reth::args::ColorMode;

    #[test]
    fn parse_color_mode() {
        let tn = Cli::try_parse_args_from(["tn", "node", "--color", "always"]).unwrap();
        assert_eq!(tn.logs.color, ColorMode::Always);
    }

    /// Tests that the help message is parsed correctly. This ensures that clap args are configured
    /// correctly and no conflicts are introduced via attributes that would result in a panic at
    /// runtime
    #[test]
    fn test_parse_help_all_subcommands() {
        let tn = Cli::<NoArgs>::command();
        for sub_command in tn.get_subcommands() {
            let err = Cli::try_parse_args_from(["tn", sub_command.get_name(), "--help"])
                .err()
                .unwrap_or_else(|| {
                    panic!("Failed to parse help message {}", sub_command.get_name())
                });

            // --help is treated as error, but
            // > Not a true "error" as it means --help or similar was used. The help message will be sent to stdout.
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
        }
    }

    /// Tests that the log directory is parsed correctly.
    ///
    /// TODO: logs should be placed in TN DEFAULT_ROOT_DIR
    #[test]
    fn parse_logs_path() {
        let tn = Cli::try_parse_args_from(["tn", "node"]).unwrap();
        let log_dir = tn.logs.log_file_directory;

        // let end = format!("{}/logs", DEFAULT_ROOT_DIR);

        let end = "reth/logs".to_string();
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
    }

    #[tokio::test]
    async fn parse_env_filter_directives() {
        let temp_dir = tempfile::tempdir().unwrap();

        std::env::set_var("RUST_LOG", "info,evm=debug");
        let tn = Cli::try_parse_args_from([
            "tn",
            "node",
            "--datadir",
            temp_dir.path().to_str().unwrap(),
            "--log.file.filter",
            "debug,net=trace",
        ])
        .unwrap();
        assert!(tn.run(|_, _, _| async move { Ok(()) }).await.is_ok());
    }
}
