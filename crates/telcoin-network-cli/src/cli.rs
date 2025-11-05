//! CLI definition and entrypoint to executable
use crate::{
    genesis, keytool, node,
    version::{LONG_VERSION, SHORT_VERSION},
    NoArgs,
};
use clap::{Parser, Subcommand};
use std::{ffi::OsString, fmt, path::PathBuf, str::FromStr};
use tn_node::engine::TnBuilder;
use tn_reth::{dirs::DEFAULT_ROOT_DIR, FileWorkerGuard, LogArgs};

/// How do we want to get the BLS key passphrase?
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum PassSource {
    /// Get the passphrase from then environment variable TN_BLS_PASSPHRASE.
    Env,
    /// Read the passphrase from stdin.  Will read the first line of stdin or until EOF.
    Stdin,
    /// Ask the user on startup, only works if running in foreground on a TTY.
    Ask,
    /// Do not use a passphrase- save the BLS key in the clear.
    /// Only do this for testing, one offs, etc.
    NoPassphrase,
}

impl PassSource {
    /// Use a passphrase to wrap the BLS key.
    pub fn with_passphrase(&self) -> bool {
        !matches!(self, Self::NoPassphrase)
    }
}

/// The main TN cli interface.
///
/// This is the entrypoint to the executable.
#[derive(Debug, Parser)]
#[command(author, version = SHORT_VERSION, long_version = LONG_VERSION, about = "Telcoin Network", long_about = None)]
pub struct Cli<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// The command to run
    #[clap(subcommand)]
    pub command: Commands<Ext>,

    /// How to get the BLS key passphrase.
    ///
    /// The default is to use the env variable TN_BLS_PASSPHRASE
    /// Note, this variable should be securily managed if used on a validator.
    #[arg(
        long,
        value_name = "TN_PASSPHRASE_SOURCE",
        verbatim_doc_comment,
        default_value = "env",
        global = true
    )]
    pub bls_passphrase_source: PassSource,

    /// The path to the data dir for all telcoin-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/telcoin-network/`
    /// - macOS: `$HOME/Library/Application Support/telcoin-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, global = true)]
    pub datadir: Option<PathBuf>,

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
    /// if let Err(err) = telcoin_network::cli::Cli::<MyArgs>::parse()
    ///     .run(None, |builder, _, tn_datadir, passphrase| {
    ///         launch_node(builder, tn_datadir, passphrase)
    ///     })
    /// {
    ///     eprintln!("Error: {err:?}");
    ///     std::process::exit(1);
    /// }
    /// ```
    pub fn run<L>(mut self, passphrase: Option<String>, launcher: L) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder, Ext, PathBuf, Option<String>) -> eyre::Result<()>,
    {
        let datadir: PathBuf = self.datadir.take().unwrap_or_else(|| {
            dirs_next::data_dir().map(|root| root.join(DEFAULT_ROOT_DIR)).unwrap_or_else(|| {
                PathBuf::from_str(&format!("./{DEFAULT_ROOT_DIR}")).expect("data dir")
            })
        });
        // add network name to logs dir
        self.logs.log_file_directory = self.logs.log_file_directory.join("telcoin-network-logs");

        let _guard = self.init_tracing()?;

        match self.command {
            Commands::Genesis(command) => command.execute(datadir),
            Commands::Node(command) => command.execute(datadir, passphrase, launcher),
            Commands::Keytool(command) => command.execute(datadir, passphrase),
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
    Genesis(Box<genesis::GenesisArgs>),

    /// Key management.
    /// Generate or read keys for node management.
    #[command(name = "keytool")]
    Keytool(keytool::KeyArgs),

    /// Start the node
    #[command(name = "node")]
    Node(Box<node::NodeCommand<Ext>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tn_config::Config;
    use tn_reth::ColorMode;

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

        // Create config files or the run() below will fail.
        Config::load_or_default(&temp_dir.path().to_path_buf(), true, "test").unwrap();
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
        assert!(tn.run(None, |_, _, _, _| { Ok(()) }).is_ok());
    }
}
