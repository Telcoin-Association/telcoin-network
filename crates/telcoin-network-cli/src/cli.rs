//! CLI definition and entrypoint to executable
use crate::{
    db, genesis, keytool, node,
    open_telemetry::init_opentracing_subscriber,
    passphrase::DEFAULT_BLS_PASSPHRASE_CREDENTIAL,
    version::{LONG_VERSION, SHORT_VERSION},
    NoArgs,
};
use clap::{Parser, Subcommand};
use std::{ffi::OsString, fmt, path::PathBuf, str::FromStr};
use tn_config::KeyConfig;
use tn_node::engine::TnBuilder;
use tn_reth::{dirs::DEFAULT_ROOT_DIR, init_reth_defaults, LogArgs};
use tn_types::error::NodeForked;
use tokio::{runtime::Builder, task::JoinHandle};
use tracing::info_span;
use zeroize::Zeroizing;

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
    /// Read the passphrase from the systemd credential named by --bls-passphrase-credential
    /// (LoadCredential= or LoadCredentialEncrypted=). Removes one final \n or \r\n only.
    Credential,
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

    /// Name of the systemd credential holding the BLS key passphrase.
    ///
    /// Used with `--bls-passphrase-source credential`, which reads the file of this name in
    /// $CREDENTIALS_DIRECTORY (set by systemd for LoadCredential= and LoadCredentialEncrypted=).
    /// An empty value selects the default. The name is checked only when the credential source
    /// is selected, so other sources and subcommands ignore it.
    #[arg(
        long,
        value_name = "NAME",
        default_value = DEFAULT_BLS_PASSPHRASE_CREDENTIAL,
        env = "TN_BLS_PASSPHRASE_CREDENTIAL",
        global = true
    )]
    pub bls_passphrase_credential: OsString,

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

impl<Ext: clap::Args + fmt::Debug> Cli<Ext> {
    /// Parsers only the default CLI arguments
    ///
    /// Seeds reth's process-global defaults first: the clap default for
    /// `--txpool.max-account-slots` is resolved while the command is built, so seeding after the
    /// parse would be too late. Prefer this over [`clap::Parser::parse`] for that reason, and note
    /// that it is generic over `Ext` so an extended binary gets the same defaults as `tn` itself.
    pub fn parse_args() -> Self {
        init_reth_defaults();
        Self::parse()
    }

    /// Parsers only the default CLI arguments from the given iterator
    ///
    /// Seeds reth's process-global defaults first, for the same reason as [`Self::parse_args`].
    pub fn try_parse_args_from<I, T>(itr: I) -> Result<Self, clap::error::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        init_reth_defaults();
        Self::try_parse_from(itr)
    }

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
    /// use telcoin_network_cli::{cli::Cli, passphrase::get_bls_passphrase_from_env};
    /// use tn_node::launch_node;
    ///
    /// /// Extra CLI args flattened into the `node` subcommand.
    /// #[derive(Debug, clap::Args)]
    /// pub struct MyArgs {
    ///     /// Example flag consumed by your ExEx.
    ///     #[arg(long)]
    ///     pub enable: bool,
    /// }
    ///
    /// // 1. Read (and clear) TN_BLS_PASSPHRASE before any threads exist.
    /// let preloaded = get_bls_passphrase_from_env();
    /// // `parse_args` rather than `clap::Parser::parse`: it seeds reth's transaction pool
    /// // defaults, which must happen before the clap command resolves its own defaults.
    /// let cli = Cli::<MyArgs>::parse_args();
    /// // 2. Resolve the passphrase for the parsed subcommand.
    /// let passphrase = cli.resolve_bls_passphrase(preloaded)?;
    /// // 3. Run, installing ExExes on the builder before launching the node.
    /// cli.run(passphrase, |mut builder, _args, tn_datadir, key_config, version| {
    ///     builder.install_exex("my-exex", |_ctx| async move { Ok(()) });
    ///     launch_node(builder, tn_datadir, key_config, version)
    /// })?;
    /// # Ok::<(), eyre::Report>(())
    /// ```
    pub fn run<L>(mut self, passphrase: Option<Zeroizing<String>>, launcher: L) -> eyre::Result<()>
    where
        L: FnOnce(TnBuilder, Ext, PathBuf, KeyConfig, &'static str) -> JoinHandle<eyre::Result<()>>,
    {
        let datadir: PathBuf = self.datadir.take().unwrap_or_else(|| {
            dirs_next::data_dir().map(|root| root.join(DEFAULT_ROOT_DIR)).unwrap_or_else(|| {
                PathBuf::from_str(&format!("./{DEFAULT_ROOT_DIR}")).expect("data dir")
            })
        });
        // add network name to logs dir
        self.logs.log_file_directory = self.logs.log_file_directory.join("telcoin-network-logs");

        match self.command {
            Commands::Db(command) => {
                let _guard = self.logs.init_tracing()?;
                command.execute(datadir)
            }
            Commands::Genesis(command) => {
                let _guard = self.logs.init_tracing()?;
                command.execute(datadir)
            }
            Commands::Node(command) => {
                // create key config for lifetime of the app
                // We DO NOT have tracing initialized at this point.
                let key_config = KeyConfig::read_config(&datadir, passphrase)?;

                let runtime = Builder::new_multi_thread()
                    .thread_name("telcoin-network")
                    .enable_io()
                    .enable_time()
                    .build()?;

                let res = runtime.block_on(async move {
                    let name = if let Some(name) = &command.node_name {
                        format!("{}-{}", name, key_config.primary_public_key().to_short_string())
                    } else if let Some(instance) = command.instance {
                        format!(
                            "node-{}-{}",
                            instance,
                            key_config.primary_public_key().to_short_string()
                        )
                    } else {
                        format!("node-{}", key_config.primary_public_key().to_short_string())
                    };
                    let mut layers_guard = init_opentracing_subscriber(
                        &name,
                        self.logs.verbosity.directive(),
                        None, /* Replace with a meter url from cli- leave off until we propertly
                               * test/implement. */
                        command.tracing_url.clone(),
                    );
                    let layers = layers_guard.take_layers();
                    let _guard = self.logs.init_tracing_with_layers(layers)?;
                    let node_join = {
                        let span = info_span!(target: "telcoin", "node-startup");
                        let _span_enter = span.enter();
                        command.execute(datadir, key_config, launcher)?
                    };
                    node_join.await?
                });
                res
            }
            Commands::Keytool(command) => {
                let _guard = self.logs.init_tracing()?;
                command.execute(datadir, passphrase)
            }
        }
    }
}

/// Commands to be executed
#[derive(Debug, Subcommand)]
pub enum Commands<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Inspect, check and repair the execution and consensus databases.
    ///
    /// Every command prints the datadir it acts on first (to stderr); `validate` takes a pack path
    /// instead, and prints that. Every command but `validate` refuses while a node holds the
    /// datadir, and holds it while it runs (on a build with the `mdbx` feature, `stats` reads
    /// beside a running node and `check` refuses). `migrate-execution` and `load-state` write;
    /// `repair`, `migrate` and `reset-execution` are dry runs that write only with `--force`; the
    /// others change no file.
    #[command(name = "db")]
    Db(db::DbCommand),

    /// Genesis ceremony for starting the network.
    #[command(name = "genesis")]
    Genesis(Box<genesis::GenesisArgs>),

    /// Key management.
    /// Generate or read keys for node management.
    #[command(name = "keytool")]
    Keytool(Box<keytool::KeyArgs>),

    /// Start the node
    #[command(name = "node")]
    Node(Box<node::NodeCommand<Ext>>),
}

/// The exit status of a node that stopped because it has forked ([`NodeForked`]): an epoch record
/// it holds differs from the one the network certified. A restart meets the same fork, so a
/// supervisor should not restart the node on this status (systemd: `RestartPreventExitStatus=10`).
///
/// 10 means nothing else: systemd names 1 to 7 and 64 to 78, clap exits 2 on a usage error, a Rust
/// panic exits 101, and 126 and above belong to the shell and to signals.
pub const FORKED_EXIT_STATUS: i32 = 10;

/// The process exit status for an error [`Cli::run`] returned: [`FORKED_EXIT_STATUS`] for a fork
/// stop ([`NodeForked::is_in`]), 1 for any other error.
pub fn exit_status(error: &eyre::Report) -> i32 {
    if NodeForked::is_in(error) {
        FORKED_EXIT_STATUS
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tn_config::Config;
    use tn_reth::ColorMode;
    use tn_types::{TaskError, TaskJoinError};

    #[test]
    fn parse_color_mode() {
        let tn = Cli::<NoArgs>::try_parse_args_from(["tn", "node", "--color", "always"]).unwrap();
        assert_eq!(tn.logs.color, ColorMode::Always);
    }

    /// A node that stopped because it has forked exits with [`FORKED_EXIT_STATUS`], however the
    /// fork reached the exit error; any other error exits 1, a message that reads like a fork
    /// included.
    #[test]
    fn exit_status_tells_a_fork_stop_by_its_type() {
        // the epoch vote round's fork, through the critical collector and the node's task manager
        let round = eyre::Report::new(NodeForked::new(
            "epoch 3: the network certified epoch record a where this node computed b; this node \
             has forked",
        ));
        let collector = TaskJoinError::CriticalExitError(
            "Epoch Vote Collector".to_string(),
            TaskError::from_report(round),
        );
        let exit = collector.to_report("Node task shutdown");
        assert_eq!(exit_status(&exit), FORKED_EXIT_STATUS, "{exit:?}");
        assert_eq!(
            exit.to_string(),
            "Node task shutdown: Critical task Epoch Vote Collector has exited unexpectedly: epoch \
             3: the network certified epoch record a where this node computed b; this node has \
             forked",
        );

        // the epoch record sync's conflict: the fork is the context over the store's refusal, and
        // the report reaches the exit as it is or under more context
        let sync = eyre::Report::new(std::io::Error::other("conflicting record"))
            .wrap_err(NodeForked::new("epoch 3: this node's execution diverged"));
        assert_eq!(exit_status(&sync), FORKED_EXIT_STATUS, "{sync:?}");
        let collector = TaskJoinError::CriticalExitError(
            "Epoch Record Collector".to_string(),
            TaskError::from_report(sync.wrap_err("startup")),
        );
        let exit = collector.to_report("a node task failed as the node stopped");
        assert_eq!(exit_status(&exit), FORKED_EXIT_STATUS, "{exit:?}");

        let other = TaskJoinError::CriticalExitError(
            "consensus engine".to_string(),
            TaskError::from_report(eyre::eyre!("this node has forked")),
        );
        assert_eq!(exit_status(&other.to_report("Node task shutdown")), 1);
        assert_eq!(exit_status(&eyre::eyre!("failed to persist")), 1);
    }

    #[test]
    fn parse_db_stats_subcommand() {
        let _ = Cli::<NoArgs>::try_parse_args_from(["tn", "db", "stats"]).expect("cli parsed");
    }

    /// Tests that the help message is parsed correctly. This ensures that clap args are configured
    /// correctly and no conflicts are introduced via attributes that would result in a panic at
    /// runtime
    #[test]
    fn test_parse_help_all_subcommands() {
        let tn = Cli::<NoArgs>::command();
        for sub_command in tn.get_subcommands() {
            let err = Cli::<NoArgs>::try_parse_args_from(["tn", sub_command.get_name(), "--help"])
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
        let tn = Cli::<NoArgs>::try_parse_args_from(["tn", "node"]).unwrap();
        let log_dir = tn.logs.log_file_directory;

        // let end = format!("{}/logs", DEFAULT_ROOT_DIR);

        let end = "reth/logs".to_string();
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
    }

    #[test]
    fn parse_env_filter_directives() {
        // held to the end: `set_var` below and `run` reading `RUST_LOG` both touch the
        // process environment
        let _env = crate::passphrase::env_lock();
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a key file or the tested command will fail.
        let temp_path = temp_dir.path();
        let tn = Cli::<NoArgs>::try_parse_from([
            "telcoin-network",
            "keytool",
            "generate",
            "validator",
            "--workers",
            "1",
            "--datadir",
            temp_path.to_str().expect("tempdir path clean"),
            "--address",
            "0",
        ])
        .expect("cli parsed");
        tn.run(None, |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
            .expect("generate keys command");

        // Create config files or the run() below will fail.
        Config::load_or_default(&temp_dir.path().to_path_buf(), "test")
            .expect("default config files written to tempdir");
        std::env::set_var("RUST_LOG", "info,evm=debug");
        // the default config is a test chain, so a faucet build needs the opt-in to start
        let tn = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "node",
            "--datadir",
            temp_dir.path().to_str().unwrap(),
            "--log.file.filter",
            "debug,net=trace",
            "--allow-faucet-on-chain",
        ])
        .unwrap();
        // run() will create a tokio runtime so we can use tokio::spawn but need to use a normal
        // (non-tokio) test
        assert!(tn.run(None, |_, _, _, _, _| { tokio::spawn(async { Ok(()) }) }).is_ok());
    }
}
