//! Key command to generate all keys for running a full validator node.

mod generate;
use self::generate::NodeType;
use crate::args::clap_genesis_parser;
use clap::{value_parser, Args, Subcommand};
use eyre::{eyre, Context};

use generate::GenerateKeys;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tn_config::{Config, ConfigFmt, ConfigTrait, TelcoinDirs as _};
use tn_reth::{
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    MaybePlatformPath, RethChainSpec,
};
use tracing::{debug, info, warn};

/// Generate keypairs and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct KeyArgs {
    /// Save an encoded keypair (Base64 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

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
        required = false,
    )]
    chain: Arc<RethChainSpec>,

    /// Generate command that creates keypairs and writes to file.
    ///
    /// TODO: rename this key "command".
    /// Intentionally leaving this here to help others identify
    /// patterns in clap.
    #[command(subcommand)]
    pub read_or_write: KeySubcommand,

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
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum KeySubcommand {
    /// Generate keys and write to file.
    #[command(name = "generate")]
    Generate(GenerateKeys),
    /// Read public keys from file.
    #[command(name = "read")]
    Read,
}

/// Read arg that reads from file.
#[derive(Debug, Args)]
pub struct Read; // public keys for: bls, network, execution, address

impl KeyArgs {
    /// Execute command
    pub fn execute(&self, passphrase: Option<String>) -> eyre::Result<()> {
        // create datadir
        let datadir = self.data_dir();
        // creates a default config if none exists
        let mut config = self.load_config()?;

        match &self.read_or_write {
            // generate keys
            KeySubcommand::Generate(args) => {
                match &args.node_type {
                    NodeType::ValidatorKeys(args) => {
                        let authority_key_path = datadir.validator_keys_path();
                        // initialize path and warn users if overwriting keys
                        self.init_path(&authority_key_path, args.force)?;
                        // execute and store keypath
                        args.execute(&mut config, &datadir, passphrase)?;

                        debug!("{config:?}");
                        Config::write_to_path(self.config_path(), config, ConfigFmt::YAML)?;
                    }
                    NodeType::ObserverKeys(args) => {
                        let authority_key_path = datadir.validator_keys_path();
                        // initialize path and warn users if overwriting keys
                        self.init_path(&authority_key_path, args.force)?;
                        // execute and store keypath
                        args.execute(&mut config, &datadir, passphrase)?;

                        debug!("{config:?}");
                        Config::write_to_path(self.config_path(), config, ConfigFmt::YAML)?;
                    }
                }
            }

            // read public key from file
            KeySubcommand::Read => todo!(),
        }

        Ok(())
    }

    /// Ensure the path exists, and if not, create it.
    fn init_path<P: AsRef<Path>>(&self, path: P, force: bool) -> eyre::Result<()> {
        let rpath = path.as_ref();

        // create the dir if it doesn't exist or is empty
        if self.is_key_dir_empty(rpath) {
            // authority dir
            std::fs::create_dir_all(rpath).wrap_err_with(|| {
                format!("Could not create authority key directory {}", rpath.display())
            })?;
        } else if !force {
            warn!("pass `force` to overwrite keys for validator");
            return Err(eyre!("cannot overwrite validator keys without passing --force"));
        }

        Ok(())
    }

    /// Check if key file directory is empty.
    fn is_key_dir_empty<P: AsRef<Path>>(&self, path: P) -> bool {
        let rpath = path.as_ref();

        if !rpath.exists() {
            true
        } else if let Ok(dir) = rpath.read_dir() {
            dir.count() == 0
        } else {
            true
        }
    }

    /// Returns the chain specific path to the data dir.
    fn data_dir(&self) -> DataDirChainPath {
        self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args()).into()
    }

    /// Returns the path to the config file.
    fn config_path(&self) -> PathBuf {
        self.config.clone().unwrap_or_else(|| self.data_dir().node_config_path())
    }

    /// Loads the reth config with the given datadir root
    fn load_config(&self) -> eyre::Result<Config> {
        debug!("loading config...");
        let config_path = self.config_path();
        debug!(?config_path);
        let config =
            Config::load_from_path::<Config>(&config_path, ConfigFmt::YAML).unwrap_or_default();
        debug!("{:?}", config);

        info!(target: "tn::cli", path = ?config_path, "Configuration loaded");

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use crate::{cli::Cli, NoArgs};
    use clap::Parser;
    use tempfile::tempdir;
    use tn_config::{Config, ConfigFmt, ConfigTrait};

    /// Test that generate keys command works.
    /// This test also ensures that confy is able to
    /// load the default config.toml, update the file,
    /// and save it.
    ///
    /// TODO: better unit test for arg methods.
    #[tokio::test]
    async fn test_generate_keypairs() {
        // use tempdir
        let tempdir = tempdir().expect("tempdir created").into_path();
        let tn = Cli::<NoArgs>::try_parse_from([
            "telcoin-network",
            "keytool",
            "generate",
            "validator",
            "--workers",
            "1",
            "--datadir",
            tempdir.to_str().expect("tempdir path clean"),
            "--address",
            "0",
        ])
        .expect("cli parsed");

        tn.run(Some("gen_keys_test".to_string()), |_, _, _, _| Ok(()))
            .expect("generate keys command");

        Config::load_from_path::<Config>(
            tempdir.join("telcoin-network.yaml").as_path(),
            ConfigFmt::YAML,
        )
        .expect("config loaded yaml okay");
    }
}
