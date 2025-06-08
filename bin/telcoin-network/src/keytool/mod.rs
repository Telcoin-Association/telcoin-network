//! Key command to generate all keys for running a node.

mod generate;
use self::generate::NodeType;
use clap::{Args, Subcommand};
use eyre::{eyre, Context};

use generate::GenerateKeys;
use std::path::Path;
use tn_config::{Config, ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs as _};
use tn_reth::{
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    MaybePlatformPath, RethChainSpec,
};
use tracing::warn;

/// Generate keypairs and node info to go with them and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct KeyArgs {
    /// Save an encoded keypair (Base58 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// Generate command that creates keypairs and writes to file.
    ///
    /// TODO: rename this key "command".
    /// Intentionally leaving this here to help others identify
    /// patterns in clap.
    #[command(subcommand)]
    pub read_or_write: KeySubcommand,
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum KeySubcommand {
    /// Generate keys and write to file.
    #[command(name = "generate")]
    Generate(GenerateKeys),
}

impl KeyArgs {
    /// Execute command
    pub fn execute(&self, passphrase: Option<String>) -> eyre::Result<()> {
        // create datadir
        let datadir = self.data_dir();

        match &self.read_or_write {
            // generate keys
            KeySubcommand::Generate(args) => {
                let args = match &args.node_type {
                    NodeType::ValidatorKeys(args) => args,
                    NodeType::ObserverKeys(args) => args,
                };
                let authority_key_path = datadir.node_keys_path();
                // initialize path and warn users if overwriting keys
                self.init_path(&authority_key_path, args.force)?;
                let mut node_info = NodeInfo::default();
                // execute and store keypath
                args.execute(&mut node_info, &datadir, passphrase)?;

                Config::write_to_path(datadir.node_info_path(), &node_info, ConfigFmt::YAML)?;
            }
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
            warn!("pass `force` to overwrite keys for node");
            return Err(eyre!("cannot overwrite node keys without passing --force"));
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
        self.datadir
            .unwrap_or_chain_default(RethChainSpec::default().chain, default_datadir_args())
            .into()
    }
}

#[cfg(test)]
mod tests {
    use crate::{cli::Cli, NoArgs};
    use clap::Parser;
    use tn_config::{Config, ConfigFmt, ConfigTrait, NodeInfo};

    /// Test that generate keys command works.
    /// This test also ensures that confy is able to
    /// load the default config.toml, update the file,
    /// and save it.
    ///
    /// TODO: better unit test for arg methods.
    #[tokio::test]
    async fn test_generate_keypairs() {
        // use tempdir
        let tempdir = tempfile::TempDir::new().expect("tempdir created");
        let temp_path = tempdir.path();
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

        tn.run(Some("gen_keys_test".to_string()), |_, _, _, _| Ok(()))
            .expect("generate keys command");

        Config::load_from_path_or_default::<NodeInfo>(
            temp_path.join("node-info.yaml").as_path(),
            ConfigFmt::YAML,
        )
        .expect("config loaded yaml okay");
    }
}
