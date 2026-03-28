//! Key command to generate all keys for running a node.

mod export_staking_args;
mod generate;
use self::{export_staking_args::ExportStakingArgs, generate::NodeType};
use clap::{Args, Subcommand};
use eyre::{eyre, Context};

use generate::GenerateKeys;
use std::path::{Path, PathBuf};
use tn_config::TelcoinDirs as _;
use tracing::warn;

/// Generate keypairs and node info to go with them and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct KeyArgs {
    /// Generate command that creates keypairs and writes to file.
    ///
    /// Intentionally leaving this here to help others identify
    /// patterns in clap.
    #[command(subcommand)]
    pub command: KeySubcommand,
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum KeySubcommand {
    /// Generate keys and write to file.
    #[command(name = "generate")]
    Generate(GenerateKeys),

    /// Export hex-encoded staking arguments from node-info.yaml.
    #[command(name = "export-staking-args")]
    ExportStakingArgs(ExportStakingArgs),
}

impl KeyArgs {
    /// Execute command
    pub fn execute(&self, datadir: PathBuf, passphrase: Option<String>) -> eyre::Result<()> {
        match &self.command {
            // generate keys
            KeySubcommand::Generate(args) => {
                let args = match &args.node_type {
                    NodeType::ValidatorKeys(args) => args,
                    NodeType::ObserverKeys(args) => args,
                };
                let authority_key_path = datadir.node_keys_path();
                // initialize path and warn users if overwriting keys
                self.init_path(&authority_key_path, args.force)?;
                // execute and store keypath
                args.execute(&datadir, passphrase)?;
            }
            // export staking args from node-info.yaml (does not use datadir or passphrase)
            KeySubcommand::ExportStakingArgs(args) => {
                args.execute()?;
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
}

#[cfg(test)]
mod tests {
    use super::export_staking_args::ExportStakingArgs;
    use crate::{cli::Cli, NoArgs};
    use clap::Parser;
    use tn_config::{Config, ConfigFmt, ConfigTrait, NodeInfo};
    use tn_types::hex;

    /// Test that generate keys command works.
    /// This test also ensures that confy is able to
    /// load the default config.toml, update the file,
    /// and save it.
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

        tn.run(Some("gen_keys_test".to_string()), |_, _, _, _| tokio::spawn(async { Ok(()) }))
            .expect("generate keys command");

        Config::load_from_path_or_default::<NodeInfo>(
            temp_path.join("node-info.yaml").as_path(),
            ConfigFmt::YAML,
        )
        .expect("config loaded yaml okay");
    }

    /// Test that export-staking-args reads node-info.yaml and produces correct byte lengths.
    #[tokio::test]
    async fn test_export_staking_args() {
        // generate keys in a temp dir
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
        tn.run(Some("export_test".to_string()), |_, _, _, _| tokio::spawn(async { Ok(()) }))
            .expect("generate keys command");

        // load node info and verify byte lengths directly
        let node_info = Config::load_from_path::<NodeInfo>(
            temp_path.join("node-info.yaml").as_path(),
            ConfigFmt::YAML,
        )
        .expect("node info loaded");

        let compressed = node_info.bls_public_key.to_bytes();
        let uncompressed_pk = node_info.bls_public_key.serialize();
        let uncompressed_sig = node_info.proof_of_possession.serialize();

        assert_eq!(compressed.len(), 96, "compressed BLS pubkey should be 96 bytes");
        assert_eq!(uncompressed_pk.len(), 192, "uncompressed BLS pubkey should be 192 bytes");
        assert_eq!(uncompressed_sig.len(), 96, "uncompressed PoP signature should be 96 bytes");

        // verify hex encoding produces valid 0x-prefixed strings
        let compressed_hex = format!("0x{}", hex::encode(compressed));
        let uncompressed_pk_hex = format!("0x{}", hex::encode(uncompressed_pk));
        let uncompressed_sig_hex = format!("0x{}", hex::encode(uncompressed_sig));

        assert!(compressed_hex.starts_with("0x"));
        assert_eq!(compressed_hex.len(), 2 + 96 * 2); // 0x + 96 bytes hex
        assert_eq!(uncompressed_pk_hex.len(), 2 + 192 * 2); // 0x + 192 bytes hex
        assert_eq!(uncompressed_sig_hex.len(), 2 + 96 * 2); // 0x + 96 bytes hex

        // also test that ExportStakingArgs::execute works with directory path
        let args =
            ExportStakingArgs { node_info: temp_path.to_path_buf(), json: false, calldata: false };
        args.execute().expect("export-staking-args with directory path");

        // and with explicit file path
        let args = ExportStakingArgs {
            node_info: temp_path.join("node-info.yaml"),
            json: false,
            calldata: false,
        };
        args.execute().expect("export-staking-args with file path");

        // and JSON output
        let args =
            ExportStakingArgs { node_info: temp_path.to_path_buf(), json: true, calldata: false };
        args.execute().expect("export-staking-args with json output");
    }

    /// Test that --calldata output produces valid ABI-encoded calldata.
    #[tokio::test]
    async fn test_export_staking_args_calldata() {
        // generate keys in a temp dir
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
        tn.run(Some("calldata_test".to_string()), |_, _, _, _| tokio::spawn(async { Ok(()) }))
            .expect("generate keys command");

        let args =
            ExportStakingArgs { node_info: temp_path.to_path_buf(), json: false, calldata: true };
        // execute and verify it doesn't error
        args.execute().expect("export-staking-args with --calldata");
    }

    /// Test mutual exclusivity of output modes via clap parsing.
    #[test]
    fn test_export_staking_args_mutual_exclusivity() {
        // --json and --calldata together should fail to parse
        let result = Cli::<NoArgs>::try_parse_from([
            "telcoin-network",
            "keytool",
            "export-staking-args",
            "--node-info",
            "/tmp/fake",
            "--json",
            "--calldata",
        ]);
        assert!(result.is_err(), "--json and --calldata should be mutually exclusive");
    }
}
