//! Key command to generate all keys for running a node.

mod export_staking_args;
mod generate;
mod pop;
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
            KeySubcommand::Generate(args) => match &args.node_type {
                // validator/observer mint fresh keys, so prepare (and guard) the key dir
                NodeType::ValidatorKeys(a) | NodeType::ObserverKeys(a) => {
                    // initialize path and warn users if overwriting keys
                    self.init_path(datadir.node_keys_path(), a.force)?;
                    // execute and store keypath
                    a.execute(&datadir, passphrase)?;
                }
                // pop re-signs against existing keys - never creates or overwrites keys
                NodeType::Pop(a) => a.execute(&datadir, passphrase)?,
            },
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
    use super::{export_staking_args::ExportStakingArgs, pop::PopArgs};
    use crate::{cli::Cli, NoArgs};
    use clap::Parser;
    use tn_config::{Config, ConfigFmt, ConfigTrait, NodeInfo};
    use tn_types::{hex, verify_proof_of_possession_bls, Address};

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

        tn.run(Some("gen_keys_test".to_string()), |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
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
        tn.run(Some("export_test".to_string()), |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
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
        tn.run(Some("calldata_test".to_string()), |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
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

    /// The target devnet execution address used in the `generate pop` tests.
    fn new_test_address() -> Address {
        Address::from_slice(
            &hex::decode("b4E5ED8167873a3CF3C405Aa7155948Db869DBE3").expect("addr hex"),
        )
    }

    /// `generate pop` re-signs the proof of possession for a new execution address
    /// using the node's *existing* keys: the BLS key, p2p info, and name are
    /// unchanged; only `execution_address` and `proof_of_possession` change, and
    /// the new PoP verifies for the new address but not the old one.
    #[tokio::test]
    async fn test_generate_pop() {
        let tempdir = tempfile::TempDir::new().expect("tempdir created");
        let temp_path = tempdir.path();

        // generate base keys + node-info (old execution address = zero address).
        // passphrase `None` -> cleartext keyfile, so `generate pop` (also `None`)
        // can read the same keys back.
        let tn = Cli::<NoArgs>::try_parse_from([
            "telcoin-network",
            "keytool",
            "generate",
            "validator",
            "--datadir",
            temp_path.to_str().expect("tempdir path clean"),
            "--address",
            "0",
        ])
        .expect("cli parsed");
        tn.run(None, |_, _, _, _, _| tokio::spawn(async { Ok(()) }))
            .expect("generate keys command");

        let node_info_path = temp_path.join("node-info.yaml");
        let before = Config::load_from_path::<NodeInfo>(&node_info_path, ConfigFmt::YAML)
            .expect("node info loaded before pop");

        // Re-sign the PoP for a new execution address. Call `execute` directly
        // rather than via a second `run`, to avoid re-initializing global tracing
        // within a single test (mirrors `test_export_staking_args`).
        let new_addr = new_test_address();
        let datadir = temp_path.to_path_buf();
        PopArgs { address: new_addr }.execute(&datadir, None).expect("generate pop");

        let after = Config::load_from_path::<NodeInfo>(&node_info_path, ConfigFmt::YAML)
            .expect("node info loaded after pop");

        // BLS identity, p2p info (network keys + addresses), and name are untouched.
        assert_eq!(before.bls_public_key, after.bls_public_key, "BLS public key must not change");
        assert_eq!(before.p2p_info, after.p2p_info, "p2p info must not change");
        assert_eq!(before.name, after.name, "node name must not change");

        // Execution address and proof of possession are updated.
        assert_eq!(before.execution_address, Address::ZERO, "old address was the zero address");
        assert_eq!(after.execution_address, new_addr, "execution address should be the new addr");
        assert_ne!(
            before.proof_of_possession, after.proof_of_possession,
            "proof of possession must be re-signed"
        );

        // The new PoP verifies for the new address, but not the old one.
        assert!(
            verify_proof_of_possession_bls(
                &after.proof_of_possession,
                &after.bls_public_key,
                &new_addr
            )
            .is_ok(),
            "new PoP must verify for the new execution address"
        );
        assert!(
            verify_proof_of_possession_bls(
                &after.proof_of_possession,
                &after.bls_public_key,
                &Address::ZERO
            )
            .is_err(),
            "new PoP must NOT verify for the old execution address"
        );
    }

    /// `generate pop` errors clearly when keys / node-info are missing, rather
    /// than panicking or silently creating new keys.
    #[tokio::test]
    async fn test_generate_pop_missing_keys_errors() {
        let tempdir = tempfile::TempDir::new().expect("tempdir created");
        let datadir = tempdir.path().to_path_buf();
        let result = PopArgs { address: new_test_address() }.execute(&datadir, None);
        assert!(result.is_err(), "generate pop must error when keys are missing");
    }

    /// The `generate pop` subcommand and its `proof-of-possession` alias are
    /// wired into clap.
    #[test]
    fn test_generate_pop_cli_parses() {
        for name in ["pop", "proof-of-possession"] {
            let parsed = Cli::<NoArgs>::try_parse_from([
                "telcoin-network",
                "keytool",
                "generate",
                name,
                "--datadir",
                "/tmp/does-not-matter",
                "--address",
                "0",
            ]);
            assert!(parsed.is_ok(), "`generate {name}` should parse");
        }
    }
}
