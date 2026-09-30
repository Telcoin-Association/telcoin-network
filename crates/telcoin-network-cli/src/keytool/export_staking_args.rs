//! Export staking arguments from node-info.yaml for ConsensusRegistry.stake() contract call.

use alloy::sol_types::SolCall;
use clap::Args;
use std::path::PathBuf;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use tn_reth::system_calls::ConsensusRegistry;
use tn_types::hex;

/// Export hex-encoded staking arguments from node-info.yaml for ConsensusRegistry.stake().
///
/// Reads the public node-info.yaml and outputs the BLS public key and proof of possession
/// in the format required by the ConsensusRegistry.stake() contract call.
///
/// This command does NOT require the BLS private key, passphrase, or datadir.
#[derive(Debug, Clone, Args)]
pub struct ExportStakingArgs {
    /// Path to node-info.yaml or its parent directory.
    #[arg(long, value_name = "PATH")]
    pub node_info: PathBuf,

    /// Output as JSON for scripting.
    #[arg(long, default_value_t = false, group = "output_mode")]
    pub json: bool,

    /// Output only the raw ABI-encoded calldata for stake().
    #[arg(long, group = "output_mode")]
    pub calldata: bool,
}

impl ExportStakingArgs {
    /// Return the ABI-encoded calldata for `ConsensusRegistry.stake()`.
    ///
    /// Loads `node-info.yaml` and builds the calldata from the BLS public key
    /// and proof of possession, matching what `--calldata` prints.
    pub fn stake_calldata(&self) -> eyre::Result<Vec<u8>> {
        let node_info = self.load_node_info()?;
        let compressed_pubkey = node_info.bls_public_key.to_bytes();
        let compressed_sig = node_info.proof_of_possession.to_bytes();

        let proof =
            ConsensusRegistry::ProofOfPossession { signature: compressed_sig.to_vec().into() };
        let call = ConsensusRegistry::stakeCall {
            blsPubkey: compressed_pubkey.to_vec().into(),
            proofOfPossession: proof,
        };

        Ok(call.abi_encode())
    }

    /// Execute the export-staking-args command.
    pub fn execute(&self) -> eyre::Result<()> {
        if self.calldata {
            let calldata = self.stake_calldata()?;
            println!("0x{}", hex::encode(calldata));
            return Ok(());
        }

        let node_info = self.load_node_info()?;

        // compressed BLS pubkey (96 bytes) - the blsPubkey param
        let compressed_pubkey = node_info.bls_public_key.to_bytes();

        // compressed PoP signature (48 bytes) - the ProofOfPossession.signature
        let compressed_sig = node_info.proof_of_possession.to_bytes();
        let compressed_pubkey_hex = format!("0x{}", hex::encode(compressed_pubkey));
        let compressed_sig_hex = format!("0x{}", hex::encode(compressed_sig));

        if self.json {
            let output = serde_json::json!({
                "blsPubkey": compressed_pubkey_hex,
                "signature": compressed_sig_hex,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("Staking arguments for ConsensusRegistry.stake():\n");
            println!("blsPubkey (compressed, {} bytes):", compressed_pubkey.len());
            println!("{compressed_pubkey_hex}\n");
            println!("proofOfPossession.signature (compressed, {} bytes):", compressed_sig.len());
            println!("{compressed_sig_hex}");
        }

        Ok(())
    }

    /// Load node info from fs.
    fn load_node_info(&self) -> eyre::Result<NodeInfo> {
        let path = if self.node_info.is_dir() {
            self.node_info.join("node-info.yaml")
        } else {
            self.node_info.clone()
        };
        Config::load_from_path::<NodeInfo>(&path, ConfigFmt::YAML)
    }
}
