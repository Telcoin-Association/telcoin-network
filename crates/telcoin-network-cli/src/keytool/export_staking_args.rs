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
        let path = if self.node_info.is_dir() {
            self.node_info.join("node-info.yaml")
        } else {
            self.node_info.clone()
        };

        let node_info = Config::load_from_path::<NodeInfo>(&path, ConfigFmt::YAML)?;

        let compressed = node_info.bls_public_key.to_bytes();
        let uncompressed_pk = node_info.bls_public_key.serialize();
        let uncompressed_sig = node_info.proof_of_possession.serialize();

        let proof = ConsensusRegistry::ProofOfPossession {
            uncompressedPubkey: uncompressed_pk.to_vec().into(),
            uncompressedSignature: uncompressed_sig.to_vec().into(),
        };
        let call = ConsensusRegistry::stakeCall {
            blsPubkey: compressed.to_vec().into(),
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

        // resolve path: append node-info.yaml if a directory is given
        let path = if self.node_info.is_dir() {
            self.node_info.join("node-info.yaml")
        } else {
            self.node_info.clone()
        };

        let node_info = Config::load_from_path::<NodeInfo>(&path, ConfigFmt::YAML)?;

        // compressed BLS pubkey (96 bytes) - used as blsPubkey param
        let compressed = node_info.bls_public_key.to_bytes();

        // uncompressed BLS pubkey (192 bytes) - used as ProofOfPossession.uncompressedPubkey
        let uncompressed_pk = node_info.bls_public_key.serialize();

        // uncompressed PoP signature (96 bytes) - used as ProofOfPossession.uncompressedSignature
        let uncompressed_sig = node_info.proof_of_possession.serialize();
        let compressed_hex = format!("0x{}", hex::encode(compressed));
        let uncompressed_pk_hex = format!("0x{}", hex::encode(uncompressed_pk));
        let uncompressed_sig_hex = format!("0x{}", hex::encode(uncompressed_sig));

        if self.json {
            let output = serde_json::json!({
                "blsPubkey": compressed_hex,
                "uncompressedPubkey": uncompressed_pk_hex,
                "uncompressedSignature": uncompressed_sig_hex,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("Staking arguments for ConsensusRegistry.stake():\n");
            println!("blsPubkey (compressed, {} bytes):", compressed.len());
            println!("{compressed_hex}\n");
            println!("proofOfPossession.uncompressedPubkey ({} bytes):", uncompressed_pk.len());
            println!("{uncompressed_pk_hex}\n");
            println!("proofOfPossession.uncompressedSignature ({} bytes):", uncompressed_sig.len());
            println!("{uncompressed_sig_hex}");
        }

        Ok(())
    }
}
