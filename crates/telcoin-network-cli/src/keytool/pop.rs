//! `generate pop` subcommand: re-sign the proof of possession for a new
//! execution address using the node's existing BLS keys.
//!
//! The proof of possession is a BLS signature over the BLS public key bound to
//! the execution address. Because the address is part of the signed message,
//! rotating a node to a new execution address requires re-signing the PoP -
//! the on-chain `stake()` call verifies the PoP against the staking address.
//!
//! Unlike `generate validator|observer`, this never mints a new BLS keypair: it
//! loads the existing keys and node-info, re-signs against the new address, and
//! rewrites only the PoP-derived fields. BLS key, network keys, p2p peer IDs,
//! and (deterministically derived) name are unchanged.

use crate::{args::clap_address_parser, keytool::generate::set_proof_of_possession};
use clap::Args;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NodeInfo, TelcoinDirs};
use tn_types::Address;
use tracing::{info, warn};

/// Re-sign the proof of possession for a new execution address.
///
/// Loads the node's existing BLS keys and `node-info.yaml`, re-signs the proof
/// of possession for `--address`, and writes the new `proof_of_possession` and
/// `execution_address` back to `node-info.yaml`. No keys are generated or
/// overwritten.
#[derive(Debug, Clone, Args)]
pub struct PopArgs {
    /// The new execution address to bind the proof of possession to.
    ///
    /// The execution layer address that should receive block rewards.
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "address",
        alias = "execution-address",
        help_heading = "The execution address to re-sign the proof of possession for.",
        env = "EXECUTION_ADDRESS",
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub address: Address,
}

impl PopArgs {
    /// Re-sign the proof of possession against the existing keys and rewrite
    /// the PoP-derived fields in `node-info.yaml`.
    pub fn execute<TND: TelcoinDirs>(
        &self,
        dir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        // existing keys - never generated here; error with a hint if absent
        let key_config = KeyConfig::read_config(dir, passphrase).map_err(|e| {
            eyre::eyre!(
                "could not load existing BLS keys from {}: {e}\n\
                 hint: generate keys first with `keytool generate validator|observer`",
                dir.node_keys_path().display()
            )
        })?;

        // existing node-info - preserves p2p info / network keys; error if absent
        let mut node_info =
            Config::load_from_path::<NodeInfo>(dir.node_info_path(), ConfigFmt::YAML).map_err(
                |e| {
                    eyre::eyre!(
                        "could not load node-info.yaml at {}: {e}\n\
                         hint: generate keys first with `keytool generate validator|observer`",
                        dir.node_info_path().display()
                    )
                },
            )?;

        if node_info.execution_address != self.address {
            warn!(target: "tn::keytool", old = %node_info.execution_address,
                new = %self.address, "replacing execution address and re-signing proof of possession");
        } else {
            info!(target: "tn::keytool", address = %self.address,
                "re-signing proof of possession (execution address unchanged)");
        }

        // re-sign against the existing keys (shared with `generate validator|observer`)
        set_proof_of_possession(&mut node_info, &key_config, self.address)?;
        Config::write_to_path(dir.node_info_path(), &node_info, ConfigFmt::YAML)?;

        // PoP is printed in the same bs58 encoding `node-info.yaml` uses (BlsSignature Display).
        println!("OK  node-info.yaml updated: {}", dir.node_info_path().display());
        println!("    execution_address:   {}", node_info.execution_address);
        println!("    proof_of_possession: {}", node_info.proof_of_possession);
        println!(
            "Next: telcoin keytool export-staking-args --node-info {}",
            dir.node_info_path().display()
        );
        Ok(())
    }
}
