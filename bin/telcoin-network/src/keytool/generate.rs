//! Generate subcommand

use crate::args::clap_address_parser;
use clap::{value_parser, Args, Subcommand};
use tn_config::{KeyConfig, TelcoinDirs, ValidatorInfo};
use tn_types::Address;
use tracing::info;

/// Generate keypairs and save them to a file.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenerateKeys {
    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub node_type: NodeType,
}

///Subcommand to generate keys for validator, primary, or worker.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeType {
    /// Generate all validator keys and write them to file.
    #[command(name = "validator", alias = "all")]
    ValidatorKeys(KeygenArgs),
    /// Generate all observer (non-validator) keys and write them to file.
    #[command(name = "observer")]
    ObserverKeys(KeygenArgs),
}

#[derive(Debug, Clone, Args)]
pub struct KeygenArgs {
    /// The number of workers for the primary.
    #[arg(long, value_name = "workers", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=4))]
    pub workers: u16,

    /// Overwrite existing keys, if present.
    ///
    /// Warning: Existing keys will be lost.
    #[arg(
        long = "force",
        alias = "overwrite",
        help_heading = "Overwrite existing keys. Warning: existing keys will be lost.",
        verbatim_doc_comment
    )]
    pub force: bool,

    /// The address for suggested fee recipient.
    ///
    /// The execution layer address, derived from `secp256k1` keypair.
    /// The validator uses this address when producing batches and blocks.
    /// Validators can pass "0" to use the zero address.
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "address",
        alias = "execution-address",
        help_heading = "The address that should receive block rewards. Pass `0` to use the zero address.",
        env = "EXECUTION_ADDRESS",
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub address: Address,
}

impl KeygenArgs {
    fn update_keys<TND: TelcoinDirs>(
        &self,
        validator_info: &mut ValidatorInfo,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        let key_config = KeyConfig::generate_and_save(tn_datadir, passphrase)?;
        let proof = key_config.generate_proof_of_possession_bls(&self.address)?;
        validator_info.bls_public_key = key_config.primary_public_key();
        validator_info.proof_of_possession = proof;

        // network keypair for authority
        let network_publickey = key_config.primary_network_public_key();
        validator_info.primary_info.network_key = network_publickey;

        // network keypair for workers
        let network_publickey = key_config.worker_network_public_key();
        validator_info.primary_info.worker_network_key = network_publickey.clone();
        for worker in validator_info.primary_info.worker_index.0.iter_mut() {
            worker.1.name = network_publickey.clone();
        }
        Ok(())
    }

    /// Create all necessary information needed for validator and save to file.
    pub fn execute<TND: TelcoinDirs>(
        &self,
        validator_info: &mut ValidatorInfo,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");

        self.update_keys(validator_info, tn_datadir, passphrase)?;

        // add execution address
        validator_info.execution_address = self.address;

        Ok(())
    }
}
