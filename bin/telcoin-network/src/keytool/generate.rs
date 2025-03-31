//! Generate subcommand

use crate::args::{clap_address_parser, clap_genesis_parser};
use clap::{value_parser, Args, Subcommand};
use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tn_config::{Config, KeyConfig, TelcoinDirs};
use tn_node::dirs::DataDirPath;
use tn_types::Address;
use tracing::info;

/// Generate keypairs and save them to a file.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenerateKeys {
    /// Save an encoded keypair (Base64 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub node_type: NodeType,
}

///Subcommand to generate keys for validator, primary, or worker.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeType {
    /// Generate all validator keys and write them to file.
    #[command(name = "validator", alias = "all")]
    ValidatorKeys(ValidatorArgs),
    // primary keys
    // worker key
    // execution key
}

#[derive(Debug, Clone, Args)]
pub struct ValidatorArgs {
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
        global = true,
    )]
    pub chain: Arc<ChainSpec>,

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
        env = "EXECUTION_ADDRESS", // TODO: this doesn't work like it should
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub address: Address,
}

impl ValidatorArgs {
    /// Create all necessary information needed for validator and save to file.
    pub fn execute<TND: TelcoinDirs>(
        &self,
        config: &mut Config,
        tn_datadir: &TND,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");

        let key_config = KeyConfig::generate_and_save(tn_datadir)?;
        let proof = key_config.generate_proof_of_possession_bls(&self.chain)?;
        config.update_protocol_key(key_config.primary_public_key())?;
        config.update_proof_of_possession(proof)?;

        // network keypair for authority
        let network_publickey = key_config.primary_network_public_key();
        config.update_primary_network_key(network_publickey)?;

        // network keypair for workers
        let network_publickey = key_config.worker_network_public_key();
        config.update_worker_network_key(network_publickey)?;

        // add execution address
        config.update_execution_address(self.address)?;

        Ok(())
    }
}
