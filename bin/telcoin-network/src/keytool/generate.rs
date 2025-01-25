//! Generate subcommand

use crate::args::{clap_address_parser, clap_genesis_parser};
use alloy::primitives::Address;
use clap::{value_parser, Args, Subcommand};
use fastcrypto::traits::KeyPair as KeyPairTraits;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use std::{path::Path, sync::Arc};
use tn_config::{Config, BLS_KEYFILE, PRIMARY_NETWORK_KEYFILE, WORKER_NETWORK_KEYFILE};
use tn_node::dirs::DataDirPath;
use tn_types::{generate_proof_of_possession_bls, BlsKeypair, NetworkKeypair};
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
    pub async fn execute(
        &self,
        authority_key_path: &Path,
        config: &mut Config,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");

        // bls keypair for consensus - drop after write to zeroize memory
        let bls_keypair = self.generate_keypair_from_rng::<BlsKeypair>()?;
        self.write_keypair_to_file(&bls_keypair, authority_key_path.join(BLS_KEYFILE))?;
        let proof = generate_proof_of_possession_bls(&bls_keypair, &self.chain)?;
        config.update_protocol_key(bls_keypair.public().clone())?;
        config.update_proof_of_possession(proof)?;
        drop(bls_keypair); // calls zeroize() for OnceCell containing private key

        // network keypair for authority
        let network_keypair = self.generate_keypair_from_rng::<NetworkKeypair>()?;
        self.write_keypair_to_file(
            &network_keypair,
            authority_key_path.join(PRIMARY_NETWORK_KEYFILE),
        )?;
        config.update_primary_network_key(network_keypair.public().clone())?;
        drop(network_keypair); // calls zeroize() for OnceCell containing private key

        // network keypair for workers
        let network_keypair = self.generate_keypair_from_rng::<NetworkKeypair>()?;
        self.write_keypair_to_file(
            &network_keypair,
            authority_key_path.join(WORKER_NETWORK_KEYFILE),
        )?;
        config.update_worker_network_key(network_keypair.public().clone())?;
        drop(network_keypair); // calls zeroize() for OnceCell containing private key

        // add execution address
        config.update_execution_address(self.address)?;

        Ok(())
    }

    /// Generate a keypair from a ChaCha20 RNG.
    ///
    /// based on: https://rust-random.github.io/book/guide-seeding.html
    ///
    /// TODO: discuss with @Utku
    pub fn generate_keypair_from_rng<KP>(&self) -> eyre::Result<KP>
    where
        // R: rand::CryptoRng + rand::RngCore,
        KP: KeyPairTraits,
        // <KP as KeyPairTraits>::PubKey: SuiPublicKey,
    {
        // this is expensive
        let rng = ChaCha20Rng::from_entropy();
        // note: StdRng uses ChaCha12
        let kp = KP::generate(&mut StdRng::from_rng(rng)?);
        Ok(kp)
    }

    /// Write Base64 encoded `privkey` to file.
    pub fn write_keypair_to_file<KP, P>(&self, keypair: &KP, path: P) -> eyre::Result<()>
    where
        KP: KeyPairTraits,
        P: AsRef<Path>,
    {
        let contents = keypair.encode_base64();
        std::fs::write(path, contents)?;
        Ok(())
    }
}
