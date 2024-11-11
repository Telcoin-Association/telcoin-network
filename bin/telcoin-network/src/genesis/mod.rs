//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

mod add_validator;
mod create_committee;
mod validate;
use self::{
    add_validator::AddValidator, create_committee::CreateCommitteeArgs, validate::ValidateArgs,
};
use crate::args::clap_genesis_parser;
use clap::{Args, Subcommand};

use rand::{rngs::StdRng, SeedableRng};
use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use reth_primitives::{keccak256, Address, GenesisAccount, U256};
use secp256k1::Secp256k1;
use std::{path::PathBuf, sync::Arc};
use tn_node::dirs::{default_datadir_args, DataDirChainPath, DataDirPath};
use tn_types::{Config, ConfigFmt, ConfigTrait, NetworkGenesis, TelcoinDirs as _};

/// Generate keypairs and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenesisArgs {
    /// Read and write to committe file.
    ///
    /// [Committee] contains quorum of validators.
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "CONFIG_FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The path to the committee file to use.
    #[arg(long, value_name = "COMMITTEE_FILE", verbatim_doc_comment)]
    pub committee_file: Option<PathBuf>,

    // TODO: support custom genesis path
    // /// The path to the genesis directory with validator information to build the committee.
    // #[arg(long, value_name = "GENESIS_DIR", verbatim_doc_comment)]
    // pub genesis_dir: Option<PathBuf>,
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
    chain: Arc<ChainSpec>,

    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub command: CeremonySubcommand,
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum CeremonySubcommand {
    /// Initialize the genesis directory.
    #[command(name = "init")]
    Initialize(Initialize),
    /// Add validator to committee.
    #[command(name = "add-validator")]
    AddValidator(AddValidator),
    /// Verify the current validators.
    #[command(name = "validate")]
    Validate(ValidateArgs),
    /// Create a committee from genesis.
    #[command(name = "create-committee", alias = "finalize")]
    CreateCommittee(CreateCommitteeArgs),
    // TODO: add more commands
    // - list validators (print peers)
    // - verify and sign (sign EL Genesis)
    // - finalize (todo)
}

/// Capture an optional test account for development.
#[derive(Debug, Clone, Args)]
pub struct Initialize {
    /// Used to add a funded account (by simple text string).  Use this on a dev cluster
    /// (must provide on all validator genesis inits) to have an account with a deterministically
    /// derived key. This is ONLY for dev testing, never use this for other chains.
    #[arg(long)]
    pub dev_funded_account: Option<String>,
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
fn account_from_word(key_word: &str) -> reth_primitives::alloy_primitives::Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand = <StdRng as SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (_, public_key) = secp.generate_keypair(&mut rand);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    Address::from_slice(&hash[12..])
}

impl GenesisArgs {
    /// Execute command
    pub async fn execute(&self) -> eyre::Result<()> {
        // // create datadir
        // let datadir = self.data_dir();
        // // creates a default config if none exists
        // let mut config = self.load_config()?;

        match &self.command {
            CeremonySubcommand::Initialize(init) => {
                // TODO: support custom genesis path
                let datadir: DataDirChainPath = self
                    .datadir
                    .unwrap_or_chain_default(self.chain.chain, default_datadir_args())
                    .into();

                // TODO: use config or CLI chain spec?
                let config_path = self.config.clone().unwrap_or(datadir.node_config_path());

                let mut tn_config: Config = Config::load_from_path(&config_path, ConfigFmt::YAML)?;
                if let Some(acct_str) = &init.dev_funded_account {
                    let addr = account_from_word(acct_str);
                    tn_config.genesis.alloc.insert(
                        addr,
                        GenesisAccount::default().with_balance(U256::from(10).pow(U256::from(27))), // One Billion TEL
                    );
                    Config::store_path(config_path, tn_config.clone(), ConfigFmt::YAML)?;
                }

                let network_genesis = NetworkGenesis::with_chain_spec(tn_config.chain_spec());
                network_genesis.write_to_path(datadir.genesis_path())?;
            }
            // add validator to the committee file
            CeremonySubcommand::AddValidator(args) => {
                args.execute().await?;
            }
            CeremonySubcommand::Validate(args) => {
                args.execute().await?;
            }
            CeremonySubcommand::CreateCommittee(args) => {
                args.execute().await?;
            }
        }

        Ok(())
    }
}
