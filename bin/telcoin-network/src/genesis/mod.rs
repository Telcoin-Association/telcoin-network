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

use reth::dirs::MaybePlatformPath;
use reth_primitives::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use tn_node::dirs::{default_datadir_args, DataDirPath, TelcoinDirs as _};
use tn_types::NetworkGenesis;

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
    Initialize,
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

impl GenesisArgs {
    /// Execute command
    pub async fn execute(&self) -> eyre::Result<()> {
        // // create datadir
        // let datadir = self.data_dir();
        // // creates a default config if none exists
        // let mut config = self.load_config()?;

        match &self.command {
            CeremonySubcommand::Initialize => {
                // TODO: support custom genesis path
                let datadir = self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args());
                let network_genesis = NetworkGenesis::new();
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
