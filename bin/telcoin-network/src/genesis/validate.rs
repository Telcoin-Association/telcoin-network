//! Validate the validators for genesis.

use crate::args::clap_genesis_parser;
use clap::Args;
use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use tn_config::NetworkGenesis;
use tn_node::dirs::{default_datadir_args, DataDirChainPath, DataDirPath};
use tracing::info;

/// Add the validator to the node
#[derive(Debug, Clone, Args)]
pub struct ValidateArgs {
    /// The path to the data dir for all telcoin-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/telcoin-network/`
    /// - macOS: `$HOME/Library/Application Support/telcoin-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "CONFIG_FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The path to the genesis directory.
    ///
    /// The GENESIS_DIRECTORY contains more directories:
    /// - committee
    /// - todo
    ///
    /// Validators add their information to the directory using VCS like
    /// github. Using individual files prevents merge conflicts.
    #[arg(long, value_name = "GENESIS_DIRECTORY", verbatim_doc_comment)]
    pub genesis: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Defaults to the custom
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        // default_value_if("dev", "true", "dev"),
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<ChainSpec>,
}

impl ValidateArgs {
    /// Execute `Validate` command
    ///
    /// Process:
    /// - loop through validators within the genesis directory
    /// - ensure valid state for validators
    ///
    /// TODO: `validate` only verifies proof of possession for now
    pub async fn execute(&self) -> eyre::Result<()> {
        info!(target: "genesis::validate", "validating validators nominated for committee");

        // load network genesis
        let datadir: DataDirChainPath =
            self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args()).into();
        let network_genesis = NetworkGenesis::load_from_path(&datadir)?;
        network_genesis.validate()
    }
}
