//! Create a committee from the validators in genesis.

use clap::Args;
use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use tn_node::dirs::{default_datadir_args, DataDirChainPath, DataDirPath};
use tn_types::{Config, ConfigFmt, ConfigTrait, NetworkGenesis, TelcoinDirs as _};

use crate::args::clap_genesis_parser;
use tracing::info;

/// Add the validator to the node
#[derive(Debug, Clone, Args)]
pub struct CreateCommitteeArgs {
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

    /// The path to the consensus registry storage yaml file.
    #[arg(long, value_name = "CONSENSUS_REGISTRY_PATH", verbatim_doc_comment)]
    pub consensus_registry: Option<PathBuf>,
}

impl CreateCommitteeArgs {
    /// Execute `Validate` command
    ///
    /// Process:
    /// - loop through validators within the genesis directory
    /// - ensure valid state for validators
    /// - write Committee to file
    /// - write WorkerCache to file
    ///
    /// TODO: `validate` only verifies proof of possession for now
    pub async fn execute(&self) -> eyre::Result<()> {
        info!(target: "genesis::add-validator", "Adding validator to committee");

        // load network genesis
        let data_dir: DataDirChainPath =
            self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args()).into();
        let mut network_genesis = NetworkGenesis::load_from_path(&data_dir)?;

        // validate only checks proof of possession for now
        //
        // the signatures must match the expected genesis file before consensus registry is added
        network_genesis.validate()?;

        // updated genesis with registry information
        network_genesis.construct_registry_genesis_accounts(self.consensus_registry.clone());

        // update the config with new genesis information
        let config_path = self.config.clone().unwrap_or(data_dir.node_config_path());
        let mut tn_config: Config = Config::load_from_path(&config_path, ConfigFmt::YAML)?;
        tn_config.genesis = network_genesis.chain_info().genesis().clone();

        // write genesis and config to file
        //
        // NOTE: CLI parser only supports JSON format for genesis
        Config::store_path(data_dir.genesis_file_path(), tn_config.genesis(), ConfigFmt::JSON)?;
        Config::store_path(config_path, tn_config, ConfigFmt::YAML)?;

        // generate committee and worker cache
        let committee = network_genesis.create_committee()?;
        let worker_cache = network_genesis.create_worker_cache()?;

        // write to file
        Config::store_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;
        Config::store_path(data_dir.worker_cache_path(), worker_cache, ConfigFmt::YAML)
    }
}
