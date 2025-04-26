//! Create a committee from the validators in genesis.

use crate::args::{clap_address_parser, clap_genesis_parser};
use clap::Args;
use std::{path::PathBuf, sync::Arc};
use tn_config::{Config, ConfigFmt, ConfigTrait, NetworkGenesis, TelcoinDirs as _};
use tn_reth::{
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    system_calls::{ConsensusRegistry, RWTEL_ADDRESS},
    MaybePlatformPath, RethChainSpec, RethEnv,
};
use tn_types::{Address, U256};
use tracing::{debug, info};

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
    /// Defaults to the adiri testnet.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<RethChainSpec>,

    /// The path to the consensus registry storage yaml file.
    #[arg(long, value_name = "CONSENSUS_REGISTRY_PATH", verbatim_doc_comment)]
    pub consensus_registry: Option<PathBuf>,

    /// The owner's address for initializing the `ConsensusRegistry` in genesis.
    ///
    /// This address is used to initialize the owner for `ConsensusRegistry`.
    /// This should be a governance-controller, multisig address in production.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "consensus-registry-owner",
        alias = "consensus_registry_owner",
        help_heading = "The owner for ConsensusRegistry",
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub consensus_registry_owner: Address,

    /// The initial stake credited to each validator in genesis.
    #[arg(
        long = "initial-stake-per-validator",
        alias = "stake",
        help_heading = "The initial stake credited to each validator in genesis. The default is 1mil TEL.",
        default_value_t = U256::from(1_000_000e18),
        verbatim_doc_comment
    )]
    pub initial_stake: U256,

    /// The minimum amount a validator can withdraw.
    #[arg(
        long = "min-withdraw-amount",
        alias = "min_withdraw",
        help_heading = "The minimal amount a validator can withdraw. The default is 1_000 TEL.",
        default_value_t = U256::from(1_000e18),
        verbatim_doc_comment
    )]
    pub min_withdrawal: U256,

    /// The amount of block rewards per epoch starting in genesis.
    #[arg(
        long = "epoch-block-rewards",
        alias = "block_rewards_per_epoch",
        help_heading = "The amount of TEL (incl 18 decimals) for the committee starting at genesis.",
        default_value_t = U256::from(20_000_000e18).checked_div(U256::from(28)).expect("U256 div works"),
        verbatim_doc_comment
    )]
    pub epoch_rewards: U256,

    /// The duration of each epoch (in secs) starting in genesis.
    #[arg(
        long = "epoch-duration-in-secs",
        alias = "epoch_length",
        help_heading = "The length of each epoch in seconds.",
        default_value_t = 60 * 60 * 24, // 24-hours
        verbatim_doc_comment
    )]
    pub epoch_duration: u32,

    /// The address for RWTEL in genesis.
    #[arg(
        long = "rwtel-contract-address",
        alias = "rwtel",
        help_heading = "The address for RWTEL contract.",
        default_value_t = RWTEL_ADDRESS,
        verbatim_doc_comment
    )]
    pub rwtel_address: Address,
}

impl CreateCommitteeArgs {
    /// Execute `Validate` command
    ///
    /// Process:
    /// - loop through validators within the genesis directory
    /// - ensure valid state for validators
    /// - write Committee to file
    /// - write WorkerCache to file
    pub fn execute(&self) -> eyre::Result<()> {
        info!(target: "genesis::add-validator", "Adding validator to committee");

        // load network genesis
        let data_dir: DataDirChainPath =
            self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args()).into();
        let mut network_genesis = NetworkGenesis::load_from_path(&data_dir)?;

        // validate only checks proof of possession for now
        //
        // the signatures must match the expected genesis file before consensus registry is added
        network_genesis.validate()?;

        // execute data so committee is on-chain and in genesis
        let validators = network_genesis.validators().values().cloned().collect();
        let genesis = network_genesis.genesis().clone();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: self.initial_stake,
            minWithdrawAmount: self.min_withdrawal,
            epochIssuance: self.epoch_rewards,
            epochDuration: self.epoch_duration,
        };

        let updated_genesis = RethEnv::create_consensus_registry_accounts_for_genesis(
            validators,
            genesis,
            initial_stake_config,
            self.consensus_registry_owner,
            self.rwtel_address,
        )?;

        // updated genesis with registry information
        network_genesis.update_chain(updated_genesis.into());

        // update the config with new genesis information
        let config_path = self.config.clone().unwrap_or(data_dir.node_config_path());
        let mut tn_config: Config = Config::load_from_path(&config_path, ConfigFmt::YAML)?;
        tn_config.genesis = network_genesis.genesis().clone();

        debug!(target: "cli", "genesis: {:#?}", tn_config.genesis);

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
