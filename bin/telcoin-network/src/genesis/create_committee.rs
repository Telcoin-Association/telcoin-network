//! Create a committee from the validators in genesis.

use crate::args::{clap_address_parser, clap_genesis_parser, clap_u232_parser};
use alloy::primitives::{aliases::U232, ruint::aliases::U256};
use clap::Args;
use std::{path::PathBuf, str::FromStr, sync::Arc};
use tn_config::{
    Config, ConfigFmt, ConfigTrait, NetworkGenesis, TelcoinDirs as _, DEPLOYMENTS_JSON,
};
use tn_reth::{
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    system_calls::ConsensusRegistry,
    MaybePlatformPath, RethChainSpec, RethEnv,
};
use tn_types::Address;
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
        value_parser = clap_u232_parser,
        default_value = "1_000_000",
        verbatim_doc_comment
    )]
    pub initial_stake: U232,

    /// The minimum amount a validator can withdraw.
    #[arg(
        long = "min-withdraw-amount",
        alias = "min_withdraw",
        help_heading = "The minimal amount a validator can withdraw. The default is 1_000 TEL.",
        value_parser = clap_u232_parser,
        default_value = "1_000",
        verbatim_doc_comment
    )]
    pub min_withdrawal: U232,

    /// The amount of block rewards per epoch starting in genesis.
    #[arg(
        long = "epoch-block-rewards",
        alias = "block_rewards_per_epoch",
        help_heading = "The per block reward (int) for each epoch. Ex) 20mil rewards per month / 31 days / 25 hour epoch interval. It's best to use conservative values.",
        value_parser = clap_u232_parser,
        default_value = "25_806",
        verbatim_doc_comment
    )]
    pub epoch_rewards: U232,

    /// The duration of each epoch (in secs) starting in genesis.
    #[arg(
        long = "epoch-duration-in-secs",
        alias = "epoch_length",
        help_heading = "The length of each epoch in seconds.",
        default_value_t = 60 * 60 * 24, // 24-hours
        verbatim_doc_comment
    )]
    pub epoch_duration: u32,
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

        // update genesis with the provided chain
        network_genesis.update_genesis(self.chain.genesis().clone());

        // validate only checks proof of possession for now
        //
        // the signatures must match the expected genesis file before consensus registry is added
        network_genesis.validate()?;

        // execute data so committee is on-chain and in genesis
        let validators: Vec<_> = network_genesis.validators().values().cloned().collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: self.initial_stake,
            minWithdrawAmount: self.min_withdrawal,
            epochIssuance: self.epoch_rewards,
            epochDuration: self.epoch_duration,
        };

        let genesis = self.chain.genesis().clone();

        // try to create a runtime if one doesn't already exist
        // this is a workaround for executing committees pre-genesis during tests and normal CLI
        // operations
        let genesis_with_consensus_registry = if tokio::runtime::Handle::try_current().is_ok() {
            // use the current runtime (ie - tests)
            RethEnv::create_consensus_registry_genesis_account(
                validators.clone(),
                genesis,
                initial_stake_config.clone(),
                self.consensus_registry_owner,
            )?
        } else {
            // no runtime exists (normal CLI operation)
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .thread_name("consensus-registry")
                .build()?;

            runtime.block_on(async {
                RethEnv::create_consensus_registry_genesis_account(
                    validators.clone(),
                    genesis,
                    initial_stake_config,
                    self.consensus_registry_owner,
                )
            })?
        };
        // use embedded ITS config from submodule, passing in decremented ITEL balance
        let genesis_stake = self
            .initial_stake
            .checked_mul(U232::from(validators.len()))
            .expect("initial validators' stake");
        let itel_balance = U256::from(clap_u232_parser("100_000_000_000")? - genesis_stake);

        let itel_address_str: String =
            RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, Some("its.InterchainTEL"))?
                .as_str()
                .expect("invalid json string")
                .to_string();
        let itel_address = Address::from_str(&itel_address_str)?;
        let precompiles =
            NetworkGenesis::fetch_precompile_genesis_accounts(itel_address, itel_balance)
                .expect("precompile fetch error");

        let updated_genesis = genesis_with_consensus_registry.extend_accounts(precompiles);

        // updated genesis with registry information
        network_genesis.update_genesis(updated_genesis);

        // update the config with new genesis information
        let config_path = self.config.clone().unwrap_or(data_dir.node_config_path());
        let mut tn_config: Config = Config::load_from_path(&config_path, ConfigFmt::YAML)?;
        tn_config.genesis = network_genesis.genesis().clone();

        debug!(target: "cli", "genesis: {:#?}", tn_config.genesis);

        // write genesis and config to file
        //
        // NOTE: CLI parser only supports JSON format for genesis
        Config::write_to_path(data_dir.genesis_file_path(), tn_config.genesis(), ConfigFmt::JSON)?;
        Config::write_to_path(config_path, tn_config, ConfigFmt::YAML)?;

        // generate committee and worker cache
        let committee = network_genesis.create_committee()?;
        let worker_cache = network_genesis.create_worker_cache()?;

        // write to file
        network_genesis.write_to_path(data_dir.genesis_path())?;
        Config::write_to_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;
        Config::write_to_path(data_dir.worker_cache_path(), worker_cache, ConfigFmt::YAML)
    }
}
