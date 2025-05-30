//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

use alloy::primitives::aliases::U232;
use clap::Args;
use rand::{rngs::StdRng, SeedableRng};
use secp256k1::Secp256k1;
use std::{str::FromStr as _, time::Duration};
use tn_config::{
    Config, ConfigFmt, ConfigTrait, NetworkGenesis, Parameters, TelcoinDirs as _, DEPLOYMENTS_JSON,
};
use tn_reth::{
    dirs::{default_datadir_args, DataDirChainPath, DataDirPath},
    system_calls::ConsensusRegistry,
    MaybePlatformPath, RethChainSpec, RethEnv,
};
use tn_types::{keccak256, now, Address, GenesisAccount, U256};
use tracing::info;

use crate::args::{clap_address_parser, clap_u232_parser};

/// Generate a new chain genesis.
#[derive(Debug, Args)]
pub struct GenesisArgs {
    /// Read and write to committe file.
    ///
    /// [Committee] contains quorum of validators.
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

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

    /// Used to add a funded account (by simple text string).  Use this on a dev cluster
    /// (must provide on all validator genesis inits) to have an account with a deterministically
    /// derived key. This is ONLY for dev testing, never use this for other chains.
    #[arg(long)]
    pub dev_funded_account: Option<String>,
    /// Max delay for a node to produce a new header.
    #[arg(long)]
    pub max_header_delay_ms: Option<u64>,
    /// Min delay for a node to produce a new header.
    #[arg(long)]
    pub min_header_delay_ms: Option<u64>,
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
pub(crate) fn account_from_word(key_word: &str) -> Address {
    if key_word.starts_with("0x") {
        key_word.parse().expect("not a valid account!")
    } else {
        let seed = keccak256(key_word.as_bytes());
        let mut rand = <StdRng as SeedableRng>::from_seed(seed.0);
        let secp = Secp256k1::new();
        let (_, public_key) = secp.generate_keypair(&mut rand);
        // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
        // tag returned by libsecp's uncompressed pubkey serialization
        let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
        Address::from_slice(&hash[12..])
    }
}

impl GenesisArgs {
    /// Execute command
    pub fn execute(&self) -> eyre::Result<()> {
        info!(target: "genesis::cerimony", "Creating a new chain genesis with initial validators");

        let chain = RethChainSpec::default();
        // load network genesis
        let data_dir: DataDirChainPath =
            self.datadir.unwrap_or_chain_default(chain.chain, default_datadir_args()).into();
        //let validators = NetworkGenesis::load_validators_from_path(&data_dir)?;
        let mut network_genesis =
            NetworkGenesis::new_from_path_and_genesis(&data_dir, chain.genesis().clone())?;

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

        let mut genesis = network_genesis.genesis().clone();
        // Configure hardforks or Reth will be cross with us...
        genesis.config.homestead_block = Some(0);
        //genesis.config.dao_fork_block = Some(0);
        genesis.config.eip150_block = Some(0);
        genesis.config.eip155_block = Some(0);
        genesis.config.eip158_block = Some(0);
        genesis.config.byzantium_block = Some(0);
        genesis.config.constantinople_block = Some(0);
        genesis.config.petersburg_block = Some(0);
        genesis.config.istanbul_block = Some(0);
        //genesis.config.muir_glacier_block = Some(0);
        genesis.config.berlin_block = Some(0);
        genesis.config.london_block = Some(0);
        //genesis.config.arrow_glacier_block = Some(0);
        //genesis.config.gray_glacier_block = Some(0);
        genesis.config.cancun_time = None; //Some(0);
        genesis.config.shanghai_time = Some(0);
        genesis.config.prague_time = None;
        genesis.config.osaka_time = None;
        genesis.timestamp = now();
        genesis.config.chain_id = 2017;
        genesis.config.terminal_total_difficulty_passed = true;
        genesis.config.terminal_total_difficulty = Some(U256::from(0));
        genesis = genesis.with_base_fee(Some(tn_types::MIN_PROTOCOL_BASE_FEE as u128));

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

        let mut updated_genesis = genesis_with_consensus_registry.extend_accounts(precompiles);
        // Changed a default config setting so update and save.
        if let Some(acct_str) = &self.dev_funded_account {
            let addr = crate::genesis::account_from_word(acct_str);
            updated_genesis.alloc.insert(
                addr,
                GenesisAccount::default().with_balance(U256::from(10).pow(U256::from(27))), // One Billion TEL
            );
        }

        // updated genesis with registry information
        network_genesis.update_genesis(updated_genesis);

        // update the config with new genesis information
        let mut parameters = Parameters::default();
        if let Some(max_header_delay_ms) = self.max_header_delay_ms {
            parameters.max_header_delay = Duration::from_millis(max_header_delay_ms);
        }
        if let Some(min_header_delay_ms) = self.min_header_delay_ms {
            parameters.min_header_delay = Duration::from_millis(min_header_delay_ms);
        }

        // write genesis and config to file
        Config::write_to_path(
            data_dir.genesis_file_path(),
            network_genesis.genesis(),
            ConfigFmt::YAML,
        )?;
        Config::write_to_path(data_dir.node_config_parameters_path(), parameters, ConfigFmt::YAML)?;

        // generate committee and worker cache
        let committee = network_genesis.create_committee()?;
        let worker_cache = network_genesis.create_worker_cache()?;

        // write to file
        Config::write_to_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;
        Config::write_to_path(data_dir.worker_cache_path(), worker_cache, ConfigFmt::YAML)?;

        Ok(())
    }
}
