//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

use clap::Args;
use secp256k1::{
    rand::{rngs::StdRng, SeedableRng},
    Secp256k1,
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tn_config::{
    Config, ConfigFmt, ConfigTrait, NetworkGenesis, Parameters, TelcoinDirs as _,
    GOVERNANCE_SAFE_ADDRESS,
};
use tn_reth::{system_calls::ConsensusRegistry, RethChainSpec, RethEnv};
use tn_types::{keccak256, set_genesis_defaults, Address, ExecHeader, GenesisAccount, U256};
use tracing::info;

use crate::args::{clap_address_parser, clap_u256_parser_to_18_decimals, maybe_hex};

/// Generate a new chain genesis.
#[derive(Debug, Args)]
pub struct GenesisArgs {
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
        default_value_t = GOVERNANCE_SAFE_ADDRESS,
        verbatim_doc_comment
    )]
    pub consensus_registry_owner: Address,

    /// The address recieves all transaction base fees.
    ///
    /// This is a governance safe contract that will distribute/manage basefees.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "basefee-address",
        alias = "basefee_address",
        help_heading = "The recipient of base fees",
        value_parser = clap_address_parser,
        default_value_t = GOVERNANCE_SAFE_ADDRESS,
        verbatim_doc_comment
    )]
    pub basefee_address: Address,

    /// The initial stake credited to each validator in genesis.
    #[arg(
        long = "initial-stake-per-validator",
        alias = "stake",
        help_heading = "The initial stake credited to each validator in genesis. The default is 1mil TEL.",
        value_parser = clap_u256_parser_to_18_decimals,
        default_value = "1_000_000",
        verbatim_doc_comment
    )]
    pub initial_stake: U256,

    /// The minimum amount a validator can withdraw.
    #[arg(
        long = "min-withdraw-amount",
        alias = "min_withdraw",
        help_heading = "The minimal amount a validator can withdraw. The default is 1_000 TEL.",
        value_parser = clap_u256_parser_to_18_decimals,
        default_value = "1_000",
        verbatim_doc_comment
    )]
    pub min_withdrawal: U256,

    /// The total amount of block rewards per epoch starting in genesis.
    #[arg(
        long = "epoch-block-rewards",
        alias = "block_rewards_per_epoch",
        help_heading = "The per block reward (int) for each epoch. Ex) 20mil rewards per month / 31 days / 25 hour epoch interval. It's best to use conservative values.",
        value_parser = clap_u256_parser_to_18_decimals,
        default_value = "25_806",
        verbatim_doc_comment
    )]
    pub epoch_rewards: U256,

    /// The duration of each epoch (in secs) starting in genesis.
    #[arg(
        long = "epoch-duration-in-secs",
        alias = "epoch_length",
        help_heading = "The length of each epoch in seconds.",
        default_value_t = 60 * 60 * 8, // 8-hours
        verbatim_doc_comment
    )]
    pub epoch_duration: u32,

    /// Used to add a funded account (by simple text string).  Use this on a dev cluster
    /// to have an account with a deterministically derived key. This is ONLY for dev
    /// testing, never use this for other chains.
    #[arg(long)]
    pub dev_funded_account: Option<String>,
    /// Max delay for a node to produce a new header.
    #[arg(long)]
    pub max_header_delay_ms: Option<u64>,
    /// Min delay for a node to produce a new header.
    #[arg(long)]
    pub min_header_delay_ms: Option<u64>,
    /// Numeric chain id that will go in the genesis.
    /// Default is 0x7e1 (2017).
    #[arg(long, default_value_t = 2017, value_parser=maybe_hex)]
    pub chain_id: u64,
    /// Per-worker fee config overrides at genesis.
    ///
    /// Format: WORKER_ID:STRATEGY:VALUE (e.g., 0:0:100000000 for worker 0 with EIP-1559 at 100M
    /// gas target, or 1:1:200 for worker 1 with static fee of 200 wei).
    /// Can be specified multiple times for different workers.
    #[arg(
        long = "worker-fee-config",
        alias = "worker_fee_config",
        help_heading = "Per-worker fee config overrides",
        value_name = "WORKER_ID:STRATEGY:VALUE",
        verbatim_doc_comment
    )]
    pub worker_fee_configs: Vec<String>,

    /// YAML file containing accounts to merge into genesis.
    /// This is intended for dev and test nets.
    #[arg(long, value_name = "YAML_FILE", verbatim_doc_comment)]
    pub accounts: Option<PathBuf>,
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
    /// Parse `--worker-fee-config` values from `"WORKER_ID:STRATEGY:VALUE"` strings
    /// into `(strategy, value)` pairs ordered by worker id.
    ///
    /// The worker id is validated to match the index position (0, 1, 2, ...).
    fn parse_worker_fee_configs(&self) -> eyre::Result<Vec<(u8, u64)>> {
        if self.worker_fee_configs.is_empty() {
            return Ok(vec![(0u8, 100_000_000_000u64)]);
        }

        let mut configs: Vec<(u16, u8, u64)> = self
            .worker_fee_configs
            .iter()
            .map(|s| {
                let parts: Vec<&str> = s.splitn(3, ':').collect();
                if parts.len() != 3 {
                    eyre::bail!(
                        "invalid --worker-fee-config format '{s}': expected WORKER_ID:STRATEGY:VALUE"
                    );
                }
                let worker_id: u16 = parts[0].parse().map_err(|e| {
                    eyre::eyre!("invalid worker id '{}' in --worker-fee-config: {e}", parts[0])
                })?;
                let strategy: u8 = parts[1].parse().map_err(|e| {
                    eyre::eyre!("invalid strategy '{}' in --worker-fee-config: {e}", parts[1])
                })?;
                let value: u64 = parts[2].parse().map_err(|e| {
                    eyre::eyre!("invalid value '{}' in --worker-fee-config: {e}", parts[2])
                })?;
                Ok((worker_id, strategy, value))
            })
            .collect::<eyre::Result<Vec<_>>>()?;

        // Sort by worker_id so positional indexing is deterministic.
        configs.sort_by_key(|(id, _, _)| *id);

        // Validate contiguous worker ids starting from 0.
        for (expected, (id, _, _)) in configs.iter().enumerate() {
            if *id as usize != expected {
                eyre::bail!(
                    "worker fee configs must be contiguous starting from 0, but worker id {id} found at position {expected}"
                );
            }
        }

        Ok(configs.into_iter().map(|(_, strategy, value)| (strategy, value)).collect())
    }

    /// Execute command
    pub fn execute(&self, data_dir: PathBuf) -> eyre::Result<()> {
        info!(target: "genesis::ceremony", "Creating a new chain genesis with initial validators");

        let chain: RethChainSpec<ExecHeader> = RethChainSpec::default();
        // load network genesis
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
        set_genesis_defaults(&mut genesis);
        genesis.config.chain_id = self.chain_id;

        let worker_fee_configs = self.parse_worker_fee_configs()?;

        // try to create a runtime if one doesn't already exist
        // this is a workaround for executing committees pre-genesis during tests and normal CLI
        // operations
        let genesis_with_consensus_registry = if tokio::runtime::Handle::try_current().is_ok() {
            // use the current runtime (ie - tests)
            RethEnv::create_consensus_registry_genesis_accounts(
                validators.clone(),
                genesis,
                initial_stake_config.clone(),
                self.consensus_registry_owner,
                worker_fee_configs.clone(),
            )?
        } else {
            // no runtime exists (normal CLI operation)
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .thread_name("consensus-registry")
                .build()?;

            runtime.block_on(async {
                RethEnv::create_consensus_registry_genesis_accounts(
                    validators.clone(),
                    genesis,
                    initial_stake_config,
                    self.consensus_registry_owner,
                    worker_fee_configs,
                )
            })?
        };
        let precompiles =
            NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompile fetch error");
        let mut updated_genesis = genesis_with_consensus_registry.extend_accounts(precompiles);
        // Changed a default config setting so update and save.
        if let Some(acct_str) = &self.dev_funded_account {
            let addr = crate::genesis::account_from_word(acct_str);
            updated_genesis.alloc.insert(
                addr,
                GenesisAccount::default().with_balance(U256::from(10).pow(U256::from(27))), // One Billion TEL
            );
        }
        // Extend genesis accounts with option account file.
        if let Some(accounts) = &self.accounts {
            let f = std::fs::File::open(accounts)?;
            let accounts: BTreeMap<Address, GenesisAccount> = serde_yaml::from_reader(f)?;
            updated_genesis.alloc.extend(accounts);
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
        parameters.basefee_address = Some(self.basefee_address);

        // write genesis and config to file
        Config::write_to_path(
            data_dir.genesis_file_path(),
            network_genesis.genesis(),
            ConfigFmt::YAML,
        )?;
        Config::write_to_path(data_dir.node_config_parameters_path(), parameters, ConfigFmt::YAML)?;

        // generate initial committee for genesis
        let committee = network_genesis.create_committee()?;

        // write to file
        Config::write_to_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::{Address, U256};

    fn args_with_configs(configs: Vec<&str>) -> GenesisArgs {
        GenesisArgs {
            consensus_registry_owner: Address::ZERO,
            basefee_address: Address::ZERO,
            initial_stake: U256::ZERO,
            min_withdrawal: U256::ZERO,
            epoch_rewards: U256::ZERO,
            epoch_duration: 0,
            dev_funded_account: None,
            max_header_delay_ms: None,
            min_header_delay_ms: None,
            chain_id: 2017,
            worker_fee_configs: configs.into_iter().map(String::from).collect(),
            accounts: None,
        }
    }

    #[test]
    fn test_parse_worker_fee_configs_empty_returns_default() {
        let args = args_with_configs(vec![]);
        let result = args.parse_worker_fee_configs().unwrap();
        assert_eq!(result, vec![(0, 100_000_000_000)]);
    }

    #[test]
    fn test_parse_worker_fee_configs_single() {
        let args = args_with_configs(vec!["0:0:30000000"]);
        let result = args.parse_worker_fee_configs().unwrap();
        assert_eq!(result, vec![(0, 30_000_000)]);
    }

    #[test]
    fn test_parse_worker_fee_configs_multiple() {
        let args = args_with_configs(vec!["0:0:30000000", "1:1:500"]);
        let result = args.parse_worker_fee_configs().unwrap();
        assert_eq!(result, vec![(0, 30_000_000), (1, 500)]);
    }

    #[test]
    fn test_parse_worker_fee_configs_sorts_by_worker_id() {
        let args = args_with_configs(vec!["1:1:500", "0:0:30000000"]);
        let result = args.parse_worker_fee_configs().unwrap();
        assert_eq!(result, vec![(0, 30_000_000), (1, 500)]);
    }

    #[test]
    fn test_parse_worker_fee_configs_non_contiguous_errors() {
        let args = args_with_configs(vec!["0:0:30000000", "2:1:500"]);
        let result = args.parse_worker_fee_configs();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_worker_fee_configs_bad_format_errors() {
        let args = args_with_configs(vec!["bad"]);
        let result = args.parse_worker_fee_configs();
        assert!(result.is_err());
    }
}
