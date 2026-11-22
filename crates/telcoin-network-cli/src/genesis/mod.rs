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
    /// Max time a worker waits before sealing a batch of pending transactions. Paces block
    /// production under transaction load; it does not seal empty batches, so it does not
    /// gate idle round advance (the header delays do that).
    #[arg(long)]
    pub max_batch_delay_ms: Option<u64>,
    /// Garbage-collection depth in DAG rounds. Lowering it demotes a lagging CVV to
    /// `CvvInactive` after fewer rounds of downtime (the follow/state-sync path). Test-only
    /// knob; when unset, `Parameters::default` uses the `MAX_GC_DEPTH` protocol default.
    #[arg(long)]
    pub gc_depth: Option<u32>,
    /// Numeric chain id that will go in the genesis.
    /// Default is 0xde7e1 (911329).  Used mostly for testing.
    #[arg(long, default_value_t = 911329, value_parser=maybe_hex)]
    pub chain_id: u64,
    /// Per-worker fee config overrides at genesis.
    ///
    /// Format: WORKER_ID:STRATEGY:VALUE (e.g., 0:0:100000000 for worker 0 with EIP-1559 at 100M
    /// gas target, or 1:1:200 for worker 1 with static fee of 200 wei).
    /// Can be specified multiple times for different workers. The number of entries must match
    /// the worker count advertised by every genesis validator.
    ///
    /// See types/src/gas_accumulator.rs for all `WorkerFeeConfig` types.
    #[arg(
        long = "worker-fee-config",
        alias = "worker_fee_config",
        help_heading = "Per-worker fee config overrides",
        value_name = "WORKER_ID:STRATEGY:VALUE",
        default_value = "0:0:18446744073709551615", // u64::MAX
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
        /// Highest strategy id the `WorkerConfigs` contract accepts; mirrors
        /// `WorkerConfigs.sol`'s `MAX_STRATEGY` (0 = EIP-1559, 1 = static fee).
        const MAX_STRATEGY: u8 = 1;

        // `WorkerConfigs.sol` reverts `NumWorkersBelowMinimum()` on an empty config set. The
        // clap default guarantees one entry when the flag is absent, but guard programmatic
        // construction too so the ceremony fails at parse time, not at the pre-genesis deploy.
        if self.worker_fee_configs.is_empty() {
            eyre::bail!(
                "at least one --worker-fee-config is required: WorkerConfigs deploys with numWorkers >= 1"
            );
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
                // reject strategies the contract's constructor would revert on
                // (`InvalidStrategy`) so a ceremony typo fails loudly at parse time
                if strategy > MAX_STRATEGY {
                    eyre::bail!(
                        "invalid strategy {strategy} in --worker-fee-config '{s}': accepted strategies are 0 (EIP-1559) and 1 (static fee)"
                    );
                }
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

    /// Validate the ceremony inputs, then generate the genesis and initial committee files.
    pub fn execute(&self, data_dir: PathBuf) -> eyre::Result<()> {
        info!(target: "genesis::ceremony", "Creating a new chain genesis with initial validators");

        let chain: RethChainSpec<ExecHeader> = RethChainSpec::default();
        // load network genesis
        let mut network_genesis =
            NetworkGenesis::new_from_path_and_genesis(&data_dir, chain.genesis().clone())?;

        // Verify signatures and the shared worker count before deploying the genesis contracts.
        network_genesis.validate()?;
        let committee = network_genesis.create_committee()?;
        let worker_fee_configs = self.parse_worker_fee_configs()?;
        let num_workers = committee.number_of_workers();
        if worker_fee_configs.len() != num_workers {
            eyre::bail!(
                "genesis validators advertise {num_workers} workers but {} --worker-fee-config \
                 entries were supplied: provide exactly one fee config per worker",
                worker_fee_configs.len()
            );
        }

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
        if let Some(max_batch_delay_ms) = self.max_batch_delay_ms {
            parameters.max_batch_delay = Duration::from_millis(max_batch_delay_ms);
        }
        if let Some(gc_depth) = self.gc_depth {
            parameters.gc_depth = gc_depth;
        }
        parameters.basefee_address = self.basefee_address;

        // write genesis and config to file
        Config::write_to_path(
            data_dir.genesis_file_path(),
            network_genesis.genesis(),
            ConfigFmt::YAML,
        )?;
        Config::write_to_path(data_dir.node_config_parameters_path(), parameters, ConfigFmt::YAML)?;

        // write to file
        Config::write_to_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    use clap::Parser as _;
    use eyre::WrapErr as _;
    use std::{collections::BTreeSet, path::Path};
    use tn_config::{genesis_validator_safes, NodeInfo, WORKER_CONFIGS_ADDRESS};
    use tn_reth::system_calls::CONSENSUS_REGISTRY_ADDRESS;
    use tn_types::{
        address, generate_proof_of_possession_bls_for_test, test_utils::CommandParser,
        verify_proof_of_possession_bls, Address, BlsKeypair, BlsPublicKey, BootstrapServer,
        Committee, Genesis, Multiaddr, NetworkKeypair, NodeP2pInfo, P2pNode, Protocol, B256,
        MAINNET_BOOTSTRAP_SERVERS, MAINNET_COMMITTEE, MAINNET_GENESIS, MAINNET_GENESIS_HASH,
        MAINNET_GENESIS_TIMESTAMP, MAINNET_PARAMETERS, U256,
    };

    /// Prepare signed validator inputs and parse the real genesis CLI defaults and fee flags.
    fn worker_count_ceremony(
        num_workers: usize,
        configs: &[&str],
    ) -> eyre::Result<(tempfile::TempDir, GenesisArgs)> {
        let directory = tempfile::tempdir()?;
        let validators = directory.path().join("genesis/validators");
        std::fs::create_dir_all(&validators)?;
        (1_u8..=4).try_for_each(|index| -> eyre::Result<()> {
            let keypair = BlsKeypair::generate(&mut StdRng::from_seed([index; 32]));
            let address = Address::from([index; 20]);
            let primary =
                (NetworkKeypair::generate_ed25519().public().clone().into(), Multiaddr::empty())
                    .into();
            let workers = (0..num_workers)
                .map(|_| {
                    (NetworkKeypair::generate_ed25519().public().clone().into(), Multiaddr::empty())
                        .into()
                })
                .collect();
            let validator = NodeInfo {
                name: format!("validator-{index}"),
                bls_public_key: *keypair.public(),
                p2p_info: NodeP2pInfo::new(primary, workers),
                execution_address: address,
                proof_of_possession: generate_proof_of_possession_bls_for_test(&keypair, &address)?,
            };
            Config::write_to_path(
                validators.join(format!("validator-{index}.yaml")),
                validator,
                ConfigFmt::YAML,
            )
        })?;
        let args = CommandParser::<GenesisArgs>::try_parse_from(
            std::iter::once("tn")
                .chain(configs.iter().flat_map(|config| ["--worker-fee-config", *config])),
        )?
        .args;
        Ok((directory, args))
    }

    /// A mismatched ceremony must name both counts and leave all output files absent.
    fn assert_worker_fee_count_mismatch(num_workers: usize, configs: &[&str]) -> eyre::Result<()> {
        let (directory, args) = worker_count_ceremony(num_workers, configs)?;
        let data_dir = directory.path().to_path_buf();
        let error = args.execute(data_dir.clone()).err().ok_or_else(|| {
            eyre::eyre!("genesis must reject a fee count different from its committee worker count")
        })?;
        let fee_count = configs.len().max(1);
        assert!(
            error.to_string().contains(&format!(
                "genesis validators advertise {num_workers} workers but {fee_count} --worker-fee-config entries were supplied"
            )),
            "unexpected ceremony error: {error}"
        );
        assert!(
            [
                data_dir.genesis_file_path(),
                data_dir.committee_path(),
                data_dir.node_config_parameters_path(),
            ]
            .iter()
            .all(|path| !path.exists()),
            "rejected ceremony must not write genesis, committee or parameters"
        );
        Ok(())
    }

    /// Extra fee entries cannot silently widen the on-chain count beyond the committee.
    #[test]
    fn test_genesis_rejects_excess_worker_fee_configs() -> eyre::Result<()> {
        assert_worker_fee_count_mismatch(1, &["0:0:30000000", "1:1:500"])
    }

    /// Omitting fee flags keeps clap's single entry, which cannot configure two workers.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn test_genesis_rejects_missing_worker_fee_configs() -> eyre::Result<()> {
        assert!(
            tn_types::forks::multi_workers_fork_active(0),
            "this lane requires the multi-workers fork at genesis"
        );
        assert_worker_fee_count_mismatch(2, &[])
    }

    /// Matching fee and validator counts retain the configured committee and write all outputs.
    #[test]
    fn test_genesis_accepts_matching_worker_fee_configs() -> eyre::Result<()> {
        let cases: &[(usize, &[&str])] = &[(1, &[]), (2, &["0:0:30000000", "1:1:500"])];
        cases
            .iter()
            .filter(|(num_workers, _)| {
                *num_workers == 1 || tn_types::forks::multi_workers_fork_active(0)
            })
            .try_for_each(|(num_workers, configs)| -> eyre::Result<()> {
                let (directory, args) = worker_count_ceremony(*num_workers, configs)?;
                let data_dir = directory.path().to_path_buf();
                args.execute(data_dir.clone())?;
                let committee: Committee =
                    Config::load_from_path(data_dir.committee_path(), ConfigFmt::YAML)?;
                assert_eq!(committee.number_of_workers(), *num_workers);
                assert!(data_dir.genesis_file_path().is_file());
                assert!(data_dir.node_config_parameters_path().is_file());
                Ok(())
            })
    }

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
            max_batch_delay_ms: None,
            gc_depth: None,
            chain_id: 2017,
            worker_fee_configs: configs.into_iter().map(String::from).collect(),
            accounts: None,
        }
    }

    #[test]
    fn test_parse_worker_fee_configs_explicit_default() {
        let args = args_with_configs(vec!["0:0:100000000000"]);
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

    /// Regression: strategies above `WorkerConfigs.sol`'s `MAX_STRATEGY` (= 1) must fail at
    /// parse time with a message naming the offending entry, instead of reverting the
    /// pre-genesis constructor.
    #[test]
    fn test_parse_worker_fee_configs_rejects_strategy_above_max() {
        let args = args_with_configs(vec!["0:0:30000000", "1:2:500"]);
        let err = args.parse_worker_fee_configs().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid strategy 2 in --worker-fee-config '1:2:500'"),
            "error must name the offending entry, got: {msg}"
        );
        assert!(
            msg.contains("accepted strategies are 0 (EIP-1559) and 1 (static fee)"),
            "error must state the accepted range, got: {msg}"
        );
    }

    /// The exact `default_value` clap injects when `--worker-fee-config` is absent must keep
    /// parsing (flag absent means one worker with the EIP-1559 strategy).
    #[test]
    fn test_parse_worker_fee_configs_clap_default() {
        let args = args_with_configs(vec!["0:0:18446744073709551615"]);
        let result = args.parse_worker_fee_configs().unwrap();
        assert_eq!(result, vec![(0, u64::MAX)]);
    }

    /// Regression: an empty config list (only reachable programmatically — clap's default
    /// guarantees one entry) must fail at parse time; the contract constructor would revert
    /// `NumWorkersBelowMinimum`.
    #[test]
    fn test_parse_worker_fee_configs_empty_errors() {
        let args = args_with_configs(vec![]);
        let err = args.parse_worker_fee_configs().unwrap_err();
        assert!(
            err.to_string().contains("at least one --worker-fee-config is required"),
            "unexpected error: {err}"
        );
    }

    /// The placeholder owner of the committed mainnet `ConsensusRegistry` and `WorkerConfigs`,
    /// the `--consensus-registry-owner` that [`regenerate_mainnet_chain_configs`] passes (see
    /// `etc/genesis.sh`).
    ///
    /// The mainnet owner is a launch decision. The commit that installs the final mainnet data
    /// passes it to the ceremony in place of this constant and keeps the constant, which
    /// [`mainnet_final_data_is_complete`] refuses to find in either contract's storage.
    const MAINNET_PLACEHOLDER_OWNER: Address = address!("3DCc9a6f3A71F0A6C8C659c65558321c374E917a");

    /// The placeholder dev-funded account of the committed mainnet genesis, the
    /// `--dev-funded-account` that [`regenerate_mainnet_chain_configs`] passes. The ceremony
    /// allocates it one billion TEL.
    ///
    /// Whether mainnet has a dev-funded account, and which, is a launch decision. The commit
    /// that installs the final mainnet data passes it in place of this constant, or drops the
    /// flag, and keeps the constant: [`mainnet_final_data_is_complete`] fails while this account
    /// holds a balance.
    const MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT: Address =
        address!("3DCc9a6f3A71F0A6C8C659c65558321c374E917a");

    /// The placeholder basefee recipient of the committed mainnet `parameters.yaml`, the
    /// `--basefee-address` that [`regenerate_mainnet_chain_configs`] passes.
    ///
    /// The mainnet recipient is a launch decision. The commit that installs the final mainnet
    /// data passes it in place of this constant and keeps the constant, which
    /// [`mainnet_final_data_is_complete`] refuses to find in `parameters.yaml`.
    const MAINNET_PLACEHOLDER_BASEFEE_ADDRESS: Address =
        address!("9999999999999999999999999999999999999999");

    /// The mainnet epoch duration in seconds: 6 hours, the same value testnet runs (issue
    /// #1512).
    ///
    /// The regeneration ceremony passes this value and the gate below pins it. Epoch duration
    /// is a genesis parameter. An operator cannot change it on a node.
    const MAINNET_EPOCH_DURATION_SECS: u32 = 21_600;

    /// The total reward for one mainnet epoch, in whole TEL.
    ///
    /// This is the testnet value and the `--epoch-block-rewards` default. It is restated here
    /// so the ceremony does not depend on the clap default. The registry pays this amount once
    /// for each epoch, so 6-hour epochs issue 103,224 TEL for each day.
    const MAINNET_EPOCH_ISSUANCE_TEL: u64 = 25_806;

    /// The number of wei in one TEL (18 decimals), the scale `--epoch-block-rewards` applies.
    const WEI_PER_TEL: u64 = 1_000_000_000_000_000_000;

    /// The `ConsensusRegistry` storage slot of `StakeConfig.epochIssuance` for stake version 0.
    ///
    /// The genesis `StakeConfig` fills four consecutive slots: stake amount, min withdrawal,
    /// epoch issuance, epoch duration. This is the third slot.
    const STAKE_CONFIG_EPOCH_ISSUANCE_SLOT: B256 =
        b256!("0x13da86008ba1c6922daee3e07db95305ef49ebced9f5467a0b8613fcc6b343e5");

    /// The `ConsensusRegistry` storage slot of `StakeConfig.epochDuration` for stake version 0.
    ///
    /// This is the fourth and last `StakeConfig` slot (see
    /// [`STAKE_CONFIG_EPOCH_ISSUANCE_SLOT`]).
    const STAKE_CONFIG_EPOCH_DURATION_SLOT: B256 =
        b256!("0x13da86008ba1c6922daee3e07db95305ef49ebced9f5467a0b8613fcc6b343e6");

    /// How to regenerate the committed mainnet chain-config placeholders when
    /// `mainnet_genesis_encodes_the_six_hour_epoch_target` fails.
    const MAINNET_REGENERATION_LEVER: &str =
        "regenerate chain-configs/mainnet with `cargo test -p telcoin-network-cli \
         regenerate_mainnet_chain_configs -- --ignored`";

    /// The committed mainnet genesis must encode 6-hour epochs and the epoch issuance that the
    /// regeneration ceremony passes (issue #1512).
    ///
    /// The epoch duration sets the batch cache throughput ceiling, and the hardware
    /// requirements are sized for 6-hour epochs. A committed file with 24-hour epochs gives a
    /// ceiling 4 times lower than the documented one. The registry pays rewards once for each
    /// epoch, so this gate pins the issuance together with the duration: a change to only one
    /// of them changes the daily issuance.
    ///
    /// The gate reads the two `StakeConfig` slots from the compiled-in genesis, the same
    /// storage the ceremony writes. If a tn-contracts storage layout change moves these slots,
    /// the lookup returns `None` and the gate fails. Update the slot constants in that change.
    #[test]
    fn mainnet_genesis_encodes_the_six_hour_epoch_target() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");
        let stake_config_word = |slot: &B256| {
            genesis
                .alloc
                .get(&CONSENSUS_REGISTRY_ADDRESS)
                .and_then(|account| account.storage.as_ref())
                .and_then(|storage| storage.get(slot))
                .map(|word| U256::from_be_bytes(word.0))
        };

        assert_eq!(
            stake_config_word(&STAKE_CONFIG_EPOCH_DURATION_SLOT),
            Some(U256::from(MAINNET_EPOCH_DURATION_SECS)),
            "mainnet genesis must encode {MAINNET_EPOCH_DURATION_SECS} s epochs in the registry \
             StakeConfig.epochDuration slot (issue #1512) - {MAINNET_REGENERATION_LEVER}"
        );
        // 25,806 TEL in wei fits in a U256, so the expected value is always `Some`
        assert_eq!(
            stake_config_word(&STAKE_CONFIG_EPOCH_ISSUANCE_SLOT),
            U256::from(MAINNET_EPOCH_ISSUANCE_TEL).checked_mul(U256::from(WEI_PER_TEL)),
            "mainnet genesis must encode {MAINNET_EPOCH_ISSUANCE_TEL} TEL for each epoch in the \
             registry StakeConfig.epochIssuance slot (issue #1512) - {MAINNET_REGENERATION_LEVER}"
        );
    }

    /// The prefix of every placeholder validator's `name`, so that readers of the committed files
    /// and [`mint_mainnet_placeholder_validators`] can tell placeholders from collected files.
    const PLACEHOLDER_VALIDATOR_NAME_PREFIX: &str = "placeholder-validator-";

    /// The BLS public keys of the placeholder validators of
    /// [`mint_mainnet_placeholder_validators`], in the base58 form their node-info files hold,
    /// `validator-00.yaml` first.
    ///
    /// Their private keys were discarded when the placeholders were minted, so a validator with
    /// one of these keys can never sign. [`mainnet_final_data_is_complete`] refuses them whatever
    /// the file's `name` says, and [`mainnet_placeholder_validator_keys_are_pinned`] keeps this
    /// list in step with the committed placeholder files.
    const PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS: [&str; 10] = [
        "p8ZQ6KCubgiiyVvvS7MaeksxqBJTL1VRZeBSUXEpDjUiM3a4hfY5bGSm5pygaDe9XZKwQKQ5yfnwaGv3LzxNuWAPHEEUn9mva5ALT6R1DFF9acKVAk4ViXS9BbB7BfvKSxc",
        "puXJBUXuQ2Zzs2vCC1ijNRxbvJdUpUcrreNCJQfHWM5YQD5svXezdToAK8VxpBp3SiRUwi1hgHV636aFwrsJZV2mRRYYQakghJH25JsT937wnUXYrpVLP7atYDxSoF2SrR5",
        "renYDn7ryuApiw2roXXJy8DkCXFfZyjWJHb2cdx4hEFMinPJ23XHkJJNF3vo98uTQUNcvEAKn2xSeHuonDCsHSyYCgdSZMdbZKDKRnq5jrBEsKJ2YsDj8C7PTZsG4rzdF8e",
        "23yKA4SQSppCfchFutqrgyiMPC8KPBFPqJp3yJT2BaxBBu77FvAkWLszakZTy1RkzrqerLPkgG7zDqJdMYmdqaDrP56pBo6DgWFa9TrFchorP8HwVDDrkRR3R7d8w6jz3BJh",
        "oyDr8TVebAbZhRDoV7mkRGRCvXv1dczvPNHuYbSvdoRwzDHWh6VdQSEgXu3wuU8FT5hbkUg3wHWdsqSFcV8ov9sez6mEtpPMyEka5eidpw7ArNFtxXt87pFo6FzHyJZ9RSj",
        "zojpLkvZEsydyW796fwaLxttHMDTUe3XSPozB1ZA7ERpzx8fTPH2S8LH8rzGutFVrXnEqKrGi8w1nWnyqCE7PHBH35mKWQvWmpiiqH88gUiiDEM7etrnUmmxTVELuzaSLFN",
        "nAKWydkkqvdKgkM3o4cN9H1fqy2iWuxwQXDebGeT5JAAvqaVcV4FTFdZ4oARKWz9XWzr1PGuTHUCegGnA5ftLJLKJp42DWVgiNm8M7N4EQSqVj8XxMCxb847HuMbCYEyzFw",
        "zRPvRaMvg4v4MMnAanm9KotWrPKzWNKCJVuat1rU3ndvJkPBDB2eUpWosWHqg3rcWZxus9ccdNZaQHmZDWoqvWfvnWsA1sif9f9SNSgut3Wwtq2U58KSUQv1WMn8Uo3C5JN",
        "ufQMPuKwN69qi5eDLTE6U2vN3kKKz6CDQbACVB7Yqwt2Jr1qMJ2ne4t44LXgAwSTzkxDyQ2vkiuYUHgsTLJiUMfWiNBLmgYon6fRJh42zyZ75gLa7dpfuFgsASMM57j1UrT",
        "22cibK1Uoa5NombUkidHK9UQZKu6GtRYDQ6YXCvK5jytXmsHMAtror34Jzkb9rfyKNHjDjyKNdT4PNGWf3xSdEsL5auiNZtrT7RvXeUE3tKVasWzahBaK5Byshb1Pndmc9ih",
    ];

    /// The network keys of the placeholder validators, in the base58 form their node-info files
    /// hold: the primary key then the worker key of `validator-00.yaml`, then those of the next
    /// file.
    ///
    /// Their private keys were discarded with the BLS keys, so a node advertising one of them has
    /// no working network identity. See [`PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS`].
    const PLACEHOLDER_VALIDATOR_NETWORK_KEYS: [&str; 20] = [
        "4XTTM3XYBz14PrkdXhMfSqALdbrG7aBArzc8a17GYr9dntxux",
        "4XTTM25MPeMYhxoLTYhLic2zaix1AVHNNkXThJGHEhnaDLnnD",
        "4XTTM8k8Xxw4Kdi2wDQ5wftGvTE9Y8JDgrqTLm9egCkZY8GYC",
        "4XTTMGUfhftUq7kmTUFzqs9rmMQrJ1LFPWhBvFP2yw4JroyZL",
        "4XTTM7TgUSdY9oAgSg1hZM3nEkn86P7n8kNnsbVpsKq3Rkz47",
        "4XTTM7dhCsTntNMK5VtZvfboqkU5xHJPgFymG1SebcEFhjB9d",
        "4XTTM3QtfKJYLGV7jENB5PT1R2N4gDPmBRabyC5jHZc1SarAU",
        "4XTTMA7Xx2rsothXtRAPDzwRDxhFUkuMQgcqHApyujZo9VjLP",
        "4XTTM2dBYELgrkXPrndRqVrQcFSKW5WLMhjNbEZttNgSt52R6",
        "4XTTM5XbbA4mMqqfvzC2Q8yobB7m4AS8cn1GyV15YCbw7KfcQ",
        "4XTTM3HCMGYPrqitffAqAQCscmgdmtQRDd8Qf2zJxmJUAjZJd",
        "4XTTM3tZDjBYLT3Jw3Ze4NpiRBB3XPnN3MGkrtmTKkAyLdm4e",
        "4XTTMDctv8ihvJJZ4kUr5SCVyrEfEtN2XvSahj5ff7aXWPDrU",
        "4XTTMDR4QxYo54NLz5Q1aRxLDaryUvZeU6rr3h8mCHyUea7CE",
        "4XTTMG6BieoqVvkGif23TCurNZi35GySrPLM3RkhC4gYgHz6X",
        "4XTTM3DjXKnyoykRoLAGkM2ZE5qK1ajgKQJfsLnfAvMmRGGkA",
        "4XTTM6iaodfksmwF9y35dxYfgdc3dbeBakZfJJ8d1BTV3Kcfh",
        "4XTTMAmiQrRSmAX5kiVv32NvoyPvePGkEYBhcPXWG7fBWwP9m",
        "4XTTMEdpquV4DA94rJZkumThdTWVkmLUMJ9iUCA3Uit7gWDUk",
        "4XTTMG9rK68J8sKXT7HCXcg3eRQYEnPneg5LmhBNrhbJd6MZW",
    ];

    /// The committed `chain-configs/mainnet` directory of this checkout.
    fn mainnet_chain_configs_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("chain-configs")
            .join("mainnet")
    }

    /// The committed genesis ceremony inputs: one node-info file per genesis validator Safe.
    fn mainnet_validators_dir() -> PathBuf {
        mainnet_chain_configs_dir().join("validators")
    }

    /// The name of genesis validator `index`'s node-info file. That validator stakes under
    /// `genesis_validator_safes()[index]`.
    fn mainnet_validator_file_name(index: usize) -> String {
        format!("validator-{index:02}.yaml")
    }

    /// Read the genesis validator inputs in `dir` and check them against `safes`.
    ///
    /// `dir` must hold exactly one file per Safe, named by [`mainnet_validator_file_name`], and
    /// nothing else. File `N` must parse as node-info, name Safe `N` as its execution address,
    /// carry a proof of possession that verifies for its BLS key and Safe `N`, and use a BLS key
    /// that no other file uses. Returns the parsed files in Safe order.
    ///
    /// The ceremony verifies each proof against whatever address its file names, and its error
    /// does not name the file. It also keys validators by BLS key, so a key listed twice would
    /// silently seat one validator fewer.
    fn check_mainnet_validators(dir: &Path, safes: &[Address]) -> eyre::Result<Vec<NodeInfo>> {
        let expected: BTreeSet<String> =
            (0..safes.len()).map(mainnet_validator_file_name).collect();
        let found = std::fs::read_dir(dir)
            .wrap_err_with(|| format!("read validators directory {}", dir.display()))?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<eyre::Result<BTreeSet<String>>>()?;
        eyre::ensure!(
            found == expected,
            "{} must hold exactly the {} files {expected:?}, one for each genesis validator Safe, \
             but holds {found:?}",
            dir.display(),
            expected.len()
        );

        let mut bls_keys = BTreeSet::new();
        safes
            .iter()
            .enumerate()
            .map(|(index, safe)| {
                let path = dir.join(mainnet_validator_file_name(index));
                let bytes =
                    std::fs::read(&path).wrap_err_with(|| format!("read {}", path.display()))?;
                let validator: NodeInfo = serde_yaml::from_slice(&bytes)
                    .wrap_err_with(|| format!("{} is not a node-info file", path.display()))?;
                eyre::ensure!(
                    validator.execution_address == *safe,
                    "{} names execution address {} but genesis validator {index} stakes under \
                     Safe {safe}",
                    path.display(),
                    validator.execution_address
                );
                verify_proof_of_possession_bls(
                    &validator.proof_of_possession,
                    &validator.bls_public_key,
                    safe,
                )
                .wrap_err_with(|| {
                    format!(
                        "{}: the proof of possession does not verify for its BLS key and Safe \
                         {safe}",
                        path.display()
                    )
                })?;
                eyre::ensure!(
                    bls_keys.insert(validator.bls_public_key),
                    "{} repeats the BLS key {} of an earlier file",
                    path.display(),
                    validator.bls_public_key
                );
                Ok(validator)
            })
            .collect()
    }

    /// Copy every entry of the committed validators directory `source` into the ceremony's
    /// validators directory `staged`, then check the copies with [`check_mainnet_validators`].
    ///
    /// The copies are what the ceremony reads, so checking them rather than `source` leaves no
    /// window in which a file can change between the check and the ceremony. Copying every
    /// entry makes a stray file in `source` fail the check instead of being skipped.
    fn stage_mainnet_validators(
        source: &Path,
        staged: &Path,
        safes: &[Address],
    ) -> eyre::Result<()> {
        for entry in std::fs::read_dir(source)
            .wrap_err_with(|| format!("read validators directory {}", source.display()))?
        {
            let entry = entry?;
            std::fs::copy(entry.path(), staged.join(entry.file_name()))
                .wrap_err_with(|| format!("copy {} into the ceremony", entry.path().display()))?;
        }
        check_mainnet_validators(staged, safes)?;
        Ok(())
    }

    /// Mint throwaway placeholder validators into `chain-configs/mainnet/validators`, one for
    /// each genesis validator Safe.
    ///
    /// The keys are throwaway. Each validator is minted by the in-process `keytool generate
    /// validator` in a temporary datadir, with its proof of possession signed over
    /// `genesis_validator_safes()[N]`, and only its public `node-info.yaml` is kept. The private
    /// halves are discarded with the temporary datadirs when the test ends, so no node can run
    /// as a placeholder validator. Real mainnet keys are never produced by this test: every
    /// operator generates their own keys on their own machine and submits only the node-info
    /// file.
    ///
    /// Placeholder `N` is named `placeholder-validator-NN` and advertises one worker on
    /// `192.0.2.(10 + N)`, port 49590 for the primary and 49594 for the worker. 192.0.2.0/24 is
    /// TEST-NET-1 (RFC 5737), which is never routed. Each run mints new keys, so it rewrites all
    /// the files.
    ///
    /// The test checks the minted set with [`check_mainnet_validators`] before it replaces
    /// anything, and it refuses to replace a file whose `name` does not start with
    /// [`PLACEHOLDER_VALIDATOR_NAME_PREFIX`], so it never overwrites collected node-info files.
    ///
    /// HOW to run: `cargo test -p telcoin-network-cli mint_mainnet_placeholder_validators --
    /// --ignored`, then run [`regenerate_mainnet_chain_configs`], pin the new keys in
    /// [`PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS`] and [`PLACEHOLDER_VALIDATOR_NETWORK_KEYS`], and
    /// commit the validator files together with the regenerated configs and the pins.
    #[test]
    #[ignore = "replaces the placeholder files in chain-configs/mainnet/validators with freshly minted throwaway validators; run explicitly, then regenerate_mainnet_chain_configs"]
    fn mint_mainnet_placeholder_validators() {
        use crate::keytool::KeyArgs;

        let safes = genesis_validator_safes();
        let tmp = tempfile::tempdir().expect("tempdir for the throwaway placeholder keys");
        let minted = tmp.path().join("validators");
        std::fs::create_dir_all(&minted).expect("create the minted validators dir");
        safes.iter().enumerate().for_each(|(index, safe)| {
            let name = format!("{PLACEHOLDER_VALIDATOR_NAME_PREFIX}{index:02}");
            let address = safe.to_string();
            let host = format!("192.0.2.{}", 10 + index);
            let primary = format!("/ip4/{host}/udp/49590/quic-v1");
            let worker = format!("/ip4/{host}/udp/49594/quic-v1");
            let datadir = tmp.path().join(&name);
            let keytool = CommandParser::<KeyArgs>::parse_from([
                "tn",
                "generate",
                "validator",
                "--address",
                address.as_str(),
                "--name",
                name.as_str(),
                "--workers",
                "1",
                "--external-primary-addr",
                primary.as_str(),
                "--external-worker-addrs",
                worker.as_str(),
            ]);
            keytool.args.execute(datadir.clone(), None).expect("keytool generate validator");
            std::fs::copy(
                datadir.node_info_path(),
                minted.join(mainnet_validator_file_name(index)),
            )
            .expect("copy node-info.yaml into the minted validators dir");
        });
        check_mainnet_validators(&minted, &safes)
            .expect("minted placeholders must pass the ceremony input checks");

        let target = mainnet_validators_dir();
        std::fs::create_dir_all(&target).expect("create chain-configs/mainnet/validators");
        let existing: Vec<PathBuf> = std::fs::read_dir(&target)
            .expect("read chain-configs/mainnet/validators")
            .map(|entry| entry.expect("read a chain-configs/mainnet/validators entry").path())
            .collect();
        existing.iter().for_each(|path| {
            let validator: Option<NodeInfo> =
                std::fs::read(path).ok().and_then(|bytes| serde_yaml::from_slice(&bytes).ok());
            assert!(
                validator
                    .as_ref()
                    .is_some_and(|info| info.name.starts_with(PLACEHOLDER_VALIDATOR_NAME_PREFIX)),
                "{} is not a placeholder minted by this test; it never replaces collected \
                 node-info files, so remove the file by hand if a placeholder must replace it",
                path.display()
            );
        });
        existing.iter().for_each(|path| {
            std::fs::remove_file(path)
                .unwrap_or_else(|e| panic!("remove placeholder {}: {e}", path.display()))
        });
        (0..safes.len()).for_each(|index| {
            let name = mainnet_validator_file_name(index);
            std::fs::copy(minted.join(&name), target.join(&name))
                .unwrap_or_else(|e| panic!("write placeholder {name}: {e}"));
        });
        check_mainnet_validators(&target, &safes)
            .expect("written placeholders must pass the ceremony input checks");
    }

    /// Regenerate the committed `chain-configs/mainnet/{genesis,committee,parameters}.yaml` from
    /// the committed validator inputs and the CURRENT tn-contracts artifacts (issue #1063).
    ///
    /// WHEN to run: after a tn-contracts submodule/artifact bump, after a change to
    /// `chain-configs/mainnet/validators`, or whenever a mainnet gate fails
    /// (`mainnet_genesis_registry_code_matches_current_artifact` /
    /// `mainnet_chain_configs_load_via_the_node_paths` in `crates/config`,
    /// `test_mainnet_genesis_registry_serves_post_fork_reads` in `crates/tn-reth`, or
    /// `mainnet_genesis_encodes_the_six_hour_epoch_target` in this module).
    ///
    /// HOW to run: `cargo test -p telcoin-network-cli regenerate_mainnet_chain_configs --
    /// --ignored` then commit the three rewritten yaml files.
    ///
    /// `bootstrap_servers.yaml` is not regenerated: the bootstrap servers are chosen by hand and
    /// need not be validators, so the ceremony's committee file is written with an empty
    /// `bootstrap_servers` map (kept, not dropped, because older binaries require the key).
    ///
    /// This is the full genesis ceremony (`GenesisArgs::execute`) driven in-process the same
    /// way the e2e suite drives it (`crates/e2e-tests/src/lib.rs`): stage the committed
    /// node-info files `chain-configs/mainnet/validators/validator-00.yaml` to `validator-09.yaml`
    /// as the ceremony's validators ([`stage_mainnet_validators`] refuses a set that does not
    /// hold one file per genesis validator Safe, each staked under and signed over its Safe),
    /// then run the ceremony with flags mirroring the committed placeholder identity:
    /// - chain id 0x2db (731), unchanged from the committed genesis;
    /// - owner [`MAINNET_PLACEHOLDER_OWNER`] and dev-funded account
    ///   [`MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT`];
    /// - basefee address [`MAINNET_PLACEHOLDER_BASEFEE_ADDRESS`], preserving the committed
    ///   parameters.yaml;
    /// - epoch duration [`MAINNET_EPOCH_DURATION_SECS`] (21600s, the mainnet target of issue
    ///   #1512). Do not decode this value from the committed file: that copied a stale 86400s into
    ///   each regeneration;
    /// - epoch issuance [`MAINNET_EPOCH_ISSUANCE_TEL`] (25,806 TEL), passed explicitly (stake
    ///   amount / min withdrawal stay on the clap defaults, which also match the committed storage:
    ///   1M TEL / 1k TEL);
    /// - 1s max/min header delays, preserving the committed parameters.yaml.
    ///
    /// The test mints no keys, so two runs over the same validator files differ only in the
    /// genesis timestamp, which the ceremony takes from the wall clock. Until the collected
    /// node-info files replace them, the committed validator files are the throwaway placeholders
    /// of [`mint_mainnet_placeholder_validators`] (`chain-configs/README.md`).
    ///
    /// All three ceremony outputs are read into memory BEFORE any file under
    /// `chain-configs/mainnet` is written, so a missing/unreadable output aborts without
    /// leaving a half-regenerated (2-of-3) config set behind.
    #[test]
    #[ignore = "regenerates chain-configs/mainnet in the working tree from its validator files and the current tn-contracts artifacts; run explicitly after an artifact bump, a validator file change, or when the mainnet config gates fail"]
    fn regenerate_mainnet_chain_configs() {
        let tmp = tempfile::tempdir().expect("tempdir for the mainnet regeneration ceremony");
        let shared = tmp.path().join("mainnet-genesis");
        let validators_dir = shared.join("genesis").join("validators");
        std::fs::create_dir_all(&validators_dir).expect("create genesis validators dir");
        stage_mainnet_validators(
            &mainnet_validators_dir(),
            &validators_dir,
            &genesis_validator_safes(),
        )
        .expect(
            "chain-configs/mainnet/validators must hold one node-info file per genesis validator \
             Safe, staked under and signed over that Safe",
        );

        let owner = MAINNET_PLACEHOLDER_OWNER.to_string();
        let dev_funded_account = MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT.to_string();
        let basefee_address = MAINNET_PLACEHOLDER_BASEFEE_ADDRESS.to_string();
        let epoch_duration = MAINNET_EPOCH_DURATION_SECS.to_string();
        let epoch_issuance = MAINNET_EPOCH_ISSUANCE_TEL.to_string();
        let ceremony = CommandParser::<GenesisArgs>::parse_from([
            "tn",
            "--chain-id",
            "0x2db",
            "--consensus-registry-owner",
            owner.as_str(),
            "--dev-funded-account",
            dev_funded_account.as_str(),
            "--basefee-address",
            basefee_address.as_str(),
            "--epoch-duration-in-secs",
            epoch_duration.as_str(),
            "--epoch-block-rewards",
            epoch_issuance.as_str(),
            "--max-header-delay-ms",
            "1000",
            "--min-header-delay-ms",
            "1000",
        ]);
        ceremony.args.execute(shared.clone()).expect("mainnet genesis ceremony");

        // splice the ceremony output over the committed placeholders: read ALL three
        // outputs first (any read failure aborts before chain-configs is touched), then
        // write, so a partial ceremony can never leave a half-regenerated config set
        let target = mainnet_chain_configs_dir();
        let outputs = [
            (shared.join("genesis").join("genesis.yaml"), target.join("genesis.yaml")),
            (shared.join("genesis").join("committee.yaml"), target.join("committee.yaml")),
            (shared.join("parameters.yaml"), target.join("parameters.yaml")),
        ];
        let committee_destination = target.join("committee.yaml");
        let contents: Vec<(std::path::PathBuf, Vec<u8>)> = outputs
            .into_iter()
            .map(|(source, destination)| {
                let bytes = std::fs::read(&source)
                    .unwrap_or_else(|e| panic!("read ceremony output {}: {e}", source.display()));
                if destination != committee_destination {
                    return (destination, bytes);
                }
                // the ceremony lists its validators as bootstrap servers; the bundled chain keeps
                // its own list in bootstrap_servers.yaml, so the committee file's map is emptied
                // in place (older binaries require the key)
                let mut committee: serde_yaml::Value = serde_yaml::from_slice(&bytes)
                    .unwrap_or_else(|e| panic!("parse ceremony committee.yaml: {e}"));
                *committee
                    .as_mapping_mut()
                    .expect("ceremony committee.yaml is a mapping")
                    .get_mut(&serde_yaml::Value::String("bootstrap_servers".into()))
                    .expect("ceremony committee.yaml lists bootstrap_servers") =
                    serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
                let yaml = serde_yaml::to_string(&committee)
                    .unwrap_or_else(|e| panic!("serialize regenerated committee.yaml: {e}"));
                (destination, yaml.into_bytes())
            })
            .collect();
        contents.iter().for_each(|(destination, bytes)| {
            std::fs::write(destination, bytes)
                .unwrap_or_else(|e| panic!("write regenerated {}: {e}", destination.display()))
        });
    }

    /// The committed mainnet committee seats exactly the ten genesis validators, with distinct BLS
    /// keys, each under its own Safe: the authority holding the BLS key of
    /// `chain-configs/mainnet/validators/validator-NN.yaml` stakes under
    /// `genesis_validator_safes()[NN]`.
    ///
    /// `mainnet_chain_configs_load_via_the_node_paths` in `crates/config` compares the
    /// committee's stake addresses with the Safes as a set. This gate ties each committed
    /// validator file to its seat, so a `committee.yaml` generated from other validator files
    /// fails here.
    #[test]
    fn mainnet_committee_seats_each_validator_file_under_its_safe() -> eyre::Result<()> {
        let safes = genesis_validator_safes();
        let validators = check_mainnet_validators(&mainnet_validators_dir(), &safes)?;
        let committee: Committee = serde_yaml::from_str(MAINNET_COMMITTEE)?;
        let bls_keys: BTreeSet<_> =
            committee.authorities().iter().map(|authority| *authority.protocol_key()).collect();
        assert_eq!(
            (committee.size(), bls_keys.len()),
            (safes.len(), safes.len()),
            "chain-configs/mainnet/committee.yaml must seat {} authorities with distinct BLS keys \
             - {MAINNET_REGENERATION_LEVER}",
            safes.len()
        );
        for (index, (validator, safe)) in validators.iter().zip(&safes).enumerate() {
            let seat = committee
                .authority_by_key(&validator.bls_public_key)
                .map(|authority| authority.execution_address());
            assert_eq!(
                seat,
                Some(*safe),
                "chain-configs/mainnet/committee.yaml must seat the BLS key of \
                 chain-configs/mainnet/validators/{} under Safe {safe} - \
                 {MAINNET_REGENERATION_LEVER}",
                mainnet_validator_file_name(index)
            );
        }
        Ok(())
    }

    /// The embedded mainnet genesis hashes to [`MAINNET_GENESIS_HASH`] and carries the timestamp
    /// [`MAINNET_GENESIS_TIMESTAMP`] (issue #1735).
    ///
    /// The hash is the one the node starts on: the sealed genesis header of the chain spec built
    /// from the genesis, which covers the header fields and the state root of the alloc. Any
    /// change to the bundled mainnet genesis changes it, a regeneration included, because the
    /// ceremony stamps the wall clock into the timestamp.
    #[test]
    fn mainnet_genesis_hash_is_pinned() -> eyre::Result<()> {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        let timestamp = genesis.timestamp;
        let hash = RethChainSpec::from(genesis).sealed_genesis_header().hash();
        assert_eq!(
            (hash, timestamp),
            (MAINNET_GENESIS_HASH, MAINNET_GENESIS_TIMESTAMP),
            "chain-configs/mainnet/genesis.yaml now hashes to {hash} with timestamp {timestamp}, \
             but crates/types/src/genesis.rs pins MAINNET_GENESIS_HASH {MAINNET_GENESIS_HASH} and \
             MAINNET_GENESIS_TIMESTAMP {MAINNET_GENESIS_TIMESTAMP}. The two constants change only \
             together with the regenerated chain-configs/mainnet files, in one commit: if this \
             change regenerated them, set the constants to the values above"
        );
        Ok(())
    }

    /// The genesis timestamp of the final mainnet genesis: 2026-12-02 12:31:00 UTC.
    const MAINNET_LAUNCH_TIMESTAMP: u64 = 1_796_214_660;

    /// Why a mainnet peer cannot reach `address`, or `None` when it starts with a public IP
    /// address and names no port 0.
    ///
    /// The node dials the IP address of a multiaddr and does not resolve DNS names, so a name is
    /// refused like a non-public address. The non-public hosts are loopback, unspecified,
    /// private (RFC 1918, fc00::/7), carrier-grade NAT (100.64.0.0/10), link-local
    /// (169.254.0.0/16, fe80::/10), documentation (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24,
    /// 2001:db8::/32), multicast (224.0.0.0/4, ff00::/8), the broadcast address and the reserved
    /// IPv4 ranges 0.0.0.0/8, 192.0.0.0/24, 198.18.0.0/15 and 240.0.0.0/4.
    fn unreachable_address_reason(address: &Multiaddr) -> Option<&'static str> {
        let ipv4_reason = |ip: std::net::Ipv4Addr| {
            let octets = ip.octets();
            if ip.is_loopback() {
                Some("a loopback address")
            } else if ip.is_unspecified() {
                Some("the unspecified address")
            } else if ip.is_private() {
                Some("a private address")
            } else if matches!(octets, [100, 64..=127, _, _]) {
                Some("a carrier-grade NAT address")
            } else if ip.is_link_local() {
                Some("a link-local address")
            } else if matches!(octets, [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]) {
                Some("in a documentation range")
            } else if ip.is_multicast() {
                Some("a multicast address")
            } else if ip.is_broadcast() {
                Some("the broadcast address")
            } else if matches!(
                octets,
                [0, ..] | [192, 0, 0, _] | [198, 18..=19, _, _] | [240..=255, ..]
            ) {
                Some("in a reserved range")
            } else {
                None
            }
        };
        let ipv6_reason = |ip: std::net::Ipv6Addr| {
            let segments = ip.segments();
            if ip.is_loopback() {
                Some("a loopback address")
            } else if ip.is_unspecified() {
                Some("the unspecified address")
            } else if ip.is_unique_local() {
                Some("a private (unique local) address")
            } else if ip.is_unicast_link_local() {
                Some("a link-local address")
            } else if ip.is_multicast() {
                Some("a multicast address")
            } else if segments[..2] == [0x2001, 0x0db8] {
                Some("in a documentation range")
            } else {
                None
            }
        };
        let host_reason = match address.iter().next() {
            Some(Protocol::Ip4(ip)) => ipv4_reason(ip),
            Some(Protocol::Ip6(ip)) => {
                ip.to_ipv4_mapped().map_or_else(|| ipv6_reason(ip), ipv4_reason)
            }
            Some(
                Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_),
            ) => Some("a DNS name, which the node does not resolve"),
            _ => Some("not an IP address"),
        };
        host_reason.or_else(|| {
            address
                .iter()
                .any(|protocol| matches!(protocol, Protocol::Udp(0) | Protocol::Tcp(0)))
                .then_some("on port 0, which no peer can dial")
        })
    }

    /// [`unreachable_address_reason`] passes public IP addresses and refuses every other kind of
    /// host the final-data gate names.
    #[test]
    fn unreachable_address_reason_refuses_non_public_hosts() {
        let reason = |address: &str| {
            unreachable_address_reason(&address.parse().expect("test multiaddr parses"))
        };
        for public in [
            "/ip4/8.8.8.8/udp/49590/quic-v1",
            "/ip6/2606:4700::1111/udp/49590/quic-v1",
            "/ip4/100.63.255.254/udp/49590/quic-v1",
            "/ip4/100.128.0.1/udp/49590/quic-v1",
            "/ip4/198.17.255.254/udp/49590/quic-v1",
            "/ip4/198.20.0.1/udp/49590/quic-v1",
            "/ip4/223.255.255.254/udp/49590/quic-v1",
        ] {
            assert_eq!(reason(public), None, "{public} is a public address");
        }
        for (address, expected) in [
            ("/ip4/127.0.0.1/udp/49590/quic-v1", "a loopback address"),
            ("/ip6/::1/udp/49590/quic-v1", "a loopback address"),
            ("/ip4/0.0.0.0/udp/49590/quic-v1", "the unspecified address"),
            ("/ip6/::/udp/49590/quic-v1", "the unspecified address"),
            ("/ip4/10.1.2.3/udp/49590/quic-v1", "a private address"),
            ("/ip4/172.16.0.1/udp/49590/quic-v1", "a private address"),
            ("/ip4/192.168.1.1/udp/49590/quic-v1", "a private address"),
            ("/ip6/::ffff:10.0.0.1/udp/49590/quic-v1", "a private address"),
            ("/ip6/fd00::1/udp/49590/quic-v1", "a private (unique local) address"),
            ("/ip6/fc00::1/udp/49590/quic-v1", "a private (unique local) address"),
            ("/ip4/100.64.0.1/udp/49590/quic-v1", "a carrier-grade NAT address"),
            ("/ip4/100.127.255.254/udp/49590/quic-v1", "a carrier-grade NAT address"),
            ("/ip4/169.254.1.1/udp/49590/quic-v1", "a link-local address"),
            ("/ip6/fe80::1/udp/49590/quic-v1", "a link-local address"),
            ("/ip4/224.0.0.1/udp/49590/quic-v1", "a multicast address"),
            ("/ip4/239.255.255.250/udp/49590/quic-v1", "a multicast address"),
            ("/ip6/ff02::1/udp/49590/quic-v1", "a multicast address"),
            ("/ip4/255.255.255.255/udp/49590/quic-v1", "the broadcast address"),
            ("/ip4/0.1.2.3/udp/49590/quic-v1", "in a reserved range"),
            ("/ip4/192.0.0.8/udp/49590/quic-v1", "in a reserved range"),
            ("/ip4/198.18.0.1/udp/49590/quic-v1", "in a reserved range"),
            ("/ip4/198.19.255.254/udp/49590/quic-v1", "in a reserved range"),
            ("/ip4/240.0.0.1/udp/49590/quic-v1", "in a reserved range"),
            ("/ip6/::ffff:100.64.0.1/udp/49590/quic-v1", "a carrier-grade NAT address"),
            ("/ip4/8.8.8.8/udp/0/quic-v1", "on port 0, which no peer can dial"),
            ("/ip6/2606:4700::1111/udp/0/quic-v1", "on port 0, which no peer can dial"),
            ("/ip4/192.0.2.10/udp/49590/quic-v1", "in a documentation range"),
            ("/ip4/198.51.100.7/udp/49590/quic-v1", "in a documentation range"),
            ("/ip4/203.0.113.9/udp/49590/quic-v1", "in a documentation range"),
            ("/ip6/2001:db8::1/udp/49590/quic-v1", "in a documentation range"),
            (
                "/dns4/validator.example.org/udp/49590/quic-v1",
                "a DNS name, which the node does not resolve",
            ),
            (
                "/dns/validator.example.org/udp/49590/quic-v1",
                "a DNS name, which the node does not resolve",
            ),
        ] {
            assert_eq!(reason(address), Some(expected), "{address}");
        }
    }

    /// The network addresses a node advertises, each labelled with its role.
    fn labelled_network_addresses(
        primary: &P2pNode,
        workers: &[P2pNode],
    ) -> Vec<(String, Multiaddr)> {
        std::iter::once(("primary".to_string(), primary.network_address.clone()))
            .chain(
                workers
                    .iter()
                    .enumerate()
                    .map(|(id, worker)| (format!("worker {id}"), worker.network_address.clone())),
            )
            .collect()
    }

    /// The identity keys of `validator`, each labelled with its role and whether it is one of
    /// the placeholder keys [`PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS`] and
    /// [`PLACEHOLDER_VALIDATOR_NETWORK_KEYS`].
    ///
    /// The keys are compared in their base58 form, which is how the node-info files and the
    /// pinned lists hold them.
    fn labelled_identity_keys(validator: &NodeInfo) -> Vec<(String, bool)> {
        let p2p = &validator.p2p_info;
        let bls_key = validator.bls_public_key.to_string();
        let network_keys = std::iter::once(("primary".to_string(), &p2p.primary))
            .chain(
                p2p.workers.iter().enumerate().map(|(id, worker)| (format!("worker {id}"), worker)),
            )
            .map(|(role, node)| {
                let key = node.network_key.to_string();
                (
                    format!("{role} network key"),
                    PLACEHOLDER_VALIDATOR_NETWORK_KEYS.contains(&key.as_str()),
                )
            });
        std::iter::once((
            "BLS key".to_string(),
            PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS.contains(&bls_key.as_str()),
        ))
        .chain(network_keys)
        .collect()
    }

    /// The storage slots of `account` in `genesis` whose word holds `address` at any byte offset.
    ///
    /// An `Ownable` owner shares its slot with other packed fields, so the search does not depend
    /// on where a contract's storage layout puts it.
    fn slots_holding_address(genesis: &Genesis, account: Address, address: Address) -> Vec<B256> {
        genesis
            .alloc
            .get(&account)
            .and_then(|account| account.storage.as_ref())
            .into_iter()
            .flatten()
            .filter(|(_, word)| {
                word.0.windows(address.len()).any(|window| window == address.as_slice())
            })
            .map(|(slot, _)| *slot)
            .collect()
    }

    /// The committed mainnet chain configs hold the final mainnet data and no placeholder.
    ///
    /// The commit that installs the final mainnet data removes the `#[ignore]`. Until then the
    /// gate fails. It reads the committed files and reports, one message per item and naming the
    /// file to fix:
    /// - a genesis timestamp other than [`MAINNET_LAUNCH_TIMESTAMP`];
    /// - a `ConsensusRegistry` or `WorkerConfigs` whose storage holds the placeholder owner
    ///   [`MAINNET_PLACEHOLDER_OWNER`];
    /// - a balance on the placeholder dev-funded account
    ///   [`MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT`];
    /// - the placeholder basefee recipient [`MAINNET_PLACEHOLDER_BASEFEE_ADDRESS`] in
    ///   `parameters.yaml`;
    /// - a validator file whose `name` starts with [`PLACEHOLDER_VALIDATOR_NAME_PREFIX`], or that
    ///   holds a placeholder BLS or network key under any `name`
    ///   ([`PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS`], [`PLACEHOLDER_VALIDATOR_NETWORK_KEYS`]), in
    ///   one message for the file that names every reason;
    /// - a validator network address, or an address in `bootstrap_servers.yaml`, that
    ///   [`unreachable_address_reason`] refuses: a non-public host such as a loopback, private,
    ///   carrier-grade NAT, link-local, documentation, multicast or reserved address, a DNS name,
    ///   which the node does not resolve, or port 0;
    /// - a `bootstrap_servers.yaml` that lists no server.
    ///
    /// A validator file's network addresses are not signed by its proof of possession, which
    /// covers only the BLS key and the execution address, and they do not enter the genesis or
    /// the committed committee (the regeneration empties its bootstrap map), so they matter only
    /// where `bootstrap_servers.yaml` is written from them.
    ///
    /// The owner, the dev-funded account and the basefee recipient are launch decisions, passed
    /// as arguments of [`regenerate_mainnet_chain_configs`]. The gate compares them with the
    /// placeholder constants that the regeneration passes today, so it cannot drift from what the
    /// regeneration writes. The final-data commit also decides the funding of the Issuance
    /// contract and the TEL total-supply slot, which this gate does not check.
    #[test]
    #[ignore = "fails until the commit that installs the final mainnet data, which removes this attribute"]
    fn mainnet_final_data_is_complete() -> eyre::Result<()> {
        let mut problems = Vec::new();

        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)?;
        if genesis.timestamp != MAINNET_LAUNCH_TIMESTAMP {
            problems.push(format!(
                "chain-configs/mainnet/genesis.yaml: the genesis timestamp is {}, not the launch \
                 timestamp {MAINNET_LAUNCH_TIMESTAMP} (2026-12-02 12:31:00 UTC)",
                genesis.timestamp
            ));
        }
        for (contract, account) in [
            ("ConsensusRegistry", CONSENSUS_REGISTRY_ADDRESS),
            ("WorkerConfigs", WORKER_CONFIGS_ADDRESS),
        ] {
            let slots = slots_holding_address(&genesis, account, MAINNET_PLACEHOLDER_OWNER);
            if !slots.is_empty() {
                problems.push(format!(
                    "chain-configs/mainnet/genesis.yaml: the {contract} owner is the placeholder \
                     {MAINNET_PLACEHOLDER_OWNER} (storage slots {slots:?}); the owner is a launch \
                     decision, set by `--consensus-registry-owner` in \
                     regenerate_mainnet_chain_configs"
                ));
            }
        }
        if let Some(account) = genesis
            .alloc
            .get(&MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT)
            .filter(|account| !account.balance.is_zero())
        {
            problems.push(format!(
                "chain-configs/mainnet/genesis.yaml: the placeholder dev-funded account \
                 {MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT} holds {} wei; the dev-funded account is a \
                 launch decision, set by `--dev-funded-account` in regenerate_mainnet_chain_configs",
                account.balance
            ));
        }
        let parameters: Parameters = serde_yaml::from_str(MAINNET_PARAMETERS)?;
        if parameters.basefee_address == MAINNET_PLACEHOLDER_BASEFEE_ADDRESS {
            problems.push(format!(
                "chain-configs/mainnet/parameters.yaml: the basefee recipient is the placeholder \
                 {MAINNET_PLACEHOLDER_BASEFEE_ADDRESS}; the recipient is a launch decision, set by \
                 `--basefee-address` in regenerate_mainnet_chain_configs"
            ));
        }

        let validators =
            check_mainnet_validators(&mainnet_validators_dir(), &genesis_validator_safes())?;
        for (index, validator) in validators.iter().enumerate() {
            let file =
                format!("chain-configs/mainnet/validators/{}", mainnet_validator_file_name(index));
            let mut reasons = Vec::new();
            if validator.name.starts_with(PLACEHOLDER_VALIDATOR_NAME_PREFIX) {
                reasons.push(format!(
                    "`{}` is a placeholder, not a collected node-info file",
                    validator.name
                ));
            }
            let placeholder_keys: Vec<String> = labelled_identity_keys(validator)
                .into_iter()
                .filter_map(|(key, placeholder)| placeholder.then_some(key))
                .collect();
            if !placeholder_keys.is_empty() {
                reasons.push(format!(
                    "the file holds placeholder keys, whose private keys do not exist: the {}",
                    placeholder_keys.join(", the ")
                ));
            }
            if !reasons.is_empty() {
                problems.push(format!("{file}: {}", reasons.join("; ")));
            }
            let p2p = &validator.p2p_info;
            for (role, address) in labelled_network_addresses(&p2p.primary, &p2p.workers) {
                if let Some(reason) = unreachable_address_reason(&address) {
                    problems.push(format!("{file}: the {role} address {address} is {reason}"));
                }
            }
        }

        let bootstrap_file = "chain-configs/mainnet/bootstrap_servers.yaml";
        match serde_yaml::from_str::<Option<BTreeMap<BlsPublicKey, BootstrapServer>>>(
            MAINNET_BOOTSTRAP_SERVERS,
        ) {
            Err(e) => problems.push(format!("{bootstrap_file} does not parse: {e}")),
            Ok(servers) => {
                let servers = servers.unwrap_or_default();
                if servers.is_empty() {
                    problems.push(format!("{bootstrap_file} lists no bootstrap server"));
                }
                for (key, server) in &servers {
                    for (role, address) in
                        labelled_network_addresses(&server.primary, &server.workers)
                    {
                        if let Some(reason) = unreachable_address_reason(&address) {
                            problems.push(format!(
                                "{bootstrap_file}: the {role} address {address} of server {key} \
                                 is {reason}"
                            ));
                        }
                    }
                }
            }
        }

        assert!(
            problems.is_empty(),
            "the committed mainnet chain configs do not hold the final mainnet data yet:\n{}",
            problems.join("\n")
        );
        Ok(())
    }

    /// Every committed validator file whose `name` starts with
    /// [`PLACEHOLDER_VALIDATOR_NAME_PREFIX`] holds only pinned placeholder keys.
    ///
    /// [`mainnet_final_data_is_complete`] refuses the placeholders by their keys as well as by
    /// their names, so the pins must list the keys of the committed placeholders. Each run of
    /// [`mint_mainnet_placeholder_validators`] mints new keys and fails this test until the pins
    /// are updated. Once the collected files replace the placeholders, no file carries the prefix
    /// and the test has nothing to compare.
    #[test]
    fn mainnet_placeholder_validator_keys_are_pinned() -> eyre::Result<()> {
        let validators =
            check_mainnet_validators(&mainnet_validators_dir(), &genesis_validator_safes())?;
        for (index, validator) in validators.iter().enumerate() {
            if !validator.name.starts_with(PLACEHOLDER_VALIDATOR_NAME_PREFIX) {
                continue;
            }
            let unpinned: Vec<String> = labelled_identity_keys(validator)
                .into_iter()
                .filter_map(|(key, placeholder)| (!placeholder).then_some(key))
                .collect();
            assert!(
                unpinned.is_empty(),
                "chain-configs/mainnet/validators/{}: the placeholder `{}` holds keys that are not \
                 pinned ({}); update PLACEHOLDER_VALIDATOR_BLS_PUBLIC_KEYS and \
                 PLACEHOLDER_VALIDATOR_NETWORK_KEYS to the keys of the committed placeholder files",
                mainnet_validator_file_name(index),
                validator.name,
                unpinned.join(", ")
            );
        }
        Ok(())
    }

    /// A node-info file for a validator whose BLS key is derived from `seed`, naming
    /// `execution_address` and carrying a proof of possession signed over `signed_address`.
    fn signed_validator(
        seed: u8,
        execution_address: Address,
        signed_address: Address,
    ) -> eyre::Result<NodeInfo> {
        let keypair = BlsKeypair::generate(&mut StdRng::from_seed([seed; 32]));
        let p2p_node = || -> P2pNode {
            (NetworkKeypair::generate_ed25519().public().clone().into(), Multiaddr::empty()).into()
        };
        Ok(NodeInfo {
            name: format!("{PLACEHOLDER_VALIDATOR_NAME_PREFIX}{seed:02}"),
            bls_public_key: *keypair.public(),
            p2p_info: NodeP2pInfo::new(p2p_node(), vec![p2p_node()]),
            execution_address,
            proof_of_possession: generate_proof_of_possession_bls_for_test(
                &keypair,
                &signed_address,
            )?,
        })
    }

    /// One valid node-info file for each Safe in `safes`, in Safe order, with distinct keys.
    fn signed_validators(safes: &[Address]) -> eyre::Result<Vec<NodeInfo>> {
        safes.iter().zip(1_u8..).map(|(safe, seed)| signed_validator(seed, *safe, *safe)).collect()
    }

    /// Write `validators` as a committed validators directory, file `N` holding entry `N`, and
    /// stage it into a ceremony directory the way [`regenerate_mainnet_chain_configs`] does.
    ///
    /// The outer result reports a broken fixture; the inner one is the staging result.
    fn stage_validators(validators: Vec<NodeInfo>) -> eyre::Result<eyre::Result<()>> {
        let directory = tempfile::tempdir()?;
        let committed = directory.path().join("committed");
        let staged = directory.path().join("staged");
        std::fs::create_dir_all(&committed)?;
        std::fs::create_dir_all(&staged)?;
        validators.into_iter().enumerate().try_for_each(|(index, validator)| {
            Config::write_to_path(
                committed.join(mainnet_validator_file_name(index)),
                validator,
                ConfigFmt::YAML,
            )
        })?;
        Ok(stage_mainnet_validators(&committed, &staged, &genesis_validator_safes()))
    }

    /// Stage `validators` and return the message of the refusal, with its causes.
    fn staging_refusal(validators: Vec<NodeInfo>) -> eyre::Result<String> {
        stage_validators(validators)?
            .err()
            .map(|error| format!("{error:#}"))
            .ok_or_else(|| eyre::eyre!("staging must refuse this validators directory"))
    }

    /// One signed file per genesis validator Safe stages, so each refusal below comes from the
    /// single defect its test introduces.
    #[test]
    fn staging_accepts_one_signed_file_per_genesis_validator_safe() -> eyre::Result<()> {
        stage_validators(signed_validators(&genesis_validator_safes())?)?
    }

    /// A missing file is refused instead of seating a smaller genesis committee.
    #[test]
    fn staging_refuses_nine_validator_files() -> eyre::Result<()> {
        let mut validators = signed_validators(&genesis_validator_safes())?;
        validators.pop();
        let refusal = staging_refusal(validators)?;
        assert!(
            refusal.contains("must hold exactly the 10 files"),
            "unexpected refusal: {refusal}"
        );
        Ok(())
    }

    /// A file staked under another validator's Safe is refused, although its proof verifies for
    /// the address it names.
    #[test]
    fn staging_refuses_a_file_staked_under_the_wrong_safe() -> eyre::Result<()> {
        let safes = genesis_validator_safes();
        let mut validators = signed_validators(&safes)?;
        validators.swap(3, 4);
        let refusal = staging_refusal(validators)?;
        assert!(
            refusal.contains(&format!(
                "validator-03.yaml names execution address {} but genesis validator 3 stakes \
                 under Safe {}",
                safes[4], safes[3]
            )),
            "unexpected refusal: {refusal}"
        );
        Ok(())
    }

    /// A proof signed over another address is refused, although the file names its own Safe.
    #[test]
    fn staging_refuses_a_proof_signed_over_another_address() -> eyre::Result<()> {
        let safes = genesis_validator_safes();
        let mut validators = signed_validators(&safes)?;
        validators[5] = signed_validator(6, safes[5], Address::repeat_byte(0x55))?;
        let refusal = staging_refusal(validators)?;
        assert!(
            refusal.contains(&format!(
                "validator-05.yaml: the proof of possession does not verify for its BLS key and \
                 Safe {}",
                safes[5]
            )),
            "unexpected refusal: {refusal}"
        );
        Ok(())
    }

    /// A BLS key in two files is refused: the ceremony keys validators by BLS key and would seat
    /// only one of them.
    #[test]
    fn staging_refuses_a_bls_key_listed_twice() -> eyre::Result<()> {
        let safes = genesis_validator_safes();
        let mut validators = signed_validators(&safes)?;
        // seed 7 is validator 6's key, signed over safe 7 so that the proof verifies
        validators[7] = signed_validator(7, safes[7], safes[7])?;
        let refusal = staging_refusal(validators)?;
        assert!(
            refusal.contains("validator-07.yaml repeats the BLS key"),
            "unexpected refusal: {refusal}"
        );
        Ok(())
    }
}
