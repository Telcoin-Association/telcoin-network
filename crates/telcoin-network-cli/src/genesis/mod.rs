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
    use tn_config::{genesis_validator_safes, NodeInfo};
    use tn_reth::system_calls::CONSENSUS_REGISTRY_ADDRESS;
    use tn_types::{
        generate_proof_of_possession_bls_for_test, test_utils::CommandParser,
        verify_proof_of_possession_bls, Address, BlsKeypair, Committee, Genesis, Multiaddr,
        NetworkKeypair, NodeP2pInfo, P2pNode, B256, MAINNET_GENESIS, U256,
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

    /// The committed mainnet placeholder owner: `--consensus-registry-owner` AND the
    /// dev-funded account of the committed `chain-configs/mainnet/genesis.yaml` (see
    /// `etc/genesis.sh`); restated here so regeneration reproduces the same identity.
    const MAINNET_PLACEHOLDER_OWNER: &str = "0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a";

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
    /// --ignored`, then run [`regenerate_mainnet_chain_configs`] and commit the validator files
    /// together with the regenerated configs.
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
    /// - owner + dev-funded account [`MAINNET_PLACEHOLDER_OWNER`];
    /// - basefee address 0x99..99, preserving the committed parameters.yaml;
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

        let epoch_duration = MAINNET_EPOCH_DURATION_SECS.to_string();
        let epoch_issuance = MAINNET_EPOCH_ISSUANCE_TEL.to_string();
        let ceremony = CommandParser::<GenesisArgs>::parse_from([
            "tn",
            "--chain-id",
            "0x2db",
            "--consensus-registry-owner",
            MAINNET_PLACEHOLDER_OWNER,
            "--dev-funded-account",
            MAINNET_PLACEHOLDER_OWNER,
            "--basefee-address",
            "0x9999999999999999999999999999999999999999",
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
