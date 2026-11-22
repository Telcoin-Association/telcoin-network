//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

use clap::Args;
use secp256k1::{
    rand::{rngs::StdRng, SeedableRng},
    Secp256k1,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};
use tn_config::{
    Config, ConfigFmt, ConfigTrait, NetworkGenesis, Parameters, TelcoinDirs as _,
    GOVERNANCE_SAFE_ADDRESS,
};
use tn_reth::{system_calls::ConsensusRegistry, RethChainSpec, RethEnv};
use tn_types::{
    keccak256, now, set_genesis_defaults_at, Address, ExecHeader, GenesisAccount, MAINNET_CHAIN_ID,
    U256,
};
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
    /// The genesis timestamp, as unix seconds or an RFC 3339 UTC timestamp.
    ///
    /// Accepts unix seconds (`1796214660`) or an RFC 3339 timestamp in UTC
    /// (`2026-12-02T12:31:00Z`). This is the earliest time any validator may create a block
    /// header. The value must be after the current time and at most 366 days ahead. Required on
    /// mainnet (chain id 731); on other chains the default is the time the ceremony runs.
    #[arg(long = "genesis-timestamp", value_name = "TIME", value_parser = parse_genesis_timestamp)]
    pub genesis_timestamp: Option<u64>,
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
        // before anything is read or written, so a refused ceremony leaves the datadir untouched
        let genesis_timestamp =
            resolve_genesis_timestamp(self.genesis_timestamp, self.chain_id, now())?;
        info!(
            target: "genesis::ceremony",
            genesis_timestamp,
            "Creating a new chain genesis with initial validators"
        );

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
        set_genesis_defaults_at(&mut genesis, genesis_timestamp);
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

/// The smallest bare integer [`parse_genesis_timestamp`] refuses as milliseconds.
///
/// 10^11 seconds is past the year 5000, while unix time in milliseconds has been above 10^12
/// since 2001, so no plausible value falls on the wrong side.
const MILLISECONDS_THRESHOLD: u64 = 100_000_000_000;

/// How far ahead of the ceremony a genesis timestamp may be: 366 days, in seconds.
const MAX_GENESIS_LEAD_SECS: u64 = 366 * 24 * 60 * 60;

/// Parse `--genesis-timestamp` as unix seconds or an RFC 3339 UTC timestamp.
///
/// A bare integer is unix seconds, and one of [`MILLISECONDS_THRESHOLD`] or more is refused.
/// Anything else goes through [`humantime::parse_rfc3339`], which accepts only UTC (`Z` or
/// `+00:00`) and refuses every other offset. A fractional second is refused because the genesis
/// timestamp is whole seconds and rounding would move it.
fn parse_genesis_timestamp(value: &str) -> eyre::Result<u64> {
    if let Ok(secs) = value.parse::<u64>() {
        if secs >= MILLISECONDS_THRESHOLD {
            eyre::bail!(
                "{secs} looks like milliseconds: pass unix seconds (below {MILLISECONDS_THRESHOLD}) \
                 or an RFC 3339 UTC timestamp such as 2026-12-02T12:31:00Z"
            );
        }
        return Ok(secs);
    }
    let since_epoch = humantime::parse_rfc3339(value)
        .map_err(|e| {
            eyre::eyre!(
                "'{value}' is neither unix seconds nor an RFC 3339 UTC timestamp such as \
                 2026-12-02T12:31:00Z: {e}"
            )
        })?
        .duration_since(UNIX_EPOCH)?;
    if since_epoch.subsec_nanos() != 0 {
        eyre::bail!("'{value}' has a fractional second: the genesis timestamp is whole seconds");
    }
    Ok(since_epoch.as_secs())
}

/// Choose the genesis timestamp for a ceremony on `chain_id` run at time `now`.
///
/// A requested timestamp must be after `now` and at most [`MAX_GENESIS_LEAD_SECS`] ahead of it.
/// Without one, a mainnet ceremony ([`MAINNET_CHAIN_ID`]) is refused and any other chain uses
/// `now`.
fn resolve_genesis_timestamp(requested: Option<u64>, chain_id: u64, now: u64) -> eyre::Result<u64> {
    let Some(timestamp) = requested else {
        if chain_id == MAINNET_CHAIN_ID {
            eyre::bail!(
                "a mainnet ceremony (chain id {MAINNET_CHAIN_ID}) requires --genesis-timestamp \
                 <TIME>: unix seconds or an RFC 3339 UTC timestamp"
            );
        }
        return Ok(now);
    };
    if timestamp <= now {
        eyre::bail!("--genesis-timestamp {timestamp} is not after the current time {now}");
    }
    let latest = now.saturating_add(MAX_GENESIS_LEAD_SECS);
    if timestamp > latest {
        eyre::bail!(
            "--genesis-timestamp {timestamp} is more than 366 days after the current time {now}: \
             the latest accepted value is {latest}"
        );
    }
    Ok(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    use clap::Parser as _;
    use tn_config::NodeInfo;
    use tn_reth::system_calls::CONSENSUS_REGISTRY_ADDRESS;
    use tn_types::{
        generate_proof_of_possession_bls_for_test, test_utils::CommandParser, Address, BlsKeypair,
        Committee, Genesis, Multiaddr, NetworkKeypair, NodeP2pInfo, B256, MAINNET_GENESIS, U256,
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
            genesis_timestamp: None,
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

    /// 2026-12-02 12:31:00 UTC as unix seconds.
    const SCHEDULED_GENESIS_SECS: u64 = 1_796_214_660;

    /// A fixed ceremony time for the clock-free validation tests.
    const CEREMONY_NOW: u64 = 1_790_000_000;

    /// A bare integer is unix seconds, up to one below the milliseconds threshold.
    #[test]
    fn parse_genesis_timestamp_reads_unix_seconds() -> eyre::Result<()> {
        assert_eq!(parse_genesis_timestamp("1796214660")?, SCHEDULED_GENESIS_SECS);
        assert_eq!(parse_genesis_timestamp("99999999999")?, MILLISECONDS_THRESHOLD - 1);
        Ok(())
    }

    /// An RFC 3339 UTC timestamp gives the same unix seconds, with `Z`, `+00:00` or a zero
    /// fraction.
    #[test]
    fn parse_genesis_timestamp_reads_rfc3339_utc() -> eyre::Result<()> {
        for value in
            ["2026-12-02T12:31:00Z", "2026-12-02T12:31:00+00:00", "2026-12-02T12:31:00.000Z"]
        {
            assert_eq!(parse_genesis_timestamp(value)?, SCHEDULED_GENESIS_SECS, "{value}");
        }
        Ok(())
    }

    /// A bare integer of 10^11 or more is refused as milliseconds, not read as a far-future
    /// second.
    #[test]
    fn parse_genesis_timestamp_rejects_milliseconds() {
        for value in ["100000000000", "1796214660000"] {
            let err = parse_genesis_timestamp(value).unwrap_err().to_string();
            assert!(err.contains("looks like milliseconds"), "{value}: {err}");
        }
    }

    /// Only UTC is accepted. An offset other than `+00:00` is refused, not converted, so a local
    /// time cannot shift the genesis by hours; `-00:00` (RFC 3339's unknown offset) is refused
    /// too.
    #[test]
    fn parse_genesis_timestamp_rejects_non_utc_offsets() {
        for value in
            ["2026-12-02T13:31:00+01:00", "2026-12-02T07:31:00-05:00", "2026-12-02T12:31:00-00:00"]
        {
            let err = parse_genesis_timestamp(value).unwrap_err().to_string();
            assert!(err.contains("is neither unix seconds nor an RFC 3339 UTC"), "{value}: {err}");
        }
    }

    /// A non-zero fractional second is refused rather than rounded.
    #[test]
    fn parse_genesis_timestamp_rejects_fractional_seconds() {
        let err = parse_genesis_timestamp("2026-12-02T12:31:00.5Z").unwrap_err().to_string();
        assert!(err.contains("has a fractional second"), "{err}");
    }

    /// Values in neither accepted form are refused.
    #[test]
    fn parse_genesis_timestamp_rejects_garbage() {
        for value in [
            "",
            "tomorrow",
            "-1",
            "1.5",
            "0x6b0f3884",
            "18446744073709551616",
            "2026-12-02",
            "2026-12-02 12:31:00",
            "2026-12-02T12:31:00",
            "2026-12-02t12:31:00z",
            "2026-13-02T12:31:00Z",
            "1969-12-31T23:59:59Z",
        ] {
            assert!(parse_genesis_timestamp(value).is_err(), "{value} must be refused");
        }
    }

    /// The flag reaches `GenesisArgs` through clap in both forms, is absent by default, and a
    /// millisecond value fails argument parsing.
    #[test]
    fn genesis_timestamp_flag_parses_through_clap() -> eyre::Result<()> {
        let parse = |extra: &[&str]| {
            CommandParser::<GenesisArgs>::try_parse_from(
                std::iter::once("tn").chain(extra.iter().copied()),
            )
            .map(|command| command.args.genesis_timestamp)
        };
        assert_eq!(parse(&[])?, None);
        assert_eq!(parse(&["--genesis-timestamp", "1796214660"])?, Some(SCHEDULED_GENESIS_SECS));
        assert_eq!(
            parse(&["--genesis-timestamp", "2026-12-02T12:31:00Z"])?,
            Some(SCHEDULED_GENESIS_SECS)
        );
        assert!(parse(&["--genesis-timestamp", "1796214660000"]).is_err());
        Ok(())
    }

    /// A timestamp at or before the ceremony time is refused; one second later is accepted.
    #[test]
    fn resolve_genesis_timestamp_rejects_past_and_present() -> eyre::Result<()> {
        for requested in [0, CEREMONY_NOW - 1, CEREMONY_NOW] {
            let err = resolve_genesis_timestamp(Some(requested), MAINNET_CHAIN_ID, CEREMONY_NOW)
                .unwrap_err()
                .to_string();
            assert!(err.contains("is not after the current time"), "{requested}: {err}");
        }
        assert_eq!(
            resolve_genesis_timestamp(Some(CEREMONY_NOW + 1), MAINNET_CHAIN_ID, CEREMONY_NOW)?,
            CEREMONY_NOW + 1
        );
        Ok(())
    }

    /// Exactly 366 days ahead is accepted; one second more is refused.
    #[test]
    fn resolve_genesis_timestamp_rejects_more_than_366_days_ahead() -> eyre::Result<()> {
        let latest = CEREMONY_NOW + MAX_GENESIS_LEAD_SECS;
        assert_eq!(
            resolve_genesis_timestamp(Some(latest), MAINNET_CHAIN_ID, CEREMONY_NOW)?,
            latest
        );
        for requested in [latest + 1, MILLISECONDS_THRESHOLD - 1, u64::MAX] {
            let err = resolve_genesis_timestamp(Some(requested), MAINNET_CHAIN_ID, CEREMONY_NOW)
                .unwrap_err()
                .to_string();
            assert!(err.contains("is more than 366 days after"), "{requested}: {err}");
        }
        Ok(())
    }

    /// A mainnet ceremony without the flag is refused with an error naming the flag.
    #[test]
    fn resolve_genesis_timestamp_requires_the_flag_on_mainnet() -> eyre::Result<()> {
        let err = resolve_genesis_timestamp(None, MAINNET_CHAIN_ID, CEREMONY_NOW)
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires --genesis-timestamp"), "{err}");
        assert_eq!(
            resolve_genesis_timestamp(
                Some(SCHEDULED_GENESIS_SECS),
                MAINNET_CHAIN_ID,
                CEREMONY_NOW
            )?,
            SCHEDULED_GENESIS_SECS
        );
        Ok(())
    }

    /// Any other chain without the flag uses the ceremony time.
    #[test]
    fn resolve_genesis_timestamp_defaults_to_now_off_mainnet() -> eyre::Result<()> {
        for chain_id in [tn_types::ADIRI_CHAIN_ID, 487, 911329] {
            assert_eq!(resolve_genesis_timestamp(None, chain_id, CEREMONY_NOW)?, CEREMONY_NOW);
        }
        Ok(())
    }

    /// A ceremony run with a future `--genesis-timestamp` writes exactly that value into the
    /// genesis it produces.
    #[test]
    fn test_genesis_writes_the_requested_timestamp() -> eyre::Result<()> {
        let (directory, mut args) = worker_count_ceremony(1, &[])?;
        let timestamp = now() + 3_600;
        args.genesis_timestamp = Some(timestamp);
        let data_dir = directory.path().to_path_buf();
        args.execute(data_dir.clone())?;
        let genesis: Genesis =
            Config::load_from_path(data_dir.genesis_file_path(), ConfigFmt::YAML)?;
        assert_eq!(genesis.timestamp, timestamp);
        Ok(())
    }

    /// A refused timestamp stops the ceremony before it reads its inputs or writes any output.
    #[test]
    fn test_genesis_refused_timestamp_writes_nothing() -> eyre::Result<()> {
        let (directory, mut args) = worker_count_ceremony(1, &[])?;
        let data_dir = directory.path().to_path_buf();
        let cases = [
            (Some(now().saturating_sub(1)), args.chain_id, "is not after the current time"),
            (None, MAINNET_CHAIN_ID, "requires --genesis-timestamp"),
        ];
        for (requested, chain_id, expected) in cases {
            args.genesis_timestamp = requested;
            args.chain_id = chain_id;
            let error = args
                .execute(data_dir.clone())
                .err()
                .ok_or_else(|| eyre::eyre!("genesis must refuse {requested:?} on {chain_id}"))?;
            assert!(error.to_string().contains(expected), "unexpected ceremony error: {error}");
            assert!(
                [
                    data_dir.genesis_file_path(),
                    data_dir.committee_path(),
                    data_dir.node_config_parameters_path(),
                ]
                .iter()
                .all(|path| !path.exists()),
                "refused ceremony must not write genesis, committee or parameters"
            );
        }

        // an empty datadir fails on the timestamp, not on its missing validators, and stays empty
        let empty = tempfile::tempdir()?;
        let error = args
            .execute(empty.path().to_path_buf())
            .err()
            .ok_or_else(|| eyre::eyre!("genesis must refuse mainnet without a timestamp"))?;
        assert!(error.to_string().contains("requires --genesis-timestamp"), "{error}");
        assert!(std::fs::read_dir(empty.path())?.next().is_none(), "refused ceremony wrote a file");
        Ok(())
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

    /// Regenerate the committed `chain-configs/mainnet/{genesis,committee,parameters}.yaml`
    /// placeholders from the CURRENT tn-contracts artifacts (issue #1063).
    ///
    /// WHEN to run: after a tn-contracts submodule/artifact bump, or whenever a mainnet gate
    /// fails (`mainnet_genesis_registry_code_matches_current_artifact` /
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
    /// way the e2e suite drives it (`crates/e2e-tests/src/lib.rs`): mint 4 placeholder
    /// validators via the in-process keytool, then run the ceremony with flags mirroring the
    /// committed placeholder identity:
    /// - chain id 0x2db (731), unchanged from the committed genesis;
    /// - owner + dev-funded account [`MAINNET_PLACEHOLDER_OWNER`];
    /// - basefee address 0x99..99, preserving the committed parameters.yaml;
    /// - epoch duration [`MAINNET_EPOCH_DURATION_SECS`] (21600s, the mainnet target of issue
    ///   #1512). Do not decode this value from the committed file: that copied a stale 86400s into
    ///   each regeneration;
    /// - epoch issuance [`MAINNET_EPOCH_ISSUANCE_TEL`] (25,806 TEL), passed explicitly (stake
    ///   amount / min withdrawal stay on the clap defaults, which also match the committed storage:
    ///   1M TEL / 1k TEL);
    /// - 1s max/min header delays, preserving the committed parameters.yaml;
    /// - genesis timestamp 1796214660 (2026-12-02 12:31:00 UTC), the scheduled mainnet genesis.
    ///
    /// The ceremony refuses a genesis timestamp that is not after the current time, so from the
    /// mainnet genesis time on it refuses this one as past and the test fails. It no longer applies
    /// then: the mainnet files are final by the genesis time and are not regenerated.
    ///
    /// The validator BLS/network keys are freshly minted placeholders by design: the
    /// committed placeholder identities' proof-of-possession signatures cannot be
    /// reproduced (the keytool mints random keys), and `chain-configs/README.md` documents
    /// these files as placeholders until the real mainnet ceremony.
    ///
    /// All three ceremony outputs are read into memory BEFORE any file under
    /// `chain-configs/mainnet` is written, so a missing/unreadable output aborts without
    /// leaving a half-regenerated (2-of-3) config set behind.
    #[test]
    #[ignore = "regenerates chain-configs/mainnet in the working tree from the current tn-contracts artifacts; run explicitly after an artifact bump or when the mainnet config gates fail"]
    fn regenerate_mainnet_chain_configs() {
        use crate::keytool::KeyArgs;

        let tmp = tempfile::tempdir().expect("tempdir for the mainnet regeneration ceremony");
        let shared = tmp.path().join("mainnet-genesis");
        let validators_dir = shared.join("genesis").join("validators");
        std::fs::create_dir_all(&validators_dir).expect("create genesis validators dir");

        // placeholder validators mirror the committed placeholder execution addresses
        let placeholders = [
            ("validator-1", "0x1111111111111111111111111111111111111111"),
            ("validator-2", "0x2222222222222222222222222222222222222222"),
            ("validator-3", "0x3333333333333333333333333333333333333333"),
            ("validator-4", "0x4444444444444444444444444444444444444444"),
        ];
        placeholders.iter().for_each(|(name, address)| {
            let datadir = tmp.path().join(name);
            let keytool = CommandParser::<KeyArgs>::parse_from([
                "tn",
                "generate",
                "validator",
                "--address",
                address,
            ]);
            keytool.args.execute(datadir.clone(), None).expect("keytool generate validator");
            std::fs::copy(
                datadir.join("node-info.yaml"),
                validators_dir.join(format!("{name}.yaml")),
            )
            .expect("copy node-info.yaml into the shared genesis dir");
        });

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
            "--genesis-timestamp",
            "1796214660",
        ]);
        ceremony.args.execute(shared.clone()).expect("mainnet genesis ceremony");

        // splice the ceremony output over the committed placeholders: read ALL three
        // outputs first (any read failure aborts before chain-configs is touched), then
        // write, so a partial ceremony can never leave a half-regenerated config set
        let target = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("chain-configs")
            .join("mainnet");
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
}
