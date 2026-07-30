//! Methods used for chain genesis.
//!
//! # The pre-genesis ceremony
//!
//! `create_consensus_registry_genesis_accounts` builds the system-contract accounts for the
//! REAL genesis by first executing their constructors on a throwaway chain:
//!
//! 1. Spin up a temporary `RethEnv` over a `TempDir` via `new_for_temp_chain` (whose 2 GiB MDBX
//!    map-size ceiling exists so many temp envs can coexist in one test process without exhausting
//!    the address space — see the consts inside that fn).
//! 2. CREATE-deploy `ConsensusRegistry`, then `WorkerConfigs`, from the owner address via
//!    `transact_pre_genesis_create` (tx nonce 0 with nonce checks disabled, zero gas price).
//! 3. Merge the transitions and harvest each contract's constructor-written storage from the
//!    resulting `BundleState`.
//! 4. Splice each artifact's `deployedBytecode` plus the harvested storage into the real `Genesis`
//!    alloc at the fixed production addresses.
//!
//! Exactly four accounts are written into genesis:
//!
//! - `BLS_G1_PRECOMPILE_ADDRESS`: a single `0xfe` (INVALID) byte (`PRECOMPILE_GENESIS_BYTECODE`),
//!   mirroring the TEL precompile's genesis account. The nonzero code exempts the account from
//!   EIP-158 state clearing, and any call that bypasses precompile dispatch executes INVALID and
//!   reverts instead of silently succeeding against an empty account.
//! - `CONSENSUS_REGISTRY_ADDRESS`: registry runtime code, harvested constructor storage, and a
//!   balance of `stakeAmount * validator count` backing the genesis validators' stakes.
//! - `ISSUANCE_ADDRESS`: issuance runtime code only. It is never constructed in the ceremony, so no
//!   storage (or balance) is spliced for it.
//! - `WORKER_CONFIGS_ADDRESS`: worker-configs runtime code and harvested constructor storage.
//!
//! ## Sharp edges
//!
//! - **Address derivation is NONCE-POSITIONAL.** Harvested storage is looked up in the bundle at
//!   `owner_address.create(0)` (registry) and `owner_address.create(1)` (worker configs) — the
//!   owner's first and second creates on the temp chain. Inserting any create before or between
//!   them silently shifts both computed addresses off the actual deployments, and the storage
//!   harvest comes up empty (or wrong) without any error.
//! - **EIP-170 is relaxed only for the registry's temp-chain deploy**: its deployed code (~28.8 KB)
//!   exceeds the 24,576-byte limit, so `limit_contract_code_size` is raised for that one create.
//!   The `WorkerConfigs` deploy runs with the default limit, and the real genesis splices runtime
//!   code straight into the alloc, where no CREATE-time size check applies.
//! - **Two fail-loud gates** protect the splice: the `__$..$__` library-link placeholder check on
//!   the registry initcode (stale-artifact guard — the registry calls the BLS precompile at a fixed
//!   address and links no library), and `ensure_pre_genesis_create_success` after each create.
//!   Without the latter, a constructor Revert/Halt would still ship the contract's runtime code but
//!   with EMPTY storage, which downstream fail-open reads mask as defaults (e.g. an ownerless,
//!   zero-worker `WorkerConfigs`).

use std::{path::Path, sync::Arc};

use alloy::{hex, sol_types::SolConstructor as _};
use eyre::OptionExt as _;
use reth::{
    args::{DatabaseArgs, DatadirArgs},
    dirs::MaybePlatformPath,
};
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_evm::{ConfigureEvm as _, Evm as _, EvmFactory as _};
use reth_node_builder::NodeConfig;
use reth_revm::{
    cached::CachedReads,
    context::result::{ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, BundleState},
    DatabaseCommit as _, State,
};
use serde_json::Value;
use tempfile::TempDir;
use tn_config::{
    NodeInfo, CONSENSUS_REGISTRY_JSON, ISSUANCE_ADDRESS, ISSUANCE_JSON, WORKER_CONFIGS_ADDRESS,
    WORKER_CONFIGS_JSON,
};
use tn_types::{
    gas_accumulator::RewardsCounter, Address, Genesis, GenesisAccount, TaskManager, B256, U256,
};
use tracing::debug;

use crate::{
    init_txpool_defaults,
    system_calls::{
        ConsensusRegistry, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS, PRECOMPILE_GENESIS_BYTECODE,
    },
    RethConfig, RethEnv, BLS_G1_PRECOMPILE_ADDRESS,
};

impl RethEnv {
    /// Create a new temp RethEnv using a specified chain spec.
    pub fn new_for_temp_chain<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<RewardsCounter>,
    ) -> eyre::Result<Self> {
        /// MDBX map-size ceiling for throwaway temp-chain envs. reth defaults to 8 TB per
        /// environment; `cargo test` runs a test binary as threads in ONE process, so N
        /// concurrent 8 TB virtual reservations exhaust the address space and MDBX aborts
        /// env-open with ENOMEM ("Cannot allocate memory (12)"). A temp chain never holds a
        /// full node's history, so a small ceiling is safe and lets many envs coexist.
        const TEMP_CHAIN_DB_MAX_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB
        /// Grow the temp DB file in small increments so throwaway DBs stay tiny on disk
        /// (reth pairs its 8 TB default with a 4 GiB step; mirror reth's `test()` 4 MiB step).
        const TEMP_CHAIN_DB_GROWTH_STEP: usize = 4 * 1024 * 1024; // 4 MiB

        // `NodeConfig::default` reads reth's process-wide pool defaults through
        // `TxPoolArgs::default`, which locks them. Seed first so a temp chain built before any
        // command is parsed does not fix the per-sender slot default at reth's 16 for the rest of
        // the process, which would then also apply to every node parsed later.
        init_txpool_defaults();

        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(db_path.as_ref().to_path_buf()),
                // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            // Bound the MDBX geometry for throwaway temp chains (see const docs).
            db: DatabaseArgs {
                max_size: Some(TEMP_CHAIN_DB_MAX_SIZE),
                growth_step: Some(TEMP_CHAIN_DB_GROWTH_STEP),
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config);
        let database = Self::new_database(&reth_config, db_path)?;
        Self::new(&reth_config, task_manager, database, None, rewards.unwrap_or_default())
    }

    /// Convenience method for compiling storage and bytecode to include consensus registry
    /// configuration in genesis.
    pub fn create_consensus_registry_genesis_accounts(
        validators: Vec<NodeInfo>,
        genesis: Genesis,
        initial_stake_config: ConsensusRegistry::StakeConfig,
        owner_address: Address,
        worker_configs: Vec<(u8, u64)>,
    ) -> eyre::Result<Genesis> {
        // create temporary reth env for execution
        let tmp_chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());
        let task_manager = TaskManager::new("Temp Task Manager");
        let tmp_dir = TempDir::new()?;
        let reth_env =
            RethEnv::new_for_temp_chain(tmp_chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        // prepare registry deployment
        let (validators, (bls_pubkeys, proofs)): (Vec<_>, (Vec<_>, Vec<_>)) = validators
            .iter()
            .map(|v| {
                let validator = ConsensusRegistry::ValidatorInfo {
                    validatorAddress: v.execution_address,
                    activationEpoch: 0,
                    exitEpoch: 0,
                    currentStatus: ConsensusRegistry::ValidatorStatus::Active,
                    isRetired: false,
                    stakeVersion: 0,
                    region: 0,
                };
                let bls_pubkey: tn_types::Bytes = v.bls_public_key.to_bytes().into();
                let proof = ConsensusRegistry::ProofOfPossession {
                    signature: v.proof_of_possession.to_bytes().into(),
                };

                (validator, (bls_pubkey, proof))
            })
            .unzip();

        let total_stake_balance = initial_stake_config
            .stakeAmount
            .checked_mul(U256::from(validators.len()))
            .ok_or_eyre("Failed to calculate total stake for consensus registry at genesis")?;
        debug!(target: "engine", ?initial_stake_config, "calling constructor for consensus registry");

        let constructor_args = ConsensusRegistry::constructorCall {
            genesisConfig_: initial_stake_config,
            initialValidators_: validators,
            blsPubkeys_: bls_pubkeys,
            proofsOfPossession: proofs,
            owner_: owner_address,
        }
        .abi_encode();

        // generate calldata for creation
        let registry_initcode_binding =
            Self::fetch_value_from_json_str(CONSENSUS_REGISTRY_JSON, Some("bytecode.object"))?;
        let registry_initcode_str =
            registry_initcode_binding.as_str().ok_or_eyre("invalid registry json")?;
        // The registry calls the BLS precompile directly at `BLS_G1_ADDRESS` (no linked library),
        // so its bytecode carries no link placeholder; deploy it as-is. Guard against a
        // stale artifact that still contains an unresolved `__$..$__` placeholder.
        if registry_initcode_str.contains("__$") {
            eyre::bail!(
                "ConsensusRegistry initcode has an unresolved library link placeholder; regenerate tn-contracts artifacts"
            );
        }
        let mut create_registry = hex::decode(registry_initcode_str)?;
        create_registry.extend(constructor_args);

        // after adding bls proof of possession, registry precompile exceeds size limit so disable
        // it for tmp chain
        let mut tmp_evm_no_eip170 =
            reth_env.inner.evm_config.evm_env(&tmp_chain.sealed_genesis_header())?;
        tmp_evm_no_eip170.cfg_env.limit_contract_code_size = Some(0x12000000);

        // deploy registry now that it can use the previously deployed blsg1 lib
        let tmp_registry_address = {
            let mut tn_evm =
                reth_env.inner.evm_config.evm_factory().create_evm(&mut db, tmp_evm_no_eip170);
            let ResultAndState { result, state } =
                tn_evm.transact_pre_genesis_create(owner_address, create_registry.into())?;
            debug!(target: "engine", "create consensus registry result:\n{:#?}", result);
            Self::ensure_pre_genesis_create_success("ConsensusRegistry", &result)?;

            tn_evm.db_mut().commit(state);

            // With BlsG1 now a precompile (no genesis deploy), the registry is the owner's first
            // create tx on the tmp chain.
            owner_address.create(0)
        };

        // deploy WorkerConfigs contract
        let tmp_worker_configs_address = {
            let mut tn_evm = reth_env.inner.evm_config.evm_factory().create_evm(
                &mut db,
                reth_env.inner.evm_config.evm_env(&tmp_chain.sealed_genesis_header())?,
            );

            let (strategies, values): (Vec<u8>, Vec<u64>) = worker_configs.iter().copied().unzip();
            let datas = vec![alloy::primitives::aliases::U184::ZERO; strategies.len()];
            let constructor_args =
                WorkerConfigs::constructorCall { strategies, values, datas, owner_: owner_address }
                    .abi_encode();

            let initcode_binding =
                Self::fetch_value_from_json_str(WORKER_CONFIGS_JSON, Some("bytecode.object"))?;
            let initcode =
                hex::decode(initcode_binding.as_str().ok_or_eyre("invalid worker configs json")?)?;
            let mut create_worker_configs = initcode;
            create_worker_configs.extend(constructor_args);

            let ResultAndState { result, state } =
                tn_evm.transact_pre_genesis_create(owner_address, create_worker_configs.into())?;
            debug!(target: "engine", "create worker configs result:\n{:#?}", result);
            Self::ensure_pre_genesis_create_success("WorkerConfigs", &result)?;

            tn_evm.db_mut().commit(state);

            // Second create tx on tmp chain (registry at nonce 0, worker configs at nonce 1).
            owner_address.create(1)
        };

        // execute the transactions to get final bundle state
        db.merge_transitions(BundleRetention::PlainState);
        let BundleState { state, contracts, reverts, state_size, reverts_size } = db.take_bundle();

        debug!(target: "engine", "contracts:\n{:#?}", contracts);
        debug!(target: "engine", "reverts:\n{:#?}", reverts);
        debug!(target: "engine", "state_size:{:#?}", state_size);
        debug!(target: "engine", "reverts_size:{:#?}", reverts_size);

        // construct real genesis using known values & tmp chain storage result. A missing
        // account or empty storage map here means the ceremony's nonce-derived tmp address no
        // longer matches where the constructor actually deployed (or the constructor wrote
        // nothing); flowing that into `with_storage(None)` would ship a deployed-but-
        // uninitialized contract — unownable, first epoch close reverts — so fail loud instead.
        let tmp_registry_storage: std::collections::BTreeMap<B256, B256> = state
            .get(&tmp_registry_address)
            .map(|account| {
                account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
            })
            .filter(|s: &std::collections::BTreeMap<B256, B256>| !s.is_empty())
            .ok_or_else(|| {
                eyre::eyre!(
                    "pre-genesis ceremony harvested no ConsensusRegistry storage at tmp address \
                     {tmp_registry_address}: constructor deployed elsewhere (nonce-derived \
                     address drift?) or wrote no slots"
                )
            })?;
        let registry_runtimecode_binding = Self::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let registry_runtimecode_str =
            registry_runtimecode_binding.as_str().ok_or_eyre("invalid registry json")?;
        let registry_runtimecode = hex::decode(registry_runtimecode_str)?;

        let tmp_worker_configs_storage: std::collections::BTreeMap<B256, B256> = state
            .get(&tmp_worker_configs_address)
            .map(|account| {
                account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
            })
            .filter(|s: &std::collections::BTreeMap<B256, B256>| !s.is_empty())
            .ok_or_else(|| {
                eyre::eyre!(
                    "pre-genesis ceremony harvested no WorkerConfigs storage at tmp address \
                     {tmp_worker_configs_address}: constructor deployed elsewhere (nonce-derived \
                     address drift?) or wrote no slots"
                )
            })?;
        let worker_configs_runtimecode_binding =
            Self::fetch_value_from_json_str(WORKER_CONFIGS_JSON, Some("deployedBytecode.object"))?;
        let worker_configs_runtimecode = hex::decode(
            worker_configs_runtimecode_binding
                .as_str()
                .ok_or_eyre("invalid worker configs json")?,
        )?;

        let issuance_json_binding =
            Self::fetch_value_from_json_str(ISSUANCE_JSON, Some("deployedBytecode.object"))?;
        let issuance_runtimecode =
            hex::decode(issuance_json_binding.as_str().ok_or_eyre("invalid issuance json")?)?;
        let genesis = genesis.extend_accounts([
            // The BLS proof-of-possession precompile lives at `BLS_G1_PRECOMPILE_ADDRESS`
            // (`BLS_G1_PRECOMPILE_ADDRESS`). Mirror the TEL precompile and give it a single `0xfe`
            // (INVALID) byte of code so the account is non-empty (never state-pruned) and any call
            // that bypasses precompile dispatch reverts instead of succeeding against an EOA.
            (
                BLS_G1_PRECOMPILE_ADDRESS,
                GenesisAccount::default().with_code(Some(PRECOMPILE_GENESIS_BYTECODE.into())),
            ),
            (
                CONSENSUS_REGISTRY_ADDRESS,
                GenesisAccount::default()
                    .with_balance(U256::from(total_stake_balance))
                    .with_code(Some(registry_runtimecode.into()))
                    .with_storage(Some(tmp_registry_storage)),
            ),
            (
                ISSUANCE_ADDRESS,
                GenesisAccount::default().with_code(Some(issuance_runtimecode.into())),
            ),
            (
                WORKER_CONFIGS_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(worker_configs_runtimecode.into()))
                    .with_storage(Some(tmp_worker_configs_storage)),
            ),
        ]);

        Ok(genesis)
    }

    /// Bail unless a pre-genesis constructor transaction succeeded.
    ///
    /// Pre-genesis creates are committed straight into genesis storage. An unchecked
    /// Revert/Halt would still ship the contract's runtime code but with EMPTY storage
    /// (e.g. an ownerless, zero-worker `WorkerConfigs`), which downstream fail-open reads
    /// mask as defaults — so the ceremony must fail loudly here instead.
    fn ensure_pre_genesis_create_success(
        contract: &str,
        result: &ExecutionResult,
    ) -> eyre::Result<()> {
        match result {
            ExecutionResult::Success { .. } => Ok(()),
            ExecutionResult::Revert { output, .. } => {
                let reason = alloy::sol_types::decode_revert_reason(output)
                    .unwrap_or_else(|| "<undecodable revert reason>".to_string());
                eyre::bail!(
                    "{contract} constructor reverted during pre-genesis create: {reason} (revert output: {output})"
                )
            }
            ExecutionResult::Halt { reason, gas_used } => eyre::bail!(
                "{contract} constructor halted during pre-genesis create: {reason:?} (gas used: {gas_used})"
            ),
        }
    }

    /// Fetches json info from the given string
    ///
    /// If a key is specified, return the corresponding nested object.
    /// Otherwise return the entire JSON
    /// With a generic this could be adjusted to handle YAML also
    pub fn fetch_value_from_json_str(json_content: &str, key: Option<&str>) -> eyre::Result<Value> {
        let json: Value = serde_json::from_str(json_content)?;
        let result = match key {
            Some(path) => {
                let key: Vec<&str> = path.split('.').collect();
                let mut current_value = &json;
                for &k in &key {
                    current_value =
                        current_value.get(k).ok_or_else(|| eyre::eyre!("key '{}' not found", k))?;
                }
                current_value.clone()
            }
            None => json,
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::{generate_proof_of_possession_bls_for_test, BlsKeypair, NodeP2pInfo};

    /// Regression: a `WorkerConfigs` constructor revert must FAIL genesis creation.
    ///
    /// Strategy 2 exceeds the contract's `MAX_STRATEGY` (= 1), so the constructor reverts
    /// `InvalidStrategy`. Before the fix the reverted state was committed anyway, shipping
    /// runtime code with empty storage: `numWorkers() = 0` and `owner() = address(0)` —
    /// permanently unownable and masked downstream by fail-open defaults.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_worker_config_strategy() {
        let err = crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(
            4,
            vec![(2u8, 30_000_000u64)],
        )
        .expect_err("strategy 2 exceeds the contract's MAX_STRATEGY and must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: an EMPTY worker config set must FAIL genesis creation (the
    /// `WorkerConfigs` constructor reverts `NumWorkersBelowMinimum`). See
    /// [`genesis_ceremony_rejects_invalid_worker_config_strategy`] for the pre-fix failure mode.
    #[tokio::test]
    async fn genesis_ceremony_rejects_empty_worker_configs() {
        let err =
            crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(4, vec![])
                .expect_err("empty worker configs must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: the `ConsensusRegistry` pre-genesis create is guarded by
    /// the same success check. A proof of possession generated for the WRONG address fails the
    /// constructor's BLS precompile verification (`InvalidProofOfPossession`), which must fail
    /// genesis creation instead of committing a half-initialized registry.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_consensus_registry_pop() {
        let validator_address = Address::from_slice(&[0x11; 20]);
        let wrong_address = Address::from_slice(&[0x22; 20]);
        let mut rng = StdRng::seed_from_u64(0);
        let bls = BlsKeypair::generate(&mut rng);
        // sign the proof of possession over the wrong address
        let pop = generate_proof_of_possession_bls_for_test(&bls, &wrong_address)
            .expect("pop generation failed");
        let validator = NodeInfo {
            name: "validator-0".to_string(),
            bls_public_key: *bls.public(),
            p2p_info: NodeP2pInfo::default(),
            execution_address: validator_address,
            proof_of_possession: pop,
        };

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").expect("parse stake amount")),
            minWithdrawAmount: U256::from(parse_ether("1_000").expect("parse min withdraw")),
            epochIssuance: U256::from(parse_ether("25_806").expect("parse epoch issuance")),
            epochDuration: 60 * 60 * 8,
        };

        let err = RethEnv::create_consensus_registry_genesis_accounts(
            vec![validator],
            tn_types::test_genesis(),
            initial_stake_config,
            Address::from_slice(&[0x99; 20]),
            vec![(0u8, 30_000_000u64)],
        )
        .expect_err("invalid proof of possession must fail genesis");
        assert!(
            format!("{err:#}").contains("ConsensusRegistry constructor reverted"),
            "error must name the ConsensusRegistry revert, got: {err:#}"
        );
    }
}
