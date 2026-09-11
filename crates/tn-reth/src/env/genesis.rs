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
//! 3. Merge the transitions and harvest each contract's constructor-written storage AND deployed
//!    runtime code from the resulting `BundleState`.
//! 4. Splice the harvested runtime code plus the harvested storage into the real `Genesis` alloc at
//!    the fixed production addresses. The DEPLOYED code is spliced (not the artifact's compile-time
//!    `deployedBytecode.object`) because forge zeroes every immutable site in the compile-time code
//!    and only the CREATE-installed code carries the constructor-patched values (issue #1278: the
//!    artifact splice shipped `ConsensusRegistry` with its five Solady EIP712 immutables zero,
//!    breaking spec-conformant `delegateStake` signatures).
//!
//! Exactly four accounts are written into genesis:
//!
//! - `BLS_G1_PRECOMPILE_ADDRESS`: a single `0xfe` (INVALID) byte (`PRECOMPILE_GENESIS_BYTECODE`),
//!   mirroring the TEL precompile's genesis account. The nonzero code exempts the account from
//!   EIP-158 state clearing, and any call that bypasses precompile dispatch executes INVALID and
//!   reverts instead of silently succeeding against an empty account.
//! - `CONSENSUS_REGISTRY_ADDRESS`: harvested registry runtime code, harvested constructor storage,
//!   and a balance of `stakeAmount * validator count` backing the genesis validators' stakes.
//! - `ISSUANCE_ADDRESS`: issuance runtime code only, from the artifact's compile-time
//!   `deployedBytecode` (it is never constructed in the ceremony, so no storage or balance is
//!   spliced for it), gated on the artifact carrying no immutables.
//! - `WORKER_CONFIGS_ADDRESS`: harvested worker-configs runtime code and harvested constructor
//!   storage.
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
//! - **Three fail-loud gates** protect the splice: the `__$..$__` library-link placeholder check on
//!   the registry initcode (stale-artifact guard — the registry calls the BLS precompile at a fixed
//!   address and links no library), `ensure_pre_genesis_create_success` after each create, and
//!   `ensure_no_immutable_references` before any compile-time artifact splice (currently only
//!   Issuance). Without the second, a constructor Revert/Halt would still ship the contract's
//!   runtime code but with EMPTY storage, which downstream fail-open reads mask as defaults (e.g.
//!   an ownerless, zero-worker `WorkerConfigs`); without the third, a future immutable in a
//!   compile-time-spliced contract would ship as silent zeros (issue #1278).

use std::{path::Path, sync::Arc};

use alloy::{hex, sol_types::SolConstructor as _};
use eyre::OptionExt as _;
use reth::{
    args::{DatabaseArgs, DatadirArgs, RpcServerArgs},
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
    gas_accumulator::GasAccumulator, Address, Genesis, GenesisAccount, TaskManager, B256, U256,
};
use tracing::debug;

use crate::{
    init_reth_defaults,
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
        rewards: Option<GasAccumulator>,
    ) -> eyre::Result<Self> {
        // `RpcServerArgs::default` reads reth's process-wide RPC-server defaults, and the first
        // read locks them. Seed before that read so a temp chain built before any command is
        // parsed does not fix reth's `ipcpath` for the rest of the process (idempotent; the seam
        // below seeds again before `NodeConfig::default`).
        init_reth_defaults();
        // A temp chain parses no CLI, so its IPC path can only be a process-global
        // default: a fixed path shared by every process on the host. Concurrent test runs
        // would race to bind it and leave the socket file behind (issue #1165: every test
        // env bound reth's `/tmp/reth.ipc`). Nothing connects to a temp chain over IPC
        // (tests that exercise IPC spawn nodes through the CLI with a per-tempdir
        // `--ipcpath`), so disable the IPC server outright.
        Self::new_for_temp_chain_with_rpc_args(
            chain,
            db_path,
            task_manager,
            rewards,
            RpcServerArgs { ipcdisable: true, ..Default::default() },
        )
    }

    /// Create a new temp RethEnv with explicit RPC server args.
    ///
    /// Test seam: temp-chain envs otherwise run on `NodeConfig::default()`. Tests that
    /// exercise flag-driven RPC behavior (for example `--rpc.txfeecap`) inject the
    /// parsed args here. Production nodes receive their args through `RethConfig`.
    ///
    /// The args pass through as given, `ipcdisable` included: [`Self::new_for_temp_chain`]
    /// disables the IPC server (issue #1165), while a caller here picks its own transports
    /// (building the RPC modules binds no socket; only [`Self::start_rpc`] does). Callers
    /// build `rpc` before this runs, so a `Default`-constructed value reads reth's RPC-server
    /// defaults first; call [`init_reth_defaults`] before building it when the seeded
    /// `ipcpath` matters.
    pub(crate) fn new_for_temp_chain_with_rpc_args<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<GasAccumulator>,
        rpc: RpcServerArgs,
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

        // `NodeConfig::default` reads reth's process-wide pool and RPC-server defaults through
        // the args types' `Default` impls, which lock them. Seed first so a temp chain built
        // before any command is parsed does not fix reth's own values (per-sender slots 16,
        // ipcpath `reth.ipc`) for the rest of the process, which would then also apply to every
        // node parsed later.
        init_reth_defaults();

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
            // Injected as given: `new_for_temp_chain` passes `ipcdisable: true` (issue #1165).
            rpc,
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
        // Harvest the runtime code the CREATE actually installed on the tmp chain, NOT the
        // artifact's compile-time `deployedBytecode.object`. forge zeroes every immutable site
        // in the compile-time code; only the deployed code carries the constructor-patched
        // values. Splicing the artifact shipped `ConsensusRegistry` with all five Solady
        // EIP712 immutables zero, so the live domain separator was built with
        // `nameHash = versionHash = 0` and spec-conformant EIP-712 signatures were rejected
        // by `delegateStake` (issue #1278).
        let harvest_deployed_code = |contract: &str, address: Address| -> eyre::Result<_> {
            state
                .get(&address)
                .and_then(|account| account.info.as_ref())
                .and_then(|info| {
                    info.code.clone().or_else(|| contracts.get(&info.code_hash).cloned())
                })
                .map(|bytecode| bytecode.original_bytes())
                .filter(|code| !code.is_empty())
                .ok_or_else(|| {
                    eyre::eyre!(
                        "pre-genesis ceremony harvested no {contract} runtime code at tmp \
                         address {address}: constructor deployed elsewhere (nonce-derived \
                         address drift?) or installed no code"
                    )
                })
        };
        let registry_runtimecode =
            harvest_deployed_code("ConsensusRegistry", tmp_registry_address)?;

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
        let worker_configs_runtimecode =
            harvest_deployed_code("WorkerConfigs", tmp_worker_configs_address)?;

        // Issuance is never constructed in the ceremony, so its COMPILE-TIME artifact code is
        // spliced directly; bail loudly if that artifact ever grows an immutable (which forge
        // would ship as silent zeros, issue #1278).
        Self::ensure_no_immutable_references("Issuance", ISSUANCE_JSON)?;
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
                    .with_code(Some(registry_runtimecode))
                    .with_storage(Some(tmp_registry_storage)),
            ),
            (
                ISSUANCE_ADDRESS,
                GenesisAccount::default().with_code(Some(issuance_runtimecode.into())),
            ),
            (
                WORKER_CONFIGS_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(worker_configs_runtimecode))
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

    /// Bail when an artifact whose COMPILE-TIME code the ceremony splices carries immutables.
    ///
    /// forge zeroes every immutable site in `deployedBytecode.object`; the real values are
    /// patched into the DEPLOYED code by the constructor at CREATE time. Contracts the
    /// ceremony deploys on the tmp chain get that deployed code harvested (immutables
    /// intact), but a contract spliced straight from the artifact would ship all-zero
    /// immutables without any error (issue #1278: the `ConsensusRegistry` splice zeroed the
    /// Solady EIP712 cache). A missing `immutableReferences` key counts as empty; a present,
    /// non-empty map fails the ceremony.
    fn ensure_no_immutable_references(contract: &str, artifact_json: &str) -> eyre::Result<()> {
        let json: Value = serde_json::from_str(artifact_json)?;
        let sites = json
            .pointer("/deployedBytecode/immutableReferences")
            .and_then(Value::as_object)
            .map(|refs| {
                refs.values().filter_map(Value::as_array).map(|sites| sites.len()).sum::<usize>()
            })
            .unwrap_or_default();
        eyre::ensure!(
            sites == 0,
            "{contract} artifact lists {sites} immutable reference site(s) in deployedBytecode; \
             splicing its compile-time code into genesis would ship every immutable as zero; \
             deploy it in the pre-genesis ceremony and harvest the deployed code instead"
        );
        Ok(())
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

    /// Regression (#1278): genesis must splice the tmp-chain DEPLOYED registry code, so the
    /// Solady EIP712 immutables are constructor-patched instead of forge-zeroed.
    ///
    /// The artifact's `deployedBytecode.immutableReferences` gives the immutable sites. The
    /// spliced code must (a) match the compile-time code byte-for-byte OUTSIDE those sites
    /// (same contract, same layout), (b) carry a non-zero value at every site, and (c) carry
    /// `keccak256("Telcoin StakeManager")` and `keccak256("1")` among the patched segments
    /// (the `_cachedNameHash`/`_cachedVersionHash` every EIP-712 digest rebuild reads).
    #[tokio::test]
    async fn genesis_registry_code_carries_patched_immutables() -> eyre::Result<()> {
        let genesis = crate::test_utils::test_genesis_with_consensus_registry(4);
        let code = genesis
            .alloc
            .get(&CONSENSUS_REGISTRY_ADDRESS)
            .and_then(|account| account.code.clone())
            .ok_or_eyre("genesis must allocate code for the ConsensusRegistry account")?;

        let artifact: Value = serde_json::from_str(CONSENSUS_REGISTRY_JSON)?;
        let artifact_code = hex::decode(
            artifact
                .pointer("/deployedBytecode/object")
                .and_then(Value::as_str)
                .ok_or_eyre("artifact must contain deployedBytecode.object")?,
        )?;
        let sites: Vec<(usize, usize)> = artifact
            .pointer("/deployedBytecode/immutableReferences")
            .and_then(Value::as_object)
            .map(|refs| {
                refs.values()
                    .filter_map(Value::as_array)
                    .flatten()
                    .filter_map(|site| {
                        site.get("start")
                            .and_then(Value::as_u64)
                            .zip(site.get("length").and_then(Value::as_u64))
                            .map(|(start, length)| (start as usize, length as usize))
                    })
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(sites.len(), 5, "registry artifact must list the 5 Solady EIP712 immutables");
        assert_eq!(code.len(), artifact_code.len(), "spliced code must be the same contract");

        // (a) identical outside immutable sites
        let inside_site =
            |i: usize| sites.iter().any(|(start, length)| i >= *start && i < start + length);
        let diverges_outside = code
            .iter()
            .zip(artifact_code.iter())
            .enumerate()
            .any(|(i, (spliced, compiled))| !inside_site(i) && spliced != compiled);
        assert!(!diverges_outside, "spliced code must differ ONLY at immutable sites");

        // (b) every immutable segment constructor-patched (the artifact ships them all-zero)
        let segments: Vec<&[u8]> =
            sites.iter().filter_map(|(start, length)| code.get(*start..start + length)).collect();
        assert_eq!(segments.len(), sites.len(), "every immutable site must be in range");
        assert!(
            segments.iter().all(|segment| segment.iter().any(|byte| *byte != 0)),
            "every EIP712 immutable must be non-zero in the spliced code"
        );

        // (c) the domain name/version hashes are among the patched values
        let name_hash = alloy::primitives::keccak256("Telcoin StakeManager");
        let version_hash = alloy::primitives::keccak256("1");
        assert!(
            segments.iter().any(|segment| *segment == name_hash.as_slice()),
            "_cachedNameHash must be keccak256(\"Telcoin StakeManager\")"
        );
        assert!(
            segments.iter().any(|segment| *segment == version_hash.as_slice()),
            "_cachedVersionHash must be keccak256(\"1\")"
        );
        Ok(())
    }

    /// The immutable-reference gate must reject an artifact WITH immutables (the registry's
    /// compile-time code, the exact splice issue #1278 shipped) and pass the artifacts the
    /// ceremony still splices compile-time (Issuance carries none).
    #[test]
    fn immutable_reference_gate_discriminates_artifacts() {
        RethEnv::ensure_no_immutable_references("Issuance", ISSUANCE_JSON)
            .expect("Issuance artifact carries no immutables; the gate must pass it");
        let err =
            RethEnv::ensure_no_immutable_references("ConsensusRegistry", CONSENSUS_REGISTRY_JSON)
                .expect_err("registry artifact carries 5 immutable sites; the gate must reject");
        assert!(
            format!("{err:#}").contains("immutable reference"),
            "error must name the immutable references, got: {err:#}"
        );
    }

    /// The archive-mode guard is wired into [`RethEnv::new`], so a pruning configuration stops
    /// node startup rather than only failing an isolated check nothing calls.
    ///
    /// `RethEnv::new` is the chokepoint every env in the process passes through, which is why the
    /// check lives there and why this test asserts the WIRING rather than the predicate (the
    /// predicate itself is covered by the `ensure_archive_mode` tests in `cli.rs`).
    #[tokio::test]
    async fn test_reth_env_new_rejects_pruned_config() -> eyre::Result<()> {
        init_reth_defaults();
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Archive Mode Test Task Manager");
        let mut config = RethConfig(NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(tmp_dir.path().to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            ..NodeConfig::default()
        });
        let database = RethEnv::new_database(&config, tmp_dir.path())?;

        // enable pruning only after the database exists, so the guard is the sole reason for the
        // failure below
        config.0.pruning.account_history_distance = Some(128);

        let err = RethEnv::new(&config, &task_manager, database, None, GasAccumulator::default())
            .expect_err("RethEnv::new must refuse a pruned config");
        assert!(err.to_string().contains("archive mode"), "unexpected error: {err}");
        Ok(())
    }

    /// A temp chain must not open an IPC socket. Its config never passes CLI parsing, so its
    /// `ipcpath` can only be a process-global default: a fixed path every process on the host
    /// shares, which concurrent test runs would race for (issue #1165: every test env bound
    /// reth's `/tmp/reth.ipc`). `ipcdisable` is asserted rather than the path because which path
    /// the global holds depends on which test in this binary read reth's RPC defaults first; the
    /// seeded path itself is pinned in the `reth_defaults` integration test that owns its
    /// process.
    #[tokio::test]
    async fn test_temp_chain_disables_ipc() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Temp Chain IPC Test Task Manager");
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
        assert!(reth_env.node_config().rpc.ipcdisable, "temp chain must disable the IPC server");
        Ok(())
    }

    /// The injected-args seam passes the args through as given: the fee cap reaches the node
    /// config, and `ipcdisable` is the caller's (the IPC-off default belongs to
    /// `new_for_temp_chain` alone, so the fee-cap RPC tests keep their IPC-only module set).
    #[tokio::test]
    async fn test_temp_chain_with_rpc_args_passes_args_through() -> eyre::Result<()> {
        init_reth_defaults();
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Temp Chain RPC Args Test Task Manager");
        let rpc = RpcServerArgs { rpc_tx_fee_cap: 1_000, ipcdisable: false, ..Default::default() };
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain,
            tmp_dir.path(),
            &task_manager,
            None,
            rpc,
        )?;
        let rpc = &reth_env.node_config().rpc;
        assert_eq!(rpc.rpc_tx_fee_cap, 1_000, "injected fee cap must reach the node config");
        assert!(!rpc.ipcdisable, "the seam must not override the caller's ipcdisable");
        Ok(())
    }
}
