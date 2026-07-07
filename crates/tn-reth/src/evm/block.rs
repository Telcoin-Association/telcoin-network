//! The types that build blocks for EVM execution.

use crate::{
    error::{TnRethError, TnRethResult},
    system_calls::{
        ConsensusRegistry::{self, RewardInfo, ValidatorStatus},
        CONSENSUS_REGISTRY_ADDRESS,
    },
    SYSTEM_ADDRESS,
};
use alloy::{
    consensus::{proofs, Block, BlockBody, Transaction, TransactionEnvelope, TxReceipt},
    eips::{
        eip2935::HISTORY_STORAGE_ADDRESS,
        eip4788::BEACON_ROOTS_ADDRESS,
        eip7685::{Requests, EMPTY_REQUESTS_HASH},
    },
    sol_types::SolCall as _,
};
use alloy_evm::{Database, Evm};
use rand::{rngs::StdRng, seq::IteratorRandom, Rng as _, SeedableRng as _};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutionResult, BlockExecutor, BlockExecutorFactory, ExecutableTx,
        InternalBlockExecutionError, TxResult,
    },
    eth::receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    execute::{BlockAssembler, BlockAssemblerInput},
    FromRecoveredTx, FromTxWithEncoded, OnStateHook, RecoveredTx,
};
use reth_primitives::logs_bloom;
use reth_primitives_traits::proofs::calculate_withdrawals_root;
use reth_revm::{
    context::{
        result::{ExecutionResult, ResultAndState},
        Block as _,
    },
    db::states::bundle_state::BundleRetention,
    DatabaseCommit as _, State,
};
use std::{collections::BTreeMap, sync::Arc};
use tn_types::{
    gas_accumulator::RewardsCounter, Address, Bytes, Encodable2718, ExecHeader, Receipt,
    TransactionSigned, Withdrawals, B256, EMPTY_WITHDRAWALS, U256,
};
use tracing::{debug, error, trace};

/// Context for TN block execution.
#[derive(Debug, Clone)]
pub struct TNBlockExecutionCtx {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root - the digest of the `ConsensusHeader`.
    pub parent_beacon_block_root: Option<B256>,
    /// The index for the batch.
    pub nonce: u64,
    /// The batch digest.
    ///
    /// This is the batch that was validated by consensus and executed
    /// to produce the EVM block.
    pub ommers_hash: B256,
    /// Keccak hash of the bls signature for the leader certificate.
    ///
    /// Executor makes closing epoch system call when this if included.
    /// The hash is stored in the `extra_data` field so clients know when the
    /// closing epoch call was made.
    pub close_epoch: Option<B256>,
    /// Difficulty- this contains the worker id and batch index:
    /// `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
    pub difficulty: U256,
    /// Counter used to allocate rewards for block leaders.
    pub rewards_counter: RewardsCounter,
}

impl TNBlockExecutionCtx {
    /// Checks if the batch_index stored in the difficulty field is zero
    /// which indicates the first batch in the executed output from consensus.
    ///
    /// The difficulty field packs two values using bit operations:
    /// `difficulty = U256::from(batch_index << 16 | worker_id as usize)`
    ///
    /// This creates a bit layout where:
    /// - Bits 0-15 (lower 16 bits): worker_id (max value 65535)
    /// - Bits 16+     (upper bits): batch_index
    ///
    /// Since worker_id can only occupy the lower 16 bits (max value 2^16 - 1 = 65535),
    /// if the entire difficulty value is less than 2^16 (65536), then no bits are set
    /// in positions 16 or higher. This mathematically guarantees that batch_index = 0.
    ///
    /// This approach avoids bit shifting operations and provides an efficient
    /// zero-check without extracting the actual batch_index value.
    ///
    /// # Example
    /// ```
    /// // If difficulty = 0x00001234, then:
    /// // - worker_id = 0x1234 (bits 0-15)
    /// // - batch_index = 0x0000 (bits 16+)
    /// // Since 0x1234 < 0x10000 (65536), batch_index is 0
    /// ```
    ///
    /// This is used during execution to write the consensus header hash
    /// to `BEACON_ROOTS` contract (eip4788).
    fn first_batch(&self) -> bool {
        self.difficulty < U256::from(65536)
    }
}

/// Block executor for Ethereum.
#[derive(Debug)]
pub(crate) struct TNBlockExecutor<Evm, Spec, R: ReceiptBuilder> {
    /// Reference to the specification object.
    spec: Spec,
    /// Context for block execution.
    pub ctx: TNBlockExecutionCtx,
    /// Inner EVM.
    evm: Evm,
    /// Receipt builder.
    receipt_builder: R,

    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by transactions in this block.
    gas_used: u64,
}

// alloy-evm
impl<'db, Evm, Spec, R, DB> TNBlockExecutor<Evm, Spec, R>
where
    DB: Database + 'db,
    DB::Error: core::fmt::Display,
    Evm: alloy_evm::Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthereumHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    /// Creates a new [`TNBlockExecutor`]
    pub(crate) fn new(evm: Evm, ctx: TNBlockExecutionCtx, spec: Spec, receipt_builder: R) -> Self {
        Self { evm, ctx, receipts: Vec::new(), gas_used: 0, spec, receipt_builder }
    }

    /// Increase the beneficiary account balance and withdraw from governance safe.
    ///
    /// This must be called once per epoch, before the conclude epoch call.
    fn apply_consensus_block_rewards(
        &mut self,
        rewards: BTreeMap<Address, u32>,
    ) -> TnRethResult<()> {
        let calldata = self.generate_apply_incentives_calldata(
            rewards.iter().map(|(address, count)| (*address, *count)).collect(),
        )?;

        trace!(target: "engine", ?calldata, "apply incentives calldata");

        // execute system call to consensus registry
        let mut res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error applying consensus block rewards contract call: {:?}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "applying consensus block rewards failed: {e}"
                )));
            }
        };

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed applying consensus block rewards call: {:?}", res.result);
            return Err(TnRethError::EVMCustom(
                "failed applying consensus block rewards".to_string(),
            ));
        }
        trace!(target: "engine", ?res, "applying consensus block rewards");

        // clean up SYSTEM_ADDRESS — only touched as the system caller, not a real state change
        res.state.remove(&SYSTEM_ADDRESS);
        // commit the changes
        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Apply the closing epoch call to ConsensusRegistry.
    fn apply_closing_epoch_contract_call(&mut self, randomness: B256) -> TnRethResult<()> {
        debug!(target: "engine", ?randomness, "applying closing contract call");
        let calldata = self.generate_conclude_epoch_calldata(randomness)?;
        trace!(target: "engine", ?calldata, "close epoch calldata");

        // execute system call to consensus registry
        let mut res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error executing closing epoch contract call: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("epoch closing execution failed: {e}")));
            }
        };

        trace!(target: "engine", ?res, "transact system call for conclude epoch");

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed to apply closing epoch call: {:?}", res.result);
            return Err(TnRethError::EVMCustom("failed to close epoch".to_string()));
        }

        trace!(target: "engine", "closing epoch logs:\n{:?}", res.result.logs());

        // clean up SYSTEM_ADDRESS — only touched as the system caller, not a real state change
        res.state.remove(&SYSTEM_ADDRESS);
        // commit the changes
        self.evm.db_mut().commit(res.state);
        Ok(())
    }

    /// The upgraded `ConsensusRegistry` runtime bytecode and its code hash, sourced from the same
    /// embedded artifact genesis deploys (`CONSENSUS_REGISTRY_JSON` `deployedBytecode.object`).
    ///
    /// Materialized once. The embedded artifact is a compile-time `include_str!` constant (the same
    /// bytes genesis generation decodes, and exercised by the fork unit test), so a decode failure
    /// here is a corrupt build — uniform across the fleet — not a live-node runtime condition;
    /// hence `expect` rather than a fallible return.
    #[cfg(feature = "adiri")]
    fn consensus_registry_runtime_code() -> &'static (reth_revm::bytecode::Bytecode, B256) {
        use reth_revm::bytecode::Bytecode;
        use std::sync::LazyLock;

        static CODE: LazyLock<(Bytecode, B256)> = LazyLock::new(|| {
            let value = crate::RethEnv::fetch_value_from_json_str(
                crate::CONSENSUS_REGISTRY_JSON,
                Some("deployedBytecode.object"),
            )
            .expect("embedded consensus registry artifact json is valid");
            let hex_str = value.as_str().expect("registry deployedBytecode.object is a string");
            let raw =
                alloy::hex::decode(hex_str).expect("registry deployedBytecode.object is valid hex");
            let bytecode = Bytecode::new_raw(raw.into());
            let code_hash = bytecode.hash_slow();
            (bytecode, code_hash)
        });

        &CODE
    }

    /// Apply the in-protocol `ConsensusRegistry` fork.
    ///
    /// Swaps the deployed registry runtime bytecode to the upgraded version — preserving the
    /// account's balance, nonce, and **all** existing storage (the new state is a clean append, so
    /// the swap rewrites only the account-code leaf) — then runs the one-time
    /// `migrateValidatorSets()` that back-fills the appended per-status `validatorSets` and cached
    /// `eligibleValidatorCount` from the preserved `currentStatus` source of truth.
    ///
    /// Fires exactly once, from the epoch-closing block that concludes
    /// `CONSENSUS_REGISTRY_FORK_EPOCH - 1`, as the FIRST step of that block's close-epoch handling
    /// — before `applyIncentives`/`concludeEpoch`, which then run on the swapped-in code with
    /// the migrated sets (the new committee read and eligible-count guard require them). From
    /// this block onward every node runs on the new code with populated sets.
    ///
    /// Determinism: the production path (`context_for_next_block`) and the replay path
    /// (`context_for_block`) build an identical `ctx.nonce`/`ctx.close_epoch` from the same
    /// `ConsensusOutput`, and this routine is a pure function of the committed state plus the
    /// embedded artifact, so every node re-derives a byte-identical `state_root`.
    ///
    /// Fatal on failure — a partial or failed migration diverges state across the fleet.
    #[cfg(feature = "adiri")]
    fn apply_consensus_registry_fork(&mut self) -> TnRethResult<()> {
        // revm `Database` trait provides `basic`; imported anonymously to avoid clashing with the
        // `alloy_evm::Database` already in module scope.
        use reth_revm::{
            state::{Account, AccountInfo, AccountStatus, EvmState},
            Database as _,
        };

        let (code, code_hash) = Self::consensus_registry_runtime_code().clone();

        // Preserve the registry account's balance + nonce; only the code changes. Reading via
        // `basic` also loads the account into the `State` cache so the code-only commit below takes
        // the `change` path (info updated, storage left intact) rather than creating a new account.
        let current = self
            .evm
            .db_mut()
            .basic(CONSENSUS_REGISTRY_ADDRESS)
            .map_err(|e| TnRethError::EVMCustom(format!("registry account read failed: {e}")))?
            .unwrap_or_default();

        // Fail closed on an unexpected pre-fork deployment. The swap + `migrateValidatorSets()`
        // assume the exact storage layout of the pinned pre-fork registry code (the registry is
        // non-upgradeable, so on the live chain this is the fixed genesis code hash); migrating
        // over anything else risks silent state corruption. Abort the block instead — the check is
        // a pure function of committed state, so every fork-capable node fails uniformly rather
        // than diverging.
        if current.code_hash != tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH {
            error!(
                target: "engine",
                pre_swap_code_hash = %current.code_hash,
                expected = %tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
                "consensus registry fork failing closed: unexpected pre-fork registry code",
            );
            return Err(TnRethError::EVMCustom(format!(
                "consensus registry fork failing closed: pre-swap code hash {} does not match \
                 the pinned pre-fork deployment {}",
                current.code_hash,
                tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
            )));
        }

        debug!(
            target: "engine",
            pre_swap_code_hash = %current.code_hash,
            new_code_hash = %code_hash,
            "applying consensus registry fork",
        );

        // Commit a code-only override: an empty storage map and a plain `Touched` status (never
        // `Created`/`SelfDestructed`) so no storage slot enters the bundle and the existing storage
        // root is preserved — only the single account-code leaf changes. The new bytecode is
        // carried inline on the account info, so it is registered in the bundle's contracts
        // (for post-restart `code_by_hash`) and is immediately loadable by the migration
        // call below.
        let account = Account {
            info: AccountInfo {
                balance: current.balance,
                nonce: current.nonce,
                code_hash,
                code: Some(code),
                ..Default::default()
            },
            status: AccountStatus::Touched,
            ..Default::default()
        };
        self.evm.db_mut().commit(EvmState::from_iter([(CONSENSUS_REGISTRY_ADDRESS, account)]));

        // Run the one-time migration. Dispatches to the just-swapped new code.
        let calldata = ConsensusRegistry::migrateValidatorSetsCall {}.abi_encode().into();
        let mut res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "error executing consensus registry migration: {:?}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "consensus registry migration failed: {e}"
                )));
            }
        };
        if !res.result.is_success() {
            error!(target: "engine", "failed consensus registry migration: {:?}", res.result);
            return Err(TnRethError::EVMCustom("failed consensus registry migration".to_string()));
        }

        // clean up SYSTEM_ADDRESS — only touched as the system caller, not a real state change
        res.state.remove(&SYSTEM_ADDRESS);
        self.evm.db_mut().commit(res.state);

        // Read back the rebuilt eligible count for an operational confirmation log. Best-effort and
        // deliberately non-fatal: the migration is already committed above, so a hiccup on this
        // cosmetic read must not abort the block (which would discard a valid, deterministic
        // migration) — and must not alarm at `error!` either, hence the `try_` variant + `debug!`.
        // Pure read — state is not committed.
        let calldata = ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into();
        let eligible = self
            .try_read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)
            .inspect_err(|e| {
                debug!(target: "engine", "non-fatal eligible-count readback after migration failed: {e}");
            })
            .ok()
            .and_then(|data| <U256 as alloy::sol_types::SolValue>::abi_decode(&data).ok());

        tracing::info!(target: "engine", ?eligible, %code_hash, "consensus registry fork applied");
        Ok(())
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata(&mut self, randomness: B256) -> TnRethResult<Bytes> {
        // shuffle all validators for new committee
        let mut new_committee = self.shuffle_new_committee(randomness)?;

        // sort addresses in ascending order (0x0...0xf)
        new_committee.sort();
        debug!(target: "engine", ?new_committee, "new committee sorted by address");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();

        Ok(bytes)
    }

    /// Generate calldata for applying incentives when concluding the epoch.
    fn generate_apply_incentives_calldata(
        &mut self,
        reward_infos: Vec<(Address, u32)>,
    ) -> TnRethResult<Bytes> {
        debug!(target: "engine", ?reward_infos, "applying incentives");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: reward_infos
                .iter()
                .map(|(address, count)| RewardInfo {
                    validatorAddress: *address,
                    consensusHeaderCount: U256::from(*count),
                })
                .collect(),
        }
        .abi_encode()
        .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    fn shuffle_new_committee(&mut self, randomness: B256) -> TnRethResult<Vec<Address>> {
        let new_committee_size = self.next_committee_size()?;

        let all_active_validators = self.read_committee_eligible_pool()?;

        debug!(target: "engine",  "validators pre-shuffle {:?}", all_active_validators);

        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        trace!(target: "engine", ?seed, "seed after");

        // used as deterministic randomness
        let mut rng = StdRng::from_seed(seed);

        // 1) separate active and pending validators
        // 2) check if active length is sufficient
        // 3) if missing, randomly select from the pending validators
        let (pending_exit, mut active_validators): (Vec<_>, Vec<_>) = all_active_validators
            .into_iter()
            .partition(|v| v.currentStatus == ValidatorStatus::PendingExit);

        let active_validator_count = active_validators.len();
        let mut validators_for_shuffle = if active_validator_count >= new_committee_size {
            // enough active validators for next committee
            active_validators
        } else {
            // NOTE: already checked if active_validator_count >= new_committee_size above
            let num_missing = new_committee_size - active_validator_count;

            // randomly take enough pending exit validators to reach new committee size
            let random_pending = pending_exit.into_iter().choose_multiple(&mut rng, num_missing);
            active_validators.extend(random_pending);
            active_validators
        };

        // simple Fisher-Yates shuffle
        for i in (1..validators_for_shuffle.len()).rev() {
            let j = rng.random_range(0..=i);
            validators_for_shuffle.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:?}", validators_for_shuffle);

        let mut new_committee =
            validators_for_shuffle.into_iter().map(|v| v.validatorAddress).collect::<Vec<_>>();

        // trim the shuffled committee to maintain correct size
        new_committee.truncate(new_committee_size);

        trace!(target: "engine",  ?new_committee_size, ?new_committee, "truncated shuffle for new committee");

        Ok(new_committee)
    }

    /// Read the committee-eligible validator pool from the consensus registry.
    ///
    /// The registry's per-status `getValidators`/`getValidatorsInfo` return ONLY the exact status
    /// set; the committee-eligible pool is the union of `{ Active, PendingActivation, PendingExit
    /// }` (the statuses for which `_eligibleForCommitteeNextEpoch` is true). The registry computes
    /// the O(1) eligible *count* on-chain and expects the protocol to assemble the eligible *pool*
    /// by unioning these queries off-chain. We use `getValidatorsInfo` (full structs) because the
    /// pending-exit validators are separated out by `currentStatus` in `shuffle_new_committee`.
    fn read_committee_eligible_pool(
        &mut self,
    ) -> TnRethResult<Vec<ConsensusRegistry::ValidatorInfo>> {
        // While the deployed registry still carries the pre-fork adiri code, `getValidatorsInfo`
        // does not exist on-chain — speak the legacy ABI instead. This keeps every pre-fork epoch
        // close (fresh-node onboarding, full resync, a fork-capable build deployed before
        // `CONSENSUS_REGISTRY_FORK_EPOCH`) executing byte-identically to the historical chain. At
        // the fork boundary `apply_consensus_registry_fork` swaps the code first, so this gate
        // already sees the upgraded hash and takes the post-fork read below.
        #[cfg(feature = "adiri")]
        if self.registry_code_is_pre_fork()? {
            return self.read_committee_eligible_pool_legacy();
        }

        let mut all_active_validators: Vec<ConsensusRegistry::ValidatorInfo> = Vec::new();
        for status in [
            ValidatorStatus::Active,
            ValidatorStatus::PendingActivation,
            ValidatorStatus::PendingExit,
        ] {
            let calldata = ConsensusRegistry::getValidatorsInfoCall { status: status.into() }
                .abi_encode()
                .into();
            let state =
                self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
            trace!(target: "engine", ?status, "get validators call:\n{:?}", state);
            let validators: Vec<ConsensusRegistry::ValidatorInfo> =
                alloy::sol_types::SolValue::abi_decode(&state)?;
            all_active_validators.extend(validators);
        }

        Ok(all_active_validators)
    }

    /// Whether the deployed `ConsensusRegistry` still carries the pre-fork adiri runtime code.
    ///
    /// Compares the registry account's code hash against the pinned
    /// [`tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`]. A pure `basic()` read: the
    /// account is neither touched nor committed, so the gate never enters the bundle/state root —
    /// it is a deterministic function of committed state, identical on the production and replay
    /// paths.
    ///
    /// A DB error propagates as an error; only a genuinely absent account (impossible on any real
    /// TN chain) falls through to the default (non-pre-fork) hash. An infra failure must never be
    /// silently read as "not V1".
    #[cfg(feature = "adiri")]
    fn registry_code_is_pre_fork(&mut self) -> TnRethResult<bool> {
        // revm `Database` trait provides `basic`; imported anonymously to avoid clashing with the
        // `alloy_evm::Database` already in module scope.
        use reth_revm::Database as _;

        let code_hash = self
            .evm
            .db_mut()
            .basic(CONSENSUS_REGISTRY_ADDRESS)
            .map_err(|e| TnRethError::EVMCustom(format!("registry account read failed: {e}")))?
            .unwrap_or_default()
            .code_hash;

        Ok(code_hash == tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH)
    }

    /// Read the committee-eligible pool via the PRE-fork registry ABI.
    ///
    /// Byte-exact replay of the protocol read every pre-fork epoch close was produced with: the
    /// pre-fork contract's `getValidators(uint8)` folds the whole committee-eligible union into
    /// the `Active` query and returns full `ValidatorInfo` structs. One call, one decode — the
    /// single-call return order feeds the Fisher-Yates shuffle exactly as the historical chain
    /// computed it, so re-executed pre-fork blocks derive byte-identical committees and state
    /// roots.
    ///
    /// The `getValidators(uint8)` selector is identical pre/post fork (only the declared return
    /// type changed), so encoding through the current `getValidatorsCall` binding produces the
    /// same calldata bytes the pre-fork node sent; the pre-fork `ValidatorInfo[]` return payload
    /// is decoded directly via `SolValue` (the struct layout is byte-identical across the fork),
    /// bypassing the binding's post-fork `address[]` return type.
    #[cfg(feature = "adiri")]
    fn read_committee_eligible_pool_legacy(
        &mut self,
    ) -> TnRethResult<Vec<ConsensusRegistry::ValidatorInfo>> {
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        trace!(target: "engine", "legacy get validators call:\n{:?}", state);
        let validators: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&state)?;

        Ok(validators)
    }

    /// Read state on-chain, logging any failure at `error!` level.
    ///
    /// Thin wrapper over [`Self::try_read_state_on_chain`] for consensus-critical reads, where a
    /// failure aborts the block and warrants an operator-facing error log.
    fn read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<Bytes> {
        self.try_read_state_on_chain(caller, contract, calldata).inspect_err(|e| {
            error!(target: "engine", ?caller, ?contract, "failed to read state on chain: {e}");
        })
    }

    /// Read state on-chain without logging failures.
    ///
    /// The two failure cases (the system call itself erroring vs an unsuccessful execution
    /// result) stay distinguishable through the error strings. Callers pick the log level their
    /// context warrants: consensus-critical reads go through [`Self::read_state_on_chain`]
    /// (`error!`), best-effort reads log at `debug!`.
    fn try_read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<Bytes> {
        // read from state
        let res = self
            .evm
            .transact_system_call(caller, contract, calldata)
            .map_err(|e| TnRethError::EVMCustom(format!("failed to read state on chain: {e}")))?;

        // retrieve data from execution result
        match res.result {
            ExecutionResult::Success { output, .. } => Ok(output.into_data()),
            e => Err(TnRethError::EVMCustom(format!("error reading state on chain: {e:?}"))),
        }
    }

    /// Return the next committee size.
    ///
    /// This is isolated into a function and requires a fork to change.
    fn next_committee_size(&mut self) -> TnRethResult<usize> {
        let calldata = ConsensusRegistry::getNextCommitteeSizeCall.abi_encode().into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        let next_committee_size: u16 = alloy::sol_types::SolValue::abi_decode(&state)?;
        trace!(target: "engine",  ?next_committee_size, "read next committee size from state");

        // this will fail on-chain if incorrect
        // NOTE: u16 -> u32/u64 safe
        Ok(next_committee_size as usize)
    }

    /// Applies the pre-block call to the EIP-4788 consensus root contract (cancun).
    fn apply_consensus_root_contract_call(&mut self) -> Result<(), BlockExecutionError> {
        if !self.spec.is_cancun_active_at_timestamp(self.evm.block().timestamp().saturating_to()) {
            return Ok(());
        }

        let parent_beacon_block_root = self
            .ctx
            .parent_beacon_block_root
            .ok_or(BlockValidationError::MissingParentBeaconBlockRoot)?;

        trace!(target: "engine", block_number=?self.evm.block().number(), ?parent_beacon_block_root, "evaluating parent root");

        // if the block number is zero (genesis block) then the parent beacon block root must
        // be 0x0 and no system transaction may occur as per EIP-4788
        if self.evm.block().number() == 0 {
            if !parent_beacon_block_root.is_zero() {
                return Err(BlockValidationError::CancunGenesisParentBeaconBlockRootNotZero {
                    parent_beacon_block_root,
                }
                .into());
            }

            return Ok(());
        }

        let mut res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            BEACON_ROOTS_ADDRESS,
            parent_beacon_block_root.0.into(),
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "failed to apply consensus root contract call: {:?}", e);
                return Err(BlockValidationError::BeaconRootContractCall {
                    parent_beacon_block_root: Box::new(parent_beacon_block_root),
                    message: e.to_string(),
                }
                .into());
            }
        };

        // NOTE: revm currently marks the caller and block beneficiary accounts as "touched"
        // after the above transact calls, and includes them in the result.
        //
        // Cleanup state here to make sure that changeset only includes the changed
        // contract storage.
        res.state.retain(|addr, _| *addr == BEACON_ROOTS_ADDRESS);
        trace!(target: "engine", ?res, "retained state");
        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Applies the pre-block call to the EIP-2935 blockhashes contract (pectra).
    fn apply_blockhashes_contract_call(&mut self) -> Result<(), BlockExecutionError> {
        trace!(target: "engine", "applying blockhashes contract call");
        if !self.spec.is_prague_active_at_timestamp(self.evm.block().timestamp().saturating_to()) {
            return Ok(());
        }

        // if the block number is zero (genesis block) then no system transaction may occur as per
        // EIP-2935
        if self.evm.block().number() == 0 {
            return Ok(());
        }

        let mut result_and_state = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            HISTORY_STORAGE_ADDRESS,
            self.ctx.parent_hash.into(),
        ) {
            Ok(res) => res,
            Err(e) => {
                error!(target: "engine", "failed to apply blockhashes contract call: {:?}", e);
                return Err(
                    BlockValidationError::BlockHashContractCall { message: e.to_string() }.into()
                );
            }
        };

        trace!(target: "engine", "result and state before: \n{:#?}", result_and_state);
        // NOTE: revm currently marks the caller and block beneficiary accounts as "touched"
        // after the above transact calls, and includes them in the result.
        //
        // Cleanup state here to make sure that changeset only includes the changed
        // contract storage.
        result_and_state.state.retain(|addr, _| *addr == HISTORY_STORAGE_ADDRESS);
        trace!(target: "engine", "result and state after: \n{:#?}", result_and_state);
        self.evm.db_mut().commit(result_and_state.state);

        Ok(())
    }
}

/// The result of executing a TN transaction.
#[derive(Debug)]
pub(crate) struct TNTxResult<H, T> {
    /// Result of the transaction execution.
    pub result: ResultAndState<H>,
    /// Type of the transaction.
    pub tx_type: T,
}

impl<H, T> TxResult for TNTxResult<H, T> {
    type HaltReason = H;

    fn result(&self) -> &ResultAndState<Self::HaltReason> {
        &self.result
    }
}

// alloy-evm
impl<'db, DB, E, Spec, R> BlockExecutor for TNBlockExecutor<E, Spec, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
    >,
    Spec: EthereumHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;
    type Result = TNTxResult<E::HaltReason, <R::Transaction as TransactionEnvelope>::TxType>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number().saturating_to());
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        // apply system calls and cleanup state
        if self.ctx.first_batch() {
            // only write consensus root once per output
            self.apply_consensus_root_contract_call()?;
        }

        // apply blockhashes cleanup state after
        self.apply_blockhashes_contract_call()?;

        Ok(())
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // don't support prague deposit requests
        let requests = Requests::default();

        // potentially close epoch boundary
        if let Some(randomness) = self.ctx.close_epoch {
            debug!(target: "engine", ?randomness, "ctx indicates close epoch");

            // In-protocol ConsensusRegistry fork boundary. `deconstruct_nonce(nonce).0` is the
            // epoch being concluded by this block; firing when `concluding_epoch + 1 ==
            // FORK_EPOCH` makes the swapped code + migrated per-status sets live for
            // the remainder of this very block.
            //
            // This MUST run BEFORE the rewards/conclude calls below. `shuffle_new_committee`
            // (inside `apply_closing_epoch_contract_call`) reads the committee-eligible
            // pool via `getValidatorsInfo` — the post-refactor ABI absent from the
            // pre-fork testnet code — and the new `concludeEpoch` guards the committee
            // size against the cached `eligibleValidatorCount`. Both only work once the
            // code is swapped and `migrateValidatorSets` has populated the sets, so the
            // fork leads. Reward math and the epoch transition then run on the new code
            // over the byte-identical preserved storage.
            //
            // Both the production and replay paths reach this with an identical `ctx`, so the
            // resulting `state_root` is byte-identical across the fleet.
            #[cfg(feature = "adiri")]
            if tn_types::deconstruct_nonce(self.ctx.nonce).0.checked_add(1)
                == Some(tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH)
            {
                self.apply_consensus_registry_fork().map_err(|e| {
                    BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
                })?;
            }

            self.apply_consensus_block_rewards(self.ctx.rewards_counter.get_address_counts())
                .map_err(|e| {
                    BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
                })?;

            self.apply_closing_epoch_contract_call(randomness).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;

            // merge transitions into bundle state
            self.evm.db_mut().merge_transitions(BundleRetention::Reverts);
        }

        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests,
                gas_used: self.gas_used,
                blob_gas_used: 0,
            },
        ))
    }

    fn set_state_hook(&mut self, _hook: Option<Box<dyn OnStateHook>>) {
        unimplemented!("not using SystemCaller - nothing to set hook on")
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, recovered) = tx.into_parts();

        // The sum of the transaction's gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block's gasLimit.
        let block_available_gas = self.evm.block().gas_limit() - self.gas_used;

        if recovered.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: recovered.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        // Execute transaction and return the result
        let result = self.evm.transact(tx_env).map_err(|err| {
            let hash = recovered.tx().trie_hash();
            BlockExecutionError::evm(err, hash)
        })?;

        Ok(TNTxResult { result, tx_type: recovered.tx().tx_type() })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> Result<u64, BlockExecutionError> {
        let TNTxResult { result: ResultAndState { result, state }, tx_type } = output;

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx_type,
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(gas_used)
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &self.receipts
    }
}

/// Block builder for TN.
#[derive(Debug, Clone)]
pub struct TNBlockAssembler<ChainSpec = reth_chainspec::ChainSpec> {
    /// The chainspec.
    pub chain_spec: Arc<ChainSpec>,
}

impl<ChainSpec> TNBlockAssembler<ChainSpec> {
    /// Creates a new [`TNBlockAssembler`].
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

// reth-evm
impl<F, ChainSpec> BlockAssembler<F> for TNBlockAssembler<ChainSpec>
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = TNBlockExecutionCtx,
        Transaction = TransactionSigned,
        Receipt = Receipt,
    >,
    ChainSpec: EthChainSpec + EthereumHardforks,
{
    type Block = Block<TransactionSigned>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Block<TransactionSigned>, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            transactions,
            output: BlockExecutionResult { receipts, gas_used, .. },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp().saturating_to();
        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = Receipt::calculate_receipt_root_no_memo(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        // set excess blob gas 0
        let excess_blob_gas = Some(0);
        let blob_gas_used =
            Some(transactions.iter().map(|tx| tx.blob_gas_used().unwrap_or_default()).sum());

        // TN-specific values
        let ommers_hash = ctx.ommers_hash; // batch hash (consensus)
        let nonce = ctx.nonce.into(); // subdag leader's nonce: ((epoch as u64) << 32) | self.round as u64
        let difficulty = ctx.difficulty; // worker id and batch index

        // use keccak256(bls_sig) if closing epoch or Bytes::default
        let extra_data = ctx.close_epoch.map(|hash| hash.to_vec().into()).unwrap_or_default();
        let (withdrawals, withdrawals_root) = if ctx.close_epoch.is_some() {
            // closing epoch so include rewards info
            let withdrawals = ctx.rewards_counter.generate_withdrawals();
            let withdrawals_root = calculate_withdrawals_root(withdrawals.as_ref());
            (Some(withdrawals), Some(withdrawals_root))
        } else {
            (Some(Withdrawals::default()), Some(EMPTY_WITHDRAWALS))
        };

        let header = ExecHeader {
            parent_hash: ctx.parent_hash,
            ommers_hash,
            beneficiary: evm_env.block_env.beneficiary(),
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao().unwrap_or_default(),
            nonce,
            base_fee_per_gas: Some(evm_env.block_env.basefee()),
            number: evm_env.block_env.number().saturating_to(),
            gas_limit: evm_env.block_env.gas_limit(),
            difficulty,
            gas_used: *gas_used,
            extra_data,
            parent_beacon_block_root: ctx.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,
            requests_hash: Some(EMPTY_REQUESTS_HASH),
        };

        Ok(Block {
            header,
            body: BlockBody { transactions, ommers: Default::default(), withdrawals },
        })
    }
}
