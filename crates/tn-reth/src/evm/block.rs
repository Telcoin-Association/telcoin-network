//! The types that build blocks for EVM execution.
//!
//! `TNBlockExecutionCtx` carries the consensus-derived inputs for one EVM block (one batch of a
//! `ConsensusOutput`), `TNBlockExecutor` executes transactions plus the TN system calls, and
//! `TNBlockAssembler` seals the results into a header. The TN-specific, consensus-critical
//! behavior lives in two places: the pre-block system calls and the epoch-close sequence in
//! `TNBlockExecutor::finish`.
//!
//! # Pre-execution order (`apply_pre_execution_changes`)
//!
//! 1. Set the EIP-161 state-clear flag per the Spurious Dragon activation.
//! 2. EIP-4788 beacon-root call — runs only on the FIRST batch of a consensus output
//!    (`TNBlockExecutionCtx::first_batch`), writing the parent `ConsensusHeader` digest once per
//!    output rather than once per block.
//! 3. EIP-2935 blockhashes call — runs on every block, recording the parent block hash.
//!
//! Both calls are additionally subject to the usual hardfork and genesis gates.
//!
//! # The epoch-close sequence (`finish`)
//!
//! When `ctx.close_epoch` is `Some(randomness)`, `finish` closes the epoch with system calls in
//! this exact order:
//!
//! 1. (`adiri` builds only) `apply_consensus_registry_fork` then `apply_worker_configs_fork` — fire
//!    only when the concluding epoch (`deconstruct_nonce(ctx.nonce).0`), plus one (checked), equals
//!    `CONSENSUS_REGISTRY_FORK_EPOCH`. The registry swap replaces the registry's runtime bytecode
//!    in place, then runs the one-time `migrateValidatorSets()`; the worker-configs swap then
//!    replaces the `WorkerConfigs` runtime bytecode (code only, no initializer). The two ship at
//!    the same boundary because the registry swap flips this very block's close onto the post-fork
//!    sequence, whose fourth call needs the `setWorkerConfigsData` selector the pre-fork
//!    `WorkerConfigs` deployment lacks — a build applying one swap but not the other aborts this
//!    block.
//! 2. `apply_closing_epoch_contract_call` - the four boundary system calls in order:
//!    `applyIncentives(RewardInfo[])`, `applySlashes(Slash[])`, `concludeEpoch(address[])`, then
//!    `setWorkerConfigsData(uint16[],uint184[])`. The reward infos carry the leader counts from
//!    `ctx.gas_accumulator`'s rewards counter; committee membership is drawn by the
//!    `randomness`-seeded Fisher-Yates shuffle (sorted by address before encoding), run AFTER
//!    `applySlashes` commits so the eligible-pool read and the committee reflect any slash-to-zero
//!    ejections. The ordering is a consensus-safety obligation the protocol owns: incentives weight
//!    on pre-slash collateral, slashes land before the committee is assembled, and `concludeEpoch`
//!    settles queued stake version changes from post-slash balances. While the deployed registry
//!    still carries the pre-fork adiri code, the close instead replays the legacy pair
//!    `applyIncentives` then `concludeEpoch(address[])` (same code-hash gate as the committee-pool
//!    read), keeping pre-fork history re-executable; the post-fork sequence differs by the
//!    interposed `applySlashes` call and the trailing worker-config write, and both share
//!    byte-identical `applyIncentives` and `concludeEpoch(address[])` selectors.
//!
//! The fourth call (`record_next_epoch_base_fees`) writes each EIP-1559 worker's next-epoch base
//! fee into its `WorkerConfigs` `data` word: an epoch-boundary snapshot that lets a node entering
//! the epoch read the fee from one state slot instead of scanning the whole prior epoch's headers
//! to recompute it. It runs last because it is the only step that reads no registry state and
//! writes to no registry state, so nothing in the epoch transition depends on its position.
//!
//! The order is load-bearing. The fork must lead: the code swap flips the code-hash gate in
//! `read_committee_eligible_pool` so the shuffle's committee-pool reads use the post-fork ABI
//! for the remainder of this very block, and the post-fork `concludeEpoch` validates the
//! committee size against the cached `eligibleValidatorCount` that only `migrateValidatorSets()`
//! populates.
//!
//! Every step is fatal on failure: the error aborts execution of the consensus output and
//! propagates out of the engine loop, so the node stops executing rather than committing a
//! block whose state diverges from the rest of the fleet.
//!
//! Note for auditors: protocol-side slashing is not live - the `slashes` array passed to
//! `applySlashes` is always empty (production builds; tests may inject slashes through the
//! `cfg(test)` seam on `TNPayload`).
//!
//! # System-call state hygiene
//!
//! `SYSTEM_ADDRESS` is a caller convention, not a real account mutation; it must never enter a
//! block's changeset. Every commit of a system-call result in this file strips it first:
//! `transact_and_commit_system_call` removes `SYSTEM_ADDRESS` from the result state, and the
//! EIP-4788/EIP-2935 pre-block calls `retain` only their target contract (also dropping the
//! touched beneficiary). Committing a system-call result without this cleanup places a spurious
//! touched account into the bundle and diverges the state root across nodes.
//!
//! # Randomness
//!
//! The epoch-close `randomness` (`ctx.close_epoch`) is whichever seed
//! `ConsensusOutput::committee_shuffle_seed` yields for the closing epoch, and that is epoch-gated
//! on `tn_types::forks::seed_signature_active`. For epochs where the gate is inactive (every
//! `adiri` epoch before `SEED_SIGNATURE_FORK_EPOCH`) it stays the LEGACY seed, keccak256 of the
//! leader certificate's aggregate BLS signature, wire-identical to pre-fork releases; for active
//! epochs it is the epoch seed chain value as of the closing commit, folded per commit in
//! `CommittedSubDag::new`. Do not read this module as describing the seed chain unconditionally:
//! the whole point of the gate is that pre-fork epochs re-execute exactly as they always did.
//!
//! Either way the value is carried through the payload, seeds the deterministic committee shuffle,
//! AND is stored as the block's `extra_data`, which is how the replay path (`context_for_block`)
//! rebuilds the ctx from the sealed header — identical up to `gas_accumulator`, which is the live
//! shared accumulator rather than header-derived (see the `evm/config.rs` module docs for the
//! replay caveat and the `block.body.withdrawals` reconstruction follow-up). The RNG draw order
//! inside the shuffle is consensus-critical: any refactor that reorders the draws selects a
//! different committee.

use crate::{
    error::{TnRethError, TnRethResult},
    system_calls::{
        decode_worker_fee_configs,
        ConsensusRegistry::{self, RewardInfo, ValidatorStatus},
        WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS,
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
    primitives::aliases::U184,
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
    DatabaseCommit as _, State,
};
use std::{collections::BTreeMap, sync::Arc};
use tn_config::WORKER_CONFIGS_ADDRESS;
use tn_types::{
    gas_accumulator::{next_base_fee_for_config, GasAccumulator, WorkerFeeConfig},
    Address, Bytes, Encodable2718, ExecHeader, Receipt, TransactionSigned, Withdrawals, WorkerId,
    B256, EMPTY_WITHDRAWALS, MIN_PROTOCOL_BASE_FEE, U256,
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
    /// The epoch-close committee-shuffle seed. `Some` only for the
    /// final batch of the epoch's last `ConsensusOutput`.
    ///
    /// When included, the executor runs the epoch-closing system calls (see the module docs) and
    /// seeds the deterministic committee shuffle with this value. It is derived per commit in
    /// `CommittedSubDag::new`: for fork-active epochs (#1086, `seed_signature_active`) it folds
    /// the previous commit's chain value with the leader header's digest-pinned `seed_signature`;
    /// pre-fork epochs retain the legacy keccak of the leader certificate's aggregate signature.
    /// It is also stored in the block's `extra_data`: both the marker
    /// clients use to recognize an epoch-closing block and the value the replay path
    /// (`context_for_block`) reads back to rebuild an identical ctx from the sealed header.
    pub close_epoch: Option<B256>,
    /// Difficulty- this contains the worker id and batch index:
    /// `U256::from(payload.batch_index << 16 | payload.worker_id as usize)`
    pub difficulty: U256,
    /// Live shared accumulator for the current epoch: per-worker gas totals, current base fees,
    /// worker count, and the leader counts used to allocate block rewards.
    ///
    /// Cloned from the EVM config by both context builders, so it is the same object every other
    /// component holds rather than a header-derived snapshot (see the `evm/config.rs` module docs
    /// for the replay caveat that follows from that). Execution reads only the rewards counter
    /// today — the gas and fee data is carried so the epoch-closing block executor can reach it.
    pub gas_accumulator: GasAccumulator,
    /// Test-only slash injection for the epoch boundary, copied from the payload
    /// (`context_for_next_block`). Feeds the executor's `epoch_boundary_slashes` seam so a test
    /// can drive a non-empty slash list through the production close path. Production builds have
    /// no such field.
    #[cfg(test)]
    pub epoch_boundary_slashes: Vec<ConsensusRegistry::Slash>,
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
    /// ```text
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

    /// The worker id packed into the low 16 bits of `difficulty`.
    ///
    /// Same mask the header-side [`crate::snapshot::worker_id_from_header`] applies, so a block's
    /// worker attribution is identical whether it is read from the execution context or from the
    /// sealed header afterwards (the gas accumulator's per-worker totals depend on that).
    fn worker_id(&self) -> WorkerId {
        (self.difficulty.into_limbs()[0] & 0xffff) as WorkerId
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

    /// Execute a system call from [`SYSTEM_ADDRESS`] to `contract` and commit its state changes.
    ///
    /// Shared implementation for the epoch system calls (`applyIncentives`, `applySlashes`,
    /// `concludeEpoch`, the legacy pre-fork close pair, `migrateValidatorSets`). Any failure —
    /// the call itself erroring or the execution result being unsuccessful — is fatal to the
    /// block; `description` names the call in the log and error strings, and a revert's decoded
    /// reason (with its selector and raw output in the log) rides along so the deterministic
    /// fleet halt this causes is diagnosable from the error alone.
    ///
    /// [`SYSTEM_ADDRESS`] is removed from the result state before commit: it is only touched as
    /// the system caller, not a real state change — leaving it in the changeset would put a
    /// spurious account into the bundle and diverge the state root. Every commit of a
    /// system-call result must uphold this invariant; the EIP-4788/EIP-2935 pre-block calls do
    /// so with a `retain` of their target contract, which also drops the touched beneficiary.
    fn transact_and_commit_system_call(
        &mut self,
        contract: Address,
        calldata: Bytes,
        description: &str,
    ) -> TnRethResult<()> {
        let mut res = match self.evm.transact_system_call(SYSTEM_ADDRESS, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error executing {description} contract call: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("{description} failed: {e}")));
            }
        };

        // return error if the call executed but did not succeed, keeping Revert (decoded
        // reason + selector) distinguishable from Halt
        match &res.result {
            ExecutionResult::Success { .. } => {}
            ExecutionResult::Revert { output, gas_used } => {
                let reason = alloy::sol_types::decode_revert_reason(output)
                    .unwrap_or_else(|| "<undecodable revert reason>".to_string());
                let selector = output
                    .get(..4)
                    .map(alloy::hex::encode_prefixed)
                    .unwrap_or_else(|| format!("<{} bytes>", output.len()));
                error!(
                    target: "engine",
                    %selector,
                    raw_output = %output,
                    gas_used,
                    "failed {description} call: reverted: {reason}"
                );
                return Err(TnRethError::EVMCustom(format!(
                    "failed {description}: reverted: {reason}"
                )));
            }
            ExecutionResult::Halt { reason, gas_used } => {
                error!(
                    target: "engine",
                    gas_used,
                    "failed {description} call: halted: {reason:?}"
                );
                return Err(TnRethError::EVMCustom(format!(
                    "failed {description}: halted: {reason:?} (gas used {gas_used})"
                )));
            }
        }
        trace!(target: "engine", ?res, "{description}");

        // clean up SYSTEM_ADDRESS — only touched as the system caller, not a real state change
        res.state.remove(&SYSTEM_ADDRESS);
        // commit the changes
        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Close the epoch through the four boundary system calls, in order:
    /// `applyIncentives(RewardInfo[])`, `applySlashes(Slash[])`, `concludeEpoch(address[])`,
    /// `setWorkerConfigsData(uint16[],uint184[])`.
    ///
    /// The order is a consensus-safety invariant. `applySlashes` commits before the committee is
    /// assembled, so the `nextCommitteeSize` read and the shuffle below both see any slash-to-zero
    /// ejections: the committee `concludeEpoch` validates cannot be stale, and an ejected
    /// validator is never seated in a future committee. Slashing is not live, so the slashes array
    /// is always empty (production builds) and `applySlashes` is a no-op today; the call is
    /// issued regardless so the sequence is correct by construction when slashing ships. The
    /// base-fee record trails the registry calls (see [`Self::record_next_epoch_base_fees`]): it
    /// touches a different contract, so nothing in the epoch transition reads what it writes.
    fn apply_closing_epoch_contract_call(
        &mut self,
        randomness: B256,
        rewards: BTreeMap<Address, u32>,
    ) -> TnRethResult<()> {
        debug!(target: "engine", ?randomness, "applying closing contract call");
        let reward_infos: Vec<(Address, u32)> =
            rewards.iter().map(|(address, count)| (*address, *count)).collect();

        // While the deployed registry still carries the pre-fork adiri code, close with the
        // legacy two-call sequence instead, so every pre-fork epoch close (fresh-node onboarding,
        // full resync) executes byte-identically to the historical chain. At the fork boundary
        // `apply_consensus_registry_fork` swaps the code first, so this gate already sees the
        // upgraded hash and takes the post-fork sequence below. The post-fork `applyIncentives`
        // and `concludeEpoch(address[])` selectors are byte-identical to the legacy ones; the
        // post-fork sequence differs only by the interposed `applySlashes` call.
        #[cfg(feature = "adiri")]
        if self.registry_code_is_pre_fork()? {
            return self.apply_closing_epoch_contract_call_legacy(randomness, reward_infos);
        }

        // 1. incentives, weighted on pre-slash collateral
        let calldata = ConsensusRegistry::applyIncentivesCall {
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
        self.transact_and_commit_system_call(
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
            "applying incentives",
        )?;

        // 2. slashes, landing before the committee is read
        let slashes = self.epoch_boundary_slashes();
        let calldata = ConsensusRegistry::applySlashesCall { slashes }.abi_encode().into();
        self.transact_and_commit_system_call(
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
            "applying slashes",
        )?;

        // 3. assemble the committee from the post-slash eligible pool, then conclude
        let calldata = self.generate_conclude_epoch_calldata(randomness)?;
        trace!(target: "engine", ?calldata, "close epoch calldata");
        self.transact_and_commit_system_call(
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
            "closing epoch",
        )?;

        // 4. publish the next epoch's per-worker base fees for the epoch that just began
        self.record_next_epoch_base_fees()
    }

    /// Record every EIP-1559 worker's NEXT-epoch base fee in the `WorkerConfigs` contract's
    /// per-worker `data` word.
    ///
    /// Fourth and last of the closing block's system calls, so the write lands in the same block
    /// that seats the new committee: a node entering the epoch reads each worker's fee from one
    /// state slot instead of scanning the whole prior epoch's headers to recompute it.
    ///
    /// The value written for a worker MUST equal what the live producer's post-close
    /// `adjust_base_fees` (in `tn_node::manager`) and every node's epoch-entry header-scan
    /// derivation (`derive_base_fees_for_entered_epoch`) compute for it — the batch validator
    /// snapshots a plain `u64` per epoch and compares base fees for exact equality, so a
    /// one-wei divergence makes peers reject each other's batches for a whole epoch. Three
    /// details carry that equivalence:
    ///
    /// - The worker set comes from the CONTRACT's `numWorkers`, not the accumulator's. Governance
    ///   may have grown the worker set mid-epoch (a `setNumWorkers` only takes effect at this
    ///   boundary), and both other seams read the count at this same closing block. A worker with
    ///   no accumulator slot yet prices from `(MIN_PROTOCOL_BASE_FEE, 0 gas)`, which is exactly
    ///   what `adjust_base_fees`' resize-then-compute and the entry walk's slot-creation anchor
    ///   produce for a fresh slot.
    /// - This block's own gas is folded into its worker's total. The accumulator does not include
    ///   it yet (`inc_block` runs after the payload executes), while the post-close adjustment and
    ///   the header scan both count this block.
    /// - The accumulator is only READ. It is shared live with the batch validator and the node
    ///   manager, so resizing or clearing it here would corrupt state those readers depend on.
    ///
    /// `Static` workers are skipped: their fee is already on-chain in the config's `value` word,
    /// so recording it would be redundant. A closing block with no EIP-1559 worker therefore
    /// issues no system call at all — an emptiness every node computes identically from the same
    /// contract state, so skipping cannot diverge the fleet.
    ///
    /// Fatal on any failure (read, decode, or a reverting/halting write), like every other step of
    /// the close: the error aborts execution of the consensus output rather than committing a
    /// block whose state diverges from the rest of the fleet. That is deliberately the opposite
    /// of the manager-side close-time update (`apply_close_time_fee_updates` in `tn-node`), which
    /// fails OPEN on a chain-global `WorkerConfigs` read failure — keeping the current fees is
    /// safe there because every node deterministically computes the same keep. A silently skipped
    /// write here would instead leave a stale `data` word for the entry-time read of the recorded
    /// fee to trust later, so fatal-and-halt beats silently-wrong state.
    fn record_next_epoch_base_fees(&mut self) -> TnRethResult<()> {
        let calldata = WorkerConfigs::getAllWorkerConfigsCall {}.abi_encode().into();
        let data = self.read_state_on_chain(SYSTEM_ADDRESS, WORKER_CONFIGS_ADDRESS, calldata)?;
        let (num_workers, entries) = decode_worker_fee_configs(&data).map_err(|e| {
            error!(target: "engine", "failed to decode worker configs at epoch close: {e}");
            TnRethError::EVMCustom(format!(
                "failed to decode worker configs while recording next-epoch base fees: {e}"
            ))
        })?;

        let own_worker = self.ctx.worker_id();
        let own_gas = self.gas_used;
        let accumulator_workers = self.ctx.gas_accumulator.num_workers();

        let mut worker_ids: Vec<WorkerId> = Vec::new();
        let mut datas: Vec<U184> = Vec::new();
        for (worker_id, entry) in entries.iter().enumerate() {
            let worker_id = worker_id as WorkerId;
            match entry.config {
                WorkerFeeConfig::Eip1559 { .. } => {}
                // a static worker's fee is the config's `value` on-chain already
                WorkerFeeConfig::Static { .. } => continue,
            }

            let (current_fee, gas_used) = if (worker_id as usize) < accumulator_workers {
                let (_blocks, gas_used, _gas_limit) =
                    self.ctx.gas_accumulator.get_values(worker_id);
                (self.ctx.gas_accumulator.base_fee(worker_id).base_fee(), gas_used)
            } else {
                // governance added this worker mid-epoch: it has no slot to read, and a fresh
                // slot is created with the min fee and zero gas
                (MIN_PROTOCOL_BASE_FEE, 0)
            };
            // this block's gas reaches the accumulator only after execution finishes
            let gas_used =
                if worker_id == own_worker { gas_used.saturating_add(own_gas) } else { gas_used };

            worker_ids.push(worker_id);
            datas.push(U184::from(next_base_fee_for_config(entry.config, current_fee, gas_used)));
        }

        if worker_ids.is_empty() {
            debug!(
                target: "engine",
                num_workers,
                own_worker,
                own_gas,
                "no eip1559 workers configured; skipping next-epoch base fee record"
            );
            return Ok(());
        }

        debug!(
            target: "engine",
            num_workers,
            own_worker,
            own_gas,
            ?worker_ids,
            ?datas,
            "recording next-epoch base fees"
        );

        let calldata = WorkerConfigs::setWorkerConfigsDataCall { workerIds: worker_ids, datas }
            .abi_encode()
            .into();
        self.transact_and_commit_system_call(
            WORKER_CONFIGS_ADDRESS,
            calldata,
            "recording next-epoch base fees",
        )
    }

    /// The slashes to submit at this epoch boundary.
    ///
    /// Slashing is not live: this always returns an empty list (production builds) and
    /// `applySlashes` runs as a no-op. An automated slash producer plugs in here and nowhere
    /// else. The caller sequences the returned slashes through `applySlashes` BEFORE the
    /// committee size and eligible pool are read, so a slash-to-zero ejection is always
    /// reflected in the committee that `concludeEpoch` validates. That end-to-end ordering is
    /// pinned by `test_epoch_boundary_slash_ejects_through_production_close`, which injects a
    /// slash-to-zero through the test seam and drives it through this production close path.
    ///
    /// Sizing contract for a future slash producer: amounts must be computed against
    /// POST-incentive balances. `applyIncentives` credits `balances[validator]` before
    /// `applySlashes` runs, and the registry ejects only when `balance <= slash.amount`
    /// (otherwise it decrements and keeps the validator seated) — so an amount sized from the
    /// pre-boundary balance under-slashes: the validator keeps the incentive delta and is never
    /// ejected.
    ///
    /// Determinism: every node must derive an identical list (content and order) from the same
    /// certified consensus output, or the boundary block diverges across the fleet.
    #[cfg(not(test))]
    fn epoch_boundary_slashes(&self) -> Vec<ConsensusRegistry::Slash> {
        Vec::new()
    }

    /// Test-only body for the epoch-boundary slash seam: yields the slashes injected through
    /// `TNPayload::with_epoch_boundary_slashes` (carried on the execution ctx). See the
    /// production body above for the ordering, sizing, and determinism contract.
    #[cfg(test)]
    fn epoch_boundary_slashes(&self) -> Vec<ConsensusRegistry::Slash> {
        self.ctx.epoch_boundary_slashes.clone()
    }

    /// Close the epoch via the PRE-fork registry ABI: `applyIncentives(RewardInfo[])` followed
    /// by `concludeEpoch(address[])`.
    ///
    /// Byte-exact replay of the two system calls every pre-fork epoch close was produced with,
    /// in the same order, so re-executed pre-fork blocks derive identical state roots. Pre-fork
    /// chains never executed `applySlashes` (slashing was never live), so it is absent here. The
    /// committee shuffle routes its pool read through the legacy ABI via the same code-hash gate
    /// (`read_committee_eligible_pool`).
    #[cfg(feature = "adiri")]
    fn apply_closing_epoch_contract_call_legacy(
        &mut self,
        randomness: B256,
        reward_infos: Vec<(Address, u32)>,
    ) -> TnRethResult<()> {
        use crate::system_calls::LegacyConsensusRegistry;

        let calldata = LegacyConsensusRegistry::applyIncentivesCall {
            rewardInfos: reward_infos
                .iter()
                .map(|(address, count)| LegacyConsensusRegistry::RewardInfo {
                    validatorAddress: *address,
                    consensusHeaderCount: U256::from(*count),
                })
                .collect(),
        }
        .abi_encode()
        .into();
        self.transact_and_commit_system_call(
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
            "applying consensus block rewards",
        )?;

        let mut new_committee = self.shuffle_new_committee(randomness)?;
        new_committee.sort();
        debug!(target: "engine", ?new_committee, "legacy new committee sorted by address");
        let calldata = LegacyConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();
        self.transact_and_commit_system_call(CONSENSUS_REGISTRY_ADDRESS, calldata, "closing epoch")
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
        use tn_config::CONSENSUS_REGISTRY_JSON;

        static CODE: LazyLock<(Bytecode, B256)> = LazyLock::new(|| {
            let value = crate::RethEnv::fetch_value_from_json_str(
                CONSENSUS_REGISTRY_JSON,
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

    /// The upgraded `WorkerConfigs` runtime bytecode and its code hash, sourced from the same
    /// embedded artifact genesis deploys (`WORKER_CONFIGS_JSON` `deployedBytecode.object`).
    ///
    /// Materialized once. The embedded artifact is a compile-time `include_str!` constant (the
    /// same bytes genesis generation decodes, and exercised by the fork unit test), so a decode
    /// failure here is a corrupt build — uniform across the fleet — not a live-node runtime
    /// condition; hence `expect` rather than a fallible return.
    #[cfg(feature = "adiri")]
    fn worker_configs_runtime_code() -> &'static (reth_revm::bytecode::Bytecode, B256) {
        use reth_revm::bytecode::Bytecode;
        use std::sync::LazyLock;
        use tn_config::WORKER_CONFIGS_JSON;

        static CODE: LazyLock<(Bytecode, B256)> = LazyLock::new(|| {
            let value = crate::RethEnv::fetch_value_from_json_str(
                WORKER_CONFIGS_JSON,
                Some("deployedBytecode.object"),
            )
            .expect("embedded worker configs artifact json is valid");
            let hex_str =
                value.as_str().expect("worker configs deployedBytecode.object is a string");
            let raw = alloy::hex::decode(hex_str)
                .expect("worker configs deployedBytecode.object is valid hex");
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
    /// — before `concludeEpoch`, which then runs on the swapped-in code with
    /// the migrated sets (the new committee read and eligible-count guard require them). From
    /// this block onward every node runs on the new code with populated sets.
    ///
    /// Determinism: the production path (`context_for_next_block`) and the replay path
    /// (`context_for_block`) build an identical `ctx.nonce`/`ctx.close_epoch` from the same
    /// `ConsensusOutput`, and this routine is a pure function of the committed state plus the
    /// embedded artifact, so every node re-derives a byte-identical `state_root`.
    ///
    /// Fail-closed gate: the swap proceeds only if the registry account's current code hash
    /// equals the pinned `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`; any other deployment aborts the
    /// block. Aborting cannot split the network: the check is a pure function of committed
    /// state, so every fork-capable node evaluates it identically and fails (or passes) in
    /// lockstep.
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
        self.transact_and_commit_system_call(
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
            "consensus registry migration",
        )?;

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
            .and_then(|data| {
                <U256 as alloy::sol_types::SolValue>::abi_decode(&data)
                    .inspect_err(|e| {
                        debug!(
                            target: "engine",
                            "non-fatal eligible-count readback after migration returned undecodable data: {e}"
                        );
                    })
                    .ok()
            });

        tracing::info!(target: "engine", ?eligible, %code_hash, "consensus registry fork applied");
        Ok(())
    }

    /// Apply the in-protocol `WorkerConfigs` fork.
    ///
    /// Swaps the deployed worker-configs runtime bytecode to the current artifact's — preserving
    /// the account's balance, nonce, and **all** existing storage (slots 0-3: `_owner`, the
    /// `_pendingOwner` + `numWorkers` packing, the `_workerConfigs` mapping, and
    /// `_workerConfigSet`), so the swap rewrites only the account-code leaf.
    ///
    /// Ships at the SAME fork boundary as [`Self::apply_consensus_registry_fork`] (the
    /// epoch-closing block of `CONSENSUS_REGISTRY_FORK_EPOCH - 1`, registry first) because the
    /// two are coupled through this very block's close: the registry swap flips the code-hash
    /// gate in `apply_closing_epoch_contract_call` onto the post-fork sequence, whose fourth
    /// call ([`Self::record_next_epoch_base_fees`]) invokes the `setWorkerConfigsData` selector
    /// — absent from the live pre-fork `WorkerConfigs` deployment (an old build whose
    /// `WorkerConfig.data` is a `uint128` and which exposes no protocol write path at all). A
    /// build applying one swap but not the other therefore diverges from fork-capable peers: its
    /// own fourth system call reverts and aborts this block.
    ///
    /// Unlike the registry fork there is NO migrate-style initializer call: the pre-fork
    /// deployment already holds `_workerConfigSet[0] = true` and `numWorkers = 1`, so
    /// `getAllWorkerConfigs` (and the `_workerConfigSet` guard inside `setWorkerConfigsData`)
    /// work immediately post-swap. The appended `maxStrategy` slot (slot 4) deliberately reads 0
    /// after the code-only swap: a documented governance runbook item — one owner
    /// `setMaxStrategy(1)` transaction after the fork (see
    /// `tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH`). The protocol write path never reads
    /// `maxStrategy`, so the zeroed ceiling gates future governance actions only.
    ///
    /// Fail-closed gate: the swap proceeds only if the worker-configs account's current code
    /// hash equals the pinned `WORKER_CONFIGS_PRE_FORK_CODE_HASH`; any other deployment aborts
    /// the block. Aborting cannot split the network: the check is a pure function of committed
    /// state, so every fork-capable node evaluates it identically and fails (or passes) in
    /// lockstep.
    ///
    /// Fatal on failure — a partial swap diverges state across the fleet.
    #[cfg(feature = "adiri")]
    fn apply_worker_configs_fork(&mut self) -> TnRethResult<()> {
        // revm `Database` trait provides `basic`; imported anonymously to avoid clashing with the
        // `alloy_evm::Database` already in module scope.
        use reth_revm::{
            state::{Account, AccountInfo, AccountStatus, EvmState},
            Database as _,
        };

        let (code, code_hash) = Self::worker_configs_runtime_code().clone();

        // Preserve the worker-configs account's balance + nonce; only the code changes. Reading
        // via `basic` also loads the account into the `State` cache so the code-only commit below
        // takes the `change` path (info updated, storage left intact) rather than creating a new
        // account.
        let current = self
            .evm
            .db_mut()
            .basic(WORKER_CONFIGS_ADDRESS)
            .map_err(|e| {
                TnRethError::EVMCustom(format!("worker configs account read failed: {e}"))
            })?
            .unwrap_or_default();

        // Fail closed on an unexpected pre-fork deployment. The code-only swap assumes the exact
        // storage layout of the pinned pre-fork worker-configs code (on the live chain this is
        // the fixed genesis code hash); swapping over anything else risks silent state
        // corruption. Abort the block instead — the check is a pure function of committed state,
        // so every fork-capable node fails uniformly rather than diverging.
        if current.code_hash != tn_types::forks::WORKER_CONFIGS_PRE_FORK_CODE_HASH {
            error!(
                target: "engine",
                pre_swap_code_hash = %current.code_hash,
                expected = %tn_types::forks::WORKER_CONFIGS_PRE_FORK_CODE_HASH,
                "worker configs fork failing closed: unexpected pre-fork worker configs code",
            );
            return Err(TnRethError::EVMCustom(format!(
                "worker configs fork failing closed: pre-swap code hash {} does not match the \
                 pinned pre-fork deployment {}",
                current.code_hash,
                tn_types::forks::WORKER_CONFIGS_PRE_FORK_CODE_HASH,
            )));
        }

        debug!(
            target: "engine",
            pre_swap_code_hash = %current.code_hash,
            new_code_hash = %code_hash,
            "applying worker configs fork",
        );

        // Commit a code-only override: an empty storage map and a plain `Touched` status (never
        // `Created`/`SelfDestructed`) so no storage slot enters the bundle and the existing
        // storage root is preserved — only the single account-code leaf changes. The new bytecode
        // is carried inline on the account info, so it is registered in the bundle's contracts
        // (for post-restart `code_by_hash`) and is immediately callable by the fourth system call
        // of this same block's close.
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
        self.evm.db_mut().commit(EvmState::from_iter([(WORKER_CONFIGS_ADDRESS, account)]));

        tracing::info!(target: "engine", %code_hash, "worker configs fork applied");
        Ok(())
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    ///
    /// The seeded shuffle decides committee membership; the shuffled addresses are then sorted
    /// ascending, so the encoded committee list is order-normalized while membership remains a
    /// pure function of the RNG draws.
    fn generate_conclude_epoch_calldata(&mut self, randomness: B256) -> TnRethResult<Bytes> {
        // shuffle all validators for new committee. Runs after `applySlashes` has committed, so
        // the eligible-pool read reflects any slash-to-zero ejections
        let mut new_committee = self.shuffle_new_committee(randomness)?;

        // sort addresses in ascending order (0x0...0xf)
        new_committee.sort();
        debug!(target: "engine", ?new_committee, "new committee sorted by address");

        let bytes = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    ///
    /// The deterministic assembly, trim, and undersized-committee check live in the pure
    /// [`assemble_new_committee`] free function; this method only performs the on-chain reads and
    /// seeds the RNG so that the assembly logic stays unit-testable without a live EVM state.
    ///
    /// `randomness` is `ctx.close_epoch`, the epoch-gated committee-shuffle seed (epoch seed chain
    /// value for fork-active epochs, legacy leader-aggregate keccak before the fork), used
    /// verbatim as the `StdRng` seed, so every node derives the same RNG
    /// stream. The downstream draw order is consensus-critical: the draws decide which
    /// validators survive the truncation to committee size.
    fn shuffle_new_committee(&mut self, randomness: B256) -> TnRethResult<Vec<Address>> {
        let new_committee_size = self.next_committee_size()?;

        let all_active_validators = self.read_committee_eligible_pool()?;

        debug!(target: "engine",  "validators pre-shuffle {:?}", all_active_validators);

        // create seed from the epoch-close randomness carried in `ctx.close_epoch`
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        trace!(target: "engine", ?seed, "seed after");

        // used as deterministic randomness
        let mut rng = StdRng::from_seed(seed);

        assemble_new_committee(new_committee_size, all_active_validators, &mut rng)
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

        // pre-block system calls; each commit retains only the target contract's state
        if self.ctx.first_batch() {
            // EIP-4788: write the consensus header digest only once per output (first batch)
            self.apply_consensus_root_contract_call()?;
        }

        // EIP-2935: record the parent block hash on every block
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
            // This MUST run BEFORE the conclude call below. `shuffle_new_committee`
            // (inside `apply_closing_epoch_contract_call`) routes its committee-pool read
            // by the registry's code hash (`read_committee_eligible_pool`): swapping first
            // flips that gate to the post-fork `getValidatorsInfo` union for the remainder
            // of this very block, and the new `concludeEpoch` guards the committee size
            // against the cached `eligibleValidatorCount` that only `migrateValidatorSets`
            // populates — so the fork leads. Reward math and the epoch transition then run
            // on the new code over the byte-identical preserved storage. (Every epoch close
            // BEFORE this boundary sees the pre-fork code hash and takes the gate's legacy
            // branch instead, keeping pre-fork history re-executable on this same binary.)
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

                // The `WorkerConfigs` swap rides the same boundary, AFTER the registry swap:
                // the registry swap just flipped this block's close onto the post-fork
                // sequence, whose fourth call (`record_next_epoch_base_fees`) needs the
                // `setWorkerConfigsData` selector the pre-fork deployment lacks. Code-only —
                // no migrate-style call — see `apply_worker_configs_fork` for the storage
                // preservation and `maxStrategy` runbook notes.
                self.apply_worker_configs_fork().map_err(|e| {
                    BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
                })?;
            }

            self.apply_closing_epoch_contract_call(
                randomness,
                self.ctx.gas_accumulator.rewards_counter().get_address_counts(),
            )
            .map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;

            // deliberately NO merge_transitions here: both reth wrappers merge after finish()
            // returns, and revm pushes a reverts entry per merge — merging in here too gives
            // every epoch-closing block a phantom empty bundle.reverts entry (len 2 vs 1)
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
        // TN does not use reth's SystemCaller, so there is nothing to attach a hook to. The
        // trait returns `()`, leaving no way to refuse: log loudly and drop the hook instead of
        // panicking mid-block. A caller that relied on hook callbacks silently degrades — the
        // error line is the only signal.
        error!(target: "engine", "set_state_hook called but TN has no SystemCaller; dropping hook");
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
        //
        // The subtraction cannot underflow: `gas_used` only grows in `commit_transaction`, by a
        // result whose gas consumption is capped by its transaction's gas limit — which this
        // check proved fits in the remaining budget. Under the execute-then-commit-per-tx
        // protocol (the trait's provided `execute_transaction*` methods and reth's
        // `BasicBlockBuilder` both pair each execution with its commit before the next), the
        // invariant `gas_used <= gas_limit` therefore holds at every entry.
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
            let withdrawals = ctx.gas_accumulator.rewards_counter().generate_withdrawals();
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

/// Deterministically assemble the next committee from the eligible validator pool.
///
/// Given `randomness`-seeded `rng`, this partitions the pool into active and pending-exit
/// validators, folds in randomly chosen pending-exit validators only when the active set is short
/// of `new_committee_size`, runs an in-place Fisher-Yates shuffle, and trims the result to the
/// target size.
///
/// [`Vec::truncate`] caps the length at `new_committee_size` but is a silent no-op when the
/// assembled pool is already smaller, so a pool below the target would otherwise flow through as an
/// undersized committee. The registry invariant `nextCommitteeSize <= eligibleValidatorCount` makes
/// that unreachable in practice, and the on-chain `concludeEpoch` guard rejects a wrong-length
/// committee, but the final client-side check fails here with the exact counts
/// ([`TnRethError::UndersizedCommittee`]) instead of forwarding calldata that can only revert
/// on-chain.
///
/// Split out of [`TNBlockExecutor::shuffle_new_committee`] as a pure function so the
/// assembly/trim/validate logic is unit-testable without a live EVM state. The RNG draw sequence is
/// preserved verbatim from the historical implementation, so committees stay byte-identical to the
/// chain's replayed history.
fn assemble_new_committee(
    new_committee_size: usize,
    eligible_pool: Vec<ConsensusRegistry::ValidatorInfo>,
    rng: &mut StdRng,
) -> TnRethResult<Vec<Address>> {
    // a zero target would sail through the final length check below (`0 == 0`) and forward
    // `concludeEpoch([])` — an opaque on-chain revert; refuse it here with a distinct message
    if new_committee_size == 0 {
        return Err(TnRethError::EVMCustom(
            "next committee size is zero: refusing to conclude the epoch with an empty committee"
                .to_string(),
        ));
    }

    // 1) separate active and pending validators
    // 2) check if active length is sufficient
    // 3) if missing, randomly select from the pending validators
    let (pending_exit, mut active_validators): (Vec<_>, Vec<_>) =
        eligible_pool.into_iter().partition(|v| v.currentStatus == ValidatorStatus::PendingExit);

    let active_validator_count = active_validators.len();
    let mut validators_for_shuffle = if active_validator_count >= new_committee_size {
        // enough active validators for next committee
        active_validators
    } else {
        // NOTE: already checked if active_validator_count >= new_committee_size above
        let num_missing = new_committee_size - active_validator_count;

        // randomly take enough pending exit validators to reach new committee size
        let random_pending = pending_exit.into_iter().choose_multiple(rng, num_missing);
        active_validators.extend(random_pending);
        active_validators
    };

    // simple Fisher-Yates shuffle; the draw order is consensus-critical, so it is preserved
    // verbatim as a `for_each` over the same reversed range rather than rewritten in a way that
    // would reorder the RNG draws.
    (1..validators_for_shuffle.len()).rev().for_each(|i| {
        let j = rng.random_range(0..=i);
        validators_for_shuffle.swap(i, j);
    });

    debug!(target: "engine",  "validators post-shuffle {:?}", validators_for_shuffle);

    let mut new_committee =
        validators_for_shuffle.into_iter().map(|v| v.validatorAddress).collect::<Vec<_>>();

    // trim the shuffled committee to maintain correct size
    new_committee.truncate(new_committee_size);

    trace!(target: "engine",  ?new_committee_size, ?new_committee, "truncated shuffle for new committee");

    // truncate only ever shrinks, so a length mismatch here means the eligible pool was undersized.
    let committee_len = new_committee.len();
    (committee_len == new_committee_size).then_some(new_committee).ok_or(
        TnRethError::UndersizedCommittee { expected: new_committee_size, got: committee_len },
    )
}

/// Unit tests for the deterministic committee-assembly logic.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        system_calls::EpochState,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            TransactionFactory,
        },
        RethChainSpec, RethEnv,
    };
    use alloy::primitives::utils::parse_ether;
    use tempfile::TempDir;
    use tn_config::NodeInfo;
    #[cfg(feature = "adiri")]
    use tn_config::{CONSENSUS_REGISTRY_JSON, WORKER_CONFIGS_JSON};
    use tn_types::{
        generate_proof_of_possession_bls_for_test, BlsKeypair, GenesisAccount, NodeP2pInfo,
        TaskManager,
    };

    /// Build a minimal [`ConsensusRegistry::ValidatorInfo`] for committee-assembly tests.
    ///
    /// Only `validatorAddress` and `currentStatus` steer [`assemble_new_committee`]; the remaining
    /// fields are irrelevant to shuffling and trimming, so they are zeroed.
    fn validator(last_byte: u8, status: ValidatorStatus) -> ConsensusRegistry::ValidatorInfo {
        ConsensusRegistry::ValidatorInfo {
            validatorAddress: Address::with_last_byte(last_byte),
            activationEpoch: 0,
            exitEpoch: 0,
            currentStatus: status,
            isRetired: false,
            stakeVersion: 0,
            region: 0,
        }
    }

    /// An eligible pool smaller than the target size must surface
    /// [`TnRethError::UndersizedCommittee`] with the exact counts instead of silently returning a
    /// short committee.
    #[test]
    fn assemble_rejects_undersized_pool() {
        let pool =
            vec![validator(1, ValidatorStatus::Active), validator(2, ValidatorStatus::Active)];
        let mut rng = StdRng::from_seed([7u8; 32]);

        let result = assemble_new_committee(5, pool, &mut rng);

        assert!(matches!(result, Err(TnRethError::UndersizedCommittee { expected: 5, got: 2 })));
    }

    /// A pool at least as large as the target yields a committee of exactly the target size drawn
    /// only from the eligible pool.
    #[test]
    fn assemble_trims_oversized_pool_to_target() {
        let pool_addrs: Vec<Address> = (1u8..=5).map(Address::with_last_byte).collect();
        let pool = pool_addrs
            .iter()
            .map(|address| ConsensusRegistry::ValidatorInfo {
                validatorAddress: *address,
                activationEpoch: 0,
                exitEpoch: 0,
                currentStatus: ValidatorStatus::Active,
                isRetired: false,
                stakeVersion: 0,
                region: 0,
            })
            .collect();
        let mut rng = StdRng::from_seed([7u8; 32]);

        let committee = assemble_new_committee(3, pool, &mut rng).ok();

        assert!(committee
            .is_some_and(|c| c.len() == 3 && c.iter().all(|address| pool_addrs.contains(address))));
    }

    /// A pool with too few active validators fills the committee from pending-exit validators and
    /// still reaches the exact target size (the folding branch of the assembly).
    #[test]
    fn assemble_fills_from_pending_exit_when_active_is_short() {
        let pool = vec![
            validator(1, ValidatorStatus::Active),
            validator(2, ValidatorStatus::PendingExit),
            validator(3, ValidatorStatus::PendingExit),
        ];
        let mut rng = StdRng::from_seed([7u8; 32]);

        let result = assemble_new_committee(3, pool, &mut rng);

        assert!(matches!(&result, Ok(committee) if committee.len() == 3));
    }

    /// At the exact-size boundary (active validator count == target) the committee is drawn only
    /// from the active set, with pending-exit validators ignored, and its deterministic order is
    /// pinned to a golden value. This locks the RNG draw sequence: any change to the shuffle path,
    /// or a `>=`-to-`>` slip in the active-vs-target comparison (which would perturb the draws via
    /// `choose_multiple(rng, 0)`), reorders the committee and fails here rather than silently
    /// diverging into a different same-length committee that the on-chain length guard cannot
    /// catch.
    #[test]
    fn assemble_at_exact_size_boundary_pins_deterministic_active_only_committee() {
        let pool = vec![
            validator(1, ValidatorStatus::Active),
            validator(2, ValidatorStatus::Active),
            validator(3, ValidatorStatus::Active),
            validator(4, ValidatorStatus::PendingExit),
            validator(5, ValidatorStatus::PendingExit),
        ];
        let mut rng = StdRng::from_seed([42u8; 32]);

        let committee = assemble_new_committee(3, pool, &mut rng).ok();

        // Golden order for seed [42; 32]; regenerate only when the shuffle algorithm changes on
        // purpose. All three addresses are from the active set, proving pending-exit is ignored.
        assert_eq!(
            committee,
            Some(vec![
                Address::with_last_byte(2),
                Address::with_last_byte(1),
                Address::with_last_byte(3),
            ])
        );
    }

    /// The `ConsensusRegistry` fork must fail closed over an unexpected pre-fork deployment.
    ///
    /// The swap + `migrateValidatorSets()` assume the exact storage layout of the pinned
    /// pre-fork registry code. Here the genesis fixture's registry account is overwritten with
    /// the post-fork artifact bytes (any hash other than
    /// `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` — a stand-in for an unknown deployment), and the
    /// fork-boundary block must abort with the fail-closed gate error instead of silently
    /// migrating over an unverified layout. (Without the gate this block would execute: the
    /// migration is idempotent on the new code, making this test the discriminating check.)
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_fails_closed_on_unexpected_code() -> eyre::Result<()> {
        // overwrite the registry's code (keeping balance + storage) with the post-fork artifact
        let mut genesis = tn_types::test_genesis();
        let v2_value = RethEnv::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let v2_code: Bytes =
            alloy::hex::decode(v2_value.as_str().expect("deployedBytecode.object is a string"))?
                .into();
        genesis
            .alloc
            .get_mut(&CONSENSUS_REGISTRY_ADDRESS)
            .expect("testnet genesis must allocate the ConsensusRegistry account")
            .code = Some(v2_code);

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let genesis_header = chain.sealed_genesis_header();

        // drive the fork boundary: the concluding epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp = TempDir::new().unwrap();
        let tm = TaskManager::new("fail closed test");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None).unwrap();
        let err = execute_payload_and_update_canonical_chain(&env, payload, vec![])
            .expect_err("fork over an unexpected registry deployment must abort the block");
        assert!(
            format!("{err:#}").contains("failing closed"),
            "abort must come from the fail-closed code-hash gate, got: {err:#}"
        );

        Ok(())
    }

    /// The `WorkerConfigs` fork must fail closed over an unexpected pre-fork deployment.
    ///
    /// Mirrors the registry's fail-closed test above: the genesis fixture's worker-configs
    /// account is overwritten with the post-fork artifact bytes (any hash other than
    /// `WORKER_CONFIGS_PRE_FORK_CODE_HASH` — a stand-in for an unknown deployment) while the
    /// registry account keeps its pinned pre-fork code, so the registry swap that leads the
    /// fork boundary succeeds and the abort is attributable to the WorkerConfigs gate alone.
    /// (Without the gate this block would execute: the code-only swap is a no-op over the
    /// current artifact, making this test the discriminating check.)
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_worker_configs_fork_fails_closed_on_unexpected_code() -> eyre::Result<()> {
        // overwrite the worker-configs code (keeping balance + storage) with the post-fork
        // artifact
        let mut genesis = tn_types::test_genesis();
        let v2_value = RethEnv::fetch_value_from_json_str(
            WORKER_CONFIGS_JSON,
            Some("deployedBytecode.object"),
        )?;
        let v2_code: Bytes =
            alloy::hex::decode(v2_value.as_str().expect("deployedBytecode.object is a string"))?
                .into();
        genesis
            .alloc
            .get_mut(&WORKER_CONFIGS_ADDRESS)
            .expect("testnet genesis must allocate the WorkerConfigs account")
            .code = Some(v2_code);

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let genesis_header = chain.sealed_genesis_header();

        // drive the fork boundary: the concluding epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp = TempDir::new().unwrap();
        let tm = TaskManager::new("worker configs fail closed test");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None).unwrap();
        let err = execute_payload_and_update_canonical_chain(&env, payload, vec![])
            .expect_err("fork over an unexpected worker-configs deployment must abort the block");
        assert!(
            format!("{err:#}").contains("worker configs fork failing closed"),
            "abort must come from the WorkerConfigs fail-closed code-hash gate, got: {err:#}"
        );

        Ok(())
    }

    /// Guards the committee backfill path in `block.rs::shuffle_new_committee`: when there are
    /// fewer strictly-active validators than the committee size, the shuffle must backfill from
    /// the `PendingExit` pool so the next committee still reaches the required size. Five
    /// genesis validators form a committee of 5; two begin exiting, leaving 3 active + 2
    /// pending-exit. Since `committeeSize (5) > active (3)`, every subsequent committee must
    /// include the two exiting validators - otherwise `concludeEpoch` would revert on its
    /// committee-size check.
    #[tokio::test]
    async fn test_committee_backfill_from_pending_exit() -> eyre::Result<()> {
        // the two validators that begin exiting need EOAs to sign their `beginExit` txns
        let mut exit_a_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(101));
        let exit_a = exit_a_eoa.address();
        let mut exit_b_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(102));
        let exit_b = exit_b_eoa.address();

        let all_validators = [
            Address::from_slice(&[0x11; 20]),
            Address::from_slice(&[0x33; 20]),
            Address::from_slice(&[0x44; 20]),
            exit_a,
            exit_b,
        ];
        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls.public(),
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let epoch_duration = 60 * 60 * 24;
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
            ),
            (exit_a, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
            (exit_b, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
        ]);

        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Backfill Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: genesis committee is the full set of 5 validators
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // two validators begin exiting (Active -> PendingExit)
        let begin_exit_a = exit_a_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );
        let begin_exit_b = exit_b_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );

        // execute the exits in the first block (no epoch close yet)
        let mut expected_epoch = 0u32;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![begin_exit_a, begin_exit_b],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // close several epochs so the post-exit committees (computed 2 epochs ahead by the shuffle)
        // become current. If the backfill is broken, `concludeEpoch` reverts on the size check.
        for round in 2..=6u64 {
            expected_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, expected_epoch, round, true);
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            // the committee must stay full at every close: active(3) < committeeSize(5) forces the
            // shuffle to backfill from the pending-exit pool each epoch
            let EpochState { validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(committee.len(), 5, "committee stays full via pending-exit backfill");
        }

        // with active(3) < committeeSize(5), the backfill must keep every committee full and
        // include the two pending-exit validators
        let EpochState { validators: committee, .. } = reth_env.epoch_state_from_canonical_tip()?;
        let committee_addrs: Vec<Address> = committee.iter().map(|v| v.validatorAddress).collect();
        assert_eq!(committee_addrs.len(), 5, "committee stays full via pending-exit backfill");
        assert!(committee_addrs.contains(&exit_a), "pending-exit validator A backfilled");
        assert!(committee_addrs.contains(&exit_b), "pending-exit validator B backfilled");

        // the backfilled validators remain PendingExit (still serving, not yet exited)
        for exiting in [exit_a, exit_b] {
            let info = reth_env.get_validator_info(canonical_header.hash(), exiting)?;
            assert_eq!(
                info.currentStatus,
                ConsensusRegistry::ValidatorStatus::PendingExit,
                "backfilled validator stays PendingExit"
            );
        }

        Ok(())
    }
}
