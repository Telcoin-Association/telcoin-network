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
    consensus::{proofs, Block, BlockBody, Transaction, TxReceipt},
    eips::{
        eip2935::HISTORY_STORAGE_ADDRESS,
        eip4788::BEACON_ROOTS_ADDRESS,
        eip7685::{Requests, EMPTY_REQUESTS_HASH},
    },
    sol_types::SolCall as _,
};
use alloy_evm::{Database, Evm};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutor, BlockExecutorFactory, CommitChanges, ExecutableTx,
        InternalBlockExecutionError, StateChangeSource,
    },
    eth::receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    execute::{BlockAssembler, BlockAssemblerInput},
    FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::logs_bloom;
use reth_primitives_traits::proofs::calculate_withdrawals_root;
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
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
        let res = match self.evm.transact_system_call(
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
        let res = match self.evm.transact_system_call(
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

        // commit the changes
        self.evm.db_mut().commit(res.state);
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

        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        trace!(target: "engine", "get validators call:\n{:?}", state);

        let mut eligible_validators: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&state)?;

        debug!(target: "engine",  "validators pre-shuffle {:?}", eligible_validators);

        // simple Fisher-Yates shuffle
        //
        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        trace!(target: "engine", ?seed, "seed after");

        let mut rng = StdRng::from_seed(seed);
        for i in (1..eligible_validators.len()).rev() {
            let j = rng.random_range(0..=i);
            eligible_validators.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:?}", eligible_validators);

        let mut new_committee =
            eligible_validators.into_iter().map(|v| v.validatorAddress).collect::<Vec<_>>();

        // trim the shuffled committee to maintain correct size
        new_committee.truncate(new_committee_size);

        trace!(target: "engine",  ?new_committee_size, ?new_committee, "truncated shuffle for new committee");

        Ok(new_committee)
    }

    /// Read state on-chain.
    fn read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<Bytes> {
        // read from state
        let res = match self.evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state on chain: {}", e);
                return Err(TnRethError::EVMCustom(format!("failed to read state on chain: {e}")));
            }
        };

        // retrieve data from execution result
        let data = match res.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                // fatal error
                error!(target: "engine", "error reading state on chain: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("error reading state on chain: {e:?}")));
            }
        };

        Ok(data)
    }

    /// Return the next committee size.
    ///
    /// This is isolated into a function and requires a fork to change.
    fn next_committee_size(&mut self) -> TnRethResult<usize> {
        // retrieve the current committee size
        let epoch = self.extract_epoch_from_nonce(self.ctx.nonce);
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        let current_committee: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&state)?;

        trace!(target: "engine",  ?current_committee, "read current committee to get the next committee size");

        // this will fail on-chain if incorrect
        Ok(current_committee.len())
    }

    /// Extract the epoch number from a header's nonce.
    fn extract_epoch_from_nonce(&self, nonce: u64) -> u32 {
        // epochs are packed into nonce as 32 bits
        let epoch = nonce >> 32;
        epoch as u32
    }

    /// Applies the pre-block call to the EIP-4788 consensus root contract (cancun).
    fn apply_consensus_root_contract_call(&mut self) -> Result<(), BlockExecutionError> {
        if !self.spec.is_cancun_active_at_timestamp(self.evm.block().timestamp.saturating_to()) {
            return Ok(());
        }

        let parent_beacon_block_root = self
            .ctx
            .parent_beacon_block_root
            .ok_or(BlockValidationError::MissingParentBeaconBlockRoot)?;

        trace!(target: "engine", block_number=?self.evm.block().number, ?parent_beacon_block_root, "evaluating parent root");

        // if the block number is zero (genesis block) then the parent beacon block root must
        // be 0x0 and no system transaction may occur as per EIP-4788
        if self.evm.block().number == 0 {
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
        if !self.spec.is_prague_active_at_timestamp(self.evm.block().timestamp.saturating_to()) {
            return Ok(());
        }

        // if the block number is zero (genesis block) then no system transaction may occur as per
        // EIP-2935
        if self.evm.block().number == 0 {
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

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number.saturating_to());
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

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>) -> CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        // The sum of the transaction's gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block's gasLimit.
        let block_available_gas = self.evm.block().gas_limit - self.gas_used;

        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        // Execute transaction.
        let ResultAndState { result, state } = self
            .evm
            .transact(&tx)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;

        if !f(&result).should_commit() {
            return Ok(None);
        }

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx: tx.tx(),
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(Some(gas_used))
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // don't support prague deposit requests
        let requests = Requests::default();

        // potentially close epoch boundary
        if let Some(randomness) = self.ctx.close_epoch {
            debug!(target: "engine", ?randomness, "ctx indicates close epoch");
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
            BlockExecutionResult { receipts: self.receipts, requests, gas_used: self.gas_used },
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
    ) -> Result<ResultAndState<<Self::Evm as Evm>::HaltReason>, BlockExecutionError> {
        // The sum of the transaction's gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block's gasLimit.
        let block_available_gas = self.evm.block().gas_limit - self.gas_used;

        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        // Execute transaction and return the result
        self.evm.transact(&tx).map_err(|err| {
            let hash = tx.tx().trie_hash();
            BlockExecutionError::evm(err, hash)
        })
    }

    fn commit_transaction(
        &mut self,
        output: ResultAndState<<Self::Evm as Evm>::HaltReason>,
        tx: impl ExecutableTx<Self>,
    ) -> Result<u64, BlockExecutionError> {
        let ResultAndState { result, state } = output;

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx: tx.tx(),
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(gas_used)
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

        let timestamp = evm_env.block_env.timestamp.saturating_to();
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
            beneficiary: evm_env.block_env.beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao.unwrap_or_default(),
            nonce,
            base_fee_per_gas: Some(evm_env.block_env.basefee),
            number: evm_env.block_env.number.saturating_to(),
            gas_limit: evm_env.block_env.gas_limit,
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
