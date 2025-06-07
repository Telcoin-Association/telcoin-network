//! The types that build blocks for EVM execution.

use crate::{
    error::{TnRethError, TnRethResult},
    system_calls::{
        ConsensusRegistry::{self, ValidatorStatus},
        CONSENSUS_REGISTRY_ADDRESS,
    },
    SYSTEM_ADDRESS,
};
use alloy::{
    consensus::{proofs, Block, BlockBody, Transaction, TxReceipt},
    eips::eip7685::Requests,
    sol_types::SolCall as _,
};
use alloy_evm::{Database, Evm};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutor, BlockExecutorFactory, CommitChanges, ExecutableTx,
        InternalBlockExecutionError,
    },
    eth::receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    execute::{BlockAssembler, BlockAssemblerInput},
    FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::{logs_bloom, TxType};
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
    DatabaseCommit as _, State,
};
use std::sync::Arc;
use tn_types::{
    Address, Bytes, Encodable2718, ExecHeader, Receipt, TransactionSigned, Withdrawals, B256,
    EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS,
};
use tracing::{debug, error};

/// Context for TN block execution.
#[derive(Debug, Clone)]
pub struct TNBlockExecutionCtx {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Option<B256>,
    /// The index for the batch.
    pub nonce: u64,
    /// The batch digest.
    ///
    /// This is the batch that was validated by consensus and is responsible for the
    /// request for execution.
    pub requests_hash: Option<B256>,
    /// Keccak hash of the bls signature for the leader certificate.
    ///
    /// Executor makes closing epoch system call when this if included.
    /// The hash is stored in the `extra_data` field so clients know when the
    /// closing epoch call was made.
    pub close_epoch: Option<B256>,
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

impl<'db, Evm, Spec, R, DB> TNBlockExecutor<Evm, Spec, R>
where
    DB: Database + 'db,
    DB::Error: core::fmt::Display,
    Evm: alloy_evm::Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthereumHardforks,
    // R: ReceiptBuilder,
    // R: ReceiptBuilder<Transaction: Transaction + Encodable2718, Receipt: TxReceipt<Log = Log>>,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    /// Creates a new [`TNBlockExecutor`]
    pub(crate) fn new(evm: Evm, ctx: TNBlockExecutionCtx, spec: Spec, receipt_builder: R) -> Self {
        Self { evm, ctx, receipts: Vec::new(), gas_used: 0, spec, receipt_builder }
    }

    /// Increase the beneficiary account balance and withdraw from governance safe.
    ///
    /// see revm-database/src/states/state.rs
    /// State::increment_balances
    fn _apply_consensus_block_reward(&mut self) -> TnRethResult<()> {
        let recipient = self.evm.block().beneficiary;
        let _original_account = self.evm.db_mut().load_cache_account(recipient).map_err(|e| {
            TnRethError::EVMCustom(format!("failed to load account for block reward: {e}"))
        })?;

        // TODO: this should be applyIncentives before closeEpoch
        // see Issue #325
        //
        // if let Some(s) = self.evm.db_mut().transition_state.as_mut() {
        //     s.add_transitions(vec![(
        //         recipient,
        //         original_account.increment_balance(balance).expect("balance is not 0"),
        //     )]);
        // }

        Ok(())
    }

    /// Apply the closing epoch call to ConsensusRegistry.
    fn apply_closing_epoch_contract_call(&mut self, randomness: B256) -> TnRethResult<()> {
        // let prev_env = Box::new(evm.context.env().clone());
        let calldata = self.generate_conclude_epoch_calldata(randomness)?;

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

        // capture closing epoch log
        debug!(target: "engine", "closing epoch logs:\n{:#?}", res.result.logs());
        let closing_epoch_logs = res.result.clone().into_logs();

        debug!(target: "engine", "committing closing epoch state:\n{:#?}", res.state);

        // commit the changes
        self.evm.db_mut().commit(res.state);

        // append receipt
        self.receipts.push(Receipt {
            logs: closing_epoch_logs,
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: 0,
        });

        Ok(())
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata(&mut self, randomness: B256) -> TnRethResult<Bytes> {
        // shuffle all validators for new committee
        let new_committee = self.shuffle_new_committee(randomness)?;

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    fn shuffle_new_committee(&mut self, randomness: B256) -> TnRethResult<Vec<Address>> {
        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let read_state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        debug!(target: "engine", "result after shuffle:\n{:#?}", read_state);

        // retrieve data from execution result
        let data = match read_state.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                // fatal error
                error!(target: "engine", "error reading state on chain: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("error reading state on chain: {e:?}")));
            }
        };

        // Use SolValue to decode the result
        let mut eligible_validators: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&data)?;

        debug!(target: "engine",  "validators pre-shuffle {:#?}", eligible_validators);

        // simple Fisher-Yates shuffle
        //
        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        debug!(target: "engine", ?seed, "seed after");

        let mut rng = StdRng::from_seed(seed);
        for i in (1..eligible_validators.len()).rev() {
            let j = rng.random_range(0..=i);
            eligible_validators.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:#?}", eligible_validators);

        let new_committee = eligible_validators.into_iter().map(|v| v.validatorAddress).collect();

        Ok(new_committee)
    }

    /// Read state on-chain.
    fn read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<ResultAndState<<Evm as alloy_evm::Evm>::HaltReason>> {
        // read from state
        let res = match self.evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state on chain: {}", e);
                return Err(TnRethError::EVMCustom(format!("failed to read state on chain: {e}")));
            }
        };

        Ok(res)
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
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number);
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        // // TODO: requires prague fork
        // self.system_caller.apply_blockhashes_contract_call(self.ctx.parent_hash, &mut self.evm)?;
        // // TODO: requires cancun
        // self.system_caller
        //     .apply_beacon_root_contract_call(self.ctx.parent_beacon_block_root, &mut self.evm)?;

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
            .transact(tx)
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
        if let Some(randomness) = self.ctx.close_epoch {
            self.apply_closing_epoch_contract_call(randomness).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;
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

        let timestamp = evm_env.block_env.timestamp;
        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = Receipt::calculate_receipt_root_no_memo(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        let withdrawals = Some(Withdrawals::default());
        let withdrawals_root = Some(EMPTY_WITHDRAWALS);

        // cancun isn't active
        let excess_blob_gas = None;
        let blob_gas_used = None;

        // TN-specific values
        let requests_hash = ctx.requests_hash; // prague inactive
        let nonce = ctx.nonce.into(); // subdag leader's nonce: ((epoch as u64) << 32) | self.round as u64
        let difficulty = evm_env.block_env.difficulty; // batch index

        // use keccak256(bls_sig) if closing epoch or Bytes::default
        let extra_data = ctx.close_epoch.map(|hash| hash.to_vec().into()).unwrap_or_default();

        let header = ExecHeader {
            parent_hash: ctx.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
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
            number: evm_env.block_env.number,
            gas_limit: evm_env.block_env.gas_limit,
            difficulty,
            gas_used: *gas_used,
            extra_data,
            parent_beacon_block_root: ctx.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,
            requests_hash,
        };

        Ok(Block {
            header,
            body: BlockBody { transactions, ommers: Default::default(), withdrawals },
        })
    }
}
