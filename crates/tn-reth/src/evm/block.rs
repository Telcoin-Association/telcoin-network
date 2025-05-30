//! The types that build blocks for EVM execution.

use alloy::{consensus::TxReceipt, eips::eip7685::Requests};
use alloy_evm::{Database, Evm};
use reth_chainspec::EthereumHardfork;
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutor, BlockExecutorFactory, BlockExecutorFor, ExecutableTx,
        StateChangePostBlockSource, StateChangeSource, SystemCaller,
    },
    eth::{
        receipt_builder::{AlloyReceiptBuilder, ReceiptBuilder, ReceiptBuilderCtx},
        spec::{EthExecutorSpec, EthSpec},
    },
    state_change::balance_increment_state,
    EthEvmFactory, EvmFactory, FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::Log;
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::{
        result::{ExecutionResult, ResultAndState},
        Transaction,
    },
    DatabaseCommit as _, Inspector, State,
};
use std::borrow::Cow;
use tn_types::{Encodable2718, ExecHeader, Withdrawals, B256};

/// Context for TN block execution.
#[derive(Debug, Clone)]
pub struct TNBlockExecutionCtx<'a> {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Option<B256>,
    // /// Block ommers
    // pub ommers: &'a [ExecHeader],
    /// Block withdrawals.
    pub withdrawals: Option<Cow<'a, Withdrawals>>,
}

/// Block executor for Ethereum.
#[derive(Debug)]
pub struct TNBlockExecutor<'a, Evm, Spec, R: ReceiptBuilder> {
    /// Reference to the specification object.
    spec: Spec,
    /// Context for block execution.
    pub ctx: TNBlockExecutionCtx<'a>,
    /// Inner EVM.
    evm: Evm,
    /// Utility to call system smart contracts.
    system_caller: SystemCaller<Spec>,
    /// Receipt builder.
    receipt_builder: R,

    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by transactions in this block.
    gas_used: u64,
}

impl<'a, Evm, Spec, R> TNBlockExecutor<'a, Evm, Spec, R>
where
    Spec: Clone,
    R: ReceiptBuilder,
{
    /// Creates a new [`TNBlockExecutor`]
    pub fn new(evm: Evm, ctx: TNBlockExecutionCtx<'a>, spec: Spec, receipt_builder: R) -> Self {
        Self {
            evm,
            ctx,
            receipts: Vec::new(),
            gas_used: 0,
            system_caller: SystemCaller::new(spec.clone()),
            spec,
            receipt_builder,
        }
    }
}

impl<'db, DB, E, Spec, R> BlockExecutor for TNBlockExecutor<'_, E, Spec, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthExecutorSpec,
    R: ReceiptBuilder<Transaction: Transaction + Encodable2718, Receipt: TxReceipt<Log = Log>>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number);
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        // TODO: requires prague fork
        self.system_caller.apply_blockhashes_contract_call(self.ctx.parent_hash, &mut self.evm)?;
        // TODO: requires cancun
        self.system_caller
            .apply_beacon_root_contract_call(self.ctx.parent_beacon_block_root, &mut self.evm)?;

        Ok(())
    }

    fn execute_transaction_with_result_closure(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>),
    ) -> Result<u64, BlockExecutionError> {
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
        let result_and_state = self
            .evm
            .transact(tx)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;
        self.system_caller
            .on_state(StateChangeSource::Transaction(self.receipts.len()), &result_and_state.state);
        let ResultAndState { result, state } = result_and_state;

        f(&result);

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

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // let requests = if self.spec.is_prague_active_at_timestamp(self.evm.block().timestamp) {
        //     // Collect all EIP-6110 deposits
        //     let deposit_requests =
        //         eip6110::parse_deposits_from_receipts(&self.spec, &self.receipts)?;

        //     let mut requests = Requests::default();

        //     if !deposit_requests.is_empty() {
        //         requests.push_request_with_type(eip6110::DEPOSIT_REQUEST_TYPE, deposit_requests);
        //     }

        //     requests.extend(self.system_caller.apply_post_execution_changes(&mut self.evm)?);
        //     requests
        // } else {
        //     Requests::default()
        // };

        // don't support prague deposit requests
        let requests = Requests::default();

        // don't support block rewards for ommers
        //
        // let mut balance_increments = post_block_balance_increments(
        //     &self.spec,
        //     self.evm.block(),
        //     self.ctx.ommers,
        //     self.ctx.withdrawals.as_deref(),
        // );

        // don't support eth dao hardfork
        //
        // // irregular state change at Ethereum DAO hardfork
        // if self
        //     .spec
        //     .ethereum_fork_activation(EthereumHardfork::Dao)
        //     .transitions_at_block(self.evm.block().number)
        // {
        //     // drain balances from hardcoded addresses.
        //     let drained_balance: u128 = self
        //         .evm
        //         .db_mut()
        //         .drain_balances(dao_fork::DAO_HARDFORK_ACCOUNTS)
        //         .map_err(|_| BlockValidationError::IncrementBalanceFailed)?
        //         .into_iter()
        //         .sum();

        //     // return balance to DAO beneficiary.
        //     *balance_increments.entry(dao_fork::DAO_HARDFORK_BENEFICIARY).or_default() +=
        //         drained_balance;
        // }

        let balance_increments = alloy::primitives::map::foldhash::HashMap::default();

        // TODO: apply rewards for beneficiary
        //
        // increment balances for consensus leader on first batch of consensus output
        self.evm
            .db_mut()
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;

        // call state hook with changes due to balance increments.
        self.system_caller.try_on_state_with(|| {
            balance_increment_state(&balance_increments, self.evm.db_mut()).map(|state| {
                (
                    StateChangeSource::PostBlock(StateChangePostBlockSource::BalanceIncrements),
                    Cow::Owned(state),
                )
            })
        })?;

        Ok((
            self.evm,
            BlockExecutionResult { receipts: self.receipts, requests, gas_used: self.gas_used },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.system_caller.with_state_hook(hook);
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }
}

/// Ethereum block executor factory.
#[derive(Debug, Clone, Default, Copy)]
pub struct EthBlockExecutorFactory<
    R = AlloyReceiptBuilder,
    Spec = EthSpec,
    EvmFactory = EthEvmFactory,
> {
    /// Receipt builder.
    receipt_builder: R,
    /// Chain specification.
    spec: Spec,
    /// EVM factory.
    evm_factory: EvmFactory,
}

impl<R, Spec, EvmFactory> EthBlockExecutorFactory<R, Spec, EvmFactory> {
    /// Creates a new [`EthBlockExecutorFactory`] with the given spec, [`EvmFactory`], and
    /// [`ReceiptBuilder`].
    pub const fn new(receipt_builder: R, spec: Spec, evm_factory: EvmFactory) -> Self {
        Self { receipt_builder, spec, evm_factory }
    }

    /// Exposes the receipt builder.
    pub const fn receipt_builder(&self) -> &R {
        &self.receipt_builder
    }

    /// Exposes the chain specification.
    pub const fn spec(&self) -> &Spec {
        &self.spec
    }

    /// Exposes the EVM factory.
    pub const fn evm_factory(&self) -> &EvmFactory {
        &self.evm_factory
    }
}

impl<R, Spec, EvmF> BlockExecutorFactory for EthBlockExecutorFactory<R, Spec, EvmF>
where
    R: ReceiptBuilder<Transaction: Transaction + Encodable2718, Receipt: TxReceipt<Log = Log>>,
    Spec: EthExecutorSpec,
    EvmF: EvmFactory<Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>>,
    Self: 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = TNBlockExecutionCtx<'a>;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        &self.evm_factory
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        TNBlockExecutor::new(evm, ctx, &self.spec, &self.receipt_builder)
    }
}
