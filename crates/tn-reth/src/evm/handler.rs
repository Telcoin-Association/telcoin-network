//! Custom handler to override EVM basefees and implement gas limit penalty.
//!
//! Source code in revm.

use crate::{basefee_address, calculate_gas_penalty, SYSTEM_ADDRESS};
use reth_revm::{
    context::result::{EVMError, InvalidTransaction},
    context_interface::{
        journaled_state::account::JournaledAccountTr, result::HaltReason, Block, ContextTr,
        JournalTr, Transaction,
    },
    handler::{
        instructions::InstructionProvider, EvmTr, FrameResult, FrameTr, Handler, PrecompileProvider,
    },
    inspector::{InspectorEvmTr, InspectorHandler},
    interpreter::{interpreter::EthInterpreter, interpreter_action::FrameInit, InterpreterResult},
    primitives::U256,
    state::EvmState,
    Database, Inspector,
};
use tn_types::Address;
use tracing::debug;

/// The handler that executes TN evm types.
///
/// This handler overwrites basefee logic and implements a quadratic penalty
/// for users who set gas limits significantly higher than their actual usage.
pub(super) struct TNEvmHandler<EVM> {
    /// Address for basefees
    basefee_address: Address,
    _phantom: core::marker::PhantomData<EVM>,
}

impl<EVM> TNEvmHandler<EVM> {
    fn new(basefee_address: Address) -> Self {
        Self { basefee_address, _phantom: core::marker::PhantomData }
    }
}

impl<EVM> Default for TNEvmHandler<EVM> {
    fn default() -> Self {
        TNEvmHandler::new(basefee_address())
    }
}

impl<EVM> Handler for TNEvmHandler<EVM>
where
    EVM: EvmTr<
        Context: ContextTr<Journal: JournalTr<State = EvmState>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
        Frame: FrameTr<FrameResult = FrameResult, FrameInit = FrameInit>,
    >,
{
    type Evm = EVM;
    type Error = EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>;
    type HaltReason = HaltReason;

    /// Reimburse the caller with unused gas, minus a quadratic penalty for
    /// over-estimating the gas limit.
    ///
    /// # Gas accounting
    ///
    /// The penalty is computed from **pre-refund** gas (`gas.spent()`), not
    /// post-refund gas (`gas.spent_sub_refunded()`). Using post-refund gas
    /// would make the penalty *larger* than intended whenever an EVM refund
    /// occurs (e.g. SSTORE clearing), because the denominator shrinks while
    /// unused gas stays the same.
    ///
    /// Standard EVM accounting (`unused_gas`, `reward_beneficiary`) continues
    /// to use post-refund gas so that callers receive the normal SSTORE
    /// refund.
    ///
    /// The penalty amount is transferred to the basefee address (governance).
    fn reimburse_caller(
        &self,
        evm: &mut Self::Evm,
        exec_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        let context = evm.ctx();
        // ignore system calls
        if context.tx().caller() == SYSTEM_ADDRESS {
            return Ok(());
        }

        let gas = exec_result.gas();
        let gas_limit = context.tx().gas_limit();
        let gas_spent = gas.spent();
        let gas_used = gas.spent_sub_refunded();
        let basefee = context.block().basefee() as u128;
        let effective_gas_price = context.tx().effective_gas_price(basefee);

        // calculate penalty for inefficient gas limit
        //
        // this is necessary to disincentivize DOS of batch proposals
        //
        // due to the nature of TN consensus, actual gas cannot be determined
        // until after consensus
        //
        // this penalty economically disincentivizes users from setting
        // >10x estimated gas limits
        //
        // NOTE: uses pre-refund gas (gas_spent) so SSTORE refunds don't inflate the penalty
        //
        // see https://github.com/Telcoin-Association/telcoin-network/issues/424
        let penalty_gas = calculate_gas_penalty(gas_limit, gas_spent);

        // calculate the actual refund amount (unused gas minus penalty)
        let unused_gas = gas_limit.saturating_sub(gas_used);
        let refund_amount = unused_gas.saturating_sub(penalty_gas);

        debug!(target: "engine", ?unused_gas, ?penalty_gas, ?refund_amount, "governance collects: {}", penalty_gas as u128 * effective_gas_price);

        // return gas to caller (minus penalty)
        if refund_amount > 0 {
            let caller = context.tx().caller();
            let refund = effective_gas_price.saturating_mul(refund_amount as u128);
            context.journal_mut().load_account_mut(caller)?.incr_balance(U256::from(refund));
        }

        // transfer penalty to basefee address
        if penalty_gas > 0 {
            let penalty = effective_gas_price.saturating_mul(penalty_gas as u128);
            context
                .journal_mut()
                .load_account_mut(self.basefee_address)?
                .incr_balance(U256::from(penalty));
        }

        Ok(())
    }

    // Override the default basefee logic
    fn reward_beneficiary(
        &self,
        evm: &mut Self::Evm,
        exec_result: &mut FrameResult,
    ) -> Result<(), Self::Error> {
        let context = evm.ctx();
        // skip for system calls — gas_price and basefee are both 0, so all amounts are 0.
        // this prevents the beneficiary and basefee_address from being spuriously touched.
        if context.tx().caller() == SYSTEM_ADDRESS {
            return Ok(());
        }
        let beneficiary = context.block().beneficiary();
        let basefee = context.block().basefee() as u128;
        let effective_gas_price = context.tx().effective_gas_price(basefee);
        let gas = exec_result.gas();
        let gas_used = gas.spent_sub_refunded() as u128;

        // transfer priority fee to coinbase/beneficiary
        // basefee amount of gas is redirected to governance multisig
        let coinbase_gas_price = effective_gas_price.saturating_sub(basefee);
        context
            .journal_mut()
            .load_account_mut(beneficiary)?
            .incr_balance(U256::from(coinbase_gas_price * gas_used));

        // send the base fee portion to a basefee account for later processing
        // (offchain).
        debug!(target: "engine", ?basefee, ?gas_used, "allocating basefees {}", basefee * gas_used);
        context
            .journal_mut()
            .load_account_mut(self.basefee_address)?
            .incr_balance(U256::from(basefee * gas_used));

        Ok(())
    }
}

impl<EVM> InspectorHandler for TNEvmHandler<EVM>
where
    EVM: InspectorEvmTr<
        Inspector: Inspector<<<Self as Handler>::Evm as EvmTr>::Context, EthInterpreter>,
        Context: ContextTr<Journal: JournalTr<State = EvmState>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
    >,
{
    type IT = EthInterpreter;
}
