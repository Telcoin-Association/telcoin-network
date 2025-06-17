//! Custom handler to override EVM basefees.

use reth_revm::{
    context::{
        result::{EVMError, InvalidTransaction},
        JournalOutput,
    },
    context_interface::{result::HaltReason, Block, ContextTr, JournalTr, Transaction},
    handler::{
        instructions::InstructionProvider, EthFrame, EvmTr, Frame, Handler, PrecompileProvider,
    },
    inspector::{InspectorEvmTr, InspectorHandler},
    interpreter::{interpreter::EthInterpreter, InterpreterResult},
    primitives::U256,
    Database, Inspector,
};
use tn_types::Address;

use crate::basefee_address;

/// The handler that executes TN evm types.
///
/// This is only intended to overwrite basefee logic for now.
pub(super) struct TNEvmHandler<EVM> {
    /// Address for basefees
    basefee_address: Option<Address>,
    _phantom: core::marker::PhantomData<EVM>,
}

impl<EVM> TNEvmHandler<EVM> {
    fn new(basefee_address: Option<Address>) -> Self {
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
        Context: ContextTr<Journal: JournalTr<FinalOutput = JournalOutput>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
    >,
{
    type Evm = EVM;
    type Error = EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>;
    type Frame = EthFrame<
        EVM,
        EVMError<<<EVM::Context as ContextTr>::Db as Database>::Error, InvalidTransaction>,
        <EVM::Instructions as InstructionProvider>::InterpreterTypes,
    >;
    type HaltReason = HaltReason;

    // overwrite the default basefee logic
    fn reward_beneficiary(
        &self,
        evm: &mut Self::Evm,
        exec_result: &mut <Self::Frame as Frame>::FrameResult,
    ) -> Result<(), Self::Error> {
        let context = evm.ctx();
        let beneficiary = context.block().beneficiary();
        let basefee = context.block().basefee() as u128;
        let effective_gas_price = context.tx().effective_gas_price(basefee);
        let gas = exec_result.gas();

        // transfer fee to coinbase/beneficiary.
        // Basefee amount of gas is redirected.
        let coinbase_gas_price = effective_gas_price.saturating_sub(basefee);
        let coinbase_account = context.journal().load_account(beneficiary)?;
        coinbase_account.data.mark_touch();

        let gas_used = (gas.spent() - gas.refunded() as u64) as u128;
        coinbase_account.data.info.balance = coinbase_account
            .data
            .info
            .balance
            .saturating_add(U256::from(coinbase_gas_price * gas_used));

        if let Some(basefee_address) = self.basefee_address {
            // Send the base fee portion to a basefee account for later processing
            // (offchain).
            let basefee_account = context.journal().load_account(basefee_address)?;
            basefee_account.data.mark_touch();
            basefee_account.data.info.balance =
                basefee_account.data.info.balance.saturating_add(U256::from(basefee * gas_used));
        }

        Ok(())
    }
}

impl<EVM> InspectorHandler for TNEvmHandler<EVM>
where
    EVM: InspectorEvmTr<
        Inspector: Inspector<<<Self as Handler>::Evm as EvmTr>::Context, EthInterpreter>,
        Context: ContextTr<Journal: JournalTr<FinalOutput = JournalOutput>>,
        Precompiles: PrecompileProvider<EVM::Context, Output = InterpreterResult>,
        Instructions: InstructionProvider<
            Context = EVM::Context,
            InterpreterTypes = EthInterpreter,
        >,
    >,
{
    type IT = EthInterpreter;
}
