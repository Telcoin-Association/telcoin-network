//! Custom handler to override EVM basefees and implement gas limit penalty.
//!
//! Source code in revm.
//!
//! [`TNEvmHandler`] overrides two post-execution hooks of revm's default handler:
//!
//! - `reward_beneficiary`: the base-fee portion of gas is CREDITED, not burned. The priority fee
//!   goes to the block beneficiary as on Ethereum, while `basefee * gas_used` is added to the
//!   chain's basefee address (`crate::basefee_address`, the governance safe unless set per-chain)
//!   for later off-chain processing.
//! - `reimburse_caller`: unused gas is refunded minus a quadratic penalty when a transaction uses
//!   less than 10% of its gas limit (`calculate_gas_penalty`, issue #424). Batch gas cannot be
//!   validated until after consensus, so wildly over-estimated limits could otherwise DoS batch
//!   proposals for free; the penalty, priced at the effective gas price, is credited to the same
//!   basefee address.
//!
//! Invariant: both hooks return early when the transaction caller is `SYSTEM_ADDRESS`, so
//! system calls (which run with zero gas price and zero base fee) never touch the beneficiary
//! or basefee accounts — otherwise every system call would spuriously mark those accounts
//! touched in the state diff.

use super::utils::gas_penalty_and_refund;
use crate::{basefee_address, SYSTEM_ADDRESS};
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

        // split unused gas into a penalty for an inefficient gas limit and the caller refund
        //
        // this is necessary to disincentivize DOS of batch proposals: due to the nature of TN
        // consensus, actual gas cannot be determined until after consensus, so the penalty
        // economically disincentivizes users from setting >10x estimated gas limits
        //
        // NOTE: uses pre-refund gas (gas_spent) so SSTORE refunds don't inflate the penalty
        //
        // the EIP-7702 authorization intrinsic is excluded from the penalty basis: each tuple is
        // charged a flat 25,000 gas from the tuple count alone (before any validity check), so a
        // padded authorization list would otherwise let a sender buy gas.spent() past the 10%
        // threshold and dodge the penalty without doing real work. the penalty is then capped at
        // unused gas so the credit to the basefee address can never exceed what the sender
        // prepaid (conservation: refund + penalty + gas_used == gas_limit)
        //
        // see https://github.com/Telcoin-Association/telcoin-network/issues/424
        let (penalty_gas, refund_amount) = gas_penalty_and_refund(
            gas_limit,
            gas_spent,
            gas_used,
            context.tx().authorization_list_len(),
        );

        debug!(target: "engine", ?penalty_gas, ?refund_amount, "governance collects: {}", effective_gas_price.saturating_mul(u128::from(penalty_gas)));

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
            .incr_balance(U256::from(coinbase_gas_price.saturating_mul(gas_used)));

        // send the base fee portion to a basefee account for later processing
        // (offchain).
        debug!(target: "engine", ?basefee, ?gas_used, "allocating basefees {}", basefee.saturating_mul(gas_used));
        context
            .journal_mut()
            .load_account_mut(self.basefee_address)?
            .incr_balance(U256::from(basefee.saturating_mul(gas_used)));

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

#[cfg(test)]
mod tests {
    //! Tests for [`TNEvmHandler::reward_beneficiary`] fee crediting.
    //!
    //! The saturation boundary test is the mutation guard for the
    //! `saturating_mul` calls: with the multiplication reverted to a raw `*`
    //! it fails (overflow-checks panic in the test profile, a wrapped and
    //! therefore wrong credit in a release build).

    use super::*;
    use crate::evm::precompile_test_utils::{TestEnv, USER};
    use reth_revm::{
        context::{BlockEnv, ContextSetters, TxEnv},
        interpreter::{interpreter_action::CallOutcome, Gas, InstructionResult},
        primitives::address,
    };
    use tn_types::{Bytes, TxKind};

    /// Block beneficiary credited with the priority-fee portion of gas payments.
    const BENEFICIARY: Address = address!("3333333000000000000000000000000000000003");

    /// Governance collector credited with the basefee portion of gas payments.
    const BASEFEE_ADDR: Address = address!("4444444000000000000000000000000000000004");

    /// A second, equally valid collector. Used only to show that the configured address is what
    /// decides which account the base fee lands in.
    const ALT_BASEFEE_ADDR: Address = address!("5555555000000000000000000000000000000005");

    /// Basefee (in wei per gas) used by both reward tests.
    const BASEFEE: u64 = 7;

    /// Build a post-execution [`FrameResult`] with `limit` gas fully spent and
    /// no refund, so `spent_sub_refunded()` equals `limit`.
    fn spent_frame(limit: u64) -> FrameResult {
        FrameResult::Call(CallOutcome::new(
            InterpreterResult {
                result: InstructionResult::Return,
                output: Bytes::default(),
                gas: Gas::new_spent(limit),
            },
            0..0,
        ))
    }

    /// Run [`TNEvmHandler::reward_beneficiary`] against a fresh [`TestEnv`]
    /// for a legacy transaction with the given gas price, spending `gas_limit`
    /// in full and crediting base fees to `collector`. Return the balances of
    /// the beneficiary and of both candidate collectors, so a caller can see
    /// which account the configured address selected.
    fn reward_crediting(collector: Address, gas_price: u128, gas_limit: u64) -> (U256, U256, U256) {
        let mut env = TestEnv::new();
        env.evm.ctx.set_block(BlockEnv {
            beneficiary: BENEFICIARY,
            basefee: BASEFEE,
            ..Default::default()
        });
        env.evm.ctx.set_tx(
            TxEnv::builder()
                .caller(USER)
                .kind(TxKind::Call(BENEFICIARY))
                .gas_limit(gas_limit)
                .gas_price(gas_price)
                .build()
                .expect("valid test tx env"),
        );
        let mut frame = spent_frame(gas_limit);
        TNEvmHandler::new(collector)
            .reward_beneficiary(&mut env.evm, &mut frame)
            .expect("reward_beneficiary succeeds");
        (
            env.get_balance(BENEFICIARY),
            env.get_balance(BASEFEE_ADDR),
            env.get_balance(ALT_BASEFEE_ADDR),
        )
    }

    /// Credit base fees to [`BASEFEE_ADDR`] and return the (beneficiary, basefee) balances.
    fn reward_with(gas_price: u128, gas_limit: u64) -> (U256, U256) {
        let (beneficiary, basefee_collector, _alt) =
            reward_crediting(BASEFEE_ADDR, gas_price, gas_limit);
        (beneficiary, basefee_collector)
    }

    /// In-range fees split exactly: the beneficiary receives
    /// `(effective_gas_price - basefee) * gas_used` and the basefee address
    /// receives `basefee * gas_used`, unchanged by the saturating arithmetic.
    #[test]
    fn reward_beneficiary_splits_priority_and_base_fee_in_range() {
        let gas_used = 21_000u64;
        let (beneficiary, basefee_collector) = reward_with(10, gas_used);
        assert_eq!(beneficiary, U256::from((10 - u128::from(BASEFEE)) * u128::from(gas_used)));
        assert_eq!(basefee_collector, U256::from(u128::from(BASEFEE) * u128::from(gas_used)));
    }

    /// At the overflow boundary the beneficiary credit saturates to
    /// `u128::MAX` instead of wrapping (release) or panicking (dev/test):
    /// `coinbase_gas_price` is `u128::MAX - basefee`, so two units of spent
    /// gas push the raw product past `u128::MAX`.
    #[test]
    fn reward_beneficiary_saturates_on_fee_product_overflow() {
        let (beneficiary, basefee_collector) = reward_with(u128::MAX, 2);
        assert_eq!(beneficiary, U256::from(u128::MAX));
        assert_eq!(basefee_collector, U256::from(u128::from(BASEFEE) * 2));
    }

    /// Two runs that agree on the block, the transaction, and the gas, and differ only in the
    /// configured base-fee address, credit two different accounts. The credited balances are part
    /// of the state, so the two runs produce two different state roots.
    ///
    /// This pins the claim the `BASEFEE_ADDRESS` documentation in `lib.rs` makes, and it is the
    /// reason `tn-config` declares [`basefee_address`](tn_config::Parameters) as a required key
    /// with no serde default: two honest nodes that disagree on this one value still agree on the
    /// batch, the certificate, and the commit order, and disagree only on the resulting state.
    /// Without this test a later change could treat the field as cosmetic and nothing would fail.
    #[test]
    fn differing_basefee_addresses_credit_different_accounts() {
        let gas_used = 21_000u64;
        let credited = U256::from(u128::from(BASEFEE) * u128::from(gas_used));

        let (beneficiary_a, primary_a, alt_a) = reward_crediting(BASEFEE_ADDR, 10, gas_used);
        let (beneficiary_b, primary_b, alt_b) = reward_crediting(ALT_BASEFEE_ADDR, 10, gas_used);

        // the beneficiary is credited identically, so the collector address is the only difference
        assert_eq!(beneficiary_a, beneficiary_b, "only the basefee address differs between runs");

        assert_eq!((primary_a, alt_a), (credited, U256::ZERO), "run A must credit BASEFEE_ADDR");
        assert_eq!(
            (primary_b, alt_b),
            (U256::ZERO, credited),
            "run B must credit ALT_BASEFEE_ADDR"
        );
        assert_ne!(
            (primary_a, alt_a),
            (primary_b, alt_b),
            "a different basefee address must produce a different account state, and therefore a \
             different state root"
        );
    }
}
