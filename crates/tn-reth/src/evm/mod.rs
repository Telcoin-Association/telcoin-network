//! All types associated with execution TN EVM.
//!
//! Heavily inspired by alloy_evm and revm.

use alloy_evm::Database;
use reth_evm::{precompiles::PrecompilesMap, Evm, EvmEnv};
use reth_revm::{
    context::{
        result::{EVMError, HaltReason, ResultAndState},
        BlockEnv, Evm as RevmEvm, TxEnv,
    },
    handler::{instructions::EthInstructions, PrecompileProvider},
    interpreter::{interpreter::EthInterpreter, InterpreterResult},
    primitives::hardfork::SpecId,
    Context, ExecuteEvm as _, InspectEvm as _, Inspector,
};
use std::ops::{Deref, DerefMut};
use tn_types::{Address, Bytes, TxKind, U256};
mod block;
mod config;
mod context;
mod factory;
pub(crate) use block::*;
pub(crate) use config::*;
pub(crate) use context::*;
pub(crate) use factory::*;

/// TN EVM implementation.
///
/// This is a wrapper type around the `revm` ethereum evm with optional [`Inspector`] (tracing)
/// support. [`Inspector`] support is configurable at runtime because it's part of the underlying
/// [`RevmEvm`] type.
///
/// TODO: review EthInstructions, EthInterpreter, etc. and update comment!!!
/// !!!
/// !!!!!!!1!
#[expect(missing_debug_implementations)]
pub struct TNEvm<DB: Database, I, PRECOMPILE = PrecompilesMap> {
    inner:
        RevmEvm<TNEvmContext<DB>, I, EthInstructions<EthInterpreter, TNEvmContext<DB>>, PRECOMPILE>,
    inspect: bool,
}

// MISSING METHODSAAAAAAAA
impl<DB: Database, I, PRECOMPILE> TNEvm<DB, I, PRECOMPILE> {
    /// Creates a new Ethereum EVM instance.
    ///
    /// The `inspect` argument determines whether the configured [`Inspector`] of the given
    /// [`RevmEvm`] should be invoked on [`Evm::transact`].
    pub const fn new(
        evm: RevmEvm<
            TNEvmContext<DB>,
            I,
            EthInstructions<EthInterpreter, TNEvmContext<DB>>,
            PRECOMPILE,
        >,
        inspect: bool,
    ) -> Self {
        Self { inner: evm, inspect }
    }

    /// Consumes self and return the inner EVM instance.
    pub fn into_inner(
        self,
    ) -> RevmEvm<TNEvmContext<DB>, I, EthInstructions<EthInterpreter, TNEvmContext<DB>>, PRECOMPILE>
    {
        self.inner
    }

    /// Provides a reference to the EVM context.
    pub const fn ctx(&self) -> &TNEvmContext<DB> {
        &self.inner.ctx
    }

    /// Provides a mutable reference to the EVM context.
    pub fn ctx_mut(&mut self) -> &mut TNEvmContext<DB> {
        &mut self.inner.ctx
    }

    // /// Provide a custom reward beneficiary callback to handle base fees for telcoin network.
    // fn set_base_fee_handler(&mut self) {
    //     // TODO- send the base fee to safe or contract to be managed offchain.
    //     let basefee_address: Option<Address> = None;
    //     // DO NOT use this testing default in mainnet.
    //     //    Some(Address::parse_checksummed("0x29615F9e735932580f699C494C11fB81296AfE8F", None)
    //     //    .expect("valid account"));
    //     evm.handler.post_execution.reward_beneficiary = Arc::new(move |ctx, gas| {
    //         // code lifted from revm mainnet/post_execution.rs and modified to do something with
    //         // base fee.
    //         let beneficiary = ctx.evm.env.block.coinbase;
    //         let effective_gas_price = ctx.evm.env.effective_gas_price();

    //         // transfer fee to coinbase/beneficiary.
    //         // Basefee amount of gas is redirected.
    //         let coinbase_gas_price =
    // effective_gas_price.saturating_sub(ctx.evm.env.block.basefee);

    //         let coinbase_account =
    //             ctx.evm.inner.journaled_state.load_account(beneficiary, &mut ctx.evm.inner.db)?;

    //         coinbase_account.data.mark_touch();
    //         let gas_used = U256::from(gas.spent() - gas.refunded() as u64);
    //         coinbase_account.data.info.balance =
    //             coinbase_account.data.info.balance.saturating_add(coinbase_gas_price * gas_used);

    //         if let Some(basefee_address) = basefee_address {
    //             // Send the base fee portion to a basefee account for later processing
    // (offchain).             let basefee = ctx.evm.env.block.basefee;
    //             let basefee_account = ctx
    //                 .evm
    //                 .inner
    //                 .journaled_state
    //                 .load_account(basefee_address, &mut ctx.evm.inner.db)?;
    //             basefee_account.data.mark_touch();
    //             basefee_account.data.info.balance =
    //                 basefee_account.data.info.balance.saturating_add(basefee * gas_used);
    //         }
    //         Ok(())
    //     });
    // }
}

impl<DB: Database, I, PRECOMPILE> Deref for TNEvm<DB, I, PRECOMPILE> {
    type Target = TNEvmContext<DB>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.ctx()
    }
}

impl<DB: Database, I, PRECOMPILE> DerefMut for TNEvm<DB, I, PRECOMPILE> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx_mut()
    }
}

// alloy-evm
impl<DB, I, PRECOMPILE> Evm for TNEvm<DB, I, PRECOMPILE>
where
    DB: Database,
    I: Inspector<TNEvmContext<DB>>,
    PRECOMPILE: PrecompileProvider<TNEvmContext<DB>, Output = InterpreterResult>,
{
    type DB = DB;
    type Tx = TxEnv;
    type Error = EVMError<DB::Error>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type Precompiles = PRECOMPILE;
    type Inspector = I;

    fn block(&self) -> &BlockEnv {
        &self.block
    }

    fn chain_id(&self) -> u64 {
        self.cfg.chain_id
    }

    fn transact_raw(&mut self, tx: Self::Tx) -> Result<ResultAndState, Self::Error> {
        if self.inspect {
            self.inner.set_tx(tx);
            self.inner.inspect_replay()
        } else {
            self.inner.transact(tx)
        }
    }

    fn transact_system_call(
        &mut self,
        caller: Address,
        contract: Address,
        data: Bytes,
    ) -> Result<ResultAndState, Self::Error> {
        let tx = TxEnv {
            caller,
            kind: TxKind::Call(contract),
            // Explicitly set nonce to 0 so revm does not do any nonce checks
            nonce: 0,
            gas_limit: 30_000_000,
            value: U256::ZERO,
            data,
            // Setting the gas price to zero enforces that no value is transferred as part of the
            // call, and that the call will not count against the block's gas limit
            gas_price: 0,
            // The chain ID check is not relevant here and is disabled if set to None
            chain_id: None,
            // Setting the gas priority fee to None ensures the effective gas price is derived from
            // the `gas_price` field, which we need to be zero
            gas_priority_fee: None,
            access_list: Default::default(),
            // blob fields can be None for this tx
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: 0,
            tx_type: 0,
            authorization_list: Default::default(),
        };

        let mut gas_limit = tx.gas_limit;
        let mut basefee = 0;
        let mut disable_nonce_check = true;

        // ensure the block gas limit is >= the tx
        core::mem::swap(&mut self.block.gas_limit, &mut gas_limit);
        // disable the base fee check for this call by setting the base fee to zero
        core::mem::swap(&mut self.block.basefee, &mut basefee);
        // disable the nonce check
        core::mem::swap(&mut self.cfg.disable_nonce_check, &mut disable_nonce_check);

        let mut res = self.transact(tx);

        // swap back to the previous gas limit
        core::mem::swap(&mut self.block.gas_limit, &mut gas_limit);
        // swap back to the previous base fee
        core::mem::swap(&mut self.block.basefee, &mut basefee);
        // swap back to the previous nonce check flag
        core::mem::swap(&mut self.cfg.disable_nonce_check, &mut disable_nonce_check);

        // NOTE: Only the contract storage should be modified. revm currently marks the
        // caller and block beneficiary accounts as "touched" after the above transact calls,
        // and includes them in the result.
        //
        // Cleanup state here to make sure that changeset only includes the changed
        // contract storage.
        if let Ok(res) = &mut res {
            res.state.retain(|addr, _| *addr == contract);
        }

        res
    }

    fn db_mut(&mut self) -> &mut Self::DB {
        &mut self.journaled_state.database
    }

    fn finish(self) -> (Self::DB, EvmEnv<Self::Spec>) {
        let Context { block: block_env, cfg: cfg_env, journaled_state, .. } = self.inner.ctx;

        (journaled_state.database, EvmEnv { block_env, cfg_env })
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.inspect = enabled;
    }

    fn precompiles(&self) -> &Self::Precompiles {
        &self.inner.precompiles
    }

    fn precompiles_mut(&mut self) -> &mut Self::Precompiles {
        &mut self.inner.precompiles
    }

    fn inspector(&self) -> &Self::Inspector {
        &self.inner.inspector
    }

    fn inspector_mut(&mut self) -> &mut Self::Inspector {
        &mut self.inner.inspector
    }
}
// Add a new impl block AFTER the Evm trait implementation
impl<DB, I, PRECOMPILE> TNEvm<DB, I, PRECOMPILE>
where
    DB: Database,
    I: Inspector<TNEvmContext<DB>>,
    PRECOMPILE: PrecompileProvider<TNEvmContext<DB>, Output = InterpreterResult>,
{
    /// Transact pre-genesis calls.
    pub(crate) fn transact_pre_genesis_create(
        &mut self,
        caller: Address,
        data: Bytes,
    ) -> Result<ResultAndState, EVMError<DB::Error>> {
        let tx = TxEnv {
            caller,
            kind: TxKind::Create,
            // Explicitly set nonce to 0 so revm does not do any nonce checks
            nonce: 0,
            gas_limit: 30_000_000,
            value: U256::ZERO,
            data,
            // Setting the gas price to zero enforces that no value is transferred as part of the
            // call, and that the call will not count against the block's gas limit
            gas_price: 0,
            // The chain ID check is not relevant here and is disabled if set to None
            chain_id: None,
            // Setting the gas priority fee to None ensures the effective gas price is derived from
            // the `gas_price` field, which we need to be zero
            gas_priority_fee: None,
            access_list: Default::default(),
            // blob fields can be None for this tx
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: 0,
            tx_type: 0,
            authorization_list: Default::default(),
        };

        let mut gas_limit = tx.gas_limit;
        let mut basefee = 0;
        let mut disable_nonce_check = true;

        // ensure the block gas limit is >= the tx
        core::mem::swap(&mut self.block.gas_limit, &mut gas_limit);
        // disable the base fee check for this call by setting the base fee to zero
        core::mem::swap(&mut self.block.basefee, &mut basefee);
        // disable the nonce check
        core::mem::swap(&mut self.cfg.disable_nonce_check, &mut disable_nonce_check);

        let res = self.transact(tx);

        // swap back to the previous gas limit
        core::mem::swap(&mut self.block.gas_limit, &mut gas_limit);
        // swap back to the previous base fee
        core::mem::swap(&mut self.block.basefee, &mut basefee);
        // swap back to the previous nonce check flag
        core::mem::swap(&mut self.cfg.disable_nonce_check, &mut disable_nonce_check);

        // unlike `Self::transact_system_call`, return the full state
        res
    }
}
