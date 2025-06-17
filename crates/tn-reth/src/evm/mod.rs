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
    handler::{instructions::EthInstructions, Handler as _, PrecompileProvider},
    inspector::{InspectorHandler, NoOpInspector},
    interpreter::{interpreter::EthInterpreter, InterpreterResult},
    primitives::hardfork::SpecId,
    Context, ExecuteEvm as _, Inspector,
};
use std::ops::{Deref, DerefMut};
use tn_types::{Address, Bytes, TxKind, U256};
mod block;
mod config;
mod context;
mod factory;
mod handler;
pub(crate) use block::*;
pub(crate) use config::*;
pub(crate) use context::*;
pub(crate) use factory::*;

use crate::evm::handler::TNEvmHandler;

/// TN EVM implementation.
///
/// This is a wrapper type around the `revm` ethereum evm with optional [`Inspector`] (tracing)
/// support. [`Inspector`] support is configurable at runtime because it's part of the underlying
/// [`RevmEvm`] type.
#[expect(missing_debug_implementations)]
pub struct TNEvm<DB: Database, I = NoOpInspector, PRECOMPILE = PrecompilesMap> {
    inner:
        RevmEvm<TNEvmContext<DB>, I, EthInstructions<EthInterpreter, TNEvmContext<DB>>, PRECOMPILE>,
    inspect: bool,
}

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
        let mut handler = TNEvmHandler::default();
        if self.inspect {
            self.inner.set_tx(tx);
            handler.inspect_run(&mut self.inner)
        } else {
            self.inner.set_tx(tx);
            handler.run(&mut self.inner)
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

        let res = self.transact(tx);

        // swap back to the previous gas limit
        core::mem::swap(&mut self.block.gas_limit, &mut gas_limit);
        // swap back to the previous base fee
        core::mem::swap(&mut self.block.basefee, &mut basefee);
        // swap back to the previous nonce check flag
        core::mem::swap(&mut self.cfg.disable_nonce_check, &mut disable_nonce_check);

        // NOTE: revm currently marks the caller and block beneficiary accounts as "touched"
        // after the above transact calls, and includes them in the result.
        //
        // System calls are used by TN protocol to update more than just the contract.
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
