//! All types associated with execution TN EVM.
//!
//! Heavily inspired by alloy_evm and revm.
//!
//! # Module map — what TN customizes vs stock reth/revm
//!
//! - `config`: `TnEvmConfig`, TN's `ConfigureEvm` implementation; builds EVM/block environments
//!   from `TNPayload` and threads the epoch-close digest between the execution context and the
//!   header's `extra_data`.
//! - `factory`: `TNEvmFactory`, the single construction point for EVM instances; installs the TEL
//!   and BLS G1 precompiles on top of the spec's stock set for every EVM it creates.
//! - `handler`: `TNEvmHandler`, post-execution overrides — the base fee is credited to the chain's
//!   basefee address instead of burned, plus a quadratic penalty on grossly over-estimated gas
//!   limits.
//! - `utils`: the `calculate_gas_penalty` formula backing that penalty, plus
//!   `gas_penalty_and_refund`, the EIP-7702-aware penalty/refund split the handler applies.
//! - `context`: type aliases and builder plumbing for the revm `Context` TN executes against.
//! - `block`: `TNBlockExecutor`/`TNBlockAssembler` — block-level execution, the epoch-closing
//!   system calls (rewards, committee shuffle, `concludeEpoch`), and header assembly.
//! - `tel_precompile` / `bls_precompile`: the TEL native-token precompile and the BLS12-381 G1
//!   precompile (proof-of-possession verification).
//!
//! Anything not listed above follows stock revm mainnet semantics. [`TNEvm`] itself is the
//! revm wrapper tying these together; its `transact_system_call` is how protocol system calls
//! (consensus registry reads/writes, epoch closing) execute outside normal transaction rules.

use alloy_evm::Database;
use reth_evm::{precompiles::PrecompilesMap, Evm, EvmEnv};
use reth_revm::{
    context::{
        result::{EVMError, HaltReason, ResultAndState},
        BlockEnv, ContextSetters, ContextTr as _, Evm as RevmEvm, TxEnv,
    },
    handler::{instructions::EthInstructions, EthFrame, Handler as _, PrecompileProvider},
    inspector::{InspectorHandler, NoOpInspector},
    interpreter::{interpreter::EthInterpreter, InterpreterResult},
    primitives::hardfork::SpecId,
    Context, Inspector,
};
use std::ops::{Deref, DerefMut};
use tn_types::{Address, Bytes, TxKind, U256};
mod block;
mod bls_precompile;
mod config;
mod context;
mod factory;
mod handler;
#[cfg(any(test, feature = "test-utils"))]
pub mod precompile_test_utils;
mod tel_precompile;
mod utils;
use crate::evm::handler::TNEvmHandler;
pub(crate) use block::*;
pub use bls_precompile::{add_bls_precompile, BLS_G1_PRECOMPILE_ADDRESS};
pub(crate) use config::*;
pub(crate) use context::*;
pub(crate) use factory::*;
#[cfg(feature = "faucet")]
pub use tel_precompile::faucet_mint_role_slot;
#[cfg(not(feature = "faucet"))]
pub use tel_precompile::TIMELOCK_DURATION;
pub use tel_precompile::{
    add_telcoin_precompile, burnCall, claimCall, grantMintRoleCall, hasMintRoleCall, mintCall,
    revokeMintRoleCall, totalSupplyCall, TELCOIN_PRECOMPILE_ADDRESS,
};
pub use utils::calculate_gas_penalty;
/// Test-gated exposure so integration and property tests exercise the real penalty/refund
/// split the handler applies, and the EIP-7623 floor clamp it feeds that split.
#[cfg(any(test, feature = "test-utils"))]
pub use utils::{effective_auth_intrinsic, gas_penalty_and_refund};

/// Gas budget for a single protocol system call.
///
/// System calls run at `gas_price: 0` and do not count toward the block gas limit, so this
/// bound exists only to stop runaway execution. The 100M headroom (vs the historical 30M
/// budget) is for the one-shot `migrateValidatorSets()` walk at the ConsensusRegistry fork
/// boundary (see `tn-types` `forks.rs`), which touches every validator in a single call; the
/// regular boundary calls fit far below it.
///
/// LOCKSTEP FLEET-UPGRADE REQUIREMENT: this value participates in consensus.
/// `transact_system_call` swaps `block.gas_limit` to this value for the call's duration, and a
/// binary built with the old 30M diverges from a 100M binary on any system call needing more
/// than 30M — exactly the calls the headroom exists for. Every validator must run a binary
/// with the same value before any close can depend on it. (Relatedly, the epoch boundary is
/// three calls — `applyIncentives`, `applySlashes`, `concludeEpoch` — vs two on `main`;
/// `applySlashes` is present in the deployed testnet and mainnet genesis bytecode and an
/// empty-array call is a state-neutral no-op, so existing history replays unchanged.)
///
/// Read-path note: contract reads that share the system-call plumbing (including RPC reads via
/// `read_contract_at_block`) inherit this ceiling — an accepted consequence of the raise.
///
/// Observability: the `tn_reth.epoch_close_*` gauges (`crate::metrics`) publish what each
/// epoch-boundary call spends against this budget, and the budget itself, so consumption is a
/// ratio a dashboard can watch rather than something discovered when a close halts the fleet.
pub(crate) const SYSTEM_CALL_GAS_LIMIT: u64 = 100_000_000;

/// Gas budget for a single pre-genesis constructor CREATE
/// (`TNEvm::transact_pre_genesis_create`).
///
/// Deliberately 30M — the chain's block gas limit — as a deploy-size ceiling for the genesis
/// ceremony, NOT a stale copy of the old 30M system-call budget (that constant is
/// [`SYSTEM_CALL_GAS_LIMIT`] and moved to 100M for epoch-boundary headroom). Pre-genesis
/// creates run at `gas_price: 0` with the block gas limit raised to match, so the bound exists
/// to stop runaway constructor execution at the same scale a live block would allow.
pub(crate) const PRE_GENESIS_CREATE_GAS_LIMIT: u64 = 30_000_000;

/// TN EVM implementation.
///
/// This is a wrapper type around the `revm` ethereum evm with optional [`Inspector`] (tracing)
/// support. [`Inspector`] support is configurable at runtime because it's part of the underlying
/// [`RevmEvm`] type.
#[expect(missing_debug_implementations)]
pub struct TNEvm<DB: Database, I = NoOpInspector, PRECOMPILE = PrecompilesMap> {
    inner: RevmEvm<
        TNEvmContext<DB>,
        I,
        EthInstructions<EthInterpreter, TNEvmContext<DB>>,
        PRECOMPILE,
        EthFrame,
    >,

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
            EthFrame,
        >,
        inspect: bool,
    ) -> Self {
        Self { inner: evm, inspect }
    }

    /// Consumes self and return the inner EVM instance.
    pub fn into_inner(
        self,
    ) -> RevmEvm<
        TNEvmContext<DB>,
        I,
        EthInstructions<EthInterpreter, TNEvmContext<DB>>,
        PRECOMPILE,
        EthFrame,
    > {
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
    type BlockEnv = BlockEnv;
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

    // revm v87
    fn transact_raw(&mut self, tx: Self::Tx) -> Result<ResultAndState, Self::Error> {
        let mut handler = TNEvmHandler::default();
        if self.inspect {
            self.inner.set_tx(tx);
            handler.inspect_run(&mut self.inner).map(|result| {
                let state = self.ctx_mut().journal_mut().finalize();
                ResultAndState::new(result, state)
            })
        } else {
            self.inner.set_tx(tx);
            handler.run(&mut self.inner).map(|result| {
                let state = self.ctx_mut().journal_mut().finalize();
                ResultAndState::new(result, state)
            })
        }
    }

    /// Execute a protocol system call from `caller` (normally `SYSTEM_ADDRESS`) to `contract`,
    /// outside normal transaction rules.
    ///
    /// Environment for the call, restored afterwards via swaps:
    /// - fixed [`SYSTEM_CALL_GAS_LIMIT`] gas budget, with the block gas limit temporarily raised to
    ///   match so the call never competes with block gas accounting;
    /// - `gas_price` 0 and the block base fee swapped to 0, so no fee is charged and the caller
    ///   needs no balance (upfront cost is zero; `value` is zero too);
    /// - nonce check disabled (`disable_nonce_check` swapped on; the tx nonce field is 0) and no
    ///   chain-id check (`chain_id: None`).
    ///
    /// The result state is returned, NOT committed — the caller commits it, and is responsible
    /// for removing `SYSTEM_ADDRESS` from the state first (see the NOTE below). The custom
    /// handler skips `reward_beneficiary`/`reimburse_caller` for `SYSTEM_ADDRESS` callers so
    /// the beneficiary and basefee accounts are never spuriously touched by system calls.
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
            gas_limit: SYSTEM_CALL_GAS_LIMIT,
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
        // Callers are responsible for removing SYSTEM_ADDRESS from the result state
        // before committing. The reward_beneficiary handler skips system calls to
        // prevent the beneficiary and basefee address from being spuriously touched.
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

    fn components(&self) -> (&Self::DB, &Self::Inspector, &Self::Precompiles) {
        (&self.inner.ctx.journaled_state.database, &self.inner.inspector, &self.inner.precompiles)
    }

    fn components_mut(&mut self) -> (&mut Self::DB, &mut Self::Inspector, &mut Self::Precompiles) {
        (
            &mut self.inner.ctx.journaled_state.database,
            &mut self.inner.inspector,
            &mut self.inner.precompiles,
        )
    }
}

// Add a new impl block AFTER the Evm trait implementation
impl<DB, I, PRECOMPILE> TNEvm<DB, I, PRECOMPILE>
where
    DB: Database,
    I: Inspector<TNEvmContext<DB>>,
    PRECOMPILE: PrecompileProvider<TNEvmContext<DB>, Output = InterpreterResult>,
{
    /// Deploy a contract during pre-genesis construction (a CREATE, not a call).
    ///
    /// Uses the same relaxed environment as `transact_system_call` — zero gas price and base fee,
    /// nonce and chain-id checks disabled — under its own [`PRE_GENESIS_CREATE_GAS_LIMIT`]
    /// budget with the block gas limit raised to match, but issues a `TxKind::Create` and
    /// returns the full result state for the caller to commit (no `SYSTEM_ADDRESS` stripping
    /// convention here).
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
            gas_limit: PRE_GENESIS_CREATE_GAS_LIMIT,
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
