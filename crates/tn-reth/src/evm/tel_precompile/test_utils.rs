//! Test-utilities for unit and integration tests.
//!
//! This module is the single source of truth for lightweight in-memory EVM test
//! infrastructure for the Telcoin precompile.

use super::*;
use crate::evm::context::{TNEvmContext, TelcoinEvm};
use alloy_evm::precompiles::PrecompilesMap;
use reth_revm::{
    bytecode::Bytecode,
    context::{
        result::{EVMError, ExecutionResult, InvalidTransaction},
        BlockEnv, Context, ContextSetters, Evm, FrameStack, TxEnv,
    },
    db::InMemoryDB,
    handler::{instructions::EthInstructions, EthPrecompiles, Handler, MainnetHandler},
    inspector::NoOpInspector,
    primitives::{address, Address, KECCAK_EMPTY},
    state::AccountInfo,
    Database, MainContext,
};
use std::collections::HashMap;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Bytes, TxKind, U256};

// --- Type aliases ---

/// EVM context type used by the test harness, backed by an [`InMemoryDB`].
///
/// Resolves to the same `Context<BlockEnv, TxEnv, CfgEnv, InMemoryDB>` that mainnet
/// code uses, but with an in-memory database for isolation.
pub type TestCtx = TNEvmContext<InMemoryDB>;

/// Fully-assembled EVM instance for tests, with the Telcoin precompile registered.
///
/// Uses [`PrecompilesMap`] containing both standard Ethereum precompiles and the
/// TEL precompile at [`TELCOIN_PRECOMPILE_ADDRESS`].
pub type TestEvmInner = TelcoinEvm<TestCtx, NoOpInspector>;

/// Result type returned by [`TestEnv::exec`] and [`TestEnv::exec_default`].
///
/// `Ok(ExecutionResult)` contains the EVM execution outcome (success, revert, or halt).
/// `Err(EVMError)` indicates a validation failure before execution (e.g., invalid nonce).
pub type TestResult =
    Result<ExecutionResult, EVMError<core::convert::Infallible, InvalidTransaction>>;

// --- Constants ---

/// Test address used as a generic unprivileged caller in unit and integration tests.
pub const USER: Address = address!("1111100000000000000000000000000000000001");

/// Test address used as a transfer/mint recipient in unit and integration tests.
pub const RECIPIENT: Address = address!("2222222000000000000000000000000000000002");

/// Genesis total supply in whole TEL units (100 billion).
///
/// The test harness seeds `TOTAL_SUPPLY_SLOT` with `GENESIS_SUPPLY * 10^18` wei.
pub const GENESIS_SUPPLY: u128 = 100_000_000_000; // 100B

// --- Test environment ---

/// Lightweight in-memory EVM environment for testing the Telcoin precompile.
///
/// Wraps a fully-configured [`TestEvmInner`] with pre-funded accounts and the TEL precompile
/// registered. Tracks per-address nonces to allow sequential calls without manual nonce
/// management.
///
/// # Default accounts
///
/// [`TestEnv::new`] creates accounts with 1 ETH (10^18 wei) each:
/// - [`GOVERNANCE_SAFE_ADDRESS`] — governance caller
/// - [`USER`] — unprivileged caller
///
/// The precompile account at [`TELCOIN_PRECOMPILE_ADDRESS`] is funded with 1000 wei and
/// seeded with [`GENESIS_SUPPLY`] in `totalSupply`.
#[derive(Debug)]
pub struct TestEnv {
    /// The EVM instance with in-memory state and the Telcoin precompile.
    pub evm: TestEvmInner,
    /// Per-address nonce tracker, auto-incremented by [`exec`](Self::exec).
    pub nonces: HashMap<Address, u64>,
}

impl TestEnv {
    /// Create a test environment with default balances.
    pub fn new() -> Self {
        Self::new_with_balances(
            U256::from(10).pow(U256::from(18)),
            U256::from(10).pow(U256::from(18)),
            U256::from(1000),
        )
    }

    /// Create a test environment with explicit initial balances for governance, user, and
    /// precompile accounts.
    pub fn new_with_balances(governance_bal: U256, user_bal: U256, precompile_bal: U256) -> Self {
        let mut db = InMemoryDB::default();

        db.insert_account_info(
            GOVERNANCE_SAFE_ADDRESS,
            AccountInfo {
                balance: governance_bal,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_info(
            USER,
            AccountInfo {
                balance: user_bal,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_info(
            TELCOIN_PRECOMPILE_ADDRESS,
            AccountInfo {
                balance: precompile_bal,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_storage(
            TELCOIN_PRECOMPILE_ADDRESS,
            U256::from(100),
            U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18)),
        )
        .unwrap();

        let block = BlockEnv { timestamp: U256::from(1000), ..Default::default() };
        let context = Context::mainnet().with_db(db).with_block(block);

        let mut precompiles = PrecompilesMap::from(EthPrecompiles::default());
        add_telcoin_precompile(&mut precompiles);

        let evm = Evm {
            ctx: context,
            inspector: NoOpInspector,
            instruction: EthInstructions::default(),
            precompiles,
            frame_stack: FrameStack::new(),
        };

        Self { evm, nonces: HashMap::new() }
    }

    /// Add an account with the given balance after construction.
    pub fn add_account(&mut self, addr: Address, balance: U256) {
        self.evm.ctx.journaled_state.database.insert_account_info(
            addr,
            AccountInfo {
                balance,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );
    }

    /// Deploy raw EVM bytecode at `addr`.
    ///
    /// The account is created with zero balance, nonce 1 (so it looks deployed), and a
    /// recomputed `code_hash`. Subsequent `CALL`s targeting `addr` execute `code` instead
    /// of being treated as a non-existent account.
    pub fn deploy_code(&mut self, addr: Address, code: Bytes) {
        let bytecode = Bytecode::new_raw(code);
        let code_hash = bytecode.hash_slow();
        self.evm.ctx.journaled_state.database.insert_account_info(
            addr,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 1,
                code_hash,
                code: Some(bytecode),
                ..Default::default()
            },
        );
    }

    /// Override the precompile's `totalSupply` storage slot.
    ///
    /// Useful for testing checked-arithmetic boundaries (overflow/underflow).
    pub fn set_total_supply(&mut self, amount: U256) {
        self.evm
            .ctx
            .journaled_state
            .database
            .insert_account_storage(TELCOIN_PRECOMPILE_ADDRESS, U256::from(100), amount)
            .unwrap();
    }

    /// Execute a precompile call with the given gas limit.
    ///
    /// Automatically increments the caller's nonce. The call targets
    /// [`TELCOIN_PRECOMPILE_ADDRESS`].
    pub fn exec(&mut self, caller: Address, calldata: Vec<u8>, gas_limit: u64) -> TestResult {
        self.exec_to(caller, TELCOIN_PRECOMPILE_ADDRESS, calldata, gas_limit)
    }

    /// Execute a transaction targeting `target` with the given gas limit.
    ///
    /// Automatically increments the caller's nonce. Useful for routing through a
    /// deployed contract (e.g. a `DELEGATECALL` relay) before reaching the precompile.
    pub fn exec_to(
        &mut self,
        caller: Address,
        target: Address,
        calldata: Vec<u8>,
        gas_limit: u64,
    ) -> TestResult {
        let nonce = self.nonces.entry(caller).or_insert(0);
        self.evm.ctx.set_tx(
            TxEnv::builder()
                .caller(caller)
                .kind(TxKind::Call(target))
                .data(calldata.into())
                .gas_limit(gas_limit)
                .nonce(*nonce)
                .build()
                .unwrap(),
        );
        *nonce += 1;
        // use mainnet handler with default gas to 0 for simpler test logic
        // pipeline tests use TN tools and account for gas usage
        MainnetHandler::default().run(&mut self.evm)
    }

    /// Execute a precompile call with the default gas limit (100,000).
    pub fn exec_default(&mut self, caller: Address, calldata: Vec<u8>) -> TestResult {
        self.exec(caller, calldata, 100_000)
    }

    /// Mint tokens via the precompile.
    ///
    /// In mainnet mode, creates a timelocked pending mint to governance (ignores `recipient`).
    /// In faucet mode, directly credits `recipient`.
    pub fn mint(&mut self, caller: Address, recipient: Address, amount: U256) -> TestResult {
        let _ = recipient; // unused in mainnet mode; suppress warning in faucet mode via cfg
        #[cfg(not(feature = "faucet"))]
        let data = {
            use alloy::sol_types::SolCall;
            super::burnable::mintCall { amount }.abi_encode()
        };
        #[cfg(feature = "faucet")]
        let data = {
            use alloy::sol_types::SolCall;
            super::faucet::mintCall { recipient, amount }.abi_encode()
        };
        self.exec_default(caller, data)
    }

    /// Read the native account balance of `account`.
    ///
    /// Prefers the in-memory journal state (which holds uncommitted modifications from
    /// previously executed test transactions); falls back to the database.
    pub fn get_balance(&mut self, account: Address) -> U256 {
        if let Some(acc) = self.evm.ctx.journaled_state.state.get(&account) {
            return acc.info.balance;
        }
        self.evm
            .ctx
            .journaled_state
            .database
            .basic(account)
            .unwrap()
            .map(|info| info.balance)
            .unwrap_or(U256::ZERO)
    }

    /// Read the precompile's `totalSupply` slot.
    ///
    /// Prefers the in-memory journal state; falls back to the database.
    pub fn get_total_supply(&mut self) -> U256 {
        self.get_storage(TELCOIN_PRECOMPILE_ADDRESS, U256::from(100))
    }

    /// Read a storage slot from `addr`.
    ///
    /// Prefers the in-memory journal state (which holds uncommitted modifications from
    /// previously executed test transactions); falls back to the database. Returns
    /// `U256::ZERO` if the slot was never written and the database has no entry.
    pub fn get_storage(&mut self, addr: Address, slot: U256) -> U256 {
        if let Some(acc) = self.evm.ctx.journaled_state.state.get(&addr) {
            if let Some(cell) = acc.storage.get(&slot) {
                return cell.present_value;
            }
        }
        self.evm.ctx.journaled_state.database.storage(addr, slot).unwrap_or(U256::ZERO)
    }

    /// Override the block timestamp for subsequent calls. Useful for testing timelocks.
    pub fn set_timestamp(&mut self, ts: u64) {
        let block = BlockEnv { timestamp: U256::from(ts), ..Default::default() };
        self.evm.ctx.set_block(block);
    }
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

// --- Assertion helpers ---

/// Assert that the result is `Ok(ExecutionResult::Success { .. })` and return the inner result.
///
/// Panics if the result is an error or a non-success execution outcome (revert/halt).
pub fn assert_success(result: &TestResult) -> &ExecutionResult {
    let r = result.as_ref().expect("expected Ok, got Err");
    assert!(matches!(r, ExecutionResult::Success { .. }), "expected Success, got {r:?}");
    r
}

/// Assert that the result is **not** a successful execution.
///
/// Accepts `Err(...)`, `Ok(Revert { .. })`, or `Ok(Halt { .. })`. Panics only on
/// `Ok(Success { .. })`.
pub fn assert_not_success(result: &TestResult) {
    if let Ok(ExecutionResult::Success { .. }) = result {
        panic!("expected non-success, got Success")
    }
}

/// Extract the raw output bytes from a successful execution result.
///
/// Panics if the result is not a success.
pub fn extract_output_bytes(result: &TestResult) -> Bytes {
    let r = assert_success(result);
    if let ExecutionResult::Success { output, .. } = r {
        output.data().clone()
    } else {
        unreachable!()
    }
}

/// Decode the first 32 bytes of a successful execution's output as a big-endian `U256`.
pub fn decode_u256(result: &TestResult) -> U256 {
    let bytes = extract_output_bytes(result);
    assert!(bytes.len() >= 32, "output too short for U256");
    U256::from_be_slice(&bytes[..32])
}

/// Decode a successful execution's output as a `bool` (`U256 != 0`).
pub fn decode_bool(result: &TestResult) -> bool {
    !decode_u256(result).is_zero()
}
