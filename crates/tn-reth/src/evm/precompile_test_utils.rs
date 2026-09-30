//! Shared in-memory EVM test harness for the native precompiles.
//!
//! This is the single source of truth for lightweight EVM test infrastructure used to exercise the
//! precompiles registered by the production factory (TEL at `0x7e1`, BLS at `0x…b151`). It builds a
//! [`TestEnv`] backed by an [`InMemoryDB`] with both precompiles registered, and provides
//! execution, balance/storage inspection, and output-decoding helpers.
//!
//! Precompile-specific helpers live alongside their precompile: the TEL token helpers
//! (`mint`, `get_total_supply`, `set_total_supply`, [`GENESIS_SUPPLY`]) are defined in
//! `tel_precompile::test_utils` as an extension of
//! [`TestEnv`]. The stateless BLS precompile needs no extra harness.

use crate::{
    evm::{
        bls_precompile::add_bls_precompile,
        context::{TNEvmContext, TelcoinEvm},
        tel_precompile::{add_telcoin_precompile, TELCOIN_PRECOMPILE_ADDRESS},
    },
    system_calls::PRECOMPILE_GENESIS_BYTECODE,
};
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
    primitives::{address, Address, Log, KECCAK_EMPTY},
    state::AccountInfo,
    Database, MainContext,
};
use std::collections::HashMap;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Bytes, TxKind, U256};

/// TEL genesis supply, re-exported here so consumers keep importing it from
/// `precompile_test_utils` even though it (and the TEL token helpers) live with the TEL
/// precompile.
pub use crate::evm::tel_precompile::test_utils::GENESIS_SUPPLY;

/// Halt classification, re-exported so tests can name the reason
/// [`assert_halt_consuming_all_gas`] hands back without importing revm themselves.
pub use reth_revm::context::result::{HaltReason, OutOfGasError};

// --- Type aliases ---

/// EVM context type used by the test harness, backed by an [`InMemoryDB`].
///
/// Resolves to the same `Context<BlockEnv, TxEnv, CfgEnv, InMemoryDB>` that mainnet
/// code uses, but with an in-memory database for isolation.
pub type TestCtx = TNEvmContext<InMemoryDB>;

/// Fully-assembled EVM instance for tests, with the native precompiles registered.
///
/// Uses [`PrecompilesMap`] containing both standard Ethereum precompiles and the
/// Telcoin precompiles (TEL at [`TELCOIN_PRECOMPILE_ADDRESS`], BLS at `0x…b151`).
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

/// Gas limit [`TestEnv::exec_default`] attaches to a call.
///
/// Named so tests that assert on gas consumption can refer to the same number the call was given
/// instead of repeating the literal.
pub const DEFAULT_GAS_LIMIT: u64 = 100_000;

// --- Test environment ---

/// Lightweight in-memory EVM environment for testing the native precompiles.
///
/// Wraps a fully-configured [`TestEvmInner`] with pre-funded accounts and both Telcoin precompiles
/// registered. Tracks per-address nonces to allow sequential calls without manual nonce
/// management.
///
/// # Default accounts
///
/// [`TestEnv::new`] creates accounts with 1 ETH (10^18 wei) each:
/// - [`GOVERNANCE_SAFE_ADDRESS`] — governance caller
/// - [`USER`] — unprivileged caller
///
/// The TEL precompile account at [`TELCOIN_PRECOMPILE_ADDRESS`] is funded with 1000 wei, given
/// the same `0xfe` code mainnet genesis assigns it, and seeded with [`GENESIS_SUPPLY`] in
/// `totalSupply`.
#[derive(Debug)]
pub struct TestEnv {
    /// The EVM instance with in-memory state and the precompiles registered.
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

        // Mirror the mainnet genesis account for the precompile: zero nonce, the requested
        // balance, and the bare `INVALID` byte `chain-configs/mainnet/genesis.yaml` assigns
        // `0x7e1`, taken from `PRECOMPILE_GENESIS_BYTECODE` rather than restated here so the
        // harness cannot drift from the constant the rest of the crate builds genesis accounts
        // from. The precompile map short-circuits before any bytecode load, so the byte never
        // executes; it is there to keep the account out of EIP-158 state clearing once its balance
        // drains to zero, so tests that burn the pool empty exercise the same account shape
        // production has. An empty account here would only diverge once something finalizes the
        // journal, which this harness never does, but the divergence is free to remove.
        let precompile_code = Bytecode::new_raw(Bytes::from_static(PRECOMPILE_GENESIS_BYTECODE));
        db.insert_account_info(
            TELCOIN_PRECOMPILE_ADDRESS,
            AccountInfo {
                balance: precompile_bal,
                nonce: 0,
                code_hash: precompile_code.hash_slow(),
                code: Some(precompile_code),
                ..Default::default()
            },
        );

        // Seed the TEL precompile's genesis supply. This is TEL-specific state, but lives in the
        // shared constructor so the many existing TEL tests get a realistic genesis from
        // `TestEnv::new()`; the stateless BLS precompile is unaffected by it.
        db.insert_account_storage(
            TELCOIN_PRECOMPILE_ADDRESS,
            U256::from(100),
            U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18)),
        )
        .unwrap();

        let block = BlockEnv { timestamp: U256::from(1000), ..Default::default() };
        let context = Context::mainnet().with_db(db).with_block(block);

        // Register both native precompiles, mirroring the production factory.
        let mut precompiles = PrecompilesMap::from(EthPrecompiles::default());
        add_telcoin_precompile(&mut precompiles);
        add_bls_precompile(&mut precompiles);

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
        self.exec_value_to(caller, target, calldata, gas_limit, U256::ZERO)
    }

    /// Execute a transaction targeting `target` with the given gas limit and attached call value.
    ///
    /// Automatically increments the caller's nonce. Nonzero `value` exercises the precompile
    /// dispatcher's payability gate.
    pub fn exec_value_to(
        &mut self,
        caller: Address,
        target: Address,
        calldata: Vec<u8>,
        gas_limit: u64,
        value: U256,
    ) -> TestResult {
        let nonce = self.nonces.entry(caller).or_insert(0);
        self.evm.ctx.set_tx(
            TxEnv::builder()
                .caller(caller)
                .kind(TxKind::Call(target))
                .data(calldata.into())
                .gas_limit(gas_limit)
                .nonce(*nonce)
                .value(value)
                .build()
                .unwrap(),
        );
        *nonce += 1;
        // use mainnet handler with default gas to 0 for simpler test logic
        // pipeline tests use TN tools and account for gas usage
        MainnetHandler::default().run(&mut self.evm)
    }

    /// Execute a precompile call with the given gas limit and attached call value.
    ///
    /// [`exec`](Self::exec) with `value` wei attached; targets [`TELCOIN_PRECOMPILE_ADDRESS`].
    pub fn exec_with_value(
        &mut self,
        caller: Address,
        calldata: Vec<u8>,
        gas_limit: u64,
        value: U256,
    ) -> TestResult {
        self.exec_value_to(caller, TELCOIN_PRECOMPILE_ADDRESS, calldata, gas_limit, value)
    }

    /// Execute a precompile call with [`DEFAULT_GAS_LIMIT`].
    pub fn exec_default(&mut self, caller: Address, calldata: Vec<u8>) -> TestResult {
        self.exec(caller, calldata, DEFAULT_GAS_LIMIT)
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

// --- Gas probe bytecode ---

/// Call opcode a [`gas_probe_code`] probe uses to reach the precompile.
///
/// The variant fixes both the opcode byte and the operand count: `CALL` takes seven stack
/// operands, `DELEGATECALL` and `STATICCALL` take six, having no `value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeCall {
    /// `CALL` (`0xf1`) — a direct frame; the only one of the three carrying `value`.
    Call,
    /// `DELEGATECALL` (`0xf4`) — an indirect frame, refused by the direct-call guard.
    DelegateCall,
    /// `STATICCALL` (`0xfa`) — a read-only frame, refused for every mutating selector.
    StaticCall,
}

/// Runtime bytecode that measures what a sub-call into `0x7e1` costs its caller.
///
/// Reads `GAS` on either side of a `call`-family instruction that forwards everything available to
/// the precompile, and returns three words: the gas the frame held before the call, the call's
/// success flag, and the gas it held after. Nothing propagates the inner failure, so the probe
/// frame returns normally and the three words reach the transaction output even when the callee
/// halts — which is the point, since a relay that reverted on failure would erase the measurement.
///
/// Disassembly (the `value` push is emitted for [`ProbeCall::Call`] only):
/// ```text
///   CALLDATASIZE; PUSH1 0; PUSH1 0; CALLDATACOPY   // mem[0..csize] = calldata
///   GAS                                            // gas before
///   PUSH1 0; PUSH1 0; CALLDATASIZE; PUSH1 0;       // retSize, retOffset, argsSize,
///                                                  // argsOffset
///   PUSH1 0                                        // value (CALL only)
///   PUSH2 0x07e1; GAS; <CALL|DELEGATECALL|STATICCALL>
///   GAS                                            // gas after
///   PUSH1 0x40; MSTORE                             // mem[64] = gas after
///   PUSH1 0x20; MSTORE                             // mem[32] = success flag
///   PUSH1 0x00; MSTORE                             // mem[0]  = gas before
///   PUSH1 0x60; PUSH1 0; RETURN                    // return those three words
/// ```
pub fn gas_probe_code(call: ProbeCall) -> Bytes {
    let (opcode, pushes_value) = match call {
        ProbeCall::Call => (0xf1, true),
        ProbeCall::DelegateCall => (0xf4, false),
        ProbeCall::StaticCall => (0xfa, false),
    };
    let mut code = vec![
        0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
        0x5a, // GAS -> gas before
        0x60, 0x00, 0x60, 0x00, // retSize = 0, retOffset = 0
        0x36, 0x60, 0x00, // argsSize = CALLDATASIZE, argsOffset = 0
    ];
    if pushes_value {
        code.extend_from_slice(&[0x60, 0x00]); // value = 0
    }
    code.extend_from_slice(&[
        0x61, 0x07, 0xe1, 0x5a, opcode, // PUSH2 0x07e1; GAS; <call opcode>
        0x5a,   // GAS -> gas after
        0x60, 0x40, 0x52, // MSTORE(0x40, gas after)
        0x60, 0x20, 0x52, // MSTORE(0x20, success flag)
        0x60, 0x00, 0x52, // MSTORE(0x00, gas before)
        0x60, 0x60, 0x60, 0x00, 0xf3, // RETURN(0, 0x60)
    ]);
    code.into()
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

/// Assert that the result is `Ok(ExecutionResult::Halt { .. })` charged the full `gas_limit`, and
/// return the halt reason.
///
/// A precompile rejection is a halt rather than a revert, and revm hands back unspent gas only for
/// instruction results that are `is_ok_or_revert()`. A rejected top-level transaction is therefore
/// charged its whole `gas_limit`, and a rejected sub-call loses the entire 63/64 it was forwarded.
///
/// [`assert_not_success`] cannot hold that line: it accepts `Err`, `Revert`, and `Halt` alike, so a
/// revm change or a dispatcher rewrite that turned a rejection into a gas-refunding revert would
/// leave every rejection test green while the documented gas semantics silently changed. Use this
/// helper in the tests that exist to back that claim, and keep [`assert_not_success`] for the ones
/// that only care that the call did not go through.
///
/// Panics on `Err(..)`, on `Ok(Success { .. })`, on `Ok(Revert { .. })`, and on a halt whose
/// `gas_used` is not exactly `gas_limit`.
pub fn assert_halt_consuming_all_gas(result: &TestResult, gas_limit: u64) -> &HaltReason {
    let r = result.as_ref().expect("expected Ok(Halt), got Err");
    let ExecutionResult::Halt { reason, gas_used } = r else { panic!("expected Halt, got {r:?}") };
    assert_eq!(*gas_used, gas_limit, "a halt spends the whole gas limit");
    reason
}

/// Extract the logs emitted by a successful execution, in emission order.
///
/// Panics if the result is not a success.
pub fn extract_logs(result: &TestResult) -> &[Log] {
    assert_success(result).logs()
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
