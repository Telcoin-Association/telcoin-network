//! Shared test infrastructure for Telcoin precompile integration tests.

use alloy::sol_types::SolCall;
use alloy_evm::precompiles::PrecompilesMap;
use reth_revm::{
    context::{
        result::{EVMError, ExecutionResult, InvalidTransaction, Output},
        BlockEnv, Context, ContextSetters, Evm, FrameStack, TxEnv,
    },
    db::InMemoryDB,
    handler::{
        instructions::EthInstructions, EthFrame, EthPrecompiles, Handler, MainnetContext,
        MainnetHandler,
    },
    inspector::NoOpInspector,
    interpreter::interpreter::EthInterpreter,
    primitives::{address, Address, KECCAK_EMPTY},
    state::AccountInfo,
    MainContext,
};
use std::collections::HashMap;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_reth::{
    add_telcoin_precompile, allowanceCall, balanceOfCall, totalSupplyCall,
    TELCOIN_PRECOMPILE_ADDRESS,
};
use tn_types::{Bytes, TxKind, U256};

// --- Type aliases ---

pub(crate) type TestCtx = MainnetContext<InMemoryDB>;

pub(crate) type TestEvmInner = Evm<
    TestCtx,
    NoOpInspector,
    EthInstructions<EthInterpreter, TestCtx>,
    PrecompilesMap,
    EthFrame<EthInterpreter>,
>;

pub(crate) type TestResult =
    Result<ExecutionResult, EVMError<core::convert::Infallible, InvalidTransaction>>;

// --- Constants ---

pub(crate) const GOVERNANCE: Address = GOVERNANCE_SAFE_ADDRESS;
pub(crate) const USER: Address = address!("1111100000000000000000000000000000000001");
pub(crate) const RECIPIENT: Address = address!("2222222000000000000000000000000000000002");
pub(crate) const GENESIS_SUPPLY: u128 = 100_000_000_000; // 100B

// --- Test environment ---

pub(crate) struct TestEnv {
    pub(crate) evm: TestEvmInner,
    pub(crate) nonces: HashMap<Address, u64>,
}

impl TestEnv {
    pub(crate) fn new() -> Self {
        Self::new_with_balances(
            U256::from(10).pow(U256::from(18)),
            U256::from(10).pow(U256::from(18)),
            U256::from(1000),
        )
    }

    pub(crate) fn new_with_balances(
        governance_bal: U256,
        user_bal: U256,
        precompile_bal: U256,
    ) -> Self {
        let mut db = InMemoryDB::default();

        db.insert_account_info(
            GOVERNANCE,
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

        // Genesis total supply: 100B * 10^18
        db.insert_account_storage(
            TELCOIN_PRECOMPILE_ADDRESS,
            U256::from(100),
            U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18)),
        )
        .unwrap();

        let mut block = BlockEnv::default();
        block.timestamp = U256::from(1000);
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
    pub(crate) fn add_account(&mut self, addr: Address, balance: U256) {
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

    pub(crate) fn exec(
        &mut self,
        caller: Address,
        calldata: Vec<u8>,
        gas_limit: u64,
    ) -> TestResult {
        let nonce = self.nonces.entry(caller).or_insert(0);
        self.evm.ctx.set_tx(
            TxEnv::builder()
                .caller(caller)
                .kind(TxKind::Call(TELCOIN_PRECOMPILE_ADDRESS))
                .data(calldata.into())
                .gas_limit(gas_limit)
                .nonce(*nonce)
                .build()
                .unwrap(),
        );
        *nonce += 1;
        MainnetHandler::default().run(&mut self.evm)
    }

    pub(crate) fn exec_default(&mut self, caller: Address, calldata: Vec<u8>) -> TestResult {
        self.exec(caller, calldata, 100_000)
    }

    pub(crate) fn get_balance(&mut self, account: Address) -> U256 {
        let result = self.exec_default(GOVERNANCE, balanceOfCall { account }.abi_encode());
        decode_u256(&result)
    }

    pub(crate) fn get_total_supply(&mut self) -> U256 {
        let result = self.exec_default(GOVERNANCE, totalSupplyCall {}.abi_encode());
        decode_u256(&result)
    }

    pub(crate) fn get_allowance(&mut self, owner: Address, spender: Address) -> U256 {
        let result = self.exec_default(GOVERNANCE, allowanceCall { owner, spender }.abi_encode());
        decode_u256(&result)
    }

    pub(crate) fn set_timestamp(&mut self, ts: u64) {
        let mut block = BlockEnv::default();
        block.timestamp = U256::from(ts);
        self.evm.ctx.set_block(block);
    }
}

// --- Assertion helpers ---

pub(crate) fn assert_success(result: &TestResult) -> &ExecutionResult {
    let r = result.as_ref().expect("expected Ok, got Err");
    assert!(matches!(r, ExecutionResult::Success { .. }), "expected Success, got {r:?}");
    r
}

pub(crate) fn assert_not_success(result: &TestResult) {
    match result {
        Ok(ExecutionResult::Success { .. }) => panic!("expected non-success, got Success"),
        _ => {}
    }
}

pub(crate) fn extract_output_bytes(result: &TestResult) -> Bytes {
    let r = assert_success(result);
    match r {
        ExecutionResult::Success { output, .. } => match output {
            Output::Call(b) => b.clone(),
            Output::Create(b, _) => b.clone(),
        },
        _ => unreachable!(),
    }
}

pub(crate) fn decode_u256(result: &TestResult) -> U256 {
    let bytes = extract_output_bytes(result);
    assert!(bytes.len() >= 32, "output too short for U256");
    U256::from_be_slice(&bytes[..32])
}

pub(crate) fn decode_bool(result: &TestResult) -> bool {
    !decode_u256(result).is_zero()
}
