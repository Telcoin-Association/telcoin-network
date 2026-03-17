//! Test-utilities for unit tests

use super::*;
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
use tn_types::{TxKind, U256};

/// Test EVM type using PrecompilesMap with the Telcoin precompile registered.
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

pub(crate) const USER: Address = address!("1111100000000000000000000000000000000001");
pub(crate) const RECIPIENT: Address = address!("2222222000000000000000000000000000000002");

pub(crate) struct TestEnv {
    pub(crate) evm: TestEvmInner,
    nonces: HashMap<Address, u64>,
}

impl TestEnv {
    pub(crate) fn new() -> Self {
        let mut db = InMemoryDB::default();

        db.insert_account_info(
            GOVERNANCE_SAFE_ADDRESS,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(18)),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_info(
            USER,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(18)),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_info(
            TELCOIN_PRECOMPILE_ADDRESS,
            AccountInfo {
                balance: U256::from(1000),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );

        db.insert_account_storage(
            TELCOIN_PRECOMPILE_ADDRESS,
            U256::from(100),
            U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18)),
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

    #[cfg(not(feature = "faucet"))]
    pub(crate) fn set_timestamp(&mut self, ts: u64) {
        let mut block = BlockEnv::default();
        block.timestamp = U256::from(ts);
        self.evm.ctx.set_block(block);
    }
}

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
