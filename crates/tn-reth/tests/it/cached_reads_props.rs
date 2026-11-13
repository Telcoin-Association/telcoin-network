//! Property-based tests for the shared build path: one build scope (`OutputBuildScope`) per
//! consensus output, with reads through the cross-block execution cache (`exec_cache.rs`).
//!
//! These tests verify, over random native transfers, storage writes, CREATE2 creations (with and
//! without a self-destruct in the creating transaction), calls into the created children,
//! zero-value sends to an empty genesis account and BLOCKHASH reads of the two blocks before the
//! current one, spread across the 2-4 unpersisted blocks of one consensus output, that the
//! execution cache is invisible to consensus: the bytes build, which reads through the cache the
//! previous block's builds checked in, builds the blocks the recovered build with the shared scope
//! builds from an empty cache (equal block hash, state root, receipts root, gas used, receipts and
//! bundle state), and no checked hit is stale.

use crate::pipeline_helpers::{
    block_differences, child_init_code, deploy_code, PipelineTestEnv, FACTORY_RUNTIME,
};
use proptest::prelude::*;
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use tn_reth::{test_utils::TransactionFactory, ExecutedBlock, OutputBuildScope, OutputTrieOverlay};
use tn_types::{Address, Bytes, B256, MIN_PROTOCOL_BASE_FEE, U256};

/// Runtime of the store contract: `sstore(key, value + sload(read))` for the calldata words
/// `(key, value, read)`, so every call reads one slot and writes one (a zero write deletes it).
const STORE_RUNTIME: [u8; 12] = [
    0x60, 0x40, 0x35, 0x54, // sload(calldataload(0x40))
    0x60, 0x20, 0x35, 0x01, // + calldataload(0x20)
    0x5f, 0x35, 0x55, 0x00, // sstore(calldataload(0), ..); stop
];

/// Runtime of the block-hash contract: `sstore(0, blockhash(number - calldataload(0)))`, so every
/// call reads one earlier block's hash through the database and writes it to state.
const BLOCK_HASH_RUNTIME: [u8; 8] = [
    0x5f, 0x35, 0x43, 0x03, 0x40, // blockhash(number - calldataload(0))
    0x5f, 0x55, 0x00, // sstore(0, ..); stop
];

/// Recipients a transfer picks from, by index: a fresh address (a zero-value transfer only
/// touches it), the two senders, the store contract, the empty genesis account and the two
/// CREATE2 children's addresses (funding one before its creation creates over a funded account).
const RECIPIENTS: usize = 7;

/// The CREATE2 children the factory can create, by variant.
const CHILDREN: u8 = 2;

/// One transaction of a generated output, sent by `from` (0 = user, 1 = the second sender).
#[derive(Debug, Clone)]
enum Op {
    /// A native transfer of `amount` wei to recipient `to`.
    Transfer { from: usize, to: usize, amount: u64 },
    /// A store-contract call: `sstore(key, value + sload(read))`.
    Store { from: usize, key: u64, value: u64, read: u64 },
    /// A factory call creating child `child` with `value` wei: without value the child
    /// self-destructs in its own creation, with value it stores slot 1 (and the factory reverts
    /// when the child already exists).
    Create { from: usize, child: u8, value: u64 },
    /// A call into child `child`: `sstore(2, word + sload(1))` (a plain touch while it does not
    /// exist).
    CallChild { from: usize, child: u8, word: u64 },
    /// A zero-value send to the empty genesis account, which clears it under EIP-161 while it is
    /// empty.
    TouchEmpty { from: usize },
    /// A block-hash contract call reading the hash of the block `depth` below the current one
    /// (the parent at depth 1; every block of an output has at least two ancestors).
    BlockHash { from: usize, depth: u64 },
}

/// One of the six operations, over small ranges so senders, recipients, slots, children and block
/// hashes repeat within and across blocks.
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..2usize, 0..RECIPIENTS, 0..3u64)
            .prop_map(|(from, to, amount)| Op::Transfer { from, to, amount }),
        3 => (0..2usize, 0..4u64, 0..3u64, 0..4u64)
            .prop_map(|(from, key, value, read)| Op::Store { from, key, value, read }),
        2 => (0..2usize, 0..CHILDREN, 0..3u64)
            .prop_map(|(from, child, value)| Op::Create { from, child, value }),
        2 => (0..2usize, 0..CHILDREN, 0..3u64)
            .prop_map(|(from, child, word)| Op::CallChild { from, child, word }),
        1 => (0..2usize).prop_map(|from| Op::TouchEmpty { from }),
        1 => (0..2usize, 1..3u64).prop_map(|(from, depth)| Op::BlockHash { from, depth }),
    ]
}

/// The blocks of one output: 2-4 blocks of 1-6 transactions each.
fn output_ops() -> impl Strategy<Value = Vec<Vec<Op>>> {
    prop::collection::vec(prop::collection::vec(op(), 1..=6), 2..=4)
}

/// The environment of one generated output: the pipeline env (its governance EOA an empty genesis
/// account), a second funded sender and the contracts deployed before the output.
struct Fixture {
    env: PipelineTestEnv,
    /// The second sender, funded by the user before the output.
    second: TransactionFactory,
    /// The store contract.
    store: Address,
    /// The CREATE2 factory.
    factory: Address,
    /// The block-hash contract.
    hasher: Address,
}

impl Fixture {
    /// Deploy the store contract, the factory and the block-hash contract and fund the second
    /// sender, in one persisted block (user nonces 0-3).
    fn new() -> eyre::Result<Self> {
        let mut env = PipelineTestEnv::new_with_empty_governance_eoa();
        let second = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(300));
        let user = env.user_factory.address();
        let (store, factory, hasher) = (user.create(0), user.create(1), user.create(2));
        let funding = U256::from(10).pow(U256::from(18));
        let txs = [
            (None, U256::ZERO, deploy_code(&STORE_RUNTIME)),
            (None, U256::ZERO, deploy_code(&FACTORY_RUNTIME)),
            (None, U256::ZERO, deploy_code(&BLOCK_HASH_RUNTIME)),
            (Some(second.address()), funding, Bytes::new()),
        ]
        .into_iter()
        .map(|(to, value, input)| {
            env.user_factory.create_eip1559_encoded(
                env.chain.clone(),
                Some(1_000_000),
                MIN_PROTOCOL_BASE_FEE.into(),
                to,
                value,
                input,
            )
        })
        .collect();
        let setup = env.execute_block(txs)?;
        eyre::ensure!((0..4).all(|index| env.tx_succeeded(&setup, index)), "setup block failed");
        Ok(Self { env, second, store, factory, hasher })
    }

    /// The CREATE2 address of child `variant`.
    fn child(&self, variant: u8) -> Address {
        self.factory.create2_from_code(B256::ZERO, child_init_code(variant))
    }

    /// Sign and encode `op`.
    fn encode(&mut self, op: &Op) -> Vec<u8> {
        let recipients = [
            Address::with_last_byte(0xa1),
            self.env.user_factory.address(),
            self.second.address(),
            self.store,
            self.env.governance_factory.address(),
            self.child(0),
            self.child(1),
        ];
        let word = |value: u64| B256::from(U256::from(value)).0;
        let (from, to, value, input): (usize, Address, u64, Bytes) = match *op {
            Op::Transfer { from, to, amount } => (from, recipients[to], amount, Bytes::new()),
            Op::Store { from, key, value, read } => {
                (from, self.store, 0, [word(key), word(value), word(read)].concat().into())
            }
            Op::Create { from, child, value } => {
                (from, self.factory, value, child_init_code(child).into())
            }
            Op::CallChild { from, child, word: w } => {
                (from, self.child(child), 0, word(w).to_vec().into())
            }
            Op::TouchEmpty { from } => {
                (from, self.env.governance_factory.address(), 0, Bytes::new())
            }
            Op::BlockHash { from, depth } => (from, self.hasher, 0, word(depth).to_vec().into()),
        };
        let chain = self.env.chain.clone();
        let sender = if from == 0 { &mut self.env.user_factory } else { &mut self.second };
        sender.create_eip1559_encoded(
            chain,
            Some(1_000_000),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(to),
            U256::from(value),
            input,
        )
    }
}

/// Build `blocks` as one unpersisted output after the fixture's setup block, each block twice
/// (`PipelineTestEnv::build_unpersisted_fresh_and_shared`), calling `check` after each block with
/// both builds. No block may have been rebuilt after a stale execution-cache hit.
///
/// Every block must include all of its transactions, and every one but a factory call (which
/// reverts when its child exists) must succeed, so a generator out of step with the senders'
/// nonces fails here instead of comparing empty blocks.
fn run_output(
    blocks: &[Vec<Op>],
    mut check: impl FnMut(&ExecutedBlock, &ExecutedBlock) -> Result<(), TestCaseError>,
) -> Result<(), TestCaseError> {
    let fail = |err: eyre::Report| TestCaseError::fail(err.to_string());
    let mut fixture = Fixture::new().map_err(fail)?;
    let mut overlay = OutputTrieOverlay::new();
    let mut scope = OutputBuildScope::new();
    for (number, ops) in blocks.iter().enumerate() {
        let txs: Vec<Vec<u8>> = ops.iter().map(|op| fixture.encode(op)).collect();
        let (fresh, shared) = fixture
            .env
            .build_unpersisted_fresh_and_shared(&txs, &mut overlay, &mut scope)
            .map_err(fail)?;
        prop_assert_eq!(
            fresh.recovered_block.transaction_count(),
            ops.len(),
            "block {} left out a transaction",
            number + 1
        );
        for (index, op) in ops.iter().enumerate() {
            prop_assert!(
                matches!(op, Op::Create { .. }) || fixture.env.tx_succeeded(&fresh, index),
                "block {} tx {} failed: {:?}",
                number + 1,
                index,
                op
            );
        }
        prop_assert_eq!(
            fixture.env.reth_env.execution_cache_rebuilds(),
            0,
            "block {} hit a stale cache entry",
            number + 1
        );
        check(&fresh, &shared)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// The execution cache builds every block of the output exactly as an empty cache.
    #[test]
    fn prop_shared_cache_builds_identical_blocks(blocks in output_ops()) {
        let mut number = 0;
        run_output(&blocks, |fresh, shared| {
            number += 1;
            let differences = block_differences(fresh, shared);
            prop_assert!(differences.is_empty(), "block {} differs in {:?}", number, differences);
            Ok(())
        })?;
    }
}
