//! Differential and persist-fault tests of the pipelined persist ([`PersistPipeline`]).
//!
//! Two execution nodes start from one genesis that funds two senders and deploys a store
//! contract, a CREATE2 factory (whose children can be created and destroyed in one transaction,
//! then recreated at the same address) and a block-hash reader that logs and stores
//! `blockhash(number - depth)`, plus an empty account. Node A executes every consensus output with
//! [`execute_consensus_output`], the sequential path, and is the oracle. Node B runs the same
//! outputs through the pipeline, with every commit held at the pipeline's test gate so the test
//! picks, per output, when the previous output's commit is released: before the output starts
//! building, right after it starts (the commit races the build), or after it has built (it waits
//! parked).
//!
//! The properties:
//! - `prop_pipelined_matches_sequential`: after every build the in-memory tip is the oracle's;
//!   after every commit the committed head and the trie tables equal the oracle's after that
//!   output, and the in-memory chain holds only unpersisted blocks, each the oracle's; at the end
//!   every block, receipt, trie table, engine update and accumulator count is the oracle's.
//! - `prop_persist_fault_leaves_durable_prefix`: a persist fault (early on every attempt, or late,
//!   after the static files advanced) on one output's commit fails the pipeline with the provider
//!   error after the first attempts the retry policy allows; the database, the in-memory chain and
//!   the announced updates end at the previous output, which equals the oracle's; the pipeline
//!   refuses further starts; and a restart (the same environment after an early fault, the reopened
//!   datadir after a late one) replays the rest to the oracle's final database.
//!
//! The deterministic tests pin the shapes the generator reaches only by chance.

use crate::IT_TEST_GUARD;
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, PoisonError},
    time::Duration,
};
use tempfile::TempDir;
use tn_engine::{
    execute_consensus_output,
    persister::{BuildReport, CommitGate, CommitReport, PersistPipeline, PipelineEvent},
    TnEngineError, PERSIST_OUTPUT_ATTEMPTS,
};
use tn_reth::{
    error::TnRethError,
    payload::BuildArguments,
    test_utils::{
        create_committee_from_state, test_genesis_with_consensus_registry, TransactionFactory,
    },
    RethChainSpec, RethEnv,
};
use tn_types::{
    gas_accumulator::GasAccumulator, keccak256, now, repack_monitor::RepackMonitor, Address,
    AuthorityIdentifier, Batch, Bytes, Certificate, CertifiedBatch, CommittedSubDag, Committee,
    ConsensusHeaderDigest, ConsensusOutput, EngineUpdate, EpochSeedChainValue, GenesisAccount,
    ReputationScores, Round, SealedBlock, SealedHeader, TaskManager, B256, MIN_PROTOCOL_BASE_FEE,
    U256,
};
use tokio::sync::mpsc;

/// Runtime of the store contract: `sstore(key, value + sload(read))` for the calldata words
/// `(key, value, read)`, so every call reads one slot and writes one (a zero write deletes it).
const STORE_RUNTIME: [u8; 12] = [
    0x60, 0x40, 0x35, 0x54, // sload(calldataload(0x40))
    0x60, 0x20, 0x35, 0x01, // + calldataload(0x20)
    0x5f, 0x35, 0x55, 0x00, // sstore(calldataload(0), ..); stop
];

/// Runtime of the block-hash reader: `h = blockhash(number - calldataload(0))`, then
/// `sstore(0, h)` and `log0(h)`, so the hash it read lands in state and in its receipt.
const BLOCK_HASH_RUNTIME: [u8; 15] = [
    0x5f, 0x35, 0x43, 0x03, 0x40, // blockhash(number - calldataload(0))
    0x80, 0x5f, 0x55, // dup1; sstore(0, h)
    0x5f, 0x52, // mstore(0, h)
    0x60, 0x20, 0x5f, 0xa0, // log0(0, 32)
    0x00, // stop
];

/// Runtime of the CREATE2 factory: `create2(callvalue, calldata, salt 0)`, reverting when the
/// create fails, so the child's address depends only on its init code (the calldata).
const FACTORY_RUNTIME: [u8; 17] = [
    0x36, 0x5f, 0x5f, 0x37, // calldatacopy(0, 0, calldatasize)
    0x5f, 0x36, 0x5f, 0x34, 0xf5, // create2(callvalue, 0, calldatasize, 0)
    0x60, 0x0f, 0x57, // jumpi(0x0f, address)
    0x5f, 0x5f, 0xfd, // revert(0, 0)
    0x5b, 0x00, // jumpdest; stop
];

/// Runtime of the factory's child: `sstore(2, calldataload(0) + sload(1))`.
const CHILD_RUNTIME: [u8; 10] = [
    0x60, 0x01, 0x54, // sload(1)
    0x5f, 0x35, 0x01, // + calldataload(0)
    0x60, 0x02, 0x55, 0x00, // sstore(2, ..); stop
];

/// The store contract (genesis).
const STORE: Address = Address::new([0x5a; 20]);
/// The CREATE2 factory (genesis).
const FACTORY: Address = Address::new([0x5b; 20]);
/// The block-hash reader (genesis).
const HASHER: Address = Address::new([0x5c; 20]);
/// An empty genesis account: a zero-value send clears it under EIP-161.
const EMPTY: Address = Address::new([0x5d; 20]);
/// An address no genesis account holds: a transfer creates it (or only touches it with zero).
const FRESH: Address = Address::new([0x7e; 20]);

/// Recipients a transfer picks from, by index: [`FRESH`], the two senders, [`STORE`], [`EMPTY`]
/// and the two CREATE2 children.
const RECIPIENTS: usize = 7;
/// The CREATE2 children the factory can create, by variant.
const CHILDREN: u8 = 2;
/// Gas limit of every generated call (a CREATE2 child costs about 80k).
const OP_GAS: u64 = 200_000;
/// Gas limit of a bulk transfer to a fresh EOA.
const TRANSFER_GAS: u64 = 21_000;
/// How long any single pipeline event may take before the test fails.
const EVENT_TIMEOUT: Duration = Duration::from_secs(120);

/// One transaction of a generated batch, sent by `from` (0 or 1).
#[derive(Debug, Clone)]
enum Op {
    /// A native transfer of `amount` wei to recipient `to`.
    Transfer { from: usize, to: usize, amount: u64 },
    /// A store-contract call: `sstore(key, value + sload(read))`.
    Store { from: usize, key: u64, value: u64, read: u64 },
    /// A factory call creating child `child` with `value` wei: without value the child
    /// self-destructs in its own creation (created and wiped in one transaction), with value it
    /// stores slot 1 (and the factory reverts when the child already exists).
    Create { from: usize, child: u8, value: u64 },
    /// A call into child `child`: `sstore(2, word + sload(1))` (a plain touch while it does not
    /// exist).
    CallChild { from: usize, child: u8, word: u64 },
    /// A zero-value send to the empty genesis account.
    TouchEmpty { from: usize },
    /// A block-hash reader call for the block `depth` below the current one; at the first block
    /// of an output, depth 1 and more reach into the previous (possibly unpersisted) output.
    BlockHash { from: usize, depth: u64 },
}

/// One batch of an output: a handful of mixed operations, or many transfers to fresh EOAs (a
/// large batch, so outputs differ widely in build and persist time).
#[derive(Debug, Clone)]
enum BatchSpec {
    /// The operations, in order.
    Ops(Vec<Op>),
    /// `count` one-wei transfers from `from`, each to an address no account holds yet.
    Bulk { from: usize, count: usize },
}

/// When the previous output's commit is released relative to this output's build (node B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Schedule {
    /// Released and committed before this output starts: an unseeded build on a durable parent.
    Before,
    /// Released right after this output starts: the commit races the (seeded) build.
    During,
    /// Released after this output has built and parked behind the held commit.
    After,
}

/// One consensus output of a generated case.
#[derive(Debug, Clone)]
struct OutputSpec {
    /// Index of the committee member leading the output (and producing its batches).
    leader: usize,
    /// The output's batches; none is an empty output.
    batches: Vec<BatchSpec>,
    /// When the previous output's commit is released (ignored for the first output).
    schedule: Schedule,
}

/// A generated sequence of consensus outputs.
#[derive(Debug, Clone)]
struct CaseSpec {
    /// The outputs, numbered from 1.
    outputs: Vec<OutputSpec>,
    /// Whether the last output closes the epoch.
    close_last: bool,
}

/// A persist fault armed before one output's commit is released.
#[derive(Debug, Clone, Copy)]
struct FaultSpec {
    /// The consensus number of the output whose persist fails (an output with blocks).
    at: u64,
    /// A late fault (after `save_blocks`, once; the retry is terminal) instead of an early fault
    /// on every attempt.
    late: bool,
}

/// An engine update as compared between the nodes: the leader round, the consensus number and
/// header digest, and the announced block.
type UpdateKey = (Round, u64, ConsensusHeaderDigest, Option<B256>);

/// A check of node B's environment after a case's final comparison.
type FinalCheck<'a> = &'a dyn Fn(&RethEnv) -> Result<(), TestCaseError>;

// ---- generators ----

/// One of the six operations, over small ranges so senders, recipients, slots, children and block
/// hashes repeat within and across blocks and outputs.
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
        2 => (0..2usize, 1..=4u64).prop_map(|(from, depth)| Op::BlockHash { from, depth }),
    ]
}

/// A small mixed batch (1-6 operations) or, one time in seven, a bulk batch of 40-120 transfers.
fn batch() -> impl Strategy<Value = BatchSpec> {
    prop_oneof![
        6 => prop::collection::vec(op(), 1..=6).prop_map(BatchSpec::Ops),
        1 => (0..2usize, 40..=120usize).prop_map(|(from, count)| BatchSpec::Bulk { from, count }),
    ]
}

/// The three release points of the previous commit.
fn schedule() -> impl Strategy<Value = Schedule> {
    prop_oneof![Just(Schedule::Before), Just(Schedule::During), Just(Schedule::After),]
}

/// An output of 0-3 batches (0 = empty) led by one of the four committee members.
fn output_spec() -> impl Strategy<Value = OutputSpec> {
    (0..4usize, prop::collection::vec(batch(), 0..=3), schedule())
        .prop_map(|(leader, batches, schedule)| OutputSpec { leader, batches, schedule })
}

/// 3-6 outputs; the last closes the epoch with probability 1/3.
fn case_spec() -> impl Strategy<Value = CaseSpec> {
    (prop::collection::vec(output_spec(), 3..=6), prop::bool::weighted(1.0 / 3.0))
        .prop_map(|(outputs, close_last)| CaseSpec { outputs, close_last })
}

/// 64 cases unless `PROPTEST_CASES` says otherwise (a case runs two nodes in about half a second
/// to a second, so each property takes under a minute), few shrink steps, no regression files in
/// the source tree.
fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    ProptestConfig {
        cases,
        max_shrink_iters: 16,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    /// The pipeline with commits released before, during or after the next build builds,
    /// commits and announces exactly what the sequential path does, output by output.
    #[test]
    fn prop_pipelined_matches_sequential(case in case_spec()) {
        let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
        let stats = block_on(run_case(&case, None, None))?;
        eprintln!("prop_pipelined_matches_sequential: {stats}");
    }

    /// A persist fault on one output's commit leaves the database, the in-memory chain and the
    /// announced updates at the previous output, spends the pipeline, and a restart replays the
    /// rest to the sequential node's final database.
    #[test]
    fn prop_persist_fault_leaves_durable_prefix(
        case in case_spec(),
        pick in 0..16usize,
        late in any::<bool>(),
    ) {
        let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
        let with_blocks: Vec<u64> = (1..)
            .zip(&case.outputs)
            .filter(|(number, spec)| {
                !spec.batches.is_empty()
                    || (case.close_last && usize::try_from(*number).ok() == Some(case.outputs.len()))
            })
            .map(|(number, _)| number)
            .collect();
        let fault = (!with_blocks.is_empty())
            .then(|| FaultSpec { at: with_blocks[pick % with_blocks.len()], late });
        let stats = block_on(run_case(&case, fault, None))?;
        eprintln!("prop_persist_fault_leaves_durable_prefix: {stats}");
    }
}

// ---- deterministic cases ----

/// A child created and destroyed in one transaction in output N's last block is recreated with
/// a storage write in N+1's first block while N's commit is held (N+1 is seeded with N's wipe),
/// written again in N+2 while N+1 is held, and wiped and recreated inside one output too.
#[test]
fn test_wipe_and_recreate_across_outputs() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let ops = |ops: Vec<Op>| BatchSpec::Ops(ops);
    let case = CaseSpec {
        outputs: vec![
            OutputSpec {
                leader: 0,
                batches: vec![
                    ops(vec![Op::Store { from: 0, key: 1, value: 2, read: 1 }]),
                    ops(vec![Op::Create { from: 1, child: 0, value: 0 }]),
                ],
                schedule: Schedule::Before,
            },
            OutputSpec {
                leader: 1,
                batches: vec![
                    ops(vec![Op::Create { from: 0, child: 0, value: 2 }]),
                    ops(vec![Op::CallChild { from: 1, child: 0, word: 3 }]),
                ],
                schedule: Schedule::After,
            },
            OutputSpec {
                leader: 2,
                batches: vec![ops(vec![
                    Op::CallChild { from: 0, child: 0, word: 5 },
                    Op::Create { from: 1, child: 1, value: 0 },
                    Op::Create { from: 0, child: 1, value: 1 },
                    Op::CallChild { from: 1, child: 1, word: 7 },
                ])],
                schedule: Schedule::During,
            },
            OutputSpec {
                leader: 3,
                batches: vec![ops(vec![Op::CallChild { from: 0, child: 1, word: 9 }])],
                schedule: Schedule::After,
            },
        ],
        close_last: false,
    };
    let stats = block_on(run_case(&case, None, None)).unwrap_or_else(|err| panic!("{err}"));
    assert_eq!(stats.seeded, 3, "outputs 2-4 build on an unpersisted parent: {stats}");
}

/// Output 2 reads, from the first three blocks it builds while output 1 (three blocks) is held,
/// the hash of each of output 1's blocks; the reader logs what it read, and each log equals the
/// hash of the block it names.
#[test]
fn test_blockhash_reads_unpersisted_previous_output() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let transfer = |from| BatchSpec::Ops(vec![Op::Transfer { from, to: 0, amount: 1 }]);
    let read = |depth| BatchSpec::Ops(vec![Op::BlockHash { from: 0, depth }]);
    let case = CaseSpec {
        outputs: vec![
            OutputSpec {
                leader: 0,
                batches: vec![transfer(0), transfer(1), transfer(0)],
                schedule: Schedule::Before,
            },
            // blocks 4, 5 and 6 read blocks 3, 2 and 1
            OutputSpec {
                leader: 1,
                batches: vec![read(1), read(3), read(5)],
                schedule: Schedule::After,
            },
        ],
        close_last: false,
    };
    let check: FinalCheck<'_> = &|env| {
        for (reader, read) in [(4u64, 3u64), (5, 2), (6, 1)] {
            let receipts = env.receipts_by_block(reader.into()).map_err(fail)?.unwrap_or_default();
            let logged = receipts.first().and_then(|receipt| receipt.logs.first());
            let expected = env.sealed_header_by_number(read).map_err(fail)?.map(|h| h.hash());
            prop_assert!(expected.is_some(), "block {} is not committed", read);
            prop_assert_eq!(
                logged.map(|log| B256::from_slice(&log.data.data)),
                expected,
                "block {} read the wrong hash of block {}",
                reader,
                read
            );
        }
        Ok(())
    };
    let stats = block_on(run_case(&case, None, Some(check))).unwrap_or_else(|err| panic!("{err}"));
    assert_eq!(stats.seeded, 1, "output 2 builds on output 1 in memory: {stats}");
}

/// A loaded output, an empty one and an empty closing one: the empty output builds (seeded) and
/// parks behind the held loaded output, the closing output builds behind the held empty output,
/// and from its build until its commit the closing output holds every start (a start is refused
/// and changes nothing). Every block and update equals the sequential node's.
#[test]
fn test_empty_and_closing_outputs_in_pipeline() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let case = CaseSpec {
        outputs: vec![
            OutputSpec {
                leader: 0,
                batches: vec![
                    BatchSpec::Ops(vec![Op::Store { from: 0, key: 1, value: 1, read: 0 }]),
                    BatchSpec::Bulk { from: 1, count: 40 },
                ],
                schedule: Schedule::Before,
            },
            OutputSpec { leader: 1, batches: Vec::new(), schedule: Schedule::After },
            OutputSpec { leader: 2, batches: Vec::new(), schedule: Schedule::After },
        ],
        close_last: true,
    };
    block_on(async {
        let pair = Pair::new(&case).await?;
        let mut driver = Driver::new(&pair.b, &pair.oracle);
        let [first, second, third] = &pair.outputs[..] else {
            return Err(fail("three outputs"));
        };
        driver.start(first, false)?;
        driver.until(Want::PersistStarted(1)).await?;
        driver.start(second, true)?;
        driver.until(Want::Built(2)).await?;
        prop_assert!(!driver.pipeline.can_start(), "the empty output is parked");
        prop_assert_eq!(driver.pipeline.unpersisted_outputs(), 2);
        driver.release();
        driver.until(Want::PersistStarted(2)).await?;
        driver.start(third, false)?;
        driver.until(Want::Built(3)).await?;
        prop_assert_eq!(driver.pipeline.barrier(), Some(3), "the closing output is a barrier");
        driver.release();
        driver.until(Want::PersistStarted(3)).await?;
        prop_assert_eq!(driver.pipeline.barrier(), Some(3), "the barrier holds until the commit");
        prop_assert!(!driver.pipeline.can_start());
        let (leader, producer) = pair.leader(0);
        let next = consensus_output(4, &leader, producer, Vec::new(), false);
        let accounting =
            (pair.b.gas.rewards_counter().get_address_counts(), pair.b.gas.get_values(0));
        prop_assert!(
            matches!(driver.pipeline.start_inline(next), Err(TnEngineError::PipelineInvariant(_))),
            "a start behind the barrier is refused"
        );
        prop_assert_eq!(
            driver.pipeline.unpersisted_outputs(),
            1,
            "the refused start changed state"
        );
        prop_assert_eq!(driver.pipeline.barrier(), Some(3));
        prop_assert_eq!(
            (pair.b.gas.rewards_counter().get_address_counts(), pair.b.gas.get_values(0)),
            accounting,
            "the refused start touched the accumulator"
        );
        driver.release();
        driver.until(Want::Committed(3)).await?;
        prop_assert_eq!(driver.pipeline.barrier(), None);
        prop_assert!(driver.pipeline.can_start(), "the commit lifts the barrier");
        prop_assert!(driver.pipeline.is_idle());
        let announced = driver.finish()?;
        pair.assert_same_chain(&pair.b.reth_env, &pair.b.gas, &announced)
    })
    .unwrap_or_else(|err| panic!("{err}"));
}

/// Every engine update reaches the receiver after its block is readable from the committed
/// database, an empty output's update after every earlier output's block, and the consensus
/// numbers strictly increase, over outputs released before, during and after the next build.
#[test]
fn test_engine_updates_follow_commits() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let mixed = |from| {
        BatchSpec::Ops(vec![
            Op::Transfer { from, to: 0, amount: 2 },
            Op::Store { from, key: 2, value: 1, read: 2 },
        ])
    };
    let spec = |leader, batches, schedule| OutputSpec { leader, batches, schedule };
    let case = CaseSpec {
        outputs: vec![
            spec(0, vec![mixed(0), mixed(1)], Schedule::Before),
            spec(1, vec![BatchSpec::Bulk { from: 0, count: 60 }], Schedule::During),
            spec(2, Vec::new(), Schedule::After),
            spec(3, vec![mixed(1)], Schedule::During),
            spec(0, vec![mixed(0)], Schedule::Before),
            spec(1, vec![mixed(1), mixed(0)], Schedule::After),
        ],
        close_last: true,
    };
    // the update checker runs inside `run_case` and fails it on any violation
    let stats = block_on(run_case(&case, None, None)).unwrap_or_else(|err| panic!("{err}"));
    assert_eq!(stats.updates, 6, "one update per output: {stats}");
}

/// An early fault on every persist attempt of output 2 while output 3 (a bulk output) builds:
/// both are rolled back to output 1, nothing past output 1 is announced, and the same
/// environment replays outputs 2-4 to the sequential node's database.
#[test]
fn test_persist_fault_while_next_output_builds() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let stats = block_on(run_case(&fault_case(), Some(FaultSpec { at: 2, late: false }), None))
        .unwrap_or_else(|err| panic!("{err}"));
    assert_eq!(stats.faults, 1, "{stats}");
}

/// As [`test_persist_fault_while_next_output_builds`] with a late fault: the static files are
/// ahead of the database after the failure, and the reopened datadir heals them and replays.
#[test]
fn test_late_persist_fault_while_next_output_builds() {
    let _guard = IT_TEST_GUARD.lock().unwrap_or_else(PoisonError::into_inner);
    let stats = block_on(run_case(&fault_case(), Some(FaultSpec { at: 2, late: true }), None))
        .unwrap_or_else(|err| panic!("{err}"));
    assert_eq!(stats.faults, 1, "{stats}");
}

/// Four outputs; output 3 is a bulk output that starts while output 2's commit is held and has
/// its commit released right after it starts.
fn fault_case() -> CaseSpec {
    let mixed = |from| {
        BatchSpec::Ops(vec![
            Op::Store { from, key: 1, value: 1, read: 1 },
            Op::Create { from, child: 0, value: 0 },
            Op::Transfer { from, to: 4, amount: 0 },
        ])
    };
    CaseSpec {
        outputs: vec![
            OutputSpec { leader: 0, batches: vec![mixed(0)], schedule: Schedule::Before },
            OutputSpec { leader: 1, batches: vec![mixed(1), mixed(0)], schedule: Schedule::After },
            OutputSpec {
                leader: 2,
                batches: vec![
                    BatchSpec::Bulk { from: 0, count: 120 },
                    BatchSpec::Bulk { from: 1, count: 120 },
                ],
                schedule: Schedule::During,
            },
            OutputSpec { leader: 3, batches: vec![mixed(1)], schedule: Schedule::After },
        ],
        close_last: true,
    }
}

// ---- one case: oracle, pipelined drive, comparison, fault and restart ----

/// What one case exercised, printed per case.
#[derive(Debug, Default)]
struct CaseStats {
    /// Outputs in the case.
    outputs: usize,
    /// Blocks the oracle built.
    blocks: u64,
    /// Outputs per release schedule (first output excluded): before, during, after the build.
    schedules: [usize; 3],
    /// Builds seeded with the committing output's deltas.
    seeded: usize,
    /// Commits that landed while the next output was still building.
    mid_build_commits: usize,
    /// Engine updates node B announced.
    updates: usize,
    /// Persist faults injected.
    faults: usize,
    /// Whether the next output's build was reported before the fault settled.
    next_built_before_fault: bool,
}

impl std::fmt::Display for CaseStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "outputs={} blocks={} schedules(first/during/after)={:?} seeded={} \
             mid_build_commits={} updates={} faults={} next_built_before_fault={}",
            self.outputs,
            self.blocks,
            self.schedules,
            self.seeded,
            self.mid_build_commits,
            self.updates,
            self.faults,
            self.next_built_before_fault
        )
    }
}

/// Run `case` on the oracle and through the pipeline (with `fault`, if any), compare, and run
/// `final_check` on node B's environment at the end.
async fn run_case(
    case: &CaseSpec,
    fault: Option<FaultSpec>,
    final_check: Option<FinalCheck<'_>>,
) -> Result<CaseStats, TestCaseError> {
    let pair = Pair::new(case).await?;
    let mut stats = CaseStats {
        outputs: pair.outputs.len(),
        blocks: pair.oracle.after.last().map_or(0, |snapshot| snapshot.tip.number),
        ..CaseStats::default()
    };
    for spec in case.outputs.iter().skip(1) {
        stats.schedules[spec.schedule as usize] += 1;
    }

    let mut driver = Driver::new(&pair.b, &pair.oracle);
    let outcome = driver.drive(&pair.outputs, &case.outputs, fault).await?;
    stats.seeded = driver.built.values().filter(|report| report.seeded).count();
    stats.mid_build_commits = driver.mid_build_commits(&case.outputs);
    prop_assert_eq!(fault.is_some(), outcome.is_some(), "an armed fault did not fail the pipeline");

    let Some((fault, failure)) = fault.zip(outcome) else {
        let announced = driver.finish()?;
        stats.updates = announced.updates.len();
        pair.assert_same_chain(&pair.b.reth_env, &pair.b.gas, &announced)?;
        if let Some(check) = final_check {
            check(&pair.b.reth_env)?;
        }
        return Ok(stats);
    };
    stats.faults = 1;
    stats.next_built_before_fault = driver.built.contains_key(&(fault.at + 1));

    // the documented outcome: the provider error, the attempts the retry policy allows, nothing
    // durable, in memory or announced past the previous output, and a spent pipeline
    let Pair { chain: _, a: _, b, outputs, oracle } = &pair;
    prop_assert!(
        matches!(failure.error, TnEngineError::Reth(TnRethError::Provider(_))),
        "unexpected error {:?}",
        failure.error
    );
    let attempts = if fault.late { 2 } else { PERSIST_OUTPUT_ATTEMPTS };
    prop_assert_eq!(b.reth_env.persist_attempt_count(), failure.attempts_before + attempts);
    let durable = oracle.tip_before(fault.at).clone();
    pair.assert_durable_prefix(&b.reth_env, fault.at)?;
    prop_assert_eq!(driver.pipeline.durable_tip().hash(), durable.hash());
    prop_assert_eq!(driver.pipeline.in_memory_tip().hash(), durable.hash());
    prop_assert!(driver.pipeline.is_idle(), "a failed pipeline settles");
    prop_assert!(!driver.pipeline.can_start(), "a failed pipeline is spent");
    let next = outputs[usize::try_from(fault.at).map_err(fail)? - 1].clone();
    prop_assert!(
        matches!(driver.pipeline.start_inline(next), Err(TnEngineError::PipelineInvariant(_))),
        "a failed pipeline refuses a start"
    );
    let prefix = driver.finish()?;
    prop_assert_eq!(
        &prefix,
        &oracle.announced_before(fault.at)?,
        "announced past the durable output"
    );

    // restart: the same environment after an early fault, the reopened datadir after a late one
    let Pair { chain, a, b, outputs, oracle } = pair;
    let b = if fault.late { b.reopen(&chain).await.map_err(fail)? } else { b };
    let pair = Pair { chain, a, b, outputs, oracle };
    if fault.late {
        // the reopened datadir (reth's startup consistency check ran) holds the same prefix
        pair.assert_durable_prefix(&pair.b.reth_env, fault.at)?;
    }
    finish_after_fault(pair, fault, prefix, stats, final_check).await
}

/// Rebuild node B's accumulator from its durable prefix, replay the outputs from the failed one
/// on, and compare the final chain with the oracle's.
async fn finish_after_fault(
    pair: Pair,
    fault: FaultSpec,
    prefix: Announced,
    mut stats: CaseStats,
    final_check: Option<FinalCheck<'_>>,
) -> Result<CaseStats, TestCaseError> {
    let failed = usize::try_from(fault.at).map_err(fail)? - 1;
    let durable = pair.b.reth_env.lookup_head().map_err(fail)?;
    // what the node's restart rebuilds from the finalized tip (`catchup_accumulator`): the
    // leader counts of the durable outputs and the gas of their blocks
    pair.b.gas.clear();
    for output in &pair.outputs[..failed] {
        pair.b.gas.rewards_counter().inc_leader_count(output.leader().author());
    }
    for number in 1..=durable.number {
        let header = pair.b.reth_env.sealed_header_by_number(number).map_err(fail)?;
        let header = header.ok_or_else(|| fail(format!("block {number} missing")))?;
        pair.b.gas.inc_block(0, header.gas_used, header.gas_limit);
    }
    let replayed = replay(&pair.b, durable, &pair.outputs[failed..]).await?;
    let mut announced = prefix;
    announced.updates.extend(replayed.updates);
    announced.canon.extend(replayed.canon);
    stats.updates = announced.updates.len();
    pair.assert_same_chain(&pair.b.reth_env, &pair.b.gas, &announced)?;
    if let Some(check) = final_check {
        check(&pair.b.reth_env)?;
    }
    Ok(stats)
}

/// Run `outputs` through a fresh pipeline on `node` from `durable`, without a gate: each output
/// starts as soon as the pipeline allows. Returns what it announced (checked).
async fn replay(
    node: &Node,
    durable: SealedHeader,
    outputs: &[ConsensusOutput],
) -> Result<Announced, TestCaseError> {
    let (watch, tx) = Watch::new(&node.reth_env);
    let mut pipeline = PersistPipeline::new(
        node.reth_env.clone(),
        durable,
        node.tasks.get_spawner(),
        node.gas.clone(),
        RepackMonitor::default(),
        tx,
    );
    for output in outputs {
        while !pipeline.can_start() {
            next_event(&mut pipeline).await?.map_err(fail)?;
        }
        pipeline.start_inline(output.clone()).map_err(fail)?;
    }
    while !pipeline.is_idle() {
        next_event(&mut pipeline).await?.map_err(fail)?;
    }
    drop(pipeline);
    watch.finish()
}

/// The next pipeline event, failing the case after [`EVENT_TIMEOUT`].
async fn next_event(
    pipeline: &mut PersistPipeline,
) -> Result<Result<PipelineEvent, TnEngineError>, TestCaseError> {
    tokio::time::timeout(EVENT_TIMEOUT, pipeline.next_event())
        .await
        .map_err(|_| fail("no pipeline event within the timeout"))
}

/// The chain, both nodes, the outputs and the oracle's run of them.
struct Pair {
    /// The genesis both nodes start from.
    chain: Arc<RethChainSpec>,
    /// The sequential node (the oracle), already run.
    a: Node,
    /// The pipelined node, untouched.
    b: Node,
    /// The case's consensus outputs, numbered from 1.
    outputs: Vec<ConsensusOutput>,
    /// What the oracle built and announced.
    oracle: OracleRun,
}

impl Pair {
    /// Build the chain and the outputs of `case`, start both nodes and run the oracle.
    async fn new(case: &CaseSpec) -> Result<Self, TestCaseError> {
        let senders = [TransactionFactory::new(), TransactionFactory::new_random()];
        let chain = chain_for([senders[0].address(), senders[1].address()]);
        let a = Node::new(&chain).await.map_err(fail)?;
        let b = Node::new(&chain).await.map_err(fail)?;
        let mut signers = Signers { chain: chain.clone(), senders, fresh: 0 };
        let (outputs, expected_txs) = build_outputs(case, &mut signers, &a.committee);
        let oracle = OracleRun::run(&a, &chain, &outputs).await?;
        let built_txs: Vec<usize> =
            oracle.blocks.iter().map(|block| block.body().transactions.len()).collect();
        prop_assert_eq!(built_txs, expected_txs, "the oracle left out a generated transaction");
        Ok(Self { chain, a, b, outputs, oracle })
    }

    /// Committee member `index`'s identifier and execution address.
    fn leader(&self, index: usize) -> (AuthorityIdentifier, Address) {
        leader(&self.a.committee, index)
    }

    /// `env`'s database holds exactly what the oracle held before output `failed` (no header past
    /// it is readable either), and nothing built after it is in memory.
    fn assert_durable_prefix(&self, env: &RethEnv, failed: u64) -> Result<(), TestCaseError> {
        let durable = self.oracle.tip_before(failed);
        let digest = self.oracle.digest_before(failed);
        prop_assert_eq!(env.lookup_head().map_err(fail)?.hash(), durable.hash(), "durable head");
        prop_assert_eq!(env.last_block_number().map_err(fail)?, durable.number);
        prop_assert_eq!(env.trie_tables_digest().map_err(fail)?, digest, "durable trie tables");
        prop_assert!(
            env.sealed_header_by_number(durable.number + 1).map_err(fail)?.is_none(),
            "a header past the durable output is in the database"
        );
        let cims = env.canonical_in_memory_state();
        prop_assert_eq!(cims.canonical_chain().count(), 0, "unpersisted blocks are in memory");
        prop_assert_eq!(env.canonical_tip().hash(), durable.hash(), "in-memory head");
        for block in self.oracle.blocks.iter().filter(|block| block.number > durable.number) {
            prop_assert!(
                cims.state_by_hash(block.hash()).is_none(),
                "rolled-back block {} is still in memory",
                block.number
            );
        }
        Ok(())
    }

    /// Node B's chain (on `env`, with accumulator `gas`, having announced `announced`) equals
    /// the oracle's: every block, its receipts, the trie tables, the in-memory state, the engine
    /// updates and canon notifications, and the accumulator.
    fn assert_same_chain(
        &self,
        env: &RethEnv,
        gas: &GasAccumulator,
        announced: &Announced,
    ) -> Result<(), TestCaseError> {
        let a = &self.a.reth_env;
        let last = self.oracle.blocks.len() as u64;
        prop_assert_eq!(env.last_block_number().map_err(fail)?, last, "committed height");
        for expected in &self.oracle.blocks {
            let number = expected.number;
            let block = env.sealed_block_by_number(number).map_err(fail)?;
            let block = block.ok_or_else(|| fail(format!("block {number} missing")))?;
            prop_assert_eq!(block.hash(), expected.hash(), "block hash at {}", number);
            prop_assert_eq!(block.state_root, expected.state_root, "state root at {}", number);
            prop_assert_eq!(block.receipts_root, expected.receipts_root, "receipts at {}", number);
            prop_assert_eq!(block.gas_used, expected.gas_used, "gas used at {}", number);
            prop_assert_eq!(&block, expected, "sealed block at {}", number);
            prop_assert_eq!(
                env.receipts_by_block(number.into()).map_err(fail)?,
                a.receipts_by_block(number.into()).map_err(fail)?,
                "receipts of block {}",
                number
            );
        }
        prop_assert_eq!(
            env.trie_tables_digest().map_err(fail)?,
            a.trie_tables_digest().map_err(fail)?,
            "trie tables"
        );
        let tip = self.oracle.after.last().map_or(&self.oracle.genesis, |snapshot| &snapshot.tip);
        prop_assert_eq!(env.lookup_head().map_err(fail)?.hash(), tip.hash(), "committed head");
        prop_assert_eq!(env.canonical_tip().hash(), tip.hash(), "in-memory head");
        prop_assert_eq!(env.canonical_in_memory_state().canonical_chain().count(), 0);
        prop_assert_eq!(&announced.updates, &self.oracle.announced.updates, "engine updates");
        prop_assert_eq!(&announced.canon, &self.oracle.announced.canon, "canon notifications");
        prop_assert_eq!(
            gas.rewards_counter().get_address_counts(),
            self.a.gas.rewards_counter().get_address_counts(),
            "leader counts"
        );
        prop_assert_eq!(gas.get_values(0), self.a.gas.get_values(0), "worker gas totals");
        Ok(())
    }
}

/// One execution node on a temp datadir, its committee installed in the accumulator.
struct Node {
    /// The node's execution environment.
    reth_env: RethEnv,
    /// Reward accounting (shared with the environment's EVM config).
    gas: GasAccumulator,
    /// The genesis committee.
    committee: Committee,
    /// Spawns the pipeline's tasks.
    tasks: TaskManager,
    /// The datadir.
    dir: TempDir,
}

impl Node {
    /// A node on a new datadir.
    async fn new(chain: &Arc<RethChainSpec>) -> eyre::Result<Self> {
        Self::open(chain, TempDir::new()?).await
    }

    /// A node on `dir`, a new or an existing datadir (reth's startup consistency check runs).
    async fn open(chain: &Arc<RethChainSpec>, dir: TempDir) -> eyre::Result<Self> {
        let tasks = TaskManager::new("persist pipeline tests");
        let gas = GasAccumulator::new(1);
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), dir.path(), &tasks, Some(gas.clone()))?;
        let committee =
            create_committee_from_state(reth_env.epoch_state_from_canonical_tip()?).await?;
        gas.rewards_counter().set_committee(committee.clone());
        Ok(Self { reth_env, gas, committee, tasks, dir })
    }

    /// Stop the node and open its datadir again: a restart.
    async fn reopen(self, chain: &Arc<RethChainSpec>) -> eyre::Result<Self> {
        let Self { reth_env, gas, committee: _, tasks, dir } = self;
        drop(reth_env);
        drop(gas);
        drop(tasks);
        Self::open(chain, dir).await
    }
}

/// The oracle's state after one output.
#[derive(Debug, Clone)]
struct Snapshot {
    /// The last block after the output.
    tip: SealedHeader,
    /// The committed trie tables after the output.
    digest: B256,
}

/// What the sequential node built and announced.
struct OracleRun {
    /// The genesis header.
    genesis: SealedHeader,
    /// The trie tables before any output.
    genesis_digest: B256,
    /// The state after output `n`, at index `n - 1`.
    after: Vec<Snapshot>,
    /// Every block, at index `number - 1`.
    blocks: Vec<SealedBlock>,
    /// The engine updates and canon notifications, in order.
    announced: Announced,
}

impl OracleRun {
    /// Execute `outputs` on `node` through the sequential path, one at a time on a blocking
    /// thread, snapshotting the committed state after each.
    async fn run(
        node: &Node,
        chain: &Arc<RethChainSpec>,
        outputs: &[ConsensusOutput],
    ) -> Result<Self, TestCaseError> {
        let genesis = chain.sealed_genesis_header();
        let genesis_digest = node.reth_env.trie_tables_digest().map_err(fail)?;
        let (watch, tx) = Watch::new(&node.reth_env);
        let mut parent = genesis.clone();
        let mut after = Vec::with_capacity(outputs.len());
        for output in outputs {
            let args = BuildArguments::new(node.reth_env.clone(), output.clone(), parent);
            let (gas, tx) = (node.gas.clone(), tx.clone());
            parent = tokio::task::spawn_blocking(move || {
                execute_consensus_output(args, gas, RepackMonitor::default(), tx)
            })
            .await
            .map_err(fail)?
            .map_err(fail)?;
            let digest = node.reth_env.trie_tables_digest().map_err(fail)?;
            after.push(Snapshot { tip: parent.clone(), digest });
        }
        drop(tx);
        let announced = watch.finish()?;
        let mut blocks = Vec::new();
        for number in 1..=parent.number {
            let block = node.reth_env.sealed_block_by_number(number).map_err(fail)?;
            blocks.push(block.ok_or_else(|| fail(format!("oracle block {number} missing")))?);
        }
        Ok(Self { genesis, genesis_digest, after, blocks, announced })
    }

    /// What the oracle announced for the outputs before output `number`.
    fn announced_before(&self, number: u64) -> Result<Announced, TestCaseError> {
        let updates: Vec<UpdateKey> =
            self.announced.updates.iter().filter(|(_, n, _, _)| *n < number).cloned().collect();
        let mut with_blocks = 0;
        for output in 1..number {
            if self.blocks_of(output)? > 0 {
                with_blocks += 1;
            }
        }
        let canon = self.announced.canon.get(..with_blocks).ok_or_else(|| fail("canon"))?.to_vec();
        Ok(Announced { updates, canon })
    }

    /// The last block before output `number`.
    fn tip_before(&self, number: u64) -> &SealedHeader {
        match number.checked_sub(2).and_then(|index| usize::try_from(index).ok()) {
            Some(index) => &self.after[index].tip,
            None => &self.genesis,
        }
    }

    /// The trie tables before output `number`.
    fn digest_before(&self, number: u64) -> B256 {
        match number.checked_sub(2).and_then(|index| usize::try_from(index).ok()) {
            Some(index) => self.after[index].digest,
            None => self.genesis_digest,
        }
    }

    /// The state after output `number`.
    fn after(&self, number: u64) -> Result<&Snapshot, TestCaseError> {
        usize::try_from(number)
            .ok()
            .and_then(|number| number.checked_sub(1))
            .and_then(|index| self.after.get(index))
            .ok_or_else(|| fail(format!("no output {number}")))
    }

    /// The blocks output `number` built.
    fn blocks_of(&self, number: u64) -> Result<u64, TestCaseError> {
        Ok(self.after(number)?.tip.number - self.tip_before(number).number)
    }

    /// The oracle's block at height `number`.
    fn block_hash(&self, number: u64) -> Option<B256> {
        let index = usize::try_from(number.checked_sub(1)?).ok()?;
        self.blocks.get(index).map(|block| block.hash())
    }
}

/// A pipeline event the driver waits for.
#[derive(Debug, Clone, Copy)]
enum Want {
    /// The output finished building.
    Built(u64),
    /// The output's commit task was spawned.
    PersistStarted(u64),
    /// The output committed.
    Committed(u64),
}

/// A pipeline failure the driver saw.
struct Failure {
    /// The error `next_event` returned.
    error: TnEngineError,
    /// The persist attempts counted before the fault was armed.
    attempts_before: u32,
}

/// Node B's pipeline with every commit held at the gate, checked against the oracle at every
/// event.
struct Driver<'a> {
    /// Node B.
    node: &'a Node,
    /// The oracle's run of the same outputs.
    oracle: &'a OracleRun,
    /// The pipeline under test.
    pipeline: PersistPipeline,
    /// Holds every commit until released, one per [`Self::release`].
    gate: CommitGate,
    /// The checker of the pipeline's engine updates and canon notifications.
    watch: Watch,
    /// Outputs started.
    started: BTreeSet<u64>,
    /// Whether each started output was expected to build seeded.
    expect_seeded: BTreeMap<u64, bool>,
    /// Build reports seen.
    built: BTreeMap<u64, BuildReport>,
    /// Outputs whose commit was dispatched, in order.
    dispatched: Vec<u64>,
    /// Commit reports seen, in order.
    committed: Vec<CommitReport>,
}

impl<'a> Driver<'a> {
    /// A gated pipeline on `node`'s genesis.
    fn new(node: &'a Node, oracle: &'a OracleRun) -> Self {
        let (watch, tx) = Watch::new(&node.reth_env);
        let mut pipeline = PersistPipeline::new(
            node.reth_env.clone(),
            oracle.genesis.clone(),
            node.tasks.get_spawner(),
            node.gas.clone(),
            RepackMonitor::default(),
            tx,
        );
        let gate = pipeline.hold_commits();
        Self {
            node,
            oracle,
            pipeline,
            gate,
            watch,
            started: BTreeSet::new(),
            expect_seeded: BTreeMap::new(),
            built: BTreeMap::new(),
            dispatched: Vec::new(),
            committed: Vec::new(),
        }
    }

    /// Run every output, releasing each held commit at the point the next output's schedule
    /// names; with `fault`, arm it right before the faulty output's commit is released and
    /// return the failure.
    async fn drive(
        &mut self,
        outputs: &[ConsensusOutput],
        specs: &[OutputSpec],
        fault: Option<FaultSpec>,
    ) -> Result<Option<Failure>, TestCaseError> {
        for (number, (output, spec)) in (1u64..).zip(outputs.iter().zip(specs)) {
            if number == 1 {
                self.start(output, false)?;
                self.until(Want::PersistStarted(1)).await?;
                continue;
            }
            let previous = number - 1;
            let faulty = fault.filter(|fault| fault.at == previous);
            let seeded = self.oracle.blocks_of(previous)? > 0;
            match spec.schedule {
                Schedule::Before => {
                    if let Some(fault) = faulty {
                        return self.release_into_fault(fault).await.map(Some);
                    }
                    self.release();
                    self.until(Want::Committed(previous)).await?;
                    self.start(output, false)?;
                }
                Schedule::During => {
                    self.start(output, seeded)?;
                    if let Some(fault) = faulty {
                        return self.release_into_fault(fault).await.map(Some);
                    }
                    self.release();
                }
                Schedule::After => {
                    self.start(output, seeded)?;
                    self.until(Want::Built(number)).await?;
                    if let Some(fault) = faulty {
                        return self.release_into_fault(fault).await.map(Some);
                    }
                    self.release();
                }
            }
            self.until(Want::PersistStarted(number)).await?;
        }
        let last = outputs.len() as u64;
        if let Some(fault) = fault.filter(|fault| fault.at == last) {
            return self.release_into_fault(fault).await.map(Some);
        }
        self.release();
        self.until(Want::Committed(last)).await?;
        prop_assert!(self.pipeline.is_idle(), "the pipeline is idle after the last commit");
        prop_assert_eq!(self.pipeline.unpersisted_outputs(), 0);
        prop_assert_eq!(self.pipeline.in_memory_tip().hash(), self.pipeline.durable_tip().hash());
        Ok(None)
    }

    /// Start `output`, which must be startable, expecting a seeded build when `seeded`.
    fn start(&mut self, output: &ConsensusOutput, seeded: bool) -> Result<(), TestCaseError> {
        let number = output.number();
        prop_assert!(self.pipeline.can_start(), "output {} cannot start", number);
        self.pipeline.start_inline(output.clone()).map_err(fail)?;
        self.started.insert(number);
        self.expect_seeded.insert(number, seeded);
        Ok(())
    }

    /// Let the next held commit start.
    fn release(&self) {
        self.gate.release_one();
    }

    /// Arm `fault`, release the faulty commit and wait for the pipeline's error. No output at or
    /// after the faulty one may commit on the way.
    async fn release_into_fault(&mut self, fault: FaultSpec) -> Result<Failure, TestCaseError> {
        let env = &self.node.reth_env;
        let attempts_before = env.persist_attempt_count();
        if fault.late {
            env.inject_late_persist_provider_faults(1);
        } else {
            env.inject_persist_provider_faults(PERSIST_OUTPUT_ATTEMPTS);
        }
        self.release();
        loop {
            match next_event(&mut self.pipeline).await? {
                Ok(event) => {
                    if let PipelineEvent::Committed(report) = &event {
                        prop_assert!(
                            report.consensus_number < fault.at,
                            "output {} committed through the fault",
                            report.consensus_number
                        );
                    }
                    self.check(event)?;
                }
                Err(error) => return Ok(Failure { error, attempts_before }),
            }
        }
    }

    /// Pump events (checking each) until `want` has been seen.
    async fn until(&mut self, want: Want) -> Result<(), TestCaseError> {
        loop {
            let seen = match want {
                Want::Built(number) => self.built.contains_key(&number),
                Want::PersistStarted(number) => self.dispatched.contains(&number),
                Want::Committed(number) => {
                    self.committed.iter().any(|report| report.consensus_number == number)
                }
            };
            if seen {
                return Ok(());
            }
            let event = next_event(&mut self.pipeline)
                .await?
                .map_err(|err| fail(format!("pipeline failed waiting for {want:?}: {err:?}")))?;
            self.check(event)?;
        }
    }

    /// Check one event against the oracle and record it.
    fn check(&mut self, event: PipelineEvent) -> Result<(), TestCaseError> {
        match event {
            PipelineEvent::Built(report) => {
                let number = report.consensus_number;
                let expected = &self.oracle.after(number)?.tip;
                prop_assert_eq!(report.tip.hash(), expected.hash(), "output {} built", number);
                prop_assert_eq!(report.blocks as u64, self.oracle.blocks_of(number)?);
                prop_assert_eq!(
                    Some(report.seeded),
                    self.expect_seeded.get(&number).copied(),
                    "output {} seeded",
                    number
                );
                prop_assert_eq!(
                    self.node.reth_env.canonical_tip().hash(),
                    report.tip.hash(),
                    "in-memory head after output {} built",
                    number
                );
                self.built.insert(number, *report);
            }
            PipelineEvent::PersistStarted { consensus_number, .. } => {
                prop_assert_eq!(
                    consensus_number,
                    self.dispatched.len() as u64 + 1,
                    "commits dispatched out of order"
                );
                prop_assert!(self.built.contains_key(&consensus_number), "dispatched unbuilt");
                self.dispatched.push(consensus_number);
            }
            PipelineEvent::Committed(report) => {
                self.check_committed(&report)?;
                self.committed.push(*report);
            }
        }
        Ok(())
    }

    /// After output `n` committed (every later commit is still held): the committed head and
    /// trie tables are the oracle's after `n`, `n`'s blocks left memory, and every block still
    /// in memory is unpersisted, resolvable and the oracle's; with `n + 1` built, the in-memory
    /// chain is exactly its blocks.
    fn check_committed(&self, report: &CommitReport) -> Result<(), TestCaseError> {
        let number = report.consensus_number;
        let env = &self.node.reth_env;
        prop_assert_eq!(number, self.committed.len() as u64 + 1, "commits out of order");
        let expected = self.oracle.after(number)?;
        prop_assert_eq!(report.tip.hash(), expected.tip.hash(), "output {} committed", number);
        prop_assert_eq!(report.parent.hash(), self.oracle.tip_before(number).hash());
        prop_assert_eq!(env.lookup_head().map_err(fail)?.hash(), expected.tip.hash());
        if report.blocks > 0 {
            prop_assert_eq!(
                env.finalized_header().map_err(fail)?.map(|header| header.hash()),
                Some(expected.tip.hash()),
                "finalized marker after output {}",
                number
            );
        }
        prop_assert_eq!(
            env.trie_tables_digest().map_err(fail)?,
            expected.digest,
            "trie tables after output {}",
            number
        );
        prop_assert_eq!(self.pipeline.durable_tip().hash(), expected.tip.hash());

        let cims = env.canonical_in_memory_state();
        for persisted in (report.parent.number + 1)..=report.tip.number {
            let hash = self.oracle.block_hash(persisted).ok_or_else(|| fail("oracle block"))?;
            prop_assert!(
                cims.state_by_hash(hash).is_none(),
                "persisted block {} is still in memory",
                persisted
            );
        }
        let mut in_memory = 0;
        for state in cims.canonical_chain() {
            in_memory += 1;
            prop_assert!(
                state.number() > expected.tip.number,
                "block {} is in memory at or below the durable tip {}",
                state.number(),
                expected.tip.number
            );
            prop_assert_eq!(
                Some(state.hash()),
                self.oracle.block_hash(state.number()),
                "in-memory block {}",
                state.number()
            );
            prop_assert!(
                env.state_by_block_hash_for_test(state.hash()).is_ok(),
                "in-memory block {} does not resolve",
                state.number()
            );
        }
        let next = number + 1;
        if let Some(built) = self.built.get(&next) {
            prop_assert_eq!(in_memory, built.blocks, "the in-memory chain is output {}", next);
            prop_assert_eq!(env.canonical_tip().hash(), built.tip.hash());
        } else if !self.started.contains(&next) {
            prop_assert_eq!(in_memory, 0, "nothing builds after output {}", number);
            prop_assert_eq!(env.canonical_tip().hash(), expected.tip.hash());
        }
        Ok(())
    }

    /// Commits of an output released during the next build that landed before that build ended.
    fn mid_build_commits(&self, specs: &[OutputSpec]) -> usize {
        self.committed
            .windows(2)
            .filter(|pair| {
                let next = pair[1].consensus_number;
                let during = usize::try_from(next)
                    .ok()
                    .and_then(|next| specs.get(next - 1))
                    .is_some_and(|spec| spec.schedule == Schedule::During);
                during && pair[0].committed_at < pair[1].build_started + pair[1].execute
            })
            .count()
    }

    /// Drop the pipeline and the gate and return what the pipeline announced (checked).
    fn finish(self) -> Result<Announced, TestCaseError> {
        let Self { pipeline, gate, watch, .. } = self;
        drop(pipeline);
        drop(gate);
        watch.finish()
    }
}

/// What a node announced: its engine updates and the tips of its canon notifications.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Announced {
    /// The engine updates, in order.
    updates: Vec<UpdateKey>,
    /// The tip of every canonical-state notification, in order.
    canon: Vec<B256>,
}

/// Watches what a node announces from now on: an [`UpdateChecker`] on a new engine update
/// channel and a subscription to the node's canon notifications.
struct Watch {
    /// The engine update checker.
    checker: UpdateChecker,
    /// Drains the canon notifications received so far into their tips.
    canon: Box<dyn FnOnce() -> Vec<B256> + Send>,
}

impl Watch {
    /// Start watching `env`; the returned sender is the engine update channel to announce on.
    fn new(env: &RethEnv) -> (Self, mpsc::Sender<EngineUpdate>) {
        let (tx, rx) = mpsc::channel(256);
        let checker = UpdateChecker::spawn(env.clone(), rx);
        let mut notifications = env.canonical_in_memory_state().subscribe_canon_state();
        let canon = Box::new(move || {
            std::iter::from_fn(|| notifications.try_recv().ok())
                .map(|notification| notification.tip().hash())
                .collect()
        });
        (Self { checker, canon }, tx)
    }

    /// What was announced, once every sender of the update channel is gone.
    fn finish(self) -> Result<Announced, TestCaseError> {
        let updates = self.checker.finish()?;
        Ok(Announced { updates, canon: (self.canon)() })
    }
}

/// Receives the pipeline's engine updates on its own thread and checks each at receipt: its
/// block is readable from the committed database (an empty output's update: every earlier
/// update's block is), and the consensus numbers strictly increase.
struct UpdateChecker {
    /// The receiving thread.
    handle: std::thread::JoinHandle<(Vec<UpdateKey>, Vec<String>)>,
}

impl UpdateChecker {
    /// Check every update `rx` receives against `env`'s committed database.
    fn spawn(env: RethEnv, mut rx: mpsc::Receiver<EngineUpdate>) -> Self {
        let handle = std::thread::spawn(move || {
            let mut updates = Vec::new();
            let mut violations = Vec::new();
            let mut last_number: Option<u64> = None;
            let mut announced = 0u64;
            while let Some(update) = rx.blocking_recv() {
                let (_, num_hash, tip) = &update;
                if last_number.is_some_and(|last| num_hash.number <= last) {
                    violations.push(format!("update {} after {last_number:?}", num_hash.number));
                }
                last_number = Some(num_hash.number);
                match tip {
                    Some(tip) => match env.lookup_head() {
                        Ok(head)
                            if head.number >= tip.number
                                && env.sealed_header_by_number(tip.number).is_ok_and(
                                    |header| {
                                        header.is_some_and(|header| header.hash() == tip.hash())
                                    },
                                ) =>
                        {
                            announced = tip.number
                        }
                        other => violations.push(format!(
                            "output {} announced block {} before it was committed: {other:?}",
                            num_hash.number, tip.number
                        )),
                    },
                    None => {
                        if !env.lookup_head().is_ok_and(|head| head.number >= announced) {
                            violations.push(format!(
                                "empty output {} announced before block {announced} committed",
                                num_hash.number
                            ));
                        }
                    }
                }
                updates.push(update_key(update));
            }
            (updates, violations)
        });
        Self { handle }
    }

    /// Wait for the channel to close and return the updates, failing on any violation.
    fn finish(self) -> Result<Vec<UpdateKey>, TestCaseError> {
        let (updates, violations) =
            self.handle.join().map_err(|_| fail("the update checker panicked"))?;
        prop_assert!(violations.is_empty(), "engine update violations: {:?}", violations);
        Ok(updates)
    }
}

/// Signs the generated operations in execution order, so each sender's nonces follow its
/// transactions through the outputs.
struct Signers {
    /// The chain the transactions are for.
    chain: Arc<RethChainSpec>,
    /// The two funded senders.
    senders: [TransactionFactory; 2],
    /// The last fresh recipient handed out.
    fresh: u64,
}

impl Signers {
    /// Sign and encode a call from sender `from`.
    fn tx(&mut self, from: usize, to: Address, value: u64, input: Bytes, gas: u64) -> Vec<u8> {
        self.senders[from].create_eip1559_encoded(
            self.chain.clone(),
            Some(gas),
            u128::from(MIN_PROTOCOL_BASE_FEE),
            Some(to),
            U256::from(value),
            input,
        )
    }

    /// Sign and encode `op`.
    fn op(&mut self, op: &Op) -> Vec<u8> {
        let recipients = [
            FRESH,
            self.senders[0].address(),
            self.senders[1].address(),
            STORE,
            EMPTY,
            child(0),
            child(1),
        ];
        let word = |value: u64| B256::from(U256::from(value)).0;
        let (from, to, value, input): (usize, Address, u64, Bytes) = match *op {
            Op::Transfer { from, to, amount } => (from, recipients[to], amount, Bytes::new()),
            Op::Store { from, key, value, read } => {
                (from, STORE, 0, [word(key), word(value), word(read)].concat().into())
            }
            Op::Create { from, child, value } => {
                (from, FACTORY, value, child_init_code(child).into())
            }
            Op::CallChild { from, child: variant, word: w } => {
                (from, child(variant), 0, word(w).to_vec().into())
            }
            Op::TouchEmpty { from } => (from, EMPTY, 0, Bytes::new()),
            Op::BlockHash { from, depth } => (from, HASHER, 0, word(depth).to_vec().into()),
        };
        self.tx(from, to, value, input, OP_GAS)
    }

    /// Sign and encode `count` one-wei transfers from `from`, each to a new address.
    fn bulk(&mut self, from: usize, count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|_| {
                self.fresh += 1;
                let to = Address::from_word(keccak256(self.fresh.to_be_bytes()));
                self.tx(from, to, 1, Bytes::new(), TRANSFER_GAS)
            })
            .collect()
    }
}

/// The case's consensus outputs and the transaction count of every block they build, in order.
fn build_outputs(
    case: &CaseSpec,
    signers: &mut Signers,
    committee: &Committee,
) -> (Vec<ConsensusOutput>, Vec<usize>) {
    let mut outputs = Vec::with_capacity(case.outputs.len());
    let mut expected_txs = Vec::new();
    for (number, spec) in (1u64..).zip(&case.outputs) {
        let (leader, producer) = leader(committee, spec.leader);
        let batches: Vec<Batch> = spec
            .batches
            .iter()
            .map(|batch| {
                let transactions = match batch {
                    BatchSpec::Ops(ops) => ops.iter().map(|op| signers.op(op)).collect(),
                    BatchSpec::Bulk { from, count } => signers.bulk(*from, *count),
                };
                Batch {
                    transactions,
                    epoch: 0,
                    beneficiary: producer,
                    base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
                    worker_id: 0,
                    received_at: None,
                }
            })
            .collect();
        let close_epoch =
            case.close_last && usize::try_from(number).ok() == Some(case.outputs.len());
        if batches.is_empty() {
            if close_epoch {
                expected_txs.push(0);
            }
        } else {
            expected_txs.extend(batches.iter().map(|batch| batch.transactions.len()));
        }
        outputs.push(consensus_output(number, &leader, producer, batches, close_epoch));
    }
    (outputs, expected_txs)
}

/// Consensus output `number` led by `leader`, with `batches` produced by `producer`.
fn consensus_output(
    number: u64,
    leader: &AuthorityIdentifier,
    producer: Address,
    batches: Vec<Batch>,
    close_epoch: bool,
) -> ConsensusOutput {
    let mut certificate = Certificate::default();
    certificate.update_header_author_for_test(leader.clone());
    certificate.update_header_round_for_test(u32::try_from(number * 2).unwrap_or(u32::MAX));
    certificate.update_header_created_at_for_test(now());
    let digests = batches.iter().map(|batch| batch.digest()).collect();
    let certified = if batches.is_empty() {
        Vec::new()
    } else {
        vec![CertifiedBatch { address: producer, batches }]
    };
    let sub_dag = CommittedSubDag::new(
        vec![Certificate::default(), certificate.clone()],
        certificate,
        number,
        ReputationScores::default(),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );
    ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        number,
        close_epoch,
        digests,
        certified,
    )
}

/// Committee member `index` (modulo the committee size): its identifier and execution address.
fn leader(committee: &Committee, index: usize) -> (AuthorityIdentifier, Address) {
    let authorities = committee.authorities();
    let authority = &authorities[index % authorities.len()];
    (authority.id(), authority.execution_address())
}

/// The genesis of every case: the consensus registry, the two senders funded, the three
/// contracts deployed and the empty account.
fn chain_for(senders: [Address; 2]) -> Arc<RethChainSpec> {
    let funding = U256::from(10).pow(U256::from(24));
    let code = |runtime: &[u8]| GenesisAccount::default().with_code(Some(runtime.to_vec().into()));
    let genesis = test_genesis_with_consensus_registry(4).extend_accounts([
        (senders[0], GenesisAccount::default().with_balance(funding)),
        (senders[1], GenesisAccount::default().with_balance(funding)),
        (STORE, code(&STORE_RUNTIME)),
        (FACTORY, code(&FACTORY_RUNTIME)),
        (HASHER, code(&BLOCK_HASH_RUNTIME)),
        (EMPTY, GenesisAccount::default()),
    ]);
    Arc::new(genesis.into())
}

/// Init code of the factory's child `variant`. Without value it self-destructs at once (created
/// and destroyed in one transaction); with value it stores `sstore(1, callvalue)` and deploys
/// [`CHILD_RUNTIME`]. The trailing `variant` byte (never executed) gives each variant its own
/// address.
fn child_init_code(variant: u8) -> Vec<u8> {
    let mut code = vec![
        0x34, 0x15, 0x60, 0x13, 0x57, // jumpi(0x13, iszero(callvalue))
        0x34, 0x60, 0x01, 0x55, // sstore(1, callvalue)
        0x60, 0x0a, 0x60, 0x16, 0x5f, 0x39, // codecopy(0, 0x16, 10)
        0x60, 0x0a, 0x5f, 0xf3, // return(0, 10)
        0x5b, 0x33, 0xff, // 0x13: jumpdest; selfdestruct(caller)
    ];
    code.extend_from_slice(&CHILD_RUNTIME);
    code.push(variant);
    code
}

/// The CREATE2 address of child `variant`.
fn child(variant: u8) -> Address {
    FACTORY.create2_from_code(B256::ZERO, child_init_code(variant))
}

/// An engine update as compared between nodes.
fn update_key((round, num_hash, tip): EngineUpdate) -> UpdateKey {
    (round, num_hash.number, num_hash.hash, tip.map(|tip| tip.hash()))
}

/// A test failure carrying `err`'s debug form.
fn fail(err: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{err:?}"))
}

/// Run `future` on a fresh multi-thread runtime (the pipeline's blocking tasks and the build
/// and commit threads need one).
fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a tokio runtime")
        .block_on(future)
}
