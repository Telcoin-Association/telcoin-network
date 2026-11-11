//! Engine contention benchmark: how signer-recovery jobs for queued outputs slow the output that
//! is executing, how much the output-scoped read cache saves, and how large the state root's share
//! of execute is.
//!
//! Every case executes whole consensus outputs (by default 6,000 transfers in 20 blocks, about a
//! loaded bench-5 output) on a temporary reth environment, block by block with the in-memory
//! chain advanced after each block, as the engine's `execute_payload` does: through
//! `RethEnv::build_block_from_recovered_payload` with one `OutputBuildScope` per output, except
//! the `fresh` read-cache arms (see `cached_reads` below). The timed
//! window is that block loop. It leaves out the engine's per-block telemetry (the repack monitor,
//! the timestamp-clamp check, the `next block executed` line and its metrics, the gas accumulator),
//! so `execute_us_tx` runs slightly below the engine's `execute_ms` per transaction; the evm, root
//! and finish laps are the engine's own. Each output is persisted, announced and finalized after
//! the window, so the next output builds on a database tip. Funded accounts send at consecutive
//! nonces from one output to the next, so every output touches the same accounts and the state
//! stays one size across the run.
//!
//! Groups (one line per case, `BENCH <group>/<case> n=<runs> median_us_tx=… p10=… p90=… …`):
//! - `finish_under_recovery/<pool>/<case>`: the `finish` lap per transaction while signer-recovery
//!   jobs of same-sized outputs run beside the build, on rayon's global pool (`global`) or on the
//!   dedicated recovery pool of three threads (`dedicated`). Cases `0`, `1` and `2` run that many
//!   jobs back to back for the whole build: the saturation bound, where every block's `finish`
//!   overlaps them. Cases `1_once` and `2_once` run one recovery per job from the start of the
//!   build, as the engine runs one job per queued output, and report the share of the build the
//!   jobs overlapped (`job_overlap`).
//! - `cached_reads/<arm>`: execute per transaction with the output-scoped read cache (`scoped`,
//!   `build_block_from_recovered_payload`) and with an empty cache per block (`fresh`,
//!   `build_block_from_batch_payload_timed`, whose own recovery lap is left out), without jobs. The
//!   plain arms give each transaction its own sender, so only the shared recipient and the system
//!   contracts recur across the blocks of an output; in the `_reuse<R>` arms every sender sends in
//!   `R` blocks of each output.
//! - `root_share/<N>`: the state-root lap as a share of execute with `N` back-to-back jobs.
//!
//! The dedicated pool is process-wide and sized once, so one process runs one pool mode:
//!
//! ```text
//! cargo bench -p tn-engine --bench engine_contention -- --pool global
//! cargo bench -p tn-engine --bench engine_contention -- --pool dedicated
//! ```
//!
//! `--quick` runs a few hundred transactions three times (a smoke run); `--groups` takes a comma
//! list of group names; `--runs`, `--warmup`, `--txs`, `--blocks` and `--reuse` override the
//! shape. The environment variables `ENGINE_CONTENTION_POOL`, `ENGINE_CONTENTION_GROUPS` and
//! `ENGINE_CONTENTION_QUICK=1` do the same; arguments win. The temporary database lives under
//! `TMPDIR`. Computed criteria follow the `BENCH` lines as `CRITERION` lines; nothing is asserted.

// A bench binary links every dependency and dev-dependency of its crate but uses only a few.
#![allow(unused_crate_dependencies)]

use std::{
    collections::VecDeque,
    hint::black_box,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tn_engine::run_on_recovery_pool;
use tn_reth::{
    payload::TNPayload,
    test_utils::{
        seeded_genesis_from_random_batches, test_genesis_with_consensus_registry,
        TransactionFactory,
    },
    BlockBuildTimings, ExecutedBlock, NewCanonicalChain, OutputBuildScope, OutputTrieOverlay,
    RethChainSpec, RethEnv,
};
use tn_test_utils::default_test_execution_node;
use tn_types::{
    max_batch_gas, now, Address, Batch, BlockHash, Bytes, Certificate, CertifiedBatch,
    CommittedSubDag, ConsensusHeaderDigest, ConsensusOutput, EpochSeedChainValue, ExecHeader,
    Hash as _, Recovered, ReputationScores, SealedHeader, TransactionSigned, B256, U256,
};

/// Transactions per output in a full run: bench 5 executed 5,962 per output at 600 × 16.
const FULL_TXS: usize = 6_000;
/// Blocks per output in a full run: bench 5 executed 23.1 per output at 600 × 16.
const FULL_BLOCKS: usize = 20;
/// Measured rounds in a full run; each round executes one output per case.
const FULL_RUNS: usize = 20;
/// Discarded rounds before the measured ones in a full run.
const FULL_WARMUP: usize = 2;
/// Transactions per output in `--quick` mode.
const QUICK_TXS: usize = 400;
/// Blocks per output in `--quick` mode.
const QUICK_BLOCKS: usize = 4;
/// Measured rounds in `--quick` mode.
const QUICK_RUNS: usize = 3;
/// Discarded rounds in `--quick` mode.
const QUICK_WARMUP: usize = 1;
/// Blocks of each output every sender sends in, in the `_reuse<R>` read-cache arms.
const DEFAULT_REUSE: usize = 4;
/// The dedicated recovery pool's size in `dedicated` mode.
const DEDICATED_THREADS: u16 = 3;
/// The most concurrent recovery jobs a case runs (and the number of job outputs recovered).
const MAX_JOBS: usize = 2;
/// The contention cases; the first is the clean baseline the others are compared with.
const CONTENTION_CASES: [Case; 5] = [
    Case { jobs: 0, once: false },
    Case { jobs: 1, once: false },
    Case { jobs: MAX_JOBS, once: false },
    Case { jobs: 1, once: true },
    Case { jobs: MAX_JOBS, once: true },
];
/// `finish` per transaction with jobs must stay within this fraction of the clean case on the
/// dedicated pool; a larger rise on the global pool counts as the slowdown.
const PASS_BAND: f64 = 0.15;
/// The global pool's `finish` excess bench 5 measured (r20261107-1504), µs per transaction.
const BENCH5_SLOWDOWN_US_TX: (f64, f64) = (8.0, 15.0);
/// Bench 5's `finish` per transaction on outputs a job overlapped against outputs none did, on
/// validator-09: 16.3 against 6.7 µs at 600 × 16 (5,962 transactions per output) and 22.1
/// against 7.8 µs at 600 × 4, whose outputs were smaller.
const BENCH5_FINISH_RATIO: (f64, f64) = (16.3 / 6.7, 22.1 / 7.8);
/// The max fee per gas of every transfer: the value the tn-reth `transaction` test helper signs.
const GAS_PRICE: u128 = 100_000;
/// The gas limit of every transfer: exactly what a value transfer to an account without code
/// uses, so a block holds up to `max_batch_gas / 21,000` of them.
const TRANSFER_GAS: u64 = 21_000;
/// Wei sent by every transfer (1 TEL), as the tn-reth `transaction` test helper sends.
const TRANSFER_WEI: u128 = 1_000_000_000_000_000_000;

/// Run the selected groups in one pool mode and print one `BENCH` line per case.
fn main() -> eyre::Result<()> {
    let config = Config::parse()?;
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    // the env's task manager and providers expect a runtime context; the builds below run on
    // this thread, outside every rayon pool, as the engine's blocking execution thread does
    let _runtime = runtime.enter();

    let cached_reads = config.groups.contains(&Group::CachedReads);
    let mut workload = Workload::new(config.txs, config.blocks, 1);
    let mut reuse_workload = (cached_reads && config.reuse > 1)
        .then(|| Workload::new(config.txs, config.blocks, config.reuse));
    // the first output of each workload funds its accounts in genesis and is executed untimed
    // before any case
    let seed_batches = workload.next_batches();
    let reuse_seed = reuse_workload.as_mut().map(Workload::next_batches);
    let funded = seed_batches.iter().chain(reuse_seed.iter().flatten());
    let (genesis, _, _) =
        seeded_genesis_from_random_batches(test_genesis_with_consensus_registry(4), funded);
    let chain_spec: Arc<RethChainSpec> = Arc::new(genesis.into());
    let db_dir = TempDir::new()?;
    let env = runtime.block_on(async {
        let node =
            default_test_execution_node(Some(chain_spec.clone()), None, db_dir.path(), None)?;
        eyre::Ok(node.get_reth_env().await)
    })?;
    let mut chain = Chain { env, tip: chain_spec.sealed_genesis_header(), number: 0 };

    print_header(&config, &db_dir);
    chain.execute(seed_batches, Reads::Scoped, &Jobs::none())?;
    if let Some(batches) = reuse_seed {
        chain.execute(batches, Reads::Scoped, &Jobs::none())?;
    }

    if config.groups.contains(&Group::FinishUnderRecovery)
        || config.groups.contains(&Group::RootShare)
    {
        // the jobs recover outputs of the same shape signed by other (unfunded) accounts: recovery
        // reads only the bytes, so these never execute
        let job_outputs: Vec<ConsensusOutput> = (0..MAX_JOBS)
            .map(|_| {
                consensus_output(0, Workload::new(config.txs, config.blocks, 1).next_batches())
            })
            .collect();
        // build the dedicated pool (or wake the global one) before the first measured output, and
        // check the jobs will recover where the line says: a dedicated pool that fails to build
        // falls back to the global pool with a warning no subscriber prints here
        let on_dedicated = run_on_recovery_pool(config.pool.threads(), || {
            black_box(RethEnv::recover_output_signers(&job_outputs[0]));
            thread::current().name().is_some_and(|name| name.starts_with("tn-recover-"))
        });
        eyre::ensure!(
            on_dedicated == (config.pool == Pool::Dedicated),
            "recovery did not run on the {} pool",
            config.pool.name()
        );
        let samples = contention_sweep(&config, &mut chain, &mut workload, &job_outputs)?;
        report_contention(&config, &samples);
    }
    if cached_reads {
        let samples =
            cached_reads_sweep(&config, &mut chain, &mut workload, reuse_workload.as_mut())?;
        report_cached_reads(&config, &samples);
    }
    Ok(())
}

/// The rayon pool the concurrent recovery jobs run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pool {
    /// Rayon's global pool, which the block builds' `finish` lap also uses (`recovery_threads:
    /// 0`).
    Global,
    /// The process-wide dedicated recovery pool of [`DEDICATED_THREADS`] threads.
    Dedicated,
}

impl Pool {
    /// The `recovery_threads` value that selects this pool.
    fn threads(self) -> u16 {
        match self {
            Self::Global => 0,
            Self::Dedicated => DEDICATED_THREADS,
        }
    }

    /// The pool's name in case names and fields.
    fn name(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Dedicated => "dedicated",
        }
    }

    /// Parse a `--pool` value.
    fn parse(value: &str) -> eyre::Result<Self> {
        match value {
            "global" => Ok(Self::Global),
            "dedicated" => Ok(Self::Dedicated),
            other => Err(eyre::eyre!("unknown pool {other:?} (global or dedicated)")),
        }
    }
}

/// A benchmark group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    /// `finish_under_recovery/<pool>/<case>`.
    FinishUnderRecovery,
    /// `cached_reads/<arm>`.
    CachedReads,
    /// `root_share/<N>`.
    RootShare,
}

impl Group {
    /// Every group, in the order they run.
    const ALL: [Self; 3] = [Self::FinishUnderRecovery, Self::RootShare, Self::CachedReads];

    /// The group's name in `--groups` and in `BENCH` lines.
    fn name(self) -> &'static str {
        match self {
            Self::FinishUnderRecovery => "finish_under_recovery",
            Self::CachedReads => "cached_reads",
            Self::RootShare => "root_share",
        }
    }

    /// Parse a comma list of group names (`all` selects every group).
    fn parse_list(list: &str) -> eyre::Result<Vec<Self>> {
        if list == "all" {
            return Ok(Self::ALL.to_vec());
        }
        list.split(',')
            .map(|name| {
                Self::ALL
                    .into_iter()
                    .find(|group| group.name() == name.trim())
                    .ok_or_else(|| eyre::eyre!("unknown group {name:?}"))
            })
            .collect()
    }
}

/// What one process runs: the pool mode, the groups and the output shape.
#[derive(Debug)]
struct Config {
    /// The pool the concurrent recovery jobs use.
    pool: Pool,
    /// The groups to run.
    groups: Vec<Group>,
    /// Whether this is a `--quick` smoke run.
    quick: bool,
    /// Transactions per output.
    txs: usize,
    /// Blocks (batches) per output.
    blocks: usize,
    /// Blocks of each output every sender sends in, in the `_reuse<R>` read-cache arms (1 runs
    /// the plain arms only).
    reuse: usize,
    /// Measured rounds per group.
    runs: usize,
    /// Discarded rounds per group, before the measured ones.
    warmup: usize,
}

impl Config {
    /// Read the environment, then the arguments (which win). `cargo bench` appends `--bench`,
    /// which is ignored.
    fn parse() -> eyre::Result<Self> {
        let mut pool = std::env::var("ENGINE_CONTENTION_POOL").ok();
        let mut groups = std::env::var("ENGINE_CONTENTION_GROUPS").ok();
        let mut quick = std::env::var("ENGINE_CONTENTION_QUICK").is_ok_and(|value| value == "1");
        let (mut txs, mut blocks, mut reuse, mut runs, mut warmup) = (None, None, None, None, None);

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let (flag, inline) = match arg.split_once('=') {
                Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
                None => (arg, None),
            };
            let mut value = || {
                inline
                    .clone()
                    .or_else(|| args.next())
                    .ok_or_else(|| eyre::eyre!("{flag} needs a value"))
            };
            match flag.as_str() {
                "--bench" => {}
                "--quick" => quick = true,
                "--pool" => pool = Some(value()?),
                "--groups" => groups = Some(value()?),
                "--txs" => txs = Some(value()?.parse()?),
                "--blocks" => blocks = Some(value()?.parse()?),
                "--reuse" => reuse = Some(value()?.parse()?),
                "--runs" => runs = Some(value()?.parse()?),
                "--warmup" => warmup = Some(value()?.parse()?),
                "--help" | "-h" => {
                    println!(
                        "engine_contention [--pool global|dedicated] [--groups all|{}] [--quick] \
                         [--txs N] [--blocks N] [--reuse N] [--runs N] [--warmup N]",
                        Group::ALL.map(Group::name).join(",")
                    );
                    std::process::exit(0);
                }
                other => return Err(eyre::eyre!("unknown argument {other:?} (try --help)")),
            }
        }

        let config = Self {
            pool: pool.as_deref().map(Pool::parse).transpose()?.unwrap_or(Pool::Global),
            groups: groups
                .as_deref()
                .map(Group::parse_list)
                .transpose()?
                .unwrap_or(Group::ALL.to_vec()),
            quick,
            txs: txs.unwrap_or(if quick { QUICK_TXS } else { FULL_TXS }),
            blocks: blocks.unwrap_or(if quick { QUICK_BLOCKS } else { FULL_BLOCKS }),
            reuse: reuse.unwrap_or(DEFAULT_REUSE),
            runs: runs.unwrap_or(if quick { QUICK_RUNS } else { FULL_RUNS }),
            warmup: warmup.unwrap_or(if quick { QUICK_WARMUP } else { FULL_WARMUP }),
        };
        eyre::ensure!(config.blocks > 0 && config.txs >= config.blocks, "need txs >= blocks > 0");
        eyre::ensure!(config.runs > 0, "need runs > 0");
        // an over-full block fails the build (not a skip) once the next transaction's gas limit
        // exceeds what is left of the block's
        let per_block = u64::try_from(config.txs.div_ceil(config.blocks))?;
        eyre::ensure!(
            per_block.saturating_mul(TRANSFER_GAS) <= max_batch_gas(0),
            "{per_block} transfers per block exceed the block gas limit"
        );
        eyre::ensure!(config.reuse > 0, "need reuse > 0");
        eyre::ensure!(
            !config.groups.contains(&Group::CachedReads)
                || (config.blocks.is_multiple_of(config.reuse)
                    && config.txs.is_multiple_of(config.reuse)),
            "--reuse must divide both --blocks and --txs"
        );
        Ok(config)
    }
}

/// Funded accounts sending one transfer per transaction slot of an output: with reuse `R`,
/// `txs / R` accounts each send in `R` blocks of every output, at consecutive nonces.
///
/// The blocks form `R` consecutive groups; in group `r` every account sends its `r`-th transfer of
/// the output, spread over the group's blocks, so an account's transfers execute in nonce order.
#[derive(Debug)]
struct Workload {
    /// The chain the transfers are signed for.
    chain: Arc<RethChainSpec>,
    /// The sending accounts.
    accounts: Vec<TransactionFactory>,
    /// Blocks (batches) per output.
    blocks: usize,
    /// Blocks of each output every account sends in.
    reuse: usize,
}

impl Workload {
    /// `txs / reuse` fresh random accounts sending in `blocks` blocks per output. `reuse` divides
    /// both `txs` and `blocks` (see [`Config::parse`]).
    fn new(txs: usize, blocks: usize, reuse: usize) -> Self {
        let accounts = (0..txs / reuse).map(|_| TransactionFactory::new_random()).collect();
        Self { chain: tn_types::test_chain_spec_arc(), accounts, blocks, reuse }
    }

    /// The next output's batches, one per block: transfers to the zero address at each account's
    /// next nonces, in the transaction shape of the tn-reth `transaction` test helper.
    fn next_batches(&mut self) -> Vec<Batch> {
        let chain = &self.chain;
        let group = self.blocks / self.reuse;
        let mut blocks = vec![Vec::new(); self.blocks];
        for round in 0..self.reuse {
            for (index, account) in self.accounts.iter_mut().enumerate() {
                blocks[round * group + index % group].push(account.create_eip1559_encoded(
                    chain.clone(),
                    Some(TRANSFER_GAS),
                    GAS_PRICE,
                    Some(Address::ZERO),
                    U256::from(TRANSFER_WEI),
                    Bytes::new(),
                ));
            }
        }
        blocks
            .into_iter()
            .map(|transactions| Batch::new_for_test(transactions, ExecHeader::default(), 0, 0))
            .collect()
    }
}

/// Wrap `batches` in a consensus output numbered `number` with one certificate, the way the engine
/// tests build theirs (`test_queued_recovery_matches_inline_recovery`).
fn consensus_output(number: u64, batches: Vec<Batch>) -> ConsensusOutput {
    let mut leader = Certificate::default();
    leader.update_header_round_for_test(u32::try_from(number).unwrap_or(u32::MAX));
    leader.update_header_created_at_for_test(now());
    let batch_digests: VecDeque<BlockHash> = batches.iter().map(|batch| batch.digest()).collect();
    let sub_dag = CommittedSubDag::new(
        vec![Certificate::default(), leader.clone()],
        leader,
        number,
        ReputationScores::default(),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );
    ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        number,
        false,
        batch_digests,
        vec![CertifiedBatch { address: Address::ZERO, batches }],
    )
}

/// How a block's build reads its parent state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reads {
    /// The output's [`OutputBuildScope`]: reads made by earlier blocks of the output are kept
    /// (the engine's path, with signers recovered before the first block).
    Scoped,
    /// An empty read cache per block (`build_block_from_batch_payload_timed`, which recovers each
    /// batch's signers itself; that lap is left out of execute).
    Fresh,
}

/// The temporary chain the bench executes on.
#[derive(Debug)]
struct Chain {
    /// The temporary reth environment.
    env: RethEnv,
    /// The canonical (and persisted) tip the next output builds on.
    tip: SealedHeader,
    /// The last output's consensus number.
    number: u64,
}

impl Chain {
    /// Execute the next output from `batches` with `jobs` recovering beside the build, then
    /// persist it. Only the block loop is timed: the output's own signer recovery (scoped) runs
    /// before the jobs start, and the persist after they stop.
    fn execute(
        &mut self,
        batches: Vec<Batch>,
        reads: Reads,
        jobs: &Jobs<'_>,
    ) -> eyre::Result<Sample> {
        self.number += 1;
        let output = consensus_output(self.number, batches);
        let signers = match reads {
            Reads::Scoped => Some(RethEnv::recover_output_signers(&output)),
            Reads::Fresh => None,
        };

        let contention = Contention::start(jobs)?;
        let built = self.build(&output, signers);
        let tally = contention.stop()?;
        let (blocks, laps) = built?;

        // a transaction the build skips would make every per-transaction figure read too fast
        let executed: usize =
            blocks.iter().map(|block| block.recovered_block.body().transactions.len()).sum();
        eyre::ensure!(
            executed == laps.txs,
            "output {} executed {executed} of {} transactions",
            self.number,
            laps.txs
        );
        // as the engine does after a build: persist, announce, then finalize in memory
        let tip = blocks
            .last()
            .map(|block| block.recovered_block.clone_sealed_header())
            .ok_or_else(|| eyre::eyre!("output {} built no block", self.number))?;
        self.env.persist_executed_output(&blocks)?;
        self.env.announce_executed_output(blocks, None)?;
        self.env.finalize_block(tip.clone())?;
        self.tip = tip;
        Ok(Sample { laps, tally, jobs: jobs.count })
    }

    /// The timed block loop: build each batch's block on the previous one and advance the
    /// in-memory chain to it, as `execute_payload` does in the engine.
    fn build(
        &self,
        output: &ConsensusOutput,
        signers: Option<Vec<Vec<Recovered<TransactionSigned>>>>,
    ) -> eyre::Result<(Vec<ExecutedBlock>, OutputLaps)> {
        let output_digest: B256 = output.digest().into();
        let gas_limit = max_batch_gas(output.leader().epoch());
        let in_memory = self.env.canonical_in_memory_state();
        let mut signers = signers.map(Vec::into_iter);
        let mut scope = OutputBuildScope::new();
        let mut overlay = OutputTrieOverlay::default();
        let mut parent = self.tip.clone();
        let mut blocks = Vec::new();
        let mut laps = OutputLaps::default();

        let start = Instant::now();
        for (batch_index, (cert_index, index_in_cert)) in
            output.flatten_batches().into_iter().enumerate()
        {
            let batch_digest = output
                .get_batch_digest(batch_index)
                .ok_or_else(|| eyre::eyre!("no digest for batch {batch_index}"))?;
            let batch = &output.batches()[cert_index].batches[index_in_cert];
            let payload = TNPayload::new(
                parent,
                batch.beneficiary,
                batch_index,
                batch_digest,
                output,
                output_digest,
                batch.base_fee_per_gas,
                gas_limit,
                output.prev_randao(batch_index, batch_digest),
                batch.worker_id,
            );
            let (block, timings) = match signers.as_mut() {
                Some(signers) => {
                    let recovered = signers
                        .next()
                        .ok_or_else(|| eyre::eyre!("no signers for batch {batch_index}"))?;
                    self.env.build_block_from_recovered_payload(payload, recovered, &mut scope)?
                }
                None => self.env.build_block_from_batch_payload_timed(
                    payload,
                    &batch.transactions,
                    &mut overlay,
                )?,
            };
            laps.add(&timings, batch.transactions.len());
            parent = block.recovered_block.clone_sealed_header();
            in_memory.set_pending_block(block.clone());
            in_memory.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
            in_memory.set_canonical_head(parent.clone());
            blocks.push(block);
        }
        laps.window = start.elapsed();
        // the fresh path recovers inside its builds: keep execute to the build itself
        laps.execute = laps.window.saturating_sub(laps.recover);
        Ok((blocks, laps))
    }
}

/// The build laps of one output, summed over its blocks.
#[derive(Debug, Default, Clone, Copy)]
struct OutputLaps {
    /// Transactions handed to the builds (all of them execute, see [`Chain::execute`]).
    txs: usize,
    /// The block loop's wall time: the window the concurrent jobs overlap.
    window: Duration,
    /// The window less `recover`.
    execute: Duration,
    /// Signer recovery inside the builds (the fresh path only).
    recover: Duration,
    /// Builder setup, pre-execution calls and the transaction loop.
    evm: Duration,
    /// The state root.
    root: Duration,
    /// The rest of `finish`: the lap that uses rayon's global pool.
    finish: Duration,
}

impl OutputLaps {
    /// Fold one block's laps and transaction count in.
    fn add(&mut self, block: &BlockBuildTimings, txs: usize) {
        self.txs += txs;
        self.recover = self.recover.saturating_add(block.recover);
        self.evm = self.evm.saturating_add(block.evm);
        self.root = self.root.saturating_add(block.root);
        self.finish = self.finish.saturating_add(block.finish);
    }
}

/// One contention case: how many recovery jobs run beside the build, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Case {
    /// Concurrent recovery jobs.
    jobs: usize,
    /// Each job recovers once from the start of the build (`true`), or back to back until the
    /// build ends (`false`).
    once: bool,
}

impl Case {
    /// The case's name in `BENCH` lines: the job count, with `_once` for one-shot jobs.
    fn name(self) -> String {
        if self.once {
            format!("{}_once", self.jobs)
        } else {
            self.jobs.to_string()
        }
    }

    /// The `job_mode` field: `none`, `loop` or `once`.
    fn mode(self) -> &'static str {
        match (self.jobs, self.once) {
            (0, _) => "none",
            (_, true) => "once",
            (_, false) => "loop",
        }
    }
}

/// What the concurrent jobs of one build do.
#[derive(Debug)]
struct Jobs<'a> {
    /// How many jobs run beside the build.
    count: usize,
    /// Each job recovers once (`true`) or back to back until the build ends (`false`).
    once: bool,
    /// The `recovery_threads` value they recover with (0 is the global pool).
    threads: u16,
    /// The outputs they recover, job `i` taking `outputs[i % len]`.
    outputs: &'a [ConsensusOutput],
}

impl Jobs<'_> {
    /// No concurrent jobs.
    fn none() -> Jobs<'static> {
        Jobs { count: 0, once: false, threads: 0, outputs: &[] }
    }
}

/// Signer-recovery jobs running beside a timed build.
///
/// Each job is a plain thread (outside every rayon pool, like the engine's blocking job) that
/// recovers its output with `run_on_recovery_pool`: once from the start of the build, as the
/// engine's job for one queued output does, or over and over until the build ends, which keeps
/// the pool saturated for the whole window (an upper bound on the overlap the engine produces).
/// [`Contention::start`] returns once every job thread is running.
#[derive(Debug)]
struct Contention {
    /// Set to end the back-to-back jobs' loops.
    stop: Arc<AtomicBool>,
    /// The job threads, each returning its tally.
    workers: Vec<thread::JoinHandle<JobTally>>,
}

impl Contention {
    /// Start `jobs.count` job threads and wait until all of them are running.
    fn start(jobs: &Jobs<'_>) -> eyre::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        if jobs.count == 0 {
            return Ok(Self { stop, workers: Vec::new() });
        }
        eyre::ensure!(!jobs.outputs.is_empty(), "concurrent jobs need outputs to recover");
        let running = Arc::new(Barrier::new(jobs.count + 1));
        let workers = (0..jobs.count)
            .map(|job| {
                let output = jobs.outputs[job % jobs.outputs.len()].clone();
                let (stop, running) = (stop.clone(), running.clone());
                let (threads, once) = (jobs.threads, jobs.once);
                thread::Builder::new().name(format!("bench-recover-{job}")).spawn(move || {
                    running.wait();
                    let mut tally = JobTally::default();
                    loop {
                        let begun = Instant::now();
                        black_box(run_on_recovery_pool(threads, || {
                            RethEnv::recover_output_signers(&output)
                        }));
                        tally.recoveries += 1;
                        tally.busy += begun.elapsed();
                        if once || stop.load(Ordering::Acquire) {
                            break;
                        }
                    }
                    tally
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        running.wait();
        Ok(Self { stop, workers })
    }

    /// Stop the jobs, wait for their last recovery and sum their tallies.
    fn stop(self) -> eyre::Result<JobTally> {
        self.stop.store(true, Ordering::Release);
        self.workers.into_iter().try_fold(JobTally::default(), |sum, worker| -> eyre::Result<_> {
            let tally = worker.join().map_err(|_| eyre::eyre!("a recovery job thread panicked"))?;
            Ok(JobTally {
                recoveries: sum.recoveries + tally.recoveries,
                busy: sum.busy + tally.busy,
            })
        })
    }
}

/// What the concurrent jobs did during one build.
#[derive(Debug, Default, Clone, Copy)]
struct JobTally {
    /// Recoveries completed, the last one possibly finishing after the build.
    recoveries: usize,
    /// Their summed wall time.
    busy: Duration,
}

/// One executed output's measurements.
#[derive(Debug, Clone, Copy)]
struct Sample {
    /// The build's laps.
    laps: OutputLaps,
    /// The concurrent jobs' tally.
    tally: JobTally,
    /// How many jobs ran beside the build.
    jobs: usize,
}

impl Sample {
    /// `lap` in microseconds per transaction of the output.
    fn per_tx(&self, lap: Duration) -> f64 {
        lap.as_secs_f64() * 1e6 / self.laps.txs.max(1) as f64
    }

    /// The share of the build window one job was recovering, capped at 1 (0 without jobs).
    fn overlap(&self) -> f64 {
        let window = self.laps.window.as_secs_f64();
        if self.jobs == 0 || window <= 0.0 {
            return 0.0;
        }
        (self.tally.busy.as_secs_f64() / self.jobs as f64).min(window) / window
    }

    /// The mean wall time of one job recovery in milliseconds (NaN without jobs).
    fn recover_ms(&self) -> f64 {
        if self.tally.recoveries == 0 {
            return f64::NAN;
        }
        self.tally.busy.as_secs_f64() * 1e3 / self.tally.recoveries as f64
    }
}

/// The finish/root contention sweep: one output per case of [`CONTENTION_CASES`] per round, in
/// an order rotated each round so neither drift nor position favours a case; the first `warmup`
/// rounds are discarded. Returns the measured samples per case, round by round.
fn contention_sweep(
    config: &Config,
    chain: &mut Chain,
    workload: &mut Workload,
    job_outputs: &[ConsensusOutput],
) -> eyre::Result<Vec<Vec<Sample>>> {
    let cases = CONTENTION_CASES.len();
    let mut samples = vec![Vec::with_capacity(config.runs); cases];
    for round in 0..config.warmup + config.runs {
        for offset in 0..cases {
            let index = (round + offset) % cases;
            let case = CONTENTION_CASES[index];
            let jobs = Jobs {
                count: case.jobs,
                once: case.once,
                threads: config.pool.threads(),
                outputs: job_outputs,
            };
            let sample = chain.execute(workload.next_batches(), Reads::Scoped, &jobs)?;
            if round >= config.warmup {
                samples[index].push(sample);
            }
        }
        eprintln!(
            "engine_contention: contention round {}/{}",
            round + 1,
            config.warmup + config.runs
        );
    }
    Ok(samples)
}

/// One arm of the read-cache A/B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Arm {
    /// Blocks of each output every sender sends in.
    reuse: usize,
    /// How the builds read their parent state.
    reads: Reads,
}

impl Arm {
    /// The arm's case name: `scoped` or `fresh`, with `_reuse<R>` when senders recur.
    fn name(self) -> String {
        let reads = match self.reads {
            Reads::Scoped => "scoped",
            Reads::Fresh => "fresh",
        };
        if self.reuse == 1 {
            reads.to_string()
        } else {
            format!("{reads}_reuse{}", self.reuse)
        }
    }
}

/// The E5 A/B without jobs: per round, one output per arm (scoped and fresh on the plain
/// workload, then on the reuse workload when there is one), in an order rotated each round; the
/// first `warmup` rounds are discarded. Returns each arm with its samples, round by round.
fn cached_reads_sweep(
    config: &Config,
    chain: &mut Chain,
    plain: &mut Workload,
    mut reuse: Option<&mut Workload>,
) -> eyre::Result<Vec<(Arm, Vec<Sample>)>> {
    let mut arms =
        vec![Arm { reuse: 1, reads: Reads::Scoped }, Arm { reuse: 1, reads: Reads::Fresh }];
    if reuse.is_some() {
        arms.push(Arm { reuse: config.reuse, reads: Reads::Scoped });
        arms.push(Arm { reuse: config.reuse, reads: Reads::Fresh });
    }
    let mut samples = vec![Vec::with_capacity(config.runs); arms.len()];
    for round in 0..config.warmup + config.runs {
        for offset in 0..arms.len() {
            let index = (round + offset) % arms.len();
            let arm = arms[index];
            let workload = if arm.reuse == 1 {
                &mut *plain
            } else {
                reuse.as_deref_mut().ok_or_else(|| eyre::eyre!("no reuse workload"))?
            };
            let sample = chain.execute(workload.next_batches(), arm.reads, &Jobs::none())?;
            if round >= config.warmup {
                samples[index].push(sample);
            }
        }
        eprintln!(
            "engine_contention: cached_reads round {}/{}",
            round + 1,
            config.warmup + config.runs
        );
    }
    Ok(arms.into_iter().zip(samples).collect())
}

/// The host's CPU count as the process sees it (affinity and cgroup limits included).
fn cpus() -> usize {
    thread::available_parallelism().map(usize::from).unwrap_or(0)
}

/// Print the machine, the thread counts and the output shape.
fn print_header(config: &Config, db_dir: &TempDir) {
    let field = |value: Option<String>, missing: &str| {
        value
            .map(|value| value.split_whitespace().collect::<Vec<_>>().join("_"))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| missing.to_string())
    };
    let host = field(std::fs::read_to_string("/proc/sys/kernel/hostname").ok(), "unknown");
    let cpu_model = field(
        std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|info| {
            info.lines()
                .find_map(|line| line.strip_prefix("model name"))
                .map(|rest| rest.trim_start_matches([' ', '\t', ':']).to_string())
        }),
        "unknown",
    );
    let rayon_env = field(std::env::var("RAYON_NUM_THREADS").ok(), "unset");
    let cases: Vec<String> = CONTENTION_CASES.iter().map(|case| case.name()).collect();
    println!(
        "# engine_contention host={host} cpu_model={cpu_model} cpus={} \
         rayon_global_threads={} RAYON_NUM_THREADS={rayon_env} pool={} recovery_threads={} \
         contention_cases={} txs_per_output={} blocks_per_output={} reuse={} runs={} warmup={} \
         quick={} groups={} db_dir={}",
        cpus(),
        rayon::current_num_threads(),
        config.pool.name(),
        config.pool.threads(),
        cases.join(","),
        config.txs,
        config.blocks,
        config.reuse,
        config.runs,
        config.warmup,
        config.quick,
        config.groups.iter().map(|group| group.name()).collect::<Vec<_>>().join(","),
        db_dir.path().display(),
    );
}

/// Print the `finish_under_recovery` and `root_share` lines selected, then the computed criteria.
fn report_contention(config: &Config, samples: &[Vec<Sample>]) {
    let pool = config.pool.name();
    let finish: Vec<Summary> =
        samples.iter().map(|cases| Summary::of(cases, |s| s.per_tx(s.laps.finish))).collect();
    let root: Vec<Summary> =
        samples.iter().map(|cases| Summary::of(cases, |s| s.per_tx(s.laps.root))).collect();
    let overlap: Vec<f64> =
        samples.iter().map(|cases| Summary::of(cases, Sample::overlap).p50).collect();
    // threads competing for the cores while the build runs: the build itself plus, with jobs,
    // every thread of the pool they saturate
    let pool_threads = match config.pool {
        Pool::Global => rayon::current_num_threads(),
        Pool::Dedicated => usize::from(DEDICATED_THREADS),
    };
    let oversubscribed = |case: Case| {
        if case.jobs > 0 && !case.once && 1 + pool_threads > cpus() {
            "yes"
        } else {
            "no"
        }
    };

    for (index, case) in CONTENTION_CASES.into_iter().enumerate() {
        let cases = &samples[index];
        let evm = Summary::of(cases, |s| s.per_tx(s.laps.evm));
        let execute = Summary::of(cases, |s| s.per_tx(s.laps.execute));
        if config.groups.contains(&Group::FinishUnderRecovery) {
            println!(
                "BENCH {}/{pool}/{} n={} median_us_tx={:.3} p10={:.3} p90={:.3} \
                 evm_us_tx={:.3} root_us_tx={:.3} execute_us_tx={:.3} job_mode={} \
                 job_recoveries={:.1} job_recover_ms={:.3} job_overlap={:.3}",
                Group::FinishUnderRecovery.name(),
                case.name(),
                finish[index].n,
                finish[index].p50,
                finish[index].p10,
                finish[index].p90,
                evm.p50,
                root[index].p50,
                execute.p50,
                case.mode(),
                Summary::of(cases, |s| s.tally.recoveries as f64).p50,
                Summary::of(cases, Sample::recover_ms).p50,
                overlap[index],
            );
        }
        if config.groups.contains(&Group::RootShare) && !case.once {
            let share =
                Summary::of(cases, |s| s.laps.root.as_secs_f64() / s.laps.execute.as_secs_f64());
            println!(
                "BENCH {}/{} n={} median_us_tx={:.3} p10={:.3} p90={:.3} share={:.4} \
                 share_p10={:.4} share_p90={:.4} execute_us_tx={:.3} pool={pool} \
                 oversubscribed={}",
                Group::RootShare.name(),
                case.jobs,
                root[index].n,
                root[index].p50,
                root[index].p10,
                root[index].p90,
                share.p50,
                share.p10,
                share.p90,
                execute.p50,
                oversubscribed(case),
            );
        }
    }

    // computed, never asserted: the orchestrator reads them
    for (index, case) in CONTENTION_CASES.into_iter().enumerate().skip(1) {
        let ratio = finish[index].p50 / finish[0].p50;
        let delta = finish[index].p50 - finish[0].p50;
        if config.groups.contains(&Group::FinishUnderRecovery) {
            match config.pool {
                Pool::Dedicated => println!(
                    "CRITERION finish_under_recovery pool={pool} case={} job_mode={} \
                     ratio={ratio:.3} delta_us_tx={delta:+.3} band={PASS_BAND} result={}",
                    case.name(),
                    case.mode(),
                    if (ratio - 1.0).abs() <= PASS_BAND { "pass" } else { "fail" },
                ),
                Pool::Global if case.once => println!(
                    "CRITERION finish_under_recovery pool={pool} case={} job_mode={} \
                     ratio={ratio:.3} delta_us_tx={delta:+.3} slowdown={} job_overlap={:.3} \
                     bench5_us_tx={}..{} in_bench5_range={}",
                    case.name(),
                    case.mode(),
                    if ratio > 1.0 + PASS_BAND { "yes" } else { "no" },
                    overlap[index],
                    BENCH5_SLOWDOWN_US_TX.0,
                    BENCH5_SLOWDOWN_US_TX.1,
                    if (BENCH5_SLOWDOWN_US_TX.0..=BENCH5_SLOWDOWN_US_TX.1).contains(&delta) {
                        "yes"
                    } else {
                        "no"
                    },
                ),
                Pool::Global => println!(
                    "CRITERION finish_under_recovery pool={pool} case={} job_mode={} \
                     ratio={ratio:.3} delta_us_tx={delta:+.3} slowdown={} \
                     bench5_ratio={:.3}..{:.3}",
                    case.name(),
                    case.mode(),
                    if ratio > 1.0 + PASS_BAND { "yes" } else { "no" },
                    BENCH5_FINISH_RATIO.0,
                    BENCH5_FINISH_RATIO.1,
                ),
            }
        }
        if config.groups.contains(&Group::RootShare) && !case.once {
            let root_ratio = root[index].p50 / root[0].p50;
            println!(
                "CRITERION root_share pool={pool} case={} root_ratio={root_ratio:.3} \
                 expectation=unchanged within_band={} oversubscribed={}",
                case.jobs,
                if (root_ratio - 1.0).abs() <= PASS_BAND { "yes" } else { "no" },
                oversubscribed(case),
            );
        }
    }
}

/// Print one `cached_reads` line per arm, then the saving per reuse factor: the median over
/// rounds of each round's fresh-minus-scoped difference.
fn report_cached_reads(config: &Config, arms: &[(Arm, Vec<Sample>)]) {
    let pool = config.pool.name();
    for (arm, cases) in arms {
        let execute = Summary::of(cases, |s| s.per_tx(s.laps.execute));
        let evm = Summary::of(cases, |s| s.per_tx(s.laps.evm));
        println!(
            "BENCH {}/{} n={} median_us_tx={:.3} p10={:.3} p90={:.3} evm_us_tx={:.3} \
             evm_p10={:.3} evm_p90={:.3} root_us_tx={:.3} finish_us_tx={:.3} \
             excluded_recover_us_tx={:.3} reuse={} pool={pool}",
            Group::CachedReads.name(),
            arm.name(),
            execute.n,
            execute.p50,
            execute.p10,
            execute.p90,
            evm.p50,
            evm.p10,
            evm.p90,
            Summary::of(cases, |s| s.per_tx(s.laps.root)).p50,
            Summary::of(cases, |s| s.per_tx(s.laps.finish)).p50,
            Summary::of(cases, |s| s.per_tx(s.laps.recover)).p50,
            arm.reuse,
        );
    }

    let mut factors: Vec<usize> = arms.iter().map(|(arm, _)| arm.reuse).collect();
    factors.dedup();
    for reuse in factors {
        let samples_of = |reads| {
            arms.iter().find(|(arm, _)| *arm == Arm { reuse, reads }).map(|(_, cases)| cases)
        };
        let (Some(scoped), Some(fresh)) = (samples_of(Reads::Scoped), samples_of(Reads::Fresh))
        else {
            continue;
        };
        let paired = |lap: fn(&OutputLaps) -> Duration| {
            median(
                scoped
                    .iter()
                    .zip(fresh)
                    .map(|(with_cache, without)| {
                        without.per_tx(lap(&without.laps))
                            - with_cache.per_tx(lap(&with_cache.laps))
                    })
                    .collect(),
            )
        };
        println!(
            "CRITERION cached_reads reuse={reuse} execute_saving_us_tx={:+.3} \
             evm_saving_us_tx={:+.3}",
            paired(|laps| laps.execute),
            paired(|laps| laps.evm),
        );
    }
}

/// The median and the 10th and 90th percentiles (nearest rank) of one value over a case's
/// samples.
#[derive(Debug, Clone, Copy)]
struct Summary {
    /// Samples.
    n: usize,
    /// 10th percentile.
    p10: f64,
    /// Median (the lower median for an even count).
    p50: f64,
    /// 90th percentile.
    p90: f64,
}

impl Summary {
    /// Summarize `value` over `samples`.
    fn of(samples: &[Sample], value: impl Fn(&Sample) -> f64) -> Self {
        let mut values: Vec<f64> = samples.iter().map(value).collect();
        values.sort_by(f64::total_cmp);
        Self {
            n: values.len(),
            p10: percentile(&values, 0.10),
            p50: percentile(&values, 0.50),
            p90: percentile(&values, 0.90),
        }
    }
}

/// The median of `values` (NaN when empty).
fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    percentile(&values, 0.50)
}

/// The nearest-rank `fraction` percentile of ascending `sorted`: the smallest value with at least
/// that fraction of the values at or below it (NaN when empty).
fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let Some(last) = sorted.len().checked_sub(1) else {
        return f64::NAN;
    };
    // the rank is ceil(fraction × n), 1-based; the clamp keeps it within the slice
    let rank = (fraction * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(last)]
}
