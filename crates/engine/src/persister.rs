//! Pipelined persist for the execution engine (`pipelined_persist` in `parameters.yaml`).
//!
//! With the switch on, the engine hands output N's persist to a blocking task and starts building
//! output N+1 on N's in-memory state while N commits. [`PersistPipeline`] is that state machine:
//! one output building, one output committing, and one parking spot for an output built while
//! the commit slot is busy. At most two outputs are executed and not durable at any time.
//!
//! The rules it keeps:
//! - Durable order: one commit slot, dispatched in build order, and an output is dispatched only
//!   when its parent is the durable tip, so the database is always a prefix of the consensus
//!   sequence.
//! - Consensus sees only durable blocks: the commit task sends the engine update (and the canon
//!   notification) after the database commit, and an output without blocks sends its update from
//!   the commit slot too, behind every earlier output's announce.
//! - One snapshot per output: before output N's commit task is spawned, a read transaction is
//!   pinned on the database as of N-1. Output N+1, when it starts while N commits, layers N's trie
//!   deltas over that snapshot, so its state roots read the same state whenever N commits.
//! - A closing output is a barrier: nothing builds on it until it has committed.
//! - Failure: the first error wins, the other slot is allowed to finish, and then every block that
//!   is in memory and not durable is rolled back to the durable tip in one call. A durable block is
//!   never rolled back, and nothing is dispatched after a failure.
//!
//! The ordering rules sit in `Slots`, which does no I/O, so they are tested from a table;
//! [`PersistPipeline`] spawns the builds and commits and awaits around it.

use crate::{
    error::{EngineResult, TnEngineError},
    payload_builder::{
        build_output, commit_output, rollback_in_memory_output, BuiltOutput, CommitFailure,
        Committed,
    },
    recovery::RecoveryHandle,
};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tn_reth::{
    payload::BuildArguments, ExecutedBlock, OutputBuildScope, OutputTrieOverlay, RethEnv,
};
use tn_types::{
    gas_accumulator::GasAccumulator, repack_monitor::RepackMonitor, BlockNumHash, ConsensusOutput,
    EngineUpdate, SealedHeader, TaskSpawner, B256,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

/// The pipelined execute/persist core: one output building while the previous one commits.
///
/// The caller (the engine's run loop) starts outputs while [`Self::can_start`] holds and awaits
/// [`Self::next_event`] while the pipeline is not idle. Every transition runs on the caller's
/// task; the builds and commits run on blocking tasks whose results the pipeline keeps in its
/// own oneshot receivers, so dropping a `next_event` future loses nothing.
///
/// After an error the pipeline is spent: it starts nothing more, and the caller returns the
/// error, which stops the node as every engine error does.
#[derive(Debug)]
pub struct PersistPipeline {
    /// The execution environment both halves run against.
    reth_env: RethEnv,
    /// Spawns the build and commit tasks on the blocking pool.
    task_spawner: TaskSpawner,
    /// Reward accounting, advanced by every build.
    gas_accumulator: GasAccumulator,
    /// Cross-producer repack telemetry, observed by every build.
    repack_monitor: RepackMonitor,
    /// The engine update channel the commit task announces on.
    engine_update_tx: mpsc::Sender<EngineUpdate>,
    /// The ordering rules.
    slots: Slots,
    /// The last block built: the parent of the next build.
    tip: SealedHeader,
    /// The last block committed and announced.
    durable: SealedHeader,
    /// The output building, if any.
    building: Option<BuildSlot>,
    /// An output built while the commit slot was busy, waiting for it.
    built: Option<Finished>,
    /// The output committing, if any.
    committing: Option<CommitSlot>,
    /// The committing output's pinned snapshot and blocks, for the next output to seed from.
    carry: Option<Carry>,
    /// The blocks of every output built and not yet durable, oldest first: what a failure rolls
    /// back.
    unpersisted: VecDeque<(u64, Vec<ExecutedBlock>)>,
    /// The first error of a failure that is still settling.
    first_error: Option<TnEngineError>,
    /// The error to return once the failure has settled.
    settled_error: Option<TnEngineError>,
    /// Transitions not yet handed to the caller.
    events: VecDeque<PipelineEvent>,
    /// The test gate every commit task passes before it starts.
    #[cfg(any(test, feature = "test-utils"))]
    gate: Option<std::sync::Arc<GateState>>,
}

impl PersistPipeline {
    /// A pipeline whose first output builds on `parent_header`, the committed database head.
    pub fn new(
        reth_env: RethEnv,
        parent_header: SealedHeader,
        task_spawner: TaskSpawner,
        gas_accumulator: GasAccumulator,
        repack_monitor: RepackMonitor,
        engine_update_tx: mpsc::Sender<EngineUpdate>,
    ) -> Self {
        Self {
            reth_env,
            task_spawner,
            gas_accumulator,
            repack_monitor,
            engine_update_tx,
            slots: Slots::new(parent_header.hash()),
            tip: parent_header.clone(),
            durable: parent_header,
            building: None,
            built: None,
            committing: None,
            carry: None,
            unpersisted: VecDeque::new(),
            first_error: None,
            settled_error: None,
            events: VecDeque::new(),
            #[cfg(any(test, feature = "test-utils"))]
            gate: None,
        }
    }

    /// Whether an output may start building now: nothing is building or parked, no closing
    /// output is waiting for its commit, no failure is settling, and the commit slot is either
    /// empty, holds an output that built no blocks, or holds the output whose pinned snapshot
    /// the next build can seed from.
    pub fn can_start(&self) -> bool {
        self.slots.can_start()
    }

    /// T1: start building `output` on the in-memory tip, with the signers of the engine's
    /// background recovery job (`Some`), or recovering them inline on the build thread (`None`).
    ///
    /// When the output before it is still committing, the build is seeded: it layers that
    /// output's trie deltas over the snapshot pinned before its commit was spawned, so every
    /// state root of this output reads the same state whenever that commit lands.
    ///
    /// A seed that fails its ordering checks is not returned here: it fails the pipeline like a
    /// failed build, and [`Self::next_event`] returns the error once the commit in flight has
    /// settled and everything unpersisted is rolled back.
    ///
    /// # Errors
    ///
    /// [`TnEngineError::PipelineInvariant`] when [`Self::can_start`] does not hold or `output`
    /// does not follow the last output started in consensus order; nothing is started or
    /// changed.
    pub(crate) fn start(
        &mut self,
        output: ConsensusOutput,
        signers: Option<RecoveryHandle>,
    ) -> EngineResult<()> {
        if !self.slots.can_start() {
            return Err(TnEngineError::PipelineInvariant(
                "an output was started while the pipeline could not start one",
            ));
        }
        let number = output.number();
        self.slots.check_order(number)?;
        let seed = if self.slots.needs_seed() {
            match self.take_seed() {
                Ok(seed) => Some(seed),
                Err(check) => {
                    let step = self.slots.fail(FailureKind::Invariant(check));
                    self.apply(step);
                    return Ok(());
                }
            }
        } else {
            None
        };
        self.slots.start(number)?;

        let seeded = seed.is_some();
        let (tx, rx) = oneshot::channel();
        let args = BuildArguments::new(self.reth_env.clone(), output, self.tip.clone());
        let gas_accumulator = self.gas_accumulator.clone();
        let repack_monitor = self.repack_monitor.clone();
        self.task_spawner.spawn_blocking_task(format!("build-output-{number}"), move || {
            let seed_start = Instant::now();
            let (scope, seed_elapsed) = match seed {
                Some(seed) => (seed.into_scope(), seed_start.elapsed()),
                None => (OutputBuildScope::new(), Duration::ZERO),
            };
            let result = build_output(args, signers, gas_accumulator, repack_monitor, scope)
                .map(|mut built| {
                    built.seed = seed_elapsed;
                    built
                })
                .inspect_err(|e| error!(target: "engine", ?e, "error building consensus output"));
            // the receiver is gone only when the pipeline was dropped (node shutdown)
            let _ = tx.send(result);
            Ok(())
        });
        self.building = Some(BuildSlot { rx, started: Instant::now(), seeded });
        Ok(())
    }

    /// [`Self::start`] recovering the output's signers inline on the build thread (tests and
    /// oracles).
    ///
    /// # Errors
    ///
    /// As [`Self::start`].
    pub fn start_inline(&mut self, output: ConsensusOutput) -> EngineResult<()> {
        self.start(output, None)
    }

    /// Whether nothing is in flight: no build, no parked output, no commit, no event or error
    /// waiting for the caller. [`Self::next_event`] never resolves while this holds.
    pub fn is_idle(&self) -> bool {
        self.building.is_none()
            && self.built.is_none()
            && self.committing.is_none()
            && self.events.is_empty()
            && self.settled_error.is_none()
    }

    /// The last block built: the parent of the next output.
    pub fn in_memory_tip(&self) -> &SealedHeader {
        &self.tip
    }

    /// The last block committed and announced.
    pub fn durable_tip(&self) -> &SealedHeader {
        &self.durable
    }

    /// Outputs started and not committed: building, parked or committing (0-2).
    pub fn unpersisted_outputs(&self) -> usize {
        self.slots.unpersisted_outputs()
    }

    /// The closing output holding the next build until it commits, if any.
    pub fn barrier(&self) -> Option<u64> {
        self.slots.barrier
    }

    /// Wait for the next transition.
    ///
    /// Cancel-safe: the slots' receivers stay in the pipeline, and a transition is applied in
    /// full before the event is returned. Pending forever when idle, so gate it on
    /// `!is_idle()`.
    ///
    /// # Errors
    ///
    /// The first error of a failed build or commit (or a [`TnEngineError::PipelineInvariant`]),
    /// once both slots have settled and every unpersisted block has been rolled back. The
    /// events of transitions that happened before it are returned first.
    pub async fn next_event(&mut self) -> EngineResult<PipelineEvent> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Ok(event);
            }
            if let Some(error) = self.settled_error.take() {
                return Err(error);
            }
            if self.building.is_none() && self.committing.is_none() {
                return std::future::pending().await;
            }
            match wait_slots(&mut self.building, &mut self.committing).await {
                SlotDone::Build(result) => self.on_build_done(*result),
                SlotDone::Commit(result) => self.on_commit_done(*result),
            }
        }
    }

    /// TEST-ONLY: hold every commit task at its start until the returned gate releases it, so a
    /// test can put the pipeline in a chosen state (N committing, N+1 built) and arm persist
    /// faults. Dropping the gate releases every commit.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn hold_commits(&mut self) -> CommitGate {
        let state = std::sync::Arc::new(GateState::default());
        self.gate = Some(state.clone());
        CommitGate { state }
    }

    /// Take the committing output's snapshot and blocks for the next build, checking that they
    /// describe exactly the blocks between the snapshot and the in-memory tip.
    fn take_seed(&mut self) -> Result<Seed, &'static str> {
        let tip_hash = self.tip.hash();
        let Some(carry) = self.carry.as_mut().filter(|carry| carry.tip_hash == tip_hash) else {
            return Err("a seeded start without the committing output's snapshot");
        };
        let first_parent = carry.blocks.first().map(|block| {
            (block.recovered_block.number.checked_sub(1), block.recovered_block.parent_hash)
        });
        if first_parent != Some((Some(carry.anchor.number), carry.anchor.hash)) {
            return Err(
                "the pinned snapshot is not the parent of the committing output's first block",
            );
        }
        let Some(overlay) = carry.overlay.take() else {
            return Err("the committing output's snapshot already seeded a build");
        };
        Ok(Seed { overlay, blocks: carry.blocks.clone(), tip: tip_hash })
    }

    /// A build finished (T2, or T5 on an error).
    fn on_build_done(
        &mut self,
        result: Result<EngineResult<BuiltOutput>, oneshot::error::RecvError>,
    ) {
        let Some(slot) = self.building.take() else {
            return;
        };
        match result.map_err(TnEngineError::from).and_then(|built| built) {
            Ok(built) => {
                let number = built.consensus_number;
                if !built.blocks.is_empty() {
                    self.unpersisted.push_back((number, built.blocks.clone()));
                }
                self.tip = built.tip.clone();
                let slot_output = SlotOutput {
                    number,
                    anchor: built.anchor.hash(),
                    tip: built.tip.hash(),
                    close_epoch: built.close_epoch,
                };
                let report = BuildReport {
                    consensus_number: number,
                    blocks: built.blocks.len(),
                    tip: built.tip.clone(),
                    close_epoch: built.close_epoch,
                    execute: built.execute,
                    seeded: slot.seeded,
                    seed: built.seed,
                };
                self.built =
                    Some(Finished { built, build_started: slot.started, built_at: Instant::now() });
                let step = self.slots.built(slot_output);
                // an output built while a failure settles is rolled back, never reported
                if self.slots.failure.is_none() {
                    self.events.push_back(PipelineEvent::Built(Box::new(report)));
                }
                self.apply(step);
            }
            Err(e) => {
                // the build rolled its own blocks back to its parent before returning
                self.record_error(e);
                let step = self.slots.build_failed();
                self.apply(step);
            }
        }
    }

    /// A commit finished (T4, or T6/T7 on an error).
    fn on_commit_done(&mut self, result: Result<CommitDone, oneshot::error::RecvError>) {
        let Some(slot) = self.committing.take() else {
            return;
        };
        match result {
            Ok(CommitDone { built, result: Ok(committed), committed_at }) => {
                let number = built.consensus_number;
                let parent = std::mem::replace(&mut self.durable, committed.tip.clone());
                self.unpersisted.retain(|(output, _)| *output != number);
                self.carry = None;
                self.events.push_back(PipelineEvent::Committed(Box::new(CommitReport {
                    consensus_number: number,
                    blocks: built.blocks.len(),
                    txs: built.txs,
                    parent,
                    tip: committed.tip,
                    close_epoch: built.close_epoch,
                    execute: built.execute,
                    persist_wait: slot.persist_wait,
                    persist: committed.persist,
                    announce: committed.announce,
                    build_started: slot.build_started,
                    committed_at,
                })));
                let step = self.slots.committed(number);
                self.apply(step);
            }
            Ok(CommitDone { result: Err(CommitFailure::Persist(e)), .. }) => {
                self.carry = None;
                self.record_error(e);
                let step = self.slots.commit_failed(FailureKind::Persist);
                self.apply(step);
            }
            Ok(CommitDone { built, result: Err(CommitFailure::PostCommit(e)), .. }) => {
                // the blocks are durable: they stay, whatever failed after the commit
                self.durable = built.tip.clone();
                self.unpersisted.retain(|(output, _)| *output != built.consensus_number);
                self.carry = None;
                self.record_error(e);
                let step = self.slots.commit_failed(FailureKind::PostCommit);
                self.apply(step);
            }
            Err(e) => {
                // the commit task ended without a result: the committed database head says
                // whether its persist landed (a durable block must never be rolled back, and a
                // block that is not durable must not stay as the in-memory head)
                self.carry = None;
                self.record_error(e.into());
                let durable =
                    self.reth_env.lookup_head().is_ok_and(|head| head.number >= slot.tip.number);
                let kind = if durable {
                    self.durable = slot.tip;
                    self.unpersisted.retain(|(output, _)| *output != slot.number);
                    FailureKind::PostCommit
                } else {
                    FailureKind::Persist
                };
                let step = self.slots.commit_failed(kind);
                self.apply(step);
            }
        }
    }

    /// Carry out what the slots decided.
    fn apply(&mut self, step: Step) {
        match step {
            Step::Wait | Step::Park => {}
            Step::Dispatch(number) => match self.built.take() {
                Some(finished) if finished.built.consensus_number == number => {
                    self.dispatch(finished)
                }
                other => {
                    // unreachable: the slots dispatch exactly the output parked here
                    self.built = other;
                    let step =
                        self.slots.refuse_dispatch("the output to dispatch is not the one built");
                    self.apply(step);
                }
            },
            Step::Fail(settled) => self.settle(settled),
        }
    }

    /// T3: pin the snapshot the next build reads, then spawn `finished`'s commit.
    fn dispatch(&mut self, finished: Finished) {
        let number = finished.built.consensus_number;
        if !finished.built.blocks.is_empty() {
            // pin BEFORE spawning the commit: the snapshot must see the database as of the
            // durable tip, never this output's commit
            match self.reth_env.pin_output_overlay() {
                Ok(overlay) if overlay.anchor_block() == Some(self.durable.num_hash()) => {
                    let tip = finished.built.tip.hash();
                    self.carry = Some(Carry {
                        overlay: Some(overlay),
                        blocks: finished.built.blocks.clone(),
                        tip_hash: tip,
                        anchor: self.durable.num_hash(),
                    });
                    self.slots.pinned(tip);
                }
                Ok(_) => {
                    // the committed database is not at the durable tip: persisting would break
                    // the durable order
                    self.built = Some(finished);
                    let step =
                        self.slots.refuse_dispatch("the pinned snapshot is not at the durable tip");
                    self.apply(step);
                    return;
                }
                Err(err) => {
                    // no overlap for one output: the next build waits for this commit
                    warn!(
                        target: "engine",
                        consensus_number = number,
                        %err,
                        "pinning the output overlay failed - next output waits for the commit"
                    );
                    self.carry = None;
                }
            }
        }

        let dispatched = Instant::now();
        let waited = dispatched.saturating_duration_since(finished.built_at);
        let tip = finished.built.tip.clone();
        let Finished { built, build_started, .. } = finished;
        let (tx, rx) = oneshot::channel();
        let reth_env = self.reth_env.clone();
        let engine_update_tx = self.engine_update_tx.clone();
        #[cfg(any(test, feature = "test-utils"))]
        let gate = self.gate.clone();
        self.task_spawner.spawn_blocking_task(format!("commit-output-{number}"), move || {
            #[cfg(any(test, feature = "test-utils"))]
            if let Some(gate) = gate {
                gate.pass();
            }
            let result = commit_output(&reth_env, &built, &engine_update_tx, waited, true);
            let committed_at = Instant::now();
            // the receiver is gone only when the pipeline was dropped (node shutdown)
            let _ = tx.send(CommitDone { built, result, committed_at });
            Ok(())
        });
        self.committing = Some(CommitSlot { rx, number, tip, build_started, persist_wait: waited });
        self.events.push_back(PipelineEvent::PersistStarted { consensus_number: number, waited });
    }

    /// Keep the first error of a failure; log and drop any later one.
    fn record_error(&mut self, e: TnEngineError) {
        if self.first_error.is_none() {
            self.first_error = Some(e);
        } else {
            warn!(target: "engine", ?e, "pipelined persist: a later error while settling a failure");
        }
    }

    /// Both slots have settled after a failure: roll every unpersisted block back to the durable
    /// tip in one call and keep the first error for the caller.
    fn settle(&mut self, settled: Settled) {
        let error = match settled.first {
            FailureKind::Invariant(check) => {
                if let Some(e) = self.first_error.take() {
                    warn!(target: "engine", ?e, "pipelined persist: a later error while settling a failure");
                }
                TnEngineError::PipelineInvariant(check)
            }
            FailureKind::Build | FailureKind::Persist | FailureKind::PostCommit => self
                .first_error
                .take()
                .unwrap_or(TnEngineError::PipelineInvariant("a failure settled without its error")),
        };
        let mut outputs = 0usize;
        let mut blocks = Vec::new();
        let mut oldest = None;
        for number in &settled.rollback {
            if let Some(position) = self.unpersisted.iter().position(|(output, _)| output == number)
            {
                if let Some((output, output_blocks)) = self.unpersisted.remove(position) {
                    oldest = oldest.or(Some(output));
                    outputs += 1;
                    blocks.extend(output_blocks);
                }
            }
        }
        if !blocks.is_empty() {
            error!(
                target: "engine",
                consensus_number = oldest.unwrap_or_default(),
                rolled_back_outputs = outputs,
                rolled_back_blocks = blocks.len(),
                durable_number = self.durable.number,
                "pipelined persist failed - rolling back unpersisted outputs"
            );
            rollback_in_memory_output(
                &self.reth_env.canonical_in_memory_state(),
                &self.durable,
                &blocks,
            );
        }
        self.tip = self.durable.clone();
        self.built = None;
        self.carry = None;
        self.unpersisted.clear();
        self.settled_error = Some(error);
    }
}

/// A transition the caller may want to record.
#[derive(Debug)]
pub enum PipelineEvent {
    /// An output finished building and is in memory.
    Built(Box<BuildReport>),
    /// An output's commit task was spawned after it waited `waited` for the commit slot.
    PersistStarted {
        /// The output's consensus number.
        consensus_number: u64,
        /// Time from the end of its build to the spawn of its commit.
        waited: Duration,
    },
    /// An output committed and was announced.
    Committed(Box<CommitReport>),
}

/// An output that finished building.
#[derive(Debug, Clone)]
pub struct BuildReport {
    /// The output's consensus number.
    pub consensus_number: u64,
    /// Blocks built (zero for a skipped empty output).
    pub blocks: usize,
    /// The in-memory tip after the output.
    pub tip: SealedHeader,
    /// Whether the output closes the epoch.
    pub close_epoch: bool,
    /// The build's `execute_ms` span.
    pub execute: Duration,
    /// Whether the build was seeded with the committing output's deltas.
    pub seeded: bool,
    /// Time spent seeding the build's overlay (zero unseeded).
    pub seed: Duration,
}

/// An output that committed and was announced.
#[derive(Debug, Clone)]
pub struct CommitReport {
    /// The output's consensus number.
    pub consensus_number: u64,
    /// Blocks committed (zero for a skipped empty output).
    pub blocks: usize,
    /// Transactions handed to the block builder.
    pub txs: usize,
    /// The durable tip before the commit.
    pub parent: SealedHeader,
    /// The durable tip after the commit.
    pub tip: SealedHeader,
    /// Whether the output closes the epoch.
    pub close_epoch: bool,
    /// The build's `execute_ms` span.
    pub execute: Duration,
    /// Time from the end of the build to the spawn of the commit.
    pub persist_wait: Duration,
    /// The persist, retries included.
    pub persist: Duration,
    /// The announce plus the in-memory finalize.
    pub announce: Duration,
    /// When the output started building.
    pub build_started: Instant,
    /// When the commit task finished.
    pub committed_at: Instant,
}

/// TEST-ONLY: releases the commit tasks held by [`PersistPipeline::hold_commits`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug)]
pub struct CommitGate {
    /// Shared with every commit task spawned after the gate was installed.
    state: std::sync::Arc<GateState>,
}

#[cfg(any(test, feature = "test-utils"))]
impl CommitGate {
    /// Let one held (or the next) commit task start.
    pub fn release_one(&self) {
        self.state.update(|count| count.permits += 1);
    }

    /// Let every held and future commit task start.
    pub fn release_all(&self) {
        self.state.update(|count| count.open = true);
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for CommitGate {
    fn drop(&mut self) {
        self.release_all();
    }
}

/// The permits behind a [`CommitGate`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default)]
struct GateState {
    /// Permits not yet taken and whether the gate is open for good.
    count: std::sync::Mutex<GateCount>,
    /// Wakes the commit tasks waiting at the gate.
    released: std::sync::Condvar,
}

/// [`GateState`]'s counters.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default)]
struct GateCount {
    /// Commits allowed to start and not yet started.
    permits: usize,
    /// Every commit may start.
    open: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl GateState {
    /// Change the counters and wake every waiting commit task.
    fn update(&self, change: impl FnOnce(&mut GateCount)) {
        let mut count = self.count.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        change(&mut count);
        self.released.notify_all();
    }

    /// Block the calling commit task until the gate lets it start.
    fn pass(&self) {
        let mut count = self.count.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while !count.open && count.permits == 0 {
            count = self.released.wait(count).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if !count.open {
            count.permits -= 1;
        }
    }
}

/// The output building and what its events need.
#[derive(Debug)]
struct BuildSlot {
    /// The build task's result.
    rx: oneshot::Receiver<EngineResult<BuiltOutput>>,
    /// When the build was started.
    started: Instant,
    /// Whether the build was seeded with the committing output's deltas.
    seeded: bool,
}

/// An output that finished building and has not been dispatched.
#[derive(Debug)]
struct Finished {
    /// The built output.
    built: BuiltOutput,
    /// When it started building.
    build_started: Instant,
    /// When its build result arrived.
    built_at: Instant,
}

/// The output committing; the output itself is in the commit task.
#[derive(Debug)]
struct CommitSlot {
    /// The commit task's result, with the output handed back.
    rx: oneshot::Receiver<CommitDone>,
    /// The output's consensus number.
    number: u64,
    /// The output's last block.
    tip: SealedHeader,
    /// When the output started building.
    build_started: Instant,
    /// Time from the end of the build to the spawn of the commit.
    persist_wait: Duration,
}

/// What a commit task hands back.
#[derive(Debug)]
struct CommitDone {
    /// The output it committed (or failed to).
    built: BuiltOutput,
    /// What the commit did.
    result: Result<Committed, CommitFailure>,
    /// When the commit task finished.
    committed_at: Instant,
}

/// The committing output's pinned snapshot and blocks: what the next output's build layers
/// under its own deltas while the committing output is not yet durable.
#[derive(Debug)]
struct Carry {
    /// The snapshot pinned before the commit task was spawned; taken by the one build it seeds.
    overlay: Option<OutputTrieOverlay>,
    /// The committing output's blocks, in order.
    blocks: Vec<ExecutedBlock>,
    /// The committing output's last block.
    tip_hash: B256,
    /// The block the snapshot was pinned at (the durable tip at dispatch).
    anchor: BlockNumHash,
}

/// What a seeded build starts from.
#[derive(Debug)]
struct Seed {
    /// The pinned snapshot, with no runs yet.
    overlay: OutputTrieOverlay,
    /// The committing output's blocks, whose deltas go into the overlay.
    blocks: Vec<ExecutedBlock>,
    /// The committing output's last block: the seeded scope's only parent.
    tip: B256,
}

impl Seed {
    /// Layer the committing output's deltas over the snapshot and hand the build a scope whose
    /// first block must build on that output's last block.
    fn into_scope(self) -> OutputBuildScope {
        let Self { mut overlay, blocks, tip } = self;
        blocks.iter().for_each(|block| overlay.extend_from_block(&block.trie_data()));
        OutputBuildScope::seeded(overlay, tip)
    }
}

/// Which slot finished.
enum SlotDone {
    /// The build slot's result.
    Build(Box<Result<EngineResult<BuiltOutput>, oneshot::error::RecvError>>),
    /// The commit slot's result.
    Commit(Box<Result<CommitDone, oneshot::error::RecvError>>),
}

/// Wait for the first of the busy slots to finish (commits first when both have).
///
/// Cancel-safe: only `&mut` receivers are awaited.
async fn wait_slots(
    building: &mut Option<BuildSlot>,
    committing: &mut Option<CommitSlot>,
) -> SlotDone {
    tokio::select! {
        biased;
        result = recv(committing.as_mut().map(|slot| &mut slot.rx)) => SlotDone::Commit(Box::new(result)),
        result = recv(building.as_mut().map(|slot| &mut slot.rx)) => SlotDone::Build(Box::new(result)),
    }
}

/// Await `rx`, or never resolve when there is none.
async fn recv<T>(rx: Option<&mut oneshot::Receiver<T>>) -> Result<T, oneshot::error::RecvError> {
    match rx {
        Some(rx) => rx.await,
        None => std::future::pending().await,
    }
}

/// The pipeline's ordering rules, without I/O: which output may start, park, dispatch, and what
/// a failure rolls back. Outputs are named by consensus number and blocks by hash.
#[derive(Debug, Default)]
struct Slots {
    /// The in-memory tip: the parent of the next build.
    tip: B256,
    /// The durable tip: the parent every dispatched output must have.
    durable: B256,
    /// The output building.
    building: Option<u64>,
    /// The output built and waiting for the commit slot.
    built: Option<SlotOutput>,
    /// The output committing.
    committing: Option<SlotOutput>,
    /// The last block of the committing output whose snapshot is pinned, while it is not
    /// durable.
    carry: Option<B256>,
    /// The closing output no build may start on until it commits.
    barrier: Option<u64>,
    /// The consensus number of the last output started.
    last_started: Option<u64>,
    /// The first failure, once one happened.
    failure: Option<FailureKind>,
    /// Outputs in memory and not durable that the failure rolls back, oldest first.
    unwind: Vec<u64>,
}

impl Slots {
    /// Idle slots on the durable tip `tip`.
    fn new(tip: B256) -> Self {
        Self { tip, durable: tip, ..Default::default() }
    }

    /// T1's guard.
    fn can_start(&self) -> bool {
        self.building.is_none()
            && self.built.is_none()
            && self.barrier.is_none()
            && self.failure.is_none()
            && (self.committing.is_none()
                || self.tip == self.durable
                || self.carry == Some(self.tip))
    }

    /// Whether the next build must be seeded: an output with blocks is committing, so the
    /// in-memory tip is not durable.
    fn needs_seed(&self) -> bool {
        self.committing.is_some() && self.tip != self.durable
    }

    /// Outputs started and not committed.
    fn unpersisted_outputs(&self) -> usize {
        usize::from(self.building.is_some())
            + usize::from(self.built.is_some())
            + usize::from(self.committing.is_some())
    }

    /// Refuse an output that does not follow the last one started: the outputs are tracked by
    /// consensus number, and the durable order is the order they start in.
    fn check_order(&self, number: u64) -> EngineResult<()> {
        if self.last_started.is_some_and(|last| number <= last) {
            return Err(TnEngineError::PipelineInvariant(
                "an output was started out of consensus order",
            ));
        }
        Ok(())
    }

    /// T1: `number` starts building on the in-memory tip; returns whether it is seeded.
    fn start(&mut self, number: u64) -> EngineResult<bool> {
        if !self.can_start() {
            return Err(TnEngineError::PipelineInvariant(
                "an output was started while the pipeline could not start one",
            ));
        }
        self.check_order(number)?;
        self.building = Some(number);
        self.last_started = Some(number);
        Ok(self.needs_seed())
    }

    /// T2: the building output finished; park it, dispatch it, or settle a pending failure.
    fn built(&mut self, output: SlotOutput) -> Step {
        self.building = None;
        let on_tip = output.anchor == self.tip;
        self.tip = output.tip;
        if output.close_epoch {
            self.barrier = Some(output.number);
        }
        if self.failure.is_some() {
            // a commit failed while this output built: it joins the rollback
            self.unwind.push(output.number);
            return self.settle_or_wait();
        }
        if !on_tip {
            self.unwind.push(output.number);
            return self.fail(FailureKind::Invariant(
                "an output was built on a block other than the in-memory tip",
            ));
        }
        if self.committing.is_some() {
            self.built = Some(output);
            Step::Park
        } else {
            self.dispatch(output)
        }
    }

    /// T3: hand `output` to the commit slot, refusing an output not built on the durable tip.
    fn dispatch(&mut self, output: SlotOutput) -> Step {
        if output.anchor != self.durable {
            self.unwind.push(output.number);
            return self.fail(FailureKind::Invariant(
                "an output was dispatched that was not built on the durable tip",
            ));
        }
        self.committing = Some(output);
        Step::Dispatch(output.number)
    }

    /// The caller could not dispatch the output it was told to (I/O-side check).
    fn refuse_dispatch(&mut self, check: &'static str) -> Step {
        if let Some(output) = self.committing.take() {
            self.unwind.push(output.number);
        }
        self.fail(FailureKind::Invariant(check))
    }

    /// T3's pin succeeded: the next build may seed from the committing output ending at `tip`.
    fn pinned(&mut self, tip: B256) {
        self.carry = Some(tip);
    }

    /// T4: the committing output `number` committed; dispatch the parked output next.
    fn committed(&mut self, number: u64) -> Step {
        let Some(output) = self.committing.take() else {
            return self
                .fail(FailureKind::Invariant("a commit finished with no output committing"));
        };
        if output.number != number {
            return self.fail(FailureKind::Invariant("a commit finished for another output"));
        }
        self.durable = output.tip;
        self.carry = None;
        if self.barrier == Some(number) {
            self.barrier = None;
        }
        if self.failure.is_some() {
            return self.settle_or_wait();
        }
        match self.built.take() {
            Some(parked) => self.dispatch(parked),
            None => Step::Wait,
        }
    }

    /// T5: the building output failed (its own blocks are already rolled back).
    fn build_failed(&mut self) -> Step {
        self.building = None;
        self.fail(FailureKind::Build)
    }

    /// T6/T7: the committing output failed before (`Persist`) or after (`PostCommit`) its
    /// database commit.
    fn commit_failed(&mut self, kind: FailureKind) -> Step {
        self.carry = None;
        if let Some(output) = self.committing.take() {
            if kind == FailureKind::PostCommit {
                self.durable = output.tip;
                if self.barrier == Some(output.number) {
                    self.barrier = None;
                }
            } else {
                self.unwind.push(output.number);
            }
        }
        self.fail(kind)
    }

    /// Record `kind` unless an earlier failure won, move a parked output into the rollback, and
    /// settle once no slot is busy.
    fn fail(&mut self, kind: FailureKind) -> Step {
        if self.failure.is_none() {
            self.failure = Some(kind);
        }
        if let Some(parked) = self.built.take() {
            self.unwind.push(parked.number);
        }
        self.settle_or_wait()
    }

    /// Wait while a slot is busy; otherwise roll everything unpersisted back to the durable tip.
    fn settle_or_wait(&mut self) -> Step {
        let Some(first) = self.failure else {
            return Step::Wait;
        };
        if self.building.is_some() || self.committing.is_some() {
            return Step::Wait;
        }
        self.tip = self.durable;
        Step::Fail(Settled { first, rollback: std::mem::take(&mut self.unwind), to: self.durable })
    }
}

/// What the slots know about one output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlotOutput {
    /// The output's consensus number.
    number: u64,
    /// The parent of its first block (its pre-output tip).
    anchor: B256,
    /// Its last block (the anchor when it built none).
    tip: B256,
    /// Whether it closes the epoch.
    close_epoch: bool,
}

/// Which failure came first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    /// A build failed.
    Build,
    /// A persist failed: nothing of that output is durable.
    Persist,
    /// The announce or finalize failed after the commit, or the commit task ended without a
    /// result.
    PostCommit,
    /// An ordering check failed.
    Invariant(&'static str),
}

/// What the caller does after a transition.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// Nothing until the next slot finishes.
    Wait,
    /// Keep the output just built parked.
    Park,
    /// Dispatch the parked output with this consensus number.
    Dispatch(u64),
    /// Every slot has settled after a failure.
    Fail(Settled),
}

/// A failure whose slots have all settled.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Settled {
    /// The failure that came first: its error is returned.
    first: FailureKind,
    /// Outputs to roll back, oldest first.
    rollback: Vec<u64>,
    /// The block they roll back to: the durable tip.
    to: B256,
}

#[cfg(test)]
mod tests {
    //! The transition table (`Slots`, no database) and the pipeline over a temp chain.

    use super::*;
    use crate::{execute_consensus_output, PERSIST_OUTPUT_ATTEMPTS};
    use assert_matches::assert_matches;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_reth::{
        error::TnRethError,
        test_utils::{
            batches, create_committee_from_state, seeded_genesis_from_random_batches,
            test_genesis_with_consensus_registry,
        },
        RethChainSpec,
    };
    use tn_types::{
        now, test_chain_spec_arc, Address, AuthorityIdentifier, Batch, Certificate, CertifiedBatch,
        CommittedSubDag, ConsensusHeaderDigest, EpochSeedChainValue, ReputationScores, TaskManager,
        U256,
    };

    // ---- the transition table ----

    /// A distinct block hash per `n`.
    fn h(n: u64) -> B256 {
        B256::from(U256::from(n))
    }

    /// An output built on `anchor` ending at `tip`.
    fn out(number: u64, anchor: B256, tip: B256) -> SlotOutput {
        SlotOutput { number, anchor, tip, close_epoch: false }
    }

    /// A built output goes to the commit slot when it is free and parks while it is busy; with
    /// an output parked nothing more may start.
    #[test]
    fn test_slots_park_built_output_while_commit_busy() -> EngineResult<()> {
        let mut slots = Slots::new(h(0));
        assert!(!slots.start(1)?, "nothing committing: unseeded");
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        assert!(slots.can_start(), "the pinned snapshot lets the next output start");
        assert!(slots.start(2)?, "output 1 is not durable: seeded");
        assert_eq!(slots.unpersisted_outputs(), 2);
        assert_eq!(slots.built(out(2, h(1), h(2))), Step::Park);
        assert!(!slots.can_start(), "an output is parked");
        assert_eq!(slots.unpersisted_outputs(), 2);
        assert_matches!(slots.start(3), Err(TnEngineError::PipelineInvariant(_)));

        // an output that does not follow the last one started is refused
        assert_eq!(slots.committed(1), Step::Dispatch(2));
        assert_eq!(slots.committed(2), Step::Wait);
        assert!(slots.can_start());
        assert_matches!(slots.start(2), Err(TnEngineError::PipelineInvariant(_)));
        assert!(!slots.start(5)?, "a later output starts");
        Ok(())
    }

    /// A tiny deterministic generator for the event interleavings.
    struct XorShift(u64);

    impl XorShift {
        /// The next value in `0..bound`.
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    /// A commit with an output parked dispatches it next, and over random interleavings of
    /// starts, builds, commits, empty outputs and failed pins, outputs dispatch and commit in
    /// build order, each on the durable tip, with at most two outputs not durable.
    #[test]
    fn test_slots_commit_dispatches_parked_output_in_order() -> EngineResult<()> {
        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        slots.start(2)?;
        assert_eq!(slots.built(out(2, h(1), h(2))), Step::Park);
        assert_eq!(slots.committed(1), Step::Dispatch(2));
        assert_eq!(slots.committed(2), Step::Wait);

        for seed in 1..=64u64 {
            let mut rng = XorShift(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            let mut slots = Slots::new(h(0));
            let (mut next, mut fresh) = (1u64, 1_000u64);
            let mut building: Option<SlotOutput> = None;
            let (mut dispatched, mut committed) = (Vec::new(), Vec::new());
            while committed.len() < 40 {
                match rng.below(3) {
                    0 if slots.can_start() && next <= 40 => {
                        slots.start(next)?;
                        let tip = if rng.below(4) == 0 {
                            slots.tip
                        } else {
                            fresh += 1;
                            h(fresh)
                        };
                        building = Some(out(next, slots.tip, tip));
                        next += 1;
                    }
                    1 => {
                        if let Some(output) = building.take() {
                            let step = slots.built(output);
                            assert_ne!(step, Step::Wait, "seed {seed}: a build stalled");
                            if let Step::Dispatch(number) = step {
                                dispatched.push(number);
                                if output.tip != output.anchor && rng.below(5) != 0 {
                                    slots.pinned(output.tip);
                                }
                            }
                        }
                    }
                    _ => {
                        if let Some(output) = slots.committing {
                            match slots.committed(output.number) {
                                Step::Dispatch(number) => {
                                    dispatched.push(number);
                                    let parked =
                                        slots.committing.expect("the parked output is committing");
                                    if parked.tip != parked.anchor && rng.below(5) != 0 {
                                        slots.pinned(parked.tip);
                                    }
                                }
                                Step::Wait => {}
                                other => panic!("seed {seed}: unexpected step {other:?}"),
                            }
                            committed.push(output.number);
                        }
                    }
                }
                assert!(slots.unpersisted_outputs() <= 2, "seed {seed}: more than two in flight");
                assert!(slots.failure.is_none(), "seed {seed}: {:?}", slots.failure);
            }
            let in_order: Vec<u64> = (1..=40).collect();
            assert_eq!(dispatched, in_order, "seed {seed}: dispatch order");
            assert_eq!(committed, in_order, "seed {seed}: commit order");
        }
        Ok(())
    }

    /// With an output committing and no pinned snapshot (the pin failed), nothing starts until
    /// the commit, and then the next output starts unseeded.
    #[test]
    fn test_slots_start_refused_without_carry_while_committing() -> EngineResult<()> {
        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        assert!(!slots.can_start(), "no snapshot to seed from");
        assert_matches!(slots.start(2), Err(TnEngineError::PipelineInvariant(_)));
        assert_eq!(slots.committed(1), Step::Wait);
        assert!(slots.can_start());
        assert!(!slots.start(2)?, "output 1 is durable: unseeded");
        Ok(())
    }

    /// A closing output holds the next start from its build through its dispatch, whether it
    /// dispatches at once or parks first, and releases it at its commit.
    #[test]
    fn test_slots_close_epoch_holds_next_start_until_commit() -> EngineResult<()> {
        let closing =
            |number, anchor, tip| SlotOutput { close_epoch: true, ..out(number, anchor, tip) };

        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(closing(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        assert_eq!(slots.barrier, Some(1));
        assert!(!slots.can_start(), "the closing output is not durable");
        assert_eq!(slots.committed(1), Step::Wait);
        assert_eq!(slots.barrier, None);
        assert!(slots.can_start());

        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        slots.start(2)?;
        assert_eq!(slots.built(closing(2, h(1), h(2))), Step::Park);
        assert!(!slots.can_start());
        assert_eq!(slots.committed(1), Step::Dispatch(2));
        slots.pinned(h(2));
        assert!(!slots.can_start(), "the closing output is dispatched, not durable");
        assert_eq!(slots.committed(2), Step::Wait);
        assert!(slots.can_start());
        Ok(())
    }

    /// I1/I11: an output whose parent is not the durable tip is never dispatched; the check
    /// fails closed with an ordering error and the output is rolled back.
    #[test]
    fn test_dispatch_refuses_output_not_built_on_durable_tip() {
        let mut slots = Slots::new(h(0));
        let step = slots.dispatch(out(2, h(1), h(2)));
        assert_matches!(
            step,
            Step::Fail(Settled { first: FailureKind::Invariant(check), rollback, to })
                if check.contains("durable tip") && rollback == vec![2] && to == h(0)
        );
        assert!(slots.committing.is_none(), "the refused output never reached the commit slot");
        assert!(!slots.can_start(), "the pipeline is spent");
        assert!(TnEngineError::PipelineInvariant("x")
            .to_string()
            .starts_with("pipelined persist ordering check failed"));

        // a build on anything but the in-memory tip fails the same way
        let mut slots = Slots::new(h(0));
        slots.start(1).expect("idle slots start");
        assert_matches!(
            slots.built(out(1, h(7), h(1))),
            Step::Fail(Settled { first: FailureKind::Invariant(_), rollback, .. })
                if rollback == vec![1]
        );
    }

    /// The first error wins and waits for the other slot: a persist failure with a build busy
    /// settles only when the build does, with one rollback of both outputs to the durable tip;
    /// a build failure followed by a persist failure keeps the build error.
    #[test]
    fn test_slots_first_error_wins_and_waits_for_other_slot() -> EngineResult<()> {
        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        slots.start(2)?;
        assert_eq!(slots.commit_failed(FailureKind::Persist), Step::Wait, "the build is busy");
        assert!(!slots.can_start());
        assert_eq!(
            slots.built(out(2, h(1), h(2))),
            Step::Fail(Settled { first: FailureKind::Persist, rollback: vec![1, 2], to: h(0) })
        );
        assert_eq!(slots.tip, h(0), "the in-memory tip is back at the durable tip");

        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        slots.start(2)?;
        assert_eq!(slots.build_failed(), Step::Wait, "the commit is busy");
        assert_eq!(
            slots.commit_failed(FailureKind::Persist),
            Step::Fail(Settled { first: FailureKind::Build, rollback: vec![1], to: h(0) })
        );

        // a post-commit failure keeps the committed output and rolls back the parked one
        let mut slots = Slots::new(h(0));
        slots.start(1)?;
        assert_eq!(slots.built(out(1, h(0), h(1))), Step::Dispatch(1));
        slots.pinned(h(1));
        slots.start(2)?;
        assert_eq!(slots.built(out(2, h(1), h(2))), Step::Park);
        assert_eq!(
            slots.commit_failed(FailureKind::PostCommit),
            Step::Fail(Settled { first: FailureKind::PostCommit, rollback: vec![2], to: h(1) })
        );
        Ok(())
    }

    // ---- the pipeline over a temp chain ----

    /// One node's execution side on a fresh temp chain.
    struct Node {
        /// The node's execution environment.
        reth_env: RethEnv,
        /// Reward accounting, with the genesis committee installed.
        gas_accumulator: GasAccumulator,
        /// The genesis header: the durable tip before any output.
        genesis: SealedHeader,
        /// Spawns the pipeline's tasks.
        tasks: TaskManager,
        /// Keeps the database directory alive.
        _dir: TempDir,
    }

    impl Node {
        /// A node on `chain` with its committee installed in the gas accumulator.
        async fn new(chain: &Arc<RethChainSpec>) -> eyre::Result<Self> {
            let dir = TempDir::new()?;
            let tasks = TaskManager::new("persister tests");
            let gas_accumulator = GasAccumulator::new(1);
            let reth_env = RethEnv::new_for_temp_chain(
                chain.clone(),
                dir.path(),
                &tasks,
                Some(gas_accumulator.clone()),
            )?;
            let committee =
                create_committee_from_state(reth_env.epoch_state_from_canonical_tip()?).await?;
            gas_accumulator.rewards_counter().set_committee(committee);
            let genesis = chain.sealed_genesis_header();
            Ok(Self { reth_env, gas_accumulator, genesis, tasks, _dir: dir })
        }

        /// A pipeline on the node's genesis, announcing on `engine_update_tx`.
        fn pipeline(&self, engine_update_tx: mpsc::Sender<EngineUpdate>) -> PersistPipeline {
            PersistPipeline::new(
                self.reth_env.clone(),
                self.genesis.clone(),
                self.tasks.get_spawner(),
                self.gas_accumulator.clone(),
                RepackMonitor::default(),
                engine_update_tx,
            )
        }

        /// The committee member that leads every output and produces every batch.
        async fn leader(&self) -> eyre::Result<(AuthorityIdentifier, Address)> {
            let committee =
                create_committee_from_state(self.reth_env.epoch_state_from_canonical_tip()?)
                    .await?;
            let authority = committee.authorities().first().cloned().expect("a committee member");
            Ok((authority.id(), authority.execution_address()))
        }

        /// The committed blocks `1..=last` and the trie tables' digest.
        fn committed(&self, last: u64) -> eyre::Result<(Vec<SealedHeader>, B256)> {
            let headers = (1..=last)
                .map(|number| {
                    self.reth_env
                        .sealed_header_by_number(number)?
                        .ok_or_else(|| eyre::eyre!("block {number} is not committed"))
                })
                .collect::<eyre::Result<Vec<_>>>()?;
            Ok((headers, self.reth_env.trie_tables_digest()?))
        }
    }

    /// A chain whose genesis funds the senders of `count` random batches, and those batches.
    fn funded_chain(count: usize) -> (Arc<RethChainSpec>, Vec<Batch>) {
        let batches = batches(test_chain_spec_arc(), count);
        let (genesis, _, _) = seeded_genesis_from_random_batches(
            test_genesis_with_consensus_registry(4),
            batches.iter(),
        );
        (Arc::new(genesis.into()), batches)
    }

    /// Consensus output `number` led by `leader` (the default, non-member author when `None`)
    /// with `batches` produced by `producer`.
    fn output(
        number: u64,
        leader: Option<&AuthorityIdentifier>,
        producer: Address,
        batches: Vec<Batch>,
        close_epoch: bool,
    ) -> ConsensusOutput {
        let mut certificate = Certificate::default();
        if let Some(leader) = leader {
            certificate.update_header_author_for_test(leader.clone());
        }
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

    /// The next event, failing the test after a minute.
    async fn next(pipeline: &mut PersistPipeline) -> EngineResult<PipelineEvent> {
        tokio::time::timeout(Duration::from_secs(60), pipeline.next_event())
            .await
            .expect("a pipeline event within a minute")
    }

    /// Drive the pipeline until it is idle, returning the events.
    async fn drain(pipeline: &mut PersistPipeline) -> EngineResult<Vec<PipelineEvent>> {
        let mut events = Vec::new();
        while !pipeline.is_idle() {
            events.push(next(pipeline).await?);
        }
        Ok(events)
    }

    /// Output 1 held in the commit slot and output 2 built (seeded) behind it.
    async fn hold_one_build_two(
        pipeline: &mut PersistPipeline,
        first: ConsensusOutput,
        second: ConsensusOutput,
    ) -> EngineResult<CommitGate> {
        let gate = pipeline.hold_commits();
        pipeline.start_inline(first)?;
        assert_matches!(
            next(pipeline).await?,
            PipelineEvent::Built(report) if report.consensus_number == 1 && !report.seeded
        );
        assert_matches!(
            next(pipeline).await?,
            PipelineEvent::PersistStarted { consensus_number: 1, .. }
        );
        assert!(pipeline.can_start(), "output 2 may build while output 1 commits");
        pipeline.start_inline(second)?;
        Ok(gate)
    }

    /// Two outputs overlap: output 2 builds (seeded) while output 1 is held in the commit slot,
    /// both are in flight at once, and they commit and announce in order. The committed chain
    /// equals that of a node that executed the same outputs sequentially.
    #[tokio::test]
    async fn test_pipeline_two_outputs_overlap_and_commit_in_order() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let first = output(1, Some(&leader), producer, batches[..2].to_vec(), false);
        let second = output(2, Some(&leader), producer, batches[2..].to_vec(), false);

        let (engine_update_tx, mut engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(&mut pipeline, first.clone(), second.clone()).await?;
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Built(report) if report.consensus_number == 2 && report.blocks == 2 && report.seeded
        );
        assert_eq!(pipeline.unpersisted_outputs(), 2);
        assert_eq!(pipeline.in_memory_tip().number, 4);
        assert_eq!(pipeline.durable_tip().number, 0);
        assert_eq!(node.reth_env.canonical_tip().number, 4, "both outputs are in memory");
        assert_eq!(node.reth_env.last_block_number()?, 0, "nothing is durable yet");
        assert!(engine_updates.try_recv().is_err(), "no update before a commit");

        gate.release_one();
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Committed(report) if report.consensus_number == 1 && report.blocks == 2
        );
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::PersistStarted { consensus_number: 2, .. }
        );
        gate.release_one();
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Committed(report) if report.consensus_number == 2 && report.blocks == 2
        );
        assert!(pipeline.is_idle());
        assert_eq!(pipeline.unpersisted_outputs(), 0);

        let updates: Vec<u64> = std::iter::from_fn(|| engine_updates.try_recv().ok())
            .map(|(_, num, _)| num.number)
            .collect();
        assert_eq!(updates, vec![1, 2], "updates follow the commits in order");

        // oracle: the sequential path on a second node
        let oracle = Node::new(&chain).await?;
        let (oracle_tx, _oracle_updates) = mpsc::channel(64);
        let mut parent = oracle.genesis.clone();
        for consensus in [first, second] {
            let args = BuildArguments::new(oracle.reth_env.clone(), consensus, parent);
            let gas = oracle.gas_accumulator.clone();
            let tx = oracle_tx.clone();
            parent = tokio::task::spawn_blocking(move || {
                execute_consensus_output(args, gas, RepackMonitor::default(), tx)
            })
            .await??;
        }
        assert_eq!(node.committed(4)?, oracle.committed(4)?, "pipelined and sequential differ");
        Ok(())
    }

    /// F2/T6: every persist attempt of output 1 fails while output 2 is built behind it; both
    /// outputs are rolled back to genesis in one call, nothing is durable and nothing was
    /// announced.
    #[tokio::test]
    async fn test_persist_failure_rolls_back_both_outputs() -> eyre::Result<()> {
        persist_failure_rolls_back_both_outputs(false).await
    }

    /// F3/T6: a late fault (after `save_blocks`) makes the retry terminal; the rollback is the
    /// same as for an early fault.
    #[tokio::test]
    async fn test_late_persist_failure_rolls_back_both_outputs() -> eyre::Result<()> {
        persist_failure_rolls_back_both_outputs(true).await
    }

    /// The body of the two persist-failure tests.
    async fn persist_failure_rolls_back_both_outputs(late: bool) -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let (engine_update_tx, mut engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(
            &mut pipeline,
            output(1, Some(&leader), producer, batches[..2].to_vec(), false),
            output(2, Some(&leader), producer, batches[2..].to_vec(), false),
        )
        .await?;
        assert_matches!(next(&mut pipeline).await?, PipelineEvent::Built(_));

        if late {
            node.reth_env.inject_late_persist_provider_faults(1);
        } else {
            node.reth_env.inject_persist_provider_faults(PERSIST_OUTPUT_ATTEMPTS);
        }
        let cims = node.reth_env.canonical_in_memory_state();
        let mut canon = cims.subscribe_canon_state();
        gate.release_all();

        assert_matches!(
            next(&mut pipeline).await,
            Err(TnEngineError::Reth(TnRethError::Provider(_)))
        );
        let attempts = if late { 2 } else { PERSIST_OUTPUT_ATTEMPTS };
        assert_eq!(node.reth_env.persist_attempt_count(), attempts);
        assert_eq!(cims.canonical_chain().count(), 0, "both outputs are rolled back");
        assert_eq!(node.reth_env.canonical_tip().hash(), node.genesis.hash());
        assert_eq!(node.reth_env.last_block_number()?, 0);
        assert!(engine_updates.try_recv().is_err(), "no engine update");
        assert!(canon.try_recv().is_err(), "no canon notification");
        assert_eq!(pipeline.in_memory_tip().hash(), node.genesis.hash());
        assert!(!pipeline.can_start(), "the pipeline is spent");
        assert!(pipeline.is_idle());
        Ok(())
    }

    /// F1: two provider faults on output 1's persist are retried inside its commit task while
    /// output 2 waits parked; both commit and nothing is rolled back.
    #[tokio::test]
    async fn test_persist_fault_retry_recovers_in_pipeline() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let (engine_update_tx, _engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(
            &mut pipeline,
            output(1, Some(&leader), producer, batches[..2].to_vec(), false),
            output(2, Some(&leader), producer, batches[2..].to_vec(), false),
        )
        .await?;
        assert_matches!(next(&mut pipeline).await?, PipelineEvent::Built(_));
        node.reth_env.inject_persist_provider_faults(PERSIST_OUTPUT_ATTEMPTS - 1);
        gate.release_all();

        let committed: Vec<u64> = drain(&mut pipeline)
            .await?
            .into_iter()
            .filter_map(|event| match event {
                PipelineEvent::Committed(report) => Some(report.consensus_number),
                _ => None,
            })
            .collect();
        assert_eq!(committed, vec![1, 2]);
        assert_eq!(node.reth_env.persist_attempt_count(), PERSIST_OUTPUT_ATTEMPTS + 1);
        assert_eq!(node.reth_env.last_block_number()?, 4);
        assert_eq!(node.reth_env.canonical_tip().number, 4);
        Ok(())
    }

    /// F4a/T5: output 2's build fails (an empty closing output whose leader is not in the
    /// committee) while output 1 is held; output 1 still commits and is announced, and then the
    /// build error is returned.
    #[tokio::test]
    async fn test_build_failure_waits_for_commit_then_errors() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(2);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let (engine_update_tx, mut engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(
            &mut pipeline,
            output(1, Some(&leader), producer, batches, false),
            output(2, None, producer, Vec::new(), true),
        )
        .await?;
        gate.release_all();

        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Committed(report) if report.consensus_number == 1
        );
        assert_matches!(next(&mut pipeline).await, Err(TnEngineError::UnknownAuthority(_)));
        assert_eq!(node.reth_env.last_block_number()?, 2, "output 1 is durable");
        assert_eq!(node.reth_env.canonical_tip().number, 2);
        assert_matches!(engine_updates.try_recv(), Ok((_, num, Some(tip))) if num.number == 1 && tip.number == 2);
        assert!(engine_updates.try_recv().is_err(), "nothing announced for output 2");
        Ok(())
    }

    /// F5/T7: the announce of output 1 fails after its commit (the engine update channel is
    /// closed); output 1 stays durable and only output 2's blocks are rolled back.
    #[tokio::test]
    async fn test_post_commit_failure_keeps_durable_output() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let (engine_update_tx, engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(
            &mut pipeline,
            output(1, Some(&leader), producer, batches[..2].to_vec(), false),
            output(2, Some(&leader), producer, batches[2..].to_vec(), false),
        )
        .await?;
        let output2_tip = match next(&mut pipeline).await? {
            PipelineEvent::Built(report) => report.tip,
            other => panic!("expected output 2's build, got {other:?}"),
        };
        drop(engine_updates);
        gate.release_all();

        assert_matches!(next(&mut pipeline).await, Err(TnEngineError::Reth(_)));
        assert_eq!(node.reth_env.last_block_number()?, 2, "output 1 stays durable");
        assert_eq!(node.reth_env.canonical_tip().number, 2);
        let cims = node.reth_env.canonical_in_memory_state();
        assert!(cims.state_by_hash(output2_tip.hash()).is_none(), "output 2 is rolled back");
        assert_eq!(pipeline.durable_tip().number, 2);
        assert_eq!(pipeline.in_memory_tip().number, 2);
        Ok(())
    }

    /// I2: an empty output's engine update waits for the commit of the output before it, so the
    /// updates reach consensus in consensus order and each names a durable block.
    #[tokio::test]
    async fn test_empty_output_update_waits_for_previous_commit() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(2);
        let node = Node::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let (engine_update_tx, mut engine_updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);
        let gate = hold_one_build_two(
            &mut pipeline,
            output(1, Some(&leader), producer, batches, false),
            output(2, Some(&leader), producer, Vec::new(), false),
        )
        .await?;
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Built(report) if report.consensus_number == 2 && report.blocks == 0
        );
        // give a wrongly ordered update every chance to arrive
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(engine_updates.try_recv().is_err(), "the empty output's update ran ahead");

        gate.release_one();
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Committed(report) if report.consensus_number == 1
        );
        let (_, num, tip) = engine_updates.recv().await.expect("output 1's update");
        assert_eq!((num.number, tip.map(|tip| tip.number)), (1, Some(2)));
        assert_eq!(node.reth_env.last_block_number()?, 2, "output 1 is durable when announced");
        assert!(engine_updates.try_recv().is_err(), "output 2's update waits for its slot");

        gate.release_one();
        drain(&mut pipeline).await?;
        let (_, num, tip) = engine_updates.recv().await.expect("output 2's update");
        assert_eq!((num.number, tip), (2, None));
        Ok(())
    }

    /// I3: output 2 built while output 1 is held (seeded) or while output 1 commits beside it
    /// equals output 2 built after output 1 committed (unseeded): every block and the trie
    /// tables.
    #[tokio::test]
    async fn test_seeded_build_matches_unseeded_build() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let unseeded = Node::new(&chain).await?;
        let (leader, producer) = unseeded.leader().await?;
        let first = output(1, Some(&leader), producer, batches[..2].to_vec(), false);
        let second = output(2, Some(&leader), producer, batches[2..].to_vec(), false);

        // unseeded: output 1 commits before output 2 starts
        let (tx, _updates_a) = mpsc::channel(64);
        let mut pipeline = unseeded.pipeline(tx);
        pipeline.start_inline(first.clone())?;
        drain(&mut pipeline).await?;
        pipeline.start_inline(second.clone())?;
        let events = drain(&mut pipeline).await?;
        assert!(events.iter().any(|event| matches!(
            event,
            PipelineEvent::Built(report) if report.consensus_number == 2 && !report.seeded
        )));

        // seeded: output 2 builds while output 1 is held
        let seeded = Node::new(&chain).await?;
        let (tx, _updates_b) = mpsc::channel(64);
        let mut pipeline = seeded.pipeline(tx);
        let gate = hold_one_build_two(&mut pipeline, first.clone(), second.clone()).await?;
        assert_matches!(
            next(&mut pipeline).await?,
            PipelineEvent::Built(report) if report.consensus_number == 2 && report.seeded
        );
        gate.release_all();
        drain(&mut pipeline).await?;

        // overlapping: output 1 commits while output 2 builds on the pinned snapshot
        let overlapping = Node::new(&chain).await?;
        let (tx, _updates_c) = mpsc::channel(64);
        let mut pipeline = overlapping.pipeline(tx);
        let gate = hold_one_build_two(&mut pipeline, first, second).await?;
        gate.release_all();
        drain(&mut pipeline).await?;

        let expected = unseeded.committed(4)?;
        assert_eq!(seeded.committed(4)?, expected, "seeded build differs");
        assert_eq!(overlapping.committed(4)?, expected, "overlapping build differs");
        Ok(())
    }
}
