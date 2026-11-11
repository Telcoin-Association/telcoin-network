// SPDX-License-Identifier: MIT or Apache-2.0
//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.
//!
//! # Trust boundary
//!
//! The [`ConsensusOutput`] this crate executes has already been committed by Bullshark, so the
//! engine deliberately re-validates almost none of it: it checks the batch/digest count, the digest
//! index bound, and (on the empty epoch-closing path only) that the leader resolves to a committee
//! member. Which inputs are trusted, why, and where each enforcing check actually lives is written
//! down per input in this crate's `README.md` ("Trust boundaries"). Read that before assuming a
//! missing check here is a bug, and before adding one.
//!
//! Two facts from it are worth carrying into any change: digest-to-batch *positional* alignment is
//! an emergent property of two parallel walks in `crates/consensus/executor/`, not an assertion in
//! this crate; and `close_epoch` is not part of the consensus digest at all — it is derived
//! node-locally, and a deserialized [`ConsensusOutput`] always reports `false`.

// silence unused lib deps that are used in IT tests
#![allow(unused_crate_dependencies)]

mod error;
mod metrics;
mod payload_builder;
pub mod persister;
mod recovery;
use crate::metrics::{EngineMetrics, ENGINE_METRICS};
use error::EngineResult;
pub use error::TnEngineError;
use futures::{future::OptionFuture, StreamExt};
use payload_builder::execute_consensus_output_with_signers;
pub use payload_builder::{execute_consensus_output, PERSIST_OUTPUT_ATTEMPTS};
use persister::{PersistPipeline, PipelineEvent};
pub use recovery::run_on_recovery_pool;
use recovery::RecoveryHandle;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tn_reth::{payload::BuildArguments, RethEnv};
use tn_types::{
    gas_accumulator::GasAccumulator, repack_monitor::RepackMonitor, ConsensusOutput, EngineUpdate,
    ExecutionBacklog, Noticer, SealedHeader, TaskSpawner,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, trace, warn};

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

/// Maximum number of consensus outputs buffered for execution.
///
/// Each queued output holds a full `CommittedSubDag` plus batches, and once its background signer
/// recovery finishes, the recovered transactions too (about the output's raw transaction bytes
/// again plus 0.3 kB per transaction, see `RecoveryHandle`), so an unbounded queue lets a lagging
/// engine consume all memory while consensus keeps committing. With the output executing, at most
/// `MAX_QUEUED_OUTPUTS + 1` outputs' recovered transactions are held at once. Once the queue is
/// full the engine stops polling the consensus stream, pushing backpressure into the `to_engine`
/// channel (and ultimately the EpochManager forwarder) instead of buffering here. The
/// `tn_engine_queued_outputs` gauge tracks occupancy.
///
/// With `pipelined_persist` on, up to two outputs are past the queue at once (one building, one
/// built and waiting for the commit slot or committing), so at most `MAX_QUEUED_OUTPUTS + 2`
/// outputs' recovered transactions are held, and the executed blocks of two outputs instead of
/// one sit in memory until their commits (`tn_engine_unpersisted_outputs`).
pub const MAX_QUEUED_OUTPUTS: usize = 8;

/// The TN consensus engine is responsible executing state that has reached consensus.
///
/// The engine makes no attempt to track consensus. It's only purpose is to receive output from
/// consensus then try to execute it.
///
/// The engine runs until either the maximum round of consensus is reached OR the sending broadcast
/// channel is dropped. If the sending channel is dropped, the engine attempts to execute any
/// remaining output that is queued up before shutting itself down gracefully. If the maximum round
/// is reached, the engine shuts down immediately.
///
/// With `pipelined_persist` on, the engine persists one output while the next one builds on its
/// in-memory state (see [`PersistPipeline`]): two outputs' blocks may then be executed and in
/// memory but not yet durable, and consensus only ever hears of an output once it is durable.
pub struct ExecutorEngine {
    /// The backlog of output from consensus that's ready to be executed, each stamped with the
    /// instant it was queued so the time it waits for execution is measurable
    /// (`tn_engine_queue_wait_seconds`), and paired with the handle of the signer recovery job
    /// started when it was queued (see [`ExecutorEngine::enqueue`]). The stamp is observability
    /// only and the job only moves the signer recovery earlier: order and capacity are
    /// unaffected.
    queued: VecDeque<(Instant, ConsensusOutput, RecoveryHandle)>,
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<PendingExecutionTask>,
    // Reth execution environment.
    reth_env: RethEnv,
    /// Optional round of consensus to finish executing before then returning. The value is used to
    /// track the subdag index from consensus output. The index is also considered the "round" of
    /// consensus and is included in executed blocks as  the block's `nonce` value.
    ///
    /// NOTE: this is primarily useful for debugging and testing
    max_round: Option<u64>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: ReceiverStream<ConsensusOutput>,
    /// The [SealedHeader] of the last fully-executed block.
    ///
    /// This information reflects the current finalized block number and hash.
    parent_header: SealedHeader,
    /// Used to receive shutdown notification.
    rx_shutdown: Noticer,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// Accumulator for epoch gas usage.
    gas_accumulator: GasAccumulator,
    /// Channel to notify consensus about processed outputs.
    /// Sends (leader_round, consensus_num_hash, `Option<SealedHeader>`) after each
    /// ConsensusOutput is processed.
    engine_update_tx: mpsc::Sender<EngineUpdate>,
    /// Monitor for cross-producer transaction re-packing (issue #1259). Monitoring is opt-in
    /// (the node's `--enable-repack-monitor` flag); the default monitor is disabled and the
    /// engine then builds no window and hashes nothing. An active monitor is owned here and
    /// cloned into each execution task so its rolling window (bounded to the most recent
    /// `REPACK_WINDOW_OUTPUTS` outputs and `REPACK_WINDOW_MAX_TXS` retained hashes, a few
    /// megabytes worst case) persists across the outputs this engine executes. A fresh engine
    /// (an epoch boundary) starts a fresh window, which at worst misses a poach racing across
    /// the boundary.
    repack_monitor: RepackMonitor,
    /// Threads in the dedicated signer recovery pool (`recovery_threads` in `parameters.yaml`);
    /// zero keeps signer recovery on the global rayon pool.
    recovery_threads: u16,
    /// The node's shared count of outputs execution trails behind: the engine records how many
    /// outputs it holds and the last one it finished. Observability for the RPC submission
    /// guard only; nothing here reads it.
    execution_backlog: ExecutionBacklog,
    /// Persist one output while the next one executes (`pipelined_persist` in
    /// `parameters.yaml`); `false` builds, persists and commits each output before the next.
    pipelined_persist: bool,
}

impl ExecutorEngine {
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// The engine waits for CL to broadcast output then tries to execute.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reth_env: RethEnv,
        max_round: Option<u64>,
        rx_consensus_output: mpsc::Receiver<ConsensusOutput>,
        parent_header: SealedHeader,
        rx_shutdown: Noticer,
        task_spawner: TaskSpawner,
        gas_accumulator: GasAccumulator,
        repack_monitor: RepackMonitor,
        engine_update_tx: mpsc::Sender<EngineUpdate>,
    ) -> Self {
        let consensus_output_stream = ReceiverStream::new(rx_consensus_output);
        // the node's env carries the node's backlog, so the engine records into it by default
        let execution_backlog = reth_env.execution_backlog().clone();

        Self {
            queued: Default::default(),
            pending_task: None,
            reth_env,
            max_round,
            consensus_output_stream,
            parent_header,
            rx_shutdown,
            task_spawner,
            gas_accumulator,
            engine_update_tx,
            repack_monitor,
            recovery_threads: 0,
            execution_backlog,
            pipelined_persist: false,
        }
    }

    /// Set the number of threads in the dedicated signer recovery pool (`recovery_threads` in
    /// `parameters.yaml`).
    ///
    /// Zero, the value [`ExecutorEngine::new`] starts with, keeps signer recovery on the global
    /// rayon pool. The node applies the configured value right after construction.
    pub fn with_recovery_threads(mut self, threads: u16) -> Self {
        self.recovery_threads = threads;
        self
    }

    /// The number of threads in the dedicated signer recovery pool; zero means the global rayon
    /// pool.
    pub fn recovery_threads(&self) -> u16 {
        self.recovery_threads
    }

    /// Share the node's [`ExecutionBacklog`] with the engine, which records into it how many
    /// outputs it holds and every output it finishes.
    ///
    /// [`ExecutorEngine::new`] starts with the handle its reth environment holds, which on the
    /// node's env is the node's own; this overrides it. The node passes that same handle right
    /// after construction.
    pub fn with_execution_backlog(mut self, backlog: ExecutionBacklog) -> Self {
        self.execution_backlog = backlog;
        self
    }

    /// The [`ExecutionBacklog`] the engine records into.
    pub fn execution_backlog(&self) -> &ExecutionBacklog {
        &self.execution_backlog
    }

    /// Set whether the engine persists one output while the next one executes
    /// (`pipelined_persist` in `parameters.yaml`).
    ///
    /// `false`, the value [`ExecutorEngine::new`] starts with, builds, persists and commits each
    /// output before the next one starts. The node applies the configured value right after
    /// construction.
    pub fn with_pipelined_persist(mut self, pipelined: bool) -> Self {
        self.pipelined_persist = pipelined;
        self
    }

    /// Whether the engine persists one output while the next one executes.
    pub fn pipelined_persist(&self) -> bool {
        self.pipelined_persist
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask {
        let (tx, rx) = oneshot::channel();

        // pop next output in queue and execute
        if let Some(Dequeued { output, signers, queue_wait, forward_wait }) =
            take_next_queued(&mut self.queued, Instant::now())
        {
            ENGINE_METRICS.queue_wait_seconds.record(queue_wait);
            let consensus_number = output.number();
            let reth_env = self.reth_env.clone();
            let parent = self.parent_header.clone();
            let task_name = format!("execution-output-{}", output.consensus_header_hash());
            let build_args = BuildArguments::new(reth_env, output, parent);

            let gas_accumulator = self.gas_accumulator.clone();
            let repack_monitor = self.repack_monitor.clone();
            let engine_update_tx = self.engine_update_tx.clone();
            let execution_backlog = self.execution_backlog.clone();
            // spawn blocking task and return future
            self.task_spawner.spawn_blocking_task(task_name, move || {
                let execution_start = Instant::now();
                // test hook (off without tn-types/test-utils): slows outputs with batches
                let has_batches = !build_args.output.batch_digests().is_empty();
                if let Some(d) = tn_types::test_hooks::execution_delay().filter(|_| has_batches) {
                    std::thread::sleep(d);
                }
                // this is safe to call on blocking thread without a semaphore bc it's held in
                // Self::pending_tesk as a single `Option`
                let result = execute_consensus_output_with_signers(
                    build_args,
                    Some(signers),
                    gas_accumulator,
                    repack_monitor,
                    engine_update_tx,
                )
                .inspect_err(|e| {
                    error!(target: "engine", ?e, "error executing consensus output");
                });
                // every finished output counts, an empty skipped one included
                if result.is_ok() {
                    execution_backlog.record_executed(consensus_number);
                }
                let task_elapsed = execution_start.elapsed();
                ENGINE_METRICS.execution_duration_seconds.record(task_elapsed);
                ENGINE_METRICS.busy_milliseconds_total.increment(duration_ms(task_elapsed));
                debug!(
                    target: "engine",
                    consensus_number,
                    queue_wait_ms = duration_ms(queue_wait),
                    task_ms = duration_ms(task_elapsed),
                    // zero for an output nothing stamped (only tests bypass the forwarder)
                    forward_wait_ms = duration_ms(forward_wait.unwrap_or_default()),
                    "output execution task finished"
                );
                if let Err(e) = tx.send(result) {
                    warn!(target: "engine", ?e, "error sending result from execute_consensus_output")
                }
                Ok(())
            });
        } else {
            let _ = tx.send(Err(TnEngineError::EmptyQueue));
        }

        // oneshot receiver for execution result
        rx
    }

    /// Check if the engine has reached the maximum round of consensus as specified by `max_round`
    /// parameter.
    ///
    /// Note: this is mainly for testing and debugging purposes.
    #[cfg(any(test, feature = "test-utils"))]
    fn has_reached_max_round(&self, progress: u64) -> bool {
        let (_, round) = tn_types::deconstruct_nonce(progress);
        let has_reached_max_round =
            self.max_round.map(|target| round as u64 >= target).unwrap_or_default();
        if has_reached_max_round {
            trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_round,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_round
    }

    /// Push `output` to the back of the queue, stamped `queued_at`, and start recovering its
    /// signers on a blocking job so the work overlaps the execution of the outputs ahead of it.
    ///
    /// The job recovers on the process-wide dedicated pool of `recovery_threads` threads, away
    /// from the global rayon pool the block builds use, or on that global pool when
    /// `recovery_threads` is zero (see [`run_on_recovery_pool`]).
    ///
    /// An output with no batches has no signers to recover and gets an already-resolved handle
    /// instead of a job, so the empty outputs committed at idle record no
    /// `tn_engine_recover_seconds` sample and log no `signers recovered` line.
    fn enqueue(&mut self, queued_at: Instant, output: ConsensusOutput) {
        let signers = if output.batches().iter().all(|cert| cert.batches.is_empty()) {
            RecoveryHandle::resolved(Vec::new())
        } else {
            RecoveryHandle::spawn(
                &output,
                &self.task_spawner,
                &ENGINE_METRICS,
                self.recovery_threads,
            )
        };
        self.queued.push_back((queued_at, output, signers));
    }

    /// Record in the shared [`ExecutionBacklog`] how many outputs the engine holds: the queued
    /// ones plus the one executing.
    fn record_held_outputs(&self) {
        let executing = usize::from(self.pending_task.is_some());
        self.execution_backlog.record_queued(self.queued.len() + executing);
    }

    /// TESTING ONLY
    ///
    /// Push a consensus output to the back of `Self::queued`, starting its signer recovery job
    /// as the run loop does (so this must be called within a tokio runtime).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn push_back_queued_for_test(&mut self, output: ConsensusOutput) {
        self.enqueue(Instant::now(), output)
    }
}

/// The [ExecutorEngine] is a future that loops through the following:
/// - receive messages from consensus
/// - add these messages to a queue
/// - pull from queue to start next execution task if idle
/// - poll any pending tasks that are currently being executed
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl ExecutorEngine {
    /// Run the engine event loop until shutdown or the consensus stream closes.
    ///
    /// The loop is chosen once, by `pipelined_persist`: off, the strictly sequential loop
    /// (`run_sequential`), where each output builds, persists and is announced before the next
    /// starts; on, the pipelined loop (`run_pipelined`), which persists one output while the next
    /// one builds. Both return only once nothing is queued or in flight (or right after the
    /// `max_round` output, test-utils).
    pub async fn run(self) -> EngineResult<()> {
        if self.pipelined_persist {
            self.run_pipelined(&ENGINE_METRICS).await
        } else {
            self.run_sequential().await
        }
    }

    /// The engine loop with `pipelined_persist` off: one output at a time builds, persists and
    /// is announced on one blocking task before the next one starts.
    async fn run_sequential(mut self) -> EngineResult<()> {
        // tracks the consensus stream returning `None`; the engine keeps draining queued and
        // in-flight work before finally shutting down.
        let mut consensus_closed = false;

        loop {
            // check for shutdown signal
            if self.rx_shutdown.noticed() {
                info!(target: "engine", "received shutdown signal...");
                // only return if there are no current tasks and the queue is empty
                // otherwise, let the loop continue so any remaining tasks and queued output is
                // executed
                // rx_shutdown should continue to poll ready so once the queue is clear should
                // shutdown.
                if self.pending_task.is_none() && self.queued.is_empty() {
                    return Ok(());
                }
            }

            // the consensus stream closed: once the queue and in-flight task drain, shut down
            // (the original re-checked this on every poll after observing the closed stream)
            if consensus_closed && self.pending_task.is_none() && self.queued.is_empty() {
                return Err(TnEngineError::ConsensusOutputStreamClosed);
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if self.pending_task.is_none() && !self.queued.is_empty() {
                // ready to begin executing next round of consensus
                self.pending_task = Some(self.spawn_execution_task());
            }

            tokio::select! {
                // poll consensus first to keep the broadcast stream from "lagging"
                biased;

                // wake on shutdown while otherwise idle; disabled once noticed (from then on the
                // top-of-loop check owns the drain-and-return path).
                _ = &self.rx_shutdown, if !self.rx_shutdown.noticed() => {}

                // check if output is available from consensus to keep broadcast stream from "lagging"
                //
                // gated on the queue bound: while the backlog is full there is always a
                // pending execution task to wake the loop, and each completion re-opens the
                // gate, so consensus backpressures instead of growing the queue unbounded
                output = self.consensus_output_stream.next(), if !consensus_closed && self.queued.len() < MAX_QUEUED_OUTPUTS => {
                    output
                        .map(|output| {
                            let queued_at = Instant::now();
                            // the hop from the forwarder to here, and the backlog it left in the
                            // channel
                            record_arrival(
                                &ENGINE_METRICS,
                                &output,
                                queued_at,
                                self.consensus_output_stream.as_ref().len(),
                            );
                            // queue the output for local execution, stamped for the queue-wait
                            // histogram, and start recovering its signers
                            let consensus_number = output.number();
                            self.enqueue(queued_at, output);
                            debug!(
                                target: "engine",
                                consensus_number,
                                depth = self.queued.len(),
                                "output queued"
                            );
                            // backlog growth means execution is falling behind consensus
                            ENGINE_METRICS.queued_outputs.set(self.queued.len() as f64);
                            self.record_held_outputs();
                        })
                        .unwrap_or_else(|| {
                            // the stream has ended; the top-of-loop check returns the error once
                            // the queue and in-flight task have drained.
                            error!(target: "engine", "ConsensusOutput channel closed. Shutting down...");
                            consensus_closed = true;
                        });
                }

                // poll receiver that returns output execution result
                Some(res) = OptionFuture::from(self.pending_task.as_mut()), if self.pending_task.is_some() => {
                    trace!(target: "engine::dump", ?res, "receiver for execution result polled ready");
                    let finalized_header = res.map_err(Into::into).and_then(|res| res)?;
                    // the in-flight task resolved; clear the slot so the next round can be spawned
                    self.pending_task = None;
                    // store last executed header in memory
                    self.parent_header = finalized_header;
                    ENGINE_METRICS.outputs_executed_total.increment(1);
                    ENGINE_METRICS.canonical_height.set(self.parent_header.number as f64);
                    ENGINE_METRICS.queued_outputs.set(self.queued.len() as f64);
                    self.record_held_outputs();
                    // the stream is not polled while the queue is full, so the channel depth
                    // also refreshes here, once per executed output
                    ENGINE_METRICS
                        .channel_depth
                        .set(self.consensus_output_stream.as_ref().len() as f64);

                    // check max_round to auto shutdown
                    #[cfg(any(test, feature = "test-utils"))]
                    if self.max_round.is_some()
                        && self.has_reached_max_round(self.parent_header.nonce.into())
                    {
                        // immediately terminate if the specified max consensus round is reached
                        return Ok(());
                    }

                    // allow loop to continue: poll broadcast stream for next output
                }
            }
        }
    }
}

impl ExecutorEngine {
    /// The engine loop with `pipelined_persist` on: a [`PersistPipeline`] builds one output while
    /// the previous one commits.
    ///
    /// Each turn: return once nothing is in flight and either nothing is queued (after shutdown,
    /// or once the consensus stream closed, as the sequential loop does) or the `max_round`
    /// output committed (test-utils); start the next queued output while the pipeline can take
    /// one; count the outputs a closing output holds back; then wait for shutdown, the next
    /// output from consensus (behind the same queue bound as the sequential loop) or the
    /// pipeline's next transition, polled only while something is in flight.
    ///
    /// No exit leaves a build or commit running, since a blocking task cannot be cancelled:
    /// shutdown and a closed stream drain the queue and the pipeline first, a pipeline error is
    /// returned only once both slots settled and every unpersisted block was rolled back, and a
    /// start the pipeline refuses waits for the outputs already in it. A loop with nothing to
    /// wait on (no arm enabled) fails closed with [`TnEngineError::PipelineInvariant`] instead
    /// of spinning or panicking; the pipeline's own rules leave no such state.
    ///
    /// A closing output is a barrier: the pipeline starts nothing on it before it committed. The
    /// barrier ends at the engine's `Committed`, which comes after the closing output's announce,
    /// so it does not by itself keep the epoch close's readers (`adjust_base_fees`, the gas
    /// accumulator's clear) on the closing block: the node forwarding nothing past a close until
    /// the next epoch starts does, as it does for the sequential loop.
    ///
    /// `metrics` is the process-wide [`ENGINE_METRICS`] outside tests.
    async fn run_pipelined(mut self, metrics: &EngineMetrics) -> EngineResult<()> {
        let mut pipeline = PersistPipeline::new(
            self.reth_env.clone(),
            self.parent_header.clone(),
            self.task_spawner.clone(),
            self.gas_accumulator.clone(),
            self.repack_monitor.clone(),
            self.engine_update_tx.clone(),
        );
        let mut books = PipelinedBooks::default();
        // the consensus stream returned `None`; the queue and the pipeline still drain
        let mut consensus_closed = false;
        let mut shutdown_logged = false;

        loop {
            if self.rx_shutdown.noticed() && !shutdown_logged {
                info!(target: "engine", "received shutdown signal...");
                shutdown_logged = true;
            }
            // every exit waits for the pipeline to be idle: its build and commit run on blocking
            // threads that cannot be cancelled
            if pipeline.is_idle() {
                // test-utils only: stop right after the max-round output committed
                if books.max_round_committed {
                    return Ok(());
                }
                if self.queued.is_empty() {
                    if self.rx_shutdown.noticed() {
                        return Ok(());
                    }
                    if consensus_closed {
                        return Err(TnEngineError::ConsensusOutputStreamClosed);
                    }
                }
            }

            if let Err(refused) = self.start_queued(&mut pipeline, &mut books, metrics) {
                return self.drain_refused(&mut pipeline, &mut books, metrics, refused).await;
            }
            books.count_barrier_holds(pipeline.barrier(), self.queued.len(), metrics);

            tokio::select! {
                // poll consensus first to keep the stream from lagging, as the sequential loop
                biased;

                // wake on shutdown; from then on the checks above own the drain-and-return path
                _ = &self.rx_shutdown, if !self.rx_shutdown.noticed() => {}

                // gated on the queue bound exactly as the sequential loop: while the queue is
                // full the pipeline is busy and its next transition wakes the loop
                output = self.consensus_output_stream.next(), if !consensus_closed && self.queued.len() < MAX_QUEUED_OUTPUTS => {
                    match output {
                        Some(output) => self.receive_output(output, &pipeline, metrics),
                        None => {
                            error!(target: "engine", "ConsensusOutput channel closed. Shutting down...");
                            consensus_closed = true;
                        }
                    }
                }

                // never polled while idle: `next_event` is then pending forever
                event = pipeline.next_event(), if !pipeline.is_idle() => {
                    self.on_pipeline_event(event?, &pipeline, &mut books, metrics);
                }

                // unreachable: an idle pipeline can start the queued output, and the checks above
                // return once nothing is queued after shutdown or a closed stream
                else => {
                    return Err(TnEngineError::PipelineInvariant(
                        "the pipelined loop had nothing to wait on",
                    ));
                }
            }
        }
    }

    /// Start the next queued output if the pipeline can take one and no built output has
    /// reached `max_round` (test-utils), recording its queue wait and keeping its waits for the
    /// `output execution task finished` line its commit logs.
    ///
    /// # Errors
    ///
    /// The pipeline's refusal ([`TnEngineError::PipelineInvariant`], an output out of consensus
    /// order): that output is not started, and the outputs already in the pipeline are still in
    /// flight.
    fn start_queued(
        &mut self,
        pipeline: &mut PersistPipeline,
        books: &mut PipelinedBooks,
        metrics: &EngineMetrics,
    ) -> EngineResult<()> {
        while pipeline.can_start() && !books.max_round_built {
            let Some(Dequeued { output, signers, queue_wait, forward_wait }) =
                take_next_queued(&mut self.queued, Instant::now())
            else {
                break;
            };
            metrics.queue_wait_seconds.record(queue_wait);
            let consensus_number = output.number();
            pipeline.start(output, Some(signers))?;
            books.started.push_back(StartedOutput { consensus_number, queue_wait, forward_wait });
            books.busy_since.get_or_insert_with(Instant::now);
            self.record_pipeline_occupancy(pipeline, metrics);
        }
        Ok(())
    }

    /// The pipeline refused a start: let every output already in it commit (or fail), recording
    /// its transitions as usual, then return `refused`. Returning at once would leave a commit
    /// running on its blocking thread with nobody to observe it.
    async fn drain_refused(
        &mut self,
        pipeline: &mut PersistPipeline,
        books: &mut PipelinedBooks,
        metrics: &EngineMetrics,
        refused: TnEngineError,
    ) -> EngineResult<()> {
        error!(
            target: "engine",
            ?refused,
            in_flight = pipeline.unpersisted_outputs(),
            "pipelined persist refused an output - draining the outputs in flight"
        );
        while !pipeline.is_idle() {
            match pipeline.next_event().await {
                Ok(event) => self.on_pipeline_event(event, pipeline, books, metrics),
                Err(later) => {
                    warn!(
                        target: "engine",
                        ?later,
                        "pipelined persist: a later error while draining after a refused output"
                    );
                    break;
                }
            }
        }
        Err(refused)
    }

    /// Queue one output from consensus, as the sequential loop's stream arm does.
    fn receive_output(
        &mut self,
        output: ConsensusOutput,
        pipeline: &PersistPipeline,
        metrics: &EngineMetrics,
    ) {
        let queued_at = Instant::now();
        record_arrival(metrics, &output, queued_at, self.consensus_output_stream.as_ref().len());
        let consensus_number = output.number();
        self.enqueue(queued_at, output);
        debug!(target: "engine", consensus_number, depth = self.queued.len(), "output queued");
        self.record_pipeline_occupancy(pipeline, metrics);
    }

    /// Record one pipeline transition.
    ///
    /// `Committed` is the pipelined loop's "output executed": the output is durable and
    /// announced, so only then does it advance the durable height, the executed count, the
    /// shared backlog's last executed output and the engine's `parent_header`, and log the
    /// output's `output execution task finished` line (`task_ms` from build start to commit).
    fn on_pipeline_event(
        &mut self,
        event: PipelineEvent,
        pipeline: &PersistPipeline,
        books: &mut PipelinedBooks,
        metrics: &EngineMetrics,
    ) {
        match event {
            PipelineEvent::Built(report) => {
                metrics.built_height.set(report.tip.number as f64);
                if self.reaches_max_round(&report.tip) {
                    books.max_round_built = true;
                }
            }
            PipelineEvent::PersistStarted { waited, .. } => {
                metrics.persist_wait_seconds.record(waited);
            }
            PipelineEvent::Committed(report) => {
                let consensus_number = report.consensus_number;
                let latency = report.committed_at.saturating_duration_since(report.build_started);
                metrics.outputs_executed_total.increment(1);
                metrics.canonical_height.set(report.tip.number as f64);
                metrics.execution_duration_seconds.record(latency);
                // the cycle, only while saturated: this output started before the last commit
                if let Some(previous) = books.last_commit.replace(report.committed_at) {
                    if report.build_started < previous {
                        metrics
                            .commit_interval_seconds
                            .record(report.committed_at.saturating_duration_since(previous));
                    }
                }
                // busy time up to this commit, which goes on while another output is in flight
                if let Some(since) = books.busy_since.take() {
                    metrics.busy_milliseconds_total.increment(duration_ms(
                        report.committed_at.saturating_duration_since(since),
                    ));
                }
                if pipeline.unpersisted_outputs() > 0 {
                    books.busy_since = Some(report.committed_at);
                }
                // executed means durable and announced, never only built: the backlog's admission
                // bound and the epoch-close gauge read it
                self.execution_backlog.record_executed(consensus_number);
                metrics.channel_depth.set(self.consensus_output_stream.as_ref().len() as f64);
                let waits = books.take_started(consensus_number);
                debug!(
                    target: "engine",
                    consensus_number,
                    queue_wait_ms = duration_ms(waits.map(|w| w.queue_wait).unwrap_or_default()),
                    task_ms = duration_ms(latency),
                    // zero for an output nothing stamped (only tests bypass the forwarder)
                    forward_wait_ms =
                        duration_ms(waits.and_then(|w| w.forward_wait).unwrap_or_default()),
                    "output execution task finished"
                );
                if self.reaches_max_round(&report.tip) {
                    books.max_round_committed = true;
                }
                self.parent_header = report.tip;
            }
        }
        self.record_pipeline_occupancy(pipeline, metrics);
    }

    /// Refresh the occupancy gauges and the shared backlog's count of the outputs the engine
    /// holds: those queued plus those started and not yet committed (an output built but not
    /// durable is still held).
    fn record_pipeline_occupancy(&self, pipeline: &PersistPipeline, metrics: &EngineMetrics) {
        let in_flight = pipeline.unpersisted_outputs();
        metrics.unpersisted_outputs.set(in_flight as f64);
        metrics.queued_outputs.set(self.queued.len() as f64);
        self.execution_backlog.record_queued(self.queued.len() + in_flight);
    }

    /// Whether `header` reaches the debug `max_round`.
    #[cfg(any(test, feature = "test-utils"))]
    fn reaches_max_round(&self, header: &SealedHeader) -> bool {
        self.max_round.is_some() && self.has_reached_max_round(header.nonce.into())
    }

    /// Whether `header` reaches the debug `max_round`: never outside test-utils builds.
    #[cfg(not(any(test, feature = "test-utils")))]
    fn reaches_max_round(&self, _header: &SealedHeader) -> bool {
        false
    }
}

impl std::fmt::Debug for ExecutorEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorEngine")
            .field("queued", &self.queued.len())
            .field("pending_task", &self.pending_task.is_some())
            .field("max_round", &self.max_round)
            .field("parent_header", &self.parent_header)
            .finish_non_exhaustive()
    }
}

/// A consensus output popped off the engine queue with the waits measured for it.
#[derive(Debug)]
struct Dequeued {
    /// The output to execute.
    output: ConsensusOutput,
    /// The output's signer recovery job, started when it was queued.
    signers: RecoveryHandle,
    /// Time in the engine queue, from arrival off the consensus stream to dispatch.
    queue_wait: Duration,
    /// Time from the forwarder's stamp to arrival off the consensus stream (see
    /// [`forward_wait`]); `None` for an unstamped output.
    forward_wait: Option<Duration>,
}

/// What the pipelined loop keeps between turns besides the pipeline: the bookkeeping behind its
/// metrics and log lines, and the test-utils `max_round` stop.
#[derive(Debug, Default)]
struct PipelinedBooks {
    /// The waits of every output started and not yet committed, oldest first.
    started: VecDeque<StartedOutput>,
    /// The start of the current busy stretch (any slot occupied), `None` while idle.
    busy_since: Option<Instant>,
    /// When the previous commit finished.
    last_commit: Option<Instant>,
    /// The closing output holding the queue back and how many queued outputs were counted
    /// against it.
    barrier_holds: Option<(u64, usize)>,
    /// A built output reached `max_round`: start nothing more (test-utils).
    max_round_built: bool,
    /// A committed output reached `max_round`: return once nothing is in flight (test-utils).
    max_round_committed: bool,
}

impl PipelinedBooks {
    /// Count the queued outputs the closing output `barrier` holds back, each once per close.
    ///
    /// Nothing starts while the barrier stands, so the queue only grows and the outputs above the
    /// count so far are the new ones.
    fn count_barrier_holds(
        &mut self,
        barrier: Option<u64>,
        queued: usize,
        metrics: &EngineMetrics,
    ) {
        let Some(closing) = barrier else {
            self.barrier_holds = None;
            return;
        };
        let counted = match self.barrier_holds {
            Some((held_by, counted)) if held_by == closing => counted,
            _ => 0,
        };
        if queued > counted {
            metrics
                .close_barrier_holds_total
                .increment(u64::try_from(queued - counted).unwrap_or(u64::MAX));
        }
        self.barrier_holds = Some((closing, queued.max(counted)));
    }

    /// Remove and return the waits of output `consensus_number`, dropping any older entry (an
    /// output always commits after the ones started before it).
    fn take_started(&mut self, consensus_number: u64) -> Option<StartedOutput> {
        while let Some(front) = self.started.pop_front() {
            if front.consensus_number == consensus_number {
                return Some(front);
            }
            if front.consensus_number > consensus_number {
                self.started.push_front(front);
                return None;
            }
        }
        None
    }
}

/// The waits of one output before its build started, logged when it commits.
#[derive(Debug, Clone, Copy)]
struct StartedOutput {
    /// The output's consensus number.
    consensus_number: u64,
    /// Time in the engine queue.
    queue_wait: Duration,
    /// Time from the forwarder's stamp to arrival; `None` for an unstamped output.
    forward_wait: Option<Duration>,
}

/// Pop the oldest queued consensus output and return it with the time it spent in the queue
/// and, for a stamped output, the forward wait before it.
///
/// FIFO exactly like the plain `pop_front` it wraps; `now` is the dispatch instant the wait is
/// measured to. A stamp later than `now` (impossible with a monotonic clock) reads as zero.
fn take_next_queued(
    queued: &mut VecDeque<(Instant, ConsensusOutput, RecoveryHandle)>,
    now: Instant,
) -> Option<Dequeued> {
    queued.pop_front().map(|(queued_at, output, signers)| Dequeued {
        forward_wait: forward_wait(&output, queued_at),
        queue_wait: now.saturating_duration_since(queued_at),
        output,
        signers,
    })
}

/// Record one output's arrival off the consensus stream: its forward wait (stamped outputs
/// only) and the depth of the `to_engine` channel left behind it.
fn record_arrival(
    metrics: &EngineMetrics,
    output: &ConsensusOutput,
    arrived: Instant,
    channel_depth: usize,
) {
    if let Some(wait) = forward_wait(output, arrived) {
        metrics.forward_wait_seconds.record(wait);
    }
    metrics.channel_depth.set(channel_depth as f64);
}

/// Time from the forwarder's stamp on `output` to its arrival off the consensus stream.
///
/// The node stamps every output just before sending it to the engine
/// (`ConsensusOutput::set_forwarded_at`), so this covers a send blocked on a full channel plus
/// the time spent in the channel. `None` for an unstamped output (tests push outputs past the
/// forwarder); a stamp later than `arrived` reads as zero.
fn forward_wait(output: &ConsensusOutput, arrived: Instant) -> Option<Duration> {
    output.forwarded_at().map(|at| arrived.saturating_duration_since(at))
}

/// Whole milliseconds in `elapsed`, saturating at `u64::MAX`, for log fields and the busy-time
/// counter.
pub(crate) fn duration_ms(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Build a distinguishable output: only the consensus number matters to the queue.
    fn output(number: u64) -> ConsensusOutput {
        ConsensusOutput::new_with_subdag(Default::default(), Default::default(), number)
    }

    /// The stamped queue pops in arrival order and reports each output's wait up to the
    /// dispatch instant, plus the forward wait before arrival for a forwarder-stamped output.
    #[test]
    fn test_take_next_queued_is_fifo_and_measures_queue_wait() {
        let base = Instant::now();
        let at = |ms: u64| base + Duration::from_millis(ms);
        let mut forwarded = output(2);
        forwarded.set_forwarded_at(at(4));
        let mut queued = VecDeque::new();
        queued.push_back((at(0), output(1), RecoveryHandle::resolved(Vec::new())));
        queued.push_back((at(10), forwarded, RecoveryHandle::resolved(Vec::new())));
        queued.push_back((at(30), output(3), RecoveryHandle::resolved(Vec::new())));

        let now = at(50);
        let popped: Vec<(u64, Duration, Option<Duration>)> =
            std::iter::from_fn(|| take_next_queued(&mut queued, now))
                .map(|dequeued| {
                    (dequeued.output.number(), dequeued.queue_wait, dequeued.forward_wait)
                })
                .collect();

        assert_eq!(
            popped,
            vec![
                (1, Duration::from_millis(50), None),
                (2, Duration::from_millis(40), Some(Duration::from_millis(6))),
                (3, Duration::from_millis(20), None),
            ]
        );
        assert!(queued.is_empty());
        assert!(take_next_queued(&mut queued, now).is_none());
    }

    /// A stamp after the dispatch instant saturates to a zero wait instead of panicking.
    #[test]
    fn test_take_next_queued_saturates_future_stamp() {
        let base = Instant::now();
        let mut queued = VecDeque::new();
        queued.push_back((
            base + Duration::from_millis(5),
            output(7),
            RecoveryHandle::resolved(Vec::new()),
        ));

        let dequeued = take_next_queued(&mut queued, base).expect("one queued output");
        assert_eq!(dequeued.output.number(), 7);
        assert_eq!(dequeued.queue_wait, Duration::ZERO);
    }

    /// A saturated `to_engine` channel: the output taken off it reports the time it sat there
    /// since the forwarder's stamp, and the depth gauge reads the backlog left behind it.
    #[test]
    fn test_saturated_channel_reports_depth_and_forward_wait() {
        const CAPACITY: usize = 4;
        let (to_engine, rx) = mpsc::channel(CAPACITY);
        let forwarded_at = Instant::now();
        for number in 1..=CAPACITY as u64 {
            let mut stamped = output(number);
            stamped.set_forwarded_at(forwarded_at);
            to_engine.try_send(stamped).expect("channel has room");
        }
        assert!(to_engine.try_send(output(99)).is_err(), "test setup: channel must be full");
        let mut stream = ReceiverStream::new(rx);

        // the outputs sit in the full channel before the engine takes the first one
        std::thread::sleep(Duration::from_millis(5));
        let received = stream.as_mut().try_recv().expect("a queued output");
        let arrived = Instant::now();
        assert_eq!(received.number(), 1);

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        ::metrics::with_local_recorder(&recorder, || {
            // constructed here (not the static) so the handles bind to the local recorder
            let metrics = EngineMetrics::new_with_labels(Vec::<::metrics::Label>::new());
            record_arrival(&metrics, &received, arrived, stream.as_ref().len());
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let recorded = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(.., value)| value)
                .unwrap_or_else(|| panic!("metric {name} not recorded"))
        };
        assert!(matches!(
            recorded("tn_engine.channel_depth"),
            DebugValue::Gauge(depth) if depth.0 == (CAPACITY - 1) as f64
        ));
        assert!(matches!(
            recorded("tn_engine.forward_wait_seconds"),
            DebugValue::Histogram(waits) if waits.len() == 1 && waits[0].0 >= 0.005
        ));

        // an output nothing stamped has no forward wait to record
        assert_eq!(forward_wait(&output(7), arrived), None);
    }

    /// Millisecond conversion truncates sub-millisecond remainders.
    #[test]
    fn test_duration_ms_truncates() {
        assert_eq!(duration_ms(Duration::from_micros(1_999)), 1);
        assert_eq!(duration_ms(Duration::ZERO), 0);
    }

    // ---- bench6 inflight-bound ----

    /// An engine fed by the returned sender, on a fresh temp chain at `path`. The returned
    /// receiver takes the engine's per-output updates and the notifier holds off shutdown.
    fn engine_on_temp_chain(
        path: &std::path::Path,
        task_manager: &tn_types::TaskManager,
    ) -> (
        ExecutorEngine,
        mpsc::Sender<ConsensusOutput>,
        mpsc::Receiver<EngineUpdate>,
        tn_types::Notifier,
    ) {
        let chain: std::sync::Arc<tn_reth::RethChainSpec> =
            std::sync::Arc::new(tn_types::test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain, path, task_manager, None).expect("temp chain env");
        let parent_header = reth_env.lookup_head().expect("genesis head");
        let (to_engine, rx_output) = mpsc::channel(MAX_QUEUED_OUTPUTS);
        let (engine_update_tx, engine_updates) = mpsc::channel(MAX_QUEUED_OUTPUTS);
        let shutdown = tn_types::Notifier::new();
        let engine = ExecutorEngine::new(
            reth_env,
            None,
            rx_output,
            parent_header,
            shutdown.subscribe(),
            task_manager.get_spawner(),
            GasAccumulator::default(),
            RepackMonitor::default(),
            engine_update_tx,
        );
        (engine, to_engine, engine_updates, shutdown)
    }

    /// The engine records into the backlog it was handed: every finished output, empty skipped
    /// ones included, and the held count, which reads zero once the engine has drained.
    #[tokio::test]
    async fn test_engine_records_finished_and_held_outputs_into_the_backlog() {
        let task_manager = tn_types::TaskManager::new("engine backlog");
        let dir = tempfile::TempDir::new().expect("temp dir");
        let (engine, to_engine, mut engine_updates, _shutdown) =
            engine_on_temp_chain(dir.path(), &task_manager);
        // by default the engine records into the handle its env holds
        engine.reth_env.execution_backlog().record_executed(1);
        assert_eq!(engine.execution_backlog().last_executed(), Some(1), "the env's handle");

        let backlog = ExecutionBacklog::new();
        let engine = engine.with_execution_backlog(backlog.clone());
        for number in 1..=3 {
            to_engine.send(output(number)).await.expect("engine receives");
            backlog.record_forwarded(number);
        }
        drop(to_engine);

        let result = tokio::time::timeout(Duration::from_secs(30), engine.run())
            .await
            .expect("engine drains within the timeout");
        assert!(matches!(result, Err(TnEngineError::ConsensusOutputStreamClosed)), "{result:?}");
        // the three empty outputs were skipped, each reported once
        for _ in 1..=3 {
            let (_, _, tip) = engine_updates.try_recv().expect("one update per output");
            assert!(tip.is_none(), "an empty output builds no block");
        }
        assert_eq!(backlog.last_executed(), Some(3));
        assert_eq!(backlog.queued(), 0);
        assert_eq!(backlog.outputs_behind(), 0);
    }

    // ---- bench6 pipelined-persist ----

    use assert_matches::assert_matches;
    use metrics_util::debugging::Snapshotter;
    use std::sync::Arc;
    use tn_reth::{
        test_utils::{
            batches, create_committee_from_state, seeded_genesis_from_random_batches,
            test_genesis_with_consensus_registry,
        },
        RethChainSpec,
    };
    use tn_types::{
        now, test_chain_spec_arc, Address, AuthorityIdentifier, Batch, Certificate, CertifiedBatch,
        CommittedSubDag, ConsensusHeaderDigest, EpochSeedChainValue, Notifier, ReputationScores,
        TaskManager, B256,
    };

    /// One node's execution side on a fresh temp chain, with its committee installed for the
    /// reward accounting (a closing output needs it).
    struct PipeNode {
        /// The node's execution environment.
        reth_env: RethEnv,
        /// Reward accounting, with the genesis committee installed.
        gas_accumulator: GasAccumulator,
        /// The genesis header: the durable tip before any output.
        genesis: SealedHeader,
        /// Spawns the engine's tasks.
        tasks: TaskManager,
        /// Keeps the database directory alive.
        _dir: tempfile::TempDir,
    }

    impl PipeNode {
        /// A node on `chain` with its committee installed in the gas accumulator.
        async fn new(chain: &Arc<RethChainSpec>) -> eyre::Result<Self> {
            let dir = tempfile::TempDir::new()?;
            let tasks = TaskManager::new("pipelined engine tests");
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

        /// The committee member that leads every output and produces every batch.
        async fn leader(&self) -> eyre::Result<(AuthorityIdentifier, Address)> {
            let committee =
                create_committee_from_state(self.reth_env.epoch_state_from_canonical_tip()?)
                    .await?;
            let authority = committee.authorities().first().cloned().expect("a committee member");
            Ok((authority.id(), authority.execution_address()))
        }

        /// An engine on the node's genesis with the switch set to `pipelined`, fed by the
        /// returned sender and announcing on the returned receiver; the notifier is its shutdown.
        fn engine(
            &self,
            max_round: Option<u64>,
            pipelined: bool,
        ) -> (ExecutorEngine, mpsc::Sender<ConsensusOutput>, mpsc::Receiver<EngineUpdate>, Notifier)
        {
            let (to_engine, rx_output) = mpsc::channel(16);
            let (engine_update_tx, engine_updates) = mpsc::channel(64);
            let shutdown = Notifier::new();
            let engine = ExecutorEngine::new(
                self.reth_env.clone(),
                max_round,
                rx_output,
                self.genesis.clone(),
                shutdown.subscribe(),
                self.tasks.get_spawner(),
                self.gas_accumulator.clone(),
                RepackMonitor::default(),
                engine_update_tx,
            )
            .with_pipelined_persist(pipelined);
            (engine, to_engine, engine_updates, shutdown)
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

        /// Number and hash of every block in reth's in-memory canonical chain, newest first.
        fn in_memory(&self) -> Vec<(u64, B256)> {
            self.reth_env
                .canonical_in_memory_state()
                .canonical_chain()
                .map(|state| (state.number(), state.hash()))
                .collect()
        }

        /// Execute `outputs` one after the other on the sequential path (the oracle).
        async fn execute_sequentially(&self, outputs: Vec<ConsensusOutput>) -> eyre::Result<()> {
            let (tx, _updates) = mpsc::channel(64);
            let mut parent = self.genesis.clone();
            for output in outputs {
                let args = BuildArguments::new(self.reth_env.clone(), output, parent);
                let (gas, tx) = (self.gas_accumulator.clone(), tx.clone());
                parent = tokio::task::spawn_blocking(move || {
                    execute_consensus_output(args, gas, RepackMonitor::default(), tx)
                })
                .await??;
            }
            Ok(())
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

    /// Consensus output `number` at leader round `2 * number`, led by `leader` with `batches`
    /// produced by `producer`.
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

    /// One output per entry of `shape` (its batch count), numbered from one, taking the batches
    /// in order; the last output closes the epoch when `close_last`.
    fn shaped_outputs(
        shape: &[usize],
        batches: &[Batch],
        leader: &AuthorityIdentifier,
        producer: Address,
        close_last: bool,
    ) -> Vec<ConsensusOutput> {
        let mut next = 0;
        shape
            .iter()
            .enumerate()
            .map(|(index, count)| {
                let taken = batches[next..next + count].to_vec();
                next += count;
                let closing = close_last && index + 1 == shape.len();
                consensus_output(index as u64 + 1, leader, producer, taken, closing)
            })
            .collect()
    }

    /// Engine metrics bound to a local debugging recorder, and the recorder's snapshotter.
    fn recorded_metrics() -> (EngineMetrics, Snapshotter) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = ::metrics::with_local_recorder(&recorder, || {
            EngineMetrics::new_with_labels(Vec::<::metrics::Label>::new())
        });
        (metrics, snapshotter)
    }

    /// Every series in one snapshot, by name (`tn_engine.<field>`).
    fn snapshot(snapshotter: &Snapshotter) -> Vec<(String, DebugValue)> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| (key.key().name().to_string(), value))
            .collect()
    }

    /// The value of `name` in `series`.
    fn value<'a>(series: &'a [(String, DebugValue)], name: &str) -> &'a DebugValue {
        series
            .iter()
            .find(|(series_name, _)| series_name == name)
            .map(|(_, value)| value)
            .unwrap_or_else(|| panic!("metric {name} not recorded"))
    }

    /// The engine updates received so far: consensus number and announced block hash.
    fn drain_updates(rx: &mut mpsc::Receiver<EngineUpdate>) -> Vec<(u64, Option<B256>)> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|(_, num, tip)| (num.number, tip.map(|header| header.hash())))
            .collect()
    }

    /// The pipeline's next event, failing the test after a minute.
    async fn next_event(pipeline: &mut PersistPipeline) -> EngineResult<PipelineEvent> {
        tokio::time::timeout(Duration::from_secs(60), pipeline.next_event())
            .await
            .expect("a pipeline event within a minute")
    }

    /// Run `engine` to its end, failing the test after a minute.
    async fn run_to_end<F: std::future::Future<Output = EngineResult<()>>>(
        run: F,
    ) -> EngineResult<()> {
        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("the engine returns within a minute")
    }

    /// I10 and the overlap: six outputs (three with two or three batches, one empty, one with
    /// one batch, the last an empty closing output) through a sequential engine and through a
    /// pipelined one whose commits are slowed so the next build overlaps them. Same blocks, same
    /// trie tables, same engine updates in the same order, nothing left in memory, and the
    /// pipelined engine did overlap: a commit interval is only recorded for an output that
    /// started before the previous output committed, with two outputs in flight.
    #[tokio::test]
    async fn test_pipelined_engine_matches_sequential_engine() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(8);
        let off = PipeNode::new(&chain).await?;
        let on = PipeNode::new(&chain).await?;
        let (leader, producer) = off.leader().await?;
        let outputs = shaped_outputs(&[3, 2, 0, 2, 1, 0], &batches, &leader, producer, true);
        // eight batch blocks plus the closing output's block
        let last_block = 9;

        let (engine, to_engine, mut off_updates, _off_shutdown) = off.engine(None, false);
        for output in &outputs {
            to_engine.send(output.clone()).await?;
        }
        drop(to_engine);
        assert_matches!(
            run_to_end(engine.run()).await,
            Err(TnEngineError::ConsensusOutputStreamClosed)
        );

        on.reth_env.inject_persist_delay(Duration::from_millis(200));
        let (metrics, snapshotter) = recorded_metrics();
        let (engine, to_engine, mut on_updates, _on_shutdown) = on.engine(None, true);
        for output in &outputs {
            to_engine.send(output.clone()).await?;
        }
        drop(to_engine);
        assert_matches!(
            run_to_end(engine.run_pipelined(&metrics)).await,
            Err(TnEngineError::ConsensusOutputStreamClosed)
        );

        assert_eq!(on.reth_env.last_block_number()?, last_block);
        assert_eq!(on.committed(last_block)?, off.committed(last_block)?, "the two engines differ");
        assert!(on.in_memory().is_empty(), "every block is durable and pruned from memory");
        let updates = drain_updates(&mut on_updates);
        assert_eq!(updates, drain_updates(&mut off_updates), "the engine updates differ");
        assert_eq!(
            updates.iter().map(|(number, _)| *number).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );

        let series = snapshot(&snapshotter);
        assert_matches!(
            value(&series, "tn_engine.commit_interval_seconds"),
            DebugValue::Histogram(cycles) if !cycles.is_empty(),
            "no output started while the previous one committed"
        );
        assert_matches!(value(&series, "tn_engine.unpersisted_outputs"), DebugValue::Gauge(g) if g.0 == 0.0);
        Ok(())
    }

    /// I4 at the engine level: nothing builds on a closing output until it has committed.
    ///
    /// The closing output (two blocks) is followed at once by the next output. The closing
    /// output's persist is armed to wait for a third block, which only a build on top of the
    /// closing output could supply: with the barrier that wait runs to its deadline. The next
    /// output is counted once as held by the barrier, its update follows the closing output's,
    /// and its block builds on the closing output's last block.
    #[tokio::test]
    async fn test_close_epoch_output_is_a_barrier() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(3);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        let closing = consensus_output(1, &leader, producer, batches[..2].to_vec(), true);
        let next = consensus_output(2, &leader, producer, batches[2..].to_vec(), false);
        node.reth_env.interleave_next_persist(3, Duration::from_millis(1_500));

        let (metrics, snapshotter) = recorded_metrics();
        let (engine, to_engine, mut updates, _shutdown) = node.engine(None, true);
        to_engine.send(closing).await?;
        to_engine.send(next).await?;
        drop(to_engine);
        assert_matches!(
            run_to_end(engine.run_pipelined(&metrics)).await,
            Err(TnEngineError::ConsensusOutputStreamClosed)
        );

        assert_eq!(
            node.reth_env.persist_interleave_expired(),
            1,
            "a block was built on the closing output before it committed"
        );
        let series = snapshot(&snapshotter);
        assert_matches!(
            value(&series, "tn_engine.close_barrier_holds_total"),
            DebugValue::Counter(1)
        );
        let (headers, _) = node.committed(3)?;
        assert_eq!(
            headers[2].parent_hash,
            headers[1].hash(),
            "the next output builds on the close"
        );
        assert_eq!(
            drain_updates(&mut updates),
            vec![(1, Some(headers[1].hash())), (2, Some(headers[2].hash()))]
        );
        assert!(node.in_memory().is_empty());
        Ok(())
    }

    /// I7: a shutdown noticed while outputs are queued and in flight drains the queue and both
    /// slots before the engine returns `Ok`: all four outputs durable and announced in order, and
    /// nothing left in memory.
    #[tokio::test]
    async fn test_pipelined_shutdown_drains_both_slots() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        node.reth_env.inject_persist_delay(Duration::from_millis(150));
        let (engine, to_engine, mut updates, shutdown) = node.engine(None, true);
        for output in shaped_outputs(&[1, 1, 1, 1], &batches, &leader, producer, false) {
            to_engine.send(output).await?;
        }
        let run = tokio::spawn(engine.run());

        // output 1 announced: output 2 is in flight behind it and outputs 3 and 4 are queued
        let (_, first, _) = tokio::time::timeout(Duration::from_secs(60), updates.recv())
            .await?
            .ok_or_else(|| eyre::eyre!("engine update channel closed"))?;
        assert_eq!(first.number, 1);
        shutdown.notify();
        let result = tokio::time::timeout(Duration::from_secs(60), run).await??;
        assert_matches!(result, Ok(()));

        assert_eq!(node.reth_env.last_block_number()?, 4, "every output is durable");
        assert_eq!(
            drain_updates(&mut updates).iter().map(|(number, _)| *number).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert!(node.in_memory().is_empty(), "nothing executed is left undurable");
        drop(to_engine);
        Ok(())
    }

    /// I7: when the consensus stream closes with outputs queued and in flight, the engine commits
    /// every one of them before it returns `ConsensusOutputStreamClosed`.
    #[tokio::test]
    async fn test_pipelined_stream_close_drains() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(5);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        node.reth_env.inject_persist_delay(Duration::from_millis(100));
        let (engine, to_engine, mut updates, _shutdown) = node.engine(None, true);
        for output in shaped_outputs(&[2, 0, 1, 2], &batches, &leader, producer, false) {
            to_engine.send(output).await?;
        }
        drop(to_engine);
        assert_matches!(
            run_to_end(engine.run()).await,
            Err(TnEngineError::ConsensusOutputStreamClosed)
        );

        assert_eq!(node.reth_env.last_block_number()?, 5, "every output is durable");
        let updates = drain_updates(&mut updates);
        assert_eq!(updates.iter().map(|(number, _)| *number).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        assert_eq!(updates[1].1, None, "the empty output builds no block");
        assert!(node.in_memory().is_empty());
        Ok(())
    }

    /// test-utils `max_round`: the engine stops right after the commit of the output that reaches
    /// it and starts nothing after that output, so the database ends at it and nothing is left in
    /// memory, though more outputs were queued.
    #[tokio::test]
    async fn test_pipelined_max_round_stops_after_commit() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        node.reth_env.inject_persist_delay(Duration::from_millis(100));
        // output 2 leads round 4
        let (engine, to_engine, mut updates, _shutdown) = node.engine(Some(4), true);
        for output in shaped_outputs(&[1, 1, 1, 1], &batches, &leader, producer, false) {
            to_engine.send(output).await?;
        }
        assert_matches!(run_to_end(engine.run()).await, Ok(()));

        assert_eq!(
            node.reth_env.last_block_number()?,
            2,
            "the database ends at the max-round output"
        );
        assert!(node.in_memory().is_empty(), "nothing after the max-round output was built");
        assert_eq!(
            drain_updates(&mut updates).iter().map(|(number, _)| *number).collect::<Vec<_>>(),
            vec![1, 2]
        );
        drop(to_engine);
        Ok(())
    }

    /// The pipelined loop records the design's series under their names: the persist wait of
    /// every dispatched output, the saturated commit interval, the occupancy and the two heights.
    #[tokio::test]
    async fn test_pipelined_metrics_recorded() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(4);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        node.reth_env.inject_persist_delay(Duration::from_millis(150));
        let (metrics, snapshotter) = recorded_metrics();
        let (engine, to_engine, _updates, _shutdown) = node.engine(None, true);
        for output in shaped_outputs(&[2, 1, 1], &batches, &leader, producer, false) {
            to_engine.send(output).await?;
        }
        drop(to_engine);
        assert_matches!(
            run_to_end(engine.run_pipelined(&metrics)).await,
            Err(TnEngineError::ConsensusOutputStreamClosed)
        );

        let series = snapshot(&snapshotter);
        assert_matches!(value(&series, "tn_engine.persist_wait_seconds"), DebugValue::Histogram(waits) if waits.len() == 3);
        assert_matches!(value(&series, "tn_engine.commit_interval_seconds"), DebugValue::Histogram(cycles) if !cycles.is_empty() && cycles.len() <= 2);
        assert_matches!(value(&series, "tn_engine.unpersisted_outputs"), DebugValue::Gauge(g) if g.0 == 0.0);
        assert_matches!(value(&series, "tn_engine.built_height"), DebugValue::Gauge(g) if g.0 == 4.0);
        assert_matches!(value(&series, "tn_engine.canonical_height"), DebugValue::Gauge(g) if g.0 == 4.0);
        assert_matches!(value(&series, "tn_engine.outputs_executed_total"), DebugValue::Counter(3));
        assert_matches!(value(&series, "tn_engine.execution_duration_seconds"), DebugValue::Histogram(latencies) if latencies.len() == 3);
        assert_matches!(value(&series, "tn_engine.queued_outputs"), DebugValue::Gauge(g) if g.0 == 0.0);
        assert_matches!(
            value(&series, "tn_engine.close_barrier_holds_total"),
            DebugValue::Counter(0)
        );
        Ok(())
    }

    /// I9: reth's in-memory chain while a commit lands beside the next output's build.
    ///
    /// Mid-build: output 1's commit is armed to land after output 2's first block and before its
    /// second ([`RethEnv::interleave_next_persist`]), so output 1's announce and `finalize_block`
    /// (which prunes persisted blocks from memory) run while output 2 builds; once output 1 has
    /// committed, memory holds only output 2's blocks, contiguous from its first, each resolving
    /// by hash. Parked: output 3's commit is armed to land after all of output 4 is built; then
    /// memory holds exactly output 4's blocks. No wait expired (each commit landed where it was
    /// armed), and the committed chain equals a sequential node's.
    #[tokio::test]
    async fn test_finalize_during_next_build_keeps_unpersisted_blocks() -> eyre::Result<()> {
        let (chain, batches) = funded_chain(10);
        let node = PipeNode::new(&chain).await?;
        let (leader, producer) = node.leader().await?;
        // blocks 1-2, 3-5, 6-7 and 8-10
        let outputs = shaped_outputs(&[2, 3, 2, 3], &batches, &leader, producer, false);
        let (engine_update_tx, _updates) = mpsc::channel(64);
        let mut pipeline = node.pipeline(engine_update_tx);

        // mid-build
        node.reth_env.interleave_next_persist(3, Duration::from_secs(30));
        pipeline.start_inline(outputs[0].clone())?;
        assert_matches!(next_event(&mut pipeline).await?, PipelineEvent::Built(report) if report.consensus_number == 1);
        assert_matches!(
            next_event(&mut pipeline).await?,
            PipelineEvent::PersistStarted { consensus_number: 1, .. }
        );
        pipeline.start_inline(outputs[1].clone())?;
        loop {
            match next_event(&mut pipeline).await? {
                PipelineEvent::Committed(report) if report.consensus_number == 1 => break,
                PipelineEvent::Built(report) if report.consensus_number == 2 => {}
                other => panic!("unexpected event before output 1 committed: {other:?}"),
            }
        }
        assert_eq!(node.reth_env.last_block_number()?, 2);
        let memory = node.in_memory();
        let mut numbers: Vec<u64> = memory.iter().map(|(number, _)| *number).collect();
        numbers.reverse();
        assert!(
            !numbers.is_empty() && numbers.iter().copied().eq(3..3 + numbers.len() as u64),
            "{memory:?}"
        );
        for (_, hash) in &memory {
            node.reth_env.state_by_block_hash_for_test(*hash)?;
        }
        while !pipeline.is_idle() {
            next_event(&mut pipeline).await?;
        }
        assert_eq!(node.reth_env.last_block_number()?, 5);

        // parked
        node.reth_env.interleave_next_persist(2 + 3, Duration::from_secs(30));
        pipeline.start_inline(outputs[2].clone())?;
        assert_matches!(next_event(&mut pipeline).await?, PipelineEvent::Built(report) if report.consensus_number == 3);
        assert_matches!(
            next_event(&mut pipeline).await?,
            PipelineEvent::PersistStarted { consensus_number: 3, .. }
        );
        pipeline.start_inline(outputs[3].clone())?;
        assert_matches!(next_event(&mut pipeline).await?, PipelineEvent::Built(report) if report.consensus_number == 4 && report.seeded);
        let before = node.in_memory();
        assert_eq!(
            before.iter().map(|(number, _)| *number).collect::<Vec<_>>(),
            vec![10, 9, 8, 7, 6]
        );
        assert_matches!(next_event(&mut pipeline).await?, PipelineEvent::Committed(report) if report.consensus_number == 3);
        let after = node.in_memory();
        assert_eq!(after, before[..3].to_vec(), "memory holds exactly output 4's blocks");
        for (_, hash) in &after {
            node.reth_env.state_by_block_hash_for_test(*hash)?;
        }
        while !pipeline.is_idle() {
            next_event(&mut pipeline).await?;
        }

        assert_eq!(node.reth_env.persist_interleave_expired(), 0, "a commit missed its point");
        assert!(node.in_memory().is_empty());
        let oracle = PipeNode::new(&chain).await?;
        oracle.execute_sequentially(outputs).await?;
        assert_eq!(node.committed(10)?, oracle.committed(10)?, "pipelined and sequential differ");
        Ok(())
    }
}
