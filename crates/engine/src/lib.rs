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
mod recovery;
use crate::metrics::{EngineMetrics, ENGINE_METRICS};
use error::EngineResult;
pub use error::TnEngineError;
use futures::{future::OptionFuture, StreamExt};
use payload_builder::execute_consensus_output_with_signers;
pub use payload_builder::{execute_consensus_output, PERSIST_OUTPUT_ATTEMPTS};
use recovery::RecoveryHandle;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tn_reth::{payload::BuildArguments, RethEnv};
use tn_types::{
    gas_accumulator::GasAccumulator, repack_monitor::RepackMonitor, ConsensusOutput, EngineUpdate,
    Noticer, SealedHeader, TaskSpawner,
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
        }
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
    /// An output with no batches has no signers to recover and gets an already-resolved handle
    /// instead of a job, so the empty outputs committed at idle record no
    /// `tn_engine_recover_seconds` sample and log no `signers recovered` line.
    fn enqueue(&mut self, queued_at: Instant, output: ConsensusOutput) {
        let signers = if output.batches().iter().all(|cert| cert.batches.is_empty()) {
            RecoveryHandle::resolved(Vec::new())
        } else {
            RecoveryHandle::spawn(&output, &self.task_spawner, &ENGINE_METRICS)
        };
        self.queued.push_back((queued_at, output, signers));
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
    pub async fn run(mut self) -> EngineResult<()> {
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

/// The engine's queued-output path, for benches: [`ExecutorEngine`] starts each output's signer
/// recovery on a blocking job when it queues the output, and executes the output with the job's
/// result once the outputs ahead of it have executed.
#[cfg(feature = "test-utils")]
pub mod test_utils {
    use crate::{
        metrics::ENGINE_METRICS, payload_builder::execute_consensus_output_with_signers,
        recovery::RecoveryHandle, TnEngineError,
    };
    use tn_reth::payload::BuildArguments;
    use tn_types::{
        gas_accumulator::GasAccumulator, repack_monitor::RepackMonitor, ConsensusOutput,
        EngineUpdate, SealedHeader, TaskSpawner,
    };
    use tokio::sync::mpsc;

    /// A queued output's signer recovery, running on a blocking job.
    #[derive(Debug)]
    pub struct QueuedRecovery(RecoveryHandle);

    impl QueuedRecovery {
        /// Start recovering `output`'s signers on a blocking job spawned on `task_spawner`, as
        /// the engine does when it queues the output. Must be called within a tokio runtime.
        pub fn spawn(output: &ConsensusOutput, task_spawner: &TaskSpawner) -> Self {
            Self(RecoveryHandle::spawn(output, task_spawner, &ENGINE_METRICS))
        }
    }

    /// [`crate::execute_consensus_output`] with the signers of `recovery`, the job started when
    /// the output was queued: the build waits for the job before its first block. Must run on a
    /// blocking thread.
    pub fn execute_queued_output(
        args: BuildArguments,
        recovery: QueuedRecovery,
        gas_accumulator: GasAccumulator,
        repack_monitor: RepackMonitor,
        engine_update_tx: mpsc::Sender<EngineUpdate>,
    ) -> Result<SealedHeader, TnEngineError> {
        execute_consensus_output_with_signers(
            args,
            Some(recovery.0),
            gas_accumulator,
            repack_monitor,
            engine_update_tx,
        )
    }
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
}
