// SPDX-License-Identifier: MIT or Apache-2.0
//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

// silence unused lib deps that are used in IT tests
#![allow(unused_crate_dependencies)]

mod error;
mod metrics;
mod payload_builder;
use crate::metrics::ENGINE_METRICS;
use error::EngineResult;
pub use error::TnEngineError;
use futures::{future::OptionFuture, StreamExt};
pub use payload_builder::execute_consensus_output;
use std::collections::VecDeque;
use tn_reth::{payload::BuildArguments, RethEnv};
use tn_types::{
    gas_accumulator::GasAccumulator, ConsensusOutput, EngineUpdate, Noticer, SealedHeader,
    TaskSpawner,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

/// Maximum number of consensus outputs buffered for execution.
///
/// Each queued output holds a full `CommittedSubDag` plus batches, so an unbounded queue lets a
/// lagging engine consume all memory while consensus keeps committing. Once the queue is full
/// the engine stops polling the consensus stream, pushing backpressure into the `to_engine`
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
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
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
    /// Sends (leader_round, consensus_num_hash, Option<SealedHeader>) after each
    /// ConsensusOutput is processed.
    engine_update_tx: mpsc::Sender<EngineUpdate>,
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
        }
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask {
        let (tx, rx) = oneshot::channel();

        // pop next output in queue and execute
        if let Some(output) = self.queued.pop_front() {
            let reth_env = self.reth_env.clone();
            let parent = self.parent_header.clone();
            let task_name = format!("execution-output-{}", output.consensus_header_hash());
            let build_args = BuildArguments::new(reth_env, output, parent);

            let gas_accumulator = self.gas_accumulator.clone();
            let engine_update_tx = self.engine_update_tx.clone();
            // spawn blocking task and return future
            self.task_spawner.spawn_blocking_task(task_name, move || {
                let execution_start = std::time::Instant::now();
                // this is safe to call on blocking thread without a semaphore bc it's held in
                // Self::pending_tesk as a single `Option`
                let result =
                    execute_consensus_output(build_args, gas_accumulator, engine_update_tx)
                        .inspect_err(|e| {
                            error!(target: "engine", ?e, "error executing consensus output");
                        });
                ENGINE_METRICS.execution_duration_seconds.record(execution_start.elapsed());
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
            tracing::trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_round,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_round
    }

    /// TESTING ONLY
    ///
    /// Push a consensus output to the back of `Self::queued`.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn push_back_queued_for_test(&mut self, output: ConsensusOutput) {
        self.queued.push_back(output)
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
                            // queue the output for local execution
                            self.queued.push_back(output);
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
                    debug!(target: "engine", ?res, "reciever for execution result polled ready");
                    let finalized_header = res.map_err(Into::into).and_then(|res| res)?;
                    // the in-flight task resolved; clear the slot so the next round can be spawned
                    self.pending_task = None;
                    // store last executed header in memory
                    self.parent_header = finalized_header;
                    ENGINE_METRICS.outputs_executed_total.increment(1);
                    ENGINE_METRICS.canonical_height.set(self.parent_header.number as f64);
                    ENGINE_METRICS.queued_outputs.set(self.queued.len() as f64);

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
