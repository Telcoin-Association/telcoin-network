// SPDX-License-Identifier: MIT or Apache-2.0
//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod error;
mod payload_builder;
use error::{EngineResult, TnEngineError};
use futures::{Future, StreamExt};
use futures_util::FutureExt;
pub use payload_builder::execute_consensus_output;
use std::{
    collections::VecDeque,
    pin::{pin, Pin},
    task::{Context, Poll},
};
use tn_reth::{payload::BuildArguments, RethEnv};
use tn_types::{
    gas_accumulator::GasAccumulator, ConsensusOutput, Noticer, SealedHeader, TaskSpawner,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

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
}

impl ExecutorEngine {
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// The engine waits for CL to broadcast output then tries to execute.
    ///
    /// Propagates any database related error.
    pub fn new(
        reth_env: RethEnv,
        max_round: Option<u64>,
        rx_consensus_output: mpsc::Receiver<ConsensusOutput>,
        parent_header: SealedHeader,
        rx_shutdown: Noticer,
        task_spawner: TaskSpawner,
        gas_accumulator: GasAccumulator,
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
            // spawn blocking task and return future
            self.task_spawner.spawn_blocking_task(task_name, move || {
                // this is safe to call on blocking thread without a semaphore bc it's held in
                // Self::pending_tesk as a single `Option`
                let result = execute_consensus_output(build_args, gas_accumulator).inspect_err(|e| {
                    error!(target: "engine", ?e, "error executing consensus output");
                });
                if let Err(e) = tx.send(result) {
                    error!(target: "engine", ?e, "error sending result from execute_consensus_output")
                }
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
        let has_reached_max_round =
            self.max_round.map(|target| progress >= target).unwrap_or_default();
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
impl Future for ExecutorEngine {
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check for shutdown signal
        if pin!(&this.rx_shutdown).poll(cx).is_ready() {
            info!(target: "engine", "received shutdown signal...");
            // only return if there are no current tasks and the queue is empty
            // otherwise, let the loop continue so any remaining tasks and queued output is
            // executed
            // rx_shutdown should continue to poll ready so once the queue is clear should shutdown.
            if this.pending_task.is_none() && this.queued.is_empty() {
                return Poll::Ready(Ok(()));
            }
        }

        loop {
            // check if output is available from consensus to keep broadcast stream from "lagging"
            match this.consensus_output_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(output)) => {
                    // queue the output for local execution
                    this.queued.push_back(output)
                }
                Poll::Ready(None) => {
                    // the stream has ended
                    error!(target: "engine", "ConsensusOutput channel closed. Shutting down...");

                    // only return if there are no current tasks and the queue is empty
                    // otherwise, let the loop continue so any remaining tasks and queued output is
                    // executed
                    if this.pending_task.is_none() && this.queued.is_empty() {
                        return Poll::Ready(Err(TnEngineError::ConsensusOutputStreamClosed));
                    }
                }

                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if this.pending_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to begin executing next round of consensus
                this.pending_task = Some(this.spawn_execution_task());
            }

            // poll receiver that returns output execution result
            if let Some(mut receiver) = this.pending_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        debug!(target: "engine", ?res, "reciever for execution result polled ready");
                        let finalized_header = res.map_err(Into::into).and_then(|res| res)?;
                        // store last executed header in memory
                        this.parent_header = finalized_header;

                        // check max_round to auto shutdown
                        #[cfg(any(test, feature = "test-utils"))]
                        if this.max_round.is_some()
                            && this.has_reached_max_round(this.parent_header.nonce.into())
                        {
                            // immediately terminate if the specified max consensus round is reached
                            return Poll::Ready(Ok(()));
                        }

                        // allow loop to continue: poll broadcast stream for next output
                    }
                    Poll::Pending => {
                        this.pending_task = Some(receiver);

                        // break loop and return Poll::Pending
                        break;
                    }
                }
            }
        }

        // all output executed, yield back to runtime
        Poll::Pending
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
