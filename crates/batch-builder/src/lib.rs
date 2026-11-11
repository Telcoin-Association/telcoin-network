// SPDX-License-Identifier: MIT or Apache-2.0
//! The block builder maintains the transaction pool and builds the next block.
//!
//! The block builder listens for canonical state changes from the engine and updates the
//! transaction pool. These updates move transactions to the correct sub-pools. Only transactions in
//! the pending pool are considered for the next block.
//!
//! Upon successfully building the next block, the block builder forwards to the worker's block
//! provider. The worker's block provider reliably broadcasts the block and tries to reach quorum
//! within a time limit. If quorum fails, the block builder receives the error and does not mine the
//! transactions. If quorum is reached, the transactions are mined and removed from the pending
//! pool. When this task removes transactions from the pending pool, it uses the current canonical
//! tip. The pool's pending base fee is never taken from a canonical update: it always comes from
//! the worker's shared [`BaseFeeContainer`](tn_types::gas_accumulator::BaseFeeContainer), the gas
//! accumulator's fee for the current epoch (issue #1262).
//!
//! With the `sealed_tx_keep` switch on, a builder whose worker seals by quorum keeps a sealed
//! batch's transactions in the pool instead, marked under the batch digest until the batch
//! executes, and skips them when it selects the next batch (issue #1807).

// it tests
#![allow(unused_crate_dependencies)]

pub use batch::{build_batch, BatchBuilderOutput};
use error::{BatchBuilderError, BatchBuilderResult};
use futures_util::{future::OptionFuture, FutureExt as _, Stream, StreamExt};
use std::{
    fmt,
    pin::Pin,
    time::{Duration, Instant},
};
use tn_reth::{
    sealed_txs::SealTicket, CanonStateNotificationStream, ChangedAccount, RethEnv, WorkerTxPool,
};
use tn_types::{
    error::BlockSealError, Address, BatchBuilderArgs, BatchSender, BlockHash, Epoch, SealedBatch,
    SealedHeader, TaskSpawner, TxHash, WorkerId,
};
use tokio::{
    sync::oneshot,
    task::JoinError,
    time::{Instant as TokioInstant, Interval, Sleep},
};
use tracing::{debug, error, field, info, info_span, warn, Instrument};

mod batch;
mod error;
mod metrics;
#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use crate::metrics::{record_base_fee, record_pending_pool_transactions};
use crate::{
    batch::BuildStats,
    metrics::{BatchBuilderMetrics, BuildReasonCounters},
};

/// The result of a successful batch build containing data needed to update the pool.
#[derive(Debug)]
struct MinedBatchResult {
    /// Collection of transactions included in the accepted batch.
    mined_transactions: Vec<TxHash>,
    /// Account changes used to update the pool after mining a batch.
    changed_accounts: Vec<ChangedAccount>,
}

/// How one batch build task ended, as the run loop consumes it. Fatal errors travel beside
/// this as the `Err` of the surrounding [`BatchBuilderResult`].
#[derive(Debug)]
enum BuildOutcome {
    /// Transaction selection produced no batch to propose. Defer the next build without
    /// sealing, updating the pool, or changing the forward-admission backoff.
    Empty,
    /// The batch reached quorum (or a forward task was admitted to deliver it): prune the
    /// mined transactions from the pool and apply the account changes.
    Mined(MinedBatchResult),
    /// The batch reached quorum and its transactions stay in the pool, marked as sealed under
    /// its digest until it executes (switch `sealed_tx_keep`, on a worker that seals by quorum).
    /// Nothing is pruned and no account changes apply: an optimistic nonce advance would make
    /// the pool discard the very transactions it keeps. The run loop builds again at once, as
    /// after a mined batch.
    Kept,
    /// Selection skipped only transactions sealed into this node's unexecuted batches (switch
    /// `sealed_tx_keep`), so nothing is buildable until one of them executes. Like
    /// [`Self::Empty`] nothing reaches the worker and the next build waits for a wake-up, but
    /// there is nothing to retry, so arrivals arm the fill delay again, as after a pass that
    /// finds nothing pending.
    AllSealed,
    /// The worker refused the seal because no forward admitted the batch
    /// ([`BlockSealError::NotValidator`], issue #1132). The pool keeps its transactions; the
    /// run loop retries on a dedicated backoff and gates its logging on state changes rather
    /// than emitting per attempt (issue #1145).
    Refused,
    /// Any other non-fatal seal failure (quorum, timeout, reporting, or the refusal once this
    /// epoch's consensus shutdown has begun). The pool keeps its transactions and the loop
    /// retries on the next delay tick, as before.
    Failed,
}

/// Ceiling for the refusal backoff (issue #1145).
///
/// The refusal condition (no committee validator advertising a reachable `worker.rpc`) can
/// persist for a whole epoch, and every retry re-walks and re-encodes the entire pending set.
/// Doubling from the configured batch delay up to this cap turns that from once a second into
/// a probe every half minute, while an endpoint that comes back is picked up within one cap at
/// worst. Matches the forwarder's endpoint cooldown, so one recovery probe is not refused
/// against a demotion window that has already expired.
const REFUSAL_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// How long an idle builder waits after a transaction arrives before it builds.
///
/// Without an arrival wake, a transaction reaching an idle builder waits for the next
/// `max_batch_delay` tick (250 ms on the devnet, half that on average) or canonical update.
/// Building on the first arrival alone would seal one transaction per batch during a burst, so
/// the first arrival arms this delay and the build takes whatever landed meanwhile. Later
/// arrivals do not extend it, so a steady stream cannot postpone the build. It is armed only
/// while no build is in flight and no refusal backoff holds the gate. After a failed or empty
/// build, arrivals arm nothing until the next tick or a mined batch, so a retry keeps the tick
/// cadence and peers that just rejected a batch are not asked again one fill delay later. The
/// hold lasts only while something is pending: once a pass of the loop finds nothing pending
/// there is nothing to retry, and the next arrival arms the delay again.
///
/// An admission into any subpool arms it, so a queued (nonce-gap) or parked arrival does too;
/// when that delay expires with nothing pending, the loop pushes the tick out by one period.
/// This cannot strand a transaction: an idle builder pushes the tick out only while nothing is
/// pending, so a transaction that becomes pending is built on the next wake at an open gate,
/// one tick later at the latest.
const BATCH_WAKE_FILL_DELAY: Duration = Duration::from_millis(40);

/// Live refusal-backoff state, present exactly while the last seal attempt was refused.
#[derive(Debug)]
struct RefusalBackoff {
    /// When the build gate re-opens. The periodic delay tick keeps firing through the
    /// backoff, so the gate is re-checked on the first tick after this passes.
    until: TokioInstant,
    /// Delay that produced `until`; doubles per consecutive refusal up to
    /// [`REFUSAL_BACKOFF_CAP`].
    delay: Duration,
    /// Consecutive refused seals, reported by the recovery log line.
    refusals: u32,
}

/// What woke the run loop for the iteration that started a build, reported as `wake=` on the
/// `batch built` line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WakeSource {
    /// The `max_batch_delay` tick. The loop's first pass reports this too: an interval's first
    /// tick completes immediately, so entering the loop is that first tick.
    Tick,
    /// A canonical state notification from the engine.
    Canonical,
    /// The previous batch's quorum ack: a mined completion continues straight into the next
    /// build. Any other completion skips one pass, so the next wake-up names the build.
    Ack,
    /// The fill delay armed by a transaction arriving at an idle builder
    /// ([`BATCH_WAKE_FILL_DELAY`]).
    ///
    /// A tick or canonical update that lands inside the fill-delay window builds at once under
    /// its own label and spends the armed delay, so the `wake=arrival` share of built batches is
    /// a lower bound on the builds an arrival prompted, not the exact share.
    Arrival,
}

impl WakeSource {
    /// The static label carried by the `batch built` line.
    const fn label(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Canonical => "canonical",
            Self::Ack => "ack",
            Self::Arrival => "arrival",
        }
    }
}

/// The run loop's view of the pool when it started a build, carried to the `batch built` line.
#[derive(Clone, Copy, Debug)]
struct BuildTrigger {
    /// What woke the loop for the iteration that started the build.
    wake: WakeSource,
    /// Pending sub-pool size when the build started.
    pending_before: usize,
    /// Queued (nonce-gap) sub-pool size when the build started.
    queued_before: usize,
}

/// Admission notifications from the worker's pool, one per transaction added to any sub-pool
/// ([`WorkerTxPool::admitted_transactions`]).
struct Arrivals(Pin<Box<dyn Stream<Item = ()> + Send>>);

impl Arrivals {
    /// Discard every notification already buffered, without waiting. Bounded by the pool's
    /// listener channel capacity; tokio's coop budget can also end it early, and whatever is
    /// left drains on the next wake-up.
    ///
    /// `now_or_never` polls with a noop waker, and the last poll of an empty channel leaves it
    /// registered. That is safe only because every caller re-polls the stream in the run loop's
    /// next `select!` before the task parks on it. The ack arm awaits the pool update between
    /// its drain and that re-poll; a notification sent meanwhile wakes nobody, but it stays in
    /// the channel and the real poll finds it.
    ///
    /// The pool keeps this listener when its channel (1024 slots by default, set by
    /// `--txpool.max-new-txns`) is full and drops the notification instead (reth's
    /// `TransactionListener::send_all`), so a full channel never ends the stream, and a dropped
    /// notification costs at most one tick.
    fn drain(&mut self) {
        while let Some(Some(())) = self.0.next().now_or_never() {}
    }
}

impl fmt::Debug for Arrivals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Arrivals")
    }
}

/// Receiver for the async task that awaits a blocking batch build and the worker's seal result.
type BuildResult = oneshot::Receiver<BatchBuilderResult<BuildOutcome>>;

/// The type that builds blocks for workers to propose.
///
/// This is a future that:
/// - listens for canonical state changes and updates the tx pool
/// - polls the transaction pool for pending transactions
///     - tries to build the next batch when there transactions are available
#[derive(Debug)]
pub struct BatchBuilder {
    /// Single active task that awaits batch construction on the blocking pool, proposes it, and
    /// returns the seal result through a oneshot channel.
    pending_task: Option<BuildResult>,
    /// The transaction pool with pending transactions.
    pool: WorkerTxPool,
    /// The sending side to the worker's batch maker.
    ///
    /// Sending the new block through this channel triggers a broadcast to all peers.
    ///
    /// The worker's block maker sends an ack once the block has been stored in db
    /// which guarantees the worker will attempt to broadcast the new block until
    /// quorum is reached.
    to_worker: BatchSender,
    /// The address for batch's beneficiary.
    address: Address,
    /// Maximum amount of time to wait before querying block builds.
    ///
    /// This interval wakes the task periodically to check on the progress of the latest built
    /// block and the pending transaction pool.
    max_delay_interval: Interval,

    /// This channel will receive a header on canonical update.  We use it to wakeup the future and
    /// save the canonical update.
    state_changed: CanonStateNotificationStream,
    /// The last canonical tip header, saved when state_changed sends a new update.
    last_canonical_update: SealedHeader,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// Worker id this batch builder belongs too.
    worker_id: WorkerId,
    /// The base fee for this epoch. Constant for the batch builder's lifetime.
    base_fee: u64,
    /// The epoch we are building batches for.
    epoch: Epoch,
    /// Prometheus metrics for this worker's batch builder.
    metrics: BatchBuilderMetrics,
    /// Pre-resolved `builds_total` series of this worker, one per build reason.
    build_counters: BuildReasonCounters,
    /// Backoff applied while every seal attempt is refused by forward admission
    /// (issue #1145). `None` whenever the last completed build was not a refusal.
    refusal_backoff: Option<RefusalBackoff>,
    /// Transaction admissions into the worker's pool; an arrival wakes an idle builder.
    arrivals: Arrivals,
    /// The fill delay armed by the first arrival at an idle builder, `None` while disarmed.
    fill_delay: Option<Pin<Box<Sleep>>>,
    /// Keep a sealed batch's transactions in the pool, marked, until they execute
    /// (`sealed_tx_keep` in `parameters.yaml`).
    sealed_tx_keep: bool,
    /// True when the worker this builder feeds seals by committee quorum
    /// (`Worker::seals_via_quorum`). `sealed_tx_keep` acts only then: a forwarding worker's
    /// batch is re-packed by the committee under other digests, so its own digest never
    /// executes and nothing would clear the marks.
    seals_via_quorum: bool,
}

impl BatchBuilder {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reth_env: &RethEnv,
        pool: WorkerTxPool,
        to_worker: BatchSender,
        address: Address,
        max_delay: Duration,
        task_spawner: TaskSpawner,
        worker_id: WorkerId,
        base_fee: u64,
        epoch: Epoch,
    ) -> BatchBuilderResult<Self> {
        let max_delay_interval = tokio::time::interval(max_delay);
        let state_changed = reth_env.canonical_block_stream();
        let last_canonical_update = Self::latest_canon_header(reth_env)?;
        let metrics = BatchBuilderMetrics::new_for_worker(worker_id);
        let arrivals = Arrivals(Box::pin(pool.admitted_transactions()));
        // constant per epoch
        metrics.base_fee.set(base_fee as f64);
        Ok(Self {
            pending_task: None,
            pool,
            to_worker,
            address,
            max_delay_interval,
            state_changed,
            last_canonical_update,
            task_spawner,
            worker_id,
            base_fee,
            epoch,
            metrics,
            build_counters: BuildReasonCounters::new_for_worker(worker_id),
            refusal_backoff: None,
            arrivals,
            fill_delay: None,
            // the env builds the worker pools from the same switch, so both sides agree
            sealed_tx_keep: reth_env.sealed_tx_keep(),
            // the forwarding default keeps today's prune path until the node says otherwise
            seals_via_quorum: false,
        })
    }

    /// Set whether a sealed batch's transactions stay in the pool, marked, until they execute
    /// (`sealed_tx_keep` in `parameters.yaml`).
    ///
    /// [`BatchBuilder::new`] starts with the value of [`RethEnv::sealed_tx_keep`], the switch the
    /// env's worker pools are built with; this overrides it. `false` removes the transactions
    /// from the pool once the batch is sealed. The node applies the configured value right after
    /// construction.
    pub fn with_sealed_tx_keep(mut self, keep: bool) -> Self {
        self.sealed_tx_keep = keep;
        self
    }

    /// Whether a sealed batch's transactions stay in the pool, marked, until they execute.
    pub fn sealed_tx_keep(&self) -> bool {
        self.sealed_tx_keep
    }

    /// Set whether the worker this builder feeds seals by committee quorum
    /// (`Worker::seals_via_quorum`).
    ///
    /// [`BatchBuilder::new`] starts with `false`, the forwarding case. Only a builder for a
    /// quorum-sealing worker marks a sealed batch's transactions (with `sealed_tx_keep` on);
    /// with `false` every quorum ack prunes the batch from the pool, as with the switch off.
    pub fn with_seals_via_quorum(mut self, seals_via_quorum: bool) -> Self {
        self.seals_via_quorum = seals_via_quorum;
        self
    }

    /// Whether the worker this builder feeds seals by committee quorum.
    pub fn seals_via_quorum(&self) -> bool {
        self.seals_via_quorum
    }

    /// True when a quorum ack marks the batch's transactions instead of pruning them: the
    /// switch is on and the worker seals by quorum.
    fn keeps_sealed(&self) -> bool {
        self.sealed_tx_keep && self.seals_via_quorum
    }

    /// Spawns a task to build the batch and propose to peers.
    ///
    /// This approach allows the batch builder to yield back to the runtime while mining batches.
    ///
    /// The task performs the following actions:
    /// - create a batch
    /// - defer an empty batch, otherwise send it to the worker's batch proposer
    /// - wait for ack that quorum was reached
    /// - convert result to fatal/non-fatal
    /// - return result
    ///
    /// Workers only propose one batch at a time. `trigger` is the run loop's view when it started
    /// this build; it only feeds the `batch built` line.
    fn spawn_execution_task(&self, trigger: BuildTrigger) -> BuildResult {
        let pool = self.pool.clone();
        let to_worker = self.to_worker.clone();

        // configure params for next building the next batch
        let build_args = BatchBuilderArgs::new(pool.clone(), self.address, self.epoch);
        let (result, done) = oneshot::channel();
        let worker_id = self.worker_id;
        let base_fee = self.base_fee;
        let metrics = self.metrics.clone();
        let build_counters = self.build_counters.clone();
        let keeps_sealed = self.keeps_sealed();

        let span = info_span!(target: "telcoin", "propose-batch", batch = field::Empty, worker_id, base_fee, epoch = self.epoch);
        let span_clone = span.clone();
        // spawn block building task and forward to worker
        self.task_spawner.spawn_task("next-batch", async move {
            let seal_start = Instant::now();
            // ack once worker reaches quorum
            let (ack, rx) = oneshot::channel();

            // this is safe to call without a semaphore bc it's held as a single `Option`
            let BatchBuilderOutput { batch, mined_transactions, changed_accounts, peer_deferred, unpackable, sealed_skipped, stats } =
                match batch::spawn_batch_build(build_args, worker_id, base_fee).await {
                    Ok(output) => output,
                    Err(e) => {
                        error!(target: "worker::batch_builder", ?e, "blocking batch build failed");
                        // Surface the join error to the run loop. A dropped sender is misreported
                        // as `AckChannelClosed`, and the task manager discards a non-critical
                        // task's `Err` without logging it.
                        let _ = result.send(Err(BatchBuilderError::BlockingTask(e)));
                        return Ok(());
                    }
                };

            // report the transactions this build left to an in-flight peer batch (issue #1329)
            metrics
                .peer_deferred_txs_total
                .increment(u64::try_from(peer_deferred).unwrap_or(u64::MAX));
            // Evictions happen during selection, even if the batch is empty or quorum fails.
            metrics.unpackable_txs_total.increment(u64::try_from(unpackable).unwrap_or(u64::MAX));
            // transactions still in flight in this node's own batches (switch `sealed_tx_keep`)
            metrics.sealed_txs_skipped_total.increment(u64::try_from(sealed_skipped).unwrap_or(u64::MAX));
            // Canonical updates can drain the pending pool after the run loop's build gate.
            // Selection can also skip every candidate, including peer-deferred transactions
            // (issue #1329). Peers reject empty batches, so record the selection metrics above
            // but send nothing to the worker in every empty-build case.
            if batch.transactions().is_empty() {
                debug!(
                    target: "worker::batch_builder",
                    peer_deferred,
                    unpackable,
                    sealed_skipped,
                    "transaction selection produced an empty batch; sealing nothing"
                );
                // a peer deferral leaves something to retry; sealed transactions only wait for
                // their own batch to execute
                let outcome = if sealed_skipped > 0 && peer_deferred == 0 {
                    BuildOutcome::AllSealed
                } else {
                    BuildOutcome::Empty
                };
                result.send(Ok(outcome)).err().into_iter().for_each(|e| {
                    error!(target: "worker::batch_builder", ?e, "failed to send no-work outcome to block builder task");
                });
                Ok(())
            } else {
                // selection has finished and the batch has not reached the worker: any execution
                // of this batch comes after this ticket, so a mark it would undo is refused
                let ticket = keeps_sealed.then(|| pool.seal_ticket());
                let batch = batch.seal_slow();
                let digest = batch.digest();
                span.record("batch", digest.to_string());
                report_batch_built(&metrics, &build_counters, worker_id, &batch, &stats, trigger);

                // forward to worker and wait for ack that quorum was reached
                if let Err(e) = to_worker.send((batch, ack)).await {
                    error!(target: "worker::batch_builder", ?e, "failed to send next batch to worker");
                    // try to return error if worker channel closed
                    let _ = result.send(Err(BatchBuilderError::WorkerChannelClosed));
                    return Err(e.into());
                }

                // wait for worker to ack quorum reached then update pool with mined txs
                match rx.await {
                    Ok(res) => {
                        // measures build + broadcast + quorum, whatever the outcome; a refusal at
                        // consensus shutdown never broadcasts, so it is left out rather than drag
                        // the histogram toward the build time alone
                        if !matches!(res, Err(BlockSealError::ConsensusShuttingDown)) {
                            metrics.seal_duration_seconds.record(seal_start.elapsed());
                        }
                        match res {
                            Ok(_) => {
                                debug!(target: "worker::batch-builder", ?res, "received ack");
                                metrics.batches_sealed_total.increment(1);
                                let mined = MinedBatchResult { mined_transactions, changed_accounts };
                                // mark before reporting completion: the run loop starts the next
                                // selection only after it receives this task's outcome
                                let outcome = match ticket {
                                    Some(ticket) => keep_sealed(&pool, ticket, digest, mined).await,
                                    None => BuildOutcome::Mined(mined),
                                };
                                // signal to Self that this task is complete
                                if let Err(e) = result.send(Ok(outcome)) {
                                    error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                                }
                            }
                            Err(error) => {
                                metrics.record_seal_failure(worker_id, &error);
                                let converted = match error {
                                    BlockSealError::FatalDBFailure => {
                                        // fatal - return error
                                        Err(BatchBuilderError::FatalDBFailure)
                                    }
                                    // The observer refusal is expected steady state whenever
                                    // no committee endpoint is reachable, so the per-attempt
                                    // line stays at debug; the run loop owns the
                                    // state-change-gated logging and the retry backoff
                                    // (issue #1145).
                                    BlockSealError::NotValidator => {
                                        debug!(target: "worker::batch_builder", "batch seal refused: no forward admitted the batch");
                                        Ok(BuildOutcome::Refused)
                                    }
                                    // the worker refuses once this epoch's consensus shutdown has
                                    // begun; a few per worker are expected at every epoch
                                    // boundary until the epoch's tasks are aborted, so this stays
                                    // at debug and leaves the pool untouched
                                    BlockSealError::ConsensusShuttingDown => {
                                        debug!(target: "worker::batch_builder", "batch seal refused: consensus shutdown has begun");
                                        Ok(BuildOutcome::Failed)
                                    }
                                    BlockSealError::QuorumRejected
                                    | BlockSealError::AntiQuorum
                                    | BlockSealError::Timeout
                                    | BlockSealError::FailedToReport
                                    | BlockSealError::FailedQuorum => {
                                        error!(target: "worker::batch_builder", ?error, "error while sealing batch");
                                        // potentially non-fatal error
                                        //
                                        // NOTE: this applies no changes to transaction pool
                                        Ok(BuildOutcome::Failed)
                                    }
                                };

                                if let Err(e) = result.send(converted) {
                                    error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(target: "worker::batch_builder", ?e, "quorum waiter failed ack failed");
                        if let Err(e) = result.send(Err(e.into())) {
                            error!(target: "worker::batch_builder", ?e, "failed to send block builder result to block builder task");
                        }
                    }
                }
                Ok(())
            }
        }.instrument(span_clone));

        // return oneshot channel for receiving completion status
        done
    }

    /// Load the latest persisted tip header, falling back to genesis before a block persists.
    fn latest_canon_header(reth_env: &RethEnv) -> BatchBuilderResult<SealedHeader> {
        reth_env
            .last_block_number()
            .and_then(|num| reth_env.sealed_header_by_number(num))
            .map(|header| header.unwrap_or_else(|| reth_env.chainspec().sealed_genesis_header()))
            .map_err(Into::into)
    }
}

/// The [BatchBuilder] is a future that loops through the following:
/// - check/apply canonical state changes that affect the next build
/// - poll any pending block building tasks
/// - otherwise, build next block if pending transactions are available
///
/// An idle builder wakes on the `max_batch_delay` tick, a canonical update, or a transaction
/// arrival; an arrival builds once the `BATCH_WAKE_FILL_DELAY` fill delay has passed.
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl BatchBuilder {
    /// True while a refusal backoff is live and its delay has not yet passed.
    ///
    /// This is the build gate's view of the backoff: the periodic delay tick keeps waking the
    /// loop through the backoff (each wake-up is a no-op check, not a rebuild of the pending
    /// set), so the gate re-opens on the first tick after the delay passes; canonical-state
    /// wake-ups in between see `true` and skip proposing.
    fn refusal_backoff_holds(&self) -> bool {
        self.refusal_backoff.as_ref().is_some_and(|backoff| TokioInstant::now() < backoff.until)
    }

    /// Note one refused seal and return the delay to hold the build gate closed: the
    /// configured batch delay on the first refusal, doubling per consecutive refusal, capped
    /// at [`REFUSAL_BACKOFF_CAP`].
    ///
    /// The state transition owns the log line (issue #1145): entering refusal warns once, and
    /// every further refusal is debug-level, so a refusal that persists for a whole epoch does
    /// not emit at the batch cadence.
    fn note_refusal(&mut self) -> Duration {
        // A prior backoff whose deadline passed more than one cap ago is a stale episode: the
        // pool emptied before the gate reopened, so no build carried the streak. Restart the
        // episode (fresh warning, base delay) rather than doubling from stale state.
        let live_prior = self
            .refusal_backoff
            .as_ref()
            .filter(|prior| prior.until.elapsed() <= REFUSAL_BACKOFF_CAP);
        let (delay, refusals) = live_prior.map_or_else(
            || {
                let delay = self.max_delay_interval.period();
                warn!(
                    target: "worker::batch_builder",
                    worker_id = self.worker_id,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "no forward admitted the sealed batch; keeping transactions pending and backing off"
                );
                (delay, 1)
            },
            |prior| {
                let refusals = prior.refusals.saturating_add(1);
                let delay = prior.delay.saturating_mul(2).min(REFUSAL_BACKOFF_CAP);
                debug!(
                    target: "worker::batch_builder",
                    worker_id = self.worker_id,
                    refusals,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "forward admission still refusing sealed batches; backing off further"
                );
                (delay, refusals)
            },
        );
        self.refusal_backoff =
            Some(RefusalBackoff { until: TokioInstant::now() + delay, delay, refusals });
        delay
    }

    /// End a refusal backoff after a completed seal, logging the recovery transition when
    /// there was one to end.
    fn note_recovery(&mut self) {
        self.refusal_backoff.take().into_iter().for_each(|ended| {
            info!(
                target: "worker::batch_builder",
                worker_id = self.worker_id,
                refused_seals = ended.refusals,
                "forward admission recovered; resuming normal batch cadence"
            );
        });
    }

    /// Run the batch builder event loop. Runs for the lifetime of the worker.
    pub async fn run(mut self) -> BatchBuilderResult<()> {
        // one line per builder per epoch: `sealed_tx_keep` acts only when `keeps_sealed` is true,
        // which needs the node to report a quorum-sealing worker, so the effective mode is logged
        // beside the configured switch
        info!(
            target: "worker::batch_builder",
            worker_id = self.worker_id,
            epoch = self.epoch,
            sealed_tx_keep = self.sealed_tx_keep,
            seals_via_quorum = self.seals_via_quorum,
            keeps_sealed = self.keeps_sealed(),
            "batch builder started"
        );
        // reproduces the original future's post-quorum-failure `break`: skip starting a build for
        // exactly one iteration so the loop backs off (parks) before retrying, instead of
        // immediately rebuilding the same (still-unmined) transactions. An arrival notification
        // sets it too: that wake-up only arms the fill delay and never builds by itself.
        let mut defer_build = false;
        // what woke the loop last; the build this iteration starts reports it
        let mut wake = WakeSource::Tick;
        // set by a failed or empty completion and cleared by the next tick, a mined batch, or any
        // pass that finds nothing pending: while set, arrivals do not arm the fill delay, so the
        // retry keeps the tick cadence
        let mut failed_hold = false;

        // loop when a successful block is built
        loop {
            // refresh the pending-pool gauge on every wake, gated or not: operators watch it
            // exactly while a backoff or an in-flight build is holding the gate closed
            let pool_size = self.pool.pool_size();
            let pending = pool_size.pending;
            self.metrics.pending_pool_transactions.set(pending as f64);
            // nothing is left to retry, so the next arrival arms the fill delay again instead of
            // waiting out a stale hold for the tick. Checked on every pass, gated or not: the
            // deferred pass after an empty build and the pass after an arrival skip the gate
            if pending == 0 {
                failed_hold = false;
            }
            // only propose one block at a time; a live refusal backoff also holds the gate
            // closed, so a canonical-state wake-up cannot bypass it (issue #1145)
            let backing_off = self.refusal_backoff_holds();
            if self.pending_task.is_none() && !defer_build && !backing_off {
                if pending == 0 {
                    // reset interval to wake up after some time
                    //
                    // only need to reset here if there is no pending block being built
                    self.max_delay_interval.reset();

                    // nothing pending
                } else {
                    // start building the next batch
                    let trigger = BuildTrigger {
                        wake,
                        pending_before: pending,
                        queued_before: pool_size.queued,
                    };
                    // the build takes whatever has arrived, so an armed fill delay is spent
                    self.fill_delay = None;
                    self.pending_task = Some(self.spawn_execution_task(trigger));

                    // don't break so pending_task receiver gets polled
                }
            }

            // consume the one-shot defer now that this build attempt has been skipped
            defer_build = false;

            tokio::select! {
                // drain canonical updates first, then the in-flight build, then the periodic tick
                biased;

                // This is used as a "wake up" when canonical state updates.
                Some(latest) = self.state_changed.next() => {
                    wake = WakeSource::Canonical;
                    // NOTE: separate background task applies these changes to the pool
                    // regardless of pending_task
                    self.last_canonical_update = latest.tip().clone_sealed_header()
                }

                // poll receiver that returns mined transactions once the batch reaches quorum
                //
                // poll here so waker is notified when ack received
                Some(res) = OptionFuture::from(self.pending_task.as_mut()), if self.pending_task.is_some() => {
                    // the build resolved; the original `take`s the receiver and only restores it
                    // while it is still pending, so clear the slot here
                    self.pending_task = None;
                    wake = WakeSource::Ack;
                    // arrivals during the build never arm the fill delay: the build gate below,
                    // or the deferred retry, already covers every transaction now in the pool
                    self.arrivals.drain();

                    debug!(target: "block-builder", ?res, "pending task complete");
                    // ensure no fatal errors
                    let outcome = res??;

                    // Refusal backoff bookkeeping (issue #1145): a refusal arms or extends the
                    // backoff, anything mined ends it. Empty, AllSealed, Refused and Failed take
                    // the pre-existing empty-mined path below: no pool update, one deferred
                    // build, park until a wake-up.
                    let MinedBatchResult { mined_transactions, changed_accounts } = match outcome {
                        BuildOutcome::Mined(mined) => {
                            failed_hold = false;
                            mined
                        }
                        BuildOutcome::Kept => {
                            // the batch's transactions stay pooled and marked, so there is no
                            // pool update to apply; the completed seal still ends a refusal
                            // backoff, and the loop builds again at once, as after a mined batch
                            failed_hold = false;
                            self.note_recovery();
                            continue;
                        }
                        BuildOutcome::AllSealed => {
                            // nothing to retry: what is pending waits for its own batch to
                            // execute, so arrivals arm the fill delay again; the empty-mined path
                            // below still defers the next build until a wake-up
                            failed_hold = false;
                            MinedBatchResult { mined_transactions: vec![], changed_accounts: vec![] }
                        }
                        BuildOutcome::Refused => {
                            self.note_refusal();
                            MinedBatchResult {
                                mined_transactions: vec![],
                                changed_accounts: vec![],
                            }
                        }
                        BuildOutcome::Empty | BuildOutcome::Failed => {
                            failed_hold = true;
                            MinedBatchResult { mined_transactions: vec![], changed_accounts: vec![] }
                        }
                    };

                    // NOTE: a mined batch that pruned nothing applies no pool update; it also
                    // proves nothing about forward admission, so it neither ends nor extends
                    // a refusal backoff (issue #1145)
                    if mined_transactions.is_empty() {
                        // reset interval to prevent immediate re-wake from stale tick
                        self.max_delay_interval.reset();

                        // return pending and wait for canonical update to wake up again
                        defer_build = true;
                        continue;
                    }

                    // a seal that actually pruned transactions ends any refusal backoff
                    self.note_recovery();

                    debug!(target: "block-builder", "applying block builder's update");

                    // update pool to remove mined transactions
                    //
                    // The pool derives its pending fee from the shared per-worker container, so
                    // a stale `last_canonical_update` (at an epoch boundary, the previous
                    // epoch's closing block) cannot reprice the pool (issue #1262).
                    self.pool.update_canonical_state(
                        &self.last_canonical_update,
                        Some(u128::MAX), // set max fee for blobs
                        mined_transactions,
                        changed_accounts,
                    ).await?;

                    // loop again to check for any other pending transactions
                    // and possibly start building the next block
                    //
                    // NOTE: continuing here is important.
                    // To prevent the following scenario, do not wait for task's waker:
                    // - there were more transactions in the pool than could fit in the first
                    //   block
                    // - pending transaction notifications already drained
                    // - have to wait for engine's next canonical update to wake up
                    continue;
                }

                // periodic wake to check the pending pool; gated to no-build-in-flight to match the
                // original, which only polled the interval while `pending_task` was `None`.
                _ = self.max_delay_interval.tick(), if self.pending_task.is_none() => {
                    wake = WakeSource::Tick;
                    failed_hold = false;
                }

                // the fill delay armed by an arrival has passed: build whatever is pending by now,
                // which may be nothing if only queued or parked transactions arrived
                Some(()) = OptionFuture::from(self.fill_delay.as_mut()), if self.fill_delay.is_some() => {
                    self.fill_delay = None;
                    wake = WakeSource::Arrival;
                }

                // a transaction entered any subpool (pending, queued or parked); polled while a
                // build is in flight too and drained so a burst is one wake. The pool's listener
                // channel is bounded and drops when full: a drop costs at most one tick
                Some(()) = self.arrivals.0.next() => {
                    self.arrivals.drain();
                    // every admission defers, pending or not: the build waits for the fill delay
                    // so the rest of a burst lands in it
                    defer_build = true;
                    // arm only at an idle, open gate: one batch in flight per worker, a live
                    // refusal backoff keeps its own cadence (issue #1145) and a failed or empty
                    // build retries on the tick
                    if self.pending_task.is_none()
                        && self.fill_delay.is_none()
                        && !failed_hold
                        && !self.refusal_backoff_holds()
                    {
                        let fill = tokio::time::sleep(BATCH_WAKE_FILL_DELAY);
                        self.fill_delay = Some(Box::pin(fill));
                    }
                }
            }
        }
    }
}

/// Mark the transactions of the quorum-acked batch `digest` as sealed, keeping them in the pool
/// (switch `sealed_tx_keep`), and return the outcome for the run loop.
///
/// `ticket` is the one taken when the batch's selection finished. A mark the pool refuses
/// because the batch already executed still yields [`BuildOutcome::Kept`]: the canonical update
/// for that execution removed what it mined and left what it skipped buildable.
async fn keep_sealed(
    pool: &WorkerTxPool,
    ticket: SealTicket,
    digest: BlockHash,
    mined: MinedBatchResult,
) -> BuildOutcome {
    let marked = pool.mark_sealed(ticket, digest, mined.mined_transactions.clone()).await;
    outcome_after_mark(marked, digest, mined)
}

/// Turn the result of [`WorkerTxPool::mark_sealed`] into the run loop's outcome.
///
/// If the mark's blocking task failed (a panic, or cancellation at runtime shutdown), the batch
/// falls back to today's switch-off path: [`BuildOutcome::Mined`] prunes its transactions and
/// advances each sender's pool nonce. That keeps the builder from re-sealing transactions the
/// worker has already sealed, at the cost of the bench-5 behaviour for this one batch: the node
/// forgets the transactions until they execute.
fn outcome_after_mark(
    marked: Result<usize, JoinError>,
    digest: BlockHash,
    mined: MinedBatchResult,
) -> BuildOutcome {
    match marked {
        Ok(marked) => {
            debug!(
                target: "worker::batch_builder",
                %digest,
                marked,
                num_txs = mined.mined_transactions.len(),
                "kept sealed transactions in the pool"
            );
            BuildOutcome::Kept
        }
        Err(error) => {
            error!(
                target: "worker::batch_builder",
                %digest,
                ?error,
                num_txs = mined.mined_transactions.len(),
                "marking sealed transactions failed; pruning them from the pool instead"
            );
            BuildOutcome::Mined(mined)
        }
    }
}

/// Emit the `batch built` line and record the build metrics for a sealed, non-empty batch.
///
/// One line per batch, never per transaction. Durations are whole milliseconds.
fn report_batch_built(
    metrics: &BatchBuilderMetrics,
    build_counters: &BuildReasonCounters,
    worker_id: WorkerId,
    batch: &SealedBatch,
    stats: &BuildStats,
    trigger: BuildTrigger,
) {
    let millis = |elapsed: Duration| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    metrics.pool_age_seconds.record(stats.pool_age_max);
    build_counters.record(stats.reason);
    info!(
        target: "consensus::metrics",
        digest = %batch.digest(),
        worker_id,
        num_txs = batch.batch().transactions().len(),
        bytes = stats.bytes,
        gas = stats.gas,
        reason = stats.reason.label(),
        pool_age_p50_ms = millis(stats.pool_age_p50),
        pool_age_max_ms = millis(stats.pool_age_max),
        build_ms = millis(stats.build),
        pending_before = trigger.pending_before,
        queued_before = trigger.queued_before,
        wake = trigger.wake.label(),
        "batch built"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::{path::Path, str::FromStr, sync::Arc, time::Duration};
    use tempfile::TempDir;
    use tn_engine::execute_consensus_output;
    use tn_network_types::{local::LocalNetwork, MockWorkerToPrimaryHang};
    use tn_reth::{
        payload::BuildArguments,
        recover_raw_transaction,
        test_utils::{create_committee_from_state, TransactionFactory},
        ForwardTargetPolicy, RethChainSpec, TxPool as _, WorkerRpcForwarder,
    };
    use tn_storage::{open_db, tables::NodeBatchesCache};
    use tn_types::{
        gas_accumulator::{BaseFeeContainer, GasAccumulator},
        test_genesis, BlsKeypair, Bytes, Certificate, CommittedSubDag, ConsensusHeaderDigest,
        ConsensusOutput, Database, Encodable2718, GenesisAccount, NoopTxnForwarder, RpcInfo,
        TaskManager, TxnForwarder, MIN_PROTOCOL_BASE_FEE, U160, U256,
    };
    use tn_worker::{test_utils::TestMakeBlockQuorumWaiter, Worker, WorkerNetworkHandle};
    use tokio::time::timeout;

    /// The trigger handed to builds a test spawns directly, outside the run loop.
    const TEST_TRIGGER: BuildTrigger =
        BuildTrigger { wake: WakeSource::Tick, pending_before: 0, queued_before: 0 };

    /// Drain the pending pool after the build gate but before the spawned task is polled.
    /// The empty result must never reach the worker, and the run loop must defer a newly
    /// affordable transaction until the reset batch delay has elapsed.
    #[tokio::test]
    async fn empty_build_after_pending_pool_drain_is_deferred() -> std::io::Result<()> {
        // Keep setup, spawned builds and acknowledgement bounded by a running clock.
        timeout(Duration::from_secs(10), async {
            let tmp_dir = TempDir::new()?;
            let TestTools { mut tx_factory, execution_components, task_manager } =
                get_test_tools(tmp_dir.path());
            let TestExecutionComponents { reth_env, txpool, chain } = execution_components;
            let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);
            let delay = Duration::from_millis(100);
            let mut batch_builder = BatchBuilder::new(
                &reth_env,
                txpool.clone(),
                to_worker,
                Address::ZERO,
                delay,
                task_manager.get_spawner(),
                0,
                MIN_PROTOCOL_BASE_FEE,
                0,
            )
            .map_err(std::io::Error::other)?;
            let hash = tx_factory
                .create_and_submit_eip1559_pool_tx(
                    chain.clone(),
                    u128::from(MIN_PROTOCOL_BASE_FEE),
                    Address::ZERO,
                    U256::from(10),
                    txpool.clone(),
                )
                .await;

            // Match the run loop's gate, then drain the pending pool before starting the build.
            // Await maintenance so the blocking build deterministically observes the empty pool.
            assert_eq!(txpool.pending_transactions().len(), 1);
            txpool
                .update_canonical_state(
                    &batch_builder.last_canonical_update,
                    None,
                    vec![],
                    vec![ChangedAccount {
                        address: tx_factory.address(),
                        nonce: 0,
                        balance: U256::ZERO,
                    }],
                )
                .await
                .map_err(std::io::Error::other)?;
            assert!(txpool.pending_transactions().is_empty());
            let done = batch_builder.spawn_execution_task(TEST_TRIGGER);

            let outcome = tokio::select! {
                outcome = done => outcome
                    .map_err(std::io::Error::other)?
                    .map_err(std::io::Error::other)?,
                _ = from_batch_builder.recv() => {
                    Err::<BuildOutcome, _>(std::io::Error::other("empty batch reached worker"))?
                }
            };
            assert_matches!(outcome, BuildOutcome::Empty);
            assert_matches!(
                from_batch_builder.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            );

            // Make the same transaction affordable before the loop consumes the empty result.
            // This also proves that an empty build did not remove it from the pool.
            txpool
                .update_canonical_state(
                    &batch_builder.last_canonical_update,
                    None,
                    vec![],
                    vec![ChangedAccount {
                        address: tx_factory.address(),
                        nonce: 0,
                        balance: U256::from(1_000_000_000_u64),
                    }],
                )
                .await
                .map_err(std::io::Error::other)?;
            assert_eq!(txpool.pending_transactions().len(), 1);
            let (result, done) = oneshot::channel();
            result
                .send(Ok(outcome))
                .map_err(|_| std::io::Error::other("build result receiver closed"))?;
            batch_builder.pending_task = Some(done);
            tokio::time::pause();
            let mut builder_task = Box::pin(batch_builder.run());

            // Register the reset deadline before advancing time. Reth's blocking validation
            // tasks inhibit paused-clock auto-advance, so move the clock explicitly.
            assert!(futures_util::poll!(&mut builder_task).is_pending());
            tokio::time::advance(delay.saturating_sub(Duration::from_millis(1))).await;
            assert!(futures_util::poll!(&mut builder_task).is_pending());
            assert_matches!(
                from_batch_builder.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            );
            // Resume before awaiting spawned work so both the interval and watchdog can
            // progress without depending on exact paused-clock timer boundaries.
            tokio::time::resume();
            let (batch, ack) = tokio::select! {
                result = &mut builder_task => Err(std::io::Error::other(format!(
                    "builder exited before proposing a batch: {result:?}"
                ))),
                batch = from_batch_builder.recv() =>
                    batch.ok_or_else(|| std::io::Error::other("worker channel closed")),
            }?;
            assert_eq!(batch.batch().transactions().len(), 1);
            let encoded = batch
                .batch()
                .transactions()
                .first()
                .ok_or_else(|| std::io::Error::other("missing transaction"))?;
            assert_eq!(
                recover_raw_transaction(encoded).map_err(std::io::Error::other)?.hash(),
                &hash
            );
            assert_eq!(txpool.pending_transactions().len(), 1);

            // Acknowledge a fatal result to terminate the builder deterministically.
            ack.send(Err(BlockSealError::FatalDBFailure))
                .map_err(|_| std::io::Error::other("batch acknowledgement receiver closed"))?;
            assert_matches!(builder_task.await, Err(BatchBuilderError::FatalDBFailure));
            Ok(())
        })
        .await
        .map_err(|_| std::io::Error::other("empty-build regression exceeded its 10-second limit"))?
    }

    #[tokio::test]
    async fn test_make_block_no_ack_txs_in_pool_still() {
        let genesis = test_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // task manger
        let task_manager = TaskManager::new("make_block_no_ack_txs_in_pool_still Task Manager");
        let tmp_dir = TempDir::new().unwrap();
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let txpool = reth_env.init_txn_pool(BaseFeeContainer::default()).unwrap();
        let address = Address::from(U160::from(33));
        let client = LocalNetwork::new_with_empty_id();
        let worker_to_primary = Arc::new(MockWorkerToPrimaryHang {});
        client
            .set_worker_to_primary_local_handler(worker_to_primary)
            .expect("register mock primary handler");
        let temp_dir = TempDir::new().unwrap();
        let store = open_db(temp_dir.path());
        let qw = TestMakeBlockQuorumWaiter::new_test();
        let timeout = Duration::from_secs(5);
        let mut block_provider = Worker::new(
            0,
            Some(qw),
            client,
            store.clone(),
            timeout,
            WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        );
        block_provider.spawn_batch_builder("test batch builder", &task_manager);

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            block_provider.batches_tx(),
            address,
            Duration::from_secs(1),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let _batch_builder = tokio::spawn(batch_builder.run());

        // wait for new batch
        let mut new_batch = None;
        for _ in 0..5 {
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
            // Ensure the block is stored
            if let Some((_, wb)) = store.iter::<NodeBatchesCache>().next() {
                new_batch = Some(wb);
                break;
            }
        }
        let new_batch = new_batch.unwrap();

        // number of transactions in the block
        let block_txs = new_batch.transactions();

        // check max tx for task matches num of transactions in block
        let num_block_txs = block_txs.len();
        assert_eq!(3, num_block_txs);

        // ensure decoded block transaction is transaction1
        let block_tx_bytes = block_txs.first().expect("one tx in block");
        let block_tx =
            recover_raw_transaction(block_tx_bytes).expect("recover raw tx for test").into_inner();

        assert_eq!(block_tx, transaction1);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // transactions should be in pool still since ack wasn't received
        // IT test ensures these transactions are cleared
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);
    }

    /// An observer worker (no quorum waiter) whose forwarder admits nothing must keep its
    /// transactions pending: every seal attempt returns `NotValidator`, the pool is not
    /// drained, and the builder task stays alive to retry on a later build.
    ///
    /// The worker's batch channel is interposed so each seal attempt is observable: the
    /// test receives each sealed batch, runs the real observer `seal` (which runs
    /// `disburse_txns`), and forwards the result on the ack channel. Two full cycles prove
    /// the builder processed the first refusal as non-fatal and kept looping.
    #[tokio::test]
    async fn test_observer_refused_forward_keeps_txs_in_pool() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let client = LocalNetwork::new_with_empty_id();
        let worker_to_primary = Arc::new(MockWorkerToPrimaryHang {});
        client
            .set_worker_to_primary_local_handler(worker_to_primary)
            .expect("register mock primary handler");
        let temp_dir = TempDir::new().unwrap();
        let store = open_db(temp_dir.path());
        let seal_timeout = Duration::from_secs(5);
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        // The real observer worker: no quorum waiter, a forwarder that admits nothing, and
        // a test network handle that discovers no validator RPC endpoints.
        let block_provider = Worker::new(
            0,
            None::<TestMakeBlockQuorumWaiter>,
            client,
            store,
            seal_timeout,
            WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        );

        // build execution block proposer with the interposed channel and a short delay so
        // the retry after a refused seal arrives quickly
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_millis(100),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let batch_builder_task = tokio::spawn(batch_builder.run());

        // plenty of time for each build attempt
        let duration = std::time::Duration::from_secs(5);

        // First attempt: the builder proposes all 3 transactions and the real observer seal
        // refuses the batch because it was not admitted to a forward task.
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("first batch built in time")
            .expect("batch builder sender open");
        assert_eq!(sealed_batch.batch().transactions().len(), 3);
        let res = block_provider.seal(sealed_batch).await;
        assert!(matches!(res, Err(BlockSealError::NotValidator)));
        let _ = ack.send(res);

        // Second attempt: another proposal proves the builder processed the first refusal
        // as non-fatal and kept looping. The same 3 transactions are proposed again.
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("second batch built in time")
            .expect("batch builder sender open");
        assert_eq!(sealed_batch.batch().transactions().len(), 3);
        let res = block_provider.seal(sealed_batch).await;
        assert!(matches!(res, Err(BlockSealError::NotValidator)));
        let _ = ack.send(res);

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // The refused attempts must not evict anything from the pool.
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // `NotValidator` is non-fatal, so the builder task must still be running.
        assert!(!batch_builder_task.is_finished());

        batch_builder_task.abort();
    }

    /// A batch admitted against an endpoint that then proves unreachable is not lost: the
    /// forward task returns every transaction that got no verdict to the worker's own pool
    /// (issue #1145), so the batch builder repackages them on a later build instead of the
    /// transactions ending up in no pool and no table.
    #[tokio::test]
    async fn forwarder_requeues_undelivered_transactions_into_the_pool() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;

        // A port with nothing behind it: the URL resolves (so the batch is admitted) and
        // every send then fails at the connection level, so no transaction gets a verdict.
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);

        // The real forwarder, wired back to the worker's own pool. `AllowPrivate`: the
        // fixture endpoint is a loopback socket, which the shipped default policy would
        // refuse before the requeue under test was ever reached.
        let forwarder = WorkerRpcForwarder::new(
            task_manager.get_spawner(),
            ForwardTargetPolicy::AllowPrivate,
            Some(txpool.clone()),
        );

        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 signed transactions, deliberately NOT submitted to the pool: the batch
        // builder prunes a batch's transactions as mined the moment the batch is admitted,
        // so the requeue is the only path that can put these into the pool
        let raw_txs: Vec<Vec<u8>> = (0..3)
            .map(|_| {
                tx_factory
                    .create_eip1559(
                        chain.clone(),
                        None,
                        gas_price,
                        Some(Address::ZERO),
                        value, // 1 TEL
                        Bytes::new(),
                    )
                    .encoded_2718()
            })
            .collect();
        assert_eq!(txpool.pool_size().pending, 0);

        let key = *BlsKeypair::from_bytes(&[7_u8; 32]).expect("valid bls key bytes").public();
        let rpc = RpcInfo {
            http: format!("http://{addr}").parse().expect("fixture url parses"),
            ws: None,
        };
        assert!(forwarder.forward_txns(raw_txs, vec![key], vec![(key, rpc)]));

        // The forward task gets no verdict for any transaction and requeues all 3 into the
        // pool when it ends; poll until they land, with one overall bounded timeout. Pinned
        // to the heap because the `then` future makes the stream `!Unpin`, which `next` needs.
        let polls = futures_util::stream::repeat(())
            .then(|_| async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                txpool.pool_size().pending
            })
            .skip_while(|pending| futures_util::future::ready(*pending < 3));
        let requeued = timeout(Duration::from_secs(45), Box::pin(polls).next()).await;
        assert_eq!(requeued.expect("undelivered transactions requeued in time"), Some(3));
    }

    /// Refused seals back off geometrically and recover cleanly (issue #1145): the first
    /// refusal waits the configured batch delay, each consecutive refusal doubles it up to
    /// [`REFUSAL_BACKOFF_CAP`] and never past it, the build gate holds while the backoff is
    /// live, and one completed seal clears the state entirely. Time is virtual here, so the
    /// gate assertions are exact rather than racing the wall clock.
    #[tokio::test(start_paused = true)]
    async fn refused_seals_back_off_geometrically_and_recover() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { execution_components, task_manager, .. } = get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, _from_batch_builder) = tokio::sync::mpsc::channel(2);

        let mut batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_millis(100),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        // No refusal yet: the gate is open.
        assert!(!batch_builder.refusal_backoff_holds());

        // The first refusal waits the configured batch delay; each consecutive refusal
        // doubles it, and the gate holds immediately after every one.
        assert_eq!(batch_builder.note_refusal(), Duration::from_millis(100));
        assert!(batch_builder.refusal_backoff_holds());
        assert_eq!(batch_builder.note_refusal(), Duration::from_millis(200));
        assert_eq!(batch_builder.note_refusal(), Duration::from_millis(400));

        // However long the refusal condition persists, the delay never passes the cap.
        (0..12).for_each(|_| {
            let delay = batch_builder.note_refusal();
            assert!(delay <= REFUSAL_BACKOFF_CAP, "backoff {delay:?} passed the cap");
        });
        assert_eq!(batch_builder.note_refusal(), REFUSAL_BACKOFF_CAP);
        assert!(batch_builder.refusal_backoff_holds());

        // One completed seal ends the backoff entirely: state cleared, gate open.
        batch_builder.note_recovery();
        assert!(batch_builder.refusal_backoff.is_none());
        assert!(!batch_builder.refusal_backoff_holds());
    }

    /// Convenience struct for creating test assets.
    struct TestTools {
        /// Factory for creating and signing valid transactions.
        tx_factory: TransactionFactory,
        /// Execution components:
        /// - BlockchainProvider (db)
        /// - TransactionPool
        /// - ChainSpec
        /// - TaskManager (so executor tasks don't drop)
        execution_components: TestExecutionComponents,
        /// Own manager so executor's tasks don't drop.
        task_manager: TaskManager,
    }

    /// Convenience type for holding execution components.
    struct TestExecutionComponents {
        /// The reth execution environment.
        reth_env: RethEnv,
        /// The transaction pool for the block builder.
        txpool: WorkerTxPool,
        /// The chainspec with seeded genesis.
        chain: Arc<RethChainSpec>,
    }

    /// Helper function to create common testing infrastructure.
    fn get_test_tools(path: &Path) -> TestTools {
        get_test_tools_with_base_fee(path, BaseFeeContainer::default())
    }

    /// Like [`get_test_tools`], but the pool sources its pending base fee from the supplied
    /// container (issue #1262).
    fn get_test_tools_with_base_fee(path: &Path, base_fee: BaseFeeContainer) -> TestTools {
        let tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        let genesis = test_genesis().extend_accounts([(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // task manger
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), path, &task_manager, None).unwrap();
        let txpool = reth_env.init_txn_pool(base_fee).unwrap();

        let execution_components = TestExecutionComponents { reth_env, txpool, chain };
        TestTools { tx_factory, execution_components, task_manager }
    }

    /// A pool whose every pending transaction is already carried by a validated peer batch must
    /// seal nothing (issue #1329).
    ///
    /// This is the steady state the deferral window creates on a validator whose whole pool sits
    /// in a peer's in-flight batch: the build gate sees a non-empty pending pool, the build
    /// defers every transaction, and the resulting batch has no transactions. Sending it would
    /// earn a fatal penalty from every peer (`BatchValidationError::EmptyBatch`), so the task
    /// reports [`BuildOutcome::Empty`] and sends nothing to the worker.
    ///
    /// Deterministic: the build task is awaited to completion, so a batch sent to the worker
    /// would already sit in the channel when `try_recv` runs.
    #[tokio::test]
    async fn test_all_deferred_pool_seals_nothing() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_millis(1),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let transaction = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let hash = tx_factory.submit_tx_to_pool(transaction.clone(), txpool.clone()).await;
        assert_eq!(&hash, transaction.hash());
        assert_eq!(txpool.pool_size().pending, 1, "the build gate sees a pending transaction");

        // a peer batch this node validated already carries the pool's only pending transaction
        txpool.record_peer_batch(&[hash]);
        assert!(txpool.is_peer_deferred(&hash));

        // await the build task itself: no sleeps, no polling of the run loop. The timeout is a
        // failure detector, not a wait: a build that seals reaches the ack wait and never
        // resolves, so it must surface as a failed assertion rather than a hung test.
        let outcome =
            timeout(Duration::from_secs(5), batch_builder.spawn_execution_task(TEST_TRIGGER))
                .await
                .expect("the build task finishes without waiting on a seal ack")
                .expect("build task reports its outcome")
                .expect("an all-deferred build is not a fatal error");

        assert_matches!(outcome, BuildOutcome::Empty);
        assert!(
            from_batch_builder.try_recv().is_err(),
            "an empty batch must never reach the worker"
        );
        assert_eq!(
            txpool.pool_size().pending,
            1,
            "the transaction stays pending for a later build"
        );
    }

    /// Test all possible errors from the worker while trying to reach quorum from peers.
    ///
    /// Non-fatal errors return empty vecs of mined transactions.
    /// Fatal error causes shutdown.
    #[tokio::test]
    async fn test_all_possible_error_outcomes() {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_millis(1),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        // expected to be 7 wei for first block
        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let batch_builder_task = tokio::spawn(batch_builder.run());

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);

        // simulate engine to create canonical blocks from empty rounds
        let mut parent = chain.sealed_genesis_header();

        let non_fatal_errors = vec![
            BlockSealError::QuorumRejected,
            BlockSealError::AntiQuorum,
            BlockSealError::Timeout,
            BlockSealError::FailedQuorum,
        ];

        let committee = create_committee_from_state(
            reth_env.epoch_state_from_canonical_tip().expect("epoch state from canonical tip"),
        )
        .await
        .expect("committee from state");
        let gas_accumulator = GasAccumulator::new(1); // 1 worker
        let leader = committee.authorities().first().expect("first authority").id();
        gas_accumulator.rewards_counter().set_committee(committee);
        // specify leader for consensus output
        let mut leader_cert = Certificate::default();
        leader_cert.update_header_author_for_test(leader);
        let headers = vec![leader_cert.header().clone()];
        let subdag = CommittedSubDag::new_with_headers_for_test(headers);
        let output = ConsensusOutput::new_with_subdag(subdag, ConsensusHeaderDigest::default(), 0);

        // receive new blocks and return non-fatal errors
        // non-fatal errors cause the loop to break and wait for txpool updates
        // submitting a new pending transaction is one of the ways this task wakes up
        for (subdag_index, error) in non_fatal_errors.into_iter().enumerate() {
            let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
                .await
                .expect("block builder built another block after canonical update")
                .expect("batch was built");

            // all 3 transactions present
            assert_eq!(sealed_batch.batch().transactions().len(), 3 + subdag_index);

            // submit another tx to pool before acking so the builder's next rebuild
            // (whenever it wakes) is guaranteed to see the new tx - the builder can't
            // rebuild while it still awaits this ack
            tx_factory
                .create_and_submit_eip1559_pool_tx(
                    chain.clone(),
                    gas_price,
                    Address::ZERO,
                    value, // 1 TEL
                    txpool.clone(),
                )
                .await;

            // canonical update to wake up task
            // execute output to trigger canonical update
            let args = BuildArguments::new(reth_env.clone(), output.clone(), parent);
            let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
            // spawn blocking for payload_builder::blocking_recv
            let owned_ga = gas_accumulator.clone();
            let final_header = tokio::task::spawn_blocking(move || {
                execute_consensus_output(
                    args,
                    owned_ga,
                    tn_types::repack_monitor::RepackMonitor::default(),
                    engine_update_tx,
                )
                .expect("output executed")
            })
            .await
            .unwrap();

            // update values for next loop
            parent = final_header;

            // send non-fatal error
            let _ = ack.send(Err(error));
        }

        // wait for next block
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");

        // expect 7 transactions after loop added 4 more
        assert_eq!(sealed_batch.batch().transactions().len(), 7);

        // now send fatal error
        let _ = ack.send(Err(BlockSealError::FatalDBFailure));

        // ensure block builder shuts down from fatal error
        let result = batch_builder_task.await.expect("ack channel delivered result");
        assert!(result.is_err());

        // yield to try and give pool a chance to update
        tokio::task::yield_now().await;

        // transactions should be in pool still since ack was error
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 7);
    }

    /// Test transactions are mined from the pool after post-quorum maintenance completes.
    #[tokio::test]
    async fn test_pool_updates_after_txs_mined() -> eyre::Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools(tmp_dir.path());
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        // build execution block proposer
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_secs(1),
            task_manager.get_spawner(),
            0,
            MIN_PROTOCOL_BASE_FEE,
            0,
        )
        .expect("batch builder");

        // expected to be 7 wei for first block
        let gas_price = reth_env.get_gas_price().unwrap();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if &hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 3);

        // spawn batch_builder once worker is ready
        let _batch_builder_task = tokio::spawn(batch_builder.run());

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);

        // receive proposed block with 3 transactions
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");

        // submit new transaction before sending ack
        let expected_tx_hash = tx_factory
            .create_and_submit_eip1559_pool_tx(
                chain.clone(),
                gas_price,
                Address::ZERO,
                value, // 1 TEL
                txpool.clone(),
            )
            .await;

        // assert first 3 txs in block
        assert_eq!(sealed_batch.batch().transactions().len(), 3);

        // assert all 4 txs in pending pool
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 4);

        // send ack to mine first 3 transactions
        let _ = ack.send(Ok(()));

        // receive next block
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");
        // send ack to mine block
        let _ = ack.send(Ok(()));

        // assert only transaction in block
        assert_eq!(sealed_batch.batch().transactions().len(), 1);

        // confirm 4th transaction hash matches one submitted
        let tx_bytes =
            sealed_batch.batch().transactions().first().expect("block transactions length is one");
        let tx = recover_raw_transaction(tx_bytes).expect("recover raw tx for test");
        assert_eq!(tx.hash(), &expected_tx_hash);

        // Wait for the acknowledged batch's blocking pool update to remove the mined transactions.
        tn_test_utils::wait_until(duration, "mined transactions removed from pool", || async {
            Ok(txpool.pool_size().pending == 0)
        })
        .await?;

        // assert all transactions mined
        let pending_pool_len = txpool.pool_size().pending;
        assert_eq!(pending_pool_len, 0);
        Ok(())
    }

    /// Regression test for issue #1262: after a mined batch the pool update must install the
    /// epoch's base fee from the shared per-worker container, not the fee carried by the stale
    /// canonical tip header (at an epoch boundary, the previous epoch's closing block).
    #[tokio::test]
    async fn test_post_batch_update_installs_epoch_base_fee() {
        const EPOCH_FEE: u64 = MIN_PROTOCOL_BASE_FEE + 1234;
        let tmp_dir = TempDir::new().unwrap();
        let TestTools { mut tx_factory, execution_components, task_manager } =
            get_test_tools_with_base_fee(tmp_dir.path(), BaseFeeContainer::new(EPOCH_FEE));
        let TestExecutionComponents { reth_env, txpool, chain, .. } = execution_components;
        let address = Address::from(U160::from(33));
        let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);

        // build execution block proposer at the epoch fee
        let batch_builder = BatchBuilder::new(
            &reth_env,
            txpool.clone(),
            to_worker,
            address,
            Duration::from_secs(1),
            task_manager.get_spawner(),
            0,
            EPOCH_FEE,
            0,
        )
        .expect("batch builder");

        // The genesis tip's header fee is the chain default. It must differ from EPOCH_FEE, or
        // the assertion below could not distinguish the container from the tip header.
        assert_ne!(reth_env.chainspec().sealed_genesis_block().base_fee_per_gas, Some(EPOCH_FEE));

        // create 3 transactions priced to afford EPOCH_FEE so they are pending at the epoch fee
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            EPOCH_FEE as u128,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            EPOCH_FEE as u128,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            EPOCH_FEE as u128,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        tx_factory.submit_tx_to_pool(transaction1, txpool.clone()).await;
        tx_factory.submit_tx_to_pool(transaction2, txpool.clone()).await;
        tx_factory.submit_tx_to_pool(transaction3, txpool.clone()).await;
        assert_eq!(txpool.pool_size().pending, 3);

        // spawn batch_builder and drive one full mined-batch cycle
        let _batch_builder_task = tokio::spawn(batch_builder.run());

        // plenty of time for block production
        let duration = std::time::Duration::from_secs(5);
        let (sealed_batch, ack) = timeout(duration, from_batch_builder.recv())
            .await
            .expect("block builder's sender didn't drop")
            .expect("batch was built");
        assert_eq!(sealed_batch.batch().transactions().len(), 3);

        // ack mines the batch; the builder then applies the post-batch pool update
        let _ = ack.send(Ok(()));

        // the update is what prunes mined transactions, so an empty pending pool proves it ran
        tn_test_utils::wait_until(duration, "post-batch pool update pruned mined txs", || async {
            Ok(txpool.pool_size().pending == 0)
        })
        .await
        .expect("post-batch pool update applied");

        assert_eq!(
            txpool.block_info().pending_basefee,
            EPOCH_FEE,
            "the post-batch pool update must install the epoch fee, not the stale canonical \
             tip header's",
        );
    }

    /// The `batch built` line and its metrics, captured without a formatting layer.
    mod batch_built_line {
        use super::*;
        use crate::{batch::BuildReason, test_utils::TestPool};
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use std::{fmt, sync::Mutex};
        use tracing::{
            field::{Field, Visit},
            span::{Attributes, Id, Record},
            Event, Metadata, Subscriber,
        };

        /// One captured event: its target and its fields, the message under `message`.
        #[derive(Debug, Default)]
        pub(super) struct Captured {
            /// The event's target.
            target: String,
            /// Field names and values, strings without quotes.
            fields: Vec<(String, String)>,
        }

        impl Captured {
            /// The value of field `name`, if the event carries it.
            pub(super) fn field(&self, name: &str) -> Option<&str> {
                self.fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
            }
        }

        impl Visit for Captured {
            fn record_str(&mut self, field: &Field, value: &str) {
                self.fields.push((field.name().to_string(), value.to_string()));
            }

            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                self.fields.push((field.name().to_string(), format!("{value:?}")));
            }
        }

        /// A subscriber that keeps every event and ignores spans.
        #[derive(Clone, Default)]
        pub(super) struct Capture(pub(super) Arc<Mutex<Vec<Captured>>>);

        impl Subscriber for Capture {
            fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
                Id::from_u64(1)
            }
            fn record(&self, _span: &Id, _values: &Record<'_>) {}
            fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
            fn enter(&self, _span: &Id) {}
            fn exit(&self, _span: &Id) {}
            fn event(&self, event: &Event<'_>) {
                let mut captured = Captured {
                    target: event.metadata().target().to_string(),
                    ..Default::default()
                };
                event.record(&mut captured);
                self.0.lock().unwrap().push(captured);
            }
        }

        /// Sign a zero-value transfer with an explicit gas limit.
        fn transfer(factory: &mut TransactionFactory, gas_limit: u64) -> Vec<u8> {
            factory
                .create_explicit_eip1559(
                    None,
                    None,
                    None,
                    None,
                    Some(gas_limit),
                    Some(Address::ZERO),
                    Some(U256::ZERO),
                    None,
                    None,
                )
                .encoded_2718()
        }

        /// A drained build and a gas-full build each emit one `batch built` line with every
        /// contract field, and feed `pool_age_seconds` and `builds_total{reason}`.
        #[test]
        fn batch_built_line_carries_reason_pool_age_and_wake() {
            let mut sender = TransactionFactory::new();
            let mut other = TransactionFactory::new_random();
            let full_gas = tn_types::max_batch_gas(0);
            let drained = vec![transfer(&mut sender, 21_000), transfer(&mut sender, 21_000)];
            let gas_full = vec![transfer(&mut sender, full_gas), transfer(&mut other, 21_000)];
            let cases = [
                (drained, WakeSource::Canonical, "drained", "2", 42_000.to_string()),
                (gas_full, WakeSource::Ack, "gas_full", "1", full_gas.to_string()),
            ];

            let capture = Capture::default();
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let mut digests = Vec::new();
            ::metrics::with_local_recorder(&recorder, || {
                tracing::subscriber::with_default(capture.clone(), || {
                    let metrics = BatchBuilderMetrics::new_for_worker(3);
                    let build_counters = BuildReasonCounters::new_for_worker(3);
                    for (txs, wake, ..) in &cases {
                        let mut pool = TestPool::new(txs);
                        let args = BatchBuilderArgs {
                            pool: &mut pool,
                            beneficiary: Address::ZERO,
                            epoch: 0,
                        };
                        let output = build_batch(args, 3, MIN_PROTOCOL_BASE_FEE);
                        let batch = output.batch.seal_slow();
                        digests.push(batch.digest().to_string());
                        let trigger =
                            BuildTrigger { wake: *wake, pending_before: 2, queued_before: 5 };
                        report_batch_built(
                            &metrics,
                            &build_counters,
                            3,
                            &batch,
                            &output.stats,
                            trigger,
                        );
                    }
                });
            });

            let events = capture.0.lock().unwrap();
            let lines: Vec<_> = events
                .iter()
                .filter(|event| event.field("message") == Some("batch built"))
                .collect();
            assert_eq!(lines.len(), cases.len(), "one line per sealed batch: {events:?}");
            for ((line, digest), (_, wake, reason, num_txs, gas)) in
                lines.iter().zip(&digests).zip(&cases)
            {
                assert_eq!(line.target, "consensus::metrics");
                assert_eq!(line.field("digest"), Some(digest.as_str()));
                assert_eq!(line.field("worker_id"), Some("3"));
                assert_eq!(line.field("num_txs"), Some(*num_txs));
                assert_eq!(line.field("gas"), Some(gas.as_str()));
                assert_eq!(line.field("reason"), Some(*reason));
                assert_eq!(line.field("wake"), Some(wake.label()));
                assert_eq!(line.field("pending_before"), Some("2"));
                assert_eq!(line.field("queued_before"), Some("5"));
                for field in ["bytes", "pool_age_p50_ms", "pool_age_max_ms", "build_ms"] {
                    let value = line.field(field).unwrap_or_else(|| panic!("{field} missing"));
                    assert!(value.parse::<u64>().is_ok(), "{field}={value} is not a number");
                }
            }

            let snapshot = snapshotter.snapshot().into_vec();
            let builds = |reason: BuildReason| {
                snapshot.iter().find_map(|(key, _, _, value)| {
                    (key.key().name() == "tn_batch_builder.builds_total"
                        && key
                            .key()
                            .labels()
                            .any(|l| l.key() == "reason" && l.value() == reason.label()))
                    .then_some(value)
                })
            };
            assert!(matches!(builds(BuildReason::Drained), Some(DebugValue::Counter(1))));
            assert!(matches!(builds(BuildReason::GasFull), Some(DebugValue::Counter(1))));
            assert!(matches!(builds(BuildReason::BytesFull), Some(DebugValue::Counter(0))));
            let pool_age = snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == "tn_batch_builder.pool_age_seconds")
                .map(|(.., value)| value);
            assert!(
                matches!(pool_age, Some(DebugValue::Histogram(samples)) if samples.len() == 2),
                "one pool-age sample per built batch: {pool_age:?}"
            );
        }
    }

    /// The arrival wake: an idle builder seals within the fill delay of a transaction arriving,
    /// and an arrival never starts a second build while one is in flight.
    mod arrival_wake {
        use super::{batch_built_line::Capture, *};
        use std::collections::VecDeque;
        use tn_types::{
            repack_monitor::RepackMonitor, CertifiedBatch, EpochSeedChainValue, ReputationScores,
        };
        use tokio::sync::mpsc::error::TryRecvError;

        /// A batch delay far beyond every assertion window: only an arrival explains a build.
        const LONG_TICK: Duration = Duration::from_secs(60);

        /// A batch delay short enough to wait for, long against the fill delay.
        const SHORT_TICK: Duration = Duration::from_secs(1);

        /// A running builder, parked on an empty pool, and its test tools.
        struct IdleBuilder {
            /// Signs the test transactions.
            tx_factory: TransactionFactory,
            /// The builder's pool.
            txpool: WorkerTxPool,
            /// The test chain.
            chain: Arc<RethChainSpec>,
            /// Batches the builder hands to the worker, with their ack senders.
            from_batch_builder: tokio::sync::mpsc::Receiver<(
                SealedBatch,
                oneshot::Sender<Result<(), BlockSealError>>,
            )>,
            /// The run loop.
            builder: tokio::task::JoinHandle<BatchBuilderResult<()>>,
            /// Kept alive for the pool's provider.
            _reth_env: RethEnv,
            /// Kept alive so the builder's spawned build tasks are not cancelled.
            _task_manager: TaskManager,
        }

        /// Start a builder with batch delay `tick` and let it park with an empty pool.
        async fn idle_builder(path: &Path, tick: Duration) -> IdleBuilder {
            let TestTools { tx_factory, execution_components, task_manager } = get_test_tools(path);
            let TestExecutionComponents { reth_env, txpool, chain } = execution_components;
            let (to_worker, from_batch_builder) = tokio::sync::mpsc::channel(2);
            let batch_builder = BatchBuilder::new(
                &reth_env,
                txpool.clone(),
                to_worker,
                Address::ZERO,
                tick,
                task_manager.get_spawner(),
                0,
                MIN_PROTOCOL_BASE_FEE,
                0,
            )
            .expect("batch builder");
            let builder = tokio::spawn(batch_builder.run());
            // the first pass sees an empty pool and pushes the tick out by a whole period
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(txpool.pool_size().pending, 0);
            IdleBuilder {
                tx_factory,
                txpool,
                chain,
                from_batch_builder,
                builder,
                _reth_env: reth_env,
                _task_manager: task_manager,
            }
        }

        /// Submit one 10-wei transfer from `tx_factory` and return its hash.
        async fn submit(
            tx_factory: &mut TransactionFactory,
            chain: &Arc<RethChainSpec>,
            txpool: &WorkerTxPool,
        ) -> TxHash {
            tx_factory
                .create_and_submit_eip1559_pool_tx(
                    chain.clone(),
                    u128::from(MIN_PROTOCOL_BASE_FEE),
                    Address::ZERO,
                    U256::from(10),
                    txpool.clone(),
                )
                .await
        }

        /// The hashes of a sealed batch's transactions.
        fn hashes(batch: &SealedBatch) -> Vec<TxHash> {
            batch
                .batch()
                .transactions()
                .iter()
                .map(|tx| *recover_raw_transaction(tx).expect("batch tx decodes").hash())
                .collect()
        }

        /// The `wake` field of every `batch built` line captured so far.
        fn wakes(capture: &Capture) -> Vec<String> {
            capture
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.field("message") == Some("batch built"))
                .filter_map(|event| event.field("wake").map(str::to_string))
                .collect()
        }

        /// One transaction reaching an idle builder is sealed once the fill delay passes, long
        /// before the batch-delay tick, and its line reports the arrival wake.
        #[tokio::test]
        async fn idle_builder_seals_an_arrival_within_the_fill_delay() {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new().unwrap();
            // the underscore bindings keep the reth env and task manager alive to the end
            let IdleBuilder {
                mut tx_factory,
                txpool,
                chain,
                mut from_batch_builder,
                builder,
                _reth_env,
                _task_manager,
            } = idle_builder(tmp_dir.path(), LONG_TICK).await;

            let submitted = Instant::now();
            let hash = submit(&mut tx_factory, &chain, &txpool).await;
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("the arrival is sealed long before the batch-delay tick")
                .expect("worker channel open");
            let waited = submitted.elapsed();

            assert!(waited >= BATCH_WAKE_FILL_DELAY, "built before the fill delay: {waited:?}");
            assert_eq!(hashes(&batch), vec![hash]);
            assert_eq!(wakes(&capture), ["arrival"]);

            ack.send(Err(BlockSealError::FatalDBFailure)).unwrap();
            assert_matches!(builder.await.unwrap(), Err(BatchBuilderError::FatalDBFailure));
        }

        /// A transaction arriving while a batch awaits its quorum ack starts no second build; it
        /// goes into the batch built after the ack, whose line reports the ack wake.
        #[tokio::test]
        async fn arrival_during_an_in_flight_build_starts_no_second_build() {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new().unwrap();
            // the underscore bindings keep the reth env and task manager alive to the end
            let IdleBuilder {
                mut tx_factory,
                txpool,
                chain,
                mut from_batch_builder,
                builder,
                _reth_env,
                _task_manager,
            } = idle_builder(tmp_dir.path(), LONG_TICK).await;

            let first = submit(&mut tx_factory, &chain, &txpool).await;
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("first batch sealed")
                .expect("worker channel open");
            assert_eq!(hashes(&batch), vec![first]);

            // several fill delays pass with the first batch unacknowledged
            let second = submit(&mut tx_factory, &chain, &txpool).await;
            tokio::time::sleep(BATCH_WAKE_FILL_DELAY * 5).await;
            assert_matches!(from_batch_builder.try_recv(), Err(TryRecvError::Empty));
            assert_eq!(txpool.pool_size().pending, 2, "nothing mined before the ack");

            ack.send(Ok(())).unwrap();
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("second batch sealed after the ack")
                .expect("worker channel open");
            assert_eq!(hashes(&batch), vec![second]);
            assert_eq!(wakes(&capture), ["arrival", "ack"]);

            ack.send(Err(BlockSealError::FatalDBFailure)).unwrap();
            assert_matches!(builder.await.unwrap(), Err(BatchBuilderError::FatalDBFailure));
        }

        /// After a failed seal an arrival does not rebuild one fill delay later: the retry waits
        /// for the batch-delay tick, as it did before the arrival wake.
        #[tokio::test]
        async fn arrival_after_a_failed_seal_waits_for_the_tick() {
            assert_retry_waits_for_the_tick(BlockSealError::QuorumRejected).await;
        }

        /// A failed seal holds the fill delay only while its transactions can still be retried.
        /// Once they leave the pool and a canonical update wakes the idle builder, the next
        /// arrival arms the fill delay again instead of waiting for the batch-delay tick.
        #[tokio::test]
        async fn arrival_after_a_failed_seal_and_an_emptied_pool_arms_the_fill_delay() {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new().unwrap();
            // the underscore binding keeps the task manager alive to the end
            let IdleBuilder {
                mut tx_factory,
                mut txpool,
                chain,
                mut from_batch_builder,
                builder,
                _reth_env: reth_env,
                _task_manager,
            } = idle_builder(tmp_dir.path(), LONG_TICK).await;

            let first = submit(&mut tx_factory, &chain, &txpool).await;
            let (failed, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("first batch sealed")
                .expect("worker channel open");
            assert_eq!(hashes(&failed), vec![first]);
            ack.send(Err(BlockSealError::QuorumRejected)).unwrap();
            // the run loop takes the failure before the pool changes under it
            tokio::time::sleep(BATCH_WAKE_FILL_DELAY).await;

            // the failed batch's transaction leaves the pool, then a block that mines it wakes
            // the idle builder with nothing pending
            txpool.remove_unsupported_txs(vec![first]);
            assert_eq!(txpool.pool_size().pending, 0);
            let output = ConsensusOutput::new(
                CommittedSubDag::new(
                    vec![Certificate::default()],
                    Certificate::default(),
                    0,
                    ReputationScores::default(),
                    None,
                    EpochSeedChainValue::genesis_placeholder(),
                ),
                ConsensusHeaderDigest::default(),
                0,
                false,
                VecDeque::from([failed.digest()]),
                vec![CertifiedBatch {
                    address: Address::ZERO,
                    batches: vec![failed.batch().clone()],
                }],
            );
            let args = BuildArguments::new(reth_env.clone(), output, chain.sealed_genesis_header());
            let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
            let block_1 = tokio::task::spawn_blocking(move || {
                execute_consensus_output(
                    args,
                    GasAccumulator::default(),
                    RepackMonitor::default(),
                    engine_update_tx,
                )
                .expect("output executed")
            })
            .await
            .unwrap();
            assert_eq!(block_1.number, 1);
            // the pool has applied the block, so the sender's next nonce is pending
            tn_test_utils::wait_until(Duration::from_secs(5), "pool applies block 1", || async {
                Ok(txpool.block_info().last_seen_block_number == 1)
            })
            .await
            .unwrap();

            let submitted = Instant::now();
            let second = submit(&mut tx_factory, &chain, &txpool).await;
            assert_eq!(txpool.pool_size().pending, 1, "the second transaction is pending");
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("the arrival is sealed long before the batch-delay tick")
                .expect("worker channel open");
            let waited = submitted.elapsed();

            assert!(waited >= BATCH_WAKE_FILL_DELAY, "built before the fill delay: {waited:?}");
            assert_eq!(hashes(&batch), vec![second]);
            assert_eq!(wakes(&capture), ["arrival", "arrival"]);

            ack.send(Err(BlockSealError::FatalDBFailure)).unwrap();
            assert_matches!(builder.await.unwrap(), Err(BatchBuilderError::FatalDBFailure));
        }

        /// An empty build that leaves nothing pending, as when selection removed every
        /// candidate itself, sets the hold with nothing to retry. The deferred pass right after
        /// it clears the hold, so the next arrival is sealed after the fill delay instead of
        /// waiting for the batch-delay tick.
        #[tokio::test]
        async fn arrival_after_an_empty_build_with_nothing_pending_arms_the_fill_delay() {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new().unwrap();
            let TestTools { mut tx_factory, execution_components, task_manager } =
                get_test_tools(tmp_dir.path());
            let TestExecutionComponents { reth_env, txpool, chain } = execution_components;
            let (to_worker, mut from_batch_builder) = tokio::sync::mpsc::channel(2);
            let mut batch_builder = BatchBuilder::new(
                &reth_env,
                txpool.clone(),
                to_worker,
                Address::ZERO,
                LONG_TICK,
                task_manager.get_spawner(),
                0,
                MIN_PROTOCOL_BASE_FEE,
                0,
            )
            .expect("batch builder");
            // the run loop's first completion is an empty build, with the pool empty after it
            let (result, done) = oneshot::channel();
            result.send(Ok(BuildOutcome::Empty)).unwrap();
            batch_builder.pending_task = Some(done);
            let builder = tokio::spawn(batch_builder.run());
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(txpool.pool_size().pending, 0);

            let submitted = Instant::now();
            let hash = submit(&mut tx_factory, &chain, &txpool).await;
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("the arrival is sealed long before the batch-delay tick")
                .expect("worker channel open");
            let waited = submitted.elapsed();

            assert!(waited >= BATCH_WAKE_FILL_DELAY, "built before the fill delay: {waited:?}");
            assert_eq!(hashes(&batch), vec![hash]);
            assert_eq!(wakes(&capture), ["arrival"]);

            ack.send(Err(BlockSealError::FatalDBFailure)).unwrap();
            assert_matches!(builder.await.unwrap(), Err(BatchBuilderError::FatalDBFailure));
        }

        /// An arrival during a live refusal backoff does not build before the backoff ends
        /// (issue #1145); the first tick after it does.
        #[tokio::test]
        async fn arrival_during_a_refusal_backoff_does_not_build() {
            assert_retry_waits_for_the_tick(BlockSealError::NotValidator).await;
        }

        /// Seal one arrival, fail its seal with `error`, submit a second transaction, and check
        /// that nothing is built until the batch-delay tick, which then carries both.
        ///
        /// The first refusal backs off for one batch delay, so both outcomes rebuild on the tick
        /// that follows the failure by [`SHORT_TICK`].
        async fn assert_retry_waits_for_the_tick(error: BlockSealError) {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new().unwrap();
            // the underscore bindings keep the reth env and task manager alive to the end
            let IdleBuilder {
                mut tx_factory,
                txpool,
                chain,
                mut from_batch_builder,
                builder,
                _reth_env,
                _task_manager,
            } = idle_builder(tmp_dir.path(), SHORT_TICK).await;

            let first = submit(&mut tx_factory, &chain, &txpool).await;
            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("first batch sealed")
                .expect("worker channel open");
            assert_eq!(hashes(&batch), vec![first]);
            let failed = Instant::now();
            ack.send(Err(error)).unwrap();

            // several fill delays pass after a new arrival without a build
            let second = submit(&mut tx_factory, &chain, &txpool).await;
            tokio::time::sleep(BATCH_WAKE_FILL_DELAY * 5).await;
            assert_matches!(from_batch_builder.try_recv(), Err(TryRecvError::Empty));

            let (batch, ack) = timeout(Duration::from_secs(5), from_batch_builder.recv())
                .await
                .expect("the tick rebuilds")
                .expect("worker channel open");
            let waited = failed.elapsed();
            assert!(waited >= SHORT_TICK, "rebuilt {waited:?} after the failure, before the tick");
            assert_eq!(hashes(&batch), vec![first, second]);
            assert_eq!(wakes(&capture), ["arrival", "tick"]);

            ack.send(Err(BlockSealError::FatalDBFailure)).unwrap();
            assert_matches!(builder.await.unwrap(), Err(BatchBuilderError::FatalDBFailure));
        }
    }

    // ---- bench6 sealed-keep ----

    /// Switch `sealed_tx_keep` on the builder side (issue #1807): a quorum ack marks the batch's
    /// transactions in the pool instead of pruning them, and selection skips them until their
    /// batch executes. Case ids (S1-S3) are those of the I-2 sealed-keep test cases.
    mod sealed_keep {
        use super::{batch_built_line::Capture, *};
        use std::collections::VecDeque;
        use tn_types::{
            repack_monitor::RepackMonitor, CertifiedBatch, EpochSeedChainValue, ReputationScores,
            TransactionSigned,
        };
        use tokio::sync::mpsc::{error::TryRecvError, Receiver};

        /// The batch delay of most tests: a failed seal retries on it, and a wait of two of them
        /// with nothing sealed shows the builder offers nothing.
        const TICK: Duration = Duration::from_millis(300);

        /// A batch delay far beyond every assertion window: only an arrival explains a build.
        const LONG_TICK: Duration = Duration::from_secs(60);

        /// The bound on every wait for a batch, an ack's effect or a block.
        const WAIT: Duration = Duration::from_secs(10);

        /// What the builder hands the worker: each sealed batch with its ack sender.
        type ToWorker = Receiver<(SealedBatch, oneshot::Sender<Result<(), BlockSealError>>)>;

        /// Three funded senders, a worker pool, and the chain its batches execute on.
        struct Bench {
            /// The first sender (the default factory).
            a: TransactionFactory,
            /// The second sender.
            b: TransactionFactory,
            /// A third sender whose batch holds the builder busy while a test arranges the pool.
            holder: TransactionFactory,
            /// The worker pool every builder here builds from.
            txpool: WorkerTxPool,
            /// The test chain.
            chain: Arc<RethChainSpec>,
            /// The execution environment the blocks are executed in.
            reth_env: RethEnv,
            /// The last executed header: the parent of the next block.
            tip: SealedHeader,
            /// Holds the reth environment's tasks and the builders' build tasks.
            task_manager: TaskManager,
        }

        impl Bench {
            /// A fresh chain at genesis with every sender funded.
            fn new(path: &Path) -> eyre::Result<Self> {
                let a = TransactionFactory::new();
                let b = TransactionFactory::new_random();
                let holder = TransactionFactory::new_random();
                let funded = || {
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000").expect("balance parses"),
                    )
                };
                let genesis = test_genesis().extend_accounts([
                    (a.address(), funded()),
                    (b.address(), funded()),
                    (holder.address(), funded()),
                ]);
                let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
                let task_manager = TaskManager::new("sealed-keep test");
                let reth_env =
                    RethEnv::new_for_temp_chain(chain.clone(), path, &task_manager, None)?;
                let txpool = reth_env.init_txn_pool(BaseFeeContainer::default())?;
                let tip = chain.sealed_genesis_header();
                Ok(Self { a, b, holder, txpool, chain, reth_env, tip, task_manager })
            }

            /// A builder over the pool with the switch at `keep`, feeding a worker that seals by
            /// quorum when `quorum` is true, and the receiver of what it seals.
            fn builder(
                &self,
                keep: bool,
                quorum: bool,
                tick: Duration,
            ) -> eyre::Result<(BatchBuilder, ToWorker)> {
                let (to_worker, from_builder) = tokio::sync::mpsc::channel(2);
                let builder = BatchBuilder::new(
                    &self.reth_env,
                    self.txpool.clone(),
                    to_worker,
                    Address::ZERO,
                    tick,
                    self.task_manager.get_spawner(),
                    0,
                    MIN_PROTOCOL_BASE_FEE,
                    0,
                )?
                .with_sealed_tx_keep(keep)
                .with_seals_via_quorum(quorum);
                Ok((builder, from_builder))
            }

            /// Add `txs` to the pool as a local client would, in order.
            async fn submit(&self, txs: &[TransactionSigned]) {
                for tx in txs {
                    self.a.submit_tx_to_pool(tx.clone(), self.txpool.clone()).await;
                }
            }

            /// Execute `batches` as one consensus output on top of the tip, one block per
            /// batch, and wait until the pool has applied the last block.
            async fn execute(&mut self, batches: &[&SealedBatch]) -> eyre::Result<SealedHeader> {
                let output = ConsensusOutput::new(
                    CommittedSubDag::new(
                        vec![Certificate::default()],
                        Certificate::default(),
                        0,
                        ReputationScores::default(),
                        None,
                        EpochSeedChainValue::genesis_placeholder(),
                    ),
                    ConsensusHeaderDigest::default(),
                    0,
                    false,
                    batches.iter().map(|batch| batch.digest()).collect::<VecDeque<_>>(),
                    vec![CertifiedBatch {
                        address: Address::ZERO,
                        batches: batches.iter().map(|batch| batch.batch().clone()).collect(),
                    }],
                );
                let args = BuildArguments::new(self.reth_env.clone(), output, self.tip.clone());
                let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
                let tip = tokio::task::spawn_blocking(move || {
                    execute_consensus_output(
                        args,
                        GasAccumulator::default(),
                        RepackMonitor::default(),
                        engine_update_tx,
                    )
                })
                .await??;
                let number = tip.number;
                let txpool = self.txpool.clone();
                tn_test_utils::wait_until(WAIT, "the pool applies the block", || {
                    let txpool = txpool.clone();
                    async move { Ok(txpool.block_info().last_seen_block_number == number) }
                })
                .await?;
                self.tip = tip.clone();
                Ok(tip)
            }

            /// The canonical nonce of `address`.
            fn nonce(&self, address: Address) -> eyre::Result<u64> {
                Ok(self.reth_env.retrieve_account(&address)?.map_or(0, |account| account.nonce))
            }

            /// Wait until every hash in `txs` is marked as sealed (`marked`) or not.
            async fn wait_marked(
                &self,
                txs: &[TransactionSigned],
                marked: bool,
            ) -> eyre::Result<()> {
                let hashes = tx_hashes(txs);
                let txpool = self.txpool.clone();
                tn_test_utils::wait_until(WAIT, "sealed marks settle", || {
                    let (txpool, hashes) = (txpool.clone(), hashes.clone());
                    async move {
                        Ok(hashes.iter().all(|hash| txpool.sealed_txs().is_marked(hash) == marked))
                    }
                })
                .await
            }

            /// Wait until no transaction of `txs` is pooled any more.
            async fn wait_gone(&self, txs: &[TransactionSigned]) -> eyre::Result<()> {
                let hashes = tx_hashes(txs);
                let txpool = self.txpool.clone();
                tn_test_utils::wait_until(WAIT, "transactions leave the pool", || {
                    let (txpool, hashes) = (txpool.clone(), hashes.clone());
                    async move { Ok(hashes.iter().all(|hash| txpool.get(hash).is_none())) }
                })
                .await
            }

            /// Wait until nothing is marked. The pool removes a mined transaction before it
            /// clears the marks of the same update, so a test that saw a transaction leave the
            /// pool waits for its mark too.
            async fn wait_no_marks(&self) -> eyre::Result<()> {
                let txpool = self.txpool.clone();
                tn_test_utils::wait_until(WAIT, "every sealed mark clears", || {
                    let txpool = txpool.clone();
                    async move { Ok(txpool.sealed_txs().is_empty()) }
                })
                .await
            }

            /// Assert every transaction of `txs` is still pooled.
            fn assert_pooled(&self, txs: &[TransactionSigned]) {
                for hash in tx_hashes(txs) {
                    assert!(self.txpool.get(&hash).is_some(), "{hash} must stay visible by hash");
                }
            }
        }

        /// `count` sequential 10-wei transfers from `factory` at the minimum base fee.
        fn transfers(
            factory: &mut TransactionFactory,
            chain: &Arc<RethChainSpec>,
            count: usize,
        ) -> Vec<TransactionSigned> {
            (0..count)
                .map(|_| {
                    factory.create_eip1559(
                        chain.clone(),
                        None,
                        u128::from(MIN_PROTOCOL_BASE_FEE),
                        Some(Address::ZERO),
                        U256::from(10),
                        Bytes::new(),
                    )
                })
                .collect()
        }

        /// The hashes of `txs`, in order.
        fn tx_hashes(txs: &[TransactionSigned]) -> Vec<TxHash> {
            txs.iter().map(|tx| *tx.hash()).collect()
        }

        /// The hashes of a sealed batch's transactions, in batch order.
        fn hashes(batch: &SealedBatch) -> Vec<TxHash> {
            batch
                .batch()
                .transactions()
                .iter()
                .map(|tx| *recover_raw_transaction(tx).expect("batch tx decodes").hash())
                .collect()
        }

        /// `hashes` sorted, for batches that mix senders.
        fn sorted(mut hashes: Vec<TxHash>) -> Vec<TxHash> {
            hashes.sort();
            hashes
        }

        /// The next batch the builder hands the worker.
        async fn recv(
            to_worker: &mut ToWorker,
        ) -> eyre::Result<(SealedBatch, oneshot::Sender<Result<(), BlockSealError>>)> {
            timeout(WAIT, to_worker.recv())
                .await?
                .ok_or_else(|| eyre::eyre!("the builder dropped the worker channel"))
        }

        /// Receive and ack `Ok` every batch the builder seals until together they carry
        /// `expected`, in order, and return them. A burst can straddle the fill delay and land
        /// in more than one batch.
        async fn recv_covering(
            to_worker: &mut ToWorker,
            expected: &[TxHash],
        ) -> eyre::Result<Vec<SealedBatch>> {
            let mut batches = Vec::new();
            let mut sealed = Vec::new();
            while sealed.len() < expected.len() {
                let (batch, batch_ack) = recv(to_worker).await?;
                sealed.extend(hashes(&batch));
                ack(batch_ack, Ok(()))?;
                batches.push(batch);
            }
            assert_eq!(sealed, expected);
            Ok(batches)
        }

        /// Acknowledge a batch with `result`.
        fn ack(
            ack: oneshot::Sender<Result<(), BlockSealError>>,
            result: Result<(), BlockSealError>,
        ) -> eyre::Result<()> {
            ack.send(result).map_err(|_| eyre::eyre!("the build task dropped its ack receiver"))
        }

        /// Assert the builder seals nothing for `wait`.
        async fn assert_no_batch(to_worker: &mut ToWorker, wait: Duration) {
            tokio::time::sleep(wait).await;
            assert_matches!(to_worker.try_recv(), Err(TryRecvError::Empty));
        }

        /// True if a pool add answered `already imported`, the answer to a resend of pooled
        /// bytes.
        fn already_known<T>(result: &Result<T, tn_reth::PoolError>) -> bool {
            matches!(result, Err(error) if error.kind.to_string() == "already imported")
        }

        /// I-2 S1: a sender's later batch executes before its earlier one, so execution skips
        /// it (`nonce too high`). The canonical update for that block clears its marks, the
        /// builder re-seals the same transactions (same digest), and every transaction executes
        /// exactly once, with no client resend. Each transaction stays visible by hash until it
        /// executes.
        #[tokio::test]
        async fn s1_a_batch_executed_out_of_order_is_resealed_and_lands_once() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let first = transfers(&mut bench.a, &bench.chain, 4);
            let second = transfers(&mut bench.a, &bench.chain, 4);
            let (builder, mut to_worker) = bench.builder(true, true, TICK)?;
            assert!(builder.keeps_sealed());
            bench.submit(&first).await;
            let task = tokio::spawn(builder.run());

            // B1 = a0..a3; a4..a7 arrive while it waits for its quorum
            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&first));
            bench.submit(&second).await;
            ack(b1_ack, Ok(()))?;

            // the ack marked B1 before the next selection, which skips it and seals a4..a7
            let (b2, b2_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b2), tx_hashes(&second));
            assert!(tx_hashes(&first).iter().all(|hash| bench.txpool.is_sealed(hash)));
            ack(b2_ack, Ok(()))?;
            bench.wait_marked(&second, true).await?;
            bench.assert_pooled(&[first.clone(), second.clone()].concat());
            assert_no_batch(&mut to_worker, TICK * 2).await;

            // B2 executes first and skips everything; its marks clear and a4..a7 are re-sealed
            let block_1 = bench.execute(&[&b2]).await?;
            assert_eq!(block_1.gas_used, 0, "a4..a7 are nonce-gapped behind B1");
            let (b3, b3_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b3), tx_hashes(&second));
            assert_eq!(b3.digest(), b2.digest(), "the re-seal is the same batch");
            bench.assert_pooled(&[first.clone(), second.clone()].concat());
            // the mark is accepted: the earlier execution of this digest predates B3's selection
            ack(b3_ack, Ok(()))?;
            bench.wait_marked(&second, true).await?;

            // B1 executes: a0..a3 land, a4..a7 wait in B3 and are not sealed a third time
            let block_2 = bench.execute(&[&b1]).await?;
            assert!(block_2.gas_used > 0);
            bench.wait_gone(&first).await?;
            bench.assert_pooled(&second);
            assert_eq!(bench.nonce(bench.a.address())?, 4);
            assert_no_batch(&mut to_worker, TICK * 2).await;

            // a8..a11 arrive while a4..a7 are still sealed: they are pending, not queued behind a
            // gap, and sealed at once (step 5 of the case)
            let third = transfers(&mut bench.a, &bench.chain, 4);
            bench.submit(&third).await;
            assert_eq!(bench.txpool.pool_size().queued, 0);
            let b4 = recv_covering(&mut to_worker, &tx_hashes(&third)).await?;
            bench.wait_marked(&third, true).await?;

            // B3 executes: a4..a7 land, each once; then a8..a11
            let block_3 = bench.execute(&[&b3]).await?;
            assert_eq!(block_3.gas_used, block_2.gas_used, "four transfers executed, once each");
            bench.execute(&b4.iter().collect::<Vec<_>>()).await?;
            bench.wait_gone(&[second, third].concat()).await?;
            bench.wait_no_marks().await?;
            assert_eq!(bench.nonce(bench.a.address())?, 12);
            assert_no_batch(&mut to_worker, TICK * 2).await;
            task.abort();
            Ok(())
        }

        /// I-2 S1 pin (switch off): both acks prune their batch and advance the sender's pool
        /// nonce, so when the later batch executes first and skips everything, nothing is left
        /// to re-seal: a4..a7 are in no pool and would land only through a client resend.
        #[tokio::test]
        async fn s1_pin_switch_off_out_of_order_execution_loses_the_skipped_batch(
        ) -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let first = transfers(&mut bench.a, &bench.chain, 4);
            let second = transfers(&mut bench.a, &bench.chain, 4);
            let (builder, mut to_worker) = bench.builder(false, true, TICK)?;
            bench.submit(&first).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&first));
            bench.submit(&second).await;
            ack(b1_ack, Ok(()))?;
            let (b2, b2_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b2), tx_hashes(&second));
            ack(b2_ack, Ok(()))?;
            bench.wait_gone(&[first.clone(), second.clone()].concat()).await?;

            let block_1 = bench.execute(&[&b2]).await?;
            assert_eq!(block_1.gas_used, 0, "a4..a7 are nonce-gapped behind B1");
            bench.execute(&[&b1]).await?;
            assert_eq!(bench.nonce(bench.a.address())?, 4);
            for hash in tx_hashes(&second) {
                assert!(bench.txpool.get(&hash).is_none(), "the switch-off node forgot {hash}");
            }
            assert_no_batch(&mut to_worker, TICK * 2).await;
            assert!(bench.txpool.sealed_txs().is_empty(), "the switch-off path never marks");
            task.abort();
            Ok(())
        }

        /// The race the seal ticket exists for: a batch executes before the builder handles its
        /// quorum ack. Whichever comes first, the refused mark or the clear of the executed
        /// digest, the transaction the block skipped ends up pooled and unmarked and is sealed
        /// again, not held marked for good; then everything lands once.
        #[tokio::test]
        async fn a_batch_executed_before_its_ack_is_sealed_again() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let first = transfers(&mut bench.a, &bench.chain, 1);
            let second = transfers(&mut bench.a, &bench.chain, 1);
            let (builder, mut to_worker) = bench.builder(true, true, TICK)?;
            bench.submit(&first).await;
            let task = tokio::spawn(builder.run());

            // B0 = a0, sealed and marked
            let (b0, b0_ack) = recv(&mut to_worker).await?;
            ack(b0_ack, Ok(()))?;
            bench.wait_marked(&first, true).await?;

            // B1 = a1 executes before its ack and skips a1, nonce-gapped behind a0
            bench.submit(&second).await;
            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&second));
            let block_1 = bench.execute(&[&b1]).await?;
            assert_eq!(block_1.gas_used, 0);
            ack(b1_ack, Ok(()))?;

            let (again, again_ack) = recv(&mut to_worker).await?;
            assert_eq!(again.digest(), b1.digest(), "a1 is sealed again, in the same batch");
            assert!(!bench.txpool.sealed_txs().is_marked(second[0].hash()));
            bench.assert_pooled(&second);
            ack(again_ack, Ok(()))?;
            bench.wait_marked(&second, true).await?;

            bench.execute(&[&b0]).await?;
            bench.execute(&[&again]).await?;
            bench.wait_gone(&[first, second].concat()).await?;
            bench.wait_no_marks().await?;
            assert_eq!(bench.nonce(bench.a.address())?, 2);
            task.abort();
            Ok(())
        }

        /// I-2 S2 (switch on): while a sealed batch waits for commit, a client resend of its
        /// bytes answers `already imported`, the builder offers none of it, and the batch
        /// executes once.
        #[tokio::test]
        async fn s2_resend_of_a_sealed_batch_is_already_known() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let txs = transfers(&mut bench.a, &bench.chain, 4);
            let (builder, mut to_worker) = bench.builder(true, true, TICK)?;
            bench.submit(&txs).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&txs));
            ack(b1_ack, Ok(()))?;
            bench.wait_marked(&txs, true).await?;

            for tx in &txs {
                let resend = bench.txpool.add_raw_transaction_external(tx.clone()).await;
                assert!(already_known(&resend), "a resend of sealed bytes is known: {resend:?}");
            }
            bench.assert_pooled(&txs);
            assert_no_batch(&mut to_worker, TICK * 2).await;

            let block = bench.execute(&[&b1]).await?;
            assert!(block.gas_used > 0);
            bench.wait_gone(&txs).await?;
            bench.wait_no_marks().await?;
            assert_eq!(bench.nonce(bench.a.address())?, 4);
            assert_no_batch(&mut to_worker, TICK * 2).await;
            task.abort();
            Ok(())
        }

        /// I-2 S2 pin (switch off): the quorum ack prunes the batch, so a client resend of its
        /// bytes is accepted as new and the builder seals the same transactions again under the
        /// same digest; the second execution skips all of them. This is the bench-5 duplicate.
        #[tokio::test]
        async fn s2_pin_switch_off_resend_reseals_the_same_digest() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let txs = transfers(&mut bench.a, &bench.chain, 4);
            let hold = transfers(&mut bench.holder, &bench.chain, 1);
            let (builder, mut to_worker) = bench.builder(false, true, TICK)?;
            assert!(!builder.keeps_sealed());
            bench.submit(&txs).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            ack(b1_ack, Ok(()))?;
            bench.wait_gone(&txs).await?;

            // the holder's batch keeps the builder busy while all four resends land, so they
            // share one batch
            bench.submit(&hold).await;
            let (held, held_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&held), tx_hashes(&hold));
            for tx in &txs {
                let resend = bench.txpool.add_raw_transaction_external(tx.clone()).await;
                assert!(resend.is_ok(), "the switch-off node forgot the bytes: {resend:?}");
            }
            ack(held_ack, Ok(()))?;
            let (again, again_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&again), tx_hashes(&txs));
            assert_eq!(again.digest(), b1.digest());
            ack(again_ack, Ok(()))?;

            let landed = bench.execute(&[&b1]).await?;
            let skipped = bench.execute(&[&again]).await?;
            assert!(landed.gas_used > 0);
            assert_eq!(skipped.gas_used, 0, "the second copy is skipped nonce-too-low");
            assert!(bench.txpool.sealed_txs().is_empty(), "the switch-off path never marks");
            task.abort();
            Ok(())
        }

        /// I-2 S3 (switch on): a resend of one sender's part of a sealed two-sender batch answers
        /// `already imported`, so no second batch carries it.
        #[tokio::test]
        async fn s3_resend_of_a_subset_is_already_known() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let a_txs = transfers(&mut bench.a, &bench.chain, 4);
            let b_txs = transfers(&mut bench.b, &bench.chain, 4);
            let both = [a_txs.clone(), b_txs.clone()].concat();
            let (builder, mut to_worker) = bench.builder(true, true, TICK)?;
            bench.submit(&both).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(sorted(hashes(&b1)), sorted(tx_hashes(&both)));
            ack(b1_ack, Ok(()))?;
            bench.wait_marked(&both, true).await?;

            for tx in &a_txs {
                let resend = bench.txpool.add_raw_transaction_external(tx.clone()).await;
                assert!(already_known(&resend), "a resend of sealed bytes is known: {resend:?}");
            }
            assert_no_batch(&mut to_worker, TICK * 2).await;

            bench.execute(&[&b1]).await?;
            bench.wait_gone(&both).await?;
            bench.wait_no_marks().await?;
            assert_eq!(bench.nonce(bench.a.address())?, 4);
            assert_eq!(bench.nonce(bench.b.address())?, 4);
            task.abort();
            Ok(())
        }

        /// I-2 S3 pin (switch off): after the ack prunes a two-sender batch, one sender's resend
        /// is accepted and sealed into a second batch with a new digest, whose execution skips
        /// every copy.
        #[tokio::test]
        async fn s3_pin_switch_off_subset_resend_seals_a_new_digest() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let a_txs = transfers(&mut bench.a, &bench.chain, 4);
            let b_txs = transfers(&mut bench.b, &bench.chain, 4);
            let hold = transfers(&mut bench.holder, &bench.chain, 1);
            let both = [a_txs.clone(), b_txs.clone()].concat();
            let (builder, mut to_worker) = bench.builder(false, true, TICK)?;
            bench.submit(&both).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            ack(b1_ack, Ok(()))?;
            bench.wait_gone(&both).await?;

            // the holder's batch keeps the builder busy while A's four resends land
            bench.submit(&hold).await;
            let (held, held_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&held), tx_hashes(&hold));
            for tx in &a_txs {
                let resend = bench.txpool.add_raw_transaction_external(tx.clone()).await;
                assert!(resend.is_ok(), "the switch-off node forgot the bytes: {resend:?}");
            }
            ack(held_ack, Ok(()))?;
            let (b2, b2_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b2), tx_hashes(&a_txs));
            assert_ne!(b2.digest(), b1.digest());
            ack(b2_ack, Ok(()))?;

            bench.execute(&[&b1]).await?;
            let skipped = bench.execute(&[&b2]).await?;
            assert_eq!(skipped.gas_used, 0, "every copy is skipped nonce-too-low");
            task.abort();
            Ok(())
        }

        /// A seal whose report fails (`FailedToReport`, as at an epoch boundary) marks nothing:
        /// its transactions stay pooled and unmarked, the retry re-seals them, and the retry's
        /// quorum ack marks them.
        #[tokio::test]
        async fn failed_report_marks_nothing_and_the_retry_reseals() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let txs = transfers(&mut bench.a, &bench.chain, 2);
            let (builder, mut to_worker) = bench.builder(true, true, TICK)?;
            bench.submit(&txs).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            ack(b1_ack, Err(BlockSealError::FailedToReport))?;

            // the retry on the next tick proves the failure was taken: nothing was marked
            let (retry, retry_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&retry), hashes(&b1));
            assert!(bench.txpool.sealed_txs().is_empty());
            bench.assert_pooled(&txs);

            ack(retry_ack, Ok(()))?;
            bench.wait_marked(&txs, true).await?;
            bench.assert_pooled(&txs);
            task.abort();
            Ok(())
        }

        /// With the switch on, a builder whose worker forwards instead of sealing by quorum
        /// prunes an acked batch as today and marks nothing: its own digest never executes.
        #[tokio::test]
        async fn observer_prunes_an_acked_batch_as_today() -> eyre::Result<()> {
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let txs = transfers(&mut bench.a, &bench.chain, 2);
            let (builder, mut to_worker) = bench.builder(true, false, TICK)?;
            assert!(builder.sealed_tx_keep() && !builder.keeps_sealed());
            bench.submit(&txs).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&txs));
            ack(b1_ack, Ok(()))?;
            bench.wait_gone(&txs).await?;
            assert!(bench.txpool.sealed_txs().is_empty());
            task.abort();
            Ok(())
        }

        /// A mark whose blocking task fails falls back to the prune path for that batch: the
        /// outcome carries the batch's hashes and optimistic account changes. A finished mark
        /// keeps the transactions, whatever it marked.
        #[tokio::test]
        async fn mark_join_error_falls_back_to_pruning() {
            let failed = tokio::task::spawn_blocking(|| -> usize { panic!("the mark task fails") })
                .await
                .expect_err("the blocking task panicked");
            let hash = TxHash::repeat_byte(7);
            let address = Address::repeat_byte(1);
            let mined = || MinedBatchResult {
                mined_transactions: vec![hash],
                changed_accounts: vec![ChangedAccount {
                    address,
                    nonce: 3,
                    balance: U256::from(5),
                }],
            };
            let digest = BlockHash::repeat_byte(9);

            assert_matches!(
                outcome_after_mark(Err(failed), digest, mined()),
                BuildOutcome::Mined(MinedBatchResult { mined_transactions, changed_accounts })
                    if mined_transactions == vec![hash]
                        && changed_accounts.iter().map(|account| (account.address, account.nonce))
                            .eq([(address, 3)])
            );
            assert_matches!(outcome_after_mark(Ok(0), digest, mined()), BuildOutcome::Kept);
            assert_matches!(outcome_after_mark(Ok(1), digest, mined()), BuildOutcome::Kept);
        }

        /// The builder logs its effective mode once when it starts: the configured switch, what
        /// the node reported about the worker, and whether marks are made. A switch that is on
        /// while nothing marks is visible on this line.
        #[tokio::test]
        async fn start_line_reports_the_effective_mode() -> eyre::Result<()> {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new()?;
            let bench = Bench::new(tmp_dir.path())?;
            for (keep, quorum) in [(true, false), (true, true)] {
                let (builder, _to_worker) = bench.builder(keep, quorum, LONG_TICK)?;
                let task = tokio::spawn(builder.run());
                tokio::time::sleep(Duration::from_millis(50)).await;
                task.abort();
            }
            let lines: Vec<_> = capture
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.field("message") == Some("batch builder started"))
                .map(|event| {
                    ["sealed_tx_keep", "seals_via_quorum", "keeps_sealed"]
                        .map(|name| event.field(name).unwrap_or_default().to_string())
                })
                .collect();
            assert_eq!(lines, [["true", "false", "false"], ["true", "true", "true"]]);
            Ok(())
        }

        /// An empty build that skipped only sealed transactions leaves nothing to retry, so the
        /// next arrival is sealed after the fill delay rather than at the batch-delay tick, as
        /// after a pass that finds nothing pending.
        #[tokio::test]
        async fn arrival_after_an_all_sealed_build_arms_the_fill_delay() -> eyre::Result<()> {
            let capture = Capture::default();
            let _guard = tracing::subscriber::set_default(capture.clone());
            let tmp_dir = TempDir::new()?;
            let mut bench = Bench::new(tmp_dir.path())?;
            let first = transfers(&mut bench.a, &bench.chain, 1);
            let second = transfers(&mut bench.a, &bench.chain, 1);
            let (builder, mut to_worker) = bench.builder(true, true, LONG_TICK)?;
            bench.submit(&first).await;
            let task = tokio::spawn(builder.run());

            let (b1, b1_ack) = recv(&mut to_worker).await?;
            assert_eq!(hashes(&b1), tx_hashes(&first));
            ack(b1_ack, Ok(()))?;
            // the build right after the ack finds only the sealed transaction
            let all_sealed = || {
                capture.0.lock().unwrap().iter().any(|event| {
                    event.field("message")
                        == Some("transaction selection produced an empty batch; sealing nothing")
                        && event.field("sealed_skipped") == Some("1")
                })
            };
            tn_test_utils::wait_until(WAIT, "an all-sealed build", || async { Ok(all_sealed()) })
                .await?;

            let submitted = Instant::now();
            bench.submit(&second).await;
            let (b2, b2_ack) = recv(&mut to_worker).await?;
            let waited = submitted.elapsed();
            assert!(waited >= BATCH_WAKE_FILL_DELAY, "built before the fill delay: {waited:?}");
            assert_eq!(hashes(&b2), tx_hashes(&second));
            let wakes: Vec<_> = capture
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.field("message") == Some("batch built"))
                .filter_map(|event| event.field("wake").map(str::to_string))
                .collect();
            assert_eq!(wakes, ["tick", "arrival"]);
            ack(b2_ack, Ok(()))?;
            task.abort();
            Ok(())
        }
    }
}
