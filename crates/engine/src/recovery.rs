//! Signer recovery for queued consensus output, off the execution thread.
//!
//! When a consensus output enters the engine queue, [`RecoveryHandle::spawn`] starts a blocking
//! job that recovers the signers of all its transactions
//! ([`RethEnv::recover_output_signers`]). The execution task resolves the handle when it reaches
//! the output's first batch, so the recovery overlaps the execution of the outputs queued ahead
//! of it. Recovery is a pure function of the certified bytes: the job moves when signers are
//! recovered, never which transactions a block holds.

use crate::{duration_ms, metrics::EngineMetrics};
use std::time::Instant;
use tn_reth::RethEnv;
use tn_types::{ConsensusOutput, Recovered, TaskSpawner, TransactionSigned};
use tokio::sync::oneshot;
use tracing::debug;

/// The pending result of one consensus output's background signer recovery.
///
/// Memory: a handle holds its output's recovered transactions from the moment its job finishes
/// until the execution task takes them. Each `Recovered<TransactionSigned>` owns a decoded copy
/// of its transaction, calldata included, so that is about the output's raw transaction bytes
/// again plus 0.3 kB per transaction (5-7.5 MB for a 12.5 k-transaction output of transfers). Only
/// outputs in the engine queue and the one executing have a handle (outputs still in the
/// `to_engine` channel have no job), so at most `MAX_QUEUED_OUTPUTS + 1` outputs' signers are held
/// at once.
///
/// Dropping a handle (an output dropped from the queue at shutdown or an epoch close) discards
/// the job's result: the job's send fails silently, and nothing panics or logs.
#[derive(Debug)]
pub(crate) struct RecoveryHandle {
    /// Receives the job's result; closes without a value if the job ends without sending one
    /// (a panic).
    rx: oneshot::Receiver<OutputSigners>,
}

impl RecoveryHandle {
    /// Start recovering `output`'s signers on a blocking job, recording into `metrics`.
    ///
    /// Must be called within a tokio runtime (the engine's run loop, or a test runtime).
    pub(crate) fn spawn(
        output: &ConsensusOutput,
        task_spawner: &TaskSpawner,
        metrics: &'static EngineMetrics,
    ) -> Self {
        let (tx, rx) = oneshot::channel();
        // the output shares its contents through an `Arc`: no transaction bytes are copied
        let output = output.clone();
        let task_name = format!("recover-signers-{}", output.number());
        task_spawner.spawn_blocking_task(task_name, move || {
            run_recovery_job(&output, tx, metrics);
            Ok(())
        });
        Self { rx }
    }

    /// Take the job's result, blocking the calling thread until the job has finished.
    ///
    /// Call it from a blocking context only (the execution task runs on one). If the job ended
    /// without a result (it panicked), the signers are recovered inline from the same bytes and
    /// `tn_engine_recovery_fallback_total` is incremented. This never fails: an error would halt
    /// the engine, and the inline recovery yields the same signers and so the same blocks.
    pub(crate) fn wait(self, output: &ConsensusOutput, metrics: &EngineMetrics) -> OutputSigners {
        match self.rx.blocking_recv() {
            Ok(signers) => signers,
            Err(_) => {
                metrics.recovery_fallback_total.increment(1);
                recover_signers(output, metrics)
            }
        }
    }

    /// A handle whose result is already available, so no job runs: the engine queues an output
    /// without batches with `resolved(Vec::new())`, and tests queue outputs this way without a
    /// runtime.
    pub(crate) fn resolved(signers: OutputSigners) -> Self {
        let (tx, rx) = oneshot::channel();
        // the receiver is alive in `rx`, so the send cannot fail
        let _ = tx.send(signers);
        Self { rx }
    }
}

/// The recovered transactions of one consensus output: one `Vec` per batch, in
/// [`ConsensusOutput::flatten_batches`] order (see [`RethEnv::recover_output_signers`]).
pub(crate) type OutputSigners = Vec<Vec<Recovered<TransactionSigned>>>;

/// Recover `output`'s signers on the calling thread, timing the work.
///
/// Records the duration in `tn_engine_recover_seconds` and logs one `signers recovered` debug
/// line (target `engine`) with the consensus number, the transactions handed to recovery and
/// `recover_ms`. The background job runs this, and so do the fallback and the callers that
/// execute an output without a job, so every recovered output logs exactly one line.
pub(crate) fn recover_signers(output: &ConsensusOutput, metrics: &EngineMetrics) -> OutputSigners {
    let start = Instant::now();
    let signers = RethEnv::recover_output_signers(output);
    let elapsed = start.elapsed();
    metrics.recover_seconds.record(elapsed);
    debug!(
        target: "engine",
        consensus_number = output.number(),
        txs = output_tx_count(output),
        recover_ms = duration_ms(elapsed),
        "signers recovered"
    );
    signers
}

/// Transactions across every batch of `output`, before any unrecoverable or invalid one is
/// dropped.
pub(crate) fn output_tx_count(output: &ConsensusOutput) -> usize {
    output.batches().iter().flat_map(|cert| cert.batches.iter()).map(|b| b.transactions.len()).sum()
}

/// The body of the background job: recover `output`'s signers and send them to the handle.
fn run_recovery_job(
    output: &ConsensusOutput,
    tx: oneshot::Sender<OutputSigners>,
    metrics: &EngineMetrics,
) {
    let signers = recover_signers(output, metrics);
    // the receiver is gone when the output was dropped from the queue (shutdown or an epoch
    // close): discarding the result is expected there, so it is neither an error nor logged
    let _ = tx.send(signers);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::{
        debugging::{DebugValue, DebuggingRecorder},
        CompositeKey,
    };
    use tn_reth::test_utils::batches;
    use tn_types::{test_chain_spec_arc, CertifiedBatch};

    /// An output whose single certificate carries three batches of signed transactions, the
    /// second also holding bytes that do not decode.
    fn output_with_transactions() -> ConsensusOutput {
        let mut batches = batches(test_chain_spec_arc(), 3);
        batches[1].transactions.push(vec![0xde, 0xad, 0xbe, 0xef]);
        let digests = batches.iter().map(|batch| batch.digest()).collect();
        ConsensusOutput::new(
            Default::default(),
            Default::default(),
            5,
            false,
            digests,
            vec![CertifiedBatch { address: Default::default(), batches }],
        )
    }

    /// One recorded metric: its key and value (units and descriptions dropped).
    type Recorded = Vec<(CompositeKey, DebugValue)>;

    /// Run `f` with engine metrics bound to a fresh local recorder, returning its result and
    /// what it recorded. One snapshot only: taking a snapshot drains the histograms.
    fn with_local_metrics<T>(f: impl FnOnce(&EngineMetrics) -> T) -> (T, Recorded) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let out = ::metrics::with_local_recorder(&recorder, || {
            // constructed here (not the static) so the handles bind to the local recorder
            let metrics = EngineMetrics::new_with_labels(Vec::<::metrics::Label>::new());
            f(&metrics)
        });
        let recorded =
            snapshotter.snapshot().into_vec().into_iter().map(|(key, _, _, value)| (key, value));
        (out, recorded.collect())
    }

    /// The fallback counter's value, or zero when it is not registered.
    fn fallbacks(recorded: &Recorded) -> u64 {
        recorded
            .iter()
            .find_map(|(key, value)| {
                match (key.key().name() == "tn_engine.recovery_fallback_total", value) {
                    (true, DebugValue::Counter(count)) => Some(*count),
                    _ => None,
                }
            })
            .unwrap_or_default()
    }

    /// The number of `tn_engine_recover_seconds` samples recorded.
    fn recover_samples(recorded: &Recorded) -> usize {
        recorded
            .iter()
            .find_map(|(key, value)| {
                match (key.key().name() == "tn_engine.recover_seconds", value) {
                    (true, DebugValue::Histogram(samples)) => Some(samples.len()),
                    _ => None,
                }
            })
            .unwrap_or_default()
    }

    /// The job's result reaches the handle unchanged: the same signers, one entry per flattened
    /// batch, as recovering on the execution thread; the job records its own duration and the
    /// fallback never runs.
    #[test]
    fn test_job_result_matches_inline_recovery() {
        let output = output_with_transactions();
        let ((signers, expected), recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel();
            run_recovery_job(&output, tx, metrics);
            (RecoveryHandle { rx }.wait(&output, metrics), RethEnv::recover_output_signers(&output))
        });

        assert_eq!(signers, expected);
        assert_eq!(signers.len(), output.flatten_batches().len());
        let recovered: Vec<usize> = signers.iter().map(Vec::len).collect();
        let handed: Vec<usize> = output
            .batches()
            .iter()
            .flat_map(|cert| cert.batches.iter())
            .map(|batch| batch.transactions.len())
            .collect();
        // the undecodable bytes of batch 2 are dropped, every signed transaction survives
        assert_eq!(recovered, [handed[0], handed[1] - 1, handed[2]]);
        assert_eq!(recover_samples(&recorded), 1, "the job records its own duration");
        assert_eq!(fallbacks(&recorded), 0, "no fallback on the job's result");
    }

    /// A job that ends without a result (its sender dropped, as when it panics) falls back to
    /// recovering inline from the same bytes: the same signers, so the same blocks, and the
    /// fallback counter reads one. Never an error.
    #[test]
    fn test_dropped_job_falls_back_to_inline_recovery() {
        let output = output_with_transactions();
        let ((signers, expected), recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel::<OutputSigners>();
            drop(tx);
            (RecoveryHandle { rx }.wait(&output, metrics), RethEnv::recover_output_signers(&output))
        });

        assert!(signers.iter().any(|batch| !batch.is_empty()), "test setup: signed transactions");
        assert_eq!(signers, expected);
        assert_eq!(fallbacks(&recorded), 1);
        assert_eq!(recover_samples(&recorded), 1, "the inline recovery is timed too");
    }

    /// An output dropped from the queue drops its handle; the job's send then fails and is
    /// discarded without a panic.
    #[test]
    fn test_job_tolerates_dropped_handle() {
        let output = output_with_transactions();
        let ((), recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel();
            drop(RecoveryHandle { rx });
            run_recovery_job(&output, tx, metrics);
        });
        assert_eq!(recover_samples(&recorded), 1);
        assert_eq!(fallbacks(&recorded), 0);
    }
}
