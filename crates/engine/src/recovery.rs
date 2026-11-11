//! Signer recovery for queued consensus output, off the execution thread.
//!
//! When a consensus output enters the engine queue, [`RecoveryHandle::spawn`] starts a blocking
//! job that recovers the signers of all its transactions
//! ([`RethEnv::recover_output_signers`]). The execution task resolves the handle when it reaches
//! the output's first batch, so the recovery overlaps the execution of the outputs queued ahead
//! of it. Recovery is a pure function of the certified bytes: the job moves when signers are
//! recovered, never which transactions a block holds.
//!
//! The recovery's parallel work runs on rayon's global pool, which the block builds' own parallel
//! phases (the `finish` lap) share, unless `recovery_threads` gives it a dedicated pool (see
//! [`run_on_recovery_pool`]). The pool decides only which threads compute the signers, never
//! which signers they compute.

use crate::{duration_ms, metrics::EngineMetrics};
use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use std::{fmt, sync::OnceLock, time::Instant};
use tn_reth::RethEnv;
use tn_types::{ConsensusOutput, Recovered, TaskSpawner, TransactionSigned};
use tokio::sync::oneshot;
use tracing::{debug, warn};

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
    /// The engine's `recovery_threads`, kept so the inline fallback recovers on the same pool as
    /// the job did; zero is rayon's global pool.
    threads: u16,
}

impl RecoveryHandle {
    /// Start recovering `output`'s signers on a blocking job, recording into `metrics`.
    ///
    /// The job's parallel work runs on the dedicated recovery pool of `threads` threads (the
    /// engine's `recovery_threads`), or on rayon's global pool when `threads` is zero. Must be
    /// called within a tokio runtime (the engine's run loop, or a test runtime).
    pub(crate) fn spawn(
        output: &ConsensusOutput,
        task_spawner: &TaskSpawner,
        metrics: &'static EngineMetrics,
        threads: u16,
    ) -> Self {
        let (tx, rx) = oneshot::channel();
        // the output shares its contents through an `Arc`: no transaction bytes are copied
        let output = output.clone();
        let task_name = format!("recover-signers-{}", output.number());
        task_spawner.spawn_blocking_task(task_name, move || {
            run_recovery_job(&output, tx, metrics, threads);
            Ok(())
        });
        Self { rx, threads }
    }

    /// Take the job's result, blocking the calling thread until the job has finished.
    ///
    /// Call it from a blocking context only (the execution task runs on one). If the job ended
    /// without a result (it panicked), the signers are recovered inline from the same bytes, on
    /// the pool the job used, `tn_engine_recovery_fallback_total` is incremented and a warning
    /// names the output. This never fails: an error would halt the engine, and the inline
    /// recovery yields the same signers and so the same blocks.
    pub(crate) fn wait(self, output: &ConsensusOutput, metrics: &EngineMetrics) -> OutputSigners {
        let Self { rx, threads } = self;
        match rx.blocking_recv() {
            Ok(signers) => signers,
            Err(_) => {
                // the task manager drops a panicked non-critical job without logging it, so this
                // line attributes the fallback counter to an output; a deterministic panic will
                // recur in the inline recovery below, this time on the execution thread
                warn!(
                    target: "engine",
                    consensus_number = output.number(),
                    "signer recovery job ended without a result - recovering inline"
                );
                metrics.recovery_fallback_total.increment(1);
                recover_signers_on(output, metrics, threads)
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
        Self { rx, threads: 0 }
    }
}

/// The recovered transactions of one consensus output: one `Vec` per batch, in
/// [`ConsensusOutput::flatten_batches`] order (see [`RethEnv::recover_output_signers`]).
pub(crate) type OutputSigners = Vec<Vec<Recovered<TransactionSigned>>>;

/// Run `recover` on the dedicated signer recovery pool of `threads` threads and return its
/// result, so any rayon work inside it (the `par_iter` of [`RethEnv::recover_output_signers`])
/// runs on that pool and not on rayon's global one.
///
/// The engine's recovery jobs and their inline fallback select their pool the same way, with the
/// engine's `recovery_threads`. This function is public as bench and test support, so a
/// benchmark can put a recovery where the engine puts it, for example
/// `run_on_recovery_pool(3, || RethEnv::recover_output_signers(&output))`. It needs no tokio
/// runtime. A thread outside any rayon pool (a plain or tokio blocking thread, as the engine's
/// jobs run on) blocks until `recover` returns; a worker of the dedicated pool runs `recover`
/// in place; a worker of another rayon pool keeps running that pool's jobs while it waits.
///
/// - `threads == 0` installs nothing: `recover` runs on the calling thread, so its rayon work uses
///   the global pool, exactly as with `recovery_threads: 0`.
/// - The pool is process-wide and built on first use, with threads named `tn-recover-{i}`; its size
///   is fixed by the first call with a non-zero `threads` (this function's or an engine's), and
///   every later call gets that same pool whatever `threads` it passes.
/// - A pool that fails to build logs one warning, and `recover` then runs on the calling thread
///   (the global pool) for the rest of the process. This never panics.
pub fn run_on_recovery_pool<R: Send>(threads: u16, recover: impl FnOnce() -> R + Send) -> R {
    install(threads, recover).0
}

/// The rayon pool a signer recovery ran on: the `pool` field of the `signers recovered` line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryPool {
    /// The process-wide dedicated recovery pool (`recovery_threads` above zero).
    Dedicated,
    /// Rayon's global pool, which the block builds share (`recovery_threads: 0`, or a dedicated
    /// pool that failed to build).
    Global,
}

impl fmt::Display for RecoveryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dedicated => "dedicated",
            Self::Global => "global",
        })
    }
}

/// Recover `output`'s signers on rayon's global pool, timing the work: [`recover_signers_on`]
/// with no dedicated pool, for the callers that execute an output without a job.
pub(crate) fn recover_signers(output: &ConsensusOutput, metrics: &EngineMetrics) -> OutputSigners {
    recover_signers_on(output, metrics, 0)
}

/// Recover `output`'s signers on the dedicated recovery pool of `threads` threads (rayon's global
/// pool when `threads` is zero, see [`run_on_recovery_pool`]), blocking the calling thread until
/// they are recovered and timing the work.
///
/// Records the duration in `tn_engine_recover_seconds` and logs one `signers recovered` debug
/// line (target `engine`) with the consensus number, the transactions handed to recovery,
/// `recover_ms` and `pool` (`dedicated` or `global`, the pool that ran the recovery). The
/// background job runs this, and so do the fallback and the callers that execute an output
/// without a job, so every recovered output logs exactly one line. On the dedicated pool
/// `recover_ms` includes any wait for a free pool thread.
///
/// `recover_ms` is on this line only. `output executed` carries `recover_wait_ms` instead, the
/// time the execution thread blocked waiting for this recovery; since the job runs beside the
/// build, `recover_ms` is not part of that line's `execute_ms` split (see
/// `EngineMetrics::recover_seconds`).
fn recover_signers_on(
    output: &ConsensusOutput,
    metrics: &EngineMetrics,
    threads: u16,
) -> OutputSigners {
    let start = Instant::now();
    let (signers, pool) = install(threads, || RethEnv::recover_output_signers(output));
    let elapsed = start.elapsed();
    metrics.recover_seconds.record(elapsed);
    // logged here, on the calling thread, after the pool has returned the signers
    debug!(
        target: "engine",
        consensus_number = output.number(),
        txs = output_tx_count(output),
        recover_ms = duration_ms(elapsed),
        pool = %pool,
        "signers recovered"
    );
    signers
}

/// Transactions across every batch of `output`, before any unrecoverable or invalid one is
/// dropped.
pub(crate) fn output_tx_count(output: &ConsensusOutput) -> usize {
    output.batches().iter().flat_map(|cert| cert.batches.iter()).map(|b| b.transactions.len()).sum()
}

/// The dedicated signer recovery pool, built once per process by the first recovery that asks for
/// one (see [`recovery_pool`]). `None` records a build that failed, so the failure warns once and
/// is not retried.
static RECOVERY_POOL: OnceLock<Option<ThreadPool>> = OnceLock::new();

/// Run `op` inside the dedicated recovery pool of `threads` threads, so the rayon work it starts
/// runs on that pool, and say which pool ran it. With `threads` zero, or no pool, `op` runs on
/// the calling thread and its rayon work uses the global pool.
fn install<R: Send>(threads: u16, op: impl FnOnce() -> R + Send) -> (R, RecoveryPool) {
    match recovery_pool(threads) {
        Some(pool) => (pool.install(op), RecoveryPool::Dedicated),
        None => (op(), RecoveryPool::Global),
    }
}

/// The process-wide dedicated recovery pool, or `None` when `threads` is zero (nothing is built)
/// or the pool failed to build.
///
/// The first call with a non-zero `threads` builds the pool with that many threads; every later
/// call returns the same pool, whatever `threads` it passes. The node reads `recovery_threads`
/// once at startup, so every engine of the process asks for the same size.
fn recovery_pool(threads: u16) -> Option<&'static ThreadPool> {
    if threads == 0 {
        return None;
    }
    RECOVERY_POOL.get_or_init(|| pool_or_warn(threads, || build_recovery_pool(threads))).as_ref()
}

/// Build a rayon pool of `threads` threads named `tn-recover-{i}`.
fn build_recovery_pool(threads: u16) -> Result<ThreadPool, ThreadPoolBuildError> {
    ThreadPoolBuilder::new()
        .num_threads(usize::from(threads))
        .thread_name(|index| format!("tn-recover-{index}"))
        .build()
}

/// The pool `build` returns, or `None` after one warning when it fails: recovery then stays on
/// the global pool, which computes the same signers.
fn pool_or_warn(
    threads: u16,
    build: impl FnOnce() -> Result<ThreadPool, ThreadPoolBuildError>,
) -> Option<ThreadPool> {
    build()
        .inspect_err(|error| {
            warn!(
                target: "engine",
                threads,
                %error,
                "signer recovery pool failed to build - recovering on the global pool"
            );
        })
        .ok()
}

/// The body of the background job: recover `output`'s signers on the pool `threads` selects and
/// send them to the handle.
fn run_recovery_job(
    output: &ConsensusOutput,
    tx: oneshot::Sender<OutputSigners>,
    metrics: &EngineMetrics,
    threads: u16,
) {
    let signers = recover_signers_on(output, metrics, threads);
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
    use std::{
        fmt,
        sync::{Arc, Mutex, PoisonError},
    };
    use tn_reth::test_utils::batches;
    use tn_types::{test_chain_spec_arc, CertifiedBatch};
    use tracing::{
        field::{Field, Visit},
        span,
        subscriber::Interest,
        Event, Level, Metadata, Subscriber,
    };

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

    /// One `engine` warning: its message and consensus number.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Warning {
        message: String,
        consensus_number: Option<u64>,
    }

    impl Visit for Warning {
        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "consensus_number" {
                self.consensus_number = Some(value);
            }
        }
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            }
        }
    }

    /// Keeps every `engine` warning. Installed for one closure with
    /// `tracing::subscriber::with_default`, so it sees the calling thread only and leaves the
    /// global subscriber alone.
    #[derive(Clone, Default)]
    struct WarnCapture(Arc<Mutex<Vec<Warning>>>);

    impl WarnCapture {
        /// The warnings captured so far, oldest first, emptying the store.
        fn take(&self) -> Vec<Warning> {
            std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    impl Subscriber for WarnCapture {
        fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
            // ask per event: the callsite cache is process-wide
            Interest::sometimes()
        }
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == "engine" && *metadata.level() == Level::WARN
        }
        fn new_span(&self, _attributes: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}
        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut warning = Warning::default();
            event.record(&mut warning);
            self.0.lock().unwrap_or_else(PoisonError::into_inner).push(warning);
        }
        fn enter(&self, _span: &span::Id) {}
        fn exit(&self, _span: &span::Id) {}
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
            run_recovery_job(&output, tx, metrics, 0);
            (
                RecoveryHandle { rx, threads: 0 }.wait(&output, metrics),
                RethEnv::recover_output_signers(&output),
            )
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
    /// recovering inline from the same bytes: the same signers, so the same blocks, the fallback
    /// counter reads one and one warning names the output. Never an error.
    #[test]
    fn test_dropped_job_falls_back_to_inline_recovery() {
        let output = output_with_transactions();
        let capture = WarnCapture::default();
        let ((signers, expected), recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel::<OutputSigners>();
            drop(tx);
            let signers = tracing::subscriber::with_default(capture.clone(), || {
                RecoveryHandle { rx, threads: 0 }.wait(&output, metrics)
            });
            (signers, RethEnv::recover_output_signers(&output))
        });

        assert!(signers.iter().any(|batch| !batch.is_empty()), "test setup: signed transactions");
        assert_eq!(signers, expected);
        assert_eq!(fallbacks(&recorded), 1);
        assert_eq!(recover_samples(&recorded), 1, "the inline recovery is timed too");
        assert_eq!(
            capture.take(),
            [Warning {
                message: "signer recovery job ended without a result - recovering inline".into(),
                consensus_number: Some(output.number()),
            }]
        );
    }

    /// An output dropped from the queue drops its handle; the job's send then fails and is
    /// discarded without a panic.
    #[test]
    fn test_job_tolerates_dropped_handle() {
        let output = output_with_transactions();
        let ((), recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel();
            drop(RecoveryHandle { rx, threads: 0 });
            run_recovery_job(&output, tx, metrics, 0);
        });
        assert_eq!(recover_samples(&recorded), 1);
        assert_eq!(fallbacks(&recorded), 0);
    }

    // ---- bench6 recovery-pool ----

    /// The dedicated pool size the tests ask for. Every test asks for the same size, so the
    /// process-wide pool has it whichever test builds it first.
    const POOL_THREADS: u16 = 3;

    /// One captured `engine` event: its level, its message and its other fields in emission
    /// order, each rendered as text.
    #[derive(Debug, Default)]
    struct Line {
        level: Option<Level>,
        message: String,
        fields: Vec<(String, String)>,
    }

    impl Line {
        /// The rendered value of the field `name`.
        fn field(&self, name: &str) -> Option<&str> {
            self.fields.iter().find(|(field, _)| field == name).map(|(_, value)| value.as_str())
        }
    }

    impl Visit for Line {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            let value = format!("{value:?}");
            if field.name() == "message" {
                self.message = value;
            } else {
                self.fields.push((field.name().to_string(), value));
            }
        }
    }

    /// Keeps every `engine` event at debug and above, on the calling thread only (installed with
    /// `tracing::subscriber::with_default`, like [`WarnCapture`]).
    #[derive(Clone, Default)]
    struct LineCapture(Arc<Mutex<Vec<Line>>>);

    impl LineCapture {
        /// The captured events whose message is `message`, oldest first, emptying the store.
        fn take(&self, message: &str) -> Vec<Line> {
            let lines = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
            lines.into_iter().filter(|line| line.message == message).collect()
        }

        /// The `pool` field of every captured `signers recovered` line, oldest first.
        fn recovered_pools(&self) -> Vec<String> {
            self.take("signers recovered")
                .iter()
                .map(|line| line.field("pool").unwrap_or("<none>").to_string())
                .collect()
        }
    }

    impl Subscriber for LineCapture {
        fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
            // ask per event: the callsite cache is process-wide
            Interest::sometimes()
        }
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == "engine" && *metadata.level() <= Level::DEBUG
        }
        fn new_span(&self, _attributes: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}
        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut line = Line { level: Some(*event.metadata().level()), ..Default::default() };
            event.record(&mut line);
            self.0.lock().unwrap_or_else(PoisonError::into_inner).push(line);
        }
        fn enter(&self, _span: &span::Id) {}
        fn exit(&self, _span: &span::Id) {}
    }

    /// A job on the dedicated pool hands over the signers recovery on the global pool computes,
    /// and its line says `pool=dedicated`; the recovery without a pool says `pool=global`.
    #[test]
    fn test_dedicated_pool_job_matches_global_recovery() {
        let output = output_with_transactions();
        let capture = LineCapture::default();
        let ((signers, expected), recorded) = with_local_metrics(|metrics| {
            tracing::subscriber::with_default(capture.clone(), || {
                let (tx, rx) = oneshot::channel();
                run_recovery_job(&output, tx, metrics, POOL_THREADS);
                let handle = RecoveryHandle { rx, threads: POOL_THREADS };
                (handle.wait(&output, metrics), recover_signers(&output, metrics))
            })
        });

        assert!(signers.iter().any(|batch| !batch.is_empty()), "test setup: signed transactions");
        assert_eq!(signers, expected);
        assert_eq!(recover_samples(&recorded), 2, "each recovery records its duration");
        assert_eq!(fallbacks(&recorded), 0, "no fallback on the job's result");
        assert_eq!(capture.recovered_pools(), ["dedicated", "global"]);
    }

    /// A job that ended without a result falls back to recovering inline on the pool it used: on
    /// the dedicated pool, the same signers as the global pool computes, the fallback counter
    /// reads one, the same warning names the output and the recovery line says `pool=dedicated`.
    #[test]
    fn test_dedicated_pool_fallback_matches_global_recovery() {
        let output = output_with_transactions();
        let capture = LineCapture::default();
        let (signers, recorded) = with_local_metrics(|metrics| {
            let (tx, rx) = oneshot::channel::<OutputSigners>();
            drop(tx);
            tracing::subscriber::with_default(capture.clone(), || {
                RecoveryHandle { rx, threads: POOL_THREADS }.wait(&output, metrics)
            })
        });

        assert!(signers.iter().any(|batch| !batch.is_empty()), "test setup: signed transactions");
        assert_eq!(signers, RethEnv::recover_output_signers(&output));
        assert_eq!(fallbacks(&recorded), 1);
        assert_eq!(recover_samples(&recorded), 1, "the inline recovery is timed too");
        let lines = std::mem::take(&mut *capture.0.lock().unwrap_or_else(PoisonError::into_inner));
        let summary: Vec<(Option<Level>, &str, Option<&str>)> = lines
            .iter()
            .map(|line| (line.level, line.message.as_str(), line.field("pool")))
            .collect();
        assert_eq!(
            summary,
            [
                (
                    Some(Level::WARN),
                    "signer recovery job ended without a result - recovering inline",
                    None
                ),
                (Some(Level::DEBUG), "signers recovered", Some("dedicated")),
            ]
        );
        assert_eq!(lines[0].field("consensus_number"), Some(output.number().to_string().as_str()));
    }

    /// The dedicated pool's threads are named `tn-recover-{i}`, and the rayon work started inside
    /// `run_on_recovery_pool` runs on them, not on the global pool.
    #[test]
    fn test_recovery_pool_threads_are_named() {
        use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};

        let (pool_threads, names) = run_on_recovery_pool(POOL_THREADS, || {
            let names: Vec<String> = (0..256)
                .into_par_iter()
                .map(|_| std::thread::current().name().unwrap_or_default().to_string())
                .collect();
            (rayon::current_num_threads(), names)
        });

        assert_eq!(pool_threads, usize::from(POOL_THREADS));
        assert!(
            names.iter().all(|name| name.starts_with("tn-recover-")),
            "every parallel item ran on the recovery pool: {names:?}"
        );
    }

    /// `recovery_threads: 0` installs nothing: no pool is handed out, and the closure runs on the
    /// calling thread, outside any rayon pool, so its rayon work uses the global pool.
    #[test]
    fn test_zero_threads_installs_nothing() {
        assert!(recovery_pool(0).is_none());
        let caller = std::thread::current().id();
        let (ran_on, worker) = run_on_recovery_pool(0, || {
            (std::thread::current().id(), rayon::current_thread_index())
        });
        assert_eq!(ran_on, caller, "the closure runs on the calling thread");
        assert_eq!(worker, None, "the calling thread is no rayon worker");
    }

    /// The pool is built once per process: a later request, even for another size, returns the
    /// first pool.
    #[test]
    fn test_recovery_pool_is_built_once() {
        let first = recovery_pool(POOL_THREADS).expect("the pool builds");
        let second = recovery_pool(POOL_THREADS + 2).expect("the pool is already built");
        assert!(std::ptr::eq(first, second), "the second request returns the first pool");
        assert_eq!(second.current_num_threads(), usize::from(POOL_THREADS));
    }

    /// A pool that fails to build is not used: one warning names the thread count, nothing
    /// panics, and no pool is returned, so recovery stays on the global pool. (The process-wide
    /// pool stores that `None`, so the build is neither retried nor warned about again.)
    #[test]
    fn test_failed_pool_build_warns_and_returns_none() {
        let capture = LineCapture::default();
        let pool = tracing::subscriber::with_default(capture.clone(), || {
            pool_or_warn(POOL_THREADS, || {
                ThreadPoolBuilder::new()
                    .num_threads(usize::from(POOL_THREADS))
                    .spawn_handler(|_| Err(std::io::Error::other("thread spawn refused")))
                    .build()
            })
        });

        assert!(pool.is_none());
        let warnings =
            capture.take("signer recovery pool failed to build - recovering on the global pool");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].level, Some(Level::WARN));
        assert_eq!(warnings[0].field("threads"), Some(POOL_THREADS.to_string().as_str()));
    }
}
