//! Prometheus metrics for the reth execution environment.
//!
//! Two counter series are exported under the `tn_reth` scope, both counting transactions that
//! `build_block_from_batch_payload` declines to include in a block:
//! `tn_reth.unrecoverable_txs_dropped_total` (alertable — a certified batch carried a
//! transaction whose signer could not be recovered) and `tn_reth.invalid_txs_skipped_total`
//! (expected — duplicates/invalid transactions across independently-batching workers). See
//! [`RethEnvMetrics`] for per-series semantics. [`report_db_metrics`] additionally samples reth
//! database metrics as a pre-scrape hook.
//!
//! Everything here binds to the process-global `metrics` recorder, so
//! `tn_metrics::install_recorder` must run first — the node installs it before opening the
//! database (`RethEnv::new_database`), and [`init`] (called from `RethEnv::new`) then forces
//! registration so both counters exist at zero from process start.

use reth_metrics::{metrics::Counter, Metrics};
use std::sync::LazyLock;

use crate::RethDb;

/// Process-wide metrics for [`crate::RethEnv`].
///
/// A `LazyLock` static is justified here for the same reason as the engine's
/// `ENGINE_METRICS`: there is exactly one execution environment per process, and threading a
/// handle through `RethEnvInner` would touch every construction site and caller signature for
/// two counters.
///
/// First use must happen after the global recorder is installed. `tn_metrics::install_recorder`
/// runs before `RethEnv::new_database` (that ordering is a documented invariant of the
/// recorder), so a first use inside a `RethEnv` method binds to the real recorder rather than
/// the noop one.
pub(crate) static RETH_METRICS: LazyLock<RethEnvMetrics> = LazyLock::new(RethEnvMetrics::default);

/// Register both counters with the global recorder without recording an event.
///
/// Registration happens when the `LazyLock` is first forced, and the only sites that force it
/// are drop paths a healthy node never takes. Without this call the series would simply not
/// exist in a scrape until the first drop, which breaks the counters in the two ways that
/// matter: an absent series is indistinguishable from a broken exporter or a mistyped metric
/// name, so the "alert on any nonzero value" rule can never be verified in its negative case;
/// and `rate()` over a series whose very first sample is already nonzero renders no step, so a
/// one-shot drop burst on an otherwise quiet node stays invisible on the dashboard.
///
/// Called from [`crate::RethEnv::new`], which runs after `tn_metrics::install_recorder`, so
/// both counters baseline at zero from process start . . . the same way the worker and
/// executor counters they sit beside on the dashboard already do.
pub(crate) fn init() {
    LazyLock::force(&RETH_METRICS);
}

/// Metrics for building canonical blocks from certified batch payloads.
///
/// Both counters cover transactions that `build_block_from_batch_payload` silently declines to
/// include in the block it returns. The two drop sites have opposite expectedness, so they are
/// counted separately rather than summed - see the per-field docs.
#[derive(Metrics)]
#[metrics(scope = "tn_reth")]
pub(crate) struct RethEnvMetrics {
    /// Transactions dropped while building a block because their signer could not be recovered.
    ///
    /// Alert on any nonzero value. A certified sub-DAG is fixed and identical on every honest
    /// node, so an unrecoverable transaction is dropped rather than halting the network (see
    /// issues #933 / #938); reaching that path at all means a batch carrying undecodable
    /// transaction bytes was certified, i.e. batch validation was bypassed somewhere upstream.
    pub(crate) unrecoverable_txs_dropped_total: Counter,

    /// Transactions skipped while building a block because the EVM rejected them as invalid.
    ///
    /// Not an error signal, and not alertable: workers batch independently, so the same
    /// transaction can legitimately reach execution twice within one consensus output and the
    /// second copy is skipped as a duplicate. A steady nonzero rate is normal operation. The
    /// alertable counter is [`RethEnvMetrics::unrecoverable_txs_dropped_total`].
    pub(crate) invalid_txs_skipped_total: Counter,
}

/// Report sampled reth database metrics (table sizes, page usage, freelist).
///
/// Intended as a pre-scrape hook for the prometheus metrics endpoint. Lives here to keep
/// reth-db types out of the node crate.
pub fn report_db_metrics(db: &RethDb) {
    use reth_db::database_metrics::DatabaseMetrics;
    db.report_metrics();
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // construct directly (not via the static) so handles bind to the local recorder
            let metrics = RethEnvMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.unrecoverable_txs_dropped_total.increment(2);
            metrics.invalid_txs_skipped_total.increment(5);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_reth.unrecoverable_txs_dropped_total");
        assert!(matches!(value, DebugValue::Counter(2)));
        let (_, _, _, value) = find("tn_reth.invalid_txs_skipped_total");
        assert!(matches!(value, DebugValue::Counter(5)));
    }

    /// Construction alone must register both series at zero, with no drop having happened.
    ///
    /// This is the property [`init`] relies on: a healthy node that never drops a transaction
    /// still exports both counters, so `absent()` distinguishes "no drops" from "exporter
    /// broken" and the first drop renders as a step from zero rather than as a series that
    /// appears already nonzero.
    #[test]
    fn test_metrics_register_at_zero_without_any_event() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // `new_with_labels` rather than `default()`: the derive caches `default()` in a
            // per-type `OnceLock`, so it would bind to whichever recorder ran first in this
            // process and make the assertion depend on test ordering
            let _metrics = RethEnvMetrics::new_with_labels(Vec::<metrics::Label>::new());
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered without an event"))
        };

        let (_, _, _, value) = find("tn_reth.unrecoverable_txs_dropped_total");
        assert!(matches!(value, DebugValue::Counter(0)));
        let (_, _, _, value) = find("tn_reth.invalid_txs_skipped_total");
        assert!(matches!(value, DebugValue::Counter(0)));
    }
}
