//! Global Prometheus recorder with a selective `reth` prefix.
//!
//! Telcoin metrics are recorded with a `tn` prefix (e.g. `tn_worker.batches_sealed_total`)
//! and pass through untouched. Everything else - reth's built-in instrumentation (db,
//! txpool, provider) and process metrics - gets a `reth.` prefix prepended so the rendered
//! names (`reth_db_*`, `reth_process_*`, ...) match a stock reth node and upstream Grafana
//! dashboards keep working.

use std::sync::{Mutex, OnceLock};

use eyre::eyre;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};

/// Buckets for histograms ending in `_seconds` (latencies).
const SECONDS_BUCKETS: &[f64] =
    &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

/// Buckets for histograms ending in `_bytes` (payload sizes), 1KiB..4MiB.
const BYTES_BUCKETS: &[f64] =
    &[1_024.0, 4_096.0, 16_384.0, 65_536.0, 262_144.0, 1_048_576.0, 4_194_304.0];

/// Buckets for gas used per block, 100k..30M.
const GAS_BUCKETS: &[f64] = &[
    100_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
    2_500_000.0,
    5_000_000.0,
    10_000_000.0,
    15_000_000.0,
    21_000_000.0,
    30_000_000.0,
];

/// Buckets for `tn_worker_batch_transactions`, the transaction count of one own batch.
///
/// The top bucket must be at or above `max_batch_gas(epoch) / 21_000`, because a transaction
/// uses at least 21,000 gas. That is 30,000,000 / 21,000 = 1,428 today. The bounds at 750 and
/// 1,250 resolve the band between a half full batch and a full batch.
///
/// [`tn_types::max_batch_gas`] takes an epoch so that a fork can raise it. A fork that raises it
/// must also raise the top bucket here. `test_batch_transactions_buckets_cover_full_batch` fails
/// until it does.
const BATCH_TRANSACTIONS_BUCKETS: &[f64] =
    &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 750.0, 1_000.0, 1_250.0, 1_500.0];

/// Buckets for `tn_executor_output_batches`, the batch count of one consensus output.
///
/// The layout is sized for one full certificate from each authority of a 100-authority
/// committee: 100 * [`tn_types::MAX_HEADER_NUM_OF_BATCHES`] (10) = 1,000 batches. These are the
/// same bounds as before #1511, so this series keeps its `le` label set across the upgrade.
///
/// A larger committee or a higher `MAX_HEADER_NUM_OF_BATCHES` puts samples in `+Inf` and needs a
/// new layout. `_sum` and `_count` stay exact. `test_output_buckets_cover_sizing_committee` fails
/// when `MAX_HEADER_NUM_OF_BATCHES` grows past this layout.
const OUTPUT_BATCHES_BUCKETS: &[f64] =
    &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0];

/// Buckets for `tn_executor_output_transactions`, the transaction count of one consensus output.
///
/// An output sums the batches of every certificate in the committed sub-DAG. It can hold every
/// batch that [`OUTPUT_BATCHES_BUCKETS`] covers, and each batch can hold up to
/// `max_batch_gas(epoch) / 21_000` transactions. The top bucket is therefore at or above the
/// committee size times the batches per certificate times the per-batch maximum:
/// 1,000 * 1,428 = 1,428,000. An output whose batch count lands in a finite batches bucket also
/// lands in a finite transactions bucket. `test_output_buckets_cover_sizing_committee` fails when
/// that stops being true.
const OUTPUT_TRANSACTIONS_BUCKETS: &[f64] = &[
    1.0,
    2.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_500.0,
    5_000.0,
    10_000.0,
    25_000.0,
    50_000.0,
    100_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
    2_500_000.0,
];

/// The handle to the global Prometheus registry. Set exactly once by [`install_recorder`].
static RECORDER_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global metrics recorder and return a handle for rendering scrapes.
///
/// Idempotent: subsequent calls return the handle from the first successful install.
/// Errors if a different global recorder is already installed (e.g. by a dependency).
///
/// # Invariant
///
/// This MUST run before any reth components are constructed (in particular before
/// `RethEnv::new_database`). Reth's derive-style metric handles bind to whatever recorder
/// is installed at construction time; metrics registered against the default noop recorder
/// are lost permanently.
pub fn install_recorder() -> eyre::Result<&'static PrometheusHandle> {
    // serialize installs so concurrent callers see the OnceLock consistently
    static INSTALL: Mutex<()> = Mutex::new(());
    let _guard = INSTALL.lock().map_err(|_| eyre!("metrics recorder install lock poisoned"))?;

    if let Some(handle) = RECORDER_HANDLE.get() {
        return Ok(handle);
    }

    let recorder = build_prometheus_recorder()?;
    let handle = recorder.handle();
    metrics::set_global_recorder(TnPrefixRecorder { inner: recorder })
        .map_err(|e| eyre!("failed to install global metrics recorder: {e}"))?;

    Ok(RECORDER_HANDLE.get_or_init(|| handle))
}

/// Build the underlying Prometheus recorder with explicit histogram buckets.
///
/// Buckets are REQUIRED for aggregatable histograms - without them the exporter renders
/// histograms as quantile summaries, which cannot be aggregated or re-quantiled in Grafana.
/// Matchers apply to the final (sanitized) metric name, after the selective prefix.
fn build_prometheus_recorder() -> eyre::Result<PrometheusRecorder> {
    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".to_string()), SECONDS_BUCKETS)?
        .set_buckets_for_metric(Matcher::Suffix("_bytes".to_string()), BYTES_BUCKETS)?
        .set_buckets_for_metric(Matcher::Full("tn_engine_block_gas_used".to_string()), GAS_BUCKETS)?
        .set_buckets_for_metric(
            Matcher::Full("tn_worker_batch_transactions".to_string()),
            BATCH_TRANSACTIONS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("tn_executor_output_transactions".to_string()),
            OUTPUT_TRANSACTIONS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("tn_executor_output_batches".to_string()),
            OUTPUT_BATCHES_BUCKETS,
        )?;

    Ok(builder.build_recorder())
}

/// Returns `true` if the metric name belongs to telcoin-network instrumentation.
///
/// Telcoin metrics use a `tn` prefix: either `tn_<scope>.<name>` (the `reth_metrics::Metrics`
/// derive joins scope and field with `.`) or a literal `tn_*` name from the `metrics!` macros.
fn is_tn_metric(name: &str) -> bool {
    name.starts_with("tn_") || name.starts_with("tn.")
}

/// A [`Recorder`] that prepends `reth.` to every metric that is not a `tn` metric.
///
/// The prometheus exporter sanitizes `.` to `_`, so `db.table_size` renders as
/// `reth_db_table_size` - identical to a stock reth node.
struct TnPrefixRecorder {
    /// The actual Prometheus registry every (rewritten) metric is forwarded to.
    inner: PrometheusRecorder,
}

impl TnPrefixRecorder {
    /// Rewrite a [`Key`], preserving labels.
    fn rewrite_key(&self, key: &Key) -> Option<Key> {
        if is_tn_metric(key.name()) {
            None
        } else {
            Some(Key::from_parts(format!("reth.{}", key.name()), key.labels()))
        }
    }

    /// Rewrite a [`KeyName`] (used by the `describe_*` calls).
    fn rewrite_key_name(&self, key_name: KeyName) -> KeyName {
        if is_tn_metric(key_name.as_str()) {
            key_name
        } else {
            KeyName::from(format!("reth.{}", key_name.as_str()))
        }
    }
}

impl Recorder for TnPrefixRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(self.rewrite_key_name(key_name), unit, description)
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(self.rewrite_key_name(key_name), unit, description)
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(self.rewrite_key_name(key_name), unit, description)
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        match self.rewrite_key(key) {
            Some(new_key) => self.inner.register_counter(&new_key, metadata),
            None => self.inner.register_counter(key, metadata),
        }
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        match self.rewrite_key(key) {
            Some(new_key) => self.inner.register_gauge(&new_key, metadata),
            None => self.inner.register_gauge(key, metadata),
        }
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        match self.rewrite_key(key) {
            Some(new_key) => self.inner.register_histogram(&new_key, metadata),
            None => self.inner.register_histogram(key, metadata),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::{max_batch_gas, Epoch, MAX_HEADER_NUM_OF_BATCHES};

    /// Minimum gas of a transaction (the intrinsic cost of a plain transfer).
    const MIN_TRANSACTION_GAS: u64 = 21_000;

    /// Committee size that the executor output buckets are sized for.
    const SIZING_COMMITTEE_SIZE: usize = 100;

    /// Epochs to check the batch gas cap at. A fork that raises the cap shows at the last epoch.
    const SIZING_EPOCHS: [Epoch; 2] = [0, Epoch::MAX];

    /// The one bucket layout that all three transaction histograms shared before #1511.
    const PREVIOUS_TRANSACTIONS_BUCKETS: &[f64] =
        &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0];

    /// The three bucket layouts that #1511 split out of the previous shared layout.
    const TRANSACTION_LAYOUTS: [&[f64]; 3] =
        [BATCH_TRANSACTIONS_BUCKETS, OUTPUT_BATCHES_BUCKETS, OUTPUT_TRANSACTIONS_BUCKETS];

    /// Most transactions that one batch can hold at `epoch`.
    fn max_transactions_per_batch(epoch: Epoch) -> f64 {
        u32::try_from(max_batch_gas(epoch) / MIN_TRANSACTION_GAS)
            .map(f64::from)
            .expect("per-batch transaction bound fits in u32")
    }

    /// Top bound of a bucket layout.
    fn top_bucket(buckets: &[f64]) -> f64 {
        buckets.last().copied().expect("bucket layout is not empty")
    }

    /// Pure prefix-rewrite test against a local (non-global) recorder.
    #[test]
    fn test_selective_reth_prefix() {
        let inner = build_prometheus_recorder().expect("recorder builds");
        let handle = inner.handle();
        let recorder = TnPrefixRecorder { inner };

        metrics::with_local_recorder(&recorder, || {
            // tn metrics pass through untouched
            metrics::counter!("tn_test_counter").increment(1);
            metrics::counter!("tn_worker.batches_sealed_total").increment(2);
            // everything else picks up the reth prefix
            metrics::counter!("db.fake_metric").increment(3);
            metrics::gauge!("process.fake_gauge").set(7.0);
        });

        let rendered = handle.render();
        assert!(rendered.contains("tn_test_counter 1"), "{rendered}");
        assert!(rendered.contains("tn_worker_batches_sealed_total 2"), "{rendered}");
        assert!(rendered.contains("reth_db_fake_metric 3"), "{rendered}");
        assert!(rendered.contains("reth_process_fake_gauge 7"), "{rendered}");
    }

    /// Histograms matched by the bucket configuration must render as aggregatable
    /// histograms (`_bucket` series), not quantile summaries.
    #[test]
    fn test_seconds_histograms_have_buckets() {
        let inner = build_prometheus_recorder().expect("recorder builds");
        let handle = inner.handle();
        let recorder = TnPrefixRecorder { inner };

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("tn_test.duration_seconds").record(0.3);
        });

        let rendered = handle.render();
        assert!(rendered.contains("tn_test_duration_seconds_bucket"), "{rendered}");
        assert!(rendered.contains("le=\"0.5\""), "{rendered}");
    }

    /// A full batch of minimum-gas transactions must land in a finite bucket of
    /// `tn_worker_batch_transactions`, not in `+Inf` (#1511).
    #[test]
    fn test_batch_transactions_buckets_cover_full_batch() {
        let top = top_bucket(BATCH_TRANSACTIONS_BUCKETS);
        SIZING_EPOCHS.into_iter().for_each(|epoch| {
            let max_txs = max_transactions_per_batch(epoch);
            assert!(
                top >= max_txs,
                "epoch {epoch}: top bucket {top} is below a full batch {max_txs}"
            );
        });
    }

    /// The executor output layouts must cover the sizing committee: one full certificate per
    /// authority for the batch count, and a full batch for each covered batch for the
    /// transaction count.
    #[test]
    fn test_output_buckets_cover_sizing_committee() {
        let sizing_batches =
            u32::try_from(SIZING_COMMITTEE_SIZE.saturating_mul(MAX_HEADER_NUM_OF_BATCHES))
                .map(f64::from)
                .expect("sizing batch count fits in u32");
        let top_batches = top_bucket(OUTPUT_BATCHES_BUCKETS);
        assert!(
            top_batches >= sizing_batches,
            "top batches bucket {top_batches} < {sizing_batches}"
        );

        let top_txs = top_bucket(OUTPUT_TRANSACTIONS_BUCKETS);
        SIZING_EPOCHS.into_iter().for_each(|epoch| {
            let covered = top_batches * max_transactions_per_batch(epoch);
            assert!(
                top_txs >= covered,
                "epoch {epoch}: top transactions bucket {top_txs} < {covered}"
            );
        });
    }

    /// Every new layout keeps the bounds of the previous shared layout, so an external query that
    /// pins one of them (for example `le="1000"`) still finds its series.
    #[test]
    fn test_bucket_layouts_keep_previous_bounds() {
        TRANSACTION_LAYOUTS.into_iter().for_each(|layout| {
            let missing: Vec<f64> = PREVIOUS_TRANSACTIONS_BUCKETS
                .iter()
                .copied()
                .filter(|bound| !layout.iter().any(|b| b.to_bits() == bound.to_bits()))
                .collect();
            assert!(missing.is_empty(), "layout {layout:?} drops previous bounds {missing:?}");
        });
    }

    /// Every bucket layout must be strictly ascending.
    #[test]
    fn test_bucket_layouts_strictly_ascending() {
        TRANSACTION_LAYOUTS.into_iter().for_each(|layout| {
            assert!(
                layout.windows(2).all(|pair| pair.first() < pair.last()),
                "layout {layout:?} is not strictly ascending"
            );
        });
    }

    /// Samples above the previous 1,000 cap must land in finite buckets (#1511): a full batch of
    /// 1,428 minimum-gas transfers, and an output that commits one such batch from each of four
    /// authorities (5,712 transactions).
    #[test]
    fn test_transaction_histograms_render_past_previous_cap() {
        let inner = build_prometheus_recorder().expect("recorder builds");
        let handle = inner.handle();
        let recorder = TnPrefixRecorder { inner };

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("tn_worker.batch_transactions").record(1_428.0);
            metrics::histogram!("tn_executor.output_transactions").record(5_712.0);
            metrics::histogram!("tn_executor.output_batches").record(40.0);
        });

        let rendered = handle.render();
        [
            "tn_worker_batch_transactions_bucket{le=\"1000\"} 0\n",
            "tn_worker_batch_transactions_bucket{le=\"1500\"} 1\n",
            "tn_executor_output_transactions_bucket{le=\"5000\"} 0\n",
            "tn_executor_output_transactions_bucket{le=\"10000\"} 1\n",
            "tn_executor_output_batches_bucket{le=\"25\"} 0\n",
            "tn_executor_output_batches_bucket{le=\"50\"} 1\n",
        ]
        .into_iter()
        .for_each(|line| assert!(rendered.contains(line), "missing {line:?} in {rendered}"));
    }
}
