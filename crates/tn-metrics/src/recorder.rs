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

/// Buckets for transaction counts per batch/output.
const TRANSACTIONS_BUCKETS: &[f64] =
    &[1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0];

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
            TRANSACTIONS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("tn_executor_output_transactions".to_string()),
            TRANSACTIONS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("tn_executor_output_batches".to_string()),
            TRANSACTIONS_BUCKETS,
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
}
