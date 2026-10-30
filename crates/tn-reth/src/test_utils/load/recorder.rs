//! A global metrics recorder that keeps the raw samples of chosen histograms.
//!
//! A Prometheus histogram keeps bucket counts only, which is too coarse for a per-block tail.
//! [`CaptureRecorder`] keeps every sample of the histograms named in its allowlist, so a bench can
//! compute its own percentiles. Everything else goes to an optional inner recorder, or nowhere,
//! as on a node run without `--metrics`.
//!
//! # Names
//!
//! The allowlist matches a key's raw name, the one the metric's own crate registers. The
//! `reth_metrics::Metrics` derive joins scope and field with `.`. tn-metrics' `reth.` prefix and
//! the exporter's `.` to `_` sanitizing happen later, inside the inner recorder, so neither
//! appears in an allowlisted name. For example:
//!
//! - reth's `save_blocks` stages (`DatabaseProviderMetrics`, scope `storage.providers.database`):
//!   `storage.providers.database.save_blocks_total`,
//!   `storage.providers.database.save_blocks_write_state`,
//!   `storage.providers.database.save_blocks_commit_mdbx`.
//! - `tn-reth`'s per-block stages (`RethEnvMetrics`, scope `tn_reth`):
//!   `tn_reth.block_execute_seconds`, `tn_reth.block_state_root_seconds`.
//!
//! # Install ordering
//!
//! Reth's derive-style handles bind to the recorder that is current when they first register, and
//! both `RethEnvMetrics` and reth's provider metrics register once per process. A bench calls
//! [`CaptureRecorder::install`] before its first `RethEnv::new_database` or `RethEnv::new`. A
//! handle registered earlier records into the noop recorder for the rest of the process.

use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use parking_lot::Mutex;
use std::{collections::HashSet, fmt, sync::Arc};

/// A [`Recorder`] that keeps every sample of the allowlisted histograms.
///
/// For a histogram whose name is in the allowlist, each sample is kept and also forwarded to the
/// inner recorder. Counters, gauges, other histograms and the `describe_*` calls go to the inner
/// recorder only, or are dropped when there is none. [`CaptureRecorder::drain`] takes the kept
/// samples by name.
///
/// Registering the same key again (for example through the `metrics::histogram!` macro, which
/// registers on every call) returns a handle to the same sample store.
pub struct CaptureRecorder {
    /// The histogram names whose samples are kept.
    allow: HashSet<&'static str>,
    /// One sample store per allowlisted key (name and labels), in first-registration order.
    captured: Mutex<Vec<(Key, Arc<CapturedHistogram>)>>,
    /// The recorder that gets every metric, kept samples included.
    inner: Option<Box<dyn Recorder + Send + Sync>>,
}

impl CaptureRecorder {
    /// Create a recorder that keeps the samples of the histograms named in `allow`.
    ///
    /// `inner`, when present, receives every metric, including a copy of each kept sample. The
    /// bench passes tn-metrics' `build_recorder` here to run as a `--metrics` node does.
    pub fn new(allow: &[&'static str], inner: Option<Box<dyn Recorder + Send + Sync>>) -> Self {
        Self { allow: allow.iter().copied().collect(), captured: Mutex::default(), inner }
    }

    /// Install this recorder as the process's global recorder and return a handle to drain it.
    ///
    /// The global recorder is what reaches the rayon, blocking and tokio threads a bench's work
    /// runs on, which a local recorder does not. It must be installed before the first
    /// `RethEnv::new_database` or `RethEnv::new` in the process (see the module docs). A process
    /// has one global recorder, so a bench that installs this is its own test binary.
    ///
    /// Errors if a global recorder is already installed.
    pub fn install(self) -> eyre::Result<Arc<Self>> {
        let recorder = Arc::new(self);
        metrics::set_global_recorder(Arc::clone(&recorder))
            .map_err(|e| eyre::eyre!("failed to install the capture recorder: {e}"))?;
        Ok(recorder)
    }

    /// Take and clear every sample recorded so far under `name`.
    ///
    /// The samples of one label set are in recording order. A name registered with several label
    /// sets returns their samples concatenated, in the order the label sets first registered. A
    /// name that is not allowlisted, or has not registered yet, returns an empty vector.
    pub fn drain(&self, name: &str) -> Vec<f64> {
        let captured = self.captured.lock();
        captured
            .iter()
            .filter(|(key, _)| key.name() == name)
            .flat_map(|(_, store)| std::mem::take(&mut *store.samples.lock()))
            .collect()
    }

    /// The allowlisted names that have registered a histogram, each once, in first-registration
    /// order.
    ///
    /// An allowlisted name missing here never registered: it is misspelled, or its handle bound
    /// to another recorder before [`CaptureRecorder::install`].
    pub fn registered(&self) -> Vec<&'static str> {
        let captured = self.captured.lock();
        let mut names: Vec<&'static str> = Vec::new();
        captured.iter().filter_map(|(key, _)| self.allow.get(key.name()).copied()).for_each(
            |name| {
                if !names.contains(&name) {
                    names.push(name);
                }
            },
        );
        names
    }

    /// Register `key` with the inner recorder, or return a noop handle.
    fn inner_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        self.inner
            .as_ref()
            .map_or_else(Histogram::noop, |inner| inner.register_histogram(key, metadata))
    }
}

impl fmt::Debug for CaptureRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureRecorder")
            .field("allow", &self.allow)
            .field("inner", &self.inner.is_some())
            .finish_non_exhaustive()
    }
}

impl Recorder for CaptureRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(inner) = &self.inner {
            inner.describe_counter(key_name, unit, description);
        }
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(inner) = &self.inner {
            inner.describe_gauge(key_name, unit, description);
        }
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        if let Some(inner) = &self.inner {
            inner.describe_histogram(key_name, unit, description);
        }
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.inner
            .as_ref()
            .map_or_else(Counter::noop, |inner| inner.register_counter(key, metadata))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.as_ref().map_or_else(Gauge::noop, |inner| inner.register_gauge(key, metadata))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        if !self.allow.contains(key.name()) {
            return self.inner_histogram(key, metadata);
        }
        let mut captured = self.captured.lock();
        if let Some((_, store)) = captured.iter().find(|(existing, _)| existing == key) {
            return Histogram::from_arc(Arc::clone(store));
        }
        let store = Arc::new(CapturedHistogram {
            samples: Mutex::default(),
            inner: self.inner_histogram(key, metadata),
        });
        captured.push((key.clone(), Arc::clone(&store)));
        Histogram::from_arc(store)
    }
}

/// The sample store behind one allowlisted histogram key.
struct CapturedHistogram {
    /// Every sample recorded since the last drain, in recording order.
    samples: Mutex<Vec<f64>>,
    /// The inner recorder's handle for the same key, or a noop handle.
    inner: Histogram,
}

impl HistogramFn for CapturedHistogram {
    fn record(&self, value: f64) {
        self.samples.lock().push(value);
        self.inner.record(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// The allowlisted histogram name the tests record under.
    const KEPT: &str = "bench.kept_seconds";

    /// A histogram name that is not allowlisted.
    const OTHER: &str = "bench.other_seconds";

    /// Samples of an allowlisted histogram are kept in recording order across repeated
    /// registrations and label sets; a drain takes them and clears the store, and a handle bound
    /// before the drain keeps recording into it.
    #[test]
    fn test_capture_recorder_keeps_and_drains_allowlisted_samples() {
        let recorder = CaptureRecorder::new(&[KEPT], None);

        let bound = metrics::with_local_recorder(&recorder, || {
            metrics::histogram!(KEPT).record(1.0);
            metrics::histogram!(KEPT).record(2.0);
            metrics::histogram!(KEPT, "stage" => "b").record(4.0);
            let bound = metrics::histogram!(KEPT);
            bound.record(3.0);
            bound
        });

        assert_eq!(recorder.registered(), vec![KEPT]);
        assert_eq!(recorder.drain(KEPT), vec![1.0, 2.0, 3.0, 4.0]);
        assert!(recorder.drain(KEPT).is_empty(), "a drain clears the samples");

        bound.record(5.0);
        assert_eq!(recorder.drain(KEPT), vec![5.0]);
    }

    /// Without an inner recorder, everything that is not an allowlisted histogram is a noop:
    /// nothing is kept for it and nothing registers.
    #[test]
    fn test_capture_recorder_drops_unlisted_metrics_without_inner() {
        let recorder = CaptureRecorder::new(&[KEPT], None);

        metrics::with_local_recorder(&recorder, || {
            metrics::describe_histogram!(OTHER, "not kept");
            metrics::histogram!(OTHER).record(1.0);
            metrics::counter!(OTHER).increment(2);
            metrics::gauge!(OTHER).set(3.0);
        });

        assert!(recorder.registered().is_empty(), "{:?}", recorder.registered());
        assert!(recorder.drain(OTHER).is_empty());
        assert!(recorder.drain(KEPT).is_empty());
    }

    /// With an inner recorder, kept samples reach both the store and the inner recorder, and
    /// everything else (unlisted histograms, counters, gauges, descriptions) reaches the inner
    /// recorder only.
    #[test]
    fn test_capture_recorder_forwards_to_inner() {
        let inner = DebuggingRecorder::new();
        let snapshotter = inner.snapshotter();
        let recorder = CaptureRecorder::new(&[KEPT], Some(Box::new(inner)));

        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!("bench.events_total", "events seen");
            metrics::histogram!(KEPT).record(1.0);
            metrics::histogram!(KEPT).record(2.0);
            metrics::histogram!(OTHER).record(3.0);
            metrics::counter!("bench.events_total").increment(4);
            metrics::gauge!("bench.depth").set(5.0);
        });

        assert_eq!(recorder.drain(KEPT), vec![1.0, 2.0]);
        assert!(recorder.drain(OTHER).is_empty(), "an unlisted histogram is not kept");

        let snapshot = snapshotter.snapshot().into_vec();
        let entry = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(_, _, description, value)| (description, value))
                .unwrap_or_else(|| panic!("{name} did not reach the inner recorder: {snapshot:?}"))
        };
        let samples = |name: &str| match entry(name).1 {
            DebugValue::Histogram(samples) => {
                samples.iter().map(|sample| sample.into_inner()).collect::<Vec<f64>>()
            }
            other => panic!("{name} is not a histogram: {other:?}"),
        };
        assert_eq!(samples(KEPT), vec![1.0, 2.0]);
        assert_eq!(samples(OTHER), vec![3.0]);
        let (description, events) = entry("bench.events_total");
        assert_eq!(description.as_deref(), Some("events seen"));
        assert_eq!(events, &DebugValue::Counter(4));
        assert_eq!(entry("bench.depth").1, &DebugValue::Gauge(5.0.into()));
    }
}
