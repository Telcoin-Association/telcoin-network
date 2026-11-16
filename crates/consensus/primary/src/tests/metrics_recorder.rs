//! Test recorder whose counters and gauges remain intact when assertions read them.

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::{
    debugging::DebugValue,
    registry::{AtomicStorage, Registry},
};
use std::sync::atomic::Ordering;

/// Test-only metric storage with non-destructive reads, matching exporter semantics.
pub(crate) struct TestMetricsRecorder {
    /// Atomic handles also returned to production metric registrations.
    registry: Registry<Key, AtomicStorage>,
}

impl TestMetricsRecorder {
    /// Create an empty recorder without installing any process-global state.
    pub(crate) fn new() -> Self {
        Self { registry: Registry::atomic() }
    }

    /// Read a counter or gauge without clearing either this metric or other metrics.
    pub(crate) fn metric(&self, name: &str) -> Option<DebugValue> {
        let key = Key::from_name(name.to_owned());
        self.registry
            .get_counter_handles()
            .get(&key)
            .map(|counter| DebugValue::Counter(counter.load(Ordering::SeqCst)))
            .or_else(|| {
                self.registry.get_gauge_handles().get(&key).map(|gauge| {
                    DebugValue::Gauge(f64::from_bits(gauge.load(Ordering::SeqCst)).into())
                })
            })
    }

    /// Whether any registered metric carries an application label.
    pub(crate) fn has_labels(&self) -> bool {
        self.registry.get_counter_handles().keys().any(|key| key.labels().next().is_some())
            || self.registry.get_gauge_handles().keys().any(|key| key.labels().next().is_some())
            || self.registry.get_histogram_handles().keys().any(|key| key.labels().next().is_some())
    }
}

impl Default for TestMetricsRecorder {
    /// Create empty atomic metric storage.
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder for TestMetricsRecorder {
    /// Descriptions do not affect assertions about recorded values.
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    /// Descriptions do not affect assertions about recorded values.
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    /// Descriptions do not affect assertions about recorded values.
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    /// Share atomic counters between metric handles and assertions.
    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.registry.get_or_create_counter(key, |counter| Counter::from_arc(counter.clone()))
    }

    /// Share atomic gauges between metric handles and assertions.
    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.registry.get_or_create_gauge(key, |gauge| Gauge::from_arc(gauge.clone()))
    }

    /// Preserve normal histogram registration even though these tests read counters and gauges.
    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |histogram| Histogram::from_arc(histogram.clone()))
    }
}
