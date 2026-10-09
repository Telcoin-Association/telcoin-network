//! Prometheus metric vocabulary for the worker gateway.
//!
//! Every name uses the `tn_worker_gateway_*` scope so that, under the shared
//! `tn-metrics` recorder, the gateway's series render beside the node's own
//! `tn_*` metrics and one Prometheus/Grafana setup covers both. Instrumentation
//! is always compiled in; when the gateway runs without `--metrics` no recorder
//! is installed and every macro below is a cheap no-op against the global noop
//! recorder.
//!
//! Three recorders partition every proxied request:
//!
//! - [`record_forwarded`] bumps `tn_worker_gateway_requests_total{outcome="forwarded"}` for a
//!   request handed to an upstream (a worker, or the `--redirect-queries` endpoint) whose answer is
//!   relayed;
//! - [`record_rejection`] bumps `tn_worker_gateway_requests_total{outcome="rejected"}` plus
//!   `tn_worker_gateway_rejections_total{reason=...}` for a request the gateway answered with a
//!   JSON-RPC error;
//! - [`record_upstream_error`] bumps `tn_worker_gateway_requests_total{outcome="upstream_error"}`
//!   plus `tn_worker_gateway_rejections_total{reason="upstream_error"}` for a request whose
//!   upstream answered an error status without a JSON body, which the gateway replaced with a
//!   JSON-RPC error.
//!
//! Their sum is the total proxied-request count, and `rejections_total` breaks
//! the non-forwarded side down by reason. The gateway's own `/health` and `/ready`
//! probes are not proxied and are deliberately not counted, so the in-flight
//! gauge and request counters reflect real client load only.
//!
//! [`record_routed`] counts every forward attempt by route and result, which
//! splits the load between the worker and the query upstream (a relayed body
//! that fails mid-stream is counted again, as `body_failed`), and
//! [`record_mixed_batch`] counts the batches sent to the query upstream only
//! because they mixed submissions with other calls.

use std::time::Instant;

use metrics::{counter, gauge, histogram};

/// Concurrent in-flight proxied requests. This is the intended
/// horizontal-autoscaling signal: unlike CPU it tracks queueing and latency
/// pressure directly, so it still climbs while a slow upstream leaves the
/// gateway's own CPU idle.
const INFLIGHT_REQUESTS: &str = "tn_worker_gateway_inflight_requests";

/// Proxied requests by terminal `outcome` (`forwarded`, `rejected` or
/// `upstream_error`).
const REQUESTS_TOTAL: &str = "tn_worker_gateway_requests_total";

/// Rejected and upstream-error proxied requests by `reason` (the
/// `GatewayError` reason label).
const REJECTIONS_TOTAL: &str = "tn_worker_gateway_rejections_total";

/// End-to-end proxied-request duration, in seconds. The `_seconds` suffix picks
/// up the recorder's latency histogram buckets.
const REQUEST_DURATION_SECONDS: &str = "tn_worker_gateway_request_duration_seconds";

/// Per-worker upstream readiness as last seen by the poller (`1` ready, `0`
/// not-ready), labelled by `worker_id`.
const UPSTREAM_READY: &str = "tn_worker_gateway_upstream_ready";

/// Forward attempts by `route` (`worker` or `query`) and `result`
/// (`forwarded`, `upstream_error`, `unreachable` or `timeout`), plus
/// `body_failed` for a forwarded response whose body then failed mid-stream.
const ROUTED_REQUESTS_TOTAL: &str = "tn_worker_gateway_routed_requests_total";

/// Batches sent whole to the query upstream because they mixed submissions
/// with other calls.
const MIXED_BATCHES_TOTAL: &str = "tn_worker_gateway_mixed_batches_total";

/// RAII guard covering one proxied request.
///
/// Entering bumps the in-flight gauge and starts the duration timer; dropping
/// releases the gauge and records the elapsed duration. Because the guard is
/// held by value for the whole handler, every exit path is covered by one
/// decrement, including a mid-flight cancel when the request-timeout layer
/// aborts the handler future (the guard is dropped as the future unwinds).
pub(crate) struct RequestInFlight {
    /// When the request entered the proxy handler.
    start: Instant,
}

impl RequestInFlight {
    /// Enter the proxy handler: increment the in-flight gauge and start timing.
    pub(crate) fn enter() -> Self {
        gauge!(INFLIGHT_REQUESTS).increment(1.0);
        Self { start: Instant::now() }
    }
}

impl Drop for RequestInFlight {
    fn drop(&mut self) {
        gauge!(INFLIGHT_REQUESTS).decrement(1.0);
        histogram!(REQUEST_DURATION_SECONDS).record(self.start.elapsed().as_secs_f64());
    }
}

/// Record a request forwarded to an upstream (a terminal success).
pub(crate) fn record_forwarded() {
    counter!(REQUESTS_TOTAL, "outcome" => "forwarded").increment(1);
}

/// Record a request the gateway answered with a JSON-RPC error, keyed by a
/// stable machine-readable `reason`.
pub(crate) fn record_rejection(reason: &'static str) {
    counter!(REQUESTS_TOTAL, "outcome" => "rejected").increment(1);
    counter!(REJECTIONS_TOTAL, "reason" => reason).increment(1);
}

/// Record a request whose upstream answered an error status without a JSON
/// body, which the gateway replaced with a JSON-RPC error. It is kept apart
/// from [`record_rejection`]'s `rejected` outcome because the gateway did
/// forward it: the upstream refused it.
pub(crate) fn record_upstream_error(reason: &'static str) {
    counter!(REQUESTS_TOTAL, "outcome" => "upstream_error").increment(1);
    counter!(REJECTIONS_TOTAL, "reason" => reason).increment(1);
}

/// Record one forward attempt on `route` (`worker` or `query`) with its
/// `result` (`forwarded`, `upstream_error`, `unreachable` or `timeout`), or a
/// forwarded response whose body then failed (`body_failed`).
pub(crate) fn record_routed(route: &'static str, result: &'static str) {
    counter!(ROUTED_REQUESTS_TOTAL, "route" => route, "result" => result).increment(1);
}

/// Record a batch sent whole to the query upstream because it mixed
/// submissions with other calls.
pub(crate) fn record_mixed_batch() {
    counter!(MIXED_BATCHES_TOTAL).increment(1);
}

/// Publish a worker's current readiness as a `0`/`1` gauge.
pub(crate) fn set_upstream_ready(worker_id: u16, ready: bool) {
    gauge!(UPSTREAM_READY, "worker_id" => worker_id.to_string()).set(if ready { 1.0 } else { 0.0 });
}

/// Test support: capture the metrics the code under test records.
#[cfg(test)]
pub(crate) mod test_utils {
    use std::collections::BTreeMap;

    use metrics::LocalRecorderGuard;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    /// One series: its name and its sorted `(label, value)` pairs.
    type SeriesKey = (String, Vec<(String, String)>);

    /// Records every metric emitted on the current thread into a
    /// [`DebuggingRecorder`] for as long as it lives.
    ///
    /// The recorder is installed thread-locally, not globally, so tests stay
    /// independent. A `#[tokio::test]` runs on a current-thread runtime, so the
    /// gateway, mock upstreams and client spawned by a test all run on the test's
    /// thread and record here.
    ///
    /// A snapshot drains the recorder (counters and gauges reset to zero), so
    /// every read accumulates the new snapshot into running totals first. That
    /// makes a counter's total its whole count and an increment/decrement
    /// gauge's total its current value; a `set` gauge is only meaningful when it
    /// is read once after the code under test sets it.
    pub(crate) struct CapturedMetrics {
        /// Drains the recorder.
        snapshotter: Snapshotter,
        /// Keeps the recorder installed on this thread.
        _guard: LocalRecorderGuard<'static>,
        /// Every series seen so far, with its accumulated value.
        totals: BTreeMap<SeriesKey, f64>,
    }

    impl CapturedMetrics {
        /// Install a fresh recorder on the current thread.
        pub(crate) fn install() -> Self {
            // leaked so the thread-local guard can borrow it for the test's
            // lifetime; each test runs in its own process under nextest
            let recorder: &'static DebuggingRecorder =
                Box::leak(Box::new(DebuggingRecorder::new()));
            let snapshotter = recorder.snapshotter();
            let guard = metrics::set_default_local_recorder(recorder);
            Self { snapshotter, _guard: guard, totals: BTreeMap::new() }
        }

        /// Fold a fresh snapshot into the running totals.
        fn accumulate(&mut self) {
            for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
                let mut labels: Vec<_> = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                labels.sort();
                let delta = match value {
                    // counts in a test stay far below 2^53, so the cast is exact
                    DebugValue::Counter(count) => count as f64,
                    DebugValue::Gauge(value) => value.into_inner(),
                    DebugValue::Histogram(values) => values.len() as f64,
                };
                *self.totals.entry((key.key().name().to_string(), labels)).or_default() += delta;
            }
        }

        /// The accumulated value of the series `name{labels}`, matching the
        /// label set exactly, or `0` if it was never recorded. A histogram reads
        /// as its number of samples.
        pub(crate) fn value(&mut self, name: &str, labels: &[(&str, &str)]) -> f64 {
            self.accumulate();
            let mut labels: Vec<_> = labels
                .iter()
                .map(|(label, value)| ((*label).to_string(), (*value).to_string()))
                .collect();
            labels.sort();
            self.totals.get(&(name.to_string(), labels)).copied().unwrap_or_default()
        }
    }
}
