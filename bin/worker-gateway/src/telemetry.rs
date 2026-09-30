//! Prometheus metric vocabulary for the worker gateway.
//!
//! Every name uses the `tn_worker_gateway_*` scope so that, under the shared
//! `tn-metrics` recorder, the gateway's series render beside the node's own
//! `tn_*` metrics and one Prometheus/Grafana setup covers both. Instrumentation
//! is always compiled in; when the gateway runs without `--metrics` no recorder
//! is installed and every macro below is a cheap no-op against the global noop
//! recorder.
//!
//! Two counters partition every proxied request:
//!
//! - [`record_forwarded`] bumps `tn_worker_gateway_requests_total{outcome="forwarded"}` for a
//!   request handed to an upstream worker;
//! - [`record_rejection`] bumps `tn_worker_gateway_requests_total{outcome="rejected"}` plus
//!   `tn_worker_gateway_rejections_total{reason=...}` for a request the gateway answered with a
//!   JSON-RPC error.
//!
//! Their sum is the total proxied-request count, and `rejections_total` breaks
//! the rejected side down by reason. The gateway's own `/health` and `/ready`
//! probes are not proxied and are deliberately not counted, so the in-flight
//! gauge and request counters reflect real client load only.

use std::time::Instant;

use metrics::{counter, gauge, histogram};

/// Concurrent in-flight proxied requests. This is the intended
/// horizontal-autoscaling signal: unlike CPU it tracks queueing and latency
/// pressure directly, so it still climbs while a slow upstream leaves the
/// gateway's own CPU idle.
const INFLIGHT_REQUESTS: &str = "tn_worker_gateway_inflight_requests";

/// Proxied requests by terminal `outcome` (`forwarded` or `rejected`).
const REQUESTS_TOTAL: &str = "tn_worker_gateway_requests_total";

/// Rejected proxied requests by `reason` (the `GatewayError` reason label).
const REJECTIONS_TOTAL: &str = "tn_worker_gateway_rejections_total";

/// End-to-end proxied-request duration, in seconds. The `_seconds` suffix picks
/// up the recorder's latency histogram buckets.
const REQUEST_DURATION_SECONDS: &str = "tn_worker_gateway_request_duration_seconds";

/// Per-worker upstream readiness as last seen by the poller (`1` ready, `0`
/// not-ready), labelled by `worker_id`.
const UPSTREAM_READY: &str = "tn_worker_gateway_upstream_ready";

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

/// Record a request forwarded to an upstream worker (a terminal success).
pub(crate) fn record_forwarded() {
    counter!(REQUESTS_TOTAL, "outcome" => "forwarded").increment(1);
}

/// Record a request the gateway answered with a JSON-RPC error, keyed by a
/// stable machine-readable `reason`.
pub(crate) fn record_rejection(reason: &'static str) {
    counter!(REQUESTS_TOTAL, "outcome" => "rejected").increment(1);
    counter!(REJECTIONS_TOTAL, "reason" => reason).increment(1);
}

/// Publish a worker's current readiness as a `0`/`1` gauge.
pub(crate) fn set_upstream_ready(worker_id: u16, ready: bool) {
    gauge!(UPSTREAM_READY, "worker_id" => worker_id.to_string()).set(if ready { 1.0 } else { 0.0 });
}
