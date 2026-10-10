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
//!   request handed to an upstream (a worker, or the `--redirect-queries` endpoint);
//! - [`record_rejection`] bumps `tn_worker_gateway_requests_total{outcome="rejected"}` plus
//!   `tn_worker_gateway_rejections_total{reason=...}` for a request the gateway answered with a
//!   JSON-RPC error.
//!
//! Their sum is the total proxied-request count, and `rejections_total` breaks
//! the rejected side down by reason. The gateway's own `/health` and `/ready`
//! probes are not proxied and are deliberately not counted, so the in-flight
//! gauge and request counters reflect real client load only.
//!
//! [`record_routed`] counts every forward attempt by route and result, which
//! splits the load between the worker and the query upstream, and
//! [`record_mixed_batch`] counts the batches sent to the query upstream only
//! because they mixed submissions with other calls.
//!
//! [`RequestBytesHeld`] keeps a gauge of the request-body bytes held against
//! the in-flight byte budget (`--max-inflight-request-bytes`).

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

/// Forward attempts by `route` (`worker` or `query`) and `result`
/// (`forwarded`, `unreachable` or `timeout`).
const ROUTED_REQUESTS_TOTAL: &str = "tn_worker_gateway_routed_requests_total";

/// Batches sent whole to the query upstream because they mixed submissions
/// with other calls.
const MIXED_BATCHES_TOTAL: &str = "tn_worker_gateway_mixed_batches_total";

/// Request-body bytes currently held against the in-flight byte budget
/// (`--max-inflight-request-bytes`): what admitted requests reserved before
/// their bodies were read, until they are forwarded or rejected.
const INFLIGHT_REQUEST_BYTES: &str = "tn_worker_gateway_inflight_request_bytes";

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

/// RAII guard covering the bytes one request holds against the in-flight byte
/// budget.
///
/// Entering raises the held-bytes gauge by the reservation; dropping lowers it
/// by the same amount, so every exit path (forwarded, rejected, timed out, or
/// cancelled by a client disconnect) is covered by one decrement.
#[derive(Debug)]
pub(crate) struct RequestBytesHeld {
    /// The reservation this guard accounts for, in bytes.
    bytes: u32,
}

impl RequestBytesHeld {
    /// Account for `bytes` newly held against the budget.
    pub(crate) fn enter(bytes: u32) -> Self {
        gauge!(INFLIGHT_REQUEST_BYTES).increment(f64::from(bytes));
        Self { bytes }
    }
}

impl Drop for RequestBytesHeld {
    fn drop(&mut self) {
        gauge!(INFLIGHT_REQUEST_BYTES).decrement(f64::from(self.bytes));
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

/// Record one forward attempt on `route` (`worker` or `query`) with its
/// `result` (`forwarded`, `unreachable` or `timeout`).
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
