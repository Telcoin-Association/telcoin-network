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
//! [`InFlightSlot`] pairs a held in-flight permit with the gauge that counts
//! it, so `tn_worker_gateway_route_inflight{route}` always equals the permits
//! held on that route's cap.

use std::{sync::Arc, time::Instant};

use metrics::{counter, gauge, histogram, Gauge};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

/// Requests holding a slot on their class's in-flight cap, by `route`
/// (`submission` or `query`).
const ROUTE_INFLIGHT: &str = "tn_worker_gateway_route_inflight";

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

/// RAII guard for one held in-flight slot: the semaphore permit plus the gauge
/// that counts it.
///
/// The gauge is raised only once the permit is held and lowered when the guard
/// drops, which also returns the permit, so the gauge cannot drift from the
/// permits actually held. A request refused for want of a slot never touches
/// the gauge.
pub(crate) struct InFlightSlot {
    /// The held permit; dropping it frees the slot.
    _permit: OwnedSemaphorePermit,
    /// The gauge counting this slot.
    gauge: Gauge,
}

impl InFlightSlot {
    /// Take a slot on the in-flight cap of route class `route` (`submission`
    /// or `query`) without waiting, or `None` when every slot is taken.
    pub(crate) fn route(slots: &Arc<Semaphore>, route: &'static str) -> Option<Self> {
        Self::try_acquire(slots, || gauge!(ROUTE_INFLIGHT, "route" => route))
    }

    /// Take a permit from `slots` without waiting and raise the gauge `gauge`
    /// builds, or `None` (gauge untouched) when no permit is free.
    fn try_acquire(slots: &Arc<Semaphore>, gauge: impl FnOnce() -> Gauge) -> Option<Self> {
        let permit = Arc::clone(slots).try_acquire_owned().ok()?;
        let gauge = gauge();
        gauge.increment(1.0);
        Some(Self { _permit: permit, gauge })
    }
}

impl Drop for InFlightSlot {
    fn drop(&mut self) {
        self.gauge.decrement(1.0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{
        Counter, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };
    use std::{collections::BTreeMap, sync::Mutex};

    /// A counting recorder: it keeps the current value of every gauge, keyed
    /// by name and labels, and discards every other metric.
    #[derive(Clone, Default)]
    struct GaugeTally(Arc<Mutex<BTreeMap<String, f64>>>);

    impl GaugeTally {
        /// The current value of the gauge rendered as `key` (0 when unseen).
        fn value(&self, key: &str) -> f64 {
            self.0.lock().expect("tally lock").get(key).copied().unwrap_or_default()
        }

        /// Apply `update` to the gauge rendered as `key`.
        fn update(&self, key: &str, update: impl FnOnce(&mut f64)) {
            update(self.0.lock().expect("tally lock").entry(key.to_string()).or_default());
        }
    }

    /// One gauge's handle into a [`GaugeTally`].
    struct TalliedGauge {
        key: String,
        tally: GaugeTally,
    }

    impl GaugeFn for TalliedGauge {
        fn increment(&self, value: f64) {
            self.tally.update(&self.key, |gauge| *gauge += value);
        }

        fn decrement(&self, value: f64) {
            self.tally.update(&self.key, |gauge| *gauge -= value);
        }

        fn set(&self, value: f64) {
            self.tally.update(&self.key, |gauge| *gauge = value);
        }
    }

    impl Recorder for GaugeTally {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            let key = key.labels().fold(key.name().to_string(), |rendered, label| {
                format!("{rendered}{{{}={}}}", label.key(), label.value())
            });
            Gauge::from_arc(Arc::new(TalliedGauge { key, tally: self.clone() }))
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    const QUERY: &str = "tn_worker_gateway_route_inflight{route=query}";
    const SUBMISSION: &str = "tn_worker_gateway_route_inflight{route=submission}";

    #[test]
    fn route_inflight_gauges_follow_held_permits() {
        let tally = GaugeTally::default();
        metrics::with_local_recorder(&tally, || {
            let query_slots = Arc::new(Semaphore::new(2));
            let submission_slots = Arc::new(Semaphore::new(1));

            let first = InFlightSlot::route(&query_slots, "query").expect("first query slot");
            let second = InFlightSlot::route(&query_slots, "query").expect("second query slot");
            assert_eq!((tally.value(QUERY), query_slots.available_permits()), (2.0, 0));

            // a full cap refuses at once and leaves the gauge alone
            assert!(InFlightSlot::route(&query_slots, "query").is_none());
            assert_eq!(tally.value(QUERY), 2.0);

            // each class counts on its own label and its own permits
            let submission =
                InFlightSlot::route(&submission_slots, "submission").expect("submission slot");
            assert_eq!((tally.value(SUBMISSION), tally.value(QUERY)), (1.0, 2.0));

            // dropping a slot lowers the gauge and returns the permit together
            drop(first);
            assert_eq!((tally.value(QUERY), query_slots.available_permits()), (1.0, 1));
            drop(second);
            drop(submission);
            assert_eq!((tally.value(QUERY), tally.value(SUBMISSION)), (0.0, 0.0));
            assert_eq!(
                (query_slots.available_permits(), submission_slots.available_permits()),
                (2, 1)
            );
        });
    }
}
