//! Prometheus metric vocabulary for the node-record API.
//!
//! Every name uses the `tn_node_record_api_*` scope so that, under the shared `tn-metrics`
//! recorder, the daemon's series render beside the node's own `tn_*` metrics and one
//! Prometheus/Grafana setup covers both. Instrumentation is always compiled in; without
//! `--metrics` no recorder is installed and every macro below is a cheap no-op against the
//! global noop recorder.
//!
//! The primary alert signal is `tn_node_record_api_last_successful_refresh_timestamp_seconds`:
//! a gauge that stops advancing means the DHT (or every key source) has been unreachable for
//! longer than the refresh interval, whatever the cause.

use std::time::{Duration, Instant};

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use metrics::{counter, gauge, histogram};
use tn_kad_client::KadClientError;

/// HTTP requests by matched `route` and response `status`.
const REQUESTS_TOTAL: &str = "tn_node_record_api_requests_total";

/// End-to-end HTTP request duration, in seconds. The `_seconds` suffix picks up the recorder's
/// latency histogram buckets.
const REQUEST_DURATION_SECONDS: &str = "tn_node_record_api_request_duration_seconds";

/// HTTP requests currently in flight.
const INFLIGHT_REQUESTS: &str = "tn_node_record_api_inflight_requests";

/// Refresh cycles by `outcome` (`ok`, `partial`, `failed`; see [`CycleOutcome`]).
const REFRESH_CYCLES_TOTAL: &str = "tn_node_record_api_refresh_cycles_total";

/// Unix time of the last cycle that fetched at least one record.
const LAST_SUCCESSFUL_REFRESH: &str =
    "tn_node_record_api_last_successful_refresh_timestamp_seconds";

/// Wall time of one refresh cycle, in seconds.
const REFRESH_DURATION_SECONDS: &str = "tn_node_record_api_refresh_duration_seconds";

/// Size of the tracked key set at the last cycle.
const KEYS_TRACKED: &str = "tn_node_record_api_keys_tracked";

/// Records currently cached.
const RECORDS_CACHED: &str = "tn_node_record_api_records_cached";

/// Cached records past the staleness threshold.
const RECORDS_STALE: &str = "tn_node_record_api_records_stale";

/// Cached records that advertise an RPC endpoint.
const RECORDS_WITH_RPC: &str = "tn_node_record_api_records_with_rpc";

/// Per-key lookups that produced no record, by `reason` (a `KadClientError` variant in
/// snake_case, or `not_found` for a clean miss).
const LOOKUP_FAILURES_TOTAL: &str = "tn_node_record_api_lookup_failures_total";

/// Key source refreshes that failed, by `source` (`rpc`, `committee_file`, `static`).
const KEY_SOURCE_FAILURES_TOTAL: &str = "tn_node_record_api_key_source_failures_total";

/// How a refresh cycle went, as the `outcome` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Every tracked key resolved to a record.
    Ok,
    /// Some keys resolved, others missed or failed.
    Partial,
    /// No key resolved (or the DHT client could not be spawned).
    Failed,
}

impl CycleOutcome {
    /// The metric label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
}

/// Axum middleware: count and time every matched request. Installed as a route layer so the
/// matched route template (not the raw path, which for `/v1/records/{key}` would be unbounded in
/// cardinality) labels the series.
pub async fn track_http(matched: Option<MatchedPath>, request: Request, next: Next) -> Response {
    let route = matched.map_or_else(|| "unmatched".to_string(), |path| path.as_str().to_string());
    gauge!(INFLIGHT_REQUESTS).increment(1.0);
    let start = Instant::now();
    let response = next.run(request).await;
    gauge!(INFLIGHT_REQUESTS).decrement(1.0);
    histogram!(REQUEST_DURATION_SECONDS, "route" => route.clone())
        .record(start.elapsed().as_secs_f64());
    counter!(REQUESTS_TOTAL, "route" => route, "status" => response.status().as_u16().to_string())
        .increment(1);
    response
}

/// Record one finished refresh cycle.
pub fn record_cycle(outcome: CycleOutcome, duration: Duration) {
    counter!(REFRESH_CYCLES_TOTAL, "outcome" => outcome.label()).increment(1);
    histogram!(REFRESH_DURATION_SECONDS).record(duration.as_secs_f64());
}

/// Publish the unix time of the last cycle that fetched at least one record.
pub fn set_last_successful_refresh(unix: u64) {
    // a gauge is f64; unix seconds fit exactly for the next few million years
    gauge!(LAST_SUCCESSFUL_REFRESH).set(unix as f64);
}

/// Publish the cache gauges after a cycle.
pub fn set_cache_gauges(keys_tracked: usize, cached: usize, stale: usize, with_rpc: usize) {
    gauge!(KEYS_TRACKED).set(keys_tracked as f64);
    gauge!(RECORDS_CACHED).set(cached as f64);
    gauge!(RECORDS_STALE).set(stale as f64);
    gauge!(RECORDS_WITH_RPC).set(with_rpc as f64);
}

/// Count one lookup that produced no record.
pub fn record_lookup_failure(reason: &'static str) {
    counter!(LOOKUP_FAILURES_TOTAL, "reason" => reason).increment(1);
}

/// Count one key source refresh that failed.
pub fn record_key_source_failure(source: &'static str) {
    counter!(KEY_SOURCE_FAILURES_TOTAL, "source" => source).increment(1);
}

/// The `reason` label for a failed lookup: the error's variant name in snake_case.
pub fn lookup_failure_reason(err: &KadClientError) -> &'static str {
    match err {
        KadClientError::InvalidBootstrapAddr { .. } => "invalid_bootstrap_addr",
        KadClientError::NoBootstrapPeers => "no_bootstrap_peers",
        KadClientError::NoBootstrapPeerReachable => "no_bootstrap_peer_reachable",
        KadClientError::NoPeerAnswered { .. } => "no_peer_answered",
        KadClientError::Timeout => "timeout",
        KadClientError::InvalidRecords { .. } => "invalid_records",
        KadClientError::Transport(_) => "transport",
        KadClientError::Shutdown => "shutdown",
    }
}

/// The `reason` label for a clean miss.
pub const NOT_FOUND_REASON: &str = "not_found";
