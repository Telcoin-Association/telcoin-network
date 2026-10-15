//! Upstream readiness tracking.
//!
//! A background poller queries each upstream's `GET /health/workers` endpoint
//! on a fixed interval and records whether that worker is accepting
//! transactions. The proxy consults this state to pick a ready upstream, and
//! the gateway's own `/ready` endpoint reflects whether any upstream is ready.
//!
//! Readiness governs the worker route only. With `--redirect-queries` set,
//! every call but a submission or one of the few reads kept on the worker
//! goes to the query upstream whatever this state says.
//! The poller also sends that upstream one `eth_chainId` call per cycle,
//! alongside the worker polls and under the same thresholds, but only so
//! `/ready/any` can report whether reads can be served: there is one
//! query URL and no fallback, so its state never gates routing, and its
//! failures still show up per request (as `502`/`504`) and in the
//! routed-request metrics.
//!
//! Every failure mode (unreachable, timed out, an HTTP error status, malformed
//! payload, worker not accepting transactions or absent from the payload) is a
//! failed poll. Readiness changes only on a run of consecutive results: a ready
//! upstream turns not-ready after `--readiness-failure-threshold` failed polls
//! in a row, and a not-ready one turns ready after
//! `--readiness-success-threshold` successful polls in a row. Every upstream
//! starts not-ready, so the gateway fails closed until its first run of
//! successes, and one slow poll alone no longer drops every gateway that polls
//! the same node.
//!
//! The poll measures the node's health listener, not the worker's RPC port, so
//! the proxy also reports each worker forward here: after
//! `--upstream-failure-threshold` forwards in a row that fail to connect (a
//! connect timeout included), the upstream is marked not-ready until the poller
//! sees a fresh run of successful polls, and later requests go to the next
//! ready upstream. A timeout after the request was sent does not count, since
//! the method and params decide how long the worker takes.

use std::{
    fmt,
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    time::Duration,
};

use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tn_types::{Noticer, TaskError};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    config::UpstreamWorker,
    proxy::{chain_id_request, QueryHeader, Route, UpstreamOrigin},
};

/// Readiness envelope version the gateway targets. Newer versions still parse,
/// because unknown fields are ignored (see [`NodeReadiness`]); this is only used
/// to log a heads-up when the shape may have changed.
const READINESS_VERSION: u32 = 1;

/// Largest readiness body the poller reads, in bytes. The node's payload is a
/// few dozen bytes per worker; a larger body is a failed poll, so a broken or
/// hostile health endpoint cannot grow the gateway's memory.
const MAX_READINESS_BODY_BYTES: usize = 64 * 1024;

/// Largest probe reply the poller reads. An `eth_chainId` answer is a few dozen
/// bytes; the cap keeps a misbehaving query upstream from making the poller
/// buffer an unbounded body every cycle.
const MAX_QUERY_PROBE_REPLY_BYTES: usize = 64 * 1024;

/// Mirror of the node's per-worker readiness entry
/// (`crates/node/src/health.rs`).
#[derive(Debug, Deserialize)]
struct WorkerReadiness {
    worker_id: u16,
    accepting_transactions: bool,
}

/// Mirror of the node's versioned readiness envelope.
///
/// Extra fields are tolerated (no `deny_unknown_fields`) so the method-aware
/// routing follow-up can extend the per-worker payload without breaking the
/// gateway's parser.
#[derive(Debug, Deserialize)]
struct NodeReadiness {
    version: u32,
    workers: Vec<WorkerReadiness>,
}

/// Readiness of a single upstream, updated in place by the poller.
#[derive(Debug)]
struct UpstreamReadiness {
    worker_id: u16,
    rpc_url: Url,
    readiness_url: Url,
    /// The published state the proxy and `/ready` read. Written only while
    /// `streak` is locked, so transitions apply one at a time.
    ready: AtomicBool,
    /// The run of consecutive poll results that drives transitions.
    streak: Mutex<PollStreak>,
    /// Forwards in a row that failed to connect (a connect timeout included);
    /// reset by a forward that gets a response and, under `streak`, when the
    /// upstream turns ready. The threshold decision reads it under `streak`.
    rpc_failures: AtomicU32,
}

impl UpstreamReadiness {
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Lock the poll streak. Nothing panics while it is held, and a poisoned
    /// lock still holds plain counters, so poisoning is ignored.
    fn streak(&self) -> MutexGuard<'_, PollStreak> {
        self.streak.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Apply one poll result and publish the resulting state.
    ///
    /// A ready upstream turns not-ready only on the `thresholds.failure`-th
    /// failed poll in a row, and a not-ready one turns ready only on the
    /// `thresholds.success`-th successful poll in a row. Transitions log at
    /// info (ready) and warn (not-ready) with the cause of the last poll; the
    /// first failed poll of an upstream that has never been ready logs its
    /// cause at info once, so a gateway that never becomes ready says why at
    /// the default level. Every other failed poll logs at debug.
    fn record_poll(&self, result: Result<(), NotReadyCause>, thresholds: ReadinessThresholds) {
        self.apply_poll(&mut self.streak(), result, thresholds);
    }

    /// [`Self::record_poll`] with `streak` already locked.
    fn apply_poll(
        &self,
        streak: &mut PollStreak,
        result: Result<(), NotReadyCause>,
        thresholds: ReadinessThresholds,
    ) {
        match result {
            Ok(()) => {
                streak.failures = 0;
                streak.successes = streak.successes.saturating_add(1);
                if streak.successes >= thresholds.success.get()
                    && !self.ready.swap(true, Ordering::Relaxed)
                {
                    streak.first_failure_logged = true;
                    self.rpc_failures.store(0, Ordering::Relaxed);
                    info!(
                        target: "gateway::readiness",
                        worker_id = self.worker_id,
                        upstream = %UpstreamOrigin(&self.rpc_url),
                        readiness = %UpstreamOrigin(&self.readiness_url),
                        successes = streak.successes,
                        "upstream worker became ready"
                    );
                }
            }
            Err(cause) => {
                streak.successes = 0;
                streak.failures = streak.failures.saturating_add(1);
                debug!(
                    target: "gateway::readiness",
                    worker_id = self.worker_id,
                    readiness = %UpstreamOrigin(&self.readiness_url),
                    %cause,
                    failures = streak.failures,
                    "readiness poll failed"
                );
                if self.is_ready() {
                    if streak.failures >= thresholds.failure.get() {
                        self.ready.store(false, Ordering::Relaxed);
                        self.log_not_ready(cause);
                    }
                } else if !streak.first_failure_logged {
                    streak.first_failure_logged = true;
                    info!(
                        target: "gateway::readiness",
                        worker_id = self.worker_id,
                        upstream = %UpstreamOrigin(&self.rpc_url),
                        readiness = %UpstreamOrigin(&self.readiness_url),
                        %cause,
                        "upstream worker not ready since startup"
                    );
                }
            }
        }
        crate::telemetry::set_upstream_ready(self.worker_id, &self.rpc_url, self.is_ready());
    }

    /// Count one forward that failed to connect (a connect timeout included).
    /// On the `threshold`-th in a row the upstream is marked not-ready, and the
    /// poller must then see a fresh run of successful polls before it turns
    /// ready.
    ///
    /// The count is read again once `streak` is locked: a poll that turned the
    /// upstream ready in between reset it, and that reset wins over a failure
    /// counted before it.
    fn record_rpc_failure(&self, threshold: NonZeroU32) {
        let failures = self
            .rpc_failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_add(1))
            })
            // the update never declines, so both arms hold the previous count
            .unwrap_or_else(|count| count)
            .saturating_add(1);
        if failures < threshold.get() {
            return;
        }
        let mut streak = self.streak();
        if self.rpc_failures.load(Ordering::Relaxed) < threshold.get() {
            return;
        }
        streak.successes = 0;
        if self.ready.swap(false, Ordering::Relaxed) {
            crate::telemetry::set_upstream_ready(self.worker_id, &self.rpc_url, false);
            self.log_not_ready(NotReadyCause::RpcPath);
        }
    }

    /// Log a transition to not-ready, with its cause, at warn.
    fn log_not_ready(&self, cause: NotReadyCause) {
        warn!(
            target: "gateway::readiness",
            worker_id = self.worker_id,
            upstream = %UpstreamOrigin(&self.rpc_url),
            readiness = %UpstreamOrigin(&self.readiness_url),
            %cause,
            "upstream worker became not-ready"
        );
    }
}

/// Shared readiness view over all configured upstreams.
#[derive(Debug)]
pub(crate) struct GatewayReadiness {
    upstreams: Vec<UpstreamReadiness>,
    thresholds: ReadinessThresholds,
    /// Whether the query upstream (`--redirect-queries`) is up, published from
    /// its probe results under the same `thresholds` as a worker's polls;
    /// always `false` without a redirect, since nothing probes it.
    query_ready: AtomicBool,
    /// The run of consecutive probe results of the query upstream.
    query_streak: Mutex<PollStreak>,
}

impl GatewayReadiness {
    /// Build the readiness view, with every upstream (and the query upstream)
    /// initially not-ready (fail closed until its first run of
    /// `thresholds.success` successful polls).
    pub(crate) fn new(upstreams: &[UpstreamWorker], thresholds: ReadinessThresholds) -> Self {
        let upstreams = upstreams
            .iter()
            .map(|upstream| UpstreamReadiness {
                worker_id: upstream.worker_id,
                rpc_url: upstream.rpc_url.clone(),
                readiness_url: upstream.readiness_url.clone(),
                ready: AtomicBool::new(false),
                streak: Mutex::new(PollStreak::default()),
                rpc_failures: AtomicU32::new(0),
            })
            .collect();
        Self {
            upstreams,
            thresholds,
            query_ready: AtomicBool::new(false),
            query_streak: Mutex::new(PollStreak::default()),
        }
    }

    /// The JSON-RPC base URL of the first ready upstream, in preference order.
    pub(crate) fn first_ready_rpc_url(&self) -> Option<Url> {
        self.upstreams
            .iter()
            .find(|upstream| upstream.is_ready())
            .map(|upstream| upstream.rpc_url.clone())
    }

    /// Whether any upstream is currently ready.
    pub(crate) fn any_ready(&self) -> bool {
        self.upstreams.iter().any(UpstreamReadiness::is_ready)
    }

    /// Count a forward to the worker at `rpc_url` that failed to connect (a
    /// connect timeout included). At `--upstream-failure-threshold` failures in
    /// a row the upstream is marked not-ready (cause "rpc path") until the
    /// poller sees `--readiness-success-threshold` successful polls in a row,
    /// so later requests go to the next ready upstream. A no-op when passive
    /// health is off.
    ///
    /// A timeout after the request was sent is not reported here, since the
    /// method and params decide how long the worker takes.
    pub(crate) fn record_rpc_failure(&self, rpc_url: &Url) {
        if let Some(threshold) = self.thresholds.rpc_failure {
            self.upstreams_at(rpc_url).for_each(|upstream| upstream.record_rpc_failure(threshold));
        }
    }

    /// Reset the failure count of the worker at `rpc_url` after a forward to
    /// it got a response.
    pub(crate) fn record_rpc_success(&self, rpc_url: &Url) {
        self.upstreams_at(rpc_url)
            // skip the store when there is nothing to reset, so the common
            // path stays a read
            .filter(|upstream| upstream.rpc_failures.load(Ordering::Relaxed) != 0)
            .for_each(|upstream| upstream.rpc_failures.store(0, Ordering::Relaxed));
    }

    /// The upstreams whose JSON-RPC URL is `rpc_url`. A forward's result
    /// belongs to every entry that shares the endpoint.
    fn upstreams_at<'a>(
        &'a self,
        rpc_url: &'a Url,
    ) -> impl Iterator<Item = &'a UpstreamReadiness> + 'a {
        self.upstreams.iter().filter(move |upstream| &upstream.rpc_url == rpc_url)
    }

    /// Whether the query upstream (`--redirect-queries`) is up, after the poll
    /// thresholds. Always `false` without a redirect.
    pub(crate) fn query_ready(&self) -> bool {
        self.query_ready.load(Ordering::Relaxed)
    }

    /// Whether the gateway can serve any route: an upstream worker is ready,
    /// or the query upstream is up. Without a redirect this is
    /// [`Self::any_ready`].
    pub(crate) fn any_route_ready(&self) -> bool {
        self.any_ready() || self.query_ready()
    }

    /// Apply one probe result of the query upstream and publish the resulting
    /// state, logging a change at info with the upstream's origin only.
    ///
    /// The query upstream flips like a worker's readiness (see
    /// [`UpstreamReadiness::record_poll`]): up only on the `thresholds.success`-th
    /// answered probe in a row, down only on the `thresholds.failure`-th failed
    /// one, so one slow probe of an upstream every gateway shares does not drop
    /// them all from `/ready/any` at once.
    fn record_query_probe(&self, up: bool, url: &Url) {
        let mut streak = self.query_streak.lock().unwrap_or_else(PoisonError::into_inner);
        let ready = if up {
            streak.failures = 0;
            streak.successes = streak.successes.saturating_add(1);
            self.query_ready() || streak.successes >= self.thresholds.success.get()
        } else {
            streak.successes = 0;
            streak.failures = streak.failures.saturating_add(1);
            self.query_ready() && streak.failures < self.thresholds.failure.get()
        };
        if self.query_ready.swap(ready, Ordering::Relaxed) != ready {
            let upstream = UpstreamOrigin(url);
            if ready {
                info!(target: "gateway::readiness", %upstream, "query upstream became ready");
            } else {
                info!(target: "gateway::readiness", %upstream, "query upstream became not-ready");
            }
        }
    }

    /// Test-only: force an upstream's readiness state.
    #[cfg(test)]
    pub(crate) fn set_ready(&self, worker_id: u16, ready: bool) {
        self.upstreams.iter().filter(|upstream| upstream.worker_id == worker_id).for_each(
            |upstream| {
                let _streak = upstream.streak();
                upstream.ready.store(ready, Ordering::Relaxed);
            },
        );
    }
}

/// How many consecutive results change an upstream's readiness.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadinessThresholds {
    /// Failed polls in a row that turn a ready upstream not-ready
    /// (`--readiness-failure-threshold`).
    pub(crate) failure: NonZeroU32,
    /// Successful polls in a row that turn a not-ready upstream ready
    /// (`--readiness-success-threshold`).
    pub(crate) success: NonZeroU32,
    /// Worker forwards in a row that fail to connect (a connect timeout
    /// included) before the upstream is marked not-ready
    /// (`--upstream-failure-threshold`), or `None` when passive health is off.
    pub(crate) rpc_failure: Option<NonZeroU32>,
}

/// The query upstream (`--redirect-queries`) as the poller probes it.
#[derive(Debug)]
pub(crate) struct QueryProbe {
    /// The query upstream's URL.
    pub(crate) url: Url,
    /// The query route's client (see [`crate::proxy::route_clients`]), so the
    /// probe travels like a redirected read: same connection pool, user agent,
    /// redirect policy and resolver.
    pub(crate) client: Client,
    /// The `--redirect-queries-header` header, sent with every probe like
    /// with every redirected read, or `None`. Its `Debug` is redacted.
    pub(crate) query_header: Option<QueryHeader>,
}

/// Poll every upstream's readiness endpoint, and probe the query upstream when
/// `query_probe` is set, on `poll_interval` until `shutdown` fires. The first
/// tick runs immediately so readiness converges promptly on startup.
pub(crate) async fn run_poller(
    readiness: Arc<GatewayReadiness>,
    client: Client,
    query_probe: Option<QueryProbe>,
    poll_interval: Duration,
    poll_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let mut ticker = interval(poll_interval);
    // Skip (do not burst) missed ticks: a cycle that overruns `poll_interval`
    // (a hanging upstream with a `poll_timeout` longer than the interval) must
    // not then fire back-to-back catch-up cycles, which would pile extra load
    // onto already-failing upstreams. `Skip` keeps at least `poll_interval`
    // spacing between cycles regardless of cycle duration.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = &shutdown => break,
            _ = ticker.tick() => {
                // race the cycle against shutdown so teardown never waits on a
                // slow poll; an abandoned cycle records nothing. The query
                // upstream is probed alongside the workers, so a cycle still
                // lasts at most one `poll_timeout`.
                let query = query_probe
                    .as_ref()
                    .map(|probe| poll_query_upstream(probe, &readiness, poll_timeout));
                let cycle = futures::future::join(
                    poll_cycle(&readiness, &client, poll_timeout),
                    futures::future::OptionFuture::from(query),
                );
                tokio::select! {
                    () = &shutdown => break,
                    _ = cycle => {}
                }
            }
        }
    }
    Ok(())
}

/// Run one poll cycle: poll every upstream at once, each bounded by
/// `poll_timeout`, and apply each result as it lands. A cycle lasts at most one
/// `poll_timeout`, however many upstreams hang, so one hanging upstream cannot
/// delay the polls of the others.
///
/// [`run_poller`] runs one cycle per tick; tests call it directly to step the
/// readiness state one cycle at a time.
async fn poll_cycle(readiness: &GatewayReadiness, client: &Client, poll_timeout: Duration) {
    let polls = readiness.upstreams.iter().map(|upstream| async move {
        let result =
            poll_one(client, &upstream.readiness_url, upstream.worker_id, poll_timeout).await;
        upstream.record_poll(result, readiness.thresholds);
    });
    futures::future::join_all(polls).await;
}

/// Probe the query upstream once and record whether it is up; any failure
/// counts as down.
async fn poll_query_upstream(
    probe: &QueryProbe,
    readiness: &GatewayReadiness,
    poll_timeout: Duration,
) {
    let upstream = UpstreamOrigin(&probe.url);
    let probe_call = fetch_query_ready(&probe.client, &probe.url, probe.query_header.as_ref());
    let ready = match timeout(poll_timeout, probe_call).await {
        Ok(Ok(ready)) => {
            if !ready {
                debug!(
                    target: "gateway::readiness",
                    %upstream,
                    "query upstream did not answer the probe with a 2xx result"
                );
            }
            ready
        }
        // a reqwest error renders the full request url, which can carry a
        // credential, so it is logged without it
        Ok(Err(err)) => {
            debug!(
                target: "gateway::readiness",
                %upstream,
                err = ?err.without_url(),
                "query upstream probe failed"
            );
            false
        }
        Err(_) => {
            debug!(target: "gateway::readiness", %upstream, "query upstream probe timed out");
            false
        }
    };
    readiness.record_query_probe(ready, &probe.url);
}

/// Send the probe call, an `eth_chainId` that any node answers cheaply and
/// without side effects, and report whether the reply is a `2xx` whose JSON
/// body has a `result` member. A reply longer than
/// [`MAX_QUERY_PROBE_REPLY_BYTES`] counts as down and is not read further.
///
/// The call is stamped like a redirected read (see [`chain_id_request`]): the
/// query-route marker and, when one is configured, `query_header`, so an
/// upstream that requires the key does not read as down.
async fn fetch_query_ready(
    client: &Client,
    url: &Url,
    query_header: Option<&QueryHeader>,
) -> reqwest::Result<bool> {
    let mut response = chain_id_request(client, Route::Query, url, query_header).send().await?;
    if !response.status().is_success() {
        return Ok(false);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_QUERY_PROBE_REPLY_BYTES {
            return Ok(false);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice::<serde_json::Value>(&body)
        .is_ok_and(|reply| reply.get("result").is_some()))
}

/// Poll a single upstream: `Ok` when the worker reports it is accepting
/// transactions, otherwise the cause of the failed poll.
async fn poll_one(
    client: &Client,
    url: &Url,
    worker_id: u16,
    poll_timeout: Duration,
) -> Result<(), NotReadyCause> {
    let accepting = timeout(poll_timeout, fetch_readiness(client, url, worker_id))
        .await
        .map_err(|_elapsed| NotReadyCause::Timeout)??;
    if accepting {
        Ok(())
    } else {
        Err(NotReadyCause::NotAccepting)
    }
}

/// Fetch and parse one upstream's readiness payload, reading at most
/// [`MAX_READINESS_BODY_BYTES`] of body.
async fn fetch_readiness(
    client: &Client,
    url: &Url,
    worker_id: u16,
) -> Result<bool, NotReadyCause> {
    let mut response = client.get(url.clone()).send().await.map_err(NotReadyCause::connection)?;
    let status = response.status();
    if !status.is_success() {
        return Err(NotReadyCause::Status(status));
    }
    // refuse a declared oversized body before reading any of it, then bound
    // the read itself, since a chunked body declares no length
    if response.content_length().is_some_and(|len| len > MAX_READINESS_BODY_BYTES as u64) {
        return Err(NotReadyCause::BodyTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(NotReadyCause::connection)? {
        if body.len().saturating_add(chunk.len()) > MAX_READINESS_BODY_BYTES {
            return Err(NotReadyCause::BodyTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    parse_ready(&body, worker_id).ok_or(NotReadyCause::MalformedPayload)
}

/// Parse a readiness payload, returning `Some(accepting)` for the requested
/// worker (or `Some(false)` when it is absent), and `None` when the payload is
/// not valid JSON of the expected shape.
fn parse_ready(bytes: &[u8], worker_id: u16) -> Option<bool> {
    let readiness: NodeReadiness = serde_json::from_slice(bytes).ok()?;
    if readiness.version != READINESS_VERSION {
        debug!(
            target: "gateway::readiness",
            version = readiness.version,
            expected = READINESS_VERSION,
            "unexpected readiness envelope version; parsing best-effort"
        );
    }
    Some(
        readiness
            .workers
            .iter()
            .find(|worker| worker.worker_id == worker_id)
            .is_some_and(|worker| worker.accepting_transactions),
    )
}

/// The run of consecutive poll results for one upstream.
#[derive(Debug, Default)]
struct PollStreak {
    /// Failed polls in a row.
    failures: u32,
    /// Successful polls in a row.
    successes: u32,
    /// Set once the upstream has been ready, or once its first failed poll
    /// has been logged at info.
    first_failure_logged: bool,
}

/// Why an upstream is not ready: the cause of a failed poll, or a mark from the
/// rpc path. Rendered as the `cause` field of the readiness logs; it never
/// carries a URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotReadyCause {
    /// The poll did not finish within `--readiness-poll-timeout`.
    Timeout,
    /// The readiness endpoint answered with a non-success status.
    Status(StatusCode),
    /// The request failed below HTTP; the class names the stage (`connect`,
    /// `request` or `body`).
    Connection(&'static str),
    /// The body is not the readiness envelope.
    MalformedPayload,
    /// The body is larger than [`MAX_READINESS_BODY_BYTES`].
    BodyTooLarge,
    /// The payload reports the worker as not accepting transactions, or does
    /// not list it.
    NotAccepting,
    /// Forwards to the worker kept failing to connect (a connect timeout
    /// included).
    RpcPath,
}

impl NotReadyCause {
    /// Reduce a transport failure to its class. The error itself is not kept:
    /// its `Display` carries the request URL.
    fn connection(err: reqwest::Error) -> Self {
        let class = if err.is_connect() {
            "connect"
        } else if err.is_body() || err.is_decode() {
            "body"
        } else {
            "request"
        };
        Self::Connection(class)
    }
}

impl fmt::Display for NotReadyCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timeout"),
            Self::Status(status) => write!(f, "HTTP status {status}"),
            Self::Connection(class) => write!(f, "connection error ({class})"),
            Self::MalformedPayload => f.write_str("malformed payload"),
            Self::BodyTooLarge => f.write_str("body too large"),
            Self::NotAccepting => f.write_str("worker not accepting transactions"),
            Self::RpcPath => f.write_str("rpc path"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{proxy::UpstreamClientBuilder, telemetry::test_utils::CapturedMetrics};
    use axum::{body::Body, response::IntoResponse as _, routing::get, Router};
    use std::{io, net::SocketAddr, sync::atomic::AtomicU8, time::Instant};
    use tn_types::Notifier;
    use tokio::net::TcpListener;

    #[test]
    fn parses_ready_worker() {
        let body = br#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#;
        assert_eq!(parse_ready(body, 0), Some(true));
    }

    /// Multi-worker responses select the requested id, including an inactive nonzero worker.
    #[test]
    fn parses_nonzero_worker_readiness() {
        let body = br#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false},{"worker_id":1,"accepting_transactions":true}]}"#;
        assert_eq!(parse_ready(body, 0), Some(false));
        assert_eq!(parse_ready(body, 1), Some(true));
        assert_eq!(parse_ready(body, 2), Some(false));

        let body = br#"{"version":1,"workers":[{"worker_id":1,"accepting_transactions":false},{"worker_id":0,"accepting_transactions":true}]}"#;
        assert_eq!(parse_ready(body, 1), Some(false));
        assert_eq!(parse_ready(body, 0), Some(true));
    }

    #[test]
    fn parses_not_accepting_worker() {
        let body = br#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false}]}"#;
        assert_eq!(parse_ready(body, 0), Some(false));
    }

    #[test]
    fn absent_worker_is_not_ready() {
        let body = br#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#;
        assert_eq!(parse_ready(body, 3), Some(false));
    }

    #[test]
    fn empty_workers_is_not_ready() {
        assert_eq!(parse_ready(br#"{"version":1,"workers":[]}"#, 0), Some(false));
    }

    #[test]
    fn tolerates_unknown_fields_and_newer_versions() {
        let body = br#"{"version":2,"extra":true,"workers":[{"worker_id":0,"accepting_transactions":true,"epoch":9}]}"#;
        assert_eq!(parse_ready(body, 0), Some(true));
    }

    #[test]
    fn malformed_payload_is_none() {
        assert_eq!(parse_ready(b"not json", 0), None);
        assert_eq!(parse_ready(br#"{"version":1}"#, 0), None);
    }

    #[test]
    fn first_ready_prefers_configured_order() {
        let upstreams = vec![
            UpstreamWorker {
                worker_id: 0,
                rpc_url: Url::parse("http://127.0.0.1:8545").expect("url"),
                readiness_url: Url::parse("http://127.0.0.1:8551/health/workers").expect("url"),
            },
            UpstreamWorker {
                worker_id: 1,
                rpc_url: Url::parse("http://127.0.0.1:9545").expect("url"),
                readiness_url: Url::parse("http://127.0.0.1:9551/health/workers").expect("url"),
            },
        ];
        let readiness = GatewayReadiness::new(&upstreams, thresholds(3, 2));
        assert!(!readiness.any_ready());
        assert_eq!(readiness.first_ready_rpc_url(), None);

        readiness.set_ready(1, true);
        assert!(readiness.any_ready());
        assert_eq!(
            readiness.first_ready_rpc_url(),
            Some(Url::parse("http://127.0.0.1:9545").expect("url"))
        );

        readiness.set_ready(0, true);
        assert_eq!(
            readiness.first_ready_rpc_url(),
            Some(Url::parse("http://127.0.0.1:8545").expect("url"))
        );
    }

    /// The mock readiness endpoint answers with [`READY_BODY`].
    const READY: u8 = 0;
    /// The mock answers with [`READY_BODY`], but only after 300 ms, which
    /// overruns [`SLOW_STEP_TIMEOUT`].
    const SLOW: u8 = 1;
    /// The mock answers `500`.
    const ERROR: u8 = 2;

    /// A readiness payload listing workers 0 to 2 as accepting transactions.
    const READY_BODY: &str = r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true},{"worker_id":1,"accepting_transactions":true},{"worker_id":2,"accepting_transactions":true}]}"#;

    /// Poll timeout for a step whose mock answers at once.
    const STEP_TIMEOUT: Duration = Duration::from_secs(2);
    /// Poll timeout for a [`SLOW`] step, well under the mock's 300 ms delay.
    const SLOW_STEP_TIMEOUT: Duration = Duration::from_millis(100);

    /// Poll thresholds with passive health off.
    fn thresholds(failure: u32, success: u32) -> ReadinessThresholds {
        ReadinessThresholds {
            failure: NonZeroU32::new(failure).expect("nonzero"),
            success: NonZeroU32::new(success).expect("nonzero"),
            rpc_failure: None,
        }
    }

    /// Serve `app` on an ephemeral loopback port for the rest of the test.
    async fn serve_mock(app: Router) -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve mock") });
        addr
    }

    /// Serve a mock `GET /health/workers` whose answer follows `mode`.
    async fn mock_readiness(mode: Arc<AtomicU8>) -> SocketAddr {
        let app = Router::new().route(
            "/health/workers",
            get(move || {
                let mode = Arc::clone(&mode);
                async move {
                    match mode.load(Ordering::SeqCst) {
                        ERROR => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                        SLOW => {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            READY_BODY.into_response()
                        }
                        _ => READY_BODY.into_response(),
                    }
                }
            }),
        );
        serve_mock(app).await
    }

    /// An upstream whose readiness endpoint is served at `addr`; its rpc port
    /// is never used.
    fn upstream_at(worker_id: u16, addr: SocketAddr) -> UpstreamWorker {
        UpstreamWorker {
            worker_id,
            rpc_url: Url::parse("http://127.0.0.1:1/").expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}/health/workers")).expect("url"),
        }
    }

    /// Run `cycles` poll cycles, one after another, with the poll timeout that
    /// fits the mock's current `mode`: a healthy poll gets a generous budget,
    /// and only a [`SLOW`] poll is meant to overrun its own.
    async fn step(readiness: &GatewayReadiness, client: &Client, mode: &AtomicU8, cycles: usize) {
        let poll_timeout =
            if mode.load(Ordering::SeqCst) == SLOW { SLOW_STEP_TIMEOUT } else { STEP_TIMEOUT };
        for _ in 0..cycles {
            poll_cycle(readiness, client, poll_timeout).await;
        }
    }

    /// One upstream behind a mock, at the CLI's default thresholds (3 failures,
    /// 2 successes), stepped until it is ready. The returned mode switches
    /// what the mock answers.
    async fn ready_upstream() -> (GatewayReadiness, Client, Arc<AtomicU8>) {
        let mode = Arc::new(AtomicU8::new(READY));
        let addr = mock_readiness(Arc::clone(&mode)).await;
        let readiness = GatewayReadiness::new(&[upstream_at(0, addr)], thresholds(3, 2));
        let client = Client::new();
        step(&readiness, &client, &mode, 1).await;
        assert!(!readiness.any_ready(), "one success is below the success threshold");
        step(&readiness, &client, &mode, 1).await;
        assert!(readiness.any_ready(), "two successes in a row make the upstream ready");
        (readiness, client, mode)
    }

    #[tokio::test]
    async fn one_failed_poll_does_not_change_readiness() {
        let (readiness, client, mode) = ready_upstream().await;

        // one slow poll overruns the poll timeout, the case that used to drop
        // every gateway polling the node at once
        mode.store(SLOW, Ordering::SeqCst);
        step(&readiness, &client, &mode, 1).await;
        assert!(readiness.any_ready(), "a single slow poll must not flip readiness");
        assert!(readiness.first_ready_rpc_url().is_some());

        // failures that never reach three in a row never flip it either: a
        // success in between restarts the failure run
        for (mode_now, label) in [
            (READY, "success"),
            (ERROR, "first failure"),
            (ERROR, "second failure"),
            (READY, "success"),
            (SLOW, "slow poll"),
            (ERROR, "second failure"),
            (READY, "success"),
        ] {
            mode.store(mode_now, Ordering::SeqCst);
            step(&readiness, &client, &mode, 1).await;
            assert!(readiness.any_ready(), "readiness flipped after a {label}");
        }
    }

    #[tokio::test]
    async fn n_consecutive_failures_flip_to_not_ready() {
        let (readiness, client, mode) = ready_upstream().await;

        mode.store(ERROR, Ordering::SeqCst);
        step(&readiness, &client, &mode, 2).await;
        assert!(readiness.any_ready(), "two failures are below the failure threshold");
        step(&readiness, &client, &mode, 1).await;
        assert!(!readiness.any_ready(), "the third failure in a row flips readiness");
        assert_eq!(readiness.first_ready_rpc_url(), None);

        // further failures keep it not-ready
        step(&readiness, &client, &mode, 2).await;
        assert!(!readiness.any_ready());
    }

    #[tokio::test]
    async fn m_consecutive_successes_flip_back() {
        let (readiness, client, mode) = ready_upstream().await;
        mode.store(ERROR, Ordering::SeqCst);
        step(&readiness, &client, &mode, 3).await;
        assert!(!readiness.any_ready());

        // one success is not enough, and a failure restarts the success run
        mode.store(READY, Ordering::SeqCst);
        step(&readiness, &client, &mode, 1).await;
        assert!(!readiness.any_ready(), "one success is below the success threshold");
        mode.store(ERROR, Ordering::SeqCst);
        step(&readiness, &client, &mode, 1).await;
        mode.store(READY, Ordering::SeqCst);
        step(&readiness, &client, &mode, 1).await;
        assert!(!readiness.any_ready(), "the failure restarted the success run");

        step(&readiness, &client, &mode, 1).await;
        assert!(readiness.any_ready(), "the second success in a row flips readiness back");
    }

    /// Log output captured by a test's thread-local subscriber.
    #[derive(Clone, Debug, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn lines(&self) -> Vec<String> {
            let bytes = self.0.lock().expect("capture lock").clone();
            String::from_utf8(bytes).expect("utf8 log").lines().map(str::to_owned).collect()
        }
    }

    impl io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("capture lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn first_failure_cause_is_logged_at_info() {
        // capture what the default `info` filter shows; the current-thread test
        // runtime polls every future on this thread, so the thread-local
        // subscriber sees the poller's events
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let mode = Arc::new(AtomicU8::new(ERROR));
        let addr = mock_readiness(Arc::clone(&mode)).await;
        let readiness = GatewayReadiness::new(&[upstream_at(0, addr)], thresholds(3, 2));
        let client = Client::new();
        step(&readiness, &client, &mode, 3).await;
        assert!(!readiness.any_ready());

        // a never-ready upstream logs its first failure's cause at info, once
        let lines = captured.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        let first = &lines[0];
        assert!(first.contains(" INFO "), "{first}");
        assert!(first.contains("not ready since startup"), "{first}");
        assert!(first.contains("worker_id=0"), "{first}");
        assert!(first.contains("cause=HTTP status 500 Internal Server Error"), "{first}");

        // the transitions log too: ready at info, then not-ready at warn with
        // the last poll's cause
        mode.store(READY, Ordering::SeqCst);
        step(&readiness, &client, &mode, 2).await;
        mode.store(SLOW, Ordering::SeqCst);
        step(&readiness, &client, &mode, 3).await;
        assert!(!readiness.any_ready());
        let lines = captured.lines();
        assert_eq!(lines.len(), 3, "{lines:#?}");
        assert!(lines[1].contains(" INFO ") && lines[1].contains("became ready"), "{lines:#?}");
        assert!(
            lines[2].contains(" WARN ")
                && lines[2].contains("became not-ready")
                && lines[2].contains("cause=timeout"),
            "{lines:#?}"
        );
        assert!(lines.iter().all(|line| !line.contains("/health/workers")), "url leaked");
    }

    #[tokio::test]
    async fn rpc_path_mark_lasts_until_m_successful_polls() {
        let mode = Arc::new(AtomicU8::new(READY));
        let addr = mock_readiness(Arc::clone(&mode)).await;
        let upstreams = [upstream_at(0, addr)];
        let rpc_url = upstreams[0].rpc_url.clone();
        let readiness = GatewayReadiness::new(
            &upstreams,
            ReadinessThresholds { rpc_failure: NonZeroU32::new(2), ..thresholds(3, 2) },
        );
        let client = Client::new();
        step(&readiness, &client, &mode, 2).await;
        assert!(readiness.any_ready());

        // a forward that gets a response restarts the failure run
        readiness.record_rpc_failure(&rpc_url);
        readiness.record_rpc_success(&rpc_url);
        readiness.record_rpc_failure(&rpc_url);
        assert!(readiness.any_ready(), "the success restarted the failure run");
        readiness.record_rpc_failure(&rpc_url);
        assert!(!readiness.any_ready(), "two failures in a row mark the upstream not-ready");

        // the health listener still answers, but the poller needs a fresh run
        // of successes, not the two it had before the mark
        step(&readiness, &client, &mode, 1).await;
        assert!(!readiness.any_ready(), "one success is below the success threshold");
        step(&readiness, &client, &mode, 1).await;
        assert!(readiness.any_ready());

        // turning ready starts the failure count over
        readiness.record_rpc_failure(&rpc_url);
        assert!(readiness.any_ready());
    }

    #[test]
    fn a_failure_counted_before_recovery_does_not_unpublish_it() {
        let upstreams = [upstream_at(0, "127.0.0.1:1".parse().expect("addr"))];
        let rpc_url = &upstreams[0].rpc_url;
        let thresholds =
            ReadinessThresholds { rpc_failure: NonZeroU32::new(2), ..thresholds(3, 2) };
        let readiness = GatewayReadiness::new(&upstreams, thresholds);
        let upstream = &readiness.upstreams[0];

        // ready, marked not-ready from the rpc path, then one poll short of
        // turning ready again
        upstream.record_poll(Ok(()), thresholds);
        upstream.record_poll(Ok(()), thresholds);
        readiness.record_rpc_failure(rpc_url);
        readiness.record_rpc_failure(rpc_url);
        assert!(!readiness.any_ready(), "two failures in a row mark the upstream not-ready");
        upstream.record_poll(Ok(()), thresholds);

        // a failing forward counts itself while the final recovery poll holds
        // the streak, and only then reaches the threshold decision
        let mut streak = upstream.streak();
        std::thread::scope(|scope| {
            let late = scope.spawn(|| readiness.record_rpc_failure(rpc_url));
            while upstream.rpc_failures.load(Ordering::SeqCst) < 3 {
                std::thread::yield_now();
            }
            upstream.apply_poll(&mut streak, Ok(()), thresholds);
            assert!(upstream.is_ready(), "the second success in a row turns it ready");
            drop(streak);
            late.join().expect("late failure");
        });

        assert_eq!(upstream.rpc_failures.load(Ordering::SeqCst), 0, "recovery reset the count");
        assert!(
            readiness.any_ready(),
            "a failure counted before the recovery reset must not unpublish the upstream"
        );
    }

    #[test]
    fn rpc_failures_are_ignored_with_passive_health_off() {
        let upstreams = [upstream_at(0, "127.0.0.1:1".parse().expect("addr"))];
        let readiness = GatewayReadiness::new(&upstreams, thresholds(3, 2));
        readiness.set_ready(0, true);
        for _ in 0..10 {
            readiness.record_rpc_failure(&upstreams[0].rpc_url);
        }
        assert!(readiness.any_ready());
    }

    #[tokio::test]
    async fn readiness_polls_run_concurrently() {
        // three upstreams, each behind its own mock that takes 300 ms to answer
        let mut upstreams = Vec::new();
        for worker_id in 0..3u16 {
            let addr = mock_readiness(Arc::new(AtomicU8::new(SLOW))).await;
            upstreams.push(upstream_at(worker_id, addr));
        }
        let readiness = GatewayReadiness::new(&upstreams, thresholds(3, 1));
        let client = Client::new();

        let started = Instant::now();
        poll_cycle(&readiness, &client, Duration::from_secs(2)).await;
        let elapsed = started.elapsed();

        // polled one after another, the cycle would take at least 900 ms
        assert!(elapsed < Duration::from_millis(600), "one cycle took {elapsed:?}");
        assert!(readiness.upstreams.iter().all(UpstreamReadiness::is_ready), "a poll failed");
    }

    #[tokio::test]
    async fn oversized_readiness_body_is_a_failed_poll() {
        /// A ready payload for worker 0, padded to exactly `len` bytes, so
        /// only the size cap can fail it.
        fn padded(len: usize) -> String {
            let head =
                r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}],"pad":""#;
            let tail = r#""}"#;
            format!("{head}{}{tail}", "x".repeat(len - head.len() - tail.len()))
        }
        let over_cap = MAX_READINESS_BODY_BYTES + 1;
        let app = Router::new()
            .route("/at-cap", get(|| async { padded(MAX_READINESS_BODY_BYTES) }))
            .route("/declared", get(move || async move { padded(over_cap) }))
            .route(
                "/chunked",
                get(move || async move {
                    let chunks: Vec<Result<Vec<u8>, io::Error>> = padded(over_cap)
                        .into_bytes()
                        .chunks(4096)
                        .map(|c| Ok(c.to_vec()))
                        .collect();
                    Body::from_stream(futures::stream::iter(chunks))
                }),
            )
            .route(
                "/endless",
                get(|| async {
                    Body::from_stream(futures::stream::repeat_with(|| {
                        Ok::<_, io::Error>(vec![b' '; 4096])
                    }))
                }),
            );
        let addr = serve_mock(app).await;
        let client = Client::new();
        let url = |path: &str| Url::parse(&format!("http://{addr}{path}")).expect("url");
        let poll = |path: &'static str| {
            let client = client.clone();
            let url = url(path);
            async move { poll_one(&client, &url, 0, Duration::from_secs(2)).await }
        };

        assert_eq!(poll("/at-cap").await, Ok(()), "a body at the cap is read whole");
        // refused on its declared length
        assert_eq!(poll("/declared").await, Err(NotReadyCause::BodyTooLarge));
        // refused by the bounded read, since a chunked body declares no length
        let chunked = client.get(url("/chunked")).send().await.expect("send");
        assert_eq!(chunked.content_length(), None);
        drop(chunked);
        assert_eq!(poll("/chunked").await, Err(NotReadyCause::BodyTooLarge));
        // an endless body shows the read stops at the cap: an unbounded read
        // would run into the poll timeout instead
        assert_eq!(
            poll("/endless").await,
            Err(NotReadyCause::BodyTooLarge),
            "the read must stop at the cap"
        );
        assert_eq!(NotReadyCause::BodyTooLarge.to_string(), "body too large");

        // and the cycle counts it as a failed poll
        let upstream = UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse("http://127.0.0.1:1/").expect("rpc url"),
            readiness_url: url("/chunked"),
        };
        let readiness = GatewayReadiness::new(&[upstream], thresholds(1, 1));
        readiness.set_ready(0, true);
        poll_cycle(&readiness, &client, Duration::from_secs(2)).await;
        assert!(!readiness.any_ready(), "an oversized body must fail the poll");
    }

    /// Two nodes whose workers share id `0` publish two readiness series, told
    /// apart by the origin of each node's RPC URL, rather than overwriting one,
    /// and a readiness transition's log line names its node by that origin too.
    #[tokio::test]
    async fn upstream_ready_series_distinguish_nodes_sharing_a_worker_id() {
        let mut metrics = CapturedMetrics::install();
        // the poller runs on this test's thread, so a thread-local subscriber
        // sees its lines
        let logs = Captured::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("gateway=trace")
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _logging = tracing::subscriber::set_default(subscriber);
        // one mock serves both nodes' readiness endpoints, each on its own path
        let mock = Router::new()
            .route(
                "/a/health/workers",
                get(|| async {
                    r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false}]}"#
                }),
            )
            .route(
                "/b/health/workers",
                get(|| async {
                    r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#
                }),
            );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            // the mock lives as long as the test; a serve error only fails the
            // readiness wait below
            let _ = axum::serve(listener, mock).await;
        });
        let node = |rpc_url: &str, path: &str| UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse(rpc_url).expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}{path}")).expect("readiness url"),
        };
        // one successful poll turns a node ready, so the poller's first cycle
        // decides both
        let readiness = Arc::new(GatewayReadiness::new(
            &[
                node("http://10.0.0.1:8545/", "/a/health/workers"),
                node("http://10.0.0.2:8545/", "/b/health/workers"),
            ],
            thresholds(3, 1),
        ));

        let shutdown = Notifier::new();
        let poller = tokio::spawn(run_poller(
            Arc::clone(&readiness),
            Client::new(),
            None,
            Duration::from_secs(3600),
            Duration::from_secs(5),
            shutdown.subscribe(),
        ));
        // the cycle polls both nodes at once, so wait until each has recorded
        // its poll: the second turns ready and the first logs its first failure
        tokio::time::timeout(Duration::from_secs(5), async {
            while !(readiness.any_ready()
                && logs.lines().iter().any(|line| line.contains("not ready since startup")))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both nodes are polled");
        shutdown.notify();
        poller.await.expect("join").expect("poller");

        let labels = |upstream: &str| {
            vec![
                ("upstream".to_string(), upstream.to_string()),
                ("worker_id".to_string(), "0".to_string()),
            ]
        };
        assert_eq!(
            metrics.series("tn_worker_gateway_upstream_ready"),
            vec![(labels("http://10.0.0.1:8545"), 0.0), (labels("http://10.0.0.2:8545"), 1.0)]
        );

        // every upstream starts not-ready, so only the second node transitions
        let lines = logs.lines();
        let became_ready: Vec<_> =
            lines.iter().filter(|line| line.contains("upstream worker became ready")).collect();
        assert_eq!(became_ready.len(), 1, "one transition: {lines:#?}");
        assert!(became_ready[0].contains("upstream=http://10.0.0.2:8545"), "{lines:#?}");
        assert!(
            !lines.iter().any(|line| line.contains("became") && line.contains("10.0.0.1")),
            "the not-ready node logged a transition: {lines:#?}"
        );
    }

    #[test]
    fn query_probe_flips_at_the_poll_thresholds() {
        let readiness = GatewayReadiness::new(&[], thresholds(3, 2));
        let url = Url::parse("http://127.0.0.1:8545/").expect("url");

        readiness.record_query_probe(true, &url);
        assert!(!readiness.query_ready(), "one answered probe does not flip it up");
        readiness.record_query_probe(true, &url);
        assert!(readiness.query_ready(), "the second answered probe in a row does");

        for _ in 0..2 {
            readiness.record_query_probe(false, &url);
            assert!(readiness.query_ready(), "fewer failed probes than the threshold keep it up");
        }
        readiness.record_query_probe(true, &url);
        readiness.record_query_probe(false, &url);
        readiness.record_query_probe(false, &url);
        assert!(readiness.query_ready(), "an answered probe restarts the failure run");
        readiness.record_query_probe(false, &url);
        assert!(!readiness.query_ready(), "the third failed probe in a row flips it down");
        assert!(!readiness.any_route_ready());
    }

    #[tokio::test]
    async fn query_probe_transitions_are_logged_at_info() {
        let captured = Captured::default();
        let sink = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("gateway=debug"))
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        // a query upstream whose answer the test flips from up to down
        let up = Arc::new(AtomicBool::new(true));
        let answer = Arc::clone(&up);
        let mock = axum::Router::new().fallback(move || {
            let up = answer.load(Ordering::SeqCst);
            async move {
                if up {
                    (axum::http::StatusCode::OK, r#"{"jsonrpc":"2.0","id":1,"result":"0x7e1"}"#)
                } else {
                    (axum::http::StatusCode::SERVICE_UNAVAILABLE, "")
                }
            }
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move { axum::serve(listener, mock).await });

        // the url carries a credential in its userinfo, path and query; the
        // logs may name only its origin
        let client = crate::proxy::proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(1)),
            Duration::from_secs(2),
        )
        .expect("client");
        let probe = QueryProbe {
            url: Url::parse(&format!("http://user:s3cr3t@{addr}/k3y?token=t0k3n")).expect("url"),
            client: client.clone(),
            query_header: None,
        };
        let readiness = GatewayReadiness::new(&[], thresholds(1, 1));
        let poll_timeout = Duration::from_secs(2);

        poll_query_upstream(&probe, &readiness, poll_timeout).await;
        assert!(readiness.query_ready());
        assert!(readiness.any_route_ready());
        // a repeat of the same state is not a transition
        poll_query_upstream(&probe, &readiness, poll_timeout).await;
        up.store(false, Ordering::SeqCst);
        poll_query_upstream(&probe, &readiness, poll_timeout).await;
        assert!(!readiness.query_ready());
        assert!(!readiness.any_route_ready());

        // a transport failure is logged without the url too
        let unreachable = QueryProbe {
            url: Url::parse("http://user:s3cr3t@127.0.0.1:1/k3y?token=t0k3n").expect("url"),
            client,
            query_header: None,
        };
        poll_query_upstream(&unreachable, &readiness, poll_timeout).await;
        assert!(!readiness.query_ready());

        let logs = captured.lines().join("\n");
        let transitions: Vec<&str> =
            logs.lines().filter(|line| line.contains("query upstream became")).collect();
        assert_eq!(transitions.len(), 2, "one line per transition, none for a repeat: {logs}");
        let origin = format!("upstream=http://{addr}");
        for (line, message) in transitions
            .iter()
            .zip(["query upstream became ready", "query upstream became not-ready"])
        {
            assert!(line.trim_start().starts_with("INFO "), "not at info: {line}");
            assert!(line.contains(message), "expected `{message}`: {line}");
            assert!(line.contains(&origin), "expected `{origin}`: {line}");
        }
        assert!(logs.contains("query upstream probe failed"), "{logs}");
        for secret in ["s3cr3t", "k3y", "t0k3n"] {
            assert!(!logs.contains(secret), "`{secret}` leaked into: {logs}");
        }
    }

    /// The probe carries `--redirect-queries-header` like a redirected read, so
    /// a query upstream that refuses calls without the key is probed as up.
    #[tokio::test]
    async fn query_probe_sends_the_query_header() {
        const API_KEY: &str = "s3cr3t-k3y-1609";
        // answers only a call carrying the key exactly once, with the
        // query-route marker, and refuses anything else with a `401`
        let mock = axum::Router::new().fallback(|headers: axum::http::HeaderMap| async move {
            let keys: Vec<&[u8]> =
                headers.get_all("x-api-key").iter().map(|value| value.as_bytes()).collect();
            let marked = headers.get("x-tn-gateway-redirect").is_some_and(|value| value == "1");
            if keys == [API_KEY.as_bytes()] && marked && headers.get("x-tn-gateway").is_none() {
                (axum::http::StatusCode::OK, r#"{"jsonrpc":"2.0","id":1,"result":"0x7e1"}"#)
            } else {
                (axum::http::StatusCode::UNAUTHORIZED, "unauthorized")
            }
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move { axum::serve(listener, mock).await });

        let client = crate::proxy::proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(1)),
            Duration::from_secs(2),
        )
        .expect("client");
        let url = Url::parse(&format!("http://{addr}/")).expect("url");
        let poll_timeout = Duration::from_secs(2);

        // without the header the upstream refuses the probe: down
        let unkeyed = QueryProbe { url: url.clone(), client: client.clone(), query_header: None };
        let readiness = GatewayReadiness::new(&[], thresholds(1, 1));
        poll_query_upstream(&unkeyed, &readiness, poll_timeout).await;
        assert!(!readiness.query_ready(), "a probe without the key must be refused");

        // with it the probe is answered: up
        let header = QueryHeader::parse(&format!("X-Api-Key: {API_KEY}")).expect("valid header");
        let keyed = QueryProbe { url, client, query_header: Some(header) };
        poll_query_upstream(&keyed, &readiness, poll_timeout).await;
        assert!(readiness.query_ready(), "the probe must carry the configured header");
        assert!(
            !format!("{keyed:?}").contains(API_KEY),
            "the probe's debug output must be redacted"
        );
    }

    /// A readiness URL for `addr` whose userinfo, path and query carry secrets.
    fn secret_url(addr: SocketAddr) -> Url {
        Url::parse(&format!("http://user:s3cr3t@{addr}/k3y?token=t0k3n")).expect("url")
    }

    /// The readiness poll logs name the upstream by origin only. Three
    /// upstreams whose RPC and readiness URLs carry secrets in their userinfo,
    /// path and query are polled while nothing listens, while the node answers
    /// `500` (reqwest's status error also carries the URL), and while the node
    /// never answers; no secret may reach the debug output, and each upstream's
    /// lines still name its origin and the poll's cause.
    #[tokio::test]
    async fn readiness_logs_hide_the_url() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("gateway=debug")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let failing_addr =
            serve_mock(Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
                .await;
        // bound but never accepted: the connect completes and no answer comes
        let silent = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let silent_addr = silent.local_addr().expect("addr");
        // nothing listens on port 1
        let refused_addr: SocketAddr = "127.0.0.1:1".parse().expect("addr");

        let addrs = [refused_addr, failing_addr, silent_addr];
        let upstreams: Vec<_> = (0u16..)
            .zip(addrs)
            .map(|(worker_id, addr)| UpstreamWorker {
                worker_id,
                rpc_url: secret_url(addr),
                readiness_url: secret_url(addr),
            })
            .collect();
        let readiness = GatewayReadiness::new(&upstreams, thresholds(3, 2));
        poll_cycle(&readiness, &Client::new(), Duration::from_millis(300)).await;
        assert!(!readiness.any_ready());

        let lines = captured.lines();
        let logs = lines.join("\n");
        assert_eq!(logs.matches("readiness poll failed").count(), 3, "{logs}");
        assert_eq!(logs.matches("not ready since startup").count(), 3, "{logs}");
        for (addr, cause) in [
            (refused_addr, "cause=connection error (connect)"),
            (failing_addr, "cause=HTTP status 500 Internal Server Error"),
            (silent_addr, "cause=timeout"),
        ] {
            // the origin ends at a field boundary, so port 1 never matches a
            // longer port
            let origin = format!("http://{addr} ");
            let named: Vec<_> = lines.iter().filter(|line| line.contains(&origin)).collect();
            assert_eq!(named.len(), 2, "{addr}: {logs}");
            assert!(named.iter().all(|line| line.contains(cause)), "{addr}: {logs}");
            assert!(logs.contains(&format!("upstream={origin}")), "{addr} missing: {logs}");
            assert!(logs.contains(&format!("readiness={origin}")), "{addr} missing: {logs}");
        }
        for secret in ["user", "s3cr3t", "k3y", "t0k3n"] {
            assert!(!logs.contains(secret), "`{secret}` leaked into: {logs}");
        }
    }
}
