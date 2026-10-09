//! Upstream readiness tracking.
//!
//! A background poller queries each upstream's `GET /health/workers` endpoint
//! on a fixed interval and records whether that worker is accepting
//! transactions. The proxy consults this state to pick a ready upstream, and
//! the gateway's own `/ready` endpoint reflects whether any upstream is ready.
//!
//! Readiness governs the worker route only. With `--redirect-queries` set,
//! non-submission calls go to the query upstream whatever this state says, and
//! that upstream is never probed: there is one query URL and no fallback, so a
//! probe would have nothing to fail over to, while N gateways polling a shared
//! public endpoint would add load to it. Its failures show up per request (as
//! `502`/`504`) and in the routed-request metrics instead.
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

use crate::{config::UpstreamWorker, proxy::UpstreamOrigin};

/// Readiness envelope version the gateway targets. Newer versions still parse,
/// because unknown fields are ignored (see [`NodeReadiness`]); this is only used
/// to log a heads-up when the shape may have changed.
const READINESS_VERSION: u32 = 1;

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
    /// reset by a forward that gets a response and when the upstream turns
    /// ready.
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
        let mut streak = self.streak();
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
        crate::telemetry::set_upstream_ready(self.worker_id, self.is_ready());
    }

    /// Count one forward that failed to connect (a connect timeout included).
    /// On the `threshold`-th in a row the upstream is marked not-ready, and the
    /// poller must then see a fresh run of successful polls before it turns
    /// ready.
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
        streak.successes = 0;
        if self.ready.swap(false, Ordering::Relaxed) {
            crate::telemetry::set_upstream_ready(self.worker_id, false);
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
}

impl GatewayReadiness {
    /// Build the readiness view, with every upstream initially not-ready
    /// (fail closed until its first run of `thresholds.success` successful
    /// polls).
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
        Self { upstreams, thresholds }
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

/// Poll every upstream's readiness endpoint on `poll_interval` until `shutdown`
/// fires. The first tick runs immediately so readiness converges promptly on
/// startup.
pub(crate) async fn run_poller(
    readiness: Arc<GatewayReadiness>,
    client: Client,
    poll_interval: Duration,
    poll_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let mut ticker = interval(poll_interval);
    // Skip (do not burst) missed ticks: a cycle that overruns `poll_interval`
    // (many slow/down upstreams, each bounded by `poll_timeout`, polled
    // sequentially) must not then fire back-to-back catch-up cycles, which would
    // pile extra load onto already-failing upstreams. `Skip` keeps at least
    // `poll_interval` spacing between cycles regardless of cycle duration.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = &shutdown => break,
            _ = ticker.tick() => {
                // race the cycle against shutdown so teardown never waits on a
                // slow poll; an abandoned cycle records nothing
                tokio::select! {
                    () = &shutdown => break,
                    () = poll_cycle(&readiness, &client, poll_timeout) => {}
                }
            }
        }
    }
    Ok(())
}

/// Run one poll cycle: poll every upstream once and apply each result.
///
/// [`run_poller`] runs one cycle per tick; tests call it directly to step the
/// readiness state one cycle at a time.
async fn poll_cycle(readiness: &GatewayReadiness, client: &Client, poll_timeout: Duration) {
    for upstream in &readiness.upstreams {
        let result =
            poll_one(client, &upstream.readiness_url, upstream.worker_id, poll_timeout).await;
        upstream.record_poll(result, readiness.thresholds);
    }
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

/// Fetch and parse one upstream's readiness payload.
async fn fetch_readiness(
    client: &Client,
    url: &Url,
    worker_id: u16,
) -> Result<bool, NotReadyCause> {
    let response = client.get(url.clone()).send().await.map_err(NotReadyCause::connection)?;
    let status = response.status();
    if !status.is_success() {
        return Err(NotReadyCause::Status(status));
    }
    let bytes = response.bytes().await.map_err(NotReadyCause::connection)?;
    parse_ready(bytes.as_ref(), worker_id).ok_or(NotReadyCause::MalformedPayload)
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
            .map(|worker| worker.accepting_transactions)
            .unwrap_or(false),
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
            Self::NotAccepting => f.write_str("worker not accepting transactions"),
            Self::RpcPath => f.write_str("rpc path"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{response::IntoResponse as _, routing::get, Router};
    use std::{io, net::SocketAddr, sync::atomic::AtomicU8};
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
    fn rpc_failures_are_ignored_with_passive_health_off() {
        let upstreams = [upstream_at(0, "127.0.0.1:1".parse().expect("addr"))];
        let readiness = GatewayReadiness::new(&upstreams, thresholds(3, 2));
        readiness.set_ready(0, true);
        for _ in 0..10 {
            readiness.record_rpc_failure(&upstreams[0].rpc_url);
        }
        assert!(readiness.any_ready());
    }
}
