//! Upstream readiness tracking.
//!
//! A background poller queries each upstream's `GET /health/workers` endpoint
//! on a fixed interval and records whether that worker is accepting
//! transactions. The proxy consults this state to pick a ready upstream, and
//! the gateway's own `/ready` endpoint reflects whether any upstream is ready.
//!
//! Readiness governs the worker route only. With `--redirect-queries` set,
//! non-submission calls go to the query upstream whatever this state says.
//! The poller also sends that upstream one `eth_chainId` call per cycle, but
//! only so `/ready/any` can report whether reads can be served: there is one
//! query URL and no fallback, so its state never gates routing, and its
//! failures still show up per request (as `502`/`504`) and in the
//! routed-request metrics.
//!
//! Every failure mode (unreachable, timed out, malformed payload, worker absent
//! from the payload) marks the upstream not-ready, so the gateway fails closed.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::StreamExt as _;
use reqwest::{header::CONTENT_TYPE, Client};
use serde::Deserialize;
use tn_types::{Noticer, TaskError};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, info};
use url::Url;

use crate::{config::UpstreamWorker, proxy::UpstreamOrigin};

/// Readiness envelope version the gateway targets. Newer versions still parse,
/// because unknown fields are ignored (see [`NodeReadiness`]); this is only used
/// to log a heads-up when the shape may have changed.
const READINESS_VERSION: u32 = 1;

/// The call the poller sends the query upstream: any node answers it cheaply
/// and without side effects.
const QUERY_PROBE_CALL: &str = r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}"#;

/// The marker header every query-route request carries (the proxy's
/// `REDIRECT_HEADER`), so the probe reaches the query upstream the way a
/// redirected read does.
const QUERY_PROBE_MARKER: &str = "x-tn-gateway-redirect";

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
    ready: AtomicBool,
}

impl UpstreamReadiness {
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

/// Shared readiness view over all configured upstreams.
#[derive(Debug)]
pub(crate) struct GatewayReadiness {
    upstreams: Vec<UpstreamReadiness>,
    /// Whether the query upstream (`--redirect-queries`) answered its last
    /// probe; always `false` without a redirect, since nothing probes it.
    query_ready: AtomicBool,
}

impl GatewayReadiness {
    /// Build the readiness view, with every upstream initially not-ready
    /// (fail closed until the first successful poll).
    pub(crate) fn new(upstreams: &[UpstreamWorker]) -> Self {
        let upstreams = upstreams
            .iter()
            .map(|upstream| UpstreamReadiness {
                worker_id: upstream.worker_id,
                rpc_url: upstream.rpc_url.clone(),
                readiness_url: upstream.readiness_url.clone(),
                ready: AtomicBool::new(false),
            })
            .collect();
        Self { upstreams, query_ready: AtomicBool::new(false) }
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

    /// Whether the query upstream (`--redirect-queries`) answered its last
    /// probe. Always `false` without a redirect.
    pub(crate) fn query_ready(&self) -> bool {
        self.query_ready.load(Ordering::Relaxed)
    }

    /// Whether the gateway can serve any route: an upstream worker is ready,
    /// or the query upstream is up. Without a redirect this is
    /// [`Self::any_ready`].
    pub(crate) fn any_route_ready(&self) -> bool {
        self.any_ready() || self.query_ready()
    }

    /// Record the query upstream's probe result, logging a change at info
    /// with the upstream's origin only.
    fn record_query_ready(&self, ready: bool, url: &Url) {
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
        self.upstreams
            .iter()
            .filter(|upstream| upstream.worker_id == worker_id)
            .for_each(|upstream| upstream.ready.store(ready, Ordering::Relaxed));
    }
}

/// The query upstream (`--redirect-queries`) as the poller probes it.
#[derive(Debug)]
pub(crate) struct QueryProbe {
    /// The query upstream's URL.
    pub(crate) url: Url,
    /// The proxy client (see [`crate::proxy::proxy_client`]), so the probe
    /// travels like a redirected read: same connection pool, user agent and
    /// redirect policy.
    pub(crate) client: Client,
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
    // (many slow/down upstreams, each bounded by `poll_timeout`, polled
    // sequentially) must not then fire back-to-back catch-up cycles, which would
    // pile extra load onto already-failing upstreams. `Skip` keeps at least
    // `poll_interval` spacing between cycles regardless of cycle duration.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = &shutdown => break,
            _ = ticker.tick() => {
                // Poll each upstream in turn, threading a `stopped` accumulator so
                // the fold short-circuits once shutdown fires (post-shutdown
                // teardown bounded by a single `poll_timeout`, not a whole cycle).
                let client = &client;
                let shutdown = &shutdown;
                futures::stream::iter(readiness.upstreams.iter())
                    .fold(false, move |stopped, upstream| async move {
                        if stopped || shutdown.noticed() {
                            true
                        } else {
                            let ready = poll_one(
                                client,
                                &upstream.readiness_url,
                                upstream.worker_id,
                                poll_timeout,
                            )
                            .await;
                            let previous = upstream.ready.swap(ready, Ordering::Relaxed);
                            crate::telemetry::set_upstream_ready(upstream.worker_id, ready);
                            // Log transitions at an operator-visible level (the
                            // default filter is `info`); per-poll noise stays at
                            // debug. Without this, a 503 `/ready` is
                            // undiagnosable from the default logs.
                            if previous != ready {
                                if ready {
                                    tracing::info!(
                                        target: "gateway::readiness",
                                        worker_id = upstream.worker_id,
                                        "upstream worker became ready"
                                    );
                                } else {
                                    tracing::warn!(
                                        target: "gateway::readiness",
                                        worker_id = upstream.worker_id,
                                        "upstream worker became not-ready"
                                    );
                                }
                            }
                            false
                        }
                    })
                    .await;
                // the query upstream goes last, behind the same shutdown check,
                // so teardown still waits on at most one probe
                if let Some(probe) = query_probe.as_ref().filter(|_| !shutdown.noticed()) {
                    poll_query_upstream(probe, &readiness, poll_timeout).await;
                }
            }
        }
    }
    Ok(())
}

/// Probe the query upstream once and record whether it is up; any failure
/// counts as down.
async fn poll_query_upstream(
    probe: &QueryProbe,
    readiness: &GatewayReadiness,
    poll_timeout: Duration,
) {
    let upstream = UpstreamOrigin(&probe.url);
    let ready = match timeout(poll_timeout, fetch_query_ready(&probe.client, &probe.url)).await {
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
    readiness.record_query_ready(ready, &probe.url);
}

/// Send the probe call and report whether the reply is a `2xx` whose JSON body
/// has a `result` member. A reply longer than [`MAX_QUERY_PROBE_REPLY_BYTES`]
/// counts as down and is not read further.
async fn fetch_query_ready(client: &Client, url: &Url) -> reqwest::Result<bool> {
    let mut response = client
        .post(url.clone())
        .header(CONTENT_TYPE, "application/json")
        .header(QUERY_PROBE_MARKER, "1")
        .body(QUERY_PROBE_CALL)
        .send()
        .await?;
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

/// Poll a single upstream, returning `false` (not-ready) on any failure.
async fn poll_one(client: &Client, url: &Url, worker_id: u16, poll_timeout: Duration) -> bool {
    timeout(poll_timeout, fetch_readiness(client, url, worker_id))
        .await
        .inspect_err(|_| debug!(target: "gateway::readiness", %url, "readiness poll timed out"))
        .ok()
        .and_then(|result| {
            result
                .inspect_err(
                    |err| debug!(target: "gateway::readiness", %url, %err, "readiness poll failed"),
                )
                .ok()
        })
        .unwrap_or(false)
}

/// Fetch and parse one upstream's readiness payload.
async fn fetch_readiness(client: &Client, url: &Url, worker_id: u16) -> eyre::Result<bool> {
    let bytes = client.get(url.clone()).send().await?.error_for_status()?.bytes().await?;
    parse_ready(bytes.as_ref(), worker_id).ok_or_else(|| eyre::eyre!("malformed readiness payload"))
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let readiness = GatewayReadiness::new(&upstreams);
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

    /// The sink a test subscriber formats its lines into.
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("capture lock")).into_owned()
        }
    }

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
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
        let client = crate::proxy::proxy_client(Duration::from_secs(1), Duration::from_secs(2))
            .expect("client");
        let probe = QueryProbe {
            url: Url::parse(&format!("http://user:s3cr3t@{addr}/k3y?token=t0k3n")).expect("url"),
            client: client.clone(),
        };
        let readiness = GatewayReadiness::new(&[]);
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
        };
        poll_query_upstream(&unreachable, &readiness, poll_timeout).await;
        assert!(!readiness.query_ready());

        let logs = captured.text();
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
}
