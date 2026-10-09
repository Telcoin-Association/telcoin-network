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
use reqwest::Client;
use serde::Deserialize;
use tn_types::{Noticer, TaskError};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::debug;
use url::Url;

use crate::config::UpstreamWorker;

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
        Self { upstreams }
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

    /// Test-only: force an upstream's readiness state.
    #[cfg(test)]
    pub(crate) fn set_ready(&self, worker_id: u16, ready: bool) {
        self.upstreams
            .iter()
            .filter(|upstream| upstream.worker_id == worker_id)
            .for_each(|upstream| upstream.ready.store(ready, Ordering::Relaxed));
    }
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
                            crate::telemetry::set_upstream_ready(
                                upstream.worker_id,
                                &upstream.rpc_url,
                                ready,
                            );
                            // Log transitions at an operator-visible level (the
                            // default filter is `info`); per-poll noise stays at
                            // debug. Without this, a 503 `/ready` is
                            // undiagnosable from the default logs.
                            if previous != ready {
                                if ready {
                                    tracing::info!(
                                        target: "gateway::readiness",
                                        worker_id = upstream.worker_id,
                                        upstream = %crate::proxy::UpstreamOrigin(&upstream.rpc_url),
                                        "upstream worker became ready"
                                    );
                                } else {
                                    tracing::warn!(
                                        target: "gateway::readiness",
                                        worker_id = upstream.worker_id,
                                        upstream = %crate::proxy::UpstreamOrigin(&upstream.rpc_url),
                                        "upstream worker became not-ready"
                                    );
                                }
                            }
                            false
                        }
                    })
                    .await;
            }
        }
    }
    Ok(())
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
    use crate::telemetry::test_utils::CapturedMetrics;
    use axum::{routing::get, Router};
    use std::sync::Mutex;
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

    /// A `tracing` writer that keeps everything written to it.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        /// Everything logged so far.
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("logs lock")).into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Two nodes whose workers share id `0` publish two readiness series, told
    /// apart by the origin of each node's RPC URL, rather than overwriting one,
    /// and a readiness transition's log line names its node by that origin too.
    #[tokio::test]
    async fn upstream_ready_series_distinguish_nodes_sharing_a_worker_id() {
        let mut metrics = CapturedMetrics::install();
        // the poller runs on this test's thread, so a thread-local subscriber
        // sees its lines
        let logs = CapturedLogs::default();
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
        // the ready node is polled last, so once it reads ready both are set
        let readiness = Arc::new(GatewayReadiness::new(&[
            node("http://10.0.0.1:8545/", "/a/health/workers"),
            node("http://10.0.0.2:8545/", "/b/health/workers"),
        ]));

        let shutdown = Notifier::new();
        let poller = tokio::spawn(run_poller(
            Arc::clone(&readiness),
            Client::new(),
            Duration::from_secs(3600),
            Duration::from_secs(5),
            shutdown.subscribe(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !readiness.any_ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the second node becomes ready");
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
        let text = logs.text();
        let became_ready: Vec<_> =
            text.lines().filter(|line| line.contains("upstream worker became ready")).collect();
        assert_eq!(became_ready.len(), 1, "one transition: {text}");
        assert!(became_ready[0].contains("upstream=http://10.0.0.2:8545"), "{text}");
        assert!(
            !text.lines().any(|line| line.contains("became") && line.contains("10.0.0.1")),
            "the not-ready node logged a transition: {text}"
        );
    }
}
