//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{future::Future, sync::Arc};

use reqwest::Client;
use tn_types::{ShutdownNotifier, TaskError, TaskManager};
use tracing::{info, warn};
use url::Url;

use crate::{
    cli::Settings,
    config::UpstreamWorker,
    proxy::{fetch_chain_id, proxy_client, QueryHeader, Route, UpstreamOrigin},
    ratelimit::{run_gc, RateLimiters, DEFAULT_MAX_PER_IP_ENTRIES},
    readiness::{run_poller, GatewayReadiness},
    server::{serve, AppState, ServerLimits},
};

/// Run the gateway until SIGTERM / ctrl-c.
///
/// Spawns two critical tasks (the HTTP server and the readiness poller) under a
/// [`TaskManager`] and blocks on `join_until_exit`, which installs the
/// SIGTERM/ctrl-c handler and drains the tasks on shutdown.
pub(crate) async fn run(settings: Settings) -> eyre::Result<()> {
    let Settings {
        listen_addr,
        upstreams,
        query_upstream,
        query_header,
        readiness_poll_interval,
        readiness_poll_timeout,
        upstream_connect_timeout,
        upstream_request_timeout,
        header_read_timeout,
        max_connections,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        rate_limit_per_ip,
        rate_limit_prefix,
        rate_limit_global,
        graceful_shutdown_timeout,
        metrics_addr,
    } = settings;

    info!(
        target: "gateway",
        %listen_addr,
        upstreams = upstreams.len(),
        redirect_queries = %query_upstream
            .as_ref()
            .map_or_else(|| String::from("off"), |url| UpstreamOrigin(url).to_string()),
        redirect_queries_header = query_header.is_some(),
        "starting worker gateway"
    );

    let readiness = Arc::new(GatewayReadiness::new(&upstreams));

    // Edge rate limiters (per-IP and/or global), or `None` when both are
    // disabled; in that case no rate-limit layer or sweep task is installed.
    let rate_limiters = RateLimiters::new(
        rate_limit_per_ip,
        rate_limit_global,
        DEFAULT_MAX_PER_IP_ENTRIES,
        rate_limit_prefix,
    );
    info!(
        target: "gateway",
        rate_limiting = rate_limiters.is_some(),
        max_request_bytes,
        ?tcp_user_timeout,
        ?max_connection_duration,
        "edge protections configured"
    );

    // Dedicated clients: the proxy enforces connect + per-request deadlines and
    // never follows redirects (see `proxy_client`); the poller bounds each
    // probe with its own tokio timeout.
    let proxy_client = proxy_client(upstream_connect_timeout, upstream_request_timeout)?;
    let readiness_client = Client::builder().connect_timeout(upstream_connect_timeout).build()?;

    let mut task_manager = TaskManager::new("worker-gateway");
    // Let in-flight requests drain within the graceful deadline (plus a small
    // margin) before the task manager reaps the server task.
    task_manager.set_join_wait(
        u64::try_from(graceful_shutdown_timeout.as_millis().saturating_add(1_000))
            .unwrap_or(u64::MAX),
    );
    let spawner = task_manager.get_spawner();
    let shutdown = ShutdownNotifier::new();

    // Optional Prometheus scrape endpoint on its own listener. Reuses the node's
    // `tn-metrics` recorder + server so the gateway's `tn_worker_gateway_*` series
    // render alongside the node's `tn_*` metrics under one collector; with
    // `--metrics` unset the `metrics` facade macros stay cheap no-ops.
    //
    // It runs under its OWN task manager, deliberately NOT passed to
    // `join_until_exit`: `start_metrics_server` spawns long-lived accept/upkeep
    // loops that have no graceful-shutdown branch, so joining them would stall
    // shutdown for the whole join deadline. Held in `_metrics_task_manager` for
    // the process lifetime and torn down with the process once the proxy has
    // drained; the endpoint stays up through the drain so a final scrape still
    // succeeds.
    let _metrics_task_manager = if let Some(metrics_addr) = metrics_addr {
        tn_metrics::install_recorder()?;
        let metrics_task_manager = TaskManager::new("worker-gateway-metrics");
        let bound = tn_metrics::start_metrics_server(
            metrics_addr,
            &metrics_task_manager.get_spawner(),
            env!("CARGO_PKG_VERSION"),
            tn_metrics::MetricsHooks::default(),
        )
        .await?;
        // Seed each worker's readiness gauge so the series exists (at 0) before
        // the first poll cycle publishes a real value.
        upstreams
            .iter()
            .for_each(|upstream| crate::telemetry::set_upstream_ready(upstream.worker_id, false));
        info!(target: "gateway", %bound, "metrics endpoint listening");
        Some(metrics_task_manager)
    } else {
        None
    };

    let state = AppState {
        readiness: Arc::clone(&readiness),
        http: proxy_client,
        query_upstream,
        query_header,
    };

    // compare the query upstream's chain with the first worker's in the
    // background, so an upstream that is slow to answer cannot hold up the
    // listener; a failed call is logged and the gateway runs on regardless
    if let Some(check) = chain_id_check_task(&state, &upstreams, &shutdown) {
        spawner.spawn_task("chain-id-check", check);
    }

    spawner.spawn_critical_task(
        "readiness-poller",
        run_poller(
            readiness,
            readiness_client,
            readiness_poll_interval,
            readiness_poll_timeout,
            shutdown.subscribe(),
        ),
    );

    let limits = ServerLimits {
        header_read_timeout,
        // One deadline must fit the body read plus the upstream response
        // headers: the upstream hop is bounded by its own request timeout, and
        // the body read gets a header-scale budget on top, so a trickled body
        // cannot hold a request slot indefinitely. (`into_settings` guarantees
        // any connection-lifetime cap is at least this deadline.)
        request_deadline: upstream_request_timeout.saturating_add(header_read_timeout),
        max_connections,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
    };

    // Sweep idle per-IP buckets while the gateway runs (only when a limiter is
    // active).
    if let Some(limiters) = &rate_limiters {
        spawner.spawn_critical_task(
            "rate-limit-gc",
            run_gc(Arc::clone(limiters), shutdown.subscribe()),
        );
    }

    spawner.spawn_critical_task(
        "gateway-server",
        serve(
            listen_addr,
            state,
            limits,
            rate_limiters,
            graceful_shutdown_timeout,
            shutdown.subscribe(),
        ),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
}

/// The startup chain-id check (see [`check_chain_ids`]) as a task for [`run`]
/// to spawn, or `None` without a query upstream or without a worker.
///
/// The task races the check against `shutdown`: the task manager waits for
/// every task it tracks before the process exits, so on a SIGTERM, or when a
/// critical task such as the server ends (a failed bind), a check still
/// waiting on a slow upstream stops at once and logs nothing instead of
/// holding the exit for up to `--upstream-request-timeout`.
fn chain_id_check_task(
    state: &AppState,
    upstreams: &[UpstreamWorker],
    shutdown: &ShutdownNotifier,
) -> Option<impl Future<Output = Result<(), TaskError>> + Send + 'static> {
    let worker = upstreams.first()?.rpc_url.clone();
    let query = state.query_upstream.clone()?;
    let client = state.http.clone();
    let query_header = state.query_header.clone();
    let shutdown = shutdown.subscribe();
    Some(async move {
        tokio::select! {
            () = shutdown => {}
            check = check_chain_ids(&client, &worker, &query, query_header.as_ref()) => {
                log_chain_id_check(&check, &worker, &query);
            }
        }
        Ok(())
    })
}

/// What startup logs after asking the first worker and the query upstream for
/// their chain ids (see [`check_chain_ids`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChainIdCheck {
    /// Both upstreams serve this chain. Logged at info.
    Match(u64),
    /// The upstreams serve different chains, so reads describe a chain other
    /// than the one submissions reach. Logged as a warning.
    Mismatch {
        /// The worker's chain id.
        worker: u64,
        /// The query upstream's chain id.
        query: u64,
    },
    /// At least one upstream gave no chain id, so nothing was compared. Logged
    /// at info: an upstream that is down at startup must not stop the gateway.
    Unchecked {
        /// Why the worker gave no chain id, or `None` when it gave one.
        worker: Option<String>,
        /// Why the query upstream gave no chain id, or `None` when it gave one.
        query: Option<String>,
    },
}

/// Ask the first worker and the query upstream for their chain ids, both at
/// once, and decide what to log.
///
/// Each call goes through `client`, the proxy client, so
/// `--upstream-request-timeout` bounds it, and only the query call carries
/// `query_header` (see [`fetch_chain_id`]). A failed call is a decision like
/// any other; it never fails startup.
pub(crate) async fn check_chain_ids(
    client: &Client,
    worker: &Url,
    query: &Url,
    query_header: Option<&QueryHeader>,
) -> ChainIdCheck {
    let (worker, query) = tokio::join!(
        fetch_chain_id(client, Route::Worker, worker, query_header),
        fetch_chain_id(client, Route::Query, query, query_header),
    );
    match (worker, query) {
        (Ok(worker), Ok(query)) if worker == query => ChainIdCheck::Match(worker),
        (Ok(worker), Ok(query)) => ChainIdCheck::Mismatch { worker, query },
        (worker, query) => ChainIdCheck::Unchecked { worker: worker.err(), query: query.err() },
    }
}

/// Log a [`ChainIdCheck`], naming each upstream by origin only.
pub(crate) fn log_chain_id_check(check: &ChainIdCheck, worker: &Url, query: &Url) {
    let worker_upstream = UpstreamOrigin(worker);
    let query_upstream = UpstreamOrigin(query);
    match check {
        ChainIdCheck::Match(chain_id) => info!(
            target: "gateway",
            chain_id,
            %worker_upstream,
            %query_upstream,
            "--redirect-queries serves the worker's chain"
        ),
        ChainIdCheck::Mismatch { worker, query } => warn!(
            target: "gateway",
            worker_chain_id = worker,
            query_chain_id = query,
            %worker_upstream,
            %query_upstream,
            "--redirect-queries serves a different chain than the worker: reads describe another \
             chain than the one submissions reach"
        ),
        ChainIdCheck::Unchecked { worker, query } => info!(
            target: "gateway",
            worker = worker.as_deref().unwrap_or("answered"),
            query = query.as_deref().unwrap_or("answered"),
            %worker_upstream,
            %query_upstream,
            "could not compare the chain ids of --redirect-queries and the worker; continuing \
             without the check"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        http::{HeaderMap, StatusCode},
        routing::post,
        Router,
    };
    use std::{
        net::SocketAddr,
        time::{Duration, Instant},
    };
    use tokio::{net::TcpListener, sync::Notify};

    /// The header the query upstream requires, and the secret value it holds.
    const API_KEY_HEADER: &str = "x-api-key";
    const API_KEY: &str = "s3cr3t-k3y-1609";

    fn api_key_header() -> QueryHeader {
        QueryHeader::parse(&format!("X-Api-Key: {API_KEY}")).expect("valid header")
    }

    /// A mock upstream answering every POST with `answer` when the request
    /// carries the API key exactly as `keyed` says it should, and with `401`
    /// otherwise, so a header sent to the wrong upstream (or missing from the
    /// right one) shows up as a failed call.
    async fn chain_id_mock(answer: &'static str, keyed: bool) -> SocketAddr {
        let mock = Router::new().route(
            "/",
            post(move |headers: HeaderMap| async move {
                let keys: Vec<&[u8]> =
                    headers.get_all(API_KEY_HEADER).iter().map(|value| value.as_bytes()).collect();
                let expected: &[&[u8]] = if keyed { &[API_KEY.as_bytes()] } else { &[] };
                if keys == expected {
                    (StatusCode::OK, answer)
                } else {
                    (StatusCode::UNAUTHORIZED, "unauthorized")
                }
            }),
        );
        serve_mock(mock).await
    }

    /// Serve `mock` on an ephemeral loopback port for the rest of the test.
    async fn serve_mock(mock: Router) -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move { axum::serve(listener, mock).await });
        addr
    }

    fn url(addr: SocketAddr) -> Url {
        Url::parse(&format!("http://{addr}/")).expect("url")
    }

    /// `eth_chainId` answers for chain 2017 and chain 1.
    const CHAIN_2017: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x7e1"}"#;
    const CHAIN_1: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#;

    fn client() -> Client {
        proxy_client(Duration::from_secs(2), Duration::from_secs(5)).expect("client")
    }

    #[tokio::test]
    async fn startup_warns_when_chain_ids_differ() {
        let header = api_key_header();
        let worker = url(chain_id_mock(CHAIN_2017, false).await);

        // the query upstream serves another chain: a warning with both ids
        let other_chain = url(chain_id_mock(CHAIN_1, true).await);
        let check = check_chain_ids(&client(), &worker, &other_chain, Some(&header)).await;
        assert_eq!(check, ChainIdCheck::Mismatch { worker: 2017, query: 1 });

        // the same chain: no warning
        let same_chain = url(chain_id_mock(CHAIN_2017, true).await);
        let check = check_chain_ids(&client(), &worker, &same_chain, Some(&header)).await;
        assert_eq!(check, ChainIdCheck::Match(2017));

        // the mocks only answer when the key reaches the query upstream and
        // not the worker, so the two outcomes above also prove its routing;
        // without the key the query upstream refuses
        let check = check_chain_ids(&client(), &worker, &same_chain, None).await;
        assert_eq!(
            check,
            ChainIdCheck::Unchecked {
                worker: None,
                query: Some("answered HTTP 401 Unauthorized".to_string())
            }
        );
    }

    #[tokio::test]
    async fn startup_proceeds_when_a_chain_id_call_fails() {
        let header = api_key_header();
        let worker = url(chain_id_mock(CHAIN_2017, false).await);
        let query = url(chain_id_mock(CHAIN_2017, true).await);
        // nothing listens on port 1
        let down = Url::parse("http://user:pass@127.0.0.1:1/k3y?token=t0k3n").expect("url");
        let junk = url(serve_mock(Router::new().route("/", post(|| async { "not json" }))).await);

        for (worker, query, worker_fails, query_fails) in [
            (&worker, &down, false, true),
            (&down, &query, true, false),
            (&down, &down, true, true),
            (&worker, &junk, false, true),
        ] {
            let check = check_chain_ids(&client(), worker, query, Some(&header)).await;
            let ChainIdCheck::Unchecked { worker: worker_reason, query: query_reason } = check
            else {
                panic!("a failed call must leave the chains unchecked, got {check:?}");
            };
            assert_eq!(
                (worker_reason.is_some(), query_reason.is_some()),
                (worker_fails, query_fails)
            );
            for reason in worker_reason.iter().chain(query_reason.iter()) {
                for secret in ["pass", "k3y", "t0k3n", API_KEY] {
                    assert!(!reason.contains(secret), "{secret:?} leaked into: {reason}");
                }
            }
        }

        // an upstream that accepts the call and never answers is cut off by the
        // proxy client's request timeout (`--upstream-request-timeout`)
        let hung = url(serve_mock(Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "late"
            }),
        ))
        .await);
        let client =
            proxy_client(Duration::from_secs(2), Duration::from_millis(200)).expect("client");
        let started = Instant::now();
        let check = check_chain_ids(&client, &worker, &hung, Some(&header)).await;
        assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
        assert!(
            matches!(check, ChainIdCheck::Unchecked { worker: None, query: Some(_) }),
            "{check:?}"
        );
    }

    /// A worker whose JSON-RPC endpoint is `rpc_url`.
    fn worker_at(rpc_url: &Url) -> UpstreamWorker {
        UpstreamWorker {
            worker_id: 0,
            rpc_url: rpc_url.clone(),
            readiness_url: rpc_url.join("health/workers").expect("readiness url"),
        }
    }

    /// Gateway state over `upstreams` that redirects queries to `query` with
    /// the API key, or redirects nothing when `query` is `None`.
    fn state(upstreams: &[UpstreamWorker], query: Option<Url>, client: Client) -> AppState {
        let query_header = query.as_ref().map(|_| api_key_header());
        AppState {
            readiness: Arc::new(GatewayReadiness::new(upstreams)),
            http: client,
            query_upstream: query,
            query_header,
        }
    }

    #[tokio::test]
    async fn chain_id_check_task_needs_a_redirect_and_a_worker() {
        let worker = url(chain_id_mock(CHAIN_2017, false).await);
        let query = url(chain_id_mock(CHAIN_2017, true).await);
        let workers = [worker_at(&worker)];
        let shutdown = ShutdownNotifier::new();

        // without --redirect-queries there is nothing to compare
        let no_redirect = state(&workers, None, client());
        assert!(chain_id_check_task(&no_redirect, &workers, &shutdown).is_none());
        // nor without a worker
        let redirect = state(&workers, Some(query), client());
        assert!(chain_id_check_task(&redirect, &[], &shutdown).is_none());

        // with both, the task runs the check to its end without a shutdown
        let task = chain_id_check_task(&redirect, &workers, &shutdown).expect("a check task");
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the check finishes")
            .expect("the task succeeds");
    }

    #[tokio::test]
    async fn chain_id_check_task_ends_promptly_on_shutdown() {
        let worker = url(chain_id_mock(CHAIN_2017, false).await);
        // a query upstream that accepts the call and never answers
        let arrived = Arc::new(Notify::new());
        let hung = {
            let arrived = Arc::clone(&arrived);
            url(serve_mock(Router::new().route(
                "/",
                post(move || {
                    let arrived = Arc::clone(&arrived);
                    async move {
                        arrived.notify_one();
                        std::future::pending::<&'static str>().await
                    }
                }),
            ))
            .await)
        };
        // a request timeout far past the bound below, so only the shutdown can
        // end the check
        let client =
            proxy_client(Duration::from_secs(2), Duration::from_secs(600)).expect("client");
        let workers = [worker_at(&worker)];
        let shutdown = ShutdownNotifier::new();
        let task = chain_id_check_task(&state(&workers, Some(hung), client), &workers, &shutdown)
            .expect("a check task");
        let task = tokio::spawn(task);

        tokio::time::timeout(Duration::from_secs(5), arrived.notified())
            .await
            .expect("the check reaches the query upstream");
        assert!(!task.is_finished(), "the check cannot finish while the query upstream hangs");

        shutdown.notify();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the check ends once shutdown fires")
            .expect("the task does not panic")
            .expect("the task succeeds");
    }
}
