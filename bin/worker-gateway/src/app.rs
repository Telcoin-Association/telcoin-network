//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use eyre::WrapErr as _;
use reqwest::Client;
use tn_types::{ShutdownNotifier, TaskError, TaskManager};
use tracing::{info, warn};
use url::Url;

use crate::{
    cli::Settings,
    config::UpstreamWorker,
    dns::{LookupHost, SystemLookup},
    proxy::{
        client_builder, fetch_chain_id, proxy_client, QueryHeader, Route, UpstreamOrigin,
        WARNING_THROTTLES,
    },
    ratelimit::{run_gc, RateLimiters, DEFAULT_MAX_PER_IP_ENTRIES},
    readiness::{run_poller, GatewayReadiness, QueryProbe, ReadinessThresholds},
    server::{
        inflight_slots, serve, serve_probes, AppState, RecentSubmissions, ServerLimits,
        RECENT_SUBMISSIONS_CAPACITY,
    },
    telemetry::run_throttle_summaries,
};

/// Context for a failure to build an upstream client. A TLS file rustls
/// rejects (a certificate it cannot parse, or a client key that does not match
/// its certificate) is already reported by flag and path when the settings are
/// resolved, so this only points at the TLS flags as a backstop.
const TLS_SETTINGS_HINT: &str = "cannot build the upstream clients; check --upstream-ca-cert, \
                                 --upstream-client-cert and --upstream-client-key";

/// Run the gateway until SIGTERM / ctrl-c.
///
/// Spawns the HTTP server, the probe listener (with `--probe-addr` set) and the
/// readiness poller as critical tasks under a [`TaskManager`] and blocks on
/// `join_until_exit`, which installs the SIGTERM/ctrl-c handler and drains the
/// tasks on shutdown.
pub(crate) async fn run(settings: Settings) -> eyre::Result<()> {
    ensure_root_store(&settings, native_root_count)?;

    // Dedicated clients, both started from the same TLS settings (see
    // `client_builder`): the proxy enforces connect + per-request deadlines and
    // never follows redirects (see `proxy_client`); the poller bounds each
    // probe with its own tokio timeout. Each resolves upstream names through
    // its own cache and lookup cap (see `UpstreamClients`).
    let UpstreamClients { proxy: proxy_client, readiness: readiness_client } =
        UpstreamClients::new(&settings, Arc::new(SystemLookup)).wrap_err(TLS_SETTINGS_HINT)?;
    let Settings {
        listen_addr,
        probe_addr,
        upstreams,
        query_upstream,
        query_header,
        recent_submission_ttl,
        denied_method_prefixes,
        readiness_poll_interval,
        readiness_poll_timeout,
        readiness_failure_threshold,
        readiness_success_threshold,
        upstream_failure_threshold,
        upstream_connect_timeout: _,
        upstream_request_timeout,
        query_request_timeout,
        upstream_ca_certs: _,
        upstream_identity: _,
        dns_cache_ttl,
        max_concurrent_dns_lookups,
        header_read_timeout,
        max_connections,
        max_connections_per_ip,
        max_inflight_submissions,
        max_inflight_queries,
        max_upstream_inflight,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        max_batch_len,
        max_inflight_request_bytes,
        http1_max_buf_size,
        rate_limit_per_ip,
        rate_limit_prefix,
        rate_limit_global,
        trusted_proxies,
        proxy_protocol,
        rate_limit_submissions,
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
        ?recent_submission_ttl,
        "starting worker gateway"
    );

    let thresholds = ReadinessThresholds {
        failure: readiness_failure_threshold,
        success: readiness_success_threshold,
        rpc_failure: upstream_failure_threshold,
    };
    let readiness = Arc::new(GatewayReadiness::new(&upstreams, thresholds));

    // Edge rate limiters (per-IP, global and the submission budget), or `None`
    // when all are disabled; in that case no rate-limit layer or sweep task is
    // installed.
    let submission_budget = rate_limit_submissions.is_some();
    let rate_limiters = RateLimiters::new(
        rate_limit_per_ip,
        rate_limit_global,
        rate_limit_submissions,
        DEFAULT_MAX_PER_IP_ENTRIES,
        rate_limit_prefix,
    );
    let trusted_proxies = Arc::new(trusted_proxies);
    if trusted_proxies.has_catch_all() {
        warn!(
            target: "gateway",
            "--trusted-proxies contains a /0 range: every client that can reach the gateway is \
             a trusted proxy, may choose its own rate-limit key and is not counted by \
             --max-connections-per-ip; list only the fronts' own addresses"
        );
    }
    // the method policy applies only without a redirect, and an empty list
    // (an empty or unset-in-template env var included) turns it off; say which
    let method_policy = if query_upstream.is_some() {
        String::from("off (--redirect-queries is set)")
    } else if denied_method_prefixes.is_empty() {
        String::from("off (--denied-method-prefixes is empty)")
    } else {
        format!("deny {}", denied_method_prefixes.join(","))
    };
    info!(
        target: "gateway",
        rate_limiting = rate_limiters.is_some(),
        trusted_proxies = trusted_proxies.len(),
        proxy_protocol,
        submission_budget,
        ?max_connections_per_ip,
        max_request_bytes,
        ?max_batch_len,
        max_inflight_submissions,
        max_inflight_queries,
        max_upstream_inflight,
        max_inflight_request_bytes = max_inflight_request_bytes.map_or(0, NonZeroU32::get),
        http1_max_buf_size,
        ?tcp_user_timeout,
        ?max_connection_duration,
        ?dns_cache_ttl,
        %max_concurrent_dns_lookups,
        %method_policy,
        "edge protections configured"
    );
    if let Some(idle_heads) =
        idle_heads_exhausting_budget(max_inflight_request_bytes, max_request_bytes, max_connections)
    {
        warn!(
            target: "gateway",
            idle_heads,
            max_connections = max_connections.get(),
            "this many idle requests, fewer than --max-connections, can hold the whole in-flight \
             byte budget until the request deadline by sending only their headers, and every \
             other request body is then refused; raise --max-inflight-request-bytes, or lower \
             --max-request-bytes or --max-connections"
        );
    }
    if query_upstream.is_none() && denied_method_prefixes.is_empty() {
        warn!(
            target: "gateway",
            "method policy is off: --denied-method-prefixes is empty and --redirect-queries is \
             unset, so every method, tn_* included, reaches the worker"
        );
    }

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
        upstreams.iter().for_each(|upstream| {
            crate::telemetry::set_upstream_ready(upstream.worker_id, &upstream.rpc_url, false)
        });
        info!(target: "gateway", %bound, "metrics endpoint listening");
        Some(metrics_task_manager)
    } else {
        None
    };

    // The poller probes the query upstream with the proxy client, so the probe
    // travels like a redirected read, through the same DNS cache and lookup
    // cap, and with the same `--redirect-queries-header`.
    let query_probe = query_upstream.clone().map(|url| QueryProbe {
        url,
        client: proxy_client.clone(),
        query_header: query_header.clone(),
    });
    // submissions are remembered only for the redirect's hash routing
    let recent_submissions = recent_submission_ttl
        .filter(|_| query_upstream.is_some())
        .map(|ttl| Arc::new(RecentSubmissions::new(ttl, RECENT_SUBMISSIONS_CAPACITY)));
    let state = AppState {
        readiness: Arc::clone(&readiness),
        http: proxy_client,
        query_upstream,
        query_header,
        recent_submissions,
        denied_method_prefixes: denied_method_prefixes.into(),
        max_batch_len,
        query_request_timeout,
        submission_slots: inflight_slots(max_inflight_submissions),
        query_slots: inflight_slots(max_inflight_queries),
        upstream_slots: inflight_slots(max_upstream_inflight),
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
            query_probe,
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
        max_connections_per_ip,
        // The per-client cap keys clients by the rate limiter's prefix
        // policy, which applies here even when per-IP rate limiting is off.
        client_prefix: rate_limit_prefix,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        // `into_settings` rejects `--proxy-protocol` without trusted proxies
        proxy_protocol: proxy_protocol.then(|| Arc::clone(&trusted_proxies)),
        max_inflight_request_bytes,
        http1_max_buf_size,
    };

    // Sweep idle per-IP buckets while the gateway runs (only when a limiter is
    // active).
    if let Some(limiters) = &rate_limiters {
        spawner.spawn_critical_task(
            "rate-limit-gc",
            run_gc(Arc::clone(limiters), shutdown.subscribe()),
        );
    }

    // Summarize a burst of throttled per-request warnings once it stops, so
    // its count does not wait for the call site's next event.
    spawner.spawn_critical_task(
        "warning-summaries",
        run_throttle_summaries(&WARNING_THROTTLES, shutdown.subscribe()),
    );
    // With `--probe-addr` set, the probes also get a listener of their own,
    // outside the client connection cap and the rate limits.
    if let Some(probe_addr) = probe_addr {
        spawner.spawn_critical_task(
            "probe-server",
            serve_probes(probe_addr, state.clone(), header_read_timeout, shutdown.subscribe()),
        );
    }

    spawner.spawn_critical_task(
        "gateway-server",
        serve(
            listen_addr,
            state,
            limits,
            rate_limiters,
            trusted_proxies,
            graceful_shutdown_timeout,
            shutdown.subscribe(),
        ),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
}

/// The gateway's two upstream HTTP clients, both started from
/// [`client_builder`], so they share the TLS settings.
#[derive(Debug)]
pub(crate) struct UpstreamClients {
    /// Forwards client requests to the workers and the query upstream, and
    /// probes the query upstream for the poller (see [`proxy_client`] and
    /// [`QueryProbe`]).
    pub(crate) proxy: Client,
    /// Polls the workers' readiness endpoints.
    pub(crate) readiness: Client,
}

impl UpstreamClients {
    /// Build both clients from `settings`, each with its own
    /// [`crate::dns::CachingResolver`] over `lookup` (see [`client_builder`]):
    /// its own cache, and its own cap of `--max-concurrent-dns-lookups`
    /// lookups at once.
    ///
    /// The readiness client must not share the proxy's resolver. Query traffic
    /// to hosts that resolve slowly can hold every proxy lookup permit, and a
    /// readiness poll refused a lookup then would mark the worker not-ready and
    /// turn a slow query upstream into refused submissions. The node's health
    /// listener closes the connection after each response, so every poll opens
    /// a new connection and goes through the resolver.
    pub(crate) fn new(settings: &Settings, lookup: Arc<dyn LookupHost>) -> reqwest::Result<Self> {
        Ok(Self {
            proxy: proxy_client(
                client_builder(settings, Arc::clone(&lookup)),
                settings.upstream_request_timeout,
            )?,
            readiness: client_builder(settings, lookup).build()?,
        })
    }
}

/// Fail startup when an `https` upstream would be verified against the native
/// root store alone and that store is empty.
///
/// reqwest builds a client over an empty native root store without complaint,
/// so on a host without CA certificates every `https` call would fail at
/// runtime instead, and a worker would just never become ready. The check
/// applies only when some configured URL (a worker's RPC or readiness URL, or
/// `--redirect-queries`) is `https` and no `--upstream-ca-cert` is given: the
/// extra CA is something to verify against on its own. `native_root_count`
/// loads the store, and is called only when the check applies.
fn ensure_root_store(
    settings: &Settings,
    native_root_count: impl FnOnce() -> usize,
) -> eyre::Result<()> {
    let https = settings
        .upstreams
        .iter()
        .flat_map(|upstream| [&upstream.rpc_url, &upstream.readiness_url])
        .chain(settings.query_upstream.as_ref())
        .any(|url| url.scheme() == "https");
    if !https || !settings.upstream_ca_certs.is_empty() {
        return Ok(());
    }
    eyre::ensure!(
        native_root_count() > 0,
        "an https upstream is configured but the system root store holds no CA certificates, \
         so every https call would fail; install the ca-certificates package, point \
         SSL_CERT_FILE or SSL_CERT_DIR at a CA bundle, or pass --upstream-ca-cert"
    );
    Ok(())
}

/// Load the platform's native root store the way reqwest does when it builds a
/// client, and count the certificates found. A file that fails to load is
/// logged and skipped, as reqwest skips it.
fn native_root_count() -> usize {
    let loaded = rustls_native_certs::load_native_certs();
    loaded.errors.iter().for_each(|err| {
        warn!(target: "gateway", %err, "failed to load part of the system root store");
    });
    loaded.certs.len()
}

/// How many requests that send only their headers hold the whole in-flight
/// byte budget, when that is fewer than `--max-connections`.
///
/// Each such request reserves up to `--max-request-bytes` from its head (the
/// whole cap when chunked) and keeps it until the request deadline, so this
/// many idle connections refuse every other request body. Returns `None` when
/// the connection cap binds first, the budget is unlimited, or the cap is `0`
/// (nothing can then be reserved).
fn idle_heads_exhausting_budget(
    max_inflight_request_bytes: Option<NonZeroU32>,
    max_request_bytes: usize,
    max_connections: NonZeroUsize,
) -> Option<usize> {
    let budget = usize::try_from(max_inflight_request_bytes?.get()).unwrap_or(usize::MAX);
    let cap = NonZeroUsize::new(max_request_bytes)?;
    let idle_heads = budget.div_ceil(cap.get());
    (idle_heads < max_connections.get()).then_some(idle_heads)
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
    use crate::cli::Cli;
    use axum::{
        http::{HeaderMap, StatusCode},
        routing::post,
        Router,
    };
    use clap::Parser as _;
    use std::{
        net::SocketAddr,
        time::{Duration, Instant},
    };
    use tokio::{net::TcpListener, sync::Notify};

    fn settings(flags: &[&str]) -> Settings {
        let argv = std::iter::once("worker-gateway").chain(flags.iter().copied());
        Cli::parse_from(argv).into_settings().expect("settings")
    }

    #[test]
    fn empty_root_store_is_a_startup_error() -> eyre::Result<()> {
        const HTTP_RPC: &str = "--upstream-rpc-url=http://10.0.0.7:8545";
        const HTTPS_RPC: &str = "--upstream-rpc-url=https://10.0.0.7:8545";
        const HTTP_READY: &str = "--upstream-readiness-url=http://10.0.0.7:8551/health/workers";
        const HTTPS_READY: &str = "--upstream-readiness-url=https://10.0.0.7:8551/health/workers";

        let https_worker = settings(&[HTTPS_RPC, HTTP_READY]);
        let message = match ensure_root_store(&https_worker, || 0) {
            Ok(()) => panic!("an https worker over an empty root store must fail startup"),
            Err(err) => format!("{err:?}"),
        };
        assert!(message.contains("ca-certificates"), "{message}");
        assert!(ensure_root_store(&https_worker, || 1).is_ok());

        // an https readiness url, or an https redirect, needs the store too
        assert!(ensure_root_store(&settings(&[HTTP_RPC, HTTPS_READY]), || 0).is_err());
        let https_redirect =
            settings(&[HTTP_RPC, HTTP_READY, "--redirect-queries=https://rpc.example.com/"]);
        assert!(ensure_root_store(&https_redirect, || 0).is_err());

        // plain http everywhere, or an extra CA, never loads the store
        let http_only = settings(&[HTTP_RPC, HTTP_READY]);
        assert!(ensure_root_store(&http_only, || panic!("the store must not be loaded")).is_ok());
        let ca_file = tempfile::NamedTempFile::new()?;
        let ca = rcgen::generate_simple_self_signed(vec!["ca.test".to_string()])?;
        std::fs::write(ca_file.path(), ca.cert.pem())?;
        let ca_flag = format!("--upstream-ca-cert={}", ca_file.path().display());
        let with_ca = settings(&[HTTPS_RPC, HTTPS_READY, ca_flag.as_str()]);
        assert!(ensure_root_store(&with_ca, || panic!("the store must not be loaded")).is_ok());
        Ok(())
    }

    const MIB: usize = 1024 * 1024;

    fn idle_heads(budget_mib: u32, cap: usize, connections: usize) -> Option<usize> {
        idle_heads_exhausting_budget(
            NonZeroU32::new(budget_mib * 1024 * 1024),
            cap,
            NonZeroUsize::new(connections).expect("nonzero"),
        )
    }

    #[test]
    fn idle_heads_are_reported_only_below_the_connection_cap() {
        // the defaults: 512 idle requests are needed, more than 500 connections
        assert_eq!(idle_heads(512, MIB, 500), None);
        // a 15 MiB cap: 35 idle requests hold the default budget
        assert_eq!(idle_heads(512, 15 * MIB, 500), Some(35));
        // a 256 MiB budget, unless --max-connections is lowered with it
        assert_eq!(idle_heads(256, MIB, 500), Some(256));
        assert_eq!(idle_heads(256, MIB, 256), None);
        // more connections than the default budget covers
        assert_eq!(idle_heads(512, MIB, 2000), Some(512));
        // an unlimited budget or a zero cap cannot be exhausted from heads
        assert_eq!(idle_heads(0, MIB, 500), None);
        assert_eq!(idle_heads(512, 0, 500), None);
    }

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
        proxy_client(
            Client::builder().connect_timeout(Duration::from_secs(2)),
            Duration::from_secs(5),
        )
        .expect("client")
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
        let client = proxy_client(
            Client::builder().connect_timeout(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
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
        let thresholds = ReadinessThresholds {
            failure: NonZeroU32::MIN,
            success: NonZeroU32::MIN,
            rpc_failure: None,
        };
        AppState {
            readiness: Arc::new(GatewayReadiness::new(upstreams, thresholds)),
            http: client,
            query_upstream: query,
            query_header,
            recent_submissions: None,
            denied_method_prefixes: Arc::from([]),
            max_batch_len: None,
            query_request_timeout: None,
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
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
        let client = proxy_client(
            Client::builder().connect_timeout(Duration::from_secs(2)),
            Duration::from_secs(600),
        )
        .expect("client");
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
