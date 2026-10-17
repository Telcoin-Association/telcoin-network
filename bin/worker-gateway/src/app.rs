//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use eyre::WrapErr as _;
use reqwest::Client;
use tn_types::{Noticer, ShutdownNotifier, TaskError, TaskManager};
use tracing::{info, warn};
use url::Url;

use crate::{
    cli::{request_deadline, submission_reserve, Settings},
    config::UpstreamWorker,
    dns::{LookupHost, SystemLookup},
    proxy::{
        client_builder, fetch_chain_id, readiness_client, route_clients, QueryHeader, Route,
        UpstreamOrigin, WARNING_THROTTLES,
    },
    ratelimit::{run_gc, RateLimiters},
    readiness::{run_poller, GatewayReadiness, QueryProbe, ReadinessThresholds},
    server::{
        inflight_slots, serve, serve_probes, AccessLog, AppState, CorsOrigins, RecentSubmissions,
        RouterLayers, ServerLimits, RECENT_SUBMISSIONS_CAPACITY,
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
///
/// Shutdown runs in two phases. On the notice both listeners set `draining`,
/// so `/ready` and `/ready/any` answer `503`, and keep accepting and serving
/// for `--shutdown-delay` while the poller keeps routing on live readiness.
/// Then the listeners close together, the poller stops, and the server drains
/// in-flight requests within `--graceful-shutdown-timeout`; the task manager's
/// join wait covers both phases.
pub(crate) async fn run(settings: Settings) -> eyre::Result<()> {
    ensure_root_store(&settings, native_root_count)?;

    // Dedicated clients, all started from `client_builder`'s settings so they
    // share the connect timeout, TLS and the no-proxy policy, and none follows
    // redirects: one forwarding client per route, whose read timeout bounds
    // the wait for a response head and every gap in its body (see
    // `route_clients`), and the poller's, which bounds each probe with its own
    // tokio timeout (see `readiness_client`). The forwarding clients share one
    // DNS cache and lookup cap, and the poller's has its own (see
    // `UpstreamClients`).
    let UpstreamClients { proxy: proxy_client, query: query_client, readiness: readiness_client } =
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
        query_request_timeout: _,
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
        max_worker_pool_reads,
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
        rate_limit_per_ip_max_entries,
        shutdown_delay,
        graceful_shutdown_timeout,
        metrics_addr,
        cors_allowed_origins,
        access_log_sample,
        access_log_max_lines_per_second,
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
        ?shutdown_delay,
        ?graceful_shutdown_timeout,
        cors = match &cors_allowed_origins {
            None => "off",
            Some(CorsOrigins::Any) => "any origin",
            Some(CorsOrigins::List(_)) => "listed origins",
        },
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
        rate_limit_per_ip_max_entries.get(),
        rate_limit_prefix,
    );
    let trusted_proxies = Arc::new(trusted_proxies);
    // the method policy applies only without a redirect, and an empty list
    // (an empty or unset-in-template env var included) turns it off; say which
    let method_policy = if query_upstream.is_some() {
        String::from("off (--redirect-queries is set)")
    } else if denied_method_prefixes.is_empty() {
        String::from("off (--denied-method-prefixes is empty)")
    } else {
        format!("deny {}", denied_method_prefixes.join(","))
    };
    // The sampled access log keys clients by the same prefix as the per-IP
    // limiter, or `None` when `--access-log-sample` is `0`.
    let access_log =
        AccessLog::new(access_log_sample, access_log_max_lines_per_second, rate_limit_prefix);
    // the worker slots reads can never take, so the operator sees what
    // submissions are guaranteed, not just `--max-inflight-submissions`:
    // without a redirect every read may take one, with one only the reads kept
    // on the worker, up to their share
    let worker_bound_reads =
        if query_upstream.is_some() { max_worker_pool_reads } else { max_inflight_queries };
    let reserve = submission_reserve(max_upstream_inflight, worker_bound_reads)
        .map_or_else(|| String::from("unlimited"), |slots| slots.to_string());
    info!(
        target: "gateway",
        rate_limiting = rate_limiters.is_some(),
        trusted_proxies = trusted_proxies.len(),
        proxy_protocol = proxy_protocol.is_some(),
        submission_budget,
        rate_limit_per_ip_max_entries = ?rate_limit_per_ip
            .map(|_| rate_limit_per_ip_max_entries.get()),
        ?max_connections_per_ip,
        // the per-ip key's prefix widths (`v4`, `v6`), which also key
        // --max-connections-per-ip
        rate_limit_v4_prefix = rate_limit_prefix.v4_bits(),
        rate_limit_v6_prefix = rate_limit_prefix.v6_bits(),
        max_request_bytes,
        ?max_batch_len,
        max_inflight_submissions,
        max_inflight_queries,
        max_upstream_inflight,
        // the reads kept on the worker take their share only with a redirect
        max_worker_pool_reads = ?query_upstream.as_ref().map(|_| max_worker_pool_reads),
        submission_reserve = %reserve,
        max_inflight_request_bytes = max_inflight_request_bytes.map_or(0, NonZeroU32::get),
        http1_max_buf_size,
        ?tcp_user_timeout,
        ?max_connection_duration,
        ?dns_cache_ttl,
        %max_concurrent_dns_lookups,
        %method_policy,
        access_log = %access_log
            .as_ref()
            .map_or_else(|| String::from("off"), |_| format!("sampling {access_log_sample}")),
        access_log_max_lines_per_second = access_log_max_lines_per_second
            .map_or(0, NonZeroU32::get),
        "edge protections configured"
    );
    if let Some(idle_heads) =
        idle_heads_exhausting_budget(max_inflight_request_bytes, max_request_bytes, max_connections)
    {
        if one_prefix_holds_budget(idle_heads, max_connections_per_ip) {
            warn!(
                target: "gateway",
                idle_heads,
                max_connections_per_ip = max_connections_per_ip.map_or(0, NonZeroUsize::get),
                "one client prefix can hold the whole in-flight byte budget with this many idle \
                 requests, within --max-connections-per-ip, until the request deadline by sending \
                 only their headers, and every other request body is then refused; raise \
                 --max-inflight-request-bytes, or lower --max-request-bytes or \
                 --max-connections-per-ip"
            );
        } else {
            warn!(
                target: "gateway",
                idle_heads,
                max_connections = max_connections.get(),
                "this many idle requests, fewer than --max-connections, can hold the whole \
                 in-flight byte budget until the request deadline by sending only their headers, \
                 and every other request body is then refused; raise \
                 --max-inflight-request-bytes, or lower --max-request-bytes or --max-connections"
            );
        }
    }
    if query_upstream.is_none() && denied_method_prefixes.is_empty() {
        warn!(
            target: "gateway",
            "method policy is off: --denied-method-prefixes is empty and --redirect-queries is \
             unset, so every method, tn_* included, reaches the worker"
        );
    }

    let mut task_manager = TaskManager::new("worker-gateway");
    // Let the server keep serving through the shutdown delay and then drain
    // in-flight requests within the graceful deadline (plus a small margin)
    // before the task manager reaps the server task.
    task_manager.set_join_wait(
        u64::try_from(
            shutdown_delay
                .saturating_add(graceful_shutdown_timeout)
                .as_millis()
                .saturating_add(1_000),
        )
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

    // The poller probes the query upstream with the query route's client, so
    // the probe travels like a redirected read, with the same read timeout,
    // through the proxy's DNS cache and lookup cap, and with the same
    // `--redirect-queries-header`. With a redirect, `route_clients` always
    // builds that client.
    let query_probe = query_upstream
        .clone()
        .zip(query_client.clone())
        .map(|(url, client)| QueryProbe { url, client, query_header: query_header.clone() });
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
        query_http: query_client,
        upstream_request_timeout,
        submission_slots: inflight_slots(max_inflight_submissions),
        query_slots: inflight_slots(max_inflight_queries),
        upstream_slots: inflight_slots(max_upstream_inflight),
        pool_read_slots: inflight_slots(max_worker_pool_reads),
        draining: Arc::default(),
    };

    // compare the query upstream's chain with the first worker's in the
    // background, so an upstream that is slow to answer cannot hold up the
    // listener; a failed call is logged and the gateway runs on regardless
    if let Some(check) = chain_id_check_task(&state, &upstreams, &shutdown) {
        spawner.spawn_task("chain-id-check", check);
    }

    spawner.spawn_critical_task(
        "readiness-poller",
        run_poller_until_listener_closes(
            readiness,
            readiness_client,
            query_probe,
            readiness_poll_interval,
            readiness_poll_timeout,
            shutdown_delay,
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
        request_deadline: request_deadline(upstream_request_timeout, header_read_timeout),
        max_connections,
        max_connections_per_ip,
        // The per-client cap keys clients by the rate limiter's prefix
        // policy, which applies here even when per-IP rate limiting is off.
        client_prefix: rate_limit_prefix,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        // `into_settings` rejects `--proxy-protocol` with no peer to read from
        proxy_protocol: proxy_protocol.map(Arc::new),
        max_inflight_request_bytes,
        http1_max_buf_size,
        shutdown_delay,
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
    // outside the client connection cap and the rate limits. It keeps
    // answering through the shutdown delay, so the kubelet and any front
    // probing it see the draining `503`, and closes with the client listener.
    if let Some(probe_addr) = probe_addr {
        spawner.spawn_critical_task(
            "probe-server",
            serve_probes(
                probe_addr,
                state.clone(),
                header_read_timeout,
                shutdown_delay,
                shutdown.subscribe(),
            ),
        );
    }

    spawner.spawn_critical_task(
        "gateway-server",
        serve(
            listen_addr,
            state,
            limits,
            RouterLayers { rate_limiters, cors: cors_allowed_origins, access_log },
            trusted_proxies,
            graceful_shutdown_timeout,
            shutdown.subscribe(),
        ),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
}

/// The gateway's upstream HTTP clients, all started from [`client_builder`]'s
/// settings, so they share the TLS settings and the no-proxy policy, and none
/// follows a redirect.
#[derive(Debug)]
pub(crate) struct UpstreamClients {
    /// Forwards client requests to the workers, and to the query upstream when
    /// `query` is `None` (see [`route_clients`]).
    pub(crate) proxy: Client,
    /// Forwards client requests to the `--redirect-queries` upstream and
    /// probes it for the poller (see [`QueryProbe`]), or `None` without a
    /// redirect.
    pub(crate) query: Option<Client>,
    /// Polls the workers' readiness endpoints (see [`readiness_client`]).
    pub(crate) readiness: Client,
}

impl UpstreamClients {
    /// Build the clients from `settings`: the forwarding clients over one
    /// shared [`crate::dns::CachingResolver`] (see [`route_clients`]) and the
    /// readiness client over its own (see [`client_builder`]), each resolver
    /// over `lookup` with its own cache and its own cap of
    /// `--max-concurrent-dns-lookups` lookups at once.
    ///
    /// The readiness client must not share the proxy's resolver. Query traffic
    /// to hosts that resolve slowly can hold every proxy lookup permit, and a
    /// readiness poll refused a lookup then would mark the worker not-ready and
    /// turn a slow query upstream into refused submissions. The node's health
    /// listener closes the connection after each response, so every poll opens
    /// a new connection and goes through the resolver.
    pub(crate) fn new(settings: &Settings, lookup: Arc<dyn LookupHost>) -> reqwest::Result<Self> {
        let routes = route_clients(settings, Arc::clone(&lookup))?;
        Ok(Self {
            proxy: routes.worker,
            query: routes.query,
            readiness: readiness_client(client_builder(settings, lookup))?,
        })
    }
}

/// Fail startup when an `https` upstream would be verified against the native
/// root store alone and that store is empty, and warn when an empty store
/// leaves `--upstream-ca-cert` as the only trusted root.
///
/// reqwest builds a client over an empty native root store without complaint,
/// so on a host without CA certificates every `https` call would fail at
/// runtime instead, and a worker would just never become ready. The check
/// applies only when some configured URL (a worker's RPC or readiness URL, or
/// `--redirect-queries`) is `https`. Without `--upstream-ca-cert` an empty
/// store is an error. With it the gateway starts, because private-CA workers
/// verify against that CA alone, but warns: the one client trust store serves
/// every upstream, so an `https` `--redirect-queries` signed by a public CA
/// would then fail every call. `native_root_count` loads the store, and is
/// called only when the check applies.
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
    if !https {
        return Ok(());
    }
    let native = native_root_count();
    if settings.upstream_ca_certs.is_empty() {
        eyre::ensure!(
            native > 0,
            "an https upstream is configured but the system root store holds no CA certificates, \
             so every https call would fail; install the ca-certificates package, point \
             SSL_CERT_FILE or SSL_CERT_DIR at a CA bundle, or pass --upstream-ca-cert"
        );
    } else if native == 0 {
        warn!(
            target: "gateway",
            "the system root store holds no CA certificates, so every https upstream, \
             --redirect-queries included, must chain to --upstream-ca-cert"
        );
    }
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

/// Whether one client prefix may open the `idle_heads` connections that hold
/// the whole in-flight byte budget (see [`idle_heads_exhausting_budget`]): true
/// when `--max-connections-per-ip` is set and allows at least that many.
fn one_prefix_holds_budget(
    idle_heads: usize,
    max_connections_per_ip: Option<NonZeroUsize>,
) -> bool {
    max_connections_per_ip.is_some_and(|per_ip| idle_heads <= per_ip.get())
}

/// The startup chain-id check (see [`check_chain_ids`]) as a task for [`run`]
/// to spawn, or `None` without a query upstream or without a worker.
///
/// The task races the check against `shutdown`: the task manager waits for
/// every task it tracks before the process exits, so on a SIGTERM, or when a
/// critical task such as the server ends (a failed bind), a check still
/// waiting on a slow upstream stops at once and logs nothing instead of
/// holding the exit while a slow upstream answers: each client's read timeout
/// bounds only the head and each gap in the body, not the whole call.
fn chain_id_check_task(
    state: &AppState,
    upstreams: &[UpstreamWorker],
    shutdown: &ShutdownNotifier,
) -> Option<impl Future<Output = Result<(), TaskError>> + Send + 'static> {
    let worker = upstreams.first()?.rpc_url.clone();
    let query = state.query_upstream.clone()?;
    // the clients forwarded calls use (see `proxy::forward`)
    let worker_client = state.http.clone();
    let query_client = state.query_http.clone().unwrap_or_else(|| state.http.clone());
    let query_header = state.query_header.clone();
    let shutdown = shutdown.subscribe();
    Some(async move {
        tokio::select! {
            () = shutdown => {}
            check = check_chain_ids(
                &worker_client,
                &query_client,
                &worker,
                &query,
                query_header.as_ref(),
            ) => {
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
/// The worker call goes through `worker_client` and the query call through
/// `query_client`, the clients forwarded calls use, so
/// `--upstream-request-timeout` and `--query-request-timeout` bound the head
/// and each gap of the answer, not the whole call, and only the query call
/// carries `query_header` (see [`fetch_chain_id`]). A failed call is a
/// decision like any other; it never fails startup.
pub(crate) async fn check_chain_ids(
    worker_client: &Client,
    query_client: &Client,
    worker: &Url,
    query: &Url,
    query_header: Option<&QueryHeader>,
) -> ChainIdCheck {
    let (worker, query) = tokio::join!(
        fetch_chain_id(worker_client, Route::Worker, worker, query_header),
        fetch_chain_id(query_client, Route::Query, query, query_header),
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

/// Run the readiness poller until the server's listener closes.
///
/// The server keeps accepting and routing submissions on this readiness view
/// for `shutdown_delay` after the shutdown notice, so the poller keeps polling
/// through the delay and stops when it ends. With no delay it stops at the
/// notice, as the listener does.
async fn run_poller_until_listener_closes(
    readiness: Arc<GatewayReadiness>,
    client: Client,
    query_probe: Option<QueryProbe>,
    poll_interval: Duration,
    poll_timeout: Duration,
    shutdown_delay: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    if shutdown_delay.is_zero() {
        return run_poller(readiness, client, query_probe, poll_interval, poll_timeout, shutdown)
            .await;
    }
    let listener_closed = ShutdownNotifier::new();
    let poller = run_poller(
        readiness,
        client,
        query_probe,
        poll_interval,
        poll_timeout,
        listener_closed.subscribe(),
    );
    tokio::pin!(poller);
    tokio::select! {
        result = &mut poller => return result,
        () = async {
            shutdown.await;
            tokio::time::sleep(shutdown_delay).await;
        } => {}
    }
    listener_closed.notify();
    poller.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cli::try_parse_test_argv,
        proxy::{proxy_client, UpstreamClientBuilder},
    };
    use axum::{
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use std::{
        net::SocketAddr,
        sync::atomic::{AtomicBool, Ordering},
        time::{Duration, Instant},
    };
    use tokio::{
        net::TcpListener,
        sync::{watch, Notify},
        task::JoinHandle,
    };

    fn settings(flags: &[&str]) -> Settings {
        let argv = std::iter::once("worker-gateway").chain(flags.iter().copied());
        try_parse_test_argv(argv).expect("argv parses").into_settings().expect("settings")
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

        // plain http everywhere never loads the store
        let http_only = settings(&[HTTP_RPC, HTTP_READY]);
        assert!(ensure_root_store(&http_only, || panic!("the store must not be loaded")).is_ok());
        Ok(())
    }

    #[test]
    fn custom_ca_with_an_empty_store_starts() -> eyre::Result<()> {
        let ca_file = tempfile::NamedTempFile::new()?;
        let ca = rcgen::generate_simple_self_signed(vec!["ca.test".to_string()])?;
        std::fs::write(ca_file.path(), ca.cert.pem())?;
        let ca_flag = format!("--upstream-ca-cert={}", ca_file.path().display());
        let with_ca = settings(&[
            "--upstream-rpc-url=https://10.0.0.7:8545",
            "--upstream-readiness-url=https://10.0.0.7:8551/health/workers",
            "--redirect-queries=https://rpc.example.com/",
            ca_flag.as_str(),
        ]);

        // the store is still loaded, so an empty one can be reported, but the
        // private CA alone lets the workers verify, so startup goes on
        let loaded = std::cell::Cell::new(false);
        let result = ensure_root_store(&with_ca, || {
            loaded.set(true);
            0
        });
        assert!(result.is_ok(), "{result:?}");
        assert!(loaded.get(), "the store must be loaded when an https url is configured");
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
    fn one_prefix_holding_the_budget_is_reported() {
        let per_ip = NonZeroUsize::new(32);
        assert!(one_prefix_holds_budget(32, per_ip), "at the per-ip cap");
        assert!(one_prefix_holds_budget(1, per_ip));
        assert!(!one_prefix_holds_budget(33, per_ip), "above the per-ip cap");
        assert!(!one_prefix_holds_budget(1, None), "no per-ip cap");
        // a 16 MiB --max-request-bytes against the default 512 MiB budget: 32
        // idle requests, which one prefix may hold at a per-ip cap of 32
        let idle_heads = idle_heads(512, 16 * MIB, 500).expect("below the connection cap");
        assert_eq!(idle_heads, 32);
        assert!(one_prefix_holds_budget(idle_heads, per_ip));
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
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(5),
        )
        .expect("client")
    }

    #[tokio::test]
    async fn startup_warns_when_chain_ids_differ() {
        let header = api_key_header();
        let client = client();
        let worker = url(chain_id_mock(CHAIN_2017, false).await);

        // the query upstream serves another chain: a warning with both ids
        let other_chain = url(chain_id_mock(CHAIN_1, true).await);
        let check = check_chain_ids(&client, &client, &worker, &other_chain, Some(&header)).await;
        assert_eq!(check, ChainIdCheck::Mismatch { worker: 2017, query: 1 });

        // the same chain: no warning
        let same_chain = url(chain_id_mock(CHAIN_2017, true).await);
        let check = check_chain_ids(&client, &client, &worker, &same_chain, Some(&header)).await;
        assert_eq!(check, ChainIdCheck::Match(2017));

        // the mocks only answer when the key reaches the query upstream and
        // not the worker, so the two outcomes above also prove its routing;
        // without the key the query upstream refuses
        let check = check_chain_ids(&client, &client, &worker, &same_chain, None).await;
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
        let client = client();

        for (worker, query, worker_fails, query_fails) in [
            (&worker, &down, false, true),
            (&down, &query, true, false),
            (&down, &down, true, true),
            (&worker, &junk, false, true),
        ] {
            let check = check_chain_ids(&client, &client, worker, query, Some(&header)).await;
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
        // query client's read timeout
        let hung = url(serve_mock(Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "late"
            }),
        ))
        .await);
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let started = Instant::now();
        let check = check_chain_ids(&client, &client, &worker, &hung, Some(&header)).await;
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
            query_http: None,
            upstream_request_timeout: Duration::from_secs(5),
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
            pool_read_slots: inflight_slots(0),
            draining: Arc::default(),
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
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
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

    #[tokio::test]
    async fn chain_id_check_task_sends_the_query_call_through_the_query_client() {
        let worker = url(chain_id_mock(CHAIN_2017, false).await);
        // a query upstream that accepts the call and never answers
        let hung = url(serve_mock(
            Router::new()
                .route("/", post(|| async { std::future::pending::<&'static str>().await })),
        )
        .await);
        // the worker client waits far past the bound below, the query client
        // gives up after 200 ms, so the check ends in time only when the
        // query call goes through the query client
        let slow = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(600),
        )
        .expect("client");
        let quick = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let workers = [worker_at(&worker)];
        let shutdown = ShutdownNotifier::new();
        let mut app_state = state(&workers, Some(hung), slow);
        app_state.query_http = Some(quick);
        let task = chain_id_check_task(&app_state, &workers, &shutdown).expect("a check task");
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the query call used the worker client")
            .expect("the task succeeds");
    }

    const POLL_INTERVAL: Duration = Duration::from_millis(20);
    const POLL_TIMEOUT: Duration = Duration::from_secs(1);

    /// Serve a node readiness endpoint for worker 0 on an ephemeral port that
    /// reports `accepting`. The returned receiver counts the not-ready answers
    /// it has served.
    async fn mock_node(accepting: Arc<AtomicBool>) -> (UpstreamWorker, watch::Receiver<usize>) {
        let (not_ready_tx, not_ready_answers) = watch::channel(0_usize);
        let app = Router::new().route(
            "/health/workers",
            get(move || {
                let accepting = accepting.load(Ordering::Relaxed);
                if !accepting {
                    not_ready_tx.send_modify(|answers| *answers += 1);
                }
                let body = serde_json::json!({
                    "version": 1,
                    "workers": [{"worker_id": 0, "accepting_transactions": accepting}],
                });
                async move { Json(body) }
            }),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move { axum::serve(listener, app).await });
        let worker = UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse(&format!("http://{addr}/")).expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}/health/workers"))
                .expect("readiness url"),
        };
        (worker, not_ready_answers)
    }

    /// Readiness of `worker` that flips on a single poll result, so each
    /// not-ready answer the mock serves is the one that marks it not ready.
    fn readiness_flipping_at_once(worker: UpstreamWorker) -> Arc<GatewayReadiness> {
        let thresholds = ReadinessThresholds {
            failure: NonZeroU32::MIN,
            success: NonZeroU32::MIN,
            rpc_failure: None,
        };
        Arc::new(GatewayReadiness::new(&[worker], thresholds))
    }

    /// Start the poller on `readiness` and wait until it has marked the
    /// (accepting) mock worker ready.
    async fn start_poller(
        readiness: &Arc<GatewayReadiness>,
        shutdown_delay: Duration,
        shutdown: Noticer,
    ) -> JoinHandle<Result<(), TaskError>> {
        let poller = tokio::spawn(run_poller_until_listener_closes(
            Arc::clone(readiness),
            Client::new(),
            None,
            POLL_INTERVAL,
            POLL_TIMEOUT,
            shutdown_delay,
            shutdown,
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !readiness.any_ready() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("the poller should mark the mock worker ready");
        poller
    }

    #[tokio::test]
    async fn readiness_poller_keeps_polling_through_the_shutdown_delay() {
        let shutdown_delay = Duration::from_secs(2);
        let accepting = Arc::new(AtomicBool::new(true));
        let (worker, mut not_ready_answers) = mock_node(Arc::clone(&accepting)).await;
        let readiness = readiness_flipping_at_once(worker);
        let shutdown = ShutdownNotifier::new();
        let poller = start_poller(&readiness, shutdown_delay, shutdown.subscribe()).await;

        let notice = Instant::now();
        shutdown.notify();
        accepting.store(false, Ordering::Relaxed);

        // a poller that stopped at the notice sends no new probe and finishes
        // at most the one in flight, so it gets at most one not-ready answer.
        // each probe is sent after the previous answer was applied, so by the
        // second not-ready answer the first one has been.
        tokio::time::timeout(shutdown_delay, not_ready_answers.wait_for(|answers| *answers >= 2))
            .await
            .expect("the poller should keep polling during the shutdown delay")
            .expect("mock node alive");
        assert!(!readiness.any_ready(), "the worker went not-ready during the delay");
        assert!(notice.elapsed() < shutdown_delay);
        assert!(!poller.is_finished(), "the poller must run until the listener closes");

        // the listener closes when the delay ends, and the poller stops then
        tokio::time::timeout(shutdown_delay + Duration::from_secs(5), poller)
            .await
            .expect("the poller should stop when the delay ends")
            .expect("join")
            .expect("poller result");
        // timers never fire early
        assert!(notice.elapsed() >= shutdown_delay);
    }

    #[tokio::test]
    async fn readiness_poller_stops_at_the_notice_without_a_delay() {
        let accepting = Arc::new(AtomicBool::new(true));
        let (worker, not_ready_answers) = mock_node(Arc::clone(&accepting)).await;
        let readiness = readiness_flipping_at_once(worker);
        let shutdown = ShutdownNotifier::new();
        let poller = start_poller(&readiness, Duration::ZERO, shutdown.subscribe()).await;

        shutdown.notify();
        accepting.store(false, Ordering::Relaxed);

        tokio::time::timeout(Duration::from_secs(5), poller)
            .await
            .expect("the poller should stop at the notice")
            .expect("join")
            .expect("poller result");
        // no probe was sent after the notice: only one already in flight can
        // have seen the worker go not-ready
        let answers = *not_ready_answers.borrow();
        assert!(answers <= 1, "{answers} not-ready answers after the notice");
    }
}
