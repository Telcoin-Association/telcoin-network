//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use eyre::WrapErr as _;
use reqwest::Client;
use tn_types::{ShutdownNotifier, TaskManager};
use tracing::{info, warn};

use crate::{
    cli::Settings,
    dns::{LookupHost, SystemLookup},
    proxy::{client_builder, proxy_client, UpstreamOrigin, WARNING_THROTTLES},
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
        recent_submission_ttl,
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
    // travels like a redirected read, through the same DNS cache and lookup cap.
    let query_probe =
        query_upstream.clone().map(|url| QueryProbe { url, client: proxy_client.clone() });
    // submissions are remembered only for the redirect's hash routing
    let recent_submissions = recent_submission_ttl
        .filter(|_| query_upstream.is_some())
        .map(|ttl| Arc::new(RecentSubmissions::new(ttl, RECENT_SUBMISSIONS_CAPACITY)));
    let state = AppState {
        readiness: Arc::clone(&readiness),
        http: proxy_client,
        query_upstream,
        recent_submissions,
        max_batch_len,
        query_request_timeout,
        submission_slots: inflight_slots(max_inflight_submissions),
        query_slots: inflight_slots(max_inflight_queries),
        upstream_slots: inflight_slots(max_upstream_inflight),
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::Parser as _;

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
}
