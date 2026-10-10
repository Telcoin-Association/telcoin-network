//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::sync::Arc;

use eyre::WrapErr as _;
use tn_types::{ShutdownNotifier, TaskManager};
use tracing::{info, warn};

use crate::{
    cli::Settings,
    proxy::{client_builder, proxy_client, UpstreamOrigin},
    ratelimit::{run_gc, RateLimiters, DEFAULT_MAX_PER_IP_ENTRIES},
    readiness::{run_poller, GatewayReadiness},
    server::{serve, AppState, ServerLimits},
};

/// Context for a failure to build an upstream client. A TLS file rustls
/// rejects (a certificate it cannot parse, or a client key that does not match
/// its certificate) is already reported by flag and path when the settings are
/// resolved, so this only points at the TLS flags as a backstop.
const TLS_SETTINGS_HINT: &str = "cannot build the upstream clients; check --upstream-ca-cert, \
                                 --upstream-client-cert and --upstream-client-key";

/// Run the gateway until SIGTERM / ctrl-c.
///
/// Spawns two critical tasks (the HTTP server and the readiness poller) under a
/// [`TaskManager`] and blocks on `join_until_exit`, which installs the
/// SIGTERM/ctrl-c handler and drains the tasks on shutdown.
pub(crate) async fn run(settings: Settings) -> eyre::Result<()> {
    ensure_root_store(&settings, native_root_count)?;

    // Both upstream clients start from the same TLS settings (see
    // `client_builder`); they are finished below.
    let proxy_client_builder = client_builder(&settings);
    let readiness_client_builder = client_builder(&settings);
    let Settings {
        listen_addr,
        upstreams,
        query_upstream,
        readiness_poll_interval,
        readiness_poll_timeout,
        upstream_connect_timeout: _,
        upstream_request_timeout,
        upstream_ca_certs: _,
        upstream_identity: _,
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
    let proxy_client =
        proxy_client(proxy_client_builder, upstream_request_timeout).wrap_err(TLS_SETTINGS_HINT)?;
    let readiness_client = readiness_client_builder.build().wrap_err(TLS_SETTINGS_HINT)?;

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

    let state = AppState { readiness: Arc::clone(&readiness), http: proxy_client, query_upstream };

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
}
