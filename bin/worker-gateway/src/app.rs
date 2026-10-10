//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use reqwest::Client;
use tn_types::{ShutdownNotifier, TaskManager};
use tracing::{info, warn};

use crate::{
    cli::Settings,
    proxy::{proxy_client, UpstreamOrigin},
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
        readiness_poll_interval,
        readiness_poll_timeout,
        upstream_connect_timeout,
        upstream_request_timeout,
        header_read_timeout,
        max_connections,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        max_inflight_request_bytes,
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
        max_inflight_request_bytes = max_inflight_request_bytes.map_or(0, NonZeroU32::get),
        ?tcp_user_timeout,
        ?max_connection_duration,
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
        max_inflight_request_bytes,
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
