//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::{sync::Arc, time::Duration};

use reqwest::Client;
use tn_types::{Noticer, ShutdownNotifier, TaskError, TaskManager};
use tracing::info;

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
        rate_limit_per_ip,
        rate_limit_prefix,
        rate_limit_global,
        shutdown_delay,
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
    let proxy_client = proxy_client(upstream_connect_timeout, upstream_request_timeout)?;
    let readiness_client = Client::builder().connect_timeout(upstream_connect_timeout).build()?;

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
        draining: Arc::default(),
    };

    spawner.spawn_critical_task(
        "readiness-poller",
        run_poller_until_listener_closes(
            readiness,
            readiness_client,
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
            shutdown_delay,
            graceful_shutdown_timeout,
            shutdown.subscribe(),
        ),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
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
    poll_interval: Duration,
    poll_timeout: Duration,
    shutdown_delay: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    if shutdown_delay.is_zero() {
        return run_poller(readiness, client, poll_interval, poll_timeout, shutdown).await;
    }
    let listener_closed = ShutdownNotifier::new();
    let poller =
        run_poller(readiness, client, poll_interval, poll_timeout, listener_closed.subscribe());
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
    use crate::config::UpstreamWorker;
    use axum::{routing::get, Json, Router};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::{net::TcpListener, sync::watch, task::JoinHandle, time::Instant};
    use url::Url;

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
        let readiness = Arc::new(GatewayReadiness::new(&[worker]));
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
        let readiness = Arc::new(GatewayReadiness::new(&[worker]));
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
