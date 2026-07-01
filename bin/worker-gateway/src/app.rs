//! Gateway wiring: build the shared state, spawn the server and readiness
//! poller as managed tasks, and run until shutdown.

use std::sync::Arc;

use reqwest::Client;
use tn_types::{Notifier, TaskManager};
use tracing::info;

use crate::{
    cli::Settings,
    readiness::{run_poller, GatewayReadiness},
    server::{serve, AppState},
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
        readiness_poll_interval,
        readiness_poll_timeout,
        upstream_connect_timeout,
        upstream_request_timeout,
        graceful_shutdown_timeout,
    } = settings;

    info!(
        target: "gateway",
        %listen_addr,
        upstreams = upstreams.len(),
        "starting worker gateway"
    );

    let readiness = Arc::new(GatewayReadiness::new(&upstreams));

    // Dedicated clients: the proxy enforces connect + per-request deadlines; the
    // poller bounds each probe with its own tokio timeout.
    let proxy_client = Client::builder()
        .connect_timeout(upstream_connect_timeout)
        .timeout(upstream_request_timeout)
        .build()?;
    let readiness_client = Client::builder().connect_timeout(upstream_connect_timeout).build()?;

    let mut task_manager = TaskManager::new("worker-gateway");
    // Let in-flight requests drain within the graceful deadline (plus a small
    // margin) before the task manager reaps the server task.
    task_manager.set_join_wait(
        u64::try_from(graceful_shutdown_timeout.as_millis().saturating_add(1_000))
            .unwrap_or(u64::MAX),
    );
    let spawner = task_manager.get_spawner();
    let shutdown = Notifier::new();

    let state = AppState { readiness: Arc::clone(&readiness), http: proxy_client };

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

    spawner.spawn_critical_task(
        "gateway-server",
        serve(
            listen_addr,
            state,
            graceful_shutdown_timeout,
            shutdown.subscribe(),
            shutdown.subscribe(),
        ),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
}
