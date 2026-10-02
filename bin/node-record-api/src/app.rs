//! Daemon wiring: build the shared state, spawn the refresh loop and the server as managed
//! tasks, and run until shutdown.

use std::sync::{Arc, RwLock};

use tn_kad_client::NetworkType;
use tn_types::{ShutdownNotifier, TaskManager};
use tracing::info;

use crate::{
    api::{routes, ApiState},
    cache::{CacheConfig, RecordCache},
    cli::Settings,
    keys::{CommitteeFileKeys, KeySet, KeySource, RpcCommitteeKeys, StaticKeys},
    ratelimit::{run_gc, RateLimiters, DEFAULT_MAX_PER_IP_ENTRIES},
    refresh::{run_loop, RefreshConfig},
    server::{router, serve, ServerLimits},
};

/// Initialize a fmt tracing subscriber honouring the `--log-filter` directive.
pub fn init_tracing(filter: &str) {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(filter);
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

/// Run the daemon until SIGTERM / ctrl-c.
///
/// Spawns the refresh loop and the HTTP server (plus the rate-limit sweep when a limiter is on)
/// as critical tasks under a [`TaskManager`] and blocks on `join_until_exit`, which installs the
/// SIGTERM/ctrl-c handler and drains the tasks on shutdown.
pub async fn run(settings: Settings) -> eyre::Result<()> {
    let Settings {
        listen_addr,
        metrics_addr,
        chain_id,
        worker_id,
        bootstrap,
        committee_file,
        static_keys,
        rpc_url,
        rpc_timeout,
        refresh_interval,
        epoch_grace,
        query_timeout,
        lookup_concurrency,
        record_ttl,
        absent_cycles_before_evict,
        header_read_timeout,
        request_timeout,
        max_connections,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
        rate_limit_per_ip,
        rate_limit_prefix,
        rate_limit_global,
        graceful_shutdown_timeout,
    } = settings;
    let network_type = NetworkType::Worker(worker_id);

    info!(
        target: "tn::node_record_api",
        %listen_addr,
        chain_id,
        worker_id,
        bootstrap = bootstrap.len(),
        rpc_source = rpc_url.is_some(),
        committee_file = committee_file.is_some(),
        static_keys = static_keys.len(),
        refresh_interval = ?refresh_interval,
        "starting node record api"
    );

    // Key sources in resolution order: the live committee first (it carries the epoch), then the
    // file and static floors. Each file was already validated by `into_settings`.
    let mut sources = Vec::new();
    if let Some(url) = rpc_url {
        sources.push(KeySource::Rpc(RpcCommitteeKeys::new(url, rpc_timeout)?));
    }
    if let Some(path) = committee_file {
        sources.push(KeySource::CommitteeFile(CommitteeFileKeys::load(path)?));
    }
    if !static_keys.is_empty() {
        sources.push(KeySource::Static(StaticKeys::new(static_keys)));
    }
    let keys = KeySet::new(sources);

    let cache = Arc::new(RwLock::new(RecordCache::new(CacheConfig {
        record_ttl,
        absent_cycles_before_evict,
        refresh_interval,
    })));

    // Edge rate limiters (per-IP and/or global), or `None` when both are
    // disabled; in that case no rate-limit layer or sweep task is installed.
    let rate_limiters = RateLimiters::new(
        rate_limit_per_ip,
        rate_limit_global,
        DEFAULT_MAX_PER_IP_ENTRIES,
        rate_limit_prefix,
    );
    info!(
        target: "tn::node_record_api",
        rate_limiting = rate_limiters.is_some(),
        max_request_bytes,
        ?tcp_user_timeout,
        ?max_connection_duration,
        "edge protections configured"
    );

    let mut task_manager = TaskManager::new("node-record-api");
    // Let in-flight requests drain within the graceful deadline (plus a small
    // margin) before the task manager reaps the server task.
    task_manager.set_join_wait(
        u64::try_from(graceful_shutdown_timeout.as_millis().saturating_add(1_000))
            .unwrap_or(u64::MAX),
    );
    let spawner = task_manager.get_spawner();
    let shutdown = ShutdownNotifier::new();

    // Optional Prometheus scrape endpoint on its own listener. Reuses the node's
    // `tn-metrics` recorder + server so the daemon's `tn_node_record_api_*` series
    // render alongside the node's `tn_*` metrics under one collector; with
    // `--metrics` unset the `metrics` facade macros stay cheap no-ops.
    //
    // It runs under its OWN task manager, deliberately NOT passed to
    // `join_until_exit`: `start_metrics_server` spawns long-lived accept/upkeep
    // loops that have no graceful-shutdown branch, so joining them would stall
    // shutdown for the whole join deadline. Held in `_metrics_task_manager` for
    // the process lifetime and torn down with the process once the server has
    // drained; the endpoint stays up through the drain so a final scrape still
    // succeeds.
    let _metrics_task_manager = if let Some(metrics_addr) = metrics_addr {
        tn_metrics::install_recorder()?;
        let metrics_task_manager = TaskManager::new("node-record-api-metrics");
        let bound = tn_metrics::start_metrics_server(
            metrics_addr,
            &metrics_task_manager.get_spawner(),
            env!("CARGO_PKG_VERSION"),
            tn_metrics::MetricsHooks::default(),
        )
        .await?;
        // Seed the cache gauges so the series exist (at 0) before the first cycle publishes
        // real values.
        crate::telemetry::set_cache_gauges(0, 0, 0, 0);
        info!(target: "tn::node_record_api", %bound, "metrics endpoint listening");
        Some(metrics_task_manager)
    } else {
        None
    };

    spawner.spawn_critical_task(
        "refresh-loop",
        run_loop(
            RefreshConfig { chain_id, network_type, bootstrap, query_timeout, lookup_concurrency },
            keys,
            Arc::clone(&cache),
            refresh_interval,
            epoch_grace,
            shutdown.subscribe(),
        ),
    );

    // Sweep idle per-IP buckets while the daemon runs (only when a limiter is active).
    if let Some(limiters) = &rate_limiters {
        spawner.spawn_critical_task(
            "rate-limit-gc",
            run_gc(Arc::clone(limiters), shutdown.subscribe()),
        );
    }

    let limits = ServerLimits {
        header_read_timeout,
        request_deadline: request_timeout,
        max_connections,
        tcp_user_timeout,
        max_connection_duration,
        max_request_bytes,
    };
    let app = router(
        routes(ApiState::new(cache, chain_id, network_type)),
        request_timeout,
        max_request_bytes,
        rate_limiters,
    );
    spawner.spawn_critical_task(
        "api-server",
        serve(listen_addr, app, limits, graceful_shutdown_timeout, shutdown.subscribe()),
    );

    task_manager.join_until_exit(shutdown).await?;
    Ok(())
}
