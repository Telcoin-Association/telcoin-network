//! Node metrics
use axum::{routing::get, Extension, Router};
use consensus_metrics::{metrics, spawn_logged_monitored_task};
use prometheus::{Error as PrometheusError, Registry};
use std::collections::HashMap;
use tn_types::{AuthorityIdentifier, Multiaddr};
use tokio::task::JoinHandle;

const METRICS_ROUTE: &str = "/metrics";
const PRIMARY_METRICS_PREFIX: &str = "tn_primary";
const _WORKER_METRICS_PREFIX: &str = "tn_worker";

pub fn new_registry() -> Result<Registry, PrometheusError> {
    Registry::new_custom(None, None)
}

pub fn primary_metrics_registry(
    authority_id: AuthorityIdentifier,
) -> Result<Registry, PrometheusError> {
    let mut labels = HashMap::new();
    labels.insert("node_name".to_string(), authority_id.to_string());
    let registry = Registry::new_custom(Some(PRIMARY_METRICS_PREFIX.to_string()), Some(labels))?;

    Ok(registry)
}

#[must_use]
pub fn start_prometheus_server(addr: Multiaddr, registry: &Registry) -> JoinHandle<()> {
    let app = Router::new().route(METRICS_ROUTE, get(metrics)).layer(Extension(registry.clone()));

    let socket_addr = addr.to_socket_addr().expect("failed to convert Multiaddr to SocketAddr");

    spawn_logged_monitored_task!(
        async move {
            axum::Server::bind(&socket_addr)
                .serve(app.into_make_service())
                .await
                .expect("axum server to bind on open port");
        },
        "MetricsServerTask"
    )
}
