// SPDX-License-Identifier: Apache-2.0
//! Consensus metrics are used throughout consensus to capture metrics while using async channels.

#![allow(missing_docs)]

use axum::{http::StatusCode, routing::get, Router};
use prometheus::{default_registry, TextEncoder};
use std::net::SocketAddr;
use tn_types::{Noticer, TaskManager};
use tokio::net::TcpListener;
use tracing::error;

mod guards;
pub mod histogram;
pub mod metered_channel;
pub use guards::*;

pub const TX_TYPE_SINGLE_WRITER_TX: &str = "single_writer";
pub const TX_TYPE_SHARED_OBJ_TX: &str = "shared_object";

pub const METRICS_ROUTE: &str = "/metrics";

/// Creates a new http server that has as a sole purpose to expose
/// and endpoint that prometheus agent can use to poll for the metrics.
/// A RegistryService is returned that can be used to get access in prometheus Registries.
pub fn start_prometheus_server(addr: SocketAddr, task_manager: &TaskManager, shutdown: Noticer) {
    let app = Router::new().route(METRICS_ROUTE, get(metrics));

    task_manager.spawn_critical_task("ConsensusMetrics", async move {
        // log error but don't crash
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).with_graceful_shutdown(shutdown).await {
                    error!(target: "prometheus", ?e, "server returned error");
                }
            }
            Err(e) => {
                error!(target: "prometheus", ?e, "failed to bind to address");
            }
        };
    });
}

pub async fn metrics() -> (StatusCode, String) {
    let metrics_families = default_registry().gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("unable to encode metrics: {error}"))
        }
    }
}
