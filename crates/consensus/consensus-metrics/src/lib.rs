// SPDX-License-Identifier: Apache-2.0
//! Consensus metrics are used throughout consensus to capture metrics while using async channels.

#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility,
    unused_crate_dependencies
)]

use axum::{http::StatusCode, routing::get, Router};
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tn_types::{Noticer, TaskManager};

use once_cell::sync::OnceCell;
use prometheus::{
    default_registry, register_int_gauge_vec_with_registry, IntGaugeVec, Registry, TextEncoder,
};
use tracing::warn;

pub use scopeguard;

mod guards;
pub mod histogram;
pub mod metered_channel;
pub use guards::*;

pub const TX_TYPE_SINGLE_WRITER_TX: &str = "single_writer";
pub const TX_TYPE_SHARED_OBJ_TX: &str = "shared_object";

#[derive(Debug)]
pub struct Metrics {
    pub tasks: IntGaugeVec,
    pub futures: IntGaugeVec,
    pub channels: IntGaugeVec,
    pub scope_iterations: IntGaugeVec,
    pub scope_duration_ns: IntGaugeVec,
    pub scope_entrance: IntGaugeVec,
}

impl Metrics {
    /// Create new [Metrics] for consensus performance.
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tasks: register_int_gauge_vec_with_registry!(
                "monitored_tasks",
                "Number of running tasks per callsite.",
                &["callsite"],
                registry,
            )?,
            futures: register_int_gauge_vec_with_registry!(
                "monitored_futures",
                "Number of pending futures per callsite.",
                &["callsite"],
                registry,
            )?,
            channels: register_int_gauge_vec_with_registry!(
                "monitored_channels",
                "Size of channels.",
                &["name"],
                registry,
            )?,
            scope_entrance: register_int_gauge_vec_with_registry!(
                "monitored_scope_entrance",
                "Number of entrance in the scope.",
                &["name"],
                registry,
            )?,
            scope_iterations: register_int_gauge_vec_with_registry!(
                "monitored_scope_iterations",
                "Total number of times where the monitored scope runs",
                &["name"],
                registry,
            )?,
            scope_duration_ns: register_int_gauge_vec_with_registry!(
                "monitored_scope_duration_ns",
                "Total duration in nanosecs where the monitored scope is running",
                &["name"],
                registry,
            )?,
        })
    }
}

/// [OnceCell] container for consensus [Metrics].
static METRICS: OnceCell<Metrics> = OnceCell::new();

/// Set the inner [Metrics] for [OnceCell].
fn init_metrics() {
    if let Ok(metrics) = Metrics::try_new(default_registry()) {
        let _ = METRICS.set(metrics).inspect_err(|_| warn!("init_metrics registry overwritten"));
    }
}

/// Return the inner [Metrics].
pub fn get_metrics() -> Option<&'static Metrics> {
    METRICS.get()
}

#[macro_export]
macro_rules! monitored_future {
    ($fut: expr) => {{
        monitored_future!(futures, $fut, "", INFO, false)
    }};

    ($fut: expr, $name: expr, $logging_level: ident) => {{
        monitored_future!(futures, $fut, $name, $logging_level, false)
    }};

    ($fut: expr, $name: expr) => {{
        monitored_future!(futures, $fut, $name, INFO, false)
    }};

    ($metric: ident, $fut: expr, $name: expr, $logging_level: ident, $logging_enabled: expr) => {{
        let location: &str = if $name.is_empty() {
            concat!(file!(), ':', line!())
        } else {
            concat!(file!(), ':', $name)
        };

        async move {
            let metrics = consensus_metrics::get_metrics();

            let _metrics_guard = if let Some(m) = metrics {
                m.$metric.with_label_values(&[location]).inc();
                Some(consensus_metrics::scopeguard::guard(m, |metrics| {
                    m.$metric.with_label_values(&[location]).dec();
                }))
            } else {
                None
            };
            let _logging_guard = if $logging_enabled {
                Some(consensus_metrics::scopeguard::guard((), |_| {
                    tracing::event!(
                        tracing::Level::$logging_level,
                        "Future {} completed",
                        location
                    );
                }))
            } else {
                None
            };

            if $logging_enabled {
                tracing::event!(tracing::Level::$logging_level, "Spawning future {}", location);
            }

            $fut.await
        }
    }};
}

#[macro_export]
macro_rules! spawn_monitored_task {
    ($fut: expr) => {
        tokio::task::spawn(consensus_metrics::monitored_future!(tasks, $fut, "", INFO, false))
    };
}

#[macro_export]
macro_rules! spawn_logged_monitored_task {
    ($fut: expr) => {
        tokio::task::spawn(consensus_metrics::monitored_future!(tasks, $fut, "", INFO, true))
    };

    ($fut: expr, $name: expr) => {
        tokio::task::spawn(consensus_metrics::monitored_future!(tasks, $fut, $name, INFO, true))
    };

    ($fut: expr, $name: expr, $logging_level: ident) => {
        tokio::task::spawn(consensus_metrics::monitored_future!(
            tasks,
            $fut,
            $name,
            $logging_level,
            true
        ))
    };
}

pub struct MonitoredScopeGuard {
    metrics: &'static Metrics,
    name: &'static str,
    timer: Instant,
}

impl Drop for MonitoredScopeGuard {
    fn drop(&mut self) {
        self.metrics
            .scope_duration_ns
            .with_label_values(&[self.name])
            .add(self.timer.elapsed().as_nanos() as i64);
        self.metrics.scope_entrance.with_label_values(&[self.name]).dec();
    }
}

/// This function creates a named scoped object, that keeps track of
/// - the total iterations where the scope is called in the `monitored_scope_iterations` metric.
/// - and the total duration of the scope in the `monitored_scope_duration_ns` metric.
///
/// The monitored scope should be single threaded, e.g. the scoped object encompass the lifetime of
/// a select loop or guarded by mutex.
/// Then the rate of `monitored_scope_duration_ns`, converted to the unit of sec / sec, would be
/// how full the single threaded scope is running.
pub fn monitored_scope(name: &'static str) -> Option<MonitoredScopeGuard> {
    let metrics = get_metrics();
    if let Some(m) = metrics {
        m.scope_iterations.with_label_values(&[name]).inc();
        m.scope_entrance.with_label_values(&[name]).inc();
        Some(MonitoredScopeGuard { metrics: m, name, timer: Instant::now() })
    } else {
        None
    }
}

pub trait MonitoredFutureExt: Future + Sized {
    fn in_monitored_scope(self, name: &'static str) -> MonitoredScopeFuture<Self>;
}

impl<F: Future> MonitoredFutureExt for F {
    fn in_monitored_scope(self, name: &'static str) -> MonitoredScopeFuture<Self> {
        MonitoredScopeFuture { f: Box::pin(self), _scope: monitored_scope(name) }
    }
}

pub struct MonitoredScopeFuture<F: Sized> {
    f: Pin<Box<F>>,
    _scope: Option<MonitoredScopeGuard>,
}

impl<F: Future> Future for MonitoredScopeFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.f.as_mut().poll(cx)
    }
}

pub const METRICS_ROUTE: &str = "/metrics";

// Creates a new http server that has as a sole purpose to expose
// and endpoint that prometheus agent can use to poll for the metrics.
// A RegistryService is returned that can be used to get access in prometheus Registries.
pub fn start_prometheus_server(addr: SocketAddr, task_manager: &TaskManager, shutdown: Noticer) {
    init_metrics();
    if cfg!(msim) {
        // prometheus uses difficult-to-support features such as TcpSocket::from_raw_fd(), so we
        // can't yet run it in the simulator.
        warn!("not starting prometheus server in simulator");
        return;
    }

    let app = Router::new().route(METRICS_ROUTE, get(metrics));

    task_manager.spawn_critical_task("ConsensusMetrics", async move {
        if let Err(e) = axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::error!(target: "prometheus", ?e, "server returned error");
        }
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
