//! Worker metrics

use prometheus::{
    default_registry, register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    Registry,
};
use std::sync::Arc;

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4,
    1.6, 1.8, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5, 10.,
    12.5, 15., 17.5, 20., 25., 30., 60., 90., 120., 180., 300.,
];

#[derive(Clone)]
pub struct Metrics {
    pub worker_metrics: Arc<WorkerMetrics>,
    pub channel_metrics: Arc<WorkerChannelMetrics>,
}

impl Metrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        // Essential/core metrics across the worker node
        let worker_metrics = Arc::new(WorkerMetrics::try_new(registry)?);

        // Channel metrics
        let channel_metrics = Arc::new(WorkerChannelMetrics::try_new(registry)?);

        Ok(Metrics { worker_metrics, channel_metrics })
    }

    pub fn new_with_registry(registry: &Registry) -> Self {
        Self::try_new(registry).expect("Prometheus error, are you using it wrong?")
    }
}

impl Default for Metrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => {
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerMetrics {
    /// Number of created batches from the batch_maker
    pub created_batch_size: HistogramVec,
    /// Time taken to create a batch
    pub created_batch_latency: HistogramVec,
    /// Latency of broadcasting batches to a quorum in seconds.
    pub batch_broadcast_quorum_latency: Histogram,
    /// Counter of remote/local batch fetch statuses.
    pub batch_fetch: IntCounterVec,
    /// Time it takes to download a payload from local worker peer
    pub worker_local_fetch_latency: Histogram,
    /// Time it takes to download a payload from remote peer
    pub worker_remote_fetch_latency: Histogram,
    /// The number of pending remote calls to request_batches
    pub pending_remote_request_batches: IntGauge,
}

impl WorkerMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            created_batch_size: register_histogram_vec_with_registry!(
                "created_batch_size",
                "Size in bytes of the created batches",
                &["reason"],
                // buckets with size in bytes
                vec![
                    100.0,
                    500.0,
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    20_000.0,
                    50_000.0,
                    100_000.0,
                    250_000.0,
                    500_000.0,
                    1_000_000.0
                ],
                registry
            )?,
            created_batch_latency: register_histogram_vec_with_registry!(
                "created_batch_latency",
                "The latency of creating (sealing) a batch",
                &["reason"],
                // buckets in seconds
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            batch_broadcast_quorum_latency: register_histogram_with_registry!(
                "batch_broadcast_quorum_latency",
                "The latency of broadcasting batches to a quorum in seconds",
                // buckets in seconds
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            batch_fetch: register_int_counter_vec_with_registry!(
                "batch_fetch",
                "Counter of remote/local batch fetch statuses",
                &["source", "status"],
                registry
            )?,
            worker_local_fetch_latency: register_histogram_with_registry!(
                "worker_local_fetch_latency",
                "Time it takes to download a payload from local storage",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            worker_remote_fetch_latency: register_histogram_with_registry!(
                "worker_remote_fetch_latency",
                "Time it takes to download a payload from remote worker peer",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            pending_remote_request_batches: register_int_gauge_with_registry!(
                "pending_remote_request_batches",
                "The number of pending remote calls to request_batches",
                registry
            )?,
        })
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "tn::metrics", ?e, "Executor::try_new metrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerChannelMetrics {
    /// occupancy of the channel from the `worker::TxReceiverhandler` to the
    /// `worker::BatchProvider`
    pub tx_batch_maker: IntGauge,
    /// occupancy of the channel from the `worker::BatchProvider` to the `worker::QuorumWaiter`
    pub tx_quorum_waiter: IntGauge,
    /// total received from the channel from the `worker::TxReceiverhandler` to the
    /// `worker::BatchProvider`
    pub tx_batch_maker_total: IntCounter,
    /// total received from the channel from the `worker::BatchProvider` to the
    /// `worker::QuorumWaiter`
    pub tx_quorum_waiter_total: IntCounter,
}

impl WorkerChannelMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tx_batch_maker: register_int_gauge_with_registry!(
                "tx_batch_maker",
                "occupancy of the channel from the `worker::TxReceiverhandler` to the `worker::BatchProvider`",
                registry
            )?,
            tx_quorum_waiter: register_int_gauge_with_registry!(
                "tx_quorum_waiter",
                "occupancy of the channel from the `worker::BatchProvider` to the `worker::QuorumWaiter`",
                registry
            )?,

            // Totals:
            tx_batch_maker_total: register_int_counter_with_registry!(
                "tx_batch_maker_total",
                "total received from the channel from the `worker::TxReceiverhandler` to the `worker::BatchProvider`",
                registry
            )?,
            tx_quorum_waiter_total: register_int_counter_with_registry!(
                "tx_quorum_waiter_total",
                "total received from the channel from the `worker::BatchProvider` to the `worker::QuorumWaiter`",
                registry
            )?,
        })
    }
}
