//! Metrics for the executor.

use prometheus::{
    default_registry, register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, HistogramVec,
    IntCounter, IntGauge, Registry,
};

// buckets defined in seconds
const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 40.0, 60.0, 80.0,
    100.0, 200.0,
];

const POSITIVE_INT_BUCKETS: &[f64] =
    &[1., 2., 5., 10., 20., 50., 100., 200., 500., 1000., 2000., 5000., 10000., 20000., 50000.];

#[derive(Clone, Debug)]
pub struct ExecutorMetrics {
    /// occupancy of the channel from the `Subscriber` to `Notifier`
    pub tx_notifier: IntGauge,
    /// Number of blocks processed by subscriber
    pub subscriber_processed_blocks: IntCounter,
    /// Round of last certificate seen by subscriber
    pub subscriber_current_round: IntGauge,
    /// Latency between when the certificate has been
    /// created and when it reached the executor
    pub subscriber_certificate_latency: Histogram,
    /// The number of certificates processed by Subscriber
    /// during the recovery period to fetch their payloads.
    pub subscriber_recovered_certificates_count: IntCounter,
    /// The number of pending payload downloads
    pub waiting_elements_subscriber: IntGauge,
    /// Latency between the time when the block has been
    /// created and when it has been fetched for execution
    pub block_execution_latency: Histogram,
    /// This is similar to block_execution_latency but without the latency of
    /// fetching blocks from remote workers.
    pub block_execution_local_latency: HistogramVec,
    /// The number of blocks per committed subdag to be fetched
    pub committed_subdag_block_count: Histogram,
    /// Latency for time taken to fetch all blocks for committed subdag
    /// either from local or remote worker.
    pub block_fetch_for_committed_subdag_total_latency: Histogram,
}

impl ExecutorMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tx_notifier: register_int_gauge_with_registry!(
                "tx_notifier",
                "occupancy of the channel from the `Subscriber` to `Notifier`",
                registry
            )?,
            subscriber_recovered_certificates_count: register_int_counter_with_registry!(
                "subscriber_recovered_certificates_count",
                "The number of certificates processed by Subscriber during the recovery period to fetch their payloads",
                registry
            )?,
            committed_subdag_block_count: register_histogram_with_registry!(
                "committed_subdag_block_count",
                "The number of blocks per committed subdag to be fetched",
                POSITIVE_INT_BUCKETS.to_vec(),
                registry
            )?,
            block_fetch_for_committed_subdag_total_latency: register_histogram_with_registry!(
                "block_fetch_for_committed_subdag_total_latency",
                "Latency for time taken to fetch all blocks for committed subdag either from local or remote worker",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            subscriber_processed_blocks: register_int_counter_with_registry!(
                "subscriber_processed_blocks",
                "Number of blocks processed by subscriber",
                registry
            )?,
            subscriber_current_round: register_int_gauge_with_registry!(
                "subscriber_current_round",
                "Round of last certificate seen by subscriber",
                registry
            )?,
            waiting_elements_subscriber: register_int_gauge_with_registry!(
                "waiting_elements_subscriber",
                "The number of pending payload downloads",
                registry
            )?,
            block_execution_latency: register_histogram_with_registry!(
                "block_execution_latency",
                "Latency between the time when the block has been created and when it has been fetched for execution",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            block_execution_local_latency: register_histogram_vec_with_registry!(
                "block_execution_local_latency",
                "This is similar to block_execution_latency but without the latency of fetching blocks from remote workers.",
                &["source"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
            subscriber_certificate_latency: register_histogram_with_registry!(
                "subscriber_certificate_latency",
                "Latency between when the certificate has been created and when it reached the executor",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )?,
        })
    }
}

impl Default for ExecutorMetrics {
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
