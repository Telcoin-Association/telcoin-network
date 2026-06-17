//! Prometheus metrics for telcoin-network nodes.
//!
//! This crate owns the two pieces every node needs to expose metrics:
//!
//! 1. [`install_recorder`] - installs the global `metrics` recorder backed by a Prometheus
//!    registry. Telcoin metrics (names starting with `tn`) pass through untouched; all other
//!    metrics (reth's built-in db/txpool/provider instrumentation, process metrics) get a `reth.`
//!    prefix so they render identically to a stock reth node.
//! 2. [`start_metrics_server`] - a minimal HTTP endpoint that renders the registry for Prometheus
//!    scrapes, with pre-scrape [`MetricsHooks`] for sampled metrics (process stats, db table
//!    sizes).
//!
//! # Naming convention
//!
//! Instrumented crates declare metrics with the `reth_metrics::Metrics` derive using a
//! `tn_<scope>` scope, e.g. `#[metrics(scope = "tn_worker")]` with field
//! `batches_sealed_total` records `tn_worker.batches_sealed_total`, which the exporter
//! sanitizes to `tn_worker_batches_sealed_total`.
//!
//! # Install ordering invariant
//!
//! [`install_recorder`] MUST run before any reth components are constructed (before
//! `RethEnv::new_database`). Derive-style metric handles bind to the recorder present at
//! construction; anything registered against the default noop recorder is silently lost.
//!
//! # Testing instrumented crates
//!
//! Never install the global recorder in tests outside this crate - the global registry is
//! shared per-process, so parallel tests would stomp each other. Use a local recorder:
//!
//! ```ignore
//! use metrics_util::debugging::{DebuggingRecorder, DebugValue};
//!
//! let recorder = DebuggingRecorder::new();
//! let snapshotter = recorder.snapshotter();
//! metrics::with_local_recorder(&recorder, || {
//!     let metrics = MyMetrics::default();
//!     metrics.my_counter.increment(1);
//! });
//! let snapshot = snapshotter.snapshot().into_vec();
//! // assert the metric registered and updated
//! ```
//!
//! Note for in-process multi-node test harnesses: all nodes share one global registry, so
//! counters merge across nodes. In production each node is its own process, so this is a
//! test-only artifact.

mod recorder;
mod server;

pub use recorder::install_recorder;
pub use server::{start_metrics_server, MetricsHooks};
