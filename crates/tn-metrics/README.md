# tn-metrics

Prometheus metrics recorder and scrape endpoint for telcoin-network nodes.

Enabled with `telcoin-network node --metrics <ip>:<port>`. One endpoint per node serves
both telcoin (`tn_*`) and reth (`reth_*`) metric namespaces in the Prometheus text format.

## Architecture

- **Recorder** (`install_recorder`): installs the global [`metrics`] recorder backed by a
  `metrics-exporter-prometheus` registry. A thin wrapper applies a *selective* prefix:
  metric names starting with `tn` pass through untouched, everything else gets `reth.`
  prepended. The result is that reth's built-in instrumentation (db, txpool, provider) and
  process metrics render as `reth_db_*`, `reth_process_*`, etc. — identical to a stock reth
  node — while telcoin metrics keep their `tn_*` names.
- **Server** (`start_metrics_server`): a hand-rolled ~100-line HTTP responder following the
  node healthcheck pattern. No request parsing, no connection limits; every connection
  receives the rendered registry with `Content-Type: text/plain; version=0.0.4`. Runs as a
  critical task on the node's `TaskManager` and performs registry upkeep every 5 seconds.
- **Hooks** (`MetricsHooks`): callbacks run before each scrape for sampled (rather than
  event-driven) metrics. The default set collects process stats; the node adds a hook for
  reth database table sizes.

## Naming convention

Instrumented crates use the `reth_metrics::Metrics` derive with a `tn_<scope>` scope:

```rust,ignore
#[derive(Metrics)]
#[metrics(scope = "tn_worker")]
struct WorkerMetrics {
    /// Total batches sealed by this worker.
    batches_sealed_total: Counter,
}
```

The derive joins scope and field with `.` (`tn_worker.batches_sealed_total`) and the
exporter sanitizes `.` to `_`, rendering `tn_worker_batches_sealed_total`. Doc comments
become Prometheus `HELP` text.

Rules:

- No high-cardinality labels (no peer ids, digests, or epochs as label values).
- A metric that needs a label has no derive field at all: the derive generates one unlabelled
  series per field. Use the `metrics::gauge!` / `metrics::counter!` macros with the matching
  `metrics::describe_*!`, which also resolve the recorder on every call. `tn-reth`'s
  `tn_reth.epoch_close_system_call_gas_used` (`crates/tn-reth/src/metrics.rs`) is the in-repo
  example — one series per epoch-boundary system call, keyed by a closed `call` label.
- The derive's `Default::default()` caches the whole struct in a `static OnceLock`, so the first
  construction in a process fixes which recorder every later handle records against.
  `Metrics::new_with_labels` does not cache, so prefer it over `default()` wherever a second
  recording in one process has to reach a different recorder — which is any test that installs a
  local recorder. `tn-reth`'s `RethEnvMetrics` tests do exactly this. Production behaviour is
  identical either way, since the node installs one recorder before the first metric exists.
- Epoch / round / height are gauge **values**, not labels.
- Histograms must end in `_seconds` or `_bytes`, or have explicit buckets registered in
  `recorder.rs` — otherwise they render as non-aggregatable quantile summaries.

## Install ordering invariant

`install_recorder()` MUST run before any reth components are constructed — in particular
before `RethEnv::new_database`. Derive-style metric handles bind to whatever recorder is
installed at construction time; metrics registered against the default noop recorder are
silently lost. The CLI installs the recorder at the top of the `node` command for this
reason.

## Testing instrumented crates

The global recorder can only be installed once per process. Only this crate's own tests
install it. Everywhere else, use a local recorder:

```rust,ignore
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

let recorder = DebuggingRecorder::new();
let snapshotter = recorder.snapshotter();
metrics::with_local_recorder(&recorder, || {
    // `new_with_labels`, not `default()`: the latter caches in a `OnceLock` and would bind to
    // whichever recorder was installed the first time this type was constructed in the process
    let metrics = WorkerMetrics::new_with_labels(Vec::<metrics::Label>::new());
    metrics.batches_sealed_total.increment(1);
});
let snapshot = snapshotter.snapshot().into_vec();
// assert on (key, unit, description, value) tuples
```

In-process multi-node test harnesses share one global registry, so counters merge across
nodes — a test-only artifact. In production each node is its own process.

## Security

The endpoint accepts any connection and responds unconditionally. Operators must firewall
the port. The recommended deployment binds to loopback (`127.0.0.1:9001`) with a local
scraper (e.g. Grafana Alloy) relaying to remote storage.
