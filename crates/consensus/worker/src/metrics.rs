//! Prometheus metrics for the worker.

use crate::quorum_waiter::QuorumWaiterError;
use parking_lot::Mutex;
use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tn_network_libp2p::PeerTier;
use tn_types::{BlsPublicKey, WorkerId};

/// Why the worker refused a sealed batch's transactions instead of handing them to the
/// transaction forwarder.
///
/// Both variants refuse the whole batch on the observer path (`disburse_txns`): the
/// transactions never reached the forwarder, so the forwarder-side series
/// (`tn_reth.forwarded_txns_*`) cannot see them (issue #1133). A refusal is not a final
/// loss: `disburse_txns` reports `BlockSealError::NotValidator`, so the batch builder
/// keeps the transactions pending and retries them on a later build (issue #1132).
#[derive(Clone, Copy, Debug)]
pub(crate) enum ForwardDropReason {
    /// No committee validator advertised a JSON-RPC endpoint to forward to.
    NoEndpointAdvertised,
    /// Discovering the committee's advertised endpoints failed outright.
    DiscoveryFailed,
}

impl ForwardDropReason {
    /// The `reason` label value this variant records under.
    const fn label(self) -> &'static str {
        match self {
            Self::NoEndpointAdvertised => "no_endpoint_advertised",
            Self::DiscoveryFailed => "discovery_failed",
        }
    }
}

/// Why an inbound sync stream was refused without being served.
///
/// Both refusals happen on stream open, before any request frame is read
/// (`WorkerNetwork::shed_inbound_sync_stream`), so no downstream counter sees the
/// stream (issue #1307). The pair is the signal, not just the sum: the shed budget has
/// no per-peer sub-cap, while the admission path it guards has one. A nonzero
/// `budget_exhausted` rate while `denied` stays low is the signature of shed slots
/// pinned by peers that never read their deny reply. The series carry no peer label, so
/// attribution needs the per-peer `debug!` lines in `shed_inbound_sync_stream`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SyncShedReason {
    /// Admission caps hit; the stream got a shed task that writes `Deny(AtCapacity)`.
    Denied,
    /// Shed budget exhausted; the stream was dropped with no reply.
    BudgetExhausted,
}

impl SyncShedReason {
    /// The `reason` label value this variant records under.
    const fn label(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::BudgetExhausted => "budget_exhausted",
        }
    }
}

/// Derive-backed metric handles for the worker, labeled per `worker`.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_worker")]
struct WorkerMetricHandles {
    /// Total batches that reached quorum, were stored, and were reported to the primary.
    batches_sealed_total: Counter,
    /// Bytes per batch that reached quorum, was stored, and was reported to the primary.
    batch_size_bytes: Histogram,
    /// Transactions per batch that reached quorum, was stored, and was reported to the primary.
    batch_transactions: Histogram,
    /// Time waiting for a proposed batch to reach quorum (or fail).
    quorum_wait_seconds: Histogram,
    /// Retries while reporting a batch to a single committee peer.
    batch_report_retries_total: Counter,
    /// Batches received from peers that failed validation.
    batch_validation_failures_total: Counter,
    /// Time to fetch all missing batches for a consensus output.
    batch_fetch_duration_seconds: Histogram,
}

/// Metrics for the worker batch pipeline.
///
/// One instance per worker id; cheap to clone (all handles are `Arc`-backed). Components
/// that construct this independently for the same worker share the same underlying series -
/// the registry identifies series by (name, labels).
#[derive(Clone, Debug)]
pub struct WorkerMetrics {
    /// The derive-backed metric handles.
    handles: WorkerMetricHandles,
    /// The worker id, kept for per-event labeled counters recorded via the `metrics!` macros.
    worker_id: WorkerId,
    /// `tn_worker.sync_serves_denied_total` for [`PeerTier::Committee`], resolved once here
    /// because a starved batch-serve pool refuses streams many times per second.
    sync_serves_denied_committee: Counter,
    /// `tn_worker.sync_serves_denied_total` for [`PeerTier::Public`], resolved once like
    /// [`Self::sync_serves_denied_committee`].
    sync_serves_denied_public: Counter,
    /// Requester-side batch request counters per peer, resolved on the first denied or
    /// failed request to that peer.
    ///
    /// Shared by clones. The map holds one entry per peer that ever denied or failed a
    /// request, which is the set of series the exporter keeps anyway, so it never outgrows
    /// the registry.
    batch_requests: Arc<Mutex<BTreeMap<BlsPublicKey, BatchRequestCounters>>>,
}

impl WorkerMetrics {
    /// Create the metric handles for `worker_id`.
    pub fn new_for_worker(worker_id: WorkerId) -> Self {
        let worker = worker_id.to_string();
        let sync_serves_denied = |tier: PeerTier| {
            metrics::counter!(
                "tn_worker.sync_serves_denied_total",
                "worker" => worker.clone(),
                "tier" => tier_label(&tier),
            )
        };
        Self {
            sync_serves_denied_committee: sync_serves_denied(PeerTier::Committee),
            sync_serves_denied_public: sync_serves_denied(PeerTier::Public),
            handles: WorkerMetricHandles::new_with_labels(&[("worker", worker)]),
            worker_id,
            batch_requests: Arc::default(),
        }
    }

    /// Record a batch that reached quorum, was stored, and was reported to the primary.
    pub(crate) fn record_batch_sealed(&self, batch_size: usize, num_txs: usize) {
        self.handles.batches_sealed_total.increment(1);
        self.handles.batch_size_bytes.record(batch_size as f64);
        self.handles.batch_transactions.record(num_txs as f64);
    }

    /// Record the time a batch proposal waited on quorum (success or failure).
    pub(crate) fn record_quorum_wait(&self, elapsed: Duration) {
        self.handles.quorum_wait_seconds.record(elapsed);
    }

    /// Record a failed quorum attempt by failure reason.
    pub(crate) fn record_quorum_failure(&self, error: &QuorumWaiterError) {
        let reason = match error {
            QuorumWaiterError::QuorumRejected => "rejected",
            QuorumWaiterError::AntiQuorum => "anti_quorum",
            QuorumWaiterError::Timeout => "timeout",
            QuorumWaiterError::Network => "network",
            QuorumWaiterError::Rpc(_) => "rpc",
            QuorumWaiterError::DroppedReceiver => "dropped",
        };
        metrics::counter!(
            "tn_worker.quorum_failures_total",
            "worker" => self.worker_id.to_string(),
            "reason" => reason,
        )
        .increment(1);
    }

    /// Record a retry while reporting a batch to a single peer.
    pub(crate) fn record_batch_report_retry(&self) {
        self.handles.batch_report_retries_total.increment(1);
    }

    /// Record a peer batch that failed validation.
    pub(crate) fn record_batch_validation_failure(&self) {
        self.handles.batch_validation_failures_total.increment(1);
    }

    /// Record batches recovered while fetching payloads for the primary.
    pub(crate) fn record_batches_fetched(&self, source: &'static str, count: usize) {
        if count == 0 {
            return;
        }
        metrics::counter!(
            "tn_worker.batches_fetched_total",
            "worker" => self.worker_id.to_string(),
            "source" => source,
        )
        .increment(count as u64);
    }

    /// Record the total duration of a payload fetch for the primary.
    pub(crate) fn record_batch_fetch_duration(&self, elapsed: Duration) {
        self.handles.batch_fetch_duration_seconds.record(elapsed);
    }

    /// Record transactions dropped by the worker before they reached the transaction
    /// forwarder, labeled by [`ForwardDropReason`].
    ///
    /// Modeled on [`Self::record_batches_fetched`]: a zero count records nothing, so a path
    /// that never dropped creates no series. Every call site pairs with a `warn!` line; the
    /// counter is what makes the loss visible on a dashboard, where an observer that drops
    /// every transaction it accepts otherwise looks healthy (issue #1133).
    pub(crate) fn record_forward_dropped(&self, reason: ForwardDropReason, count: usize) {
        if count > 0 {
            metrics::counter!(
                "tn_worker.forwarded_txns_dropped_total",
                "worker" => self.worker_id.to_string(),
                "reason" => reason.label(),
            )
            .increment(u64::try_from(count).unwrap_or(u64::MAX));
        }
    }

    /// Record an inbound sync stream refused without being served, labeled by
    /// [`SyncShedReason`].
    ///
    /// One stream per call, so every refusal creates or bumps its series. The counter
    /// pairs with the `debug!` lines in `shed_inbound_sync_stream`, which the default
    /// `info` filter hides. On the requester side a budget-exhausted drop reads as a
    /// generic `failed to read sync ack frame` I/O error, so this responder-side counter
    /// is the only place that condition is visible (issue #1307).
    pub(crate) fn record_sync_stream_shed(&self, reason: SyncShedReason) {
        metrics::counter!(
            "tn_worker.sync_streams_shed_total",
            "worker" => self.worker_id.to_string(),
            "reason" => reason.label(),
        )
        .increment(1);
    }

    /// Record an inbound sync request refused because the batch-serve pool is full for the
    /// peer's `tier` (the `AtCapacity` arm of `admit_and_serve_sync_stream`).
    ///
    /// Unlike [`Self::record_sync_stream_shed`], the stream passed the opening phase: the
    /// request frame was read, and only the serve permit was missing. The requester sees
    /// `Deny(AtCapacity)` and records [`Self::record_batch_request_denied`] on its side.
    pub(crate) fn record_sync_serve_denied(&self, tier: PeerTier) {
        match tier {
            PeerTier::Committee => self.sync_serves_denied_committee.increment(1),
            PeerTier::Public => self.sync_serves_denied_public.increment(1),
        }
    }

    /// Record a batch request that `peer` answered with `Deny`; the peer stays eligible for
    /// a later pass of the same `request_batches` call.
    pub(crate) fn record_batch_request_denied(&self, peer: &BlsPublicKey) {
        self.batch_request_counters(peer).denied.increment(1);
    }

    /// Record a batch request to `peer` that failed for any reason other than `Deny`
    /// (open error, unsupported protocol, timeout, protocol error); the peer is skipped for
    /// the rest of the `request_batches` call.
    pub(crate) fn record_batch_request_failed(&self, peer: &BlsPublicKey) {
        self.batch_request_counters(peer).failed.increment(1);
    }

    /// The requester-side counters for `peer`, resolved and cached on first use.
    fn batch_request_counters(&self, peer: &BlsPublicKey) -> BatchRequestCounters {
        self.batch_requests
            .lock()
            .entry(*peer)
            .or_insert_with(|| BatchRequestCounters::new(self.worker_id, peer))
            .clone()
    }
}

/// Pre-resolved requester-side counters for batch requests sent to one peer.
#[derive(Clone, Debug)]
struct BatchRequestCounters {
    /// `tn_worker.batch_requests_denied_total` for the peer.
    denied: Counter,
    /// `tn_worker.batch_requests_failed_total` for the peer.
    failed: Counter,
}

impl BatchRequestCounters {
    /// Resolve both counters for `peer` under `worker_id`; the `peer` label is the bs58 key.
    fn new(worker_id: WorkerId, peer: &BlsPublicKey) -> Self {
        let (worker, peer) = (worker_id.to_string(), peer.to_string());
        Self {
            denied: metrics::counter!(
                "tn_worker.batch_requests_denied_total",
                "worker" => worker.clone(),
                "peer" => peer.clone(),
            ),
            failed: metrics::counter!(
                "tn_worker.batch_requests_failed_total",
                "worker" => worker,
                "peer" => peer,
            ),
        }
    }
}

/// The `tier` label value for a sync admission [`PeerTier`].
pub(crate) const fn tier_label(tier: &PeerTier) -> &'static str {
    match tier {
        PeerTier::Committee => "committee",
        PeerTier::Public => "public",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::BlsKeypair;

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let metrics = WorkerMetrics::new_for_worker(0);
            metrics.record_batch_sealed(2_048, 7);
            metrics.record_quorum_wait(Duration::from_millis(120));
            metrics.record_quorum_failure(&QuorumWaiterError::Timeout);
            metrics.record_batch_report_retry();
            metrics.record_batch_validation_failure();
            metrics.record_batches_fetched("local", 3);
            metrics.record_batches_fetched("remote", 0); // no-op, no series
            metrics.record_batch_fetch_duration(Duration::from_millis(80));
            metrics.record_forward_dropped(ForwardDropReason::NoEndpointAdvertised, 4);
            metrics.record_forward_dropped(ForwardDropReason::DiscoveryFailed, 0); // no-op, no series
            metrics.record_sync_stream_shed(SyncShedReason::Denied);
            metrics.record_sync_stream_shed(SyncShedReason::Denied);
            metrics.record_sync_stream_shed(SyncShedReason::BudgetExhausted);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (key, _, _, value) = find("tn_worker.batches_sealed_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "worker" && l.value() == "0"));

        let (key, _, _, value) = find("tn_worker.quorum_failures_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "reason" && l.value() == "timeout"));

        let (key, _, _, value) = find("tn_worker.batches_fetched_total");
        assert!(matches!(value, DebugValue::Counter(3)));
        assert!(key.key().labels().any(|l| l.key() == "source" && l.value() == "local"));

        find("tn_worker.batch_size_bytes");
        find("tn_worker.batch_transactions");
        find("tn_worker.quorum_wait_seconds");
        find("tn_worker.batch_report_retries_total");
        find("tn_worker.batch_validation_failures_total");
        find("tn_worker.batch_fetch_duration_seconds");

        let (key, _, _, value) = find("tn_worker.forwarded_txns_dropped_total");
        assert!(matches!(value, DebugValue::Counter(4)));
        assert!(key
            .key()
            .labels()
            .any(|l| l.key() == "reason" && l.value() == "no_endpoint_advertised"));
        assert!(key.key().labels().any(|l| l.key() == "worker" && l.value() == "0"));

        // the zero-count fetch must not create a series
        assert!(
            !snapshot.iter().any(|(key, ..)| key.key().name() == "tn_worker.batches_fetched_total"
                && key.key().labels().any(|l| l.value() == "remote")),
            "zero-count fetch should not register a remote series"
        );

        // neither must the zero-count forward drop
        assert!(
            !snapshot.iter().any(|(key, ..)| {
                key.key().name() == "tn_worker.forwarded_txns_dropped_total"
                    && key.key().labels().any(|l| l.value() == "discovery_failed")
            }),
            "zero-count forward drop should not register a series"
        );

        // one shed series per reason, both under this worker's label
        let shed_series = |reason: &str| {
            snapshot.iter().find(|(key, ..)| {
                key.key().name() == "tn_worker.sync_streams_shed_total"
                    && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
                    && key.key().labels().any(|l| l.key() == "worker" && l.value() == "0")
            })
        };
        assert!(
            shed_series("denied")
                .is_some_and(|(.., value)| matches!(value, DebugValue::Counter(2))),
            "denied shed series should count both refusals"
        );
        assert!(
            shed_series("budget_exhausted")
                .is_some_and(|(.., value)| matches!(value, DebugValue::Counter(1))),
            "budget-exhausted shed series should count the dropped stream"
        );
    }

    /// The `tier` label of each admission tier.
    #[test]
    fn tier_label_maps_each_tier() {
        assert_eq!(tier_label(&PeerTier::Committee), "committee");
        assert_eq!(tier_label(&PeerTier::Public), "public");
    }

    /// The batch-starvation series: serve denials per tier on the responder side, and denied
    /// and failed batch requests per peer on the requester side.
    #[test]
    fn test_batch_starvation_counters() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let key = |seed| *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public();
        let (denier, failer) = (key(1), key(2));

        metrics::with_local_recorder(&recorder, || {
            let metrics = WorkerMetrics::new_for_worker(3);
            metrics.record_sync_serve_denied(PeerTier::Public);
            metrics.record_sync_serve_denied(PeerTier::Public);
            metrics.record_batch_request_denied(&denier);
            // a clone shares the per-peer cache and records into the same series
            metrics.clone().record_batch_request_denied(&denier);
            metrics.record_batch_request_failed(&failer);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let count = |name: &str, label: (&str, &str)| {
            let series: Vec<_> = snapshot
                .iter()
                .filter(|(key, ..)| {
                    key.key().name() == name
                        && key.key().labels().any(|l| l.key() == "worker" && l.value() == "3")
                        && key.key().labels().any(|l| l.key() == label.0 && l.value() == label.1)
                })
                .collect();
            match series.as_slice() {
                [] => None,
                [(.., DebugValue::Counter(n))] => Some(*n),
                other => panic!("{name} {label:?}: expected one counter series, got {other:?}"),
            }
        };

        assert_eq!(count("tn_worker.sync_serves_denied_total", ("tier", "public")), Some(2));
        // resolved eagerly, so the committee series exists at zero before any denial
        assert_eq!(count("tn_worker.sync_serves_denied_total", ("tier", "committee")), Some(0));

        let (denier, failer) = (denier.to_string(), failer.to_string());
        assert_eq!(count("tn_worker.batch_requests_denied_total", ("peer", &denier)), Some(2));
        assert_eq!(count("tn_worker.batch_requests_failed_total", ("peer", &failer)), Some(1));
        // each peer's counters resolve together on its first event
        assert_eq!(count("tn_worker.batch_requests_failed_total", ("peer", &denier)), Some(0));
        assert_eq!(count("tn_worker.batch_requests_denied_total", ("peer", &failer)), Some(0));
    }
}
