//! Prometheus metrics for the worker.

use crate::quorum_waiter::QuorumWaiterError;
use parking_lot::Mutex;
use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};
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
    /// Reported batches refused because the reporter is at its bound of uncommitted stored
    /// batches (issue #1794).
    batch_reports_over_quota_total: Counter,
    /// Time to fetch all missing batches for a consensus output.
    batch_fetch_duration_seconds: Histogram,
    /// Time to report a sealed batch's digest to the primary (success or failure).
    batch_report_seconds: Histogram,
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
    /// Requester-side batch request counters per `peer` series, resolved on the first denied
    /// or failed request recorded under it. The key is a committee member's key, or `None` for
    /// the `peer="other"` series every other peer shares.
    ///
    /// Shared by clones. Bounded by the committee it was last pruned against, plus `other`:
    /// adding an entry first drops the members missing from the committee it is recorded
    /// with, so a member that left at an epoch change stays cached only until the next entry
    /// is added (see [`Self::with_batch_request_counters`]).
    batch_requests: Arc<Mutex<BTreeMap<Option<BlsPublicKey>, BatchRequestCounters>>>,
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

    /// Record the time one report of a sealed batch to the primary took (success or failure).
    pub(crate) fn record_batch_report(&self, elapsed: Duration) {
        self.handles.batch_report_seconds.record(elapsed);
    }

    /// Record a retry while reporting a batch to a single peer.
    pub(crate) fn record_batch_report_retry(&self) {
        self.handles.batch_report_retries_total.increment(1);
    }

    /// Record a peer batch that failed validation.
    pub(crate) fn record_batch_validation_failure(&self) {
        self.handles.batch_validation_failures_total.increment(1);
    }

    /// Record a reported batch refused because the reporter is at its bound of uncommitted
    /// stored batches.
    pub(crate) fn record_batch_report_over_quota(&self) {
        self.handles.batch_reports_over_quota_total.increment(1);
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

    /// Record a batch request that `peer` answered with `Deny(AtCapacity)` or
    /// `Deny(Unavailable)`; the peer stays eligible for a later pass of the same
    /// `request_batches` call. A `Deny(Busy)` is not counted: worker serves never answer it.
    ///
    /// `committee` is the requester's current committee: a member records under its own
    /// `peer` series and every other peer under `peer="other"` (see
    /// [`Self::with_batch_request_counters`]).
    pub(crate) fn record_batch_request_denied(
        &self,
        peer: &BlsPublicKey,
        committee: &HashSet<BlsPublicKey>,
    ) {
        self.with_batch_request_counters(peer, committee, |counters| counters.denied.increment(1));
    }

    /// Record a batch request to `peer` that failed for any reason other than `Deny`
    /// (open error, unsupported protocol, a deadline before any batch arrived, protocol error,
    /// or `Ack` then `End` with none of the requested batches); the peer is skipped for the
    /// rest of the `request_batches` call. A stream cut by a deadline after some batches
    /// arrived keeps them and is not counted here.
    ///
    /// `committee` picks the `peer` series as for [`Self::record_batch_request_denied`].
    pub(crate) fn record_batch_request_failed(
        &self,
        peer: &BlsPublicKey,
        committee: &HashSet<BlsPublicKey>,
    ) {
        self.with_batch_request_counters(peer, committee, |counters| counters.failed.increment(1));
    }

    /// Apply `record` to the requester-side counters of the series `peer` records under,
    /// resolving and caching them on first use.
    ///
    /// Only members of `committee`, the requester's current committee, get a series of their
    /// own, keyed by their key. Any peer with a BLS binding can be asked for batches
    /// (`connected_peers` lists the committee first, then every other connected or dialing
    /// peer), so a key label for the rest would add two series for every identity that denies
    /// or fails a request; they all record under `None`, labelled `other`.
    ///
    /// Before it caches a new entry, this drops every cached member that is not in
    /// `committee`, so the map holds at most the committee it was last pruned against, plus
    /// `other`. A member that left the committee at an epoch change records under `other`
    /// from then on; its own series stays in the exporter at its last value, so the exported
    /// series are bounded by the committee members seen over the node's uptime, plus `other`.
    ///
    /// `record` runs under the map's lock, so it must stay synchronous and short (one counter
    /// increment); the lock is never held across an `.await`.
    fn with_batch_request_counters(
        &self,
        peer: &BlsPublicKey,
        committee: &HashSet<BlsPublicKey>,
        record: impl FnOnce(&BatchRequestCounters),
    ) {
        let member = committee.contains(peer).then_some(*peer);
        let mut batch_requests = self.batch_requests.lock();
        if !batch_requests.contains_key(&member) {
            batch_requests
                .retain(|cached, _| cached.as_ref().is_none_or(|key| committee.contains(key)));
        }
        record(
            batch_requests
                .entry(member)
                .or_insert_with(|| BatchRequestCounters::new(self.worker_id, member.as_ref())),
        );
    }
}

/// Pre-resolved requester-side counters for batch requests under one `peer` series.
#[derive(Clone, Debug)]
struct BatchRequestCounters {
    /// `tn_worker.batch_requests_denied_total` for the series.
    denied: Counter,
    /// `tn_worker.batch_requests_failed_total` for the series.
    failed: Counter,
}

impl BatchRequestCounters {
    /// Resolve both counters under `worker_id` for `member`'s series: the `peer` label is the
    /// member's bs58 key, or `other` for `None`.
    fn new(worker_id: WorkerId, member: Option<&BlsPublicKey>) -> Self {
        let worker = worker_id.to_string();
        let peer = member.map_or_else(|| "other".to_string(), ToString::to_string);
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
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::collections::BTreeSet;
    use tn_types::BlsKeypair;

    /// `tn_worker.batch_requests_denied_total`.
    const DENIED: &str = "tn_worker.batch_requests_denied_total";
    /// `tn_worker.batch_requests_failed_total`.
    const FAILED: &str = "tn_worker.batch_requests_failed_total";

    /// A BLS public key derived from `seed`.
    fn bls_key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// Every batch-request counter series of worker `worker` in one snapshot, keyed by
    /// (metric name, `peer` label). A snapshot resets the counters, so take one per test.
    fn batch_request_series(
        snapshotter: &Snapshotter,
        worker: &str,
    ) -> BTreeMap<(String, String), u64> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _, _, value)| {
                let key = composite.key();
                let name = key.name();
                let ours = key.labels().any(|l| l.key() == "worker" && l.value() == worker);
                if !ours || ![DENIED, FAILED].contains(&name) {
                    return None;
                }
                let peer = key.labels().find(|l| l.key() == "peer")?.value().to_string();
                let DebugValue::Counter(count) = value else {
                    panic!("{name} is not a counter");
                };
                Some(((name.to_string(), peer), count))
            })
            .collect()
    }

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
            metrics.record_batch_report_over_quota();
            metrics.record_batches_fetched("local", 3);
            metrics.record_batches_fetched("remote", 0); // no-op, no series
            metrics.record_batch_fetch_duration(Duration::from_millis(80));
            metrics.record_forward_dropped(ForwardDropReason::NoEndpointAdvertised, 4);
            metrics.record_forward_dropped(ForwardDropReason::DiscoveryFailed, 0); // no-op, no series
            metrics.record_sync_stream_shed(SyncShedReason::Denied);
            metrics.record_sync_stream_shed(SyncShedReason::Denied);
            metrics.record_sync_stream_shed(SyncShedReason::BudgetExhausted);
            metrics.record_batch_report(Duration::from_millis(30));
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
        find("tn_worker.batch_reports_over_quota_total");
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

        // one report-time sample under this worker's label
        let (key, _, _, value) = find("tn_worker.batch_report_seconds");
        assert!(matches!(value, DebugValue::Histogram(samples) if samples.len() == 1));
        assert!(key.key().labels().any(|l| l.key() == "worker" && l.value() == "0"));
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
        let (denier, failer) = (bls_key(1), bls_key(2));
        // both are committee members, so each records under its own key
        let committee = HashSet::from([denier, failer]);

        metrics::with_local_recorder(&recorder, || {
            let metrics = WorkerMetrics::new_for_worker(3);
            metrics.record_sync_serve_denied(PeerTier::Public);
            metrics.record_sync_serve_denied(PeerTier::Public);
            metrics.record_batch_request_denied(&denier, &committee);
            // a clone shares the per-peer cache and records into the same series
            metrics.clone().record_batch_request_denied(&denier, &committee);
            metrics.record_batch_request_failed(&failer, &committee);
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

    /// Batch request outcomes from peers outside the committee share one `peer="other"`
    /// series per counter and one cache entry, however many distinct peers there are; a
    /// committee member keeps its own series.
    #[test]
    fn non_committee_batch_request_peers_share_one_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let member = bls_key(0);
        let committee = HashSet::from([member]);
        let outsiders: Vec<_> = (1..=50).map(bls_key).collect();

        let metrics = metrics::with_local_recorder(&recorder, || {
            let metrics = WorkerMetrics::new_for_worker(4);
            for peer in &outsiders {
                metrics.record_batch_request_denied(peer, &committee);
                metrics.record_batch_request_failed(peer, &committee);
            }
            metrics.record_batch_request_denied(&member, &committee);
            metrics
        });

        assert_eq!(metrics.batch_requests.lock().len(), 2, "the member and one `other` entry");
        let (member, other) = (member.to_string(), "other".to_string());
        assert_eq!(
            batch_request_series(&snapshotter, "4"),
            BTreeMap::from([
                ((DENIED.to_string(), member.clone()), 1),
                ((DENIED.to_string(), other.clone()), 50),
                ((FAILED.to_string(), member), 0),
                ((FAILED.to_string(), other), 50),
            ])
        );
    }

    /// The cache holds at most the committee it was last pruned against, plus `other`. A
    /// member that leaves at an epoch change records under `other` from then on and is dropped
    /// from the cache when the next entry is added; its own series keeps the count it had.
    #[test]
    fn batch_request_cache_follows_the_committee() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (a, b, c) = (bls_key(1), bls_key(2), bls_key(3));
        let (epoch_1, epoch_2) = (HashSet::from([a, b]), HashSet::from([b, c]));

        let metrics = metrics::with_local_recorder(&recorder, || {
            let metrics = WorkerMetrics::new_for_worker(5);
            metrics.record_batch_request_denied(&a, &epoch_1);
            metrics.record_batch_request_denied(&b, &epoch_1);
            // `a` left: it records under `other`, and adding that entry drops `a` from the cache
            metrics.record_batch_request_denied(&a, &epoch_2);
            metrics.record_batch_request_denied(&c, &epoch_2);
            metrics
        });

        // `None` is the shared `other` entry
        let cached: BTreeSet<_> = metrics.batch_requests.lock().keys().copied().collect();
        assert_eq!(cached, BTreeSet::from([Some(b), Some(c), None]));
        let series = batch_request_series(&snapshotter, "5");
        for peer in [a.to_string(), b.to_string(), c.to_string(), "other".to_string()] {
            assert_eq!(series.get(&(DENIED.to_string(), peer.clone())), Some(&1), "{peer}");
            assert_eq!(series.get(&(FAILED.to_string(), peer.clone())), Some(&0), "{peer}");
        }
        assert_eq!(series.len(), 8, "two counters for each of a, b, c and other: {series:?}");
    }
}
