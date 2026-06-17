//! Prometheus metrics for the primary (consensus layer).

use crate::{ConsensusBusApp, NodeMode};
use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use tn_types::{Noticer, TaskSpawner};
use tracing::debug;

/// Metrics for the primary's consensus pipeline.
///
/// A single instance lives on the application-lifetime consensus bus
/// ([`ConsensusBusApp`](crate::ConsensusBusApp)), which already reaches the proposer,
/// certifier, certificate manager, and consensus driver. Handles are `Arc`-backed and
/// cheap to clone.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_primary")]
pub struct PrimaryMetrics {
    /// Headers proposed by this primary.
    pub(crate) headers_proposed_total: Counter,
    /// Batch digests queued in the proposer awaiting inclusion in a header.
    pub(crate) proposer_pending_digests: Gauge,
    /// Votes received from peers on own header proposals.
    pub(crate) votes_received_total: Counter,
    /// Invalid votes received from peers on own header proposals.
    pub(crate) invalid_votes_total: Counter,
    /// Failed vote requests during header certification.
    pub(crate) vote_request_failures_total: Counter,
    /// Certificates formed from own headers reaching vote quorum.
    pub(crate) certificates_formed_total: Counter,
    /// Time to certify an own header proposal (collect a quorum of votes).
    pub(crate) certificate_form_duration_seconds: Histogram,
    /// Certificates received from peers and accepted for processing.
    pub(crate) certificates_received_total: Counter,
    /// Certificates parked awaiting missing parents.
    pub(crate) certificates_pending: Gauge,
    /// Certificate fetch operations started.
    pub(crate) certificate_fetches_total: Counter,
    /// Certificates retrieved via fetch operations.
    pub(crate) certificates_fetched_total: Counter,
    /// Duration of successful certificate fetch operations.
    pub(crate) certificate_fetch_duration_seconds: Histogram,
    /// Current primary round (mirrored from the round watch channel).
    pub(crate) round: Gauge,
    /// Highest committed round (mirrored from the committed-round watch channel).
    pub(crate) committed_round: Gauge,
    /// Committed subdags.
    pub(crate) subdags_committed_total: Counter,
    /// Time from leader header creation to subdag commit (whole seconds - the
    /// timestamps are second-granularity, so this flags pathological commits, not p50s).
    pub(crate) commit_latency_seconds: Histogram,
}

/// Spawn a node-lifetime task that mirrors consensus watch channels into gauges.
///
/// The watch senders are mutated from several crates via `send_replace`/`send_modify`;
/// mirroring the watch (the existing source of truth) is rot-proof compared to
/// instrumenting every mutation site, and gauges only need scrape-interval freshness.
///
/// Spawn once per process (from the epoch manager, before the epoch loop) on the
/// node-lifetime spawner.
pub fn spawn_bus_metrics_mirror(
    bus: &ConsensusBusApp,
    task_spawner: &TaskSpawner,
    shutdown: Noticer,
) {
    let metrics = bus.metrics().clone();
    let mut rx_round = bus.primary_round_updates().subscribe();
    let mut rx_committed = bus.committed_round_updates().subscribe();
    let mut rx_mode = bus.node_mode().subscribe();
    let mut rx_recent = bus.recent_blocks().subscribe();
    let mut rx_published = bus.last_published_consensus_num_hash().subscribe();

    // prime gauges with current values
    metrics.round.set(*rx_round.borrow_and_update() as f64);
    metrics.committed_round.set(*rx_committed.borrow_and_update() as f64);
    set_node_mode_gauges(&rx_mode.borrow_and_update());
    let mut executed_height =
        rx_recent.borrow_and_update().latest_consensus_block_num_hash().number;
    let mut gossip_height = rx_published.borrow_and_update().1;
    set_consensus_height_gauges(executed_height, gossip_height);

    task_spawner.spawn_task("bus-metrics-mirror", async move {
        loop {
            tokio::select! {
                _ = &shutdown => break,
                res = rx_round.changed() => {
                    if res.is_err() {
                        break;
                    }
                    metrics.round.set(*rx_round.borrow_and_update() as f64);
                }
                res = rx_committed.changed() => {
                    if res.is_err() {
                        break;
                    }
                    metrics.committed_round.set(*rx_committed.borrow_and_update() as f64);
                }
                res = rx_mode.changed() => {
                    if res.is_err() {
                        break;
                    }
                    let mode = *rx_mode.borrow_and_update();
                    set_node_mode_gauges(&mode);
                }
                res = rx_recent.changed() => {
                    if res.is_err() {
                        break;
                    }
                    executed_height =
                        rx_recent.borrow_and_update().latest_consensus_block_num_hash().number;
                    set_consensus_height_gauges(executed_height, gossip_height);
                }
                res = rx_published.changed() => {
                    if res.is_err() {
                        break;
                    }
                    gossip_height = rx_published.borrow_and_update().1;
                    set_consensus_height_gauges(executed_height, gossip_height);
                }
            }
        }
        debug!(target: "primary::metrics", "bus metrics mirror shutting down");
        Ok(())
    });
}

/// Consensus-height gauges and the falling-behind alarm.
///
/// `sync_distance` is how far the latest verified gossip height is ahead of local
/// execution - a persistently growing value means this node is falling behind.
fn set_consensus_height_gauges(executed: u64, gossip: u64) {
    metrics::gauge!("tn_node.last_executed_consensus_height").set(executed as f64);
    metrics::gauge!("tn_node.latest_gossip_consensus_height").set(gossip as f64);
    metrics::gauge!("tn_node.consensus_sync_distance").set(gossip.saturating_sub(executed) as f64);
}

/// One-hot `tn_node_mode{mode=...}` gauges: exactly one of the three series is 1.
///
/// Grafana renders this as a state timeline; one-hot avoids encoding the enum as
/// magic numbers.
fn set_node_mode_gauges(mode: &NodeMode) {
    let states = [
        ("cvv_active", mode.is_active_cvv()),
        ("cvv_inactive", mode.is_cvv_inactive()),
        ("observer", mode.is_observer()),
    ];
    for (label, active) in states {
        metrics::gauge!("tn_node.mode", "mode" => label).set(if active { 1.0 } else { 0.0 });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // new_with_labels: Default::default() caches the first-bound recorder process-wide
            let metrics = PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.headers_proposed_total.increment(1);
            metrics.votes_received_total.increment(3);
            metrics.invalid_votes_total.increment(1);
            metrics.certificates_formed_total.increment(1);
            metrics.certificate_form_duration_seconds.record(0.05);
            metrics.certificates_received_total.increment(2);
            metrics.certificates_pending.set(4.0);
            metrics.round.set(17.0);
            metrics.committed_round.set(15.0);
            metrics.subdags_committed_total.increment(1);
            metrics.commit_latency_seconds.record(1.0);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_primary.headers_proposed_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        let (_, _, _, value) = find("tn_primary.votes_received_total");
        assert!(matches!(value, DebugValue::Counter(3)));
        let (_, _, _, value) = find("tn_primary.round");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 17.0));
        let (_, _, _, value) = find("tn_primary.committed_round");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 15.0));
        find("tn_primary.certificate_form_duration_seconds");
        find("tn_primary.certificates_pending");
        find("tn_primary.subdags_committed_total");
        find("tn_primary.commit_latency_seconds");
    }

    /// The mirror task must keep the round gauges in sync with the watch channels.
    #[tokio::test]
    async fn test_bus_metrics_mirror() {
        use tn_types::{Notifier, TaskManager};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // bind the bus metrics to the local recorder during construction
        let bus = metrics::with_local_recorder(&recorder, ConsensusBusApp::new);

        let task_manager = TaskManager::default();
        let shutdown = Notifier::new();
        spawn_bus_metrics_mirror(&bus, &task_manager.get_spawner(), shutdown.subscribe());

        bus.primary_round_updates().send_replace(42);
        bus.committed_round_updates().send_replace(40);

        // wait for the mirror to observe the change
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let snapshot = snapshotter.snapshot().into_vec();
            let round = snapshot.iter().find_map(|(key, _, _, value)| {
                (key.key().name() == "tn_primary.round").then_some(value)
            });
            if matches!(round, Some(DebugValue::Gauge(g)) if g.0 == 42.0) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "mirror never updated round gauge");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        shutdown.notify();
    }
}
