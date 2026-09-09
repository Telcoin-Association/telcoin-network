//! Prometheus metrics for the libp2p consensus networks.
//!
//! Both the primary and worker networks instantiate the same types, so every series
//! carries a `network` label (`primary` or `worker-{id}`) set at construction.

use crate::{peers::Penalty, types::NetworkType};
use reth_metrics::{
    metrics::{Counter, Gauge},
    Metrics,
};

/// Map a [`NetworkType`] to its metric label value.
pub(crate) fn network_label(network_type: &NetworkType) -> String {
    match network_type {
        NetworkType::Primary => "primary".to_owned(),
        NetworkType::Worker(id) => format!("worker-{id}"),
    }
}

/// Derive-backed handles for swarm-level metrics.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_network")]
struct SwarmMetricHandles {
    /// Gossip messages published by this node.
    gossip_published_total: Counter,
    /// Gossip messages received from peers.
    gossip_received_total: Counter,
    /// Gossip messages rejected (failed verification against authorized publishers).
    gossip_rejected_total: Counter,
    /// Inbound provider announcements dropped by the per-source rate limit.
    add_provider_rate_limited_total: Counter,
    /// Graceful peer-exchange disconnects awaiting the peer's ack.
    px_disconnects_pending: Gauge,
    /// Outbound requests in flight.
    outbound_requests_pending: Gauge,
}

/// Swarm-level metrics owned by `ConsensusNetwork`.
#[derive(Clone, Debug)]
pub(crate) struct SwarmMetrics {
    /// The derive-backed handles.
    handles: SwarmMetricHandles,
    /// The network label value for per-event labeled counters.
    network: String,
}

impl SwarmMetrics {
    /// Create the swarm metric handles for `network_type`.
    pub(crate) fn new_for(network_type: &NetworkType) -> Self {
        let network = network_label(network_type);
        Self {
            handles: SwarmMetricHandles::new_with_labels(&[("network", network.clone())]),
            network,
        }
    }

    /// Record a successfully published gossip message.
    pub(crate) fn record_gossip_published(&self) {
        self.handles.gossip_published_total.increment(1);
    }

    /// Record a gossip message received from a peer.
    pub(crate) fn record_gossip_received(&self) {
        self.handles.gossip_received_total.increment(1);
    }

    /// Record a gossip message that failed verification.
    pub(crate) fn record_gossip_rejected(&self) {
        self.handles.gossip_rejected_total.increment(1);
    }

    /// Record an inbound provider announcement dropped before a store write.
    pub(crate) fn record_add_provider_rate_limited(&self) {
        self.handles.add_provider_rate_limited_total.increment(1);
    }

    /// Update the in-flight request gauges (called once per event-loop iteration).
    pub(crate) fn set_pending(&self, px_disconnects: usize, outbound_requests: usize) {
        self.handles.px_disconnects_pending.set(px_disconnects as f64);
        self.handles.outbound_requests_pending.set(outbound_requests as f64);
    }

    /// Record an outbound request failure by failure kind.
    pub(crate) fn record_outbound_failure(&self, kind: &'static str) {
        metrics::counter!(
            "tn_network.outbound_request_failures_total",
            "network" => self.network.clone(),
            "kind" => kind,
        )
        .increment(1);
    }
}

/// Derive-backed handles for peer-manager metrics.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_network")]
struct PeerManagerMetricHandles {
    /// Currently connected peers.
    connected_peers: Gauge,
    /// Peers known with a resolved network record (BLS key -> address).
    known_peers: Gauge,
    /// Peers tracked for discovery.
    discovery_peers: Gauge,
    /// Peers currently banned.
    banned_peers: Gauge,
    /// Connections closed (all directions).
    connections_closed_total: Counter,
    /// Failed dial attempts.
    dial_failures_total: Counter,
    /// 1 once at least one external address is confirmed (NAT traversal possible).
    external_addr_confirmed: Gauge,
    /// Peers banned for bad reputation (flow; `banned_peers` is the stock).
    peers_banned_total: Counter,
}

/// Peer-manager metrics, threaded `ConsensusNetwork::new` -> `TNBehavior::new` ->
/// `PeerManager::new`.
#[derive(Clone, Debug)]
pub(crate) struct PeerManagerMetrics {
    /// The derive-backed handles.
    handles: PeerManagerMetricHandles,
    /// The network label value for per-event labeled counters.
    network: String,
}

impl PeerManagerMetrics {
    /// Create the peer manager metric handles for `network_type`.
    pub(crate) fn new_for(network_type: &NetworkType) -> Self {
        let network = network_label(network_type);
        Self {
            handles: PeerManagerMetricHandles::new_with_labels(&[("network", network.clone())]),
            network,
        }
    }

    /// Update the peer-count gauges (called from the peer manager heartbeat).
    pub(crate) fn set_peer_counts(
        &self,
        connected: usize,
        known: usize,
        discovery: usize,
        banned: usize,
    ) {
        self.handles.connected_peers.set(connected as f64);
        self.handles.known_peers.set(known as f64);
        self.handles.discovery_peers.set(discovery as f64);
        self.handles.banned_peers.set(banned as f64);
    }

    /// Record an established connection by direction ({`in`, `out`}).
    pub(crate) fn record_connection_established(&self, direction: &'static str) {
        metrics::counter!(
            "tn_network.connections_established_total",
            "network" => self.network.clone(),
            "direction" => direction,
        )
        .increment(1);
    }

    /// Record a closed connection.
    pub(crate) fn record_connection_closed(&self) {
        self.handles.connections_closed_total.increment(1);
    }

    /// Record a failed dial attempt.
    pub(crate) fn record_dial_failure(&self) {
        self.handles.dial_failures_total.increment(1);
    }

    /// Mark that an external address has been confirmed.
    pub(crate) fn record_external_addr_confirmed(&self) {
        self.handles.external_addr_confirmed.set(1.0);
    }

    /// Record an application-layer penalty by severity.
    pub(crate) fn record_penalty(&self, penalty: &Penalty) {
        let severity = match penalty {
            Penalty::Mild => "mild",
            Penalty::Medium => "medium",
            Penalty::Severe => "severe",
            Penalty::Fatal => "fatal",
        };
        metrics::counter!(
            "tn_network.peer_penalties_total",
            "network" => self.network.clone(),
            "severity" => severity,
        )
        .increment(1);
    }

    /// Record a reputation ban.
    pub(crate) fn record_peer_banned(&self) {
        self.handles.peers_banned_total.increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Primary and worker metrics register their expected labels and update every handle.
    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let swarm = SwarmMetrics::new_for(&NetworkType::Primary);
            swarm.record_gossip_published();
            swarm.record_gossip_received();
            swarm.record_gossip_rejected();
            swarm.set_pending(1, 2);
            swarm.record_outbound_failure("timeout");

            let peers = PeerManagerMetrics::new_for(&NetworkType::Worker(0));
            peers.set_peer_counts(4, 10, 3, 1);
            peers.record_connection_established("in");
            peers.record_connection_closed();
            peers.record_dial_failure();
            peers.record_external_addr_confirmed();
            peers.record_penalty(&Penalty::Severe);
            peers.record_peer_banned();
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (key, _, _, value) = find("tn_network.gossip_published_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "network" && l.value() == "primary"));

        let (key, _, _, value) = find("tn_network.connected_peers");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 4.0));
        assert!(key.key().labels().any(|l| l.key() == "network" && l.value() == "worker-0"));

        let (key, _, _, _) = find("tn_network.outbound_request_failures_total");
        assert!(key.key().labels().any(|l| l.key() == "kind" && l.value() == "timeout"));

        let (key, _, _, _) = find("tn_network.peer_penalties_total");
        assert!(key.key().labels().any(|l| l.key() == "severity" && l.value() == "severe"));

        let (key, _, _, _) = find("tn_network.connections_established_total");
        assert!(key.key().labels().any(|l| l.key() == "direction" && l.value() == "in"));

        find("tn_network.px_disconnects_pending");
        find("tn_network.outbound_requests_pending");
        find("tn_network.banned_peers");
        find("tn_network.peers_banned_total");
        find("tn_network.external_addr_confirmed");
        find("tn_network.dial_failures_total");
        find("tn_network.connections_closed_total");
    }

    /// Worker swarms must retain independent gauges and counters in the shared recorder.
    #[test]
    fn test_worker_metrics_are_isolated() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let first = SwarmMetrics::new_for(&NetworkType::Worker(0));
            let second = SwarmMetrics::new_for(&NetworkType::Worker(1));
            first.set_pending(3, 3);
            second.set_pending(7, 7);
            [&first, &second].into_iter().for_each(|swarm| {
                swarm.record_gossip_published();
                swarm.record_outbound_failure("timeout");
            });

            let first = PeerManagerMetrics::new_for(&NetworkType::Worker(0));
            let second = PeerManagerMetrics::new_for(&NetworkType::Worker(1));
            first.set_peer_counts(3, 3, 3, 3);
            second.set_peer_counts(7, 7, 7, 7);
            [&first, &second].into_iter().for_each(|peers| {
                peers.record_connection_established("in");
                peers.record_penalty(&Penalty::Severe);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        [("worker-0", 3.0), ("worker-1", 7.0)].into_iter().for_each(|(network, expected)| {
            let value = |name| {
                snapshot
                    .iter()
                    .find(|(key, ..)| {
                        key.key().name() == name
                            && key
                                .key()
                                .labels()
                                .any(|label| label.key() == "network" && label.value() == network)
                    })
                    .map(|(_, _, _, value)| value)
            };
            [
                "tn_network.px_disconnects_pending",
                "tn_network.outbound_requests_pending",
                "tn_network.connected_peers",
                "tn_network.known_peers",
                "tn_network.discovery_peers",
                "tn_network.banned_peers",
            ]
            .into_iter()
            .for_each(|name| {
                assert!(
                    matches!(value(name), Some(DebugValue::Gauge(g)) if g.0 == expected),
                    "{name} must retain {network}'s gauge value"
                );
            });
            [
                "tn_network.gossip_published_total",
                "tn_network.outbound_request_failures_total",
                "tn_network.connections_established_total",
                "tn_network.peer_penalties_total",
            ]
            .into_iter()
            .for_each(|name| {
                assert!(
                    matches!(value(name), Some(DebugValue::Counter(1))),
                    "{name} must count {network}'s events separately"
                );
            });
        });
    }
}
