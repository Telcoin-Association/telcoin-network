//! Tests for the per-class serve permit pools and their series.

use super::{capacity_for, ServePermits, MAX_PERMITS_PER_PEER};
use crate::{service_class::ServiceClass, types::NetworkType};
use metrics::{SharedString, Unit};
use metrics_util::{
    debugging::{DebugValue, DebuggingRecorder},
    CompositeKey,
};
use rand::{rngs::StdRng, SeedableRng};
use tn_config::ServeBudget;
use tn_types::{BlsKeypair, BlsPublicKey};

/// The capacity gauge, in permits.
const CAPACITY: &str = "tn_network.serve_permit_capacity";
/// The held gauge, in permits.
const HELD: &str = "tn_network.serve_permits_held";
/// The offered counter.
const OFFERED: &str = "tn_network.serve_requests_offered_total";
/// The admitted counter.
const ADMITTED: &str = "tn_network.serve_requests_admitted_total";
/// The shed counter shared with the swarm.
const SHED: &str = "tn_network.inbound_requests_shed_total";

/// The BLS public key of test peer `seed`.
fn peer(seed: u8) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
}

/// One entry of a debugging recorder snapshot.
type Series = (CompositeKey, Option<Unit>, Option<SharedString>, DebugValue);

/// The value of series `name` for `class` under `network`, matching `reason` when given.
fn series_value(
    snapshot: &[Series],
    name: &str,
    network: &str,
    class: &str,
    reason: Option<&str>,
) -> Option<DebugValue> {
    snapshot
        .iter()
        .find(|(key, ..)| {
            key.key().name() == name
                && key.key().labels().any(|l| l.key() == "network" && l.value() == network)
                && key.key().labels().any(|l| l.key() == "class" && l.value() == class)
                && reason.is_none_or(|reason| {
                    key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
                })
        })
        .map(|(_, _, _, value)| value.clone())
}

/// Whether the counter series is registered with `expected`.
fn counter_is(
    snapshot: &[Series],
    name: &str,
    network: &str,
    class: &str,
    reason: Option<&str>,
    expected: u64,
) -> bool {
    matches!(
        series_value(snapshot, name, network, class, reason),
        Some(DebugValue::Counter(value)) if value == expected
    )
}

/// Whether the gauge series is registered with `expected`.
fn gauge_is(snapshot: &[Series], name: &str, network: &str, class: &str, expected: f64) -> bool {
    matches!(
        series_value(snapshot, name, network, class, None),
        Some(DebugValue::Gauge(value)) if value.0 == expected
    )
}

/// Construction registers every series at zero for all six classes under the swarm label, and
/// the capacity gauges carry the configured budget.
#[test]
fn construction_registers_every_class_at_zero() -> Result<(), serde_json::Error> {
    let budget: ServeBudget = serde_json::from_value(serde_json::json!({ "vote": 3, "batch": 2 }))?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let permits = metrics::with_local_recorder(&recorder, || {
        ServePermits::new(&NetworkType::Worker(1), budget)
    });
    let snapshot = snapshotter.snapshot().into_vec();
    ServiceClass::ALL.iter().for_each(|class| {
        let label = class.label();
        let capacity = capacity_for(*class, &budget);
        assert!(gauge_is(&snapshot, CAPACITY, "worker-1", label, f64::from(capacity)), "{label}");
        assert!(gauge_is(&snapshot, HELD, "worker-1", label, 0.0), "{label}");
        assert!(counter_is(&snapshot, OFFERED, "worker-1", label, None, 0), "{label}");
        assert!(counter_is(&snapshot, ADMITTED, "worker-1", label, None, 0), "{label}");
        assert!(counter_is(&snapshot, SHED, "worker-1", label, Some("serve_budget"), 0), "{label}");
        assert_eq!(permits.capacity(*class), capacity, "{label}");
        assert_eq!(permits.held(*class), 0, "{label}");
    });
    assert_eq!(permits.capacity(ServiceClass::Vote), 3);
    assert_eq!(permits.capacity(ServiceClass::Batch), 2);
    assert_eq!(permits.capacity(ServiceClass::Gossip), 0);
    assert_eq!(permits.capacity(ServiceClass::Other), 0);
    Ok(())
}

/// With one vote permit held, the second vote offer sheds once under `serve_budget`, the
/// counters show two offers and one admit, and the other classes still admit.
#[test]
fn saturated_class_sheds_once_and_isolates_other_classes() -> Result<(), serde_json::Error> {
    let budget: ServeBudget = serde_json::from_value(serde_json::json!({ "vote": 1, "batch": 1 }))?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let held = metrics::with_local_recorder(&recorder, || {
        let permits = ServePermits::new(&NetworkType::Primary, budget);
        let vote = permits.try_acquire(ServiceClass::Vote, peer(1));
        assert!(vote.is_some());
        assert!(permits.try_acquire(ServiceClass::Vote, peer(1)).is_none());
        assert_eq!(permits.held(ServiceClass::Vote), 1);
        let batch = permits.try_acquire(ServiceClass::Batch, peer(1));
        let other = permits.try_acquire(ServiceClass::Other, peer(1));
        assert!(batch.is_some());
        assert!(other.is_some());
        (permits, vote, batch, other)
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(counter_is(&snapshot, SHED, "primary", "vote", Some("serve_budget"), 1));
    assert!(counter_is(&snapshot, OFFERED, "primary", "vote", None, 2));
    assert!(counter_is(&snapshot, ADMITTED, "primary", "vote", None, 1));
    assert!(gauge_is(&snapshot, HELD, "primary", "vote", 1.0));
    assert!(counter_is(&snapshot, SHED, "primary", "batch", Some("serve_budget"), 0));
    assert!(counter_is(&snapshot, OFFERED, "primary", "batch", None, 1));
    assert!(gauge_is(&snapshot, HELD, "primary", "batch", 1.0));
    assert!(counter_is(&snapshot, SHED, "primary", "other", Some("serve_budget"), 0));
    assert!(gauge_is(&snapshot, HELD, "primary", "other", 1.0));
    drop(held);
    Ok(())
}

/// Dropping a permit returns the held count and gauge to zero and frees the slot for the next
/// offer.
#[test]
fn dropping_permits_returns_held_to_zero() -> Result<(), serde_json::Error> {
    let budget: ServeBudget = serde_json::from_value(serde_json::json!({ "batch": 2 }))?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let permits = metrics::with_local_recorder(&recorder, || {
        let permits = ServePermits::new(&NetworkType::Worker(0), budget);
        let first = permits.try_acquire(ServiceClass::Batch, peer(1));
        let second = permits.try_acquire(ServiceClass::Batch, peer(1));
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(permits.try_acquire(ServiceClass::Batch, peer(1)).is_none());
        assert_eq!(permits.held(ServiceClass::Batch), 2);
        drop(first);
        assert_eq!(permits.held(ServiceClass::Batch), 1);
        drop(second);
        assert_eq!(permits.held(ServiceClass::Batch), 0);
        // the freed slot admits again; the permit drops at the end of the statement
        assert!(permits.try_acquire(ServiceClass::Batch, peer(1)).is_some());
        assert_eq!(permits.held(ServiceClass::Batch), 0);
        permits
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(gauge_is(&snapshot, HELD, "worker-0", "batch", 0.0));
    assert!(counter_is(&snapshot, OFFERED, "worker-0", "batch", None, 4));
    assert!(counter_is(&snapshot, ADMITTED, "worker-0", "batch", None, 3));
    assert!(counter_is(&snapshot, SHED, "worker-0", "batch", Some("serve_budget"), 1));
    assert_eq!(permits.held(ServiceClass::Batch), 0);
    Ok(())
}

/// A zero capacity admits every offer over many acquires from one peer, so it has no per-peer
/// share, and only observes.
#[test]
fn zero_capacity_is_unbounded_and_only_observes() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let permits = metrics::with_local_recorder(&recorder, || {
        let permits = ServePermits::new(&NetworkType::Primary, ServeBudget::default());
        let held: Vec<_> =
            (0..64).filter_map(|_| permits.try_acquire(ServiceClass::Vote, peer(1))).collect();
        assert_eq!(held.len(), 64);
        assert_eq!(permits.held(ServiceClass::Vote), 64);
        assert_eq!(permits.capacity(ServiceClass::Vote), 0);
        drop(held);
        assert_eq!(permits.held(ServiceClass::Vote), 0);
        permits
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(gauge_is(&snapshot, CAPACITY, "primary", "vote", 0.0));
    assert!(gauge_is(&snapshot, HELD, "primary", "vote", 0.0));
    assert!(counter_is(&snapshot, OFFERED, "primary", "vote", None, 64));
    assert!(counter_is(&snapshot, ADMITTED, "primary", "vote", None, 64));
    assert!(counter_is(&snapshot, SHED, "primary", "vote", Some("serve_budget"), 0));
    assert_eq!(permits.capacity(ServiceClass::Vote), 0);
}

/// Unmetered pools register no series and admit every class.
#[test]
fn unmetered_pools_register_nothing_and_admit() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let admitted = metrics::with_local_recorder(&recorder, || {
        let permits = ServePermits::unmetered();
        ServiceClass::ALL
            .iter()
            .map(|class| permits.try_acquire(*class, peer(1)).is_some())
            .collect::<Vec<_>>()
    });
    assert!(admitted.iter().all(|admitted| *admitted));
    assert!(snapshotter.snapshot().into_vec().is_empty());
}

/// One peer holds at most its share of an enforcing class. Its next offer sheds while the class
/// still has free permits, other peers are admitted up to the capacity, and a dropped permit
/// returns one slot of the share.
#[test]
fn one_peer_is_capped_at_its_share_and_other_peers_are_admitted() -> Result<(), serde_json::Error> {
    let budget: ServeBudget = serde_json::from_value(serde_json::json!({ "vote": 4 }))?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let permits = metrics::with_local_recorder(&recorder, || {
        let permits = ServePermits::new(&NetworkType::Primary, budget);
        let (a, b, c) = (peer(1), peer(2), peer(3));
        let a_first = permits.try_acquire(ServiceClass::Vote, a);
        let a_second = permits.try_acquire(ServiceClass::Vote, a);
        assert!(a_first.is_some());
        assert!(a_second.is_some());
        // two permits are free, but `a` holds its share
        assert!(permits.try_acquire(ServiceClass::Vote, a).is_none());
        let held_by_b: Vec<_> = (0..MAX_PERMITS_PER_PEER)
            .filter_map(|_| permits.try_acquire(ServiceClass::Vote, b))
            .collect();
        assert_eq!(held_by_b.len(), 2);
        // the class is full, so a peer without a permit sheds too
        assert!(permits.try_acquire(ServiceClass::Vote, c).is_none());
        assert_eq!(permits.held(ServiceClass::Vote), 4);
        // a dropped permit returns one slot of the class and one of the share of `a`
        drop(a_first);
        let a_again = permits.try_acquire(ServiceClass::Vote, a);
        assert!(a_again.is_some());
        assert_eq!(permits.held(ServiceClass::Vote), 4);
        drop((a_second, a_again, held_by_b));
        assert_eq!(permits.held(ServiceClass::Vote), 0);
        // every share is free again; the permit drops at the end of the statement
        assert!(permits.try_acquire(ServiceClass::Vote, c).is_some());
        permits
    });
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(gauge_is(&snapshot, HELD, "primary", "vote", 0.0));
    assert!(counter_is(&snapshot, OFFERED, "primary", "vote", None, 8));
    assert!(counter_is(&snapshot, ADMITTED, "primary", "vote", None, 6));
    assert!(counter_is(&snapshot, SHED, "primary", "vote", Some("serve_budget"), 2));
    assert_eq!(permits.held(ServiceClass::Vote), 0);
    Ok(())
}
