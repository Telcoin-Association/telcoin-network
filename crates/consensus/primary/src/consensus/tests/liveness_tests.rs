//! Deterministic quorum observation boundary, identity and lifecycle tests.

use super::*;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;

/// Restored uncommitted rounds retain authors that post-restart arrivals cannot reconstruct.
#[test]
fn quorum_recovered_uncommitted_authors_are_not_sampled_again() {
    use tn_types::{Certificate, Hash as _};
    let recorder = DebuggingRecorder::new();
    let committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    let authors: Vec<_> = committee.authorities().iter().map(|authority| authority.id()).collect();
    let genesis = Certificate::genesis(&committee).iter().map(|cert| cert.digest()).collect();
    let (certificates, _) =
        crate::test_utils::make_optimal_certificates(&committee, 1..=2, &genesis, &authors);
    let dag = certificates.into_iter().fold(Dag::new(), |mut dag, cert| {
        dag.entry(cert.round()).or_default().insert(cert.origin().clone(), (cert.digest(), cert));
        dag
    });
    let mut observer = RoundQuorumObserver::from_recovered_dag(
        committee.clone(),
        test_metrics(&recorder),
        0,
        &dag,
    );
    let now = Instant::now();
    authors.iter().take(1).for_each(|author| {
        observer.observe(committee.epoch(), 2, author, now);
    });
    assert!(observer.pending.is_none());
    authors.iter().take(3).for_each(|author| {
        observer.observe(committee.epoch(), 3, author, now);
    });
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 0);
}

/// Read a recorded metric without relying on process-global recorder installation.
fn metric(recorder: &DebuggingRecorder, name: &str) -> Option<DebugValue> {
    recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, ..)| key.key().name() == name)
        .map(|(_, _, _, value)| value)
}

/// Assert a counter's exact process-lifetime value.
fn assert_counter(recorder: &DebuggingRecorder, name: &str, expected: u64) {
    assert!(
        matches!(metric(recorder, name), Some(DebugValue::Counter(value)) if value == expected),
        "{name}: expected {expected}, got {:?}",
        metric(recorder, name)
    );
}

/// Assert a lifecycle gauge's exact value.
fn assert_gauge(recorder: &DebuggingRecorder, name: &str, expected: f64) {
    assert!(
        matches!(metric(recorder, name), Some(DebugValue::Gauge(value)) if value.0 == expected)
    );
}

/// Bind fresh handles to the test recorder, avoiding the default metric handle cache.
fn test_metrics(recorder: &DebuggingRecorder) -> PrimaryMetrics {
    metrics::with_local_recorder(recorder, || {
        PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new())
    })
}

/// A partial round is evaluated once, even with duplicates, repeated polls and late recovery.
#[test]
fn quorum_timeout_deduplicates_authors_polls_and_late_arrivals() {
    let recorder = DebuggingRecorder::new();
    let committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    let authors = committee.authorities();
    let now = Instant::now();
    let mut observer = RoundQuorumObserver::new(committee.clone(), test_metrics(&recorder), 0);
    authors.iter().take(2).for_each(|authority| {
        observer.observe(committee.epoch(), 1, &authority.id(), now);
        observer.observe(committee.epoch(), 1, &authority.id(), now);
    });
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW - Duration::from_millis(1));
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 0);
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
    authors.iter().for_each(|authority| {
        observer.observe(committee.epoch(), 1, &authority.id(), now + QUORUM_OBSERVATION_WINDOW);
        observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    });
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
    assert!(observer.pending.is_none());
}

/// Three distinct unit-weight authors satisfy the actual four-member committee threshold.
#[test]
fn quorum_threshold_and_higher_round_boundary() {
    let recorder = DebuggingRecorder::new();
    let committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    assert_eq!(committee.quorum_threshold(), 3);
    let now = Instant::now();
    let mut observer = RoundQuorumObserver::new(committee.clone(), test_metrics(&recorder), 0);
    committee.authorities().iter().take(3).for_each(|authority| {
        observer.observe(committee.epoch(), 1, &authority.id(), now);
    });
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 0);
    committee.authorities().iter().take(1).for_each(|authority| {
        observer.observe(committee.epoch(), 2, &authority.id(), now);
    });
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 0);
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 2);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
}

/// The arrival at the deadline is late even when it would supply the missing quorum vote.
#[test]
fn quorum_deadline_is_fixed_at_first_arrival() {
    let recorder = DebuggingRecorder::new();
    let committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    let now = Instant::now();
    let mut observer = RoundQuorumObserver::new(committee.clone(), test_metrics(&recorder), 0);
    committee.authorities().iter().enumerate().take(3).for_each(|(index, authority)| {
        let elapsed = if index == 0 { Duration::ZERO } else { QUORUM_OBSERVATION_WINDOW };
        observer.observe(committee.epoch(), 1, &authority.id(), now + elapsed);
    });
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
}

/// Voting power accumulation uses supplied weights once per identity, rather than block count.
#[test]
fn quorum_voting_power_is_unique_author_weight() {
    let first = AuthorityIdentifier::from_bytes([1; 32]);
    let second = AuthorityIdentifier::from_bytes([2; 32]);
    let mut observation = RoundObservation {
        round: 1,
        first_seen: Instant::now(),
        authors: BTreeSet::new(),
        voting_power: 0,
    };
    observation.add_author(&first, 5);
    observation.add_author(&first, 5);
    observation.add_author(&second, 2);
    assert_eq!(observation.voting_power, 7);
    assert_eq!(observation.authors.len(), 2);
}

/// Round jumps, reordering, recovered history and nonmembers never grow retained round state.
#[test]
fn quorum_round_state_is_bounded_and_recovery_skips_history() {
    let recorder = DebuggingRecorder::new();
    let committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    let now = Instant::now();
    let mut observer = RoundQuorumObserver::new(committee.clone(), test_metrics(&recorder), 10);
    committee.authorities().iter().for_each(|authority| {
        observer.observe(committee.epoch(), 0, &authority.id(), now);
        observer.observe(committee.epoch(), 10, &authority.id(), now);
        observer.observe(committee.epoch() + 1, 11, &authority.id(), now);
    });
    observer.observe(committee.epoch(), 11, &AuthorityIdentifier::from_bytes([255; 32]), now);
    assert!(observer.pending.is_none());
    committee.authorities().iter().for_each(|authority| {
        observer.observe(committee.epoch(), 100, &authority.id(), now);
        observer.observe(committee.epoch(), 99, &authority.id(), now);
    });
    assert!(observer
        .pending
        .as_ref()
        .is_some_and(|pending| pending.authors.len() == committee.size()));
    committee.authorities().iter().take(1).for_each(|authority| {
        observer.observe(committee.epoch(), Round::MAX, &authority.id(), now);
    });
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 1);
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 2);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
    assert!(observer.pending.is_none());
}

/// Shutdown disables observations; a new epoch starts from its own cursor and commit baseline.
#[test]
fn quorum_epoch_rollover_and_commit_signal_lifecycle() {
    let recorder = DebuggingRecorder::new();
    let metrics = test_metrics(&recorder);
    let old_committee = CommitteeFixture::builder(MemDatabase::default).build().committee();
    let now = Instant::now();
    let mut observer = RoundQuorumObserver::new(old_committee.clone(), metrics.clone(), 0);
    assert_gauge(&recorder, "tn_primary.last_commit_timestamp_seconds", 0.0);
    old_committee.authorities().iter().take(1).for_each(|authority| {
        observer.observe(old_committee.epoch(), 1, &authority.id(), now);
    });
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    observer.record_commit();
    assert!(matches!(metric(&recorder, "tn_primary.last_commit_timestamp_seconds"),
        Some(DebugValue::Gauge(value)) if value.0 > 0.0));
    drop(observer);
    assert_gauge(&recorder, "tn_primary.quorum_observation_active", 0.0);
    let committee = CommitteeFixture::builder(MemDatabase::default)
        .epoch(old_committee.epoch() + 1)
        .build()
        .committee();
    let mut observer = RoundQuorumObserver::new(committee.clone(), metrics, 0);
    assert_gauge(&recorder, "tn_primary.quorum_observation_active", 1.0);
    assert_gauge(&recorder, "tn_primary.last_commit_timestamp_seconds", 0.0);
    committee.authorities().iter().for_each(|authority| {
        observer.observe(old_committee.epoch(), 1, &authority.id(), now);
    });
    assert!(observer.pending.is_none());
    committee.authorities().iter().take(3).for_each(|authority| {
        observer.observe(committee.epoch(), 1, &authority.id(), now);
    });
    observer.evaluate_expired(now + QUORUM_OBSERVATION_WINDOW);
    assert_counter(&recorder, "tn_primary.rounds_evaluated_total", 2);
    assert_counter(&recorder, "tn_primary.round_quorum_shortfalls_total", 1);
    assert!(recorder.snapshotter().snapshot().into_vec().iter().all(|(key, ..)| key
        .key()
        .labels()
        .next()
        .is_none()));
}
