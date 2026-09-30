//! Committed sub-dag timestamps and reputation scores, built from the committee fixture.

use indexmap::IndexMap;
use std::{collections::BTreeSet, num::NonZeroUsize};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    forks::{
        seed_signature_active, seed_signature_fork_epoch_override, subsecond_timestamp_active,
        subsecond_timestamp_fork_epoch_override,
    },
    AuthorityIdentifier, Certificate, CommittedSubDag, Epoch, EpochSeedChainValue, Hash as _,
    HeaderBuilder, ReputationScores, Round, SequenceNumber, TimestampMs, TimestampSec,
};

#[test]
fn test_zero_timestamp_in_sub_dag() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier::default())
        .round(2)
        .epoch(0)
        .created_at(50)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

    // AND we initialise the sub dag via the "restore" way
    let sub_dag_round = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round.commit_timestamp(), 50);
}

#[test]
fn test_monotonically_incremented_commit_timestamps() {
    // Create a certificate (leader) of round 2 with a high timestamp
    let newer_timestamp = 100;
    let older_timestamp = 50;

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier::default())
        .round(2)
        .epoch(0)
        .created_at(newer_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

    // AND
    let sub_dag_round_2 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round_2.commit_timestamp(), newer_timestamp);

    // Now create the leader of round 4 with the older timestamp
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier::default())
        .round(4)
        .epoch(0)
        .created_at(older_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

    // WHEN create the sub dag based on the "previously committed" sub dag.
    let sub_dag_round_4 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        2,
        ReputationScores::default(),
        Some(sub_dag_round_2.clone()),
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );

    // THEN the latest sub dag should have the highest committed timestamp - basically the
    // same as the previous commit round
    assert_eq!(sub_dag_round_4.commit_timestamp(), sub_dag_round_2.commit_timestamp());
}

/// Post-fork, a commit lands strictly after the previous one: with the previous commit at `T`, a
/// leader older than or equal to `T` commits at `T + 1 ms` and a newer leader keeps its own time.
/// The second `T` is the last millisecond of its second, so the clamp carries into the next whole
/// second and `commit_timestamp` has to follow it.
///
/// This pins the strict clamp in `CommittedSubDag::new_with_commit_floor`: dropping its `+ 1`
/// (raising to the floor itself) commits the older and equal leaders at `T`, and a non-strict
/// bound (keeping any leader `>=` the floor) commits the equal leader at `T`.
#[test]
fn post_fork_commit_lands_strictly_after_the_previous_commit() {
    pin_forks();
    let fixture = fixture_at(POST_FORK_EPOCH);
    for t in [1_700_000_000_500, 1_700_000_000_999] {
        let previous_ms = TimestampMs::from_millis(t);
        let previous = commit(sub_dag_certificates(&fixture, 2, previous_ms), 1, None, None);
        assert_eq!(
            previous.commit_timestamp_ms(),
            previous_ms,
            "a first commit without a floor keeps its leader's time"
        );
        for (label, leader_ms, expected_ms) in
            [("equal", t, t + 1), ("older", t - 5, t + 1), ("newer", t + 5, t + 5)]
        {
            let leader_ms = TimestampMs::from_millis(leader_ms);
            let sub_dag =
                commit(sub_dag_certificates(&fixture, 4, leader_ms), 2, Some(&previous), None);
            assert_eq!(
                sub_dag.leader().created_at_ms(),
                leader_ms,
                "T = {t}, {label} leader: the fixture must keep the sub-second part"
            );
            assert_eq!(
                sub_dag.commit_timestamp_ms(),
                TimestampMs::from_millis(expected_ms),
                "T = {t}, {label} leader"
            );
            assert_eq!(
                sub_dag.commit_timestamp(),
                sub_dag.commit_timestamp_ms().secs(),
                "T = {t}, {label} leader: seconds must floor the millisecond commit time"
            );
        }
    }
}

/// Pre-fork, the commit timestamp is the legacy whole-seconds `max(previous commit_timestamp,
/// leader created_at)` with a sub-second part of 0, both before and after the seed-signature
/// fork. An epoch commit floor above every timestamp is ignored: the sub-dag, digest included, is
/// the one `CommittedSubDag::new` commits.
#[test]
fn pre_fork_commit_is_the_legacy_seconds_max() {
    pin_forks();
    let previous_secs: TimestampSec = 1_700_000_000;
    let floor = Some(TimestampMs::from_parts(previous_secs + 100, 0));
    for epoch in PRE_FORK_EPOCHS {
        let fixture = fixture_at(epoch);
        let previous_ms = TimestampMs::from_parts(previous_secs, 0);
        let previous = commit(sub_dag_certificates(&fixture, 2, previous_ms), 1, None, None);
        assert_eq!(previous.commit_timestamp(), previous_secs, "epoch {epoch}: first commit");
        for leader_secs in [previous_secs - 1, previous_secs, previous_secs + 2] {
            // the header builder drops the sub-second part of a pre-fork header
            let certificates =
                sub_dag_certificates(&fixture, 4, TimestampMs::from_parts(leader_secs, 750));
            let sub_dag = commit(certificates.clone(), 2, Some(&previous), floor);
            let expected = previous_secs.max(leader_secs);
            assert_eq!(
                sub_dag.leader().created_at_ms(),
                TimestampMs::from_parts(leader_secs, 0),
                "epoch {epoch}: a pre-fork leader carries whole seconds only"
            );
            assert_eq!(sub_dag.commit_timestamp(), expected, "epoch {epoch}, leader {leader_secs}");
            assert_eq!(
                sub_dag.commit_timestamp_ms(),
                TimestampMs::from_parts(expected, 0),
                "epoch {epoch}, leader {leader_secs}: the sub-second part must stay 0"
            );
            let legacy = commit_without_floor(certificates, 2, Some(&previous));
            assert_eq!(
                sub_dag.digest(),
                legacy.digest(),
                "epoch {epoch}, leader {leader_secs}: digest must match `CommittedSubDag::new`"
            );
            assert_eq!(sub_dag, legacy, "epoch {epoch}, leader {leader_secs}");
        }
    }
}

/// The epoch commit floor `F` bounds the first commit of a post-fork epoch (no previous sub-dag):
/// a leader older than or equal to `F` commits at `F + 1 ms`, a newer leader keeps its own time.
/// Pre-fork the floor is ignored, so a leader older than `F` commits at its own whole seconds,
/// exactly as `CommittedSubDag::new` commits it.
#[test]
fn epoch_commit_floor_seeds_the_first_post_fork_commit() {
    pin_forks();
    let floor_ms: u64 = 1_700_000_010_250;
    let floor = Some(TimestampMs::from_millis(floor_ms));

    let fixture = fixture_at(POST_FORK_EPOCH);
    for (label, leader_ms, expected_ms) in [
        ("older", floor_ms - 1_000, floor_ms + 1),
        ("equal", floor_ms, floor_ms + 1),
        ("newer", floor_ms + 5, floor_ms + 5),
    ] {
        let leader_ms = TimestampMs::from_millis(leader_ms);
        let sub_dag = commit(sub_dag_certificates(&fixture, 2, leader_ms), 1, None, floor);
        assert_eq!(
            sub_dag.commit_timestamp_ms(),
            TimestampMs::from_millis(expected_ms),
            "{label} leader"
        );
        assert_eq!(sub_dag.commit_timestamp(), sub_dag.commit_timestamp_ms().secs(), "{label}");
    }

    let leader_secs = floor_ms / 1000 - 5;
    for epoch in PRE_FORK_EPOCHS {
        let fixture = fixture_at(epoch);
        let certificates =
            sub_dag_certificates(&fixture, 2, TimestampMs::from_parts(leader_secs, 0));
        let sub_dag = commit(certificates.clone(), 1, None, floor);
        assert_eq!(
            sub_dag.commit_timestamp_ms(),
            TimestampMs::from_parts(leader_secs, 0),
            "epoch {epoch}: the floor must be ignored pre-fork"
        );
        assert_eq!(
            sub_dag.digest(),
            commit_without_floor(certificates, 1, None).digest(),
            "epoch {epoch}: digest must match `CommittedSubDag::new`"
        );
    }
}

/// At the fork seam, a pre-fork previous sub-dag with `commit_timestamp = s` floors the first
/// post-fork commit at `s * 1000` ms, so that commit lands at `s * 1000 + 1` or later. The previous
/// sub-dag was itself held at `s` by its predecessor while its leader was created at `s - 3`, so a
/// post-fork leader between the two must still land after `s`: the floor is the previous commit
/// time, not the previous leader's creation time.
#[test]
fn fork_seam_floors_on_the_pre_fork_commit_seconds() {
    pin_forks();
    let s: TimestampSec = 1_700_000_020;
    let floor_ms = s * 1000;
    let post_fork = fixture_at(POST_FORK_EPOCH);
    for epoch in PRE_FORK_EPOCHS {
        let pre_fork = fixture_at(epoch);
        let first = commit(
            sub_dag_certificates(&pre_fork, 2, TimestampMs::from_parts(s, 0)),
            1,
            None,
            None,
        );
        let previous = commit(
            sub_dag_certificates(&pre_fork, 4, TimestampMs::from_parts(s - 3, 0)),
            2,
            Some(&first),
            None,
        );
        assert_eq!(*previous.leader().created_at(), s - 3, "epoch {epoch}: previous leader");
        assert_eq!(previous.commit_timestamp(), s, "epoch {epoch}: the legacy max must hold s");
        assert_eq!(
            previous.commit_timestamp_ms(),
            TimestampMs::from_millis(floor_ms),
            "epoch {epoch}: a pre-fork commit resolves to whole seconds times 1000"
        );

        for (label, leader_ms, expected_ms) in [
            ("between the previous leader and its commit", floor_ms - 1_500, floor_ms + 1),
            ("one ms before the floor", floor_ms - 1, floor_ms + 1),
            ("equal to the floor", floor_ms, floor_ms + 1),
            ("newer", floor_ms + 5, floor_ms + 5),
        ] {
            let leader_ms = TimestampMs::from_millis(leader_ms);
            let sub_dag =
                commit(sub_dag_certificates(&post_fork, 2, leader_ms), 3, Some(&previous), None);
            let commit_ms = sub_dag.commit_timestamp_ms();
            assert_eq!(commit_ms, TimestampMs::from_millis(expected_ms), "epoch {epoch}: {label}");
            assert!(
                commit_ms > TimestampMs::from_millis(floor_ms),
                "epoch {epoch}: {label} must commit after s * 1000"
            );
            assert_eq!(sub_dag.commit_timestamp(), commit_ms.secs(), "epoch {epoch}: {label}");
        }
    }
}

#[test]
fn test_authority_sorting_in_reputation_scores() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();

    let mut scores = ReputationScores::new(&committee);

    let mut ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // adding some scores
    scores.add_score(ids.first().unwrap(), 0);
    scores.add_score(ids.get(1).unwrap(), 10);
    scores.add_score(ids.get(2).unwrap(), 10);
    scores.add_score(ids.get(3).unwrap(), 10);
    scores.add_score(ids.get(4).unwrap(), 10);
    scores.add_score(ids.get(5).unwrap(), 20);
    scores.add_score(ids.get(6).unwrap(), 30);
    scores.add_score(ids.get(7).unwrap(), 30);
    scores.add_score(ids.get(8).unwrap(), 40);
    scores.add_score(ids.get(9).unwrap(), 40);

    // the expected authorities
    let expected_authorities = vec![
        (ids.pop().unwrap(), 40),
        (ids.pop().unwrap(), 40),
        (ids.pop().unwrap(), 30),
        (ids.pop().unwrap(), 30),
        (ids.pop().unwrap(), 20),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 0),
    ];

    // sorting the authorities
    let sorted_authorities = scores.authorities_by_score_desc();
    assert_eq!(sorted_authorities, expected_authorities);
}

/// Seed-signature fork epoch pinned by [`pin_forks`].
const SEED_SIGNATURE_FORK: Epoch = 1;

/// Sub-second timestamp fork epoch pinned by [`pin_forks`].
const SUBSECOND_TIMESTAMP_FORK: Epoch = 2;

/// Pre-fork epochs under [`pin_forks`]: one before the seed-signature fork (legacy header layout,
/// aggregate-signature randomness) and one after it (seed signatures, whole-second timestamps).
const PRE_FORK_EPOCHS: [Epoch; 2] = [0, 1];

/// The first epoch with millisecond timestamps under [`pin_forks`].
const POST_FORK_EPOCH: Epoch = SUBSECOND_TIMESTAMP_FORK;

/// Pins this test process's fork schedule to [`SEED_SIGNATURE_FORK`] and
/// [`SUBSECOND_TIMESTAMP_FORK`], so the same pre-fork and post-fork epochs exist side by side on
/// every build, whatever that build's own fork points are.
///
/// The gates read their `test-utils` environment overrides once per process, so this must run
/// before anything consults a gate, including building a committee fixture. nextest runs each test
/// in its own process, which keeps one test's pin from reaching another; a single-process `cargo
/// test` run shares one latch across the whole test binary instead. Reading the overrides back
/// turns a value that latched before the pin into a named failure.
fn pin_forks() {
    std::env::set_var("TN_SEED_SIGNATURE_FORK_EPOCH", SEED_SIGNATURE_FORK.to_string());
    std::env::set_var("TN_SUBSECOND_TIMESTAMP_FORK_EPOCH", SUBSECOND_TIMESTAMP_FORK.to_string());
    assert_eq!(
        seed_signature_fork_epoch_override(),
        Some(SEED_SIGNATURE_FORK),
        "TN_SEED_SIGNATURE_FORK_EPOCH latched to another value before this test pinned it"
    );
    assert_eq!(
        subsecond_timestamp_fork_epoch_override(),
        Some(SUBSECOND_TIMESTAMP_FORK),
        "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH latched to another value before this test pinned it"
    );
    // anti-vacuity: every epoch must sit in the regime its assertions assume
    let [legacy, seed_only] = PRE_FORK_EPOCHS;
    assert!(!seed_signature_active(legacy), "epoch {legacy} must predate the seed-signature fork");
    assert!(seed_signature_active(seed_only), "epoch {seed_only} must carry seed signatures");
    for epoch in PRE_FORK_EPOCHS {
        assert!(
            !subsecond_timestamp_active(epoch),
            "epoch {epoch} must predate the sub-second fork"
        );
    }
    assert!(
        subsecond_timestamp_active(POST_FORK_EPOCH),
        "epoch {POST_FORK_EPOCH} must carry millisecond timestamps"
    );
}

/// A committee fixture whose committee is at `epoch`.
fn fixture_at(epoch: Epoch) -> CommitteeFixture<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(epoch).build();
    assert_eq!(fixture.committee().epoch(), epoch, "fixture committee epoch");
    fixture
}

/// The certificates of a sub-dag led at `round` by the fixture's first authority, leader last:
/// one certificate per authority at `round - 1` created 1 ms before the leader (truncated to whole
/// seconds in pre-fork epochs), then the leader created at `leader_ms` with all of them as
/// parents. Every certificate carries the other
/// authorities' votes and its author's seed signature for its round and the committee's epoch.
///
/// The `round - 1` certificates keep the fixture's genesis parents rather than chaining to the
/// previous sub-dag: the commit timestamp reads only the leader's epoch and creation time, and
/// the clamped cases pair leaders with previous commits that no valid same-epoch DAG produces,
/// which is exactly the condition the clamp guards against.
fn sub_dag_certificates(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    leader_ms: TimestampMs,
) -> Vec<Certificate> {
    let committee = fixture.committee();
    let parent_ms = TimestampMs::from_millis(leader_ms.as_millis() - 1);
    let mut certificates: Vec<Certificate> = fixture
        .authorities()
        .map(|authority| {
            let header = authority
                .header_builder_at_round(&committee, round - 1)
                .created_at_ms(parent_ms)
                .build();
            fixture.certificate(&header)
        })
        .collect();
    let parents = certificates.iter().map(|certificate| certificate.digest()).collect();
    let leader = fixture
        .first_authority()
        .header_builder_at_round(&committee, round)
        .parents(parents)
        .created_at_ms(leader_ms)
        .build();
    certificates.push(fixture.certificate(&leader));
    certificates
}

/// Commits `certificates` (leader last) through `CommittedSubDag::new_with_commit_floor`.
fn commit(
    certificates: Vec<Certificate>,
    sub_dag_index: SequenceNumber,
    previous: Option<&CommittedSubDag>,
    epoch_commit_floor: Option<TimestampMs>,
) -> CommittedSubDag {
    let leader = certificates.last().expect("a sub-dag ends with its leader").clone();
    CommittedSubDag::new_with_commit_floor(
        certificates,
        leader,
        sub_dag_index,
        ReputationScores::default(),
        previous,
        epoch_commit_floor,
        EpochSeedChainValue::genesis_placeholder(),
    )
}

/// Commits `certificates` (leader last) through `CommittedSubDag::new`, which takes no epoch
/// commit floor.
fn commit_without_floor(
    certificates: Vec<Certificate>,
    sub_dag_index: SequenceNumber,
    previous: Option<&CommittedSubDag>,
) -> CommittedSubDag {
    let leader = certificates.last().expect("a sub-dag ends with its leader").clone();
    CommittedSubDag::new(
        certificates,
        leader,
        sub_dag_index,
        ReputationScores::default(),
        previous.cloned(),
        EpochSeedChainValue::genesis_placeholder(),
    )
}
