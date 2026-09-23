//! Property-based tests for `CommittedSubDag` and `ConsensusOutput` commit-time invariants under
//! the sub-second fork gate.
//!
//! A sub-DAG stores its commit time as whole seconds (`commit_timestamp`) and a sub-second part
//! (`commit_timestamp_millis`). The sub-second part is on the wire and in the digest only when
//! [`subsecond_timestamp_active`] holds for the leader's own epoch, and `commit_timestamp_ms` is
//! the single resolution point: a stored zero falls back to the leader's `created_at_ms`. These
//! tests verify, for committed, uninitialised, and decoded sub-DAGs:
//! - `commit_timestamp()` is the floor of `commit_timestamp_ms()`, including the zero fallback
//! - `reaches_epoch_boundary(B)` holds exactly when the commit time is at least `1000 * B`
//!   milliseconds, on the sub-DAG and on the output
//! - a bcs round trip is the identity, a decoded `commit_timestamp_millis` is always below 1000,
//!   and it is 0 whenever the leader's epoch is pre-fork
//! - post-fork, a commit lands at `max(leader, floor + 1 ms)`: strictly after the previous sub-DAG,
//!   or after the epoch commit floor for the first commit of an epoch
//! - `ConsensusOutput::committed_at()` is the floor of `committed_at_ms()`
//!
//! Adiri epochs span the pre-fork (seconds-only) and the sub-second layouts; every epoch is
//! sub-second active on other builds. The epoch and timestamp strategies mirror
//! `header_timestamp_props`.

use proptest::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;
#[cfg(feature = "adiri")]
use tn_types::forks::{SEED_SIGNATURE_FORK_EPOCH, SUBSECOND_TIMESTAMP_FORK_EPOCH};
use tn_types::{
    encode, forks::subsecond_timestamp_active, try_decode, AuthorityIdentifier, Certificate,
    CommittedSubDag, ConsensusOutput, Epoch, EpochSeedChainValue, Hash as _, Header, HeaderBuilder,
    ReputationScores, Round, TimestampMs, TimestampSec, B256,
};

/// Largest whole-second value at which `TimestampMs::from_parts(secs, 999)` still fits in a
/// `u64` millisecond count, so `from_parts` never saturates at or below it.
const MAX_EXACT_SECS: TimestampSec = u64::MAX / 1000 - 1;

/// The top of the millisecond range, where every clamp saturates instead of wrapping.
fn max_ms() -> TimestampMs {
    TimestampMs::from_millis(u64::MAX)
}

// --- wire shadows ---

/// The pre-fork `CommittedSubDag` wire layout (four fields) with derived serde, so a test can
/// write arbitrary stored values and read the raw stored seconds back.
#[derive(Serialize, Deserialize)]
struct LegacyWire {
    /// Committed headers, the leader last.
    headers: Vec<Header>,
    /// Reputation scores as of the commit.
    reputation_scores: ReputationScores,
    /// Stored whole seconds of the commit time.
    commit_timestamp: TimestampSec,
    /// Committee-shuffle randomness.
    randomness: B256,
}

/// The sub-second `CommittedSubDag` wire layout: the legacy fields, then
/// `commit_timestamp_millis`.
#[derive(Serialize, Deserialize)]
struct SubsecondWire {
    /// Committed headers, the leader last.
    headers: Vec<Header>,
    /// Reputation scores as of the commit.
    reputation_scores: ReputationScores,
    /// Stored whole seconds of the commit time.
    commit_timestamp: TimestampSec,
    /// Committee-shuffle randomness.
    randomness: B256,
    /// Stored sub-second part of the commit time.
    commit_timestamp_millis: u16,
}

/// bcs bytes of a sub-DAG over `headers` storing `secs`, in the sub-second layout (carrying
/// `millis`) when `subsecond_layout` is set and in the legacy layout otherwise.
///
/// The layout is chosen by the caller rather than by the leader's epoch so tests can also write
/// the wrong one.
fn wire_bytes(
    headers: Vec<Header>,
    secs: TimestampSec,
    millis: u16,
    subsecond_layout: bool,
) -> Vec<u8> {
    let reputation_scores = ReputationScores::default();
    let randomness = B256::default();
    if subsecond_layout {
        encode(&SubsecondWire {
            headers,
            reputation_scores,
            commit_timestamp: secs,
            randomness,
            commit_timestamp_millis: millis,
        })
    } else {
        encode(&LegacyWire { headers, reputation_scores, commit_timestamp: secs, randomness })
    }
}

/// Decode `bytes`, failing the case with `context` when they do not decode.
fn decode_or_fail<T: DeserializeOwned>(
    bytes: &[u8],
    context: &dyn Debug,
) -> Result<T, TestCaseError> {
    try_decode(bytes)
        .map_err(|err| TestCaseError::fail(format!("{context:?} failed to decode: {err}")))
}

/// The raw stored commit time of `sub_dag`, read back from its own encoding through the layout
/// its leader's epoch selects: `(commit_timestamp, Some(commit_timestamp_millis))` for the
/// sub-second layout, `(commit_timestamp, None)` for the legacy layout, which has no sub-second
/// field.
///
/// Decoding requires every byte to be consumed, so this also proves the encoding carries the
/// sub-second field exactly when the leader's epoch is active.
fn stored(sub_dag: &CommittedSubDag) -> Result<(TimestampSec, Option<u16>), TestCaseError> {
    let bytes = encode(sub_dag);
    let context = format!("sub-dag at leader epoch {}", sub_dag.leader_epoch());
    if subsecond_timestamp_active(sub_dag.leader_epoch()) {
        let wire: SubsecondWire = decode_or_fail(&bytes, &context)?;
        Ok((wire.commit_timestamp, Some(wire.commit_timestamp_millis)))
    } else {
        let wire: LegacyWire = decode_or_fail(&bytes, &context)?;
        Ok((wire.commit_timestamp, None))
    }
}

// --- sub-DAG specs ---

/// The headers of a sub-DAG under test: committed ancestors of the leader, then the leader.
#[derive(Clone, Debug)]
struct HeadersSpec {
    /// Epoch of every header, so also the leader epoch that selects the sub-DAG's layout.
    epoch: Epoch,
    /// Leader round.
    round: Round,
    /// Leader creation time, before the header gate drops a dormant sub-second part.
    leader_created_at: TimestampMs,
    /// How many milliseconds before the leader each ancestor was created.
    ancestor_offsets: Vec<u64>,
}

impl HeadersSpec {
    /// The leader header.
    fn leader(&self) -> Header {
        header(self.epoch, self.round, AuthorityIdentifier::default(), self.leader_created_at)
    }

    /// Every header, the ancestors first and the leader last.
    fn headers(&self) -> Vec<Header> {
        self.ancestor_offsets
            .iter()
            .zip(1u8..)
            .map(|(offset, author)| {
                let created_at = TimestampMs::from_millis(
                    self.leader_created_at.as_millis().saturating_sub(*offset),
                );
                header(
                    self.epoch,
                    self.round.saturating_sub(1),
                    AuthorityIdentifier::from_bytes([author; 32]),
                    created_at,
                )
            })
            .chain([self.leader()])
            .collect()
    }
}

/// A header by `author` at `epoch` and `round` created at `created_at`, the epoch set first so
/// the builder's gate decides the sub-second part.
fn header(
    epoch: Epoch,
    round: Round,
    author: AuthorityIdentifier,
    created_at: TimestampMs,
) -> Header {
    HeaderBuilder::default()
        .author(author)
        .round(round)
        .epoch(epoch)
        .created_at_ms(created_at)
        .build()
}

/// A certificate over `header`.
fn certificate(header: Header) -> Certificate {
    let mut certificate = Certificate::default();
    certificate.update_header_for_test(header);
    certificate
}

/// How a sub-DAG under test got its stored commit time.
#[derive(Clone, Debug)]
enum Origin {
    /// `CommittedSubDag::new_with_commit_floor`, after `previous` when there is one.
    Committed {
        /// The previously committed sub-DAG.
        previous: Option<Box<SubDagSpec>>,
        /// The epoch commit floor.
        floor: Option<TimestampMs>,
    },
    /// `CommittedSubDag::new_with_headers_for_test`: the stored commit time is the legacy
    /// uninitialised zero, which resolves through the leader.
    Uninitialised,
    /// Decoded from bytes storing `secs` and, in the sub-second layout only, `millis`.
    ///
    /// Reaches stored values no constructor produces: a zero second with a non-zero sub-second
    /// part, seconds beyond the millisecond range, a commit time older than its leader.
    Decoded {
        /// Stored whole seconds.
        secs: TimestampSec,
        /// Stored sub-second part; always 0 at a dormant epoch, whose layout has no such field.
        millis: u16,
    },
}

/// A sub-DAG under test.
#[derive(Clone, Debug)]
struct SubDagSpec {
    /// The committed headers.
    headers: HeadersSpec,
    /// Where the stored commit time came from.
    origin: Origin,
}

/// Commit a sub-DAG over `headers` after `previous`, bounded below by `floor`.
fn commit(
    headers: &HeadersSpec,
    previous: Option<&CommittedSubDag>,
    floor: Option<TimestampMs>,
) -> CommittedSubDag {
    let certificates: Vec<_> = headers.headers().into_iter().map(certificate).collect();
    CommittedSubDag::new_with_commit_floor(
        certificates,
        certificate(headers.leader()),
        1,
        ReputationScores::default(),
        previous,
        floor,
        EpochSeedChainValue::genesis_placeholder(),
    )
}

/// Build the sub-DAG `spec` describes.
fn build(spec: &SubDagSpec) -> Result<CommittedSubDag, TestCaseError> {
    match &spec.origin {
        Origin::Committed { previous, floor } => {
            let previous = previous.as_deref().map(build).transpose()?;
            Ok(commit(&spec.headers, previous.as_ref(), *floor))
        }
        Origin::Uninitialised => {
            Ok(CommittedSubDag::new_with_headers_for_test(spec.headers.headers()))
        }
        Origin::Decoded { secs, millis } => {
            let active = subsecond_timestamp_active(spec.headers.epoch);
            decode_or_fail(&wire_bytes(spec.headers.headers(), *secs, *millis, active), spec)
        }
    }
}

/// `later` is strictly after `earlier`, except at the top of the millisecond range, where
/// nothing is later: there the clamp saturates and `later` must equal `earlier` instead of
/// wrapping.
fn prop_assert_strictly_after(
    earlier: TimestampMs,
    later: TimestampMs,
) -> Result<(), TestCaseError> {
    if earlier == max_ms() {
        prop_assert_eq!(later, earlier, "the clamp must saturate at {}", earlier);
    } else {
        prop_assert!(later > earlier, "{} is not strictly after {}", later, earlier);
    }
    Ok(())
}

/// Whether a commit at `commit` has reached `boundary` by the millisecond threshold, computed
/// in `u128` so `1000 * boundary` cannot overflow.
fn reaches_by_millis(commit: TimestampMs, boundary: TimestampSec) -> bool {
    u128::from(commit.as_millis()) >= 1000 * u128::from(boundary)
}

/// An epoch boundary to test a commit against.
#[derive(Clone, Copy, Debug)]
enum Boundary {
    /// Any boundary at all.
    Absolute(TimestampSec),
    /// This many seconds from the commit's own whole second, so both outcomes and the exact
    /// threshold are exercised.
    Relative(i64),
}

impl Boundary {
    /// The boundary in whole seconds for a commit at `commit_secs`.
    fn resolve(self, commit_secs: TimestampSec) -> TimestampSec {
        match self {
            Self::Absolute(boundary) => boundary,
            Self::Relative(offset) => commit_secs.saturating_add_signed(offset),
        }
    }
}

// --- strategies ---

/// An epoch at which the sub-second fork is active on this build: the adiri placeholder fork
/// epoch, or genesis elsewhere.
fn active_epoch() -> Epoch {
    #[cfg(feature = "adiri")]
    {
        SUBSECOND_TIMESTAMP_FORK_EPOCH
    }
    #[cfg(not(feature = "adiri"))]
    {
        0
    }
}

/// Epochs covering every sub-DAG layout this build produces, with each fork boundary weighted
/// in: pre-fork epochs (legacy and seed-signature-only headers) and sub-second active epochs.
#[cfg(feature = "adiri")]
fn epoch_strategy() -> impl Strategy<Value = Epoch> {
    prop_oneof![dormant_epoch_strategy(), active_epoch_strategy()]
}

/// Epochs covering every sub-DAG layout this build produces. Non-adiri builds are sub-second
/// active from genesis, so any epoch will do; the range ends are weighted in.
#[cfg(not(feature = "adiri"))]
fn epoch_strategy() -> impl Strategy<Value = Epoch> {
    prop_oneof![any::<Epoch>(), prop::sample::select(vec![0, 1, Epoch::MAX])]
}

/// Adiri epochs before the sub-second fork, spanning the legacy and the seed-signature-only
/// header layouts and the seed-signature fork boundary.
#[cfg(feature = "adiri")]
fn dormant_epoch_strategy() -> impl Strategy<Value = Epoch> {
    prop_oneof![
        0..SEED_SIGNATURE_FORK_EPOCH,
        SEED_SIGNATURE_FORK_EPOCH..SUBSECOND_TIMESTAMP_FORK_EPOCH,
        prop::sample::select(vec![
            0,
            SEED_SIGNATURE_FORK_EPOCH - 1,
            SEED_SIGNATURE_FORK_EPOCH,
            SUBSECOND_TIMESTAMP_FORK_EPOCH - 1,
        ]),
    ]
}

/// Epochs at which the sub-second fork is active on this build.
#[cfg(feature = "adiri")]
fn active_epoch_strategy() -> impl Strategy<Value = Epoch> {
    SUBSECOND_TIMESTAMP_FORK_EPOCH..=Epoch::MAX
}

/// Epochs at which the sub-second fork is active on this build: every epoch.
#[cfg(not(feature = "adiri"))]
fn active_epoch_strategy() -> impl Strategy<Value = Epoch> {
    epoch_strategy()
}

/// Whole seconds up to [`MAX_EXACT_SECS`], weighted toward the range ends and present-day
/// wall-clock values.
fn secs_strategy() -> impl Strategy<Value = TimestampSec> {
    prop_oneof![
        0..=MAX_EXACT_SECS,
        1_600_000_000u64..2_000_000_000,
        prop::sample::select(vec![0, 1, MAX_EXACT_SECS - 1, MAX_EXACT_SECS]),
    ]
}

/// Valid sub-second parts, weighted toward the range ends.
fn millis_strategy() -> impl Strategy<Value = u16> {
    prop_oneof![0..=999u16, prop::sample::select(vec![0u16, 1, 998, 999])]
}

/// Millisecond timestamps for leaders and epoch floors.
///
/// Mostly `from_parts` inside the range where it never saturates, plus any millisecond count
/// at all, plus the saturation edge: seconds from `u64::MAX / 1000` upward, where `from_parts`
/// either lands exactly at the top of the range or saturates to `u64::MAX`.
fn timestamp_strategy() -> impl Strategy<Value = TimestampMs> {
    let exact = (secs_strategy(), millis_strategy())
        .prop_map(|(secs, millis)| TimestampMs::from_parts(secs, millis));
    let saturating = (
        prop_oneof![Just(u64::MAX / 1000), (u64::MAX / 1000)..=u64::MAX, Just(u64::MAX)],
        millis_strategy(),
    )
        .prop_map(|(secs, millis)| TimestampMs::from_parts(secs, millis));
    prop_oneof![
        8 => exact,
        1 => any::<u64>().prop_map(TimestampMs::from_millis),
        1 => saturating,
    ]
}

/// Arbitrary `commit_timestamp_millis` wire values, with the valid range, the rejected range,
/// and the boundary between them each weighted in.
fn wire_millis_strategy() -> impl Strategy<Value = u16> {
    prop_oneof![
        0..1000u16,
        1000..=u16::MAX,
        prop::sample::select(vec![0u16, 999, 1000, 1001, u16::MAX]),
    ]
}

/// Stored commit seconds: the exact range, any `u64`, and the edges where a stored zero falls
/// back to the leader and where `from_parts` starts to saturate.
fn stored_secs_strategy() -> impl Strategy<Value = TimestampSec> {
    prop_oneof![
        secs_strategy(),
        any::<TimestampSec>(),
        prop::sample::select(vec![0, u64::MAX / 1000, u64::MAX / 1000 + 1, u64::MAX]),
    ]
}

/// How long before the leader an ancestor was created: mostly within a few seconds, sometimes
/// any gap (saturating at time zero).
fn ancestor_offset_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![4 => 1..=5_000u64, 1 => any::<u64>()]
}

/// Sub-DAG headers at `epoch`: any leader round and creation time, and up to two ancestors.
fn headers_strategy(epoch: Epoch) -> impl Strategy<Value = HeadersSpec> {
    (any::<Round>(), timestamp_strategy(), prop::collection::vec(ancestor_offset_strategy(), 0..3))
        .prop_map(move |(round, leader_created_at, ancestor_offsets)| HeadersSpec {
            epoch,
            round,
            leader_created_at,
            ancestor_offsets,
        })
}

/// Origins without a previous sub-DAG for a sub-DAG at `epoch`; a decoded sub-second part is
/// drawn only where the epoch's layout carries one.
fn base_origin_strategy(epoch: Epoch) -> BoxedStrategy<Origin> {
    let millis = if subsecond_timestamp_active(epoch) {
        millis_strategy().boxed()
    } else {
        Just(0u16).boxed()
    };
    prop_oneof![
        prop::option::of(timestamp_strategy())
            .prop_map(|floor| Origin::Committed { previous: None, floor }),
        Just(Origin::Uninitialised),
        (stored_secs_strategy(), millis)
            .prop_map(|(secs, millis)| Origin::Decoded { secs, millis }),
    ]
    .boxed()
}

/// Sub-DAGs at `epoch` without a previous sub-DAG.
fn base_sub_dag_strategy(epoch: Epoch) -> impl Strategy<Value = SubDagSpec> {
    (headers_strategy(epoch), base_origin_strategy(epoch))
        .prop_map(|(headers, origin)| SubDagSpec { headers, origin })
}

/// Sub-DAGs at `epoch` from every origin, including ones committed after a previous sub-DAG of
/// the same epoch.
fn sub_dag_at(epoch: Epoch) -> BoxedStrategy<SubDagSpec> {
    let chained = (
        headers_strategy(epoch),
        base_sub_dag_strategy(epoch),
        prop::option::of(timestamp_strategy()),
    )
        .prop_map(|(headers, previous, floor)| SubDagSpec {
            headers,
            origin: Origin::Committed { previous: Some(Box::new(previous)), floor },
        });
    prop_oneof![3 => base_sub_dag_strategy(epoch), 2 => chained].boxed()
}

/// Sub-DAGs at every epoch this build knows, from every origin.
fn sub_dag_strategy() -> impl Strategy<Value = SubDagSpec> {
    epoch_strategy().prop_flat_map(sub_dag_at)
}

/// Epoch boundaries: any value at all, or within two seconds of the commit.
fn boundary_strategy() -> impl Strategy<Value = Boundary> {
    prop_oneof![
        prop_oneof![
            any::<TimestampSec>(),
            prop::sample::select(vec![0, u64::MAX / 1000, u64::MAX])
        ]
        .prop_map(Boundary::Absolute),
        (-2i64..=2).prop_map(Boundary::Relative),
    ]
}

// --- resolution: property 1 ---

proptest! {
    /// Property 1: `commit_timestamp()` is the whole seconds of `commit_timestamp_ms()` for
    /// every sub-DAG, including one whose stored zero falls back to the leader.
    #[test]
    fn prop_commit_timestamp_is_the_floor_of_commit_timestamp_ms(spec in sub_dag_strategy()) {
        let sub_dag = build(&spec)?;
        prop_assert_eq!(
            sub_dag.commit_timestamp(),
            sub_dag.commit_timestamp_ms().secs(),
            "{:?}",
            spec
        );
    }

    /// Property 1, resolution rule: `commit_timestamp_ms()` is the stored seconds and sub-second
    /// part, except that a stored zero (both parts) falls back to the leader's `created_at_ms()`.
    /// A pre-fork sub-DAG resolves with a sub-second part of 0.
    #[test]
    fn prop_commit_timestamp_ms_resolves_the_stored_fields(spec in sub_dag_strategy()) {
        let sub_dag = build(&spec)?;
        let (secs, millis) = stored(&sub_dag)?;
        let millis = millis.unwrap_or(0);
        let expected = if secs == 0 && millis == 0 {
            sub_dag.leader().created_at_ms()
        } else {
            TimestampMs::from_parts(secs, millis)
        };
        prop_assert_eq!(sub_dag.commit_timestamp_ms(), expected, "stored ({}, {}) for {:?}", secs, millis, spec);
    }

    /// Property 1, zero fallback: an uninitialised sub-DAG resolves to its leader's creation
    /// time exactly, in milliseconds and in whole seconds, at every epoch.
    #[test]
    fn prop_uninitialised_sub_dag_resolves_through_the_leader(
        headers in epoch_strategy().prop_flat_map(headers_strategy),
    ) {
        let sub_dag = CommittedSubDag::new_with_headers_for_test(headers.headers());
        prop_assert_eq!(stored(&sub_dag)?.0, 0, "{:?} must store the legacy zero", headers);
        let leader = headers.leader();
        prop_assert_eq!(sub_dag.commit_timestamp_ms(), leader.created_at_ms(), "{:?}", headers);
        prop_assert_eq!(sub_dag.commit_timestamp(), *leader.created_at(), "{:?}", headers);
    }
}

// --- epoch boundary: property 2 ---

proptest! {
    /// Property 2: `CommittedSubDag::reaches_epoch_boundary(B)` holds exactly when the resolved
    /// commit time is at least `1000 * B` milliseconds, for any boundary.
    #[test]
    fn prop_sub_dag_boundary_is_the_millisecond_threshold(
        spec in sub_dag_strategy(),
        boundary in boundary_strategy(),
    ) {
        let sub_dag = build(&spec)?;
        let commit_ms = sub_dag.commit_timestamp_ms();
        let boundary = boundary.resolve(commit_ms.secs());
        prop_assert_eq!(
            sub_dag.reaches_epoch_boundary(boundary),
            reaches_by_millis(commit_ms, boundary),
            "commit {} against boundary {} for {:?}",
            commit_ms, boundary, spec
        );
    }

    /// Property 2 on `ConsensusOutput`: `reaches_epoch_boundary(B)` holds exactly when the
    /// wrapped sub-DAG's commit time is at least `1000 * B` milliseconds, for any boundary.
    #[test]
    fn prop_output_boundary_is_the_millisecond_threshold(
        spec in sub_dag_strategy(),
        boundary in boundary_strategy(),
        number in any::<u64>(),
    ) {
        let sub_dag = build(&spec)?;
        let commit_ms = sub_dag.commit_timestamp_ms();
        let boundary = boundary.resolve(commit_ms.secs());
        let output = ConsensusOutput::new_with_subdag(sub_dag, Default::default(), number);
        prop_assert_eq!(
            output.reaches_epoch_boundary(boundary),
            reaches_by_millis(commit_ms, boundary),
            "commit {} against boundary {} for {:?}",
            commit_ms, boundary, spec
        );
    }
}

// --- serde: property 3 ---

proptest! {
    /// Property 3: a bcs round trip reproduces the sub-DAG exactly (equal value, same bytes on
    /// re-encode, same digest, same resolved commit time) at every epoch.
    #[test]
    fn prop_bcs_round_trip_is_identity(spec in sub_dag_strategy()) {
        let sub_dag = build(&spec)?;
        let bytes = encode(&sub_dag);
        let decoded: CommittedSubDag = decode_or_fail(&bytes, &spec)?;
        prop_assert_eq!(&decoded, &sub_dag, "{:?}", spec);
        prop_assert_eq!(encode(&decoded), bytes, "{:?}", spec);
        prop_assert_eq!(decoded.digest(), sub_dag.digest(), "{:?}", spec);
        prop_assert_eq!(decoded.commit_timestamp_ms(), sub_dag.commit_timestamp_ms(), "{:?}", spec);
    }

    /// Property 3: a decoded sub-DAG carries a sub-second part below 1000 when its leader's epoch
    /// is active, and none at all (it resolves with 0) when the leader's epoch is pre-fork.
    #[test]
    fn prop_decoded_millis_is_below_1000_and_zero_pre_fork(spec in sub_dag_strategy()) {
        let sub_dag = build(&spec)?;
        let decoded: CommittedSubDag = decode_or_fail(&encode(&sub_dag), &spec)?;
        match stored(&decoded)? {
            (_, Some(millis)) => {
                prop_assert!(millis < 1000, "decoded millis {} for {:?}", millis, spec);
            }
            (secs, None) => {
                prop_assert!(
                    !subsecond_timestamp_active(decoded.leader_epoch()),
                    "an active leader's sub-dag lost its sub-second field: {:?}",
                    spec
                );
                // with no stored sub-second part, the resolution is whole seconds or, through the
                // zero fallback, a pre-fork leader's whole seconds
                prop_assert_eq!(decoded.leader().created_at_millis(), 0, "{:?}", spec);
                let expected = if secs == 0 {
                    decoded.leader().created_at_ms()
                } else {
                    TimestampMs::from_parts(secs, 0)
                };
                prop_assert_eq!(decoded.commit_timestamp_ms(), expected, "{:?}", spec);
            }
        }
    }

    /// Property 3, wire side: at an active leader epoch, bytes storing an arbitrary
    /// `commit_timestamp_millis` decode only when it is below 1000, and then keep it and the
    /// seconds exactly; every larger value is rejected instead of carried into the seconds.
    #[test]
    fn prop_active_wire_millis_of_1000_or_more_are_rejected(
        (headers, secs) in active_epoch_strategy()
            .prop_flat_map(|epoch| (headers_strategy(epoch), stored_secs_strategy())),
        wire_millis in wire_millis_strategy(),
    ) {
        prop_assert!(subsecond_timestamp_active(headers.epoch), "epoch {} must be active", headers.epoch);
        let bytes = wire_bytes(headers.headers(), secs, wire_millis, true);
        match try_decode::<CommittedSubDag>(&bytes) {
            Ok(decoded) => {
                prop_assert!(wire_millis < 1000, "wire millis {} decoded for {:?}", wire_millis, headers);
                prop_assert_eq!(stored(&decoded)?, (secs, Some(wire_millis)), "{:?}", headers);
                prop_assert_eq!(encode(&decoded), bytes, "{:?}", headers);
            }
            Err(err) => prop_assert!(
                wire_millis >= 1000,
                "wire millis {} rejected for {:?}: {}",
                wire_millis, headers, err
            ),
        }
    }

    /// Property 3, layout: an active leader's sub-DAG must carry `commit_timestamp_millis`, so
    /// the four-field legacy bytes are rejected rather than read with an implied 0.
    #[test]
    fn prop_active_leader_rejects_the_legacy_layout(
        (headers, secs) in active_epoch_strategy()
            .prop_flat_map(|epoch| (headers_strategy(epoch), stored_secs_strategy())),
    ) {
        let bytes = wire_bytes(headers.headers(), secs, 0, false);
        prop_assert!(
            try_decode::<CommittedSubDag>(&bytes).is_err(),
            "legacy bytes decoded at active epoch {}",
            headers.epoch
        );
    }

    /// Property 3, pre-fork (adiri): legacy bytes at a pre-fork leader epoch decode with a
    /// sub-second part of 0, resolving to the stored whole seconds (or, through the zero
    /// fallback, the leader's whole seconds), and re-encode without a sub-second field.
    #[cfg(feature = "adiri")]
    #[test]
    fn prop_pre_fork_sub_dag_decodes_with_zero_millis(
        (headers, secs) in dormant_epoch_strategy()
            .prop_flat_map(|epoch| (headers_strategy(epoch), stored_secs_strategy())),
    ) {
        prop_assert!(!subsecond_timestamp_active(headers.epoch), "epoch {} must be dormant", headers.epoch);
        let bytes = wire_bytes(headers.headers(), secs, 0, false);
        let decoded: CommittedSubDag = decode_or_fail(&bytes, &headers)?;
        prop_assert_eq!(encode(&decoded), bytes, "{:?}", headers);
        prop_assert_eq!(decoded.leader().created_at_millis(), 0, "{:?}", headers);
        let expected = if secs == 0 {
            decoded.leader().created_at_ms()
        } else {
            TimestampMs::from_parts(secs, 0)
        };
        prop_assert_eq!(decoded.commit_timestamp_ms(), expected, "stored {} for {:?}", secs, headers);
    }

    /// Property 3, pre-fork (adiri): bytes appending a `commit_timestamp_millis` to a pre-fork
    /// leader's sub-DAG are rejected, whatever its value, so no sub-second part can reach a
    /// pre-fork commit.
    #[cfg(feature = "adiri")]
    #[test]
    fn prop_pre_fork_leader_rejects_the_subsecond_layout(
        (headers, secs) in dormant_epoch_strategy()
            .prop_flat_map(|epoch| (headers_strategy(epoch), stored_secs_strategy())),
        wire_millis in wire_millis_strategy(),
    ) {
        let bytes = wire_bytes(headers.headers(), secs, wire_millis, true);
        prop_assert!(
            try_decode::<CommittedSubDag>(&bytes).is_err(),
            "sub-second bytes with millis {} decoded at pre-fork epoch {}",
            wire_millis, headers.epoch
        );
    }
}

// --- clamp: property 4 ---

proptest! {
    /// Property 4: for any previous sub-DAG P and leader L of the same active epoch, the commit
    /// lands at `max(L, P + 1 ms)` whatever epoch floor is passed: strictly after P (saturating
    /// at the top of the range instead of wrapping) and never before L.
    #[test]
    fn prop_post_fork_commit_is_strictly_after_the_previous_sub_dag(
        (previous, leader, floor) in active_epoch_strategy().prop_flat_map(|epoch| (
            sub_dag_at(epoch),
            headers_strategy(epoch),
            prop::option::of(timestamp_strategy()),
        )),
    ) {
        prop_assert!(subsecond_timestamp_active(leader.epoch), "epoch {} must be active", leader.epoch);
        let previous = build(&previous)?;
        let previous_ms = previous.commit_timestamp_ms();
        let leader_ms = leader.leader().created_at_ms();
        let commit_ms = commit(&leader, Some(&previous), floor).commit_timestamp_ms();
        prop_assert_eq!(
            commit_ms,
            leader_ms.max(previous_ms.saturating_add_millis(1)),
            "leader {} after {}",
            leader_ms, previous_ms
        );
        prop_assert!(commit_ms >= leader_ms, "commit {} precedes leader {}", commit_ms, leader_ms);
        prop_assert_strictly_after(previous_ms, commit_ms)?;
    }

    /// Property 4, first commit of an epoch: with no previous sub-DAG, an active leader L and an
    /// epoch floor F commit at `max(L, F + 1 ms)`, strictly after F and never before L; without
    /// a floor the commit is exactly L.
    #[test]
    fn prop_post_fork_first_commit_is_strictly_after_the_epoch_floor(
        leader in active_epoch_strategy().prop_flat_map(headers_strategy),
        floor in timestamp_strategy(),
    ) {
        let leader_ms = leader.leader().created_at_ms();
        let commit_ms = commit(&leader, None, Some(floor)).commit_timestamp_ms();
        prop_assert_eq!(
            commit_ms,
            leader_ms.max(floor.saturating_add_millis(1)),
            "leader {} over floor {}",
            leader_ms, floor
        );
        prop_assert!(commit_ms >= leader_ms, "commit {} precedes leader {}", commit_ms, leader_ms);
        prop_assert_strictly_after(floor, commit_ms)?;
        prop_assert_eq!(
            commit(&leader, None, None).commit_timestamp_ms(),
            leader_ms,
            "no floor must keep the leader time"
        );
    }

    /// Property 4 along a chain: successive commits of one active epoch, led by arbitrary
    /// (possibly regressing) leader times after an optional epoch floor, strictly increase and
    /// never precede their leaders.
    #[test]
    fn prop_post_fork_commit_chain_strictly_increases(
        (leaders, floor) in active_epoch_strategy().prop_flat_map(|epoch| (
            prop::collection::vec(headers_strategy(epoch), 1..8),
            prop::option::of(timestamp_strategy()),
        )),
    ) {
        let mut previous: Option<CommittedSubDag> = None;
        for leader in &leaders {
            let sub_dag = commit(leader, previous.as_ref(), floor);
            let commit_ms = sub_dag.commit_timestamp_ms();
            prop_assert!(
                commit_ms >= sub_dag.leader().created_at_ms(),
                "commit {} precedes its leader",
                commit_ms
            );
            match (&previous, floor) {
                (Some(previous), _) => {
                    prop_assert_strictly_after(previous.commit_timestamp_ms(), commit_ms)?
                }
                (None, Some(floor)) => prop_assert_strictly_after(floor, commit_ms)?,
                (None, None) => {}
            }
            previous = Some(sub_dag);
        }
    }

    /// Property 4, precedence: a previous sub-DAG overrides any epoch commit floor at every
    /// epoch, and `CommittedSubDag::new` is `new_with_commit_floor` without a floor.
    #[test]
    fn prop_previous_sub_dag_overrides_the_epoch_floor(
        (previous, leader, floor) in epoch_strategy().prop_flat_map(|epoch| (
            sub_dag_at(epoch),
            headers_strategy(epoch),
            timestamp_strategy(),
        )),
    ) {
        let previous = build(&previous)?;
        let with_floor = commit(&leader, Some(&previous), Some(floor));
        let without_floor = commit(&leader, Some(&previous), None);
        prop_assert_eq!(&with_floor, &without_floor, "floor {} must be ignored", floor);
        let via_new = CommittedSubDag::new(
            leader.headers().into_iter().map(certificate).collect(),
            certificate(leader.leader()),
            1,
            ReputationScores::default(),
            Some(previous),
            EpochSeedChainValue::genesis_placeholder(),
        );
        prop_assert_eq!(&via_new, &without_floor, "`new` must delegate without a floor");
    }

    /// Property 4 at the fork seam (adiri): a pre-fork previous sub-DAG resolves to whole
    /// seconds, and the first sub-second commit after it lands at `max(L, P + 1 ms)`.
    #[cfg(feature = "adiri")]
    #[test]
    fn prop_fork_seam_floors_on_the_pre_fork_whole_seconds(
        previous in dormant_epoch_strategy().prop_flat_map(sub_dag_at),
        leader in headers_strategy(SUBSECOND_TIMESTAMP_FORK_EPOCH),
    ) {
        let previous = build(&previous)?;
        let previous_ms = previous.commit_timestamp_ms();
        // a pre-fork commit has no sub-second part; only a saturated resolution shows one
        if previous_ms != max_ms() {
            prop_assert_eq!(previous_ms.subsec_millis(), 0, "pre-fork commit {}", previous_ms);
        }
        let leader_ms = leader.leader().created_at_ms();
        let commit_ms = commit(&leader, Some(&previous), None).commit_timestamp_ms();
        prop_assert_eq!(
            commit_ms,
            leader_ms.max(previous_ms.saturating_add_millis(1)),
            "leader {} after pre-fork {}",
            leader_ms, previous_ms
        );
        prop_assert_strictly_after(previous_ms, commit_ms)?;
    }

    /// Pre-fork (adiri), the commit keeps the legacy seconds rule: the max of the previous
    /// sub-DAG's raw stored seconds (0 when absent, no zero fallback) and the leader's seconds,
    /// no sub-second field, and the epoch commit floor ignored.
    #[cfg(feature = "adiri")]
    #[test]
    fn prop_pre_fork_commit_is_the_legacy_seconds_max(
        (previous, leader, floor) in dormant_epoch_strategy().prop_flat_map(|epoch| (
            prop::option::of(sub_dag_at(epoch)),
            headers_strategy(epoch),
            prop::option::of(timestamp_strategy()),
        )),
    ) {
        let previous = previous.as_ref().map(build).transpose()?;
        let previous_secs = match &previous {
            Some(previous) => stored(previous)?.0,
            None => 0,
        };
        let sub_dag = commit(&leader, previous.as_ref(), floor);
        let expected = previous_secs.max(*leader.leader().created_at());
        prop_assert_eq!(stored(&sub_dag)?, (expected, None), "previous raw seconds {}", previous_secs);
        prop_assert_eq!(&sub_dag, &commit(&leader, previous.as_ref(), None), "floor must be ignored");
    }
}

// --- output: property 5 ---

proptest! {
    /// Property 5: `ConsensusOutput::committed_at()` is the whole seconds of
    /// `committed_at_ms()`, which is the wrapped sub-DAG's resolved commit time.
    #[test]
    fn prop_output_committed_at_is_the_floor_of_committed_at_ms(
        spec in sub_dag_strategy(),
        number in any::<u64>(),
    ) {
        let sub_dag = build(&spec)?;
        let output = ConsensusOutput::new_with_subdag(sub_dag.clone(), Default::default(), number);
        prop_assert_eq!(output.committed_at(), output.committed_at_ms().secs(), "{:?}", spec);
        prop_assert_eq!(output.committed_at_ms(), sub_dag.commit_timestamp_ms(), "{:?}", spec);
    }

    /// Property 5 across the wire: a bcs round trip of the output keeps its consensus header
    /// hash and its commit time, and the decoded output still floors `committed_at_ms()`.
    #[test]
    fn prop_decoded_output_keeps_its_commit_time(
        spec in sub_dag_strategy(),
        number in any::<u64>(),
    ) {
        let output = ConsensusOutput::new_with_subdag(build(&spec)?, Default::default(), number);
        let decoded: ConsensusOutput = decode_or_fail(&encode(&output), &spec)?;
        prop_assert_eq!(decoded.consensus_header_hash(), output.consensus_header_hash(), "{:?}", spec);
        prop_assert_eq!(decoded.committed_at_ms(), output.committed_at_ms(), "{:?}", spec);
        prop_assert_eq!(decoded.committed_at(), decoded.committed_at_ms().secs(), "{:?}", spec);
    }
}

/// A stored zero second with a non-zero sub-second part is an initialised commit time, not the
/// legacy default: it resolves to itself rather than falling back to the leader.
#[test]
fn test_zero_seconds_with_millis_is_not_the_legacy_default() {
    let leader = header(
        active_epoch(),
        2,
        AuthorityIdentifier::default(),
        TimestampMs::from_parts(1_700_000_000, 250),
    );
    let bytes = wire_bytes(vec![leader], 0, 5, true);
    let sub_dag: CommittedSubDag = try_decode(&bytes).expect("valid sub-second bytes");
    assert_eq!(sub_dag.commit_timestamp_ms(), TimestampMs::from_parts(0, 5));
    assert_eq!(sub_dag.commit_timestamp(), 0);
    assert!(sub_dag.reaches_epoch_boundary(0));
    assert!(!sub_dag.reaches_epoch_boundary(1));
}

/// A previous commit at the top of the millisecond range leaves nothing strictly later: the
/// clamp saturates there instead of wrapping to the start of time.
#[test]
fn test_saturated_previous_commit_clamps_without_wrapping() {
    let epoch = active_epoch();
    let previous_leader =
        header(epoch, 2, AuthorityIdentifier::default(), TimestampMs::from_parts(1, 0));
    let bytes = wire_bytes(vec![previous_leader], u64::MAX, 999, true);
    let previous: CommittedSubDag = try_decode(&bytes).expect("valid sub-second bytes");
    assert_eq!(previous.commit_timestamp_ms(), max_ms(), "stored seconds past the range saturate");
    let leader = HeadersSpec {
        epoch,
        round: 4,
        leader_created_at: TimestampMs::from_parts(1_700_000_000, 0),
        ancestor_offsets: Vec::new(),
    };
    let sub_dag = commit(&leader, Some(&previous), None);
    assert_eq!(sub_dag.commit_timestamp_ms(), max_ms());
    assert_eq!(stored(&sub_dag).expect("re-decodes"), (u64::MAX / 1000, Some(615)));
}
