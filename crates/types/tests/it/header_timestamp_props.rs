//! Property-based tests for `Header` creation-time invariants under the sub-second fork gate.
//!
//! A header's creation time is split into whole seconds (`created_at`) and a sub-second part
//! (`created_at_millis`). The sub-second part is kept, encoded, and covered by the digest only
//! when [`subsecond_timestamp_active`] holds for the header's own epoch. These tests verify, on
//! every construction path (`Header::new`, and `HeaderBuilder::created_at_ms` with the epoch set
//! before or after the timestamp):
//! - `created_at()` is the input's floored seconds, whether the gate is active or dormant
//! - `created_at_millis()` is the input's sub-second part when active and 0 when dormant
//! - `created_at_ms()` recombines the two fields and agrees with `created_at()` on the seconds
//! - a bcs round trip is the identity, and a decoded `created_at_millis` is always below 1000
//! - between the builder's `created_at` and `created_at_ms`, the call made last wins
//! - every construction path produces the same header, and the digest binds the sub-second part
//!   exactly when the gate holds
//!
//! Adiri epochs span all three header layouts (legacy, seed-signature only, sub-second active);
//! every epoch is sub-second active on other builds.

use indexmap::IndexMap;
use proptest::prelude::*;
use std::collections::BTreeSet;
#[cfg(feature = "adiri")]
use tn_types::forks::{SEED_SIGNATURE_FORK_EPOCH, SUBSECOND_TIMESTAMP_FORK_EPOCH};
use tn_types::{
    encode, forks::subsecond_timestamp_active, try_decode, AuthorityIdentifier, BlockHash,
    BlockNumHash, BlsSignature, Epoch, Header, HeaderBuilder, HeaderDigest, Round, TimestampMs,
    TimestampSec, WorkerId,
};

/// Largest whole-second value at which `TimestampMs::from_parts(secs, 999)` still fits in a
/// `u64` millisecond count, so `from_parts` never saturates at or below it.
const MAX_EXACT_SECS: TimestampSec = u64::MAX / 1000 - 1;

/// Every header field other than the epoch and the creation time.
///
/// Varied so the timestamp properties hold on non-default headers and the round trip covers a
/// populated payload and parent set.
#[derive(Clone, Debug)]
struct OtherFields {
    /// Header author.
    author: AuthorityIdentifier,
    /// Header round.
    round: Round,
    /// Batch digests and the worker each came from.
    payload: IndexMap<BlockHash, WorkerId>,
    /// Parent header digests.
    parents: BTreeSet<HeaderDigest>,
    /// Latest execution block known to the author.
    latest_execution_block: BlockNumHash,
}

/// The ways a header under test is constructed.
#[derive(Clone, Copy, Debug)]
enum Construction {
    /// `Header::new`.
    New,
    /// `HeaderBuilder` with `epoch` called before `created_at_ms`.
    BuilderEpochFirst,
    /// `HeaderBuilder` with `epoch` called after `created_at_ms`; the builder gates at `build`,
    /// so the order must not matter.
    BuilderEpochLast,
}

/// Every [`Construction`], so each property runs on all of them.
const CONSTRUCTIONS: [Construction; 3] =
    [Construction::New, Construction::BuilderEpochFirst, Construction::BuilderEpochLast];

/// One timestamp setter call on [`HeaderBuilder`].
#[derive(Clone, Copy, Debug)]
enum Setter {
    /// `HeaderBuilder::created_at` with whole seconds.
    Secs(TimestampSec),
    /// `HeaderBuilder::created_at_ms`.
    Ms(TimestampMs),
}

/// A builder carrying `fields`, with the epoch and creation time left at their defaults.
fn builder_with(fields: &OtherFields) -> HeaderBuilder {
    HeaderBuilder::default()
        .author(fields.author.clone())
        .round(fields.round)
        .payload(fields.payload.clone())
        .parents(fields.parents.clone())
        .latest_execution_block(fields.latest_execution_block)
}

/// Build a header at `epoch` created at `created_at` through `construction`.
fn build(
    construction: Construction,
    epoch: Epoch,
    created_at: TimestampMs,
    fields: &OtherFields,
) -> Header {
    match construction {
        Construction::New => Header::new(
            fields.author.clone(),
            fields.round,
            epoch,
            fields.payload.clone(),
            fields.parents.clone(),
            fields.latest_execution_block,
            BlsSignature::default(),
            created_at,
        ),
        Construction::BuilderEpochFirst => {
            builder_with(fields).epoch(epoch).created_at_ms(created_at).build()
        }
        Construction::BuilderEpochLast => {
            builder_with(fields).created_at_ms(created_at).epoch(epoch).build()
        }
    }
}

/// The sub-second part a header at `epoch` must record for `created_at`.
fn expected_millis(epoch: Epoch, created_at: TimestampMs) -> u16 {
    if subsecond_timestamp_active(epoch) {
        created_at.subsec_millis()
    } else {
        0
    }
}

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

/// Epochs covering every header layout this build produces, with each fork boundary weighted
/// in: the legacy layout below the seed-signature fork, the seed-signature-only layout up to
/// the sub-second fork (both dormant), and the sub-second layout from the sub-second fork on.
#[cfg(feature = "adiri")]
fn epoch_strategy() -> impl Strategy<Value = Epoch> {
    prop_oneof![dormant_epoch_strategy(), active_epoch_strategy()]
}

/// Epochs covering every header layout this build produces. Non-adiri builds are sub-second
/// active from genesis, so any epoch will do; the range ends are weighted in.
#[cfg(not(feature = "adiri"))]
fn epoch_strategy() -> impl Strategy<Value = Epoch> {
    prop_oneof![any::<Epoch>(), prop::sample::select(vec![0, 1, Epoch::MAX])]
}

/// Adiri epochs before the sub-second fork, spanning the legacy and the seed-signature-only
/// layouts and the seed-signature fork boundary.
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

/// Creation times for the header under test.
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

/// Header fields other than the epoch and the creation time: any author and round, up to three
/// payload batches and parents, and any latest execution block.
fn other_fields_strategy() -> impl Strategy<Value = OtherFields> {
    (
        any::<[u8; 32]>(),
        any::<Round>(),
        prop::collection::vec((any::<[u8; 32]>(), any::<WorkerId>()), 0..4),
        prop::collection::btree_set(any::<[u8; 32]>(), 0..4),
        any::<u64>(),
        any::<[u8; 32]>(),
    )
        .prop_map(|(author, round, payload, parents, number, hash)| OtherFields {
            author: AuthorityIdentifier::from_bytes(author),
            round,
            payload: payload
                .into_iter()
                .map(|(digest, worker_id)| (BlockHash::from(digest), worker_id))
                .collect(),
            parents: parents.into_iter().map(HeaderDigest::new).collect(),
            latest_execution_block: BlockNumHash { number, hash: BlockHash::from(hash) },
        })
}

/// Arbitrary `created_at_millis` wire values, with the valid range, the rejected range, and the
/// boundary between them each weighted in.
fn wire_millis_strategy() -> impl Strategy<Value = u16> {
    prop_oneof![
        0..1000u16,
        1000..=u16::MAX,
        prop::sample::select(vec![0u16, 999, 1000, 1001, u16::MAX]),
    ]
}

/// A builder timestamp setter call: whole seconds anywhere in `u64`, or any creation time.
fn setter_strategy() -> impl Strategy<Value = Setter> {
    prop_oneof![
        prop_oneof![secs_strategy(), any::<TimestampSec>()].prop_map(Setter::Secs),
        timestamp_strategy().prop_map(Setter::Ms),
    ]
}

// --- construction: properties 1 to 3 ---

proptest! {
    /// Property 1: `created_at()` is the input's floored seconds on every construction path,
    /// whether or not the sub-second fork is active for the header's epoch.
    #[test]
    fn prop_created_at_is_floored_seconds(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                *header.created_at(),
                ms.secs(),
                "{:?} at epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// Property 2: `created_at_millis()` is the input's sub-second part when
    /// `subsecond_timestamp_active` holds for the header's epoch and 0 when it does not, on
    /// every construction path.
    #[test]
    fn prop_created_at_millis_follows_the_gate(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        let expected = expected_millis(epoch, ms);
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                header.created_at_millis(),
                expected,
                "{:?} at epoch {} (active: {}) for {}",
                construction, epoch, subsecond_timestamp_active(epoch), ms
            );
        }
    }

    /// Property 3: `created_at_ms()` is exactly `from_parts(created_at(), created_at_millis())`
    /// and its whole seconds equal `created_at()`, on every construction path.
    #[test]
    fn prop_created_at_ms_recombines_the_fields(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            let combined = header.created_at_ms();
            prop_assert_eq!(
                combined,
                TimestampMs::from_parts(*header.created_at(), header.created_at_millis()),
                "{:?} at epoch {} for {}",
                construction, epoch, ms
            );
            prop_assert_eq!(
                combined.secs(),
                *header.created_at(),
                "{:?} at epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// Consequence of properties 1 to 3: an active header reproduces its creation time exactly,
    /// and a dormant one reproduces it floored to the whole second.
    #[test]
    fn prop_created_at_ms_is_the_input_up_to_the_gate(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        let expected = if subsecond_timestamp_active(epoch) {
            ms
        } else {
            TimestampMs::from_parts(ms.secs(), 0)
        };
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                header.created_at_ms(),
                expected,
                "{:?} at epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// Adiri pins the dormant side of property 2 without consulting the gate: every epoch
    /// before `SUBSECOND_TIMESTAMP_FORK_EPOCH` records a sub-second part of 0.
    #[cfg(feature = "adiri")]
    #[test]
    fn prop_pre_fork_epochs_drop_the_millis(
        epoch in dormant_epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                header.created_at_millis(),
                0,
                "{:?} kept a sub-second part at pre-fork epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// Non-adiri builds pin the active side of property 2 without consulting the gate: every
    /// epoch, genesis included, keeps the sub-second part.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn prop_every_epoch_keeps_the_millis(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        for construction in CONSTRUCTIONS {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                header.created_at_millis(),
                ms.subsec_millis(),
                "{:?} dropped the sub-second part at epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// `Header::new` and the builder, with the epoch set before or after the timestamp, produce
    /// the same header for the same inputs, so the gate is applied identically on every path.
    #[test]
    fn prop_construction_paths_agree(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        let reference = build(Construction::New, epoch, ms, &fields);
        for construction in [Construction::BuilderEpochFirst, Construction::BuilderEpochLast] {
            let header = build(construction, epoch, ms, &fields);
            prop_assert_eq!(
                header.digest(),
                reference.digest(),
                "{:?} disagrees with Header::new at epoch {} for {}",
                construction, epoch, ms
            );
            // the digest leaves out a dormant sub-second part, so compare it directly too
            prop_assert_eq!(
                header.created_at_millis(),
                reference.created_at_millis(),
                "{:?} disagrees with Header::new at epoch {} for {}",
                construction, epoch, ms
            );
        }
    }

    /// The digest, which votes and certificates sign, binds `created_at_millis` exactly when the
    /// gate holds: two headers differing only in their sub-second input have distinct digests
    /// at an active epoch and the same digest at a dormant one.
    #[test]
    fn prop_digest_binds_millis_only_when_active(
        epoch in epoch_strategy(),
        secs in secs_strategy(),
        (first_millis, second_millis) in (0..=999u16, 1..=999u16)
            .prop_map(|(millis, offset)| (millis, (millis + offset) % 1000)),
        fields in other_fields_strategy(),
    ) {
        let first_ms = TimestampMs::from_parts(secs, first_millis);
        let second_ms = TimestampMs::from_parts(secs, second_millis);
        let first = build(Construction::New, epoch, first_ms, &fields);
        let second = build(Construction::New, epoch, second_ms, &fields);
        if subsecond_timestamp_active(epoch) {
            prop_assert_ne!(
                first.digest(),
                second.digest(),
                "millis {} and {} share a digest at active epoch {}",
                first_millis, second_millis, epoch
            );
        } else {
            prop_assert_eq!(
                first.digest(),
                second.digest(),
                "millis {} and {} differ in digest at dormant epoch {}",
                first_millis, second_millis, epoch
            );
        }
    }
}

// --- serde: property 4 ---

proptest! {
    /// Property 4: a bcs round trip reproduces the header exactly (same digest, same bytes on
    /// re-encode, same timestamp fields) at every epoch the gate allows, and the decoded
    /// `created_at_millis` is below 1000.
    #[test]
    fn prop_bcs_round_trip_is_identity(
        epoch in epoch_strategy(),
        ms in timestamp_strategy(),
        fields in other_fields_strategy(),
    ) {
        let header = build(Construction::New, epoch, ms, &fields);
        let bytes = encode(&header);
        let decoded: Header = try_decode(&bytes).map_err(|err| {
            TestCaseError::fail(format!("epoch {epoch} for {ms} failed to decode: {err}"))
        })?;
        prop_assert_eq!(decoded.digest(), header.digest(), "epoch {} for {}", epoch, ms);
        prop_assert_eq!(encode(&decoded), bytes, "epoch {} for {}", epoch, ms);
        prop_assert_eq!(decoded.epoch(), epoch);
        prop_assert_eq!(decoded.created_at(), header.created_at(), "epoch {} for {}", epoch, ms);
        prop_assert_eq!(
            decoded.created_at_millis(),
            header.created_at_millis(),
            "epoch {} for {}",
            epoch, ms
        );
        prop_assert_eq!(
            decoded.created_at_ms(),
            header.created_at_ms(),
            "epoch {} for {}",
            epoch, ms
        );
        prop_assert!(
            decoded.created_at_millis() < 1000,
            "decoded millis {} at epoch {} for {}",
            decoded.created_at_millis(), epoch, ms
        );
    }

    /// Property 4, wire side: at an active epoch, bytes carrying an arbitrary `created_at_millis`
    /// decode only when the value is below 1000, and then keep it exactly with the seconds
    /// untouched; every larger value is rejected instead of carried into the seconds.
    #[test]
    fn prop_decoded_millis_is_always_below_1000(
        epoch in active_epoch_strategy(),
        secs in secs_strategy(),
        encoded_millis in millis_strategy(),
        wire_millis in wire_millis_strategy(),
        fields in other_fields_strategy(),
    ) {
        prop_assert!(subsecond_timestamp_active(epoch), "epoch {} must be active", epoch);
        let header =
            build(Construction::New, epoch, TimestampMs::from_parts(secs, encoded_millis), &fields);
        let mut bytes = encode(&header);
        // `created_at_millis` is the final wire field and bcs writes a u16 as two little-endian
        // bytes, so the last two bytes are exactly that field
        let millis_at = bytes.len() - 2;
        prop_assert_eq!(
            &bytes[millis_at..],
            &encoded_millis.to_le_bytes()[..],
            "created_at_millis is not the trailing u16 at epoch {}",
            epoch
        );
        bytes[millis_at..].copy_from_slice(&wire_millis.to_le_bytes());
        match try_decode::<Header>(&bytes) {
            Ok(decoded) => {
                prop_assert!(
                    wire_millis < 1000,
                    "wire millis {} decoded at epoch {}",
                    wire_millis, epoch
                );
                prop_assert_eq!(decoded.created_at_millis(), wire_millis);
                prop_assert_eq!(*decoded.created_at(), secs);
            }
            Err(err) => prop_assert!(
                wire_millis >= 1000,
                "wire millis {} rejected at epoch {}: {}",
                wire_millis, epoch, err
            ),
        }
    }
}

// --- builder: property 5 ---

proptest! {
    /// Property 5: between `HeaderBuilder::created_at` and `created_at_ms`, the call made last
    /// wins whatever came before: seconds last yields a sub-second part of 0, milliseconds last
    /// yields its seconds and (subject to the gate) its sub-second part.
    #[test]
    fn prop_builder_last_timestamp_setter_wins(
        epoch in epoch_strategy(),
        earlier in prop::collection::vec(setter_strategy(), 0..5),
        last in setter_strategy(),
        epoch_set_last in any::<bool>(),
    ) {
        let builder = if epoch_set_last {
            HeaderBuilder::default()
        } else {
            HeaderBuilder::default().epoch(epoch)
        };
        let builder = earlier.iter().chain([&last]).fold(builder, |builder, setter| match *setter {
            Setter::Secs(secs) => builder.created_at(secs),
            Setter::Ms(ms) => builder.created_at_ms(ms),
        });
        let builder = if epoch_set_last { builder.epoch(epoch) } else { builder };
        let header = builder.build();
        let (secs, millis) = match last {
            Setter::Secs(secs) => (secs, 0),
            Setter::Ms(ms) => (ms.secs(), expected_millis(epoch, ms)),
        };
        prop_assert_eq!(*header.created_at(), secs, "last {:?} after {:?}", last, earlier);
        prop_assert_eq!(header.created_at_millis(), millis, "last {:?} after {:?}", last, earlier);
    }
}

/// The epoch anchors the strategies rely on sit on the expected side of the gate, so the
/// properties above exercise both the active and the dormant branch on adiri and are not
/// vacuous.
#[test]
fn test_strategy_anchors_straddle_the_gate() {
    #[cfg(feature = "adiri")]
    {
        for epoch in [0, SEED_SIGNATURE_FORK_EPOCH - 1, SEED_SIGNATURE_FORK_EPOCH] {
            assert!(!subsecond_timestamp_active(epoch), "epoch {epoch} must be dormant");
        }
        assert!(
            !subsecond_timestamp_active(SUBSECOND_TIMESTAMP_FORK_EPOCH - 1),
            "the epoch before the sub-second fork must be dormant"
        );
        assert!(
            subsecond_timestamp_active(SUBSECOND_TIMESTAMP_FORK_EPOCH),
            "the sub-second fork epoch must be active"
        );
    }
    #[cfg(not(feature = "adiri"))]
    {
        for epoch in [0, 1, Epoch::MAX] {
            assert!(subsecond_timestamp_active(epoch), "epoch {epoch} must be active");
        }
    }
}

/// A creation time whose `from_parts` construction saturated is recorded without wrapping: the
/// header keeps `u64::MAX` split into its floored seconds and remainder, and reproduces it.
#[test]
fn test_saturated_creation_time_is_recorded_without_wrapping() {
    let saturated = TimestampMs::from_parts(u64::MAX, 999);
    assert_eq!(saturated.as_millis(), u64::MAX, "from_parts must saturate");
    let fields = OtherFields {
        author: AuthorityIdentifier::default(),
        round: 1,
        payload: IndexMap::new(),
        parents: BTreeSet::new(),
        latest_execution_block: BlockNumHash::default(),
    };
    for construction in CONSTRUCTIONS {
        let header = build(construction, active_epoch(), saturated, &fields);
        assert_eq!(*header.created_at(), u64::MAX / 1000, "{construction:?} seconds");
        assert_eq!(header.created_at_millis(), 615, "{construction:?} sub-second part");
        assert_eq!(header.created_at_ms(), saturated, "{construction:?} combined timestamp");
    }
}

/// Property 3's seconds equality holds only up to `u64::MAX / 1000` seconds: a raw
/// `created_at` above that (reachable through `HeaderBuilder::created_at` or decoded bytes)
/// is kept as-is in `created_at()`, while `created_at_ms()` saturates at `u64::MAX` instead of
/// wrapping.
#[test]
fn test_raw_seconds_beyond_the_millisecond_range_saturate_created_at_ms() {
    for secs in [u64::MAX / 1000 + 1, u64::MAX] {
        let header = HeaderBuilder::default().epoch(active_epoch()).created_at(secs).build();
        assert_eq!(*header.created_at(), secs, "raw seconds must be kept as-is");
        assert_eq!(header.created_at_ms(), TimestampMs::from_millis(u64::MAX));
        let decoded: Header = try_decode(&encode(&header)).expect("raw seconds round trip");
        assert_eq!(*decoded.created_at(), secs, "decode must keep the raw seconds");
    }
}
