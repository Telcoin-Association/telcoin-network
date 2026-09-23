//! Deterministic parameter sweep over the epoch-gated `Header` wire layout (#1032).
//!
//! Every combination of (epoch, payload length, parents length, sub-second millis) below must
//! round-trip to byte-identical wire bytes and preserve the header digest. The grid is
//! exhaustive by construction (an iterator product, not sampled), and all field content comes
//! from a per-combination seeded rng (no entropy), so a failure reproduces byte-for-byte.
//!
//! The epoch axis brackets the seed-signature fork boundary and reaches every header layout the
//! build can produce: under `adiri` it covers both sides of `SEED_SIGNATURE_FORK_EPOCH`
//! including its immediate neighbors (seven and eight fields) plus the top of the epoch range
//! (nine fields, sub-second active); without `adiri` both forks are active from genesis, so two
//! representative epochs pin the V2-only behavior in the default-feature suite.

use indexmap::IndexMap;
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use std::collections::BTreeSet;
use tn_types::{
    decode, encode,
    forks::{seed_signature_active, subsecond_timestamp_active},
    AuthorityIdentifier, BlockHash, BlockNumHash, BlsKeypair, DefaultHashFunction, Epoch, Header,
    HeaderBuilder, HeaderDigest, Round, Signer as _, TimestampMs, TimestampSec, WorkerId,
};

/// Wire size of a BLS signature (the 48-byte compressed min-sig point plus its one-byte bcs
/// length prefix): the exact byte count the seed-signature gate adds to a header, pinned
/// exactly as in `nesting_tests.rs`.
const BLS_SIGNATURE_WIRE_BYTES: usize = 48 + 1;

/// Wire size of `created_at_millis`: bcs writes a `u16` as two fixed little-endian bytes with no
/// length prefix, so this is the exact byte count the sub-second gate adds to a header.
const MILLIS_WIRE_BYTES: usize = 2;

/// Independently-encoded legacy (seven-field) mirror of one grid point's header content: the
/// same fields in `HeaderInner`'s declaration order, the payload in its `serde_seq` wire
/// shape, and NO `seed_signature` or `created_at_millis` field — encoded through this derive,
/// never through `Header`'s hand-written epoch-gated serializer.
///
/// Its encoded length is the sweep's byte-level baseline (modeled on the `HeaderRepr` mirrors
/// in `header.rs`'s test module): asserting `encoded_header_len == mirror_len + seed·SIG +
/// subsecond·MILLIS` pins both gates to the actual wire, where the round-trip and accessor
/// checks alone are tautologies of the same gates and cannot see a serializer that
/// unconditionally includes (or strips) either field.
#[derive(serde::Serialize)]
struct LegacyHeaderMirror {
    /// Mirrors `HeaderInner::author` (fixed 32 wire bytes).
    author: AuthorityIdentifier,
    /// Mirrors `HeaderInner::round`.
    round: Round,
    /// Mirrors `HeaderInner::epoch`.
    epoch: Epoch,
    /// Mirrors `HeaderInner::created_at`.
    created_at: TimestampSec,
    /// Mirrors `HeaderInner::payload` in the same `serde_seq` shape the header wire uses.
    #[serde(with = "indexmap::map::serde_seq")]
    payload: IndexMap<BlockHash, WorkerId>,
    /// Mirrors `HeaderInner::parents`.
    parents: BTreeSet<HeaderDigest>,
    /// Mirrors `HeaderInner::latest_execution_block`.
    latest_execution_block: BlockNumHash,
}

/// The sweep grid needs four distinct epochs. `SEED_SIGNATURE_FORK_EPOCH - 1` below already
/// turns a fork epoch of 0 into a const-eval error; this guard also makes a fork epoch of 1
/// loud, which would otherwise silently collapse the grid to three distinct epochs.
#[cfg(feature = "adiri")]
const _: () = assert!(tn_types::forks::SEED_SIGNATURE_FORK_EPOCH > 1);

/// Epochs swept under `adiri`: the legacy floor, both sides of the seed-signature fork boundary
/// (the last legacy epoch and the first fork-active one), and the top of the epoch range, which
/// is sub-second active on every adiri build because the gate compares `epoch >=` a `u32` fork
/// epoch.
#[cfg(feature = "adiri")]
const SWEEP_EPOCHS: [Epoch; 4] = [
    0,
    tn_types::forks::SEED_SIGNATURE_FORK_EPOCH - 1,
    tn_types::forks::SEED_SIGNATURE_FORK_EPOCH,
    Epoch::MAX,
];

/// Epochs swept without `adiri`: every epoch has both forks active, so genesis plus one later
/// epoch pin the always-V2 layout in the default-feature suite.
#[cfg(not(feature = "adiri"))]
const SWEEP_EPOCHS: [Epoch; 2] = [0, 5];

/// Collection sizes swept for both the payload map and the parents set: the empty edge, a
/// single element, and several elements.
const SWEEP_LENS: [usize; 3] = [0, 1, 3];

/// Sub-second parts swept at every other grid point: zero (which a V2 header still writes), the
/// smallest nonzero value, and the largest valid one.
const SWEEP_MILLIS: [u16; 3] = [0, 1, 999];

/// Exclusive upper bound on the swept whole-second `created_at`. Below it, `secs * 1000 + millis`
/// fits a `u64` for every millis in `0..1000`, so `TimestampMs::from_parts` never saturates and
/// the built header keeps exactly the drawn seconds and millis.
const SWEEP_SECS_BOUND: TimestampSec = u64::MAX / 1000;

/// Deterministic per-combination rng seed, so every grid point generates stable content
/// independent of iteration order. Independent of the millis axis: grid points that differ
/// only in millis share every other field.
fn combo_seed(epoch: Epoch, payload_len: usize, parents_len: usize) -> u64 {
    let payload = u64::try_from(payload_len).expect("sweep lengths are tiny");
    let parents = u64::try_from(parents_len).expect("sweep lengths are tiny");
    u64::from(epoch)
        .wrapping_mul(1_000_003)
        .wrapping_add(payload.wrapping_mul(31))
        .wrapping_add(parents)
}

/// Blake3 over raw wire bytes: an independent restatement of the digest-preimage ==
/// wire-bytes contract for every grid point.
fn wire_digest(bytes: &[u8]) -> HeaderDigest {
    let mut hasher = DefaultHashFunction::new();
    hasher.update(bytes);
    HeaderDigest::new(hasher.finalize().into())
}

/// Round-trip one grid point: seeded random field content, exact byte round trip, digest
/// preservation, digest == blake3(wire bytes), gate visibility matching
/// [`seed_signature_active`] and [`subsecond_timestamp_active`] for the header's own epoch,
/// `created_at_millis` normalized to 0 whenever the sub-second gate is inactive, and a
/// byte-level wire-length oracle against the independently-encoded [`LegacyHeaderMirror`]
/// baseline.
fn assert_header_round_trip(epoch: Epoch, payload_len: usize, parents_len: usize, millis: u16) {
    let point = format!("grid point ({epoch}, {payload_len}, {parents_len}, {millis})");
    let mut rng = StdRng::seed_from_u64(combo_seed(epoch, payload_len, parents_len));
    let payload: IndexMap<BlockHash, WorkerId> = (0..payload_len)
        .map(|worker| {
            (
                BlockHash::from(rng.random::<[u8; 32]>()),
                WorkerId::try_from(worker).expect("sweep lengths are tiny"),
            )
        })
        .collect();
    let parents: BTreeSet<HeaderDigest> =
        (0..parents_len).map(|_| HeaderDigest::new(rng.random::<[u8; 32]>())).collect();
    // BLS signing is deterministic and the keypair comes from the seeded rng, so the
    // signature bytes are stable per grid point.
    let keypair = BlsKeypair::generate(&mut rng);
    let message = format!("header-sweep-{epoch}-{payload_len}-{parents_len}");
    let seed_signature = keypair.sign(message.as_bytes());
    let author = AuthorityIdentifier::from_bytes(rng.random::<[u8; 32]>());
    let round: Round = rng.random();
    let created_at: TimestampSec = rng.random::<TimestampSec>() % SWEEP_SECS_BOUND;
    let latest_execution_block =
        BlockNumHash::new(rng.random(), BlockHash::from(rng.random::<[u8; 32]>()));

    // the millisecond setter on every point, so the builder's normalization is what keeps
    // the sub-second part off headers whose epoch has the gate inactive
    let header = HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(epoch)
        .created_at_ms(TimestampMs::from_parts(created_at, millis))
        .payload(payload.clone())
        .parents(parents.clone())
        .latest_execution_block(latest_execution_block)
        .seed_signature(seed_signature)
        .build();

    let bytes = encode(&header);
    let seed_active = seed_signature_active(epoch);
    let millis_active = subsecond_timestamp_active(epoch);

    // Byte-level oracle, independent of both serde gates AND the accessors' gates: the wire
    // must be exactly the legacy seven-field baseline, plus one BLS signature when (and only
    // when) the seed-signature fork is active for this epoch, plus one `u16` when (and only
    // when) the sub-second fork is. Holds in both cfg lanes: without `adiri` both forks are
    // active everywhere, so the expectation is baseline + signature + millis everywhere.
    let mirror = LegacyHeaderMirror {
        author,
        round,
        epoch,
        created_at,
        payload,
        parents,
        latest_execution_block,
    };
    let baseline_len = encode(&mirror).len();
    let expected_len = baseline_len
        + usize::from(seed_active) * BLS_SIGNATURE_WIRE_BYTES
        + usize::from(millis_active) * MILLIS_WIRE_BYTES;
    assert_eq!(
        expected_len,
        bytes.len(),
        "{point} wire length must equal the legacy baseline plus exactly the epoch-gated \
         signature and millis bytes"
    );
    // `created_at_millis` is the last header field, so when present it is the wire's tail
    if millis_active {
        assert_eq!(
            Some(&millis.to_le_bytes()),
            bytes.last_chunk::<MILLIS_WIRE_BYTES>(),
            "{point} wire must end in the little-endian created_at_millis"
        );
    }
    let decoded: Header = decode(&bytes);
    assert_eq!(bytes, encode(&decoded), "{point} must round-trip byte-exactly");
    assert_eq!(header.digest(), decoded.digest(), "{point} must preserve the digest");
    assert_eq!(
        header.digest(),
        wire_digest(&bytes),
        "{point} digest preimage must be the wire bytes"
    );
    assert_eq!(
        seed_active,
        decoded.seed_signature().is_some(),
        "{point} seed-signature gate visibility must follow the epoch"
    );
    let expected_millis = if millis_active { millis } else { 0 };
    assert_eq!(
        expected_millis,
        header.created_at_millis(),
        "{point} build must keep created_at_millis only when the sub-second gate is active"
    );
    assert_eq!(
        expected_millis,
        decoded.created_at_millis(),
        "{point} decoded created_at_millis must follow the epoch"
    );
    assert_eq!(created_at, *decoded.created_at(), "{point} whole seconds must survive untouched");
}

/// The exhaustive grid: every `(epoch, payload_len, parents_len, millis)` combination of the
/// sweep axes above, via an iterator product (no sampling, no loop keywords).
#[test]
fn test_header_layout_parameter_sweep() {
    // anti-vacuity: the millis assertions only bite on V2 epochs and the normalization ones only
    // on earlier layouts, so the epoch axis must reach every layout this build can produce
    let layouts: BTreeSet<(bool, bool)> = SWEEP_EPOCHS
        .iter()
        .map(|&epoch| (seed_signature_active(epoch), subsecond_timestamp_active(epoch)))
        .collect();
    let expected_layouts: BTreeSet<(bool, bool)> = if cfg!(feature = "adiri") {
        BTreeSet::from([(false, false), (true, false), (true, true)])
    } else {
        BTreeSet::from([(true, true)])
    };
    assert_eq!(
        expected_layouts, layouts,
        "the epoch axis must cover every (seed-active, subsecond-active) header layout"
    );

    SWEEP_EPOCHS.iter().for_each(|&epoch| {
        SWEEP_LENS.iter().for_each(|&payload_len| {
            SWEEP_LENS.iter().for_each(|&parents_len| {
                SWEEP_MILLIS.iter().for_each(|&millis| {
                    assert_header_round_trip(epoch, payload_len, parents_len, millis)
                });
            });
        });
    });
}
