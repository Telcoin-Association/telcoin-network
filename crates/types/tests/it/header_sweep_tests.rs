//! Deterministic parameter sweep over the epoch-gated `Header` wire layout (#1032).
//!
//! Every combination of (epoch, payload length, parents length) below must round-trip to
//! byte-identical wire bytes and preserve the header digest. The grid is exhaustive by
//! construction (an iterator product, not sampled), and all field content comes from a
//! per-combination seeded rng (no entropy), so a failure reproduces byte-for-byte.
//!
//! The epoch axis brackets the fork boundary: under `adiri` it covers both sides of
//! `SEED_SIGNATURE_FORK_EPOCH` including its immediate neighbors;
//! without `adiri` every epoch is fork-active, so two representative epochs pin the V1-only
//! behavior in the default-feature suite.

use indexmap::IndexMap;
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use std::collections::BTreeSet;
use tn_types::{
    decode, encode, forks::seed_signature_active, AuthorityIdentifier, BlockHash, BlockNumHash,
    BlsKeypair, DefaultHashFunction, Epoch, Header, HeaderBuilder, HeaderDigest, Round,
    Signer as _, TimestampSec, WorkerId,
};

/// Wire size of a BLS signature (the 48-byte compressed min-sig point plus its one-byte bcs
/// length prefix): the exact byte count the epoch gate adds to a header, pinned exactly as in
/// `nesting_tests.rs`.
const BLS_SIGNATURE_WIRE_BYTES: usize = 48 + 1;

/// Independently-encoded legacy (seven-field) mirror of one grid point's header content: the
/// same fields in `HeaderInner`'s declaration order, the payload in its `serde_seq` wire
/// shape, and NO `seed_signature` field — encoded through this derive, never through
/// `Header`'s hand-written epoch-gated serializer.
///
/// Its encoded length is the sweep's byte-level baseline (modeled on the `HeaderRepr` mirrors
/// in `header.rs`'s test module): asserting `encoded_header_len == mirror_len + gate·SIG`
/// pins the gate to the actual wire, where the round-trip and accessor checks alone are
/// tautologies of the same gate and cannot see a serializer that unconditionally includes
/// (or strips) the field.
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

/// Epochs swept under `adiri`: the legacy floor, both sides of the fork boundary (the last
/// legacy epoch and the first fork-active one), and the top of the epoch range.
#[cfg(feature = "adiri")]
const SWEEP_EPOCHS: [Epoch; 4] = [
    0,
    tn_types::forks::SEED_SIGNATURE_FORK_EPOCH - 1,
    tn_types::forks::SEED_SIGNATURE_FORK_EPOCH,
    Epoch::MAX,
];

/// Epochs swept without `adiri`: every epoch is fork-active, so genesis plus one later epoch
/// pin the always-V1 layout in the default-feature suite.
#[cfg(not(feature = "adiri"))]
const SWEEP_EPOCHS: [Epoch; 2] = [0, 5];

/// Collection sizes swept for both the payload map and the parents set: the empty edge, a
/// single element, and several elements.
const SWEEP_LENS: [usize; 3] = [0, 1, 3];

/// Deterministic per-combination rng seed, so every grid point generates stable content
/// independent of iteration order.
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
/// [`seed_signature_active`] for the header's own epoch, and a byte-level wire-length oracle
/// against the independently-encoded [`LegacyHeaderMirror`] baseline.
fn assert_header_round_trip(epoch: Epoch, payload_len: usize, parents_len: usize) {
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
    let created_at: TimestampSec = rng.random();
    let latest_execution_block =
        BlockNumHash::new(rng.random(), BlockHash::from(rng.random::<[u8; 32]>()));

    let header = HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(epoch)
        .created_at(created_at)
        .payload(payload.clone())
        .parents(parents.clone())
        .latest_execution_block(latest_execution_block)
        .seed_signature(seed_signature)
        .build();

    let bytes = encode(&header);

    // Byte-level oracle, independent of both serde gates AND the accessor's gate: the wire
    // must be exactly the legacy seven-field baseline plus one BLS signature when (and only
    // when) the fork is active for this epoch. Holds in both cfg lanes: without `adiri`
    // every epoch is active, so the expectation is baseline + signature everywhere.
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
    let expected_len =
        baseline_len + if seed_signature_active(epoch) { BLS_SIGNATURE_WIRE_BYTES } else { 0 };
    assert_eq!(
        expected_len,
        bytes.len(),
        "grid point ({epoch}, {payload_len}, {parents_len}) wire length must equal the \
         legacy baseline plus exactly the epoch-gated signature bytes"
    );
    let decoded: Header = decode(&bytes);
    assert_eq!(
        bytes,
        encode(&decoded),
        "grid point ({epoch}, {payload_len}, {parents_len}) must round-trip byte-exactly"
    );
    assert_eq!(
        header.digest(),
        decoded.digest(),
        "grid point ({epoch}, {payload_len}, {parents_len}) must preserve the digest"
    );
    assert_eq!(
        header.digest(),
        wire_digest(&bytes),
        "grid point ({epoch}, {payload_len}, {parents_len}) digest preimage must be the wire bytes"
    );
    assert_eq!(
        seed_signature_active(epoch),
        decoded.seed_signature().is_some(),
        "grid point ({epoch}, {payload_len}, {parents_len}) gate visibility must follow the epoch"
    );
}

/// The exhaustive grid: every `(epoch, payload_len, parents_len)` combination of the sweep
/// axes above, via an iterator product (no sampling, no loop keywords).
#[test]
fn test_header_layout_parameter_sweep() {
    SWEEP_EPOCHS.iter().for_each(|&epoch| {
        SWEEP_LENS.iter().for_each(|&payload_len| {
            SWEEP_LENS
                .iter()
                .for_each(|&parents_len| assert_header_round_trip(epoch, payload_len, parents_len));
        });
    });
}
