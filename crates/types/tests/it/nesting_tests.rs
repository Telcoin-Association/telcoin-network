//! Nesting proof for the epoch-gated `Header` wire layout (#1032, #1086 PR-1).
//!
//! bcs raises no local error when a hand-written visitor disagrees with the serializer about
//! a struct's field count: the parent decoder simply resumes at whatever offset the visitor
//! left it. These tests prove the property the in-band gating plan rests on: a visitor that
//! stops after the seven legacy fields leaves its parent positioned exactly at the next
//! sibling, at depth (headers inside certificates inside one `Vec`). They also prove the
//! negative: bytes whose layout selector (the header's own `epoch`) disagrees with the
//! payload MUST fail to decode rather than resume at a wrong offset and silently succeed.

use std::collections::BTreeSet;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{decode, encode, Certificate, Hash as _};

#[cfg(feature = "adiri")]
use tn_types::try_decode;

/// Wire size of a BLS signature (the 48-byte compressed min-sig point plus its one-byte
/// bcs length prefix): the exact byte count the epoch gate adds to (or strips from) a
/// header, used to prove the two layouts genuinely differ on the wire.
#[cfg(feature = "adiri")]
const BLS_SIGNATURE_WIRE_BYTES: usize = 48 + 1;

/// Byte offset of the little-endian `epoch` field inside an encoded [`Certificate`]: the
/// certificate's first field is its header, whose wire layout starts with the fixed 32-byte
/// author followed by the 4-byte round.
#[cfg(feature = "adiri")]
const CERT_EPOCH_OFFSET: usize = 32 + 4;

/// Build one pre-fork certificate (epoch 0, legacy seven-field layout under `adiri`) and one
/// fork-active certificate (epoch `u32::MAX`, far past the `adiri` fork epoch, so fork-active
/// under both cfgs) over the same fixture committee.
///
/// Both headers come from the same authority with the same payload and parent shape (empty
/// payload, the genesis parents, round 1), so the only wire difference between them besides
/// field values is the epoch-gated eighth field.
#[cfg(feature = "adiri")]
fn mixed_epoch_certificates() -> (Certificate, Certificate) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority = fixture.first_authority();
    let legacy = fixture.certificate(&authority.header(&committee));
    // Re-stamp the seed signature for the fork epoch: the seed message binds `(epoch, round)`.
    let v1_header = authority
        .header_builder(&committee)
        .epoch(u32::MAX)
        .seed_signature(authority.seed_signature(u32::MAX, 1))
        .build();
    let v1 = fixture.certificate(&v1_header);
    (legacy, v1)
}

/// Tier-1 nesting proof (adiri): a `Vec<Certificate>` mixing legacy- and fork-active-epoch
/// elements round-trips element-wise (digests preserved, gate visibility per element) and
/// re-encodes to the original bytes, so the legacy visitor's early stop leaves the vector
/// decoder at the exact start of the next element in both the legacy→V1 and V1→legacy
/// transitions.
#[cfg(feature = "adiri")]
#[test]
fn test_mixed_epoch_certificate_vector_round_trip() {
    let (legacy, v1) = mixed_epoch_certificates();
    // Anti-vacuity: the eighth field is genuinely on the wire for exactly one layout. Same
    // author shape, payload, and parents, so the sizes differ by exactly one BLS signature.
    assert_eq!(
        encode(v1.header()).len(),
        encode(legacy.header()).len() + BLS_SIGNATURE_WIRE_BYTES,
        "fork-active header must carry exactly one extra BLS signature on the wire"
    );

    // Both transition orders in one vector: legacy→V1 and V1→legacy.
    let originals = vec![legacy.clone(), v1, legacy];
    let bytes = encode(&originals);
    let decoded: Vec<Certificate> = decode(&bytes);

    assert_eq!(originals.len(), decoded.len(), "mixed vector lost elements across decode");
    originals.iter().zip(decoded.iter()).for_each(|(original, roundtripped)| {
        // Decode recomputes header digests from the wire bytes, so digest equality proves
        // each element was re-read from exactly its own bytes.
        assert_eq!(
            original.digest(),
            roundtripped.digest(),
            "certificate digest changed across the mixed-vector round trip"
        );
        assert_eq!(
            original, roundtripped,
            "certificate changed across the mixed-vector round trip"
        );
    });

    let gate: Vec<bool> =
        decoded.iter().map(|cert| cert.header().seed_signature().is_some()).collect();
    assert_eq!(
        vec![false, true, false],
        gate,
        "epoch gate must select the layout per element, not per message"
    );

    assert_eq!(
        bytes,
        encode(&decoded),
        "re-encode of the decoded mixed vector must reproduce the original bytes"
    );
}

/// Negative nesting proof (adiri), a permanent keeper: corrupting the fork-active element's
/// `epoch` (the in-band layout selector) to the dormant side of the fork makes the decoder
/// parse that element with the WRONG field count, and the decode must fail loudly: a
/// wrong-offset resume that silently succeeded would let an epoch corruption rewrite the
/// rest of the vector.
#[cfg(feature = "adiri")]
#[test]
fn test_mixed_epoch_vector_epoch_corruption_fails_loudly() {
    let (legacy, v1) = mixed_epoch_certificates();
    let originals = vec![legacy, v1];
    let bytes = encode(&originals);

    // Offset of the V1 element's epoch: one uleb length byte (2 < 128), the legacy
    // certificate's bytes, then the header prefix inside the V1 certificate.
    let legacy_len = encode(originals.first().expect("vector has two elements")).len();
    let epoch_pos = 1 + legacy_len + CERT_EPOCH_OFFSET;

    // Guard: the offset arithmetic must land on the little-endian bytes of epoch u32::MAX.
    let window: Vec<u8> = bytes.iter().skip(epoch_pos).take(4).copied().collect();
    assert_eq!(vec![0xFF; 4], window, "offset arithmetic must land on the V1 epoch field");

    // Rewrite the epoch to the last dormant epoch (`SEED_SIGNATURE_FORK_EPOCH - 1`), the
    // corruption nearest the boundary: the element now claims the legacy layout, so the
    // decoder stops after seven fields and the 49 signature bytes are left misinterpreted as
    // the certificate's tail.
    let dormant_bytes = (tn_types::forks::SEED_SIGNATURE_FORK_EPOCH - 1).to_le_bytes();
    let corrupted: Vec<u8> = bytes
        .iter()
        .enumerate()
        .map(|(position, byte)| {
            position
                .checked_sub(epoch_pos)
                .and_then(|offset| dormant_bytes.get(offset))
                .unwrap_or(byte)
        })
        .copied()
        .collect();
    assert!(
        try_decode::<Vec<Certificate>>(&corrupted).is_err(),
        "epoch-corrupted mixed vector must fail to decode, not resume at a wrong offset"
    );
}

/// Epoch-0 vector round trip, run under EVERY cfg: under `adiri` all elements are legacy
/// (seven-field) headers, elsewhere all are fork-active (eight-field) headers, so the
/// non-adiri suite exercises the same container path with the gate open from genesis.
#[test]
fn test_epoch_zero_certificate_vector_round_trip() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let genesis: BTreeSet<_> = fixture.genesis().collect();
    let (_, headers) = fixture.headers_round(0, &genesis);
    let originals: Vec<Certificate> =
        headers.iter().map(|header| fixture.certificate(header)).collect();

    let bytes = encode(&originals);
    let decoded: Vec<Certificate> = decode(&bytes);

    assert_eq!(originals.len(), decoded.len(), "vector lost elements across decode");
    originals.iter().zip(decoded.iter()).for_each(|(original, roundtripped)| {
        assert_eq!(
            original.digest(),
            roundtripped.digest(),
            "certificate digest changed across the round trip"
        );
    });
    assert_eq!(
        bytes,
        encode(&decoded),
        "re-encode of the decoded vector must reproduce the original bytes"
    );
}
