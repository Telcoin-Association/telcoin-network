//! Nesting proof for the epoch-gated `Header` wire layouts (#1032, #1086 PR-1).
//!
//! bcs raises no local error when a hand-written visitor disagrees with the serializer about
//! a struct's field count: the parent decoder simply resumes at whatever offset the visitor
//! left it. These tests prove the property the in-band gating plan rests on: a visitor that
//! stops after the seven legacy fields, or after the eight seed-signature fields, leaves its
//! parent positioned exactly at the next sibling, at depth (headers inside certificates inside
//! one `Vec`). They also prove the negative: bytes whose layout selector (the header's own
//! `epoch`) disagrees with the payload MUST fail to decode rather than resume at a wrong offset
//! and silently succeed.

use std::collections::BTreeSet;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{decode, encode, Certificate, Hash as _};

#[cfg(feature = "adiri")]
use tn_types::{
    forks::{SEED_SIGNATURE_FORK_EPOCH, SUBSECOND_TIMESTAMP_FORK_EPOCH},
    try_decode, Epoch, TimestampMs,
};

/// Wire size of a BLS signature (the 48-byte compressed min-sig point plus its one-byte
/// bcs length prefix): the exact byte count the epoch gate adds to (or strips from) a
/// header, used to prove the two layouts genuinely differ on the wire.
#[cfg(feature = "adiri")]
const BLS_SIGNATURE_WIRE_BYTES: usize = 48 + 1;

/// Wire size of `created_at_millis` (bcs writes a `u16` as two fixed little-endian bytes): the
/// exact byte count the sub-second gate adds to a header, used to prove the eight- and
/// nine-field layouts genuinely differ on the wire.
#[cfg(feature = "adiri")]
const MILLIS_WIRE_BYTES: usize = 2;

/// Byte offset of the little-endian `epoch` field inside an encoded [`Certificate`]: the
/// certificate's first field is its header, whose wire layout starts with the fixed 32-byte
/// author followed by the 4-byte round.
#[cfg(feature = "adiri")]
const CERT_EPOCH_OFFSET: usize = 32 + 4;

/// Build one pre-fork certificate (epoch 0, legacy seven-field layout under `adiri`) and one
/// seed-signature certificate (epoch `SEED_SIGNATURE_FORK_EPOCH`, the eight-field layout) over
/// the same fixture committee.
///
/// `SEED_SIGNATURE_FORK_EPOCH` is the first epoch with the eighth field. The sub-second fork
/// can only be armed above the live adiri chain, which is already past it, so unless a
/// `TN_SUBSECOND_TIMESTAMP_FORK_EPOCH` override pins the fork at or below it, this header keeps
/// the eight-field layout. Both headers come from the same authority with the same payload and
/// parent shape (empty payload, the genesis parents, round 1), so the only wire difference
/// between them besides field values is the epoch-gated eighth field.
#[cfg(feature = "adiri")]
fn mixed_epoch_certificates() -> (Certificate, Certificate) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority = fixture.first_authority();
    let legacy = fixture.certificate(&authority.header(&committee));
    // Re-stamp the seed signature for the fork epoch: the seed message binds `(epoch, round)`.
    let v1_header = authority
        .header_builder(&committee)
        .epoch(SEED_SIGNATURE_FORK_EPOCH)
        .seed_signature(authority.seed_signature(SEED_SIGNATURE_FORK_EPOCH, 1))
        .build();
    let v1 = fixture.certificate(&v1_header);
    (legacy, v1)
}

/// Build one seed-signature certificate (epoch `SUBSECOND_TIMESTAMP_FORK_EPOCH - 1`, the last
/// eight-field epoch) and one sub-second certificate (epoch `SUBSECOND_TIMESTAMP_FORK_EPOCH`,
/// the first nine-field epoch) over the same fixture committee, bracketing the build's compiled
/// sub-second fork boundary exactly.
///
/// Both headers come from the same authority with the same payload and parent shape (empty
/// payload, the genesis parents, round 1) and are stamped with the same creation time: a
/// whole-second value plus `millis`. The builder keeps `millis` only on the sub-second side, so
/// the only wire difference between the two besides field values is the epoch-gated ninth
/// field.
#[cfg(feature = "adiri")]
fn subsecond_boundary_certificates(millis: u16) -> (Certificate, Certificate) {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority = fixture.first_authority();
    let created_at = TimestampMs::from_parts(1_700_000_063, millis);
    let [v1, v2] =
        [SUBSECOND_TIMESTAMP_FORK_EPOCH - 1, SUBSECOND_TIMESTAMP_FORK_EPOCH].map(|epoch| {
            // re-stamp the seed signature for each epoch: the seed message binds `(epoch, round)`
            let header = authority
                .header_builder(&committee)
                .epoch(epoch)
                .created_at_ms(created_at)
                .seed_signature(authority.seed_signature(epoch, 1))
                .build();
            fixture.certificate(&header)
        });
    (v1, v2)
}

/// Copy of `bytes` with the little-endian epoch at `epoch_pos` rewritten from `from` to `to`.
///
/// Guards the offset arithmetic first: the four bytes at `epoch_pos` must encode `from`, so a
/// miscomputed offset fails here instead of silently corrupting some other field.
#[cfg(feature = "adiri")]
fn rewrite_epoch(bytes: &[u8], epoch_pos: usize, from: Epoch, to: Epoch) -> Vec<u8> {
    let window: Vec<u8> = bytes.iter().skip(epoch_pos).take(4).copied().collect();
    assert_eq!(
        from.to_le_bytes().to_vec(),
        window,
        "offset arithmetic must land on the element's epoch field"
    );
    let to_bytes = to.to_le_bytes();
    bytes
        .iter()
        .enumerate()
        .map(|(position, byte)| {
            position.checked_sub(epoch_pos).and_then(|offset| to_bytes.get(offset)).unwrap_or(byte)
        })
        .copied()
        .collect()
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

    // Rewrite the epoch to the last dormant epoch (`SEED_SIGNATURE_FORK_EPOCH - 1`), the
    // corruption nearest the boundary: the element now claims the legacy layout, so the
    // decoder stops after seven fields and the 49 signature bytes are left misinterpreted as
    // the certificate's tail.
    let corrupted =
        rewrite_epoch(&bytes, epoch_pos, SEED_SIGNATURE_FORK_EPOCH, SEED_SIGNATURE_FORK_EPOCH - 1);
    assert!(
        try_decode::<Vec<Certificate>>(&corrupted).is_err(),
        "epoch-corrupted mixed vector must fail to decode, not resume at a wrong offset"
    );
}

/// Tier-1 nesting proof across the sub-second fork boundary (adiri): a `Vec<Certificate>`
/// mixing eight-field (V1) and nine-field (V2) elements round-trips element-wise (digests
/// preserved, `created_at_millis` per element) and re-encodes to the original bytes, so the
/// V1 visitor's stop after `seed_signature` leaves the vector decoder at the exact start of
/// the next element in both the V1→V2 and V2→V1 transitions.
#[cfg(feature = "adiri")]
#[test]
fn test_subsecond_boundary_certificate_vector_round_trip() {
    // 999 is the largest valid millisecond part, and both of its little-endian bytes
    // (`0xE7 0x03`) are nonzero, so a two-byte misalignment cannot pass them off as zero fill
    let (v1, v2) = subsecond_boundary_certificates(999);
    // anti-vacuity: the ninth field is genuinely on the wire for exactly one layout. same
    // author shape, payload, parents, and seconds, so the sizes differ by exactly one `u16`
    assert_eq!(
        encode(v2.header()).len(),
        encode(v1.header()).len() + MILLIS_WIRE_BYTES,
        "sub-second header must carry exactly one extra u16 on the wire"
    );

    // both transition orders in one vector: V1→V2 and V2→V1
    let originals = vec![v1.clone(), v2, v1];
    let bytes = encode(&originals);
    let decoded: Vec<Certificate> = decode(&bytes);

    assert_eq!(originals.len(), decoded.len(), "mixed vector lost elements across decode");
    originals.iter().zip(decoded.iter()).for_each(|(original, roundtripped)| {
        // decode recomputes header digests from the wire bytes, so digest equality proves
        // each element was re-read from exactly its own bytes
        assert_eq!(
            original.digest(),
            roundtripped.digest(),
            "certificate digest changed across the V1/V2 round trip"
        );
        assert_eq!(original, roundtripped, "certificate changed across the V1/V2 round trip");
    });

    // both layouts carry the seed signature, so only the ninth field tells them apart
    assert!(
        decoded.iter().all(|cert| cert.header().seed_signature().is_some()),
        "every element on either side of the sub-second fork must carry the seed signature"
    );
    let millis: Vec<u16> = decoded.iter().map(|cert| cert.header().created_at_millis()).collect();
    assert_eq!(
        vec![0, 999, 0],
        millis,
        "sub-second gate must select the layout per element, not per message"
    );

    assert_eq!(
        bytes,
        encode(&decoded),
        "re-encode of the decoded V1/V2 vector must reproduce the original bytes"
    );
}

/// Negative nesting proof across the sub-second fork boundary (adiri): corrupting the V2
/// element's `epoch` to the last V1 epoch makes the decoder stop after eight fields, leaving
/// the two `created_at_millis` bytes to be misread as the start of the certificate's remaining
/// fields. That is the field-count mismatch a binary without the ninth field hits on a V2
/// header, and the decode must fail loudly rather than resume two bytes early and succeed.
///
/// Which certificate field rejects the shifted bytes depends on their value, so the proof runs
/// for zero (both stranded bytes zero), the smallest nonzero, and the largest valid millisecond
/// part.
#[cfg(feature = "adiri")]
#[test]
fn test_subsecond_boundary_vector_epoch_corruption_fails_loudly() {
    [0, 1, 999].into_iter().for_each(|millis| {
        let (v1, v2) = subsecond_boundary_certificates(millis);
        let originals = vec![v1, v2];
        let bytes = encode(&originals);

        // offset of the V2 element's epoch: one uleb length byte (2 < 128), the V1
        // certificate's bytes, then the header prefix inside the V2 certificate
        let v1_len = encode(originals.first().expect("vector has two elements")).len();
        let epoch_pos = 1 + v1_len + CERT_EPOCH_OFFSET;

        let corrupted = rewrite_epoch(
            &bytes,
            epoch_pos,
            SUBSECOND_TIMESTAMP_FORK_EPOCH,
            SUBSECOND_TIMESTAMP_FORK_EPOCH - 1,
        );
        assert!(
            try_decode::<Vec<Certificate>>(&corrupted).is_err(),
            "epoch-corrupted V1/V2 vector (millis {millis}) must fail to decode, not resume at \
             a wrong offset"
        );
    });
}

/// Epoch-0 vector round trip, run under EVERY cfg: under `adiri` all elements are legacy
/// (seven-field) headers, elsewhere all are nine-field headers (both forks active from
/// genesis), so the non-adiri suite exercises the same container path with the gates open
/// from genesis.
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
