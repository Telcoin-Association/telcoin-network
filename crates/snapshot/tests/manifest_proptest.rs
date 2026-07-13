//! Property-based tests for the snapshot [`Manifest`] and [`Pointer`] JSON contract.
//!
//! These exercise the crate's public (de)serialization surface exactly as an external consumer
//! would, verifying the invariants every snapshot reader relies on:
//!
//! 1. Round-trip totality: any valid `Manifest`/`Pointer` survives `to_json` -> `from_json`
//!    unchanged, for arbitrary field values (unicode strings, empty or many artifacts).
//! 2. Unknown-field tolerance: extra keys carrying arbitrary (possibly nested) JSON values,
//!    injected at the top level and inside every nested object (`counts`, each `artifacts` entry),
//!    are ignored on parse.
//! 3. Version/kind gate totality: any `format_version` other than [`FORMAT_VERSION`] is rejected
//!    (both documents), and any `kind` other than [`KIND_STATE_ONLY`] is rejected for a `Manifest`
//!    -- always as a `Manifest` error, never silently accepted.
//! 4. Serialization injectivity: changing any single scalar field changes the JSON bytes, so no
//!    scalar field is dropped from the serialized form.
//! 5. `Pointer::matches_chain(c)` is true exactly when the pointer's `chain_id` equals `c`.
//!
//! Strategies build every 32-byte hash/digest from arbitrary `[u8; 32]` and pin `format_version`
//! and `kind` to their only accepted values for the "valid document" generators, so no committee,
//! filesystem, or crypto fixture is needed.

// This standalone integration-test binary intentionally uses only a subset of the crate's
// dev-dependencies; mirror the crate lib's allow so `unused_crate_dependencies` (a CI-hard
// warning) does not fire on the ones the sibling integration tests use.
#![allow(unused_crate_dependencies)]

use proptest::prelude::*;
use serde_json::{json, Value};
use tn_snapshot::{
    manifest::{
        ArtifactEntry, ArtifactKind, Counts, Manifest, Pointer, FORMAT_VERSION, KIND_STATE_ONLY,
    },
    SnapshotError,
};
use tn_types::{BlockNumHash, ConsensusNumHash, EpochDigest, B256};

// ------------------------------------------------------------------------------------------------
// Strategies
// ------------------------------------------------------------------------------------------------

/// Arbitrary 32-byte hash built from arbitrary bytes.
fn arb_b256() -> impl Strategy<Value = B256> {
    prop::array::uniform32(any::<u8>()).prop_map(B256::from)
}

/// Arbitrary lowercase-hex sha256 string (64 chars). The value is opaque to validation, but a
/// realistic shape keeps the documents readable when a case shrinks.
fn arb_sha256_hex() -> impl Strategy<Value = String> {
    prop::string::string_regex("[0-9a-f]{64}").expect("static regex is valid")
}

/// Arbitrary artifact category.
fn arb_artifact_kind() -> impl Strategy<Value = ArtifactKind> {
    prop_oneof![
        Just(ArtifactKind::StateChunk),
        Just(ArtifactKind::Headers),
        Just(ArtifactKind::EpochRecords),
    ]
}

/// Arbitrary artifact entry with a separator-free name.
///
/// `Manifest::validate` requires each artifact name to be a single path component, so the name
/// strategy is drawn from a separator-free, non-empty character set. `arb_manifest` additionally
/// prefixes each name with its index, which makes names unique and rules out the `.`/`..` specials.
fn arb_artifact_entry() -> impl Strategy<Value = ArtifactEntry> {
    let name = prop::string::string_regex("[a-zA-Z0-9._-]{1,16}").expect("static regex is valid");
    (name, arb_artifact_kind(), any::<u64>(), arb_sha256_hex())
        .prop_map(|(name, kind, size, sha256)| ArtifactEntry { name, kind, size, sha256 })
}

/// Arbitrary aggregate counts.
fn arb_counts() -> impl Strategy<Value = Counts> {
    (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
        |(accounts, storage_slots, bytecodes, headers, records)| Counts {
            accounts,
            storage_slots,
            bytecodes,
            headers,
            records,
        },
    )
}

/// Arbitrary execution checkpoint.
fn arb_block_num_hash() -> impl Strategy<Value = BlockNumHash> {
    (any::<u64>(), arb_b256()).prop_map(|(number, hash)| BlockNumHash::new(number, hash))
}

/// Arbitrary consensus checkpoint. The digest routes through `B256` so it, too, comes from
/// arbitrary bytes.
fn arb_consensus_num_hash() -> impl Strategy<Value = ConsensusNumHash> {
    (any::<u64>(), arb_b256()).prop_map(|(number, hash)| ConsensusNumHash::new(number, hash.into()))
}

/// Arbitrary *valid* manifest: `format_version` and `kind` are pinned to the only accepted values;
/// every other field ranges freely, including empty or many artifacts.
fn arb_manifest() -> impl Strategy<Value = Manifest> {
    let identity = (any::<u64>(), arb_b256(), any::<u32>());
    let checkpoints =
        (arb_block_num_hash(), arb_consensus_num_hash(), arb_b256().prop_map(EpochDigest::from));
    let meta = (
        any::<u64>(),
        any::<String>(),
        prop::collection::vec(arb_artifact_entry(), 0..8),
        arb_counts(),
        any::<bool>(),
    );
    (identity, checkpoints, meta).prop_map(
        |(
            (chain_id, genesis_hash, epoch),
            (final_state, final_consensus, epoch_record_digest),
            (created_at, node_version, artifacts, counts, fee_derivable),
        )| {
            // prefix each name with its index so names are unique and never `.`/`..`; combined with
            // the separator-free base this satisfies Manifest::validate for any generated set.
            let artifacts = artifacts
                .into_iter()
                .enumerate()
                .map(|(i, mut entry)| {
                    entry.name = format!("{i:03}-{}", entry.name);
                    entry
                })
                .collect();
            Manifest {
                format_version: FORMAT_VERSION,
                kind: KIND_STATE_ONLY.to_string(),
                chain_id,
                genesis_hash,
                epoch,
                final_state,
                final_consensus,
                epoch_record_digest,
                created_at,
                node_version,
                artifacts,
                counts,
                fee_derivable,
            }
        },
    )
}

/// Arbitrary *valid* pointer: `format_version` pinned, everything else free.
fn arb_pointer() -> impl Strategy<Value = Pointer> {
    (any::<u64>(), any::<u32>(), any::<String>(), arb_sha256_hex(), any::<u64>()).prop_map(
        |(chain_id, epoch, manifest_key, manifest_sha256, updated_at)| Pointer {
            format_version: FORMAT_VERSION,
            chain_id,
            epoch,
            manifest_key,
            manifest_sha256,
            updated_at,
        },
    )
}

/// Arbitrary `format_version` that is *not* the accepted [`FORMAT_VERSION`]. Covers the whole
/// `u32` domain except the single valid value.
fn arb_bad_format_version() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), (FORMAT_VERSION + 1)..=u32::MAX]
}

/// Arbitrary `kind` string that is *not* [`KIND_STATE_ONLY`]. The lone colliding case is nudged
/// off the accepted value rather than filtered, so no test budget is wasted.
fn arb_bad_kind() -> impl Strategy<Value = String> {
    any::<String>().prop_map(|s| if s == KIND_STATE_ONLY { format!("{s}-invalid") } else { s })
}

/// Arbitrary JSON value, including nested arrays and objects, for unknown-field injection.
/// Numbers are drawn from `i64` to avoid non-representable float edge cases.
fn arb_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<String>().prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 12, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::hash_map(any::<String>(), inner, 0..4)
                .prop_map(|map| Value::Object(map.into_iter().collect())),
        ]
    })
}

// ------------------------------------------------------------------------------------------------
// Property 1: round-trip totality
// ------------------------------------------------------------------------------------------------

proptest! {
    /// Any valid manifest survives `to_json` -> `from_json` unchanged.
    #[test]
    fn prop_manifest_round_trips(manifest in arb_manifest()) {
        let bytes = manifest.to_json().expect("valid manifest serializes");
        let parsed = Manifest::from_json(&bytes).expect("serialized manifest parses");
        prop_assert_eq!(parsed, manifest);
    }

    /// Any valid pointer survives `to_json` -> `from_json` unchanged.
    #[test]
    fn prop_pointer_round_trips(pointer in arb_pointer()) {
        let bytes = pointer.to_json().expect("valid pointer serializes");
        let parsed = Pointer::from_json(&bytes).expect("serialized pointer parses");
        prop_assert_eq!(parsed, pointer);
    }
}

// ------------------------------------------------------------------------------------------------
// Property 2: unknown-field tolerance
// ------------------------------------------------------------------------------------------------

/// Insert each `(key, value)` into `obj` under a collision-proof `__extra_<key>` name, so an
/// injected key can never shadow a real field (which would change the parsed value).
fn inject_extras(obj: &mut serde_json::Map<String, Value>, extras: &[(String, Value)]) {
    for (k, v) in extras {
        obj.insert(format!("__extra_{k}"), v.clone());
    }
}

proptest! {
    /// Unknown fields -- arbitrary, possibly nested JSON values -- injected at the top level and
    /// inside every nested object (`counts`, each `artifacts` entry) are ignored on parse.
    #[test]
    fn prop_manifest_tolerates_unknown_fields(
        manifest in arb_manifest(),
        extras in prop::collection::vec((any::<String>(), arb_json_value()), 1..4),
    ) {
        let mut value = serde_json::to_value(&manifest).expect("manifest serializes to value");

        // nested: the counts object
        if let Some(obj) = value.get_mut("counts").and_then(Value::as_object_mut) {
            inject_extras(obj, &extras);
        }
        // nested: every artifact entry object
        if let Some(entries) = value.get_mut("artifacts").and_then(Value::as_array_mut) {
            for entry in entries.iter_mut() {
                if let Some(obj) = entry.as_object_mut() {
                    inject_extras(obj, &extras);
                }
            }
        }
        // top level
        if let Some(obj) = value.as_object_mut() {
            inject_extras(obj, &extras);
        }

        let bytes = serde_json::to_vec(&value).expect("augmented value serializes");
        let parsed = Manifest::from_json(&bytes).expect("manifest with unknown fields parses");
        prop_assert_eq!(parsed, manifest);
    }

    /// Unknown top-level fields are ignored when parsing a pointer.
    #[test]
    fn prop_pointer_tolerates_unknown_fields(
        pointer in arb_pointer(),
        extras in prop::collection::vec((any::<String>(), arb_json_value()), 1..4),
    ) {
        let mut value = serde_json::to_value(&pointer).expect("pointer serializes to value");
        if let Some(obj) = value.as_object_mut() {
            inject_extras(obj, &extras);
        }
        let bytes = serde_json::to_vec(&value).expect("augmented value serializes");
        let parsed = Pointer::from_json(&bytes).expect("pointer with unknown fields parses");
        prop_assert_eq!(parsed, pointer);
    }
}

// ------------------------------------------------------------------------------------------------
// Property 3: version / kind gate totality
// ------------------------------------------------------------------------------------------------

proptest! {
    /// Any `format_version` other than the accepted one makes `Manifest::from_json` fail with a
    /// `Manifest` error -- never silently accepted, never a different variant.
    #[test]
    fn prop_manifest_rejects_bad_format_version(
        manifest in arb_manifest(),
        bad in arb_bad_format_version(),
    ) {
        let mut value = serde_json::to_value(&manifest).expect("manifest serializes to value");
        value["format_version"] = json!(bad);
        let bytes = serde_json::to_vec(&value).expect("value serializes");
        let result = Manifest::from_json(&bytes);
        prop_assert!(
            matches!(&result, Err(SnapshotError::Manifest(_))),
            "format_version {bad} must be rejected, got {result:?}"
        );
    }

    /// Any `kind` other than `state-only` makes `Manifest::from_json` fail with a `Manifest` error.
    #[test]
    fn prop_manifest_rejects_bad_kind(manifest in arb_manifest(), bad in arb_bad_kind()) {
        let mut value = serde_json::to_value(&manifest).expect("manifest serializes to value");
        value["kind"] = json!(bad);
        let bytes = serde_json::to_vec(&value).expect("value serializes");
        let result = Manifest::from_json(&bytes);
        prop_assert!(
            matches!(&result, Err(SnapshotError::Manifest(_))),
            "kind {bad:?} must be rejected, got {result:?}"
        );
    }

    /// Any `format_version` other than the accepted one makes `Pointer::from_json` fail with a
    /// `Manifest` error.
    #[test]
    fn prop_pointer_rejects_bad_format_version(
        pointer in arb_pointer(),
        bad in arb_bad_format_version(),
    ) {
        let mut value = serde_json::to_value(&pointer).expect("pointer serializes to value");
        value["format_version"] = json!(bad);
        let bytes = serde_json::to_vec(&value).expect("value serializes");
        let result = Pointer::from_json(&bytes);
        prop_assert!(
            matches!(&result, Err(SnapshotError::Manifest(_))),
            "pointer format_version {bad} must be rejected, got {result:?}"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// Property 4: serialization injectivity (single-field mutation)
// ------------------------------------------------------------------------------------------------

/// Return a clone of `manifest` with exactly one scalar field changed to a guaranteed-different
/// value. `which` selects the field; each arm's mutation is total (never a no-op) regardless of
/// the original value.
fn mutate_scalar(manifest: &Manifest, which: usize) -> Manifest {
    let mut m = manifest.clone();
    match which {
        0 => m.chain_id = m.chain_id.wrapping_add(1),
        1 => m.created_at = m.created_at.wrapping_add(1),
        2 => m.epoch = m.epoch.wrapping_add(1),
        3 => m.fee_derivable = !m.fee_derivable,
        4 => m.node_version.push('x'),
        5 => {
            let mut bytes = m.genesis_hash.0;
            bytes[0] ^= 1;
            m.genesis_hash = B256::from(bytes);
        }
        6 => {
            m.final_state =
                BlockNumHash::new(m.final_state.number.wrapping_add(1), m.final_state.hash);
        }
        7 => m.counts.accounts = m.counts.accounts.wrapping_add(1),
        _ => unreachable!("field selector out of range"),
    }
    m
}

proptest! {
    /// Changing any single scalar field yields both a distinct value and distinct JSON bytes:
    /// every scalar field is faithfully present in the serialized form.
    #[test]
    fn prop_scalar_field_change_changes_bytes(manifest in arb_manifest(), which in 0usize..8) {
        let mutated = mutate_scalar(&manifest, which);
        prop_assert_ne!(&manifest, &mutated);

        let original = manifest.to_json().expect("valid manifest serializes");
        let changed = mutated.to_json().expect("valid mutated manifest serializes");
        prop_assert!(
            original != changed,
            "mutating scalar field #{which} left the JSON bytes unchanged"
        );
    }
}

// ------------------------------------------------------------------------------------------------
// Property 5: matches_chain
// ------------------------------------------------------------------------------------------------

proptest! {
    /// `matches_chain(c)` is true exactly when the pointer's `chain_id` equals `c`. `force_equal`
    /// guarantees the equal branch is exercised, not just near-misses.
    #[test]
    fn prop_matches_chain_iff_equal(
        chain_id in any::<u64>(),
        query in any::<u64>(),
        force_equal in any::<bool>(),
    ) {
        let query = if force_equal { chain_id } else { query };
        let pointer = Pointer {
            format_version: FORMAT_VERSION,
            chain_id,
            epoch: 0,
            manifest_key: String::new(),
            manifest_sha256: String::new(),
            updated_at: 0,
        };
        prop_assert_eq!(pointer.matches_chain(query), chain_id == query);
    }
}

// ------------------------------------------------------------------------------------------------
// Companion deterministic tests (fast regression anchors)
// ------------------------------------------------------------------------------------------------

/// A concrete, minimal valid manifest for the deterministic checks below.
fn sample_manifest() -> Manifest {
    Manifest {
        format_version: FORMAT_VERSION,
        kind: KIND_STATE_ONLY.to_string(),
        chain_id: 2017,
        genesis_hash: B256::from([0x11u8; 32]),
        epoch: 7,
        final_state: BlockNumHash::new(4200, B256::from([0x22u8; 32])),
        final_consensus: ConsensusNumHash::new(99, B256::from([0x33u8; 32]).into()),
        epoch_record_digest: EpochDigest::from(B256::from([0xabu8; 32])),
        created_at: 1_700_000_000,
        node_version: "tn-test/0.1.0".to_string(),
        artifacts: vec![],
        counts: Counts::default(),
        fee_derivable: true,
    }
}

/// A concrete, minimal valid pointer.
fn sample_pointer() -> Pointer {
    Pointer {
        format_version: FORMAT_VERSION,
        chain_id: 2017,
        epoch: 7,
        manifest_key: "epoch-0000000007/manifest.json".to_string(),
        manifest_sha256: "cc".repeat(32),
        updated_at: 1_700_000_500,
    }
}

/// A manifest with no artifacts still round-trips (empty-collection edge case).
#[test]
fn round_trip_empty_artifacts() {
    let manifest = sample_manifest();
    let parsed = Manifest::from_json(&manifest.to_json().unwrap()).unwrap();
    assert_eq!(parsed, manifest);
}

/// A manifest carrying all three artifact kinds round-trips (kebab-case enum coverage).
#[test]
fn round_trip_all_artifact_kinds() {
    let mut manifest = sample_manifest();
    manifest.artifacts = vec![
        ArtifactEntry {
            name: "state-000.jsonl.zst".to_string(),
            kind: ArtifactKind::StateChunk,
            size: 1,
            sha256: "aa".repeat(32),
        },
        ArtifactEntry {
            name: "headers.jsonl.zst".to_string(),
            kind: ArtifactKind::Headers,
            size: 2,
            sha256: "bb".repeat(32),
        },
        ArtifactEntry {
            name: "epochs.jsonl.zst".to_string(),
            kind: ArtifactKind::EpochRecords,
            size: 3,
            sha256: "cc".repeat(32),
        },
    ];
    let parsed = Manifest::from_json(&manifest.to_json().unwrap()).unwrap();
    assert_eq!(parsed, manifest);
}

/// A unicode `node_version` round-trips faithfully.
#[test]
fn round_trip_unicode_node_version() {
    let mut manifest = sample_manifest();
    manifest.node_version = "tñ-node/0.1.0 ✓ 🚀".to_string();
    let parsed = Manifest::from_json(&manifest.to_json().unwrap()).unwrap();
    assert_eq!(parsed, manifest);
}

/// A concrete unknown `format_version` is rejected as a `Manifest` error.
#[test]
fn bad_format_version_rejected() {
    let manifest = sample_manifest();
    let mut value = serde_json::to_value(&manifest).unwrap();
    value["format_version"] = json!(2);
    let err = Manifest::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
}

/// A concrete unknown `kind` is rejected as a `Manifest` error.
#[test]
fn bad_kind_rejected() {
    let manifest = sample_manifest();
    let mut value = serde_json::to_value(&manifest).unwrap();
    value["kind"] = json!("full-history");
    let err = Manifest::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
}

/// `matches_chain` agrees with equality on concrete values.
#[test]
fn matches_chain_exact() {
    let pointer = sample_pointer();
    assert!(pointer.matches_chain(2017));
    assert!(!pointer.matches_chain(2018));
}
