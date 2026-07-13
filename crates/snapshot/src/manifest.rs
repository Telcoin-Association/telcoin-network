//! The snapshot manifest format and its (de)serialization.
//!
//! Two JSON documents describe a snapshot bucket, and both live here:
//!
//! - [`Manifest`] (`manifest.json`, one per `epoch-<N>` directory) fully describes a single
//!   snapshot: the chain/genesis it binds to, the epoch it captures, the final execution and
//!   consensus checkpoints, the list of [`ArtifactEntry`] objects with their integrity digests, and
//!   aggregate [`Counts`].
//! - [`Pointer`] (`latest.json`, at the bucket root) names the newest [`Manifest`] a fresh node
//!   should fetch, pinned by its chain id and a sha256 of the manifest bytes.
//!
//! Every consumer must deserialize through [`Manifest::from_json`] / [`Pointer::from_json`] and
//! serialize through the matching `to_json`. Those helpers run [`Manifest::validate`] /
//! [`Pointer::validate`], which reject an unknown `format_version` or `kind` up front. Unknown
//! *fields* are intentionally tolerated (no `deny_unknown_fields`) so a future snapshot variant
//! can add fields without breaking old readers.

use crate::{SnapshotError, SnapshotResult};
use serde::{Deserialize, Serialize};
use tn_types::{BlockNumHash, ConsensusNumHash, Epoch, EpochDigest, B256};

/// The only manifest and pointer `format_version` this build understands.
///
/// [`Manifest::validate`] and [`Pointer::validate`] reject any other value, so bumping this
/// constant is a deliberate, breaking schema change that older nodes will refuse.
pub const FORMAT_VERSION: u32 = 1;

/// The `kind` discriminator for a state-only snapshot (final EVM state plus the checkpoints
/// needed to resume, without full history).
///
/// It is the only value [`Manifest::validate`] currently accepts; a future full-history variant
/// would use a different string and require a reader that understands it.
pub const KIND_STATE_ONLY: &str = "state-only";

/// Object key of the [`Pointer`] document at the bucket root.
pub const LATEST_KEY: &str = "latest.json";

/// A signed, self-describing manifest for one state-only snapshot.
///
/// A manifest binds the snapshot to a specific chain (`chain_id` + `genesis_hash`) and epoch,
/// records the closing execution and consensus checkpoints, and lists every artifact object with
/// a sha256 so a downloader can verify integrity before use. The [`epoch_record_digest`] ties the
/// snapshot to the signed [`EpochRecord`](tn_types::EpochRecord) chain, which is the trust anchor
/// the verify step walks.
///
/// Construct one directly and serialize with [`Manifest::to_json`]; parse untrusted bytes with
/// [`Manifest::from_json`] so validation always runs.
///
/// [`epoch_record_digest`]: Manifest::epoch_record_digest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version of this document; only [`FORMAT_VERSION`] is accepted by
    /// [`Manifest::validate`].
    pub format_version: u32,
    /// Snapshot variant discriminator; only [`KIND_STATE_ONLY`] is currently valid.
    pub kind: String,
    /// EVM chain id this snapshot was produced for.
    pub chain_id: u64,
    /// Genesis block hash, binding the snapshot to a specific chain genesis.
    pub genesis_hash: B256,
    /// Epoch whose closing state this snapshot captures.
    pub epoch: Epoch,
    /// Block number and hash of the last executed block of [`epoch`](Self::epoch); the execution
    /// checkpoint the restored node resumes from.
    pub final_state: BlockNumHash,
    /// Consensus header number and hash of the last consensus output of [`epoch`](Self::epoch);
    /// the consensus checkpoint that pairs with [`final_state`](Self::final_state).
    pub final_consensus: ConsensusNumHash,
    /// Digest of the [`EpochRecord`](tn_types::EpochRecord) for [`epoch`](Self::epoch), serialized
    /// as a lowercase `0x`-prefixed hex string.
    ///
    /// Unlike the digest type's intrinsic base58 encoding, this field is pinned to hex so every
    /// 32-byte hash in the manifest reads uniformly; see the crate-internal adapter used on it.
    #[serde(with = "epoch_digest_hex")]
    pub epoch_record_digest: EpochDigest,
    /// Unix timestamp (seconds) at which this manifest was produced.
    pub created_at: u64,
    /// Version string of the node that produced the snapshot.
    pub node_version: String,
    /// The data objects that make up this snapshot, each with its own integrity digest.
    pub artifacts: Vec<ArtifactEntry>,
    /// Aggregate item counts across all artifacts, for quick inspection and sanity checks.
    pub counts: Counts,
    /// Whether the captured state is sufficient to derive EIP-1559 base fees after restore.
    pub fee_derivable: bool,
}

impl Manifest {
    /// Parse a manifest from its JSON bytes and validate it.
    ///
    /// This is the only sanctioned way to deserialize a [`Manifest`]: it runs
    /// [`Manifest::validate`] after parsing, so an unknown `format_version` or `kind` is rejected
    /// rather than silently accepted. Unknown fields are tolerated for forward compatibility.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Json`] if the bytes are not valid manifest JSON, or
    /// [`SnapshotError::Manifest`] if the parsed document fails validation.
    pub fn from_json(bytes: &[u8]) -> SnapshotResult<Self> {
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Serialize this manifest to canonical JSON bytes after validating it.
    ///
    /// Validating on the way out guarantees the crate never emits a manifest that
    /// [`Manifest::from_json`] would later reject.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Manifest`] if the manifest is invalid, or [`SnapshotError::Json`]
    /// if serialization fails.
    pub fn to_json(&self) -> SnapshotResult<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// Check the version, kind, and artifact-name invariants that every consumer relies on.
    ///
    /// Every consumer joins an artifact's [`name`](ArtifactEntry::name) onto a local staging
    /// directory to form the path it downloads to, so each name must be a single relative path
    /// component: non-empty, neither `.` nor `..`, and free of `/` or `\` separators. A name
    /// carrying a separator or a parent reference could otherwise escape the staging directory and
    /// write to an arbitrary path. Names must also be unique, because two entries sharing a name
    /// would silently overwrite one another in staging. The exporter only ever emits single, unique
    /// component names, so an honest publisher is unaffected; the check runs on both parse and
    /// serialize so a tampered manifest is rejected before any artifact is fetched.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Manifest`] if `format_version` is not [`FORMAT_VERSION`], `kind` is
    /// not [`KIND_STATE_ONLY`], or any artifact name is not a unique single path component.
    pub fn validate(&self) -> SnapshotResult<()> {
        if self.format_version != FORMAT_VERSION {
            return Err(SnapshotError::Manifest(format!(
                "unsupported manifest format_version {} (expected {FORMAT_VERSION})",
                self.format_version
            )));
        }
        if self.kind != KIND_STATE_ONLY {
            return Err(SnapshotError::Manifest(format!(
                "unsupported manifest kind {:?} (expected {KIND_STATE_ONLY:?})",
                self.kind
            )));
        }
        let mut seen = std::collections::HashSet::with_capacity(self.artifacts.len());
        for artifact in &self.artifacts {
            let name = artifact.name.as_str();
            if name.is_empty()
                || name == "."
                || name == ".."
                || name.contains('/')
                || name.contains('\\')
            {
                return Err(SnapshotError::Manifest(format!(
                    "artifact name {name:?} is not a single relative path component"
                )));
            }
            if !seen.insert(name) {
                return Err(SnapshotError::Manifest(format!(
                    "artifact name {name:?} is listed more than once"
                )));
            }
        }
        Ok(())
    }

    /// Look up an artifact by its [`name`](ArtifactEntry::name).
    ///
    /// Returns `None` if no artifact with that name is present.
    pub fn artifact(&self, name: &str) -> Option<&ArtifactEntry> {
        self.artifacts.iter().find(|entry| entry.name == name)
    }
}

/// The bucket-root pointer to the newest available snapshot.
///
/// A joining node reads `latest.json` first, confirms it targets the right chain with
/// [`Pointer::matches_chain`], then fetches the [`Manifest`] named by
/// [`manifest_key`](Self::manifest_key) and checks it against
/// [`manifest_sha256`](Self::manifest_sha256).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    /// Schema version of this document; only [`FORMAT_VERSION`] is accepted by
    /// [`Pointer::validate`].
    pub format_version: u32,
    /// EVM chain id the referenced snapshot was produced for.
    pub chain_id: u64,
    /// Epoch of the snapshot this pointer currently advertises as latest.
    pub epoch: Epoch,
    /// Opaque object-store key of the referenced [`Manifest`] within the bucket.
    ///
    /// The key layout (zero-padded `epoch-<N>` directories) is owned by the store module; this
    /// field carries whatever key that module produced, unmodified.
    pub manifest_key: String,
    /// Lowercase-hex sha256 of the referenced manifest's serialized bytes.
    pub manifest_sha256: String,
    /// Unix timestamp (seconds) at which this pointer was last updated.
    pub updated_at: u64,
}

impl Pointer {
    /// Parse a pointer from its JSON bytes and validate it.
    ///
    /// Like [`Manifest::from_json`], this runs [`Pointer::validate`] after parsing so an unknown
    /// `format_version` is rejected. Unknown fields are tolerated for forward compatibility.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Json`] if the bytes are not valid pointer JSON, or
    /// [`SnapshotError::Manifest`] if the parsed document fails validation.
    pub fn from_json(bytes: &[u8]) -> SnapshotResult<Self> {
        let pointer: Self = serde_json::from_slice(bytes)?;
        pointer.validate()?;
        Ok(pointer)
    }

    /// Serialize this pointer to canonical JSON bytes after validating it.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Manifest`] if the pointer is invalid, or [`SnapshotError::Json`]
    /// if serialization fails.
    pub fn to_json(&self) -> SnapshotResult<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// Check the version invariant that every consumer relies on.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Manifest`] if `format_version` is not [`FORMAT_VERSION`].
    pub fn validate(&self) -> SnapshotResult<()> {
        if self.format_version != FORMAT_VERSION {
            return Err(SnapshotError::Manifest(format!(
                "unsupported latest-pointer format_version {} (expected {FORMAT_VERSION})",
                self.format_version
            )));
        }
        Ok(())
    }

    /// Return `true` if this pointer advertises a snapshot for `chain_id`.
    ///
    /// Verification uses this to reject a `latest.json` belonging to a different chain before
    /// spending a round trip on the manifest it points at.
    pub fn matches_chain(&self, chain_id: u64) -> bool {
        self.chain_id == chain_id
    }
}

/// One object listed in a [`Manifest`], with the metadata needed to fetch and verify it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEntry {
    /// Object name of this artifact relative to its epoch directory (for example
    /// `state-000.jsonl.zst`).
    pub name: String,
    /// Category of data this artifact holds.
    pub kind: ArtifactKind,
    /// Size of the artifact in bytes, as stored (compressed) in the bucket.
    pub size: u64,
    /// Lowercase-hex sha256 (64 characters) of the artifact's stored bytes, checked on download.
    pub sha256: String,
}

/// The category of a snapshot [`ArtifactEntry`].
///
/// Serialized in kebab-case (`state-chunk`, `headers`, `epoch-records`). An unrecognized value
/// fails deserialization, which is correct within [`FORMAT_VERSION`]: new artifact categories
/// require a schema bump that older nodes already reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactKind {
    /// A chunk of EVM state (accounts, storage slots, and bytecode).
    StateChunk,
    /// Execution block headers for the snapshot's epoch.
    Headers,
    /// The signed epoch-record chain used to anchor trustless verification.
    EpochRecords,
}

/// Aggregate item counts recorded in a [`Manifest`] for quick inspection and sanity checks.
///
/// These are summary totals only; verification relies on the per-artifact digests, not these
/// counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Counts {
    /// Total number of accounts across all state chunks.
    pub accounts: u64,
    /// Total number of storage slots across all state chunks.
    pub storage_slots: u64,
    /// Total number of distinct contract bytecodes.
    pub bytecodes: u64,
    /// Total number of execution headers included.
    pub headers: u64,
    /// Total number of epoch records included.
    pub records: u64,
}

/// Serde adapter that renders an [`EpochDigest`] as a lowercase `0x`-prefixed hex string.
///
/// [`EpochDigest`]'s intrinsic human-readable serde emits base58, but the manifest pins every
/// 32-byte hash to lowercase hex so operators and downstream tooling read them uniformly. The
/// adapter routes through [`B256`], whose serde is the canonical lowercase-hex encoding, using the
/// lossless conversions the digest type already provides.
mod epoch_digest_hex {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tn_types::{EpochDigest, B256};

    pub(super) fn serialize<S>(digest: &EpochDigest, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        B256::from(*digest).serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<EpochDigest, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(EpochDigest::from(B256::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
            artifacts: vec![
                ArtifactEntry {
                    name: "state-000.jsonl.zst".to_string(),
                    kind: ArtifactKind::StateChunk,
                    size: 1234,
                    sha256: "aa".repeat(32),
                },
                ArtifactEntry {
                    name: "headers.jsonl.zst".to_string(),
                    kind: ArtifactKind::Headers,
                    size: 56,
                    sha256: "bb".repeat(32),
                },
            ],
            counts: Counts {
                accounts: 10,
                storage_slots: 20,
                bytecodes: 3,
                headers: 5,
                records: 8,
            },
            fee_derivable: true,
        }
    }

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

    #[test]
    fn manifest_round_trips_through_json() {
        let manifest = sample_manifest();
        let bytes = manifest.to_json().unwrap();
        let parsed = Manifest::from_json(&bytes).unwrap();
        assert_eq!(manifest, parsed);
    }

    #[test]
    fn pointer_round_trips_through_json() {
        let pointer = sample_pointer();
        let bytes = pointer.to_json().unwrap();
        let parsed = Pointer::from_json(&bytes).unwrap();
        assert_eq!(pointer, parsed);
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        let manifest = sample_manifest();
        let mut value = serde_json::to_value(&manifest).unwrap();
        // a future schema may add fields; older readers must ignore them, not error
        value
            .as_object_mut()
            .unwrap()
            .insert("future_only_field".to_string(), json!({ "nested": true }));
        let bytes = serde_json::to_vec(&value).unwrap();

        let parsed = Manifest::from_json(&bytes).unwrap();
        assert_eq!(manifest, parsed);
    }

    #[test]
    fn unknown_format_version_is_rejected() {
        let manifest = sample_manifest();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value["format_version"] = json!(2);
        let bytes = serde_json::to_vec(&value).unwrap();

        let err = Manifest::from_json(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "expected Manifest error, got {err:?}");
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let manifest = sample_manifest();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value["kind"] = json!("full-history");
        let bytes = serde_json::to_vec(&value).unwrap();

        let err = Manifest::from_json(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "expected Manifest error, got {err:?}");
    }

    #[test]
    fn artifact_names_must_be_single_components() {
        for bad in ["", ".", "..", "a/b", "/abs", "a\\b", "../x"] {
            let mut manifest = sample_manifest();
            manifest.artifacts[0].name = bad.to_string();
            // serialize raw (bypassing to_json's outbound validation) so from_json runs the check
            // on the parse path every consumer uses
            let bytes = serde_json::to_vec(&manifest).unwrap();
            let err = Manifest::from_json(&bytes).unwrap_err();
            assert!(
                matches!(err, SnapshotError::Manifest(_)),
                "name {bad:?} must be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn duplicate_artifact_names_are_rejected() {
        let mut manifest = sample_manifest();
        let name = manifest.artifacts[0].name.clone();
        manifest.artifacts[1].name = name;
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let err = Manifest::from_json(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "got {err:?}");
    }

    #[test]
    fn pointer_unknown_format_version_is_rejected() {
        let pointer = sample_pointer();
        let mut value = serde_json::to_value(&pointer).unwrap();
        value["format_version"] = json!(9);
        let bytes = serde_json::to_vec(&value).unwrap();

        let err = Pointer::from_json(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotError::Manifest(_)), "expected Manifest error, got {err:?}");
    }

    #[test]
    fn artifact_lookup_finds_entries_by_name() {
        let manifest = sample_manifest();
        assert_eq!(
            manifest.artifact("state-000.jsonl.zst").map(|entry| entry.kind),
            Some(ArtifactKind::StateChunk)
        );
        assert_eq!(
            manifest.artifact("headers.jsonl.zst").map(|entry| entry.kind),
            Some(ArtifactKind::Headers)
        );
        assert!(manifest.artifact("does-not-exist").is_none());
    }

    #[test]
    fn artifact_kind_serializes_as_kebab_case() {
        assert_eq!(serde_json::to_value(ArtifactKind::StateChunk).unwrap(), json!("state-chunk"));
        assert_eq!(serde_json::to_value(ArtifactKind::Headers).unwrap(), json!("headers"));
        assert_eq!(
            serde_json::to_value(ArtifactKind::EpochRecords).unwrap(),
            json!("epoch-records")
        );
    }

    #[test]
    fn hash_and_digest_fields_serialize_as_0x_hex() {
        let manifest = sample_manifest();
        let value = serde_json::to_value(&manifest).unwrap();

        let genesis = value["genesis_hash"].as_str().unwrap();
        assert!(genesis.starts_with("0x") && genesis.len() == 66, "genesis_hash: {genesis}");

        // epoch_record_digest is pinned to hex even though EpochDigest's native serde is base58
        let digest = value["epoch_record_digest"].as_str().unwrap();
        assert_eq!(digest, format!("0x{}", "ab".repeat(32)));

        let final_state_hash = value["final_state"]["hash"].as_str().unwrap();
        assert!(
            final_state_hash.starts_with("0x") && final_state_hash.len() == 66,
            "final_state.hash: {final_state_hash}"
        );

        // and the hex digest decodes back to the exact same EpochDigest
        let parsed = Manifest::from_json(&manifest.to_json().unwrap()).unwrap();
        assert_eq!(parsed.epoch_record_digest, manifest.epoch_record_digest);
    }

    #[test]
    fn pointer_matches_only_its_own_chain() {
        let pointer = sample_pointer();
        assert!(pointer.matches_chain(2017));
        assert!(!pointer.matches_chain(1));
    }
}
