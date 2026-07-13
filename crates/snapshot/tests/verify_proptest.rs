//! Property-based tests for [`tn_snapshot::verify::verify_snapshot`] over synthetic snapshots.
//!
//! `verify_snapshot` is the trust boundary a fresh node crosses before laying downloaded bytes onto
//! its datadir: from nothing but the local genesis committee and genesis hash it re-establishes the
//! whole snapshot. These tests exercise that boundary as an external consumer would, staging real
//! zstd artifacts on disk and driving the public entry point, so two invariants hold across the
//! entire input space the exporter can produce:
//!
//! 1. **Valid-chain acceptance is total.** For any chain the strategy can draw -- arbitrary
//!    committee rotations (each epoch a freshly generated committee of a possibly different size,
//!    so `next_committee` genuinely differs from `committee`), arbitrary quorum-meeting signer
//!    subsets, arbitrary header-window lengths and start offsets, and arbitrary state-chunk bytes
//!    -- `verify_snapshot` accepts, and the returned [`VerifiedSnapshot`] matches the inputs: `N+1`
//!    records for epochs `0..=N`, a header window of the staged length whose recomputed hashes
//!    chain, the epoch-0 committee equal to the local genesis trust root, and the window
//!    terminating exactly at the manifest checkpoint.
//! 2. **Any single-point corruption is rejected, in the right error family.** From a valid case we
//!    apply exactly one mutation drawn from a `prop_oneof!` chooser -- a broken record link, a
//!    broken committee handoff, a sub-quorum certificate, a records-vector length change, a mutated
//!    manifest digest/counts, a broken header link or final-state binding, a flipped artifact byte,
//!    or a chain-id/genesis/version mismatch -- and require `verify_snapshot` to return `Err`, with
//!    `Integrity` for byte/size corruption, `Manifest` for chain-id/genesis/version issues, and
//!    `Verification` for every broken chain, quorum, header, or binding invariant.
//!
//! The chain-building fixtures are replicated from the `#[cfg(test)]` module in
//! `crates/snapshot/src/verify.rs` (unreachable from an integration-test binary): per-epoch
//! `Arc<BlsKeypair>` sets sorted by public key so a keypair's index equals its committee position
//! and its certificate-bitmap index, `Committee::new_for_test` for the genesis trust root, and the
//! `Default`-in-field-position trick that builds the certificate's roaring bitmap without a roaring
//! dependency. The staged artifact byte formats mirror `export.rs` exactly (zstd over a JSONL state
//! chunk, `serde_json` header array, and BCS `(record, cert)` chain).

// This standalone integration-test binary intentionally uses only a subset of the crate's
// dev-dependencies; mirror the crate lib's allow so `unused_crate_dependencies` (a CI-hard warning)
// does not fire on the ones the sibling integration tests use.
#![allow(unused_crate_dependencies)]

use proptest::prelude::*;
use rand::{rngs::StdRng, SeedableRng as _};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::{tempdir, TempDir};
use tn_snapshot::{
    manifest::{ArtifactEntry, ArtifactKind, Counts, Manifest, FORMAT_VERSION, KIND_STATE_ONLY},
    verify::{verify_snapshot, DecompressLimits, VerifiedSnapshot},
    SnapshotError, SnapshotResult,
};
use tn_types::{
    encode, hex, Address, Authority, BlockNumHash, BlsAggregateSignature, BlsKeypair, BlsPublicKey,
    BlsSignature, BlsSigner, Committee, ConsensusNumHash, EpochCertificate, EpochDigest,
    EpochRecord, ExecHeader, SealedHeader, Signer, B256,
};

// Artifact object names, matching `export.rs` exactly (verify keys on `ArtifactKind`, not the name,
// but staging the real names keeps a shrunk failure legible).
const STATE_ARTIFACT: &str = "state-000.jsonl.zst";
const HEADERS_ARTIFACT: &str = "headers.json.zst";
const RECORDS_ARTIFACT: &str = "epoch-records.bcs.zst";

/// zstd level used by the exporter; reused so staged bytes match production shape.
const ZSTD_LEVEL: i32 = 3;

/// First block number of the shipped header window. Block numbers are otherwise arbitrary to the
/// verifier; only their relative structure (contiguity, the epoch-start relationship) matters, so a
/// fixed base keeps the earlier-epoch checkpoints comfortably below the window.
const WINDOW_START: u64 = 1000;

/// Local chain id and genesis hash the manifest is bound to; the trust root the verifier compares
/// against unless a corruption deliberately diverges them.
const CHAIN_ID: u64 = 2017;
const GENESIS_HASH: B256 = B256::new([0xaau8; 32]);

// ================================================================================================
// Fixture builders (replicated from crates/snapshot/src/verify.rs `#[cfg(test)]`)
// ================================================================================================

/// A [`BlsSigner`] over an owned keypair, so [`EpochRecord::sign_vote`] can drive it.
#[derive(Clone)]
struct TestSigner(Arc<BlsKeypair>);

impl BlsSigner for TestSigner {
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
        self.0.sign(msg)
    }

    fn public_key(&self) -> BlsPublicKey {
        *self.0.public()
    }
}

/// Generate `n` keypairs from `rng`.
fn gen_keys(n: usize, rng: &mut StdRng) -> Vec<Arc<BlsKeypair>> {
    (0..n).map(|_| Arc::new(BlsKeypair::generate(rng))).collect()
}

/// Sort keypairs by public key to match `Committee::bls_keys` ordering, so a keypair's index equals
/// its committee position and its certificate-bitmap index.
fn sort_keys(mut keys: Vec<Arc<BlsKeypair>>) -> Vec<Arc<BlsKeypair>> {
    keys.sort_by_key(|k| *k.public());
    keys
}

/// The public keys of a keypair set, in the set's order.
fn pubkeys(keys: &[Arc<BlsKeypair>]) -> Vec<BlsPublicKey> {
    keys.iter().map(|k| *k.public()).collect()
}

/// Build a real [`Committee`] from a sorted keypair set (the epoch-0 trust root). Its `bls_keys()`
/// equals the same sorted public keys the verifier compares `records[0].committee` against.
fn committee_of(keys: &[Arc<BlsKeypair>]) -> Committee {
    let authorities: BTreeMap<BlsPublicKey, Authority> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let pk = *k.public();
            (pk, Authority::new_for_test(pk, Address::from([i as u8 + 1; 20])))
        })
        .collect();
    Committee::new_for_test(authorities, 0, BTreeMap::new())
}

/// Sign `record` with the committee positions in `positions`, aggregating into a certificate.
///
/// `positions` must be ascending (as every caller here produces) so the aggregated signature, the
/// bitmap, and the committee-index-ordered keys the verifier gathers all line up.
fn certify(
    record: &EpochRecord,
    keys: &[Arc<BlsKeypair>],
    positions: &[usize],
) -> EpochCertificate {
    let sigs: Vec<BlsSignature> = positions
        .iter()
        .map(|&p| record.sign_vote(&TestSigner(keys[p].clone())).signature)
        .collect();
    let signature =
        BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate").to_signature();
    // Default::default() infers roaring::RoaringBitmap in field position without naming the crate;
    // insert() then populates it in the committee-index order verify_with_cert expects.
    let mut cert = EpochCertificate {
        epoch_hash: record.digest(),
        signature,
        signed_authorities: Default::default(),
    };
    for &p in positions {
        cert.signed_authorities.insert(p as u32);
    }
    cert
}

/// Build `count` internally hash-linked headers starting at block `start`.
///
/// Alternating headers carry `Some` post-merge fields (base fee, withdrawals root) while the rest
/// leave them `None`, so the JSON header codec is exercised on exactly the `Option` mix that makes
/// a positional BCS encoding of `ExecHeader` unsound.
fn build_headers(start: u64, count: usize) -> Vec<ExecHeader> {
    let mut headers = Vec::with_capacity(count);
    let mut parent = B256::from([0x77u8; 32]);
    for i in 0..count {
        let (base_fee_per_gas, withdrawals_root) = if i % 2 == 0 {
            (Some(1_000_000_000 + i as u64), Some(B256::from([0x55u8; 32])))
        } else {
            (None, None)
        };
        let header = ExecHeader {
            number: start + i as u64,
            parent_hash: parent,
            base_fee_per_gas,
            withdrawals_root,
            ..Default::default()
        };
        parent = SealedHeader::seal_slow(header.clone()).hash();
        headers.push(header);
    }
    headers
}

/// Pick a quorum-meeting subset of committee positions.
///
/// Returns an ascending set of size `size - drop_count` where `drop_count` in `0..=(size -
/// quorum)`, so the result always meets `quorum`. `choice` selects both how many signers to omit
/// and which, spreading coverage across "exactly quorum" and "everyone signs" and across which
/// position is dropped.
fn quorum_subset(size: usize, quorum: usize, choice: u64) -> Vec<usize> {
    let slack = size - quorum;
    let drop_count = (choice % (slack as u64 + 1)) as usize;
    let start = (choice / (slack as u64 + 1)) as usize % size;
    let dropped: BTreeSet<usize> = (0..drop_count).map(|i| (start + i) % size).collect();
    (0..size).filter(|p| !dropped.contains(p)).collect()
}

/// An unsigned description of a record chain plus its header window; [`finish`] signs it.
/// Corruptions perturb exactly one field of a valid spec before signing so each rejection maps to
/// one check.
struct ChainSpec {
    /// Per-epoch committee keypairs, each vec sorted by public key.
    committees: Vec<Vec<Arc<BlsKeypair>>>,
    /// The local genesis committee, built from epoch 0's keys.
    genesis: Committee,
    /// The `next_committee` field written into each record.
    next_committees: Vec<Vec<BlsPublicKey>>,
    /// Optional `parent_hash` overrides (None auto-chains to the previous digest).
    parent_overrides: Vec<Option<EpochDigest>>,
    /// The committee positions that sign each record's certificate.
    signer_positions: Vec<Vec<usize>>,
    /// The `final_state` checkpoint written into each record.
    final_states: Vec<BlockNumHash>,
    /// The `final_consensus` checkpoint written into each record.
    final_consensus: Vec<ConsensusNumHash>,
    /// The execution-header window for epoch N.
    headers: Vec<ExecHeader>,
}

/// A signed chain plus the pieces a test needs to assemble and perturb a staged snapshot.
struct ChainParts {
    genesis: Committee,
    records: Vec<(EpochRecord, EpochCertificate)>,
    headers: Vec<ExecHeader>,
}

/// A staged snapshot on disk with its derived manifest and trust root.
struct Fixture {
    /// Held so the staged directory outlives the test; never read directly.
    _dir: TempDir,
    staged: PathBuf,
    manifest: Manifest,
    genesis: Committee,
    genesis_hash: B256,
    chain_id: u64,
}

/// Sign a [`ChainSpec`] into an ordered `(record, certificate)` chain, auto-chaining `parent_hash`
/// where not overridden.
fn finish(spec: &ChainSpec) -> ChainParts {
    let mut records: Vec<(EpochRecord, EpochCertificate)> = Vec::new();
    for k in 0..spec.committees.len() {
        let parent_hash = match spec.parent_overrides[k] {
            Some(digest) => digest,
            None if k == 0 => EpochDigest::default(),
            None => records[k - 1].0.digest(),
        };
        let record = EpochRecord {
            epoch: k as u32,
            committee: pubkeys(&spec.committees[k]),
            next_committee: spec.next_committees[k].clone(),
            parent_hash,
            final_state: spec.final_states[k],
            final_consensus: spec.final_consensus[k],
        };
        let cert = certify(&record, &spec.committees[k], &spec.signer_positions[k]);
        records.push((record, cert));
    }
    ChainParts { genesis: spec.genesis.clone(), records, headers: spec.headers.clone() }
}

/// Lowercase-hex sha256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Compress `plain`, write it under `dir/name`, and return the manifest entry describing the staged
/// (compressed) file, with the sha256 and size the verifier will recompute.
fn write_artifact(dir: &Path, name: &str, kind: ArtifactKind, plain: &[u8]) -> ArtifactEntry {
    let compressed = zstd::encode_all(plain, ZSTD_LEVEL).expect("compress artifact");
    std::fs::write(dir.join(name), &compressed).expect("write artifact");
    ArtifactEntry {
        name: name.to_string(),
        kind,
        size: compressed.len() as u64,
        sha256: sha256_hex(&compressed),
    }
}

/// Stage all three artifacts and derive a manifest bound to the chain's epoch-N record.
fn assemble(parts: &ChainParts, chain_id: u64, genesis_hash: B256, state: &[u8]) -> Fixture {
    let dir = tempdir().expect("tempdir");
    let staged = dir.path().to_path_buf();
    let n = parts.records.len() as u32 - 1;
    let last = &parts.records[n as usize].0;

    // opaque state bytes: verify checks integrity and decompresses, but never parses them.
    let state = write_artifact(&staged, STATE_ARTIFACT, ArtifactKind::StateChunk, state);
    let headers = write_artifact(
        &staged,
        HEADERS_ARTIFACT,
        ArtifactKind::Headers,
        &serde_json::to_vec(&parts.headers).expect("encode headers"),
    );
    let records = write_artifact(
        &staged,
        RECORDS_ARTIFACT,
        ArtifactKind::EpochRecords,
        &encode(&parts.records),
    );

    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        kind: KIND_STATE_ONLY.to_string(),
        chain_id,
        genesis_hash,
        epoch: n,
        final_state: last.final_state,
        final_consensus: last.final_consensus,
        epoch_record_digest: last.digest(),
        created_at: 1,
        node_version: "tn-test/0".to_string(),
        artifacts: vec![state, headers, records],
        counts: Counts {
            accounts: 0,
            storage_slots: 0,
            bytecodes: 0,
            headers: parts.headers.len() as u64,
            records: parts.records.len() as u64,
        },
        fee_derivable: true,
    };
    Fixture { _dir: dir, staged, manifest, genesis: parts.genesis.clone(), genesis_hash, chain_id }
}

/// Run the verifier against a fixture with default decompress limits.
fn run(fx: &Fixture) -> SnapshotResult<VerifiedSnapshot> {
    verify_snapshot(
        &fx.manifest,
        &fx.staged,
        &fx.genesis,
        fx.genesis_hash,
        fx.chain_id,
        DecompressLimits::default(),
    )
}

// ================================================================================================
// Chain-parameter strategy: the whole valid input space
// ================================================================================================

/// The generated shape of one valid chain, before signing and staging. Both property suites draw
/// from this; suite 2 additionally draws a [`Corruption`] to apply.
#[derive(Debug, Clone)]
struct ChainParams {
    /// Seed for the keypair RNG, so the chain is deterministic per case.
    seed: u64,
    /// Per-epoch committee sizes (length `N+1`), each in `3..=4`. Distinct sizes across epochs
    /// make committees rotate by size as well as by key.
    sizes: Vec<usize>,
    /// Per-epoch selector for the quorum-meeting signer subset.
    signer_choices: Vec<u64>,
    /// Header-window length in `3..=6`.
    window_len: usize,
    /// How many blocks into the window epoch N begins (`0..window_len`): 0 means the window starts
    /// exactly at epoch N's first block, larger values ship a history tail below it.
    gap: usize,
    /// Arbitrary state-chunk bytes; verify never parses them.
    state_content: Vec<u8>,
}

/// Draw a valid chain shape: `N` in `1..=3` (so `N+1` records, epoch-0 snapshots excluded), each
/// committee size in `3..=4`, a window length in `3..=6`, an arbitrary epoch-start gap, and
/// arbitrary state bytes.
fn arb_chain_params() -> impl Strategy<Value = ChainParams> {
    (2usize..=4)
        .prop_flat_map(|epochs| {
            (
                prop::collection::vec(3usize..=4, epochs), // per-epoch committee sizes
                prop::collection::vec(any::<u64>(), epochs), // per-epoch signer-subset selectors
                3usize..=6,                                // window length
                any::<u64>(),                              // gap selector (reduced mod window_len)
                prop::collection::vec(any::<u8>(), 1..256), // state-chunk bytes
                any::<u64>(),                              // keypair RNG seed
            )
        })
        .prop_map(|(sizes, signer_choices, window_len, gap_raw, state_content, seed)| {
            let gap = (gap_raw % window_len as u64) as usize;
            ChainParams { seed, sizes, signer_choices, window_len, gap, state_content }
        })
}

/// Build the unsigned [`ChainSpec`] for `params`: fresh rotating committees, a hash-linked header
/// window, checkpoints wired so the window covers epoch N's first block, and quorum-meeting signer
/// subsets. `final_states[N]` is fixed from the *original* window here, so a later header mutation
/// can break the final-state binding without silently moving the checkpoint with it.
fn build_base_spec(params: &ChainParams) -> ChainSpec {
    let mut rng = StdRng::seed_from_u64(params.seed);
    let n = params.sizes.len() - 1;

    // each epoch gets an independently generated, sorted committee, so `next_committee` genuinely
    // differs from `committee` and the handoff is a real rotation.
    let committees: Vec<Vec<Arc<BlsKeypair>>> =
        params.sizes.iter().map(|&s| sort_keys(gen_keys(s, &mut rng))).collect();
    let genesis = committee_of(&committees[0]);

    let headers = build_headers(WINDOW_START, params.window_len);
    let tip = SealedHeader::seal_slow(headers[params.window_len - 1].clone());
    let final_state_n = BlockNumHash::new(tip.header().number, tip.hash());

    // epoch N begins `gap` blocks into the window; epoch N-1 ends one block before that, so the
    // window's first block (WINDOW_START) is <= epoch N's first block, as the verifier requires.
    let epoch_n_start = WINDOW_START + params.gap as u64;
    let final_states: Vec<BlockNumHash> = (0..=n)
        .map(|k| {
            if k == n {
                final_state_n
            } else if k == n - 1 {
                BlockNumHash::new(epoch_n_start - 1, B256::from([0x13u8; 32]))
            } else {
                // earlier epochs are unconstrained by verify; distinct small checkpoints suffice.
                BlockNumHash::new(10 * (k as u64 + 1), B256::from([k as u8 + 1; 32]))
            }
        })
        .collect();
    let final_consensus: Vec<ConsensusNumHash> = (0..=n as u64)
        .map(|i| ConsensusNumHash::new(i * 10, B256::from([i as u8 + 8; 32]).into()))
        .collect();

    // handoff wiring: epoch k advertises epoch k+1's committee; epoch N's next_committee is
    // unchecked by verify, so it reuses its own keys.
    let next_committees: Vec<Vec<BlsPublicKey>> =
        (0..=n).map(|k| pubkeys(&committees[if k < n { k + 1 } else { k }])).collect();

    let signer_positions: Vec<Vec<usize>> = (0..=n)
        .map(|k| {
            let size = params.sizes[k];
            let quorum = ((size * 2) / 3) + 1;
            quorum_subset(size, quorum, params.signer_choices[k])
        })
        .collect();

    ChainSpec {
        committees,
        genesis,
        next_committees,
        parent_overrides: vec![None; n + 1],
        signer_positions,
        final_states,
        final_consensus,
        headers,
    }
}

// ================================================================================================
// Property 1: valid-chain acceptance is total
// ================================================================================================

proptest! {
    /// Any chain the strategy can draw verifies, and the returned snapshot matches the inputs.
    #[test]
    fn prop_valid_chain_is_accepted(params in arb_chain_params()) {
        let spec = build_base_spec(&params);
        let parts = finish(&spec);
        let n = parts.records.len() - 1;
        let fx = assemble(&parts, CHAIN_ID, GENESIS_HASH, &params.state_content);

        let verified = run(&fx).map_err(|e| TestCaseError::fail(format!("valid chain rejected: {e}")))?;

        // lengths and single state chunk.
        prop_assert_eq!(verified.records.len(), n + 1);
        prop_assert_eq!(verified.headers.len(), params.window_len);
        prop_assert_eq!(verified.state_chunks.len(), 1);

        // every record sits at its own epoch.
        for (k, (record, _)) in verified.records.iter().enumerate() {
            prop_assert_eq!(record.epoch, k as u32);
        }

        // the epoch-0 committee is exactly the local genesis trust root.
        let genesis_keys: Vec<BlsPublicKey> = fx.genesis.bls_keys().into_iter().collect();
        prop_assert_eq!(&verified.records[0].0.committee, &genesis_keys);

        // committees genuinely rotate, and each record chains to its predecessor by digest and
        // inherits the predecessor's next_committee.
        prop_assert_ne!(&verified.records[0].0.committee, &verified.records[1].0.committee);
        for k in 1..=n {
            prop_assert_eq!(
                &verified.records[k].0.committee,
                &verified.records[k - 1].0.next_committee
            );
            prop_assert_eq!(verified.records[k].0.parent_hash, verified.records[k - 1].0.digest());
        }

        // the returned window is sealed with recomputed hashes that chain consecutively.
        for i in 1..verified.headers.len() {
            prop_assert_eq!(
                verified.headers[i].header().number,
                verified.headers[i - 1].header().number + 1
            );
            prop_assert_eq!(verified.headers[i].header().parent_hash, verified.headers[i - 1].hash());
        }

        // and it terminates exactly at the manifest's execution checkpoint.
        let last = verified.headers.last().expect("non-empty window");
        prop_assert_eq!(last.header().number, verified.manifest.final_state.number);
        prop_assert_eq!(last.hash(), verified.manifest.final_state.hash);
    }
}

// ================================================================================================
// Property 2: any single-point corruption is rejected
// ================================================================================================

/// The error family a corruption must surface.
#[derive(Debug, Clone, Copy)]
enum Family {
    /// Broken chain/quorum/header/binding invariant.
    Verification,
    /// Chain-id, genesis-hash, or version mismatch.
    Manifest,
    /// Byte/size/decompression integrity failure.
    Integrity,
}

/// A single-point corruption applied to an otherwise valid case. Parameter picks are reduced modulo
/// the actual chain dimensions when applied, so no arm can index out of range.
#[derive(Debug, Clone)]
enum Corruption {
    /// Re-point a later record's `parent_hash` to an unrelated digest (re-signed, so the cert is
    /// valid but the chain link breaks).
    ParentHashLink { pick: u64 },
    /// Give the epoch-0 record a non-zero `parent_hash`.
    Epoch0Parent,
    /// Swap one committee member of a later epoch for a fresh key, breaking the handoff from its
    /// predecessor's `next_committee`.
    Handoff { epoch_pick: u64, pos_pick: u64 },
    /// List the same key twice in a later epoch's committee, so the record names a duplicate
    /// validator.
    DuplicateCommitteeKey { epoch_pick: u64 },
    /// Certify the last record with one signer fewer than its super-quorum.
    SubQuorumLast,
    /// Collapse a later epoch's `final_consensus` height onto its predecessor's, breaking the
    /// strict increase across the chain.
    NonMonotonicFinalConsensus { pick: u64 },
    /// Break one header's parent link.
    HeaderLink { pick: u64 },
    /// Mutate the last header via a non-linking field so the recomputed window no longer ends at
    /// the bound `final_state`.
    FinalStateBinding,
    /// Point the manifest's `epoch_record_digest` at the wrong digest.
    ManifestDigest,
    /// Make `counts.records` disagree with the staged record count.
    CountsRecords,
    /// Make `counts.headers` disagree with the staged header count.
    CountsHeaders,
    /// Re-stage the records artifact one record short (integrity intact, length wrong).
    TruncateRecords,
    /// Re-stage the records artifact with a duplicated final record (integrity intact, length
    /// wrong).
    DuplicateRecords,
    /// Verify against a different local chain id.
    ChainId,
    /// Verify against a different local genesis hash.
    GenesisHash,
    /// Set an unsupported manifest `format_version`.
    FormatVersion,
    /// Flip one byte of one staged artifact file (size intact, sha256 broken).
    ByteFlip { artifact_pick: u64, byte_pick: u64 },
}

impl Corruption {
    /// The error family this corruption must produce.
    fn expected_family(&self) -> Family {
        match self {
            Corruption::ChainId | Corruption::GenesisHash | Corruption::FormatVersion => {
                Family::Manifest
            }
            Corruption::ByteFlip { .. } => Family::Integrity,
            _ => Family::Verification,
        }
    }
}

/// Draw one corruption; parametric arms carry raw picks reduced to valid indices at apply time.
fn arb_corruption() -> impl Strategy<Value = Corruption> {
    prop_oneof![
        any::<u64>().prop_map(|pick| Corruption::ParentHashLink { pick }),
        Just(Corruption::Epoch0Parent),
        (any::<u64>(), any::<u64>())
            .prop_map(|(epoch_pick, pos_pick)| Corruption::Handoff { epoch_pick, pos_pick }),
        any::<u64>().prop_map(|epoch_pick| Corruption::DuplicateCommitteeKey { epoch_pick }),
        Just(Corruption::SubQuorumLast),
        any::<u64>().prop_map(|pick| Corruption::NonMonotonicFinalConsensus { pick }),
        any::<u64>().prop_map(|pick| Corruption::HeaderLink { pick }),
        Just(Corruption::FinalStateBinding),
        Just(Corruption::ManifestDigest),
        Just(Corruption::CountsRecords),
        Just(Corruption::CountsHeaders),
        Just(Corruption::TruncateRecords),
        Just(Corruption::DuplicateRecords),
        Just(Corruption::ChainId),
        Just(Corruption::GenesisHash),
        Just(Corruption::FormatVersion),
        (any::<u64>(), any::<u64>()).prop_map(|(artifact_pick, byte_pick)| Corruption::ByteFlip {
            artifact_pick,
            byte_pick
        }),
    ]
}

/// Apply the corruptions that act on the unsigned spec (re-signed by [`finish`], so the certificate
/// stays valid and the failure is the specific chain/quorum/header invariant). Fixture-level
/// corruptions are no-ops here.
fn apply_spec_corruption(c: &Corruption, spec: &mut ChainSpec, rng: &mut StdRng) {
    let n = spec.committees.len() - 1;
    match c {
        Corruption::ParentHashLink { pick } => {
            let k = 1 + (*pick as usize % n); // 1..=n (n >= 1)
            spec.parent_overrides[k] = Some(EpochDigest::from(B256::from([0xEEu8; 32])));
        }
        Corruption::Epoch0Parent => {
            spec.parent_overrides[0] = Some(EpochDigest::from(B256::from([0x01u8; 32])));
        }
        Corruption::Handoff { epoch_pick, pos_pick } => {
            let k = 1 + (*epoch_pick as usize % n); // a later epoch, never the genesis committee
            let j = *pos_pick as usize % spec.committees[k].len();
            // the fresh key differs from the one epoch k-1's next_committee still advertises at j.
            spec.committees[k][j] = Arc::new(BlsKeypair::generate(rng));
        }
        Corruption::DuplicateCommitteeKey { epoch_pick } => {
            let k = 1 + (*epoch_pick as usize % n); // a later epoch, never the genesis committee
                                                    // list the same key twice; the dedup pass fires before the handoff pass, so the
                                                    // duplicate is the rejection.
            spec.committees[k][1] = spec.committees[k][0].clone();
        }
        Corruption::SubQuorumLast => {
            let size = spec.committees[n].len();
            let quorum = ((size * 2) / 3) + 1;
            spec.signer_positions[n] = (0..quorum - 1).collect();
        }
        Corruption::NonMonotonicFinalConsensus { pick } => {
            let k = 1 + (*pick as usize % n); // a later epoch; epoch 0 has no predecessor to trail
                                              // collapse this epoch's consensus checkpoint onto its predecessor's, so the broken
                                              // strict-increase is the only violated invariant.
            spec.final_consensus[k] = spec.final_consensus[k - 1];
        }
        Corruption::HeaderLink { pick } => {
            let i = *pick as usize % spec.headers.len();
            spec.headers[i].parent_hash.0[0] ^= 0xFF;
        }
        Corruption::FinalStateBinding => {
            let last = spec.headers.len() - 1;
            // changes the tip's recomputed hash without touching any parent link or
            // final_states[N].
            spec.headers[last].gas_limit ^= 0xFFFF;
        }
        _ => {}
    }
}

/// Re-stage the records artifact with a modified record vector and re-derive its manifest entry, so
/// the sha256/size still match (the length disagreement, not integrity, is what verify catches).
fn restage_records(fx: &mut Fixture, recs: Vec<(EpochRecord, EpochCertificate)>) {
    let entry =
        write_artifact(&fx.staged, RECORDS_ARTIFACT, ArtifactKind::EpochRecords, &encode(&recs));
    for artifact in &mut fx.manifest.artifacts {
        if artifact.name == RECORDS_ARTIFACT {
            *artifact = entry.clone();
        }
    }
}

/// Apply the corruptions that act on the assembled fixture. Spec-level corruptions are no-ops here.
fn apply_fixture_corruption(c: &Corruption, fx: &mut Fixture, parts: &ChainParts) {
    match c {
        Corruption::ManifestDigest => {
            fx.manifest.epoch_record_digest = EpochDigest::from(B256::from([0xEEu8; 32]));
        }
        Corruption::CountsRecords => {
            fx.manifest.counts.records = fx.manifest.counts.records.wrapping_add(1);
        }
        Corruption::CountsHeaders => {
            fx.manifest.counts.headers = fx.manifest.counts.headers.wrapping_add(1);
        }
        Corruption::TruncateRecords => {
            let mut recs = parts.records.clone();
            recs.pop();
            restage_records(fx, recs);
        }
        Corruption::DuplicateRecords => {
            let mut recs = parts.records.clone();
            recs.push(recs.last().expect("non-empty chain").clone());
            restage_records(fx, recs);
        }
        Corruption::ChainId => {
            fx.chain_id = fx.manifest.chain_id.wrapping_add(1);
        }
        Corruption::GenesisHash => {
            fx.genesis_hash = B256::from([0xBBu8; 32]);
        }
        Corruption::FormatVersion => {
            fx.manifest.format_version = FORMAT_VERSION + 1;
        }
        Corruption::ByteFlip { artifact_pick, byte_pick } => {
            let names = [STATE_ARTIFACT, HEADERS_ARTIFACT, RECORDS_ARTIFACT];
            let path = fx.staged.join(names[*artifact_pick as usize % names.len()]);
            let mut bytes = std::fs::read(&path).expect("read artifact");
            let idx = *byte_pick as usize % bytes.len();
            bytes[idx] ^= 0xFF;
            std::fs::write(&path, &bytes).expect("write artifact");
        }
        _ => {}
    }
}

proptest! {
    /// A valid case plus exactly one corruption is always rejected, in the corruption's error family.
    #[test]
    fn prop_single_point_corruption_is_rejected(
        params in arb_chain_params(),
        corruption in arb_corruption(),
    ) {
        // an RNG independent of the committee RNG, for the fresh key a handoff corruption needs.
        let mut rng = StdRng::seed_from_u64(params.seed ^ 0x5eed_5eed_5eed_5eed);
        let mut spec = build_base_spec(&params);
        apply_spec_corruption(&corruption, &mut spec, &mut rng);
        let parts = finish(&spec);
        let mut fx = assemble(&parts, CHAIN_ID, GENESIS_HASH, &params.state_content);
        apply_fixture_corruption(&corruption, &mut fx, &parts);

        let result = run(&fx);
        prop_assert!(result.is_err(), "corruption {corruption:?} was accepted");
        if let Err(err) = result {
            let family = corruption.expected_family();
            let ok = matches!(
                (family, &err),
                (Family::Verification, SnapshotError::Verification(_))
                    | (Family::Manifest, SnapshotError::Manifest(_))
                    | (Family::Integrity, SnapshotError::Integrity(_))
            );
            prop_assert!(ok, "corruption {corruption:?} expected {family:?}, got {err:?}");
        }
    }
}

// ================================================================================================
// Companion deterministic tests (fast regression anchors)
// ================================================================================================

/// A concrete two-epoch (N=1) valid chain with 4-member committees and a 4-header window.
fn concrete_params() -> ChainParams {
    ChainParams {
        seed: 7,
        sizes: vec![4, 4],
        signer_choices: vec![0, 1],
        window_len: 4,
        gap: 0,
        state_content: b"{\"root\":\"0x00\"}\naccount-a\naccount-b\n".to_vec(),
    }
}

/// Build, stage, and verify the concrete valid chain.
#[test]
fn accepts_concrete_valid_chain() {
    let params = concrete_params();
    let spec = build_base_spec(&params);
    let parts = finish(&spec);
    let fx = assemble(&parts, CHAIN_ID, GENESIS_HASH, &params.state_content);

    let verified = run(&fx).expect("valid chain must verify");
    assert_eq!(verified.records.len(), 2);
    assert_eq!(verified.headers.len(), 4);
    assert_eq!(verified.state_chunks.len(), 1);
    // committees rotate and the handoff holds.
    assert_ne!(verified.records[0].0.committee, verified.records[1].0.committee);
    assert_eq!(verified.records[1].0.committee, verified.records[0].0.next_committee);
    // the window terminates at the manifest checkpoint.
    let last = verified.headers.last().unwrap();
    assert_eq!(last.hash(), verified.manifest.final_state.hash);
}

/// Apply a concrete corruption to the concrete chain and return the error.
fn corrupt_concrete(corruption: &Corruption) -> SnapshotError {
    let params = concrete_params();
    let mut rng = StdRng::seed_from_u64(1);
    let mut spec = build_base_spec(&params);
    apply_spec_corruption(corruption, &mut spec, &mut rng);
    let parts = finish(&spec);
    let mut fx = assemble(&parts, CHAIN_ID, GENESIS_HASH, &params.state_content);
    apply_fixture_corruption(corruption, &mut fx, &parts);
    run(&fx).expect_err("corruption must be rejected")
}

/// A broken committee handoff is a `Verification` error naming the handoff.
#[test]
fn rejects_broken_handoff_concrete() {
    match corrupt_concrete(&Corruption::Handoff { epoch_pick: 0, pos_pick: 0 }) {
        SnapshotError::Verification(msg) => assert!(msg.contains("handoff mismatch"), "{msg}"),
        other => panic!("expected Verification, got {other:?}"),
    }
}

/// A last-record certificate one signer short of quorum is a `Verification` error.
#[test]
fn rejects_sub_quorum_last_concrete() {
    match corrupt_concrete(&Corruption::SubQuorumLast) {
        SnapshotError::Verification(msg) => {
            assert!(msg.contains("failed certificate verification"), "{msg}")
        }
        other => panic!("expected Verification, got {other:?}"),
    }
}

/// A flipped artifact byte is an `Integrity` error naming the sha256 mismatch.
#[test]
fn rejects_byte_flip_concrete() {
    match corrupt_concrete(&Corruption::ByteFlip { artifact_pick: 0, byte_pick: 0 }) {
        SnapshotError::Integrity(msg) => assert!(msg.contains("sha256 mismatch"), "{msg}"),
        other => panic!("expected Integrity, got {other:?}"),
    }
}

/// A truncated records artifact is a `Verification` error over the record count.
#[test]
fn rejects_truncated_records_concrete() {
    match corrupt_concrete(&Corruption::TruncateRecords) {
        SnapshotError::Verification(msg) => assert!(msg.contains("records"), "{msg}"),
        other => panic!("expected Verification, got {other:?}"),
    }
}

/// A mismatched local chain id is a `Manifest` error.
#[test]
fn rejects_chain_id_concrete() {
    match corrupt_concrete(&Corruption::ChainId) {
        SnapshotError::Manifest(msg) => assert!(msg.contains("chain_id mismatch"), "{msg}"),
        other => panic!("expected Manifest, got {other:?}"),
    }
}

/// An unsupported manifest format version is a `Manifest` error.
#[test]
fn rejects_bad_format_version_concrete() {
    match corrupt_concrete(&Corruption::FormatVersion) {
        SnapshotError::Manifest(msg) => assert!(msg.contains("format_version"), "{msg}"),
        other => panic!("expected Manifest, got {other:?}"),
    }
}
