//! Trustless verification of a downloaded snapshot against a local trust root.
//!
//! A fresh node fetches a snapshot from an object store it does not control. Before any of those
//! bytes touch the datadir, they pass through [`verify_snapshot`], which re-establishes trust from
//! the one thing the node already has: its local genesis committee (`committee.yaml`, loaded by the
//! caller) and genesis hash.
//!
//! The trust anchor is the signed [`EpochRecord`] chain. Epoch `0`'s record
//! must commit to the local genesis committee; every later record must chain to its predecessor by
//! `parent_hash` and inherit the predecessor's `next_committee`; and every record must carry an
//! aggregate-BLS certificate signed by a super-quorum (`2/3 + 1`) of *its own* committee. Once the
//! chain verifies, the manifest is bound to it (`epoch_record_digest`, `final_state`,
//! `final_consensus`), and the execution-header window is checked to recompute to the manifest's
//! final execution checkpoint. A malicious bucket can therefore only withhold a snapshot or serve
//! an older, still-valid one — it cannot forge a chain for a different committee.
//!
//! Everything compares typed values, never serialized strings: digests and hashes have more than
//! one textual encoding (for example a consensus digest renders as base58 while an execution hash
//! renders as hex), so string comparison would be both wrong and fragile.
//!
//! # Header artifact codec
//!
//! The execution-header window is transported through `decode_headers`, the single place that
//! knows the header wire codec. See that function for why the codec is JSON — the encoding the
//! exporter (`export::stage_zstd_artifact` on `serde_json::to_vec(&Vec<ExecHeader>)`) also uses.

use crate::{
    manifest::{ArtifactEntry, ArtifactKind, Manifest},
    SnapshotError, SnapshotResult,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
};
use tn_types::{
    try_decode, BlsPublicKey, Committee, EpochCertificate, EpochDigest, EpochRecord, ExecHeader,
    SealedHeader, B256,
};

/// I/O granularity for streaming hashing and decompression.
const STREAM_BUF_SIZE: usize = 256 * 1024;

/// A snapshot whose every artifact has passed [`verify_snapshot`].
///
/// Holding a `VerifiedSnapshot` is proof that the trust checks succeeded, so restore can consume
/// these fields without re-parsing untrusted bytes. The parsed `records` and `headers` are exactly
/// the structures the checks ran against; `state_chunks` are the compressed on-disk chunk files
/// whose sha256 digests matched the manifest, listed in manifest order (chunk `0` first) so they
/// concatenate back into the original state dump.
#[derive(Debug, Clone)]
pub struct VerifiedSnapshot {
    /// The manifest that was verified, retained for its checkpoints and metadata.
    pub manifest: Manifest,
    /// The full epoch-record chain, epochs `0..=N` in order, each paired with its certificate.
    pub records: Vec<(EpochRecord, EpochCertificate)>,
    /// The execution-header window, sealed with freshly recomputed hashes.
    pub headers: Vec<SealedHeader>,
    /// Paths to the verified, still-compressed state-chunk files, in manifest (concatenation)
    /// order.
    pub state_chunks: Vec<PathBuf>,
}

/// Per-artifact caps on decompressed size, the anti-zip-bomb guard for [`verify_snapshot`].
///
/// Decompression is bounded so a small, highly compressible artifact cannot expand without limit.
/// The defaults are generous enough for realistic chains; a CLI verifying an unusually large chain
/// can raise them explicitly. Records and headers are held in memory while parsed, so their caps
/// double as memory bounds; state chunks are streamed and discarded, so their cap only bounds work.
#[derive(Debug, Clone, Copy)]
pub struct DecompressLimits {
    /// Maximum decompressed size of the epoch-records artifact.
    pub records: u64,
    /// Maximum decompressed size of the header-window artifact.
    pub headers: u64,
    /// Maximum decompressed size of a single state chunk.
    pub state_chunk: u64,
}

impl Default for DecompressLimits {
    fn default() -> Self {
        Self {
            // records and headers are parsed in memory; state chunks are streamed to a sink.
            records: 512 * 1024 * 1024,
            headers: 2 * 1024 * 1024 * 1024,
            state_chunk: 8 * 1024 * 1024 * 1024,
        }
    }
}

/// Verify a staged snapshot against a local trust root, returning its parsed, trusted contents.
///
/// `staged_dir` must already contain every artifact file named in `manifest`; the caller (a CLI
/// `verify` command or the restore orchestrator) downloads them first. `genesis_committee` and
/// `genesis_hash` come from the node's local configuration and are the only trust the verification
/// starts from. `chain_id` is the local chain id. `limits` caps decompressed sizes.
///
/// The checks run in this order and every failure names what broke:
///
/// - **manifest**: [`Manifest::validate`], then `chain_id` and `genesis_hash` must match, and an
///   epoch-`0` snapshot is rejected outright (nothing precedes genesis, so sync from genesis).
/// - **artifacts**: the manifest must list at least one state chunk and exactly one each of the
///   header and record artifacts; every listed file must exist with a matching size and sha256.
/// - **record chain**: contiguous epochs `0..=N` with `N == manifest.epoch`, epoch `0` bound to the
///   local genesis committee with a zero `parent_hash`, each later record chaining by `parent_hash`
///   and inheriting the previous `next_committee`, and every record certified by a super-quorum of
///   its own committee.
/// - **binding**: the last record's digest, `final_state`, and `final_consensus` equal the
///   manifest's.
/// - **header window**: non-empty, consecutively numbered, internally hash-linked, ending exactly
///   at `manifest.final_state`, and starting no later than epoch `N`'s first block.
///
/// # Errors
///
/// Returns [`SnapshotError::Manifest`] for a malformed or mis-targeted manifest,
/// [`SnapshotError::Integrity`] for a missing file, size/sha256 mismatch, or a decompression cap
/// breach, and [`SnapshotError::Verification`] for any broken chain, certificate, binding, or
/// header-window invariant.
pub fn verify_snapshot(
    manifest: &Manifest,
    staged_dir: &Path,
    genesis_committee: &Committee,
    genesis_hash: B256,
    chain_id: u64,
    limits: DecompressLimits,
) -> SnapshotResult<VerifiedSnapshot> {
    // A. static manifest checks and chain binding.
    manifest.validate()?;
    if manifest.chain_id != chain_id {
        return Err(SnapshotError::Manifest(format!(
            "chain_id mismatch: manifest is for {}, local chain is {chain_id}",
            manifest.chain_id
        )));
    }
    if manifest.genesis_hash != genesis_hash {
        return Err(SnapshotError::Manifest(format!(
            "genesis_hash mismatch: manifest is for {}, local genesis is {genesis_hash}",
            manifest.genesis_hash
        )));
    }
    // epoch 0 is genesis itself: there is no prior record to anchor a header window, so a snapshot
    // at epoch 0 carries no benefit over syncing from genesis and is not supported.
    if manifest.epoch == 0 {
        return Err(SnapshotError::Verification(
            "epoch-0 snapshots are unsupported; sync from genesis instead".to_string(),
        ));
    }
    let n = manifest.epoch;

    // B. locate the required artifacts by kind and verify each file's integrity.
    let (records_entry, headers_entry, state_entries) = partition_artifacts(manifest)?;
    for entry in &manifest.artifacts {
        verify_artifact_integrity(staged_dir, entry)?;
    }

    // C. decode and verify the epoch-record chain.
    let records_bytes = decompress_to_vec(&staged_dir.join(&records_entry.name), limits.records)?;
    let records = decode_records(&records_bytes)?;
    verify_record_chain(&records, n, genesis_committee, manifest)?;

    // D. bind the manifest to the last record.
    verify_manifest_binding(&records, n, manifest)?;

    // E. decode and verify the execution-header window.
    let headers_bytes = decompress_to_vec(&staged_dir.join(&headers_entry.name), limits.headers)?;
    let headers = decode_headers(&headers_bytes)?;
    let sealed = verify_header_window(headers, &records, n, manifest)?;

    // decompress each state chunk to enforce the anti-bomb cap and confirm the zstd stream is
    // well-formed; the bytes themselves are already pinned by the sha256 check above and are
    // streamed to a sink rather than retained (reth recomputes the state root on import).
    let mut state_chunks = Vec::with_capacity(state_entries.len());
    for entry in &state_entries {
        let path = staged_dir.join(&entry.name);
        decompress_to_sink(&path, &mut io::sink(), limits.state_chunk)?;
        state_chunks.push(path);
    }

    Ok(VerifiedSnapshot { manifest: manifest.clone(), records, headers: sealed, state_chunks })
}

/// Split the manifest's artifacts into the record, header, and state entries it must contain.
///
/// Enforces the structural shape of a state-only snapshot: at least one state chunk and exactly one
/// each of the header and record artifacts. A missing or duplicated required kind is a malformed
/// manifest.
fn partition_artifacts(
    manifest: &Manifest,
) -> SnapshotResult<(&ArtifactEntry, &ArtifactEntry, Vec<&ArtifactEntry>)> {
    let mut records_entry = None;
    let mut headers_entry = None;
    let mut state_entries = Vec::new();
    for entry in &manifest.artifacts {
        match entry.kind {
            ArtifactKind::StateChunk => state_entries.push(entry),
            ArtifactKind::Headers => {
                if headers_entry.replace(entry).is_some() {
                    return Err(SnapshotError::Manifest(
                        "manifest lists more than one header artifact".to_string(),
                    ));
                }
            }
            ArtifactKind::EpochRecords => {
                if records_entry.replace(entry).is_some() {
                    return Err(SnapshotError::Manifest(
                        "manifest lists more than one epoch-records artifact".to_string(),
                    ));
                }
            }
        }
    }
    if state_entries.is_empty() {
        return Err(SnapshotError::Manifest("manifest lists no state-chunk artifacts".to_string()));
    }
    let records_entry = records_entry.ok_or_else(|| {
        SnapshotError::Manifest("manifest lists no epoch-records artifact".to_string())
    })?;
    let headers_entry = headers_entry
        .ok_or_else(|| SnapshotError::Manifest("manifest lists no header artifact".to_string()))?;
    Ok((records_entry, headers_entry, state_entries))
}

/// Confirm a staged artifact file exists and matches the manifest entry's size and sha256.
///
/// The digest is recomputed over the stored (compressed) bytes and compared case-insensitively to
/// the manifest's lowercase-hex value, mirroring the download-time check.
fn verify_artifact_integrity(staged_dir: &Path, entry: &ArtifactEntry) -> SnapshotResult<()> {
    let path = staged_dir.join(&entry.name);
    if !path.is_file() {
        return Err(SnapshotError::Integrity(format!(
            "artifact {} is missing from the staged directory",
            entry.name
        )));
    }
    let size = std::fs::metadata(&path)?.len();
    if size != entry.size {
        return Err(SnapshotError::Integrity(format!(
            "size mismatch for {}: manifest says {}, file is {size}",
            entry.name, entry.size
        )));
    }
    let actual = sha256_file(&path)?;
    if !actual.eq_ignore_ascii_case(&entry.sha256) {
        return Err(SnapshotError::Integrity(format!(
            "sha256 mismatch for {}: manifest says {}, file hashes to {actual}",
            entry.name, entry.sha256
        )));
    }
    Ok(())
}

/// Verify the epoch-record chain against the local genesis committee.
///
/// Runs every record-chain invariant: contiguous epochs `0..=n`, epoch `0` bound to the trust root,
/// `parent_hash`/`next_committee` chaining for later epochs, and a super-quorum certificate on
/// every record. The manifest's `counts.records` is enforced against the actual length.
///
/// The cheap structural checks run before any BLS pairing; certificates are verified last, so a
/// malformed or foreign chain is rejected without paying for a single signature verification.
fn verify_record_chain(
    records: &[(EpochRecord, EpochCertificate)],
    n: u32,
    genesis_committee: &Committee,
    manifest: &Manifest,
) -> SnapshotResult<()> {
    // pass 1: length. every later pass iterates this vector, so pin its length first.
    let expected_len = n as usize + 1;
    if records.len() != expected_len {
        return Err(SnapshotError::Verification(format!(
            "expected {expected_len} records for epochs 0..={n}, found {}",
            records.len()
        )));
    }
    if manifest.counts.records != records.len() as u64 {
        return Err(SnapshotError::Verification(format!(
            "counts.records {} does not match the {} records present",
            manifest.counts.records,
            records.len()
        )));
    }

    // the passes below are ordered cheapest-first, and the per-record BLS certificate check (pass
    // 3) runs last on purpose. each `verify_with_cert` costs a pairing per signer, and the
    // records vector is decompressed under a cap that admits millions of entries, so a hostile
    // bucket could otherwise force millions of pairings just by shipping a long, well-formed
    // but foreign chain. the structural passes are all O(1) per record, and the epoch-0 anchor
    // (pass 2b) pins the chain to this node's local genesis committee, so a snapshot for a
    // different chain dies at zero pairings.

    // pass 2a: every record must sit at its own epoch. this runs before the anchor and link passes
    // because their messages describe records as "epoch k", which is only meaningful once index and
    // epoch coincide.
    for (k, (record, _cert)) in records.iter().enumerate() {
        if record.epoch != k as u32 {
            return Err(SnapshotError::Verification(format!(
                "record at index {k} claims epoch {}, expected {k}",
                record.epoch
            )));
        }
        // both committee lists must name distinct validators. next_committee is checked here as
        // well as committee because epoch N's next_committee is consumed unchecked by restore's
        // reconstruct_committee, so a duplicate there would otherwise slip through.
        reject_duplicate_keys(record.epoch, "committee", &record.committee)?;
        reject_duplicate_keys(record.epoch, "next_committee", &record.next_committee)?;
    }

    // pass 2b: epoch 0 anchors to the local genesis committee. production builds this record's
    // committee from `Committee::bls_keys` (a sorted set), so compare against the same ordered key
    // material; a mismatch means the snapshot belongs to a different chain than this node.
    let genesis_keys: Vec<BlsPublicKey> = genesis_committee.bls_keys().into_iter().collect();
    if records[0].0.committee != genesis_keys {
        return Err(SnapshotError::Verification(
            "epoch-0 committee does not match the local genesis committee (trust-root mismatch)"
                .to_string(),
        ));
    }
    if records[0].0.parent_hash != EpochDigest::default() {
        return Err(SnapshotError::Verification(
            "epoch-0 record has a non-zero parent_hash".to_string(),
        ));
    }

    // pass 2c: later records chain by parent digest and inherit the previous epoch's
    // next_committee.
    for k in 1..records.len() {
        let record = &records[k].0;
        let prev = &records[k - 1].0;
        if record.parent_hash != prev.digest() {
            return Err(SnapshotError::Verification(format!(
                "record for epoch {k} parent_hash does not match epoch {} digest",
                k - 1
            )));
        }
        if record.committee != prev.next_committee {
            return Err(SnapshotError::Verification(format!(
                "record for epoch {k} committee does not match epoch {} next_committee (handoff mismatch)",
                k - 1
            )));
        }
        // final_consensus is the epoch's last consensus block number, so it must strictly increase
        // across the chain: number_to_epoch binary-searches these numbers, and two epochs sharing a
        // consensus height (or one going backwards) would corrupt that mapping.
        if record.final_consensus.number <= prev.final_consensus.number {
            return Err(SnapshotError::Verification(format!(
                "record for epoch {k} final_consensus number {} does not increase over epoch {} ({})",
                record.final_consensus.number,
                k - 1,
                prev.final_consensus.number
            )));
        }
    }

    // pass 3: verify every record's aggregate-BLS certificate. this is the expensive pass, a
    // pairing per signer, and runs last so the cheap structural checks above reject a malformed
    // or foreign chain before any pairing is computed.
    for (k, (record, cert)) in records.iter().enumerate() {
        if !record.verify_with_cert(cert) {
            return Err(SnapshotError::Verification(format!(
                "record for epoch {k} failed certificate verification (digest, quorum, or aggregate signature)"
            )));
        }
    }
    Ok(())
}

/// Reject a committee-key list that names the same [`BlsPublicKey`] more than once.
///
/// A record's `committee` and `next_committee` must each be a set of distinct validators. A
/// duplicate is dangerous because the certificate is an aggregate BLS signature checked against the
/// keys gathered by committee index: one physical validator sitting at two positions can sign the
/// epoch message once and have its key counted at every position it occupies, pushing the aggregate
/// over the member-count super-quorum while representing a single vote. BLS aggregation is linear,
/// so the signature over a repeated key verifies against that same repeated key — the certificate
/// check alone cannot catch it. Rejecting duplicates up front keeps "super-quorum of members"
/// honest and is the reason this runs in the cheap structural pass, before any pairing.
fn reject_duplicate_keys(epoch: u32, field: &str, keys: &[BlsPublicKey]) -> SnapshotResult<()> {
    let mut seen: HashSet<&BlsPublicKey> = HashSet::with_capacity(keys.len());
    for key in keys {
        if !seen.insert(key) {
            return Err(SnapshotError::Verification(format!(
                "record for epoch {epoch} lists duplicate key {key} in its {field}"
            )));
        }
    }
    Ok(())
}

/// Bind the manifest's checkpoints to the last record in the chain.
///
/// The record for epoch `n` is the snapshot's subject; its digest and closing execution/consensus
/// checkpoints must be exactly what the manifest advertises.
fn verify_manifest_binding(
    records: &[(EpochRecord, EpochCertificate)],
    n: u32,
    manifest: &Manifest,
) -> SnapshotResult<()> {
    let last = &records[n as usize].0;
    if last.digest() != manifest.epoch_record_digest {
        return Err(SnapshotError::Verification(
            "manifest epoch_record_digest does not match the epoch-N record digest".to_string(),
        ));
    }
    if last.final_state != manifest.final_state {
        return Err(SnapshotError::Verification(
            "manifest final_state does not match the epoch-N record final_state".to_string(),
        ));
    }
    if last.final_consensus != manifest.final_consensus {
        return Err(SnapshotError::Verification(
            "manifest final_consensus does not match the epoch-N record final_consensus"
                .to_string(),
        ));
    }
    Ok(())
}

/// Verify the execution-header window and return it sealed with recomputed hashes.
///
/// Each header is re-sealed so its hash is recomputed from its own bytes rather than trusted. The
/// window must be non-empty, consecutively numbered, internally hash-linked, end exactly at
/// `manifest.final_state`, and begin no later than epoch `n`'s first block — the block after epoch
/// `n-1`'s final state. The manifest's `counts.headers` is enforced against the actual length.
fn verify_header_window(
    headers: Vec<ExecHeader>,
    records: &[(EpochRecord, EpochCertificate)],
    n: u32,
    manifest: &Manifest,
) -> SnapshotResult<Vec<SealedHeader>> {
    if headers.is_empty() {
        return Err(SnapshotError::Verification("header window is empty".to_string()));
    }
    if manifest.counts.headers != headers.len() as u64 {
        return Err(SnapshotError::Verification(format!(
            "counts.headers {} does not match the {} headers present",
            manifest.counts.headers,
            headers.len()
        )));
    }

    // recompute every hash from the header bytes; this is what makes the window trustless.
    let sealed: Vec<SealedHeader> = headers.into_iter().map(SealedHeader::seal_slow).collect();

    for i in 1..sealed.len() {
        if sealed[i].header().number != sealed[i - 1].header().number + 1 {
            return Err(SnapshotError::Verification(format!(
                "header numbers are not consecutive at index {i}: {} then {}",
                sealed[i - 1].header().number,
                sealed[i].header().number
            )));
        }
        if sealed[i].header().parent_hash != sealed[i - 1].hash() {
            return Err(SnapshotError::Verification(format!(
                "header at index {i} does not link to its predecessor's recomputed hash"
            )));
        }
    }

    // the window must terminate exactly at the manifest's execution checkpoint.
    let last = sealed.last().expect("non-empty checked above");
    if last.header().number != manifest.final_state.number
        || last.hash() != manifest.final_state.hash
    {
        return Err(SnapshotError::Verification(
            "last header does not match manifest.final_state (number or hash)".to_string(),
        ));
    }

    // the window must reach back to (or before) epoch n's first block so restore has the full range
    // it needs; epoch n begins one block after epoch n-1's final state.
    let epoch_n_start = records[(n - 1) as usize].0.final_state.number + 1;
    let first_number = sealed[0].header().number;
    if first_number > epoch_n_start {
        return Err(SnapshotError::Verification(format!(
            "header window starts at block {first_number}, after epoch {n} begins at block {epoch_n_start}"
        )));
    }
    Ok(sealed)
}

/// Decode the epoch-records artifact.
///
/// The artifact is BCS — tn-types' canonical codec — over the same `(record, certificate)` pairs
/// the epoch-records store persists. A decode failure means the bytes are not a valid record chain.
fn decode_records(bytes: &[u8]) -> SnapshotResult<Vec<(EpochRecord, EpochCertificate)>> {
    try_decode(bytes).map_err(|e| {
        SnapshotError::Verification(format!("failed to decode epoch-records artifact: {e}"))
    })
}

/// Decode the execution-header window artifact into `ExecHeader`s.
///
/// This is the single point that knows the header wire codec; it must stay in lockstep with the
/// exporter, which produces the artifact as `serde_json::to_vec(&Vec<ExecHeader>)` (plain headers,
/// no hashes) before zstd.
///
/// JSON rather than the more natural `alloy_rlp::encode(&Vec<ExecHeader>)` because `alloy-rlp` is
/// not a dependency of this crate and is not re-exported by `tn-types`/`tn-reth`, and a raw BCS
/// encoding of `alloy`'s `Header` is unsound: its skipped optional fields break positional
/// decoding. JSON is the header type's native serde and round-trips the full `Option` mix. The
/// choice does not weaken verification — every hash is recomputed from the decoded header via
/// `seal_slow`, so the transport codec only has to reproduce the header fields, which it does.
fn decode_headers(bytes: &[u8]) -> SnapshotResult<Vec<ExecHeader>> {
    serde_json::from_slice(bytes).map_err(|e| {
        SnapshotError::Verification(format!("failed to decode header-window artifact: {e}"))
    })
}

/// Decompress a zstd file fully into memory, refusing to exceed `cap` bytes.
///
/// Used for the bounded records and header artifacts, which must be held whole to parse.
fn decompress_to_vec(src: &Path, cap: u64) -> SnapshotResult<Vec<u8>> {
    let mut out = Vec::new();
    decompress_to_sink(src, &mut out, cap)?;
    Ok(out)
}

/// Stream a zstd file into `sink`, refusing to emit more than `cap` decompressed bytes.
///
/// This is the anti-zip-bomb guard: decompressed bytes are counted as produced and a breach returns
/// [`SnapshotError::Integrity`] before the cap is exceeded, matching the store module's
/// file-to-file helper. Passing [`io::sink`] validates and bounds an artifact without retaining it.
fn decompress_to_sink<W: Write>(src: &Path, sink: &mut W, cap: u64) -> SnapshotResult<u64> {
    let mut decoder = zstd::Decoder::new(BufReader::new(File::open(src)?))?;
    let mut buffer = vec![0u8; STREAM_BUF_SIZE];
    let mut total: u64 = 0;
    loop {
        let read = decoder.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        if total > cap {
            return Err(SnapshotError::Integrity(format!(
                "decompressed size exceeds cap of {cap} bytes for {}",
                src.display()
            )));
        }
        sink.write_all(&buffer[..read])?;
    }
    Ok(total)
}

/// Hash a file with sha256, streaming it in fixed-size chunks, and return lowercase hex.
fn sha256_file(path: &Path) -> SnapshotResult<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; STREAM_BUF_SIZE];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

/// Encode bytes as a lowercase hex string.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // writing to a string is infallible
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::manifest::{Counts, FORMAT_VERSION, KIND_STATE_ONLY};
    use rand::{rngs::StdRng, SeedableRng};
    use std::{collections::BTreeMap, sync::Arc};
    use tempfile::tempdir;
    use tn_types::{
        encode, Address, Authority, BlockNumHash, BlsAggregateSignature, BlsKeypair, BlsSignature,
        BlsSigner, ConsensusNumHash, Signer,
    };

    // ----- fixture builders (mirrored by tests/verify_proptest.rs) -----
    //
    // a chain is built from raw, per-epoch `Arc<BlsKeypair>` sets (like the epoch_votes/epoch unit
    // tests), each sorted by public key so a keypair's index matches its position in the record's
    // `committee` vec and in the certificate's signer bitmap — the same ordering production derives
    // from `Committee::bls_keys`. `sign_chain` then produces a `(record, certificate)` per epoch,
    // and `assemble` compresses the three artifacts into a staged dir and derives a bound manifest.
    // negative tests perturb exactly one input to `finish`/`assemble` (or one staged byte) so each
    // rejection maps to a single failing check.

    /// A `BlsSigner` over an owned keypair, so `EpochRecord::sign_vote` can drive it.
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

    /// An unsigned description of a record chain plus its header window; `finish` signs it.
    pub(crate) struct ChainSpec {
        /// per-epoch committee keypairs, each vec sorted by public key.
        committees: Vec<Vec<Arc<BlsKeypair>>>,
        /// the local genesis committee, built from epoch 0's keys.
        genesis: Committee,
        /// the `next_committee` field written into each record.
        next_committees: Vec<Vec<BlsPublicKey>>,
        /// optional `parent_hash` overrides (None auto-chains to the previous digest).
        parent_overrides: Vec<Option<EpochDigest>>,
        /// number of committee positions that sign each record's certificate.
        signer_counts: Vec<usize>,
        /// the `final_state` checkpoint written into each record.
        final_states: Vec<BlockNumHash>,
        /// the `final_consensus` checkpoint written into each record.
        final_consensus: Vec<ConsensusNumHash>,
        /// the execution-header window for epoch N.
        headers: Vec<ExecHeader>,
    }

    /// A signed chain plus the pieces a test needs to assemble and perturb a staged snapshot.
    pub(crate) struct ChainParts {
        pub(crate) genesis: Committee,
        pub(crate) records: Vec<(EpochRecord, EpochCertificate)>,
        pub(crate) headers: Vec<ExecHeader>,
    }

    /// A staged snapshot on disk with its derived manifest and trust root.
    pub(crate) struct Fixture {
        _dir: tempfile::TempDir,
        pub(crate) staged: PathBuf,
        pub(crate) manifest: Manifest,
        pub(crate) genesis: Committee,
        pub(crate) genesis_hash: B256,
        pub(crate) chain_id: u64,
    }

    fn gen_keys(n: usize, rng: &mut StdRng) -> Vec<Arc<BlsKeypair>> {
        (0..n).map(|_| Arc::new(BlsKeypair::generate(rng))).collect()
    }

    /// Sort keypairs by public key to match `Committee::bls_keys` ordering.
    fn sort_keys(keys: Vec<Arc<BlsKeypair>>) -> Vec<Arc<BlsKeypair>> {
        let mut keys = keys;
        keys.sort_by_key(|k| *k.public());
        keys
    }

    fn pubkeys(keys: &[Arc<BlsKeypair>]) -> Vec<BlsPublicKey> {
        keys.iter().map(|k| *k.public()).collect()
    }

    /// Build a real `Committee` from a sorted keypair set (epoch 0's trust root).
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
        // Default::default() infers roaring::RoaringBitmap in field position without naming the
        // crate; insert() then populates it in the committee-index order verify_with_cert expects.
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
    /// Alternating headers carry `Some` post-merge fields (base fee, withdrawals root) while the
    /// rest leave them `None`, so the JSON header codec is exercised on exactly the `Option` mix
    /// that makes a positional BCS encoding of `ExecHeader` unsound.
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

    /// A valid 4-epoch (N=3) rotating chain whose header window covers epoch 3's first block.
    pub(crate) fn happy_spec() -> ChainSpec {
        let mut rng = StdRng::seed_from_u64(42);
        // rotating committees with a size change to exercise both.
        let sizes = [4usize, 4, 5, 4];
        let committees: Vec<Vec<Arc<BlsKeypair>>> =
            sizes.iter().map(|&s| sort_keys(gen_keys(s, &mut rng))).collect();
        let genesis = committee_of(&committees[0]);
        let n = committees.len() - 1;

        let window_start = 1000u64;
        let count = 6usize;
        let headers = build_headers(window_start, count);
        let last = SealedHeader::seal_slow(headers[count - 1].clone());
        let final_state_n = BlockNumHash::new(last.header().number, last.hash());

        // epoch n-1 must end one block before the window starts.
        let final_states = vec![
            BlockNumHash::new(99, B256::from([0x11u8; 32])),
            BlockNumHash::new(199, B256::from([0x12u8; 32])),
            BlockNumHash::new(window_start - 1, B256::from([0x13u8; 32])),
            final_state_n,
        ];
        let final_consensus: Vec<ConsensusNumHash> = (0..=n as u64)
            .map(|i| ConsensusNumHash::new(i * 10, B256::from([i as u8 + 8; 32]).into()))
            .collect();
        let next_committees: Vec<Vec<BlsPublicKey>> = (0..committees.len())
            .map(|k| {
                // epoch N's next_committee is unchecked by verify; reuse its own keys.
                let src = if k + 1 < committees.len() { k + 1 } else { k };
                pubkeys(&committees[src])
            })
            .collect();

        ChainSpec {
            committees,
            genesis,
            next_committees,
            parent_overrides: vec![None; sizes.len()],
            signer_counts: sizes.to_vec(),
            final_states,
            final_consensus,
            headers,
        }
    }

    /// Sign a `ChainSpec` into an ordered `(record, certificate)` chain.
    pub(crate) fn finish(spec: &ChainSpec) -> ChainParts {
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
            let positions: Vec<usize> = (0..spec.signer_counts[k]).collect();
            let cert = certify(&record, &spec.committees[k], &positions);
            records.push((record, cert));
        }
        ChainParts { genesis: spec.genesis.clone(), records, headers: spec.headers.clone() }
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex_lower(&Sha256::digest(bytes))
    }

    /// Compress `plain`, write it under `dir/name`, and return the manifest entry describing it.
    fn write_artifact(dir: &Path, name: &str, kind: ArtifactKind, plain: &[u8]) -> ArtifactEntry {
        let compressed = zstd::encode_all(plain, 3).expect("compress artifact");
        std::fs::write(dir.join(name), &compressed).expect("write artifact");
        ArtifactEntry {
            name: name.to_string(),
            kind,
            size: compressed.len() as u64,
            sha256: sha256_hex(&compressed),
        }
    }

    /// Encode the header window with the same codec [`decode_headers`] reads.
    fn encode_headers(headers: &[ExecHeader]) -> Vec<u8> {
        serde_json::to_vec(headers).expect("encode headers")
    }

    /// Stage all three artifacts and derive a manifest bound to the chain.
    pub(crate) fn assemble(parts: &ChainParts, chain_id: u64, genesis_hash: B256) -> Fixture {
        let dir = tempdir().unwrap();
        let staged = dir.path().to_path_buf();
        let n = parts.records.len() as u32 - 1;
        let last = &parts.records[n as usize].0;

        // opaque state bytes: verify checks integrity and decompresses, but never parses them.
        let state = write_artifact(
            &staged,
            "state-000.jsonl.zst",
            ArtifactKind::StateChunk,
            b"root-line\naccount-a\naccount-b\n",
        );
        let headers = write_artifact(
            &staged,
            "headers.rlp.zst",
            ArtifactKind::Headers,
            &encode_headers(&parts.headers),
        );
        let records = write_artifact(
            &staged,
            "epoch-records.bcs.zst",
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
        Fixture {
            _dir: dir,
            staged,
            manifest,
            genesis: parts.genesis.clone(),
            genesis_hash,
            chain_id,
        }
    }

    /// Assemble a valid fixture with a fixed chain id and genesis hash.
    fn happy_fixture() -> Fixture {
        assemble(&finish(&happy_spec()), 2017, B256::from([0xaau8; 32]))
    }

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

    #[track_caller]
    fn expect_verification(err: SnapshotError, fragment: &str) {
        match err {
            SnapshotError::Verification(msg) => {
                assert!(msg.contains(fragment), "expected {fragment:?} in Verification, got: {msg}")
            }
            other => panic!("expected Verification({fragment:?}), got {other:?}"),
        }
    }

    #[track_caller]
    fn expect_integrity(err: SnapshotError, fragment: &str) {
        match err {
            SnapshotError::Integrity(msg) => {
                assert!(msg.contains(fragment), "expected {fragment:?} in Integrity, got: {msg}")
            }
            other => panic!("expected Integrity({fragment:?}), got {other:?}"),
        }
    }

    #[track_caller]
    fn expect_manifest(err: SnapshotError, fragment: &str) {
        match err {
            SnapshotError::Manifest(msg) => {
                assert!(msg.contains(fragment), "expected {fragment:?} in Manifest, got: {msg}")
            }
            other => panic!("expected Manifest({fragment:?}), got {other:?}"),
        }
    }

    // ----- happy path -----

    #[test]
    fn accepts_a_valid_rotating_snapshot() {
        let fx = happy_fixture();
        let verified = run(&fx).expect("valid snapshot must verify");

        assert_eq!(verified.records.len(), 4);
        assert_eq!(verified.headers.len(), 6);
        assert_eq!(verified.state_chunks.len(), 1);
        // committees genuinely rotate between epochs.
        assert_ne!(verified.records[0].0.committee, verified.records[1].0.committee);
        assert_eq!(verified.records[1].0.committee, verified.records[0].0.next_committee);
        // the returned headers are sealed and terminate at the manifest checkpoint.
        let last = verified.headers.last().unwrap();
        assert_eq!(last.hash(), verified.manifest.final_state.hash);
        // the JSON header codec round-trips the Some/None optional-field mix (base fee) intact.
        assert!(verified.headers.iter().any(|h| h.header().base_fee_per_gas.is_some()));
        assert!(verified.headers.iter().any(|h| h.header().base_fee_per_gas.is_none()));
    }

    // ----- record-chain rejections -----

    #[test]
    fn rejects_broken_parent_hash() {
        let mut spec = happy_spec();
        // record 2's cert stays valid (signed over this digest), but the link to record 1 breaks.
        spec.parent_overrides[2] = Some(EpochDigest::from(B256::from([0xEEu8; 32])));
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "parent_hash does not match epoch 1 digest");
    }

    #[test]
    fn rejects_committee_handoff_mismatch() {
        let mut rng = StdRng::seed_from_u64(7);
        let mut spec = happy_spec();
        // epoch 1 hands off to an unrelated committee that is not epoch 2's committee.
        spec.next_committees[1] = pubkeys(&sort_keys(gen_keys(4, &mut rng)));
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "handoff mismatch");
    }

    #[test]
    fn rejects_sub_quorum_certificate() {
        let mut spec = happy_spec();
        // epoch 1 is a committee of 4, super_quorum = 3; two signers is short.
        spec.signer_counts[1] = 2;
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "epoch 1 failed certificate verification");
    }

    #[test]
    fn rejects_wrong_genesis_committee() {
        let mut rng = StdRng::seed_from_u64(99);
        let mut fx = happy_fixture();
        // a fresh, unrelated committee is not the chain's genesis committee.
        fx.genesis = committee_of(&sort_keys(gen_keys(4, &mut rng)));
        expect_verification(run(&fx).unwrap_err(), "trust-root mismatch");
    }

    #[test]
    fn rejects_wrong_epoch0_parent_hash() {
        let mut spec = happy_spec();
        spec.parent_overrides[0] = Some(EpochDigest::from(B256::from([0x01u8; 32])));
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "non-zero parent_hash");
    }

    #[test]
    fn foreign_chain_rejected_before_certificate_checks() {
        // garbage every certificate: were the BLS pass to run first, epoch 0's record would fail
        // with "certificate verification" before any structural check could speak.
        let mut parts = finish(&happy_spec());
        for (_record, cert) in &mut parts.records {
            cert.epoch_hash = EpochDigest::from(B256::from([0xEEu8; 32]));
        }
        let mut fx = assemble(&parts, 2017, B256::from([0xaau8; 32]));
        // a foreign trust root: the cheap epoch-0 anchor must fire before any pairing is computed.
        let mut rng = StdRng::seed_from_u64(123);
        fx.genesis = committee_of(&sort_keys(gen_keys(4, &mut rng)));
        expect_verification(run(&fx).unwrap_err(), "trust-root mismatch");
    }

    #[test]
    fn rejects_duplicate_committee_key() {
        let mut spec = happy_spec();
        // epoch 1's committee lists the same key twice. the dedup pass (2a) fires before the
        // handoff pass (2c), so the duplicate — not the handoff mismatch it also induces — is the
        // rejection, and a single physical validator cannot occupy two quorum slots.
        spec.committees[1][1] = spec.committees[1][0].clone();
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "in its committee");
    }

    #[test]
    fn rejects_duplicate_next_committee_key() {
        let mut spec = happy_spec();
        let n = spec.committees.len() - 1;
        // epoch N's next_committee is never checked by the handoff, but restore's
        // reconstruct_committee consumes it, so a duplicate there must still be rejected.
        spec.next_committees[n][1] = spec.next_committees[n][0];
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "in its next_committee");
    }

    #[test]
    fn rejects_non_monotonic_final_consensus() {
        // an equal consensus height between epochs 1 and 2 is not a strict increase.
        let mut spec = happy_spec();
        spec.final_consensus[2] = spec.final_consensus[1];
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "does not increase");

        // a strictly decreasing consensus height is likewise rejected.
        let mut spec = happy_spec();
        spec.final_consensus[2] = ConsensusNumHash::new(5, B256::from([0x22u8; 32]).into());
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "does not increase");
    }

    // ----- manifest binding rejections -----

    #[test]
    fn rejects_manifest_digest_binding_mismatch() {
        let mut fx = happy_fixture();
        fx.manifest.epoch_record_digest = EpochDigest::from(B256::from([0xEEu8; 32]));
        expect_verification(run(&fx).unwrap_err(), "epoch_record_digest does not match");
    }

    #[test]
    fn rejects_counts_records_mismatch() {
        let mut fx = happy_fixture();
        fx.manifest.counts.records = 99;
        expect_verification(run(&fx).unwrap_err(), "counts.records");
    }

    // ----- header-window rejections -----

    #[test]
    fn rejects_tampered_header_link() {
        let mut spec = happy_spec();
        // break the link between headers 2 and 3 without touching the terminal checkpoint.
        spec.headers[3].parent_hash = B256::from([0xEEu8; 32]);
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "does not link to its predecessor");
    }

    #[test]
    fn rejects_wrong_final_hash() {
        let mut spec = happy_spec();
        // mutate the terminal header's hash via a non-linking field after the checkpoint is fixed,
        // so the binding still holds but the recomputed window no longer ends at final_state.
        let last = spec.headers.len() - 1;
        spec.headers[last].gas_limit ^= 0xFFFF;
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "does not match manifest.final_state");
    }

    #[test]
    fn rejects_window_not_covering_epoch_start() {
        let mut spec = happy_spec();
        // pull epoch 2's final state back so epoch 3 begins before the window's first header.
        spec.final_states[2] = BlockNumHash::new(994, B256::from([0x13u8; 32]));
        let fx = assemble(&finish(&spec), 2017, B256::from([0xaau8; 32]));
        expect_verification(run(&fx).unwrap_err(), "after epoch 3 begins");
    }

    #[test]
    fn rejects_counts_headers_mismatch() {
        let mut fx = happy_fixture();
        fx.manifest.counts.headers = 3;
        expect_verification(run(&fx).unwrap_err(), "counts.headers");
    }

    // ----- artifact / manifest rejections -----

    #[test]
    fn rejects_artifact_sha256_mismatch() {
        let fx = happy_fixture();
        // flip a byte of a staged file without changing its length; size passes, sha256 fails.
        let path = fx.staged.join("state-000.jsonl.zst");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        expect_integrity(run(&fx).unwrap_err(), "sha256 mismatch");
    }

    #[test]
    fn rejects_missing_artifact_file() {
        let fx = happy_fixture();
        std::fs::remove_file(fx.staged.join("epoch-records.bcs.zst")).unwrap();
        expect_integrity(run(&fx).unwrap_err(), "is missing from the staged directory");
    }

    #[test]
    fn rejects_epoch0_snapshot() {
        let mut fx = happy_fixture();
        fx.manifest.epoch = 0;
        expect_verification(run(&fx).unwrap_err(), "epoch-0 snapshots are unsupported");
    }

    #[test]
    fn rejects_chain_id_mismatch() {
        let mut fx = happy_fixture();
        fx.chain_id = 1;
        expect_manifest(run(&fx).unwrap_err(), "chain_id mismatch");
    }

    #[test]
    fn rejects_genesis_hash_mismatch() {
        let mut fx = happy_fixture();
        fx.genesis_hash = B256::from([0xBBu8; 32]);
        expect_manifest(run(&fx).unwrap_err(), "genesis_hash mismatch");
    }

    #[test]
    fn rejects_decompression_bomb() {
        let fx = happy_fixture();
        // a 1-byte cap forces the header artifact over its limit while decompressing.
        let tight = DecompressLimits { records: 1, headers: 1, state_chunk: 1 };
        let err = verify_snapshot(
            &fx.manifest,
            &fx.staged,
            &fx.genesis,
            fx.genesis_hash,
            fx.chain_id,
            tight,
        )
        .unwrap_err();
        expect_integrity(err, "exceeds cap");
    }
}
