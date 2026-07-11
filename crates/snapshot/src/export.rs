//! Packaging local chain data into an uploadable snapshot.
//!
//! [`export_epoch`] turns a node's on-disk state for one closed epoch `N` into a set of
//! integrity-hashed artifact files staged under a local directory, plus a [`Manifest`] describing
//! them. It does NOT upload anything — staging and uploading are deliberately separate so the same
//! function serves both the background uploader service and an offline `create` CLI. The caller
//! pins the state view at the boundary quiet point and hands it in; export streams from that pin
//! and never re-pins.
//!
//! # Staging layout
//!
//! Everything lands in `<staging_root>/epoch-<N zero-padded 10>/` (the same naming
//! [`SnapshotStore::epoch_dir`] uses for bucket keys). The directory is wiped and recreated at the
//! start of every run so a partial prior attempt cannot leak stale files into a fresh snapshot. On
//! success it holds exactly the artifact files plus `manifest.json`.
//!
//! # Artifact byte formats (the verify/restore side decodes from this exact spec)
//!
//! 1. **State** — `state-000.jsonl.zst`, `state-001.jsonl.zst`, … The reth init-state JSONL stream
//!    produced by [`PinnedStateView::export_state_jsonl`], split at LINE boundaries into chunks of
//!    ~[`STATE_CHUNK_TARGET_UNCOMPRESSED`] uncompressed bytes each, every chunk zstd-compressed.
//!    The state-root line is the first line of chunk 0 only; concatenating the decompressed chunks
//!    reproduces the dump byte stream exactly. Streamed through [`StateChunkWriter`] so no whole
//!    chunk is ever buffered in memory.
//! 2. **Headers** — `headers.json.zst`. The window headers as one `serde_json` array of
//!    [`ExecHeader`] (plain headers, no hashes — the verifier recomputes each hash via
//!    `hash_slow`), zstd-compressed.
//! 3. **Records** — `epoch-records.bcs.zst`. `Vec<(EpochRecord, EpochCertificate)>` for epochs
//!    `0..=N` in order, encoded with [`tn_types::encode`] (BCS — the same canonical codec the
//!    [`EpochRecordDb`] pack files persist with), zstd-compressed.
//!
//! Each [`ArtifactEntry`]'s `sha256` is the lowercase-hex digest of the COMPRESSED file as staged,
//! and its `size` is that file's compressed byte length — both computed while the bytes are
//! written, with no second pass over the file.
//!
//! # Why headers are JSON rather than RLP
//!
//! The natural on-the-wire encoding for a header list is `alloy_rlp::encode(&Vec<ExecHeader>)`, but
//! `alloy-rlp` is not reachable from this crate (it is neither a direct dependency nor re-exported
//! by `tn-types`/`tn-reth`), and a raw BCS encoding of alloy's [`ExecHeader`] is unsound: the type
//! carries `#[serde(skip_serializing_if)]` `Option` fields, and skipping a field corrupts a
//! non-self-describing format like BCS (this is exactly why alloy ships a separate
//! `serde_bincode_compat::Header`). `serde_json` is reachable and is the header's native serde, so
//! it round-trips every field, including the optional ones, without a compatibility shim.
//!
//! # Fee derivability
//!
//! A restored node, entering epoch `N+1`, re-derives each worker's EIP-1559 base fee from epoch
//! `N`'s genuine blocks. A worker with an EIP-1559 config that produced no genuine block in epoch
//! `N` has no chain-observable anchor and would make the node walk into pre-snapshot history it
//! does not have. [`export_epoch`] records whether every such worker is covered in
//! [`Manifest::fee_derivable`] and surfaces the same flag on [`ExportedSnapshot`] so the service
//! can skip publishing an un-derivable epoch while the CLI can still report it.

use crate::{
    manifest::{ArtifactEntry, ArtifactKind, Counts, Manifest, FORMAT_VERSION, KIND_STATE_ONLY},
    store::SnapshotStore,
    SnapshotError, SnapshotResult,
};
use eyre::WrapErr as _;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};
use tn_reth::{
    is_worker_batch_block, worker_id_from_header, PinnedStateView, RethEnv, StateExportStats,
};
use tn_storage::epoch_records::EpochRecordDb;
use tn_types::{
    encode, gas_accumulator::WorkerFeeConfig, hex, BlockNumHash, Epoch, EpochCertificate,
    EpochRecord, ExecHeader, SealedHeader, WorkerId,
};
use tracing::info;

/// Target uncompressed size of a single state chunk, in bytes (~256 MiB).
///
/// [`StateChunkWriter`] rotates to a new chunk once the active chunk crosses this many uncompressed
/// bytes AND reaches a line boundary, so a chunk may run slightly over when a single line straddles
/// the threshold. The value trades chunk count against per-chunk memory during download/verify.
pub const STATE_CHUNK_TARGET_UNCOMPRESSED: u64 = 256 * 1024 * 1024;

/// Minimum number of blocks below `B` the shipped header window must reach when epoch `N` is short.
///
/// The window always covers all of epoch `N` (so the restored node's base-fee scan is
/// self-sufficient), but a very short epoch would ship too few headers to satisfy other backward
/// reads (e.g. `BLOCKHASH`). Extending down to at least this depth keeps a useful history tail
/// without shipping the whole chain.
pub const HEADER_WINDOW_MIN_DEPTH: u64 = 1024;

/// zstd compression level for staged artifacts; a fast default suited to write-once snapshots.
const ZSTD_LEVEL: i32 = 3;

/// Filename prefix for state chunk artifacts.
const STATE_CHUNK_PREFIX: &str = "state";

/// Object name of the headers artifact within an epoch directory.
const HEADERS_ARTIFACT_NAME: &str = "headers.json.zst";

/// Object name of the epoch-records artifact within an epoch directory.
const RECORDS_ARTIFACT_NAME: &str = "epoch-records.bcs.zst";

/// Filename of the manifest staged alongside the artifacts.
const MANIFEST_FILENAME: &str = "manifest.json";

/// Inputs for [`export_epoch`].
///
/// The reth environment, epoch record, and epoch-records database are borrowed; the pinned state
/// view is taken by value so `export_epoch` owns it and drops it promptly once the state dump
/// finishes, releasing the MDBX reader (and letting the environment reclaim retained pages). The
/// service pins the view at the boundary quiet point and moves it in here — export never re-pins.
#[derive(Debug)]
pub struct ExportArgs<'a> {
    /// Live execution environment, used for the read-only header, registry, and worker-config
    /// lookups the export needs.
    pub reth_env: &'a RethEnv,
    /// The state view pinned at the epoch boundary. Consumed and dropped by `export_epoch`.
    pub pinned_state: PinnedStateView,
    /// The signed [`EpochRecord`] of the closed epoch `N` being exported. Supplies the final
    /// execution and consensus checkpoints and the epoch number.
    pub epoch_record: &'a EpochRecord,
    /// Source of the `(EpochRecord, EpochCertificate)` chain for epochs `0..=N`.
    pub epoch_records: &'a EpochRecordDb,
    /// Directory under which the per-epoch staging directory is created.
    pub staging_root: &'a Path,
    /// EVM chain id, pinned into the manifest so a downloader can reject a foreign chain.
    pub chain_id: u64,
    /// Genesis block hash, pinned into the manifest as the chain-genesis binding.
    pub genesis_hash: tn_types::B256,
    /// Version string of the node producing the snapshot.
    pub node_version: String,
    /// Manifest creation timestamp (Unix seconds), supplied by the caller for deterministic
    /// output.
    pub created_at: u64,
}

/// Export the closed epoch `N` into a locally-staged, integrity-hashed snapshot.
///
/// Stages the state, header, and epoch-record artifacts plus a `manifest.json` under
/// `<staging_root>/epoch-<N>/` and returns an [`ExportedSnapshot`] describing them. The heavy state
/// dump streams from the pinned view straight into rotating zstd chunk files; the small header and
/// record artifacts are encoded in memory and compressed. This does not upload — the caller uploads
/// the returned files.
///
/// The cheap validations (registry read, header-window contiguity, fee-derivability, record
/// completeness) run BEFORE the expensive state dump so a malformed input fails fast.
///
/// # Determinism
///
/// Given the same chain state and the same `created_at`/`node_version`, the artifact bytes and
/// manifest are byte-identical across runs: the state dump order is fixed by the plain-state cursor
/// walk, the header window is a contiguous range, and the record list is ordered by epoch.
///
/// # Errors
///
/// Returns an error if the header window is missing, non-contiguous, or does not end at the closed
/// epoch's final block; if the registry/worker-config reads fail; if any `(EpochRecord,
/// EpochCertificate)` in `0..=N` is missing from the database; if the database's epoch-`N` record
/// disagrees with the supplied one; or on any staging I/O failure.
pub async fn export_epoch(args: ExportArgs<'_>) -> SnapshotResult<ExportedSnapshot> {
    let ExportArgs {
        reth_env,
        pinned_state,
        epoch_record,
        epoch_records,
        staging_root,
        chain_id,
        genesis_hash,
        node_version,
        created_at,
    } = args;

    let epoch = epoch_record.epoch;
    let final_state = epoch_record.final_state;
    let final_consensus = epoch_record.final_consensus;
    let b = final_state.number;
    let b_hash = final_state.hash;

    // wipe and recreate the per-epoch staging dir so a partial prior run cannot leak stale files
    let staging_dir = staging_root.join(SnapshotStore::epoch_dir(epoch));
    if staging_dir.exists() {
        std::fs::remove_dir_all(&staging_dir)?;
    }
    std::fs::create_dir_all(&staging_dir)?;

    // epoch N's first block, read from the registry AT the closing block B (the ring buffer holds
    // the four most recent epochs, so N is always resolvable here). this mirrors how the epoch
    // manager recovers the range it scans for entered-epoch base fees.
    let epoch_info = reth_env
        .get_epoch_info_at_block(epoch, b_hash)
        .wrap_err("snapshot export: reading epoch info at the closing block")?;
    // clamp to 1: constructor-seeded epochs report blockHeight 0, and genesis is never shipped.
    let epoch_start = epoch_info.blockHeight.max(1);
    // extend the window at least HEADER_WINDOW_MIN_DEPTH below B when the epoch is short.
    let window_start = epoch_start.min(b.saturating_sub(HEADER_WINDOW_MIN_DEPTH)).max(1);

    // collect and validate the contiguous header window ending at B
    let window = reth_env
        .blocks_for_range(window_start..=b)
        .wrap_err("snapshot export: reading the header window")?;
    validate_window(&window, window_start, final_state)?;
    let b_header = window.last().expect("validate_window guarantees a non-empty window");
    let state_root = b_header.state_root;

    // fee-derivability precheck over the epoch-N portion of the window ([epoch_start..=B]); the
    // extended tail below epoch_start does not participate in epoch-N fee attribution.
    let (_num_workers, configs) = reth_env
        .get_worker_fee_configs_at_block(b_hash)
        .wrap_err("snapshot export: reading worker fee configs at the closing block")?;
    let producers = worker_producers_in_epoch(&window, epoch_start);
    let fee_derivable = fee_derivable_for(&configs, &producers);

    // read the full record/cert chain 0..=N and confirm record N agrees with the supplied one
    let records = read_epoch_records(epoch_records, epoch).await?;
    match records.last() {
        Some((record_n, _)) if record_n.digest() == epoch_record.digest() => {}
        Some(_) => {
            return Err(export_error(format!(
                "epoch record {epoch} in the database does not match the supplied closed-epoch \
                 record"
            )))
        }
        None => return Err(export_error(format!("no epoch records found for epoch {epoch}"))),
    }

    // stream the state dump into rotating zstd chunk files, hashing each chunk as it is written
    let mut state_writer =
        StateChunkWriter::new(staging_dir.clone(), STATE_CHUNK_TARGET_UNCOMPRESSED, ZSTD_LEVEL);
    let stats = pinned_state
        .export_state_jsonl(state_root, &mut state_writer)
        .wrap_err("snapshot export: streaming the plain-state dump")?;
    let state_chunks = state_writer.finish()?;
    // release the mdbx pin promptly now that the dump is complete
    drop(pinned_state);

    // headers artifact: json array of plain execution headers (see module docs on why not rlp)
    let headers: Vec<ExecHeader> = window.iter().map(|header| header.header().clone()).collect();
    let headers_bytes = serde_json::to_vec(&headers)?;
    let (headers_size, headers_sha) =
        stage_zstd_artifact(&staging_dir, HEADERS_ARTIFACT_NAME, &headers_bytes)?;

    // records artifact: bcs-encoded (record, cert) chain, the same codec the epoch pack files use
    let records_bytes = encode(&records);
    let (records_size, records_sha) =
        stage_zstd_artifact(&staging_dir, RECORDS_ARTIFACT_NAME, &records_bytes)?;

    // assemble the artifact list (state chunks first, then headers, then records) and the file list
    let mut artifacts = Vec::with_capacity(state_chunks.len() + 2);
    let mut artifact_files = Vec::with_capacity(state_chunks.len() + 2);
    for chunk in &state_chunks {
        artifacts.push(ArtifactEntry {
            name: chunk.name.clone(),
            kind: ArtifactKind::StateChunk,
            size: chunk.compressed,
            sha256: chunk.sha256.clone(),
        });
        artifact_files.push(staging_dir.join(&chunk.name));
    }
    artifacts.push(ArtifactEntry {
        name: HEADERS_ARTIFACT_NAME.to_string(),
        kind: ArtifactKind::Headers,
        size: headers_size,
        sha256: headers_sha,
    });
    artifact_files.push(staging_dir.join(HEADERS_ARTIFACT_NAME));
    artifacts.push(ArtifactEntry {
        name: RECORDS_ARTIFACT_NAME.to_string(),
        kind: ArtifactKind::EpochRecords,
        size: records_size,
        sha256: records_sha,
    });
    artifact_files.push(staging_dir.join(RECORDS_ARTIFACT_NAME));

    let counts = Counts {
        accounts: stats.accounts,
        storage_slots: stats.storage_slots,
        bytecodes: stats.bytecodes,
        headers: window.len() as u64,
        records: records.len() as u64,
    };

    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        kind: KIND_STATE_ONLY.to_string(),
        chain_id,
        genesis_hash,
        epoch,
        final_state,
        final_consensus,
        epoch_record_digest: epoch_record.digest(),
        created_at,
        node_version,
        artifacts,
        counts,
        fee_derivable,
    };

    // to_json validates the manifest, so a staged snapshot can never carry one from_json rejects
    let manifest_bytes = manifest.to_json()?;
    std::fs::write(staging_dir.join(MANIFEST_FILENAME), &manifest_bytes)?;

    info!(
        target: "tn::snapshot",
        epoch,
        block = b,
        state_chunks = state_chunks.len(),
        accounts = stats.accounts,
        storage_slots = stats.storage_slots,
        headers = window.len(),
        records = records.len(),
        fee_derivable,
        "staged epoch snapshot"
    );

    Ok(ExportedSnapshot { manifest, staging_dir, artifacts: artifact_files, fee_derivable, stats })
}

/// The result of a successful [`export_epoch`]: the staged manifest, where it lives, and the facts
/// a caller needs to decide whether and what to upload.
#[derive(Debug, Clone)]
pub struct ExportedSnapshot {
    /// The assembled manifest, already written to `manifest.json` in
    /// [`staging_dir`](Self::staging_dir).
    pub manifest: Manifest,
    /// The per-epoch staging directory holding every artifact plus `manifest.json`.
    pub staging_dir: PathBuf,
    /// Absolute paths of the staged artifact files (state chunks, headers, records), in manifest
    /// order. Excludes `manifest.json`, which the uploader publishes last.
    pub artifacts: Vec<PathBuf>,
    /// Whether every EIP-1559 worker has a chain-observable base-fee anchor in epoch `N`. When
    /// `false` the service should skip publishing this epoch; a restored node would otherwise halt
    /// deriving base fees. Mirrors [`Manifest::fee_derivable`].
    pub fee_derivable: bool,
    /// Aggregate counts from the state dump (accounts, storage slots, bytecodes, byte total).
    pub stats: StateExportStats,
}

/// Read the `(EpochRecord, EpochCertificate)` chain for epochs `0..=final_epoch` in order.
///
/// The service only exports after waiting for epoch `N`'s certificate, so every record and its
/// certificate must be present; a missing entry is an error naming the offending epoch.
async fn read_epoch_records(
    db: &EpochRecordDb,
    final_epoch: Epoch,
) -> SnapshotResult<Vec<(EpochRecord, EpochCertificate)>> {
    let mut chain = Vec::with_capacity(final_epoch as usize + 1);
    for epoch in 0..=final_epoch {
        let record = db.record_by_epoch(epoch).await.ok_or_else(|| {
            export_error(format!("epoch record {epoch} is missing from the epoch-records database"))
        })?;
        let cert = db.cert_by_digest(record.digest()).await.ok_or_else(|| {
            export_error(format!(
                "epoch certificate for epoch {epoch} is missing from the epoch-records database"
            ))
        })?;
        chain.push((record, cert));
    }
    Ok(chain)
}

/// Validate that `window` is a contiguous, ascending run of headers from `expected_start` up to and
/// including `final_state`.
fn validate_window(
    window: &[SealedHeader],
    expected_start: u64,
    final_state: BlockNumHash,
) -> SnapshotResult<()> {
    let first = window.first().ok_or_else(|| {
        export_error(format!("empty header window for final block {}", final_state.number))
    })?;
    if first.number != expected_start {
        return Err(export_error(format!(
            "header window starts at block {}, expected {expected_start}",
            first.number
        )));
    }
    for (i, header) in window.iter().enumerate() {
        let expected = expected_start + i as u64;
        if header.number != expected {
            return Err(export_error(format!(
                "header window is not contiguous at index {i}: expected block {expected}, got {}",
                header.number
            )));
        }
    }
    let last = window.last().expect("non-empty window checked above");
    if last.number != final_state.number || last.hash() != final_state.hash {
        return Err(export_error(format!(
            "header window tip {}:{} does not match the final block {}:{}",
            last.number,
            last.hash(),
            final_state.number,
            final_state.hash
        )));
    }
    Ok(())
}

/// The set of worker ids that produced a genuine batch block within the epoch-`N` portion of the
/// window (blocks at or above `epoch_start`).
///
/// The window may reach below `epoch_start` for history-tail reasons, but only epoch `N`'s own
/// genuine blocks anchor epoch `N`'s base fees, so blocks below `epoch_start` are excluded.
fn worker_producers_in_epoch(window: &[SealedHeader], epoch_start: u64) -> BTreeSet<WorkerId> {
    window
        .iter()
        .filter(|header| header.number >= epoch_start && is_worker_batch_block(header))
        .map(worker_id_from_header)
        .collect()
}

/// Whether every EIP-1559-configured worker has a genuine block among `producers`.
///
/// `Static` workers need no chain anchor — their fee is pinned by config — so they never block
/// derivability. Returns `true` only when no EIP-1559 worker is missing.
fn fee_derivable_for(configs: &[WorkerFeeConfig], producers: &BTreeSet<WorkerId>) -> bool {
    configs.iter().enumerate().all(|(worker_id, config)| {
        !matches!(config, WorkerFeeConfig::Eip1559 { .. })
            || producers.contains(&(worker_id as WorkerId))
    })
}

/// Compress `raw` with zstd into `dir/name`, returning the staged file's `(compressed_size,
/// sha256_hex)`.
///
/// Hashes the compressed bytes as they stream to disk, so the returned digest and size describe the
/// on-disk file exactly, with no second read. The digest is lowercase hex.
fn stage_zstd_artifact(dir: &Path, name: &str, raw: &[u8]) -> SnapshotResult<(u64, String)> {
    let file = File::create(dir.join(name))?;
    let hashing = HashingWriter::new(BufWriter::new(file));
    let mut encoder = zstd::Encoder::new(hashing, ZSTD_LEVEL)?;
    encoder.write_all(raw)?;
    let mut hashing = encoder.finish()?;
    hashing.flush()?;
    Ok(hashing.finalize())
}

/// Build a [`SnapshotError::Other`] carrying an export-specific message.
fn export_error(message: String) -> SnapshotError {
    SnapshotError::Other(eyre::eyre!("snapshot export: {message}"))
}

/// A rotating [`Write`] sink that splits an incoming JSONL byte stream into zstd-compressed chunk
/// files, rotating only at line boundaries once a chunk crosses a size target.
///
/// It is fed the exact byte stream [`PinnedStateView::export_state_jsonl`] writes (the state-root
/// line followed by one line per account) and never buffers a whole chunk in memory: bytes flow
/// straight through a per-chunk zstd encoder into a hashing file writer. Rotation happens only when
/// the active chunk's uncompressed size has reached the target AND the stream is at a line
/// boundary, so a decoded chunk always ends on a complete line and concatenating the decoded chunks
/// reproduces the input byte-for-byte. The state-root line, written first, therefore lands in chunk
/// 0 only.
struct StateChunkWriter {
    /// Directory the chunk files are created in.
    dir: PathBuf,
    /// Uncompressed-byte size at which the active chunk becomes eligible to rotate.
    threshold: u64,
    /// zstd compression level for each chunk.
    level: i32,
    /// Index of the next chunk file to open (also the count of chunks opened so far).
    next_index: usize,
    /// The chunk currently being written, if any.
    active: Option<ActiveChunk>,
    /// True when the last byte written was a newline (or nothing has been written yet), i.e. the
    /// stream is between lines and rotation is allowed.
    at_line_start: bool,
    /// Metadata for chunks already sealed, in order.
    finished: Vec<StateChunkMeta>,
}

impl StateChunkWriter {
    /// Create a writer that stages chunks into `dir`, rotating past `threshold` uncompressed bytes.
    fn new(dir: PathBuf, threshold: u64, level: i32) -> Self {
        Self {
            dir,
            threshold,
            level,
            next_index: 0,
            active: None,
            at_line_start: true,
            finished: Vec::new(),
        }
    }

    /// Ensure an active chunk exists, opening the next chunk file if necessary.
    fn ensure_active(&mut self) -> io::Result<&mut ActiveChunk> {
        if self.active.is_none() {
            let name = format!("{STATE_CHUNK_PREFIX}-{:03}.jsonl.zst", self.next_index);
            let file = File::create(self.dir.join(&name))?;
            let encoder = zstd::Encoder::new(HashingWriter::new(BufWriter::new(file)), self.level)?;
            self.active = Some(ActiveChunk { name, encoder, uncompressed: 0 });
            self.next_index += 1;
        }
        Ok(self.active.as_mut().expect("active chunk just set"))
    }

    /// Finalize the active chunk (if any), recording its size and digest.
    fn seal_active(&mut self) -> io::Result<()> {
        // the active chunk's `uncompressed` tally drives rotation but is not needed once sealed
        if let Some(ActiveChunk { name, encoder, .. }) = self.active.take() {
            // finish flushes the final zstd frame into the hashing writer, then flush pushes the
            // buffered compressed bytes to disk; the digest already covers every compressed byte.
            let mut hashing = encoder.finish()?;
            hashing.flush()?;
            let (compressed, sha256) = hashing.finalize();
            self.finished.push(StateChunkMeta { name, compressed, sha256 });
        }
        Ok(())
    }

    /// Seal the final chunk and return every chunk's metadata in order.
    fn finish(mut self) -> io::Result<Vec<StateChunkMeta>> {
        self.seal_active()?;
        Ok(self.finished)
    }
}

impl Write for StateChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut start = 0;
        while start < buf.len() {
            // rotate only between lines, and only once the active chunk has reached the target
            let active_len = self.active.as_ref().map_or(0, |chunk| chunk.uncompressed);
            if self.at_line_start && active_len >= self.threshold {
                self.seal_active()?;
            }

            match buf[start..].iter().position(|&byte| byte == b'\n') {
                Some(rel) => {
                    // write through and including the newline, landing at a line boundary
                    let end = start + rel + 1;
                    let chunk = self.ensure_active()?;
                    chunk.encoder.write_all(&buf[start..end])?;
                    chunk.uncompressed += (end - start) as u64;
                    self.at_line_start = true;
                    start = end;
                }
                None => {
                    // no newline in the remainder: write it all, still mid-line
                    let chunk = self.ensure_active()?;
                    chunk.encoder.write_all(&buf[start..])?;
                    chunk.uncompressed += (buf.len() - start) as u64;
                    self.at_line_start = false;
                    start = buf.len();
                }
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // chunk frames are finalized in seal_active/finish; there is nothing to flush mid-stream
        // without inserting a zstd flush block, so this is intentionally a no-op.
        Ok(())
    }
}

/// The chunk [`StateChunkWriter`] is currently writing into.
struct ActiveChunk {
    /// File name of this chunk within the staging directory.
    name: String,
    /// zstd encoder writing compressed bytes into a hashing file writer.
    encoder: zstd::Encoder<'static, HashingWriter<BufWriter<File>>>,
    /// Uncompressed bytes written into this chunk so far.
    uncompressed: u64,
}

/// Size and digest of one sealed state chunk.
#[derive(Clone)]
struct StateChunkMeta {
    /// File name of the chunk within the staging directory.
    name: String,
    /// Compressed (on-disk) size of the chunk.
    compressed: u64,
    /// Lowercase-hex sha256 of the compressed chunk bytes.
    sha256: String,
}

/// A [`Write`] adapter that sha256-hashes and counts every byte it forwards.
///
/// Used to derive an artifact's compressed size and digest in a single pass, without a second read
/// of the staged file.
struct HashingWriter<W> {
    /// The wrapped writer.
    inner: W,
    /// Running sha256 over every byte forwarded to `inner`.
    hasher: Sha256,
    /// Count of bytes forwarded to `inner`.
    bytes: u64,
}

impl<W: Write> HashingWriter<W> {
    /// Wrap `inner` with a fresh hasher and zeroed counter.
    fn new(inner: W) -> Self {
        Self { inner, hasher: Sha256::new(), bytes: 0 }
    }

    /// Consume the writer and return `(bytes_written, lowercase_hex_sha256)`.
    fn finalize(self) -> (u64, String) {
        (self.bytes, hex::encode(self.hasher.finalize()))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        // hash exactly what was forwarded, so a short write cannot double-count on the retry
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::{decode, BlsKeypair, ConsensusNumHash, EpochDigest, B256, U256};

    /// Decompress a staged `.zst` file into its raw bytes.
    fn decompress(path: &Path) -> Vec<u8> {
        zstd::decode_all(File::open(path).expect("open chunk")).expect("decode chunk")
    }

    /// Independently sha256 a file's bytes as lowercase hex.
    fn sha256_file(path: &Path) -> String {
        let bytes = std::fs::read(path).expect("read file");
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        hex::encode(hasher.finalize())
    }

    /// A synthetic sealed header with a distinct number and an optional base fee, exercising the
    /// `Option` fields that make raw BCS unsound but JSON safe.
    fn header(number: u64, base_fee: Option<u64>) -> ExecHeader {
        ExecHeader {
            number,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            base_fee_per_gas: base_fee,
            difficulty: U256::from(number),
            ..Default::default()
        }
    }

    /// A synthetic sealed header, marked as a genuine batch block for `worker_id` when `genuine`.
    fn sealed(number: u64, worker_id: u16, genuine: bool) -> SealedHeader {
        SealedHeader::seal_slow(ExecHeader {
            number,
            difficulty: U256::from(worker_id as u64),
            ommers_hash: if genuine { B256::repeat_byte(0xbb) } else { B256::ZERO },
            ..Default::default()
        })
    }

    #[test]
    fn state_chunk_writer_rotates_only_at_line_boundaries() {
        let dir = tempfile::tempdir().unwrap();

        // synthetic jsonl: a root line then several account-like lines, none containing newlines
        let mut input = Vec::new();
        input.extend_from_slice(b"{\"root\":\"0xabc\"}\n");
        for i in 0..40 {
            input.extend_from_slice(
                format!("{{\"address\":\"0x{i:02x}\",\"balance\":{i}}}\n").as_bytes(),
            );
        }

        // a small threshold forces multiple chunks; feed the stream in awkward slices that split
        // lines across write() calls to prove rotation still only happens at newlines
        let mut writer = StateChunkWriter::new(dir.path().to_path_buf(), 64, ZSTD_LEVEL);
        let mut offset = 0;
        for step in [7usize, 3, 50, 1, 200, 5] {
            let end = (offset + step).min(input.len());
            writer.write_all(&input[offset..end]).unwrap();
            offset = end;
            if offset == input.len() {
                break;
            }
        }
        writer.write_all(&input[offset..]).unwrap();
        let chunks = writer.finish().unwrap();

        assert!(chunks.len() > 1, "small threshold must produce multiple chunks");

        // per-chunk size and digest match the staged files, and each decoded chunk ends on a line
        let mut concatenated = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            let expected = format!("state-{i:03}.jsonl.zst");
            assert_eq!(chunk.name, expected, "chunk names are zero-padded and sequential");
            let path = dir.path().join(&chunk.name);
            let raw = decompress(&path);
            assert_eq!(
                chunk.compressed,
                std::fs::metadata(&path).unwrap().len(),
                "compressed size"
            );
            assert_eq!(chunk.sha256, sha256_file(&path), "digest matches the staged file");
            assert_eq!(raw.last(), Some(&b'\n'), "each chunk ends on a complete line");
            concatenated.extend_from_slice(&raw);
        }

        // the concatenation reproduces the input exactly, and the root line is in chunk 0 only
        assert_eq!(concatenated, input, "decoded chunks concatenate to the exact input");
        let chunk0 = decompress(&dir.path().join(&chunks[0].name));
        assert!(chunk0.starts_with(b"{\"root\":\"0xabc\"}\n"), "root line opens chunk 0");
        for chunk in &chunks[1..] {
            let raw = decompress(&dir.path().join(&chunk.name));
            assert!(!raw.starts_with(b"{\"root\""), "the root line appears in chunk 0 only");
        }
    }

    #[test]
    fn state_chunk_writer_single_chunk_for_small_input() {
        let dir = tempfile::tempdir().unwrap();
        let input = b"{\"root\":\"0x00\"}\n{\"address\":\"0x01\"}\n".to_vec();

        let mut writer = StateChunkWriter::new(
            dir.path().to_path_buf(),
            STATE_CHUNK_TARGET_UNCOMPRESSED,
            ZSTD_LEVEL,
        );
        writer.write_all(&input).unwrap();
        let chunks = writer.finish().unwrap();

        assert_eq!(chunks.len(), 1, "input below the threshold stays in one chunk");
        assert_eq!(decompress(&dir.path().join(&chunks[0].name)), input);
    }

    #[test]
    fn headers_artifact_round_trips_through_json_zstd() {
        let dir = tempfile::tempdir().unwrap();
        // mix Some/None base fees to exercise the skip_serializing_if fields that break raw BCS
        let headers = vec![header(1, Some(7)), header(2, None), header(3, Some(1_000_000_000))];

        let bytes = serde_json::to_vec(&headers).unwrap();
        let (size, sha) = stage_zstd_artifact(dir.path(), HEADERS_ARTIFACT_NAME, &bytes).unwrap();

        let path = dir.path().join(HEADERS_ARTIFACT_NAME);
        assert_eq!(size, std::fs::metadata(&path).unwrap().len(), "reported size matches the file");
        assert_eq!(sha, sha256_file(&path), "reported digest matches the file");

        let decoded: Vec<ExecHeader> = serde_json::from_slice(&decompress(&path)).unwrap();
        assert_eq!(decoded, headers, "headers survive json + zstd unchanged");
    }

    #[test]
    fn records_artifact_round_trips_through_bcs_zstd() {
        let dir = tempfile::tempdir().unwrap();

        // build a couple of records with distinct fields. the certificate half of the shipped tuple
        // holds a roaring bitmap that is not constructible from this crate's dependency set, so the
        // (record, cert) tuple's BCS round-trip is covered by tn-storage's own epoch-records tests
        // (same codec); here we exercise the encode + zstd wrapper over the record payload itself.
        let mut rng = StdRng::from_seed([7u8; 32]);
        let key = BlsKeypair::generate(&mut rng);
        let pubkey = *key.public();
        let records = vec![
            EpochRecord {
                epoch: 0,
                committee: vec![pubkey],
                next_committee: vec![pubkey],
                parent_hash: EpochDigest::default(),
                final_state: BlockNumHash::new(10, B256::repeat_byte(0x11)),
                final_consensus: ConsensusNumHash::new(3, B256::repeat_byte(0x22).into()),
            },
            EpochRecord {
                epoch: 1,
                committee: vec![pubkey],
                next_committee: vec![pubkey],
                parent_hash: EpochDigest::from(B256::repeat_byte(0x33)),
                final_state: BlockNumHash::new(20, B256::repeat_byte(0x44)),
                final_consensus: ConsensusNumHash::new(6, B256::repeat_byte(0x55).into()),
            },
        ];

        let bytes = encode(&records);
        let (size, sha) = stage_zstd_artifact(dir.path(), RECORDS_ARTIFACT_NAME, &bytes).unwrap();

        let path = dir.path().join(RECORDS_ARTIFACT_NAME);
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());
        assert_eq!(sha, sha256_file(&path));

        let decoded: Vec<EpochRecord> = decode(&decompress(&path));
        assert_eq!(decoded, records, "records survive bcs + zstd unchanged");
    }

    #[test]
    fn hashing_writer_reports_size_and_digest_of_forwarded_bytes() {
        let payload = b"the quick brown fox".repeat(1000);
        let mut sink = HashingWriter::new(Vec::new());
        sink.write_all(&payload).unwrap();
        let (size, sha) = sink.finalize();

        assert_eq!(size, payload.len() as u64);
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        assert_eq!(sha, hex::encode(hasher.finalize()));
    }

    #[test]
    fn validate_window_accepts_a_contiguous_run_ending_at_b() {
        let window: Vec<SealedHeader> = (5..=8).map(|n| sealed(n, 0, true)).collect();
        let final_state = BlockNumHash::new(8, window[3].hash());
        assert!(validate_window(&window, 5, final_state).is_ok());
    }

    #[test]
    fn validate_window_rejects_gaps_wrong_start_and_wrong_tip() {
        let good: Vec<SealedHeader> = (5..=8).map(|n| sealed(n, 0, true)).collect();
        let final_state = BlockNumHash::new(8, good[3].hash());

        // wrong start
        assert!(validate_window(&good, 4, final_state).is_err());

        // non-contiguous: drop the middle block
        let gapped = vec![good[0].clone(), good[1].clone(), good[3].clone()];
        assert!(validate_window(&gapped, 5, BlockNumHash::new(8, good[3].hash())).is_err());

        // tip hash mismatch
        let wrong_tip = BlockNumHash::new(8, B256::repeat_byte(0xff));
        assert!(validate_window(&good, 5, wrong_tip).is_err());

        // empty window
        assert!(validate_window(&[], 5, final_state).is_err());
    }

    #[test]
    fn worker_producers_excludes_below_epoch_start_and_non_genuine() {
        let window = vec![
            sealed(3, 0, true),  // below epoch_start (4) -> excluded even though genuine
            sealed(4, 1, true),  // worker 1, genuine, in range
            sealed(5, 2, false), // worker 2, empty-close block -> excluded
            sealed(6, 3, true),  // worker 3, genuine, in range
            sealed(0, 9, true),  // genesis -> never genuine
        ];
        let producers = worker_producers_in_epoch(&window, 4);
        assert_eq!(producers, BTreeSet::from([1u16, 3u16]));
    }

    #[test]
    fn fee_derivable_requires_every_eip1559_worker_to_produce() {
        let configs = vec![
            WorkerFeeConfig::Eip1559 { target_gas: 10 },
            WorkerFeeConfig::Static { fee: 5 },
            WorkerFeeConfig::Eip1559 { target_gas: 20 },
        ];

        // worker 0 and 2 are eip1559; both present -> derivable
        assert!(fee_derivable_for(&configs, &BTreeSet::from([0u16, 2u16])));
        // static worker 1 needs no anchor -> its absence is fine
        assert!(fee_derivable_for(&configs, &BTreeSet::from([0u16, 2u16])));
        // eip1559 worker 2 missing -> not derivable
        assert!(!fee_derivable_for(&configs, &BTreeSet::from([0u16])));
        // no producers at all, but only static workers -> derivable
        assert!(fee_derivable_for(&[WorkerFeeConfig::Static { fee: 1 }], &BTreeSet::new()));
    }
}
