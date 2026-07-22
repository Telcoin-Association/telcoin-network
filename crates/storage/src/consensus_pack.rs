//! Implement a Pack file to contain consensus chain data (Batches and ConsensusHeaders).
//! Stored per epoch.

use std::{
    cmp::max,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    error::Error,
    fmt::Display,
    hash::BuildHasherDefault,
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, BlockNumHash,
    BlsPublicKey, CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch, EpochRecord, Hash as _, Round,
    B256, MAX_GC_DEPTH, MAX_HEADER_NUM_OF_BATCHES,
};
use tokio::{
    io::{AsyncRead, BufReader},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};
use tracing::{debug, error};

use crate::archive::{
    data_file::create_dir_synced,
    digest_index::index::HdxIndex,
    error::{fetch::FetchError, open::OpenError},
    fxhasher::FxHasher,
    index::Index as _,
    pack::{write_value, DataHeader, Pack, PackCompression, DATA_HEADER_BYTES},
    pack_iter::AsyncPackIter,
    position_index::index::{PosIndexValue, PositionIndex},
};

/// Current version for new pack files.
pub const PACK_VERSION: u16 = 1;

/// Metadata for an Epoch.  Should always be the first record in a consensus pack.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
pub struct EpochMeta {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    /// Store the full committee not just Bls Keys so we can reconstruct ConsensusOutput easier.
    pub committee: Committee,
    /// The first consensus block number of this epoch.
    pub start_consensus_number: u64,
    /// The block number and hash of the last execution state of the previous epoch.
    /// Basically the execution genesis for this epoch.
    pub genesis_exec_state: BlockNumHash,
    /// The hash of the last ['ConsensusHeader'] of the previous epoch.
    /// This is the "genesis" consensus ofder  this epoch.
    pub genesis_consensus: ConsensusNumHash,
}

/// Descriminant type for records in a Consensus Pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PackRecord {
    EpochMeta(EpochMeta),
    Batch(Batch),
    Consensus(Box<ConsensusHeader>),
}

impl PackRecord {
    fn into_consensus(self) -> Result<ConsensusHeader, PackError> {
        if let Self::Consensus(header) = self {
            Ok(*header)
        } else {
            Err(PackError::NotConsensus)
        }
    }
    fn into_batch(self) -> Result<Batch, PackError> {
        if let Self::Batch(batch) = self {
            Ok(batch)
        } else {
            Err(PackError::NotBatch)
        }
    }
    fn into_epoch(self) -> Result<EpochMeta, PackError> {
        if let Self::EpochMeta(epoch) = self {
            Ok(epoch)
        } else {
            Err(PackError::NotEpoch)
        }
    }
}

enum PackMessage {
    ConsensusOutput(ConsensusOutput, oneshot::Sender<Result<u64, PackError>>),
    ContainsConsensusHeaderNumber(u64, oneshot::Sender<bool>),
    ContainsConsensusHeader(ConsensusHeaderDigest, oneshot::Sender<bool>),
    ConsensusHeader(ConsensusHeaderDigest, oneshot::Sender<Option<ConsensusHeader>>),
    ConsensusHeaderNumber(u64, oneshot::Sender<Result<ConsensusHeader, PackError>>),
    Persist(oneshot::Sender<Result<(), PackError>>),
    BytesForConsensus(u64, oneshot::Sender<Result<Vec<u8>, PackError>>),
    OutputEndForConsensus(u64, oneshot::Sender<Result<u64, PackError>>),
    ReadLastCommitted(oneshot::Sender<Result<HashMap<AuthorityIdentifier, Round>, PackError>>),
    ReadLatestFinalRep(oneshot::Sender<Result<Option<CommittedSubDag>, PackError>>),
    ContainsBatch(B256, oneshot::Sender<bool>),
    Batch(B256, oneshot::Sender<Option<Batch>>),
    CountLeaders(Round, RewardsCounter, oneshot::Sender<Result<(), PackError>>),
    LatestConsensusHeader(oneshot::Sender<Option<ConsensusHeader>>),
    Shutdown,
    // Flush the write buffer to the data file WITHOUT fsync, so freshly appended bytes
    /// become visible to other file handles on the same file (visibility, not durability).
    FlushData(oneshot::Sender<Result<(), PackError>>),
}

/// Manage a single pack file of consensus data (typically one epoch os the consensus chain).
#[derive(Debug, Clone)]
pub struct ConsensusPack {
    tx: Sender<PackMessage>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    epoch: Epoch,
    committee: Committee,
    compression: PackCompression,
    is_static: bool,
    version: u16, // Version of the underlying data pack file.
}

fn run_pack_loop(mut inner: Inner, mut rx: Receiver<PackMessage>) {
    // When this returns None then the channel is consumed and closed, so exit the thread.
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            PackMessage::ConsensusOutput(output, tx) => {
                let _ = tx.send(inner.save_consensus_output(&output));
            }
            PackMessage::ContainsConsensusHeaderNumber(number, tx) => {
                let _ = tx.send(inner.contains_consensus_header_number(number));
            }
            PackMessage::ContainsConsensusHeader(digest, tx) => {
                let _ = tx.send(inner.contains_consensus_header(digest));
            }
            PackMessage::ConsensusHeader(digest, tx) => {
                let _ = tx.send(inner.consensus_header_by_digest(digest));
            }
            PackMessage::ConsensusHeaderNumber(number, tx) => {
                let _ = tx.send(inner.consensus_header_by_number(number));
            }
            PackMessage::Persist(tx) => {
                let _ = tx.send(inner.persist());
            }
            PackMessage::BytesForConsensus(number, tx) => {
                let _ = tx.send(inner.bytes_for_consensus(number));
            }
            PackMessage::OutputEndForConsensus(number, tx) => {
                let _ = tx.send(inner.output_end_for_consensus(number));
            }
            PackMessage::ReadLastCommitted(tx) => {
                let _ = tx.send(inner.read_last_committed());
            }
            PackMessage::ReadLatestFinalRep(tx) => {
                let _ = tx.send(inner.read_latest_commit_with_final_reputation_scores());
            }
            PackMessage::ContainsBatch(digest, tx) => {
                let _ = tx.send(inner.contains_batch(digest));
            }
            PackMessage::Batch(digest, tx) => {
                let _ = tx.send(inner.batch(digest));
            }
            PackMessage::CountLeaders(last_executed_round, rewards_counter, tx) => {
                let _ = tx.send(inner.count_leaders(last_executed_round, &rewards_counter));
            }
            PackMessage::LatestConsensusHeader(tx) => {
                let _ = tx.send(inner.latest_consensus_header());
            }
            PackMessage::Shutdown => {
                let _ = inner.persist();
                break;
            }
            PackMessage::FlushData(tx) => {
                let _ = tx.send(inner.flush_data());
            }
        }
    }
}

impl Drop for ConsensusPack {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handle) == 1 {
            // If we are the last ConsensusPack then shutdown thread and wait for it to persist and
            // exit.
            if let Some(handle) = self.handle.lock().take() {
                if self.tx.try_send(PackMessage::Shutdown).is_ok() {
                    if let Err(e) = handle.join() {
                        error!(target: "consensus_pack", ?e, "Failed to join consensus pack thread");
                    }
                }
            }
        }
    }
}

impl ConsensusPack {
    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    pub fn open_append<P: Into<PathBuf>>(
        path: P,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<ConsensusPack, PackError> {
        Self::open_append_inner(path, previous_epoch, committee, PACK_VERSION)
    }

    /// Test-only: open an append pack forcing a specific on-disk data version so tests can
    /// construct genuine v0 (legacy, batches-first) pack files.
    #[cfg(test)]
    pub(crate) fn open_append_version<P: Into<PathBuf>>(
        path: P,
        previous_epoch: EpochRecord,
        committee: Committee,
        version: u16,
    ) -> Result<ConsensusPack, PackError> {
        Self::open_append_inner(path, previous_epoch, committee, version)
    }

    /// Shared body for [`Self::open_append`] stamping the given on-disk data `version`.
    fn open_append_inner<P: Into<PathBuf>>(
        path: P,
        previous_epoch: EpochRecord,
        committee: Committee,
        version: u16,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let epoch = committee.epoch();
        let inner = Inner::open_append(path.clone(), &previous_epoch, committee.clone(), version)?;
        let version = inner.version();
        let compression = inner.data.header().compression();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            epoch,
            committee,
            compression,
            is_static: false,
            version,
        })
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    pub fn open_append_exists<P: Into<PathBuf>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let inner = Inner::open_append_exists(path.clone(), epoch)?;
        let version = inner.version();
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            epoch,
            committee,
            compression,
            is_static: false,
            version,
        })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    /// Note, you should call persist() on the returned pack to make sure it
    /// opened cleanly.
    pub fn open_static<P: Into<PathBuf>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let inner = Inner::open_static(path.clone(), epoch)?;
        let version = inner.version();
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            epoch,
            committee,
            compression,
            is_static: true,
            version,
        })
    }

    /// Create a new set of epoch static files to write consensus output into.
    pub async fn stream_import<P: Into<PathBuf>, R: AsyncRead + Unpin>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let inner = Inner::stream_import(
            path,
            stream,
            epoch,
            previous_epoch,
            final_consensus_number,
            timeout,
        )
        .await?;
        let version = inner.version();
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || {
            run_pack_loop(inner, rx);
        });
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            epoch,
            committee,
            compression,
            is_static: true,
            version,
        })
    }

    /// Is this packfile static- i.e. complete and read only.
    pub fn is_static(&self) -> bool {
        self.is_static
    }

    /// Return the epoch for this pack file.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Return the committee persisted in this pack's [`EpochMeta`] — the epoch-START
    /// snapshot this epoch's consensus output is decoded and verified against.
    ///
    /// Every open path keeps this handle-level copy faithful to the on-disk meta:
    /// `open_append` either writes it as the new meta or errors on a meta mismatch, and
    /// the reopen/import paths clone it out of the persisted record.
    pub(crate) fn committee(&self) -> &Committee {
        &self.committee
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    /// Returns when save is complete and provides how many bytes the output took in the pack file.
    pub async fn save_consensus_output(
        &self,
        consensus: ConsensusOutput,
    ) -> Result<u64, PackError> {
        let (tx, rx) = oneshot::channel();
        let len = if self.tx.send(PackMessage::ConsensusOutput(consensus, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)??
        } else {
            return Err(PackError::SendFailed);
        };
        Ok(len)
    }

    /// Load and return the consensus output form this epoch.
    pub async fn get_consensus_output(&self, number: u64) -> Result<ConsensusOutput, PackError> {
        let (tx, rx) = oneshot::channel();
        let bytes = if self.tx.send(PackMessage::BytesForConsensus(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)??
        } else {
            return Err(PackError::SendFailed);
        };
        let cursor = Cursor::new(bytes);
        let reader = BufReader::new(cursor);
        match self.version {
            0 => {
                bytes_to_output_legacy(
                    reader,
                    self.compression,
                    Duration::from_secs(5),
                    &self.committee,
                )
                .await
            }
            1 => {
                bytes_to_output(reader, self.compression, Duration::from_secs(5), &self.committee)
                    .await
            }
            _ => Err(PackError::InvalidVersion(PACK_VERSION, self.version)),
        }
    }

    /// Decode pack-file `bytes` (as produced by [`Self::get_consensus_output_bytes`] / streamed via
    /// `request_consensus_output`) into a [`ConsensusOutput`] using this pack's committee and
    /// compression. The committee resolves each certificate author to an execution address, so the
    /// pack must be for the same epoch as the bytes.
    pub async fn decode_output(&self, bytes: Vec<u8>) -> Result<ConsensusOutput, PackError> {
        let cursor = Cursor::new(bytes);
        let reader = BufReader::new(cursor);
        bytes_to_output(reader, self.compression, Duration::from_secs(5), &self.committee).await
    }

    /// Stream-decode a v1 (header-first) pack-encoded [`ConsensusOutput`] from `reader`, verifying
    /// the header's digest equals `expected_digest` the instant the header record is read — BEFORE
    /// any batch record is buffered. Used on the requested-output receive path so an unverified
    /// peer stream cannot force buffering/decoding more than a single ≤`MAX_RECORD_SIZE` header
    /// record before the known hash is checked. Uses this pack's committee (author -> execution
    /// address) and compression, so the pack must be for the same epoch as the stream.
    pub async fn decode_output_stream<R: AsyncRead + Unpin>(
        &self,
        reader: R,
        expected_digest: ConsensusHeaderDigest,
    ) -> Result<ConsensusOutput, PackError> {
        bytes_to_verified_output(
            reader,
            self.compression,
            Duration::from_secs(5),
            &self.committee,
            expected_digest,
        )
        .await
    }

    /// Load and return the pack file bytes for consensus output form this epoch.
    pub async fn get_consensus_output_bytes(&self, number: u64) -> Result<Vec<u8>, PackError> {
        let (tx, rx) = oneshot::channel();
        let mut bytes = if self.tx.send(PackMessage::BytesForConsensus(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)?
        } else {
            Err(PackError::SendFailed)
        }?;
        match self.version {
            0 => {
                let cursor = Cursor::new(bytes.clone());
                let reader = BufReader::new(cursor);
                let out = bytes_to_output_legacy(
                    reader,
                    self.compression,
                    Duration::from_secs(5),
                    &self.committee,
                )
                .await?;
                let batches = collect_batches(&out);
                let header: ConsensusHeader = out.into();
                bytes.clear();
                let mut value_buffer = Vec::new();
                let mut compress_buffer = Vec::new();
                // Re-encode as PackRecord-wrapped records (header first) to match the on-disk v1
                // format the consumer decodes; the raw v1 serve path (below) returns these same
                // PackRecord records straight from the pack file.
                write_value(
                    &PackRecord::Consensus(Box::new(header)),
                    &mut bytes,
                    &mut value_buffer,
                    &mut compress_buffer,
                    PackCompression::ZStd,
                )?;
                for (_, batch) in batches.into_iter() {
                    write_value(
                        &PackRecord::Batch(batch),
                        &mut bytes,
                        &mut value_buffer,
                        &mut compress_buffer,
                        PackCompression::ZStd,
                    )?;
                }
                Ok(bytes)
            }
            1 => Ok(bytes),
            _ => Err(PackError::InvalidVersion(PACK_VERSION, self.version)),
        }
    }

    /// Return the byte offset in the data file just past the end of the consensus output for
    /// `number`. Streaming `[0, output_end)` of the data file yields a verifiable prefix of the
    /// pack containing every output up to and including `number` (plus the data header). Errors
    /// if `number` is outside the range this pack contains.
    pub async fn consensus_output_end(&self, number: u64) -> Result<u64, PackError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::OutputEndForConsensus(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)?
        } else {
            Err(PackError::SendFailed)
        }
    }

    /// True if consensus header by digest is found by digest.
    pub async fn contains_consensus_header_number(&self, number: u64) -> Result<bool, PackError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ContainsConsensusHeaderNumber(number, tx)).await.is_ok() {
            Ok(rx.await.map_err(|_| PackError::ReceiveFailed)?)
        } else {
            Err(PackError::SendFailed)
        }
    }

    /// True if consensus header by digest is found by digest.
    pub async fn contains_consensus_header(&self, digest: ConsensusHeaderDigest) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ContainsConsensusHeader(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(
        &self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ConsensusHeader(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Retrieve a consensus header by number.
    pub async fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> Result<ConsensusHeader, PackError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(PackMessage::ConsensusHeaderNumber(number, tx))
            .await
            .map_err(|_| PackError::SendFailed)?;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }

    pub async fn persist(&self) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Persist(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }

    // public handle method (sibling of `persist`, consensus_pack.rs:412):
    pub async fn flush_data(&self) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::FlushData(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }

    /// Read the last committed rounds for authorities from the epoch.
    pub async fn read_last_committed(
        &self,
    ) -> Result<HashMap<AuthorityIdentifier, Round>, PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::ReadLastCommitted(tx)).await;
        if let Ok(r) = rx.await {
            r
        } else {
            Err(PackError::SendFailed)
        }
    }

    /// Reads from storage the latest commit sub dag from the epoch where its
    /// ReputationScores are marked as "final". If none exists then this
    /// method returns `None`.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &self,
    ) -> Result<Option<CommittedSubDag>, PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::ReadLatestFinalRep(tx)).await;
        if let Ok(r) = rx.await {
            r
        } else {
            Err(PackError::SendFailed)
        }
    }

    /// Return the latest consensus header by reading directly from the pack index.
    /// Unlike consensus_header_latest on ConsensusChain, this does not rely on the
    /// slot files (LatestConsensus) and is always consistent with read_last_committed.
    pub async fn latest_consensus_header(&self) -> Option<ConsensusHeader> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::LatestConsensusHeader(tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// True if the pack contains the batch for digest.
    pub async fn contains_batch(&self, digest: BlockHash) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::ContainsBatch(digest, tx)).await;
        rx.await.unwrap_or_default()
    }

    /// Return the Batch for digest if found.
    pub async fn batch(&self, digest: BlockHash) -> Option<Batch> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Batch(digest, tx)).await;
        rx.await.unwrap_or_default()
    }

    /// Count leaders in this pack (in rewards_counter) lower than last_executed_round.
    pub async fn count_leaders(
        &self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ =
            self.tx.send(PackMessage::CountLeaders(last_executed_round, rewards_counter, tx)).await;
        if let Ok(r) = rx.await {
            r
        } else {
            Err(PackError::SendFailed)
        }
    }
}

pub const DATA_NAME: &str = Inner::DATA_NAME;

#[derive(Debug)]
struct Inner {
    data: Pack<PackRecord>,
    /// Positional index pointing to the first byte of ConsensusHeader, the first byte of the first
    /// Batch and the byte past the end of the ConsensusHeader at a position. In short the first
    /// and last (exclusive) bytes of the encoded data for a ConsensusOutput as well as just
    /// the ConsensusHeader.
    consensus_pos_idx: PositionIndex<IndexPositions>,
    consensus_digests: HdxIndex,
    batch_digests: HdxIndex,
    epoch_meta: EpochMeta,
}

impl Inner {
    const DATA_NAME: &str = "data";
    const CONSENSUS_POS_NAME: &str = "idx";
    const CONSENSUS_HASH_NAME: &str = "hash";
    const BATCH_HASH_NAME: &str = "bhash";

    /// Determine if the pack and indexes appear to have been closed cleanly.
    fn files_consistent(
        data: &Pack<PackRecord>,
        consensus_pos_idx: &mut PositionIndex<IndexPositions>,
        consensus_digests: &HdxIndex,
        batch_digests: &HdxIndex,
    ) -> bool {
        let pack_len = data.file_len();
        let consensus_final = consensus_digests.data_file_length();
        let batch_final = batch_digests.data_file_length();
        if pack_len != consensus_final || pack_len != batch_final {
            return false;
        }
        if !consensus_pos_idx.is_empty() {
            let last_record_end = match consensus_pos_idx.load(consensus_pos_idx.len() as u64 - 1) {
                Ok(p) => p.output_end,
                Err(_) => return false,
            };
            pack_len == last_record_end
        } else {
            true
        }
    }

    /// Truncate the pack file in order to get back to a clean state.
    fn trunc_and_heal(
        data: &mut Pack<PackRecord>,
        consensus_pos_idx: &mut PositionIndex<IndexPositions>,
        consensus_digests: &mut HdxIndex,
        batch_digests: &mut HdxIndex,
    ) -> Result<(), PackError> {
        let pack_len = data.file_len();
        let consensus_final = consensus_digests.data_file_length();
        let batch_final = batch_digests.data_file_length();
        if pack_len > consensus_final || pack_len > batch_final {
            if consensus_final > batch_final && batch_final > DATA_HEADER_BYTES as u64 {
                data.truncate(batch_final)?;
            } else if consensus_final > DATA_HEADER_BYTES as u64 {
                data.truncate(consensus_final)?;
            }
            // Note we leave the digest indexes with potentially some missing digests.
            // This should be OK since they will have to be overwritten with same digests
            // when they are readded and lookups should handle this.
            // Alternatively we would need to regen the indexes from scratch or painstakenly
            // remove digests, both would be expensive operations.
        }
        let pack_len = data.file_len();
        if !consensus_pos_idx.is_empty() {
            let mut new_pack_len = pack_len;
            // Make sure we are not indexing any records that no longer exist.
            // Also make sure we don not have a partial record left on a short file.
            let start_idx = consensus_pos_idx.len() as u64 - 1;
            let mut idx = start_idx;
            loop {
                if let Ok(last_record) = consensus_pos_idx.load(idx) {
                    if idx != start_idx {
                        // Keep the bytes index in sync with the consensus index so the two
                        // never diverge in length after healing a damaged pack.
                        consensus_pos_idx.truncate_to_index(idx)?;
                    }
                    new_pack_len = last_record.output_end;
                    if new_pack_len <= pack_len {
                        break;
                    }
                }
                if idx == 0 {
                    if idx != start_idx {
                        consensus_pos_idx.truncate_all()?;
                    }
                    break;
                }
                idx -= 1;
            }
            // Only ever shrink: `truncate` is `set_len`, so a `new_pack_len` above the current
            // length (an index entry claiming an `output_end` past the data we actually have)
            // would zero-extend the pack.  Clamp defensively.
            let new_pack_len = new_pack_len.min(pack_len);
            if new_pack_len != pack_len {
                data.truncate(new_pack_len)?;
            }
        }
        // Reconcile the digest indexes' tracked data file length with the (possibly truncated)
        // pack so files_consistent holds even if no save follows this heal.  Lookups use the
        // live pack length, so this only affects the consistency check on a later open.
        let healed_len = data.file_len();
        consensus_digests.set_data_file_length(healed_len);
        batch_digests.set_data_file_length(healed_len);
        Ok(())
    }

    /// Return the version of the underlying data pack file.
    fn version(&self) -> u16 {
        self.data.version()
    }

    /// Open a PDX index file and return the open index.
    fn open_pdx_file<P: AsRef<Path>, T: PosIndexValue>(
        dir: P,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<PositionIndex<T>, PackError> {
        let base_dir = dir.as_ref().join(Self::CONSENSUS_POS_NAME);
        let consensus_pos_idx =
            PositionIndex::open_pdx_file(&base_dir, data_header, "index_pos.pdx", read_only)
                .map_err(OpenError::IndexFileOpen)?;
        Ok(consensus_pos_idx)
    }

    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    fn open_append<P: AsRef<Path>>(
        path: P,
        previous_epoch: &EpochRecord,
        committee: Committee,
        version: u16,
    ) -> Result<Self, PackError> {
        let epoch = committee.epoch();
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = create_dir_synced(&base_dir);
        let pack_file = base_dir.join(Self::DATA_NAME);
        let have_pack = std::fs::exists(&pack_file).unwrap_or_default();
        let mut data: Pack<PackRecord> =
            Pack::open(&pack_file, epoch as u64, false, PackCompression::ZStd, version)?;
        let start_consensus_number =
            if epoch == 0 { 1 } else { previous_epoch.final_consensus.number + 1 };
        let epoch_meta = EpochMeta {
            epoch,
            committee,
            start_consensus_number,
            genesis_exec_state: previous_epoch.final_state,
            genesis_consensus: previous_epoch.final_consensus,
        };

        if let Ok(meta) = data.fetch(DATA_HEADER_BYTES as u64) {
            let meta = meta.into_epoch()?;
            if epoch_meta != meta {
                return Err(PackError::InvalidEpoch(
                    epoch,
                    format!("open append has unexpected meta data, expected {epoch_meta:?} got {meta:?}"),
                ));
            }
        } else {
            data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                .map_err(|e| PackError::Append(e.to_string()))?;
        }
        let mut consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), false)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut batch_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::BATCH_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        if !have_pack {
            // If this is a new DB then update the file lengths in indexes after create.
            let len = data.file_len();
            consensus_digests.set_data_file_length(len);
            batch_digests.set_data_file_length(len);
        }
        // Repair damage.
        Self::trunc_and_heal(
            &mut data,
            &mut consensus_pos_idx,
            &mut consensus_digests,
            &mut batch_digests,
        )?;
        Ok(Self { data, consensus_digests, consensus_pos_idx, batch_digests, epoch_meta })
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    fn open_append_exists<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));

        let mut data = Pack::<PackRecord>::open(
            base_dir.join(Self::DATA_NAME),
            epoch as u64,
            false,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let epoch_meta = data
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?
            .into_epoch()?;
        let mut consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), false)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut batch_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::BATCH_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;

        // Repair damage.
        Self::trunc_and_heal(
            &mut data,
            &mut consensus_pos_idx,
            &mut consensus_digests,
            &mut batch_digests,
        )?;
        Ok(Self { data, consensus_digests, consensus_pos_idx, batch_digests, epoch_meta })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    fn open_static<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));

        let mut data = Pack::<PackRecord>::open(
            base_dir.join(Self::DATA_NAME),
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let epoch_meta = data
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?
            .into_epoch()?;
        let mut consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), true)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
            true,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let batch_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::BATCH_HASH_NAME),
            data.header(),
            builder,
            true,
        )
        .map_err(OpenError::IndexFileOpen)?;

        if !Self::files_consistent(
            &data,
            &mut consensus_pos_idx,
            &consensus_digests,
            &batch_digests,
        ) {
            // Corrupt static file is bad (damaged at rest?), produce an error.
            return Err(PackError::CorruptPack);
        }
        Ok(Self { data, consensus_digests, consensus_pos_idx, batch_digests, epoch_meta })
    }

    /// Create a new set of epoch static files to write consensus output into.
    async fn stream_import<P: AsRef<Path>, R: AsyncRead + Unpin>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = create_dir_synced(&base_dir);
        let mut stream_iter = AsyncPackIter::<PackRecord, R>::open(stream, epoch as u64)
            .await
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let mut data = Pack::open(
            base_dir.join(Self::DATA_NAME),
            epoch as u64,
            false,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let epoch_meta = if let Some(meta) = next_output_record(&mut stream_iter, timeout).await? {
            meta.into_epoch()?
        } else {
            return Err(PackError::NotEpoch);
        };
        verify_epoch_meta(epoch, previous_epoch, &epoch_meta)?;
        data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
            .map_err(|e| PackError::Append(e.to_string()))?;
        let consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), false)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let batch_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::BATCH_HASH_NAME),
            data.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let mut parent_digest = if epoch == 0 {
            ConsensusHeader::default().digest()
        } else {
            previous_epoch.final_consensus.hash
        };
        let mut pack =
            Self { data, consensus_pos_idx, consensus_digests, batch_digests, epoch_meta };
        loop {
            // The header's parent link is verified INSIDE the decoder via
            // `HeaderExpectation::Parent` — early (before batches) on the v1 header-first path — so
            // a forked/forged output is rejected before its batches are buffered. `parent_digest`
            // advances to this output's digest for the next iteration below.
            let output = if stream_iter.version() == 0 {
                match iter_to_output_legacy(
                    &mut stream_iter,
                    timeout,
                    &pack.epoch_meta.committee,
                    HeaderExpectation::Parent(parent_digest),
                )
                .await
                {
                    Ok(output) => output,
                    Err(PackError::NotConsensus) => break,
                    Err(e) => return Err(e),
                }
            } else {
                match iter_to_output(
                    &mut stream_iter,
                    timeout,
                    &pack.epoch_meta.committee,
                    HeaderExpectation::Parent(parent_digest),
                )
                .await
                {
                    Ok(output) => output,
                    Err(PackError::NotConsensus) => break,
                    Err(e) => return Err(e),
                }
            };
            let consensus_number = output.number();
            if consensus_number > final_consensus_number {
                return Err(PackError::InvalidConsensusNumber(
                    consensus_number,
                    final_consensus_number,
                ));
            }
            parent_digest = output.digest();
            pack.save_consensus_output(&output)?;
        }
        Ok(pack)
    }

    /// Write the batches for consensus to the pack file.
    fn save_consensus_batches(
        &mut self,
        consensus: &ConsensusOutput,
    ) -> Result<Option<u64>, PackError> {
        let batches = collect_batches(consensus);
        let mut first_batch_pos = None;
        // Save all the required batches into the pack file.
        for (batch_digest, batch) in batches.into_iter() {
            let position = self
                .data
                .append(&PackRecord::Batch(batch))
                .map_err(|e| PackError::Append(e.to_string()))?;
            if first_batch_pos.is_none() {
                first_batch_pos = Some(position);
            }
            self.batch_digests
                .save(batch_digest, position)
                .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
            let len = self.data.file_len();
            self.consensus_digests.set_data_file_length(len);
            self.batch_digests.set_data_file_length(len);
        }
        Ok(first_batch_pos)
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    /// Returns the number of bytes the encoded ConsensusOutput takes in the pack file.
    fn save_consensus_output(&mut self, consensus: &ConsensusOutput) -> Result<u64, PackError> {
        let consensus_number = consensus.number();
        // Adjusted consensus index for this pack file.
        let consensus_idx = consensus_number.saturating_sub(self.epoch_meta.start_consensus_number);
        let epoch = consensus.sub_dag().leader_epoch();
        if epoch != self.epoch_meta.epoch {
            // Trying to save to the wrong epoch...
            return Err(PackError::InvalidEpoch(
                epoch,
                format!(
                    "Tried to save output from epoch {epoch} to the pack file for epoch {}",
                    self.epoch_meta.epoch
                ),
            ));
        }
        // Make sure this number is valid before we write anything...
        if (consensus_idx as usize) < self.consensus_pos_idx.len() {
            // If we have saved this output already then ignore it.
            // Note this can be important when we replay consensus from downloaded pack files.
            // We do need to return the bytes this output requires in the pack file.
            let pos = self.consensus_pos_idx.load(consensus_idx)?;
            return Ok(pos.output_end.saturating_sub(pos.output_start));
        } else if consensus_idx as usize != self.consensus_pos_idx.len() {
            return Err(PackError::InvalidConsensusNumber(
                self.consensus_pos_idx.len() as u64 + self.epoch_meta.start_consensus_number,
                consensus_number,
            ));
        }
        let first_batch_pos =
            if self.version() == 0 { self.save_consensus_batches(consensus)? } else { None };
        // Now save the consensus header.
        let consensus_digest = consensus.consensus_header_hash();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
        if self.version() > 0 {
            self.save_consensus_batches(consensus)?;
        };
        let batch_pos = if let Some(batch_pos) = first_batch_pos { batch_pos } else { position };
        self.consensus_digests
            .save(consensus_digest.into(), position)
            .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
        let len = self.data.file_len();
        self.consensus_pos_idx
            .save(consensus_idx, IndexPositions::new(position, batch_pos, len))
            .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
        self.consensus_digests.set_data_file_length(len);
        self.batch_digests.set_data_file_length(len);

        Ok(len.saturating_sub(batch_pos))
    }

    /// True if consensus header by digest is found by digest.
    fn contains_consensus_header_number(&self, number: u64) -> bool {
        number >= self.epoch_meta.start_consensus_number
            && number < self.consensus_pos_idx.len() as u64 + self.epoch_meta.start_consensus_number
    }

    /// True if consensus header is found by digest.
    fn contains_consensus_header(&mut self, digest: ConsensusHeaderDigest) -> bool {
        // This is a bit more complicated (the pos file_len check) because in a very rare
        // case of repairing a damaged pack we might have something in the index not in the
        // pack file (yet).
        if let Ok(pos) = self.consensus_digests.load(digest.into()) {
            pos < self.data.file_len()
        } else {
            false
        }
    }

    /// Retrieve a consensus header by digest.
    fn consensus_header_by_digest(
        &mut self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader> {
        let pos = self.consensus_digests.load(digest.into()).ok()?;
        // This is not strickly needed, the fetch below will fail if
        // we try to read past the end of the file but this potentially
        // short circuits a lot of checks for a small cost.
        // Note, this could happen if a file is damaged and repaired.
        if pos >= self.data.file_len() {
            return None;
        }
        let header = self.data.fetch(pos).ok()?.into_consensus().ok()?;
        // Verify the digest.  There is an extremely unlikely edge case where
        // a repaired DB could write a new header to the same location as an
        // old header.  This makes sure the contract is always intact.
        if header.digest() != digest {
            return None;
        }
        Some(header)
    }

    /// Retrieve a consensus header by number.
    fn consensus_header_by_number(&mut self, number: u64) -> Result<ConsensusHeader, PackError> {
        if number < self.epoch_meta.start_consensus_number {
            return Err(PackError::ConsensusNumberTooLow);
        }
        if number >= (self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64)
        {
            return Err(PackError::ConsensusNumberTooHigh);
        }
        let pos = self
            .consensus_pos_idx
            .load(number.saturating_sub(self.epoch_meta.start_consensus_number))?
            .consensus_header;
        self.data.fetch(pos)?.into_consensus()
    }

    fn persist(&mut self) -> Result<(), PackError> {
        if !self.data.read_only() {
            self.data.commit().map_err(|e| PackError::PersistError(e.to_string()))?;
            self.consensus_pos_idx.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
            self.consensus_digests.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
            self.batch_digests.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        }
        Ok(())
    }

    // Inner method (sibling of Inner::persist, consensus_pack.rs:1051) — flush only, no syncs:
    fn flush_data(&mut self) -> Result<(), PackError> {
        if !self.data.read_only() {
            self.data.flush().map_err(|e| PackError::PersistError(e.to_string()))?;
        }
        Ok(())
    }

    /// Read and return all the bytes for consensus number (all batches and the consensus header).
    fn bytes_for_consensus(&mut self, number: u64) -> Result<Vec<u8>, PackError> {
        // Validate the range like consensus_header_by_number; without this a number below
        // start_consensus_number would saturate to index 0 and silently return the epoch's
        // first output instead of an error.
        if number < self.epoch_meta.start_consensus_number {
            return Err(PackError::ConsensusNumberTooLow);
        }
        if number >= self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64 {
            return Err(PackError::ConsensusNumberTooHigh);
        }
        let rec_pos_idx = number.saturating_sub(self.epoch_meta.start_consensus_number);
        let IndexPositions { consensus_header: _, output_start, output_end } = self
            .consensus_pos_idx
            .load(rec_pos_idx)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let bytes = self
            .data
            .read_bytes(output_start, output_end)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        Ok(bytes)
    }

    /// Return the byte offset in the data file just past the end of the consensus output for
    /// `number` (the `output_end` of its index entry). Range-checked like `bytes_for_consensus`.
    fn output_end_for_consensus(&mut self, number: u64) -> Result<u64, PackError> {
        if number < self.epoch_meta.start_consensus_number {
            return Err(PackError::ConsensusNumberTooLow);
        }
        if number >= self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64 {
            return Err(PackError::ConsensusNumberTooHigh);
        }
        let rec_pos_idx = number.saturating_sub(self.epoch_meta.start_consensus_number);
        let pos = self
            .consensus_pos_idx
            .load(rec_pos_idx)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        Ok(pos.output_end)
    }

    /// Return the latest consensus header by reading directly from the pack index,
    /// bypassing the slot file (LatestConsensus). Used during startup recovery to
    /// get a ground-truth latest header consistent with read_last_committed.
    fn latest_consensus_header(&mut self) -> Option<ConsensusHeader> {
        if self.consensus_pos_idx.is_empty() {
            return None;
        }
        let latest_number =
            self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64 - 1;
        self.consensus_header_by_number(latest_number).ok()
    }

    fn read_last_committed(&mut self) -> Result<HashMap<AuthorityIdentifier, Round>, PackError> {
        let mut res = HashMap::new();
        let iter = self.consensus_pos_idx.rev_iter(50)?;
        for pos in iter {
            let pos = pos?;
            let block = self.data.fetch(pos.consensus_header)?.into_consensus()?;
            let id = block.sub_dag.leader().author().clone();
            let round = block.sub_dag.leader_round();
            let headers = block.sub_dag.headers();
            res.entry(id).and_modify(|r| *r = max(*r, round)).or_insert_with(|| round);
            for h in headers {
                res.entry(h.author().clone())
                    .and_modify(|r| *r = max(*r, h.round()))
                    .or_insert_with(|| h.round());
            }
        }
        Ok(res)
    }

    fn read_latest_commit_with_final_reputation_scores(
        &mut self,
    ) -> Result<Option<CommittedSubDag>, PackError> {
        let iter = self.consensus_pos_idx.rev_iter(1000)?;
        for pos in iter {
            let pos = pos?;
            let commit = self.data.fetch(pos.consensus_header)?.into_consensus()?.sub_dag;
            // found a final of schedule score, so we'll return that
            if commit.reputation_scores().final_of_schedule {
                debug!(
                    "Found latest final reputation scores: {:?} from commit",
                    commit.reputation_scores(),
                );
                return Ok(Some(commit));
            }
        }
        debug!("No final reputation scores have been found");
        Ok(None)
    }

    /// True if the pack contains the batch for digest.
    fn contains_batch(&mut self, digest: BlockHash) -> bool {
        // This is a bit more complicated (the pos file_len check) because in a very rare
        // case of repairing a damaged pack we might have something in the index not in the
        // pack file (yet).
        if let Ok(pos) = self.batch_digests.load(digest) {
            pos < self.data.file_len()
        } else {
            false
        }
    }

    /// Return the Batch for digest if found.
    fn batch(&mut self, digest: BlockHash) -> Option<Batch> {
        let pos = self.batch_digests.load(digest).ok()?;
        // This is not strickly needed, the fetch below will fail if
        // we try to read past the end of the file but this potentially
        // short circuits a lot of checks for a small cost.
        // Note, this could happen if a file is damaged and repaired.
        if pos >= self.data.file_len() {
            return None;
        }
        let batch = self.data.fetch(pos).ok()?.into_batch().ok()?;
        // Verify the digest.  There is an extremely unlikely edge case where
        // a repaired DB could write a new batch to the same location as an
        // old batch.  This makes sure the contract is always intact.
        if batch.digest() != digest {
            return None;
        }
        Some(batch)
    }

    /// Count leaders in this pack (in rewards_counter) lower than last_executed_round.
    fn count_leaders(
        &mut self,
        last_executed_round: Round,
        rewards_counter: &RewardsCounter,
    ) -> Result<(), PackError> {
        let headers = self.consensus_pos_idx.len();
        let iter = self.consensus_pos_idx.rev_iter(headers)?;
        for pos in iter {
            let pos = pos?;
            let header = self
                .data
                .fetch(pos.consensus_header)
                .map_err(|e| PackError::Fetch(e.to_string()))?
                .into_consensus()?;
            let leader_round = header.sub_dag.leader_round();

            if leader_round == 0 {
                continue;
            }
            if leader_round > last_executed_round {
                continue;
            }

            rewards_counter.inc_leader_count(header.sub_dag.leader().author());
        }
        Ok(())
    }
}

/// Gathers all the batches from consensus into an ordered Map by digest.
fn collect_batches(consensus: &ConsensusOutput) -> BTreeMap<BlockHash, Batch> {
    let mut batches = BTreeMap::new();
    // We want to make sure batches are saved to the pack in a deterministic order, so
    // collect them in a BTreeMap.
    for cert_batch in consensus.batches() {
        for batch in &cert_batch.batches {
            let digest = batch.digest();
            // Should not have duplicate batches across output.
            // They will be de-duped in the pack file by the BTreeMap if they do exist.
            batches.insert(digest, batch.clone());
        }
    }
    batches
}

/// Verify a streamed [`EpochMeta`] record links correctly to the previous epoch's record.
///
/// Extracted from [`Inner::stream_import`] as a free function (it is stateless) so the offline pack
/// validator ([`crate::pack_validate`]) can reuse the exact same epoch-linkage checks without
/// duplicating them.
pub(crate) fn verify_epoch_meta(
    epoch: Epoch,
    previous_epoch: &EpochRecord,
    epoch_meta: &EpochMeta,
) -> Result<(), PackError> {
    if epoch != epoch_meta.epoch {
        return Err(PackError::InvalidEpoch(
            epoch,
            format!("meta data epoch is {}", epoch_meta.epoch),
        ));
    }
    let start_consensus_number =
        if epoch == 0 { 1 } else { previous_epoch.final_consensus.number + 1 };
    if start_consensus_number != epoch_meta.start_consensus_number {
        return Err(PackError::InvalidEpoch(
            epoch,
            format!(
                "expected start consensus number {start_consensus_number}, got {}",
                epoch_meta.start_consensus_number
            ),
        ));
    }
    if previous_epoch.final_state != epoch_meta.genesis_exec_state {
        return Err(PackError::InvalidEpoch(
            epoch,
            format!(
                "expected final state {:?} meta final state {:?}",
                previous_epoch.final_state, epoch_meta.genesis_exec_state
            ),
        ));
    }
    if previous_epoch.final_consensus != epoch_meta.genesis_consensus {
        return Err(PackError::InvalidEpoch(
            epoch,
            format!(
                "expected final consensus {:?} meta final consensus {:?}",
                previous_epoch.final_consensus, epoch_meta.genesis_consensus
            ),
        ));
    }
    let committee: BTreeSet<BlsPublicKey> = previous_epoch.next_committee.iter().copied().collect();
    if epoch_meta.committee.bls_keys() != committee {
        return Err(PackError::InvalidEpoch(
            epoch,
            "epoch meta has unexpected committee".to_string(),
        ));
    }
    Ok(())
}

/// Upper bound on how many `Batch` records `iter_to_output` will buffer before the terminating
/// `Consensus` record, derived from the committee that produced the output.
///
/// [`MAX_GC_DEPTH`] is the garbage-collection horizon, not the depth a commit actually reaches.
/// `order_dag` descends only to `gc_round + 1` and additionally skips any round already committed
/// per authority, so in practice a sub-DAG is only a handful of rounds deep (a leader commits every
/// couple of rounds).  It serves purely as a safe ceiling: because no certificate at or below
/// `gc_round` can ever be linked into a commit, a single `CommittedSubDag` references certificates
/// from at most [`MAX_GC_DEPTH`] distinct rounds, a deliberately loose over-estimate that holds
/// regardless of commit cadence.  With at most one certificate per authority per round and at most
/// [`MAX_HEADER_NUM_OF_BATCHES`] batches per header (the proposer self-limits and
/// `Header::validate` rejects oversized inbound headers), a legitimately committed output therefore
/// references at most `committee.size() * MAX_GC_DEPTH * MAX_HEADER_NUM_OF_BATCHES` unique batches.
/// Bounding the reader at exactly that maximum keeps the writer and reader in agreement by
/// construction (every executed output can be reconstructed) while still capping an unauthenticated
/// peer flood of `Batch` records (the sub-DAG is only authenticated *after* decode, and the
/// per-record size cap does not bound their count).
fn max_batches_per_output(committee: &Committee) -> usize {
    committee.size().saturating_mul(MAX_GC_DEPTH as usize).saturating_mul(MAX_HEADER_NUM_OF_BATCHES)
}

/// What the caller already knows about the consensus header of the output being decoded, used to
/// reject a bad or forged header the instant it is read — before any `Batch` record is buffered.
///
/// The header-first (v1) pack ordering makes this early check possible: the `ConsensusHeader` is
/// the first record, so an authenticated header (or a verified parent link) bounds everything that
/// follows to the batches it declares.
#[derive(Debug, Clone, Copy)]
pub enum HeaderExpectation {
    /// Nothing is known up front (local reads / full-pack replay): no early check.
    None,
    /// The header's OWN digest is known (single-output fetch against an already-verified hash). A
    /// mismatch is [`PackError::UnexpectedConsensusDigest`].
    Digest(ConsensusHeaderDigest),
    /// The header's PARENT digest is known (epoch-pack forward chain link). A mismatch is
    /// [`PackError::InvalidConsensusChain`].
    Parent(ConsensusHeaderDigest),
}

/// Verify a freshly read `header` against what the caller already knows ([`HeaderExpectation`]).
/// Called the instant the header record is decoded — before reading batches on the v1 path — so a
/// wrong/forged header is rejected without buffering the batches it declares.
fn check_header_expectation(
    header: &ConsensusHeader,
    expectation: HeaderExpectation,
) -> Result<(), PackError> {
    match expectation {
        HeaderExpectation::None => Ok(()),
        HeaderExpectation::Digest(expected) => {
            let got = header.digest();
            (got == expected)
                .then_some(())
                .ok_or(PackError::UnexpectedConsensusDigest { expected, got })
        }
        HeaderExpectation::Parent(expected) => {
            (header.parent_hash == expected).then_some(()).ok_or(PackError::InvalidConsensusChain)
        }
    }
}

/// Take an async stream of bytes that in pack file representation of ConsensusOutput and return the
/// ConsensusOutput.
pub async fn bytes_to_output<R: AsyncRead + Unpin>(
    stream: R,
    compression: PackCompression,
    timeout: Duration,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    let mut stream_iter =
        AsyncPackIter::<PackRecord, R>::open_partial(stream, compression, PACK_VERSION)
            .await
            .map_err(|e| PackError::ReadError(e.to_string()))?;
    iter_to_output(&mut stream_iter, timeout, committee, HeaderExpectation::None).await
}

/// Take an async (v1, header-first) stream of pack-encoded ConsensusOutput bytes and return the
/// ConsensusOutput, verifying the header's digest equals `expected_digest` the instant it is read —
/// BEFORE any batch record is buffered. Used on the requested-output receive path, where the
/// expected header hash is already known (from verified gossip / a verified descendant's parent).
/// A mismatch is [`PackError::UnexpectedConsensusDigest`] and no batch bytes are read.
pub async fn bytes_to_verified_output<R: AsyncRead + Unpin>(
    stream: R,
    compression: PackCompression,
    timeout: Duration,
    committee: &Committee,
    expected_digest: ConsensusHeaderDigest,
) -> Result<ConsensusOutput, PackError> {
    let mut stream_iter =
        AsyncPackIter::<PackRecord, R>::open_partial(stream, compression, PACK_VERSION)
            .await
            .map_err(|e| PackError::ReadError(e.to_string()))?;
    iter_to_output(&mut stream_iter, timeout, committee, HeaderExpectation::Digest(expected_digest))
        .await
}

/// Take an async stream of bytes that in pack file representation of ConsensusOutput and return the
/// ConsensusOutput.
pub async fn bytes_to_output_legacy<R: AsyncRead + Unpin>(
    stream: R,
    compression: PackCompression,
    timeout: Duration,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    let mut stream_iter = AsyncPackIter::<PackRecord, R>::open_partial(stream, compression, 0)
        .await
        .map_err(|e| PackError::ReadError(e.to_string()))?;
    iter_to_output_legacy(&mut stream_iter, timeout, committee, HeaderExpectation::None).await
}

/// Private helper to read the next record from a pack iterator or timeout if it takes
/// longer than timeout.
async fn next_output_record<R: AsyncRead + Unpin>(
    iter: &mut AsyncPackIter<PackRecord, R>,
    timeout: Duration,
) -> Result<Option<PackRecord>, PackError> {
    match tokio::time::timeout(timeout, iter.next()).await {
        Ok(Some(Ok(rec))) => Ok(Some(rec)),
        Ok(Some(Err(e))) => Err(PackError::ReadError(e.to_string())),
        Ok(None) => Ok(None),
        Err(_) => Err(PackError::ReadError("timeout".to_string())),
    }
}

/// Take an iter over PackRecords that represent a ConsensusOutput and return the ConsensusOutput.
async fn iter_to_output<R: AsyncRead + Unpin>(
    stream_iter: &mut AsyncPackIter<PackRecord, R>,
    timeout: Duration,
    committee: &Committee,
    expectation: HeaderExpectation,
) -> Result<ConsensusOutput, PackError> {
    let mut referenced_batches = HashSet::new();
    let consensus_header = if let Some(record) = next_output_record(stream_iter, timeout).await? {
        match record {
            PackRecord::EpochMeta(_epoch_meta) => {
                return Err(PackError::EpochLoad("unexpected epoch meta data found".to_string()))
            }
            PackRecord::Batch(_batch) => {
                return Err(PackError::BatchLoad("unexpected batch found".to_string()))
            }
            PackRecord::Consensus(consensus_header) => consensus_header,
        }
    } else {
        return Err(PackError::NotConsensus);
    };
    // Header-first (v1): verify what the caller already knows BEFORE reading/buffering any batches.
    // An authenticated header (Digest) or a verified parent link (Parent) bounds everything that
    // follows to the batches the header declares; a wrong/forged header is rejected here.
    check_header_expectation(&consensus_header, expectation)?;
    let parent_hash = consensus_header.parent_hash;
    let deliver = consensus_header.sub_dag;
    let num_blocks = deliver.num_primary_batches();
    let num_certs = deliver.len();

    let sub_dag = deliver;
    if num_blocks == 0 {
        return Ok(ConsensusOutput::new_with_subdag(sub_dag, parent_hash, consensus_header.number));
    }

    let mut expected_batch_digests = BTreeSet::new();
    let mut batch_digests = VecDeque::with_capacity(num_certs);
    for header in sub_dag.headers() {
        for (digest, _) in header.payload().iter() {
            expected_batch_digests.insert(*digest);
            batch_digests.push_back(*digest);
        }
    }
    let expected_digest_count = expected_batch_digests.len();
    // Bound how many batch records we will read for one output before the terminating
    // condition.  The header is read first, so a hostile stream cannot flood batches ahead of
    // it, but the header's sub-dag (attacker-controlled, bounded only by MAX_RECORD_SIZE) can
    // still declare a huge number of payload digests.  Reject early — before reading/buffering
    // any batches — like the legacy path does.  A legitimate ConsensusOutput references far
    // fewer batches than this.
    let max_batches = max_batches_per_output(committee);
    if expected_digest_count > max_batches {
        return Err(PackError::TooManyBatches(max_batches));
    }
    let mut expected_batch_digests = expected_batch_digests.into_iter();

    let mut available_batches = HashMap::new();
    // Load and verify batches.  Batches are matched positionally against `expected_batch_digests`
    // (sorted digest order): producers write them in `BTreeMap`/`BTreeSet` digest order (see
    // `collect_batches` / `save_consensus_batches`), so the stream MUST arrive in that same order.
    // Out-of-order input is rejected below rather than silently reordered.
    let mut digest_count = 0;
    while let Some(record) = next_output_record(stream_iter, timeout).await? {
        match record {
            PackRecord::EpochMeta(_epoch_meta) => {
                return Err(PackError::EpochLoad("unexpected epoch meta data found".to_string()))
            }
            PackRecord::Batch(batch) => {
                let Some(expected_digest) = expected_batch_digests.next() else {
                    return Err(PackError::EpochLoad("unexpected batch found".to_string()));
                };
                let digest = batch.digest();
                if expected_digest != digest {
                    return Err(PackError::EpochLoad(format!(
                        "unexpected batch found, expected {expected_digest}, got {}",
                        digest
                    )));
                }
                referenced_batches.insert(digest);
                available_batches.insert(digest, batch);
                digest_count += 1;
                if digest_count == expected_digest_count {
                    // We loaded all the batches, so we are done.
                    break;
                }
            }
            PackRecord::Consensus(_consensus_header) => {
                return Err(PackError::EpochLoad("unexpected consensusheader found".to_string()))
            }
        }
    }

    // map all fetched batches to their respective certificates for applying block rewards
    let mut batches = Vec::with_capacity(num_certs);
    for header in sub_dag.headers() {
        // create collection of batches to execute for this certificate
        let mut cert_batches = Vec::with_capacity(header.payload().len());

        // retrieve fetched batch by digest
        for digest in header.payload().keys() {
            if let Some(batch) = available_batches.remove(digest) {
                cert_batches.push(batch);
            } else if referenced_batches.contains(digest) {
                // Handle the case with dup batches.  This should be rare to non-existant so not
                // worried about the poor efficiency here.  This allows us
                // to remove in the common case to avoid a batch clone.
                if let Some(batch) = batches
                    .iter()
                    .flat_map(|cb: &CertifiedBatch| cb.batches.iter())
                    .chain(cert_batches.iter())
                    .find(|b| b.digest() == *digest)
                {
                    #[cfg(not(feature = "adiri"))]
                    cert_batches.push(batch.clone());

                    #[cfg(feature = "adiri")]
                    if sub_dag.leader_epoch() > tn_types::forks::ADIRI_DUP_BATCH_EPOCH {
                        // ADIRI BUG
                        // Epoch 74 and possibly other early epochs of adiri testnet had a bug
                        // with duplicate batches. We have to
                        // recreate it in order to sync testnet so we skip this push
                        // on adiri with early epochs.
                        cert_batches.push(batch.clone());
                    }
                } else {
                    return Err(PackError::MissingBatch);
                }
            } else {
                return Err(PackError::MissingBatch);
            }
        }

        let address = committee.authority(header.author()).map(|a| a.execution_address());
        if let Some(address) = address {
            // main collection for execution
            batches.push(CertifiedBatch { address, batches: cert_batches });
        } else {
            return Err(PackError::MissingAuthority);
        }
    }
    Ok(ConsensusOutput::new(
        sub_dag,
        parent_hash,
        consensus_header.number,
        false,
        batch_digests,
        batches,
    ))
}

/// Take an iter over PackRecords that represent a ConsensusOutput and return the ConsensusOutput.
/// Legacy version, expects Batches then the ConsensusHeader.
async fn iter_to_output_legacy<R: AsyncRead + Unpin>(
    stream_iter: &mut AsyncPackIter<PackRecord, R>,
    timeout: Duration,
    committee: &Committee,
    expectation: HeaderExpectation,
) -> Result<ConsensusOutput, PackError> {
    let mut header = None;
    let mut available_batches = HashMap::new();
    let mut referenced_batches = HashSet::new();
    let mut batch_records = 0_usize;
    let max_batches = max_batches_per_output(committee);
    while let Some(record) = next_output_record(stream_iter, timeout).await? {
        match record {
            PackRecord::EpochMeta(_epoch_meta) => {
                return Err(PackError::EpochLoad("unexpected epoch meta data found".to_string()))
            }
            PackRecord::Batch(batch) => {
                // Bound how many batch records a (possibly hostile) stream can deliver before the
                // terminating Consensus record arrives.  Without this an `EpochMeta`/`Consensus`
                // -less flood of Batch records would grow `available_batches` until OOM; the
                // per-record size cap (MAX_RECORD_SIZE) only bounds individual records.  The bound
                // is the maximum a legitimately committed output for this committee
                // can reference (see `max_batches_per_output`), so an honest deep
                // sub-DAG is never rejected.
                batch_records += 1;
                if batch_records > max_batches {
                    return Err(PackError::TooManyBatches(max_batches));
                }
                let batch_digest = batch.digest();
                available_batches.insert(batch_digest, batch);
            }
            PackRecord::Consensus(consensus_header) => {
                // v0 is header-last, so this is as early as the check can run (batches are already
                // buffered); it keeps the parent-link/digest invariant co-located with the header
                // read so `stream_import` need not re-check after decode.
                check_header_expectation(&consensus_header, expectation)?;
                for header in consensus_header.sub_dag.headers() {
                    for (digest, _) in header.payload().iter() {
                        if !available_batches.contains_key(digest) {
                            return Err(PackError::MissingBatches);
                        }
                        referenced_batches.insert(*digest);
                    }
                }
                // batches.len() will generally equal referenced_batches.len() but if it is
                // greater than we had batches that were not accounted for.
                // It is possible (at time of writing) for a batch to
                // be in more than one subdag.  This is also why we don't just
                // remove batches as we check above.
                if available_batches.len() > referenced_batches.len() {
                    return Err(PackError::ExtraBatches);
                }
                header = Some(consensus_header);
                break;
            }
        }
    }
    if let Some(consensus_header) = header {
        let parent_hash = consensus_header.parent_hash;
        let deliver = consensus_header.sub_dag;
        let num_blocks = deliver.num_primary_batches();
        let num_certs = deliver.len();

        let sub_dag = deliver;
        if num_blocks == 0 {
            return Ok(ConsensusOutput::new_with_subdag(
                sub_dag,
                parent_hash,
                consensus_header.number,
            ));
        }

        let mut batch_digests = VecDeque::with_capacity(num_certs);
        for header in sub_dag.headers() {
            for (digest, _) in header.payload().iter() {
                batch_digests.push_back(*digest);
            }
        }

        // map all fetched batches to their respective certificates for applying block rewards
        let mut batches = Vec::with_capacity(num_certs);
        for header in sub_dag.headers() {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(header.payload().len());

            // retrieve fetched batch by digest
            for digest in header.payload().keys() {
                if let Some(batch) = available_batches.remove(digest) {
                    cert_batches.push(batch);
                } else if referenced_batches.contains(digest) {
                    // Handle the case with dup batches.  This should be rare to non-existant so not
                    // worried about the poor efficiency here.  This allows us
                    // to remove in the common case to avoid a batch clone.
                    if let Some(batch) = batches
                        .iter()
                        .flat_map(|cb: &CertifiedBatch| cb.batches.iter())
                        .chain(cert_batches.iter())
                        .find(|b| b.digest() == *digest)
                    {
                        #[cfg(not(feature = "adiri"))]
                        cert_batches.push(batch.clone());

                        #[cfg(feature = "adiri")]
                        if sub_dag.leader_epoch() > tn_types::forks::ADIRI_DUP_BATCH_EPOCH {
                            // ADIRI BUG
                            // Epoch 74 and possibly other early epochs of adiri testnet had a bug
                            // with duplicate batches. We have to
                            // recreate it in order to sync testnet so we skip this push
                            // on adiri with early epochs.
                            cert_batches.push(batch.clone());
                        }
                    } else {
                        return Err(PackError::MissingBatch);
                    }
                } else {
                    return Err(PackError::MissingBatch);
                }
            }

            let address = committee.authority(header.author()).map(|a| a.execution_address());
            if let Some(address) = address {
                // main collection for execution
                batches.push(CertifiedBatch { address, batches: cert_batches });
            } else {
                return Err(PackError::MissingAuthority);
            }
        }
        Ok(ConsensusOutput::new(
            sub_dag,
            parent_hash,
            consensus_header.number,
            false,
            batch_digests,
            batches,
        ))
    } else {
        Err(PackError::NotConsensus)
    }
}

/// Values stored in the position index.
/// Note for v1 format consensus_header and output_start will be the same value.
/// Once v0 is gone we can remove or repurpose on of these fields.
#[derive(Debug, Copy, Clone)]
struct IndexPositions {
    /// The first byte of the ConsensusHeader record for position.
    consensus_header: u64,
    /// The first byte of the first Batch for the output at position.
    /// Reading bytes from output_start..output_end will provide all the
    /// bytes to build the consensus output at position.
    output_start: u64,
    /// The byte after the ConsensusHeader for the output at position.
    output_end: u64,
}

impl IndexPositions {
    fn new(consensus_header: u64, output_start: u64, output_end: u64) -> Self {
        Self { consensus_header, output_start, output_end }
    }
}
impl PosIndexValue for IndexPositions {
    fn encode(&self, buffer: &mut [u8]) {
        if buffer.len() != Self::buffer_len() {
            // Not passing in the exact sized buffer is a coding error so panic vs return error.
            panic!("buffer not 28 bytes");
        }
        let mut crc32_hasher = crc32fast::Hasher::new();
        buffer[..8].copy_from_slice(&self.consensus_header.to_le_bytes());
        buffer[8..16].copy_from_slice(&self.output_start.to_le_bytes());
        buffer[16..24].copy_from_slice(&self.output_end.to_le_bytes());
        crc32_hasher.update(&buffer[0..24]);
        let crc32 = crc32_hasher.finalize();
        buffer[24..28].copy_from_slice(&crc32.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> Result<Self, FetchError> {
        if bytes.len() != Self::buffer_len() {
            // Not passing in the exact sized bytes is a coding error so panic vs return error.
            panic!("input not 28 bytes");
        }
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&bytes[0..24]);
        let crc32 = crc32_hasher.finalize();
        let mut buf32 = [0_u8; 4];
        buf32.copy_from_slice(&bytes[24..28]);
        let crc32_from_buffer = u32::from_le_bytes(buf32);
        if crc32 != crc32_from_buffer {
            return Err(FetchError::CrcFailed);
        }
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        let consensus_header = u64::from_le_bytes(buf);
        buf.copy_from_slice(&bytes[8..16]);
        let output_start = u64::from_le_bytes(buf);
        buf.copy_from_slice(&bytes[16..24]);
        let output_end = u64::from_le_bytes(buf);
        Ok(Self { consensus_header, output_start, output_end })
    }

    /// 28, three u64s and u32 crc.
    fn buffer_len() -> usize {
        28
    }
}

#[derive(Debug, Clone)]
pub enum PackError {
    IO(Arc<io::Error>),
    MissingBatch,
    BatchLoad(String),
    EpochLoad(String),
    Append(String),
    IndexAppend(String),
    Fetch(String),
    Open(Arc<OpenError>),
    ReadOnly,
    NotConsensus,
    NotBatch,
    NotEpoch,
    ReadError(String),
    MissingAuthority,
    InvalidConsensusChain,
    ExtraBatches,
    MissingBatches,
    InvalidEpoch(Epoch, String),
    SendFailed,
    ReceiveFailed,
    PersistError(String),
    InvalidConsensusNumber(u64, u64),
    ConsensusNumberAlreadyAdded,
    CorruptPack,
    ConsensusNumberTooLow,
    ConsensusNumberTooHigh,
    TooManyBatches(usize),
    /// Data pack file version is too new.
    InvalidVersion(u16, u16),
    /// A streamed consensus header's digest did not match the expected (already-verified) digest.
    /// Signals an unambiguous fork or peer misbehavior on the requested-output receive path.
    UnexpectedConsensusDigest {
        expected: ConsensusHeaderDigest,
        got: ConsensusHeaderDigest,
    },
}

impl Error for PackError {}
impl Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::IO(error) => write!(f, "IO({error})"),
            PackError::MissingBatch => write!(f, "Missing Batch"),
            PackError::BatchLoad(error) => write!(f, "Batch Load Error ({error})"),
            PackError::EpochLoad(error) => write!(f, "Epoch Load Error ({error})"),
            PackError::Append(error) => write!(f, "Data Append Error ({error})"),
            PackError::IndexAppend(error) => write!(f, "Index Append Error ({error})"),
            PackError::Fetch(error) => write!(f, "Fetch Error ({error})"),
            PackError::Open(error) => write!(f, "Open Error {error}"),
            PackError::ReadOnly => write!(f, "Read Only"),
            PackError::NotConsensus => write!(f, "Record is not a consensus header"),
            PackError::NotBatch => write!(f, "Record is not a Batch"),
            PackError::NotEpoch => write!(f, "Record is not an EpochMeta"),
            PackError::ReadError(error) => write!(f, "Read Error {error}"),
            PackError::MissingAuthority => write!(f, "Missing authority"),
            PackError::InvalidConsensusChain => write!(f, "Broken consensus record chain"),
            PackError::ExtraBatches => write!(f, "Extra batches in pack file"),
            PackError::MissingBatches => write!(f, "Missing batches in pack file"),
            PackError::InvalidEpoch(epoch, msg) => {
                write!(f, "Epoch meta data incorrect, epoch {epoch}: {msg}")
            }
            PackError::SendFailed => write!(f, "Internal channel send failed"),
            PackError::ReceiveFailed => write!(f, "Internal channel receive failed"),
            PackError::PersistError(e) => write!(f, "Failed to persist: {e}"),
            PackError::InvalidConsensusNumber(expected, got) => {
                write!(f, "Consensus output MUST be added in consective order by number, expected {expected} and got {got}")
            }
            PackError::ConsensusNumberAlreadyAdded => {
                write!(
                    f,
                    "Consensus output MUST be added in consective order by number (already added)"
                )
            }
            PackError::CorruptPack => write!(f, "Pack file is corrupt"),
            PackError::ConsensusNumberTooLow => write!(f, "Consensus number too low for this file"),
            PackError::ConsensusNumberTooHigh => {
                write!(f, "Consensus number too high for this file")
            }
            PackError::TooManyBatches(max) => {
                write!(f, "Too many batches buffered for one consensus output (max {max})")
            }
            PackError::InvalidVersion(expected, got) => {
                write!(f, "Pack file version too new: got {got}, expected {expected}")
            }
            PackError::UnexpectedConsensusDigest { expected, got } => {
                write!(f, "Consensus header digest mismatch: expected {expected}, got {got}")
            }
        }
    }
}

impl From<OpenError> for PackError {
    fn from(value: OpenError) -> Self {
        Self::Open(Arc::new(value))
    }
}

impl From<FetchError> for PackError {
    fn from(value: FetchError) -> Self {
        Self::Fetch(value.to_string())
    }
}

impl From<io::Error> for PackError {
    fn from(value: io::Error) -> Self {
        Self::IO(Arc::new(value))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        collections::VecDeque,
        fs::{File, OpenOptions},
        io::{Seek as _, SeekFrom},
        sync::Arc,
        time::Duration,
    };

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        test_genesis, Batch, BlockHash, Certificate, CertifiedBatch, CommittedSubDag, Committee,
        ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, EpochRecord, ExecHeader, Hash,
        HeaderBuilder, ReputationScores,
    };

    use crate::{
        archive::pack::PackCompression,
        consensus_pack::{max_batches_per_output, ConsensusPack, Inner, PACK_VERSION},
        mem_db::MemDatabase,
    };

    /// Build a [`ConsensusOutput`] whose single leader header references `num_batches` unique
    /// batches, standing in for a deep sub-DAG that exceeds the old fixed reconstruction cap
    /// but stays within the committee-derived bound.
    fn make_wide_test_output(
        committee: &Committee,
        chain: Arc<RethChainSpec>,
        number: u64,
        parent: ConsensusHeaderDigest,
        num_batches: usize,
    ) -> ConsensusOutput {
        // Reuse one transaction across many cheaply-distinct batches (each batch differs only by
        // its `epoch` field, which is enough to give it a unique digest) so a wide output
        // does not generate O(n^2) transactions.
        let txs =
            tn_reth::test_utils::batches(chain, 1).pop().expect("one batch").transactions().clone();
        let batches: Vec<Batch> = (0..num_batches as u32)
            .map(|epoch| Batch::new_for_test(txs.clone(), ExecHeader::default(), 0, epoch))
            .collect();
        let authorities = committee.authorities();
        let authority = authorities.first().expect("committee has authorities");
        let author_id = authority.id();
        let producer = authority.execution_address();

        let mut leader = Certificate::default();
        leader.update_header_author_for_test(author_id);
        // Accumulate the whole payload on a single builder so the header is only hashed once.
        let header = batches
            .iter()
            .fold(HeaderBuilder::from_header(leader.header()), |builder, batch| {
                builder.with_payload_batch(batch, 0_u16)
            })
            .build();
        leader.update_header_for_test(header);
        leader.update_header_round_for_test(1);
        leader.update_header_epoch_for_test(committee.epoch());

        let batch_digests: VecDeque<BlockHash> = batches.iter().map(|b| b.digest()).collect();
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
        );
        ConsensusOutput::new(
            sub_dag,
            parent,
            number,
            false,
            batch_digests,
            vec![CertifiedBatch { address: producer, batches }],
        )
    }

    pub(crate) fn make_test_output(
        committee: &Committee,
        authority_index: usize,
        chain: Arc<RethChainSpec>,
        number: u64,
        parent: ConsensusHeaderDigest,
    ) -> ConsensusOutput {
        let batches_1 = tn_reth::test_utils::batches(chain, 4); // create 4 batches
        let authority_1 = committee
            .authorities()
            .get(authority_index)
            .expect("first in 4 auth committee for tests")
            .id();
        let batch_producer = committee
            .authorities()
            .get(authority_index)
            .expect("authority in committee")
            .execution_address();

        let mut leader_1 = Certificate::default();
        // update cert
        leader_1.update_header_author_for_test(authority_1);
        for batch in &batches_1 {
            let mut builder = HeaderBuilder::from_header(leader_1.header());
            builder = builder.with_payload_batch(&batch, 0_u16);
            leader_1.update_header_for_test(builder.build());
        }
        let sub_dag_index_1 = 1;
        leader_1.update_header_round_for_test(sub_dag_index_1 as u32);
        leader_1.update_header_epoch_for_test(committee.epoch());
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = CommittedSubDag::new(
            vec![leader_1.clone()],
            leader_1,
            sub_dag_index_1,
            reputation_scores,
            previous_sub_dag,
        );
        ConsensusOutput::new(
            subdag_1.clone(),
            parent,
            number,
            false,
            batch_digests_1.clone(),
            vec![CertifiedBatch { address: batch_producer, batches: batches_1 }],
        )
    }

    /// Make a test output with two certificates from different authorities that share one
    /// batch digest.  The shared batch is only stored once in the pack file but must be
    /// assigned to both certificates when the output is rebuilt.
    fn make_test_output_shared_batch(
        committee: &Committee,
        chain: Arc<RethChainSpec>,
        number: u64,
        parent: ConsensusHeaderDigest,
    ) -> ConsensusOutput {
        let mut batches = tn_reth::test_utils::batches(chain, 3);
        let batch_2 = batches.pop().expect("three batches");
        let batch_1 = batches.pop().expect("three batches");
        let batch_0 = batches.pop().expect("three batches");

        let authorities = committee.authorities();
        let authority_a = authorities.first().expect("first in 4 auth committee");
        let authority_b = authorities.get(1).expect("second in 4 auth committee");

        let mut cert_a = Certificate::default();
        cert_a.update_header_author_for_test(authority_a.id());
        for batch in [&batch_0, &batch_1] {
            let builder =
                HeaderBuilder::from_header(cert_a.header()).with_payload_batch(batch, 0_u16);
            cert_a.update_header_for_test(builder.build());
        }
        cert_a.update_header_round_for_test(1);
        cert_a.update_header_epoch_for_test(committee.epoch());

        let mut cert_b = Certificate::default();
        cert_b.update_header_author_for_test(authority_b.id());
        // batch_1 is shared with cert_a's payload.
        for batch in [&batch_1, &batch_2] {
            let builder =
                HeaderBuilder::from_header(cert_b.header()).with_payload_batch(batch, 0_u16);
            cert_b.update_header_for_test(builder.build());
        }
        cert_b.update_header_round_for_test(1);
        cert_b.update_header_epoch_for_test(committee.epoch());

        let sub_dag = CommittedSubDag::new(
            vec![cert_a.clone(), cert_b.clone()],
            cert_b,
            1,
            ReputationScores::default(),
            None,
        );
        let batch_digests: VecDeque<BlockHash> =
            [batch_0.digest(), batch_1.digest(), batch_1.digest(), batch_2.digest()]
                .into_iter()
                .collect();
        ConsensusOutput::new(
            sub_dag,
            parent,
            number,
            false,
            batch_digests,
            vec![
                CertifiedBatch {
                    address: authority_a.execution_address(),
                    batches: vec![batch_0, batch_1.clone()],
                },
                CertifiedBatch {
                    address: authority_b.execution_address(),
                    batches: vec![batch_1, batch_2],
                },
            ],
        )
    }

    pub(crate) fn compare_outputs(output1: &ConsensusOutput, output2: &ConsensusOutput) {
        assert_eq!(output1.digest(), output2.digest(), "Consensus Output have different hashes");
        assert_eq!(
            output1.batch_digests().len(),
            output2.batch_digests().len(),
            "Batch digests not the same length"
        );
        for (bi, batch_digest) in output1.batch_digests().iter().enumerate() {
            assert_eq!(
                batch_digest,
                output2.batch_digests().get(bi).unwrap(),
                "Batch digests are not the same"
            );
        }
        assert_eq!(output1.batches().len(), output2.batches().len(), "Batches not the same length");
        for (bi, batch) in output1.batches().iter().enumerate() {
            let batch2 = output2.batches().get(bi).unwrap();
            assert_eq!(batch.address, batch2.address);
            assert_eq!(
                batch.batches.len(),
                batch2.batches.len(),
                "Batch lengths within the certified batch are not the same"
            );
            for (b1, b2) in batch.batches.iter().zip(batch2.batches.iter()) {
                assert_eq!(b1, b2, "Batches (with certified batch) not the same");
            }
        }
    }

    /// Poll `condition` every 25ms until it holds, panicking with a clear message if it
    /// does not become true within 10s. Bounded, event-driven replacement for fixed sleeps.
    async fn wait_for(mut condition: impl AsyncFnMut() -> bool, msg: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition().await {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out after 10s waiting for {msg}"));
    }

    #[tokio::test]
    async fn test_pack_save_wrong_epoch_rejected() {
        let temp_dir = TempDir::with_prefix("test_pack_wrong_epoch").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
            .expect("open pack");

        // An output whose leader epoch differs from the pack's epoch must be rejected by
        // Inner::save_consensus_output rather than appended at a saturated index.
        let next_committee = committee.advance_epoch_for_test(1);
        let parent = ConsensusHeader::default().digest();
        let wrong = make_test_output(&next_committee, 0, chain.clone(), 1, parent);
        assert_ne!(wrong.sub_dag().leader_epoch(), committee.epoch());
        let err = pack.save_consensus_output(wrong).await;

        assert!(
            matches!(err, Err(super::PackError::InvalidEpoch(..))),
            "expected InvalidEpoch, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_consensus_pack() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            // If we can't find the recort then this we should be starting at epoch 0- use this
            // filler.
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        // Create and load some data in initial file.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");

        let num_outputs = 1000;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            pack.save_consensus_output(consensus_output).await.unwrap();
        }
        for i in 0..num_outputs {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("consensus output for {}", i + 1));
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }

        pack.persist().await.expect("persist");
        drop(pack);

        // Reopen in append and load some more data.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        for i in 0..num_outputs {
            let consensus_output = make_test_output(
                &committee,
                i % 4,
                chain.clone(),
                (i + num_outputs) as u64 + 1,
                parent,
            );
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            pack.save_consensus_output(consensus_output).await.unwrap();
        }
        for i in 0..(num_outputs * 2) {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .unwrap_or_else(|e| panic!("failed output on {i}: {e}"));
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        pack.persist().await.expect("persist");
        drop(pack);

        // Open read only and verify.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).unwrap();
        for i in 0..(num_outputs * 2) {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        assert!(pack.get_consensus_output(num_outputs as u64 * 2).await.is_ok());
        drop(pack);

        // Make sure we can stream the file to create another pack file.
        {
            let temp_dir2 = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
            let stream =
                tokio::fs::File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME))
                    .await
                    .expect("log file");
            let pack = ConsensusPack::stream_import(
                temp_dir2.path(),
                stream,
                0,
                &previous_epoch,
                num_outputs as u64 * 2,
                Duration::from_secs(5),
            )
            .await
            .expect("open pack");
            // stream_import fully drains the stream before returning, so the data already
            // lives in the pack thread; wait (bounded) for the last output to be readable
            // instead of sleeping a fixed 2s.
            wait_for(
                async || pack.get_consensus_output(num_outputs as u64 * 2).await.is_ok(),
                "last stream-imported consensus output to be readable",
            )
            .await;
            for i in 0..num_outputs {
                let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
                let output = outputs.get(i as usize).unwrap();
                compare_outputs(&output_db, output);
            }
            for i in 0..num_outputs {
                let output_db =
                    pack.get_consensus_output((i + num_outputs) as u64 + 1).await.unwrap();
                let output = outputs.get(i as usize + num_outputs).unwrap();
                compare_outputs(&output_db, output);
            }
            assert!(pack.get_consensus_output(num_outputs as u64 * 2).await.is_ok());
            drop(pack);

            let mut f1 = File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME))
                .expect("log file");
            let mut f2 = File::open(temp_dir2.path().join("epoch-0").join(Inner::DATA_NAME))
                .expect("log file");
            assert_eq!(
                f1.seek(SeekFrom::End(0)).unwrap(),
                f2.seek(SeekFrom::End(0)).unwrap(),
                "files not the same length"
            );
        }

        let mut stream = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME))
            .expect("log file");
        let stream_len = stream.seek(SeekFrom::End(0)).expect("stream length");
        stream.set_len(stream_len - 1).unwrap(); // Truncate last byte which will damage last record.
        drop(stream);
        // Reopen in append and load some more data.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        for i in 0..(num_outputs * 2) - 1 {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        assert!(pack.get_consensus_output(num_outputs as u64 * 2).await.is_err());
        let last_output = outputs.last().unwrap().clone();
        pack.save_consensus_output(last_output).await.unwrap();

        for i in 0..(num_outputs * 2) - 1 {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }

        let output_db = pack.get_consensus_output(num_outputs as u64 * 2).await.unwrap();
        let output = outputs.get((num_outputs as usize * 2) - 1).unwrap();
        compare_outputs(&output_db, output);
        pack.persist().await.unwrap();
        drop(pack);
        let mut stream =
            File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME)).expect("log file");
        let stream2_len = stream.seek(SeekFrom::End(0)).expect("stream length");
        assert_eq!(stream_len, stream2_len);
        drop(stream);

        let mut stream = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME))
            .expect("log file");
        let stream_len = stream.seek(SeekFrom::End(0)).expect("stream length");
        stream.set_len(stream_len + 100).unwrap(); // Truncate last byte which will damage last record.
        drop(stream);
        // Reopen in append and load some more data.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        for i in 0..(num_outputs * 2) {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        drop(pack);
        let mut stream =
            File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME)).expect("log file");
        let stream2_len = stream.seek(SeekFrom::End(0)).expect("stream length");
        drop(stream);
        assert_eq!(stream_len, stream2_len);
    }

    /// Regression test: one batch digest referenced by two certificates within a single
    /// consensus output.  The batch is stored once in the pack file and must be assigned
    /// to both certificates when the output is rebuilt (previously failed with
    /// PackError::MissingBatch).
    #[tokio::test]
    async fn test_consensus_pack_dup_batch_across_certs() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack_dup").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");

        // Output 1 contains the shared batch, output 2 is a normal output to confirm
        // the pack continues cleanly after a duplicate.
        let output_1 = make_test_output_shared_batch(
            &committee,
            chain.clone(),
            1,
            ConsensusHeader::default().digest(),
        );
        let output_2 = make_test_output(&committee, 2, chain.clone(), 2, output_1.digest().into());
        pack.save_consensus_output(output_1.clone()).await.unwrap();
        pack.save_consensus_output(output_2.clone()).await.unwrap();

        compare_outputs(&pack.get_consensus_output(1).await.expect("dup batch output"), &output_1);
        compare_outputs(&pack.get_consensus_output(2).await.expect("output after dup"), &output_2);
        pack.persist().await.expect("persist");
        drop(pack);

        // Read back through the read only static path.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
        compare_outputs(&pack.get_consensus_output(1).await.expect("dup batch output"), &output_1);
        compare_outputs(&pack.get_consensus_output(2).await.expect("output after dup"), &output_2);
        drop(pack);

        // Stream into a new pack (peer epoch sync path) and read back.
        let temp_dir2 = TempDir::with_prefix("test_consensus_pack_dup2").expect("temp dir");
        let stream = tokio::fs::File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME))
            .await
            .expect("log file");
        let pack = ConsensusPack::stream_import(
            temp_dir2.path(),
            stream,
            0,
            &previous_epoch,
            2,
            Duration::from_secs(5),
        )
        .await
        .expect("stream import");
        compare_outputs(&pack.get_consensus_output(1).await.expect("dup batch output"), &output_1);
        compare_outputs(&pack.get_consensus_output(2).await.expect("output after dup"), &output_2);
        drop(pack);
    }

    fn test_previous_epoch(committee: &Committee) -> EpochRecord {
        EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        }
    }

    /// CP1: a peer stream that floods batch records without a terminating Consensus record must
    /// be rejected with TooManyBatches instead of buffering them all into memory.
    #[tokio::test]
    async fn test_iter_to_output_caps_buffered_batches() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_output, max_batches_per_output, PackError, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_batch_cap").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        // The reader bound is derived from this committee, so an unauthenticated flood past it must
        // still be rejected to guard against OOM.
        let max_batches = max_batches_per_output(&committee);

        // Build a record stream of more batch records than the cap with no Consensus record.
        let path = temp_dir.path().join("batch_only");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            let batch = tn_reth::test_utils::batches(chain.clone(), 1).pop().expect("one batch");
            for _ in 0..(max_batches + 5) {
                pack.append(&PackRecord::Batch(batch.clone())).expect("append batch");
            }
            pack.commit().expect("commit");
        }
        // bytes_to_output uses open_partial (no header) so feed the records past the data header.
        let file_bytes = std::fs::read(&path).expect("read file");
        let records = file_bytes[DATA_HEADER_BYTES..].to_vec();

        let res = bytes_to_output(
            Cursor::new(records),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
        )
        .await;
        // New format will fail by starting with a batch.  This would be TooManyBatches with v0.
        assert!(matches!(res, Err(PackError::BatchLoad(_))), "expected BatchLoad");
    }

    /// CP1b: in the v1 (header-first) format a hostile header whose sub-dag declares more than the
    /// committee-derived bound (`max_batches_per_output`) of payload digests must be rejected up
    /// front, before any batch is read, rather than allocating/reading a batch per declared digest.
    #[tokio::test]
    async fn test_iter_to_output_caps_expected_batches() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_output, max_batches_per_output, PackError, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_expected_cap").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // Craft a single consensus header whose leader references more distinct batch digests than
        // the committee-derived bound, cheaply (one shared tx, header hashed once).
        let max_batches = max_batches_per_output(&committee);
        let output = make_wide_test_output(
            &committee,
            chain.clone(),
            1,
            ConsensusHeader::default().digest(),
            max_batches + 5,
        );
        assert!(output.sub_dag().num_primary_batches() > max_batches, "test must exceed the cap");

        // Write just the header record (v1: header first) with no batch records to follow.
        let path = temp_dir.path().join("header_only");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            pack.append(&PackRecord::Consensus(Box::new(output.consensus_header())))
                .expect("append header");
            pack.commit().expect("commit");
        }
        let file_bytes = std::fs::read(&path).expect("read file");
        let records = file_bytes[DATA_HEADER_BYTES..].to_vec();

        let res = bytes_to_output(
            Cursor::new(records),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
        )
        .await;
        assert!(matches!(res, Err(PackError::TooManyBatches(_))), "expected TooManyBatches");
    }

    /// A `ConsensusOutput` that references more than the old fixed 1000-batch cap but stays within
    /// the committee-derived bound must round-trip through the pack: it is executed live on
    /// every node, so it must always be reconstructable.  Regression test for the writer/reader
    /// batch-count asymmetry (#896) — before the fix the write succeeded but the read failed
    /// with `TooManyBatches`, wedging restart replay and observer sync.
    #[tokio::test]
    async fn test_deep_output_round_trips() {
        let temp_dir = TempDir::with_prefix("test_cp_deep_output").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        let max_batches = max_batches_per_output(&committee);
        assert!(max_batches > 1_000, "derived bound must exceed the old fixed 1000 cap");
        // Exceeds the old fixed cap yet stays within the committee-derived bound.
        let num_batches = 1_100;
        assert!(num_batches < max_batches, "test output must fit within the derived bound");

        let previous_epoch = test_previous_epoch(&committee);
        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
            .expect("open pack");
        let parent = ConsensusHeader::default().digest();
        let output = make_wide_test_output(&committee, chain.clone(), 1, parent, num_batches);
        pack.save_consensus_output(output.clone()).await.expect("save deep output");
        let read_back = pack.get_consensus_output(1).await.expect("read back deep output");
        compare_outputs(&output, &read_back);
    }

    /// The verified single-output decode ([`bytes_to_verified_output`]) accepts an output whose
    /// header hashes to the expected digest and returns the equal output, and rejects one that does
    /// not with [`PackError::UnexpectedConsensusDigest`] carrying the real header digest.
    #[tokio::test]
    async fn test_bytes_to_verified_output_accepts_and_rejects() {
        use crate::consensus_pack::{bytes_to_verified_output, PackError};
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_verified_output").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
            .expect("open pack");
        let parent = ConsensusHeader::default().digest();
        let original = make_test_output(&committee, 0, chain, 1, parent);
        pack.save_consensus_output(original.clone()).await.unwrap();
        pack.persist().await.expect("persist");
        // v1 pack serves header-first record bytes (no data header), exactly what the sync stream
        // reassembles and what `bytes_to_verified_output` (open_partial) consumes.
        let bytes = pack.get_consensus_output_bytes(1).await.expect("bytes");

        // Correct digest: accepted and equal to the original.
        let decoded = bytes_to_verified_output(
            Cursor::new(bytes.clone()),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
            original.digest(),
        )
        .await
        .expect("verified decode");
        compare_outputs(&decoded, &original);

        // Wrong digest: rejected with UnexpectedConsensusDigest reporting the real header digest.
        let wrong = ConsensusHeader::default().digest();
        assert_ne!(wrong, original.digest(), "wrong digest must differ from the real one");
        let res = bytes_to_verified_output(
            Cursor::new(bytes),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
            wrong,
        )
        .await;
        match res {
            Err(PackError::UnexpectedConsensusDigest { expected, got }) => {
                assert_eq!(expected, wrong);
                assert_eq!(got, original.digest());
            }
            other => panic!("expected UnexpectedConsensusDigest, got {other:?}"),
        }
        drop(pack);
    }

    /// Load-bearing security assertion: the verified decode rejects a wrong-hash header BEFORE
    /// reading any batch. Fed a header-only stream (the header declares batches, none follow), a
    /// wrong expected digest yields [`PackError::UnexpectedConsensusDigest`] — NOT a
    /// missing/too-many-batches error — proving the header hash is checked before batch records are
    /// read (so an unverified peer cannot force buffering the declared batches). A zero-batch
    /// header with the correct digest is accepted (the `num_blocks == 0` short-circuit runs
    /// only after the header check passes).
    #[tokio::test]
    async fn test_bytes_to_verified_output_rejects_before_batches() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_verified_output, PackError, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_verified_early").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // Serialize a single Consensus header record (v1: header first) with NO batch records.
        let header_only = |name: &str, header: PackRecord| -> Vec<u8> {
            let path = temp_dir.path().join(name);
            {
                let mut pack: Pack<PackRecord> =
                    Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                        .expect("open pack");
                pack.append(&header).expect("append header");
                pack.commit().expect("commit");
            }
            std::fs::read(&path).expect("read file")[DATA_HEADER_BYTES..].to_vec()
        };

        // A header that declares batches, with none following. A wrong expected digest is caught at
        // the header — if the check ran after batches this would be a MissingBatch / read error.
        let output = make_test_output(&committee, 0, chain, 1, ConsensusHeader::default().digest());
        assert!(output.sub_dag().num_primary_batches() > 0, "header must declare batches");
        let records =
            header_only("hdr", PackRecord::Consensus(Box::new(output.consensus_header())));
        let res = bytes_to_verified_output(
            Cursor::new(records),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
            ConsensusHeader::default().digest(),
        )
        .await;
        match res {
            Err(PackError::UnexpectedConsensusDigest { got, .. }) => {
                assert_eq!(got, output.digest(), "must report the real header digest");
            }
            other => {
                panic!("expected UnexpectedConsensusDigest before any batch read, got {other:?}")
            }
        }

        // A zero-batch header with the CORRECT digest is accepted.
        let empty = ConsensusHeader::default();
        let records = header_only("empty", PackRecord::Consensus(Box::new(empty.clone())));
        let decoded = bytes_to_verified_output(
            Cursor::new(records),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
            empty.digest(),
        )
        .await
        .expect("zero-batch verified decode");
        assert_eq!(decoded.consensus_header().digest(), empty.digest());
    }

    /// A v0 (legacy, batches-first) pack must serve its outputs as v1 (header-first) bytes via
    /// `get_consensus_output_bytes`, so all peer-facing bytes are v1 regardless of on-disk format.
    #[tokio::test]
    async fn test_v0_output_served_as_v1_bytes() {
        use crate::{
            archive::pack_iter::AsyncPackIter,
            consensus_pack::{
                bytes_to_output, bytes_to_output_legacy, bytes_to_verified_output, PackError,
                PackRecord,
            },
        };
        use std::io::Cursor;
        use tokio::io::BufReader;

        let temp_dir = TempDir::with_prefix("test_v0_served_v1").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Force a genuine v0 pack file (header stamped version 0 -> batches-first on disk).
        let pack = ConsensusPack::open_append_version(
            temp_dir.path(),
            previous_epoch,
            committee.clone(),
            0,
        )
        .expect("open v0 pack");
        assert_eq!(pack.version, 0, "constructor must produce a v0 pack file");

        let num_outputs = 5;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.unwrap();
        }
        pack.persist().await.expect("persist");

        for (i, original) in outputs.iter().enumerate() {
            let number = i as u64 + 1;
            let bytes = pack.get_consensus_output_bytes(number).await.expect("bytes");

            // 1. Header-first: the first record must be a Consensus header (v1 ordering). A v0 file
            //    would have yielded a Batch first.
            let mut iter = AsyncPackIter::<PackRecord, _>::open_partial(
                BufReader::new(Cursor::new(bytes.clone())),
                PackCompression::ZStd,
                PACK_VERSION,
            )
            .await
            .expect("open partial");
            match iter.next().await {
                Some(Ok(PackRecord::Consensus(_))) => {}
                other => panic!("expected first record to be a Consensus header, got {other:?}"),
            }

            // 2. Decodes with the v1 decoder and matches the original output.
            let decoded = bytes_to_output(
                BufReader::new(Cursor::new(bytes.clone())),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
            )
            .await
            .expect("v1 decode");
            compare_outputs(&decoded, original);

            // 2b. The verified single-output decode accepts these served v1 bytes with the real
            //     header digest and rejects a flipped digest with UnexpectedConsensusDigest.
            let verified = bytes_to_verified_output(
                BufReader::new(Cursor::new(bytes.clone())),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
                original.digest(),
            )
            .await
            .expect("verified v1 decode");
            compare_outputs(&verified, original);
            let rejected = bytes_to_verified_output(
                BufReader::new(Cursor::new(bytes.clone())),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
                ConsensusHeader::default().digest(),
            )
            .await;
            assert!(
                matches!(rejected, Err(PackError::UnexpectedConsensusDigest { .. })),
                "flipped digest must be rejected, got {rejected:?}"
            );

            // 3. The bytes are truly re-ordered: the legacy (batches-first) decoder rejects them.
            let legacy = bytes_to_output_legacy(
                BufReader::new(Cursor::new(bytes)),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
            )
            .await;
            assert!(legacy.is_err(), "legacy decode of v1 bytes must fail, got {legacy:?}");

            // The local read path still honors the on-disk v0 format (legacy decode).
            compare_outputs(
                &pack.get_consensus_output(number).await.expect("local read"),
                original,
            );
        }
        drop(pack);
    }

    /// A v0 (batches-first) pack that stores a SHARED batch (one digest referenced by two certs)
    /// must serve that output as v1 (header-first) bytes that decode back to the identical output,
    /// with the shared batch reassigned to BOTH certs. Guards the exact mixed-testnet path: a
    /// pre-upgrade v0 file served as v1 for a duplicate-batch output.
    #[tokio::test]
    async fn test_v0_shared_batch_served_as_v1_bytes() {
        use crate::consensus_pack::{bytes_to_output, bytes_to_verified_output};
        use std::io::Cursor;
        use tokio::io::BufReader;

        let temp_dir = TempDir::with_prefix("test_v0_shared_v1").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Genuine v0 (batches-first) pack on disk.
        let pack = ConsensusPack::open_append_version(
            temp_dir.path(),
            previous_epoch,
            committee.clone(),
            0,
        )
        .expect("open v0 pack");
        assert_eq!(pack.version, 0, "constructor must produce a v0 pack file");

        // Output 1 shares one batch across two certs; output 2 is a normal output confirming the
        // pack keeps serving cleanly after a duplicate.
        let out1 = make_test_output_shared_batch(
            &committee,
            chain.clone(),
            1,
            ConsensusHeader::default().digest(),
        );
        let out2 = make_test_output(&committee, 2, chain.clone(), 2, out1.digest().into());
        pack.save_consensus_output(out1.clone()).await.unwrap();
        pack.save_consensus_output(out2.clone()).await.unwrap();
        pack.persist().await.expect("persist");

        for original in [&out1, &out2] {
            let number = original.number();
            // v0 stored -> served as v1 (header-first) bytes.
            let bytes = pack.get_consensus_output_bytes(number).await.expect("bytes");

            // v1 decode reconstructs the identical output, including the shared batch assigned to
            // both certs (compare_outputs deep-checks batch_digests incl. the dup and each cert).
            let decoded = bytes_to_output(
                BufReader::new(Cursor::new(bytes.clone())),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
            )
            .await
            .expect("v1 decode");
            compare_outputs(&decoded, original);

            // The verified single-output path also round-trips a v0-origin shared-batch output.
            let verified = bytes_to_verified_output(
                BufReader::new(Cursor::new(bytes)),
                PackCompression::ZStd,
                Duration::from_secs(5),
                &committee,
                original.digest(),
            )
            .await
            .expect("verified v1 decode");
            compare_outputs(&verified, original);
        }
        drop(pack);
    }

    /// CP2: get_consensus_output with a number below start_consensus_number must error rather
    /// than saturating to index 0 and silently returning the first output.
    #[tokio::test]
    async fn test_get_consensus_output_rejects_below_range() {
        let temp_dir = TempDir::with_prefix("test_cp_oob_number").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
            .expect("open pack");
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..3 {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest().into();
            pack.save_consensus_output(output).await.unwrap();
        }
        // start_consensus_number is 1 for epoch 0; 0 is below range.
        assert!(pack.get_consensus_output(0).await.is_err(), "number below start must error");
        assert!(pack.get_consensus_output(1).await.is_ok(), "in-range number works");
    }

    /// CP3: a pack healed on open (truncating a damaged tail) but not followed by a save must
    /// still reconcile the index lengths, so a later read-only open passes the consistency check.
    #[tokio::test]
    async fn test_heal_without_save_then_open_static() {
        let temp_dir = TempDir::with_prefix("test_cp_heal_static").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open pack");
            let mut parent = ConsensusHeader::default().digest();
            for i in 0..5 {
                let output =
                    make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
                parent = output.digest().into();
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }

        // Damage the tail of the data file (truncate last byte of the last record).
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            let len = f.metadata().expect("meta").len();
            f.set_len(len - 1).expect("truncate");
        }

        // Open append: heals (truncates the damaged record) but we do NOT save afterward.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append heals");
            pack.persist().await.expect("persist after heal");
        }

        // A read-only open runs files_consistent; with the index lengths reconciled during heal
        // this must succeed rather than reporting CorruptPack.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after heal");
        // The healed pack dropped the damaged 5th output; the first four remain readable.
        assert!(pack.get_consensus_output(1).await.is_ok());
        assert!(pack.get_consensus_output(4).await.is_ok());
    }

    /// `verify_epoch_meta` committee linkage across the shapes a mid-epoch on-chain ejection
    /// (governance `burn` / slash-to-zero) produces. The committee check is set-based
    /// (`BTreeSet`), so the stored order of `next_committee` must not matter — only shrinking
    /// or growing the set does.
    #[test]
    fn test_verify_epoch_meta_across_ejection_shapes() {
        use std::collections::BTreeMap;

        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{Address, Authority, BlsKeypair, BlsPublicKey, ConsensusNumHash, Epoch};

        use crate::consensus_pack::{verify_epoch_meta, EpochMeta, PackError};

        let mut rng = StdRng::seed_from_u64(0xE2EC7);
        let keypairs: Vec<BlsKeypair> = (0..5).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let keys: Vec<BlsPublicKey> = keypairs.iter().map(|kp| *kp.public()).collect();

        let build_committee = |members: &[BlsPublicKey], epoch: Epoch| {
            let authorities = members
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, Authority::new_for_test(*k, Address::repeat_byte(i as u8 + 1))))
                .collect::<BTreeMap<_, _>>();
            Committee::new_for_test(authorities, epoch, BTreeMap::default())
        };

        let meta_for = |epoch: Epoch, committee: &Committee, prev: &EpochRecord| EpochMeta {
            epoch,
            committee: committee.clone(),
            start_consensus_number: prev.final_consensus.number + 1,
            genesis_exec_state: prev.final_state,
            genesis_consensus: prev.final_consensus,
        };

        // Swap-and-pop ejection of keys[2] out of the five-member committee.
        let survivors = vec![keys[0], keys[1], keys[4], keys[3]];
        let committee5 = build_committee(&keys, 1);
        let committee4 = build_committee(&survivors, 1);

        // rec0: pre-ejection record. `next_committee` is deliberately stored in reversed
        // order to pin that the comparison is order-insensitive.
        let mut next0 = keys.clone();
        next0.reverse();
        let rec0 = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: next0,
            final_consensus: ConsensusNumHash::new(10, ConsensusHeaderDigest::default()),
            ..Default::default()
        };

        // Full record committee vs full meta committee (any order) → Ok.
        verify_epoch_meta(1, &rec0, &meta_for(1, &committee5, &rec0))
            .expect("full vs full must verify");

        // Full record vs shrunken meta → Err: the meta was rebuilt from a post-ejection
        // chain read while the record predates the burn.
        let err = verify_epoch_meta(1, &rec0, &meta_for(1, &committee4, &rec0))
            .expect_err("full vs shrunken must fail");
        assert!(matches!(err, PackError::InvalidEpoch(1, _)), "got {err:?}");

        // rec1: the ejection epoch's record — committee and next committee both shrunken.
        let rec1 = EpochRecord {
            epoch: 1,
            committee: survivors.clone(),
            next_committee: survivors.clone(),
            parent_hash: rec0.digest(),
            final_consensus: ConsensusNumHash::new(20, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        let committee4_next = committee4.advance_epoch_for_test(2);
        let committee5_next = committee5.advance_epoch_for_test(2);

        // Shrunken record vs shrunken meta → Ok: the epoch after the ejection opens cleanly.
        verify_epoch_meta(2, &rec1, &meta_for(2, &committee4_next, &rec1))
            .expect("shrunken vs shrunken must verify");

        // Shrunken record vs full meta → Err: a committee cannot silently grow back.
        let err = verify_epoch_meta(2, &rec1, &meta_for(2, &committee5_next, &rec1))
            .expect_err("shrunken vs full must fail");
        assert!(matches!(err, PackError::InvalidEpoch(2, _)), "got {err:?}");
    }
}
