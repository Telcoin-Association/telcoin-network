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
    digest_index::HdxIndex,
    error::{
        fetch::FetchError,
        load_header::LoadHeaderError,
        open::OpenError::{self, DataFileOpen},
    },
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
    LatestConsensusHeader(oneshot::Sender<Result<Option<ConsensusHeader>, PackError>>),
    Shutdown,
    // Flush the write buffer to the data file WITHOUT fsync, so freshly appended bytes
    /// become visible to other file handles on the same file (visibility, not durability).
    FlushData(oneshot::Sender<Result<(), PackError>>),
    /// Reconcile the physical data file down to its logical length (drop mmap capacity padding) so
    /// a separate file handle reading to EOF observes exactly the written bytes.
    ReconcileDataLen(oneshot::Sender<Result<(), PackError>>),
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
            PackMessage::ReconcileDataLen(tx) => {
                let _ = tx.send(inner.reconcile_data_len());
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
    fn open_append_version<P: Into<PathBuf>>(
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
        decode_output_bytes(bytes, self.version, self.compression, &self.committee).await
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
        let bytes = if self.tx.send(PackMessage::BytesForConsensus(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)?
        } else {
            Err(PackError::SendFailed)
        }?;
        serve_output_bytes(bytes, self.version, self.compression, &self.committee).await
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

    /// Reconcile the on-disk data file down to its logical length (dropping any mmap capacity
    /// padding) so a separate handle reading the file to EOF observes exactly the written bytes.
    pub async fn reconcile_data_len(&self) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::ReconcileDataLen(tx)).await;
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
    ///
    /// Fails closed: a read or channel failure is returned, never reported as "no header". See the
    /// note on the inner reader.
    pub async fn latest_consensus_header(&self) -> Result<Option<ConsensusHeader>, PackError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::LatestConsensusHeader(tx)).await.is_ok() {
            rx.await.unwrap_or(Err(PackError::SendFailed))
        } else {
            Err(PackError::SendFailed)
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

    /// Rebuild the position and digest indexes from the data-log WAL and, if the log's final record
    /// is torn, truncate it so the pack is self-consistent again.
    ///
    /// Runs on open when [`Self::files_consistent`] fails — either the indexes were not synced (so
    /// they lag the durable data log) or an unclean shutdown left a torn record at the tail. The
    /// data log is the source of truth: the indexes are dropped and rebuilt by replaying every
    /// complete consensus output in insert order (`EpochMeta`, then a `Consensus` header followed
    /// by its `Batch` records). Each output is finalized only once all of its records have been
    /// read (the header's sub-dag names exactly how many batches it owns), so a torn *next*
    /// header keeps the last complete output while a torn *batch* drops just its own
    /// (incomplete) output. Damage anywhere but the final record cannot be a clean tail and is
    /// reported as [`PackError::CorruptPack`].
    ///
    /// v1 (header-first) format only: `open_static` rejects an inconsistent read-only pack rather
    /// than healing, so recovery only runs on the writable append opens, which are always current
    /// format.
    fn recover_pack<P: AsRef<Path>>(
        data: &mut Pack<PackRecord>,
        base_dir: P,
        mut consensus_pos_idx: PositionIndex<IndexPositions>,
        consensus_digests: HdxIndex,
        batch_digests: HdxIndex,
    ) -> Result<(PositionIndex<IndexPositions>, HdxIndex, HdxIndex), PackError> {
        if Self::files_consistent(data, &mut consensus_pos_idx, &consensus_digests, &batch_digests)
        {
            return Ok((consensus_pos_idx, consensus_digests, batch_digests));
        }
        let base_dir = base_dir.as_ref();
        // Highest record end either index attests as durably indexed: the digest index's
        // `data_file_length` (the commit marker written last on an index sync) and the position
        // index's last `output_end`. Indexes sync on a clean close, not on every `persist()`, so
        // the data WAL can legitimately run *past* this with unacked appends -- a torn record out
        // there is the normal mmap out-of-order-writeback tail, safe to drop even if later records
        // still decode. But a tear that leaves the recovered prefix ending *below* an attested
        // record end means an acked record was lost -- real corruption. Capture both before the
        // indexes are reset just below. (A fresh/rebuilt index reports `DATA_HEADER_BYTES`, so the
        // guard degrades to "trust the WAL, truncate at the first tear".)
        let attested_end = {
            let digest_end = consensus_digests.data_file_length();
            let pos_end = if consensus_pos_idx.is_empty() {
                DATA_HEADER_BYTES as u64
            } else {
                consensus_pos_idx
                    .load(consensus_pos_idx.len() as u64 - 1)
                    .map(|p| p.output_end)
                    .unwrap_or(DATA_HEADER_BYTES as u64)
            };
            digest_end.max(pos_end)
        };
        // The data log is authoritative; discard the (stale/damaged) indexes and start fresh. The
        // digest indexes are directories (index.hdx + index.odx), so remove the whole directory.
        consensus_pos_idx.truncate_all()?;
        drop(consensus_digests);
        drop(batch_digests);
        std::fs::remove_dir_all(base_dir.join(Self::CONSENSUS_HASH_NAME))?;
        std::fs::remove_dir_all(base_dir.join(Self::BATCH_HASH_NAME))?;
        let (mut consensus_digests, mut batch_digests) =
            Self::open_digest_indexes(base_dir, data.header(), false)?;

        let mut iter = data.raw_iter().map_err(DataFileOpen)?;
        // 0-based local index of the output within this pack (mirrors `save_consensus_output`).
        let mut idx: u64 = 0;
        // Byte offset just past the last fully-recovered record (the EpochMeta or a complete
        // output). Anything after it is an incomplete/torn tail and is truncated away at the end.
        let mut consistent_end = iter.position()?;

        loop {
            let header_pos = iter.position()?;
            match iter.next() {
                // Clean EOF on an output boundary: every complete output has been replayed.
                None => break,
                // The leading EpochMeta carries no index data (epoch_meta is already loaded and the
                // pos index is 0-based); skip it, but keep it in the consistent prefix.
                Some(Ok(PackRecord::EpochMeta(_))) => {
                    consistent_end = iter.position()?;
                    continue;
                }
                Some(Ok(PackRecord::Consensus(consensus_header))) => {
                    consensus_digests
                        .save(consensus_header.digest().into(), header_pos)
                        .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
                    // The header's sub-dag names exactly the batch records this output owns.
                    let expected = Self::expected_batch_count(&consensus_header);
                    let mut torn = false;
                    for _ in 0..expected {
                        let batch_pos = iter.position()?;
                        match iter.next() {
                            Some(Ok(PackRecord::Batch(batch))) => {
                                batch_digests
                                    .save(batch.digest(), batch_pos)
                                    .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
                            }
                            // A decodable non-batch where a batch is required is structurally
                            // impossible in append order, so it is genuine corruption, not an
                            // unacked tail -- fail regardless of where it sits.
                            Some(Ok(_)) => return Err(Self::corrupt_pack(base_dir)),
                            // Torn/short/short-EOF inside the output: it is incomplete, drop it.
                            Some(Err(_)) | None => {
                                torn = true;
                                break;
                            }
                        }
                    }
                    if torn {
                        // Incomplete output ends the consistent prefix. Dropping it is safe unless
                        // the tear sits within acked data *and* readable records survive past it
                        // (mid-log corruption, not the final record). Past the attested watermark
                        // it is an unacked out-of-order-writeback tail, so
                        // skip the scan entirely.
                        if consistent_end < attested_end && !Self::tail_is_torn(&mut iter) {
                            return Err(Self::corrupt_pack(base_dir));
                        }
                        break; // consistent_end still marks the end of the last complete output
                    }
                    let output_end = iter.position()?;
                    consensus_pos_idx
                        .save(idx, IndexPositions::new(header_pos, header_pos, output_end))
                        .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
                    idx += 1;
                    consistent_end = output_end;
                }
                // A torn record where the next output's header would start. The last complete
                // output is already finalized; this ends the consistent prefix. It
                // is only fatal when the tear sits within acked data *and* readable
                // records survive past it (mid-log corruption); past the attested
                // watermark it is an unacked tail, safe to drop.
                Some(Err(_)) => {
                    if consistent_end < attested_end && !Self::tail_is_torn(&mut iter) {
                        return Err(Self::corrupt_pack(base_dir));
                    }
                    break;
                }
                // v1 is header-first, so a decodable batch (or a stray second epoch meta) where a
                // header is expected is append-order-impossible -- genuine corruption, not a tail.
                Some(Ok(_)) => return Err(Self::corrupt_pack(base_dir)),
            }
        }

        drop(iter);
        // Drop any incomplete/torn tail so the log ends exactly at the last complete output.
        if consistent_end < data.file_len() {
            data.truncate(consistent_end)?;
        }
        // Reconcile the digest indexes' tracked data length with the (possibly truncated) log so
        // `files_consistent` holds on the next open even if no save follows this recovery.
        let len = data.file_len();
        consensus_digests.set_data_file_length(len);
        batch_digests.set_data_file_length(len);
        Ok((consensus_pos_idx, consensus_digests, batch_digests))
    }

    /// Number of batch records the output for `header` owns — the dedup of its sub-dag's payload
    /// digests, matching what `save_consensus_batches` writes via `collect_batches`. Zero when the
    /// output references no batches.
    fn expected_batch_count(header: &ConsensusHeader) -> usize {
        let mut digests = BTreeSet::new();
        for cert_header in header.sub_dag.headers() {
            for (digest, _) in cert_header.payload().iter() {
                digests.insert(*digest);
            }
        }
        digests.len()
    }

    /// A [`PackError::CorruptPack`] carrying the pack location and operator remediation, for when
    /// recovery finds durably-committed data damaged (a tear below the acked watermark, or a
    /// structurally impossible record). Unlike an unacked torn tail this cannot be healed by
    /// truncation, so the message points the operator at `db validate` and warns off the chain-data
    /// directories.
    fn corrupt_pack(base_dir: &Path) -> PackError {
        PackError::CorruptPack(format!(
            "epoch pack {} is corrupt: durably-committed consensus data is damaged and cannot be \
             repaired by truncating the log. Inspect it with `telcoin-network db validate {}`. Do \
             NOT delete the chain-data directories (`db`, `static_files`, `consensus-db`)",
            base_dir.display(),
            base_dir.display(),
        ))
    }

    /// After recovery hits a damaged record *within acked data*, decide whether the rest of the log
    /// is a clean torn/zero-padded tail (safe to truncate) or mid-log corruption (an error). A torn
    /// tail yields only unreadable garbage until EOF; if any later record still decodes then valid
    /// data survived past the damage, so the damaged record was not the final one. Only consulted
    /// when the tear is at/below the attested watermark -- past it, an unacked
    /// out-of-order-writeback tail is expected to hold decodable records and is truncated
    /// without this scan.
    fn tail_is_torn(
        iter: &mut crate::archive::pack_iter::PackIter<PackRecord, std::fs::File>,
    ) -> bool {
        loop {
            match iter.next() {
                None => return true,
                Some(Ok(_)) => return false,
                Some(Err(_)) => continue,
            }
        }
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

    /// Open (creating if empty) both of an epoch's digest indexes, returning
    /// `(consensus_digests, batch_digests)`. The digest index is always the cache-free,
    /// memory-mapped [`HdxIndex`].
    fn open_digest_indexes(
        base_dir: &Path,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<(HdxIndex, HdxIndex), PackError> {
        let consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data_header,
            BuildHasherDefault::<FxHasher>::default(),
            read_only,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let batch_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::BATCH_HASH_NAME),
            data_header,
            BuildHasherDefault::<FxHasher>::default(),
            read_only,
        )
        .map_err(OpenError::IndexFileOpen)?;
        Ok((consensus_digests, batch_digests))
    }

    /// True when the data file ends inside its own first record and no consensus output is
    /// indexed, i.e. the pack carries a torn [`EpochMeta`] with nothing reachable behind it.
    ///
    /// Two independent witnesses, because neither is sufficient alone:
    ///
    /// * [`Pack::record_size`] validates the size prefix and the record crc without decompressing,
    ///   so `UnexpectedEof` there means the first record is not fully on disk. That is distinct
    ///   from a complete record failing its crc or its decode, and because the data file is a
    ///   sequential append-only log, a file that stops inside record zero cannot hold a record
    ///   after it.
    /// * The size prefix is covered by the record crc, but that crc can only be checked once the
    ///   whole claimed extent is read; a flipped bit that inflates the extent past EOF yields
    ///   `UnexpectedEof` *before* the crc is reached, so it is indistinguishable from a real tear
    ///   on a pack that holds data.  The position index settles the destructive path: every
    ///   consensus read resolves through it, so an empty one means no output was ever committed
    ///   here. Residual gap (tracked separately): when the position index is *non-empty*, a
    ///   past-EOF-inflated meta prefix still mimics a tear and fails the open with `EpochLoad`,
    ///   bricking the epoch at startup on a single fault.  A proper fix is an independent checksum
    ///   on the size prefix -- an on-disk format change deferred to its own PR.
    fn first_record_is_dataless_tear(
        data: &mut Pack<PackRecord>,
        consensus_pos_idx: &PositionIndex<IndexPositions>,
    ) -> bool {
        let ends_inside_first_record = matches!(
            data.record_size(DATA_HEADER_BYTES as u64),
            Err(FetchError::IO(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof
        );
        ends_inside_first_record && consensus_pos_idx.is_empty()
    }

    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    ///
    /// A data file holding record bytes must begin with a readable [`EpochMeta`] matching this
    /// epoch.  An unreadable first record fails the open rather than appending a second meta,
    /// except for the one window that is provably dataless -- a tear inside the meta itself with
    /// nothing indexed behind it -- which truncates back to the header and rewrites the meta.
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

        // Opened ahead of the meta check: the torn-record heal below needs the position index as
        // an independent witness of whether any consensus output was ever committed here.
        let consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), false)?;

        // Set on every branch that writes a fresh meta at the data header.  Either way the pack
        // is byte-for-byte a freshly created one, so it needs the same index initialization.
        let mut wrote_fresh_meta = false;

        // Discriminate a missing meta by length, not by fetch error kind: fetch reports a read
        // at EOF as an io error, the same class as a torn or corrupt record, so record bytes
        // past the header mean the first record must load as a matching meta.
        let pack_len = data.file_len();
        if pack_len > DATA_HEADER_BYTES as u64 {
            match data.fetch(DATA_HEADER_BYTES as u64) {
                Ok(record) => {
                    let meta = record.into_epoch()?;
                    if epoch_meta != meta {
                        return Err(PackError::InvalidEpoch(
                            epoch,
                            format!("open append has unexpected meta data, expected {epoch_meta:?} got {meta:?}"),
                        ));
                    }
                }
                Err(e) => {
                    // A complete-but-corrupt first record keeps failing the open: recovery
                    // (`recover_pack`) only trims the torn tail, and records behind the meta stay
                    // addressable through the indexes, so discarding them here would be silent data
                    // loss.  Only the provably dataless tear self-heals, matching the header-only
                    // branch below.
                    if !Self::first_record_is_dataless_tear(&mut data, &consensus_pos_idx) {
                        return Err(PackError::EpochLoad(format!(
                            "epoch {epoch} pack {} ({pack_len} bytes): first record (the epoch \
                             meta) is unreadable: {e}. The pack cannot be opened by any path. \
                             Inspect it with `telcoin-network db validate {}`; if it holds no \
                             output worth recovering, stop the node and remove that one \
                             `epoch-{epoch}` directory so the epoch is rebuilt on restart. Do NOT \
                             delete the chain-data directories (`db`, `static_files`, \
                             `consensus-db`)",
                            pack_file.display(),
                            base_dir.display(),
                        )));
                    }
                    // The old meta is unreadable, so it cannot be compared against: the
                    // rewritten one is whatever this caller supplied.  Log the committee it is
                    // being rebuilt from so a drifted epoch-start snapshot is traceable.
                    tracing::warn!(
                        target: "consensus_pack",
                        epoch,
                        path = ?pack_file,
                        len = pack_len,
                        committee_size = epoch_meta.committee.size(),
                        start_consensus_number,
                        "first pack record (the epoch meta) is torn and nothing is indexed \
                         behind it; truncating to the data header and rewriting the meta from \
                         the caller's previous_epoch + committee"
                    );
                    data.truncate(DATA_HEADER_BYTES as u64)
                        .map_err(|e| PackError::IO(Arc::new(e)))?;
                    data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                        .map_err(|e| PackError::Append(e.to_string()))?;
                    wrote_fresh_meta = true;
                }
            }
        } else {
            // Header-only file: brand new, or a crash landed between the header write and the
            // meta append.  Either way appending the meta initializes the pack.
            data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                .map_err(|e| PackError::Append(e.to_string()))?;
            wrote_fresh_meta = true;
        }
        let (mut consensus_digests, mut batch_digests) =
            Self::open_digest_indexes(&base_dir, data.header(), false)?;
        if !have_pack || wrote_fresh_meta {
            // If this is a new DB then update the file lengths in indexes after create.  A pack
            // whose meta was just rewritten at the data header counts as new: the indexes still
            // carry the length from before the damage, and leaving that stale would make
            // recovery truncate the meta we just wrote back down to it whenever the rewritten
            // meta is the longer one -- returning a live pack with a torn first record.
            let len = data.file_len();
            consensus_digests.set_data_file_length(len);
            batch_digests.set_data_file_length(len);
        }
        // Rebuild the indexes from the data-log WAL and truncate any torn tail record.
        let (consensus_pos_idx, consensus_digests, batch_digests) = Self::recover_pack(
            &mut data,
            &base_dir,
            consensus_pos_idx,
            consensus_digests,
            batch_digests,
        )?;
        Ok(Self { data, consensus_digests, consensus_pos_idx, batch_digests, epoch_meta })
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    fn open_append_exists<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let pack_file = base_dir.join(Self::DATA_NAME);

        let mut data = Pack::<PackRecord>::open(
            &pack_file,
            epoch as u64,
            false,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        // This door does not create the epoch directory, so the hint must not suggest removing it
        // (that would leave the node unable to start); it points at `db validate` only.
        let epoch_meta = data
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| {
                PackError::EpochLoad(format!(
                    "epoch {epoch} pack {}: first record (the epoch meta) is unreadable: {e}. \
                     Inspect it with `telcoin-network db validate {}`. Do NOT delete the \
                     chain-data directories (`db`, `static_files`, `consensus-db`)",
                    pack_file.display(),
                    base_dir.display(),
                ))
            })?
            .into_epoch()?;
        let consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), false)?;
        let (consensus_digests, batch_digests) =
            Self::open_digest_indexes(&base_dir, data.header(), false)?;

        // Rebuild the indexes from the data-log WAL and truncate any torn tail record.
        let (consensus_pos_idx, consensus_digests, batch_digests) = Self::recover_pack(
            &mut data,
            &base_dir,
            consensus_pos_idx,
            consensus_digests,
            batch_digests,
        )?;
        Ok(Self { data, consensus_digests, consensus_pos_idx, batch_digests, epoch_meta })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    fn open_static<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let pack_file = base_dir.join(Self::DATA_NAME);

        let mut data = Pack::<PackRecord>::open(
            &pack_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let epoch_meta = data
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| {
                PackError::EpochLoad(format!(
                    "epoch {epoch} pack {}: first record (the epoch meta) is unreadable: {e}. \
                     Inspect it with `telcoin-network db validate {}`. Do NOT delete the \
                     chain-data directories (`db`, `static_files`, `consensus-db`)",
                    pack_file.display(),
                    base_dir.display(),
                ))
            })?
            .into_epoch()?;
        let mut consensus_pos_idx = Self::open_pdx_file(&base_dir, data.header(), true)?;
        let (consensus_digests, batch_digests) =
            Self::open_digest_indexes(&base_dir, data.header(), true)?;

        if !Self::files_consistent(
            &data,
            &mut consensus_pos_idx,
            &consensus_digests,
            &batch_digests,
        ) {
            // Corrupt static file is bad (damaged at rest?), produce an error. Read-only opens do
            // not heal, so this is terminal — surface the same remediation as the recovery path.
            return Err(Self::corrupt_pack(&base_dir));
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
        let (consensus_digests, batch_digests) =
            Self::open_digest_indexes(&base_dir, data.header(), false)?;
        let mut parent_digest_expectation = if epoch == 0 {
            // Don't worry about consensus block 1 in epoch 0, if it is invalid other verifications
            // will fail (for instance epoch 0 final state will not verify). This can be set but
            // doing so forces fork aware code here and verification will fail with an invalid value
            // either way.
            HeaderExpectation::None
        } else {
            HeaderExpectation::Parent(previous_epoch.final_consensus.hash)
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
                    parent_digest_expectation,
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
                    parent_digest_expectation,
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
            parent_digest_expectation = HeaderExpectation::Parent(output.digest());
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
    ///
    /// Returns `None` both for a digest that was never written here (a legitimate, quiet miss)
    /// and for a record the index claims exists but that could not be read; the latter is logged
    /// at `error!` first so a stored-but-unreadable header can never be silently mistaken for an
    /// absent one. The `Option` return shape is kept to bound the blast radius of this
    /// hardening. Under the epoch-gated seed-signature serde, pre-fork packs stay decodable and
    /// the logged arms should never fire - they are the difference between a loud signal and
    /// silent chain corruption for every future format change.
    fn consensus_header_by_digest(
        &mut self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader> {
        let epoch = self.epoch_meta.epoch;
        let pos = self
            .consensus_digests
            .load(digest.into())
            .inspect_err(|e| {
                if !fetch_error_is_absent(e) {
                    error!(target: "consensus_pack", epoch, ?digest, "consensus digest index lookup failed (not a miss): {e}");
                }
            })
            .ok()?;
        // This is not strickly needed, the fetch below will fail if
        // we try to read past the end of the file but this potentially
        // short circuits a lot of checks for a small cost.
        // Note, this could happen if a file is damaged and repaired.
        if pos >= self.data.file_len() {
            return None;
        }
        let header = self
            .data
            .fetch(pos)
            .inspect_err(|e| {
                error!(target: "consensus_pack", epoch, ?digest, pos, "indexed consensus record exists but failed to load from the pack: {e}");
            })
            .ok()?
            .into_consensus()
            .inspect_err(|e| {
                error!(target: "consensus_pack", epoch, ?digest, pos, "indexed consensus record exists but did not decode as a consensus header: {e}");
            })
            .ok()?;
        // Verify the digest.  There is an extremely unlikely edge case where
        // a repaired DB could write a new header to the same location as an
        // old header.  This makes sure the contract is always intact.
        if header.digest() != digest {
            error!(target: "consensus_pack", epoch, ?digest, number = header.number, pos, "consensus header loaded from the pack does not hash to its indexed digest");
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
            // Note, we don't sync indexes.  The data file acts as a WAL we can use to clean up and
            // rebuild if we crash and it causes corruption.
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

    /// Prepare the on-disk data file for an external reader: flush pending writes (the buffered
    /// backend's write buffer / the mmap dirty pages), then reconcile the physical length down to
    /// the logical `end`, dropping any mmap capacity padding — so a separate file handle reading to
    /// EOF sees exactly `[0, end)`. No-op for a read-only pack, whose file was already reconciled
    /// when its writer closed.
    fn reconcile_data_len(&mut self) -> Result<(), PackError> {
        if !self.data.read_only() {
            self.data.flush().map_err(|e| PackError::PersistError(e.to_string()))?;
            self.data.reconcile_len().map_err(|e| PackError::PersistError(e.to_string()))?;
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
    ///
    /// A read failure is propagated rather than reported as "no header". Recovery uses this
    /// header's sub-dag as the epoch seed chain anchor, and an absent anchor means "start a fresh
    /// chain", so collapsing an error into `None` would silently re-root the chain and fork
    /// execution permanently.
    fn latest_consensus_header(&mut self) -> Result<Option<ConsensusHeader>, PackError> {
        if self.consensus_pos_idx.is_empty() {
            return Ok(None);
        }
        let latest_number =
            self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64 - 1;
        self.consensus_header_by_number(latest_number).map(Some)
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

/// True when a [`FetchError`] from a digest-index lookup means the key is simply not present (a
/// legitimate miss), as opposed to a record that exists but could not be read.
///
/// The pack read paths use this to stay quiet on routine misses while logging every other
/// failure at `error!`: collapsing "exists but unreadable" into a silent `None` is exactly the
/// failure mode that let a startup resume fall back to a default consensus header at number 0
/// with no signal. `pub(crate)` so the epoch-record store
/// ([`crate::epoch_records`]) classifies absence with exactly the same rule instead of growing
/// a second, divergent classification.
pub(crate) fn fetch_error_is_absent(err: &FetchError) -> bool {
    match err {
        FetchError::NotFound => true,
        FetchError::DeserializeValue(_)
        | FetchError::IO(_)
        | FetchError::CrcFailed
        | FetchError::CorruptIndex(_)
        | FetchError::RequestedSizeTooLarge(_, _)
        | FetchError::RequestedDecompressSizeTooLarge(_) => false,
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
///
/// Beyond the linkage this also pins the record's two internal identities to each other: the
/// embedded [`Committee`]'s own epoch must equal the record's `epoch`. The committee's epoch is
/// what SELECTED its bcs layout on the way in —
/// [`multi_workers_fork_active`](tn_types::forks::multi_workers_fork_active) reads the epoch
/// carried inside the value being decoded, not the record's — so a record whose committee claims a
/// different epoch was parsed under a layout the record's own epoch does not select, and no later
/// reader can tell. Both halves are then used as if they agreed: `epoch_meta.epoch` decides which
/// outputs [`Inner::save_consensus_output`] accepts, while `epoch_meta.committee` is the validator
/// set every output in the pack is decoded and verified against, and
/// [`ConsensusPack::stream_import`] reports `epoch()` from the request while `committee()` comes
/// from this record. A local write cannot break the equality — [`Inner::open_append`] derives the
/// record's epoch FROM the committee — so a divergence only ever arrives over the wire (peer epoch
/// sync) or from an imported bundle, which is precisely what this function screens.
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
    if epoch_meta.committee.epoch() != epoch_meta.epoch {
        return Err(PackError::InvalidEpoch(
            epoch,
            format!(
                "meta data epoch is {} but its committee is for epoch {}",
                epoch_meta.epoch,
                epoch_meta.committee.epoch()
            ),
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

/// Decode raw pack-file `bytes` for one consensus output into a [`ConsensusOutput`], dispatching on
/// the pack data `version` (v0 legacy vs v1 header-first) and using the pack's `compression` and
/// `committee`.
pub(crate) async fn decode_output_bytes(
    bytes: Vec<u8>,
    version: u16,
    compression: PackCompression,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    let cursor = Cursor::new(bytes);
    let reader = BufReader::new(cursor);
    match version {
        0 => bytes_to_output_legacy(reader, compression, Duration::from_secs(5), committee).await,
        1 => bytes_to_output(reader, compression, Duration::from_secs(5), committee).await,
        _ => Err(PackError::InvalidVersion(PACK_VERSION, version)),
    }
}

/// Produce the v1 (header-first) pack-record bytes served to peers for one consensus output from
/// the raw `bytes` read out of the data file: a passthrough for v1, or a re-encode of a v0 legacy
/// output into v1 records (header first, then batches). Uses the pack's
/// `version`/`compression`/`committee`. Shared by both pack front-ends.
pub(crate) async fn serve_output_bytes(
    mut bytes: Vec<u8>,
    version: u16,
    compression: PackCompression,
    committee: &Committee,
) -> Result<Vec<u8>, PackError> {
    match version {
        0 => {
            let cursor = Cursor::new(bytes.clone());
            let reader = BufReader::new(cursor);
            let out =
                bytes_to_output_legacy(reader, compression, Duration::from_secs(5), committee)
                    .await?;
            let batches = collect_batches(&out);
            let header: ConsensusHeader = out.into();
            bytes.clear();
            let mut value_buffer = Vec::new();
            let mut compress_buffer = Vec::new();
            // Re-encode as PackRecord-wrapped records (header first) to match the on-disk v1
            // format the consumer decodes; the raw v1 serve path returns these same PackRecord
            // records straight from the pack file.
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
        _ => Err(PackError::InvalidVersion(PACK_VERSION, version)),
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
            // Internal invariant: `encode` is only ever handed our own fixed-size scratch buffer
            // (never on-disk bytes), so a wrong length is a caller coding error, not data
            // corruption -- panic. (`decode`, which IS fed on-disk bytes, returns an error
            // instead.)
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
            // A wrong-length slice means the on-disk PDX entry is truncated/malformed. Surface it
            // as an error rather than panic so a corrupt position index can never take
            // down the pack's background thread (the append-only-log integrity rule:
            // corruption is reported, not crashed on).
            return Err(FetchError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "position index entry is not 28 bytes",
            )));
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
    /// The pack holds damaged durably-committed data that recovery cannot repair by truncation.
    /// Carries an operator-facing message with the pack path and remediation guidance.
    CorruptPack(String),
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

impl PackError {
    /// True when a static-pack open failed because the epoch's files are absent on disk: the
    /// data-file or an index-file open bottomed out in io `NotFound`. [`Inner::open_static`]
    /// opens the data file before anything else, so a missing `epoch-{N}` directory (or a
    /// never-created epoch) always surfaces as the data file's `NotFound`; an index file can
    /// bottom out there on its own while the data file still opens, because `stream_import`
    /// removes an incomplete epoch directory entry by entry before re-importing it, and the
    /// pre-classifier lookup fell back to staging during that window.
    ///
    /// Callers use this to distinguish an epoch whose files are not (or are no longer) on disk
    /// (a normal miss, answered with `None`) from files that are present but unreadable
    /// (corrupt pack, damaged header or index, non-`NotFound` I/O failure): a storage READ
    /// error that must propagate instead of being collapsed into a miss.
    pub fn is_missing_static_files(&self) -> bool {
        matches!(
            self,
            PackError::Open(open_error)
                if matches!(
                    open_error.as_ref(),
                    OpenError::DataFileOpen(LoadHeaderError::IO(io_error))
                    | OpenError::IndexFileOpen(LoadHeaderError::IO(io_error))
                        if io_error.kind() == io::ErrorKind::NotFound
                )
        )
    }
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
            PackError::CorruptPack(msg) => write!(f, "{msg}"),
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
        num::NonZeroUsize,
        sync::Arc,
        time::Duration,
    };

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        test_genesis, Batch, BlockHash, Certificate, CertifiedBatch, CommittedSubDag, Committee,
        ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch,
        EpochRecord, ExecHeader, Hash, HeaderBuilder, ReputationScores,
    };

    use crate::{
        archive::pack::{Pack, PackCompression, DATA_HEADER_BYTES},
        consensus_pack::{max_batches_per_output, ConsensusPack, Inner, PackRecord, PACK_VERSION},
        mem_db::MemDatabase,
    };

    /// Build a [`ConsensusOutput`] whose single leader header references `num_batches` unique
    /// batches, standing in for a deep sub-DAG that exceeds the old fixed reconstruction cap
    /// but stays within the committee-derived bound.
    ///
    /// Reused by `pack_bench` as the single output-width knob for the observation benchmark.
    pub(crate) fn make_wide_test_output(
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
            tn_types::EpochSeedChainValue::genesis_placeholder(),
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
            tn_types::EpochSeedChainValue::genesis_placeholder(),
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
            tn_types::EpochSeedChainValue::genesis_placeholder(),
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

    /// Epoch for the shared-batch scenarios: one above the adiri dup-batch replay cutoff
    /// (`ADIRI_DUP_BATCH_EPOCH`, 160), so the rebuilt output keeps the shared batch under every
    /// feature set and `compare_outputs` stays cfg-free (#1128). The literal is hard-coded
    /// because the constant only exists under the `adiri` feature; the assertion below pins the
    /// relation where the constant is visible. The replay (drop) side at low epochs is pinned by
    /// `test_shared_batch_replay_below_adiri_dup_cutoff`.
    const SHARED_BATCH_EPOCH: Epoch = 161;

    #[cfg(feature = "adiri")]
    const _: () = assert!(SHARED_BATCH_EPOCH > tn_types::forks::ADIRI_DUP_BATCH_EPOCH);

    /// Previous-epoch record linking a pack opened at [`SHARED_BATCH_EPOCH`]: final consensus
    /// number 0 keeps `start_consensus_number` at 1 and the final consensus hash keeps the
    /// first output's parent at the default header digest, so the scenario keeps the shape the
    /// epoch-0 tests use.
    fn shared_batch_previous_epoch(committee: &Committee) -> EpochRecord {
        EpochRecord {
            // 160 here is only `SHARED_BATCH_EPOCH - 1`, not the adiri cutoff; the
            // open, verify, and stream-import paths do not read this field.
            epoch: SHARED_BATCH_EPOCH - 1,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            final_consensus: ConsensusNumHash::new(0, ConsensusHeader::default().digest()),
            ..Default::default()
        }
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

    /// Exercise the full `ConsensusPack` lifecycle (append, read-back, persist, reopen-static) on
    /// the memory-mapped file backend, so the mmap wiring for the data file + position index is
    /// covered by the default suite (the side-by-side timing lives in the `#[ignore]`d
    /// `pack_file_bench`).
    #[tokio::test]
    async fn test_consensus_pack_mmap_backend() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack_mmap").expect("temp dir");
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
            .expect("open mmap pack");

        let num_outputs = 50u64;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for number in 1..=num_outputs {
            let output =
                make_test_output(&committee, (number as usize) % 4, chain.clone(), number, parent);
            parent = output.consensus_header_hash();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.expect("save");
        }
        for (i, output) in outputs.iter().enumerate() {
            let db = pack.get_consensus_output(i as u64 + 1).await.expect("read back");
            compare_outputs(&db, output);
        }
        // Exercise the mmap digest index (hdx + odx overflow): every header and batch digest must
        // resolve through the hash index.
        for output in &outputs {
            assert!(
                pack.contains_consensus_header(output.consensus_header_hash()).await,
                "header digest must be found in the mmap hdx",
            );
            for batch_digest in output.batch_digests() {
                assert!(
                    pack.contains_batch(*batch_digest).await,
                    "batch digest must be found in the mmap hdx",
                );
            }
        }
        pack.persist().await.expect("persist");
        drop(pack);

        // Reopen the finished pack read-only on the mmap backend and re-verify every output and a
        // digest lookup (the reopened mmap hdx/odx).
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static mmap");
        for (i, output) in outputs.iter().enumerate() {
            let db = pack.get_consensus_output(i as u64 + 1).await.expect("read back static");
            compare_outputs(&db, output);
        }
        for output in &outputs {
            assert!(
                pack.contains_consensus_header(output.consensus_header_hash()).await,
                "header digest must be found after mmap reopen",
            );
        }
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
    /// PackError::MissingBatch).  Runs at [`SHARED_BATCH_EPOCH`], above the adiri dup-batch
    /// replay cutoff, so the expectation holds for every feature set (#1128).
    #[tokio::test]
    async fn test_consensus_pack_dup_batch_across_certs() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack_dup").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // Run above the adiri dup-batch replay cutoff so the duplicate survives the rebuild
        // under every feature set and one unconditional comparison serves both builds.
        let committee = fixture.committee().advance_epoch_for_test(SHARED_BATCH_EPOCH);
        let previous_epoch = shared_batch_previous_epoch(&committee);
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
        let pack =
            ConsensusPack::open_static(temp_dir.path(), SHARED_BATCH_EPOCH).expect("open static");
        compare_outputs(&pack.get_consensus_output(1).await.expect("dup batch output"), &output_1);
        compare_outputs(&pack.get_consensus_output(2).await.expect("output after dup"), &output_2);
        drop(pack);

        // Stream into a new pack (peer epoch sync path) and read back.
        let temp_dir2 = TempDir::with_prefix("test_consensus_pack_dup2").expect("temp dir");
        let stream = tokio::fs::File::open(
            temp_dir.path().join(format!("epoch-{SHARED_BATCH_EPOCH}")).join(Inner::DATA_NAME),
        )
        .await
        .expect("log file");
        let pack = ConsensusPack::stream_import(
            temp_dir2.path(),
            stream,
            SHARED_BATCH_EPOCH,
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
    /// pre-upgrade v0 file served as v1 for a duplicate-batch output. Runs at
    /// [`SHARED_BATCH_EPOCH`], above the adiri dup-batch replay cutoff, so the expectation
    /// holds for every feature set (#1128).
    #[tokio::test]
    async fn test_v0_shared_batch_served_as_v1_bytes() {
        use crate::consensus_pack::{bytes_to_output, bytes_to_verified_output};
        use std::io::Cursor;
        use tokio::io::BufReader;

        let temp_dir = TempDir::with_prefix("test_v0_shared_v1").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        // Above the adiri replay cutoff: the shared batch must reach both certs on rebuild
        // under every feature set.
        let committee = fixture.committee().advance_epoch_for_test(SHARED_BATCH_EPOCH);
        let previous_epoch = shared_batch_previous_epoch(&committee);

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

    /// Adiri replay pin: at epochs at or below `ADIRI_DUP_BATCH_EPOCH` the rebuild must DROP a
    /// shared batch from the second certificate, reproducing the historical duplicate-batch
    /// outputs so adiri testnet can sync (the gates in `iter_to_output` and
    /// `iter_to_output_legacy`). Exercises the skip side of both decoders from one v0 pack: the
    /// legacy (batches-first) local read and the v1 (header-first) decode of the served bytes.
    /// The push side above the cutoff is exercised by the two shared-batch tests at
    /// [`SHARED_BATCH_EPOCH`] (#1128).
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_shared_batch_replay_below_adiri_dup_cutoff() {
        use crate::consensus_pack::bytes_to_output;
        use std::io::Cursor;
        use tokio::io::BufReader;

        let temp_dir = TempDir::with_prefix("test_dup_replay").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        // The fixture committee is at epoch 0, at or below the replay cutoff.
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // v0 pack so the local read exercises the legacy (batches-first) decoder.
        let pack = ConsensusPack::open_append_version(
            temp_dir.path(),
            previous_epoch,
            committee.clone(),
            0,
        )
        .expect("open v0 pack");
        let original = make_test_output_shared_batch(
            &committee,
            chain.clone(),
            1,
            ConsensusHeader::default().digest(),
        );
        // The replay gate reads the leader epoch of the sub-dag, so the guard pins
        // that quantity, not the committee epoch it was stamped from.
        assert!(
            original.sub_dag().leader_epoch() <= tn_types::forks::ADIRI_DUP_BATCH_EPOCH,
            "scenario must run at or below the replay cutoff"
        );
        pack.save_consensus_output(original.clone()).await.expect("save shared-batch output");
        pack.persist().await.expect("persist");

        // The digest both certificates reference: the one listed twice in batch_digests.
        let shared_digest = original
            .batch_digests()
            .iter()
            .find(|digest| {
                original.batch_digests().iter().filter(|other| other == digest).count() == 2
            })
            .copied()
            .expect("scenario shares one digest across certs");

        let assert_replay_shape = |rebuilt: &ConsensusOutput| {
            // The sub-dag, parent link and declared digest list (duplicate included) survive
            // untouched; only the second certificate's materialized batches change.
            assert_eq!(rebuilt.digest(), original.digest(), "consensus digest must be preserved");
            assert_eq!(
                rebuilt.batch_digests(),
                original.batch_digests(),
                "declared digests keep the duplicate"
            );
            let cert_a = rebuilt.batches().first().expect("two certified batches");
            let cert_a_original = original.batches().first().expect("two certified batches");
            assert_eq!(cert_a.address, cert_a_original.address);
            assert_eq!(cert_a.batches, cert_a_original.batches, "first certificate is untouched");
            let cert_b = rebuilt.batches().get(1).expect("two certified batches");
            let cert_b_original = original.batches().get(1).expect("two certified batches");
            assert_eq!(cert_b.address, cert_b_original.address);
            let expected: Vec<Batch> = cert_b_original
                .batches
                .iter()
                .filter(|batch| batch.digest() != shared_digest)
                .cloned()
                .collect();
            assert_eq!(expected.len(), 1, "one unshared batch must remain");
            assert_eq!(
                cert_b.batches, expected,
                "replay must drop the shared batch from the second certificate"
            );
        };

        // Legacy (batches-first) local read of the v0 pack.
        assert_replay_shape(&pack.get_consensus_output(1).await.expect("local v0 read"));

        // The same output served as v1 (header-first) bytes and decoded by the v1 path.
        let bytes = pack.get_consensus_output_bytes(1).await.expect("bytes");
        let decoded = bytes_to_output(
            BufReader::new(Cursor::new(bytes)),
            PackCompression::ZStd,
            Duration::from_secs(5),
            &committee,
        )
        .await
        .expect("v1 decode");
        assert_replay_shape(&decoded);
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

    /// CP3: a pack recovered on open (rebuilding the indexes from the WAL and truncating a torn
    /// tail record) but not followed by a save must still reconcile the index lengths, so a
    /// later read-only open passes the consistency check.
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
        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after recover");
        // The recovered pack dropped the incomplete 5th output; the first four remain readable.
        assert!(pack.get_consensus_output(1).await.is_ok());
        assert!(pack.get_consensus_output(4).await.is_ok());
        assert!(pack.get_consensus_output(5).await.is_err(), "torn 5th output must be dropped");
    }

    /// Build `n` sequential outputs into a fresh pack at `temp_dir` and persist the data log.
    async fn build_test_pack(
        temp_dir: &TempDir,
        committee: &Committee,
        chain: &Arc<RethChainSpec>,
        previous_epoch: &EpochRecord,
        n: u64,
    ) {
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..n {
            let output =
                make_test_output(committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            pack.save_consensus_output(output).await.expect("save output");
        }
        pack.persist().await.expect("persist");
    }

    /// Recovery rebuilds BOTH indexes purely from the data-log WAL: after the position and digest
    /// indexes are lost (the "don't sync indexes" regime taken to its limit), reopening replays the
    /// log so every output is again reachable by number and by digest, and the pack is consistent.
    #[tokio::test]
    async fn test_recover_rebuilds_indexes_from_wal() {
        let temp_dir = TempDir::with_prefix("test_recover_rebuild").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 5).await;

        // Lose the indexes entirely; only the data log survives.
        let epoch_dir = temp_dir.path().join("epoch-0");
        for name in ["idx", "hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove index dir");
        }

        // Reopen (append) -> files_consistent fails -> recover_pack rebuilds from the log.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append rebuilds");
            pack.persist().await.expect("persist");
        }

        // Consistent again, and every output is reachable by number and by digest.
        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after rebuild");
        for i in 1..=5u64 {
            let out = pack.get_consensus_output(i).await.expect("output by number");
            assert!(
                pack.contains_consensus_header(out.consensus_header_hash()).await,
                "consensus digest for output {i} must be indexed"
            );
            if let Some(bd) =
                out.batches().first().and_then(|c| c.batches.first()).map(|b| b.digest())
            {
                assert!(
                    pack.contains_batch(bd).await,
                    "batch digest for output {i} must be indexed"
                );
            }
        }
    }

    /// A torn *next* output header (a partial record appended after several complete outputs) is
    /// dropped without losing the last complete output — recovery finalizes each output before
    /// reading the next header, so a broken next header only truncates itself.
    #[tokio::test]
    async fn test_recover_torn_next_header_keeps_last_output() {
        use std::io::Write as _;
        let temp_dir = TempDir::with_prefix("test_recover_torn_header").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Append a torn next-output header: a size prefix whose payload was never written, so the
        // next open short-reads it (a torn tail record) rather than seeing a clean EOF.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let mut f = OpenOptions::new().append(true).open(&data_path).expect("open data");
            f.write_all(&1024u32.to_le_bytes()).expect("write torn size prefix");
        }

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append recovers");
            pack.persist().await.expect("persist");
        }

        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after recover");
        for i in 1..=3u64 {
            assert!(pack.get_consensus_output(i).await.is_ok(), "output {i} must be preserved");
        }
        assert!(
            pack.get_consensus_output(4).await.is_err(),
            "no phantom output from the torn header"
        );
    }

    /// Corruption of a non-final record (a bit-flip with valid outputs still after it) is not a
    /// clean torn tail: recovery reports `CorruptPack` rather than silently discarding good data.
    #[tokio::test]
    async fn test_recover_mid_log_corruption_errors() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_recover_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Corrupt a byte inside output 2's header payload (output 2 begins where output 1 ends).
        // Staying past the 4-byte record size prefix leaves the framing intact, so output 2's
        // batches and output 3 still decode AFTER the damage -> provably not the final record.
        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary + 20)).expect("seek");
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).expect("read");
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(boundary + 20)).expect("seek back");
            f.write_all(&byte).expect("write");
        }
        // Force recovery to run by dropping the digest indexes so files_consistent fails.
        for name in ["hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove digest dir");
        }

        let res =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(res, Err(PackError::CorruptPack(_))),
            "mid-log corruption must error, got {res:?}"
        );
    }

    /// The mmap backend ends `persist()` at an `msync` only, and the kernel may write dirty pages
    /// back out of order, so a power loss can leave `[k good][k+1 torn][k+2 good]` in the region
    /// past the last index sync -- an unacked tail, not committed data. Recovery must truncate it
    /// and let the node start, not reject the pack because a record still decodes after the tear.
    /// (Before the fix, `tail_is_torn` saw the decodable `k+2` and returned `CorruptPack`, bricking
    /// startup on the normal mmap power-loss shape.) The mid-log-corruption test above is the
    /// mirror image: it keeps the position index, which attests outputs 2/3 as committed, so the
    /// same byte-level damage is (correctly) fatal there.
    #[tokio::test]
    async fn test_recover_truncates_unacked_torn_tail_with_later_good_record() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        let temp_dir = TempDir::with_prefix("test_recover_unacked_tail").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // End of output 1 is the last consistent point once output 2 is torn.
        let output1_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let full_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(full_len > output1_end, "outputs 2 and 3 must extend past output 1");

        // Corrupt a byte inside output 2's header payload (past the 4-byte size prefix, so the
        // framing stays intact and output 3 still decodes AFTER the damage).
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(output1_end + 20)).expect("seek");
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).expect("read");
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(output1_end + 20)).expect("seek back");
            f.write_all(&byte).expect("write");
        }
        // Reset every index so recovery runs with no attested watermark past output 1 -- the
        // on-disk state a real crash leaves, since indexes sync on a clean close, not on
        // `persist()`. With nothing attesting outputs 2/3, the torn region is an unacked tail.
        for name in ["hash", "bhash", "idx"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove index dir");
        }

        // Recovery must accept the pack (truncate the torn tail) rather than brick startup.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("unacked torn tail must recover, not brick startup");
            pack.persist().await.expect("persist after recovery");
        }

        // The log is truncated back to the end of output 1; outputs 2 and 3 are dropped.
        let recovered_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(
            recovered_len, output1_end,
            "recovery must truncate the torn tail back to the last complete output before the tear"
        );
        let pack = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("recovered pack must open read-only and pass files_consistent");
        assert_eq!(
            pack.get_consensus_output(1).await.expect("output 1 survives").number(),
            1,
            "the last complete output before the tear must read back"
        );
        assert!(
            pack.get_consensus_output(2).await.is_err(),
            "the torn output 2 (and everything after) must be gone"
        );
    }

    /// A pack whose first record (the epoch meta) is corrupt must fail `open_append` with
    /// `EpochLoad` instead of treating the unreadable record as absent and appending a second
    /// meta after it.  The flipped byte leaves a *complete* record on disk, so the tear heal
    /// does not apply: the file continues past the meta and those records stay addressable.
    #[tokio::test]
    async fn test_open_append_rejects_corrupt_first_record() {
        let temp_dir = TempDir::with_prefix("test_cp_corrupt_meta").expect("temp dir");
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
            for i in 0..3 {
                let output =
                    make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
                parent = output.digest();
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }

        // Flip one byte inside the meta record's value; the record crc covers it, so the
        // meta fetch itself fails rather than decoding to different values.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let mut bytes = std::fs::read(&data_path).expect("read data");
        bytes[DATA_HEADER_BYTES + 6] ^= 0xff;
        std::fs::write(&data_path, &bytes).expect("write data");
        let len_before = bytes.len() as u64;

        let result = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone());
        assert!(
            matches!(result, Err(super::PackError::EpochLoad(_))),
            "expected EpochLoad, got {result:?}"
        );
        let len_after = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(len_before, len_after, "failed open must leave the data file untouched");
    }

    /// A physically sound pack classifies as `None` (run the logical validator for the rest).
    #[tokio::test]
    async fn test_classify_physical_corruption_clean_pack() {
        use crate::pack_validate::classify_physical_corruption;
        let temp_dir = TempDir::with_prefix("test_classify_clean").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        assert!(
            classify_physical_corruption(&data_path, 0).expect("classify").is_none(),
            "a clean pack must classify as physically sound"
        );
    }

    /// A torn record with nothing readable after it is a truncatable trailing tail.
    #[tokio::test]
    async fn test_classify_physical_corruption_torn_trailing_tail() {
        use crate::pack_validate::{classify_physical_corruption, CorruptionKind};
        let temp_dir = TempDir::with_prefix("test_classify_torn_tail").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let output2_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(2).await.expect("output 2 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // Truncate a few bytes into output 3's header payload: torn record, nothing after.
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(output2_end + 6).expect("truncate");
        }
        let c = classify_physical_corruption(&data_path, 0).expect("classify").expect("corruption");
        assert_eq!(c.kind, CorruptionKind::TornTrailingTail);
        assert!(c.kind.is_truncatable(), "a torn trailing tail is truncatable");
        assert!(!c.decodable_after);
    }

    /// A CRC-failed interior record with valid records after it is data-losing mid-log corruption.
    #[tokio::test]
    async fn test_classify_physical_corruption_mid_log() {
        use crate::pack_validate::{classify_physical_corruption, CorruptionKind};
        let temp_dir = TempDir::with_prefix("test_classify_mid_log").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let output1_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // Flip a byte inside output 2's header payload (past the size prefix): its CRC fails but
        // output 3 still decodes after it.
        let mut bytes = std::fs::read(&data_path).expect("read data");
        bytes[output1_end as usize + 20] ^= 0xff;
        std::fs::write(&data_path, &bytes).expect("write data");
        let c = classify_physical_corruption(&data_path, 0).expect("classify").expect("corruption");
        assert_eq!(c.kind, CorruptionKind::MidLogCorruption);
        assert!(!c.kind.is_truncatable(), "mid-log corruption is not truncatable");
        assert!(c.decodable_after);
    }

    /// A torn epoch-meta with no outputs behind it is a truncatable, effectively-empty pack.
    #[tokio::test]
    async fn test_classify_physical_corruption_torn_meta_empty() {
        use crate::pack_validate::{classify_physical_corruption, CorruptionKind};
        let temp_dir = TempDir::with_prefix("test_classify_torn_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        // Meta-only pack (no outputs), then tear the meta within its size prefix.
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 0).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(DATA_HEADER_BYTES as u64 + 2).expect("truncate");
        }
        let c = classify_physical_corruption(&data_path, 0).expect("classify").expect("corruption");
        assert_eq!(c.kind, CorruptionKind::TornMetaEmpty);
        assert!(c.kind.is_truncatable(), "an empty torn-meta pack is truncatable");
    }

    /// An unreadable epoch-meta with outputs behind it is data loss (the outputs are unreachable).
    #[tokio::test]
    async fn test_classify_physical_corruption_corrupt_meta_with_data() {
        use crate::pack_validate::{classify_physical_corruption, CorruptionKind};
        let temp_dir = TempDir::with_prefix("test_classify_corrupt_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // Flip a byte inside the (complete) meta record's payload: its CRC fails but the outputs
        // behind it still decode.
        let mut bytes = std::fs::read(&data_path).expect("read data");
        bytes[DATA_HEADER_BYTES + 6] ^= 0xff;
        std::fs::write(&data_path, &bytes).expect("write data");
        let c = classify_physical_corruption(&data_path, 0).expect("classify").expect("corruption");
        assert_eq!(c.kind, CorruptionKind::CorruptMetaWithData);
        assert!(!c.kind.is_truncatable(), "corrupt meta with data behind it is not truncatable");
        assert!(c.decodable_after);
    }

    /// A pack whose first record is torn (a crash mid meta append left only part of the size
    /// prefix) with nothing indexed behind it is the one recoverable window: `open_append`
    /// truncates back to the data header and rewrites the meta, exactly as the header-only
    /// branch does one instant earlier.  Nothing reachable is lost -- the meta is rebuilt from
    /// `previous_epoch` + `committee`.
    #[tokio::test]
    async fn test_open_append_heals_dataless_torn_first_record() {
        let temp_dir = TempDir::with_prefix("test_cp_torn_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Open + persist a clean meta-only pack, then DROP it so `Drop` truncates the mmap capacity
        // padding away and the on-disk file is exactly its logical length — the same basis
        // `healed_len` is measured on below. (A live pack is physically padded to `capacity`; the
        // file is reconciled to `end` only at `try_clone`/`Drop`, so measuring while the pack is
        // alive would read the transient padding, not the logical meta-only length.)
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open pack");
            pack.persist().await.expect("persist");
        }
        let clean_len = std::fs::metadata(&data_path).expect("metadata").len();

        // Tear the meta record mid size-prefix: record bytes exist past the header, but the
        // first record cannot be read and no output was ever committed behind it.
        let torn_len = DATA_HEADER_BYTES as u64 + 2;
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(torn_len).expect("truncate");
        }

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("torn meta with no data behind it must heal");
            // The healed pack is writable: the rewritten meta put it back in service.
            let parent = ConsensusHeader::default().digest();
            let output = make_test_output(&committee, 0, chain.clone(), 1, parent);
            pack.save_consensus_output(output).await.unwrap();
            pack.persist().await.expect("persist after heal");
        }

        // The torn bytes were dropped rather than appended after, so the meta sits at the data
        // header again and the pack grew past its clean meta-only length by the saved output.
        let healed_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            healed_len > clean_len,
            "healed pack ({healed_len}) must hold the meta plus the new output (clean meta-only \
             was {clean_len})"
        );

        // Both read-only doors agree the recovered pack is sound.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after heal");
        let output = pack.get_consensus_output(1).await.expect("read output through static open");
        assert_eq!(output.number(), 1, "static read must return the output saved after the heal");
    }

    /// A live (persisted-but-not-dropped) pack's `data` file is physically padded to its mmap
    /// capacity. The state export copies `consensus_data` with a raw `std::fs::copy`, so without
    /// reconciling first the bundle would carry that trailing zero padding and the importer's
    /// record walk would fail its CRC on the zeros (the e2e "crc32 mismatch").
    /// `reconcile_data_len` — called via `ConsensusChain::reconcile_current` right before the
    /// export copy — truncates the file to its logical length so a raw copy round-trips through
    /// `stream_import`.
    #[tokio::test]
    async fn test_reconcile_before_copy_lets_raw_copy_stream_import() {
        let temp_dir = TempDir::with_prefix("test_cp_reconcile_copy").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // Append a few outputs and persist WITHOUT dropping: the pack stays live, so its data file
        // keeps the mmap capacity padding (persist msyncs but does not truncate).
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        let num_outputs = 3usize;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), i as u64 + 1, parent);
            parent = output.digest().into();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.unwrap();
        }
        pack.persist().await.expect("persist");
        let padded_len = std::fs::metadata(&data_path).expect("metadata").len();

        // Reconcile (what `ConsensusChain::reconcile_current` does before the export copy):
        // truncate the padding away so the on-disk file is exactly its logical length.
        pack.reconcile_data_len().await.expect("reconcile");
        let reconciled_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            reconciled_len < padded_len,
            "reconcile must shrink the padded file ({padded_len}) to its logical length \
             ({reconciled_len})"
        );

        // A raw copy of the reconciled file — exactly what the export does for `consensus_data` —
        // must stream_import cleanly, with no trailing padding for the record walk to choke on.
        let bundle_dir = TempDir::with_prefix("test_cp_reconcile_bundle").expect("temp dir");
        let copy_dst = bundle_dir.path().join("consensus_data");
        std::fs::copy(&data_path, &copy_dst).expect("copy reconciled data");
        let dst_dir = TempDir::with_prefix("test_cp_reconcile_dst").expect("temp dir");
        let stream = tokio::fs::File::open(&copy_dst).await.expect("open copy");
        let imported = ConsensusPack::stream_import(
            dst_dir.path(),
            stream,
            0,
            &previous_epoch,
            num_outputs as u64,
            Duration::from_secs(5),
        )
        .await
        .expect("raw copy of a reconciled pack must stream_import");
        wait_for(
            async || imported.get_consensus_output(num_outputs as u64).await.is_ok(),
            "last stream-imported output to be readable",
        )
        .await;
        for i in 0..num_outputs {
            let got = imported.get_consensus_output(i as u64 + 1).await.unwrap();
            compare_outputs(&got, &outputs[i]);
        }
        drop(imported);
        drop(pack);
    }

    /// The heal above must stay narrow.  A first record whose size prefix has been corrupted to
    /// run past EOF looks torn, but the position index still holds committed outputs behind it,
    /// so `open_append` must fail closed rather than truncate real consensus data away.
    #[tokio::test]
    async fn test_open_append_rejects_torn_first_record_with_indexed_outputs() {
        let temp_dir = TempDir::with_prefix("test_cp_torn_with_data").expect("temp dir");
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
            for i in 0..3 {
                let output =
                    make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
                parent = output.digest();
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }

        // Inflate the meta record's size prefix so its declared extent runs past EOF: the record
        // reads as torn even though three outputs sit behind it, fully addressable through the
        // position index.  The value stays under MAX_RECORD_SIZE so the read reaches EOF rather
        // than tripping the size guard.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let mut bytes = std::fs::read(&data_path).expect("read data");
        let len_before = bytes.len() as u64;
        let inflated = len_before as u32;
        bytes[DATA_HEADER_BYTES..DATA_HEADER_BYTES + 4].copy_from_slice(&inflated.to_le_bytes());
        std::fs::write(&data_path, &bytes).expect("write data");

        let result = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone());
        assert!(
            matches!(result, Err(super::PackError::EpochLoad(_))),
            "expected EpochLoad, got {result:?}"
        );
        let len_after = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(len_before, len_after, "failed open must leave the data file untouched");
    }

    /// The heal rebuilds the pack from its data header, so the digest indexes must be
    /// reinitialized with it.  Left carrying the length from before the tear, recovery would
    /// truncate the meta just written back down to that stale length whenever the rewritten
    /// meta is the longer one, leaving the pack torn again and re-healing on every restart.
    /// `recover_pack` re-stamps `set_data_file_length` after replay, so the `open_static` gate
    /// below (which runs `files_consistent`) only opens once the tracked length matches the log.
    #[tokio::test]
    async fn test_heal_reinitializes_index_lengths_for_a_longer_meta() {
        let temp_dir = TempDir::with_prefix("test_cp_heal_longer_meta").expect("temp dir");
        let small = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(4).expect("nonzero"))
            .build();
        let small_committee = small.committee();
        let prev_small = test_previous_epoch(&small_committee);

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                prev_small.clone(),
                small_committee.clone(),
            )
            .expect("open pack");
            pack.persist().await.expect("persist");
        }

        // Tear the meta, leaving the digest indexes synced to the pre-tear length.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let small_len = std::fs::metadata(&data_path).expect("metadata").len();
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(DATA_HEADER_BYTES as u64 + 2).expect("truncate");
        }

        // Reopen with a larger committee so the rewritten meta is longer than the stale length.
        let big = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(10).expect("nonzero"))
            .build();
        let big_committee = big.committee();
        let prev_big = test_previous_epoch(&big_committee);
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                prev_big.clone(),
                big_committee.clone(),
            )
            .expect("heal with a longer meta");
            pack.persist().await.expect("persist after heal");
        }

        // The healed pack must hold the whole new meta, not a copy truncated to the old length.
        let healed_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            healed_len > small_len,
            "healed pack ({healed_len}) was cut back to the stale index length ({small_len})"
        );

        // open_static both runs files_consistent and re-reads the meta, so it is the gate that
        // catches a meta left torn by a stale-length truncate.
        ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("healed pack must open read-only with a readable meta");
    }

    /// Same stale-index hazard as above, but landing in the header-only branch: the data file
    /// is rolled back to exactly `DATA_HEADER_BYTES` while the digest indexes keep their larger
    /// pre-damage length.  That branch writes a fresh meta too, so it needs the same index
    /// reinitialization -- without `recover_pack` re-stamping `set_data_file_length`, recovery
    /// cuts the new meta down to the stale length and hands back a live pack whose first record
    /// is torn.
    #[tokio::test]
    async fn test_header_only_reinitializes_index_lengths_for_a_longer_meta() {
        let temp_dir = TempDir::with_prefix("test_cp_header_only_longer").expect("temp dir");
        let small = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(4).expect("nonzero"))
            .build();
        let small_committee = small.committee();
        let prev_small = test_previous_epoch(&small_committee);

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                prev_small.clone(),
                small_committee.clone(),
            )
            .expect("open pack");
            pack.persist().await.expect("persist");
        }

        // Roll the data file back to exactly the header, leaving the digest indexes synced to
        // the pre-damage length.  This lands in the header-only branch, not the tear heal.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let small_len = std::fs::metadata(&data_path).expect("metadata").len();
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(DATA_HEADER_BYTES as u64).expect("truncate");
        }

        let big = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(10).expect("nonzero"))
            .build();
        let big_committee = big.committee();
        let prev_big = test_previous_epoch(&big_committee);
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                prev_big.clone(),
                big_committee.clone(),
            )
            .expect("header-only reopen with a longer meta");
            pack.persist().await.expect("persist after reopen");
        }

        let healed_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            healed_len > small_len,
            "rewritten meta ({healed_len}) was cut back to the stale index length ({small_len})"
        );
        ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("pack must open read-only with a readable meta");
    }

    /// A torn *tail* on a pack whose meta is intact is the one recovery path the
    /// `wrote_fresh_meta` guard never touches: `open_append` reads a valid first record
    /// (`have_pack == true`, `wrote_fresh_meta == false`), so the guard at the top of the open
    /// does not fire and the only thing that reconciles the digest indexes' tracked
    /// `data_file_length` down to the truncated log is `recover_pack`'s `set_data_file_length`
    /// re-stamp after replay.  Delete that re-stamp and `files_consistent` fails on the next open,
    /// so this pins it directly -- unlike the longer-meta heals above, which `recover_pack` would
    /// still fix even with the guard removed.
    #[tokio::test]
    async fn test_recover_pack_restamps_index_length_on_a_torn_tail() {
        let temp_dir = TempDir::with_prefix("test_cp_torn_tail_restamp").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // End of output 2 is the last self-consistent point once output 3 is torn.
        let output2_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(2).await.expect("output 2 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let full_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(full_len > output2_end, "output 3 must extend past output 2");

        // Tear output 3 mid-record, a few bytes past output 2's end.  The digest indexes still
        // track `full_len`, so recovery is forced to rebuild -- but the meta stays intact, so the
        // wrote_fresh_meta guard does not fire and only `recover_pack` can re-stamp the length.
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(output2_end + 4).expect("truncate");
        }

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("torn tail with an intact meta must recover");
            pack.persist().await.expect("persist after recovery");
        }

        // recover_pack dropped the torn output 3 and re-stamped the tracked length to the last
        // complete output, so the log ends exactly at output 2 again.
        let recovered_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(
            recovered_len, output2_end,
            "recovery must trim the torn tail back to the last complete output"
        );

        // `open_static` runs `files_consistent`, whose exact-equality check (`data_file_length`
        // == `file_len`) only holds if `recover_pack` re-stamped the shortened length onto the
        // rebuilt indexes.  Output 2 must survive; the torn output 3 must be gone.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("recovered pack must pass files_consistent and open read-only");
        assert_eq!(
            pack.get_consensus_output(2).await.expect("output 2 survives").number(),
            2,
            "the last complete output must read back after recovery"
        );
        assert!(
            pack.get_consensus_output(3).await.is_err(),
            "the torn output 3 must not be readable after recovery"
        );
    }

    /// The other half of the narrowing: an empty position index alone is not licence to
    /// truncate.  This meta record is whole on disk and merely fails its crc, which is
    /// corruption at rest rather than an interrupted append, so `open_append` must fail closed
    /// even though nothing is indexed behind it.
    #[tokio::test]
    async fn test_open_append_rejects_corrupt_first_record_without_outputs() {
        let temp_dir = TempDir::with_prefix("test_cp_corrupt_no_data").expect("temp dir");
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
            pack.persist().await.expect("persist");
        }

        // Flip a byte inside the meta value.  The record is complete -- size prefix and crc
        // suffix are both present -- so this reads as corruption, not as a tear.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let mut bytes = std::fs::read(&data_path).expect("read data");
        bytes[DATA_HEADER_BYTES + 6] ^= 0xff;
        std::fs::write(&data_path, &bytes).expect("write data");
        let len_before = bytes.len() as u64;

        let result = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone());
        assert!(
            matches!(result, Err(super::PackError::EpochLoad(_))),
            "expected EpochLoad, got {result:?}"
        );
        let len_after = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(len_before, len_after, "failed open must leave the data file untouched");
    }

    /// The crash window "data header written, meta never appended" must stay recoverable:
    /// `open_append` on a header-only data file appends the meta and the pack works from
    /// then on.
    #[tokio::test]
    async fn test_open_append_appends_meta_to_header_only_file() {
        let temp_dir = TempDir::with_prefix("test_cp_header_only").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Write only the data header, the state a crash right after pack creation leaves.
        let base_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&base_dir).expect("create epoch dir");
        let data_path = base_dir.join(Inner::DATA_NAME);
        {
            let mut raw: Pack<PackRecord> =
                Pack::open(&data_path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("raw pack");
            raw.commit().expect("commit header");
        }
        assert_eq!(
            std::fs::metadata(&data_path).expect("metadata").len(),
            DATA_HEADER_BYTES as u64,
            "setup must produce a header-only data file"
        );

        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append on header-only file");
            let parent = ConsensusHeader::default().digest();
            let output = make_test_output(&committee, 0, chain.clone(), 1, parent);
            pack.save_consensus_output(output).await.unwrap();
            pack.persist().await.expect("persist");
        }
        assert!(
            std::fs::metadata(&data_path).expect("metadata").len() > DATA_HEADER_BYTES as u64,
            "meta must have been appended"
        );

        // Reopening finds the appended meta and compares clean: recovering the crash window
        // is idempotent.
        {
            let pack =
                ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
                    .expect("reopen append");
            assert!(pack.get_consensus_output(1).await.is_ok());
            pack.persist().await.expect("persist after reopen");
        }

        // The append reopen heals through `recover_pack`; `open_static` is the door that runs
        // `files_consistent`, so a read-only reopen pins the recovered pack for read-only
        // consumers too.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("open static after header-only recovery");
        let output = pack
            .get_consensus_output(1)
            .await
            .expect("recovered output reads back through the static path");
        assert_eq!(output.number(), 1, "static read must return the recovered output");
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

    /// `verify_epoch_meta` pins the [`EpochMeta`]'s embedded committee to the record's own epoch.
    ///
    /// The committee's epoch is what selects its bcs layout, so a meta whose outer epoch and
    /// committee epoch disagree carries a committee decoded under a layout that record does not
    /// select — while `epoch_meta.epoch` and `epoch_meta.committee` are used downstream as if they
    /// agreed. Every other check here is satisfied (identical key set, matching start number and
    /// genesis links), so only the committee-epoch check can reject these metas, and the error text
    /// is asserted to prove it is the arm that fired.
    #[test]
    fn test_verify_epoch_meta_rejects_committee_epoch_mismatch() {
        use std::collections::BTreeMap;

        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{Address, Authority, BlsKeypair, BlsPublicKey};

        use crate::consensus_pack::{verify_epoch_meta, EpochMeta, PackError};

        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let keys: Vec<BlsPublicKey> =
            (0..3).map(|_| *BlsKeypair::generate(&mut rng).public()).collect();
        let authorities = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, Authority::new_for_test(*k, Address::repeat_byte(i as u8 + 1))))
            .collect::<BTreeMap<_, _>>();
        let committee = Committee::new_for_test(authorities, 1, BTreeMap::default());

        let previous = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys.clone(),
            final_consensus: ConsensusNumHash::new(77, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        let meta_with = |committee: Committee| EpochMeta {
            epoch: 1,
            committee,
            start_consensus_number: previous.final_consensus.number + 1,
            genesis_exec_state: previous.final_state,
            genesis_consensus: previous.final_consensus,
        };

        // A committee carrying the record's own epoch verifies.
        verify_epoch_meta(1, &previous, &meta_with(committee.clone()))
            .expect("a committee at the record's epoch must verify");

        // A committee from either side of the record's epoch does not, even though its key set is
        // the one the previous record hands off to.
        for committee_epoch in [0, 2] {
            let meta = meta_with(committee.advance_epoch_for_test(committee_epoch));
            let Err(err) = verify_epoch_meta(1, &previous, &meta) else {
                panic!("committee epoch {committee_epoch} must not verify in an epoch-1 record")
            };
            assert!(
                matches!(err, PackError::InvalidEpoch(1, _)),
                "committee epoch {committee_epoch}: got {err:?}"
            );
            assert!(
                err.to_string().contains(&format!("committee is for epoch {committee_epoch}")),
                "committee epoch {committee_epoch}: a different check rejected this meta: {err}"
            );
        }
    }

    /// PEER PATH: `stream_import` rejects a record stream whose [`EpochMeta`] carries a committee
    /// for a different epoch, and rejects it before appending anything.
    ///
    /// This is the door the check exists for: a local write cannot produce such a meta, since
    /// `Inner::open_append` derives the record's epoch from the committee it is handed. Only a
    /// hostile or buggy peer (or an imported bundle) can, and the committee it ships is the
    /// validator set every output in the pack would then be verified against.
    #[tokio::test]
    async fn test_stream_import_rejects_committee_epoch_mismatch() {
        use crate::{
            archive::pack::Pack,
            consensus_pack::{EpochMeta, PackError, PackRecord},
        };

        let temp_dir = TempDir::with_prefix("test_cp_meta_committee_epoch").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // The container and every linkage field are well formed for epoch 0; only the embedded
        // committee claims epoch 1.
        let source = temp_dir.path().join("peer_stream");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&source, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open peer stream");
            pack.append(&PackRecord::EpochMeta(EpochMeta {
                epoch: 0,
                committee: committee.advance_epoch_for_test(1),
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            }))
            .expect("append hostile meta");
            pack.commit().expect("commit peer stream");
        }

        let target = TempDir::with_prefix("test_cp_meta_committee_epoch_out").expect("temp dir");
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        let err = ConsensusPack::stream_import(
            target.path(),
            stream,
            0,
            &previous_epoch,
            1,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a meta whose committee is for another epoch must not import");
        assert!(matches!(err, PackError::InvalidEpoch(0, _)), "got {err:?}");
        assert!(
            err.to_string().contains("committee is for epoch 1"),
            "a different check rejected the import: {err}"
        );

        // Rejected before the append: the epoch has no readable meta record, so no reader can pick
        // the hostile committee up.
        assert!(
            ConsensusPack::open_append_exists(target.path(), 0).is_err(),
            "the rejected meta was appended anyway"
        );
    }

    /// Deterministic BLS seed signature for fork-active fixture headers: the keypair comes
    /// from a seeded rng and BLS signing is deterministic, so the fixture bytes are stable
    /// across runs.
    fn nesting_seed_signature(seed: u64) -> tn_types::BlsSignature {
        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::Signer as _;

        let keypair = tn_types::BlsKeypair::generate(&mut StdRng::seed_from_u64(seed));
        keypair.sign(b"pack-nesting-fixture")
    }

    /// A header pinned to `epoch` whose remaining fields all derive from `tag`, giving each
    /// fixture header distinct, fully deterministic bytes.
    fn nesting_header(epoch: tn_types::Epoch, tag: u8) -> tn_types::Header {
        use tn_types::{AuthorityIdentifier, HeaderDigest};

        HeaderBuilder::default()
            .author(AuthorityIdentifier::from_bytes([tag; 32]))
            .round(u32::from(tag))
            .epoch(epoch)
            .created_at(u64::from(tag))
            .parents([HeaderDigest::new([tag; 32])].into_iter().collect())
            .seed_signature(nesting_seed_signature(u64::from(tag)))
            .build()
    }

    /// Tier-1 nesting proof at the pack level (#1032, #1086 PR-1): a [`PackRecord::Consensus`]
    /// whose sub-DAG nests headers of BOTH wire layouts must round-trip byte-exactly through
    /// the pack record codec with deep equality and per-element epoch-gate visibility.
    ///
    /// Under `adiri` the epoch-0 elements are legacy seven-field headers and the `u32::MAX`
    /// element carries `seed_signature`, so the record exercises the legacy→V1 and V1→legacy
    /// visitor hand-offs three levels deep (record → consensus header → sub-DAG → headers);
    /// without `adiri` every element is fork-active and the same record pins the all-V1 path
    /// in the default-feature suite.
    #[test]
    fn test_pack_record_mixed_epoch_sub_dag_round_trip() {
        use crate::consensus_pack::PackRecord;
        use tn_types::{decode, encode, Epoch};

        // Epoch 0 is legacy only under `adiri`; `Epoch::MAX` (far past the `adiri` fork
        // epoch) is fork-active in every build.
        let headers = vec![
            nesting_header(0, 0x11),
            nesting_header(Epoch::MAX, 0x22),
            nesting_header(0, 0x33),
        ];
        let expected_gate: Vec<bool> =
            headers.iter().map(|header| header.seed_signature().is_some()).collect();
        // Anti-vacuity: the sub-DAG genuinely mixes both layouts under `adiri`.
        #[cfg(feature = "adiri")]
        assert_eq!(vec![false, true, false], expected_gate, "adiri epoch 0 must be legacy");
        #[cfg(not(feature = "adiri"))]
        assert_eq!(vec![true, true, true], expected_gate, "non-adiri epochs are all fork-active");

        let consensus = ConsensusHeader {
            parent_hash: ConsensusHeaderDigest::default(),
            sub_dag: CommittedSubDag::new_with_headers_for_test(headers),
            number: 42,
            extra: Default::default(),
        };
        let record = PackRecord::Consensus(Box::new(consensus.clone()));

        // `encode` is exactly the serialization `write_value` runs before framing and
        // compression, so a byte round trip here is a byte round trip of the stored record.
        let bytes = encode(&record);
        let decoded: PackRecord = decode(&bytes);
        assert_eq!(
            bytes,
            encode(&decoded),
            "re-encode of the decoded pack record must reproduce the original bytes"
        );

        let decoded_consensus = decoded
            .into_consensus()
            .expect("Consensus record must decode back to the Consensus variant");
        assert_eq!(consensus, decoded_consensus, "pack-record round trip must be deeply equal");
        assert_eq!(
            consensus.digest(),
            decoded_consensus.digest(),
            "consensus header digest must survive the round trip"
        );
        assert_eq!(
            consensus.sub_dag.digest(),
            decoded_consensus.sub_dag.digest(),
            "sub-dag digest must survive the round trip"
        );

        let decoded_gate: Vec<bool> = decoded_consensus
            .sub_dag
            .headers()
            .iter()
            .map(|header| header.seed_signature().is_some())
            .collect();
        assert_eq!(
            expected_gate, decoded_gate,
            "per-element gate visibility must survive the round trip"
        );
    }

    /// Epoch of the frozen pre-fork pack: 406.
    ///
    /// One epoch below `CONSENSUS_REGISTRY_FORK_EPOCH` (407), the documented arming floor of the
    /// multi-workers fork (#554), and the same epoch `tn_types`' `LEGACY_FIXTURE_EPOCH`
    /// pins — so the frozen committee vector there and the frozen pack file here describe one wire
    /// moment from opposite ends of the stack.
    ///
    /// One below the floor rather than the floor itself, because the fork may legally be armed AT
    /// 407: the gate is `>=`, so 407 would become post-fork and the anti-vacuity assert in
    /// `test_golden_legacy_pack_regenerates` would fail. 406 is structurally pre-fork under every
    /// legal arming, so the embedded [`Committee`] encodes in the legacy single-worker layout
    /// whichever epoch the arming PR picks. It is still at or above `SEED_SIGNATURE_FORK_EPOCH`
    /// (383), so the nested headers carry `seed_signature`: exactly the shape of an epoch pack
    /// sitting on an adiri node's disk today.
    const LEGACY_PACK_EPOCH: Epoch = 406;

    /// Final consensus number of the epoch before [`LEGACY_PACK_EPOCH`].
    ///
    /// Nonzero on purpose: it keeps the fixture on the `previous_epoch.final_consensus.number + 1`
    /// branch of `Inner::open_append` rather than the epoch-0 special case, so the frozen
    /// `start_consensus_number` is a value the linkage checks can actually disagree with.
    const LEGACY_PACK_PREV_CONSENSUS: u64 = 9_100;

    /// First consensus number in the frozen pack.
    ///
    /// Only the `adiri` lane ever reads it: on a post-fork build the frozen bytes never decode far
    /// enough to have a consensus range at all.
    #[cfg(feature = "adiri")]
    const LEGACY_PACK_FIRST_CONSENSUS: u64 = LEGACY_PACK_PREV_CONSENSUS + 1;

    /// Last consensus number in the frozen pack, which holds two outputs.
    const LEGACY_PACK_LAST_CONSENSUS: u64 = LEGACY_PACK_PREV_CONSENSUS + 2;

    /// FROZEN pre-fork epoch pack: the complete, sealed `epoch-406/data` file (978 bytes) a build
    /// of this crate writes at [`LEGACY_PACK_EPOCH`] on the `adiri` lane — data header, then an
    /// `EpochMeta` record whose [`Committee`] is in the legacy single-worker layout, then two
    /// consensus outputs (header record followed by its batch record, the v1 ordering).
    ///
    /// This is the layout of every consensus pack already on adiri disk: `Committee` is embedded in
    /// `EpochMeta`, the FIRST record of every pack, and bcs is not self-describing. Before the
    /// epoch-gated encoder landed, every adiri node restarting against an existing consensus-db
    /// failed to decode this record and exited 1, and epoch-pack peer sync broke across versions.
    /// The tests below open these exact bytes through every read door in the crate.
    ///
    /// If a test here fails, work out WHICH pin moved before touching this constant:
    ///
    /// - [`LEGACY_PACK_EPOCH`] moving is the one legitimate reason to re-freeze these bytes: the
    ///   epoch is a field of the `EpochMeta`, of every nested header and of every batch, so its
    ///   bytes move with it. Regenerate from `test_golden_legacy_pack_regenerates`, never by hand.
    /// - `tn_types`' `golden_legacy_committee_wire_bytes_pinned` also failing means the committee
    ///   wire layout moved. That is a compatibility break to fix in the encoder, NOT a constant to
    ///   refresh — refreshing it strands every pack already written.
    /// - that pin still passing means the container moved, not the payload: the record framing, the
    ///   crc, the zstd encoder, or the fixture. `test_golden_legacy_pack_regenerates` cross-checks
    ///   the committee bytes inside the frozen container for exactly this reason, so a green
    ///   committee assertion next to a red byte-identity assertion is the "container moved" signal.
    ///
    /// The fixture takes no rng, no clock and no OS-assigned port, so nothing else can move it.
    const GOLDEN_LEGACY_PACK_HEX: &str = "74656c6e6574010091dcabc6ad9363a20100000001000000a9434593aa01000028b52ffd00580d0d0004170096010000026085ae9977dafa1a29bfeecb4ec68ac8b9690e9adfc6757f6fd30dcc0e040d918c7eee1c50cff8f6e749017ebf77fa1d570f9a74ab3abf73a4ab9a2da56e2603e23f75800adc0f0c6cbfb7c9e3b9044fb7f3fbede2ba642ce75e375d5bbf6e8735140260ac7fa63dfc38bbf3712e27a180391bca4ccabf609c5967a0592eff420b6235f3f2b323051cb099acc3969aca310f7ff4191b2d6db43fafc2c9592f7e5f73981107975d3d92b843891e724dbc9f05b5eee5a3b2b1fc782ede8149f30830b8444414010b047f00000191029c41cd032408011220b14a3296426492458270c2e577fdc549b6d67155e5800b0bf96c3f4106b4ae7100a029c602145e9a9f1672b151652377fa8c23fc78ded1add621fe177d33b8ebcd4214009c40e0fcb53429020d03e8f4e471ec73993f9329ad0d76e69cfdfcb08c94aa39fdab28055139b0f6daf33b3fdc294e2bfc1ad914de91f7d6e9f717b4509c047fbc6acc008d2300921000205e8c207a1100b88654650c7403e7886b049c00d2c0070e2df686f8d09a0f642c550918b62b0d7882a193613a78d24e27a71005806fcb4ec500000028b52ffd0058e50500740a02207a017b403bb2f1bcfe223c27bf0d350aa0dc2e7f02f257580d538ac8a36f21223d29010000009601000001000120c0a1c7fd531551d30b3db2802e873b75067059e1d41d433b8e086c0b79143b5e002000309455941aa83bcaa9f33fa21533d526c4b824ef51132c5629a21619f9975befc7fcf1097d590c01356c37ba346bfc2c42203cd4585ccad5b8f09b28c2444dfda62125ab6d4c235efc635ecc10ddf4f447048d2307000833c000bf60980d828607f61b184a03dc39a570361e00000028b52ffd0058ad000060010108019601000014000700031000044e2523027f4c188fe300000028b52ffd0058d50600640c0220e9ef2e767a88948d2c477c097f516a936acdc86a3ea64f290630d28c5030d6e3015237b9c5d795289c9a054ba2761ff631cd622c9d94df6ab749814cede8549d3f02000000960100000200012031a3a82b0b9828c83bafb69afd821c11801ec62cfb6a88b7fed03599e21b507c002000309455941aa83bcaa9f33fa21533d526c4b824ef51132c5629a21619f9975befc7fcf1097d590c01356c37ba346bfc2c42206f9ec6c464377d4f9f935156387873e428662cfe18ef258641136a71aa5cb7da8e2306000833c000bf60980d828607f637033003692185471e00000028b52ffd0058ad000060010108029601000014000700031000044e252302fb1782dc";

    /// Decode [`GOLDEN_LEGACY_PACK_HEX`], failing loudly on a malformed constant.
    fn golden_legacy_pack_bytes() -> Vec<u8> {
        tn_types::hex::decode(GOLDEN_LEGACY_PACK_HEX).expect("frozen hex vector must be valid hex")
    }

    /// Lay the frozen bytes down as a bare `epoch-406/data` file with NO sidecar indexes, and
    /// return its path.
    ///
    /// The strictest shape a read door can be handed: nothing but the pack stream, so whatever it
    /// learns about the epoch it learns by decoding the `EpochMeta` record itself.
    fn write_golden_legacy_data_file(dir: &std::path::Path) -> std::path::PathBuf {
        let base = dir.join(format!("epoch-{LEGACY_PACK_EPOCH}"));
        std::fs::create_dir_all(&base).expect("create pack dir");
        let path = base.join(Inner::DATA_NAME);
        std::fs::write(&path, golden_legacy_pack_bytes()).expect("write frozen data file");
        path
    }

    /// Deterministic BLS keypair for fixture slot `slot`.
    ///
    /// A fixed scalar rather than a seeded rng, so the derived public key — and with it the
    /// `authorities` map order and every frozen byte above — survives `rand` and `blst` bumps as
    /// well as reruns. The leading bytes stay zero, which keeps the scalar nonzero and far below
    /// the BLS12-381 group order, the only two values `blst` rejects.
    #[cfg(feature = "adiri")]
    fn legacy_pack_bls_keypair(slot: u8) -> tn_types::BlsKeypair {
        let mut scalar = [0_u8; 32];
        scalar[30] = slot;
        scalar[31] = 0x2A;
        tn_types::BlsKeypair::from_bytes(&scalar)
            .expect("fixture bls scalar is a valid private key")
    }

    /// A fixture [`P2pNode`](tn_types::P2pNode) from a fixed ed25519 seed and port.
    ///
    /// ed25519 secret keys *are* 32-byte seeds, so a fixed seed yields a fixed public key with no
    /// rng in the path, and the multiaddr comes from a literal rather than an OS-assigned port.
    /// `rpc` stays `None`: the `Some` arm needs a `url::Url`, which this crate does not depend on,
    /// and `tn_types`' frozen committee vectors already pin both arms.
    #[cfg(feature = "adiri")]
    fn legacy_pack_p2p_node(tag: u8, slot: u8, port: u16) -> tn_types::P2pNode {
        let mut seed = [0_u8; 32];
        seed[0] = tag;
        seed[1] = slot;
        tn_types::P2pNode {
            network_address: format!("/ip4/127.0.0.1/udp/{port}/quic-v1")
                .parse()
                .expect("fixture multiaddr parses"),
            network_key: tn_types::NetworkKeypair::ed25519_from_bytes(seed)
                .expect("a 32-byte array is a valid ed25519 secret seed")
                .public()
                .clone()
                .into(),
            rpc: None,
        }
    }

    /// The frozen pack's committee: two authorities, each with a single-worker bootstrap server.
    ///
    /// Two is the minimum — `CommitteeInner::load` asserts a committee larger than one — and the
    /// point of the fixture is the wire layout, not the quorum math, so it stays at the minimum to
    /// keep the frozen vector small. One worker per server is the only shape the legacy layout can
    /// express at all.
    #[cfg(feature = "adiri")]
    fn legacy_pack_committee() -> Committee {
        use std::collections::BTreeMap;

        use tn_types::{Address, Authority, BootstrapServer};

        let mut authorities = BTreeMap::new();
        let mut bootstrap_servers = BTreeMap::new();
        for slot in 0..2_u8 {
            let key = *legacy_pack_bls_keypair(slot).public();
            authorities.insert(key, Authority::new_for_test(key, Address::repeat_byte(slot + 1)));
            bootstrap_servers.insert(
                key,
                BootstrapServer::new(
                    legacy_pack_p2p_node(0xB0, slot, 40_000 + u16::from(slot)),
                    vec![legacy_pack_p2p_node(0xC0, slot, 41_000 + u16::from(slot))],
                ),
            );
        }
        Committee::new_for_test(authorities, LEGACY_PACK_EPOCH, bootstrap_servers)
    }

    /// The previous epoch's record the frozen pack links to.
    ///
    /// Every field here is frozen INTO the pack: `open_append` copies `final_state` and
    /// `final_consensus` into the `EpochMeta` and derives `start_consensus_number` from them, and
    /// `verify_epoch_meta` re-checks all three plus the committee key set on every import and
    /// validation. Feeding this record to those doors therefore pins the meta's fields, not just
    /// its committee.
    #[cfg(feature = "adiri")]
    fn legacy_pack_previous_epoch(committee: &Committee) -> EpochRecord {
        EpochRecord {
            epoch: LEGACY_PACK_EPOCH - 1,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            final_state: tn_types::BlockNumHash::new(4_242, tn_types::B256::repeat_byte(0x5E)),
            final_consensus: ConsensusNumHash::new(
                LEGACY_PACK_PREV_CONSENSUS,
                ConsensusHeaderDigest::from([0x7A_u8; 32]),
            ),
            ..Default::default()
        }
    }

    /// One fully deterministic output for the frozen pack: a single-certificate sub-DAG whose
    /// leader header references exactly one batch.
    ///
    /// `tag` seeds every value that distinguishes one fixture output from another (round,
    /// `created_at`, the batch's payload bytes, and which authority authors it), so the two outputs
    /// differ in every hashed field while neither reaches for a clock or an rng. The seed signature
    /// is a real BLS signature over a fixed message: BLS signing takes no nonce, so it is
    /// reproducible, and it is on the wire at this epoch because the seed-signature fork is active
    /// here.
    #[cfg(feature = "adiri")]
    fn legacy_pack_output(
        committee: &Committee,
        number: u64,
        parent: ConsensusHeaderDigest,
        tag: u8,
    ) -> ConsensusOutput {
        use tn_types::Signer as _;

        let batch =
            Batch::new_for_test(vec![vec![tag; 8]], ExecHeader::default(), 0, LEGACY_PACK_EPOCH);
        let authorities = committee.authorities();
        let authority = authorities
            .get(usize::from(tag) % authorities.len())
            .expect("modulo keeps the index in range");
        let seed_signature = legacy_pack_bls_keypair(0xF0).sign(b"pack-fixture-seed");
        let header = HeaderBuilder::default()
            .author(authority.id())
            .round(u32::from(tag))
            .epoch(LEGACY_PACK_EPOCH)
            .created_at(u64::from(tag))
            .seed_signature(seed_signature)
            .with_payload_batch(&batch, 0_u16)
            .build();
        let mut leader = Certificate::default();
        leader.update_header_for_test(header);
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            u64::from(tag),
            ReputationScores::default(),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let batch_digests: VecDeque<BlockHash> = [batch.digest()].into_iter().collect();
        ConsensusOutput::new(
            sub_dag,
            parent,
            number,
            false,
            batch_digests,
            vec![CertifiedBatch { address: authority.execution_address(), batches: vec![batch] }],
        )
    }

    /// The frozen pack's two outputs, chained: the first parents off the previous epoch's final
    /// consensus header, the second off the first. Tags 1 and 2 land on different authorities, so
    /// both committee members author one output and the decoder's author lookup is exercised for
    /// each.
    #[cfg(feature = "adiri")]
    fn legacy_pack_outputs(committee: &Committee, previous: &EpochRecord) -> Vec<ConsensusOutput> {
        let mut parent = previous.final_consensus.hash;
        let mut number = previous.final_consensus.number + 1;
        let mut outputs = Vec::new();
        for tag in [1_u8, 2] {
            let output = legacy_pack_output(committee, number, parent, tag);
            parent = output.digest();
            number += 1;
            outputs.push(output);
        }
        outputs
    }

    /// Write the fixture pack through the normal write path (`open_append` +
    /// `save_consensus_output`) into `dir` and return the resulting `data` file bytes.
    ///
    /// On the `adiri` lane at [`LEGACY_PACK_EPOCH`] the gated encoder emits the legacy committee
    /// layout, which `tn_types`' differentials prove is byte-identical to the pre-#554 derive. A
    /// pack this writes at that epoch therefore IS a pre-fork pack, byte for byte — which is what
    /// makes freezing its output a fixture of history rather than of this build.
    #[cfg(feature = "adiri")]
    async fn write_legacy_pack(dir: &std::path::Path) -> Vec<u8> {
        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        let pack = ConsensusPack::open_append(dir, previous_epoch.clone(), committee.clone())
            .expect("open fixture pack for append");
        for output in legacy_pack_outputs(&committee, &previous_epoch) {
            pack.save_consensus_output(output).await.expect("save fixture output");
        }
        pack.persist().await.expect("persist fixture pack");
        drop(pack);
        std::fs::read(dir.join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME))
            .expect("read fixture data file")
    }

    /// Materialize a complete pack directory (data file plus sidecar indexes) in `dir` FROM the
    /// frozen bytes, by feeding them to `stream_import` the way a peer stream arrives.
    ///
    /// `stream_import` is the only path that builds a pack's indexes from a record stream, so it is
    /// how the doors that need sidecars (`open_static`, and reading outputs back through
    /// `open_append_exists` / `open_append`) get an on-disk pack whose `data` file is provably the
    /// frozen constant — asserted here, so every caller inherits the guarantee.
    #[cfg(feature = "adiri")]
    async fn import_golden_legacy_pack(dir: &std::path::Path) {
        let frozen = golden_legacy_pack_bytes();
        let source = dir.join("peer_stream");
        std::fs::write(&source, &frozen).expect("write peer stream");
        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        let pack = ConsensusPack::stream_import(
            dir,
            stream,
            LEGACY_PACK_EPOCH,
            &previous_epoch,
            LEGACY_PACK_LAST_CONSENSUS,
            Duration::from_secs(5),
        )
        .await
        .expect("stream import of the frozen pre-fork pack");
        pack.persist().await.expect("persist imported pack");
        drop(pack);
        let on_disk =
            std::fs::read(dir.join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME))
                .expect("read imported data file");
        assert_eq!(
            tn_types::hex::encode(&on_disk),
            GOLDEN_LEGACY_PACK_HEX,
            "the materialized pack's data file is not the frozen pre-fork bytes"
        );
    }

    /// Assert `pack`'s handle-level committee is the frozen pre-fork committee, in the legacy
    /// single-worker shape.
    ///
    /// `Committee`'s `PartialEq` deliberately ignores bootstrap servers, so the map is compared
    /// separately — without that a pack whose bootstrap hints decoded to something else entirely
    /// would compare equal.
    #[cfg(feature = "adiri")]
    fn assert_legacy_pack_committee(pack: &ConsensusPack) {
        let expected = legacy_pack_committee();
        assert_eq!(pack.epoch(), LEGACY_PACK_EPOCH, "pack epoch moved");
        assert_eq!(pack.committee().epoch(), LEGACY_PACK_EPOCH, "meta committee epoch moved");
        assert_eq!(pack.committee().size(), 2, "meta committee authority count moved");
        assert_eq!(
            pack.committee().number_of_workers(),
            1,
            "the legacy layout carries no worker count, so it must decode as single-worker"
        );
        assert_eq!(pack.committee().bootstrap_servers().len(), 2, "bootstrap server count moved");
        assert!(
            pack.committee().bootstrap_servers().values().all(|server| server.num_workers() == 1),
            "the legacy layout holds exactly one worker per bootstrap server"
        );
        assert_eq!(*pack.committee(), expected, "the meta holds a different committee");
        assert_eq!(
            pack.committee().bootstrap_servers(),
            expected.bootstrap_servers(),
            "the meta holds different bootstrap servers"
        );
    }

    /// Read both frozen outputs back through `pack` and compare them to the fixture, and confirm
    /// the frozen `start_consensus_number` by rejecting the number just below it.
    #[cfg(feature = "adiri")]
    async fn assert_legacy_pack_outputs(pack: &ConsensusPack) {
        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        let expected = legacy_pack_outputs(&committee, &previous_epoch);
        for output in &expected {
            let read_back = pack.get_consensus_output(output.number()).await.unwrap_or_else(|e| {
                panic!("read output {} from the frozen pack: {e}", output.number())
            });
            compare_outputs(&read_back, output);
        }
        assert!(
            pack.get_consensus_output(LEGACY_PACK_PREV_CONSENSUS).await.is_err(),
            "a number below the frozen start_consensus_number must be rejected"
        );
        assert!(
            !pack
                .contains_consensus_header_number(LEGACY_PACK_PREV_CONSENSUS)
                .await
                .expect("query the frozen pack"),
            "the frozen pack must not claim the previous epoch's final consensus number"
        );
        assert!(
            pack.contains_consensus_header_number(LEGACY_PACK_LAST_CONSENSUS)
                .await
                .expect("query the frozen pack"),
            "the frozen pack must claim its own last consensus number"
        );
    }

    /// ANCHOR (adiri): the frozen pre-fork pack is exactly what this build's normal write path
    /// produces at [`LEGACY_PACK_EPOCH`], so the fixture cannot drift away from the encoder it is
    /// meant to hold still.
    ///
    /// Also the anti-vacuity check for the whole group: it asserts the two gates that decide the
    /// frozen layout, so a stray `TN_MULTI_WORKERS_FORK_EPOCH` in the environment (or the fork
    /// being armed) fails here with a diagnosis instead of downstream as an unexplained byte diff.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_regenerates() {
        use tn_types::{encode, forks};

        assert!(
            !forks::multi_workers_fork_active(LEGACY_PACK_EPOCH),
            "epoch {LEGACY_PACK_EPOCH} must be PRE-fork for the frozen pack to be a legacy-layout \
             pack; is TN_MULTI_WORKERS_FORK_EPOCH set in the environment, or has the fork been \
             armed?"
        );
        assert!(
            forks::seed_signature_active(LEGACY_PACK_EPOCH),
            "epoch {LEGACY_PACK_EPOCH} must be seed-signature-active for the frozen headers to \
             carry seed_signature; is TN_SEED_SIGNATURE_FORK_EPOCH set in the environment?"
        );

        let first = TempDir::with_prefix("golden_legacy_pack_a").expect("temp dir");
        let bytes = write_legacy_pack(first.path()).await;
        assert_eq!(
            tn_types::hex::encode(&bytes),
            GOLDEN_LEGACY_PACK_HEX,
            "the pre-fork write path diverged from the frozen pack"
        );

        // a second, independent write of the same fixture must produce the same file: no rng,
        // clock or OS-assigned port leaked into the fixture
        let second = TempDir::with_prefix("golden_legacy_pack_b").expect("temp dir");
        assert_eq!(
            write_legacy_pack(second.path()).await,
            bytes,
            "the fixture pack is not reproducible"
        );

        // Cross-check the committee bytes INSIDE the frozen container. This separates the two ways
        // the byte-identity assertion above can fail: if this still passes, the container moved
        // (framing, crc, zstd) and not the committee wire layout.
        let pack = ConsensusPack::open_append_exists(first.path(), LEGACY_PACK_EPOCH)
            .expect("reopen the pack just written");
        assert_eq!(
            tn_types::hex::encode(encode(pack.committee())),
            tn_types::hex::encode(encode(&legacy_pack_committee())),
            "the committee stored in the frozen pack is not the legacy-layout fixture committee"
        );
        assert_legacy_pack_committee(&pack);
    }

    /// DOOR 1 (adiri): warm restart — `open_append_exists`, the door that exited 1 in production.
    ///
    /// Driven twice over the same frozen bytes: first as a bare `data` file with no sidecar
    /// indexes, which is the strictest form (everything the door knows about the epoch it decodes
    /// out of the `EpochMeta` record itself), then over a full pack directory, where the frozen
    /// outputs must also read back. Opening for append must not rewrite the file either way.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_opens_append_exists() {
        let bare = TempDir::with_prefix("golden_legacy_warm_bare").expect("temp dir");
        let data_file = write_golden_legacy_data_file(bare.path());
        {
            let pack = ConsensusPack::open_append_exists(bare.path(), LEGACY_PACK_EPOCH)
                .expect("warm restart against a bare frozen pre-fork data file");
            assert_eq!(pack.version, PACK_VERSION, "frozen pack version moved");
            assert!(!pack.is_static(), "a warm-restart handle is writable");
            assert_legacy_pack_committee(&pack);
            pack.persist().await.expect("persist");
        }
        assert_eq!(
            tn_types::hex::encode(std::fs::read(&data_file).expect("reread data file")),
            GOLDEN_LEGACY_PACK_HEX,
            "opening a pre-fork pack for append rewrote or truncated it"
        );

        let full = TempDir::with_prefix("golden_legacy_warm_full").expect("temp dir");
        import_golden_legacy_pack(full.path()).await;
        let pack = ConsensusPack::open_append_exists(full.path(), LEGACY_PACK_EPOCH)
            .expect("warm restart against a complete frozen pre-fork pack");
        assert_legacy_pack_committee(&pack);
        assert_legacy_pack_outputs(&pack).await;
    }

    /// DOOR 2 (adiri): historical reads — `open_static` over the frozen bytes.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_opens_static() {
        let dir = TempDir::with_prefix("golden_legacy_static").expect("temp dir");
        import_golden_legacy_pack(dir.path()).await;

        let pack = ConsensusPack::open_static(dir.path(), LEGACY_PACK_EPOCH)
            .expect("read-only open of the frozen pre-fork pack");
        assert!(pack.is_static(), "open_static must yield a read-only handle");
        assert_legacy_pack_committee(&pack);
        assert_legacy_pack_outputs(&pack).await;
    }

    /// DOOR 3 (adiri): peer epoch sync — `stream_import` of the frozen bytes.
    ///
    /// The load-bearing assertion is byte identity: importing and then serving a pre-fork pack must
    /// leave the meta record exactly as it arrived, because those same bytes are what this node
    /// hands to a peer still running a pre-fork build. A rewritten meta would decode here and
    /// nowhere else.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_stream_imports() {
        let dir = TempDir::with_prefix("golden_legacy_import").expect("temp dir");
        // asserts the imported data file is byte-identical to the frozen bytes
        import_golden_legacy_pack(dir.path()).await;

        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        let expected = legacy_pack_outputs(&committee, &previous_epoch);
        let pack = ConsensusPack::open_append_exists(dir.path(), LEGACY_PACK_EPOCH)
            .expect("reopen the imported pack");
        assert_legacy_pack_committee(&pack);
        assert_legacy_pack_outputs(&pack).await;

        // serving: the bytes handed to a peer decode back to the same outputs under the pack's own
        // (legacy-layout) committee
        for output in &expected {
            let served = pack
                .get_consensus_output_bytes(output.number())
                .await
                .expect("serve a frozen output to a peer");
            let decoded = pack.decode_output(served).await.expect("peer-side decode");
            compare_outputs(&decoded, output);
        }
        pack.persist().await.expect("persist after serving");
        drop(pack);

        let on_disk = std::fs::read(
            dir.path().join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME),
        )
        .expect("reread the imported data file");
        assert_eq!(
            tn_types::hex::encode(&on_disk),
            GOLDEN_LEGACY_PACK_HEX,
            "importing and serving a pre-fork pack rewrote its bytes, so peers on a pre-fork build \
             would no longer be able to read it"
        );
    }

    /// DOOR 4 (adiri): the meta-compare arm of `open_append`.
    ///
    /// Reopening a pre-fork pack with the same committee takes the compare branch — the meta this
    /// build constructs must equal the one decoded off disk — so it must succeed WITHOUT appending
    /// a second `EpochMeta` record. The file length and bytes are unchanged.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_reopens_append_without_duplicate_meta() {
        let dir = TempDir::with_prefix("golden_legacy_reappend").expect("temp dir");
        import_golden_legacy_pack(dir.path()).await;
        let data_file =
            dir.path().join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME);
        let len_before = std::fs::metadata(&data_file).expect("stat data file").len();

        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        {
            let pack =
                ConsensusPack::open_append(dir.path(), previous_epoch.clone(), committee.clone())
                    .expect("reopen the frozen pre-fork pack for append with the same committee");
            assert_legacy_pack_committee(&pack);
            assert_legacy_pack_outputs(&pack).await;
            pack.persist().await.expect("persist");
        }

        assert_eq!(
            std::fs::metadata(&data_file).expect("stat data file").len(),
            len_before,
            "open_append grew a pre-fork pack, so it appended a duplicate EpochMeta"
        );
        assert_eq!(
            tn_types::hex::encode(std::fs::read(&data_file).expect("reread data file")),
            GOLDEN_LEGACY_PACK_HEX,
            "open_append rewrote the frozen pre-fork bytes"
        );

        // A validation pass proves the file still holds exactly one EpochMeta: a second one is
        // reported as an EpochMetaMismatch issue.
        let report = crate::pack_validate::validate_pack_file(
            &data_file,
            LEGACY_PACK_EPOCH,
            Some(&previous_epoch),
        )
        .expect("validate after reopen");
        assert_eq!(
            report.verdict,
            crate::pack_validate::Verdict::Valid,
            "reopened pre-fork pack no longer validates: {:?}",
            report.issues
        );
    }

    /// DOOR 5 (adiri): the offline validator — `validate_pack_file` over the bare frozen bytes.
    ///
    /// Run with the previous epoch's record so the full `verify_epoch_meta` linkage executes: this
    /// is what pins the frozen meta's `start_consensus_number`, `genesis_exec_state`,
    /// `genesis_consensus` and committee key set, not just its committee layout.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_golden_legacy_pack_validates() {
        use crate::pack_validate::{validate_pack_file, Verdict};

        let dir = TempDir::with_prefix("golden_legacy_validate").expect("temp dir");
        let data_file = write_golden_legacy_data_file(dir.path());
        let previous_epoch = legacy_pack_previous_epoch(&legacy_pack_committee());

        let report = validate_pack_file(&data_file, LEGACY_PACK_EPOCH, Some(&previous_epoch))
            .expect("validate the frozen pre-fork pack");
        assert_eq!(
            report.verdict,
            Verdict::Valid,
            "the frozen pre-fork pack must validate clean: {:?}",
            report.issues
        );
        assert_eq!(report.epoch, LEGACY_PACK_EPOCH);
        assert_eq!(
            report.start_consensus_number, LEGACY_PACK_FIRST_CONSENSUS,
            "frozen start_consensus_number moved"
        );
        assert_eq!(report.consensus_count, 2, "frozen consensus record count moved");
        assert_eq!(report.batch_count, 2, "frozen batch record count moved");
        assert_eq!(report.first_consensus_number, Some(LEGACY_PACK_FIRST_CONSENSUS));
        assert_eq!(report.last_consensus_number, Some(LEGACY_PACK_LAST_CONSENSUS));
    }

    /// PIN (non-adiri): the frozen pre-fork bytes are indecodable in a build whose multi-workers
    /// gate is active from genesis, and every read door says so loudly.
    ///
    /// This documents the build-gate contract at the storage layer rather than a gap: no non-adiri
    /// network carries pre-fork packs, so mainnet's gate is active at every epoch and the legacy
    /// layout is unreadable there BY DESIGN. What matters is that it fails with an error instead of
    /// decoding into a plausible-looking committee — a silent misparse of the first record of every
    /// pack is how a node ends up verifying consensus against the wrong validator set.
    #[cfg(not(feature = "adiri"))]
    #[tokio::test]
    async fn test_golden_legacy_pack_rejected_without_adiri() {
        assert!(
            tn_types::forks::multi_workers_fork_active(LEGACY_PACK_EPOCH),
            "a non-adiri build must be post-fork at every epoch, epoch {LEGACY_PACK_EPOCH} included"
        );

        let dir = TempDir::with_prefix("golden_legacy_non_adiri").expect("temp dir");
        let data_file = write_golden_legacy_data_file(dir.path());

        // the warm-restart door: the meta fails to decode, so the open fails
        let warm = ConsensusPack::open_append_exists(dir.path(), LEGACY_PACK_EPOCH);
        assert!(
            matches!(warm, Err(super::PackError::EpochLoad(_))),
            "expected the legacy-layout meta to fail decoding, got {:?}",
            warm.map(|pack| pack.epoch())
        );

        // The offline validator reports the same failure rather than a clean pack. Anti-vacuity:
        // the failure must NOT be an open error — the frozen container (data header, framing) is
        // well formed on every lane, and only the legacy-layout record inside it is unreadable
        // here. Without that the assertion would also pass on a garbage constant.
        let validated =
            crate::pack_validate::validate_pack_file(&data_file, LEGACY_PACK_EPOCH, None);
        match validated {
            Err(super::PackError::Open(e)) => {
                panic!("the frozen pack container must open on any lane, got open error {e}")
            }
            Err(_) => {}
            Ok(report) => panic!(
                "the offline validator must reject legacy-layout bytes on a post-fork build, got \
                 {:?}",
                report.verdict
            ),
        }

        // the peer-sync door: the first streamed record fails to decode
        let source = dir.path().join("peer_stream");
        std::fs::write(&source, golden_legacy_pack_bytes()).expect("write peer stream");
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        let imported = TempDir::with_prefix("golden_legacy_non_adiri_import").expect("temp dir");
        let import = ConsensusPack::stream_import(
            imported.path(),
            stream,
            LEGACY_PACK_EPOCH,
            &EpochRecord::default(),
            LEGACY_PACK_LAST_CONSENSUS,
            Duration::from_secs(5),
        )
        .await;
        assert!(
            import.is_err(),
            "peer sync must reject legacy-layout bytes on a post-fork build, got {:?}",
            import.map(|pack| pack.epoch())
        );
    }
}
