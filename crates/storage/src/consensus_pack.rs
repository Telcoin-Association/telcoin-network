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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tn_types::{
    gas_accumulator::RewardsCounter, try_decode, AuthorityIdentifier, Batch, BlockHash,
    BlockNumHash, BlsPublicKey, CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch, EpochRecord, Hash as _,
    LegacyCommittee, Round, B256,
};
use tokio::{
    io::{AsyncRead, BufReader},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot, watch,
    },
};
use tracing::{debug, error, warn};

use crate::archive::{
    data_file::fsync_directory,
    digest_index::index::HdxIndex,
    error::{fetch::FetchError, open::OpenError},
    fxhasher::FxHasher,
    index::Index as _,
    pack::{DataHeader, Pack, PackCompression, DATA_HEADER_BYTES},
    pack_iter::{AsyncPackIter, TryDecodeRecord},
    position_index::index::{PosIndexValue, PositionIndex},
};

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

/// Pre-`rpc` [`EpochMeta`] wire layout. See [`tn_types::LegacyCommittee`].
///
/// The only field whose on-disk format changed across the v0.11.0-adiri fork is `committee`:
/// #730 added `P2pNode.rpc`, and the committee's bootstrap servers embed `P2pNode`. Swapping in
/// [`LegacyCommittee`] (rpc stripped) lets an epoch pack written before the fork decode its
/// first record; every other field is wire-identical (the #724 `BlockNumHash` →
/// `ConsensusNumHash` change for `genesis_consensus` is byte-for-byte the same: `u64` + 32-byte
/// hash).
#[derive(Deserialize)]
struct LegacyEpochMeta {
    /// The epoch this record is for.
    epoch: Epoch,
    /// The active committee for this epoch (pre-`rpc` layout).
    committee: LegacyCommittee,
    /// The first consensus block number of this epoch.
    start_consensus_number: u64,
    /// The execution genesis for this epoch.
    genesis_exec_state: BlockNumHash,
    /// The "genesis" consensus header of this epoch.
    genesis_consensus: ConsensusNumHash,
}

impl From<LegacyEpochMeta> for EpochMeta {
    fn from(legacy: LegacyEpochMeta) -> Self {
        let LegacyEpochMeta {
            epoch,
            committee,
            start_consensus_number,
            genesis_exec_state,
            genesis_consensus,
        } = legacy;
        Self {
            epoch,
            committee: committee.into(),
            start_consensus_number,
            genesis_exec_state,
            genesis_consensus,
        }
    }
}

/// Pre-`rpc` [`PackRecord`] wire layout, used only to decode the first (`EpochMeta`) record of
/// an epoch pack written before the fork.
///
/// The variant order matches [`PackRecord`] so the BCS enum discriminant lines up. Only the
/// `EpochMeta` variant differs (it carries a [`LegacyEpochMeta`]); `Batch`/`Consensus` reuse the
/// current, wire-stable types and exist only to keep the discriminant indexes aligned — we only
/// ever read the first record, which is always `EpochMeta` (index 0), so their payloads are
/// decoded-but-never-inspected, hence `allow(dead_code)`.
#[derive(Deserialize)]
#[allow(dead_code)]
enum LegacyPackRecord {
    EpochMeta(LegacyEpochMeta),
    Batch(Batch),
    Consensus(Box<ConsensusHeader>),
}

impl LegacyPackRecord {
    fn into_epoch(self) -> Result<EpochMeta, PackError> {
        if let Self::EpochMeta(epoch) = self {
            Ok(epoch.into())
        } else {
            Err(PackError::NotEpoch)
        }
    }
}

/// The only [`TryDecodeRecord`] override: try the current format, then fall back to the pre-fork
/// (`P2pNode.rpc`-less) layout. The fallback is the single decode chokepoint shared by every
/// reader — sync `PackIter`, async `AsyncPackIter` (full and partial peer streams), and
/// random-access `Pack::fetch` — so historic imports and live p2p streams of a pre-fork epoch
/// behave identically.
impl TryDecodeRecord for PackRecord {
    fn try_decode_record(bytes: &[u8]) -> Result<Self, FetchError> {
        if let Ok(rec) = try_decode::<PackRecord>(bytes) {
            return Ok(rec);
        }
        // Only the first (`EpochMeta`) record of a pre-fork pack carries the rpc-stripped
        // committee; `Batch`/`Consensus` records are wire-stable, so a genuine decode failure on
        // those falls through here and fails the legacy decode too — still fatal. `bytes` is
        // already CRC-verified, so this can never mask corruption (that errors earlier).
        let meta = try_decode::<LegacyPackRecord>(bytes)
            .map_err(|e| FetchError::DeserializeValue(e.to_string()))?
            .into_epoch()
            .map_err(|e| FetchError::DeserializeValue(e.to_string()))?;
        warn!(
            target: "consensus_pack",
            epoch = meta.epoch,
            "decoded pre-upgrade epoch meta via legacy compat; rpc stripped"
        );
        Ok(PackRecord::EpochMeta(meta))
    }
}

enum PackMessage {
    ConsensusOutput(ConsensusOutput),
    ContainsConsensusHeaderNumber(u64, oneshot::Sender<bool>),
    ContainsConsensusHeader(ConsensusHeaderDigest, oneshot::Sender<bool>),
    ConsensusHeader(ConsensusHeaderDigest, oneshot::Sender<Option<ConsensusHeader>>),
    ConsensusHeaderNumber(u64, oneshot::Sender<Result<ConsensusHeader, PackError>>),
    Persist(oneshot::Sender<Result<(), PackError>>),
    BytesForConsensus(u64, oneshot::Sender<Result<Vec<u8>, PackError>>),
    ReadLastCommitted(oneshot::Sender<Result<HashMap<AuthorityIdentifier, Round>, PackError>>),
    ReadLatestFinalRep(oneshot::Sender<Result<Option<CommittedSubDag>, PackError>>),
    ContainsBatch(B256, oneshot::Sender<bool>),
    Batch(B256, oneshot::Sender<Option<Batch>>),
    CountLeaders(Round, RewardsCounter, oneshot::Sender<Result<(), PackError>>),
    LatestConsensusHeader(oneshot::Sender<Option<ConsensusHeader>>),
    Shutdown,
}

/// Manage a single pack file of consensus data (typically one epoch os the consensus chain).
#[derive(Debug, Clone)]
pub struct ConsensusPack {
    tx: Sender<PackMessage>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    error: watch::Receiver<Option<PackError>>,
    epoch: Epoch,
    committee: Committee,
    compression: PackCompression,
}

fn run_pack_loop(
    mut inner: Inner,
    mut rx: Receiver<PackMessage>,
    tx_error: watch::Sender<Option<PackError>>,
) {
    // When this returns None then the channel is consumed and closed, so exit the thread.
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            PackMessage::ConsensusOutput(output) => {
                if let Err(e) = inner.save_consensus_output(&output) {
                    tx_error.send_replace(Some(e));
                }
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

/// Per-record read timeout used when [`ConsensusPack::rewrite_legacy_epoch`] streams an old pack
/// through `stream_import`. Reads come from a local file, so this only guards against a truncated
/// or wedged record, not a slow peer.
const REWRITE_RECORD_TIMEOUT: Duration = Duration::from_secs(30);

/// Outcome of [`ConsensusPack::rewrite_legacy_epoch`] for a single epoch.
#[derive(Debug, Clone)]
pub enum RewriteOutcome {
    /// The pack's first record already decoded under the current format; nothing was changed.
    AlreadyCurrent,
    /// The pack was rewritten into the current serialization.
    Migrated {
        /// Number of consensus-header (output) records carried over.
        records: usize,
        /// `data` file length before the rewrite.
        size_before: u64,
        /// `data` file length after the rewrite.
        size_after: u64,
        /// Path the original epoch directory was preserved at.
        backup: PathBuf,
    },
}

impl ConsensusPack {
    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    pub fn open_append<P: Into<PathBuf>>(
        path: P,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let epoch = committee.epoch();
        let inner = Inner::open_append(path.clone(), &previous_epoch, committee.clone())?;
        let compression = inner.data.header().compression();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx, tx_error));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            error,
            epoch,
            committee,
            compression,
        })
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    pub fn open_append_exists<P: Into<PathBuf>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let inner = Inner::open_append_exists(path.clone(), epoch)?;
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx, tx_error));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            error,
            epoch,
            committee,
            compression,
        })
    }

    /// Filesystem path to the epoch pack's primary `data` file for `epoch` under `path`.
    ///
    /// Lets a caller tell a genuinely fresh epoch (no pack on disk yet) apart from a pack
    /// that exists but fails to open, so the latter can fail loudly instead of being
    /// silently dropped to `None` and resurfacing later as a misleading `NoCurrentEpoch`.
    pub fn epoch_pack_path<P: AsRef<Path>>(path: P, epoch: Epoch) -> PathBuf {
        path.as_ref().join(format!("epoch-{epoch}")).join(DATA_NAME)
    }

    /// Open up the static files for previous epoch.  These will be read only.
    /// Note, you should call persist() on the returned pack to make sure it
    /// opened cleanly.
    pub fn open_static<P: Into<PathBuf>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let inner = Inner::open_static(path.clone(), epoch)?;
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || run_pack_loop(inner, rx, tx_error));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            error,
            epoch,
            committee,
            compression,
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
        let (tx_error, error) = watch::channel(None);
        let inner = Inner::stream_import(
            path,
            stream,
            epoch,
            previous_epoch,
            final_consensus_number,
            timeout,
        )
        .await?;
        let compression = inner.data.header().compression();
        let committee = inner.epoch_meta.committee.clone();
        let handle = std::thread::spawn(move || {
            run_pack_loop(inner, rx, tx_error);
        });
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            error,
            epoch,
            committee,
            compression,
        })
    }

    /// Return the epoch for this pack file.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Return a delayed error value.
    /// Work is sent to a background thread and any errors are recorded.
    pub fn get_error(&self) -> Result<(), PackError> {
        match &*self.error.borrow() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    /// This backgrounds the save, will return any previous error from a past operation.
    pub async fn save_consensus_output(&self, consensus: ConsensusOutput) -> Result<(), PackError> {
        self.get_error()?;
        if self.tx.send(PackMessage::ConsensusOutput(consensus)).await.is_err() {
            Err(PackError::SendFailed)
        } else {
            Ok(())
        }
    }

    /// Load and return the consensus output form this epoch.
    pub async fn get_consensus_output(&self, number: u64) -> Result<ConsensusOutput, PackError> {
        self.get_error()?;
        let (tx, rx) = oneshot::channel();
        let bytes = if self.tx.send(PackMessage::BytesForConsensus(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)??
        } else {
            return Err(PackError::SendFailed);
        };
        let cursor = Cursor::new(bytes);
        let reader = BufReader::new(cursor);
        bytes_to_output(reader, self.compression, Duration::from_secs(5), &self.committee).await
    }

    /// True if consensus header by digest is found by digest.
    pub async fn contains_consensus_header_number(&self, number: u64) -> Result<bool, PackError> {
        self.get_error()?;
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
        self.get_error()?;
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Persist(tx)).await;
        rx.await.map_err(|_| match &*self.error.borrow() {
            Some(e) => e.clone(),
            None => PackError::ReceiveFailed,
        })?
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

    /// True if the epoch-`epoch` pack under `epochs_dir` still uses the pre-fork (legacy)
    /// `EpochMeta` serialization and therefore needs rewriting.
    ///
    /// Read-only and cheap: opens only the `data` file and probes its first record under the
    /// current format. Used for the `--dry-run` report and as the rewrite's idempotency guard.
    pub fn epoch_pack_needs_rewrite<P: AsRef<Path>>(
        epochs_dir: P,
        epoch: Epoch,
    ) -> Result<bool, PackError> {
        let mut data = Pack::<PackRecord>::open(
            Self::epoch_pack_path(epochs_dir.as_ref(), epoch),
            epoch as u64,
            true,
            PackCompression::ZStd,
        )?;
        Ok(!Self::first_record_is_current(&mut data)?)
    }

    /// True if the pack's first `EpochMeta` record decodes under the *current* `PackRecord` format
    /// with no legacy fallback. Reads the raw bytes then attempts only the current decode
    /// (deliberately not `try_decode_record`, whose fallback would mask a legacy record).
    fn first_record_is_current(data: &mut Pack<PackRecord>) -> Result<bool, PackError> {
        let bytes = data
            .fetch_raw(DATA_HEADER_BYTES as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?;
        Ok(try_decode::<PackRecord>(&bytes).is_ok())
    }

    /// Count `Consensus` (output) records in a pack by iterating the data file. The first record
    /// (`EpochMeta`) decodes via the compat fallback when legacy; `Batch`/`Consensus` records are
    /// wire-stable so they decode directly.
    fn count_consensus_records(data: &Pack<PackRecord>) -> Result<usize, PackError> {
        let mut count = 0_usize;
        for rec in data.raw_iter().map_err(|e| PackError::ReadError(e.to_string()))? {
            if let PackRecord::Consensus(_) =
                rec.map_err(|e| PackError::ReadError(e.to_string()))?
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Rewrite a pre-fork (legacy) epoch pack into the current serialization, in place.
    ///
    /// Pipes the old `data` file through [`stream_import`](Self::stream_import) — the exact path a
    /// peer uses — so a migrated pack is byte-for-byte what a fresh sync would produce: the legacy
    /// `EpochMeta` is decoded via the compat fallback and re-appended in the current format
    /// (adding the rpc `Option` byte), every output is rebuilt and re-indexed, and the in-pack
    /// consensus chain is verified.
    ///
    /// `epochs_dir` holds the `epoch-{N}` subdirs (`consensus-db/epochs`). `tmp_dir` is scratch
    /// space **on the same filesystem** as `epochs_dir` so the final swap is an atomic rename: the
    /// rewrite is staged at `tmp_dir/epoch-{N}`, verified, then swapped in, and the original is
    /// preserved at `epochs_dir/epoch-{N}.bak-<unix-secs>`.
    ///
    /// Idempotent — a pack already in the current format returns [`RewriteOutcome::AlreadyCurrent`]
    /// without touching disk. No `EpochRecordDb` is needed: the verification inputs are synthesised
    /// from the pack's own `EpochMeta`, and the real integrity check is the in-pack consensus chain
    /// that `stream_import` validates (parent links, monotonic numbers, batch references).
    pub async fn rewrite_legacy_epoch<P1: AsRef<Path>, P2: AsRef<Path>>(
        epochs_dir: P1,
        epoch: Epoch,
        tmp_dir: P2,
        force: bool,
    ) -> Result<RewriteOutcome, PackError> {
        let epochs_dir = epochs_dir.as_ref();
        let tmp_dir = tmp_dir.as_ref();
        let epoch_dir = epochs_dir.join(format!("epoch-{epoch}"));
        let pack_path = Self::epoch_pack_path(epochs_dir, epoch);

        // Probe + read meta (tolerant) + count outputs, reading only the old `data` file. The old
        // indexes are never read — the rewrite rebuilds them from the record stream — so they get
        // carried into the backup untouched.
        let (epoch_meta, output_count, size_before) = {
            let mut old =
                Pack::<PackRecord>::open(&pack_path, epoch as u64, true, PackCompression::ZStd)?;
            if !force && Self::first_record_is_current(&mut old)? {
                return Ok(RewriteOutcome::AlreadyCurrent);
            }
            let raw = old
                .fetch_raw(DATA_HEADER_BYTES as u64)
                .map_err(|e| PackError::EpochLoad(e.to_string()))?;
            let epoch_meta = PackRecord::try_decode_record(&raw)
                .map_err(|e| PackError::EpochLoad(e.to_string()))?
                .into_epoch()?;
            let output_count = Self::count_consensus_records(&old)?;
            (epoch_meta, output_count, old.file_len())
        };

        // Synthesise the previous epoch from this pack's own EpochMeta so stream_import's
        // verify_epoch_meta holds without an EpochRecordDb (final_state / final_consensus / next
        // committee all derive from the meta).
        let previous_epoch = EpochRecord {
            epoch: epoch.saturating_sub(1),
            final_state: epoch_meta.genesis_exec_state,
            final_consensus: epoch_meta.genesis_consensus,
            next_committee: epoch_meta.committee.bls_keys().into_iter().collect(),
            ..Default::default()
        };
        let final_consensus_number =
            epoch_meta.start_consensus_number.saturating_add(output_count as u64).saturating_sub(1);

        // Stream the old `data` file into a fresh, current-format pack at tmp_dir/epoch-{N}.
        let tmp_epoch_dir = tmp_dir.join(format!("epoch-{epoch}"));
        let _ = std::fs::remove_dir_all(&tmp_epoch_dir); // drop any stale partial from a prior run
        std::fs::create_dir_all(tmp_dir)?;
        let size_after = {
            let stream = tokio::fs::File::open(&pack_path).await?;
            let pack = Self::stream_import(
                tmp_dir,
                stream,
                epoch,
                &previous_epoch,
                final_consensus_number,
                REWRITE_RECORD_TIMEOUT,
            )
            .await?;
            pack.persist().await?;
            drop(pack); // join the writer thread so the staged files are durable before we verify
                        // Verify the staged pack BEFORE the destructive swap: it must open consistently, its
                        // first record must decode under the current path (no fallback), and it must carry the
                        // same number of outputs as the original.
            Self::verify_rewritten(tmp_dir, epoch, output_count)?
        };

        // Atomic swap with a timestamped backup of the original epoch dir.
        let stamp =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default();
        let backup = epochs_dir.join(format!("epoch-{epoch}.bak-{stamp}"));
        std::fs::rename(&epoch_dir, &backup)?;
        if let Err(e) = std::fs::rename(&tmp_epoch_dir, &epoch_dir) {
            // Restore so we never leave the operator without an epoch dir.
            let _ = std::fs::rename(&backup, &epoch_dir);
            return Err(e.into());
        }
        fsync_directory(epochs_dir)?;

        // Confirm the swapped-in pack opens cleanly at its real location.
        drop(Self::open_static(epochs_dir, epoch)?);

        Ok(RewriteOutcome::Migrated { records: output_count, size_before, size_after, backup })
    }

    /// Open the staged/migrated pack read-only and assert it is sound: `open_static` runs
    /// `files_consistent` (indexes match the data file), the first record decodes under the
    /// current path with no fallback, and the output count matches `expected_outputs`. Returns the
    /// rewritten `data` file length.
    fn verify_rewritten<P: AsRef<Path>>(
        dir: P,
        epoch: Epoch,
        expected_outputs: usize,
    ) -> Result<u64, PackError> {
        drop(Self::open_static(dir.as_ref(), epoch)?);

        let mut data = Pack::<PackRecord>::open(
            Self::epoch_pack_path(dir.as_ref(), epoch),
            epoch as u64,
            true,
            PackCompression::ZStd,
        )?;
        if !Self::first_record_is_current(&mut data)? {
            return Err(PackError::EpochLoad(
                "rewritten pack first record does not decode under the current format".to_string(),
            ));
        }
        let output_count = Self::count_consensus_records(&data)?;
        if output_count != expected_outputs {
            return Err(PackError::InvalidConsensusNumber(
                expected_outputs as u64,
                output_count as u64,
            ));
        }
        Ok(data.file_len())
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
            let last_record = match consensus_pos_idx.load(consensus_pos_idx.len() as u64 - 1) {
                Ok(p) => p.consensus_header,
                Err(_) => return false,
            };
            let mut iter = match data.raw_iter() {
                Ok(i) => i,
                Err(_) => return false,
            };
            if iter.set_position(last_record).is_err() {
                return false;
            }
            match (iter.next(), iter.next()) {
                (Some(_), None) => iter.position().unwrap_or_default() == pack_len,
                _ => false,
            }
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
                    let record_size_res = data.record_size(last_record.consensus_header);
                    let record_valid = record_size_res.is_ok();
                    if record_valid {
                        if idx != start_idx {
                            // Keep the bytes index in sync with the consensus index so the two
                            // never diverge in length after healing a damaged pack.
                            consensus_pos_idx.truncate_to_index(idx)?;
                        }
                        new_pack_len = last_record.consensus_header
                            + record_size_res.unwrap_or_default() as u64;
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

    /// Open a PDX index file and return the open index.
    /// This will handle an update of the index on older testnet epochs.
    fn open_pdx_file_with_update<P: AsRef<Path>, T: PosIndexValue>(
        dir: P,
        read_only: bool,
        data: &mut Pack<PackRecord>,
    ) -> Result<PositionIndex<T>, PackError> {
        let base_dir = dir.as_ref().join(Self::CONSENSUS_POS_NAME);
        if PositionIndex::<T>::pdx_file_exists(&base_dir, "index.pdx")
            && !PositionIndex::<T>::pdx_file_exists(&base_dir, "index_pos.pdx")
        {
            warn!(target: "consensus_pack", "Found old but not new position index, updating");
            // We have an old index but, need to create a new one and build it.
            // This code should only effect OG testnet nodes and should not need
            // to be maintained forever.
            let mut old_idx: PositionIndex<u64> =
                PositionIndex::open_pdx_file(&base_dir, data.header(), "index.pdx", false)
                    .map_err(OpenError::IndexFileOpen)?;
            let _ = std::fs::remove_file(base_dir.join("index_pos.pdx.tmp"));
            let mut new_idx: PositionIndex<IndexPositions> =
                PositionIndex::open_pdx_file(&base_dir, data.header(), "index_pos.pdx.tmp", false)
                    .map_err(OpenError::IndexFileOpen)?;
            let mut end = 0;
            for i in 0..old_idx.len() {
                let idx = i as u64;
                let start = if idx > 0 {
                    end
                } else {
                    DATA_HEADER_BYTES as u64 + data.record_size(DATA_HEADER_BYTES as u64)? as u64
                };
                let Ok(pos) = old_idx.load(idx) else {
                    break;
                };
                let Ok(record_size) = data.record_size(pos) else {
                    break;
                };
                end = pos + record_size as u64;
                new_idx
                    .save(idx, IndexPositions::new(pos, start, end))
                    .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
            }
            drop(new_idx);
            drop(old_idx);
            std::fs::rename(base_dir.join("index_pos.pdx.tmp"), base_dir.join("index_pos.pdx"))?;
            fsync_directory(&base_dir)?;
            let _ = std::fs::remove_file(base_dir.join("index.pdx"));
        }
        let consensus_pos_idx =
            PositionIndex::open_pdx_file(&base_dir, data.header(), "index_pos.pdx", read_only)
                .map_err(OpenError::IndexFileOpen)?;
        Ok(consensus_pos_idx)
    }

    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    fn open_append<P: AsRef<Path>>(
        path: P,
        previous_epoch: &EpochRecord,
        committee: Committee,
    ) -> Result<Self, PackError> {
        let epoch = committee.epoch();
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = std::fs::create_dir_all(&base_dir);
        let pack_file = base_dir.join(Self::DATA_NAME);
        let have_pack = std::fs::exists(&pack_file).unwrap_or_default();
        let mut data: Pack<PackRecord> =
            Pack::open(&pack_file, epoch as u64, false, PackCompression::ZStd)?;
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
        let mut consensus_pos_idx = Self::open_pdx_file_with_update(&base_dir, false, &mut data)?;
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

    /// Read and decode the `EpochMeta` first record of an epoch pack.
    ///
    /// Tolerance for packs written before the v0.11.0-adiri fork now lives in
    /// [`PackRecord::try_decode_record`], which `data.fetch` routes through: it tries the current
    /// format, then falls back to the rpc-stripped [`LegacyPackRecord`] layout for the one
    /// pre-fork record that needs it (defaulting `rpc` to `None`). IO / CRC / size errors stay
    /// fatal, and a record that decodes as neither layout is fatal too — preserving the
    /// loud-failure contract that surfaces real corruption at startup. The on-disk record is
    /// never rewritten; new appends use the current format.
    fn fetch_epoch_meta(data: &mut Pack<PackRecord>) -> Result<EpochMeta, PackError> {
        data.fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?
            .into_epoch()
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    fn open_append_exists<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));

        let mut data = Pack::<PackRecord>::open(
            base_dir.join(Self::DATA_NAME),
            epoch as u64,
            false,
            PackCompression::ZStd,
        )?;
        let epoch_meta = Self::fetch_epoch_meta(&mut data)?;
        let mut consensus_pos_idx = Self::open_pdx_file_with_update(&base_dir, false, &mut data)?;
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
        )?;
        let epoch_meta = Self::fetch_epoch_meta(&mut data)?;
        let mut consensus_pos_idx = Self::open_pdx_file_with_update(&base_dir, true, &mut data)?;
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

    /// Verify a streamed EpochMeta record is valid.
    fn verify_epoch_meta(
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
        let committee: BTreeSet<BlsPublicKey> =
            previous_epoch.next_committee.iter().copied().collect();
        if epoch_meta.committee.bls_keys() != committee {
            return Err(PackError::InvalidEpoch(
                epoch,
                "epoch meta has unexpected committee".to_string(),
            ));
        }
        Ok(())
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
        /// Private helper to read the next record from a pack iterator or timeout if it takes
        /// longer than timeout.
        async fn next<R: AsyncRead + Unpin>(
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
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = std::fs::create_dir_all(&base_dir);
        let mut stream_iter = AsyncPackIter::<PackRecord, R>::open(stream, epoch as u64)
            .await
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let mut data =
            Pack::open(base_dir.join(Self::DATA_NAME), epoch as u64, false, PackCompression::ZStd)?;
        let epoch_meta = if let Some(meta) = next(&mut stream_iter, timeout).await? {
            meta.into_epoch()?
        } else {
            return Err(PackError::NotEpoch);
        };
        Self::verify_epoch_meta(epoch, previous_epoch, &epoch_meta)?;
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
            let output =
                match iter_to_output(&mut stream_iter, timeout, &pack.epoch_meta.committee).await {
                    Ok(output) => output,
                    Err(PackError::NotConsensus) => break,
                    Err(e) => return Err(e),
                };
            if output.parent_hash() != parent_digest {
                return Err(PackError::InvalidConsensusChain);
            }
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

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    fn save_consensus_output(&mut self, consensus: &ConsensusOutput) -> Result<(), PackError> {
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
            return Ok(());
        } else if consensus_idx as usize != self.consensus_pos_idx.len() {
            return Err(PackError::InvalidConsensusNumber(
                self.consensus_pos_idx.len() as u64 + self.epoch_meta.start_consensus_number,
                consensus_number,
            ));
        }
        let mut batches = BTreeMap::new();
        // We want to make sure batches are saved to the pack in a deterministic order, so
        // collect them in a BTreeMap.  We probably don't actually need this but this
        // means we do not impose any extra restrictions on consensus output.
        for cert_batch in consensus.batches() {
            for batch in &cert_batch.batches {
                let digest = batch.digest();
                // Should not have duplicate batches across output.
                // Will work if we do but will save batches more than once in a pack.
                batches.insert(digest, batch.clone());
            }
        }
        let mut first_batch_pos = None;
        // Save all the required batcdhes into the pack file.
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
        // Now save the consensus header.
        let consensus_digest = consensus.consensus_header_hash();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
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

        Ok(())
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

/// Upper bound on how many `Batch` records `iter_to_output` will buffer before the
/// terminating `Consensus` record.  This caps memory use when reading a (possibly hostile)
/// peer stream; a legitimate ConsensusOutput references far fewer batches than this.
#[cfg(not(test))]
const MAX_BATCHES_PER_OUTPUT: usize = 1_000;
/// Lowered in tests so the cap can be exercised cheaply; legitimate test outputs use only a
/// handful of batches, well under this.
#[cfg(test)]
const MAX_BATCHES_PER_OUTPUT: usize = 50;

/// Take an async stream of bytes that in pack file representation of ConsensusOutput and return the
/// ConsensusOutput.
pub async fn bytes_to_output<R: AsyncRead + Unpin>(
    stream: R,
    compression: PackCompression,
    timeout: Duration,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    let mut stream_iter = AsyncPackIter::<PackRecord, R>::open_partial(stream, compression)
        .await
        .map_err(|e| PackError::ReadError(e.to_string()))?;
    iter_to_output(&mut stream_iter, timeout, committee).await
}

/// Take an iter over PackRecords that represent a ConsensusOutput and return the ConsensusOutput.
async fn iter_to_output<R: AsyncRead + Unpin>(
    stream_iter: &mut AsyncPackIter<PackRecord, R>,
    timeout: Duration,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    /// Private helper to read the next record from a pack iterator or timeout if it takes
    /// longer than timeout.
    async fn next<R: AsyncRead + Unpin>(
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
    let mut header = None;
    let mut available_batches = HashMap::new();
    let mut referenced_batches = HashSet::new();
    let mut batch_records = 0_usize;
    while let Some(record) = next(stream_iter, timeout).await? {
        match record {
            PackRecord::EpochMeta(_epoch_meta) => {
                return Err(PackError::EpochLoad("unexpected epoch meta data found".to_string()))
            }
            PackRecord::Batch(batch) => {
                // Bound how many batch records a (possibly hostile) stream can deliver before the
                // terminating Consensus record arrives.  Without this an `EpochMeta`/`Consensus`
                // -less flood of Batch records would grow `available_batches` until OOM; the
                // per-record size cap (MAX_RECORD_SIZE) only bounds individual records.  A
                // legitimate ConsensusOutput references far fewer batches than this.
                batch_records += 1;
                if batch_records > MAX_BATCHES_PER_OUTPUT {
                    return Err(PackError::TooManyBatches(MAX_BATCHES_PER_OUTPUT));
                }
                let batch_digest = batch.digest();
                available_batches.insert(batch_digest, batch);
            }
            PackRecord::Consensus(consensus_header) => {
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
        path::Path,
        sync::Arc,
        time::Duration,
    };

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        test_genesis, BlockHash, Certificate, CertifiedBatch, CommittedSubDag, Committee,
        ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, EpochRecord, Hash, HeaderBuilder,
        ReputationScores,
    };

    use crate::{
        archive::{
            index::Index as _,
            pack::{DataHeader, PackCompression},
            pack_iter::TryDecodeRecord,
            position_index::index::PositionIndex,
        },
        consensus_pack::{ConsensusPack, IndexPositions, Inner},
        mem_db::MemDatabase,
    };

    // A CRC-valid-but-undecodable first record is built with `Pack<u8>` (see the undecodable
    // fatal test); a single raw byte is enough to exercise the "decodes as neither layout" path.
    impl TryDecodeRecord for u8 {}

    /// Pre-fork (`#730`-less) wire mirrors plus a builder, shared by the legacy-decode tests.
    ///
    /// Field order MUST match the historical structs so the BCS bytes are exactly what a pre-fork
    /// binary wrote: `P2pNode` without `rpc`, `CommitteeInner`'s serialized fields (`authorities`,
    /// `epoch`, `bootstrap_servers`), `EpochMeta`, and `PackRecord` (EpochMeta at variant index 0).
    pub(crate) mod legacy_wire {
        use std::collections::BTreeMap;

        use serde::{Deserialize, Serialize};
        use tn_types::{
            encode, Authority, BlockNumHash, BlsPublicKey, Committee, ConsensusNumHash, Epoch,
            Multiaddr, NetworkPublicKey,
        };

        use crate::archive::pack_iter::TryDecodeRecord;

        // Fields are private (the tests only build these via `wire_epoch_meta` and use them as
        // opaque records to encode / append); the derives serialize private fields fine.
        #[derive(Debug, Serialize, Deserialize)]
        pub(super) struct WireP2pNode {
            network_address: Multiaddr,
            network_key: NetworkPublicKey,
        }
        #[derive(Debug, Serialize, Deserialize)]
        pub(super) struct WireBootstrap {
            primary: WireP2pNode,
            worker: WireP2pNode,
        }
        #[derive(Debug, Serialize, Deserialize)]
        pub(super) struct WireCommittee {
            authorities: BTreeMap<BlsPublicKey, Authority>,
            epoch: Epoch,
            bootstrap_servers: BTreeMap<BlsPublicKey, WireBootstrap>,
        }
        #[derive(Debug, Serialize, Deserialize)]
        pub(super) struct WireEpochMeta {
            epoch: Epoch,
            committee: WireCommittee,
            start_consensus_number: u64,
            genesis_exec_state: BlockNumHash,
            genesis_consensus: ConsensusNumHash,
        }
        #[derive(Debug, Serialize, Deserialize)]
        pub(super) enum WirePackRecord {
            EpochMeta(WireEpochMeta),
        }
        impl TryDecodeRecord for WirePackRecord {}

        /// Re-encode `committee` in the pre-`rpc` layout (drop every `P2pNode.rpc`), wrapped as the
        /// `EpochMeta` first record of a pack.
        pub(super) fn wire_epoch_meta(
            committee: &Committee,
            start_consensus_number: u64,
            genesis_exec_state: BlockNumHash,
            genesis_consensus: ConsensusNumHash,
        ) -> WirePackRecord {
            let epoch = committee.epoch();
            let authorities: BTreeMap<BlsPublicKey, Authority> =
                committee.authorities().into_iter().map(|a| (*a.protocol_key(), a)).collect();
            let bootstrap_servers: BTreeMap<BlsPublicKey, WireBootstrap> = committee
                .bootstrap_servers()
                .into_iter()
                .map(|(key, server)| {
                    (
                        key,
                        WireBootstrap {
                            primary: WireP2pNode {
                                network_address: server.primary.network_address,
                                network_key: server.primary.network_key,
                            },
                            worker: WireP2pNode {
                                network_address: server.worker.network_address,
                                network_key: server.worker.network_key,
                            },
                        },
                    )
                })
                .collect();
            WirePackRecord::EpochMeta(WireEpochMeta {
                epoch,
                committee: WireCommittee { authorities, epoch, bootstrap_servers },
                start_consensus_number,
                genesis_exec_state,
                genesis_consensus,
            })
        }

        /// BCS bytes of the legacy `EpochMeta` first record for `committee`.
        pub(super) fn wire_epoch_meta_bytes(
            committee: &Committee,
            start_consensus_number: u64,
            genesis_exec_state: BlockNumHash,
            genesis_consensus: ConsensusNumHash,
        ) -> Vec<u8> {
            encode(&wire_epoch_meta(
                committee,
                start_consensus_number,
                genesis_exec_state,
                genesis_consensus,
            ))
        }
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
        pack.save_consensus_output(wrong).await.expect("queued to pack thread");

        // The rejection is delivered asynchronously via the watch channel; poll for it.
        let mut err = None;
        for _ in 0..100 {
            if let Err(e) = pack.get_error() {
                err = Some(e);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            matches!(err, Some(super::PackError::InvalidEpoch(..))),
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
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
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
            tokio::time::sleep(Duration::from_secs(2)).await;
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

            let mut f1 = File::open(temp_dir.path().join("epoch-0")).expect("log file");
            let mut f2 = File::open(temp_dir2.path().join("epoch-0")).expect("log file");
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

    /// Convert a pack directory's position index into the legacy format written by older
    /// nodes: an "index.pdx" of u64 consensus header positions and no "index_pos.pdx".
    /// The pack data file is identical between the two formats so this faithfully
    /// recreates an old pack directory for migration testing.
    fn make_legacy_index(epoch_dir: &Path) {
        let idx_dir = epoch_dir.join("idx");
        let header = DataHeader::new(0, PackCompression::ZStd);
        let mut new_idx: PositionIndex<IndexPositions> =
            PositionIndex::open_pdx_file(&idx_dir, &header, "index_pos.pdx", true)
                .expect("open new index");
        let mut old_idx: PositionIndex<u64> =
            PositionIndex::open_pdx_file(&idx_dir, &header, "index.pdx", false)
                .expect("open legacy index");
        assert!(old_idx.is_empty(), "legacy index must start empty");
        for i in 0..new_idx.len() as u64 {
            let positions = new_idx.load(i).expect("new index entry");
            old_idx.save(i, positions.consensus_header).expect("save legacy entry");
        }
        old_idx.sync().expect("sync legacy index");
        drop(old_idx);
        drop(new_idx);
        std::fs::remove_file(idx_dir.join("index_pos.pdx")).expect("remove new index");
    }

    /// Test the one time migration of a legacy position index (consensus header positions
    /// only) to the new index containing consensus output byte ranges.  Covers the append
    /// path, the read only static path and recovery from a stale tmp file left by a crash
    /// between the tmp write and the rename.
    #[tokio::test]
    async fn test_consensus_pack_index_migration() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack_migration").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let idx_dir = epoch_dir.join("idx");

        // Build a pack with the current format.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        let num_outputs = 10;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), i as u64 + 1, parent);
            parent = output.digest().into();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.unwrap();
        }
        pack.persist().await.expect("persist");
        drop(pack);

        // Migrate on the append path.  Reading the first output verifies the migrated
        // byte range starts after the EpochMeta record.
        make_legacy_index(&epoch_dir);
        assert!(PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index.pdx"));
        assert!(!PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index_pos.pdx"));
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open legacy pack for append");
        assert!(
            PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index_pos.pdx"),
            "migration creates the new index"
        );
        assert!(
            !PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index.pdx"),
            "migration removes the old index"
        );
        for (i, output) in outputs.iter().enumerate() {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("output {} after append migration", i + 1));
            compare_outputs(&output_db, output);
        }
        // The pack keeps working for new appends after migration.
        let output = make_test_output(&committee, 0, chain.clone(), num_outputs as u64 + 1, parent);
        outputs.push(output.clone());
        pack.save_consensus_output(output).await.unwrap();
        let output_db = pack
            .get_consensus_output(num_outputs as u64 + 1)
            .await
            .expect("appended output after migration");
        compare_outputs(&output_db, outputs.last().unwrap());
        pack.persist().await.expect("persist");
        drop(pack);

        // Migrate on the read only static path.
        make_legacy_index(&epoch_dir);
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open legacy pack static");
        for (i, output) in outputs.iter().enumerate() {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("output {} after static migration", i + 1));
            compare_outputs(&output_db, output);
        }
        drop(pack);

        // Migrate with a stale partial tmp file left by a "crash" between the tmp write
        // and the rename.  The migration must discard it and rebuild.
        make_legacy_index(&epoch_dir);
        {
            let header = DataHeader::new(0, PackCompression::ZStd);
            let mut stale: PositionIndex<IndexPositions> =
                PositionIndex::open_pdx_file(&idx_dir, &header, "index_pos.pdx.tmp", false)
                    .expect("open stale tmp");
            stale.save(0, IndexPositions::new(1, 1, 1)).expect("save stale entry");
            stale.sync().expect("sync stale tmp");
        }
        assert!(PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index_pos.pdx.tmp"));
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open legacy pack with stale tmp");
        assert!(
            !PositionIndex::<u64>::pdx_file_exists(&idx_dir, "index_pos.pdx.tmp"),
            "stale tmp removed by migration"
        );
        for (i, output) in outputs.iter().enumerate() {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .expect(&format!("output {} after stale tmp migration", i + 1));
            compare_outputs(&output_db, output);
        }
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
            consensus_pack::{bytes_to_output, PackError, PackRecord, MAX_BATCHES_PER_OUTPUT},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_batch_cap").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // Build a record stream of more batch records than the cap with no Consensus record.
        let path = temp_dir.path().join("batch_only");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd).expect("open pack");
            let batch = tn_reth::test_utils::batches(chain.clone(), 1).pop().expect("one batch");
            for _ in 0..(MAX_BATCHES_PER_OUTPUT + 5) {
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
        assert!(matches!(res, Err(PackError::TooManyBatches(_))), "expected TooManyBatches");
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

    /// CP4 (the v0.11.0-adiri startup blocker): an epoch pack whose first `EpochMeta` record was
    /// written before #730 added `P2pNode.rpc` must still open. Its committee's bootstrap-server
    /// `P2pNode`s lack the trailing rpc Option byte, so the current decoder fails with "expected
    /// option type"; `open_append_exists` must fall back to the legacy layout and default `rpc`
    /// to `None`, preserving the committee.
    #[tokio::test]
    async fn test_open_append_exists_decodes_pre_rpc_epoch_pack() {
        use crate::archive::pack::Pack;
        use tn_types::{encode, try_decode, BlockNumHash, ConsensusNumHash};

        let temp_dir = TempDir::with_prefix("test_cp_pre_rpc").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let epoch = committee.epoch();

        // Re-encode the fixture committee in the pre-`rpc` layout (drops every `P2pNode.rpc`).
        let wire = legacy_wire::wire_epoch_meta(
            &committee,
            1,
            BlockNumHash::default(),
            ConsensusNumHash::default(),
        );

        // These bytes decode ONLY via the legacy layout — proving the compat path is exercised and
        // not a fluke of the fixture happening to be current-format compatible. (If the committee
        // carried no bootstrap `P2pNode`s the two layouts would coincide and the current decode
        // would succeed, failing the first assertion.)
        let bcs = encode(&wire);
        assert!(
            try_decode::<super::PackRecord>(&bcs).is_err(),
            "pre-rpc bytes must NOT decode as the current PackRecord",
        );
        assert!(
            try_decode::<super::LegacyPackRecord>(&bcs).is_ok(),
            "pre-rpc bytes must decode as the legacy PackRecord",
        );

        // Write the legacy record as the first (EpochMeta) record of an epoch pack on disk.
        let epoch_dir = temp_dir.path().join(format!("epoch-{epoch}"));
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        {
            let mut pack: Pack<legacy_wire::WirePackRecord> = Pack::open(
                epoch_dir.join(Inner::DATA_NAME),
                epoch as u64,
                false,
                PackCompression::ZStd,
            )
            .expect("open wire pack");
            pack.append(&wire).expect("append legacy epoch meta");
            pack.commit().expect("commit wire pack");
        }

        // The real open path must succeed via the legacy fallback and preserve the committee.
        let pack = ConsensusPack::open_append_exists(temp_dir.path(), epoch)
            .expect("pre-rpc epoch pack opens via legacy compat");
        assert_eq!(pack.epoch(), epoch);
        let opened = &pack.committee;
        assert_eq!(opened.epoch(), committee.epoch());
        assert_eq!(opened.size(), committee.size());
        for (key, original) in committee.bootstrap_servers() {
            let restored = opened.get_bootstrap(&key).expect("bootstrap server preserved");
            assert_eq!(restored.primary.network_address, original.primary.network_address);
            assert_eq!(restored.worker.network_address, original.worker.network_address);
            assert!(restored.primary.rpc.is_none(), "primary rpc defaulted to None");
            assert!(restored.worker.rpc.is_none(), "worker rpc defaulted to None");
        }
        drop(pack);
    }

    /// The legacy fallback must not regress the hot path: a current-format epoch pack still opens
    /// through `open_append_exists` via the fast decode (no fallback, no warning).
    #[tokio::test]
    async fn test_open_append_exists_current_format_fast_path() {
        let temp_dir = TempDir::with_prefix("test_cp_current_fast").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Create a normal current-format pack (EpochMeta + a few outputs) and close it.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append");
            let mut parent = ConsensusHeader::default().digest();
            for i in 0..3 {
                let output =
                    make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
                parent = output.digest().into();
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }

        // Reopen the current-format pack through the same path the legacy fix touches.
        let pack = ConsensusPack::open_append_exists(temp_dir.path(), committee.epoch())
            .expect("current-format pack opens via fast path");
        assert_eq!(pack.epoch(), committee.epoch());
        assert_eq!(pack.committee.size(), committee.size());
        assert!(pack.get_consensus_output(1).await.is_ok(), "data readable after reopen");
    }

    /// A CRC-valid first record that decodes as neither the current nor the legacy `PackRecord`
    /// must stay fatal — the legacy fallback widens what we accept, but only to the one proven
    /// pre-fork layout, never to genuine garbage.
    #[tokio::test]
    async fn test_open_append_exists_undecodable_record_is_fatal() {
        use crate::archive::pack::Pack;
        use tn_types::Epoch;

        let temp_dir = TempDir::with_prefix("test_cp_undecodable").expect("temp dir");
        let epoch: Epoch = 0;
        let epoch_dir = temp_dir.path().join(format!("epoch-{epoch}"));
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");

        // A valid header + a CRC-valid record whose payload is `[0x05]`: a BCS enum variant index
        // of 5, which neither `PackRecord` nor `LegacyPackRecord` (each with 3 variants) accepts.
        // So the current decode fails with DeserializeValue, the legacy decode also fails, and the
        // open must surface the error rather than silently widening acceptance.
        {
            let mut pack: Pack<u8> = Pack::open(
                epoch_dir.join(Inner::DATA_NAME),
                epoch as u64,
                false,
                PackCompression::ZStd,
            )
            .expect("open raw pack");
            pack.append(&5_u8).expect("append undecodable record");
            pack.commit().expect("commit");
        }

        let err = ConsensusPack::open_append_exists(temp_dir.path(), epoch)
            .expect_err("an undecodable first record must be fatal");
        assert!(matches!(err, super::PackError::EpochLoad(_)), "expected EpochLoad, got {err:?}");
    }

    /// CP5 — the p2p-stream blocker Step 4 missed: a pre-rpc `EpochMeta` first record must decode
    /// through *every* reader, not just the local `open_*` path. Exercises the sync `PackIter`, the
    /// async `AsyncPackIter::open` (historic full-epoch import), and `AsyncPackIter::open_partial`
    /// (live output stream) — all now route decode through `PackRecord::try_decode_record`.
    #[tokio::test]
    async fn test_pack_iters_decode_pre_rpc_first_record() {
        use super::{LegacyPackRecord, PackRecord};
        use crate::archive::{
            pack::{Pack, DATA_HEADER_BYTES},
            pack_iter::{AsyncPackIter, PackIter},
        };
        use std::io::Cursor;
        use tn_types::{try_decode, BlockNumHash, ConsensusNumHash};

        let temp_dir = TempDir::with_prefix("test_cp_iter_pre_rpc").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let epoch = committee.epoch();

        // Guard: the decompressed record bytes decode ONLY via the legacy layout.
        let bcs = legacy_wire::wire_epoch_meta_bytes(
            &committee,
            1,
            BlockNumHash::default(),
            ConsensusNumHash::default(),
        );
        assert!(try_decode::<PackRecord>(&bcs).is_err(), "guard: bytes must be legacy-only");
        assert!(try_decode::<LegacyPackRecord>(&bcs).is_ok(), "guard: legacy layout decodes");

        // Write a one-record pack (header + framed, zstd-compressed legacy EpochMeta) the same way
        // a real pack would, so the readers hit the production decompress + decode path.
        let data_path = temp_dir.path().join("data");
        let wire = legacy_wire::wire_epoch_meta(
            &committee,
            1,
            BlockNumHash::default(),
            ConsensusNumHash::default(),
        );
        {
            let mut pack: Pack<legacy_wire::WirePackRecord> =
                Pack::open(&data_path, epoch as u64, false, PackCompression::ZStd)
                    .expect("open wire pack");
            pack.append(&wire).expect("append legacy meta");
            pack.commit().expect("commit");
        }
        let file_bytes = std::fs::read(&data_path).expect("read data");

        // 1. Sync PackIter over the full pack (header + record).
        {
            let file = std::fs::File::open(&data_path).expect("open file");
            let mut iter =
                PackIter::<PackRecord, _>::open(file, epoch as u64).expect("sync iter open");
            match iter.next() {
                Some(Ok(PackRecord::EpochMeta(meta))) => assert_eq!(meta.epoch, epoch),
                other => panic!("sync PackIter expected EpochMeta via fallback, got {other:?}"),
            }
        }

        // 2. Async AsyncPackIter::open over the full pack (historic full-epoch import path).
        {
            let file = tokio::fs::File::open(&data_path).await.expect("open file");
            let mut iter = AsyncPackIter::<PackRecord, _>::open(file, epoch as u64)
                .await
                .expect("async iter open");
            match iter.next().await {
                Some(Ok(PackRecord::EpochMeta(meta))) => assert_eq!(meta.epoch, epoch),
                other => {
                    panic!("AsyncPackIter::open expected EpochMeta via fallback, got {other:?}")
                }
            }
        }

        // 3. AsyncPackIter::open_partial over just the records, no header (live output path).
        {
            let records = file_bytes[DATA_HEADER_BYTES..].to_vec();
            let mut iter = AsyncPackIter::<PackRecord, _>::open_partial(
                Cursor::new(records),
                PackCompression::ZStd,
            )
            .await
            .expect("partial iter open");
            match iter.next().await {
                Some(Ok(PackRecord::EpochMeta(meta))) => assert_eq!(meta.epoch, epoch),
                other => panic!("open_partial expected EpochMeta via fallback, got {other:?}"),
            }
        }
    }

    /// The fallback must not regress the hot path: a current-format first record decodes directly,
    /// with the raw bytes accepted by the current decoder (no fallback) and the async iterator
    /// yielding it.
    #[tokio::test]
    async fn test_pack_iter_current_first_record_fast_path() {
        use super::PackRecord;
        use crate::archive::{
            pack::{Pack, DATA_HEADER_BYTES},
            pack_iter::AsyncPackIter,
        };
        use tn_types::try_decode;

        let temp_dir = TempDir::with_prefix("test_cp_iter_current").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        {
            let pack =
                ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
                    .expect("open append");
            pack.persist().await.expect("persist");
        }
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // The raw first-record bytes are accepted by the current decoder — i.e. the fast path.
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&data_path, 0, true, PackCompression::ZStd).expect("open");
            let raw = pack.fetch_raw(DATA_HEADER_BYTES as u64).expect("fetch_raw");
            assert!(try_decode::<PackRecord>(&raw).is_ok(), "current first record uses fast path");
        }

        // And the async iterator yields it.
        let file = tokio::fs::File::open(&data_path).await.expect("open");
        let mut iter =
            AsyncPackIter::<PackRecord, _>::open(file, 0).await.expect("async iter open");
        assert!(
            matches!(iter.next().await, Some(Ok(PackRecord::EpochMeta(_)))),
            "current EpochMeta decodes via the async iterator",
        );
    }

    /// CP6: `rewrite_legacy_epoch` turns a pre-fork epoch pack into a current-format one that opens
    /// with no legacy fallback, preserves every output and the committee, backs up the original,
    /// and is idempotent (a second run is a no-op).
    #[tokio::test]
    async fn test_rewrite_legacy_epoch() {
        use super::{PackRecord, RewriteOutcome};
        use crate::archive::pack::{Pack, DATA_HEADER_BYTES};

        let root = TempDir::with_prefix("test_cp_rewrite").expect("temp dir");
        let epochs_dir = root.path().join("epochs");
        let tmp_dir = root.path().join("tmp");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let epoch = committee.epoch(); // 0
        let previous_epoch = test_previous_epoch(&committee);

        // 1. Build a normal current-format pack in scratch and capture its outputs.
        let scratch = TempDir::with_prefix("test_cp_rewrite_src").expect("temp dir");
        let num_outputs = 8_u64;
        let mut outputs = Vec::new();
        {
            let pack =
                ConsensusPack::open_append(scratch.path(), previous_epoch, committee.clone())
                    .expect("open append");
            let mut parent = ConsensusHeader::default().digest();
            for i in 0..num_outputs {
                let output =
                    make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
                parent = output.digest();
                outputs.push(output.clone());
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }
        let scratch_data = scratch.path().join(format!("epoch-{epoch}")).join(Inner::DATA_NAME);

        // 2. Read the current EpochMeta and the length of its on-disk record, so we can keep the
        //    (wire-stable) output records that follow it.
        let (meta, current_meta_len) = {
            let mut p: Pack<PackRecord> =
                Pack::open(&scratch_data, epoch as u64, true, PackCompression::ZStd).expect("open");
            let meta =
                p.fetch(DATA_HEADER_BYTES as u64).expect("fetch meta").into_epoch().expect("meta");
            let len = p.record_size(DATA_HEADER_BYTES as u64).expect("record_size") as usize;
            (meta, len)
        };
        let scratch_bytes = std::fs::read(&scratch_data).expect("read scratch");
        let outputs_bytes = scratch_bytes[DATA_HEADER_BYTES + current_meta_len..].to_vec();

        // 3. Forge a faithful pre-fork pack: a legacy (rpc-stripped) first record carrying the same
        //    meta fields, concatenated in front of the verbatim output records.
        let legacy_prefix = {
            let src = scratch.path().join("legacy_meta");
            let wire = legacy_wire::wire_epoch_meta(
                &committee,
                meta.start_consensus_number,
                meta.genesis_exec_state,
                meta.genesis_consensus,
            );
            let mut lp: Pack<legacy_wire::WirePackRecord> =
                Pack::open(&src, epoch as u64, false, PackCompression::ZStd).expect("open legacy");
            lp.append(&wire).expect("append legacy meta");
            lp.commit().expect("commit");
            drop(lp);
            std::fs::read(&src).expect("read legacy prefix")
        };
        let epoch_dir = epochs_dir.join(format!("epoch-{epoch}"));
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        let mut legacy_data = legacy_prefix;
        legacy_data.extend_from_slice(&outputs_bytes);
        std::fs::write(epoch_dir.join(Inner::DATA_NAME), &legacy_data).expect("write legacy data");

        // The forged pack must be detected as legacy (its first record is legacy-only).
        assert!(
            ConsensusPack::epoch_pack_needs_rewrite(&epochs_dir, epoch).expect("probe"),
            "forged pack must be detected as legacy",
        );

        // 4. Rewrite.
        let outcome = ConsensusPack::rewrite_legacy_epoch(&epochs_dir, epoch, &tmp_dir, false)
            .await
            .expect("rewrite");
        let backup = match outcome {
            RewriteOutcome::Migrated { records, backup, .. } => {
                assert_eq!(records, num_outputs as usize, "all outputs carried over");
                backup
            }
            RewriteOutcome::AlreadyCurrent => panic!("expected Migrated"),
        };
        assert!(backup.exists(), "original epoch dir preserved as backup");

        // 5. The rewritten pack now decodes on the fast path (no fallback) ...
        assert!(
            !ConsensusPack::epoch_pack_needs_rewrite(&epochs_dir, epoch).expect("probe2"),
            "rewritten pack is current-format",
        );
        // ... preserves every output, read back through the real static path ...
        let pack = ConsensusPack::open_static(&epochs_dir, epoch).expect("open static");
        for (i, original) in outputs.iter().enumerate() {
            let got = pack.get_consensus_output(i as u64 + 1).await.expect("output");
            compare_outputs(&got, original);
        }
        // ... and preserves the committee (with rpc defaulted to None).
        assert_eq!(pack.committee.size(), committee.size());
        for (key, original) in committee.bootstrap_servers() {
            let restored = pack.committee.get_bootstrap(&key).expect("bootstrap preserved");
            assert_eq!(restored.primary.network_address, original.primary.network_address);
            assert!(restored.primary.rpc.is_none(), "rpc defaulted to None");
        }
        drop(pack);

        // 6. Idempotent: a second rewrite is a no-op.
        let again = ConsensusPack::rewrite_legacy_epoch(&epochs_dir, epoch, &tmp_dir, false)
            .await
            .expect("rewrite 2");
        assert!(matches!(again, RewriteOutcome::AlreadyCurrent), "second run is AlreadyCurrent");
    }
}
