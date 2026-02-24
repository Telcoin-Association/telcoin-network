//! Implement a Pack file to contain consensus chain data (Batches and ConsensusHeaders).
//! Stored per epoch.

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    error::Error,
    fmt::Display,
    hash::BuildHasherDefault,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
};

use serde::{Deserialize, Serialize};
use tn_types::{
    Batch, BlockHash, BlockNumHash, CertifiedBatch, Committee, ConsensusHeader, ConsensusOutput,
    Epoch, EpochRecord, B256,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};

use crate::archive::{
    digest_index::index::HdxIndex,
    error::open::OpenError,
    fxhasher::FxHasher,
    index::Index as _,
    pack::{Pack, DATA_HEADER_BYTES},
    pack_iter::PackIter,
    position_index::index::PositionIndex,
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
    pub genesis_consensus: BlockNumHash,
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
    ConsensusOutput(ConsensusOutput),
    ContainsConsensusHeaderNumber(u64, oneshot::Sender<bool>),
    ContainsConsensusHeader(B256, oneshot::Sender<bool>),
    ConsensusHeader(B256, oneshot::Sender<Option<ConsensusHeader>>),
    GetConsensusOutput(u64, oneshot::Sender<Result<ConsensusOutput, PackError>>),
    Persist(oneshot::Sender<Result<(), PackError>>),
}

/// Manage a single pack file of consensus data (typically one epoch os the consensus chain).
#[derive(Debug, Clone)]
pub struct ConsensusPack {
    tx: Sender<PackMessage>,
    _handle: Arc<JoinHandle<()>>,
    error: watch::Receiver<Option<PackError>>,
    epoch: Epoch,
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
            PackMessage::GetConsensusOutput(number, tx) => {
                let _ = tx.send(inner.get_consensus_output(number));
            }
            PackMessage::Persist(tx) => {
                let _ = tx.send(inner.persist());
            }
        }
    }
}

impl ConsensusPack {
    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    pub fn open_append<P: Into<PathBuf>>(
        path: P,
        epoch: Epoch,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || {
            match Inner::open_append(path, epoch, &previous_epoch, committee) {
                Ok(inner) => {
                    run_pack_loop(inner, rx, tx_error);
                }
                Err(e) => {
                    tx_error.send_replace(Some(e));
                }
            }
        });
        Ok(Self { tx, _handle: Arc::new(handle), error, epoch })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    pub fn open_static<P: Into<PathBuf>>(
        path: P,
        epoch: Epoch,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || match Inner::open_static(path, epoch) {
            Ok(inner) => {
                run_pack_loop(inner, rx, tx_error);
            }
            Err(e) => {
                tx_error.send_replace(Some(e));
            }
        });
        Ok(Self { tx, _handle: Arc::new(handle), error, epoch })
    }

    /// Create a new set of epoch static files to write consensus output into.
    pub fn stream_import<P: Into<PathBuf>, R: Read + Seek + Send + 'static>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: EpochRecord,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || {
            match Inner::stream_import(path, stream, epoch, &previous_epoch) {
                Ok(inner) => {
                    run_pack_loop(inner, rx, tx_error);
                }
                Err(e) => {
                    tx_error.send_replace(Some(e));
                }
            }
        });
        Ok(Self { tx, _handle: Arc::new(handle), error, epoch })
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
        if self.tx.send(PackMessage::GetConsensusOutput(number, tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::ReceiveFailed)?
        } else {
            Err(PackError::SendFailed)
        }
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
    pub async fn contains_consensus_header(&self, digest: B256) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ContainsConsensusHeader(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(&self, digest: B256) -> Option<ConsensusHeader> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ConsensusHeader(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    pub async fn persist(&self) -> Result<(), PackError> {
        self.get_error()?;
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Persist(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }
}

#[derive(Debug)]
struct Inner {
    data: Pack<PackRecord>,
    consensus_idx: PositionIndex,
    consensus_digests: HdxIndex,
    batch_digests: HdxIndex,
    epoch_meta: EpochMeta,
}

impl Inner {
    const DATA_NAME: &str = "data";
    const CONSENSUS_POS_NAME: &str = "idx";
    const CONSENSUS_HASH_NAME: &str = "hash";
    const BATCH_HASH_NAME: &str = "bhash";

    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    fn open_append<P: AsRef<Path>>(
        path: P,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        committee: Committee,
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = std::fs::create_dir_all(&base_dir);
        let mut data: Pack<PackRecord> = Pack::open(base_dir.join(Self::DATA_NAME), false)?;
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
                return Err(PackError::InvalidEpoch);
            }
        } else {
            data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                .map_err(|e| PackError::Append(e.to_string()))?;
        }
        let consensus_idx = PositionIndex::open_pdx_file(
            base_dir.join(Self::CONSENSUS_POS_NAME),
            data.header(),
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
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
        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    fn open_static<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));

        let mut data = Pack::<PackRecord>::open(base_dir.join(Self::DATA_NAME), true)?;
        let epoch_meta = data
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?
            .into_epoch()?;
        let consensus_idx = PositionIndex::open_pdx_file(
            base_dir.join(Self::CONSENSUS_POS_NAME),
            data.header(),
            true,
        )
        .map_err(OpenError::IndexFileOpen)?;
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

        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Create a new set of epoch static files to write consensus output into.
    fn stream_import<P: AsRef<Path>, R: Read + Seek>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = std::fs::create_dir_all(&base_dir);
        let mut stream_iter = PackIter::<PackRecord, R>::open(stream)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let mut data = Pack::open(base_dir.join(Self::DATA_NAME), false)?;
        let epoch_meta = if let Some(Ok(meta)) = stream_iter.next() {
            meta.into_epoch()?
        } else {
            return Err(PackError::NotEpoch);
        };
        if epoch != epoch_meta.epoch {
            return Err(PackError::InvalidEpoch);
        }
        data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
            .map_err(|e| PackError::Append(e.to_string()))?;
        let mut consensus_idx = PositionIndex::open_pdx_file(
            base_dir.join(Self::CONSENSUS_POS_NAME),
            data.header(),
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
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
        let mut parent_digest = previous_epoch.final_consensus.hash;
        let mut batches = HashSet::new();
        for record in stream_iter {
            let record = record.map_err(|e| PackError::ReadError(e.to_string()))?;
            match record {
                PackRecord::EpochMeta(_epoch_meta) => {
                    return Err(PackError::EpochLoad("epoch meta data found twice".to_string()))
                }
                PackRecord::Batch(batch) => {
                    let batch_digest = batch.digest();
                    batches.insert(batch_digest);
                    let position = data
                        .append(&PackRecord::Batch(batch))
                        .map_err(|e| PackError::Append(e.to_string()))?;
                    batch_digests
                        .save(batch_digest, position)
                        .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
                }
                PackRecord::Consensus(consensus_header) => {
                    if consensus_header.parent_hash != parent_digest {
                        return Err(PackError::InvalidConsensusChain);
                    }
                    for cert in &consensus_header.sub_dag.certificates {
                        for (digest, _) in cert.header().payload().iter() {
                            if !batches.remove(digest) {
                                return Err(PackError::MissingBatches);
                            }
                        }
                    }
                    if !batches.is_empty() {
                        return Err(PackError::ExtraBatches);
                    }
                    let consensus_digest = consensus_header.digest();
                    parent_digest = consensus_digest;
                    let consensus_number = consensus_header.number;
                    let position = data
                        .append(&PackRecord::Consensus(consensus_header))
                        .map_err(|e| PackError::Append(e.to_string()))?;
                    consensus_digests
                        .save(consensus_digest, position)
                        .map_err(|e| PackError::IndexAppend(format!("consensus digest {e}")))?;
                    let consensus_idx_pos =
                        consensus_number.saturating_sub(epoch_meta.start_consensus_number);
                    consensus_idx
                        .save(consensus_idx_pos, position)
                        .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
                }
            }
        }
        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    fn save_consensus_output(&mut self, consensus: &ConsensusOutput) -> Result<(), PackError> {
        let consensus_number = consensus.number();
        // Adjusted consensus index for this pack file.
        let consensus_idx = consensus_number.saturating_sub(self.epoch_meta.start_consensus_number);
        // Make sure this number is valid before we write anything...
        if (consensus_idx as usize) < self.consensus_idx.len() {
            // If we have saved this output already then ignore it.
            return Ok(());
        } else if consensus_idx as usize != self.consensus_idx.len() {
            return Err(PackError::InvalidConsensusNumber);
        }
        let mut batches = BTreeMap::new();
        // We want to make sure batches are saved to the pack in a deterministic order, so
        // collect them in a BTreeMap.  We probably don't actually need this but this
        // means we do not impose any extra restrictions on consensus output.
        for cert_batch in consensus.batches() {
            for batch in &cert_batch.batches {
                let digest = batch.digest();
                // Filter out any batches we already have saved.
                if !self.batch_digests.contains(digest) {
                    batches.insert(digest, batch.clone());
                }
            }
        }
        // Save all the required batcdhes into the pack file.
        for (batch_digest, batch) in batches.into_iter() {
            let position = self
                .data
                .append(&PackRecord::Batch(batch))
                .map_err(|e| PackError::Append(e.to_string()))?;
            self.batch_digests
                .save(batch_digest, position)
                .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
        }
        // Now save the consensus header.
        let consensus_digest = consensus.consensus_header_hash();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.consensus_digests
            .save(consensus_digest, position)
            .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
        self.consensus_idx
            .save(consensus_idx, position)
            .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;

        Ok(())
    }

    /// Load and return the consensus output form this epoch.
    fn get_consensus_output(&mut self, number: u64) -> Result<ConsensusOutput, PackError> {
        let rec_pos_idx = number.saturating_sub(self.epoch_meta.start_consensus_number);
        let position = self
            .consensus_idx
            .load(rec_pos_idx)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let header = self
            .data
            .fetch(position)
            .map_err(|e| PackError::ReadError(e.to_string()))?
            .into_consensus()?;
        let parent_hash = header.parent_hash;
        let deliver = header.sub_dag;
        let num_blocks = deliver.num_primary_blocks();
        let num_certs = deliver.len();

        let sub_dag = deliver;
        if num_blocks == 0 {
            return Ok(ConsensusOutput::new_with_subdag(sub_dag, parent_hash, number));
        }

        let mut batch_set: HashSet<BlockHash> = HashSet::new();

        let mut batch_digests = VecDeque::with_capacity(num_certs);
        for cert in sub_dag.certificates() {
            for (digest, _) in cert.header().payload().iter() {
                batch_set.insert(*digest);
                batch_digests.push_back(*digest);
            }
        }

        // map all fetched batches to their respective certificates for applying block rewards
        let mut batches = Vec::with_capacity(num_certs);
        for cert in &sub_dag.certificates {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(cert.header().payload().len());

            // retrieve fetched batch by digest
            for digest in cert.header().payload().keys() {
                let position = self
                    .batch_digests
                    .load(*digest)
                    .map_err(|e| PackError::ReadError(e.to_string()))?;
                let batch = self
                    .data
                    .fetch(position)
                    .map_err(|e| PackError::ReadError(e.to_string()))?
                    .into_batch()?;
                cert_batches.push(batch);
            }

            let address =
                self.epoch_meta.committee.authority(cert.origin()).map(|a| a.execution_address());
            if let Some(address) = address {
                // main collection for execution
                batches.push(CertifiedBatch { address, batches: cert_batches });
            } else {
                return Err(PackError::MissingAuthority);
            }
        }
        Ok(ConsensusOutput::new(sub_dag, parent_hash, number, false, batch_digests, batches))
    }

    /// True if consensus header by digest is found by digest.
    fn contains_consensus_header_number(&self, number: u64) -> bool {
        number >= self.epoch_meta.start_consensus_number
            && number < self.consensus_idx.len() as u64 + self.epoch_meta.start_consensus_number
    }

    /// True if consensus header is found by digest.
    fn contains_consensus_header(&mut self, digest: B256) -> bool {
        self.consensus_digests.contains(digest)
    }

    /// Retrieve a consensus header by digest.
    fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
        let pos = self.consensus_digests.load(digest).ok()?;
        let rec = self.data.fetch(pos).ok()?;
        rec.into_consensus().ok()
    }

    fn persist(&mut self) -> Result<(), PackError> {
        self.data.commit().map_err(|e| PackError::PersistError(e.to_string()))?;
        self.consensus_idx.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        self.consensus_digests.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        self.batch_digests.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        Ok(())
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
    InvalidEpoch,
    SendFailed,
    ReceiveFailed,
    PersistError(String),
    InvalidConsensusNumber,
    ConsensusNumberAlreadyAdded,
}

impl Error for PackError {}
impl Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::IO(error) => write!(f, "IO({error}"),
            PackError::MissingBatch => write!(f, "Missing Batch"),
            PackError::BatchLoad(error) => write!(f, "Batch Load Error ({error})"),
            PackError::EpochLoad(error) => write!(f, "Epoch Load Error ({error})"),
            PackError::Append(error) => write!(f, "Data Append Error ({error})"),
            PackError::IndexAppend(error) => write!(f, "Index Append Error ({error})"),
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
            PackError::InvalidEpoch => write!(f, "Epoch meta data incorrect"),
            PackError::SendFailed => write!(f, "Internal channel send failed"),
            PackError::ReceiveFailed => write!(f, "Internal channel receive failed"),
            PackError::PersistError(e) => write!(f, "Failed to persist: {e}"),
            PackError::InvalidConsensusNumber => {
                write!(f, "Consensus output MUST be added in consective order by number")
            }
            PackError::ConsensusNumberAlreadyAdded => {
                write!(
                    f,
                    "Consensus output MUST be added in consective order by number (already added)"
                )
            }
        }
    }
}

impl From<OpenError> for PackError {
    fn from(value: OpenError) -> Self {
        Self::Open(Arc::new(value))
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::VecDeque,
        fs::File,
        io::{Seek as _, SeekFrom},
        sync::Arc,
        time::Duration,
    };

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        now, test_genesis, BlockHash, Certificate, CertifiedBatch, CommittedSubDag, Committee,
        ConsensusOutput, EpochRecord, Hash, ReputationScores,
    };

    use crate::{
        consensus_pack::{ConsensusPack, Inner},
        mem_db::MemDatabase,
    };

    fn make_test_output(
        committee: &Committee,
        authority_index: usize,
        chain: Arc<RethChainSpec>,
        number: u64,
        parent: BlockHash,
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

        let timestamp = now();
        let mut leader_1 = Certificate::default();
        // update cert
        leader_1.update_created_at_for_test(timestamp);
        leader_1.header_mut_for_test().author = authority_1;
        for batch in &batches_1 {
            leader_1.header.payload.insert(batch.digest(), 0_u16);
        }
        let sub_dag_index_1 = 1;
        leader_1.header.round = sub_dag_index_1 as u32;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = Arc::new(CommittedSubDag::new(
            vec![leader_1.clone()],
            leader_1,
            sub_dag_index_1,
            reputation_scores,
            previous_sub_dag,
        ));
        ConsensusOutput::new(
            subdag_1.clone(),
            parent,
            number,
            false,
            batch_digests_1.clone(),
            vec![CertifiedBatch { address: batch_producer, batches: batches_1 }],
        )
    }

    fn compare_outputs(output1: &ConsensusOutput, output2: &ConsensusOutput) {
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
        let pack = ConsensusPack::open_append(
            temp_dir.path(),
            0,
            previous_epoch.clone(),
            committee.clone(),
        )
        .expect("open pack");

        let num_outputs = 1000;
        let mut outputs = Vec::new();
        let mut parent = BlockHash::default();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            pack.save_consensus_output(consensus_output).await.unwrap();
        }
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }

        pack.persist().await.expect("persist");
        drop(pack);

        // Reopen in append and load some more data.
        let pack = ConsensusPack::open_append(
            temp_dir.path(),
            0,
            previous_epoch.clone(),
            committee.clone(),
        )
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
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output((i + num_outputs) as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize + num_outputs).unwrap();
            compare_outputs(&output_db, output);
        }
        pack.persist().await.expect("persist");
        drop(pack);

        // Open read only and verify.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open pack");
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output((i + num_outputs) as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize + num_outputs).unwrap();
            compare_outputs(&output_db, output);
        }
        drop(pack);

        // Make sure we can stream the file to create another pack file.
        let temp_dir2 = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
        let stream =
            File::open(temp_dir.path().join("epoch-0").join(Inner::DATA_NAME)).expect("log file");
        let pack = ConsensusPack::stream_import(temp_dir2.path(), stream, 0, previous_epoch)
            .expect("open pack");
        tokio::time::sleep(Duration::from_secs(2)).await;
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
        for i in 0..num_outputs {
            let output_db = pack.get_consensus_output((i + num_outputs) as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize + num_outputs).unwrap();
            compare_outputs(&output_db, output);
        }
        drop(pack);

        let mut f1 = File::open(temp_dir.path().join("epoch-0")).expect("log file");
        let mut f2 = File::open(temp_dir2.path().join("epoch-0")).expect("log file");
        assert_eq!(
            f1.seek(SeekFrom::End(0)).unwrap(),
            f2.seek(SeekFrom::End(0)).unwrap(),
            "files not the same length"
        );
    }
}
