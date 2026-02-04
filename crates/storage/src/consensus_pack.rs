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
    Batch, BlockHash, BlockNumHash, BlsPublicKey, CertifiedBatch, Committee, ConsensusHeader,
    ConsensusOutput, Database, Epoch, EpochRecord, B256,
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};

use crate::{
    archive::{
        digest_index::index::HdxIndex,
        error::open::OpenError,
        fxhasher::FxHasher,
        index::Index as _,
        pack::Pack,
        pack_iter::PackIter,
        position_index::index::{PositionIndex, PACK_HEADER_SIZE},
    },
    tables::Batches,
};

/// Metadata for an Epoch.  Should always be the first record in a consensus pack.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
pub struct EpochMeta {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    pub committee: Vec<BlsPublicKey>,
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
}

/// Manage a single pack file of consensus data (typically one epoch os the consensus chain).
#[derive(Debug, Clone)]
pub struct ConsensusPack {
    tx: Sender<PackMessage>,
    _handle: Arc<JoinHandle<()>>,
    error: watch::Receiver<Option<PackError>>,
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
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let handle =
            std::thread::spawn(move || match Inner::open_append(path, epoch, &previous_epoch) {
                Ok(inner) => {
                    run_pack_loop(inner, rx, tx_error);
                }
                Err(e) => {
                    tx_error.send_replace(Some(e));
                }
            });
        Ok(Self { tx, _handle: Arc::new(handle), error })
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
        Ok(Self { tx, _handle: Arc::new(handle), error })
    }

    /// Create a new set of epoch static files to write consensus output into.
    pub fn stream_import<P: Into<PathBuf>, R: Read + Seek + Send + 'static>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: EpochRecord,
        start_consensus_number: u64,
    ) -> Result<ConsensusPack, PackError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || {
            match Inner::stream_import(path, stream, epoch, &previous_epoch, start_consensus_number)
            {
                Ok(inner) => {
                    run_pack_loop(inner, rx, tx_error);
                }
                Err(e) => {
                    tx_error.send_replace(Some(e));
                }
            }
        });
        Ok(Self { tx, _handle: Arc::new(handle), error })
    }

    /* XXXX
    /// Save all the batches and consensus header to the pack file.
    pub fn save_consensus<DB: Database>(
        &self,
        consensus: ConsensusHeader,
        db: DB,
    ) -> Result<(), PackError> {
        Ok(())
    }
    */

    /// Return a delayed error value.
    /// Work is sent to a background thread and any errors are recorded.
    pub fn get_error(&self) -> Result<(), PackError> {
        match &*self.error.borrow() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    pub fn save_consensus_output(&self, consensus: ConsensusOutput) -> Result<(), PackError> {
        let _ = self.tx.send(PackMessage::ConsensusOutput(consensus));
        Ok(())
    }

    /* XXXX
    /// Load and return the consensus output form this epoch.
    pub fn get_consensus_output(
        &self,
        number: u64,
        committee: &Committee,
    ) -> Result<ConsensusOutput, PackError> {
    }
    */

    /// True if consensus header by digest is found by digest.
    pub async fn contains_consensus_header_number(&self, number: u64) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ContainsConsensusHeaderNumber(number, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
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
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let _ = std::fs::create_dir_all(&base_dir);
        let mut data: Pack<PackRecord> = Pack::open(base_dir.join(Self::DATA_NAME), false)?;
        let start_consensus_number =
            if epoch == 0 { 1 } else { previous_epoch.final_consensus.number + 1 };
        let epoch_meta = EpochMeta {
            epoch,
            committee: previous_epoch.next_committee.clone(),
            start_consensus_number,
            genesis_exec_state: previous_epoch.final_state,
            genesis_consensus: previous_epoch.final_consensus,
        };

        if let Ok(meta) = data.fetch(PACK_HEADER_SIZE as u64) {
            let meta = meta.into_epoch()?;
            if epoch_meta != meta {
                return Err(PackError::InvalidEpoch);
            }
        } else {
            data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                .map_err(|e| PackError::Append(e.to_string()))?;
        }
        let consensus_idx =
            PositionIndex::open_pdx_file(base_dir.join(Self::CONSENSUS_POS_NAME), data.header())
                .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let batch_digests =
            HdxIndex::open_hdx_file(base_dir.join(Self::BATCH_HASH_NAME), data.header(), builder)
                .map_err(OpenError::IndexFileOpen)?;
        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Open up the static files for previous epoch.  These will be read only.
    fn open_static<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));

        let mut data = Pack::<PackRecord>::open(base_dir.join(Self::DATA_NAME), true)?;
        let epoch_meta = data
            .fetch(PACK_HEADER_SIZE as u64)
            .map_err(|e| PackError::EpochLoad(e.to_string()))?
            .into_epoch()?;
        let consensus_idx =
            PositionIndex::open_pdx_file(base_dir.join(Self::CONSENSUS_POS_NAME), data.header())
                .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let batch_digests =
            HdxIndex::open_hdx_file(base_dir.join(Self::BATCH_HASH_NAME), data.header(), builder)
                .map_err(OpenError::IndexFileOpen)?;

        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Create a new set of epoch static files to write consensus output into.
    fn stream_import<P: AsRef<Path>, R: Read + Seek>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        start_consensus_number: u64,
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let mut stream_iter = PackIter::<PackRecord, R>::open(stream)
            .map_err(|e| PackError::ReadError(e.to_string()))?;
        let mut data = Pack::open(base_dir.join(Self::DATA_NAME), false)?;
        let epoch_meta = EpochMeta {
            epoch,
            committee: previous_epoch.next_committee.clone(),
            start_consensus_number,
            genesis_exec_state: previous_epoch.final_state,
            genesis_consensus: previous_epoch.final_consensus,
        };
        let stream_meta = if let Some(Ok(meta)) = stream_iter.next() {
            meta.into_epoch()?
        } else {
            return Err(PackError::NotEpoch);
        };
        if epoch_meta != stream_meta {
            return Err(PackError::InvalidEpoch);
        }
        data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
            .map_err(|e| PackError::Append(e.to_string()))?;
        let mut consensus_idx =
            PositionIndex::open_pdx_file(base_dir.join(Self::CONSENSUS_POS_NAME), data.header())
                .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut consensus_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CONSENSUS_HASH_NAME),
            data.header(),
            builder,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut batch_digests =
            HdxIndex::open_hdx_file(base_dir.join(Self::BATCH_HASH_NAME), data.header(), builder)
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
                        .save(batch_digest.as_slice(), position)
                        .map_err(|e| PackError::IndexAppend(e.to_string()))?;
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
                        .save(consensus_digest.as_slice(), position)
                        .map_err(|e| PackError::IndexAppend(e.to_string()))?;
                    let consensus_idx_pos =
                        consensus_number.saturating_sub(epoch_meta.start_consensus_number);
                    consensus_idx
                        .save(consensus_idx_pos, position)
                        .map_err(|e| PackError::IndexAppend(e.to_string()))?;
                }
            }
        }
        Ok(Self { data, consensus_idx, consensus_digests, batch_digests, epoch_meta })
    }

    /// Save all the batches and consensus header to the pack file.
    fn _save_consensus<DB: Database>(
        &mut self,
        consensus: ConsensusHeader,
        db: DB,
    ) -> Result<(), PackError> {
        let mut batches = Vec::new();
        // Collect all the batches for the consensus header.
        for cert in &consensus.sub_dag.certificates {
            for (batch_digest, _) in cert.header().payload() {
                match db
                    .get::<Batches>(batch_digest)
                    .map_err(|e| PackError::BatchLoad(e.to_string()))?
                {
                    Some(batch) => batches.push((batch_digest, batch)),
                    None => return Err(PackError::MissingBatch),
                }
            }
        }
        // Save all the required batcdhes into the pack file.
        for (batch_digest, batch) in batches.drain(..) {
            let position = self
                .data
                .append(&PackRecord::Batch(batch))
                .map_err(|e| PackError::Append(e.to_string()))?;
            self.batch_digests
                .save(batch_digest.as_slice(), position)
                .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        }
        // Now save the consensus header.
        let consensus_digest = consensus.digest();
        let consensus_number = consensus.number;
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus)))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.consensus_digests
            .save(consensus_digest.as_slice(), position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        let consensus_idx = consensus_number.saturating_sub(self.epoch_meta.start_consensus_number);
        self.consensus_idx
            .save(consensus_idx, position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;

        Ok(())
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    fn save_consensus_output(&mut self, consensus: &ConsensusOutput) -> Result<(), PackError> {
        let mut batches = BTreeMap::new();
        // We want to make sure batches are saved to the pack in a deterministic order, so
        // collect them in a BTreeMap.  We probably don't actually need this but this
        // means we do not impose any extra restrictions on consensus output.
        for cert_batch in consensus.batches() {
            for batch in &cert_batch.batches {
                batches.insert(batch.digest(), batch.clone());
            }
        }
        // Save all the required batcdhes into the pack file.
        for (batch_digest, batch) in batches.into_iter() {
            let position = self
                .data
                .append(&PackRecord::Batch(batch))
                .map_err(|e| PackError::Append(e.to_string()))?;
            self.batch_digests
                .save(batch_digest.as_slice(), position)
                .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        }
        // Now save the consensus header.
        let consensus_digest = consensus.consensus_header_hash();
        let consensus_number = consensus.number();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.consensus_digests
            .save(consensus_digest.as_slice(), position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        let consensus_idx = consensus_number.saturating_sub(self.epoch_meta.start_consensus_number);
        self.consensus_idx
            .save(consensus_idx, position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;

        Ok(())
    }

    /// Load and return the consensus output form this epoch.
    fn _get_consensus_output(
        &mut self,
        number: u64,
        committee: &Committee,
    ) -> Result<ConsensusOutput, PackError> {
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
                //XXXXconsensus_output.batch_digests.push_back(*digest);
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
                    .load(digest.as_slice())
                    .map_err(|e| PackError::ReadError(e.to_string()))?;
                let batch = self
                    .data
                    .fetch(position)
                    .map_err(|e| PackError::ReadError(e.to_string()))?
                    .into_batch()?;
                cert_batches.push(batch);
            }

            let address = committee.authority(cert.origin()).map(|a| a.execution_address());
            if let Some(address) = address {
                // main collection for execution
                //XXXXconsensus_output.batches.push(CertifiedBatch { address, batches: cert_batches });
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

    /// True if consensus header by digest is found by digest.
    fn contains_consensus_header(&mut self, digest: B256) -> bool {
        self.consensus_digests.load(digest.as_slice()).is_ok()
    }

    /// Retrieve a consensus header by digest.
    fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
        let pos = self.consensus_digests.load(digest.as_slice()).ok()?;
        let rec = self.data.fetch(pos).ok()?;
        rec.into_consensus().ok()
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
        }
    }
}

impl From<OpenError> for PackError {
    fn from(value: OpenError) -> Self {
        Self::Open(Arc::new(value))
    }
}
