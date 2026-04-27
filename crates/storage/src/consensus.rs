//! Wrap access to the epoch consensus files into a single interface.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use parking_lot::Mutex;
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, BlockNumHash,
    CommittedSubDag, Committee, ConsensusHeader, ConsensusOutput, Epoch, EpochRecord, Round, B256,
};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncRead, AsyncSeek},
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
};
use tracing::error;

use crate::{
    consensus_pack::{ConsensusPack, PackError, DATA_NAME},
    epoch_records::{EpochDbError, EpochRecordDb},
};

pub trait ReadStream: AsyncRead + AsyncSeek + Send + Unpin {}
impl ReadStream for AsyncFile {}

/// Simple enum for which of two saved consensus states we are using.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConsensusSlot {
    /// Use the first save file.
    Slot1,
    /// Use the second save file.
    Slot2,
}

/// Inner data to allow proper shared clones.
#[derive(Debug)]
struct LatestConsensusInner {
    /// Epoch of the last saved output.
    epoch: Epoch,
    /// Number of the last saved output.
    number: u64,
    /// Track which slot/file to write to next.
    current_slot: ConsensusSlot,
}

/// Manage and persist the latest consensus state.
#[derive(Debug, Clone)]
struct LatestConsensus {
    /// Shared state for clones.
    state: Arc<Mutex<LatestConsensusInner>>,
    /// Sender for messages to the background thread.
    tx: Sender<LatestConsensusCommand>,
    /// Background thread join handle.
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Drop for LatestConsensus {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handle) == 1 {
            // If we are the last ConsensusPack then shutdown thread and wait for it persist and
            // exit.
            if let Some(handle) = self.handle.lock().take() {
                if self.tx.try_send(LatestConsensusCommand::Shutdown).is_ok() {
                    let _ = handle.join();
                }
            }
        }
    }
}

/// Commands for the background thread.
enum LatestConsensusCommand {
    /// Save an update to the next slot.
    Update(Epoch, Epoch, u64, ConsensusSlot),
    /// Fully persist the slot files.
    Persist(oneshot::Sender<()>),
    /// Persist then shutdown the background thread.
    Shutdown,
}

impl LatestConsensus {
    /// Read the Epoch and number from a slot file.
    fn read_slot(slot: &mut File) -> Result<(Epoch, u64), ConsensusChainError> {
        if slot.seek(SeekFrom::End(0))? == 0 {
            Ok((0, 0))
        } else {
            slot.seek(SeekFrom::Start(0))?;
            let mut buffer32_epoch = [0_u8; 4];
            let mut buffer32_crc = [0_u8; 4];
            let mut buffer64 = [0_u8; 8];
            slot.read_exact(&mut buffer32_epoch)?;
            slot.read_exact(&mut buffer64)?;
            slot.read_exact(&mut buffer32_crc)?;
            let mut crc32_hasher = crc32fast::Hasher::new();
            crc32_hasher.update(&buffer32_epoch);
            crc32_hasher.update(&buffer64);
            let crc32 = crc32_hasher.finalize();
            let crc32_read = u32::from_le_bytes(buffer32_crc);
            if crc32 == crc32_read {
                Ok((u32::from_le_bytes(buffer32_epoch), u64::from_le_bytes(buffer64)))
            } else {
                Err(ConsensusChainError::CrcError)
            }
        }
    }

    /// Create a new latest consensus that saves files into base_path.
    fn new(base_path: &Path) -> Result<Self, ConsensusChainError> {
        let slot1_path = base_path.join("consensus_slot1");
        let slot2_path = base_path.join("consensus_slot2");
        {
            // If we are opening for write then make sure the file exists.
            // This function will create it if it does not exist or produce
            // an error if it does so ignore the errors.
            let _ = File::create_new(&slot1_path);
            let _ = File::create_new(&slot2_path);
        }
        let mut slot1 = OpenOptions::new().read(true).write(true).open(&slot1_path)?;
        let mut slot2 = OpenOptions::new().read(true).write(true).open(&slot2_path)?;
        let (slot1_epoch, slot1_number) = Self::read_slot(&mut slot1)?;
        let (slot2_epoch, slot2_number) = Self::read_slot(&mut slot2)?;

        let (tx, mut rx) = mpsc::channel(1000);
        let handle = std::thread::spawn(move || {
            fn sync_all_with_log(f: &File) {
                if let Err(e) = f.sync_all() {
                    error!(target: "consensus_chain", ?e, "failed to sync a file");
                }
            }
            while let Some(com) = rx.blocking_recv() {
                match com {
                    LatestConsensusCommand::Update(old_epoch, epoch, number, slot) => {
                        let f = match slot {
                            ConsensusSlot::Slot1 => &mut slot1,
                            ConsensusSlot::Slot2 => &mut slot2,
                        };
                        let mut buffer = [0_u8; 16];
                        buffer[0..4].copy_from_slice(&epoch.to_le_bytes());
                        buffer[4..12].copy_from_slice(&number.to_le_bytes());
                        let mut crc32_hasher = crc32fast::Hasher::new();
                        crc32_hasher.update(&epoch.to_le_bytes());
                        crc32_hasher.update(&number.to_le_bytes());
                        let crc32 = crc32_hasher.finalize();
                        buffer[12..16].copy_from_slice(&crc32.to_le_bytes());
                        if let Err(e) = f.seek(SeekFrom::Start(0)) {
                            error!(target: "consensus_chain", ?e, ?slot, "failed to sync a latest consensus state file");
                            continue;
                        }
                        if let Err(e) = f.write_all(&buffer) {
                            error!(target: "consensus_chain", ?e, ?slot, "failed to write to a latest consensus state file");
                            continue;
                        }
                        if old_epoch != epoch {
                            sync_all_with_log(f);
                        }
                    }
                    LatestConsensusCommand::Persist(tx) => {
                        sync_all_with_log(&slot1);
                        sync_all_with_log(&slot2);
                        let _ = tx.send(());
                    }
                    LatestConsensusCommand::Shutdown => {
                        sync_all_with_log(&slot1);
                        sync_all_with_log(&slot2);
                        break;
                    }
                }
            }
        });
        let me = if slot1_epoch == slot2_epoch {
            if slot1_number > slot2_number {
                Self {
                    state: Arc::new(Mutex::new(LatestConsensusInner {
                        epoch: slot1_epoch,
                        number: slot1_number,
                        current_slot: ConsensusSlot::Slot1,
                    })),
                    tx,
                    handle: Arc::new(Mutex::new(Some(handle))),
                }
            } else {
                Self {
                    state: Arc::new(Mutex::new(LatestConsensusInner {
                        epoch: slot2_epoch,
                        number: slot2_number,
                        current_slot: ConsensusSlot::Slot2,
                    })),
                    tx,
                    handle: Arc::new(Mutex::new(Some(handle))),
                }
            }
        } else if slot1_epoch > slot2_epoch {
            Self {
                state: Arc::new(Mutex::new(LatestConsensusInner {
                    epoch: slot1_epoch,
                    number: slot1_number,
                    current_slot: ConsensusSlot::Slot1,
                })),
                tx,
                handle: Arc::new(Mutex::new(Some(handle))),
            }
        } else {
            Self {
                state: Arc::new(Mutex::new(LatestConsensusInner {
                    epoch: slot2_epoch,
                    number: slot2_number,
                    current_slot: ConsensusSlot::Slot2,
                })),
                tx,
                handle: Arc::new(Mutex::new(Some(handle))),
            }
        };
        Ok(me)
    }

    /// Update the local state and send a message to save to disk in background.
    async fn update(&self, epoch: Epoch, number: u64) {
        let (old_epoch, current_slot) = {
            let mut state = self.state.lock();
            let old_epoch = state.epoch;
            state.epoch = epoch;
            state.number = number;
            match state.current_slot {
                ConsensusSlot::Slot1 => state.current_slot = ConsensusSlot::Slot2,
                ConsensusSlot::Slot2 => state.current_slot = ConsensusSlot::Slot1,
            }
            let current_slot = state.current_slot;
            (old_epoch, current_slot)
        };
        if let Err(e) = self
            .tx
            .send(LatestConsensusCommand::Update(old_epoch, epoch, number, current_slot))
            .await
        {
            error!(target: "consensus_chain", ?e, "failed to send consensus latest update to background thread!");
        }
    }

    /// Persist the saved state fully to disk.
    async fn persist(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(LatestConsensusCommand::Persist(tx)).await;
        let _ = rx.await;
    }

    /// Return the current Epoch.
    fn epoch(&self) -> Epoch {
        self.state.lock().epoch
    }

    /// Return the current number.
    fn number(&self) -> u64 {
        self.state.lock().number
    }

    /// Return the current slot value (for testing).
    #[cfg(test)]
    fn current_slot(&self) -> ConsensusSlot {
        self.state.lock().current_slot
    }
}

/// Implement a databse for consensus data.
#[derive(Debug, Clone)]
pub struct ConsensusChain {
    /// Base path for files.
    base_path: PathBuf,
    /// Current pack for the epoch being written.
    current_pack: Arc<Mutex<Option<ConsensusPack>>>,
    /// Track the latest consensus that was saved.
    latest_consensus: LatestConsensus,
    /// Simple cache of recent pack files.
    recent_packs: Arc<Mutex<VecDeque<ConsensusPack>>>,
    epochs: Arc<EpochRecordDb>,
}

impl ConsensusChain {
    /// How many recently opened pack files to maintain.
    const PACK_CACHE_SIZE: usize = 10;

    /// Create a new empty consensus chain.
    pub fn new(base_path: PathBuf) -> Result<ConsensusChain, ConsensusChainError> {
        let latest_consensus = LatestConsensus::new(&base_path)?;
        // If we have a pack for the last epoch open it so we can read data early.
        let current_pack = Arc::new(Mutex::new(
            ConsensusPack::open_append_exists(&base_path, latest_consensus.epoch()).ok(),
        ));
        let recent_packs = Arc::new(Mutex::new(VecDeque::default()));
        let epochs = Arc::new(EpochRecordDb::open(&base_path)?);
        Ok(Self { base_path, current_pack, latest_consensus, recent_packs, epochs })
    }

    /// Create a new empty consensus chain with a dummy epoch 0 pack ready.
    pub async fn new_for_test(
        base_path: PathBuf,
        committee: Committee,
    ) -> Result<ConsensusChain, ConsensusChainError> {
        let me = Self::new(base_path)?;
        let rec = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            final_consensus: BlockNumHash::new(1000, BlockHash::default()),
            ..Default::default()
        };
        me.new_epoch(rec.clone(), committee).await?;
        me.epochs().save_record(rec).await?;
        Ok(me)
    }

    /// Move the writable state to a new epoch.
    /// This will create a new pack file if needed.
    pub async fn new_epoch(
        &self,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<(), ConsensusChainError> {
        if previous_epoch.epoch != committee.epoch().saturating_sub(1) {
            return Err(ConsensusChainError::PrevCommitteeEpochMismatch);
        }
        if let Some(old_pack) = self.current_pack() {
            if old_pack.epoch() == committee.epoch() {
                return Ok(());
            }
            old_pack.persist().await?;
            let mut recents = self.recent_packs.lock();
            if recents.len() > Self::PACK_CACHE_SIZE {
                let _ = recents.pop_front();
            }
            recents.push_back(old_pack);
        }
        let pack = ConsensusPack::open_append(&self.base_path, previous_epoch, committee)?;
        pack.persist().await?; // Surface any open errors.
        *self.current_pack.lock() = Some(pack);
        Ok(())
    }

    /// Provide a reference to the epochs database.
    pub fn epochs(&self) -> &EpochRecordDb {
        &self.epochs
    }

    /// Populate an epoch pack from a stream.
    /// This will resolve once the stream has been written.
    pub async fn stream_import<R: AsyncRead + Unpin>(
        &self,
        stream: R,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        timeout: Duration,
    ) -> Result<(), ConsensusChainError> {
        let epoch = epoch_record.epoch;
        if let Ok(pack) = self.get_static(epoch).await {
            if let Some(last_header) = pack.latest_consensus_header().await {
                if epoch_record.final_consensus.number == last_header.number
                    && epoch_record.final_consensus.hash == last_header.digest()
                {
                    // If we already have a complete pack file then we are done, no need to
                    // stream...
                    return Ok(());
                }
            }
        }
        // Import path will use RAII to remove the import dir when we are done.
        let import_path = ImportPath::new(&self.base_path, epoch);
        // Store our files out of the way while we import so we don't use them until ready.
        let path = import_path.path();
        let res_pack =
            ConsensusPack::stream_import(path, stream, epoch, previous_epoch, timeout).await;
        match res_pack {
            Ok(pack) => {
                let base_dir = self.base_path.join(format!("epoch-{epoch}"));
                let path_base_dir = path.join(format!("epoch-{epoch}"));
                pack.persist().await?;
                match pack.latest_consensus_header().await {
                    Some(last_header) => {
                        if epoch_record.final_consensus.number != last_header.number
                            || epoch_record.final_consensus.hash != last_header.digest()
                        {
                            // Invalid final consensus header...
                            return Err(ConsensusChainError::InvalidImport);
                        }
                    }
                    None => {
                        // Missing a final consensus header...
                        return Err(ConsensusChainError::EmptyImport);
                    }
                }
                let mut current_pack = self.current_pack.lock();
                let replace_current = if let Some(current_pack) = &*current_pack {
                    current_pack.epoch() == epoch
                } else {
                    false
                };
                if replace_current {
                    *current_pack = None;
                }
                drop(pack);
                drop(current_pack);
                // Make sure we don't have any cruft in the final dir.
                if std::fs::exists(&base_dir).unwrap_or_default() {
                    // If this exists it is incomplete (see check at start of function).
                    // This remove will leave a tiny window before the rename where it is
                    // not available.  This may produce errors that should be handled correctly if
                    // so.
                    let _ = std::fs::remove_dir_all(&base_dir);
                }
                let rename_err = std::fs::rename(&path_base_dir, &base_dir);
                // Invalidate the cache AFTER the rename so a concurrent get_static that
                // missed the cache and opened FDs on the old (now-unlinked) inode cannot
                // leave a stale entry behind for other callers — any entry cached during
                // the race is purged here. Readers after this point fall through and
                // see the new on-disk pack.
                self.recent_packs.lock().retain(|p| p.epoch() != epoch);
                rename_err?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Return a stream reader for the log file of epoch.
    /// Verifies the epoch pack is complete or will return an error.
    pub async fn get_epoch_stream(
        &self,
        epoch: Epoch,
    ) -> Result<Box<dyn ReadStream>, ConsensusChainError> {
        if let Ok(pack) = self.get_static(epoch).await {
            if let Some((epoch_record, _)) = self.epochs().get_epoch_by_number(epoch).await {
                match pack.latest_consensus_header().await {
                    Some(last_header) => {
                        if epoch_record.final_consensus.number == last_header.number
                            && epoch_record.final_consensus.hash == last_header.digest()
                        {
                            drop(pack);
                            // Remove the other open file.
                            // Should not matter a "complete" pack file should not be changed or
                            // moved again.
                            let base_dir = self.base_path.join(format!("epoch-{epoch}"));
                            let stream = AsyncFile::open(base_dir.join(DATA_NAME)).await?;
                            Ok(Box::new(stream))
                        } else {
                            Err(ConsensusChainError::StreamUnavailable)
                        }
                    }
                    None => Err(ConsensusChainError::StreamUnavailable),
                }
            } else {
                Err(ConsensusChainError::StreamUnavailable)
            }
        } else {
            Err(ConsensusChainError::StreamUnavailable)
        }
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file for the
    /// current epoch. This should be called "in-order" as consensus is executed.
    pub async fn save_consensus_output(
        &self,
        consensus: ConsensusOutput,
    ) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            if consensus.number() > self.latest_consensus.number() {
                let epoch = consensus.sub_dag().leader_epoch();
                let number = consensus.number();
                pack.save_consensus_output(consensus).await?;
                self.latest_consensus.update(epoch, number).await;
            }
            Ok(())
        } else {
            Ok(()) // If no current then this is a no-op.
        }
    }

    /// Load and return the consensus output from the current epoch.
    pub async fn get_consensus_output_current(
        &self,
        number: u64,
    ) -> Result<ConsensusOutput, ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            Ok(pack.get_consensus_output(number).await?)
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            if epoch == pack.epoch() {
                return Ok(pack.consensus_header_by_digest(digest).await);
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.consensus_header_by_digest(digest).await)
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Retrieve a consensus header by number.
    pub async fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        let epoch = self.epochs.number_to_epoch(number);
        if let Some(pack) = &self.current_pack() {
            if epoch == pack.epoch() {
                return Ok(Some(pack.consensus_header_by_number(number).await?));
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(Some(pack.consensus_header_by_number(number).await?))
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Retrieve the last known ConsensusHeader that was executed.
    pub async fn consensus_header_latest(
        &self,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        self.latest_consensus_header_from_pack(self.latest_consensus.epoch()).await
    }

    /// Return the last consensus number that was processed.
    pub fn latest_consensus_number(&self) -> u64 {
        self.latest_consensus.number()
    }

    /// Return the last consensus epoch that was processed.
    pub fn latest_consensus_epoch(&self) -> Epoch {
        self.latest_consensus.epoch()
    }

    /// Resolve when the current epoch is fully persisted to storage.
    pub async fn persist_current(&self) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            pack.persist().await?;
        }
        self.latest_consensus.persist().await;
        Ok(())
    }

    /// Return the latest consensus header for `epoch` by reading directly from the pack index,
    /// bypassing the slot files (LatestConsensus). This is always consistent with
    /// read_last_committed and should be used during startup recovery.
    pub async fn latest_consensus_header_from_pack(
        &self,
        epoch: Epoch,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            if pack.epoch() == epoch {
                return Ok(pack.latest_consensus_header().await);
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.latest_consensus_header().await)
        } else {
            Ok(None)
        }
    }

    /// Read the last committed rounds for authorities from an epoch.
    pub async fn read_last_committed(&self, epoch: Epoch) -> HashMap<AuthorityIdentifier, Round> {
        if let Some(pack) = &self.current_pack() {
            if pack.epoch() == epoch {
                return pack.read_last_committed().await;
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            pack.read_last_committed().await
        } else {
            HashMap::new()
        }
    }

    /// Read the final committed sub dag with final reputation scores.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Option<Arc<CommittedSubDag>> {
        if let Some(pack) = &self.current_pack() {
            if pack.epoch() == epoch {
                return pack.read_latest_commit_with_final_reputation_scores().await;
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            pack.read_latest_commit_with_final_reputation_scores().await
        } else {
            None
        }
    }

    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    pub async fn write_subdag_for_test(&self, number: u64, sub_dag: Arc<CommittedSubDag>) {
        let output = ConsensusOutput::new(
            sub_dag,
            BlockHash::default(),
            number,
            false,
            VecDeque::new(),
            Vec::new(),
        );
        self.save_consensus_output(output)
            .await
            .expect("error saving a consensus output to persistant storage!");
    }

    /// True if the current epoch pack contains the batch for digest.
    pub async fn contains_current_batch(&self, digest: BlockHash) -> bool {
        if let Some(pack) = &self.current_pack() {
            pack.contains_batch(digest).await
        } else {
            false
        }
    }

    /// Return a vector of batches matching the provided digests (if found).
    pub async fn get_batches(
        &self,
        epoch: Epoch,
        digests: impl Iterator<Item = &BlockHash>,
    ) -> Vec<Batch> {
        let mut result = Vec::new();
        if let Ok(pack) = self.get_static(epoch).await {
            for digest in digests {
                if let Some(batch) = pack.batch(*digest).await {
                    result.push(batch);
                }
            }
        }
        result
    }

    /// Count leaders in this pack (in rewards_counter) lower than last_executed_round.
    /// This works on the current epoch/pack.
    pub async fn count_leaders(
        &self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            Ok(pack.count_leaders(last_executed_round, rewards_counter).await?)
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Return a clone of the current pack.
    fn current_pack(&self) -> Option<ConsensusPack> {
        self.current_pack.lock().clone()
    }

    /// Get a static pack file from the cache if available or create and cache if not.
    async fn get_static(&self, epoch: Epoch) -> Result<ConsensusPack, PackError> {
        if let Some(pack) = self.current_pack() {
            if pack.epoch() == epoch {
                return Ok(pack.clone());
            }
        }
        {
            let mut recents = self.recent_packs.lock();
            for p in recents.iter() {
                if p.epoch() == epoch {
                    return Ok(p.clone());
                }
            }
            if recents.len() > Self::PACK_CACHE_SIZE {
                let _ = recents.pop_front();
            }
        }
        let pack = ConsensusPack::open_static(&self.base_path, epoch);
        pack.persist().await?; // Surface any open errors.
        self.recent_packs.lock().push_back(pack.clone());
        Ok(pack)
    }
}

#[derive(Debug)]
pub enum ConsensusChainError {
    PackError(PackError),
    NoCurrentEpoch,
    IO(std::io::Error),
    EpochMismatch,
    PrevCommitteeEpochMismatch,
    CrcError,
    EpochDbError(EpochDbError),
    EmptyImport,
    InvalidImport,
    StreamUnavailable,
}

impl Error for ConsensusChainError {}
impl Display for ConsensusChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusChainError::PackError(e) => write!(f, "Pack Error: {e}"),
            ConsensusChainError::NoCurrentEpoch => write!(f, "No current epoch set"),
            ConsensusChainError::IO(e) => write!(f, "IO Error: {e}"),
            ConsensusChainError::EpochMismatch => {
                write!(f, "Current epoch does not contain the latest consensus header")
            }
            ConsensusChainError::PrevCommitteeEpochMismatch => {
                write!(f, "Current committee epoch and previous epoch not in sync")
            }
            ConsensusChainError::CrcError => write!(f, "Crc error"),
            ConsensusChainError::EpochDbError(e) => write!(f, "Epoch DB Error: {e}"),
            ConsensusChainError::EmptyImport => write!(f, "No consensus in imported pack file"),
            ConsensusChainError::InvalidImport => {
                write!(f, "Bad final consensus in imported pack file")
            }
            ConsensusChainError::StreamUnavailable => {
                write!(f, "Incomplete data to stream a pack file")
            }
        }
    }
}

impl From<PackError> for ConsensusChainError {
    fn from(value: PackError) -> Self {
        Self::PackError(value)
    }
}

impl From<std::io::Error> for ConsensusChainError {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<EpochDbError> for ConsensusChainError {
    fn from(value: EpochDbError) -> Self {
        Self::EpochDbError(value)
    }
}

/// Helper to create the stream import dir and remove on Drop.
struct ImportPath {
    path: PathBuf,
}

impl ImportPath {
    /// New ImportPath rooted at base_path.
    fn new(base_path: &Path, epoch: Epoch) -> Self {
        // Store our files out of the way while we import so we don't use them until ready.
        let path = base_path.join(format!("import-{epoch}"));
        // We need to start with a clean import dir since we do not restart.
        // Note, this should not exist but just in case...
        let _ = std::fs::remove_dir_all(&path);
        Self { path }
    }

    /// Return the contained path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ImportPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::consensus::{ConsensusSlot, LatestConsensus};
    use std::{sync::Arc, time::Duration};

    use tn_types::{test_genesis, BlockHash, BlockNumHash, Epoch, EpochRecord, Hash as _, B256};

    use crate::{
        consensus::ConsensusChain,
        consensus_pack::test::{compare_outputs, make_test_output},
        mem_db::MemDatabase,
    };
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;

    #[tokio::test]
    async fn test_consensus_store_latest_consensus() {
        let temp_dir = TempDir::with_prefix("test_latest_consensus").unwrap();
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch(), 0);
        assert_eq!(latest.number(), 0);
        assert_eq!(latest.current_slot(), ConsensusSlot::Slot2);
        latest.update(1, 10).await;
        assert_eq!(latest.epoch(), 1);
        assert_eq!(latest.number(), 10);
        assert_eq!(latest.current_slot(), ConsensusSlot::Slot1);
        latest.update(2, 20).await;
        assert_eq!(latest.epoch(), 2);
        assert_eq!(latest.number(), 20);
        assert_eq!(latest.current_slot(), ConsensusSlot::Slot2);
        latest.persist().await;
        drop(latest);
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch(), 2);
        assert_eq!(latest.number(), 20);
        assert_eq!(latest.current_slot(), ConsensusSlot::Slot2);
    }

    #[tokio::test]
    async fn test_consensus_store_db_stream() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        // Create and load some data in initial file.
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 1000;
        let mut outputs = Vec::new();
        let mut parent = BlockHash::default();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
        }
        let last = outputs.last().unwrap();
        let mut epoch_record = previous_epoch.clone();
        epoch_record.final_consensus = BlockNumHash::new(last.number(), last.digest().into());
        for i in 0..num_outputs {
            let output_db =
                consensus_chain.get_consensus_output_current(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }

        consensus_chain.persist_current().await.expect("persist");
        //drop(consensus_chain);

        let temp_dir2 = TempDir::with_prefix("test_consensus_pack2").expect("temp dir");
        let consensus_chain2 = ConsensusChain::new(temp_dir2.path().to_owned()).unwrap();
        consensus_chain.epochs().save_record(epoch_record.clone()).await.expect("save epoch");
        let stream = consensus_chain.get_epoch_stream(0).await.unwrap();
        consensus_chain2
            .stream_import(stream, &epoch_record, &previous_epoch, Duration::from_secs(5))
            .await
            .unwrap();
        consensus_chain2.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        for i in 0..num_outputs {
            let output_db =
                consensus_chain2.get_consensus_output_current(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }
    }

    #[tokio::test]
    async fn test_consensus_store_general() {
        let temp_dir = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        // Create and load some data in initial file.
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 100;
        let mut outputs = Vec::new();
        let mut parent = BlockHash::default();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            let last_header = consensus_output.consensus_header();
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
            let latest = consensus_chain.consensus_header_latest().await.unwrap().unwrap();
            assert_eq!(last_header.digest(), latest.digest(), "latest header mismatch {i}");
        }

        let previous_epoch = EpochRecord {
            // If we can't find the recort then this we should be starting at epoch 0- use this
            // filler.
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            parent_hash: previous_epoch.digest(),
            final_consensus: BlockNumHash { number: 100, hash: BlockHash::default() },
            ..Default::default()
        };
        let committee = committee.advance_epoch_for_test(1);
        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        consensus_chain.epochs().save_record(previous_epoch.clone()).await.unwrap();

        for i in num_outputs..(num_outputs * 2) {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            let last_header = consensus_output.consensus_header();
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
            let latest = consensus_chain
                .consensus_header_latest()
                .await
                .expect(&format!("to have latest {i}"))
                .unwrap();
            assert_eq!(last_header.digest(), latest.digest(), "latest header mismatch {i}");
        }

        let previous_epoch = EpochRecord {
            // If we can't find the recort then this we should be starting at epoch 0- use this
            // filler.
            epoch: 1,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            parent_hash: previous_epoch.digest(),
            final_consensus: BlockNumHash { number: 200, hash: BlockHash::default() },
            ..Default::default()
        };
        let committee = committee.advance_epoch_for_test(2);
        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        consensus_chain.epochs().save_record(previous_epoch.clone()).await.unwrap();

        for i in (num_outputs * 2)..(num_outputs * 3) {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            let last_header = consensus_output.consensus_header();
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
            //consensus_chain.persist_current().await.expect(&format!("Failed to save on {i}"));
            let latest = consensus_chain.consensus_header_latest().await.unwrap().unwrap();
            assert_eq!(last_header.digest(), latest.digest(), "latest header mismatch {i}");
        }

        for i in 0..(num_outputs * 3) {
            let header_db = consensus_chain
                .consensus_header_by_number(i as u64 + 1)
                .await
                .expect(&format!("Failed to get header by number on {i}"))
                .unwrap();
            let output = outputs.get(i as usize).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }

        consensus_chain.persist_current().await.expect("persist chain");
        drop(consensus_chain);
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        consensus_chain.epochs().save_record(previous_epoch.clone()).await.unwrap();

        // Check that last consenus held over a DB shutdown/restart.
        let last_header = outputs.last().unwrap().consensus_header();
        let latest = consensus_chain.consensus_header_latest().await.unwrap().unwrap();
        assert_eq!(last_header.digest(), latest.digest(), "latest header mismatch after reload");

        // Test that all our outputs are still good.
        for i in 0..(num_outputs * 3) {
            let header_db = consensus_chain
                .consensus_header_by_number(i as u64 + 1)
                .await
                .unwrap()
                .expect(&format!("something on {i}"));
            let output = outputs.get(i as usize).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }
        // Now by digest
        for i in 0..(num_outputs * 3) {
            let epoch = (i / num_outputs) as Epoch;
            let digest: B256 = outputs[i].digest().into();
            let header_db =
                consensus_chain.consensus_header_by_digest(epoch, digest).await.unwrap().unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }

        // Now with epochs.
        for i in 0..(num_outputs * 3) {
            let epoch = i / num_outputs;
            let header_db = consensus_chain
                .consensus_header_by_number(i as u64 + 1)
                .await
                .expect(&format!("failed to get header by number epoch {epoch}, {i}"))
                .unwrap();
            let output = outputs.get(i as usize).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }
        for i in 0..(num_outputs * 3) {
            let epoch = i / num_outputs;
            let digest: B256 = outputs[i].digest().into();
            let header_db = consensus_chain
                .consensus_header_by_digest(epoch as u32, digest)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }
    }
}
