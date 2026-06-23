//! Wrap access to the epoch consensus files into a single interface.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use parking_lot::Mutex;
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, CommittedSubDag,
    Committee, ConsensusChainReader, ConsensusChainWriter, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusNumHash, ConsensusOutput, Epoch, EpochRecord, ReadStream, Round,
};
use tokio::{
    fs::File as AsyncFile,
    io::AsyncRead,
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
};
use tracing::{error, warn};

use crate::{
    consensus_pack::{ConsensusPack, PackError, DATA_NAME},
    epoch_records::{EpochDbError, EpochRecordDb},
};

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
        // A torn or corrupt slot must not be fatal: the slots are a double-buffered hint and
        // the pack files are ground truth, so fall back to the other slot (or a fresh (0, 0))
        // rather than failing to open the chain.  Failing here would panic the node at startup
        // on a single damaged slot, defeating the whole point of having two of them.
        let (slot1_epoch, slot1_number) = Self::read_slot(&mut slot1).unwrap_or_else(|e| {
            warn!(target: "consensus_chain", ?e, "consensus_slot1 unreadable; falling back to the other slot");
            (0, 0)
        });
        let (slot2_epoch, slot2_number) = Self::read_slot(&mut slot2).unwrap_or_else(|e| {
            warn!(target: "consensus_chain", ?e, "consensus_slot2 unreadable; falling back to the other slot");
            (0, 0)
        });

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
    /// Serializes epoch-{N} directory mutation between `new_epoch` (open/append)
    /// and `stream_import` (remove+rename), preventing a transient-ENOENT crash.
    ///
    /// Both critical sections cross `.await` points, so this is a `tokio::sync::Mutex`
    /// (not the `parking_lot::Mutex` used for the other fields). Always acquired *before*
    /// `current_pack`/`recent_packs` to keep a single lock order and avoid deadlock.
    pack_install: Arc<tokio::sync::Mutex<()>>,
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
        let pack_install = Arc::new(tokio::sync::Mutex::new(()));
        Ok(Self { base_path, current_pack, latest_consensus, recent_packs, epochs, pack_install })
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
            final_consensus: ConsensusNumHash::new(1000, ConsensusHeaderDigest::default()),
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
        // Serialize the open/append + current_pack swap against stream_import's
        // remove+rename of the same epoch-{N} directory. Held across the whole
        // function (open_append is local file creation — fast). Acquired before
        // current_pack/recent_packs to preserve lock order.
        let _install = self.pack_install.lock().await;
        if previous_epoch.epoch != committee.epoch().saturating_sub(1) {
            return Err(ConsensusChainError::PrevCommitteeEpochMismatch);
        }
        if let Some(old_pack) = self.current_pack() {
            if old_pack.epoch() == committee.epoch() {
                return Ok(());
            }
            old_pack.persist().await?;
            let mut recents = self.recent_packs.lock();
            // Evict before pushing so the cache stays capped at PACK_CACHE_SIZE.
            if recents.len() >= Self::PACK_CACHE_SIZE {
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

    /// Return true if this process is already streaming this epoch.
    pub fn already_streaming_epoch(&self, epoch: Epoch) -> bool {
        ImportPath::is_streaming(&self.base_path, epoch)
    }

    /// Populate an epoch pack from a stream.
    /// This will resolve once the stream has been written.
    /// Note, if called on an epoch while streaming that epoch will just return Ok(()).
    pub async fn stream_import<R: AsyncRead + Unpin>(
        &self,
        stream: R,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        timeout: Duration,
    ) -> Result<(), ConsensusChainError> {
        let epoch = epoch_record.epoch;
        let epoch_final_hash = epoch_record.final_consensus.hash;
        if let Ok(pack) = self.get_static(epoch).await {
            if let Some(last_header) = pack.latest_consensus_header().await {
                if epoch_record.final_consensus.number == last_header.number
                    && epoch_final_hash == last_header.digest()
                {
                    // If we already have a complete pack file then we are done, no need to
                    // stream...
                    return Ok(());
                }
            }
        }
        // Import path will use RAII to remove the import dir when we are done.
        let Some(import_path) = ImportPath::new(&self.base_path, epoch)? else {
            // If this returns None then we are already importing this epoch.
            return Ok(());
        };
        // Store our files out of the way while we import so we don't use them until ready.
        let path = import_path.path();
        let res_pack = ConsensusPack::stream_import(
            path,
            stream,
            epoch,
            previous_epoch,
            epoch_record.final_consensus.number,
            timeout,
        )
        .await;
        match res_pack {
            Ok(pack) => {
                let base_dir = self.base_path.join(format!("epoch-{epoch}"));
                let path_base_dir = path.join(format!("epoch-{epoch}"));
                pack.persist().await?;
                match pack.latest_consensus_header().await {
                    Some(last_header) => {
                        // The chain was verified as it was streamed.  So if the final block matches
                        // the expected final_consensus then the entire pack
                        // file should be valid.
                        if epoch_record.final_consensus.number != last_header.number
                            || epoch_final_hash != last_header.digest()
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
                // Acquire the install lock only now — after the (multi-second) network
                // download has finished writing into the temp import dir. It must NOT wrap
                // the download (that would block unrelated epoch transitions on network I/O).
                // Held through the remove+rename and cache invalidation below so new_epoch's
                // open_append cannot observe the transient window where epoch-{N} is unlinked.
                let _install = self.pack_install.lock().await;
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
                        let epoch_final_hash = epoch_record.final_consensus.hash;
                        if epoch_record.final_consensus.number == last_header.number
                            && epoch_final_hash == last_header.digest()
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
        let number = consensus.number();
        if number > self.latest_consensus.number() {
            let epoch = consensus.sub_dag().leader_epoch();
            if let Some(pack) = &self.current_pack() {
                if epoch != pack.epoch() {
                    // The output's epoch does not match the current pack. Saving it would either
                    // corrupt this pack or poison its async error channel. The pack
                    // layer also rejects this (defense in depth), but the reject is asynchronous so
                    // we must guard here to avoid advancing latest_consensus to a wrong-epoch
                    // pointer for data that was never persisted.
                    // This is an error and should not happen on a properly working node.
                    error!(target: "consensus-chain", epoch, pack_epoch = pack.epoch(), number, "Refused to save consensus output: epoch does not match the current pack.");
                    return Err(ConsensusChainError::InvalidPackEpoch(pack.epoch(), epoch));
                } else {
                    pack.save_consensus_output(consensus).await?;
                    self.latest_consensus.update(epoch, number).await;
                }
            } else if let Ok(pack) = self.get_static(epoch).await {
                // We may be replaying consensus from old epochs and not have a current pack to save
                // too.
                if pack.contains_consensus_header_number(number).await.unwrap_or_default() {
                    // We should have this saved already if no current_pack but let's confirm before
                    // we save the latest.
                    self.latest_consensus.update(epoch, number).await;
                } else {
                    // Note this should not happen but don't want to kill node on an error since it
                    // will hopefully self correct soon.
                    warn!(target: "consensus-chain", epoch, number, "Failed to update latest consensus, data not in expected pack file.");
                }
            } else {
                // Note this should not happen but don't want to kill node on an error since it will
                // hopefully self correct soon.
                warn!(target: "consensus-chain", epoch, number, "Failed to update latest consensus, pack file that should contain data is missing.");
            }
        }
        Ok(())
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
        digest: ConsensusHeaderDigest,
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

    /// Retrieve the raw consensus output bytes by number.
    pub async fn consensus_output_bytes_by_number(
        &self,
        number: u64,
    ) -> Result<Option<Vec<u8>>, ConsensusChainError> {
        let epoch = self.epochs.number_to_epoch(number);
        if let Some(pack) = &self.current_pack() {
            if epoch == pack.epoch() {
                return Ok(Some(pack.get_consensus_output_bytes(number).await?));
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(Some(pack.get_consensus_output_bytes(number).await?))
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Return true if we have a complete pack file for epoch_record.
    pub async fn is_epoch_complete(&self, epoch_record: &EpochRecord) -> bool {
        match self.consensus_header_by_number(epoch_record.final_consensus.number).await {
            Ok(result) => result.is_some(),
            Err(e) => {
                error!(target: "consensus-chain", epoch=?epoch_record.epoch, "DB error checking epoch completeness: {e}");
                false
            }
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
    pub async fn read_last_committed(
        &self,
        epoch: Epoch,
    ) -> Result<HashMap<AuthorityIdentifier, Round>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            if pack.epoch() == epoch {
                return Ok(pack.read_last_committed().await?);
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.read_last_committed().await?)
        } else {
            Ok(HashMap::new())
        }
    }

    /// Read the final committed sub dag with final reputation scores.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Result<Option<CommittedSubDag>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack() {
            if pack.epoch() == epoch {
                return Ok(pack.read_latest_commit_with_final_reputation_scores().await?);
            }
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.read_latest_commit_with_final_reputation_scores().await?)
        } else {
            Ok(None)
        }
    }

    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    pub async fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag) {
        let output = ConsensusOutput::new(
            sub_dag,
            ConsensusHeaderDigest::default(),
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
            // Evict before the open+push below so the cache stays capped at PACK_CACHE_SIZE.
            if recents.len() >= Self::PACK_CACHE_SIZE {
                let _ = recents.pop_front();
            }
        }
        let pack = ConsensusPack::open_static(&self.base_path, epoch)?;
        self.recent_packs.lock().push_back(pack.clone());
        Ok(pack)
    }
}

impl ConsensusChainReader for ConsensusChain {
    async fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: ConsensusHeaderDigest,
    ) -> eyre::Result<Option<ConsensusHeader>> {
        ConsensusChain::consensus_header_by_digest(self, epoch, digest).await.map_err(Into::into)
    }

    async fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> eyre::Result<Option<ConsensusHeader>> {
        ConsensusChain::consensus_header_by_number(self, number).await.map_err(Into::into)
    }

    async fn consensus_output_bytes_by_number(&self, number: u64) -> eyre::Result<Option<Vec<u8>>> {
        ConsensusChain::consensus_output_bytes_by_number(self, number).await.map_err(Into::into)
    }

    async fn consensus_header_latest(&self) -> eyre::Result<Option<ConsensusHeader>> {
        ConsensusChain::consensus_header_latest(self).await.map_err(Into::into)
    }

    async fn latest_consensus_header_from_pack(
        &self,
        epoch: Epoch,
    ) -> eyre::Result<Option<ConsensusHeader>> {
        ConsensusChain::latest_consensus_header_from_pack(self, epoch).await.map_err(Into::into)
    }

    fn latest_consensus_number(&self) -> u64 {
        ConsensusChain::latest_consensus_number(self)
    }

    fn latest_consensus_epoch(&self) -> Epoch {
        ConsensusChain::latest_consensus_epoch(self)
    }

    async fn read_last_committed(
        &self,
        epoch: Epoch,
    ) -> eyre::Result<HashMap<AuthorityIdentifier, Round>> {
        Ok(ConsensusChain::read_last_committed(self, epoch).await?)
    }

    async fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> eyre::Result<Option<CommittedSubDag>> {
        Ok(ConsensusChain::read_latest_commit_with_final_reputation_scores(self, epoch).await?)
    }

    async fn get_consensus_output_current(&self, number: u64) -> eyre::Result<ConsensusOutput> {
        ConsensusChain::get_consensus_output_current(self, number).await.map_err(Into::into)
    }

    async fn is_epoch_complete(&self, epoch_record: &EpochRecord) -> bool {
        ConsensusChain::is_epoch_complete(self, epoch_record).await
    }

    async fn contains_current_batch(&self, digest: BlockHash) -> bool {
        ConsensusChain::contains_current_batch(self, digest).await
    }

    async fn get_batches<'a>(
        &'a self,
        epoch: Epoch,
        digests: impl Iterator<Item = &'a BlockHash> + Send + 'a,
    ) -> Vec<Batch> {
        ConsensusChain::get_batches(self, epoch, digests).await
    }

    async fn count_leaders(
        &self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<()> {
        ConsensusChain::count_leaders(self, last_executed_round, rewards_counter)
            .await
            .map_err(Into::into)
    }

    async fn get_epoch_stream(&self, epoch: Epoch) -> eyre::Result<Box<dyn ReadStream>> {
        ConsensusChain::get_epoch_stream(self, epoch).await.map_err(Into::into)
    }

    fn already_streaming_epoch(&self, epoch: Epoch) -> bool {
        ConsensusChain::already_streaming_epoch(self, epoch)
    }
}

impl ConsensusChainWriter for ConsensusChain {
    async fn save_consensus_output(&self, consensus: ConsensusOutput) -> eyre::Result<()> {
        ConsensusChain::save_consensus_output(self, consensus).await.map_err(Into::into)
    }

    async fn new_epoch(
        &self,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> eyre::Result<()> {
        ConsensusChain::new_epoch(self, previous_epoch, committee).await.map_err(Into::into)
    }

    async fn stream_import<R: AsyncRead + Unpin + Send>(
        &self,
        stream: R,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        timeout: Duration,
    ) -> eyre::Result<()> {
        ConsensusChain::stream_import(self, stream, epoch_record, previous_epoch, timeout)
            .await
            .map_err(Into::into)
    }

    async fn persist_current(&self) -> eyre::Result<()> {
        ConsensusChain::persist_current(self).await.map_err(Into::into)
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
    InvalidPackEpoch(Epoch, Epoch),
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
            ConsensusChainError::InvalidPackEpoch(pack_epoch, epoch) => {
                write!(f, "Tried to save an output from epoch {epoch} into the current pack epoch {pack_epoch}")
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

/// Lock to prevent races when creating ImportPath's.
static IMPORT_PATH_LOCK: Mutex<()> = Mutex::new(());

/// Helper to create the stream import dir and remove on Drop.
struct ImportPath {
    path: PathBuf,
}

impl ImportPath {
    /// New ImportPath rooted at base_path.
    /// Returns None if this process is already importing for this epoch.
    fn new(base_path: &Path, epoch: Epoch) -> io::Result<Option<Self>> {
        // Store our files out of the way while we import so we don't use them until ready.
        let path = base_path.join(format!("import-{epoch}"));
        let pid = std::process::id();
        let proc_path = path.join(format!("{pid}.inproc"));
        // Grab the single lock so we can avoid races on the off chance we try to
        // import the same epoch twice at the same time.
        let _guard = IMPORT_PATH_LOCK.lock();
        if proc_path.exists() {
            // This process is already streaming this pack file so just return.
            return Ok(None);
        }
        // We need to start with a clean import dir since we do not restart.
        // Note, this should not exist but just in case...
        let _ = std::fs::remove_dir_all(&path);
        // Create a sentinel for this process to avoid double streams.
        let _ = std::fs::create_dir_all(&path);
        File::create(proc_path)?;
        Ok(Some(Self { path }))
    }

    /// True if this epoch is already being streamed.
    fn is_streaming(base_path: &Path, epoch: Epoch) -> bool {
        let pid = std::process::id();
        let path = base_path.join(format!("import-{epoch}")).join(format!("{pid}.inproc"));
        path.exists()
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
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tn_types::{
        test_genesis, ConsensusHeader, ConsensusHeaderDigest, ConsensusNumHash, Epoch, EpochRecord,
        Hash as _,
    };

    use crate::{
        consensus::{ConsensusChain, ConsensusChainError},
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

    /// A corrupt slot file must not fail to open the chain; the other (valid) slot is used.
    /// The slots are a double-buffered hint, so a single damaged slot must be recoverable
    /// rather than panicking the node at startup.
    #[tokio::test]
    async fn test_latest_consensus_recovers_from_corrupt_slot() {
        use std::{
            fs::OpenOptions,
            io::{Seek as _, SeekFrom, Write as _},
        };

        let temp_dir = TempDir::with_prefix("test_corrupt_slot").unwrap();
        {
            let latest = LatestConsensus::new(temp_dir.path()).unwrap();
            // Two updates so both slots hold data: slot1 = (1, 10), slot2 = (2, 20).
            latest.update(1, 10).await;
            latest.update(2, 20).await;
            latest.persist().await;
        }

        // Corrupt the slot holding the most recent value (slot2) by flipping a payload byte,
        // which breaks its CRC.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(temp_dir.path().join("consensus_slot2"))
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF]).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen: slot2 is unreadable (CRC fail) but must fall back to slot1's valid value
        // instead of erroring.
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch(), 1, "recovered epoch from the good slot");
        assert_eq!(latest.number(), 10, "recovered number from the good slot");

        // Corrupting the remaining slot too falls back to a fresh (0, 0).
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(temp_dir.path().join("consensus_slot1"))
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF]).unwrap();
            f.sync_all().unwrap();
        }
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch(), 0, "both slots corrupt -> fresh start");
        assert_eq!(latest.number(), 0, "both slots corrupt -> fresh start");
    }

    #[tokio::test]
    async fn test_save_consensus_output_wrong_epoch_rejected() {
        let temp_dir = TempDir::with_prefix("test_wrong_epoch").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save a few legitimate epoch-0 outputs.
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..3u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest().into();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        assert_eq!(consensus_chain.latest_consensus.number(), 3);
        assert_eq!(consensus_chain.latest_consensus.epoch(), 0);

        // Feed an output whose leader epoch is 1 while the current pack is still epoch 0.
        // It must be rejected with InvalidPackEpoch before latest_consensus advances or the data
        // is saved.
        let next_committee = committee.advance_epoch_for_test(1);
        let wrong = make_test_output(&next_committee, 0, chain.clone(), 4, parent);
        assert_eq!(wrong.sub_dag().leader_epoch(), 1, "test output must be from epoch 1");
        let err = consensus_chain
            .save_consensus_output(wrong)
            .await
            .expect_err("wrong-epoch output must be rejected");
        assert!(
            matches!(err, ConsensusChainError::InvalidPackEpoch(0, 1)),
            "expected InvalidPackEpoch(0, 1), got {err:?}"
        );

        assert_eq!(
            consensus_chain.latest_consensus.number(),
            3,
            "latest_consensus must not advance on a wrong-epoch output"
        );
        assert_eq!(consensus_chain.latest_consensus.epoch(), 0);
        assert!(
            consensus_chain.get_consensus_output_current(4).await.is_err(),
            "wrong-epoch output must not be persisted to the epoch-0 pack"
        );
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
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest().into();
            outputs.push(consensus_output.clone());
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
        }
        let last = outputs.last().unwrap();
        let mut epoch_record = previous_epoch.clone();
        epoch_record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());
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
    async fn test_consensus_output_bytes_by_number() {
        use crate::{archive::pack::PackCompression, consensus_pack::bytes_to_output};
        use std::io::Cursor;
        use tokio::io::BufReader;

        let temp_dir = TempDir::with_prefix("test_output_bytes").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save some outputs, keeping the originals to compare against.
        let num_outputs = 10;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest().into();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }

        // Each saved number returns Some(bytes) that decode back to the original output.
        for i in 0..num_outputs {
            let number = i as u64 + 1;
            let bytes = consensus_chain
                .consensus_output_bytes_by_number(number)
                .await
                .expect("query ok")
                .expect("bytes present");
            assert!(!bytes.is_empty(), "bytes for {number} should not be empty");
            // Packs are always written with ZStd, mirror get_consensus_output's decode path.
            let reader = BufReader::new(Cursor::new(bytes));
            let decoded =
                bytes_to_output(reader, PackCompression::ZStd, Duration::from_secs(5), &committee)
                    .await
                    .expect("decode output bytes");
            compare_outputs(&decoded, &outputs[i]);
        }

        // A number below the pack's start is out of range and must error.
        assert!(
            consensus_chain.consensus_output_bytes_by_number(0).await.is_err(),
            "number below start must error"
        );

        // A fresh chain with no epoch opened has no data and returns Ok(None).
        let empty_dir = TempDir::with_prefix("test_output_bytes_empty").expect("temp dir");
        let empty_chain = ConsensusChain::new(empty_dir.path().to_owned()).unwrap();
        assert!(
            empty_chain.consensus_output_bytes_by_number(1).await.expect("query ok").is_none(),
            "missing data must return None"
        );
    }

    /// Regression test for the `pack_install` lock.
    ///
    /// A validator that restarts while behind runs two subsystems against the same
    /// on-disk `epoch-{N}` directory at once: the epoch-transition loop (`new_epoch` ->
    /// `open_append`, which creates/opens `epoch-{N}/data`) and state-sync (`stream_import`,
    /// which does `remove_dir_all(epoch-{N})` immediately followed by
    /// `rename(import/epoch-{N} -> epoch-{N})`). Without serialization, `new_epoch` can open
    /// `epoch-{N}/data` in the tiny window after the directory was removed and before the
    /// imported one is renamed into place, getting ENOENT and failing the epoch transition;
    /// `stream_import`'s `rename` can likewise fail with ENOTEMPTY if `new_epoch` re-created
    /// the directory inside that window.
    ///
    /// This drives both methods concurrently against the same epoch over many iterations,
    /// each on a fresh chain so the import always performs the full remove+rename rather
    /// than short-circuiting on an already-complete pack. It passes reliably with the lock
    /// and fails intermittently if the lock acquisition in either method is removed or
    /// reordered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_new_epoch_stream_import_race() {
        // Build a complete epoch-0 pack on a source chain to stream from each iteration.
        let source_dir = TempDir::with_prefix("test_race_source").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(source_dir.path().to_owned()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 50;
        let mut parent = ConsensusHeader::default().digest();
        let mut last = None;
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest().into();
            last = Some(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }
        source.persist_current().await.expect("persist");
        let last = last.expect("at least one output");
        let mut epoch_record = previous_epoch.clone();
        epoch_record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());
        source.epochs().save_record(epoch_record.clone()).await.expect("save epoch");

        let iterations = 50;
        for iter in 0..iterations {
            // A fresh target each iteration guarantees `stream_import` does the real
            // remove+rename instead of returning early on an existing complete pack.
            let target_dir = TempDir::with_prefix("test_race_target").expect("temp dir");
            let target = Arc::new(ConsensusChain::new(target_dir.path().to_owned()).unwrap());
            let stream = source.get_epoch_stream(0).await.expect("source epoch stream");

            // Hammer `new_epoch` for the whole duration of the single concurrent
            // `stream_import` below, clearing the cached pack before each call so it actually
            // runs `open_append` (rather than short-circuiting) and lands inside the import's
            // remove->rename window.
            let done = Arc::new(AtomicBool::new(false));
            let new_epoch_task = {
                let target = target.clone();
                let previous_epoch = previous_epoch.clone();
                let committee = committee.clone();
                let done = done.clone();
                tokio::spawn(async move {
                    let mut result = Ok(());
                    while !done.load(Ordering::Relaxed) {
                        *target.current_pack.lock() = None;
                        if let Err(e) =
                            target.new_epoch(previous_epoch.clone(), committee.clone()).await
                        {
                            result = Err(e);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    result
                })
            };

            let import_result = target
                .stream_import(stream, &epoch_record, &previous_epoch, Duration::from_secs(5))
                .await;
            done.store(true, Ordering::Relaxed);
            let new_epoch_result = new_epoch_task.await.expect("new_epoch task panicked");

            assert!(
                import_result.is_ok(),
                "stream_import lost the race with new_epoch on iteration {iter}: {import_result:?}"
            );
            assert!(
                new_epoch_result.is_ok(),
                "new_epoch lost the race with stream_import on iteration {iter}: {new_epoch_result:?}"
            );

            // The imported epoch-0 pack must be complete and readable after all the racing.
            *target.current_pack.lock() = None;
            let pack = target.get_static(0).await.expect("epoch-0 pack readable after race");
            let header = pack.latest_consensus_header().await.expect("epoch-0 has a final header");
            assert_eq!(header.number, epoch_record.final_consensus.number, "final header number");
            assert_eq!(header.digest(), epoch_record.final_consensus.hash, "final header digest");
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
        let mut parent = ConsensusHeaderDigest::default();
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
            final_consensus: ConsensusNumHash {
                number: 100,
                hash: ConsensusHeaderDigest::default(),
            },
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
            final_consensus: ConsensusNumHash {
                number: 200,
                hash: ConsensusHeaderDigest::default(),
            },
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
            let digest = outputs[i].digest();
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
            let digest = outputs[i].digest();
            let header_db = consensus_chain
                .consensus_header_by_digest(epoch as u32, digest)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }
    }
}
