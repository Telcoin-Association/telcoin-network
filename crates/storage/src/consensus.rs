//! Wrap access to the epoch consensus files into a single interface.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, BlockHash, CommittedSubDag, Committee,
    ConsensusHeader, ConsensusOutput, Epoch, EpochRecord, Round, B256,
};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

use crate::consensus_pack::{ConsensusPack, PackError, DATA_NAME};

pub trait ReadStream: Read + Seek + Send {}
impl ReadStream for File {}

/// Simple enum for which of two saved consensus states we are using.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConsensusSlot {
    Slot1,
    Slot2,
}

/// Manage and persist the latest consensus state.
#[derive(Debug, Clone)]
struct LatestConsensus {
    epoch: Epoch,
    number: u64,
    current_slot: ConsensusSlot,
    tx: Sender<LatestConsenusCommand>,
}

enum LatestConsenusCommand {
    Update(Epoch, u64, ConsensusSlot),
    Persist(oneshot::Sender<()>),
}

impl LatestConsensus {
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
        let mut buffer32_epoch = [0_u8; 4];
        let mut buffer32_crc = [0_u8; 4];
        let mut buffer64 = [0_u8; 8];
        let _ = slot1.read_exact(&mut buffer32_epoch);
        let _ = slot1.read_exact(&mut buffer64);
        let _ = slot1.read_exact(&mut buffer32_crc);
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer32_epoch);
        crc32_hasher.update(&buffer64);
        let crc32 = crc32_hasher.finalize();
        let crc32_read = u32::from_le_bytes(buffer32_crc);
        let (slot1_epoch, slot1_number) = if crc32 == crc32_read {
            (u32::from_le_bytes(buffer32_epoch), u64::from_le_bytes(buffer64))
        } else {
            (0, 0)
        };

        let _ = slot2.read_exact(&mut buffer32_epoch);
        let _ = slot2.read_exact(&mut buffer64);
        let _ = slot2.read_exact(&mut buffer32_crc);
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer32_epoch);
        crc32_hasher.update(&buffer64);
        let crc32 = crc32_hasher.finalize();
        let crc32_read = u32::from_le_bytes(buffer32_crc);
        let (slot2_epoch, slot2_number) = if crc32 == crc32_read {
            (u32::from_le_bytes(buffer32_epoch), u64::from_le_bytes(buffer64))
        } else {
            (0, 0)
        };

        let (tx, mut rx) = mpsc::channel(1000);
        let me = if slot1_epoch == slot2_epoch {
            if slot1_number > slot2_number {
                Self {
                    epoch: slot1_epoch,
                    number: slot1_number,
                    current_slot: ConsensusSlot::Slot1,
                    tx,
                }
            } else {
                Self {
                    epoch: slot2_epoch,
                    number: slot2_number,
                    current_slot: ConsensusSlot::Slot2,
                    tx,
                }
            }
        } else if slot1_epoch > slot2_epoch {
            Self {
                epoch: slot1_epoch,
                number: slot1_number,
                current_slot: ConsensusSlot::Slot1,
                tx,
            }
        } else {
            Self {
                epoch: slot2_epoch,
                number: slot2_number,
                current_slot: ConsensusSlot::Slot2,
                tx,
            }
        };
        std::thread::spawn(move || {
            while let Some(com) = rx.blocking_recv() {
                match com {
                    LatestConsenusCommand::Update(epoch, number, slot) => {
                        let f = match slot {
                            ConsensusSlot::Slot1 => &mut slot1,
                            ConsensusSlot::Slot2 => &mut slot2,
                        };
                        let _ = f.seek(SeekFrom::Start(0));
                        let _ = f.write_all(&epoch.to_le_bytes());
                        let _ = f.write_all(&number.to_le_bytes());
                        let mut crc32_hasher = crc32fast::Hasher::new();
                        crc32_hasher.update(&epoch.to_le_bytes());
                        crc32_hasher.update(&number.to_le_bytes());
                        let crc32 = crc32_hasher.finalize();
                        let _ = f.write_all(&crc32.to_le_bytes());
                        let _ = f.sync_all();
                    }
                    LatestConsenusCommand::Persist(tx) => {
                        let _ = tx.send(());
                    }
                }
            }
        });
        Ok(me)
    }

    async fn update(&mut self, epoch: Epoch, number: u64) {
        self.epoch = epoch;
        self.number = number;
        match self.current_slot {
            ConsensusSlot::Slot1 => self.current_slot = ConsensusSlot::Slot2,
            ConsensusSlot::Slot2 => self.current_slot = ConsensusSlot::Slot1,
        }
        let _ = self.tx.send(LatestConsenusCommand::Update(epoch, number, self.current_slot)).await;
    }

    async fn persist(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(LatestConsenusCommand::Persist(tx)).await;
        let _ = rx.await;
    }
}

#[derive(Debug, Clone)]
pub struct ConsensusChain {
    base_path: PathBuf,
    current_pack: Option<ConsensusPack>,
    latest_consensus: LatestConsensus,
    recent_packs: Arc<Mutex<VecDeque<ConsensusPack>>>,
}

impl ConsensusChain {
    /// How many recently opened pack files to maintain.
    const PACK_CACHE_SIZE: usize = 10;

    /// Create a new empty consensus chain.
    pub async fn new(base_path: PathBuf) -> Result<ConsensusChain, ConsensusChainError> {
        let latest_consensus = LatestConsensus::new(&base_path)?;
        // If we have a pack for the last epoch open it so we can read data early.
        let current_pack =
            ConsensusPack::open_append_exists(&base_path, latest_consensus.epoch).ok();
        let recent_packs = Arc::new(Mutex::new(VecDeque::default()));
        Ok(Self { base_path, current_pack, latest_consensus, recent_packs })
    }

    /// Create a new empty consensus chain with a dummy epoch 0 pack ready.
    pub async fn new_for_test(
        base_path: PathBuf,
        committee: Committee,
    ) -> Result<ConsensusChain, ConsensusChainError> {
        let mut me = Self::new(base_path).await?;
        me.new_epoch(
            EpochRecord {
                epoch: 0,
                committee: committee.bls_keys().iter().copied().collect(),
                next_committee: committee.bls_keys().iter().copied().collect(),
                ..Default::default()
            },
            committee,
        )
        .await?;
        Ok(me)
    }

    pub async fn new_epoch(
        &mut self,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<(), ConsensusChainError> {
        if previous_epoch.epoch != committee.epoch().saturating_sub(1) {
            return Err(ConsensusChainError::PrevCommitteeEpochMismatch);
        }
        if let Some(old_pack) = self.current_pack.take() {
            if old_pack.epoch() == committee.epoch() {
                self.current_pack = Some(old_pack);
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
        self.current_pack = Some(pack);
        Ok(())
    }

    /// Populate an epoch pack from a stream.
    /// This will resolve once the stream has been written.
    pub async fn stream_import<R: Read + Seek + Send + 'static>(
        &self,
        stream: R,
        epoch: Epoch,
        previous_epoch: EpochRecord,
    ) -> Result<(), ConsensusChainError> {
        let pack = ConsensusPack::stream_import(&self.base_path, stream, epoch, previous_epoch)?;
        Ok(pack.persist().await?)
    }

    /// Return a stream reader for the log file of epoch.
    pub async fn get_epoch_stream(
        &self,
        epoch: Epoch,
    ) -> Result<Box<dyn ReadStream>, ConsensusChainError> {
        let base_dir = self.base_path.join(format!("epoch-{epoch}"));
        let stream = File::open(base_dir.join(DATA_NAME))?;
        Ok(Box::new(stream))
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file for the
    /// current epoch. This should be called "in-order" as consensus is executed.
    pub async fn save_consensus_output(
        &mut self,
        consensus: ConsensusOutput,
    ) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            if consensus.number() > self.latest_consensus.number {
                self.latest_consensus
                    .update(consensus.sub_dag().leader_epoch(), consensus.number())
                    .await;
                pack.save_consensus_output(consensus).await?;
            } else {
                eprintln!(
                    "XXXX tried to save previous {} on {}",
                    consensus.number(),
                    self.latest_consensus.number
                );
            }
            Ok(())
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Load and return the consensus output from the current epoch.
    pub async fn get_consensus_output_current(
        &self,
        number: u64,
    ) -> Result<ConsensusOutput, ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            Ok(pack.get_consensus_output(number).await?)
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(
        &self,
        epoch: Option<Epoch>,
        digest: B256,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            if let Some(epoch) = epoch {
                if epoch == pack.epoch() {
                    return Ok(pack.consensus_header_by_digest(digest).await);
                }
            } else if let Some(header) = pack.consensus_header_by_digest(digest).await {
                return Ok(Some(header));
            }
        }
        if let Some(epoch) = epoch {
            if let Ok(pack) = self.get_static(epoch).await {
                Ok(pack.consensus_header_by_digest(digest).await)
            } else {
                // Don't have this epoch data.
                Ok(None)
            }
        } else {
            let mut epoch = self.current_pack.as_ref().map(|p| p.epoch());
            while let Some(try_epoch) = epoch {
                if let Ok(pack) = self.get_static(try_epoch).await {
                    if let Some(header) = pack.consensus_header_by_digest(digest).await {
                        return Ok(Some(header));
                    }
                }
                epoch = if try_epoch > 0 { Some(try_epoch - 1) } else { None };
            }
            Ok(None)
        }
    }

    /// Retrieve a consensus header by number.
    pub async fn consensus_header_by_number(
        &self,
        epoch: Option<Epoch>,
        number: u64,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            if let Some(epoch) = epoch {
                if epoch == pack.epoch() {
                    return Ok(Some(pack.consensus_header_by_number(number).await?));
                }
            } else if let Ok(header) = pack.consensus_header_by_number(number).await {
                return Ok(Some(header));
            }
        }
        if let Some(epoch) = epoch {
            let pack = self.get_static(epoch).await?;
            Ok(Some(pack.consensus_header_by_number(number).await?))
        } else {
            let mut epoch = self.current_pack.as_ref().map(|p| p.epoch());
            // XXXX- TODO this is dumb, we should be able to use epoch records to deduce the epoch
            // for number.
            while let Some(try_epoch) = epoch {
                if let Ok(pack) = self.get_static(try_epoch).await {
                    if let Ok(header) = pack.consensus_header_by_number(number).await {
                        return Ok(Some(header));
                    }
                }
                epoch = if try_epoch > 0 { Some(try_epoch - 1) } else { None };
            }
            Ok(None)
        }
    }

    /// Retrieve the last known ConsensusHeader that was executed.
    pub async fn consensus_header_latest(
        &self,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            if self.latest_consensus.epoch == pack.epoch() {
                Ok(Some(pack.consensus_header_by_number(self.latest_consensus.number).await?))
            } else {
                let pack = self.get_static(self.latest_consensus.epoch).await?;
                Ok(Some(pack.consensus_header_by_number(self.latest_consensus.number).await?))
            }
        } else {
            let pack = self.get_static(self.latest_consensus.epoch).await?;
            Ok(Some(pack.consensus_header_by_number(self.latest_consensus.number).await?))
        }
    }

    /// Return the last consensus number that was processed.
    pub fn latest_consensus_number(&self) -> u64 {
        self.latest_consensus.number
    }

    /// Return the last consensus epoch that was processed.
    pub fn latest_consensus_epoch(&self) -> Epoch {
        self.latest_consensus.epoch
    }

    /// Resolve when the current epoch is fully persisted to storage.
    pub async fn persist_current(&self) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            pack.persist().await?;
        }
        self.latest_consensus.persist().await;
        Ok(())
    }

    /// Read the last committed rounds for authorities from an epoch.
    pub async fn read_last_committed(
        &mut self,
        epoch: Epoch,
    ) -> HashMap<AuthorityIdentifier, Round> {
        if let Some(pack) = &mut self.current_pack {
            if pack.epoch() == epoch {
                return pack.read_last_committed().await;
            }
        }
        if let Ok(mut pack) = self.get_static(epoch).await {
            pack.read_last_committed().await
        } else {
            HashMap::new()
        }
    }

    /// Read the final committed sub dag with final reputation scores.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &mut self,
        epoch: Epoch,
    ) -> Option<Arc<CommittedSubDag>> {
        if let Some(pack) = &mut self.current_pack {
            if pack.epoch() == epoch {
                return pack.read_latest_commit_with_final_reputation_scores().await;
            }
        }
        if let Ok(mut pack) = self.get_static(epoch).await {
            pack.read_latest_commit_with_final_reputation_scores().await
        } else {
            None
        }
    }

    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    pub async fn write_subdag_for_test(&mut self, number: u64, sub_dag: Arc<CommittedSubDag>) {
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
    pub async fn contains_current_batch(&mut self, digest: BlockHash) -> bool {
        if let Some(pack) = &mut self.current_pack {
            pack.contains_batch(digest).await
        } else {
            false
        }
    }

    /// Count leaders in this pack (in rewards_counter) lower than last_executed_round.
    /// This works on the current epoch/pack.
    pub async fn count_leaders(
        &mut self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &mut self.current_pack {
            Ok(pack.count_leaders(last_executed_round, rewards_counter).await?)
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Get a static pack file from the cache if available or create and cache if not.
    async fn get_static(&self, epoch: Epoch) -> Result<ConsensusPack, PackError> {
        if let Some(pack) = &self.current_pack {
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

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::consensus::{ConsensusSlot, LatestConsensus};
    use std::sync::Arc;

    use tn_types::{test_genesis, BlockHash, BlockNumHash, EpochRecord, Hash as _, B256};

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
        let mut latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch, 0);
        assert_eq!(latest.number, 0);
        assert_eq!(latest.current_slot, ConsensusSlot::Slot2);
        latest.update(1, 10).await;
        assert_eq!(latest.epoch, 1);
        assert_eq!(latest.number, 10);
        assert_eq!(latest.current_slot, ConsensusSlot::Slot1);
        latest.update(2, 20).await;
        assert_eq!(latest.epoch, 2);
        assert_eq!(latest.number, 20);
        assert_eq!(latest.current_slot, ConsensusSlot::Slot2);
        latest.persist().await;
        drop(latest);
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch, 2);
        assert_eq!(latest.number, 20);
        assert_eq!(latest.current_slot, ConsensusSlot::Slot2);
    }

    #[tokio::test]
    async fn test_consensus_store_db_stream() {
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
        let mut consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).await.unwrap();
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
        for i in 0..num_outputs {
            let output_db =
                consensus_chain.get_consensus_output_current(i as u64 + 1).await.unwrap();
            let output = outputs.get(i as usize).unwrap();
            compare_outputs(&output_db, output);
        }

        consensus_chain.persist_current().await.expect("persist");
        //drop(consensus_chain);

        let temp_dir2 = TempDir::with_prefix("test_consensus_pack2").expect("temp dir");
        let mut consensus_chain2 = ConsensusChain::new(temp_dir2.path().to_owned()).await.unwrap();
        let stream = consensus_chain.get_epoch_stream(0).await.unwrap();
        consensus_chain2.stream_import(stream, 0, previous_epoch.clone()).await.unwrap();
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
        let mut consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).await.unwrap();
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
                .consensus_header_by_number(None, i as u64 + 1)
                .await
                .expect(&format!("Failed to get header by number on {i}"))
                .unwrap();
            let output = outputs.get(i as usize).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }

        consensus_chain.persist_current().await.expect("persist chain");
        drop(consensus_chain);
        let mut consensus_chain = ConsensusChain::new(temp_dir.path().to_owned()).await.unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Check that last consenus held over a DB shutdown/restart.
        let last_header = outputs.last().unwrap().consensus_header();
        let latest = consensus_chain.consensus_header_latest().await.unwrap().unwrap();
        assert_eq!(last_header.digest(), latest.digest(), "latest header mismatch after reload");

        // Test that all our outputs are still good.
        for i in 0..(num_outputs * 3) {
            let header_db = consensus_chain
                .consensus_header_by_number(None, i as u64 + 1)
                .await
                .unwrap()
                .expect(&format!("something on {i}"));
            let output = outputs.get(i as usize).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }
        // Now by digest
        for i in 0..(num_outputs * 3) {
            let digest: B256 = outputs[i].digest().into();
            let header_db =
                consensus_chain.consensus_header_by_digest(None, digest).await.unwrap().unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }

        // Now with epochs.
        for i in 0..(num_outputs * 3) {
            let epoch = i / num_outputs;
            let header_db = consensus_chain
                .consensus_header_by_number(Some(epoch as u32), i as u64 + 1)
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
                .consensus_header_by_digest(Some(epoch as u32), digest)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }
    }
}
