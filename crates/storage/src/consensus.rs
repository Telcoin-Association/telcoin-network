//! Wrap access to the epoch consensus files into a single interface.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{Read as _, Seek as _, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use tn_types::{
    AuthorityIdentifier, Batch, BlockHash, CommittedSubDag, Committee, ConsensusHeader,
    ConsensusOutput, Epoch, EpochRecord, Round, B256,
};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

use crate::consensus_pack::{ConsensusPack, PackError};

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
        } else {
            if slot1_epoch > slot2_epoch {
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
}

impl ConsensusChain {
    /// Create a new empty consensus chain.
    pub fn new(base_path: PathBuf) -> Result<ConsensusChain, ConsensusChainError> {
        let latest_consensus = LatestConsensus::new(&base_path)?;
        Ok(Self { base_path, current_pack: None, latest_consensus })
    }

    /// Create a new empty consensus chain with a dummy epoch 0 pack ready.
    pub fn new_for_test(
        base_path: PathBuf,
        committee: Committee,
    ) -> Result<ConsensusChain, ConsensusChainError> {
        let mut me = Self::new(base_path)?;
        me.new_epoch(
            EpochRecord {
                epoch: 0,
                committee: committee.bls_keys().iter().copied().collect(),
                next_committee: committee.bls_keys().iter().copied().collect(),
                ..Default::default()
            },
            committee,
        )?;
        Ok(me)
    }

    pub fn new_epoch(
        &mut self,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<(), ConsensusChainError> {
        if let Some(old_pack) = self.current_pack.take() {
            if old_pack.epoch() == committee.epoch() {
                self.current_pack = Some(old_pack);
                return Ok(());
            }
            // Need to stash this in a cache... XXXX
        }
        let pack = ConsensusPack::open_append(&self.base_path, previous_epoch, committee)?;
        self.current_pack = Some(pack);
        Ok(())
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file for the current epoch.
    /// This should be called "in-order" as consensus is executed.
    pub async fn save_consensus_output(
        &mut self,
        consensus: ConsensusOutput,
    ) -> Result<(), ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            self.latest_consensus
                .update(consensus.sub_dag().leader_epoch(), consensus.number())
                .await;
            pack.save_consensus_output(consensus).await?;
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
            } else {
                return Ok(pack.consensus_header_by_digest(digest).await);
            }
        }
        if let Some(epoch) = epoch {
            let pack = ConsensusPack::open_static(&self.base_path, epoch)?;
            Ok(pack.consensus_header_by_digest(digest).await)
        } else {
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
                    return Ok(pack.consensus_header_by_number(number).await);
                }
            } else {
                return Ok(pack.consensus_header_by_number(number).await);
            }
        }
        if let Some(epoch) = epoch {
            let pack = ConsensusPack::open_static(&self.base_path, epoch)?;
            Ok(pack.consensus_header_by_number(number).await)
        } else {
            Ok(None)
        }
    }

    /// Retrieve the last known ConsensusHeader that was executed.
    pub async fn consensus_header_latest(
        &self,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = &self.current_pack {
            if self.latest_consensus.epoch + 1 == pack.epoch() {
                Ok(None)
            } else if self.latest_consensus.epoch != pack.epoch() {
                Err(ConsensusChainError::EpochMismatch)
            } else {
                Ok(pack.consensus_header_by_number(self.latest_consensus.number).await)
            }
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
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
        if let Ok(mut pack) = ConsensusPack::open_static(&self.base_path, epoch) {
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
        if let Ok(mut pack) = ConsensusPack::open_static(&self.base_path, epoch) {
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

    /// True if the current epoch pack contains the batch for digest.
    pub async fn current_batch(&mut self, digest: BlockHash) -> Option<Batch> {
        if let Some(pack) = &mut self.current_pack {
            pack.batch(digest).await
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum ConsensusChainError {
    PackError(PackError),
    NoCurrentEpoch,
    IO(std::io::Error),
    EpochMismatch,
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

    #[tokio::test]
    async fn test_latest_consensus() {
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
}
