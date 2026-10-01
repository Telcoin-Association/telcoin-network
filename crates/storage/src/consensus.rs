//! Wrap access to the epoch consensus files into a single interface.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    thread::JoinHandle,
    time::Duration,
};

use parking_lot::Mutex;
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, CommittedSubDag,
    Committee, ConsensusChainReader, ConsensusChainWriter, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusOutput, Epoch, EpochRecord, ReadStream, Round,
};
use tokio::{
    fs::File as AsyncFile,
    io::AsyncRead,
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
};
use tracing::{debug, error, warn};

use crate::{
    archive::data_file::fsync_directory,
    consensus_pack::{install_dir_rename_aside, ConsensusPack, PackError, DATA_NAME},
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
            // If we are the last reference then shutdown thread and wait for it to persist and
            // exit. Reaching this with a live handle means close() was NOT used: a correct
            // close().await already took the handle, so the block below is skipped. Drop is the
            // safety net; the proper async path is close().await.
            if let Some(handle) = self.handle.lock().take() {
                warn!(target: "consensus_chain", "LatestConsensus dropped without calling close(); sealing as a fallback");
                if self.tx.try_send(LatestConsensusCommand::Shutdown).is_err() {
                    // Full bounded channel — detach. The detached thread exits when the last Sender
                    // drops, but (unlike the pack actors) its channel-closed exit does NOT fsync
                    // the slot files, so a power loss right after this misuse
                    // path can leave a stale hint. Tolerable: the slots are
                    // only a hint, reconciled against the pack on open
                    // (`clamp_latest_to_pack`).
                    error!(target: "consensus_chain", "Failed to send shutdown message to LatestConsensus (should be using close())");
                    return;
                }
                let join = move || {
                    if let Err(e) = handle.join() {
                        error!(target: "consensus_chain", ?e, "Failed to join latest-consensus thread");
                    }
                };
                // Never block a multi-threaded runtime worker on the slot fsyncs: offload the join
                // to the blocking pool. On a current-thread runtime (nothing else
                // to starve) or no runtime, a synchronous join keeps "sealed on
                // return". `close().await` is still the intended path.
                match tokio::runtime::Handle::try_current() {
                    Ok(rt) if rt.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                        rt.spawn_blocking(join);
                    }
                    _ => join(),
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
    /// Persist then shutdown the background thread.
    /// Notify async when done (avoid blocking any tokio tasks waiting on join()).
    AsyncShutdown(oneshot::Sender<()>),
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

    /// Encode a slot file's 16-byte payload: `epoch` (u32 LE) + `number` (u64 LE) + a crc32 (LE)
    /// over the two. Mirrors [`Self::read_slot`].
    fn encode_slot(epoch: Epoch, number: u64) -> [u8; 16] {
        let mut buffer = [0_u8; 16];
        buffer[0..4].copy_from_slice(&epoch.to_le_bytes());
        buffer[4..12].copy_from_slice(&number.to_le_bytes());
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&epoch.to_le_bytes());
        crc32_hasher.update(&number.to_le_bytes());
        let crc32 = crc32_hasher.finalize();
        buffer[12..16].copy_from_slice(&crc32.to_le_bytes());
        buffer
    }

    /// The epoch [`Self::new`] would resume from under `base_path`, read without creating or
    /// writing the slot files: the higher of the two slots' epochs, an unreadable slot counting as
    /// `(0, 0)` exactly as `new` treats it. `None` when neither slot file exists.
    fn read_epoch(base_path: &Path) -> Option<Epoch> {
        let mut found = false;
        let mut epoch = 0;
        for name in ["consensus_slot1", "consensus_slot2"] {
            if let Ok(mut slot) = File::open(base_path.join(name)) {
                found = true;
                epoch = epoch.max(Self::read_slot(&mut slot).map_or(0, |(e, _)| e));
            }
        }
        found.then_some(epoch)
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
                // Note, that code called in this thread should NEVER panic since that will orphan
                // the slot file. This is acceptable since panic should never occur
                // in properly written code.
                match com {
                    LatestConsensusCommand::Update(old_epoch, epoch, number, slot) => {
                        let f = match slot {
                            ConsensusSlot::Slot1 => &mut slot1,
                            ConsensusSlot::Slot2 => &mut slot2,
                        };
                        let buffer = LatestConsensus::encode_slot(epoch, number);
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
                    LatestConsensusCommand::AsyncShutdown(tx) => {
                        sync_all_with_log(&slot1);
                        sync_all_with_log(&slot2);
                        let _ = tx.send(());
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

    /// Reconcile the hint's number DOWN to `number` (the recovered pack's latest). In-memory only:
    /// the pack is ground truth, the node re-persists the slots as it saves outputs, and a crash
    /// before that just re-clamps idempotently at the next open. Keeps the epoch (which
    /// [`ConsensusChain::new`] uses to open the current pack).
    fn clamp_to(&self, number: u64) {
        self.state.lock().number = number;
    }

    /// Clean-close now REGARDLESS of remaining clones (mirrors `ConsensusPack::seal_now`, incl. its
    /// channel-only-handle memory-safety argument: a surviving clone holds only `tx`/`handle`, so
    /// its reads/writes fail cleanly on the closed channel after the actor exits — nothing
    /// dangles). Idempotent/drop-safe: the join handle is taken under the lock, so only the
    /// first caller seals; a later call or a surviving clone's `Drop` finds it gone and no-ops.
    /// `ConsensusChain::close` calls this at graceful shutdown (the `Drop` fallback still
    /// covers non-graceful paths).
    async fn seal_now(&self) {
        let Some(_handle) = self.handle.lock().take() else {
            return;
        };
        let (tx, rx) = oneshot::channel();
        if self.tx.send(LatestConsensusCommand::AsyncShutdown(tx)).await.is_ok() {
            // Lets do an async wait for confirmition vs a sync join() on the thread.
            let _ = rx.await;
        }
    }

    /// Return the current slot value (for testing).
    #[cfg(test)]
    fn current_slot(&self) -> ConsensusSlot {
        self.state.lock().current_slot
    }
}

/// A verified prefix of an in-progress epoch streamed from a peer, held in a side directory for
/// catch-up reads. `final_number` is the highest consensus number it contains.
#[derive(Debug, Clone)]
struct StagingPack {
    pack: ConsensusPack,
    final_number: u64,
    /// The directory this import staged into (its own, see
    /// [`ConsensusChain::import_partial_to_staging`]); removed when the pack is cleared or
    /// replaced.
    dir: PathBuf,
}

/// Implement a databse for consensus data.
#[derive(Debug, Clone)]
pub struct ConsensusChain {
    /// Base path for files.
    base_path: PathBuf,
    /// Current pack for the epoch being written.
    /// It is in an Arc and Mutex so clones of ConsensusChain stay in sync.
    current_pack: Arc<Mutex<ConsensusPack>>,
    /// Track the latest consensus that was saved.
    latest_consensus: LatestConsensus,
    /// Simple cache of recent pack files.
    recent_packs: Arc<Mutex<VecDeque<ConsensusPack>>>,
    /// Bumped every time an `epoch-{N}` directory is installed/replaced or the current pack is
    /// swapped (`stream_import` install, `new_epoch` handoff). `get_static` snapshots it
    /// before its unlocked `open_static` and refuses to cache the opened handle if it changed
    /// meanwhile — so a pack opened against a since-replaced (stale) inode is served once but
    /// never poisons the cache.
    install_generation: Arc<AtomicU64>,
    epochs: Arc<EpochRecordDb>,
    /// Serializes epoch-{N} directory mutation between `new_epoch` (open/append)
    /// and `stream_import` (remove+rename), preventing a transient-ENOENT crash.
    ///
    /// Both critical sections cross `.await` points, so this is a `tokio::sync::Mutex`
    /// (not the `parking_lot::Mutex` used for the other fields). Always acquired *before*
    /// `current_pack`/`recent_packs` to keep a single lock order and avoid deadlock.
    pack_install: Arc<tokio::sync::Mutex<()>>,
    /// Read-only "staging" pack holding a verified PREFIX of an (in-progress) epoch streamed from
    /// a peer for catch-up. Unlike `current_pack`, this lives in its own `staging-{epoch}-{n}`
    /// directory and is NEVER renamed over the live `epoch-{N}` dir, so importing it cannot
    /// race the in-order build of the main pack. Outputs are read from here during catch-up,
    /// then written to the main pack in order through the normal save path; cleared once
    /// drained.
    staging: Arc<Mutex<Option<StagingPack>>>,
    /// Per-epoch serialization of read-side heals (see [`Self::heal_static`]), so two readers of
    /// the same legacy or damaged epoch never build its heal twice, without holding
    /// `pack_install` (which gates epoch handoff) for the length of a rebuild.
    heal_locks: Arc<Mutex<HashMap<Epoch, Arc<tokio::sync::Mutex<()>>>>>,
    /// Epochs whose read-side heal failed recently: when, and with what. A failed heal is not
    /// retried until [`ReadSideHeal::RETRY_BACKOFF`] has passed, so a corrupt epoch that peers
    /// keep asking for does not re-run a full-epoch scan or copy per request. Environmental
    /// failures (a full disk, descriptor exhaustion) are not remembered: they say nothing
    /// about the epoch and clear on their own.
    heal_failures: Arc<Mutex<HashMap<Epoch, (std::time::Instant, PackError)>>>,
    /// Serializes [`Self::import_partial_to_staging`], so two downloads of a staging prefix never
    /// run side by side (there is a single staging slot). Chain-wide and held across the whole
    /// transfer, which the sync reader's per-frame timeout and throughput floor bound. Each import
    /// also stages into a directory of its own, so clearing the installed pack never touches an
    /// import still streaming.
    staging_import: Arc<tokio::sync::Mutex<()>>,
}

impl ConsensusChain {
    /// How many recently opened pack files to maintain.
    const PACK_CACHE_SIZE: usize = 10;

    /// Create a new empty consensus chain.
    pub fn new(
        base_path: PathBuf,
        committee_zero: Committee,
    ) -> Result<ConsensusChain, ConsensusChainError> {
        let latest_consensus = LatestConsensus::new(&base_path)?;

        // Roll back any interrupted install/migration BEFORE opening the current pack: restore an
        // `epoch-N.replaced` aside whose `epoch-N` went missing (a crash between the two install
        // renames), so the hint-epoch open below cannot brick and no past epoch is lost. Must run
        // before both the open and the sweep.
        Self::recover_incomplete_installs(&base_path);

        // If we have a pack for the last epoch open it so we can read data early.
        let current_pack = if latest_consensus.number() == 0 && latest_consensus.epoch() == 0 {
            // If we are just starting then we need to pre-open the epoch 0 pack.
            let previous_epoch = EpochRecord {
                // If we can't find the record then we should be starting at epoch 0- use
                // this filler.
                epoch: 0,
                committee: committee_zero.bls_keys().into_iter().collect(),
                next_committee: committee_zero.bls_keys().into_iter().collect(),
                ..Default::default()
            };
            Arc::new(Mutex::new(ConsensusPack::open_append(
                &base_path,
                previous_epoch,
                committee_zero,
            )?))
        } else {
            // If we are running already then we should have a pack for the latest epoch so it is
            // Ok to error out here if it is missing. Open it in append mode (not static): this
            // runs recover_pack to repair a torn write from a hard crash mid-epoch, and leaves
            // the pack writable so the node can resume saving outputs for this epoch without
            // waiting for new_epoch to flip a read-only pack to append.
            Arc::new(Mutex::new(ConsensusPack::open_append_exists(
                &base_path,
                latest_consensus.epoch(),
            )?))
        };
        let recent_packs = Arc::new(Mutex::new(VecDeque::default()));
        let epochs = Arc::new(EpochRecordDb::open(&base_path)?);
        let pack_install = Arc::new(tokio::sync::Mutex::new(()));
        // Any staging dirs left from a previous run are stale; start clean. The staging pack only
        // ever holds transient, re-fetchable catch-up data.
        Self::remove_all_staging_and_import_dirs(&base_path);
        let staging = Arc::new(Mutex::new(None));
        Ok(Self {
            base_path,
            current_pack,
            latest_consensus,
            recent_packs,
            install_generation: Arc::new(AtomicU64::new(0)),
            epochs,
            pack_install,
            staging,
            heal_locks: Arc::new(Mutex::new(HashMap::new())),
            heal_failures: Arc::new(Mutex::new(HashMap::new())),
            staging_import: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Create a new empty consensus chain with a dummy epoch 0 pack ready.
    pub async fn new_for_test(
        base_path: PathBuf,
        committee: Committee,
    ) -> Result<ConsensusChain, ConsensusChainError> {
        let me = Self::new(base_path, committee.clone())?;
        let rec = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        me.new_epoch(rec.clone(), committee).await?;
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
        let old_pack = self.current_pack();
        // Same-epoch re-entry (mid-epoch restart, or a mode change that re-runs
        // epoch setup) intentionally reuses the already-open pack, matching on
        // epoch number alone. The pack's persisted `EpochMeta` — committee
        // included — is the epoch-START snapshot; chain state can legitimately
        // diverge from it mid-epoch (a governance burn shrinks the on-chain
        // committee immediately), so comparing anything beyond the epoch number
        // here would reject the re-entry and strand the node. Keeping the
        // original pack is load-bearing: this epoch's consensus output must be
        // decoded and verified against the committee the epoch started with.
        // For an imported epoch that snapshot is the serving peer's copy,
        // authenticated on its BLS key set only.
        if old_pack.epoch() == committee.epoch() && !old_pack.is_static() {
            // TRIPWIRE (diagnostics only): the pack's persisted committee is the epoch-START
            // snapshot, and the entry `committee` is derived from a read pinned to that same
            // epoch-start state — so the two BLS key sets can differ only if an unpinned
            // (canonical-tip) entry read is ever reintroduced. Warn loudly at the first
            // mid-epoch re-entry after a governance burn instead of staying silent until the
            // epoch record splits. Do NOT error: either way the pack's epoch-start snapshot
            // remains authoritative for decoding this epoch's output, and the re-entry must
            // proceed.
            let pack_keys = old_pack.committee().bls_keys();
            let entry_keys = committee.bls_keys();
            if pack_keys != entry_keys {
                let missing_from_entry: Vec<_> = pack_keys.difference(&entry_keys).collect();
                let added_in_entry: Vec<_> = entry_keys.difference(&pack_keys).collect();
                warn!(
                    target: "consensus-chain",
                    epoch = committee.epoch(),
                    pack_committee_len = pack_keys.len(),
                    entry_committee_len = entry_keys.len(),
                    ?missing_from_entry,
                    ?added_in_entry,
                    "same-epoch re-entry committee differs from the pack's epoch-start snapshot; \
                     an unpinned (tip-based) entry read has likely been reintroduced — keeping \
                     the pack's committee"
                );
            }
            return Ok(());
        }
        old_pack.persist().await?;
        let epoch = committee.epoch();
        let pack = ConsensusPack::open_append(&self.base_path, previous_epoch, committee)?;
        if let Err(e) = pack.persist().await {
            // Surface any open errors — async-close the just-opened pack (the only handle) instead
            // of letting its blocking `Drop` join the background thread on this tokio worker.
            pack.close().await;
            return Err(e.into());
        }
        *self.current_pack.lock() = pack;
        // Drop any cached read-only handle for either epoch. `epoch` is now the live writer, and a
        // handle cached before this handoff (e.g. a meta-only epoch read on restart before
        // `new_epoch` reached it) would keep serving that stale view once the epoch is sealed and
        // read back through `get_static`. The old epoch is re-sealed below and reopened fresh.
        // The live pack just changed; invalidate any `get_static` open racing this handoff, then
        // evict (a reader that inserts between the two would otherwise cache its stale handle
        // under the old generation).
        self.install_generation.fetch_add(1, Ordering::Release);
        for stale in self.take_cached(&[epoch, old_pack.epoch()]) {
            stale.close().await;
        }
        if let Some(staging_epoch) = self.staging_epoch() {
            // If we have moved past the staging pack then clear it.
            // Should get cleared in the normal course but this is
            // stopgap just in case.
            if staging_epoch < epoch {
                self.clear_staging().await;
            }
        }
        // Seal the previous epoch's pack before releasing `pack_install`, so it is a sealed static
        // epoch the moment this handoff returns. The next epoch's subscriber reads it immediately
        // (its parent is that epoch's last output), and `get_static` only accepts a sealed pack:
        // leaving the seal to a straggling clone's `Drop` would make that read fail as
        // `CorruptPack` and halt the node.
        Self::seal_previous_pack(old_pack).await;
        Ok(())
    }

    /// How long an epoch handoff waits for in-flight readers of the previous epoch's pack to drop
    /// their clones before force-sealing it.
    const HANDOFF_SEAL_GRACE: Duration = Duration::from_millis(500);

    /// Seal the just-replaced previous-epoch pack now, even if a reader still holds a clone.
    ///
    /// Readers clone the current pack only for the duration of one request, so give them a short
    /// grace to finish, then seal regardless. `seal_now` is safe under a live clone: clones hold
    /// only the actor's channel, requests already queued are served before the seal, and a later
    /// request on a straggling clone fails cleanly instead of the epoch staying unsealed until that
    /// clone drops. The seal runs on the actor thread and is awaited asynchronously, so this never
    /// blocks a tokio worker.
    async fn seal_previous_pack(old_pack: ConsensusPack) {
        let deadline = tokio::time::Instant::now() + Self::HANDOFF_SEAL_GRACE;
        while !old_pack.is_sole_handle() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if !old_pack.is_sole_handle() {
            warn!(
                target: "consensus::store",
                epoch = old_pack.epoch(),
                "previous-epoch pack still has live handle(s) at handoff; sealing it anyway (a \
                 straggling request on it will fail)"
            );
        }
        old_pack.seal_now().await;
    }

    /// Remove every `recent_packs` entry for any of `epochs` and return them, so the caller can
    /// close them OUTSIDE the cache lock (a last-handle close must not run under it).
    fn take_cached(&self, epochs: &[Epoch]) -> Vec<ConsensusPack> {
        let mut recents = self.recent_packs.lock();
        let mut kept = VecDeque::with_capacity(recents.len());
        let mut taken = Vec::new();
        while let Some(p) = recents.pop_front() {
            if epochs.contains(&p.epoch()) {
                taken.push(p);
            } else {
                kept.push_back(p);
            }
        }
        *recents = kept;
        taken
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
            // Idempotency / anti-truncation guard: if we already contain the requested final
            // consensus header, there is nothing to import. Checking *by number* (rather than only
            // the pack's latest header) means a PARTIAL request whose final is `(n, hash)` is a
            // no-op when we already hold `n` — even inside a longer pack — so we never
            // remove+rename `epoch-{N}` down to a shorter prefix and truncate data. For
            // a number we don't have, this lookup errors, so an incomplete pack still
            // streams as before.
            if let Ok(have) =
                pack.consensus_header_by_number(epoch_record.final_consensus.number).await
            {
                if have.digest() == epoch_final_hash {
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
                let path_base_dir = path.join(format!("epoch-{epoch}"));
                // Validate the imported pack; on ANY failure async-close it (the only handle)
                // instead of leaving it to the blocking `Drop` join on this tokio
                // worker. The chain was verified as it was streamed, so a final
                // block matching the expected `final_consensus` means the entire
                // pack file is valid.
                let outcome: Result<(), ConsensusChainError> = async {
                    pack.persist().await?;
                    match pack.latest_consensus_header().await? {
                        Some(h)
                            if epoch_record.final_consensus.number == h.number
                                && epoch_final_hash == h.digest() =>
                        {
                            Ok(())
                        }
                        // Invalid final consensus header...
                        Some(_) => Err(ConsensusChainError::InvalidImport),
                        // Missing a final consensus header...
                        None => Err(ConsensusChainError::EmptyImport),
                    }
                }
                .await;
                if let Err(e) = outcome {
                    pack.close().await;
                    return Err(e);
                }
                // Acquire the install lock only now — after the (multi-second) network
                // download has finished writing into the temp import dir. It must NOT wrap
                // the download (that would block unrelated epoch transitions on network I/O).
                // Held through the remove+rename and cache invalidation below so new_epoch's
                // open_append cannot observe the transient window where epoch-{N} is unlinked.
                let _install = self.pack_install.lock().await;
                let replace_current = self.current_pack.lock().epoch() == epoch;
                // Async-close the imported pack (the only handle) before the remove+rename:
                // `close()` returns only after the inner `MmapDataFile`s drop (FDs
                // released), same as the old blocking `Drop::join`, but without
                // stalling a tokio worker.
                pack.close().await;
                // Atomically install the imported dir (rename-aside): the live epoch-{N} dir is
                // moved aside and only removed after the new one is renamed in and
                // the parent is fsync'd, so a rename failure never leaves
                // `current_pack` writing to an unlinked inode (on failure
                // the old dir is restored and the error propagates).
                let install_result =
                    Self::install_imported_epoch_dir(&self.base_path, epoch, &path_base_dir);
                // Invalidate the cache and bump the install generation regardless of the install
                // outcome: a post-rename `fsync_directory` failure still leaves the NEW inode in
                // place, so any concurrent get_static that opened FDs on the old
                // inode must not leave a stale entry (and a racing open must not
                // cache the stale handle — see the generation guard
                // in `get_static`). Harmless when the rename failed and the old inode was restored
                // (same inode; the re-open below just re-reads it).
                self.install_generation.fetch_add(1, Ordering::Release);
                for p in self.take_cached(&[epoch]) {
                    p.close().await;
                }
                // Surface a genuine install failure only AFTER invalidating the cache above.
                install_result?;
                if replace_current {
                    // Do this directly, using get_static() will short circuit on the old pack...
                    // Swap the old pack out under the lock, then async-close it after the guard
                    // drops.
                    let new_static = ConsensusPack::open_static(&self.base_path, epoch)?;
                    let old = std::mem::replace(&mut *self.current_pack.lock(), new_static);
                    old.close().await;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Return a stream reader for the data file of `epoch` together with its logical length.
    /// Verifies the epoch pack is complete or returns an error. The caller streams exactly `[0,
    /// data_len)`.
    pub async fn get_epoch_stream(
        &self,
        epoch: Epoch,
    ) -> Result<(Box<dyn ReadStream>, u64), ConsensusChainError> {
        if let Ok(pack) = self.get_static(epoch).await {
            if let Some((epoch_record, _)) = self.epochs().get_epoch_by_number(epoch).await {
                match pack.latest_consensus_header().await? {
                    Some(last_header) => {
                        let epoch_final_hash = epoch_record.final_consensus.hash;
                        if epoch_record.final_consensus.number == last_header.number
                            && epoch_final_hash == last_header.digest()
                        {
                            // Return the logical data length so the caller streams exactly
                            // `[0, data_len)`. This bound is LOAD-BEARING, not belt-and-suspenders:
                            // a sealed pack is physically `end + 8`
                            // (the clean-close sentinel) and a live
                            // pack is padded out to mmap capacity, so without it the stream would
                            // carry the sentinel / padding and the
                            // importer's `AsyncPackIter` would reject
                            // the trailing bytes. It also lets the pack own its length instead of a
                            // network-triggered truncate of the served file.
                            let data_len = pack.data_file_len().await?;
                            drop(pack);
                            let base_dir = self.base_path.join(format!("epoch-{epoch}"));
                            let stream = AsyncFile::open(base_dir.join(DATA_NAME)).await?;
                            Ok((Box::new(stream), data_len))
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

    /// Return a stream reader for the data file of `epoch` together with the number of bytes that
    /// should be sent to deliver a verifiable PREFIX of the pack: every consensus output up to and
    /// including `last_consensus_number` (a chain consensus number, not a pack-relative index).
    ///
    /// Unlike [`Self::get_epoch_stream`], this does NOT require the epoch pack to be complete, so
    /// it can stream the in-progress current epoch up to an already-persisted, verifiable
    /// point. The returned byte length is the `output_end` offset of `last_consensus_number`;
    /// the caller streams `[0, len)` of the data file. The pack is persisted first so those
    /// bytes are flushed to disk.
    pub async fn get_partial_epoch_stream(
        &self,
        epoch: Epoch,
        last_consensus_number: u64,
    ) -> Result<(Box<dyn ReadStream>, u64), ConsensusChainError> {
        let pack =
            self.get_static(epoch).await.map_err(|_| ConsensusChainError::StreamUnavailable)?;
        let end = pack.consensus_output_end(last_consensus_number).await?;
        // Flush (no fsync) so every byte counted in `end` is written to the file and thus visible
        // to the separate AsyncFile handle opened below. Visibility, not durability — avoids the
        // expensive network-triggerable fsync on the live pack.
        pack.flush_data().await?;
        let base_dir = self.base_path.join(format!("epoch-{epoch}"));
        let stream = AsyncFile::open(base_dir.join(DATA_NAME)).await?;
        Ok((Box::new(stream), end))
    }

    /// The epoch a node opens for append when it starts on the epochs directory `base_path`: the
    /// latest-consensus hint (the `consensus_slot1`/`consensus_slot2` files), read without creating
    /// or writing them. Every epoch below it is a past epoch the node only reads; the directory
    /// listing is not authoritative (a later `epoch-{N}` can exist before the node switches to it).
    /// `None` when there are no slot files.
    pub fn resume_epoch(base_path: &Path) -> Option<Epoch> {
        LatestConsensus::read_epoch(base_path)
    }

    /// Remove any leftover `staging-*`, `import-*`, or `epoch-*.migrating` directories under
    /// `base_path` (stale from a prior run — `*.migrating` is a half-built pack from an interrupted
    /// v1/v0→v2 migration; both are always re-fetchable/re-derivable so deleting them is safe).
    ///
    /// `epoch-N.replaced` is deliberately NOT swept here — it is a rename-aside backup that may be
    /// the only surviving copy of `epoch-N` if a crash landed between the two install renames.
    /// [`Self::recover_incomplete_installs`] (run first, at startup) restores or removes it based
    /// on whether `epoch-N` exists.
    ///
    /// Also removes any read-side heal staging a crash left inside an `epoch-N` directory (rebuilt
    /// index copies only; see [`ConsensusPack::remove_stale_heal_dirs`]).
    fn remove_all_staging_and_import_dirs(base_path: &Path) {
        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
                if name.starts_with("staging-")
                    || name.starts_with("import-")
                    || name.ends_with(".migrating")
                {
                    let _ = std::fs::remove_dir_all(entry.path());
                } else if name.strip_prefix("epoch-").is_some_and(|n| n.parse::<Epoch>().is_ok()) {
                    ConsensusPack::remove_stale_heal_dirs(&entry.path());
                }
            }
        }
    }

    /// Roll back any interrupted install/migration before the current pack is opened. Both
    /// [`Self::install_imported_epoch_dir`] and the pack-format migration move the live `epoch-N`
    /// aside to `epoch-N.replaced` and only then rename the replacement into place; a crash between
    /// those two renames (or an install failure whose restore rename also failed) can leave NO
    /// `epoch-N`. For each `epoch-N.replaced`: if `epoch-N` is MISSING, restore it (the aside
    /// is the last good copy) so startup does not fail on the hint epoch or silently lose a
    /// past epoch; if `epoch-N` EXISTS, the aside is a stale backup from a completed install
    /// and is removed. Deleting `.replaced` unconditionally (the old behavior) could brick
    /// startup or lose an epoch.
    fn recover_incomplete_installs(base_path: &Path) {
        let Ok(entries) = std::fs::read_dir(base_path) else { return };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else { continue };
            // `epoch-N.replaced` -> live dir name `epoch-N`.
            let Some(live_name) = name.strip_suffix(".replaced") else { continue };
            if !live_name.starts_with("epoch-") {
                continue;
            }
            let aside = entry.path();
            let live = base_path.join(live_name);
            if std::fs::exists(&live).unwrap_or(false) {
                // Completed install left a stale backup; drop it.
                let _ = std::fs::remove_dir_all(&aside);
            } else {
                // Interrupted install: restore the old good copy.
                match std::fs::rename(&aside, &live) {
                    Ok(()) => {
                        let _ = fsync_directory(base_path);
                        warn!(
                            target: "consensus::store",
                            dir = %live.display(),
                            "restored epoch dir from an interrupted install (epoch-N.replaced -> epoch-N)"
                        );
                    }
                    Err(e) => error!(
                        target: "consensus::store",
                        %e,
                        aside = %aside.display(),
                        "failed to restore epoch dir from .replaced; manual recovery may be required"
                    ),
                }
            }
        }
    }

    /// Atomically install a freshly-imported `epoch-{epoch}` directory at `base_path`, replacing
    /// any existing one, via rename-aside: the live dir is moved to `epoch-{epoch}.replaced`
    /// and only removed after the import is renamed into place and the parent directory is
    /// fsync'd. If the install rename fails, the old dir is restored (same inode) so a live
    /// `current_pack` is never left writing to an unlinked inode. A crash mid-swap leaves a
    /// `*.replaced` dir that [`Self::recover_incomplete_installs`] restores (or removes) on the
    /// next start.
    fn install_imported_epoch_dir(
        base_path: &Path,
        epoch: Epoch,
        import_dir: &Path,
    ) -> Result<(), ConsensusChainError> {
        let base_dir = base_path.join(format!("epoch-{epoch}"));
        let aside = base_path.join(format!("epoch-{epoch}.replaced"));
        install_dir_rename_aside(base_path, &base_dir, &aside, import_dir)?;
        Ok(())
    }

    /// Import a verified PARTIAL pack (a prefix of an in-progress epoch streamed from a peer) into
    /// a side "staging" directory and keep it open for reading.
    ///
    /// This intentionally does NOT use [`Self::stream_import`] (which removes+renames the live
    /// `epoch-{N}` dir): the staged pack lives in its own `staging-{epoch}-{n}` directory and is
    /// only ever read, so a
    /// node that is concurrently building the same epoch in order (via
    /// [`Self::save_consensus_output`]) cannot race it. Verifies the streamed prefix ends
    /// exactly at `epoch_record.final_consensus`. Concurrent calls run one after another (each
    /// replaces what the previous one staged), never interleaved in the staging directory.
    pub async fn import_partial_to_staging<R: AsyncRead + Unpin>(
        &self,
        stream: R,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        timeout: Duration,
    ) -> Result<(), ConsensusChainError> {
        let _serial = self.staging_import.lock().await;
        let epoch = epoch_record.epoch;
        // A directory of its own (never another import's, never the installed pack's), so neither
        // clearing the installed pack nor a stale attempt can remove files this import is writing.
        // The startup sweep removes every `staging-*` directory left behind.
        static STAGING_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
        let staging_base = self.base_path.join(format!("staging-{epoch}-{seq}"));
        std::fs::create_dir_all(&staging_base)?;
        let final_number = epoch_record.final_consensus.number;
        let pack = match ConsensusPack::stream_import(
            &staging_base,
            stream,
            epoch,
            previous_epoch,
            final_number,
            timeout,
        )
        .await
        {
            Ok(pack) => pack,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&staging_base);
                return Err(e.into());
            }
        };
        // Validate the streamed prefix; on ANY failure async-close the pack (the only handle)
        // instead of the blocking `Drop` join on this tokio worker, then drop the staging
        // dir. The chain was verified link-by-link as it streamed; confirm the prefix ends
        // exactly at the requested final consensus so the staged data is trustworthy.
        let outcome: Result<(), ConsensusChainError> = async {
            pack.persist().await?;
            match pack.latest_consensus_header().await? {
                Some(last)
                    if last.number == final_number
                        && last.digest() == epoch_record.final_consensus.hash =>
                {
                    Ok(())
                }
                // Invalid final consensus header...
                Some(_) => Err(ConsensusChainError::InvalidImport),
                // Missing a final consensus header...
                None => Err(ConsensusChainError::EmptyImport),
            }
        }
        .await;
        if let Err(e) = outcome {
            // Close first (releases the pack's FDs and stops its thread) then remove the dir.
            pack.close().await;
            let _ = std::fs::remove_dir_all(&staging_base);
            return Err(e);
        }
        // Install the new staging pack; if one was still installed, async-close it outside the
        // lock rather than dropping it (blocking-join) under the guard, then remove its directory.
        let previous =
            self.staging.lock().replace(StagingPack { pack, final_number, dir: staging_base });
        if let Some(previous) = previous {
            previous.pack.close().await;
            let _ = std::fs::remove_dir_all(&previous.dir);
        }
        Ok(())
    }

    /// The highest consensus number held by the staging pack, if any.
    pub fn staging_final(&self) -> Option<u64> {
        self.staging.lock().as_ref().map(|s| s.final_number)
    }

    /// The epoch of the staging pack, if any.
    pub fn staging_epoch(&self) -> Option<Epoch> {
        self.staging.lock().as_ref().map(|s| s.pack.epoch())
    }

    /// Read a full consensus output (with batches) from the staging pack, if it covers `number`.
    /// Used for module unit tests.
    #[cfg(test)]
    async fn staging_consensus_output(&self, number: u64) -> Option<ConsensusOutput> {
        let pack = self.staging.lock().as_ref().map(|s| s.pack.clone())?;
        pack.get_consensus_output(number).await.ok()
    }

    /// Drop the staging pack and remove its directory. Safe to call when none is staged.
    ///
    /// Closes the staging pack with `close().await` so its background-thread join does not block a
    /// tokio worker, then removes the staging directory.
    pub async fn clear_staging(&self) {
        let staged = self.staging.lock().take();
        if let Some(staged) = staged {
            staged.pack.close().await;
            let _ = std::fs::remove_dir_all(&staged.dir);
        }
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file for the
    /// current epoch. This should be called "in-order" as consensus is executed.
    /// Returns the number of bytes the encoded Output takes on disk IF this is written to the
    /// current pack or 0 if the output already resides in a static (imported) pack.
    ///
    /// A number at or below the latest saved consensus is a hard
    /// [`ConsensusChainError::NonMonotonicConsensusNumber`] error, never a silent skip. The old
    /// silent `Ok(0)` path let a node whose startup resume collapsed to a default header at
    /// number 0 discard every subsequent output with no error and no log - identically on every
    /// node - so consensus height froze with no signal. Failing hard turns that state into a
    /// loud halt.
    pub async fn save_consensus_output(
        &self,
        consensus: ConsensusOutput,
    ) -> Result<u64, ConsensusChainError> {
        let number = consensus.number();
        let latest = self.latest_consensus.number();
        let epoch = consensus.sub_dag().leader_epoch();
        let pack = &self.current_pack();
        if number <= latest {
            // Consensus numbers must strictly increase; a non-increasing number means this
            // node's view of "latest" and the incoming output stream disagree (e.g. a startup
            // resume that silently fell back to a default header at number 0).
            error!(target: "consensus-chain", number, latest, "Refused to save consensus output: number does not advance the latest saved consensus.");
            Err(ConsensusChainError::NonMonotonicConsensusNumber { latest, number })
        } else if epoch != pack.epoch() {
            // The output's epoch does not match the current pack. Saving it would either
            // corrupt this pack or poison its async error channel. The pack
            // layer also rejects this (defense in depth), but the reject is asynchronous so
            // we must guard here to avoid advancing latest_consensus to a wrong-epoch
            // pointer for data that was never persisted.
            // This is an error and should not happen on a properly working node.
            error!(target: "consensus-chain", epoch, pack_epoch = pack.epoch(), number, "Refused to save consensus output: epoch does not match the current pack.");
            Err(ConsensusChainError::InvalidPackEpoch(pack.epoch(), epoch))
        } else if !pack.is_static() {
            // If this an open pack file then save.
            // Note, saving an output that is already in the pack is a no-op, not an error
            // so this is fine.
            let output_bytes = pack.save_consensus_output(consensus).await?;
            self.latest_consensus.update(epoch, number).await;
            Ok(output_bytes)
        } else if !pack.contains_consensus_header_number(number).await.unwrap_or_default() {
            // If this is a static file and this output is missing this is an error...
            error!(target: "consensus-chain", epoch, number, "Failed to update latest consensus, data not in expected pack file.");
            Err(ConsensusChainError::CantSaveAndNotAvailable(number))
        } else {
            // The static (imported) pack already holds this number: replay over an imported epoch
            // only needs to advance latest_consensus, nothing is rewritten. But only for the SAME
            // output (as the writable save checks): a different one must not be reported as
            // persisted, and then executed, while the pack keeps the other.
            let stored = pack.consensus_header_by_number(number).await?.digest();
            let got = consensus.consensus_header_hash();
            if stored != got {
                return Err(PackError::ConflictingOutput { number, stored, got }.into());
            }
            self.latest_consensus.update(epoch, number).await;
            Ok(0)
        }
    }

    /// Load and return the consensus output from the current epoch.
    pub async fn get_consensus_output_current(
        &self,
        number: u64,
    ) -> Result<ConsensusOutput, ConsensusChainError> {
        Ok(self.current_pack().get_consensus_output(number).await?)
    }

    /// Retrieve a consensus header by digest.
    ///
    /// `Ok(None)` strictly means "no such record held here": the digest is unknown to the packs
    /// this node holds (fresh node, epoch never downloaded, or a header it never saw). A pack
    /// that exists on disk but cannot be opened is a hard error, never `None` - collapsing that
    /// failure into `None` let the startup resume path fall back to a default header at number 0
    /// with no error and no log (see [`Self::latest_consensus_header_from_pack`] for the same
    /// rationale one layer down). Under the epoch-gated seed-signature serde, pre-fork packs
    /// stay decodable so the error path should never fire; it is the difference between a loud
    /// halt and silent chain corruption for every future format change.
    pub async fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: ConsensusHeaderDigest,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = self.current_pack_for(epoch) {
            let direct = pack.consensus_header_by_digest(digest).await;
            return if direct.is_some() {
                Ok(direct)
            } else if let Some(staging) = self.staging() {
                // Fallback check on staging before settling on a legitimate `None`.
                Ok(staging.pack.consensus_header_by_digest(digest).await)
            } else {
                Ok(None)
            };
        }
        if let Some(pack) = self.get_static_if_present(epoch).await? {
            // A sealed epoch whose pack is present but unreadable propagated as `Err` from
            // `get_static_if_present` just above; only a genuinely absent epoch (files never on
            // disk) reaches the staging fallback / `Ok(None)` arms below. (The current-epoch
            // branch still collapses pack-internal read failures to `None`: the in-memory
            // channel wrapper types them away below this layer.)
            Ok(pack.consensus_header_by_digest(digest).await)
        } else if let Some(staging) = self.staging() {
            if epoch == staging.pack.epoch() {
                Ok(staging.pack.consensus_header_by_digest(digest).await)
            } else {
                Ok(None)
            }
        } else {
            // Don't have this epoch data: legitimately absent, not a failure.
            Ok(None)
        }
    }

    /// Retrieve a consensus header by number.
    pub async fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        let epoch = self.epochs.number_to_epoch(number);
        if let Some(pack) = self.current_pack_for(epoch) {
            return match pack.consensus_header_by_number(number).await {
                Ok(r) => Ok(Some(r)),
                Err(e) => {
                    if let Some(staging) = self.staging() {
                        // Fallback check on staging before returning an error.
                        if let Ok(r) = staging.pack.consensus_header_by_number(number).await {
                            Ok(Some(r))
                        } else {
                            Err(e.into())
                        }
                    } else {
                        Err(e.into())
                    }
                }
            };
        }
        // A present-but-corrupt sealed pack must surface as `Err`, not be masked as a miss; only a
        // genuinely absent epoch (`Ok(None)`) falls through to staging. Mirrors
        // `consensus_header_by_digest`.
        if let Some(pack) = self.get_static_if_present(epoch).await? {
            Ok(Some(pack.consensus_header_by_number(number).await?))
        } else if let Some(staging) = self.staging() {
            // Don't expose any staging errors.
            if epoch == staging.pack.epoch() {
                Ok(staging.pack.consensus_header_by_number(number).await.ok())
            } else {
                Ok(None)
            }
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Retrieve the consensus output by number.
    pub async fn consensus_output_by_number(
        &self,
        number: u64,
    ) -> Result<Option<ConsensusOutput>, ConsensusChainError> {
        let epoch = self.epochs.number_to_epoch(number);
        if let Some(pack) = self.current_pack_for(epoch) {
            return match pack.get_consensus_output(number).await {
                Ok(r) => Ok(Some(r)),
                Err(e) => {
                    if let Some(staging) = self.staging() {
                        // Fallback check on staging before returning an error.
                        if let Ok(r) = staging.pack.get_consensus_output(number).await {
                            Ok(Some(r))
                        } else {
                            Err(e.into())
                        }
                    } else {
                        Err(e.into())
                    }
                }
            };
        }
        // A present-but-corrupt sealed pack must surface as `Err`, not be masked as a miss; only a
        // genuinely absent epoch (`Ok(None)`) falls through to staging. Mirrors
        // `consensus_header_by_digest`.
        if let Some(pack) = self.get_static_if_present(epoch).await? {
            Ok(Some(pack.get_consensus_output(number).await?))
        } else if let Some(staging) = self.staging() {
            // Note we don't want to expose staging errors, we either find the record or we don't at
            // this point.
            if epoch == staging.pack.epoch() {
                Ok(staging.pack.get_consensus_output(number).await.ok())
            } else {
                Ok(None)
            }
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Decode raw pack-file `bytes` for `epoch` (e.g. fetched via `request_consensus_output`) into
    /// a [`ConsensusOutput`], using the committee from the pack we hold for `epoch` (current /
    /// static / staging). Errors with [`ConsensusChainError::NoCurrentEpoch`] if we have no
    /// pack for `epoch` (so cannot resolve its committee) — the caller should treat that as
    /// "not yet decodable".
    pub async fn decode_consensus_output(
        &self,
        epoch: Epoch,
        bytes: Vec<u8>,
    ) -> Result<ConsensusOutput, ConsensusChainError> {
        let pack = self.current_pack();
        if epoch == pack.epoch() {
            return Ok(pack.decode_output(bytes).await?);
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.decode_output(bytes).await?)
        } else if let Some(staging) = self.staging() {
            if epoch == staging.pack.epoch() {
                Ok(staging.pack.decode_output(bytes).await?)
            } else {
                Err(ConsensusChainError::NoCurrentEpoch)
            }
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// Stream-decode raw v1 (header-first) pack-file bytes for `epoch` from `reader` (e.g. the
    /// reassembled `request_consensus_output` sync stream) into a verified [`ConsensusOutput`],
    /// using the committee from the pack we hold for `epoch` (current / static / staging). Verifies
    /// the header's digest equals `expected_hash` the instant the header record is read — before
    /// any batch is buffered — so a wrong/forged output is rejected without buffering its
    /// batches, and the unverified pre-check buffer is bounded to a single header record rather
    /// than the whole output. Errors with [`ConsensusChainError::NoCurrentEpoch`] if we have no
    /// pack for `epoch` (so cannot resolve its committee) — the caller should treat that as
    /// "not yet decodable". Each record must arrive within `record_timeout`.
    pub async fn stream_decode_consensus_output<R: AsyncRead + Unpin>(
        &self,
        epoch: Epoch,
        reader: R,
        expected_hash: ConsensusHeaderDigest,
        record_timeout: Duration,
    ) -> Result<ConsensusOutput, ConsensusChainError> {
        let pack = self.current_pack();
        if epoch == pack.epoch() {
            return Ok(pack.decode_output_stream(reader, expected_hash, record_timeout).await?);
        }
        if let Ok(pack) = self.get_static(epoch).await {
            Ok(pack.decode_output_stream(reader, expected_hash, record_timeout).await?)
        } else if let Some(staging) = self.staging() {
            if epoch == staging.pack.epoch() {
                Ok(staging.pack.decode_output_stream(reader, expected_hash, record_timeout).await?)
            } else {
                Err(ConsensusChainError::NoCurrentEpoch)
            }
        } else {
            Err(ConsensusChainError::NoCurrentEpoch)
        }
    }

    /// True if the consensus chain contains a pack or partial pack for epoch.
    ///
    /// Useful to determine if calls will find a pack file to work with (like
    /// stream_decode_consenus_output()). This will also warm the pack cache if this is not the
    /// current epochs pack.
    pub async fn contains_decode_epoch(&self, epoch: Epoch) -> bool {
        let pack = self.current_pack();
        if epoch == pack.epoch() {
            return true;
        }
        if let Ok(_pack) = self.get_static(epoch).await {
            true
        } else if let Some(staging) = self.staging() {
            epoch == staging.pack.epoch()
        } else {
            false
        }
    }

    /// Retrieve the raw consensus output bytes by number.
    pub async fn consensus_output_bytes_by_number(
        &self,
        number: u64,
    ) -> Result<Option<Vec<u8>>, ConsensusChainError> {
        let epoch = self.epochs.number_to_epoch(number);
        if let Some(pack) = self.current_pack_for(epoch) {
            return match pack.get_consensus_output_bytes(number).await {
                Ok(r) => Ok(Some(r)),
                Err(e) => {
                    if let Some(staging) = self.staging() {
                        // Fallback check on staging before returning an error.
                        if let Ok(r) = staging.pack.get_consensus_output_bytes(number).await {
                            Ok(Some(r))
                        } else {
                            Err(e.into())
                        }
                    } else {
                        Err(e.into())
                    }
                }
            };
        }
        // A present-but-corrupt sealed pack must surface as `Err`, not be masked as a miss; only a
        // genuinely absent epoch (`Ok(None)`) falls through to staging. Mirrors
        // `consensus_header_by_digest`.
        if let Some(pack) = self.get_static_if_present(epoch).await? {
            Ok(Some(pack.get_consensus_output_bytes(number).await?))
        } else if let Some(staging) = self.staging() {
            if epoch == staging.pack.epoch() {
                // Do not expose staging errors, find data or not.
                Ok(staging.pack.get_consensus_output_bytes(number).await.ok())
            } else {
                Ok(None)
            }
        } else {
            // Don't have this epoch data.
            Ok(None)
        }
    }

    /// Return true if we have a complete pack file for epoch_record.
    pub async fn is_epoch_complete(&self, epoch_record: &EpochRecord) -> bool {
        match self.consensus_header_by_number(epoch_record.final_consensus.number).await {
            Ok(result) => result.is_some(),
            // an incomplete pack ends before the epoch's final output, so this is the normal answer
            Err(ConsensusChainError::PackError(PackError::ConsensusNumberTooHigh)) => {
                debug!(
                    target: "consensus-chain",
                    epoch=?epoch_record.epoch,
                    "epoch pack is incomplete"
                );
                false
            }
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

    /// Reconcile the `LatestConsensus` hint down to the recovered current pack at startup.
    ///
    /// The hint (a durable, fsync'd slot) can end up AHEAD of the pack after a power loss: the slot
    /// is written before `persist_current` msyncs the pack, so a crash can leave `(epoch, k)`
    /// durable while output k is not — including deterministically at every epoch boundary,
    /// where the pack then recovers meta-only. On restart the executor re-derives its parent
    /// from the pack (`k-1`) and re-saves output k, which the ahead hint refuses
    /// (`NonMonotonicConsensusNumber`) → a crash-loop. The pack is ground truth, so clamp the
    /// hint to the pack's actual latest number. Call once, right after opening the chain for
    /// writing.
    pub async fn clamp_latest_to_pack(&self) -> Result<(), ConsensusChainError> {
        let pack_latest = self.current_pack().latest_consensus_number().await?;
        let hint = self.latest_consensus.number();
        if hint > pack_latest {
            warn!(
                target: "consensus_chain",
                hint, pack = pack_latest,
                "LatestConsensus hint is ahead of the recovered pack; clamping to the pack tip"
            );
            self.latest_consensus.clamp_to(pack_latest);
        }
        Ok(())
    }

    /// Return the last consensus epoch that was processed.
    pub fn latest_consensus_epoch(&self) -> Epoch {
        self.latest_consensus.epoch()
    }

    /// Write the "latest consensus" slot hint under `base_path` to `(epoch, number)` so a node
    /// opened there resumes from that consensus output instead of genesis. Writes one slot
    /// (`consensus_slot1`); the other stays `(0, 0)` and loses the `LatestConsensus::new`
    /// reconciliation. Used by `db load-state` after rebuilding an imported epoch's packs.
    pub fn write_latest_consensus_hint(
        base_path: &Path,
        epoch: Epoch,
        number: u64,
    ) -> Result<(), ConsensusChainError> {
        let buffer = LatestConsensus::encode_slot(epoch, number);
        let mut slot = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(base_path.join("consensus_slot1"))?;
        slot.seek(SeekFrom::Start(0))?;
        slot.write_all(&buffer)?;
        slot.sync_all()?;
        // Durably link the (possibly newly-created) `consensus_slot1` entry into `base_path`:
        // `sync_all` flushes the file, not the parent directory entry that names it, so a crash
        // right after `db load-state` could otherwise lose the slot and reset resume to genesis.
        fsync_directory(base_path)?;
        Ok(())
    }

    /// Resolve when the current epoch is fully persisted to storage.
    pub async fn persist_current(&self) -> Result<(), ConsensusChainError> {
        let pack = &self.current_pack();
        pack.persist().await?;
        self.latest_consensus.persist().await;
        Ok(())
    }

    /// Poll (up to `timeout`) until this is the sole owner of the shared pack state — i.e. no other
    /// `ConsensusChain` clone remains (notably the worker RPC server's `EngineToPrimaryRpc`, which
    /// reth's stop-less `RpcServerHandle` releases only as the jsonrpsee task winds down) — so a
    /// following [`Self::close`] seals with no other chain clone still reading or writing.
    /// `current_pack`'s strong count is the proxy: every chain clone bumps it, so `== 1` means
    /// sole. Returns whether sole ownership was reached within `timeout`.
    pub async fn wait_until_sole_owner(&self, timeout: Duration) -> bool {
        let poll = async {
            while Arc::strong_count(&self.current_pack) != 1 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        tokio::time::timeout(timeout, poll).await.is_ok()
    }

    /// Async-close every background thread this chain owns — the current epoch pack, the cached
    /// sealed packs, the staging pack, the latest-consensus slot writer, and the epoch-record DB —
    /// instead of letting each object's `Drop` run a blocking thread `join()`.
    ///
    /// Every component is force-sealed via `seal_now`, whether or not this holds the last
    /// `ConsensusChain` reference: a chain clone that outlived the drain (see
    /// [`Self::wait_until_sole_owner`] — in practice a winding-down RPC connection), or a
    /// transient handle to one pack, would otherwise leave that component unsealed. Sealing under
    /// it (its in-flight reads then fail — benign at shutdown) avoids a full WAL recovery on the
    /// next start. Only called at graceful shutdown.
    ///
    /// Force-sealing under a live clone is memory-safe: each component is an actor whose mmaps live
    /// only on its own thread and whose clones are channel-only handles, so the truncate/unmap runs
    /// on the owning thread and a surviving clone's later reads/writes fail cleanly on the
    /// closed channel — see the safety note on `ConsensusPack::seal_now`.
    pub async fn close(self) {
        let Self { current_pack, latest_consensus, recent_packs, epochs, staging, .. } = self;
        // Every component is force-sealed with `seal_now`, never the sole-owner-gated `close`:
        // even when this is the last `ConsensusChain`, a transient handle to one of its packs (a
        // reader mid-request holding a clone) would make `close` a no-op and leave that pack to a
        // WAL recovery on the next start. `seal_now` is idempotent, and at shutdown its result is
        // the same as the sole-owner path.
        if Arc::strong_count(&current_pack) > 1 {
            warn!(
                target: "consensus::store",
                "current pack still shared at shutdown (a clone outlived the drain); \
                 force-sealing so the next start skips WAL recovery"
            );
        }
        // Clone each handle out of its guard, then DROP the guard before the `.await` (a
        // `parking_lot` guard must not be held across an await point).
        let pack = current_pack.lock().clone();
        pack.seal_now().await;
        let packs: Vec<_> = recent_packs.lock().iter().cloned().collect();
        for pack in packs {
            pack.seal_now().await;
        }
        let staged = staging.lock().clone();
        if let Some(staged) = staged {
            staged.pack.seal_now().await;
        }
        latest_consensus.seal_now().await;
        epochs.seal_now().await;
    }

    /// The logical data length (`end`) of the current epoch's pack: the number of real record
    /// bytes, excluding the mmap capacity padding past `end`. The state export copies the pack's
    /// `data` file and must bound its read to this length so it captures exactly the written
    /// records (`[0, end)`, immutable append-only bytes) and never the trailing padding — a raw
    /// read-to-EOF copy would otherwise include the padding and fail the importer's record-CRC
    /// walk. Bounding by length needs no truncation and is immune to any concurrent append.
    ///
    /// `expected_epoch` guards against an epoch handoff racing between the caller deriving its
    /// epoch (and the source path it pairs this length with) and this read: it errors with
    /// `InvalidPackEpoch` rather than returning a length that belongs to a different epoch's pack.
    pub async fn current_data_len(
        &self,
        expected_epoch: Epoch,
    ) -> Result<u64, ConsensusChainError> {
        let pack = self.current_pack();
        if pack.epoch() != expected_epoch {
            return Err(ConsensusChainError::InvalidPackEpoch(pack.epoch(), expected_epoch));
        }
        Ok(pack.data_file_len().await?)
    }

    /// Return the latest consensus header for `epoch` by reading directly from the pack index,
    /// bypassing the slot files (LatestConsensus). This is always consistent with
    /// read_last_committed and should be used during startup recovery.
    ///
    /// A pack that cannot be opened is an error, never `None`. `None` means "this epoch has
    /// committed nothing", which seeds the epoch seed chain at its root, so collapsing a failed
    /// open into `None` would silently re-root the chain and fork execution permanently (see
    /// [`EpochSeedChainValue`](tn_types::EpochSeedChainValue)). Callers that legitimately probe an
    /// epoch this node may not hold locally - state sync's partial-pack catch-up - degrade the
    /// error themselves at the call site, where "not held" is the intended reading.
    pub async fn latest_consensus_header_from_pack(
        &self,
        epoch: Epoch,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        if let Some(pack) = self.current_pack_for(epoch) {
            return Ok(pack.latest_consensus_header().await?);
        }
        self.get_static(epoch).await?.latest_consensus_header().await.map_err(Into::into)
    }

    /// Read the last committed rounds for authorities from an epoch.
    ///
    /// A pack that cannot be opened is an error, never an empty map, for the same reason as
    /// [`Self::latest_consensus_header_from_pack`]: an empty map is indistinguishable from "this
    /// epoch has committed nothing", and startup recovery reads both cursors from this same pack.
    /// Swallowing the open failure in both made them fail open together, which reproduces one layer
    /// down exactly the silent re-root the fallible recovery path exists to prevent.
    pub async fn read_last_committed(
        &self,
        epoch: Epoch,
    ) -> Result<HashMap<AuthorityIdentifier, Round>, ConsensusChainError> {
        if let Some(pack) = self.current_pack_for(epoch) {
            return Ok(pack.read_last_committed().await?);
        }
        self.get_static(epoch).await?.read_last_committed().await.map_err(Into::into)
    }

    /// Read the final committed sub dag with final reputation scores.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Result<Option<CommittedSubDag>, ConsensusChainError> {
        if let Some(pack) = self.current_pack_for(epoch) {
            return Ok(pack.read_latest_commit_with_final_reputation_scores().await?);
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
        self.current_pack().contains_batch(digest).await
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
        Ok(self.current_pack().count_leaders(last_executed_round, rewards_counter).await?)
    }

    /// Return a clone of the current pack.
    fn current_pack(&self) -> ConsensusPack {
        self.current_pack.lock().clone()
    }

    /// A clone of the current pack if it is `epoch`'s, else `None`. Readers that fall through to
    /// `get_static` for another epoch use this so the live pack's clone is gone before they
    /// await: an epoch handoff waits (briefly) for every clone of the pack it seals.
    fn current_pack_for(&self, epoch: Epoch) -> Option<ConsensusPack> {
        let current = self.current_pack.lock();
        (current.epoch() == epoch).then(|| current.clone())
    }

    /// Return a clone of the staging pack.
    fn staging(&self) -> Option<StagingPack> {
        self.staging.lock().clone()
    }

    /// Get a static pack file from the cache if available or create and cache if not.
    async fn get_static(&self, epoch: Epoch) -> Result<ConsensusPack, PackError> {
        let pack = self.current_pack();
        if pack.epoch() == epoch {
            return Ok(pack);
        }
        // Purge any cached pack whose background actor thread has died (panic): it would otherwise
        // be served from the cache and silently answer every lookup as not-found. Only rebuild
        // the deque when a dead entry is actually present, and drop the
        // dead packs OUTSIDE the lock — a last-handle `Drop` must not run under the cache
        // lock (same rule as the eviction below; mirrors the pop-front-into-kept pattern in
        // `save`). A dead pack's actor has exited (or is finishing its unwind), so its `Drop`
        // only reaps the thread: the join returns at once and nothing is sealed.
        let dead = {
            let mut recents = self.recent_packs.lock();
            if recents.iter().all(|p| p.is_alive()) {
                Vec::new()
            } else {
                let mut kept = VecDeque::with_capacity(recents.len());
                let mut dead = Vec::new();
                while let Some(p) = recents.pop_front() {
                    if p.is_alive() {
                        kept.push_back(p);
                    } else {
                        dead.push(p);
                    }
                }
                *recents = kept;
                dead
            }
        };
        drop(dead);
        // Evict the oldest entry OUT of the lock scope: a `parking_lot` guard cannot be held across
        // the `.await` below, and if the evicted pack is the last handle its `close()` must not run
        // a blocking `Drop::join` on a tokio worker (let alone while holding the cache
        // lock).
        let evicted = {
            let mut recents = self.recent_packs.lock();
            if let Some(p) = recents.iter().find(|p| p.epoch() == epoch) {
                return Ok(p.clone());
            }
            // Evict before the open+push below so the cache stays capped at PACK_CACHE_SIZE.
            if recents.len() >= Self::PACK_CACHE_SIZE {
                recents.pop_front()
            } else {
                None
            }
        };
        if let Some(old) = evicted {
            old.close().await;
        }
        // `new_epoch` swaps `current_pack` and only THEN seals the previous writer
        // (`seal_previous_pack` stamps the clean-close sentinels and truncates the mmap
        // padding, always before releasing `pack_install`); an import or migration install
        // renames an `epoch-N` directory under the same lock, leaving it briefly absent. A reader
        // that opens the epoch in either window would misreport it (unsealed → `CorruptPack`,
        // absent → missing). So on any failure, wait out the in-flight handoff/install by taking
        // `pack_install`, then retry once. A retry that still fails with the data log present is
        // a damaged or legacy-index pack: heal it read-side (see `heal_static`); a torn/unclean
        // data log stays terminal and surfaces. Safe from re-entrancy: no `get_static` caller
        // holds `pack_install` (new_epoch/replace_current use `open_static` directly).
        // Snapshot the install generation BEFORE the unlocked open below; if an install/handoff
        // completes while we open, the handle may point at a since-replaced inode and must not be
        // cached.
        let gen_before = self.install_generation.load(Ordering::Acquire);
        let pack = match ConsensusPack::open_static(&self.base_path, epoch) {
            Ok(pack) => pack,
            // Genuinely absent and no install in flight: a plain miss, answered without waiting
            // on `pack_install`. Checked in this order: an install moves the live dir aside to
            // `epoch-N.replaced` before the new one appears, so a reader that saw no data log
            // mid-install still sees the aside.
            Err(e) if e.is_missing_static_files() && self.epoch_absent(epoch) => return Err(e),
            Err(_) => {
                // Wait out the in-flight handoff/import, then decide and retry while still
                // holding the lock (a sync open; nothing awaits under it), so a following install
                // cannot slip between the wait and the retry. The epoch we want may now BE the
                // live current pack — e.g. a `get_static(N+1)` that raced `new_epoch(N+1)`:
                // serve it directly instead of `open_static`-ing the active writer (which fails
                // the clean-close sentinel gate and would misreport a healthy epoch as
                // CorruptPack).
                let retried = {
                    let _install = self.pack_install.lock().await;
                    let live = self.current_pack();
                    if live.epoch() == epoch {
                        return Ok(live);
                    }
                    ConsensusPack::open_static(&self.base_path, epoch)
                };
                match retried {
                    Ok(pack) => pack,
                    // Genuinely absent: no data log. (A missing INDEX beside a present data log
                    // is damage to heal, not absence: e.g. a crash between discarding and
                    // recreating the index directories.)
                    Err(e) if e.is_missing_static_files() && self.epoch_absent(epoch) => {
                        return Err(e)
                    }
                    Err(e) => return self.heal_and_reopen(epoch, Some(e)).await,
                }
            }
        };
        // A legacy (pre-v2) sealed epoch opens, but it was indexed under the old digest-key
        // placement, so a by-digest lookup would silently miss present records (the by-number
        // position index is placement-independent). Migrate it — which rebuilds its indexes and
        // re-seals it as v2 — before serving it. One-time per epoch. The live current epoch was
        // returned early above, so this only ever touches a sealed past epoch. A failed migration
        // (a corrupt legacy log) surfaces rather than serving a stale-index handle.
        if pack.is_legacy() {
            pack.close().await;
            return self.heal_and_reopen(epoch, None).await;
        }
        // Final check after grabbing the lock again that another task did not also create the pack.
        // Decide under the brief lock, then release it BEFORE any `.await` — a `parking_lot` guard
        // must not be held across `close()` (same rule as the eviction block above), and the
        // redundant pack's `close()` must not run under the cache lock. Unlikely to trigger but
        // possible.
        let (existing, evicted) = {
            let mut recents = self.recent_packs.lock();
            if let Some(p) = recents.iter().find(|p| p.epoch() == epoch) {
                (Some(p.clone()), None)
            } else if self.install_generation.load(Ordering::Acquire) != gen_before {
                // An install/handoff completed while we were opening `open_static` unlocked, so
                // this handle may point at the pre-install (stale) inode. Serve it
                // to this caller (it holds a correct prefix of the same chain) but
                // do NOT cache it — a later `get_static` opens
                // the freshly-installed inode.
                (None, None)
            } else {
                // Re-check the cap here too: two concurrent opens of distinct uncached epochs can
                // each clear the eviction block above and then both push, overshooting
                // PACK_CACHE_SIZE. Evict the oldest again if needed (closed outside the lock
                // below).
                let evicted =
                    if recents.len() >= Self::PACK_CACHE_SIZE { recents.pop_front() } else { None };
                recents.push_back(pack.clone());
                (None, evicted)
            }
        };
        if let Some(old) = evicted {
            old.close().await; // close the re-evicted pack outside the lock
        }
        if let Some(p) = existing {
            pack.close().await; // close the redundant open outside the lock
            Ok(p)
        } else {
            Ok(pack)
        }
    }

    /// Heal sealed past epoch `epoch` read-side (see [`Self::heal_static`]) and open the result.
    ///
    /// `cause` is the open error that led here, and is what the caller sees if the heal fails (the
    /// heal's own error is logged and remembered by `heal_static`) — unless it is a missing-files
    /// error: the read was routed here because the data log IS present (only an index is
    /// missing), and returning that error would let `get_static_if_present` report a damaged
    /// epoch as one this node does not hold. Then, as without a cause (a legacy pack that opened
    /// but must be migrated first), the heal's own error surfaces.
    ///
    /// The epoch can become the live writer while the heal runs (the heal then stands down, or
    /// fails on the writer's now-unsealed log): serve the live pack rather than `open_static`
    /// the active writer, whose unsealed files fail the clean-close sentinel gate. The reopened
    /// handle is returned uncached; the next `get_static` caches a fresh open.
    async fn heal_and_reopen(
        &self,
        epoch: Epoch,
        cause: Option<PackError>,
    ) -> Result<ConsensusPack, PackError> {
        let healed = self.heal_static(epoch).await;
        let live = self.current_pack();
        if live.epoch() == epoch {
            return Ok(live);
        }
        healed.map_err(|e| match cause {
            Some(cause) if !cause.is_missing_static_files() => cause,
            _ => e,
        })?;
        let pack = ConsensusPack::open_static(&self.base_path, epoch)?;
        if pack.is_legacy() {
            pack.close().await;
            return Err(PackError::CorruptPack(format!(
                "epoch {epoch}: legacy (pre-v2) pack is not yet migrated; retry the read"
            )));
        }
        Ok(pack)
    }

    /// True if the data log of `epoch` exists on disk.
    fn epoch_data_present(&self, epoch: Epoch) -> bool {
        self.base_path.join(format!("epoch-{epoch}")).join(DATA_NAME).is_file()
    }

    /// True if this node holds nothing of `epoch`: no data log, and no `epoch-N.replaced` copy
    /// that an in-flight install moved aside (checked first, since an install removes the live
    /// directory before the replacement appears).
    fn epoch_absent(&self, epoch: Epoch) -> bool {
        !self.base_path.join(format!("epoch-{epoch}.replaced")).exists()
            && !self.epoch_data_present(epoch)
    }

    /// Heal sealed past epoch `epoch` read-side so [`ConsensusPack::open_static`] can serve it:
    /// migrate a legacy (pre-v2) pack, or rebuild a clean v2 pack's derived indexes. See
    /// [`ConsensusPack::build_static_heal`] for what is (and is never) healed.
    ///
    /// The build is a full-epoch WAL replay or copy, so it runs on a blocking thread and outside
    /// `pack_install`: epoch handoffs and imports are not held up behind it. Builds of the same
    /// epoch are serialized, and a second reader finds the work done. Only the install (a few
    /// renames) takes `pack_install`. It is abandoned if the epoch became the live writer
    /// meanwhile, and the pack layer discards it if the epoch's data log was replaced. A
    /// failure is remembered for [`ReadSideHeal::RETRY_BACKOFF`], so repeated reads of a corrupt
    /// epoch fail fast.
    ///
    /// The heal runs as its own task and this only waits for it. A reader whose future is dropped
    /// (an epoch-scoped task aborted at the epoch boundary, a request that timed out) stops
    /// waiting without abandoning the heal: the task keeps the epoch's heal lock until its build
    /// is installed or discarded, so the next reader waits for it instead of starting a second
    /// build, no staging copy is left behind, and a heal that outlasts any one reader still
    /// completes.
    async fn heal_static(&self, epoch: Epoch) -> Result<(), PackError> {
        let heal = ReadSideHeal {
            base_path: self.base_path.clone(),
            current_pack: Arc::downgrade(&self.current_pack),
            pack_install: self.pack_install.clone(),
            install_generation: self.install_generation.clone(),
            heal_locks: self.heal_locks.clone(),
            heal_failures: self.heal_failures.clone(),
        };
        if let Some(recent) = heal.recent_failure(epoch) {
            return Err(recent);
        }
        tokio::spawn(heal.run(epoch))
            .await
            .map_err(|e| PackError::PersistError(format!("epoch {epoch} heal task failed: {e}")))?
    }

    /// Open the sealed static pack for `epoch` if its files exist on disk.
    ///
    /// Distinguishes a genuinely absent epoch (`Ok(None)`, a normal miss) from files that are
    /// present but unreadable (`Err`): a corrupt pack, a damaged or unopenable index, or a
    /// non-`NotFound` I/O failure is a storage READ error that must surface to the caller
    /// instead of being collapsed into a miss.
    async fn get_static_if_present(
        &self,
        epoch: Epoch,
    ) -> Result<Option<ConsensusPack>, ConsensusChainError> {
        self.get_static(epoch)
            .await
            .map(Some)
            .or_else(|error| error.is_missing_static_files().then_some(None).ok_or(error))
            .map_err(Into::into)
    }
}

/// What a read-side heal of a past epoch works with (see `ConsensusChain::heal_static`), owned so
/// the heal can run as its own task. The live pack is held only weakly, so a heal still running at
/// shutdown never keeps the chain from reaching sole ownership
/// ([`ConsensusChain::wait_until_sole_owner`]).
struct ReadSideHeal {
    base_path: PathBuf,
    current_pack: Weak<Mutex<ConsensusPack>>,
    pack_install: Arc<tokio::sync::Mutex<()>>,
    install_generation: Arc<AtomicU64>,
    heal_locks: Arc<Mutex<HashMap<Epoch, Arc<tokio::sync::Mutex<()>>>>>,
    heal_failures: Arc<Mutex<HashMap<Epoch, (std::time::Instant, PackError)>>>,
}

impl ReadSideHeal {
    /// How long a failed read-side heal of an epoch is remembered before it is attempted again.
    const RETRY_BACKOFF: Duration = Duration::from_secs(60);

    /// Heal `epoch` under its heal lock (see `ConsensusChain::heal_static`).
    async fn run(self, epoch: Epoch) -> Result<(), PackError> {
        let epoch_lock = self.heal_locks.lock().entry(epoch).or_default().clone();
        let result = {
            let _serial = epoch_lock.lock().await;
            // Readers queued behind a build that just failed find its failure here rather than
            // each re-running the build.
            match self.recent_failure(epoch) {
                Some(recent) => Err(recent),
                None => {
                    let result = self.build_and_install(epoch).await;
                    // Before the lock is released, so a queued reader sees the outcome.
                    self.record(epoch, &result);
                    result
                }
            }
        };
        let mut locks = self.heal_locks.lock();
        // Drop the map entry once no other reader is waiting on it (the map and this heal hold the
        // only references).
        if Arc::strong_count(&epoch_lock) <= 2 {
            locks.remove(&epoch);
        }
        result
    }

    /// The failure a heal of `epoch` hit within [`Self::RETRY_BACKOFF`], if any.
    fn recent_failure(&self, epoch: Epoch) -> Option<PackError> {
        self.heal_failures
            .lock()
            .get(&epoch)
            .filter(|(failed_at, _)| failed_at.elapsed() < Self::RETRY_BACKOFF)
            .map(|(_, e)| e.clone())
    }

    /// Remember a failed heal of `epoch` for [`Self::RETRY_BACKOFF`], or forget one that succeeded.
    fn record(&self, epoch: Epoch, result: &Result<(), PackError>) {
        match result {
            Ok(()) => {
                self.heal_failures.lock().remove(&epoch);
            }
            Err(e) => {
                warn!(target: "consensus::store", epoch, %e, "read-side heal of a past epoch failed");
                // Not remembered: an environmental failure, or a failure on the epoch that became
                // the live writer meanwhile (its now-unsealed log fails the heal, which is not a
                // verdict on the epoch).
                let environmental =
                    e.is_environmental_index_error() || matches!(e, PackError::IO(_));
                if !environmental && self.live_epoch() != Some(epoch) {
                    self.heal_failures.lock().insert(epoch, (std::time::Instant::now(), e.clone()));
                }
            }
        }
    }

    /// The epoch of the chain's live (writable) pack, or `None` once the chain is gone.
    fn live_epoch(&self) -> Option<Epoch> {
        self.current_pack.upgrade().map(|pack| pack.lock().epoch())
    }

    /// Build the heal on a blocking thread, then install it under `pack_install`.
    async fn build_and_install(&self, epoch: Epoch) -> Result<(), PackError> {
        let base_path = self.base_path.clone();
        let heal = tokio::task::spawn_blocking(move || {
            ConsensusPack::build_static_heal(&base_path, epoch)
        })
        .await
        .map_err(|e| PackError::PersistError(format!("epoch {epoch} heal task failed: {e}")))??;
        // `None`: nothing to do (another reader healed it, or it opens fine).
        let Some(heal) = heal else { return Ok(()) };
        let _install = self.pack_install.lock().await;
        if self.live_epoch() == Some(epoch) {
            // It became the live writer while we built: never swap files under the writer. The
            // dropped heal removes its staging.
            return Ok(());
        }
        ConsensusPack::install_static_heal(&self.base_path, epoch, heal)?;
        // The epoch's files changed: a `get_static` that opened them across the swap must not
        // cache its handle.
        self.install_generation.fetch_add(1, Ordering::Release);
        Ok(())
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

    async fn consensus_output_by_number(
        &self,
        number: u64,
    ) -> eyre::Result<Option<ConsensusOutput>> {
        ConsensusChain::consensus_output_by_number(self, number).await.map_err(Into::into)
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

    async fn get_epoch_stream(&self, epoch: Epoch) -> eyre::Result<(Box<dyn ReadStream>, u64)> {
        ConsensusChain::get_epoch_stream(self, epoch).await.map_err(Into::into)
    }

    fn already_streaming_epoch(&self, epoch: Epoch) -> bool {
        ConsensusChain::already_streaming_epoch(self, epoch)
    }
}

impl ConsensusChainWriter for ConsensusChain {
    async fn save_consensus_output(&self, consensus: ConsensusOutput) -> eyre::Result<u64> {
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

/// Errors returned by [`ConsensusChain`] operations (open, save, stream import, epoch handoff).
#[derive(Debug)]
pub enum ConsensusChainError {
    /// An underlying pack file operation failed; wraps the [`PackError`].
    PackError(PackError),
    /// No current (writable) epoch is set on the chain.
    NoCurrentEpoch,
    /// An I/O error occurred; wraps the [`std::io::Error`].
    IO(std::io::Error),
    /// The current epoch does not contain the latest consensus header.
    EpochMismatch,
    /// The current committee epoch and the previous epoch record are out of sync.
    PrevCommitteeEpochMismatch,
    /// A CRC check failed while reading a record.
    CrcError,
    /// An epoch record database operation failed; wraps the [`EpochDbError`].
    EpochDbError(EpochDbError),
    /// The imported pack file contained no consensus output.
    EmptyImport,
    /// The final consensus output in the imported pack file was invalid.
    InvalidImport,
    /// The chain lacks the complete data needed to stream a pack file to a peer.
    StreamUnavailable,
    /// Tried to save an output whose epoch does not match the current pack epoch
    /// (fields: `pack_epoch`, `epoch`).
    InvalidPackEpoch(Epoch, Epoch),
    /// The pack file is static (sealed) and the requested consensus number is missing,
    /// so the output cannot be saved (field: consensus `number`).
    CantSaveAndNotAvailable(u64),
    /// A consensus output arrived with a number at or below the latest saved consensus number
    /// (fields: `latest`, `number`). Consensus numbers must strictly increase, so this means the
    /// node's view of "latest" and the incoming output stream disagree - e.g. a startup resume
    /// that silently collapsed to a default header at number 0. Failing hard (instead of the old
    /// silent `Ok(0)` skip) turns that state into a loud halt rather than a node that quietly
    /// discards every subsequent output.
    NonMonotonicConsensusNumber {
        /// The latest consensus number already recorded by this chain.
        latest: u64,
        /// The non-increasing number carried by the rejected output.
        number: u64,
    },
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
            ConsensusChainError::CantSaveAndNotAvailable(number) => {
                write!(f, "Pack file is static and Consensus {number} missing, can't save")
            }
            ConsensusChainError::NonMonotonicConsensusNumber { latest, number } => {
                write!(f, "Consensus output number {number} does not advance the latest saved consensus number {latest}")
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
        collections::BTreeMap,
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tn_types::{
        test_genesis, Authority, BlsPublicKey, Committee, ConsensusHeader, ConsensusHeaderDigest,
        ConsensusNumHash, ConsensusOutput, Epoch, EpochRecord, Hash as _,
    };

    use crate::{
        consensus::{ConsensusChain, ConsensusChainError},
        consensus_pack::test::{compare_outputs, make_test_output},
        mem_db::MemDatabase,
    };
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;

    /// An interrupted install leaves `epoch-N.replaced` with `epoch-N` missing (a crash
    /// between the two install renames). Startup must ROLL BACK — restore `epoch-N` from the
    /// aside — not delete it (which would brick startup on the hint epoch or lose a past
    /// epoch). A stale aside next to an existing `epoch-N` is removed.
    #[test]
    fn test_recover_incomplete_installs_rolls_back_and_cleans() {
        let temp_dir = TempDir::with_prefix("recover_installs").expect("temp dir");
        let base = temp_dir.path();

        // Interrupted install: only `epoch-5.replaced` exists (the last good copy), no `epoch-5`.
        std::fs::create_dir(base.join("epoch-5.replaced")).expect("mk aside");
        std::fs::write(base.join("epoch-5.replaced").join("marker"), b"good").expect("marker");

        // Completed install left a stale aside next to a live `epoch-6`.
        std::fs::create_dir(base.join("epoch-6")).expect("mk live");
        std::fs::create_dir(base.join("epoch-6.replaced")).expect("mk stale aside");

        ConsensusChain::recover_incomplete_installs(base);

        // epoch-5 restored from its aside (marker preserved), aside gone.
        assert!(base.join("epoch-5").is_dir(), "epoch-5 must be restored from .replaced");
        assert!(
            base.join("epoch-5").join("marker").exists(),
            "restored epoch-5 must keep its contents"
        );
        assert!(!base.join("epoch-5.replaced").exists(), "the aside must be consumed");
        // epoch-6 untouched, its stale aside removed.
        assert!(base.join("epoch-6").is_dir(), "existing epoch-6 must be left in place");
        assert!(!base.join("epoch-6.replaced").exists(), "a stale aside must be removed");
    }

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
    async fn write_latest_consensus_hint_sets_resume_point() {
        // `db load-state` writes one slot so a restarted node resumes at the imported epoch's final
        // consensus rather than genesis.
        let temp_dir = TempDir::with_prefix("write_hint").unwrap();
        ConsensusChain::write_latest_consensus_hint(temp_dir.path(), 3, 42).expect("write hint");

        // A fresh LatestConsensus (what the node opens at startup) must pick up the hint from the
        // one written slot; the empty second slot reads as (0, 0) and loses reconciliation.
        let latest = LatestConsensus::new(temp_dir.path()).unwrap();
        assert_eq!(latest.epoch(), 3);
        assert_eq!(latest.number(), 42);
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
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save a few legitimate epoch-0 outputs.
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..3u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
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

    /// `ConsensusChain::close` async-closes every background thread it owns (packs,
    /// latest-consensus slot writer, epoch DB) without a blocking `Drop` join, and seals them:
    /// a reopen from the same path finds every saved output.
    #[tokio::test]
    async fn test_consensus_chain_close_seals() {
        let temp_dir = TempDir::with_prefix("test_chain_close").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain_spec: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..3u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain_spec.clone(), i + 1, parent);
            parent = output.digest();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        // Async-close the whole chain (sole reference) instead of dropping.
        consensus_chain.close().await;

        // Reopen from the same path and confirm the outputs survived the close.
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        for i in 1..=3u64 {
            assert!(
                reopened.get_consensus_output_current(i).await.is_ok(),
                "output {i} reads back after chain close"
            );
        }
        reopened.close().await;
    }

    /// A non-increasing consensus number is a hard error, never a silent `Ok(0)` skip.
    ///
    /// Reverting `save_consensus_output` to the silent skip lets a node whose startup resume
    /// collapsed to a default header at number 0 discard every subsequent output while
    /// reporting success (consensus height frozen with no signal); this test rejects that by
    /// requiring `NonMonotonicConsensusNumber` and an unchanged `latest_consensus`.
    #[tokio::test]
    async fn test_save_consensus_output_non_monotonic_rejected() {
        let temp_dir = TempDir::with_prefix("test_non_monotonic").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save legitimate epoch-0 outputs 1 and 2.
        let parent = ConsensusHeader::default().digest();
        let output1 = make_test_output(&committee, 0, chain.clone(), 1, parent);
        let output2 = make_test_output(&committee, 1, chain.clone(), 2, output1.digest());
        consensus_chain.save_consensus_output(output1).await.unwrap();
        consensus_chain.save_consensus_output(output2.clone()).await.unwrap();
        assert_eq!(consensus_chain.latest_consensus.number(), 2);

        // Re-sending an already-saved number (a stale resume replaying output 2) must fail
        // hard instead of silently reporting success.
        let err = consensus_chain
            .save_consensus_output(output2)
            .await
            .expect_err("a non-increasing consensus number must be rejected");
        assert!(
            matches!(
                err,
                ConsensusChainError::NonMonotonicConsensusNumber { latest: 2, number: 2 }
            ),
            "expected NonMonotonicConsensusNumber {{ latest: 2, number: 2 }}, got {err:?}"
        );
        assert_eq!(
            consensus_chain.latest_consensus.number(),
            2,
            "latest_consensus must not change on a rejected output"
        );
    }

    /// Item #13: after a power loss the durable `LatestConsensus` hint can be AHEAD of the
    /// recovered pack, so the executor's re-derived output is refused
    /// (`NonMonotonicConsensusNumber`) and the node crash-loops. `clamp_latest_to_pack`
    /// reconciles the hint to the pack tip so it resumes.
    #[tokio::test]
    async fn test_clamp_latest_to_pack_reconciles_ahead_slot() {
        let temp_dir = TempDir::with_prefix("test_clamp_ahead").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee.clone()).await.unwrap();

        // Save outputs 1 and 2: pack tip = 2, hint = 2.
        let parent = ConsensusHeader::default().digest();
        let output1 = make_test_output(&committee, 0, chain.clone(), 1, parent);
        let output2 = make_test_output(&committee, 1, chain.clone(), 2, output1.digest());
        let output3 = make_test_output(&committee, 2, chain.clone(), 3, output2.digest());
        consensus_chain.save_consensus_output(output1).await.unwrap();
        consensus_chain.save_consensus_output(output2).await.unwrap();
        assert_eq!(consensus_chain.latest_consensus.number(), 2);

        // Simulate the power loss: the slot was fsync'd to 3 while output 3 never reached the pack.
        consensus_chain.latest_consensus.update(0, 3).await;
        assert_eq!(consensus_chain.latest_consensus.number(), 3);

        // Reproduce the crash-loop: the re-derived output 3 is refused by the ahead hint.
        let err = consensus_chain
            .save_consensus_output(output3.clone())
            .await
            .expect_err("an ahead hint must refuse the re-derived output");
        assert!(
            matches!(
                err,
                ConsensusChainError::NonMonotonicConsensusNumber { latest: 3, number: 3 }
            ),
            "expected NonMonotonicConsensusNumber {{ latest: 3, number: 3 }}, got {err:?}"
        );

        // The clamp reconciles the hint to the pack tip (2).
        consensus_chain.clamp_latest_to_pack().await.expect("clamp");
        assert_eq!(consensus_chain.latest_consensus.number(), 2, "hint clamped to the pack tip");

        // Now the re-derived output 3 is accepted and the node makes progress.
        consensus_chain.save_consensus_output(output3).await.expect("output 3 saved after clamp");
        assert_eq!(consensus_chain.latest_consensus.number(), 3);
    }

    /// Item #13, the deterministic epoch-boundary case: the current pack recovered meta-only (no
    /// outputs) while the hint says a number was saved. `latest_consensus_header` returns `None`
    /// there, so the clamp must use `latest_consensus_number` (= `start_consensus_number - 1`).
    #[tokio::test]
    async fn test_clamp_latest_to_pack_meta_only_pack() {
        let temp_dir = TempDir::with_prefix("test_clamp_meta_only").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee).await.unwrap();
        // Meta-only epoch-0 pack: start_consensus_number == 1, no outputs -> latest number 0.
        assert_eq!(consensus_chain.latest_consensus.number(), 0);

        // A not-ahead hint is a no-op.
        consensus_chain.clamp_latest_to_pack().await.expect("clamp no-op");
        assert_eq!(consensus_chain.latest_consensus.number(), 0);

        // Power loss left the hint ahead (an output was fsync'd to the slot but not the pack).
        consensus_chain.latest_consensus.update(0, 5).await;
        assert_eq!(consensus_chain.latest_consensus.number(), 5);
        consensus_chain.clamp_latest_to_pack().await.expect("clamp");
        assert_eq!(
            consensus_chain.latest_consensus.number(),
            0,
            "meta-only pack: hint clamped to start_consensus_number - 1"
        );
    }

    /// A power loss can leave the durable latest-consensus marker one output ahead of the pack
    /// that recovery rebuilds at the next open. The clamp the node runs right after opening must
    /// lower the marker to the pack tail after a real reopen. It is in-memory only, so a second
    /// open must clamp again, and the next save must be accepted and leave a marker that the
    /// following open agrees with.
    #[tokio::test]
    async fn test_clamp_latest_to_pack_across_reopen() {
        let temp_dir = TempDir::with_prefix("test_marker_ahead").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        let k = 5u64;
        let mut parent = ConsensusHeader::default().digest();
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.clamp_latest_to_pack().await.unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        for i in 0..k {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        consensus_chain.persist_current().await.expect("persist");
        consensus_chain.close().await;
        // the marker for output k + 1 reached disk but the output itself did not
        ConsensusChain::write_latest_consensus_hint(temp_dir.path(), 0, k + 1).expect("write hint");

        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("first reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the reopen reads the ahead marker");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_epoch(), 0);
        assert_eq!(reopened.latest_consensus_number(), k, "marker clamped to the pack tail");
        reopened.close().await;

        // the clamp never reaches the slot files, so a second open must clamp again
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("second reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the slot files still hold k + 1");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_epoch(), 0);
        assert_eq!(reopened.latest_consensus_number(), k, "clamp repeats on every open");
        let next = make_test_output(&committee, (k % 4) as usize, chain.clone(), k + 1, parent);
        reopened
            .save_consensus_output(next)
            .await
            .expect("the output after the pack tail must be accepted");
        reopened.persist_current().await.expect("persist");
        reopened.close().await;
        // slot1 still holds the k + 1 hint, so only the slot the save flipped to shows its marker
        let mut slot2 =
            std::fs::File::open(temp_dir.path().join("consensus_slot2")).expect("open slot2");
        assert_eq!(
            LatestConsensus::read_slot(&mut slot2).expect("read slot2"),
            (0, k + 1),
            "the save wrote its marker to consensus_slot2"
        );

        // both slots now hold k + 1, so the next open agrees with the pack before any clamp
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("third reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the reopen reads k + 1");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the clamp leaves k + 1");
        let latest = reopened.consensus_header_latest().await.unwrap().expect("latest header");
        assert_eq!(latest.number, k + 1);
        reopened.close().await;
    }

    /// The first save of a new epoch fsyncs the marker because the epoch changed, while the output
    /// itself is only msynced at the next persist. After a power loss the marker names the new
    /// epoch's first output and that pack holds only its epoch meta. The clamp after a real
    /// reopen must keep the new epoch and lower the number to the previous epoch's last output,
    /// and the first output of the new epoch must be accepted afterwards.
    #[tokio::test]
    async fn test_clamp_latest_to_pack_epoch_ahead_across_reopen() {
        let temp_dir = TempDir::with_prefix("test_marker_epoch_ahead").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let committee1 = committee.advance_epoch_for_test(1);

        let k = 5u64;
        let mut parent = ConsensusHeader::default().digest();
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.clamp_latest_to_pack().await.unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        for i in 0..k {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        consensus_chain.persist_current().await.expect("persist");
        let epoch0_record =
            EpochRecord { final_consensus: ConsensusNumHash::new(k, parent), ..previous_epoch };
        // opens and persists the epoch 1 pack with only its epoch meta
        consensus_chain.new_epoch(epoch0_record, committee1.clone()).await.unwrap();
        consensus_chain.close().await;
        // the marker for epoch 1's first output reached disk but the output itself did not
        ConsensusChain::write_latest_consensus_hint(temp_dir.path(), 1, k + 1).expect("write hint");

        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("first reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the reopen reads the ahead marker");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_epoch(), 1, "marker keeps the new epoch");
        assert_eq!(reopened.latest_consensus_number(), k, "marker clamped to the previous final");
        // nothing is saved in epoch 1 yet, callers fall back to the last executed header
        assert!(reopened.consensus_header_latest().await.unwrap().is_none());
        reopened.close().await;

        // the clamp never reaches the slot files, so a second open must clamp again
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("second reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the slot files still hold k + 1");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_epoch(), 1);
        assert_eq!(reopened.latest_consensus_number(), k, "clamp repeats on every open");
        let next = make_test_output(&committee1, (k % 4) as usize, chain.clone(), k + 1, parent);
        reopened
            .save_consensus_output(next)
            .await
            .expect("the first output of the new epoch must be accepted");
        assert_eq!(reopened.latest_consensus_number(), k + 1);
        reopened.persist_current().await.expect("persist");
        reopened.close().await;
        // slot1 still holds the k + 1 hint, so only the slot the save flipped to shows its marker
        let mut slot2 =
            std::fs::File::open(temp_dir.path().join("consensus_slot2")).expect("open slot2");
        assert_eq!(
            LatestConsensus::read_slot(&mut slot2).expect("read slot2"),
            (1, k + 1),
            "the save wrote its marker to consensus_slot2"
        );

        // both slots now hold k + 1, so the next open agrees with the pack before any clamp
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("third reopen");
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the reopen reads k + 1");
        reopened.clamp_latest_to_pack().await.unwrap();
        assert_eq!(reopened.latest_consensus_epoch(), 1);
        assert_eq!(reopened.latest_consensus_number(), k + 1, "the clamp leaves k + 1");
        let latest = reopened.consensus_header_latest().await.unwrap().expect("latest header");
        assert_eq!(latest.number, k + 1);
        reopened.close().await;
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
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 1000;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest();
            outputs.push(consensus_output.clone());
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
        }
        let last = outputs.last().unwrap();
        let mut epoch_record = previous_epoch.clone();
        epoch_record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());
        for i in 0..num_outputs {
            let output_db =
                consensus_chain.get_consensus_output_current(i as u64 + 1).await.unwrap();
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }

        consensus_chain.persist_current().await.expect("persist");
        //drop(consensus_chain);

        let temp_dir2 = TempDir::with_prefix("test_consensus_pack2").expect("temp dir");
        let consensus_chain2 =
            ConsensusChain::new(temp_dir2.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.epochs().save_record(epoch_record.clone()).await.expect("save epoch");
        use tokio::io::AsyncReadExt as _;
        let (stream, len) = consensus_chain.get_epoch_stream(0).await.unwrap();
        consensus_chain2
            .stream_import(stream.take(len), &epoch_record, &previous_epoch, Duration::from_secs(5))
            .await
            .unwrap();
        consensus_chain2.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        for i in 0..num_outputs {
            let output_db = consensus_chain2
                .get_consensus_output_current(i as u64 + 1)
                .await
                .unwrap_or_else(|_| panic!("Failed to get on {i}"));
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }
    }

    /// A node that crashes mid-epoch can leave the pack's data file longer than its indexes
    /// (a torn write). On restart `ConsensusChain::new` must heal that pack rather than fail to
    /// open, otherwise the node cannot restart. This opens the latest epoch with
    /// `open_append_exists` (which runs `recover_pack`); the old `open_static` path returned
    /// `CorruptPack` here.
    #[tokio::test]
    async fn test_new_heals_torn_write_on_restart() {
        use crate::consensus_pack::DATA_NAME;
        use std::io::Write as _;

        let temp_dir = TempDir::with_prefix("test_torn_write").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        // Write a handful of outputs and persist, then drop the chain (clean on-disk state).
        let mut outputs = Vec::new();
        {
            let consensus_chain =
                ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
            consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
            let mut parent = ConsensusHeader::default().digest();
            for i in 0..5u64 {
                let output =
                    make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
                parent = output.digest();
                outputs.push(output.clone());
                consensus_chain.save_consensus_output(output).await.unwrap();
            }
            consensus_chain.persist_current().await.expect("persist");
        }

        // Simulate a torn write: append garbage to the data file so its length runs ahead of the
        // indexes' tracked data-file length (exactly what files_consistent rejects).
        {
            let data_path = temp_dir.path().join("epoch-0").join(DATA_NAME);
            let mut f =
                std::fs::OpenOptions::new().append(true).open(&data_path).expect("open data file");
            f.write_all(&[0xAB; 64]).expect("append garbage");
            f.sync_all().expect("sync");
        }

        // Restart: new() must open + heal the latest epoch pack rather than erroring.
        let reopened = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone())
            .expect("heal on open");
        // All previously saved outputs are still readable after healing.
        for (i, output) in outputs.iter().enumerate() {
            let got = reopened
                .get_consensus_output_current(i as u64 + 1)
                .await
                .expect("output readable after heal");
            compare_outputs(&got, output);
        }
    }

    /// #1075: `PackError::is_missing_static_files` must be true ONLY when epoch files are
    /// absent on disk (io `NotFound` from the data-file or an index-file open), and false for
    /// files that are present but unreadable - the distinction `get_static_if_present` uses to
    /// keep storage read errors from masquerading as misses.
    #[test]
    fn test_is_missing_static_files_classifies_absent_vs_unreadable() {
        use crate::{
            archive::error::{load_header::LoadHeaderError, open::OpenError},
            consensus_pack::{ConsensusPack, PackError, DATA_NAME},
        };

        let temp_dir = TempDir::with_prefix("test_missing_static").expect("temp dir");

        // Never-created epoch: the data-file open fails with io NotFound.
        let absent = ConsensusPack::open_static(temp_dir.path(), 7)
            .expect_err("open_static of a never-created epoch must fail");
        assert!(
            absent.is_missing_static_files(),
            "io NotFound on the data file must classify as missing: {absent:?}"
        );

        // Present but unreadable: a garbage data file fails the header load, not NotFound.
        let epoch_dir = temp_dir.path().join("epoch-7");
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        std::fs::write(epoch_dir.join(DATA_NAME), [0xAB; 64]).expect("write garbage data file");
        let unreadable = ConsensusPack::open_static(temp_dir.path(), 7)
            .expect_err("open_static of a garbage data file must fail");
        assert!(
            !unreadable.is_missing_static_files(),
            "header damage must NOT classify as missing: {unreadable:?}"
        );

        // The index-file arm: an interrupted import cleanup unlinks an epoch directory entry
        // by entry, so an index file can be the one that is gone while the data file still
        // opens. io NotFound there is still "absent on disk", preserving the staging fallback
        // the pre-classifier lookup had during that window.
        let index_missing = PackError::Open(std::sync::Arc::new(OpenError::IndexFileOpen(
            LoadHeaderError::IO(std::io::Error::from(std::io::ErrorKind::NotFound)),
        )));
        assert!(
            index_missing.is_missing_static_files(),
            "io NotFound on an index file must classify as missing: {index_missing:?}"
        );

        // Same channel, any other io kind: present but unreadable, a read error.
        let index_unreadable = PackError::Open(std::sync::Arc::new(OpenError::IndexFileOpen(
            LoadHeaderError::IO(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        )));
        assert!(
            !index_unreadable.is_missing_static_files(),
            "a non-NotFound index failure must NOT classify as missing: {index_unreadable:?}"
        );
    }

    /// #1075 regression: `consensus_header_by_digest` must distinguish a non-current epoch whose
    /// static pack is present but UNREADABLE (a storage read error, `Err`) from an epoch that
    /// was never on disk (a confirmed miss, `Ok(None)`). The startup restore guard treats `None`
    /// as "no record" and instructs the operator to delete chain data; before this distinction
    /// an unreadable pack was collapsed into that same `None`.
    #[tokio::test]
    async fn test_header_by_digest_distinguishes_unreadable_pack_from_absent() {
        use crate::consensus_pack::DATA_NAME;

        let temp_dir = TempDir::with_prefix("test_unreadable_pack").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        // Two outputs saved sequentially (consecutive consensus numbers, digest-chained).
        let genesis_digest = ConsensusHeader::default().digest();
        let first = make_test_output(&committee, 1, chain.clone(), 1, genesis_digest);
        let first_digest: ConsensusHeaderDigest = first.digest();
        consensus_chain.save_consensus_output(first).await.unwrap();
        let second = make_test_output(&committee, 2, chain.clone(), 2, first_digest);
        let second_digest: ConsensusHeaderDigest = second.digest();
        consensus_chain.save_consensus_output(second).await.unwrap();
        consensus_chain.persist_current().await.expect("persist");

        // Positive control: the lookup plumbing resolves a header that is really there, so the
        // negative cases below cannot pass vacuously.
        let found = consensus_chain
            .consensus_header_by_digest(0, second_digest)
            .await
            .expect("current-epoch lookup must not error")
            .expect("saved header must resolve");
        assert_eq!(found.number, 2, "resolved header should be the last saved output");

        // An epoch that never existed on disk is a confirmed miss, not an error.
        let absent = consensus_chain.consensus_header_by_digest(99, second_digest).await;
        assert!(
            matches!(absent, Ok(None)),
            "a never-created epoch must resolve Ok(None): {absent:?}"
        );

        // An epoch whose static files are present but unreadable is a storage read ERROR; it
        // must never be conflated with the miss above (the restore guard would tell the
        // operator to delete recoverable chain data).
        let epoch_dir = temp_dir.path().join("epoch-1");
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        std::fs::write(epoch_dir.join(DATA_NAME), [0xAB; 64]).expect("write garbage data file");
        let unreadable = consensus_chain.consensus_header_by_digest(1, second_digest).await;
        assert!(
            unreadable.is_err(),
            "a present-but-unreadable static pack must surface as Err: {unreadable:?}"
        );
    }

    /// `get_static` retries `open_static` once under `pack_install` so a healthy epoch
    /// mapped mid-handoff (padded, not-yet-sentineled) is not misreported as corrupt. The retry
    /// must still surface GENUINE at-rest corruption rather than mask it, and must terminate
    /// without deadlocking on the lock. (The transient-handoff benefit itself — a racing writer
    /// sealing the epoch during the retry — is exercised by
    /// `test_new_epoch_stream_import_race`.)
    #[tokio::test]
    async fn test_get_static_retry_still_surfaces_corruption() {
        use crate::consensus_pack::DATA_NAME;

        let temp_dir = TempDir::with_prefix("test_get_static_retry").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee).await.unwrap();

        // A present-but-unreadable static pack for a non-current epoch: `get_static` must return
        // `Err` after its single retry — the retry re-opens under `pack_install` (no
        // deadlock, no infinite loop) and does not mask genuine corruption as a miss.
        let epoch_dir = temp_dir.path().join("epoch-1");
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        std::fs::write(epoch_dir.join(DATA_NAME), [0xAB; 64]).expect("write garbage data file");
        let result = consensus_chain.get_static(1).await;
        assert!(
            result.is_err(),
            "genuine at-rest corruption must surface through get_static's retry: {result:?}"
        );
        // The failed heal is remembered, so a second read fails fast instead of re-running it.
        assert!(
            consensus_chain.heal_failures.lock().contains_key(&1),
            "a failed read-side heal must be backed off"
        );
        assert!(consensus_chain.get_static(1).await.is_err(), "a backed-off epoch still fails");
    }

    /// A sealed past epoch whose DATA log is clean but whose digest index will not open (e.g. an
    /// index-format change, or at-rest index damage) self-heals on read: `get_static` rebuilds the
    /// derived indexes from the WAL and returns the data instead of refusing. (A torn/unclean DATA
    /// log is NOT healed this way — see `test_get_static_retry_still_surfaces_corruption`.)
    #[tokio::test]
    async fn test_get_static_rebuilds_indexes_for_clean_pack() {
        use crate::consensus_pack::{pack_unsealed_version, DATA_NAME};

        let temp_dir = TempDir::with_prefix("test_static_index_heal").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let epoch0 = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(epoch0.clone(), committee.clone()).await.unwrap();

        // Three outputs in epoch 0.
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeaderDigest::default();
        for n in 1..=3u64 {
            let output = make_test_output(&committee, (n as usize) % 4, chain.clone(), n, parent);
            parent = output.digest();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }

        // Advance to epoch 1: this seals epoch 0 (clean-close sentinel) and makes it a sealed past
        // epoch that is opened read-only from now on.
        let record0 = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            parent_hash: epoch0.digest(),
            final_consensus: ConsensusNumHash { number: 3, hash: ConsensusHeaderDigest::default() },
            ..Default::default()
        };
        let committee1 = committee.advance_epoch_for_test(1);
        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(record0.clone(), committee1).await.unwrap();
        consensus_chain.epochs().save_record(record0).await.unwrap();

        // Break epoch 0's consensus-digest index so it will not open, leaving the DATA log (and its
        // clean-close sentinel) intact — truncating the hdx below its header forces an index-open
        // failure without touching the source of truth.
        let hdx = temp_dir.path().join("epoch-0").join("hash").join("index.hdx");
        assert!(hdx.exists(), "sealed epoch 0 must have a digest index at {}", hdx.display());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&hdx)
            .expect("open hdx")
            .set_len(16)
            .expect("truncate hdx");
        // Precondition for the read-side heal: the DATA log is still cleanly sealed.
        let clean = pack_unsealed_version(&temp_dir.path().join("epoch-0").join(DATA_NAME), 0);
        assert!(
            matches!(clean, Some((_, false))),
            "epoch 0 data must be cleanly sealed: {clean:?}"
        );

        let data_path = temp_dir.path().join("epoch-0").join(DATA_NAME);
        let data_before = std::fs::read(&data_path).expect("read data");
        let modified_before = std::fs::metadata(&data_path).and_then(|m| m.modified()).ok();

        // Reading a past-epoch output opens epoch 0 read-only, hits the broken index, and must
        // self-heal (rebuild from the WAL) rather than error.
        let got = consensus_chain
            .consensus_output_by_number(1)
            .await
            .expect("read must self-heal a clean pack's broken index, not error")
            .expect("output 1 must resolve after the index rebuild");
        compare_outputs(&got, &outputs[0]);

        // A second read hits the now-rebuilt index and still resolves correctly.
        let again = consensus_chain
            .consensus_header_by_number(3)
            .await
            .expect("second read must not error")
            .expect("output 3 must resolve from the rebuilt index");
        assert_eq!(again.digest(), outputs[2].consensus_header().digest());

        // The heal rebuilt only the derived indexes: the sealed data log was never opened for
        // writing, so a crash mid-heal could not have left it unsealed.
        assert_eq!(std::fs::read(&data_path).expect("read data"), data_before, "data rewritten");
        assert_eq!(
            std::fs::metadata(&data_path).and_then(|m| m.modified()).ok(),
            modified_before,
            "the read-side heal must not write to the data log"
        );
    }

    /// A past epoch whose data log is present but whose index directory is missing (e.g. a crash
    /// between discarding and recreating the index directories) is damaged, not absent: a read
    /// rebuilds the index instead of reporting the epoch as not held.
    #[tokio::test]
    async fn test_get_static_heals_missing_index_dir() {
        let temp_dir = TempDir::with_prefix("test_static_missing_index").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, last) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        consensus_chain.persist_current().await.unwrap();
        consensus_chain
            .new_epoch(record0.clone(), committee.advance_epoch_for_test(1))
            .await
            .unwrap();
        consensus_chain.epochs().save_record(record0).await.unwrap();

        std::fs::remove_dir_all(temp_dir.path().join("epoch-0").join("hash"))
            .expect("remove the consensus digest index dir");

        let header = consensus_chain
            .consensus_header_by_digest(0, last)
            .await
            .expect("a missing index must heal, not error")
            .expect("a present epoch with a missing index must not read as absent");
        assert_eq!(header.number, 3);
    }

    /// A reader that stops waiting on a read-side heal (an aborted task, a timed-out request) must
    /// not abandon the heal: the build still installs, the next reader waits for it instead of
    /// starting a second one, and no staging copy is left behind.
    #[tokio::test]
    async fn test_a_cancelled_heal_still_completes_without_leftovers() {
        let temp_dir = TempDir::with_prefix("test_cancelled_heal").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, last) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        consensus_chain.persist_current().await.unwrap();
        consensus_chain
            .new_epoch(record0.clone(), committee.advance_epoch_for_test(1))
            .await
            .unwrap();
        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::remove_dir_all(epoch_dir.join("hash")).expect("remove the digest index dir");

        // Poll the read once (it starts the heal) and drop it.
        let cancelled =
            tokio::time::timeout(Duration::ZERO, consensus_chain.get_static(0)).await.is_err();
        let pack = consensus_chain.get_static(0).await.expect("the next read gets the healed pack");
        assert!(pack.contains_consensus_header(last).await, "the heal was installed");
        // Room for a build the cancelled read orphaned to finish, so a leak would show.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let leftovers: Vec<_> = std::fs::read_dir(&epoch_dir)
            .expect("read epoch dir")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".reindex"))
            .collect();
        assert!(leftovers.is_empty(), "cancelled={cancelled}: staging left behind: {leftovers:?}");
    }

    /// A past epoch whose index directory is missing but whose data log is present and damaged
    /// (here unsealed, so the heal refuses) is unreadable, not absent: the read must surface an
    /// error, never `Ok(None)`, or a restart would prime consensus number 0 from "not held".
    #[tokio::test]
    async fn test_missing_index_with_a_damaged_log_is_not_reported_absent() {
        use crate::consensus_pack::DATA_NAME;

        let temp_dir = TempDir::with_prefix("test_missing_index_damaged").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, last) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        consensus_chain.persist_current().await.unwrap();
        consensus_chain
            .new_epoch(record0.clone(), committee.advance_epoch_for_test(1))
            .await
            .unwrap();
        consensus_chain.epochs().save_record(record0).await.unwrap();

        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::remove_dir_all(epoch_dir.join("hash")).expect("remove the digest index dir");
        // Strip the clean-close sentinel: an unsealed v2 log is refused by the read-side heal.
        let data = std::fs::OpenOptions::new()
            .write(true)
            .open(epoch_dir.join(DATA_NAME))
            .expect("open data log");
        let len = data.metadata().expect("meta").len();
        data.set_len(len - crate::archive::data_file::SENTINEL_LEN).expect("strip sentinel");
        drop(data);

        let result = consensus_chain.consensus_header_by_digest(0, last).await;
        assert!(
            result.is_err(),
            "a present but unhealable epoch must surface an error, not absence: {result:?}"
        );
    }

    /// Replaying consensus over an imported (static) epoch advances the latest pointer without
    /// rewriting anything — but only for the output the pack holds. A different output under a
    /// stored number must be refused, as the writable save refuses it, never reported persisted.
    #[tokio::test]
    async fn test_static_replay_refuses_a_different_output_under_a_stored_number() {
        use crate::consensus_pack::{ConsensusPack, PackError};

        let temp_dir = TempDir::with_prefix("test_static_replay_conflict").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, _) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        consensus_chain.persist_current().await.unwrap();
        consensus_chain
            .new_epoch(record0.clone(), committee.advance_epoch_for_test(1))
            .await
            .unwrap();

        // Make the sealed epoch-0 pack the current one, as an imported epoch is, and rewind the
        // latest pointer so a replay of number 2 is admitted.
        let imported = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
        assert!(imported.is_static());
        let replaced = std::mem::replace(&mut *consensus_chain.current_pack.lock(), imported);
        replaced.close().await;
        consensus_chain.latest_consensus.update(0, 1).await;

        let stored = consensus_chain
            .get_consensus_output_current(2)
            .await
            .expect("output 2 is in the imported pack");
        let other =
            make_test_output(&committee, 3, chain.clone(), 2, ConsensusHeaderDigest::default());
        assert_ne!(other.digest(), stored.digest(), "precondition: a different output");
        let err = consensus_chain
            .save_consensus_output(other)
            .await
            .expect_err("a different output under a stored number must be refused");
        assert!(
            matches!(
                err,
                ConsensusChainError::PackError(PackError::ConflictingOutput { number: 2, .. })
            ),
            "got {err:?}"
        );
        assert_eq!(consensus_chain.latest_consensus.number(), 1, "nothing advanced");

        // The pack's own output replays as a no-op that advances the pointer.
        consensus_chain.save_consensus_output(stored).await.expect("the same output replays");
        assert_eq!(consensus_chain.latest_consensus.number(), 2);
    }

    /// A read-side heal can finish after its epoch became the live writer (the heal then stands
    /// down). The caller must get the live pack, not a static open of the active writer's epoch.
    #[tokio::test]
    async fn test_heal_and_reopen_serves_the_live_pack() {
        let temp_dir = TempDir::with_prefix("test_heal_live").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, _) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        consensus_chain.persist_current().await.unwrap();
        consensus_chain
            .new_epoch(record0.clone(), committee.advance_epoch_for_test(1))
            .await
            .unwrap();

        // Epoch 0 is now a healthy sealed past epoch, so its heal has nothing to do. Make an
        // epoch-0 writer the live pack, as if the epoch went live while the heal ran. It
        // lives outside the chain's directory, so the sealed files the heal inspects stay
        // untouched.
        let live_dir = TempDir::with_prefix("test_heal_live_writer").expect("temp dir");
        let epoch0 = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let live0 = crate::consensus_pack::ConsensusPack::open_append(
            live_dir.path(),
            epoch0,
            committee.clone(),
        )
        .expect("open an epoch-0 writer");
        let replaced = std::mem::replace(&mut *consensus_chain.current_pack.lock(), live0);
        replaced.close().await;

        let pack = consensus_chain
            .heal_and_reopen(0, None)
            .await
            .expect("the heal of a healthy sealed epoch succeeds");
        assert_eq!(pack.epoch(), 0);
        assert!(
            !pack.is_static(),
            "the live writer must be served, not a static open of its epoch"
        );
    }

    /// Build a chain whose current epoch 0 holds outputs 1..=3, and the record that closes it.
    async fn chain_with_epoch0_outputs(
        temp_dir: &TempDir,
        committee: &Committee,
        chain: &Arc<RethChainSpec>,
    ) -> (ConsensusChain, EpochRecord, ConsensusHeaderDigest) {
        let epoch0 = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(epoch0.clone(), committee.clone()).await.unwrap();
        let mut parent = ConsensusHeaderDigest::default();
        for n in 1..=3u64 {
            let output = make_test_output(committee, (n as usize) % 4, chain.clone(), n, parent);
            parent = output.digest();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        let record0 = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            parent_hash: epoch0.digest(),
            final_consensus: ConsensusNumHash { number: 3, hash: parent },
            ..Default::default()
        };
        (consensus_chain, record0, parent)
    }

    /// The epoch handoff seals the previous epoch's pack before `new_epoch` returns, even while a
    /// reader still holds a clone of it. The next epoch's subscriber reads that epoch right away
    /// (`latest_consensus_header_from_pack`) and `get_static` accepts only a sealed pack. When the
    /// seal was left to the straggling clone's `Drop`, that read failed as `CorruptPack` and the
    /// subscriber halted the node.
    #[tokio::test]
    async fn test_new_epoch_seals_previous_even_with_live_clone() {
        let temp_dir = TempDir::with_prefix("test_handoff_live_clone").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, _) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;

        // A reader holds the live epoch-0 pack across the handoff.
        let held = consensus_chain.current_pack();
        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(record0, committee.advance_epoch_for_test(1)).await.unwrap();

        let latest = consensus_chain
            .latest_consensus_header_from_pack(0)
            .await
            .expect("the previous epoch must be readable right after the handoff")
            .expect("epoch 0 holds outputs");
        assert_eq!(latest.number, 3);
        // The straggler's later request fails cleanly rather than touching the sealed pack.
        assert!(
            held.latest_consensus_header().await.is_err(),
            "a request on a clone of a sealed pack must fail cleanly"
        );
    }

    /// `new_epoch` drops a cached read-only handle for the epoch it opens for writing. After a
    /// clean restart right after an epoch opened (a sealed, meta-only `epoch-N` on disk), a read
    /// can cache `epoch-N` before `new_epoch(N)` reaches it. Once N ends, reads of N must see the
    /// outputs written during N, not that stale meta-only view.
    #[tokio::test]
    async fn test_new_epoch_evicts_stale_cached_handle() {
        use crate::consensus_pack::ConsensusPack;

        let temp_dir = TempDir::with_prefix("test_handoff_stale_cache").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (consensus_chain, record0, mut parent) =
            chain_with_epoch0_outputs(&temp_dir, &committee, &chain).await;
        let committee1 = committee.advance_epoch_for_test(1);

        // A sealed, meta-only epoch 1 already on disk, and a read that caches it while epoch 0 is
        // still the live pack.
        ConsensusPack::open_append(temp_dir.path(), record0.clone(), committee1.clone())
            .expect("create meta-only epoch 1")
            .close()
            .await;
        consensus_chain.get_static(1).await.expect("a meta-only epoch 1 opens read-only");

        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(record0.clone(), committee1.clone()).await.unwrap();
        for n in 4..=5u64 {
            let output = make_test_output(&committee1, (n as usize) % 4, chain.clone(), n, parent);
            parent = output.digest();
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        let record1 = EpochRecord {
            epoch: 1,
            committee: committee1.bls_keys().iter().copied().collect(),
            next_committee: committee1.bls_keys().iter().copied().collect(),
            parent_hash: record0.digest(),
            final_consensus: ConsensusNumHash { number: 5, hash: parent },
            ..Default::default()
        };
        consensus_chain.persist_current().await.unwrap();
        consensus_chain.new_epoch(record1, committee.advance_epoch_for_test(2)).await.unwrap();

        let latest = consensus_chain
            .latest_consensus_header_from_pack(1)
            .await
            .expect("epoch 1 must be readable")
            .expect("epoch 1 must show the outputs written while it was live");
        assert_eq!(latest.number, 5);
    }

    /// An install over a missing `epoch-N` whose previous copy survives only as
    /// `epoch-N.replaced` (an earlier install whose rename AND restore both failed) must restore
    /// that copy first, never delete it: if this install's rename fails too, the previous copy is
    /// still in place.
    #[test]
    fn test_install_keeps_the_only_copy_when_the_install_fails() {
        let tmp = TempDir::with_prefix("test_install_only_copy").expect("temp dir");
        let base = tmp.path();
        std::fs::create_dir(base.join("epoch-7.replaced")).expect("mk aside");
        std::fs::write(base.join("epoch-7.replaced").join("marker"), b"only copy").expect("marker");

        // The staged import does not exist, so the install rename fails.
        let missing_import = base.join("import-7").join("epoch-7");
        assert!(ConsensusChain::install_imported_epoch_dir(base, 7, &missing_import).is_err());
        assert_eq!(
            std::fs::read(base.join("epoch-7").join("marker")).expect("previous copy"),
            b"only copy",
            "a failed install must leave the only previous copy in place"
        );
    }

    /// `install_imported_epoch_dir` must never unlink the live epoch dir before the
    /// new one is safely in place. On success the import replaces it and the rename-aside
    /// backup is cleaned; on a failed install rename the old dir is restored (same inode), so a
    /// live writer is never left on an unlinked/absent path.
    #[test]
    fn test_install_imported_epoch_dir_atomic_swap() {
        let tmp = TempDir::with_prefix("test_install_epoch").expect("temp dir");
        let base = tmp.path();
        let make_dir = |p: &std::path::Path, marker: &str| {
            std::fs::create_dir_all(p).expect("mkdir");
            std::fs::write(p.join("data"), marker).expect("write marker");
        };
        let base_dir = base.join("epoch-0");
        let aside = base.join("epoch-0.replaced");

        // Success: an existing epoch-0 is replaced by the import; the aside is cleaned.
        make_dir(&base_dir, "OLD");
        let import = base.join("import-0");
        make_dir(&import, "NEW");
        ConsensusChain::install_imported_epoch_dir(base, 0, &import).expect("install succeeds");
        assert_eq!(
            std::fs::read_to_string(base_dir.join("data")).unwrap(),
            "NEW",
            "new content installed"
        );
        assert!(!import.exists(), "import dir consumed by the rename");
        assert!(!aside.exists(), "rename-aside backup cleaned on success");

        // Failure-restore: a missing import makes the install rename fail (ENOENT); the old dir
        // must be restored with its original content and no aside left behind.
        make_dir(&base_dir, "KEEP");
        let missing = base.join("import-does-not-exist");
        let err = ConsensusChain::install_imported_epoch_dir(base, 0, &missing);
        assert!(err.is_err(), "install must fail when the import dir is absent: {err:?}");
        assert!(base_dir.exists(), "old epoch-0 must still exist after a failed install");
        assert_eq!(
            std::fs::read_to_string(base_dir.join("data")).unwrap(),
            "KEEP",
            "old content restored (the live dir was never unlinked)"
        );
        assert!(!aside.exists(), "aside restored back into place, not left behind");
    }

    /// A partial stream of the in-progress (incomplete) current epoch must deliver a verifiable
    /// prefix: importing `[0, output_end(k))` yields exactly outputs `1..=k`.
    #[tokio::test]
    async fn test_consensus_partial_stream() {
        use tokio::io::AsyncReadExt as _;

        let temp_dir = TempDir::with_prefix("test_partial_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save outputs but DO NOT finish the epoch — this is the in-progress current epoch.
        let num_outputs = 20u64;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }

        // Stream a verifiable prefix up to consensus number `k` (well before the latest).
        let k = 12u64;
        let (stream, len) = consensus_chain
            .get_partial_epoch_stream(0, k)
            .await
            .expect("partial stream of in-progress epoch");
        // The network layer enforces the byte limit; emulate that here with `take(len)`.
        let limited = stream.take(len);

        // The importer verifies against an epoch record whose final_consensus is the stop point.
        let cutoff = &outputs[(k - 1) as usize];
        assert_eq!(cutoff.number(), k);
        let mut partial_record = previous_epoch.clone();
        partial_record.final_consensus = ConsensusNumHash::new(cutoff.number(), cutoff.digest());

        let temp_dir2 = TempDir::with_prefix("test_partial_dst").expect("temp dir");
        let consensus_chain2 =
            ConsensusChain::new(temp_dir2.path().to_owned(), committee.clone()).unwrap();
        consensus_chain2
            .stream_import(limited, &partial_record, &previous_epoch, Duration::from_secs(5))
            .await
            .expect("import verifiable partial prefix");
        consensus_chain2.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Outputs 1..=k are present and match.
        for i in 0..k {
            let output_db =
                consensus_chain2.get_consensus_output_current(i + 1).await.expect("prefix output");
            compare_outputs(&output_db, &outputs[i as usize]);
        }
        // Nothing past the cutoff was streamed.
        assert!(
            consensus_chain2.get_consensus_output_current(k + 1).await.is_err(),
            "outputs past the partial cutoff must not be present"
        );
    }

    /// `import_partial_to_staging` must produce a readable verified prefix in a side dir WITHOUT
    /// touching the live `epoch-{N}` dir, and `clear_staging` must remove it.
    #[tokio::test]
    async fn test_import_partial_to_staging() {
        use tokio::io::AsyncReadExt as _;

        let src_dir = TempDir::with_prefix("test_staging_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(src_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let num_outputs = 20u64;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }

        // Build the partial prefix stream up to `k` from the source's in-progress epoch.
        let k = 12u64;
        let (stream, len) = source.get_partial_epoch_stream(0, k).await.expect("partial stream");
        let limited = stream.take(len);
        let cutoff = &outputs[(k - 1) as usize];
        let mut partial_record = previous_epoch.clone();
        partial_record.final_consensus = ConsensusNumHash::new(cutoff.number(), cutoff.digest());

        // Import into a DESTINATION chain's staging area. The destination has the epoch open but
        // empty (no outputs written) — staging must not disturb its live `epoch-0` dir.
        let dst_dir = TempDir::with_prefix("test_staging_dst").expect("temp dir");
        let dest = ConsensusChain::new(dst_dir.path().to_owned(), committee.clone()).unwrap();
        dest.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        dest.import_partial_to_staging(
            limited,
            &partial_record,
            &previous_epoch,
            Duration::from_secs(5),
        )
        .await
        .expect("staging import of verified prefix");

        assert_eq!(dest.staging_final(), Some(k), "staging final should be k");
        // Staged outputs (with batches) are readable and match the source.
        for i in 0..k {
            let staged = dest.staging_consensus_output(i + 1).await.expect("staged output present");
            compare_outputs(&staged, &outputs[i as usize]);
        }
        // Past the staged cutoff there is nothing.
        assert!(dest.staging_consensus_output(k + 1).await.is_none());
        // The live epoch-0 pack was untouched (still empty — staging is a separate dir).
        assert!(
            dest.get_consensus_output_current(1).await.is_err(),
            "staging import must not write into the live epoch dir"
        );
        let staged = staging_dirs(dst_dir.path(), 0);
        assert_eq!(staged.len(), 1, "one staging dir should exist");
        let staging_path = staged[0].clone();

        // clear_staging closes the pack and removes the dir.
        dest.clear_staging().await;
        assert_eq!(dest.staging_final(), None);
        assert!(dest.staging_consensus_output(1).await.is_none());
        assert!(
            !std::fs::exists(&staging_path).unwrap_or(true),
            "staging dir should be removed after clear_staging"
        );
    }

    /// Two partial imports of the same epoch must not interleave in its staging directory: the
    /// second waits for the first, instead of clearing the directory under the first's open files
    /// while it is still streaming.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_staging_imports_run_one_at_a_time() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let src_dir = TempDir::with_prefix("test_staging_serial_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(src_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let mut parent = ConsensusHeader::default().digest();
        let mut last = None;
        for i in 0..8u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            last = Some(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }
        let last = last.expect("outputs saved");
        let (stream, len) = source.get_partial_epoch_stream(0, 8).await.expect("partial stream");
        let mut bytes = Vec::new();
        stream.take(len).read_to_end(&mut bytes).await.expect("read partial stream");
        let mut record = previous_epoch.clone();
        record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());

        let dst_dir = TempDir::with_prefix("test_staging_serial_dst").expect("temp dir");
        let dest = ConsensusChain::new(dst_dir.path().to_owned(), committee.clone()).unwrap();
        dest.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // The first import streams half its bytes, then stalls until released.
        let (mut writer, reader) = tokio::io::duplex(1 << 20);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let half = bytes.len() / 2;
        let feed = {
            let bytes = bytes.clone();
            tokio::spawn(async move {
                writer.write_all(&bytes[..half]).await.expect("write first half");
                let _ = release_rx.await;
                writer.write_all(&bytes[half..]).await.expect("write second half");
                writer.shutdown().await.expect("end stream");
            })
        };
        let first = {
            let (dest, record, previous_epoch) =
                (dest.clone(), record.clone(), previous_epoch.clone());
            tokio::spawn(async move {
                dest.import_partial_to_staging(
                    reader,
                    &record,
                    &previous_epoch,
                    Duration::from_secs(30),
                )
                .await
            })
        };
        let first_streaming = || {
            staging_dirs(dst_dir.path(), 0)
                .iter()
                .any(|d| d.join("epoch-0").join(crate::consensus_pack::DATA_NAME).exists())
        };
        for _ in 0..200 {
            if first_streaming() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(first_streaming(), "the first import is streaming into staging");

        let second = {
            let (dest, record, previous_epoch) =
                (dest.clone(), record.clone(), previous_epoch.clone());
            let bytes = bytes.clone();
            tokio::spawn(async move {
                dest.import_partial_to_staging(
                    std::io::Cursor::new(bytes),
                    &record,
                    &previous_epoch,
                    Duration::from_secs(30),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!second.is_finished(), "the second import waits for the first");
        assert!(first_streaming(), "the first import's files are left alone");

        release_tx.send(()).expect("release the first import");
        feed.await.expect("feeder");
        first.await.expect("first task").expect("first import");
        second.await.expect("second task").expect("second import");
        assert_eq!(dest.staging_final(), Some(8));
        assert!(dest.staging_consensus_output(8).await.is_some(), "the staged prefix is readable");
        dest.clear_staging().await;
    }

    /// A node upgraded from a pre-sentinel build holds its past epochs as legacy packs (v0
    /// batches-first or v1 header-first). Serving one to a syncing peer never streams the legacy
    /// file: `get_epoch_stream` reads through `get_static`, which migrates the epoch to the current
    /// format on disk first, so the peer receives exactly the migrated file and imports every
    /// output intact.
    #[tokio::test]
    async fn test_epoch_stream_of_legacy_epoch_serves_current_format() {
        use crate::{
            archive::pack::{Pack, PackCompression},
            consensus_pack::{ConsensusPack, PackRecord, DATA_NAME, PACK_VERSION},
        };
        use tokio::io::AsyncReadExt as _;

        let on_disk_version = |data: &std::path::Path| {
            Pack::<PackRecord>::open(data, 0, true, PackCompression::ZStd, PACK_VERSION)
                .expect("open data read-only")
                .version()
        };
        for version in [0_u16, 1] {
            let temp_dir = TempDir::with_prefix("test_legacy_epoch_stream").expect("temp dir");
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let committee = fixture.committee();
            let epoch0 = EpochRecord {
                epoch: 0,
                committee: committee.bls_keys().iter().copied().collect(),
                next_committee: committee.bls_keys().iter().copied().collect(),
                ..Default::default()
            };

            // Epoch 0 runs to completion, then the node writes into epoch 1, so a restart resumes
            // epoch 1 and epoch 0 is a past epoch, read only through `get_static`.
            let node = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
            node.new_epoch(epoch0.clone(), committee.clone()).await.unwrap();
            let mut outputs = Vec::new();
            let mut parent = ConsensusHeaderDigest::default();
            for n in 1..=3u64 {
                let output =
                    make_test_output(&committee, (n as usize) % 4, chain.clone(), n, parent);
                parent = output.digest();
                outputs.push(output.clone());
                node.save_consensus_output(output).await.unwrap();
            }
            let record0 = EpochRecord {
                epoch: 0,
                committee: committee.bls_keys().iter().copied().collect(),
                next_committee: committee.bls_keys().iter().copied().collect(),
                parent_hash: epoch0.digest(),
                final_consensus: ConsensusNumHash::new(3, parent),
                ..Default::default()
            };
            let committee1 = committee.advance_epoch_for_test(1);
            node.persist_current().await.unwrap();
            node.new_epoch(record0.clone(), committee1.clone()).await.unwrap();
            node.epochs().save_record(record0.clone()).await.unwrap();
            let output4 = make_test_output(&committee1, 0, chain.clone(), 4, parent);
            node.save_consensus_output(output4).await.unwrap();
            node.close().await;

            // Epoch 0 as a pre-sentinel build wrote it: the same outputs in a v{version} pack with
            // no clean-close sentinel.
            let legacy_dir = TempDir::with_prefix("test_legacy_epoch_src").expect("temp dir");
            let legacy = ConsensusPack::open_append_version(
                legacy_dir.path(),
                epoch0.clone(),
                committee.clone(),
                version,
            )
            .expect("open legacy pack");
            for output in outputs.iter().cloned() {
                legacy.save_consensus_output(output).await.expect("save legacy output");
            }
            legacy.persist().await.expect("persist legacy pack");
            legacy.close().await;
            let legacy_data = legacy_dir.path().join("epoch-0").join(DATA_NAME);
            let len = std::fs::metadata(&legacy_data).expect("meta").len();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&legacy_data)
                .expect("open legacy data")
                .set_len(len - crate::archive::data_file::SENTINEL_LEN)
                .expect("strip the sentinel");
            let epoch0_dir = temp_dir.path().join("epoch-0");
            std::fs::remove_dir_all(&epoch0_dir).expect("remove epoch 0");
            std::fs::rename(legacy_dir.path().join("epoch-0"), &epoch0_dir).expect("swap in");
            let data = epoch0_dir.join(DATA_NAME);
            assert_eq!(on_disk_version(&data), version, "precondition: a legacy epoch 0");

            // The upgraded node restarts and a peer asks for epoch 0.
            let node = ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
            let (stream, len) = node.get_epoch_stream(0).await.expect("epoch 0 is served");
            let mut served = Vec::new();
            stream.take(len).read_to_end(&mut served).await.expect("read the served stream");

            assert_eq!(on_disk_version(&data), PACK_VERSION, "v{version}: migrated before serving");
            let file = std::fs::read(&data).expect("read migrated data");
            assert!(
                served.len() as u64 == len && file.starts_with(&served),
                "v{version}: the served bytes are the migrated file's content"
            );
            let peer_dir = TempDir::with_prefix("test_legacy_epoch_peer").expect("temp dir");
            let imported = ConsensusPack::stream_import(
                peer_dir.path(),
                &served[..],
                0,
                &epoch0,
                3,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|e| panic!("v{version}: the peer imports the served pack: {e:?}"));
            for output in &outputs {
                let got = imported.get_consensus_output(output.number()).await.expect("imported");
                compare_outputs(&got, output);
            }
            imported.close().await;
            node.close().await;
        }
    }

    /// The per-import staging directories (`staging-{epoch}-{n}`) under `base`.
    fn staging_dirs(base: &std::path::Path, epoch: Epoch) -> Vec<std::path::PathBuf> {
        let prefix = format!("staging-{epoch}-");
        std::fs::read_dir(base)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
                    .map(|e| e.path())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Clearing the staged pack (the subscriber caught up past it) while another import of the same
    /// epoch is streaming must remove only the cleared pack's files, never the in-flight import's:
    /// that import completes and its staged pack is still on disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_clear_staging_spares_an_in_flight_import() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let src_dir = TempDir::with_prefix("test_staging_clear_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(src_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let mut parent = ConsensusHeader::default().digest();
        let mut last = None;
        for i in 0..8u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            last = Some(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }
        let last = last.expect("outputs saved");
        let (stream, len) = source.get_partial_epoch_stream(0, 8).await.expect("partial stream");
        let mut bytes = Vec::new();
        stream.take(len).read_to_end(&mut bytes).await.expect("read partial stream");
        let mut record = previous_epoch.clone();
        record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());

        let dst_dir = TempDir::with_prefix("test_staging_clear_dst").expect("temp dir");
        let dest = ConsensusChain::new(dst_dir.path().to_owned(), committee.clone()).unwrap();
        dest.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        // A first import is staged and installed.
        dest.import_partial_to_staging(
            std::io::Cursor::new(bytes.clone()),
            &record,
            &previous_epoch,
            Duration::from_secs(30),
        )
        .await
        .expect("first import");

        // A second import of the same epoch streams half its bytes, then stalls.
        let (mut writer, reader) = tokio::io::duplex(1 << 20);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let half = bytes.len() / 2;
        let feed = {
            let bytes = bytes.clone();
            tokio::spawn(async move {
                writer.write_all(&bytes[..half]).await.expect("write first half");
                let _ = release_rx.await;
                writer.write_all(&bytes[half..]).await.expect("write second half");
                writer.shutdown().await.expect("end stream");
            })
        };
        let second = {
            let (dest, record, previous_epoch) =
                (dest.clone(), record.clone(), previous_epoch.clone());
            tokio::spawn(async move {
                dest.import_partial_to_staging(
                    reader,
                    &record,
                    &previous_epoch,
                    Duration::from_secs(30),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!second.is_finished(), "the second import is mid-stream");

        // The subscriber clears the installed (first) staged pack meanwhile.
        dest.clear_staging().await;
        release_tx.send(()).expect("release the second import");
        feed.await.expect("feeder");
        second.await.expect("second task").expect("second import");
        assert_eq!(dest.staging_final(), Some(8));
        let staged_on_disk = staging_dirs(dst_dir.path(), 0)
            .iter()
            .any(|d| d.join("epoch-0").join(crate::consensus_pack::DATA_NAME).exists());
        assert!(staged_on_disk, "the second import's staged pack is still on disk");
        dest.clear_staging().await;
    }

    /// A partial import whose streamed prefix does not end at the expected `final_consensus` must
    /// be rejected with `InvalidImport`. This exercises the error path where the imported pack
    /// is the only handle: it must be async-`close()`d (not left to a blocking `Drop` join on
    /// the worker) and its staging dir removed. The call must return promptly — a hang here
    /// would mean `close()` deadlocked — and leave nothing staged.
    #[tokio::test]
    async fn test_import_partial_to_staging_invalid_rejects_and_cleans_up() {
        use tokio::io::AsyncReadExt as _;

        let src_dir = TempDir::with_prefix("test_staging_invalid_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(src_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let num_outputs = 20u64;
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            source.save_consensus_output(output).await.unwrap();
        }

        // Build a valid prefix up to `k`, but claim the WRONG final hash (zero) for it. The
        // stream's internal link-by-link chain is fine, so stream_import succeeds — only
        // the final-hash check in import_partial_to_staging fails, taking the
        // `InvalidImport` error path.
        let k = 12u64;
        let (stream, len) = source.get_partial_epoch_stream(0, k).await.expect("partial stream");
        let limited = stream.take(len);
        let mut bad_record = previous_epoch.clone();
        bad_record.final_consensus = ConsensusNumHash::new(k, Default::default());

        let dst_dir = TempDir::with_prefix("test_staging_invalid_dst").expect("temp dir");
        let dest = ConsensusChain::new(dst_dir.path().to_owned(), committee.clone()).unwrap();
        dest.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let err = dest
            .import_partial_to_staging(
                limited,
                &bad_record,
                &previous_epoch,
                Duration::from_secs(5),
            )
            .await
            .expect_err("mismatched final hash must be rejected");
        assert!(
            matches!(err, ConsensusChainError::InvalidImport),
            "expected InvalidImport, got {err:?}"
        );
        // Nothing staged, and the staging dir was cleaned up after the pack was closed.
        assert_eq!(dest.staging_final(), None, "a rejected import must not leave a staged pack");
        assert!(
            staging_dirs(dst_dir.path(), 0).is_empty(),
            "rejected import must remove its staging dir"
        );
        // The chain is still usable (the error path did not poison a lock or leave the dest
        // wedged).
        assert!(dest.get_consensus_output_current(1).await.is_err());
    }

    /// With the in-progress epoch open as `current_pack` but only built up to `k`, reads for
    /// numbers in `(k, m]` must FALL THROUGH to the staged prefix rather than erroring out of
    /// the current-pack branch. Covers the output, header, and bytes read paths.
    #[tokio::test]
    async fn test_reads_fall_through_to_staging_for_open_epoch() {
        use tokio::io::AsyncReadExt as _;

        let src_dir = TempDir::with_prefix("test_staging_shadow_src").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        // Source: a full in-progress epoch 0 with `num_outputs` outputs.
        let source = ConsensusChain::new(src_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let num_outputs = 20u64;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }

        // Build a verified prefix up to `m` to stage into the destination.
        let m = 12u64;
        let (stream, len) = source.get_partial_epoch_stream(0, m).await.expect("partial stream");
        let limited = stream.take(len);
        let cutoff = &outputs[(m - 1) as usize];
        let mut partial_record = previous_epoch.clone();
        partial_record.final_consensus = ConsensusNumHash::new(cutoff.number(), cutoff.digest());

        // Destination: open epoch 0 as the live current pack and build it ONLY up to `k` (k < m),
        // in order — exactly the state a catching-up node is in.
        let dst_dir = TempDir::with_prefix("test_staging_shadow_dst").expect("temp dir");
        let dest = ConsensusChain::new(dst_dir.path().to_owned(), committee.clone()).unwrap();
        dest.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let k = 5u64;
        for i in 0..k {
            dest.save_consensus_output(outputs[i as usize].clone()).await.unwrap();
        }
        dest.import_partial_to_staging(
            limited,
            &partial_record,
            &previous_epoch,
            Duration::from_secs(5),
        )
        .await
        .expect("staging import of verified prefix");
        assert_eq!(dest.staging_final(), Some(m));

        // Sanity: the live epoch-0 pack really only holds 1..=k — numbers in (k, m] are NOT in it,
        // so any successful read of them below must have come from staging.
        assert!(dest.get_consensus_output_current(k).await.is_ok());
        assert!(
            dest.get_consensus_output_current(k + 1).await.is_err(),
            "live current pack must not contain numbers past k"
        );

        // 1..=k come from the live current pack; (k, m] fall through to staging (the fix). All
        // three read paths must behave the same.
        for j in 1..=m {
            let out = dest
                .consensus_output_by_number(j)
                .await
                .expect("output read should not error")
                .expect("output should be present (current pack or staging)");
            compare_outputs(&out, &outputs[(j - 1) as usize]);

            let header = dest
                .consensus_header_by_number(j)
                .await
                .expect("header read should not error")
                .expect("header should be present (current pack or staging)");
            assert_eq!(header.number, j);

            assert!(
                dest.consensus_output_bytes_by_number(j)
                    .await
                    .expect("bytes read should not error")
                    .is_some(),
                "output bytes should be present (current pack or staging)"
            );
        }

        // A number held by neither the live pack nor the staged prefix surfaces an error.
        assert!(
            dest.consensus_output_by_number(m + 1).await.is_err(),
            "a number past both the live pack and staging should error"
        );

        // After clearing staging, the previously staging-served numbers are gone again, but the
        // live pack's own numbers remain.
        dest.clear_staging().await;
        assert!(
            dest.consensus_output_by_number(k + 1).await.is_err(),
            "after clear_staging, numbers past k are no longer available"
        );
        let out = dest
            .consensus_output_by_number(k)
            .await
            .expect("read should not error")
            .expect("live pack number still present");
        compare_outputs(&out, &outputs[(k - 1) as usize]);
    }

    /// `stream_import` must be a no-op (and crucially must NOT truncate) when we already hold the
    /// requested final consensus header — including a PARTIAL request whose final is behind our
    /// latest. Guards the anti-truncation early-return.
    #[tokio::test]
    async fn test_stream_import_idempotent_no_truncation() {
        let temp_dir = TempDir::with_prefix("test_import_idempotent").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 20u64;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }
        consensus_chain.persist_current().await.expect("persist");

        // A partial import whose final (`k`) is BEHIND our latest must early-return without
        // touching the pack. The stream is never read, so an empty reader is fine.
        let k = 12u64;
        let cutoff = &outputs[(k - 1) as usize];
        let mut partial_record = previous_epoch.clone();
        partial_record.final_consensus = ConsensusNumHash::new(cutoff.number(), cutoff.digest());
        consensus_chain
            .stream_import(
                tokio::io::empty(),
                &partial_record,
                &previous_epoch,
                Duration::from_secs(5),
            )
            .await
            .expect("partial import of data we already hold must be Ok");

        // Every output is still present — nothing was truncated to the partial prefix.
        for i in 0..num_outputs {
            consensus_chain
                .get_consensus_output_current(i + 1)
                .await
                .expect("output still present after no-op partial import");
        }

        // An exact full-final no-op is also Ok.
        let last = outputs.last().unwrap();
        let mut full_record = previous_epoch.clone();
        full_record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());
        consensus_chain
            .stream_import(
                tokio::io::empty(),
                &full_record,
                &previous_epoch,
                Duration::from_secs(5),
            )
            .await
            .expect("exact full import of data we already hold must be Ok");
    }

    /// A process killed during an import leaves `import-{epoch}/{pid}.inproc` behind. When the
    /// next process gets the same pid (pid 1 in a container), that sentinel would make every later
    /// import of the epoch a silent no-op. Opening the chain must sweep stale import dirs, so the
    /// epoch is no longer in flight and an import of it installs the pack.
    #[tokio::test]
    async fn test_new_clears_stale_import_dirs() {
        let source_dir = TempDir::with_prefix("test_stale_import_src").expect("temp dir");
        let target_dir = TempDir::with_prefix("test_stale_import_dst").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        // a complete epoch 0 pack to import from
        let source = ConsensusChain::new(source_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..5u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            source.save_consensus_output(output).await.unwrap();
        }
        source.persist_current().await.expect("persist");
        let epoch_record = EpochRecord {
            final_consensus: ConsensusNumHash::new(5, parent),
            ..previous_epoch.clone()
        };
        source.epochs().save_record(epoch_record.clone()).await.expect("save record");

        // the sentinel a killed import of epoch 0 leaves when this process had its pid
        let import_dir = target_dir.path().join("import-0");
        std::fs::create_dir_all(&import_dir).expect("create import dir");
        std::fs::File::create(import_dir.join(format!("{}.inproc", std::process::id())))
            .expect("create sentinel");

        let target = ConsensusChain::new(target_dir.path().to_owned(), committee.clone()).unwrap();
        assert!(!import_dir.exists(), "the open must remove the stale import dir");
        assert!(
            !target.already_streaming_epoch(0),
            "a stale import sentinel must not survive open"
        );
        target.epochs().save_record(epoch_record.clone()).await.expect("save record");
        assert!(!target.is_epoch_complete(&epoch_record).await);
        use tokio::io::AsyncReadExt as _;
        let (stream, len) = source.get_epoch_stream(0).await.expect("epoch stream");
        target
            .stream_import(stream.take(len), &epoch_record, &previous_epoch, Duration::from_secs(5))
            .await
            .expect("import");
        assert!(target.is_epoch_complete(&epoch_record).await, "the import must install the pack");
        source.close().await;
        target.close().await;
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
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        // Save some outputs, keeping the originals to compare against.
        let num_outputs = 10;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }

        // Each saved number returns Some(bytes) that decode back to the original output.
        for (i, expected) in outputs.iter().enumerate().take(num_outputs) {
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
            compare_outputs(&decoded, expected);
        }

        // A number below the pack's start is out of range and must error.
        assert!(
            consensus_chain.consensus_output_bytes_by_number(0).await.is_err(),
            "number below start must error"
        );

        // A fresh chain with no epoch opened has no data and returns Ok(None).
        let empty_dir = TempDir::with_prefix("test_output_bytes_empty").expect("temp dir");
        let empty_chain =
            ConsensusChain::new(empty_dir.path().to_owned(), committee.clone()).unwrap();
        assert!(
            empty_chain.consensus_output_bytes_by_number(1).await.is_err(),
            "empty chain will should return a too high error"
        );
    }

    /// `stream_decode_consensus_output` resolves the epoch's committee (here the current pack),
    /// stream-decodes the reassembled bytes, and verifies the header digest against the expected
    /// hash: a matching hash returns the equal output, a wrong hash is rejected with
    /// `UnexpectedConsensusDigest` (the wrapper for the requested-output receive path).
    #[tokio::test]
    async fn test_stream_decode_consensus_output() {
        use crate::consensus_pack::PackError;
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_stream_decode").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let mut parent = ConsensusHeader::default().digest();
        let mut outputs = Vec::new();
        for i in 0..5u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            consensus_chain.save_consensus_output(output).await.unwrap();
        }

        for (i, original) in outputs.iter().enumerate() {
            let number = i as u64 + 1;
            let bytes = consensus_chain
                .consensus_output_bytes_by_number(number)
                .await
                .expect("query ok")
                .expect("bytes present");

            // Correct hash: resolves the current pack's committee, decodes, and verifies.
            let decoded = consensus_chain
                .stream_decode_consensus_output(
                    0,
                    Cursor::new(bytes.clone()),
                    original.digest(),
                    Duration::from_secs(5),
                )
                .await
                .expect("verified stream decode");
            compare_outputs(&decoded, original);

            // Wrong hash: rejected with UnexpectedConsensusDigest through the ConsensusChainError.
            let res = consensus_chain
                .stream_decode_consensus_output(
                    0,
                    Cursor::new(bytes),
                    ConsensusHeader::default().digest(),
                    Duration::from_secs(5),
                )
                .await;
            assert!(
                matches!(
                    res,
                    Err(ConsensusChainError::PackError(
                        PackError::UnexpectedConsensusDigest { .. }
                    ))
                ),
                "wrong hash must be rejected, got {res:?}"
            );
        }
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
        let source = ConsensusChain::new(source_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 50;
        let mut parent = ConsensusHeader::default().digest();
        let mut last = None;
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = output.digest();
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
            let target = Arc::new(
                ConsensusChain::new(target_dir.path().to_owned(), committee.clone()).unwrap(),
            );
            use tokio::io::AsyncReadExt as _;
            let (stream, len) = source.get_epoch_stream(0).await.expect("source epoch stream");

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
                .stream_import(
                    stream.take(len),
                    &epoch_record,
                    &previous_epoch,
                    Duration::from_secs(5),
                )
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
            let pack = target.get_static(0).await.expect("epoch-0 pack readable after race");
            let header = pack
                .latest_consensus_header()
                .await
                .expect("epoch-0 pack readable")
                .expect("epoch-0 has a final header");
            assert_eq!(header.number, epoch_record.final_consensus.number, "final header number");
            assert_eq!(header.digest(), epoch_record.final_consensus.hash, "final header digest");
        }
    }

    /// An observer that caught up via `stream_import` ends up with a *static* (read-only)
    /// pack for the imported epoch as its current pack, while replaying that epoch's outputs
    /// advances `latest_consensus` into it. On restart `ConsensusChain::new` takes the
    /// "already running" branch (`latest_consensus` is no longer `0/0`) and must re-open the
    /// imported epoch with `open_append_exists` rather than erroring. This locks in that
    /// invariant: a node that only ever obtained an epoch by import can still restart and
    /// serve the data.
    #[tokio::test]
    async fn test_new_reopens_imported_epoch_on_restart() {
        // Build a complete epoch-0 pack on a source chain to stream from.
        let source_dir = TempDir::with_prefix("test_import_restart_source").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let source = ConsensusChain::new(source_dir.path().to_owned(), committee.clone()).unwrap();
        source.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 10u64;
        let mut parent = ConsensusHeader::default().digest();
        let mut outputs = Vec::new();
        for i in 0..num_outputs {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            source.save_consensus_output(output).await.unwrap();
        }
        source.persist_current().await.expect("persist source");
        let last = outputs.last().expect("at least one output").clone();
        let mut epoch_record = previous_epoch.clone();
        epoch_record.final_consensus = ConsensusNumHash::new(last.number(), last.digest());
        source.epochs().save_record(epoch_record.clone()).await.expect("save epoch record");

        let target_dir = TempDir::with_prefix("test_import_restart_target").expect("temp dir");
        {
            let target =
                ConsensusChain::new(target_dir.path().to_owned(), committee.clone()).unwrap();
            // Import epoch 0 from the source: the target's current pack becomes a static pack.
            use tokio::io::AsyncReadExt as _;
            let (stream, len) = source.get_epoch_stream(0).await.expect("source epoch stream");
            target
                .stream_import(
                    stream.take(len),
                    &epoch_record,
                    &previous_epoch,
                    Duration::from_secs(5),
                )
                .await
                .expect("stream import");
            // Replay the imported outputs the way an executing observer would; this advances
            // latest_consensus into the (static) imported epoch without rewriting the pack.
            for output in &outputs {
                target.save_consensus_output(output.clone()).await.expect("replay imported output");
            }
            target.persist_current().await.expect("persist target");
        }

        // Restart: new() must re-open the imported epoch (open_append_exists) and not error,
        // even though the only on-disk pack for that epoch came from stream_import.
        let reopened = ConsensusChain::new(target_dir.path().to_owned(), committee.clone())
            .expect("reopen imported epoch on restart");
        for output in &outputs {
            let got = reopened
                .get_consensus_output_current(output.number())
                .await
                .expect("imported output readable after restart");
            compare_outputs(&got, output);
        }
    }

    /// Returns `committee` as a node's own chain read may see it while agreeing with a peer on
    /// everything import authenticates: same epoch and BLS keys, but every execution address
    /// changed and, where the multi-worker fork is active for its epoch, one more worker. Before
    /// the fork (adiri builds) the committee layout encodes a single worker, so only the addresses
    /// drift. Imported outputs still decode under it because the worker count never shrinks.
    fn drifted(committee: &Committee) -> Committee {
        use tn_types::forks::multi_workers_fork_active;

        let authorities = committee
            .authorities()
            .into_iter()
            .map(|authority| {
                let key = *authority.protocol_key();
                // the bitwise complement differs from the original address in every byte
                (key, Authority::new_for_test(key, !authority.execution_address()))
            })
            .collect();
        // the pre-fork layout refuses to encode any worker count but one
        let added = if multi_workers_fork_active(committee.epoch()) { 1 } else { 0 };
        let workers = NonZeroUsize::new(committee.number_of_workers() + added)
            .expect("a positive worker count");
        let drifted =
            Committee::new_for_test(authorities, committee.epoch(), committee.bootstrap_servers())
                .with_num_workers(workers);
        assert_eq!(drifted.bls_keys(), committee.bls_keys(), "drift must keep the BLS keys");
        assert_ne!(&drifted, committee, "drift must change the committee");
        drifted
    }

    /// Asserts each of `expected` reads back from `chain`'s current pack with the same header and
    /// batches, and with every batch producer resolved through `local`: the chain-derived
    /// committee the pack was opened with, not the peer-served meta on disk.
    async fn assert_outputs_decode_with(
        chain: &ConsensusChain,
        local: &Committee,
        expected: &[ConsensusOutput],
    ) {
        for want in expected {
            let number = want.number();
            let got =
                chain.get_consensus_output_current(number).await.expect("imported output readable");
            assert_eq!(got.digest(), want.digest(), "output {number} header");
            assert_eq!(got.batch_digests(), want.batch_digests(), "output {number} batch digests");
            let producer = local
                .authority(got.leader().author())
                .expect("leader is in the local committee")
                .execution_address();
            assert_eq!(got.batches().len(), want.batches().len(), "output {number} batch count");
            for (got_batch, want_batch) in got.batches().iter().zip(want.batches()) {
                assert_eq!(got_batch.batches, want_batch.batches, "output {number} batches");
                assert_eq!(got_batch.address, producer, "output {number} batch producer");
            }
        }
    }

    /// A node that imports a future epoch before reaching it opens that pack through `new_epoch`
    /// once it gets there. Its committee comes from its own chain read and may differ from the
    /// peer-served pack meta in fields import never authenticates (execution addresses, worker
    /// count). The open must accept that drift, keep the chain-derived committee, and serve the
    /// imported outputs.
    #[tokio::test]
    async fn test_new_epoch_opens_imported_future_epoch_with_drifted_committee() {
        // A source chain with a complete epoch 1 behind a complete epoch 0.
        let source_dir = TempDir::with_prefix("test_drift_future_source").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let committee1 = committee.advance_epoch_for_test(1);
        let (source, record0, mut parent) =
            chain_with_epoch0_outputs(&source_dir, &committee, &chain).await;
        source.persist_current().await.expect("persist source epoch 0");
        source.new_epoch(record0.clone(), committee1.clone()).await.expect("source epoch 1");
        let mut outputs = Vec::new();
        for n in 4..=6u64 {
            let output = make_test_output(&committee1, (n as usize) % 4, chain.clone(), n, parent);
            parent = output.digest();
            outputs.push(output.clone());
            source.save_consensus_output(output).await.expect("save epoch-1 output");
        }
        source.persist_current().await.expect("persist source epoch 1");
        let record1 = EpochRecord {
            epoch: 1,
            committee: committee1.bls_keys().iter().copied().collect(),
            next_committee: committee1.bls_keys().iter().copied().collect(),
            parent_hash: record0.digest(),
            final_consensus: ConsensusNumHash { number: 6, hash: parent },
            ..Default::default()
        };
        source.epochs().save_record(record0.clone()).await.expect("save epoch-0 record");
        source.epochs().save_record(record1.clone()).await.expect("save epoch-1 record");

        // The target imports epoch 1 while still in epoch 0.
        let target_dir = TempDir::with_prefix("test_drift_future_target").expect("temp dir");
        let target = ConsensusChain::new(target_dir.path().to_owned(), committee.clone()).unwrap();
        use tokio::io::AsyncReadExt as _;
        let (stream, len) = source.get_epoch_stream(1).await.expect("source epoch-1 stream");
        target
            .stream_import(stream.take(len), &record1, &record0, Duration::from_secs(5))
            .await
            .expect("import future epoch 1");

        let local1 = drifted(&committee1);
        target
            .new_epoch(record0, local1.clone())
            .await
            .expect("new_epoch must open the imported epoch-1 pack despite committee drift");
        assert_eq!(target.current_pack().epoch(), 1);
        assert_eq!(target.current_pack().committee(), &local1, "pack keeps the chain committee");
        assert_outputs_decode_with(&target, &local1, &outputs).await;
    }

    /// Importing the epoch a node is in swaps its current pack for a static, read-only copy of
    /// the peer's pack. The next `new_epoch` for that epoch reopens it for appending with the
    /// node's own chain-derived committee, which may differ from the imported meta in fields
    /// import never authenticates. The reopen must accept that drift and keep serving the
    /// imported outputs.
    #[tokio::test]
    async fn test_new_epoch_replaces_static_current_import_with_drifted_committee() {
        let source_dir = TempDir::with_prefix("test_drift_static_source").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (source, record0, _) = chain_with_epoch0_outputs(&source_dir, &committee, &chain).await;
        source.persist_current().await.expect("persist source");
        source.epochs().save_record(record0.clone()).await.expect("save epoch record");
        let mut outputs = Vec::new();
        for n in 1..=3u64 {
            outputs.push(source.get_consensus_output_current(n).await.expect("source output"));
        }
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        let local = drifted(&committee);
        let target_dir = TempDir::with_prefix("test_drift_static_target").expect("temp dir");
        let target = ConsensusChain::new(target_dir.path().to_owned(), local.clone()).unwrap();
        use tokio::io::AsyncReadExt as _;
        let (stream, len) = source.get_epoch_stream(0).await.expect("source epoch stream");
        target
            .stream_import(stream.take(len), &record0, &previous_epoch, Duration::from_secs(5))
            .await
            .expect("import current epoch 0");
        assert!(
            target.current_pack().is_static(),
            "importing the current epoch installs it static"
        );

        target
            .new_epoch(previous_epoch, local.clone())
            .await
            .expect("new_epoch must reopen the imported current epoch despite committee drift");
        assert!(!target.current_pack().is_static(), "new_epoch must replace the static pack");
        assert_eq!(target.current_pack().epoch(), 0);
        assert_eq!(target.current_pack().committee(), &local, "pack keeps the chain committee");
        assert_outputs_decode_with(&target, &local, &outputs).await;
    }

    /// A node that imported epoch 0 without replaying it still has `latest_consensus` at `0/0`,
    /// so on restart `ConsensusChain::new` opens epoch 0 with `open_append` against the imported
    /// meta, using its own committee. That committee may differ from the imported meta in fields
    /// import never authenticates; startup must accept the drift and serve the imported outputs.
    #[tokio::test]
    async fn test_new_restarts_after_epoch0_import_with_drifted_committee() {
        let source_dir = TempDir::with_prefix("test_drift_restart_source").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        let (source, record0, _) = chain_with_epoch0_outputs(&source_dir, &committee, &chain).await;
        source.persist_current().await.expect("persist source");
        source.epochs().save_record(record0.clone()).await.expect("save epoch record");
        let mut outputs = Vec::new();
        for n in 1..=3u64 {
            outputs.push(source.get_consensus_output_current(n).await.expect("source output"));
        }
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };

        let local = drifted(&committee);
        let target_dir = TempDir::with_prefix("test_drift_restart_target").expect("temp dir");
        {
            let target = ConsensusChain::new(target_dir.path().to_owned(), local.clone()).unwrap();
            use tokio::io::AsyncReadExt as _;
            let (stream, len) = source.get_epoch_stream(0).await.expect("source epoch stream");
            target
                .stream_import(stream.take(len), &record0, &previous_epoch, Duration::from_secs(5))
                .await
                .expect("import epoch 0");
            // no replay: latest_consensus stays at 0/0, so the restart takes the epoch-0 branch
        }

        let reopened = ConsensusChain::new(target_dir.path().to_owned(), local.clone())
            .expect("restart must open the imported epoch 0 despite committee drift");
        assert_eq!(reopened.current_pack().epoch(), 0);
        assert_eq!(reopened.current_pack().committee(), &local, "pack keeps the chain committee");
        assert_outputs_decode_with(&reopened, &local, &outputs).await;
    }

    /// `current_data_len` must reject a mismatched epoch: the state export pairs the returned
    /// length with a source path built from a separately-derived epoch, so a length read from a
    /// different (handoff-raced) current pack would bound the wrong file. It returns the length
    /// for the matching epoch and `InvalidPackEpoch` otherwise.
    #[tokio::test]
    async fn test_current_data_len_guards_epoch() {
        let temp_dir = TempDir::with_prefix("test_current_data_len_epoch").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee.clone()).await.unwrap();
        assert_eq!(consensus_chain.current_pack().epoch(), 0);

        // Matching epoch: returns the current pack's logical length.
        let len = consensus_chain.current_data_len(0).await.expect("len for the current epoch");
        assert_eq!(len, consensus_chain.current_pack().data_file_len().await.unwrap());

        // Mismatched epoch: refuse rather than return another epoch's length.
        let err =
            consensus_chain.current_data_len(1).await.expect_err("must reject a mismatched epoch");
        assert!(
            matches!(err, ConsensusChainError::InvalidPackEpoch(0, 1)),
            "expected InvalidPackEpoch(0, 1), got {err:?}"
        );
    }

    /// A same-epoch `new_epoch` re-entry whose committee differs from the pack's persisted
    /// epoch-start snapshot — what a canonical-tip-seeded entry read would pass after a
    /// mid-epoch governance burn swap-and-pops a validator — must still return Ok and keep
    /// the original pack untouched. The committee cross-check on that path is a warn-only
    /// tripwire; the pack's epoch-start snapshot stays authoritative for decoding this
    /// epoch's output.
    #[tokio::test]
    async fn test_same_epoch_reentry_with_shrunken_committee_keeps_pack() {
        let temp_dir = TempDir::with_prefix("test_reentry_shrunken").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        // Open epoch 0 with committee A (the epoch-start snapshot the pack persists).
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();
        assert_eq!(consensus_chain.current_pack().epoch(), 0);
        assert_eq!(consensus_chain.current_pack().committee().bls_keys(), committee.bls_keys());

        // Committee B: same epoch with one member dropped, mirroring the post-burn on-chain
        // committee a tip-based read would return.
        let mut authorities: BTreeMap<BlsPublicKey, Authority> =
            committee.authorities().into_iter().map(|a| (*a.protocol_key(), a)).collect();
        let (dropped, _) = authorities.pop_last().expect("fixture committee is non-empty");
        let shrunken = Committee::new_for_test(authorities, 0, BTreeMap::default());
        assert_eq!(shrunken.epoch(), committee.epoch());
        assert!(shrunken.bls_keys().len() < committee.bls_keys().len());

        // Same-epoch re-entry with the mismatched committee: Ok (the tripwire only warns),
        // and the current pack is unchanged — same epoch, still committee A.
        consensus_chain
            .new_epoch(previous_epoch, shrunken)
            .await
            .expect("same-epoch re-entry with a mismatched committee must remain Ok");
        let pack = consensus_chain.current_pack();
        assert_eq!(pack.epoch(), 0, "re-entry must not change the current pack's epoch");
        assert_eq!(
            pack.committee().bls_keys(),
            committee.bls_keys(),
            "pack must retain the epoch-start committee snapshot"
        );
        assert!(
            pack.committee().bls_keys().contains(&dropped),
            "the burned member stays in the pack's snapshot for decoding this epoch"
        );
    }

    /// #23: `wait_until_sole_owner` bounds the wait for another `ConsensusChain` clone (in production
    /// the worker RPC server's `EngineToPrimaryRpc`) to drop, so `close()` runs instead of
    /// no-opping through `Arc::try_unwrap`. It reports false while a clone lives and true once
    /// this is sole.
    #[tokio::test]
    async fn test_wait_until_sole_owner_bounds_the_clone_wait() {
        let temp_dir = TempDir::with_prefix("test_sole_owner").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee.clone()).await.unwrap();

        // A second clone (models the RPC server's `EngineToPrimaryRpc` clone) keeps it non-sole, so
        // the bounded wait times out and reports false.
        let clone = consensus_chain.clone();
        assert!(
            !consensus_chain.wait_until_sole_owner(std::time::Duration::from_millis(100)).await,
            "must not report sole ownership while another clone lives"
        );

        // Once the only other clone drops, the wait resolves to sole ownership promptly.
        drop(clone);
        assert!(
            consensus_chain.wait_until_sole_owner(std::time::Duration::from_secs(2)).await,
            "must report sole ownership after the only other clone drops"
        );

        // Sole owner now, so close() actually runs (does not no-op).
        consensus_chain.close().await;
    }

    /// Graceful shutdown seals the current epoch even when the chain is the sole owner but a
    /// transient handle to its current pack is still alive (a reader mid-request): the pack's own
    /// `close()` would no-op under that handle and leave the epoch to a WAL recovery on restart.
    #[tokio::test]
    async fn test_close_seals_the_current_pack_under_a_live_pack_handle() {
        use crate::consensus_pack::{pack_unsealed_version, DATA_NAME, PACK_VERSION};
        let temp_dir = TempDir::with_prefix("test_close_pack_handle").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch, committee.clone()).await.unwrap();

        let live_handle = consensus_chain.current_pack();
        consensus_chain.close().await;
        let data = temp_dir.path().join("epoch-0").join(DATA_NAME);
        assert_eq!(
            pack_unsealed_version(&data, 0),
            Some((PACK_VERSION, false)),
            "the current pack is sealed at shutdown"
        );
        drop(live_handle);
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
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
        consensus_chain.new_epoch(previous_epoch.clone(), committee.clone()).await.unwrap();

        let num_outputs = 100;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeaderDigest::default();
        for i in 0..num_outputs {
            let consensus_output =
                make_test_output(&committee, i % 4, chain.clone(), (i as u64) + 1, parent);
            parent = consensus_output.digest();
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
            parent = consensus_output.digest();
            outputs.push(consensus_output.clone());
            let last_header = consensus_output.consensus_header();
            consensus_chain.save_consensus_output(consensus_output).await.unwrap();
            let latest = consensus_chain
                .consensus_header_latest()
                .await
                .unwrap_or_else(|_| panic!("to have latest {i}"))
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
            parent = consensus_output.digest();
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
                .unwrap_or_else(|_| panic!("Failed to get header by number on {i}"))
                .unwrap();
            let output = outputs.get(i).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }

        consensus_chain.persist_current().await.expect("persist chain");
        drop(consensus_chain);
        let consensus_chain =
            ConsensusChain::new(temp_dir.path().to_owned(), committee.clone()).unwrap();
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
                .unwrap_or_else(|| panic!("something on {i}"));
            let output = outputs.get(i).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }
        // Now by digest
        for (i, output) in outputs.iter().enumerate().take(num_outputs * 3) {
            let epoch = (i / num_outputs) as Epoch;
            let digest = output.digest();
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
                .unwrap_or_else(|_| panic!("failed to get header by number epoch {epoch}, {i}"))
                .unwrap();
            let output = outputs.get(i).unwrap().consensus_header();
            assert_eq!(header_db.digest(), output.digest(), "consensus headers mismatch {i}");
        }
        for (i, output) in outputs.iter().enumerate().take(num_outputs * 3) {
            let epoch = i / num_outputs;
            let digest = output.digest();
            let header_db = consensus_chain
                .consensus_header_by_digest(epoch as u32, digest)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(digest, header_db.digest(), "consensus headers mismatch (by digest) {i}");
        }
    }
}
